"""v2 StateStore: 统一内存状态机与后端持久化事务的深模块。

内敛原子双写一致性、六集合全局互斥、commit 失败 3-strike 收敛记账与
依赖图级联分析。单一出口纪律：

- Job 终结事务唯一经由本模块的 apply_* 出口族收敛（成功 / 失败 / 重试 /
  跳过 / 批量失败 / 级联）——backend commit + 内存镜像 + 统计 + 级联
  在同一出口内对齐，内存永不领先磁盘；
- 失败终态内存登记唯一经由 ``mark_failed_memory``（wall/failed 互斥的
  单点维护）；
- attempt 收尾事件钩子唯一经由 ``fire_attempt_finished`` 触发（异常隔离
  + hook_errors 计数）；
- attempts 轨迹为旁路 append-only 观测面：apply_* 终态时同步收尾轨迹行，
  更新失败降级告警、绝不阻断主事务。
"""

from __future__ import annotations

import copy
import datetime
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from ..backend.base import AbstractStateBackend
from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.job import Job, JobRuntimeState, inject_worker_resource
from ..models.state import PipelineState, uid_from_job_dict
from ..utils.jsonutil import dumps
from .admission import ImmediateRequeuePolicy, RequeuePolicy, RerunPolicy
from .errorclass import ERR_COMMIT_FAILURE, ERR_JOB_DEPENDENCY, ErrorClassifier
from .types import TRANSIENT_KIND_STAT_KEYS, AttemptFinish

logger = logging.getLogger("tasklite.v2")

# commit 连续失败达阈值后按确定性坏输入收敛进失败档案（此前为无限崩溃
# 重启循环）；默认 3 次。
COMMIT_FAILURE_THRESHOLD = 3


def _utc_now_iso() -> str:
    """当前时刻的 UTC ISO 8601 串（first_enqueued_at / 轨迹收尾共用）。"""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


@dataclass(frozen=True)
class FailureEntry:
    """失败档案只读查询的结构化不可变条目（运维查询面的载荷形状）。

    - ``uid``: 作业唯一标识 (task_type::job_id)
    - ``error``: 原始错误消息/错误码
    - ``meta``: 完整失败档案 meta 字典视图
    - ``job_payload``: 原始业务 payload 快照（失败落盘无可存快照时为 None）
    """

    uid: str
    error: str
    meta: dict[str, Any]
    job_payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class SuccessOutcome:
    """apply_success 原子转移结果。"""
    uid: str
    wall_meta: dict[str, Any]
    spawned_uids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class FailureOutcome:
    """apply_failure 原子转移结果。"""
    uid: str
    error_meta: dict[str, Any]
    cascaded_uids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RetryOutcome:
    """apply_retry 原子转移结果。"""
    uid: str
    retry_dict: dict[str, Any]
    transient_kind: str | None = None


@dataclass(frozen=True)
class SkipOutcome:
    """apply_skip 原子转移结果。"""
    uid: str
    was_known: bool


@dataclass(frozen=True)
class BulkFailureOutcome:
    """apply_bulk_failure 原子转移结果。"""
    failed_uids: list[str]
    cascaded_uids: list[str] = field(default_factory=list)
    remaining_queue_count: int = 0


class StateStore:
    """统一内存状态机与后端持久化事务的深模块。"""

    def __init__(
        self,
        backend: AbstractStateBackend,
        state: PipelineState | None = None,
        *,
        commit_failure_threshold: int = COMMIT_FAILURE_THRESHOLD,
        classifier: ErrorClassifier | None = None,
        on_attempt_finished: Callable[..., None] | None = None,
        stats: Any | None = None,
        rerun_policy: RerunPolicy | None = None,
        requeue_policy: RequeuePolicy | None = None,
    ) -> None:
        self._backend = backend
        self._state = state if state is not None else PipelineState({}, {}, {}, [])
        self._threshold = commit_failure_threshold
        self._classifier = classifier if classifier is not None else ErrorClassifier()
        self._on_attempt_finished = on_attempt_finished
        self._stats = stats
        self._rerun_policy = rerun_policy if rerun_policy is not None else RerunPolicy()
        # 重试节奏唯一出口：核心不做任何节奏计算，默认立即重入队。
        self._requeue_policy = (
            requeue_policy if requeue_policy is not None else ImmediateRequeuePolicy()
        )

    # ── 统计与事件钩子 ────────────────────────────────────────────────

    def _record_stat(self, key: str, amount: int = 1) -> None:
        """更新统计指标（单一真相源）。"""
        if self._stats is not None:
            self._stats[key] = self._stats.get(key, 0) + amount

    def record_stat(self, key: str, amount: int = 1) -> None:
        """公开统计指标记录接缝（供孤儿延迟等特殊派发事件使用）。"""
        self._record_stat(key, amount)

    @property
    def stats(self) -> Any:
        """统计字典引用（可能为 None）。"""
        return self._stats

    def set_stats(self, stats: Any) -> None:
        """重设统计字典（run 启动期绑定会话统计）。"""
        self._stats = stats

    def set_on_attempt_finished(self, cb: Callable[..., None] | None) -> None:
        """设置 attempt 收尾事件回调。"""
        self._on_attempt_finished = cb

    def fire_attempt_finished(self, uid: str, *, outcome: AttemptFinish) -> None:
        """attempt 收尾事件钩子的单一出口（异常隔离 + hook_errors 计数）。

        钩子在统计更新之后触发（调用方保证），钩子内读统计一致；钩子
        抛异常只计数不外传——用户回调缺陷不得击穿事件泵。
        """
        if self._on_attempt_finished is None:
            return
        try:
            self._on_attempt_finished(uid, outcome=outcome)
        except Exception as e:
            logger.warning(f"on_attempt_finished hook raised for {uid}: {e}")
            self._record_stat("hook_errors", 1)

    def _finish_attempt(
        self, attempt_id: int | None, *, outcome: str, error: str | None = None
    ) -> None:
        """attempts 旁路轨迹行收尾；失败降级告警，绝不阻断主事务。

        attempt_id 为 None 表示本次终态没有对应的已派发轨迹行（伪 entry
        / 未派发路径），无行可收尾。
        """
        if attempt_id is None:
            return
        try:
            self._backend.update_attempt(
                attempt_id,
                outcome=outcome,
                finished_at=_utc_now_iso(),
                error=error,
            )
        except Exception as e:
            logger.warning(
                f"Attempt trace finalization degraded for attempt {attempt_id} "
                f"(outcome={outcome}): {e}"
            )

    @staticmethod
    def _attempt_error(meta: Mapping[str, Any]) -> str | None:
        """从失败 meta 提取轨迹行错误串（空串收敛为 None）。"""
        error = str(meta.get("error") or "")
        return error or None

    # ── 摄入管道（enqueue / spawn）──────────────────────────────────

    def normalize_and_validate_job(self, job: Job | dict[str, Any]) -> dict[str, Any]:
        """规范化并校验单个作业（序列化预检 → 策略注入 → 工人资源注入 →
        首入队时间填充）。

        不可 JSON 序列化的 payload 在入口拒绝——潜伏到后端落盘处才崩会
        把单作业坏输入放大为整 run 崩溃。
        """
        if isinstance(job, Job):
            try:
                dumps(job.payload)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"Payload for job {job.uid} is not JSON-serializable: {e}"
                ) from e
            job_dict = job.to_dict()
        elif isinstance(job, dict):
            if "task_type" not in job or "job_id" not in job:
                raise ValueError("Job dict must contain 'task_type' and 'job_id'")
            try:
                dumps(job.get("payload", {}))
            except (TypeError, ValueError) as e:
                uid = uid_from_job_dict(job)
                raise ValueError(
                    f"Payload for job {uid} is not JSON-serializable: {e}"
                ) from e
            job_dict = copy.deepcopy(job)
        else:
            raise TypeError(
                f"Expected Job or dict, got {type(job).__name__}"
            )

        self._finalize_intake_dict(job_dict, str(job_dict["task_type"]))
        return job_dict

    def normalize_spawned_job(self, job: Job) -> dict[str, Any]:
        """动态子作业的摄入规范化（成功提交路径的 spawn 侧单点）。

        与 enqueue 同规的三步收尾 + 整 dict 序列化预检；预检失败抛
        ValueError——调用方按「拒绝该子作业、不连坐父作业成功提交、
        不触发 commit 崩溃契约」的语义处置。
        """
        job_dict = job.to_dict()
        self._finalize_intake_dict(job_dict, job.task_type)
        try:
            dumps(job_dict)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Spawned job {job.uid} is not JSON-serializable: {e}"
            ) from e
        return job_dict

    def _finalize_intake_dict(self, job_dict: dict[str, Any], task_type: str) -> None:
        """摄入三步收尾：RerunPolicy 策略注入 → ``__workers__`` 资源注入 →
        first_enqueued_at 填充。

        first_enqueued_at 仅在缺失时填充（UTC ISO）：重试/spawn 回流的
        dict 已携带首次入队时间，绝不刷新——「首次入队时间」是端到端
        追溯链的锚点。
        """
        self._rerun_policy.normalize_job_dict(job_dict, task_type)
        inject_worker_resource(job_dict)
        if not job_dict.get("first_enqueued_at"):
            job_dict["first_enqueued_at"] = _utc_now_iso()

    def enqueue_jobs(
        self,
        jobs: Job | Sequence[Job] | dict[str, Any] | Sequence[dict[str, Any]],
        *,
        front: bool = False,
    ) -> list[str]:
        """统一作业入队摄入管道（类型校验、序列化预检、策略规范化、资源
        注入、首入队时间填充、单事务原子插入）。

        增量入队只写后端、不动内存镜像（内存队列在 run 启动加载期由
        恢复编排层重建）；返回实际插入后端的作业 UID 列表（自动去重）。
        """
        if isinstance(jobs, (Job, dict)):
            jobs_list = [jobs]
        elif isinstance(jobs, (list, tuple)):
            jobs_list = list(jobs)
        else:
            raise TypeError(
                "enqueue_jobs() expects a Job or a list of Job objects/dicts, "
                f"got {type(jobs).__name__}"
            )
        if not jobs_list:
            return []

        jobs_dicts = [self.normalize_and_validate_job(j) for j in jobs_list]
        return self._backend.enqueue_jobs(jobs_dicts, front=front)

    def requeue_transient(
        self,
        job_dict: dict[str, Any],
        *,
        transient_kind: str,
        attempt_id: int | None = None,
        error: str | None = None,
    ) -> None:
        """瞬态信号降级回队的单一出口（军规：零预算 + 立即重入队 + 零污染）。

        节奏唯一经 RequeuePolicy 规划（默认立即、队首插队——同轮内优先
        重扫，瞬态作业不被新入队作业排挤到队尾）；统计键走
        TRANSIENT_KIND_STAT_KEYS 登记表；不触碰 runtime——不烧 commit /
        dispatch 失败预算、不写 last_retry_error。``attempt_id`` 非 None 时
        同步把已派发轨迹行收尾为 requeued（旁路观测面单一出口内闭环）。
        """
        plan = self._requeue_policy.plan_requeue(job_dict, transient_kind=transient_kind)
        self._state.requeue_jobs([job_dict], front=plan.front)
        stat_key = TRANSIENT_KIND_STAT_KEYS.get(transient_kind)
        if stat_key:
            self._record_stat(stat_key, 1)
        self._finish_attempt(attempt_id, outcome="requeued", error=error)

    def finish_attempt(
        self, attempt_id: int | None, *, outcome: str, error: str | None = None
    ) -> None:
        """attempts 旁路轨迹行收尾的公开接缝（不经 apply_* 事务的路径使用）。

        典型消费方：派发机器的异常分支 requeue（未走 apply_retry 事务但
        本次执行已终结为重入队）。收尾失败降级告警，不阻断主流程。
        """
        self._finish_attempt(attempt_id, outcome=outcome, error=error)

    # ── 内存状态受控接缝（视图与转发）────────────────────────────────

    def mark_failed_memory(
        self, uid: str, meta: dict[str, Any], *, unregister: bool = True
    ) -> None:
        """失败终态内存登记的唯一尾段（wall/failed 互斥单点维护）。

        先经失败档案元数据规范化，再落 failed 集合——state.mark_failed
        同步摘除 wall 同名行（成功/失败互删语义）；``unregister=False``
        供批量崩溃契约路径先登记、由调用方统一处置在途集合。
        """
        normalized = self._classifier.normalize_failed_meta(meta)
        self._state.mark_failed(uid, normalized)
        if unregister:
            self._state.unregister_in_flight(uid)

    @property
    def state(self) -> PipelineState:
        """底层 PipelineState 引用（供调度器/恢复编排层直读）。"""
        return self._state

    @property
    def backend(self) -> AbstractStateBackend:
        """底层持久化后端引用。"""
        return self._backend

    @property
    def queue(self) -> list[dict[str, Any]]:
        """内存作业队列视图。"""
        return self._state.queue

    @property
    def wall(self) -> dict[str, dict[str, Any]]:
        """成功历史集合视图。"""
        return self._state.wall

    @property
    def failed(self) -> dict[str, dict[str, Any]]:
        """失败档案集合视图。"""
        return self._state.failed

    @property
    def cursors(self) -> dict[str, str]:
        """游标字典视图。"""
        return self._state.cursors

    @property
    def in_flight_uids(self) -> frozenset[str]:
        """当前在途作业 UID 集合快照。"""
        return self._state.in_flight_uids

    @property
    def wall_uids(self) -> frozenset[str]:
        """已成功作业 UID 集合快照。

        不变式：对外只交不可变快照——PipelineState 内部活索引绝不被
        调用方持有引用（误改会静默破坏 wall 去重一致性）。
        """
        return frozenset(self._state.wall_uids)

    @property
    def failed_uids(self) -> frozenset[str]:
        """已失败作业 UID 集合快照（不可变契约同 wall_uids）。"""
        return frozenset(self._state.failed_uids)

    @property
    def queue_uids(self) -> frozenset[str]:
        """排队作业 UID 集合快照（不可变契约同 wall_uids）。"""
        return frozenset(self._state.queue_uids)

    def pop_job(self, idx: int) -> dict[str, Any]:
        """弹出指定位置作业并同步 UID 索引。"""
        return self._state.pop_job(idx)

    def spawn_jobs(self, job_dicts: list[dict[str, Any]], *, front: bool = True) -> None:
        """批量入队作业并同步 UID 索引。"""
        self._state.spawn_jobs(job_dicts, front=front)

    def requeue_jobs(self, job_dicts: list[dict[str, Any]], *, front: bool = True) -> None:
        """重入队作业（崩溃恢复/重试）并同步 UID 索引。"""
        self._state.requeue_jobs(job_dicts, front=front)

    def is_known(self, uid: str) -> bool:
        """检查作业是否已在系统任一集合（wall/failed/queue/in_flight）中。"""
        return self._state.is_known(uid)

    def is_completed(self, uid: str) -> bool:
        """检查作业是否已在 wall 成功集合中。"""
        return uid in self._state.wall

    def is_failed(self, uid: str) -> bool:
        """检查作业是否已在失败档案中。"""
        return uid in self._state.failed

    @property
    def is_empty(self) -> bool:
        """队列是否为空。"""
        return self._state.is_empty

    def clear_in_flight(self) -> None:
        """清空在途集合并维护重跑豁免集合。"""
        self._state.clear_in_flight()

    def register_in_flight(self, uid: str) -> None:
        """登记在途 UID。"""
        self._state.register_in_flight(uid)

    def mark_rerun_active(self, uid: str) -> None:
        """登记重跑豁免 UID（派发期准入放行的 wall/failed 命中重跑）。"""
        self._state.mark_rerun_active(uid)

    def unregister_in_flight(self, uid: str) -> None:
        """注销在途 UID。"""
        self._state.unregister_in_flight(uid)

    def set_state(self, state: PipelineState | None) -> None:
        """重新设置内存状态（run 启动加载期使用）。"""
        self._state = state if state is not None else PipelineState({}, {}, {}, [])

    def set_backend(self, backend: AbstractStateBackend) -> None:
        """重新设置持久化后端。"""
        self._backend = backend

    # ── 事务性原子终态转移（apply_* 出口族）──────────────────────────

    def apply_success(
        self,
        uid: str,
        result_meta: dict[str, Any],
        *,
        spawned_jobs: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
        declared_inputs: Sequence[dict[str, Any]] = (),
        run_id: str | None = None,
        job_dict: dict[str, Any] | None = None,
        attempt_id: int | None = None,
    ) -> SuccessOutcome:
        """原子终态转移：成功（wall 落盘 + 清失败档案残行 + 子任务/cursor）。"""
        wall_meta = copy.deepcopy(result_meta) if result_meta else {}
        wall_meta.setdefault("last_run_at", _utc_now_iso())
        if run_id is not None:
            wall_meta.setdefault("last_run_id", run_id)
        if declared_inputs:
            deduped = {
                entry.get("path"): entry
                for entry in declared_inputs
                if isinstance(entry, dict) and entry.get("path")
            }
            if deduped:
                wall_meta["inputs"] = list(deduped.values())

        # 增量记录 run_count（防御非 int 脏数据；重跑成功从对侧终态续数）
        prev_meta = self._state.wall.get(uid) or self._state.failed.get(uid)
        prev_count = 0
        if isinstance(prev_meta, dict):
            try:
                prev_count = int(prev_meta.get("run_count", 0))
            except (TypeError, ValueError):
                prev_count = 0
        wall_meta["run_count"] = prev_count + 1

        spawned_list = list(spawned_jobs)
        committed = self._backend.commit_job_success(
            uid,
            wall_meta,
            spawned_jobs=spawned_list,
            cursor_updates=cursor_updates,
        )

        if committed:
            if spawned_list:
                self._state.spawn_jobs(spawned_list, front=True)
            if cursor_updates:
                self._state.update_cursors(cursor_updates)
            self._state.mark_success(uid, wall_meta)
            self._state.unregister_in_flight(uid)
            self._record_stat("completed", 1)
            self._finish_attempt(attempt_id, outcome="succeeded")
            spawned_uids = [uid_from_job_dict(j) for j in spawned_list]
            return SuccessOutcome(uid=uid, wall_meta=wall_meta, spawned_uids=spawned_uids)

        # commit 失败走 3-strike 收敛契约
        self.handle_commit_failure_strike(uid, "commit_job_success", job_dict, attempt_id=attempt_id)
        return SuccessOutcome(uid=uid, wall_meta=wall_meta)

    def apply_failure(
        self,
        uid: str,
        error_meta: dict[str, Any],
        *,
        job_dict: dict[str, Any] | None = None,
        count_as: str = "failed",
        cascade: bool = True,
        attempt_id: int | None = None,
    ) -> FailureOutcome:
        """原子终态转移：永久失败（进入失败档案 + wall 互删 + 级联下游）。"""
        normalized_meta = self._classifier.normalize_failed_meta(error_meta)
        job_payload = self._extract_job_payload(job_dict)
        committed = self._backend.commit_job_failure(uid, normalized_meta, job_payload)

        if committed:
            self.mark_failed_memory(uid, normalized_meta)
            self._record_stat(count_as, 1)
            self._finish_attempt(
                attempt_id,
                outcome="failed",
                error=self._attempt_error(normalized_meta),
            )
            cascaded_uids: list[str] = []
            if cascade:
                cascaded_uids = self.cascade_fail(uid)
            return FailureOutcome(uid=uid, error_meta=normalized_meta, cascaded_uids=cascaded_uids)

        self.handle_commit_failure_strike(
            uid,
            self._attempt_error(normalized_meta) or "commit_job_failure",
            job_dict,
            attempt_id=attempt_id,
        )
        return FailureOutcome(uid=uid, error_meta=normalized_meta)

    @staticmethod
    def _extract_job_payload(job_dict: dict[str, Any] | None) -> dict[str, Any] | None:
        """提取可 JSON 序列化的业务 payload 快照（失败档案落盘用）。

        不可序列化时降级为 None 并告警，绝不让快照失败把正常失败提交
        打成 commit 失败（那会误触 3-strike 崩溃契约）。
        """
        if not isinstance(job_dict, dict):
            return None
        payload = job_dict.get("payload")
        if not isinstance(payload, dict):
            return None
        try:
            dumps(payload)
        except (TypeError, ValueError):
            logger.warning(
                f"Job payload for {job_dict.get('job_id', '?')} is not "
                f"JSON-serializable; failure archive payload snapshot skipped."
            )
            return None
        return payload

    def apply_retry(
        self,
        uid: str,
        job_dict: dict[str, Any],
        retry_dict: dict[str, Any],
        *,
        transient_kind: str | None = None,
        attempt_id: int | None = None,
        error: str | None = None,
    ) -> RetryOutcome:
        """原子状态转移：重试重入队（节奏经 RequeuePolicy 唯一出口）。"""
        plan = self._requeue_policy.plan_requeue(retry_dict, transient_kind=transient_kind)
        committed = self._backend.commit_retry(uid, retry_dict, front=plan.front)
        if committed:
            self._state.unregister_in_flight(uid)
            self._state.requeue_jobs([retry_dict], front=plan.front)
            self._record_stat("retried", 1)
            if transient_kind is not None:
                stat_key = TRANSIENT_KIND_STAT_KEYS.get(transient_kind)
                if stat_key:
                    self._record_stat(stat_key, 1)
            self._finish_attempt(attempt_id, outcome="requeued", error=error)
            return RetryOutcome(uid=uid, retry_dict=retry_dict, transient_kind=transient_kind)

        self.handle_commit_failure_strike(uid, "commit_retry", job_dict, attempt_id=attempt_id)
        return RetryOutcome(uid=uid, retry_dict=retry_dict, transient_kind=transient_kind)

    def apply_skip(
        self,
        uid: str,
        *,
        job_dict: dict[str, Any] | None = None,
        attempt_id: int | None = None,
    ) -> SkipOutcome:
        """原子状态转移：去重跳过（只清队列残行，不写 wall/failed）。"""
        committed = self._backend.commit_skip(uid)
        if committed:
            self._state.unregister_in_flight(uid)
            self._record_stat("skipped", 1)
            self._finish_attempt(attempt_id, outcome="skipped")
            return SkipOutcome(uid=uid, was_known=True)

        self.handle_skip_commit_strike(uid, job_dict, attempt_id=attempt_id)
        raise AssertionError(  # pragma: no cover - 契约防御
            f"skip commit strike for {uid} unexpectedly returned normally; "
            f"contract requires raising _CommitCrashSignal."
        )

    def apply_bulk_failure(
        self,
        uids_metas: Sequence[tuple[str, dict[str, Any]]],
        *,
        remaining_queue: Sequence[dict[str, Any]] | None = None,
        reason: str = "deadlock",
    ) -> BulkFailureOutcome:
        """原子批量失败（死锁归因或批量熔断）。"""
        normalized_metas = [
            (uid, self._classifier.normalize_failed_meta(meta))
            for uid, meta in uids_metas
        ]
        committed = self._backend.commit_bulk_failure(normalized_metas)

        if committed:
            if remaining_queue is None:
                failed_set = {u for u, _ in normalized_metas}
                remaining_queue = [
                    jd for jd in self._state.queue
                    if uid_from_job_dict(jd) not in failed_set
                ]
            self._state.replace_queue(list(remaining_queue))
            self._record_stat("failed", len(normalized_metas))
            for uid, meta in normalized_metas:
                self._state.mark_failed(uid, meta)
                self._state.unregister_in_flight(uid)
                self.fire_attempt_finished(
                    uid,
                    outcome=AttemptFinish(success=False, going_to_retry=False, meta=meta),
                )
            return BulkFailureOutcome(
                failed_uids=[u for u, _ in normalized_metas],
                remaining_queue_count=len(self._state.queue),
            )

        # 批量 commit 失败：3-strike 逐条收敛
        queue = list(self._state.queue)
        kept_any = False
        for uid, meta in normalized_metas:
            matching = [j for j in queue if uid_from_job_dict(j) == uid]
            jd = matching[0] if matching else {
                "task_type": uid.split("::")[0],
                "job_id": uid.split("::")[1],
            }
            failures = self._register_commit_failure(jd)
            if failures >= self._threshold:
                strike_meta = {
                    "error": ERR_COMMIT_FAILURE,
                    "commit_failures": failures,
                    "fatal": True,
                }
                single_committed = self._backend.commit_job_failure(
                    uid, strike_meta, job_payload=self._extract_job_payload(jd)
                )
                if single_committed:
                    queue = [j for j in queue if uid_from_job_dict(j) != uid]
                    self.mark_failed_memory(uid, strike_meta)
                    self._record_stat("failed", 1)
                    self.fire_attempt_finished(
                        uid,
                        outcome=AttemptFinish(
                            success=False, going_to_retry=False, meta=strike_meta
                        ),
                    )
                else:
                    kept_any = True
            else:
                kept_any = True

        self._state.replace_queue(queue)
        if kept_any:
            raise _CommitCrashSignal(
                f"Backend commit_bulk_failure returned False for "
                f"{len(normalized_metas)} jobs. {len(queue)} kept in queue with "
                f"incremented commit failure count. Crashing to retry."
            )
        return BulkFailureOutcome(
            failed_uids=[u for u, _ in normalized_metas],
            remaining_queue_count=len(self._state.queue),
        )

    def cascade_fail(self, failed_uid: str) -> list[str]:
        """父作业失败后 O(1) 级联标记全部下游为依赖失败。"""
        cascade_uids = self._state.cascade_fail(failed_uid)
        if not cascade_uids:
            return []
        cascade_metas = [
            (uid, {"error": ERR_JOB_DEPENDENCY, "failed_dependency": failed_uid})
            for uid in cascade_uids
        ]
        committed = self._backend.commit_bulk_failure(cascade_metas)
        if committed:
            cascade_set = set(cascade_uids)
            remaining = [
                jd for jd in self._state.queue
                if uid_from_job_dict(jd) not in cascade_set
            ]
            self._state.replace_queue(remaining)
            self._record_stat("cascade_failed", len(cascade_uids))
            for uid, meta in cascade_metas:
                self.mark_failed_memory(uid, meta)
                self.fire_attempt_finished(
                    uid,
                    outcome=AttemptFinish(success=False, going_to_retry=False, meta=meta),
                )
            return cascade_uids

        # 批量 commit 失败：3-strike 逐条收敛后仍存留则崩溃
        remaining, kept = self.handle_bulk_failure_strike(
            "commit_bulk_failure", cascade_metas, list(self._state.queue)
        )
        self._state.replace_queue(remaining)
        if kept:
            raise _CommitCrashSignal(
                f"Backend commit_bulk_failure returned False for "
                f"{len(cascade_metas)} cascade job(s); {len(remaining)} kept in "
                f"queue. Crashing to retry; on-disk queue preserved."
            )
        return cascade_uids

    # ── commit 失败 3-strike 收敛契约 ─────────────────────────────────

    def _register_commit_failure(self, job_dict: dict[str, Any]) -> int:
        """3-strike 计数登记（计数递增的单一事实源）。

        计数唯一表示是 runtime 命名空间的 ``_commit_failures``（经
        JobRuntimeState 强类型往返）。
        """
        rt_state = JobRuntimeState.from_dict(job_dict.get("runtime"))
        failures = rt_state.record_commit_failure()
        job_dict["runtime"] = rt_state.to_dict()
        return failures

    def handle_commit_failure_strike(
        self,
        uid: str,
        reason: str,
        job_dict: dict[str, Any] | None = None,
        *,
        attempt_id: int | None = None,
    ) -> None:
        """单条 commit 失败的 3-strike 收敛处理（崩溃契约入口）。

        未达阈值：requeue 内存 + 上抛 _CommitCrashSignal（磁盘队列原样
        保留，崩溃重启后重试，防无限重试循环）；达阈值：按确定性坏输入
        收敛进失败档案（触发钩子）并抛 _JobTerminated；阈值熔断写入也
        失败时同样 requeue + 崩溃。
        """
        if job_dict is None:
            job_dict = {
                "task_type": uid.split("::")[0],
                "job_id": uid.split("::")[1],
            }

        failures = self._register_commit_failure(job_dict)

        if failures >= self._threshold:
            logger.critical(
                f"Backend commit failed {failures} times for {uid} ({reason}); "
                f"treating as deterministic bad input, sending to failure "
                f"archive instead of crashing."
            )
            strike_meta = {
                "error": ERR_COMMIT_FAILURE,
                "fatal": True,
                "commit_failures": failures,
            }
            strike_committed = self._backend.commit_job_failure(
                uid, strike_meta, job_payload=self._extract_job_payload(job_dict)
            )
            if strike_committed:
                self.mark_failed_memory(uid, strike_meta)
                self._record_stat("failed", 1)
                self._finish_attempt(
                    attempt_id, outcome="failed", error=ERR_COMMIT_FAILURE
                )
                self.fire_attempt_finished(
                    uid,
                    outcome=AttemptFinish(
                        success=False, going_to_retry=False, meta=strike_meta
                    ),
                )
                raise _JobTerminated(
                    f"Job {uid} permanently failed after {failures} commit attempts."
                )

        # 未达阈值或熔断写入仍失败：requeue 并上抛崩溃信号
        self._requeue_and_crash(uid, job_dict, reason, attempt_id=attempt_id)

    def handle_skip_commit_strike(
        self,
        uid: str,
        job_dict: dict[str, Any] | None = None,
        *,
        attempt_id: int | None = None,
    ) -> None:
        """commit_skip 失败时的收敛处理：只 requeue + 崩溃，绝不写失败档案。"""
        self._requeue_and_crash(uid, job_dict, "commit_skip", attempt_id=attempt_id)

    def handle_bulk_failure_strike(
        self,
        reason: str,
        uids_metas: list[tuple[str, dict[str, Any]]],
        queue_job_dicts: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], bool]:
        """批量 commit 失败的 3-strike 逐条收敛处理。

        返回（存留队列, 是否有作业仍留队）：留队者带递增的 commit 失败
        计数，达阈值的作业逐条尝试单写失败档案（成功即出队），单写也
        失败则留队待下次启动。
        """
        affected = {uid for uid, _ in uids_metas}
        meta_by_uid = dict(uids_metas)
        remaining: list[dict[str, Any]] = []
        has_kept_affected = False
        for jd in queue_job_dicts:
            uid = uid_from_job_dict(jd)
            if uid not in affected:
                remaining.append(jd)
                continue
            failures = self._register_commit_failure(jd)
            if failures >= self._threshold:
                archive_meta = meta_by_uid[uid]
                single_committed = self._backend.commit_job_failure(
                    uid, archive_meta, job_payload=self._extract_job_payload(jd)
                )
                if single_committed:
                    self.mark_failed_memory(uid, archive_meta, unregister=False)
                    self._record_stat("failed", 1)
                    self.fire_attempt_finished(
                        uid,
                        outcome=AttemptFinish(
                            success=False, going_to_retry=False, meta=archive_meta
                        ),
                    )
                    continue
                logger.critical(
                    f"Bulk 3-strike: failure archive write also failed for "
                    f"{uid} ({reason}); keeping in queue for next boot."
                )
            has_kept_affected = True
            remaining.append(jd)
        return remaining, has_kept_affected

    def _requeue_and_crash(
        self,
        uid: str,
        job_dict: dict[str, Any] | None,
        reason: str,
        *,
        attempt_id: int | None = None,
    ) -> None:
        """单一出口：所有「commit 失败 → requeue 内存 + 崩溃」路径的收敛点。"""
        self._state.unregister_in_flight(uid)
        if job_dict is not None:
            self._state.requeue_jobs([job_dict], front=True)
        self._finish_attempt(attempt_id, outcome="requeued", error=reason)
        raise _CommitCrashSignal(
            f"Backend commit returned False for {uid} ({reason}). "
            f"On-disk queue preserved; crashing to avoid unbounded retry loop."
        )


__all__ = [
    "BulkFailureOutcome",
    "COMMIT_FAILURE_THRESHOLD",
    "FailureEntry",
    "FailureOutcome",
    "RetryOutcome",
    "SkipOutcome",
    "StateStore",
    "SuccessOutcome",
]
