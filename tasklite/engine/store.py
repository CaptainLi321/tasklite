"""v2 StateStore: 统一内存状态机与后端持久化事务的深模块。

内敛原子双写一致性、六集合全局互斥、commit 失败 3-strike 收敛记账与
依赖图级联分析。单一出口纪律：

- Job 终结事务唯一经由本模块的 apply_* 出口族收敛（成功 / 失败 / 重试 /
  跳过 / 批量失败 / 级联）——backend commit + 内存镜像 + 统计 + 级联
  在同一出口内对齐，内存永不领先磁盘；
- 失败终态内存登记唯一经由 ``mark_failed_memory``（successes/failed 互斥的
  单点维护）；
- attempt 收尾事件钩子唯一经由 ``fire_attempt_finished`` 触发（异常隔离
  + hook_errors 计数）；
- attempts 轨迹为 append-only 观测面：成功收尾与 successes 同事务提交（可信
  基线来源）；失败/重试/跳过收尾更新失败降级告警、绝不阻断主事务。
"""

from __future__ import annotations

import copy
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ..backend.base import AbstractStateBackend
from ..backend.sqlite_backend import TARGET_SCHEMA_VERSION
from ..exceptions import TaskLiteError, _CommitCrashSignal, _JobTerminated
from ..models.artifact import ArtifactDeclaration
from ..models.attempt import (
    ATTEMPT_FAILED,
    ATTEMPT_REQUEUED,
    ATTEMPT_RUNNING,
    ATTEMPT_SKIPPED,
    ATTEMPT_SUCCEEDED,
    AttemptRecord,
)
from ..models.job import Job, JobRuntimeState, inject_worker_resource
from ..models.state import PipelineState, job_uid_from_snapshot
from ..selection import Exclude
from ..utils.clock import parse_utc_iso_to_us, utc_now_iso
from ..utils.encoding import parse_job_uid
from ..utils.jsonutil import dumps, loads
from .admission import (
    ImmediateRequeuePolicy,
    RequeuePolicy,
    TriggerRegistry,
    extract_effective_job_config,
    extract_trusted_metadata_timestamp_us,
    is_trusted_success_metadata,
)
from .errorclass import ERR_COMMIT_FAILURE, ERR_JOB_DEPENDENCY, ErrorClassifier
from .types import TRANSIENT_KIND_STAT_KEYS, AttemptOutcome

logger = logging.getLogger("tasklite")

# commit 连续失败达阈值后按确定性坏输入收敛进失败档案（此前为无限崩溃
# 重启循环）；默认 3 次。
COMMIT_FAILURE_THRESHOLD = 3


@dataclass(frozen=True)
class FailureEntry:
    """失败档案只读查询的结构化不可变条目（运维查询面的载荷形状）。

    - ``job_uid``: 作业唯一标识 (task_type::job_id)
    - ``error_message``: 原始错误消息/错误码
    - ``metadata``: 完整失败档案 meta 字典视图
    - ``payload``: 原始业务 payload 快照（失败落盘无可存快照时为 None）
    """

    job_uid: str
    error_message: str
    metadata: dict[str, Any]
    payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class SuccessOutcome:
    """apply_success 原子转移结果。"""
    job_uid: str
    success_metadata: dict[str, Any]
    spawned_uids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class FailureOutcome:
    """apply_failure 原子转移结果。"""
    job_uid: str
    failure_metadata: dict[str, Any]
    cascaded_uids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RetryOutcome:
    """apply_retry 原子转移结果。"""
    job_uid: str
    retry_job_snapshot: dict[str, Any]
    transient_kind: str | None = None


@dataclass(frozen=True)
class SkipOutcome:
    """apply_skip 原子转移结果。"""
    job_uid: str
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
        trigger_registry: TriggerRegistry | None = None,
        requeue_policy: RequeuePolicy | None = None,
        base_dir: str | Path | None = None,
        protected_roots: Sequence[str | Path] = (),
    ) -> None:
        self._backend = backend
        self._state = state if state is not None else PipelineState({}, {}, {}, [])
        self._threshold = commit_failure_threshold
        self._classifier = classifier if classifier is not None else ErrorClassifier()
        self._on_attempt_finished = on_attempt_finished
        self._stats = stats
        self._trigger_registry = (
            trigger_registry
            if trigger_registry is not None
            else TriggerRegistry(base_dir=base_dir)
        )
        # 重试节奏唯一出口：核心不做任何节奏计算，默认立即重入队。
        self._requeue_policy = (
            requeue_policy if requeue_policy is not None else ImmediateRequeuePolicy()
        )
        self._exclude: Exclude | None = None
        self._temporary_excluded_uids: set[str] = set()
        self.protected_roots: tuple[Path, ...] = tuple(
            Path(root).resolve() for root in protected_roots
        )
        self.authorizer: Callable[[str, str], bool | None] | None = None

    @property
    def base_dir(self) -> Path:
        """当前实例固定的相对路径解析基准目录。"""
        return self._trigger_registry.base_dir

    # ── 统计与事件钩子 ────────────────────────────────────────────────

    def _record_stat(self, key: str, amount: int = 1) -> None:
        """更新统计指标（单一真相源）。"""
        if self._stats is not None:
            self._stats[key] = self._stats.get(key, 0) + amount

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

    def set_run_exclusions(
        self,
        exclude: Exclude | None,
        temporary_uids: set[str] | None = None,
    ) -> None:
        """内部注入面：设置本 Run 排除规则与临时排除集合。"""
        self._exclude = exclude
        self._temporary_excluded_uids = set(temporary_uids) if temporary_uids is not None else set()

    def temporarily_exclude(self, job_uid: str) -> None:
        """登记单个临时排除 UID。"""
        self._temporary_excluded_uids.add(job_uid)

    def _selection_excluded_uids(self) -> frozenset[str]:
        """当前队列中被排除拦截（Exclude 或临时排除）的合法作业 UID 集合。"""
        if self._exclude is None and not self._temporary_excluded_uids:
            return frozenset()
        excluded: set[str] = set(self._temporary_excluded_uids)
        if self._exclude is not None:
            for job_snapshot in self._state.queue:
                try:
                    job = Job.from_dict(job_snapshot)
                except (KeyError, TypeError, ValueError, TaskLiteError):
                    continue
                if self._exclude.matches_task_type(job.task_type):
                    excluded.add(job.uid)
        return frozenset(excluded)

    def fire_attempt_finished(self, job_uid: str, *, outcome: AttemptOutcome) -> None:
        """attempt 收尾事件钩子的单一出口（异常隔离 + hook_errors 计数）。

        钩子在统计更新之后触发（调用方保证），钩子内读统计一致；钩子
        抛异常只计数不外传——用户回调缺陷不得击穿事件泵。
        """
        if self._on_attempt_finished is None:
            return
        try:
            self._on_attempt_finished(job_uid, outcome=outcome)
        except Exception as e:
            logger.warning(f"on_attempt_finished hook raised for {job_uid}: {e}")
            self._record_stat("hook_errors", 1)

    def _finish_attempt(
        self, attempt_id: int | None, *, outcome: str, error_message: str | None = None
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
                finished_at=utc_now_iso(),
                error_message=error_message,
            )
        except Exception as e:
            logger.warning(
                f"Attempt trace finalization degraded for attempt {attempt_id} "
                f"(outcome={outcome}): {e}"
            )

    @staticmethod
    def _attempt_error(metadata: Mapping[str, Any]) -> str | None:
        """从失败 meta 提取轨迹行错误串（空串收敛为 None）。"""
        error_message = str(metadata.get("error_message") or "")
        return error_message or None

    # ── 游标（get_cursor / set_cursor）─────────────────────────────

    def get_cursor(self, key: str) -> str | None:
        """查询游标最新已提交值。"""
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        val = self._backend.get_cursor(key)
        if val is not None:
            return val
        return self._state.get_cursor(key)

    def set_cursor(self, key: str, value: str | None) -> None:
        """设置或删除单个游标键值（直接提交写后端并更新内存镜像）。"""
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        if value is not None and not isinstance(value, str):
            raise TypeError(f"cursor value must be str or None, got {type(value).__name__}")
        self._backend.set_cursor(key, value)
        self._state.set_cursor(key, value)

    def get_job_snapshot(self, uid: str) -> dict[str, Any] | None:
        """查找指定 uid 的完整作业快照（依次检索队列、成功档案、失败档案与 Attempt 轨迹）。"""
        for q_snap in self._state.queue:
            if job_uid_from_snapshot(q_snap) == uid:
                return copy.deepcopy(q_snap)
        for b_snap in self._backend.load_queue():
            if job_uid_from_snapshot(b_snap) == uid:
                return copy.deepcopy(b_snap)
        succ = self._backend.load_successes().get(uid)
        if isinstance(succ, dict):
            s_snap = succ.get("job_snapshot")
            if isinstance(s_snap, dict):
                return copy.deepcopy(s_snap)
        failed = self._backend.load_failed().get(uid)
        if isinstance(failed, dict):
            f_snap = failed.get("job_snapshot")
            if isinstance(f_snap, dict):
                return copy.deepcopy(f_snap)
        settled = self._backend.load_settled_attempts(uid)
        for att in reversed(settled):
            att_snap = att.get("job_snapshot")
            if isinstance(att_snap, dict):
                return copy.deepcopy(att_snap)
        payloads = self._backend.load_failure_payloads()
        if uid in payloads:
            try:
                tt, jid = parse_job_uid(uid)
                rebuilt_job = Job(task_type=tt, job_id=jid, payload=payloads[uid] or {})
                return self.normalize_and_validate_job(rebuilt_job)
            except Exception:
                pass
        return None

    def get_all_known_jobs(self) -> dict[str, dict[str, Any]]:
        """检索当前已知的全部作业完整快照映射（queue/successes/failed/attempts 并集）。"""
        known: dict[str, dict[str, Any]] = {}
        try:
            if hasattr(self._backend, "_get_conn") and hasattr(self._backend, "path") and self._backend.path.exists():
                with self._backend._get_conn() as conn:
                    cur = conn.execute("SELECT job_uid, job_snapshot FROM attempts WHERE job_snapshot IS NOT NULL")
                    for row in cur:
                        if row[0] not in known and row[1]:
                            try:
                                snap = loads(row[1])
                                if isinstance(snap, dict):
                                    known[row[0]] = snap
                            except Exception:
                                pass
            elif hasattr(self._backend, "_attempts"):
                with getattr(self._backend, "_lock"):
                    for att in getattr(self._backend, "_attempts").values():
                        snap = getattr(att, "job_snapshot", None)
                        if isinstance(att, dict):
                            snap = att.get("job_snapshot")
                        u = getattr(att, "job_uid", None) or (att.get("job_uid") if isinstance(att, dict) else None)
                        if u and isinstance(snap, dict) and u not in known:
                            known[u] = copy.deepcopy(snap)
        except Exception:
            pass
        for u, meta in self._backend.load_failed().items():
            if isinstance(meta, dict) and isinstance(meta.get("job_snapshot"), dict):
                known[u] = copy.deepcopy(meta["job_snapshot"])
            elif u not in known:
                payload = self._backend.load_failure_payloads().get(u)
                if payload is not None:
                    try:
                        tt, jid = parse_job_uid(u)
                        known[u] = self.normalize_and_validate_job(Job(task_type=tt, job_id=jid, payload=payload or {}))
                    except Exception:
                        pass
        for u, meta in self._backend.load_successes().items():
            if isinstance(meta, dict) and isinstance(meta.get("job_snapshot"), dict):
                known[u] = copy.deepcopy(meta["job_snapshot"])
        for snap in self._backend.load_queue():
            u = job_uid_from_snapshot(snap)
            known[u] = copy.deepcopy(snap)
        for snap in self._state.queue:
            u = job_uid_from_snapshot(snap)
            known[u] = copy.deepcopy(snap)
        return known

    def _find_downstream_uids(self, root_uid: str, all_known: dict[str, dict[str, Any]]) -> list[str]:
        """按已知记录的 depends_on 拓扑构建反向依赖图，遍历收集全部下游作业 UID。"""
        adjacency: dict[str, set[str]] = defaultdict(set)
        for u, snap in all_known.items():
            for dep in snap.get("depends_on") or ():
                adjacency[dep].add(u)
        downstream: list[str] = []
        visited = {root_uid}
        queue = [root_uid]
        while queue:
            curr = queue.pop(0)
            for child in sorted(adjacency.get(curr, ())):
                if child not in visited:
                    visited.add(child)
                    queue.append(child)
                    downstream.append(child)
        return downstream

    def set_job_status(
        self,
        job_or_uid: Job | str,
        status: str,
        *,
        recursive: bool,
        note: str | None = None,
    ) -> None:
        """人工设置作业状态（pending/succeeded/failed），原子处理目标及递归下游。"""
        if status not in ("pending", "succeeded", "failed"):
            raise ValueError(
                f"status must be one of 'pending', 'succeeded', 'failed', got {status!r}"
            )
        if not isinstance(recursive, bool):
            raise TypeError(f"recursive must be a bool, got {type(recursive).__name__}")
        if note is not None and not isinstance(note, str):
            raise TypeError(f"note must be a str or None, got {type(note).__name__}")

        if isinstance(job_or_uid, Job):
            target_job = job_or_uid
            target_uid = job_or_uid.uid
        elif isinstance(job_or_uid, str):
            if not job_or_uid:
                raise TypeError("job_or_uid cannot be empty")
            parse_job_uid(job_or_uid)
            target_job = None
            target_uid = job_or_uid
        else:
            raise TypeError(
                f"job_or_uid must be a Job or str, got {type(job_or_uid).__name__}"
            )

        if target_job is not None:
            intake_snap = self.normalize_and_validate_job(target_job)
            existing_snap = self.get_job_snapshot(target_uid)
            if existing_snap is not None:
                if extract_effective_job_config(existing_snap) != extract_effective_job_config(intake_snap):
                    raise ValueError(
                        f"Job configuration conflict for UID {target_uid!r}: "
                        f"passed Job does not match existing effective configuration"
                    )
                target_snap = existing_snap
            else:
                target_snap = intake_snap
        else:
            found = self.get_job_snapshot(target_uid)
            if found is None:
                raise KeyError(f"No existing snapshot found for job UID {target_uid!r}")
            target_snap = found

        all_known = self.get_all_known_jobs()
        all_known[target_uid] = target_snap

        affected: list[tuple[str, dict[str, Any]]] = [(target_uid, target_snap)]
        if recursive:
            downstream_uids = self._find_downstream_uids(target_uid, all_known)
            for child_uid in downstream_uids:
                child_snap = all_known.get(child_uid)
                if child_snap is not None:
                    affected.append((child_uid, child_snap))

        updates: list[tuple[str, str, dict[str, Any], dict[str, Any] | None]] = []
        for u, snap in affected:
            if status == "pending":
                updates.append((u, "pending", snap, None))
            elif status == "succeeded":
                annotations = {"completion_source": "manual"}
                if note is not None:
                    annotations["note"] = note
                succ_meta: dict[str, Any] = {
                    "job_snapshot": snap,
                    "annotations": annotations,
                    "settled_at": utc_now_iso(),
                    "run_id": None,
                    "attempt_no": None,
                    "started_at": None,
                    "artifacts": {},
                    "inputs": [],
                }
                updates.append((u, "succeeded", snap, succ_meta))
            elif status == "failed":
                fail_meta: dict[str, Any] = {
                    "job_snapshot": snap,
                    "annotations": {"completion_source": "manual"},
                    "error_category": "manual",
                    "error_message": note or "Manual status set to failed",
                    "settled_at": utc_now_iso(),
                    "run_id": None,
                    "attempt_no": None,
                }
                if note is not None:
                    fail_meta["note"] = note
                updates.append((u, "failed", snap, fail_meta))

        self._backend.apply_manual_status(updates)
        for u, st, snap, up_meta in updates:
            if st == "pending":
                self._state.discard_success(u)
                self._state.discard_failed(u)
                self._state.remove_queued_uid(u)
                self._state.spawn_jobs([snap], front=False)
            elif st == "succeeded":
                self._state.remove_queued_uid(u)
                self._state.mark_success(u, up_meta or {})
            elif st == "failed":
                self._state.remove_queued_uid(u)
                self._state.mark_failed(u, up_meta or {})

    def clear_history(self, uid: str, records: set[str]) -> int:
        """从 successes/failed/attempts 清除指定 job_uid 的记录，返回删除条数。"""
        total = self._backend.clear_history(uid, records)
        if "successes" in records:
            self._state.discard_success(uid)
        if "failures" in records:
            self._state.discard_failed(uid)
        return total

    # ── 摄入管道（enqueue / spawn）──────────────────────────────────

    def _require_registered_task(self, task_type: str) -> None:
        """新声明必须对应本实例已注册的 Task。"""
        tasks = self._trigger_registry.tasks
        if tasks is None or task_type not in tasks:
            raise TaskLiteError(
                f"unknown task_type {task_type!r} (register the task before "
                f"submitting its jobs)"
            )

    def normalize_and_validate_job(self, job: Job | dict[str, Any]) -> dict[str, Any]:
        """规范化非新声明作业快照（管理 API 与历史快照兼容路径）。

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
            job_snapshot = copy.deepcopy(job.to_dict())
        elif isinstance(job, dict):
            if "task_type" not in job or "job_id" not in job:
                raise ValueError("Job dict must contain 'task_type' and 'job_id'")
            payload_raw = job.get("payload")
            if payload_raw is not None and not isinstance(payload_raw, dict):
                raise TypeError(
                    f"Payload must be a dict or None, got {type(payload_raw).__name__}"
                )
            try:
                dumps(payload_raw if payload_raw is not None else {})
            except (TypeError, ValueError) as e:
                job_uid = job_uid_from_snapshot(job)
                raise ValueError(
                    f"Payload for job {job_uid} is not JSON-serializable: {e}"
                ) from e
            job_snapshot = copy.deepcopy(job)
        else:
            raise TypeError(
                f"Expected Job or dict, got {type(job).__name__}"
            )

        self.finalize_intake_snapshot(job_snapshot, str(job_snapshot["task_type"]))
        return job_snapshot

    def _normalize_new_job_snapshot(self, job: Job) -> dict[str, Any]:
        """解析并校验新声明的有效配置，要求 Task 已注册。"""
        if not isinstance(job, Job):
            raise TypeError(
                f"enqueue() expects a Job, got {type(job).__name__}"
            )
        self._require_registered_task(job.task_type)
        job_snapshot = copy.deepcopy(job.to_dict())
        self.finalize_intake_snapshot(job_snapshot, job.task_type)
        try:
            dumps(job_snapshot)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Payload for job {job.uid} is not JSON-serializable: {e}"
            ) from e
        return job_snapshot

    def normalize_spawned_job(self, job: Job) -> dict[str, Any]:
        """动态子作业的摄入规范化（成功提交路径的 spawn 侧单点）。

        与 enqueue 同规执行注册事实校验、三步收尾与整 dict 序列化预检；
        预检失败抛 ValueError，由父结算整体拒绝并转父失败。
        """
        self._require_registered_task(job.task_type)
        job_snapshot = job.to_dict()
        self.finalize_intake_snapshot(job_snapshot, job.task_type)
        try:
            dumps(job_snapshot)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Spawned job {job.uid} is not JSON-serializable: {e}"
            ) from e
        return job_snapshot

    def finalize_intake_snapshot(self, job_snapshot: dict[str, Any], task_type: str) -> None:
        """摄入三步收尾：TriggerRegistry 策略注入 → ``__workers__`` 资源注入 →
        first_enqueued_at 填充。

        first_enqueued_at 仅在缺失时填充（UTC ISO）：重试/spawn 回流的
        dict 已携带首次入队时间，绝不刷新——「首次入队时间」是端到端
        追溯链的锚点。
        """
        self._trigger_registry.normalize_job_snapshot(job_snapshot, task_type)
        inject_worker_resource(job_snapshot)
        if not job_snapshot.get("first_enqueued_at"):
            job_snapshot["first_enqueued_at"] = utc_now_iso()

    def enqueue_job(self, job: Job) -> bool:
        """单作业入队摄入管道：解析有效配置、捕获独立快照并校验同 UID 幂等/冲突。"""
        if not isinstance(job, Job):
            raise TypeError(
                f"enqueue() expects a Job, got {type(job).__name__}"
            )
        job_snapshot = self._normalize_new_job_snapshot(job)
        target_uid = job.uid

        for queued in self._backend.load_queue():
            try:
                queued_uid = job_uid_from_snapshot(queued)
            except (KeyError, TypeError, ValueError, TaskLiteError):
                continue
            if queued_uid == target_uid:
                if extract_effective_job_config(queued) == extract_effective_job_config(job_snapshot):
                    return False
                raise TaskLiteError(
                    f"Job {target_uid!r} is already queued with different effective configuration"
                )

        # 依赖引用存在性在提交时检查：只能引用已存在的 Job（队列或当前终态档案）
        if job.depends_on:
            queued_snapshots = self._backend.load_queue()
            existing_uids = {job_uid_from_snapshot(q) for q in queued_snapshots}
            existing_uids.update(self._backend.load_successes().keys())
            existing_uids.update(self._backend.load_failed().keys())
            for dep in job.depends_on:
                if dep not in existing_uids:
                    raise TaskLiteError(
                        f"Job {job.uid!r} references non-existent dependency {dep!r} in depends_on"
                    )
        inserted = self._backend.enqueue_jobs([job_snapshot])
        return bool(inserted)

    def requeue_transient(
        self,
        job_snapshot: dict[str, Any],
        *,
        transient_kind: str,
        attempt_id: int | None = None,
        error_message: str | None = None,
    ) -> None:
        """瞬态信号降级回队的单一出口（军规：零预算 + 立即重入队 + 零污染）。

        节奏唯一经 RequeuePolicy 规划（默认立即、队首插队——同轮内优先
        重扫，瞬态作业不被新入队作业排挤到队尾）；统计键走
        TRANSIENT_KIND_STAT_KEYS 登记表；不触碰 runtime——不烧 commit /
        dispatch 失败预算、不写 last_retry_error。``attempt_id`` 非 None 时
        同步把已派发轨迹行收尾为 requeued（旁路观测面单一出口内闭环）。
        """
        plan = self._requeue_policy.plan_requeue(job_snapshot, transient_kind=transient_kind)
        self._state.requeue_jobs([job_snapshot], front=plan.front)
        stat_key = TRANSIENT_KIND_STAT_KEYS.get(transient_kind)
        if stat_key:
            self._record_stat(stat_key, 1)
        self._finish_attempt(attempt_id, outcome=ATTEMPT_REQUEUED, error_message=error_message)

    def finish_attempt(
        self, attempt_id: int | None, *, outcome: str, error_message: str | None = None
    ) -> None:
        """attempts 旁路轨迹行收尾的公开接缝（不经 apply_* 事务的路径使用）。

        典型消费方：派发机器的异常分支 requeue（未走 apply_retry 事务但
        本次执行已终结为重入队）。收尾失败降级告警，不阻断主流程。
        """
        self._finish_attempt(attempt_id, outcome=outcome, error_message=error_message)

    def mark_job_running(self, job_uid: str, *, run_id: str, started_at: str) -> int:
        """记录作业进入 running 状态并推进编号（事务性单一出口）。"""
        try:
            next_no = self._backend.mark_job_running(job_uid, run_id=run_id, started_at=started_at)
            for item in self._state.queue:
                if job_uid_from_snapshot(item) == job_uid:
                    item["status"] = "running"
                    item["run_id"] = run_id
                    item["attempt_no"] = next_no
                    item["started_at"] = started_at
                    break
            return next_no
        except Exception as e:
            logger.critical(f"Failed to mark job running for {job_uid}: {e}")
            raise _CommitCrashSignal(f"Failed to mark job {job_uid} running: {e}") from e

    def record_attempt_dispatch(
        self,
        job: Job,
        *,
        execution_id: str,
        run_id: str | None,
    ) -> int | None:
        """记录作业派发尝试（写入 running 轨迹行；开行异常向外传播以阻止无行派发）。"""
        if not run_id:
            return None
        record = AttemptRecord(
            job_uid=job.uid,
            activation_no=job.activation_no,
            attempt_no=job.attempt_no,
            execution_id=execution_id,
            run_id=run_id,
            started_at=utc_now_iso(),
            outcome=ATTEMPT_RUNNING,
        )
        return self._backend.append_attempt(record)

    def record_attempt_skip(
        self,
        job: Job,
        error_message: str,
        *,
        run_id: str | None,
        dispatch_no: int,
    ) -> None:
        """Skip 终态审计（旁路 attempts 轨迹：先插 running 开行，再收尾为 skipped）。"""
        if not run_id:
            return
        try:
            now = utc_now_iso()
            execution_id = f"{run_id}.skip.{dispatch_no}"
            attempt_id = self._backend.append_attempt(
                AttemptRecord(
                    job_uid=job.uid,
                    activation_no=job.activation_no,
                    attempt_no=job.attempt_no,
                    execution_id=execution_id,
                    run_id=run_id,
                    started_at=now,
                    outcome=ATTEMPT_RUNNING,
                )
            )
            self._backend.update_attempt(
                attempt_id,
                outcome=ATTEMPT_SKIPPED,
                finished_at=now,
                error_message=error_message,
            )
        except Exception as e:
            logger.warning(
                f"Attempt trace append degraded for skipped {job.uid}: {e}"
            )


    def trusted_last_succeeded_at_us(
        self, job_uid: str, *, success_metadata: Mapping[str, Any] | None = None
    ) -> int | None:
        """可信成功基线（整数微秒）：无可信档案则返回 None（调用方保守执行）。

        不变式：基线只取自同事务提交的 attempts succeeded 行；先以 successes
        档案框架元数据（seed 的空字典、缺键或坏值一律不可信）为闸门，且校验
        successes 框架时间戳与 attempts 实际完成时间戳严格一致，防止
        clear_history 后残留的旧 attempts 成功行被拼接成伪基线。
        """
        if success_metadata is None:
            success_metadata = self._state.successes.get(job_uid)
        meta_ts = extract_trusted_metadata_timestamp_us(success_metadata)
        if meta_ts is None:
            return None
        finished_at = self._backend.load_latest_succeeded_finished_at(job_uid)
        if finished_at is None:
            return None
        try:
            attempt_ts = parse_utc_iso_to_us(finished_at)
        except (TypeError, ValueError):
            return None
        if meta_ts != attempt_ts:
            return None
        return attempt_ts

    # ── 内存状态受控接缝 ─────────────────────────────────────────────

    def mark_failed_memory(
        self, job_uid: str, metadata: dict[str, Any], *, unregister: bool = True
    ) -> None:
        """失败终态内存登记的唯一尾段（successes/failed 互斥单点维护）。

        先经失败档案元数据规范化，再落 failed 集合——state.mark_failed
        同步摘除 successes 同名行（成功/失败互删语义）；``unregister=False``
        供批量崩溃契约路径先登记、由调用方统一处置在途集合。
        """
        normalized = self._classifier.normalize_failure_metadata(metadata)
        self._state.mark_failed(job_uid, normalized)
        if unregister:
            self._state.unregister_in_flight(job_uid)

    @property
    def state(self) -> PipelineState:
        """底层 PipelineState 引用（内存状态唯一访问路径，供调度器/
        恢复编排层/机器群直读）。"""
        return self._state

    @property
    def backend(self) -> AbstractStateBackend:
        """底层持久化后端引用（所有权唯一锚点，换库经 set_backend 单点）。"""
        return self._backend

    def set_state(self, state: PipelineState | None) -> None:
        """重新设置内存状态（run 启动加载期使用）。"""
        self._state = state if state is not None else PipelineState({}, {}, {}, [])

    def set_backend(self, backend: AbstractStateBackend) -> None:
        """换库唯一锚点：backend 所有权归 StateStore 单持。

        不变式：本方法是全引擎唯一的后端重绑定点——其余组件（runtime /
        OpsConsole / 门面）一律经 ``store.backend`` 只读派生，换库一处
        动作、全部持有者必然同步。
        """
        self._backend = backend

    # ── 路径授权管理与判定 ──────────────────────────────────────────

    def is_path_protected(self, path: str | Path) -> bool:
        """检查路径是否与受保护根目录（如状态目录）重叠。"""
        try:
            raw = Path(os.fspath(path))
            target = (self.base_dir / raw).resolve() if not raw.is_absolute() else raw.resolve()
        except Exception:
            return True
        for root in self.protected_roots:
            try:
                if root.is_relative_to(target) or target.is_relative_to(root):
                    return True
            except (ValueError, TypeError):
                pass
        return False

    def is_path_authorized(self, path: str | Path) -> bool:
        """检查指定规范绝对路径是否命中已有授权。"""
        raw = Path(os.fspath(path))
        target = (self.base_dir / raw).resolve() if not raw.is_absolute() else raw.resolve()
        norm_path = os.path.normpath(str(target))
        if self._backend.is_path_authorized(norm_path):
            return True
        if str(path) != norm_path and self._backend.is_path_authorized(os.path.normpath(os.fspath(path))):
            return True
        return False

    def record_authorization(
        self,
        path: str | Path,
        target_type: str,
        *,
        authorized_at: str | None = None,
    ) -> None:
        """持久化记录一条文件系统路径授权。"""
        raw = Path(os.fspath(path))
        target = (self.base_dir / raw).resolve() if not raw.is_absolute() else raw.resolve()
        norm_path = os.path.normpath(str(target))
        self._backend.record_authorization(norm_path, target_type, authorized_at=authorized_at)

    def list_authorizations(self) -> list[dict[str, Any]]:
        """查询全部已持久化的授权记录。"""
        return self._backend.list_authorizations()

    def revoke_authorization(self, path: str | Path) -> bool:
        """按规范绝对路径精确撤销授权。"""
        raw = Path(os.fspath(path))
        target = (self.base_dir / raw).resolve() if not raw.is_absolute() else raw.resolve()
        norm_path = os.path.normpath(str(target))
        res = self._backend.revoke_authorization(norm_path)
        if not res and str(path) != norm_path:
            res = self._backend.revoke_authorization(os.path.normpath(os.fspath(path)))
        return res

    def check_or_request_authorization(
        self, path: str | Path, target_type: str | None = None
    ) -> bool | None:
        """检查已有授权或发起内部授权询问；已授权或同意持久化记录并返回 True，等待返回 None，拒绝返回 False。"""
        if self.is_path_protected(path):
            return False
        raw = Path(os.fspath(path))
        target = (self.base_dir / raw).resolve() if not raw.is_absolute() else raw.resolve()
        norm_path = os.path.normpath(str(target))
        if self._backend.is_path_authorized(norm_path):
            return True
        effective_type = (
            target_type
            if target_type is not None
            else ("directory" if os.path.isdir(norm_path) else "file")
        )
        if os.environ.get("TASKLITE_AUTO_APPROVE") == "1":
            self._backend.record_authorization(norm_path, effective_type)
            return True
        if self.authorizer is not None and callable(self.authorizer):
            res = self.authorizer(norm_path, effective_type)
            if res is True:
                self._backend.record_authorization(norm_path, effective_type)
                return True
            elif res is False:
                return False
            elif res is None:
                return None
            return False
        return False

    # ── 事务性原子终态转移（apply_* 出口族）──────────────────────────

    def apply_success(
        self,
        job_uid: str,
        result_metadata: dict[str, Any],
        *,
        spawned_job_snapshots: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
        declared_inputs: Sequence[str] = (),
        declared_artifacts: Sequence[ArtifactDeclaration] = (),
        run_id: str | None = None,
        job_snapshot: dict[str, Any] | None = None,
        attempt_no: int = 1,
        started_at: str | None = None,
        files: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        attempt_id: int | None = None,
        execution_id: str | None = None,
        finished_at: str | None = None,
    ) -> SuccessOutcome:
        """原子终态转移：成功（同事务终结对应 attempts 行 + successes 落盘 + 清失败档案残行 + 子任务/cursor）。"""
        effective_finished_at = finished_at if finished_at is not None else utc_now_iso()
        success_metadata = copy.deepcopy(result_metadata) if result_metadata else {}
        declarations = tuple(declared_artifacts)
        success_metadata["artifacts"] = {
            declaration.name: {"path": declaration.path}
            for declaration in declarations if declaration.name is not None
        }
        success_metadata["last_succeeded_at"] = effective_finished_at
        if run_id is not None:
            success_metadata["last_success_run_id"] = run_id
        else:
            success_metadata.pop("last_success_run_id", None)
        if job_snapshot is not None:
            success_metadata["job_snapshot"] = copy.deepcopy(job_snapshot)
        success_metadata.pop("inputs", None)
        if declared_inputs:
            input_paths = list(dict.fromkeys(
                path for path in declared_inputs if isinstance(path, str) and path
            ))
            if input_paths:
                success_metadata["inputs"] = input_paths

        # 增量记录 success_count（防御非 int 脏数据；重跑成功从对侧终态续数）
        prev_meta = self._state.successes.get(job_uid) or self._state.failed.get(job_uid)
        prev_count = 0
        if isinstance(prev_meta, dict):
            try:
                prev_count = int(prev_meta.get("success_count", 0))
            except (TypeError, ValueError):
                prev_count = 0
        success_metadata["success_count"] = prev_count + 1

        spawned_snapshots = list(spawned_job_snapshots)
        if self._backend.schema_version >= TARGET_SCHEMA_VERSION:
            committed = self._backend.commit_job_settlement(
                job_uid=job_uid,
                outcome="success",
                attempt_no=attempt_no,
                run_id=run_id or "run",
                settled_at=effective_finished_at,
                started_at=started_at,
                job_snapshot=job_snapshot,
                files=files,
                result=result,
                error_message=None,
                success_metadata=success_metadata,
                spawned_job_snapshots=spawned_snapshots,
                cursor_updates=cursor_updates,
            )
        else:
            committed = self._backend.commit_job_success(
                job_uid,
                success_metadata,
                attempt_id=attempt_id,
                execution_id=execution_id,
                finished_at=effective_finished_at,
                spawned_job_snapshots=spawned_snapshots,
                cursor_updates=cursor_updates,
            )

        if committed:
            if spawned_snapshots:
                self._state.spawn_jobs(spawned_snapshots, front=True)
            if cursor_updates:
                self._state.update_cursors(cursor_updates)
            self._state.mark_success(job_uid, success_metadata)
            self._state.unregister_in_flight(job_uid)
            self._record_stat("succeeded", 1)
            spawned_uids = [job_uid_from_snapshot(j) for j in spawned_snapshots]
            return SuccessOutcome(job_uid=job_uid, success_metadata=success_metadata, spawned_uids=spawned_uids)

        if self._backend.schema_version >= TARGET_SCHEMA_VERSION:
            raise _CommitCrashSignal(f"commit_job_settlement returned False for {job_uid}")
        # commit 失败走 3-strike 收敛契约（旧 schema 保留）
        self.handle_commit_failure_strike(job_uid, "commit_job_success", job_snapshot, attempt_id=attempt_id)
        return SuccessOutcome(job_uid=job_uid, success_metadata=success_metadata)

    def apply_failure(
        self,
        job_uid: str,
        failure_metadata: dict[str, Any],
        *,
        job_snapshot: dict[str, Any] | None = None,
        count_as: str = "failed",
        cascade: bool = True,
        attempt_no: int | None = None,
        run_id: str | None = None,
        started_at: str | None = None,
        settled_at: str | None = None,
        files: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        attempt_id: int | None = None,
    ) -> FailureOutcome:
        """原子终态转移：永久失败（进入失败档案 + successes 互删 + 级联下游）。"""
        normalized_meta = self._classifier.normalize_failure_metadata(failure_metadata)
        if job_snapshot is not None:
            normalized_meta["job_snapshot"] = copy.deepcopy(job_snapshot)
        payload = self._extract_job_payload(job_snapshot)
        if self._backend.schema_version >= TARGET_SCHEMA_VERSION and attempt_no is not None:
            effective_settled_at = settled_at if settled_at is not None else utc_now_iso()
            committed = self._backend.commit_job_settlement(
                job_uid=job_uid,
                outcome="failure",
                attempt_no=attempt_no,
                run_id=run_id or "run",
                settled_at=effective_settled_at,
                started_at=started_at,
                job_snapshot=job_snapshot,
                files=files,
                result=result,
                error_message=self._attempt_error(normalized_meta),
                failure_metadata=normalized_meta,
                payload=payload,
            )
        else:
            committed = self._backend.commit_job_failure(job_uid, normalized_meta, payload)

        if committed:
            self.mark_failed_memory(job_uid, normalized_meta)
            self._record_stat(count_as, 1)
            if self._backend.schema_version < TARGET_SCHEMA_VERSION:
                self._finish_attempt(
                    attempt_id,
                    outcome=ATTEMPT_FAILED,
                    error_message=self._attempt_error(normalized_meta),
                )
            cascaded_uids: list[str] = []
            if cascade:
                cascaded_uids = self.cascade_fail(job_uid)
            return FailureOutcome(job_uid=job_uid, failure_metadata=normalized_meta, cascaded_uids=cascaded_uids)

        if self._backend.schema_version >= TARGET_SCHEMA_VERSION:
            raise _CommitCrashSignal(f"Failed to commit failure for {job_uid}")
        self.handle_commit_failure_strike(
            job_uid,
            self._attempt_error(normalized_meta) or "commit_job_failure",
            job_snapshot,
            attempt_id=attempt_id,
        )
        return FailureOutcome(job_uid=job_uid, failure_metadata=normalized_meta)

    @staticmethod
    def _extract_job_payload(job_snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
        """提取可 JSON 序列化的业务 payload 快照（失败档案落盘用）。

        不可序列化时降级为 None 并告警，绝不让快照失败把正常失败提交
        打成 commit 失败（那会误触 3-strike 崩溃契约）。
        """
        if not isinstance(job_snapshot, dict):
            return None
        payload = job_snapshot.get("payload")
        if not isinstance(payload, dict):
            return None
        try:
            dumps(payload)
        except (TypeError, ValueError):
            logger.warning(
                f"Job payload for {job_snapshot.get('job_id', '?')} is not "
                f"JSON-serializable; failure archive payload snapshot skipped."
            )
            return None
        return payload

    def apply_retry(
        self,
        job_uid: str,
        job_snapshot: dict[str, Any],
        retry_job_snapshot: dict[str, Any],
        *,
        transient_kind: str | None = None,
        attempt_no: int = 1,
        run_id: str | None = None,
        started_at: str | None = None,
        files: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        error_message: str | None = None,
        attempt_id: int | None = None,
    ) -> RetryOutcome:
        """原子状态转移：重试重入队（节奏经 RequeuePolicy 唯一出口）。"""
        plan = self._requeue_policy.plan_requeue(retry_job_snapshot, transient_kind=transient_kind)
        if self._backend.schema_version >= TARGET_SCHEMA_VERSION:
            committed = self._backend.commit_job_settlement(
                job_uid=job_uid,
                outcome="retry",
                attempt_no=attempt_no,
                run_id=run_id or "run",
                settled_at=utc_now_iso(),
                started_at=started_at,
                job_snapshot=job_snapshot,
                files=files,
                result=result,
                error_message=error_message,
                requeued_job_snapshot=retry_job_snapshot,
            )
        else:
            committed = self._backend.commit_retry(job_uid, retry_job_snapshot, front=plan.front)

        if committed:
            self._state.unregister_in_flight(job_uid)
            self._state.requeue_jobs([retry_job_snapshot], front=plan.front)
            self._record_stat("retried", 1)
            if transient_kind is not None:
                stat_key = TRANSIENT_KIND_STAT_KEYS.get(transient_kind)
                if stat_key:
                    self._record_stat(stat_key, 1)
            if self._backend.schema_version < TARGET_SCHEMA_VERSION:
                self._finish_attempt(attempt_id, outcome=ATTEMPT_REQUEUED, error_message=error_message)
            return RetryOutcome(job_uid=job_uid, retry_job_snapshot=retry_job_snapshot, transient_kind=transient_kind)

        if self._backend.schema_version >= TARGET_SCHEMA_VERSION:
            raise _CommitCrashSignal(f"commit_retry returned False for {job_uid}")
        self.handle_commit_failure_strike(job_uid, "commit_retry", job_snapshot, attempt_id=attempt_id)
        return RetryOutcome(job_uid=job_uid, retry_job_snapshot=retry_job_snapshot, transient_kind=transient_kind)

    def apply_pending(
        self,
        job_uid: str,
        job_snapshot: dict[str, Any],
        *,
        attempt_no: int = 1,
        run_id: str | None = None,
        started_at: str | None = None,
        files: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> bool:
        """受阻或停机执行回滚到待派发队列（写盘重置为 pending，释放在途并还原内存队列）。"""
        settled_at = utc_now_iso()
        committed = self._backend.commit_job_settlement(
            job_uid=job_uid,
            outcome="pending",
            attempt_no=attempt_no,
            run_id=run_id or "run",
            settled_at=settled_at,
            started_at=started_at,
            job_snapshot=job_snapshot,
            files=files,
            error_message=error_message,
        )
        if committed:
            self._state.unregister_in_flight(job_uid)
            self._state.requeue_jobs([job_snapshot], front=True)
            return True
        raise _CommitCrashSignal(f"commit_job_settlement(pending) returned False for {job_uid}")

    def apply_skip(
        self,
        job_uid: str,
        *,
        job_snapshot: dict[str, Any] | None = None,
        attempt_id: int | None = None,
    ) -> SkipOutcome:
        """原子状态转移：终态跳过（清除磁盘与内存队列，不写 successes/failed）。"""
        committed = self._backend.commit_skip(job_uid)
        if committed:
            self._state.remove_queued_uid(job_uid)
            self._state.unregister_in_flight(job_uid)
            self._record_stat("skipped", 1)
            if self._backend.schema_version < TARGET_SCHEMA_VERSION:
                self._finish_attempt(attempt_id, outcome=ATTEMPT_SKIPPED)
            return SkipOutcome(job_uid=job_uid, was_known=True)

        if self._backend.schema_version >= TARGET_SCHEMA_VERSION:
            raise _CommitCrashSignal(f"commit_skip returned False for {job_uid}")
        self.handle_skip_commit_strike(job_uid, job_snapshot, attempt_id=attempt_id)
        raise AssertionError(  # pragma: no cover - 契约防御
            f"skip commit strike for {job_uid} unexpectedly returned normally; "
            f"contract requires raising _CommitCrashSignal."
        )

    def apply_bulk_failure(
        self,
        uids_metas: Sequence[tuple[str, dict[str, Any]]],
    ) -> BulkFailureOutcome:
        """原子批量失败（死锁归因或批量熔断）；剩余队列由失败集单点派生。

        执行选择激活时按队列行 task_type 过滤被排除/跳过 uid：它们绝不进入
        失败档案（过滤后为空则直接返回空结果，由运行期选择仲裁收敛）。
        """
        excluded_uids = self._selection_excluded_uids()
        if excluded_uids:
            uids_metas = [
                (job_uid, metadata) for job_uid, metadata in uids_metas if job_uid not in excluded_uids
            ]
            if not uids_metas:
                return BulkFailureOutcome(
                    failed_uids=[], remaining_queue_count=len(self._state.queue)
                )
        normalized_metas = [
            (job_uid, self._classifier.normalize_failure_metadata(metadata))
            for job_uid, metadata in uids_metas
        ]
        committed = self._backend.commit_job_failures(normalized_metas)

        if committed:
            failed_set = {u for u, _ in normalized_metas}
            remaining_queue = [
                jd for jd in self._state.queue
                if job_uid_from_snapshot(jd) not in failed_set
            ]
            self._state.replace_queue(list(remaining_queue))
            self._record_stat("failed", len(normalized_metas))
            for job_uid, metadata in normalized_metas:
                self._state.mark_failed(job_uid, metadata)
                self._state.unregister_in_flight(job_uid)
                self.fire_attempt_finished(
                    job_uid,
                    outcome=AttemptOutcome(success=False, will_retry=False, metadata=metadata),
                )
            return BulkFailureOutcome(
                failed_uids=[u for u, _ in normalized_metas],
                remaining_queue_count=len(self._state.queue),
            )

        if self._backend.schema_version >= TARGET_SCHEMA_VERSION:
            raise _CommitCrashSignal("Backend commit_job_failures returned False")

        # 批量 commit 失败：3-strike 逐条收敛（与级联路径共用单一实现）
        queue, kept_any = self.handle_bulk_failure_strike(
            "commit_job_failures", normalized_metas, list(self._state.queue)
        )
        self._state.replace_queue(queue)
        if kept_any:
            raise _CommitCrashSignal(
                f"Backend commit_job_failures returned False for "
                f"{len(normalized_metas)} jobs. {len(queue)} kept in queue with "
                f"incremented commit failure count. Crashing to retry."
            )
        return BulkFailureOutcome(
            failed_uids=[u for u, _ in normalized_metas],
            remaining_queue_count=len(self._state.queue),
        )

    def cascade_fail(self, failed_uid: str) -> list[str]:
        """父作业失败后 O(1) 级联标记全部下游为依赖失败。

        执行选择激活时，被排除作业是传播终止点：不进入级联失败集合，也不
        向其下游继续传播（下游保持待依赖留队，由后续 run 消费）。
        """
        excluded_uids = self._selection_excluded_uids()
        cascade_uids = self._state.cascade_fail(
            failed_uid, excluded_uids=excluded_uids
        )
        if excluded_uids:
            # 兜底过滤：终态写入出口绝不接受被排除 uid（与 DFS 终止点双保险）。
            cascade_uids = [job_uid for job_uid in cascade_uids if job_uid not in excluded_uids]
        if not cascade_uids:
            return []
        cascade_metas = [
            (job_uid, {"error_message": ERR_JOB_DEPENDENCY, "failed_dependency": failed_uid})
            for job_uid in cascade_uids
        ]
        committed = self._backend.commit_job_failures(cascade_metas)
        if committed:
            cascade_set = set(cascade_uids)
            remaining = [
                jd for jd in self._state.queue
                if job_uid_from_snapshot(jd) not in cascade_set
            ]
            self._state.replace_queue(remaining)
            self._record_stat("cascade_failed", len(cascade_uids))
            for job_uid, metadata in cascade_metas:
                self.mark_failed_memory(job_uid, metadata)
                self.fire_attempt_finished(
                    job_uid,
                    outcome=AttemptOutcome(success=False, will_retry=False, metadata=metadata),
                )
            return cascade_uids

        if self._backend.schema_version >= TARGET_SCHEMA_VERSION:
            raise _CommitCrashSignal(f"Backend commit_job_failures returned False for cascade of {failed_uid}")

        # 批量 commit 失败：3-strike 逐条收敛后仍存留则崩溃
        remaining, kept = self.handle_bulk_failure_strike(
            "commit_job_failures", cascade_metas, list(self._state.queue)
        )
        self._state.replace_queue(remaining)
        if kept:
            raise _CommitCrashSignal(
                f"Backend commit_job_failures returned False for "
                f"{len(cascade_metas)} cascade job(s); {len(remaining)} kept in "
                f"queue. Crashing to retry; on-disk queue preserved."
            )
        return cascade_uids

    # ── commit 失败 3-strike 收敛契约 ─────────────────────────────────

    def _register_commit_failure(self, job_snapshot: dict[str, Any]) -> int:
        """3-strike 计数登记（计数递增的单一事实源）。

        计数唯一表示是 runtime 命名空间的 ``_commit_failures``（经
        JobRuntimeState 强类型往返）。
        """
        rt_state = JobRuntimeState.from_dict(job_snapshot.get("runtime"))
        failures = rt_state.record_commit_failure()
        job_snapshot["runtime"] = rt_state.to_dict()
        return failures

    def handle_commit_failure_strike(
        self,
        job_uid: str,
        reason: str,
        job_snapshot: dict[str, Any] | None = None,
        *,
        attempt_id: int | None = None,
    ) -> None:
        """单条 commit 失败的 3-strike 收敛处理（崩溃契约入口）。

        未达阈值：requeue 内存 + 上抛 _CommitCrashSignal（磁盘队列原样
        保留，崩溃重启后重试，防无限重试循环）；达阈值：按确定性坏输入
        收敛进失败档案（触发钩子）并抛 _JobTerminated；阈值熔断写入也
        失败时同样 requeue + 崩溃。
        """
        if job_snapshot is None:
            try:
                task_type, job_id = parse_job_uid(job_uid)
            except TaskLiteError:
                task_type, _, job_id = job_uid.partition("::")
            job_snapshot = {
                "task_type": task_type,
                "job_id": job_id,
            }

        failures = self._register_commit_failure(job_snapshot)

        if failures >= self._threshold:
            logger.critical(
                f"Backend commit failed {failures} times for {job_uid} ({reason}); "
                f"treating as deterministic bad input, sending to failure "
                f"archive instead of crashing."
            )
            strike_meta = {
                "error_message": ERR_COMMIT_FAILURE,
                "is_fatal": True,
                "commit_failures": failures,
            }
            strike_committed = self._backend.commit_job_failure(
                job_uid, strike_meta, payload=self._extract_job_payload(job_snapshot)
            )
            if strike_committed:
                self.mark_failed_memory(job_uid, strike_meta)
                self._record_stat("failed", 1)
                self._finish_attempt(
                    attempt_id, outcome=ATTEMPT_FAILED, error_message=ERR_COMMIT_FAILURE
                )
                self.fire_attempt_finished(
                    job_uid,
                    outcome=AttemptOutcome(
                        success=False, will_retry=False, metadata=strike_meta
                    ),
                )
                raise _JobTerminated(
                    f"Job {job_uid} permanently failed after {failures} commit attempts."
                )

        # 未达阈值或熔断写入仍失败：requeue 并上抛崩溃信号
        self._requeue_and_crash(job_uid, job_snapshot, reason, attempt_id=attempt_id)

    def handle_skip_commit_strike(
        self,
        job_uid: str,
        job_snapshot: dict[str, Any] | None = None,
        *,
        attempt_id: int | None = None,
    ) -> None:
        """commit_skip 失败时的收敛处理：只 requeue + 崩溃，绝不写失败档案。"""
        self._requeue_and_crash(job_uid, job_snapshot, "commit_skip", attempt_id=attempt_id)

    def handle_bulk_failure_strike(
        self,
        reason: str,
        uids_metas: list[tuple[str, dict[str, Any]]],
        queue_job_snapshots: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], bool]:
        """批量 commit 失败的 3-strike 逐条收敛（批量崩溃契约唯一实现）。

        遍历归因全集而非仅队列内行——队列外 uid 以合成最小 dict 同规
        计账与熔断，绝不静默丢失记账。返回（存留队列, 是否有归因作业
        仍留队）：留队者带递增的 commit 失败计数，达阈值者经
        ``_bulk_strike_finalize`` 熔断出队，单写也失败则留队待下次启动。
        """
        queue = list(queue_job_snapshots)
        kept_any = False
        for job_uid, _ in uids_metas:
            matching = [j for j in queue if job_uid_from_snapshot(j) == job_uid]
            if matching:
                jd = matching[0]
            else:
                try:
                    task_type, job_id = parse_job_uid(job_uid)
                except TaskLiteError:
                    task_type, _, job_id = job_uid.partition("::")
                jd = {
                    "task_type": task_type,
                    "job_id": job_id,
                }
            failures = self._register_commit_failure(jd)
            if failures >= self._threshold:
                if self._bulk_strike_finalize(job_uid, jd, failures=failures):
                    queue = [j for j in queue if job_uid_from_snapshot(j) != job_uid]
                    continue
                logger.critical(
                    f"Bulk 3-strike: failure archive write also failed for "
                    f"{job_uid} ({reason}); keeping in queue for next boot."
                )
            kept_any = True
        return queue, kept_any

    def _bulk_strike_finalize(
        self,
        job_uid: str,
        job_snapshot: dict[str, Any],
        *,
        failures: int,
    ) -> bool:
        """批量 3-strike 熔断收尾单点（死锁批量与级联批量两路共用）。

        统一语义：熔断 meta 与单条 handle_commit_failure_strike 同规——
        归因收敛为 ERR_COMMIT_FAILURE strike_meta（原始归因让位于计数控
        迹）；``unregister=False``——批量契约路径的失败登记与在途注销
        分离，在途集合交由调用方统一处置。返回单写失败档案是否落盘。
        """
        strike_meta = {
            "error_message": ERR_COMMIT_FAILURE,
            "is_fatal": True,
            "commit_failures": failures,
        }
        single_committed = self._backend.commit_job_failure(
            job_uid, strike_meta, payload=self._extract_job_payload(job_snapshot)
        )
        if not single_committed:
            return False
        self.mark_failed_memory(job_uid, strike_meta, unregister=False)
        self._record_stat("failed", 1)
        self.fire_attempt_finished(
            job_uid,
            outcome=AttemptOutcome(success=False, will_retry=False, metadata=strike_meta),
        )
        return True

    def _requeue_and_crash(
        self,
        job_uid: str,
        job_snapshot: dict[str, Any] | None,
        reason: str,
        *,
        attempt_id: int | None = None,
    ) -> None:
        """单一出口：所有「commit 失败 → requeue 内存 + 崩溃」路径的收敛点。"""
        self._state.unregister_in_flight(job_uid)
        if job_snapshot is not None:
            self._state.requeue_jobs([job_snapshot], front=True)
        self._finish_attempt(attempt_id, outcome=ATTEMPT_REQUEUED, error_message=reason)
        raise _CommitCrashSignal(
            f"Backend commit returned False for {job_uid} ({reason}). "
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
