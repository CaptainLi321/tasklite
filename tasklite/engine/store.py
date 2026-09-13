"""StateStore: 统一内存状态机与后端持久化事务的深模块。

内敛原子双写一致性、六集合全局互斥、3-strike 崩溃记账与依赖图级联分析。
"""

from __future__ import annotations

import copy
import datetime
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, NamedTuple, Optional, Sequence, Set, Tuple, Union

from .governor import DeadlockGovernor
from .policy import ExecutionPolicy
from ..backend.base import AbstractStateBackend
from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.job import Job, JobRuntimeState, inject_worker_resource
from ..models.state import PipelineState, uid_from_job_dict
from ..taxonomy import (
    ERR_COMMIT_FAILURE_DLQ,
    ERR_JOB_DEPENDENCY,
    ErrorTaxonomy,
    _DEFAULT_TAXONOMY,
)
from ..utils.jsonutil import dumps

logger = logging.getLogger("tasklite")

COMMIT_FAILURE_DLQ_THRESHOLD = 3


class DLQEntry(NamedTuple):
    """DLQ 只读查询返回的结构化不可变条目。

    - ``uid``: 作业唯一标识 (task_type::job_id)
    - ``error_type``: 结构化错误分类 (fatal / dependency / deadlock / transient_exhausted 等)
    - ``error``: 原始错误消息/错误码
    - ``attempts``: 累计写入 DLQ 的次数 (_attempt 计数)
    - ``failed_at``: 最近一次失败时间 (UTC ISO 8601 字符串或 None)
    - ``meta``: 完整 DLQ payload 字典视图
    """

    uid: str
    error_type: str
    error: str
    attempts: int
    failed_at: Optional[str]
    meta: Dict[str, Any]


__all__ = [
    "BulkFailureOutcome",
    "COMMIT_FAILURE_DLQ_THRESHOLD",
    "DLQEntry",
    "DeadlockGovernor",
    "FailureOutcome",
    "RetryOutcome",
    "SkipOutcome",
    "StateStore",
    "SuccessOutcome",
]


@dataclass(frozen=True)
class SuccessOutcome:
    """apply_success 原子转移结果。"""
    uid: str
    wall_meta: Dict[str, Any]
    spawned_uids: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class FailureOutcome:
    """apply_failure 原子转移结果。"""
    uid: str
    error_meta: Dict[str, Any]
    cascaded_uids: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class RetryOutcome:
    """apply_retry 原子转移结果。"""
    uid: str
    retry_dict: Dict[str, Any]
    is_interrupted: bool = False
    is_lock_conflict: bool = False
    is_rate_limited: bool = False


@dataclass(frozen=True)
class SkipOutcome:
    """apply_skip 原子转移结果。"""
    uid: str
    was_known: bool


@dataclass(frozen=True)
class BulkFailureOutcome:
    """apply_bulk_failure 原子转移结果。"""
    failed_uids: List[str]
    cascaded_uids: List[str] = field(default_factory=list)
    remaining_queue_count: int = 0


class StateStore:
    """统一内存状态机与后端持久化事务的深模块。"""

    def __init__(
        self,
        backend: AbstractStateBackend,
        state: Optional[PipelineState] = None,
        commit_failure_dlq_threshold: int = 3,
        taxonomy: Optional[ErrorTaxonomy] = None,
        on_job_completed: Optional[Callable[[str, Dict[str, Any], bool, bool], None]] = None,
        governor: Optional[DeadlockGovernor] = None,
        stats: Optional[Any] = None,
        policy: Optional[ExecutionPolicy] = None,
    ) -> None:
        self._backend = backend
        self._state = state or PipelineState({}, {}, {}, [])
        self._threshold = commit_failure_dlq_threshold
        self._taxonomy = taxonomy or _DEFAULT_TAXONOMY
        self._on_job_completed = on_job_completed
        self._governor = governor if governor is not None else DeadlockGovernor()
        self._stats = stats
        self._policy = policy if policy is not None else ExecutionPolicy()

    def _record_stat(self, key: str, amount: int = 1) -> None:
        """更新统计指标（单一真相源）。"""
        if self._stats is not None:
            self._stats[key] = self._stats.get(key, 0) + amount

    def record_stat(self, key: str, amount: int = 1) -> None:
        """公开统计指标记录接缝（供孤儿延迟等特殊派发事件使用）。"""
        self._record_stat(key, amount)

    @property
    def stats(self) -> Any:
        return self._stats

    def set_stats(self, stats: Any) -> None:
        self._stats = stats

    def normalize_and_validate_job(self, job: Union[Job, Dict[str, Any]]) -> Dict[str, Any]:
        """规范化并校验单个作业（JSON 可序列化预检、discovery 策略规范化、工人资源注入）。"""
        if isinstance(job, Job):
            try:
                dumps(job.payload)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"Payload for job {job.uid} is not JSON-serializable: {e}"
                ) from e
            job_dict = job.to_dict()
            task_type = job.task_type
        elif isinstance(job, dict):
            if "task_type" not in job or "job_id" not in job:
                raise ValueError("Job dict must contain 'task_type' and 'job_id'")
            payload = job.get("payload", {})
            try:
                dumps(payload)
            except (TypeError, ValueError) as e:
                uid = uid_from_job_dict(job)
                raise ValueError(
                    f"Payload for job {uid} is not JSON-serializable: {e}"
                ) from e
            job_dict = copy.deepcopy(job)
            task_type = str(job_dict["task_type"])
        else:
            raise TypeError(
                f"Expected Job or dict, got {type(job).__name__}"
            )

        self._policy.normalize_job_dict(job_dict, task_type)
        inject_worker_resource(job_dict)
        return job_dict

    def enqueue_jobs(
        self,
        jobs: Union[Job, Sequence[Job], Dict[str, Any], Sequence[Dict[str, Any]]],
        *,
        front: bool = False,
    ) -> List[str]:
        """统一作业入队摄入管道（类型校验、序列化预检、策略规范化、资源注入、单事务原子插入）。

        返回实际插入后端的作业 UID 列表（自动去重）。
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

        jobs_dicts: List[Dict[str, Any]] = []
        for j in jobs_list:
            jd = self.normalize_and_validate_job(j)
            jobs_dicts.append(jd)

        if not jobs_dicts:
            return []

        inserted = self._backend.enqueue_jobs(jobs_dicts, front=front)
        return inserted

    def mark_failed(self, uid: str, meta: Dict[str, Any]) -> None:
        """统一失败登记：清 wall 旧记录 + mark_failed。"""
        sanitized = self._taxonomy.normalize_dlq_meta(meta)
        self._state.mark_failed(uid, sanitized)

    @property
    def state(self) -> PipelineState:
        """底层 PipelineState 引用（供兼容层或调度器直读）。"""
        return self._state

    @property
    def backend(self) -> AbstractStateBackend:
        """底层持久化后端引用。"""
        return self._backend

    @property
    def queue(self) -> List[Dict[str, Any]]:
        """内存作业队列视图。"""
        return self._state.queue

    @property
    def wall(self) -> Dict[str, Dict[str, Any]]:
        """成功历史集合视图。"""
        return self._state.wall

    @property
    def failed(self) -> Dict[str, Dict[str, Any]]:
        """死信队列集合视图。"""
        return self._state.failed

    @property
    def cursors(self) -> Dict[str, str]:
        """游标字典视图。"""
        return self._state.cursors

    @property
    def in_flight_uids(self) -> FrozenSet[str]:
        """当前在途作业 UID 集合快照。"""
        return self._state.in_flight_uids

    @property
    def wall_uids(self) -> FrozenSet[str]:
        """已成功作业 UID 集合快照。

        不变式：对外只交不可变快照——PipelineState 内部活索引绝不被
        调用方持有引用（误改会静默破坏 wall 去重一致性）。
        """
        return frozenset(self._state.wall_uids)

    @property
    def failed_uids(self) -> FrozenSet[str]:
        """已失败作业 UID 集合快照（不可变契约同 wall_uids）。"""
        return frozenset(self._state.failed_uids)

    @property
    def queue_uids(self) -> FrozenSet[str]:
        """排队作业 UID 集合快照（不可变契约同 wall_uids）。"""
        return frozenset(self._state.queue_uids)

    def pop_job(self, idx: int) -> dict:
        """弹出指定位置作业并同步 UID 索引。"""
        return self._state.pop_job(idx)

    def spawn_jobs(self, job_dicts: List[Dict[str, Any]], front: bool = True) -> None:
        """批量入队作业并同步 UID 索引。"""
        self._state.spawn_jobs(job_dicts, front=front)

    def requeue_jobs(self, job_dicts: List[Dict[str, Any]], front: bool = True) -> None:
        """重入队作业（崩溃恢复/重试）并同步 UID 索引。"""
        self._state.requeue_jobs(job_dicts, front=front)

    def replace_queue(self, job_dicts: List[Dict[str, Any]]) -> None:
        """整体替换队列并重建 UID 索引。"""
        self._state.replace_queue(job_dicts)

    def is_known(self, uid: str) -> bool:
        """检查作业是否已在系统任一集合（wall/failed/queue/in_flight）中。"""
        return self._state.is_known(uid)

    def is_completed(self, uid: str) -> bool:
        """检查作业是否已在 wall 成功集合中。"""
        return uid in self._state.wall

    def is_failed(self, uid: str) -> bool:
        """检查作业是否已在 failed 失败集合中。"""
        return uid in self._state.failed

    @property
    def attempted_uids(self) -> Set[str]:
        """wall∪failed 终态 UID 集合快照。"""
        return self._state.attempted_uids

    @property
    def is_empty(self) -> bool:
        """队列是否为空。"""
        return self._state.is_empty

    def find_dependency_cycles(self) -> List[str]:
        """找出当前队列依赖图中的环成员。"""
        return self._state.find_dependency_cycles()

    def all_known_uids(self) -> Set[str]:
        """返回当前系统已知全部 UID 集合。"""
        return self._state.all_known_uids()

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

    def set_state(self, state: Optional[PipelineState]) -> None:
        """重新设置内存状态（run 启动加载期使用）。"""
        self._state = state if state is not None else PipelineState({}, {}, {}, [])

    def set_backend(self, backend: AbstractStateBackend) -> None:
        """重新设置持久化后端。"""
        self._backend = backend

    def set_on_job_completed(
        self, cb: Optional[Callable[[str, Dict[str, Any], bool, bool], None]]
    ) -> None:
        """设置终态完成事件回调。"""
        self._on_job_completed = cb

    # ── 1. 事务性原子终态转移 ──────────────────────────────────────────────

    def apply_success(
        self,
        uid: str,
        result_meta: Dict[str, Any],
        *,
        spawned_jobs: Sequence[Dict[str, Any]] = (),
        cursor_updates: Optional[Mapping[str, Optional[str]]] = None,
        declared_inputs: Sequence[Dict[str, Any]] = (),
        run_id: Optional[str] = None,
        job_dict: Optional[Dict[str, Any]] = None,
    ) -> SuccessOutcome:
        """原子终态转移：成功。"""
        wall_meta = copy.deepcopy(result_meta) if result_meta else {}
        wall_meta.setdefault("last_run_at", datetime.datetime.now(datetime.timezone.utc).isoformat())
        if run_id is not None:
            wall_meta.setdefault("last_run_id", run_id)
        if declared_inputs:
            deduped = {entry.get("path"): entry for entry in declared_inputs if isinstance(entry, dict) and entry.get("path")}
            if deduped:
                wall_meta["inputs"] = list(deduped.values())

        # 增量记录 run_count（防御非 int 脏数据）
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
            spawned_uids = [uid_from_job_dict(j) for j in spawned_list]
            return SuccessOutcome(uid=uid, wall_meta=wall_meta, spawned_uids=spawned_uids)

        # Commit 失败走 3-strike 崩溃契约
        self._handle_commit_failure(uid, "commit_job_success", job_dict)
        return SuccessOutcome(uid=uid, wall_meta=wall_meta)

    def apply_failure(
        self,
        uid: str,
        error_meta: Dict[str, Any],
        job_dict: Optional[Dict[str, Any]] = None,
        *,
        count_as: str = "failed",
        cascade: bool = True,
    ) -> FailureOutcome:
        """原子终态转移：永久失败（进入 DLQ）。"""
        sanitized_meta = self._taxonomy.normalize_dlq_meta(error_meta)
        committed = self._backend.commit_job_failure(uid, sanitized_meta)

        if committed:
            self._state.mark_failed(uid, sanitized_meta)
            self._state.unregister_in_flight(uid)
            self._record_stat(count_as, 1)
            cascaded_uids: List[str] = []
            if cascade:
                cascaded_uids = self.cascade_fail(uid)
            return FailureOutcome(uid=uid, error_meta=sanitized_meta, cascaded_uids=cascaded_uids)

        self.commit_failed_crash(uid, sanitized_meta.get("error", "commit_job_failure"), job_dict)
        return FailureOutcome(uid=uid, error_meta=sanitized_meta)

    def apply_retry(
        self,
        uid: str,
        job_dict: Dict[str, Any],
        retry_dict: Dict[str, Any],
        *,
        front: bool = False,
        is_interrupted: bool = False,
        is_lock_conflict: bool = False,
        is_rate_limited: bool = False,
    ) -> RetryOutcome:
        """原子状态转移：重试重入队。"""
        committed = self._backend.commit_retry(uid, retry_dict, front=front)
        if committed:
            self._state.unregister_in_flight(uid)
            self._state.requeue_jobs([retry_dict], front=front)
            self._record_stat("retried", 1)
            if is_interrupted:
                self._record_stat("interrupted_reruns", 1)
            elif is_lock_conflict:
                self._record_stat("deferred_orphan", 1)
            elif is_rate_limited:
                self._record_stat("rate_limited_reruns", 1)
            return RetryOutcome(
                uid=uid,
                retry_dict=retry_dict,
                is_interrupted=is_interrupted,
                is_lock_conflict=is_lock_conflict,
                is_rate_limited=is_rate_limited,
            )

        self.commit_failed_crash(uid, "commit_retry", job_dict)
        return RetryOutcome(
            uid=uid,
            retry_dict=retry_dict,
            is_interrupted=is_interrupted,
            is_lock_conflict=is_lock_conflict,
            is_rate_limited=is_rate_limited,
        )

    def _requeue_and_crash(self, uid: str, job_dict: Optional[Dict[str, Any]], reason: str) -> None:
        """单一出口：所有「commit 失败 → requeue 内存 + 崩溃」路径的收敛点。"""
        self._state.unregister_in_flight(uid)
        if job_dict is not None:
            self._state.requeue_jobs([job_dict], front=True)
        raise _CommitCrashSignal(
            f"Backend commit returned False for {uid} ({reason}). "
            f"On-disk queue preserved; crashing to avoid unbounded retry loop."
        )

    def commit_skip_crash(self, uid: str, job_dict: Optional[Dict[str, Any]] = None) -> None:
        """commit_skip 失败时的终态：只 requeue + 崩溃，绝不写 DLQ。"""
        self._requeue_and_crash(uid, job_dict, "commit_skip")

    def apply_skip(
        self,
        uid: str,
        job_dict: Optional[Dict[str, Any]] = None,
    ) -> SkipOutcome:
        """原子状态转移：去重跳过。"""
        committed = self._backend.commit_skip(uid)
        if committed:
            self._state.unregister_in_flight(uid)
            self._record_stat("skipped", 1)
            return SkipOutcome(uid=uid, was_known=True)

        self.commit_skip_crash(uid, job_dict)
        raise AssertionError(
            f"_commit_skip_crash for {uid} unexpectedly returned normally; "
            f"contract requires raising _CommitCrashSignal."
        )

    def apply_bulk_failure(
        self,
        uids_metas: Sequence[Tuple[str, Dict[str, Any]]],
        *,
        remaining_queue: Optional[Sequence[Dict[str, Any]]] = None,
        reason: str = "deadlock",
    ) -> BulkFailureOutcome:
        """原子批量失败（死锁归因或批量熔断）。"""
        sanitized_metas = [
            (uid, self._taxonomy.normalize_dlq_meta(meta))
            for uid, meta in uids_metas
        ]
        committed = self._backend.commit_bulk_failure(sanitized_metas)

        if committed:
            if remaining_queue is None:
                failed_set = {u for u, _ in sanitized_metas}
                remaining_queue = [
                    jd for jd in self._state.queue
                    if uid_from_job_dict(jd) not in failed_set
                ]
            self._state.replace_queue(list(remaining_queue))
            self._record_stat("failed", len(sanitized_metas))
            for uid, meta in sanitized_metas:
                self._state.mark_failed(uid, meta)
                self._state.unregister_in_flight(uid)
                if self._on_job_completed:
                    self._on_job_completed(uid, meta, False, False)
            return BulkFailureOutcome(
                failed_uids=[u for u, _ in sanitized_metas],
                remaining_queue_count=len(self._state.queue),
            )

        # 3-strike 批量处理
        queue = list(self._state.queue)
        kept_any = False
        for uid, meta in sanitized_metas:
            matching = [j for j in queue if uid_from_job_dict(j) == uid]
            jd = matching[0] if matching else {"task_type": uid.split("::")[0], "job_id": uid.split("::")[1]}
            rt_state = JobRuntimeState.from_dict(jd.get("runtime"))
            if "_commit_failures" in jd and not rt_state.commit_failures:
                try:
                    rt_state.commit_failures = int(jd["_commit_failures"])
                except (ValueError, TypeError):
                    pass
            failures = rt_state.record_commit_failure()
            jd["runtime"] = rt_state.to_dict()
            jd["_commit_failures"] = failures
            if failures >= self._threshold:
                single_committed = self._backend.commit_job_failure(
                    uid, {"error": ERR_COMMIT_FAILURE_DLQ, "commit_failures": failures, "fatal": True}
                )
                if single_committed:
                    queue = [j for j in queue if uid_from_job_dict(j) != uid]
                    self._state.mark_failed(uid, {"error": ERR_COMMIT_FAILURE_DLQ, "fatal": True})
                    self._record_stat("failed", 1)
                    if self._on_job_completed:
                        self._on_job_completed(uid, {"error": ERR_COMMIT_FAILURE_DLQ, "fatal": True}, False, False)
                else:
                    kept_any = True
            else:
                kept_any = True

        self._state.replace_queue(queue)
        if kept_any:
            raise _CommitCrashSignal(
                f"Backend commit_bulk_failure returned False for {len(sanitized_metas)} jobs. "
                f"{len(queue)} kept in queue with incremented _commit_failures. Crashing to retry."
            )
        return BulkFailureOutcome(
            failed_uids=[u for u, _ in sanitized_metas],
            remaining_queue_count=len(self._state.queue),
        )

    def cascade_fail(self, failed_uid: str) -> List[str]:
        """父 job 失败后 O(1) 级联标记全部下游为依赖失败。"""
        cascade_uids = self._state.fail_cascade(failed_uid)
        if not cascade_uids:
            return []
        cascade_metas = [
            (cuid, {"error": ERR_JOB_DEPENDENCY, "failed_dependency": failed_uid})
            for cuid in cascade_uids
        ]
        committed = self._backend.commit_bulk_failure(cascade_metas)
        if committed:
            cascade_set = set(cascade_uids)
            remaining = [jd for jd in self._state.queue if uid_from_job_dict(jd) not in cascade_set]
            self._state.replace_queue(remaining)
            self._record_stat("cascade_failed", len(cascade_uids))
            for cuid, cm in cascade_metas:
                sanitized_cm = self._taxonomy.normalize_dlq_meta(cm)
                self._state.mark_failed(cuid, sanitized_cm)
                if self._on_job_completed:
                    self._on_job_completed(cuid, sanitized_cm, False, False)
            return cascade_uids

        # commit_bulk_failure 失败走 3-strike 崩溃契约
        remaining, kept = self.commit_bulk_failed_crash(
            "commit_bulk_failure", cascade_metas, list(self._state.queue)
        )
        self._state.replace_queue(remaining)
        if kept:
            raise _CommitCrashSignal(
                f"Backend commit_bulk_failure returned False for "
                f"{len(cascade_metas)} cascade job(s); {len(remaining)} kept in queue. "
                f"Crashing to retry; on-disk queue preserved."
            )
        return cascade_uids

    def commit_failed_crash(self, uid: str, reason: str, job_dict: Optional[Dict[str, Any]] = None) -> None:
        """对外暴露的 3-strike commit 失败收敛处理。"""
        self._handle_commit_failure(uid, reason, job_dict)

    def commit_bulk_failed_crash(
        self, reason: str, uids_metas: List[Tuple[str, Dict[str, Any]]], queue_job_dicts: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """bulk commit 失败的 3-strike 处理。"""
        affected = {uid for uid, _ in uids_metas}
        meta_by_uid = dict(uids_metas)
        remaining: List[Dict[str, Any]] = []
        has_kept_affected = False
        for jd in queue_job_dicts:
            uid = uid_from_job_dict(jd)
            if uid not in affected:
                remaining.append(jd)
                continue
            rt = jd.setdefault("runtime", {})
            if not isinstance(rt, dict):
                rt = {}
                jd["runtime"] = rt
            failures = rt.get("_commit_failures", 0) + 1
            rt["_commit_failures"] = failures
            if failures >= self._threshold:
                dlq_meta = meta_by_uid[uid]
                single_committed = self._backend.commit_job_failure(uid, dlq_meta)
                if single_committed:
                    self.apply_failed(uid, dlq_meta, unregister=False)
                    self._record_stat("failed", 1)
                    if self._on_job_completed:
                        self._on_job_completed(uid, dlq_meta, False, False)
                    continue
                logger.critical(
                    f"Bulk 3-strike: DLQ also failed for {uid} ({reason}); "
                    f"keeping in queue for next boot."
                )
            has_kept_affected = True
            remaining.append(jd)
        return remaining, has_kept_affected

    def apply_failed(
        self, uid: str, meta: Dict[str, Any], *, unregister: bool = True
    ) -> None:
        """失败登记的内存尾段。"""
        sanitized = self._taxonomy.normalize_dlq_meta(meta)
        self._state.mark_failed(uid, sanitized)
        if unregister:
            self._state.unregister_in_flight(uid)

    # ── 2. 3-Strike 崩溃契约内部实现 ─────────────────────────────────────

    def _handle_commit_failure(
        self,
        uid: str,
        reason: str,
        job_dict: Optional[Dict[str, Any]],
    ) -> None:
        """3-strike commit 失败收敛处理。"""
        if job_dict is None:
            job_dict = {"task_type": uid.split("::")[0], "job_id": uid.split("::")[1]}

        rt_state = JobRuntimeState.from_dict(job_dict.get("runtime"))
        if "_commit_failures" in job_dict and not rt_state.commit_failures:
            try:
                rt_state.commit_failures = int(job_dict["_commit_failures"])
            except (ValueError, TypeError):
                pass
        failures = rt_state.record_commit_failure()
        job_dict["runtime"] = rt_state.to_dict()
        job_dict["_commit_failures"] = failures

        if failures >= self._threshold:
            logger.critical(
                f"Backend commit failed {failures} times for {uid} ({reason}); "
                f"treating as deterministic bad input, sending to DLQ instead of crashing."
            )
            dlq_meta = {
                "error": ERR_COMMIT_FAILURE_DLQ,
                "fatal": True,
                "commit_failures": failures,
            }
            dlq_committed = self._backend.commit_job_failure(uid, dlq_meta)
            if dlq_committed:
                self.apply_failed(uid, dlq_meta)
                self._record_stat("failed", 1)
                if self._on_job_completed:
                    self._on_job_completed(uid, dlq_meta, False, False)
                raise _JobTerminated(
                    f"Job {uid} permanently failed after {failures} commit attempts."
                )

        # 未达阈值或单条 DLQ 仍失败：requeue 并上抛崩溃信号
        self._state.unregister_in_flight(uid)
        self._state.requeue_jobs([job_dict], front=True)
        raise _CommitCrashSignal(
            f"Backend commit returned False for {uid} ({reason}); "
            f"_commit_failures={failures}/{self._threshold}. "
            f"On-disk queue preserved; crashing to avoid unbounded retry loop."
        )

    # ── 3. 死锁治理深模块引用 ──────────────────────────────────────────

    @property
    def governor(self) -> DeadlockGovernor:
        """关联的死锁治理状态机深模块。"""
        return self._governor

