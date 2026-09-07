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

from .governor import (
    DEADLOCK_GAP_MAX_ROUNDS,
    DEP_GRACE_SECONDS,
    DeadlockGovernor,
)
from ..backend.base import AbstractStateBackend
from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.job import Job, JobRuntimeState
from ..models.state import PipelineState, uid_from_job_dict
from ..taxonomy import (
    ERR_COMMIT_FAILURE_DLQ,
    ERR_DEADLOCK_GAP,
    ERR_DEPENDENCY_DEADLOCK,
    ERR_JOB_DEPENDENCY,
    ERR_MALFORMED_JOB,
    ERR_RESOURCE_DEADLOCK,
    ErrorTaxonomy,
    _DEFAULT_TAXONOMY,
)

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
    "DEADLOCK_GAP_MAX_ROUNDS",
    "DEP_GRACE_SECONDS",
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
        ctx: Optional[Any] = None,
        governor: Optional[DeadlockGovernor] = None,
    ) -> None:
        self._backend = backend
        self._state = state or PipelineState({}, {}, {}, [])
        self._threshold = commit_failure_dlq_threshold
        self._taxonomy = taxonomy or _DEFAULT_TAXONOMY
        self._on_job_completed = on_job_completed
        self._ctx = ctx
        self._governor = governor or getattr(ctx, "governor", None) or DeadlockGovernor()

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
        """已成功作业 UID 集合快照。"""
        return self._state.wall_uids

    @property
    def failed_uids(self) -> FrozenSet[str]:
        """已失败作业 UID 集合快照。"""
        return self._state.failed_uids

    @property
    def queue_uids(self) -> FrozenSet[str]:
        """排队作业 UID 集合快照。"""
        return self._state.queue_uids

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
    ) -> RetryOutcome:
        """原子状态转移：重试重入队。"""
        committed = self._backend.commit_retry(uid, retry_dict, front=front)
        if committed:
            self._state.unregister_in_flight(uid)
            self._state.requeue_jobs([retry_dict], front=front)
            return RetryOutcome(uid=uid, retry_dict=retry_dict)

        self.commit_failed_crash(uid, "commit_retry", job_dict)
        return RetryOutcome(uid=uid, retry_dict=retry_dict)

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
            for uid, meta in sanitized_metas:
                self._state.mark_failed(uid, meta)
                self._state.unregister_in_flight(uid)
                if self._ctx is not None and hasattr(self._ctx, "stats"):
                    self._ctx.stats["failed"] += 1
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
                    if self._ctx is not None and hasattr(self._ctx, "stats"):
                        self._ctx.stats["failed"] += 1
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
                    if self._ctx is not None and hasattr(self._ctx, "stats"):
                        self._ctx.stats["failed"] += 1
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

    # ── 3. 死锁归因与宽限分析（委托 DeadlockGovernor 深模块）───────────

    @property
    def governor(self) -> DeadlockGovernor:
        """关联的死锁治理状态机深模块。"""
        return self._governor

    def _extract_deadlock_uids(self, sched: Any, field_name: str, index_name: str) -> Set[str]:
        """统一提取归因 UID 集合（向后兼容委托给 governor）。"""
        return self._governor._extract_deadlock_uids(sched, field_name, index_name, self._state)

    @staticmethod
    def _split_deadlock_by_uids(
        queue: List[Dict[str, Any]],
        target_uids: Set[str],
        error: str,
    ) -> Tuple[List[Tuple[str, Dict[str, Any]]], List[Dict[str, Any]]]:
        """把队列拆分为「进 DLQ 的肇事者」与「保留的剩余队列」（委托 DeadlockGovernor）。"""
        return DeadlockGovernor._split_deadlock_by_uids(queue, target_uids, error)

    @staticmethod
    def _split_deadlock(
        queue: List[Dict[str, Any]],
        error: str,
        *,
        extract_uid: Callable[[Dict[str, Any]], str],
        include: Callable[[int, str], bool],
    ) -> Tuple[List[Tuple[str, Dict[str, Any]]], List[Dict[str, Any]]]:
        """把队列拆分为「进 DLQ 的肇事者」与「保留的剩余队列」（向后兼容签名）。"""
        uids_metas: List[Tuple[str, Dict[str, Any]]] = []
        remaining_queue: List[Dict[str, Any]] = []
        for idx, jd in enumerate(queue):
            uid = extract_uid(jd)
            if include(idx, uid):
                uids_metas.append((uid, {"error": error, "root_cause": True}))
            else:
                remaining_queue.append(jd)
        return uids_metas, remaining_queue

    def _dependency_grace(
        self,
        missing_identifiers: Union[Sequence[int], Set[str], Sequence[str]],
        *,
        has_potential_spawners: Optional[bool] = None,
        ctx: Optional[Any] = None,
        scheduler: Optional[Any] = None,
        grace_seconds: float = DEP_GRACE_SECONDS,
    ) -> bool:
        """宽限：缺失依赖的 job 是否应等待而非立即 DLQ（委托 DeadlockGovernor）。"""
        gov = getattr(ctx, "governor", None) or self._governor
        effective_scheduler = scheduler or (getattr(ctx, "scheduler", None) if ctx is not None else getattr(self._ctx, "scheduler", None))
        return gov.check_dependency_grace(
            self._state,
            missing_identifiers,
            has_potential_spawners=has_potential_spawners,
            scheduler=effective_scheduler,
            grace_seconds=grace_seconds,
        )

    def _deadlock_gap_or_escalate(
        self,
        log_prefix: str,
        *,
        ctx: Optional[Any] = None,
        max_rounds: int = DEADLOCK_GAP_MAX_ROUNDS,
    ) -> bool:
        """死锁分类缺口的连续轮次升级逻辑（委托 DeadlockGovernor）。"""
        gov = getattr(ctx, "governor", None) or self._governor
        return gov.check_gap_or_escalate(log_prefix, max_rounds=max_rounds)

    def handle_deadlock(
        self,
        sched: Any,
        *,
        ctx: Optional[Any] = None,
        scheduler: Optional[Any] = None,
        dep_grace_seconds: float = DEP_GRACE_SECONDS,
        deadlock_gap_max_rounds: int = DEADLOCK_GAP_MAX_ROUNDS,
    ) -> bool:
        """处理死锁：细粒度归因 + bulk_failure + cascade（委托 DeadlockGovernor）。"""
        gov = getattr(ctx, "governor", None) or self._governor
        effective_scheduler = scheduler or (getattr(ctx, "scheduler", None) if ctx is not None else getattr(self._ctx, "scheduler", None))
        return gov.resolve_deadlock(
            sched,
            store=self,
            state=self._state,
            scheduler=effective_scheduler,
            ctx=ctx or self._ctx,
            dep_grace_seconds=dep_grace_seconds,
            deadlock_gap_max_rounds=deadlock_gap_max_rounds,
        )


    # ── 5. 统一状态查询与管理接缝 ────────────────────────────────────────

    def list_dlq(self) -> List[DLQEntry]:
        """只读查询 DLQ，返回结构化条目（uid / error_type / error / attempts / failed_at / meta）。"""
        failed = self._backend.load_failed()
        entries: List[DLQEntry] = []
        for uid, meta in sorted(failed.items()):
            if not isinstance(meta, dict):
                entries.append(
                    DLQEntry(
                        uid=uid,
                        error_type=self._taxonomy.classify(meta).dlq_error_type,
                        error="",
                        attempts=0,
                        failed_at=None,
                        meta={},
                    )
                )
                continue
            attempts = meta.get("_attempt", 0)
            if not isinstance(attempts, int):
                attempts = 0
            entries.append(
                DLQEntry(
                    uid=uid,
                    error_type=self._taxonomy.classify(meta).dlq_error_type,
                    error=str(meta.get("error", "")),
                    attempts=attempts,
                    failed_at=meta.get("failed_at"),
                    meta=dict(meta),
                )
            )
        return entries

    def clear_dlq(
        self,
        task_types: Optional[Sequence[str]] = None,
        *,
        keep_fatal: bool = True,
    ) -> int:
        """从 DLQ 删除匹配条目（默认保留 fatal=true 的确定性失败），返回删除数。"""
        if task_types is not None:
            if not isinstance(task_types, (list, tuple)):
                raise TypeError(
                    f"task_types must be a list/tuple of str or None, "
                    f"got {type(task_types).__name__}"
                )
            for t in task_types:
                if not isinstance(t, str) or not t:
                    raise TypeError(
                        f"task_types must contain only non-empty str, got {t!r}"
                    )
            task_types = list(task_types)
        failed = self._backend.load_failed()
        to_delete = [
            uid for uid, meta in failed.items()
            if (task_types is None or any(uid.startswith(t + "::") for t in task_types))
            and not (keep_fatal and isinstance(meta, dict) and meta.get("fatal"))
        ]
        if not to_delete:
            return 0
        deleted_count = self._backend.delete_failed(to_delete)
        for uid in to_delete:
            self._state.failed.pop(uid, None)
            self._state._failed_uids.discard(uid)
        return deleted_count

    def clear_history(
        self,
        targets: Union[str, Sequence[str]],
        *,
        where: Sequence[str] = ("wall", "failed"),
    ) -> int:
        """从 wall 和/或 DLQ 删除条目。"""
        if isinstance(targets, str):
            patterns = [targets]
        elif isinstance(targets, (list, tuple)):
            patterns = list(targets)
        else:
            raise TypeError(
                f"targets must be a str or a list/tuple of str, "
                f"got {type(targets).__name__}"
            )
        for p in patterns:
            if not isinstance(p, str):
                raise TypeError(
                    f"targets must contain only str, got {type(p).__name__} ({p!r})"
                )
        if not isinstance(where, (list, tuple)):
            raise TypeError(
                f"where must be a sequence of 'wall'/'failed', got {type(where).__name__}"
            )
        where_set = set(where)
        unknown = where_set - {"wall", "failed"}
        if unknown:
            raise ValueError(
                f"where contains unknown target(s): {sorted(unknown)!r}; "
                f"allowed: 'wall', 'failed'"
            )

        def _matches(uid: str) -> bool:
            return any(
                uid == p or (p.endswith("::") and uid.startswith(p))
                for p in patterns
            )

        total = 0
        if "wall" in where:
            wall = self._backend.load_wall()
            matched = [u for u in wall if _matches(u)]
            if matched:
                total += self._backend.delete_wall(matched)
                for u in matched:
                    self._state.wall.pop(u, None)
                    self._state._wall_uids.discard(u)
        if "failed" in where:
            failed = self._backend.load_failed()
            matched = [u for u in failed if _matches(u)]
            if matched:
                total += self._backend.delete_failed(matched)
                for u in matched:
                    self._state.failed.pop(u, None)
                    self._state._failed_uids.discard(u)
        return total

    def seed_wall(self, uids: Sequence[str]) -> int:
        """把 uid 批量写入 wall（存档迁移标记「已处理」），返回实际写入数。"""
        if not isinstance(uids, (list, tuple)):
            raise TypeError(
                f"uids must be a list/tuple of str, got {type(uids).__name__}"
            )
        for u in uids:
            if not isinstance(u, str) or u.count("::") != 1:
                raise ValueError(
                    f"seed_wall uid must be 'task_type::job_id' str with exactly "
                    f"one '::' separator, got {u!r}"
                )
            task_type, job_id = u.split("::", 1)
            if not task_type or not job_id:
                raise ValueError(
                    f"seed_wall uid must have non-empty task_type and job_id, got {u!r}"
                )
        written = self._backend.seed_wall(list(uids))
        for u in uids:
            self._state.wall[u] = {}
            self._state._wall_uids.add(u)
        return written

    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（幂等）——存档迁移/进度书签恢复。"""
        self._backend.seed_cursor(key, value)
        self._state.cursors[key] = value
