"""StateStore: 统一内存状态机与后端持久化事务的深模块。

内敛原子双写一致性、六集合全局互斥、3-strike 崩溃记账与依赖图级联分析。
"""

from __future__ import annotations

import copy
import datetime
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Sequence, Set, Tuple, Union

from ..backend.base import AbstractStateBackend
from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.job import Job
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
DEP_GRACE_SECONDS = 60.0
DEADLOCK_GAP_MAX_ROUNDS = 5

__all__ = [
    "BulkFailureOutcome",
    "COMMIT_FAILURE_DLQ_THRESHOLD",
    "DEADLOCK_GAP_MAX_ROUNDS",
    "DEP_GRACE_SECONDS",
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
    ) -> None:
        self._backend = backend
        self._state = state or PipelineState({}, {}, {}, [])
        self._threshold = commit_failure_dlq_threshold
        self._taxonomy = taxonomy or _DEFAULT_TAXONOMY
        self._on_job_completed = on_job_completed
        self._ctx = ctx

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

        self._handle_commit_failure(uid, sanitized_meta.get("error", "commit_job_failure"), job_dict)
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

        self._handle_commit_failure(uid, "commit_retry", job_dict)
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
        return SkipOutcome(uid=uid, was_known=False)

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
            failures = jd.get("_commit_failures", 0) + 1
            jd["_commit_failures"] = failures
            if failures >= self._threshold:
                single_committed = self._backend.commit_job_failure(
                    uid, {"error": ERR_COMMIT_FAILURE_DLQ, "commit_failures": failures, "fatal": True}
                )
                if single_committed:
                    queue = [j for j in queue if uid_from_job_dict(j) != uid]
                    self._state.mark_failed(uid, {"error": ERR_COMMIT_FAILURE_DLQ, "fatal": True})
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

        raw_rt = job_dict.get("runtime")
        if not isinstance(raw_rt, dict):
            raw_rt = {}
            job_dict["runtime"] = raw_rt

        failures = raw_rt.get("_commit_failures", job_dict.get("_commit_failures", 0)) + 1
        raw_rt["_commit_failures"] = failures
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

    # ── 3. 死锁归因与宽限分析 ────────────────────────────────────────────

    @staticmethod
    def _split_deadlock(
        queue: List[Dict[str, Any]],
        error: str,
        *,
        extract_uid: Callable[[Dict[str, Any]], str],
        include: Callable[[int, str], bool],
    ) -> Tuple[List[Tuple[str, Dict[str, Any]]], List[Dict[str, Any]]]:
        """把队列拆分为「进 DLQ 的肇事者」与「保留的剩余队列」。"""
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
        missing_indices: Sequence[int],
        *,
        ctx: Optional[Any] = None,
        scheduler: Optional[Any] = None,
        grace_seconds: float = DEP_GRACE_SECONDS,
    ) -> bool:
        """宽限：缺失依赖的 job 是否应等待而非立即 DLQ。"""
        effective_ctx = ctx or self._ctx
        missing_set = set(missing_indices)
        state = self._state
        missing_uids: Set[str] = set()
        for i in missing_indices:
            if 0 <= i < len(state.queue):
                try:
                    missing_uids.add(Job.from_dict(state.queue[i]).uid)
                except (KeyError, TypeError, ValueError):
                    pass

        episode = getattr(effective_ctx, "episode", None) if effective_ctx is not None else None
        if episode is not None:
            if episode.dep_grace_missing is not None and episode.dep_grace_missing != missing_uids:
                episode.dep_grace_deadline = None
            episode.dep_grace_missing = frozenset(missing_uids)
            if hasattr(effective_ctx, "dep_grace_seconds"):
                grace_seconds = effective_ctx.dep_grace_seconds

        for i, jd in enumerate(state.queue):
            if i in missing_set:
                continue
            try:
                if scheduler is None and effective_ctx is not None and hasattr(effective_ctx, "scheduler"):
                    scheduler = effective_ctx.scheduler
                if scheduler is not None and hasattr(scheduler, "cached_job"):
                    job = scheduler.cached_job(jd)
                else:
                    job = Job.from_dict(jd)
            except (KeyError, TypeError, ValueError):
                continue
            if all(dep in state.wall for dep in job.depends_on):
                now = time.monotonic()
                deadline = episode.dep_grace_deadline if episode is not None else None
                if deadline is None:
                    deadline = now + grace_seconds
                    if episode is not None:
                        episode.dep_grace_deadline = deadline
                    logger.warning(
                        f"DEPENDENCY GRACE: {len(missing_indices)} job(s) waiting "
                        f"on missing deps; granting {grace_seconds}s "
                        f"grace (runnable job(s) may spawn them)."
                    )
                if now < deadline:
                    time.sleep(0.5)
                    return True
                logger.error(
                    f"DEPENDENCY GRACE EXPIRED: {len(missing_indices)} job(s) "
                    f"still waiting on missing deps after "
                    f"{grace_seconds}s; treating as deadlock (DLQ)."
                )
                return False
        return False

    def _deadlock_gap_or_escalate(
        self,
        log_prefix: str,
        *,
        ctx: Optional[Any] = None,
        max_rounds: int = DEADLOCK_GAP_MAX_ROUNDS,
    ) -> bool:
        """死锁分类缺口的连续轮次升级逻辑。"""
        effective_ctx = ctx or self._ctx
        episode = getattr(effective_ctx, "episode", None) if effective_ctx is not None else None
        if effective_ctx is not None and hasattr(effective_ctx, "deadlock_gap_max_rounds"):
            max_rounds = effective_ctx.deadlock_gap_max_rounds

        current_rounds = (episode.deadlock_gap_rounds if episode is not None else 0) + 1
        if episode is not None:
            episode.deadlock_gap_rounds = current_rounds

        if current_rounds < max_rounds:
            logger.error(
                f"{log_prefix}: refusing to fail the whole queue, retrying next round "
                f"({current_rounds}/{max_rounds})."
            )
            time.sleep(0.5)
            return False
        logger.critical(
            f"{log_prefix} persisted for {max_rounds} rounds; "
            f"escalating to whole-queue DLQ ({ERR_DEADLOCK_GAP})."
        )
        return True

    def handle_deadlock(
        self,
        sched: Any,
        *,
        ctx: Optional[Any] = None,
        scheduler: Optional[Any] = None,
        dep_grace_seconds: float = DEP_GRACE_SECONDS,
        deadlock_gap_max_rounds: int = DEADLOCK_GAP_MAX_ROUNDS,
    ) -> bool:
        """处理死锁：细粒度归因 + bulk_failure + cascade。"""
        effective_ctx = ctx or self._ctx
        state = self._state
        if effective_ctx is not None:
            if hasattr(effective_ctx, "scheduler"):
                scheduler = effective_ctx.scheduler
            if hasattr(effective_ctx, "dep_grace_seconds"):
                dep_grace_seconds = effective_ctx.dep_grace_seconds
            if hasattr(effective_ctx, "deadlock_gap_max_rounds"):
                deadlock_gap_max_rounds = effective_ctx.deadlock_gap_max_rounds

        if sched.malformed_indices:
            logger.error(f"Deadlock: {len(sched.malformed_indices)} job(s) have malformed dict (unparseable).")
            root = set(sched.malformed_indices)
            uids_metas, remaining_queue = self._split_deadlock(
                list(state.queue),
                ERR_MALFORMED_JOB,
                extract_uid=uid_from_job_dict,
                include=lambda idx, uid, root=root: idx in root,
            )
        elif sched.unknown_resource_indices:
            logger.error(f"Deadlock: {len(sched.unknown_resource_indices)} job(s) reference unknown resource(s).")
            root = set(sched.unknown_resource_indices)
            uids_metas, remaining_queue = self._split_deadlock(
                list(state.queue),
                ERR_RESOURCE_DEADLOCK,
                extract_uid=lambda jd: Job.from_dict(jd).uid,
                include=lambda idx, uid, root=root: idx in root,
            )
        elif sched.missing_dependency_indices:
            if self._dependency_grace(
                sched.missing_dependency_indices,
                ctx=effective_ctx,
                scheduler=scheduler,
                grace_seconds=dep_grace_seconds,
            ):
                return False
            logger.error(f"Deadlock: {len(sched.missing_dependency_indices)} job(s) have unresolvable (missing) dependencies.")
            root = set(sched.missing_dependency_indices)
            uids_metas, remaining_queue = self._split_deadlock(
                list(state.queue),
                ERR_DEPENDENCY_DEADLOCK,
                extract_uid=lambda jd: Job.from_dict(jd).uid,
                include=lambda idx, uid, root=root: idx in root,
            )
        elif sched.impossible_resource_indices:
            logger.error(f"Deadlock: {len(sched.impossible_resource_indices)} job(s) request impossible resource amounts (exceeds capacity).")
            root = set(sched.impossible_resource_indices)
            uids_metas, remaining_queue = self._split_deadlock(
                list(state.queue),
                ERR_RESOURCE_DEADLOCK,
                extract_uid=lambda jd: Job.from_dict(jd).uid,
                include=lambda idx, uid, root=root: idx in root,
            )
        elif sched.waiting_for_dependency:
            cycle_uids = set(state.find_dependency_cycles())
            if not cycle_uids:
                escalated = self._deadlock_gap_or_escalate(
                    "Deadlock classification gap (waiting_for_dependency without cycle)",
                    ctx=effective_ctx,
                    max_rounds=deadlock_gap_max_rounds,
                )
                if not escalated:
                    return False
                uids_metas = [
                    (Job.from_dict(jd).uid,
                     {"error": ERR_DEADLOCK_GAP, "root_cause": True})
                    for jd in state.queue
                ]
                remaining_queue = []
            else:
                logger.error(
                    f"Deadlock detected: dependency cycle among {len(cycle_uids)} job(s): "
                    f"{sorted(cycle_uids)}"
                )
                uids_metas, remaining_queue = self._split_deadlock(
                    list(state.queue),
                    ERR_DEPENDENCY_DEADLOCK,
                    extract_uid=lambda jd: Job.from_dict(jd).uid,
                    include=lambda idx, uid, roots=cycle_uids: uid in roots,
                )
        else:
            escalated = self._deadlock_gap_or_escalate(
                "Deadlock: unclassifiable deadlock (no known root cause)",
                ctx=effective_ctx,
                max_rounds=deadlock_gap_max_rounds,
            )
            if not escalated:
                return False
            uids_metas = [
                (Job.from_dict(jd).uid,
                 {"error": ERR_DEADLOCK_GAP, "root_cause": True})
                for jd in state.queue
            ]
            remaining_queue = []

        committed = self._backend.commit_bulk_failure(uids_metas)
        if committed:
            episode = getattr(effective_ctx, "episode", None) if effective_ctx is not None else None
            if episode is not None:
                episode.deadlock_gap_rounds = 0
            for uid, meta in uids_metas:
                self.apply_failed(uid, meta, unregister=False)
                if effective_ctx is not None and hasattr(effective_ctx, "stats"):
                    effective_ctx.stats["failed"] += 1
                if effective_ctx is not None and hasattr(effective_ctx, "fire_job_completed"):
                    effective_ctx.fire_job_completed(uid, meta, False, False)
                elif self._on_job_completed:
                    self._on_job_completed(uid, meta, False, False)
            state.replace_queue(remaining_queue)
            return not remaining_queue

        queue, kept = self.commit_bulk_failed_crash(
            "commit_bulk_failure", uids_metas, list(state.queue)
        )
        state.replace_queue(queue)
        if kept:
            raise _CommitCrashSignal(
                f"Backend commit_bulk_failure returned False for "
                f"{len(uids_metas)} deadlock job(s); {len(queue)} kept in queue "
                f"with incremented _commit_failures (3-strike will DLQ them). "
                f"Crashing to retry; on-disk queue preserved."
            )
        return not queue

    # ── 4. 内存状态与查询代理 ────────────────────────────────────────────

    @property
    def queue(self) -> List[Dict[str, Any]]:
        return self._state.queue

    @property
    def wall(self) -> Dict[str, Dict[str, Any]]:
        return self._state.wall

    @property
    def failed(self) -> Dict[str, Dict[str, Any]]:
        return self._state.failed

    @property
    def cursors(self) -> Dict[str, str]:
        return self._state.cursors

    @property
    def queue_uids(self) -> Set[str]:
        return self._state.queue_uids

    @property
    def wall_uids(self) -> Set[str]:
        return self._state.wall_uids

    @property
    def failed_uids(self) -> Set[str]:
        return self._state.failed_uids

    @property
    def attempted_uids(self) -> Set[str]:
        return self._state.attempted_uids

    @property
    def in_flight_uids(self) -> FrozenSet[str]:
        return self._state.in_flight_uids

    @property
    def is_empty(self) -> bool:
        return self._state.is_empty

    def is_known(self, uid: str) -> bool:
        return self._state.is_known(uid)

    def find_dependency_cycles(self) -> List[str]:
        return self._state.find_dependency_cycles()

    def fail_cascade(self, failed_uid: str) -> List[str]:
        return self._state.fail_cascade(failed_uid)

    def pop_job(self, idx: int) -> Dict[str, Any]:
        return self._state.pop_job(idx)

    def spawn_jobs(self, job_dicts: List[Dict[str, Any]], front: bool = True) -> None:
        self._state.spawn_jobs(job_dicts, front=front)

    def requeue_jobs(self, job_dicts: List[Dict[str, Any]], front: bool = True) -> None:
        self._state.requeue_jobs(job_dicts, front=front)

    def register_in_flight(self, uid: str) -> None:
        self._state.register_in_flight(uid)

    def unregister_in_flight(self, uid: str) -> None:
        self._state.unregister_in_flight(uid)

    def clear_in_flight(self) -> None:
        self._state.clear_in_flight()

    def replace_queue(self, job_dicts: List[Dict[str, Any]]) -> None:
        self._state.replace_queue(job_dicts)
