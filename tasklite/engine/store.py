"""StateStore: 统一内存状态机与后端持久化事务的深模块。

内敛原子双写一致性、六集合全局互斥、3-strike 崩溃记账与依赖图级联分析。
"""

from __future__ import annotations

import copy
import datetime
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Set, Tuple, Union

from ..backend.base import AbstractStateBackend
from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.state import PipelineState, uid_from_job_dict
from ..taxonomy import ERR_COMMIT_FAILURE_DLQ, ERR_JOB_DEPENDENCY, ErrorTaxonomy, _DEFAULT_TAXONOMY

logger = logging.getLogger("tasklite")


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
    ) -> None:
        self._backend = backend
        self._state = state or PipelineState({}, {}, {}, [])
        self._threshold = commit_failure_dlq_threshold
        self._taxonomy = taxonomy or _DEFAULT_TAXONOMY
        self._on_job_completed = on_job_completed

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

        if job_dict is not None:
            self._state.requeue_jobs([job_dict], front=True)
        self._state.unregister_in_flight(uid)
        raise _CommitCrashSignal(
            f"Backend commit_skip returned False for {uid}; requeued to front. "
            f"Crashing to retry; on-disk queue preserved."
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

    # ── 3. 内存状态与查询代理 ────────────────────────────────────────────

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
