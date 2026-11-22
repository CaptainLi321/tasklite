"""原子图修整事务上下文（EditTransaction，ADR-0012 §3.1）。

提供 Run 外唯一的状态图增、删、替换与纯状态流转事务：
- 内存通过轻量三表视图容器 ``_GraphTables`` 维护基准态与工作图（queue / successes / failed）
  + 待补写遗留 running 失败 Attempt；
- 仅强制维护 Schema 6 固有的 ``successes ∩ failed == ∅`` 互斥不变式；
- ``set_status`` 与 ``remove`` 零依赖 ``TaskRegistry``（空实例与 CLI 可直接使用）；
- 退出提交期单次全量攒错（防删出新孤儿、活跃/成功/触碰节点零孤儿与无环、
  ``from_output`` 契约闭合、非执行 ``succeeded`` 物理产出在场），全部通过后经
  ``GraphEditCommit`` 单写事务原子落盘。
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Collection, Mapping
import copy
from dataclasses import dataclass, field
import logging
import sys
from typing import Any, Literal

from .backend.base import GraphEditCommit, RunningAttemptRecovery
from .engine.recovery import build_unsettled_running_recovery
from .engine.store import StateStore
from .exceptions import TaskLiteError
from .models.graph import build_dependents_index, collect_downstream, find_cycle_components
from .models.job import Job
from .models.records import (
    build_manual_failure_entry,
    build_manual_success_record,
    extract_job_snapshot_from_archive,
)
from .snapshot import StateSnapshot
from .utils.encoding import parse_job_uid

logger = logging.getLogger("tasklite")

_VALID_ADD_STATUSES = frozenset({"pending", "succeeded", "failed"})
_VALID_SET_STATUSES = frozenset({"pending", "failed"})
_VALID_REMOVE_ENTRIES = frozenset({"queue", "successes", "failures"})
_STATUS_ORDER = ("pending", "running", "succeeded", "failed")


@dataclass
class _QueueSlot:
    """内存工作图队列槽位。

    ``queue_position`` 为非 ``None`` 时表示未触碰的基准行（提交时零扰动）；
    任意原语写入或更新队列时将其置为 ``None``（提交时按写入序排入队尾）。
    """

    job_uid: str
    job_snapshot: dict[str, Any]
    status: Literal["pending", "running"] = "pending"
    execution_id: str | None = None
    run_id: str | None = None
    attempt_no: int | None = None
    started_at: str | None = None
    queue_position: int | None = None


@dataclass
class _GraphTables:
    """轻量三表视图容器，统一承载 ``queue > successes > failed`` 优先级查询与投影。"""

    queue: dict[str, _QueueSlot] = field(default_factory=dict)
    successes: dict[str, dict[str, Any]] = field(default_factory=dict)
    failed: dict[str, tuple[dict[str, Any], dict[str, Any] | None]] = field(default_factory=dict)

    def all_uids(self) -> set[str]:
        return set(self.queue) | set(self.successes) | set(self.failed)

    def __contains__(self, uid: object) -> bool:
        return isinstance(uid, str) and (
            uid in self.queue or uid in self.successes or uid in self.failed
        )

    def effective_status(self, uid: str) -> Literal["pending", "running", "succeeded", "failed"] | None:
        slot = self.queue.get(uid)
        if slot is not None:
            return slot.status
        if uid in self.successes:
            return "succeeded"
        if uid in self.failed:
            return "failed"
        return None

    def effective_snapshot(self, uid: str) -> dict[str, Any] | None:
        slot = self.queue.get(uid)
        if slot is not None:
            return copy.deepcopy(slot.job_snapshot)
        if uid in self.successes:
            return extract_job_snapshot_from_archive(
                uid, self.successes[uid], fill_missing_identity=True
            )
        if uid in self.failed:
            meta, payload = self.failed[uid]
            return extract_job_snapshot_from_archive(
                uid, meta, payload=payload, fill_missing_identity=True
            )
        return None

    def count_statuses(self) -> dict[str, int]:
        counts = {s: 0 for s in _STATUS_ORDER}
        for uid in self.all_uids():
            st = self.effective_status(uid)
            if st in counts:
                counts[st] += 1
        return counts

    def to_snapshot(self, *, attempts: list[dict[str, Any]], source: str) -> StateSnapshot:
        queue_rows = [
            {
                "job_uid": slot.job_uid,
                "job": copy.deepcopy(slot.job_snapshot),
                "status": slot.status,
                "run_id": slot.run_id,
                "attempt_no": slot.attempt_no,
                "started_at": slot.started_at,
            }
            for slot in self.queue.values()
        ]
        return StateSnapshot._from_tables(
            queue_rows=queue_rows,
            successes=copy.deepcopy(self.successes),
            failed=copy.deepcopy(self.failed),
            attempts=attempts,
            source=source,
        )


@dataclass(frozen=True)
class _ValidationReport:
    """闭环体检结果：硬错误、OS 异常与只读告警。"""

    errors: tuple[str, ...]
    os_errors: tuple[str, ...]
    warnings: tuple[str, ...]


def _validate_uid(uid: Any, *, param_name: str = "uid") -> str:
    """校验 canonical 'task_type::job_id' 形态，非法一律抛 TaskLiteError。"""
    if not isinstance(uid, str) or not uid:
        raise TaskLiteError(f"{param_name} must be a non-empty str, got {uid!r}")
    try:
        parse_job_uid(uid)
    except Exception as exc:
        raise TaskLiteError(
            f"{param_name} must be a valid canonical 'task_type::job_id' str, got {uid!r}"
        ) from exc
    return uid


def _format_signed(delta: int) -> str:
    if delta > 0:
        return f"+{delta}"
    if delta < 0:
        return str(delta)
    return "+0"


def _summarize_file_entry(spec: Any) -> str:
    if not isinstance(spec, Mapping):
        return repr(spec)
    role = spec.get("role", "output")
    if spec.get("producer_uid") is not None:
        return f"from_output({spec['producer_uid']!r}, {spec.get('output_name')!r}, role={role!r})"
    return f"{spec.get('path')!r} (role={role!r})"


class EditTransaction:
    """Run 外原子图修整事务上下文管理器（由 ``TaskLite.edit()`` 创建）。"""

    def __init__(
        self,
        store: StateStore,
        *,
        dry_run: bool = True,
        note: str | None = None,
        ensure_active: Callable[[str], None] | None = None,
        set_editing: Callable[[bool], None] | None = None,
    ) -> None:
        self._store = store
        self._dry_run = dry_run
        self._note = note
        self._ensure_active = ensure_active
        self._set_editing = set_editing

        self._entered = False
        self._active = False
        self._exited = False
        self._lock_cm: Any = None

        # 基准只读三表视图与可变工作三表视图
        self._base = _GraphTables()
        self._work = _GraphTables()
        self._base_running_rows: dict[str, dict[str, Any]] = {}
        self._base_attempts: list[dict[str, Any]] = []
        self._job_sequences: dict[str, int] = {}
        self._settled_attempt_nos: dict[str, frozenset[int]] = {}
        self._recovery_attempts: dict[str, RunningAttemptRecovery] = {}

        # 变更与告警追踪（用于闭环校验与结构化 diff）
        self._touched_uids: set[str] = set()
        self._added_or_replaced_uids: set[str] = set()
        self._written_succeeded_uids: set[str] = set()
        self._partial_removed_uids: set[str] = set()
        self._origin_of: dict[str, str] = {}
        self._replaced_in_place_uids: set[str] = set()

        self._version = 0
        self._logged_warnings_version = -1
        self._before_snapshot: StateSnapshot | None = None
        self._after_cache: tuple[int, StateSnapshot] | None = None

    # ── 上下文管理器生命周期 ───────────────────────────────────────────

    def __enter__(self) -> EditTransaction:
        if self._entered or self._exited:
            raise TaskLiteError("EditTransaction cannot be entered more than once")
        if self._ensure_active is not None:
            self._ensure_active("edit")

        lock_cm = self._store.exclusive_access()
        lock_cm.__enter__()
        self._lock_cm = lock_cm
        editing_marked = False
        try:
            if self._set_editing is not None:
                self._set_editing(True)
                editing_marked = True
            snap = self._store.load_consistent_snapshot()
            successes = snap["successes"]
            failed = snap["failed"]
            overlap = successes.keys() & failed.keys()
            if overlap:
                raise TaskLiteError(
                    f"Invariant conflict: Jobs have both terminal records: {overlap}"
                )

            failure_payloads = snap["failure_payloads"]
            self._base_attempts = copy.deepcopy(snap["attempts"])
            self._job_sequences = dict(snap.get("job_sequences") or {})

            settled_map: dict[str, set[int]] = defaultdict(set)
            for att in self._base_attempts:
                att_uid = att.get("job_uid")
                att_no = att.get("attempt_no")
                if isinstance(att_uid, str) and isinstance(att_no, int):
                    settled_map[att_uid].add(att_no)
            self._settled_attempt_nos = {
                uid: frozenset(nos) for uid, nos in settled_map.items()
            }

            for row in snap["queue_jobs"]:
                uid = row["job_uid"]
                st: Literal["pending", "running"] = (
                    "running" if row.get("status") == "running" else "pending"
                )
                slot = _QueueSlot(
                    job_uid=uid,
                    job_snapshot=copy.deepcopy(row["job"]),
                    status=st,
                    execution_id=row.get("execution_id"),
                    run_id=row.get("run_id"),
                    attempt_no=row.get("attempt_no"),
                    started_at=row.get("started_at"),
                    queue_position=row.get("queue_position"),
                )
                self._base.queue[uid] = copy.deepcopy(slot)
                self._work.queue[uid] = slot
                if st == "running":
                    self._base_running_rows[uid] = copy.deepcopy(row)

            for uid, meta in successes.items():
                self._base.successes[uid] = copy.deepcopy(meta)
                self._work.successes[uid] = copy.deepcopy(meta)

            for uid, meta in failed.items():
                payload = failure_payloads.get(uid)
                self._base.failed[uid] = (copy.deepcopy(meta), copy.deepcopy(payload))
                self._work.failed[uid] = (copy.deepcopy(meta), copy.deepcopy(payload))

            self._origin_of = {uid: uid for uid in self._base.all_uids()}
            self._before_snapshot = self._base.to_snapshot(
                attempts=copy.deepcopy(self._base_attempts),
                source="edit:before",
            )
            self._entered = True
            self._active = True
            return self
        except BaseException:
            if editing_marked and self._set_editing is not None:
                self._set_editing(False)
            self._release_lock(*sys.exc_info())
            raise

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        self._active = False
        self._exited = True
        try:
            if exc_type is not None:
                return False

            report = self._validate(capture_os_errors=False)
            self._emit_warnings(report.warnings)
            if report.errors:
                summary = "\n".join(f"  - {err}" for err in report.errors)
                raise TaskLiteError(
                    f"EditTransaction validation failed ({len(report.errors)} error(s)):\n{summary}"
                )

            if not self._dry_run:
                commit = self._build_commit()
                self._store.apply_graph_edit(commit)

            # 冻结最终 after 快照（退出后永久可读）
            _ = self.after
            return False
        finally:
            try:
                if self._set_editing is not None:
                    self._set_editing(False)
            finally:
                self._release_lock(exc_type, exc_val, exc_tb)

    def _release_lock(self, exc_type: Any = None, exc_val: Any = None, exc_tb: Any = None) -> None:
        lock_cm = self._lock_cm
        if lock_cm is not None:
            self._lock_cm = None
            lock_cm.__exit__(exc_type, exc_val, exc_tb)

    def _require_entered(self) -> None:
        if not self._entered:
            raise TaskLiteError("EditTransaction must be used within a 'with' context")

    def _require_active(self, op_name: str) -> None:
        if not self._entered:
            raise TaskLiteError(
                f"Cannot call {op_name}() before entering EditTransaction context"
            )
        if not self._active:
            raise TaskLiteError(
                f"Cannot call {op_name}() after EditTransaction has exited"
            )

    def _bump_version(self) -> None:
        self._version += 1
        self._after_cache = None

    # ── 工作图遍历与遗留 running 恢复准备 ──────────────────────────────

    def _collect_downstream_uids(self, root_uid: str) -> list[str]:
        """沿当前工作图有效快照的正向 depends_on 闭包收集全部下游 uid（确定性字典序）。"""
        edges: dict[str, list[str]] = {}
        for uid in self._work.all_uids():
            snap = self._work.effective_snapshot(uid)
            if snap is None:
                continue
            edges[uid] = [dep for dep in (snap.get("depends_on") or ()) if isinstance(dep, str)]
        dependents = build_dependents_index(edges)
        return collect_downstream(root_uid, dependents)

    def _prepare_running_recovery(self, uid: str) -> RunningAttemptRecovery | None:
        """若 uid 当前处于遗留 running，预校验并构造恢复记录（不产生副作用）。"""
        slot = self._work.queue.get(uid)
        if slot is None or slot.status != "running":
            return None
        if uid in self._recovery_attempts:
            return self._recovery_attempts[uid]
        base_row = self._base_running_rows.get(uid)
        if base_row is None:
            raise TaskLiteError(
                f"Invariant conflict: running job {uid!r} has no baseline execution record"
            )
        recovery, _ = build_unsettled_running_recovery(
            base_row,
            attempt_sequence=self._job_sequences.get(uid, 0),
            settled_attempt_nos=self._settled_attempt_nos.get(uid, frozenset()),
        )
        return recovery

    # ── 6 个公开原语 ───────────────────────────────────────────────────

    def __contains__(self, uid: object) -> bool:
        """存在性查询：校验 canonical uid 语法，返回是否存在于三表工作图。"""
        self._require_entered()
        valid_uid = _validate_uid(uid)
        return valid_uid in self._work

    def add(
        self,
        job: Job,
        *,
        status: Literal["pending", "succeeded", "failed"],
    ) -> None:
        """纯新增节点至指定分区；同 uid 已存在或 task_type 未注册立即抛 TaskLiteError。"""
        self._require_active("add")
        if not isinstance(job, Job):
            raise TaskLiteError(f"add() expects a Job, got {type(job).__name__}")
        if type(status) is not str or status not in _VALID_ADD_STATUSES:
            raise TaskLiteError(
                f"status must be one of 'pending', 'succeeded', 'failed', got {status!r}"
            )
        uid = job.uid
        if uid in self._work:
            raise TaskLiteError(
                f"Job {uid!r} already exists in working graph (use replace() to update it)"
            )
        snap = self._store.normalize_new_job_snapshot(job, caller="add")

        if status == "pending":
            self._work.queue[uid] = _QueueSlot(job_uid=uid, job_snapshot=snap, status="pending")
        elif status == "succeeded":
            self._work.successes[uid] = build_manual_success_record(
                snap, path_policy=self._store.path_policy, note=self._note
            )
            self._written_succeeded_uids.add(uid)
        else:
            self._work.failed[uid] = build_manual_failure_entry(snap, note=self._note)

        self._touched_uids.add(uid)
        self._added_or_replaced_uids.add(uid)
        self._bump_version()

    def replace(
        self,
        old_uid: str,
        new_job: Job,
        *,
        status: Literal["pending", "succeeded", "failed"] | None = None,
    ) -> None:
        """演进替换单个节点（可改名、改配置；status=None 继承原有效态，遗留 running 收敛为 failed）。"""
        self._require_active("replace")
        valid_old = _validate_uid(old_uid, param_name="old_uid")
        if not isinstance(new_job, Job):
            raise TaskLiteError(f"replace() expects a Job for new_job, got {type(new_job).__name__}")
        self._replace_batch([(valid_old, new_job)], status=status, caller="replace")

    def replace_many(
        self,
        mapping: Mapping[str, Job],
        *,
        status: Literal["pending", "succeeded", "failed"] | None = None,
    ) -> None:
        """批量演进替换（先摘除全部旧键再放入新 Job，天然支持编号顺移或互换）。"""
        self._require_active("replace_many")
        if not isinstance(mapping, Mapping):
            raise TaskLiteError(
                f"replace_many() expects a Mapping[str, Job], got {type(mapping).__name__}"
            )
        if status is not None and (type(status) is not str or status not in _VALID_ADD_STATUSES):
            raise TaskLiteError(
                f"status must be one of 'pending', 'succeeded', 'failed', or None; got {status!r}"
            )
        if not mapping:
            return
        pairs: list[tuple[str, Job]] = []
        for raw_old, new_job in mapping.items():
            valid_old = _validate_uid(raw_old, param_name="old_uid")
            if not isinstance(new_job, Job):
                raise TaskLiteError(
                    f"replace_many() expects Job values, got {type(new_job).__name__} for {valid_old!r}"
                )
            pairs.append((valid_old, new_job))
        self._replace_batch(pairs, status=status, caller="replace_many")

    def _replace_batch(
        self,
        pairs: list[tuple[str, Job]],
        *,
        status: Literal["pending", "succeeded", "failed"] | None,
        caller: str = "replace",
    ) -> None:
        if status is not None and (type(status) is not str or status not in _VALID_ADD_STATUSES):
            raise TaskLiteError(
                f"status must be one of 'pending', 'succeeded', 'failed', or None; got {status!r}"
            )

        # 全量前置校验（任何错误都不留半成品变更）
        seen_new_uids: set[str] = set()
        old_uids = {old_uid for old_uid, _ in pairs}
        prepared: list[tuple[str, str, str, str, dict[str, Any], RunningAttemptRecovery | None]] = []

        # Phase 0：模拟摘除所有旧键后的剩余分区占用集合（异名替换必须清空三表以杜绝幽灵旧节点）
        sim_queue = set(self._work.queue)
        sim_succ = set(self._work.successes)
        sim_fail = set(self._work.failed)

        for old_uid, new_job in pairs:
            old_status = self._work.effective_status(old_uid)
            if old_status is None:
                raise TaskLiteError(f"Job {old_uid!r} does not exist in working graph")
            new_uid = new_job.uid
            if new_uid in seen_new_uids:
                raise TaskLiteError(
                    f"Duplicate target job UID {new_uid!r} in replace_many()"
                )
            seen_new_uids.add(new_uid)

            snap = self._store.normalize_new_job_snapshot(new_job, caller=caller)
            recovery = self._prepare_running_recovery(old_uid) if old_status == "running" else None
            target_status = (
                status
                if status is not None
                else ("failed" if old_status == "running" else old_status)
            )
            prepared.append((old_uid, new_uid, old_status, target_status, snap, recovery))

            if new_uid != old_uid:
                sim_queue.discard(old_uid)
                sim_succ.discard(old_uid)
                sim_fail.discard(old_uid)
            elif old_status in ("pending", "running"):
                sim_queue.discard(old_uid)
            elif old_status == "succeeded":
                sim_succ.discard(old_uid)
            elif old_status == "failed":
                sim_fail.discard(old_uid)

        surviving_after_vacate = sim_queue | sim_succ | sim_fail
        for old_uid, new_uid, _old_status, _target_status, _snap, _recovery in prepared:
            if new_uid != old_uid and new_uid in surviving_after_vacate:
                raise TaskLiteError(
                    f"Replacement target UID {new_uid!r} conflicts with existing job in working graph"
                )
            if new_uid not in old_uids and new_uid in self._work:
                raise TaskLiteError(
                    f"Replacement target UID {new_uid!r} conflicts with existing job in working graph"
                )

        # Phase A：摘除全部旧键（异名替换清三表确保 old_uid 彻底消失；同名替换仅摘除有效态分区）
        orig_for_old: dict[str, str | None] = {}
        for old_uid, new_uid, prev_status, _target_status, _snap, recovery in prepared:
            if recovery is not None and old_uid not in self._recovery_attempts:
                self._recovery_attempts[old_uid] = recovery
            if old_uid != new_uid:
                self._work.queue.pop(old_uid, None)
                self._work.successes.pop(old_uid, None)
                self._work.failed.pop(old_uid, None)
                self._written_succeeded_uids.discard(old_uid)
                self._replaced_in_place_uids.discard(old_uid)
                self._added_or_replaced_uids.discard(old_uid)
                self._partial_removed_uids.discard(old_uid)
            elif prev_status in ("pending", "running"):
                self._work.queue.pop(old_uid, None)
            elif prev_status == "succeeded":
                self._work.successes.pop(old_uid, None)
                self._written_succeeded_uids.discard(old_uid)
            elif prev_status == "failed":
                self._work.failed.pop(old_uid, None)

            if old_uid in self._origin_of:
                if old_uid not in self._work or old_uid == new_uid:
                    orig_for_old[old_uid] = self._origin_of.pop(old_uid)
                else:
                    orig_for_old[old_uid] = self._origin_of[old_uid]
            else:
                orig_for_old[old_uid] = None

        # Phase B：将全部新 Job 写入目标分区（显式 status='pending' 同步清空历史终态档案）
        for old_uid, new_uid, _old_status, next_status, snap, _recovery in prepared:
            if next_status == "pending":
                if status is not None:
                    self._work.successes.pop(new_uid, None)
                    self._work.failed.pop(new_uid, None)
                    self._written_succeeded_uids.discard(new_uid)
                self._work.queue.pop(new_uid, None)
                self._work.queue[new_uid] = _QueueSlot(
                    job_uid=new_uid, job_snapshot=snap, status="pending"
                )
            elif next_status == "succeeded":
                self._work.failed.pop(new_uid, None)
                self._work.successes[new_uid] = build_manual_success_record(
                    snap, path_policy=self._store.path_policy, note=self._note
                )
                self._written_succeeded_uids.add(new_uid)
            else:
                self._work.successes.pop(new_uid, None)
                self._written_succeeded_uids.discard(new_uid)
                self._work.failed[new_uid] = build_manual_failure_entry(snap, note=self._note)

            orig = orig_for_old.get(old_uid)
            if orig is not None:
                self._origin_of[new_uid] = orig
                if new_uid == orig:
                    self._replaced_in_place_uids.add(new_uid)
            self._touched_uids.add(old_uid)
            self._touched_uids.add(new_uid)
            self._added_or_replaced_uids.add(new_uid)

        self._bump_version()

    def set_status(
        self,
        uid: str,
        status: Literal["pending", "failed"],
        *,
        recursive: bool,
    ) -> None:
        """纯状态流转（仅 pending/failed，零依赖 TaskRegistry，命中遗留 running 自动补写失败 Attempt）。"""
        self._require_active("set_status")
        valid_uid = _validate_uid(uid)
        if type(status) is not str or status not in _VALID_SET_STATUSES:
            raise TaskLiteError(
                f"set_status() status must be 'pending' or 'failed', got {status!r}"
            )
        if type(recursive) is not bool:
            raise TaskLiteError(f"recursive must be a bool, got {type(recursive).__name__}")
        if valid_uid not in self._work:
            raise TaskLiteError(f"Job {valid_uid!r} does not exist in working graph")

        targets = [valid_uid]
        if recursive:
            targets.extend(self._collect_downstream_uids(valid_uid))

        # 前置校验全部待恢复的遗留 running
        prepared: list[tuple[str, dict[str, Any], RunningAttemptRecovery | None]] = []
        for t_uid in targets:
            snap = self._work.effective_snapshot(t_uid)
            if snap is None:
                continue
            recovery = self._prepare_running_recovery(t_uid)
            prepared.append((t_uid, snap, recovery))

        for t_uid, snap, recovery in prepared:
            if recovery is not None and t_uid not in self._recovery_attempts:
                self._recovery_attempts[t_uid] = recovery
            if status == "pending":
                self._work.successes.pop(t_uid, None)
                self._written_succeeded_uids.discard(t_uid)
                self._work.failed.pop(t_uid, None)
                self._work.queue.pop(t_uid, None)
                self._work.queue[t_uid] = _QueueSlot(
                    job_uid=t_uid, job_snapshot=snap, status="pending"
                )
            else:
                self._work.queue.pop(t_uid, None)
                self._work.successes.pop(t_uid, None)
                self._written_succeeded_uids.discard(t_uid)
                self._work.failed[t_uid] = build_manual_failure_entry(snap, note=self._note)
            self._touched_uids.add(t_uid)

        self._bump_version()

    def remove(
        self,
        uid: str,
        *,
        entries: Collection[Literal["queue", "successes", "failures"]] = (
            "queue",
            "successes",
            "failures",
        ),
        recursive: bool,
    ) -> int:
        """按字面从指定分区移除条目（零依赖 TaskRegistry，绝不删 attempts/磁盘文件或重置序列）。"""
        self._require_active("remove")
        valid_uid = _validate_uid(uid)
        if type(recursive) is not bool:
            raise TaskLiteError(f"recursive must be a bool, got {type(recursive).__name__}")
        if not isinstance(entries, Collection) or isinstance(entries, (str, bytes)):
            raise TaskLiteError(
                "entries must be a collection of 'queue', 'successes', 'failures', "
                f"got {type(entries).__name__}"
            )
        entry_set = set(entries)
        if not entry_set:
            raise TaskLiteError("entries must not be empty")
        unknown = entry_set - _VALID_REMOVE_ENTRIES
        if unknown:
            raise TaskLiteError(
                f"entries contains unknown or forbidden target(s): {sorted(unknown)!r}; "
                f"allowed: {sorted(_VALID_REMOVE_ENTRIES)!r}"
            )

        targets = [valid_uid]
        if recursive and valid_uid in self._work:
            targets.extend(self._collect_downstream_uids(valid_uid))

        # 前置校验若删 queue 时命中的遗留 running
        recoveries: dict[str, RunningAttemptRecovery] = {}
        if "queue" in entry_set:
            for t_uid in targets:
                rec = self._prepare_running_recovery(t_uid)
                if rec is not None:
                    recoveries[t_uid] = rec

        removed_count = 0
        is_partial = entry_set != _VALID_REMOVE_ENTRIES
        for t_uid in targets:
            removed_any = False
            if "queue" in entry_set and t_uid in self._work.queue:
                if t_uid in recoveries and t_uid not in self._recovery_attempts:
                    self._recovery_attempts[t_uid] = recoveries[t_uid]
                del self._work.queue[t_uid]
                removed_any = True
            if "successes" in entry_set and t_uid in self._work.successes:
                del self._work.successes[t_uid]
                self._written_succeeded_uids.discard(t_uid)
                removed_any = True
            if "failures" in entry_set and t_uid in self._work.failed:
                del self._work.failed[t_uid]
                removed_any = True

            if removed_any:
                removed_count += 1
                self._touched_uids.add(t_uid)
                if t_uid not in self._work:
                    self._origin_of.pop(t_uid, None)
                    self._replaced_in_place_uids.discard(t_uid)
                    self._added_or_replaced_uids.discard(t_uid)
                elif is_partial:
                    self._partial_removed_uids.add(t_uid)

        if removed_count > 0:
            self._bump_version()
        return removed_count

    # ── 快照视图与结构化 Diff ──────────────────────────────────────────

    def _overlay_attempts(self) -> list[dict[str, Any]]:
        combined = copy.deepcopy(self._base_attempts)
        next_id = max(
            (int(a.get("attempt_id") or 0) for a in combined),
            default=0,
        ) + 1
        for rec in self._recovery_attempts.values():
            combined.append({
                "attempt_id": next_id,
                "job_uid": rec.job_uid,
                "attempt_no": rec.attempt_no,
                "run_id": rec.run_id,
                "outcome": "failure",
                "started_at": rec.started_at,
                "settled_at": rec.settled_at,
                "job": copy.deepcopy(rec.job_snapshot),
                "files": copy.deepcopy(rec.files),
                "result": copy.deepcopy(rec.result),
                "error_message": rec.error_message,
            })
            next_id += 1
        return combined

    @property
    def before(self) -> StateSnapshot:
        """进入事务前的只读一致基准快照（退出 with 后永久可读）。"""
        self._require_entered()
        assert self._before_snapshot is not None
        return self._before_snapshot

    @property
    def after(self) -> StateSnapshot:
        """当前暂存工作图的只读一致投影快照（含待补写遗留 running Attempt，退出后永久可读）。"""
        self._require_entered()
        if self._after_cache is not None and self._after_cache[0] == self._version:
            return self._after_cache[1]
        snap = self._work.to_snapshot(
            attempts=self._overlay_attempts(),
            source="edit:after",
        )
        self._after_cache = (self._version, snap)
        return snap

    def diff(self) -> str:
        """生成面向人类的 6 段紧凑对比报告（永不因校验失败或 OSError 抛异常）。"""
        self._require_entered()
        report = self._validate(capture_os_errors=True)
        self._emit_warnings(report.warnings)

        before_counts = self._base.count_statuses()
        after_counts = self._work.count_statuses()
        before_total = sum(before_counts.values())
        after_total = sum(after_counts.values())

        mode_tag = "dry_run=True" if self._dry_run else "dry_run=False"
        lines: list[str] = [f"TaskLite Edit Diff ({mode_tag}):"]
        for st in _STATUS_ORDER:
            b_val = before_counts[st]
            a_val = after_counts[st]
            lines.append(f"  {st:<9}: {b_val} -> {a_val} ({_format_signed(a_val - b_val)})")
        lines.append(
            f"  {'total':<9}: {before_total} -> {after_total} "
            f"({_format_signed(after_total - before_total)})"
        )
        if self._recovery_attempts:
            lines.append(
                f"  recovery_attempts: +{len(self._recovery_attempts)} "
                f"({', '.join(sorted(self._recovery_attempts))})"
            )

        removed_uids, replaced_pairs, added_uids, status_changes = self._classify_diff_changes()

        lines.append("")
        lines.append("[- REMOVED]")
        if not removed_uids:
            lines.append("  (none)")
        else:
            removed_succeeded = [
                u for u in removed_uids if self._base.effective_status(u) == "succeeded"
            ]
            removed_other = [
                u for u in removed_uids if self._base.effective_status(u) != "succeeded"
            ]
            for u in removed_succeeded:
                lines.append(f"  - [succeeded] {u}  (!) removed completed job")
            for u in removed_other:
                st = self._base.effective_status(u) or "unknown"
                lines.append(f"  - [{st}] {u}")

        lines.append("")
        lines.append("[⇄ REPLACED]")
        if not replaced_pairs:
            lines.append("  (none)")
        else:
            for orig_uid, new_uid in replaced_pairs:
                header = f"  ⇄ {orig_uid} -> {new_uid}" if orig_uid != new_uid else f"  ⇄ {new_uid}"
                b_st = self._base.effective_status(orig_uid)
                a_st = self._work.effective_status(new_uid)
                changes = self._format_replacement_field_diffs(orig_uid, new_uid, b_st, a_st)
                if changes:
                    lines.append(f"{header}: " + "; ".join(changes))
                else:
                    lines.append(f"{header}: (no field changes)")

        lines.append("")
        lines.append("[+ ADDED]")
        if not added_uids:
            lines.append("  (none)")
        else:
            for u in added_uids:
                st = self._work.effective_status(u) or "unknown"
                lines.append(f"  + [{st}] {u}")

        lines.append("")
        lines.append("[↻ STATUS]")
        if not status_changes:
            lines.append("  (none)")
        else:
            for u, prev_st, next_st in status_changes:
                lines.append(f"  ↻ {u}: {prev_st} -> {next_st}")

        lines.append("")
        lines.append("[WARNINGS]")
        if not report.warnings:
            lines.append("  (none)")
        else:
            for warn in report.warnings:
                lines.append(f"  ! {warn}")

        lines.append("")
        lines.append("[VALIDATION]")
        if not report.errors and not report.os_errors:
            lines.append("  PASS")
        else:
            if report.errors:
                lines.append(f"  FAIL ({len(report.errors)} error(s)):")
                for err in report.errors:
                    lines.append(f"    - {err}")
            if report.os_errors:
                lines.append(f"  OS ERROR ({len(report.os_errors)} error(s)):")
                for oerr in report.os_errors:
                    lines.append(f"    - {oerr}")

        return "\n".join(lines)

    def _classify_diff_changes(
        self,
    ) -> tuple[list[str], list[tuple[str, str]], list[str], list[tuple[str, str, str]]]:
        g_before = self._base.all_uids()
        g_after = self._work.all_uids()

        surviving_origins = {
            orig for cur, orig in self._origin_of.items() if cur in g_after
        }
        removed_uids = sorted(g_before - surviving_origins)

        replaced_pairs: list[tuple[str, str]] = []
        replaced_cur_uids: set[str] = set()
        for cur in sorted(g_after):
            orig = self._origin_of.get(cur)
            if orig is not None and (cur != orig or cur in self._replaced_in_place_uids):
                replaced_pairs.append((orig, cur))
                replaced_cur_uids.add(cur)

        added_uids = sorted(u for u in g_after if u not in self._origin_of)

        status_changes: list[tuple[str, str, str]] = []
        for u in sorted(g_after & g_before):
            if u in replaced_cur_uids:
                continue
            b_st = self._base.effective_status(u)
            a_st = self._work.effective_status(u)
            if b_st is not None and a_st is not None and b_st != a_st:
                status_changes.append((u, b_st, a_st))

        return removed_uids, replaced_pairs, added_uids, status_changes

    def _file_entries_equivalent(self, before: Any, after: Any) -> bool:
        """比对 files 单条配置：具体路径若仅写法不同但指向同一规范路径则视为等价。"""
        if before == after:
            return True
        if not isinstance(before, Mapping) or not isinstance(after, Mapping):
            return False
        if before.get("producer_uid") is not None or after.get("producer_uid") is not None:
            return False
        before_rest = {k: v for k, v in before.items() if k != "path"}
        after_rest = {k: v for k, v in after.items() if k != "path"}
        if before_rest != after_rest:
            return False
        b_path = before.get("path")
        a_path = after.get("path")
        if isinstance(b_path, str) and b_path and isinstance(a_path, str) and a_path:
            try:
                b_key = self._store.path_policy.inspect(b_path, mode="lookup").comparison_key
                a_key = self._store.path_policy.inspect(a_path, mode="lookup").comparison_key
                return b_key == a_key
            except (ValueError, OSError):
                return False
        return False

    def _format_replacement_field_diffs(
        self,
        orig_uid: str,
        new_uid: str,
        before_status: str | None,
        after_status: str | None,
    ) -> list[str]:
        parts: list[str] = []
        if before_status != after_status:
            parts.append(f"status: {before_status} -> {after_status}")
        b_rec = self.before.get_job(orig_uid)
        a_rec = self.after.get_job(new_uid)
        b_snap: Mapping[str, Any] = b_rec["job"] if b_rec is not None else {}
        a_snap: Mapping[str, Any] = a_rec["job"] if a_rec is not None else {}

        if b_snap.get("depends_on") != a_snap.get("depends_on"):
            parts.append(f"depends_on: {b_snap.get('depends_on')} -> {a_snap.get('depends_on')}")

        b_files = b_snap.get("files") or {}
        a_files = a_snap.get("files") or {}
        if b_files != a_files:
            file_diffs: list[str] = []
            for key in sorted(set(b_files) | set(a_files)):
                if key not in b_files:
                    file_diffs.append(f"+{key}={_summarize_file_entry(a_files[key])}")
                elif key not in a_files:
                    file_diffs.append(f"-{key}")
                elif not self._file_entries_equivalent(b_files[key], a_files[key]):
                    file_diffs.append(
                        f"{key}: {_summarize_file_entry(b_files[key])} -> "
                        f"{_summarize_file_entry(a_files[key])}"
                    )
            if file_diffs:
                parts.append("files{" + ", ".join(file_diffs) + "}")

        b_payload = b_snap.get("payload") or {}
        a_payload = a_snap.get("payload") or {}
        if b_payload != a_payload:
            changed_keys = sorted(
                k for k in (set(b_payload) | set(a_payload))
                if b_payload.get(k) != a_payload.get(k)
            )
            parts.append(f"payload keys changed: {changed_keys}")

        for cfg_key in ("resources", "max_retries_per_run", "timeout_seconds", "trigger"):
            if b_snap.get(cfg_key) != a_snap.get(cfg_key):
                parts.append(f"{cfg_key}: {b_snap.get(cfg_key)!r} -> {a_snap.get(cfg_key)!r}")

        return parts

    def _emit_warnings(self, warnings: tuple[str, ...]) -> None:
        if self._logged_warnings_version == self._version:
            return
        self._logged_warnings_version = self._version
        for warn in warnings:
            logger.warning("TaskLite edit warning: %s", warn)

    # ── 4 项硬 Error 单次全量攒错 + 4 类只读 Warning ──────────────────

    def _validate(self, *, capture_os_errors: bool) -> _ValidationReport:
        errors: list[str] = []
        os_errors: list[str] = []
        warnings: list[str] = []

        g_before = self._base.all_uids()
        g_after = self._work.all_uids()

        after_status: dict[str, str] = {}
        after_snaps: dict[str, dict[str, Any]] = {}
        for uid in g_after:
            st = self._work.effective_status(uid)
            snap = self._work.effective_snapshot(uid)
            if st is not None:
                after_status[uid] = st
            if snap is not None:
                after_snaps[uid] = snap

        # ── 硬 Error 1：防删出新孤儿（存活节点原本存在的上游不可消失）
        reported_missing_edges: set[tuple[str, str]] = set()
        for u in sorted(g_after & g_before):
            b_snap = self._base.effective_snapshot(u)
            a_snap = after_snaps.get(u)
            if b_snap is None or a_snap is None:
                continue
            before_deps = set(b_snap.get("depends_on") or ())
            for p in a_snap.get("depends_on") or ():
                if p in before_deps and p in g_before and p not in g_after:
                    reported_missing_edges.add((u, p))
                    errors.append(
                        f"Surviving job {u!r} lost existing dependency {p!r} "
                        f"(upstream was removed without updating or removing {u!r})"
                    )

        # ── 硬 Error 2：活跃/成功/add-replace 触碰节点零悬空依赖 + 迭代式无环
        checked_nodes: set[str] = {
            u
            for u in g_after
            if after_status.get(u) in ("pending", "running", "succeeded")
            or u in self._added_or_replaced_uids
        }
        for u in sorted(checked_nodes):
            a_snap = after_snaps.get(u)
            if a_snap is None:
                continue
            for p in a_snap.get("depends_on") or ():
                if p not in g_after and (u, p) not in reported_missing_edges:
                    reported_missing_edges.add((u, p))
                    errors.append(
                        f"Job {u!r} (status={after_status.get(u)}) references "
                        f"missing dependency {p!r}"
                    )

        edges = {
            u: [p for p in (after_snaps[u].get("depends_on") or ()) if isinstance(p, str)]
            for u in g_after
            if u in after_snaps
        }
        for cycle in find_cycle_components(g_after, edges):
            if any(node in checked_nodes for node in cycle):
                errors.append(
                    f"Dependency cycle detected involving: {', '.join(cycle)}"
                )

        # ── 硬 Error 3：from_output(p, key) 契约闭合 + Warning #4（动态声明提示）
        for u in sorted(checked_nodes):
            a_snap = after_snaps.get(u)
            if a_snap is None:
                continue
            u_deps = set(a_snap.get("depends_on") or ())
            u_files = a_snap.get("files") or {}
            if not isinstance(u_files, Mapping):
                continue
            for fname, raw_spec in u_files.items():
                if not isinstance(raw_spec, Mapping):
                    continue
                p = raw_spec.get("producer_uid")
                if p is None:
                    continue
                key = raw_spec.get("output_name")
                if p not in u_deps:
                    errors.append(
                        f"Job {u!r} file {fname!r} references from_output({p!r}, {key!r}), "
                        f"but {p!r} is not in direct depends_on"
                    )
                if p not in g_after:
                    errors.append(
                        f"Job {u!r} file {fname!r} references from_output({p!r}, {key!r}), "
                        f"but producer {p!r} does not exist"
                    )
                    continue

                p_status = after_status.get(p)
                if p_status == "succeeded":
                    succ_meta = self._work.successes.get(p) or {}
                    p_outer_files = succ_meta.get("files") or {}
                    p_artifacts = succ_meta.get("artifacts") or {}
                    file_desc = (
                        p_outer_files.get(key)
                        if isinstance(p_outer_files, Mapping)
                        else None
                    )
                    art_desc = (
                        p_artifacts.get(key)
                        if isinstance(p_artifacts, Mapping)
                        else None
                    )
                    valid_published = False
                    if isinstance(file_desc, Mapping):
                        valid_published = (
                            file_desc.get("role") == "output"
                            and not bool(file_desc.get("delete_on_success", False))
                            and isinstance(file_desc.get("path"), str)
                            and bool(file_desc.get("path"))
                            and isinstance(art_desc, Mapping)
                            and isinstance(art_desc.get("path"), str)
                            and bool(art_desc.get("path"))
                        )
                    elif isinstance(art_desc, Mapping):
                        valid_published = isinstance(art_desc.get("path"), str) and bool(
                            art_desc.get("path")
                        )
                    if not valid_published:
                        errors.append(
                            f"Job {u!r} file {fname!r} references from_output({p!r}, {key!r}), "
                            f"but succeeded producer {p!r} did not publish retained output {key!r}"
                        )
                else:
                    p_snap = after_snaps.get(p) or {}
                    p_static_files = p_snap.get("files") or {}
                    if isinstance(p_static_files, Mapping) and key in p_static_files:
                        static_desc = p_static_files[key]
                        if (
                            not isinstance(static_desc, Mapping)
                            or static_desc.get("role") != "output"
                            or bool(static_desc.get("delete_on_success", False))
                        ):
                            errors.append(
                                f"Job {u!r} file {fname!r} references from_output({p!r}, {key!r}), "
                                f"but producer {p!r} statically declares {key!r} as non-retained "
                                f"or non-output"
                            )
                    else:
                        warnings.append(
                            f"Job {u!r} file {fname!r} references from_output({p!r}, {key!r}), "
                            f"which is not statically declared on non-succeeded producer {p!r} "
                            f"(relies on runtime ctx.declare_file)"
                        )

        # ── 硬 Error 4：本次 add/replace 最终处于 succeeded 的保留型 output 物理在场
        path_policy = self._store.path_policy
        for uid in sorted(self._written_succeeded_uids):
            if after_status.get(uid) != "succeeded":
                continue
            manual_succ_meta = self._work.successes.get(uid)
            if manual_succ_meta is None:
                continue
            snap = manual_succ_meta.get("job_snapshot") or {}
            raw_files = snap.get("files") or {}
            if not isinstance(raw_files, Mapping):
                continue
            for fname, raw_spec in raw_files.items():
                if not isinstance(raw_spec, Mapping):
                    continue
                if raw_spec.get("role") != "output" or bool(
                    raw_spec.get("delete_on_success", False)
                ):
                    continue
                raw_path = raw_spec.get("path")
                if not isinstance(raw_path, str) or not raw_path:
                    errors.append(
                        f"Manual succeeded job {uid!r} output {fname!r} has no concrete path"
                    )
                    continue
                try:
                    inspected = path_policy.inspect(raw_path, mode="strict")
                except ValueError as exc:
                    errors.append(
                        f"Manual succeeded job {uid!r} output {fname!r} failed path check: {exc}"
                    )
                    continue
                except OSError as exc:
                    if capture_os_errors:
                        os_errors.append(
                            f"Manual succeeded job {uid!r} output {fname!r} OS error: {exc}"
                        )
                        continue
                    raise
                if not inspected.exists:
                    errors.append(
                        f"Manual succeeded job {uid!r} output {fname!r} does not exist on disk: "
                        f"{inspected.canonical_path}"
                    )
                else:
                    # 验盘确认的规范路径当场同步回填至外层 files/artifacts 并失效 after 缓存
                    canonical_path = inspected.canonical_path
                    outer_files = manual_succ_meta.setdefault("files", {})
                    artifacts = manual_succ_meta.setdefault("artifacts", {})
                    changed = False
                    existing_file = outer_files.get(fname)
                    if isinstance(existing_file, dict) and existing_file.get("path") != canonical_path:
                        existing_file["path"] = canonical_path
                        changed = True
                    existing_art = artifacts.get(fname)
                    if not isinstance(existing_art, dict) or existing_art.get("path") != canonical_path:
                        artifacts[fname] = {"path": canonical_path}
                        changed = True
                    if changed:
                        self._after_cache = None

        # ── 只读 Warning #1 / #2 / #3（Warning #4 已在 Error 3 循环收集）
        removed_uids, _, _, _ = self._classify_diff_changes()
        for uid in removed_uids:
            if self._base.effective_status(uid) == "succeeded":
                warnings.append(f"Removing completed [succeeded] job {uid!r}")

        for uid in sorted(self._partial_removed_uids):
            if uid not in g_after:
                continue
            remaining_parts: list[str] = []
            if uid in self._work.queue:
                remaining_parts.append("queue")
            if uid in self._work.successes:
                remaining_parts.append("successes")
            if uid in self._work.failed:
                remaining_parts.append("failures")
            if remaining_parts:
                warnings.append(
                    f"Job {uid!r} was removed from selected partition(s) but still remains in "
                    f"{remaining_parts} (effective status: {after_status.get(uid)})"
                )

        for uid in sorted(self._touched_uids):
            if after_status.get(uid) != "pending":
                continue
            archives: list[str] = []
            if uid in self._work.successes:
                archives.append("successes")
            if uid in self._work.failed:
                archives.append("failures")
            if archives:
                warnings.append(
                    f"Pending job {uid!r} also has historical {archives} archive "
                    f"(next run() will evaluate Trigger against residual archive)"
                )

        return _ValidationReport(
            errors=tuple(errors),
            os_errors=tuple(os_errors),
            warnings=tuple(warnings),
        )

    # ── O(Δ) 声明式提交载荷构造 ───────────────────────────────────────

    def _build_commit(self) -> GraphEditCommit:
        queue_writes: dict[str, dict[str, Any] | None] = {}
        for uid in self._base.queue:
            if uid not in self._work.queue:
                queue_writes[uid] = None
        for uid, slot in self._work.queue.items():
            base_slot = self._base.queue.get(uid)
            if (
                base_slot is None
                or slot.queue_position is None
                or slot.status != base_slot.status
                or slot.job_snapshot != base_slot.job_snapshot
            ):
                queue_writes[uid] = copy.deepcopy(slot.job_snapshot)

        success_writes: dict[str, dict[str, Any] | None] = {}
        for uid in self._base.successes:
            if uid not in self._work.successes:
                success_writes[uid] = None
        for uid, meta in self._work.successes.items():
            if uid not in self._base.successes or meta != self._base.successes[uid]:
                success_writes[uid] = copy.deepcopy(meta)

        failed_writes: dict[str, tuple[dict[str, Any], dict[str, Any] | None] | None] = {}
        for uid in self._base.failed:
            if uid not in self._work.failed:
                failed_writes[uid] = None
        for uid, entry in self._work.failed.items():
            if uid not in self._base.failed or entry != self._base.failed[uid]:
                failed_writes[uid] = (copy.deepcopy(entry[0]), copy.deepcopy(entry[1]))

        return GraphEditCommit(
            queue_writes=queue_writes,
            success_writes=success_writes,
            failed_writes=failed_writes,
            recovery_attempts=tuple(self._recovery_attempts.values()),
        )


__all__ = [
    "EditTransaction",
]
