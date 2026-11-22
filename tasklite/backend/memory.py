"""v2 纯内存状态后端（InMemoryStateBackend）。

实现 AbstractStateBackend 完整契约，提供零文件系统 IO 的纯内存状态
存储：瞬态管线、单元测试、沙盒执行。快照隔离的 delta 事务语义与
SQLite 腿对齐——「校验先行、统一落变」模拟事务回滚：抛异常时全部集合
与调用前完全一致。
"""
from __future__ import annotations

import copy
import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal, Mapping, Sequence, cast

from .base import (
    AbstractStateBackend,
    FailureEntry,
    GraphEditCommit,
    is_valid_authorization_target_type,
    normalize_authorization_path,
    normalize_failure_entries,
    public_queue_snapshot,
    queue_snapshot_execution_id,
    resolve_authorized_at,
    validate_authorization_target_type,
    validate_cursor_key,
    validate_cursor_value,
    validate_graph_edit_commit,
    validate_mark_job_running_args,
    validate_mark_job_running_state,
    validate_recovery_attempt_state,
    validate_running_settlement_state,
    validate_settlement_args,
)
from ..exceptions import TaskLiteError
from ..models.state import job_uid_from_snapshot
from ..utils.artifact import TargetType, evaluate_authorization

logger = logging.getLogger("tasklite")


@dataclass
class _MemoryQueueRow:
    """纯内存队列结构化行记录（与 SQLite ``queue`` 表列职责逐一对应，不向业务快照混写运行态键）。"""

    job_uid: str
    job_snapshot: dict[str, Any]
    status: str = "pending"
    execution_id: str | None = None
    run_id: str | None = None
    attempt_no: int | None = None
    started_at: str | None = None


class InMemoryStateBackend(AbstractStateBackend):
    """纯内存状态后端：单锁串行化 + 深拷贝隔离的快照语义。

    所有 load_* 返回深拷贝、所有写路径对入参深拷贝——外部可变别名
    不得穿透快照隔离。attempts 以 dict 模拟自增主键（id 严格递增，
    插入序即 id 序）。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._exec_lock = threading.Lock()
        self._successes: dict[str, dict[str, Any]] = {}
        self._failed: dict[str, dict[str, Any]] = {}
        self._failure_payloads: dict[str, dict[str, Any]] = {}
        self._cursors: dict[str, str] = {}
        self._queue: list[_MemoryQueueRow] = []
        self._engine_metadata: dict[str, str] = {}
        self._attempts: dict[int, dict[str, Any]] = {}
        self._attempt_seq = 1
        self._job_sequences: dict[str, int] = {}
        self._authorizations: dict[str, dict[str, Any]] = {}

    @contextmanager
    def exclusive_access(self) -> Iterator[None]:
        """非阻塞获取内存后端进程内独占执行锁，离开作用域归还。"""
        if not self._exec_lock.acquire(blocking=False):
            raise TaskLiteError(
                "Another run() or management operation is in progress; "
                "concurrent runs or management operations on the same state are forbidden."
            )
        try:
            yield
        finally:
            self._exec_lock.release()

    # 全量加载 -------------------------------------------------------

    def load_successes(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._successes)

    def load_failed(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._failed)

    # delta 提交 -----------------------------------------------------

    def commit_job_failures(self, entries: Sequence[FailureEntry]) -> None:
        normalized = normalize_failure_entries(entries)
        if not normalized:
            return
        with self._lock:
            try:
                # 所有可失败的复制与队列构造先于统一发布（payload=None 保留既有快照）。
                staged = [
                    (
                        job_uid,
                        copy.deepcopy(meta or {}),
                        copy.deepcopy(payload) if payload is not None else None,
                    )
                    for job_uid, meta, payload in normalized
                ]
                fail_uids = {uid for uid, _, _ in staged}
                new_failed = dict(self._failed)
                new_payloads = dict(self._failure_payloads)
                for uid, meta_copy, payload_copy in staged:
                    new_failed[uid] = meta_copy
                    if payload_copy is not None:
                        new_payloads[uid] = payload_copy
                new_successes = {
                    uid: meta for uid, meta in self._successes.items() if uid not in fail_uids
                }
                new_queue = [row for row in self._queue if row.job_uid not in fail_uids]
                self._failed = new_failed
                self._failure_payloads = new_payloads
                self._successes = new_successes
                self._queue = new_queue
            except Exception as e:
                logger.critical(f"Failed to commit failure archive entries: {e}")
                raise

    def commit_skip(self, job_uid: str) -> None:
        with self._lock:
            self._queue = [row for row in self._queue if row.job_uid != job_uid]

    # 增量入队 -------------------------------------------------------

    def enqueue_jobs(self, job_snapshots: list[dict[str, Any]], *, front: bool = False) -> list[str]:
        if not job_snapshots:
            return []
        with self._lock:
            existing = {row.job_uid for row in self._queue}
            fresh: list[_MemoryQueueRow] = []
            batch_seen: set[str] = set()
            for j in job_snapshots:
                u = job_uid_from_snapshot(j)
                if u in existing or u in batch_seen:
                    continue
                batch_seen.add(u)
                fresh.append(
                    _MemoryQueueRow(
                        job_uid=u,
                        job_snapshot=public_queue_snapshot(j),
                        status=j.get("status", "pending"),
                        execution_id=queue_snapshot_execution_id(j),
                        run_id=j.get("run_id"),
                        attempt_no=j.get("attempt_no"),
                        started_at=j.get("started_at"),
                    )
                )
            if not fresh:
                return []
            if front:
                self._queue[0:0] = fresh
            else:
                self._queue.extend(fresh)
            return [row.job_uid for row in fresh]

    # 管理写与查询 --------------------------------------------------

    def commit_graph_edit(self, commit: GraphEditCommit) -> None:
        """在单锁 Copy-on-Write 临界区内校验并原子应用图修整变更，故障零污染。"""
        if not validate_graph_edit_commit(commit):
            return

        with self._lock:
            new_attempts = dict(self._attempts)
            new_attempt_seq = self._attempt_seq

            for rec in commit.recovery_attempts:
                running = next(
                    (row for row in self._queue if row.job_uid == rec.job_uid),
                    None,
                )
                settled = any(
                    attempt.get("job_uid") == rec.job_uid
                    and attempt.get("attempt_no") == rec.attempt_no
                    for attempt in new_attempts.values()
                )
                validate_recovery_attempt_state(
                    rec,
                    present_in_queue=running is not None,
                    queue_status=running.status if running is not None else None,
                    queue_run_id=running.run_id if running is not None else None,
                    queue_attempt_no=running.attempt_no if running is not None else None,
                    queue_execution_id=running.execution_id if running is not None else None,
                    attempt_sequence=self._job_sequences.get(rec.job_uid),
                    already_settled=settled,
                )
                new_attempts[new_attempt_seq] = {
                    "attempt_id": new_attempt_seq,
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
                }
                new_attempt_seq += 1

            touched_queue_uids = set(commit.queue_writes)
            new_queue = [
                row for row in self._queue if row.job_uid not in touched_queue_uids
            ]
            for uid, snap in commit.queue_writes.items():
                if snap is not None:
                    new_queue.append(
                        _MemoryQueueRow(
                            job_uid=uid,
                            job_snapshot=public_queue_snapshot(snap),
                            status="pending",
                            execution_id=None,
                            run_id=None,
                            attempt_no=None,
                            started_at=None,
                        )
                    )

            new_successes = dict(self._successes)
            for uid, meta in commit.success_writes.items():
                if meta is None:
                    new_successes.pop(uid, None)
                else:
                    new_successes[uid] = copy.deepcopy(meta)

            new_failed = dict(self._failed)
            new_failure_payloads = dict(self._failure_payloads)
            for uid, entry in commit.failed_writes.items():
                if entry is None:
                    new_failed.pop(uid, None)
                    new_failure_payloads.pop(uid, None)
                else:
                    fail_meta, payload = entry
                    new_failed[uid] = copy.deepcopy(fail_meta)
                    if payload is not None:
                        new_failure_payloads[uid] = copy.deepcopy(payload)
                    else:
                        new_failure_payloads.pop(uid, None)

            overlap = new_successes.keys() & new_failed.keys()
            if overlap:
                sample = next(iter(sorted(overlap)))
                raise TaskLiteError(
                    f"Invariant conflict: job {sample!r} cannot exist in both successes and failed"
                )

            self._attempts = new_attempts
            self._attempt_seq = new_attempt_seq
            self._queue = new_queue
            self._successes = new_successes
            self._failed = new_failed
            self._failure_payloads = new_failure_payloads

    def get_cursor(self, key: str) -> str | None:
        """查询单个游标的最新已提交值，不存在返回 None。"""
        validate_cursor_key(key)
        with self._lock:
            return self._cursors.get(key)

    def set_cursor(self, key: str, value: str | None) -> None:
        """设置或删除单个游标（None 为删除）。"""
        validate_cursor_key(key)
        validate_cursor_value(value)
        with self._lock:
            if value is None:
                self._cursors.pop(key, None)
            else:
                self._cursors[key] = value

    # 尝试轨迹（append-only 旁路观测面）--------------------------------

    def load_latest_succeeded_finished_at(self, job_uid: str) -> str | None:
        """按 attempt_id 倒序读取指定 Job 最新成功 Attempt 的结算时间。"""
        with self._lock:
            for rec in reversed(list(self._attempts.values())):
                if rec.get("job_uid") == job_uid and rec.get("outcome") == "success":
                    return rec["settled_at"]
            return None

    # 元数据 ---------------------------------------------------------

    def set_engine_metadata(self, key: str, value: str) -> None:
        with self._lock:
            self._engine_metadata[key] = str(value)

    # 状态事务与编号 -------------------------------------------------

    def mark_job_running(
        self, job_uid: str, *, execution_id: str, run_id: str, started_at: str
    ) -> int:
        validate_mark_job_running_args(
            execution_id=execution_id, run_id=run_id, started_at=started_at
        )
        with self._lock:
            job_idx = None
            for idx, row in enumerate(self._queue):
                if row.job_uid == job_uid:
                    job_idx = idx
                    break
            validate_mark_job_running_state(
                job_uid,
                present_in_queue=job_idx is not None,
                queue_status=self._queue[job_idx].status if job_idx is not None else None,
            )
            assert job_idx is not None
            current = self._queue[job_idx]

            next_no = self._job_sequences.get(job_uid, 0) + 1
            self._job_sequences[job_uid] = next_no

            self._queue[job_idx] = _MemoryQueueRow(
                job_uid=current.job_uid,
                job_snapshot=current.job_snapshot,
                status="running",
                execution_id=execution_id,
                run_id=run_id,
                attempt_no=next_no,
                started_at=started_at,
            )
            return next_no

    def load_consistent_snapshot(self) -> dict[str, Any]:
        """在单锁内原子捕获一致内存状态快照供恢复、入队、StateSnapshot 与 EditTransaction 使用。"""
        with self._lock:
            return {
                "queue_jobs": self.load_queue_jobs(),
                "successes": copy.deepcopy(self._successes),
                "failed": copy.deepcopy(self._failed),
                "failure_payloads": copy.deepcopy(self._failure_payloads),
                "cursors": dict(self._cursors),
                # attempts 单条记录入库时已深拷贝不可变，锁内仅浅拷贝外层列表以控制峰值内存；
                # 查询输出的深拷贝隔离由快照投影核心负责。
                "attempts": list(self._attempts.values()),
                "job_sequences": dict(self._job_sequences),
            }

    def load_queue_jobs(self, *, status: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            results: list[dict[str, Any]] = []
            for pos, row in enumerate(self._queue):
                if status is not None and row.status != status:
                    continue
                results.append({
                    "job_uid": row.job_uid,
                    "queue_position": pos,
                    "job": copy.deepcopy(row.job_snapshot),
                    "status": row.status,
                    "execution_id": row.execution_id,
                    "run_id": row.run_id,
                    "attempt_no": row.attempt_no,
                    "started_at": row.started_at,
                })
            return results

    def commit_job_settlement(
        self,
        *,
        job_uid: str,
        outcome: str,
        attempt_no: int,
        execution_id: str,
        run_id: str,
        settled_at: str,
        started_at: str | None = None,
        job_snapshot: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        error_message: str | None = None,
        success_metadata: dict[str, Any] | None = None,
        spawned_job_snapshots: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
        failure_metadata: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        requeued_job_snapshot: dict[str, Any] | None = None,
        record_attempt: bool = True,
    ) -> None:
        validate_settlement_args(
            outcome=outcome,
            attempt_no=attempt_no,
            execution_id=execution_id,
            run_id=run_id,
            settled_at=settled_at,
            started_at=started_at,
            cursor_updates=cursor_updates,
            spawned_job_snapshots=spawned_job_snapshots,
        )

        with self._lock:
            running = next(
                (row for row in self._queue if row.job_uid == job_uid), None
            )
            settled = any(
                attempt.get("job_uid") == job_uid
                and attempt.get("attempt_no") == attempt_no
                for attempt in self._attempts.values()
            )
            validate_running_settlement_state(
                job_uid,
                present_in_queue=running is not None,
                queue_status=running.status if running is not None else None,
                queue_run_id=running.run_id if running is not None else None,
                queue_attempt_no=running.attempt_no if running is not None else None,
                queue_execution_id=running.execution_id if running is not None else None,
                run_id=run_id,
                attempt_no=attempt_no,
                execution_id=execution_id,
                already_settled=settled,
            )

            new_attempts = dict(self._attempts)
            new_attempt_id = self._attempt_seq
            if record_attempt:
                attempt_outcome = (
                    outcome if outcome in ("success", "failure", "retry") else "failure"
                )
                attempt_entry: dict[str, Any] = {
                    "attempt_id": new_attempt_id,
                    "job_uid": job_uid,
                    "attempt_no": attempt_no,
                    "run_id": run_id,
                    "outcome": attempt_outcome,
                    "started_at": started_at,
                    "settled_at": settled_at,
                    "job": copy.deepcopy(job_snapshot),
                    "files": copy.deepcopy(files),
                    "result": copy.deepcopy(result),
                    "error_message": error_message,
                }
                new_attempts[new_attempt_id] = attempt_entry

            new_queue = list(self._queue)
            new_successes = dict(self._successes)
            new_failed = dict(self._failed)
            new_failure_payloads = dict(self._failure_payloads)
            new_cursors = dict(self._cursors)

            if outcome == "success":
                remaining = [row for row in new_queue if row.job_uid != job_uid]
                if spawned_job_snapshots:
                    remaining_uids = {row.job_uid for row in remaining}
                    seen: set[str] = set()
                    staged_spawned: list[_MemoryQueueRow] = []
                    for sj in spawned_job_snapshots:
                        suid = job_uid_from_snapshot(sj)
                        if suid in remaining_uids or suid in seen:
                            raise TaskLiteError(
                                f"Spawned job {suid} conflicts with queue for {job_uid}"
                            )
                        seen.add(suid)
                        staged_spawned.append(
                            _MemoryQueueRow(
                                job_uid=suid,
                                job_snapshot=public_queue_snapshot(sj),
                                status="pending",
                                execution_id=None,
                                run_id=None,
                                attempt_no=None,
                                started_at=None,
                            )
                        )
                    remaining[0:0] = staged_spawned
                new_queue = remaining

                new_successes[job_uid] = copy.deepcopy(success_metadata or {})
                new_failed.pop(job_uid, None)
                new_failure_payloads.pop(job_uid, None)

                if cursor_updates:
                    for k, v in cursor_updates.items():
                        if v is None:
                            new_cursors.pop(k, None)
                        else:
                            new_cursors[k] = v

            elif outcome == "failure":
                new_queue = [row for row in new_queue if row.job_uid != job_uid]
                new_failed[job_uid] = copy.deepcopy(failure_metadata or {})
                if payload is not None:
                    new_failure_payloads[job_uid] = copy.deepcopy(payload)
                else:
                    new_failure_payloads.pop(job_uid, None)
                new_successes.pop(job_uid, None)

            elif outcome == "retry":
                for idx, row in enumerate(new_queue):
                    if row.job_uid == job_uid:
                        target_snap = (
                            public_queue_snapshot(requeued_job_snapshot)
                            if requeued_job_snapshot is not None
                            else copy.deepcopy(row.job_snapshot)
                        )
                        new_queue[idx] = _MemoryQueueRow(
                            job_uid=row.job_uid,
                            job_snapshot=target_snap,
                            status="pending",
                            execution_id=None,
                            run_id=None,
                            attempt_no=None,
                            started_at=None,
                        )
                        break

            elif outcome == "pending":
                for idx, row in enumerate(new_queue):
                    if row.job_uid == job_uid:
                        new_queue[idx] = _MemoryQueueRow(
                            job_uid=row.job_uid,
                            job_snapshot=copy.deepcopy(row.job_snapshot),
                            status="pending",
                            execution_id=None,
                            run_id=None,
                            attempt_no=None,
                            started_at=None,
                        )
                        break

            self._attempts = new_attempts
            if record_attempt:
                self._attempt_seq += 1
            self._queue = new_queue
            self._successes = new_successes
            self._failed = new_failed
            self._failure_payloads = new_failure_payloads
            self._cursors = new_cursors

    def _existing_auth_pairs(self) -> list[tuple[str, TargetType]]:
        return [
            (rec["path"], cast(TargetType, rec["target_type"]))
            for rec in self._authorizations.values()
        ]

    def grant_authorization(
        self,
        path: str,
        target_type: Literal["file", "directory"],
        *,
        authorized_at: str | None = None,
    ) -> dict[str, Any]:
        norm_path = normalize_authorization_path(path)
        validated_type = validate_authorization_target_type(target_type, error_type=TaskLiteError)
        ts = resolve_authorized_at(authorized_at)
        with self._lock:
            match = evaluate_authorization(
                self._existing_auth_pairs(),
                norm_path,
                validated_type,
            )
            if match.grant_conflict is not None:
                raise TaskLiteError(match.grant_conflict)
            record = {
                "path": norm_path,
                "target_type": validated_type,
                "authorized_at": ts,
            }
            self._authorizations[norm_path] = record
            return dict(record)

    def record_authorization(
        self,
        path: str,
        target_type: str,
        *,
        authorized_at: str | None = None,
    ) -> None:
        norm_path = normalize_authorization_path(path)
        validated_type = validate_authorization_target_type(target_type, error_type=ValueError)
        ts = resolve_authorized_at(authorized_at)
        with self._lock:
            match = evaluate_authorization(
                self._existing_auth_pairs(),
                norm_path,
                validated_type,
            )
            if match.exact_path is not None and match.exact_path != norm_path:
                self._authorizations.pop(match.exact_path, None)
            self._authorizations[norm_path] = {
                "path": norm_path,
                "target_type": validated_type,
                "authorized_at": ts,
            }

    def list_authorizations(self) -> list[dict[str, Any]]:
        with self._lock:
            records = [dict(rec) for rec in self._authorizations.values()]
            return sorted(records, key=lambda r: (r["authorized_at"], r["path"]))

    def revoke_authorization(self, path: str) -> bool:
        norm_path = normalize_authorization_path(path)
        with self._lock:
            match = evaluate_authorization(self._existing_auth_pairs(), norm_path)
            if match.exact_path is None:
                return False
            self._authorizations.pop(match.exact_path, None)
            return True

    def is_path_authorized(self, path: str, target_type: str) -> bool:
        norm_path = normalize_authorization_path(path)
        if not is_valid_authorization_target_type(target_type):
            return False
        with self._lock:
            return evaluate_authorization(
                self._existing_auth_pairs(),
                norm_path,
                cast(TargetType, target_type),
            ).covered

    def close(self) -> None:
        """关闭后端并释放资源（幂等）。"""
        pass


__all__ = ["InMemoryStateBackend"]
