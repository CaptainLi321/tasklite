"""v2 纯内存状态后端（InMemoryStateBackend）。

实现 AbstractStateBackend 完整契约，提供零文件系统 IO 的纯内存状态
存储：瞬态管线、单元测试、沙盒执行。快照隔离的 delta 事务语义与
SQLite 腿对齐——「校验先行、统一落变」模拟事务回滚：返回 False 或
抛异常时全部集合与调用前完全一致。
"""
from __future__ import annotations

import copy
import logging
import os
import threading
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence, cast

from .base import (
    AbstractStateBackend,
    GraphEditCommit,
    QUEUE_EXECUTION_ID_KEY,
    RunningAttemptRecovery,
    execution_id_matches_run,
    public_queue_snapshot,
    queue_snapshot_execution_id,
    queue_snapshot_with_execution_id,
    validate_graph_edit_commit,
    validate_queue_replacement,
    validate_recovery_attempt_state,
)
from ..exceptions import TaskLiteError
from ..models.state import job_uid_from_snapshot
from ..utils.artifact import TargetType, evaluate_authorization
from ..utils.clock import utc_now_iso

logger = logging.getLogger("tasklite")


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
        self._queue: list[dict[str, Any]] = []
        self._engine_metadata: dict[str, str] = {}
        self._attempts: dict[int, dict[str, Any]] = {}
        self._attempt_seq = 1
        self._job_sequences: dict[str, int] = {}
        self._authorizations: dict[str, dict[str, Any]] = {}

    # 全量加载 -------------------------------------------------------

    def load_successes(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._successes)

    def load_failed(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._failed)

    def load_failure_payloads(self) -> dict[str, dict[str, Any]]:
        """读取失败档案各 uid 的原始业务 payload 快照（无快照的 uid 不出现）。"""
        with self._lock:
            return copy.deepcopy(self._failure_payloads)

    def load_cursors(self) -> dict[str, str]:
        with self._lock:
            return dict(self._cursors)

    def load_queue(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                public_queue_snapshot(j)
                for j in self._queue if j.get("status", "pending") == "pending"
            ]

    # delta 提交 -----------------------------------------------------


    def commit_job_failure(
        self, job_uid: str, failure_metadata: dict, payload: dict[str, Any] | None = None
    ) -> bool:
        with self._lock:
            try:
                # 校验先行：失败 meta 与 payload 快照的拷贝在变更前完成，
                # 任一步失败 ⇒ failed/queue/successes 整体不变（对齐 SQLite 回滚）。
                meta_copy = copy.deepcopy(failure_metadata or {})
                payload_copy = (
                    copy.deepcopy(payload) if payload is not None else None
                )
                remaining = [j for j in self._queue if job_uid_from_snapshot(j) != job_uid]
                self._failed[job_uid] = meta_copy
                # payload 快照 latest-wins；None 表示本路径无 payload，保留
                # 既有快照（与 SQLite 腿 _write_failed_row 的保留语义对齐）。
                if payload_copy is not None:
                    self._failure_payloads[job_uid] = payload_copy
                self._queue = remaining
                # 同一事务语义内清理 successes 旧记录：重跑任务失败时旧成功
                # 记录作废（最终状态唯一），防 successes∩failed 并存。
                self._successes.pop(job_uid, None)
                return True
            except Exception as e:
                logger.critical(f"Failed to commit job failure for {job_uid}: {e}")
                raise


    def commit_job_failures(self, job_uids_metadata: list[tuple[str, dict]]) -> None:
        with self._lock:
            # 所有可失败的复制与队列构造先于统一发布。
            entries = [(job_uid, copy.deepcopy(meta or {})) for job_uid, meta in job_uids_metadata]
            fail_uids = {u for u, _ in entries}
            remaining = [j for j in self._queue if job_uid_from_snapshot(j) not in fail_uids]
            failed = self._failed | dict(entries)
            successes = {uid: meta for uid, meta in self._successes.items() if uid not in fail_uids}
            self._failed = failed
            self._successes = successes
            self._queue = remaining

    def commit_skip(self, job_uid: str) -> None:
        with self._lock:
            self._queue = [j for j in self._queue if job_uid_from_snapshot(j) != job_uid]


    # 增量入队 -------------------------------------------------------

    def enqueue_jobs(self, job_snapshots: list[dict[str, Any]], *, front: bool = False) -> list[str]:
        if not job_snapshots:
            return []
        with self._lock:
            existing = {job_uid_from_snapshot(j) for j in self._queue}
            fresh: list[dict[str, Any]] = []
            batch_seen: set[str] = set()
            for j in job_snapshots:
                u = job_uid_from_snapshot(j)
                # 批次内重复（同一调用传相同 uid 两次）与队列中已有的
                # uid 都跳过（首个存储，后续跳过）。
                if u in existing or u in batch_seen:
                    continue
                batch_seen.add(u)
                fresh.append(copy.deepcopy(j))
            if not fresh:
                return []
            if front:
                self._queue[0:0] = fresh
            else:
                self._queue.extend(fresh)
            return [job_uid_from_snapshot(j) for j in fresh]

    # 管理写与查询 --------------------------------------------------

    def commit_graph_edit(self, commit: GraphEditCommit) -> None:
        """在单锁 Copy-on-Write 临界区内校验并原子应用图修整变更，故障零污染。"""
        if not validate_graph_edit_commit(commit):
            return

        with self._lock:
            new_attempts = dict(self._attempts)
            new_attempt_seq = self._attempt_seq

            # 1. 复核遗留 running 执行身份并追加失败 Attempt（严格 append-only）
            for rec in commit.recovery_attempts:
                running = next(
                    (item for item in self._queue if job_uid_from_snapshot(item) == rec.job_uid),
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
                    queue_status=running.get("status") if running is not None else None,
                    queue_run_id=running.get("run_id") if running is not None else None,
                    queue_attempt_no=running.get("attempt_no") if running is not None else None,
                    queue_execution_id=(
                        queue_snapshot_execution_id(running) if running is not None else None
                    ),
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

            # 2. 按触碰 uid 构造新三表（未触碰行含遗留 running 原样保留）
            touched_queue_uids = set(commit.queue_writes)
            new_queue = [
                j for j in self._queue if job_uid_from_snapshot(j) not in touched_queue_uids
            ]
            for uid, snap in commit.queue_writes.items():
                if snap is not None:
                    clean = public_queue_snapshot(snap)
                    clean["status"] = "pending"
                    clean["run_id"] = None
                    clean["attempt_no"] = None
                    clean["started_at"] = None
                    new_queue.append(clean)

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

            # 3. 强制复核 successes ∩ failed == ∅ 不变式
            overlap = new_successes.keys() & new_failed.keys()
            if overlap:
                sample = next(iter(sorted(overlap)))
                raise TaskLiteError(
                    f"Invariant conflict: job {sample!r} cannot exist in both successes and failed"
                )

            # 4. 一次性原子发布全部引用
            self._attempts = new_attempts
            self._attempt_seq = new_attempt_seq
            self._queue = new_queue
            self._successes = new_successes
            self._failed = new_failed
            self._failure_payloads = new_failure_payloads

    def get_cursor(self, key: str) -> str | None:
        """查询单个游标的最新已提交值，不存在返回 None。"""
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        with self._lock:
            return self._cursors.get(key)

    def set_cursor(self, key: str, value: str | None) -> None:
        """设置或删除单个游标（None 为删除）。"""
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        if value is not None and not isinstance(value, str):
            raise TypeError(f"cursor value must be str or None, got {type(value).__name__}")
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

    def get_engine_metadata(self, key: str) -> str | None:
        with self._lock:
            return self._engine_metadata.get(key)

    def set_engine_metadata(self, key: str, value: str) -> None:
        with self._lock:
            self._engine_metadata[key] = str(value)

    # 状态事务与编号 -------------------------------------------------

    def mark_job_running(
        self, job_uid: str, *, execution_id: str, run_id: str, started_at: str
    ) -> int:
        if not isinstance(execution_id, str) or not execution_id:
            raise ValueError("execution_id must be a non-empty str")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty str")
        if not execution_id_matches_run(execution_id, run_id):
            raise ValueError(f"execution_id {execution_id!r} does not belong to run {run_id!r}")
        if not isinstance(started_at, str) or not started_at:
            raise ValueError("started_at must be a non-empty str")
        with self._lock:
            job_idx = None
            for idx, item in enumerate(self._queue):
                if job_uid_from_snapshot(item) == job_uid:
                    job_idx = idx
                    break
            if job_idx is None:
                raise ValueError(f"Job {job_uid} not found in queue")
            job_data = self._queue[job_idx]
            if job_data.get("status") == "running":
                raise ValueError(f"Job {job_uid} is already running")

            next_no = self._job_sequences.get(job_uid, 0) + 1
            self._job_sequences[job_uid] = next_no

            updated_job = queue_snapshot_with_execution_id(job_data, execution_id)
            updated_job["status"] = "running"
            updated_job["run_id"] = run_id
            updated_job["attempt_no"] = next_no
            updated_job["started_at"] = started_at
            self._queue[job_idx] = updated_job
            return next_no

    def get_attempt_sequence(self, job_uid: str) -> int:
        with self._lock:
            return self._job_sequences.get(job_uid, 0)

    def load_consistent_snapshot(self) -> dict[str, Any]:
        """在单锁内原子捕获一致内存状态快照供 StateSnapshot 与 EditTransaction 只读加载。"""
        with self._lock:
            return {
                "queue_jobs": self.load_queue_jobs(),
                "successes": self.load_successes(),
                "failed": self.load_failed(),
                "failure_payloads": self.load_failure_payloads(),
                # attempts 单条记录入库时已深拷贝不可变，锁内仅浅拷贝外层列表以控制峰值内存；
                # 查询输出的深拷贝隔离由快照投影核心负责。
                "attempts": list(self._attempts.values()),
                "job_sequences": dict(self._job_sequences),
            }

    def load_settled_attempts(self, job_uid: str) -> list[dict[str, Any]]:
        with self._lock:
            entries: list[dict[str, Any]] = []
            for att in self._attempts.values():
                if att.get("job_uid") == job_uid:
                    entries.append(copy.deepcopy(att))
            entries.sort(key=lambda x: x.get("attempt_no", 0))
            return entries

    def load_queue_jobs(self, *, status: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            results: list[dict[str, Any]] = []
            for pos, j in enumerate(self._queue):
                j_status = j.get("status", "pending")
                if status is not None and j_status != status:
                    continue
                results.append({
                    "job_uid": job_uid_from_snapshot(j),
                    "queue_position": pos,
                    "job": public_queue_snapshot(j),
                    "status": j_status,
                    "execution_id": queue_snapshot_execution_id(j),
                    "run_id": j.get("run_id"),
                    "attempt_no": j.get("attempt_no"),
                    "started_at": j.get("started_at"),
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
        if outcome not in ("success", "failure", "retry", "pending"):
            raise ValueError(f"Invalid outcome: {outcome}")
        if not isinstance(attempt_no, int) or isinstance(attempt_no, bool) or attempt_no < 1:
            raise TypeError(f"attempt_no must be int >= 1, got {attempt_no!r}")
        if not isinstance(execution_id, str) or not execution_id:
            raise ValueError(f"execution_id must be non-empty str, got {execution_id!r}")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError(f"run_id must be non-empty str, got {run_id!r}")
        if not execution_id_matches_run(execution_id, run_id):
            raise ValueError(f"execution_id {execution_id!r} does not belong to run {run_id!r}")
        if not isinstance(settled_at, str) or not settled_at:
            raise ValueError(f"settled_at must be non-empty str, got {settled_at!r}")
        if started_at is not None and (not isinstance(started_at, str) or not started_at):
            raise ValueError("started_at must be non-empty str or None")
        if cursor_updates:
            for k, v in cursor_updates.items():
                if not isinstance(k, str) or not k:
                    raise ValueError(f"cursor key must be a non-empty str, got {k!r}")
                if v is not None and not isinstance(v, str):
                    raise TypeError(f"cursor value must be str or None, got {type(v).__name__}")
        if spawned_job_snapshots:
            validate_queue_replacement(list(spawned_job_snapshots))

        with self._lock:
            running = next(
                (item for item in self._queue if job_uid_from_snapshot(item) == job_uid), {}
            )
            if (
                running.get("status"),
                running.get("run_id"),
                running.get("attempt_no"),
                queue_snapshot_execution_id(running),
            ) != ("running", run_id, attempt_no, execution_id):
                raise TaskLiteError(f"Settlement does not match the running execution for {job_uid}")
            if any(
                attempt.get("job_uid") == job_uid
                and attempt.get("attempt_no") == attempt_no
                for attempt in self._attempts.values()
            ):
                raise TaskLiteError(f"Execution for {job_uid} attempt {attempt_no} is already settled")

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
                remaining = [j for j in new_queue if job_uid_from_snapshot(j) != job_uid]
                if spawned_job_snapshots:
                    remaining_uids = {job_uid_from_snapshot(j) for j in remaining}
                    seen: set[str] = set()
                    for sj in spawned_job_snapshots:
                        suid = job_uid_from_snapshot(sj)
                        if suid in remaining_uids or suid in seen:
                            raise TaskLiteError(
                                f"Spawned job {suid} conflicts with queue for {job_uid}"
                            )
                        seen.add(suid)
                    for s in reversed(spawned_job_snapshots):
                        s_copy = copy.deepcopy(s)
                        s_copy.setdefault("status", "pending")
                        remaining.insert(0, s_copy)
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
                new_queue = [j for j in new_queue if job_uid_from_snapshot(j) != job_uid]
                new_failed[job_uid] = copy.deepcopy(failure_metadata or {})
                if payload is not None:
                    new_failure_payloads[job_uid] = copy.deepcopy(payload)
                new_successes.pop(job_uid, None)

            elif outcome == "retry":
                found = False
                for idx, j in enumerate(new_queue):
                    if job_uid_from_snapshot(j) == job_uid:
                        target = copy.deepcopy(requeued_job_snapshot if requeued_job_snapshot is not None else j)
                        target["status"] = "pending"
                        target["run_id"] = None
                        target["attempt_no"] = target.get("attempt_no") or 1
                        target["started_at"] = None
                        target.pop(QUEUE_EXECUTION_ID_KEY, None)
                        new_queue[idx] = target
                        found = True
                        break
                if not found and requeued_job_snapshot is not None:
                    target = copy.deepcopy(requeued_job_snapshot)
                    target["status"] = "pending"
                    new_queue.append(target)

            elif outcome == "pending":
                for idx, j in enumerate(new_queue):
                    if job_uid_from_snapshot(j) == job_uid:
                        target = copy.deepcopy(j)
                        target["status"] = "pending"
                        target["run_id"] = None
                        target["attempt_no"] = target.get("attempt_no") or 1
                        target["started_at"] = None
                        target.pop(QUEUE_EXECUTION_ID_KEY, None)
                        new_queue[idx] = target
                        break

            # 统一原子落变
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
        norm_path = os.path.normpath(os.fspath(path))
        if target_type not in ("file", "directory"):
            raise TaskLiteError(
                f"target_type must be 'file' or 'directory', got {target_type!r}"
            )
        ts = authorized_at if authorized_at is not None else utc_now_iso()
        with self._lock:
            match = evaluate_authorization(
                self._existing_auth_pairs(),
                norm_path,
                target_type,
            )
            if match.grant_conflict is not None:
                raise TaskLiteError(match.grant_conflict)
            record = {
                "path": norm_path,
                "target_type": target_type,
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
        norm_path = os.path.normpath(os.fspath(path))
        if target_type not in ("file", "directory"):
            raise ValueError(f"target_type must be 'file' or 'directory', got {target_type!r}")
        ts = authorized_at if authorized_at is not None else utc_now_iso()
        with self._lock:
            match = evaluate_authorization(
                self._existing_auth_pairs(),
                norm_path,
                cast(TargetType, target_type),
            )
            if match.exact_path is not None and match.exact_path != norm_path:
                self._authorizations.pop(match.exact_path, None)
            self._authorizations[norm_path] = {
                "path": norm_path,
                "target_type": target_type,
                "authorized_at": ts,
            }

    def list_authorizations(self) -> list[dict[str, Any]]:
        with self._lock:
            records = [dict(rec) for rec in self._authorizations.values()]
            return sorted(records, key=lambda r: (r["authorized_at"], r["path"]))

    def revoke_authorization(self, path: str) -> bool:
        norm_path = os.path.normpath(os.fspath(path))
        with self._lock:
            match = evaluate_authorization(self._existing_auth_pairs(), norm_path)
            if match.exact_path is None:
                return False
            self._authorizations.pop(match.exact_path, None)
            return True

    def is_path_authorized(self, path: str, target_type: str) -> bool:
        norm_path = os.path.normpath(os.fspath(path))
        if target_type not in ("file", "directory"):
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
