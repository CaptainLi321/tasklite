"""v2 纯内存状态后端（InMemoryStateBackend）。

实现 AbstractStateBackend 完整契约，提供零文件系统 IO 的纯内存状态
存储：瞬态管线、单元测试、沙盒执行。快照隔离的 delta 事务语义与
SQLite 腿对齐——「校验先行、统一落变」模拟事务回滚：返回 False 或
抛异常时全部集合与调用前完全一致。
"""
from __future__ import annotations

import copy
import logging
import threading
from dataclasses import replace
from typing import Any, Callable, Mapping, Sequence

from .base import (
    AbstractStateBackend,
    validate_attempt_dispatch,
    validate_attempt_finish,
    validate_queue_replacement,
)
from ..models.attempt import AttemptRecord
from ..models.state import job_uid_from_snapshot

logger = logging.getLogger("tasklite")


class InMemoryStateBackend(AbstractStateBackend):
    """纯内存状态后端：单锁串行化 + 深拷贝隔离的快照语义。

    所有 load_* 返回深拷贝、所有写路径对入参深拷贝——外部可变别名
    不得穿透快照隔离。attempts 以 dict 模拟自增主键（id 严格递增，
    插入序即 id 序）。
    """

    def __init__(self, *, schema_version: int = 5) -> None:
        self.schema_version = schema_version
        self._lock = threading.RLock()
        self._successes: dict[str, dict[str, Any]] = {}
        self._failed: dict[str, dict[str, Any]] = {}
        self._failure_payloads: dict[str, dict[str, Any]] = {}
        self._cursors: dict[str, str] = {}
        self._queue: list[dict[str, Any]] = []
        self._engine_metadata: dict[str, str] = {}
        self._attempts: dict[int, Any] = {}
        self._attempt_seq = 1
        self._job_sequences: dict[str, int] = {}

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
            return copy.deepcopy(self._queue)

    # 整表重写 -------------------------------------------------------

    def _dedup_copy(self, job_snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """整表替换行的单一出口（save_queue 与 replace_queue_atomic 共用）。

        替换集先经 ``validate_queue_replacement`` 校验（与 SQLite 腿同一
        出口、同一时机——赋值之前 fail-loud，``_queue`` 保持调用前状态）。
        保存兜底去重：重复 uid 保留首条 + 告警，与 SQLite 腿
        ``_rewrite_queue_rows`` 同语义；条目一律 deepcopy，杜绝外部可变
        别名穿透快照隔离。
        """
        validate_queue_replacement(job_snapshots)
        seen: set[str] = set()
        clean: list[dict[str, Any]] = []
        for j in job_snapshots:
            u = job_uid_from_snapshot(j)
            if u in seen:
                logger.warning(f"save_queue: duplicate uid {u} dropped (kept first).")
                continue
            seen.add(u)
            clean.append(copy.deepcopy(j))
        return clean

    def save_queue(self, job_snapshots: list[dict[str, Any]]) -> None:
        with self._lock:
            self._queue = self._dedup_copy(job_snapshots)

    def replace_queue_atomic(
        self,
        compute: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    ) -> None:
        with self._lock:
            # 锁内读真相 → 纯计算 → 替换：compute 抛异常时 _queue 赋值
            # 未发生，队列保持调用前状态（对齐 SQLite 腿事务回滚）。
            self._queue = self._dedup_copy(compute(copy.deepcopy(self._queue)))

    # delta 提交 -----------------------------------------------------

    def commit_job_success(
        self,
        job_uid: str,
        result_metadata: dict,
        *,
        attempt_id: int | None = None,
        execution_id: str | None = None,
        finished_at: str | None = None,
        spawned_job_snapshots: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
    ) -> bool:
        with self._lock:
            try:
                # 校验先行：全部 deepcopy、attempts 行校验与冲突判定在任何变更前完成。
                # 不变式：返回 False / 抛异常 ⇒ successes/attempts/queue/failed/cursors 与
                # 调用前完全一致（对齐 SQLite 事务回滚；store 的连续失败
                # 崩溃契约以「后端未变」为前提做重启重建）。
                if not isinstance(attempt_id, int) or isinstance(attempt_id, bool):
                    logger.critical(
                        f"commit_job_success for {job_uid}: attempt_id must be an int, got {attempt_id!r}"
                    )
                    return False
                if execution_id is not None and (not isinstance(execution_id, str) or not execution_id):
                    logger.critical(
                        f"commit_job_success for {job_uid}: execution_id must be a non-empty str or None, got {execution_id!r}"
                    )
                    return False
                validate_attempt_finish("succeeded", finished_at, None)
                current_attempt = self._attempts.get(attempt_id)
                if current_attempt is None:
                    logger.critical(
                        f"commit_job_success for {job_uid}: attempt {attempt_id} does not exist"
                    )
                    return False
                if current_attempt.job_uid != job_uid:
                    logger.critical(
                        f"commit_job_success for {job_uid}: attempt {attempt_id} job_uid mismatch "
                        f"(expected {job_uid!r}, got {current_attempt.job_uid!r})"
                    )
                    return False
                if execution_id is not None and current_attempt.execution_id != execution_id:
                    logger.critical(
                        f"commit_job_success for {job_uid}: attempt {attempt_id} execution_id mismatch "
                        f"(expected {execution_id!r}, got {current_attempt.execution_id!r})"
                    )
                    return False
                if current_attempt.outcome != "running":
                    logger.critical(
                        f"commit_job_success for {job_uid}: attempt {attempt_id} already finalized "
                        f"with outcome {current_attempt.outcome!r}"
                    )
                    return False
                updated_attempt = replace(
                    current_attempt,
                    outcome="succeeded",
                    finished_at=finished_at,
                    error_message=None,
                )
                success_metadata = copy.deepcopy(result_metadata or {})
                spawned_copy = [copy.deepcopy(j) for j in spawned_job_snapshots]
                remaining = [j for j in self._queue if job_uid_from_snapshot(j) != job_uid]
                if spawned_copy:
                    remaining_uids = {job_uid_from_snapshot(j) for j in remaining}
                    seen: set[str] = set()
                    for sj in spawned_copy:
                        suid = job_uid_from_snapshot(sj)
                        # spawned uid 撞上删除 popped 后的队列既有条目，或
                        # 批内自相重复，均判定内存/磁盘漂移 → False 走崩溃
                        # 契约，绝不产出重复队列条目（对齐 SQLite
                        # INSERT OR IGNORE + rowcount 漂移检测）。
                        if suid in remaining_uids or suid in seen:
                            logger.critical(
                                f"commit_job_success for {job_uid}: spawned job {suid} "
                                f"conflicts with queue (drift). Returning False."
                            )
                            return False
                        seen.add(suid)
                cursor_sets: dict[str, str] = {}
                cursor_dels: set[str] = set()
                if cursor_updates:
                    for k, v in cursor_updates.items():
                        if v is None:
                            cursor_dels.add(k)
                        else:
                            cursor_sets[k] = str(v)
                # 校验全部通过，统一落变（落变段不再调用任何可失败操作）
                self._attempts[attempt_id] = updated_attempt
                self._successes[job_uid] = success_metadata
                self._queue = spawned_copy + remaining  # 队首插入 spawned 并保序
                # 成功 commit 清理 failed 同名残行：与「job 最终状态唯一」
                # 语义一致，防 successes∩failed 并存。
                self._failed.pop(job_uid, None)
                self._failure_payloads.pop(job_uid, None)
                self._cursors.update(cursor_sets)
                for k in cursor_dels:
                    self._cursors.pop(k, None)
                return True
            except Exception as e:
                logger.critical(f"Failed to commit job success for {job_uid}: {e}")
                return False

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
                return False

    def commit_retry(
        self,
        popped_job_uid: str,
        requeued_job_snapshot: dict[str, Any],
        *,
        front: bool = False,
    ) -> bool:
        with self._lock:
            try:
                requeued_copy = copy.deepcopy(requeued_job_snapshot)
                ruid = job_uid_from_snapshot(requeued_copy)
                remaining = [j for j in self._queue if job_uid_from_snapshot(j) != popped_job_uid]
                # 对齐 SQLite 普通 INSERT 冲突回滚：requeued uid 撞上删除
                # popped 后的既有条目 → False 走崩溃契约，绝不产出重复条目；
                # uid == popped_job_uid 属同 job 重插（DELETE+INSERT），安全放行。
                if ruid != popped_job_uid and any(job_uid_from_snapshot(j) == ruid for j in remaining):
                    logger.critical(
                        f"commit_retry for {popped_job_uid}: requeued uid {ruid} "
                        f"conflicts with existing queue entry. Returning False."
                    )
                    return False
                if front:
                    self._queue = [requeued_copy] + remaining
                else:
                    self._queue = remaining + [requeued_copy]
                return True
            except Exception as e:
                logger.critical(f"Failed to commit retry for {popped_job_uid}: {e}")
                return False

    def commit_job_failures(self, job_uids_metadata: list[tuple[str, dict]]) -> bool:
        with self._lock:
            try:
                # 校验先行：全部失败行拷贝在任何变更前完成——任一行失败则
                # queue/successes/failed 整体不变（对齐 SQLite 事务回滚，杜绝
                # 「队列已整体删除、档案未落」的半成品失败态）。
                entries = [(job_uid, copy.deepcopy(meta or {})) for job_uid, meta in job_uids_metadata]
                fail_uids = {u for u, _ in job_uids_metadata}
                remaining = [j for j in self._queue if job_uid_from_snapshot(j) not in fail_uids]
                for job_uid, meta in entries:
                    self._failed[job_uid] = meta
                    # 级联/死锁批量失败时 successes 旧成功记录作废（最终状态
                    # 唯一），与 commit_job_failure 对称。
                    self._successes.pop(job_uid, None)
                self._queue = remaining
                return True
            except Exception as e:
                logger.critical(f"Failed to commit bulk failure: {e}")
                return False

    def commit_skip(self, job_uid: str) -> bool:
        with self._lock:
            self._queue = [j for j in self._queue if job_uid_from_snapshot(j) != job_uid]
            return True

    def append_failed(self, job_uid: str, metadata: dict[str, Any] | None = None) -> None:
        with self._lock:
            self._failed[job_uid] = copy.deepcopy(metadata or {})

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

    # 定向删除与种子 --------------------------------------------------

    def delete_queue_uids(self, job_uids: list[str]) -> int:
        """按 uid 定向批量删除队列条目，与 SQLite 腿同语义：不触碰其余条目。"""
        if not job_uids:
            return 0
        with self._lock:
            del_set = set(job_uids)
            kept = [j for j in self._queue if job_uid_from_snapshot(j) not in del_set]
            removed = len(self._queue) - len(kept)
            self._queue = kept
            return removed

    def delete_failed(self, job_uids: list[str]) -> int:
        with self._lock:
            del_set = set(job_uids)
            count = 0
            for u in del_set:
                if u in self._failed:
                    del self._failed[u]
                    self._failure_payloads.pop(u, None)
                    count += 1
            return count

    def delete_successes(self, job_uids: list[str]) -> int:
        with self._lock:
            del_set = set(job_uids)
            count = 0
            for u in del_set:
                if u in self._successes:
                    del self._successes[u]
                    count += 1
            return count

    def seed_successes(self, job_uids: list[str]) -> int:
        """把 uid 批量写入 successes（meta 空 dict）。

        不变式：successes/failed 全局互斥——已在 failed 的 uid 拒绝种子；
        先查后写（同锁内），冲突整体拒绝、零写入，不静默清除失败档案
        记录（失败历史须先经 delete_failed 显式清除）。
        """
        with self._lock:
            conflict = sorted({u for u in job_uids if u in self._failed})
            if conflict:
                raise ValueError(
                    f"seed_successes refuses uid(s) already in failed: {conflict}; "
                    f"successes/failed must stay disjoint"
                )
            count = 0
            for u in job_uids:
                self._successes[u] = {}
                count += 1
            return count

    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（UPSERT，幂等）。

        与 SQLiteStateBackend.seed_cursor 同契约 fail-loud：key 非空 str、
        value 必须 str——静默 str() 强转会让同一非法入参在本腿写数字串、
        sqlite 腿抛异常（跨后端行为分歧），且与内存镜像值型漂移。
        """
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        if not isinstance(value, str):
            raise TypeError(f"cursor value must be str, got {type(value).__name__}")
        with self._lock:
            self._cursors[key] = value

    # 尝试轨迹（append-only 旁路观测面）--------------------------------

    def append_attempt(self, record: AttemptRecord) -> int:
        """派发即插行：分配自增轨迹 id 并落内存表（单语句语义，无读-改-写窗口）。"""
        validate_attempt_dispatch(record)
        with self._lock:
            attempt_id = self._attempt_seq
            self._attempt_seq += 1
            self._attempts[attempt_id] = record
            return attempt_id

    def update_attempt(
        self,
        attempt_id: int,
        *,
        outcome: str,
        finished_at: str,
        error_message: str | None = None,
    ) -> bool:
        """收尾轨迹行：仅允许把 running 行收敛为终态一次。

        读-判-写在锁内串行完成；目标 id 不存在返回 False，对已收尾行
        再次收尾抛 ValueError（双重终结是调用方协议缺陷，fail-loud）。
        """
        validate_attempt_finish(outcome, finished_at, error_message)
        with self._lock:
            current = self._attempts.get(attempt_id)
            if current is None:
                return False
            if current.outcome != "running":
                raise ValueError(
                    f"attempt {attempt_id} already finalized with outcome "
                    f"{current.outcome!r}; double finalization is a protocol violation"
                )
            self._attempts[attempt_id] = replace(
                current, outcome=outcome, finished_at=finished_at, error_message=error_message
            )
            return True

    def load_attempts(self, job_uid: str) -> list[AttemptRecord]:
        """按 job_uid 读取轨迹（dict 插入序 = 自增 id 序）。"""
        with self._lock:
            return [rec for rec in self._attempts.values() if rec.job_uid == job_uid]

    def load_latest_succeeded_finished_at(self, job_uid: str) -> str | None:
        """按 attempt_id 倒序查找指定 job_uid 最新一条 succeeded 轨迹行的 finished_at。"""
        with self._lock:
            for rec in reversed(self._attempts.values()):
                if rec.job_uid == job_uid and rec.outcome == "succeeded":
                    if isinstance(rec.finished_at, str) and rec.finished_at:
                        return rec.finished_at
                    return None
            return None

    # 元数据 ---------------------------------------------------------

    def get_engine_metadata(self, key: str) -> str | None:
        with self._lock:
            return self._engine_metadata.get(key)

    def set_engine_metadata(self, key: str, value: str) -> None:
        with self._lock:
            self._engine_metadata[key] = str(value)

    # 状态事务与编号（ADR-0011 目标格式与执行结算内部准备）───────────

    def mark_job_running(self, job_uid: str, *, run_id: str, started_at: str) -> int:
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty str")
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

            updated_job = copy.deepcopy(job_data)
            updated_job["status"] = "running"
            updated_job["run_id"] = run_id
            updated_job["attempt_no"] = next_no
            updated_job["started_at"] = started_at
            self._queue[job_idx] = updated_job
            return next_no

    def get_attempt_sequence(self, job_uid: str) -> int:
        with self._lock:
            return self._job_sequences.get(job_uid, 0)

    def clear_attempts(self, job_uid: str) -> int:
        with self._lock:
            initial = len(self._attempts)
            self._attempts = {
                k: v for k, v in self._attempts.items()
                if (v.job_uid if isinstance(v, AttemptRecord) else v.get("job_uid")) != job_uid
            }
            return initial - len(self._attempts)

    def load_settled_attempts(self, job_uid: str) -> list[dict[str, Any]]:
        with self._lock:
            entries: list[dict[str, Any]] = []
            for att in self._attempts.values():
                if isinstance(att, dict) and att.get("job_uid") == job_uid:
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
                    "job": copy.deepcopy(j),
                    "status": j_status,
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
    ) -> bool:
        if outcome not in ("success", "failure", "retry", "pending"):
            raise ValueError(f"Invalid outcome: {outcome}")
        if not isinstance(attempt_no, int) or isinstance(attempt_no, bool) or attempt_no < 1:
            raise TypeError(f"attempt_no must be int >= 1, got {attempt_no!r}")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError(f"run_id must be non-empty str, got {run_id!r}")
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
            new_attempts = dict(self._attempts)
            new_attempt_id = self._attempt_seq
            attempt_outcome = outcome if outcome in ("success", "failure", "retry") else "failure"
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
                            logger.critical(
                                f"commit_job_settlement for {job_uid}: spawned job {suid} "
                                f"conflicts with queue (drift). Returning False."
                            )
                            return False
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
                        target["attempt_no"] = None
                        target["started_at"] = None
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
                        target["attempt_no"] = None
                        target["started_at"] = None
                        new_queue[idx] = target
                        break

            # 统一原子落变
            self._attempts = new_attempts
            self._attempt_seq += 1
            self._queue = new_queue
            self._successes = new_successes
            self._failed = new_failed
            self._failure_payloads = new_failure_payloads
            self._cursors = new_cursors
            return True


__all__ = ["InMemoryStateBackend"]
