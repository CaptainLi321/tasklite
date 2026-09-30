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
from ..models.state import uid_from_job_dict

logger = logging.getLogger("tasklite.v2")


class InMemoryStateBackend(AbstractStateBackend):
    """纯内存状态后端：单锁串行化 + 深拷贝隔离的快照语义。

    所有 load_* 返回深拷贝、所有写路径对入参深拷贝——外部可变别名
    不得穿透快照隔离。attempts 以 dict 模拟自增主键（id 严格递增，
    插入序即 id 序）。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._wall: dict[str, dict[str, Any]] = {}
        self._failed: dict[str, dict[str, Any]] = {}
        self._failed_payloads: dict[str, dict[str, Any]] = {}
        self._cursors: dict[str, str] = {}
        self._queue: list[dict[str, Any]] = []
        self._meta: dict[str, str] = {}
        self._attempts: dict[int, AttemptRecord] = {}
        self._attempt_seq = 1

    # 全量加载 -------------------------------------------------------

    def load_wall(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._wall)

    def load_failed(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._failed)

    def load_failed_payloads(self) -> dict[str, dict[str, Any]]:
        """读取失败档案各 uid 的原始业务 payload 快照（无快照的 uid 不出现）。"""
        with self._lock:
            return copy.deepcopy(self._failed_payloads)

    def load_cursors(self) -> dict[str, str]:
        with self._lock:
            return dict(self._cursors)

    def load_queue(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._queue)

    # 整表重写 -------------------------------------------------------

    def _dedup_copy(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """整表替换行的单一出口（save_queue 与 replace_queue_atomic 共用）。

        替换集先经 ``validate_queue_replacement`` 校验（与 SQLite 腿同一
        出口、同一时机——赋值之前 fail-loud，``_queue`` 保持调用前状态）。
        保存兜底去重：重复 uid 保留首条 + 告警，与 SQLite 腿
        ``_rewrite_queue_rows`` 同语义；条目一律 deepcopy，杜绝外部可变
        别名穿透快照隔离。
        """
        validate_queue_replacement(jobs)
        seen: set[str] = set()
        clean: list[dict[str, Any]] = []
        for j in jobs:
            u = uid_from_job_dict(j)
            if u in seen:
                logger.warning(f"save_queue: duplicate uid {u} dropped (kept first).")
                continue
            seen.add(u)
            clean.append(copy.deepcopy(j))
        return clean

    def save_queue(self, jobs: list[dict[str, Any]]) -> None:
        with self._lock:
            self._queue = self._dedup_copy(jobs)

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
        uid: str,
        result_meta: dict,
        *,
        spawned_jobs: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
    ) -> bool:
        with self._lock:
            try:
                # 校验先行：全部 deepcopy 与冲突判定在任何变更前完成。
                # 不变式：返回 False / 抛异常 ⇒ wall/queue/failed/cursors 与
                # 调用前完全一致（对齐 SQLite 事务回滚；store 的连续失败
                # 崩溃契约以「后端未变」为前提做重启重建）。
                wall_meta = copy.deepcopy(result_meta or {})
                spawned_copy = [copy.deepcopy(j) for j in spawned_jobs]
                remaining = [j for j in self._queue if uid_from_job_dict(j) != uid]
                if spawned_copy:
                    remaining_uids = {uid_from_job_dict(j) for j in remaining}
                    seen: set[str] = set()
                    for sj in spawned_copy:
                        suid = uid_from_job_dict(sj)
                        # spawned uid 撞上删除 popped 后的队列既有条目，或
                        # 批内自相重复，均判定内存/磁盘漂移 → False 走崩溃
                        # 契约，绝不产出重复队列条目（对齐 SQLite
                        # INSERT OR IGNORE + rowcount 漂移检测）。
                        if suid in remaining_uids or suid in seen:
                            logger.critical(
                                f"commit_job_success for {uid}: spawned job {suid} "
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
                self._wall[uid] = wall_meta
                self._queue = spawned_copy + remaining  # 队首插入 spawned 并保序
                # 成功 commit 清理 failed 同名残行：与「job 最终状态唯一」
                # 语义一致，防 wall∩failed 并存。
                self._failed.pop(uid, None)
                self._failed_payloads.pop(uid, None)
                self._cursors.update(cursor_sets)
                for k in cursor_dels:
                    self._cursors.pop(k, None)
                return True
            except Exception as e:
                logger.critical(f"Failed to commit job success for {uid}: {e}")
                return False

    def commit_job_failure(
        self, uid: str, result_meta: dict, job_payload: dict[str, Any] | None = None
    ) -> bool:
        with self._lock:
            try:
                # 校验先行：失败 meta 与 payload 快照的拷贝在变更前完成，
                # 任一步失败 ⇒ failed/queue/wall 整体不变（对齐 SQLite 回滚）。
                meta_copy = copy.deepcopy(result_meta or {})
                payload_copy = (
                    copy.deepcopy(job_payload) if job_payload is not None else None
                )
                remaining = [j for j in self._queue if uid_from_job_dict(j) != uid]
                self._failed[uid] = meta_copy
                # payload 快照 latest-wins；None 表示本路径无 payload，保留
                # 既有快照（与 SQLite 腿 _write_failed_row 的保留语义对齐）。
                if payload_copy is not None:
                    self._failed_payloads[uid] = payload_copy
                self._queue = remaining
                # 同一事务语义内清理 wall 旧记录：重跑任务失败时旧成功
                # 记录作废（最终状态唯一），防 wall∩failed 并存。
                self._wall.pop(uid, None)
                return True
            except Exception as e:
                logger.critical(f"Failed to commit job failure for {uid}: {e}")
                return False

    def commit_retry(
        self,
        popped_uid: str,
        requeued_job: dict[str, Any],
        *,
        front: bool = False,
    ) -> bool:
        with self._lock:
            try:
                requeued_copy = copy.deepcopy(requeued_job)
                ruid = uid_from_job_dict(requeued_copy)
                remaining = [j for j in self._queue if uid_from_job_dict(j) != popped_uid]
                # 对齐 SQLite 普通 INSERT 冲突回滚：requeued uid 撞上删除
                # popped 后的既有条目 → False 走崩溃契约，绝不产出重复条目；
                # uid == popped_uid 属同 job 重插（DELETE+INSERT），安全放行。
                if ruid != popped_uid and any(uid_from_job_dict(j) == ruid for j in remaining):
                    logger.critical(
                        f"commit_retry for {popped_uid}: requeued uid {ruid} "
                        f"conflicts with existing queue entry. Returning False."
                    )
                    return False
                if front:
                    self._queue = [requeued_copy] + remaining
                else:
                    self._queue = remaining + [requeued_copy]
                return True
            except Exception as e:
                logger.critical(f"Failed to commit retry for {popped_uid}: {e}")
                return False

    def commit_bulk_failure(self, uids_metas: list[tuple[str, dict]]) -> bool:
        with self._lock:
            try:
                # 校验先行：全部失败行拷贝在任何变更前完成——任一行失败则
                # queue/wall/failed 整体不变（对齐 SQLite 事务回滚，杜绝
                # 「队列已整体删除、档案未落」的半成品失败态）。
                entries = [(uid, copy.deepcopy(meta or {})) for uid, meta in uids_metas]
                fail_uids = {u for u, _ in uids_metas}
                remaining = [j for j in self._queue if uid_from_job_dict(j) not in fail_uids]
                for uid, meta in entries:
                    self._failed[uid] = meta
                    # 级联/死锁批量失败时 wall 旧成功记录作废（最终状态
                    # 唯一），与 commit_job_failure 对称。
                    self._wall.pop(uid, None)
                self._queue = remaining
                return True
            except Exception as e:
                logger.critical(f"Failed to commit bulk failure: {e}")
                return False

    def commit_skip(self, uid: str) -> bool:
        with self._lock:
            self._queue = [j for j in self._queue if uid_from_job_dict(j) != uid]
            return True

    def append_failed(self, uid: str, payload: dict[str, Any] | None = None) -> None:
        with self._lock:
            self._failed[uid] = copy.deepcopy(payload or {})

    # 增量入队 -------------------------------------------------------

    def enqueue_jobs(self, jobs: list[dict[str, Any]], *, front: bool = False) -> list[str]:
        if not jobs:
            return []
        with self._lock:
            existing = {uid_from_job_dict(j) for j in self._queue}
            fresh: list[dict[str, Any]] = []
            batch_seen: set[str] = set()
            for j in jobs:
                u = uid_from_job_dict(j)
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
            return [uid_from_job_dict(j) for j in fresh]

    # 定向删除与种子 --------------------------------------------------

    def delete_queue_uids(self, uids: list[str]) -> int:
        """按 uid 定向批量删除队列条目，与 SQLite 腿同语义：不触碰其余条目。"""
        if not uids:
            return 0
        with self._lock:
            del_set = set(uids)
            kept = [j for j in self._queue if uid_from_job_dict(j) not in del_set]
            removed = len(self._queue) - len(kept)
            self._queue = kept
            return removed

    def delete_failed(self, uids: list[str]) -> int:
        with self._lock:
            del_set = set(uids)
            count = 0
            for u in del_set:
                if u in self._failed:
                    del self._failed[u]
                    self._failed_payloads.pop(u, None)
                    count += 1
            return count

    def delete_wall(self, uids: list[str]) -> int:
        with self._lock:
            del_set = set(uids)
            count = 0
            for u in del_set:
                if u in self._wall:
                    del self._wall[u]
                    count += 1
            return count

    def seed_wall(self, uids: list[str]) -> int:
        """把 uid 批量写入 wall（meta 空 dict）。

        不变式：wall/failed 全局互斥——已在 failed 的 uid 拒绝种子；
        先查后写（同锁内），冲突整体拒绝、零写入，不静默清除失败档案
        记录（失败历史须先经 delete_failed 显式清除）。
        """
        with self._lock:
            conflict = sorted({u for u in uids if u in self._failed})
            if conflict:
                raise ValueError(
                    f"seed_wall refuses uid(s) already in failed: {conflict}; "
                    f"wall/failed must stay disjoint"
                )
            count = 0
            for u in uids:
                self._wall[u] = {}
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
        error: str | None = None,
    ) -> bool:
        """收尾轨迹行：仅允许把 running 行收敛为终态一次。

        读-判-写在锁内串行完成；目标 id 不存在返回 False，对已收尾行
        再次收尾抛 ValueError（双重终结是调用方协议缺陷，fail-loud）。
        """
        validate_attempt_finish(outcome, finished_at, error)
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
                current, outcome=outcome, finished_at=finished_at, error=error
            )
            return True

    def load_attempts(self, job_uid: str) -> list[AttemptRecord]:
        """按 job_uid 读取轨迹（dict 插入序 = 自增 id 序）。"""
        with self._lock:
            return [rec for rec in self._attempts.values() if rec.job_uid == job_uid]

    # 元数据 ---------------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        with self._lock:
            return self._meta.get(key)

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._meta[key] = str(value)


__all__ = ["InMemoryStateBackend"]
