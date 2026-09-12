"""纯内存状态后端适配器（InMemoryStateBackend）。

实现 AbstractStateBackend 完整契约，提供零文件系统 IO 的纯内存状态存储。
适用于瞬态管线、单元测试、沙盒执行与 CI 矩阵测试。
"""
from __future__ import annotations

import copy
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .base import AbstractStateBackend, classify_error_type
from ..models.state import uid_from_job_dict

logger = logging.getLogger("tasklite")


class InMemoryStateBackend(AbstractStateBackend):
    """纯内存状态后端。提供快照隔离的 delta 事务语义，与 SQLite 后端行为完全对齐。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._wall: Dict[str, Dict[str, Any]] = {}
        self._failed: Dict[str, Dict[str, Any]] = {}
        self._cursors: Dict[str, str] = {}
        self._queue: List[Dict[str, Any]] = []
        self._meta: Dict[str, str] = {}

    def load_wall(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._wall)

    def load_failed(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._failed)

    def load_cursors(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._cursors)

    def load_queue(self) -> List[Dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._queue)

    def save_queue(self, jobs: List[Dict[str, Any]]) -> None:
        with self._lock:
            seen = set()
            clean = []
            for j in jobs:
                u = uid_from_job_dict(j)
                if u in seen:
                    logger.warning(f"save_queue: duplicate uid {u} dropped (kept first).")
                    continue
                seen.add(u)
                clean.append(copy.deepcopy(j))
            self._queue = clean

    def _build_dlq_meta(self, meta: Optional[dict], prev: Any) -> Dict[str, Any]:
        """计算 DLQ 行终值（纯函数，不变更任何状态）：_attempt 计数 + error_type + failed_at。

        不变式：``_attempt`` 是写入事件计数而非逻辑失败次数；既有计数损坏
        （非 dict 记录或非 int 计数）时静默重置为 1 并照常提交，绝不因脏计数
        抛 TypeError——与 SQLite 后端「损坏行重置计数、写入成功」行为对齐。
        ``merged`` 已显式携带 ``_attempt`` 时保留调用方值（计数由最先 DLQ
        该 uid 的路径权威给定）。
        """
        merged = copy.deepcopy(meta or {})
        if "error_type" not in merged:
            merged["error_type"] = classify_error_type(merged)
        if "failed_at" not in merged:
            merged["failed_at"] = datetime.now(timezone.utc).isoformat()
        if not isinstance(prev, dict):
            merged["_attempt"] = 1
            return merged
        prev_attempt = prev.get("_attempt")
        if isinstance(prev_attempt, int):
            merged["_attempt"] = prev_attempt + 1
        elif "_attempt" not in merged:
            merged["_attempt"] = 1
        return merged

    def _write_dlq_entry(self, uid: str, meta: Optional[dict]) -> None:
        """DLQ 写入单一出口：终值经 _build_dlq_meta 计算后落变。"""
        self._failed[uid] = self._build_dlq_meta(meta, self._failed.get(uid))

    def commit_job_success(
        self,
        uid: str,
        result_meta: dict,
        *,
        spawned_jobs: List[Dict[str, Any]] = (),
        cursor_updates: Optional[Dict[str, str]] = None,
    ) -> bool:
        with self._lock:
            try:
                # 校验先行：全部 deepcopy 与冲突判定在任何变更前完成。
                # 不变式：返回 False / 抛异常 ⇒ wall/queue/failed/cursors 与
                # 调用前完全一致（对齐 SQLite 事务回滚；store 的 3-strike
                # 崩溃契约以「后端未变」为前提做重启重建）。
                wall_meta = copy.deepcopy(result_meta or {})
                spawned_copy = [copy.deepcopy(j) for j in spawned_jobs]
                remaining = [j for j in self._queue if uid_from_job_dict(j) != uid]
                if spawned_copy:
                    remaining_uids = {uid_from_job_dict(j) for j in remaining}
                    seen: set = set()
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
                cursor_sets: Dict[str, str] = {}
                cursor_dels: set = set()
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
                # 语义一致，防 wall∩failed 并存污染 _attempt 计数。
                self._failed.pop(uid, None)
                self._cursors.update(cursor_sets)
                for k in cursor_dels:
                    self._cursors.pop(k, None)
                return True
            except Exception as e:
                logger.critical(f"Failed to commit job success for {uid}: {e}")
                return False

    def commit_job_failure(self, uid: str, result_meta: dict) -> bool:
        with self._lock:
            try:
                # 校验先行：DLQ 终值计算与队列 uid 提取全部在变更前完成，
                # 任一步失败 ⇒ failed/queue/wall 整体不变（对齐 SQLite 回滚）。
                new_meta = self._build_dlq_meta(result_meta, self._failed.get(uid))
                remaining = [j for j in self._queue if uid_from_job_dict(j) != uid]
                self._failed[uid] = new_meta
                self._queue = remaining
                # 同一事务语义内清理 wall 旧记录：rerun 任务重跑失败时旧成功
                # 记录作废（最终状态唯一），防 wall∩failed 并存。
                self._wall.pop(uid, None)
                return True
            except Exception as e:
                logger.critical(f"Failed to commit job failure for {uid}: {e}")
                return False

    def commit_retry(
        self,
        popped_uid: str,
        requeued_job: Dict[str, Any],
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
                # uid == popped_uid 属同 job 重插，安全放行。
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

    def commit_bulk_failure(self, uids_metas: List[Tuple[str, dict]]) -> bool:
        with self._lock:
            try:
                # 校验先行：全部 DLQ 行计算在任何变更前完成——任一行失败则
                # queue/wall/failed 整体不变（对齐 SQLite 事务回滚，杜绝
                # 「队列已整体删除、DLQ 未落」的半成品失败态）。
                new_entries: List[Tuple[str, Dict[str, Any]]] = []
                overlay: Dict[str, Any] = {}
                for uid, meta in uids_metas:
                    # 批内同 uid 多次出现时模拟 SQLite 同事务顺序写：
                    # 后一行读取前一行结果，_attempt 连续递增。
                    prev = overlay.get(uid, self._failed.get(uid))
                    entry = self._build_dlq_meta(meta, prev)
                    overlay[uid] = entry
                    new_entries.append((uid, entry))
                fail_uids = {u for u, _ in uids_metas}
                remaining = [j for j in self._queue if uid_from_job_dict(j) not in fail_uids]
                for uid, entry in new_entries:
                    self._failed[uid] = entry
                    # rerun 任务被级联/死锁批量 DLQ 时 wall 旧成功记录作废
                    # （最终状态唯一），与 commit_job_failure 对称。
                    self._wall.pop(uid, None)
                self._queue = remaining
                return True
            except Exception as e:
                logger.critical(f"Failed to commit bulk failure: {e}")
                return False

    def append_failed(self, uid: str, payload: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            self._write_dlq_entry(uid, payload or {})

    def commit_skip(self, uid: str) -> bool:
        with self._lock:
            self._queue = [j for j in self._queue if uid_from_job_dict(j) != uid]
            return True

    def get_meta(self, key: str) -> Optional[str]:
        with self._lock:
            return self._meta.get(key)

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._meta[key] = str(value)

    def enqueue_jobs(self, jobs: List[Dict[str, Any]], *, front: bool = False) -> List[str]:
        if not jobs:
            return []
        with self._lock:
            existing = {uid_from_job_dict(j) for j in self._queue}
            fresh: List[Dict[str, Any]] = []
            batch_seen = set()
            for j in jobs:
                u = uid_from_job_dict(j)
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

    def delete_failed(self, uids: List[str]) -> int:
        with self._lock:
            del_set = set(uids)
            count = 0
            for u in del_set:
                if u in self._failed:
                    del self._failed[u]
                    count += 1
            return count

    def delete_wall(self, uids: List[str]) -> int:
        with self._lock:
            del_set = set(uids)
            count = 0
            for u in del_set:
                if u in self._wall:
                    del self._wall[u]
                    count += 1
            return count

    def seed_wall(self, uids: List[str]) -> int:
        with self._lock:
            count = 0
            for u in uids:
                self._wall[u] = {}
                count += 1
            return count

    def seed_cursor(self, key: str, value: str) -> None:
        with self._lock:
            self._cursors[key] = str(value)
