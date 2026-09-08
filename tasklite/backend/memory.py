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

    def _write_dlq_entry(self, uid: str, meta: dict) -> None:
        """DLQ 写入单一出口：维护 _attempt 计数、error_type 与 failed_at。"""
        new_meta = copy.deepcopy(meta or {})
        prev = self._failed.get(uid, {})
        prev_attempts = prev.get("_attempt", 0) if isinstance(prev, dict) else 0
        new_meta["_attempt"] = prev_attempts + 1

        if "error_type" not in new_meta:
            new_meta["error_type"] = classify_error_type(new_meta)

        if "failed_at" not in new_meta:
            new_meta["failed_at"] = datetime.now(timezone.utc).isoformat()

        self._failed[uid] = new_meta

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
                self._wall[uid] = copy.deepcopy(result_meta or {})
                self._queue = [j for j in self._queue if uid_from_job_dict(j) != uid]
                self._failed.pop(uid, None)

                if spawned_jobs:
                    existing_uids = {uid_from_job_dict(j) for j in self._queue}
                    for sj in spawned_jobs:
                        suid = uid_from_job_dict(sj)
                        if suid in existing_uids:
                            logger.critical(
                                f"commit_job_success for {uid}: spawned job {suid} "
                                f"conflicts with queue (drift). Returning False."
                            )
                            return False
                    # 队首插入 spawned_jobs 并保序
                    self._queue[0:0] = [copy.deepcopy(j) for j in spawned_jobs]

                if cursor_updates:
                    for k, v in cursor_updates.items():
                        if v is None:
                            self._cursors.pop(k, None)
                        else:
                            self._cursors[k] = str(v)
                return True
            except Exception as e:
                logger.critical(f"Failed to commit job success for {uid}: {e}")
                return False

    def commit_job_failure(self, uid: str, result_meta: dict) -> bool:
        with self._lock:
            try:
                self._write_dlq_entry(uid, result_meta)
                self._queue = [j for j in self._queue if uid_from_job_dict(j) != uid]
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
                self._queue = [j for j in self._queue if uid_from_job_dict(j) != popped_uid]
                if front:
                    self._queue.insert(0, copy.deepcopy(requeued_job))
                else:
                    self._queue.append(copy.deepcopy(requeued_job))
                return True
            except Exception as e:
                logger.critical(f"Failed to commit retry for {popped_uid}: {e}")
                return False

    def commit_bulk_failure(self, uids_metas: List[Tuple[str, dict]]) -> bool:
        with self._lock:
            try:
                fail_uids = {u for u, _ in uids_metas}
                self._queue = [j for j in self._queue if uid_from_job_dict(j) not in fail_uids]
                for uid, meta in uids_metas:
                    self._write_dlq_entry(uid, meta)
                    self._wall.pop(uid, None)
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
