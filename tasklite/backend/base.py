"""Abstract state backend interface for tasklite."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from ..taxonomy import (
    ERR_COMMIT_FAILURE_DLQ,
    ERR_DEADLOCK_GAP,
    ERR_DEPENDENCY_DEADLOCK,
    ERR_DISPATCH_FAILURE,
    ERR_JOB_DEPENDENCY,
    ERR_MALFORMED_JOB,
    ERR_MAX_RETRIES,
    ERR_NO_HANDLER,
    ERR_PAYLOAD_VALIDATION,
    ERR_RESOURCE_DEADLOCK,
    ERROR_TYPE_COMMIT_FAILURE,
    ERROR_TYPE_DEADLOCK,
    ERROR_TYPE_DEPENDENCY,
    ERROR_TYPE_DISPATCH,
    ERROR_TYPE_FATAL,
    ERROR_TYPE_NO_HANDLER,
    ERROR_TYPE_TRANSIENT_EXHAUSTED,
    ERROR_TYPE_UNKNOWN,
    ERROR_TYPE_VALIDATION,
    classify_error_type,
)



class AbstractStateBackend(ABC):
    """Abstract interface for pipeline state persistence.

    Implementations are called only from the single-threaded parent process;
    subprocess workers do not touch the backend.
    """

    @abstractmethod
    def load_wall(self) -> Dict[str, Dict[str, Any]]: ...

    @abstractmethod
    def load_failed(self) -> Dict[str, Dict[str, Any]]: ...

    @abstractmethod
    def load_cursors(self) -> Dict[str, str]: ...

    @abstractmethod
    def load_queue(self) -> List[Dict[str, Any]]: ...

    @abstractmethod
    def save_queue(self, jobs: List[Dict[str, Any]]) -> None: ...

    @abstractmethod
    def commit_job_success(
        self,
        uid: str,
        result_meta: dict,
        *,
        spawned_jobs: List[Dict[str, Any]] = (),
        cursor_updates: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Persist a successful job as a delta, atomically.

        Effects (all in one transaction):
          - wall: INSERT/REPLACE uid -> result_meta
          - queue: DELETE the popped uid
          - queue: INSERT spawned_jobs at FRONT (preserving their order)
          - cursors: merge cursor_updates

        Returns True on success. Returns False if any step failed — on failure
        the backend MUST leave the on-disk queue unchanged (the popped uid
        remains on disk); the caller is responsible for deciding whether to
        re-queue the job. Callers MUST NOT update in-memory wall/cursor state
        when False is returned.
        """

    @abstractmethod
    def commit_job_failure(
        self,
        uid: str,
        result_meta: dict,
    ) -> bool:
        """Persist a failed job to the DLQ as a delta, atomically.

        Effects: failed_dlq INSERT/REPLACE uid -> result_meta; queue DELETE uid.
        Returns True on success. Returns False if persistence failed — on
        failure the backend MUST leave the on-disk queue unchanged (the popped
        uid remains on disk); the caller is responsible for deciding whether to
        re-queue the job. Callers MUST NOT update in-memory failed state when
        False is returned.
        """

    @abstractmethod
    def commit_retry(
        self,
        popped_uid: str,
        requeued_job: Dict[str, Any],
        *,
        front: bool = False,
    ) -> bool:
        """Persist a retry requeue as a delta, atomically.

        Effects: queue DELETE popped_uid; queue INSERT requeued_job at front (if
        ``front=True``) or back. No wall/DLQ writes. Returns True on success.
        Returns False on failure — on-disk queue unchanged (popped uid remains).
        """

    @abstractmethod
    def commit_bulk_failure(
        self,
        uids_metas: List[Tuple[str, dict]],
    ) -> bool:
        """Best-effort bulk mark-failed (deadlock case), as a delta.

        Effects: failed_dlq INSERT/REPLACE each uid -> meta; queue DELETE each
        uid. Returns True if all DLQ writes succeeded and the deletions
        committed. Returns False if ANY DLQ write failed — on-disk queue is
        preserved. SQLite backend is atomic, so it either returns True or
        raises (-> False). Remaining (non-deleted) rows keep their order.
        """

    @abstractmethod
    def append_failed(self, uid: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """Append a record to the DLQ (failed log) without touching the queue.

        Used for out-of-band DLQ appends (e.g. user-driven inspection).
        Implementations should be idempotent on ``uid``.
        """

    @abstractmethod
    def commit_skip(self, uid: str) -> bool:
        """Delta 删除队列中已完成/已失败（重复）的 uid。

        去重命中（uid 已在 wall/failed）时调用——磁盘队列中该 uid 是残留
        条目，直接删除以同步内存/磁盘（内存 = 磁盘 − in-flight）。
        不写 wall/failed（重复条目不改变任何状态，只是清理）。
        返回 True 成功；False 时 on-disk 队列不变，调用方走崩溃契约。
        """

    @abstractmethod
    def delete_queue_uids(self, uids: List[str]) -> int:
        """按 uid 定向批量删除队列行（加载期 repair 的差量落盘唯一出口）。

        与 save_queue 的全表重写相对：只 DELETE 指定 uid 的行，其余行原样
        保留。不变式：删除集在调用前确定，不在删除集内的行（含并发进程
        刚入队的新行）无论与本事务先后提交都必然存活——陈旧加载快照永远
        不会覆盖他进程的新写入。单事务原子；失败抛异常（不吞），磁盘保持
        调用前状态（残留行由下次加载重新判定，天然幂等）。返回实际删除行数。
        """

    @abstractmethod
    def get_meta(self, key: str) -> Optional[str]:
        """读取一条框架级元数据（如 fencing 的 last_run_id）。无则返回 None。"""

    @abstractmethod
    def set_meta(self, key: str, value: str) -> None:
        """写入一条框架级元数据（UPSERT 语义）。失败抛异常（不吞）。"""

    @abstractmethod
    def enqueue_jobs(self, jobs: List[Dict[str, Any]], *, front: bool = False) -> List[str]:
        """批量增量入队（enqueue 与 run 并发时不做全量覆盖）。

        Effects: 按 front 在队列头/尾插入 jobs（保序），**单事务原子**。
        返回实际插入的 uid 列表（跳过与现有队列重复的 uid）。
        失败时事务回滚并抛异常（不吞）。

        与 commit_* 的 delta 语义对齐：不做 ``DELETE FROM queue`` 全表
        重写——运行中 run() 的 delta commit 与该入队并发时，不会因
        全量 save_queue 覆盖而丢失 run() 已提交的变更
        （写入侧不覆盖，读-改-写窗口仍在）。
        """

    @abstractmethod
    def delete_failed(self, uids: List[str]) -> int:
        """从 DLQ 批量删除指定 uid（clear_dlq/clear_history 的后端实现）。

        返回实际删除行数。不触碰 queue/wall。仅限 run() 之外调用
        （改变 is_known 判定基础，与 enqueue 同纪律）。
        """

    @abstractmethod
    def delete_wall(self, uids: List[str]) -> int:
        """从 wall 批量删除指定 uid（clear_history 的后端实现 + commit 路径事务内清理）。返回实际删除行数。

        两种调用上下文：
        - ``clear_history(...)``（运行外管理 API）批量删除；
        - commit_job_failure / commit_bulk_failure 的同一事务内逐 uid 删除
          （rerun 任务重跑失败时 wall 旧记录作废，最终状态唯一）。

        **运行中**的调用仅限 commit 事务内部（随 DLQ 写入原子提交，不改变
        进行中的派发判定）；独立调用（clear_history）仅限 run 之外（改变
        is_known 判定基础，与 enqueue 同纪律）。
        """

    @abstractmethod
    def seed_wall(self, uids: List[str]) -> int:
        """把 uid 批量写入 wall（meta 为空 dict）——存档迁移标记「已处理」。

        用途：媒体/数据资产项目的存档迁移（硬链接 + wall 种子），
        不必裸 SQL INSERT 框架内部表。返回实际写入行数。
        幂等：已存在的 uid 被覆盖（meta 重置为空）。
        """

    @abstractmethod
    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（UPSERT 语义，幂等）——存档迁移/进度书签恢复。

        注意：discovery 已见判定走 wall/failed（见 discovery.py
        头部），不再使用 cursor——新代码的「已见预填」请用 ``seed_wall``
        （把 process 任务的 uid 写入 wall）。本方法服务通用业务 cursor。
        """
