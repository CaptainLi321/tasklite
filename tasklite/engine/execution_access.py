"""状态库执行权的词法作用域，统一 Run 与管理操作的互斥。"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, cast

from ..backend.base import AbstractStateBackend
from ..backend.memory import InMemoryStateBackend
from ..backend.sqlite_backend import SQLiteStateBackend
from ..exceptions import TaskLiteError
from ..utils.lockfile import release_lock, try_acquire_lock


@contextmanager
def exclusive_state_access(backend: AbstractStateBackend) -> Iterator[None]:
    """非阻塞取得内置后端执行权，离开作用域时归还本次取得的锁。"""
    if isinstance(backend, SQLiteStateBackend):
        fd = try_acquire_lock(backend.path, timeout=0)
        if fd is None:
            raise TaskLiteError(
                f"Another run() or management operation is in progress for "
                f"{backend.path}; concurrent runs or management operations on the same state are forbidden."
            )
        try:
            yield
        finally:
            release_lock(fd)
    else:
        # 后端只由门面构造；内存锁在后端初始化时创建，不在争用时补造。
        lock = cast(InMemoryStateBackend, backend)._exec_lock
        if not lock.acquire(blocking=False):
            raise TaskLiteError(
                "Another run() or management operation is in progress; "
                "concurrent runs or management operations on the same state are forbidden."
            )
        try:
            yield
        finally:
            lock.release()
