"""v2 跨平台单文件锁：库级执行与管理互斥（flock / msvcrt 互斥）。

用于主进程按状态库路径获取库级互斥锁，防止并发执行或管理冲突。
"""
from __future__ import annotations

import errno
import os
import sys
import time
from pathlib import Path


def lock_path_for(state_path: str | Path) -> Path:
    """由状态库路径派生对应的库级互斥锁文件路径（幂等处理已有 .lock 后缀）。"""
    p = Path(state_path)
    if p.name.endswith(".lock"):
        return p
    return Path(f"{p}.lock")


def _lock_path(directory_or_path: str | Path, name: str | None = None) -> Path:
    """name → 锁文件路径。若 name 为 None，直接派生锁文件路径。"""
    if name is None:
        return lock_path_for(directory_or_path)
    return Path(directory_or_path) / f"{name}.lock"


def _is_lock_contention(exc: OSError) -> bool:
    """判断 OSError 是否属于锁争用（而非无效 fd 或底层 IO 故障）。"""
    if sys.platform == "win32":
        return (
            exc.errno in (errno.EACCES, errno.EDEADLK)
            or getattr(exc, "winerror", None) in (33, 32)
        )
    return exc.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK)


def try_acquire_lock(
    directory_or_path: str | Path,
    name: str | None = None,
    *,
    timeout: float = 0.0,
) -> int | None:
    """尝试获取排他锁（非阻塞或带超时）。

    Returns:
        成功返回 fd（调用方必须 ``release_lock``）；**仅锁被其他进程占用**
        时返回 None。锁文件**创建后不删除**。

    Raises:
        OSError: 锁文件打不开/建不出/IO 故障（权限、磁盘满、无效 fd 等）——与
            「锁被占」语义不同，不得混入 None。
    """
    path = _lock_path(directory_or_path, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
    deadline = None if timeout <= 0 else time.monotonic() + timeout
    try:
        while True:
            if _try_lock_fd(fd):
                return fd
            if deadline is None or time.monotonic() >= deadline:
                os.close(fd)
                return None
            time.sleep(0.1)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        raise


def _try_lock_fd(fd: int) -> bool:
    """对已打开的 fd 尝试非阻塞排他锁（平台分派）。"""
    if sys.platform == "win32":
        import msvcrt

        # msvcrt.locking 按当前文件偏移加锁，固定归零确保始终锁第 0 字节。
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return True
        except OSError as exc:
            if _is_lock_contention(exc):
                return False
            raise
    else:
        import fcntl

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError as exc:
            if _is_lock_contention(exc):
                return False
            raise


def release_lock(fd: int) -> None:
    """释放并关闭锁 fd。"""
    if fd is None:
        return
    try:
        if sys.platform == "win32":
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        os.close(fd)
    except OSError:
        pass


__all__ = [
    "lock_path_for",
    "release_lock",
    "try_acquire_lock",
]
