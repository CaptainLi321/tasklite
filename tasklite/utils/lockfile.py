"""v2 跨平台单文件锁：库级执行与管理互斥（flock 互斥）。

用于主进程在指定目录（如 ipc_dir 或 state_dir）获取库级互斥锁，
防止同目录并发执行或管理冲突。
"""
from __future__ import annotations

import os
import time
from pathlib import Path


def _lock_path(ipc_dir_or_path: str | Path, name: str | None = None) -> Path:
    """name → 锁文件路径。若 name 为 None，直接派生锁文件路径。"""
    if name is None:
        p = Path(ipc_dir_or_path)
        if p.name.endswith(".lock"):
            return p
        return Path(str(p) + ".lock")
    return Path(ipc_dir_or_path) / f"{name}.lock"


def try_acquire_lock(
    ipc_dir_or_path: str | Path,
    name: str | None = None,
    *,
    timeout: float = 0.0,
) -> int | None:
    """尝试获取排他锁（非阻塞或带超时）。

    Returns:
        成功返回 fd（调用方必须 ``release_lock``）；**仅锁被其他进程占用**
        时返回 None。锁文件**创建后不删除**。

    Raises:
        OSError: 锁文件打不开/建不出（权限、磁盘满等环境故障）——与
            「锁被占」语义不同，不得混入 None。
    """
    path = _lock_path(ipc_dir_or_path, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
    deadline = None if timeout <= 0 else time.monotonic() + timeout
    while True:
        if _try_lock_fd(fd):
            return fd
        if deadline is None:
            os.close(fd)
            return None
        if time.monotonic() >= deadline:
            os.close(fd)
            return None
        time.sleep(0.1)


def _try_lock_fd(fd: int) -> bool:
    """对已打开的 fd 尝试非阻塞排他锁（平台分派）。"""
    if os.name == "nt":
        import msvcrt
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
            return True
        except OSError:
            return False
    import fcntl
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        return False


def release_lock(fd: int) -> None:
    """释放并关闭锁 fd。"""
    if fd is None:
        return
    try:
        if os.name == "nt":
            import msvcrt
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
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
    "release_lock",
    "try_acquire_lock",
]
