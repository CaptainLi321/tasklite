"""v2 跨平台单文件锁：uid→锁文件映射与孤儿探测（flock 互斥）。

设计约束：
- **worker 持锁、主进程仅探测**——不变式：锁生命周期严格等于子进程执行体
  生命周期。主进程持锁会在其崩溃时释放（fd 关闭 → 内核释放 flock），孤儿
  worker 仍活着锁却空闲 → 新 run 探测通过 → 双跑（正是要防的场景）。
- 锁文件 ``{uid}.lock`` 永不删除（unlink 后新进程 create 同名文件拿到的是
  新 inode 的锁，与旧持锁者不互斥——经典 unlink-recreate 竞争）。空文件
  累积可接受，pipeline 停止时可整目录离线清理。
- uid 派生锁文件名复用 ``encoding.safe_uid_filename`` 单射编码（不得另造
  一套映射）。单射性证明关键点：**先转义 ``%`` 为 ``%25``、后转义 ``::``
  为 ``%3A%3A``**——若先转义 ``::``，输入 ``t::x::y`` 与字面
  ``t%3A%3Ax%3A%3Ay`` 会在第二步碰撞同像，多对一映射使锁文件互串；
  转义符优先转义保证输出中 ``%`` 唯一引导一个 %XX 序列，解码可逆。
- 锁冲突（``lock_conflict``）是瞬态信号：不烧重试预算、降级写盘回队、
  零污染；锁路径环境故障（OSError）与「锁被占」语义不同——环境故障
  fail-soft 上抛供调用方按同构瞬态信号降级，不得混入 None 静默重试。
"""
from __future__ import annotations

import os
import time
from pathlib import Path

from .encoding import safe_uid_filename


def _lock_path(ipc_dir: str, uid: str) -> Path:
    """uid → 锁文件路径（与 result/signals/outputs 共享单射文件名映射）。"""
    return Path(ipc_dir) / f"{safe_uid_filename(uid)}.lock"


def try_acquire_lock(ipc_dir: str, uid: str, *, timeout: float = 0.0) -> int | None:
    """尝试获取 ``{uid}.lock`` 排他锁（非阻塞或带超时）。

    Returns:
        成功返回 fd（调用方必须 ``release_lock``）；**仅锁被其他执行体占用**
        时返回 None。锁文件**创建后不删除**。

    Raises:
        OSError: 锁文件打不开/建不出（权限、磁盘满等环境故障）——与
            「锁被占」（瞬态、可 defer 重试）语义不同，不得混入 None：
            否则调用方会把环境故障误当锁冲突静默重试，真实错误被吞。

    POSIX：``flock(LOCK_EX | LOCK_NB)``。Windows：``msvcrt.locking``
    （``LK_NBLCK``，锁 offset 0 的 1 字节）。``timeout > 0`` 时以短间隔
    轮询（worker 入口持锁允许 ≤2s 短等待）。
    """
    path = _lock_path(ipc_dir, uid)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
    deadline = None if timeout <= 0 else time.monotonic() + timeout
    while True:
        if _try_lock_fd(fd):
            return fd
        if deadline is None:
            # 无 timeout = 纯非阻塞语义（主进程探测/单次尝试）：失败即返回
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
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]  # 锁 offset 0 的 1 字节
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
    """释放并关闭锁 fd。POSIX 解锁；Windows 移动文件指针到 offset 0 后解锁。"""
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


def probe_lock(ipc_dir: str, uid: str) -> bool:
    """主进程探测：非阻塞试锁，成功即释放，返回「无其他执行体持锁」。

    仅用于判断孤儿 worker 是否存活——探测成功释放锁，不持有。探测失败
    （孤儿持锁）→ 调用方按 ``lock_conflict`` 瞬态信号 defer（不烧预算、
    降级写盘、零污染），节奏归 RequeuePolicy。
    """
    fd = try_acquire_lock(ipc_dir, uid)
    if fd is None:
        return False
    release_lock(fd)
    return True


__all__ = [
    "probe_lock",
    "release_lock",
    "try_acquire_lock",
]
