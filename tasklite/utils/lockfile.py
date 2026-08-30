"""跨平台文件锁 + uid→文件名安全映射（flock 孤儿探测）。

设计约束：
- **worker 持锁、主进程仅探测**——锁的生命周期 = 执行体生命周期。主进程
  持锁会在其崩溃时释放（fd 关闭 → 内核释放 flock），孤儿 worker 仍活着
  锁却空闲 → 新 run 探测通过 → 双跑（正是要防的场景）。
- 锁文件 `{uid}.lock` 永不删除（unlink 后新进程 create 同名文件拿到的是
  新 inode 的锁，与旧持锁者不互斥——经典 unlink-recreate 竞争）。空文件
  累积可接受，pipeline 停止时可整目录离线清理。
- uid 含 ``::``（Windows 文件名禁 ``:``）→ 统一转义为安全字符，供锁文件
  与 executor 的 result/signals/outputs 文件名共享（不得另造一套映射；
  同时保证 fence 结果文件名的 Windows 兼容）。
"""
import os
import time
from pathlib import Path
from typing import Optional

# uid → 文件名安全形式：task_type::job_id → task_type%3A%3Ajob_id
# （百分号编码 `::`，先转义 `%` 保证单射——见 safe_uid_filename docstring）
_UID_ESCAPE = "::"
_UID_ESCAPED = "%3A%3A"
_PERCENT_ESCAPE = "%"
_PERCENT_ESCAPED = "%25"
# 文件系统危险字符——路径分隔符（POSIX `/`、Windows `\`）、
# NUL（os.open 拒绝）、glob 元字符（`*?[]` 会注入 _iter_stale_result_paths 的
# glob 匹配、跨 uid 删除他人结果文件）。全部转义为 %XX 保持单射可逆。
# 单冒号 `:` 同样加入——Windows 文件名禁 `:`（NTFS 保留
# 字符），job_id 只禁 `::` 可合法含单冒号（如 "id:with:colons"），不转义则
# Windows 派发路径非法；`::` 由 _UID_ESCAPED 替换处理（单冒号先于 `::`
# 替换逐字符转义，二者结果一致：`::` → `%3A%3A`，单射保持）。
_FS_ESCAPE_CHARS = {
    "/": "%2F",
    "\\": "%5C",
    "\x00": "%00",
    "*": "%2A",
    "?": "%3F",
    "[": "%5B",
    "]": "%5D",
    ":": "%3A",
}


def safe_uid_filename(uid: str) -> str:
    """把 job uid 映射为对任意文件系统安全（无 ``:`` 等保留字符）的文件名。

    与 executor 的 result/signals/outputs 路径函数共享——统一映射，不得
    另造一套。

    直接 ``uid.replace("::", "_%3A%3A_")`` 非单射——
    job_id 可合法含 ``%``，当 job_id 恰含字面 ``_%3A%3A_`` 时（如
    ``t::x::y`` 与 ``t::x_%3A%3A_y``）两个不同 uid 映射到同一文件名，
    锁文件/signals/outputs/结果文件全碰撞。本实现先转义 ``%`` 为
    ``%25`` 再转义 ``::``——编码序列中的 ``%`` 永不与用户输入的
    ``%`` 混淆（后者已被 %25 吸收），映射单射且可逆。

    job_id 只禁止 ``::``，可含 ``/``、``..``、
    ``*`` 等——若不转义，uid 派生的 IPC 文件路径可逃逸 state_dir
    （``os.open`` 创建/``os.replace`` 覆盖/``unlink`` 删除任意路径），
    且 ``t::a//b`` 与 ``t::a/b`` 在文件系统级碰撞。转义全部文件系统
    危险字符后：映射仍单射（%XX 可逆），且派生路径不含分隔符——
    ``..`` 无法成为路径组件、glob 元字符不参与匹配。
    """
    escaped = uid.replace(_PERCENT_ESCAPE, _PERCENT_ESCAPED)
    for ch, enc in _FS_ESCAPE_CHARS.items():
        escaped = escaped.replace(ch, enc)
    return escaped


def _lock_path(ipc_dir: str, uid: str) -> Path:
    return Path(ipc_dir) / f"{safe_uid_filename(uid)}.lock"


def try_acquire_lock(ipc_dir: str, uid: str, *, timeout: float = 0.0) -> Optional[int]:
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
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)  # 锁 offset 0 的 1 字节
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


def probe_lock(ipc_dir: str, uid: str) -> bool:
    """主进程探测：非阻塞试锁，成功即释放，返回「无其他执行体持锁」。

    仅用于判断孤儿 worker 是否存活——探测成功释放锁，
    不持有。探测失败（孤儿持锁）→ 调用方 requeue + 短退避。
    """
    fd = try_acquire_lock(ipc_dir, uid)
    if fd is None:
        return False
    release_lock(fd)
    return True
