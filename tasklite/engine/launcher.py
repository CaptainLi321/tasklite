"""Worker 启动器与跨平台进程树内部接缝（纯生产子进程实现）。

收拢两层内部接缝：
1. WorkerLauncher + PreparedWorker + ManagedWorkerHandle + LaunchedWorker：
   以线性两阶段协议（``prepare()`` 冷启动与握手停在放行门前 → ``release()`` 发送
   ``b"GO"`` 或 ``abort()`` 零泄漏回收）隔离真实子进程生命周期；
2. ProcessTreeAdapter + _BasePopenWorkerHandle：
   抽取 Windows / POSIX 共用的 ``Popen`` 等待、退出码缓存与幂等关闭骨架，仅在
   ``WindowsManagedWorkerHandle``（无窗口独立控制台 + 匿名 Job Object + 跳板
   嵌套绑定 + lingering 保活 ``hJob``）与 ``PosixManagedWorkerHandle``（独立会话
   进程组 + 先 ``killpg`` 后 ``wait``）中保留真实平台差异。
"""

from __future__ import annotations

import ctypes
import logging
import os
import pickle
import secrets
import signal
import subprocess
import sys
import tempfile
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from multiprocessing.connection import Client, Listener
from pathlib import Path
from typing import Any, Protocol

from ..utils.clock import utc_now_iso
from ..utils.ipc import (
    HANDSHAKE_ROLE_CANCEL,
    HANDSHAKE_ROLE_DOWNLINK,
    HANDSHAKE_ROLE_UPLINK,
    WorkerPipeReceiver,
    WorkerReplySender,
    decode_handshake_frame,
    encode_handshake_frame,
    encode_json_frame,
)
from ._worker_runtime import WorkerLaunchSpec
from .types import (
    ENV_IPC_ADDR,
    ENV_IPC_AUTHKEY,
    ENV_IPC_FAMILY,
    LAUNCH_GO_TOKEN,
)

logger = logging.getLogger("tasklite")

# 记录主进程启动时的原始工作目录，供子进程展开 sys.path 中的空字符串条目。
_ORIGINAL_DIR: str = os.path.abspath(os.getcwd())

# 仓库根或已安装包父目录，确保子进程在任意 cwd 下都能找到 tasklite 包。
_PACKAGE_ROOT: str = str(Path(__file__).resolve().parents[2])

# POSIX AF_UNIX 路径最大安全长度阈值（留足余量防 sun_path 溢出）。
_POSIX_SOCKET_PATH_LIMIT: int = 95

# Windows 强杀 Job Object 时的退出码（如实暴露给 OS 退出码字段，不伪造 -9）。
_WIN_TERMINATE_EXIT_CODE: int = 1

# Windows Job Object 常量：仅设 KILL_ON_JOB_CLOSE，严禁 BREAKAWAY_OK。
_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS: int = 9
_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: int = 0x2000
# OpenProcess 最小无特权权限（PROCESS_SET_QUOTA | PROCESS_TERMINATE），供嵌套 Job 绑定 venv 真实子进程。
_WIN_PROCESS_JOB_ASSIGN_ACCESS: int = 0x0100 | 0x0001

_DEFAULT_HANDSHAKE_TIMEOUT: float = 15.0
_HANDSHAKE_POLL_SLICE: float = 0.05


class _WorkerLaunchCancelled(Exception):
    """最终启动门前观察到停止请求，子进程不得放行执行。"""


class ManagedWorkerHandle(Protocol):
    """屏蔽 Popen / hJob / pgid / 进程内替身差异的统一句柄协议。"""

    @property
    def pid(self) -> int | None: ...

    @property
    def exitcode(self) -> int | None: ...

    @property
    def was_force_killed(self) -> bool: ...

    def is_alive(self) -> bool: ...

    def join(self, timeout: float | None = None) -> None: ...

    def kill_tree(self) -> None: ...

    def close(self) -> None: ...


@dataclass
class LaunchedWorker:
    """一次已放行的 worker 启动产物。"""

    handle: ManagedWorkerHandle
    receiver: WorkerPipeReceiver
    reply_sender: WorkerReplySender
    started_at: str


class PreparedWorker:
    """已完成冷启动、握手与进程树绑定，停在放行门前的待放行 worker。

    两阶段线性契约：
    - ``release()``：记录启动时间戳并放行 worker（如发送 ``b"GO"``），返回 ``LaunchedWorker``；
      若放行本身失败则自动执行 ``abort()`` 后原样上抛；
    - ``abort()``：幂等终止并回收进程树、句柄与全部双工连接，确保任意未放行路径零泄漏。
    """

    def __init__(
        self,
        *,
        handle: ManagedWorkerHandle,
        receiver: WorkerPipeReceiver,
        reply_sender: WorkerReplySender,
        on_release: Callable[[], None],
        on_abort: Callable[[], None] | None = None,
    ) -> None:
        self.handle = handle
        self.receiver = receiver
        self.reply_sender = reply_sender
        self._on_release = on_release
        self._on_abort = on_abort
        self._lock = threading.Lock()
        self._released = False
        self._aborted = False

    def release(self) -> LaunchedWorker:
        """放行 worker 开始执行 handler 并返回已启动产物。"""
        with self._lock:
            if self._aborted:
                raise RuntimeError("PreparedWorker already aborted")
            if self._released:
                raise RuntimeError("PreparedWorker already released")
            self._released = True
        started_at = utc_now_iso()
        try:
            self._on_release()
        except BaseException:
            self.abort()
            raise
        return LaunchedWorker(
            handle=self.handle,
            receiver=self.receiver,
            reply_sender=self.reply_sender,
            started_at=started_at,
        )

    def abort(self) -> None:
        """幂等中止未放行的 worker 并回收进程树、句柄与双工连接。

        内置标准 5 步清理（kill_tree -> join -> reply_sender.close ->
        receiver.close -> handle.close），若提供可选 ``on_abort`` 额外钩子则在其后执行。
        """
        with self._lock:
            if self._aborted:
                return
            self._aborted = True
        try:
            self.handle.kill_tree()
        except Exception:
            pass
        try:
            self.handle.join(timeout=5.0)
        except Exception:
            pass
        try:
            self.reply_sender.close()
        except Exception:
            pass
        try:
            self.receiver.close(timeout=2.0)
        except Exception:
            pass
        try:
            self.handle.close()
        except Exception:
            pass
        if self._on_abort is not None:
            self._on_abort()


class WorkerLauncher(ABC):
    """Worker 启动器抽象接缝。"""

    @abstractmethod
    def prepare(
        self,
        spec: WorkerLaunchSpec,
        *,
        on_message: Callable[[], None] | None,
        cancel_event: threading.Event,
    ) -> PreparedWorker:
        """冷启动 worker 并完成握手与进程树绑定，停在放行门前返回 PreparedWorker。

        准备阶段任何失败或取消必须零泄漏清理后再抛出异常。
        """


class ProcessTreeAdapter(ABC):
    """跨平台进程树隔离与回收适配器接缝。"""

    @abstractmethod
    def popen_kwargs(self) -> dict[str, Any]:
        """返回传给 ``subprocess.Popen`` 的平台隔离参数。"""

    @abstractmethod
    def attach_and_wrap(
        self,
        proc: subprocess.Popen[Any],
        *,
        worker_pid: int | None = None,
    ) -> ManagedWorkerHandle:
        """将已启动但尚未执行用户代码的子进程纳入进程树管理并返回句柄。"""

    @abstractmethod
    def kill_unattached(self, proc: subprocess.Popen[Any]) -> None:
        """强杀并回收尚未完成进程树绑定的空壳子进程。"""


# ── Popen 公共句柄基类 ───────────────────────────────────────────────────────


class _BasePopenWorkerHandle(ABC):
    """Windows 与 POSIX 共用的 ``Popen`` 句柄生命周期骨架。

    统一承载线程安全的 ``pid`` / ``exitcode`` / ``was_force_killed`` /
    ``is_alive`` / ``join`` / ``kill`` / ``close`` 骨架；平台子类仅实现
    ``kill_tree()`` 与可选的退出/关闭钩子。
    """

    def __init__(self, proc: subprocess.Popen[Any]) -> None:
        self._proc = proc
        self._lock = threading.RLock()
        self._was_force_killed = False
        self._closed = False
        self._cached_exitcode: int | None = None

    def _on_exited_locked(self) -> None:
        """子类钩子（在 ``self._lock`` 内调用）：子进程被观测到已退出时触发。"""

    def _take_close_resources_locked(self) -> Any:
        """子类钩子（在 ``self._lock`` 内调用）：取出待释放的平台原生句柄。"""
        return None

    def _release_close_resources(self, resources: Any) -> None:
        """子类钩子（在锁外调用）：释放由 ``_take_close_resources_locked`` 取出的资源。"""

    @property
    def pid(self) -> int | None:
        return self._proc.pid

    @property
    def exitcode(self) -> int | None:
        with self._lock:
            if self._cached_exitcode is not None:
                return self._cached_exitcode
            if self._closed:
                return self._proc.returncode
            code = self._proc.poll()
            if code is not None:
                self._cached_exitcode = code
                self._on_exited_locked()
            return code

    @property
    def was_force_killed(self) -> bool:
        with self._lock:
            return self._was_force_killed

    def is_alive(self) -> bool:
        with self._lock:
            if self._closed:
                return False
            code = self._proc.poll()
            if code is not None:
                self._cached_exitcode = code
                self._on_exited_locked()
                return False
            return True

    def join(self, timeout: float | None = None) -> None:
        with self._lock:
            if self._closed:
                return
        try:
            code = self._proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return
        except OSError:
            return
        with self._lock:
            self._cached_exitcode = code
            self._on_exited_locked()

    @abstractmethod
    def kill_tree(self) -> None:
        """按当前平台语义原子终止整棵进程树。"""

    def kill(self) -> None:
        """兼容测试调用接口，直接委托进程树终止。"""
        self.kill_tree()

    def close(self) -> None:
        """幂等关闭句柄并释放平台原生资源。"""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._cached_exitcode is None:
                self._cached_exitcode = self._proc.returncode
            resources = self._take_close_resources_locked()
        self._release_close_resources(resources)


# ── Windows 进程树适配（无窗口独立控制台 + Job Object）────────────────────────


def _build_win_job_structures() -> tuple[Any, Any]:
    """按 64 位安全对齐构造 Windows Job Object ctypes 结构体并绑定 kernel32。"""
    from ctypes import wintypes

    class _BasicLimitInfo(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_uint64),
            ("WriteOperationCount", ctypes.c_uint64),
            ("OtherOperationCount", ctypes.c_uint64),
            ("ReadTransferCount", ctypes.c_uint64),
            ("WriteTransferCount", ctypes.c_uint64),
            ("OtherTransferCount", ctypes.c_uint64),
        ]

    class _ExtendedLimitInfo(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimitInfo),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)

    kernel.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
    kernel.CreateJobObjectW.restype = wintypes.HANDLE

    kernel.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    kernel.SetInformationJobObject.restype = wintypes.BOOL

    kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel.AssignProcessToJobObject.restype = wintypes.BOOL

    kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel.TerminateJobObject.restype = wintypes.BOOL

    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL

    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE

    kernel.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel.TerminateProcess.restype = wintypes.BOOL

    return kernel, _ExtendedLimitInfo


_WIN_KERNEL_CACHE: tuple[Any, Any] | None = None
_WIN_KERNEL_LOCK = threading.Lock()


def _get_win_kernel() -> tuple[Any, Any]:
    """惰性初始化并缓存 64 位安全 kernel32 绑定与 Job 结构体。"""
    global _WIN_KERNEL_CACHE
    if _WIN_KERNEL_CACHE is None:
        with _WIN_KERNEL_LOCK:
            if _WIN_KERNEL_CACHE is None:
                _WIN_KERNEL_CACHE = _build_win_job_structures()
    return _WIN_KERNEL_CACHE


def _win_os_error(action: str, err_code: int) -> OSError:
    message = ctypes.FormatError(err_code) if hasattr(ctypes, "FormatError") else f"error {err_code}"
    return OSError(err_code, f"{action} failed: {message}")


class WindowsManagedWorkerHandle(_BasePopenWorkerHandle):
    """绑定 Windows 匿名 Job Object 的线程安全进程树句柄。

    不变式：``hJob`` 在句柄存活及 lingering 期间始终保活，``kill_tree()`` 仅调用
    ``TerminateJobObject`` 终止进程树，仅在 ``close()`` 时才幂等释放 ``hJob``，
    避免提前关闭 Job 句柄误杀仍在收尾的进程。
    """

    def __init__(
        self,
        proc: subprocess.Popen[Any],
        h_jobs: int | Sequence[int],
        kernel: Any,
    ) -> None:
        super().__init__(proc)
        self._h_jobs: tuple[int, ...] = (
            (int(h_jobs),)
            if isinstance(h_jobs, int)
            else tuple(int(h) for h in h_jobs)
        )
        self._kernel = kernel

    def kill_tree(self) -> None:
        """通过 TerminateJobObject 原子终止整棵进程树（不提前关闭 hJob）。"""
        with self._lock:
            self._was_force_killed = True
            h_jobs = () if self._closed else self._h_jobs
        for h_job in h_jobs:
            try:
                self._kernel.TerminateJobObject(h_job, _WIN_TERMINATE_EXIT_CODE)
            except Exception as exc:
                logger.warning(f"TerminateJobObject failed for pid={self._proc.pid}: {exc}")
        try:
            if self._proc.poll() is None:
                self._proc.kill()
        except OSError:
            pass

    def _take_close_resources_locked(self) -> tuple[int, ...]:
        h_jobs = self._h_jobs
        self._h_jobs = ()
        return h_jobs

    def _release_close_resources(self, resources: Any) -> None:
        for h_job in resources:
            try:
                self._kernel.CloseHandle(h_job)
            except Exception as exc:
                logger.warning(f"CloseHandle(hJob) failed for pid={self._proc.pid}: {exc}")

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class WindowsProcessTreeAdapter(ProcessTreeAdapter):
    """Windows 进程树隔离适配器：独立无窗口控制台 + 匿名 Job Object。"""

    def __init__(
        self,
        *,
        kernel: Any | None = None,
        extended_limit_cls: Any | None = None,
    ) -> None:
        self._kernel = kernel
        self._extended_limit_cls = extended_limit_cls

    def _resolve_kernel(self) -> tuple[Any, Any]:
        if self._kernel is not None and self._extended_limit_cls is not None:
            return self._kernel, self._extended_limit_cls
        return _get_win_kernel()

    @staticmethod
    def _create_kill_on_close_job(kernel: Any, extended_limit_cls: Any) -> int:
        """创建仅启用 KILL_ON_JOB_CLOSE（严禁 BREAKAWAY_OK）的匿名 Job Object。"""
        h_job = kernel.CreateJobObjectW(None, None)
        if not h_job:
            err = ctypes.get_last_error()
            raise _win_os_error("CreateJobObjectW", err)
        try:
            info = extended_limit_cls()
            info.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            ok = kernel.SetInformationJobObject(
                h_job,
                _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
                ctypes.byref(info),
                ctypes.sizeof(info),
            )
            if not ok:
                err = ctypes.get_last_error()
                raise _win_os_error("SetInformationJobObject", err)
        except BaseException:
            try:
                kernel.CloseHandle(h_job)
            except Exception:
                pass
            raise
        return int(h_job)

    def popen_kwargs(self) -> dict[str, Any]:
        create_no_window = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
        create_new_group = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
        return {
            "creationflags": create_no_window | create_new_group,
            "stdin": subprocess.DEVNULL,
        }

    def attach_and_wrap(
        self,
        proc: subprocess.Popen[Any],
        *,
        worker_pid: int | None = None,
    ) -> ManagedWorkerHandle:
        """绑定进程树：在 venvlauncher 跳板场景下同时以嵌套 Job 绑定真实 worker 进程。

        Windows 下虚拟环境 ``python.exe`` 为跳板壳进程，在握手前已派生出真正执行
        ``_worker_bootstrap`` 的解释器子进程；仅绑定跳板壳不会覆盖其此前派生的真实
        worker，因此须为真实 ``worker_pid`` 建立独立嵌套 Job Object（此时其阻塞于
        握手屏障、尚未执行任何用户代码，后续派生的多层孙进程自动继承该 Job）。
        """
        kernel, extended_limit_cls = self._resolve_kernel()
        created_jobs: list[int] = []
        try:
            # 1. 若存在 venvlauncher 跳板（worker_pid != proc.pid），先为真实 worker 建嵌套 Job
            if worker_pid is not None and worker_pid != proc.pid:
                h_worker_proc = kernel.OpenProcess(
                    _WIN_PROCESS_JOB_ASSIGN_ACCESS,
                    False,
                    int(worker_pid),
                )
                if not h_worker_proc:
                    err = ctypes.get_last_error()
                    raise _win_os_error("OpenProcess", err)
                worker_assigned = False
                try:
                    h_worker_job = self._create_kill_on_close_job(kernel, extended_limit_cls)
                    created_jobs.append(h_worker_job)
                    ok = kernel.AssignProcessToJobObject(h_worker_job, int(h_worker_proc))
                    if not ok:
                        err = ctypes.get_last_error()
                        raise _win_os_error("AssignProcessToJobObject", err)
                    worker_assigned = True
                except BaseException:
                    if not worker_assigned:
                        try:
                            kernel.TerminateProcess(
                                int(h_worker_proc), _WIN_TERMINATE_EXIT_CODE
                            )
                        except Exception:
                            pass
                    raise
                finally:
                    try:
                        kernel.CloseHandle(h_worker_proc)
                    except Exception:
                        pass

            # 2. 为 Popen 直接句柄绑定 Job Object
            h_proc_job = self._create_kill_on_close_job(kernel, extended_limit_cls)
            created_jobs.append(h_proc_job)
            raw_handle = getattr(proc, "_handle", None)
            if raw_handle is None:
                raise OSError("subprocess.Popen missing native _handle on Windows")
            ok = kernel.AssignProcessToJobObject(h_proc_job, int(raw_handle))
            if not ok:
                err = ctypes.get_last_error()
                raise _win_os_error("AssignProcessToJobObject", err)
        except BaseException:
            for h_job in created_jobs:
                try:
                    kernel.TerminateJobObject(h_job, _WIN_TERMINATE_EXIT_CODE)
                except Exception:
                    pass
                try:
                    kernel.CloseHandle(h_job)
                except Exception:
                    pass
            self.kill_unattached(proc)
            raise

        return WindowsManagedWorkerHandle(proc, created_jobs, kernel)

    def kill_unattached(self, proc: subprocess.Popen[Any]) -> None:
        try:
            if proc.poll() is None:
                proc.kill()
        except OSError:
            pass
        try:
            proc.wait(timeout=5.0)
        except Exception:
            pass


# ── POSIX 进程树适配（独立会话 + 先 killpg 后 wait）──────────────────────────


class PosixManagedWorkerHandle(_BasePopenWorkerHandle):
    """POSIX 独立会话进程组句柄。

    不变式：``kill_tree()`` 严格在组长进程尚未被 ``wait()`` 回收前向整个进程组
    发送 ``SIGKILL``，杜绝先回收组长 PID 引起的 PID 复用误杀。
    """

    def __init__(self, proc: subprocess.Popen[Any]) -> None:
        super().__init__(proc)
        self._pgid = proc.pid
        self._reaped = False

    def _on_exited_locked(self) -> None:
        self._reaped = True

    def kill_tree(self) -> None:
        """先对进程组发送 SIGKILL（不提前 wait 组长），再由 join() 回收。"""
        with self._lock:
            self._was_force_killed = True
            if self._reaped or self._closed:
                return
            pgid = self._pgid
        killpg = getattr(os, "killpg", None)
        sigkill = getattr(signal, "SIGKILL", signal.SIGTERM)
        if killpg is not None:
            try:
                killpg(pgid, sigkill)
            except ProcessLookupError:
                pass
            except PermissionError:
                try:
                    self._proc.kill()
                except OSError:
                    pass
        else:
            try:
                self._proc.kill()
            except OSError:
                pass


class PosixProcessTreeAdapter(ProcessTreeAdapter):
    """POSIX 进程树隔离适配器：start_new_session 建新进程组。"""

    def popen_kwargs(self) -> dict[str, Any]:
        return {
            "start_new_session": True,
            "stdin": subprocess.DEVNULL,
        }

    def attach_and_wrap(
        self,
        proc: subprocess.Popen[Any],
        *,
        worker_pid: int | None = None,
    ) -> ManagedWorkerHandle:
        return PosixManagedWorkerHandle(proc)

    def kill_unattached(self, proc: subprocess.Popen[Any]) -> None:
        killpg = getattr(os, "killpg", None)
        sigkill = getattr(signal, "SIGKILL", signal.SIGTERM)
        if killpg is not None:
            try:
                killpg(proc.pid, sigkill)
            except (ProcessLookupError, PermissionError):
                pass
        else:
            try:
                proc.kill()
            except OSError:
                pass
        try:
            proc.wait(timeout=5.0)
        except Exception:
            pass


def default_process_tree_adapter() -> ProcessTreeAdapter:
    """按当前操作系统返回默认进程树适配器。"""
    if os.name == "nt":
        return WindowsProcessTreeAdapter()
    return PosixProcessTreeAdapter()


# ── 单 Listener 双物理连接握手接收器 ──────────────────────────────────────────


def _make_ipc_address() -> tuple[str, str]:
    """按平台生成单次握手监听地址与 family。"""
    token = secrets.token_hex(12)
    if os.name == "nt":
        return rf"\\.\pipe\tasklite-{os.getpid()}-{token}", "AF_PIPE"
    sock_name = f"tl-{os.getpid()}-{token[:16]}.sock"
    candidate = os.path.join(tempfile.gettempdir(), sock_name)
    if len(candidate.encode("utf-8")) > _POSIX_SOCKET_PATH_LIMIT:
        candidate = os.path.join("/tmp", sock_name)
    return candidate, "AF_UNIX"


class _HandshakeAcceptor:
    """守护线程接收 worker 的 uplink / downlink 双物理连接，支持 cancel 自连唤醒。"""

    def __init__(
        self,
        listener: Listener,
        *,
        address: str,
        family: str,
        authkey: bytes,
    ) -> None:
        self._listener = listener
        self._address = address
        self._family = family
        self._authkey = authkey
        self._lock = threading.Lock()
        self._cancelled = threading.Event()
        self._done = threading.Event()
        self.uplink: Any = None
        self.downlink: Any = None
        self.worker_pid: int | None = None
        self.error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._accept_loop,
            name="tasklite-ipc-handshake",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def wait_done(self, timeout: float) -> bool:
        return self._done.wait(timeout=timeout)

    def _accept_loop(self) -> None:
        try:
            while not self._cancelled.is_set():
                try:
                    conn = self._listener.accept()
                except BaseException as exc:
                    if not self._cancelled.is_set():
                        self.error = exc
                    return

                try:
                    raw = conn.recv_bytes()
                    role, raw_pid = decode_handshake_frame(raw)
                except BaseException as exc:
                    try:
                        conn.close()
                    except OSError:
                        pass
                    if not self._cancelled.is_set():
                        self.error = exc
                    return

                if role == HANDSHAKE_ROLE_CANCEL:
                    try:
                        conn.close()
                    except OSError:
                        pass
                    return

                with self._lock:
                    if self._cancelled.is_set():
                        try:
                            conn.close()
                        except OSError:
                            pass
                        return
                    if raw_pid is not None:
                        self.worker_pid = raw_pid
                    if role == HANDSHAKE_ROLE_UPLINK and self.uplink is None:
                        self.uplink = conn
                    elif role == HANDSHAKE_ROLE_DOWNLINK and self.downlink is None:
                        self.downlink = conn
                    else:
                        try:
                            conn.close()
                        except OSError:
                            pass
                        self.error = RuntimeError(f"Unexpected worker handshake role: {role!r}")
                        return
                    if self.uplink is not None and self.downlink is not None:
                        return
        finally:
            self._done.set()

    def cancel_and_join(self, timeout: float = 2.0) -> None:
        """置取消标志并在必要时经短生命周期守护线程自连发送 cancel 帧，绝不阻塞主线程。"""
        self._cancelled.set()
        if self._done.is_set():
            self._thread.join(timeout=timeout)
            return
        if not self._thread.is_alive():
            return

        def _send_cancel_wake() -> None:
            try:
                wake_conn = Client(
                    address=self._address,
                    family=self._family,
                    authkey=self._authkey,
                )
                try:
                    wake_conn.send_bytes(encode_handshake_frame(HANDSHAKE_ROLE_CANCEL))
                finally:
                    wake_conn.close()
            except BaseException:
                pass

        waker = threading.Thread(
            target=_send_cancel_wake,
            name="tasklite-ipc-handshake-cancel",
            daemon=True,
        )
        waker.start()
        self._thread.join(timeout=timeout)

    def join(self, timeout: float = 2.0) -> None:
        if self._thread.is_alive():
            self._thread.join(timeout=timeout)


# ── 两阶段引导元数据构造 ─────────────────────────────────────────────────────


def _should_skip_main_name(spec_name: str) -> bool:
    if spec_name == "__main__" or spec_name.endswith(".__main__"):
        return True
    root = spec_name.split(".", 1)[0]
    return root in {"pytest", "_pytest"}


def _should_skip_main_path(main_path: str) -> bool:
    if not main_path or "<" in main_path or ">" in main_path:
        return True
    path = Path(main_path)
    if path.suffix.lower() not in {".py", ".pyw"}:
        return True
    stem = path.stem.lower()
    if stem in {"__main__", "pytest", "py.test", "pytest-script"} or stem.startswith("pytest"):
        return True
    return not path.is_file()


def _build_prep_data(spec: WorkerLaunchSpec) -> dict[str, Any]:
    """构造子进程纯 JSON 引导数据（路径、工作目录、__mp_main__ 与执行身份）。"""
    expanded_sys_path = [
        _ORIGINAL_DIR if entry == "" else entry for entry in sys.path
    ]
    init_main_from_name: str | None = None
    init_main_from_path: str | None = None

    main_mod = sys.modules.get("__main__")
    if main_mod is not None:
        main_spec = getattr(main_mod, "__spec__", None)
        spec_name = getattr(main_spec, "name", None) if main_spec is not None else None
        if isinstance(spec_name, str) and spec_name:
            if not _should_skip_main_name(spec_name):
                init_main_from_name = spec_name
        else:
            main_file = getattr(main_mod, "__file__", None)
            if isinstance(main_file, str) and main_file:
                resolved = (
                    main_file
                    if os.path.isabs(main_file)
                    else os.path.normpath(os.path.join(_ORIGINAL_DIR, main_file))
                )
                if not _should_skip_main_path(resolved):
                    init_main_from_path = resolved

    return {
        "job_uid": spec.job.uid,
        "execution_id": spec.execution_id,
        "run_id": spec.run_id or None,
        "attempt_no": spec.attempt_no,
        "cwd": os.getcwd(),
        "orig_dir": _ORIGINAL_DIR,
        "sys_path": expanded_sys_path,
        "init_main_from_name": init_main_from_name,
        "init_main_from_path": init_main_from_path,
    }


def _build_bootstrap_env(address: str, family: str, authkey: bytes) -> dict[str, str]:
    """组装子进程环境变量：注入私有 IPC 凭据并确保 tasklite 包可被 -m 定位。"""
    env = os.environ.copy()
    env[ENV_IPC_ADDR] = address
    env[ENV_IPC_FAMILY] = family
    env[ENV_IPC_AUTHKEY] = authkey.hex()

    existing_pythonpath = env.get("PYTHONPATH", "")
    path_entries = [p for p in existing_pythonpath.split(os.pathsep) if p] if existing_pythonpath else []
    norm_existing = {os.path.normcase(os.path.abspath(p)) for p in path_entries}
    if os.path.normcase(os.path.abspath(_PACKAGE_ROOT)) not in norm_existing:
        path_entries.append(_PACKAGE_ROOT)
        env["PYTHONPATH"] = os.pathsep.join(path_entries)
    return env


# ── 真实子进程启动器（单 Listener 双物理连接 + 两阶段握手屏障）────────────────


class SubprocessWorkerLauncher(WorkerLauncher):
    """基于 Popen + Listener/Client 握手屏障与进程树适配器的真实子进程启动器。"""

    def __init__(
        self,
        *,
        tree_adapter: ProcessTreeAdapter | None = None,
        handshake_timeout: float = _DEFAULT_HANDSHAKE_TIMEOUT,
    ) -> None:
        self._tree_adapter = tree_adapter or default_process_tree_adapter()
        self._handshake_timeout = handshake_timeout

    @staticmethod
    def _cleanup_socket_file(address: str, family: str) -> None:
        if family == "AF_UNIX" and address:
            try:
                os.unlink(address)
            except OSError:
                pass

    def prepare(
        self,
        spec: WorkerLaunchSpec,
        *,
        on_message: Callable[[], None] | None,
        cancel_event: threading.Event,
    ) -> PreparedWorker:
        if cancel_event.is_set():
            raise _WorkerLaunchCancelled(
                f"stop requested before worker launch for {spec.job.uid}"
            )

        prep_bytes = encode_json_frame(_build_prep_data(spec))
        payload_bytes = pickle.dumps(
            {
                "handler": spec.handler,
                "job": spec.job,
                "job_context": spec.job_context,
                "execution_id": spec.execution_id,
                "timeout_seconds": spec.timeout_seconds,
                "run_id": spec.run_id,
                "attempt_no": spec.attempt_no,
            }
        )

        address, family = _make_ipc_address()
        authkey = os.urandom(32)
        listener: Listener | None = None
        acceptor: _HandshakeAcceptor | None = None
        proc: subprocess.Popen[Any] | None = None
        managed_handle: ManagedWorkerHandle | None = None
        uplink: Any = None
        downlink: Any = None
        receiver: WorkerPipeReceiver | None = None
        reply_sender: WorkerReplySender | None = None

        try:
            listener = Listener(address=address, family=family, authkey=authkey)
            acceptor = _HandshakeAcceptor(
                listener,
                address=str(listener.address),
                family=family,
                authkey=authkey,
            )
            acceptor.start()

            if cancel_event.is_set():
                raise _WorkerLaunchCancelled(
                    f"stop requested before worker process creation for {spec.job.uid}"
                )

            env = _build_bootstrap_env(str(listener.address), family, authkey)
            popen_kwargs = self._tree_adapter.popen_kwargs()
            proc = subprocess.Popen(
                [sys.executable, "-m", "tasklite.engine._worker_bootstrap"],
                env=env,
                **popen_kwargs,
            )

            deadline = time.monotonic() + self._handshake_timeout
            while not acceptor.wait_done(_HANDSHAKE_POLL_SLICE):
                if cancel_event.is_set():
                    raise _WorkerLaunchCancelled(
                        f"stop requested during worker handshake for {spec.job.uid}"
                    )
                if proc.poll() is not None:
                    raise RuntimeError(
                        f"Worker bootstrap process exited prematurely with code {proc.returncode}"
                    )
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Worker bootstrap handshake timed out after {self._handshake_timeout}s"
                    )

            acceptor.join(timeout=2.0)
            uplink = acceptor.uplink
            downlink = acceptor.downlink

            if acceptor.error is not None:
                raise RuntimeError(f"Worker handshake failed: {acceptor.error}") from acceptor.error
            if cancel_event.is_set():
                raise _WorkerLaunchCancelled(
                    f"stop requested after worker handshake for {spec.job.uid}"
                )
            if uplink is None or downlink is None:
                raise RuntimeError("Worker handshake ended without both uplink and downlink connections")

            # 收齐双连接后立即关闭 Listener，不再接受任何连接
            listener.close()
            listener = None
            self._cleanup_socket_file(address, family)

            # 零竞态进程树绑定：此时 worker 阻塞在 downlink.recv_bytes()，尚未执行用户代码
            managed_handle = self._tree_adapter.attach_and_wrap(
                proc,
                worker_pid=acceptor.worker_pid,
            )

            if cancel_event.is_set():
                raise _WorkerLaunchCancelled(
                    f"stop requested after process tree attach for {spec.job.uid}"
                )

            # 第一阶段下发预序列化数据（worker 仅缓存字节，仍阻塞于 b"GO" 放行门）
            downlink.send_bytes(prep_bytes)
            downlink.send_bytes(payload_bytes)

            receiver = WorkerPipeReceiver(
                uplink,
                job_uid=spec.job.uid,
                execution_id=spec.execution_id,
                run_id=spec.run_id or None,
                attempt_no=spec.attempt_no,
                on_message=on_message,
            )
            uplink = None  # 所有权转移给 receiver

            reply_sender = WorkerReplySender(
                downlink,
                job_uid=spec.job.uid,
                execution_id=spec.execution_id,
                run_id=spec.run_id or None,
                attempt_no=spec.attempt_no,
            )

            bound_handle = managed_handle
            bound_receiver = receiver
            bound_reply_sender = reply_sender
            bound_downlink = downlink
            bound_cancel = cancel_event

            def _release_subprocess() -> None:
                if bound_cancel.is_set():
                    raise _WorkerLaunchCancelled(
                        f"stop requested before release for {spec.job.uid}"
                    )
                bound_downlink.send_bytes(LAUNCH_GO_TOKEN)

            return PreparedWorker(
                handle=bound_handle,
                receiver=bound_receiver,
                reply_sender=bound_reply_sender,
                on_release=_release_subprocess,
            )
        except BaseException:
            # 清理顺序红线：先非阻塞唤醒 acceptor 并尽快关 listener（打断任何未接通的
            # 唤醒自连），再终止进程树，随后复核 acceptor 退出并关闭 receiver/reply_sender 与句柄。
            if acceptor is not None:
                acceptor.cancel_and_join(timeout=0.5)
            if listener is not None:
                try:
                    listener.close()
                except Exception:
                    pass
                listener = None
            if acceptor is not None:
                if uplink is None and acceptor.uplink is not None and receiver is None:
                    uplink = acceptor.uplink
                if downlink is None and acceptor.downlink is not None and reply_sender is None:
                    downlink = acceptor.downlink
            if managed_handle is not None:
                try:
                    managed_handle.kill_tree()
                except Exception:
                    pass
                try:
                    managed_handle.join(timeout=5.0)
                except Exception:
                    pass
            elif proc is not None:
                # 尚未绑定进程树前先关裸连接，令阻塞于握手屏障的 worker 立即因 EOF 退出
                for raw_conn in (downlink, uplink):
                    if raw_conn is not None:
                        try:
                            raw_conn.close()
                        except OSError:
                            pass
                downlink = None
                uplink = None
                self._tree_adapter.kill_unattached(proc)

            if acceptor is not None:
                acceptor.join(timeout=1.5)
                if uplink is None and acceptor.uplink is not None and receiver is None:
                    uplink = acceptor.uplink
                if downlink is None and acceptor.downlink is not None and reply_sender is None:
                    downlink = acceptor.downlink

            if reply_sender is not None:
                try:
                    reply_sender.close()
                except Exception:
                    pass
            elif downlink is not None:
                try:
                    downlink.close()
                except OSError:
                    pass

            if receiver is not None:
                try:
                    receiver.close(timeout=2.0)
                except Exception:
                    pass
            elif uplink is not None:
                try:
                    uplink.close()
                except OSError:
                    pass

            if managed_handle is not None:
                try:
                    managed_handle.close()
                except Exception:
                    pass

            self._cleanup_socket_file(address, family)
            raise


__all__ = [
    "LaunchedWorker",
    "ManagedWorkerHandle",
    "PosixManagedWorkerHandle",
    "PosixProcessTreeAdapter",
    "PreparedWorker",
    "ProcessTreeAdapter",
    "SubprocessWorkerLauncher",
    "WindowsManagedWorkerHandle",
    "WindowsProcessTreeAdapter",
    "WorkerLauncher",
    "_BasePopenWorkerHandle",
    "_WorkerLaunchCancelled",
    "default_process_tree_adapter",
]
