"""ExecutionChannel: 统一子进程执行、阶梯看门狗、IPC 通信与跨平台文件锁的深模块。

提供极简接缝（ExecutionChannelProtocol），内敛子进程派发、看门狗阶梯终止、
两级降级落盘、跨平台排他文件锁与 TOCTOU 闭环清理的底层复杂性。
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import signal
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple, Union

from ..exceptions import (
    FatalError,
    RetryError,
)
from ..taxonomy import classify_exception
from ..models.context import TaskContext
from ..models.job import Job
from ..utils.ipc import (
    ArtifactCleanupMode,
    ArtifactJournal,
    decode_raw_result as _decode_raw_result,
    encode_raw_result as _encode_raw_result,
)
from ..utils.jsonutil import dumps
from ..utils import lockfile

logger = logging.getLogger("tasklite")

_KEY_TRACEBACK = "traceback"
_RESULT_DIR_ENV = "TASKLITE_IPC_DIR"
_RESULT_TMP_SUFFIX = ".result.json.tmp"
_RESULT_SUFFIX = ".result.json"


def _normalize_handler_result(result: Any) -> Tuple[bool, Dict[str, Any]]:
    """Normalize handler return value to (success, metadata) tuple."""
    if result is None:
        return True, {}
    if isinstance(result, bool):
        return result, {}
    if isinstance(result, dict):
        try:
            dumps(result)
        except (TypeError, ValueError) as e:
            logger.error(
                f"Handler returned dict with non-JSON-serializable metadata: {e}. "
                f"Job will be marked as failed."
            )
            return False, {"error": f"invalid result metadata: not JSON-serializable: {e}"}
        return True, result
    if isinstance(result, tuple) and len(result) == 2:
        if not isinstance(result[0], bool) or not isinstance(result[1], dict):
            logger.error(
                f"Handler returned invalid tuple: expected (bool, dict), got "
                f"({type(result[0]).__name__}, {type(result[1]).__name__}). Job will be marked as failed."
            )
            return False, {"error": "invalid handler return tuple: expected (bool, dict)"}
        try:
            dumps(result[1])
        except (TypeError, ValueError) as e:
            logger.error(
                f"Handler returned tuple with non-JSON-serializable metadata: {e}. "
                f"Job will be marked as failed."
            )
            return False, {"error": f"invalid result metadata: not JSON-serializable: {e}"}
        return result[0], result[1]
    logger.error(
        f"Handler returned unrecognized type: {type(result).__name__}. "
        f"Expected None, bool, dict, or tuple(bool, dict). "
        f"Job will be marked as failed."
    )
    return False, {"error": f"invalid handler return type: {type(result).__name__}"}


def _decode_ipc_result(
    res: dict, p: Any, job: Job, ipc_dir: Optional[str] = None
) -> "ExecutionResult":
    """解析子进程通过结果文件回传的结果字典。"""
    success = False
    result_meta: Dict[str, Any] = {}
    retry_requested = False
    retry_error: Optional[str] = None
    new_jobs: List[Job] = []
    cursor_updates: Dict[str, str] = {}
    resource_suspensions: List[Tuple[str, float]] = []
    lock_conflict = False
    interrupted = False

    if isinstance(res, dict) and "status" in res:
        if res["status"] == "success":
            if "raw_result" not in res:
                logger.error(f"Corrupt result file for {job.uid}: missing 'raw_result' key")
                result_meta = {"error": "CORRUPT_RESULT_FILE: missing raw_result"}
            else:
                try:
                    raw_result = _decode_raw_result(res["raw_result"])
                    success, result_meta = _normalize_handler_result(raw_result)
                except Exception as e:
                    success = False
                    result_meta = {
                        "error": f"CORRUPT_RESULT_FILE: decode failed: {e}",
                        _KEY_TRACEBACK: traceback.format_exc(),
                    }
                    logger.error(f"Result decode failed for {job.uid}: {e}")
            new_jobs = []
            for jd in res.get("new_jobs") or []:
                try:
                    new_jobs.append(Job.from_dict(jd))
                except (KeyError, TypeError, ValueError) as e:
                    success = False
                    result_meta = {
                        "error": f"invalid spawned job dict: {e}",
                        _KEY_TRACEBACK: traceback.format_exc(),
                    }
                    logger.error(f"Malformed spawned job in {job.uid}: {e}")
                    break
            if success:
                _cu = res.get("cursor_updates", {})
                _rs = res.get("resource_suspensions", [])
                if isinstance(_cu, dict):
                    valid_cu = {}
                    cu_ok = True
                    for k, v in _cu.items():
                        if isinstance(k, str) and (v is None or isinstance(v, str)):
                            valid_cu[k] = v
                        else:
                            success = False
                            result_meta = {"error": f"invalid cursor_updates value: {k!r}={v!r}"}
                            cu_ok = False
                            break
                    if cu_ok:
                        cursor_updates = valid_cu
                else:
                    success = False
                    result_meta = {"error": f"invalid cursor_updates type: {type(_cu).__name__}"}
                if isinstance(_rs, list):
                    valid_rs = []
                    for item in _rs:
                        if (
                            isinstance(item, (list, tuple))
                            and len(item) == 2
                            and isinstance(item[0], str)
                            and isinstance(item[1], (int, float))
                            and not isinstance(item[1], bool)
                        ):
                            valid_rs.append((item[0], float(item[1])))
                        else:
                            success = False
                            result_meta = {"error": f"invalid resource_suspension entry: {item!r}"}
                            break
                    if valid_rs:
                        resource_suspensions = valid_rs
                else:
                    success = False
                    result_meta = {"error": f"invalid resource_suspensions type: {type(_rs).__name__}"}
        elif res["status"] == "retry":
            retry_requested = True
            retry_error = res.get("error")
            lock_conflict = bool(res.get("lock_conflict", False))
        elif res["status"] == "interrupted":
            retry_requested = True
            retry_error = res.get("error")
            interrupted = True
        elif res["status"] == "fatal":
            success = False
            result_meta = {
                "error": res.get("error", "FATAL (no message)"),
                _KEY_TRACEBACK: res.get(_KEY_TRACEBACK),
                "fatal": True,
            }
            logger.error(f"Fatal error in {job.uid}:\n{res.get(_KEY_TRACEBACK, '')}")
        else:
            success = False
            result_meta = {
                "error": res.get("error", f"unknown status {res['status']!r}"),
                _KEY_TRACEBACK: res.get(_KEY_TRACEBACK),
            }
            logger.error(f"Worker Crashed for {job.uid}:\n{res.get(_KEY_TRACEBACK, '')}")
    elif p is not None and getattr(p, "exitcode", None) is not None and p.exitcode != 0:
        success = False
        result_meta = {"error": f"PROCESS_CRASH_EXITCODE_{p.exitcode}"}
    else:
        result_meta = {"error": "NO_IPC_RESULT"}

    if success and ipc_dir is not None:
        ok, err = ArtifactJournal(ipc_dir).verify_outputs(job.uid)
        if not ok:
            logger.error(f"Verification failed for {job.uid}: {err}")
            success = False
            result_meta = {"error": err}

    return ExecutionResult(
        success=success,
        result_meta=result_meta,
        retry_requested=retry_requested,
        retry_error=retry_error,
        new_jobs=new_jobs,
        cursor_updates=cursor_updates,
        resource_suspensions=resource_suspensions,
        lock_conflict=lock_conflict,
        interrupted=interrupted,
    )


def _mp_worker_wrapper(handler_func: Callable, job: Job, ctx: TaskContext, ipc_dir: str) -> None:
    """Wrapper for multiprocessing worker execution."""
    uid = job.uid
    incarnation = getattr(ctx, "incarnation", None)
    if not incarnation:
        raise RuntimeError(
            f"worker started without incarnation for {uid}: "
            f"fencing requires ctx.incarnation set by submit"
        )
    journal = ArtifactJournal(ipc_dir)
    transient_registry = getattr(ctx, "transient_registry", ()) or ()
    _lock_fd = lockfile.try_acquire_lock(ipc_dir, uid, timeout=2.0)
    if _lock_fd is None:
        journal.write_result_with_degradation(
            uid,
            {
                "status": "retry",
                "lock_conflict": True,
                "error": f"LOCK_CONFLICT: another execution body holds {uid} lock",
            },
            incarnation=incarnation,
        )
        return
    try:
        raw_result = handler_func(job, ctx)
        journal.write_result_with_degradation(
            uid,
            {
                "status": "success",
                "raw_result": _encode_raw_result(raw_result),
                "new_jobs": [j.to_dict() for j in ctx.new_jobs],
                "resource_suspensions": ctx.resource_suspensions,
                "cursor_updates": ctx.cursor_updates,
            },
            incarnation=incarnation,
        )
    except RetryError as e:
        journal.write_result_with_degradation(
            uid, {"status": "retry", "error": str(e)}, incarnation=incarnation
        )
    except FatalError as e:
        journal.write_result_with_degradation(
            uid,
            {
                "status": "fatal",
                "error": str(e),
                _KEY_TRACEBACK: traceback.format_exc(),
            },
            incarnation=incarnation,
        )
    except Exception as e:
        kind = classify_exception(
            e,
            transient_registry,
            fatal_exceptions=getattr(ctx, "fatal_exceptions", None),
            transient_exceptions=getattr(ctx, "transient_exceptions", None),
        )
        if kind == "retry":
            journal.write_result_with_degradation(
                uid,
                {"status": "retry", "error": f"{type(e).__name__}: {e}"},
                incarnation=incarnation,
            )
        elif kind == "fatal":
            journal.write_result_with_degradation(
                uid,
                {
                    "status": "fatal",
                    "error": f"{type(e).__name__}: {e}",
                    _KEY_TRACEBACK: traceback.format_exc(),
                },
                incarnation=incarnation,
            )
        else:
            journal.write_result_with_degradation(
                uid,
                {
                    "status": "error",
                    "error": f"{type(e).__name__}: {e}",
                    _KEY_TRACEBACK: traceback.format_exc(),
                },
                incarnation=incarnation,
            )
    except KeyboardInterrupt as e:
        journal.write_result_with_degradation(
            uid,
            {
                "status": "interrupted",
                "error": f"WORKER_INTERRUPTED: {type(e).__name__}",
                _KEY_TRACEBACK: traceback.format_exc(),
            },
            incarnation=incarnation,
        )
    except SystemExit as e:
        journal.write_result_with_degradation(
            uid,
            {
                "status": "error",
                "error": f"WORKER_INTERRUPTED: {type(e).__name__}",
                _KEY_TRACEBACK: traceback.format_exc(),
            },
            incarnation=incarnation,
        )
    finally:
        lockfile.release_lock(_lock_fd)


@dataclass
class ExecutionResult:
    """Outcome of a single multiprocessing job execution."""
    success: bool = False
    result_meta: Dict[str, Any] = field(default_factory=dict)
    retry_requested: bool = False
    retry_error: Optional[str] = None
    new_jobs: List[Job] = field(default_factory=list)
    cursor_updates: Dict[str, str] = field(default_factory=dict)
    resource_suspensions: List[Tuple[str, float]] = field(default_factory=list)
    lock_conflict: bool = False
    interrupted: bool = False
    going_to_retry: Optional[bool] = None


@dataclass
class JobHandle:
    """一个 in-flight 子进程的句柄。"""
    uid: str
    process: Any
    deadline: float
    timeout: float
    job: Job
    ipc_dir: str
    incarnation: Optional[str] = None


ExecutionHandle = JobHandle


@dataclass(frozen=True)
class AbortOutcome:
    """强制终止/异常停机时的收尾结果。"""
    completed: List[Tuple[JobHandle, ExecutionResult]]
    cancelled: List[JobHandle]


class ExecutionChannelProtocol(Protocol):
    """ExecutionChannel 统一协议接口。"""

    def spawn(
        self,
        handler_func: Callable[[Job, TaskContext], Any],
        job: Job,
        ctx: TaskContext,
        timeout: float,
    ) -> JobHandle: ...

    def poll_completed(
        self, handles: Sequence[JobHandle]
    ) -> List[Tuple[JobHandle, ExecutionResult]]: ...

    def probe_orphan_lock(self, uid: str) -> bool: ...

    def claim_stale_result(self, uid: str, job: Job) -> Optional[ExecutionResult]: ...

    def drain_active_signals(self, uids: Iterable[str]) -> List[Tuple[str, str, float]]: ...

    def abort_in_flight(self, handles: Sequence[JobHandle]) -> AbortOutcome: ...

    def cleanup_in_flight(self, handles: Sequence[JobHandle]) -> None: ...

    def cleanup_artifacts(self, uid: str, *, mode: ArtifactCleanupMode) -> None: ...

    def read_declared_inputs(self, uid: str) -> List[dict]: ...


class ExecutionChannel:
    """标准子进程执行通道深模块实现。"""

    _JOIN_REAP_TIMEOUT: float = 5.0

    def __init__(
        self,
        ipc_dir: Optional[Union[str, Path, Any]] = None,
        *,
        mp_ctx: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        if ipc_dir is not None and hasattr(ipc_dir, "Process") and mp_ctx is None:
            mp_ctx = ipc_dir
            ipc_dir = None
        if ipc_dir is None and "ipc_dir" in kwargs:
            ipc_dir = kwargs["ipc_dir"]
        if mp_ctx is None and "mp_ctx" in kwargs:
            mp_ctx = kwargs["mp_ctx"]

        self._mp_ctx = mp_ctx or mp.get_context("spawn")
        self.ipc_dir = str(ipc_dir) if ipc_dir is not None else None
        if self.ipc_dir:
            try:
                Path(self.ipc_dir).mkdir(parents=True, exist_ok=True)
            except OSError:
                pass

    @property
    def journal(self) -> ArtifactJournal:
        ipc = getattr(self, "ipc_dir", None)
        j = getattr(self, "_journal", None)
        if j is None or j.ipc_dir != ipc:
            j = ArtifactJournal(ipc)
            self._journal = j
        return j

    @journal.setter
    def journal(self, value: ArtifactJournal) -> None:
        self._journal = value

    @staticmethod
    def _finalize_process(p: Any) -> None:
        """清理单个子进程：kill + join + close。"""
        try:
            if p.is_alive():
                p.kill()
        except Exception:
            pass
        try:
            p.join(timeout=ExecutionChannel._JOIN_REAP_TIMEOUT)
        except Exception:
            pass
        try:
            _p_close = getattr(p, "close", None)
            if _p_close is not None:
                _p_close()
        except Exception:
            pass

    def spawn(
        self,
        handler_func: Callable[[Job, TaskContext], Any],
        job: Job,
        ctx: TaskContext,
        timeout: float,
        acquired_resources: Optional[Any] = None,
        ipc_dir: Optional[str] = None,
        **kwargs: Any,
    ) -> JobHandle:
        """启动隔离子进程执行 handler，建立 incarnation fencing，立即返回句柄。"""
        effective_ipc_dir = ipc_dir or self.ipc_dir or os.environ.get(_RESULT_DIR_ENV)
        if not effective_ipc_dir:
            raise ValueError("ipc_dir is required for ExecutionChannel.spawn()")
        Path(effective_ipc_dir).mkdir(parents=True, exist_ok=True)
        ctx.ipc_dir = effective_ipc_dir
        incarnation = getattr(ctx, "incarnation", None)
        p = None
        try:
            p = self._mp_ctx.Process(
                target=_mp_worker_wrapper, args=(handler_func, job, ctx, effective_ipc_dir)
            )
            p.start()
        except Exception:
            if p is not None:
                self._finalize_process(p)
            raise
        deadline = time.monotonic() + timeout
        return JobHandle(
            uid=job.uid,
            process=p,
            deadline=deadline,
            timeout=timeout,
            job=job,
            ipc_dir=effective_ipc_dir,
            incarnation=incarnation,
        )

    # 兼容别名
    submit = spawn

    def poll_completed(
        self, handles: Sequence[JobHandle]
    ) -> List[Tuple[JobHandle, ExecutionResult]]:
        """非阻塞扫描所有 in-flight handle，返回本次已完成的 (handle, result)。"""
        completed: List[Tuple[JobHandle, ExecutionResult]] = []

        for handle in handles:
            now = time.monotonic()
            p = handle.process
            res_path = self.journal.result_path(handle.uid, handle.incarnation)

            res = self.journal.read_result(res_path) if res_path.exists() else None
            if res is not None:
                completed.append((handle, self._collect_outcome(handle, res)))
                continue

            if not p.is_alive():
                res = self.journal.read_result(res_path) if res_path.exists() else None
                completed.append((handle, self._collect_outcome(handle, res)))
                continue

            if now > handle.deadline:
                p.join(timeout=0.5)
                if p.is_alive():
                    try:
                        p.kill()
                    except Exception:
                        pass
                    is_timeout = True
                else:
                    is_timeout = p.exitcode == 0 or p.exitcode is None
                res = self.journal.read_result(res_path) if res_path.exists() else None
                completed.append(
                    (handle, self._collect_outcome(handle, res, is_timeout=is_timeout))
                )

        return completed

    # 兼容别名
    reap_completed = poll_completed

    def _collect_outcome(
        self, handle: JobHandle, res: Optional[dict], *, is_timeout: bool = False
    ) -> ExecutionResult:
        """收敛 drain 三段同构尾部：解析结果 -> 收割进程 -> 清理 IPC 文件。"""
        p = handle.process
        result: Optional[ExecutionResult] = None
        try:
            if res is not None:
                result = _decode_ipc_result(res, p, handle.job, handle.ipc_dir)
            else:
                result = self._build_terminal_failure(p, handle, is_timeout=is_timeout)
        finally:
            self._finalize_process(p)
            try:
                pending_signals = self.journal.drain_signals(handle.uid)
                if pending_signals:
                    if result is not None:
                        result.resource_suspensions = (
                            list(result.resource_suspensions) + list(pending_signals)
                        )
                        logger.info(
                            f"Salvaged {len(pending_signals)} suspend signal(s) "
                            f"from {handle.uid} before IPC cleanup"
                        )
            except Exception as e:
                logger.warning(f"Failed to salvage signals for {handle.uid}: {e}")
            self.journal.cleanup_ipc_files(handle.uid, handle.incarnation)
        return result

    def claim_stale_result(self, uid: str, job: Job) -> Optional[ExecutionResult]:
        """启动/派发前崩溃恢复：认领并消费上次 run 遗留的已落盘残留结果。"""
        res = self.journal.claim_stale_result(uid)
        if res is None:
            return None
        return _decode_ipc_result(res, None, job, self.ipc_dir)

    # 兼容别名
    consume_stale_result = claim_stale_result

    @staticmethod
    def _build_terminal_failure(
        p: Any, handle: JobHandle, *, is_timeout: bool
    ) -> ExecutionResult:
        """构造进程终止（崩溃/超时）但无结果文件时的 ExecutionResult。"""
        exitcode = p.exitcode
        retry_requested = False
        retry_error: Optional[str] = None
        if is_timeout:
            if getattr(handle.job, "timeout_is_transient", False):
                return ExecutionResult(
                    success=False,
                    retry_requested=True,
                    retry_error=f"TIMEOUT ({handle.timeout}s)",
                )
            result_meta = {"error": f"TIMEOUT ({handle.timeout}s)"}
        elif exitcode is not None and exitcode < 0:
            try:
                sig_name = signal.Signals(-exitcode).name
            except (ValueError, AttributeError):
                sig_name = f"SIGNO{-exitcode}"
            result_meta = {
                "error": f"PROCESS_CRASH_EXITCODE_{exitcode}",
                "signal": sig_name,
                "oom_hint": -exitcode == int(signal.SIGKILL),
            }
            retry_requested = True
            retry_error = f"PROCESS_SIGNAL_DEATH: killed by {sig_name} ({exitcode})"
            logger.warning(
                f"Worker for {handle.uid} died by {sig_name}; will retry with backoff"
            )
        elif exitcode is not None and exitcode != 0:
            result_meta = {"error": f"PROCESS_CRASH_EXITCODE_{exitcode}"}
        else:
            result_meta = {"error": "NO_IPC_RESULT"}

        return ExecutionResult(
            success=False,
            result_meta=result_meta,
            retry_requested=retry_requested,
            retry_error=retry_error,
        )

    def probe_orphan_lock(self, uid: str) -> bool:
        """非阻塞探测执行锁。True 表示无孤儿持锁可安全执行；False 表示孤儿活跃需 defer。"""
        return lockfile.probe_lock(self.ipc_dir, uid)

    def drain_active_signals(self, uids: Iterable[str]) -> List[Tuple[str, str, float]]:
        """原子排空所有在途任务追加的 suspend 信号。"""
        signals: List[Tuple[str, str, float]] = []
        for uid in uids:
            for r_name, secs in self.journal.drain_signals(uid):
                signals.append((uid, r_name, secs))
        return signals

    def abort_in_flight(self, handles: Sequence[JobHandle]) -> AbortOutcome:
        """TOCTOU 闭环中止：排空信号 -> 初查分类 -> 进程终止 -> 重探测闭环 -> 残留清理。"""
        if not handles:
            return AbortOutcome(completed=[], cancelled=[])

        done_pairs: List[Tuple[JobHandle, ExecutionResult]] = []
        pending_handles: List[JobHandle] = []

        for h in handles:
            incarnation = getattr(h, "incarnation", None)
            res_p = self.journal.result_path(h.uid, incarnation)
            if res_p.exists():
                raw_res = self.journal.read_result(res_p)
                if (
                    raw_res is not None
                    and isinstance(raw_res, dict)
                    and "status" in raw_res
                    and raw_res.get("status") != "interrupted"
                ):
                    decoded = _decode_ipc_result(raw_res, None, h.job, self.ipc_dir)
                    done_pairs.append((h, decoded))
                    continue
            pending_handles.append(h)

        if pending_handles:
            self.finalize_processes(pending_handles)

        truly_cancelled: List[JobHandle] = []
        for h in pending_handles:
            incarnation = getattr(h, "incarnation", None)
            res_p = self.journal.result_path(h.uid, incarnation)
            if res_p.exists():
                raw_res = self.journal.read_result(res_p)
                if (
                    raw_res is not None
                    and isinstance(raw_res, dict)
                    and "status" in raw_res
                    and raw_res.get("status") != "interrupted"
                ):
                    decoded = _decode_ipc_result(raw_res, None, h.job, self.ipc_dir)
                    done_pairs.append((h, decoded))
                    continue
            self.cleanup_artifacts(h.uid, mode=ArtifactCleanupMode.FAILURE_OR_RETRY)
            truly_cancelled.append(h)

        return AbortOutcome(completed=done_pairs, cancelled=truly_cancelled)

    def cleanup_in_flight(self, handles: Sequence[JobHandle]) -> None:
        """主循环异常退出时 kill + join 所有残留 in-flight 子进程并清理 IPC。"""
        for handle in handles:
            try:
                self._finalize_process(handle.process)
                self.journal.cleanup_ipc_files(handle.uid, handle.incarnation)
            except Exception as e:
                logger.error(f"Error cleaning up in-flight job {handle.uid}: {e}")

    # 兼容别名
    cleanup = cleanup_in_flight

    def finalize_processes(self, handles: Sequence[JobHandle]) -> None:
        """只 kill + join 残留子进程，不删 IPC 文件。"""
        for handle in handles:
            try:
                self._finalize_process(handle.process)
            except Exception as e:
                logger.error(f"Error finalizing in-flight job {handle.uid}: {e}")

    def cleanup_artifacts(self, uid: str, *, mode: ArtifactCleanupMode) -> None:
        """统一收敛产物与 IPC 临时文件的生命周期清理。"""
        self.journal.cleanup(uid, mode=mode)

    def read_declared_inputs(self, uid: str) -> List[dict]:
        """读取任务声明的输入清单。"""
        try:
            return self.journal.read_inputs(uid)
        except Exception:
            return []


MultiprocessingExecutor = ExecutionChannel

__all__ = [
    "AbortOutcome",
    "ArtifactCleanupMode",
    "ExecutionChannel",
    "ExecutionChannelProtocol",
    "ExecutionHandle",
    "ExecutionResult",
    "JobHandle",
    "MultiprocessingExecutor",
    "_decode_ipc_result",
    "_decode_raw_result",
    "_encode_raw_result",
    "_mp_worker_wrapper",
    "_normalize_handler_result",
]
