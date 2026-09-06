"""ExecutionChannel: 收敛子进程生命周期、看门狗、IPC 通信与跨平台文件锁的深模块。

提供极简接缝（ExecutionChannelProtocol），内敛 1200+ 行涉及阶梯看门狗、
两级降级落盘、跨平台排他文件锁与 TOCTOU 闭环清理的底层复杂性。
"""

from __future__ import annotations

import enum
import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple, Union

from ..models.context import TaskContext
from ..models.job import Job
from ..utils.ipc import (
    inputs_path,
    outputs_path,
    signals_path,
)
from ..utils.lockfile import probe_lock, safe_uid_filename
from .executor import (
    ExecutionResult,
    MultiprocessingExecutor,
    _decode_ipc_result,
    _iter_stale_result_paths,
    cleanup_ipc_files,
    read_inputs,
    read_outputs,
    read_result_file,
    read_signals,
    result_path,
)

logger = logging.getLogger("tasklite")


class ArtifactCleanupMode(str, enum.Enum):
    """产物与声明清理模式。"""
    PRE_SUBMIT = "pre_submit"          # 派发前清理已死孤儿的残留声明与信号
    SUCCESS = "success"                # 任务成功：保留 output，清理 cache 临时文件与 IPC/声明文件
    FAILURE_OR_RETRY = "failure_retry" # 任务失败/重试：清理 cleanup_on_fail 产物、cache 临时文件与 IPC 文件


@dataclass(frozen=True)
class ExecutionHandle:
    """in-flight 正在执行的执行体句柄（不可变值对象）。"""
    uid: str
    process: Any
    deadline: float
    timeout: float
    job: Job
    incarnation: str


@dataclass(frozen=True)
class AbortOutcome:
    """强制终止/异常停机时的收尾结果。"""
    completed: List[Tuple[ExecutionHandle, ExecutionResult]]
    cancelled: List[ExecutionHandle]


class ExecutionChannelProtocol(Protocol):
    """ExecutionChannel 统一协议接口。"""

    def spawn(
        self,
        handler_func: Callable[[Job, TaskContext], Any],
        job: Job,
        ctx: TaskContext,
        timeout: float,
    ) -> ExecutionHandle: ...

    def poll_completed(
        self, handles: Sequence[ExecutionHandle]
    ) -> List[Tuple[ExecutionHandle, ExecutionResult]]: ...

    def probe_orphan_lock(self, uid: str) -> bool: ...

    def claim_stale_result(self, uid: str, job: Job) -> Optional[ExecutionResult]: ...

    def drain_active_signals(self, uids: Iterable[str]) -> List[Tuple[str, str, float]]: ...

    def abort_in_flight(self, handles: Sequence[ExecutionHandle]) -> AbortOutcome: ...

    def cleanup_artifacts(self, uid: str, *, mode: ArtifactCleanupMode) -> None: ...

    def read_declared_inputs(self, uid: str) -> List[dict]: ...


class ExecutionChannel:
    """标准子进程执行通道深模块实现。"""

    def __init__(
        self,
        ipc_dir: Union[str, Path],
        *,
        executor: Optional[MultiprocessingExecutor] = None,
        mp_ctx: Optional[Any] = None,
    ) -> None:
        self.ipc_dir = str(ipc_dir)
        Path(self.ipc_dir).mkdir(parents=True, exist_ok=True)
        self._executor = executor or MultiprocessingExecutor(mp_ctx=mp_ctx, ipc_dir=self.ipc_dir)

    @property
    def executor(self) -> MultiprocessingExecutor:
        return self._executor

    def spawn(
        self,
        handler_func: Callable[[Job, TaskContext], Any],
        job: Job,
        ctx: TaskContext,
        timeout: float,
    ) -> ExecutionHandle:
        """启动隔离子进程执行 handler，建立 incarnation fencing，立即返回句柄。"""
        raw_handle = self._executor.submit(
            handler_func, job, ctx, timeout, ipc_dir=self.ipc_dir
        )
        return ExecutionHandle(
            uid=raw_handle.uid,
            process=raw_handle.process,
            deadline=raw_handle.deadline,
            timeout=raw_handle.timeout,
            job=raw_handle.job,
            incarnation=ctx.incarnation,
        )

    def poll_completed(
        self, handles: Sequence[ExecutionHandle]
    ) -> List[Tuple[ExecutionHandle, ExecutionResult]]:
        """非阻塞轮询已完成的执行体（正常完成/崩溃收割/看门狗阶梯终止）。"""
        if not handles:
            return []

        # 适配底层 executor 的 handle 结构
        raw_handles = [
            self._to_raw_handle(h) for h in handles
        ]
        completed_raw = self._executor.reap_completed(raw_handles)

        handle_map = {h.uid: h for h in handles}
        results: List[Tuple[ExecutionHandle, ExecutionResult]] = []
        for raw_h, res in completed_raw:
            if raw_h.uid in handle_map:
                results.append((handle_map[raw_h.uid], res))
        return results

    def probe_orphan_lock(self, uid: str) -> bool:
        """非阻塞探测执行锁。True 表示无孤儿持锁可安全执行；False 表示孤儿活跃需 defer。"""
        return probe_lock(self.ipc_dir, uid)

    def claim_stale_result(self, uid: str, job: Job) -> Optional[ExecutionResult]:
        """启动/派发前崩溃恢复：认领并消费上次 run 遗留的已落盘残留结果。"""
        return self._executor.consume_stale_result(uid, job)

    def drain_active_signals(self, uids: Iterable[str]) -> List[Tuple[str, str, float]]:
        """原子排空所有在途任务追加的 suspend 信号（返回 [(uid, resource_name, seconds), ...]）。"""
        signals: List[Tuple[str, str, float]] = []
        for uid in uids:
            for r_name, secs in read_signals(self.ipc_dir, uid):
                signals.append((uid, r_name, secs))
        return signals

    def abort_in_flight(self, handles: Sequence[ExecutionHandle]) -> AbortOutcome:
        """TOCTOU 闭环中止：排空信号 -> 初查分类 -> 进程终止 -> 重探测闭环 -> 残留清理。"""
        if not handles:
            return AbortOutcome(completed=[], cancelled=[])

        done_pairs: List[Tuple[ExecutionHandle, ExecutionResult]] = []
        pending_handles: List[ExecutionHandle] = []

        # 1. 信号排空与初查
        for h in handles:
            res_p = result_path(self.ipc_dir, h.uid, h.incarnation)
            if res_p.exists():
                raw_res = read_result_file(res_p)
                if raw_res and raw_res.get("status") != "interrupted":
                    decoded = _decode_ipc_result(raw_res, None, h.job, self.ipc_dir)
                    done_pairs.append((h, decoded))
                    continue
            pending_handles.append(h)

        # 2. 终止未完成的子进程
        if pending_handles:
            raw_pending = [self._to_raw_handle(h) for h in pending_handles]
            self._executor.finalize_processes(raw_pending)

        # 3. 重查 TOCTOU 闭环：kill 期间可能恰好写入了结果
        truly_cancelled: List[ExecutionHandle] = []
        for h in pending_handles:
            res_p = result_path(self.ipc_dir, h.uid, h.incarnation)
            consumed = False
            if res_p.exists():
                raw_res = read_result_file(res_p)
                if raw_res and raw_res.get("status") != "interrupted":
                    decoded = _decode_ipc_result(raw_res, None, h.job, self.ipc_dir)
                    done_pairs.append((h, decoded))
                    consumed = True
            if not consumed:
                # 真正未完成任务：清理 IPC 与半成品
                self.cleanup_artifacts(h.uid, mode=ArtifactCleanupMode.FAILURE_OR_RETRY)
                truly_cancelled.append(h)

        return AbortOutcome(completed=done_pairs, cancelled=truly_cancelled)

    def cleanup_artifacts(self, uid: str, *, mode: ArtifactCleanupMode) -> None:
        """统一收敛产物与 IPC 临时文件的生命周期清理。"""
        if mode == ArtifactCleanupMode.PRE_SUBMIT:
            for p in (outputs_path(self.ipc_dir, uid), inputs_path(self.ipc_dir, uid), signals_path(self.ipc_dir, uid)):
                try:
                    if p.exists():
                        p.unlink()
                except OSError:
                    pass
            for sp in _iter_stale_result_paths(self.ipc_dir, uid):
                try:
                    sp.unlink()
                except OSError:
                    pass
            return

        if mode == ArtifactCleanupMode.SUCCESS:
            try:
                for out_path, _, kind in read_outputs(self.ipc_dir, uid):
                    if kind == "cache":
                        Path(out_path).unlink(missing_ok=True)
                outputs_path(self.ipc_dir, uid).unlink(missing_ok=True)
                inputs_path(self.ipc_dir, uid).unlink(missing_ok=True)
            except (OSError, Exception):
                pass
            cleanup_ipc_files(self.ipc_dir, uid)
            return

        if mode == ArtifactCleanupMode.FAILURE_OR_RETRY:
            try:
                for out_path, cleanup, kind in read_outputs(self.ipc_dir, uid):
                    if kind == "cache" or cleanup:
                        out_path_obj = Path(out_path)
                        if out_path_obj.exists():
                            if out_path_obj.is_dir():
                                shutil.rmtree(out_path_obj, ignore_errors=True)
                            else:
                                out_path_obj.unlink(missing_ok=True)
            except Exception:
                pass
            finally:
                for p in (outputs_path(self.ipc_dir, uid), inputs_path(self.ipc_dir, uid)):
                    try:
                        if p.exists():
                            p.unlink()
                    except OSError:
                        pass
                cleanup_ipc_files(self.ipc_dir, uid)

    def read_declared_inputs(self, uid: str) -> List[dict]:
        """读取任务声明的输入清单。"""
        try:
            return read_inputs(self.ipc_dir, uid)
        except Exception:
            return []

    def _to_raw_handle(self, handle: ExecutionHandle) -> Any:
        from .executor import JobHandle
        return JobHandle(
            uid=handle.uid,
            process=handle.process,
            deadline=handle.deadline,
            timeout=handle.timeout,
            job=handle.job,
            ipc_dir=self.ipc_dir,
            incarnation=handle.incarnation,
        )
