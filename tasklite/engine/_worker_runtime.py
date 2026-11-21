"""Worker 侧纯执行模块与跨平台停止信号单一事实源。

本模块仅依赖模型层、IPC 底层工具与异常定义，严禁导入 ``channel``、``launcher``、
``dispatch``、``runtime`` 等父进程调度模块，确保隔离子进程冷启动路径轻量化且彻底
消除 ``channel <-> launcher`` 环形依赖。
"""

from __future__ import annotations

import logging
import os
import signal
import threading
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ..exceptions import _ArtifactIOError
from ..models.context import JobContext
from ..models.job import Job
from ..models.result import TaskResult
from ..utils.ipc import WorkerPipeSender, WorkerReplyReceiver

logger = logging.getLogger("tasklite")


def stop_signals(signal_module: Any = signal) -> tuple[Any, ...]:
    """返回当前平台统一的停止信号集合（主进程注册与 worker 屏蔽共用唯一事实源）。"""
    sigs: list[Any] = [signal_module.SIGTERM, signal_module.SIGINT]
    sigbreak = getattr(signal_module, "SIGBREAK", None)
    if os.name == "nt" and sigbreak is not None:
        sigs.append(sigbreak)
    return tuple(sigs)


def ignore_worker_stop_signals() -> None:
    """在 worker 主线程忽略平台停止信号，避免主进程停机信号穿透 worker。

    进程内线程替身在非主线程运行，不修改宿主主线程信号表。
    """
    if threading.current_thread() is not threading.main_thread():
        return
    for sig in stop_signals():
        signal.signal(sig, signal.SIG_IGN)


@dataclass(frozen=True)
class WorkerLaunchSpec:
    """进程 seam 具名契约——下发 worker 执行体的不可变启动载荷。

    ``execution_id``（``{run_id}.{dispatch_no}`` 执行身份）与管道端点归本 spec，
    不借道 ``JobContext`` 公开属性穿透进程边界，亦不携带父进程控制回调。
    """

    handler: Callable[[Job, JobContext], Any]
    job: Job
    job_context: JobContext
    execution_id: str
    timeout_seconds: float
    pipe_sender: WorkerPipeSender | None = None
    reply_receiver: WorkerReplyReceiver | None = None
    run_id: str = ""
    attempt_no: int = 1


def run_worker_entry(spec: WorkerLaunchSpec) -> None:
    """Worker 执行体入口：隔离信号、执行 handler 并经瞬时管道单向回传结果。

    不变式：执行体任何出口（含未捕获异常）均在 ``finally`` 中关闭管道端点。
    """
    ignore_worker_stop_signals()
    handler_func = spec.handler
    job = spec.job
    if getattr(spec, "attempt_no", None) is not None:
        job._attempt_no = spec.attempt_no
    ctx = spec.job_context
    job_uid = job.uid
    execution_id = spec.execution_id
    if not execution_id:
        raise RuntimeError(
            f"worker started without execution_id for {job_uid}: "
            f"fencing requires WorkerLaunchSpec.execution_id"
        )
    pipe_sender = spec.pipe_sender
    reply_receiver = spec.reply_receiver
    if pipe_sender is not None or reply_receiver is not None:
        ctx._bind_pipe_sender(pipe_sender, reply_receiver)

    def _write_result(payload: dict[str, Any]) -> None:
        if pipe_sender is None:
            return
        try:
            pipe_sender.send_result(payload)
        except OSError as exc:
            logger.error(
                f"worker pipe result send failed for {job_uid} ({execution_id}): {exc}"
            )

    try:
        try:
            raw_result = handler_func(job, ctx)
            if isinstance(raw_result, TaskResult):
                if raw_result.status == "success":
                    _write_result({
                        "status": "success",
                        "message": raw_result.message,
                        "metadata": raw_result.metadata,
                        "spawned_jobs": [j.to_dict() for j in ctx._spawned_jobs],
                        "cursor_updates": dict(ctx._cursor_updates),
                    })
                elif raw_result.status == "failure":
                    _write_result({
                        "status": "failure",
                        "message": raw_result.message,
                        "metadata": raw_result.metadata,
                        "exception": None,
                    })
                else:
                    _write_result({
                        "status": "retry",
                        "message": raw_result.message,
                        "metadata": raw_result.metadata,
                        "consume_budget": raw_result.consume_budget,
                    })
            else:
                _write_result({
                    "status": "failure",
                    "message": (
                        f"invalid handler return type: expected TaskResult, "
                        f"got {type(raw_result).__name__}"
                    ),
                    "metadata": {},
                    "exception": None,
                })
        except _ArtifactIOError as e:
            _write_result({"status": "artifact_error", "error_message": str(e)})
        except BaseException as e:
            _write_result({
                "status": "failure",
                "message": (
                    type(e).__name__ if not str(e)
                    else f"{type(e).__name__}: {e}"
                ),
                "metadata": {},
                "exception": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                },
            })
    finally:
        if pipe_sender is not None:
            pipe_sender.close()
        if reply_receiver is not None:
            reply_receiver.close()


__all__ = [
    "WorkerLaunchSpec",
    "ignore_worker_stop_signals",
    "run_worker_entry",
    "stop_signals",
]
