"""v2 执行通道：子进程 spawn、阶梯看门狗、瞬时 Pipe 收割、fencing 与 TOCTOU 中止。

ExecutionChannel 统一内敛子进程派发、看门狗阶梯终止、单向 Pipe 消息收割、
跨平台排他文件锁探测与 TOCTOU 闭环清理的底层复杂性。

spawn 上下文显式取 ``mp.get_context("spawn")``：fork 会把父进程的锁
状态/线程/连接池半成品复制进子进程（引擎侧任何锁的持有状态都会在
子进程镜像里失真），spawn 注册表经 WorkerLaunchSpec 显式下发、不经
继承，进程 seam 的全部载荷具名可审计。
"""

from __future__ import annotations

import dataclasses
import logging
import multiprocessing as mp
import os
import signal
import threading
import time
import traceback
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..exceptions import TaskLiteError, _ArtifactIOError
from ..models.context import JobContext
from ..models.job import Job
from ..models.result import TaskResult, validate_strict_json_object
from ..utils.clock import utc_now_iso
from ..utils.ipc import (
    PIPE_MSG_DECLARE,
    PIPE_MSG_RESULT,
    WorkerPipeMessage,
    WorkerPipeReceiver,
    WorkerPipeSender,
    WorkerReplyReceiver,
    WorkerReplySender,
    create_worker_pipe,
    create_worker_reply_pipe,
)
from .types import AuthorizationOutcome, JobHandle

logger = logging.getLogger("tasklite")


def _parse_spawned_jobs(raw_spawned_jobs: Any) -> tuple[list[Job], str | None]:
    """解析结果负载的 spawned_jobs 字段；坏形态收敛为错误串，绝不抛 TypeError。

    不变式：结果负载任何异常形态都收敛为任务级失败记账，不得以
    TypeError 穿透收割链使单任务损坏放大为整管崩溃。
    """
    if not isinstance(raw_spawned_jobs, list):
        return [], f"invalid spawned_jobs type: {type(raw_spawned_jobs).__name__}"
    spawned_jobs: list[Job] = []
    for job_snapshot in raw_spawned_jobs:
        try:
            spawned_jobs.append(Job.from_dict(job_snapshot))
        except (KeyError, TypeError, ValueError, TaskLiteError) as e:
            return [], f"invalid spawned job dict: {e}"
    return spawned_jobs, None


def _parse_cursor_updates(raw_cursors: Any) -> tuple[dict[str, str | None], str | None]:
    """解析 cursor_updates 字段：键 str、值 str/None，其余收敛为错误串。"""
    if not isinstance(raw_cursors, dict):
        return {}, f"invalid cursor_updates type: {type(raw_cursors).__name__}"
    for key, value in raw_cursors.items():
        if not (isinstance(key, str) and (value is None or isinstance(value, str))):
            return {}, f"invalid cursor_updates value: {key!r}={value!r}"
    return dict(raw_cursors), None


def _validate_exception_dict(raw_exc: Any) -> dict[str, str] | None:
    """校验 worker 传递的结构化异常字典（type / message / traceback）。"""
    if raw_exc is None:
        return None
    if type(raw_exc) is not dict:
        raise TypeError(f"exception must be a dict or None, got {type(raw_exc).__name__}")
    exc_type = raw_exc.get("type")
    exc_msg = raw_exc.get("message")
    exc_tb = raw_exc.get("traceback")
    if (
        type(exc_type) is not str
        or type(exc_msg) is not str
        or type(exc_tb) is not str
    ):
        raise TypeError("exception dict must contain str fields 'type', 'message', 'traceback'")
    return {
        "type": exc_type,
        "message": exc_msg,
        "traceback": exc_tb,
    }


def _decode_success_branch(
    res: dict[str, Any], job: Job
) -> tuple[
    bool,
    str | None,
    dict[str, Any],
    list[Job],
    dict[str, str | None],
]:
    """success 状态解码：metadata 严格 JSON 对象校验 + 子字段防御解析。

    返回 (success, message, result_metadata, spawned_jobs, cursor_updates)；
    任一子段损坏即 success=False、message 携带错误串且 result_metadata 为空字典（不混入框架键）。
    """
    if "metadata" not in res:
        logger.error(f"Corrupt result payload for {job.uid}: missing 'metadata' key")
        return False, "Invalid worker result: missing metadata", {}, [], {}
    raw_msg = res.get("message")
    if raw_msg is not None and type(raw_msg) is not str:
        logger.error(f"Corrupt result payload for {job.uid}: invalid 'message' type")
        return False, "Invalid worker result: invalid message type", {}, [], {}
    try:
        result_metadata = validate_strict_json_object(
            res["metadata"], field_name="metadata"
        )
    except Exception as e:
        logger.error(f"Result decode failed for {job.uid}: {e}")
        return (
            False,
            f"Invalid worker result: decode failed: {e}",
            {},
            [],
            {},
        )

    spawned_jobs, jobs_error = _parse_spawned_jobs(res.get("spawned_jobs", []))
    if jobs_error is not None:
        logger.error(f"Malformed spawned jobs in {job.uid}: {jobs_error}")
        return False, jobs_error, {}, [], {}

    cursor_updates, cursor_error = _parse_cursor_updates(res.get("cursor_updates", {}))
    if cursor_error is not None:
        logger.error(f"Invalid cursor_updates in {job.uid}: {cursor_error}")
        return False, cursor_error, {}, [], {}
    return True, raw_msg, result_metadata, spawned_jobs, cursor_updates


def _unexpected_result_fields(res: dict[str, Any], allowed: frozenset[str]) -> list[str]:
    return sorted(set(res) - allowed)


def _corrupt_result(job: Job, reason: str) -> "ExecutionResult":
    message = f"Invalid worker result: {reason}"
    logger.error(f"Invalid worker result for {job.uid}: {reason}")
    return ExecutionResult(
        success=False,
        message=message,
    )


def _decode_ipc_result(
    res: dict[str, Any],
    job: Job,
) -> "ExecutionResult":
    """解析子进程通过管道回传的显式结果字典。

    损坏或未知形态一律收敛为一次任务级失败；进程退出、超时和外部中断
    不伪造 TaskResult.retry。
    """
    if not isinstance(res, dict):
        return _corrupt_result(job, f"payload must be a dict, got {type(res).__name__}")

    status = res.get("status")
    if status == "success":
        unexpected = _unexpected_result_fields(
            res,
            frozenset(
                {"status", "message", "metadata", "spawned_jobs", "cursor_updates"}
            ),
        )
        if unexpected:
            return _corrupt_result(job, f"unsupported fields {unexpected}")
        success, message, result_metadata, spawned_jobs, cursor_updates = (
            _decode_success_branch(res, job)
        )
        if not success:
            return _corrupt_result(job, str(message).removeprefix("Invalid worker result: "))
        return ExecutionResult(
            success=True,
            result_metadata=result_metadata,
            message=message,
            spawned_jobs=spawned_jobs,
            cursor_updates=cursor_updates,
        )

    if status == "failure":
        unexpected = _unexpected_result_fields(
            res, frozenset({"status", "message", "metadata", "exception"})
        )
        if unexpected:
            return _corrupt_result(job, f"unsupported fields {unexpected}")
        try:
            raw_msg = res.get("message")
            if raw_msg is not None and type(raw_msg) is not str:
                raise TypeError("message must be str or None")
            result_metadata = validate_strict_json_object(
                res.get("metadata", {}), field_name="metadata"
            )
            exception = _validate_exception_dict(res.get("exception"))
            return ExecutionResult(
                success=False,
                result_metadata=result_metadata,
                message=raw_msg,
                exception=exception,
            )
        except Exception as e:
            return _corrupt_result(job, f"invalid failure payload: {e}")

    if status == "retry":
        unexpected = _unexpected_result_fields(
            res, frozenset({"status", "message", "metadata", "consume_budget"})
        )
        if unexpected:
            return _corrupt_result(job, f"unsupported fields {unexpected}")
        try:
            raw_msg = res.get("message")
            if raw_msg is not None and type(raw_msg) is not str:
                raise TypeError("message must be str or None")
            result_metadata = validate_strict_json_object(
                res.get("metadata", {}), field_name="metadata"
            )
            raw_consume = res.get("consume_budget")
            if type(raw_consume) is not bool:
                raise TypeError("consume_budget must be bool")
            return ExecutionResult(
                success=False,
                result_metadata=result_metadata,
                message=raw_msg,
                consume_budget=raw_consume,
                retry_requested=True,
            )
        except Exception as e:
            return _corrupt_result(job, f"invalid retry payload: {e}")

    if status == "artifact_error":
        unexpected = _unexpected_result_fields(
            res, frozenset({"status", "error_message"})
        )
        if unexpected:
            return _corrupt_result(job, f"unsupported fields {unexpected}")
        raw_error = res.get("error_message")
        if type(raw_error) is not str or not raw_error:
            return _corrupt_result(job, "artifact_error requires a non-empty error_message")
        return ExecutionResult(success=False, artifact_error=raw_error)

    if status is None:
        return _corrupt_result(job, "missing status")
    return _corrupt_result(job, f"unknown status {status!r}")


def _isolate_worker_process_signals() -> None:
    """让进程组停止信号只由主进程处理。"""
    if os.name != "posix":
        return
    setsid = getattr(os, "setsid", None)
    if setsid is not None:
        try:
            setsid()
        except OSError:
            pass
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


def _worker_process_entry(spec: "WorkerLaunchSpec") -> None:
    """spawn 子进程执行体入口：执行 handler 并经瞬时管道发送结果。

    不变式：结果通过管道单向回传；执行体任何出口（含未捕获异常）
    均在 finally 统一关闭管道写端。
    """
    _isolate_worker_process_signals()
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


@dataclass
class ExecutionResult:
    """单次子进程执行的收割产物（收割/中止路径共形）。"""

    success: bool = False
    result_metadata: dict[str, Any] = field(default_factory=dict)
    message: str | None = None
    exception: dict[str, str] | None = None
    consume_budget: bool = True
    retry_requested: bool = False
    spawned_jobs: list[Job] = field(default_factory=list)
    cursor_updates: dict[str, str | None] = field(default_factory=dict)
    will_retry: bool = False
    artifact_error: str | None = None
    authorization_outcome: AuthorizationOutcome | None = None
    annotations: dict[str, Any] = field(default_factory=dict)

    @property
    def metadata(self) -> dict[str, Any]:
        """业务结果元数据，完整保留用户合法键名。"""
        return self.result_metadata

    @property
    def error(self) -> str:
        """结果说明或错误消息。"""
        return self.message if self.message is not None else ""

    @property
    def traceback(self) -> str | None:
        """结构化异常堆栈（无异常时为 None）。"""
        if isinstance(self.exception, dict):
            return self.exception.get("traceback")
        return None


@dataclass(frozen=True)
class WorkerLaunchSpec:
    """进程 seam 具名契约——spawn 下发子进程执行体的全部载荷。

    execution_id（``{run_id}.{dispatch_no}`` 执行身份）与管道发送端归
    本 spec，不借道 JobContext 公开属性穿透进程边界。
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
    launch_guard: Callable[[], bool] | None = None


@dataclass(frozen=True)
class AbortOutcome:
    """强制终止/异常停机时的收尾结果。"""

    completed: list[tuple[JobHandle, ExecutionResult]]
    cancelled: list[JobHandle]


class _WorkerLaunchCancelled(Exception):
    """最终启动门前观察到停止请求，子进程不得启动。"""


class ExecutionChannel:
    """标准子进程执行通道深模块实现。"""

    _JOIN_REAP_TIMEOUT: float = 5.0

    def __init__(
        self,
        *,
        process_context: Any | None = None,
        output_roots: str | Path | Sequence[str | Path] | None = None,
        protected_roots: Sequence[str | Path] = (),
    ) -> None:
        self._process_context = process_context or mp.get_context("spawn")
        self.output_roots = output_roots
        self.protected_roots = tuple(protected_roots)
        self._receivers: dict[str, WorkerPipeReceiver] = {}
        self._in_process_senders: dict[str, WorkerPipeSender] = {}
        self._reply_senders: dict[str, WorkerReplySender] = {}
        self._declaration_handler: Callable[[JobHandle, dict[str, Any]], dict[str, Any] | None] | None = None
        self._wakeup_callback: Callable[[], None] | None = None
        self._handles: dict[str, JobHandle] = {}
        self._results: dict[str, dict[str, Any]] = {}
        self._errors: dict[str, Exception] = {}
        self._lingering_workers: list[tuple[str, Any, WorkerPipeReceiver | None]] = []
        self._health_stops: dict[str, threading.Event] = {}

    @staticmethod
    def _terminate_and_join_process(p: Any) -> None:
        """终止并等待子进程退出（不调用 close，保留 exitcode 供解码读取）。"""
        if getattr(p, "_closed", False):
            return
        try:
            if p.is_alive():
                p.kill()
        except Exception as e:
            logger.warning(f"Process kill failed during finalize: {e}")
        try:
            if not isinstance(p, mp.process.BaseProcess) or getattr(p, "_popen", None) is not None:
                p.join(timeout=ExecutionChannel._JOIN_REAP_TIMEOUT)
        except Exception as e:
            logger.warning(f"Process join failed during finalize: {e}")

    @staticmethod
    def _finalize_process(p: Any) -> None:
        """清理单个子进程：kill + join + close（各步失败降级但留痕，不阻断收割）。"""
        if getattr(p, "_closed", False):
            return
        ExecutionChannel._terminate_and_join_process(p)
        try:
            process_close = getattr(p, "close", None)
            if process_close is not None:
                process_close()
        except Exception as e:
            logger.warning(f"Process close failed during finalize: {e}")

    def spawn(self, spec: WorkerLaunchSpec) -> JobHandle:
        """启动隔离子进程执行 handler，建立瞬时管道与 execution_id 绑定，立即返回句柄。"""
        receiver, sender = create_worker_pipe(
            self._process_context,
            job_uid=spec.job.uid,
            execution_id=spec.execution_id,
            run_id=spec.run_id or None,
            attempt_no=spec.attempt_no,
            on_message=self._notify_wakeup,
        )
        reply_receiver, reply_sender = create_worker_reply_pipe(
            self._process_context,
            job_uid=spec.job.uid,
            execution_id=spec.execution_id,
            run_id=spec.run_id or None,
            attempt_no=spec.attempt_no,
        )
        spec.job_context._bind_pipe_sender(sender, reply_receiver)
        launch_spec = dataclasses.replace(
            spec,
            pipe_sender=sender,
            reply_receiver=reply_receiver,
            launch_guard=None,
        )
        p = None
        try:
            p = self._process_context.Process(target=_worker_process_entry, args=(launch_spec,))
            if spec.launch_guard is not None and not spec.launch_guard():
                raise _WorkerLaunchCancelled(
                    f"stop requested before worker process start for {spec.job.uid}"
                )
            started_at = utc_now_iso()
            p.start()
        except Exception:
            sender.close()
            receiver.close()
            reply_receiver.close()
            reply_sender.close()
            if p is not None:
                self._finalize_process(p)
            raise
        if isinstance(p, mp.process.BaseProcess):
            sender.close()
            reply_receiver.close()
        else:
            self._in_process_senders[spec.execution_id] = sender
        self._reply_senders[spec.execution_id] = reply_sender
        timeout_at_monotonic = time.monotonic() + spec.timeout_seconds
        handle = JobHandle(
            job_uid=spec.job.uid,
            process=p,
            timeout_at_monotonic=timeout_at_monotonic,
            timeout_seconds=spec.timeout_seconds,
            started_at=started_at,
            job=spec.job,
            execution_id=spec.execution_id,
            run_id=spec.run_id or None,
            attempt_no=spec.attempt_no,
        )
        self._receivers[spec.execution_id] = receiver
        self._handles[spec.execution_id] = handle
        self._watch_process_health(spec.execution_id, p)
        return handle

    def _watch_process_health(self, execution_id: str, process: Any) -> None:
        """独立探测 worker 存活，仅在状态变化时唤醒主循环。"""
        stop = threading.Event()
        self._health_stops[execution_id] = stop

        def watch() -> None:
            try:
                while not stop.wait(0.05):
                    try:
                        if not process.is_alive():
                            self._notify_wakeup()
                            break
                    except Exception:
                        self._notify_wakeup()
                        break
            finally:
                self._health_stops.pop(execution_id, None)

        threading.Thread(
            target=watch,
            name=f"tasklite-worker-health-{execution_id}",
            daemon=True,
        ).start()

    def _accept_message(self, handle: JobHandle, msg: WorkerPipeMessage) -> None:
        """消费一条已校验信封的管道消息并更新通道内存状态。"""
        if msg.kind == PIPE_MSG_DECLARE:
            if not isinstance(msg.payload, dict):
                raise ValueError(f"invalid declare payload for {handle.job_uid}")
            reply_sender = self._reply_senders.get(handle.execution_id or "")
            if self._declaration_handler is not None and reply_sender is not None:
                reply = self._declaration_handler(handle, msg.payload)
                if reply is not None:
                    try:
                        reply_sender.send_reply(reply)
                    except OSError:
                        pass
        elif msg.kind == PIPE_MSG_RESULT:
            if not isinstance(msg.payload, dict):
                raise ValueError(f"invalid result payload for {handle.job_uid}")
            exec_id = handle.execution_id or ""
            if exec_id not in self._errors:
                self._results.setdefault(exec_id, msg.payload)

    def _process_pipe_item(
        self, handle: JobHandle, item: WorkerPipeMessage | Exception
    ) -> None:
        exec_id = handle.execution_id or ""
        if isinstance(item, OSError):
            logger.debug(
                f"Worker pipe stream ended with OSError for {handle.job_uid} ({exec_id}): {item}"
            )
            return
        if isinstance(item, Exception):
            if exec_id not in self._results:
                self._errors.setdefault(exec_id, item)
            return
        try:
            self._accept_message(handle, item)
        except Exception as exc:
            if exec_id not in self._results:
                self._errors.setdefault(exec_id, exc)

    def _receive_available(self, handle: JobHandle) -> None:
        """非阻塞拉取 handle 对应接收端当前已入队的全部消息。"""
        exec_id = handle.execution_id
        if not exec_id:
            return
        receiver = self._receivers.get(exec_id)
        if receiver is None:
            return
        for item in receiver.take_messages():
            self._process_pipe_item(handle, item)

    def _drain_finished(self, handle: JobHandle) -> None:
        """在进程终止后关闭写端并排空接收端直至 EOF。"""
        exec_id = handle.execution_id
        if not exec_id:
            return
        sender = self._in_process_senders.pop(exec_id, None)
        if sender is not None:
            sender.close()
        receiver = self._receivers.get(exec_id)
        if receiver is None:
            return
        for item in receiver.drain_to_eof(timeout=self._JOIN_REAP_TIMEOUT):
            self._process_pipe_item(handle, item)

    def set_declaration_handler(
        self,
        handler: Callable[[JobHandle, dict[str, Any]], dict[str, Any] | None] | None,
    ) -> None:
        self._declaration_handler = handler

    def set_wakeup_callback(self, callback: Callable[[], None] | None) -> None:
        """绑定 IPC 消息到达时的主循环唤醒入口。"""
        self._wakeup_callback = callback

    def _notify_wakeup(self) -> None:
        callback = self._wakeup_callback
        if callback is not None:
            callback()

    def terminate_process(self, handle: JobHandle) -> None:
        """终止指定作业的子进程。"""
        p = handle.process
        if p is not None:
            try:
                if p.is_alive():
                    p.kill()
            except Exception:
                pass

    def get_reply_sender(self, execution_id: str | None) -> WorkerReplySender | None:
        """获取指定 execution_id 的回复发送端。"""
        if not execution_id:
            return None
        return self._reply_senders.get(execution_id)

    def _close_handle_pipe(self, execution_id: str | None) -> None:
        if not execution_id:
            return
        stop = self._health_stops.pop(execution_id, None)
        if stop is not None:
            stop.set()
        sender = self._in_process_senders.pop(execution_id, None)
        if sender is not None:
            sender.close()
        receiver = self._receivers.pop(execution_id, None)
        if receiver is not None:
            receiver.close(timeout=self._JOIN_REAP_TIMEOUT)
        reply_sender = self._reply_senders.pop(execution_id, None)
        if reply_sender is not None:
            reply_sender.close()

    def _consume_result_if_present(self, handle: JobHandle) -> ExecutionResult | None:
        """读取已收到的结果负载 → 守卫过滤 → 解码。

        守卫契约：任何字典结果都是一次尝试结果；内容损坏由解码器收敛为失败。
        """
        exec_id = getattr(handle, "execution_id", None)
        if not exec_id or exec_id in self._errors:
            return None
        raw_res = self._results.get(exec_id)
        if not isinstance(raw_res, dict):
            return None
        self._results.pop(exec_id, None)
        return _decode_ipc_result(raw_res, handle.job)

    def _reap_lingering_workers(self) -> None:
        """非阻塞轮询已结算但残留的子进程与管道接收端，进程死亡后回收句柄。"""
        still_lingering: list[tuple[str, Any, WorkerPipeReceiver | None]] = []
        for exec_id, p, receiver in self._lingering_workers:
            try:
                alive = p.is_alive()
            except Exception:
                alive = False
            if not alive:
                if receiver is not None:
                    try:
                        receiver.close(timeout=0.2)
                    except Exception as e:
                        logger.debug(f"Lingering receiver close warning for {exec_id}: {e}")
                self._finalize_process(p)
            else:
                if receiver is not None and receiver.eof:
                    try:
                        receiver.close(timeout=0.1)
                        receiver = None
                    except Exception:
                        pass
                still_lingering.append((exec_id, p, receiver))
        self._lingering_workers = still_lingering

    def wait_for_lingering_workers(self) -> None:
        """正常 Run 收口前等待已结算 worker 退出，超时后回收残留进程。"""
        if not self._lingering_workers:
            return
        deadline = time.monotonic() + self._JOIN_REAP_TIMEOUT
        lingering = self._lingering_workers
        self._lingering_workers = []
        for exec_id, process, receiver in lingering:
            remaining = deadline - time.monotonic()
            try:
                while process.is_alive() and remaining > 0:
                    process.join(timeout=min(0.05, remaining))
                    remaining = deadline - time.monotonic()
                    if process.is_alive() and remaining > 0:
                        time.sleep(min(0.01, remaining))
            except Exception as exc:
                logger.warning(f"Settled worker wait failed for {exec_id}: {exc}")
            if process.is_alive():
                logger.warning(f"Settled worker {exec_id} did not exit before Run end")
            self._finalize_process(process)
            if receiver is not None:
                try:
                    receiver.close(timeout=0.2)
                except Exception as exc:
                    logger.debug(f"Settled receiver close warning for {exec_id}: {exc}")

    def reap_completed(
        self, handles: Sequence[JobHandle]
    ) -> list[tuple[JobHandle, ExecutionResult]]:
        """非阻塞扫描所有 in-flight handle，返回本次已完成的 (handle, result)。

        有效结果到达即结算：收到完整有效 TaskResult 即认为本次执行已返回并
        交付结算，不将 worker 进程退出作为额外成功条件，不以前置 join 阻塞
        主循环。结果返回后仍在运行的子进程移交后台非阻塞回收，不阻塞其他 Job
        派发或结算，亦不因之后的进程退出状态改写有效结果。
        """
        self._reap_lingering_workers()
        completed: list[tuple[JobHandle, ExecutionResult]] = []

        for handle in handles:
            now = time.monotonic()
            p = handle.process
            exec_id = handle.execution_id or ""
            self._receive_available(handle)

            authorization_outcome = handle.authorization_outcome
            if authorization_outcome is not None:
                is_alive = False
                try:
                    is_alive = p.is_alive()
                except Exception:
                    pass
                if not is_alive:
                    self._handles.pop(exec_id, None)
                    receiver = self._receivers.pop(exec_id, None)
                    if receiver is not None:
                        try:
                            receiver.close(timeout=0.2)
                        except Exception:
                            pass
                    self._close_handle_pipe(exec_id)
                    self._results.pop(exec_id, None)
                    self._errors.pop(exec_id, None)
                    self._finalize_process(p)
                    decoded = ExecutionResult(
                        success=False,
                        message=authorization_outcome.message,
                        exception=authorization_outcome.exception,
                        authorization_outcome=authorization_outcome,
                    )
                    completed.append((handle, decoded))
                continue

            # 1. 有效结果已到达：立即收割结果，不阻塞等待子进程退出
            if exec_id in self._results:
                raw_res = self._results.pop(exec_id)
                decoded = _decode_ipc_result(raw_res, handle.job)
                self._handles.pop(exec_id, None)
                receiver = self._receivers.pop(exec_id, None)
                self._errors.pop(exec_id, None)
                # 结果后不再回复声明；接收端独立移交，不等待 worker 退出。
                self._close_handle_pipe(exec_id)

                is_alive = False
                try:
                    is_alive = p.is_alive()
                except Exception:
                    pass

                if not is_alive:
                    if receiver is not None:
                        try:
                            receiver.close(timeout=0.2)
                        except Exception:
                            pass
                    self._finalize_process(p)
                else:
                    self._lingering_workers.append((exec_id, p, receiver))

                completed.append((handle, decoded))
                continue

            # 2. 管道异常分支
            if exec_id in self._errors:
                completed.append((handle, self._collect_outcome(handle)))
                continue

            # 3. 进程已死亡但无有效结果（崩溃/被终止/无结果退出）
            if not p.is_alive():
                completed.append((handle, self._collect_outcome(handle)))
                continue

            # 4. 超时：从启动到结果到达计单次超时（等待授权期间暂停不计）
            if not handle.waiting_auth and now > handle.timeout_at_monotonic:
                p.join(timeout=0.5)
                if p.is_alive():
                    try:
                        p.kill()
                    except Exception:
                        pass
                completed.append(
                    (handle, self._collect_outcome(handle, is_timeout=True))
                )

        return completed

    def _collect_outcome(
        self, handle: JobHandle, res: dict | None = None, *, is_timeout: bool = False
    ) -> ExecutionResult:
        """收敛收割尾部：终止并收割子进程 → 排空管道至 EOF → 解码结果 → 关闭管道。"""
        authorization_outcome = handle.authorization_outcome
        if authorization_outcome is not None:
            p = handle.process
            exec_id = handle.execution_id or ""
            try:
                self._terminate_and_join_process(p)
            finally:
                self._finalize_process(p)
                self._close_handle_pipe(exec_id)
            self._results.pop(exec_id, None)
            self._errors.pop(exec_id, None)
            return ExecutionResult(
                success=False,
                message=authorization_outcome.message,
                exception=authorization_outcome.exception,
                authorization_outcome=authorization_outcome,
            )
        p = handle.process
        exec_id = handle.execution_id or ""
        result: ExecutionResult | None = None
        try:
            self._terminate_and_join_process(p)
            self._drain_finished(handle)
            if exec_id in self._errors:
                err = self._errors.pop(exec_id)
                self._results.pop(exec_id, None)
                raise err
            payload = res if res is not None else self._results.pop(exec_id, None)
            if payload is not None and not is_timeout:
                result = _decode_ipc_result(payload, handle.job)
            else:
                result = self._build_terminal_failure(p, handle, is_timeout=is_timeout)
        finally:
            self._finalize_process(p)
            self._close_handle_pipe(exec_id)
            self._handles.pop(exec_id, None)
            self._results.pop(exec_id, None)
        return result

    @staticmethod
    def _build_terminal_failure(
        p: Any, handle: JobHandle, *, is_timeout: bool
    ) -> ExecutionResult:
        """构造进程终止（崩溃/超时）但无有效结果时的失败证据。"""
        exitcode = getattr(p, "exitcode", None)
        annotations: dict[str, Any] = {}
        if is_timeout:
            message = f"Worker timed out after {handle.timeout_seconds}s"
            annotations["timed_out"] = True
        elif isinstance(exitcode, int) and not isinstance(exitcode, bool) and exitcode < 0:
            try:
                signal_name = signal.Signals(-exitcode).name
            except (ValueError, AttributeError):
                signal_name = f"SIGNO{-exitcode}"
            message = f"Worker exited on signal {signal_name} ({exitcode}) without a result"
            annotations["signal"] = signal_name
        elif isinstance(exitcode, int) and not isinstance(exitcode, bool) and exitcode > 0:
            message = f"Worker exited with code {exitcode} without a result"
        else:
            message = "Worker exited without a result frame"
        if isinstance(exitcode, int) and not isinstance(exitcode, bool):
            annotations["exitcode"] = exitcode
        return ExecutionResult(
            success=False,
            message=message,
            annotations=annotations,
        )

    def abort_in_flight(self, handles: Sequence[JobHandle]) -> AbortOutcome:
        """TOCTOU 闭环中止：初查分类 → 进程终止 → 排空管道至 EOF → 重探测闭环 → 关闭管道。"""
        if not handles:
            return AbortOutcome(completed=[], cancelled=[])

        done_pairs: list[tuple[JobHandle, ExecutionResult]] = []
        pending_handles: list[JobHandle] = []

        for handle in handles:
            self._receive_available(handle)
            decoded = self._consume_result_if_present(handle)
            if decoded is not None:
                done_pairs.append((handle, decoded))
                continue
            pending_handles.append(handle)

        for handle, _ in done_pairs:
            try:
                handle.process.join(timeout=self._JOIN_REAP_TIMEOUT)
            except Exception as error:
                logger.warning(f"Completed process join failed: {error}")
        self.finalize_processes(handles)

        for handle, decoded in done_pairs:
            self._drain_finished(handle)
            exec_id = handle.execution_id or ""
            self._close_handle_pipe(exec_id)
            self._handles.pop(exec_id, None)
            self._results.pop(exec_id, None)
            self._errors.pop(exec_id, None)

        truly_cancelled: list[JobHandle] = []
        for handle in pending_handles:
            self._drain_finished(handle)
            decoded = self._consume_result_if_present(handle)
            if decoded is not None:
                done_pairs.append((handle, decoded))
            else:
                truly_cancelled.append(handle)
            exec_id = handle.execution_id or ""
            self._close_handle_pipe(exec_id)
            self._handles.pop(exec_id, None)
            self._results.pop(exec_id, None)
            self._errors.pop(exec_id, None)

        return AbortOutcome(completed=done_pairs, cancelled=truly_cancelled)

    def cleanup_in_flight(self, handles: Sequence[JobHandle]) -> None:
        """主循环异常退出时 kill + join 所有残留 in-flight 子进程并关闭管道与声明。"""
        for exec_id, p, receiver in self._lingering_workers:
            try:
                self._finalize_process(p)
                if receiver is not None:
                    receiver.close(timeout=0.5)
            except Exception as e:
                logger.error(f"Error cleaning up lingering worker {exec_id}: {e}")
        self._lingering_workers.clear()

        for handle in handles:
            try:
                self._finalize_process(handle.process)
                exec_id = handle.execution_id or ""
                self._close_handle_pipe(exec_id)
                self._handles.pop(exec_id, None)
                self._results.pop(exec_id, None)
                self._errors.pop(exec_id, None)
            except Exception as e:
                logger.error(f"Error cleaning up in-flight job {handle.job_uid}: {e}")

    def finalize_processes(self, handles: Sequence[JobHandle]) -> None:
        """只 kill + join 残留子进程，不删声明。"""
        for exec_id, p, receiver in self._lingering_workers:
            try:
                self._finalize_process(p)
                if receiver is not None:
                    receiver.close(timeout=0.5)
            except Exception:
                pass
        self._lingering_workers.clear()

        for handle in handles:
            try:
                self._finalize_process(handle.process)
            except Exception as e:
                logger.error(f"Error finalizing in-flight job {handle.job_uid}: {e}")

    def close(self) -> None:
        """关闭通道并回收所有在途与残留子进程和管道资源。"""
        self.cleanup_in_flight(list(self._handles.values()))

__all__ = [
    "AbortOutcome",
    "ExecutionChannel",
    "ExecutionResult",
    "JobHandle",
    "WorkerLaunchSpec",
    "_WorkerLaunchCancelled",
    "_decode_ipc_result",
    "_worker_process_entry",
]
