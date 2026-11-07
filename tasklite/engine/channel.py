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
import time
import traceback
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..exceptions import TaskLiteError, _ArtifactIOError
from ..models.artifact import ArtifactDeclaration
from ..models.context import JobContext
from ..models.job import Job
from ..models.result import TaskResult, validate_strict_json_object
from ..utils import lockfile
from ..utils.ipc import (
    ArtifactCleanupMode,
    ArtifactJournal,
    PIPE_MSG_INPUT,
    PIPE_MSG_OUTPUT,
    PIPE_MSG_RESULT,
    PIPE_MSG_SUSPEND,
    WorkerPipeMessage,
    WorkerPipeReceiver,
    WorkerPipeSender,
    create_worker_pipe,
)
from .errorclass import _DEFAULT_CLASSIFIER
from .types import JobHandle

logger = logging.getLogger("tasklite")

_KEY_TRACEBACK = "traceback"
_RESULT_DIR_ENV = "TASKLITE_IPC_DIR"


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


def _parse_resource_suspensions(
    raw_suspensions: Any,
) -> tuple[list[tuple[str, float]], str | None]:
    """解析 resource_suspensions 字段：元素 (str, 数字) 二元组，其余报错。"""
    if not isinstance(raw_suspensions, list):
        return [], f"invalid resource_suspensions type: {type(raw_suspensions).__name__}"
    valid: list[tuple[str, float]] = []
    for item in raw_suspensions:
        if (
            isinstance(item, (list, tuple))
            and len(item) == 2
            and isinstance(item[0], str)
            and isinstance(item[1], (int, float))
            and not isinstance(item[1], bool)
        ):
            valid.append((item[0], float(item[1])))
        else:
            return [], f"invalid resource_suspension entry: {item!r}"
    return valid, None


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
    list[tuple[str, float]],
]:
    """success 状态解码：metadata 严格 JSON 对象校验 + 三个子字段防御解析。

    返回 (success, message, result_metadata, spawned_jobs, cursor_updates, resource_suspensions)；
    任一子段损坏即 success=False、message 携带错误串且 result_metadata 为空字典（不混入框架键）。
    """
    if "metadata" not in res:
        logger.error(f"Corrupt result payload for {job.uid}: missing 'metadata' key")
        return False, "CORRUPT_RESULT_FILE: missing metadata", {}, [], {}, []
    raw_msg = res.get("message")
    if raw_msg is not None and type(raw_msg) is not str:
        logger.error(f"Corrupt result payload for {job.uid}: invalid 'message' type")
        return False, "CORRUPT_RESULT_FILE: invalid message type", {}, [], {}, []
    try:
        result_metadata = validate_strict_json_object(
            res["metadata"], field_name="metadata"
        )
    except Exception as e:
        logger.error(f"Result decode failed for {job.uid}: {e}")
        return (
            False,
            f"CORRUPT_RESULT_FILE: decode failed: {e}",
            {},
            [],
            {},
            [],
        )

    spawned_jobs, jobs_error = _parse_spawned_jobs(res.get("spawned_jobs", []))
    if jobs_error is not None:
        logger.error(f"Malformed spawned jobs in {job.uid}: {jobs_error}")
        return False, jobs_error, {}, [], {}, []

    cursor_updates, cursor_error = _parse_cursor_updates(res.get("cursor_updates", {}))
    if cursor_error is not None:
        logger.error(f"Invalid cursor_updates in {job.uid}: {cursor_error}")
        return False, cursor_error, {}, [], {}, []
    suspensions, suspension_error = _parse_resource_suspensions(
        res.get("resource_suspensions", [])
    )
    if suspension_error is not None:
        logger.error(f"Invalid resource_suspensions in {job.uid}: {suspension_error}")
        return False, suspension_error, {}, [], {}, []
    return True, raw_msg, result_metadata, spawned_jobs, cursor_updates, suspensions


def _decode_ipc_result(
    res: dict[str, Any],
    p: Any,
    job: Job,
    ipc_dir: str | None = None,
    *,
    output_roots: Sequence[str | Path] | None = None,
) -> "ExecutionResult":
    """解析子进程通过管道回传的结果字典（防御解析的单一入口）。

    损坏/注入形态一律收敛为任务级失败或瞬态重试记账，任何输入都不
    穿透异常；业务 result_metadata 绝不混入框架保留键；failure 与 retry
    不发布 spawned_jobs 与 cursor_updates。
    """
    success = False
    message: str | None = None
    result_metadata: dict[str, Any] = {}
    exception: dict[str, str] | None = None
    consume_budget = False
    retry_requested = False
    retry_error: str | None = None
    spawned_jobs: list[Job] = []
    cursor_updates: dict[str, str | None] = {}
    resource_suspensions: list[tuple[str, float]] = []
    transient_kind: str | None = None
    artifact_error: str | None = None

    status = res.get("status") if isinstance(res, dict) else None
    legacy_keys = (
        {"new_jobs", "error", "raw_result"} & res.keys()
        if isinstance(res, dict)
        else set()
    )
    if legacy_keys:
        message = f"CORRUPT_RESULT_FILE: unsupported fields {sorted(legacy_keys)}"
    elif status == "success":
        (
            success,
            message,
            result_metadata,
            spawned_jobs,
            cursor_updates,
            resource_suspensions,
        ) = _decode_success_branch(res, job)
    elif status == "failure":
        try:
            raw_msg = res.get("message")
            if raw_msg is not None and type(raw_msg) is not str:
                raise TypeError("message must be str or None")
            result_metadata = validate_strict_json_object(
                res.get("metadata", {}), field_name="metadata"
            )
            exception = _validate_exception_dict(res.get("exception"))
            suspensions, suspension_error = _parse_resource_suspensions(
                res.get("resource_suspensions", [])
            )
            if suspension_error is not None:
                raise ValueError(suspension_error)
            message = raw_msg
            resource_suspensions = suspensions
        except Exception as e:
            message = f"CORRUPT_RESULT_FILE: invalid failure payload: {e}"
            result_metadata = {}
            exception = None
            resource_suspensions = []
    elif status == "retry":
        try:
            raw_msg = res.get("message")
            if raw_msg is None and isinstance(res.get("error_message"), str):
                raw_msg = res["error_message"]
            if raw_msg is not None and type(raw_msg) is not str:
                raise TypeError("message must be str or None")
            result_metadata = validate_strict_json_object(
                res.get("metadata", {}), field_name="metadata"
            )
            raw_consume = res.get("consume_budget", True)
            if type(raw_consume) is not bool:
                raise TypeError("consume_budget must be bool")
            suspensions, suspension_error = _parse_resource_suspensions(
                res.get("resource_suspensions", [])
            )
            if suspension_error is not None:
                raise ValueError(suspension_error)
            kind = res.get("transient_kind")
            transient_kind = kind if isinstance(kind, str) else None
            retry_requested = True
            message = raw_msg
            retry_error = raw_msg
            consume_budget = False if transient_kind is not None else raw_consume
            resource_suspensions = suspensions
        except Exception as e:
            retry_requested = False
            message = f"CORRUPT_RESULT_FILE: invalid retry payload: {e}"
            result_metadata = {}
            consume_budget = False
            resource_suspensions = []
    elif status == "interrupted":
        retry_requested = True
        raw_msg = (
            res.get("message")
            if isinstance(res.get("message"), str)
            else res.get("error_message")
        )
        message = str(raw_msg) if raw_msg is not None else None
        retry_error = message
        transient_kind = "interrupted"
        consume_budget = False
        suspensions, suspension_error = _parse_resource_suspensions(
            res.get("resource_suspensions", [])
        )
        if suspension_error is None:
            resource_suspensions = suspensions
    elif status == "artifact_error":
        artifact_error = str(
            res.get("error_message")
            or res.get("message")
            or "Artifact declaration I/O failed"
        )
    elif status is not None:
        message = f"CORRUPT_RESULT_FILE: unknown status {status!r}"
        logger.error(f"Worker returned unknown status for {job.uid}: {status!r}")
    else:
        att = _DEFAULT_CLASSIFIER.classify_process_death(
            getattr(p, "exitcode", None), timed_out=False
        )
        raw_att_msg = att.result_metadata.get("error_message")
        message = str(raw_att_msg) if raw_att_msg is not None else None
        result_metadata = dict(att.result_metadata)
        retry_error = att.retry_error
        retry_requested = att.retry_requested
        consume_budget = bool(retry_requested)

    return ExecutionResult(
        success=success,
        result_metadata=result_metadata,
        message=message,
        exception=exception,
        consume_budget=consume_budget,
        retry_requested=retry_requested,
        retry_error=retry_error,
        spawned_jobs=spawned_jobs,
        cursor_updates=cursor_updates,
        resource_suspensions=resource_suspensions,
        transient_kind=transient_kind,
        artifact_error=artifact_error,
    )


def _worker_process_entry(spec: "WorkerLaunchSpec") -> None:
    """spawn 子进程执行体入口：持锁执行 handler 并经瞬时管道发送结果。

    不变式：锁生命周期严格等于本执行体生命周期（try 前获取、finally
    释放）——锁先于 handler 获取保证同 uid 执行体全局互斥，finally 释放
    保证执行体任何出口（含未捕获异常）都交还锁并关闭管道写端。
    """
    handler_func = spec.handler
    job = spec.job
    ctx = spec.job_context
    ipc_dir = spec.ipc_dir
    job_uid = job.uid
    execution_id = spec.execution_id
    if not execution_id:
        raise RuntimeError(
            f"worker started without execution_id for {job_uid}: "
            f"fencing requires WorkerLaunchSpec.execution_id"
        )
    pipe_sender = spec.pipe_sender
    if pipe_sender is not None:
        ctx._bind_pipe_sender(pipe_sender)

    def _write_result(payload: dict[str, Any]) -> None:
        if pipe_sender is None:
            return
        try:
            pipe_sender.send_result(payload)
        except OSError as exc:
            logger.error(
                f"worker pipe result send failed for {job_uid} ({execution_id}): {exc}"
            )

    _lock_fd = None
    try:
        try:
            _lock_fd = lockfile.try_acquire_lock(ipc_dir, job_uid, timeout=2.0)
        except OSError as e:
            _write_result({
                "status": "retry",
                "transient_kind": "lock_conflict",
                "error_message": f"LOCK_ENV_FAULT: cannot open lock file for {job_uid} "
                         f"(errno={getattr(e, 'errno', None)}): {e}",
            })
            return
        if _lock_fd is None:
            _write_result({
                "status": "retry",
                "transient_kind": "lock_conflict",
                "error_message": f"LOCK_CONFLICT: another execution body holds {job_uid} lock",
            })
            return
        try:
            raw_result = handler_func(job, ctx)
            if isinstance(raw_result, TaskResult):
                if raw_result.status == "success":
                    _write_result({
                        "status": "success",
                        "message": raw_result.message,
                        "metadata": raw_result.metadata,
                        "spawned_jobs": [j.to_dict() for j in ctx.spawned_jobs],
                        "resource_suspensions": list(ctx.resource_suspensions),
                        "cursor_updates": dict(ctx.cursor_updates),
                    })
                elif raw_result.status == "failure":
                    _write_result({
                        "status": "failure",
                        "message": raw_result.message,
                        "metadata": raw_result.metadata,
                        "exception": None,
                        "resource_suspensions": list(ctx.resource_suspensions),
                    })
                else:
                    _write_result({
                        "status": "retry",
                        "message": raw_result.message,
                        "metadata": raw_result.metadata,
                        "consume_budget": raw_result.consume_budget,
                        "resource_suspensions": list(ctx.resource_suspensions),
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
                    "resource_suspensions": list(ctx.resource_suspensions),
                })
        except _ArtifactIOError as e:
            _write_result({"status": "artifact_error", "error_message": str(e)})
        except Exception as e:
            _write_result({
                "status": "failure",
                "message": f"{type(e).__name__}: {e}",
                "metadata": {},
                "exception": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                },
                "resource_suspensions": list(ctx.resource_suspensions),
            })
        except KeyboardInterrupt as e:
            _write_result({
                "status": "interrupted",
                "error_message": f"WORKER_INTERRUPTED: {type(e).__name__}",
                "resource_suspensions": list(ctx.resource_suspensions),
            })
        except SystemExit as e:
            _write_result({
                "status": "failure",
                "message": f"SystemExit: {e}",
                "metadata": {},
                "exception": {
                    "type": type(e).__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                },
                "resource_suspensions": list(ctx.resource_suspensions),
            })
    finally:
        if _lock_fd is not None:
            lockfile.release_lock(_lock_fd)
        if pipe_sender is not None:
            pipe_sender.close()


@dataclass
class ExecutionResult:
    """单次子进程执行的收割产物（收割/中止路径共形）。"""

    success: bool = False
    result_metadata: dict[str, Any] = field(default_factory=dict)
    message: str | None = None
    exception: dict[str, str] | None = None
    consume_budget: bool = True
    retry_requested: bool = False
    retry_error: str | None = None
    spawned_jobs: list[Job] = field(default_factory=list)
    cursor_updates: dict[str, str | None] = field(default_factory=dict)
    resource_suspensions: list[tuple[str, float]] = field(default_factory=list)
    transient_kind: str | None = None
    will_retry: bool = False
    artifact_error: str | None = None
    annotations: dict[str, Any] = field(default_factory=dict)

    @property
    def metadata(self) -> dict[str, Any]:
        """业务结果元数据（与 result_metadata 同源，不含框架保留键）。"""
        return self.result_metadata

    @property
    def error(self) -> str:
        """结果说明/错误消息（优先取 message，回退 retry_error 或内部 error_message）。"""
        if self.message is not None:
            return self.message
        if self.retry_error is not None:
            return self.retry_error
        raw = self.result_metadata.get("error_message")
        return str(raw) if raw is not None else ""

    @property
    def traceback(self) -> str | None:
        """结构化异常堆栈（无异常时为 None）。"""
        if isinstance(self.exception, dict):
            return self.exception.get("traceback")
        raw_tb = self.result_metadata.get(_KEY_TRACEBACK)
        return str(raw_tb) if raw_tb is not None else None


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
    ipc_dir: str
    timeout_seconds: float
    pipe_sender: WorkerPipeSender | None = None


@dataclass(frozen=True)
class AbortOutcome:
    """强制终止/异常停机时的收尾结果。

    salvaged_signals：杀进程后补排空捞回的 (uid, resource, seconds)——
    cancelled 任务的挂起信号经本列表带回上层应用（suspend 的 max 语义保证重复应用幂等）。
    """

    completed: list[tuple[JobHandle, ExecutionResult]]
    cancelled: list[JobHandle]
    salvaged_signals: list[tuple[str, str, float]] = field(default_factory=list)


class ExecutionChannel:
    """标准子进程执行通道深模块实现。"""

    _JOIN_REAP_TIMEOUT: float = 5.0

    def __init__(
        self,
        ipc_dir: str | Path | None = None,
        *,
        process_context: Any | None = None,
        output_roots: str | Path | Sequence[str | Path] | None = None,
        protected_roots: Sequence[str | Path] = (),
    ) -> None:
        self._process_context = process_context or mp.get_context("spawn")
        self.ipc_dir = str(ipc_dir) if ipc_dir is not None else None
        self.output_roots = output_roots
        self.protected_roots = tuple(protected_roots)
        self._receivers: dict[str, WorkerPipeReceiver] = {}
        self._in_process_senders: dict[str, WorkerPipeSender] = {}
        self._handles: dict[str, JobHandle] = {}
        self._results: dict[str, dict[str, Any]] = {}
        self._errors: dict[str, Exception] = {}
        self._pending_signals: list[tuple[str, str, float]] = []
        if self.ipc_dir:
            try:
                Path(self.ipc_dir).mkdir(parents=True, exist_ok=True)
            except OSError:
                pass

    @property
    def journal(self) -> ArtifactJournal:
        ipc = getattr(self, "ipc_dir", None)
        cached = getattr(self, "_journal", None)
        if cached is None or cached.ipc_dir != ipc:
            cached = ArtifactJournal(
                ipc,
                output_roots=getattr(self, "output_roots", None),
                protected_roots=getattr(self, "protected_roots", ()),
            )
            self._journal = cached
        return cached

    @journal.setter
    def journal(self, value: ArtifactJournal) -> None:
        self._journal = value

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
        effective_ipc_dir = spec.ipc_dir or self.ipc_dir or os.environ.get(_RESULT_DIR_ENV)
        if not effective_ipc_dir:
            raise ValueError("ipc_dir is required for ExecutionChannel.spawn()")
        Path(effective_ipc_dir).mkdir(parents=True, exist_ok=True)
        if self.ipc_dir is None:
            self.ipc_dir = str(effective_ipc_dir)
        spec.job_context.ipc_dir = effective_ipc_dir
        self.journal.clear_declarations(spec.job.uid)
        receiver, sender = create_worker_pipe(
            self._process_context,
            job_uid=spec.job.uid,
            execution_id=spec.execution_id,
        )
        spec.job_context._bind_pipe_sender(sender)
        launch_spec = dataclasses.replace(spec, pipe_sender=sender)
        p = None
        try:
            p = self._process_context.Process(target=_worker_process_entry, args=(launch_spec,))
            p.start()
        except Exception:
            sender.close()
            receiver.close()
            if p is not None:
                self._finalize_process(p)
            raise
        if isinstance(p, mp.process.BaseProcess):
            sender.close()
        else:
            self._in_process_senders[spec.execution_id] = sender
        timeout_at_monotonic = time.monotonic() + spec.timeout_seconds
        handle = JobHandle(
            job_uid=spec.job.uid,
            process=p,
            timeout_at_monotonic=timeout_at_monotonic,
            timeout_seconds=spec.timeout_seconds,
            job=spec.job,
            ipc_dir=effective_ipc_dir,
            execution_id=spec.execution_id,
        )
        self._receivers[spec.execution_id] = receiver
        self._handles[spec.execution_id] = handle
        return handle

    def _accept_message(self, handle: JobHandle, msg: WorkerPipeMessage) -> None:
        """消费一条已校验信封的管道消息并更新通道内存状态。"""
        if msg.kind == PIPE_MSG_OUTPUT:
            if not isinstance(msg.payload, dict):
                raise ValueError(f"invalid artifact declaration for {handle.job_uid}")
            try:
                declaration = ArtifactDeclaration.from_dict(msg.payload)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid artifact declaration for {handle.job_uid}") from exc
            self.journal.record_artifact(handle.job_uid, declaration, prepare=False)
        elif msg.kind == PIPE_MSG_INPUT:
            if not isinstance(msg.payload, dict):
                raise ValueError(f"invalid input declaration for {handle.job_uid}")
            self.journal.record_input_entry(handle.job_uid, msg.payload)
        elif msg.kind == PIPE_MSG_SUSPEND:
            if (
                not isinstance(msg.payload, (list, tuple))
                or len(msg.payload) != 2
                or not isinstance(msg.payload[0], str)
                or not msg.payload[0]
                or not isinstance(msg.payload[1], (int, float))
                or isinstance(msg.payload[1], bool)
            ):
                raise ValueError(f"invalid suspend signal for {handle.job_uid}: {msg.payload!r}")
            self._pending_signals.append(
                (handle.job_uid, msg.payload[0], float(msg.payload[1]))
            )
        elif msg.kind == PIPE_MSG_RESULT:
            if not isinstance(msg.payload, dict):
                raise ValueError(f"invalid result payload for {handle.job_uid}")
            exec_id = handle.execution_id or ""
            if exec_id not in self._errors:
                self._results[exec_id] = msg.payload

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
            self._errors.setdefault(exec_id, item)
            self._results.pop(exec_id, None)
            return
        try:
            self._accept_message(handle, item)
        except Exception as exc:
            self._errors.setdefault(exec_id, exc)
            self._results.pop(exec_id, None)

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

    def _close_handle_pipe(self, execution_id: str | None) -> None:
        if not execution_id:
            return
        sender = self._in_process_senders.pop(execution_id, None)
        if sender is not None:
            sender.close()
        receiver = self._receivers.pop(execution_id, None)
        if receiver is not None:
            receiver.close(timeout=self._JOIN_REAP_TIMEOUT)

    def _salvage_signals(
        self, job_uid: str, decoded: ExecutionResult | None
    ) -> list[tuple[str, float]]:
        """提取该 job_uid 已收取但尚未被上层排空的挂起信号。"""
        pending: list[tuple[str, float]] = []
        remaining: list[tuple[str, str, float]] = []
        for u, r_name, secs in self._pending_signals:
            if u == job_uid:
                pending.append((r_name, secs))
            else:
                remaining.append((u, r_name, secs))
        self._pending_signals = remaining
        if decoded is not None and pending:
            decoded.resource_suspensions = (
                list(decoded.resource_suspensions) + list(pending)
            )
        return pending

    def _consume_result_if_present(self, handle: JobHandle) -> ExecutionResult | None:
        """读取已收到的结果负载 → 守卫过滤 → 解码 → 信号并入。

        守卫契约：结果须为带 status 键的 dict 且非 interrupted 报告——
        worker 报告被中断的结果按「未完成」处理（重入队），不作为终态。
        """
        exec_id = getattr(handle, "execution_id", None)
        if not exec_id or exec_id in self._errors:
            return None
        raw_res = self._results.get(exec_id)
        if not (
            raw_res is not None
            and isinstance(raw_res, dict)
            and "status" in raw_res
            and raw_res.get("status") != "interrupted"
        ):
            return None
        self._results.pop(exec_id, None)
        decoded = _decode_ipc_result(
            raw_res, None, handle.job, self.ipc_dir,
            output_roots=getattr(self, "output_roots", None),
        )
        self._salvage_signals(handle.job_uid, decoded)
        return decoded

    def reap_completed(
        self, handles: Sequence[JobHandle]
    ) -> list[tuple[JobHandle, ExecutionResult]]:
        """非阻塞扫描所有 in-flight handle，返回本次已完成的 (handle, result)。"""
        completed: list[tuple[JobHandle, ExecutionResult]] = []

        for handle in handles:
            now = time.monotonic()
            p = handle.process
            exec_id = handle.execution_id or ""
            self._receive_available(handle)

            if exec_id in self._errors or exec_id in self._results:
                completed.append((handle, self._collect_outcome(handle)))
                continue

            if not p.is_alive():
                completed.append((handle, self._collect_outcome(handle)))
                continue

            if now > handle.timeout_at_monotonic:
                p.join(timeout=0.5)
                if p.is_alive():
                    try:
                        p.kill()
                    except Exception:
                        pass
                    is_timeout = True
                else:
                    is_timeout = False
                completed.append(
                    (handle, self._collect_outcome(handle, is_timeout=is_timeout))
                )

        return completed

    def _collect_outcome(
        self, handle: JobHandle, res: dict | None = None, *, is_timeout: bool = False
    ) -> ExecutionResult:
        """收敛收割尾部：终止并收割子进程 → 排空管道至 EOF → 解码结果 → 关闭管道。"""
        p = handle.process
        exec_id = handle.execution_id or ""
        result: ExecutionResult | None = None
        try:
            if not is_timeout and (exec_id in self._results or res is not None):
                try:
                    p.join(timeout=0.5)
                except Exception:
                    pass
            self._terminate_and_join_process(p)
            self._drain_finished(handle)
            if exec_id in self._errors:
                err = self._errors.pop(exec_id)
                self._results.pop(exec_id, None)
                raise err
            payload = res if res is not None else self._results.pop(exec_id, None)
            if payload is not None:
                result = _decode_ipc_result(
                    payload, p, handle.job, handle.ipc_dir,
                    output_roots=getattr(self, "output_roots", None),
                )
            else:
                result = self._build_terminal_failure(p, handle, is_timeout=is_timeout)
        finally:
            self._finalize_process(p)
            pending_signals = self._salvage_signals(handle.job_uid, result)
            if pending_signals:
                logger.info(
                    f"Salvaged {len(pending_signals)} suspend signal(s) "
                    f"from {handle.job_uid} before IPC cleanup"
                )
            self._close_handle_pipe(exec_id)
            self._handles.pop(exec_id, None)
            self._results.pop(exec_id, None)
        return result

    @staticmethod
    def _build_terminal_failure(
        p: Any, handle: JobHandle, *, is_timeout: bool
    ) -> ExecutionResult:
        """构造进程终止（崩溃/超时）但无结果消息时的 ExecutionResult。"""
        att = _DEFAULT_CLASSIFIER.classify_process_death(
            p.exitcode,
            timed_out=is_timeout,
            timeout_seconds=handle.timeout_seconds,
            timeout_is_transient=getattr(handle.job, "timeout_is_transient", False),
        )
        if att.retry_requested and "signal" in att.result_metadata:
            logger.warning(
                f"Worker for {handle.job_uid} died by {att.result_metadata['signal']}; "
                f"will retry (requeue pacing is owned by RequeuePolicy)"
            )
        return ExecutionResult(
            success=False,
            result_metadata=att.result_metadata,
            retry_requested=att.retry_requested,
            retry_error=att.retry_error,
        )

    def probe_orphan_lock(self, job_uid: str) -> bool:
        """非阻塞探测执行锁。True 表示无孤儿持锁可安全执行；False 表示孤儿活跃需 defer。"""
        if self.ipc_dir is None:
            return True
        return lockfile.probe_lock(self.ipc_dir, job_uid)

    def drain_active_signals(self, uids: Iterable[str]) -> list[tuple[str, str, float]]:
        """排空指定在途任务已发送的 suspend 信号。"""
        uid_set = set(uids)
        for handle in list(self._handles.values()):
            if handle.job_uid in uid_set:
                self._receive_available(handle)
        signals: list[tuple[str, str, float]] = []
        remaining: list[tuple[str, str, float]] = []
        for job_uid, r_name, secs in self._pending_signals:
            if job_uid in uid_set:
                signals.append((job_uid, r_name, secs))
            else:
                remaining.append((job_uid, r_name, secs))
        self._pending_signals = remaining
        return signals

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
            self._salvage_signals(handle.job_uid, decoded)
            exec_id = handle.execution_id or ""
            self._close_handle_pipe(exec_id)
            self._handles.pop(exec_id, None)
            self._results.pop(exec_id, None)
            self._errors.pop(exec_id, None)

        salvaged: list[tuple[str, str, float]] = []
        truly_cancelled: list[JobHandle] = []
        for handle in pending_handles:
            self._drain_finished(handle)
            decoded = self._consume_result_if_present(handle)
            if decoded is not None:
                done_pairs.append((handle, decoded))
            else:
                salvaged.extend(
                    (handle.job_uid, r_name, secs)
                    for r_name, secs in self._salvage_signals(handle.job_uid, None)
                )
                truly_cancelled.append(handle)
            exec_id = handle.execution_id or ""
            self._close_handle_pipe(exec_id)
            self._handles.pop(exec_id, None)
            self._results.pop(exec_id, None)
            self._errors.pop(exec_id, None)

        return AbortOutcome(
            completed=done_pairs,
            cancelled=truly_cancelled,
            salvaged_signals=salvaged,
        )

    def cleanup_in_flight(self, handles: Sequence[JobHandle]) -> None:
        """主循环异常退出时 kill + join 所有残留 in-flight 子进程并关闭管道与声明。"""
        for handle in handles:
            try:
                self._finalize_process(handle.process)
                exec_id = handle.execution_id or ""
                self._close_handle_pipe(exec_id)
                self._handles.pop(exec_id, None)
                self._results.pop(exec_id, None)
                self._errors.pop(exec_id, None)
                self._salvage_signals(handle.job_uid, None)
                self.journal.clear_declarations(handle.job_uid)
            except Exception as e:
                logger.error(f"Error cleaning up in-flight job {handle.job_uid}: {e}")

    def finalize_processes(self, handles: Sequence[JobHandle]) -> None:
        """只 kill + join 残留子进程，不删声明。"""
        for handle in handles:
            try:
                self._finalize_process(handle.process)
            except Exception as e:
                logger.error(f"Error finalizing in-flight job {handle.job_uid}: {e}")

    def cleanup_artifacts(self, job_uid: str, *, mode: ArtifactCleanupMode) -> None:
        """统一收敛产物与内存声明的生命周期清理。"""
        self.journal.cleanup(job_uid, mode=mode)

    def read_declared_artifacts(self, job_uid: str) -> tuple[ArtifactDeclaration, ...]:
        """读取框架授权值，日志错误不得降级为空清单后发布成功。"""
        return self.journal.read_artifacts(job_uid)

    def read_declared_inputs(self, job_uid: str) -> list[str]:
        """读取任务声明的输入路径清单。"""
        return self.journal.read_inputs(job_uid)


__all__ = [
    "AbortOutcome",
    "ArtifactCleanupMode",
    "ExecutionChannel",
    "ExecutionResult",
    "JobHandle",
    "WorkerLaunchSpec",
    "_decode_ipc_result",
    "_worker_process_entry",
]
