"""执行通道：线性两阶段启动、单一会话状态机、阶梯看门狗、瞬时 Pipe 收割与 TOCTOU 中止。

ExecutionChannel 以每执行单一会话对象（``_ExecutionSession``）统一管理
``prepared -> active -> lingering -> closed`` 全生命周期状态，通过
``prepare_spawn -> release_spawn / abort_spawn`` 线性两阶段协议驱动底层
``WorkerLauncher``，对真实子进程与进程内替身完全透明无特判。
"""

from __future__ import annotations

import enum
import logging
import signal
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from ..exceptions import TaskLiteError
from ..models.job import Job
from ..models.result import validate_strict_json_object
from ..utils.ipc import (
    PIPE_MSG_DECLARE,
    PIPE_MSG_RESULT,
    WorkerPipeMessage,
    WorkerPipeReceiver,
    WorkerReplySender,
)
from ._worker_runtime import WorkerLaunchSpec
from .launcher import (
    PreparedWorker,
    SubprocessWorkerLauncher,
    WorkerLauncher,
    _WorkerLaunchCancelled,
)
from .types import JobHandle, is_importing_mp_main

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
class PreparedLaunch:
    """已完成冷启动准备、等待主进程放行或中止的执行令牌。"""

    spec: WorkerLaunchSpec
    session_key: str


class _SessionState(str, enum.Enum):
    """单次 worker 执行会话的显式状态机（单向推进至 closed）。"""

    PREPARED = "prepared"
    ACTIVE = "active"
    LINGERING = "lingering"
    CLOSED = "closed"


@dataclass
class _ExecutionSession:
    """单次 worker 执行的唯一会话状态对象（取代多份并行字典与残留列表）。"""

    session_key: str
    state: _SessionState = _SessionState.PREPARED
    cancel_event: threading.Event = field(default_factory=threading.Event)
    prepared_worker: PreparedWorker | None = None
    handle: JobHandle | None = None
    process: Any = None
    receiver: WorkerPipeReceiver | None = None
    reply_sender: WorkerReplySender | None = None
    health_stop: threading.Event | None = None
    result: dict[str, Any] | None = None
    error: Exception | None = None


class ExecutionChannel:
    """标准子进程执行通道深模块实现。"""

    _JOIN_REAP_TIMEOUT: float = 5.0

    def __init__(
        self,
        *,
        launcher: WorkerLauncher | None = None,
    ) -> None:
        self._launcher: WorkerLauncher = (
            launcher if launcher is not None else SubprocessWorkerLauncher()
        )
        self._sessions_lock = threading.RLock()
        self._sessions: dict[str, _ExecutionSession] = {}
        self._declaration_handler: Callable[[JobHandle, dict[str, Any]], dict[str, Any] | None] | None = None
        self._is_timeout_paused_cb: Callable[[JobHandle], bool] | None = None
        self._is_aborted_cb: Callable[[JobHandle], bool] | None = None
        self._get_timeout_at_cb: Callable[[JobHandle], float] | None = None
        self._wakeup_callback: Callable[[], None] | None = None

    @property
    def launcher(self) -> WorkerLauncher:
        """返回当前绑定的 worker 启动器（供内部与测试 seam 只读观察）。"""
        return self._launcher

    def set_launcher(self, launcher: WorkerLauncher) -> WorkerLauncher:
        """受控替换底层 worker 启动器并返回先前启动器（供测试 seam 使用）。"""
        if not isinstance(launcher, WorkerLauncher):
            raise TypeError(
                f"launcher must be a WorkerLauncher instance, got {type(launcher).__name__}"
            )
        previous = self._launcher
        self._launcher = launcher
        return previous

    def _find_session(self, handle: JobHandle) -> _ExecutionSession | None:
        exec_id = handle.execution_id
        with self._sessions_lock:
            if exec_id:
                session = self._sessions.get(exec_id)
                if session is not None:
                    return session
            for session in self._sessions.values():
                if session.handle is handle:
                    return session
        return None

    def _get_session_by_id(self, execution_id: str | None) -> _ExecutionSession | None:
        if not execution_id:
            return None
        with self._sessions_lock:
            return self._sessions.get(execution_id)

    @property
    def has_active_workers(self) -> bool:
        """通道是否存在准备中、在途或残留的 worker 会话。"""
        self._reap_lingering_workers()
        with self._sessions_lock:
            return any(
                session.state is not _SessionState.CLOSED
                for session in self._sessions.values()
            )

    def active_handles(self) -> list[JobHandle]:
        """返回当前处于 ``active`` 状态的全部在途执行句柄快照。"""
        with self._sessions_lock:
            return [
                session.handle
                for session in self._sessions.values()
                if session.state is _SessionState.ACTIVE and session.handle is not None
            ]

    def has_received_result(self, execution_id: str | None) -> bool:
        """查询指定执行会话是否已通过管道收到结果帧（未消费）。"""
        session = self._get_session_by_id(execution_id)
        return session is not None and session.result is not None

    def cancel_pending_launches(self) -> None:
        """立即唤醒并中止全部尚在准备/握手阶段的未放行启动。"""
        with self._sessions_lock:
            events = [
                session.cancel_event
                for session in self._sessions.values()
                if session.state is _SessionState.PREPARED
            ]
        for ev in events:
            ev.set()

    @staticmethod
    def _kill_process(p: Any) -> None:
        """优先调用进程树原子终止接口，兼容测试进程桩的 kill()。"""
        kill_tree = getattr(p, "kill_tree", None)
        if callable(kill_tree):
            kill_tree()
            return
        kill = getattr(p, "kill", None)
        if callable(kill):
            kill()

    @staticmethod
    def _is_process_closed(p: Any) -> bool:
        """经 ManagedWorkerHandle 公开属性查询句柄是否已关闭。"""
        return bool(getattr(p, "is_closed", False))

    @staticmethod
    def _terminate_and_join_process(p: Any) -> None:
        """终止并等待子进程退出（不调用 close，保留 exitcode 供解码读取）。"""
        if ExecutionChannel._is_process_closed(p):
            return
        try:
            is_alive_fn = getattr(p, "is_alive", None)
            if callable(is_alive_fn) and is_alive_fn():
                ExecutionChannel._kill_process(p)
        except Exception as e:
            logger.warning(f"Process kill failed during finalize: {e}")
        try:
            join_fn = getattr(p, "join", None)
            if callable(join_fn):
                join_fn(timeout=ExecutionChannel._JOIN_REAP_TIMEOUT)
        except Exception as e:
            logger.warning(f"Process join failed during finalize: {e}")

    @staticmethod
    def _finalize_process(p: Any) -> None:
        """清理单个子进程：kill_tree + join + close（各步失败降级但留痕，不阻断收割）。"""
        if ExecutionChannel._is_process_closed(p):
            return
        ExecutionChannel._terminate_and_join_process(p)
        try:
            process_close = getattr(p, "close", None)
            if callable(process_close):
                process_close()
        except Exception as e:
            logger.warning(f"Process close failed during finalize: {e}")

    def _stop_health_watch(self, session: _ExecutionSession) -> None:
        with self._sessions_lock:
            stop = session.health_stop
            session.health_stop = None
        if stop is not None:
            stop.set()

    def _close_reply_sender(self, session: _ExecutionSession) -> None:
        with self._sessions_lock:
            reply_sender = session.reply_sender
            session.reply_sender = None
        if reply_sender is not None:
            try:
                reply_sender.close()
            except Exception as exc:
                logger.debug(f"Reply sender close warning for {session.session_key}: {exc}")

    def _close_receiver(
        self,
        session: _ExecutionSession,
        *,
        timeout: float | None = None,
    ) -> None:
        with self._sessions_lock:
            receiver = session.receiver
            session.receiver = None
        if receiver is not None:
            try:
                receiver.close(
                    timeout=self._JOIN_REAP_TIMEOUT if timeout is None else timeout
                )
            except Exception as exc:
                logger.debug(f"Receiver close warning for {session.session_key}: {exc}")

    def _transition_to_lingering(self, session: _ExecutionSession) -> None:
        """将已产出结果但进程尚未退出的会话转入 lingering（保活 process 与 receiver）。"""
        self._stop_health_watch(session)
        self._close_reply_sender(session)
        with self._sessions_lock:
            session.handle = None
            session.result = None
            session.error = None
            session.state = _SessionState.LINGERING

    def _close_session(
        self,
        session: _ExecutionSession | None,
        *,
        finalize_process: bool = True,
        receiver_timeout: float | None = None,
    ) -> None:
        """幂等关闭单个会话：停止健康探测、关闭全部管道、按需回收进程并移出状态表。"""
        if session is None:
            return
        with self._sessions_lock:
            health_stop = session.health_stop
            session.health_stop = None
            process = session.process
            prepared_worker = session.prepared_worker
            reply_sender = session.reply_sender
            receiver = session.receiver
            session.prepared_worker = None
            session.handle = None
            session.process = None
            session.reply_sender = None
            session.receiver = None
            session.result = None
            session.error = None
            session.state = _SessionState.CLOSED
            self._sessions.pop(session.session_key, None)

        if health_stop is not None:
            health_stop.set()
        if prepared_worker is not None:
            try:
                prepared_worker.abort()
            except Exception as exc:
                logger.error(f"Prepared worker abort warning for {session.session_key}: {exc}")
        if finalize_process and process is not None:
            self._finalize_process(process)
        if reply_sender is not None:
            try:
                reply_sender.close()
            except Exception as exc:
                logger.debug(f"Reply sender close warning for {session.session_key}: {exc}")
        if receiver is not None:
            try:
                receiver.close(
                    timeout=self._JOIN_REAP_TIMEOUT if receiver_timeout is None else receiver_timeout
                )
            except Exception as exc:
                logger.debug(f"Receiver close warning for {session.session_key}: {exc}")

    # ── 线性两阶段启动协议（prepare_spawn -> release_spawn / abort_spawn）──

    def prepare_spawn(self, spec: WorkerLaunchSpec) -> PreparedLaunch:
        """阶段一：冷启动 worker 并完成握手与进程树绑定，停在放行门前。"""
        if is_importing_mp_main():
            raise RuntimeError(
                "ExecutionChannel.spawn() cannot be called while bootstrapping a worker process. "
                "Guard top-level execution with `if __name__ == '__main__':`."
            )

        cancel_event = threading.Event()
        session_key = spec.execution_id if spec.execution_id else f"_pending_{id(cancel_event)}"
        session = _ExecutionSession(
            session_key=session_key,
            state=_SessionState.PREPARED,
            cancel_event=cancel_event,
        )
        with self._sessions_lock:
            self._sessions[session_key] = session

        try:
            prepared_worker = self._launcher.prepare(
                spec,
                on_message=self._notify_wakeup,
                cancel_event=cancel_event,
            )
            with self._sessions_lock:
                if session.state is _SessionState.CLOSED or cancel_event.is_set():
                    session.state = _SessionState.CLOSED
                    self._sessions.pop(session_key, None)
                    should_abort = True
                else:
                    session.prepared_worker = prepared_worker
                    should_abort = False
            if should_abort:
                prepared_worker.abort()
                raise _WorkerLaunchCancelled(
                    f"stop requested before worker release for {spec.job.uid}"
                )
            return PreparedLaunch(spec=spec, session_key=session_key)
        except BaseException:
            with self._sessions_lock:
                session.prepared_worker = None
                session.state = _SessionState.CLOSED
                self._sessions.pop(session_key, None)
            raise

    def release_spawn(self, prepared: PreparedLaunch) -> JobHandle:
        """阶段二（正常路径）：放行 worker（发送 b"GO"），转入 active 并返回句柄。"""
        spec = prepared.spec
        with self._sessions_lock:
            session = self._sessions.get(prepared.session_key)
            if session is None or session.state is not _SessionState.PREPARED:
                raise RuntimeError(
                    f"Cannot release execution session {prepared.session_key!r}: not prepared"
                )
            prepared_worker = session.prepared_worker
            if prepared_worker is None:
                raise RuntimeError(
                    f"Cannot release execution session {prepared.session_key!r}: missing prepared worker"
                )
            cancelled = session.cancel_event.is_set()

        if cancelled:
            self.abort_spawn(prepared)
            raise _WorkerLaunchCancelled(
                f"stop requested before worker release for {spec.job.uid}"
            )

        try:
            launched = prepared_worker.release()
        except BaseException:
            with self._sessions_lock:
                session.prepared_worker = None
                session.state = _SessionState.CLOSED
                self._sessions.pop(prepared.session_key, None)
            raise

        timeout_at_monotonic = time.monotonic() + spec.timeout_seconds
        handle = JobHandle(
            job_uid=spec.job.uid,
            process=launched.handle,
            timeout_at_monotonic=timeout_at_monotonic,
            timeout_seconds=spec.timeout_seconds,
            started_at=launched.started_at,
            job=spec.job,
            execution_id=spec.execution_id,
            run_id=spec.run_id or None,
            attempt_no=spec.attempt_no,
        )
        with self._sessions_lock:
            session.prepared_worker = None
            session.handle = handle
            session.process = launched.handle
            session.receiver = launched.receiver
            session.reply_sender = launched.reply_sender
            session.state = _SessionState.ACTIVE
        self._watch_process_health(session)
        return handle

    def abort_spawn(self, prepared: PreparedLaunch) -> None:
        """阶段二（中止路径）：幂等终止并回收尚未放行的 worker 与会话。"""
        with self._sessions_lock:
            session = self._sessions.get(prepared.session_key)
            if session is None:
                return
            session.cancel_event.set()
            if session.state is not _SessionState.PREPARED:
                return
            prepared_worker = session.prepared_worker
            session.prepared_worker = None
            session.state = _SessionState.CLOSED
            self._sessions.pop(prepared.session_key, None)
        if prepared_worker is not None:
            prepared_worker.abort()

    def spawn(self, spec: WorkerLaunchSpec) -> JobHandle:
        """单步便捷启动接口：连续执行 prepare_spawn 与 release_spawn。"""
        prepared = self.prepare_spawn(spec)
        try:
            return self.release_spawn(prepared)
        except BaseException:
            self.abort_spawn(prepared)
            raise

    def _watch_process_health(self, session: _ExecutionSession) -> None:
        """独立探测 worker 存活，仅在状态变化时唤醒主循环。"""
        stop = threading.Event()
        process = session.process
        session.health_stop = stop

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
                if session.health_stop is stop:
                    session.health_stop = None

        threading.Thread(
            target=watch,
            name=f"tasklite-worker-health-{session.session_key}",
            daemon=True,
        ).start()

    def _accept_message(self, handle: JobHandle, msg: WorkerPipeMessage) -> None:
        """消费一条已校验信封的管道消息并更新会话状态。"""
        session = self._find_session(handle)
        if msg.kind == PIPE_MSG_DECLARE:
            if not isinstance(msg.payload, dict):
                raise ValueError(f"invalid declare payload for {handle.job_uid}")
            reply_sender = session.reply_sender if session is not None else None
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
            if session is not None and session.error is None and session.result is None:
                session.result = msg.payload

    def _process_pipe_item(
        self, handle: JobHandle, item: WorkerPipeMessage | Exception
    ) -> None:
        session = self._find_session(handle)
        exec_id = handle.execution_id or ""
        if isinstance(item, OSError):
            logger.debug(
                f"Worker pipe stream ended with OSError for {handle.job_uid} ({exec_id}): {item}"
            )
            return
        if isinstance(item, Exception):
            if session is not None and session.result is None and session.error is None:
                session.error = item
            return
        try:
            self._accept_message(handle, item)
        except Exception as exc:
            if session is not None and session.result is None and session.error is None:
                session.error = exc

    def _receive_available(self, handle: JobHandle) -> None:
        """非阻塞拉取 handle 对应接收端当前已入队的全部消息。"""
        session = self._find_session(handle)
        if session is None or session.receiver is None:
            return
        for item in session.receiver.take_messages():
            self._process_pipe_item(handle, item)

    def _drain_finished(self, handle: JobHandle) -> None:
        """在进程终止后排空接收端直至 EOF。"""
        session = self._find_session(handle)
        if session is None or session.receiver is None:
            return
        for item in session.receiver.drain_to_eof(timeout=self._JOIN_REAP_TIMEOUT):
            self._process_pipe_item(handle, item)

    def set_declaration_handler(
        self,
        handler: Callable[[JobHandle, dict[str, Any]], dict[str, Any] | None] | None,
    ) -> None:
        self._declaration_handler = handler

    def set_authorization_queries(
        self,
        *,
        is_timeout_paused: Callable[[JobHandle], bool] | None = None,
        is_aborted: Callable[[JobHandle], bool] | None = None,
        get_timeout_at: Callable[[JobHandle], float] | None = None,
    ) -> None:
        """绑定运行期授权状态只读查询回调（超时暂停判定、中止判定与有效截止时刻）。"""
        self._is_timeout_paused_cb = is_timeout_paused
        self._is_aborted_cb = is_aborted
        self._get_timeout_at_cb = get_timeout_at

    def _is_timeout_paused(self, handle: JobHandle) -> bool:
        cb = self._is_timeout_paused_cb
        return bool(cb(handle)) if cb is not None else False

    def _is_aborted(self, handle: JobHandle) -> bool:
        cb = self._is_aborted_cb
        return bool(cb(handle)) if cb is not None else False

    def _get_timeout_at(self, handle: JobHandle) -> float:
        cb = self._get_timeout_at_cb
        return cb(handle) if cb is not None else handle.timeout_at_monotonic

    def set_wakeup_callback(self, callback: Callable[[], None] | None) -> None:
        """绑定 IPC 消息到达时的主循环唤醒入口。"""
        self._wakeup_callback = callback

    def _notify_wakeup(self) -> None:
        callback = self._wakeup_callback
        if callback is not None:
            callback()

    def is_handle_alive(self, handle: JobHandle) -> bool:
        """受控判定在途句柄对应子进程是否仍在运行（屏蔽已关闭句柄的 ValueError）。"""
        p = handle.process
        if p is None or self._is_process_closed(p):
            return False
        is_alive_fn = getattr(p, "is_alive", None)
        if not callable(is_alive_fn):
            return False
        try:
            return bool(is_alive_fn())
        except (ValueError, OSError):
            return False

    def terminate_process(self, handle: JobHandle) -> None:
        """终止指定作业的子进程树。"""
        p = handle.process
        if p is not None:
            try:
                is_alive_fn = getattr(p, "is_alive", None)
                if callable(is_alive_fn) and is_alive_fn():
                    self._kill_process(p)
            except Exception:
                pass

    def get_reply_sender(self, execution_id: str | None) -> WorkerReplySender | None:
        """获取指定活跃会话的回复发送端。"""
        session = self._get_session_by_id(execution_id)
        if session is None or session.state is not _SessionState.ACTIVE:
            return None
        return session.reply_sender

    def send_declaration_reply(
        self,
        execution_id: str | None,
        reply: dict[str, Any],
    ) -> None:
        """向指定活跃执行会话发送动态声明回复帧（对端已关闭时静默收敛 OSError）。"""
        reply_sender = self.get_reply_sender(execution_id)
        if reply_sender is None:
            return
        try:
            reply_sender.send_reply(reply)
        except OSError:
            pass

    def _lingering_sessions(self) -> list[_ExecutionSession]:
        """返回当前处于 ``lingering`` 状态的全部会话快照。"""
        with self._sessions_lock:
            return [
                s for s in self._sessions.values()
                if s.state is _SessionState.LINGERING
            ]

    def _cleanup_lingering_sessions(
        self,
        *,
        receiver_timeout: float = 0.5,
        log_errors: bool = True,
    ) -> None:
        """回收全部处于 ``lingering`` 状态的会话，异常收敛不阻断后续清理。"""
        for session in self._lingering_sessions():
            try:
                self._close_session(
                    session,
                    finalize_process=True,
                    receiver_timeout=receiver_timeout,
                )
            except Exception as e:
                if log_errors:
                    logger.error(f"Error cleaning up lingering worker {session.session_key}: {e}")

    def _reap_lingering_workers(self) -> None:
        """非阻塞轮询已结算但残留的 lingering 会话，进程死亡后回收句柄与接收端。"""
        for session in self._lingering_sessions():
            p = session.process
            try:
                alive = p is not None and p.is_alive()
            except Exception:
                alive = False
            if not alive:
                self._close_session(session, finalize_process=True, receiver_timeout=0.2)
            elif session.receiver is not None and session.receiver.eof:
                self._close_receiver(session, timeout=0.1)

    def wait_for_lingering_workers(self) -> None:
        """正常 Run 收口前等待已结算 lingering 会话退出，超时后回收残留进程。"""
        lingering = self._lingering_sessions()
        if not lingering:
            return
        deadline = time.monotonic() + self._JOIN_REAP_TIMEOUT
        for session in lingering:
            process = session.process
            if process is not None:
                remaining = deadline - time.monotonic()
                try:
                    while process.is_alive() and remaining > 0:
                        process.join(timeout=min(0.05, remaining))
                        remaining = deadline - time.monotonic()
                        if process.is_alive() and remaining > 0:
                            time.sleep(min(0.01, remaining))
                except Exception as exc:
                    logger.warning(f"Settled worker wait failed for {session.session_key}: {exc}")
                try:
                    if process.is_alive():
                        logger.warning(f"Settled worker {session.session_key} did not exit before Run end")
                except Exception:
                    pass
            self._close_session(session, finalize_process=True, receiver_timeout=0.2)

    def reap_completed(
        self, handles: Sequence[JobHandle]
    ) -> list[tuple[JobHandle, ExecutionResult]]:
        """非阻塞扫描所有 in-flight handle，返回本次已完成的 (handle, result)。

        有效结果到达即结算：收到完整有效 TaskResult 即认为本次执行已返回并
        交付结算，不将 worker 进程退出作为额外成功条件，不以前置 join 阻塞
        主循环。结果返回后仍在运行的子进程转入 lingering 后台非阻塞回收，
        不阻塞其他 Job 派发或结算，亦不因之后的进程退出状态改写有效结果。
        """
        self._reap_lingering_workers()
        completed: list[tuple[JobHandle, ExecutionResult]] = []

        for handle in handles:
            now = time.monotonic()
            p = handle.process
            session = self._find_session(handle)
            self._receive_available(handle)

            if self._is_aborted(handle):
                if not self.is_handle_alive(handle):
                    if session is not None:
                        self._close_session(session, finalize_process=True, receiver_timeout=0.2)
                    else:
                        self._finalize_process(p)
                    completed.append(
                        (
                            handle,
                            ExecutionResult(
                                success=False,
                                message="Execution aborted by authorization",
                            ),
                        )
                    )
                continue

            # 1. 有效结果已到达：立即收割结果，不阻塞等待子进程退出
            if session is not None and session.result is not None:
                raw_res = session.result
                session.result = None
                decoded = _decode_ipc_result(raw_res, handle.job)

                if not self.is_handle_alive(handle):
                    self._close_session(session, finalize_process=True, receiver_timeout=0.2)
                else:
                    # 结果后不再回复声明；会话转入 lingering，保活进程句柄与接收端。
                    self._transition_to_lingering(session)

                completed.append((handle, decoded))
                continue

            # 2. 管道异常分支
            if session is not None and session.error is not None:
                completed.append((handle, self._collect_outcome(handle)))
                continue

            # 3. 进程已死亡但无有效结果（崩溃/被终止/无结果退出）
            if not p.is_alive():
                completed.append((handle, self._collect_outcome(handle)))
                continue

            # 4. 超时：从启动到结果到达计单次超时（等待授权期间暂停不计）
            if not self._is_timeout_paused(handle) and now > self._get_timeout_at(handle):
                p.join(timeout=0.5)
                if p.is_alive():
                    try:
                        self._kill_process(p)
                    except Exception:
                        pass
                completed.append(
                    (handle, self._collect_outcome(handle, is_timeout=True))
                )

        return completed

    def _collect_outcome(
        self, handle: JobHandle, res: dict | None = None, *, is_timeout: bool = False
    ) -> ExecutionResult:
        """收敛收割尾部：终止并收割子进程 → 排空管道至 EOF → 解码结果 → 关闭会话。"""
        p = handle.process
        session = self._find_session(handle)
        if self._is_aborted(handle):
            try:
                self._terminate_and_join_process(p)
            finally:
                if session is not None:
                    self._close_session(session, finalize_process=True)
                else:
                    self._finalize_process(p)
            return ExecutionResult(
                success=False,
                message="Execution aborted by authorization",
            )

        result: ExecutionResult | None = None
        try:
            self._terminate_and_join_process(p)
            self._drain_finished(handle)
            if session is not None and session.error is not None:
                err = session.error
                session.error = None
                session.result = None
                raise err
            payload = (
                res
                if res is not None
                else (session.result if session is not None else None)
            )
            if session is not None:
                session.result = None
            if payload is not None and not is_timeout:
                result = _decode_ipc_result(payload, handle.job)
            else:
                result = self._build_terminal_failure(p, handle, is_timeout=is_timeout)
        finally:
            if session is not None:
                self._close_session(session, finalize_process=True)
            else:
                self._finalize_process(p)
        return result

    @staticmethod
    def _build_terminal_failure(
        p: Any, handle: JobHandle, *, is_timeout: bool
    ) -> ExecutionResult:
        """构造进程终止（崩溃/超时/强杀）但无有效结果时的失败证据。"""
        raw_exitcode = getattr(p, "exitcode", None)
        exitcode: int | None = (
            raw_exitcode
            if isinstance(raw_exitcode, int) and not isinstance(raw_exitcode, bool)
            else None
        )
        was_force_killed = bool(getattr(p, "was_force_killed", False))
        annotations: dict[str, Any] = {}
        if is_timeout:
            message = f"Worker timed out after {handle.timeout_seconds}s"
            annotations["timed_out"] = True
        elif exitcode is not None and exitcode < 0:
            try:
                signal_name = signal.Signals(-exitcode).name
            except (ValueError, AttributeError):
                signal_name = f"SIGNO{-exitcode}"
            message = f"Worker exited on signal {signal_name} ({exitcode}) without a result"
            annotations["signal"] = signal_name
        elif was_force_killed:
            message = "Worker was forcefully terminated without a result"
            annotations["terminated"] = True
        elif exitcode is not None and exitcode > 0:
            message = f"Worker exited with code {exitcode} without a result"
        else:
            message = "Worker exited without a result frame"
        if exitcode is not None:
            annotations["exitcode"] = exitcode
        return ExecutionResult(
            success=False,
            message=message,
            annotations=annotations,
        )

    def cleanup_in_flight(self, handles: Sequence[JobHandle]) -> None:
        """主循环异常退出时取消未放行启动、终止并回收所有残留与在途会话。"""
        self.cancel_pending_launches()
        self._cleanup_lingering_sessions(receiver_timeout=0.5, log_errors=True)

        for handle in handles:
            try:
                handle_session = self._find_session(handle)
                if handle_session is not None:
                    self._close_session(handle_session, finalize_process=True)
                else:
                    self._finalize_process(handle.process)
            except Exception as e:
                logger.error(f"Error cleaning up in-flight job {handle.job_uid}: {e}")

    def close(self) -> None:
        """关闭通道并回收所有准备中、在途与残留会话的进程和管道资源。"""
        self.cancel_pending_launches()
        self.cleanup_in_flight(self.active_handles())
        with self._sessions_lock:
            remaining = list(self._sessions.values())
        for session in remaining:
            try:
                self._close_session(session, finalize_process=True, receiver_timeout=0.5)
            except Exception as e:
                logger.error(f"Error closing remaining session {session.session_key}: {e}")


__all__ = [
    "ExecutionChannel",
    "ExecutionResult",
    "JobHandle",
    "PreparedLaunch",
    "WorkerLaunchSpec",
]
