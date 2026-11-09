"""瞬时 Worker 管道通信：信封协议、有界反压、声明请求与回复身份校验。

统一内敛瞬时管道 IPC：

- 单向管道传输：基于 ``multiprocessing.Pipe(duplex=False)`` 的原生帧收发
  （``send_bytes`` / ``recv_bytes``），每 worker 独立读取线程与有界队列，
  承载 ``declare`` / ``result`` 信封消息；
- 动态声明：worker 侧请求回复声明，主进程侧完成解析、授权与准备；
- 回复身份：响应严格绑定 job_uid、execution_id 与 request_id。

分层红线：本模块是模型层（JobContext）的 IPC 声明与信号传输下沉点；
本层严禁出现任何重试节奏语义（节奏归 RequeuePolicy）。
"""

from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from ..exceptions import _ArtifactIOError
from .jsonutil import dumps, loads

logger = logging.getLogger("tasklite")


class _PipeConnection(Protocol):
    def send_bytes(self, buf: bytes, /) -> None: ...
    def recv_bytes(self) -> bytes: ...
    def poll(self, timeout: float, /) -> bool: ...
    def close(self) -> None: ...


# 管道消息种类常量
PIPE_MSG_INPUT = "input"
PIPE_MSG_RESULT = "result"
PIPE_MSG_DECLARE = "declare"
VALID_PIPE_MESSAGE_KINDS = frozenset(
    {
        PIPE_MSG_RESULT,
        PIPE_MSG_DECLARE,
    }
)
_PIPE_EOF = object()


@dataclass(frozen=True)
class WorkerPipeMessage:
    """已校验的 worker 管道信封消息。"""

    job_uid: str
    execution_id: str
    kind: str
    payload: Any
    run_id: str | None = None
    attempt_no: int | None = None


def decode_worker_pipe_message(
    raw: bytes,
    *,
    expected_job_uid: str,
    expected_execution_id: str,
    expected_run_id: str | None = None,
    expected_attempt_no: int | None = None,
) -> WorkerPipeMessage:
    """解析并校验单帧管道消息信封与归属身份。"""
    try:
        envelope = loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid worker pipe JSON frame: {exc}") from exc
    if not isinstance(envelope, dict):
        raise ValueError("worker pipe frame must decode to a JSON object")
    job_uid = envelope.get("job_uid")
    execution_id = envelope.get("execution_id")
    kind = envelope.get("kind")
    if job_uid != expected_job_uid:
        raise ValueError(f"worker pipe job_uid mismatch: {job_uid!r} != {expected_job_uid!r}")
    if execution_id != expected_execution_id:
        raise ValueError(
            f"worker pipe execution_id mismatch: {execution_id!r} != {expected_execution_id!r}"
        )
    run_id = envelope.get("run_id")
    if expected_run_id is not None and run_id != expected_run_id:
        raise ValueError(
            f"worker pipe run_id mismatch: {run_id!r} != {expected_run_id!r}"
        )
    attempt_no = envelope.get("attempt_no")
    if expected_attempt_no is not None and attempt_no != expected_attempt_no:
        raise ValueError(
            f"worker pipe attempt_no mismatch: {attempt_no!r} != {expected_attempt_no!r}"
        )
    if kind not in VALID_PIPE_MESSAGE_KINDS:
        raise ValueError(f"unknown worker pipe message kind: {kind!r}")
    return WorkerPipeMessage(
        job_uid=expected_job_uid,
        execution_id=expected_execution_id,
        kind=kind,
        payload=envelope.get("payload"),
        run_id=run_id if isinstance(run_id, str) else None,
        attempt_no=(
            attempt_no
            if isinstance(attempt_no, int) and not isinstance(attempt_no, bool)
            else None
        ),
    )


class WorkerPipeSender:
    """Worker 侧单向管道发送端。

    不变式：单连接写操作以进程内线程锁串行化；支持随 WorkerLaunchSpec
    跨 spawn/fork 序列化传递并在子进程内重建线程锁。
    """

    def __init__(
        self,
        connection: _PipeConnection,
        *,
        job_uid: str,
        execution_id: str,
        run_id: str | None = None,
        attempt_no: int | None = None,
    ) -> None:
        self._conn = connection
        self.job_uid = job_uid
        self.execution_id = execution_id
        self.run_id = run_id
        self.attempt_no = attempt_no
        self._lock = threading.Lock()
        self._closed = False

    def __getstate__(self) -> dict[str, Any]:
        return {
            "conn": self._conn,
            "job_uid": self.job_uid,
            "execution_id": self.execution_id,
            "run_id": self.run_id,
            "attempt_no": self.attempt_no,
            "closed": self._closed,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._conn = state["conn"]
        self.job_uid = state["job_uid"]
        self.execution_id = state["execution_id"]
        self.run_id = state.get("run_id")
        self.attempt_no = state.get("attempt_no")
        self._closed = state["closed"]
        self._lock = threading.Lock()

    def send_message(self, kind: str, payload: Any) -> None:
        """编码并发送单条信封消息。"""
        if kind not in VALID_PIPE_MESSAGE_KINDS:
            raise ValueError(f"unknown worker pipe message kind: {kind!r}")
        envelope = {
            "job_uid": self.job_uid,
            "execution_id": self.execution_id,
            "run_id": self.run_id,
            "attempt_no": self.attempt_no,
            "kind": kind,
            "payload": payload,
        }
        frame = dumps(envelope).encode("utf-8")
        with self._lock:
            if self._closed:
                raise OSError("worker pipe sender is closed")
            self._conn.send_bytes(frame)

    def send_result(self, result_dict: dict[str, Any]) -> None:
        """发送执行结果负载。"""
        self.send_message(PIPE_MSG_RESULT, result_dict)

    def send_declare(
        self,
        request_id: str,
        name: str,
        spec_dict: dict[str, Any],
    ) -> None:
        """发送动态文件声明请求并由主进程准备。"""
        self.send_message(
            PIPE_MSG_DECLARE,
            {
                "request_id": request_id,
                "name": name,
                "spec": spec_dict,
            },
        )

    def close(self) -> None:
        """幂等关闭写端连接。"""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._conn.close()
            except OSError:
                pass


class WorkerPipeReceiver:
    """父进程侧单向管道接收端（每 worker 专用读取线程 + 有界队列）。

    不变式：每个连接仅由唯一后台读取线程调用 ``recv_bytes()``，主调度线程
    仅经有界队列非阻塞或带超时取消息；队列满时自然反压至 OS 管道缓冲区，
    不额外开辟无限内存缓冲。
    """

    def __init__(
        self,
        connection: _PipeConnection,
        *,
        job_uid: str,
        execution_id: str,
        run_id: str | None = None,
        attempt_no: int | None = None,
        max_queued_messages: int = 64,
        on_message: Callable[[], None] | None = None,
    ) -> None:
        if max_queued_messages < 1:
            raise ValueError("max_queued_messages must be >= 1")
        self._conn = connection
        self.job_uid = job_uid
        self.execution_id = execution_id
        self.run_id = run_id
        self.attempt_no = attempt_no
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=max_queued_messages)
        self._stop = threading.Event()
        self._eof = threading.Event()
        self._delivering = False
        self._closed = False
        self._on_message = on_message
        self._thread = threading.Thread(
            target=self._reader_loop,
            name=f"tasklite-pipe-recv-{execution_id}",
            daemon=True,
        )
        self._thread.start()

    def _has_pending_pipe_data(self) -> bool:
        if self._closed or self._eof.is_set():
            return False
        try:
            return bool(self._conn.poll(0))
        except OSError:
            return False

    def _reader_loop(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    try:
                        ready = self._conn.poll(0.05)
                    except BrokenPipeError as exc:
                        if getattr(exc, "winerror", None) == 109:
                            break
                        raise
                    if not ready:
                        continue
                    self._delivering = True
                    try:
                        raw = self._conn.recv_bytes()
                        if not self._put_item(raw):
                            break
                    finally:
                        self._delivering = False
                except EOFError:
                    break
                except OSError as exc:
                    if not self._stop.is_set():
                        self._put_item(exc)
                    break
        finally:
            self._delivering = False
            self._eof.set()
            self._put_item(_PIPE_EOF)

    def _put_item(self, item: Any) -> bool:
        while not self._stop.is_set():
            try:
                self._queue.put(item, timeout=0.02)
                if self._on_message is not None:
                    try:
                        self._on_message()
                    except Exception:
                        logger.exception("Worker pipe wake-up callback failed")
                return True
            except queue.Full:
                if item is _PIPE_EOF:
                    try:
                        self._queue.get_nowait()
                    except queue.Empty:
                        pass
                continue
        return False

    @property
    def eof(self) -> bool:
        """读取线程是否已观测到 EOF 或终止。"""
        return self._eof.is_set()

    def take_raw(self) -> list[bytes | Exception]:
        """非阻塞取出当前已入队的全部原始帧或异常。"""
        deadline = time.monotonic() + 0.05
        while (
            not self._closed
            and not self._eof.is_set()
            and (self._delivering or self._has_pending_pipe_data())
            and not self._queue.full()
            and time.monotonic() < deadline
        ):
            time.sleep(0.001)
        items: list[bytes | Exception] = []
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is _PIPE_EOF:
                self._eof.set()
                break
            items.append(item)
        return items

    def take_messages(self) -> list[WorkerPipeMessage | Exception]:
        """非阻塞取出并解码当前已入队的全部消息（校验异常以 Exception 元素返回）。"""
        out: list[WorkerPipeMessage | Exception] = []
        for item in self.take_raw():
            if isinstance(item, Exception):
                out.append(item)
                continue
            try:
                out.append(
                    decode_worker_pipe_message(
                        item,
                        expected_job_uid=self.job_uid,
                        expected_execution_id=self.execution_id,
                        expected_run_id=self.run_id,
                        expected_attempt_no=self.attempt_no,
                    )
                )
            except Exception as exc:
                out.append(exc)
        return out

    def drain_to_eof(self, timeout: float = 5.0) -> list[WorkerPipeMessage | Exception]:
        """排空直到写端关闭触发 EOF（或超时）。"""
        deadline = time.monotonic() + max(0.0, timeout)
        out: list[WorkerPipeMessage | Exception] = []
        while True:
            out.extend(self.take_messages())
            if self._eof.is_set():
                out.extend(self.take_messages())
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                out.extend(self.take_messages())
                break
            try:
                item = self._queue.get(timeout=min(0.05, remaining))
            except queue.Empty:
                continue
            if item is _PIPE_EOF:
                self._eof.set()
                out.extend(self.take_messages())
                break
            if isinstance(item, Exception):
                out.append(item)
                continue
            try:
                out.append(
                    decode_worker_pipe_message(
                        item,
                        expected_job_uid=self.job_uid,
                        expected_execution_id=self.execution_id,
                        expected_run_id=self.run_id,
                        expected_attempt_no=self.attempt_no,
                    )
                )
            except Exception as exc:
                out.append(exc)
        return out

    def close(self, timeout: float = 3.0) -> None:
        """幂等关闭读端连接并等待读取线程退出。

        不变式：调用者必须先终止持有写端的子进程并关闭本进程残留写端句柄；
        若写端仍存活并卡在半包写入，读取线程无法在超时内退出，此处显式抛
        RuntimeError 暴露调用序违规。
        """
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            raise RuntimeError(
                f"worker pipe receiver thread for {self.execution_id} did not exit within {timeout}s; "
                "ensure worker process is terminated and sender handle is closed before closing receiver"
            )
        try:
            self._conn.close()
        except OSError:
            pass


def create_worker_pipe(
    process_context: Any = None,
    *,
    job_uid: str,
    execution_id: str,
    run_id: str | None = None,
    attempt_no: int | None = None,
    max_queued_messages: int = 64,
    on_message: Callable[[], None] | None = None,
) -> tuple[WorkerPipeReceiver, WorkerPipeSender]:
    """创建一对绑定的单向 (WorkerPipeReceiver, WorkerPipeSender)。"""
    pipe_fn = getattr(process_context, "Pipe", None)
    recv_conn, send_conn = (
        pipe_fn(duplex=False) if callable(pipe_fn) else mp.Pipe(duplex=False)
    )
    receiver = WorkerPipeReceiver(
        recv_conn,
        job_uid=job_uid,
        execution_id=execution_id,
        run_id=run_id,
        attempt_no=attempt_no,
        max_queued_messages=max_queued_messages,
        on_message=on_message,
    )
    sender = WorkerPipeSender(
        send_conn,
        job_uid=job_uid,
        execution_id=execution_id,
        run_id=run_id,
        attempt_no=attempt_no,
    )
    return receiver, sender


class WorkerReplySender:
    """主进程侧单向响应管道发送端。

    不变式：绑定 execution_id 单向向对应 worker 发送处理回复。
    """

    def __init__(
        self,
        connection: _PipeConnection,
        *,
        job_uid: str,
        execution_id: str,
        run_id: str | None = None,
        attempt_no: int | None = None,
    ) -> None:
        self._conn = connection
        self.job_uid = job_uid
        self.execution_id = execution_id
        self.run_id = run_id
        self.attempt_no = attempt_no
        self._lock = threading.Lock()
        self._closed = False

    def send_reply(self, payload: dict[str, Any]) -> None:
        """编码并发送单帧回复。"""
        envelope = {
            "job_uid": self.job_uid,
            "execution_id": self.execution_id,
            "run_id": self.run_id,
            "attempt_no": self.attempt_no,
            "payload": payload,
        }
        frame = dumps(envelope).encode("utf-8")
        with self._lock:
            if self._closed:
                raise OSError("worker reply sender is closed")
            self._conn.send_bytes(frame)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._conn.close()
            except OSError:
                pass


class WorkerReplyReceiver:
    """Worker 侧单向响应管道接收端。

    等待主进程回复，严格校验 execution_id 与 request_id，丢弃过期回复。
    """

    def __init__(
        self,
        connection: _PipeConnection,
        *,
        job_uid: str,
        execution_id: str,
        run_id: str | None = None,
        attempt_no: int | None = None,
    ) -> None:
        self._conn = connection
        self.job_uid = job_uid
        self.execution_id = execution_id
        self.run_id = run_id
        self.attempt_no = attempt_no
        self._closed = False

    def wait_reply(self, request_id: str, timeout: float | None = None) -> dict[str, Any]:
        """等待匹配执行身份与请求身份的回复；默认由主进程决定释放时机。"""
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("request_id must be a non-empty str")
        deadline = (
            None
            if timeout is None
            else time.monotonic() + max(0.1, timeout)
        )
        while True:
            if self._closed:
                raise OSError("worker reply receiver is closed")
            if deadline is None:
                poll_timeout = 0.05
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Worker timed out waiting for declaration reply (req={request_id})"
                    )
                poll_timeout = min(0.05, remaining)
            try:
                ready = self._conn.poll(poll_timeout)
            except (OSError, EOFError) as exc:
                raise OSError(
                    f"Worker reply pipe closed before receiving reply: {exc}"
                ) from exc
            if not ready:
                continue
            try:
                raw = self._conn.recv_bytes()
            except EOFError as exc:
                raise OSError(f"Worker reply pipe closed before receiving reply: {exc}") from exc
            except OSError as exc:
                raise OSError(f"Worker reply pipe closed before receiving reply: {exc}") from exc
            try:
                envelope = loads(raw.decode("utf-8"))
            except Exception:
                continue
            if not isinstance(envelope, dict):
                continue
            if envelope.get("job_uid") != self.job_uid:
                continue
            if envelope.get("execution_id") != self.execution_id:
                continue
            if self.run_id is not None and envelope.get("run_id") != self.run_id:
                continue
            if self.attempt_no is not None and envelope.get("attempt_no") != self.attempt_no:
                continue
            payload = envelope.get("payload")
            if isinstance(payload, dict) and payload.get("request_id") == request_id:
                return payload

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._conn.close()
        except OSError:
            pass


def create_worker_reply_pipe(
    process_context: Any = None,
    *,
    job_uid: str,
    execution_id: str,
    run_id: str | None = None,
    attempt_no: int | None = None,
) -> tuple[WorkerReplyReceiver, WorkerReplySender]:
    """创建一对绑定的单向回复管道 (WorkerReplyReceiver[worker端], WorkerReplySender[主进程端])。"""
    pipe_fn = getattr(process_context, "Pipe", None)
    recv_conn, send_conn = (
        pipe_fn(duplex=False) if callable(pipe_fn) else mp.Pipe(duplex=False)
    )
    receiver = WorkerReplyReceiver(
        recv_conn,
        job_uid=job_uid,
        execution_id=execution_id,
        run_id=run_id,
        attempt_no=attempt_no,
    )
    sender = WorkerReplySender(
        send_conn,
        job_uid=job_uid,
        execution_id=execution_id,
        run_id=run_id,
        attempt_no=attempt_no,
    )
    return receiver, sender


__all__ = [
    "PIPE_MSG_DECLARE",
    "PIPE_MSG_RESULT",
    "VALID_PIPE_MESSAGE_KINDS",
    "WorkerPipeMessage",
    "WorkerPipeReceiver",
    "WorkerPipeSender",
    "WorkerReplyReceiver",
    "WorkerReplySender",
    "create_worker_pipe",
    "create_worker_reply_pipe",
    "decode_worker_pipe_message",
]
