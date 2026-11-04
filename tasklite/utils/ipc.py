"""瞬时 Worker 管道通信与产物清单：信封协议、有界反压、声明管理与生命周期清理。

统一内敛瞬时管道 IPC 与产物清单的完整生命周期管理：

- 单向管道传输：基于 ``multiprocessing.Pipe(duplex=False)`` 的原生帧收发
  （``send_bytes`` / ``recv_bytes``），每 worker 独立读取线程与有界队列，
  承载 ``input`` / ``output`` / ``suspend`` / ``result`` 四类信封消息；
- 路径与沙盒校验：空字节防御、跨盘多根沙盒匹配、相对路径规范重定位；
- 产物与输入声明：worker 侧预检、前删与实时管道发送，父进程侧内存态聚合、
  存在性校验与成败授权清理；
- 产物生命周期清理：按 PRE_SUBMIT / SUCCESS / FAILURE_OR_RETRY 模式清理
  已声明临时文件与目录，删除前消费侧沙盒归属复检。

分层红线：本模块是模型层（JobContext）的 IPC 声明与信号传输下沉点；
本层严禁出现任何重试节奏语义（节奏归 RequeuePolicy）。
"""

from __future__ import annotations

import enum
import json
import logging
import multiprocessing as mp
import os
import queue
import threading
import time
from dataclasses import dataclass
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any, Sequence

from ..exceptions import _ArtifactIOError
from ..models.artifact import (
    ArtifactDeclaration,
    append_artifact_declaration,
    validate_artifact_declarations,
)
from .artifact import ArtifactPathPolicy
from .jsonutil import dumps, loads

logger = logging.getLogger("tasklite")

# 管道消息种类常量
PIPE_MSG_INPUT = "input"
PIPE_MSG_OUTPUT = "output"
PIPE_MSG_SUSPEND = "suspend"
PIPE_MSG_RESULT = "result"
VALID_PIPE_MESSAGE_KINDS = frozenset(
    {
        PIPE_MSG_INPUT,
        PIPE_MSG_OUTPUT,
        PIPE_MSG_SUSPEND,
        PIPE_MSG_RESULT,
    }
)
_PIPE_EOF = object()
_RAW_TUPLE_SENTINEL = "__tl_tuple_v1"


@dataclass(frozen=True)
class WorkerPipeMessage:
    """已校验的 worker 管道信封消息。"""

    job_uid: str
    execution_id: str
    kind: str
    payload: Any


def decode_worker_pipe_message(
    raw: bytes,
    *,
    expected_job_uid: str,
    expected_execution_id: str,
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
    if kind not in VALID_PIPE_MESSAGE_KINDS:
        raise ValueError(f"unknown worker pipe message kind: {kind!r}")
    return WorkerPipeMessage(
        job_uid=expected_job_uid,
        execution_id=expected_execution_id,
        kind=kind,
        payload=envelope.get("payload"),
    )


class WorkerPipeSender:
    """Worker 侧单向管道发送端。

    不变式：单连接写操作以进程内线程锁串行化；支持随 WorkerLaunchSpec
    跨 spawn/fork 序列化传递并在子进程内重建线程锁。
    """

    def __init__(
        self,
        connection: Connection,
        *,
        job_uid: str,
        execution_id: str,
    ) -> None:
        self._conn = connection
        self.job_uid = job_uid
        self.execution_id = execution_id
        self._lock = threading.Lock()
        self._closed = False

    def __getstate__(self) -> dict[str, Any]:
        return {
            "conn": self._conn,
            "job_uid": self.job_uid,
            "execution_id": self.execution_id,
            "closed": self._closed,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self._conn = state["conn"]
        self.job_uid = state["job_uid"]
        self.execution_id = state["execution_id"]
        self._closed = state["closed"]
        self._lock = threading.Lock()

    def send_message(self, kind: str, payload: Any) -> None:
        """编码并发送单条信封消息。"""
        if kind not in VALID_PIPE_MESSAGE_KINDS:
            raise ValueError(f"unknown worker pipe message kind: {kind!r}")
        envelope = {
            "job_uid": self.job_uid,
            "execution_id": self.execution_id,
            "kind": kind,
            "payload": payload,
        }
        frame = dumps(envelope).encode("utf-8")
        with self._lock:
            if self._closed:
                raise OSError("worker pipe sender is closed")
            self._conn.send_bytes(frame)

    def send_input(self, resolved_path: str) -> None:
        """发送规范输入路径声明；底层管道错误转为 _ArtifactIOError。"""
        try:
            self.send_message(PIPE_MSG_INPUT, {"path": resolved_path})
        except OSError as exc:
            raise _ArtifactIOError(str(exc)) from exc

    def send_output(self, declaration: ArtifactDeclaration) -> None:
        """发送输出/缓存声明；底层管道错误转为 _ArtifactIOError。"""
        try:
            self.send_message(PIPE_MSG_OUTPUT, declaration.to_dict())
        except OSError as exc:
            raise _ArtifactIOError(str(exc)) from exc

    def send_suspend(self, resource_name: str, seconds: float) -> None:
        """发送资源挂起信号。"""
        self.send_message(PIPE_MSG_SUSPEND, [resource_name, float(seconds)])

    def send_result(self, result_dict: dict[str, Any]) -> None:
        """发送执行结果负载。"""
        self.send_message(PIPE_MSG_RESULT, result_dict)

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
        connection: Connection,
        *,
        job_uid: str,
        execution_id: str,
        max_queued_messages: int = 64,
    ) -> None:
        if max_queued_messages < 1:
            raise ValueError("max_queued_messages must be >= 1")
        self._conn = connection
        self.job_uid = job_uid
        self.execution_id = execution_id
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=max_queued_messages)
        self._stop = threading.Event()
        self._eof = threading.Event()
        self._delivering = False
        self._closed = False
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
                    if not self._conn.poll(0.05):
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
    max_queued_messages: int = 64,
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
        max_queued_messages=max_queued_messages,
    )
    sender = WorkerPipeSender(
        send_conn,
        job_uid=job_uid,
        execution_id=execution_id,
    )
    return receiver, sender


def encode_raw_result(raw_result: Any) -> Any:
    """编码 handler 返回值以便 JSON 传输（tuple 经哨兵标记还原）。"""
    if isinstance(raw_result, tuple):
        return [_RAW_TUPLE_SENTINEL, list(raw_result)]
    return raw_result


def decode_raw_result(encoded: Any) -> Any:
    """还原 handler 返回值。

    哨兵碰撞防御：用户返回值恰好形如 ``[哨兵, x]`` 且还原段非 list/tuple
    时原样返回——强行 ``tuple(x)`` 会在非序列还原段上抛 TypeError 穿透
    收割链（单任务损坏放大为整管崩溃）。
    """
    if (
        isinstance(encoded, list)
        and len(encoded) == 2
        and encoded[0] == _RAW_TUPLE_SENTINEL
        and isinstance(encoded[1], (list, tuple))
    ):
        return tuple(encoded[1])
    return encoded


class ArtifactCleanupMode(str, enum.Enum):
    """产物生命周期清理模式。"""

    PRE_SUBMIT = "pre_submit"
    SUCCESS = "success"
    FAILURE_OR_RETRY = "failure_or_retry"


class ArtifactJournal:
    """产物清单与管道声明管理深模块。

    封装沙盒越界防御、输入与输出声明登记、Worker 管道实时上报、
    产物存在性校验与多模式文件生命周期清理逻辑。
    """

    def __init__(
        self,
        ipc_dir: str | Path | None = None,
        output_roots: str | Path | Sequence[str | Path] | None = None,
        protected_roots: Sequence[str | Path] = (),
        *,
        pipe_sender: WorkerPipeSender | None = None,
    ) -> None:
        self.protected_roots = tuple(protected_roots)
        self.ipc_dir: str | None = str(ipc_dir) if ipc_dir is not None else None
        if output_roots is None:
            self.output_roots: list[Path] | None = None
        elif isinstance(output_roots, (str, Path)):
            self.output_roots = [Path(output_roots).resolve()]
        else:
            self.output_roots = [Path(r).resolve() for r in output_roots]
        self._pipe_sender: WorkerPipeSender | None = pipe_sender
        self._artifacts: dict[str, tuple[ArtifactDeclaration, ...]] = {}
        self._inputs: dict[str, list[str]] = {}

    def bind_pipe_sender(self, sender: WorkerPipeSender | None) -> None:
        """绑定或解绑当前 worker 的瞬时管道发送端。"""
        self._pipe_sender = sender

    # ── 路径沙盒与规范化 ──────────────────────────────────────────

    @staticmethod
    def resolve_and_validate_path(
        raw_path: str | Path,
        output_roots: str | Path | Sequence[str | Path] | None = None,
        *,
        sandbox: bool = True,
    ) -> str:
        """解析声明路径为规范绝对路径（沙盒校验 + 相对重定位共用逻辑）。"""
        raw = str(raw_path)
        if "\x00" in raw:
            raise ValueError(f"Output path contains a null byte: {raw!r}")

        if output_roots is not None and sandbox:
            p = Path(raw)
            if isinstance(output_roots, (list, tuple)):
                roots = [Path(r) for r in output_roots]
            else:
                roots = [Path(str(output_roots))]
            if not p.is_absolute():
                # 相对路径按第一个根重定位
                p = roots[0] / p
            resolved_path = Path(os.path.abspath(str(p)))
            try:
                resolved_path = resolved_path.resolve()
            except (OSError, RuntimeError):
                logger.debug(f"Could not resolve output path '{raw}', using abspath fallback.")
            if not any(resolved_path.is_relative_to(r) for r in roots):
                raise ValueError(
                    f"Output path '{raw}' resolves outside output_roots {roots!r}"
                )
            return str(resolved_path)

        p = Path(raw)
        try:
            if p.parent.exists():
                return str(p.resolve())
            return os.path.abspath(str(p))
        except (OSError, RuntimeError):
            return os.path.abspath(str(p))

    # ── 声明登记与管道发送 ────────────────────────────────────────

    def record_artifact(
        self, job_uid: str, declaration: ArtifactDeclaration, *, prepare: bool = False,
    ) -> None:
        """校验同执行授权与路径范围后登记，绑定管道时同步上报父进程。"""
        try:
            self._validate_artifact_path(declaration)
        except OSError as error:
            raise _ArtifactIOError(str(error)) from error
        previous = self._artifacts.get(job_uid, ())
        declarations = append_artifact_declaration(previous, declaration)
        if len(declarations) == len(previous):
            return
        if prepare:
            try:
                self.path_policy.remove_existing(declaration)
            except FileExistsError as error:
                if not declaration.delete_existing:
                    raise
                raise _ArtifactIOError(str(error)) from error
            except OSError as error:
                raise _ArtifactIOError(str(error)) from error
            try:
                Path(declaration.path).parent.mkdir(parents=True, exist_ok=True)
            except OSError as error:
                raise _ArtifactIOError(str(error)) from error
        if self._pipe_sender is not None:
            self._pipe_sender.send_output(declaration)
        self._artifacts[job_uid] = declarations

    @property
    def path_policy(self) -> ArtifactPathPolicy:
        """本执行配置对应的路径与保护范围。"""
        return ArtifactPathPolicy(
            output_roots=self.output_roots,
            protected_roots=(*self.protected_roots, *((self.ipc_dir,) if self.ipc_dir else ())),
        )

    def _validate_artifact_path(self, declaration: ArtifactDeclaration) -> None:
        resolved = self.path_policy.resolve(declaration.path, sandbox=declaration.sandbox)
        if resolved != declaration.path:
            raise ValueError("artifact declaration must use the canonical path")

    def record_input_entry(self, job_uid: str, entry: dict[str, Any]) -> None:
        """校验并记录一条输入声明条目，绑定管道时同步上报父进程。"""
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str) or not entry["path"]:
            raise ValueError(f"invalid input declaration for {job_uid}")
        resolved_path = entry["path"]
        if self._pipe_sender is not None:
            self._pipe_sender.send_input(resolved_path)
        self._inputs.setdefault(job_uid, []).append(resolved_path)

    def record_input_file(self, job_uid: str, resolved_path: str) -> str:
        """记录一条规范输入路径声明并返回该路径字符串。"""
        self.record_input_entry(job_uid, {"path": resolved_path})
        return resolved_path

    # ── 读取与校验（父进程 Engine 侧）─────────────────────────────

    def read_inputs(self, job_uid: str) -> list[str]:
        """读取已记录的输入路径列表。"""
        return list(self._inputs.get(job_uid, ()))

    def read_artifacts(self, job_uid: str) -> tuple[ArtifactDeclaration, ...]:
        """严格读取并复核本执行授权清单。"""
        declarations = self._artifacts.get(job_uid, ())
        for declaration in declarations:
            if declaration.name is not None:
                self._validate_artifact_path(declaration)
        return validate_artifact_declarations(declarations)

    # ── 产物校验与生命周期清理 ────────────────────────────────────

    def verify_artifacts(self, declarations: Sequence[ArtifactDeclaration]) -> str | None:
        """仅验证需保留的产物；临时路径允许已被 rename 移走。"""
        for declaration in declarations:
            self._validate_artifact_path(declaration)
            if not declaration.delete_on_success:
                try:
                    os.stat(declaration.path)
                except FileNotFoundError:
                    return f"Missing output {declaration.path}"
        return None

    def cleanup_declared_artifacts(
        self, declarations: Sequence[ArtifactDeclaration], *, success: bool,
    ) -> None:
        """只执行该结局的删除授权，错误不得降级为清理完成。"""
        policy = self.path_policy
        for declaration in declarations:
            policy.cleanup(declaration, success=success)

    def clear_declarations(self, job_uid: str) -> None:
        """业务转移已完成后清除内存声明；失败不改写已经提交的事实。"""
        self._artifacts.pop(job_uid, None)
        self._inputs.pop(job_uid, None)

    def cleanup(self, job_uid: str, mode: str | ArtifactCleanupMode) -> None:
        """清理本执行授权产物；派发前仅丢弃残留声明，不重放删除。"""
        mode = ArtifactCleanupMode(mode)
        if mode is not ArtifactCleanupMode.PRE_SUBMIT:
            self.cleanup_declared_artifacts(
                self.read_artifacts(job_uid), success=mode is ArtifactCleanupMode.SUCCESS,
            )
        self.clear_declarations(job_uid)


__all__ = [
    "ArtifactCleanupMode",
    "ArtifactJournal",
    "PIPE_MSG_INPUT",
    "PIPE_MSG_OUTPUT",
    "PIPE_MSG_RESULT",
    "PIPE_MSG_SUSPEND",
    "VALID_PIPE_MESSAGE_KINDS",
    "WorkerPipeMessage",
    "WorkerPipeReceiver",
    "WorkerPipeSender",
    "create_worker_pipe",
    "decode_raw_result",
    "decode_worker_pipe_message",
    "encode_raw_result",
]
