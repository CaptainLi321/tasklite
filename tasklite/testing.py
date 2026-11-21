"""v2 官方测试工具（单测 JobContext 构造器、进程内 WorkerLauncher 与运行状态模拟）。

注意：本模块为测试辅助工具，本版本暂不承诺向后兼容与冻结扩展协议。
handler 单测构造 JobContext，提供对文件声明、游标、依赖引用与派生的隔离测试支持。
``fake_ctx`` 生成真实 JobContext 实例，严格遵循产物沙盒策略、受保护路径、
依赖合法性校验，不绕过授权或状态事实宣称真实能力。

Examples:
    >>> from pathlib import Path
    >>> from tasklite import Job, FileSpec, TaskResult
    >>> from tasklite.testing import fake_ctx
    >>> job = Job("fetch", "1", files={"out": FileSpec("data.json", role="output")})
    >>> ctx = fake_ctx(job, tmp_root=Path("/tmp"), cursors={"since": "2026-01-01"})
    >>> out_path = ctx.file_path("out")
    >>> ctx.set_cursor("since", "2026-01-02")
    >>> res = TaskResult.success(metadata={"count": 10})
"""
from __future__ import annotations

import dataclasses
import tempfile
import threading
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

from .engine._worker_runtime import WorkerLaunchSpec, run_worker_entry
from .engine.launcher import (
    PreparedWorker,
    WorkerLauncher,
    _WorkerLaunchCancelled,
)
from .models.context import JobContext
from .models.job import Job
from .utils.ipc import (
    WorkerPipeSender,
    WorkerReplyReceiver,
    create_worker_pipe,
    create_worker_reply_pipe,
)

__all__ = [
    "InProcessWorkerLauncher",
    "authorizer",
    "cursor_updates",
    "fake_ctx",
    "in_process_workers",
    "running",
    "spawned_jobs",
]


class _ThreadWorkerHandle:
    """进程内线程 Worker 句柄：直接实现 ``ManagedWorkerHandle`` 并管理 worker 侧管道端点。

    不变式：与真实 OS 子进程退出/强杀时内核立即关闭写端 FD 保持一致，线程退出、
    ``kill_tree()`` 或 ``close()`` 均会幂等关闭 worker 侧 ``pipe_sender`` 与
    ``reply_receiver``，令父进程侧 ``drain_to_eof`` 立即观测到 EOF，无需
    ``ExecutionChannel`` 对进程内替身保留特殊清理分支。
    """

    def __init__(
        self,
        *,
        entry: Callable[[], None],
        pipe_sender: WorkerPipeSender,
        reply_receiver: WorkerReplyReceiver,
    ) -> None:
        self._entry = entry
        self._pipe_sender: WorkerPipeSender | None = pipe_sender
        self._reply_receiver: WorkerReplyReceiver | None = reply_receiver
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._exitcode: int | None = None
        self._was_force_killed = False
        self._closed = False

    def _close_worker_pipes(self) -> None:
        with self._lock:
            sender = self._pipe_sender
            reply_receiver = self._reply_receiver
            self._pipe_sender = None
            self._reply_receiver = None
        if sender is not None:
            try:
                sender.close()
            except OSError:
                pass
        if reply_receiver is not None:
            try:
                reply_receiver.close()
            except OSError:
                pass

    def _run(self) -> None:
        try:
            self._entry()
            with self._lock:
                if self._exitcode is None:
                    self._exitcode = 0
        except BaseException:
            with self._lock:
                if self._exitcode is None:
                    self._exitcode = 1
        finally:
            self._close_worker_pipes()

    def start(self) -> None:
        """启动后台线程运行 worker 执行体。"""
        with self._lock:
            if self._closed:
                return
            thread = threading.Thread(target=self._run, daemon=True)
            self._thread = thread
        thread.start()

    @property
    def pid(self) -> int | None:
        return None

    @property
    def exitcode(self) -> int | None:
        with self._lock:
            return self._exitcode

    @property
    def was_force_killed(self) -> bool:
        with self._lock:
            return self._was_force_killed

    def is_alive(self) -> bool:
        with self._lock:
            if self._closed:
                return False
            thread = self._thread
        return thread is not None and thread.is_alive()

    def join(self, timeout: float | None = None) -> None:
        with self._lock:
            if self._closed:
                return
            thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        if not self.is_alive():
            self._close_worker_pipes()

    def kill_tree(self) -> None:
        with self._lock:
            self._was_force_killed = True
            if self._exitcode is None:
                self._exitcode = -9
        self._close_worker_pipes()

    def kill(self) -> None:
        self.kill_tree()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._thread = None
        self._close_worker_pipes()


class InProcessWorkerLauncher(WorkerLauncher):
    """基于真实双工 Pipe 与后台线程的进程内标准 ``WorkerLauncher`` 实现。"""

    def __init__(
        self,
        *,
        entry: Callable[[WorkerLaunchSpec], None] = run_worker_entry,
    ) -> None:
        self._entry = entry

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

        receiver, sender = create_worker_pipe(
            job_uid=spec.job.uid,
            execution_id=spec.execution_id,
            run_id=spec.run_id or None,
            attempt_no=spec.attempt_no,
            on_message=on_message,
        )
        reply_receiver, reply_sender = create_worker_reply_pipe(
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
        )
        handle = _ThreadWorkerHandle(
            entry=lambda: self._entry(launch_spec),
            pipe_sender=sender,
            reply_receiver=reply_receiver,
        )

        def _release() -> None:
            if cancel_event.is_set():
                raise _WorkerLaunchCancelled(
                    f"stop requested before worker start for {spec.job.uid}"
                )
            handle.start()

        def _abort() -> None:
            try:
                if handle.is_alive():
                    handle.kill_tree()
            except Exception:
                pass
            try:
                handle.join(timeout=5.0)
            except Exception:
                pass
            try:
                handle.close()
            except Exception:
                pass
            try:
                reply_sender.close()
            except Exception:
                pass
            try:
                receiver.close(timeout=2.0)
            except Exception:
                pass

        return PreparedWorker(
            handle=handle,
            receiver=receiver,
            reply_sender=reply_sender,
            on_release=_release,
            on_abort=_abort,
        )


def _resolve_launcher_holders(target: Any) -> list[Any]:
    holders: list[Any] = []
    if hasattr(target, "_launcher"):
        holders.append(target)
    channel = getattr(target, "_channel", getattr(target, "channel", None))
    if channel is not None and hasattr(channel, "_launcher") and channel not in holders:
        holders.append(channel)
    if not holders:
        raise TypeError(
            f"in_process_workers target has no _launcher: {type(target).__name__}"
        )
    return holders


def _install_launcher(
    target: Any,
    launcher: WorkerLauncher | None = None,
) -> list[tuple[Any, Any]]:
    if launcher is not None and not isinstance(launcher, WorkerLauncher):
        raise TypeError(
            f"in_process_workers expects a WorkerLauncher instance or None, "
            f"got {type(launcher).__name__}"
        )
    effective_launcher: WorkerLauncher = (
        launcher if launcher is not None else InProcessWorkerLauncher()
    )
    holders = _resolve_launcher_holders(target)
    previous = [(holder, holder._launcher) for holder in holders]
    for holder in holders:
        holder._launcher = effective_launcher
    return previous


@contextmanager
def authorizer(
    target: Any,
    fn: Callable[[str, str], bool | None] | None,
) -> Iterator[Any]:
    """临时为 TaskLite / EngineRuntime / StateStore 注入授权回调。

    进入上下文时保存原 ``authorizer`` 并替换为 ``fn``，退出时自动恢复原值，
    避免测试直接修改 ``_runtime.store.authorizer`` 导致跨用例污染。

    Yields:
        被注入授权回调的底层 ``StateStore`` 实例。
    """
    rt = getattr(target, "_runtime", target)
    store = getattr(rt, "store", rt)
    if not hasattr(store, "authorizer"):
        raise TypeError(
            f"authorizer target has no store.authorizer attribute: {type(target).__name__}"
        )
    previous = store.authorizer
    store.authorizer = fn
    try:
        yield store
    finally:
        store.authorizer = previous


@contextmanager
def in_process_workers(
    target: Any,
    launcher: WorkerLauncher | None = None,
) -> Iterator[Any]:
    """在测试上下文中启用进程内/自定义 ``WorkerLauncher`` 替身。

    保留真实单向 ``Pipe`` 通信边界，仅将子进程启动器替换为标准 ``WorkerLauncher``
    实例（默认使用线程替身 ``InProcessWorkerLauncher()``），退出上下文时自动恢复
    原有的 ``_launcher``。

    Yields:
        传入的 ``target`` 对象本身。
    """
    previous = _install_launcher(target, launcher)
    try:
        yield target
    finally:
        for holder, orig in previous:
            holder._launcher = orig


def spawned_jobs(ctx: JobContext) -> list[Job]:
    """返回 ``JobContext`` 在本次执行期间通过 ``ctx.spawn()`` 登记的子作业浅拷贝。"""
    if not isinstance(ctx, JobContext):
        raise TypeError(f"spawned_jobs expects a JobContext, got {type(ctx).__name__}")
    return list(ctx._spawned_jobs)


def cursor_updates(ctx: JobContext) -> dict[str, str | None]:
    """返回 ``JobContext`` 在本次执行期间通过 ``ctx.set_cursor()`` 暂存的游标更新浅拷贝。"""
    if not isinstance(ctx, JobContext):
        raise TypeError(f"cursor_updates expects a JobContext, got {type(ctx).__name__}")
    return dict(ctx._cursor_updates)


@contextmanager
def running(target: Any) -> Iterator[Any]:
    """把 TaskLite / EngineRuntime 置于「run 期间」状态（测试专用句柄）。

    管理 API 的 run 期守卫读 ``_runtime._is_running``——测试直接翻转该
    私有标志需自备 try/finally 复位，漏复位会污染同测试会话的后续用例。
    本句柄保证复位；只翻一个布尔，不触碰 RunSession 生命周期
    （begin()/钩子语义属 execute()，守卫测试只要旗子）。

    Yields:
        被置位的对象本身（EngineRuntime 或其包装的 TaskLite）。
    """
    rt = getattr(target, "_runtime", target)
    rt._is_running = True
    try:
        yield rt
    finally:
        rt._is_running = False


def fake_ctx(
    job: Job,
    *,
    successes: Iterable[str] | None = None,
    artifacts: dict[str, dict[str, dict[str, str]]] | None = None,
    protected_roots: tuple[str | Path, ...] = (),
    failed: Iterable[str] | None = None,
    cursors: dict[str, str] | None = None,
    tmp_root: Any | None = None,
    base_dir: str | Path | None = None,
    known_job_uids: Iterable[str] | None = None,
    files: Mapping[str, Any] | None = None,
) -> JobContext:
    """构造 handler 单测用 JobContext。

    Args:
        job: 测试目标 job。
        successes: 已成功完成的 uid 集合（依赖闭合与产物引用命中源）。
        artifacts: 按生产者 UID 注入具名成功描述；仍须显式提供 successes 和直接依赖。
        protected_roots: 不可声明为产物的框架状态目录。
        failed: 已进失败档案的 uid 集合。
        cursors: 游标初值映射（``ctx.get_cursor`` 命中源）。
        tmp_root: 提供时在其下创建一次性临时目录作为相对路径解析基准（``base_dir``）。
            目录清理由调用方的 tmp_root 机制（如 pytest tmp_path 的会话级
            回收）统一承担。
        base_dir: 相对路径解析基准目录（默认当前工作目录）。
        known_job_uids: 允许被引用的已知 Job UID 集合（派生校验用）。
        files: 预声明文件规范字典（默认继承 job.files）。

    Returns:
        JobContext: 内部容器结构由本函数兜底装配的真实上下文实例。
    """
    if tmp_root is not None and base_dir is None:
        base_dir = Path(tempfile.mkdtemp(dir=str(tmp_root)))
    return JobContext(
        job,
        set(successes) if successes is not None else set(),
        set(failed) if failed is not None else set(),
        dict(cursors) if cursors is not None else {},
        artifact_descriptions=artifacts,
        protected_roots=protected_roots,
        base_dir=base_dir,
        known_job_uids=frozenset(known_job_uids) if known_job_uids is not None else None,
        files=files,
    )
