"""v2 官方测试工具（单测 JobContext 构造器与运行状态模拟）。

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

import multiprocessing as mp
import tempfile
import threading
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

from .models.context import JobContext
from .models.job import Job

__all__ = [
    "authorizer",
    "cursor_updates",
    "fake_ctx",
    "in_process_workers",
    "running",
    "spawned_jobs",
]


class _InProcessWorker:
    """进程内线程 Worker 替身：在真实 Pipe 边界上执行目标入口。"""

    def __init__(
        self,
        *,
        target: Callable[..., Any],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._target = target
        self._args = args
        self._kwargs = dict(kwargs or {})
        self.exitcode: int | None = None
        self._thread: threading.Thread | None = None

    def _run(self) -> None:
        try:
            self._target(*self._args, **self._kwargs)
            if self.exitcode is None:
                self.exitcode = 0
        except BaseException:
            if self.exitcode is None:
                self.exitcode = 1

    def start(self) -> None:
        """启动后台线程运行 worker 入口。"""
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def is_alive(self) -> bool:
        """返回替身线程是否仍在运行。"""
        return self._thread is not None and self._thread.is_alive()

    def join(self, timeout: float | None = None) -> None:
        """等待替身线程结束。"""
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def kill(self) -> None:
        """标记强制终止退出码（不销毁进程内主线程）。"""
        if self.exitcode is None:
            self.exitcode = -9

    def terminate(self) -> None:
        """标记信号终止退出码。"""
        if self.exitcode is None:
            self.exitcode = -15

    def close(self) -> None:
        """释放替身线程引用。"""
        self._thread = None


class _InProcessSpawnContext:
    """只替换 Process，Pipe 仍由真实 spawn 上下文创建。"""

    def __init__(self, process_class: type) -> None:
        self._context = mp.get_context("spawn")
        self.Process = process_class

    def Pipe(self, *args: Any, **kwargs: Any) -> Any:
        return self._context.Pipe(*args, **kwargs)


def _resolve_process_context_holders(target: Any) -> list[Any]:
    holders: list[Any] = []
    if hasattr(target, "_process_context"):
        holders.append(target)
    channel = getattr(target, "_channel", getattr(target, "channel", None))
    if channel is not None and hasattr(channel, "_process_context") and channel not in holders:
        holders.append(channel)
    if not holders:
        raise TypeError(
            f"in_process_workers target has no _process_context: {type(target).__name__}"
        )
    return holders


def _install_process_context(
    target: Any,
    process_class: type | None = None,
) -> list[tuple[Any, Any]]:
    context = _InProcessSpawnContext(process_class or _InProcessWorker)
    holders = _resolve_process_context_holders(target)
    previous = [(holder, holder._process_context) for holder in holders]
    for holder in holders:
        holder._process_context = context
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
    process_class: type | None = None,
) -> Iterator[Any]:
    """在测试上下文中启用进程内/线程轻量 Worker 替身。

    保留真实单向 ``Pipe`` 通信边界，仅将子进程创建替换为进程内受控替身
    （默认使用线程替身，也可显式传入自定义 ``process_class``），退出上下文时
    自动恢复原有的 ``_process_context``。

    Yields:
        传入的 ``target`` 对象本身。
    """
    previous = _install_process_context(target, process_class)
    try:
        yield target
    finally:
        for holder, orig in previous:
            holder._process_context = orig


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

