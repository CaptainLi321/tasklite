"""v2 官方测试工具（单测 JobContext 构造器与运行状态模拟）。

注意：本模块为测试辅助工具，本版本暂不承诺向后兼容与冻结扩展协议。
handler 单测构造 JobContext，提供对文件声明、游标、依赖引用与派生的隔离测试支持。
``fake_ctx`` 生成真实 JobContext 实例，严格遵循产物沙盒策略、受保护路径、
资源注册与依赖合法性校验，不绕过授权或状态事实宣称真实能力。

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

import tempfile
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

from .models.context import JobContext
from .models.job import Job

__all__ = ["fake_ctx", "running"]


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
    resources: Iterable[str] | None = None,
    tmp_root: Any | None = None,
    output_roots: Any | None = None,
    ipc_dir: str | None = None,
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
        resources: 已注册资源名集合——``ctx.suspend_resource`` 按名单
            fail-loud 校验（与生产派发路径同语义）；缺省为空集，suspend
            一律拒绝（测试挂起语义必须显式传名单）。
        tmp_root: 提供时在其下创建一次性临时目录作为 ``output_roots``，
            并建 ``ipc`` 子目录（declare_output/declare_cache 的产物
            清单记录随之可用）。路径经 ``ctx.output_roots`` / ``ctx.ipc_dir``
            读取；目录清理由调用方的 tmp_root 机制（如 pytest tmp_path
            的会话级回收）统一承担。
        output_roots: 直接指定产物根目录（与 tmp_root 互斥使用）。
        ipc_dir: 直接指定 IPC 目录（caller 选定目录，非 tmp_root 子目录）。
        base_dir: 相对路径解析基准目录（默认当前工作目录）。
        known_job_uids: 允许被引用的已知 Job UID 集合（派生校验用）。
        files: 预声明文件规范字典（默认继承 job.files）。

    Returns:
        JobContext: 内部容器结构由本函数兜底装配的真实上下文实例。
    """
    if tmp_root is not None:
        base = Path(tempfile.mkdtemp(dir=str(tmp_root)))
        output_roots = base
        if ipc_dir is None:
            ipc_dir = str(base / "ipc")
            Path(ipc_dir).mkdir(parents=True, exist_ok=True)
    elif ipc_dir is not None:
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)
    return JobContext(
        job,
        set(successes) if successes is not None else set(),
        set(failed) if failed is not None else set(),
        dict(cursors) if cursors is not None else {},
        output_roots=Path(output_roots) if isinstance(output_roots, str) else output_roots,
        ipc_dir=ipc_dir,
        resource_names=frozenset(resources or ()),
        artifact_descriptions=artifacts,
        protected_roots=protected_roots,
        base_dir=base_dir,
        known_job_uids=frozenset(known_job_uids) if known_job_uids is not None else None,
        files=files,
    )

