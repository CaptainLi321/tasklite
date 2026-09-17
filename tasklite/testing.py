"""官方测试工具（冻结 TaskContext 内部构造结构对下游测试的影响面）。

handler 单测需要构造 TaskContext，直接以位置参数硬编码 wall/failed/
cursors 的内部容器结构会随框架演进碎裂——本模块是唯一承诺稳定的测试
构造入口（参数形态变更视为破坏性变更，与公开 API 同等对待）。
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from .models.context import TaskContext
from .models.job import Job

__all__ = ["fake_ctx"]


def fake_ctx(
    job: Job,
    *,
    wall: Optional[Iterable[str]] = None,
    failed: Optional[Iterable[str]] = None,
    cursors: Optional[Dict[str, str]] = None,
    resources: Optional[Iterable[str]] = None,
    tmp_root: Optional[Any] = None,
) -> TaskContext:
    """构造 handler 单测用 TaskContext。

    Args:
        job: 测试目标 job。
        wall: 已成功完成的 uid 集合（任意可迭代，``ctx.is_completed`` 命中源）。
        failed: 已进 DLQ 的 uid 集合（任意可迭代，``ctx.is_failed`` 命中源）。
        cursors: 游标初值映射（``ctx.get_cursor`` 命中源）。
        resources: 已注册资源名集合——提供时 ``ctx.suspend_resource`` 按
            名单 fail-loud 校验（与生产派发路径同语义）；缺省不校验。
        tmp_root: 提供时在其下创建一次性临时目录作为 ``output_root``，
            并建 ``ipc`` 子目录（declare_output/declare_cache 的产物
            清单记录随之可用）。路径经 ``ctx.output_root`` / ``ctx.ipc_dir``
            读取；目录清理由调用方的 tmp_root 机制（如 pytest tmp_path
            的会话级回收）统一承担。

    Returns:
        TaskContext: 内部容器结构由本函数兜底装配的上下文实例。
    """
    output_root: Optional[Path] = None
    ipc_dir: Optional[str] = None
    if tmp_root is not None:
        base = Path(tempfile.mkdtemp(dir=str(tmp_root)))
        output_root = base
        ipc_dir = str(base / "ipc")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)
    return TaskContext(
        job,
        set(wall) if wall is not None else set(),
        set(failed) if failed is not None else set(),
        dict(cursors) if cursors is not None else {},
        output_root=output_root,
        ipc_dir=ipc_dir,
        transient_registry=(),
        resource_names=frozenset(resources) if resources is not None else None,
    )
