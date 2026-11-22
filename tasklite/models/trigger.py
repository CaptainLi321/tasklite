"""主进程触发条件的只读判定上下文与内置触发策略。

分层约束：本模块位于 ``tasklite/models/``，严禁导入 ``engine`` / ``backend`` / ``wrappers``。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Mapping

from ..exceptions import TaskLiteError
from ..utils.clock import mtime_ns_to_us

if TYPE_CHECKING:
    from .job import Job


@dataclass(frozen=True)
class TriggerContext:
    """隔离于运行态的 Job 快照与成功历史视图。"""

    job: Job
    has_succeeded: bool
    has_failed: bool
    success_metadata: Mapping[str, Any] | None
    last_succeeded_at_us: int | None


def once(ctx: TriggerContext) -> bool:
    """只在尚无历史终态时执行。"""
    return not ctx.has_succeeded and not ctx.has_failed


def until_success(ctx: TriggerContext) -> bool:
    """尚未成功的作业可执行。"""
    return not ctx.has_succeeded


def always(ctx: TriggerContext) -> bool:
    """每次入队均执行。"""
    return True


def on_input_change(ctx: TriggerContext) -> bool:
    """仅比较输入路径自身的微秒 mtime，缺基线时保守执行。"""
    if not ctx.has_succeeded or ctx.last_succeeded_at_us is None:
        return True
    inputs = ctx.success_metadata.get("inputs") if ctx.success_metadata else None
    if not isinstance(inputs, list) or not inputs:
        raise TaskLiteError("on_input_change requires a non-empty inputs list")
    changed = False
    for path in inputs:
        if not isinstance(path, str) or not path:
            raise TaskLiteError("on_input_change inputs must contain non-empty paths")
        # 所有输入都须存在，不能让较早命中的变化掩盖后续缺失路径。
        changed |= mtime_ns_to_us(os.stat(path).st_mtime_ns) > ctx.last_succeeded_at_us
    return changed


BUILTIN_TRIGGER_FUNCTIONS: dict[str, Callable[[TriggerContext], bool]] = {
    "once": once,
    "until_success": until_success,
    "always": always,
    "on_input_change": on_input_change,
}

# 内置触发器名称单点派生自 BUILTIN_TRIGGER_FUNCTIONS，消除重复硬编码。
BUILTIN_TRIGGERS: tuple[str, ...] = tuple(BUILTIN_TRIGGER_FUNCTIONS)

__all__ = [
    "BUILTIN_TRIGGERS",
    "BUILTIN_TRIGGER_FUNCTIONS",
    "TriggerContext",
    "always",
    "on_input_change",
    "once",
    "until_success",
]
