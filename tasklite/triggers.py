"""配置层内置触发策略；核心只负责注册与求值接线。"""
from __future__ import annotations

import os

from .models.trigger import TriggerContext
from .utils.clock import mtime_ns_to_us


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
        raise ValueError("on_input_change requires a non-empty inputs list")
    changed = False
    for path in inputs:
        if not isinstance(path, str) or not path:
            raise ValueError("on_input_change inputs must contain non-empty paths")
        # 所有输入都须存在，不能让较早命中的变化掩盖后续缺失路径。
        changed |= mtime_ns_to_us(os.stat(path).st_mtime_ns) > ctx.last_succeeded_at_us
    return changed


BUILTIN_TRIGGER_FUNCTIONS = {
    "once": once,
    "until_success": until_success,
    "always": always,
    "on_input_change": on_input_change,
}
