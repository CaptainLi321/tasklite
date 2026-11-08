"""运行排除（Exclude）：单次 Run 按 task_type 排除候选作业的值对象。

运行选择使用 ``run(exclude=Exclude(task_type="..."))``；本版本仅接受单个 Exclude
或 None。Exclude 仅按 task_type 精确匹配。命中作业及其受影响下游本次留队（延迟），
不生成 attempt、不写失败档案、不消耗重试预算与资源；下一次 run 若不排除可继续执行。
"""
from __future__ import annotations

from dataclasses import dataclass

from .models.task import validate_task_type


@dataclass(frozen=True)
class Exclude:
    """按 ``task_type`` 完整名称精确匹配排除（关键字参数构造、不可变值对象）。"""

    task_type: str

    def __init__(self, *, task_type: str) -> None:
        validate_task_type(task_type)
        object.__setattr__(self, "task_type", task_type)

    def matches_task_type(self, task_type: str) -> bool:
        return task_type == self.task_type


__all__ = ["Exclude"]
