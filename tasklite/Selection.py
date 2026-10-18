"""执行选择：pipeline 级「默认全执行 + 指定 task_type 不执行」的规则值对象。

Selection 是 TaskLite 的属性（``pipeline.selection(...)`` 配置），不是
``run()`` 的参数：未选中作业不派发、不写 attempt 轨迹、不进入 wall/
失败档案（含级联失败与批量熔断出口）、不消耗资源与重试预算，原样留在
持久化 queue（跨设备队列交接的前提）。

规则是职责单一的值对象（``Exclude(type="pdf_to_md")``）。``Rule`` 协议
要求 ``matches_type(task_type)``——调度热路径与终态写入出口都只做
task_type 维度判定，缺少该入口的规则在注册时 fail-loud 拒绝，不会静默
漏判。

规则属配置代码：``matches_type`` 抛出的异常不被吞掉，直接穿透运行主循环
（``run()`` 以 ERROR 上抛）——配置错误 fail-loud，绝不静默降级为「全执行」
或「全排除」。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from .models.task import validate_task_type


class Rule(Protocol):
    """选择规则协议：``matches_type(task_type)`` 为真表示该类型不执行。"""

    def matches_type(self, task_type: str) -> bool: ...


@dataclass(frozen=True)
class Exclude:
    """按 ``job.task_type`` 精确排除的规则（当前唯一内置规则）。

    构造期只做 task_type 格式校验，不校验注册表存在性——选择规则可以先于
    ``register_task`` 配置，也可以跨设备复用（不感知对端注册了哪些任务）。
    """

    type: str

    def __post_init__(self) -> None:
        validate_task_type(self.type)

    def matches_type(self, task_type: str) -> bool:
        return task_type == self.type


class Selection:
    """规则集（引擎内部持有）：构造后规则元组不再变更；空规则 = 全执行，任一命中 = 不选中。"""

    def __init__(self, rules: Iterable[Rule] = ()):
        materialized = tuple(rules)
        for rule in materialized:
            if not callable(getattr(rule, "matches_type", None)):
                raise TypeError(
                    "selection rules must implement Rule.matches_type(task_type), "
                    f"got {type(rule).__name__} ({rule!r})"
                )
        self._rules = materialized

    @property
    def is_active(self) -> bool:
        """是否配置了任何规则（空规则集即默认全执行）。"""
        return bool(self._rules)

    def allows_type(self, task_type: str) -> bool:
        """调度热路径与终态写入出口的唯一判定入口。"""
        return not any(rule.matches_type(task_type) for rule in self._rules)


__all__ = ["Exclude", "Rule", "Selection"]
