"""执行选择（Selection）：pipeline 级单机「本次不执行 / 终态跳过」规则集。

Selection 是 TaskLite 的属性（``pipeline.selection(...)`` 配置），不是
``run()`` 的参数。两类规则语义严格区分：

- ``Exclude(type=...)``：**本次不执行、原样留队**。命中作业不派发、不写
  attempt 轨迹、不进入 wall/失败档案、不消耗资源与重试预算；其下游因依赖
  未完成而停摆，但同样留队，下一次 run（上游不再被排除/换运行配置/换策略）自动
  恢复执行。留队是「延迟」而非「失败」。
- ``Skip(type=...)``：**终态跳过**。命中作业从 queue 移除，不进入 wall /
  失败档案（既不成功也不失败）；直接或间接依赖它的下游永远无法完成，
  级联判定为跳过（``ERR_DEPENDENCY_SKIPPED``），一并移出 queue，下次运行
  不再重试。Skip 的终态记录落 append-only attempt 轨迹（outcome=skipped）
  与 ``stats["skipped"]``，属于旁路观测面，不参与全局互斥。

同一 task_type 同时命中 Skip 与 Exclude 时 **Skip 优先**（终态意图强于
延迟意图）；非 ``Skip`` 形状的自定义规则一律归入 Exclude 语义。

规则是职责单一的值对象。``Rule`` 协议要求 ``matches_type(task_type)``——
调度热路径与终态写入出口都只做 task_type 维度判定，缺少该入口的规则在
注册时 fail-loud 拒绝，不会静默漏判。规则属配置代码：``matches_type``
抛出的异常不被吞掉，直接穿透运行主循环（``run()`` 以 ERROR 上抛）——
配置错误 fail-loud，绝不静默降级为「全执行」或「全排除」。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from .models.task import validate_task_type


class Rule(Protocol):
    """选择规则协议：``matches_type(task_type)`` 为真表示命中。"""

    def matches_type(self, task_type: str) -> bool: ...


@dataclass(frozen=True)
class Exclude:
    """按 ``job.task_type`` 精确排除：本次不执行、原样留队（延迟语义）。

    构造期只做 task_type 格式校验，不校验注册表存在性——选择规则可以先于
    ``register_task`` 配置，也可以跨运行窗口复用（不感知对端注册了哪些任务）。
    """

    type: str

    def __post_init__(self) -> None:
        validate_task_type(self.type)

    def matches_type(self, task_type: str) -> bool:
        return task_type == self.type


@dataclass(frozen=True)
class Skip:
    """按 ``job.task_type`` 精确跳过：终态跳过 + 下游级联跳过（永久语义）。

    构造期只做 task_type 格式校验（同 Exclude）。
    """

    type: str

    def __post_init__(self) -> None:
        validate_task_type(self.type)

    def matches_type(self, task_type: str) -> bool:
        return task_type == self.type


class Selection:
    """规则集（引擎内部持有）：构造后规则元组不再变更；空规则 = 全执行。"""

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

    @property
    def has_skip_rules(self) -> bool:
        """是否配置了终态跳过规则（Skip 关闭时运行期跳过阶段整体短路）。"""
        return any(isinstance(rule, Skip) for rule in self._rules)

    def is_skipped(self, task_type: str) -> bool:
        """是否命中终态跳过规则（Skip 优先于 Exclude）。"""
        return any(
            isinstance(rule, Skip) and rule.matches_type(task_type)
            for rule in self._rules
        )

    def is_excluded(self, task_type: str) -> bool:
        """是否命中本次排除规则（Skip 命中时不再按 Exclude 处理）。"""
        if self.is_skipped(task_type):
            return False
        return any(
            not isinstance(rule, Skip) and rule.matches_type(task_type)
            for rule in self._rules
        )

    def allows_type(self, task_type: str) -> bool:
        """本 run 是否允许派发该 task_type（既未 Skip 也未 Exclude）。"""
        return not self.is_skipped(task_type) and not self.is_excluded(task_type)


__all__ = ["Exclude", "Rule", "Selection", "Skip"]
