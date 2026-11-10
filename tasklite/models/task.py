"""Task 规格层：进程内注册的静态任务模板（ADR-0004 三层模型的规格位）。

Task = 产生同构 Job 序列的静态规格模板：task_type 名、handler、默认
resources 与默认重试/超时/触发策略。代码即规格，不落盘；
注册于 TaskRegistry（``register_task`` 的唯一落点）。

与 Job 的字段分工：Task 持有「该类任务全部实例共享」的默认值，Job 的
配置字段在构造时可覆盖这些默认；执行编号由后端分配，不属于 Task 默认配置。

本模块同时承载模型层共享的校验单点（``validate_task_type`` /
``validate_retry_budget`` / ``validate_timeout_seconds`` /
``validate_resource_amounts``）：Task 的规格字段与 Job 的同名字段共用
同一套拒绝规则与报错文案，单点维护防止两侧语义漂移。
"""
from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ..exceptions import TaskLiteError


def validate_trigger(trigger: Any) -> None:
    """触发器引用仅允许非空名称，名称解析留在主进程派发点。"""
    if not isinstance(trigger, str):
        raise TaskLiteError("trigger must be a non-empty str")
    if not trigger:
        raise TaskLiteError("trigger must be a non-empty str")


def _is_finite(value: Any) -> bool:
    """超大整数与非有限浮点数采用相同拒绝规则。"""
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def validate_resource_amounts(resources: Any, where: str = "resources") -> None:
    """校验资源映射，拒绝会破坏容量账目或持久化身份的名称与数值。"""
    if not isinstance(resources, dict):
        raise TaskLiteError(
            f"resources in {where} must be a dict, got {type(resources).__name__}"
        )
    for res_name, amount in resources.items():
        if not isinstance(res_name, str) or not res_name:
            raise TaskLiteError(
                f"resource name in {where} must be a non-empty str, "
                f"got {type(res_name).__name__} ({res_name!r})"
            )
        if not isinstance(amount, (int, float)) or isinstance(amount, bool):
            raise TaskLiteError(
                f"resource '{res_name}' amount in {where} must be a number, "
                f"got {type(amount).__name__} ({amount!r})"
            )
        if not _is_finite(amount):
            raise TaskLiteError(
                f"resource '{res_name}' amount in {where} is too large "
                f"(exceeds float range), got {amount!r}"
            )
        if amount < 0:
            raise TaskLiteError(
                f"resource '{res_name}' amount in {where} must be non-negative, "
                f"got {amount!r}"
            )


def validate_task_type(task_type: Any) -> None:
    """task_type 入口校验：非空 str（Task 规格与 Job 实例共用）。"""
    if not isinstance(task_type, str):
        raise TaskLiteError(
            f"task_type must be a str, got {type(task_type).__name__} ({task_type!r})"
        )
    if not task_type:
        raise TaskLiteError(f"task_type must be a non-empty str, got {task_type!r}")
    try:
        task_type.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise TaskLiteError(
            f"task_type must be valid UTF-8 str, got {task_type!r}"
        ) from exc


def _validate_handler(handler: Any) -> None:
    """handler 入口校验：必须可调用。"""
    if not callable(handler):
        raise TaskLiteError(
            f"handler must be callable, got {type(handler).__name__} ({handler!r})"
        )


def validate_retry_budget(max_retries_per_run: Any) -> None:
    """max_retries_per_run 入口校验：非 bool 的 int 且 >= 0（Task/Job 共用）。

    isinstance 检查必须在 < 0 之前——字符串脏数据会先触发比较处的原始
    TypeError，友好报错失效。
    """
    if not isinstance(max_retries_per_run, int) or isinstance(max_retries_per_run, bool):
        raise TaskLiteError(
            f"max_retries_per_run must be an int, got {type(max_retries_per_run).__name__} "
            f"({max_retries_per_run!r})"
        )
    if max_retries_per_run < 0:
        raise TaskLiteError(f"max_retries_per_run must be >= 0, got {max_retries_per_run}")


def validate_timeout_seconds(timeout_seconds: Any) -> None:
    """timeout_seconds 入口校验：有限正数（Task/Job 共用）。

    NaN 使 ``timeout_seconds <= 0`` 校验恒 False 而漏过（NaN <= 0 为 False），
    deadline = monotonic + NaN = NaN，``now > NaN`` 恒 False → 看门狗
    永不触发，挂死 job 永不 kill——NaN/Inf 一律拒绝。bool 是 int 子类，
    同样拒绝（timeout_seconds=True 被接受为 1 秒）。
    """
    if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool):
        raise TaskLiteError(
            f"timeout_seconds must be a number, got {type(timeout_seconds).__name__} ({timeout_seconds!r})"
        )
    if not _is_finite(timeout_seconds) or timeout_seconds <= 0:
        raise TaskLiteError(f"timeout_seconds must be finite and > 0, got {timeout_seconds}")


@dataclass(frozen=True)
class Task:
    """产生同构 Job 序列的静态规格模板（不落盘，代码即规格）。

    Attributes:
        task_type: 任务类型名（须与 Job.task_type 一致）；非空 str。
        handler: 任务处理函数，期望签名 ``handler(job, ctx)``。
        resources: 该类任务的默认资源占用映射；Job 构造时可用
            ``resources`` 按 key 覆盖。构造后浅拷贝隔离外部就地变异。
        max_retries_per_run: 单次 Run 内默认消耗预算重试的上限（默认 3），
            与跨 Run 的执行编号相互独立。
        timeout_seconds: 默认执行超时秒数（默认 3600），有限正数。
        trigger: 首次尝试的触发器名称，默认 once；发现任务默认 always。
    """

    task_type: str
    handler: Callable[..., Any]
    resources: dict[str, float] | None = field(kw_only=True, default=None)
    max_retries_per_run: int = field(kw_only=True, default=3)
    timeout_seconds: int | float = field(kw_only=True, default=3600)
    trigger: str = field(kw_only=True, default="once")

    def __post_init__(self) -> None:
        validate_task_type(self.task_type)
        _validate_handler(self.handler)
        validate_retry_budget(self.max_retries_per_run)
        validate_timeout_seconds(self.timeout_seconds)
        validate_trigger(self.trigger)
        resources_raw = self.resources if self.resources is not None else {}
        # 容器类型与 amount 校验须先于拷贝——str/序列等可迭代类型会在
        # dict() 拷贝处抛不可读的原始 ValueError
        validate_resource_amounts(resources_raw, "resources")
        resources = dict(resources_raw) if resources_raw else {}
        # frozen 实例的字段规范化须经 object.__setattr__；浅拷贝隔离外部
        # 就地变异（规格不可变，默认资源账目不得被注册方漂移）
        object.__setattr__(self, "resources", resources)


class TaskRegistry:
    """进程内 Task 规格注册表（``register_task`` 的唯一落点）。

    不变式：task_type → Task 一对一；重复注册显式报错而非静默覆盖
    （覆盖会让已入队 Job 挂到新 handler 上，规格与实例错配），未注册
    lookup 显式报错（拼错 task_type 的 Job 不得静默无人处理）。
    """

    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}

    def register(self, task: Task) -> None:
        """注册 Task 规格；task_type 重复注册明确拒绝。"""
        if not isinstance(task, Task):
            raise TaskLiteError(
                f"task must be a Task instance, got {type(task).__name__} ({task!r})"
            )
        if task.task_type in self._tasks:
            raise TaskLiteError(
                f"task_type {task.task_type!r} already registered; duplicate "
                f"registration would silently replace the live spec"
            )
        self._tasks[task.task_type] = task

    def lookup(self, task_type: str) -> Task:
        """按 task_type 查询规格；未注册明确拒绝。"""
        if not isinstance(task_type, str):
            raise TaskLiteError(
                f"task_type must be a str, got {type(task_type).__name__} "
                f"({task_type!r})"
            )
        try:
            return self._tasks[task_type]
        except KeyError:
            raise TaskLiteError(
                f"unknown task_type {task_type!r} (register the task before "
                f"enqueuing its jobs)"
            ) from None

    def get(self, task_type: str) -> Task | None:
        """按 task_type 查询规格；未注册返回 None（dict-like 读口）。

        供 ResourceManager 等按 Mapping 语义消费默认资源的装配组件
        使用；缺省回退语义（None → 无默认）由调用方表达。
        """
        return self._tasks.get(task_type)

    def __contains__(self, task_type: object) -> bool:
        return isinstance(task_type, str) and task_type in self._tasks

    def task_types(self) -> tuple[str, ...]:
        """已注册 task_type 的只读元组视图（注册序）。"""
        return tuple(self._tasks)


__all__ = [
    "Task",
    "TaskRegistry",
    "validate_resource_amounts",
    "validate_retry_budget",
    "validate_task_type",
    "validate_timeout_seconds",
    "validate_trigger",
]
