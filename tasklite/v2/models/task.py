"""Task 规格层：进程内注册的静态任务模板（ADR-0004 三层模型的规格位）。

Task = 产生同构 Job 序列的静态规格模板：task_type 名、handler、默认
resources、payload_schema 与默认重试/超时策略。代码即规格，不落盘；
注册于 TaskRegistry（``register_task`` 的唯一落点）。

与 Job 的字段分工：Task 持有「该类任务全部实例共享」的默认值，Job 的
规格位字段在构造时可覆盖这些默认；实例位字段（attempt_no /
activation_no / first_enqueued_at）只属于单次激活，不出现在 Task。

本模块同时承载模型层共享的校验单点（``validate_task_type`` /
``validate_retry_budget`` / ``validate_timeout`` / ``validate_timeout_flag``
/ ``validate_resource_amounts``）：Task 的规格字段与 Job 的同名字段共用
同一套拒绝规则与报错文案，单点维护防止两侧语义漂移。
"""
from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


def _is_finite(value: Any) -> bool:
    """math.isfinite 的溢出安全版：超出 float 范围的超大 int（如 10**400）
    转换溢出抛 OverflowError（ArithmeticError 子类，会命中 fatal 启发式），
    此处按「非有限」收敛，交由调用方既有 ValueError/TypeError 通道明确
    报错。"""
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def validate_resource_amounts(resources: Any, where: str = "resources") -> None:
    """校验资源 amount 映射（Task 默认资源与 Job resources 的共用单点）。

    负值/NaN/Inf 会在调度器 acquire 时抛 ValueError（若抛在 try 之外，
    部分资源永久泄漏），且 NaN 会毒化容量型资源的 used 账目导致 livelock，
    构造期即入口拒绝。bool 是 int 子类，isinstance(True, (int, float))
    为 True 会放行，故显式拒绝。非 str 资源名在调度侧判为未知资源
    （永不可跑），且经 JSON 落盘后键强转为 str，同一作业跨重启从死锁
    翻转为可跑（身份与账目漂移），同样入口拒绝。
    """
    if not isinstance(resources, dict):
        raise TypeError(
            f"resources in {where} must be a dict, got {type(resources).__name__}"
        )
    for res_name, amount in resources.items():
        if not isinstance(res_name, str) or not res_name:
            raise TypeError(
                f"resource name in {where} must be a non-empty str, "
                f"got {type(res_name).__name__} ({res_name!r})"
            )
        if not isinstance(amount, (int, float)) or isinstance(amount, bool):
            raise TypeError(
                f"resource '{res_name}' amount in {where} must be a number, "
                f"got {type(amount).__name__} ({amount!r})"
            )
        if not _is_finite(amount):
            # 超大 int（如 10**400）使 isfinite 转 float 溢出抛 OverflowError
            # ——与 NaN/Inf 同路收敛为 ValueError，而非异常类型漂移
            raise ValueError(
                f"resource '{res_name}' amount in {where} is too large "
                f"(exceeds float range), got {amount!r}"
            )
        if amount < 0:
            raise ValueError(
                f"resource '{res_name}' amount in {where} must be non-negative, "
                f"got {amount!r}"
            )


def validate_task_type(task_type: Any) -> None:
    """task_type 入口校验：非空 str 且不含 '::'（uid 分隔符）。

    Task 规格与 Job 实例共用（同名同规则），报错文案为两侧单一真相。
    """
    if not isinstance(task_type, str):
        raise TypeError(
            f"task_type must be a str, got {type(task_type).__name__} ({task_type!r})"
        )
    if not task_type:
        raise ValueError(f"task_type must be a non-empty str, got {task_type!r}")
    if "::" in task_type:
        raise ValueError(f"task_type must not contain '::', got {task_type!r}")


def _validate_handler(handler: Any) -> None:
    """handler 入口校验：必须可调用。"""
    if not callable(handler):
        raise TypeError(
            f"handler must be callable, got {type(handler).__name__} ({handler!r})"
        )


def _validate_payload_schema(payload_schema: Any) -> None:
    """payload_schema 入口校验：None 或类型对象。

    文档约定 payload_schema 必须是 TypedDict 类；传实例/字符串等会在
    运行时校验时静默失效，入口 fail-loud。
    """
    if payload_schema is not None and not isinstance(payload_schema, type):
        raise TypeError(
            f"payload_schema must be a type (e.g. TypedDict class) or None, "
            f"got {type(payload_schema).__name__}"
        )


def validate_retry_budget(max_retries: Any) -> None:
    """max_retries 入口校验：非 bool 的 int 且 >= 0（Task/Job 共用）。

    isinstance 检查必须在 < 0 之前——字符串脏数据会先触发比较处的原始
    TypeError，友好报错失效。
    """
    if not isinstance(max_retries, int) or isinstance(max_retries, bool):
        raise TypeError(
            f"max_retries must be an int, got {type(max_retries).__name__} "
            f"({max_retries!r})"
        )
    if max_retries < 0:
        raise ValueError(f"max_retries must be >= 0, got {max_retries}")


def validate_timeout(timeout: Any) -> None:
    """timeout 入口校验：有限正数（Task/Job 共用）。

    NaN 使 ``timeout <= 0`` 校验恒 False 而漏过（NaN <= 0 为 False），
    deadline = monotonic + NaN = NaN，``now > NaN`` 恒 False → 看门狗
    永不触发，挂死 job 永不 kill——NaN/Inf 一律拒绝。bool 是 int 子类，
    同样拒绝（timeout=True 被接受为 1 秒）。
    """
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
        raise TypeError(
            f"timeout must be a number, got {type(timeout).__name__} ({timeout!r})"
        )
    if not _is_finite(timeout) or timeout <= 0:
        raise ValueError(f"timeout must be finite and > 0, got {timeout}")


def validate_timeout_flag(timeout_is_transient: Any) -> None:
    """timeout_is_transient 入口校验：必须为 bool（Task/Job 共用）。

    非 bool 值（如字符串）在执行侧的 ``job.timeout_is_transient`` 判断时
    被当作真值，静默改变超时归类语义，入口拒绝。
    """
    if not isinstance(timeout_is_transient, bool):
        raise TypeError(
            f"timeout_is_transient must be a bool, got "
            f"{type(timeout_is_transient).__name__} ({timeout_is_transient!r})"
        )


@dataclass(frozen=True)
class Task:
    """产生同构 Job 序列的静态规格模板（不落盘，代码即规格）。

    Attributes:
        task_type: 任务类型名（须与 Job.task_type 一致）；非空 str 且不含
            ``::``（uid 分隔符）。
        handler: 任务处理函数，期望签名 ``handler(job, ctx)``，返回值语义：
            None/True 表示成功，False 表示失败，dict 作为成功元数据，
            tuple[bool, dict] 组合形式；抛 RetryError 推回队列重试，抛
            FatalError 直接进失败档案（不消耗重试预算）。
        default_resources: 该类任务的默认资源占用映射；Job 构造时可用
            ``resources`` 覆盖。构造后浅拷贝隔离外部就地变异。
        payload_schema: 可选 TypedDict 类，派发前对 payload 做运行时
            校验，校验失败直接进失败档案。
        max_retries: 默认重试预算上限（对齐 v2 Job 默认 3）；允许执行
            条件为 ``attempt_no <= max_retries + 1``。
        timeout: 默认执行超时秒数（对齐 v2 Job 默认 3600），有限正数。
        timeout_is_transient: 超时是否视为瞬态失败（默认 False 保持保守
            语义）。True 时看门狗超时按重试处理而非直接进失败档案，
            适用于设计上可能长跑的任务——「超时」的语义是「没跑完」，
            不等于「确定性失败」。
    """

    task_type: str
    handler: Callable[..., Any]
    default_resources: dict[str, float] | None = None
    payload_schema: type | None = None
    max_retries: int = 3
    timeout: int | float = 3600
    # kw-only：规格位尾部布尔以显式关键字表达，位置传参在构造点静默
    # 漂移（如误占 timeout 位）由签名直接拒绝
    timeout_is_transient: bool = field(kw_only=True, default=False)

    def __post_init__(self) -> None:
        validate_task_type(self.task_type)
        _validate_handler(self.handler)
        _validate_payload_schema(self.payload_schema)
        validate_retry_budget(self.max_retries)
        validate_timeout(self.timeout)
        validate_timeout_flag(self.timeout_is_transient)
        resources_raw = self.default_resources if self.default_resources is not None else {}
        # 容器类型与 amount 校验须先于拷贝——str/序列等可迭代类型会在
        # dict() 拷贝处抛不可读的原始 ValueError
        validate_resource_amounts(resources_raw, "default_resources")
        resources = dict(resources_raw) if resources_raw else {}
        # frozen 实例的字段规范化须经 object.__setattr__；浅拷贝隔离外部
        # 就地变异（规格不可变，默认资源账目不得被注册方漂移）
        object.__setattr__(self, "default_resources", resources)


class TaskRegistry:
    """进程内 Task 规格注册表（``register_task`` 的唯一落点）。

    不变式：task_type → Task 一对一；重复注册显式报错而非静默覆盖
    （覆盖会让已入队 Job 挂到新 handler 上，规格与实例错配），未注册
    lookup 显式报错（拼错 task_type 的 Job 不得静默无人处理）。
    """

    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}

    def register(self, task: Task) -> None:
        """注册 Task 规格；task_type 重复注册显式 ValueError。"""
        if not isinstance(task, Task):
            raise TypeError(
                f"task must be a Task instance, got {type(task).__name__} ({task!r})"
            )
        if task.task_type in self._tasks:
            raise ValueError(
                f"task_type {task.task_type!r} already registered; duplicate "
                f"registration would silently replace the live spec"
            )
        self._tasks[task.task_type] = task

    def lookup(self, task_type: str) -> Task:
        """按 task_type 查询规格；未注册显式 KeyError。"""
        if not isinstance(task_type, str):
            raise TypeError(
                f"task_type must be a str, got {type(task_type).__name__} "
                f"({task_type!r})"
            )
        try:
            return self._tasks[task_type]
        except KeyError:
            raise KeyError(
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
    "validate_timeout",
    "validate_timeout_flag",
]
