"""触发器求值与重入队接缝：核心仅负责注册与派发接线。"""
from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol

from ..exceptions import TaskLiteError
from ..models.job import Job
from ..models.task import validate_trigger
from ..models.trigger import BUILTIN_TRIGGER_FUNCTIONS, TriggerContext
from ..utils.clock import parse_utc_iso_to_us


def extract_trusted_metadata_timestamp_us(
    metadata: Mapping[str, Any] | None,
) -> int | None:
    """从 successes 档案 metadata 中提取并校验框架写入的可信完成时间戳（整数微秒）。

    过滤人工档案的空字典 ``{}``、缺失或非正整数的 ``success_count``、
    非法 ``last_success_run_id`` 以及缺失或无法解析的 ``last_succeeded_at``。
    """
    if not isinstance(metadata, Mapping) or not metadata:
        return None
    success_count = metadata.get("success_count")
    if (
        not isinstance(success_count, int)
        or isinstance(success_count, bool)
        or success_count < 1
    ):
        return None
    if "last_success_run_id" in metadata:
        run_id = metadata.get("last_success_run_id")
        if not isinstance(run_id, str) or not run_id:
            return None
    last_succeeded_at = metadata.get("last_succeeded_at")
    if not isinstance(last_succeeded_at, str) or not last_succeeded_at:
        return None
    try:
        return parse_utc_iso_to_us(last_succeeded_at)
    except (TypeError, ValueError):
        return None


class TerminalHistory(Protocol):
    """准入判定消费的终态历史契约（PipelineState 满足）。"""

    @property
    def successes(self) -> Mapping[str, dict]: ...

    @property
    def failed(self) -> Mapping[str, dict]: ...


class TriggerRegistry:
    """触发器名称注册与主进程 ``TriggerContext`` 求值的唯一出口。"""

    def __init__(
        self,
        *,
        functions: Mapping[str, Callable[[TriggerContext], bool]] | None = None,
    ) -> None:
        self.functions = dict(
            BUILTIN_TRIGGER_FUNCTIONS if functions is None else functions
        )

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name in self.functions

    def require_registered(self, name: Any) -> str:
        """校验触发器名称已注册；未注册时 fail-loud 抛 TaskLiteError。"""
        if not isinstance(name, str) or name not in self.functions:
            raise TaskLiteError(f"unregistered trigger {name!r}")
        return name

    def register(self, name: str, fn: Callable[[TriggerContext], bool]) -> None:
        validate_trigger(name)
        if name in self.functions:
            raise TaskLiteError(f"trigger {name!r} already registered")
        if not callable(fn):
            raise TaskLiteError("trigger function must be callable")
        self.functions[name] = fn

    def admit(
        self,
        job_snapshot: dict,
        history: TerminalHistory,
        *,
        baseline_lookup: Callable[[str], int | None] | None = None,
    ) -> bool:
        """依赖就绪后构造隔离快照；任何判定错误原样向外抛出。"""
        job = Job.from_dict(copy.deepcopy(job_snapshot))
        name = job.trigger or "once"
        try:
            fn = self.functions[name]
        except KeyError:
            raise TaskLiteError(f"unregistered trigger {name!r}") from None
        uid = job.uid
        metadata = history.successes.get(uid)
        ctx = TriggerContext(
            job=job,
            has_succeeded=uid in history.successes,
            has_failed=uid in history.failed,
            success_metadata=(
                MappingProxyType(copy.deepcopy(metadata)) if metadata is not None else None
            ),
            last_succeeded_at_us=(
                baseline_lookup(uid)
                if baseline_lookup is not None and uid in history.successes else None
            ),
        )
        result = fn(ctx)
        if type(result) is not bool:
            raise TaskLiteError(f"trigger {name!r} must return bool, got {type(result).__name__}")
        return result


@dataclass(frozen=True)
class RequeuePlan:
    """重入队节奏规划（不可变值对象）。

    ``front`` 为 True 表示插队首（同轮内优先重扫，重试不被新入队作业
    排挤到饥饿尾部）；``delay_seconds`` 是延迟类策略的表达位——默认
    实现恒为 0（立即）。
    """

    front: bool
    delay_seconds: float = 0.0


class RequeuePolicy(ABC):
    """已决定重试的作业经此内部接缝规划重入队位置。"""

    @abstractmethod
    def plan_requeue(self, job_snapshot: dict) -> RequeuePlan:
        """为已获准重试的作业规划节奏与位置。"""


class ImmediateRequeuePolicy(RequeuePolicy):
    """默认立即插入队首；重试预算由结算机器判断。"""

    def plan_requeue(self, job_snapshot: dict) -> RequeuePlan:
        return RequeuePlan(front=True, delay_seconds=0.0)


__all__ = [
    "ImmediateRequeuePolicy",
    "RequeuePlan",
    "RequeuePolicy",
    "TerminalHistory",
    "TriggerRegistry",
    "extract_trusted_metadata_timestamp_us",
]
