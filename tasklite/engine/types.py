"""引擎值对象与枚举类型。

纯数据结构，零业务逻辑依赖——可被任意模块安全导入而不触发环形导入。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any, Callable, Dict, NamedTuple, Optional

EMPTY_STATS = {
    "completed": 0,
    "failed": 0,
    "retried": 0,
    "skipped": 0,
    "hook_errors": 0,
    "deferred_orphan": 0,
    "interrupted_reruns": 0,
    "rate_limited_reruns": 0,
    "cascade_failed": 0,
}


class StopMode(enum.Enum):
    """停机状态机三态。"""
    NONE = "none"
    DRAINING = "draining"
    ABORTING = "aborting"


class ExitReason(str, enum.Enum):
    """引擎退出原因。"""
    COMPLETED = "completed"
    STOPPED_DRAINING = "stopped_draining"
    STOPPED_ABORTING = "stopped_aborting"
    INTERRUPTED = "interrupted"
    ERROR = "error"


class TaskStats(dict):
    """运行统计字典。"""

    def __init__(self) -> None:
        super().__init__(EMPTY_STATS)

    @property
    def completed(self) -> int:
        return self["completed"]

    @property
    def failed(self) -> int:
        return self["failed"]

    @property
    def retried(self) -> int:
        return self["retried"]

    @property
    def skipped(self) -> int:
        return self["skipped"]

    @property
    def hook_errors(self) -> int:
        return self["hook_errors"]

    @property
    def deferred_orphan(self) -> int:
        return self["deferred_orphan"]

    @property
    def interrupted_reruns(self) -> int:
        return self["interrupted_reruns"]

    @property
    def rate_limited_reruns(self) -> int:
        return self["rate_limited_reruns"]

    @property
    def cascade_failed(self) -> int:
        return self["cascade_failed"]


@dataclass(frozen=True)
class ExecutionOptions:
    """execute() 单次运行的动态选项。"""
    install_signals: bool = True
    acquire_run_lock: bool = True


@dataclass(frozen=True)
class StepOutcome:
    """step() 单步事件泵的执行产物。"""
    dispatched_count: int
    completed_count: int
    is_idle: bool
    should_wait: bool
    wait_time: float
    deadlock_detected: bool
    stop_mode: StopMode
    should_terminate: bool = False
    exit_reason: Optional[str] = None


@dataclass(frozen=True)
class RunSummary:
    """引擎执行完成后的不可变运行摘要。"""
    exit_reason: ExitReason
    stats: "TaskStats"
    run_id: str
    duration_seconds: float
    unhandled_exception: Optional[BaseException] = None


class HandlerEntry(NamedTuple):
    """注册 handler 的结构化条目——调度器与派发路径按字段名访问。"""
    func: Callable
    default_resources: Dict[str, float]
    payload_schema: Optional[type]
