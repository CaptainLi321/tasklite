"""引擎值对象与枚举类型。

纯数据结构，零业务逻辑依赖——可被任意模块安全导入而不触发环形导入。
Handler 归属 Task 规格层（``models/task.py`` 的 Task 单点持有 handler、
默认资源与 payload_schema），本模块不再有条目形态。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any

from ..models.job import Job

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
    "excluded_blocked": 0,
}

# 瞬态信号种类登记表：kind → 统计键。瞬态军规（不烧重试预算 + 豁免失败
# 档案 + 零污染）按 kind 单值判定，新信号在此登记一行并同步 wire 写端
# （channel worker 侧）与降级写盘保真（utils/ipc）即可全链生效；
# 完备性由 tests/engine/test_types.py 的注册表遍历锁定。
TRANSIENT_KIND_STAT_KEYS = {
    "interrupted": "interrupted_reruns",
    "lock_conflict": "deferred_orphan",
    "rate_limited": "rate_limited_reruns",
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


class RunStats(dict):
    """运行统计字典（键集即公开监控面，读写一律下标风格）。"""

    def __init__(self) -> None:
        super().__init__(EMPTY_STATS)


@dataclass(frozen=True)
class AttemptOutcome:
    """一次 attempt 收尾事件的不可变载荷（on_attempt_finished 钩子唯一形状）。

    布尔语义经值对象承载、不裸传：``success`` 与 ``will_retry``
    正交——成功 True/False；失败将重试 False/True；失败耗尽进失败档案
    False/False。``metadata`` 是终态元数据字典视图（成功或失败档案）。
    """

    success: bool
    will_retry: bool
    metadata: dict[str, Any]


@dataclass
class JobHandle:
    """一个在途子进程的句柄（在飞追踪与收割看门狗共用的值对象）。

    ``timeout_at_monotonic``（monotonic 时刻）与 ``timeout_seconds`` 驱动看门狗超时判定；
    ``execution_id``（``run_id.dispatch_no``）是本次执行体的身份串，
    结果认证与锁归属都锚定它。进程对象本身以 ``Any`` 承载——进程 seam
    的具体形态由执行通道装配，本值对象不绑定。
    """

    job_uid: str
    process: Any
    timeout_at_monotonic: float
    timeout_seconds: float
    job: Job
    ipc_dir: str
    execution_id: str | None = None


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
    exit_reason: str | None = None


@dataclass(frozen=True)
class RunSummary:
    """引擎执行完成后的不可变运行摘要。

    契约：execute() 的未处理异常一律经 raise 通道原样上抛，摘要仅在
    无异常终结（完成/优雅停机）时返回——execute() 自身永不回填
    unhandled_exception（恒为 None）；该字段供包装层构造异常 run 的
    摘要时显式回填。
    """

    exit_reason: ExitReason
    stats: RunStats
    run_id: str
    duration_seconds: float
    unhandled_exception: BaseException | None = None


__all__ = [
    "EMPTY_STATS",
    "TRANSIENT_KIND_STAT_KEYS",
    "AttemptOutcome",
    "StopMode",
    "ExitReason",
    "RunStats",
    "ExecutionOptions",
    "JobHandle",
    "StepOutcome",
    "RunSummary",
]
