"""引擎值对象与枚举类型。

纯数据结构，零业务逻辑依赖——可被任意模块安全导入而不触发环形导入。
Handler 与默认执行配置归属 Task 规格层（``models/task.py``）。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..models.job import Job

if TYPE_CHECKING:
    from ..selection import Exclude


class StopMode(enum.Enum):
    """停机状态机模式。"""

    NONE = "none"
    DRAINING = "draining"


class AuthorizationStatus(str, enum.Enum):
    """主进程文件授权的统一状态。"""

    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    STOPPED = "stopped"
    PREPARATION_FAILED = "preparation_failed"


@dataclass(frozen=True)
class AuthorizationOutcome:
    """受阻执行的终态授权结果及可持久化原因。"""

    status: AuthorizationStatus
    message: str
    path: str
    exception: dict[str, str] | None = None


class ExitReason(str, enum.Enum):
    """引擎退出原因。"""

    COMPLETED = "completed"
    STOPPED_DRAINING = "stopped_draining"
    INTERRUPTED = "interrupted"
    ERROR = "error"


@dataclass
class JobHandle:
    """一个在途子进程的纯句柄值对象（在飞追踪与收割看门狗共用）。

    ``timeout_at_monotonic``（monotonic 时刻）与 ``timeout_seconds`` 驱动看门狗超时判定；
    ``execution_id``（``run_id.dispatch_no``）是本次执行体的身份串，
    结果认证与归属都锚定它。进程对象本身以 ``Any`` 承载——进程 seam
    的具体形态由执行通道装配，本值对象不绑定。
    """

    job_uid: str
    process: Any
    timeout_at_monotonic: float
    timeout_seconds: float
    job: Job
    execution_id: str | None = None
    run_id: str | None = None
    attempt_no: int | None = None
    started_at: str | None = None


@dataclass(frozen=True)
class ExecutionOptions:
    """execute() 单次运行的动态选项。"""

    install_signals: bool = True
    exclude: Exclude | None = None


@dataclass(frozen=True)
class StepOutcome:
    """step() 单步事件泵的执行产物。"""

    dispatched_count: int
    settled_count: int
    is_idle: bool
    should_wait: bool
    wait_time: float
    deadlock_detected: bool
    stop_mode: StopMode
    should_terminate: bool = False
    exit_reason: str | None = None


# 子进程引导私有 IPC 环境变量键名与放行令牌（集中定义，避免导入链提前加载 _worker_bootstrap）
ENV_IPC_ADDR = "_TASKLITE_IPC_ADDR"
ENV_IPC_FAMILY = "_TASKLITE_IPC_FAMILY"
ENV_IPC_AUTHKEY = "_TASKLITE_IPC_AUTHKEY"
LAUNCH_GO_TOKEN = b"GO"

# 引导期 __mp_main__ 重入守卫：拦截缺失 if __name__ == "__main__" 时的递归启动
_IMPORTING_MP_MAIN: bool = False


def is_importing_mp_main() -> bool:
    """当前进程是否正处于 worker 引导导入 ``__mp_main__`` 阶段。"""
    return _IMPORTING_MP_MAIN


def _set_importing_mp_main(value: bool) -> None:
    """内部接口：仅由 worker 引导模块在导入 __mp_main__ 前后切换重入标志。"""
    global _IMPORTING_MP_MAIN
    _IMPORTING_MP_MAIN = value


__all__ = [
    "AuthorizationOutcome",
    "AuthorizationStatus",
    "ENV_IPC_ADDR",
    "ENV_IPC_AUTHKEY",
    "ENV_IPC_FAMILY",
    "StopMode",
    "ExitReason",
    "ExecutionOptions",
    "JobHandle",
    "LAUNCH_GO_TOKEN",
    "StepOutcome",
    "is_importing_mp_main",
]
