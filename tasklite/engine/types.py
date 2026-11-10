"""引擎值对象与枚举类型。

纯数据结构，零业务逻辑依赖——可被任意模块安全导入而不触发环形导入。
Handler 与默认执行配置归属 Task 规格层（``models/task.py``）。
"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..models.artifact import FileSpec
from ..models.job import Job

if TYPE_CHECKING:
    from ..selection import Exclude


class StopMode(enum.Enum):
    """停机状态机模式。"""

    NONE = "none"
    DRAINING = "draining"


class AuthorizationStatus(str, enum.Enum):
    """动态文件声明的授权请求状态。"""

    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    STOPPED = "stopped"
    PREPARATION_FAILED = "preparation_failed"


@dataclass
class PendingDeclaration:
    """一次等待主进程决定的动态文件声明。"""

    request_id: str
    name: str
    source: FileSpec
    bound: FileSpec
    resolved_path: str
    status: AuthorizationStatus = AuthorizationStatus.PENDING


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
    ipc_dir: str | None = None
    execution_id: str | None = None
    run_id: str | None = None
    attempt_no: int | None = None
    paused_remaining: float | None = None
    pending_declaration: PendingDeclaration | None = None
    authorization_outcome: AuthorizationOutcome | None = None

    @property
    def waiting_auth(self) -> bool:
        """当前执行是否正在等待动态文件声明授权。"""
        return (
            self.pending_declaration is not None
            and self.pending_declaration.status is AuthorizationStatus.PENDING
        )

    def pause_timeout(self) -> None:
        """等待外部授权或操作时暂停超时计时。"""
        if self.paused_remaining is None:
            self.paused_remaining = max(0.0, self.timeout_at_monotonic - time.monotonic())

    def resume_timeout(self) -> None:
        """恢复超时计时。"""
        if self.paused_remaining is not None:
            self.timeout_at_monotonic = time.monotonic() + self.paused_remaining
            self.paused_remaining = None


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


__all__ = [
    "AuthorizationOutcome",
    "AuthorizationStatus",
    "StopMode",
    "ExitReason",
    "ExecutionOptions",
    "JobHandle",
    "PendingDeclaration",
    "StepOutcome",
]
