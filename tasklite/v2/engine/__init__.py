"""v2 引擎层：调度 seam 与机器群。

本包承载引擎机器群与共享值对象（当前已立模块：错误分类 errorclass、
引擎值对象 types、资源体系 resource、准入与重入队策略 admission、
等待决策 wait、在飞追踪 in_flight、只读扫描调度器 scheduler、
死锁治理 governor、执行通道 channel、恢复编排 recovery、状态仓库
store、派发机器 dispatch、完成机器 completion）。调度逻辑仅留
seam：选择点收敛在 scheduler 的 scan_next_runnable（访问序经
OrderingPolicy），重试节奏收敛在 RequeuePolicy（默认立即重入队）。

分层红线：本包属核心层，严禁 import ``v2/wrappers/`` 与 ``v2/contrib/``，
亦不得依赖 v1 旧树任何模块；可依赖 ``v2/{models,utils,backend}``。
"""
from .admission import (
    ImmediateRequeuePolicy,
    PreflightAction,
    PreflightDecision,
    RequeuePlan,
    RequeuePolicy,
    RerunPolicy,
)
from .channel import (
    AbortOutcome,
    ExecutionChannel,
    ExecutionResult,
    WorkerLaunchSpec,
)
from .completion import CompletionMachine
from .dispatch import DispatchKind, DispatchMachine, DispatchOutcome
from .errorclass import ErrorClassifier
from .governor import (
    DEADLOCK_GAP_MAX_ROUNDS,
    DEP_GRACE_SECONDS,
    DeadlockDecision,
    DeadlockGovernor,
)
from .in_flight import InFlightJob, InFlightTracker
from .recovery import RecoveryOrchestrator
from .scheduler import (
    DeadlockAttribution,
    FifoOrderingPolicy,
    JobFacts,
    JobScheduler,
    OrderingPolicy,
    ScheduleResult,
    StandstillFacts,
)
from .store import (
    BulkFailureOutcome,
    COMMIT_FAILURE_THRESHOLD,
    FailureEntry,
    FailureOutcome,
    RetryOutcome,
    SkipOutcome,
    StateStore,
    SuccessOutcome,
)
from .resource import (
    CapacityResource,
    RateLimitResource,
    Resource,
    ResourceEvaluation,
    ResourceManager,
)
from .types import (
    EMPTY_STATS,
    TRANSIENT_KIND_STAT_KEYS,
    AttemptFinish,
    ExitReason,
    JobHandle,
    StopMode,
    TaskStats,
)
from .wait import LoopFacts, WaitDecision, decide_wait

__all__ = [
    "ErrorClassifier",
    "EMPTY_STATS",
    "TRANSIENT_KIND_STAT_KEYS",
    "AttemptFinish",
    "ExitReason",
    "StopMode",
    "TaskStats",
    "AbortOutcome",
    "ExecutionChannel",
    "ExecutionResult",
    "WorkerLaunchSpec",
    "DispatchKind",
    "DispatchMachine",
    "DispatchOutcome",
    "CompletionMachine",
    "Resource",
    "RateLimitResource",
    "CapacityResource",
    "ResourceEvaluation",
    "ResourceManager",
    "ImmediateRequeuePolicy",
    "PreflightAction",
    "PreflightDecision",
    "RequeuePlan",
    "RequeuePolicy",
    "RerunPolicy",
    "LoopFacts",
    "WaitDecision",
    "decide_wait",
    "InFlightJob",
    "InFlightTracker",
    "RecoveryOrchestrator",
    "JobHandle",
    "DeadlockAttribution",
    "FifoOrderingPolicy",
    "JobFacts",
    "JobScheduler",
    "OrderingPolicy",
    "ScheduleResult",
    "StandstillFacts",
    "DEADLOCK_GAP_MAX_ROUNDS",
    "DEP_GRACE_SECONDS",
    "DeadlockDecision",
    "DeadlockGovernor",
    "BulkFailureOutcome",
    "COMMIT_FAILURE_THRESHOLD",
    "FailureEntry",
    "FailureOutcome",
    "RetryOutcome",
    "SkipOutcome",
    "StateStore",
    "SuccessOutcome",
]
