"""v2 引擎层：调度 seam 与机器群。

本包承载引擎机器群与共享值对象（当前已立模块：错误分类 errorclass、
引擎值对象 types、资源体系 resource）。调度逻辑仅留 seam：选择点收敛在
scheduler 的 scan_next_runnable，重试节奏收敛在 RequeuePolicy（默认
立即重入队）。

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
from .errorclass import ErrorClassifier
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
    ExitReason,
    StopMode,
    TaskStats,
)
from .wait import LoopFacts, WaitDecision, decide_wait

__all__ = [
    "ErrorClassifier",
    "EMPTY_STATS",
    "TRANSIENT_KIND_STAT_KEYS",
    "ExitReason",
    "StopMode",
    "TaskStats",
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
]
