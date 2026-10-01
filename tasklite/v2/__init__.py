"""TaskLite v2 子包：Task/Job/Attempt 三层模型（ADR-0004）。

模型：Task 为进程内注册的静态规格模板；Job 为一次有界激活的逻辑实例
（uid = task_type::job_id）；Attempt 为 append-only 的执行轨迹。编码族
统一 ``encode_*`` 可逆单射转义；失败终态集合称「失败档案 failed」；
重试节奏由 RequeuePolicy seam 收敛，核心不含退避与排序计算。

公开面已集成：TaskLite 门面（六步调用契约）、模型层、异常族、错误
分类（ErrorClassifier 与 ERR_* 错误码）、单射编码族、资源体系、失败
档案条目、用户钩子三件套、准入与重入队策略 seam、排序策略 seam、
持久化后端与运行摘要值对象，均经本文件集中导出（``from
tasklite.v2 import ...`` 是唯一承诺稳定的导入面）。版本随 v1 主包
``tasklite.__version__`` 单一事实源，本子包不设 __version__。

隔离红线：v2 严禁 import v1——``tasklite.v2`` 不依赖旧树任何模块，
保证独立演进与最终整体替换；v1 处于冻结期，仅允许缺陷修复。分层
依赖红线镜像：``v2/models/`` 严禁 import ``v2/engine/``，
``v2/utils/`` 严禁 import ``v2/wrappers/``，v2 核心层严禁依赖
``v2/contrib/``。

设计契约、命名映射表与迁移军规总纲见 docs/adr/0004-v2-parallel-rebuild.md。
"""
from __future__ import annotations

# ── 门面（六步调用契约的唯一入口）────────────────────────────────────
from .pipeline import TaskLite

# ── 模型层：规格 / 实例 / 轨迹 / 上下文 ──────────────────────────────
from .models.attempt import AttemptRecord
from .models.context import JobContext
from .models.job import Job, JobRuntimeState
from .models.task import Task, TaskRegistry

# ── 异常族 ───────────────────────────────────────────────────────────
from .exceptions import (
    FatalError,
    PipelineError,
    RateLimitHit,
    RetryError,
)

# ── 用户钩子三件套 ───────────────────────────────────────────────────
from .hooks import job_ref, progress_hook, slice_list

# ── 错误分类 ─────────────────────────────────────────────────────────
from .engine.errorclass import (
    ERR_COMMIT_FAILURE,
    ERR_DEADLOCK_GAP,
    ERR_DEPENDENCY_DEADLOCK,
    ERR_DISPATCH_FAILURE,
    ERR_IPC_WRITE_DEGRADED_PREFIX,
    ERR_JOB_DEPENDENCY,
    ERR_MALFORMED_JOB,
    ERR_MAX_RETRIES,
    ERR_NO_HANDLER,
    ERR_NO_IPC_RESULT,
    ERR_PAYLOAD_VALIDATION,
    ERR_PROCESS_CRASH_PREFIX,
    ERR_PROCESS_SIGNAL_DEATH,
    ERR_RESOURCE_DEADLOCK,
    ERR_TIMEOUT_PREFIX,
    ErrorCategory,
    ErrorClassification,
    ErrorClassifier,
    ValidationErrorItem,
    ValidationResult,
    classify_error_type,
    classify_exception,
    is_transient_exception,
    validate_payload_errors,
)

# ── 资源体系 ─────────────────────────────────────────────────────────
from .engine.resource import (
    CapacityResource,
    RateLimitResource,
    Resource,
)

# ── 失败档案条目 ─────────────────────────────────────────────────────
from .engine.store import FailureEntry

# ── 策略 seam：准入（rerun）与重入队（节奏）─────────────────────────
from .engine.admission import (
    ImmediateRequeuePolicy,
    RequeuePolicy,
    RerunPolicy,
)

# ── 策略 seam：调度排序 ──────────────────────────────────────────────
from .engine.scheduler import FifoOrderingPolicy, OrderingPolicy

# ── 运行值对象：run()/stop() 调用边界直接遭遇的形状 ──────────────────
from .engine.types import (
    AttemptFinish,
    ExitReason,
    RunSummary,
    StopMode,
    TaskStats,
)

# ── 持久化后端 ───────────────────────────────────────────────────────
from .backend import InMemoryStateBackend, SQLiteStateBackend

# ── 单射编码族 ───────────────────────────────────────────────────────
from .utils.encoding import (
    content_fingerprint,
    encode_content_id,
    encode_identifier,
    encode_job_component,
    percent_encode,
    safe_uid_filename,
)

# ── JSON 序列化统一出口 ──────────────────────────────────────────────
from .utils import jsonutil

__all__ = [
    # 门面
    "TaskLite",
    # 模型层
    "AttemptRecord",
    "Job",
    "JobContext",
    "JobRuntimeState",
    "Task",
    "TaskRegistry",
    # 异常族
    "FatalError",
    "PipelineError",
    "RateLimitHit",
    "RetryError",
    # 钩子三件套
    "job_ref",
    "progress_hook",
    "slice_list",
    # 错误分类
    "ErrorCategory",
    "ErrorClassification",
    "ErrorClassifier",
    "ValidationErrorItem",
    "ValidationResult",
    "classify_error_type",
    "classify_exception",
    "is_transient_exception",
    "validate_payload_errors",
    "ERR_DEPENDENCY_DEADLOCK",
    "ERR_JOB_DEPENDENCY",
    "ERR_PAYLOAD_VALIDATION",
    "ERR_MAX_RETRIES",
    "ERR_NO_HANDLER",
    "ERR_RESOURCE_DEADLOCK",
    "ERR_MALFORMED_JOB",
    "ERR_COMMIT_FAILURE",
    "ERR_DISPATCH_FAILURE",
    "ERR_DEADLOCK_GAP",
    "ERR_TIMEOUT_PREFIX",
    "ERR_PROCESS_CRASH_PREFIX",
    "ERR_NO_IPC_RESULT",
    "ERR_PROCESS_SIGNAL_DEATH",
    "ERR_IPC_WRITE_DEGRADED_PREFIX",
    # 资源体系
    "CapacityResource",
    "RateLimitResource",
    "Resource",
    # 失败档案
    "FailureEntry",
    # 策略 seam
    "ImmediateRequeuePolicy",
    "RequeuePolicy",
    "RerunPolicy",
    "FifoOrderingPolicy",
    "OrderingPolicy",
    # 运行值对象
    "AttemptFinish",
    "ExitReason",
    "RunSummary",
    "StopMode",
    "TaskStats",
    # 持久化后端
    "InMemoryStateBackend",
    "SQLiteStateBackend",
    # 单射编码族
    "content_fingerprint",
    "encode_content_id",
    "encode_identifier",
    "encode_job_component",
    "percent_encode",
    "safe_uid_filename",
    # 序列化
    "jsonutil",
]
