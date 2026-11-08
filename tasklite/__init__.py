"""TaskLite：Task/Job/Attempt 三层模型的任务编排引擎（ADR-0004 / ADR-0005 / ADR-0011）。

模型：Task 为进程内注册的静态规格模板；Job 为一次有界激活的逻辑实例
（uid = task_type::job_id）；TaskResult 为 handler 唯一合法返回类型；
Attempt 为 append-only 的执行轨迹。编码族统一 ``encode_*`` 可逆单射转义；
失败终态集合称「失败档案 failed」；重试节奏由 RequeuePolicy seam 收敛，
核心不含退避与排序计算。

公开面：TaskLite 门面、模型层、公开契约异常、单射编码族、资源体系、失败档案
条目、用户钩子三件套、准入与重入队策略 seam、排序策略 seam、执行选择
规则值对象、持久化后端与运行摘要值对象，均经本文件集中导出
（``from tasklite import ...`` 是唯一承诺稳定的导入面）。``__version__``
为版本单一事实源（pyproject 动态读取点）。

分层单向依赖红线：``tasklite/models/`` 严禁 import
``tasklite/engine/``（IPC 声明读写一律下沉 ``tasklite/utils/ipc.py``），
``tasklite/utils/`` 严禁 import ``tasklite/wrappers/``，核心层
（engine/backend/models/utils）严禁依赖 ``tasklite/contrib/``。

设计契约与命名映射见 docs/adr/0004-v2-parallel-rebuild.md；
上位裁决见 docs/adr/0005-v2-promotion.md。
"""
from __future__ import annotations

__version__ = "2.2.1"

# ── 门面（六步调用契约的唯一入口）────────────────────────────────────
from .pipeline import TaskLite

# ── 执行选择（pipeline 级规则值对象与规则集）─────────────────────────
from .selection import Exclude, Selection, Skip

# ── 模型层：规格 / 实例 / 结果 / 轨迹 / 上下文 ────────────────────────
from .models.artifact import FileSpec
from .models.attempt import AttemptRecord
from .models.context import JobContext
from .models.result import TaskResult
from .models.trigger import TriggerContext
from .models.job import Job, JobRuntimeState
from .models.task import Task, TaskRegistry, validate_resource_amounts

# ── 异常族 ───────────────────────────────────────────────────────────
from .exceptions import (
    TaskLiteError,
)

# ── 用户钩子 ─────────────────────────────────────────────────────────
from .hooks import job_ref, slice_list

# ── 资源体系 ─────────────────────────────────────────────────────────
from .engine.resource import (
    CapacityResource,
    RateLimitResource,
    Resource,
)

# ── 失败档案条目与挂起条目 ───────────────────────────────────────────
from .engine.ops import SuspendEntry
from .engine.store import FailureEntry

# ── 策略 seam：准入（trigger）与重入队（节奏）─────────────────────────
from .engine.admission import (
    ImmediateRequeuePolicy,
    RequeuePlan,
    RequeuePolicy,
    TriggerRegistry,
)

# ── 策略 seam：调度排序 ──────────────────────────────────────────────
from .engine.scheduler import FifoOrderingPolicy, OrderingPolicy

# ── 运行值对象：run()/stop() 调用边界直接遭遇的形状 ──────────────────
from .engine.types import (
    AttemptOutcome,
    ExitReason,
    StopMode,
)

# ── 持久化后端 ───────────────────────────────────────────────────────
from .backend import InMemoryStateBackend, SQLiteStateBackend

# ── 单射编码族 ───────────────────────────────────────────────────────
from .utils.encoding import (
    content_fingerprint,
    encode_content_id,
    encode_identifier,
    encode_job_component,
    make_job_uid,
    parse_job_uid,
    percent_encode,
)

# ── JSON 序列化统一出口 ──────────────────────────────────────────────
from .utils import jsonutil

__all__ = [
    # 门面
    "TaskLite",
    # 执行选择
    "Exclude",
    "Selection",
    "Skip",
    # 模型层
    "AttemptRecord",
    "FileSpec",
    "Job",
    "JobContext",
    "TaskResult",
    "TriggerContext",
    "JobRuntimeState",
    "Task",
    "TaskRegistry",
    "validate_resource_amounts",
    # 异常族
    "TaskLiteError",
    # 钩子
    "job_ref",
    "slice_list",
    # 资源体系
    "CapacityResource",
    "RateLimitResource",
    "Resource",
    # 失败档案与挂起
    "FailureEntry",
    "SuspendEntry",
    # 策略 seam
    "ImmediateRequeuePolicy",
    "RequeuePlan",
    "RequeuePolicy",
    "TriggerRegistry",
    "FifoOrderingPolicy",
    "OrderingPolicy",
    # 运行值对象
    "AttemptOutcome",
    "ExitReason",
    "StopMode",
    # 持久化后端
    "InMemoryStateBackend",
    "SQLiteStateBackend",
    # 单射编码族
    "content_fingerprint",
    "encode_content_id",
    "encode_identifier",
    "encode_job_component",
    "make_job_uid",
    "parse_job_uid",
    "percent_encode",
    # 序列化
    "jsonutil",
]
