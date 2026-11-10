"""TaskLite：Task/Job/Attempt 三层模型的任务编排引擎（ADR-0004 / ADR-0005 / ADR-0011）。

模型：Task 为进程内注册的静态规格模板；Job 为具有稳定身份的逻辑实例
（uid = task_type::job_id）；TaskResult 为 handler 唯一合法返回类型；
Attempt 为 append-only 的执行轨迹。

公开面：根包仅导出 ADR-0011 契约已定的核心 10 个名称：
TaskLite, Task, Job, JobContext, TaskResult, FileSpec,
StateSnapshot, TaskLiteError, make_job_uid, parse_job_uid。
内置资源从 tasklite.resource 导入（CapacityResource, IntervalResource）；
运行排除规则从 tasklite.selection 导入（Exclude）。
不在根包重复导出。``__version__`` 为版本单一事实源（pyproject 动态读取点）。

分层单向依赖红线：``tasklite/models/`` 严禁 import
``tasklite/engine/``（IPC 声明读写一律下沉 ``tasklite/utils/ipc.py``），
``tasklite/utils/`` 严禁 import ``tasklite/wrappers/``，核心层
（engine/backend/models/utils）严禁依赖 ``tasklite/contrib/``。
"""
from __future__ import annotations

__version__ = "2.2.1"

# ── 门面与执行核心 ────────────────────────────────────────────────────
from .pipeline import TaskLite

# ── 模型层：规格 / 实例 / 结果 / 上下文 / 文件 ────────────────────────
from .models.task import Task
from .models.job import Job
from .models.context import JobContext
from .models.result import TaskResult
from .models.artifact import FileSpec

# ── 只读观察快照 ───────────────────────────────────────────────────────
from .snapshot import StateSnapshot

# ── 异常族 ───────────────────────────────────────────────────────────
from .exceptions import TaskLiteError

# ── 身份工具 ─────────────────────────────────────────────────────────
from .utils.encoding import make_job_uid, parse_job_uid

__all__ = [
    "TaskLite",
    "Task",
    "Job",
    "JobContext",
    "TaskResult",
    "FileSpec",
    "StateSnapshot",
    "TaskLiteError",
    "make_job_uid",
    "parse_job_uid",
]
