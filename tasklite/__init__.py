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
（engine/backend/models/utils）严禁依赖 ``tasklite/wrappers/``。
"""
from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

__version__ = "3.0.0.dev0"

if TYPE_CHECKING:
    from .exceptions import TaskLiteError
    from .models.artifact import FileSpec
    from .models.context import JobContext
    from .models.job import Job
    from .models.result import TaskResult
    from .models.task import Task
    from .pipeline import TaskLite
    from .snapshot import StateSnapshot
    from .utils.encoding import make_job_uid, parse_job_uid

# 根包延迟导出表（PEP 562）：避免子进程引导导入 ``tasklite.engine._worker_bootstrap``
# 时连带拉起父进程门面、快照与持久化树。
_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "TaskLite": (".pipeline", "TaskLite"),
    "Task": (".models.task", "Task"),
    "Job": (".models.job", "Job"),
    "JobContext": (".models.context", "JobContext"),
    "TaskResult": (".models.result", "TaskResult"),
    "FileSpec": (".models.artifact", "FileSpec"),
    "StateSnapshot": (".snapshot", "StateSnapshot"),
    "TaskLiteError": (".exceptions", "TaskLiteError"),
    "make_job_uid": (".utils.encoding", "make_job_uid"),
    "parse_job_uid": (".utils.encoding", "parse_job_uid"),
}

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


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    value = getattr(importlib.import_module(module_name, __name__), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
