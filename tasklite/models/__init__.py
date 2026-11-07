"""模型层：Task 规格 / Job 逻辑实例 / TaskResult 执行结果 / Attempt 执行轨迹 / 管线状态与执行上下文。

子模块路径：
task（Task 规格与注册表）、job（Job 实例与运行期边带状态）、
result（TaskResult 显式返回结果）、attempt（append-only 执行轨迹）、
state（PipelineState 六集合状态）、context（JobContext 执行上下文）。

分层红线：本包严禁 import ``tasklite/engine/``（IPC 声明读写一律下沉
``tasklite/utils/ipc.py``）。
"""

from .result import TaskResult, TaskResultStatus

__all__ = [
    "TaskResult",
    "TaskResultStatus",
]

