"""v2 模型层：Task 规格 / Job 逻辑实例 / Attempt 执行轨迹 / 管线状态与执行上下文。

分层红线：本包严禁 import ``v2/engine/``（IPC 声明读写一律下沉
``v2/utils/ipc.py``），亦不得依赖 v1 旧树任何模块。
"""
from .task import Task, TaskRegistry

__all__ = ["Task", "TaskRegistry"]
