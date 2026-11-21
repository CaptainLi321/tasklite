"""单元测试显式使用的轻量进程替身委托模块。

真实实现已收编至 ``tasklite.testing.in_process_workers``，本模块仅保留兼容委托。
"""

from __future__ import annotations

from tasklite.engine.launcher import InProcessWorkerLauncher
from tasklite.pipeline import TaskLite
from tasklite.testing import (
    _install_launcher,
    in_process_workers,
)

__all__ = [
    "InProcessWorkerLauncher",
    "in_process_workers",
    "install_fake_worker_launcher",
]


def install_fake_worker_launcher(pipeline: TaskLite, process_class: type) -> None:
    """为单个 TaskLite 实例显式安装进程替身启动器（委托给 tasklite.testing）。"""
    _install_launcher(pipeline, process_class)
