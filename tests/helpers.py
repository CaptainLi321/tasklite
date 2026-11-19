"""单元测试显式使用的轻量进程替身委托模块。

真实实现已收编至 ``tasklite.testing.in_process_workers``，本模块仅保留兼容委托。
"""

from __future__ import annotations

from tasklite.pipeline import TaskLite
from tasklite.testing import (
    _InProcessSpawnContext as FakeSpawnContext,
    _install_process_context,
    in_process_workers,
)

__all__ = [
    "FakeSpawnContext",
    "in_process_workers",
    "install_fake_process_context",
]


def install_fake_process_context(pipeline: TaskLite, process_class: type) -> None:
    """为单个 TaskLite 实例显式安装进程替身（委托给 tasklite.testing）。"""
    _install_process_context(pipeline, process_class)
