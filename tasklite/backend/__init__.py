"""持久层：抽象契约与 SQLite / 内存双实现（ADR-0004）。

分层红线：本包属核心层，严禁 import ``tasklite/engine/`` 与
``tasklite/wrappers/``、严禁依赖 ``tasklite/contrib/``；可依赖
``tasklite/models/`` 与 ``tasklite/utils/``。
"""
from .base import (
    AbstractStateBackend,
    validate_attempt_dispatch,
    validate_attempt_finish,
    validate_queue_replacement,
)
from .memory import InMemoryStateBackend
from .sqlite_backend import SQLiteStateBackend

__all__ = [
    "AbstractStateBackend",
    "InMemoryStateBackend",
    "SQLiteStateBackend",
    "validate_attempt_dispatch",
    "validate_attempt_finish",
    "validate_queue_replacement",
]
