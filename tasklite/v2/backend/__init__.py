"""v2 持久层：抽象契约与 SQLite / 内存双实现（ADR-0004）。

分层红线：本包属核心层，严禁 import ``v2/engine/`` 与 ``v2/wrappers/``、
严禁依赖 ``v2/contrib/``；可依赖 ``v2/models/`` 与 ``v2/utils/``，且不得
依赖 v1 旧树任何模块。
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
