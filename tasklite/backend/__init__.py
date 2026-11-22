"""持久层：抽象契约与 SQLite / 内存双实现（ADR-0004）。

分层红线：本包属核心层，严禁 import ``tasklite/engine/`` 与
``tasklite/wrappers/``、严禁依赖 ``tasklite/contrib/``；可依赖
``tasklite/models/`` 与 ``tasklite/utils/``。
"""
from .base import (
    AbstractStateBackend,
    FailureEntry,
    GraphEditCommit,
    RunningAttemptRecovery,
    validate_graph_edit_commit,
    validate_queue_replacement,
    validate_recovery_attempt_state,
)
from .memory import InMemoryStateBackend
from .sqlite_backend import (
    SQLiteStateBackend,
    TARGET_SCHEMA_VERSION,
    open_existing_sqlite_readonly,
    verify_existing_sqlite_database,
)

__all__ = [
    "AbstractStateBackend",
    "FailureEntry",
    "GraphEditCommit",
    "InMemoryStateBackend",
    "RunningAttemptRecovery",
    "SQLiteStateBackend",
    "TARGET_SCHEMA_VERSION",
    "open_existing_sqlite_readonly",
    "validate_graph_edit_commit",
    "validate_queue_replacement",
    "validate_recovery_attempt_state",
    "verify_existing_sqlite_database",
]
