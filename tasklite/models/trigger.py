"""主进程触发条件的只读判定上下文。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .job import Job


@dataclass(frozen=True)
class TriggerContext:
    """隔离于运行态的 Job 快照与成功历史视图。"""

    job: Job
    has_succeeded: bool
    has_failed: bool
    success_metadata: Mapping[str, Any] | None
    last_succeeded_at_us: int | None
