"""EngineRuntime 的静态装配配置。

RunConfig 是调优参数默认值的唯一解析点：调用方一律透传 Optional 原始值，
经 ``RunConfig.resolve()`` 规范化为最终类型（None → 常量默认），此后任何
模块不得再做二次默认值判断。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional, Sequence, Tuple, Union

from .governor import DEADLOCK_GAP_MAX_ROUNDS, DEP_GRACE_SECONDS
from .store import COMMIT_FAILURE_DLQ_THRESHOLD


@dataclass(frozen=True)
class RunConfig:
    """EngineRuntime 的静态装配配置规范（冻结引用而非拷贝）。"""
    name: str
    ipc_dir: str
    output_root: Optional[Union[Path, Sequence[Path]]] = None
    strict_picklable: bool = False
    dep_grace_seconds: float = DEP_GRACE_SECONDS
    commit_failure_dlq_threshold: int = COMMIT_FAILURE_DLQ_THRESHOLD
    deadlock_gap_max_rounds: int = DEADLOCK_GAP_MAX_ROUNDS
    fatal_exceptions: Optional[Tuple[type, ...]] = None
    transient_exceptions: Optional[Tuple[type, ...]] = None
    on_run_start: Optional[Callable[[], None]] = None
    on_run_end: Optional[Callable[[str], None]] = None
    on_job_completed: Optional[Callable[[str, Dict, bool, bool], None]] = None

    @classmethod
    def resolve(
        cls,
        *,
        name: str,
        ipc_dir: str,
        output_root: Optional[Union[Path, Sequence[Path]]] = None,
        strict_picklable: bool = False,
        dep_grace_seconds: Optional[float] = None,
        commit_failure_dlq_threshold: Optional[int] = None,
        deadlock_gap_max_rounds: Optional[int] = None,
        fatal_exceptions: Optional[Tuple[type, ...]] = None,
        transient_exceptions: Optional[Tuple[type, ...]] = None,
        on_run_start: Optional[Callable[[], None]] = None,
        on_run_end: Optional[Callable[[str], None]] = None,
        on_job_completed: Optional[Callable[[str, Dict, bool, bool], None]] = None,
    ) -> "RunConfig":
        """唯一规范化入口：Optional 原始值 → 常量默认 + 最终类型。"""
        return cls(
            name=name,
            ipc_dir=ipc_dir,
            output_root=output_root,
            strict_picklable=strict_picklable,
            dep_grace_seconds=(
                float(dep_grace_seconds)
                if dep_grace_seconds is not None else DEP_GRACE_SECONDS
            ),
            commit_failure_dlq_threshold=(
                int(commit_failure_dlq_threshold)
                if commit_failure_dlq_threshold is not None else COMMIT_FAILURE_DLQ_THRESHOLD
            ),
            deadlock_gap_max_rounds=(
                int(deadlock_gap_max_rounds)
                if deadlock_gap_max_rounds is not None else DEADLOCK_GAP_MAX_ROUNDS
            ),
            fatal_exceptions=(
                tuple(fatal_exceptions)
                if fatal_exceptions is not None else None
            ),
            transient_exceptions=(
                tuple(transient_exceptions)
                if transient_exceptions is not None else None
            ),
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            on_job_completed=on_job_completed,
        )
