"""EngineRuntime 的静态装配配置。

RunConfig 是调优参数默认值的唯一解析点：调用方一律透传 Optional 原始值，
经 ``RunConfig.resolve()`` / ``resolve_tuning()`` 规范化为最终类型
（None → 常量默认），此后任何模块不得再做二次默认值判断。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Mapping, Optional, Sequence, Tuple, Union

from .governor import DEADLOCK_GAP_MAX_ROUNDS, DEP_GRACE_SECONDS, DeadlockGovernor
from .policy import ExecutionPolicy
from .resource import ResourceManager
from .store import COMMIT_FAILURE_DLQ_THRESHOLD
from ..backend.base import AbstractStateBackend
from ..taxonomy import ErrorTaxonomy
from .types import HandlerEntry

if False:  # TYPE_CHECKING 等价（避免运行时环形导入）
    from .channel import ExecutionChannel


@dataclass(frozen=True)
class Tuning:
    """三参数调优标量的已解析形态（governor 构造等前置场景使用）。"""
    dep_grace_seconds: float
    commit_failure_dlq_threshold: int
    deadlock_gap_max_rounds: int


def resolve_tuning(
    *,
    dep_grace_seconds: Optional[float] = None,
    commit_failure_dlq_threshold: Optional[int] = None,
    deadlock_gap_max_rounds: Optional[int] = None,
) -> Tuning:
    """调优标量规范化（与 RunConfig.resolve 共用同一真相源）。

    合法性域：dep_grace_seconds 有限正值（0/负 → 依赖宽限立即判死；
    NaN 比较恒 False / inf → 宽限永不裁决活锁）；两个轮次阈值为正整数
    （0 → 提交失败或无根因零轮即升级整队列 DLQ，丧失恢复窗口）。
    """
    grace = (
        float(dep_grace_seconds)
        if dep_grace_seconds is not None else DEP_GRACE_SECONDS
    )
    # 超大 int 转 float 溢出抛 OverflowError（ArithmeticError 子类），
    # 收敛为 ValueError 与其余非法值同路 fail-loud
    try:
        finite_grace = math.isfinite(grace)
    except OverflowError:
        finite_grace = False
    if not finite_grace or grace <= 0:
        raise ValueError(
            f"dep_grace_seconds must be a finite number > 0, got {dep_grace_seconds!r}"
        )
    threshold = (
        int(commit_failure_dlq_threshold)
        if commit_failure_dlq_threshold is not None else COMMIT_FAILURE_DLQ_THRESHOLD
    )
    if threshold < 1:
        raise ValueError(
            f"commit_failure_dlq_threshold must be an int >= 1, "
            f"got {commit_failure_dlq_threshold!r}"
        )
    gap_rounds = (
        int(deadlock_gap_max_rounds)
        if deadlock_gap_max_rounds is not None else DEADLOCK_GAP_MAX_ROUNDS
    )
    if gap_rounds < 1:
        raise ValueError(
            f"deadlock_gap_max_rounds must be an int >= 1, "
            f"got {deadlock_gap_max_rounds!r}"
        )
    return Tuning(
        dep_grace_seconds=grace,
        commit_failure_dlq_threshold=threshold,
        deadlock_gap_max_rounds=gap_rounds,
    )


@dataclass(frozen=True)
class RunConfig:
    """EngineRuntime 的静态装配配置规范（冻结引用而非拷贝）。

    装配件在 TaskLite 构造期即建并按引用共享（handlers / discovery_rerun
    的装配期变更经 run 期守卫约束）；StateStore 由 EngineRuntime 构造期
    创建（其 on_job_completed 回调绑定 RunSession，随会话存活）。
    """
    # ── 装配件 ──
    name: str
    ipc_dir: str
    backend: AbstractStateBackend
    resources: ResourceManager
    handlers: Mapping[str, HandlerEntry]
    channel: "ExecutionChannel"
    taxonomy: ErrorTaxonomy
    discovery_rerun: Mapping[str, str]
    governor: DeadlockGovernor
    policy: ExecutionPolicy
    # ── 调优标量（默认值唯一落点）──
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
        backend: AbstractStateBackend,
        resources: ResourceManager,
        handlers: Mapping[str, HandlerEntry],
        channel: "ExecutionChannel",
        taxonomy: ErrorTaxonomy,
        discovery_rerun: Mapping[str, str],
        governor: DeadlockGovernor,
        policy: ExecutionPolicy,
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
        tuning = resolve_tuning(
            dep_grace_seconds=dep_grace_seconds,
            commit_failure_dlq_threshold=commit_failure_dlq_threshold,
            deadlock_gap_max_rounds=deadlock_gap_max_rounds,
        )
        return cls(
            name=name,
            ipc_dir=ipc_dir,
            backend=backend,
            resources=resources,
            handlers=handlers,
            channel=channel,
            taxonomy=taxonomy,
            discovery_rerun=discovery_rerun,
            governor=governor,
            policy=policy,
            output_root=output_root,
            strict_picklable=strict_picklable,
            dep_grace_seconds=tuning.dep_grace_seconds,
            commit_failure_dlq_threshold=tuning.commit_failure_dlq_threshold,
            deadlock_gap_max_rounds=tuning.deadlock_gap_max_rounds,
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
