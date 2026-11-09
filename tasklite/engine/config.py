"""EngineRuntime 的静态装配配置。

RunConfig 是调优参数默认值的唯一解析点：调用方一律透传可空（X | None）
原始值，经 ``RunConfig.resolve()`` / ``resolve_tuning()`` 规范化为最终
类型（None → 常量默认），此后任何模块不得再做二次默认值判断。

零退避军规：本模块（连同全部 v2 调优面）不存在任何退避类配置项——
重试节奏唯一经 RequeuePolicy seam 表达（默认立即重入队）。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence, TYPE_CHECKING

from .admission import ImmediateRequeuePolicy, RequeuePolicy, TriggerRegistry
from .errorclass import ErrorClassifier
from .governor import DEADLOCK_GAP_MAX_ROUNDS, DeadlockGovernor
from .resource import ResourceManager
from .scheduler import FifoOrderingPolicy, OrderingPolicy
from .store import COMMIT_FAILURE_THRESHOLD
from ..backend.base import AbstractStateBackend
from ..models.task import TaskRegistry

if TYPE_CHECKING:  # 仅静态分析导入，避免运行时环形导入
    from .channel import ExecutionChannel


@dataclass(frozen=True)
class Tuning:
    """两参数调优标量的已解析形态（governor 构造等前置场景使用）。"""
    commit_failure_threshold: int
    deadlock_gap_max_rounds: int


def _positive_int_rounds(value: int | None, default: int, name: str) -> int:
    """轮次阈值规范化：仅真 int（bool/浮点/数字字符串 TypeError）且 >= 1。

    浮点截断（如 1.9 → 1）会静默落入「零恢复窗口」致命域，字符串强转
    则让类型漂移无告警穿透，一律 fail-loud。
    """
    if value is None:
        value = default
    elif not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(
            f"{name} must be an int, got {type(value).__name__} ({value!r})"
        )
    if value < 1:
        raise ValueError(f"{name} must be an int >= 1, got {value!r}")
    return value


def resolve_tuning(
    *,
    commit_failure_threshold: int | None = None,
    deadlock_gap_max_rounds: int | None = None,
) -> Tuning:
    """调优标量规范化（与 RunConfig.resolve 共用同一真相源）。

    两个阈值为正整数（仅真 int——bool、浮点与数字字符串一律 TypeError：
    浮点截断（如 1.9 → 1）会静默落入「零恢复窗口」致命域，字符串强转
    则让类型漂移无告警穿透）。
    """
    threshold = _positive_int_rounds(
        commit_failure_threshold, COMMIT_FAILURE_THRESHOLD,
        "commit_failure_threshold",
    )
    gap_rounds = _positive_int_rounds(
        deadlock_gap_max_rounds, DEADLOCK_GAP_MAX_ROUNDS,
        "deadlock_gap_max_rounds",
    )
    return Tuning(
        commit_failure_threshold=threshold,
        deadlock_gap_max_rounds=gap_rounds,
    )


@dataclass(frozen=True)
class RunConfig:
    """EngineRuntime 的静态装配配置规范（冻结引用而非拷贝）。

    装配件在门面构造期即建并按引用共享（Task 注册表与 trigger 策略的
    装配期变更经 run 期守卫约束）；StateStore 由 EngineRuntime 构造期
    创建（其 on_attempt_finished 回调绑定 RunSession，随会话存活）。
    """
    # ── 装配件 ──
    backend: AbstractStateBackend
    resources: ResourceManager
    tasks: TaskRegistry
    channel: "ExecutionChannel"
    classifier: ErrorClassifier
    governor: DeadlockGovernor
    trigger_registry: TriggerRegistry
    requeue_policy: RequeuePolicy
    ordering_policy: OrderingPolicy
    name: str = "tasklite"
    ipc_dir: str | None = None
    output_roots: Path | Sequence[Path] | None = None
    protected_roots: tuple[Path, ...] = ()
    strict_picklable: bool = True
    commit_failure_threshold: int = COMMIT_FAILURE_THRESHOLD
    deadlock_gap_max_rounds: int = DEADLOCK_GAP_MAX_ROUNDS

    @classmethod
    def resolve(
        cls,
        *,
        backend: AbstractStateBackend,
        resources: ResourceManager,
        tasks: TaskRegistry,
        channel: "ExecutionChannel",
        classifier: ErrorClassifier,
        governor: DeadlockGovernor,
        trigger_registry: TriggerRegistry,
        name: str = "tasklite",
        ipc_dir: str | None = None,
        requeue_policy: RequeuePolicy | None = None,
        ordering_policy: OrderingPolicy | None = None,
        output_roots: Path | Sequence[Path] | None = None,
        protected_roots: tuple[Path, ...] = (),
        strict_picklable: bool = True,
        commit_failure_threshold: int | None = None,
        deadlock_gap_max_rounds: int | None = None,
    ) -> "RunConfig":
        """唯一规范化入口：可空原始值 → 常量默认 + 最终类型。"""
        tuning = resolve_tuning(
            commit_failure_threshold=commit_failure_threshold,
            deadlock_gap_max_rounds=deadlock_gap_max_rounds,
        )
        return cls(
            name=name,
            ipc_dir=ipc_dir,
            protected_roots=protected_roots,
            backend=backend,
            resources=resources,
            tasks=tasks,
            channel=channel,
            classifier=classifier,
            governor=governor,
            trigger_registry=trigger_registry,
            requeue_policy=(
                requeue_policy if requeue_policy is not None else ImmediateRequeuePolicy()
            ),
            ordering_policy=ordering_policy if ordering_policy is not None else FifoOrderingPolicy(),
            output_roots=output_roots,
            strict_picklable=strict_picklable,
            commit_failure_threshold=tuning.commit_failure_threshold,
            deadlock_gap_max_rounds=tuning.deadlock_gap_max_rounds,
        )


__all__ = [
    "RunConfig",
    "Tuning",
    "resolve_tuning",
]
