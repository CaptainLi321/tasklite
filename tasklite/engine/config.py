"""EngineRuntime 的静态装配配置与内部策略默认值。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, TYPE_CHECKING

from .admission import ImmediateRequeuePolicy, RequeuePolicy, TriggerRegistry
from .governor import DeadlockGovernor
from .resource import ResourceManager
from .scheduler import FifoOrderingPolicy, OrderingPolicy
from ..backend.base import AbstractStateBackend
from ..models.task import TaskRegistry

if TYPE_CHECKING:  # 仅静态分析导入，避免运行时环形导入
    from .channel import ExecutionChannel


@dataclass(frozen=True)
class RunConfig:
    """EngineRuntime 的静态装配配置规范（冻结引用而非拷贝）。

    装配件在门面构造期即建并按引用共享（Task 注册表与 trigger 策略的
    装配期变更经 run 期守卫约束）；StateStore 由 EngineRuntime 构造期
    创建并持有状态事务。
    """
    # ── 装配件 ──
    backend: AbstractStateBackend
    resources: ResourceManager
    tasks: TaskRegistry
    channel: "ExecutionChannel"
    governor: DeadlockGovernor
    trigger_registry: TriggerRegistry
    requeue_policy: RequeuePolicy
    ordering_policy: OrderingPolicy
    name: str = "tasklite"
    ipc_dir: str | None = None
    output_roots: Path | Sequence[Path] | None = None
    protected_roots: tuple[Path, ...] = ()

    @classmethod
    def resolve(
        cls,
        *,
        backend: AbstractStateBackend,
        resources: ResourceManager,
        tasks: TaskRegistry,
        channel: "ExecutionChannel",
        governor: DeadlockGovernor,
        trigger_registry: TriggerRegistry,
        name: str = "tasklite",
        ipc_dir: str | None = None,
        requeue_policy: RequeuePolicy | None = None,
        ordering_policy: OrderingPolicy | None = None,
        output_roots: Path | Sequence[Path] | None = None,
        protected_roots: tuple[Path, ...] = (),
    ) -> "RunConfig":
        """按引用装配依赖，并解析内部策略默认值。"""
        return cls(
            name=name,
            ipc_dir=ipc_dir,
            protected_roots=protected_roots,
            backend=backend,
            resources=resources,
            tasks=tasks,
            channel=channel,
            governor=governor,
            trigger_registry=trigger_registry,
            requeue_policy=(
                requeue_policy if requeue_policy is not None else ImmediateRequeuePolicy()
            ),
            ordering_policy=ordering_policy if ordering_policy is not None else FifoOrderingPolicy(),
            output_roots=output_roots,
        )


__all__ = [
    "RunConfig",
]
