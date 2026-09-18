"""三机器窄依赖共享装配库（测试侧唯一构造入口）。

unit 层的 make_env/_make_machine/make_orchestrator 各自手搓一份装配，
completion 版还以 8 字段 SimpleNamespace 复刻「Context 整袋」形态。
本模块把装配收敛为显式 NamedTuple：字段即依赖清单，机器互持真实引用，
测试按字段取用而非穿透位置元组。
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, NamedTuple, Optional
from unittest.mock import MagicMock

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.channel import ExecutionChannel
from tasklite.engine.completion import CompletionMachine
from tasklite.engine.dispatch import DispatchMachine
from tasklite.engine.inflight import InFlightTracker
from tasklite.engine.policy import ExecutionPolicy
from tasklite.engine.recovery import RecoveryOrchestrator
from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.engine.scheduler import JobScheduler
from tasklite.engine.session import RunSession
from tasklite.engine.store import StateStore
from tasklite.engine.types import TaskStats
from tasklite.models.job import WORKER_RESOURCE
from tasklite.models.state import PipelineState
from tasklite.taxonomy import ErrorTaxonomy

__all__ = [
    "MachineEnv",
    "make_machines",
    "make_recovery_orchestrator",
    "FakeChannel",
    "FakeInFlight",
]


class FakeChannel:
    """RecoveryOrchestrator 消费面的具名替身——三个读口，字段即配置。

    遵循「具名 spec 契约、禁按位置反解」：方法名即消费面契约，构造
    参数即返回值配置，不靠 MagicMock 的运行时属性组装。
    """

    def __init__(self, *, active_signals=(), all_signals=(), drain_all_error=None):
        self.active_signals = list(active_signals)
        self.all_signals = list(all_signals)
        self.drain_all_error = drain_all_error

    def drain_active_signals(self, uids):
        return list(self.active_signals)

    def drain_all_signals(self):
        if self.drain_all_error is not None:
            raise self.drain_all_error
        return list(self.all_signals)

    def abort_in_flight(self, handles):
        from tasklite.engine.channel import AbortOutcome
        return AbortOutcome(completed=[], cancelled=[])


class FakeInFlight:
    """apply_pending_signals 消费面替身（只读 active_uids）。"""

    def __init__(self, uids=()):
        self._uids = list(uids)

    def active_uids(self):
        return list(self._uids)


class MachineEnv(NamedTuple):
    """显式依赖清单——字段即装配产物，测试按名取用。"""

    backend: InMemoryStateBackend
    store: StateStore
    stats: TaskStats
    session: RunSession
    in_flight: InFlightTracker
    channel: ExecutionChannel
    resources: ResourceManager
    completion: CompletionMachine
    dispatch: DispatchMachine
    recovery: RecoveryOrchestrator
    ipc_dir: str
    output_root: str


def make_machines(
    tmp_path,
    *,
    capacity: float = 2.0,
    extra_resources: Optional[Dict] = None,
    handlers: Optional[Dict] = None,
    threshold: int = 3,
) -> MachineEnv:
    """显式装配三机器的窄依赖集合（全部真实互持，零 mock）。

    worker 槽位资源固定 ``__workers__``（容量 capacity），
    ``extra_resources`` 追加业务资源；``threshold`` 为 commit 失败
    3-strike 阈值。
    """
    ipc_dir = str(tmp_path / "ipc")
    Path(ipc_dir).mkdir(parents=True, exist_ok=True)
    resources_map = {WORKER_RESOURCE: CapacityResource(WORKER_RESOURCE, capacity)}
    if extra_resources:
        resources_map.update(extra_resources)
    resource_mgr = ResourceManager(resources_map)

    backend = InMemoryStateBackend()
    stats = TaskStats()
    policy = ExecutionPolicy()
    store = StateStore(
        backend,
        state=PipelineState({}, {}, {}, []),
        taxonomy=ErrorTaxonomy(),
        stats=stats,
        policy=policy,
        commit_failure_dlq_threshold=threshold,
    )
    session = RunSession()
    session.run_id = "test_run"
    in_flight = InFlightTracker()
    channel = ExecutionChannel(ipc_dir)

    completion = CompletionMachine(
        store=store, policy=policy, channel=channel,
        resources=resource_mgr, in_flight=in_flight, session=session,
    )
    dispatch = DispatchMachine(
        store=store,
        scheduler=JobScheduler(resource_mgr),
        policy=policy,
        resources=resource_mgr,
        channel=channel,
        in_flight=in_flight,
        session=session,
        completion=completion,
        handlers=handlers if handlers is not None else {},
        taxonomy=ErrorTaxonomy(),
        output_root=str(tmp_path / "out"),
        ipc_dir=ipc_dir,
        commit_failure_dlq_threshold=threshold,
    )
    recovery = RecoveryOrchestrator(
        store=store, channel=channel, resources=resource_mgr,
        in_flight=in_flight, policy=policy, completion=completion,
    )
    return MachineEnv(
        backend=backend, store=store, stats=stats, session=session,
        in_flight=in_flight, channel=channel, resources=resource_mgr,
        completion=completion, dispatch=dispatch, recovery=recovery,
        ipc_dir=ipc_dir, output_root=str(tmp_path / "out"),
    )


def make_recovery_orchestrator(backend=None, *, state=None, resource_mgr=None,
                               channel=None, in_flight=None, completion=None,
                               policy=None, sqlite_path=None):
    """RecoveryOrchestrator 窄依赖装配：未提供的协作方落 MagicMock
    （repair 纯逻辑单测不触真实 IPC/结算面）。"""
    if backend is None:
        backend = SQLiteStateBackend(sqlite_path) if sqlite_path else InMemoryStateBackend()
    return RecoveryOrchestrator(
        store=StateStore(backend, state=state or PipelineState({}, {}, {}, [])),
        channel=channel if channel is not None else MagicMock(),
        resources=resource_mgr or ResourceManager(),
        in_flight=in_flight if in_flight is not None else MagicMock(),
        policy=policy or ExecutionPolicy(),
        completion=completion if completion is not None else MagicMock(),
    )
