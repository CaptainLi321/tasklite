"""Unit tests for EngineRuntime deep module."""

from __future__ import annotations

import multiprocessing as mp
import time
from pathlib import Path
import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.channel import ExecutionChannel
from tasklite.engine.runtime import (
    EngineRuntime,
    ExecutionOptions,
    ExitReason,
    RunContext,
    RunSummary,
    RuntimeConfig,
    StepOutcome,
    StopMode,
    TaskStats,
    WORKER_RESOURCE,
)
from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.models.context import TaskContext
from tasklite.models.job import Job
from tasklite.models.state import PipelineState
from tasklite.pipeline import TaskLite


def dummy_runtime_handler(job: Job, ctx: TaskContext) -> dict:
    return {"status": "ok", "uid": job.uid}


class TestEngineRuntimeConfiguration:
    """测试 EngineRuntime 构造与静态配置装配。"""

    def test_default_assembly_and_wiring(self, tmp_path):
        ipc_dir = str(tmp_path / "ipc")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)

        config = RuntimeConfig(
            name="test_runtime",
            ipc_dir=ipc_dir,
            output_root=tmp_path,
        )
        backend = InMemoryStateBackend()
        resources = ResourceManager()
        resources[WORKER_RESOURCE] = CapacityResource(WORKER_RESOURCE, 2.0)
        handlers = {}

        runtime = EngineRuntime(
            config=config,
            backend=backend,
            resources=resources,
            handlers=handlers,
            executor=None,
            transient_registry=None,
            discovery_rerun={},
        )

        assert runtime.config.name == "test_runtime"
        assert runtime.backend is backend
        assert runtime.stop_mode == StopMode.NONE
        assert not runtime.is_running
        assert isinstance(runtime.stats, TaskStats)
        assert isinstance(runtime.ctx, RunContext)
        assert isinstance(runtime.channel, ExecutionChannel)


class TestEngineRuntimeStopStateMachine:
    """测试 EngineRuntime 停机状态机单调流转契约。"""

    def test_stop_state_transitions(self, tmp_path):
        ipc_dir = str(tmp_path / "ipc")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)

        config = RuntimeConfig(name="test_stop", ipc_dir=ipc_dir)
        backend = InMemoryStateBackend()
        resources = ResourceManager()
        runtime = EngineRuntime(
            config=config,
            backend=backend,
            resources=resources,
            handlers={},
            executor=None,
            transient_registry=None,
            discovery_rerun={},
        )

        assert runtime.stop_mode == StopMode.NONE

        # 首次 stop(force=False) -> DRAINING
        mode1 = runtime.request_stop(force=False)
        assert mode1 == StopMode.DRAINING
        assert runtime.stop_mode == StopMode.DRAINING

        # 二次 stop(force=False) 升级为 ABORTING
        mode2 = runtime.request_stop(force=False)
        assert mode2 == StopMode.ABORTING
        assert runtime.stop_mode == StopMode.ABORTING

        # 再次 stop 保持 ABORTING（幂等）
        mode3 = runtime.request_stop(force=False)
        assert mode3 == StopMode.ABORTING

    def test_stop_force_direct_aborting(self, tmp_path):
        ipc_dir = str(tmp_path / "ipc")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)

        config = RuntimeConfig(name="test_force", ipc_dir=ipc_dir)
        backend = InMemoryStateBackend()
        resources = ResourceManager()
        runtime = EngineRuntime(
            config=config,
            backend=backend,
            resources=resources,
            handlers={},
            executor=None,
            transient_registry=None,
            discovery_rerun={},
        )

        mode = runtime.request_stop(force=True)
        assert mode == StopMode.ABORTING
        assert runtime.stop_mode == StopMode.ABORTING


class TestEngineRuntimeStepPump:
    """测试 EngineRuntime 确定性单步事件泵 step()。"""

    def test_step_on_empty_state(self, tmp_path):
        ipc_dir = str(tmp_path / "ipc")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)

        config = RuntimeConfig(name="test_step_empty", ipc_dir=ipc_dir)
        backend = InMemoryStateBackend()
        resources = ResourceManager()
        resources[WORKER_RESOURCE] = CapacityResource(WORKER_RESOURCE, 2.0)
        runtime = EngineRuntime(
            config=config,
            backend=backend,
            resources=resources,
            handlers={},
            executor=None,
            transient_registry=None,
            discovery_rerun={},
        )

        outcome = runtime.step()
        assert isinstance(outcome, StepOutcome)
        assert outcome.is_idle is True
        assert outcome.dispatched_count == 0
        assert outcome.completed_count == 0

    def test_step_draining_and_aborting_modes(self, tmp_path):
        ipc_dir = str(tmp_path / "ipc")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)
        config = RuntimeConfig(name="test_step_modes", ipc_dir=ipc_dir)
        backend = InMemoryStateBackend()
        resources = ResourceManager()
        resources[WORKER_RESOURCE] = CapacityResource(WORKER_RESOURCE, 2.0)
        runtime = EngineRuntime(
            config=config,
            backend=backend,
            resources=resources,
            handlers={},
            executor=None,
            transient_registry=None,
            discovery_rerun={},
        )

        runtime.request_stop(force=False)
        outcome_draining = runtime.step()
        assert outcome_draining.exit_reason == "stopped_draining"
        assert outcome_draining.should_terminate is True

        runtime.request_stop(force=True)
        outcome_aborting = runtime.step()
        assert outcome_aborting.exit_reason == "stopped_aborting"
        assert outcome_aborting.should_terminate is True


class TestEngineRuntimeExecutionLifecycle:
    """测试 EngineRuntime 完整生命周期 execute() 与安全网。"""

    def test_execute_empty_queue_completed(self, tmp_path):
        ipc_dir = str(tmp_path / "ipc")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)

        events = []

        def on_start():
            events.append("start")

        def on_end(reason: str):
            events.append(f"end:{reason}")

        config = RuntimeConfig(
            name="test_exec",
            ipc_dir=ipc_dir,
            on_run_start=on_start,
            on_run_end=on_end,
        )
        backend = InMemoryStateBackend()
        resources = ResourceManager()
        resources[WORKER_RESOURCE] = CapacityResource(WORKER_RESOURCE, 2.0)
        runtime = EngineRuntime(
            config=config,
            backend=backend,
            resources=resources,
            handlers={},
            executor=None,
            transient_registry=None,
            discovery_rerun={},
        )

        summary = runtime.execute()
        assert isinstance(summary, RunSummary)
        assert summary.exit_reason == ExitReason.COMPLETED
        assert summary.duration_seconds >= 0
        assert summary.unhandled_exception is None
        assert events == ["start", "end:completed"]

    def test_strict_picklable_unpicklable_handler_fails(self, tmp_path):
        ipc_dir = str(tmp_path / "ipc")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)

        events = []

        config = RuntimeConfig(
            name="test_picklable",
            ipc_dir=ipc_dir,
            strict_picklable=True,
            on_run_end=lambda reason: events.append(reason),
        )
        backend = InMemoryStateBackend()
        resources = ResourceManager()

        # 使用不可 pickle 的 local lambda
        local_fn = lambda j, c: True

        from tasklite.pipeline import HandlerEntry
        handlers = {"unpicklable": HandlerEntry(local_fn, {}, None)}

        runtime = EngineRuntime(
            config=config,
            backend=backend,
            resources=resources,
            handlers=handlers,
            executor=None,
            transient_registry=None,
            discovery_rerun={},
        )

        with pytest.raises(TypeError, match="not picklable"):
            runtime.execute()

        assert "error" in events

    def test_concurrent_run_lock_rejection(self, tmp_path):
        ipc_dir = str(tmp_path / "ipc")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)

        config = RuntimeConfig(name="test_lock", ipc_dir=ipc_dir)
        backend = InMemoryStateBackend()
        resources = ResourceManager()
        resources[WORKER_RESOURCE] = CapacityResource(WORKER_RESOURCE, 2.0)

        runtime1 = EngineRuntime(
            config=config,
            backend=backend,
            resources=resources,
            handlers={},
            executor=None,
            transient_registry=None,
            discovery_rerun={},
        )

        runtime2 = EngineRuntime(
            config=config,
            backend=backend,
            resources=resources,
            handlers={},
            executor=None,
            transient_registry=None,
            discovery_rerun={},
        )

        # 模拟 runtime1 持有锁
        from tasklite.utils.lockfile import try_acquire_lock, release_lock
        lock_fd = try_acquire_lock(ipc_dir, "__pipeline_run__")
        assert lock_fd is not None

        try:
            with pytest.raises(RuntimeError, match="Another run.*in progress"):
                runtime2.execute()
        finally:
            release_lock(lock_fd)


class TestTaskLiteRuntimeFacadeIntegration:
    """测试 TaskLite 宿主门面与 EngineRuntime 架构一致性。"""

    def test_tasklite_exposes_runtime_and_delegates(self, tmp_path):
        p = TaskLite(name="facade_test", state_dir=str(tmp_path), backend="memory")
        assert isinstance(p.runtime, EngineRuntime)
        assert p.runtime.config.name == "facade_test"
        assert p.runtime.ctx is p._ctx
        assert p.runtime.scheduler is p.scheduler
        assert p.store is p._ctx.store
        assert p.channel is p._ctx.channel

        p.register_handler("dummy", dummy_runtime_handler)
        p.enqueue([Job("dummy", "1")])

        # 执行
        p.run()
        assert p.stats["completed"] == 1
        assert "dummy::1" in p.backend.load_wall()

    def test_tasklite_lifecycle_management_guards(self, tmp_path):
        p = TaskLite(name="guard_test", state_dir=str(tmp_path), backend="memory")
        p.register_handler("dummy", dummy_runtime_handler)

        # 模拟运行中状态
        p._runtime._is_running = True
        try:
            with pytest.raises(RuntimeError, match="outside run"):
                p.enqueue([Job("dummy", "1")])
            with pytest.raises(RuntimeError, match="outside run"):
                p.list_dlq()
            with pytest.raises(RuntimeError, match="outside run"):
                p.clear_dlq()
            with pytest.raises(RuntimeError, match="outside run"):
                p.clear_history("dummy::1")
            with pytest.raises(RuntimeError, match="outside run"):
                p.seed_wall(["dummy::1"])
            with pytest.raises(RuntimeError, match="outside run"):
                p.seed_cursor("k", "v")
        finally:
            p._runtime._is_running = False

