"""Unit tests for EngineRuntime deep module."""

from __future__ import annotations

import pytest

from tests.helpers import make_runtime

from tasklite.engine.runtime import (
    EngineRuntime,
    ExecutionOptions,
    ExitReason,
    RunSummary,
    StepOutcome,
    StopMode,
    TaskStats,
)
from tasklite.engine.session import RunSession
from tasklite.models.context import TaskContext
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite


def dummy_runtime_handler(job: Job, ctx: TaskContext) -> dict:
    return {"status": "ok", "uid": job.uid}


class TestEngineRuntimeConfiguration:
    """测试 EngineRuntime 构造与静态配置装配。"""

    def test_default_assembly_and_wiring(self, tmp_path):
        runtime = make_runtime(tmp_path, name="test_runtime")

        assert runtime.config.name == "test_runtime"
        assert runtime.stop_mode == StopMode.NONE
        assert not runtime.is_running
        assert isinstance(runtime.stats, TaskStats)
        assert isinstance(runtime.session, RunSession)
        assert runtime.channel is runtime.config.channel
        assert runtime.store is not None


class TestEngineRuntimeStopStateMachine:
    """测试 EngineRuntime 停机状态机单调流转契约。"""

    def test_stop_state_transitions(self, tmp_path):
        runtime = make_runtime(tmp_path, name="test_stop")

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
        runtime = make_runtime(tmp_path, name="test_force")

        mode = runtime.request_stop(force=True)
        assert mode == StopMode.ABORTING
        assert runtime.stop_mode == StopMode.ABORTING


class TestEngineRuntimeStepPump:
    """测试 EngineRuntime 确定性单步事件泵 step()。"""

    def test_step_on_empty_state(self, tmp_path):
        runtime = make_runtime(tmp_path, name="test_step_empty")

        outcome = runtime.step()
        assert isinstance(outcome, StepOutcome)
        assert outcome.is_idle is True
        assert outcome.dispatched_count == 0
        assert outcome.completed_count == 0

    def test_step_draining_and_aborting_modes(self, tmp_path):
        runtime = make_runtime(tmp_path, name="test_step_modes")

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
        events = []

        def on_start():
            events.append("start")

        def on_end(reason: str):
            events.append(f"end:{reason}")

        runtime = make_runtime(
            tmp_path, name="test_exec", on_run_start=on_start, on_run_end=on_end,
        )

        summary = runtime.execute()
        assert isinstance(summary, RunSummary)
        assert summary.exit_reason == ExitReason.COMPLETED
        assert summary.duration_seconds >= 0
        assert summary.unhandled_exception is None
        assert events == ["start", "end:completed"]

    def test_execute_resets_session_state_between_runs(self, tmp_path):
        runtime = make_runtime(tmp_path, name="test_exec_reset")

        runtime.execute()
        first_run_id = runtime.session.run_id
        assert first_run_id

        runtime.session.stats["completed"] = 5
        runtime.request_stop(force=True)

        runtime.execute()
        assert runtime.session.run_id != first_run_id
        assert runtime.session.stop_mode == StopMode.NONE
        assert runtime.session.stats["completed"] == 0

    def test_strict_picklable_unpicklable_handler_fails(self, tmp_path):
        events = []

        # 使用不可 pickle 的 local lambda
        local_fn = lambda j, c: True  # noqa: E731

        from tasklite.pipeline import HandlerEntry
        handlers = {"unpicklable": HandlerEntry(local_fn, {}, None)}

        runtime = make_runtime(
            tmp_path, name="test_picklable", handlers=handlers,
            strict_picklable=True,
            on_run_end=lambda reason: events.append(reason),
        )

        with pytest.raises(TypeError, match="not picklable"):
            runtime.execute()

        assert "error" in events

    def test_concurrent_run_lock_rejection(self, tmp_path):
        runtime1 = make_runtime(tmp_path, name="test_lock")
        ipc_dir = runtime1.config.ipc_dir

        # 模拟 runtime1 持有锁
        from tasklite.utils.lockfile import try_acquire_lock, release_lock
        lock_fd = try_acquire_lock(ipc_dir, "__pipeline_run__")
        assert lock_fd is not None

        try:
            runtime2 = make_runtime(tmp_path, name="test_lock")
            with pytest.raises(RuntimeError, match="Another run.*in progress"):
                runtime2.execute()
        finally:
            release_lock(lock_fd)


class TestTaskLiteRuntimeFacadeIntegration:
    """测试 TaskLite 宿主门面与 EngineRuntime 架构一致性。"""

    def test_tasklite_exposes_runtime_and_delegates(self, tmp_path):
        p = TaskLite(name="facade_test", state_dir=str(tmp_path), backend="memory")
        assert isinstance(p._runtime, EngineRuntime)
        assert p._runtime.config.name == "facade_test"
        assert p._runtime.scheduler is p.scheduler
        assert p.store is p._runtime.store
        assert p.channel is p._runtime.channel
        assert p.in_flight is p._runtime.in_flight

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
