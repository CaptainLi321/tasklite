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
        """strict_picklable 预检失败：TypeError 上抛且起止钩子均不触发。

        不变式：on_run_start/on_run_end 起止对称——预检发生在 session
        begin 之前，run 尚未开始，fire_run_end 不得触发（与初始化段故障
        同纪律）；错误传播语义不变，实例可复跑。
        """
        events = []

        # 使用不可 pickle 的 local lambda
        local_fn = lambda j, c: True  # noqa: E731

        from tasklite.pipeline import HandlerEntry
        handlers = {"unpicklable": HandlerEntry(local_fn, {}, None)}

        runtime = make_runtime(
            tmp_path, name="test_picklable", handlers=handlers,
            strict_picklable=True,
            on_run_start=lambda: events.append("start"),
            on_run_end=lambda reason: events.append(f"end:{reason}"),
        )

        with pytest.raises(TypeError, match="not picklable"):
            runtime.execute()

        # run 未 begin：起止钩子均不触发（起止对称），运行态已复位
        assert events == []
        assert not runtime.is_running
        assert runtime._run_lock_fd is None

        # 预检故障不留残留状态：换可 pickle handler 后同实例正常复跑，
        # 起止钩子一对一配对触发
        runtime.handlers.clear()
        runtime.handlers["dummy"] = HandlerEntry(dummy_runtime_handler, {}, None)
        summary = runtime.execute()
        assert summary.exit_reason is ExitReason.COMPLETED
        assert events == ["start", "end:completed"]

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


class TestEngineRuntimeInitFailureSafety:
    """初始化段（run 锁获取 + 信号陷阱）异常安全回归。

    不变式：任何离开 execute() 的路径都必须复位 _is_running 并释放已获取
    的锁 fd；同实例随后可正常复跑。try_acquire_lock 契约：环境故障抛
    OSError（语义上抛不吞），仅「锁被占」返回 None。
    """

    def test_lock_env_failure_resets_running_and_allows_rerun(self, tmp_path, monkeypatch):
        """锁获取抛 OSError（权限/磁盘满类环境故障）后标志复位、锁无泄漏、可复跑。"""
        import tasklite.engine.runtime as runtime_module

        events: list = []
        runtime = make_runtime(
            tmp_path, name="test_env_failure",
            on_run_end=lambda reason: events.append(reason),
        )

        with monkeypatch.context() as m:
            def _raise_env_failure(ipc_dir, uid, *, timeout=0):
                raise OSError(13, "Permission denied")

            m.setattr(runtime_module, "try_acquire_lock", _raise_env_failure)
            with pytest.raises(OSError):
                runtime.execute()

        # 故障路径收尾：运行标志复位、无锁 fd 残留、未开始的 run 不发 on_run_end
        assert not runtime.is_running
        assert runtime._run_lock_fd is None
        assert events == []

        # 故障注入解除后，同实例可正常启动并完成（复跑成功同时证明锁已释放：
        # 遗留 fd 的 flock 会让下一次 try_acquire_lock 返回 None）
        summary = runtime.execute()
        assert summary.exit_reason is ExitReason.COMPLETED
        assert not runtime.is_running
        assert runtime._run_lock_fd is None

    def test_keyboard_interrupt_during_init_releases_lock_and_resets_flag(
        self, tmp_path, monkeypatch,
    ):
        """初始化中途 KeyboardInterrupt 后标志复位、真实锁 fd 已释放、可复跑。"""
        import signal as signal_module

        import tasklite.engine.runtime as runtime_module
        from tasklite.utils.lockfile import release_lock, try_acquire_lock

        runtime = make_runtime(tmp_path, name="test_init_interrupt")

        class _InstallSignalInterrupt:
            """信号陷阱安装点抛 KeyboardInterrupt 的确定性故障注入。

            此刻 run 锁已获取并注册，覆盖「锁已持、主循环未启」的中断窗口。
            """

            SIGTERM = signal_module.SIGTERM
            SIGINT = signal_module.SIGINT

            @staticmethod
            def signal(signum, handler):
                raise KeyboardInterrupt

        monkeypatch.setattr(runtime_module, "signal", _InstallSignalInterrupt)

        with pytest.raises(KeyboardInterrupt):
            runtime.execute()

        monkeypatch.undo()
        assert not runtime.is_running
        assert runtime._run_lock_fd is None

        # 直接验证 __pipeline_run__ 锁可再次获取（无 fd 泄漏），探测后即释放
        lock_fd = try_acquire_lock(runtime.config.ipc_dir, "__pipeline_run__", timeout=0)
        assert lock_fd is not None
        release_lock(lock_fd)

        summary = runtime.execute()
        assert summary.exit_reason is ExitReason.COMPLETED
        assert not runtime.is_running


class TestTaskLiteRuntimeFacadeIntegration:
    """测试 TaskLite 宿主门面与 EngineRuntime 架构一致性。"""

    def test_tasklite_exposes_runtime_and_delegates(self, tmp_path):
        p = TaskLite(name="facade_test", state_dir=str(tmp_path), backend="memory")
        assert isinstance(p._runtime, EngineRuntime)
        assert p._runtime.config.name == "facade_test"
        # 深模块唯一持有点是 EngineRuntime（装配引用一致性见本文件
        # TestEngineRuntimeConfiguration；门面不再外泄深模块属性）
        assert p._runtime.store.backend is p.backend

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
