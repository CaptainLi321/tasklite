"""v2 EngineRuntime 运行时测试：四相事件泵、承重网与停机控制流。

覆盖：装配与接线、停机状态机单调流转、step 单步事件泵（空态/排空/
强杀三早退）、execute 生命周期（起止钩子对称、异常经 raise 通道上抛、
会话复位、strict 预检、单运行锁、初始化段故障复位）、run() 重入守卫、
崩溃承重网（在途清扫 + 崩溃保队 + 收尾钩子）、fencing 屏障与
DRAINING / ABORTING 停机语义（确定性 fake 进程驱动）。
"""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import ImmediateRequeuePolicy, RerunPolicy
from tasklite.engine.channel import ExecutionChannel
from tasklite.engine.config import RunConfig
from tasklite.engine.errorclass import ErrorClassifier
from tasklite.engine.governor import DeadlockGovernor
from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.engine.runtime import EngineRuntime
from tasklite.engine.session import RunSession
from tasklite.engine.store import StateStore
from tasklite.engine.types import (
    AttemptFinish,
    ExitReason,
    RunSummary,
    StepOutcome,
    StopMode,
    TaskStats,
)
from tasklite.models.job import Job, WORKER_RESOURCE
from tasklite.models.task import Task, TaskRegistry
from tasklite.utils.ipc import ArtifactJournal


def ok_handler(job, ctx):
    """占位 handler（模块级可 pickle，满足 strict 预检）。"""
    return True


# ── fake 进程族（与真实 worker 共用 IPC 结果文件路径）──────────────────


def _write_success_result(spec) -> None:
    """按 WorkerLaunchSpec 具名契约写成功结果文件（含认证令牌）。"""
    ArtifactJournal(spec.ipc_dir).write_result_atomic(
        spec.job.uid,
        {
            "status": "success",
            "raw_result": True,
            "new_jobs": [],
            "resource_suspensions": [],
            "cursor_updates": {},
            "auth": spec.result_token,
        },
        incarnation=spec.incarnation,
    )


def make_instant_process_class():
    """FakeProcess：start() 即写成功结果并退出（E2E 冒烟用）。"""

    class InstantProcess:
        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self.args = args
            self._alive = False
            self.exitcode = 0

        def start(self):
            if self.args:
                _write_success_result(self.args[0])
            self._alive = False

        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

        def kill(self):
            self.exitcode = -9

        def close(self):
            pass

    return InstantProcess


def make_gate_process_class(enter_event, release_event):
    """FakeProcess：start() 置位 enter_event 后保持 alive；release_event
    置位后写成功结果并退出——精确控制「执行体活动期」窗口。"""

    class GateProcess:
        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self.args = args
            self._alive = False
            self._written = False
            self.exitcode = 0

        def start(self):
            self._alive = True
            enter_event.set()

        def _complete_if_released(self):
            if not release_event.is_set() or self._written:
                return
            self._written = True
            if self.args:
                _write_success_result(self.args[0])
            self._alive = False

        def is_alive(self):
            self._complete_if_released()
            return self._alive

        def join(self, timeout=None):
            self._complete_if_released()

        def kill(self):
            self._alive = False
            self.exitcode = -9

        def close(self):
            pass

    return GateProcess


def make_stay_alive_process_class(enter_event):
    """FakeProcess：start() 置位 enter_event 后永不写结果、保持 alive
    （被 kill 才退出）。"""

    class StayAliveProcess:
        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self.args = args
            self._alive = False
            self.exitcode = 0

        def start(self):
            self._alive = True
            enter_event.set()

        def is_alive(self):
            return self._alive

        def join(self, timeout=None):
            pass

        def kill(self):
            self._alive = False
            self.exitcode = -9

        def close(self):
            pass

    return StayAliveProcess


class RuntimeEnv:
    """v2 EngineRuntime 显式装配（InMemory 后端零 IO；字段即依赖清单）。"""

    def __init__(
        self,
        tmp_path,
        *,
        name: str = "test_runtime",
        capacity: float = 2.0,
        strict_picklable: bool = True,
        on_run_start=None,
        on_run_end=None,
        on_attempt_finished=None,
    ):
        self.ipc_dir = str(tmp_path / "ipc")
        Path(self.ipc_dir).mkdir(parents=True, exist_ok=True)
        self.backend = InMemoryStateBackend()
        self.resources = ResourceManager(
            {WORKER_RESOURCE: CapacityResource(WORKER_RESOURCE, capacity)}
        )
        self.tasks = TaskRegistry()
        self.hooks: list[tuple[str, AttemptFinish]] = []
        self.runtime = EngineRuntime(
            config=RunConfig.resolve(
                name=name,
                ipc_dir=self.ipc_dir,
                backend=self.backend,
                resources=self.resources,
                tasks=self.tasks,
                channel=ExecutionChannel(self.ipc_dir),
                classifier=ErrorClassifier(),
                governor=DeadlockGovernor(),
                rerun_policy=RerunPolicy(),
                requeue_policy=ImmediateRequeuePolicy(),
                output_root=tmp_path,
                strict_picklable=strict_picklable,
                on_run_start=on_run_start,
                on_run_end=on_run_end,
                on_attempt_finished=(
                    on_attempt_finished
                    if on_attempt_finished is not None
                    else (lambda uid, *, outcome: self.hooks.append((uid, outcome)))
                ),
            ),
        )

    def register(self, task_type: str, handler=ok_handler) -> None:
        self.tasks.register(Task(task_type=task_type, handler=handler))


# ── 装配与停机状态机 ──────────────────────────────────────────────────


class TestEngineRuntimeConfiguration:
    """测试 EngineRuntime 构造与静态配置装配。"""

    def test_default_assembly_and_wiring(self, tmp_path):
        env = RuntimeEnv(tmp_path, name="test_runtime")
        runtime = env.runtime

        assert runtime.config.name == "test_runtime"
        assert runtime.stop_mode == StopMode.NONE
        assert not runtime.is_running
        assert isinstance(runtime.stats, TaskStats)
        assert isinstance(runtime.session, RunSession)
        assert isinstance(runtime.store, StateStore)
        assert runtime.channel is runtime.config.channel
        assert runtime.backend is env.backend
        assert runtime.tasks is env.tasks
        assert runtime.state is runtime.store.state

    def test_stop_state_transitions(self, tmp_path):
        runtime = RuntimeEnv(tmp_path).runtime

        assert runtime.stop_mode == StopMode.NONE
        # 首次 stop(force=False) -> DRAINING
        assert runtime.request_stop(force=False) == StopMode.DRAINING
        # 二次 stop(force=False) 升级为 ABORTING（单调）
        assert runtime.request_stop(force=False) == StopMode.ABORTING
        # 再次 stop 保持 ABORTING（幂等）
        assert runtime.request_stop(force=False) == StopMode.ABORTING

    def test_stop_force_direct_aborting(self, tmp_path):
        runtime = RuntimeEnv(tmp_path).runtime
        assert runtime.request_stop(force=True) == StopMode.ABORTING
        assert runtime.stop_mode == StopMode.ABORTING


# ── step 单步事件泵 ──────────────────────────────────────────────────


class TestEngineRuntimeStepPump:
    """测试 EngineRuntime 确定性单步事件泵 step()。"""

    def test_step_on_empty_state(self, tmp_path):
        runtime = RuntimeEnv(tmp_path).runtime
        outcome = runtime.step()
        assert isinstance(outcome, StepOutcome)
        assert outcome.is_idle is True
        assert outcome.dispatched_count == 0
        assert outcome.completed_count == 0
        assert outcome.should_terminate is True

    def test_step_draining_and_aborting_modes(self, tmp_path):
        runtime = RuntimeEnv(tmp_path).runtime

        runtime.request_stop(force=False)
        outcome_draining = runtime.step()
        assert outcome_draining.exit_reason == "stopped_draining"
        assert outcome_draining.should_terminate is True

        runtime.request_stop(force=True)
        outcome_aborting = runtime.step()
        assert outcome_aborting.exit_reason == "stopped_aborting"
        assert outcome_aborting.should_terminate is True


# ── execute 生命周期 ─────────────────────────────────────────────────


class TestEngineRuntimeExecutionLifecycle:
    """测试 EngineRuntime 完整生命周期 execute() 与安全网。"""

    def test_execute_empty_queue_completed(self, tmp_path):
        events = []

        env = RuntimeEnv(
            tmp_path, name="test_exec",
            on_run_start=lambda: events.append("start"),
            on_run_end=lambda reason: events.append(f"end:{reason}"),
        )
        summary = env.runtime.execute()
        assert isinstance(summary, RunSummary)
        assert summary.exit_reason is ExitReason.COMPLETED
        assert summary.duration_seconds >= 0
        assert summary.unhandled_exception is None
        assert summary.run_id == env.runtime.session.run_id
        assert events == ["start", "end:completed"]

    def test_execute_completes_job_end_to_end(self, tmp_path, monkeypatch):
        """四相泵 E2E：派发 → 回收 → 结算进 wall，attempts 轨迹收尾成功。"""
        env = RuntimeEnv(tmp_path)
        env.register("t")
        env.runtime.store.enqueue_jobs([Job("t", "j1", payload={})])
        monkeypatch.setattr(
            env.runtime.channel, "_mp_ctx",
            SimpleNamespace(Process=make_instant_process_class()),
        )

        summary = env.runtime.execute()

        assert summary.exit_reason is ExitReason.COMPLETED
        assert env.runtime.stats["completed"] == 1
        assert "t::j1" in env.backend.load_wall()
        assert env.backend.load_queue() == []
        # attempt 收尾钩子经单一出口触发（成功形态值对象）
        assert [(uid, out.success, out.going_to_retry) for uid, out in env.hooks] == [
            ("t::j1", True, False)
        ]
        records = env.backend.load_attempts("t::j1")
        assert records[-1].outcome == "succeeded"
        assert records[-1].run_id == summary.run_id

    def test_unhandled_exception_propagates_via_raise_channel(self, tmp_path, monkeypatch):
        """异常通道契约：run 体内未处理异常一律经 raise 通道原样上抛，
        execute() 不返回异常摘要；故障解除后同实例可复跑。"""
        runtime = RuntimeEnv(tmp_path).runtime

        def _boom():
            raise RuntimeError("run body exploded")

        monkeypatch.setattr(runtime, "prepare_run_state", _boom)
        with pytest.raises(RuntimeError, match="run body exploded"):
            runtime.execute()
        monkeypatch.undo()
        assert not runtime.is_running, "异常路径必须复位运行标志"

        summary = runtime.execute()
        assert summary.exit_reason is ExitReason.COMPLETED
        assert summary.unhandled_exception is None

    def test_execute_resets_session_state_between_runs(self, tmp_path):
        runtime = RuntimeEnv(tmp_path).runtime

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
        """strict 预检失败：TypeError 上抛且起止钩子均不触发。

        不变式：on_run_start/on_run_end 起止对称——预检发生在 session
        begin 之前，run 尚未开始，fire_run_end 不得触发；错误传播语义
        稳定（重复 execute 仍 fail-loud），运行态逐次复位。
        """
        events = []
        # 不可 pickle 的 local lambda
        local_fn = lambda j, c: True  # noqa: E731
        env = RuntimeEnv(
            tmp_path, name="test_picklable", strict_picklable=True,
            on_run_start=lambda: events.append("start"),
            on_run_end=lambda reason: events.append(f"end:{reason}"),
        )
        env.tasks.register(Task(task_type="unpicklable", handler=local_fn))

        with pytest.raises(TypeError, match="not picklable"):
            env.runtime.execute()
        # run 未 begin：起止钩子均不触发（起止对称），运行态已复位
        assert events == []
        assert not env.runtime.is_running
        assert env.runtime._run_lock_fd is None

        # 预检故障不留残留状态：同实例复跑仍稳定 fail-loud 且逐次复位
        with pytest.raises(TypeError, match="not picklable"):
            env.runtime.execute()
        assert events == []
        assert not env.runtime.is_running

        # 对照：可 pickle handler 的装配正常完成（钩子一对一配对触发）
        env_ok = RuntimeEnv(
            tmp_path / "ok", name="test_picklable_ok",
            on_run_start=lambda: events.append("start"),
            on_run_end=lambda reason: events.append(f"end:{reason}"),
        )
        env_ok.register("dummy")
        summary = env_ok.runtime.execute()
        assert summary.exit_reason is ExitReason.COMPLETED
        assert events == ["start", "end:completed"]

    def test_strict_picklable_disabled_allows_local_handler(self, tmp_path, monkeypatch):
        """预检关闭：不可 pickle handler 不在入口拒绝（交由派发期处置）。"""
        env = RuntimeEnv(tmp_path, strict_picklable=False)
        local_fn = lambda j, c: True  # noqa: E731
        env.tasks.register(Task(task_type="local", handler=local_fn))
        monkeypatch.setattr(
            env.runtime.channel, "_mp_ctx",
            SimpleNamespace(Process=make_instant_process_class()),
        )
        env.runtime.store.enqueue_jobs([Job("local", "j1", payload={})])
        summary = env.runtime.execute()
        assert summary.exit_reason is ExitReason.COMPLETED
        assert "local::j1" in env.backend.load_wall()

    def test_concurrent_run_lock_rejection(self, tmp_path):
        from tasklite.utils.lockfile import release_lock, try_acquire_lock

        env1 = RuntimeEnv(tmp_path, name="test_lock")
        # 模拟同 state_dir 已有 run 持锁
        lock_fd = try_acquire_lock(env1.ipc_dir, "__pipeline_run__")
        assert lock_fd is not None
        try:
            env2 = RuntimeEnv(tmp_path, name="test_lock")
            with pytest.raises(RuntimeError, match="Another run.*in progress"):
                env2.runtime.execute()
        finally:
            release_lock(lock_fd)


class TestEngineRuntimeInitFailureSafety:
    """初始化段（run 锁获取 + 信号陷阱）异常安全回归。

    不变式：任何离开 execute() 的路径都必须复位 _is_running 并释放已
    获取的锁 fd；同实例随后可正常复跑。try_acquire_lock 契约：环境
    故障抛 OSError（语义上抛不吞），仅「锁被占」返回 None。
    """

    def test_lock_env_failure_resets_running_and_allows_rerun(self, tmp_path, monkeypatch):
        """锁获取抛 OSError（权限/磁盘满类环境故障）后标志复位、锁无
        泄漏、可复跑。"""
        import tasklite.engine.runtime as runtime_module

        events: list[str] = []
        env = RuntimeEnv(
            tmp_path, name="test_env_failure",
            on_run_end=lambda reason: events.append(reason),
        )

        def _raise_env_failure(ipc_dir, uid, *, timeout=0):
            raise OSError(13, "Permission denied")

        monkeypatch.setattr(runtime_module, "try_acquire_lock", _raise_env_failure)
        with pytest.raises(OSError):
            env.runtime.execute()

        # 故障路径收尾：运行标志复位、无锁 fd 残留、未开始的 run 不发 on_run_end
        assert not env.runtime.is_running
        assert env.runtime._run_lock_fd is None
        assert events == []

        # 故障注入解除后，同实例可正常启动并完成（复跑成功同时证明锁已
        # 释放：遗留 fd 的 flock 会让下一次 try_acquire_lock 返回 None）
        monkeypatch.undo()
        summary = env.runtime.execute()
        assert summary.exit_reason is ExitReason.COMPLETED
        assert not env.runtime.is_running
        assert env.runtime._run_lock_fd is None

    def test_keyboard_interrupt_during_init_releases_lock_and_resets_flag(
        self, tmp_path, monkeypatch,
    ):
        """初始化中途 KeyboardInterrupt 后标志复位、真实锁 fd 已释放、可复跑。"""
        import signal as signal_module

        import tasklite.engine.runtime as runtime_module
        from tasklite.utils.lockfile import release_lock, try_acquire_lock

        env = RuntimeEnv(tmp_path, name="test_init_interrupt")

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
            env.runtime.execute()

        monkeypatch.undo()
        assert not env.runtime.is_running
        assert env.runtime._run_lock_fd is None

        # 直接验证 __pipeline_run__ 锁可再次获取（无 fd 泄漏）
        lock_fd = try_acquire_lock(env.ipc_dir, "__pipeline_run__", timeout=0)
        assert lock_fd is not None
        release_lock(lock_fd)

        summary = env.runtime.execute()
        assert summary.exit_reason is ExitReason.COMPLETED
        assert not env.runtime.is_running


# ── run() 重入守卫 ───────────────────────────────────────────────────


class TestRunReentryGuard:
    def test_reentrant_run_rejected_and_reset_after(self, tmp_path, monkeypatch):
        """执行期间二次 execute() 必须 RuntimeError；结束后标志复位且可复跑。"""
        enter, release = threading.Event(), threading.Event()
        env = RuntimeEnv(tmp_path, capacity=1)
        env.register("t")
        env.runtime.store.enqueue_jobs([Job("t", "j1", payload={})])
        monkeypatch.setattr(
            env.runtime.channel, "_mp_ctx",
            SimpleNamespace(Process=make_gate_process_class(enter, release)),
        )
        runtime = env.runtime

        run_errors: list[BaseException] = []

        def run_once():
            try:
                runtime.execute()
            except BaseException as e:  # 正常路径不应抛，兜底记录便于诊断
                run_errors.append(e)

        t = threading.Thread(target=run_once)
        t.start()
        try:
            assert enter.wait(10), "前置：execute() 应已派发执行体进入活动期"
            assert runtime.is_running, "run() 执行期间 is_running 必须为真"

            # 执行期间同实例再次 run() 必须拒绝（重入守卫）
            with pytest.raises(RuntimeError, match=r"^Pipeline run\(\) already in progress"):
                runtime.execute()
        finally:
            release.set()
        t.join(timeout=15)
        assert not t.is_alive(), "首次 run() 应在放行后正常结束"
        assert run_errors == [], f"首次 run() 不得异常: {run_errors}"
        assert not runtime.is_running, "run() 结束后运行标志必须复位"

        # 结束后恢复正常：同实例可再次 run() 并完成队列
        runtime.execute()
        assert "t::j1" in env.backend.load_wall(), "复跑必须正常完成作业"


# ── 崩溃承重网与 fencing 屏障 ────────────────────────────────────────


class TestCrashNet:
    """_run_with_crash_net 统一异常承重网（确定性单测）。"""

    def test_crash_net_aborts_saves_and_fires_run_end(self, tmp_path, monkeypatch):
        runtime = RuntimeEnv(tmp_path).runtime
        calls: list[str] = []

        def _boom():
            raise RuntimeError("loop exploded")

        monkeypatch.setattr(runtime, "run_loop", _boom)
        monkeypatch.setattr(
            runtime._recovery, "abort_in_flight", lambda: calls.append("abort")
        )
        monkeypatch.setattr(
            runtime._recovery, "save_queue_crash_safe", lambda: calls.append("save")
        )
        fired: list[str] = []
        monkeypatch.setattr(runtime.session, "on_run_end", fired.append)

        with pytest.raises(RuntimeError, match="loop exploded"):
            runtime._run_with_crash_net()

        assert calls == ["abort", "save"], "承重网必须先在途清扫再崩溃保队"
        assert fired == ["error"], "承重网必须以异常终局触发收尾钩子"

    def test_crash_net_uses_session_exit_reason(self, tmp_path, monkeypatch):
        """KI 承重网终局 reason 为 interrupted（异常类型优先）。"""
        runtime = RuntimeEnv(tmp_path).runtime

        def _interrupt():
            raise KeyboardInterrupt

        monkeypatch.setattr(runtime, "run_loop", _interrupt)
        monkeypatch.setattr(
            runtime._recovery, "abort_in_flight", lambda: None
        )
        monkeypatch.setattr(
            runtime._recovery, "save_queue_crash_safe", lambda: None
        )
        fired: list[str] = []
        monkeypatch.setattr(runtime.session, "on_run_end", fired.append)

        with pytest.raises(KeyboardInterrupt):
            runtime._run_with_crash_net()
        assert fired == ["interrupted"]

    def test_persist_suspension_failure_degraded_not_fatal(self, tmp_path, monkeypatch):
        """收尾期挂起持久化失败降级告警，不遮蔽正常终局钩子。"""
        runtime = RuntimeEnv(tmp_path).runtime
        monkeypatch.setattr(runtime, "run_loop", lambda: None)

        def _boom(*_a, **_kw):
            raise OSError(5, "disk gone")

        monkeypatch.setattr(
            runtime._recovery, "persist_resource_suspensions", _boom
        )
        fired: list[str] = []
        monkeypatch.setattr(runtime.session, "on_run_end", fired.append)

        runtime._run_with_crash_net()
        assert fired == ["completed"]

    def test_run_id_meta_failure_fails_loud(self, tmp_path, monkeypatch):
        """set_meta 失败 → execute() 抛异常，而非静默降级为无 fence 运行。"""
        env = RuntimeEnv(tmp_path)
        env.register("t")

        def boom(*_a, **_kw):
            raise RuntimeError("meta table is broken")

        monkeypatch.setattr(env.backend, "set_meta", boom)
        with pytest.raises(RuntimeError, match="meta table is broken"):
            env.runtime.execute()


class TestPrepareRunState:
    """fencing 屏障：run_id/result_token 轮换与 channel 同步。"""

    def test_prepare_rotates_identity_and_persists(self, tmp_path):
        env = RuntimeEnv(tmp_path)
        runtime = env.runtime
        runtime.session.run_id = "stale"
        runtime.session.dispatch_seq = 9

        state = runtime.prepare_run_state()

        assert runtime.session.run_id and runtime.session.run_id != "stale"
        assert env.backend.get_meta("last_run_id") == runtime.session.run_id
        # 认证令牌与 run_id 同生命周期轮换并同步执行通道
        assert runtime.channel.result_token == runtime.session.result_token
        assert runtime.session.dispatch_seq == 0
        assert runtime.store.state is state

    def test_prepare_clears_scheduler_cache_each_round(self, tmp_path):
        env = RuntimeEnv(tmp_path)
        runtime = env.runtime
        jd = Job("t", "j1", payload={}).to_dict()
        runtime.scheduler.cached_job(jd)  # 预热跨轮缓存
        assert runtime.scheduler._job_cache, "前置：调度缓存应已预热"
        env.backend.enqueue_jobs([jd])
        runtime.prepare_run_state()
        assert not runtime.scheduler._job_cache, "run 启动屏障必须清空调度缓存"
        assert runtime.store.state.queue, "装载后内存队列须有磁盘作业"

    def test_two_runs_rotate_distinct_identity(self, tmp_path):
        env = RuntimeEnv(tmp_path)
        runtime = env.runtime
        runtime.prepare_run_state()
        first = runtime.session.run_id
        runtime.prepare_run_state()
        assert runtime.session.run_id != first


# ── DRAINING / ABORTING 停机语义（确定性 fake 进程）──────────────────


class TestGracefulDraining:
    def test_draining_waits_for_inflight_and_keeps_undispatched(
        self, tmp_path, monkeypatch,
    ):
        """DRAINING：stop 时 in-flight 仍在运行 → 不 kill、等它自然完成并
        commit；未派发作业保留在磁盘队列（下次 run 继续）。"""
        enter, release = threading.Event(), threading.Event()
        env = RuntimeEnv(tmp_path, capacity=1)
        env.register("t")
        env.runtime.store.enqueue_jobs(
            [Job("t", "j1", payload={}), Job("t", "j2", payload={})]
        )
        monkeypatch.setattr(
            env.runtime.channel, "_mp_ctx",
            SimpleNamespace(Process=make_gate_process_class(enter, release)),
        )
        runtime = env.runtime
        observed: dict[str, Any] = {}

        def controller():
            # 等 j1 派发进 in-flight（j2 因 max_workers=1 留队）
            assert enter.wait(10)
            observed["inflight_at_stop"] = len(runtime.in_flight)
            proc = next(iter(runtime.in_flight.values())).handle.process
            runtime.request_stop()  # 默认 DRAINING：不 kill
            observed["queue_during_draining"] = [
                jd["job_id"] for jd in runtime.store.state.queue
            ]
            observed["alive_during_draining"] = proc.is_alive()
            release.set()  # 放行：in-flight 自然完成

        t = threading.Thread(target=controller)
        t.start()
        summary = runtime.execute()
        t.join()

        assert observed["inflight_at_stop"] == 1, "stop 时 in-flight 必须非空"
        assert observed["queue_during_draining"] == ["j2"], \
            "DRAINING 期间不得派发 j2，j2 必须保留在队列"
        assert observed["alive_during_draining"] is True, \
            "DRAINING 不得 kill in-flight 进程"
        assert summary.exit_reason is ExitReason.STOPPED_DRAINING
        # DRAINING：in-flight job 完成并进 wall；j2 留磁盘队列
        assert "t::j1" in env.backend.load_wall()
        assert "t::j2" not in env.backend.load_wall()
        assert any(jd.get("job_id") == "j2" for jd in env.backend.load_queue())


class TestForceAbort:
    def test_aborting_kills_inflight_and_requeues(self, tmp_path, monkeypatch):
        """ABORTING：stop(force=True) 在 in-flight 期间 kill 子进程并
        requeue——作业未 commit 保留在磁盘队列（at-least-once）。"""
        enter = threading.Event()
        env = RuntimeEnv(tmp_path, capacity=1)
        env.register("t")
        env.runtime.store.enqueue_jobs([Job("t", "j1", payload={})])
        monkeypatch.setattr(
            env.runtime.channel, "_mp_ctx",
            SimpleNamespace(Process=make_stay_alive_process_class(enter)),
        )
        runtime = env.runtime
        observed: dict[str, Any] = {}

        def controller():
            assert enter.wait(10)
            observed["proc"] = next(iter(runtime.in_flight.values())).handle.process
            runtime.request_stop(force=True)

        t = threading.Thread(target=controller)
        t.start()
        summary = runtime.execute()
        t.join()

        assert summary.exit_reason is ExitReason.STOPPED_ABORTING
        proc = observed["proc"]
        assert proc.is_alive() is False, "ABORTING 必须 kill in-flight 子进程"
        assert proc.exitcode == -9
        # 作业未 commit（未进 wall），保留在磁盘队列
        assert "t::j1" not in env.backend.load_wall()
        assert any(jd.get("job_id") == "j1" for jd in env.backend.load_queue())

    def test_deadlock_detected_flag_propagates_to_step_outcome(
        self, tmp_path, monkeypatch,
    ):
        """无可运行候选且无 in-flight 时：governor 熔断结果经命名仲裁
        对象透传到 StepOutcome.deadlock_detected。"""
        from tasklite.engine.governor import DeadlockDecision

        env = RuntimeEnv(tmp_path)
        runtime = env.runtime
        # 队列装载一个依赖缺失作业 → 调度扫描给出 NO_CANDIDATE 停摆事实
        runtime.store.state.spawn_jobs(
            [Job("t", "blocked", payload={}, depends_on=["t::missing"]).to_dict()],
            front=False,
        )
        monkeypatch.setattr(
            runtime.governor, "arbitrate",
            lambda facts, store: DeadlockDecision(
                action="resolved", should_terminate=True,
                failed_uids=["t::blocked"],
            ),
        )
        outcome = runtime.step()
        assert outcome.deadlock_detected is True
        assert outcome.should_terminate is True
