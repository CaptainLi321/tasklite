"""Run 生命周期、正常停止与信号调度控制测试。

锁定行为：
- stop() 跨线程请求并唤醒，重复 stop/Ctrl+C 幂等，不升级强停；无 Run 时无操作；
- 停止后收到 retry：预算允许时保持 pending，下次 Run 重判 Trigger 且预算重置；耗尽为 failed；
- 停止后收到失败/超时：依赖下游级联失败，其余保持 pending；新派生不派发；
- install_signals=True 仅主线程安装，子线程安装抛出，正常退出恢复原处理器；False 由宿主控制；
- 框架异常直接抛出，不伪装正常排空。
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from tasklite import FileSpec, StateSnapshot
from tasklite.engine.resource import CapacityResource
from tasklite.engine.types import ExecutionOptions, StopMode
from tasklite.models.job import Job
from tasklite.models.result import TaskResult
from tasklite.pipeline import TaskLite
from tasklite.testing import running
from tests.helpers import install_fake_process_context


# ── 模块级 handler（真实跨进程协作）─────────────────────────────────────


def success_handler(job, ctx):
    return TaskResult.success()


def write_effect_handler(job, ctx):
    Path(job.payload["effect"]).write_text("started")
    return TaskResult.success()


def lingering_success_handler(job, ctx):
    release_path = Path(job.payload["release_path"])
    done_path = Path(job.payload["done_path"])

    def hold_process() -> None:
        deadline = time.monotonic() + 20.0
        while not release_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        done_path.write_text("done", encoding="utf-8")

    threading.Thread(target=hold_process, daemon=False).start()
    return TaskResult.success()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("stage", ["preflight", "governor", "exclusions"])
def test_stop_during_initialization_is_preserved(tmp_path, monkeypatch, backend, stage):
    effect = tmp_path / "effect"
    with TaskLite(
        backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None,
    ) as pipeline:
        pipeline.register_task("work", write_effect_handler)
        pipeline.enqueue(Job("work", "one", payload={"effect": str(effect)}))
        runtime = pipeline._runtime
        runtime.session.record_retry("work::one")
        entered, proceed = threading.Event(), threading.Event()
        errors = []
        target, name = {
            "preflight": (runtime, "_preflight_picklable_handlers"),
            "governor": (runtime.governor, "reset"),
            "exclusions": (runtime.scheduler, "reset_run"),
        }[stage]
        original = getattr(target, name)

        def blocked(*args, **kwargs):
            entered.set()
            assert proceed.wait(10)
            return original(*args, **kwargs)

        def run():
            try:
                pipeline.run(install_signals=False)
            except BaseException as error:
                errors.append(error)

        with monkeypatch.context() as patch:
            patch.setattr(target, name, blocked)
            thread = threading.Thread(target=run)
            thread.start()
            try:
                assert entered.wait(10)
                assert pipeline.is_running
                assert runtime.session.get_retry_count("work::one") == 0
                pipeline.stop()
                assert runtime.stop_mode == StopMode.DRAINING
            finally:
                proceed.set()
                thread.join(10)
            assert not thread.is_alive()
            assert errors == []
        with StateSnapshot.open(pipeline) as state:
            job = state.get_job("work::one")
            assert job is not None and job["status"] == "pending"
            assert list(state.iter_attempts()) == []
        assert not effect.exists()
        assert not pipeline.is_running
        pipeline.stop()
        pipeline.run(install_signals=False)
        assert effect.read_text() == "started"
        with StateSnapshot.open(pipeline) as state:
            assert [a["outcome"] for a in state.iter_attempts()] == ["success"]
        assert runtime.stop_mode == StopMode.NONE


def test_stop_during_execution_lock_acquisition_is_preserved(tmp_path, monkeypatch):
    import tasklite.engine.runtime as runtime_module

    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db") as pipeline:
        pipeline.register_task("work", success_handler)
        pipeline.enqueue(Job("work", "one"))
        entered, proceed = threading.Event(), threading.Event()
        original = runtime_module.try_acquire_lock
        errors = []

        def blocked(*args, **kwargs):
            entered.set()
            assert proceed.wait(10)
            return original(*args, **kwargs)

        def run():
            try:
                pipeline.run(install_signals=False)
            except BaseException as error:
                errors.append(error)

        with monkeypatch.context() as patch:
            patch.setattr(runtime_module, "try_acquire_lock", blocked)
            thread = threading.Thread(target=run)
            thread.start()
            try:
                assert entered.wait(10)
                pipeline.stop()
            finally:
                proceed.set()
                thread.join(10)
            assert not thread.is_alive()
            assert errors == []
        with StateSnapshot.open(pipeline) as state:
            assert state.get_job("work::one")["status"] == "pending"
            assert list(state.iter_attempts()) == []
        pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as state:
            assert [a["outcome"] for a in state.iter_attempts()] == ["success"]


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("stage", ["preflight", "governor", "exclusions", "unbind"])
def test_initialization_failure_releases_run_for_retry(tmp_path, monkeypatch, backend, stage):
    with TaskLite(
        backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None,
    ) as pipeline:
        runtime = pipeline._runtime
        target, name = {
            "preflight": (runtime, "_preflight_picklable_handlers"),
            "governor": (runtime.governor, "reset"),
            "exclusions": (runtime.scheduler, "reset_run"),
            "unbind": (runtime.config.resources, "unbind_notifier"),
        }[stage]
        error = RuntimeError("initialization failed")

        def fail(*args, **kwargs):
            raise error

        with monkeypatch.context() as patch:
            patch.setattr(target, name, fail)
            with pytest.raises(RuntimeError) as raised:
                pipeline.run(install_signals=False)
            assert raised.value is error
        assert not pipeline.is_running
        pipeline.run(install_signals=False)


@pytest.mark.parametrize("fail_second", [False, True])
def test_signal_during_installation_is_preserved_and_handlers_restored(
    tmp_path, monkeypatch, fail_second,
):
    import tasklite.engine.runtime as runtime_module

    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db") as pipeline:
        pipeline.register_task("work", success_handler)
        pipeline.enqueue(Job("work", "one"))
        originals = {signal.SIGTERM: object(), signal.SIGINT: object()}
        handlers = originals.copy()
        error = RuntimeError("signal installation failed")

        def install(signum, handler):
            previous = handlers[signum]
            if handler == pipeline._runtime._handle_signal:
                if signum == signal.SIGINT and fail_second:
                    raise error
                handlers[signum] = handler
                handler(signum, None)
            else:
                handlers[signum] = handler
            return previous

        with monkeypatch.context() as patch:
            patch.setattr(runtime_module.signal, "signal", install)
            if fail_second:
                with pytest.raises(RuntimeError) as raised:
                    pipeline.run()
                assert raised.value is error
            else:
                pipeline.run()
        assert handlers == originals
        assert not pipeline.is_running
        with StateSnapshot.open(pipeline) as state:
            assert state.get_job("work::one")["status"] == "pending"
            assert list(state.iter_attempts()) == []
        pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as state:
            assert [a["outcome"] for a in state.iter_attempts()] == ["success"]


def test_stop_arriving_after_run_exit_does_not_mutate_session(tmp_path, monkeypatch):
    with TaskLite(backend="memory") as pipeline:
        runtime = pipeline._runtime
        entered, proceed = threading.Event(), threading.Event()
        stop_entered, stop_proceed = threading.Event(), threading.Event()
        original_preflight = runtime._preflight_picklable_handlers
        original_stop = runtime.request_stop
        errors = []

        def preflight():
            entered.set()
            assert proceed.wait(10)
            original_preflight()

        def delayed_stop():
            stop_entered.set()
            assert stop_proceed.wait(10)
            return original_stop()

        def run():
            try:
                pipeline.run(install_signals=False)
            except BaseException as error:
                errors.append(error)

        with monkeypatch.context() as patch:
            patch.setattr(runtime, "_preflight_picklable_handlers", preflight)
            patch.setattr(runtime, "request_stop", delayed_stop)
            run_thread = threading.Thread(target=run)
            stop_thread = threading.Thread(target=pipeline.stop)
            run_thread.start()
            try:
                assert entered.wait(10)
                stop_thread.start()
                assert stop_entered.wait(10)
            finally:
                proceed.set()
                run_thread.join(10)
                stop_proceed.set()
                if stop_thread.ident is not None:
                    stop_thread.join(10)
            assert not run_thread.is_alive()
            assert not stop_thread.is_alive()
            assert errors == []
        assert not pipeline.is_running
        assert runtime.stop_mode == StopMode.NONE
        pipeline.register_task("work", success_handler)
        pipeline.enqueue(Job("work", "one"))
        pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as state:
            assert [a["outcome"] for a in state.iter_attempts()] == ["success"]


def test_stop_in_initial_authorizer_keeps_job_pending(tmp_path, monkeypatch):
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    effect = tmp_path / "effect.txt"
    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db") as pipeline:
        capacity = CapacityResource("disk", max_capacity=1)
        pipeline.register_resource(capacity)
        pipeline.register_task("work", write_effect_handler, resources={"disk": 1})
        pipeline.enqueue(Job(
            "work", "one", payload={"effect": str(effect)},
            files={"target": FileSpec(str(tmp_path / "output.txt"), role="output", delete_existing=True)},
        ))

        def stop_and_approve(path, target_type):
            assert capacity.used == 0
            pipeline.stop()
            return True

        pipeline.authorizer = stop_and_approve
        pipeline.run()
        with StateSnapshot.open(pipeline) as state:
            job = state.get_job("work::one")
            assert job is not None and job["status"] == "pending"
            assert list(state.iter_attempts(job_uid="work::one")) == []
        assert capacity.used == 0
        assert not effect.exists()


def test_stop_between_pop_and_running_mark_requeues_without_attempt(
    tmp_path, monkeypatch
):
    effect = tmp_path / "effect.txt"
    with TaskLite(
        backend="sqlite", state_path=tmp_path / "state.db", max_workers=1
    ) as pipeline:
        capacity = CapacityResource("disk", max_capacity=1)
        pipeline.register_resource(capacity)
        pipeline.register_task("work", write_effect_handler, resources={"disk": 1})
        pipeline.enqueue(Job("work", "one", payload={"effect": str(effect)}))

        state_type = type(pipeline._runtime.store.state)
        original_pop = state_type.pop_job
        stop_requested = threading.Event()

        def pop_then_stop(self, index):
            snapshot = original_pop(self, index)
            if not stop_requested.is_set():
                stop_requested.set()
                pipeline.stop()
            return snapshot

        monkeypatch.setattr(state_type, "pop_job", pop_then_stop)
        pipeline.run()

        with StateSnapshot.open(pipeline) as snapshot:
            job = snapshot.get_job("work::one")
            assert job is not None and job["status"] == "pending"
            assert list(snapshot.iter_attempts(job_uid="work::one")) == []
        assert capacity.used == 0
        assert not effect.exists()


def test_stop_after_running_mark_rolls_back_without_spawning(
    tmp_path, monkeypatch
):
    effect = tmp_path / "effect.txt"
    with TaskLite(
        backend="sqlite", state_path=tmp_path / "state.db", max_workers=1
    ) as pipeline:
        capacity = CapacityResource("disk", max_capacity=1)
        pipeline.register_resource(capacity)
        pipeline.register_task("work", write_effect_handler, resources={"disk": 1})
        pipeline.enqueue(Job("work", "one", payload={"effect": str(effect)}))

        store = pipeline._runtime.store
        original_mark_running = store.mark_job_running
        stop_requested = threading.Event()

        def mark_then_stop(*args, **kwargs):
            attempt_no = original_mark_running(*args, **kwargs)
            if not stop_requested.is_set():
                stop_requested.set()
                pipeline.stop()
            return attempt_no

        monkeypatch.setattr(store, "mark_job_running", mark_then_stop)
        pipeline.run()

        with StateSnapshot.open(pipeline) as snapshot:
            job = snapshot.get_job("work::one")
            assert job is not None and job["status"] == "pending"
            attempts = list(snapshot.iter_attempts(job_uid="work::one"))
        assert attempts == []
        assert capacity.used == 0
        assert not effect.exists()


def test_stop_inside_dispatch_log_prevents_worker_launch(tmp_path, monkeypatch):
    effect = tmp_path / "effect.txt"
    with TaskLite(
        backend="sqlite", state_path=tmp_path / "state.db", max_workers=1
    ) as pipeline:
        capacity = CapacityResource("disk", max_capacity=1)
        pipeline.register_resource(capacity)
        pipeline.register_task("work", write_effect_handler, resources={"disk": 1})
        pipeline.enqueue(Job("work", "one", payload={"effect": str(effect)}))

        import tasklite.engine.dispatch as dispatch_module

        original_info = dispatch_module.logger.info
        stop_fired = False

        def stop_during_run_log(message, *args, **kwargs):
            nonlocal stop_fired
            if message == "RUN: work::one" and not stop_fired:
                stop_fired = True
                pipeline.stop()
            return original_info(message, *args, **kwargs)

        monkeypatch.setattr(dispatch_module.logger, "info", stop_during_run_log)
        pipeline.run()

        assert stop_fired
        with StateSnapshot.open(pipeline) as snapshot:
            job = snapshot.get_job("work::one")
            assert job is not None and job["status"] == "pending"
            assert list(snapshot.iter_attempts(job_uid="work::one")) == []
        assert capacity.used == 0
        assert not effect.exists()


def test_stop_before_process_start_prevents_worker_launch(tmp_path):
    effect = tmp_path / "effect.txt"
    with TaskLite(
        backend="sqlite", state_path=tmp_path / "state.db", max_workers=1
    ) as pipeline:
        capacity = CapacityResource("disk", max_capacity=1)
        pipeline.register_resource(capacity)
        pipeline.register_task("work", write_effect_handler, resources={"disk": 1})
        pipeline.enqueue(Job("work", "one", payload={"effect": str(effect)}))

        start_calls: list[bool] = []
        stop_fired = False

        class StopBeforeStartProcess:
            def __init__(self, **_kwargs):
                nonlocal stop_fired
                stop_fired = True
                pipeline.stop()
                self.exitcode = None

            def start(self) -> None:
                start_calls.append(True)

            def is_alive(self) -> bool:
                return False

            def join(self, timeout=None) -> None:
                pass

            def kill(self) -> None:
                pass

            def close(self) -> None:
                pass

        install_fake_process_context(pipeline, StopBeforeStartProcess)
        pipeline.run()

        assert stop_fired
        assert start_calls == []
        with StateSnapshot.open(pipeline) as snapshot:
            job = snapshot.get_job("work::one")
            assert job is not None and job["status"] == "pending"
            assert list(snapshot.iter_attempts(job_uid="work::one")) == []
        assert capacity.used == 0
        assert not effect.exists()


def file_synced_retry_handler(job, ctx):
    ready_path = Path(job.payload["ready_path"])
    release_path = Path(job.payload["release_path"])
    ready_path.write_text("1")
    t0 = time.monotonic()
    while not release_path.exists() and (time.monotonic() - t0 < 5.0):
        time.sleep(0.02)
    return TaskResult.retry(message="transient retry")


def file_synced_fail_handler(job, ctx):
    ready_path = Path(job.payload["ready_path"])
    release_path = Path(job.payload["release_path"])
    ready_path.write_text("1")
    t0 = time.monotonic()
    while not release_path.exists() and (time.monotonic() - t0 < 5.0):
        time.sleep(0.02)
    return TaskResult.failure(message="boom")


_TRIGGER_CALLS = [0]


def counting_trigger(ctx):
    _TRIGGER_CALLS[0] += 1
    return True


def broken_trigger(ctx):
    raise RuntimeError("framework trigger crashed")


class NeverAvailableResource(CapacityResource):
    def can_acquire(self, amount: float) -> tuple[bool, float]:
        return False, 5.0


# ── 测试用例 ──────────────────────────────────────────────────────────


def test_stop_when_idle_is_noop_and_idempotent(tmp_path):
    """无活跃 Run 时调用 stop() 无操作、幂等，不影响后续 Run。"""
    p = TaskLite(backend="memory")
    p.register_task("t", success_handler)
    p.enqueue(Job("t", "j1"))

    # run 外调用 stop() 无操作
    p.stop()
    p.stop()

    # 后续 run 正常执行完毕
    p.run()
    assert "t::j1" in p._runtime.store.backend.load_successes()


def test_stop_with_retry_budget_reset_and_trigger_rearm(tmp_path):
    """停止后收到 retry：预算允许时留 pending，下次 Run 重新评估 Trigger 且预算重置。"""
    _TRIGGER_CALLS[0] = 0

    ready_file = tmp_path / "ready_1"
    release_file = tmp_path / "release_1"

    p = TaskLite(backend="memory", max_workers=1)
    p.register_trigger("counting", counting_trigger)
    p.register_task(
        "t_retry",
        file_synced_retry_handler,
        max_retries_per_run=1,
        trigger="counting",
    )
    p.enqueue(Job("t_retry", "j1", payload={"ready_path": str(ready_file), "release_path": str(release_file)}))

    # 控制线程：等待 worker 启动写出 ready_file，在主进程调用 stop()，然后写 release_file 放行
    def controller():
        t0 = time.monotonic()
        while not ready_file.exists() and (time.monotonic() - t0 < 10.0):
            time.sleep(0.02)
        assert ready_file.exists()
        p.stop()
        release_file.write_text("1")

    t = threading.Thread(target=controller)
    t.start()
    p.run()
    t.join(timeout=5.0)

    # 第 1 次 Run 结束：因 stop() 停止新派发，且单次 retry 在预算内（1 <= 1），保持 pending
    assert "t_retry::j1" not in p._runtime.store.backend.load_successes()
    assert len(p._runtime.store.backend.load_failed()) == 0
    assert any(jd["job_id"] == "j1" for jd in p._runtime.store.backend.load_queue())
    assert _TRIGGER_CALLS[0] == 1

    # 第 2 次 Run：新 Run 重新评估 Trigger，且重试预算重置
    ready_file.unlink()
    release_file.unlink()
    # 释放文件立即可用，使后续 handler 执行后迅速返回 retry 直至耗尽
    release_file.write_text("1")

    p.run()
    assert _TRIGGER_CALLS[0] == 2
    # 预算耗尽后转为失败档案
    failures = p._runtime.store.backend.load_failed()
    assert len(failures) == 1
    assert "t_retry::j1" in failures


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_stop_failure_cascades_and_undispatched_remain_pending(tmp_path, backend):
    """停止后在途失败：下游依赖级联失败，独立未派发作业原样保持 pending。"""
    ready_file = tmp_path / "ready_fail"
    release_file = tmp_path / "release_fail"

    p = TaskLite(backend=backend, state_path=tmp_path / "state.db", max_workers=1)
    p.register_task("t_fail", file_synced_fail_handler)
    p.register_task("t_downstream", success_handler)
    p.register_task("t_other", success_handler)

    p.enqueue(Job("t_fail", "j_fail", payload={"ready_path": str(ready_file), "release_path": str(release_file)}))
    p.enqueue(Job("t_downstream", "j_down", depends_on=["t_fail::j_fail"]))
    p.enqueue(Job("t_other", "j_other"))

    def controller():
        t0 = time.monotonic()
        while not ready_file.exists() and (time.monotonic() - t0 < 10.0):
            time.sleep(0.02)
        assert ready_file.exists()
        p.stop()
        release_file.write_text("1")

    t = threading.Thread(target=controller)
    t.start()
    p.run()
    t.join(timeout=5.0)

    # t_fail::j_fail 失败
    # t_downstream::j_down 级联失败
    # t_other::j_other 未派发（因为 max_workers=1 且收到 stop），保持 pending
    failures = p._runtime.store.backend.load_failed()
    assert "t_fail::j_fail" in failures
    assert "t_downstream::j_down" in failures

    queue = [jd["job_id"] for jd in p._runtime.store.backend.load_queue()]
    assert "j_other" in queue
    assert "t_other::j_other" not in p._runtime.store.backend.load_successes()
    assert "t_other::j_other" not in failures


def test_stop_from_another_thread_wakes_scheduler_wait(tmp_path):
    """另线程调用 stop() 能立即解除主循环等待。"""
    p = TaskLite(backend="memory")

    p.resources["gate"] = NeverAvailableResource("gate", max_capacity=1.0)
    p.register_task("t_wait", success_handler, resources={"gate": 1.0})
    p.enqueue(Job("t_wait", "j1", resources={"gate": 1.0}))

    run_started = threading.Event()
    run_finished = threading.Event()

    def run_worker():
        run_started.set()
        p.run(install_signals=False)
        run_finished.set()

    t = threading.Thread(target=run_worker)
    t.start()
    assert run_started.wait(5.0)

    # 等主循环进入等待
    time.sleep(0.1)

    t0 = time.monotonic()
    p.stop()
    # 唤醒后应在极短时间内退出，远小于 5.0 秒
    assert run_finished.wait(2.0)
    elapsed = time.monotonic() - t0
    assert elapsed < 1.0
    t.join(timeout=2.0)


def test_run_waits_for_settled_worker_process_exit(tmp_path):
    release_path = tmp_path / "release-worker"
    done_path = tmp_path / "worker-exited"
    p = TaskLite(backend="sqlite", state_path=tmp_path / "worker-drain.db")
    p.register_task("lingering", lingering_success_handler)
    p.enqueue(Job(
        "lingering",
        "j1",
        payload={"release_path": str(release_path), "done_path": str(done_path)},
    ))

    finished = threading.Event()
    errors: list[BaseException] = []

    def run_pipeline() -> None:
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    deadline = time.monotonic() + 10.0
    while "lingering::j1" not in p._runtime.store.backend.load_successes() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert "lingering::j1" in p._runtime.store.backend.load_successes()
    assert not finished.is_set(), "有效结果已结算，但 Run 必须等待 worker 进程退出"

    release_path.write_text("release", encoding="utf-8")
    runner.join(timeout=20.0)

    assert not runner.is_alive()
    assert errors == []
    assert done_path.exists()
    p.close()


@pytest.mark.skipif(os.name != "posix", reason="进程组信号仅在 POSIX 平台验证")
def test_process_group_sigterm_drains_worker_and_preserves_settlement(tmp_path):
    state_path = tmp_path / "signal.db"
    release_path = tmp_path / "release-signal"
    done_path = tmp_path / "signal-run-done"
    ready_success = tmp_path / "ready-success"
    ready_failure = tmp_path / "ready-failure"
    project_root = Path(__file__).resolve().parents[2]
    script = tmp_path / "run_signal_pipeline.py"
    script.write_text(
        f"""
import time
from pathlib import Path

from tasklite import Job, TaskLite, TaskResult

RELEASE = Path({str(release_path)!r})


def wait_for_release():
    deadline = time.monotonic() + 20.0
    while not RELEASE.exists() and time.monotonic() < deadline:
        time.sleep(0.01)


def drain_success(job, ctx):
    Path(job.payload["ready"]).write_text("ready", encoding="utf-8")
    wait_for_release()
    ctx.spawn(Job("spawned", "child"))
    return TaskResult.success()


def drain_failure(job, ctx):
    Path(job.payload["ready"]).write_text("ready", encoding="utf-8")
    wait_for_release()
    return TaskResult.failure(message="drain failure")


def unexpected_handler(job, ctx):
    Path(job.payload["marker"]).write_text("ran", encoding="utf-8")
    return TaskResult.success()


def main():
    pipeline = TaskLite(backend="sqlite", state_path={str(state_path)!r}, max_workers=2)
    pipeline.register_task("drain_success", drain_success)
    pipeline.register_task("drain_failure", drain_failure)
    pipeline.register_task("spawned", unexpected_handler)
    pipeline.register_task("dependent", unexpected_handler)
    pipeline.register_task("other", unexpected_handler)
    pipeline.enqueue(Job(
        "drain_success",
        "s1",
        payload={{"ready": {str(ready_success)!r}}},
    ))
    pipeline.enqueue(Job(
        "drain_failure",
        "f1",
        payload={{"ready": {str(ready_failure)!r}}},
    ))
    pipeline.enqueue(Job(
        "dependent",
        "d1",
        depends_on=["drain_failure::f1"],
        payload={{"marker": {str(tmp_path / "dependent-marker")!r}}},
    ))
    pipeline.enqueue(Job(
        "other",
        "o1",
        payload={{"marker": {str(tmp_path / "other-marker")!r}}},
    ))
    pipeline.run()
    Path({str(done_path)!r}).write_text("done", encoding="utf-8")
    pipeline.close()


if __name__ == "__main__":
    main()
""",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)
    process = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 20.0
        while (
            not ready_success.exists() or not ready_failure.exists()
        ) and time.monotonic() < deadline:
            if process.poll() is not None:
                break
            time.sleep(0.01)
        assert ready_success.exists() and ready_failure.exists()

        os.killpg(process.pid, signal.SIGTERM)
        os.killpg(process.pid, signal.SIGTERM)
        release_path.write_text("release", encoding="utf-8")
        stdout, stderr = process.communicate(timeout=30.0)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5.0)

    assert process.returncode == 0, f"stdout={stdout}\nstderr={stderr}"
    assert done_path.exists()
    with StateSnapshot.open(state_path) as state:
        succeeded = {job["job_uid"] for job in state.iter_jobs(status="succeeded")}
        failed = {job["job_uid"] for job in state.iter_jobs(status="failed")}
        pending = {job["job_uid"] for job in state.iter_jobs(status="pending")}
        attempts = {
            job_uid: [
                attempt["outcome"]
                for attempt in state.iter_attempts(job_uid=job_uid)
            ]
            for job_uid in ("drain_success::s1", "drain_failure::f1")
        }

    assert "drain_success::s1" in succeeded
    assert {"drain_failure::f1", "dependent::d1"} <= failed
    assert {"spawned::child", "other::o1"} <= pending
    assert "spawned::child" not in succeeded
    assert "other::o1" not in succeeded
    assert attempts["drain_success::s1"] == ["success"]
    assert attempts["drain_failure::f1"] == ["failure"]
    assert not (tmp_path / "dependent-marker").exists()
    assert not (tmp_path / "other-marker").exists()


def test_repeat_ctrl_c_does_not_upgrade_to_force_abort(tmp_path):
    """多次 Ctrl+C 信号保持 DRAINING，不升级为强制终止。"""
    p = TaskLite(backend="memory")
    runtime = p._runtime

    with running(p):
        assert runtime.stop_mode == StopMode.NONE
        runtime._handle_signal(signal.SIGINT, None)
        assert runtime.stop_mode == StopMode.DRAINING

        # 第二次信号仍然是 DRAINING
        runtime._handle_signal(signal.SIGINT, None)
        assert runtime.stop_mode == StopMode.DRAINING


def test_framework_exception_raises_directly(tmp_path):
    """Trigger 异常直接抛出，不伪装正常排空。"""
    p = TaskLite(backend="memory")

    p.register_trigger("broken", broken_trigger)
    p.register_task("t", success_handler, trigger="broken")
    p.enqueue(Job("t", "j1"))

    with pytest.raises(RuntimeError, match="framework trigger crashed"):
        p.run()


def test_install_signals_in_child_thread_fails_loud_and_false_succeeds(tmp_path):
    """install_signals=True 仅主线程安装，子线程安装抛出；False 由宿主控制。"""
    p = TaskLite(backend="memory")
    p.register_task("t", success_handler)
    p.enqueue(Job("t", "j1"))

    err_captured: list[BaseException] = []

    def run_with_signals():
        try:
            p.run(install_signals=True)
        except BaseException as e:
            err_captured.append(e)

    t = threading.Thread(target=run_with_signals)
    t.start()
    t.join(timeout=5.0)

    assert len(err_captured) == 1
    assert isinstance(err_captured[0], ValueError)

    # 用 install_signals=False 在子线程成功执行
    success_captured = [False]

    def run_without_signals():
        p.run(install_signals=False)
        success_captured[0] = True

    t2 = threading.Thread(target=run_without_signals)
    t2.start()
    t2.join(timeout=5.0)

    assert success_captured[0] is True
    assert "t::j1" in p._runtime.store.backend.load_successes()


def test_main_thread_install_signals_restores_original_handler(tmp_path):
    """主线程正常运行退出后恢复原信号处理器。"""
    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)

    p = TaskLite(backend="memory")
    p.register_task("t", success_handler)
    p.enqueue(Job("t", "j1"))

    p.run(install_signals=True)

    assert signal.getsignal(signal.SIGINT) == original_sigint
    assert signal.getsignal(signal.SIGTERM) == original_sigterm
