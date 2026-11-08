"""Run 生命周期、正常停止与信号调度控制测试。

锁定行为：
- stop() 跨线程请求并唤醒，重复 stop/Ctrl+C 幂等，不升级强停；无 Run 时无操作；
- 停止后收到 retry：预算允许时保持 pending，下次 Run 重判 Trigger 且预算重置；耗尽为 failed；
- 停止后收到失败/超时：依赖下游级联失败，其余保持 pending；新派生不派发；
- install_signals=True 仅主线程安装，子线程安装抛出，正常退出恢复原处理器；False 由宿主控制；
- 框架异常直接抛出，不伪装正常排空。
"""

from __future__ import annotations

import signal
import threading
import time
from pathlib import Path

import pytest

from tasklite.engine.resource import CapacityResource
from tasklite.engine.types import ExecutionOptions, StopMode
from tasklite.models.job import Job
from tasklite.models.result import TaskResult
from tasklite.pipeline import TaskLite
from tasklite.testing import running


# ── 模块级 handler（真实跨进程协作）─────────────────────────────────────


def success_handler(job, ctx):
    return TaskResult.success()


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
    p = TaskLite("p_idle", state_dir=tmp_path / "state", backend="memory")
    p.register_task("t", success_handler)
    p.enqueue(Job("t", "j1"))

    # run 外调用 stop() 无操作
    p.stop()
    p.stop()

    # 后续 run 正常执行完毕
    p.run()
    assert "t::j1" in p.backend.load_successes()


def test_stop_with_retry_budget_reset_and_trigger_rearm(tmp_path):
    """停止后收到 retry：预算允许时留 pending，下次 Run 重新评估 Trigger 且预算重置。"""
    _TRIGGER_CALLS[0] = 0

    ready_file = tmp_path / "ready_1"
    release_file = tmp_path / "release_1"

    p = TaskLite("p_retry", state_dir=tmp_path / "state", backend="memory", max_workers=1)
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
    assert "t_retry::j1" not in p.backend.load_successes()
    assert len(p.list_failures()) == 0
    assert any(jd["job_id"] == "j1" for jd in p.backend.load_queue())
    assert _TRIGGER_CALLS[0] == 1

    # 第 2 次 Run：新 Run 重新评估 Trigger，且重试预算重置
    ready_file.unlink()
    release_file.unlink()
    # 释放文件立即可用，使后续 handler 执行后迅速返回 retry 直至耗尽
    release_file.write_text("1")

    p.run()
    assert _TRIGGER_CALLS[0] == 2
    # 预算耗尽后转为失败档案
    failures = p.list_failures()
    assert len(failures) == 1
    assert failures[0].job_uid == "t_retry::j1"


def test_stop_failure_cascades_and_undispatched_remain_pending(tmp_path):
    """停止后在途失败：下游依赖级联失败，独立未派发作业原样保持 pending。"""
    ready_file = tmp_path / "ready_fail"
    release_file = tmp_path / "release_fail"

    p = TaskLite("p_fail", state_dir=tmp_path / "state", backend="memory", max_workers=1)
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
    failures = {f.job_uid: f for f in p.list_failures()}
    assert "t_fail::j_fail" in failures
    assert "t_downstream::j_down" in failures

    queue = [jd["job_id"] for jd in p.backend.load_queue()]
    assert "j_other" in queue
    assert "t_other::j_other" not in p.backend.load_successes()
    assert "t_other::j_other" not in failures


def test_stop_from_another_thread_wakes_scheduler_wait(tmp_path):
    """另线程调用 stop() 能立即解除主循环等待。"""
    p = TaskLite("p_wake", state_dir=tmp_path / "state", backend="memory")

    p.resources["gate"] = NeverAvailableResource("gate", 1.0)
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


def test_repeat_ctrl_c_does_not_upgrade_to_force_abort(tmp_path):
    """多次 Ctrl+C 信号保持 DRAINING，不升级为强制终止。"""
    p = TaskLite("p_sig", state_dir=tmp_path / "state", backend="memory")
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
    p = TaskLite("p_crash", state_dir=tmp_path / "state", backend="memory")

    p.register_trigger("broken", broken_trigger)
    p.register_task("t", success_handler, trigger="broken")
    p.enqueue(Job("t", "j1"))

    with pytest.raises(RuntimeError, match="framework trigger crashed"):
        p.run()


def test_install_signals_in_child_thread_fails_loud_and_false_succeeds(tmp_path):
    """install_signals=True 仅主线程安装，子线程安装抛出；False 由宿主控制。"""
    p = TaskLite("p_thread", state_dir=tmp_path / "state", backend="memory")
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
    assert "t::j1" in p.backend.load_successes()


def test_main_thread_install_signals_restores_original_handler(tmp_path):
    """主线程正常运行退出后恢复原信号处理器。"""
    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)

    p = TaskLite("p_restore", state_dir=tmp_path / "state", backend="memory")
    p.register_task("t", success_handler)
    p.enqueue(Job("t", "j1"))

    p.run(install_signals=True)

    assert signal.getsignal(signal.SIGINT) == original_sigint
    assert signal.getsignal(signal.SIGTERM) == original_sigterm
