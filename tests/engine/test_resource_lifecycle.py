"""内置资源分配、事件等待、三态与生命周期必须验收用例集。

覆盖验收清单：
1. 容量 2 允许两个并发，释放后第三个启动；额外通知可早于原计时唤醒，check-to-wait 间不丢通知。
2. 长等待无周期资源扫描，无恢复时间不能被当永久不可满足。
3. 查询/获取/释放异常使 Run 失败；资源明确不可能则 Job 失败无未执行 Attempt；释放先于最终提交且不在两者间派发。
"""

import threading
import time
from typing import Any

import pytest

from pathlib import Path

from tasklite import (
    Job,
    TaskLite,
    TaskResult,
)
from tasklite.engine.resource import (
    Resource,
    ResourceQueryResult,
    RunNotifier,
)
from tasklite.resource import (
    CapacityResource,
    IntervalResource,
)


def _fast_handler(job, ctx):
    return TaskResult.success(metadata={"ran": job.job_id})


def _sync_capacity_handler(job, ctx):
    data_dir = Path(job.payload["dir"])
    jid = job.job_id
    (data_dir / f"started_{jid}").touch()
    release_flag = data_dir / f"release_{jid}"
    deadline = time.monotonic() + 10.0
    while not release_flag.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    return TaskResult.success()


def test_capacity_two_concurrency_and_third_starts_on_release(tmp_path):
    """容量 2 允许两个并发，释放后第三个启动。"""
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()

    p = TaskLite(
        backend="memory",
        max_workers=4,
    )
    cap = CapacityResource("slots", max_capacity=2.0)
    p.register_resource(cap)

    p.register_task("sync", _sync_capacity_handler, resources={"slots": 1.0})
    payload = {"dir": str(sync_dir)}
    p.enqueue(Job("sync", "j1", payload=payload))
    p.enqueue(Job("sync", "j2", payload=payload))
    p.enqueue(Job("sync", "j3", payload=payload))

    run_thread = threading.Thread(target=p.run, kwargs={"install_signals": False})
    run_thread.start()

    # 等待前两个任务启动并占用容量
    deadline = time.monotonic() + 5.0
    while not ((sync_dir / "started_j1").exists() and (sync_dir / "started_j2").exists()):
        if time.monotonic() > deadline:
            raise TimeoutError("j1 or j2 failed to start")
        time.sleep(0.02)

    # 第三个任务因容量为 2 暂不能启动
    assert not (sync_dir / "started_j3").exists()

    # 释放第一个任务
    (sync_dir / "release_j1").touch()

    # 第三个任务随后立即被唤醒并启动
    deadline = time.monotonic() + 5.0
    while not (sync_dir / "started_j3").exists():
        if time.monotonic() > deadline:
            raise TimeoutError("j3 failed to start after j1 release")
        time.sleep(0.02)

    # 释放剩余任务
    (sync_dir / "release_j2").touch()
    (sync_dir / "release_j3").touch()

    run_thread.join(timeout=5.0)
    assert not run_thread.is_alive()

    successes = p.backend.load_successes()
    assert "sync::j1" in successes
    assert "sync::j2" in successes
    assert "sync::j3" in successes
    assert cap.used == 0.0


def test_check_to_wait_race_does_not_lose_notification():
    """验证 check 与 wait 之间的并发通知不会丢失，立即唤醒。"""
    notifier = RunNotifier()
    # 模拟主循环在快照之后、wait 之前发生通知
    seq = notifier.snapshot()
    notifier.notify()
    # 即使 timeout 给很大，由于 generation counter 已推进，wait 必须立即返回 True
    start = time.monotonic()
    woken = notifier.wait(seq, timeout=10.0)
    elapsed = time.monotonic() - start
    assert woken is True
    assert elapsed < 0.5


def test_extra_notification_wakes_earlier_than_timer(tmp_path):
    """额外通知可早于原计时唤醒。"""
    p = TaskLite(
        backend="memory",
        max_workers=2,
    )
    res = IntervalResource("gate", interval_seconds=100.0)
    p.register_resource(res)
    p.register_task("fast", _fast_handler, resources={"gate": 1.0})

    p.enqueue(Job("fast", "j1"))
    p.enqueue(Job("fast", "j2"))

    # 首作业消费 100s 间隔
    run_thread = threading.Thread(target=p.run, kwargs={"install_signals": False})
    run_thread.start()

    # 等待 j1 完成
    deadline = time.monotonic() + 5.0
    while "fast::j1" not in p.backend.load_successes() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert "fast::j1" in p.backend.load_successes()

    # 此时 j2 需等待 100s。外部提前放行（重设 next_available 并通知唤醒）
    res.next_available = time.monotonic()
    start_wake = time.monotonic()
    res.notify()

    # 主循环应早于 100s（< 2s）被提前唤醒并完成 j2
    deadline = time.monotonic() + 5.0
    while "fast::j2" not in p.backend.load_successes() and time.monotonic() < deadline:
        time.sleep(0.02)

    wake_duration = time.monotonic() - start_wake
    run_thread.join(timeout=5.0)
    assert not run_thread.is_alive()

    assert "fast::j2" in p.backend.load_successes()
    assert wake_duration < 2.0, f"通知必须提前唤醒主循环，实测耗时 {wake_duration}s"


def test_long_wait_no_periodic_scanning(tmp_path):
    """长等待无周期资源扫描：单次计时等待，不进行 1s/0.5s 周期轮询。"""
    p = TaskLite(
        backend="memory",
        max_workers=2,
    )
    res = IntervalResource("long_wait", interval_seconds=3600.0)
    p.register_resource(res)
    res.acquire(1.0)  # 消费时间片使其需等待 3600s
    p.register_task("fast", _fast_handler, resources={"long_wait": 1.0})
    p.enqueue(Job("fast", "j1"))

    # 观察 step() 的调用次数
    step_calls = 0
    orig_step = p._runtime.step

    def count_step(*args, **kwargs):
        nonlocal step_calls
        step_calls += 1
        return orig_step(*args, **kwargs)

    p._runtime.step = count_step  # type: ignore[assignment]

    run_thread = threading.Thread(target=p.run, kwargs={"install_signals": False})
    run_thread.start()

    # 让主循环运行 0.3s
    time.sleep(0.3)
    p.stop()
    run_thread.join(timeout=5.0)

    # step 仅在初次扫描无可运行候选时调用一次，进入长等待，绝不进行高频周期轮询
    assert step_calls <= 3, f"长等待期间不得周期轮询，实测 step 次数 {step_calls}"


def test_unknown_wait_time_not_treated_as_impossible(tmp_path):
    """无恢复时间不能被当永久不可满足：等待外部状态变更通知，不误判死锁/失败。"""
    class ExternalNotifierResource(Resource):
        def __init__(self, name: str):
            super().__init__(name)
            self.ready = False

        def can_acquire(self, amount: float) -> ResourceQueryResult:
            if self.ready:
                return ResourceQueryResult(is_available=True, wait_seconds=0.0)
            # 无恢复时间，等待通知
            return ResourceQueryResult(is_available=False, wait_seconds=None, is_impossible=False)

        def acquire(self, amount: float) -> None:
            pass

        def release(self, amount: float) -> None:
            pass

    p = TaskLite(
        backend="memory",
        max_workers=2,
    )
    ext_res = ExternalNotifierResource("ext")
    p.register_resource(ext_res)
    p.register_task("fast", _fast_handler, resources={"ext": 1.0})
    p.enqueue(Job("fast", "j1"))

    run_thread = threading.Thread(target=p.run, kwargs={"install_signals": False})
    run_thread.start()

    # 处于暂不可用且无恢复时间，不能被推断为死锁或失败
    time.sleep(0.1)
    assert "fast::j1" not in p.backend.load_failed()

    # 外部状态就绪并触发通知
    ext_res.ready = True
    ext_res.notify()

    deadline = time.monotonic() + 5.0
    while "fast::j1" not in p.backend.load_successes() and time.monotonic() < deadline:
        time.sleep(0.02)

    run_thread.join(timeout=5.0)
    assert "fast::j1" in p.backend.load_successes()


def test_impossible_resource_fails_job_without_unexecuted_attempt(tmp_path):
    """资源明确不可能则 Job 失败无未执行 Attempt，且级联下游。"""
    p = TaskLite(
        backend="memory",
        max_workers=2,
    )
    p.register_resource(CapacityResource("slots", max_capacity=2.0))
    p.register_task("fast", _fast_handler)

    # j1 申请 10.0，超过容量 2.0，明确不可满足
    p.enqueue(Job("fast", "j1", resources={"slots": 10.0}))
    p.enqueue(Job("fast", "j2", depends_on=["fast::j1"]))

    p.run(install_signals=False)

    failed = p.backend.load_failed()
    assert "fast::j1" in failed
    assert "fast::j2" in failed

    # 未执行不制造 Attempt
    attempts = p.backend.load_attempts("fast::j1")
    assert attempts == [], f"明确不可满足的作业不得创建 Attempt 记录，实测: {attempts}"


def test_resource_exception_fails_run(tmp_path):
    """查询/获取/释放异常使 Run 失败（框架级故障）。"""
    class BadQueryResource(Resource):
        def can_acquire(self, amount: float) -> ResourceQueryResult:
            raise RuntimeError("query exploded")

        def acquire(self, amount: float) -> None:
            pass

        def release(self, amount: float) -> None:
            pass

    p = TaskLite(backend="memory")
    p.register_resource(BadQueryResource("bad_q"))
    p.register_task("fast", _fast_handler, resources={"bad_q": 1.0})
    p.enqueue(Job("fast", "j1"))

    with pytest.raises(RuntimeError, match="query exploded"):
        p.run(install_signals=False)

    class BadAcquireResource(Resource):
        def can_acquire(self, amount: float) -> ResourceQueryResult:
            return ResourceQueryResult(is_available=True)

        def acquire(self, amount: float) -> None:
            raise RuntimeError("acquire exploded")

        def release(self, amount: float) -> None:
            pass

    p2 = TaskLite(backend="memory")
    p2.register_resource(BadAcquireResource("bad_acq"))
    p2.register_task("fast", _fast_handler, resources={"bad_acq": 1.0})
    p2.enqueue(Job("fast", "j2"))

    with pytest.raises(RuntimeError, match="acquire exploded"):
        p2.run(install_signals=False)

    class BadReleaseResource(Resource):
        def can_acquire(self, amount: float) -> ResourceQueryResult:
            return ResourceQueryResult(is_available=True)

        def acquire(self, amount: float) -> None:
            pass

        def release(self, amount: float) -> None:
            raise RuntimeError("release exploded")

    p3 = TaskLite(backend="memory")
    p3.register_resource(BadReleaseResource("bad_rel"))
    p3.register_task("fast", _fast_handler, resources={"bad_rel": 1.0})
    p3.enqueue(Job("fast", "j3"))

    with pytest.raises(RuntimeError, match="release exploded"):
        p3.run(install_signals=False)


def test_release_prior_to_final_commit_and_no_dispatch_between(tmp_path):
    """释放先于最终提交且不在两者间派发。"""
    p = TaskLite(backend="memory")
    cap = CapacityResource("slots", max_capacity=1.0)
    p.register_resource(cap)

    events: list[str] = []

    orig_release = cap.release
    def hooked_release(amount):
        events.append("released")
        orig_release(amount)
    cap.release = hooked_release  # type: ignore[assignment]

    orig_commit = p.backend.commit_job_success
    orig_settle = p.backend.commit_job_settlement
    def hooked_commit(*args, **kwargs):
        events.append("committed")
        return orig_commit(*args, **kwargs)
    def hooked_settle(*args, **kwargs):
        events.append("committed")
        return orig_settle(*args, **kwargs)
    p.backend.commit_job_success = hooked_commit  # type: ignore[assignment]
    p.backend.commit_job_settlement = hooked_settle  # type: ignore[assignment]

    orig_spawn = p._runtime._dispatch.dispatch_next
    def hooked_dispatch(*args, **kwargs):
        events.append("dispatched")
        return orig_spawn(*args, **kwargs)
    p._runtime._dispatch.dispatch_next = hooked_dispatch  # type: ignore[assignment]

    p.register_task("fast", _fast_handler, resources={"slots": 1.0})
    p.enqueue(Job("fast", "j1"))

    p.run(install_signals=False)

    assert "released" in events
    assert "committed" in events
    rel_idx = events.index("released")
    com_idx = events.index("committed")
    assert rel_idx < com_idx, f"释放必须先于最终提交: {events}"
    # 两者之间不得有派发
    between = events[rel_idx + 1:com_idx]
    assert "dispatched" not in between, f"释放与提交之间不得派发: {between}"
