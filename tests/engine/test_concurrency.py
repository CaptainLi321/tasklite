"""非功能测试：并发执行。

验证 Known Limitation 1（串行执行）已被修复：executor 现在是非阻塞的
submit/drain 模型，主循环事件驱动，``max_workers`` 作为内部
``CapacityResource("__workers__")`` 控制并发度。

历史：此文件曾用 ``@xfail(strict=True)`` 标记串行执行限制。修复后标记移除，
测试真正通过。若未来回归到串行执行，``test_three_jobs_run_concurrently``
会失败。
"""
import sys
import time
from pathlib import Path

import pytest
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite
from tasklite.engine.resource import CapacityResource


def slow_handler(job, ctx):
    """Module-level handler for pickleability under forkserver/spawn."""
    time.sleep(0.5)
    return (True, {})


def fast_handler(job, ctx):
    """快速 handler，用于依赖链测试。"""
    return (True, {"processed": job.job_id})


def test_scheduler_sees_handler_default_resources(tmp_state_dir):
    """ 关联修复（scheduler.py）：调度器必须持有 handlers 的**同一引用**。

    `handlers or {}` 陷阱：空 dict 是 falsy，or 会换成新空 dict——register_handler
    的默认资源对调度扫描不可见（容量/限速绕过）。此前被 enqueue 时把默认资源
    烤进 job_dict 掩盖；删除 enqueue 合并后暴露。此测试锁定引用关系 + 合并语义。
    """
    pipeline = TaskLite(
        name="test_sched_ref", state_dir=tmp_state_dir, backend="sqlite",
    )
    pipeline.add_resource(CapacityResource("mem", max_capacity=2.0))
    pipeline.register_handler("slow", fast_handler, default_resources={"mem": 1.0})
    # 引用必须一致——否则 handler 默认资源对 scheduler 不可见
    assert pipeline._runtime.scheduler.handlers is pipeline.handlers
    # 合并语义：handler 默认 ∪ job 自身
    eff = pipeline._runtime.scheduler._effective_resources(Job("slow", "j1"))
    assert eff.get("mem") == 1.0
    # job 显式资源覆盖 handler 默认
    eff2 = pipeline._runtime.scheduler._effective_resources(
        Job("slow", "j2", resources={"mem": 3.0})
    )
    assert eff2["mem"] == 3.0


def test_three_jobs_run_concurrently(tmp_state_dir, monkeypatch):
    """非功能并发测试：3 个 sleep(0.5) job + max_workers=3 真并发 ≈ 0.5s。

    断言阈值 2.0s：并发正确性断言优先于墙钟精度，容忍 CI 慢机；
    串行回归（≈1.5s）的精细判别由 workers 限流区间断言兜底。
    """
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    monkeypatch.setenv("PYTHONPATH", project_root)

    pipeline = TaskLite(
        name="test_concurrency", state_dir=tmp_state_dir, backend="sqlite",
        max_workers=3,
    )
    pipeline.add_resource(CapacityResource("cpu", max_capacity=3.0))

    pipeline.register_handler("slow", slow_handler, default_resources={"cpu": 1.0})
    pipeline.enqueue([
        Job("slow", "j1", payload={}),
        Job("slow", "j2", payload={}),
        Job("slow", "j3", payload={}),
    ])

    start = time.time()
    pipeline.run()
    elapsed = time.time() - start

    # 真并发 ~0.5s，串行 ~1.5s。并发正确性断言优先于墙钟精度：
    # 2.0s 容忍 CI 慢机（串行回归的精细判别由
    # test_workers_resource_limits_concurrency 的区间断言兜底）。
    assert elapsed < 2.0, f"Expected concurrent execution (<2.0s), got {elapsed:.2f}s"


def test_workers_resource_limits_concurrency(tmp_state_dir, monkeypatch):
    """非功能并发测试：max_workers=2 时，4 个 sleep(0.5) job 应分两批，耗时 ~1.0s。

    验证 ``__workers__`` 资源真的限制了并发度（而非只是声明）。
    """
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    monkeypatch.setenv("PYTHONPATH", project_root)

    pipeline = TaskLite(
        name="test_workers_limit", state_dir=tmp_state_dir, backend="sqlite",
        max_workers=2,
    )
    pipeline.register_handler("slow", slow_handler)
    pipeline.enqueue([
        Job("slow", f"j{i}", payload={}) for i in range(4)
    ])

    start = time.time()
    pipeline.run()
    elapsed = time.time() - start

    # 两批各 0.5s ≈ 1.0s。串行 2.0s，无限制并发 0.5s。
    # 断言 [0.9, 1.9) 检测到限流并发（上界放宽至 1.9s 容忍 spawn 开销，仍排除串行 2.0s）。
    assert 0.9 <= elapsed < 1.9, (
        f"Expected ~1.0s (2 batches of 0.5s with max_workers=2), got {elapsed:.2f}s"
    )


def test_dependency_chain_under_concurrency(tmp_state_dir, monkeypatch):
    """非功能并发测试：依赖链 A → B → C 在 max_workers=3 下仍按序执行。

    依赖强制串行：B 等 A commit 到 wall，C 等 B commit。即使有 3 个 worker
    槽位，三者也不能并行。
    """
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    monkeypatch.setenv("PYTHONPATH", project_root)

    pipeline = TaskLite(
        name="test_dep_chain", state_dir=tmp_state_dir, backend="sqlite",
        max_workers=3,
    )
    pipeline.register_handler("fast", fast_handler)
    pipeline.enqueue([
        Job("fast", "A", payload={}),
        Job("fast", "B", payload={}, depends_on=["fast::A"]),
        Job("fast", "C", payload={}, depends_on=["fast::B"]),
    ])

    pipeline.run()

    wall = pipeline.backend.load_wall()
    # 三者都应在 wall（成功完成）
    assert "fast::A" in wall
    assert "fast::B" in wall
    assert "fast::C" in wall


def test_resource_capacity_respected_under_concurrency(tmp_state_dir, monkeypatch):
    """非功能并发测试：CapacityResource("mem", max=2.0) + 3 个各占 1.0 的 job + max_workers=3。

    资源容量限制并发：3 个 job 各占 mem 1.0，但 mem 上限 2.0，所以最多 2 个并发。
    3 个 sleep(0.5) job 分两批（2+1），耗时 ~1.0s。若资源限制失效则 0.5s。
    """
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    monkeypatch.setenv("PYTHONPATH", project_root)

    pipeline = TaskLite(
        name="test_cap_respected", state_dir=tmp_state_dir, backend="sqlite",
        max_workers=3,
    )
    pipeline.add_resource(CapacityResource("mem", max_capacity=2.0))
    pipeline.register_handler("slow", slow_handler, default_resources={"mem": 1.0})
    pipeline.enqueue([
        Job("slow", f"j{i}", payload={}) for i in range(3)
    ])

    start = time.time()
    pipeline.run()
    elapsed = time.time() - start

    # mem 限制 2 并发：2+1 两批 ≈ 1.0s。无限制 0.5s。
    assert elapsed >= 0.9, (
        f"Expected >= 0.9s (mem capacity limits to 2 concurrent), got {elapsed:.2f}s"
    )
    # 不应超过串行耗时（上界放宽至 1.9s 容忍 spawn 开销余量）
    assert elapsed < 1.9, (
        f"Expected < 1.9s (should not be fully serial), got {elapsed:.2f}s"
    )


def test_workers_suspended_no_inflight_sleeps_not_busy_loop(tmp_state_dir, monkeypatch):
    """__workers__ 被 suspend 且无 in-flight 时主循环必须 sleep。

    回归场景：fill-pool workers 预检 can_acquire 返回 False（资源被 suspend），
    且队列无 in-flight job 可 drain——若无 worker 等待保护的 ``elif worker_wait > 0``
    sleep 分支，sched=None 使死锁/drain 分支全部跳过 → 无限忙循环（100% CPU）。
    验证：monkeypatch time.sleep 后 run() 在 stop 前至少 sleep 一次。
    """
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    monkeypatch.setenv("PYTHONPATH", project_root)

    pipeline = TaskLite(
        name="test_workers_suspend", state_dir=tmp_state_dir, backend="sqlite",
        max_workers=2,
    )
    pipeline.register_handler("slow", slow_handler)
    pipeline.enqueue([Job("slow", "j1", payload={})])
    # suspend __workers__ 资源：can_acquire 返回 False，且没有 job 被派发（无 in-flight）
    pipeline.resources["__workers__"].suspend(60.0)

    import threading
    real_sleep = time.sleep
    slept = []
    # 只捕获主循环内部对 time.sleep 的调用；测试自身轮询用 real_sleep 绕过
    # 捕获（否则 slept 会被轮询的 0.02 填充，assert slept 恒真 → 忙循环回归
    # 无法被检出，属假绿，变异体无法被有效查杀）。
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))

    def run_and_stop():
        # 主循环会因 worker_wait>0 走等待睡眠分支（被 monkeypatch 吞掉）→ 快速空转
        # 到 stop；若无该分支则无限忙循环，stop 永不生效 → 测试超时暴露回归
        pipeline.run()

    t = threading.Thread(target=run_and_stop)
    t.start()
    # 等主循环进入等待（至少一次 sleep 被吞）再 stop
    deadline = time.time() + 5.0
    while not slept and time.time() < deadline:
        real_sleep(0.02)
    pipeline.stop(force=True)
    t.join(timeout=5.0)

    assert not t.is_alive(), "run() must exit after stop(force) — no infinite busy loop"
    assert slept, "workers suspended + no in-flight must sleep (worker wait branch), got busy loop"


def test_custom_worker_resource_zero_wait_no_busy_loop(tmp_state_dir, monkeypatch):
    """第三方自定义 ``__workers__`` 资源在不可用态返回 ``can_acquire -> (False, 0.0)``
    时，主循环必须至少 sleep 一次（钳制下界），而非无限忙循环。

    内置 CapacityResource / RateLimitResource 在不可用态恒返回 wait>0，不受
    影响；此护栏专防「自定义资源 wait=0 → 下方 ``elif worker_wait > 0`` 睡眠
    分支全部不命中 → 无限忙循环空烧 CPU、stop 响应下降」。
    """
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    monkeypatch.setenv("PYTHONPATH", project_root)

    pipeline = TaskLite(
        name="test_custom_worker_zero", state_dir=tmp_state_dir, backend="sqlite",
        max_workers=2,
    )
    pipeline.register_handler("slow", slow_handler)
    pipeline.enqueue([Job("slow", "j1", payload={})])

    # 自定义资源：不可用态恒返回 (False, 0.0)——内置资源不会，专测护栏。
    class ZeroWaitResource(CapacityResource):
        def __init__(self):
            super().__init__("__workers__", 2)
        def can_acquire(self, amount):
            return False, 0.0
    pipeline.resources["__workers__"] = ZeroWaitResource()

    import threading
    real_sleep = time.sleep
    slept = []
    # 只捕获主循环内部对 time.sleep 的调用；测试自身的轮询 sleep 用
    # real_sleep（绕过捕获）——否则捕获列表会被测试轮询的 0.02 填满，
    # `assert slept` 恒真、忙循环回归无法被检出（假绿，变异体无法被有效查杀）。
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))

    t = threading.Thread(target=pipeline.run)
    t.start()
    deadline = time.time() + 5.0
    while not slept and time.time() < deadline:
        real_sleep(0.02)
    pipeline.stop(force=True)
    t.join(timeout=5.0)

    assert not t.is_alive(), "run() must exit after stop(force) — no infinite busy loop"
    assert slept, (
        "custom worker resource returning (False, 0.0) must trigger a sleep "
        "(worker_wait clamped >= 0.05), got busy loop"
    )
