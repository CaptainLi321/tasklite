"""v2 并发执行回归（非功能测试）：多作业真并发、workers 上限、并发下
依赖链、资源容量限流与防忙转护栏。

主循环为事件驱动的 submit/drain 模型，``max_workers`` 经内部
``__workers__`` CapacityResource 控制并发度。若回归到串行执行，执行
窗口重叠断言与限流峰值断言会失败。

时序敏感：满载环境按 AGENTS.md 红线 1 的 TLE 容忍处理。壁钟断言已按
宽松余量改写（上界取串行预算的 3 倍）；判别力主担改由确定性证据承担——
handler 落盘执行时间窗 (start, end)，按事件扫求最大重叠并发数：真并发
/限流语义与宿主机负载无关（满载只会拉长窗口、不会消除或凭空制造重叠），
壁钟断言仅作辅助。
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest

from tasklite.v2 import CapacityResource, Job, TaskLite

PROJECT_ROOT = str(Path(__file__).resolve().parents[3])


@pytest.fixture(autouse=True)
def _subprocess_pythonpath(monkeypatch):
    """子进程按包限定名 import 本模块（handler 反序列化）所需路径环境。"""
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    monkeypatch.setenv("PYTHONPATH", PROJECT_ROOT)


def slow_window_handler(job, ctx):
    """sleep 0.5 并把执行时间窗落盘（模块级可 pickle；窗口供重叠断言）。"""
    start = time.time()
    time.sleep(0.5)
    end = time.time()
    window_file = ctx.output_root / f"window_{job.job_id}.txt"
    window_file.write_text(f"{start} {end}", encoding="utf-8")
    return True, {}


def fast_handler(job, ctx):
    """快速 handler，用于依赖链测试（模块级可 pickle）。"""
    return True, {"processed": job.job_id}


def _load_windows(output_root: Path) -> list[tuple[float, float]]:
    """读取全部执行时间窗文件，返回 (start, end) 列表。"""
    windows = []
    for path in sorted(output_root.glob("window_*.txt")):
        start_str, end_str = path.read_text(encoding="utf-8").split()
        windows.append((float(start_str), float(end_str)))
    return windows


def _peak_overlap(windows: list[tuple[float, float]]) -> int:
    """按事件扫求时间窗的最大并发重叠数（端点相接不计为重叠）。"""
    events: list[tuple[float, int]] = []
    for start, end in windows:
        events.append((start, 1))
        events.append((end, -1))
    events.sort()
    current = peak = 0
    for _, delta in events:
        current += delta
        peak = max(peak, current)
    return peak


def test_scheduler_shares_task_registry_reference(tmp_state_dir):
    """调度器必须持有 Task 注册表的**同一引用**（Task 默认资源可见性）。

    注册表按引用共享：ResourceManager(tasks=...) 拿到 TaskRegistry 活
    引用，register_task 注册的默认资源对调度扫描即时可见；若装配期
    传了拷贝或 None，Task 默认资源对调度侧不可见（容量/限速绕过）。
    同时锁定合并语义：Task 默认 ∪ job 声明，声明值覆盖默认值。
    """
    pipeline = TaskLite(
        name="test_sched_ref", state_dir=tmp_state_dir, backend="sqlite",
    )
    pipeline.register_resource(CapacityResource("mem", max_capacity=2.0))
    pipeline.register_task("fast_task", fast_handler, default_resources={"mem": 1.0})

    resource_mgr = pipeline._runtime.scheduler.resource_mgr
    assert resource_mgr is pipeline.resources
    assert resource_mgr.tasks is pipeline.tasks

    merged = resource_mgr.effective_resources("fast_task", {})
    assert merged.get("mem") == 1.0
    overridden = resource_mgr.effective_resources("fast_task", {"mem": 3.0})
    assert overridden["mem"] == 3.0


def test_three_jobs_run_concurrently(tmp_state_dir):
    """3 个 sleep(0.5) job + max_workers=3 真并发。

    主判据（负载无关）：三执行窗存在公共重叠（串行执行时后窗的 start
    必晚于前窗的 end，重叠恒不存在）。辅助壁钟断言上界取串行预算 1.5s
    的 3 倍，仅容忍 spawn/调度开销，不承担串行回归判别。
    """
    pipeline = TaskLite(
        name="test_concurrency",
        state_dir=tmp_state_dir,
        backend="sqlite",
        output_root=tmp_state_dir / "output",
        max_workers=3,
    )
    pipeline.register_resource(CapacityResource("cpu", max_capacity=3.0))
    pipeline.register_task("slow", slow_window_handler, default_resources={"cpu": 1.0})
    pipeline.enqueue([Job("slow", f"j{i}") for i in range(3)])

    start = time.perf_counter()
    pipeline.run()
    elapsed = time.perf_counter() - start

    windows = _load_windows(tmp_state_dir / "output")
    assert len(windows) == 3, "三个作业都应落盘执行窗口"
    assert _peak_overlap(windows) == 3, (
        "max_workers=3 时三个作业必须存在同一时刻全部在执行的窗口"
    )
    assert elapsed < 4.5, f"并发执行应在串行预算 3 倍内完成，got {elapsed:.2f}s"


def test_workers_resource_limits_concurrency(tmp_state_dir):
    """max_workers=2 时 4 个作业的最大并发重叠数必须等于 2。

    验证 ``__workers__`` 资源真的限制了并发度（而非只是声明）：峰值
    重叠数 >2 即限流失效，<2 即过度串行化。壁钟区间仅作辅助（下界
    0.9s 判别限流存在——两批 sleep(0.5) 的壁钟下限，满载只会更长；
    上界取 v1 阈值的 3 倍容忍满载 spawn 开销）。
    """
    pipeline = TaskLite(
        name="test_workers_limit",
        state_dir=tmp_state_dir,
        backend="sqlite",
        output_root=tmp_state_dir / "output",
        max_workers=2,
    )
    pipeline.register_task("slow", slow_window_handler)
    pipeline.enqueue([Job("slow", f"j{i}") for i in range(4)])

    start = time.perf_counter()
    pipeline.run()
    elapsed = time.perf_counter() - start

    windows = _load_windows(tmp_state_dir / "output")
    assert len(windows) == 4
    assert _peak_overlap(windows) == 2, (
        "max_workers=2 必须把最大并发重叠钳制在 2（限流失效会 >2，"
        "过度串行化会 <2）"
    )
    assert 0.9 <= elapsed < 5.7, (
        f"两批限流执行应在 [0.9, 5.7)s 内，got {elapsed:.2f}s"
    )


def test_dependency_chain_under_concurrency(tmp_state_dir):
    """依赖链 A → B → C 在 max_workers=3 下仍按序执行。

    依赖强制串行：B 等 A commit 到 wall，C 等 B。即使有 3 个 worker
    槽位，三者也不能并行——以 attempts 轨迹的时序边界锁定（下游
    started_at 不得早于上游 finished_at）。
    """
    pipeline = TaskLite(
        name="test_dep_chain", state_dir=tmp_state_dir, backend="sqlite",
        max_workers=3,
    )
    pipeline.register_task("fast", fast_handler)
    pipeline.enqueue([
        Job("fast", "A"),
        Job("fast", "B", depends_on=["fast::A"]),
        Job("fast", "C", depends_on=["fast::B"]),
    ])
    pipeline.run()

    wall = pipeline.backend.load_wall()
    assert "fast::A" in wall
    assert "fast::B" in wall
    assert "fast::C" in wall

    traces = {
        uid: pipeline.backend.load_attempts(uid)[0]
        for uid in ("fast::A", "fast::B", "fast::C")
    }
    assert traces["fast::A"].finished_at <= traces["fast::B"].started_at, (
        "B 必须在 A 终态提交之后才开始执行"
    )
    assert traces["fast::B"].finished_at <= traces["fast::C"].started_at, (
        "C 必须在 B 终态提交之后才开始执行"
    )


def test_resource_capacity_respected_under_concurrency(tmp_state_dir):
    """CapacityResource("mem", max=2.0) + 3 个各占 1.0 的作业 + max_workers=3。

    资源容量限制并发：3 个作业各占 mem 1.0、上限 2.0 → 峰值重叠必须
    等于 2（资源限制失效会 =3，槽位饥饿会 <2）。壁钟区间同 workers
    上限测试的满载适配。
    """
    pipeline = TaskLite(
        name="test_cap_respected",
        state_dir=tmp_state_dir,
        backend="sqlite",
        output_root=tmp_state_dir / "output",
        max_workers=3,
    )
    pipeline.register_resource(CapacityResource("mem", max_capacity=2.0))
    pipeline.register_task("slow", slow_window_handler, default_resources={"mem": 1.0})
    pipeline.enqueue([Job("slow", f"j{i}") for i in range(3)])

    start = time.perf_counter()
    pipeline.run()
    elapsed = time.perf_counter() - start

    windows = _load_windows(tmp_state_dir / "output")
    assert len(windows) == 3
    assert _peak_overlap(windows) == 2, (
        "mem 容量 2.0 必须把最大并发重叠钳制在 2"
    )
    assert 0.9 <= elapsed < 5.7, (
        f"容量限流执行应在 [0.9, 5.7)s 内，got {elapsed:.2f}s"
    )


def test_workers_suspended_no_inflight_sleeps_not_busy_loop(tmp_state_dir, monkeypatch):
    """``__workers__`` 被挂起且无 in-flight 时主循环必须 sleep。

    场景：填池预检 can_acquire 返回 False（资源被挂起），且队列无
    in-flight job 可 drain——若无 worker 等待保护的等待分支，等待决策
    全部不命中 → 无限忙循环（100% CPU）。验证：monkeypatch time.sleep
    后 run() 在 stop 前至少 sleep 一次。
    """
    pipeline = TaskLite(
        name="test_workers_suspend", state_dir=tmp_state_dir, backend="sqlite",
        max_workers=2,
    )
    pipeline.register_task("slow", fast_handler)
    pipeline.enqueue([Job("slow", "j1")])
    # 挂起 __workers__：can_acquire 返回 False 且没有 job 被派发（无 in-flight）
    pipeline.resources["__workers__"].suspend(60.0)

    real_sleep = time.sleep
    slept: list[float] = []
    # 只捕获主循环内部对 time.sleep 的调用；测试自身轮询用 real_sleep
    # 绕过捕获（否则 slept 会被轮询填充，assert slept 恒真 → 忙循环回归
    # 无法被检出，属假绿）。
    monkeypatch.setattr("time.sleep", lambda seconds: slept.append(seconds))

    def run_pipeline() -> None:
        pipeline.run()

    thread = threading.Thread(target=run_pipeline)
    thread.start()
    deadline = time.time() + 10.0
    while not slept and time.time() < deadline:
        real_sleep(0.02)
    pipeline.stop(force=True)
    thread.join(timeout=5.0)

    assert not thread.is_alive(), "stop(force) 后 run() 必须退出——无无限忙循环"
    assert slept, "workers 挂起 + 无 in-flight 时主循环必须进入等待睡眠，而非忙循环"


def test_custom_worker_resource_zero_wait_no_busy_loop(tmp_state_dir, monkeypatch):
    """自定义 ``__workers__`` 资源在不可用态返回 ``(False, 0.0)`` 时主循环
    必须至少 sleep 一次（等待下限钳制），而非无限忙循环。

    内置 CapacityResource / RateLimitResource 在不可用态恒返回 wait>0，
    不受影响；本护栏专防「自定义资源 wait=0 → worker 等待分支全部不
    命中 → 无限忙循环空烧 CPU、stop 响应下降」。
    """

    class ZeroWaitResource(CapacityResource):
        """不可用态恒返回 (False, 0.0) 的自定义 worker 资源。"""

        def __init__(self) -> None:
            super().__init__("__workers__", 2.0)

        def can_acquire(self, amount: float) -> tuple[bool, float]:
            return False, 0.0

    pipeline = TaskLite(
        name="test_custom_worker_zero", state_dir=tmp_state_dir, backend="sqlite",
        max_workers=2,
    )
    pipeline.register_task("slow", fast_handler)
    pipeline.enqueue([Job("slow", "j1")])
    pipeline.resources["__workers__"] = ZeroWaitResource()

    real_sleep = time.sleep
    slept: list[float] = []
    monkeypatch.setattr("time.sleep", lambda seconds: slept.append(seconds))

    thread = threading.Thread(target=pipeline.run)
    thread.start()
    deadline = time.time() + 10.0
    while not slept and time.time() < deadline:
        real_sleep(0.02)
    pipeline.stop(force=True)
    thread.join(timeout=5.0)

    assert not thread.is_alive(), "stop(force) 后 run() 必须退出——无无限忙循环"
    assert slept, (
        "自定义 worker 资源返回 (False, 0.0) 必须经等待下限钳制触发 sleep，"
        "而非忙循环"
    )
