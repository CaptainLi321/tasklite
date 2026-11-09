"""v2 并发执行回归（非功能测试）：多作业真并发、workers 上限、并发下
依赖链与资源容量限流。

主循环为事件驱动的 submit/drain 模型，``max_workers`` 经内部
``__workers__`` CapacityResource 控制并发度。handler 以文件屏障保持
执行窗口，测试通过开始/结束窗口与显式进入握手观察峰值并发。

时序敏感：满载环境按 AGENTS.md 红线 1 的 TLE 容忍处理。语义判据不依赖
固定 sleep 或总耗时，只以执行窗口重叠和文件屏障握手为准；超时仅作看门狗。
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest

from tasklite import Job, TaskLite, TaskResult
from tasklite.resource import CapacityResource

PROJECT_ROOT = str(Path(__file__).resolve().parents[3])


@pytest.fixture(autouse=True)
def _subprocess_pythonpath(monkeypatch):
    """子进程按包限定名 import 本模块（handler 反序列化）所需路径环境。"""
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    monkeypatch.setenv("PYTHONPATH", PROJECT_ROOT)


def barrier_window_handler(job, ctx):
    """进入后等待释放文件，并落盘执行时间窗（模块级可 pickle）。"""
    start = time.time()
    window_file = ctx.output_roots / f"window_{job.job_id}.txt"
    window_file.parent.mkdir(parents=True, exist_ok=True)
    window_file.write_text(f"{start}", encoding="utf-8")

    release_path = Path(job.payload["release_path"])
    deadline = time.monotonic() + 20.0
    while not release_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not release_path.exists():
        raise RuntimeError(f"barrier release timed out for {job.job_id}")

    end = time.time()
    window_file.write_text(f"{start} {end}", encoding="utf-8")
    return TaskResult.success()


def fast_handler(job, ctx):
    """快速 handler，用于依赖链测试（模块级可 pickle）。"""
    return TaskResult.success(metadata={"processed": job.job_id})


def _load_windows(output_roots: Path) -> list[tuple[float, float]]:
    """读取全部执行时间窗文件，返回 (start, end) 列表。"""
    windows = []
    for path in sorted(output_roots.glob("window_*.txt")):
        start_str, end_str = path.read_text(encoding="utf-8").split()
        windows.append((float(start_str), float(end_str)))
    return windows


def _entered_jobs(output_roots: Path) -> set[str]:
    return {
        path.stem.removeprefix("window_")
        for path in output_roots.glob("window_*.txt")
    }


def _wait_for_entered(
    output_roots: Path,
    expected: int,
    *,
    timeout: float = 20.0,
) -> set[str]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        entered = _entered_jobs(output_roots)
        if len(entered) >= expected:
            return entered
        time.sleep(0.01)
    raise AssertionError(
        f"only {_entered_jobs(output_roots)} entered, expected at least {expected}"
    )


def _barrier_job(
    task_type: str,
    job_id: str,
    release_dir: Path,
) -> Job:
    return Job(
        task_type,
        job_id,
        payload={"release_path": str(release_dir / job_id)},
    )


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


def test_task_default_resources_limit_real_workers(tmp_state_dir):
    """Task 默认资源在真实派发与候选准入中生效。"""
    output_roots = tmp_state_dir / "output"
    release_dir = tmp_state_dir / "release-default"
    release_dir.mkdir()
    pipeline = TaskLite(
        backend="sqlite",
        state_path=tmp_state_dir / "test_sched_ref.db",
        output_roots=output_roots,
        max_workers=2,
    )
    pipeline.register_resource(CapacityResource("mem", max_capacity=1.0))
    pipeline.register_task(
        "defaulted",
        barrier_window_handler,
        resources={"mem": 1.0},
    )
    pipeline.enqueue(_barrier_job("defaulted", "j0", release_dir))
    pipeline.enqueue(_barrier_job("defaulted", "j1", release_dir))

    errors: list[BaseException] = []

    def run_pipeline() -> None:
        try:
            pipeline.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        entered = _wait_for_entered(output_roots, 1)
        assert entered == {"j0"}, "Task 默认资源未限制并发候选"
        (release_dir / "j0").write_text("release", encoding="utf-8")
        assert _wait_for_entered(output_roots, 2) == {"j0", "j1"}
    finally:
        for job_id in ("j0", "j1"):
            (release_dir / job_id).touch(exist_ok=True)
        runner.join(timeout=20.0)
        pipeline.close()

    assert not runner.is_alive()
    assert errors == []
    assert _peak_overlap(_load_windows(output_roots)) == 1


def test_three_jobs_run_concurrently(tmp_state_dir):
    """3 个文件屏障 job + max_workers=3 真并发。

    三执行窗存在公共重叠；串行执行时后窗的 start 必晚于前窗的 end。
    """
    output_roots = tmp_state_dir / "output"
    release_dir = tmp_state_dir / "release-three"
    release_dir.mkdir()
    pipeline = TaskLite(
        backend="sqlite",
        state_path=tmp_state_dir / "test_concurrency.db",
        output_roots=output_roots,
        max_workers=3,
    )
    pipeline.register_resource(CapacityResource("cpu", max_capacity=3.0))
    pipeline.register_task("barrier", barrier_window_handler, resources={"cpu": 1.0})
    for i in range(3):
        pipeline.enqueue(_barrier_job("barrier", f"j{i}", release_dir))

    errors: list[BaseException] = []

    def run_pipeline() -> None:
        try:
            pipeline.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        assert _wait_for_entered(output_roots, 3) == {"j0", "j1", "j2"}
        assert runner.is_alive()
    finally:
        for job_id in ("j0", "j1", "j2"):
            (release_dir / job_id).touch(exist_ok=True)
        runner.join(timeout=20.0)
        pipeline.close()

    assert not runner.is_alive()
    assert errors == []
    windows = _load_windows(output_roots)
    assert len(windows) == 3, "三个作业都应落盘执行窗口"
    assert _peak_overlap(windows) == 3, (
        "max_workers=3 时三个作业必须存在同一时刻全部在执行的窗口"
    )


def test_workers_resource_limits_concurrency(tmp_state_dir):
    """max_workers=2 时 4 个文件屏障作业的最大并发重叠数必须等于 2。

    第一批恰好进入两个作业后释放，再以第二批进入握手完成后半段；
    峰值重叠数 >2 即限流失效，<2 即过度串行化。
    """
    output_roots = tmp_state_dir / "output"
    release_dir = tmp_state_dir / "release-workers"
    release_dir.mkdir()
    pipeline = TaskLite(
        backend="sqlite",
        state_path=tmp_state_dir / "test_workers_limit.db",
        output_roots=output_roots,
        max_workers=2,
    )
    pipeline.register_task("barrier", barrier_window_handler)
    for i in range(4):
        pipeline.enqueue(_barrier_job("barrier", f"j{i}", release_dir))

    errors: list[BaseException] = []

    def run_pipeline() -> None:
        try:
            pipeline.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        first_wave = _wait_for_entered(output_roots, 2)
        assert first_wave == {"j0", "j1"}, f"首批并发数错误: {first_wave}"
        for job_id in first_wave:
            (release_dir / job_id).touch(exist_ok=True)
        assert _wait_for_entered(output_roots, 4) == {"j0", "j1", "j2", "j3"}
    finally:
        for i in range(4):
            (release_dir / f"j{i}").touch(exist_ok=True)
        runner.join(timeout=20.0)
        pipeline.close()

    assert not runner.is_alive()
    assert errors == []
    windows = _load_windows(output_roots)
    assert len(windows) == 4
    assert _peak_overlap(windows) == 2, (
        "max_workers=2 必须把最大并发重叠钳制在 2（限流失效会 >2，"
        "过度串行化会 <2）"
    )


def test_dependency_chain_under_concurrency(tmp_state_dir):
    """依赖链 A → B → C 在 max_workers=3 下仍按序执行。

    依赖强制串行：B 等 A commit 到 successes，C 等 B。即使有 3 个 worker
    槽位，三者也不能并行——以 attempts 轨迹的时序边界锁定（下游
    started_at 不得早于上游 finished_at）。
    """
    pipeline = TaskLite(
        backend="sqlite",
        state_path=tmp_state_dir / "test_dep_chain.db",
        max_workers=3,
    )
    pipeline.register_task("fast", fast_handler)
    pipeline.enqueue(Job("fast", "A"))
    pipeline.enqueue(Job("fast", "B", depends_on=["fast::A"]))
    pipeline.enqueue(Job("fast", "C", depends_on=["fast::B"]))
    pipeline.run()

    successes = pipeline.backend.load_successes()
    assert "fast::A" in successes
    assert "fast::B" in successes
    assert "fast::C" in successes

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
    """CapacityResource("mem", max=2.0) + 3 个文件屏障作业 + max_workers=3。

    资源容量限制并发：3 个作业各占 mem 1.0、上限 2.0 → 峰值重叠必须
    等于 2（资源限制失效会 =3，槽位饥饿会 <2）。
    """
    output_roots = tmp_state_dir / "output"
    release_dir = tmp_state_dir / "release-capacity"
    release_dir.mkdir()
    pipeline = TaskLite(
        backend="sqlite",
        state_path=tmp_state_dir / "test_cap_respected.db",
        output_roots=output_roots,
        max_workers=3,
    )
    pipeline.register_resource(CapacityResource("mem", max_capacity=2.0))
    pipeline.register_task(
        "barrier",
        barrier_window_handler,
        resources={"mem": 1.0},
    )
    for i in range(3):
        pipeline.enqueue(_barrier_job("barrier", f"j{i}", release_dir))

    errors: list[BaseException] = []

    def run_pipeline() -> None:
        try:
            pipeline.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        first_wave = _wait_for_entered(output_roots, 2)
        assert first_wave == {"j0", "j1"}, f"容量首批并发数错误: {first_wave}"
        for job_id in first_wave:
            (release_dir / job_id).touch(exist_ok=True)
        assert _wait_for_entered(output_roots, 3) == {"j0", "j1", "j2"}
    finally:
        for i in range(3):
            (release_dir / f"j{i}").touch(exist_ok=True)
        runner.join(timeout=20.0)
        pipeline.close()

    assert not runner.is_alive()
    assert errors == []
    windows = _load_windows(output_roots)
    assert len(windows) == 3
    assert _peak_overlap(windows) == 2, (
        "mem 容量 2.0 必须把最大并发重叠钳制在 2"
    )
