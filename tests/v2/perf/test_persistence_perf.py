"""v2 持久层性能回归防护（非功能测试）：热路径 O(1) 增量语义。

锁定两条热路径不随队列规模退化——``commit_job_success`` 删一行 + 插
spawned（空时零插入）、``enqueue_jobs`` 单事务增量插入，均为逐 job
delta 操作而非整表重写。回归形态（commit/enqueue 退化成 save_queue
式 DELETE 全表 + 重插）会让 10k 队列侧延迟膨胀两个数量级以上。

满载环境适配：绝对毫秒预算在宿主机高负载下恒红不可靠，改用
「空库基线 × 宽松常数倍」的相对断言——O(1) 正确实现下两侧延迟同为
一次 ``synchronous=FULL`` fsync 事务 + 常数条语句，比值接近 1；
O(N) 回归下比值远超阈值。每侧取多次采样的最小值（最贴近真实成本、
对调度噪声最不敏感）再比较。
"""
from __future__ import annotations

import time

from tasklite.v2.backend.sqlite_backend import SQLiteStateBackend
from tasklite.v2.models.job import Job

# 相对断言的判别倍数：O(1) 两侧比值 ≈1；全表重写回归下 10k 行重插与
# 单行 delta 的成本比远超本值（两数量级以上），中间无合法灰区。
_SCALE_FACTOR = 25.0
# 基线趋近零时的保护下限（秒）——防止极端快基线把比值断言变成绝对断言。
_FLOOR_SECONDS = 0.05
_SAMPLE_COUNT = 3
_QUEUE_DEPTH = 10_000


def _bench_commit_job_success(backend: SQLiteStateBackend, *, samples: int) -> float:
    """多次采样 commit_job_success 单事务延迟，返回最小值。"""
    best = float("inf")
    for _ in range(samples):
        start = time.perf_counter()
        result = backend.commit_job_success(
            "test::latency_probe",
            result_meta={"ok": True},
            spawned_jobs=[],
            cursor_updates={},
        )
        elapsed = time.perf_counter() - start
        assert result is True, "commit_job_success 必须成功（否则测的是失败路径延迟）"
        best = min(best, elapsed)
    return best


def test_commit_job_success_scales_constant_with_queue_size(tmp_path):
    """10k 队列下 commit_job_success 延迟保持在空库基线的常数倍内。

    delta 语义：删 popped uid（此处不在队列，no-op）+ 插空 spawned +
    写 wall——工作量与队列规模无关。若实现退化为整表重写，10k 队列侧
    每次提交要重写全部行，延迟远超空库侧常数倍。
    """
    baseline_backend = SQLiteStateBackend(tmp_path / "baseline.db")
    baseline = _bench_commit_job_success(baseline_backend, samples=_SAMPLE_COUNT)

    busy_backend = SQLiteStateBackend(tmp_path / "busy.db")
    jobs = [
        Job("test", f"existing_{i}", payload={}).to_dict()
        for i in range(_QUEUE_DEPTH)
    ]
    busy_backend.save_queue(jobs)
    busy = _bench_commit_job_success(busy_backend, samples=_SAMPLE_COUNT)

    assert busy < max(baseline * _SCALE_FACTOR, baseline + _FLOOR_SECONDS), (
        f"commit_job_success 疑似随队列规模退化：10k 队列 {busy * 1000:.1f}ms "
        f"vs 空库基线 {baseline * 1000:.1f}ms（倍数阈值 {_SCALE_FACTOR}）"
    )
    # 队列不受影响：probe uid 不在队列、无 spawned 插入
    assert len(busy_backend.load_queue()) == _QUEUE_DEPTH
    assert "test::latency_probe" in busy_backend.load_wall()


def test_enqueue_jobs_latency_constant_with_queue_size(tmp_path):
    """synchronous=FULL 下增量 enqueue 延迟保持在空库基线的常数倍内。

    enqueue 是交互预算的直接目标路径（应答后断电不丢任务要求 FULL，
    但延迟必须可用）。``enqueue_jobs`` 的契约是单事务增量插入、不做
    DELETE 全表重写——若退化成整表重写，10k 队列侧每次入队成本随
    规模线性增长，被本断言拦截。
    """
    baseline_backend = SQLiteStateBackend(tmp_path / "enqueue_baseline.db")
    warmup = Job("test", "warmup", payload={}).to_dict()
    baseline_backend.save_queue([warmup])

    def _bench_sequential_enqueues(backend: SQLiteStateBackend, tag: str) -> float:
        batch = [
            Job("test", f"{tag}_{i}", payload={}).to_dict() for i in range(50)
        ]
        start = time.perf_counter()
        for job_dict in batch:
            backend.enqueue_jobs([job_dict])
        return (time.perf_counter() - start) / len(batch)

    baseline = _bench_sequential_enqueues(baseline_backend, "probe")

    busy_backend = SQLiteStateBackend(tmp_path / "enqueue_busy.db")
    busy_backend.save_queue([
        Job("test", f"existing_{i}", payload={}).to_dict()
        for i in range(_QUEUE_DEPTH)
    ])
    busy = _bench_sequential_enqueues(busy_backend, "probe")

    assert busy < max(baseline * _SCALE_FACTOR, baseline + _FLOOR_SECONDS), (
        f"enqueue_jobs 疑似随队列规模退化：10k 队列 {busy * 1000:.1f}ms/次 "
        f"vs 空库基线 {baseline * 1000:.1f}ms/次（倍数阈值 {_SCALE_FACTOR}）"
    )
    assert len(busy_backend.load_queue()) == _QUEUE_DEPTH + 50
