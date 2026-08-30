"""Non-functional test: persistence scaling.
Verifies the SQLite backend uses O(1) incremental (delta) operations on the
hot path — ``commit_job_success`` deletes one row by uid and inserts only
spawned jobs, instead of rewriting the entire queue. With 10k queued jobs a
single commit should complete in < 50ms (typically < 5ms).
"""
import time
import pytest
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.models.job import Job

def test_commit_job_success_scales_with_queue_size(tmp_path):
    """非功能测试：队列规模扩大时 commit_job_success 单事务延迟保持在 50ms 内。
    Delta API: ``commit_job_success`` deletes the popped uid (a no-op here
    since "completed_job" is not in the queue) and inserts spawned_jobs
    (empty here) → O(1) work regardless of queue size.
    """
    backend = SQLiteStateBackend(tmp_path / "perf.db")
    # Build a queue with 10000 dummy jobs
    jobs = [Job("test", f"existing_{i}", payload={}).to_dict() for i in range(10000)]
    backend.save_queue(jobs)
    t0 = time.perf_counter()
    result = backend.commit_job_success(
        uid="test::completed_job",
        result_meta={"ok": True},
        spawned_jobs=[],
        cursor_updates={},
    )
    elapsed = time.perf_counter() - t0
    assert result is True, "commit should succeed"
    assert elapsed < 0.05, f"Expected < 50ms, got {elapsed*1000:.1f}ms"
    # Queue unchanged: completed_job was not in it, no spawned_jobs added
    assert len(backend.load_queue()) == 10000

def test_enqueue_latency_budget_under_synchronous_full(tmp_path):
    """性能护栏：synchronous=FULL 下 enqueue 单事务 < 50ms。
    NORMAL→FULL 升级后每次 commit 多一次 WAL fsync——本测试锁定升级
    不破坏交互预算；enqueue 是本次修复的直接目标路径：NORMAL 下 enqueue
    应答后断电任务会静默蒸发且无 job 可重跑，故必须 FULL，但延迟需保持可用。
    """
    from tasklite.backend.sqlite_backend import SQLiteStateBackend
    from tasklite.models.job import Job
    backend = SQLiteStateBackend(tmp_path / "enqueue_perf.db")
    job = Job("test", "latency_probe", payload={"k": "v"}).to_dict()
    # 预热一次（建表/首次页分配），再测稳态单次 enqueue 延迟
    backend.save_queue([job])
    jobs = [Job("test", f"probe_{i}", payload={}).to_dict() for i in range(50)]
    t0 = time.perf_counter()
    for j in jobs:
        backend.save_queue([j])
    elapsed = time.perf_counter() - t0
    per_call = elapsed / len(jobs)
    assert per_call < 0.05, (
        f"Expected enqueue avg < 50ms under synchronous=FULL, got {per_call*1000:.1f}ms"
    )
