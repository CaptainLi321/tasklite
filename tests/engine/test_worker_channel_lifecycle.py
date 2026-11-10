"""Worker 通信通道结果到达与进程回收分离回归测试。

不变式覆盖：
1. 有效结果到达即结算：真实 worker 返回结果后即使子进程未退出，其他 Job
   仍可结算，Run 返回前等待该进程退出且不改写有效结果；
2. 无结果退出、启动失败与超时各自留下实际失败信息，进程和 Pipe 句柄完整回收；
3. 逐 Job 执行锁与孤儿探测已移除，不产生隐式零预算重试；库级互斥仍有效。
"""

from __future__ import annotations

import multiprocessing as mp
import os
from pathlib import Path
import threading
import time

import pytest

from tasklite import Job, StateSnapshot, TaskLite, TaskResult
from tasklite.resource import CapacityResource
from tasklite.engine.channel import (
    ExecutionChannel,
    WorkerLaunchSpec,
)
from tasklite.models.context import JobContext
from tasklite.utils.lockfile import release_lock, try_acquire_lock


def _lingering_worker(job: Job, ctx: JobContext) -> TaskResult:
    """返回成功结果后在子进程后台线程挂起，模拟 worker 未立即退出。"""
    sync_dir = Path(job.payload["sync_dir"])
    (sync_dir / "job1_returned").write_text("ok", encoding="utf-8")
    
    # 启动后台常驻线程防止子进程自然终结
    def linger():
        time.sleep(3.0)
    
    t = threading.Thread(target=linger, daemon=False)
    t.start()
    return TaskResult.success(metadata={"step": 1})


def _fast_worker(job: Job, ctx: JobContext) -> TaskResult:
    return TaskResult.success(metadata={"fast": True})


def _crash_without_result(job: Job, ctx: JobContext) -> TaskResult:
    os._exit(42)


def _hang_forever(job: Job, ctx: JobContext) -> TaskResult:
    time.sleep(60.0)
    return TaskResult.success(metadata={"unreachable": True})


def _success_then_exit_error(job: Job, ctx: JobContext) -> TaskResult:
    def die_later():
        time.sleep(0.05)
        os._exit(99)
    threading.Thread(target=die_later, daemon=True).start()
    return TaskResult.success(metadata={"returned": True})


def _raise_handler_error(job: Job, ctx: JobContext) -> TaskResult:
    raise ValueError("handler exploded")


def _raise_keyboard_interrupt(job: Job, ctx: JobContext) -> TaskResult:
    raise KeyboardInterrupt()


def _return_invalid_result(job: Job, ctx: JobContext) -> TaskResult:
    return {"status": "success"}  # type: ignore[return-value]


def _wait_until(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


def test_worker_lingers_after_result_does_not_block_dispatch_and_run_waits(tmp_path):
    """结果到达后继续调度其他 Job，Run 结束前等待残留 worker 退出。"""
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir(parents=True, exist_ok=True)

    pipeline = TaskLite(
        backend="sqlite",
        state_path=tmp_path / "channel_separation.db",
        output_roots=tmp_path / "output",
        max_workers=2,
    )
    pipeline.register_task("lingering", _lingering_worker)
    pipeline.register_task("fast", _fast_worker)

    pipeline.enqueue(Job("lingering", "j1", payload={"sync_dir": str(sync_dir)}))
    pipeline.enqueue(Job("fast", "j2"))

    errors: list[BaseException] = []

    def run_pipeline() -> None:
        try:
            pipeline.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    assert _wait_until(lambda: (sync_dir / "job1_returned").exists(), timeout=10.0)
    assert runner.is_alive(), "Run 必须等待仍在运行的已结算 worker"
    runner.join(timeout=10.0)

    assert not runner.is_alive()
    assert errors == []
    successes = pipeline._runtime.store.backend.load_successes()
    assert "lingering::j1" in successes
    assert "fast::j2" in successes
    assert successes["lingering::j1"]["result"]["metadata"]["step"] == 1
    assert successes["fast::j2"]["result"]["metadata"]["fast"] is True

    # 管道与进程资源已回收
    pipeline.stop()
    active_recv = [t for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")]
    assert not active_recv


def test_worker_crash_without_result_records_failure_and_reaps(tmp_path):
    """无结果崩溃子进程留下明确错误信息并回收进程与管道句柄。"""
    with TaskLite(
        backend="sqlite",
        state_path=tmp_path / "channel_crash.db",
        max_workers=1,
    ) as pipeline:
        pipeline.register_task("crash", _crash_without_result)
        pipeline.enqueue(Job("crash", "j1", max_retries_per_run=3))

        pipeline.run()

        failed = pipeline._runtime.store.backend.load_failed()
        assert list(failed) == ["crash::j1"]
        failed_meta = failed["crash::j1"]
        assert failed_meta["result"]["message"] == "Worker exited with code 42 without a result"
        assert failed_meta["result"]["annotations"]["exitcode"] == 42
        assert failed_meta["result"]["metadata"] == {}
        assert "retry_error" not in failed_meta

        attempts = pipeline._runtime.store.backend.load_settled_attempts("crash::j1")
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failure"

        assert not [t for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")]


@pytest.mark.parametrize(
    ("task_type", "handler", "message", "exception_type"),
    [
        ("error", _raise_handler_error, "ValueError: handler exploded", "ValueError"),
        ("interrupt", _raise_keyboard_interrupt, "KeyboardInterrupt", "KeyboardInterrupt"),
        (
            "invalid",
            _return_invalid_result,
            "invalid handler return type: expected TaskResult, got dict",
            None,
        ),
    ],
)
def test_worker_faults_settle_one_failure(
    tmp_path, task_type, handler, message, exception_type
):
    with TaskLite(
        backend="sqlite",
        state_path=tmp_path / f"channel_{task_type}.db",
        max_workers=1,
    ) as pipeline:
        pipeline.register_task(task_type, handler)
        pipeline.enqueue(Job(task_type, "j1", max_retries_per_run=3))

        pipeline.run()

        failed = pipeline._runtime.store.backend.load_failed()
        assert list(failed) == [f"{task_type}::j1"]
        entry = failed[f"{task_type}::j1"]
        assert entry["result"]["message"] == message
        assert entry["error_message"] == message
        if exception_type is None:
            assert entry["result"]["exception"] is None
        else:
            assert entry["result"]["exception"]["type"] == exception_type

        attempts = pipeline._runtime.store.backend.load_settled_attempts(f"{task_type}::j1")
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failure"
        assert pipeline._runtime.store.backend.load_queue() == []


def test_worker_timeout_terminates_and_marks_failed_without_retry(tmp_path):
    """超时结束执行并失败，不自动重试，句柄得到回收。"""
    with TaskLite(
        backend="sqlite",
        state_path=tmp_path / "channel_timeout.db",
        max_workers=1,
    ) as pipeline:
        pipeline.register_task("hang", _hang_forever)
        pipeline.enqueue(Job("hang", "j1", timeout_seconds=0.2, max_retries_per_run=3))

        t0 = time.monotonic()
        pipeline.run()
        elapsed = time.monotonic() - t0

        assert elapsed < 3.0, f"超时未及时触发，耗时 {elapsed}s"

        failed = pipeline._runtime.store.backend.load_failed()
        assert "hang::j1" in failed
        err = failed["hang::j1"].get("error_message") or ""
        assert "timed out" in err
        assert failed["hang::j1"]["result"]["metadata"] == {}
        assert failed["hang::j1"]["result"]["annotations"]["timed_out"] is True

        # 验证超时未烧重试预算或自动重试，一次 Attempt 直接终结
        attempts = pipeline._runtime.store.backend.load_settled_attempts("hang::j1")
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failure"

        assert not [t for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")]


def test_no_per_job_lock_file_and_library_mutex_valid(tmp_path):
    """逐 Job 锁不再创建，库级执行/管理互斥仍有效。"""
    state_path = tmp_path / "channel_mutex.db"

    pipeline = TaskLite(
        backend="sqlite",
        state_path=state_path,
        max_workers=1,
    )
    pipeline.register_task("fast", _fast_worker)
    pipeline.enqueue(Job("fast", "j1"))
    pipeline.run()

    # 验证没有为 fast::j1 生成 {uid}.lock 文件
    assert not any("fast" in p.name and p.name.endswith(".lock") for p in tmp_path.glob("*.lock"))

    # 验证库级互斥仍正常生效
    run_lock = try_acquire_lock(state_path)
    assert run_lock is not None
    try:
        # 重复获取同一库级锁被互斥阻断
        second = try_acquire_lock(state_path)
        assert second is None
    finally:
        release_lock(run_lock)

    # 释放后可再次获取
    third = try_acquire_lock(state_path)
    assert third is not None
    release_lock(third)
    pipeline.stop()


def test_worker_exit_error_after_result_does_not_rewrite_success(tmp_path):
    """子进程在有效结果发出后发生非零退出或崩溃，有效结果不受改写。"""
    with TaskLite(
        backend="sqlite",
        state_path=tmp_path / "channel_success_then_exit.db",
        max_workers=1,
    ) as pipeline:
        pipeline.register_task("die_later", _success_then_exit_error)
        pipeline.enqueue(Job("die_later", "j1"))
        pipeline.run()

        successes = pipeline._runtime.store.backend.load_successes()
        assert "die_later::j1" in successes
        assert successes["die_later::j1"]["result"]["metadata"]["returned"] is True
        assert pipeline._runtime.store.backend.load_failed() == {}

        attempts = pipeline._runtime.store.backend.load_settled_attempts("die_later::j1")
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "success"

        assert not [t for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")]


def _send_malformed_result(job: Job, ctx: JobContext) -> TaskResult:
    assert ctx._pipe_sender is not None
    ctx._pipe_sender.send_result(job.payload["result"])
    return TaskResult.success(metadata={"untrusted": True})


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("payload", [
    {"status": "success", "metadata": []},
    {"status": "success", "metadata": {}, "spawned_jobs": 7},
    {"status": "failure", "metadata": {}, "exception": {"type": "bad"}},
    {"status": "retry", "metadata": {}, "consume_budget": "yes"},
])
def test_malformed_worker_result_settles_one_failure(tmp_path, backend, payload):
    with TaskLite(backend=backend, state_path=tmp_path / "state.db") as pipeline:
        pipeline.register_task("malformed", _send_malformed_result)
        job = Job("malformed", "result", payload={"result": payload}, max_retries_per_run=3)
        pipeline.enqueue(job)
        pipeline.run()
        with StateSnapshot.open(pipeline) as snapshot:
            row = snapshot.get_job(job.uid)
            assert row["status"] == "failed"
            assert row["attempt_no"] == 1
            assert row["result"]["message"]
            assert row["result"]["metadata"] == {}
            assert row["result"]["exception"] is None
            attempt, = snapshot.iter_attempts(job_uid=job.uid)
            assert attempt["outcome"] == "failure"
            assert attempt["result"] == row["result"]
            assert list(snapshot.iter_jobs(status="pending")) == []
            assert list(snapshot.iter_jobs(status="succeeded")) == []
