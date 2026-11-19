"""声明环境故障沿执行结果返回并阻断结算，业务异常仍按任务错误处理。"""
from __future__ import annotations

import pytest

from tasklite import Job, StateSnapshot, TaskLite, TaskResult
from tasklite.testing import in_process_workers


def _handler_io_fault(job, ctx):
    raise OSError("external tool read failed")


def _artifact_error_result_handler(job, ctx):
    assert ctx._pipe_sender is not None
    ctx._pipe_sender.send_result(
        {"status": "artifact_error", "error_message": "declaration write denied"}
    )
    return TaskResult.success(metadata={"swallowed": True})


def test_handler_io_fault_settles_as_normal_task_failure():
    """handler 自身抛出 OSError 按任务失败结算，不触发环境故障中止。"""
    with TaskLite(backend="memory") as pipeline:
        pipeline.register_task("transport", _handler_io_fault)
        job = Job("transport", "one", max_retries_per_run=3)
        pipeline.enqueue(job)
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)

        with StateSnapshot.open(pipeline) as snap:
            rec = snap.get_job(job.uid)
            assert rec is not None
            assert rec["status"] == "failed"
            assert "external tool read failed" in (rec["result"]["message"] or "")
            assert rec["result"]["exception"]["type"] == "OSError"
            attempts = list(snap.iter_attempts(job_uid=job.uid))
            assert len(attempts) == 1
            assert attempts[0]["outcome"] == "failure"


def test_artifact_error_preserves_running_queue_and_raises_base_exception():
    """worker 回传 artifact_error 时框架以 BaseException 中止并保留 running 队列快照。"""
    with TaskLite(backend="memory") as pipeline:
        pipeline.register_task("transport", _artifact_error_result_handler)
        job = Job("transport", "one", payload={"k": "v"}, max_retries_per_run=3)
        pipeline.enqueue(job)
        with in_process_workers(pipeline):
            with pytest.raises(BaseException, match="declaration write denied") as caught:
                pipeline.run(install_signals=False)
        assert not isinstance(caught.value, Exception)

        with StateSnapshot.open(pipeline) as snap:
            rec = snap.get_job(job.uid)
            assert rec is not None
            assert rec["status"] == "running"
            assert rec["attempt_no"] == 1
            assert rec["job"]["payload"] == {"k": "v"}
            assert rec["job"]["max_retries_per_run"] == 3
            assert list(snap.iter_jobs(status="succeeded")) == []
            assert list(snap.iter_jobs(status="failed")) == []
            assert list(snap.iter_attempts(job_uid=job.uid)) == []
