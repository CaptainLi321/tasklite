"""结果说明与异常证据不参与隐式重试决策。"""
from __future__ import annotations

import os
import time

import pytest

from tasklite import Job, StateSnapshot, TaskLite, TaskResult
from tasklite.testing import in_process_workers
from tasklite.wrappers.http import HttpResponse


def outcome_handler(job, ctx):
    mode = job.payload["mode"]
    if mode == "exit":
        os._exit(7)
    if mode == "timeout":
        time.sleep(2.0)
    if mode in ("ValueError", "ConnectionError", "TimeoutError", "SystemExit"):
        errors = {
            "ValueError": ValueError,
            "ConnectionError": ConnectionError,
            "TimeoutError": TimeoutError,
            "SystemExit": SystemExit,
        }
        raise errors[mode]("原始执行证据")
    if mode == "http":
        return HttpResponse(status_code=503, headers={}, body=b"error").to_task_result()
    metadata = {
        "error_message": "MAX_RETRIES_EXCEEDED",
        "is_fatal": True,
        "is_transient": True,
        "error_category": "retry",
    }
    if mode == "retry":
        return TaskResult.retry(message="用户显式重试", metadata=metadata)
    return TaskResult.failure(message="用户显式失败", metadata=metadata)


@pytest.mark.parametrize(
    "mode",
    ["ValueError", "ConnectionError", "TimeoutError", "SystemExit", "exit", "timeout", "failure", "retry", "http"],
)
def test_only_explicit_result_requests_another_attempt(mode):
    with TaskLite(backend="memory") as app:
        app.register_task("t", outcome_handler, max_retries_per_run=1)
        job = Job("t", "a", payload={"mode": mode}, timeout_seconds=0.1 if mode == "timeout" else None)
        app.enqueue(job)
        if mode in ("exit", "timeout"):
            app.run(install_signals=False)
        else:
            with in_process_workers(app):
                app.run(install_signals=False)
        with StateSnapshot.open(app) as snapshot:
            current = snapshot.get_job(job.uid)
            attempts = list(snapshot.iter_attempts(job_uid=job.uid))
        assert current is not None and current["status"] == "failed"
        assert len(attempts) == (2 if mode in ("retry", "http") else 1)
        assert attempts[-1]["outcome"] == "failure"
        if mode in ("ValueError", "ConnectionError", "TimeoutError", "SystemExit"):
            exception = current["result"]["exception"]
            assert exception["type"] == mode
            assert exception["message"] == "原始执行证据"
            assert "outcome_handler" in exception["traceback"]
        if mode in ("failure", "retry"):
            assert current["result"]["message"] == ("用户显式重试" if mode == "retry" else "用户显式失败")
            assert current["result"]["metadata"]["is_fatal"] is True
            assert current["result"]["metadata"]["is_transient"] is True
        assert "error_category" not in current
        assert "is_fatal" not in current
