"""真实执行与人工管理使用独立的业务结果和框架档案字段。"""
from __future__ import annotations

import copy

import pytest

from tasklite import Job, StateSnapshot, TaskLite, TaskResult


def metadata_handler(job, ctx):
    factory = TaskResult.success if job.payload["success"] else TaskResult.failure
    return factory(message=job.payload["message"], metadata=job.payload["metadata"])


@pytest.fixture(params=["memory", "sqlite"])
def pipeline(request, tmp_path):
    with TaskLite(backend=request.param, state_path=tmp_path / "state.db") as pipeline:
        pipeline.register_task("t", metadata_handler)
        yield pipeline


@pytest.mark.parametrize("success", [True, False])
@pytest.mark.parametrize("message", [None, "业务说明"])
@pytest.mark.parametrize("annotations", ["业务字符串", {"completion_source": "manual", "note": "业务备注"}])
def test_business_keys_do_not_control_execution_records(pipeline, success, message, annotations):
    metadata = {
        "annotations": annotations, "result": "业务结果", "job_snapshot": "业务快照",
        "files": "业务文件", "metadata": {"nested": [None, True, 1, "中文"]},
        "error_message": "JOB_DEPENDENCY", "error_category": "manual",
        "message": "业务消息键", "exception": {"type": "业务异常"},
        "run_id": "业务运行", "attempt_no": 999, "started_at": "业务开始",
        "settled_at": "业务结束", "artifacts": "业务产物", "inputs": "业务输入",
        "success_count": 999, "last_succeeded_at": "业务时间",
        "last_success_run_id": "业务身份", "cleanup_error": "业务清理",
    }
    expected = copy.deepcopy(metadata)
    job = Job("t", "executed", payload={"success": success, "message": message, "metadata": metadata})
    pipeline.enqueue(job)
    pipeline.run()
    backend = pipeline._runtime.store.backend
    archive = (backend.load_successes() if success else backend.load_failed())[job.uid]
    assert archive["result"]["metadata"] == expected
    assert archive["annotations"]["completion_source"] == "execution"
    assert "metadata" not in archive
    assert "cleanup_error" not in archive
    assert archive["job_snapshot"]["job_id"] == "executed"
    assert archive["files"] == {}
    if not success:
        assert archive.get("error_message") == message
        assert archive["error_category"] != "manual"
    with StateSnapshot.open(pipeline) as snapshot:
        row = snapshot.get_job(job.uid)
        assert row["status"] == ("succeeded" if success else "failed")
        assert row["run_id"] and row["run_id"] != "业务运行"
        assert row["attempt_no"] == 1
        assert row["started_at"] and row["settled_at"]
        assert row["files"] == {}
        assert row["job"]["payload"] == job.payload
        assert row["result"]["metadata"] == expected
        assert row["result"]["message"] == message
        assert row["result"]["exception"] is None
        assert row["result"]["annotations"]["completion_source"] == "execution"
        assert list(snapshot.iter_jobs()) == [row]
        attempt, = snapshot.iter_attempts()
        assert attempt["result"] == row["result"]
        row["result"]["metadata"]["metadata"]["nested"].append("查询变异")
        assert snapshot.get_job(job.uid)["result"]["metadata"] == expected

    for status in ("failed", "succeeded", "pending"):
        pipeline.set_job_status(job.uid, status, recursive=False, note="人工说明")
        with StateSnapshot.open(pipeline) as snapshot:
            row = snapshot.get_job(job.uid)
            assert row["status"] == status
            assert row["run_id"] is None and row["attempt_no"] is None
            assert len(list(snapshot.iter_attempts())) == 1
            if status == "pending":
                assert row["result"] is None
            else:
                assert row["result"]["metadata"] == {}
                assert row["result"]["annotations"]["completion_source"] == "manual"
                assert row["started_at"] is None and row["settled_at"]
                if status == "succeeded":
                    assert row["result"]["annotations"]["note"] == "人工说明"
                else:
                    assert row["result"]["message"] == "人工说明"
