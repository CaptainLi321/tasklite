"""框架结构字段边界与业务数据隔离。"""
from dataclasses import asdict

import pytest

from tasklite.models.job import Job
from tasklite.models.attempt import AttemptRecord


def test_job_timeout_seconds_roundtrip_and_user_fields():
    user = {"timeout": 17, "error": {"new_jobs": [], "fatal": True}, "timeout_seconds": 29}
    job = Job("t", "j", payload=user, timeout_seconds=13, runtime=user)
    snapshot = job.to_dict()
    assert snapshot["timeout_seconds"] == 13
    assert "timeout" not in snapshot
    restored = Job.from_dict(snapshot)
    assert restored.payload == user
    assert restored.runtime.to_dict() == user
    assert restored.timeout_seconds == 13


def test_job_rejects_old_timeout_even_with_new_field():
    with pytest.raises(ValueError, match="timeout"):
        Job.from_dict({"task_type": "t", "job_id": "j", "timeout": 7, "timeout_seconds": 9})


@pytest.mark.parametrize("legacy", ["incarnation", "error"])
def test_attempt_rejects_old_fields(legacy):
    record = AttemptRecord("t::j", 1, 1, "r.1", "r", "now", "running")
    data = asdict(record)
    data[legacy] = "old"
    with pytest.raises(ValueError, match=legacy):
        AttemptRecord.from_dict(data)


@pytest.mark.parametrize("legacy", ["new_jobs", "error"])
@pytest.mark.parametrize("status", ["success", "retry", "interrupted", "fatal", "error"])
def test_worker_rejects_old_envelope_fields(legacy, status):
    from types import SimpleNamespace
    from tasklite.engine.channel import _decode_ipc_result

    result = _decode_ipc_result(
        {"status": status, "raw_result": True, legacy: [], "spawned_jobs": []},
        SimpleNamespace(exitcode=0), Job("t", "j"),
    )
    assert not result.success
    assert not result.retry_requested
    assert "CORRUPT_RESULT_FILE" in result.result_metadata["error_message"]
    assert legacy in result.result_metadata["error_message"]
