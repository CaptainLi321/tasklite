"""框架结构字段边界与业务数据隔离。"""
from pathlib import Path

import pytest

from tasklite import TaskLiteError
from tasklite.models.artifact import FileSpec, resolve_job_files_config
from tasklite.models.job import Job


def test_job_timeout_seconds_roundtrip_and_user_fields():
    user = {"timeout": 17, "error": {"new_jobs": [], "fatal": True}, "timeout_seconds": 29, "runtime": {"_commit_failures": 8}, "activation_no": 4, "first_enqueued_at": "业务日期"}
    job = Job._restore("t", "j", payload=user, timeout_seconds=13)
    snapshot = job.to_dict()
    assert snapshot["timeout_seconds"] == 13
    assert "timeout" not in snapshot
    restored = Job.from_dict(snapshot)
    assert restored.payload == user
    assert restored.timeout_seconds == 13


def test_job_files_snapshot_matches_adr_sample_and_isolates_mutation(tmp_path):
    base = tmp_path / "work"
    result_path = Path("results/processed.csv")
    consumer = Job(
        "process",
        "report-001",
        depends_on=["generate::report-001"],
        files={
            "source": FileSpec.from_output("generate::report-001", "report", role="input"),
            "result": FileSpec(result_path, role="output"),
        },
    )
    snapshot = consumer.to_dict()
    snapshot["files"] = resolve_job_files_config(
        snapshot["files"], base_dir=base, depends_on=snapshot["depends_on"]
    )
    consumer.files["extra"] = FileSpec("results/extra.csv", role="cache")
    consumer.depends_on.append("other::1")

    assert snapshot["files"] == {
        "source": {
            "producer_uid": "generate::report-001",
            "output_name": "report",
            "role": "input",
            "delete_existing": False,
            "delete_on_failure": False,
            "delete_on_success": False,
        },
        "result": {
            "path": str(base / "results" / "processed.csv"),
            "role": "output",
            "delete_existing": False,
            "delete_on_failure": False,
            "delete_on_success": False,
        },
    }
    restored = Job.from_dict(snapshot)
    assert set(restored.files.keys()) == {"source", "result"}
    assert restored.files["source"] == FileSpec.from_output(
        "generate::report-001", "report", role="input"
    )
    assert restored.files["result"] == FileSpec(
        base / "results" / "processed.csv", role="output"
    )
    resolved_outer = {
        "source": restored.files["source"].bind_output_path(
            base / "results" / "report.csv"
        ).to_dict(),
        "result": restored.files["result"].to_dict(),
    }
    assert resolved_outer["source"]["path"] == str(base / "results" / "report.csv")
    assert "producer_uid" not in resolved_outer["source"]


def test_job_rejects_old_timeout_even_with_new_field():
    with pytest.raises(TaskLiteError, match="timeout"):
        Job.from_dict({"task_type": "t", "job_id": "j", "timeout": 7, "timeout_seconds": 9})


@pytest.mark.parametrize("unsupported", ["new_jobs", "error"])
@pytest.mark.parametrize(
    "payload",
    [
        {
            "status": "success",
            "metadata": {},
            "spawned_jobs": [],
            "cursor_updates": {},
        },
        {"status": "failure", "metadata": {}},
        {"status": "retry", "metadata": {}, "consume_budget": True},
    ],
)
def test_worker_rejects_unsupported_envelope_fields(unsupported, payload):
    from tasklite.engine.channel import _decode_ipc_result

    result = _decode_ipc_result(
        dict(payload, **{unsupported: []}),
        Job("t", "j"),
    )
    assert not result.success
    assert not result.retry_requested
    assert "Invalid worker result" in (result.message or "")
    assert unsupported in (result.message or "")
