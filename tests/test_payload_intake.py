"""业务输入按同一 JSON 对象契约进入两个后端。"""
from __future__ import annotations

import copy

import pytest

from tasklite import Job, StateSnapshot, TaskLite, TaskResult


def accept_payload(job, ctx):
    return TaskResult.success(metadata=job.payload)


@pytest.fixture(params=["memory", "sqlite"])
def pipeline(request, tmp_path):
    with TaskLite(backend=request.param, state_path=tmp_path / "state.db") as pipeline:
        pipeline.register_task("t", accept_payload)
        yield pipeline


@pytest.mark.parametrize("payload", [
    {1: "integer"},
    {1: "integer", "1": "string"},
    {"nested": [{None: "null"}]},
    {"nested": [{"items": (1, 2)}]},
])
@pytest.mark.parametrize("intake", ["enqueue", "manual", "snapshot"])
def test_invalid_payload_is_rejected_without_records(pipeline, payload, intake):
    job = Job("t", "bad")
    job.payload = payload
    with pytest.raises(TypeError, match="payload"):
        if intake == "enqueue":
            pipeline.enqueue(job)
        elif intake == "manual":
            pipeline.set_job_status(job, "succeeded", recursive=False)
        else:
            pipeline._runtime.store.normalize_and_validate_job(job.to_dict())
    with StateSnapshot.open(pipeline) as snapshot:
        assert list(snapshot.iter_jobs()) == []
        assert list(snapshot.iter_attempts()) == []


def test_json_payload_is_complete_idempotent_and_isolated(pipeline):
    payload = {
        "items": [None, True, False, 0, -7, 1.25, "中文", {"": "empty key"}],
        "object": {},
        "array": [],
    }
    expected = copy.deepcopy(payload)
    job = Job("t", "ok", payload=payload)
    assert pipeline.enqueue(job) is True
    assert pipeline.enqueue(job) is False
    assert pipeline.enqueue(Job("t", "ok", payload=expected)) is False
    payload["items"].append("外部变异")
    job.payload["items"].append("Job 变异")
    with StateSnapshot.open(pipeline) as snapshot:
        row = snapshot.get_job(job.uid)
        assert row["job"]["payload"] == expected
        row["job"]["payload"]["items"].append("查询变异")
        assert snapshot.get_job(job.uid)["job"]["payload"] == expected
    with StateSnapshot.open(pipeline) as snapshot:
        assert snapshot.get_job(job.uid)["job"]["payload"] == expected
    pipeline.run()
    with StateSnapshot.open(pipeline) as snapshot:
        settled = snapshot.get_job(job.uid)
        assert settled["job"]["payload"] == expected
        assert settled["result"]["metadata"] == expected


def test_manual_job_captures_json_payload_without_attempts(pipeline):
    expected = {"items": [None, False, 1, 1.25, "中文", {"": []}]}
    job = Job("t", "manual", payload=expected)
    pipeline.set_job_status(job, "succeeded", recursive=False)
    pipeline.set_job_status(Job("t", "manual", payload=expected), "succeeded", recursive=False)
    job.payload["items"].clear()
    with StateSnapshot.open(pipeline) as snapshot:
        assert snapshot.get_job(job.uid)["job"]["payload"] == expected
        assert list(snapshot.iter_attempts()) == []
