"""Hypothesis property tests for state-backend and handler-result invariants.

These tests verify core invariants hold across a wide range of generated inputs:

- INV-1: JSON backend commit failure preserves the on-disk queue (no state split).
- INV-2: ``_normalize_handler_result`` rejects unrecognized return types.
- INV-3: ``Job.to_dict`` / ``Job.from_dict`` round-trips preserve fields.
- INV-4: SQLite backend ``commit_job_success`` atomically advances wall + queue +
  cursors, or leaves the wall untouched on failure.
"""

import uuid

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.executor import _normalize_handler_result
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite
from tests.helpers import make_pipeline


job_dict_strategy = st.fixed_dictionaries({
    "task_type": st.sampled_from(["test", "download", "process"]),
    "job_id": st.one_of(
        st.integers(min_value=1, max_value=1000).map(str),
        st.text(min_size=1, max_size=10),
    ),
    "payload": st.dictionaries(
        st.text(min_size=1, max_size=5),
        st.integers(),
        max_size=3,
    ),
})


@pytest.mark.hypothesis
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    value=st.one_of(
        st.integers(),
        st.text(),
        st.lists(st.integers()),
        st.floats(allow_nan=False, allow_infinity=False),
    )
)
def test_normalize_rejects_unknown_types(tmp_path, value):
    """INV-2: Any handler return value whose type is not in
    {None, bool, dict, tuple(bool, dict)} is rejected: success is False and the
    metadata carries an "error" key so the job is routed to the DLQ instead of
    being silently marked successful.
    """
    # 实例方法转发层已删——直接测 executor 模块级实现。
    success, meta = _normalize_handler_result(value)
    assert success is False
    assert "error" in meta


@pytest.mark.hypothesis
@settings(max_examples=50)
@given(
    job=st.builds(
        Job,
        task_type=st.sampled_from(["test", "download", "process"]),
        job_id=st.integers(min_value=1, max_value=1000).map(str),
        payload=st.dictionaries(
            st.text(min_size=1, max_size=5),
            st.integers(),
            max_size=3,
        ),
    )
)
def test_job_roundtrip_preserves_fields(job):
    """INV-3: Serializing a Job to a dict and back preserves the task_type,
    job_id, and payload fields exactly.
    """
    restored = Job.from_dict(job.to_dict())
    assert restored.task_type == job.task_type
    assert restored.job_id == job.job_id
    assert restored.payload == job.payload


@pytest.mark.hypothesis
@settings(max_examples=50)
@given(
    job=st.builds(
        Job,
        task_type=st.sampled_from(["test", "download"]),
        job_id=st.integers(min_value=1, max_value=100).map(str),
        runtime=st.dictionaries(
            st.sampled_from([
                "_backoff_until", "_backoff_wall_deadline",
                "_commit_failures", "_last_retry_error",
            ]),
            st.one_of(st.floats(min_value=0, max_value=1e9, allow_nan=False, allow_infinity=False),
                      st.text(min_size=0, max_size=50)),
            max_size=4,
        ),
    )
)
def testruntime_roundtrip_preserves_keys(job):
    """INV-5 : 往返不变式——任意带 runtime 字段的 job 经
    to_dict → from_dict → to_dict 后 runtime 子树不丢键。

    现状代码（散装下划线键时代）该测试会失败（to_dict 丢字段）；
    runtime 命名空间使新增字段只改一个 schema 位置、结构上不可能丢。
    """
    rt1 = job.to_dict()["runtime"]
    restored = Job.from_dict(job.to_dict())
    rt2 = restored.to_dict()["runtime"]
    assert set(rt1.keys()) == set(rt2.keys()), (
        f"runtime 往返丢键: {set(rt1) ^ set(rt2)}"
    )
    for k, v in rt1.items():
        assert rt2.get(k) == v, f"runtime 键 {k} 值变化: {v!r} != {rt2.get(k)!r}"


@pytest.mark.hypothesis
@settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    job_dict=job_dict_strategy,
    cursor_updates=st.dictionaries(
        st.text(min_size=1, max_size=10),
        st.text(min_size=1, max_size=10),
        max_size=5,
    ),
)
def test_commit_success_advances_wall_atomically(tmp_path, job_dict, cursor_updates):
    """INV-4: SQLite ``commit_job_success`` is atomic. On success the wall
    contains the uid, the queue equals spawned_jobs, and every cursor update is
    persisted. On failure (not expected under valid input) the wall must not
    contain the uid. Each example uses a fresh database to avoid cross-example
    state contamination.
    """
    state_dir = tmp_path / f"ex_{uuid.uuid4().hex[:8]}"
    backend = SQLiteStateBackend(state_dir / "state.db")
    uid = f"{job_dict['task_type']}::{job_dict['job_id']}"
    result_meta = {"processed": True}

    # delta API: spawned_jobs 插入队头；空队列上 commit 后 queue == [job_dict]
    result = backend.commit_job_success(
        uid, result_meta,
        spawned_jobs=[job_dict],
        cursor_updates=cursor_updates,
    )

    wall = backend.load_wall()
    queue = backend.load_queue()
    cursors = backend.load_cursors()

    if result is True:
        assert uid in wall
        assert queue == [job_dict]
        for k, v in cursor_updates.items():
            assert cursors.get(k) == str(v)
    else:
        assert uid not in wall
