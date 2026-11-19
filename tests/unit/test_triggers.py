"""触发器与任务模板的公开契约单元测试。"""
from __future__ import annotations

import os

import pytest

from tasklite import Job, StateSnapshot, TaskLite, TaskLiteError, TaskResult
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.models.state import PipelineState
from tasklite.models.trigger import TriggerContext
from tasklite.resource import CapacityResource
from tasklite.selection import Exclude
from tasklite.testing import in_process_workers, running
from tasklite.triggers import BUILTIN_TRIGGER_FUNCTIONS, on_input_change


def handle(job, ctx):
    return TaskResult.success()


def test_custom_trigger_observes_inherited_task_spec():
    with TaskLite(backend="memory") as pipeline:
        seen = []
        pipeline.register_trigger("inspect", lambda ctx: seen.append(ctx.job.to_dict()) or False)
        pipeline.register_task("t", handle, max_retries_per_run=7, timeout_seconds=12, trigger="inspect")
        pipeline.enqueue(Job("t", "a"))
        assert pipeline.run() is None
        with StateSnapshot.open(pipeline) as snap:
            assert list(snap.iter_jobs()) == []
        assert seen[0]["max_retries_per_run"] == 7
        assert seen[0]["timeout_seconds"] == 12
        assert seen[0]["trigger"] == "inspect"


def test_fresh_trigger_skip_allows_dependent_failure():
    with TaskLite(backend="memory") as pipeline:
        seen = []
        pipeline.register_trigger("observe", lambda ctx: seen.append(ctx.job.uid) or False)
        pipeline.register_task("t", handle, trigger="observe")
        pipeline.enqueue(Job("t", "a"))
        pipeline.enqueue(Job("t", "b", depends_on=["t::a"]))
        assert pipeline.run() is None
        assert seen == ["t::a"]
        with StateSnapshot.open(pipeline) as snap:
            assert snap.get_job("t::b")["status"] == "failed"  # type: ignore[index]


def test_task_resource_merge_and_explicit_false_zero_overrides():
    with TaskLite(backend="memory") as pipeline:
        pipeline.register_resource(CapacityResource("gpu", max_capacity=5.0))
        pipeline.register_resource(CapacityResource("cpu", max_capacity=5.0))
        pipeline.register_task(
            "t",
            handle,
            resources={"gpu": 2, "cpu": 1},
            max_retries_per_run=7,
            timeout_seconds=12,
            trigger="always",
        )
        pipeline.enqueue(
            Job("t", "a", resources={"gpu": 0}, max_retries_per_run=0, timeout_seconds=2, trigger="once")
        )
        with StateSnapshot.open(pipeline) as snap:
            row = snap.get_job("t::a")["job"]  # type: ignore[index]
            assert row["resources"] == {"gpu": 0, "cpu": 1, "__workers__": 1.0}
            assert (row["max_retries_per_run"], row["timeout_seconds"], row["trigger"]) == (0, 2, "once")


def test_restored_none_spec_inherits_task_before_trigger(tmp_path):
    db_path = tmp_path / "restore.db"
    backend = SQLiteStateBackend(db_path)
    snapshot = Job("t", "a").to_dict()
    assert Job.from_dict(snapshot).to_dict() == snapshot
    backend.enqueue_jobs([snapshot])
    backend.close()

    with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
        seen = []
        pipeline.register_trigger("inspect", lambda ctx: seen.append(ctx.job.max_retries_per_run) or False)
        pipeline.register_task("t", handle, max_retries_per_run=9, trigger="inspect")
        pipeline.run(install_signals=False)
        assert seen == [9]


def test_unknown_trigger_raises_preserving_candidate_and_budget(tmp_path):
    with TaskLite(backend="sqlite", state_path=tmp_path / "trigger.db") as pipeline:
        pipeline.register_task("t", handle)
        pipeline.enqueue(Job("t", "a"))
        with StateSnapshot.open(pipeline) as snap:
            before_jobs = list(snap.iter_jobs())
        with pytest.raises(TaskLiteError, match="unregistered trigger"):
            pipeline.enqueue(Job("t", "b", trigger="missing"))
        with StateSnapshot.open(pipeline) as snap:
            assert list(snap.iter_jobs()) == before_jobs
            assert list(snap.iter_attempts(job_uid="t::b")) == []


def test_inherited_unknown_trigger_rejected_at_enqueue(tmp_path):
    with TaskLite(backend="sqlite", state_path=tmp_path / "trigger.db") as pipeline:
        pipeline.register_task("t", handle, trigger="missing")
        with pytest.raises(TaskLiteError, match="unregistered trigger"):
            pipeline.enqueue(Job("t", "a"))
        with StateSnapshot.open(pipeline) as snap:
            assert list(snap.iter_jobs()) == []


def test_custom_trigger_is_evaluated_only_after_dependency_readiness():
    with TaskLite(backend="memory") as pipeline:
        seen = []
        pipeline.register_trigger("observe", lambda ctx: seen.append(ctx.job.uid) or True)
        pipeline.register_task("t", handle, trigger="observe")
        pipeline.enqueue(Job("t", "a"))
        pipeline.enqueue(Job("t", "b", depends_on=["t::a"]))
        with in_process_workers(pipeline):
            pipeline.run()
        assert seen == ["t::a", "t::b"]


def test_callback_failure_and_non_bool_return_preserve_queue():
    for name, fn, error in [
        ("error", lambda ctx: 1 / 0, ZeroDivisionError),
        ("integer", lambda ctx: 1, TaskLiteError),
        ("none", lambda ctx: None, TaskLiteError),
    ]:
        with TaskLite(backend="memory") as pipeline:
            pipeline.register_trigger(name, fn)
            pipeline.register_task("t", handle, trigger=name)
            pipeline.enqueue(Job("t", "a"))
            with StateSnapshot.open(pipeline) as snap:
                before = list(snap.iter_jobs())
            with pytest.raises(error):
                pipeline.run()
            with StateSnapshot.open(pipeline) as snap:
                assert list(snap.iter_jobs()) == before
                assert list(snap.iter_attempts(job_uid="t::a")) == []


def test_register_trigger_rejects_duplicate_builtin_and_invalid_callable():
    with TaskLite(backend="memory") as pipeline:
        for name in ["once", "until_success", "on_input_change", "always"]:
            with pytest.raises(TaskLiteError):
                pipeline.register_trigger(name, lambda ctx: True)
        with pytest.raises(TaskLiteError):
            pipeline.register_trigger("", lambda ctx: True)
        with pytest.raises(TaskLiteError):
            pipeline.register_trigger("custom", 1)  # type: ignore[arg-type]
        pipeline.register_trigger("custom", lambda ctx: True)
        with pytest.raises(TaskLiteError):
            pipeline.register_trigger("custom", lambda ctx: True)
        with running(pipeline), pytest.raises(TaskLiteError):
            pipeline.register_trigger("another", lambda ctx: True)


def test_builtin_trigger_matrix():
    for name, expected in {
        "once": (True, False, False),
        "until_success": (True, False, True),
        "always": (True, True, True),
        "on_input_change": (True, True, True),
    }.items():
        outcomes = tuple(
            BUILTIN_TRIGGER_FUNCTIONS[name](
                TriggerContext(Job("t", "a"), succeeded, failed, {} if succeeded else None, None)
            )
            for succeeded, failed in [(False, False), (True, False), (False, True)]
        )
        assert outcomes == expected


def test_mtime_microsecond_boundary_size_only_and_no_cache(tmp_path):
    path = tmp_path / "input"
    path.write_text("first")
    baseline_ns = 1_700_000_000_123_456_000
    ctx = TriggerContext(Job("t", "a"), True, False, {"inputs": [str(path)]}, baseline_ns // 1000)
    for offset, expected in [(-1000, False), (0, False), (999, False), (1000, True)]:
        os.utime(path, ns=(baseline_ns, baseline_ns + offset))
        assert on_input_change(ctx) is expected
    path.write_text("changed length")
    os.utime(path, ns=(baseline_ns, baseline_ns))
    assert on_input_change(ctx) is False
    os.utime(path, ns=(baseline_ns, baseline_ns + 1000))
    assert on_input_change(ctx) is True


def test_directory_mtime_does_not_recurse(tmp_path):
    folder = tmp_path / "folder"
    folder.mkdir()
    child = folder / "child"
    child.write_text("first")
    baseline_ns = 1_700_000_000_000_000_000
    os.utime(folder, ns=(baseline_ns, baseline_ns))
    child.write_text("changed")
    os.utime(child, ns=(baseline_ns, baseline_ns + 1000))
    ctx = TriggerContext(Job("t", "a"), True, False, {"inputs": [str(folder)]}, baseline_ns // 1000)
    assert on_input_change(ctx) is False


def test_input_contract_errors_and_untrusted_baseline(tmp_path):
    for inputs in [None, [], "bad", [42], [""]]:
        ctx = TriggerContext(Job("t", "a"), True, False, {"inputs": inputs}, 1)
        with pytest.raises(TaskLiteError):
            on_input_change(ctx)
        assert on_input_change(TriggerContext(ctx.job, True, False, ctx.success_metadata, None)) is True
    existing = tmp_path / "exists"
    existing.write_text("new")
    missing = tmp_path / "missing"
    ctx = TriggerContext(Job("t", "a"), True, False, {"inputs": [str(existing), str(missing)]}, 1)
    with pytest.raises(FileNotFoundError):
        on_input_change(ctx)


def test_failure_history_skipped_this_batch_allows_downstream():
    with TaskLite(backend="memory") as pipeline:
        pipeline.register_task("t", handle)
        pipeline.set_job_status(Job("t", "a"), "failed", recursive=False, note="prior failure")
        seen = []
        pipeline.register_trigger("observe", lambda ctx: seen.append(ctx.job.uid) or False)
        pipeline.enqueue(Job("t", "a", trigger="observe"))
        pipeline.enqueue(Job("t", "b", depends_on=["t::a"], trigger="observe"))
        pipeline.run()
        assert seen == ["t::a"]
        with StateSnapshot.open(pipeline) as snap:
            assert {r["job_uid"] for r in snap.iter_jobs(status="failed")} == {"t::a", "t::b"}


def test_trigger_skip_is_local_to_run():
    with TaskLite(backend="memory") as pipeline:
        seen = []
        pipeline.register_trigger("observe", lambda ctx: seen.append(ctx.job.uid) or False)
        pipeline.register_task("parent", handle, trigger="observe")
        pipeline.register_task("child", handle, trigger="observe")
        pipeline.enqueue(Job("parent", "a"))
        pipeline.enqueue(Job("child", "b", depends_on=["parent::a"]))
        pipeline.run(exclude=Exclude(task_type="child"))
        assert seen == ["parent::a"]
        pipeline.run()
        assert seen == ["parent::a"]
        with StateSnapshot.open(pipeline) as snap:
            assert [r["job_uid"] for r in snap.iter_jobs(status="failed")] == ["child::b"]


def test_stop_after_trigger_skip_does_not_persist_dependency_readiness():
    with TaskLite(backend="memory") as pipeline:
        def stop_and_skip(ctx):
            pipeline.stop()
            return False

        pipeline.register_trigger("stop", stop_and_skip)
        pipeline.register_task("t", handle)
        pipeline.register_task("child", handle)
        pipeline.enqueue(Job("t", "a", trigger="stop"))
        pipeline.enqueue(Job("child", "b", depends_on=["t::a"]))
        pipeline.run(exclude=Exclude(task_type="child"))
        with StateSnapshot.open(pipeline) as snap:
            assert [r["job"]["job_id"] for r in snap.iter_jobs(status="pending")] == ["b"]
        pipeline.run()
        with StateSnapshot.open(pipeline) as snap:
            assert [r["job_uid"] for r in snap.iter_jobs(status="failed")] == ["child::b"]


def test_trigger_skip_does_not_grant_dependency_readiness():
    state = PipelineState({}, {}, {}, [])
    state.mark_trigger_skipped("t::a")
    assert not state.dependency_ready("t::a")


def test_trigger_error_aborts_inflight_without_burning_budget():
    class WaitingProcess:
        def __init__(self, **kwargs):
            self.exitcode = None
            self.alive = False

        def start(self):
            self.alive = True

        def is_alive(self):
            return self.alive

        def kill(self):
            self.alive = False
            self.exitcode = -9

        def terminate(self):
            self.kill()

        def join(self, timeout=None):
            pass

        def close(self):
            pass

    with TaskLite(backend="memory", max_workers=2) as pipeline:
        pipeline.register_trigger("error", lambda ctx: 1 / 0)
        pipeline.register_task("t", handle)
        pipeline.enqueue(Job("t", "a"))
        pipeline.enqueue(Job("t", "b", trigger="error"))
        with in_process_workers(pipeline, WaitingProcess):
            with pytest.raises(ZeroDivisionError):
                pipeline.run()
        with StateSnapshot.open(pipeline) as snap:
            rows = list(snap.iter_jobs())
            assert {r["job_uid"]: r["status"] for r in rows} == {
                "t::a": "running",
                "t::b": "pending",
            }
            assert next(r["attempt_no"] for r in rows if r["job_uid"] == "t::a") == 1
            assert list(snap.iter_attempts(job_uid="t::b")) == []
            assert list(snap.iter_jobs(status="failed")) == []


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_trigger_false_removes_candidate_without_terminal_or_attempt(tmp_path, backend):
    with TaskLite(backend=backend, state_path=tmp_path / "state.db") as pipeline:
        pipeline.register_trigger("reject", lambda ctx: False)
        pipeline.register_task("work", handle, trigger="reject")
        pipeline.enqueue(Job("work", "one"))
        pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as snap:
            assert list(snap.iter_jobs()) == []
            assert list(snap.iter_attempts(job_uid="work::one")) == []
