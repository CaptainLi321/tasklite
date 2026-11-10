"""已进入 running 的执行共享结算流程，框架故障只保留恢复证据。"""
import pytest

from tasklite import FileSpec, Job, StateSnapshot, TaskLite, TaskResult
from tasklite.engine.resource import CapacityResource
from tasklite.utils.artifact import ArtifactPathPolicy


def _success(job, ctx):
    ctx.set_cursor("result", "committed")
    ctx.spawn(Job("work", "child"))
    return TaskResult.success()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("fault", [None, "cleanup", "commit"])
def test_pre_delete_failure_uses_one_completion(tmp_path, monkeypatch, backend, fault):
    target = tmp_path / "target"
    target.write_text("business", encoding="utf-8")
    pipeline = TaskLite(backend=backend, state_path=tmp_path / "state.db")
    pipeline.authorizer = lambda path, kind: True
    capacity = CapacityResource("disk", max_capacity=1)
    pipeline.register_resource(capacity)
    pipeline.register_task("work", _success, resources={"disk": 1})
    job = Job("work", "one", files={
        "target": FileSpec(target, role="cache", delete_existing=True, delete_on_failure=True),
    })
    pipeline.enqueue(job)
    runtime = pipeline._runtime
    events = []
    completed = []
    original_complete = runtime._completion.complete_job
    original_release = capacity.release
    original_cleanup = ArtifactPathPolicy.cleanup
    original_commit = runtime.store.backend.commit_job_settlement
    error = OSError("execution commit unavailable")

    def fail_pre_delete(*args):
        events.append("pre_delete")
        raise PermissionError("pre-delete denied by filesystem")

    def cleanup(policy, declaration, **kwargs):
        events.append("cleanup")
        assert capacity.used == 1
        if fault == "cleanup":
            raise PermissionError("failure cleanup denied")
        return original_cleanup(policy, declaration, **kwargs)

    def release(amount):
        events.append("release")
        row, = runtime.store.backend.load_queue_jobs(status="running")
        assert row["job_uid"] == job.uid
        return original_release(amount)

    def commit(*args, **kwargs):
        events.append("commit")
        assert capacity.used == 0
        if fault == "commit":
            raise error
        return original_commit(*args, **kwargs)

    def complete(entry, result, **kwargs):
        completed.append(entry)
        assert entry.handle is None
        assert entry.job_uid in runtime.store.state.in_flight_uids
        return original_complete(entry, result, **kwargs)

    monkeypatch.setattr("tasklite.engine.dispatch._execute_pre_deletes", fail_pre_delete)
    monkeypatch.setattr(ArtifactPathPolicy, "cleanup", cleanup)
    monkeypatch.setattr(capacity, "release", release)
    monkeypatch.setattr(runtime.store.backend, "commit_job_settlement", commit)
    monkeypatch.setattr(runtime._completion, "complete_job", complete)
    if fault == "commit":
        with pytest.raises(OSError) as caught:
            pipeline.run(install_signals=False)
        assert caught.value is error
        assert runtime.store.backend.load_settled_attempts(job.uid) == []
        assert runtime.store.backend.load_failed() == {}
        assert runtime.store.state.failed == {}
        row, = runtime.store.backend.load_queue_jobs(status="running")
        assert (row["run_id"], row["attempt_no"]) == (completed[0].run_id, 1)
    else:
        pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as snapshot:
            record = snapshot.get_job(job.uid)
            attempt, = snapshot.iter_attempts(job_uid=job.uid)
            assert record["status"] == "failed"
            assert attempt["outcome"] == "failure"
            assert attempt["attempt_no"] == record["attempt_no"] == 1
            assert attempt["run_id"] == record["run_id"] == completed[0].run_id
            assert attempt["started_at"] is None
            assert attempt["files"]["target"]["path"] == str(target)
            assert attempt["result"]["exception"]["type"] == "PermissionError"
            if fault == "cleanup":
                assert attempt["result"]["annotations"]["cleanup_error"] == "failure cleanup denied"
        assert target.exists() == (fault == "cleanup")
    assert len(completed) == 1
    assert events == ["pre_delete", "cleanup", "release", "commit"]
    assert capacity.used == 0
    assert pipeline.get_cursor("result") is None
    assert runtime.store.backend.load_queue_jobs(status="pending") == []
    pipeline.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("stage", ["context", "spawn", "interrupt", "pre_delete_framework"])
def test_launch_framework_fault_preserves_running_without_completion(tmp_path, monkeypatch, backend, stage):
    target = tmp_path / "target"
    target.write_text("business", encoding="utf-8")
    pipeline = TaskLite(backend=backend, state_path=tmp_path / "state.db")
    pipeline.authorizer = lambda path, kind: True
    capacity = CapacityResource("disk", max_capacity=1)
    pipeline.register_resource(capacity)
    pipeline.register_task("work", _success, resources={"disk": 1})
    pipeline.enqueue(Job("work", "one", files={
        "target": FileSpec(target, role="cache", delete_on_failure=True),
    }))
    runtime = pipeline._runtime
    error = (KeyboardInterrupt() if stage == "interrupt" else
             RuntimeError("pre-delete framework unavailable") if stage == "pre_delete_framework" else
             OSError("launch framework unavailable"))
    completed = []
    cleanup = []

    def fail(*args, **kwargs):
        raise error

    if stage == "pre_delete_framework":
        monkeypatch.setattr("tasklite.engine.dispatch._execute_pre_deletes", fail)
    else:
        monkeypatch.setattr(runtime._dispatch if stage == "context" else runtime.channel,
                            "_build_context" if stage == "context" else "spawn", fail)
    monkeypatch.setattr(runtime._completion, "complete_job", lambda *args, **kwargs: completed.append(args))
    monkeypatch.setattr(ArtifactPathPolicy, "cleanup", lambda *args, **kwargs: cleanup.append(args))
    with pytest.raises(type(error)) as caught:
        pipeline.run(install_signals=False)
    assert caught.value is error
    row, = runtime.store.backend.load_queue_jobs(status="running")
    assert runtime.store.backend.load_settled_attempts("work::one") == []
    assert runtime.store.backend.load_failed() == {}
    assert target.read_text(encoding="utf-8") == "business"
    assert capacity.used == 0
    assert completed == cleanup == []
    assert runtime.store.state.queue == []
    monkeypatch.undo()
    pipeline.run(install_signals=False)
    attempt, = runtime.store.backend.load_settled_attempts("work::one")
    assert attempt["outcome"] == "failure"
    assert attempt["run_id"] == row["run_id"]
    assert attempt["attempt_no"] == row["attempt_no"]
    assert target.read_text(encoding="utf-8") == "business"
    pipeline.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("decision", [True, False, "stop"])
def test_pre_delete_failure_defers_completion_until_cleanup_authorization(tmp_path, monkeypatch, backend, decision):
    target = tmp_path / "target"
    target.write_text("business", encoding="utf-8")
    pipeline = TaskLite(backend=backend, state_path=tmp_path / "state.db")
    decisions = []
    completed = []
    directory_decision = [None]

    def authorize(path, kind):
        decisions.append(kind)
        return True if kind == "file" else directory_decision[0]

    pipeline.authorizer = authorize
    pipeline.register_task("work", _success)
    pipeline.enqueue(Job("work", "one", files={
        "target": FileSpec(target, role="cache", delete_existing=True, delete_on_failure=True),
    }))
    runtime = pipeline._runtime
    original_step = runtime.step
    original_complete = runtime._completion.complete_job
    waiting = []

    def fail_pre_delete(*args):
        target.unlink()
        target.mkdir()
        (target / "child").write_text("business", encoding="utf-8")
        raise OSError("pre-delete failed after type change")

    def complete(entry, result, **kwargs):
        completed.append(entry)
        return original_complete(entry, result, **kwargs)

    def step(*args, **kwargs):
        result = original_step(*args, **kwargs)
        if runtime._completion.waiting_authorization and not waiting:
            waiting.append(True)
            assert completed == []
            assert runtime.in_flight["work::one"].handle is None
            assert runtime.store.backend.load_settled_attempts("work::one") == []
            if decision == "stop":
                pipeline.stop()
            else:
                directory_decision[0] = decision
        return result

    monkeypatch.setattr("tasklite.engine.dispatch._execute_pre_deletes", fail_pre_delete)
    monkeypatch.setattr(runtime._completion, "complete_job", complete)
    monkeypatch.setattr(runtime, "step", step)
    pipeline.run(install_signals=False)
    assert waiting == [True]
    assert len(completed) == 1
    attempt, = runtime.store.backend.load_settled_attempts("work::one")
    assert attempt["outcome"] == "failure"
    assert attempt["attempt_no"] == 1
    assert attempt["run_id"] == completed[0].run_id
    assert not runtime._completion.waiting_authorization
    assert not runtime.in_flight
    if decision is True:
        assert not target.exists()
        assert "work::one" in runtime.store.backend.load_failed()
    else:
        assert (target / "child").read_text(encoding="utf-8") == "business"
        pending, = runtime.store.backend.load_queue_jobs(status="pending")
        assert pending["job_uid"] == "work::one"
        assert runtime.store.backend.load_failed() == {}
        assert attempt["result"]["annotations"]["authorization_status"] == (
            "stopped" if decision == "stop" else "denied"
        )
    assert decisions[0] == "file"
    assert "directory" in decisions
    pipeline.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_stop_after_pre_delete_rolls_back_without_result(tmp_path, monkeypatch, backend):
    target = tmp_path / "target"
    target.write_text("business", encoding="utf-8")
    pipeline = TaskLite(backend=backend, state_path=tmp_path / "state.db")
    pipeline.authorizer = lambda path, kind: True
    capacity = CapacityResource("disk", max_capacity=1)
    pipeline.register_resource(capacity)
    pipeline.register_task("work", _success, resources={"disk": 1})
    pipeline.enqueue(Job("work", "one", files={
        "target": FileSpec(target, role="cache", delete_existing=True, delete_on_failure=True),
    }))
    runtime = pipeline._runtime
    completed = []
    releases = []
    original_release = capacity.release
    from tasklite.engine.dispatch import _execute_pre_deletes

    def delete_then_stop(*args):
        _execute_pre_deletes(*args)
        pipeline.stop()

    def release(amount):
        releases.append(amount)
        return original_release(amount)

    monkeypatch.setattr("tasklite.engine.dispatch._execute_pre_deletes", delete_then_stop)
    monkeypatch.setattr(runtime._completion, "complete_job", lambda *args, **kwargs: completed.append(args))
    monkeypatch.setattr(capacity, "release", release)
    pipeline.run(install_signals=False)
    pending, = runtime.store.backend.load_queue_jobs(status="pending")
    assert pending["job_uid"] == "work::one"
    assert runtime.store.backend.load_settled_attempts("work::one") == []
    assert runtime.store.backend.load_failed() == {}
    assert runtime.store.backend.get_attempt_sequence("work::one") == 1
    assert completed == []
    assert releases == [1]
    assert capacity.used == 0
    assert not target.exists()
    assert not runtime.in_flight
    pipeline.close()
