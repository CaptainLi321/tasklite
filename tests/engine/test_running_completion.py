"""已进入 running 的执行共享结算流程，框架故障只保留恢复证据。"""
from contextlib import ExitStack
from pathlib import Path
import traceback

import pytest

from tasklite import FileSpec, Job, StateSnapshot, TaskLite, TaskResult
from tasklite.resource import CapacityResource
from tasklite.testing import (
    authorizer,
    cleanup_hook,
    get_resource,
    in_process_workers,
    inject_resource,
)


def _success(job, ctx):
    ctx.set_cursor("result", "committed")
    ctx.spawn(Job("work", "child"))
    return TaskResult.success()


@pytest.mark.parametrize("fault", [None, "cleanup", "commit"])
def test_pre_delete_failure_uses_one_completion(tmp_path, monkeypatch, fault):
    target = tmp_path / "target"
    target.write_text("business", encoding="utf-8")
    pipeline = TaskLite(backend="memory")
    pipeline.register_resource(CapacityResource("disk", max_capacity=1))
    capacity = get_resource(pipeline, "disk")
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
    original_commit = runtime.store.backend.commit_job_settlement
    error = OSError("execution commit unavailable")

    def delete_target(original_delete, path, *, allow_broken_leaf_symlink, **kwargs):
        if allow_broken_leaf_symlink:
            events.append("pre_delete")
            raise PermissionError("pre-delete denied by filesystem")
        events.append("cleanup")
        assert capacity.used == 1
        if fault == "cleanup":
            raise PermissionError("failure cleanup denied")
        return original_delete(
            path, allow_broken_leaf_symlink=allow_broken_leaf_symlink, **kwargs
        )

    def release(amount):
        events.append("release")
        with StateSnapshot.open(pipeline) as snap:
            assert snap.get_job(job.uid)["status"] == "running"
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
        return original_complete(entry, result, **kwargs)

    monkeypatch.setattr(capacity, "release", release)
    monkeypatch.setattr(runtime.store.backend, "commit_job_settlement", commit)
    monkeypatch.setattr(runtime._completion, "complete_job", complete)
    with (
        in_process_workers(pipeline),
        cleanup_hook(pipeline, delete_target),
        authorizer(pipeline, lambda path, kind: True),
    ):
        if fault == "commit":
            with pytest.raises(OSError) as caught:
                pipeline.run(install_signals=False)
            assert caught.value is error
            with StateSnapshot.open(pipeline) as snapshot:
                record = snapshot.get_job(job.uid)
                assert record["status"] == "running"
                assert (record["run_id"], record["attempt_no"]) == (completed[0].run_id, 1)
                assert list(snapshot.iter_attempts(job_uid=job.uid)) == []
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
    pipeline.close()


@pytest.mark.parametrize("stage", ["context", "spawn", "process_start", "interrupt", "pre_delete_framework"])
def test_launch_framework_fault_preserves_running_without_completion(tmp_path, monkeypatch, stage):
    target = tmp_path / "target"
    target.write_text("business", encoding="utf-8")
    pipeline = TaskLite(backend="memory")
    pipeline.register_resource(CapacityResource("disk", max_capacity=1))
    capacity = get_resource(pipeline, "disk")
    pipeline.register_task("work", _success, resources={"disk": 1})
    pipeline.enqueue(Job("work", "one", files={
        "target": FileSpec(target, role="cache", delete_existing=stage == "pre_delete_framework", delete_on_failure=True),
    }))
    runtime = pipeline._runtime
    error = (KeyboardInterrupt() if stage == "interrupt" else
             RuntimeError("pre-delete framework unavailable") if stage == "pre_delete_framework" else
             OSError("launch framework unavailable"))
    completed = []
    cleanup = []

    def fail(*args, **kwargs):
        raise error

    from tasklite.engine.launcher import PreparedWorker, WorkerLauncher
    from tasklite.testing import InProcessWorkerLauncher
    from tasklite.utils.ipc import create_worker_pipe, create_worker_reply_pipe

    class _NoopHandle:
        pid = None
        exitcode = None
        was_force_killed = False

        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

        def kill_tree(self):
            pass

        def close(self):
            pass

    class _FailingReleaseLauncher(WorkerLauncher):
        def prepare(self, spec, *, on_message, cancel_event):
            receiver, sender = create_worker_pipe(
                job_uid=spec.job.uid,
                execution_id=spec.execution_id,
                run_id=spec.run_id or None,
                attempt_no=spec.attempt_no,
                on_message=on_message,
            )
            reply_receiver, reply_sender = create_worker_reply_pipe(
                job_uid=spec.job.uid,
                execution_id=spec.execution_id,
                run_id=spec.run_id or None,
                attempt_no=spec.attempt_no,
            )

            def _abort():
                sender.close()
                reply_receiver.close()
                reply_sender.close()
                receiver.close(timeout=2.0)

            return PreparedWorker(
                handle=_NoopHandle(),
                receiver=receiver,
                reply_sender=reply_sender,
                on_release=fail,
                on_abort=_abort,
            )

    class _FailingPrepareLauncher(WorkerLauncher):
        def prepare(self, spec, *, on_message, cancel_event):
            if stage == "context":
                # 验证冷启动中途建立管道后出错也能完整回收并返回原始异常
                receiver, sender = create_worker_pipe(
                    job_uid=spec.job.uid,
                    execution_id=spec.execution_id,
                    run_id=spec.run_id or None,
                    attempt_no=spec.attempt_no,
                    on_message=on_message,
                )
                try:
                    raise error
                finally:
                    sender.close()
                    receiver.close(timeout=2.0)
            raise error

    def guarded_delete(_orig, *args, allow_broken_leaf_symlink=False, **kwargs):
        if stage == "pre_delete_framework" and allow_broken_leaf_symlink:
            raise error
        cleanup.append(args)

    monkeypatch.setattr(runtime._completion, "complete_job", lambda *args, **kwargs: completed.append(args))
    launcher = (
        _FailingReleaseLauncher()
        if stage == "process_start"
        else (_FailingPrepareLauncher() if stage in ("context", "spawn", "interrupt") else None)
    )
    with (
        in_process_workers(pipeline, launcher),
        authorizer(pipeline, lambda path, kind: True),
        cleanup_hook(pipeline, guarded_delete),
    ):
        with pytest.raises(type(error)) as caught:
            pipeline.run(install_signals=False)
        assert caught.value is error
        with StateSnapshot.open(pipeline) as snap:
            running_row = snap.get_job("work::one")
            assert running_row["status"] == "running"
            assert list(snap.iter_attempts(job_uid="work::one")) == []
        assert target.read_text(encoding="utf-8") == "business"
        assert capacity.used == 0
        assert completed == cleanup == []
        monkeypatch.undo()
    with in_process_workers(pipeline), authorizer(pipeline, lambda path, kind: True):
        pipeline.run(install_signals=False)

    with StateSnapshot.open(pipeline) as snap:
        failed_row = snap.get_job("work::one")
        attempt, = snap.iter_attempts(job_uid="work::one")
        assert failed_row["status"] == "failed"
        assert attempt["outcome"] == "failure"
        assert attempt["started_at"] is None
        assert attempt["run_id"] == running_row["run_id"]
        assert attempt["attempt_no"] == running_row["attempt_no"]
    assert target.read_text(encoding="utf-8") == "business"
    pipeline.close()


@pytest.mark.parametrize("decision", [True, False, "stop"])
def test_pre_delete_failure_defers_completion_until_cleanup_authorization(tmp_path, monkeypatch, decision):
    target = tmp_path / "target"
    target.write_text("business", encoding="utf-8")
    pipeline = TaskLite(backend="memory")
    decisions = []
    completed = []
    directory_decision = [None]

    def authorize(path, kind):
        decisions.append(kind)
        return True if kind == "file" else directory_decision[0]

    pipeline.register_task("work", _success)
    pipeline.enqueue(Job("work", "one", files={
        "target": FileSpec(target, role="cache", delete_existing=True, delete_on_failure=True),
    }))
    runtime = pipeline._runtime
    original_step = runtime.step
    original_complete = runtime._completion.complete_job
    waiting = []

    def fail_pre_delete(original_delete, path, *, allow_broken_leaf_symlink, **kwargs):
        if allow_broken_leaf_symlink:
            target.unlink()
            target.mkdir()
            (target / "child").write_text("business", encoding="utf-8")
            raise OSError("pre-delete failed after type change")
        return original_delete(
            path, allow_broken_leaf_symlink=allow_broken_leaf_symlink, **kwargs
        )

    def complete(entry, result, **kwargs):
        completed.append(entry)
        return original_complete(entry, result, **kwargs)

    def step(*args, **kwargs):
        result = original_step(*args, **kwargs)
        if runtime.authorization.has_pending_authorization and not waiting:
            waiting.append(True)
            assert completed == []
            assert runtime.in_flight["work::one"].handle is None
            with StateSnapshot.open(pipeline) as snap:
                assert list(snap.iter_attempts(job_uid="work::one")) == []
            if decision == "stop":
                pipeline.stop()
            else:
                directory_decision[0] = decision
        return result

    monkeypatch.setattr(runtime._completion, "complete_job", complete)
    monkeypatch.setattr(runtime, "step", step)
    with (
        in_process_workers(pipeline),
        cleanup_hook(pipeline, fail_pre_delete),
        authorizer(pipeline, authorize),
    ):
        pipeline.run(install_signals=False)
    assert waiting == [True]
    assert len(completed) == 1
    with StateSnapshot.open(pipeline) as snap:
        attempt, = snap.iter_attempts(job_uid="work::one")
        rec = snap.get_job("work::one")
    assert attempt["outcome"] == "failure"
    assert attempt["attempt_no"] == 1
    assert attempt["run_id"] == completed[0].run_id
    assert not runtime.authorization.has_pending_authorization
    assert not runtime.in_flight
    if decision is True:
        assert not target.exists()
        assert rec["status"] == "failed"
    else:
        assert (target / "child").read_text(encoding="utf-8") == "business"
        assert rec["status"] == "pending"
        assert attempt["result"]["annotations"]["authorization_status"] == (
            "stopped" if decision == "stop" else "denied"
        )
    assert decisions[0] == "file"
    assert "directory" in decisions
    pipeline.close()


@pytest.mark.parametrize("stage", ["pre_delete", "context", "process"])
def test_stop_before_worker_start_rolls_back_without_result(tmp_path, monkeypatch, stage):
    target = tmp_path / "target"
    target.write_text("business", encoding="utf-8")
    pipeline = TaskLite(backend="memory")
    pipeline.register_resource(CapacityResource("disk", max_capacity=1))
    capacity = get_resource(pipeline, "disk")
    pipeline.register_task("work", _success, resources={"disk": 1})
    pipeline.enqueue(Job("work", "one", files={
        "target": FileSpec(target, role="cache", delete_existing=True, delete_on_failure=True),
    }))
    runtime = pipeline._runtime
    completed = []
    releases = []
    original_release = capacity.release

    def delete_then_stop(original_delete, path, *args, **kwargs):
        res = original_delete(path, *args, **kwargs)
        if stage == "pre_delete":
            pipeline.stop()
        return res

    from tasklite.engine.launcher import PreparedWorker, WorkerLauncher, _WorkerLaunchCancelled
    from tasklite.utils.ipc import create_worker_pipe, create_worker_reply_pipe

    class _NoopHandle:
        pid = None
        exitcode = None
        was_force_killed = False

        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

        def kill_tree(self):
            pass

        def close(self):
            pass

    class _StopOnPrepareLauncher(WorkerLauncher):
        def prepare(self, spec, *, on_message, cancel_event):
            pipeline.stop()
            if stage == "context":
                raise _WorkerLaunchCancelled("stop requested during worker preparation")
            receiver, sender = create_worker_pipe(
                job_uid=spec.job.uid,
                execution_id=spec.execution_id,
                run_id=spec.run_id or None,
                attempt_no=spec.attempt_no,
                on_message=on_message,
            )
            reply_receiver, reply_sender = create_worker_reply_pipe(
                job_uid=spec.job.uid,
                execution_id=spec.execution_id,
                run_id=spec.run_id or None,
                attempt_no=spec.attempt_no,
            )

            def _abort():
                sender.close()
                reply_receiver.close()
                reply_sender.close()
                receiver.close(timeout=2.0)

            return PreparedWorker(
                handle=_NoopHandle(),
                receiver=receiver,
                reply_sender=reply_sender,
                on_release=lambda: None,
                on_abort=_abort,
            )

    def release(amount):
        releases.append(amount)
        return original_release(amount)

    monkeypatch.setattr(runtime._completion, "complete_job", lambda *args, **kwargs: completed.append(args))
    monkeypatch.setattr(capacity, "release", release)
    launcher = _StopOnPrepareLauncher() if stage in ("context", "process") else None
    with (
        in_process_workers(pipeline, launcher),
        cleanup_hook(pipeline, delete_then_stop),
        authorizer(pipeline, lambda path, kind: True),
    ):
        pipeline.run(install_signals=False)
    with StateSnapshot.open(pipeline) as snapshot:
        pending = snapshot.get_job("work::one")
        assert pending["status"] == "pending"
        assert pending["started_at"] is None
        assert list(snapshot.iter_attempts(job_uid="work::one")) == []
    assert runtime.store.backend.load_consistent_snapshot()["job_sequences"].get("work::one", 0) == 1
    assert completed == []
    assert releases == [1]
    assert capacity.used == 0
    assert not target.exists()
    assert not runtime.in_flight
    pipeline.close()


def _replace_cache_with_directory(job, ctx):
    if job.job_id != "committed":
        target = ctx.file_path("cache")
        target.unlink()
        target.mkdir()
        (target / "child").write_text("business", encoding="utf-8")
    return TaskResult.success()


@pytest.mark.parametrize("stage,cached", [
    ("prepare", ()),
    ("authorize", ()),
    ("authorize", ("fault",)),
    ("release", ("fault", "later")),
    ("commit", ("later", "fault")),
    ("commit", ()),
])
def test_completion_framework_fault_stops_batch_and_cached_results(
    tmp_path, monkeypatch, cached, stage,
):
    fault_name = cached[0] if cached else "fault"
    later_name = "later" if fault_name == "fault" else "fault"
    pipeline = TaskLite(backend="memory", max_workers=3)
    pipeline.register_resource(CapacityResource("disk", max_capacity=3))
    capacity = get_resource(pipeline, "disk")
    pipeline.register_task("work", _replace_cache_with_directory, resources={"disk": 1})
    pipeline.enqueue(Job("work", "committed"))
    for name in ("fault", "later"):
        target = tmp_path / name
        target.write_text("initial", encoding="utf-8")
        pipeline.enqueue(Job("work", name, files={
            "cache": FileSpec(target, role="output", delete_on_success=True),
        }))
    runtime = pipeline._runtime
    completion = runtime._completion
    store = runtime.store.backend
    original_reap = runtime.channel.reap_completed
    original_settle = completion.settle_reaped
    original_inspect = runtime.store.path_policy.adapter.inspect_entry
    original_release = runtime.config.resources.release_all
    original_commit = store.commit_job_settlement
    held = {}
    armed = []
    events = []
    releases = []
    identities = {}
    committed = []
    fault = RuntimeError("completion framework unavailable")
    cause = OSError("underlying framework unavailable")

    def fail():
        events.append(("fault", stage))
        raise fault from cause

    def authorize(path, kind):
        name = Path(path).name
        if armed:
            events.append(("authorize", name))
            if stage == "authorize" and name == fault_name:
                fail()
        return True if kind == "file" or armed else None

    def reap(handles):
        held.update({handle.job_uid: (handle, result) for handle, result in original_reap(
            [handle for handle in handles if handle.job_uid not in held]
        )})
        if len(held) < 3:
            return []
        return [held.pop("work::" + name) for name in ("committed", "fault", "later")]

    def settle(results):
        if not results:
            return original_settle(results)
        identities.update({row["job_uid"]: row for row in store.load_queue_jobs(status="running")})
        pairs = {handle.job_uid.split("::")[1]: (handle, result) for handle, result in results}
        original_settle([pairs["committed"], *(pairs[name] for name in cached)])
        committed.append([a for a in store.load_consistent_snapshot()["attempts"] if a["job_uid"] == "work::committed"])
        armed.append(True)
        return original_settle([pairs[name] for name in ("fault", "later") if name not in cached])

    def inspect_entry(path):
        if armed and stage == "prepare" and Path(path).name == fault_name:
            fail()
        return original_inspect(path)

    def release(*args, **kwargs):
        uid = kwargs["job_uid"]
        releases.append(uid)
        result = original_release(*args, **kwargs)
        if armed and stage == "release" and uid == "work::" + fault_name:
            fail()
        return result

    def commit(*args, **kwargs):
        if armed:
            events.append(("commit", args[0] if args else kwargs.get("job_uid")))
            if stage == "commit":
                fail()
        return original_commit(*args, **kwargs)

    def delete_target(original_delete, path, *args, **kwargs):
        if armed:
            events.append(("cleanup", Path(path).name))
        return original_delete(path, *args, **kwargs)

    monkeypatch.setattr(runtime.channel, "reap_completed", reap)
    monkeypatch.setattr(completion, "settle_reaped", settle)
    monkeypatch.setattr(runtime.store.path_policy.adapter, "inspect_entry", inspect_entry)
    monkeypatch.setattr(runtime.config.resources, "release_all", release)
    monkeypatch.setattr(store, "commit_job_settlement", commit)
    with in_process_workers(pipeline), authorizer(pipeline, authorize):
        with cleanup_hook(pipeline, delete_target):
            with pytest.raises(RuntimeError) as caught:
                pipeline.run(install_signals=False)
        assert caught.value is fault
        assert caught.value.__cause__ is cause
        assert "fail" in [frame.name for frame in traceback.extract_tb(caught.tb)]
        assert events[events.index(("fault", stage)) + 1:] == []
        assert all(name not in (later_name, "work::" + later_name) for _, name in events)
        assert (tmp_path / later_name / "child").read_text(encoding="utf-8") == "business"
        with StateSnapshot.open(pipeline) as snap:
            assert {r["job_uid"] for r in snap.iter_jobs(status="succeeded")} == {"work::committed"}
            assert list(snap.iter_jobs(status="failed")) == []
            assert {r["job_uid"] for r in snap.iter_jobs(status="running")} == {"work::fault", "work::later"}
        assert sorted(releases) == ["work::committed", "work::fault", "work::later"]
        assert capacity.used == 0
        assert not runtime.in_flight
        assert not runtime.authorization.has_pending_authorization
        monkeypatch.undo()
        pipeline.run(install_signals=False)
    with StateSnapshot.open(pipeline) as snap:
        for name in ("fault", "later"):
            uid = "work::" + name
            attempt, = snap.iter_attempts(job_uid=uid)
            row = identities[uid]
            assert attempt["outcome"] == "failure"
            assert attempt["started_at"] is None
            assert (attempt["run_id"], attempt["attempt_no"]) == (row["run_id"], row["attempt_no"])
            assert snap.get_job(uid)["status"] == "failed"
    snap_after = store.load_consistent_snapshot()
    assert [a for a in snap_after["attempts"] if a["job_uid"] == "work::committed"] == committed[0]
    before = {uid: [a for a in snap_after["attempts"] if a["job_uid"] == uid] for uid in identities}
    with in_process_workers(pipeline):
        pipeline.run(install_signals=False)
    snap_final = store.load_consistent_snapshot()
    assert {uid: [a for a in snap_final["attempts"] if a["job_uid"] == uid] for uid in identities} == before
    assert (tmp_path / later_name / "child").read_text(encoding="utf-8") == "business"
    pipeline.close()


@pytest.mark.parametrize("stage", ["resource", "pre_delete", "context", "process"])
def test_running_snapshot_projects_queue_started_at_until_settlement(tmp_path, monkeypatch, stage):
    clock = ["2026-11-10T00:00:00Z"]
    launch_time = "2026-11-10T00:01:00Z"
    returned_time = "2026-11-10T00:02:00Z"
    target = tmp_path / "cache"
    target.write_text("business", encoding="utf-8")
    db_path = tmp_path / "state.db"
    # resource/pre_delete 在 sqlite 上校验双数据源一致；其余阶段在 memory 复用同契约
    backend = "sqlite" if stage in ("resource", "pre_delete") else "memory"
    with TaskLite(backend=backend, state_path=db_path if backend == "sqlite" else None) as pipeline, ExitStack() as snapshots:
        pipeline.register_resource(CapacityResource("disk", max_capacity=1))
        capacity = get_resource(pipeline, "disk")
        pipeline.register_task("work", _replace_cache_with_directory, resources={"disk": 1})
        pipeline.enqueue(Job("work", "committed", files={
            "cache": FileSpec(target, role="cache", delete_existing=True),
        }))
        runtime = pipeline._runtime
        sources = [pipeline]
        if backend == "sqlite":
            sources.append(db_path)
        for source in sources:
            with StateSnapshot.open(source) as snapshot:
                pending = snapshot.get_job("work::committed")
                assert list(snapshot.iter_jobs(status="pending")) == [pending]
                assert all(pending[key] is None for key in (
                    "result", "run_id", "attempt_no", "started_at", "settled_at",
                ))
        running_snapshots = []
        from tasklite.engine import dispatch

        identities = []
        starts = []
        submitted = []

        def record_running_snapshot():
            with StateSnapshot.open(pipeline) as base_snap:
                row, = list(base_snap.iter_jobs(status="running"))
            identities.append(row)
            assert not runtime.channel.active_handles()
            assert not runtime.in_flight
            for source in sources:
                snapshot = snapshots.enter_context(StateSnapshot.open(source))
                current = snapshot.get_job("work::committed")
                assert list(snapshot.iter_jobs(status="running")) == [current]
                assert current["status"] == "running"
                assert (current["run_id"], current["attempt_no"]) == (row["run_id"], row["attempt_no"])
                assert current["started_at"] == row["started_at"]
                assert all(current[key] is None for key in ("result", "settled_at"))
                assert list(snapshot.iter_attempts(job_uid="work::committed")) == []
                running_snapshots.append((snapshot, current))

        from tasklite.engine.launcher import PreparedWorker
        from tasklite.engine.resource import CapacityResource as InternalCapacityResource
        from tasklite.testing import InProcessWorkerLauncher

        class _TrackingLauncher(InProcessWorkerLauncher):
            def prepare(self, spec, *, on_message, cancel_event):
                if stage == "context":
                    record_running_snapshot()
                    clock[0] = launch_time
                prepared = super().prepare(
                    spec,
                    on_message=on_message,
                    cancel_event=cancel_event,
                )
                if stage == "process":
                    record_running_snapshot()
                    clock[0] = launch_time
                orig_release = prepared._on_release

                def tracked_release():
                    starts.append(clock[0])
                    orig_release()
                    clock[0] = returned_time

                return PreparedWorker(
                    handle=prepared.handle,
                    receiver=prepared.receiver,
                    reply_sender=prepared.reply_sender,
                    on_release=tracked_release,
                    on_abort=prepared.abort,
                )

        class _ObservingCapacity(InternalCapacityResource):
            def acquire(self, amount: float) -> None:
                record_running_snapshot()
                super().acquire(amount)
                clock[0] = launch_time

        def tracked_delete(original_delete, path, *args, **kwargs):
            if stage == "pre_delete":
                record_running_snapshot()
            result = original_delete(path, *args, **kwargs)
            if stage == "pre_delete":
                clock[0] = launch_time
            return result

        tracking_launcher = _TrackingLauncher()
        observed_capacity = _ObservingCapacity("disk", max_capacity=1) if stage == "resource" else capacity
        with (
            in_process_workers(pipeline, tracking_launcher),
            inject_resource(pipeline, "disk", observed_capacity),
            cleanup_hook(pipeline, tracked_delete),
        ):
            original_commit = runtime.store.backend.commit_job_settlement

            def commit(*args, **kwargs):
                submitted.append(kwargs["execution_id"])
                return original_commit(*args, **kwargs)

            monkeypatch.setattr(runtime.store.backend, "commit_job_settlement", commit)
            monkeypatch.setattr(dispatch, "utc_now_iso", lambda: clock[0])
            monkeypatch.setattr("tasklite.engine.launcher.utc_now_iso", lambda: clock[0])
            with authorizer(pipeline, lambda path, kind: True):
                pipeline.run(install_signals=False)

        assert starts == [launch_time]
        assert not target.exists()
        assert observed_capacity.used == 0
        row, = identities
        assert row["started_at"] == "2026-11-10T00:00:00Z"
        for source in sources:
            with StateSnapshot.open(source) as snapshot:
                record = snapshot.get_job("work::committed")
                assert list(snapshot.iter_jobs(status="succeeded")) == [record]
                attempt, = snapshot.iter_attempts(job_uid="work::committed")
                assert attempt["outcome"] == "success"
                assert record["started_at"] == attempt["started_at"] == launch_time
                assert (attempt["run_id"], attempt["attempt_no"]) == (row["run_id"], row["attempt_no"])
        for snapshot, current in running_snapshots:
            assert snapshot.get_job("work::committed") == current
            assert list(snapshot.iter_jobs(status="running")) == [current]
            assert list(snapshot.iter_attempts(job_uid="work::committed")) == []
        assert submitted == [f"{row['run_id']}.{row['attempt_no']}"]
