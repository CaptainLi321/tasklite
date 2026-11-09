"""触发器与任务模板的公开契约。"""
from tasklite import Job, TaskLite, TaskLiteError, TaskResult


def handle(job, ctx):
    return TaskResult.success()


def test_custom_trigger_observes_inherited_task_spec():
    pipeline = TaskLite(backend="memory")
    seen = []
    pipeline.register_trigger("inspect", lambda ctx: seen.append(ctx.job.to_dict()) or False)
    pipeline.register_task("t", handle, max_retries_per_run=7, timeout_seconds=12,
                           trigger="inspect")
    pipeline.enqueue(Job("t", "a"))
    res = pipeline.run()
    assert res is None
    assert pipeline._runtime.store.backend.load_queue() == []
    assert seen[0]["max_retries_per_run"] == 7
    assert seen[0]["timeout_seconds"] == 12
    assert seen[0]["trigger"] == "inspect"


def test_fresh_trigger_skip_allows_dependent_trigger():
    pipeline = TaskLite(backend="memory")
    seen = []
    pipeline.register_trigger("observe", lambda ctx: seen.append(ctx.job.uid) or False)
    pipeline.register_task("t", handle, trigger="observe")
    pipeline.enqueue(Job("t", "a"))
    pipeline.enqueue(Job("t", "b", depends_on=["t::a"]))
    res = pipeline.run()
    assert res is None
    # 上游被 trigger 跳过移除候选后，下游因缺失终态记录直接失败，不得获得就绪例外
    assert seen == ["t::a"]
    assert "t::b" in pipeline._runtime.store.backend.load_failed()


def test_task_resource_merge_and_explicit_false_zero_overrides():
    from tasklite.resource import CapacityResource
    pipeline = TaskLite(backend="memory")
    pipeline.register_resource(CapacityResource("gpu", max_capacity=5.0))
    pipeline.register_resource(CapacityResource("cpu", max_capacity=5.0))
    pipeline.register_task("t", handle, resources={"gpu": 2, "cpu": 1},
                           max_retries_per_run=7, timeout_seconds=12,
                           trigger="always")
    pipeline.enqueue(Job("t", "a", resources={"gpu": 0}, max_retries_per_run=0,
                         timeout_seconds=2, trigger="once"))
    row = pipeline._runtime.store.backend.load_queue()[0]
    assert row["resources"] == {"gpu": 0, "cpu": 1, "__workers__": 1.0}
    assert (row["max_retries_per_run"], row["timeout_seconds"], row["trigger"]) == (0, 2, "once")


def test_restored_none_spec_inherits_task_before_trigger():
    pipeline = TaskLite(backend="memory")
    seen = []
    pipeline.register_trigger("inspect", lambda ctx: seen.append(ctx.job.max_retries_per_run) or False)
    pipeline.register_task("t", handle, max_retries_per_run=9, trigger="inspect")
    snapshot = Job("t", "a").to_dict()
    assert Job.from_dict(snapshot).to_dict() == snapshot
    pipeline._runtime.store.backend.save_queue([snapshot])
    pipeline.run()
    assert seen == [9]


def test_unknown_trigger_raises_preserving_candidate_and_budget(tmp_path):
    import pytest
    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "trigger.db")
    pipeline.register_task("t", handle)
    pipeline.enqueue(Job("t", "a"))
    before_queue = pipeline._runtime.store.backend.load_queue()
    before_successes = pipeline._runtime.store.backend.load_successes()
    before_failed = pipeline._runtime.store.backend.load_failed()
    with pytest.raises(ValueError, match="unregistered trigger"):
        pipeline.enqueue(Job("t", "b", trigger="missing"))
    assert pipeline._runtime.store.backend.load_queue() == before_queue
    assert pipeline._runtime.store.backend.load_successes() == before_successes
    assert pipeline._runtime.store.backend.load_failed() == {}
    assert pipeline._runtime.store.backend.load_attempts("t::b") == []


def test_inherited_unknown_trigger_rejected_at_enqueue(tmp_path):
    import pytest
    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "trigger.db")
    pipeline.register_task("t", handle, trigger="missing")
    with pytest.raises(ValueError, match="unregistered trigger"):
        pipeline.enqueue(Job("t", "a"))
    assert pipeline._runtime.store.backend.load_queue() == []
    assert pipeline._runtime.store.backend.load_successes() == {}
    assert pipeline._runtime.store.backend.load_failed() == {}


def test_custom_trigger_is_evaluated_only_after_dependency_readiness():
    pipeline = TaskLite(backend="memory")
    seen = []
    pipeline.register_trigger("observe", lambda ctx: seen.append(ctx.job.uid) or True)
    pipeline.register_task("t", handle, trigger="observe")
    pipeline.enqueue(Job("t", "a"))
    pipeline.enqueue(Job("t", "b", depends_on=["t::a"]))
    pipeline.run()
    assert seen == ["t::a", "t::b"]


def test_callback_failure_and_non_bool_return_preserve_queue():
    import pytest
    for name, fn, error in [
        ("error", lambda ctx: 1 / 0, ZeroDivisionError),
        ("integer", lambda ctx: 1, TypeError),
        ("none", lambda ctx: None, TypeError),
    ]:
        pipeline = TaskLite(backend="memory")
        pipeline.register_trigger(name, fn)
        pipeline.register_task("t", handle, trigger=name)
        pipeline.enqueue(Job("t", "a"))
        before = pipeline._runtime.store.backend.load_queue()
        with pytest.raises(error):
            pipeline.run()
        assert pipeline._runtime.store.backend.load_queue() == before
        assert pipeline._runtime.store.backend.load_attempts("t::a") == []
        assert pipeline._runtime.store.backend.load_failed() == {}


def test_register_trigger_rejects_duplicate_builtin_and_invalid_callable():
    import pytest
    from tasklite.testing import running
    pipeline = TaskLite(backend="memory")
    for name in ["once", "until_success", "on_input_change", "always"]:
        with pytest.raises(ValueError):
            pipeline.register_trigger(name, lambda ctx: True)
    with pytest.raises(ValueError):
        pipeline.register_trigger("", lambda ctx: True)
    with pytest.raises(TypeError):
        pipeline.register_trigger("custom", 1)
    pipeline.register_trigger("custom", lambda ctx: True)
    with pytest.raises(ValueError):
        pipeline.register_trigger("custom", lambda ctx: True)
    with running(pipeline), pytest.raises(TaskLiteError):
        pipeline.register_trigger("another", lambda ctx: True)


def test_builtin_trigger_matrix():
    from tasklite.models.trigger import TriggerContext
    from tasklite.triggers import BUILTIN_TRIGGER_FUNCTIONS
    for name, expected in {
        "once": (True, False, False),
        "until_success": (True, False, True),
        "always": (True, True, True),
        "on_input_change": (True, True, True),
    }.items():
        outcomes = tuple(BUILTIN_TRIGGER_FUNCTIONS[name](
            TriggerContext(Job("t", "a"), succeeded, failed, {} if succeeded else None, None)
        ) for succeeded, failed in [(False, False), (True, False), (False, True)])
        assert outcomes == expected


def test_mtime_microsecond_boundary_size_only_and_no_cache(tmp_path):
    import os
    from tasklite.models.trigger import TriggerContext
    from tasklite.triggers import on_input_change
    path = tmp_path / "input"
    path.write_text("first")
    baseline_ns = 1_700_000_000_123_456_000
    ctx = TriggerContext(Job("t", "a"), True, False,
                         {"inputs": [str(path)]}, baseline_ns // 1000)
    for offset, expected in [(-1000, False), (0, False), (999, False), (1000, True)]:
        os.utime(path, ns=(baseline_ns, baseline_ns + offset))
        assert on_input_change(ctx) is expected
    path.write_text("changed length")
    os.utime(path, ns=(baseline_ns, baseline_ns))
    assert on_input_change(ctx) is False
    os.utime(path, ns=(baseline_ns, baseline_ns + 1000))
    assert on_input_change(ctx) is True


def test_directory_mtime_does_not_recurse(tmp_path):
    import os
    from tasklite.models.trigger import TriggerContext
    from tasklite.triggers import on_input_change
    folder = tmp_path / "folder"
    folder.mkdir()
    child = folder / "child"
    child.write_text("first")
    baseline_ns = 1_700_000_000_000_000_000
    os.utime(folder, ns=(baseline_ns, baseline_ns))
    child.write_text("changed")
    os.utime(child, ns=(baseline_ns, baseline_ns + 1000))
    ctx = TriggerContext(Job("t", "a"), True, False,
                         {"inputs": [str(folder)]}, baseline_ns // 1000)
    assert on_input_change(ctx) is False


def test_input_contract_errors_and_untrusted_baseline(tmp_path):
    import pytest
    from tasklite.models.trigger import TriggerContext
    from tasklite.triggers import on_input_change
    for inputs in [None, [], "bad", [42], [""]]:
        ctx = TriggerContext(Job("t", "a"), True, False, {"inputs": inputs}, 1)
        with pytest.raises(ValueError):
            on_input_change(ctx)
        assert on_input_change(TriggerContext(ctx.job, True, False, ctx.success_metadata, None)) is True
    existing = tmp_path / "exists"
    existing.write_text("new")
    missing = tmp_path / "missing"
    ctx = TriggerContext(Job("t", "a"), True, False,
                         {"inputs": [str(existing), str(missing)]}, 1)
    with pytest.raises(FileNotFoundError):
        on_input_change(ctx)


def test_failure_history_skipped_this_batch_allows_downstream():
    pipeline = TaskLite(backend="memory")
    pipeline.register_task("t", handle)
    pipeline._runtime.store.backend.append_failed("t::a", {"error_message": "prior failure"})
    seen = []
    pipeline.register_trigger("observe", lambda ctx: seen.append(ctx.job.uid) or False)
    pipeline.enqueue(Job("t", "a", trigger="observe"))
    pipeline.enqueue(Job("t", "b", depends_on=["t::a"], trigger="observe"))
    pipeline.run()
    # 上游被 trigger 跳过后，按终态档案判定；上游仅有 failure，下游级联失败，不得求值 trigger
    assert seen == ["t::a"]
    assert set(pipeline._runtime.store.backend.load_failed().keys()) == {"t::a", "t::b"}


def test_trigger_skip_is_local_to_run():
    from tasklite.selection import Exclude
    pipeline = TaskLite(backend="memory")
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
    assert list(pipeline._runtime.store.backend.load_failed().keys()) == ["child::b"]


def test_stop_after_trigger_skip_does_not_persist_dependency_readiness():
    pipeline = TaskLite(backend="memory")
    def stop_and_skip(ctx):
        pipeline.stop()
        return False
    from tasklite.selection import Exclude
    pipeline.register_trigger("stop", stop_and_skip)
    pipeline.register_task("t", handle)
    pipeline.register_task("child", handle)
    pipeline.enqueue(Job("t", "a", trigger="stop"))
    pipeline.enqueue(Job("child", "b", depends_on=["t::a"]))
    pipeline.run(exclude=Exclude(task_type="child"))
    assert [row["job_id"] for row in pipeline._runtime.store.backend.load_queue()] == ["b"]
    pipeline.run()
    assert list(pipeline._runtime.store.backend.load_failed().keys()) == ["child::b"]


def test_trigger_skip_does_not_grant_dependency_readiness():
    from tasklite.models.state import PipelineState
    state = PipelineState({}, {}, {}, [])
    state.mark_trigger_skipped("t::a")
    assert not state.dependency_ready("t::a")


def test_trigger_error_aborts_inflight_without_burning_budget(monkeypatch):
    import multiprocessing as mp
    import pytest
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
    monkeypatch.setattr(mp, "Process", WaitingProcess)
    pipeline = TaskLite(backend="memory", max_workers=2)
    pipeline.register_trigger("error", lambda ctx: 1 / 0)
    pipeline.register_task("t", handle)
    pipeline.enqueue(Job("t", "a"))
    pipeline.enqueue(Job("t", "b", trigger="error"))
    with pytest.raises(ZeroDivisionError):
        pipeline.run()
    queue_rows = pipeline._runtime.store.backend.load_queue_jobs()
    assert {row["job_uid"]: row["status"] for row in queue_rows} == {
        "t::a": "running", "t::b": "pending",
    }
    rows = {row["job"]["job_id"]: row["job"] for row in queue_rows}
    assert set(rows) == {"a", "b"}
    assert all(row["attempt_no"] == 1 for row in rows.values())
    assert pipeline._runtime.store.backend.load_attempts("t::b") == []
    assert pipeline._runtime.store.backend.load_failed() == {}
