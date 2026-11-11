"""v2 CompletionMachine 完成机器测试：唯一收尾出口、三态分支与批量结算。

覆盖：complete_job 成功/失败/重试生命周期（资源释放、清理模式、收尾
钩子恰好一次）、显式重试预算判定与零预算显式重试、动态子任务去重与坏
子作业拒绝、
settle_reaped / settle_aborted 批量结算。
"""

from __future__ import annotations

from collections import defaultdict

from tasklite.triggers import BUILTIN_TRIGGER_FUNCTIONS

from typing import Any

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import TriggerRegistry
from tasklite.engine.channel import ExecutionChannel, ExecutionResult
from tasklite.engine.completion import CompletionMachine

from tasklite.engine.in_flight import InFlightJob, InFlightTracker
from tasklite.engine.resource import (
    CapacityResource,
    IntervalResource,
    ResourceManager,
)
from tasklite.engine.session import RunSession
from tasklite.engine.store import StateStore
from tasklite.engine.types import JobHandle
from tasklite.models.artifact import FileSpec
from tasklite.models.job import Job
from tasklite.models.state import PipelineState
from tasklite.models.task import Task, TaskRegistry


def _identity(entry):
    return {"execution_id": entry.execution_id, "run_id": entry.run_id, "attempt_no": entry.attempt_no}


class FakeProcess:
    def __init__(self, pid: int = 12345):
        self.pid = pid


class CompletionEnv:
    """完成机器窄依赖装配（字段即依赖清单）。"""

    def __init__(self, tmp_path, *, extra_resources=None):

        resources_map = {
            "__workers__": CapacityResource("__workers__", max_capacity=2.0),
        }
        if extra_resources:
            resources_map.update(extra_resources)
        self.resources = ResourceManager(resources_map)
        self.backend = InMemoryStateBackend()
        self.stats = defaultdict(int)
        self.tasks = TaskRegistry()
        self.trigger_registry = TriggerRegistry(
            tasks=self.tasks,
            functions=BUILTIN_TRIGGER_FUNCTIONS,
        )
        self.store = StateStore(
            self.backend,
            PipelineState({}, {}, {}, []),
            stats=self.stats,
            trigger_registry=self.trigger_registry,
        )
        self.session = RunSession()
        self.session.begin("b" * 32)
        self.in_flight = InFlightTracker()
        self.channel = ExecutionChannel()
        self.completion = CompletionMachine(
            store=self.store,
            trigger_registry=self.trigger_registry,
            channel=self.channel,
            resources=self.resources,
            in_flight=self.in_flight,
            session=self.session,
        )

    def register_task(self, task_type: str) -> None:
        self.tasks.register(Task(task_type, lambda job, ctx: None))

    def track_entry(
        self,
        job: Job,
        *,
        lease=None,
        execution_id=None,
        run_id=None,
        attempt_no=None,
        acquired=None,
        files=None,
    ) -> InFlightJob:
        if execution_id is None:
            identity = self.start_execution(job)
            execution_id, run_id, attempt_no = identity["execution_id"], identity["run_id"], identity["attempt_no"]
        job._attempt_no = attempt_no
        snapshot = job.to_dict()
        self.store.finalize_intake_snapshot(snapshot, job.task_type)
        job.__dict__.update(Job.from_dict(snapshot).__dict__)
        if acquired is None and lease is None:
            acquired = self.resources.acquire_all(job.task_type, job.resources, job_uid=job.uid)
        entry = InFlightJob(
            job_uid=job.uid,
            job_snapshot=job.to_dict(),
            job=job,
            lease=lease,
            acquired=acquired if acquired is not None else [],
            execution_id=execution_id,
            run_id=run_id,
            attempt_no=attempt_no,
            active_files=dict(files if files is not None else job.files),
            active_file_sources=dict(files if files is not None else job.files),
        )
        self.in_flight.track(entry, state=self.store.state)
        return entry


    def start_execution(self, job):
        self.backend.enqueue_jobs([job.to_dict()])
        next_no = self.backend.get_attempt_sequence(job.uid) + 1
        execution_id = f"{self.session.run_id}.{next_no}"
        attempt_no = self.backend.mark_job_running(
            job.uid, execution_id=execution_id, run_id=self.session.run_id,
            started_at="2026-01-01T00:00:00Z",
        )
        return {"execution_id": execution_id, "run_id": self.session.run_id, "attempt_no": attempt_no}

    def make_handle(self, uid: str) -> JobHandle:
        return JobHandle(
            job_uid=uid,
            process=FakeProcess(),
            timeout_at_monotonic=1e18,
            timeout_seconds=60.0,
            job=Job(*uid.split("::", 1)),
            execution_id=f"{self.session.run_id}.1",
        )


class TestCompleteJobSuccess:
    """complete_job 成功生命周期。"""

    def test_success_lifecycle(self, tmp_path, monkeypatch):
        env = CompletionEnv(tmp_path, extra_resources={"gpu": CapacityResource("gpu", max_capacity=10.0)})
        input_file = tmp_path / "in.csv"
        input_file.write_text("input", encoding="utf-8")
        cache_file = tmp_path / "temporary.cache"
        cache_file.write_text("temporary", encoding="utf-8")
        job = Job(
            "t",
            "j1",
            resources={"gpu": 2.0},
            files={
                "input": FileSpec(input_file, role="input"),
                "cache": FileSpec(
                    cache_file,
                    role="cache",
                    delete_on_success=True,
                ),
            },
        )
        identity = env.start_execution(job)
        entry = env.track_entry(job, **identity)
        env.store.authorizer = lambda *_args: True

        assert env.resources["gpu"].used == 2.0

        result = ExecutionResult(success=True, result_metadata={"score": 100})
        env.completion.complete_job(entry, result)

        assert env.resources["gpu"].used == 0.0
        assert env.store.state.successes["t::j1"]["result"]["metadata"]["score"] == 100
        assert env.store.state.successes["t::j1"]["last_success_run_id"] == env.session.run_id
        assert env.store.state.successes["t::j1"]["inputs"] == [str(input_file)]
        assert env.stats["succeeded"] == 1
        assert "t::j1" not in env.store.state.in_flight_uids
        assert not cache_file.exists()
        attempt, = env.backend.load_settled_attempts("t::j1")
        assert attempt["outcome"] == "success"

    def test_cursor_updates_applied(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1")
        entry = env.track_entry(job, **env.start_execution(job))

        env.completion.complete_job(
            entry,
            ExecutionResult(
                success=True,
                result_metadata={},
                cursor_updates={"pos": "42"},
            ),
        )
        assert env.store.state.cursors["pos"] == "42"


class TestCompleteJobFailure:
    """complete_job 失败生命周期。"""

    def test_failure_archives_and_cascades(self, tmp_path, monkeypatch):
        env = CompletionEnv(tmp_path)
        env.store.state.replace_queue([
            Job("child", "c1", depends_on=["t::j1"]).to_dict(),
        ])
        partial = tmp_path / "partial.txt"
        partial.write_text("partial", encoding="utf-8")
        job = Job(
            "t",
            "j1",
            files={
                "partial": FileSpec(
                    partial,
                    role="output",
                    delete_on_failure=True,
                ),
            },
        )
        identity = env.start_execution(job)
        entry = env.track_entry(job, **identity)
        env.store.authorizer = lambda *_args: True

        env.completion.complete_job(
            entry, ExecutionResult(success=False, message="boom", result_metadata={"error_message": "业务错误"})
        )

        assert env.store.state.failed["t::j1"]["error_message"] == "boom"
        assert env.store.state.failed["child::c1"]["error_message"] == "Dependency failed or has no record"
        assert env.stats["failed"] == 1
        assert env.stats["cascade_failed"] == 1
        assert not partial.exists()
        attempt, = env.backend.load_settled_attempts("t::j1")
        assert attempt["outcome"] == "failure"


class TestRetryBudget:
    """显式重试的预算判定与编号推进。"""

    def test_business_retry_retains_settled_number_until_dispatch(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1", max_retries_per_run=2)
        identity = env.start_execution(job)
        entry = env.track_entry(job, **identity)

        result = ExecutionResult(
            retry_requested=True, message="connection blip"
        )
        env.completion.complete_job(entry, result)

        assert result.will_retry is True
        assert env.stats["retried"] == 1
        assert env.store.state.queue[0]["attempt_no"] == 1
        attempt, = env.backend.load_settled_attempts("t::j1")
        assert attempt["outcome"] == "retry"
        assert attempt["result"]["message"] == "connection blip"
        assert entry.job.attempt_no == attempt["attempt_no"] == 1
        assert env.session.get_retry_count(job.uid) == 1
        assert env.session.is_retry_candidate(job.uid) is True

    def test_retry_job_snapshot_preserves_custom_fields(self, tmp_path):
        """重试 = 原作业原样重入队：顶层自定义字段与已注入资源随重试往返保留。"""
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1", resources={"__workers__": 1.0},
                  timeout_seconds=23, payload={"timeout_seconds": "业务时限"})
        entry = env.track_entry(job)
        job_snapshot = dict(entry.job_snapshot)
        job_snapshot["custom_note"] = "keep-me"

        env.completion.apply_result(
            "t::j1", job, job_snapshot,
            ExecutionResult(retry_requested=True, message="x"), **_identity(entry),
        )
        assert env.store.state.queue[0]["custom_note"] == "keep-me"
        assert env.store.state.queue[0]["resources"] == {"__workers__": 1.0}
        restored = Job.from_dict(env.store.state.queue[0])
        assert restored.timeout_seconds == 23
        assert restored.payload == {"timeout_seconds": "业务时限"}

    def test_budget_exhaustion_archives(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1", max_retries_per_run=1)
        first = env.track_entry(job)
        env.completion.complete_job(first, ExecutionResult(
            retry_requested=True, message="first failure",
        ))
        job = Job.from_dict(env.store.state.pop_job(0))
        identity = env.start_execution(job)
        entry = env.track_entry(job, **identity)

        result = ExecutionResult(retry_requested=True, message="second")
        env.completion.complete_job(entry, result)

        assert result.will_retry is False
        assert "t::j1" in env.store.state.failed
        assert env.store.state.failed["t::j1"]["error_message"] == 'Retry budget exhausted'
        assert env.stats["failed"] == 1
        attempts = env.backend.load_settled_attempts("t::j1")
        assert [(a["attempt_no"], a["outcome"], a["result"]["message"]) for a in attempts] == [
            (1, "retry", "first failure"), (2, "failure", "second"),
        ]
        assert env.store.state.failed["t::j1"]["result"]["message"] == "second"
        assert env.session.get_retry_count(job.uid) == 1
        assert env.session.is_retry_candidate(job.uid) is False

    def test_explicit_zero_budget_retry_advances_attempt_no(self, tmp_path):
        """显式零预算重试不受预算限制，但编号仍推进。"""
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1", max_retries_per_run=0)
        for _ in range(4):
            previous = env.track_entry(job)
            env.completion.complete_job(previous, ExecutionResult(
                retry_requested=True, consume_budget=False,
            ))
            env.store.state.pop_job(0)
        identity = env.start_execution(job)
        entry = env.track_entry(job, **identity)

        result = ExecutionResult(
            retry_requested=True,
            message="suppressed failure",
            consume_budget=False,
        )
        env.completion.complete_job(entry, result)

        assert result.will_retry is True
        assert "t::j1" not in env.store.state.failed
        assert env.store.state.queue[0]["attempt_no"] == 5
        assert env.stats["retried"] == 5
        assert env.session.get_retry_count(job.uid) == 0
        assert env.session.is_retry_candidate(job.uid) is True


class TestSpawnedJobs:
    """动态子任务去重与坏子作业拒绝。"""

    def test_spawn_dedup_queue_hit_blocks_rerun(self, tmp_path):
        """已入队的相同有效配置只保留一份，即使触发器允许重跑。"""
        env = CompletionEnv(tmp_path)
        env.register_task("child")
        env.store.state.register_in_flight("child::x")
        env.store.apply_success("child::x", result={"message": None, "exception": None, "metadata": {}, "annotations": {}}, **env.start_execution(Job("child", "x")))
        existing = env.store.normalize_spawned_job(Job("child", "x", trigger="always"))
        env.store.state.spawn_jobs([existing])

        parent = Job("parent", "p1")
        entry = env.track_entry(parent, **env.start_execution(parent))
        env.completion.apply_result(
            "parent::p1", parent, entry.job_snapshot,
            ExecutionResult(
                success=True,
                spawned_jobs=[Job("child", "x", trigger="always")],
            ),
            **_identity(entry),
            entry=entry,
        )
        assert "parent::p1" in env.store.state.successes
        x_rows = [jd for jd in env.store.state.queue if jd["job_id"] == "x"]
        assert len(x_rows) == 1, "spawn 必须被 queue 命中拦截"

    def test_spawn_wall_hit_every_run_admitted(self, tmp_path):
        env = CompletionEnv(tmp_path)
        env.store.state.register_in_flight("child::y")
        env.register_task("child")
        env.store.apply_success("child::y", result={"message": None, "exception": None, "metadata": {}, "annotations": {}}, **env.start_execution(Job("child", "y")))
        # successes 命中 + every_run：spawn 放行（唯一终态历史行被替换语义）

        parent = Job("parent", "p1")
        entry = env.track_entry(parent, **env.start_execution(parent))
        env.completion.apply_result(
            "parent::p1", parent, entry.job_snapshot,
            ExecutionResult(
                success=True,
                spawned_jobs=[Job("child", "y", trigger="always",
                              timeout_seconds=29, payload={"timeout_seconds": "业务时限"})],
            ),
            **_identity(entry),
        )
        assert "parent::p1" in env.store.state.successes
        assert "child::y" in env.store.state.queue_uids
        restored = Job.from_dict(env.store.state.queue[0])
        assert restored.timeout_seconds == 29
        assert restored.payload == {"timeout_seconds": "业务时限"}

    def test_spawn_dedup_within_batch(self, tmp_path):
        env = CompletionEnv(tmp_path)
        parent = Job("parent", "p1")
        env.register_task("child")
        entry = env.track_entry(parent, **env.start_execution(parent))
        env.completion.apply_result(
            "parent::p1", parent, entry.job_snapshot,
            ExecutionResult(
                success=True,
                spawned_jobs=[Job("child", "a"), Job("child", "a")],
            ),
            **_identity(entry),
        )
        rows = [jd for jd in env.store.state.queue if jd["job_id"] == "a"]
        assert len(rows) == 1

    def test_empty_dispatch_snapshot_does_not_fall_back_to_live_state(self, tmp_path):
        env = CompletionEnv(tmp_path)
        env.register_task("child")
        env.store.state.spawn_jobs([
            env.store.normalize_and_validate_job(Job("known", "live")),
        ])
        parent = Job("parent", "p1")
        entry = env.track_entry(parent, **env.start_execution(parent))
        assert entry.known_job_uids == frozenset()

        env.completion.apply_result(
            parent.uid,
            parent,
            entry.job_snapshot,
            ExecutionResult(
                success=True,
                spawned_jobs=[
                    Job("child", "future", depends_on=["known::live"]),
                ],
            ),
            **_identity(entry),
            entry=entry,
        )

        assert parent.uid in env.store.state.failed
        assert "Invalid spawned job" in env.store.state.failed[parent.uid]["error_message"]
        assert "child::future" not in env.store.state.queue_uids

    @pytest.mark.parametrize("bad_payload", [
        {1: "integer", "1": "string"},
        {"items": [(1, 2)]},
        {"bad": object()},
    ])
    def test_invalid_json_spawned_job_rejects_parent(self, tmp_path, bad_payload):
        """非法子作业导致父作业失败，整个派生集合与游标不发布。"""
        env = CompletionEnv(tmp_path)
        env.register_task("child")

        child = Job("child", "bad")
        child.payload = bad_payload
        parent = Job("parent", "p1")
        entry = env.track_entry(parent, **env.start_execution(parent))
        env.completion.apply_result(
            "parent::p1", parent, entry.job_snapshot,
            ExecutionResult(
                success=True,
                result_metadata={"ok": 1},
                spawned_jobs=[Job("child", "valid"), child],
                cursor_updates={"offset": "999"},
            ),
            **_identity(entry),
        )
        assert "parent::p1" not in env.store.state.successes
        assert "parent::p1" in env.store.state.failed
        assert "Invalid spawned job" in env.store.state.failed["parent::p1"]["error_message"]
        assert "child::bad" not in env.store.state.failed
        assert env.backend.load_settled_attempts("child::bad") == []
        assert env.backend.load_cursors() == {}
        assert env.store.state.queue == []


class TestSuspensionApplication:
    """资源释放契约。"""

    def test_resource_released_before_commit(self, tmp_path):
        cap = CapacityResource("slots", max_capacity=1.0)
        env = CompletionEnv(tmp_path, extra_resources={"slots": cap})
        cap.acquire(1.0)
        assert cap.used == 1.0

        job = Job("t", "j1", resources={"slots": 1.0})
        entry = env.track_entry(job, **env.start_execution(job), acquired=[("slots", 1.0)])

        env.completion.complete_job(
            entry,
            ExecutionResult(success=True, result_metadata={}),
        )
        assert cap.used == 0.0, "任务完成前必须释放资源"


class TestIdentityVacuity:
    """apply_result 的身份真空断言。"""

    def test_expect_in_flight_assertion(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1")
        with pytest.raises(AssertionError, match="identity vacuity"):
            env.completion.apply_result(
                "t::j1", job, job.to_dict(),
                ExecutionResult(success=True),
                expect_in_flight=True,
            )

    def test_pseudo_entry_skips_assertion(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1")
        aid = env.start_execution(job)
        env.completion.apply_result(
            "t::j1", job, job.to_dict(),
            ExecutionResult(success=True),
            expect_in_flight=False,
            **aid,
        )
        assert "t::j1" in env.store.state.successes


class TestSettleReaped:
    """收割批量结算。"""

    def test_settle_reaped_mixed_outcomes(self, tmp_path):
        env = CompletionEnv(tmp_path)
        j1 = Job("t", "j1")
        e1 = env.track_entry(j1, **env.start_execution(j1))
        e1.handle = env.make_handle("t::j1")
        j2 = Job("t", "j2")
        e2 = env.track_entry(j2, **env.start_execution(j2))
        e2.handle = env.make_handle("t::j2")

        count = env.completion.settle_reaped([
            (e1.handle, ExecutionResult(success=True, result_metadata={})),
            (e2.handle, ExecutionResult(success=False, result_metadata={"error_message": "x"})),
        ])
        assert count == 2
        assert "t::j1" in env.store.state.successes
        assert "t::j2" in env.store.state.failed
        assert len(env.in_flight) == 0
        assert env.store.state.in_flight_uids == frozenset()
        assert env.backend.load_settled_attempts("t::j1")[-1]["outcome"] == "success"

    def test_settle_reaped_skips_unknown_handle(self, tmp_path):
        env = CompletionEnv(tmp_path)
        handle = env.make_handle("t::ghost")
        assert env.completion.settle_reaped(
            [(handle, ExecutionResult(success=True))]
        ) == 0


class TestSettleAborted:
    """中止分类结算。"""

    def test_abort_releases_entries_without_committing_results(self, tmp_path):
        env = CompletionEnv(tmp_path)
        j1, j2 = Job("t", "j1"), Job("t", "j2")
        e1, e2 = env.track_entry(j1), env.track_entry(j2)
        env.completion.settle_aborted(
            cancelled_entries=[e1],
            done_entries=[(e2, ExecutionResult(success=True))],
        )
        assert not env.store.state.queue
        assert not env.store.state.successes
        assert not env.store.state.failed
        assert not env.in_flight
        assert not env.store.state.in_flight_uids
        assert env.resources["__workers__"].used == 0

    def test_release_failure_still_unregisters_all_entries(self, tmp_path, monkeypatch):
        env = CompletionEnv(tmp_path)
        first = env.track_entry(Job("t", "first"))
        second = env.track_entry(Job("t", "second"))
        fault = OSError("resource release unavailable")

        def fail_release(*args):
            raise fault

        monkeypatch.setattr(first, "release_resources", fail_release)
        with pytest.raises(OSError) as caught:
            env.completion.settle_aborted([first], [(second, ExecutionResult(success=True))])
        assert caught.value is fault
        assert not env.in_flight
        assert not env.store.state.in_flight_uids
        assert not env.store.state.successes
        assert not second.acquired


@pytest.mark.parametrize("success", [True, False])
@pytest.mark.parametrize("directory", [True, False])
def test_completion_publishes_only_successful_framework_artifacts(tmp_path, success, directory):
    env = CompletionEnv(tmp_path)
    output = tmp_path / "report"
    if directory:
        output.mkdir()
        (output / "part").write_text("report")
    else:
        output.write_text("report")
    job = Job(
        "t",
        "named",
        files={"report": FileSpec(output, role="output")},
    )
    entry = env.track_entry(job, **env.start_execution(job))
    result = ExecutionResult(
        success=success, result_metadata={"artifacts": {"forged": {"path": "/fake"}}},
    )
    env.completion.complete_job(entry, result)
    if success:
        expected = {"report": {"path": str(output)}}
        assert env.store.state.successes[job.uid]["artifacts"] == expected
        assert env.backend.load_successes()[job.uid]["artifacts"] == expected
    else:
        assert job.uid not in env.backend.load_successes()
    assert output.exists()


def test_symlink_output_settles_failure_without_false_success(tmp_path):
    env = CompletionEnv(tmp_path)
    target = tmp_path / "real.txt"
    target.write_text("ok")
    link = tmp_path / "sym.txt"
    link.symlink_to(target)
    job = Job(
        "t",
        "invalid_output",
        files={"primary": FileSpec(link, role="output")},
    )
    entry = env.track_entry(job, **env.start_execution(job))
    env.completion.complete_job(entry, ExecutionResult(success=True))
    assert env.backend.load_successes() == {}
    assert job.uid in env.backend.load_failed()
    assert "symlink" in env.backend.load_failed()[job.uid]["error_message"]
    assert env.backend.load_settled_attempts(job.uid)[0]["outcome"] == "failure"
    assert env.stats["succeeded"] == 0


def test_successful_directory_cleanup_precedes_success_transaction(tmp_path, monkeypatch):
    env = CompletionEnv(tmp_path)
    temporary = tmp_path / "temporary"
    temporary.mkdir()
    (temporary / "part").write_text("content")
    job = Job(
        "t",
        "temporary_directory",
        files={
            "temporary": FileSpec(
                temporary,
                role="output",
                delete_on_success=True,
            ),
        },
    )
    entry = env.track_entry(job, **env.start_execution(job))
    env.store.authorizer = lambda *_args: True
    commit = env.backend.commit_job_settlement

    def commit_after_cleanup(*args, **kwargs):
        assert not temporary.exists()
        return commit(*args, **kwargs)

    monkeypatch.setattr(env.backend, "commit_job_settlement", commit_after_cleanup)
    env.completion.complete_job(entry, ExecutionResult(success=True))
    assert job.uid in env.backend.load_successes()
    assert env.backend.load_successes()[job.uid]["artifacts"] == {
        "temporary": {"path": str(temporary)},
    }
    assert not temporary.exists()


@pytest.mark.parametrize("success", [True, False])
def test_cleanup_failure_settles_attempt_failure(tmp_path, monkeypatch, success):
    from tasklite.utils.artifact import ArtifactPathPolicy

    env = CompletionEnv(tmp_path)
    job = Job("t", "cleanup_failure", max_retries_per_run=0)
    output = tmp_path / "output"
    output.write_text("content")
    cleanup_flag = (
        {"delete_on_success": True}
        if success
        else {"delete_on_failure": True}
    )
    job.files["output"] = FileSpec(
        output,
        role="output",
        **cleanup_flag,
    )
    entry = env.track_entry(job, **env.start_execution(job))
    env.store.authorizer = lambda *_args: True

    def fail_cleanup(self, declaration, *, success):
        raise PermissionError("cleanup unavailable")

    monkeypatch.setattr(ArtifactPathPolicy, "cleanup", fail_cleanup)
    env.completion.complete_job(entry, ExecutionResult(success=success))
    assert env.backend.load_successes() == {}
    assert "cleanup unavailable" in env.backend.load_failed()[job.uid]["error_message"]
    assert env.backend.load_settled_attempts(job.uid)[0]["outcome"] == "failure"
    assert output.exists()


def test_abort_does_not_request_or_execute_cleanup(tmp_path, monkeypatch):
    from tasklite.utils.artifact import ArtifactPathPolicy

    env = CompletionEnv(tmp_path)
    env.store.authorizer = lambda *_args: pytest.fail("异常退出不能发起授权")
    entries = [
        env.track_entry(
            Job("t", name, max_retries_per_run=3),
            files={
                "temporary": FileSpec(
                    tmp_path / name,
                    role="output",
                    delete_on_failure=True,
                ),
            },
        )
        for name in ("one", "two")
    ]

    def denied(self, declaration, *, success):
        pytest.fail("异常退出不能补执行文件清理")

    monkeypatch.setattr(ArtifactPathPolicy, "cleanup", denied)
    env.completion.settle_aborted(entries, [])
    assert not env.in_flight
    assert not env.store.state.in_flight_uids
    assert not env.store.state.queue
    assert env.resources["__workers__"].used == 0
    assert not env.backend.load_successes()
    assert not env.store.state.failed


def test_completion_rejects_missing_output(tmp_path, monkeypatch):
    from tasklite.utils.artifact import ArtifactPathPolicy

    env = CompletionEnv(tmp_path)
    missing = tmp_path / "missing"
    job = Job(
        "t",
        "missing",
        files={"data": FileSpec(missing, role="output")},
    )
    entry = env.track_entry(job)
    checked: list[str] = []
    original = ArtifactPathPolicy.entry_exists

    def observe(self, declaration):
        checked.append(declaration.path)
        return original(self, declaration)

    monkeypatch.setattr(ArtifactPathPolicy, "entry_exists", observe)
    env.completion.complete_job(entry, ExecutionResult(success=True))
    assert checked == [str(missing)]
    assert env.backend.load_successes() == {}
    assert "Missing output" in env.backend.load_failed()[job.uid]["error_message"]


def test_declaration_environment_fault_preserves_running_state(tmp_path):
    from tasklite.exceptions import _ArtifactIOError

    env = CompletionEnv(
        tmp_path,
        extra_resources={"api": IntervalResource("api", interval_seconds=0.01)},
    )
    entry = env.track_entry(Job("t", "limited"))

    with pytest.raises(_ArtifactIOError, match="write denied"):
        env.completion.complete_job(
            entry,
            ExecutionResult(
                artifact_error="write denied",
            ),
        )
    assert entry.job.attempt_no == 1
    assert not env.store.state.failed


def test_retry_cleanup_failure_settles_failure_without_retry(tmp_path, monkeypatch):
    from tasklite.utils.artifact import ArtifactPathPolicy

    env = CompletionEnv(tmp_path)
    target = tmp_path / "temporary"
    target.write_text("partial", encoding="utf-8")
    job = Job(
        "t",
        "retry_cleanup",
        max_retries_per_run=3,
        files={
            "temporary": FileSpec(
                target,
                role="cache",
                delete_on_failure=True,
            ),
        },
    )
    entry = env.track_entry(job, **env.start_execution(job))
    env.store.authorizer = lambda *_args: True

    def denied(self, declaration, *, success):
        raise PermissionError("cleanup denied")

    monkeypatch.setattr(ArtifactPathPolicy, "cleanup", denied)
    result = ExecutionResult(
        retry_requested=True,
        message="business failure",
        consume_budget=True,
    )
    env.completion.complete_job(entry, result)
    assert result.will_retry is False
    assert env.store.state.queue == []
    assert job.uid in env.backend.load_failed()
    assert "Retry cleanup failed" in env.backend.load_failed()[job.uid]["error_message"]
    assert env.backend.load_settled_attempts(job.uid)[0]["outcome"] == "failure"


def test_reaped_batch_finishes_other_result_after_cleanup_error(tmp_path, monkeypatch):
    from tasklite.utils.artifact import ArtifactPathPolicy

    env = CompletionEnv(
        tmp_path,
        extra_resources={"api": IntervalResource("api", interval_seconds=0.01)},
    )
    broken = tmp_path / "broken"
    broken.write_text("partial", encoding="utf-8")
    first = Job(
        "t",
        "first",
        files={
            "broken": FileSpec(
                broken,
                role="output",
                delete_on_failure=True,
            ),
        },
    )
    second = Job("t", "second")
    for job in (first, second):
        env.track_entry(job, **env.start_execution(job))
    env.store.authorizer = lambda *_args: True

    def denied(self, declaration, *, success):
        raise PermissionError("cleanup denied")

    monkeypatch.setattr(ArtifactPathPolicy, "cleanup", denied)
    settled = env.completion.settle_reaped([
            (env.make_handle(first.uid), ExecutionResult(success=False)),
            (env.make_handle(second.uid), ExecutionResult(success=True)),
        ])
    assert settled == 2
    assert second.uid in env.backend.load_successes()
    assert first.uid in env.backend.load_failed()
    assert "cleanup denied" in env.backend.load_failed()[first.uid]["error_message"]
    assert not env.store.state.queue
    assert env.backend.load_settled_attempts(first.uid)[0]["outcome"] == "failure"
    assert env.resources["__workers__"].used == 0
    assert not env.in_flight


def test_inflight_duplicate_spawn_compares_effective_configuration(tmp_path):
    """在途同 UID 配置冲突拒绝父成功，不影响既有在途执行。"""
    env = CompletionEnv(tmp_path)
    env.register_task("child")
    child = Job("child", "same", payload={"version": 1})
    env.track_entry(child, **env.start_execution(child))
    parent = Job("parent", "one")
    entry = env.track_entry(parent, **env.start_execution(parent))
    env.completion.apply_result(
        parent.uid, parent, entry.job_snapshot,
        ExecutionResult(success=True, spawned_jobs=[Job("child", "same", payload={"version": 2})], cursor_updates={"offset": "bad"}),
        **_identity(entry),
    )
    assert parent.uid in env.store.state.failed
    assert "configuration" in env.store.state.failed[parent.uid]["error_message"]
    assert child.uid in env.store.state.in_flight_uids
    assert env.backend.load_cursors() == {}
