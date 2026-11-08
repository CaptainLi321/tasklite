"""v2 CompletionMachine 完成机器测试：唯一收尾出口、三态分支与批量结算。

覆盖：complete_job 成功/失败/重试生命周期（资源释放、清理模式、收尾
钩子恰好一次）、重试预算判定（attempt_no 对 max_retries+1）与瞬态信号
零预算、动态子任务去重与坏子作业拒绝、3-strike 终结的防双触发、
settle_reaped / settle_aborted 批量结算。
"""

from __future__ import annotations

from tasklite.triggers import BUILTIN_TRIGGER_FUNCTIONS

from pathlib import Path
from typing import Any

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import TriggerRegistry
from tasklite.engine.channel import (
    ArtifactCleanupMode,
    ExecutionChannel,
    ExecutionResult,
)
from tasklite.engine.completion import CompletionMachine
from tasklite.engine.errorclass import ERR_MAX_RETRIES, ErrorClassifier
from tasklite.engine.in_flight import InFlightJob, InFlightTracker
from tasklite.engine.resource import (
    CapacityResource,
    RateLimitResource,
    ResourceManager,
)
from tasklite.engine.store import StateStore
from tasklite.engine.types import AttemptOutcome, JobHandle, RunStats
from tasklite.models.job import Job, JobRuntimeState
from tasklite.models.state import PipelineState


class StubSession:
    """运行会话结构契约桩（run_id）。"""

    def __init__(self, run_id: str = "b" * 32) -> None:
        self.run_id = run_id

    def next_dispatch_no(self) -> int:
        raise AssertionError("completion 不消费 dispatch_no")


class FakeProcess:
    def __init__(self, pid: int = 12345):
        self.pid = pid


class CompletionEnv:
    """完成机器窄依赖装配（字段即依赖清单）。"""

    def __init__(self, tmp_path, *, extra_resources=None):
        self.ipc_dir = str(tmp_path / "ipc")
        Path(self.ipc_dir).mkdir(parents=True, exist_ok=True)

        resources_map = {
            "__workers__": CapacityResource("__workers__", 2.0),
        }
        if extra_resources:
            resources_map.update(extra_resources)
        self.resources = ResourceManager(resources_map)
        self.backend = InMemoryStateBackend()
        self.stats = RunStats()
        self.hooks: list[tuple[str, AttemptOutcome]] = []
        self.store = StateStore(
            self.backend,
            PipelineState({}, {}, {}, []),
            stats=self.stats,
            on_attempt_finished=lambda uid, *, outcome: self.hooks.append((uid, outcome)),
        )
        self.session = StubSession()
        self.in_flight = InFlightTracker()
        self.channel = ExecutionChannel(self.ipc_dir)
        self.completion = CompletionMachine(
            store=self.store,
            trigger_registry=TriggerRegistry(functions=BUILTIN_TRIGGER_FUNCTIONS),
            channel=self.channel,
            resources=self.resources,
            in_flight=self.in_flight,
            session=self.session,
        )

    def track_entry(self, job: Job, *, lease=None, attempt_id=None) -> InFlightJob:
        snapshot = job.to_dict()
        self.store.finalize_intake_snapshot(snapshot, job.task_type)
        job.__dict__.update(Job.from_dict(snapshot).__dict__)
        if lease is None:
            lease = self.resources.reserve(job.task_type, job.resources, job_uid=job.uid)
        entry = InFlightJob(
            job_uid=job.uid,
            job_snapshot=job.to_dict(),
            job=job,
            lease=lease,
            attempt_id=attempt_id,
        )
        self.in_flight.track(entry, state=self.store.state)
        return entry

    def open_attempt(self, job: Job, *, attempt_no=1, activation_no=1) -> int:
        from tasklite.models.attempt import AttemptRecord

        record = AttemptRecord(
            job_uid=job.uid,
            activation_no=activation_no,
            attempt_no=attempt_no,
            execution_id=f"{self.session.run_id}.1",
            run_id=self.session.run_id,
            started_at="2026-01-01T00:00:00+00:00",
            outcome="running",
        )
        return self.backend.append_attempt(record)

    def make_handle(self, uid: str) -> JobHandle:
        return JobHandle(
            job_uid=uid,
            process=FakeProcess(),
            timeout_at_monotonic=1e18,
            timeout_seconds=60.0,
            job=Job(*uid.split("::", 1)),
            ipc_dir=self.ipc_dir,
            execution_id=f"{self.session.run_id}.1",
        )


class TestCompleteJobSuccess:
    """complete_job 成功生命周期。"""

    def test_success_lifecycle(self, tmp_path, monkeypatch):
        env = CompletionEnv(tmp_path, extra_resources={"gpu": CapacityResource("gpu", 10.0)})
        job = Job("t", "j1", resources={"gpu": 2.0})
        attempt_id = env.open_attempt(job)
        entry = env.track_entry(job, attempt_id=attempt_id)

        assert env.resources["gpu"].used == 2.0
        cleaned: list[tuple[str, ArtifactCleanupMode]] = []
        monkeypatch.setattr(
            env.channel.journal, "cleanup_declared_artifacts",
            lambda declarations, *, success: cleaned.append(success),
        )
        monkeypatch.setattr(
            env.channel, "read_declared_inputs",
            lambda uid: ["/data/in.csv"],
        )

        result = ExecutionResult(success=True, result_metadata={"score": 100})
        env.completion.complete_job(entry, result)

        assert env.resources["gpu"].used == 0.0
        assert env.store.state.successes["t::j1"]["score"] == 100
        assert env.store.state.successes["t::j1"]["last_success_run_id"] == env.session.run_id
        assert env.store.state.successes["t::j1"]["inputs"] == ["/data/in.csv"]
        assert env.stats["succeeded"] == 1
        assert "t::j1" not in env.store.state.in_flight_uids
        assert cleaned == [True]
        # 收尾钩子恰好一次（成功语义）
        assert env.hooks == [
            ("t::j1", AttemptOutcome(success=True, will_retry=False,
                                    metadata={"score": 100}))
        ]
        assert env.backend.load_attempts("t::j1")[-1].outcome == "succeeded"

    def test_cursor_updates_applied(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1")
        entry = env.track_entry(job, attempt_id=env.open_attempt(job))

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
        job = Job("t", "j1")
        attempt_id = env.open_attempt(job)
        entry = env.track_entry(job, attempt_id=attempt_id)
        cleaned: list[tuple[str, ArtifactCleanupMode]] = []
        monkeypatch.setattr(
            env.channel.journal, "cleanup_declared_artifacts",
            lambda declarations, *, success: cleaned.append(success),
        )

        env.completion.complete_job(
            entry, ExecutionResult(success=False, result_metadata={"error_message": "boom"})
        )

        assert env.store.state.failed["t::j1"]["error_message"] == "boom"
        assert env.store.state.failed["child::c1"]["error_message"] == "JOB_DEPENDENCY"
        assert env.stats["failed"] == 1
        assert env.stats["cascade_failed"] == 1
        assert cleaned[-1] is False
        assert env.backend.load_attempts("t::j1")[-1].outcome == "failed"
        finish_by_uid = {uid: fin for uid, fin in env.hooks}
        assert finish_by_uid["t::j1"].success is False
        assert finish_by_uid["t::j1"].will_retry is False


class TestRetryBudget:
    """重试预算判定与瞬态零预算。"""

    def test_business_retry_advances_attempt_no(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1", max_retries_per_run=2)
        attempt_id = env.open_attempt(job)
        entry = env.track_entry(job, attempt_id=attempt_id)

        result = ExecutionResult(
            retry_requested=True, retry_error="connection blip"
        )
        env.completion.complete_job(entry, result)

        assert result.will_retry is True
        assert env.stats["retried"] == 1
        assert env.store.state.queue[0]["attempt_no"] == 2
        assert env.store.state.queue[0]["first_enqueued_at"] == job.first_enqueued_at
        rt = JobRuntimeState.from_dict(env.store.state.queue[0].get("runtime"))
        assert rt.last_retry_error == "connection blip"
        assert env.backend.load_attempts("t::j1")[-1].outcome == "requeued"
        assert env.hooks[-1] == (
            "t::j1",
            AttemptOutcome(success=False, will_retry=True, metadata={}),
        )

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
            ExecutionResult(retry_requested=True, retry_error="x"),
        )
        assert env.store.state.queue[0]["custom_note"] == "keep-me"
        assert env.store.state.queue[0]["resources"] == {"__workers__": 1.0}
        restored = Job.from_dict(env.store.state.queue[0])
        assert restored.timeout_seconds == 23
        assert restored.payload == {"timeout_seconds": "业务时限"}

    def test_budget_exhaustion_archives(self, tmp_path):
        env = CompletionEnv(tmp_path)
        # attempt_no=2 且已消耗 1 次重试：max_retries_per_run=1 预算已耗尽
        job = Job._restore("t", "j1", max_retries_per_run=1, attempt_no=2)
        job.runtime.record_retry_error("first failure")
        env.completion._record_retry(job.uid)
        attempt_id = env.open_attempt(job, attempt_no=2)
        entry = env.track_entry(job, attempt_id=attempt_id)
        entry.job_snapshot["runtime"] = job.runtime.to_dict()

        result = ExecutionResult(retry_requested=True, retry_error="second")
        env.completion.complete_job(entry, result)

        assert result.will_retry is False
        assert "t::j1" in env.store.state.failed
        assert env.store.state.failed["t::j1"]["error_message"] == ERR_MAX_RETRIES
        assert env.store.state.failed["t::j1"]["last_retry_error"] == "first failure"
        assert env.store.state.failed["t::j1"]["retry_error"] == "second"
        assert env.stats["failed"] == 1
        assert env.backend.load_attempts("t::j1")[-1].outcome == "failed"

    def test_transient_zero_budget_requeues_beyond_budget(self, tmp_path):
        """瞬态信号：attempt_no 超预算仍重入队，attempt_no 不推进。"""
        env = CompletionEnv(tmp_path)
        job = Job._restore("t", "j1", max_retries_per_run=0, attempt_no=5)
        attempt_id = env.open_attempt(job, attempt_no=5)
        entry = env.track_entry(job, attempt_id=attempt_id)

        result = ExecutionResult(
            retry_requested=True,
            retry_error="interrupted",
            transient_kind="interrupted",
        )
        env.completion.complete_job(entry, result)

        assert result.will_retry is True
        assert "t::j1" not in env.store.state.failed
        assert env.store.state.queue[0]["attempt_no"] == 5
        rt = JobRuntimeState.from_dict(env.store.state.queue[0].get("runtime"))
        assert rt.last_retry_error == ""
        assert env.stats["interrupted_reruns"] == 1

    def test_rate_limited_transient_kind_counted(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1")
        entry = env.track_entry(job)

        env.completion.complete_job(
            entry,
            ExecutionResult(
                retry_requested=True,
                retry_error="HTTP 429",
                transient_kind="rate_limited",
            ),
        )
        assert env.stats["rate_limited_reruns"] == 1
        assert env.stats["retried"] == 1


class TestThreeStrikeTermination:
    """3-strike 终结路径的防双触发。"""

    def test_strike_termination_hook_exactly_once(self, tmp_path):
        class StrikeBackend:
            """commit_job_success 恒失败、失败档案写入放行的注入桩。"""

            def __init__(self, real):
                self._real = real

            def commit_job_success(self, *a, **k):
                return False

            def __getattr__(self, name):
                return getattr(self._real, name)

        env = CompletionEnv(tmp_path)
        strike = StrikeBackend(env.backend)
        env.store.set_backend(strike)

        job = Job("t", "j1")
        entry = env.track_entry(job)
        # 预埋 2 次 commit 失败：本次 commit_job_success 失败即达 3-strike
        job.runtime.commit_failures = 2
        entry.job_snapshot["runtime"] = job.runtime.to_dict()

        result = ExecutionResult(success=True, result_metadata={"score": 1})
        env.completion.complete_job(entry, result)  # 不上抛 _JobTerminated

        assert result.will_retry is False
        assert "t::j1" in env.store.state.failed
        assert env.store.state.failed["t::j1"]["error_message"] == "COMMIT_FAILURE"
        # 钩子恰好一次：3-strike 分支已触发，尾部跳过
        assert len(env.hooks) == 1
        assert env.hooks[0][1].success is False
        assert env.hooks[0][1].metadata["error_message"] == "COMMIT_FAILURE"


class TestSpawnedJobs:
    """动态子任务去重与坏子作业拒绝。"""

    def test_spawn_dedup_queue_hit_blocks_rerun(self, tmp_path):
        """queue/in-flight 命中无条件拦截，优先于 every_run 豁免。"""
        env = CompletionEnv(tmp_path)
        env.store.state.register_in_flight("child::x")
        env.store.apply_success("child::x", {}, attempt_id=env.open_attempt(Job("child", "x")))
        # X 重跑中（queue，every_run 放行重跑）
        env.store.state.spawn_jobs([Job("child", "x", trigger="always").to_dict()])

        parent = Job("parent", "p1")
        entry = env.track_entry(parent, attempt_id=env.open_attempt(parent))
        env.completion.apply_result(
            "parent::p1", parent, entry.job_snapshot,
            ExecutionResult(
                success=True,
                spawned_jobs=[Job("child", "x", trigger="always")],
            ),
            attempt_id=entry.attempt_id,
        )
        assert "parent::p1" in env.store.state.successes
        x_rows = [jd for jd in env.store.state.queue if jd["job_id"] == "x"]
        assert len(x_rows) == 1, "spawn 必须被 queue 命中拦截"

    def test_spawn_wall_hit_every_run_admitted(self, tmp_path):
        env = CompletionEnv(tmp_path)
        env.store.state.register_in_flight("child::y")
        env.store.apply_success("child::y", {}, attempt_id=env.open_attempt(Job("child", "y")))
        # successes 命中 + every_run：spawn 放行（唯一终态历史行被替换语义）

        parent = Job("parent", "p1")
        entry = env.track_entry(parent, attempt_id=env.open_attempt(parent))
        env.completion.apply_result(
            "parent::p1", parent, entry.job_snapshot,
            ExecutionResult(
                success=True,
                spawned_jobs=[Job("child", "y", trigger="always",
                              timeout_seconds=29, payload={"timeout_seconds": "业务时限"})],
            ),
            attempt_id=entry.attempt_id,
        )
        assert "parent::p1" in env.store.state.successes
        assert "child::y" in env.store.state.queue_uids
        restored = Job.from_dict(env.store.state.queue[0])
        assert restored.timeout_seconds == 29
        assert restored.payload == {"timeout_seconds": "业务时限"}

    def test_spawn_dedup_within_batch(self, tmp_path):
        env = CompletionEnv(tmp_path)
        parent = Job("parent", "p1")
        entry = env.track_entry(parent, attempt_id=env.open_attempt(parent))
        env.completion.apply_result(
            "parent::p1", parent, entry.job_snapshot,
            ExecutionResult(
                success=True,
                spawned_jobs=[Job("child", "a"), Job("child", "a")],
            ),
            attempt_id=entry.attempt_id,
        )
        rows = [jd for jd in env.store.state.queue if jd["job_id"] == "a"]
        assert len(rows) == 1

    def test_unserializable_spawned_job_rejects_parent(self, tmp_path):
        """非法子作业导致父作业失败，整个派生集合与游标不发布。"""
        env = CompletionEnv(tmp_path)

        class Unserializable:
            pass

        parent = Job("parent", "p1")
        entry = env.track_entry(parent, attempt_id=env.open_attempt(parent))
        env.completion.apply_result(
            "parent::p1", parent, entry.job_snapshot,
            ExecutionResult(
                success=True,
                result_metadata={"ok": 1},
                spawned_jobs=[Job("child", "bad", payload={"x": Unserializable()})],
            ),
            attempt_id=entry.attempt_id,
        )
        assert "parent::p1" not in env.store.state.successes
        assert "parent::p1" in env.store.state.failed
        assert "Invalid spawned job" in env.store.state.failed["parent::p1"]["error_message"]
        assert "child::bad" not in env.store.state.failed
        assert env.backend.load_attempts("child::bad") == []


class TestSuspensionApplication:
    """结果携带的资源挂起全局应用。"""

    def test_suspensions_applied_and_persisted(self, tmp_path):
        from tasklite.utils.jsonutil import loads

        env = CompletionEnv(
            tmp_path, extra_resources={"api": RateLimitResource("api", 0.01)}
        )
        job = Job("t", "j1")
        entry = env.track_entry(job, attempt_id=env.open_attempt(job))

        env.completion.complete_job(
            entry,
            ExecutionResult(
                success=True,
                result_metadata={},
                resource_suspensions=[("api", 30.0)],
            ),
        )
        assert env.resources["api"].suspended_until() is not None
        raw = env.backend.get_engine_metadata("resource_suspensions")
        assert raw is not None and "api" in loads(raw)

    def test_unknown_resource_suspension_skipped(self, tmp_path, caplog):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1")
        entry = env.track_entry(job, attempt_id=env.open_attempt(job))

        env.completion.complete_job(
            entry,
            ExecutionResult(
                success=True, result_metadata={}, resource_suspensions=[("ghost", 5.0)],
            ),
        )
        assert "t::j1" in env.store.state.successes


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
        aid = env.open_attempt(job)
        env.completion.apply_result(
            "t::j1", job, job.to_dict(),
            ExecutionResult(success=True),
            expect_in_flight=False,
            attempt_id=aid,
        )
        assert "t::j1" in env.store.state.successes


class TestSettleReaped:
    """收割批量结算。"""

    def test_settle_reaped_mixed_outcomes(self, tmp_path):
        env = CompletionEnv(tmp_path)
        j1 = Job("t", "j1")
        e1 = env.track_entry(j1, attempt_id=env.open_attempt(j1))
        e1.handle = env.make_handle("t::j1")
        j2 = Job("t", "j2")
        e2 = env.track_entry(j2, attempt_id=env.open_attempt(j2))
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
        assert env.backend.load_attempts("t::j1")[-1].outcome == "succeeded"

    def test_settle_reaped_skips_unknown_handle(self, tmp_path):
        env = CompletionEnv(tmp_path)
        handle = env.make_handle("t::ghost")
        assert env.completion.settle_reaped(
            [(handle, ExecutionResult(success=True))]
        ) == 0


class TestSettleAborted:
    """中止分类结算。"""

    def test_cancelled_requeued_done_committed(self, tmp_path):
        env = CompletionEnv(tmp_path)
        j1 = Job("t", "j1")
        e1 = env.track_entry(j1, attempt_id=env.open_attempt(j1))
        j2 = Job("t", "j2")
        e2 = env.track_entry(j2, attempt_id=env.open_attempt(j2))

        env.completion.settle_aborted(
            cancelled_entries=[e1],
            done_entries=[(e2, ExecutionResult(success=True, result_metadata={}))],
        )
        assert [jd["job_id"] for jd in env.store.state.queue] == ["j1"]
        assert "t::j2" in env.store.state.successes
        assert len(env.in_flight) == 0
        assert env.store.state.in_flight_uids == frozenset()

    def test_commit_crash_signal_last_one_propagates(self, tmp_path):
        class FailingBackend(InMemoryStateBackend):
            def commit_job_success(self, *a, **k):
                return False

        env = CompletionEnv(tmp_path)
        env.store.set_backend(FailingBackend())
        j1 = Job("t", "j1")
        e1 = env.track_entry(j1)

        from tasklite.exceptions import _CommitCrashSignal

        with pytest.raises(_CommitCrashSignal):
            env.completion.settle_aborted(
                cancelled_entries=[],
                done_entries=[(e1, ExecutionResult(success=True))],
            )
        assert len(env.in_flight) == 0
        assert env.store.state.in_flight_uids == frozenset()


class TestHookFireDiscipline:
    """收尾钩子触发纪律。"""

    def test_retry_hook_carries_going_to_retry(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1", max_retries_per_run=3)
        entry = env.track_entry(job)
        env.completion.complete_job(
            entry, ExecutionResult(retry_requested=True, retry_error="flaky")
        )
        assert env.hooks == [
            ("t::j1", AttemptOutcome(success=False, will_retry=True, metadata={}))
        ]

    def test_hook_error_isolated(self, tmp_path):
        env = CompletionEnv(tmp_path)
        env.store.set_on_attempt_finished(
            lambda uid, *, outcome: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        job = Job("t", "j1")
        entry = env.track_entry(job, attempt_id=env.open_attempt(job))
        env.completion.complete_job(entry, ExecutionResult(success=True))
        assert env.stats["hook_errors"] == 1
        assert "t::j1" in env.store.state.successes


@pytest.mark.parametrize("success", [True, False])
@pytest.mark.parametrize("directory", [True, False])
def test_completion_publishes_only_successful_framework_artifacts(tmp_path, success, directory):
    from tasklite.models.artifact import ArtifactDeclaration

    env = CompletionEnv(tmp_path)
    job = Job("t", "named")
    aid = env.open_attempt(job)
    entry = env.track_entry(job, attempt_id=aid)
    output = tmp_path / "report"
    if directory:
        output.mkdir()
        (output / "part").write_text("report")
    else:
        output.write_text("report")
    env.channel.journal.record_artifact(
        job.uid, ArtifactDeclaration(str(output), name="report"),
    )
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


def test_invalid_artifact_log_propagates_through_completion_without_false_success(tmp_path):
    from tasklite.models.artifact import ArtifactDeclaration

    env = CompletionEnv(tmp_path)
    job = Job("t", "invalid_manifest")
    aid = env.open_attempt(job)
    entry = env.track_entry(job, attempt_id=aid)
    target = tmp_path / "real.txt"
    target.write_text("ok")
    link = tmp_path / "sym.txt"
    link.symlink_to(target)
    env.channel.journal._artifacts[job.uid] = (
        ArtifactDeclaration(str(link), name="primary"),
    )
    with pytest.raises(ValueError, match="symlink"):
        env.completion.complete_job(entry, ExecutionResult(success=True))
    assert env.backend.load_successes() == {}
    assert job.uid in env.store.state.in_flight_uids
    assert env.backend.load_attempts(job.uid)[0].outcome == "running"
    assert env.stats["succeeded"] == 0
    assert env.hooks == []
    assert env.channel.journal._artifacts.get(job.uid) is not None


def test_successful_directory_cleanup_precedes_success_transaction(tmp_path, monkeypatch):
    from tasklite.models.artifact import ArtifactDeclaration

    env = CompletionEnv(tmp_path)
    job = Job("t", "temporary_directory")
    entry = env.track_entry(job, attempt_id=env.open_attempt(job))
    temporary = tmp_path / "temporary"
    temporary.mkdir()
    (temporary / "part").write_text("content")
    env.channel.journal.record_artifact(
        job.uid, ArtifactDeclaration(str(temporary), delete_on_success=True),
    )
    commit = env.backend.commit_job_success

    def commit_after_cleanup(*args, **kwargs):
        assert not temporary.exists()
        return commit(*args, **kwargs)

    monkeypatch.setattr(env.backend, "commit_job_success", commit_after_cleanup)
    env.completion.complete_job(entry, ExecutionResult(success=True))
    assert job.uid in env.backend.load_successes()


@pytest.mark.parametrize("success", [True, False])
def test_cleanup_failure_preserves_original_attempt_and_no_terminal(tmp_path, monkeypatch, success):
    from tasklite.models.artifact import ArtifactDeclaration
    from tasklite.utils.artifact import ArtifactPathPolicy

    env = CompletionEnv(tmp_path)
    job = Job("t", "cleanup_failure", max_retries_per_run=0)
    entry = env.track_entry(job, attempt_id=env.open_attempt(job))
    output = tmp_path / "output"
    output.write_text("content")
    env.channel.journal.record_artifact(
        job.uid, ArtifactDeclaration(str(output), delete_on_failure=True, delete_on_success=True),
    )

    def fail_cleanup(self, declaration, *, success):
        raise PermissionError("cleanup unavailable")

    monkeypatch.setattr(ArtifactPathPolicy, "cleanup", fail_cleanup)
    with pytest.raises(PermissionError, match="cleanup unavailable"):
        env.completion.complete_job(entry, ExecutionResult(success=success))
    assert env.backend.load_successes() == {}
    assert env.backend.load_failed() == {}
    assert env.backend.load_attempts(job.uid)[0].outcome == "running"
    assert entry.job_snapshot["attempt_no"] == 1
    assert env.hooks == []
    assert env.channel.journal.read_artifacts(job.uid) != ()


def test_abort_cleanup_failure_requeues_every_original_snapshot(tmp_path, monkeypatch):
    from tasklite.utils.artifact import ArtifactPathPolicy
    from tasklite.models.artifact import ArtifactDeclaration

    env = CompletionEnv(tmp_path)
    entries = [env.track_entry(Job('t', name, max_retries_per_run=3)) for name in ('one', 'two')]
    for entry in entries:
        env.channel.journal.record_artifact(entry.job_uid, ArtifactDeclaration(
            str(tmp_path / entry.job.job_id), delete_on_failure=True,
        ))

    def denied(self, declaration, *, success):
        raise PermissionError('cleanup denied')

    monkeypatch.setattr(ArtifactPathPolicy, 'cleanup', denied)
    with pytest.raises(PermissionError, match='cleanup denied'):
        env.completion.settle_aborted(entries, [])
    assert not env.in_flight
    assert not env.store.state.in_flight_uids
    assert {job['job_id'] for job in env.store.state.queue} == {'one', 'two'}
    assert all(job['attempt_no'] == 1 for job in env.store.state.queue)
    assert env.resources['__workers__'].used == 0
    assert not env.backend.load_successes()
    assert not env.store.state.failed
    assert not env.hooks


def test_declaration_removal_failure_preserves_committed_success(tmp_path, monkeypatch):
    env = CompletionEnv(tmp_path)
    job = Job('t', 'committed')
    entry = env.track_entry(job, attempt_id=env.open_attempt(job))
    handle = env.make_handle(job.uid)

    def denied(uid):
        raise PermissionError('log unlink denied')

    monkeypatch.setattr(env.channel.journal, 'clear_declarations', denied)
    with pytest.raises(PermissionError, match='log unlink denied'):
        env.completion.settle_reaped([(handle, ExecutionResult(success=True))])
    assert job.uid in env.backend.load_successes()
    assert not env.store.state.queue
    assert not env.store.state.failed
    assert not env.in_flight
    assert len(env.hooks) == 1
    assert env.hooks[0][1].success


def test_completion_reads_declarations_once_and_rejects_missing_output(tmp_path, monkeypatch):
    from tasklite.models.artifact import ArtifactDeclaration

    env = CompletionEnv(tmp_path)
    job = Job('t', 'missing')
    entry = env.track_entry(job)
    env.channel.journal.record_artifact(job.uid, ArtifactDeclaration(str(tmp_path / 'missing'), name='data'))
    original = env.channel.journal.read_artifacts
    calls = []

    def read(uid):
        calls.append(uid)
        return original(uid)

    monkeypatch.setattr(env.channel.journal, 'read_artifacts', read)
    env.completion.complete_job(entry, ExecutionResult(success=True))
    assert calls == [job.uid]
    assert not env.backend.load_successes()
    assert 'Missing output' in env.store.state.failed[job.uid]['error_message']


@pytest.mark.parametrize('declaration_error', [False, True])
def test_environment_fault_preserves_resource_suspension(tmp_path, monkeypatch, declaration_error):
    from tasklite.exceptions import _ArtifactIOError
    from tasklite.utils.jsonutil import loads

    env = CompletionEnv(tmp_path, extra_resources={'api': RateLimitResource('api', 0.01)})
    entry = env.track_entry(Job('t', 'limited'))

    def denied(declarations, *, success):
        raise PermissionError('cleanup denied')

    monkeypatch.setattr(env.channel.journal, 'cleanup_declared_artifacts', denied)
    with pytest.raises((_ArtifactIOError, PermissionError)):
        env.completion.complete_job(entry, ExecutionResult(
            artifact_error='write denied' if declaration_error else None,
            retry_requested=True, transient_kind='rate_limited',
            resource_suspensions=[('api', 30.0)],
        ))
    assert env.resources['api'].suspended_until() is not None
    assert 'api' in loads(env.backend.get_engine_metadata('resource_suspensions'))
    assert entry.job.attempt_no == 1
    assert not env.store.state.failed
    assert not env.hooks


def test_reaped_batch_finishes_other_result_after_cleanup_error(tmp_path, monkeypatch):
    from tasklite.models.artifact import ArtifactDeclaration

    env = CompletionEnv(tmp_path, extra_resources={'api': RateLimitResource('api', 0.01)})
    first, second = Job('t', 'first'), Job('t', 'second')
    for job in (first, second):
        env.track_entry(job, attempt_id=env.open_attempt(job))
    env.channel.journal.record_artifact(first.uid, ArtifactDeclaration(str(tmp_path / 'broken')))

    def denied(declarations, *, success):
        if declarations:
            raise PermissionError('cleanup denied')

    monkeypatch.setattr(env.channel.journal, 'cleanup_declared_artifacts', denied)
    with pytest.raises(PermissionError):
        env.completion.settle_reaped([
            (env.make_handle(first.uid), ExecutionResult(success=False)),
            (env.make_handle(second.uid), ExecutionResult(success=True, resource_suspensions=[('api', 30.0)])),
        ])
    assert env.resources['api'].suspended_until() is not None
    assert second.uid in env.backend.load_successes()
    assert [snapshot['job_id'] for snapshot in env.store.state.queue] == ['first']
    assert not env.store.state.failed
    assert env.resources['__workers__'].used == 0
    assert not env.in_flight
    assert len(env.hooks) == 1
    assert env.hooks[0][0] == second.uid
