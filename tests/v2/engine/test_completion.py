"""v2 CompletionMachine 完成机器测试：唯一收尾出口、三态分支与批量结算。

覆盖：complete_job 成功/失败/重试生命周期（资源释放、清理模式、收尾
钩子恰好一次）、重试预算判定（attempt_no 对 max_retries+1）与瞬态信号
零预算、动态子任务去重与坏子作业拒绝、3-strike 终结的防双触发、
settle_reaped / settle_aborted 批量结算。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tasklite.v2.backend.memory import InMemoryStateBackend
from tasklite.v2.engine.admission import RerunPolicy
from tasklite.v2.engine.channel import (
    ArtifactCleanupMode,
    ExecutionChannel,
    ExecutionResult,
)
from tasklite.v2.engine.completion import CompletionMachine
from tasklite.v2.engine.errorclass import ERR_MAX_RETRIES, ErrorClassifier
from tasklite.v2.engine.in_flight import InFlightJob, InFlightTracker
from tasklite.v2.engine.resource import (
    CapacityResource,
    RateLimitResource,
    ResourceManager,
)
from tasklite.v2.engine.store import StateStore
from tasklite.v2.engine.types import AttemptFinish, JobHandle, TaskStats
from tasklite.v2.models.job import Job, JobRuntimeState
from tasklite.v2.models.state import PipelineState


class StubSession:
    """运行会话结构契约桩（run_id）。"""

    def __init__(self, run_id: str = "b" * 32) -> None:
        self.run_id = run_id
        self.result_token = "t" * 64

    def next_dispatch_seq(self) -> int:
        raise AssertionError("completion 不消费 dispatch_seq")


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
        self.stats = TaskStats()
        self.hooks: list[tuple[str, AttemptFinish]] = []
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
            rerun_policy=RerunPolicy(),
            channel=self.channel,
            resources=self.resources,
            in_flight=self.in_flight,
            session=self.session,
        )

    def track_entry(self, job: Job, *, lease=None, attempt_id=None) -> InFlightJob:
        if lease is None:
            lease = self.resources.reserve(job.task_type, job.resources, uid=job.uid)
        entry = InFlightJob(
            uid=job.uid,
            job_dict=job.to_dict(),
            job=job,
            lease=lease,
            attempt_id=attempt_id,
        )
        self.in_flight.track(entry, state=self.store.state)
        return entry

    def open_attempt(self, job: Job, *, attempt_no=1, activation_no=1) -> int:
        from tasklite.v2.models.attempt import AttemptRecord

        record = AttemptRecord(
            job_uid=job.uid,
            activation_no=activation_no,
            attempt_no=attempt_no,
            incarnation=f"{self.session.run_id}.1",
            run_id=self.session.run_id,
            started_at="2026-01-01T00:00:00+00:00",
            outcome="running",
        )
        return self.backend.append_attempt(record)

    def make_handle(self, uid: str) -> JobHandle:
        return JobHandle(
            uid=uid,
            process=FakeProcess(),
            deadline=1e18,
            timeout=60.0,
            job=Job(*uid.split("::", 1)),
            ipc_dir=self.ipc_dir,
            incarnation=f"{self.session.run_id}.1",
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
            env.channel, "cleanup_artifacts",
            lambda uid, *, mode: cleaned.append((uid, mode)),
        )
        monkeypatch.setattr(
            env.channel, "read_declared_inputs",
            lambda uid: [{"path": "/data/in.csv", "kind": "file", "size": 3}],
        )

        result = ExecutionResult(success=True, result_meta={"score": 100})
        env.completion.complete_job(entry, result)

        assert env.resources["gpu"].used == 0.0
        assert env.store.state.wall["t::j1"]["score"] == 100
        assert env.store.state.wall["t::j1"]["last_run_id"] == env.session.run_id
        assert env.store.state.wall["t::j1"]["inputs"][0]["path"] == "/data/in.csv"
        assert env.stats["completed"] == 1
        assert "t::j1" not in env.store.state.in_flight_uids
        assert cleaned == [("t::j1", ArtifactCleanupMode.SUCCESS)]
        # 收尾钩子恰好一次（成功语义）
        assert env.hooks == [
            ("t::j1", AttemptFinish(success=True, going_to_retry=False,
                                    meta={"score": 100}))
        ]
        assert env.backend.load_attempts("t::j1")[-1].outcome == "succeeded"

    def test_cursor_updates_applied(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1")
        entry = env.track_entry(job)

        env.completion.complete_job(
            entry,
            ExecutionResult(
                success=True,
                result_meta={},
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
            env.channel, "cleanup_artifacts",
            lambda uid, *, mode: cleaned.append((uid, mode)),
        )

        env.completion.complete_job(
            entry, ExecutionResult(success=False, result_meta={"error": "boom"})
        )

        assert env.store.state.failed["t::j1"]["error"] == "boom"
        assert env.store.state.failed["child::c1"]["error"] == "JOB_DEPENDENCY"
        assert env.stats["failed"] == 1
        assert env.stats["cascade_failed"] == 1
        assert cleaned[-1] == ("t::j1", ArtifactCleanupMode.FAILURE_OR_RETRY)
        assert env.backend.load_attempts("t::j1")[-1].outcome == "failed"
        finish_by_uid = {uid: fin for uid, fin in env.hooks}
        assert finish_by_uid["t::j1"].success is False
        assert finish_by_uid["t::j1"].going_to_retry is False


class TestRetryBudget:
    """重试预算判定与瞬态零预算。"""

    def test_business_retry_advances_attempt_no(self, tmp_path):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1", max_retries=2)
        attempt_id = env.open_attempt(job)
        entry = env.track_entry(job, attempt_id=attempt_id)

        result = ExecutionResult(
            retry_requested=True, retry_error="connection blip"
        )
        env.completion.complete_job(entry, result)

        assert result.going_to_retry is True
        assert env.stats["retried"] == 1
        assert env.store.state.queue[0]["attempt_no"] == 2
        assert env.store.state.queue[0]["first_enqueued_at"] == job.first_enqueued_at
        rt = JobRuntimeState.from_dict(env.store.state.queue[0].get("runtime"))
        assert rt.last_retry_error == "connection blip"
        assert env.backend.load_attempts("t::j1")[-1].outcome == "requeued"
        assert env.hooks[-1] == (
            "t::j1",
            AttemptFinish(success=False, going_to_retry=True, meta={}),
        )

    def test_retry_dict_preserves_custom_fields(self, tmp_path):
        """重试 = 原作业原样重入队：顶层自定义字段与已注入资源随重试往返保留。"""
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1", resources={"__workers__": 1.0})
        entry = env.track_entry(job)
        job_dict = dict(entry.job_dict)
        job_dict["custom_note"] = "keep-me"

        env.completion.apply_result(
            "t::j1", job, job_dict,
            ExecutionResult(retry_requested=True, retry_error="x"),
        )
        assert env.store.state.queue[0]["custom_note"] == "keep-me"
        assert env.store.state.queue[0]["resources"] == {"__workers__": 1.0}

    def test_budget_exhaustion_archives(self, tmp_path):
        env = CompletionEnv(tmp_path)
        # attempt_no=2 且 max_retries=1：允许执行条件 attempt_no <= 2 已到界
        job = Job("t", "j1", max_retries=1, attempt_no=2)
        job.runtime.record_retry_error("first failure")
        attempt_id = env.open_attempt(job, attempt_no=2)
        entry = env.track_entry(job, attempt_id=attempt_id)
        entry.job_dict["runtime"] = job.runtime.to_dict()

        result = ExecutionResult(retry_requested=True, retry_error="second")
        env.completion.complete_job(entry, result)

        assert result.going_to_retry is False
        assert "t::j1" in env.store.state.failed
        assert env.store.state.failed["t::j1"]["error"] == ERR_MAX_RETRIES
        assert env.store.state.failed["t::j1"]["last_retry_error"] == "first failure"
        assert env.store.state.failed["t::j1"]["retry_error"] == "second"
        assert env.stats["failed"] == 1
        assert env.backend.load_attempts("t::j1")[-1].outcome == "failed"

    def test_transient_zero_budget_requeues_beyond_budget(self, tmp_path):
        """瞬态信号：attempt_no 超预算仍重入队，attempt_no 不推进。"""
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1", max_retries=0, attempt_no=5)
        attempt_id = env.open_attempt(job, attempt_no=5)
        entry = env.track_entry(job, attempt_id=attempt_id)

        result = ExecutionResult(
            retry_requested=True,
            retry_error="interrupted",
            transient_kind="interrupted",
        )
        env.completion.complete_job(entry, result)

        assert result.going_to_retry is True
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
        entry.job_dict["runtime"] = job.runtime.to_dict()

        result = ExecutionResult(success=True, result_meta={"score": 1})
        env.completion.complete_job(entry, result)  # 不上抛 _JobTerminated

        assert result.going_to_retry is False
        assert "t::j1" in env.store.state.failed
        assert env.store.state.failed["t::j1"]["error"] == "COMMIT_FAILURE"
        # 钩子恰好一次：3-strike 分支已触发，尾部跳过
        assert len(env.hooks) == 1
        assert env.hooks[0][1].success is False
        assert env.hooks[0][1].meta["error"] == "COMMIT_FAILURE"


class TestSpawnedJobs:
    """动态子任务去重与坏子作业拒绝。"""

    def test_spawn_dedup_queue_hit_blocks_rerun(self, tmp_path):
        """queue/in-flight 命中无条件拦截，优先于 every_run 豁免。"""
        env = CompletionEnv(tmp_path)
        env.store.state.register_in_flight("child::x")
        env.store.apply_success("child::x", {})
        # X 重跑中（queue，every_run 放行重跑）
        env.store.state.spawn_jobs([Job("child", "x", rerun="every_run").to_dict()])

        parent = Job("parent", "p1")
        entry = env.track_entry(parent)
        env.completion.apply_result(
            "parent::p1", parent, entry.job_dict,
            ExecutionResult(
                success=True,
                new_jobs=[Job("child", "x", rerun="every_run")],
            ),
        )
        assert "parent::p1" in env.store.state.wall
        x_rows = [jd for jd in env.store.state.queue if jd["job_id"] == "x"]
        assert len(x_rows) == 1, "spawn 必须被 queue 命中拦截"

    def test_spawn_wall_hit_every_run_admitted(self, tmp_path):
        env = CompletionEnv(tmp_path)
        env.store.state.register_in_flight("child::y")
        env.store.apply_success("child::y", {})
        # wall 命中 + every_run：spawn 放行（唯一终态历史行被替换语义）

        parent = Job("parent", "p1")
        entry = env.track_entry(parent)
        env.completion.apply_result(
            "parent::p1", parent, entry.job_dict,
            ExecutionResult(
                success=True,
                new_jobs=[Job("child", "y", rerun="every_run")],
            ),
        )
        assert "parent::p1" in env.store.state.wall
        assert "child::y" in env.store.state.queue_uids

    def test_spawn_dedup_within_batch(self, tmp_path):
        env = CompletionEnv(tmp_path)
        parent = Job("parent", "p1")
        entry = env.track_entry(parent)
        env.completion.apply_result(
            "parent::p1", parent, entry.job_dict,
            ExecutionResult(
                success=True,
                new_jobs=[Job("child", "a"), Job("child", "a")],
            ),
        )
        rows = [jd for jd in env.store.state.queue if jd["job_id"] == "a"]
        assert len(rows) == 1

    def test_unserializable_spawned_job_rejected_not_parent(self, tmp_path):
        """坏子作业独立进失败档案，父作业成功提交不受连坐。"""
        env = CompletionEnv(tmp_path)

        class Unserializable:
            pass

        parent = Job("parent", "p1")
        entry = env.track_entry(parent)
        env.completion.apply_result(
            "parent::p1", parent, entry.job_dict,
            ExecutionResult(
                success=True,
                result_meta={"ok": 1},
                new_jobs=[Job("child", "bad", payload={"x": Unserializable()})],
            ),
        )
        assert "parent::p1" in env.store.state.wall
        assert "child::bad" in env.store.state.failed
        assert "INVALID_SPAWNED_JOB" in env.store.state.failed["child::bad"]["error"]
        # 伪 entry 无轨迹行：拒绝路径不落 attempt
        assert env.backend.load_attempts("child::bad") == []


class TestSuspensionApplication:
    """结果携带的资源挂起全局应用。"""

    def test_suspensions_applied_and_persisted(self, tmp_path):
        from tasklite.v2.utils.jsonutil import loads

        env = CompletionEnv(
            tmp_path, extra_resources={"api": RateLimitResource("api", 0.01)}
        )
        job = Job("t", "j1")
        entry = env.track_entry(job)

        env.completion.complete_job(
            entry,
            ExecutionResult(
                success=True,
                result_meta={},
                resource_suspensions=[("api", 30.0)],
            ),
        )
        assert env.resources["api"].suspended_until() is not None
        raw = env.backend.get_meta("resource_suspensions")
        assert raw is not None and "api" in loads(raw)

    def test_unknown_resource_suspension_skipped(self, tmp_path, caplog):
        env = CompletionEnv(tmp_path)
        job = Job("t", "j1")
        entry = env.track_entry(job)

        env.completion.complete_job(
            entry,
            ExecutionResult(
                success=True, result_meta={}, resource_suspensions=[("ghost", 5.0)],
            ),
        )
        assert "t::j1" in env.store.state.wall


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
        env.completion.apply_result(
            "t::j1", job, job.to_dict(),
            ExecutionResult(success=True),
            expect_in_flight=False,
        )
        assert "t::j1" in env.store.state.wall


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
            (e1.handle, ExecutionResult(success=True, result_meta={})),
            (e2.handle, ExecutionResult(success=False, result_meta={"error": "x"})),
        ])
        assert count == 2
        assert "t::j1" in env.store.state.wall
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
        e1 = env.track_entry(j1)
        j2 = Job("t", "j2")
        e2 = env.track_entry(j2)

        env.completion.settle_aborted(
            cancelled_entries=[e1],
            done_entries=[(e2, ExecutionResult(success=True, result_meta={}))],
        )
        assert [jd["job_id"] for jd in env.store.state.queue] == ["j1"]
        assert "t::j2" in env.store.state.wall
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

        from tasklite.v2.exceptions import _CommitCrashSignal

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
        job = Job("t", "j1", max_retries=3)
        entry = env.track_entry(job)
        env.completion.complete_job(
            entry, ExecutionResult(retry_requested=True, retry_error="flaky")
        )
        assert env.hooks == [
            ("t::j1", AttemptFinish(success=False, going_to_retry=True, meta={}))
        ]

    def test_hook_error_isolated(self, tmp_path):
        env = CompletionEnv(tmp_path)
        env.store.set_on_attempt_finished(
            lambda uid, *, outcome: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        job = Job("t", "j1")
        entry = env.track_entry(job)
        env.completion.complete_job(entry, ExecutionResult(success=True))
        assert env.stats["hook_errors"] == 1
        assert "t::j1" in env.store.state.wall
