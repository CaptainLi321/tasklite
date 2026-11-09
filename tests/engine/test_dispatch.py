"""v2 DispatchMachine 派发机器测试：五关预检、两阶段租约、轨迹开行与瞬态分支。

覆盖：DispatchOutcome 单一 kind 枚举判据、五关时序契约（dedup →
dep-failed → no-handler → orphan-probe → stale-restore）、trigger 放行的
激活推进（activation_no+1 / attempt_no 重置）、retry 候选来源与累计预算
分离、派发即插 attempts 轨迹行与 execution_id 生成、派发失败 3-strike 收敛。
"""

from __future__ import annotations

from collections import defaultdict

from tasklite.triggers import BUILTIN_TRIGGER_FUNCTIONS

from pathlib import Path
from typing import Any

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import TriggerRegistry
from tasklite.engine.channel import ExecutionChannel, WorkerLaunchSpec
from tasklite.engine.dispatch import (
    DispatchKind,
    DispatchMachine,
    DispatchOutcome,
)
from tasklite.engine.errorclass import (
    ERR_DISPATCH_FAILURE,
    ERR_JOB_DEPENDENCY,
    ERR_NO_HANDLER,
    ErrorClassifier,
)
from tasklite.engine.in_flight import InFlightTracker
from tasklite.engine.recovery import RecoveryOrchestrator
from tasklite.engine.resource import (
    CapacityResource,
    IntervalResource,
    ResourceManager,
)
from tasklite.engine.scheduler import JobScheduler
from tasklite.engine.store import StateStore
from tasklite.engine.types import AttemptOutcome, StopMode
from tasklite.models.artifact import FileSpec
from tasklite.models.job import Job, JobRuntimeState, RT_DISPATCH_FAILURES
from tasklite.models.state import PipelineState
from tasklite.models.task import Task, TaskRegistry


class StubSession:
    """派发消费的运行会话结构契约桩。"""

    def __init__(self, run_id: str = "a" * 32) -> None:
        self.run_id = run_id
        self.stop_mode = StopMode.NONE
        self._dispatch_no = 0
        self._retry_counts: dict[str, int] = {}
        self._retry_candidates: set[str] = set()

    def next_dispatch_no(self) -> int:
        self._dispatch_no += 1
        return self._dispatch_no

    def get_retry_count(self, job_uid: str) -> int:
        return self._retry_counts.get(job_uid, 0)

    def is_retry_candidate(self, job_uid: str) -> bool:
        return job_uid in self._retry_candidates

    def mark_retry_candidate(self, job_uid: str) -> None:
        self._retry_candidates.add(job_uid)

    def clear_retry_candidate(self, job_uid: str) -> None:
        self._retry_candidates.discard(job_uid)


class RecordingCompletion:
    """完成机器结构契约桩：记录伪 entry 提交（stale-restore 门消费）。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def complete_job(self, entry, result) -> None:
        self.calls.append((entry.job_uid, result))

    def settle_aborted(self, cancelled, done) -> None:
        pass


class FakeProcess:
    def __init__(self, pid: int = 12345):
        self.pid = pid


class DispatchEnv:
    """派发机器窄依赖装配（字段即依赖清单）。"""

    def __init__(self, tmp_path, *, capacity: float = 2.0, extra_resources=None):
        self.ipc_dir = str(tmp_path / "ipc")
        Path(self.ipc_dir).mkdir(parents=True, exist_ok=True)
        self.output_roots = str(tmp_path / "out")

        resources_map = {
            "__workers__": CapacityResource("__workers__", max_capacity=capacity),
        }
        if extra_resources:
            resources_map.update(extra_resources)
        self.resources = ResourceManager(resources_map)
        self.backend = InMemoryStateBackend()
        self.stats = defaultdict(int)
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
        self.completion = RecordingCompletion()
        self.recovery = RecoveryOrchestrator(
            store=self.store,
            channel=self.channel,
            resources=self.resources,
            in_flight=self.in_flight,
            completion=self.completion,
        )
        self.tasks = TaskRegistry()
        self.dispatch = DispatchMachine(
            store=self.store,
            scheduler=JobScheduler(self.resources),
            trigger_registry=TriggerRegistry(functions=BUILTIN_TRIGGER_FUNCTIONS),
            resources=self.resources,
            channel=self.channel,
            in_flight=self.in_flight,
            session=self.session,
            recovery=self.recovery,
            tasks=self.tasks,
            classifier=ErrorClassifier(),
            output_roots=self.output_roots,
            ipc_dir=self.ipc_dir,
            commit_failure_threshold=3,
        )

    def register(self, task_type: str) -> None:
        self.tasks.register(Task(
            task_type=task_type,
            handler=lambda job, ctx: True,
        ))

    def spawn_handle(self, uid: str, execution_id: str):
        from tasklite.engine.types import JobHandle

        return JobHandle(
            job_uid=uid,
            process=FakeProcess(),
            timeout_at_monotonic=1e18,
            timeout_seconds=60.0,
            job=Job(*uid.split("::", 1)),
            ipc_dir=self.ipc_dir,
            execution_id=execution_id,
        )


class TestDispatchOutcomeKind:
    """dispatch_next 的单一 kind 枚举判据。"""

    def test_empty_queue_is_no_candidate(self, tmp_path):
        env = DispatchEnv(tmp_path, capacity=1)
        outcome = env.dispatch.dispatch_next()
        assert isinstance(outcome, DispatchOutcome)
        assert outcome.kind is DispatchKind.NO_CANDIDATE
        assert outcome.entry is None
        assert outcome.standstill.min_wait == float("inf")

    def test_workers_exhausted_is_worker_saturated(self, tmp_path):
        env = DispatchEnv(tmp_path, capacity=0)
        env.store.state.spawn_jobs([Job("t", "j1").to_dict()])
        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.WORKER_SATURATED
        assert outcome.worker_wait > 0
        assert outcome.entry is None

    def test_missing_dependency_settles_without_worker_slot(self, tmp_path):
        env = DispatchEnv(tmp_path, capacity=0)
        env.store.state.spawn_jobs([
            Job("t", "gap", depends_on=["t::missing"]).to_dict(),
            Job("t", "downstream", depends_on=["t::gap"]).to_dict(),
        ])

        outcome = env.dispatch.dispatch_next()

        assert outcome.kind is DispatchKind.HANDLED_NO_SUBPROCESS
        assert set(env.backend.load_failed()) == {"t::gap", "t::downstream"}
        assert env.backend.load_failed()["t::gap"]["missing_dependency"] == "t::missing"
        assert env.backend.load_attempts("t::gap") == []
        assert env.backend.load_attempts("t::downstream") == []

    def test_stop_keeps_missing_dependency_pending(self, tmp_path):
        env = DispatchEnv(tmp_path)
        env.session.stop_mode = StopMode.DRAINING
        missing = Job("t", "gap", depends_on=["t::missing"]).to_dict()
        env.store.state.spawn_jobs([missing])

        outcome = env.dispatch.dispatch_next()

        assert outcome.kind is DispatchKind.HANDLED_NO_SUBPROCESS
        assert env.backend.load_failed() == {}
        assert env.store.state.queue == [missing]

    def test_authorization_pending_precedes_missing_dependency(self, tmp_path):
        env = DispatchEnv(tmp_path)
        env.register("t")
        target = tmp_path / "out" / "result.txt"
        target.parent.mkdir()
        target.write_text("stale", encoding="utf-8")
        env.store.authorizer = lambda *_args: None
        env.store.state.spawn_jobs([
            Job(
                "t",
                "authorized",
                files={
                    "result": FileSpec(
                        target,
                        role="output",
                        delete_existing=True,
                    ),
                },
            ).to_dict(),
            Job("t", "gap", depends_on=["t::missing"]).to_dict(),
        ])

        first = env.dispatch.dispatch_next()
        second = env.dispatch.dispatch_next()

        assert first.kind is DispatchKind.AUTHORIZATION_PENDING
        assert second.kind is DispatchKind.AUTHORIZATION_PENDING
        assert set(env.store.state.queue_uids) == {"t::authorized", "t::gap"}
        assert env.backend.load_failed() == {}
        assert env.backend.load_attempts("t::gap") == []

    def test_spawned_entry_carries_attempt_row(self, tmp_path, monkeypatch):
        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.spawn_jobs([Job("t", "j1").to_dict()])

        spawned_specs: list[WorkerLaunchSpec] = []
        monkeypatch.setattr(
            env.channel,
            "spawn",
            lambda spec: spawned_specs.append(spec) or env.spawn_handle(
                spec.job.uid, spec.execution_id
            ),
        )

        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.SPAWNED
        entry = outcome.entry
        assert entry is not None
        assert entry.job_uid == "t::j1"
        assert entry.attempt_id is not None
        assert "t::j1" in env.in_flight
        assert "t::j1" in env.store.state.in_flight_uids

        # 派发即插 running 轨迹行 + execution_id 生成于派发点
        records = env.backend.load_attempts("t::j1")
        assert len(records) == 1
        assert records[0].outcome == "running"
        assert records[0].execution_id == f"{env.session.run_id}.1"
        assert records[0].run_id == env.session.run_id
        assert records[0].activation_no == 1
        assert records[0].attempt_no == 1
        assert spawned_specs[0].execution_id == records[0].execution_id


class TestDedupGate:
    """预检关 1——去重与 trigger 放行。"""

    def test_wall_hit_without_rerun_skips(self, tmp_path):
        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.spawn_jobs([Job("t", "j1").to_dict()])
        env.store.state.pop_job(0)
        env.store.state.register_in_flight("t::j1")
        aid = env.store.record_attempt_dispatch(
            Job("t", "j1"), execution_id=f"{env.session.run_id}.1", run_id=env.session.run_id
        )
        env.store.apply_success("t::j1", {}, attempt_id=aid, execution_id=f"{env.session.run_id}.1")
        env.store.state.spawn_jobs([Job("t", "j1").to_dict()])

        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.HANDLED_NO_SUBPROCESS
        assert env.stats["skipped"] == 1
        assert "t::j1" not in env.store.state.queue_uids
        # 跳过同样落轨迹行（skipped 结局）
        records = env.backend.load_attempts("t::j1")
        assert records[-1].outcome == "skipped"

    def test_rerun_admission_advances_activation(self, tmp_path, monkeypatch):
        """every_run 放行：activation_no+1、attempt_no 重置 1、字面 trigger 键落行。"""
        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.register_in_flight("t::j1")
        aid = env.store.record_attempt_dispatch(
            Job("t", "j1"), execution_id=f"{env.session.run_id}.1", run_id=env.session.run_id
        )
        env.store.apply_success("t::j1", {}, attempt_id=aid, execution_id=f"{env.session.run_id}.1")
        env.store.state.spawn_jobs([Job("t", "j1", trigger="always").to_dict()])
        monkeypatch.setattr(
            env.channel, "spawn",
            lambda spec: env.spawn_handle(spec.job.uid, spec.execution_id),
        )

        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.SPAWNED
        entry = outcome.entry
        assert entry is not None
        assert entry.job.activation_no == 2
        assert entry.job.attempt_no == 1
        assert entry.job_snapshot["trigger"] == "always"
        assert entry.job_snapshot["activation_no"] == 2
        assert entry.job_snapshot["attempt_no"] == 1
        # 轨迹行按新激活代号落表
        records = env.backend.load_attempts("t::j1")
        assert records[-1].activation_no == 2
        assert records[-1].attempt_no == 1
        # 豁免已登记（in-flight 互斥不击落 successes 命中的重跑）
        assert "t::j1" in env.store.state.in_flight_uids

    def test_failed_hit_with_until_success_reruns(self, tmp_path, monkeypatch):
        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.register_in_flight("t::j1")
        aid = env.store.record_attempt_dispatch(
            Job("t", "j1"), execution_id=f"{env.session.run_id}.1", run_id=env.session.run_id
        )
        env.store.apply_failure("t::j1", {"error_message": "boom"}, attempt_id=aid)
        env.store.state.spawn_jobs(
            [Job("t", "j1", trigger="until_success").to_dict()]
        )
        monkeypatch.setattr(
            env.channel, "spawn",
            lambda spec: env.spawn_handle(spec.job.uid, spec.execution_id),
        )

        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.SPAWNED
        assert outcome.entry.job.activation_no == 2


class TestRejectGates:
    """预检关 2/3 与载荷校验拒绝路径。"""

    def test_no_handler_gate_fails_with_hook(self, tmp_path):
        env = DispatchEnv(tmp_path)
        env.store.state.spawn_jobs([Job("ghost", "j1").to_dict()])

        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.HANDLED_NO_SUBPROCESS
        assert env.stats["failed"] == 1
        assert "ghost::j1" in env.store.state.failed
        assert env.store.state.failed["ghost::j1"]["error_message"] == ERR_NO_HANDLER
        assert env.hooks == [
            ("ghost::j1", AttemptOutcome(success=False, will_retry=False,
                                        metadata=env.store.state.failed["ghost::j1"]))
        ]
        records = env.backend.load_attempts("ghost::j1")
        assert records[-1].outcome == "failed"

    def test_dep_failed_gate_cascades_downstream(self, tmp_path):
        env = DispatchEnv(tmp_path)
        env.register("child")
        env.store.state.spawn_jobs([
            Job("child", "b", depends_on=["parent::a"]).to_dict(),
            Job("child", "c", depends_on=["child::b"]).to_dict(),
        ])
        from tasklite.engine.scheduler import ScheduleResult

        sched = ScheduleResult(
            runnable_idx=0, pending_dep_failure="parent::a", kind="dep_failed"
        )
        outcome = env.dispatch.dispatch_job(sched)
        assert outcome is None
        assert env.stats["cascade_failed"] == 2
        assert env.store.state.failed["child::b"]["error_message"] == ERR_JOB_DEPENDENCY
        assert env.store.state.failed["child::b"]["failed_dependency"] == "parent::a"
        assert env.store.state.failed["child::c"]["error_message"] == ERR_JOB_DEPENDENCY

    def test_valid_payload_proceeds_to_spawn(self, tmp_path, monkeypatch):
        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.spawn_jobs([Job("t", "j1", payload={"name": "x"}).to_dict()])
        monkeypatch.setattr(
            env.channel, "spawn",
            lambda spec: env.spawn_handle(spec.job.uid, spec.execution_id),
        )
        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.SPAWNED


class TestPreSubmitCleanupAndActiveSignals:
    """声明式前删与在途信号排空。"""

    def test_authorization_approval_rechecks_all_resources(self, tmp_path, monkeypatch):
        env = DispatchEnv(tmp_path, capacity=1)
        env.register("t")
        target = tmp_path / "out" / "result.txt"
        target.parent.mkdir()
        target.write_text("stale", encoding="utf-8")

        def approve_after_consuming_slot(path, target_type):
            env.resources["__workers__"].acquire(1.0)
            return True

        env.store.authorizer = approve_after_consuming_slot
        env.store.state.spawn_jobs([
            Job(
                "t",
                "j1",
                resources={"__workers__": 1.0},
                files={
                    "result": FileSpec(
                        target,
                        role="output",
                        delete_existing=True,
                    ),
                },
            ).to_dict(),
        ])
        spawned: list[WorkerLaunchSpec] = []
        monkeypatch.setattr(
            env.channel,
            "spawn",
            lambda spec: spawned.append(spec) or env.spawn_handle(
                spec.job.uid, spec.execution_id
            ),
        )

        outcome = env.dispatch.dispatch_next()

        assert outcome.kind is DispatchKind.HANDLED_NO_SUBPROCESS
        assert spawned == []
        assert "t::j1" in env.store.state.queue_uids
        assert target.read_text(encoding="utf-8") == "stale"
        assert env.resources["__workers__"].used == 1.0
        env.resources["__workers__"].release(1.0)

    def test_declared_delete_existing_runs_once_before_spawn(self, tmp_path, monkeypatch):
        env = DispatchEnv(tmp_path)
        env.register("t")
        target = tmp_path / "out" / "result.txt"
        target.parent.mkdir()
        target.write_text("stale", encoding="utf-8")
        env.store.authorizer = lambda *_args: True
        env.store.state.spawn_jobs([
            Job(
                "t",
                "j1",
                files={
                    "result": FileSpec(
                        target,
                        role="output",
                        delete_existing=True,
                    ),
                },
            ).to_dict(),
        ])
        spawn_observed: list[bool] = []
        monkeypatch.setattr(
            env.channel, "spawn",
            lambda spec: spawn_observed.append(target.exists())
            or env.spawn_handle(spec.job.uid, spec.execution_id),
        )

        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.SPAWNED
        assert spawn_observed == [False]

    def test_interval_resource_defers_without_burning_budget(
        self, tmp_path
    ):
        """IntervalResource 处于间隔窗内：作业留队，不消耗派发失败预算。"""
        rate_limit = IntervalResource("api", interval_seconds=3600.0)
        env = DispatchEnv(tmp_path, extra_resources={"api": rate_limit})
        env.register("t")
        rate_limit.acquire(1.0)  # 消费时间片
        job = Job("t", "j1", resources={"api": 1.0})
        env.store.state.spawn_jobs([job.to_dict()])

        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.NO_CANDIDATE
        assert "t::j1" in env.store.state.queue_uids
        rt = JobRuntimeState.from_dict(env.store.state.queue[0].get("runtime"))
        assert rt.dispatch_failures == 0, "资源等待不得计入派发失败 3-strike"


class TestDispatchFailureStrike:
    """派发异常分支：3-strike 派发失败预算收敛。"""

    def _spawn_raises(self, env, exc):
        def boom(spec):
            raise exc

        env.channel.spawn = boom

    def test_below_threshold_requeues_and_raises(self, tmp_path):
        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.spawn_jobs([Job("t", "j1").to_dict()])
        self._spawn_raises(env, RuntimeError("boom"))

        with pytest.raises(RuntimeError, match="boom"):
            env.dispatch.dispatch_next()
        assert "t::j1" in env.store.state.queue_uids
        rt = JobRuntimeState.from_dict(env.store.state.queue[0].get("runtime"))
        assert rt.dispatch_failures == 1
        records = env.backend.load_attempts("t::j1")
        assert records[-1].outcome == "requeued"

    def test_at_threshold_terminates_into_failure_archive(self, tmp_path):
        env = DispatchEnv(tmp_path)
        env.register("t")
        jd = Job("t", "j1").to_dict()
        jd["runtime"] = {RT_DISPATCH_FAILURES: 2}
        env.store.state.spawn_jobs([jd])
        self._spawn_raises(env, RuntimeError("deterministic badness"))

        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.HANDLED_NO_SUBPROCESS
        assert "t::j1" in env.store.state.failed
        assert env.store.state.failed["t::j1"]["error_message"] == ERR_DISPATCH_FAILURE
        assert env.store.state.failed["t::j1"]["failures"] == 3
        records = env.backend.load_attempts("t::j1")
        assert records[-1].outcome == "failed"
        assert [uid for uid, _ in env.hooks] == ["t::j1"]

    def test_interrupt_requeues_and_reraises(self, tmp_path):
        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.spawn_jobs([Job("t", "j1").to_dict()])
        self._spawn_raises(env, KeyboardInterrupt())

        with pytest.raises(KeyboardInterrupt):
            env.dispatch.dispatch_next()
        assert "t::j1" in env.store.state.queue_uids

    def test_commit_crash_signal_passes_through_untouched(self, tmp_path):
        from tasklite.exceptions import _CommitCrashSignal

        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.spawn_jobs([Job("t", "j1").to_dict()])

        def boom(spec):
            raise _CommitCrashSignal("backend down")

        env.channel.spawn = boom
        with pytest.raises(_CommitCrashSignal):
            env.dispatch.dispatch_next()
        # 崩溃信号穿透：不计派发失败预算、不写失败档案（交运行循环崩溃网
        # 按磁盘真相统一收尾）
        assert "t::j1" not in env.store.state.failed
        assert env.stats["failed"] == 0
        records = env.backend.load_attempts("t::j1")
        assert records[-1].outcome == "running"

    def test_assertion_error_passes_through(self, tmp_path):
        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.spawn_jobs([Job("t", "j1").to_dict()])

        def boom(spec):
            raise AssertionError("invariant broken")

        env.channel.spawn = boom
        with pytest.raises(AssertionError):
            env.dispatch.dispatch_next()
        assert "t::j1" not in env.store.state.failed
        records = env.backend.load_attempts("t::j1")
        assert records[-1].outcome == "running"

    def test_worker_start_failure_releases_acquired_slots(self, tmp_path, monkeypatch):
        env = DispatchEnv(tmp_path, capacity=1)
        env.register("t")
        env.store.state.spawn_jobs([
            Job("t", "j1", resources={"__workers__": 1.0}).to_dict(),
        ])

        def fail_spawn(spec):
            raise OSError("process start failed")

        monkeypatch.setattr(env.channel, "spawn", fail_spawn)

        with pytest.raises(OSError, match="process start failed"):
            env.dispatch.dispatch_next()

        assert env.resources["__workers__"].used == 0.0
        assert "t::j1" not in env.in_flight


class TestGateOrderContract:
    """顺序契约：dep-failed 先于 dedup。"""

    def test_dep_failed_takes_precedence_over_dedup(self, tmp_path):
        """带失败依赖的作业优先判定为依赖失败，不得借旧成功跳过。"""
        env = DispatchEnv(tmp_path)
        env.register("t")
        env.backend.commit_job_failure("dead::parent", {"error_message": "dead"})
        env.store.state.mark_failed("dead::parent", {"error_message": "dead"})
        env.store.state.spawn_jobs([Job("t", "j1").to_dict()])
        env.store.state.pop_job(0)
        env.store.state.register_in_flight("t::j1")
        aid = env.store.record_attempt_dispatch(
            Job("t", "j1"), execution_id=f"{env.session.run_id}.1", run_id=env.session.run_id
        )
        env.store.apply_success("t::j1", {}, attempt_id=aid, execution_id=f"{env.session.run_id}.1")
        env.store.state.spawn_jobs([
            Job("t", "j1", depends_on=["dead::parent"]).to_dict()
        ])

        outcome = env.dispatch.dispatch_next()
        assert outcome.kind is DispatchKind.HANDLED_NO_SUBPROCESS
        assert env.stats["skipped"] == 0
        assert env.stats["cascade_failed"] == 1
        assert "t::j1" in env.store.state.failed


class TestTraceAppendReliability:
    """轨迹开行可靠保障：派发开行失败保队且不启动 handler。"""

    def test_append_failure_preserves_queue_without_starting_handler(self, tmp_path, monkeypatch):
        from tasklite.exceptions import _CommitCrashSignal

        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.spawn_jobs([Job("t", "j1").to_dict()])
        monkeypatch.setattr(
            env.backend,
            "append_attempt",
            lambda record: (_ for _ in ()).throw(RuntimeError("trace down")),
        )
        spawned: list[Any] = []
        monkeypatch.setattr(
            env.channel, "spawn",
            lambda spec: spawned.append(spec) or env.spawn_handle(spec.job.uid, spec.execution_id),
        )

        with pytest.raises(_CommitCrashSignal, match="Failed to append attempt row"):
            env.dispatch.dispatch_next()
        assert spawned == []
        assert "t::j1" in env.store.state.queue_uids
        assert "t::j1" not in env.in_flight
        assert "t::j1" not in env.store.state.in_flight_uids
        assert env.backend.load_attempts("t::j1") == []


class TestAdmissionEvaluationTiming:
    """求值时序契约：求值前不 pop 丢队，来源只覆盖显式 retry。"""

    def test_evaluation_exception_preserves_candidate_in_queue(self, tmp_path, monkeypatch):
        """准入求值异常时候选原位保留在队列中，不 pop 丢队。"""
        env = DispatchEnv(tmp_path)
        env.register("t")
        job = Job("t", "j1")
        env.store.state.spawn_jobs([job.to_dict()])

        def broken_admit(*args, **kwargs):
            raise ValueError("bad trigger evaluation")

        monkeypatch.setattr(env.dispatch._trigger_registry, "admit", broken_admit)

        with pytest.raises(ValueError, match="bad trigger evaluation"):
            env.dispatch.dispatch_next()

        assert "t::j1" in env.store.state.queue_uids
        assert env.store.state.queue[0]["job_id"] == "j1"
        assert "t::j1" not in env.store.state.in_flight_uids

    @pytest.mark.parametrize("registered_as_retry", [False, True])
    def test_trigger_evaluation_depends_on_retry_source_not_budget(
        self, tmp_path, monkeypatch, registered_as_retry
    ):
        """累计预算不为普通候选提供准入豁免；显式来源才跳过求值。"""
        env = DispatchEnv(tmp_path)
        env.register("t")
        env.store.state.mark_success("t::j1", {})
        retry_job = Job._restore("t", "j1", attempt_no=2, activation_no=1)
        env.store.state.spawn_jobs([retry_job.to_dict()])
        env.session._retry_counts["t::j1"] = 1
        if registered_as_retry:
            env.session.mark_retry_candidate("t::j1")
        monkeypatch.setattr(
            env.channel, "spawn",
            lambda spec: env.spawn_handle(spec.job.uid, spec.execution_id),
        )

        outcome = env.dispatch.dispatch_next()
        if registered_as_retry:
            assert outcome.kind is DispatchKind.SPAWNED
            assert outcome.entry.job.attempt_no == 2
        else:
            assert outcome.kind is DispatchKind.HANDLED_NO_SUBPROCESS
            assert env.stats["skipped"] == 1
            assert "t::j1" not in env.store.state.queue_uids
