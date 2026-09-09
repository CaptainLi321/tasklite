"""CompletionMachine 完成与结算深模块单元测试套件。"""
from __future__ import annotations

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.channel import ArtifactCleanupMode, ExecutionChannel, ExecutionResult, JobHandle
from tasklite.engine.completion import CompletionMachine
from tasklite.engine.inflight import InFlightJob, InFlightTracker
from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.engine.runtime import RunContext
from tasklite.engine.scheduler import JobScheduler
from tasklite.engine.store import StateStore
from tasklite.exceptions import _CommitCrashSignal, _JobTerminated
from tasklite.models.job import Job
from tasklite.models.state import PipelineState


class FakeProcess:
    def __init__(self, pid=12345):
        self.pid = pid


def _make_context(tmp_path):
    backend = InMemoryStateBackend()
    gpu = CapacityResource("gpu", 10.0)
    rm = ResourceManager({"gpu": gpu})
    handlers = {}
    scheduler = JobScheduler(rm, handlers)
    ctx = RunContext(
        name="test_pipeline",
        backend=backend,
        scheduler=scheduler,
        resources=rm,
        handlers=handlers,
        ipc_dir=str(tmp_path / "ipc"),
    )
    ctx.set_state(PipelineState({}, {}, {}, []))
    return ctx


class TestCompletionMachineBasicSettlement:
    def test_complete_job_success_lifecycle(self, tmp_path):
        ctx = _make_context(tmp_path)
        completion = CompletionMachine(ctx)

        gpu = ctx.resource_mgr["gpu"]
        lease = ctx.resource_mgr.reserve("t", {"gpu": 2.0}, uid="t::j1")
        job = Job("t", "j1", {"gpu": 2.0})
        entry = InFlightJob(uid="t::j1", job_dict=job.to_dict(), job=job, lease=lease)
        ctx.in_flight.track(entry, state=ctx.store)

        assert gpu.used == 2.0
        assert "t::j1" in ctx.store.in_flight_uids

        result = ExecutionResult(
            success=True,
            result_meta={"score": 100},
        )

        completed_events = []
        ctx.on_job_completed = lambda uid, meta, success, retry: completed_events.append(
            (uid, meta, success, retry)
        )

        completion.complete_job(entry, result)

        assert gpu.used == 0.0
        assert "t::j1" in ctx.store.wall
        assert ctx.store.wall["t::j1"]["score"] == 100
        assert ctx.stats.completed == 1
        assert len(completed_events) == 1
        assert completed_events[0] == ("t::j1", {"score": 100}, True, False)

    def test_settle_reaped_batch(self, tmp_path):
        ctx = _make_context(tmp_path)
        completion = CompletionMachine(ctx)

        j1 = Job("t", "j1", {})
        h1 = JobHandle("t::j1", FakeProcess(101), 100.0, 10.0, j1, ctx.ipc_dir, "run1.1")
        e1 = InFlightJob("t::j1", j1.to_dict(), j1, [], h1, None)
        ctx.in_flight.track(e1, state=ctx.store)

        j2 = Job("t", "j2", {})
        h2 = JobHandle("t::j2", FakeProcess(102), 100.0, 10.0, j2, ctx.ipc_dir, "run1.2")
        e2 = InFlightJob("t::j2", j2.to_dict(), j2, [], h2, None)
        ctx.in_flight.track(e2, state=ctx.store)

        reaped = [
            (h1, ExecutionResult(success=True, result_meta={})),
            (h2, ExecutionResult(success=False, result_meta={"error": "fatal"})),
        ]

        count = completion.settle_reaped(reaped)
        assert count == 2
        assert "t::j1" in ctx.store.wall
        assert "t::j2" in ctx.store.failed
        assert "t::j1" not in ctx.in_flight
        assert "t::j2" not in ctx.in_flight
        assert ctx.store.in_flight_uids == frozenset()

    def test_settle_aborted_classification(self, tmp_path):
        ctx = _make_context(tmp_path)
        completion = CompletionMachine(ctx)

        j1 = Job("t", "j1", {})
        e1 = InFlightJob("t::j1", j1.to_dict(), j1, [], None, None)
        ctx.in_flight.track(e1, state=ctx.store)

        j2 = Job("t", "j2", {})
        e2 = InFlightJob("t::j2", j2.to_dict(), j2, [], None, None)
        ctx.in_flight.track(e2, state=ctx.store)

        cancelled = [e1]
        done = [(e2, ExecutionResult(success=True, result_meta={}))]

        completion.settle_aborted(cancelled, done)

        # e1 requeued to front of queue
        assert len(ctx.store.queue) == 1
        assert ctx.store.queue[0]["job_id"] == "j1"
        # e2 committed to wall
        assert "t::j2" in ctx.store.wall
        # both cleared from in_flight
        assert len(ctx.in_flight) == 0
        assert ctx.store.in_flight_uids == frozenset()

    def test_restore_stale_result_creates_pseudo_and_settles(self, tmp_path):
        ctx = _make_context(tmp_path)
        completion = CompletionMachine(ctx)

        job = Job("t", "stale_job", {})
        channel = ctx.channel
        channel.consume_stale_result = lambda uid, j: ExecutionResult(
            success=True, result_meta={"restored": True}
        )

        restored = completion.restore_stale_result("t::stale_job", job, job.to_dict())
        assert restored is True
        assert "t::stale_job" in ctx.store.wall
        assert ctx.store.wall["t::stale_job"]["restored"] is True
