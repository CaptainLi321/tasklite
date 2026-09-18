"""CompletionMachine 完成与结算深模块单元测试套件。"""
from __future__ import annotations

import types

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.channel import ArtifactCleanupMode, ExecutionResult, JobHandle
from tests.machines import make_machines
from tasklite.engine.completion import CompletionMachine
from tasklite.engine.inflight import InFlightJob, InFlightTracker
from tasklite.engine.policy import ExecutionPolicy
from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.engine.scheduler import JobScheduler
from tasklite.engine.session import RunSession
from tasklite.engine.store import StateStore
from tasklite.engine.types import TaskStats
from tasklite.exceptions import _CommitCrashSignal, _JobTerminated
from tasklite.models.job import Job
from tasklite.models.state import PipelineState
from tasklite.taxonomy import ErrorTaxonomy


class FakeProcess:
    def __init__(self, pid=12345):
        self.pid = pid


class TestCompletionMachineBasicSettlement:
    def test_complete_job_success_lifecycle(self, tmp_path):
        from tasklite.engine.resource import CapacityResource
        env = make_machines(tmp_path, extra_resources={"gpu": CapacityResource("gpu", 10.0)})

        gpu = env.resources["gpu"]
        lease = env.resources.reserve("t", {"gpu": 2.0}, uid="t::j1")
        job = Job("t", "j1", {"gpu": 2.0})
        entry = InFlightJob(uid="t::j1", job_dict=job.to_dict(), job=job, lease=lease)
        env.in_flight.track(entry, state=env.store)

        assert gpu.used == 2.0
        assert "t::j1" in env.store.in_flight_uids

        result = ExecutionResult(
            success=True,
            result_meta={"score": 100},
        )

        completed_events = []
        env.session.on_job_completed = lambda uid, meta, success, retry: completed_events.append(
            (uid, meta, success, retry)
        )

        env.completion.complete_job(entry, result)

        assert gpu.used == 0.0
        assert "t::j1" in env.store.wall
        assert env.store.wall["t::j1"]["score"] == 100
        assert env.stats.completed == 1
        assert len(completed_events) == 1
        assert completed_events[0] == ("t::j1", {"score": 100}, True, False)

    def test_settle_reaped_batch(self, tmp_path):
        from tasklite.engine.resource import CapacityResource
        env = make_machines(tmp_path, extra_resources={"gpu": CapacityResource("gpu", 10.0)})

        j1 = Job("t", "j1", {})
        h1 = JobHandle("t::j1", FakeProcess(101), 100.0, 10.0, j1, env.ipc_dir, "run1.1")
        e1 = InFlightJob("t::j1", j1.to_dict(), j1, [], h1, None)
        env.in_flight.track(e1, state=env.store)

        j2 = Job("t", "j2", {})
        h2 = JobHandle("t::j2", FakeProcess(102), 100.0, 10.0, j2, env.ipc_dir, "run1.2")
        e2 = InFlightJob("t::j2", j2.to_dict(), j2, [], h2, None)
        env.in_flight.track(e2, state=env.store)

        reaped = [
            (h1, ExecutionResult(success=True, result_meta={})),
            (h2, ExecutionResult(success=False, result_meta={"error": "fatal"})),
        ]

        count = env.completion.settle_reaped(reaped)
        assert count == 2
        assert "t::j1" in env.store.wall
        assert "t::j2" in env.store.failed
        assert "t::j1" not in env.in_flight
        assert "t::j2" not in env.in_flight
        assert env.store.in_flight_uids == frozenset()

    def test_settle_aborted_classification(self, tmp_path):
        from tasklite.engine.resource import CapacityResource
        env = make_machines(tmp_path, extra_resources={"gpu": CapacityResource("gpu", 10.0)})

        j1 = Job("t", "j1", {})
        e1 = InFlightJob("t::j1", j1.to_dict(), j1, [], None, None)
        env.in_flight.track(e1, state=env.store)

        j2 = Job("t", "j2", {})
        e2 = InFlightJob("t::j2", j2.to_dict(), j2, [], None, None)
        env.in_flight.track(e2, state=env.store)

        cancelled = [e1]
        done = [(e2, ExecutionResult(success=True, result_meta={}))]

        env.completion.settle_aborted(cancelled, done)

        # e1 requeued to front of queue
        assert len(env.store.queue) == 1
        assert env.store.queue[0]["job_id"] == "j1"
        # e2 committed to wall
        assert "t::j2" in env.store.wall
        # both cleared from in_flight
        assert len(env.in_flight) == 0
        assert env.store.in_flight_uids == frozenset()

    def test_restore_stale_result_creates_pseudo_and_settles(self, tmp_path):
        from tasklite.engine.resource import CapacityResource
        env = make_machines(tmp_path, extra_resources={"gpu": CapacityResource("gpu", 10.0)})

        job = Job("t", "stale_job", {})
        channel = env.channel
        channel.claim_stale_result = lambda uid, j: ExecutionResult(
            success=True, result_meta={"restored": True}
        )

        restored = env.completion.restore_stale_result("t::stale_job", job, job.to_dict())
        assert restored is True
        assert "t::stale_job" in env.store.wall
        assert env.store.wall["t::stale_job"]["restored"] is True
