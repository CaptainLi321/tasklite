"""InFlightTracker 在途任务生命周期跟踪深模块单元测试套件。"""

import pytest

from tasklite.engine.channel import JobHandle
from tasklite.engine.inflight import InFlightJob, InFlightTracker
from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.models.job import Job
from tasklite.models.state import PipelineState


class FakeProcess:
    def __init__(self, pid=12345):
        self.pid = pid


class TestInFlightTrackerBasicMapping:
    def test_dict_like_operations(self):
        tracker = InFlightTracker()
        job = Job("t", "j1", {})
        entry = InFlightJob("t::j1", job.to_dict(), job, [], None, None)

        tracker["t::j1"] = entry
        assert "t::j1" in tracker
        assert len(tracker) == 1
        assert tracker["t::j1"] is entry
        assert list(tracker.keys()) == ["t::j1"]
        assert list(tracker.values()) == [entry]

        del tracker["t::j1"]
        assert "t::j1" not in tracker
        assert len(tracker) == 0


class TestInFlightTrackerLifecycleAndStateSync:
    def test_register_and_unregister_syncs_with_pipeline_state(self):
        tracker = InFlightTracker()
        state = PipelineState(wall={}, failed={}, cursors={}, queue=[])

        job = Job("t", "j1", {})
        entry = InFlightJob("t::j1", job.to_dict(), job, [("gpu", 1.0)], None, 100.0)

        tracker.register(entry, state=state)
        assert "t::j1" in tracker
        assert "t::j1" in state.in_flight_uids

        unregistered = tracker.unregister("t::j1", state=state)
        assert unregistered is entry
        assert "t::j1" not in tracker
        assert "t::j1" not in state.in_flight_uids

    def test_dispatch_and_settle_semantic_seams(self):
        tracker = InFlightTracker()
        state = PipelineState(wall={}, failed={}, cursors={}, queue=[])

        job = Job("t", "j1", {})
        entry = InFlightJob("t::j1", job.to_dict(), job, [("gpu", 1.0)], None, 100.0)

        dispatched = tracker.dispatch(entry, state=state)
        assert dispatched is entry
        assert "t::j1" in tracker
        assert "t::j1" in state.in_flight_uids

        settled = tracker.settle("t::j1", state=state)
        assert settled is entry
        assert "t::j1" not in tracker
        assert "t::j1" not in state.in_flight_uids


class TestInFlightTrackerHandlesAndPseudo:
    def test_active_handles_filters_pseudo_entries(self):
        tracker = InFlightTracker()
        j1 = Job("t", "j1", {})
        h1 = JobHandle(
            uid="t::j1",
            process=FakeProcess(101),
            deadline=100.0,
            timeout=10.0,
            job=j1,
            ipc_dir="/tmp/ipc",
            incarnation="run1.1",
        )
        e1 = InFlightJob("t::j1", j1.to_dict(), j1, [], h1, None)

        j2 = Job("t", "j2", {})
        e2 = InFlightTracker.create_pseudo_entry("t::j2", j2.to_dict(), j2)

        tracker.register(e1)
        tracker.register(e2)

        assert not e1.is_pseudo
        assert e2.is_pseudo
        assert tracker.active_handles() == [h1]


class TestInFlightTrackerResourceRelease:
    def test_release_all_acquired_clears_resources(self):
        gpu = CapacityResource("gpu", 10.0)
        gpu.acquire(4.0)
        rm = ResourceManager({"gpu": gpu})

        tracker = InFlightTracker()
        j1 = Job("t", "j1", {})
        e1 = InFlightJob("t::j1", j1.to_dict(), j1, [("gpu", 2.0)], None, None)
        j2 = Job("t", "j2", {})
        e2 = InFlightJob("t::j2", j2.to_dict(), j2, [("gpu", 2.0)], None, None)

        tracker.register(e1)
        tracker.register(e2)

        assert gpu.used == 4.0
        tracker.release_all_acquired(rm)
        assert gpu.used == 0.0

    def test_entry_self_release_with_lease(self):
        gpu = CapacityResource("gpu", 10.0)
        rm = ResourceManager({"gpu": gpu})
        lease = rm.reserve("t", {"gpu": 3.0}, uid="t::j1")
        assert gpu.used == 3.0

        j1 = Job("t", "j1", {"gpu": 3.0})
        entry = InFlightJob(uid="t::j1", job_dict=j1.to_dict(), job=j1, lease=lease)
        assert entry.acquired == [("gpu", 3.0)]

        entry.release_resources(rm)
        assert gpu.used == 0.0
        assert entry.acquired == []

    def test_active_uids_and_classify_aborted(self):
        tracker = InFlightTracker()
        j1 = Job("t", "j1", {})
        e1 = InFlightJob("t::j1", j1.to_dict(), j1, [], None, None)
        j2 = Job("t", "j2", {})
        e2 = InFlightJob("t::j2", j2.to_dict(), j2, [], None, None)

        tracker.register(e1)
        tracker.register(e2)

        assert set(tracker.active_uids()) == {"t::j1", "t::j2"}

        cancelled, done = tracker.classify_aborted({"t::j1": {"status": "ok"}})
        assert len(cancelled) == 1
        assert cancelled[0].uid == "t::j2"
        assert len(done) == 1
        assert done[0][0].uid == "t::j1"
        assert done[0][1] == {"status": "ok"}

