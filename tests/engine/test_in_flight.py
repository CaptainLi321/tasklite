"""v2 在途作业追踪器测试：登记/结算、句柄提取与资源安全释放。"""

from __future__ import annotations

from tasklite.engine.in_flight import InFlightJob, InFlightTracker
from tasklite.engine.resource import (
    CapacityResource,
    NullResourceLease,
    ResourceManager,
)
from tasklite.engine.types import JobHandle
from tasklite.models.job import Job
from tasklite.models.state import PipelineState


class FakeProcess:
    def __init__(self, pid: int = 12345):
        self.pid = pid


class TestInFlightTrackerBasicMapping:
    def test_dict_like_operations(self):
        tracker = InFlightTracker()
        job = Job("t", "j1")
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

    def test_get_and_pop_defaults(self):
        tracker = InFlightTracker()
        job = Job("t", "j1")
        entry = InFlightJob("t::j1", job.to_dict(), job)
        tracker.track(entry)
        assert tracker.get("t::missing") is None
        assert tracker.pop("t::j1") is entry
        assert tracker.pop("t::j1") is None


class TestInFlightTrackerLifecycleAndStateSync:
    def test_track_and_settle_syncs_with_pipeline_state(self):
        tracker = InFlightTracker()
        state = PipelineState(wall={}, failed={}, cursors={}, queue=[])

        job = Job("t", "j1")
        entry = InFlightJob("t::j1", job.to_dict(), job, [("gpu", 1.0)], None, 100.0)

        tracker.track(entry, state=state)
        assert "t::j1" in tracker
        assert "t::j1" in state.in_flight_uids

        settled = tracker.settle("t::j1", state=state)
        assert settled is entry
        assert "t::j1" not in tracker
        assert "t::j1" not in state.in_flight_uids

    def test_track_settle_semantic_seams(self):
        tracker = InFlightTracker()
        state = PipelineState(wall={}, failed={}, cursors={}, queue=[])

        job = Job("t", "j1")
        entry = InFlightJob("t::j1", job.to_dict(), job, [("gpu", 1.0)], None, 100.0)

        tracked = tracker.track(entry, state=state)
        assert tracked is entry
        assert "t::j1" in tracker
        assert "t::j1" in tracker.uids
        assert "t::j1" in state.in_flight_uids

        settled = tracker.settle("t::j1", state=state)
        assert settled is entry
        assert "t::j1" not in tracker
        assert "t::j1" not in tracker.uids
        assert "t::j1" not in state.in_flight_uids

    def test_single_source_of_truth_without_state_param(self):
        """InFlightTracker 自闭环作为在途状态单一真相源（无需显式透传 state）。"""
        tracker = InFlightTracker()
        job = Job("t", "j1")
        entry = InFlightJob("t::j1", job.to_dict(), job, [], None, None)

        tracker.track(entry)
        assert tracker.uids == frozenset(["t::j1"])
        assert "t::j1" in tracker

        settled = tracker.settle("t::j1")
        assert settled is entry
        assert tracker.uids == frozenset()
        assert "t::j1" not in tracker

    def test_settle_unknown_uid_is_noop(self):
        tracker = InFlightTracker()
        state = PipelineState(wall={}, failed={}, cursors={}, queue=[])
        assert tracker.settle("t::ghost", state=state) is None
        assert state.in_flight_uids == frozenset()

    def test_clear_drops_all_entries(self):
        tracker = InFlightTracker()
        job_a = Job("t", "ja")
        job_b = Job("t", "jb")
        tracker.track(InFlightJob("t::ja", job_a.to_dict(), job_a))
        tracker.track(InFlightJob("t::jb", job_b.to_dict(), job_b))
        tracker.clear()
        assert tracker.uids == frozenset()
        assert tracker.active_handles() == []


class TestInFlightTrackerHandlesAndPseudo:
    def test_active_handles_filters_pseudo_entries(self):
        tracker = InFlightTracker()
        job_real = Job("t", "j1")
        handle = JobHandle(
            uid="t::j1",
            process=FakeProcess(101),
            deadline=100.0,
            timeout=10.0,
            job=job_real,
            ipc_dir="/tmp/ipc",
            incarnation="run1.1",
        )
        entry_real = InFlightJob("t::j1", job_real.to_dict(), job_real, [], handle, None)

        job_ghost = Job("t", "j2")
        entry_ghost = InFlightTracker.create_pseudo_entry("t::j2", job_ghost.to_dict(), job_ghost)

        tracker.track(entry_real)
        tracker.track(entry_ghost)

        assert not entry_real.is_pseudo
        assert entry_ghost.is_pseudo
        assert tracker.active_handles() == [handle]

    def test_watchdog_fields_carried_on_handle(self):
        """看门狗字段（deadline/timeout/job_start）随条目透传，timeout 语义原样。"""
        job = Job("t", "jw")
        handle = JobHandle(
            uid="t::jw",
            process=FakeProcess(7),
            deadline=500.0,
            timeout=30.0,
            job=job,
            ipc_dir="/tmp/ipc",
            incarnation="run9.42",
        )
        entry = InFlightJob("t::jw", job.to_dict(), job, [], handle, 470.0)
        assert entry.handle is handle
        assert entry.handle.deadline - entry.job_start == 30.0


class TestInFlightTrackerResourceRelease:
    def test_release_all_resources_clears_used(self):
        gpu = CapacityResource("gpu", 10.0)
        gpu.acquire(4.0)
        rm = ResourceManager({"gpu": gpu})

        tracker = InFlightTracker()
        job_a = Job("t", "ja")
        entry_a = InFlightJob("t::ja", job_a.to_dict(), job_a, [("gpu", 2.0)], None, None)
        job_b = Job("t", "jb")
        entry_b = InFlightJob("t::jb", job_b.to_dict(), job_b, [("gpu", 2.0)], None, None)

        tracker.track(entry_a)
        tracker.track(entry_b)

        assert gpu.used == 4.0
        tracker.release_all_resources(rm)
        assert gpu.used == 0.0

    def test_entry_self_release_with_lease(self):
        gpu = CapacityResource("gpu", 10.0)
        rm = ResourceManager({"gpu": gpu})
        lease = rm.reserve("t", {"gpu": 3.0}, uid="t::j1")
        assert gpu.used == 3.0

        job = Job("t", "j1", {"gpu": 3.0})
        entry = InFlightJob(uid="t::j1", job_dict=job.to_dict(), job=job, lease=lease)
        assert entry.acquired == [("gpu", 3.0)]

        entry.release_resources(rm)
        assert gpu.used == 0.0
        assert entry.acquired == []

    def test_release_resources_idempotent(self):
        gpu = CapacityResource("gpu", 10.0)
        gpu.acquire(2.0)
        rm = ResourceManager({"gpu": gpu})
        job = Job("t", "j1")
        entry = InFlightJob("t::j1", job.to_dict(), job, [("gpu", 2.0)], None, None)
        entry.release_resources(rm)
        entry.release_resources(rm)
        assert gpu.used == 0.0
        assert gpu.release_overruns == 0, "二次释放不得记为超量（幂等归还）"

    def test_pseudo_entry_has_null_lease_and_safe_release(self):
        job = Job("t", "j1")
        pseudo = InFlightTracker.create_pseudo_entry("t::j1", job.to_dict(), job)
        assert isinstance(pseudo.lease, NullResourceLease)
        assert pseudo.lease.acquired == []
        assert pseudo.acquired == []
        # release / claim / cancel 均为安全 no-op
        pseudo.lease.claim()
        pseudo.lease.cancel()
        pseudo.release_resources()
        assert pseudo.acquired == []

    def test_active_uids_and_classify_aborted(self):
        tracker = InFlightTracker()
        job_a = Job("t", "ja")
        entry_a = InFlightJob("t::ja", job_a.to_dict(), job_a, [], None, None)
        job_b = Job("t", "jb")
        entry_b = InFlightJob("t::jb", job_b.to_dict(), job_b, [], None, None)

        tracker.track(entry_a)
        tracker.track(entry_b)

        assert set(tracker.active_uids()) == {"t::ja", "t::jb"}

        cancelled, done = tracker.classify_aborted({"t::ja": {"status": "ok"}})
        assert len(cancelled) == 1
        assert cancelled[0].uid == "t::jb"
        assert len(done) == 1
        assert done[0][0].uid == "t::ja"
        assert done[0][1] == {"status": "ok"}
