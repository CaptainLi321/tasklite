"""v2 在途作业追踪器测试：只读视图、状态同步写口、句柄提取与资源安全释放。"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping

import pytest

from tasklite.engine.in_flight import InFlightJob, InFlightTracker
from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.engine.types import JobHandle
from tasklite.models.job import Job
from tasklite.models.state import PipelineState


class FakeProcess:
    def __init__(self, pid: int = 12345):
        self.pid = pid


class TestInFlightTrackerReadOnlyMapping:
    def test_read_only_mapping_rejects_raw_mutations(self):
        tracker = InFlightTracker()
        assert isinstance(tracker, Mapping)
        assert not isinstance(tracker, MutableMapping)
        assert not hasattr(tracker, "pop")
        assert not hasattr(tracker, "clear")

        job = Job("t", "j1")
        entry = InFlightJob("t::j1", job.to_dict(), job, [], None, None)

        with pytest.raises(TypeError):
            tracker["t::j1"] = entry  # type: ignore[index]

        tracker.track(entry)
        assert "t::j1" in tracker
        assert len(tracker) == 1
        assert tracker["t::j1"] is entry
        assert tracker.get("t::missing") is None
        assert list(tracker.keys()) == ["t::j1"]
        assert list(tracker.values()) == [entry]

        with pytest.raises(TypeError):
            del tracker["t::j1"]  # type: ignore[attr-defined]


class TestInFlightTrackerLifecycleAndStateSync:
    def test_track_and_settle_syncs_with_pipeline_state(self):
        tracker = InFlightTracker()
        state = PipelineState(successes={}, failed={}, cursors={}, queue=[])

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
        state = PipelineState(successes={}, failed={}, cursors={}, queue=[])

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
        state = PipelineState(successes={}, failed={}, cursors={}, queue=[])
        assert tracker.settle("t::ghost", state=state) is None
        assert state.in_flight_uids == frozenset()

    def test_clear_all_drops_all_entries_and_syncs_state(self):
        tracker = InFlightTracker()
        state = PipelineState(successes={}, failed={}, cursors={}, queue=[])
        job_a = Job("t", "ja")
        job_b = Job("t", "jb")
        tracker.track(InFlightJob("t::ja", job_a.to_dict(), job_a), state=state)
        tracker.track(InFlightJob("t::jb", job_b.to_dict(), job_b), state=state)
        assert state.in_flight_uids == frozenset({"t::ja", "t::jb"})

        tracker.clear_all(state=state)
        assert tracker.uids == frozenset()
        assert tracker.active_handles() == []
        assert state.in_flight_uids == frozenset()


class TestInFlightTrackerHandles:
    def test_active_handles_filters_entries_without_worker_handle(self):
        tracker = InFlightTracker()
        job_real = Job("t", "j1")
        handle = JobHandle(
            job_uid="t::j1",
            process=FakeProcess(101),
            timeout_at_monotonic=100.0,
            timeout_seconds=10.0,
            job=job_real,
            execution_id="run1.1",
        )
        entry_real = InFlightJob("t::j1", job_real.to_dict(), job_real, [], handle, None)

        job_no_worker = Job("t", "j2")
        entry_no_worker = InFlightJob("t::j2", job_no_worker.to_dict(), job_no_worker)

        tracker.track(entry_real)
        tracker.track(entry_no_worker)

        assert tracker.active_handles() == [handle]
        assert set(tracker.active_uids()) == {"t::j1", "t::j2"}

    def test_watchdog_fields_carried_on_handle(self):
        """看门狗字段（timeout_at_monotonic/timeout/started_monotonic）随条目透传。"""
        job = Job("t", "jw")
        handle = JobHandle(
            job_uid="t::jw",
            process=FakeProcess(7),
            timeout_at_monotonic=500.0,
            timeout_seconds=30.0,
            job=job,
            execution_id="run9.42",
        )
        entry = InFlightJob("t::jw", job.to_dict(), job, [], handle, 470.0)
        assert entry.handle is handle
        assert entry.handle.timeout_at_monotonic - entry.started_monotonic == 30.0


class TestInFlightTrackerResourceRelease:
    def test_release_all_resources_clears_used(self):
        gpu = CapacityResource("gpu", max_capacity=10.0)
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
        gpu = CapacityResource("gpu", max_capacity=10.0)
        rm = ResourceManager({"gpu": gpu})
        acquired = rm.acquire_all("t", {"gpu": 3.0})
        assert gpu.used == 3.0

        job = Job("t", "j1", payload={"gpu": 3.0})
        entry = InFlightJob(job_uid="t::j1", job_snapshot=job.to_dict(), job=job, acquired=acquired)
        assert entry.acquired == [("gpu", 3.0)]

        entry.release_resources(rm)
        assert gpu.used == 0.0
        assert entry.acquired == []

    def test_release_resources_idempotent(self):
        gpu = CapacityResource("gpu", max_capacity=10.0)
        gpu.acquire(2.0)
        rm = ResourceManager({"gpu": gpu})
        job = Job("t", "j1")
        entry = InFlightJob("t::j1", job.to_dict(), job, [("gpu", 2.0)], None, None)
        entry.release_resources(rm)
        entry.release_resources(rm)
        assert gpu.used == 0.0
        assert gpu.release_overruns == 0, "二次释放不得记为超量（幂等归还）"
