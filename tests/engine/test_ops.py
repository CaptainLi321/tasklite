"""管理历史删除的 Store 单一出口回归。"""

from __future__ import annotations

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.store import StateStore
from tasklite.models.attempt import AttemptRecord
from tasklite.models.job import Job


def _make_store() -> tuple[InMemoryStateBackend, StateStore]:
    backend = InMemoryStateBackend()
    return backend, StateStore(backend)


class TestClearHistoryThroughStore:
    """参数校验、目标选择与故障传播都集中在 StateStore。"""

    def test_selected_records_are_deleted_without_other_side_effects(self):
        backend, store = _make_store()
        backend.enqueue_jobs([Job("t", "queued").to_dict()])
        backend.apply_manual_status([
            ("t::success", "succeeded", {"task_type": "t", "job_id": "success"}, None),
            ("t::failure", "failed", {"task_type": "t", "job_id": "failure"}, None),
        ])
        backend.append_attempt(AttemptRecord(
            job_uid="t::success",
            activation_no=1,
            attempt_no=1,
            execution_id="run.1",
            run_id="run",
            started_at="2026-01-01T00:00:00Z",
            outcome="running",
        ))
        store.set_cursor("cursor", "value")

        assert store.clear_history("t::success", ["successes"]) == 1
        assert store.clear_history("t::failure", ["failures"]) == 1
        assert store.clear_history("t::success", ["attempts"]) == 1

        assert backend.load_successes() == {}
        assert backend.load_failed() == {}
        assert backend.load_attempts("t::success") == []
        assert backend.load_cursors() == {"cursor": "value"}
        assert [job["job_uid"] for job in backend.load_queue_jobs()] == ["t::queued"]

    def test_empty_and_unknown_targets_are_rejected_before_backend_write(self):
        backend, store = _make_store()
        before = backend.load_successes()
        with pytest.raises(TypeError, match="records must be a collection"):
            store.clear_history("t::a", "successes")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="must not be empty"):
            store.clear_history("t::a", [])
        with pytest.raises(ValueError, match="unknown target"):
            store.clear_history("t::a", ["other"])
        assert backend.load_successes() == before

    def test_backend_failure_propagates_without_memory_cleanup(self, monkeypatch):
        backend, store = _make_store()
        backend.apply_manual_status([
            ("t::a", "succeeded", {"task_type": "t", "job_id": "a"}, None),
        ])
        store.set_state(store.state)

        def fail_clear(uid: str, records: set[str]) -> int:
            raise OSError("history store unavailable")

        monkeypatch.setattr(backend, "clear_history", fail_clear)
        with pytest.raises(OSError, match="history store unavailable"):
            store.clear_history("t::a", ["successes"])

        assert "t::a" in backend.load_successes()
