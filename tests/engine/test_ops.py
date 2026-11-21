"""图修整事务经 StateStore 单一出口的回归测试。"""

from __future__ import annotations

import pytest

from tasklite import TaskLiteError
from tasklite.backend.base import GraphEditCommit
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.edit import EditTransaction
from tasklite.engine.store import StateStore
from tasklite.models.job import Job


def _make_store() -> tuple[InMemoryStateBackend, StateStore]:
    backend = InMemoryStateBackend()
    return backend, StateStore(backend)


class TestEditTransactionThroughStore:
    """参数校验、目标选择、attempts 不可删与故障传播都集中在 EditTransaction + StateStore。"""

    def test_selected_entries_are_removed_without_other_side_effects(self):
        backend, store = _make_store()
        backend.enqueue_jobs([Job("t", "queued").to_dict()])
        backend.commit_graph_edit(
            GraphEditCommit(
                success_writes={"t::success": {"job_snapshot": {"task_type": "t", "job_id": "success"}}},
                failed_writes={"t::failure": ({"job_snapshot": {"task_type": "t", "job_id": "failure"}}, None)},
            )
        )
        snapshot = Job("t", "success").to_dict()
        backend.enqueue_jobs([snapshot])
        backend.mark_job_running(
            "t::success",
            execution_id="run.1",
            run_id="run",
            started_at="2026-01-01T00:00:00Z",
        )
        backend.commit_job_settlement(
            job_uid="t::success",
            outcome="success",
            attempt_no=1,
            execution_id="run.1",
            run_id="run",
            settled_at="2026-01-01T00:00:01Z",
            success_metadata={"job_snapshot": snapshot},
        )
        store.set_cursor("cursor", "value")

        with EditTransaction(store, dry_run=False) as tx:
            assert tx.remove("t::success", entries=("successes",), recursive=False) == 1
            assert tx.remove("t::failure", entries=("failures",), recursive=False) == 1
            with pytest.raises(TaskLiteError, match="unknown or forbidden"):
                tx.remove("t::success", entries=("attempts",), recursive=False)  # type: ignore[arg-type]

        assert backend.load_successes() == {}
        assert backend.load_failed() == {}
        assert len(backend.load_settled_attempts("t::success")) == 1
        assert backend.load_cursors() == {"cursor": "value"}
        assert [job["job_uid"] for job in backend.load_queue_jobs()] == ["t::queued"]

    def test_empty_and_unknown_targets_are_rejected_before_backend_write(self):
        backend, store = _make_store()
        before = backend.load_successes()
        with EditTransaction(store, dry_run=False) as tx:
            with pytest.raises(TaskLiteError, match="entries must be a collection"):
                tx.remove("t::a", entries="successes", recursive=False)  # type: ignore[arg-type]
            with pytest.raises(TaskLiteError, match="must not be empty"):
                tx.remove("t::a", entries=[], recursive=False)
            with pytest.raises(TaskLiteError, match="unknown or forbidden"):
                tx.remove("t::a", entries=["other"], recursive=False)  # type: ignore[list-item]
        assert backend.load_successes() == before

    def test_backend_failure_propagates_without_memory_cleanup(self, monkeypatch):
        backend, store = _make_store()
        store.apply_graph_edit(
            GraphEditCommit(
                success_writes={"t::a": {"job_snapshot": {"task_type": "t", "job_id": "a"}}},
            )
        )

        def fail_commit(commit: GraphEditCommit) -> None:
            raise OSError("history store unavailable")

        monkeypatch.setattr(backend, "commit_graph_edit", fail_commit)
        with pytest.raises(OSError, match="history store unavailable"):
            with EditTransaction(store, dry_run=False) as tx:
                tx.remove("t::a", entries=("successes",), recursive=False)

        assert "t::a" in backend.load_successes()
        assert "t::a" in store.state.successes
