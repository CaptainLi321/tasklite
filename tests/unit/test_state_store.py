"""Unit tests for StateStore deep module."""

from __future__ import annotations

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.store import (
    BulkFailureOutcome,
    FailureOutcome,
    RetryOutcome,
    SkipOutcome,
    StateStore,
    SuccessOutcome,
)
from tasklite.taxonomy import ERR_COMMIT_FAILURE_DLQ, ERR_JOB_DEPENDENCY
from tasklite.exceptions import _CommitCrashSignal, _JobTerminated
from tasklite.models.job import Job
from tasklite.models.state import PipelineState


class TestStateStoreSuccess:
    """测试 apply_success 原子状态转移与不变式。"""

    def test_apply_success_basic(self):
        backend = InMemoryStateBackend()
        backend.save_queue([{"task_type": "process", "job_id": "1"}])
        state = PipelineState({}, {}, {}, [{"task_type": "process", "job_id": "1"}])
        store = StateStore(backend, state)

        # 派发标记 in-flight
        state.pop_job(0)
        state.register_in_flight("process::1")

        outcome = store.apply_success(
            "process::1",
            {"score": 100},
            run_id="run_123",
            declared_inputs=[{"path": "/tmp/a.txt", "fingerprint": "abc"}],
        )

        assert isinstance(outcome, SuccessOutcome)
        assert outcome.uid == "process::1"
        assert "process::1" in store.wall
        assert store.wall["process::1"]["score"] == 100
        assert store.wall["process::1"]["last_run_id"] == "run_123"
        assert store.wall["process::1"]["run_count"] == 1
        assert "process::1" not in store.in_flight_uids
        assert store.is_empty is True

    def test_apply_success_with_spawn_and_cursor(self):
        backend = InMemoryStateBackend()
        state = PipelineState({}, {}, {}, [])
        store = StateStore(backend, state)

        store.register_in_flight("root::1")
        spawned = [{"task_type": "child", "job_id": "c1"}, {"task_type": "child", "job_id": "c2"}]

        outcome = store.apply_success(
            "root::1",
            {},
            spawned_jobs=spawned,
            cursor_updates={"batch_pos": "100", "old_cursor": None},
        )

        assert len(outcome.spawned_uids) == 2
        assert store.cursors["batch_pos"] == "100"
        assert "old_cursor" not in store.cursors
        assert len(store.queue) == 2
        assert "child::c1" in store.queue_uids
        assert "child::c2" in store.queue_uids


class TestStateStoreFailureAndCascade:
    """测试 apply_failure 与依赖级联。"""

    def test_apply_failure_with_cascade(self):
        backend = InMemoryStateBackend()
        # queue 中有依赖 parent::1 的下游
        child_job = Job("child", "1", depends_on=["parent::1"]).to_dict()
        grandchild_job = Job("grandchild", "1", depends_on=["child::1"]).to_dict()
        queue = [{"task_type": "parent", "job_id": "1"}, child_job, grandchild_job]

        state = PipelineState({}, {}, {}, queue)
        store = StateStore(backend, state)

        state.pop_job(0)
        state.register_in_flight("parent::1")

        outcome = store.apply_failure(
            "parent::1",
            {"error": "crash"},
            job_dict={"task_type": "parent", "job_id": "1"},
            cascade=True,
        )

        assert isinstance(outcome, FailureOutcome)
        assert outcome.uid == "parent::1"
        assert set(outcome.cascaded_uids) == {"child::1", "grandchild::1"}
        assert "parent::1" in store.failed
        assert "child::1" in store.failed
        assert "grandchild::1" in store.failed
        assert "parent::1" not in store.in_flight_uids
        assert "parent::1" not in store.wall


class TestStateStoreCommitCrashContract:
    """测试 commit 失败阈值与 DLQ 熔断契约。"""

    class FailingBackend(InMemoryStateBackend):
        def commit_job_success(self, uid, wall_meta, *, spawned_jobs=None, cursor_updates=None):
            return False

        def commit_job_failure(self, uid, failed_meta):
            if failed_meta.get("error", "").startswith(ERR_COMMIT_FAILURE_DLQ):
                return True  # DLQ 熔断写入成功
            return False

    def test_commit_failure_threshold_retry_then_dlq(self):
        backend = self.FailingBackend()
        state = PipelineState({}, {}, {}, [])
        store = StateStore(backend, state, commit_failure_dlq_threshold=3)

        job_dict = {"task_type": "task", "job_id": "1"}

        # 第 1 次 commit 失败：requeue 并抛 _CommitCrashSignal
        store.register_in_flight("task::1")
        with pytest.raises(_CommitCrashSignal, match="_commit_failures=1/3"):
            store.apply_success("task::1", {}, job_dict=job_dict)
        assert job_dict["_commit_failures"] == 1
        assert "task::1" in store.queue_uids

        # 第 2 次 commit 失败：requeue 并抛 _CommitCrashSignal
        store.pop_job(0)
        store.register_in_flight("task::1")
        with pytest.raises(_CommitCrashSignal, match="_commit_failures=2/3"):
            store.apply_success("task::1", {}, job_dict=job_dict)
        assert job_dict["_commit_failures"] == 2

        # 第 3 次 commit 失败：达到阈值 3，转入 DLQ 并抛 _JobTerminated
        store.pop_job(0)
        store.register_in_flight("task::1")
        with pytest.raises(_JobTerminated, match="permanently failed"):
            store.apply_success("task::1", {}, job_dict=job_dict)
        assert "task::1" in store.failed
        assert store.failed["task::1"]["fatal"] is True


class TestStateStoreBulkFailure:
    """测试 apply_bulk_failure 与死锁批量归因。"""

    def test_apply_bulk_failure(self):
        backend = InMemoryStateBackend()
        queue = [{"task_type": "cycle", "job_id": "1"}, {"task_type": "cycle", "job_id": "2"}, {"task_type": "safe", "job_id": "1"}]
        state = PipelineState({}, {}, {}, queue)
        store = StateStore(backend, state)

        uids_metas = [
            ("cycle::1", {"error": "DEPENDENCY_DEADLOCK"}),
            ("cycle::2", {"error": "DEPENDENCY_DEADLOCK"}),
        ]
        remaining = [{"task_type": "safe", "job_id": "1"}]

        outcome = store.apply_bulk_failure(uids_metas, remaining_queue=remaining)
        assert isinstance(outcome, BulkFailureOutcome)
        assert len(outcome.failed_uids) == 2
        assert outcome.remaining_queue_count == 1
        assert "cycle::1" in store.failed
        assert "cycle::2" in store.failed
        assert "safe::1" in store.queue_uids


class TestStateStoreQueueAndMembership:
    """测试 StateStore 对 queue/wall/failed/in_flight 与成员判定的统合接缝。"""

    def test_queue_and_membership_delegation(self):
        backend = InMemoryStateBackend()
        state = PipelineState(wall={"w::1": {}}, failed={"f::1": {}}, cursors={"c": "v"}, queue=[{"task_type": "q", "job_id": "1"}])
        store = StateStore(backend, state)

        assert store.is_completed("w::1") is True
        assert store.is_failed("f::1") is True
        assert store.is_known("w::1") is True
        assert store.is_known("q::1") is True
        assert store.is_known("unknown::1") is False

        assert "w::1" in store.wall_uids
        assert "f::1" in store.failed_uids
        assert "q::1" in store.queue_uids

        # pop_job
        popped = store.pop_job(0)
        assert popped["job_id"] == "1"
        assert "q::1" not in store.queue_uids

        # requeue_jobs
        store.requeue_jobs([{"task_type": "q", "job_id": "2"}], front=True)
        assert "q::2" in store.queue_uids

        # pop and register in_flight
        store.pop_job(0)
        store.register_in_flight("q::2")
        assert "q::2" in store.in_flight_uids
        store.unregister_in_flight("q::2")
        assert "q::2" not in store.in_flight_uids
