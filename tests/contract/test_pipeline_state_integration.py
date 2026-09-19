"""Integration tests for the mutable PipelineState refactor.

Verifies the no-update-on-False contract and the re-insertion contracts:
  A) commit_job_success returns False  -> wall NOT updated, job re-queued.
  B) executor.execute raises           -> job re-inserted, NOT in failed.
  C) executor.execute raises KbdInt    -> job re-inserted at front, saved to disk.
  D) unknown resource                  -> job marked RESOURCE_DEADLOCK in failed
                                           (not silently dropped).
"""

import pytest

from tasklite.exceptions import _CommitCrashSignal
from tasklite.models.job import Job
from tasklite.models.state import PipelineState
from tests.helpers import (
    _write_fake_result,
    FakeManager,
    make_fake_process_class,
    make_pipeline,
    ok_handler,
    patch_multiprocessing_for_fakes,
    true_handler,
)

def _patch_mp(monkeypatch, process_class=None):
    """Patch multiprocessing Process/Queue/Manager for in-process tests."""
    if process_class is not None:
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=process_class)
    monkeypatch.setattr("multiprocessing.Manager", lambda: FakeManager())


class TestPipelineStateCommitContract:
    """Verify the only-update-on-committed-True contract (Task 4-6 refactor)."""

    def test_pipeline_does_not_update_state_when_commit_fails(self, tmp_path, monkeypatch):
        """Test A: commit_job_success returns False -> pipeline crashes (crash-only).

        Wall NOT updated, job re-queued. Cursor updates and spawned jobs
        from the handler must also be rolled back (post_pop_state used).
        """
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", ok_handler)
        pipeline.enqueue([Job("test", "j1", payload={})])

        class SpawningFakeProcess:
            def __init__(self, target=None, args=(), kwargs=None, **_kw):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                if self.args:
                    _write_fake_result(self.args[0].ipc_dir, self.args[0].job.uid, {
                        "status": "success",
                        "raw_result": True,
                        "new_jobs": [{"task_type": "test", "job_id": "spawned"}],
                        "resource_suspensions": [],
                        "cursor_updates": {"cursor_key": "cursor_value"},
                    }, incarnation=self.args[0].incarnation,
                       auth_token=getattr(self.args[0], "result_token", None))

            def join(self, timeout=None):
                self._alive = False

            def is_alive(self):
                return self._alive

            def kill(self):
                self._alive = False

        _patch_mp(monkeypatch, SpawningFakeProcess)

        # commit_job_success always returns False → crash-only
        def fake_commit(*args, **kwargs):
            return False

        monkeypatch.setattr(pipeline.backend, "commit_job_success", fake_commit)

        with pytest.raises(_CommitCrashSignal, match="Backend commit returned False"):
            pipeline.run()

        # Wall must NOT contain the job (commit returned False)
        assert "test::j1" not in pipeline._runtime.state.wall, (
            "wall must not be updated when commit_job_success returns False"
        )
        # Job must be re-queued in the in-memory state
        uids = [Job.from_dict(j).uid for j in pipeline._runtime.state.queue]
        assert set(uids) == {"test::j1"}, (
            "job must be re-queued in pipeline._runtime.state.queue when commit fails"
        )
        # Spawned job must NOT leak into the queue on commit failure
        assert "test::spawned" not in uids, (
            "spawned job must not persist when commit_job_success returns False"
        )
        # Cursor updates must NOT be persisted
        assert pipeline.backend.load_cursors() == {}, (
            "cursor updates must not persist when commit_job_success returns False"
        )
        # Cross-state consistency: in-memory queue must match on-disk queue
        assert pipeline._runtime.state.queue == pipeline.backend.load_queue(), (
            "in-memory queue must match on-disk queue after commit failure"
        )

    def test_pipeline_reinserts_job_on_commit_job_failure_false(self, tmp_path, monkeypatch):
        """commit_job_failure returns False -> pipeline crashes (crash-only),
        job re-queued at front, not in DLQ."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", ok_handler)
        pipeline.enqueue([Job("test", "j1", payload={})])

        _patch_mp(monkeypatch, make_fake_process_class("error"))

        def fake_commit(*args, **kwargs):
            return False

        monkeypatch.setattr(pipeline.backend, "commit_job_failure", fake_commit)

        with pytest.raises(_CommitCrashSignal, match="Backend commit returned False"):
            pipeline.run()

        assert "test::j1" not in pipeline._runtime.state.failed, (
            "job must not be in failed when commit_job_failure returns False"
        )
        uids = [Job.from_dict(j).uid for j in pipeline._runtime.state.queue]
        assert uids == ["test::j1"], f"job must be re-queued at front, got: {uids}"
        assert pipeline._runtime.state.queue == pipeline.backend.load_queue()

    def test_pipeline_reinserts_job_on_executor_exception(self, tmp_path, monkeypatch):
        """Test B: executor.submit raises RuntimeError -> job re-inserted, NOT in failed.

        An unexpected exception from the executor (not a handler-level failure,
        which is caught by the executor and returned as a failure result) must
        re-insert the job and propagate the exception.
        """
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", ok_handler)
        pipeline.enqueue([Job("test", "j1", payload={})])

        _patch_mp(monkeypatch)  # FakeManager so manager.list() doesn't fork

        def raising_submit(*args, **kwargs):
            raise RuntimeError("executor crashed")

        monkeypatch.setattr(pipeline._runtime.channel, "spawn", raising_submit)

        with pytest.raises(RuntimeError, match="executor crashed"):
            pipeline.run()

        # Job must be re-inserted in queue (full-set: only j1, nothing else)
        uids = [Job.from_dict(j).uid for j in pipeline._runtime.state.queue]
        assert set(uids) == {"test::j1"}, (
            "job must be re-inserted in queue when executor raises"
        )
        # Cross-state consistency: in-memory queue must match on-disk queue
        assert pipeline._runtime.state.queue == pipeline.backend.load_queue(), (
            "in-memory queue must match on-disk queue after executor exception"
        )
        # Job must NOT be in failed (exception path, not failure path)
        assert "test::j1" not in pipeline._runtime.state.failed, (
            "job must not be marked as failed when executor raises an exception"
        )

    def test_pipeline_reinserts_job_on_keyboard_interrupt(self, tmp_path, monkeypatch):
        """Test C: KeyboardInterrupt -> job re-inserted at front, queue saved to disk.

        Two jobs are enqueued; the first triggers KeyboardInterrupt during submit.
        The popped job must be re-inserted at the FRONT of the queue (before j2)
        and the queue must be persisted to disk.
        """
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", ok_handler)
        pipeline.enqueue([
            Job("test", "j1", payload={}),
            Job("test", "j2", payload={}),
        ])

        _patch_mp(monkeypatch)

        def interrupt_submit(*args, **kwargs):
            raise KeyboardInterrupt()

        monkeypatch.setattr(pipeline._runtime.channel, "spawn", interrupt_submit)

        with pytest.raises(KeyboardInterrupt):
            pipeline.run()

        # Queue must be saved to disk with j1 re-inserted at front
        # Full-set: both j1 and j2 must remain (j1 at front, j2 after)
        queue = pipeline.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in queue]
        assert uids[0] == "test::j1", (
            f"j1 must be re-inserted at front of queue, got {uids}"
        )
        assert set(uids) == {"test::j1", "test::j2"}, (
            f"both j1 and j2 must remain in queue, got {uids}"
        )
        # Cross-state consistency: in-memory queue must match on-disk queue
        assert pipeline._runtime.state.queue == pipeline.backend.load_queue(), (
            "in-memory queue must match on-disk queue after KeyboardInterrupt"
        )

    def test_pipeline_marks_resource_deadlock(self, tmp_path, monkeypatch):
        """Test D: unknown resource -> job marked RESOURCE_DEADLOCK in failed.

        A job requiring a resource not registered with the pipeline triggers a
        resource deadlock. The job must be marked as failed with error
        RESOURCE_DEADLOCK (not silently dropped or left in the queue forever).
        """
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", ok_handler)
        pipeline.enqueue([Job("test", "j1", payload={}, resources={"nonexistent": 1.0})])

        # No FakeProcess needed — handler is never called (deadlock detected first).
        # Use FakeManager to avoid spawning a real manager subprocess.
        _patch_mp(monkeypatch)

        pipeline.run()

        # Job must be in failed with RESOURCE_DEADLOCK error
        assert "test::j1" in pipeline._runtime.state.failed, (
            "job requiring unknown resource must be marked as failed, not silently dropped"
        )
        assert pipeline._runtime.state.failed["test::j1"]["error"] == "RESOURCE_DEADLOCK", (
            f"error must be RESOURCE_DEADLOCK, got {pipeline._runtime.state.failed['test::j1']}"
        )
        # Queue must be empty (cleared by commit_bulk_failure)
        assert pipeline._runtime.state.queue == [], (
            "queue must be emptied after resource deadlock"
        )
        # Also verify on-disk state matches in-memory state
        failed = pipeline.backend.load_failed()
        assert "test::j1" in failed


class TestInFlightStateConsistency:
    """in-flight 集合纳入 state，is_known 统一谓词。"""

    def test_is_known_covers_all_four_sources(self):
        """is_known 必须涵盖 wall/failed/queue/in-flight 四者（统一事实源）。"""
        st = PipelineState({"w1": {}}, {"f1": {}}, {}, [{"task_type": "t", "job_id": "q1"}])
        st.register_in_flight("i1")
        assert st.is_known("w1")
        assert st.is_known("f1")
        assert st.is_known("t::q1")
        assert st.is_known("i1")
        assert not st.is_known("ghost")

    def test_register_unregister_in_flight(self):
        """register/unregister 维护 in-flight 集合。"""
        st = PipelineState({}, {}, {}, [])
        st.register_in_flight("a")
        assert "a" in st.in_flight_uids
        st.unregister_in_flight("a")
        assert "a" not in st.in_flight_uids

    def test_spawn_dedup_uses_in_flight(self, tmp_path):
        """ 核心回归：C 已派发（in-flight、不在队列）后，同 uid 再 spawn 必须被拒。

        状态判定需全量覆盖已见集合与运行中集合：C 被 A 完成时 spawn、
        随即被派发（从队列移除进入 in-flight）后，B 完成再 spawn 同 uid C 会被
        误放行 → C 双重执行。旧测试在测试内**重实现**生产
        公式（tautology，断言自己算出的结果）——改为走真实 _apply_result 的
        spawn 去重路径，断言 C 不被重复入队。
        """
        from tasklite.engine.channel import ExecutionResult
        from tasklite.models.job import inject_worker_resource

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("parent", true_handler)
        pipeline.register_handler("child", true_handler)
        pipeline.enqueue([Job("child", "c", payload={})])
        state = PipelineState(
            pipeline.backend.load_wall(), pipeline.backend.load_failed(),
            pipeline.backend.load_cursors(), pipeline.backend.load_queue(),
        )
        pipeline._runtime.store.set_state(state)
        # C 已被 A spawn 且派发：不在队列（pop 出队）、在 in-flight
        state.pop_job(0)
        state.register_in_flight("child::c")

        # B 完成时 spawn 同 uid C → 真实 _apply_result 去重路径必须拦截
        job = Job("parent", "b", payload={})
        job_dict = job.to_dict()
        inject_worker_resource(job_dict)
        result = ExecutionResult(success=True, new_jobs=[Job("child", "c", payload={})])
        pipeline._runtime._completion.apply_result("parent::b", job, job_dict, result, expect_in_flight=False)

        # C 未被重复入队（in-flight 命中拦截，spawn 的 C 未新增）：
        # 磁盘 queue 仍只有最初 enqueue 的那 1 条 C（pop_job 只改内存，
        # 磁盘 C 从未被 commit 删除）；B 正常进 wall。
        q = pipeline.backend.load_queue()
        c_count = sum(1 for jd in q
                      if f"{jd['task_type']}::{jd['job_id']}" == "child::c")
        assert c_count == 1, f"in-flight 的 C 不得被重复 spawn 入队（应仍 1 条）: {q}"
        wall = pipeline.backend.load_wall()
        assert "parent::b" in wall

    def test_queue_and_in_flight_mutually_exclusive(self):
        """DEBUG 不变式：同一 uid 不可能同时在队列与 in-flight（派发即出队）。"""
        st = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "x"}])
        # 派发：pop 出队 → 登记 in-flight → 二者互斥
        st.pop_job(0)
        st.register_in_flight("t::x")
        assert "t::x" not in st._queue_uids
        assert "t::x" in st._in_flight_uids
