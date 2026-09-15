"""Pipeline backend integration tests: SQLite lifecycle + backoff persistence."""

import inspect
import multiprocessing
import sqlite3
import time
from unittest import mock

import pytest
from tasklite.engine.runtime import StopMode
from tasklite.pipeline import TaskLite
from tasklite.models.job import Job
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tests.helpers import _write_fake_result, make_fake_process_class, make_ipc_process_class, patch_multiprocessing_for_fakes


class CursorSetterFakeProcess:
    """Simulates a subprocess that calls ``ctx.set_cursor("high_water", "999")``
    by placing the corresponding cursor_updates in the IPC result.

    Shared by the JSON and SQLite cursor-persistence end-to-end tests.
    """

    def __init__(self, target=None, args=(), kwargs=None, **_kw):
        self.args = args
        self._alive = False
        self.exitcode = 0

    def start(self):
        self._alive = True
        if self.args:
            spec = self.args[0]
            _write_fake_result(spec.ipc_dir, spec.job.uid, {
                "status": "success",
                "raw_result": True,
                "new_jobs": [],
                "resource_suspensions": [],
                "cursor_updates": {"high_water": "999"},
            }, incarnation=spec.incarnation, auth_token=getattr(spec, "result_token", None))

    def join(self, timeout=None):
        self._alive = False

    def is_alive(self):
        return self._alive

    def kill(self):
        self._alive = False


class TestBackoffPersistence:
    """REQ-11.4: backoff 状态跨重启持久化（wall_deadline 格式）。"""

    def test_backoff_survives_pipeline_restart(self, tmp_path, monkeypatch):
        """带 wall_deadline 的退避 job 在重启后仍被跳过（语义）。"""
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "test_backoff_persist_state.db"

        # 持久化格式为 wall-clock 绝对截止 _backoff_wall_deadline
        # （monotonic _backoff_until 仅内存使用，_run_body 加载时换算）。
        import time as _time
        future_wall = _time.time() + 3600
        job = Job("test", "j1", payload={}, retries=1, max_retries=5)
        job_dict = job.to_dict()
        job_dict["runtime"]["_backoff_wall_deadline"] = future_wall

        # Save directly to DB, then create new pipeline pointing to same DB
        from tasklite.backend.sqlite_backend import SQLiteStateBackend
        backend = SQLiteStateBackend(db_path)
        backend.save_queue([job_dict])

        pipeline = TaskLite(
            name="test_backoff_persist", state_dir=state_dir, backend="sqlite"
        )
        pipeline.register_handler("test", lambda j, c: (True, {}))

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        # Finite backoff makes the scheduler sleep ~1M s waiting for
        # backoff to expire. Short-circuit: request a clean stop on the first
        # sleep so the loop exits while preserving the backoff job in the queue.
        def _stop_on_sleep(_s):
            pipeline.stop()
        monkeypatch.setattr("tasklite.pipeline.time.sleep", _stop_on_sleep)

        pipeline.run()

        # Cross-state consistency: in-memory state agrees with on-disk
        assert pipeline._runtime.state.queue == pipeline.backend.load_queue()

        # Job NOT in wall (skipped due to backoff)
        wall = pipeline.backend.load_wall()
        assert "test::j1" not in wall

        # Queue still contains the job
        queue = pipeline.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in queue]
        assert set(uids) == {"test::j1"}


class TestBackoffStaleUntilCleanup:
    """残留 monotonic ``_backoff_until``（无 wall_deadline）
    跨重启不得阻塞 job——孤儿 defer曾只写 _backoff_until，崩溃持久化
    后重启 monotonic 归零，旧值被调度器误判为未来退避（阻塞数小时）。
    _run_body 加载时必须清除无有效 wall_deadline 的残留 _backoff_until。"""

    def test_stale_backoff_until_without_wall_deadline_cleared(self, tmp_path, monkeypatch):
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "test_backoff_stale_state.db"

        # 模拟残留：只有 monotonic _backoff_until（无 wall_deadline）
        job = Job("test", "j1", payload={})
        job_dict = job.to_dict()
        job_dict["runtime"]["_backoff_until"] = time.monotonic() + 3600

        backend = SQLiteStateBackend(db_path)
        backend.save_queue([job_dict])

        pipeline = TaskLite(
            name="test_backoff_stale", state_dir=state_dir, backend="sqlite"
        )
        pipeline.register_handler("test", lambda j, c: (True, {}))

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        # 若残留 _backoff_until 未被清除，调度器会 sleep ~1M s；stop 短路验证
        def _stop_on_sleep(_s):
            pipeline.stop()
        monkeypatch.setattr("tasklite.pipeline.time.sleep", _stop_on_sleep)

        pipeline.run()

        # 加载期必须清除残留：job 已可运行 → 执行成功进 wall（而非卡在退避）
        wall = pipeline.backend.load_wall()
        assert "test::j1" in wall, \
            "残留 _backoff_until 必须被 _run_body 清除，job 应照常执行"
        # 内存与磁盘一致
        assert pipeline.backend.load_queue() == []


class TestBackendIntegrationWeirdCases:
    """Cross-backend integration edge cases covering cursor persistence,
    rollback semantics, multi-job ordering, and rollback param acceptance."""

    def test_sqlite_backend_rollback_param_accepted(self, tmp_state_dir, monkeypatch):
        """SQLite's commit_job_success is atomic — verify calling it (no
        rollback_job_dict param) doesn't crash and returns True."""
        pipeline = TaskLite(name="test_sqlite_rollback", state_dir=tmp_state_dir, backend="sqlite")
        pipeline.register_handler("test", lambda j, c: (True, {}))

        # Direct backend call: should not raise.
        result = pipeline.backend.commit_job_success(
            "test::j1",
            {"ok": True},
            cursor_updates={},
        )
        assert result is True, "SQLite commit_job_success should return True on success"

        wall = pipeline.backend.load_wall()
        assert "test::j1" in wall

    def test_sqlite_backend_rollback_param_on_failure_accepted(self, tmp_state_dir):
        """SQLite commit_job_failure (no rollback_job_dict param) should not raise."""
        pipeline = TaskLite(name="test_sqlite_fail_rb", state_dir=tmp_state_dir, backend="sqlite")

        result = pipeline.backend.commit_job_failure(
            "test::j1",
            {"error": "boom"},
        )
        assert result is True
        failed = pipeline.backend.load_failed()
        assert "test::j1" in failed

    def test_sqlite_backend_cursor_persistence_across_restart(self, tmp_state_dir):
        """SQLite cursor updates persist across pipeline instances."""
        pipeline1 = TaskLite(name="test_sql_cur", state_dir=tmp_state_dir, backend="sqlite")
        pipeline1.backend.commit_job_success(
            "test::j1",
            {"ok": True},
            cursor_updates={"since_id": "abc123"},
        )
        pipeline2 = TaskLite(name="test_sql_cur", state_dir=tmp_state_dir, backend="sqlite")
        loaded = pipeline2.backend.load_cursors()
        assert loaded.get("since_id") == "abc123"

    def test_sqlite_backend_multiple_jobs_success(self, tmp_state_dir, monkeypatch):
        """SQLite backend: 3 jobs succeed → all in wall, queue empty."""
        pipeline = TaskLite(name="test_sql_multi", state_dir=tmp_state_dir, backend="sqlite")
        pipeline.register_handler("test", lambda j, c: (True, {}))
        pipeline.enqueue([
            Job("test", "s1", payload={}),
            Job("test", "s2", payload={}),
            Job("test", "s3", payload={}),
        ])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "test::s1" in wall
        assert "test::s2" in wall
        assert "test::s3" in wall
        assert pipeline.backend.load_queue() == []

    def test_sqlite_backend_bulk_failure_preserves_remaining_queue(self, tmp_state_dir):
        """commit_bulk_failure on SQLite preserves the remaining queue in order."""
        pipeline = TaskLite(name="test_bulk_sql", state_dir=tmp_state_dir, backend="sqlite")
        # 预置 queue 含 dead1, dead2, keep1, keep2（delta bulk failure 按 uid 删除）
        pipeline.backend.save_queue([
            Job("test", "dead1").to_dict(),
            Job("test", "dead2").to_dict(),
            Job("test", "keep1").to_dict(),
            Job("test", "keep2").to_dict(),
        ])
        pipeline.backend.commit_bulk_failure(
            [("test::dead1", {"error": "x"}), ("test::dead2", {"error": "y"})],
        )
        failed = pipeline.backend.load_failed()
        assert "test::dead1" in failed
        assert "test::dead2" in failed
        q = pipeline.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in q]
        assert uids == ["test::keep1", "test::keep2"]

    def test_sqlite_cursor_persists_across_restart_via_real_run(self, tmp_state_dir, monkeypatch):
        """End-to-end: a handler that sets a cursor via ctx.set_cursor persists
        the cursor across pipeline restarts (SQLite backend).

        Same as the JSON variant but exercises the SQLite cursor persistence
        path through the real ``pipeline.run()`` loop.
        """
        pipeline = TaskLite(name="test_e2e_cur_sql", state_dir=tmp_state_dir, backend="sqlite")
        pipeline.register_handler("cursor_setter", lambda j, c: (True, {}))
        pipeline.enqueue([Job("cursor_setter", "j1", payload={})])

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=CursorSetterFakeProcess)

        pipeline.run()

        # Construct a brand new pipeline pointing at the same state_dir.
        pipeline2 = TaskLite(name="test_e2e_cur_sql", state_dir=tmp_state_dir, backend="sqlite")
        loaded = pipeline2.backend.load_cursors()
        assert loaded.get("high_water") == "999", (
            f"Cursor should survive restart via real run, got: {loaded}"
        )


# ============================================================
# Unified backend failure contract (Task 2)
# ============================================================


class TestBackendFailureContract:
    """On internal failure, both backends return ``False`` and leave the
    on-disk queue unchanged (it still contains the popped job). The backend
    does NOT apply the delta (no wall/DLQ write, no queue mutation) on
    failure — the on-disk queue is left unchanged so the pipeline can retry.

    This avoids the state-split bug where spawned_jobs would be persisted
    to disk despite the job not being committed.
    """

    # ── Test 2: JSON failure path ──────────────────────────────────────

    def test_sqlite_commit_success_returns_false_and_preserves_queue(self, tmp_path):
        """commit_job_success: when the SQLite transaction raises (e.g. disk
        full), the backend returns False and the on-disk queue is left
        unchanged (the transaction was rolled back by the context manager).

        Monkeypatch strategy: patch ``sqlite3.connect`` so the FIRST call
        (the commit transaction via ``_get_conn``) returns a mock connection
        whose ``execute`` raises ``sqlite3.OperationalError``. Subsequent
        calls (post-commit ``load_queue``) delegate to the real
        ``sqlite3.connect``.
        """
        backend = SQLiteStateBackend(tmp_path / "test_sql_success_contract.db")
        job_a = Job("test", "a", payload={}).to_dict()
        job_b = Job("test", "b", payload={}).to_dict()
        backend.save_queue([job_a, job_b])

        real_connect = sqlite3.connect
        call_count = {"n": 0}

        def fake_connect(path, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # The commit transaction — make execute raise on first write.
                conn = mock.MagicMock()
                conn.__enter__.return_value = conn
                conn.__exit__.return_value = False  # do NOT suppress the exception
                conn.execute.side_effect = sqlite3.OperationalError("disk full")
                return conn
            return real_connect(path, *args, **kwargs)

        with mock.patch("sqlite3.connect", side_effect=fake_connect):
            result = backend.commit_job_success(
                "test::a",
                {"ok": True},
                cursor_updates=None,
            )

        assert result is False, (
            "commit_job_success must return False when the transaction fails"
        )
        uids = [Job.from_dict(j).uid for j in backend.load_queue()]
        assert uids == ["test::a", "test::b"], (
            f"On-disk queue should be unchanged after rollback, got: {uids}"
        )
        assert call_count["n"] >= 1

    # ── Test 4: SQLite failure path ────────────────────────────────────

    def test_sqlite_commit_failure_returns_false_and_preserves_queue(self, tmp_path):
        """commit_job_failure: when the SQLite transaction raises, the
        backend returns False and the on-disk queue is left unchanged
        (the transaction was rolled back by the context manager).
        """
        backend = SQLiteStateBackend(tmp_path / "test_sql_fail_contract.db")
        job_a = Job("test", "a", payload={}).to_dict()
        job_b = Job("test", "b", payload={}).to_dict()
        backend.save_queue([job_a, job_b])

        real_connect = sqlite3.connect
        call_count = {"n": 0}

        def fake_connect(path, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                conn = mock.MagicMock()
                conn.__enter__.return_value = conn
                conn.__exit__.return_value = False
                conn.execute.side_effect = sqlite3.OperationalError("disk full")
                return conn
            return real_connect(path, *args, **kwargs)

        with mock.patch("sqlite3.connect", side_effect=fake_connect):
            result = backend.commit_job_failure(
                "test::a",
                {"error": "boom"},
            )

        assert result is False, (
            "commit_job_failure must return False when the transaction fails"
        )
        uids = [Job.from_dict(j).uid for j in backend.load_queue()]
        assert uids == ["test::a", "test::b"], (
            f"On-disk queue should be unchanged after rollback, got: {uids}"
        )
        assert call_count["n"] >= 1

    # ── Test 5: Signature regression guard ─────────────────────────────

    def test_commit_signatures_do_not_accept_rollback_job_dict(self):
        """Regression guard: ``commit_job_success`` and ``commit_job_failure``
        on BOTH backends must NOT have a ``rollback_job_dict`` parameter
        (removed in Task 1 — backends look up the rollback job by uid
        internally). Prevents re-adding the parameter accidentally.
        """
        for cls in (SQLiteStateBackend,):
            for method_name in ("commit_job_success", "commit_job_failure"):
                sig = inspect.signature(getattr(cls, method_name))
                assert "rollback_job_dict" not in sig.parameters, (
                    f"{cls.__name__}.{method_name} must not accept "
                    f"rollback_job_dict (found params: {list(sig.parameters)})"
                )


class TestPipelineCommitFailureRequeue:
    """Pipeline-level test: commit_job_failure returning False re-queues the job."""

    def test_pipeline_requeues_job_on_commit_job_failure_false(self, tmp_state_dir, monkeypatch):
        """When backend.commit_job_failure returns False, pipeline crashes (crash-only)
        and on-disk queue preserves the job at front."""
        pipeline = TaskLite(name="test_fail_requeue", state_dir=tmp_state_dir, backend="sqlite")
        pipeline.register_handler("test", lambda j, c: (True, {}))
        pipeline.enqueue([Job("test", "j1", payload={})])

        FakeP = make_fake_process_class("error")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        def always_failing_commit(*args, **kwargs):
            return False

        pipeline.backend.commit_job_failure = always_failing_commit

        with pytest.raises(BaseException, match="Backend commit returned False"):
            pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "test::j1" not in failed, "Job must not be in DLQ when commit_job_failure returns False"

        q = pipeline.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in q]
        assert uids == ["test::j1"], f"Job should be re-queued at front, got: {uids}"
