"""Invariant tests for tasklite.
Each test encodes a specific invariant that must hold across backend
implementations and pipeline lifecycle events. These tests are intentionally
narrow — they verify one contract per test so that a regression points at
exactly the violated invariant.
"""
import sqlite3
from unittest import mock
import pytest
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.channel import _normalize_handler_result
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite
from tests.helpers import make_fake_process_class, make_pipeline, patch_multiprocessing_for_fakes

class TestInvariants:
    """Pipeline + backend invariants that must always hold."""
    # ── handler result normalization ───────────────────────────
    @pytest.mark.parametrize(
        "value",
        [
            42,
            "hello",
            [1, 2, 3],
            (1, 2, 3),
            object(),
        ],
        ids=["int", "str", "list", "tuple_len3", "object"],
    )
    def test_normalize_handler_result_rejects_unknown_types(self, tmp_path, value):
        """_normalize_handler_result rejects any return type other
        than None, bool, dict, or tuple(bool, dict). Unknown types yield
        success=False with an "error" key in the metadata so the job is
        routed to the DLQ instead of being silently marked successful.
        None is intentionally excluded from the parametrization — None is
        a valid success return.
        """
        # 实例方法转发层已删——直接测 executor 模块级实现。
        success, meta = _normalize_handler_result(value)
        assert success is False, (
            f"Expected success=False for {type(value).__name__}, got True"
        )
        assert "error" in meta, (
            f"Expected 'error' key in meta for {type(value).__name__}, got: {meta}"
        )
    # ── in-memory state matches on-disk state after run ────────
    def test_pipeline_run_in_memory_matches_on_disk(self, tmp_path, monkeypatch):
        """After a successful pipeline run, the in-memory queue
        (``pipeline._runtime.state.queue``) matches the on-disk queue loaded
        from the backend. With all jobs completed, both must be empty.
        This guards against drift between the pipeline's in-memory state
        snapshot and the durable backend state.
        """
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))
        pipeline.enqueue([
            Job("test", "j1", payload={}),
            Job("test", "j2", payload={}),
            Job("test", "j3", payload={}),
        ])
        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        pipeline.run()
        in_memory_uids = set(Job.from_dict(j).uid for j in pipeline._runtime.state.queue)
        on_disk_uids = set(Job.from_dict(j).uid for j in pipeline.backend.load_queue())
        assert in_memory_uids == on_disk_uids, (
            f"In-memory queue {in_memory_uids} != on-disk queue {on_disk_uids}"
        )
        assert pipeline.backend.load_queue() == [], (
            "On-disk queue must be empty after all jobs complete"
        )
        wall = pipeline.backend.load_wall()
        for uid in ("test::j1", "test::j2", "test::j3"):
            assert uid in wall, f"Missing {uid} in wall after successful run"
    # ── wall contains exactly the completed jobs ───────────────
    def test_pipeline_run_wall_contains_completed(self, tmp_path, monkeypatch):
        """After a successful pipeline run of 3 jobs, the wall
        contains EXACTLY those 3 uids — full set equality, not mere
        membership.
        Catches both missing entries (job not persisted) and extra entries
        (job persisted that shouldn't have been, e.g. from a retry leak).
        """
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))
        pipeline.enqueue([
            Job("test", "j1", payload={}),
            Job("test", "j2", payload={}),
            Job("test", "j3", payload={}),
        ])
        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        pipeline.run()
        wall_keys = set(pipeline.backend.load_wall().keys())
        assert wall_keys == {"test::j1", "test::j2", "test::j3"}, (
            f"Wall keys must be exactly the 3 completed uids, got: {wall_keys}"
        )
    # ── SQLite PRAGMA configuration persists / is applied ──────
    def test_sqlite_pragma_synchronous_persists(self, tmp_path):
        """SQLiteStateBackend configures durable + per-connection
        PRAGMAs correctly.
        Two distinct PRAGMA scopes are verified:
        1. ``journal_mode=WAL`` — database-level, persists across fresh
           connections. A raw ``sqlite3.connect(path)`` (NOT using
           ``backend._get_conn``) must report ``wal``.
        2. ``synchronous=FULL`` — per-connection（持久性增强：NORMAL→FULL，
           消除 enqueue 应答后断电任务静默蒸发）. SQLite does NOT persist
           ``synchronous`` across fresh connections (it is per-connection
           by design, confirmed by the backend's own ``_get_conn``
           docstring). The backend guarantees ``synchronous=FULL`` on
           every connection it opens via ``_get_conn``, which is what
           all backend methods use.
        0=OFF, 1=NORMAL, 2=FULL (for synchronous reference).
        """
        db_path = tmp_path / "inv6_state.db"
        backend = SQLiteStateBackend(db_path)
        # 1. Persistent database-level setting: open a FRESH connection
        # (not using backend._get_conn) and verify journal_mode=WAL.
        conn = sqlite3.connect(db_path)
        try:
            journal_row = conn.execute("PRAGMA journal_mode").fetchone()
        finally:
            conn.close()
        assert journal_row is not None, "PRAGMA journal_mode returned no row"
        assert str(journal_row[0]).lower() == "wal", (
            f"Expected journal_mode=WAL (persistent database-level setting), "
            f"got: {journal_row[0]}"
        )
        # 2. Per-connection setting: verify the backend's connection helper
 # applies synchronous=FULL on every new connection.
        with backend._get_conn() as bconn:
            sync_row = bconn.execute("PRAGMA synchronous").fetchone()
        assert sync_row is not None, "PRAGMA synchronous returned no row"
        assert sync_row[0] == 2, (
            f"Expected synchronous=FULL (2) on backend connection, "
            f"got {sync_row[0]} (0=OFF, 1=NORMAL, 2=FULL)"
        )
