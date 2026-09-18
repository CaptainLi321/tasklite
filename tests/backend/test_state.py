"""Comprehensive tests for SQLite state backend and TaskContext methods.

SQLite backend: SQLiteStateBackend (ACID wall, DLQ, queue, cursors).
JSON 后端已移除，不再有对应测试。
"""

import json
import sqlite3
from pathlib import Path

import pytest

from tasklite.testing import fake_ctx
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.models.context import TaskContext
from tasklite.models.job import Job
from tasklite.models.state import PipelineState, uid_from_job_dict


# ============================================================
# SQLiteStateBackend tests
# ============================================================

class TestSQLiteStateBackend:
    """ACID state backend: wall, failed_dlq, queue, cursors."""

    # helpers -----------------------------------------------------------

    @staticmethod
    def _get_tables(conn: sqlite3.Connection) -> set[str]:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        return {r[0] for r in rows}

    # init & pragmas ----------------------------------------------------

    def test_init_creates_all_four_tables(self, tmp_path):
        db_path = tmp_path / "state.db"
        backend = SQLiteStateBackend(db_path)

        with sqlite3.connect(db_path) as conn:
            tables = self._get_tables(conn)
        # sqlite_sequence is auto-created for AUTOINCREMENT; just require subset
        assert {"wall", "failed_dlq", "queue", "cursors"} <= tables

    def test_wal_mode_is_set(self, tmp_path):
        db_path = tmp_path / "state.db"
        SQLiteStateBackend(db_path)

        with sqlite3.connect(db_path) as conn:
            (journal_mode,) = conn.execute("PRAGMA journal_mode").fetchone()
        assert journal_mode.lower() == "wal"

    def test_backend_uses_synchronous_full(self, tmp_path):
        """后端连接显式使用 synchronous=FULL。

        NORMAL 下 WAL commit 不 fsync——enqueue 应答后断电任务静默蒸发
        （无 job 可重跑，at-least-once 吸收不了）；FULL 保证已应答事务落盘，
        单次开销实测约 0.3ms（性能护栏验证）。
        """
        db_path = tmp_path / "state.db"
        backend = SQLiteStateBackend(db_path)

        with backend._get_conn() as conn:
            (sync,) = conn.execute("PRAGMA synchronous").fetchone()
        assert sync == 2 # FULL

    def test_init_is_idempotent(self, tmp_path):
        """Calling init twice does not error out (CREATE TABLE IF NOT EXISTS)."""
        db_path = tmp_path / "state.db"
        SQLiteStateBackend(db_path)
        backend2 = SQLiteStateBackend(db_path)

        assert backend2.load_wall() == {}
        assert backend2.load_queue() == []

    # empty loads -------------------------------------------------------

    @pytest.mark.parametrize(
        "method, expected",
        [
            ("load_wall", {}),
            ("load_failed", {}),
            ("load_cursors", {}),
            ("load_queue", []),
        ],
    )
    def test_empty_loads_return_empty(self, tmp_path, method, expected):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert getattr(backend, method)() == expected

    # commit_job_success ------------------------------------------------

    def test_commit_job_success_atomic_writes_all_tables(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")

        backend.commit_job_success(
            uid="job_001",
            result_meta={"status": "done", "bytes": 1024},
            spawned_jobs=[{"job": "download", "uid": "post_42"}],
            cursor_updates={"cursor_artist_1": "page_5"},
        )

        # Wall
        wall = backend.load_wall()
        assert "job_001" in wall
        assert wall["job_001"] == {"status": "done", "bytes": 1024}

        # Queue contains spawned job (popped uid was not present)
        queue = backend.load_queue()
        assert queue == [{"job": "download", "uid": "post_42"}]

        # Cursors updated
        cursors = backend.load_cursors()
        assert cursors == {"cursor_artist_1": "page_5"}

    def test_commit_job_success_empty_queue_and_nil_cursors(self, tmp_path):
        """commit_job_success with no spawned jobs / no cursors removes the popped job."""
        backend = SQLiteStateBackend(tmp_path / "state.db")

        # Seed a queue entry whose uid we will commit (delta: delete popped uid)
        backend.save_queue([Job("t", "uid_x").to_dict()])
        assert len(backend.load_queue()) == 1

        backend.commit_job_success("t::uid_x", {"ok": True}, cursor_updates=None)

        assert backend.load_wall() == {"t::uid_x": {"ok": True}}
        assert backend.load_queue() == []
        assert backend.load_cursors() == {}

    def test_commit_job_success_multiple_cursor_updates(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success(
            "uid",
            {},
            cursor_updates={"a": "1", "b": "2", "c": "3"},
        )
        cursors = backend.load_cursors()
        assert cursors == {"a": "1", "b": "2", "c": "3"}

    # commit_job_failure ------------------------------------------------

    def test_commit_job_failure_records_in_dlq_and_clears_queue(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([Job("t", "failed_1").to_dict()])

        backend.commit_job_failure(
            uid="t::failed_1",
            result_meta={"error": "timeout"},
        )

        failed = backend.load_failed()
        assert failed["t::failed_1"]["error"] == "timeout"
        assert failed["t::failed_1"]["_attempt"] == 1  # 2.8f: 首次失败 attempt=1

        # Queue should be emptied (popped uid deleted)
        assert backend.load_queue() == []

    def test_commit_job_failure_preserves_remaining_queue(self, tmp_path):
        """After failure, the failed job is removed; remaining queue preserved."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([Job("t", "discard").to_dict(), Job("t", "next").to_dict()])

        backend.commit_job_failure(
            uid="t::discard",
            result_meta={},
        )

        assert backend.load_queue() == [Job("t", "next").to_dict()]
        assert "t::discard" in backend.load_failed()

    # commit_bulk_failure -----------------------------------------------

    def test_commit_bulk_failure_records_multiple_failures(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([Job("t", "a").to_dict(), Job("t", "b").to_dict()])

        backend.commit_bulk_failure(
            uids_metas=[("t::a", {"err": "x"}), ("t::b", {"err": "y"})],
        )

        failed = backend.load_failed()
        assert set(failed.keys()) == {"t::a", "t::b"}
        # commit_bulk_failure 也走 _write_dlq_row 单一出口 → 写 _attempt
        assert failed["t::a"]["err"] == "x"
        assert failed["t::a"]["_attempt"] == 1
        assert failed["t::b"]["err"] == "y"
        assert failed["t::b"]["_attempt"] == 1
        assert backend.load_queue() == []

    def test_commit_bulk_failure_empty_uids_does_nothing(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_bulk_failure(uids_metas=[])
        assert backend.load_failed() == {}

    # save_queue --------------------------------------------------------

    def test_save_queue_overwrites_previous_queue(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([{"a": 1}, {"b": 2}])
        backend.save_queue([{"c": 3}])

        assert backend.load_queue() == [{"c": 3}]

    def test_save_queue_empty_clears_all(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([{"hanging": "job"}])
        backend.save_queue([])

        assert backend.load_queue() == []

    def test_save_queue_preserves_order(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        jobs = [{"idx": i} for i in range(10)]
        backend.save_queue(jobs)
        assert backend.load_queue() == jobs

    def test_save_queue_db_error_raises(self, tmp_path, monkeypatch):
        """save_queue raises on SQLite error instead of silently losing data."""
        backend = SQLiteStateBackend(tmp_path / "state.db")

        def _boom(*args, **kwargs):
            raise sqlite3.OperationalError("disk full")

        monkeypatch.setattr(sqlite3, "connect", _boom)
        with pytest.raises(sqlite3.OperationalError, match="disk full"):
            backend.save_queue([{"job": "x"}])

    # append_failed -----------------------------------------------------

    def test_append_failed_adds_to_dlq(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.append_failed("fail_a", {"reason": "crash"})
        backend.append_failed("fail_b", {"reason": "oom"})

        failed = backend.load_failed()
        # _write_dlq_row 统一补 error_type + failed_at 结构化字段
        for uid, meta in failed.items():
            assert meta["reason"] in ("crash", "oom")
            assert meta["_attempt"] == 1
            assert meta["error_type"] == "unknown"
            assert meta["failed_at"]

    def test_append_failed_overwrites_existing_uid(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.append_failed("dup", {"first": 1})
        backend.append_failed("dup", {"second": 2})

        # INSERT OR REPLACE — second write wins; _attempt 经单一出口递增
        failed = backend.load_failed()["dup"]
        assert failed["second"] == 2
        assert failed["_attempt"] == 2
        assert failed["error_type"] == "unknown"

    # idempotent init on existing DB with data --------------------------

    def test_reopen_does_not_lose_data(self, tmp_path):
        """Re-initializing backend on existing DB preserves all data."""
        db_path = tmp_path / "state.db"
        b1 = SQLiteStateBackend(db_path)
        b1.commit_job_success("uid", {"ok": True}, cursor_updates={"c": "v"})

        # Re-open
        b2 = SQLiteStateBackend(db_path)
        assert b2.load_wall() == {"uid": {"ok": True}}
        assert b2.load_cursors() == {"c": "v"}

    def test_init_failure_raisesruntime_error(self, tmp_path):
        """Creating a DB on an unwritable path → RuntimeError with descriptive message."""
        db_path = tmp_path / "state.db"
        db_path.mkdir()  # Make it a directory, not a file → sqlite3.connect fails
        with pytest.raises(RuntimeError, match="SQLite backend initialization failed"):
            SQLiteStateBackend(db_path)

    def test_old_schema_rejected(self, tmp_path):
        """旧 schema 不再自动迁移，启动即 fail-loud。"""
        db_path = tmp_path / "old_schema.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            'CREATE TABLE queue (idx INTEGER PRIMARY KEY AUTOINCREMENT, job_data TEXT)'
        )
        conn.execute(
            'INSERT INTO queue (job_data) VALUES (?)',
            ('{"task_type": "test", "job_id": "j1", "payload": {}}',),
        )
        conn.commit()
        conn.close()
        with pytest.raises(RuntimeError, match="Unsupported legacy queue schema"):
            SQLiteStateBackend(db_path)


# ============================================================
# TaskContext methods tests
# ============================================================

class TestUidFromJobDictFallback:
    """uid_from_job_dict 的兜底路径测试。

    非标准 dict（无 task_type/job_id 键且无法反序列化）→ 稳定 hash 兜底
    uid（``_unknown::<64hex>``）——身份非真空的防线之一。
    """

    def test_standard_dict_fast_path(self):
        """标准 dict：task_type/job_id 直接拼接，不经反序列化。"""
        assert uid_from_job_dict({"task_type": "t", "job_id": "a"}) == "t::a"

    def test_malformed_dict_falls_back_to_hash(self):
        """畸形 dict（无标准键）→ 稳定 hash uid，前缀 _unknown::。"""
        uid = uid_from_job_dict({"weird": True, "n": 1})
        assert uid.startswith("_unknown::")
        assert len(uid) == len("_unknown::") + 64

    def test_hash_is_deterministic(self):
        """同一畸形 dict 两次调用产出相同 hash（去重判定依赖确定性）。"""
        d = {"weird": True, "n": 1}
        assert uid_from_job_dict(d) == uid_from_job_dict(d)

    def test_hash_ignores_key_order(self):
        """sort_keys 序列化 → 键序无关（同一内容不同字面量顺序同 uid）。"""
        a = uid_from_job_dict({"x": 1, "y": 2})
        b = uid_from_job_dict({"y": 2, "x": 1})
        assert a == b

    def test_unserializable_values_fall_back(self):
        """JSON 不可序列化值走 default=str 兜底，不抛异常。"""
        uid = uid_from_job_dict({"obj": object()})
        assert uid.startswith("_unknown::")


class TestTaskContextMethods:
    """Tests for TaskContext.is_completed() and is_failed() with populated sets."""

    @staticmethod
    def _make_ctx(**overrides) -> TaskContext:
        job = Job("t", "j1")
        defaults = {
            "wall": set(),
            "failed": set(),
            "cursors": {},
        }
        defaults.update(overrides)
        return fake_ctx(job, **defaults)

    def test_is_completed_true_for_wall_member(self):
        """is_completed returns True if uid in wall_keys, False otherwise."""
        ctx = self._make_ctx(wall={"t::j1", "t::x"})
        assert ctx.is_completed("t::j1") is True
        assert ctx.is_completed("t::j2") is False

    def test_is_failed_true_for_failed_member(self):
        """is_failed returns True if uid in failed_keys, False otherwise."""
        ctx = self._make_ctx(failed={"t::j1", "t::x"})
        assert ctx.is_failed("t::j1") is True
        assert ctx.is_failed("t::j2") is False

    def test_is_completed_and_is_failed_independent(self):
        """is_completed and is_failed are independent — same uid can't be in both, but different uids can."""
        ctx = self._make_ctx(
            wall={"t::a", "t::done"}, failed={"t::b", "t::fail"}
        )
        assert ctx.is_completed("t::a") and not ctx.is_failed("t::a")
        assert ctx.is_failed("t::b") and not ctx.is_completed("t::b")

    def test_get_cursor_returns_value(self):
        """get_cursor returns cursor value for known key, None for unknown."""
        ctx = self._make_ctx(
            cursors={"artist_123": "page_5", "artist_456": "page_1"}
        )
        assert ctx.get_cursor("artist_123") == "page_5"
        assert ctx.get_cursor("artist_456") == "page_1"
        assert ctx.get_cursor("nonexistent") is None

    def test_get_cursor_empty_cursors(self):
        """get_cursor returns None when cursors dict is empty."""
        ctx = self._make_ctx()
        assert ctx.get_cursor("any_key") is None

    def test_set_cursor_updates_both_internal_and_updates(self):
        ctx = self._make_ctx()
        ctx.set_cursor("k", "v")
        assert ctx.cursor_updates == {"k": "v"}
        assert ctx.get_cursor("k") == "v"

    def test_set_cursor_preserves_value_unchanged(self):
        """set_cursor no longer coerces values; callers should pass strings."""
        ctx = self._make_ctx()
        ctx.set_cursor("k", "42")
        assert ctx.cursor_updates["k"] == "42"
        assert ctx.get_cursor("k") == "42"

    def test_set_cursor_rejects_non_string_key(self):
        """set_cursor 拒绝非字符串 key，避免后端写入失败。"""
        ctx = self._make_ctx()
        with pytest.raises(TypeError, match="cursor key must be str"):
            ctx.set_cursor(123, "val")

    def test_set_cursor_rejects_non_string_value(self):
        """set_cursor 拒绝非字符串 value，避免后端 str() 强转掩盖 bug。"""
        ctx = self._make_ctx()
        with pytest.raises(TypeError, match="cursor value must be str"):
            ctx.set_cursor("key", 123)

    def test_suspend_resource_appends_to_list(self):
        ctx = self._make_ctx()
        ctx.suspend_resource("api", 10.0)
        assert ctx.resource_suspensions == [("api", 10.0)]

    def test_declare_output_with_path_object(self):
        ctx = self._make_ctx(output_root=None)
        r = ctx.declare_output(Path("/tmp/test_file.txt"))
        assert Path(r).is_absolute()


# ============================================================
# Backend robustness: rollback params, bulk order, corruption, concurrency
# ============================================================


class TestSQLiteBackendRobustness:
    """Edge cases for SQLite backend: rollback params, bulk order, corruption."""

    def test_commit_job_success_with_rollback_param_accepted(self, tmp_path):
        """SQLite commit_job_success is atomic (no rollback_job_dict param)."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        # 预置 queue 含 uid_1（delta: 删除 popped uid）
        backend.save_queue([Job("t", "uid_1").to_dict(), Job("t", "next").to_dict()])
        # Should not raise; rollback is owned internally by the backend.
        backend.commit_job_success(
            uid="t::uid_1",
            result_meta={"ok": True},
            spawned_jobs=[],
            cursor_updates={"k": "v"},
        )
        assert backend.load_wall() == {"t::uid_1": {"ok": True}}
        assert backend.load_queue() == [Job("t", "next").to_dict()]

    def test_commit_job_failure_with_rollback_param_accepted(self, tmp_path):
        """SQLite commit_job_failure is atomic (no rollback_job_dict param)."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([Job("t", "fail_uid").to_dict(), Job("t", "next").to_dict()])
        backend.commit_job_failure(
            uid="t::fail_uid",
            result_meta={"error": "boom"},
        )
        assert backend.load_failed()["t::fail_uid"]["error"] == "boom"
        assert backend.load_failed()["t::fail_uid"]["_attempt"] == 1  # 2.8f
        assert backend.load_queue() == [Job("t", "next").to_dict()]

    def test_commit_bulk_failure_preserves_queue_order(self, tmp_path):
        """Bulk fail 2 of 3 jobs → remaining queue preserves order."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        # 预置 queue 含 a, b, c（uid 来自 task_type::job_id）
        backend.save_queue([
            Job("t", "a").to_dict(),
            Job("t", "b").to_dict(),
            Job("t", "c").to_dict(),
        ])
        # Bulk fail first 2, keep the 3rd
        backend.commit_bulk_failure(
            uids_metas=[("t::a", {"err": "x"}), ("t::b", {"err": "y"})],
        )
        queue = backend.load_queue()
        assert queue == [Job("t", "c").to_dict()]
        failed = backend.load_failed()
        assert set(failed.keys()) == {"t::a", "t::b"}

    def test_commit_bulk_failure_preserves_multi_queue_order(self, tmp_path):
        """Bulk fail with multiple remaining queue items → order preserved."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([Job("t", f"j{i}").to_dict() for i in range(5)])
        # bulk fail "t::j0"，保留 j1..j4
        backend.commit_bulk_failure(
            uids_metas=[("t::j0", {})],
        )
        queue = backend.load_queue()
        assert [Job.from_dict(j).job_id for j in queue] == ["j1", "j2", "j3", "j4"]

    def test_save_queue_large_list(self, tmp_path):
        """1000 jobs saved and loaded back in order."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        jobs = [{"idx": i, "task_type": "t", "job_id": f"j{i}"} for i in range(1000)]
        backend.save_queue(jobs)
        loaded = backend.load_queue()
        assert len(loaded) == 1000
        assert loaded[0]["idx"] == 0
        assert loaded[999]["idx"] == 999
        assert [j["idx"] for j in loaded] == list(range(1000))

    def test_load_wall_corrupt_payload_raises(self, tmp_path):
        """Wall row with non-JSON payload → load_wall raises RuntimeError."""
        db_path = tmp_path / "state.db"
        backend = SQLiteStateBackend(db_path)
        # Inject a corrupt wall row directly via sqlite3
        with sqlite3.connect(db_path) as conn:
            conn.execute("INSERT INTO wall (uid, payload) VALUES (?, ?)",
                         ("bad_uid", "not valid json{{{"))
        with pytest.raises(RuntimeError, match="Corrupted wall payload"):
            backend.load_wall()

    def test_load_queue_corrupt_payload_raises(self, tmp_path):
        """Queue row with non-JSON job_data → load_queue raises RuntimeError."""
        db_path = tmp_path / "state.db"
        backend = SQLiteStateBackend(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO queue (uid, seq, job_data) VALUES (?, ?, ?)",
                ("bad", 0, "not valid json{{{"),
            )
        with pytest.raises(RuntimeError, match="Corrupted queue payload"):
            backend.load_queue()

    def test_load_failed_corrupt_payload_raises(self, tmp_path):
        """DLQ row with non-JSON payload → load_failed raises RuntimeError."""
        db_path = tmp_path / "state.db"
        backend = SQLiteStateBackend(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.execute("INSERT INTO failed_dlq (uid, payload) VALUES (?, ?)",
                         ("bad_uid", "not valid json{{{"))
        with pytest.raises(RuntimeError, match="Corrupted failed DLQ payload"):
            backend.load_failed()

    def test_load_queue_locked_db_raises(self, tmp_path, monkeypatch):
        """Database locked → load_queue raises RuntimeError instead of returning []."""
        backend = SQLiteStateBackend(tmp_path / "state.db")

        def _locked(*args, **kwargs):
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(sqlite3, "connect", _locked)
        with pytest.raises(RuntimeError, match="Failed to load queue"):
            backend.load_queue()

    def test_append_failed_with_none_payload(self, tmp_path):
        """append_failed(uid, None) → stores {} (via `payload or {}`)."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.append_failed("uid_none", None)
        failed = backend.load_failed()
        meta = failed["uid_none"]
        assert meta["_attempt"] == 1
        assert meta["error_type"] == "unknown"
        assert meta["failed_at"]

    def test_append_failed_with_complex_payload(self, tmp_path):
        """Nested dict/list payload survives JSON roundtrip."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        complex_payload = {
            "error": "timeout",
            "details": {
                "traceback": "line1\nline2",
                "context": {"a": 1, "b": [2, 3, 4]},
            },
            "retries": 3,
        }
        backend.append_failed("uid_complex", complex_payload)
        failed = backend.load_failed()
        meta = failed["uid_complex"]
        # _attempt 由 _write_dlq_row 注入；统一补 error_type + failed_at
        assert meta["error"] == "timeout"
        assert meta["details"] == complex_payload["details"]
        assert meta["_attempt"] == 1
        assert meta["error_type"] == "unknown"
        assert meta["failed_at"]

    def test_cursors_value_coerced_to_string(self, tmp_path):
        """commit_job_success 不再 str() 强转；set_cursor 上游保证 str。
        传入 str 值原样存储；非 str 值由 set_cursor 的 TypeError 拦截。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success(
            "uid", {},
            cursor_updates={"str_val": "hello", "num_str": "123"},
        )
        cursors = backend.load_cursors()
        assert cursors["str_val"] == "hello"
        assert cursors["num_str"] == "123"

    def test_reopen_after_corrupt_queue(self, tmp_path):
        """Corrupt queue table → reopen → load_queue returns []."""
        db_path = tmp_path / "state.db"
        backend = SQLiteStateBackend(db_path)
        # Drop the queue table entirely
        with sqlite3.connect(db_path) as conn:
            conn.execute("DROP TABLE queue")
        # Reopen — _init_db recreates the table via CREATE TABLE IF NOT EXISTS
        backend2 = SQLiteStateBackend(db_path)
        assert backend2.load_queue() == []

    def test_reopen_after_corrupt_wall(self, tmp_path):
        """Corrupt wall table → reopen → load_wall returns {}."""
        db_path = tmp_path / "state.db"
        backend = SQLiteStateBackend(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.execute("DROP TABLE wall")
        backend2 = SQLiteStateBackend(db_path)
        assert backend2.load_wall() == {}

    def test_concurrent_save_queue_threading(self, tmp_path):
        """Two threads calling save_queue → no corruption (SQLite serializes)."""
        import threading
        backend = SQLiteStateBackend(tmp_path / "state.db")
        errors = []

        def writer(start):
            try:
                for i in range(start, start + 50):
                    backend.save_queue([{"thread_write": i}])
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer, args=(0,))
        t2 = threading.Thread(target=writer, args=(1000,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert errors == [], f"Concurrent writes raised: {errors}"
        # Final state should be a valid single-entry queue
        queue = backend.load_queue()
        assert len(queue) == 1
        assert "thread_write" in queue[0]

    def test_empty_string_uid_accepted(self, tmp_path):
        """Empty string uid is accepted (PRIMARY KEY allows '')."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success("", {"empty": True}, cursor_updates={})
        wall = backend.load_wall()
        assert "" in wall
        assert wall[""] == {"empty": True}

    def test_unicode_uid_accepted(self, tmp_path):
        """Unicode uid is accepted."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        uid = "task::日本語_テスト"
        backend.commit_job_success(uid, {"ok": True}, cursor_updates={})
        wall = backend.load_wall()
        assert uid in wall

    def test_overwrite_wall_entry_on_same_uid(self, tmp_path):
        """Same uid committed twice → second overwrites first (INSERT OR REPLACE)."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success("uid", {"v": 1}, cursor_updates={})
        backend.commit_job_success("uid", {"v": 2}, cursor_updates={})
        wall = backend.load_wall()
        assert wall["uid"] == {"v": 2}

    def test_commit_job_success_empty_cursor_updates(self, tmp_path):
        """cursor_updates={} → no cursor writes, no error."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success("uid", {}, cursor_updates={})
        assert backend.load_cursors() == {}

    def test_commit_job_success_none_cursor_updates(self, tmp_path):
        """cursor_updates=None → no cursor writes, no error."""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success("uid", {}, cursor_updates=None)
        assert backend.load_cursors() == {}

    # commit_retry 普通 INSERT（不再静默丢弃冲突 uid） -----------

    def test_commit_retry_conflicting_uid_returns_false(self, tmp_path):
        """requeued_job 的 uid 与队列既有行冲突 → 普通 INSERT 抛
        IntegrityError → 返回 False，on-disk 队列不变（旧 INSERT OR IGNORE
        会静默丢弃该 job 却返回 True）。"""
        backend = SQLiteStateBackend(tmp_path / "retry.db")
        job_a = Job("t", "a", payload={}).to_dict()
        job_b = Job("t", "b", payload={}).to_dict()
        backend.save_queue([job_a, job_b])
        # requeued_job 的 uid 与仍在队列中的 job_a 冲突（t::a 已在队列）
        result = backend.commit_retry("x::ghost", job_a)
        assert result is False
        assert backend.load_queue() == [job_a, job_b], (
            "on-disk queue must be unchanged on conflict"
        )

    def test_commit_retry_same_uid_reinsert_ok(self, tmp_path):
        """popped_uid 已先删除，同 uid 重插（正常重试路径）仍成功。"""
        backend = SQLiteStateBackend(tmp_path / "retry2.db")
        job = Job("t", "a", payload={}).to_dict()
        backend.save_queue([job])
        retried = dict(job, retries=1)
        result = backend.commit_retry("t::a", retried)
        assert result is True
        q = backend.load_queue()
        assert len(q) == 1
        assert q[0]["retries"] == 1


class TestReplaceQueueAtomicWriteTransaction:
    """replace_queue_atomic 的写事务收敛契约（SQLite 腿特有）。

    不变式：磁盘真相读取、compute 合并、写回都在同一 BEGIN IMMEDIATE
    写事务内——并发方（跨进程 enqueue/commit）要么先于本事务提交（进入
    磁盘真相、参与合并），要么排队等事务提交后再落盘，绝无中间态覆盖。
    """

    def test_compute_runs_inside_write_transaction(self, tmp_path):
        """compute 执行期间写锁必须已被本事务持有。

        用零 busy-timeout 的探针连接尝试 BEGIN IMMEDIATE：拿到锁 = 本事务
        未持锁（读-改-写存在无锁窗口，缺陷结构）；立即 busy 失败 = 窗口
        已被写事务消除。确定性判定，不依赖真实多进程竞速。
        """
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([Job("t", "k1").to_dict()])
        observed = {}

        def compute(disk_q):
            probe = sqlite3.connect(backend.path, timeout=0.0)
            try:
                probe.execute('BEGIN IMMEDIATE')
                observed["lock_free"] = True
                probe.rollback()
            except sqlite3.OperationalError:
                observed["lock_free"] = False
            finally:
                probe.close()
            return disk_q

        backend.replace_queue_atomic(compute)

        assert observed["lock_free"] is False, (
            "compute 期间他进程连接必须拿不到写锁（读-改-写已收敛进写事务）"
        )

    def test_replace_raises_and_preserves_disk_on_corrupted_payload(self, tmp_path):
        """读阶段失败（队列行损坏）→ 异常传播 + 整体回滚保持磁盘原状，
        compute 不获得执行机会（杜绝以部分真相继续合并/覆盖）。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([Job("t", "k1").to_dict()])
        with sqlite3.connect(backend.path) as conn:
            conn.execute("UPDATE queue SET job_data = 'not-json'")
            conn.commit()

        compute_called = []
        with pytest.raises(Exception):
            backend.replace_queue_atomic(
                lambda disk_q: compute_called.append(disk_q) or []
            )

        assert compute_called == []
        with sqlite3.connect(backend.path) as conn:
            rows = conn.execute("SELECT job_data FROM queue").fetchall()
        assert rows == [("not-json",)], "读失败必须回滚，磁盘行原样保留"


# ============================================================
# PipelineState (mutable container) tests
# ============================================================


class TestPipelineState:
    """Tests for the mutable PipelineState container and its in-place ops."""

    # helpers -----------------------------------------------------------

    @staticmethod
    def _make_state() -> PipelineState:
        return PipelineState(
            wall={"uid_a": {"status": "ok"}},
            failed={"uid_b": {"error": "boom"}},
            cursors={"k1": "v1"},
            queue=[
                {"task_type": "t", "job_id": "j1"},
                {"task_type": "t", "job_id": "j2"},
            ],
        )

    # 1. pop_job (in-place) --------------------------------------------

    def test_pop_job_returns_popped_and_mutates(self):
        """pop_job(idx) removes and returns the dict; state mutated in place."""
        state = self._make_state()

        popped = state.pop_job(0)

        assert popped == {"task_type": "t", "job_id": "j1"}
        assert len(state.queue) == 1
        assert state.queue[0] == {"task_type": "t", "job_id": "j2"}

    def test_pop_job_syncs_queue_uids(self):
        """pop_job removes the uid from _queue_uids."""
        state = self._make_state()
        state.pop_job(0)
        assert "t::j1" not in state.queue_uids
        assert "t::j2" in state.queue_uids

    # 2. cursor updates (in-place) -------------------------------------

    def test_update_cursors_merges(self):
        """Existing cursors preserved, new keys added, overlaps overwritten."""
        state = self._make_state()

        state.update_cursors({"k1": "overwritten", "k2": "new"})

        assert state.cursors["k1"] == "overwritten"
        assert state.cursors["k2"] == "new"
        assert len(state.cursors) == 2

    def test_update_cursors_none_deletes(self):
        """值为 None 的更新删除该游标（与后端 None-删除语义一致），
        不再把 {key: None} 写进 cursors。"""
        state = self._make_state()  # cursors = {"k1": "v1"}

        state.update_cursors({"k1": None, "ghost": None})

        assert "k1" not in state.cursors
        assert "ghost" not in state.cursors
        assert state.cursors == {}

    # 3. spawned jobs front/back (in-place) ----------------------------

    def test_spawn_jobs_front_and_back(self):
        """front=True prepends, front=False appends."""
        state = self._make_state()
        spawned = [{"task_type": "t", "job_id": "spawn1"}]

        state.spawn_jobs(spawned, front=True)
        assert state.queue[0] == {"task_type": "t", "job_id": "spawn1"}
        assert state.queue[1] == {"task_type": "t", "job_id": "j1"}
        assert len(state.queue) == 3
        assert "t::spawn1" in state.queue_uids

        state2 = self._make_state()
        state2.spawn_jobs(spawned, front=False)
        assert state2.queue[-1] == {"task_type": "t", "job_id": "spawn1"}
        assert state2.queue[0] == {"task_type": "t", "job_id": "j1"}
        assert len(state2.queue) == 3

    # 4. requeued job (in-place) ---------------------------------------

    def test_requeue_jobs(self):
        """Requeued job is inserted at front by default, back when front=False."""
        state = self._make_state()
        job = {"task_type": "t", "job_id": "requeue"}

        state.requeue_jobs([job])
        assert state.queue[0] == {"task_type": "t", "job_id": "requeue"}
        assert len(state.queue) == 3

        state2 = self._make_state()
        state2.requeue_jobs([job], front=False)
        assert state2.queue[-1] == {"task_type": "t", "job_id": "requeue"}
        assert len(state2.queue) == 3

    # 5. constructor ----------------------------------------------------

    def test_constructor_initializes_fields(self):
        """PipelineState(...) copies inputs and precomputes queue_uids."""
        state = PipelineState(
            wall={"w": {"a": 1}},
            failed={"f": {"b": 2}},
            cursors={"c": "v"},
            queue=[{"task_type": "t", "job_id": "j1"}],
        )

        assert dict(state.wall) == {"w": {"a": 1}}
        assert dict(state.failed) == {"f": {"b": 2}}
        assert dict(state.cursors) == {"c": "v"}
        assert state.queue == [{"task_type": "t", "job_id": "j1"}]
        assert state.queue_uids == {"t::j1"}

    # 6. wall_keys / failed_keys ----------------------------------------

    def test_wall_keys_and_failed_keys(self):
        """wall and failed expose their keys directly."""
        state = self._make_state()

        assert set(state.wall.keys()) == {"uid_a"}
        assert set(state.failed.keys()) == {"uid_b"}

    # 7. is_empty -------------------------------------------------------

    def test_is_empty(self):
        """is_empty is True for an empty queue, False otherwise."""
        empty = PipelineState({}, {}, {}, [])
        nonempty = self._make_state()

        assert empty.is_empty is True
        assert nonempty.is_empty is False

    # 8. mark_success / mark_failed deep-copy new meta ------------------------

    def test_mark_success_deep_copies_meta(self):
        """mark_success 对新 meta 做深拷贝，外部修改不污染状态。"""
        meta = {"result": {"value": 42}}
        state = PipelineState({}, {}, {}, [])

        state.mark_success("uid", meta)
        meta["result"]["value"] = 0

        assert state.wall["uid"]["result"]["value"] == 42

    def test_mark_failed_deep_copies_meta(self):
        """mark_failed 对新 meta 做深拷贝，外部修改不污染状态。"""
        meta = {"error": {"code": "E1"}}
        state = PipelineState({}, {}, {}, [])

        state.mark_failed("uid", meta)
        meta["error"]["code"] = "E2"

        assert state.failed["uid"]["error"]["code"] == "E1"

    # 9. replace_queue ---------------------------------------------------

    def test_replace_queue_rebuilds_uids(self):
        """replace_queue 整体替换队列并重建 _queue_uids（死锁批量移除场景）。"""
        state = self._make_state()
        state.replace_queue([{"task_type": "t", "job_id": "j2"}])

        assert state.queue == [{"task_type": "t", "job_id": "j2"}]
        assert state.queue_uids == {"t::j2"}


# ============================================================
# JobScheduler malformed-input handling
# ============================================================


class TestSchedulerMalformedHandling:
    """调度器对畸形 job dict（缺 task_type 等）的健壮性。"""

    def test_malformed_job_dict_does_not_crash_scheduler(self):
        """畸形 job dict（缺 task_type）不会让 scheduler 崩溃，
        返回 malformed_uids 指向问题条目，跳过它继续扫描后续 job。"""
        from tasklite.engine.scheduler import JobScheduler

        # 构造一个永远可获得的资源，使 valid job 直接 runnable
        resources = {
            "__workers__": type(
                "R",
                (),
                {"can_acquire": lambda self, a: (True, 0.0)},
            )()
        }
        scheduler = JobScheduler(resources)
        # 畸形 job 必须放在 runnable job 之前，否则 scan 会因找到 runnable
        # job 立即 break，永远不会扫描到畸形条目。
        q_data = [
            {"job_id": "bad"},  # 缺 task_type — 畸形 (index 0)
            {"task_type": "test", "job_id": "good"},  # valid (index 1)
            {"task_type": "test", "job_id": "good2"},  # valid (index 2)
        ]
        from tasklite.models.state import PipelineState
        result = scheduler.pop_next_runnable(PipelineState({}, {}, {}, q_data), frozenset())
        # 畸形 job UID 被记录
        assert len(result.malformed_uids) == 1
        assert result.malformed_uids[0].startswith("_unknown::")
        # 跳过畸形 job 后第一个 valid job (index 1) 是 runnable
        assert result.runnable_idx == 1
