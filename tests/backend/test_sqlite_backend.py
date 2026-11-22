"""SQLiteStateBackend 专属契约测试。

收敛原 test_schema_acceptance.py、test_sqlite_backend.py 与 test_state_primitives.py 中
SQLite 专属部分：
- Schema 6 建表（含 authorizations/job_sequences）、幂等初始化与特殊字符路径；
- 只读预检、旧版本/缺表/缺列/异形表/视图/生成列/大小写兼容表拒绝并原样保留原库；
- WAL fail-loud 与 per-connection synchronous=FULL；
- 损坏 JSON 行、底层 sqlite3.Error 原样传播、触发器注入写故障回滚；
- 多线程并发混合写与两连接交错前插的 queue_position 唯一性与保序。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any
from unittest import mock

import pytest

from tasklite import TaskLiteError, parse_job_uid
from tasklite.backend import SQLiteStateBackend
import tasklite.backend.sqlite_backend as sb
from tasklite.models.job import Job
from tasklite.models.state import job_uid_from_snapshot

N_THREADS = 6
N_ROUNDS = 12


def _job_snapshot(job_id: str, **overrides: Any) -> dict[str, Any]:
    data = Job("t", job_id).to_dict()
    data.update(overrides)
    return data


def _pending_queue(backend: SQLiteStateBackend) -> list[dict[str, Any]]:
    return [row["job"] for row in backend.load_queue_jobs(status="pending")]


def _settle_success(
    backend: SQLiteStateBackend,
    job_uid: str,
    result_metadata: dict[str, Any],
    *,
    attempt_no: int = 1,
    settled_at: str = "2026-05-01T00:00:05+00:00",
    **kwargs: Any,
) -> None:
    snapshot = next(
        (job for job in _pending_queue(backend) if job_uid_from_snapshot(job) == job_uid), None
    )
    if snapshot is None:
        task_type, job_id = parse_job_uid(job_uid)
        snapshot = Job(task_type, job_id).to_dict()
        backend.enqueue_jobs([snapshot])
    sequence = backend.mark_job_running(
        job_uid,
        execution_id=f"run_seed.{attempt_no}",
        run_id="run_seed",
        started_at="2026-05-01T00:00:00+00:00",
    )
    backend.commit_job_settlement(
        job_uid=job_uid,
        outcome="success",
        attempt_no=sequence,
        execution_id=f"run_seed.{attempt_no}",
        run_id="run_seed",
        settled_at=settled_at,
        started_at="2026-05-01T00:00:00+00:00",
        job_snapshot=snapshot,
        success_metadata=result_metadata,
        **kwargs,
    )


def _queue_uids(backend: SQLiteStateBackend) -> list[str]:
    return [job_uid_from_snapshot(j) for j in _pending_queue(backend)]


def _db_snapshot(path) -> tuple[Any, Any, list[str]]:
    conn = sqlite3.connect(path)
    try:
        return (
            conn.execute("PRAGMA user_version").fetchone(),
            conn.execute("PRAGMA journal_mode").fetchone(),
            list(conn.iterdump()),
        )
    finally:
        conn.close()


class TestSchemaInitAndAcceptance:
    """Schema 6 建表、幂等初始化、只读预检与旧库/异形库拒绝保护。"""

    def test_init_creates_all_tables_columns_indexes_and_version(self, tmp_path):
        db_path = tmp_path / "state.db"
        SQLiteStateBackend(db_path).close()
        with sqlite3.connect(db_path) as conn:
            tables = {
                r[0]
                for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            expected = {
                "queue": {"job_uid", "queue_position", "job_snapshot", "status", "run_id", "attempt_no", "started_at"},
                "successes": {"job_uid", "metadata"},
                "failed": {"job_uid", "metadata", "payload"},
                "attempts": {
                    "attempt_id", "job_uid", "attempt_no", "run_id", "started_at", "settled_at",
                    "outcome", "error_message", "job_snapshot", "files", "result",
                },
                "job_sequences": {"job_uid", "last_attempt_no"},
                "authorizations": {"path", "target_type", "authorized_at"},
                "cursors": {"key", "value"},
                "engine_metadata": {"key", "value"},
            }
            assert set(expected) <= tables
            assert version == 6
            for table, columns in expected.items():
                assert {row[1] for row in conn.execute(f"PRAGMA table_xinfo({table})")} == columns
            indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
            assert {"idx_queue_position", "idx_attempts_job_uid", "idx_attempts_job_uid_outcome"} <= indexes
            assert "idx_queue_seq" not in indexes

    def test_init_is_idempotent_and_preserves_data(self, tmp_path):
        db_path = tmp_path / "state.db"
        b_first = SQLiteStateBackend(db_path)
        _settle_success(b_first, "t::one", {"ok": True}, cursor_updates={"c": "v"})
        b_first.enqueue_jobs([_job_snapshot("queued")])
        b_first.close()

        b_second = SQLiteStateBackend(db_path)
        assert b_second.load_successes() == {"t::one": {"ok": True}}
        assert b_second.load_consistent_snapshot()["cursors"] == {"c": "v"}
        assert _queue_uids(b_second) == ["t::queued"]
        b_second.close()

    @pytest.mark.parametrize("empty_file", [False, True])
    def test_fresh_database_and_special_characters(self, tmp_path, empty_file):
        path = tmp_path / "空 %# space.db"
        if empty_file:
            path.touch()
        backend = SQLiteStateBackend(path)
        backend.set_cursor("error", "new_jobs")
        backend.close()
        reopened = SQLiteStateBackend(path)
        assert reopened.load_consistent_snapshot()["cursors"] == {"error": "new_jobs"}
        reopened.close()
        with sqlite3.connect(path) as conn:
            assert conn.execute("PRAGMA user_version").fetchone() == (6,)
            assert conn.execute("PRAGMA journal_mode").fetchone() == ("wal",)

    def test_current_schema_allows_extra_user_table_and_trigger(self, tmp_path):
        path = tmp_path / "extra.db"
        SQLiteStateBackend(path).close()
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE user_data (value TEXT)")
            conn.execute("INSERT INTO user_data VALUES ('kept')")
            conn.execute("CREATE TABLE events (value TEXT)")
            conn.execute("CREATE TRIGGER queue AFTER INSERT ON events BEGIN SELECT 1; END")
        before = _db_snapshot(path)
        SQLiteStateBackend(path).close()
        assert _db_snapshot(path) == before

    @pytest.mark.parametrize("version", [0, 1, 2, 3, 4, 5, 99])
    def test_nonempty_incompatible_database_is_preserved(self, tmp_path, version):
        path = tmp_path / "original.db"
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE records (value TEXT)")
            conn.execute("INSERT INTO records VALUES ('precious')")
            conn.execute(f"PRAGMA user_version={version}")
        before = _db_snapshot(path)
        original_bytes = path.read_bytes()
        with pytest.raises(TaskLiteError) as caught:
            SQLiteStateBackend(path)
        assert path.read_bytes() == original_bytes
        assert _db_snapshot(path) == before
        assert str(path) in str(caught.value)
        assert "schema 6" in str(caught.value)
        assert "保留原库" in str(caught.value)

    @pytest.mark.parametrize(
        "setup",
        [
            "CREATE TABLE cursors (key TEXT, value TEXT)",
            "CREATE TABLE queue (job_uid TEXT, queue_position INTEGER, job_snapshot TEXT)",
            "CREATE TABLE http_snapshots (request_key TEXT)",
            "CREATE VIEW empty_view AS SELECT 1",
            "CREATE TABLE sqliteXuser (value TEXT)",
            "CREATE TABLE queue (idx INTEGER PRIMARY KEY AUTOINCREMENT, job_data TEXT)",
        ],
    )
    def test_version_zero_with_any_application_object_is_rejected(self, tmp_path, setup):
        path = tmp_path / "nonempty.db"
        with sqlite3.connect(path) as conn:
            conn.execute(setup)
        before = _db_snapshot(path)
        with pytest.raises(TaskLiteError, match="version 0 非空库|非空库|Unsupported"):
            SQLiteStateBackend(path)
        assert _db_snapshot(path) == before

    @pytest.mark.parametrize(
        "change",
        [
            "PRAGMA user_version=0",
            "PRAGMA user_version=3",
            "PRAGMA user_version=4",
            "DROP TABLE queue",
            "DROP TABLE authorizations",
            "ALTER TABLE authorizations ADD COLUMN unexpected TEXT",
            "ALTER TABLE authorizations RENAME COLUMN authorized_at TO granted_at",
            "ALTER TABLE failed ADD COLUMN unexpected TEXT",
            "ALTER TABLE attempts RENAME COLUMN error_message TO error",
            "CREATE TABLE wall (job_uid TEXT)",
            "CREATE TABLE meta (key TEXT, value TEXT)",
            "CREATE TABLE WALL (value TEXT)",
            "CREATE TABLE META (value TEXT)",
            "ALTER TABLE queue ADD COLUMN generated TEXT GENERATED ALWAYS AS ('extra')",
        ],
    )
    def test_existing_schema_must_remain_complete_and_unmixed(self, tmp_path, change):
        path = tmp_path / "existing.db"
        backend = SQLiteStateBackend(path)
        backend.commit_job_failures([("t::j", {"sentinel": "metadata"}, {"sentinel": "payload"})])
        backend.close()
        with sqlite3.connect(path) as conn:
            conn.execute(change)
        before = _db_snapshot(path)
        with pytest.raises(TaskLiteError):
            SQLiteStateBackend(path)
        assert _db_snapshot(path) == before

    def test_same_named_view_is_rejected_without_repair(self, tmp_path):
        path = tmp_path / "view.db"
        SQLiteStateBackend(path).close()
        with sqlite3.connect(path) as conn:
            conn.execute("DROP TABLE queue")
            conn.execute("CREATE VIEW queue AS SELECT '' AS job_uid, 0 AS queue_position, '' AS job_snapshot")
        before = _db_snapshot(path)
        with pytest.raises(TaskLiteError, match="必须为 table"):
            SQLiteStateBackend(path)
        assert _db_snapshot(path) == before

    def test_legacy_v1_shape_rejected(self, tmp_path):
        db_path = tmp_path / "legacy_state.db"
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE queue (idx INTEGER PRIMARY KEY AUTOINCREMENT, job_data TEXT)")
            conn.execute("CREATE TABLE failed_dlq (uid TEXT PRIMARY KEY, payload TEXT)")
            conn.execute("PRAGMA user_version = 2")
        with pytest.raises(TaskLiteError, match="Unsupported database schema version"):
            SQLiteStateBackend(db_path)

    def test_corrupt_database_is_not_treated_as_empty(self, tmp_path):
        path = tmp_path / "corrupt.db"
        data = b"not a sqlite database"
        path.write_bytes(data)
        with pytest.raises(sqlite3.DatabaseError, match="not a database"):
            SQLiteStateBackend(path)
        assert path.read_bytes() == data

    def test_unwritable_path_preserves_sqlite_error(self, tmp_path):
        db_path = tmp_path / "state.db"
        db_path.mkdir()
        with pytest.raises(sqlite3.OperationalError):
            SQLiteStateBackend(db_path)


class TestWALReadonlyPreflightAndPragmas:
    """WAL fail-loud、synchronous=FULL、只读预检与初始化重检守卫。"""

    def test_wal_mode_persists_and_synchronous_is_full(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "wal_state.db")
        try:
            with backend._get_conn() as conn:
                (sync,) = conn.execute("PRAGMA synchronous").fetchone()
            assert sync == 2
        finally:
            backend.close()
        with sqlite3.connect(tmp_path / "wal_state.db") as conn:
            (journal_mode,) = conn.execute("PRAGMA journal_mode").fetchone()
        assert journal_mode.lower() == "wal"

    def test_init_fails_loud_when_wal_unavailable(self, tmp_path, monkeypatch):
        real_connect = sqlite3.connect

        class _StaticCursor:
            def __init__(self, row):
                self._row = row

            def fetchone(self):
                return self._row

            def __iter__(self):
                return iter(())

        class NoWalConn:
            def __init__(self, path, timeout=30.0):
                self._conn = real_connect(path, timeout=timeout)

            def execute(self, sql, *args):
                if "journal_mode=WAL" in sql:
                    return _StaticCursor(("delete",))
                return self._conn.execute(sql, *args)

            def commit(self):
                return self._conn.commit()

            def rollback(self):
                return self._conn.rollback()

            def close(self):
                return self._conn.close()

            def __getattr__(self, name):
                return getattr(self._conn, name)

        monkeypatch.setattr(sb.sqlite3, "connect", NoWalConn)
        with pytest.raises(TaskLiteError, match="journal_mode=WAL could not be engaged"):
            SQLiteStateBackend(tmp_path / "nowal_state.db")

    def test_readonly_preflight_sees_committed_wal(self, tmp_path, monkeypatch):
        path = tmp_path / "wal.db"
        keeper = sqlite3.connect(path)
        try:
            keeper.execute("PRAGMA journal_mode=WAL")
            keeper.execute("PRAGMA wal_autocheckpoint=0")
            keeper.execute("CREATE TABLE precious (value TEXT)")
            keeper.execute("INSERT INTO precious VALUES ('in WAL')")
            keeper.execute("PRAGMA user_version=3")
            keeper.commit()
            assert path.with_name(path.name + "-wal").stat().st_size > 0
            real_connect = sqlite3.connect
            calls = []

            def connect(database, *args, **kwargs):
                calls.append((database, kwargs))
                return real_connect(database, *args, **kwargs)

            monkeypatch.setattr(sqlite3, "connect", connect)
            with pytest.raises(TaskLiteError, match="schema version 3"):
                SQLiteStateBackend(path)
            assert len(calls) == 1
            assert calls[0][1]["uri"] is True
            assert "mode=ro" in calls[0][0]
            assert path.with_name(path.name + "-wal").exists()
            assert keeper.execute("SELECT * FROM precious").fetchall() == [("in WAL",)]
            assert keeper.execute("PRAGMA user_version").fetchone() == (3,)
        finally:
            keeper.close()

    def test_readonly_error_never_falls_back_to_write_connection(self, tmp_path, monkeypatch):
        path = tmp_path / "existing.db"
        path.touch()
        real_connect = sqlite3.connect
        calls = []

        def connect(database, *args, **kwargs):
            calls.append((database, kwargs))
            if kwargs.get("uri"):
                raise sqlite3.OperationalError("read access denied")
            return real_connect(database, *args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", connect)
        with pytest.raises(sqlite3.OperationalError, match="read access denied"):
            SQLiteStateBackend(path)
        assert len(calls) == 1
        assert calls[0][1]["uri"] is True
        assert "mode=ro" in calls[0][0]
        assert "immutable" not in calls[0][0]
        assert path.read_bytes() == b""

    @pytest.mark.parametrize("when", ["after_preflight", "after_wal"])
    def test_initialization_rechecks_before_application_writes(self, tmp_path, monkeypatch, when):
        path = tmp_path / "changed.db"
        path.touch()
        real_connect = sqlite3.connect
        writes = []

        class Connection(sqlite3.Connection):
            def execute(self, sql, *args, **kwargs):
                if sql == "PRAGMA journal_mode=WAL" and when == "after_wal":
                    result = super().execute(sql, *args, **kwargs)
                    super().execute("PRAGMA user_version=3")
                    return result
                if sql.startswith(("CREATE", "INSERT", "UPDATE", "DELETE", "PRAGMA user_version =")):
                    writes.append(sql)
                return super().execute(sql, *args, **kwargs)

        def connect(database, *args, **kwargs):
            if not kwargs.get("uri"):
                if when == "after_preflight":
                    with real_connect(database) as mutator:
                        mutator.execute("PRAGMA user_version=3")
                kwargs["factory"] = Connection
            return real_connect(database, *args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", connect)
        with pytest.raises(TaskLiteError, match="schema version 3"):
            SQLiteStateBackend(path)
        assert writes == []
        with real_connect(path) as conn:
            assert conn.execute("PRAGMA user_version").fetchone() == (3,)
            assert conn.execute("SELECT name FROM sqlite_master").fetchall() == []

    def test_schema_creation_failure_rolls_back_tables_indexes_and_version(self, tmp_path, monkeypatch):
        path = tmp_path / "atomic.db"
        real_connect = sqlite3.connect

        class Connection(sqlite3.Connection):
            def execute(self, sql, *args, **kwargs):
                if "idx_attempts_job_uid" in sql:
                    raise sqlite3.OperationalError("index creation failed")
                return super().execute(sql, *args, **kwargs)

        def connect(database, *args, **kwargs):
            kwargs["factory"] = Connection
            return real_connect(database, *args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", connect)
        with pytest.raises(sqlite3.OperationalError, match="index creation failed"):
            SQLiteStateBackend(path)
        with real_connect(path) as conn:
            assert conn.execute("PRAGMA user_version").fetchone() == (0,)
            assert conn.execute("SELECT name FROM sqlite_master").fetchall() == []


class TestSqliteFaultsCorruptionAndPersistence:
    """SQLite 故障传播、损坏 JSON 暴露、写事务回滚与跨实例持久性。"""

    def test_transaction_failure_propagates_and_preserves_queue(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.enqueue_jobs([_job_snapshot("alpha"), _job_snapshot("beta")])

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
            with pytest.raises(sqlite3.OperationalError, match="disk full"):
                backend.commit_job_failures([("t::alpha", {"error_message": "boom"})])
        assert _queue_uids(backend) == ["t::alpha", "t::beta"]
        assert backend.load_failed() == {}
        backend.close()

    def test_unserializable_payload_propagates_and_rolls_back(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with pytest.raises(TypeError):
            backend.commit_job_failures([("t::one", {"error_message": "x"}, {"bad": object()})])
        assert backend.load_failed() == {}
        backend.close()

    def test_corrupt_json_rows_raise_decode_error(self, tmp_path):
        path = tmp_path / "state.db"
        backend = SQLiteStateBackend(path)
        with sqlite3.connect(path) as conn:
            conn.execute("INSERT INTO successes (job_uid, metadata) VALUES (?, ?)", ("bad_s", "not valid json{{{"))
        with pytest.raises(json.JSONDecodeError, match="Expecting value"):
            backend.load_successes()

        with sqlite3.connect(path) as conn:
            conn.execute("DELETE FROM successes")
            conn.execute("INSERT INTO queue (job_uid, queue_position, job_snapshot) VALUES (?, ?, ?)", ("bad_q", 0, "not valid json{{{"))
        with pytest.raises(json.JSONDecodeError, match="Expecting value"):
            backend.load_queue_jobs(status="pending")

        with sqlite3.connect(path) as conn:
            conn.execute("DELETE FROM queue")
            conn.execute("INSERT INTO failed (job_uid, metadata) VALUES (?, ?)", ("bad_f", "not valid json{{{"))
        with pytest.raises(json.JSONDecodeError, match="Expecting value"):
            backend.load_failed()
        backend.close()

    def test_load_queue_locked_db_raises(self, tmp_path, monkeypatch):
        backend = SQLiteStateBackend(tmp_path / "state.db")

        def _locked(*args, **kwargs):
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(sqlite3, "connect", _locked)
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            backend.load_queue_jobs(status="pending")
        backend.close()

    @pytest.mark.parametrize(
        ("operation", "table"),
        [
            ("INSERT", "attempts"),
            ("INSERT", "successes"),
            ("DELETE", "failed"),
            ("DELETE", "queue"),
            ("INSERT", "queue"),
            ("INSERT", "cursors"),
            ("DELETE", "cursors"),
        ],
    )
    def test_settlement_write_fault_preserves_all_tables_and_original_error(
        self, tmp_path, operation, table
    ):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failures([("t::parent", {"old": True}, {"input": 1})])
        backend.enqueue_jobs([_job_snapshot("parent"), _job_snapshot("other")])
        backend.set_cursor("remove", "old")
        backend.mark_job_running("t::parent", execution_id="run.1", run_id="run", started_at="start")
        with sqlite3.connect(backend.path) as conn:
            conn.execute(
                f"CREATE TRIGGER fail_write BEFORE {operation} ON {table} "
                "BEGIN SELECT RAISE(ABORT, 'synthetic settlement failure'); END"
            )
            before = list(conn.iterdump())
        with pytest.raises(sqlite3.IntegrityError, match="synthetic settlement failure"):
            backend.commit_job_settlement(
                job_uid="t::parent",
                outcome="success",
                attempt_no=1,
                execution_id="run.1",
                run_id="run",
                settled_at="finish",
                success_metadata={"new": True},
                spawned_job_snapshots=[_job_snapshot("child")],
                cursor_updates={"set": "new", "remove": None},
            )
        with sqlite3.connect(backend.path) as conn:
            assert list(conn.iterdump()) == before
        snap = backend.load_consistent_snapshot()
        assert [a for a in snap["attempts"] if a["job_uid"] == "t::parent"] == []
        assert snap["job_sequences"].get("t::parent", 0) == 1
        backend.close()

    def test_settled_history_persists_across_instances(self, tmp_path):
        path = tmp_path / "state.db"
        backend = SQLiteStateBackend(path)
        _settle_success(backend, "t::parent", {"ok": True})
        before = [a for a in backend.load_consistent_snapshot()["attempts"] if a["job_uid"] == "t::parent"]
        backend.close()

        reopened = SQLiteStateBackend(path)
        assert [a for a in reopened.load_consistent_snapshot()["attempts"] if a["job_uid"] == "t::parent"] == before
        assert reopened.load_latest_succeeded_finished_at("t::parent") == "2026-05-01T00:00:05+00:00"
        reopened.close()


class TestConcurrentMixedWrites:
    """多线程独立连接混合写同一 db 文件，验证 seq 唯一性与档案收敛。"""

    @staticmethod
    def _worker(
        tid: int,
        backend: SQLiteStateBackend,
        barrier: threading.Barrier,
        errors: list[str],
    ) -> None:
        try:
            for k in range(N_ROUNDS):
                barrier.wait(timeout=60)
                backend.enqueue_jobs(
                    [{"task_type": "q", "job_id": f"e{tid}-{k}"}],
                    front=(k % 2 == 0),
                )
                _settle_success(
                    backend,
                    f"c::{tid}-{k}",
                    {"ok": True},
                    spawned_job_snapshots=[{"task_type": "s", "job_id": f"s{tid}-{k}"}],
                    cursor_updates={f"cur_{tid}": str(k)},
                )
                backend.commit_job_failures([
                    ("shared::uid", {"error_message": "boom"}),
                    (f"f::{tid}-{k}", {"error_message": "x"}),
                ])
        except Exception as e:
            errors.append(f"thread {tid} failed: {e!r}")

    def test_concurrent_mixed_writes_seq_unique_archive_exact(self, tmp_path):
        db_path = tmp_path / "concurrent_state.db"
        backends = [SQLiteStateBackend(db_path) for _ in range(N_THREADS)]
        try:
            backends[0].enqueue_jobs([{"task_type": "q", "job_id": "seed"}])

            barrier = threading.Barrier(N_THREADS)
            errors: list[str] = []
            threads = [
                threading.Thread(
                    target=self._worker,
                    args=(t, backends[t], barrier, errors),
                    name=f"concurrency-writer-{t}",
                )
                for t in range(N_THREADS)
            ]
            for th in threads:
                th.start()
            for th in threads:
                th.join(timeout=120)
            assert not any(th.is_alive() for th in threads), "工作线程未在时限内退出"
            assert errors == [], f"并发写路径出现异常或失败返回: {errors}"

            with sqlite3.connect(db_path) as conn:
                total, distinct = conn.execute(
                    "SELECT COUNT(*), COUNT(DISTINCT queue_position) FROM queue"
                ).fetchone()
            assert total == distinct
            assert total == 1 + 2 * N_THREADS * N_ROUNDS

            failed = backends[0].load_failed()
            assert failed["shared::uid"] == {"error_message": "boom"}
            assert len(failed) == N_THREADS * N_ROUNDS + 1
            assert len(backends[0].load_successes()) == N_THREADS * N_ROUNDS
        finally:
            for b in backends:
                b.close()


class TestInterleavedTwoConnections:
    """两连接交错执行的确定性护栏。"""

    def test_interleaved_front_enqueue_seq_unique_and_ordered(self, tmp_path):
        db_path = tmp_path / "interleave_state.db"
        backend_a = SQLiteStateBackend(db_path)
        backend_b = SQLiteStateBackend(db_path)
        try:
            insert_order: list[str] = []
            for i in range(30):
                backend_a.enqueue_jobs([{"task_type": "t", "job_id": f"a{i}"}], front=True)
                insert_order.append(f"t::a{i}")
                backend_b.enqueue_jobs([{"task_type": "t", "job_id": f"b{i}"}], front=True)
                insert_order.append(f"t::b{i}")

            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    "SELECT job_uid FROM queue ORDER BY queue_position ASC"
                ).fetchall()
                total, distinct = conn.execute(
                    "SELECT COUNT(*), COUNT(DISTINCT queue_position) FROM queue"
                ).fetchone()
            assert [r[0] for r in rows] == list(reversed(insert_order))
            assert total == distinct == 60
        finally:
            backend_a.close()
            backend_b.close()

    def test_interleaved_shared_failure_uid_single_final_row(self, tmp_path):
        db_path = tmp_path / "interleave_failed.db"
        backend_a = SQLiteStateBackend(db_path)
        backend_b = SQLiteStateBackend(db_path)
        try:
            for i in range(20):
                backend_a.commit_job_failures([("t::u", {"round": i})])
                backend_b.commit_job_failures([("t::u", {"round": i})])

            failed = backend_a.load_failed()
            assert set(failed) == {"t::u"}
            assert failed["t::u"]["round"] == 19
        finally:
            backend_a.close()
            backend_b.close()
