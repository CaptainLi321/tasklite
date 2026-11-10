"""状态库接纳与拒绝保护契约。"""
import sqlite3

import pytest

from tasklite import TaskLiteError

from tasklite.backend import SQLiteStateBackend


def snapshot(path):
    with sqlite3.connect(path) as conn:
        return (conn.execute("PRAGMA user_version").fetchone(),
                conn.execute("PRAGMA journal_mode").fetchone(),
                list(conn.iterdump()))


@pytest.mark.parametrize("version", [0, 1, 2, 3, 4, 99])
def test_nonempty_incompatible_database_is_preserved(tmp_path, version):
    path = tmp_path / "original.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE records (value TEXT)")
        conn.execute("INSERT INTO records VALUES ('precious')")
        conn.execute(f"PRAGMA user_version={version}")
    before = snapshot(path)
    with pytest.raises(TaskLiteError) as caught:
        SQLiteStateBackend(path)
    assert snapshot(path) == before
    assert str(path) in str(caught.value)
    assert "5" in str(caught.value)
    assert "保留原库" in str(caught.value)


@pytest.mark.parametrize("setup", [
    "CREATE TABLE cursors (key TEXT, value TEXT)",
    "CREATE TABLE queue (job_uid TEXT, queue_position INTEGER, job_snapshot TEXT)",
    "CREATE TABLE http_snapshots (request_key TEXT)",
    "CREATE VIEW empty_view AS SELECT 1",
])
def test_version_zero_with_any_application_object_is_rejected(tmp_path, setup):
    path = tmp_path / "nonempty.db"
    with sqlite3.connect(path) as conn:
        conn.execute(setup)
    before = snapshot(path)
    with pytest.raises(TaskLiteError, match="version 0 非空库"):
        SQLiteStateBackend(path)
    assert snapshot(path) == before


@pytest.mark.parametrize("change", [
    "PRAGMA user_version=0",
    "PRAGMA user_version=3",
    "PRAGMA user_version=4",
    "DROP TABLE queue",
    "ALTER TABLE failed ADD COLUMN unexpected TEXT",
    "ALTER TABLE attempts RENAME COLUMN error_message TO error",
    "CREATE TABLE wall (job_uid TEXT)",
    "CREATE TABLE meta (key TEXT, value TEXT)",
])
def test_existing_schema_must_remain_complete_and_unmixed(tmp_path, change):
    path = tmp_path / "existing.db"
    backend = SQLiteStateBackend(path)
    backend.commit_job_failure("t::j", {"sentinel": "metadata"}, {"sentinel": "payload"})
    with sqlite3.connect(path) as conn:
        conn.execute(change)
    before = snapshot(path)
    with pytest.raises(TaskLiteError):
        SQLiteStateBackend(path)
    assert snapshot(path) == before


def test_same_named_view_is_rejected_without_repair(tmp_path):
    path = tmp_path / "view.db"
    SQLiteStateBackend(path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE queue")
        conn.execute("CREATE VIEW queue AS SELECT '' AS job_uid, 0 AS queue_position, '' AS job_snapshot")
    before = snapshot(path)
    with pytest.raises(TaskLiteError, match="必须为 table"):
        SQLiteStateBackend(path)
    assert snapshot(path) == before


@pytest.mark.parametrize("empty_file", [False, True])
def test_fresh_database_and_special_characters(tmp_path, empty_file):
    path = tmp_path / "空 %# space.db"
    if empty_file:
        path.touch()
    backend = SQLiteStateBackend(path)
    backend.set_cursor("error", "new_jobs")
    assert SQLiteStateBackend(path).load_cursors() == {"error": "new_jobs"}
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (5,)
        assert conn.execute("PRAGMA journal_mode").fetchone() == ("wal",)


def test_schema_four_allows_extra_user_table(tmp_path):
    path = tmp_path / "extra.db"
    SQLiteStateBackend(path)
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE user_data (value TEXT)")
        conn.execute("INSERT INTO user_data VALUES ('kept')")
    before = snapshot(path)
    SQLiteStateBackend(path)
    assert snapshot(path) == before


def test_corrupt_database_is_not_treated_as_empty(tmp_path):
    path = tmp_path / "corrupt.db"
    data = b"not a sqlite database"
    path.write_bytes(data)
    with pytest.raises(RuntimeError, match="无法只读确认"):
        SQLiteStateBackend(path)
    assert path.read_bytes() == data


def test_readonly_preflight_sees_committed_wal(tmp_path, monkeypatch):
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


def test_readonly_error_never_falls_back_to_write_connection(tmp_path, monkeypatch):
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
    with pytest.raises(RuntimeError, match="read access denied"):
        SQLiteStateBackend(path)
    assert len(calls) == 1
    assert calls[0][1]["uri"] is True
    assert "mode=ro" in calls[0][0]
    assert "immutable" not in calls[0][0]
    assert path.read_bytes() == b""


@pytest.mark.parametrize("when", ["after_preflight", "after_wal"])
def test_initialization_rechecks_before_application_writes(tmp_path, monkeypatch, when):
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


def test_schema_creation_failure_rolls_back_tables_indexes_and_version(tmp_path, monkeypatch):
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
    with pytest.raises(RuntimeError, match="index creation failed"):
        SQLiteStateBackend(path)
    with real_connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (0,)
        assert conn.execute("SELECT name FROM sqlite_master").fetchall() == []


def test_version_zero_cannot_hide_user_table_with_similar_internal_prefix(tmp_path):
    path = tmp_path / "foreign.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE sqliteXuser (value TEXT)")
    before = snapshot(path)
    with pytest.raises(TaskLiteError, match="非空库"):
        SQLiteStateBackend(path)
    assert snapshot(path) == before


def test_core_generated_extra_column_is_rejected(tmp_path):
    path = tmp_path / "generated.db"
    SQLiteStateBackend(path)
    with sqlite3.connect(path) as conn:
        conn.execute("ALTER TABLE queue ADD COLUMN generated TEXT GENERATED ALWAYS AS ('extra')")
    before = snapshot(path)
    with pytest.raises(TaskLiteError, match="Unsupported table shape"):
        SQLiteStateBackend(path)
    assert snapshot(path) == before


def test_user_trigger_name_cannot_hide_core_table(tmp_path):
    path = tmp_path / "trigger.db"
    SQLiteStateBackend(path)
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE events (value TEXT)")
        conn.execute("CREATE TRIGGER queue AFTER INSERT ON events BEGIN SELECT 1; END")
    before = snapshot(path)
    SQLiteStateBackend(path)
    assert snapshot(path) == before


@pytest.mark.parametrize("legacy", ["WALL", "META"])
def test_legacy_table_names_are_case_insensitive(tmp_path, legacy):
    path = tmp_path / "mixed.db"
    SQLiteStateBackend(path)
    with sqlite3.connect(path) as conn:
        conn.execute(f"CREATE TABLE {legacy} (value TEXT)")
    before = snapshot(path)
    with pytest.raises(TaskLiteError, match="异代"):
        SQLiteStateBackend(path)
    assert snapshot(path) == before
