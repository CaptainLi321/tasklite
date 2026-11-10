"""v2 SQLiteStateBackend 契约测试。

覆盖面：
- schema 初始化 / 空加载 / delta 提交 / 损坏行容灾 / 整表重写矩阵；
- WAL fail-loud 与 per-connection synchronous=FULL 的持久化不变式；
- 零旧库兼容（版本号与异代表形状 fail-loud 拒绝）；
- 失败档案 API（payload 快照面与 终态替换语义）；
- 游标入口校验；
- 并发混合写与交错队头入队的 seq 唯一性（失败写入事件计数由 attempts
  旁路轨迹承接，断言以档案行唯一为准）。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any
from unittest import mock

import pytest

from tasklite import TaskLiteError

from tasklite.backend import SQLiteStateBackend
from tasklite.models.job import Job
from tasklite.models.state import job_uid_from_snapshot

N_THREADS = 6
N_ROUNDS = 12


def _job_snapshot(job_id: str, **overrides) -> dict:
    data = Job("t", job_id).to_dict()
    data.update(overrides)
    return data


def _settle_success(
    backend, job_uid, result_metadata, *, attempt_no=1,
    settled_at="2026-05-01T00:00:05+00:00", **kwargs,
):
    snapshot = next(
        (job for job in backend.load_queue() if job_uid_from_snapshot(job) == job_uid), None
    )
    if snapshot is None:
        from tasklite import parse_job_uid

        task_type, job_id = parse_job_uid(job_uid)
        snapshot = Job(task_type, job_id).to_dict()
        backend.enqueue_jobs([snapshot])
    sequence = backend.mark_job_running(
        job_uid, execution_id=f"run_seed.{attempt_no}", run_id="run_seed",
        started_at="2026-05-01T00:00:00+00:00",
    )
    backend.commit_job_settlement(
        job_uid=job_uid, outcome="success", attempt_no=sequence,
        execution_id=f"run_seed.{attempt_no}", run_id="run_seed", settled_at=settled_at,
        started_at="2026-05-01T00:00:00+00:00", job_snapshot=snapshot,
        success_metadata=result_metadata, **kwargs,
    )


def _queue_uids(backend: SQLiteStateBackend) -> list[str]:
    return [job_uid_from_snapshot(j) for j in backend.load_queue()]


def _get_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {r[0] for r in rows}


class TestSchemaInit:
    """建表、版本号与幂等初始化。"""

    def test_init_creates_all_tables_and_version(self, tmp_path):
        db_path = tmp_path / "state.db"
        SQLiteStateBackend(db_path)
        with sqlite3.connect(db_path) as conn:
            tables = _get_tables(conn)
            version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert {"queue", "successes", "failed", "attempts", "cursors", "engine_metadata"} <= tables
        assert version == 6
        expected = {
            "queue": {"job_uid", "queue_position", "job_snapshot", "status", "run_id", "attempt_no", "started_at"},
            "successes": {"job_uid", "metadata"},
            "failed": {"job_uid", "metadata", "payload"},
            "attempts": {"attempt_id", "job_uid", "attempt_no", "run_id", "started_at", "settled_at",
                         "outcome", "error_message", "job_snapshot", "files", "result"},
            "job_sequences": {"job_uid", "last_attempt_no"},
            "authorizations": {"path", "target_type", "authorized_at"},
            "cursors": {"key", "value"},
            "engine_metadata": {"key", "value"},
        }
        with sqlite3.connect(db_path) as conn:
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

        b_second = SQLiteStateBackend(db_path)
        assert b_second.load_successes() == {"t::one": {"ok": True}}
        assert b_second.load_cursors() == {"c": "v"}
        assert _queue_uids(b_second) == ["t::queued"]

    def test_empty_loads_return_empty(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.load_successes() == {}
        assert backend.load_failed() == {}
        assert backend.load_failure_payloads() == {}
        assert backend.load_cursors() == {}
        assert backend.load_queue() == []
        assert backend.load_settled_attempts("t::missing") == []

    def test_unwritable_path_preserves_sqlite_error(self, tmp_path):
        db_path = tmp_path / "state.db"
        db_path.mkdir()  # 目录占位 → sqlite3.connect 失败
        with pytest.raises(sqlite3.OperationalError):
            SQLiteStateBackend(db_path)


class TestWALAndPragmas:
    """WAL fail-loud 与 per-connection synchronous=FULL。"""

    def test_wal_mode_persists_at_database_level(self, tmp_path):
        SQLiteStateBackend(tmp_path / "wal_state.db")
        with sqlite3.connect(tmp_path / "wal_state.db") as conn:
            (journal_mode,) = conn.execute("PRAGMA journal_mode").fetchone()
        assert journal_mode.lower() == "wal"

    def test_fresh_connection_has_synchronous_full(self, tmp_path):
        """持久化不变式：backend 连接每次都设 synchronous=FULL。

        NORMAL 下 WAL commit 不 fsync——enqueue 应答后断电时 INSERT 事务
        消失且无 job 可重跑（at-least-once 吸收不了），FULL 保证已应答
        事务落盘。
        """
        backend = SQLiteStateBackend(tmp_path / "pragma_state.db")
        with backend._get_conn() as conn:
            (sync,) = conn.execute("PRAGMA synchronous").fetchone()
        assert sync == 2  # FULL

    def test_init_fails_loud_when_wal_unavailable(self, tmp_path, monkeypatch):
        """WAL 无法生效（PRAGMA 静默降级）必须拒绝启动，不可带病运行。"""
        import tasklite.backend.sqlite_backend as sb

        real_connect = sqlite3.connect

        class NoWalConn:
            """模拟 WAL 无法生效的连接：journal_mode=WAL 返回 'delete'。"""

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

        class _StaticCursor:
            def __init__(self, row):
                self._row = row

            def fetchone(self):
                return self._row

            def __iter__(self):
                return iter(())

        monkeypatch.setattr(sb.sqlite3, "connect", NoWalConn)
        with pytest.raises(TaskLiteError, match="journal_mode=WAL could not be engaged"):
            SQLiteStateBackend(tmp_path / "nowal_state.db")


class TestLegacyDatabaseRejected:
    """零旧库兼容：旧版本号与异代表形状一律 fail-loud 拒绝。"""

    @staticmethod
    def _make_legacy_db(db_path, user_version: int) -> None:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE queue (idx INTEGER PRIMARY KEY AUTOINCREMENT, job_data TEXT)"
        )
        conn.execute(
            # 异形旧库 fixture：表名与列形均不符合 v2 schema，形状校验须拒识
            "CREATE TABLE failed_dlq (uid TEXT PRIMARY KEY, payload TEXT)"
        )
        conn.execute(f"PRAGMA user_version = {user_version}")
        conn.commit()
        conn.close()

    def test_v1_version_rejected(self, tmp_path):
        """v1 旧库（版本低于 v2 首代）拒绝启动，不写迁移路径。"""
        db_path = tmp_path / "legacy_state.db"
        self._make_legacy_db(db_path, user_version=2)
        with pytest.raises(TaskLiteError, match="Unsupported database schema version"):
            SQLiteStateBackend(db_path)

    def test_foreign_table_shape_rejected(self, tmp_path):
        """user_version=0 但表已存在且形状不符 → 形状校验 fail-loud。"""
        db_path = tmp_path / "foreign_shape.db"
        self._make_legacy_db(db_path, user_version=0)
        with pytest.raises(TaskLiteError, match="Unsupported table shape"):
            SQLiteStateBackend(db_path)

    def test_v1_cursors_shape_rejected(self, tmp_path):
        """v1 后期库（列名 key/value 的 cursors）即使版本号伪装也过不了形状校验。"""
        db_path = tmp_path / "legacy_cursors.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE cursors (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()
        conn.close()
        with pytest.raises(TaskLiteError, match="Unsupported table shape"):
            SQLiteStateBackend(db_path)


class TestSuccessSettlement:
    """成功 delta：同事务终结对应 attempts 行 + successes REPLACE + popped 删除 + failed 残行清理 + spawn 队头 + cursors。"""

    def test_atomic_writes_all_tables(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert _settle_success(
            backend,
            "t::source",
            {"status": "done", "bytes": 1024},
            spawned_job_snapshots=[_job_snapshot("child")],
            cursor_updates={"cursor_artist": "page_late"},
        ) is None

        assert backend.load_successes() == {"t::source": {"status": "done", "bytes": 1024}}
        assert _queue_uids(backend) == ["t::child"]
        assert backend.load_cursors() == {"cursor_artist": "page_late"}
        (rec,) = backend.load_settled_attempts("t::source")
        assert rec["outcome"] == "success"
        assert rec["settled_at"] == "2026-05-01T00:00:05+00:00"

    def test_removes_popped_uid_without_spawn(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_snapshot("target"), _job_snapshot("next")])

        assert _settle_success(backend, "t::target", {"ok": True}) is None
        assert backend.load_successes() == {"t::target": {"ok": True}}
        assert _queue_uids(backend) == ["t::next"]
        assert backend.load_cursors() == {}

    def test_multiple_cursor_updates(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        _settle_success(backend, "t::one", {}, cursor_updates={"a": "one", "b": "two", "c": "three"})
        assert backend.load_cursors() == {"a": "one", "b": "two", "c": "three"}

    def test_cursor_none_value_deletes(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        _settle_success(backend, "t::one", {}, cursor_updates={"k": "v"})
        _settle_success(backend, "t::two", {}, cursor_updates={"k": None})
        assert backend.load_cursors() == {}

    def test_same_uid_overwrites_wall(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        _settle_success(backend, "t::one", {"v": "first"}, attempt_no=1)
        _settle_success(backend, "t::one", {"v": "second"}, attempt_no=2)
        assert backend.load_successes() == {"t::one": {"v": "second"}}

    def test_deletes_failed_same_uid_row(self, tmp_path):
        """最终状态唯一：成功 commit 删失败档案同名残行（含快照）。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::one", {"error_message": "stale"}, payload={"k": 1})

        assert _settle_success(backend, "t::one", {"ok": True}) is None
        assert backend.load_failed() == {}
        assert backend.load_failure_payloads() == {}


class TestCommitJobFailure:
    """失败 delta：failed REPLACE + popped 删除 + successes 同名行删除。"""

    def test_records_archive_and_clears_queue(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_snapshot("doomed")])

        assert backend.commit_job_failure("t::doomed", {"error_message": "timeout"}) is True
        # v2 语义：失败 meta 精确等于传入（历史由 attempts 轨迹承接）
        assert backend.load_failed() == {"t::doomed": {"error_message": "timeout"}}
        assert backend.load_queue() == []

    def test_preserves_remaining_queue_order(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_snapshot(j) for j in ("first", "second", "third")])
        backend.commit_job_failure("t::second", {"error_message": "boom"})
        assert _queue_uids(backend) == ["t::first", "t::third"]

    def test_deletes_wall_same_uid_in_same_transaction(self, tmp_path):
        """重跑任务失败时 successes 旧成功记录作废（最终状态唯一、互删语义）。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        _settle_success(backend, "t::one", {"ok": True})

        assert backend.commit_job_failure("t::one", {"error_message": "boom"}) is True
        assert backend.load_successes() == {}
        assert set(backend.load_failed()) == {"t::one"}
        assert not (set(backend.load_successes()) & set(backend.load_failed()))

    def test_transaction_failure_propagates_and_preserves_queue(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_snapshot("alpha"), _job_snapshot("beta")])

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
                backend.commit_job_failure("t::alpha", {"error_message": "boom"})
        assert _queue_uids(backend) == ["t::alpha", "t::beta"]
        assert backend.load_failed() == {}


class TestCommitBulkFailure:
    """批量失败 delta：档案行 + 队列精准删除 + successes 批量清理。"""

    def test_records_multiple_and_clears_queue(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_snapshot("a"), _job_snapshot("b")])

        assert backend.commit_job_failures(
            [("t::a", {"err": "x"}), ("t::b", {"err": "y"})]
        ) is None
        assert backend.load_failed() == {"t::a": {"err": "x"}, "t::b": {"err": "y"}}
        assert backend.load_queue() == []

    def test_preserves_remaining_queue_order(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_snapshot(j) for j in ("dead_one", "dead_two", "keep_one", "keep_two")])
        backend.commit_job_failures([("t::dead_one", {"error_message": "x"}), ("t::dead_two", {"error_message": "y"})])
        assert _queue_uids(backend) == ["t::keep_one", "t::keep_two"]

    def test_deletes_wall_rows_same_transaction(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        _settle_success(backend, "t::one", {"ok": True})
        _settle_success(backend, "t::two", {"ok": True})
        assert backend.commit_job_failures([("t::one", {"error_message": "x"}), ("t::two", {"error_message": "y"})]) is None
        assert backend.load_successes() == {}

    def test_empty_list_is_noop(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.commit_job_failures([]) is None
        assert backend.load_failed() == {}


class TestSaveAndEnqueueQueue:
    """整表重写与增量入队。"""

    def test_save_queue_overwrites_and_clears(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_snapshot("one"), _job_snapshot("two")])
        backend.save_queue([_job_snapshot("three")])
        assert _queue_uids(backend) == ["t::three"]
        backend.save_queue([])
        assert backend.load_queue() == []

    def test_save_queue_preserves_order_large(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        jobs = [_job_snapshot(f"item_{i}") for i in range(500)]
        backend.save_queue(jobs)
        loaded = backend.load_queue()
        assert [j["job_id"] for j in loaded] == [f"item_{i}" for i in range(500)]

    def test_save_queue_db_error_raises(self, tmp_path, monkeypatch):
        backend = SQLiteStateBackend(tmp_path / "state.db")

        def _boom(*args, **kwargs):
            raise sqlite3.OperationalError("disk full")

        monkeypatch.setattr(sqlite3, "connect", _boom)
        with pytest.raises(sqlite3.OperationalError, match="disk full"):
            backend.save_queue([_job_snapshot("one")])

    def test_enqueue_front_back_and_dedup(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.enqueue_jobs([_job_snapshot("base")]) == ["t::base"]
        assert backend.enqueue_jobs([_job_snapshot("front")], front=True) == ["t::front"]
        assert backend.enqueue_jobs([_job_snapshot("back")]) == ["t::back"]
        # 队列已有与批次内重复都跳过
        assert backend.enqueue_jobs([_job_snapshot("base"), _job_snapshot("fresh"), _job_snapshot("fresh")]) == ["t::fresh"]
        assert _queue_uids(backend) == ["t::front", "t::base", "t::back", "t::fresh"]

    def test_delete_queue_uids_targeted(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.enqueue_jobs([_job_snapshot(j) for j in ("a", "b", "c")])
        assert backend.delete_queue_uids(["t::a", "t::c", "t::ghost"]) == 2
        assert _queue_uids(backend) == ["t::b"]

    def test_commit_skip_removes_uid_only(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.enqueue_jobs([_job_snapshot("stale"), _job_snapshot("live")])
        assert backend.commit_skip("t::stale") is None
        assert _queue_uids(backend) == ["t::live"]
        assert backend.load_successes() == {} and backend.load_failed() == {}


class TestCorruptionAndLocks:
    """损坏行与锁错误 fail-loud（不吞、不静默空返回）。"""

    def test_load_successes_corrupt_metadata_raises(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with sqlite3.connect(tmp_path / "state.db") as conn:
            conn.execute(
                "INSERT INTO successes (job_uid, metadata) VALUES (?, ?)",
                ("bad_uid", "not valid json{{{"),
            )
        with pytest.raises(json.JSONDecodeError, match="Expecting value"):
            backend.load_successes()

    def test_load_queue_corrupt_job_snapshot_raises(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with sqlite3.connect(tmp_path / "state.db") as conn:
            conn.execute(
                "INSERT INTO queue (job_uid, queue_position, job_snapshot) VALUES (?, ?, ?)",
                ("bad", 0, "not valid json{{{"),
            )
        with pytest.raises(json.JSONDecodeError, match="Expecting value"):
            backend.load_queue()

    def test_load_failed_corrupt_metadata_raises(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with sqlite3.connect(tmp_path / "state.db") as conn:
            conn.execute(
                "INSERT INTO failed (job_uid, metadata) VALUES (?, ?)",
                ("bad_uid", "not valid json{{{"),
            )
        with pytest.raises(json.JSONDecodeError, match="Expecting value"):
            backend.load_failed()

    def test_load_queue_locked_db_raises(self, tmp_path, monkeypatch):
        backend = SQLiteStateBackend(tmp_path / "state.db")

        def _locked(*args, **kwargs):
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(sqlite3, "connect", _locked)
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            backend.load_queue()

    def test_dropped_table_rejected_on_reopen(self, tmp_path):
        path = tmp_path / "state.db"
        SQLiteStateBackend(path)
        with sqlite3.connect(path) as conn:
            conn.execute("DROP TABLE queue")
        with pytest.raises(TaskLiteError, match="Unsupported table shape for 'queue'"):
            SQLiteStateBackend(path)


class TestReplaceQueueAtomic:
    """读-改-写收敛的整表替换（SQLite 腿写事务契约）。"""

    def test_compute_runs_inside_write_transaction(self, tmp_path):
        """compute 执行期间写锁必须已被本事务持有。

        用零 busy-timeout 的探针连接尝试 BEGIN IMMEDIATE：拿到锁 = 本事务
        未持锁（读-改-写存在无锁窗口，缺陷结构）；立即 busy 失败 = 窗口
        已被写事务消除。确定性判定，不依赖真实多进程竞速。
        """
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_snapshot("keep")])
        observed: dict[str, bool] = {}

        def compute(disk_q):
            probe = sqlite3.connect(backend.path, timeout=0.0)
            try:
                probe.execute("BEGIN IMMEDIATE")
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
        """读阶段失败（队列行损坏）→ 异常传播 + 整体回滚，compute 不获执行机会。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_snapshot("keep")])
        with sqlite3.connect(backend.path) as conn:
            conn.execute("UPDATE queue SET job_snapshot = 'not-json'")
            conn.commit()

        compute_called: list[Any] = []
        with pytest.raises(ValueError):
            backend.replace_queue_atomic(
                lambda disk_q: compute_called.append(disk_q) or []
            )

        assert compute_called == []
        with sqlite3.connect(backend.path) as conn:
            rows = conn.execute("SELECT job_snapshot FROM queue").fetchall()
        assert rows == [("not-json",)], "读失败必须回滚，磁盘行原样保留"


class TestSqliteFailureArchive:
    """失败档案 API：append 幂等与 payload 快照面。"""

    def test_failure_archive_overwrites_same_uid(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::dup", {"first": 1})
        backend.commit_job_failure("t::dup", {"second": 2})
        assert backend.load_failed() == {"t::dup": {"second": 2}}

    def test_failure_archive_empty_metadata(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::nil", {})
        assert backend.load_failed() == {"t::nil": {}}

    def test_payload_snapshot_roundtrip_latest_wins(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.commit_job_failure("t::one", {"error_message": "first"}, payload={"v": 1}) is True
        assert backend.load_failure_payloads() == {"t::one": {"v": 1}}
        assert backend.commit_job_failure("t::one", {"error_message": "second"}, payload={"v": 2}) is True
        assert backend.load_failure_payloads() == {"t::one": {"v": 2}}

    def test_none_payload_keeps_previous_snapshot(self, tmp_path):
        """无 payload 的写入路径（append/覆盖写）不得抹掉既有快照。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::one", {"error_message": "first"}, payload={"k": 1})
        backend.commit_job_failure("t::one", {"error_message": "second"})
        assert backend.load_failure_payloads() == {"t::one": {"k": 1}}

    def test_no_snapshot_entry_absent_from_payload_view(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::other", {"error_message": "x"})
        assert backend.load_failure_payloads() == {}

    def test_unserializable_payload_propagates_and_rolls_back(self, tmp_path):
        """payload 序列化故障原样传播，不留下部分档案。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with pytest.raises(TypeError):
            backend.commit_job_failure("t::one", {"error_message": "x"}, payload={"bad": object()})
        assert backend.load_failed() == {}

    def test_delete_failed_clears_meta_and_snapshot(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::one", {"error_message": "x"}, payload={"k": 1})
        backend.commit_job_failure("t::two", {"error_message": "y"})
        assert backend.delete_failed(["t::one", "t::ghost"]) == 1
        assert backend.load_failed() == {"t::two": {"error_message": "y"}}
        assert backend.load_failure_payloads() == {}

    def test_delete_wall_targeted(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        _settle_success(backend, "t::one", {"ok": True})
        _settle_success(backend, "t::two", {"ok": True})
        assert backend.delete_successes(["t::one", "t::ghost"]) == 1
        assert set(backend.load_successes()) == {"t::two"}


class TestSeedWallAndCursor:
    """种子化：successes/failed 互斥 + cursor 入口校验。"""

    def test_apply_manual_status_and_clear_history(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        snap = {"job_uid": "t::fresh_one", "task_type": "t", "job_id": "fresh_one", "payload": {}}
        backend.apply_manual_status([("t::fresh_one", "succeeded", snap, None)])
        assert "t::fresh_one" in backend.load_successes()
        deleted = backend.clear_history("t::fresh_one", records=["successes"])
        assert deleted == 1
        assert "t::fresh_one" not in backend.load_successes()

    def test_set_cursor_valid_roundtrip_upsert_and_delete(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.set_cursor("k", "v_one")
        assert backend.get_cursor("k") == "v_one"
        backend.set_cursor("k", "v_two")
        assert backend.get_cursor("k") == "v_two"
        assert backend.load_cursors() == {"k": "v_two"}
        backend.set_cursor("k", None)
        assert backend.get_cursor("k") is None
        assert backend.load_cursors() == {}
        backend.set_cursor("k", "")
        assert backend.get_cursor("k") == ""
        assert backend.load_cursors() == {"k": ""}

    @pytest.mark.parametrize("key,value", [(None, "v"), ("k", 123), ("", "v")])
    def test_set_cursor_rejects_bad_types_zero_write(self, tmp_path, key, value):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with pytest.raises(TypeError):
            backend.set_cursor(key, value)
        assert backend.load_cursors() == {}


class TestConcurrentMixedWrites:
    """多线程（每线程独立连接）混合写同一 db 文件的竞态回归。

    传统 sqlite3 隔离模式只为 DML 隐式开事务，SELECT 在 autocommit 下
    逐条取快照——seq 分配若不在 ``BEGIN IMMEDIATE`` 写事务内，多连接会
    基于同一份旧 min/max 各算各的，落盘重复 seq，队头保序契约被破坏。
    """

    @staticmethod
    def _worker(tid: int, backend: SQLiteStateBackend,
                barrier: threading.Barrier, errors: list) -> None:
        try:
            for k in range(N_ROUNDS):
                # 每轮起点同步：让各线程的读窗口高度重叠，最大化并发
                # 读到同一 min/max(seq) 的竞争概率。
                barrier.wait(timeout=60)
                # 路径一：增量入队（front/back 交错，两个分配方向都覆盖）
                backend.enqueue_jobs(
                    [{"task_type": "q", "job_id": f"e{tid}-{k}"}],
                    front=(k % 2 == 0),
                )
                # 路径二：成功 commit（同事务追加 Attempt + successes 写 + popped 删除 + spawned 队头插入 + cursor）
                _settle_success(
                    backend,
                    f"c::{tid}-{k}",
                    {"ok": True},
                    spawned_job_snapshots=[{"task_type": "s", "job_id": f"s{tid}-{k}"}],
                    cursor_updates={f"cur_{tid}": str(k)},
                )
                # 路径三：共享 uid 的失败档案并发写（按 uid REPLACE、快照保留）
                backend.commit_job_failure("shared::uid", {"error_message": "boom"})
                # 路径四：失败 commit（失败档案写入的另一个并发入口）
                if not backend.commit_job_failure(f"f::{tid}-{k}", {"error_message": "x"}):
                    errors.append(f"commit_job_failure returned False (t{tid} k{k})")
        except Exception as e:
            errors.append(f"thread {tid} failed: {e!r}")

    def test_concurrent_mixed_writes_seq_unique_archive_exact(self, tmp_path):
        db_path = tmp_path / "concurrent_state.db"
        # 主线程先完成初始化与建表：N 个线程并发跑 _init_db 的
        # journal_mode 切换（WAL 生效需短暂独占）可能互相拿不到锁。
        backends = [SQLiteStateBackend(db_path) for _ in range(N_THREADS)]
        backends[0].enqueue_jobs([{"task_type": "q", "job_id": "seed"}])

        barrier = threading.Barrier(N_THREADS)
        errors: list = []
        threads = [
            threading.Thread(
                target=self._worker, args=(t, backends[t], barrier, errors),
                name=f"concurrency-writer-{t}",
            )
            for t in range(N_THREADS)
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=120)
        assert not any(th.is_alive() for th in threads), (
            "工作线程未在时限内退出（疑似锁等待超时/死锁）"
        )
        assert errors == [], f"并发写路径出现异常或失败返回: {errors}"

        with sqlite3.connect(db_path) as conn:
            total, distinct = conn.execute(
                "SELECT COUNT(*), COUNT(DISTINCT queue_position) FROM queue"
            ).fetchone()
        # seq 全局唯一：重复 seq 直接破坏队头保序契约
        assert total == distinct, (
            f"seq 重复：queue 共 {total} 行，仅 {distinct} 个唯一 seq"
        )
        # 行数精确：初始 1 + 每线程每轮 enqueue 1 行 + spawned 1 行
        assert total == 1 + 2 * N_THREADS * N_ROUNDS

        # 失败档案：共享 uid 按唯一终态收敛为一行，meta 为最后一次写入
        failed = backends[0].load_failed()
        assert failed["shared::uid"] == {"error_message": "boom"}
        assert len(failed) == N_THREADS * N_ROUNDS + 1

        # successes 全部落盘（每个 c:: uid 一次成功 commit）
        successes = backends[0].load_successes()
        assert len(successes) == N_THREADS * N_ROUNDS


class TestInterleavedTwoConnections:
    """两连接交错执行的确定性护栏：不依赖时序，验证交错后的最终不变式。"""

    def test_interleaved_front_enqueue_seq_unique_and_ordered(self, tmp_path):
        db_path = tmp_path / "interleave_state.db"
        backend_a = SQLiteStateBackend(db_path)
        backend_b = SQLiteStateBackend(db_path)

        insert_order: list[str] = []
        for i in range(30):
            backend_a.enqueue_jobs(
                [{"task_type": "t", "job_id": f"a{i}"}], front=True
            )
            insert_order.append(f"t::a{i}")
            backend_b.enqueue_jobs(
                [{"task_type": "t", "job_id": f"b{i}"}], front=True
            )
            insert_order.append(f"t::b{i}")

        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT job_uid FROM queue ORDER BY queue_position ASC"
            ).fetchall()
            total, distinct = conn.execute(
                "SELECT COUNT(*), COUNT(DISTINCT queue_position) FROM queue"
            ).fetchone()
        # 队头保序契约：后插入的 seq 更小，seq 升序 = 插入逆序
        assert [r[0] for r in rows] == list(reversed(insert_order))
        assert total == distinct == 60

    def test_interleaved_shared_failure_uid_single_final_row(self, tmp_path):
        """两连接交错写同一 uid 失败档案：按 uid 唯一终态收敛为一行。"""
        db_path = tmp_path / "interleave_failed.db"
        backend_a = SQLiteStateBackend(db_path)
        backend_b = SQLiteStateBackend(db_path)

        for i in range(20):
            backend_a.commit_job_failure("t::u", {"round": i})
            backend_b.commit_job_failure("t::u", {"round": i})

        failed = backend_a.load_failed()
        assert set(failed) == {"t::u"}
        assert failed["t::u"]["round"] == 19


@pytest.mark.parametrize(("operation", "table"), [
    ("INSERT", "attempts"), ("INSERT", "successes"), ("DELETE", "failed"),
    ("DELETE", "queue"), ("INSERT", "queue"), ("INSERT", "cursors"), ("DELETE", "cursors"),
])
def test_settlement_write_fault_preserves_all_tables_and_original_error(tmp_path, operation, table):
    backend = SQLiteStateBackend(tmp_path / "state.db")
    backend.commit_job_failure("t::parent", {"old": True}, {"input": 1})
    backend.enqueue_jobs([_job_snapshot("parent"), _job_snapshot("other")])
    backend.set_cursor("remove", "old")
    backend.mark_job_running("t::parent", execution_id="run.1", run_id="run", started_at="start")
    with sqlite3.connect(backend.path) as conn:
        conn.execute(f"CREATE TRIGGER fail_write BEFORE {operation} ON {table} "
                     "BEGIN SELECT RAISE(ABORT, 'synthetic settlement failure'); END")
        before = list(conn.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="synthetic settlement failure"):
        backend.commit_job_settlement(job_uid="t::parent", outcome="success", attempt_no=1,
                                      execution_id="run.1", run_id="run", settled_at="finish",
                                      success_metadata={"new": True}, spawned_job_snapshots=[_job_snapshot("child")],
                                      cursor_updates={"set": "new", "remove": None})
    with sqlite3.connect(backend.path) as conn:
        assert list(conn.iterdump()) == before
    assert backend.load_settled_attempts("t::parent") == []
    assert backend.get_attempt_sequence("t::parent") == 1


def test_settled_history_persists_across_instances(tmp_path):
    path = tmp_path / "state.db"
    backend = SQLiteStateBackend(path)
    _settle_success(backend, "t::parent", {"ok": True})
    before = backend.load_settled_attempts("t::parent")
    reopened = SQLiteStateBackend(path)
    assert reopened.load_settled_attempts("t::parent") == before
    assert reopened.load_latest_succeeded_finished_at("t::parent") == "2026-05-01T00:00:05+00:00"
