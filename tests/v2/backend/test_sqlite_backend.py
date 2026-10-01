"""v2 SQLiteStateBackend 契约测试。

覆盖面：
- schema 初始化 / 空加载 / delta 提交 / 损坏行容灾 / 整表重写矩阵；
- WAL fail-loud 与 per-connection synchronous=FULL 的持久化不变式；
- 零旧库兼容（版本号与异代表形状 fail-loud 拒绝）；
- 失败档案 API（payload 快照面与 append 幂等语义）；
- seed/cursor 入口校验；
- 并发混合写与交错队头入队的 seq 唯一性（失败写入事件计数由 attempts
  旁路轨迹承接，断言以档案行唯一为准）。
"""

from __future__ import annotations

import sqlite3
import threading
from typing import Any
from unittest import mock

import pytest

from tasklite.v2.backend import SQLiteStateBackend
from tasklite.v2.models.attempt import ATTEMPT_RUNNING, AttemptRecord
from tasklite.v2.models.job import Job
from tasklite.v2.models.state import uid_from_job_dict

N_THREADS = 6
N_ROUNDS = 12


def _job_dict(job_id: str, **overrides) -> dict:
    data = Job("t", job_id).to_dict()
    data.update(overrides)
    return data


def _running_attempt(job_uid: str, attempt_no: int = 1, run_id: str = "run_seed") -> AttemptRecord:
    return AttemptRecord(
        job_uid=job_uid,
        activation_no=1,
        attempt_no=attempt_no,
        incarnation=f"{run_id}.{attempt_no}",
        run_id=run_id,
        started_at="2026-05-01T00:00:00+00:00",
        outcome="running",
    )


def _queue_uids(backend: SQLiteStateBackend) -> list[str]:
    return [uid_from_job_dict(j) for j in backend.load_queue()]


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
        assert {"queue", "wall", "failed", "attempts", "cursors", "meta"} <= tables
        assert version == 3

    def test_init_is_idempotent_and_preserves_data(self, tmp_path):
        db_path = tmp_path / "state.db"
        b_first = SQLiteStateBackend(db_path)
        b_first.commit_job_success("t::one", {"ok": True}, cursor_updates={"c": "v"})
        b_first.enqueue_jobs([_job_dict("queued")])

        b_second = SQLiteStateBackend(db_path)
        assert b_second.load_wall() == {"t::one": {"ok": True}}
        assert b_second.load_cursors() == {"c": "v"}
        assert _queue_uids(b_second) == ["t::queued"]

    def test_empty_loads_return_empty(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.load_wall() == {}
        assert backend.load_failed() == {}
        assert backend.load_failed_payloads() == {}
        assert backend.load_cursors() == {}
        assert backend.load_queue() == []
        assert backend.load_attempts("t::missing") == []

    def test_unwritable_path_raises_runtime_error(self, tmp_path):
        db_path = tmp_path / "state.db"
        db_path.mkdir()  # 目录占位 → sqlite3.connect 失败
        with pytest.raises(RuntimeError, match="SQLite backend initialization failed"):
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
        import tasklite.v2.backend.sqlite_backend as sb

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
        with pytest.raises(RuntimeError, match="journal_mode=WAL could not be engaged"):
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
        with pytest.raises(RuntimeError, match="Unsupported database schema version"):
            SQLiteStateBackend(db_path)

    def test_foreign_table_shape_rejected(self, tmp_path):
        """user_version=0 但表已存在且形状不符 → 形状校验 fail-loud。"""
        db_path = tmp_path / "foreign_shape.db"
        self._make_legacy_db(db_path, user_version=0)
        with pytest.raises(RuntimeError, match="Unsupported table shape"):
            SQLiteStateBackend(db_path)

    def test_v1_cursors_shape_rejected(self, tmp_path):
        """v1 后期库（列名 key/value 的 cursors）即使版本号伪装也过不了形状校验。"""
        db_path = tmp_path / "legacy_cursors.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE cursors (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()
        conn.close()
        with pytest.raises(RuntimeError, match="Unsupported table shape for 'cursors'"):
            SQLiteStateBackend(db_path)


class TestCommitJobSuccess:
    """成功 delta：wall REPLACE + popped 删除 + failed 残行清理 + spawn 队头 + cursors。"""

    def test_atomic_writes_all_tables(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.commit_job_success(
            "t::source",
            {"status": "done", "bytes": 1024},
            spawned_jobs=[_job_dict("child")],
            cursor_updates={"cursor_artist": "page_late"},
        ) is True

        assert backend.load_wall() == {"t::source": {"status": "done", "bytes": 1024}}
        assert _queue_uids(backend) == ["t::child"]
        assert backend.load_cursors() == {"cursor_artist": "page_late"}

    def test_removes_popped_uid_without_spawn(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_dict("target"), _job_dict("next")])

        assert backend.commit_job_success("t::target", {"ok": True}) is True
        assert backend.load_wall() == {"t::target": {"ok": True}}
        assert _queue_uids(backend) == ["t::next"]
        assert backend.load_cursors() == {}

    def test_multiple_cursor_updates(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success("t::one", {}, cursor_updates={"a": "one", "b": "two", "c": "three"})
        assert backend.load_cursors() == {"a": "one", "b": "two", "c": "three"}

    def test_cursor_none_value_deletes(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success("t::one", {}, cursor_updates={"k": "v"})
        backend.commit_job_success("t::two", {}, cursor_updates={"k": None})
        assert backend.load_cursors() == {}

    def test_same_uid_overwrites_wall(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success("t::one", {"v": "first"})
        backend.commit_job_success("t::one", {"v": "second"})
        assert backend.load_wall() == {"t::one": {"v": "second"}}

    def test_deletes_failed_same_uid_row(self, tmp_path):
        """最终状态唯一：成功 commit 删失败档案同名残行（含快照）。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::one", {"error": "stale"}, job_payload={"k": 1})

        assert backend.commit_job_success("t::one", {"ok": True}) is True
        assert backend.load_failed() == {}
        assert backend.load_failed_payloads() == {}

    def test_spawn_conflict_returns_false_and_preserves_disk(self, tmp_path):
        """spawned uid 撞磁盘既有行（漂移窗口）→ False + 全量回滚。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        job_a = _job_dict("alpha")
        job_b = _job_dict("beta")
        backend.save_queue([job_a, job_b])

        assert backend.commit_job_success("t::alpha", {"ok": True}, spawned_jobs=[job_b]) is False
        # 回滚：wall 未写、popped 未删、队列原样
        assert backend.load_wall() == {}
        assert _queue_uids(backend) == ["t::alpha", "t::beta"]

    def test_transaction_failure_returns_false_and_preserves_queue(self, tmp_path):
        """SQLite 事务抛异常（如 disk full）→ False + on-disk 队列不变。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_dict("alpha"), _job_dict("beta")])

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
            result = backend.commit_job_success("t::alpha", {"ok": True})

        assert result is False
        assert _queue_uids(backend) == ["t::alpha", "t::beta"]


class TestCommitJobFailure:
    """失败 delta：failed REPLACE + popped 删除 + wall 同名行删除。"""

    def test_records_archive_and_clears_queue(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_dict("doomed")])

        assert backend.commit_job_failure("t::doomed", {"error": "timeout"}) is True
        # v2 语义：失败 meta 精确等于传入（历史由 attempts 轨迹承接）
        assert backend.load_failed() == {"t::doomed": {"error": "timeout"}}
        assert backend.load_queue() == []

    def test_preserves_remaining_queue_order(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_dict(j) for j in ("first", "second", "third")])
        backend.commit_job_failure("t::second", {"error": "boom"})
        assert _queue_uids(backend) == ["t::first", "t::third"]

    def test_deletes_wall_same_uid_in_same_transaction(self, tmp_path):
        """重跑任务失败时 wall 旧成功记录作废（最终状态唯一、互删语义）。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success("t::one", {"ok": True})

        assert backend.commit_job_failure("t::one", {"error": "boom"}) is True
        assert backend.load_wall() == {}
        assert set(backend.load_failed()) == {"t::one"}
        assert not (set(backend.load_wall()) & set(backend.load_failed()))

    def test_transaction_failure_returns_false_and_preserves_queue(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_dict("alpha"), _job_dict("beta")])

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
            result = backend.commit_job_failure("t::alpha", {"error": "boom"})

        assert result is False
        assert _queue_uids(backend) == ["t::alpha", "t::beta"]
        assert backend.load_failed() == {}


class TestCommitBulkFailure:
    """批量失败 delta：档案行 + 队列精准删除 + wall 批量清理。"""

    def test_records_multiple_and_clears_queue(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_dict("a"), _job_dict("b")])

        assert backend.commit_bulk_failure(
            [("t::a", {"err": "x"}), ("t::b", {"err": "y"})]
        ) is True
        assert backend.load_failed() == {"t::a": {"err": "x"}, "t::b": {"err": "y"}}
        assert backend.load_queue() == []

    def test_preserves_remaining_queue_order(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_dict(j) for j in ("dead_one", "dead_two", "keep_one", "keep_two")])
        backend.commit_bulk_failure([("t::dead_one", {"error": "x"}), ("t::dead_two", {"error": "y"})])
        assert _queue_uids(backend) == ["t::keep_one", "t::keep_two"]

    def test_deletes_wall_rows_same_transaction(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success("t::one", {"ok": True})
        backend.commit_job_success("t::two", {"ok": True})
        assert backend.commit_bulk_failure([("t::one", {"error": "x"}), ("t::two", {"error": "y"})]) is True
        assert backend.load_wall() == {}

    def test_empty_list_is_noop(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.commit_bulk_failure([]) is True
        assert backend.load_failed() == {}


class TestCommitRetry:
    """重试 delta：同 uid DELETE+INSERT、front/back、冲突 False。"""

    def test_same_uid_reinsert_with_updated_fields(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_dict("retry_one")])

        requeued = _job_dict("retry_one", attempt_no=2)
        assert backend.commit_retry("t::retry_one", requeued) is True
        queue = backend.load_queue()
        assert len(queue) == 1
        assert queue[0]["attempt_no"] == 2

    def test_conflicting_uid_returns_false_and_preserves_disk(self, tmp_path):
        """requeued uid 撞队列既有行 → 普通 INSERT 冲突 → False，磁盘原样。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        job_a = _job_dict("alpha")
        job_b = _job_dict("beta")
        backend.save_queue([job_a, job_b])

        assert backend.commit_retry("t::ghost", job_a) is False
        assert backend.load_queue() == [job_a, job_b]

    def test_front_and_back_placement(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.enqueue_jobs([_job_dict("alpha"), _job_dict("beta")])

        assert backend.commit_retry("t::alpha", _job_dict("alpha", attempt_no=2), front=True) is True
        assert _queue_uids(backend) == ["t::alpha", "t::beta"]
        assert backend.commit_retry("t::alpha", _job_dict("alpha", attempt_no=3), front=False) is True
        assert _queue_uids(backend) == ["t::beta", "t::alpha"]


class TestSaveAndEnqueueQueue:
    """整表重写与增量入队。"""

    def test_save_queue_overwrites_and_clears(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_dict("one"), _job_dict("two")])
        backend.save_queue([_job_dict("three")])
        assert _queue_uids(backend) == ["t::three"]
        backend.save_queue([])
        assert backend.load_queue() == []

    def test_save_queue_preserves_order_large(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        jobs = [_job_dict(f"item_{i}") for i in range(500)]
        backend.save_queue(jobs)
        loaded = backend.load_queue()
        assert [j["job_id"] for j in loaded] == [f"item_{i}" for i in range(500)]

    def test_save_queue_db_error_raises(self, tmp_path, monkeypatch):
        backend = SQLiteStateBackend(tmp_path / "state.db")

        def _boom(*args, **kwargs):
            raise sqlite3.OperationalError("disk full")

        monkeypatch.setattr(sqlite3, "connect", _boom)
        with pytest.raises(sqlite3.OperationalError, match="disk full"):
            backend.save_queue([_job_dict("one")])

    def test_enqueue_front_back_and_dedup(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.enqueue_jobs([_job_dict("base")]) == ["t::base"]
        assert backend.enqueue_jobs([_job_dict("front")], front=True) == ["t::front"]
        assert backend.enqueue_jobs([_job_dict("back")]) == ["t::back"]
        # 队列已有与批次内重复都跳过
        assert backend.enqueue_jobs([_job_dict("base"), _job_dict("fresh"), _job_dict("fresh")]) == ["t::fresh"]
        assert _queue_uids(backend) == ["t::front", "t::base", "t::back", "t::fresh"]

    def test_delete_queue_uids_targeted(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.enqueue_jobs([_job_dict(j) for j in ("a", "b", "c")])
        assert backend.delete_queue_uids(["t::a", "t::c", "t::ghost"]) == 2
        assert _queue_uids(backend) == ["t::b"]

    def test_commit_skip_removes_uid_only(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.enqueue_jobs([_job_dict("stale"), _job_dict("live")])
        assert backend.commit_skip("t::stale") is True
        assert _queue_uids(backend) == ["t::live"]
        assert backend.load_wall() == {} and backend.load_failed() == {}


class TestCorruptionAndLocks:
    """损坏行与锁错误 fail-loud（不吞、不静默空返回）。"""

    def test_load_wall_corrupt_payload_raises(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with sqlite3.connect(tmp_path / "state.db") as conn:
            conn.execute(
                "INSERT INTO wall (uid, payload) VALUES (?, ?)",
                ("bad_uid", "not valid json{{{"),
            )
        with pytest.raises(RuntimeError, match="Corrupted wall payload"):
            backend.load_wall()

    def test_load_queue_corrupt_payload_raises(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with sqlite3.connect(tmp_path / "state.db") as conn:
            conn.execute(
                "INSERT INTO queue (uid, seq, job_data) VALUES (?, ?, ?)",
                ("bad", 0, "not valid json{{{"),
            )
        with pytest.raises(RuntimeError, match="Corrupted queue payload"):
            backend.load_queue()

    def test_load_failed_corrupt_payload_raises(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with sqlite3.connect(tmp_path / "state.db") as conn:
            conn.execute(
                "INSERT INTO failed (uid, payload) VALUES (?, ?)",
                ("bad_uid", "not valid json{{{"),
            )
        with pytest.raises(RuntimeError, match="Corrupted failed payload"):
            backend.load_failed()

    def test_load_queue_locked_db_raises(self, tmp_path, monkeypatch):
        backend = SQLiteStateBackend(tmp_path / "state.db")

        def _locked(*args, **kwargs):
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(sqlite3, "connect", _locked)
        with pytest.raises(RuntimeError, match="Failed to load queue"):
            backend.load_queue()

    def test_dropped_table_recreated_on_reopen(self, tmp_path):
        db_path = tmp_path / "state.db"
        SQLiteStateBackend(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.execute("DROP TABLE queue")
        backend_reopened = SQLiteStateBackend(db_path)
        assert backend_reopened.load_queue() == []


class TestReplaceQueueAtomic:
    """读-改-写收敛的整表替换（SQLite 腿写事务契约）。"""

    def test_compute_runs_inside_write_transaction(self, tmp_path):
        """compute 执行期间写锁必须已被本事务持有。

        用零 busy-timeout 的探针连接尝试 BEGIN IMMEDIATE：拿到锁 = 本事务
        未持锁（读-改-写存在无锁窗口，缺陷结构）；立即 busy 失败 = 窗口
        已被写事务消除。确定性判定，不依赖真实多进程竞速。
        """
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([_job_dict("keep")])
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
        backend.save_queue([_job_dict("keep")])
        with sqlite3.connect(backend.path) as conn:
            conn.execute("UPDATE queue SET job_data = 'not-json'")
            conn.commit()

        compute_called: list[Any] = []
        with pytest.raises(ValueError):
            backend.replace_queue_atomic(
                lambda disk_q: compute_called.append(disk_q) or []
            )

        assert compute_called == []
        with sqlite3.connect(backend.path) as conn:
            rows = conn.execute("SELECT job_data FROM queue").fetchall()
        assert rows == [("not-json",)], "读失败必须回滚，磁盘行原样保留"


class TestSqliteFailureArchive:
    """失败档案 API：append 幂等与 payload 快照面。"""

    def test_append_failed_overwrites_same_uid(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.append_failed("t::dup", {"first": 1})
        backend.append_failed("t::dup", {"second": 2})
        assert backend.load_failed() == {"t::dup": {"second": 2}}

    def test_append_failed_none_payload_stores_empty(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.append_failed("t::nil", None)
        assert backend.load_failed() == {"t::nil": {}}

    def test_payload_snapshot_roundtrip_latest_wins(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.commit_job_failure("t::one", {"error": "first"}, job_payload={"v": 1}) is True
        assert backend.load_failed_payloads() == {"t::one": {"v": 1}}
        assert backend.commit_job_failure("t::one", {"error": "second"}, job_payload={"v": 2}) is True
        assert backend.load_failed_payloads() == {"t::one": {"v": 2}}

    def test_none_payload_keeps_previous_snapshot(self, tmp_path):
        """无 payload 的写入路径（append/覆盖写）不得抹掉既有快照。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::one", {"error": "first"}, job_payload={"k": 1})
        backend.append_failed("t::one", {"error": "second"})
        assert backend.load_failed_payloads() == {"t::one": {"k": 1}}

    def test_no_snapshot_entry_absent_from_payload_view(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.append_failed("t::other", {"error": "x"})
        assert backend.load_failed_payloads() == {}

    def test_unserializable_payload_returns_false(self, tmp_path):
        """payload 不可序列化 → 序列化异常回滚 → False（不落半行）。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.commit_job_failure("t::one", {"error": "x"}, job_payload={"bad": object()}) is False
        assert backend.load_failed() == {}

    def test_delete_failed_clears_meta_and_snapshot(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::one", {"error": "x"}, job_payload={"k": 1})
        backend.append_failed("t::two", {"error": "y"})
        assert backend.delete_failed(["t::one", "t::ghost"]) == 1
        assert backend.load_failed() == {"t::two": {"error": "y"}}
        assert backend.load_failed_payloads() == {}

    def test_delete_wall_targeted(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_success("t::one", {"ok": True})
        backend.commit_job_success("t::two", {"ok": True})
        assert backend.delete_wall(["t::one", "t::ghost"]) == 1
        assert set(backend.load_wall()) == {"t::two"}


class TestSqliteAttemptTrajectory:
    """attempts 轨迹：插入、一次收尾、查询、跨实例持久、旁路不可触碰。"""

    def test_append_assigns_increasing_autoincrement_ids(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        first = backend.append_attempt(_running_attempt("t::one"))
        second = backend.append_attempt(_running_attempt("t::one", attempt_no=2))
        third = backend.append_attempt(_running_attempt("t::other"))
        assert first < second < third

    def test_append_rejects_non_dispatch_shape(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with pytest.raises(ValueError, match="fresh running row"):
            backend.append_attempt(
                AttemptRecord(
                    job_uid="t::one", activation_no=1, attempt_no=1,
                    incarnation="run_seed.1", run_id="run_seed",
                    started_at="2026-05-01T00:00:00+00:00",
                    outcome="skipped",
                )
            )
        assert backend.load_attempts("t::one") == []

    def test_update_finalizes_running_row_only_once(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        attempt_id = backend.append_attempt(_running_attempt("t::one"))

        assert backend.update_attempt(
            attempt_id, outcome="failed",
            finished_at="2026-05-01T00:00:05+00:00", error="boom",
        ) is True
        (rec,) = backend.load_attempts("t::one")
        assert rec.outcome == "failed"
        assert rec.error == "boom"
        # append-only：身份列保持插入原值，仅收尾三列被写
        assert rec.incarnation == "run_seed.1"
        assert rec.started_at == "2026-05-01T00:00:00+00:00"

        with pytest.raises(ValueError, match="already finalized"):
            backend.update_attempt(
                attempt_id, outcome="succeeded",
                finished_at="2026-05-01T00:00:09+00:00",
            )

    def test_update_unknown_id_returns_false(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.update_attempt(
            999, outcome="skipped", finished_at="2026-05-01T00:00:05+00:00"
        ) is False

    def test_update_rejects_running_outcome(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        attempt_id = backend.append_attempt(_running_attempt("t::one"))
        with pytest.raises(ValueError, match="outcome must be one of"):
            backend.update_attempt(
                attempt_id, outcome="running",
                finished_at="2026-05-01T00:00:05+00:00",
            )
        (rec,) = backend.load_attempts("t::one")
        assert rec.outcome == ATTEMPT_RUNNING

    def test_load_attempts_filters_by_uid_in_id_order(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.append_attempt(_running_attempt("t::one"))
        backend.append_attempt(_running_attempt("t::other"))
        backend.append_attempt(_running_attempt("t::one", attempt_no=2))
        records = backend.load_attempts("t::one")
        assert [r.attempt_no for r in records] == [1, 2]

    def test_attempts_persist_across_instances(self, tmp_path):
        db_path = tmp_path / "state.db"
        b_first = SQLiteStateBackend(db_path)
        attempt_id = b_first.append_attempt(_running_attempt("t::one"))
        b_first.update_attempt(
            attempt_id, outcome="succeeded", finished_at="2026-05-01T00:00:05+00:00"
        )

        b_second = SQLiteStateBackend(db_path)
        (rec,) = b_second.load_attempts("t::one")
        assert rec.outcome == "succeeded"

    def test_attempts_untouched_by_terminal_commits(self, tmp_path):
        """旁路观测面：终态提交（含同 uid 失败）不触碰 attempts 行。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.enqueue_jobs([_job_dict("one")])
        attempt_id = backend.append_attempt(_running_attempt("t::one"))
        backend.update_attempt(
            attempt_id, outcome="failed",
            finished_at="2026-05-01T00:00:05+00:00", error="boom",
        )

        backend.commit_job_failure("t::one", {"error": "boom"}, job_payload={"k": 1})
        (rec,) = backend.load_attempts("t::one")
        assert rec.outcome == "failed"
        assert rec.error == "boom"

    def test_raw_rows_only_grow_never_rewrite_history(self, tmp_path):
        """append-only 硬断言：直查表行，历史行的身份列与已收尾列不再变化。"""
        db_path = tmp_path / "state.db"
        backend = SQLiteStateBackend(db_path)
        first_id = backend.append_attempt(_running_attempt("t::one"))
        second_id = backend.append_attempt(_running_attempt("t::other"))
        backend.update_attempt(
            first_id, outcome="requeued",
            finished_at="2026-05-01T00:00:05+00:00", error="transient",
        )

        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT id, job_uid, started_at, outcome, finished_at, error "
                "FROM attempts ORDER BY id ASC"
            ).fetchall()
        assert len(rows) == 2
        assert rows[0] == (
            first_id, "t::one", "2026-05-01T00:00:00+00:00",
            "requeued", "2026-05-01T00:00:05+00:00", "transient",
        )
        assert rows[1][:2] == (second_id, "t::other")
        assert rows[1][3] == "running" and rows[1][4] is None


class TestSeedWallAndCursor:
    """种子化：wall/failed 互斥 + cursor 入口校验。"""

    def test_seed_wall_writes_empty_meta_idempotently(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        assert backend.seed_wall(["t::fresh_one", "t::fresh_two"]) == 2
        assert backend.load_wall() == {"t::fresh_one": {}, "t::fresh_two": {}}
        assert backend.seed_wall(["t::fresh_one"]) == 1
        assert backend.load_wall()["t::fresh_one"] == {}

    def test_seed_wall_rejects_uid_already_in_failed_atomically(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.append_failed("t::bad", {"error": "boom"})

        with pytest.raises(ValueError, match="already in failed"):
            backend.seed_wall(["t::ok_entry", "t::bad", "t::ok_next"])

        # 冲突整体拒绝：零写入，失败档案记录原样保留
        assert backend.load_wall() == {}
        assert set(backend.load_failed()) == {"t::bad"}

    def test_seed_wall_after_delete_failed_succeeds(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.append_failed("t::one", {"error": "boom"})
        assert backend.delete_failed(["t::one"]) == 1
        assert backend.seed_wall(["t::one"]) == 1
        assert backend.load_wall() == {"t::one": {}}

    def test_seed_cursor_valid_roundtrip_upsert(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.seed_cursor("k", "v_one")
        backend.seed_cursor("k", "v_two")
        assert backend.load_cursors() == {"k": "v_two"}

    @pytest.mark.parametrize("key,value", [(None, "v"), ("k", 123), ("", "v")])
    def test_seed_cursor_rejects_bad_types_zero_write(self, tmp_path, key, value):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        with pytest.raises(TypeError):
            backend.seed_cursor(key, value)
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
                # 路径二：成功 commit（wall 写 + popped 删除 + spawned 队头插入 + cursor）
                if not backend.commit_job_success(
                    f"c::{tid}-{k}",
                    {"ok": True},
                    spawned_jobs=[{"task_type": "s", "job_id": f"s{tid}-{k}"}],
                    cursor_updates={f"cur_{tid}": str(k)},
                ):
                    errors.append(f"commit_job_success returned False (t{tid} k{k})")
                # 路径三：共享 uid 的失败档案并发写（按 uid REPLACE、快照保留）
                backend.append_failed("shared::uid", {"error": "boom"})
                # 路径四：失败 commit（失败档案写入的另一个并发入口）
                if not backend.commit_job_failure(f"f::{tid}-{k}", {"error": "x"}):
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
                "SELECT COUNT(*), COUNT(DISTINCT seq) FROM queue"
            ).fetchone()
        # seq 全局唯一：重复 seq 直接破坏队头保序契约
        assert total == distinct, (
            f"seq 重复：queue 共 {total} 行，仅 {distinct} 个唯一 seq"
        )
        # 行数精确：初始 1 + 每线程每轮 enqueue 1 行 + spawned 1 行
        # （c::/f:: uid 从不在 queue 中，其 DELETE 是 no-op）
        assert total == 1 + 2 * N_THREADS * N_ROUNDS

        # 失败档案：共享 uid 按唯一终态收敛为一行，meta 为最后一次写入
        failed = backends[0].load_failed()
        assert failed["shared::uid"] == {"error": "boom"}
        assert len(failed) == N_THREADS * N_ROUNDS + 1

        # wall 全部落盘（每个 c:: uid 一次成功 commit）
        wall = backends[0].load_wall()
        assert len(wall) == N_THREADS * N_ROUNDS


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
                "SELECT uid FROM queue ORDER BY seq ASC"
            ).fetchall()
            total, distinct = conn.execute(
                "SELECT COUNT(*), COUNT(DISTINCT seq) FROM queue"
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
            backend_a.append_failed("t::u", {"round": i})
            backend_b.append_failed("t::u", {"round": i})

        failed = backend_a.load_failed()
        assert set(failed) == {"t::u"}
        assert failed["t::u"]["round"] == 19
