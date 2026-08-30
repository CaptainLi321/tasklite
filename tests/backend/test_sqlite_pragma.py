"""PRAGMA scope verification test.

Verifies that the SQLite backend's per-connection PRAGMA settings
(synchronous=FULL) are applied to every new connection, not just
during _init_db. This catches the bug where PRAGMAs were set only in
_init_db but subsequent sqlite3.connect calls reverted to default.
"""
import sqlite3
import pytest
from tasklite.backend.sqlite_backend import SQLiteStateBackend


def test_fresh_connection_has_synchronous_full(tmp_path):
    """持久化不变式：backend 连接使用 synchronous=FULL。

    The backend's _get_conn helper applies PRAGMA synchronous=FULL
    on every connection. NORMAL 下 commit 不 fsync WAL——enqueue 应答后
    断电时 INSERT 事务消失且无 job 可重跑（at-least-once 吸收不了），
    故升级为 FULL 保证已应答事务落盘。
    """
    backend = SQLiteStateBackend(tmp_path / "pragma_test.db")

    # Use the backend's connection helper (this is what all backend methods use)
    with backend._get_conn() as conn:
        cursor = conn.execute("PRAGMA synchronous")
        result = cursor.fetchone()

    # 0=OFF, 1=NORMAL, 2=FULL
    assert result[0] == 2, f"Expected synchronous=FULL (2), got {result[0]}"


def test_init_db_persists_wal_mode(tmp_path):
    """WAL journal_mode is database-level and persists across connections.

    This is a companion test: WAL persists (database-level), but
    synchronous is per-connection (must be set on each new conn).
    """
    backend = SQLiteStateBackend(tmp_path / "wal_test.db")

    # Open a completely fresh connection (not using _get_conn)
    with sqlite3.connect(tmp_path / "wal_test.db") as conn:
        cursor = conn.execute("PRAGMA journal_mode")
        result = cursor.fetchone()

    # WAL mode persists at the database level
    assert result[0].lower() == "wal", f"Expected journal_mode=WAL, got {result[0]}"


def test_init_db_fails_loud_when_wal_unavailable(tmp_path, monkeypatch):
    """WAL 无法生效时必须 fail-loud 拒绝启动。

    rollback journal + synchronous=NORMAL 是 SQLite 文档标注的「断电可损坏」
    配置。若 PRAGMA journal_mode=WAL 静默返回当前模式（NFS 无锁/只读介质/
    被其他连接持有），引擎会以「断电安全」招牌运行在可损坏配置下——必须
    在 _init_db 抛 RuntimeError 而非静默继续（防御触发测试）。
    """
    import tasklite.backend.sqlite_backend as sb

    real_connect = sqlite3.connect

    class NoWalConn:
        """模拟 WAL 无法生效的连接：journal_mode=WAL 返回 'delete'。"""

        def __init__(self, path, timeout=30.0):
            self._conn = real_connect(path, timeout=timeout)

        def execute(self, sql, *a):
            if "journal_mode=WAL" in sql:
                # 返回带 fetchone 的假 cursor，模拟 WAL 静默降级为 delete
                return _WalCursor()
            return self._conn.execute(sql, *a)

        def commit(self):
            return self._conn.commit()

        def rollback(self):
            return self._conn.rollback()

        def close(self):
            return self._conn.close()

        def __getattr__(self, name):
            return getattr(self._conn, name)

    class _WalCursor:
        def fetchone(self):
            return ("delete",)

        def __iter__(self):
            return iter(())

    monkeypatch.setattr(sb.sqlite3, "connect", NoWalConn)
    with pytest.raises(RuntimeError, match="journal_mode=WAL could not be engaged"):
        SQLiteStateBackend(tmp_path / "nowal.db")
