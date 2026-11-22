"""SQLite 状态后端（SQLiteStateBackend）。

队列、档案、Attempt、编号、游标与授权共享单 ACID 库。
全新 schema、零旧库兼容：user_version 只认本代版本号或全新库（0），
任何旧版本（含 v1 各代）fail-loud 拒绝启动，不写迁移路径。
"""
from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence, cast

from .base import (
    AbstractStateBackend,
    FailureEntry,
    GraphEditCommit,
    is_valid_authorization_target_type,
    normalize_authorization_path,
    normalize_failure_entries,
    public_queue_snapshot,
    queue_snapshot_execution_id,
    queue_snapshot_with_execution_id,
    resolve_authorized_at,
    validate_authorization_target_type,
    validate_cursor_key,
    validate_cursor_value,
    validate_graph_edit_commit,
    validate_mark_job_running_args,
    validate_mark_job_running_state,
    validate_recovery_attempt_state,
    validate_running_settlement_state,
    validate_settlement_args,
)
from ..exceptions import TaskLiteError
from ..models.state import job_uid_from_snapshot
from ..utils.artifact import TargetType, evaluate_authorization
from ..utils.jsonutil import dumps, loads
from ..utils.lockfile import release_lock, try_acquire_lock

logger = logging.getLogger("tasklite")

# 只接纳本代完整形状或真正空库，不提供自动迁移。
TARGET_SCHEMA_VERSION = 6

# 核心表的精确列集合；不扩大为 SQL 类型与约束审计。
_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "queue": (
        "job_uid", "queue_position", "job_snapshot", "status", "run_id", "attempt_no", "started_at",
    ),
    "successes": ("job_uid", "metadata"),
    "failed": ("job_uid", "metadata", "payload"),
    "attempts": (
        "attempt_id", "job_uid", "attempt_no", "run_id", "outcome", "started_at",
        "settled_at", "job_snapshot", "files", "result", "error_message",
    ),
    "job_sequences": ("job_uid", "last_attempt_no"),
    "cursors": ("key", "value"),
    "engine_metadata": ("key", "value"),
    "authorizations": ("path", "target_type", "authorized_at"),
}


def _schema_error(path: Path, detail: str) -> TaskLiteError:
    return TaskLiteError(
        f"{path}: {detail}; 目标 schema {TARGET_SCHEMA_VERSION}。"
        "请保留原库，使用匹配版本读取历史，或显式选择新的状态库路径。"
    )


def _check_schema(
    conn: sqlite3.Connection, path: Path,
) -> int:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    objects = list(conn.execute(
        "SELECT name, type FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'"
    ))
    if version == 0:
        if objects:
            raise _schema_error(
                path,
                f"Unsupported table shape: version 0 非空库，已有对象 {sorted(objects)}"
            )
        return version
    if version != TARGET_SCHEMA_VERSION:
        raise _schema_error(path, f"Unsupported database schema version {version}")
    table_objects = {name: kind for name, kind in objects if kind in ("table", "view")}
    if {"wall", "meta"} & {name.casefold() for name, kind in objects if kind == "table"}:
        raise _schema_error(
            path, f"Unsupported table shape: version {version} 含异代 wall/meta 表"
        )
    for table, expected in _TABLE_COLUMNS.items():
        if table_objects.get(table) != "table":
            raise _schema_error(
                path,
                f"Unsupported table shape for '{table}': version {version}, "
                f"对象类型 {table_objects.get(table)!r}，必须为 table"
            )
        columns = {row[1] for row in conn.execute(f'PRAGMA table_xinfo("{table}")')}
        if columns != set(expected):
            raise _schema_error(
                path,
                f"Unsupported table shape for '{table}': version {version}, "
                f"expected columns {expected}, found {sorted(columns)}"
            )
    return version


def open_existing_sqlite_readonly(
    db_path: str | Path,
    *,
    begin_transaction: bool = True,
    timeout: float = 30.0,
) -> sqlite3.Connection:
    """校验并以强制只读 URI 打开已存在的 TaskLite Schema 6 状态库。

    按顺序拒绝不存在路径、目录、0 字节空文件、非 SQLite 文件与未初始化/不兼容
    Schema 库；全程零磁盘写入，不生成 ``-wal`` / ``-shm`` 旁路文件。
    """
    path = Path(db_path)
    if not path.exists():
        raise TaskLiteError(f"Database file does not exist: {path}")
    if path.is_dir():
        raise TaskLiteError(f"Path is a directory, not a database file: {path}")
    if path.stat().st_size == 0:
        raise TaskLiteError(f"Uninitialized TaskLite database at {path}")

    uri = path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=timeout)
    try:
        if begin_transaction:
            conn.execute("BEGIN")
        if _check_schema(conn, path) == 0:
            raise TaskLiteError(f"Uninitialized TaskLite database at {path}")
    except BaseException:
        if begin_transaction:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
        conn.close()
        raise
    return conn


def verify_existing_sqlite_database(db_path: str | Path) -> Path:
    """只读探活已存在的 TaskLite Schema 6 状态库并立即释放连接（零磁盘写入）。"""
    path = Path(db_path)
    conn = open_existing_sqlite_readonly(path, begin_transaction=False)
    conn.close()
    return path


class SQLiteStateBackend(AbstractStateBackend):
    """原子 SQLite 状态后端：逐 job 单事务碎片化的 delta 提交。"""

    def __init__(self, filepath: str | Path):
        self.path = Path(filepath)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def exclusive_access(self) -> Iterator[None]:
        """非阻塞获取 ``<state_path>.lock`` 跨进程文件锁，离开作用域归还。"""
        fd = try_acquire_lock(self.path, timeout=0)
        if fd is None:
            raise TaskLiteError(
                f"Another run() or management operation is in progress for "
                f"{self.path}; concurrent runs or management operations on the same state are forbidden."
            )
        try:
            yield
        finally:
            release_lock(fd)

    @contextmanager
    def _get_conn(self) -> Iterator[sqlite3.Connection]:
        """连接上下文管理器：成功时 commit，异常时 rollback，始终 close。

        ``synchronous=FULL`` 保证已应答事务落盘；读-改-写方法必须在任意 SELECT
        之前显式 ``BEGIN IMMEDIATE`` 串行化写事务。
        """
        conn = sqlite3.connect(self.path, timeout=30.0)
        try:
            conn.execute('PRAGMA synchronous=FULL')
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # 初始化与 schema -------------------------------------------------

    def _check_schema(self, conn: sqlite3.Connection) -> int:
        return _check_schema(conn, self.path)

    def _preflight_schema(self) -> None:
        try:
            self.path.stat()
        except FileNotFoundError:
            return
        conn = None
        try:
            # 普通只读连接必须看到 WAL 中已提交的版本与结构。
            conn = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)
            self._check_schema(conn)
        finally:
            if conn is not None:
                conn.close()

    def _init_db(self) -> None:
        self._preflight_schema()
        with self._get_conn() as conn:
            self._check_schema(conn)
            wal_row = conn.execute('PRAGMA journal_mode=WAL').fetchone()
            if not wal_row or str(wal_row[0]).lower() != 'wal':
                raise TaskLiteError(
                    f"journal_mode=WAL could not be engaged on {self.path} "
                    f"(got {wal_row[0] if wal_row else None!r})."
                )
            conn.execute("BEGIN IMMEDIATE")
            version = self._check_schema(conn)
            if version == 0:
                self._create_tables(conn)
                self._create_indexes(conn)
                conn.execute(f"PRAGMA user_version = {TARGET_SCHEMA_VERSION}")

    def _create_tables(self, conn: sqlite3.Connection) -> None:
        """只在已确认的空库写事务中建立核心表。"""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS queue (
                job_uid TEXT PRIMARY KEY,
                queue_position INTEGER NOT NULL,
                job_snapshot TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                run_id TEXT,
                attempt_no INTEGER,
                started_at TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS successes (
                job_uid TEXT PRIMARY KEY,
                metadata TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS failed (
                job_uid TEXT PRIMARY KEY,
                metadata TEXT,
                payload TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS attempts (
                attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_uid TEXT NOT NULL,
                attempt_no INTEGER NOT NULL,
                run_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                started_at TEXT,
                settled_at TEXT NOT NULL,
                job_snapshot TEXT,
                files TEXT,
                result TEXT,
                error_message TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS job_sequences (
                job_uid TEXT PRIMARY KEY,
                last_attempt_no INTEGER NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS cursors (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS engine_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS authorizations (
                path TEXT PRIMARY KEY,
                target_type TEXT NOT NULL,
                authorized_at TEXT NOT NULL
            )
        ''')

    def _create_indexes(self, conn: sqlite3.Connection) -> None:
        """建索引（在形状校验之后：异形表先给出明确的拒绝信息）。"""
        conn.execute('CREATE INDEX IF NOT EXISTS idx_queue_position ON queue(queue_position)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_attempts_job_uid ON attempts(job_uid)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_attempts_job_uid_outcome ON attempts(job_uid, outcome)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_attempts_job_uid_attempt_no ON attempts(job_uid, attempt_no)')

    # 序号分配 ---------------------------------------------------------

    def _queue_position_range(self, conn: sqlite3.Connection, count: int, front: bool) -> list[int]:
        """分配 count 个保序 queue_position；调用方必须已持 ``BEGIN IMMEDIATE`` 写事务。"""
        if count <= 0:
            return []
        min_queue_position, max_queue_position = conn.execute(
            'SELECT MIN(queue_position), MAX(queue_position) FROM queue'
        ).fetchone()
        if min_queue_position is None:
            min_queue_position, max_queue_position = 0, -1
        if front:
            base = min_queue_position - count
            return list(range(base, min_queue_position))
        base = max_queue_position + 1
        return list(range(base, base + count))

    # 全量加载 ---------------------------------------------------------

    def load_successes(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        with self._get_conn() as conn:
            cursor = conn.execute('SELECT job_uid, metadata FROM successes')
            return {row[0]: loads(row[1]) for row in cursor}

    def load_failed(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        with self._get_conn() as conn:
            cursor = conn.execute('SELECT job_uid, metadata FROM failed')
            return {row[0]: loads(row[1]) for row in cursor}

    # delta 提交 ---------------------------------------------------------

    def _write_failed_row(
        self,
        conn: sqlite3.Connection,
        job_uid: str,
        meta: dict[str, Any],
        payload: dict[str, Any] | None = None,
    ) -> None:
        """失败档案行写入单出口（``payload=None`` 保留既有快照，须在 ``BEGIN IMMEDIATE`` 内调用）。"""
        row = conn.execute(
            'SELECT payload FROM failed WHERE job_uid = ?', (job_uid,)
        ).fetchone()
        merged = dict(meta or {})
        effective_payload = (
            payload
            if payload is not None
            else (loads(row[0]) if row is not None and row[0] is not None else None)
        )
        conn.execute(
            'INSERT OR REPLACE INTO failed (job_uid, metadata, payload) VALUES (?, ?, ?)',
            (
                job_uid,
                dumps(merged),
                dumps(effective_payload) if effective_payload is not None else None,
            ),
        )

    def commit_job_failures(self, entries: Sequence[FailureEntry]) -> None:
        """原子批量写失败档案 + 按 job_uid 删除队列行 + 清理 successes 对侧记录。"""
        normalized = normalize_failure_entries(entries)
        if not normalized:
            return
        try:
            with self._get_conn() as conn:
                conn.execute('BEGIN IMMEDIATE')
                for job_uid, meta, payload in normalized:
                    self._write_failed_row(conn, job_uid, meta, payload)
                uid_params = [(job_uid,) for job_uid, _, _ in normalized]
                conn.executemany('DELETE FROM queue WHERE job_uid = ?', uid_params)
                conn.executemany('DELETE FROM successes WHERE job_uid = ?', uid_params)
        except Exception as e:
            logger.critical(f"Failed to commit failure archive entries in {self.path.name}: {e}")
            raise

    def commit_skip(self, job_uid: str) -> None:
        """移除 Trigger False 或重复候选；不写 successes/failed。"""
        try:
            with self._get_conn() as conn:
                conn.execute('DELETE FROM queue WHERE job_uid = ?', (job_uid,))
        except Exception as e:
            logger.critical(f"Failed to commit skip for {job_uid} in {self.path.name}: {e}")
            raise

    # 增量入队 -----------------------------------------------------------

    def enqueue_jobs(self, job_snapshots: list[dict[str, Any]], *, front: bool = False) -> list[str]:
        """批量增量入队：单事务原子插入，跳过重复 job_uid。"""
        if not job_snapshots:
            return []
        try:
            with self._get_conn() as conn:
                conn.execute('BEGIN IMMEDIATE')
                existing = {
                    row[0] for row in conn.execute('SELECT job_uid FROM queue')
                }
                fresh: list[dict[str, Any]] = []
                batch_seen: set[str] = set()
                for j in job_snapshots:
                    u = job_uid_from_snapshot(j)
                    if u in existing or u in batch_seen:
                        continue
                    batch_seen.add(u)
                    fresh.append(j)
                if not fresh:
                    return []
                queue_positions = self._queue_position_range(conn, len(fresh), front=front)
                conn.executemany(
                    'INSERT INTO queue (job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?)',
                    [
                        (
                            job_uid_from_snapshot(j),
                            pos,
                            dumps(j),
                            j.get("status", "pending"),
                            j.get("run_id"),
                            j.get("attempt_no"),
                            j.get("started_at"),
                        )
                        for j, pos in zip(fresh, queue_positions)
                    ],
                )
                return [job_uid_from_snapshot(j) for j in fresh]
        except Exception as e:
            logger.critical(f"Failed to enqueue job_snapshots to {self.path.name}: {e}")
            raise

    # 管理写与查询 -------------------------------------------------------

    def load_consistent_snapshot(self) -> dict[str, Any]:
        """在单一只读事务内原子捕获全部核心表一致快照（含 cursors 与 job_sequences）。"""
        if not self.path.exists():
            return {
                "queue_jobs": [],
                "successes": {},
                "failed": {},
                "failure_payloads": {},
                "cursors": {},
                "attempts": [],
                "job_sequences": {},
            }
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=30.0)
        try:
            conn.execute("BEGIN")
            queue_rows: list[dict[str, Any]] = []
            for row in conn.execute(
                "SELECT job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at "
                "FROM queue ORDER BY queue_position ASC"
            ):
                stored_job = loads(row[2])
                if not isinstance(stored_job, dict):
                    raise RuntimeError(f"Corrupted queue job_snapshot for {row[0]!r}")
                queue_rows.append({
                    "job_uid": row[0],
                    "queue_position": row[1],
                    "job": public_queue_snapshot(stored_job),
                    "status": row[3],
                    "execution_id": queue_snapshot_execution_id(stored_job),
                    "run_id": row[4],
                    "attempt_no": row[5],
                    "started_at": row[6],
                })

            successes = {
                row[0]: loads(row[1])
                for row in conn.execute("SELECT job_uid, metadata FROM successes")
            }

            failed: dict[str, dict[str, Any]] = {}
            failure_payloads: dict[str, dict[str, Any]] = {}
            for row in conn.execute("SELECT job_uid, metadata, payload FROM failed"):
                failed[row[0]] = loads(row[1])
                if row[2] is not None:
                    failure_payloads[row[0]] = loads(row[2])

            cursors = {
                row[0]: row[1]
                for row in conn.execute("SELECT key, value FROM cursors")
            }

            attempts: list[dict[str, Any]] = []
            for row in conn.execute(
                "SELECT attempt_id, job_uid, attempt_no, run_id, outcome, started_at, settled_at, "
                "job_snapshot, files, result, error_message "
                "FROM attempts ORDER BY attempt_id ASC"
            ):
                attempts.append({
                    "attempt_id": row[0],
                    "job_uid": row[1],
                    "attempt_no": row[2],
                    "run_id": row[3],
                    "outcome": row[4],
                    "started_at": row[5],
                    "settled_at": row[6],
                    "job": loads(row[7]) if row[7] else None,
                    "files": loads(row[8]) if row[8] else None,
                    "result": loads(row[9]) if row[9] else None,
                    "error_message": row[10],
                })

            job_sequences = {
                row[0]: row[1]
                for row in conn.execute("SELECT job_uid, last_attempt_no FROM job_sequences")
            }

            return {
                "queue_jobs": queue_rows,
                "successes": successes,
                "failed": failed,
                "failure_payloads": failure_payloads,
                "cursors": cursors,
                "attempts": attempts,
                "job_sequences": job_sequences,
            }
        finally:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            conn.close()

    def commit_graph_edit(self, commit: GraphEditCommit) -> None:
        """在单 BEGIN IMMEDIATE 事务内复核遗留 running 补写并按触碰 uid 精准更新三表（O(Δ)）。"""
        if not validate_graph_edit_commit(commit):
            return

        with self._get_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")

            for rec in commit.recovery_attempts:
                row = conn.execute(
                    "SELECT status, run_id, attempt_no, job_snapshot FROM queue WHERE job_uid = ?",
                    (rec.job_uid,),
                ).fetchone()
                stored_job = loads(row[3]) if (row is not None and row[3]) else None
                job_dict = stored_job if isinstance(stored_job, dict) else None
                seq_row = conn.execute(
                    "SELECT last_attempt_no FROM job_sequences WHERE job_uid = ?",
                    (rec.job_uid,),
                ).fetchone()
                settled = bool(
                    conn.execute(
                        "SELECT 1 FROM attempts WHERE job_uid = ? AND attempt_no = ?",
                        (rec.job_uid, rec.attempt_no),
                    ).fetchone()
                )
                validate_recovery_attempt_state(
                    rec,
                    present_in_queue=row is not None,
                    queue_status=row[0] if (row is not None and job_dict is not None) else None,
                    queue_run_id=row[1] if (row is not None and job_dict is not None) else None,
                    queue_attempt_no=row[2] if (row is not None and job_dict is not None) else None,
                    queue_execution_id=(
                        queue_snapshot_execution_id(job_dict) if job_dict is not None else None
                    ),
                    attempt_sequence=seq_row[0] if seq_row is not None else None,
                    already_settled=settled,
                )
                conn.execute(
                    "INSERT INTO attempts ("
                    "job_uid, attempt_no, run_id, outcome, started_at, settled_at, "
                    "job_snapshot, files, result, error_message"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        rec.job_uid,
                        rec.attempt_no,
                        rec.run_id,
                        "failure",
                        rec.started_at,
                        rec.settled_at,
                        dumps(rec.job_snapshot) if rec.job_snapshot is not None else None,
                        dumps(rec.files) if rec.files is not None else None,
                        dumps(rec.result) if rec.result is not None else None,
                        rec.error_message,
                    ),
                )

            if commit.queue_writes:
                conn.executemany(
                    "DELETE FROM queue WHERE job_uid = ?",
                    [(uid,) for uid in commit.queue_writes],
                )
                to_insert = [
                    (uid, public_queue_snapshot(snap))
                    for uid, snap in commit.queue_writes.items()
                    if snap is not None
                ]
                if to_insert:
                    positions = self._queue_position_range(conn, len(to_insert), front=False)
                    conn.executemany(
                        "INSERT INTO queue ("
                        "job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at"
                        ") VALUES (?, ?, ?, 'pending', NULL, NULL, NULL)",
                        [
                            (uid, pos, dumps(snap))
                            for (uid, snap), pos in zip(to_insert, positions)
                        ],
                    )

            for uid, meta in commit.success_writes.items():
                if meta is None:
                    conn.execute("DELETE FROM successes WHERE job_uid = ?", (uid,))
                else:
                    conn.execute(
                        "INSERT OR REPLACE INTO successes (job_uid, metadata) VALUES (?, ?)",
                        (uid, dumps(meta)),
                    )

            for uid, entry in commit.failed_writes.items():
                if entry is None:
                    conn.execute("DELETE FROM failed WHERE job_uid = ?", (uid,))
                else:
                    fail_meta, payload = entry
                    conn.execute(
                        "INSERT OR REPLACE INTO failed (job_uid, metadata, payload) VALUES (?, ?, ?)",
                        (
                            uid,
                            dumps(fail_meta),
                            dumps(payload) if payload is not None else None,
                        ),
                    )

            overlap_row = conn.execute(
                "SELECT s.job_uid FROM successes AS s "
                "INNER JOIN failed AS f ON s.job_uid = f.job_uid LIMIT 1"
            ).fetchone()
            if overlap_row is not None:
                raise TaskLiteError(
                    f"Invariant conflict: job {overlap_row[0]!r} cannot exist in both successes and failed"
                )

    def get_cursor(self, key: str) -> str | None:
        """查询单个游标的最新已提交值，不存在返回 None。"""
        validate_cursor_key(key)
        if not self.path.exists():
            return None
        with self._get_conn() as conn:
            row = conn.execute("SELECT value FROM cursors WHERE key = ?", (key,)).fetchone()
            return row[0] if row is not None else None

    def set_cursor(self, key: str, value: str | None) -> None:
        """设置或删除单个游标（None 为删除）。"""
        validate_cursor_key(key)
        validate_cursor_value(value)
        try:
            with self._get_conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if value is None:
                    conn.execute("DELETE FROM cursors WHERE key = ?", (key,))
                else:
                    conn.execute(
                        "INSERT OR REPLACE INTO cursors (key, value) VALUES (?, ?)",
                        (key, value),
                    )
        except Exception as e:
            logger.critical(f"Failed to set cursor '{key}' in {self.path.name}: {e}")
            raise

    # 尝试轨迹（append-only 旁路观测面）------------------------------------

    def load_latest_succeeded_finished_at(self, job_uid: str) -> str | None:
        if not self.path.exists():
            return None
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT settled_at FROM attempts "
                "WHERE job_uid = ? AND outcome = 'success' "
                "ORDER BY attempt_id DESC LIMIT 1",
                (job_uid,),
            ).fetchone()
            if row is None or not isinstance(row[0], str) or not row[0]:
                return None
            return row[0]

    # 元数据 ---------------------------------------------------------------

    def set_engine_metadata(self, key: str, value: str) -> None:
        try:
            with self._get_conn() as conn:
                conn.execute(
                    'INSERT OR REPLACE INTO engine_metadata (key, value) VALUES (?, ?)',
                    (key, value),
                )
        except Exception as e:
            logger.critical(f"Failed to write engine_metadata '{key}' in {self.path.name}: {e}")
            raise

    # 状态事务与编号 -------------------------------------------------

    def mark_job_running(
        self, job_uid: str, *, execution_id: str, run_id: str, started_at: str
    ) -> int:
        validate_mark_job_running_args(
            execution_id=execution_id, run_id=run_id, started_at=started_at
        )
        with self._get_conn() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute(
                'SELECT status, job_snapshot FROM queue WHERE job_uid = ?',
                (job_uid,),
            ).fetchone()
            validate_mark_job_running_state(
                job_uid,
                present_in_queue=row is not None,
                queue_status=row[0] if row is not None else None,
            )
            assert row is not None
            job_snapshot = loads(row[1])
            if not isinstance(job_snapshot, dict):
                raise ValueError(f"Job {job_uid} has an invalid queue snapshot")

            seq_row = conn.execute(
                'SELECT last_attempt_no FROM job_sequences WHERE job_uid = ?',
                (job_uid,),
            ).fetchone()
            next_no = seq_row[0] + 1 if seq_row is not None else 1

            conn.execute(
                'INSERT OR REPLACE INTO job_sequences (job_uid, last_attempt_no) VALUES (?, ?)',
                (job_uid, next_no),
            )
            conn.execute(
                'UPDATE queue SET job_snapshot = ?, status = ?, run_id = ?, '
                'attempt_no = ?, started_at = ? WHERE job_uid = ?',
                (
                    dumps(queue_snapshot_with_execution_id(job_snapshot, execution_id)),
                    "running",
                    run_id,
                    next_no,
                    started_at,
                    job_uid,
                ),
            )
            return next_no

    def load_queue_jobs(self, *, status: str | None = None) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        with self._get_conn() as conn:
            if status is not None:
                cursor = conn.execute(
                    'SELECT job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at '
                    'FROM queue WHERE status = ? ORDER BY queue_position ASC',
                    (status,),
                )
            else:
                cursor = conn.execute(
                    'SELECT job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at '
                    'FROM queue ORDER BY queue_position ASC'
                )
            results: list[dict[str, Any]] = []
            for row in cursor:
                stored_job = loads(row[2])
                if not isinstance(stored_job, dict):
                    raise RuntimeError(f"Corrupted queue job_snapshot for {row[0]!r}")
                results.append({
                    "job_uid": row[0],
                    "queue_position": row[1],
                    "job": public_queue_snapshot(stored_job),
                    "status": row[3],
                    "execution_id": queue_snapshot_execution_id(stored_job),
                    "run_id": row[4],
                    "attempt_no": row[5],
                    "started_at": row[6],
                })
            return results

    def commit_job_settlement(
        self,
        *,
        job_uid: str,
        outcome: str,
        attempt_no: int,
        execution_id: str,
        run_id: str,
        settled_at: str,
        started_at: str | None = None,
        job_snapshot: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        error_message: str | None = None,
        success_metadata: dict[str, Any] | None = None,
        spawned_job_snapshots: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
        failure_metadata: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        requeued_job_snapshot: dict[str, Any] | None = None,
        record_attempt: bool = True,
    ) -> None:
        validate_settlement_args(
            outcome=outcome,
            attempt_no=attempt_no,
            execution_id=execution_id,
            run_id=run_id,
            settled_at=settled_at,
            started_at=started_at,
            cursor_updates=cursor_updates,
            spawned_job_snapshots=spawned_job_snapshots,
        )

        with self._get_conn() as conn:
            conn.execute('BEGIN IMMEDIATE')

            running = conn.execute(
                "SELECT status, run_id, attempt_no, job_snapshot "
                "FROM queue WHERE job_uid = ?",
                (job_uid,),
            ).fetchone()
            stored_job = loads(running[3]) if (running is not None and running[3]) else None
            job_dict = stored_job if isinstance(stored_job, dict) else None
            settled = bool(
                conn.execute(
                    "SELECT 1 FROM attempts WHERE job_uid = ? AND attempt_no = ?",
                    (job_uid, attempt_no),
                ).fetchone()
            )
            validate_running_settlement_state(
                job_uid,
                present_in_queue=running is not None and job_dict is not None,
                queue_status=running[0] if (running is not None and job_dict is not None) else None,
                queue_run_id=running[1] if (running is not None and job_dict is not None) else None,
                queue_attempt_no=running[2] if (running is not None and job_dict is not None) else None,
                queue_execution_id=(
                    queue_snapshot_execution_id(job_dict) if job_dict is not None else None
                ),
                run_id=run_id,
                attempt_no=attempt_no,
                execution_id=execution_id,
                already_settled=settled,
            )
            assert job_dict is not None

            if record_attempt:
                attempt_outcome = (
                    outcome if outcome in ("success", "failure", "retry") else "failure"
                )
                conn.execute(
                    'INSERT INTO attempts ('
                    'job_uid, attempt_no, run_id, outcome, started_at, settled_at, '
                    'job_snapshot, files, result, error_message'
                    ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (
                        job_uid,
                        attempt_no,
                        run_id,
                        attempt_outcome,
                        started_at,
                        settled_at,
                        dumps(job_snapshot) if job_snapshot is not None else None,
                        dumps(files) if files is not None else None,
                        dumps(result) if result is not None else None,
                        error_message,
                    ),
                )

            if outcome == "success":
                conn.execute(
                    'INSERT OR REPLACE INTO successes (job_uid, metadata) VALUES (?, ?)',
                    (job_uid, dumps(success_metadata or {})),
                )
                conn.execute('DELETE FROM failed WHERE job_uid = ?', (job_uid,))
                conn.execute('DELETE FROM queue WHERE job_uid = ?', (job_uid,))

                if spawned_job_snapshots:
                    queue_positions = self._queue_position_range(conn, len(spawned_job_snapshots), front=True)
                    rows = [
                        (
                            job_uid_from_snapshot(j),
                            pos,
                            dumps(public_queue_snapshot(j)),
                            "pending",
                            None,
                            None,
                            None,
                        )
                        for j, pos in zip(spawned_job_snapshots, queue_positions)
                    ]
                    cur = conn.executemany(
                        'INSERT OR IGNORE INTO queue ('
                        'job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at'
                        ') VALUES (?, ?, ?, ?, ?, ?, ?)',
                        rows,
                    )
                    if cur.rowcount is not None and cur.rowcount < len(spawned_job_snapshots):
                        logger.critical(
                            f"commit_job_settlement for {job_uid}: "
                            f"{len(spawned_job_snapshots) - cur.rowcount} spawned job(s) "
                            f"conflicted on disk. Rolling back."
                        )
                        raise TaskLiteError(f"Spawned job conflicts with queue for {job_uid}")

                if cursor_updates:
                    for k, v in cursor_updates.items():
                        if v is None:
                            conn.execute('DELETE FROM cursors WHERE key = ?', (k,))
                        else:
                            conn.execute(
                                'INSERT OR REPLACE INTO cursors (key, value) VALUES (?, ?)',
                                (k, v),
                            )

            elif outcome == "failure":
                conn.execute(
                    'INSERT OR REPLACE INTO failed (job_uid, metadata, payload) VALUES (?, ?, ?)',
                    (job_uid, dumps(failure_metadata or {}), dumps(payload) if payload is not None else None),
                )
                conn.execute('DELETE FROM successes WHERE job_uid = ?', (job_uid,))
                conn.execute('DELETE FROM queue WHERE job_uid = ?', (job_uid,))

            elif outcome == "retry":
                target_snap = (
                    public_queue_snapshot(requeued_job_snapshot)
                    if requeued_job_snapshot is not None
                    else public_queue_snapshot(job_dict)
                )
                conn.execute(
                    'UPDATE queue SET job_snapshot = ?, status = ?, run_id = NULL, '
                    'attempt_no = NULL, started_at = NULL WHERE job_uid = ?',
                    (dumps(target_snap), "pending", job_uid),
                )

            elif outcome == "pending":
                conn.execute(
                    'UPDATE queue SET job_snapshot = ?, status = ?, run_id = NULL, '
                    'attempt_no = NULL, started_at = NULL WHERE job_uid = ?',
                    (dumps(public_queue_snapshot(job_dict)), "pending", job_uid),
                )

    @staticmethod
    def _load_auth_pairs(conn: sqlite3.Connection) -> list[tuple[str, TargetType]]:
        rows = conn.execute('SELECT path, target_type FROM authorizations').fetchall()
        return [(row[0], cast(TargetType, row[1])) for row in rows]

    def grant_authorization(
        self,
        path: str,
        target_type: Literal["file", "directory"],
        *,
        authorized_at: str | None = None,
    ) -> dict[str, Any]:
        norm_path = normalize_authorization_path(path)
        validated_type = validate_authorization_target_type(target_type, error_type=TaskLiteError)
        ts = resolve_authorized_at(authorized_at)
        with self._get_conn() as conn:
            conn.execute('BEGIN IMMEDIATE')
            match = evaluate_authorization(
                self._load_auth_pairs(conn),
                norm_path,
                validated_type,
            )
            if match.grant_conflict is not None:
                raise TaskLiteError(match.grant_conflict)
            conn.execute(
                'INSERT INTO authorizations (path, target_type, authorized_at) VALUES (?, ?, ?)',
                (norm_path, validated_type, ts),
            )
            return {
                "path": norm_path,
                "target_type": validated_type,
                "authorized_at": ts,
            }

    def record_authorization(
        self,
        path: str,
        target_type: str,
        *,
        authorized_at: str | None = None,
    ) -> None:
        norm_path = normalize_authorization_path(path)
        validated_type = validate_authorization_target_type(target_type, error_type=ValueError)
        ts = resolve_authorized_at(authorized_at)
        with self._get_conn() as conn:
            conn.execute('BEGIN IMMEDIATE')
            match = evaluate_authorization(
                self._load_auth_pairs(conn),
                norm_path,
                validated_type,
            )
            if match.exact_path is not None and match.exact_path != norm_path:
                conn.execute('DELETE FROM authorizations WHERE path = ?', (match.exact_path,))
            conn.execute(
                'INSERT OR REPLACE INTO authorizations (path, target_type, authorized_at) VALUES (?, ?, ?)',
                (norm_path, validated_type, ts),
            )

    def list_authorizations(self) -> list[dict[str, Any]]:
        with self._get_conn() as conn:
            cur = conn.execute(
                'SELECT path, target_type, authorized_at FROM authorizations ORDER BY authorized_at ASC, path ASC'
            )
            return [
                {"path": row[0], "target_type": row[1], "authorized_at": row[2]}
                for row in cur.fetchall()
            ]

    def revoke_authorization(self, path: str) -> bool:
        norm_path = normalize_authorization_path(path)
        with self._get_conn() as conn:
            conn.execute('BEGIN IMMEDIATE')
            match = evaluate_authorization(self._load_auth_pairs(conn), norm_path)
            if match.exact_path is None:
                return False
            cur = conn.execute('DELETE FROM authorizations WHERE path = ?', (match.exact_path,))
            return bool(cur.rowcount and cur.rowcount > 0)

    def is_path_authorized(self, path: str, target_type: str) -> bool:
        norm_path = normalize_authorization_path(path)
        if not is_valid_authorization_target_type(target_type):
            return False
        with self._get_conn() as conn:
            return evaluate_authorization(
                self._load_auth_pairs(conn),
                norm_path,
                cast(TargetType, target_type),
            ).covered

    def close(self) -> None:
        """关闭后端并释放资源（幂等）。"""
        pass


__all__ = [
    "SQLiteStateBackend",
    "TARGET_SCHEMA_VERSION",
    "open_existing_sqlite_readonly",
    "verify_existing_sqlite_database",
]
