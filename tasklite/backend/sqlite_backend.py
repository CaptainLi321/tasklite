"""SQLite 状态后端（SQLiteStateBackend）。

队列、档案、Attempt、编号、游标与授权共享单 ACID 库。
全新 schema、零旧库兼容：user_version 只认本代版本号或全新库（0），
任何旧版本（含 v1 各代）fail-loud 拒绝启动，不写迁移路径。
"""
from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .base import (
    AbstractStateBackend,
    execution_id_matches_run,
    public_queue_snapshot,
    queue_snapshot_execution_id,
    queue_snapshot_with_execution_id,
    validate_queue_replacement,
)
from ..exceptions import TaskLiteError
from ..models.state import job_uid_from_snapshot
from ..utils.clock import utc_now_iso
from ..utils.jsonutil import dumps, loads

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


class SQLiteStateBackend(AbstractStateBackend):
    """原子 SQLite 状态后端：逐 job 单事务碎片化的 delta 提交。"""

    def __init__(self, filepath: str | Path):
        self.path = Path(filepath)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _get_conn(self):
        """连接上下文管理器：成功时 commit，异常时 rollback，始终 close。

        ``__exit__`` 中显式 ``conn.close()``——事务管理与连接关闭都由
        上下文管理器承担，调用方无法泄漏 fd。

        ``synchronous=FULL`` 是 per-connection 设置，每次新建连接都需设置；
        ``journal_mode=WAL`` 是 database-level，仅在 ``_init_db`` 中设置一次。
        持久化保障：WAL+NORMAL 下 commit 不 fsync WAL，断电可回滚最后若干
        事务——执行中 job 的回滚可被 at-least-once 重跑吸收，但 enqueue
        应答后断电时 INSERT 事务消失且无 job 可重跑（任务静默蒸发）。
        FULL 保证已应答事务落盘。

        读-改-写事务纪律：``sqlite3`` 传统隔离模式只为 DML 隐式开事务，
        SELECT 在 autocommit 下逐条取快照——「读 min/max(queue_position) → 分配 →
        INSERT」「读 payload 快照 → REPLACE」「读 running 轨迹行 →
        UPDATE」若不显式开事务，两个连接会基于同一份旧快照各算各的，
        落盘出现重复 queue_position（idx_queue_position 非唯一，拦不住）或丢快照。此类
        方法必须在任何 SELECT 之前显式 ``BEGIN IMMEDIATE``：先取写锁再
        读，读与写在同一事务内；并发方要么先于本事务提交（本事务读到其
        结果），要么排队等锁。锁等待由 connect(timeout=30.0) 的 busy
        timeout 承担——并发方等待写锁而非立即报 database is locked。
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
        """建索引（在形状校验之后：异形表先给出明确的拒绝信息，而非
        在缺列上建索引时抛出费解的 no such column）。"""
        conn.execute('CREATE INDEX IF NOT EXISTS idx_queue_position ON queue(queue_position)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_attempts_job_uid ON attempts(job_uid)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_attempts_job_uid_outcome ON attempts(job_uid, outcome)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_attempts_job_uid_attempt_no ON attempts(job_uid, attempt_no)')


    # 序号分配 ---------------------------------------------------------

    def _queue_position_range(self, conn: sqlite3.Connection, count: int, front: bool) -> list[int]:
        """分配 count 个保序 queue_position：front 取 min_queue_position - count..min_queue_position-1，back 取 max_queue_position+1..。

        调用方必须已通过 ``BEGIN IMMEDIATE`` 持写事务：MIN/MAX 的读
        与调用方随后的 INSERT 必须原子——否则两个连接读到同一 min/max
        后各自分配，落盘重复 queue_position（idx_queue_position 非唯一索引，拦不住），
        队头保序契约被破坏。
        """
        if count <= 0:
            return []
        min_queue_position, max_queue_position = conn.execute('SELECT MIN(queue_position), MAX(queue_position) FROM queue').fetchone()
        if min_queue_position is None:  # 空表
            min_queue_position, max_queue_position = 0, -1
        if front:
            base = min_queue_position - count
            return list(range(base, min_queue_position))  # 首条最小，保序
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

    def load_failure_payloads(self) -> dict[str, dict[str, Any]]:
        """读取失败档案各 job_uid 的原始业务 payload 快照（无快照的 job_uid 不出现）。"""
        if not self.path.exists():
            return {}
        with self._get_conn() as conn:
            cursor = conn.execute(
                'SELECT job_uid, payload FROM failed WHERE payload IS NOT NULL'
            )
            return {row[0]: loads(row[1]) for row in cursor}

    def load_cursors(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        with self._get_conn() as conn:
            cursor = conn.execute('SELECT key, value FROM cursors')
            return {row[0]: row[1] for row in cursor}

    def load_queue(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        with self._get_conn() as conn:
            cursor = conn.execute(
                "SELECT job_snapshot FROM queue WHERE status = 'pending' ORDER BY queue_position ASC"
            )
            return [public_queue_snapshot(loads(row[0])) for row in cursor]

    # 整表重写 ---------------------------------------------------------

    def _rewrite_queue_rows(self, conn: sqlite3.Connection, job_snapshots: list[dict[str, Any]]) -> None:
        """queue 表整表重写的单一出口（save_queue 与 replace_queue_atomic 共用）。

        写变前先经 ``validate_queue_replacement`` 校验替换集（None/非法形状
        fail-loud 抛 TypeError，不触发 DELETE）——否则 None 会让「DELETE
        全表 + INSERT 全跳」静默清空队列且事务正常提交。
        保存兜底去重：传入重复 job_uid 时保留首条 + 告警（而非 REPLACE 静默
        覆盖为最后一条）——与加载期去重策略一致，杜绝队列 job_uid 索引与
        queue list 的漂移。调用方必须已持写事务（显式或隐式）。
        """
        validate_queue_replacement(job_snapshots)
        conn.execute('DELETE FROM queue')
        if job_snapshots:
            seen: set[str] = set()
            rows: list[tuple] = []
            for j in job_snapshots:
                u = job_uid_from_snapshot(j)
                if u in seen:
                    logger.warning(
                        f"save_queue: duplicate job_uid {u} dropped (kept first)."
                    )
                    continue
                seen.add(u)
                rows.append((
                    u,
                    len(rows),
                    dumps(j),
                    j.get("status", "pending"),
                    j.get("run_id"),
                    j.get("attempt_no"),
                    j.get("started_at"),
                ))
            conn.executemany(
                'INSERT OR REPLACE INTO queue ('
                'job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at'
                ') VALUES (?, ?, ?, ?, ?, ?, ?)',
                rows,
            )

    def save_queue(self, job_snapshots: list[dict[str, Any]]) -> None:
        """全量重写队列（测试装配 / replace_queue_atomic 事务内步骤，罕见 O(N)）。

        红线：无读基准的整表覆盖——崩溃恢复路径的「合并保存」严禁直接
        调用本方法（读-改-写窗口会抹除窗口期他进程已应答的入队），
        必须经 replace_queue_atomic 收敛进单个写事务。
        """
        try:
            with self._get_conn() as conn:
                self._rewrite_queue_rows(conn, job_snapshots)
        except Exception as e:
            logger.critical(f"Failed to save queue to {self.path.name}: {e}")
            raise

    def replace_queue_atomic(
        self,
        compute: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    ) -> None:
        try:
            with self._get_conn() as conn:
                # 读-改-写事务纪律：先取写锁再读磁盘真相，compute 与写回
                # 同事务——并发 enqueue 要么先于本事务提交（进入磁盘真相、
                # 参与合并），要么等本事务提交后再落盘，绝无中间态覆盖。
                conn.execute('BEGIN IMMEDIATE')
                disk_queue = [
                    loads(row[0])
                    for row in conn.execute(
                        'SELECT job_snapshot FROM queue ORDER BY queue_position ASC'
                    )
                ]
                self._rewrite_queue_rows(conn, compute(disk_queue))
        except Exception as e:
            logger.critical(f"Failed to replace queue atomically in {self.path.name}: {e}")
            raise

    # delta 提交 ---------------------------------------------------------


    def _write_failed_row(
        self,
        conn: sqlite3.Connection,
        job_uid: str,
        meta: dict,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """失败档案行写入的单一出口（commit_job_failure / commit_job_failures 共用）。

        failed 按 job_uid 唯一终态：INSERT OR REPLACE（最终状态唯一）；失败
        次数与执行时间线由 attempts 旁路轨迹承接，本表不注入计数字段、
        不解释 meta 内容。

        ``payload`` 是原始业务输入的 JSON 快照（补跑用）；传 None
        时保留既有快照不覆盖——无 payload 的写入路径（bulk 级联/append）
        不得抹掉此前快照。

        调用方必须已通过 ``BEGIN IMMEDIATE`` 持写事务：保留快照的
        SELECT → REPLACE 序列在 autocommit 下会丢并发快照（两个连接各
        基于同一旧行决定保留/覆盖，交错落盘后快照来源不确定）。
        """
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
            (job_uid, dumps(merged),
             dumps(effective_payload) if effective_payload is not None else None),
        )

    def commit_job_failure(
        self, job_uid: str, failure_metadata: dict, payload: dict[str, Any] | None = None
    ) -> bool:
        """原子 delta：写失败档案 + 删 popped job_uid + 删 successes 同名行。

        失败时 on-disk 队列不变。同一事务内 ``DELETE FROM successes``——重跑
        任务失败时 successes 里的旧成功记录必须作废（最终状态唯一）；否则磁盘
        successes∩failed 并存，破坏全局互斥。事务内单出口、原子、幂等（once
        任务失败时 successes 本无该行，DELETE no-op）。
        """
        try:
            with self._get_conn() as conn:
                # 快照保留的读-改-写必须持写事务串行化（详见 _get_conn）。
                conn.execute('BEGIN IMMEDIATE')
                self._write_failed_row(conn, job_uid, failure_metadata, payload)
                conn.execute('DELETE FROM queue WHERE job_uid = ?', (job_uid,))
                conn.execute('DELETE FROM successes WHERE job_uid = ?', (job_uid,))
        except Exception as e:
            logger.critical(f"Failed to commit job failure for {job_uid} in {self.path.name}: {e}")
            raise
        return True

    def commit_skip(self, job_uid: str) -> None:
        """移除 Trigger False 或重复候选；不写 successes/failed。"""
        try:
            with self._get_conn() as conn:
                conn.execute('DELETE FROM queue WHERE job_uid = ?', (job_uid,))
        except Exception as e:
            logger.critical(f"Failed to commit skip for {job_uid} in {self.path.name}: {e}")
            raise


    def commit_job_failures(self, job_uids_metadata: list[tuple[str, dict]]) -> None:
        """原子 delta：批量写失败档案 + 按 job_uid 精准删除队列行 + 批量清 successes 旧记录。

        与 commit_job_failure 对称，同一事务内批量 ``DELETE FROM successes``
        ——重跑任务被级联/死锁批量失败时，successes 旧成功记录作废（最终状态
        唯一）；剩余（未删除）行保持原 queue_position 顺序。失败时 on-disk 队列不变。
        """
        try:
            with self._get_conn() as conn:
                # 快照保留的读-改-写必须持写事务串行化（详见 _get_conn）。
                conn.execute('BEGIN IMMEDIATE')
                for job_uid, meta in job_uids_metadata:
                    self._write_failed_row(conn, job_uid, meta)
                conn.executemany(
                    'DELETE FROM queue WHERE job_uid = ?',
                    [(job_uid,) for job_uid, _ in job_uids_metadata],
                )
                conn.executemany(
                    'DELETE FROM successes WHERE job_uid = ?',
                    [(job_uid,) for job_uid, _ in job_uids_metadata],
                )
        except Exception as e:
            logger.critical(f"Failed to commit bulk failure in {self.path.name}: {e}")
            raise


    # 增量入队 -----------------------------------------------------------

    def enqueue_jobs(self, job_snapshots: list[dict[str, Any]], *, front: bool = False) -> list[str]:
        """批量增量入队：单事务原子插入，跳过重复 job_uid。

        与 save_queue 的区别：不做 DELETE 全表重写——与运行中 run() 的
        delta commit 并发时不覆盖其已提交变更（写入侧不覆盖，读-改-写
        窗口仍由 replace_queue_atomic 收敛）。失败抛异常（不吞）。
        """
        if not job_snapshots:
            return []
        try:
            with self._get_conn() as conn:
                # 去重 SELECT 与 queue_position 分配读必须与后续 INSERT 同事务：
                # 先取写锁再读，并发 front 入队才不会基于过期 min/max(queue_position)
                # 分配出重复 queue_position（事务纪律详见 _get_conn docstring）。
                conn.execute('BEGIN IMMEDIATE')
                existing = {
                    row[0] for row in conn.execute('SELECT job_uid FROM queue')
                }
                fresh: list[dict[str, Any]] = []
                batch_seen: set[str] = set()
                for j in job_snapshots:
                    u = job_uid_from_snapshot(j)
                    # 批次内重复（同一 enqueue 调用传相同 job_uid 两次）与
                    # 队列中已有的 job_uid 都跳过（首个存储，后续跳过）。
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

    # 定向删除与种子 -------------------------------------------------------

    def delete_queue_uids(self, job_uids: list[str]) -> int:
        """按 job_uid 定向批量删除队列行（加载期 repair 差量落盘），不触碰其余行。"""
        if not job_uids:
            return 0
        try:
            with self._get_conn() as conn:
                cur = conn.executemany(
                    'DELETE FROM queue WHERE job_uid = ?', [(u,) for u in job_uids]
                )
                return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            logger.critical(f"Failed to delete queue uids in {self.path.name}: {e}")
            raise

    def delete_failed(self, job_uids: list[str]) -> int:
        """从失败档案批量删除指定 job_uid（clear_failures 的后端实现）。"""
        if not job_uids:
            return 0
        try:
            with self._get_conn() as conn:
                cur = conn.executemany(
                    'DELETE FROM failed WHERE job_uid = ?', [(u,) for u in job_uids]
                )
                return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            logger.critical(f"Failed to delete failed entries in {self.path.name}: {e}")
            raise

    def delete_successes(self, job_uids: list[str]) -> int:
        """从 successes 批量删除指定 job_uid（clear_history 的后端实现）。"""
        if not job_uids:
            return 0
        try:
            with self._get_conn() as conn:
                cur = conn.executemany(
                    'DELETE FROM successes WHERE job_uid = ?', [(u,) for u in job_uids]
                )
                return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            logger.critical(f"Failed to delete successes entries in {self.path.name}: {e}")
            raise

    def apply_manual_status(
        self,
        updates: Sequence[tuple[str, str, dict[str, Any], dict[str, Any] | None]],
    ) -> None:
        """批量原子应用人工状态变更 (job_uid, status, snapshot, metadata)。"""
        if not updates:
            return
        with self._get_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for job_uid, status, snapshot, metadata in updates:
                if status == "pending":
                    conn.execute("DELETE FROM successes WHERE job_uid = ?", (job_uid,))
                    conn.execute("DELETE FROM failed WHERE job_uid = ?", (job_uid,))
                    row = conn.execute(
                        "SELECT queue_position FROM queue WHERE job_uid = ?", (job_uid,)
                    ).fetchone()
                    if row is not None:
                        conn.execute(
                            "UPDATE queue SET job_snapshot = ?, status = 'pending', run_id = NULL, attempt_no = NULL, started_at = NULL WHERE job_uid = ?",
                            (dumps(snapshot), job_uid),
                        )
                    else:
                        pos = self._queue_position_range(conn, 1, front=False)[0]
                        conn.execute(
                            "INSERT INTO queue (job_uid, queue_position, job_snapshot, status) VALUES (?, ?, ?, 'pending')",
                            (job_uid, pos, dumps(snapshot)),
                        )
                elif status == "succeeded":
                    conn.execute("DELETE FROM queue WHERE job_uid = ?", (job_uid,))
                    conn.execute("DELETE FROM failed WHERE job_uid = ?", (job_uid,))
                    conn.execute(
                        "INSERT OR REPLACE INTO successes (job_uid, metadata) VALUES (?, ?)",
                        (job_uid, dumps(metadata or {})),
                    )
                elif status == "failed":
                    conn.execute("DELETE FROM queue WHERE job_uid = ?", (job_uid,))
                    conn.execute("DELETE FROM successes WHERE job_uid = ?", (job_uid,))
                    payload = snapshot.get("payload") or {}
                    conn.execute(
                        "INSERT OR REPLACE INTO failed (job_uid, metadata, payload) VALUES (?, ?, ?)",
                        (job_uid, dumps(metadata or {}), dumps(payload)),
                    )
                else:
                    raise ValueError(f"Unsupported manual status: {status!r}")

    @staticmethod
    def _snapshot_from_metadata(raw: str | None) -> dict[str, Any] | None:
        if not raw:
            return None
        metadata = loads(raw)
        if not isinstance(metadata, dict):
            return None
        snapshot = metadata.get("job_snapshot")
        return snapshot if isinstance(snapshot, dict) else None

    def load_known_job_snapshot(self, uid: str) -> dict[str, Any] | None:
        """按队列、成功、失败、Attempt 历史的优先级读取完整配置。"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT job_snapshot FROM queue WHERE job_uid = ?", (uid,)
            ).fetchone()
            if row and row[0]:
                snapshot = loads(row[0])
                if isinstance(snapshot, dict):
                    return public_queue_snapshot(snapshot)

            row = conn.execute(
                "SELECT metadata FROM successes WHERE job_uid = ?", (uid,)
            ).fetchone()
            if row:
                snapshot = self._snapshot_from_metadata(row[0])
                if snapshot is not None:
                    return snapshot

            row = conn.execute(
                "SELECT metadata FROM failed WHERE job_uid = ?", (uid,)
            ).fetchone()
            if row:
                snapshot = self._snapshot_from_metadata(row[0])
                if snapshot is not None:
                    return snapshot

            row = conn.execute(
                "SELECT job_snapshot FROM attempts "
                "WHERE job_uid = ? AND job_snapshot IS NOT NULL "
                "ORDER BY attempt_id DESC LIMIT 1",
                (uid,),
            ).fetchone()
            if row and row[0]:
                snapshot = loads(row[0])
                if isinstance(snapshot, dict):
                    return snapshot
        return None

    def load_known_job_snapshots(self) -> dict[str, dict[str, Any]]:
        """读取全部已知 Job 的完整配置，低优先级来源先写入。"""
        known: dict[str, dict[str, Any]] = {}
        with self._get_conn() as conn:
            for uid, raw in conn.execute(
                "SELECT job_uid, job_snapshot FROM attempts "
                "WHERE job_snapshot IS NOT NULL ORDER BY attempt_id ASC"
            ):
                snapshot = loads(raw) if raw else None
                if isinstance(snapshot, dict):
                    known[uid] = snapshot

            for uid, raw in conn.execute("SELECT job_uid, metadata FROM failed"):
                snapshot = self._snapshot_from_metadata(raw)
                if snapshot is not None:
                    known[uid] = snapshot

            for uid, raw in conn.execute("SELECT job_uid, metadata FROM successes"):
                snapshot = self._snapshot_from_metadata(raw)
                if snapshot is not None:
                    known[uid] = snapshot

            for uid, raw in conn.execute("SELECT job_uid, job_snapshot FROM queue"):
                snapshot = loads(raw) if raw else None
                if isinstance(snapshot, dict):
                    known[uid] = public_queue_snapshot(snapshot)
        return known

    def clear_history(self, uid: str, records: set[str]) -> int:
        """从 successes/failed/attempts 清除指定 job_uid 的记录，返回删除条数。"""
        if not self.path.exists():
            return 0
        total = 0
        with self._get_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if "successes" in records:
                cur = conn.execute("DELETE FROM successes WHERE job_uid = ?", (uid,))
                total += cur.rowcount if cur.rowcount is not None else 0
            if "failures" in records:
                cur = conn.execute("DELETE FROM failed WHERE job_uid = ?", (uid,))
                total += cur.rowcount if cur.rowcount is not None else 0
            if "attempts" in records:
                cur = conn.execute("DELETE FROM attempts WHERE job_uid = ?", (uid,))
                total += cur.rowcount if cur.rowcount is not None else 0
        return total

    def get_cursor(self, key: str) -> str | None:
        """查询单个游标的最新已提交值，不存在返回 None。"""
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        if not self.path.exists():
            return None
        with self._get_conn() as conn:
            row = conn.execute("SELECT value FROM cursors WHERE key = ?", (key,)).fetchone()
            return row[0] if row is not None else None

    def set_cursor(self, key: str, value: str | None) -> None:
        """设置或删除单个游标（None 为删除）。"""
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        if value is not None and not isinstance(value, str):
            raise TypeError(f"cursor value must be str or None, got {type(value).__name__}")
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

    def get_engine_metadata(self, key: str) -> str | None:
        if not self.path.exists():
            return None
        with self._get_conn() as conn:
            row = conn.execute('SELECT value FROM engine_metadata WHERE key = ?', (key,)).fetchone()
            return row[0] if row else None

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
        if not isinstance(execution_id, str) or not execution_id:
            raise ValueError("execution_id must be a non-empty str")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty str")
        if not execution_id_matches_run(execution_id, run_id):
            raise ValueError(f"execution_id {execution_id!r} does not belong to run {run_id!r}")
        if not isinstance(started_at, str) or not started_at:
            raise ValueError("started_at must be a non-empty str")
        with self._get_conn() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute(
                'SELECT status, job_snapshot FROM queue WHERE job_uid = ?',
                (job_uid,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Job {job_uid} not found in queue")
            if row[0] == "running":
                raise ValueError(f"Job {job_uid} is already running")
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

    def get_attempt_sequence(self, job_uid: str) -> int:
        if not self.path.exists():
            return 0
        with self._get_conn() as conn:
            row = conn.execute(
                'SELECT last_attempt_no FROM job_sequences WHERE job_uid = ?',
                (job_uid,),
            ).fetchone()
            return row[0] if row is not None else 0

    def clear_attempts(self, job_uid: str) -> int:
        if not self.path.exists():
            return 0
        with self._get_conn() as conn:
            conn.execute('BEGIN IMMEDIATE')
            cur = conn.execute('DELETE FROM attempts WHERE job_uid = ?', (job_uid,))
            return cur.rowcount

    def load_settled_attempts(self, job_uid: str) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        with self._get_conn() as conn:
            cursor = conn.execute(
                'SELECT attempt_id, job_uid, attempt_no, run_id, outcome, started_at, settled_at, '
                'job_snapshot, files, result, error_message '
                'FROM attempts WHERE job_uid = ? ORDER BY attempt_no ASC',
                (job_uid,),
            )
            results: list[dict[str, Any]] = []
            for row in cursor:
                results.append({
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
            return results

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

        if outcome not in ("success", "failure", "retry", "pending"):
            raise ValueError(f"Invalid outcome: {outcome}")
        if not isinstance(attempt_no, int) or isinstance(attempt_no, bool) or attempt_no < 1:
            raise TypeError(f"attempt_no must be int >= 1, got {attempt_no!r}")
        if not isinstance(execution_id, str) or not execution_id:
            raise ValueError(f"execution_id must be non-empty str, got {execution_id!r}")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError(f"run_id must be non-empty str, got {run_id!r}")
        if not execution_id_matches_run(execution_id, run_id):
            raise ValueError(f"execution_id {execution_id!r} does not belong to run {run_id!r}")
        if not isinstance(settled_at, str) or not settled_at:
            raise ValueError(f"settled_at must be non-empty str, got {settled_at!r}")
        if started_at is not None and (not isinstance(started_at, str) or not started_at):
            raise ValueError("started_at must be non-empty str or None")
        if cursor_updates:
            for k, v in cursor_updates.items():
                if not isinstance(k, str) or not k:
                    raise ValueError(f"cursor key must be a non-empty str, got {k!r}")
                if v is not None and not isinstance(v, str):
                    raise TypeError(f"cursor value must be str or None, got {type(v).__name__}")
        if spawned_job_snapshots:
            validate_queue_replacement(list(spawned_job_snapshots))

        with self._get_conn() as conn:
            conn.execute('BEGIN IMMEDIATE')

            running = conn.execute(
                "SELECT status, run_id, attempt_no, job_snapshot "
                "FROM queue WHERE job_uid = ?",
                (job_uid,),
            ).fetchone()
            if running is None:
                raise TaskLiteError(f"Settlement does not match the running execution for {job_uid}")
            stored_job = loads(running[3]) if running[3] else None
            if not isinstance(stored_job, dict):
                raise TaskLiteError(
                    f"Settlement does not match the running execution for {job_uid}"
                )
            if (
                running[:3] != ("running", run_id, attempt_no)
                or queue_snapshot_execution_id(stored_job) != execution_id
            ):
                raise TaskLiteError(
                    f"Settlement does not match the running execution for {job_uid}"
                )
            if conn.execute(
                "SELECT 1 FROM attempts WHERE job_uid = ? AND attempt_no = ?", (job_uid, attempt_no)
            ).fetchone():
                raise TaskLiteError(f"Execution for {job_uid} attempt {attempt_no} is already settled")

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
                            dumps(j),
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
                if requeued_job_snapshot is not None:
                    conn.execute(
                        'UPDATE queue SET job_snapshot = ?, status = ?, run_id = NULL, '
                        'attempt_no = NULL, started_at = NULL WHERE job_uid = ?',
                        (
                            dumps(public_queue_snapshot(requeued_job_snapshot)),
                            "pending",
                            job_uid,
                        ),
                    )
                else:
                    conn.execute(
                        'UPDATE queue SET job_snapshot = ?, status = ?, run_id = NULL, '
                        'attempt_no = NULL, started_at = NULL WHERE job_uid = ?',
                        (dumps(public_queue_snapshot(stored_job)), "pending", job_uid),
                    )

            elif outcome == "pending":
                conn.execute(
                    'UPDATE queue SET job_snapshot = ?, status = ?, run_id = NULL, '
                    'attempt_no = NULL, started_at = NULL WHERE job_uid = ?',
                    (dumps(public_queue_snapshot(stored_job)), "pending", job_uid),
                )

    def record_authorization(
        self,
        path: str,
        target_type: str,
        *,
        authorized_at: str | None = None,
    ) -> None:
        norm_path = os.path.normpath(os.fspath(path))
        if target_type not in ("file", "directory"):
            raise ValueError(f"target_type must be 'file' or 'directory', got {target_type!r}")
        ts = authorized_at if authorized_at is not None else utc_now_iso()
        with self._get_conn() as conn:
            conn.execute(
                'INSERT OR REPLACE INTO authorizations (path, target_type, authorized_at) VALUES (?, ?, ?)',
                (norm_path, target_type, ts),
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
        norm_path = os.path.normpath(os.fspath(path))
        with self._get_conn() as conn:
            cur = conn.execute('DELETE FROM authorizations WHERE path = ?', (norm_path,))
            return bool(cur.rowcount and cur.rowcount > 0)

    def is_path_authorized(self, path: str, target_type: str) -> bool:
        norm_path = os.path.normpath(os.fspath(path))
        target_p = Path(norm_path)
        with self._get_conn() as conn:
            cur = conn.execute(
                'SELECT 1 FROM authorizations WHERE path = ? AND target_type = ?',
                (norm_path, target_type),
            )
            if cur.fetchone() is not None:
                return True
            cur = conn.execute("SELECT path FROM authorizations WHERE target_type = 'directory'")
            for row in cur.fetchall():
                dir_path = Path(row[0])
                try:
                    if target_p.is_relative_to(dir_path):
                        return True
                except (ValueError, TypeError):
                    pass
        return False

    def close(self) -> None:
        """关闭后端并释放资源（幂等）。"""
        pass


__all__ = ["SQLiteStateBackend", "TARGET_SCHEMA_VERSION"]

