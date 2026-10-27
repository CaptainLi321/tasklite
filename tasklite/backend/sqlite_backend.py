"""SQLite 状态后端（SQLiteStateBackend）。

queue / successes / failed / attempts / cursors / engine_metadata 六表合一的单 ACID 库。
全新 schema、零旧库兼容：user_version 只认本代版本号或全新库（0），
任何旧版本（含 v1 各代）fail-loud 拒绝启动，不写迁移路径。
"""
from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .base import (
    AbstractStateBackend,
    validate_attempt_dispatch,
    validate_attempt_finish,
    validate_queue_replacement,
)
from ..models.attempt import AttemptRecord
from ..models.state import job_uid_from_snapshot
from ..utils.jsonutil import dumps, loads

logger = logging.getLogger("tasklite")

# 只接纳本代完整形状或真正空库，不提供自动迁移。
_SCHEMA_VERSION = 4

# 核心表的精确列集合；不扩大为 SQL 类型与约束审计。
_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "queue": ("job_uid", "queue_position", "job_snapshot"),
    "successes": ("job_uid", "metadata"),
    "failed": ("job_uid", "metadata", "payload"),
    "attempts": (
        "attempt_id", "job_uid", "activation_no", "attempt_no", "execution_id",
        "run_id", "started_at", "finished_at", "outcome", "error_message",
    ),
    "cursors": ("key", "value"),
    "engine_metadata": ("key", "value"),
}


class SQLiteStateBackend(AbstractStateBackend):
    """原子 SQLite 状态后端：逐 job 单事务碎片化的 delta 提交。"""

    def __init__(self, filepath: str | Path):
        self.path = Path(filepath)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._init_db()
        except Exception as e:
            raise self._schema_error(f"SQLite backend initialization failed: {e}") from e

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

    def _schema_error(self, detail: str) -> RuntimeError:
        return RuntimeError(
            f"{self.path}: {detail}; 目标 schema {_SCHEMA_VERSION}。"
            "请保留原库，使用匹配版本读取历史，或显式选择新的状态库路径。"
        )

    def _check_schema(self, conn: sqlite3.Connection) -> int:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        objects = list(conn.execute(
            "SELECT name, type FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'"
        ))
        if version == 0:
            if objects:
                raise self._schema_error(
                    f"Unsupported table shape: version 0 非空库，已有对象 {sorted(objects)}"
                )
            return version
        if version != _SCHEMA_VERSION:
            raise self._schema_error(f"Unsupported database schema version {version}")
        table_objects = {name: kind for name, kind in objects if kind in ("table", "view")}
        if {"wall", "meta"} & {name.casefold() for name, kind in objects if kind == "table"}:
            raise self._schema_error(f"Unsupported table shape: version {version} 含异代 wall/meta 表")
        for table, expected in _TABLE_COLUMNS.items():
            if table_objects.get(table) != "table":
                raise self._schema_error(
                    f"Unsupported table shape for '{table}': version {version}, "
                    f"对象类型 {table_objects.get(table)!r}，必须为 table"
                )
            columns = {row[1] for row in conn.execute(f'PRAGMA table_xinfo("{table}")')}
            if columns != set(expected):
                raise self._schema_error(
                    f"Unsupported table shape for '{table}': version {version}, "
                    f"expected columns {expected}, found {sorted(columns)}"
                )
        return version

    def _preflight_schema(self) -> None:
        try:
            self.path.stat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise self._schema_error(f"无法只读确认版本或形状：{exc}") from exc
        conn = None
        try:
            # 普通只读连接必须看到 WAL 中已提交的版本与结构。
            conn = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)
            self._check_schema(conn)
        except (sqlite3.Error, OSError) as exc:
            raise self._schema_error(f"无法只读确认版本或形状：{exc}") from exc
        finally:
            if conn is not None:
                conn.close()

    def _init_db(self) -> None:
        self._preflight_schema()
        with self._get_conn() as conn:
            self._check_schema(conn)
            wal_row = conn.execute('PRAGMA journal_mode=WAL').fetchone()
            if not wal_row or str(wal_row[0]).lower() != 'wal':
                raise RuntimeError(
                    f"journal_mode=WAL could not be engaged on {self.path} "
                    f"(got {wal_row[0] if wal_row else None!r})."
                )
            conn.execute("BEGIN IMMEDIATE")
            version = self._check_schema(conn)
            if version == 0:
                self._create_tables(conn)
                self._create_indexes(conn)
                conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")

    def _create_tables(self, conn: sqlite3.Connection) -> None:
        """只在已确认的空库写事务中建立六张核心表。"""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS queue (
                job_uid TEXT PRIMARY KEY,
                queue_position INTEGER NOT NULL,
                job_snapshot TEXT NOT NULL
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
                activation_no INTEGER NOT NULL,
                attempt_no INTEGER NOT NULL,
                execution_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                outcome TEXT NOT NULL,
                error_message TEXT
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

    def _create_indexes(self, conn: sqlite3.Connection) -> None:
        """建索引（在形状校验之后：异形表先给出明确的拒绝信息，而非
        在缺列上建索引时抛出费解的 no such column）。"""
        conn.execute('CREATE INDEX IF NOT EXISTS idx_queue_position ON queue(queue_position)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_attempts_job_uid ON attempts(job_uid)')

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
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT job_uid, metadata FROM successes')
                return {row[0]: loads(row[1]) for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load successes from {self.path.name}: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Corrupted successes metadata in {self.path.name}: {e}") from e

    def load_failed(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT job_uid, metadata FROM failed')
                return {row[0]: loads(row[1]) for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load failed archive from {self.path.name}: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Corrupted failed metadata in {self.path.name}: {e}") from e

    def load_failure_payloads(self) -> dict[str, dict[str, Any]]:
        """读取失败档案各 job_uid 的原始业务 payload 快照（无快照的 job_uid 不出现）。"""
        if not self.path.exists():
            return {}
        try:
            with self._get_conn() as conn:
                cursor = conn.execute(
                    'SELECT job_uid, payload FROM failed WHERE payload IS NOT NULL'
                )
                return {row[0]: loads(row[1]) for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load failure payloads from {self.path.name}: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Corrupted failed payload in {self.path.name}: {e}") from e

    def load_cursors(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT key, value FROM cursors')
                return {row[0]: row[1] for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load cursors from {self.path.name}: {e}") from e

    def load_queue(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT job_snapshot FROM queue ORDER BY queue_position ASC')
                return [loads(row[0]) for row in cursor]
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load queue from {self.path.name}: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Corrupted queue job_snapshot in {self.path.name}: {e}") from e

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
            rows: list[tuple[str, int, str]] = []
            for j in job_snapshots:
                u = job_uid_from_snapshot(j)
                if u in seen:
                    logger.warning(
                        f"save_queue: duplicate job_uid {u} dropped (kept first)."
                    )
                    continue
                seen.add(u)
                rows.append((u, len(rows), dumps(j)))
            conn.executemany(
                'INSERT OR REPLACE INTO queue (job_uid, queue_position, job_snapshot) VALUES (?, ?, ?)',
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

    def commit_job_success(
        self,
        job_uid: str,
        result_metadata: dict,
        *,
        spawned_job_snapshots: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
    ) -> bool:
        """原子 delta：写 successes + 删 popped job_uid + 删 failed 残行 + 队头插 spawned + 更新 cursors。

        失败时事务回滚，on-disk 队列不变（popped job_uid 仍在磁盘）。
        """
        try:
            with self._get_conn() as conn:
                # spawned 队头插入的 queue_position 分配是读-改-写：先取写锁再读，
                # 与并发的 front 入队互斥，否则两方读到同一 min_queue_position。
                conn.execute('BEGIN IMMEDIATE')
                conn.execute(
                    'INSERT OR REPLACE INTO successes (job_uid, metadata) VALUES (?, ?)',
                    (job_uid, dumps(result_metadata or {})),
                )
                conn.execute('DELETE FROM queue WHERE job_uid = ?', (job_uid,))
                # 成功 commit 时清理 failed 同名残行——否则同一 job_uid 永久
                # 「既成功又失败」（失败档案与 successes 矛盾）。幂等，与
                # 「job 最终状态唯一」语义一致。
                conn.execute('DELETE FROM failed WHERE job_uid = ?', (job_uid,))
                if spawned_job_snapshots:
                    queue_positions = self._queue_position_range(conn, len(spawned_job_snapshots), front=True)
                    cur = conn.executemany(
                        'INSERT OR IGNORE INTO queue (job_uid, queue_position, job_snapshot) VALUES (?, ?, ?)',
                        [(job_uid_from_snapshot(j), pos, dumps(j))
                         for j, pos in zip(spawned_job_snapshots, queue_positions)],
                    )
                    # 防止 INSERT OR IGNORE 静默丢弃 spawn：若某个 spawned
                    # job_uid 与磁盘队列冲突（内存 is_known 预筛后的漂移窗口），
                    # 该 job 只存在于内存、磁盘从未持久化，进程崩溃后静默
                    # 丢失（at-least-once 违约）。executemany 的 rowcount
                    # 返回实际插入行数，rowcount < len 即检测到漂移 → 抛
                    # 异常回滚返回 False 走崩溃契约（重启后 at-least-once
                    # 重扫），而非静默掩盖。
                    if cur.rowcount is not None and cur.rowcount < len(spawned_job_snapshots):
                        logger.critical(
                            f"commit_job_success for {job_uid}: "
                            f"{len(spawned_job_snapshots) - cur.rowcount} spawned job(s) "
                            f"silently dropped by INSERT OR IGNORE (disk/memory "
                            f"drift). Returning False to trigger crash contract."
                        )
                        raise RuntimeError(
                            f"spawned job job_uid conflict on disk for {job_uid}: "
                            f"inserted {cur.rowcount}/{len(spawned_job_snapshots)}"
                        )
                if cursor_updates:
                    # None 值表示删除游标；非 None 值为 str（set_cursor 校验保证）
                    for k, v in cursor_updates.items():
                        if v is None:
                            conn.execute('DELETE FROM cursors WHERE key = ?', (k,))
                        else:
                            conn.execute(
                                'INSERT OR REPLACE INTO cursors (key, value) VALUES (?, ?)',
                                (k, v),
                            )
        except Exception as e:
            logger.critical(f"Failed to commit job success for {job_uid} in {self.path.name}: {e}")
            return False
        return True

    def _write_failed_row(
        self,
        conn: sqlite3.Connection,
        job_uid: str,
        meta: dict,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """失败档案行写入的单一出口（commit_job_failure / commit_job_failures / append_failed 共用）。

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
        successes∩failed 并存，破坏全局互斥。事务内单出口、原子、幂等（never
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
            return False
        return True

    def commit_skip(self, job_uid: str) -> bool:
        """delta 删除队列中已终态（重复）的 job_uid；不写 successes/failed。"""
        try:
            with self._get_conn() as conn:
                conn.execute('DELETE FROM queue WHERE job_uid = ?', (job_uid,))
        except Exception as e:
            logger.critical(f"Failed to commit skip for {job_uid} in {self.path.name}: {e}")
            return False
        return True

    def commit_retry(
        self, popped_job_uid: str, requeued_job_snapshot: dict[str, Any], *, front: bool = False
    ) -> bool:
        """原子 delta：DELETE popped_job_uid + 按 front 插入 requeued_job_snapshot（同 job_uid DELETE+INSERT）。

        不写 successes/failed。popped_job_uid 已先删除，故同 job_uid 重插安全（用
        INSERT 而非 REPLACE）。不用 INSERT OR IGNORE——若 requeued job_uid 与
        队列既有行冲突（本不该发生），静默丢弃会让 job 无声消失却返回
        True；普通 INSERT 让真实冲突抛异常 → 返回 False → 走崩溃契约。
        失败时 on-disk 队列不变（popped job_uid 仍在）。
        """
        try:
            with self._get_conn() as conn:
                # queue_position 分配的读与随后的 INSERT 之间不能让并发方插入提交：
                # 先取写锁再读（事务纪律详见 _get_conn docstring）。
                conn.execute('BEGIN IMMEDIATE')
                conn.execute('DELETE FROM queue WHERE job_uid = ?', (popped_job_uid,))
                queue_positions = self._queue_position_range(conn, 1, front=front)
                conn.execute(
                    'INSERT INTO queue (job_uid, queue_position, job_snapshot) VALUES (?, ?, ?)',
                    (job_uid_from_snapshot(requeued_job_snapshot), queue_positions[0], dumps(requeued_job_snapshot)),
                )
        except Exception as e:
            logger.critical(f"Failed to commit retry for {popped_job_uid} in {self.path.name}: {e}")
            return False
        return True

    def commit_job_failures(self, job_uids_metadata: list[tuple[str, dict]]) -> bool:
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
            return False
        return True

    def append_failed(self, job_uid: str, metadata: dict[str, Any] | None = None) -> None:
        try:
            with self._get_conn() as conn:
                # 快照保留的读-改-写必须持写事务串行化，否则并发 append
                # 基于同一旧快照各写各的（详见 _get_conn）。经
                # _write_failed_row 单一出口写入。
                conn.execute('BEGIN IMMEDIATE')
                self._write_failed_row(conn, job_uid, metadata or {})
        except Exception as e:
            logger.error(f"Failed to append to failed archive in {self.path.name}: {e}")
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
                    'INSERT INTO queue (job_uid, queue_position, job_snapshot) VALUES (?, ?, ?)',
                    [(job_uid_from_snapshot(j), pos, dumps(j))
                     for j, pos in zip(fresh, queue_positions)],
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

    def seed_successes(self, job_uids: list[str]) -> int:
        """把 job_uid 批量写入 successes（meta 空 dict）——存档迁移标记「已处理」。

        幂等：已存在的 job_uid 被覆盖（meta 重置为空）。
        不变式：successes/failed 全局互斥——已在 failed 的 job_uid 拒绝种子；
        冲突检查与写入收敛进同一 ``BEGIN IMMEDIATE`` 写事务（先取写锁
        再读，详见 _get_conn），冲突时零写入（整体拒绝），不静默清除
        失败档案记录（失败历史须先经 delete_failed 显式清除）。
        """
        if not job_uids:
            return 0
        unique_uids = list(dict.fromkeys(job_uids))
        try:
            with self._get_conn() as conn:
                conn.execute('BEGIN IMMEDIATE')
                placeholders = ','.join('?' * len(unique_uids))
                conflict = sorted(
                    row[0] for row in conn.execute(
                        f'SELECT job_uid FROM failed WHERE job_uid IN ({placeholders})',
                        unique_uids,
                    )
                )
                if conflict:
                    raise ValueError(
                        f"seed_successes refuses job_uid(s) already in failed: {conflict}; "
                        f"successes/failed must stay disjoint"
                    )
                cur = conn.executemany(
                    'INSERT OR REPLACE INTO successes (job_uid, metadata) VALUES (?, ?)',
                    [(u, dumps({})) for u in job_uids],
                )
                return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            logger.critical(f"Failed to seed successes in {self.path.name}: {e}")
            raise

    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（UPSERT，幂等）；入口校验与 Memory 腿同契约 fail-loud。"""
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        if not isinstance(value, str):
            raise TypeError(f"cursor value must be str, got {type(value).__name__}")
        try:
            with self._get_conn() as conn:
                conn.execute(
                    'INSERT OR REPLACE INTO cursors (key, value) VALUES (?, ?)',
                    (key, value),
                )
        except Exception as e:
            logger.critical(f"Failed to seed cursor '{key}' in {self.path.name}: {e}")
            raise

    # 尝试轨迹（append-only 旁路观测面）------------------------------------

    def append_attempt(self, record: AttemptRecord) -> int:
        """派发即插行：单语句 INSERT，自增 id 由 SQLite 原子分配（无读-改-写窗口）。"""
        validate_attempt_dispatch(record)
        try:
            with self._get_conn() as conn:
                cur = conn.execute(
                    'INSERT INTO attempts (job_uid, activation_no, attempt_no, '
                    'execution_id, run_id, started_at, finished_at, outcome, error_message) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (
                        record.job_uid, record.activation_no, record.attempt_no,
                        record.execution_id, record.run_id, record.started_at,
                        record.finished_at, record.outcome, record.error_message,
                    ),
                )
                return int(cur.lastrowid)
        except Exception as e:
            logger.critical(
                f"Failed to append attempt for {record.job_uid} in {self.path.name}: {e}"
            )
            raise

    def update_attempt(
        self,
        attempt_id: int,
        *,
        outcome: str,
        finished_at: str,
        error_message: str | None = None,
    ) -> bool:
        """收尾轨迹行：读-判-写在 ``BEGIN IMMEDIATE`` 写事务内串行完成。

        目标 id 不存在返回 False（旁路观测面缺行不触发崩溃契约）；对已
        收尾行再次收尾抛 ValueError（双重终结是调用方协议缺陷，
        fail-loud）。只 UPDATE 收尾三列，身份列永不改写。
        """
        validate_attempt_finish(outcome, finished_at, error_message)
        try:
            with self._get_conn() as conn:
                # 读 running 状态 → 判定 → UPDATE 是读-改-写：持写事务
                # 串行化，防并发双重收尾交错漏判（详见 _get_conn）。
                conn.execute('BEGIN IMMEDIATE')
                row = conn.execute(
                    'SELECT outcome FROM attempts WHERE attempt_id = ?', (attempt_id,)
                ).fetchone()
                if row is None:
                    return False
                if row[0] != "running":
                    raise ValueError(
                        f"attempt {attempt_id} already finalized with outcome "
                        f"{row[0]!r}; double finalization is a protocol violation"
                    )
                conn.execute(
                    'UPDATE attempts SET outcome = ?, finished_at = ?, error_message = ? '
                    'WHERE attempt_id = ?',
                    (outcome, finished_at, error_message, attempt_id),
                )
                return True
        except Exception as e:
            logger.critical(
                f"Failed to update attempt {attempt_id} in {self.path.name}: {e}"
            )
            raise

    def load_attempts(self, job_uid: str) -> list[AttemptRecord]:
        if not self.path.exists():
            return []
        try:
            with self._get_conn() as conn:
                rows = conn.execute(
                    'SELECT job_uid, activation_no, attempt_no, execution_id, '
                    'run_id, started_at, finished_at, outcome, error_message '
                    'FROM attempts WHERE job_uid = ? ORDER BY attempt_id ASC',
                    (job_uid,),
                ).fetchall()
                return [
                    AttemptRecord.from_dict(dict(zip(_TABLE_COLUMNS["attempts"][1:], row)))
                    for row in rows
                ]
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(
                f"Failed to load attempts for {job_uid!r} from {self.path.name}: {e}"
            ) from e

    # 元数据 ---------------------------------------------------------------

    def get_engine_metadata(self, key: str) -> str | None:
        if not self.path.exists():
            return None
        try:
            with self._get_conn() as conn:
                row = conn.execute('SELECT value FROM engine_metadata WHERE key = ?', (key,)).fetchone()
                return row[0] if row else None
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to read engine_metadata '{key}' from {self.path.name}: {e}") from e

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


__all__ = ["SQLiteStateBackend"]
