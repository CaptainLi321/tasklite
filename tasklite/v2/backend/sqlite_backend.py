"""v2 SQLite 状态后端（SQLiteStateBackend）。

queue / wall / failed / attempts / cursors / meta 六表合一的单 ACID 库。
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
from ..models.state import uid_from_job_dict
from ..utils.jsonutil import dumps, loads

logger = logging.getLogger("tasklite.v2")

# v2 首代 schema 版本。版本号空间与 v1 连续（v1 各代均低于本值），
# 旧库一律拒绝——v2 表结构全新，不存在可迁移路径。
_SCHEMA_VERSION = 3

# 期望表列形状（建表 DDL 的单一事实源；启动时校验既有表形状，捕获
# user_version=0 但表已存在的手改库 / 半建库）。
_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "queue": ("uid", "seq", "job_data"),
    "wall": ("uid", "payload"),
    "failed": ("uid", "payload", "job_payload"),
    "attempts": (
        "id", "job_uid", "activation_no", "attempt_no", "incarnation",
        "run_id", "started_at", "finished_at", "outcome", "error",
    ),
    "cursors": ("k", "v"),
    "meta": ("k", "v"),
}

# attempts 查询列（SELECT 列序 → AttemptRecord.from_dict 键序）
_ATTEMPT_COLUMNS: tuple[str, ...] = _TABLE_COLUMNS["attempts"][1:]


class SQLiteStateBackend(AbstractStateBackend):
    """原子 SQLite 状态后端：逐 job 单事务碎片化的 delta 提交。"""

    def __init__(self, filepath: str | Path):
        self.path = Path(filepath)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._init_db()
        except Exception as e:
            raise RuntimeError(f"SQLite backend initialization failed: {e}") from e

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
        SELECT 在 autocommit 下逐条取快照——「读 min/max(seq) → 分配 →
        INSERT」「读 job_payload 快照 → REPLACE」「读 running 轨迹行 →
        UPDATE」若不显式开事务，两个连接会基于同一份旧快照各算各的，
        落盘出现重复 seq（idx_queue_seq 非唯一，拦不住）或丢快照。此类
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

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            # WAL 返回值必须验证——rollback journal（无 WAL）即使配
            # synchronous=FULL 也是 SQLite 文档标注的「断电可损坏/性能
            # 陷阱」配置。当 WAL 无法生效时（NFS 无锁/只读介质/被其他
            # 连接持有），PRAGMA 静默返回当前模式而不报错，引擎会以
            # 「断电安全」招牌运行在可损坏配置下。
            # fail-loud：宁可拒绝启动，不可带病运行。
            wal_row = conn.execute('PRAGMA journal_mode=WAL').fetchone()
            if not wal_row or str(wal_row[0]).lower() != 'wal':
                raise RuntimeError(
                    f"journal_mode=WAL could not be engaged on {self.path.name} "
                    f"(got {wal_row[0] if wal_row else None!r}). Refusing to run in "
                    f"a power-loss-corruptible configuration; check filesystem "
                    f"locking support (NFS) and that no other connection holds the DB."
                )
            user_ver = conn.execute("PRAGMA user_version").fetchone()[0]
            if user_ver not in (0, _SCHEMA_VERSION):
                raise RuntimeError(
                    f"Unsupported database schema version {user_ver} on "
                    f"{self.path.name}; this backend only accepts fresh databases "
                    f"or schema version {_SCHEMA_VERSION} (v2 schema is new, no "
                    f"migration path exists; recreate the state database)."
                )
            self._create_tables(conn)
            self._verify_table_shapes(conn)
            self._create_indexes(conn)
            if user_ver == 0:
                conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")

    def _create_tables(self, conn: sqlite3.Connection) -> None:
        """按 v2 schema 建表（全新库；既有表形状由形状校验把关）。"""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS queue (
                uid TEXT PRIMARY KEY,
                seq INTEGER NOT NULL,
                job_data TEXT NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS wall (
                uid TEXT PRIMARY KEY,
                payload TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS failed (
                uid TEXT PRIMARY KEY,
                payload TEXT,
                job_payload TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_uid TEXT NOT NULL,
                activation_no INTEGER NOT NULL,
                attempt_no INTEGER NOT NULL,
                incarnation TEXT NOT NULL,
                run_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                outcome TEXT NOT NULL,
                error TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS cursors (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS meta (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            )
        ''')

    def _create_indexes(self, conn: sqlite3.Connection) -> None:
        """建索引（在形状校验之后：异形表先给出明确的拒绝信息，而非
        在缺列上建索引时抛出费解的 no such column）。"""
        conn.execute('CREATE INDEX IF NOT EXISTS idx_queue_seq ON queue(seq)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_attempts_job_uid ON attempts(job_uid)')

    def _verify_table_shapes(self, conn: sqlite3.Connection) -> None:
        """既有表列形状必须与 v2 schema 完全一致，否则 fail-loud。

        user_version=0 但表已存在的库（手改/半建/异代库未写版本号）在此
        拦截——静默混用异代形状会让 INSERT 局部成功，坏行潜伏到加载期。
        """
        for table, expected in _TABLE_COLUMNS.items():
            cols = {
                row[1]
                for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if cols != set(expected):
                raise RuntimeError(
                    f"Unsupported table shape for '{table}' on {self.path.name}: "
                    f"expected columns {expected}, found {sorted(cols)}. "
                    f"v2 schema is new, no migration path exists; recreate the "
                    f"state database."
                )

    # 序号分配 ---------------------------------------------------------

    def _seq_range(self, conn: sqlite3.Connection, count: int, front: bool) -> list[int]:
        """分配 count 个保序 seq：front 取 min_seq - count..min_seq-1，back 取 max_seq+1..。

        调用方必须已通过 ``BEGIN IMMEDIATE`` 持写事务：MIN/MAX 的读
        与调用方随后的 INSERT 必须原子——否则两个连接读到同一 min/max
        后各自分配，落盘重复 seq（idx_queue_seq 非唯一索引，拦不住），
        队头保序契约被破坏。
        """
        if count <= 0:
            return []
        min_seq, max_seq = conn.execute('SELECT MIN(seq), MAX(seq) FROM queue').fetchone()
        if min_seq is None:  # 空表
            min_seq, max_seq = 0, -1
        if front:
            base = min_seq - count
            return list(range(base, min_seq))  # 首条最小，保序
        base = max_seq + 1
        return list(range(base, base + count))

    # 全量加载 ---------------------------------------------------------

    def load_wall(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT uid, payload FROM wall')
                return {row[0]: loads(row[1]) for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load wall from {self.path.name}: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Corrupted wall payload in {self.path.name}: {e}") from e

    def load_failed(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT uid, payload FROM failed')
                return {row[0]: loads(row[1]) for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load failed archive from {self.path.name}: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Corrupted failed payload in {self.path.name}: {e}") from e

    def load_failed_payloads(self) -> dict[str, dict[str, Any]]:
        """读取失败档案各 uid 的原始业务 payload 快照（无快照的 uid 不出现）。"""
        if not self.path.exists():
            return {}
        try:
            with self._get_conn() as conn:
                cursor = conn.execute(
                    'SELECT uid, job_payload FROM failed WHERE job_payload IS NOT NULL'
                )
                return {row[0]: loads(row[1]) for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load failure payloads from {self.path.name}: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Corrupted failed job_payload in {self.path.name}: {e}") from e

    def load_cursors(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT k, v FROM cursors')
                return {row[0]: row[1] for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load cursors from {self.path.name}: {e}") from e

    def load_queue(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT job_data FROM queue ORDER BY seq ASC')
                return [loads(row[0]) for row in cursor]
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load queue from {self.path.name}: {e}") from e
        except ValueError as e:
            raise RuntimeError(f"Corrupted queue payload in {self.path.name}: {e}") from e

    # 整表重写 ---------------------------------------------------------

    def _rewrite_queue_rows(self, conn: sqlite3.Connection, jobs: list[dict[str, Any]]) -> None:
        """queue 表整表重写的单一出口（save_queue 与 replace_queue_atomic 共用）。

        写变前先经 ``validate_queue_replacement`` 校验替换集（None/非法形状
        fail-loud 抛 TypeError，不触发 DELETE）——否则 None 会让「DELETE
        全表 + INSERT 全跳」静默清空队列且事务正常提交。
        保存兜底去重：传入重复 uid 时保留首条 + 告警（而非 REPLACE 静默
        覆盖为最后一条）——与加载期去重策略一致，杜绝队列 uid 索引与
        queue list 的漂移。调用方必须已持写事务（显式或隐式）。
        """
        validate_queue_replacement(jobs)
        conn.execute('DELETE FROM queue')
        if jobs:
            seen: set[str] = set()
            rows: list[tuple[str, int, str]] = []
            for j in jobs:
                u = uid_from_job_dict(j)
                if u in seen:
                    logger.warning(
                        f"save_queue: duplicate uid {u} dropped (kept first)."
                    )
                    continue
                seen.add(u)
                rows.append((u, len(rows), dumps(j)))
            conn.executemany(
                'INSERT OR REPLACE INTO queue (uid, seq, job_data) VALUES (?, ?, ?)',
                rows,
            )

    def save_queue(self, jobs: list[dict[str, Any]]) -> None:
        """全量重写队列（测试装配 / replace_queue_atomic 事务内步骤，罕见 O(N)）。

        红线：无读基准的整表覆盖——崩溃恢复路径的「合并保存」严禁直接
        调用本方法（读-改-写窗口会抹除窗口期他进程已应答的入队），
        必须经 replace_queue_atomic 收敛进单个写事务。
        """
        try:
            with self._get_conn() as conn:
                self._rewrite_queue_rows(conn, jobs)
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
                disk_q = [
                    loads(row[0])
                    for row in conn.execute(
                        'SELECT job_data FROM queue ORDER BY seq ASC'
                    )
                ]
                self._rewrite_queue_rows(conn, compute(disk_q))
        except Exception as e:
            logger.critical(f"Failed to replace queue atomically in {self.path.name}: {e}")
            raise

    # delta 提交 ---------------------------------------------------------

    def commit_job_success(
        self,
        uid: str,
        result_meta: dict,
        *,
        spawned_jobs: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
    ) -> bool:
        """原子 delta：写 wall + 删 popped uid + 删 failed 残行 + 队头插 spawned + 更新 cursors。

        失败时事务回滚，on-disk 队列不变（popped uid 仍在磁盘）。
        """
        try:
            with self._get_conn() as conn:
                # spawned 队头插入的 seq 分配是读-改-写：先取写锁再读，
                # 与并发的 front 入队互斥，否则两方读到同一 min_seq。
                conn.execute('BEGIN IMMEDIATE')
                conn.execute(
                    'INSERT OR REPLACE INTO wall (uid, payload) VALUES (?, ?)',
                    (uid, dumps(result_meta or {})),
                )
                conn.execute('DELETE FROM queue WHERE uid = ?', (uid,))
                # 成功 commit 时清理 failed 同名残行——否则同一 uid 永久
                # 「既成功又失败」（失败档案与 wall 矛盾）。幂等，与
                # 「job 最终状态唯一」语义一致。
                conn.execute('DELETE FROM failed WHERE uid = ?', (uid,))
                if spawned_jobs:
                    seqs = self._seq_range(conn, len(spawned_jobs), front=True)
                    cur = conn.executemany(
                        'INSERT OR IGNORE INTO queue (uid, seq, job_data) VALUES (?, ?, ?)',
                        [(uid_from_job_dict(j), s, dumps(j))
                         for j, s in zip(spawned_jobs, seqs)],
                    )
                    # 防止 INSERT OR IGNORE 静默丢弃 spawn：若某个 spawned
                    # uid 与磁盘队列冲突（内存 is_known 预筛后的漂移窗口），
                    # 该 job 只存在于内存、磁盘从未持久化，进程崩溃后静默
                    # 丢失（at-least-once 违约）。executemany 的 rowcount
                    # 返回实际插入行数，rowcount < len 即检测到漂移 → 抛
                    # 异常回滚返回 False 走崩溃契约（重启后 at-least-once
                    # 重扫），而非静默掩盖。
                    if cur.rowcount is not None and cur.rowcount < len(spawned_jobs):
                        logger.critical(
                            f"commit_job_success for {uid}: "
                            f"{len(spawned_jobs) - cur.rowcount} spawned job(s) "
                            f"silently dropped by INSERT OR IGNORE (disk/memory "
                            f"drift). Returning False to trigger crash contract."
                        )
                        raise RuntimeError(
                            f"spawned job uid conflict on disk for {uid}: "
                            f"inserted {cur.rowcount}/{len(spawned_jobs)}"
                        )
                if cursor_updates:
                    # None 值表示删除游标；非 None 值为 str（set_cursor 校验保证）
                    for k, v in cursor_updates.items():
                        if v is None:
                            conn.execute('DELETE FROM cursors WHERE k = ?', (k,))
                        else:
                            conn.execute(
                                'INSERT OR REPLACE INTO cursors (k, v) VALUES (?, ?)',
                                (k, v),
                            )
        except Exception as e:
            logger.critical(f"Failed to commit job success for {uid} in {self.path.name}: {e}")
            return False
        return True

    def _write_failed_row(
        self,
        conn: sqlite3.Connection,
        uid: str,
        meta: dict,
        job_payload: dict[str, Any] | None = None,
    ) -> None:
        """失败档案行写入的单一出口（commit_job_failure / commit_bulk_failure / append_failed 共用）。

        failed 按 uid 唯一终态：INSERT OR REPLACE（最终状态唯一）；失败
        次数与执行时间线由 attempts 旁路轨迹承接，本表不注入计数字段、
        不解释 meta 内容。

        ``job_payload`` 是原始业务 payload 的 JSON 快照（补跑用）；传 None
        时保留既有快照不覆盖——无 payload 的写入路径（bulk 级联/append）
        不得抹掉此前快照。

        调用方必须已通过 ``BEGIN IMMEDIATE`` 持写事务：保留快照的
        SELECT → REPLACE 序列在 autocommit 下会丢并发快照（两个连接各
        基于同一旧行决定保留/覆盖，交错落盘后快照来源不确定）。
        """
        row = conn.execute(
            'SELECT job_payload FROM failed WHERE uid = ?', (uid,)
        ).fetchone()
        merged = dict(meta or {})
        effective_payload = (
            job_payload
            if job_payload is not None
            else (loads(row[0]) if row is not None and row[0] is not None else None)
        )
        conn.execute(
            'INSERT OR REPLACE INTO failed (uid, payload, job_payload) VALUES (?, ?, ?)',
            (uid, dumps(merged),
             dumps(effective_payload) if effective_payload is not None else None),
        )

    def commit_job_failure(
        self, uid: str, result_meta: dict, job_payload: dict[str, Any] | None = None
    ) -> bool:
        """原子 delta：写失败档案 + 删 popped uid + 删 wall 同名行。

        失败时 on-disk 队列不变。同一事务内 ``DELETE FROM wall``——重跑
        任务失败时 wall 里的旧成功记录必须作废（最终状态唯一）；否则磁盘
        wall∩failed 并存，破坏全局互斥。事务内单出口、原子、幂等（never
        任务失败时 wall 本无该行，DELETE no-op）。
        """
        try:
            with self._get_conn() as conn:
                # 快照保留的读-改-写必须持写事务串行化（详见 _get_conn）。
                conn.execute('BEGIN IMMEDIATE')
                self._write_failed_row(conn, uid, result_meta, job_payload)
                conn.execute('DELETE FROM queue WHERE uid = ?', (uid,))
                conn.execute('DELETE FROM wall WHERE uid = ?', (uid,))
        except Exception as e:
            logger.critical(f"Failed to commit job failure for {uid} in {self.path.name}: {e}")
            return False
        return True

    def commit_skip(self, uid: str) -> bool:
        """delta 删除队列中已终态（重复）的 uid；不写 wall/failed。"""
        try:
            with self._get_conn() as conn:
                conn.execute('DELETE FROM queue WHERE uid = ?', (uid,))
        except Exception as e:
            logger.critical(f"Failed to commit skip for {uid} in {self.path.name}: {e}")
            return False
        return True

    def commit_retry(
        self, popped_uid: str, requeued_job: dict[str, Any], *, front: bool = False
    ) -> bool:
        """原子 delta：DELETE popped_uid + 按 front 插入 requeued_job（同 uid DELETE+INSERT）。

        不写 wall/failed。popped_uid 已先删除，故同 uid 重插安全（用
        INSERT 而非 REPLACE）。不用 INSERT OR IGNORE——若 requeued uid 与
        队列既有行冲突（本不该发生），静默丢弃会让 job 无声消失却返回
        True；普通 INSERT 让真实冲突抛异常 → 返回 False → 走崩溃契约。
        失败时 on-disk 队列不变（popped uid 仍在）。
        """
        try:
            with self._get_conn() as conn:
                # seq 分配的读与随后的 INSERT 之间不能让并发方插入提交：
                # 先取写锁再读（事务纪律详见 _get_conn docstring）。
                conn.execute('BEGIN IMMEDIATE')
                conn.execute('DELETE FROM queue WHERE uid = ?', (popped_uid,))
                seqs = self._seq_range(conn, 1, front=front)
                conn.execute(
                    'INSERT INTO queue (uid, seq, job_data) VALUES (?, ?, ?)',
                    (uid_from_job_dict(requeued_job), seqs[0], dumps(requeued_job)),
                )
        except Exception as e:
            logger.critical(f"Failed to commit retry for {popped_uid} in {self.path.name}: {e}")
            return False
        return True

    def commit_bulk_failure(self, uids_metas: list[tuple[str, dict]]) -> bool:
        """原子 delta：批量写失败档案 + 按 uid 精准删除队列行 + 批量清 wall 旧记录。

        与 commit_job_failure 对称，同一事务内批量 ``DELETE FROM wall``
        ——重跑任务被级联/死锁批量失败时，wall 旧成功记录作废（最终状态
        唯一）；剩余（未删除）行保持原 seq 顺序。失败时 on-disk 队列不变。
        """
        try:
            with self._get_conn() as conn:
                # 快照保留的读-改-写必须持写事务串行化（详见 _get_conn）。
                conn.execute('BEGIN IMMEDIATE')
                for uid, meta in uids_metas:
                    self._write_failed_row(conn, uid, meta)
                conn.executemany(
                    'DELETE FROM queue WHERE uid = ?',
                    [(uid,) for uid, _ in uids_metas],
                )
                conn.executemany(
                    'DELETE FROM wall WHERE uid = ?',
                    [(uid,) for uid, _ in uids_metas],
                )
        except Exception as e:
            logger.critical(f"Failed to commit bulk failure in {self.path.name}: {e}")
            return False
        return True

    def append_failed(self, uid: str, payload: dict[str, Any] | None = None) -> None:
        try:
            with self._get_conn() as conn:
                # 快照保留的读-改-写必须持写事务串行化，否则并发 append
                # 基于同一旧快照各写各的（详见 _get_conn）。经
                # _write_failed_row 单一出口写入。
                conn.execute('BEGIN IMMEDIATE')
                self._write_failed_row(conn, uid, payload or {})
        except Exception as e:
            logger.error(f"Failed to append to failed archive in {self.path.name}: {e}")
            raise

    # 增量入队 -----------------------------------------------------------

    def enqueue_jobs(self, jobs: list[dict[str, Any]], *, front: bool = False) -> list[str]:
        """批量增量入队：单事务原子插入，跳过重复 uid。

        与 save_queue 的区别：不做 DELETE 全表重写——与运行中 run() 的
        delta commit 并发时不覆盖其已提交变更（写入侧不覆盖，读-改-写
        窗口仍由 replace_queue_atomic 收敛）。失败抛异常（不吞）。
        """
        if not jobs:
            return []
        try:
            with self._get_conn() as conn:
                # 去重 SELECT 与 seq 分配读必须与后续 INSERT 同事务：
                # 先取写锁再读，并发 front 入队才不会基于过期 min/max(seq)
                # 分配出重复 seq（事务纪律详见 _get_conn docstring）。
                conn.execute('BEGIN IMMEDIATE')
                existing = {
                    row[0] for row in conn.execute('SELECT uid FROM queue')
                }
                fresh: list[dict[str, Any]] = []
                batch_seen: set[str] = set()
                for j in jobs:
                    u = uid_from_job_dict(j)
                    # 批次内重复（同一 enqueue 调用传相同 uid 两次）与
                    # 队列中已有的 uid 都跳过（首个存储，后续跳过）。
                    if u in existing or u in batch_seen:
                        continue
                    batch_seen.add(u)
                    fresh.append(j)
                if not fresh:
                    return []
                seqs = self._seq_range(conn, len(fresh), front=front)
                conn.executemany(
                    'INSERT INTO queue (uid, seq, job_data) VALUES (?, ?, ?)',
                    [(uid_from_job_dict(j), s, dumps(j))
                     for j, s in zip(fresh, seqs)],
                )
                return [uid_from_job_dict(j) for j in fresh]
        except Exception as e:
            logger.critical(f"Failed to enqueue jobs to {self.path.name}: {e}")
            raise

    # 定向删除与种子 -------------------------------------------------------

    def delete_queue_uids(self, uids: list[str]) -> int:
        """按 uid 定向批量删除队列行（加载期 repair 差量落盘），不触碰其余行。"""
        if not uids:
            return 0
        try:
            with self._get_conn() as conn:
                cur = conn.executemany(
                    'DELETE FROM queue WHERE uid = ?', [(u,) for u in uids]
                )
                return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            logger.critical(f"Failed to delete queue uids in {self.path.name}: {e}")
            raise

    def delete_failed(self, uids: list[str]) -> int:
        """从失败档案批量删除指定 uid（clear_failures 的后端实现）。"""
        if not uids:
            return 0
        try:
            with self._get_conn() as conn:
                cur = conn.executemany(
                    'DELETE FROM failed WHERE uid = ?', [(u,) for u in uids]
                )
                return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            logger.critical(f"Failed to delete failed entries in {self.path.name}: {e}")
            raise

    def delete_wall(self, uids: list[str]) -> int:
        """从 wall 批量删除指定 uid（clear_history 的后端实现）。"""
        if not uids:
            return 0
        try:
            with self._get_conn() as conn:
                cur = conn.executemany(
                    'DELETE FROM wall WHERE uid = ?', [(u,) for u in uids]
                )
                return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            logger.critical(f"Failed to delete wall entries in {self.path.name}: {e}")
            raise

    def seed_wall(self, uids: list[str]) -> int:
        """把 uid 批量写入 wall（meta 空 dict）——存档迁移标记「已处理」。

        幂等：已存在的 uid 被覆盖（meta 重置为空）。
        不变式：wall/failed 全局互斥——已在 failed 的 uid 拒绝种子；
        冲突检查与写入收敛进同一 ``BEGIN IMMEDIATE`` 写事务（先取写锁
        再读，详见 _get_conn），冲突时零写入（整体拒绝），不静默清除
        失败档案记录（失败历史须先经 delete_failed 显式清除）。
        """
        if not uids:
            return 0
        unique_uids = list(dict.fromkeys(uids))
        try:
            with self._get_conn() as conn:
                conn.execute('BEGIN IMMEDIATE')
                placeholders = ','.join('?' * len(unique_uids))
                conflict = sorted(
                    row[0] for row in conn.execute(
                        f'SELECT uid FROM failed WHERE uid IN ({placeholders})',
                        unique_uids,
                    )
                )
                if conflict:
                    raise ValueError(
                        f"seed_wall refuses uid(s) already in failed: {conflict}; "
                        f"wall/failed must stay disjoint"
                    )
                cur = conn.executemany(
                    'INSERT OR REPLACE INTO wall (uid, payload) VALUES (?, ?)',
                    [(u, dumps({})) for u in uids],
                )
                return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            logger.critical(f"Failed to seed wall in {self.path.name}: {e}")
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
                    'INSERT OR REPLACE INTO cursors (k, v) VALUES (?, ?)',
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
                    'incarnation, run_id, started_at, finished_at, outcome, error) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (
                        record.job_uid, record.activation_no, record.attempt_no,
                        record.incarnation, record.run_id, record.started_at,
                        record.finished_at, record.outcome, record.error,
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
        error: str | None = None,
    ) -> bool:
        """收尾轨迹行：读-判-写在 ``BEGIN IMMEDIATE`` 写事务内串行完成。

        目标 id 不存在返回 False（旁路观测面缺行不触发崩溃契约）；对已
        收尾行再次收尾抛 ValueError（双重终结是调用方协议缺陷，
        fail-loud）。只 UPDATE 收尾三列，身份列永不改写。
        """
        validate_attempt_finish(outcome, finished_at, error)
        try:
            with self._get_conn() as conn:
                # 读 running 状态 → 判定 → UPDATE 是读-改-写：持写事务
                # 串行化，防并发双重收尾交错漏判（详见 _get_conn）。
                conn.execute('BEGIN IMMEDIATE')
                row = conn.execute(
                    'SELECT outcome FROM attempts WHERE id = ?', (attempt_id,)
                ).fetchone()
                if row is None:
                    return False
                if row[0] != "running":
                    raise ValueError(
                        f"attempt {attempt_id} already finalized with outcome "
                        f"{row[0]!r}; double finalization is a protocol violation"
                    )
                conn.execute(
                    'UPDATE attempts SET outcome = ?, finished_at = ?, error = ? '
                    'WHERE id = ?',
                    (outcome, finished_at, error, attempt_id),
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
                    'SELECT job_uid, activation_no, attempt_no, incarnation, '
                    'run_id, started_at, finished_at, outcome, error '
                    'FROM attempts WHERE job_uid = ? ORDER BY id ASC',
                    (job_uid,),
                ).fetchall()
                return [
                    AttemptRecord.from_dict(dict(zip(_ATTEMPT_COLUMNS, row)))
                    for row in rows
                ]
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(
                f"Failed to load attempts for {job_uid!r} from {self.path.name}: {e}"
            ) from e

    # 元数据 ---------------------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        if not self.path.exists():
            return None
        try:
            with self._get_conn() as conn:
                row = conn.execute('SELECT v FROM meta WHERE k = ?', (key,)).fetchone()
                return row[0] if row else None
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to read meta '{key}' from {self.path.name}: {e}") from e

    def set_meta(self, key: str, value: str) -> None:
        try:
            with self._get_conn() as conn:
                conn.execute(
                    'INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)',
                    (key, value),
                )
        except Exception as e:
            logger.critical(f"Failed to write meta '{key}' in {self.path.name}: {e}")
            raise


__all__ = ["SQLiteStateBackend"]
