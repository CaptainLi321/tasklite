from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Union

from .base import AbstractStateBackend, classify_error_type
from ..models.state import uid_from_job_dict
from ..utils.jsonutil import dumps, loads

logger = logging.getLogger("tasklite")




_SCHEMA_VERSION = 1


class SQLiteStateBackend(AbstractStateBackend):
    """Atomic SQLite state backend for tasklite.
    Combines queue, wall_log, and failed_log into a single ACID-compliant database."""

    def __init__(self, filepath: Union[str, Path]):
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

        ``synchronous=FULL`` 是 per-connection 设置，每次新建连接都需设置。
        ``journal_mode=WAL`` 是 database-level，仅在 ``_init_db`` 中设置一次。

        持久化保障：NORMAL → FULL。WAL+NORMAL 下 commit 不 fsync WAL，
        断电可回滚最后若干事务——执行中 job 的回滚可被 at-least-once 重跑
        吸收，但 **enqueue 应答后断电**时 INSERT 事务消失且无 job 可重跑
        （任务静默蒸发），文档「丢失的事务对应 job 重跑」对非队列事务不
        成立。FULL 保证已应答事务落盘；性能由 tests/perf/test_persistence_perf.py
        预算护栏验证（WAL 下 FULL 每次仅多一次 WAL fsync）。

        读-改-写事务纪律：``sqlite3`` 传统隔离模式（isolation_level 未显式
        配置）只为 DML 隐式开事务，SELECT 在 autocommit 下逐条取快照——
        「读 min/max(seq) → 分配 → INSERT」或「读 ``_attempt`` → 递增 →
        REPLACE」若不显式开事务，两个连接会基于同一份旧快照各算各的，
        落盘后出现重复 seq（idx_queue_seq 非唯一，拦不住）或丢计数。此类
        方法必须在任何 SELECT 之前显式 ``BEGIN IMMEDIATE``：先取写锁再读，
        读与写在同一事务内；并发方要么先于本事务提交（本事务读到其结果），
        要么排队等锁（本事务提交后其读到本事务的结果）。锁等待由
        connect(timeout=30.0) 的 busy timeout 承担——并发方等待写锁而非
        立即报 database is locked。
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

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            # WAL 返回值必须验证——rollback journal
            # （无 WAL）即使配 synchronous=FULL 也是 SQLite 文档标注的「断电
            # 可损坏/性能陷阱」配置。当 WAL 无法生效时（NFS 无锁/只读介质/
            # 被其他连接持有），PRAGMA 静默返回当前模式而不报错，引擎会以
            # 「断电安全」招牌运行在可损坏配置下。
            # fail-loud：宁可拒绝启动，不可带病运行（README 将 WAL 列为卖点）。
            wal_row = conn.execute('PRAGMA journal_mode=WAL').fetchone()
            if not wal_row or str(wal_row[0]).lower() != 'wal':
                raise RuntimeError(
                    f"journal_mode=WAL could not be engaged on {self.path.name} "
                    f"(got {wal_row[0] if wal_row else None!r}). Refusing to run in "
                    f"a power-loss-corruptible configuration; check filesystem "
                    f"locking support (NFS) and that no other connection holds the DB."
                )
            user_ver = conn.execute("PRAGMA user_version").fetchone()[0]
            if user_ver > _SCHEMA_VERSION:
                raise RuntimeError(
                    f"Unsupported database schema version {user_ver} on {self.path.name}; "
                    f"this runtime only supports up to version {_SCHEMA_VERSION}."
                )
            conn.execute('''
                CREATE TABLE IF NOT EXISTS wall (
                    uid TEXT PRIMARY KEY,
                    payload TEXT
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS failed_dlq (
                    uid TEXT PRIMARY KEY,
                    payload TEXT
                )
            ''')
            self._ensure_queue_schema(conn)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS cursors (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            # fencing：框架级元数据表（last_run_id 等）
            conn.execute('''
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            ''')
            if user_ver < _SCHEMA_VERSION:
                conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")

    def get_meta(self, key: str) -> Optional[str]:
        if not self.path.exists():
            return None
        try:
            with self._get_conn() as conn:
                row = conn.execute('SELECT value FROM meta WHERE key = ?', (key,)).fetchone()
                return row[0] if row else None
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to read meta '{key}' from {self.path.name}: {e}") from e

    def set_meta(self, key: str, value: str) -> None:
        try:
            with self._get_conn() as conn:
                conn.execute(
                    'INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)',
                    (key, value),
                )
        except Exception as e:
            logger.critical(f"Failed to write meta '{key}' in {self.path.name}: {e}")
            raise

    def _ensure_queue_schema(self, conn) -> None:
        """确保 queue 表使用当前 schema（uid PK + seq 保序列）。

        只接受新 schema；旧 schema（``idx PK + job_data``）不再自动迁移，
        发现旧库直接 fail-loud，提示重建 state 库。
        """
        cols = conn.execute("PRAGMA table_info(queue)").fetchall()
        if not cols:
            conn.execute('''
                CREATE TABLE queue (
                    uid TEXT PRIMARY KEY,
                    seq INTEGER NOT NULL,
                    job_data TEXT NOT NULL
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_queue_seq ON queue(seq)')
            return
        col_names = {c[1] for c in cols}
        if {'uid', 'seq', 'job_data'} <= col_names:
            conn.execute('CREATE INDEX IF NOT EXISTS idx_queue_seq ON queue(seq)')
            return
        raise RuntimeError(
            "Unsupported legacy queue schema; automatic migration has been "
            "removed. Recreate the state database or migrate manually."
        )

    def _seq_range(self, conn, count: int, front: bool) -> List[int]:
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

    def load_wall(self) -> Dict[str, Dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT uid, payload FROM wall')
                return {row[0]: loads(row[1]) for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load wall from {self.path.name}: {e}") from e
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Corrupted wall payload in {self.path.name}: {e}") from e

    def append_failed(self, uid: str, payload: Optional[Dict[str, Any]] = None) -> None:
        try:
            with self._get_conn() as conn:
                # _attempt 读-递增-写必须持写事务串行化，否则并发 append
                # 基于同一旧计数各写各的，丢失败历史（详见 _get_conn）。
                conn.execute('BEGIN IMMEDIATE')
                # 经 _write_dlq_row 单一出口：保留既有 _attempt 计数，
                # 与 commit_* 路径的失败历史口径一致。
                self._write_dlq_row(conn, uid, payload or {})
        except Exception as e:
            logger.error(f"Failed to append to DLQ in {self.path.name}: {e}")
            raise

    def load_failed(self) -> Dict[str, Dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT uid, payload FROM failed_dlq')
                return {row[0]: loads(row[1]) for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load failed DLQ from {self.path.name}: {e}") from e
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Corrupted failed DLQ payload in {self.path.name}: {e}") from e

    def load_queue(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT job_data FROM queue ORDER BY seq ASC')
                return [loads(row[0]) for row in cursor]
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load queue from {self.path.name}: {e}") from e
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Corrupted queue payload in {self.path.name}: {e}") from e

    def save_queue(self, jobs: List[Dict[str, Any]]) -> None:
        """全量重写队列（bootstrap / 崩溃恢复用，罕见 O(N)）。

        保存兜底去重：传入重复 uid 时保留首条 + 告警（而非 REPLACE 静默
        覆盖为最后一条）——与加载期去重策略一致，杜绝 `_queue_uids` set 与
        queue list 的漂移。
        """
        try:
            with self._get_conn() as conn:
                conn.execute('DELETE FROM queue')
                if jobs:
                    seen: set = set()
                    rows = []
                    for i, j in enumerate(jobs):
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
        except Exception as e:
            logger.critical(f"Failed to save queue to {self.path.name}: {e}")
            raise

    def enqueue_jobs(self, jobs: List[Dict[str, Any]], *, front: bool = False) -> List[str]:
        """批量增量入队：单事务原子插入，跳过重复 uid。

        与 save_queue 的区别：不做 DELETE 全表重写——与 run() 的 delta
        commit 并发时不覆盖其已提交变更（写入侧不覆盖，读-改-写窗口
        仍在）。返回实际插入的 uid 列表。
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
                fresh: List[Dict[str, Any]] = []
                batch_seen: set = set()
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

    def load_cursors(self) -> Dict[str, str]:
        if not self.path.exists():
            return {}
        try:
            with self._get_conn() as conn:
                cursor = conn.execute('SELECT key, value FROM cursors')
                return {row[0]: row[1] for row in cursor}
        except (sqlite3.Error, OSError) as e:
            raise RuntimeError(f"Failed to load cursors from {self.path.name}: {e}") from e

    def commit_job_success(self, uid: str, result_meta: dict, *, spawned_jobs=(), cursor_updates: Optional[Dict[str, str]] = None) -> bool:
        """原子 delta：写 wall + 删除 popped uid + 队头插入 spawned_jobs + 更新 cursors。

        失败时事务回滚，on-disk 队列不变（popped uid 仍在磁盘）。
        spawned_jobs 用 INSERT OR IGNORE 防御重复 uid，不重排既有条目。
        """
        try:
            with self._get_conn() as conn:
                # spawned 队头插入的 seq 分配是读-改-写：先取写锁再读，
                # 与并发的 front 入队互斥，否则两方读到同一 min_seq。
                conn.execute('BEGIN IMMEDIATE')
                conn.execute('INSERT OR REPLACE INTO wall (uid, payload) VALUES (?, ?)',
                             (uid, dumps(result_meta or {})))
                conn.execute('DELETE FROM queue WHERE uid = ?', (uid,))
                # 成功 commit 时清理 failed_dlq 同名残行——
                # 否则同一 uid 永久「既成功又失败」（DLQ 失败历史与 wall 矛盾，
                # _attempt 计数被陈旧行污染）。幂等，与「job 最终状态唯一」语义一致。
                conn.execute('DELETE FROM failed_dlq WHERE uid = ?', (uid,))
                if spawned_jobs:
                    seqs = self._seq_range(conn, len(spawned_jobs), front=True)
                    cur = conn.executemany(
                        'INSERT OR IGNORE INTO queue (uid, seq, job_data) VALUES (?, ?, ?)',
                        [(uid_from_job_dict(j), s, dumps(j))
                         for j, s in zip(spawned_jobs, seqs)],
                    )
                    # 防止 INSERT OR IGNORE 静默丢弃 spawn：若某个
                    # spawned uid 与磁盘队列冲突（内存 is_known 预筛后的漂移
                    # 窗口），该 job 只存在于内存、磁盘从未持久化，进程崩溃
                    # 后静默丢失（at-least-once 违约）。executemany 的 rowcount
                    # 返回实际插入行数（全冲突=0/部分冲突=实际数），
                    # rowcount < len 即检测到漂移 → 返回 False 走崩溃契约
                    # （重启后 at-least-once 重扫），而非静默掩盖。
                    if cur.rowcount is not None and cur.rowcount < len(spawned_jobs):
                        logger.critical(
                            f"commit_job_success for {uid}: {len(spawned_jobs) - cur.rowcount} "
                            f"spawned job(s) silently dropped by INSERT OR IGNORE "
                            f"(disk/memory drift). Returning False to trigger crash contract."
                        )
                        raise RuntimeError(
                            f"spawned job uid conflict on disk for {uid}: "
                            f"inserted {cur.rowcount}/{len(spawned_jobs)}"
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
            logger.critical(f"Failed to commit job success for {uid} in {self.path.name}: {e}")
            return False
        return True

    def _write_dlq_row(self, conn, uid: str, meta: dict) -> None:
        """DLQ 行写入的单一出口。

        读既有 ``_attempt`` 计数 → 递增或初始化 → INSERT OR REPLACE。
        ``commit_job_failure`` / ``commit_bulk_failure`` / ``append_failed``
        三处共用单一出口，统一保留 ``_attempt`` 计数——同一 uid 的
        失败次数不取决于最先 DLQ 它的路径（README「失败历史可观测」契约）。

        写入计数语义：``_attempt`` 是**写入事件计数**而非
        逻辑失败次数——同一逻辑失败可被多条路径写入（如级联 bulk + 单条
        failure）各计一次。这是有意简化：跨路径去重需要调用方标记同一失败
        批次，收益低于复杂度。读者应将 ``_attempt`` 解读为「该 uid 被
        写入 DLQ 的次数」（可观测性），而非精确的失败次数。

        统一补结构化字段：``error_type``（``classify_error_type``
        推导）与 ``failed_at``（UTC ISO 时间戳）。所有 DLQ 写入路径（含死锁/级联/
        重试耗尽）都带分类与时间，list_dlq() 查询可直接按类型过滤。

        调用方必须已通过 ``BEGIN IMMEDIATE`` 持写事务：SELECT 旧计数 →
        递增 → REPLACE 的序列在 autocommit 下会丢并发计数（两个连接
        各基于同一旧值 +1 落盘，只留一次）。
        """
        row = conn.execute(
            'SELECT payload FROM failed_dlq WHERE uid = ?', (uid,)
        ).fetchone()
        merged = dict(meta or {})
        if "error_type" not in merged:
            merged["error_type"] = classify_error_type(merged)
        if "failed_at" not in merged:
            merged["failed_at"] = datetime.now(timezone.utc).isoformat()
        if row is not None:
            try:
                prev = loads(row[0])
                if isinstance(prev, dict) and isinstance(prev.get("_attempt"), int):
                    merged["_attempt"] = prev["_attempt"] + 1
                elif isinstance(prev, dict) and "_attempt" not in merged:
                    # 既有记录无 _attempt（旧路径写入）→ 初始化为 1
                    merged["_attempt"] = 1
            except (json.JSONDecodeError, TypeError, ValueError):
                merged["_attempt"] = 1  # 既有记录损坏：从 1 重新计数
        else:
            merged["_attempt"] = 1
        conn.execute('INSERT OR REPLACE INTO failed_dlq (uid, payload) VALUES (?, ?)',
                     (uid, dumps(merged)))

    def commit_job_failure(self, uid: str, result_meta: dict) -> bool:
        """原子 delta：写 DLQ + 删除 popped uid + 清理 wall 旧记录。失败时 on-disk 队列不变。

        同 uid 多次失败时保留失败历史——DLQ 是 INSERT OR REPLACE
        （PK 覆盖），直接覆盖会丢失「这是第几次失败」。读取既有记录的
        ``_attempt`` 计数并递增后合并写入，让同一 uid 的失败历史可观测。
        写盘经 ``_write_dlq_row`` 单一出口（_attempt 计数路径收敛）。

        同一事务内 ``DELETE FROM wall``——rerun 任务重跑
        失败时 wall 里的旧成功记录必须作废（最终状态唯一）；否则磁盘
        wall∩failed 并存 → 下次 run DEBUG 断言崩。事务内单出口、原子、
        幂等（never 任务失败时 wall 本无该行，DELETE no-op）。
        """
        try:
            with self._get_conn() as conn:
                # _attempt 读-递增-写必须持写事务串行化（详见 _get_conn）。
                conn.execute('BEGIN IMMEDIATE')
                self._write_dlq_row(conn, uid, result_meta)
                conn.execute('DELETE FROM queue WHERE uid = ?', (uid,))
                conn.execute('DELETE FROM wall WHERE uid = ?', (uid,))
        except Exception as e:
            logger.critical(f"Failed to commit job failure for {uid} in {self.path.name}: {e}")
            return False
        return True

    def commit_skip(self, uid: str) -> bool:
        """Delta 删除队列中已完成/已失败（重复）的 uid。

        去重命中时调用——磁盘队列中该 uid 是残留条目，DELETE 清理以同步
        内存/磁盘。不写 wall/failed。失败时 on-disk 队列不变。
        """
        try:
            with self._get_conn() as conn:
                conn.execute('DELETE FROM queue WHERE uid = ?', (uid,))
        except Exception as e:
            logger.critical(f"Failed to commit skip for {uid} in {self.path.name}: {e}")
            return False
        return True

    def commit_retry(self, popped_uid: str, requeued_job: Dict[str, Any], *, front: bool = False) -> bool:
        """原子 delta：删除 popped_uid + 按 front 插入 requeued_job。不写 wall/DLQ。

        popped_uid 已先删除，故同 uid 重插安全（用 INSERT 而非 REPLACE）。
        不用 INSERT OR IGNORE——若 requeued_job 的 uid 与队列中
        既有行冲突（本不该发生），静默丢弃会让 job 无声消失却返回 True；
        普通 INSERT 让真实冲突抛异常 → 返回 False → 走 crash-safe 路径。
        失败时 on-disk 队列不变（popped uid 仍在）。
        """
        try:
            with self._get_conn() as conn:
                # seq 分配（_seq_range）的读与随后的 INSERT 之间不能让并发方
                # 插入提交：先取写锁再读（事务纪律详见 _get_conn docstring）。
                conn.execute('BEGIN IMMEDIATE')
                conn.execute('DELETE FROM queue WHERE uid = ?', (popped_uid,))
                seqs = self._seq_range(conn, 1, front=front)
                conn.execute('INSERT INTO queue (uid, seq, job_data) VALUES (?, ?, ?)',
                             (uid_from_job_dict(requeued_job), seqs[0],
                              dumps(requeued_job)))
        except Exception as e:
            logger.critical(f"Failed to commit retry for {popped_uid} in {self.path.name}: {e}")
            return False
        return True

    def commit_bulk_failure(self, uids_metas: List[Tuple[str, dict]]) -> bool:
        """原子 delta：批量写 DLQ + 批量删除这些 uid + 批量清理 wall 旧记录。失败时 on-disk 队列不变。

        删除是按 uid 精准删除，剩余条目保持原 seq 顺序。
        写盘经 ``_write_dlq_row`` 单一出口（_attempt 计数路径收敛，
        全部写入路径保留失败历史）。

        与 commit_job_failure 对称，同一事务内批量
        ``DELETE FROM wall``——rerun 任务被级联/死锁批量 DLQ 时，wall 旧
        成功记录作废（最终状态唯一），避免磁盘 wall∩failed 并存。
        """
        try:
            with self._get_conn() as conn:
                # _attempt 读-递增-写必须持写事务串行化（详见 _get_conn）。
                conn.execute('BEGIN IMMEDIATE')
                for uid, meta in uids_metas:
                    self._write_dlq_row(conn, uid, meta)
                conn.executemany('DELETE FROM queue WHERE uid = ?',
                                 [(uid,) for uid, _ in uids_metas])
                conn.executemany('DELETE FROM wall WHERE uid = ?',
                                 [(uid,) for uid, _ in uids_metas])
        except Exception as e:
            logger.critical(f"Failed to commit bulk failure in {self.path.name}: {e}")
            return False
        return True

    def delete_failed(self, uids: List[str]) -> int:
        """从 DLQ 批量删除指定 uid（clear_dlq/clear_history 的后端）。"""
        if not uids:
            return 0
        try:
            with self._get_conn() as conn:
                cur = conn.executemany(
                    'DELETE FROM failed_dlq WHERE uid = ?', [(u,) for u in uids]
                )
                return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            logger.critical(f"Failed to delete failed entries in {self.path.name}: {e}")
            raise

    def delete_wall(self, uids: List[str]) -> int:
        """从 wall 批量删除指定 uid（clear_history 的后端）。"""
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

    def seed_wall(self, uids: List[str]) -> int:
        """把 uid 批量写入 wall（meta 空 dict）——存档迁移标记「已处理」。

        幂等：已存在的 uid 被覆盖（meta 重置为空）。
        """
        if not uids:
            return 0
        try:
            with self._get_conn() as conn:
                cur = conn.executemany(
                    'INSERT OR REPLACE INTO wall (uid, payload) VALUES (?, ?)',
                    [(u, dumps({})) for u in uids],
                )
                return cur.rowcount if cur.rowcount is not None else 0
        except Exception as e:
            logger.critical(f"Failed to seed wall in {self.path.name}: {e}")
            raise

    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（UPSERT，幂等）。"""
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
