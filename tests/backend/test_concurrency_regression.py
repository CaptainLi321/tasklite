"""SQLite 后端并发回归测试：seq 分配与 DLQ ``_attempt`` 计数的读-改-写竞态。

传统 sqlite3 隔离模式（isolation_level 未显式配置）只为 DML 隐式开
事务，SELECT 在 autocommit 下逐条取快照——「读 min/max(seq) → 分配 →
INSERT」与「读 ``_attempt`` → 递增 → REPLACE」在两连接并发时会基于
同一份旧快照各算各的：落盘后 seq 重复（idx_queue_seq 非唯一索引拦不
住，队头保序契约被破坏）或失败计数丢失。修复后读-改-写方法在任何
SELECT 之前显式 ``BEGIN IMMEDIATE``，把读纳入写事务（先取写锁再读）。

多线程测试为概率性触发（真实竞态窗口在微秒级）；确定性交错测试则
兜底验证「两连接交错执行后 seq 唯一且严格反映交错顺序」。
"""

import sqlite3
import threading

from tasklite.backend.sqlite_backend import SQLiteStateBackend


N_THREADS = 6
N_ROUNDS = 12


def _queue_seq_stats(db_path) -> "tuple[int, int]":
    """返回 (queue 总行数, seq 去重后的行数)。"""
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT seq) FROM queue"
        ).fetchone()


class TestConcurrentSeqAllocation:
    """多线程（每线程独立 backend/连接）混合写同一 db 文件的竞态回归。"""

    @staticmethod
    def _worker(tid: int, backend: SQLiteStateBackend,
                barrier: threading.Barrier, errors: list) -> None:
        try:
            for k in range(N_ROUNDS):
                # 每轮起点同步：让各线程的读窗口高度重叠，最大化并发重叠下
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
                    errors.append(
                        f"commit_job_success returned False (t{tid} k{k})"
                    )
                # 路径三：共享 uid 的 DLQ 计数——并发读-递增-写是否丢计数
                backend.append_failed("shared::uid", {"error": "boom"})
                # 路径四：失败 commit（DLQ 单一出口的另一个并发入口）
                if not backend.commit_job_failure(f"f::{tid}-{k}", {"error": "x"}):
                    errors.append(
                        f"commit_job_failure returned False (t{tid} k{k})"
                    )
        except Exception as e:
            errors.append(f"thread {tid} failed: {e!r}")

    def test_concurrent_mixed_writes_seq_unique_and_attempt_exact(self, tmp_path):
        """并发混合写：seq 全唯一、queue 行数精确、_attempt 不丢计数。"""
        db_path = tmp_path / "concurrent.db"
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

        # seq 全局唯一：重复 seq 直接破坏队头保序契约
        total, distinct = _queue_seq_stats(db_path)
        assert total == distinct, (
            f"seq 重复：queue 共 {total} 行，仅 {distinct} 个唯一 seq"
        )
        # 行数精确：初始 1 + 每线程每轮 enqueue 1 行 + spawned 1 行
        # （c::/f:: uid 从不在 queue 中，其 DELETE 是 no-op）
        assert total == 1 + 2 * N_THREADS * N_ROUNDS

        # _attempt 精确计数：共享 uid 每线程每轮被写一次 DLQ
        failed = backends[0].load_failed()
        assert failed["shared::uid"]["_attempt"] == N_THREADS * N_ROUNDS, (
            "并发 DLQ 写丢失计数：_attempt 应精确等于写入次数"
        )
        for t in range(N_THREADS):
            for k in range(N_ROUNDS):
                assert failed[f"f::{t}-{k}"]["_attempt"] == 1
        assert len(failed) == N_THREADS * N_ROUNDS + 1

        # wall 全部落盘（每个 c:: uid 一次成功 commit）
        wall = backends[0].load_wall()
        assert len(wall) == N_THREADS * N_ROUNDS


class TestInterleavedTwoConnections:
    """两连接交错执行的确定性护栏：不依赖时序，验证交错后的最终不变式。"""

    def test_two_connections_interleaved_front_enqueue(self, tmp_path):
        """两连接交错队头入队：seq 全唯一，且升序恰为插入逆序（后插者 seq 更小）。"""
        db_path = tmp_path / "interleave.db"
        backend_a = SQLiteStateBackend(db_path)
        backend_b = SQLiteStateBackend(db_path)

        insert_order = []
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
        # 队头保序契约：后插入的 seq 更小，seq 升序 = 插入逆序
        assert [r[0] for r in rows] == list(reversed(insert_order))
        total, distinct = _queue_seq_stats(db_path)
        assert total == distinct == 60

    def test_two_connections_interleaved_append_failed_counts_exact(self, tmp_path):
        """两连接交错对同一 uid 写 DLQ：_attempt 精确等于写入次数。"""
        db_path = tmp_path / "interleave_dlq.db"
        backend_a = SQLiteStateBackend(db_path)
        backend_b = SQLiteStateBackend(db_path)

        for i in range(20):
            backend_a.append_failed("t::u", {"round": i})
            backend_b.append_failed("t::u", {"round": i})

        failed = backend_a.load_failed()
        assert failed["t::u"]["_attempt"] == 40, (
            "交错 DLQ 写丢失计数：_attempt 应精确等于写入次数"
        )
