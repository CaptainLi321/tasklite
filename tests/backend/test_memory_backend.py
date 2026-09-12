"""InMemoryStateBackend 契约与功能测试。"""

import json
import sqlite3

import pytest
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.models.job import Job
from tasklite.models.state import uid_from_job_dict
from tasklite.pipeline import TaskLite


def _calc_worker(job, ctx):
    return {"doubled": job.payload["x"] * 2}


class TestInMemoryStateBackendContract:
    def test_basic_crud_and_isolation(self):
        b = InMemoryStateBackend()
        # 初始状态为空
        assert b.load_wall() == {}
        assert b.load_failed() == {}
        assert b.load_cursors() == {}
        assert b.load_queue() == []

        # enqueue_jobs
        j1 = {"task_type": "t", "job_id": "1", "payload": {"a": 1}}
        j2 = {"task_type": "t", "job_id": "2", "payload": {"a": 2}}
        inserted = b.enqueue_jobs([j1, j2])
        assert inserted == ["t::1", "t::2"]
        assert len(b.load_queue()) == 2

        # 幂等去重入队
        inserted2 = b.enqueue_jobs([j1, {"task_type": "t", "job_id": "3"}])
        assert inserted2 == ["t::3"]
        assert len(b.load_queue()) == 3

    def test_commit_job_success_with_spawn_and_cursor(self):
        b = InMemoryStateBackend()
        j1 = {"task_type": "t", "job_id": "1"}
        b.enqueue_jobs([j1])

        spawn = {"task_type": "t", "job_id": "child"}
        ok = b.commit_job_success(
            "t::1",
            {"status": "ok"},
            spawned_jobs=[spawn],
            cursor_updates={"page": "2"},
        )
        assert ok is True
        wall = b.load_wall()
        assert "t::1" in wall
        assert wall["t::1"]["status"] == "ok"
        assert b.load_cursors() == {"page": "2"}
        q = b.load_queue()
        assert len(q) == 1
        assert q[0]["job_id"] == "child"

    def test_commit_job_failure_and_dlq_metadata(self):
        b = InMemoryStateBackend()
        j1 = {"task_type": "t", "job_id": "fail1"}
        b.enqueue_jobs([j1])

        ok = b.commit_job_failure("t::fail1", {"error": "NETWORK_TIMEOUT"})
        assert ok is True
        failed = b.load_failed()
        assert "t::fail1" in failed
        entry = failed["t::fail1"]
        assert entry["error"] == "NETWORK_TIMEOUT"
        assert entry["_attempt"] == 1
        assert "failed_at" in entry
        assert entry["error_type"] == "unknown"
        assert b.load_queue() == []

        # 结构化错误与 fatal 分类测试
        b.commit_job_failure("t::fatal", {"error": "Boom", "fatal": True})
        b.commit_job_failure("t::deadlock", {"error": "DEADLOCK_CLASSIFICATION_GAP"})
        b.commit_job_failure("t::dep", {"error": "JOB_DEPENDENCY"})
        b.commit_job_failure("t::retries", {"error": "MAX_RETRIES_EXCEEDED"})

        res = b.load_failed()
        assert res["t::fatal"]["error_type"] == "fatal"
        assert res["t::deadlock"]["error_type"] == "deadlock"
        assert res["t::dep"]["error_type"] == "dependency"
        assert res["t::retries"]["error_type"] == "transient_exhausted"

    def test_commit_retry(self):
        b = InMemoryStateBackend()
        j1 = {"task_type": "t", "job_id": "retry1"}
        b.enqueue_jobs([j1])

        retry_job = {"task_type": "t", "job_id": "retry1", "retries": 1}
        ok = b.commit_retry("t::retry1", retry_job, front=True)
        assert ok is True
        q = b.load_queue()
        assert len(q) == 1
        assert q[0]["retries"] == 1
        assert b.load_wall() == {}

    def test_meta_persistence(self):
        b = InMemoryStateBackend()
        assert b.get_meta("key1") is None
        b.set_meta("key1", "val1")
        assert b.get_meta("key1") == "val1"


class TestTaskLiteWithMemoryBackend:
    def test_tasklite_runs_with_memory_backend(self, tmp_path):
        p = TaskLite("mem_test", state_dir=tmp_path, backend="memory")
        assert p.backend_type == "memory"

        p.register_handler("calc", _calc_worker)
        p.enqueue(Job("calc", "1", payload={"x": 10}))
        p.enqueue(Job("calc", "2", payload={"x": 20}))

        p.run()
        assert p.stats.completed == 2
        wall = p.backend.load_wall()
        assert "calc::1" in wall
        assert wall["calc::1"]["doubled"] == 20
        assert "calc::2" in wall
        assert wall["calc::2"]["doubled"] == 40


class _UncommittableMeta:
    """令 DLQ 行计算必然失败的 meta 载荷：memory 侧 deepcopy 拒绝，SQLite 侧 JSON 序列化拒绝。"""

    def __deepcopy__(self, memo):
        raise ValueError("uncommittable meta")


def _atomic_snapshot(backend):
    """与实现无关的后端状态快照（剔除 failed_at 等非确定字段），用于原子性比对。"""
    return {
        "queue": [uid_from_job_dict(j) for j in backend.load_queue()],
        "wall": set(backend.load_wall()),
        "failed_attempts": {u: m.get("_attempt") for u, m in backend.load_failed().items()},
        "cursors": backend.load_cursors(),
    }


def _seed_failed_raw(backend, uid: str, payload: dict) -> None:
    """绕过 DLQ 归一化直接种入原始记录（模拟外部脏数据），两后端等效。"""
    if isinstance(backend, InMemoryStateBackend):
        backend._failed[uid] = dict(payload)
        return
    conn = sqlite3.connect(backend.path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO failed_dlq (uid, payload) VALUES (?, ?)",
            (uid, json.dumps(payload)),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(params=["memory", "sqlite"])
def dual_backend(request, tmp_path):
    """同一断言集作用于 memory 与 sqlite 两后端，即构成行为对齐断言。"""
    if request.param == "memory":
        return InMemoryStateBackend()
    return SQLiteStateBackend(tmp_path / "atomic_state.db")


class TestBackendCommitAtomicity:
    """commit_* 失败/冲突不变式：返回 False ⇒ 后端状态与调用前完全一致。

    3-strike 崩溃契约以「commit 返回 False ⇒ 后端未变」为前提做重启重建，
    后端任何先部分落盘再报失败的路径都会让 job 及其 spawned 任务静默消失。
    """

    def test_spawn_conflict_returns_false_keeping_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([
            {"task_type": "t", "job_id": "1"},
            {"task_type": "t", "job_id": "child"},
        ])
        b.append_failed("t::1", {"error": "stale"})
        before = _atomic_snapshot(b)

        ok = b.commit_job_success(
            "t::1",
            {"status": "ok"},
            spawned_jobs=[{"task_type": "t", "job_id": "child"}],
            cursor_updates={"cur": "9"},
        )

        assert ok is False
        # popped uid 仍在队列、wall/failed/cursors 均未被触碰
        assert _atomic_snapshot(b) == before
        assert "t::1" in [uid_from_job_dict(j) for j in b.load_queue()]

        # 冲突被拒后后端仍可正常提交同一 job（spawned 改为不冲突的新 uid）
        ok = b.commit_job_success(
            "t::1",
            {"status": "ok"},
            spawned_jobs=[{"task_type": "t", "job_id": "fresh"}],
        )
        assert ok is True
        # spawned 队首插入，预置的 t::child 条目保持原位
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::fresh", "t::child"]
        assert "t::1" in b.load_wall()

    def test_spawn_batch_duplicate_uid_rejected_without_dup_entries(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([{"task_type": "t", "job_id": "1"}])
        before = _atomic_snapshot(b)

        ok = b.commit_job_success(
            "t::1",
            {},
            spawned_jobs=[
                {"task_type": "t", "job_id": "x"},
                {"task_type": "t", "job_id": "x"},
            ],
        )

        assert ok is False
        after = _atomic_snapshot(b)
        assert after == before
        # 绝不产出重复队列条目却报成功
        uids = [uid_from_job_dict(j) for j in b.load_queue()]
        assert len(uids) == len(set(uids))

    def test_success_commit_with_spawn_and_cursor_still_applies(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([{"task_type": "t", "job_id": "1"}])
        b.append_failed("t::1", {"error": "stale"})

        ok = b.commit_job_success(
            "t::1",
            {"status": "ok"},
            spawned_jobs=[
                {"task_type": "t", "job_id": "c1"},
                {"task_type": "t", "job_id": "c2"},
            ],
            cursor_updates={"page": "2"},
        )

        assert ok is True
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::c1", "t::c2"]
        wall = b.load_wall()
        assert wall["t::1"]["status"] == "ok"
        # 成功 commit 清理 failed 同名残行
        assert "t::1" not in b.load_failed()
        assert b.load_cursors() == {"page": "2"}

    def test_retry_uid_conflict_returns_false_keeping_queue_intact(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([
            {"task_type": "t", "job_id": "A"},
            {"task_type": "t", "job_id": "B"},
        ])
        before = _atomic_snapshot(b)

        ok = b.commit_retry(
            "t::A",
            {"task_type": "t", "job_id": "B", "retries": 1},
            front=True,
        )

        assert ok is False
        assert _atomic_snapshot(b) == before
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::A", "t::B"]

    def test_retry_same_uid_requeue_still_succeeds(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([
            {"task_type": "t", "job_id": "A"},
            {"task_type": "t", "job_id": "B"},
        ])

        assert b.commit_retry("t::A", {"task_type": "t", "job_id": "A", "retries": 1}, front=True) is True
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::A", "t::B"]
        assert b.commit_retry("t::A", {"task_type": "t", "job_id": "A", "retries": 2}, front=False) is True
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::B", "t::A"]

    def test_bulk_failure_write_error_leaves_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([
            {"task_type": "t", "job_id": "1"},
            {"task_type": "t", "job_id": "2"},
        ])
        b.seed_wall(["t::9"])
        before = _atomic_snapshot(b)

        ok = b.commit_bulk_failure([
            ("t::1", {"error": "d1"}),
            ("t::2", {"payload": _UncommittableMeta()}),
        ])

        assert ok is False
        # 队列绝不被整体删除、wall 绝不被清理
        assert _atomic_snapshot(b) == before

    def test_job_failure_write_error_leaves_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([{"task_type": "t", "job_id": "1"}])
        before = _atomic_snapshot(b)

        ok = b.commit_job_failure("t::1", {"payload": _UncommittableMeta()})

        assert ok is False
        assert _atomic_snapshot(b) == before
        assert "t::1" in [uid_from_job_dict(j) for j in b.load_queue()]

    def test_bulk_failure_with_dirty_attempt_resets_count_and_succeeds(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([
            {"task_type": "t", "job_id": "1"},
            {"task_type": "t", "job_id": "2"},
        ])
        _seed_failed_raw(b, "t::2", {"_attempt": "dirty", "error": "legacy"})

        ok = b.commit_bulk_failure([
            ("t::1", {"error": "d1"}),
            ("t::2", {"error": "d2"}),
        ])

        # 脏计数静默重置并照常提交，绝不报失败留下半成品状态
        assert ok is True
        assert [uid_from_job_dict(j) for j in b.load_queue()] == []
        assert b.load_failed()["t::2"]["_attempt"] == 1
        assert "t::2" in b.load_failed()

    def test_job_failure_with_dirty_attempt_resets_count_and_succeeds(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([{"task_type": "t", "job_id": "1"}])
        _seed_failed_raw(b, "t::1", {"_attempt": "dirty", "error": "legacy"})

        ok = b.commit_job_failure("t::1", {"error": "boom"})

        assert ok is True
        assert b.load_failed()["t::1"]["_attempt"] == 1
        assert b.load_queue() == []

    def test_existing_int_attempt_overrides_incoming_meta_attempt(self, dual_backend):
        b = dual_backend
        b.append_failed("t::1", {"error": "first"})

        ok = b.commit_job_failure("t::1", {"error": "second", "_attempt": 99})

        assert ok is True
        # 既有计数权威：incoming 显式 _attempt 被既有计数 +1 覆盖
        assert b.load_failed()["t::1"]["_attempt"] == 2

    def test_incoming_attempt_preserved_when_existing_row_lacks_count(self, dual_backend):
        b = dual_backend
        _seed_failed_raw(b, "t::1", {"error": "legacy"})

        ok = b.commit_job_failure("t::1", {"error": "again", "_attempt": 7})

        assert ok is True
        assert b.load_failed()["t::1"]["_attempt"] == 7


class TestReplaceQueueAtomic:
    """读-改-写收敛的整表替换原语契约（双腿对齐）。

    不变式：compute 在写锁内接收磁盘真相快照；compute 抛异常 ⇒ 队列与
    调用前完全一致（对齐 SQLite 事务回滚）；替换以 compute 返回值为准。
    """

    def test_replace_result_wins_and_absent_rows_are_removed(self, dual_backend):
        b = dual_backend
        b.save_queue([
            {"task_type": "t", "job_id": "k1"},
            {"task_type": "t", "job_id": "gone"},
        ])

        def compute(disk_q):
            assert [uid_from_job_dict(j) for j in disk_q] == ["t::k1", "t::gone"]
            updated = dict(disk_q[0], payload={"v": 2})
            fresh = {"task_type": "t", "job_id": "n1"}
            return [updated, fresh]

        b.replace_queue_atomic(compute)

        q = b.load_queue()
        assert [uid_from_job_dict(j) for j in q] == ["t::k1", "t::n1"]
        assert q[0]["payload"] == {"v": 2}

    def test_compute_receives_rows_enqueued_before_replace(self, dual_backend):
        """替换前已提交（已应答成功）的入队必须进入磁盘真相快照，
        不得被内存态或陈旧快照替代——这是窗口期入队存活的前提。"""
        b = dual_backend
        b.save_queue([{"task_type": "t", "job_id": "k1"}])
        b.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        seen = {}

        def compute(disk_q):
            seen["uids"] = [uid_from_job_dict(j) for j in disk_q]
            return disk_q

        b.replace_queue_atomic(compute)

        assert seen["uids"] == ["t::k1", "t::late"]
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::k1", "t::late"]

    def test_replace_rolls_back_when_compute_raises(self, dual_backend):
        b = dual_backend
        b.save_queue([
            {"task_type": "t", "job_id": "k1"},
            {"task_type": "t", "job_id": "k2"},
        ])

        def boom(disk_q):
            raise RuntimeError("merge failed")

        with pytest.raises(RuntimeError):
            b.replace_queue_atomic(boom)

        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::k1", "t::k2"]


class TestQueueReplacementShapeValidation:
    """整表替换集形状契约（双腿一致）。

    不变式：替换集非法（None / 非序列 / 元素非 dict）必须在任何写变之前
    fail-loud 抛 TypeError——SQLite 腿若在 DELETE 后才察觉，会静默清空
    整条队列且事务正常提交；Memory 腿对同一输入必须行为一致。
    """

    def test_replace_with_none_compute_result_fails_loud(self, dual_backend):
        """compute 漏写 return（返回 None）→ TypeError，队列原状。"""
        b = dual_backend
        b.save_queue([
            {"task_type": "t", "job_id": "k1"},
            {"task_type": "t", "job_id": "k2"},
        ])

        with pytest.raises(TypeError):
            b.replace_queue_atomic(lambda disk_q: None)

        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::k1", "t::k2"]

    def test_replace_with_non_sequence_fails_loud(self, dual_backend):
        """替换集为非序列（int / 单个 dict）→ TypeError，队列原状。"""
        b = dual_backend
        b.save_queue([{"task_type": "t", "job_id": "k1"}])

        with pytest.raises(TypeError):
            b.replace_queue_atomic(lambda disk_q: 42)
        with pytest.raises(TypeError):
            b.replace_queue_atomic(lambda disk_q: {"task_type": "t", "job_id": "x"})

        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::k1"]

    def test_replace_with_non_dict_item_fails_loud(self, dual_backend):
        """替换集元素非 job dict → TypeError，队列原状。"""
        b = dual_backend
        b.save_queue([{"task_type": "t", "job_id": "k1"}])

        with pytest.raises(TypeError):
            b.replace_queue_atomic(
                lambda disk_q: [{"task_type": "t", "job_id": "ok"}, "not-a-dict"]
            )

        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::k1"]

    def test_save_queue_none_fails_loud(self, dual_backend):
        """save_queue(None) → TypeError，队列原状（空列表仍合法清空）。"""
        b = dual_backend
        b.save_queue([{"task_type": "t", "job_id": "k1"}])

        with pytest.raises(TypeError):
            b.save_queue(None)
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::k1"]

        b.save_queue([])
        assert b.load_queue() == []


class TestSeedWallFailedMutualExclusion:
    """seed_wall 拒绝已存在 failed 的 uid（wall/failed 全局互斥，两后端同契约）。

    seed 链路若静默覆盖会留下 wall∩failed 重叠：下一次派发时状态一致性
    断言崩溃、配合崩溃恢复形成重启循环。语义定为整体拒绝（零写入）——
    DLQ 记录须先经 delete_failed/clear_dlq 显式清除，静默清除会丢失败历史。
    """

    def test_seed_wall_rejects_uid_already_in_failed(self, dual_backend):
        b = dual_backend
        b.append_failed("t::x", {"error": "boom"})

        with pytest.raises(ValueError, match="already in failed"):
            b.seed_wall(["t::x"])

        # 零写入：拒绝后 wall 不含该 uid，failed 记录原样保留
        assert "t::x" not in b.load_wall()
        assert "t::x" in b.load_failed()

    def test_seed_wall_conflict_is_atomic_no_partial_write(self, dual_backend):
        """冲突批次整体拒绝：batch 内非冲突 uid 亦不得部分写入。"""
        b = dual_backend
        b.append_failed("t::bad", {"error": "boom"})

        with pytest.raises(ValueError, match="already in failed"):
            b.seed_wall(["t::ok1", "t::bad", "t::ok2"])

        assert set(b.load_wall()) == set()
        assert "t::bad" in b.load_failed()

    def test_seed_wall_fresh_uid_unaffected(self, dual_backend):
        """正常 seed（无 failed 冲突）行为不变：幂等覆盖写入。"""
        b = dual_backend
        assert b.seed_wall(["t::fresh1", "t::fresh2"]) == 2
        assert set(b.load_wall()) == {"t::fresh1", "t::fresh2"}
        # 幂等：重复 seed 仍成功（meta 重置为空）
        assert b.seed_wall(["t::fresh1"]) == 1
        assert b.load_wall()["t::fresh1"] == {}
