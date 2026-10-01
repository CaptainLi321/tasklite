"""v2 backend 双腿契约对齐测试：同一断言套件参数化跑 SQLite 与 Memory。

契约一致性属性（每条断言对两腿同构即构成对齐证明）：
- enqueue 后可见（幂等去重、front/back 保序）；
- pop 后不可见（delta 终态提交后 uid 从磁盘队列消失，队列行从不单独
  「裸弹出」——弹出的 uid 只在 commit 事务内删除）；
- 终态互斥（成功删 failed 同名行、失败删 wall 行，最终状态唯一）；
- attempts append-only（历史行只增不改，每行仅允许一次收尾）。

另覆盖双腿契约面：commit 失败/冲突不变式（False ⇒ 后端与调用前
完全一致）、replace_queue_atomic、替换集形状校验、seed_wall 互斥、
seed_cursor 入口校验。
"""

from __future__ import annotations

import pytest

from tasklite.v2.backend import InMemoryStateBackend, SQLiteStateBackend
from tasklite.v2.models.attempt import ATTEMPT_RUNNING, AttemptRecord
from tasklite.v2.models.job import Job
from tasklite.v2.models.state import uid_from_job_dict


def _job_dict(job_id: str, **overrides) -> dict:
    data = Job("t", job_id).to_dict()
    data.update(overrides)
    return data


def _running_attempt(job_uid: str, attempt_no: int = 1) -> AttemptRecord:
    return AttemptRecord(
        job_uid=job_uid,
        activation_no=1,
        attempt_no=attempt_no,
        incarnation=f"run_seed.{attempt_no}",
        run_id="run_seed",
        started_at="2026-05-01T00:00:00+00:00",
        outcome="running",
    )


def _queue_uids(backend) -> list[str]:
    return [uid_from_job_dict(j) for j in backend.load_queue()]


def _snapshot(backend) -> dict:
    """与实现无关的后端状态快照，用于原子性比对（False ⇒ 全集不变）。"""
    return {
        "queue": _queue_uids(backend),
        "wall": backend.load_wall(),
        "failed": backend.load_failed(),
        "failure_payloads": backend.load_failed_payloads(),
        "cursors": backend.load_cursors(),
    }


@pytest.fixture(params=["memory", "sqlite"])
def dual_backend(request, tmp_path):
    """同一断言集作用于 memory 与 sqlite 两腿，即构成行为对齐断言。"""
    if request.param == "memory":
        return InMemoryStateBackend()
    return SQLiteStateBackend(tmp_path / "parity_state.db")


class TestCoreContractProperties:
    """四条核心契约属性的双腿一致性。"""

    def test_enqueue_visible_idempotent_and_ordered(self, dual_backend):
        b = dual_backend
        assert b.enqueue_jobs([_job_dict("one"), _job_dict("two")]) == ["t::one", "t::two"]
        # enqueue 后可见：保序落盘
        assert _queue_uids(b) == ["t::one", "t::two"]
        # 幂等去重：队列已有与批次内重复都跳过
        assert b.enqueue_jobs([_job_dict("one"), _job_dict("three"), _job_dict("three")]) == ["t::three"]
        assert _queue_uids(b) == ["t::one", "t::two", "t::three"]
        # front 入队插队首、保序
        assert b.enqueue_jobs([_job_dict("frontal")], front=True) == ["t::frontal"]
        assert _queue_uids(b) == ["t::frontal", "t::one", "t::two", "t::three"]

    def test_popped_uid_invisible_after_terminal_commit(self, dual_backend):
        """终态 delta 提交后 uid 从队列消失（成功/失败/批量/跳过四路径）。"""
        b = dual_backend
        b.enqueue_jobs([_job_dict(j) for j in ("success_one", "fail_one", "bulk_one", "skip_one")])

        assert b.commit_job_success("t::success_one", {"ok": True}) is True
        assert b.commit_job_failure("t::fail_one", {"error": "x"}) is True
        assert b.commit_bulk_failure([("t::bulk_one", {"error": "y"})]) is True
        assert b.commit_skip("t::skip_one") is True

        assert _queue_uids(b) == []
        assert set(b.load_wall()) == {"t::success_one"}
        assert set(b.load_failed()) == {"t::fail_one", "t::bulk_one"}

    def test_popped_uid_requeued_visible_via_commit_retry(self, dual_backend):
        """重试 delta 是同 uid DELETE+INSERT：重入队后以新行可见。"""
        b = dual_backend
        b.enqueue_jobs([_job_dict("one")])
        requeued = _job_dict("one", attempt_no=2)
        assert b.commit_retry("t::one", requeued) is True
        queue = b.load_queue()
        assert _queue_uids(b) == ["t::one"]
        assert queue[0]["attempt_no"] == 2

    def test_success_failure_mutually_delete_opposite_side(self, dual_backend):
        """终态互斥：成功删 failed 同名行、失败删 wall 行（最终状态唯一）。"""
        b = dual_backend
        b.commit_job_failure("t::one", {"error": "stale"}, job_payload={"k": 1})
        b.commit_job_success("t::other", {"ok": True})

        assert b.commit_job_success("t::one", {"ok": True}) is True
        assert b.load_failed() == {}
        assert b.load_failed_payloads() == {}

        assert b.commit_job_failure("t::other", {"error": "boom"}) is True
        assert b.load_wall() == {"t::one": {"ok": True}}
        assert not (set(b.load_wall()) & set(b.load_failed()))

    def test_attempts_append_only_never_rewrites_history(self, dual_backend):
        """attempts 只增不改：历史行字段稳定、每行仅一次收尾。"""
        b = dual_backend
        first = b.append_attempt(_running_attempt("t::one"))
        second = b.append_attempt(_running_attempt("t::one", attempt_no=2))
        assert first < second

        assert b.update_attempt(
            first, outcome="failed",
            finished_at="2026-05-01T00:00:05+00:00", error="boom",
        ) is True

        third = b.append_attempt(_running_attempt("t::other"))
        assert third > second

        records = b.load_attempts("t::one")
        assert [r.attempt_no for r in records] == [1, 2]
        assert records[0].outcome == "failed"
        assert records[0].incarnation == "run_seed.1"
        assert records[1].outcome == ATTEMPT_RUNNING

        # 已收尾行不可二次收尾；未知 id 返回 False（双腿同构）
        with pytest.raises(ValueError, match="already finalized"):
            b.update_attempt(
                first, outcome="succeeded", finished_at="2026-05-01T00:00:09+00:00"
            )
        assert b.update_attempt(
            999999, outcome="skipped", finished_at="2026-05-01T00:00:09+00:00"
        ) is False

    def test_meta_roundtrip_upsert(self, dual_backend):
        b = dual_backend
        assert b.get_meta("fencing") is None
        b.set_meta("fencing", "run_seed")
        b.set_meta("fencing", "run_next")
        assert b.get_meta("fencing") == "run_next"


class _UncommittableMeta:
    """令失败行落库必然失败的 meta 载荷：memory 侧 deepcopy 拒绝，
    SQLite 侧 JSON 序列化拒绝。"""

    def __deepcopy__(self, memo):
        raise ValueError("uncommittable meta")


class TestBackendCommitAtomicity:
    """commit_* 失败/冲突不变式：返回 False ⇒ 后端状态与调用前完全一致。

    连续失败崩溃契约以「commit 返回 False ⇒ 后端未变」为前提做重启
    重建，后端任何先部分落盘再报失败的路径都会让 job 及其 spawned
    任务静默消失。
    """

    def test_spawn_conflict_returns_false_keeping_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_dict("one"), _job_dict("child")])
        b.append_failed("t::one", {"error": "stale"})
        before = _snapshot(b)

        ok = b.commit_job_success(
            "t::one",
            {"status": "ok"},
            spawned_jobs=[_job_dict("child")],
            cursor_updates={"cur": "nine"},
        )

        assert ok is False
        # popped uid 仍在队列、wall/failed/cursors 均未被触碰
        assert _snapshot(b) == before
        assert "t::one" in _queue_uids(b)

        # 冲突被拒后后端仍可正常提交同一 job（spawned 改为不冲突的新 uid）
        ok = b.commit_job_success(
            "t::one", {"status": "ok"}, spawned_jobs=[_job_dict("fresh")]
        )
        assert ok is True
        # spawned 队首插入，预置的 t::child 条目保持原位
        assert _queue_uids(b) == ["t::fresh", "t::child"]
        assert "t::one" in b.load_wall()

    def test_spawn_batch_duplicate_uid_rejected_without_dup_entries(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_dict("one")])
        before = _snapshot(b)

        ok = b.commit_job_success(
            "t::one",
            {},
            spawned_jobs=[_job_dict("x_marks"), _job_dict("x_marks")],
        )

        assert ok is False
        assert _snapshot(b) == before
        # 绝不产出重复队列条目却报成功
        uids = _queue_uids(b)
        assert len(uids) == len(set(uids))

    def test_retry_uid_conflict_returns_false_keeping_queue_intact(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_dict("alpha"), _job_dict("beta")])
        before = _snapshot(b)

        ok = b.commit_retry("t::alpha", _job_dict("beta", attempt_no=2), front=True)

        assert ok is False
        assert _snapshot(b) == before
        assert _queue_uids(b) == ["t::alpha", "t::beta"]

    def test_retry_same_uid_requeue_still_succeeds(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_dict("alpha"), _job_dict("beta")])

        assert b.commit_retry("t::alpha", _job_dict("alpha", attempt_no=2), front=True) is True
        assert _queue_uids(b) == ["t::alpha", "t::beta"]
        assert b.commit_retry("t::alpha", _job_dict("alpha", attempt_no=3), front=False) is True
        assert _queue_uids(b) == ["t::beta", "t::alpha"]

    def test_bulk_failure_write_error_leaves_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_dict("one"), _job_dict("two")])
        b.seed_wall(["t::nine"])
        before = _snapshot(b)

        ok = b.commit_bulk_failure([
            ("t::one", {"error": "d_one"}),
            ("t::two", {"payload": _UncommittableMeta()}),
        ])

        assert ok is False
        # 队列绝不被整体删除、wall 绝不被清理
        assert _snapshot(b) == before

    def test_job_failure_write_error_leaves_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_dict("one")])
        before = _snapshot(b)

        ok = b.commit_job_failure("t::one", {"payload": _UncommittableMeta()})

        assert ok is False
        assert _snapshot(b) == before
        assert "t::one" in _queue_uids(b)

    def test_success_commit_with_spawn_and_cursor_still_applies(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_dict("one")])
        b.append_failed("t::one", {"error": "stale"})

        ok = b.commit_job_success(
            "t::one",
            {"status": "ok"},
            spawned_jobs=[_job_dict("child_one"), _job_dict("child_two")],
            cursor_updates={"page": "two"},
        )

        assert ok is True
        assert _queue_uids(b) == ["t::child_one", "t::child_two"]
        assert b.load_wall()["t::one"]["status"] == "ok"
        # 成功 commit 清理 failed 同名残行
        assert "t::one" not in b.load_failed()
        assert b.load_cursors() == {"page": "two"}


class TestReplaceQueueAtomic:
    """读-改-写收敛的整表替换原语契约（双腿对齐）。

    不变式：compute 在写锁内接收磁盘真相快照；compute 抛异常 ⇒ 队列与
    调用前完全一致（对齐 SQLite 事务回滚）；替换以 compute 返回值为准。
    """

    def test_replace_result_wins_and_absent_rows_are_removed(self, dual_backend):
        b = dual_backend
        b.save_queue([_job_dict("keep"), _job_dict("gone")])

        def compute(disk_q):
            assert [uid_from_job_dict(j) for j in disk_q] == ["t::keep", "t::gone"]
            updated = dict(disk_q[0], payload={"v": "two"})
            fresh = _job_dict("new_one")
            return [updated, fresh]

        b.replace_queue_atomic(compute)

        q = b.load_queue()
        assert [uid_from_job_dict(j) for j in q] == ["t::keep", "t::new_one"]
        assert q[0]["payload"] == {"v": "two"}

    def test_compute_receives_rows_enqueued_before_replace(self, dual_backend):
        """替换前已提交（已应答成功）的入队必须进入磁盘真相快照，
        不得被内存态或陈旧快照替代——这是窗口期入队存活的前提。"""
        b = dual_backend
        b.save_queue([_job_dict("keep")])
        b.enqueue_jobs([_job_dict("late")])

        seen: dict[str, list[str]] = {}

        def compute(disk_q):
            seen["uids"] = [uid_from_job_dict(j) for j in disk_q]
            return disk_q

        b.replace_queue_atomic(compute)

        assert seen["uids"] == ["t::keep", "t::late"]
        assert _queue_uids(b) == ["t::keep", "t::late"]

    def test_replace_rolls_back_when_compute_raises(self, dual_backend):
        b = dual_backend
        b.save_queue([_job_dict("keep"), _job_dict("keep_two")])

        def boom(disk_q):
            raise RuntimeError("merge failed")

        with pytest.raises(RuntimeError):
            b.replace_queue_atomic(boom)

        assert _queue_uids(b) == ["t::keep", "t::keep_two"]


class TestQueueReplacementShapeValidation:
    """整表替换集形状契约（双腿一致）。

    不变式：替换集非法（None / 非序列 / 元素非 dict）必须在任何写变之前
    fail-loud 抛 TypeError——SQLite 腿若在 DELETE 后才察觉，会静默清空
    整条队列且事务正常提交；Memory 腿对同一输入必须行为一致。
    """

    def test_replace_with_none_compute_result_fails_loud(self, dual_backend):
        """compute 漏写 return（返回 None）→ TypeError，队列原状。"""
        b = dual_backend
        b.save_queue([_job_dict("keep"), _job_dict("keep_two")])

        with pytest.raises(TypeError):
            b.replace_queue_atomic(lambda disk_q: None)

        assert _queue_uids(b) == ["t::keep", "t::keep_two"]

    def test_replace_with_non_sequence_fails_loud(self, dual_backend):
        b = dual_backend
        b.save_queue([_job_dict("keep")])

        with pytest.raises(TypeError):
            b.replace_queue_atomic(lambda disk_q: 42)
        with pytest.raises(TypeError):
            b.replace_queue_atomic(lambda disk_q: {"task_type": "t", "job_id": "x"})

        assert _queue_uids(b) == ["t::keep"]

    def test_replace_with_non_dict_item_fails_loud(self, dual_backend):
        b = dual_backend
        b.save_queue([_job_dict("keep")])

        with pytest.raises(TypeError):
            b.replace_queue_atomic(
                lambda disk_q: [_job_dict("ok_entry"), "not-a-dict"]
            )

        assert _queue_uids(b) == ["t::keep"]

    def test_save_queue_none_fails_loud(self, dual_backend):
        """save_queue(None) → TypeError，队列原状（空列表仍合法清空）。"""
        b = dual_backend
        b.save_queue([_job_dict("keep")])

        with pytest.raises(TypeError):
            b.save_queue(None)
        assert _queue_uids(b) == ["t::keep"]

        b.save_queue([])
        assert b.load_queue() == []


class TestSeedWallMutualExclusion:
    """seed_wall 拒绝已存在 failed 的 uid（wall/failed 全局互斥，双腿同契约）。

    seed 链路若静默覆盖会留下 wall∩failed 重叠：下一次派发时状态一致性
    断言崩溃、配合崩溃恢复形成重启循环。语义定为整体拒绝（零写入）——
    失败档案记录须先经 delete_failed 显式清除，静默清除会丢失败历史。
    """

    def test_seed_wall_rejects_uid_already_in_failed(self, dual_backend):
        b = dual_backend
        b.append_failed("t::x_marks", {"error": "boom"})

        with pytest.raises(ValueError, match="already in failed"):
            b.seed_wall(["t::x_marks"])

        # 零写入：拒绝后 wall 不含该 uid，失败档案记录原样保留
        assert "t::x_marks" not in b.load_wall()
        assert "t::x_marks" in b.load_failed()

    def test_seed_wall_conflict_is_atomic_no_partial_write(self, dual_backend):
        """冲突批次整体拒绝：batch 内非冲突 uid 亦不得部分写入。"""
        b = dual_backend
        b.append_failed("t::bad_entry", {"error": "boom"})

        with pytest.raises(ValueError, match="already in failed"):
            b.seed_wall(["t::ok_entry", "t::bad_entry", "t::ok_next"])

        assert set(b.load_wall()) == set()
        assert "t::bad_entry" in b.load_failed()

    def test_seed_wall_fresh_uid_idempotent(self, dual_backend):
        """正常 seed（无 failed 冲突）行为不变：幂等覆盖写入。"""
        b = dual_backend
        assert b.seed_wall(["t::fresh_one", "t::fresh_two"]) == 2
        assert set(b.load_wall()) == {"t::fresh_one", "t::fresh_two"}
        # 幂等：重复 seed 仍成功（meta 重置为空）
        assert b.seed_wall(["t::fresh_one"]) == 1
        assert b.load_wall()["t::fresh_one"] == {}


class TestSeedCursorParity:
    """seed_cursor 入口校验双腿对齐（同一非法入参同抛 TypeError）。"""

    @pytest.mark.parametrize("key,value", [(None, "v"), ("k", 123), ("", "v")])
    def test_bad_types_rejected_zero_write(self, dual_backend, key, value):
        b = dual_backend
        with pytest.raises(TypeError):
            b.seed_cursor(key, value)
        assert b.load_cursors() == {}

    def test_valid_value_roundtrip_upsert(self, dual_backend):
        b = dual_backend
        b.seed_cursor("k", "v_one")
        b.seed_cursor("k", "v_two")
        assert b.load_cursors() == {"k": "v_two"}
