"""v2 backend 双腿契约对齐测试：同一断言套件参数化跑 SQLite 与 Memory。

契约一致性属性（每条断言对两腿同构即构成对齐证明）：
- enqueue 后可见（幂等去重、front/back 保序）；
- pop 后不可见（delta 终态提交后 uid 从磁盘队列消失，队列行从不单独
  「裸弹出」——弹出的 uid 只在 commit 事务内删除）；
- 终态互斥（成功删 failed 同名行、失败删 successes 行，最终状态唯一）；
- attempts append-only（历史行只增不改，每行仅允许一次收尾）。

另覆盖双腿契约面：commit 失败/冲突不变式（False ⇒ 后端与调用前
完全一致）、replace_queue_atomic、替换集形状校验、seed_successes 互斥、
seed_cursor 入口校验。
"""

from __future__ import annotations

import pytest

from tasklite.backend import InMemoryStateBackend, SQLiteStateBackend
from tasklite.models.attempt import ATTEMPT_RUNNING, AttemptRecord
from tasklite.models.job import Job
from tasklite.models.state import job_uid_from_snapshot


def _job_snapshot(job_id: str, **overrides) -> dict:
    data = Job("t", job_id).to_dict()
    data.update(overrides)
    return data


def _running_attempt(job_uid: str, attempt_no: int = 1) -> AttemptRecord:
    return AttemptRecord(
        job_uid=job_uid,
        activation_no=1,
        attempt_no=attempt_no,
        execution_id=f"run_seed.{attempt_no}",
        run_id="run_seed",
        started_at="2026-05-01T00:00:00+00:00",
        outcome="running",
    )


def _open_and_commit_success(
    backend,
    job_uid: str,
    result_metadata: dict,
    *,
    attempt_no: int = 1,
    finished_at: str = "2026-05-01T00:00:05+00:00",
    **kwargs,
) -> bool:
    attempt_id = backend.append_attempt(_running_attempt(job_uid, attempt_no=attempt_no))
    return backend.commit_job_success(
        job_uid=job_uid,
        result_metadata=result_metadata,
        attempt_id=attempt_id,
        execution_id=f"run_seed.{attempt_no}",
        finished_at=finished_at,
        **kwargs,
    )


def _queue_uids(backend) -> list[str]:
    return [job_uid_from_snapshot(j) for j in backend.load_queue()]


def _snapshot(backend) -> dict:
    """与实现无关的后端状态快照，用于原子性比对（False ⇒ 全集不变）。"""
    return {
        "queue": _queue_uids(backend),
        "wall": backend.load_successes(),
        "failed": backend.load_failed(),
        "failure_payloads": backend.load_failure_payloads(),
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
        assert b.enqueue_jobs(job_snapshots=[_job_snapshot("one"), _job_snapshot("two")]) == ["t::one", "t::two"]
        # enqueue 后可见：保序落盘
        assert _queue_uids(b) == ["t::one", "t::two"]
        # 幂等去重：队列已有与批次内重复都跳过
        assert b.enqueue_jobs([_job_snapshot("one"), _job_snapshot("three"), _job_snapshot("three")]) == ["t::three"]
        assert _queue_uids(b) == ["t::one", "t::two", "t::three"]
        # front 入队插队首、保序
        assert b.enqueue_jobs([_job_snapshot("frontal")], front=True) == ["t::frontal"]
        assert _queue_uids(b) == ["t::frontal", "t::one", "t::two", "t::three"]

    def test_popped_uid_invisible_after_terminal_commit(self, dual_backend):
        """终态 delta 提交后 uid 从队列消失（成功/失败/批量/跳过四路径）。"""
        b = dual_backend
        b.enqueue_jobs([_job_snapshot(j) for j in ("success_one", "fail_one", "bulk_one", "skip_one")])

        assert _open_and_commit_success(b, job_uid="t::success_one", result_metadata={"ok": True}) is True
        assert b.commit_job_failure(job_uid="t::fail_one", failure_metadata={"error_message": "x"}) is True
        assert b.commit_job_failures(job_uids_metadata=[("t::bulk_one", {"error_message": "y"})]) is True
        assert b.commit_skip(job_uid="t::skip_one") is True

        assert _queue_uids(b) == []
        assert set(b.load_successes()) == {"t::success_one"}
        assert set(b.load_failed()) == {"t::fail_one", "t::bulk_one"}

    def test_popped_uid_requeued_visible_via_commit_retry(self, dual_backend):
        """重试 delta 是同 uid DELETE+INSERT：重入队后以新行可见。"""
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one")])
        requeued = _job_snapshot("one", attempt_no=2)
        assert b.commit_retry(popped_job_uid="t::one", requeued_job_snapshot=requeued) is True
        queue = b.load_queue()
        assert _queue_uids(b) == ["t::one"]
        assert queue[0]["attempt_no"] == 2

    def test_success_failure_mutually_delete_opposite_side(self, dual_backend):
        """终态互斥：成功删 failed 同名行、失败删 successes 行（最终状态唯一）。"""
        b = dual_backend
        metadata = {"error_message": "stale", "payload": "档案元数据", "nested": {"new_jobs": [1]}}
        payload = {"error": "业务输入", "metadata": "业务值", "nested": {"timeout": 9}}
        assert b.commit_job_failure(
            job_uid="t::one", failure_metadata=metadata, payload=payload
        ) is True
        assert b.load_failed()["t::one"] == metadata
        assert b.load_failure_payloads()["t::one"] == payload
        _open_and_commit_success(b, "t::other", {"ok": True})

        assert _open_and_commit_success(b, "t::one", {"ok": True}) is True
        assert b.load_failed() == {}
        assert b.load_failure_payloads() == {}

        assert b.commit_job_failure("t::other", {"error_message": "boom"}) is True
        assert b.load_successes() == {"t::one": {"ok": True}}
        assert not (set(b.load_successes()) & set(b.load_failed()))

    def test_attempts_append_only_never_rewrites_history(self, dual_backend):
        """attempts 只增不改：历史行字段稳定、每行仅一次收尾。"""
        b = dual_backend
        first = b.append_attempt(_running_attempt("t::one"))
        second = b.append_attempt(_running_attempt("t::one", attempt_no=2))
        assert first < second

        assert b.update_attempt(
            first, outcome="failed",
            finished_at="2026-05-01T00:00:05+00:00", error_message="boom",
        ) is True

        third = b.append_attempt(_running_attempt("t::other"))
        assert third > second

        records = b.load_attempts("t::one")
        assert [r.attempt_no for r in records] == [1, 2]
        assert records[0].outcome == "failed"
        assert records[0].execution_id == "run_seed.1"
        assert records[1].outcome == ATTEMPT_RUNNING

        # 已收尾行不可二次收尾；未知 id 返回 False（双腿同构）
        with pytest.raises(ValueError, match="already finalized"):
            b.update_attempt(
                first, outcome="succeeded", finished_at="2026-05-01T00:00:09+00:00"
            )
        assert b.update_attempt(
            999999, outcome="skipped", finished_at="2026-05-01T00:00:09+00:00"
        ) is False

    def test_load_latest_succeeded_finished_at_narrow_query(self, dual_backend):
        """按 attempt_id 倒序精确定位最新一次成功完成时间戳，不受前后非成功行干扰。"""
        b = dual_backend
        assert b.load_latest_succeeded_finished_at("t::none") is None

        # 仅有 running / failed 轨迹时无成功基线
        a1 = b.append_attempt(_running_attempt("t::job", attempt_no=1))
        b.update_attempt(a1, outcome="failed", finished_at="2026-05-01T00:00:01+00:00", error_message="boom")
        assert b.load_latest_succeeded_finished_at("t::job") is None

        # 写入第一次成功
        a2 = b.append_attempt(_running_attempt("t::job", attempt_no=2))
        b.update_attempt(a2, outcome="succeeded", finished_at="2026-05-01T00:00:02+00:00")
        assert b.load_latest_succeeded_finished_at("t::job") == "2026-05-01T00:00:02+00:00"

        # 写入第二次成功（时间更新）
        a3 = b.append_attempt(_running_attempt("t::job", attempt_no=3))
        b.update_attempt(a3, outcome="succeeded", finished_at="2026-05-01T00:00:05+00:00")
        assert b.load_latest_succeeded_finished_at("t::job") == "2026-05-01T00:00:05+00:00"

        # 随后发生失败重跑或在途执行，最新成功时间戳不受干扰
        a4 = b.append_attempt(_running_attempt("t::job", attempt_no=4))
        b.update_attempt(a4, outcome="failed", finished_at="2026-05-01T00:00:06+00:00", error_message="err")
        b.append_attempt(_running_attempt("t::job", attempt_no=5))
        assert b.load_latest_succeeded_finished_at("t::job") == "2026-05-01T00:00:05+00:00"

    def test_meta_roundtrip_upsert(self, dual_backend):
        b = dual_backend
        assert b.get_engine_metadata("fencing") is None
        b.set_engine_metadata("fencing", "run_seed")
        b.set_engine_metadata("fencing", "run_next")
        assert b.get_engine_metadata("fencing") == "run_next"


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
        b.enqueue_jobs([_job_snapshot("one"), _job_snapshot("child")])
        b.append_failed(job_uid="t::one", metadata={"error_message": "stale"})
        attempt_id = b.append_attempt(_running_attempt("t::one"))
        before = _snapshot(b)

        ok = b.commit_job_success(
            "t::one",
            {"status": "ok"},
            attempt_id=attempt_id,
            execution_id="run_seed.1",
            finished_at="2026-05-01T00:00:05+00:00",
            spawned_job_snapshots=[_job_snapshot("child")],
            cursor_updates={"cur": "nine"},
        )

        assert ok is False
        # popped uid 仍在队列、attempts/successes/failed/cursors 均未被触碰
        assert _snapshot(b) == before
        assert b.load_attempts("t::one")[0].outcome == "running"
        assert "t::one" in _queue_uids(b)

        # 冲突被拒后后端仍可正常提交同一 job（spawned 改为不冲突的新 uid）
        ok = b.commit_job_success(
            "t::one",
            {"status": "ok"},
            attempt_id=attempt_id,
            execution_id="run_seed.1",
            finished_at="2026-05-01T00:00:05+00:00",
            spawned_job_snapshots=[_job_snapshot("fresh")],
        )
        assert ok is True
        # spawned 队首插入，预置的 t::child 条目保持原位
        assert _queue_uids(b) == ["t::fresh", "t::child"]
        assert "t::one" in b.load_successes()
        assert b.load_attempts("t::one")[0].outcome == "succeeded"

    def test_spawn_batch_duplicate_uid_rejected_without_dup_entries(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one")])
        attempt_id = b.append_attempt(_running_attempt("t::one"))
        before = _snapshot(b)

        ok = b.commit_job_success(
            "t::one",
            {},
            attempt_id=attempt_id,
            execution_id="run_seed.1",
            finished_at="2026-05-01T00:00:05+00:00",
            spawned_job_snapshots=[_job_snapshot("x_marks"), _job_snapshot("x_marks")],
        )

        assert ok is False
        assert _snapshot(b) == before
        assert b.load_attempts("t::one")[0].outcome == "running"
        # 绝不产出重复队列条目却报成功
        uids = _queue_uids(b)
        assert len(uids) == len(set(uids))

    def test_retry_uid_conflict_returns_false_keeping_queue_intact(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("alpha"), _job_snapshot("beta")])
        before = _snapshot(b)

        ok = b.commit_retry("t::alpha", _job_snapshot("beta", attempt_no=2), front=True)

        assert ok is False
        assert _snapshot(b) == before
        assert _queue_uids(b) == ["t::alpha", "t::beta"]

    def test_retry_same_uid_requeue_still_succeeds(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("alpha"), _job_snapshot("beta")])

        assert b.commit_retry("t::alpha", _job_snapshot("alpha", attempt_no=2), front=True) is True
        assert _queue_uids(b) == ["t::alpha", "t::beta"]
        assert b.commit_retry("t::alpha", _job_snapshot("alpha", attempt_no=3), front=False) is True
        assert _queue_uids(b) == ["t::beta", "t::alpha"]

    def test_bulk_failure_write_error_leaves_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one"), _job_snapshot("two")])
        b.seed_successes(["t::nine"])
        before = _snapshot(b)

        ok = b.commit_job_failures([
            ("t::one", {"error_message": "d_one"}),
            ("t::two", {"payload": _UncommittableMeta()}),
        ])

        assert ok is False
        # 队列绝不被整体删除、successes 绝不被清理
        assert _snapshot(b) == before

    def test_job_failure_write_error_leaves_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one")])
        before = _snapshot(b)

        ok = b.commit_job_failure("t::one", {"payload": _UncommittableMeta()})

        assert ok is False
        assert _snapshot(b) == before
        assert "t::one" in _queue_uids(b)

    def test_success_commit_with_spawn_and_cursor_still_applies(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one")])
        b.append_failed("t::one", {"error_message": "stale"})

        ok = _open_and_commit_success(
            b,
            "t::one",
            {"status": "ok"},
            spawned_job_snapshots=[_job_snapshot("child_one"), _job_snapshot("child_two")],
            cursor_updates={"page": "two"},
        )

        assert ok is True
        assert _queue_uids(b) == ["t::child_one", "t::child_two"]
        assert b.load_successes()["t::one"]["status"] == "ok"
        # 成功 commit 清理 failed 同名残行
        assert "t::one" not in b.load_failed()
        assert b.load_cursors() == {"page": "two"}

    def test_success_commit_rejects_invalid_attempt_row_keeping_all_tables_unchanged(self, dual_backend):
        """成功提交校验 attempts 行（缺行、uid/执行代不符、已收尾），失败时全表与历史行零改动。"""
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one")])
        b.append_failed("t::one", {"error_message": "stale"},)
        b.set_cursor("cur", "initial")

        # 历史已收尾行（不可被后续成功提交触碰）
        hist_id = b.append_attempt(_running_attempt("t::one", attempt_no=1))
        assert b.update_attempt(
            hist_id, outcome="failed", finished_at="2026-05-01T00:00:01+00:00", error_message="boom"
        ) is True
        other_id = b.append_attempt(_running_attempt("t::other", attempt_no=1))
        active_id = b.append_attempt(_running_attempt("t::one", attempt_no=2))
        before = _snapshot(b)
        before_attempts_one = b.load_attempts("t::one")
        before_attempts_other = b.load_attempts("t::other")

        invalid_cases = [
            # 1. attempt_id 为 None
            dict(attempt_id=None, execution_id="run_seed.2", finished_at="2026-05-01T00:00:05+00:00"),
            # 2. attempt_id 不存在
            dict(attempt_id=999999, execution_id="run_seed.2", finished_at="2026-05-01T00:00:05+00:00"),
            # 3. attempt_id 对应其他 job_uid
            dict(attempt_id=other_id, execution_id="run_seed.1", finished_at="2026-05-01T00:00:05+00:00"),
            # 4. execution_id 不匹配
            dict(attempt_id=active_id, execution_id="wrong_exec.2", finished_at="2026-05-01T00:00:05+00:00"),
            # 5. 目标 attempt_id 已经非 running（已收尾）
            dict(attempt_id=hist_id, execution_id="run_seed.1", finished_at="2026-05-01T00:00:05+00:00"),
            # 6. finished_at 缺失或为空
            dict(attempt_id=active_id, execution_id="run_seed.2", finished_at=None),
            dict(attempt_id=active_id, execution_id="run_seed.2", finished_at=""),
        ]
        for case in invalid_cases:
            ok = b.commit_job_success(
                "t::one",
                {"status": "ok"},
                spawned_job_snapshots=[_job_snapshot("child")],
                cursor_updates={"cur": "updated"},
                **case,
            )
            assert ok is False, f"expected False for {case}"
            assert _snapshot(b) == before
            assert b.load_attempts("t::one") == before_attempts_one
            assert b.load_attempts("t::other") == before_attempts_other

        # 随后传入合法参数时正常原子提交，历史行仍不改
        ok = b.commit_job_success(
            "t::one",
            {"status": "ok"},
            attempt_id=active_id,
            execution_id="run_seed.2",
            finished_at="2026-05-01T00:00:05+00:00",
            spawned_job_snapshots=[_job_snapshot("child")],
            cursor_updates={"cur": "updated"},
        )
        assert ok is True
        attempts_after = b.load_attempts("t::one")
        assert attempts_after[0] == before_attempts_one[0]
        assert attempts_after[1].outcome == "succeeded"
        assert attempts_after[1].finished_at == "2026-05-01T00:00:05+00:00"
        assert b.load_attempts("t::other") == before_attempts_other


class TestReplaceQueueAtomic:
    """读-改-写收敛的整表替换原语契约（双腿对齐）。

    不变式：compute 在写锁内接收磁盘真相快照；compute 抛异常 ⇒ 队列与
    调用前完全一致（对齐 SQLite 事务回滚）；替换以 compute 返回值为准。
    """

    def test_replace_result_wins_and_absent_rows_are_removed(self, dual_backend):
        b = dual_backend
        b.save_queue(job_snapshots=[_job_snapshot("keep"), _job_snapshot("gone")])

        def compute(disk_q):
            assert [job_uid_from_snapshot(j) for j in disk_q] == ["t::keep", "t::gone"]
            updated = dict(disk_q[0], payload={"v": "two"})
            fresh = _job_snapshot("new_one")
            return [updated, fresh]

        b.replace_queue_atomic(compute)

        q = b.load_queue()
        assert [job_uid_from_snapshot(j) for j in q] == ["t::keep", "t::new_one"]
        assert q[0]["payload"] == {"v": "two"}

    def test_compute_receives_rows_enqueued_before_replace(self, dual_backend):
        """替换前已提交（已应答成功）的入队必须进入磁盘真相快照，
        不得被内存态或陈旧快照替代——这是窗口期入队存活的前提。"""
        b = dual_backend
        b.save_queue([_job_snapshot("keep")])
        b.enqueue_jobs([_job_snapshot("late")])

        seen: dict[str, list[str]] = {}

        def compute(disk_q):
            seen["uids"] = [job_uid_from_snapshot(j) for j in disk_q]
            return disk_q

        b.replace_queue_atomic(compute)

        assert seen["uids"] == ["t::keep", "t::late"]
        assert _queue_uids(b) == ["t::keep", "t::late"]

    def test_replace_rolls_back_when_compute_raises(self, dual_backend):
        b = dual_backend
        b.save_queue([_job_snapshot("keep"), _job_snapshot("keep_two")])

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
        b.save_queue([_job_snapshot("keep"), _job_snapshot("keep_two")])

        with pytest.raises(TypeError):
            b.replace_queue_atomic(lambda disk_q: None)

        assert _queue_uids(b) == ["t::keep", "t::keep_two"]

    def test_replace_with_non_sequence_fails_loud(self, dual_backend):
        b = dual_backend
        b.save_queue([_job_snapshot("keep")])

        with pytest.raises(TypeError):
            b.replace_queue_atomic(lambda disk_q: 42)
        with pytest.raises(TypeError):
            b.replace_queue_atomic(lambda disk_q: {"task_type": "t", "job_id": "x"})

        assert _queue_uids(b) == ["t::keep"]

    def test_replace_with_non_dict_item_fails_loud(self, dual_backend):
        b = dual_backend
        b.save_queue([_job_snapshot("keep")])

        with pytest.raises(TypeError):
            b.replace_queue_atomic(
                lambda disk_q: [_job_snapshot("ok_entry"), "not-a-dict"]
            )

        assert _queue_uids(b) == ["t::keep"]

    def test_save_queue_none_fails_loud(self, dual_backend):
        """save_queue(None) → TypeError，队列原状（空列表仍合法清空）。"""
        b = dual_backend
        b.save_queue([_job_snapshot("keep")])

        with pytest.raises(TypeError):
            b.save_queue(None)
        assert _queue_uids(b) == ["t::keep"]

        b.save_queue([])
        assert b.load_queue() == []


class TestSeedWallMutualExclusion:
    """seed_successes 拒绝已存在 failed 的 uid（successes/failed 全局互斥，双腿同契约）。

    seed 链路若静默覆盖会留下 successes∩failed 重叠：下一次派发时状态一致性
    断言崩溃、配合崩溃恢复形成重启循环。语义定为整体拒绝（零写入）——
    失败档案记录须先经 delete_failed 显式清除，静默清除会丢失败历史。
    """

    def test_seed_wall_rejects_uid_already_in_failed(self, dual_backend):
        b = dual_backend
        b.append_failed("t::x_marks", {"error_message": "boom"})

        with pytest.raises(ValueError, match="already in failed"):
            b.seed_successes(["t::x_marks"])

        # 零写入：拒绝后 successes 不含该 uid，失败档案记录原样保留
        assert "t::x_marks" not in b.load_successes()
        assert "t::x_marks" in b.load_failed()

    def test_seed_wall_conflict_is_atomic_no_partial_write(self, dual_backend):
        """冲突批次整体拒绝：batch 内非冲突 uid 亦不得部分写入。"""
        b = dual_backend
        b.append_failed("t::bad_entry", {"error_message": "boom"})

        with pytest.raises(ValueError, match="already in failed"):
            b.seed_successes(["t::ok_entry", "t::bad_entry", "t::ok_next"])

        assert set(b.load_successes()) == set()
        assert "t::bad_entry" in b.load_failed()

    def test_seed_wall_fresh_uid_idempotent(self, dual_backend):
        """正常 seed（无 failed 冲突）行为不变：幂等覆盖写入。"""
        b = dual_backend
        assert b.seed_successes(["t::fresh_one", "t::fresh_two"]) == 2
        assert set(b.load_successes()) == {"t::fresh_one", "t::fresh_two"}
        # 幂等：重复 seed 仍成功（meta 重置为空）
        assert b.seed_successes(["t::fresh_one"]) == 1
        assert b.load_successes()["t::fresh_one"] == {}


class TestSetCursorParity:
    """set_cursor 入口校验双腿对齐（同一非法入参同抛 TypeError）。"""

    @pytest.mark.parametrize("key,value", [(None, "v"), ("k", 123), ("", "v")])
    def test_bad_types_rejected_zero_write(self, dual_backend, key, value):
        b = dual_backend
        with pytest.raises(TypeError):
            b.set_cursor(key, value)
        assert b.load_cursors() == {}

    def test_valid_value_roundtrip_upsert_and_delete(self, dual_backend):
        b = dual_backend
        b.set_cursor("k", "v_one")
        assert b.get_cursor("k") == "v_one"
        b.set_cursor("k", "v_two")
        assert b.get_cursor("k") == "v_two"
        assert b.load_cursors() == {"k": "v_two"}
        b.set_cursor("k", None)
        assert b.get_cursor("k") is None
        assert b.load_cursors() == {}


@pytest.mark.parametrize("payload", [None, {}, {"error": "业务", "error_message": "新业务", "nested": {"new_jobs": [], "spawned_jobs": [1]}}])
def test_archive_metadata_and_payload_are_independent_untranslated_copies(dual_backend, payload):
    import copy

    backend = dual_backend
    metadata = {"error": "旧业务键", "error_message": "框架诊断", "fatal": "业务", "is_fatal": False,
                "_attempt": 9, "attempt_no": 0, "nested": {"timeout": 17, "timeout_seconds": 18}}
    expected_metadata = copy.deepcopy(metadata)
    expected_payload = copy.deepcopy(payload)
    assert backend.commit_job_failure("t::separate", metadata, payload=payload)
    metadata["nested"]["timeout"] = 99
    if payload:
        payload["nested"]["new_jobs"].append(99)
    assert backend.load_failed() == {"t::separate": expected_metadata}
    expected_payloads = {} if payload is None else {"t::separate": expected_payload}
    assert backend.load_failure_payloads() == expected_payloads
    backend.load_failed()["t::separate"]["nested"]["timeout"] = 999
    assert backend.load_failed() == {"t::separate": expected_metadata}
    assert backend.commit_job_failure("t::separate", {"replacement": True}, payload=None)
    assert backend.load_failure_payloads() == expected_payloads
