"""v2 backend 双腿契约对齐测试：同一断言套件参数化跑 SQLite 与 Memory。

契约一致性属性（每条断言对两腿同构即构成对齐证明）：
- enqueue 后可见（幂等去重、front/back 保序）；
- pop 后不可见（delta 终态提交后 uid 从磁盘队列消失，队列行从不单独
  「裸弹出」——弹出的 uid 只在 commit 事务内删除）；
- 终态互斥（成功删 failed 同名行、失败删 successes 行，最终状态唯一）；
- attempts append-only（仅结算时追加，历史行只增不改）。

另覆盖双腿契约面：commit 失败/冲突不变式（异常 ⇒ 后端与调用前完全一致）、replace_queue_atomic、替换集形状校验、apply_manual_status 互斥、
游标入口校验。
"""

from __future__ import annotations

import pytest

from tasklite.backend import InMemoryStateBackend, SQLiteStateBackend
from tasklite.models.job import Job
from tasklite.models.state import job_uid_from_snapshot


def _job_snapshot(job_id: str, **overrides) -> dict:
    data = Job("t", job_id).to_dict()
    data.update(overrides)
    return data


def _settle_success(
    backend, job_uid, result_metadata, *, attempt_no=1,
    settled_at="2026-05-01T00:00:05+00:00", **kwargs,
):
    snapshot = next(
        (job for job in backend.load_queue() if job_uid_from_snapshot(job) == job_uid), None
    )
    if snapshot is None:
        from tasklite import parse_job_uid

        task_type, job_id = parse_job_uid(job_uid)
        snapshot = Job(task_type, job_id).to_dict()
        backend.enqueue_jobs([snapshot])
    sequence = backend.mark_job_running(
        job_uid, execution_id=f"run_seed.{attempt_no}", run_id="run_seed",
        started_at="2026-05-01T00:00:00+00:00",
    )
    backend.commit_job_settlement(
        job_uid=job_uid, outcome="success", attempt_no=sequence,
        execution_id=f"run_seed.{attempt_no}", run_id="run_seed", settled_at=settled_at,
        started_at="2026-05-01T00:00:00+00:00", job_snapshot=snapshot,
        success_metadata=result_metadata, **kwargs,
    )


def _queue_uids(backend) -> list[str]:
    return [job_uid_from_snapshot(j) for j in backend.load_queue()]


def _snapshot(backend) -> dict:
    """后端可观察状态快照，用于故障原子性比对。"""
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

        assert _settle_success(b, job_uid="t::success_one", result_metadata={"ok": True}) is None
        assert b.commit_job_failure(job_uid="t::fail_one", failure_metadata={"error_message": "x"}) is True
        assert b.commit_job_failures(job_uids_metadata=[("t::bulk_one", {"error_message": "y"})]) is None
        assert b.commit_skip(job_uid="t::skip_one") is None

        assert _queue_uids(b) == []
        assert set(b.load_successes()) == {"t::success_one"}
        assert set(b.load_failed()) == {"t::fail_one", "t::bulk_one"}


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
        _settle_success(b, "t::other", {"ok": True})

        assert _settle_success(b, "t::one", {"ok": True}) is None
        assert b.load_failed() == {}
        assert b.load_failure_payloads() == {}

        assert b.commit_job_failure("t::other", {"error_message": "boom"}) is True
        assert b.load_successes() == {"t::one": {"ok": True}}
        assert not (set(b.load_successes()) & set(b.load_failed()))


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
    """commit_* 故障时原样抛异常且后端状态与调用前完全一致。"""


    def test_bulk_failure_write_error_leaves_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one"), _job_snapshot("two")])
        b.apply_manual_status([("t::nine", "succeeded", _job_snapshot("nine"), None)])
        before = _snapshot(b)

        with pytest.raises((ValueError, TypeError)):
            b.commit_job_failures([
                ("t::one", {"error_message": "d_one"}),
                ("t::two", {"payload": _UncommittableMeta()}),
            ])
        # 队列绝不被整体删除、successes 绝不被清理
        assert _snapshot(b) == before

    def test_job_failure_write_error_leaves_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one")])
        before = _snapshot(b)

        with pytest.raises((ValueError, TypeError)):
            b.commit_job_failure("t::one", {"payload": _UncommittableMeta()})
        assert _snapshot(b) == before
        assert "t::one" in _queue_uids(b)

    def test_success_commit_with_spawn_and_cursor_still_applies(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one")])
        b.commit_job_failure("t::one", {"error_message": "stale"})

        ok = _settle_success(
            b,
            "t::one",
            {"status": "ok"},
            spawned_job_snapshots=[_job_snapshot("child_one"), _job_snapshot("child_two")],
            cursor_updates={"page": "two"},
        )

        assert ok is None
        assert _queue_uids(b) == ["t::child_one", "t::child_two"]
        assert b.load_successes()["t::one"]["status"] == "ok"
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


class TestApplyManualStatusAndClearHistoryParity:
    """apply_manual_status 与 clear_history 双腿同契约对齐。"""

    def test_apply_manual_status_succeeded_and_failed_and_pending(self, dual_backend):
        b = dual_backend
        snap_a = _job_snapshot("a")
        snap_b = _job_snapshot("b")
        snap_c = _job_snapshot("c")

        # 初始入队 a, b, c
        b.enqueue_jobs([snap_a, snap_b, snap_c])
        assert set(_queue_uids(b)) == {"t::a", "t::b", "t::c"}

        # manual succeeded: 从 queue 移除，写入 successes
        b.apply_manual_status([(
            "t::a",
            "succeeded",
            snap_a,
            {"annotations": {"completion_source": "manual"}},
        )])
        assert "t::a" not in _queue_uids(b)
        assert "t::a" in b.load_successes()
        meta_a = b.load_successes()["t::a"]
        assert meta_a.get("annotations", {}).get("completion_source") == "manual"

        # manual failed: 从 queue 移除，写入 failed
        b.apply_manual_status([(
            "t::b",
            "failed",
            snap_b,
            {"note": "测试失败", "annotations": {"completion_source": "manual"}},
        )])
        assert "t::b" not in _queue_uids(b)
        assert "t::b" in b.load_failed()
        meta_b = b.load_failed()["t::b"]
        assert meta_b.get("note") == "测试失败"
        assert meta_b.get("annotations", {}).get("completion_source") == "manual"

        # manual pending: 从 successes/failed 恢复到 queue
        b.apply_manual_status([("t::a", "pending", snap_a, None), ("t::b", "pending", snap_b, None)])
        assert "t::a" not in b.load_successes()
        assert "t::b" not in b.load_failed()
        assert set(_queue_uids(b)) == {"t::a", "t::b", "t::c"}

    def test_clear_history_parity(self, dual_backend):
        b = dual_backend
        snap = _job_snapshot("hist")
        # 成功
        b.apply_manual_status([("t::hist", "succeeded", snap, None)])
        assert "t::hist" in b.load_successes()
        # 清除 successes
        deleted = b.clear_history("t::hist", records=["successes"])
        assert deleted == 1
        assert "t::hist" not in b.load_successes()

        # 写入 failed
        b.apply_manual_status([("t::hist", "failed", snap, None)])
        assert "t::hist" in b.load_failed()
        deleted = b.clear_history("t::hist", records=["failures"])
        assert deleted == 1
        assert "t::hist" not in b.load_failed()

        # 空 records 或不存在
        assert b.clear_history("t::hist", records=["successes", "failures"]) == 0


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


@pytest.mark.parametrize("conflict", ["existing", "duplicate"])
def test_spawn_conflict_rolls_back_complete_settlement(dual_backend, conflict):
    backend = dual_backend
    backend.commit_job_failure("t::parent", {"old": True}, {"input": 1})
    backend.enqueue_jobs([_job_snapshot("parent"), _job_snapshot("existing")])
    backend.set_cursor("offset", "old")
    backend.mark_job_running("t::parent", execution_id="run.1", run_id="run", started_at="start")
    before = _snapshot(backend)
    before_queue = backend.load_queue_jobs()
    spawned = [_job_snapshot("existing")] if conflict == "existing" else [_job_snapshot("child")] * 2
    from tasklite import TaskLiteError
    with pytest.raises(TaskLiteError, match="conflict"):
        backend.commit_job_settlement(job_uid="t::parent", outcome="success", attempt_no=1,
                                      execution_id="run.1", run_id="run", settled_at="finish",
                                      success_metadata={"new": True}, spawned_job_snapshots=spawned,
                                      cursor_updates={"offset": "new"})
    assert _snapshot(backend) == before
    assert backend.load_queue_jobs() == before_queue
    assert backend.load_settled_attempts("t::parent") == []
    assert backend.get_attempt_sequence("t::parent") == 1


def test_settled_history_is_immutable_isolated_and_orders_by_sequence(dual_backend):
    backend = dual_backend
    job = _job_snapshot("parent")
    backend.enqueue_jobs([job])
    for number, outcome in enumerate(("retry", "retry", "success"), 1):
        assert backend.mark_job_running("t::parent", execution_id=f"run.{number}",
                                        run_id="run", started_at=f"start-{number}") == number
        assert len(backend.load_settled_attempts("t::parent")) == number - 1
        backend.commit_job_settlement(job_uid="t::parent", outcome=outcome, attempt_no=number,
                                      execution_id=f"run.{number}", run_id="run", settled_at=f"finish-{number}",
                                      started_at=f"start-{number}", job_snapshot=job,
                                      files={"output": {"path": "/output"}}, result={"metadata": {"n": number}})
        if number == 1:
            first = backend.load_settled_attempts("t::parent")[0]
    records = backend.load_settled_attempts("t::parent")
    assert records[0] == first
    assert [record["attempt_no"] for record in records] == [1, 2, 3]
    assert [record["outcome"] for record in records] == ["retry", "retry", "success"]
    assert len({record["attempt_id"] for record in records}) == 3
    assert backend.load_latest_succeeded_finished_at("t::parent") == "finish-3"
    records[0]["result"]["metadata"]["n"] = 999
    records[0]["files"]["output"]["path"] = "changed"
    assert backend.load_settled_attempts("t::parent")[0] == first
    assert backend.load_latest_succeeded_finished_at("t::missing") is None
    backend.commit_job_failure("t::parent", {"preflight": True})
    backend.commit_skip("t::parent")
    assert backend.load_settled_attempts("t::parent")[0] == first
