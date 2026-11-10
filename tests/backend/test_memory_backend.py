"""v2 InMemoryStateBackend 契约测试。

覆盖：基础 CRUD 与幂等入队、delta 提交（成功/失败/重试/批量/跳过）、
失败档案 API（payload 快照 / 定向清除）、attempts 轨迹
追加结算与查询、种子化入口校验、快照隔离。
"""

from __future__ import annotations

import pytest

from tasklite.backend import InMemoryStateBackend
from tasklite.models.job import Job
from tasklite.models.state import job_uid_from_snapshot


def _job_snapshot(job_id: str, **overrides) -> dict:
    """标准 job dict 构造（backend 存储行形态）。"""
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


class TestInMemoryBackendContract:
    """基础 CRUD、幂等入队与 meta 持久化。"""

    def test_initial_state_empty(self):
        b = InMemoryStateBackend()
        assert b.load_successes() == {}
        assert b.load_failed() == {}
        assert b.load_failure_payloads() == {}
        assert b.load_cursors() == {}
        assert b.load_queue() == []
        assert b.load_settled_attempts("t::missing") == []
        assert b.get_engine_metadata("any") is None

    def test_enqueue_dedup_within_and_across_batches(self):
        b = InMemoryStateBackend()
        first = _job_snapshot("one")
        second = _job_snapshot("two")
        assert b.enqueue_jobs([first, second]) == ["t::one", "t::two"]
        assert len(b.load_queue()) == 2

        # 队列已有 uid 与批次内重复都跳过，只插入新条目
        inserted = b.enqueue_jobs([first, _job_snapshot("one"), _job_snapshot("three")])
        assert inserted == ["t::three"]
        assert [job_uid_from_snapshot(j) for j in b.load_queue()] == [
            "t::one", "t::two", "t::three",
        ]

    def test_enqueue_front_and_back_ordering(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_snapshot("base")])
        b.enqueue_jobs([_job_snapshot("front")], front=True)
        b.enqueue_jobs([_job_snapshot("back")], front=False)
        assert [job_uid_from_snapshot(j) for j in b.load_queue()] == [
            "t::front", "t::base", "t::back",
        ]

    def test_meta_persistence(self):
        b = InMemoryStateBackend()
        assert b.get_engine_metadata("key_one") is None
        b.set_engine_metadata("key_one", "val_one")
        assert b.get_engine_metadata("key_one") == "val_one"
        b.set_engine_metadata("key_one", "val_two")
        assert b.get_engine_metadata("key_one") == "val_two"

    def test_success_settlement_with_spawn_and_cursor(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_snapshot("one")])

        ok = _settle_success(
            b,
            "t::one",
            {"status": "ok"},
            spawned_job_snapshots=[_job_snapshot("child")],
            cursor_updates={"page": "two"},
        )
        assert ok is None
        assert b.load_successes()["t::one"] == {"status": "ok"}
        assert b.load_cursors() == {"page": "two"}
        assert [job_uid_from_snapshot(j) for j in b.load_queue()] == ["t::child"]
        (rec,) = b.load_settled_attempts("t::one")
        assert rec["outcome"] == "success"
        assert rec["settled_at"] == "2026-05-01T00:00:05+00:00"

    def test_success_settlement_stores_exact_meta_without_injection(self):
        """失败/成功 meta 原样落库：backend 不解释 meta、不注入派生字段。"""
        b = InMemoryStateBackend()
        assert _settle_success(b, "t::one", {"v": 1}) is None
        assert b.load_successes() == {"t::one": {"v": 1}}

    def test_commit_job_failure_records_archive_and_clears_queue(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_snapshot("doomed")])

        ok = b.commit_job_failure("t::doomed", {"error_message": "NETWORK_TIMEOUT"})
        assert ok is True
        # v2 语义：失败 meta 精确等于调用方传入（历史与时间线由 attempts 承接）
        assert b.load_failed() == {"t::doomed": {"error_message": "NETWORK_TIMEOUT"}}
        assert b.load_queue() == []
        assert b.load_successes() == {}

    def test_commit_job_failure_preserves_remaining_queue(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_snapshot("discard"), _job_snapshot("next")])
        assert b.commit_job_failure("t::discard", {}) is True
        assert [job_uid_from_snapshot(j) for j in b.load_queue()] == ["t::next"]


    def test_commit_job_failures_preserves_remaining_order(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_snapshot(j) for j in ("first", "second", "third")])
        assert b.commit_job_failures([("t::first", {"err": "x"}), ("t::second", {"err": "y"})]) is None
        assert [job_uid_from_snapshot(j) for j in b.load_queue()] == ["t::third"]
        assert set(b.load_failed()) == {"t::first", "t::second"}

    def test_commit_job_failures_empty_is_noop(self):
        b = InMemoryStateBackend()
        assert b.commit_job_failures([]) is None
        assert b.load_failed() == {}

    def test_commit_skip_removes_stale_uid_only(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_snapshot("stale"), _job_snapshot("live")])
        assert b.commit_skip("t::stale") is None
        assert [job_uid_from_snapshot(j) for j in b.load_queue()] == ["t::live"]
        assert b.load_successes() == {} and b.load_failed() == {}

    def test_delete_queue_uids_targeted(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_snapshot(j) for j in ("a", "b", "c")])
        assert b.delete_queue_uids(["t::a", "t::c", "t::ghost"]) == 2
        assert [job_uid_from_snapshot(j) for j in b.load_queue()] == ["t::b"]


class TestMemoryFailureArchive:
    """失败档案 API：带外登记、payload 快照、定向清除、互删语义。"""

    def test_failure_archive_overwrites_same_uid(self):
        b = InMemoryStateBackend()
        b.commit_job_failure("t::dup", {"first": 1})
        b.commit_job_failure("t::dup", {"second": 2})
        assert b.load_failed() == {"t::dup": {"second": 2}}

    def test_failure_archive_empty_metadata(self):
        b = InMemoryStateBackend()
        b.commit_job_failure("t::nil", {})
        assert b.load_failed() == {"t::nil": {}}

    def test_payload_snapshot_roundtrip_and_latest_wins(self):
        b = InMemoryStateBackend()
        assert b.commit_job_failure("t::one", {"error_message": "first"}, payload={"v": 1}) is True
        assert b.load_failure_payloads() == {"t::one": {"v": 1}}
        assert b.commit_job_failure("t::one", {"error_message": "second"}, payload={"v": 2}) is True
        assert b.load_failure_payloads() == {"t::one": {"v": 2}}

    def test_none_payload_keeps_previous_snapshot(self):
        """无 payload 的写入路径（带外登记/覆盖写）不得抹掉既有快照。"""
        b = InMemoryStateBackend()
        assert b.commit_job_failure("t::one", {"error_message": "first"}, payload={"k": 1}) is True
        b.commit_job_failure("t::one", {"error_message": "second"})
        assert b.load_failure_payloads() == {"t::one": {"k": 1}}

    def test_no_snapshot_entry_absent_from_payload_view(self):
        b = InMemoryStateBackend()
        b.commit_job_failure("t::other", {"error_message": "x"})
        assert b.load_failure_payloads() == {}

    def test_delete_failed_clears_meta_and_snapshot(self):
        b = InMemoryStateBackend()
        b.commit_job_failure("t::one", {"error_message": "x"}, payload={"k": 1})
        b.commit_job_failure("t::two", {"error_message": "y"})
        assert b.delete_failed(["t::one", "t::ghost"]) == 1
        assert b.load_failed() == {"t::two": {"error_message": "y"}}
        assert b.load_failure_payloads() == {}

    def test_delete_wall_targeted(self):
        b = InMemoryStateBackend()
        _settle_success(b, "t::one", {"ok": True})
        _settle_success(b, "t::two", {"ok": True})
        assert b.delete_successes(["t::one", "t::ghost"]) == 1
        assert set(b.load_successes()) == {"t::two"}

    def test_success_commit_deletes_failed_same_uid(self):
        """最终状态唯一：成功 commit 删失败档案同名行（含快照）。"""
        b = InMemoryStateBackend()
        b.commit_job_failure("t::one", {"error_message": "stale"}, payload={"k": 1})
        assert _settle_success(b, "t::one", {"ok": True}) is None
        assert b.load_failed() == {}
        assert b.load_failure_payloads() == {}

    def test_failure_commit_deletes_wall_same_uid(self):
        """最终状态唯一：失败 commit 删 successes 同名行。"""
        b = InMemoryStateBackend()
        _settle_success(b, "t::one", {"ok": True})
        assert b.commit_job_failure("t::one", {"error_message": "boom"}) is True
        assert b.load_successes() == {}
        assert set(b.load_failed()) == {"t::one"}


class TestMemorySeedAndCursor:
    """种子化：successes/failed 互斥拒绝 + cursor 入口校验。"""

    def test_apply_manual_status_and_clear_history(self):
        b = InMemoryStateBackend()
        snap = {"job_uid": "t::fresh_one", "task_type": "t", "job_id": "fresh_one", "payload": {}}
        b.apply_manual_status([("t::fresh_one", "succeeded", snap, None)])
        assert "t::fresh_one" in b.load_successes()
        deleted = b.clear_history("t::fresh_one", records=["successes"])
        assert deleted == 1
        assert "t::fresh_one" not in b.load_successes()

    @pytest.mark.parametrize("key,value", [(None, "v"), ("k", 123), ("", "v")])
    def test_set_cursor_rejects_bad_types(self, key, value):
        b = InMemoryStateBackend()
        with pytest.raises(TypeError):
            b.set_cursor(key, value)
        assert b.load_cursors() == {}

    def test_set_cursor_valid_roundtrip_upsert_and_delete(self):
        b = InMemoryStateBackend()
        b.set_cursor("k", "v_one")
        assert b.get_cursor("k") == "v_one"
        b.set_cursor("k", "v_two")
        assert b.get_cursor("k") == "v_two"
        assert b.load_cursors() == {"k": "v_two"}
        b.set_cursor("k", None)
        assert b.get_cursor("k") is None
        assert b.load_cursors() == {}
        b.set_cursor("k", "")
        assert b.get_cursor("k") == ""
        assert b.load_cursors() == {"k": ""}


class TestMemorySnapshotIsolation:
    """快照隔离：load 返回深拷贝，写路径入参深拷贝。"""

    def test_load_queue_result_mutation_is_invisible(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_snapshot("one", payload={"x": 1})])
        snapshot = b.load_queue()
        snapshot[0]["payload"]["x"] = 999
        snapshot.pop()
        assert b.load_queue()[0]["payload"] == {"x": 1}

    def test_load_wall_and_failed_results_mutation_is_invisible(self):
        b = InMemoryStateBackend()
        _settle_success(b, "t::one", {"meta": {"v": 1}})
        b.commit_job_failure("t::two", {"meta": {"v": 2}})
        b.load_successes()["t::one"]["meta"]["v"] = 999
        b.load_failed()["t::two"]["meta"]["v"] = 999
        assert b.load_successes()["t::one"]["meta"] == {"v": 1}
        assert b.load_failed()["t::two"]["meta"] == {"v": 2}

    def test_enqueue_input_mutation_is_invisible(self):
        b = InMemoryStateBackend()
        job = _job_snapshot("one", payload={"x": 1})
        b.enqueue_jobs([job])
        job["payload"]["x"] = 999
        assert b.load_queue()[0]["payload"] == {"x": 1}
