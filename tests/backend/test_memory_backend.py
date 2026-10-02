"""v2 InMemoryStateBackend 契约测试。

覆盖：基础 CRUD 与幂等入队、delta 提交（成功/失败/重试/批量/跳过）、
失败档案 API（带外登记 / payload 快照 / 定向清除）、attempts 轨迹
插入-收尾-查询、种子化入口校验、快照隔离。
"""

from __future__ import annotations

import pytest

from tasklite.backend import InMemoryStateBackend
from tasklite.models.attempt import ATTEMPT_RUNNING, AttemptRecord
from tasklite.models.job import Job
from tasklite.models.state import uid_from_job_dict


def _job_dict(job_id: str, **overrides) -> dict:
    """标准 job dict 构造（backend 存储行形态）。"""
    data = Job("t", job_id).to_dict()
    data.update(overrides)
    return data


def _running_attempt(job_uid: str, attempt_no: int = 1) -> AttemptRecord:
    """派发即插行标准形态。"""
    return AttemptRecord(
        job_uid=job_uid,
        activation_no=1,
        attempt_no=attempt_no,
        incarnation=f"run_seed.{attempt_no}",
        run_id="run_seed",
        started_at="2026-05-01T00:00:00+00:00",
        outcome="running",
    )


class TestInMemoryBackendContract:
    """基础 CRUD、幂等入队与 meta 持久化。"""

    def test_initial_state_empty(self):
        b = InMemoryStateBackend()
        assert b.load_wall() == {}
        assert b.load_failed() == {}
        assert b.load_failed_payloads() == {}
        assert b.load_cursors() == {}
        assert b.load_queue() == []
        assert b.load_attempts("t::missing") == []
        assert b.get_meta("any") is None

    def test_enqueue_dedup_within_and_across_batches(self):
        b = InMemoryStateBackend()
        first = _job_dict("one")
        second = _job_dict("two")
        assert b.enqueue_jobs([first, second]) == ["t::one", "t::two"]
        assert len(b.load_queue()) == 2

        # 队列已有 uid 与批次内重复都跳过，只插入新条目
        inserted = b.enqueue_jobs([first, _job_dict("one"), _job_dict("three")])
        assert inserted == ["t::three"]
        assert [uid_from_job_dict(j) for j in b.load_queue()] == [
            "t::one", "t::two", "t::three",
        ]

    def test_enqueue_front_and_back_ordering(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_dict("base")])
        b.enqueue_jobs([_job_dict("front")], front=True)
        b.enqueue_jobs([_job_dict("back")], front=False)
        assert [uid_from_job_dict(j) for j in b.load_queue()] == [
            "t::front", "t::base", "t::back",
        ]

    def test_meta_persistence(self):
        b = InMemoryStateBackend()
        assert b.get_meta("key_one") is None
        b.set_meta("key_one", "val_one")
        assert b.get_meta("key_one") == "val_one"
        b.set_meta("key_one", "val_two")
        assert b.get_meta("key_one") == "val_two"

    def test_commit_job_success_with_spawn_and_cursor(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_dict("one")])

        ok = b.commit_job_success(
            "t::one",
            {"status": "ok"},
            spawned_jobs=[_job_dict("child")],
            cursor_updates={"page": "two"},
        )
        assert ok is True
        assert b.load_wall()["t::one"] == {"status": "ok"}
        assert b.load_cursors() == {"page": "two"}
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::child"]

    def test_commit_job_success_stores_exact_meta_without_injection(self):
        """失败/成功 meta 原样落库：backend 不解释 meta、不注入派生字段。"""
        b = InMemoryStateBackend()
        assert b.commit_job_success("t::one", {"v": 1}) is True
        assert b.load_wall() == {"t::one": {"v": 1}}

    def test_commit_job_failure_records_archive_and_clears_queue(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_dict("doomed")])

        ok = b.commit_job_failure("t::doomed", {"error": "NETWORK_TIMEOUT"})
        assert ok is True
        # v2 语义：失败 meta 精确等于调用方传入（历史与时间线由 attempts 承接）
        assert b.load_failed() == {"t::doomed": {"error": "NETWORK_TIMEOUT"}}
        assert b.load_queue() == []
        assert b.load_wall() == {}

    def test_commit_job_failure_preserves_remaining_queue(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_dict("discard"), _job_dict("next")])
        assert b.commit_job_failure("t::discard", {}) is True
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::next"]

    def test_commit_retry_same_uid_requeue(self):
        """同 uid 重试 = DELETE+INSERT：实例位随 job_data 自然往返。"""
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_dict("retry_one")])

        requeued = _job_dict("retry_one", attempt_no=2)
        assert b.commit_retry("t::retry_one", requeued, front=True) is True
        queue = b.load_queue()
        assert len(queue) == 1
        assert queue[0]["attempt_no"] == 2
        assert b.load_wall() == {} and b.load_failed() == {}

    def test_commit_retry_conflicting_uid_returns_false(self):
        b = InMemoryStateBackend()
        job_a = _job_dict("alpha")
        job_b = _job_dict("beta")
        b.enqueue_jobs([job_a, job_b])

        assert b.commit_retry("t::ghost", job_a) is False
        # 冲突后 on-disk 队列原样（绝不产出重复条目却报成功）
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::alpha", "t::beta"]

    def test_commit_bulk_failure_preserves_remaining_order(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_dict(j) for j in ("first", "second", "third")])
        assert b.commit_bulk_failure([("t::first", {"err": "x"}), ("t::second", {"err": "y"})]) is True
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::third"]
        assert set(b.load_failed()) == {"t::first", "t::second"}

    def test_commit_bulk_failure_empty_is_noop(self):
        b = InMemoryStateBackend()
        assert b.commit_bulk_failure([]) is True
        assert b.load_failed() == {}

    def test_commit_skip_removes_stale_uid_only(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_dict("stale"), _job_dict("live")])
        assert b.commit_skip("t::stale") is True
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::live"]
        assert b.load_wall() == {} and b.load_failed() == {}

    def test_delete_queue_uids_targeted(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_dict(j) for j in ("a", "b", "c")])
        assert b.delete_queue_uids(["t::a", "t::c", "t::ghost"]) == 2
        assert [uid_from_job_dict(j) for j in b.load_queue()] == ["t::b"]


class TestMemoryAttemptTrajectory:
    """attempts 轨迹：插入（派发即插行）→ 收尾（一次）→ 查询。"""

    def test_append_assigns_increasing_ids(self):
        b = InMemoryStateBackend()
        first = b.append_attempt(_running_attempt("t::one"))
        second = b.append_attempt(_running_attempt("t::one", attempt_no=2))
        third = b.append_attempt(_running_attempt("t::other"))
        assert first < second < third

    def test_append_rejects_non_dispatch_shape(self):
        b = InMemoryStateBackend()
        with pytest.raises(ValueError, match="fresh running row"):
            b.append_attempt(
                AttemptRecord(
                    job_uid="t::one", activation_no=1, attempt_no=1,
                    incarnation="run_seed.1", run_id="run_seed",
                    started_at="2026-05-01T00:00:00+00:00",
                    outcome="succeeded",
                    finished_at="2026-05-01T00:00:05+00:00",
                )
            )
        assert b.load_attempts("t::one") == []

    def test_update_finalizes_running_row(self):
        b = InMemoryStateBackend()
        attempt_id = b.append_attempt(_running_attempt("t::one"))
        ok = b.update_attempt(
            attempt_id, outcome="requeued",
            finished_at="2026-05-01T00:00:05+00:00", error="RateLimitHit",
        )
        assert ok is True
        (rec,) = b.load_attempts("t::one")
        assert rec.outcome == "requeued"
        assert rec.finished_at == "2026-05-01T00:00:05+00:00"
        assert rec.error == "RateLimitHit"
        # 身份字段保持插入原值（append-only：只写收尾三列）
        assert rec.incarnation == "run_seed.1"
        assert rec.started_at == "2026-05-01T00:00:00+00:00"

    def test_update_unknown_id_returns_false(self):
        b = InMemoryStateBackend()
        assert b.update_attempt(99, outcome="skipped", finished_at="2026-05-01T00:00:05+00:00") is False

    def test_double_finalization_raises(self):
        b = InMemoryStateBackend()
        attempt_id = b.append_attempt(_running_attempt("t::one"))
        assert b.update_attempt(attempt_id, outcome="succeeded", finished_at="2026-05-01T00:00:05+00:00")
        with pytest.raises(ValueError, match="already finalized"):
            b.update_attempt(attempt_id, outcome="failed", finished_at="2026-05-01T00:00:09+00:00")

    def test_update_rejects_running_and_bad_shapes(self):
        b = InMemoryStateBackend()
        attempt_id = b.append_attempt(_running_attempt("t::one"))
        with pytest.raises(ValueError, match="outcome must be one of"):
            b.update_attempt(attempt_id, outcome="running", finished_at="2026-05-01T00:00:05+00:00")
        with pytest.raises(TypeError, match="finished_at must be a str"):
            b.update_attempt(attempt_id, outcome="succeeded", finished_at=None)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="error must be a non-empty str or None"):
            b.update_attempt(attempt_id, outcome="succeeded", finished_at="2026-05-01T00:00:05+00:00", error="")
        # 门卫失败后轨迹行仍是 running
        (rec,) = b.load_attempts("t::one")
        assert rec.outcome == ATTEMPT_RUNNING

    def test_load_attempts_filters_by_uid_in_id_order(self):
        b = InMemoryStateBackend()
        b.append_attempt(_running_attempt("t::one"))
        b.append_attempt(_running_attempt("t::other"))
        b.append_attempt(_running_attempt("t::one", attempt_no=2))
        records = b.load_attempts("t::one")
        assert [r.attempt_no for r in records] == [1, 2]

    def test_attempts_untouched_by_terminal_commits(self):
        """旁路观测面：终态提交不触碰 attempts 行。"""
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_dict("one")])
        attempt_id = b.append_attempt(_running_attempt("t::one"))
        b.update_attempt(attempt_id, outcome="failed", finished_at="2026-05-01T00:00:05+00:00", error="boom")
        b.commit_job_failure("t::one", {"error": "boom"}, job_payload={"k": 1})

        (rec,) = b.load_attempts("t::one")
        assert rec.outcome == "failed"
        assert rec.error == "boom"


class TestMemoryFailureArchive:
    """失败档案 API：带外登记、payload 快照、定向清除、互删语义。"""

    def test_append_failed_overwrites_same_uid(self):
        b = InMemoryStateBackend()
        b.append_failed("t::dup", {"first": 1})
        b.append_failed("t::dup", {"second": 2})
        assert b.load_failed() == {"t::dup": {"second": 2}}

    def test_append_failed_none_payload_stores_empty(self):
        b = InMemoryStateBackend()
        b.append_failed("t::nil", None)
        assert b.load_failed() == {"t::nil": {}}

    def test_payload_snapshot_roundtrip_and_latest_wins(self):
        b = InMemoryStateBackend()
        assert b.commit_job_failure("t::one", {"error": "first"}, job_payload={"v": 1}) is True
        assert b.load_failed_payloads() == {"t::one": {"v": 1}}
        assert b.commit_job_failure("t::one", {"error": "second"}, job_payload={"v": 2}) is True
        assert b.load_failed_payloads() == {"t::one": {"v": 2}}

    def test_none_payload_keeps_previous_snapshot(self):
        """无 payload 的写入路径（带外登记/覆盖写）不得抹掉既有快照。"""
        b = InMemoryStateBackend()
        assert b.commit_job_failure("t::one", {"error": "first"}, job_payload={"k": 1}) is True
        b.append_failed("t::one", {"error": "second"})
        assert b.load_failed_payloads() == {"t::one": {"k": 1}}

    def test_no_snapshot_entry_absent_from_payload_view(self):
        b = InMemoryStateBackend()
        b.append_failed("t::other", {"error": "x"})
        assert b.load_failed_payloads() == {}

    def test_delete_failed_clears_meta_and_snapshot(self):
        b = InMemoryStateBackend()
        b.commit_job_failure("t::one", {"error": "x"}, job_payload={"k": 1})
        b.append_failed("t::two", {"error": "y"})
        assert b.delete_failed(["t::one", "t::ghost"]) == 1
        assert b.load_failed() == {"t::two": {"error": "y"}}
        assert b.load_failed_payloads() == {}

    def test_delete_wall_targeted(self):
        b = InMemoryStateBackend()
        b.commit_job_success("t::one", {"ok": True})
        b.commit_job_success("t::two", {"ok": True})
        assert b.delete_wall(["t::one", "t::ghost"]) == 1
        assert set(b.load_wall()) == {"t::two"}

    def test_success_commit_deletes_failed_same_uid(self):
        """最终状态唯一：成功 commit 删失败档案同名行（含快照）。"""
        b = InMemoryStateBackend()
        b.commit_job_failure("t::one", {"error": "stale"}, job_payload={"k": 1})
        assert b.commit_job_success("t::one", {"ok": True}) is True
        assert b.load_failed() == {}
        assert b.load_failed_payloads() == {}

    def test_failure_commit_deletes_wall_same_uid(self):
        """最终状态唯一：失败 commit 删 wall 同名行。"""
        b = InMemoryStateBackend()
        b.commit_job_success("t::one", {"ok": True})
        assert b.commit_job_failure("t::one", {"error": "boom"}) is True
        assert b.load_wall() == {}
        assert set(b.load_failed()) == {"t::one"}


class TestMemorySeedAndCursor:
    """种子化：wall/failed 互斥拒绝 + cursor 入口校验。"""

    def test_seed_wall_writes_empty_meta_idempotently(self):
        b = InMemoryStateBackend()
        assert b.seed_wall(["t::fresh_one", "t::fresh_two"]) == 2
        assert b.load_wall() == {"t::fresh_one": {}, "t::fresh_two": {}}
        assert b.seed_wall(["t::fresh_one"]) == 1
        assert b.load_wall()["t::fresh_one"] == {}

    def test_seed_wall_rejects_uid_already_in_failed_atomically(self):
        b = InMemoryStateBackend()
        b.append_failed("t::bad", {"error": "boom"})
        with pytest.raises(ValueError, match="already in failed"):
            b.seed_wall(["t::ok_entry", "t::bad", "t::ok_next"])
        # 冲突整体拒绝：零写入，失败档案记录原样保留
        assert b.load_wall() == {}
        assert set(b.load_failed()) == {"t::bad"}

    def test_seed_wall_after_delete_failed_succeeds(self):
        b = InMemoryStateBackend()
        b.append_failed("t::one", {"error": "boom"})
        assert b.delete_failed(["t::one"]) == 1
        assert b.seed_wall(["t::one"]) == 1
        assert b.load_wall() == {"t::one": {}}

    @pytest.mark.parametrize("key,value", [(None, "v"), ("k", 123), ("", "v")])
    def test_seed_cursor_rejects_bad_types(self, key, value):
        b = InMemoryStateBackend()
        with pytest.raises(TypeError):
            b.seed_cursor(key, value)
        assert b.load_cursors() == {}

    def test_seed_cursor_valid_roundtrip_upsert(self):
        b = InMemoryStateBackend()
        b.seed_cursor("k", "v_one")
        b.seed_cursor("k", "v_two")
        assert b.load_cursors() == {"k": "v_two"}


class TestMemorySnapshotIsolation:
    """快照隔离：load 返回深拷贝，写路径入参深拷贝。"""

    def test_load_queue_result_mutation_is_invisible(self):
        b = InMemoryStateBackend()
        b.enqueue_jobs([_job_dict("one", payload={"x": 1})])
        snapshot = b.load_queue()
        snapshot[0]["payload"]["x"] = 999
        snapshot.pop()
        assert b.load_queue()[0]["payload"] == {"x": 1}

    def test_load_wall_and_failed_results_mutation_is_invisible(self):
        b = InMemoryStateBackend()
        b.commit_job_success("t::one", {"meta": {"v": 1}})
        b.append_failed("t::two", {"meta": {"v": 2}})
        b.load_wall()["t::one"]["meta"]["v"] = 999
        b.load_failed()["t::two"]["meta"]["v"] = 999
        assert b.load_wall()["t::one"]["meta"] == {"v": 1}
        assert b.load_failed()["t::two"]["meta"] == {"v": 2}

    def test_enqueue_input_mutation_is_invisible(self):
        b = InMemoryStateBackend()
        job = _job_dict("one", payload={"x": 1})
        b.enqueue_jobs([job])
        job["payload"]["x"] = 999
        assert b.load_queue()[0]["payload"] == {"x": 1}
