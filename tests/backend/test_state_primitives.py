"""状态事务与编号准备测试（双后端对齐、故障原子性、编号持久性与旧库拒绝保护）。

契约验证：
- 事务故障不留下部分子任务/游标/Attempt；SQLite 与内存相同操作得到等价事实。
- 清空档案和 Attempt 不删除编号进度，进度不作为 Job 或依赖存在证据。
- 使用合成旧格式副本验证检查不改写内容；没有任何真实业务库路径。
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from tasklite.backend import (
    AbstractStateBackend,
    InMemoryStateBackend,
    SQLiteStateBackend,
    TARGET_SCHEMA_VERSION,
)
from tasklite.models.job import Job
from tasklite.models.state import job_uid_from_snapshot


def _make_job(job_id: str, **overrides) -> dict[str, Any]:
    data = Job("t", job_id).to_dict()
    data.update(overrides)
    return data


def _state_facts(backend: AbstractStateBackend) -> dict[str, Any]:
    """提取后端全部可观测事实，用于双后端等价性核对。"""
    return {
        "queue": backend.load_queue_jobs(),
        "successes": backend.load_successes(),
        "failed": backend.load_failed(),
        "failure_payloads": backend.load_failure_payloads(),
        "cursors": backend.load_cursors(),
    }


def _sqlite_snapshot(path):
    with sqlite3.connect(path) as conn:
        return (
            conn.execute("PRAGMA user_version").fetchone(),
            conn.execute("PRAGMA journal_mode").fetchone(),
            list(conn.iterdump()),
        )


@pytest.fixture(params=["memory", "sqlite"])
def v6_backend(request, tmp_path) -> AbstractStateBackend:
    if request.param == "memory":
        return InMemoryStateBackend(schema_version=TARGET_SCHEMA_VERSION)
    db_path = tmp_path / "v6_state.db"
    return SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)


class TestStatePrimitivesParity:
    """双后端事务原子性与等价性测试。"""

    def test_atomic_settlement_failure_leaves_no_partial_state(self, v6_backend):
        b = v6_backend
        # 初始入队
        parent_job = _make_job("parent")
        b.enqueue_jobs([parent_job])
        b.mark_job_running("t::parent", run_id="run_1", started_at="2026-11-08T00:00:00Z")

        before = _state_facts(b)
        assert len(before["queue"]) == 1
        assert before["queue"][0]["status"] == "running"

        # 1. 游标值类型非法，应整体失败并回滚
        with pytest.raises(TypeError):
            b.commit_job_settlement(
                job_uid="t::parent",
                outcome="success",
                attempt_no=1,
                run_id="run_1",
                settled_at="2026-11-08T00:00:01Z",
                success_metadata={"ok": True},
                spawned_job_snapshots=[_make_job("child_1")],
                cursor_updates={"bad_cursor": 12345},  # type: ignore[dict-item]
            )

        # 状态完全未被触碰
        assert _state_facts(b) == before
        assert b.load_settled_attempts("t::parent") == []

        # 2. 派生任务 UID 冲突导致事务提交失败
        # 先入队一个 conflict_child
        b.enqueue_jobs([_make_job("conflict_child")])
        before2 = _state_facts(b)

        # 尝试派生同名 child
        committed = b.commit_job_settlement(
            job_uid="t::parent",
            outcome="success",
            attempt_no=1,
            run_id="run_1",
            settled_at="2026-11-08T00:00:02Z",
            success_metadata={"ok": True},
            spawned_job_snapshots=[_make_job("conflict_child")],
            cursor_updates={"good_cursor": "val"},
        )
        assert committed is False
        # 失败后没有任何部分子任务、游标或 Attempt 写入
        assert _state_facts(b) == before2
        assert b.load_settled_attempts("t::parent") == []

    def test_settlement_outcomes_parity(self, v6_backend):
        b = v6_backend

        # --- 场景 1: success ---
        b.enqueue_jobs([_make_job("job_s")])
        att_no = b.mark_job_running("t::job_s", run_id="run_s", started_at="2026-11-08T00:00:00Z")
        assert att_no == 1

        ok = b.commit_job_settlement(
            job_uid="t::job_s",
            outcome="success",
            attempt_no=1,
            run_id="run_s",
            started_at="2026-11-08T00:00:00Z",
            settled_at="2026-11-08T00:00:01Z",
            job_snapshot=_make_job("job_s"),
            result={"message": "done", "metadata": {"x": 1}},
            success_metadata={"x": 1},
            spawned_job_snapshots=[_make_job("child_s")],
            cursor_updates={"cur_1": "cval"},
        )
        assert ok is True

        # 校验成功状态与副作用
        assert "t::job_s" in b.load_successes()
        assert "t::job_s" not in [j["job_uid"] for j in b.load_queue_jobs()]
        assert b.load_cursors() == {"cur_1": "cval"}
        queue_jobs = b.load_queue_jobs()
        assert len(queue_jobs) == 1
        assert queue_jobs[0]["job_uid"] == "t::child_s"
        assert queue_jobs[0]["status"] == "pending"

        attempts = b.load_settled_attempts("t::job_s")
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "success"
        assert attempts[0]["attempt_no"] == 1
        assert attempts[0]["run_id"] == "run_s"

        # --- 场景 2: failure ---
        b.enqueue_jobs([_make_job("job_f")])
        b.mark_job_running("t::job_f", run_id="run_f", started_at="2026-11-08T01:00:00Z")

        ok = b.commit_job_settlement(
            job_uid="t::job_f",
            outcome="failure",
            attempt_no=1,
            run_id="run_f",
            settled_at="2026-11-08T01:00:01Z",
            failure_metadata={"error": "oom"},
            payload={"input": "data"},
        )
        assert ok is True
        assert "t::job_f" in b.load_failed()
        assert b.load_failure_payloads().get("t::job_f") == {"input": "data"}
        assert "t::job_f" not in [j["job_uid"] for j in b.load_queue_jobs()]
        f_attempts = b.load_settled_attempts("t::job_f")
        assert len(f_attempts) == 1
        assert f_attempts[0]["outcome"] == "failure"

        # --- 场景 3: retry (留队重试) ---
        b.enqueue_jobs([_make_job("job_r")])
        b.mark_job_running("t::job_r", run_id="run_r", started_at="2026-11-08T02:00:00Z")

        ok = b.commit_job_settlement(
            job_uid="t::job_r",
            outcome="retry",
            attempt_no=1,
            run_id="run_r",
            settled_at="2026-11-08T02:00:01Z",
        )
        assert ok is True
        assert "t::job_r" not in b.load_successes()
        assert "t::job_r" not in b.load_failed()
        r_jobs = [j for j in b.load_queue_jobs() if j["job_uid"] == "t::job_r"]
        assert len(r_jobs) == 1
        assert r_jobs[0]["status"] == "pending"
        assert r_jobs[0]["run_id"] is None
        r_attempts = b.load_settled_attempts("t::job_r")
        assert len(r_attempts) == 1
        assert r_attempts[0]["outcome"] == "retry"

        # --- 场景 4: pending (授权拒绝/中断) ---
        b.enqueue_jobs([_make_job("job_p")])
        b.mark_job_running("t::job_p", run_id="run_p", started_at="2026-11-08T03:00:00Z")

        ok = b.commit_job_settlement(
            job_uid="t::job_p",
            outcome="pending",
            attempt_no=1,
            run_id="run_p",
            settled_at="2026-11-08T03:00:01Z",
            error_message="authorization denied",
        )
        assert ok is True
        assert "t::job_p" not in b.load_successes()
        assert "t::job_p" not in b.load_failed()
        p_jobs = [j for j in b.load_queue_jobs() if j["job_uid"] == "t::job_p"]
        assert len(p_jobs) == 1
        assert p_jobs[0]["status"] == "pending"
        p_attempts = b.load_settled_attempts("t::job_p")
        assert len(p_attempts) == 1
        assert p_attempts[0]["outcome"] == "failure"
        assert p_attempts[0]["error_message"] == "authorization denied"


class TestAttemptSequenceIndependence:
    """编号进度独立于可清除历史。"""

    def test_cleaning_history_and_attempts_preserves_attempt_sequence(self, v6_backend):
        b = v6_backend
        job_uid = "t::target_seq"

        # 1. 首次执行进入 running
        b.enqueue_jobs([_make_job("target_seq")])
        seq1 = b.mark_job_running(job_uid, run_id="run_1", started_at="2026-11-08T00:00:00Z")
        assert seq1 == 1
        assert b.get_attempt_sequence(job_uid) == 1

        # 结算为成功
        b.commit_job_settlement(
            job_uid=job_uid,
            outcome="success",
            attempt_no=1,
            run_id="run_1",
            settled_at="2026-11-08T00:00:01Z",
            success_metadata={"status": "ok"},
        )
        assert len(b.load_settled_attempts(job_uid)) == 1
        assert job_uid in b.load_successes()

        # 2. 清除档案与轨迹
        deleted_succ = b.delete_successes([job_uid])
        assert deleted_succ == 1
        cleared_att = b.clear_attempts(job_uid)
        assert cleared_att == 1

        # 确认档案与轨迹完全清空
        assert job_uid not in b.load_successes()
        assert b.load_settled_attempts(job_uid) == []
        assert [j["job_uid"] for j in b.load_queue_jobs()] == []

        # 关键断言：编号进度未被带走
        assert b.get_attempt_sequence(job_uid) == 1

        # 编号记录不得伪装成队列任务或成功档案
        assert b.load_queue() == []
        assert b.load_successes() == {}
        assert b.load_failed() == {}

        # 3. 重新入队并再次进入 running
        b.enqueue_jobs([_make_job("target_seq")])
        seq2 = b.mark_job_running(job_uid, run_id="run_2", started_at="2026-11-08T01:00:00Z")
        # 关键断言：编号单调递增为 2，未重置为 1
        assert seq2 == 2
        assert b.get_attempt_sequence(job_uid) == 2


class TestDatabaseRejectionAndPreservation:
    """旧格式拒绝与无害性保护。"""

    @pytest.mark.parametrize("old_version", [0, 1, 2, 3, 4, 5])
    def test_target_format_rejects_and_preserves_synthetic_old_formats(self, tmp_path, old_version):
        path = tmp_path / f"synthetic_v{old_version}.db"
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE legacy_data (col TEXT)")
            conn.execute("INSERT INTO legacy_data VALUES ('do_not_modify')")
            conn.execute(f"PRAGMA user_version = {old_version}")

        before = _sqlite_snapshot(path)

        with pytest.raises(RuntimeError) as caught:
            SQLiteStateBackend(path, schema_version=TARGET_SCHEMA_VERSION)

        # 拒绝信息明确包含目标 schema 6
        assert "6" in str(caught.value)
        assert "保留原库" in str(caught.value)

        # 数据库内容与 user_version 严格未变
        assert _sqlite_snapshot(path) == before

    def test_legacy_executor_rejects_and_preserves_target_format(self, tmp_path):
        path = tmp_path / "v6_db.db"
        backend = SQLiteStateBackend(path, schema_version=TARGET_SCHEMA_VERSION)
        backend.enqueue_jobs([_make_job("j1")])
        backend.mark_job_running("t::j1", run_id="r1", started_at="2026-11-08T00:00:00Z")
        backend.commit_job_settlement(
            job_uid="t::j1",
            outcome="success",
            attempt_no=1,
            run_id="r1",
            settled_at="2026-11-08T00:00:01Z",
            success_metadata={"result": "preserved"},
        )

        before = _sqlite_snapshot(path)

        # 使用旧默认执行器（schema_version 5）尝试打开
        with pytest.raises(RuntimeError) as caught:
            SQLiteStateBackend(path)  # 默认 schema_version=5

        assert "5" in str(caught.value)
        assert "Unsupported database schema version 6" in str(caught.value)

        # 新库未被旧执行器改写或破坏
        assert _sqlite_snapshot(path) == before
