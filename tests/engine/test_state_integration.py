"""状态与执行结算集成回归测试套件。

验证 ADR-0011 核心验收契约：
1. running 提交前/后、结果事务中/后的崩溃窗口有确定性覆盖；恢复不重复生成记录。
2. 父成功与派生/游标共同接受，非法子项不允许父成功；失败/retry 丢弃暂存效果。
3. 每次尝试含零预算重试推进 attempt_no，跨 Run 不重用；手工状态不制造 Attempt。
4. 启动时不变量冲突 fail-loud（队列 running 与终态档案并存报 RuntimeError）。
"""
from __future__ import annotations

from pathlib import Path
import pytest

from tasklite import Job, TaskLite, TaskResult
from tasklite.backend.sqlite_backend import SQLiteStateBackend, TARGET_SCHEMA_VERSION


def test_startup_invariant_conflict_raises_runtime_error(tmp_path: Path):
    """启动时不变量冲突：队列中存在 running 且同时已在终态档案中，立即抛 RuntimeError。"""
    db_path = tmp_path / "conflict_state.db"
    backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
    backend.enqueue_jobs([{"task_type": "t", "job_id": "1"}])
    backend.mark_job_running("t::1", run_id="run_old", started_at="2026-01-01T00:00:00+00:00")
    # 人工注入冲突：将 t::1 同时写入 successes
    backend.commit_job_settlement(
        job_uid="t::1",
        outcome="success",
        attempt_no=1,
        run_id="run_old",
        settled_at="2026-01-01T00:00:01+00:00",
        success_metadata={"score": 100},
    )
    # 强制在 queue 中重新插入 running 行（制造 queue running 与 successes 同存的不变量破坏）
    with backend._get_conn() as conn:
        conn.execute(
            "INSERT INTO queue (job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at) "
            "VALUES ('t::1', 0, '{\"task_type\":\"t\",\"job_id\":\"1\"}', 'running', 'run_old', 1, '2026-01-01T00:00:00+00:00')"
        )

    pipeline = TaskLite(backend="sqlite", state_path=db_path)
    with pytest.raises(RuntimeError, match="Invariant conflict"):
        pipeline.run()


def test_leftover_running_recovery_and_idempotence(tmp_path: Path):
    """崩溃窗口：running 提交后未完成结算，下次启动单次收敛为 failed，幂等不重复记行。"""
    db_path = tmp_path / "recovery_state.db"
    backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
    backend.enqueue_jobs([{"task_type": "work", "job_id": "orphan"}])
    att_no = backend.mark_job_running("work::orphan", run_id="run_crash", started_at="2026-01-01T00:00:00+00:00")
    assert att_no == 1

    pipeline = TaskLite(backend="sqlite", state_path=db_path)
    # 启动时恢复
    pipeline.run()

    failed = pipeline.backend.load_failed()
    assert "work::orphan" in failed
    assert failed["work::orphan"]["error_message"] == "上次执行未完成结算，实际结果未知"
    assert failed["work::orphan"]["run_id"] == "run_crash"

    attempts = pipeline.backend.load_attempts("work::orphan")
    assert len(attempts) == 1
    assert attempts[0].outcome == "failed"
    assert attempts[0].attempt_no == 1
    assert attempts[0].run_id == "run_crash"
    assert pipeline.backend.load_queue() == []

    # 再次启动，幂等不产生重复 attempts
    pipeline2 = TaskLite(backend="sqlite", state_path=db_path)
    pipeline2.run()
    attempts2 = pipeline2.backend.load_attempts("work::orphan")
    assert len(attempts2) == 1


def parent_spawn_and_cursor_handler(job, ctx):
    ctx.set_cursor("offset", "100")
    ctx.spawn(Job("child", "c1", payload={"data": 1}))
    return TaskResult.success(metadata={"result": "parent_ok"})


def child_consumer_handler(job, ctx):
    return TaskResult.success(metadata={"data": job.payload.get("data")})


def test_parent_success_atomic_with_spawned_and_cursor(tmp_path: Path):
    """父作业成功与动态派生及游标同事务提交；子作业入队为 pending，游标已落盘。"""
    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "spawn_cursor.db")
    pipeline.register_task("parent", parent_spawn_and_cursor_handler)
    pipeline.register_task("child", child_consumer_handler)
    pipeline.enqueue(Job("parent", "p1"))
    pipeline.run()

    assert "parent::p1" in pipeline.backend.load_successes()
    assert "child::c1" in pipeline.backend.load_successes()
    assert pipeline.backend.load_cursors().get("offset") == "100"


class BadPayload:
    pass


def bad_parent_handler(job, ctx):
    ctx.set_cursor("offset", "999")
    ctx.spawn(Job("child", "bad", payload={"bad": BadPayload()}))
    return TaskResult.success(metadata={"ok": 1})


def test_invalid_spawned_job_rejects_parent_and_discards_effects(tmp_path: Path):
    """非法派生子作业导致父作业失败，所有暂存派生任务和游标更新整体丢弃。"""
    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "bad_spawn.db")
    pipeline.register_task("parent", bad_parent_handler)
    pipeline.enqueue(Job("parent", "p1"))
    pipeline.run()

    assert "parent::p1" not in pipeline.backend.load_successes()
    assert "parent::p1" in pipeline.backend.load_failed()
    assert "not JSON-serializable" in pipeline.backend.load_failed()["parent::p1"]["error_message"]
    assert "offset" not in pipeline.backend.load_cursors()
    assert pipeline.backend.load_queue() == []


def file_counter_retry_handler(job, ctx):
    counter_path = Path(job.payload["counter_file"])
    count = int(counter_path.read_text()) if counter_path.exists() else 0
    count += 1
    counter_path.write_text(str(count))
    if count == 1:
        # 零预算重试
        return TaskResult.retry(message="try again", consume_budget=False)
    return TaskResult.success(metadata={"runs": count})


def test_attempt_no_monotonic_advance_including_zero_budget_retry(tmp_path: Path):
    """attempt_no 单调递增：每次尝试（含零预算重试）推进编号，跨 Run 不重用。"""
    counter_file = tmp_path / "runs.txt"
    counter_file.write_text("0")

    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "monotonic.db")
    pipeline.register_task("worker", file_counter_retry_handler)
    pipeline.enqueue(Job("worker", "m1", payload={"counter_file": str(counter_file)}))
    pipeline.run()

    attempts = pipeline.backend.load_attempts("worker::m1")
    assert len(attempts) == 2
    assert [a.attempt_no for a in attempts] == [1, 2]
    assert [a.outcome for a in attempts] == ["requeued", "succeeded"]
