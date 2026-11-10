"""全链路端到端集成验收套件。

覆盖 ADR-0011 定稿核心跨功能场景与验收清单：
1. 停止与重试生命周期：受控停止、在途任务安全收敛与重启后重试成功；
2. 授权等待与失败级联：授权等待可中断，失败父任务级联阻断下游拓扑；
3. 清空历史与单调编号：清理历史档案不重置计数器，二次执行延续单调递增编号；
4. 派生原子性与坏子项隔离：成功派生原子入队，失败或异常派生零部分发布；
5. 快照并发隔离观察：写事务提交不干扰已打开只读快照的一致性视图；
6. 格式准入与旧库拒绝证据：Schema 6 严格准入，旧版本或异形库 fail-loud 拒绝且原样保留。
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
import pytest

from tasklite import (
    FileSpec,
    Job,
    StateSnapshot,
    Task,
    TaskLite,
    TaskLiteError,
    TaskResult,
)
from tasklite.backend.sqlite_backend import SQLiteStateBackend, TARGET_SCHEMA_VERSION


# ── 模块级 Handler ────────────────────────────────────────────────────


def _succeed_handler(job, ctx):
    return TaskResult.success(metadata={"echo": job.payload})


def _retry_then_succeed_handler(job, ctx):
    if job.attempt_no < 2:
        return TaskResult.retry(message="transient failure, requesting retry")
    return TaskResult.success(metadata={"final_attempt": job.attempt_no})


def _slow_cancellable_handler(job, ctx):
    for _ in range(50):
        time.sleep(0.02)
    return TaskResult.success()


def _fail_handler(job, ctx):
    return TaskResult.failure(message="unrecoverable failure")


def _downstream_handler(job, ctx):
    return TaskResult.success(metadata={"downstream": True})


def _atomic_spawn_handler(job, ctx):
    ctx.spawn(Job("leaf", "c1", payload={"parent": job.job_id}))
    ctx.spawn(Job("leaf", "c2", payload={"parent": job.job_id}))
    return TaskResult.success(metadata={"spawned": 2})


def _bad_spawn_handler(job, ctx):
    ctx.spawn(Job("leaf", "bad_c1", payload={"parent": job.job_id}))
    # 模拟在派生过程中抛出异常
    raise ValueError("unexpected abort during spawn")


def _auth_required_handler(job, ctx):
    ctx.declare_file(
        "protected_out",
        FileSpec("auth_needed.txt", role="output", delete_existing=True),
    )
    return TaskResult.success()


# ── 验收场景 1：停止与重试生命周期 ────────────────────────────────────


def test_stop_and_retry_lifecycle(tmp_path):
    """受控停止、在途任务安全收敛与重启后重试成功。"""
    db_path = tmp_path / "stop_retry.db"
    app = TaskLite(backend="sqlite", state_path=db_path)
    app.register_task(Task("slow", _slow_cancellable_handler))
    app.register_task(Task("flaky", _retry_then_succeed_handler, max_retries_per_run=3))

    app.enqueue(Job("flaky", "f1"))
    app.enqueue(Job("slow", "s1"))

    # 在独立线程启动运行
    run_thread = threading.Thread(target=app.run, kwargs={"install_signals": False})
    run_thread.start()

    # 等待至少有任务进入 running 状态后调用 stop
    for _ in range(50):
        time.sleep(0.02)
        if app.is_running:
            break

    app.stop()
    run_thread.join(timeout=5.0)
    assert not run_thread.is_alive()
    assert not app.is_running

    # 再次启动运行，之前未完成或重试的任务能够继续执行并成功
    app.run(install_signals=False)

    with StateSnapshot.open(db_path) as snap:
        flaky_job = snap.get_job("flaky::f1")
        assert flaky_job is not None
        assert flaky_job["status"] == "succeeded"
        assert flaky_job["result"] is not None
        assert flaky_job["result"]["metadata"]["final_attempt"] == 2

        slow_job = snap.get_job("slow::s1")
        assert slow_job is not None
        assert slow_job["status"] == "succeeded"


# ── 验收场景 2：授权等待与失败级联 ────────────────────────────────────


def test_authorization_waiting_and_failure_cascade(tmp_path, monkeypatch):
    """授权等待安全可中断，失败父任务级联阻断所有下游依赖。"""
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "auth_cascade.db"
    app = TaskLite(backend="sqlite", state_path=db_path, max_workers=2)

    # 1. 模拟一个未决授权（返回 None 导致等待）
    def _authorizer(path, target_type):
        if "auth_needed.txt" in str(path):
            return None  # 未决授权
        return True

    app.authorizer = _authorizer
    app.register_task(Task("auth_task", _auth_required_handler))
    app.register_task(Task("fail_parent", _fail_handler))
    app.register_task(Task("child", _downstream_handler))
    app.register_task(Task("grandchild", _downstream_handler))

    # 构造 DAG 依赖链：fail_parent::p1 -> child::c1 -> grandchild::gc1
    app.enqueue(Job("fail_parent", "p1"))
    app.enqueue(Job("child", "c1", depends_on=["fail_parent::p1"]))
    app.enqueue(Job("grandchild", "gc1", depends_on=["child::c1"]))

    # 以及一个等待授权的任务
    app.enqueue(Job("auth_task", "a1"))
    (tmp_path / "auth_needed.txt").write_text("existing", encoding="utf-8")

    def _stop_monitor():
        for _ in range(50):
            time.sleep(0.05)
            # 一旦失败父任务落入 failed，发出停止以解除授权等待
            with StateSnapshot.open(db_path) as snap:
                p_job = snap.get_job("fail_parent::p1")
                if p_job and p_job["status"] == "failed":
                    app.stop()
                    break

    monitor_th = threading.Thread(target=_stop_monitor, daemon=True)
    monitor_th.start()

    app.run(install_signals=False)
    monitor_th.join(timeout=3.0)

    with StateSnapshot.open(db_path) as snap:
        # 1. 父任务失败
        parent_job = snap.get_job("fail_parent::p1")
        assert parent_job is not None
        assert parent_job["status"] == "failed"

        # 2. 下游所有依赖级联标记为失败，未执行
        child_job = snap.get_job("child::c1")
        assert child_job is not None
        assert child_job["status"] == "failed"
        assert child_job["result"] is not None
        assert "JOB_DEPENDENCY" in (child_job["result"]["message"] or "")

        gc_job = snap.get_job("grandchild::gc1")
        assert gc_job is not None
        assert gc_job["status"] == "failed"

        # 3. 授权等待的任务随 stop 恢复为 pending，不写失败档案
        auth_job = snap.get_job("auth_task::a1")
        assert auth_job is not None
        assert auth_job["status"] == "pending"


# ── 验收场景 3：清空历史与单调编号 ────────────────────────────────────


def test_clear_history_and_monotonic_sequence(tmp_path):
    """清理历史档案不重置计数器，二次执行延续单调递增编号。"""
    db_path = tmp_path / "seq.db"
    app = TaskLite(backend="sqlite", state_path=db_path)
    app.register_task(Task("work", _succeed_handler))

    app.enqueue(Job("work", "item1", payload={"n": 1}))
    app.run(install_signals=False)

    with StateSnapshot.open(db_path) as snap:
        job = snap.get_job("work::item1")
        assert job is not None
        assert job["status"] == "succeeded"
        attempts = list(snap.iter_attempts(job_uid="work::item1"))
        assert len(attempts) == 1
        first_attempt_no = attempts[0]["attempt_no"]
        assert first_attempt_no == 1

    # 清空历史记录（包含 successes 和 attempts 轨迹）
    deleted = app.clear_history("work::item1", records=["successes", "attempts"])
    assert deleted >= 1

    with StateSnapshot.open(db_path) as snap:
        assert snap.get_job("work::item1") is None
        assert list(snap.iter_attempts(job_uid="work::item1")) == []

    # 重新入队同一作业并执行
    app.enqueue(Job("work", "item1", payload={"n": 2}))
    app.run(install_signals=False)

    with StateSnapshot.open(db_path) as snap:
        job = snap.get_job("work::item1")
        assert job is not None
        assert job["status"] == "succeeded"
        new_attempts = list(snap.iter_attempts(job_uid="work::item1"))
        assert len(new_attempts) == 1
        # 不变式：编号单调递增，不因历史清理重置为 1
        assert new_attempts[0]["attempt_no"] > first_attempt_no
        assert new_attempts[0]["attempt_no"] == 2


# ── 验收场景 4：派生原子性与坏子项隔离 ────────────────────────────────


def test_spawn_atomicity_and_bad_child_isolation(tmp_path):
    """成功派生原子入队，失败或异常派生零部分发布。"""
    db_path = tmp_path / "spawn.db"
    app = TaskLite(backend="sqlite", state_path=db_path)
    app.register_task(Task("parent_ok", _atomic_spawn_handler))
    app.register_task(Task("parent_bad", _bad_spawn_handler))
    app.register_task(Task("leaf", _succeed_handler))

    # 1. 成功派生：原子入队并全部成功执行
    app.enqueue(Job("parent_ok", "ok_1"))
    app.run(install_signals=False)

    with StateSnapshot.open(db_path) as snap:
        assert snap.get_job("parent_ok::ok_1")["status"] == "succeeded"
        assert snap.get_job("leaf::c1")["status"] == "succeeded"
        assert snap.get_job("leaf::c2")["status"] == "succeeded"

    # 2. 失败派生：父任务异常，已声明的子任务不发生部分发布
    app.enqueue(Job("parent_bad", "bad_1"))
    app.run(install_signals=False)

    with StateSnapshot.open(db_path) as snap:
        assert snap.get_job("parent_bad::bad_1")["status"] == "failed"
        # bad_c1 绝对未入队
        assert snap.get_job("leaf::bad_c1") is None


# ── 验收场景 5：快照并发隔离观察 ────────────────────────────────────


def test_concurrent_snapshot_observation(tmp_path):
    """写事务提交不干扰已打开只读快照的一致性视图。"""
    db_path = tmp_path / "snap_iso.db"
    app = TaskLite(backend="sqlite", state_path=db_path)
    app.register_task(Task("work", _succeed_handler))

    app.enqueue(Job("work", "j1", payload={"step": 1}))

    # 在外部提交前打开只读快照
    with StateSnapshot.open(db_path) as snap:
        initial_job = snap.get_job("work::j1")
        assert initial_job is not None
        assert initial_job["status"] == "pending"

        # 外部执行并发写提交
        app.enqueue(Job("work", "j2", payload={"step": 2}))
        app.run(install_signals=False)

        # 验证在快照生命周期内，一致性视图被保持：
        # j1 仍然观察为 pending，外部新入队的 j2 不可见
        view_j1 = snap.get_job("work::j1")
        assert view_j1 is not None
        assert view_j1["status"] == "pending"
        assert snap.get_job("work::j2") is None

        snap_jobs = list(snap.iter_jobs())
        assert len(snap_jobs) == 1
        assert snap_jobs[0]["job_uid"] == "work::j1"

    # 关闭并重新打开快照后，观察到最新持久化提交
    with StateSnapshot.open(db_path) as snap_new:
        assert snap_new.get_job("work::j1")["status"] == "succeeded"
        assert snap_new.get_job("work::j2")["status"] == "succeeded"
        assert len(list(snap_new.iter_jobs())) == 2


# ── 验收场景 6：格式准入与旧库拒绝证据 ────────────────────────────────


def test_schema_acceptance_and_rejection_proof(tmp_path):
    """Schema 6 严格准入，旧版本或异形库 fail-loud 拒绝且原样保留。"""
    # 1. 正常 Schema 6 库准入
    valid_db = tmp_path / "valid_v6.db"
    app = TaskLite(backend="sqlite", state_path=valid_db)
    app.register_task(Task("work", _succeed_handler))
    app.enqueue(Job("work", "item"))
    app.run(install_signals=False)
    app.close()

    conn = sqlite3.connect(valid_db)
    v = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.close()
    assert v == TARGET_SCHEMA_VERSION == 6

    # 2. 旧 Schema 5 库拒绝并保留文件内容
    old_db = tmp_path / "old_v5.db"
    conn = sqlite3.connect(old_db)
    conn.execute("PRAGMA user_version = 5")
    conn.execute("CREATE TABLE dummy (id INT)")
    conn.commit()
    conn.close()

    mtime_before = old_db.stat().st_mtime_ns

    with pytest.raises(TaskLiteError, match="Unsupported database schema version 5"):
        SQLiteStateBackend(old_db)

    # 确认原文件未被覆盖或修改
    assert old_db.stat().st_mtime_ns == mtime_before

    # 3. 非空 Schema 0 库拒绝
    non_empty_v0 = tmp_path / "legacy_v0.db"
    conn = sqlite3.connect(non_empty_v0)
    conn.execute("CREATE TABLE legacy_data (data TEXT)")
    conn.commit()
    conn.close()

    with pytest.raises(TaskLiteError, match="version 0 非空库"):
        SQLiteStateBackend(non_empty_v0)
