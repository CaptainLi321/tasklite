"""全链路端到端集成验收套件。

覆盖核心跨功能场景与验收清单：
1. 停止与重试生命周期：受控停止、在途任务安全收敛与重启后重试成功；
2. 授权等待与失败级联：授权等待可中断，失败父任务级联阻断下游拓扑；
3. 清空历史与单调编号：清理历史档案不重置计数器，二次执行延续单调递增编号；
4. 派生原子性与坏子项隔离：成功派生原子入队，失败或异常派生零部分发布；
5. 快照并发隔离观察：写事务提交不干扰已打开只读快照的一致性视图。
"""
from __future__ import annotations

from pathlib import Path
import threading
import time

from tasklite import (
    FileSpec,
    Job,
    StateSnapshot,
    Task,
    TaskLite,
    TaskResult,
)
from tasklite.testing import authorizer, in_process_workers


def _succeed_handler(job, ctx):
    return TaskResult.success(metadata={"echo": job.payload})


def _retry_then_succeed_handler(job, ctx):
    assert "attempt_no" not in job.to_dict()
    if job.attempt_no < 2:
        return TaskResult.retry(message="transient failure, requesting retry")
    return TaskResult.success(metadata={"final_attempt": job.attempt_no})


def _barrier_slow_handler(job, ctx):
    ready = Path(job.payload["ready"])
    release = Path(job.payload["release"])
    ready.write_text("ready", encoding="utf-8")
    deadline = time.monotonic() + 10.0
    while not release.exists() and time.monotonic() < deadline:
        time.sleep(0.005)
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
    raise ValueError("unexpected abort during spawn")


def _auth_required_handler(job, ctx):
    ctx.declare_file(
        "protected_out",
        FileSpec("auth_needed.txt", role="output", delete_existing=True),
    )
    return TaskResult.success()


def test_stop_and_retry_lifecycle(tmp_path):
    """受控停止、在途任务安全收敛与重启后重试成功。"""
    db_path = tmp_path / "stop_retry.db"
    ready = tmp_path / "slow.ready"
    release = tmp_path / "slow.release"
    app = TaskLite(backend="sqlite", state_path=db_path)
    app.register_task(Task("slow", _barrier_slow_handler))
    app.register_task(Task("flaky", _retry_then_succeed_handler, max_retries_per_run=3))

    app.enqueue(Job("slow", "s1", payload={"ready": str(ready), "release": str(release)}))
    app.enqueue(Job("flaky", "f1"))

    def _run():
        with in_process_workers(app):
            app.run(install_signals=False)

    run_thread = threading.Thread(target=_run)
    run_thread.start()
    try:
        deadline = time.monotonic() + 10.0
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.005)
        assert ready.exists()
        app.stop()
        release.write_text("go", encoding="utf-8")
        run_thread.join(timeout=10.0)
        assert not run_thread.is_alive()
    finally:
        release.touch(exist_ok=True)
        app.stop()
        run_thread.join(timeout=5.0)

    assert not app.is_running
    with in_process_workers(app):
        app.run(install_signals=False)

    with StateSnapshot.open(db_path) as snap:
        flaky_job = snap.get_job("flaky::f1")
        assert flaky_job is not None and flaky_job["status"] == "succeeded"
        assert flaky_job["result"]["metadata"]["final_attempt"] == 2
        slow_job = snap.get_job("slow::s1")
        assert slow_job is not None and slow_job["status"] == "succeeded"
    app.close()


def test_authorization_waiting_and_failure_cascade(tmp_path, monkeypatch):
    """授权等待安全可中断，失败父任务级联阻断所有下游依赖。"""
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "auth_cascade.db"
    app = TaskLite(backend="sqlite", state_path=db_path, max_workers=2)

    def _authorizer(path, target_type):
        if "auth_needed.txt" in str(path):
            return None
        return True

    app.register_task(Task("auth_task", _auth_required_handler))
    app.register_task(Task("fail_parent", _fail_handler))
    app.register_task(Task("child", _downstream_handler))
    app.register_task(Task("grandchild", _downstream_handler))

    app.enqueue(Job("fail_parent", "p1"))
    app.enqueue(Job("child", "c1", depends_on=["fail_parent::p1"]))
    app.enqueue(Job("grandchild", "gc1", depends_on=["child::c1"]))
    app.enqueue(Job("auth_task", "a1"))
    (tmp_path / "auth_needed.txt").write_text("existing", encoding="utf-8")

    def _stop_monitor():
        for _ in range(100):
            time.sleep(0.02)
            with StateSnapshot.open(db_path) as snap:
                p_job = snap.get_job("fail_parent::p1")
                if p_job and p_job["status"] == "failed":
                    app.stop()
                    break

    monitor_th = threading.Thread(target=_stop_monitor, daemon=True)
    monitor_th.start()

    # auth_task 等待声明回复期间将被强制终止，使用真实子进程
    with authorizer(app, _authorizer):
        app.run(install_signals=False)
    monitor_th.join(timeout=3.0)

    with StateSnapshot.open(db_path) as snap:
        assert snap.get_job("fail_parent::p1")["status"] == "failed"
        child_job = snap.get_job("child::c1")
        assert child_job["status"] == "failed"
        assert "Dependency failed or has no record" in (child_job["result"]["message"] or "")
        assert snap.get_job("grandchild::gc1")["status"] == "failed"
        assert snap.get_job("auth_task::a1")["status"] == "pending"
    app.close()


def test_clear_history_and_monotonic_sequence(tmp_path):
    """清理历史档案不重置计数器，二次执行延续单调递增编号。"""
    db_path = tmp_path / "seq.db"
    app = TaskLite(backend="sqlite", state_path=db_path)
    app.register_task(Task("work", _succeed_handler))

    app.enqueue(Job("work", "item1", payload={"n": 1}))
    with in_process_workers(app):
        app.run(install_signals=False)

    with StateSnapshot.open(db_path) as snap:
        assert snap.get_job("work::item1")["status"] == "succeeded"
        attempts = list(snap.iter_attempts(job_uid="work::item1"))
        assert len(attempts) == 1
        first_attempt_no = attempts[0]["attempt_no"]
        assert first_attempt_no == 1

    assert app.clear_history("work::item1", records=["successes", "attempts"]) >= 1
    with StateSnapshot.open(db_path) as snap:
        assert snap.get_job("work::item1") is None
        assert list(snap.iter_attempts(job_uid="work::item1")) == []

    app.enqueue(Job("work", "item1", payload={"n": 2}))
    with in_process_workers(app):
        app.run(install_signals=False)

    with StateSnapshot.open(db_path) as snap:
        assert snap.get_job("work::item1")["status"] == "succeeded"
        new_attempts = list(snap.iter_attempts(job_uid="work::item1"))
        assert len(new_attempts) == 1
        assert new_attempts[0]["attempt_no"] == 2 > first_attempt_no
    app.close()


def test_spawn_atomicity_and_bad_child_isolation():
    """成功派生原子入队，失败或异常派生零部分发布。"""
    app = TaskLite(backend="memory")
    app.register_task(Task("parent_ok", _atomic_spawn_handler))
    app.register_task(Task("parent_bad", _bad_spawn_handler))
    app.register_task(Task("leaf", _succeed_handler))

    app.enqueue(Job("parent_ok", "ok_1"))
    with in_process_workers(app):
        app.run(install_signals=False)

    with StateSnapshot.open(app) as snap:
        assert snap.get_job("parent_ok::ok_1")["status"] == "succeeded"
        assert snap.get_job("leaf::c1")["status"] == "succeeded"
        assert snap.get_job("leaf::c2")["status"] == "succeeded"

    app.enqueue(Job("parent_bad", "bad_1"))
    with in_process_workers(app):
        app.run(install_signals=False)

    with StateSnapshot.open(app) as snap:
        assert snap.get_job("parent_bad::bad_1")["status"] == "failed"
        assert snap.get_job("leaf::bad_c1") is None
    app.close()


def test_concurrent_snapshot_observation(tmp_path):
    """写事务提交不干扰已打开只读快照的一致性视图。"""
    db_path = tmp_path / "snap_iso.db"
    app = TaskLite(backend="sqlite", state_path=db_path)
    app.register_task(Task("work", _succeed_handler))
    app.enqueue(Job("work", "j1", payload={"step": 1}))

    with StateSnapshot.open(db_path) as snap:
        initial_job = snap.get_job("work::j1")
        assert initial_job is not None and initial_job["status"] == "pending"

        app.enqueue(Job("work", "j2", payload={"step": 2}))
        with in_process_workers(app):
            app.run(install_signals=False)

        view_j1 = snap.get_job("work::j1")
        assert view_j1 is not None and view_j1["status"] == "pending"
        assert snap.get_job("work::j2") is None
        snap_jobs = list(snap.iter_jobs())
        assert [j["job_uid"] for j in snap_jobs] == ["work::j1"]

    with StateSnapshot.open(db_path) as snap_new:
        assert snap_new.get_job("work::j1")["status"] == "succeeded"
        assert snap_new.get_job("work::j2")["status"] == "succeeded"
        assert len(list(snap_new.iter_jobs())) == 2
    app.close()
