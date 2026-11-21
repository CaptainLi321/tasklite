"""文件操作授权查询、撤销、自动同意与等待生命周期验收用例集。

验收场景：
1. 一个 worker 等待授权时暂停新派发，另一 worker 仍能完成；stop 解除等待。
2. 拒绝与停止均先结束受阻 worker，再原子登记失败 Attempt 并回到 pending。
3. 仅拒绝触发本 Run 依赖链排除，SQLite 重开后重新判断并复用既有授权。
4. 动态目标先经硬性路径校验，自动批准不能绕过输出范围或受保护路径。
5. list_authorizations / revoke_authorization 限 Run 外调用，规范路径精确撤销。
"""

import os
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from tasklite import (
    FileSpec,
    Job,
    StateSnapshot,
    TaskLite,
    TaskLiteError,
    TaskResult,
)
from tasklite.resource import CapacityResource
from tasklite.testing import authorizer as inject_authorizer
from tasklite.testing import in_process_workers, running


def _quick_handler(job, ctx):
    return TaskResult.success(metadata={"quick": True})


def _dir_handler(job, ctx):
    path = Path(ctx.declare_file(
        "sub_file",
        FileSpec("managed_dir/sub/item.txt", role="output", delete_existing=True),
    ))
    path.write_text("updated", encoding="utf-8")
    return TaskResult.success()


def _cleanup_fail_handler(job, ctx):
    path = Path(ctx.declare_file(
        "clean_target",
        FileSpec("clean_me.txt", role="output", delete_on_success=True),
    ))
    path.write_text("content", encoding="utf-8")
    return TaskResult.success()


def _protected_path_handler(job, ctx):
    ctx.declare_file(
        "bad_out",
        FileSpec("t_auto_approve.db", role="output", delete_existing=True),
    )
    return TaskResult.success()


def _good_handler(job, ctx):
    path = Path(ctx.declare_file(
        "out1",
        FileSpec("my_out.txt", role="output", delete_existing=True),
    ))
    path.write_text("updated", encoding="utf-8")
    return TaskResult.success()


class _MutableAuthorizer:
    def __init__(self, allowed: bool = False):
        self.allowed = allowed

    def __call__(self, path: str, target_type: str) -> bool:
        return self.allowed


class _ControlledAuthorizer:
    def __init__(self, decision: bool | None = None):
        self.decision = decision
        self.calls = 0
        self.first_call = threading.Event()

    def __call__(self, path: str, target_type: str) -> bool | None:
        self.calls += 1
        self.first_call.set()
        return self.decision


class _BlockingAuthorizer:
    def __init__(self) -> None:
        self.decision: bool | None = None
        self.calls = 0
        self.first_call = threading.Event()
        self.release = threading.Event()

    def __call__(self, path: str, target_type: str) -> bool | None:
        self.calls += 1
        self.first_call.set()
        assert self.release.wait(10.0)
        return self.decision


def _wait_for(predicate, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition did not become true before timeout")


def _jobs_by_status(pipeline: TaskLite, status: str) -> set[str]:
    with StateSnapshot.open(pipeline) as snap:
        return {row["job_uid"] for row in snap.iter_jobs(status=status)}


def _failed_record(pipeline: TaskLite, uid: str) -> dict | None:
    with StateSnapshot.open(pipeline) as snap:
        rec = snap.get_job(uid)
    if rec is not None and rec["status"] == "failed":
        return rec
    return None


def _attempts_for(pipeline: TaskLite, uid: str) -> list[dict]:
    with StateSnapshot.open(pipeline) as snap:
        return list(snap.iter_attempts(job_uid=uid))


def _declare_then_write_handler(job, ctx):
    path = Path(ctx.declare_file(
        "dynamic",
        FileSpec(job.payload["path"], role="output", delete_existing=True),
    ))
    path.write_text(job.payload["content"], encoding="utf-8")
    return TaskResult.success()


def _marker_handler(job, ctx):
    Path(job.payload["marker"]).write_text("started", encoding="utf-8")
    return TaskResult.success()


def _barrier_then_declare_handler(job, ctx):
    Path(job.payload["entered"]).write_text("entered", encoding="utf-8")
    trigger = Path(job.payload["trigger"])
    deadline = time.monotonic() + 10.0
    while not trigger.exists() and time.monotonic() < deadline:
        time.sleep(0.005)
    if not trigger.exists():
        raise RuntimeError("declaration trigger timed out")
    path = Path(ctx.declare_file(
        "dynamic",
        FileSpec(job.payload["path"], role="output", delete_existing=True),
    ))
    path.write_text(job.payload["content"], encoding="utf-8")
    return TaskResult.success()


def _wait_for_gate_then_quick_handler(job, ctx):
    gate = Path(job.payload["gate"])
    deadline = time.monotonic() + 10.0
    while not gate.exists() and time.monotonic() < deadline:
        time.sleep(0.005)
    if not gate.exists():
        raise RuntimeError("quick handler gate timed out")
    return TaskResult.success(metadata={"quick": True})


def test_worker_declaration_wakes_main_loop_wait(tmp_path, monkeypatch, caplog):
    """主循环等待在途事件时，worker 动态声明必须立即推进授权与日志。"""
    monkeypatch.chdir(tmp_path)
    caplog.set_level("INFO", logger="tasklite")
    target = tmp_path / "waiting-declare.txt"
    target.write_text("old", encoding="utf-8")
    entered = tmp_path / "worker-entered"
    trigger = tmp_path / "worker-trigger"

    p = TaskLite(backend="memory", max_workers=1)
    auth = _ControlledAuthorizer(decision=True)
    p.register_task("declared", _barrier_then_declare_handler)
    p.register_task("follow", _quick_handler)
    p.enqueue(Job(
        "declared",
        "d1",
        payload={
            "entered": str(entered),
            "trigger": str(trigger),
            "path": str(target),
            "content": "new",
        },
    ))
    p.enqueue(Job("follow", "f1"))

    wait_observed = threading.Event()
    wait_timeouts: list[float | None] = []
    original_wait = p._runtime._wait

    def tracked_wait(timeout, last_seq=None):
        wait_timeouts.append(timeout)
        wait_observed.set()
        return original_wait(timeout, last_seq=last_seq)

    monkeypatch.setattr(p._runtime, "_wait", tracked_wait)
    errors: list[BaseException] = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    with in_process_workers(p), inject_authorizer(p, auth):
        runner = threading.Thread(target=run_pipeline)
        runner.start()
        try:
            _wait_for(entered.exists)
            assert wait_observed.wait(10.0), "主循环应已在等待在途完成"
            trigger.write_text("go", encoding="utf-8")
            assert auth.first_call.wait(10.0)
            runner.join(timeout=15.0)
            assert not runner.is_alive()
            assert errors == []
        finally:
            trigger.touch(exist_ok=True)
            p.stop()
            runner.join(timeout=5.0)

    assert any(timeout is None or timeout > 5.0 for timeout in wait_timeouts)
    assert target.read_text(encoding="utf-8") == "new"
    assert {"declared::d1", "follow::f1"} <= _jobs_by_status(p, "succeeded")
    assert any("FILE DECLARATION:" in record.message for record in caplog.records)
    assert any("FILE AUTHORIZATION APPROVED:" in record.message for record in caplog.records)
    p.close()


def test_worker_wait_authorization_pauses_dispatch_while_other_worker_settles(
    tmp_path, monkeypatch,
):
    """双 worker 下，授权等待暂停新派发，但不阻塞另一在途 worker 结算。"""
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "pending_auth.txt"
    target.write_text("old", encoding="utf-8")
    third_marker = tmp_path / "third-started"
    quick_gate = tmp_path / "quick-gate"

    p = TaskLite(backend="memory", max_workers=2)
    auth = _ControlledAuthorizer(decision=None)
    p.register_task("slow_decl", _declare_then_write_handler)
    p.register_task("quick", _wait_for_gate_then_quick_handler)
    p.register_task("third", _marker_handler)

    p.enqueue(Job(
        "slow_decl",
        "s1",
        payload={"path": str(target), "content": "updated"},
    ))
    p.enqueue(Job("quick", "q1", payload={"gate": str(quick_gate)}))
    p.enqueue(Job("third", "t1", payload={"marker": str(third_marker)}))

    gate_observed = threading.Event()
    third_spawned = threading.Event()
    original_fill = p._runtime._fill_dispatch_pool
    original_spawn = p._runtime.channel.spawn

    def tracked_fill(limit, draining):
        result = original_fill(limit, draining)
        if any(handle.waiting_auth for handle in p._runtime.in_flight.active_handles()):
            gate_observed.set()
        return result

    def tracked_spawn(spec):
        if spec.job.uid == "third::t1":
            third_spawned.set()
        return original_spawn(spec)

    monkeypatch.setattr(p._runtime, "_fill_dispatch_pool", tracked_fill)
    monkeypatch.setattr(p._runtime.channel, "spawn", tracked_spawn)
    errors = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    with in_process_workers(p), inject_authorizer(p, auth):
        runner = threading.Thread(target=run_pipeline)
        runner.start()
        try:
            assert auth.first_call.wait(10.0)
            quick_gate.write_text("go", encoding="utf-8")
            _wait_for(lambda: "quick::q1" in _jobs_by_status(p, "succeeded"))
            assert "slow_decl::s1" not in _jobs_by_status(p, "succeeded")
            assert not third_marker.exists()
            assert any(handle.waiting_auth for handle in p._runtime.in_flight.active_handles())
            gate_observed.clear()
            assert gate_observed.wait(10.0)
            assert not third_spawned.is_set()

            auth.decision = True
            runner.join(timeout=20.0)
            assert not runner.is_alive()
            assert errors == []
        finally:
            p.stop()
            runner.join(timeout=5.0)

    assert _jobs_by_status(p, "succeeded") == {"slow_decl::s1", "quick::q1", "third::t1"}
    assert target.read_text(encoding="utf-8") == "updated"
    assert third_marker.exists()
    p.close()


def test_authorization_wait_does_not_consume_execution_timeout(tmp_path, monkeypatch):
    """等待越过原执行截止后批准，恢复剩余时限并让 worker 正常完成。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "paused-timeout.txt"
    target.write_text("old", encoding="utf-8")

    p = TaskLite(backend="memory")
    auth = _BlockingAuthorizer()
    deadline_passed = False
    reap_after_deadline = threading.Event()
    original_reap = p._runtime.channel.reap_completed

    def tracked_reap(handles):
        result = original_reap(handles)
        if deadline_passed:
            reap_after_deadline.set()
        return result

    monkeypatch.setattr(p._runtime.channel, "reap_completed", tracked_reap)
    p.register_task("work", _declare_then_write_handler, timeout_seconds=30)
    p.enqueue(Job("work", "one", payload={"path": str(target), "content": "updated"}))

    errors = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    with in_process_workers(p), inject_authorizer(p, auth):
        runner = threading.Thread(target=run_pipeline)
        runner.start()
        try:
            assert auth.first_call.wait(10.0)
            _wait_for(lambda: any(h.waiting_auth for h in p._runtime.in_flight.active_handles()))
            handle = next(h for h in p._runtime.in_flight.active_handles() if h.waiting_auth)
            assert handle.paused_remaining is not None
            handle.timeout_at_monotonic = time.monotonic() - 1.0
            deadline_passed = True
            auth.release.set()
            assert reap_after_deadline.wait(10.0)

            auth.decision = True
            runner.join(timeout=20.0)
            assert not runner.is_alive()
            assert errors == []
        finally:
            auth.release.set()
            p.stop()
            runner.join(timeout=5.0)

    assert "work::one" in _jobs_by_status(p, "succeeded")
    assert target.read_text(encoding="utf-8") == "updated"
    p.close()


def test_directory_authorization_reusable_and_subpath_revocation(tmp_path, monkeypatch):
    """目录授权在 SQLite 重开后可复用，撤销子路径不产生父目录例外。"""
    monkeypatch.chdir(tmp_path)
    p = TaskLite(backend="sqlite", state_path=tmp_path / "t_dir_auth.db")

    dir_path = (tmp_path / "managed_dir").resolve()
    sub_file = (dir_path / "sub" / "item.txt").resolve()
    sub_file.parent.mkdir(parents=True, exist_ok=True)
    sub_file.write_text("sub content", encoding="utf-8")

    p.register_task("dir_task", _dir_handler)
    p.enqueue(Job("dir_task", "d1"))

    with in_process_workers(p), inject_authorizer(p, None) as store:
        p.run(install_signals=False)
        assert "dir_task::d1" not in _jobs_by_status(p, "succeeded")
        store.record_authorization(dir_path, "directory")
    assert any(a["path"] == str(dir_path) and a["target_type"] == "directory" for a in p.list_authorizations())
    p.close()

    reopened = TaskLite(backend="sqlite", state_path=tmp_path / "t_dir_auth.db")
    reopened.register_task("dir_task", _dir_handler)
    assert reopened.revoke_authorization(sub_file) is False
    with in_process_workers(reopened), inject_authorizer(reopened, None) as store:
        assert store.is_path_authorized(sub_file) is True
        reopened.run(install_signals=False)
    assert "dir_task::d1" in _jobs_by_status(reopened, "succeeded")

    assert reopened.revoke_authorization(dir_path) is True
    with inject_authorizer(reopened, None) as store:
        assert store.is_path_authorized(sub_file) is False
    reopened.close()


def test_cleanup_failure_vs_authorization_denial_destinations(tmp_path, monkeypatch):
    """实际清理错误记为 failed 档案，而不回退 pending。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    p = TaskLite(backend="memory")

    p.register_task("clean_task", _cleanup_fail_handler)
    p.enqueue(Job("clean_task", "c1"))

    with in_process_workers(p), patch(
        "tasklite.utils.artifact.ArtifactPathPolicy.cleanup",
        side_effect=OSError("Read-only file system"),
    ):
        p.run(install_signals=False)

    rec = _failed_record(p, "clean_task::c1")
    assert rec is not None
    assert "Success cleanup failed" in (rec["result"]["message"] or "")
    assert "clean_task::c1" not in _jobs_by_status(p, "succeeded")
    p.close()


@pytest.mark.parametrize("deferred", [False, True], ids=["immediate", "after-wait"])
def test_authorization_denial_settles_attempt_and_keeps_job_pending(
    tmp_path, monkeypatch, deferred,
):
    """即时与等待后拒绝都先结束 worker，再原子登记失败 Attempt 并回 pending；SQLite 重开可继续。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    state_path = tmp_path / "denial.db"
    target = tmp_path / "target.txt"
    target.write_text("old data", encoding="utf-8")

    p = TaskLite(backend="sqlite", state_path=state_path, max_workers=2)
    auth = _ControlledAuthorizer(decision=False if not deferred else None)
    p.register_task("target_task", _declare_then_write_handler)
    p.register_task("middle", _quick_handler)
    p.register_task("leaf", _quick_handler)
    p.register_task("independent", _quick_handler)

    target_job = Job(
        "target_task", "t1",
        payload={"path": str(target), "content": "new data"},
        trigger="always",
    )
    middle_job = Job("middle", "m1", depends_on=["target_task::t1"], trigger="always")
    leaf_job = Job("leaf", "l1", depends_on=["middle::m1"], trigger="always")
    with p.edit(dry_run=False) as tx:
        for historical_job in (target_job, middle_job, leaf_job):
            tx.add(historical_job, status="succeeded")
    p.enqueue(target_job)
    p.enqueue(middle_job)
    p.enqueue(leaf_job)
    p.enqueue(Job("independent", "i1"))

    errors = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    with in_process_workers(p), inject_authorizer(p, auth):
        runner = threading.Thread(target=run_pipeline)
        runner.start()
        try:
            assert auth.first_call.wait(10.0)
            if deferred:
                auth.decision = False
            runner.join(timeout=20.0)
            assert not runner.is_alive()
            assert errors == []
        finally:
            p.stop()
            runner.join(timeout=5.0)

    assert target.read_text(encoding="utf-8") == "old data"
    assert "independent::i1" in _jobs_by_status(p, "succeeded")
    assert _jobs_by_status(p, "failed") == set()
    assert {"target_task::t1", "middle::m1", "leaf::l1"} <= _jobs_by_status(p, "pending")
    assert {"target_task::t1", "middle::m1", "leaf::l1"} <= p._runtime.session.temporary_excluded_uids
    assert _attempts_for(p, "middle::m1") == []
    assert _attempts_for(p, "leaf::l1") == []
    attempt, = _attempts_for(p, "target_task::t1")
    assert attempt["outcome"] == "failure"
    assert "authorization denied for" in (attempt["result"]["message"] or "")
    assert attempt["result"]["annotations"]["authorization_status"] == "denied"
    assert p._runtime.session.retry_counts.get("target_task::t1", 0) == 0
    p.close()

    reopened = TaskLite(backend="sqlite", state_path=state_path, max_workers=2)
    reopened.register_task("target_task", _declare_then_write_handler)
    reopened.register_task("middle", _quick_handler)
    reopened.register_task("leaf", _quick_handler)
    with in_process_workers(reopened), inject_authorizer(reopened, _MutableAuthorizer(allowed=True)):
        reopened.run(install_signals=False)

    assert {"target_task::t1", "middle::m1", "leaf::l1"} <= _jobs_by_status(reopened, "succeeded")
    assert [r["outcome"] for r in _attempts_for(reopened, "target_task::t1")] == ["failure", "success"]
    assert target.read_text(encoding="utf-8") == "new data"
    reopened.close()


def test_stop_during_authorization_wait_records_stopped_reason_without_exclusion(
    tmp_path, monkeypatch,
):
    """stop 解除授权等待，记录停止原因，关闭全 Run 派发而不排除单个 Job。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "stopped.txt"
    target.write_text("old data", encoding="utf-8")
    stopped_marker = tmp_path / "stopped-marker"
    quick_gate = tmp_path / "quick-gate"

    from tasklite.engine.resource import CapacityResource as InternalCapacityResource

    p = TaskLite(backend="memory", max_workers=2)
    release_observations: list[tuple[set[str], list[dict]]] = []

    class ObservingCapacity(InternalCapacityResource):
        def release(self, amount: float) -> None:
            release_observations.append((
                _jobs_by_status(p, "running"),
                _attempts_for(p, "waiting::w1"),
            ))
            super().release(amount)

    p._resources["slot"] = ObservingCapacity("slot", max_capacity=1.0)
    auth = _BlockingAuthorizer()
    p.register_task("waiting", _declare_then_write_handler)
    p.register_task("quick", _wait_for_gate_then_quick_handler)
    p.register_task("after_stop", _marker_handler)
    p.enqueue(Job(
        "waiting", "w1",
        payload={"path": str(target), "content": "new data"},
        resources={"slot": 1.0},
    ))
    p.enqueue(Job("quick", "q1", payload={"gate": str(quick_gate)}))
    p.enqueue(Job("after_stop", "a1", payload={"marker": str(stopped_marker)}))

    errors = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    with inject_authorizer(p, auth):
        runner = threading.Thread(target=run_pipeline)
        runner.start()
        try:
            assert auth.first_call.wait(10.0)
            quick_gate.write_text("go", encoding="utf-8")
            _wait_for(lambda: any(h.waiting_auth for h in p._runtime.in_flight.active_handles()))
            p.stop()
            assert "waiting::w1" in _jobs_by_status(p, "running")
            auth.release.set()
            runner.join(timeout=20.0)
            assert not runner.is_alive()
            assert errors == []
        finally:
            auth.release.set()
            p.stop()
            runner.join(timeout=5.0)

        assert target.read_text(encoding="utf-8") == "old data"
        assert _jobs_by_status(p, "failed") == set()
        attempt, = _attempts_for(p, "waiting::w1")
        assert attempt["outcome"] == "failure"
        assert "authorization wait stopped for" in (attempt["result"]["message"] or "")
        assert attempt["result"]["annotations"]["authorization_status"] == "stopped"
        assert "waiting::w1" not in p._runtime.session.temporary_excluded_uids
        assert p._runtime.session.retry_counts.get("waiting::w1", 0) == 0
        assert p._runtime.in_flight.active_handles() == []
        assert any("waiting::w1" in running_uids and not attempts for running_uids, attempts in release_observations)
        assert not stopped_marker.exists()
        assert {"waiting::w1", "after_stop::a1"} <= _jobs_by_status(p, "pending")

        auth.decision = True
        p.run(install_signals=False)
    assert {"waiting::w1", "after_stop::a1"} <= _jobs_by_status(p, "succeeded")
    assert stopped_marker.exists()
    assert target.read_text(encoding="utf-8") == "new data"
    p.close()


def test_changed_dynamic_target_cannot_use_prior_wait_to_bypass_protection(
    tmp_path, monkeypatch,
):
    """等待期间将普通目标替换为受保护文件的链接时，后续决策仍先校验实际目标。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = allowed / "target.txt"
    target.write_text("old", encoding="utf-8")
    state_path = tmp_path / "changed-target.db"

    p = TaskLite(backend="sqlite", state_path=state_path)
    auth = _ControlledAuthorizer(decision=None)
    p.register_task("work", _declare_then_write_handler)
    p.enqueue(Job("work", "one", payload={"path": str(target), "content": "overwrite"}))

    errors = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    with inject_authorizer(p, auth):
        runner = threading.Thread(target=run_pipeline)
        runner.start()
        try:
            assert auth.first_call.wait(10.0)
            target.unlink()
            target.symlink_to(state_path)
            auth.decision = True
            runner.join(timeout=20.0)
            assert not runner.is_alive()
            assert errors == []
        finally:
            p.stop()
            runner.join(timeout=5.0)

    assert target.is_symlink()
    assert target.resolve() == state_path.resolve()
    assert p.list_authorizations() == []
    assert _jobs_by_status(p, "succeeded") == set()
    failure = _failed_record(p, "work::one")
    assert failure is not None and "symlink" in (failure["result"]["message"] or "")
    attempt, = _attempts_for(p, "work::one")
    assert attempt["outcome"] == "failure"
    assert "work::one" not in p._runtime.session.temporary_excluded_uids
    p.close()


def test_auto_approve_persists_actual_path_and_forbids_protected_roots(tmp_path, monkeypatch):
    """TASKLITE_DANGEROUS_AUTO_APPROVE=1 只自动同意实际请求路径，持久记录，保护路径仍禁止。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    state_path = tmp_path / "t_auto_approve.db"
    p = TaskLite(backend="sqlite", state_path=state_path)

    p.register_task("good_task", _good_handler)
    p.register_task("bad_task", _protected_path_handler)

    p.enqueue(Job("good_task", "g1"))
    (tmp_path / "my_out.txt").write_text("old", encoding="utf-8")
    with in_process_workers(p):
        p.run(install_signals=False)

    assert "good_task::g1" in _jobs_by_status(p, "succeeded")
    auths = p.list_authorizations()
    expected_path = str((tmp_path / "my_out.txt").resolve())
    assert any(a["path"] == expected_path and a["target_type"] == "file" for a in auths)
    unrelated_path = str((tmp_path / "unrelated.txt").resolve())
    assert not any(a["path"] == unrelated_path for a in auths)
    p.close()

    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    reopened = TaskLite(backend="sqlite", state_path=state_path)
    reopened.register_task("good_task", _good_handler)
    reopened.register_task("bad_task", _protected_path_handler)
    assert any(
        a["path"] == expected_path and a["target_type"] == "file"
        for a in reopened.list_authorizations()
    )
    reopened.enqueue(Job("good_task", "g2"))
    with in_process_workers(reopened):
        reopened.run(install_signals=False)
    assert "good_task::g2" in _jobs_by_status(reopened, "succeeded")

    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    reopened.enqueue(Job("bad_task", "b1"))
    # bad_task 动态声明受保护路径会被强制终止，需真实子进程
    reopened.run(install_signals=False)
    assert "bad_task::b1" not in _jobs_by_status(reopened, "succeeded")
    with inject_authorizer(reopened, None) as store:
        assert not store.is_path_authorized(state_path)
    reopened.close()


def test_list_and_revoke_authorizations_surface_and_run_guard(tmp_path, monkeypatch):
    """list_authorizations 与 revoke_authorization 规范格式、精确匹配与 Run 守卫。"""
    monkeypatch.chdir(tmp_path)
    p = TaskLite(backend="memory")

    f1 = tmp_path / "doc.txt"
    f1.write_text("data", encoding="utf-8")

    assert p.list_authorizations() == []

    with inject_authorizer(p, None) as store:
        store.record_authorization(f1, "file")
    records = p.list_authorizations()
    assert len(records) == 1
    rec = records[0]
    assert rec["path"] == str(f1.resolve())
    assert rec["target_type"] == "file"
    assert isinstance(rec["authorized_at"], str)

    assert p.revoke_authorization(f1) is True
    assert p.revoke_authorization(f1) is False
    assert p.list_authorizations() == []

    with running(p):
        with pytest.raises(TaskLiteError, match="is only allowed outside run"):
            p.list_authorizations()
        with pytest.raises(TaskLiteError, match="is only allowed outside run"):
            p.revoke_authorization("foo.txt")
    p.close()


_FORBIDDEN_CASES = [
    ("static", "interactive", "delete_existing", "protected"),
    ("static", "interactive", "delete_on_success", "filesystem-root"),
    ("static", "interactive", "delete_on_failure", "symlink"),
    ("static", "automatic", "delete_on_success", "protected"),
    ("static", "automatic", "delete_on_failure", "filesystem-root"),
    ("static", "automatic", "delete_existing", "symlink"),
    ("dynamic", "interactive", "delete_on_failure", "protected"),
    ("dynamic", "interactive", "delete_existing", "filesystem-root"),
    ("dynamic", "interactive", "delete_on_success", "symlink"),
    ("dynamic", "automatic", "delete_existing", "protected"),
    ("dynamic", "automatic", "delete_on_success", "filesystem-root"),
    ("dynamic", "automatic", "delete_on_failure", "symlink"),
    ("static", "automatic", "delete_on_success", "symlink"),
    ("dynamic", "automatic", "delete_existing", "symlink"),
]


def _declare_spec_handler(job, ctx):
    ctx.declare_file("target", FileSpec.from_dict(job.payload["spec"]))
    return TaskResult.success()


@pytest.mark.parametrize("declaration,approval,operation,forbidden", _FORBIDDEN_CASES)
def test_forbidden_file_operations_never_request_or_persist_authorization(
    tmp_path, monkeypatch, declaration, approval, operation, forbidden,
):
    """禁止目标在静态与动态入口均先拒绝，不询问、不保存授权、不进入删除。"""
    monkeypatch.chdir(tmp_path)
    if approval == "automatic":
        monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    else:
        monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    state_path = allowed / "state.db"
    valuable = tmp_path / "valuable"
    valuable.write_text("valuable", encoding="utf-8")
    targets = {
        "protected": state_path,
        "filesystem-root": Path(allowed.anchor),
        "symlink": allowed / "linked",
    }
    targets["symlink"].symlink_to(valuable)
    target = targets[forbidden]
    calls = []
    removals = []
    # protected 用例显式用 sqlite 以校验受保护 state_path，其余用 memory
    backend = "sqlite" if forbidden == "protected" else "memory"
    with TaskLite(backend=backend, state_path=state_path if backend == "sqlite" else None) as p:
        if forbidden == "protected":
            # 确保 memory/sqlite 均覆盖到
            assert p._state_path == state_path
        monkeypatch.setattr(
            "tasklite.utils.artifact.ArtifactPathPolicy._remove",
            lambda *args, **kwargs: removals.append(args),
        )
        spec = FileSpec(target, role="output", **{operation: True})
        p.register_task("work", _declare_spec_handler if declaration == "dynamic" else _quick_handler)
        p.enqueue(Job(
            "work", "one", payload={"spec": spec.to_dict()},
            files={"target": spec} if declaration == "static" else {},
        ))
        # dynamic 入口会触发主进程强制终止阻塞在 wait_reply 的 worker，需真实子进程
        if declaration == "dynamic":
            with inject_authorizer(p, lambda *args: calls.append(args) or True):
                p.run(install_signals=False)
        else:
            with in_process_workers(p), inject_authorizer(p, lambda *args: calls.append(args) or True):
                p.run(install_signals=False)
        assert calls == []
        assert p.list_authorizations() == []
        assert removals == []
        assert valuable.read_text(encoding="utf-8") == "valuable"
        assert target.exists()
        assert "work::one" in _jobs_by_status(p, "failed")
        assert len(_attempts_for(p, "work::one")) == (1 if declaration == "dynamic" else 0)


def test_static_forbidden_declaration_prevents_authorizing_other_targets(tmp_path, monkeypatch):
    """同批预声明全部校验完成后才开始询问，禁止目标不会留下部分授权。"""
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    good = allowed / "good"
    valuable = tmp_path / "valuable"
    bad = allowed / "bad"
    good.write_text("good", encoding="utf-8")
    valuable.write_text("valuable", encoding="utf-8")
    bad.symlink_to(valuable)
    with TaskLite(backend="memory") as p:
        calls = []
        p.register_task("work", _quick_handler)
        p.enqueue(Job("work", "one", files={
            "good": FileSpec(good, role="output", delete_existing=True),
            "bad": FileSpec(bad, role="output", delete_existing=True),
        }))
        with in_process_workers(p), inject_authorizer(p, lambda *args: calls.append(args) or True):
            p.run(install_signals=False)
        assert calls == []
        assert p.list_authorizations() == []
        assert good.read_text(encoding="utf-8") == "good"
        assert bad.is_symlink()
        assert valuable.read_text(encoding="utf-8") == "valuable"


@pytest.mark.parametrize("changed", ["waiting", "other-declaration"])
def test_static_authorization_wait_revalidates_target_before_asking_again(tmp_path, monkeypatch, changed):
    """待决后目标变为链接，下一次询问前拒绝且保留目标。"""
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    valuable = tmp_path / "valuable"
    target.write_text("old", encoding="utf-8")
    valuable.write_text("valuable", encoding="utf-8")
    other = tmp_path / "other"
    other.write_text("other", encoding="utf-8")
    changed_target = target if changed == "waiting" else other
    calls = []

    def authorize(*args):
        calls.append(args)
        if len(calls) == 1:
            changed_target.unlink()
            changed_target.symlink_to(valuable)
            return None
        return True

    with TaskLite(backend="memory") as p:
        p.register_task("work", _quick_handler)
        p.enqueue(Job("work", "one", files={
            "target": FileSpec(target, role="output", delete_existing=True),
            "other": FileSpec(other, role="output", delete_on_success=True),
        }))
        with in_process_workers(p), inject_authorizer(p, authorize):
            p.run(install_signals=False)
        assert len(calls) == 1
        assert p.list_authorizations() == []
        assert changed_target.is_symlink()
        assert valuable.read_text(encoding="utf-8") == "valuable"
        failure = _failed_record(p, "work::one")
        assert failure is not None and "symlink" in (failure["result"]["message"] or "")
        assert _attempts_for(p, "work::one") == []


@pytest.mark.parametrize("declaration", ["static", "dynamic"])
def test_broken_symlink_preparation_authorizes_and_removes_only_the_link(
    tmp_path, monkeypatch, declaration,
):
    """前删断链时授权其目录项路径，不授权或创建缺失的链接目标。"""
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    missing = tmp_path / "missing"
    link = tmp_path / "link"
    link.symlink_to(missing)
    spec = FileSpec(link, role="cache", delete_existing=True)
    with TaskLite(backend="memory") as p:
        calls = []
        p.register_task("work", _declare_spec_handler if declaration == "dynamic" else _quick_handler)
        p.enqueue(Job(
            "work", "one", payload={"spec": spec.to_dict()},
            files={"target": spec} if declaration == "static" else {},
        ))
        with in_process_workers(p), inject_authorizer(p, lambda *args: calls.append(args) or True) as store:
            p.run(install_signals=False)
            assert not store.is_path_authorized(missing)
        assert calls == [(str(link), "file")]
        authorization, = p.list_authorizations()
        assert authorization["path"] == str(link)
        assert authorization["target_type"] == "file"
        assert not os.path.lexists(link)
        assert not missing.exists()
        assert p.revoke_authorization(missing) is False
        assert p.revoke_authorization(link) is True
        assert p.list_authorizations() == []
        assert "work::one" in _jobs_by_status(p, "succeeded")


def _fixed_base_file_handler(job, ctx):
    os.chdir(job.payload["worker_cwd"])
    if job.payload["declaration"] == "dynamic":
        target = ctx.declare_file(
            "target", FileSpec("../allowed/result", role="output", delete_existing=True),
        )
    else:
        target = ctx.file_path("target")
    assert not target.exists()
    target.write_text("updated", encoding="utf-8")
    return TaskResult.success()


@pytest.mark.parametrize("declaration", ["static", "dynamic"])
def test_allowed_file_operations_keep_instance_path_base(
    tmp_path, monkeypatch, declaration,
):
    """主进程与真实 spawn worker 改变工作目录后，相对路径仍按实例创建基准解析。"""
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    base = tmp_path / "base"
    allowed = tmp_path / "allowed"
    other = tmp_path / "other"
    for directory in (base, allowed, other):
        directory.mkdir()
    target = allowed / "result"
    target.write_text("old", encoding="utf-8")
    monkeypatch.chdir(base)
    with TaskLite(backend="sqlite", state_path=base / "state.db") as p:
        calls = []
        p.register_task("work", _fixed_base_file_handler)
        monkeypatch.chdir(other)
        p.enqueue(Job(
            "work", "one", payload={"worker_cwd": str(other), "declaration": declaration},
            files={"target": FileSpec("../allowed/result", role="output", delete_existing=True)}
            if declaration == "static" else {},
        ))
        with inject_authorizer(p, lambda *args: calls.append(args) or True):
            p.run(install_signals=False)
        assert calls == [(str(target), "file")]
        authorization, = p.list_authorizations()
        assert authorization["path"] == str(target)
        assert authorization["target_type"] == "file"
        assert target.read_text(encoding="utf-8") == "updated"
        assert "work::one" in _jobs_by_status(p, "succeeded")


def _replace_file_with_tree_handler(job, ctx):
    target = ctx.file_path("target")
    if target.is_file():
        target.unlink()
    target.mkdir(exist_ok=True)
    (target / "child").write_text("business data", encoding="utf-8")
    if job.payload["outcome"] == "failure":
        return TaskResult.failure("business failure")
    if job.payload["outcome"] == "retry":
        return TaskResult.retry(message="business retry")
    ctx.set_cursor("tree-result", "committed")
    return TaskResult.success(metadata={"business": "result"})


_SCOPE_CONFIRM_CASES = [
    *[
        ("success", initial, decision)
        for initial in ("file", "missing")
        for decision in ("approve", "deny", "wait-approve", "wait-deny", "stop")
    ],
    ("success", "directory", "approve"),
    ("failure", "file", "approve"),
    ("failure", "file", "deny"),
    ("failure", "file", "wait-approve"),
    ("failure", "file", "stop"),
    ("retry", "file", "approve"),
    ("retry", "file", "deny"),
    ("retry", "file", "wait-approve"),
    ("retry", "file", "stop"),
]


@pytest.mark.parametrize("outcome,initial,decision", _SCOPE_CONFIRM_CASES)
def test_worker_target_type_change_requires_cleanup_scope_confirmation(
    tmp_path, monkeypatch, initial, outcome, decision,
):
    """worker 改变目标类型后，清理授权等待、拒绝和停止均保持结算语义。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    if initial == "file":
        target.write_text("initial", encoding="utf-8")
    elif initial == "directory":
        target.mkdir()
    p = TaskLite(backend="memory")
    calls = []
    waiting = threading.Event()
    final_decision = [None if decision.startswith("wait") or decision == "stop" else decision == "approve"]

    def authorize(path, kind):
        calls.append((path, kind))
        if len(calls) == 1:
            return True
        waiting.set()
        return final_decision[0]

    p.register_task("replace", _replace_file_with_tree_handler)
    p.register_task("downstream", _quick_handler)
    p.enqueue(Job(
        "replace", "one", payload={"outcome": outcome}, max_retries_per_run=1,
        files={"target": FileSpec(
            target, role="cache", delete_on_success=outcome == "success",
            delete_on_failure=outcome != "success",
        )},
    ))
    p.enqueue(Job("downstream", "one", depends_on=["replace::one"]))
    errors = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    with in_process_workers(p), inject_authorizer(p, authorize):
        runner = threading.Thread(target=run_pipeline)
        runner.start()
        try:
            if initial != "directory" and (decision.startswith("wait") or decision == "stop"):
                assert waiting.wait(10)
                _wait_for(lambda: p._runtime._completion.waiting_authorization)
                assert target.joinpath("child").read_text(encoding="utf-8") == "business data"
                assert _attempts_for(p, "replace::one") == []
                assert "replace::one" in _jobs_by_status(p, "running")
                assert p._runtime.session.retry_counts.get("replace::one", 0) == 0
                if decision == "stop":
                    p.stop()
                else:
                    final_decision[0] = decision == "wait-approve"
            runner.join(timeout=20)
            assert not runner.is_alive()
            assert errors == []
        finally:
            p.stop()
            runner.join(timeout=5)
    accepted = initial == "directory" or decision in ("approve", "wait-approve")
    assert calls[0] == (str(target), "directory" if initial == "directory" else "file")
    assert all(call == (str(target), "directory") for call in calls[1:])
    attempts = _attempts_for(p, "replace::one")
    if accepted:
        assert not target.exists()
        if initial == "directory":
            assert len(calls) == 1
        elif decision == "approve":
            assert len(calls) == 2
        assert len(attempts) == (2 if outcome == "retry" else 1)
        assert p.get_cursor("tree-result") == ("committed" if outcome == "success" else None)
    else:
        assert target.joinpath("child").read_text(encoding="utf-8") == "business data"
        attempt, = attempts
        assert attempt["outcome"] == "failure"
        assert attempt["result"]["annotations"]["authorization_status"] == ("stopped" if decision == "stop" else "denied")
        assert _jobs_by_status(p, "pending") == {"replace::one", "downstream::one"}
        assert _jobs_by_status(p, "failed") == set()
        assert _jobs_by_status(p, "succeeded") == set()
        assert p._runtime.session.retry_counts.get("replace::one", 0) == 0
        assert p.get_cursor("tree-result") is None
        assert p.list_authorizations()[0]["target_type"] == "file"
    p.close()


def _replace_and_linger_handler(job, ctx):
    result = _replace_file_with_tree_handler(job, ctx)

    def linger():
        gate = Path(job.payload["exit_gate"])
        deadline = time.monotonic() + 15
        while not gate.exists() and time.monotonic() < deadline:
            time.sleep(0.005)

    threading.Thread(target=linger, daemon=False).start()
    return result


@pytest.mark.parametrize("decision", [True, False])
def test_cleanup_wait_preserves_result_and_settles_other_worker_before_single_completion(
    tmp_path, monkeypatch, decision,
):
    """清理待决时继续结算在途结果、暂停新派发；只调用一次收尾且拒绝先终止进程。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("initial", encoding="utf-8")
    independent_gate, exit_gate = tmp_path / "independent", tmp_path / "exit"
    p = TaskLite(backend="memory", max_workers=2)
    requested = threading.Event()
    decided = [None]
    calls = []

    def authorize(path, kind):
        calls.append(kind)
        if kind == "file":
            return True
        requested.set()
        return decided[0]

    p.register_task("replace", _replace_and_linger_handler)
    p.register_task("independent", _wait_for_gate_then_quick_handler)
    p.register_task("follow", _marker_handler)
    p.enqueue(Job("replace", "one", payload={"outcome": "success", "exit_gate": str(exit_gate)},
                  files={"target": FileSpec(target, role="cache", delete_on_success=True)}))
    p.enqueue(Job("independent", "one", payload={"gate": str(independent_gate)}))
    marker = tmp_path / "new-dispatch"
    p.enqueue(Job("follow", "one", payload={"marker": str(marker)}))
    complete_calls = []
    original_complete = p._runtime._completion.complete_job
    original_pending = p._runtime.store.apply_pending

    def complete(entry, result, **kwargs):
        complete_calls.append(entry.job_uid)
        return original_complete(entry, result, **kwargs)

    def pending(**kwargs):
        handle = p._runtime.in_flight[kwargs["job_uid"]].handle
        try:
            alive = handle.process.is_alive()
        except ValueError:
            alive = False
        assert not alive
        return original_pending(**kwargs)

    monkeypatch.setattr(p._runtime._completion, "complete_job", complete)
    monkeypatch.setattr(p._runtime.store, "apply_pending", pending)
    errors = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    with inject_authorizer(p, authorize):
        runner = threading.Thread(target=run_pipeline)
        runner.start()
        try:
            assert requested.wait(10)
            _wait_for(lambda: p._runtime._completion.waiting_authorization)
            entry = p._runtime.in_flight["replace::one"]
            assert entry.handle.process.is_alive()
            entry.handle.timeout_at_monotonic = time.monotonic() - 1
            independent_gate.touch()
            _wait_for(lambda: "independent::one" in _jobs_by_status(p, "succeeded"))
            assert not marker.exists()
            assert complete_calls == ["independent::one"]
            assert target.joinpath("child").exists()
            decided[0] = decision
            if decision:
                _wait_for(lambda: "replace::one" in _jobs_by_status(p, "succeeded"))
                assert entry.handle.process.is_alive()
                assert not target.exists()
                exit_gate.touch()
            runner.join(20)
            assert not runner.is_alive()
            assert errors == []
            assert complete_calls.count("replace::one") == 1
            assert len(_attempts_for(p, "replace::one")) == 1
        finally:
            exit_gate.touch()
            independent_gate.touch()
            p.stop()
            runner.join(5)
            p.close()


def _replace_file_with_link_handler(job, ctx):
    target = ctx.file_path("target")
    target.unlink()
    target.symlink_to(job.payload["destination"], target_is_directory=True)
    return TaskResult.success()


def test_worker_leaf_link_replacement_never_expands_file_authorization(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("initial", encoding="utf-8")
    destination = tmp_path / "business"
    destination.mkdir()
    child = destination / "child"
    child.write_text("preserve", encoding="utf-8")
    p = TaskLite(backend="memory")
    calls = []
    p.register_task("replace", _replace_file_with_link_handler)
    p.enqueue(Job("replace", "one", payload={"destination": str(destination)},
                  files={"target": FileSpec(target, role="cache", delete_on_success=True)}))
    with in_process_workers(p), inject_authorizer(p, lambda *args: calls.append(args) or True):
        p.run(install_signals=False)
    assert target.is_symlink()
    assert child.read_text(encoding="utf-8") == "preserve"
    assert calls == [(str(target), "file")]
    assert p.list_authorizations()[0]["target_type"] == "file"
    attempt, = _attempts_for(p, "replace::one")
    assert attempt["outcome"] == "failure"
    assert "symlink" in (attempt["result"]["message"] or "")
    p.close()


def _replace_second_target_handler(job, ctx):
    second = ctx.file_path("second")
    second.unlink()
    second.mkdir()
    (second / "child").write_text("second data", encoding="utf-8")
    return TaskResult.success()


@pytest.mark.parametrize("approve_first_tree", [True, False])
def test_later_authorization_callback_cannot_expand_previously_checked_target(
    tmp_path, monkeypatch, approve_first_tree,
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    first, second = tmp_path / "first", tmp_path / "second"
    for path in (first, second):
        path.write_text("initial", encoding="utf-8")
    p = TaskLite(backend="memory")
    calls = []

    def authorize(path, kind):
        calls.append((path, kind))
        if path == str(second) and kind == "directory":
            first.unlink()
            first.mkdir()
            (first / "child").write_text("first data", encoding="utf-8")
        if path == str(first) and kind == "directory":
            return approve_first_tree
        return True

    p.register_task("replace", _replace_second_target_handler)
    p.enqueue(Job("replace", "one", files={
        "first": FileSpec(first, role="cache", delete_on_success=True),
        "second": FileSpec(second, role="cache", delete_on_success=True),
    }))
    with in_process_workers(p), inject_authorizer(p, authorize):
        p.run(install_signals=False)
    assert calls == [(str(first), "file"), (str(second), "file"),
                     (str(second), "directory"), (str(first), "directory")]
    if approve_first_tree:
        assert not first.exists() and not second.exists()
    else:
        assert first.joinpath("child").read_text(encoding="utf-8") == "first data"
        assert second.joinpath("child").read_text(encoding="utf-8") == "second data"
        assert _jobs_by_status(p, "failed") == set()
        assert "replace::one" in _jobs_by_status(p, "pending")
    p.close()


def _failure_handler(job, ctx):
    return TaskResult.failure("business failure")


@pytest.mark.parametrize("timing", ["delete_existing", "delete_on_success", "delete_on_failure"])
def test_type_change_at_deletion_snapshot_fails_without_directory_permission(
    tmp_path, monkeypatch, timing,
):
    """最后的对象快照发现范围已扩大时明确失败，不递归删除未授权目录。"""
    from tasklite.utils.artifact import ArtifactPathPolicy

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("initial", encoding="utf-8")
    p = TaskLite(backend="memory")
    calls = []
    p.register_task("work", _failure_handler if timing == "delete_on_failure" else _quick_handler)
    p.enqueue(Job("work", "one", files={
        "target": FileSpec(target, role="cache", **{timing: True}),
    }))
    original_remove = ArtifactPathPolicy._remove

    def replace_before_snapshot(policy, path, declaration, **kwargs):
        if path == target:
            target.unlink()
            target.mkdir()
            (target / "child").write_text("preserve", encoding="utf-8")
        return original_remove(policy, path, declaration, **kwargs)

    monkeypatch.setattr(ArtifactPathPolicy, "_remove", replace_before_snapshot)
    with in_process_workers(p), inject_authorizer(p, lambda *args: calls.append(args) or True):
        p.run(install_signals=False)
    assert target.joinpath("child").read_text(encoding="utf-8") == "preserve"
    assert calls == [(str(target), "file")]
    failed = _failed_record(p, "work::one")
    assert failed is not None and "authorization scope changed before deletion" in (failed["result"]["message"] or "")
    p.close()


@pytest.mark.parametrize("cached", [False, True], ids=["channel-result", "cleanup-result"])
@pytest.mark.parametrize("decision", [True, False, None])
def test_run_fault_leaves_uncommitted_cleanup_result_running_for_recovery(
    tmp_path, monkeypatch, cached, decision,
):
    """收到有效结果不等于已提交；异常退出不补结算，下次按原身份恢复失败。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("initial", encoding="utf-8")
    p = TaskLite(backend="memory")
    final_decision = [None if cached else decision]
    calls = []

    def authorize(path, kind):
        calls.append((path, kind))
        return True if kind == "file" else final_decision[0]

    p.register_task("replace", _replace_file_with_tree_handler)
    p.register_task("committed", _quick_handler)
    p.enqueue(Job("committed", "one"))
    with in_process_workers(p):
        p.run(install_signals=False)
    with StateSnapshot.open(p) as snap:
        committed = snap.get_job("committed::one")
    p.enqueue(Job("replace", "one", payload={"outcome": "success"}, files={
        "target": FileSpec(target, role="cache", delete_on_success=True),
    }))
    fault = RuntimeError("framework fault after valid worker result")
    runtime = p._runtime
    if cached:
        original_step = runtime.step

        def faulty_step(*args, **kwargs):
            outcome = original_step(*args, **kwargs)
            if runtime._completion.waiting_authorization:
                final_decision[0] = decision
                raise fault
            return outcome

        monkeypatch.setattr(runtime, "step", faulty_step)
    else:
        original_drain = runtime._drain_and_settle

        def faulty_drain():
            for handle in runtime.in_flight.active_handles():
                runtime.channel._receive_available(handle)
                if handle.execution_id in runtime.channel._results:
                    raise fault
            return original_drain()

        monkeypatch.setattr(runtime, "_drain_and_settle", faulty_drain)
    with in_process_workers(p), inject_authorizer(p, authorize):
        with pytest.raises(RuntimeError) as caught:
            p.run(install_signals=False)
    assert caught.value is fault
    assert calls == [(str(target), "file")] + ([(str(target), "directory")] if cached else [])
    assert target.joinpath("child").read_text(encoding="utf-8") == "business data"
    assert _attempts_for(p, "replace::one") == []
    with StateSnapshot.open(p) as snap:
        running_job = snap.get_job("replace::one")
        assert running_job is not None and running_job["status"] == "running"
        assert running_job["attempt_no"] == 1
        assert snap.get_job("committed::one") == committed
    assert _jobs_by_status(p, "failed") == set()
    assert p.get_cursor("tree-result") is None
    assert not runtime._completion.waiting_authorization
    assert not runtime.in_flight

    monkeypatch.undo()
    with in_process_workers(p):
        p.run(install_signals=False)
    attempt, = _attempts_for(p, "replace::one")
    assert attempt["outcome"] == "failure"
    assert attempt["run_id"] == running_job["run_id"]
    assert attempt["attempt_no"] == running_job["attempt_no"]
    assert _jobs_by_status(p, "running") == set()
    assert "replace::one" in _jobs_by_status(p, "failed")
    assert target.joinpath("child").read_text(encoding="utf-8") == "business data"
    with in_process_workers(p):
        p.run(install_signals=False)
    assert _attempts_for(p, "replace::one") == [attempt]
    p.close()


def test_success_cleanup_error_does_not_run_unapproved_failure_cleanup(tmp_path, monkeypatch):
    from tasklite.utils.artifact import ArtifactPathPolicy

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    first, second = tmp_path / "first", tmp_path / "second"
    for path in (first, second):
        path.write_text("initial", encoding="utf-8")
    p = TaskLite(backend="memory")
    calls = []
    p.register_task("replace", _replace_second_target_handler)
    p.enqueue(Job("replace", "one", files={
        "first": FileSpec(first, role="cache", delete_on_success=True),
        "second": FileSpec(second, role="cache", delete_on_failure=True),
    }))

    def fail_cleanup(policy, declaration, *, success):
        assert declaration.path == str(first)
        assert success
        raise OSError("cleanup write failure")

    monkeypatch.setattr(ArtifactPathPolicy, "cleanup", fail_cleanup)
    with in_process_workers(p), inject_authorizer(p, lambda *args: calls.append(args) or True):
        p.run(install_signals=False)
    assert first.read_text(encoding="utf-8") == "initial"
    assert second.joinpath("child").read_text(encoding="utf-8") == "second data"
    assert calls == [(str(first), "file"), (str(second), "file")]
    assert "replace::one" in _jobs_by_status(p, "failed")
    p.close()


# ── Run 外显式预授权 TaskLite.grant_authorization 验收用例集 ──────────────


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_grant_authorization_enables_non_interactive_run_and_persists(
    tmp_path, monkeypatch, backend,
):
    """grant_authorization 支持不存在的目标、零磁盘副作用，并支撑无交互 Run 清理。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    state_path = tmp_path / "preauth.db"
    kwargs = {"state_path": state_path} if backend == "sqlite" else {}

    work_dir = tmp_path / "future_outputs"
    single_file = tmp_path / "future_single.txt"
    assert not work_dir.exists() and not single_file.exists()

    with TaskLite(backend=backend, **kwargs) as p:
        dir_rec = p.grant_authorization("future_outputs", target_type="directory")
        file_rec = p.grant_authorization(single_file, target_type="file")
        assert not work_dir.exists() and not single_file.exists()
        assert dir_rec["path"] == str(work_dir.resolve())
        assert dir_rec["target_type"] == "directory"
        assert isinstance(dir_rec["authorized_at"], str) and dir_rec["authorized_at"]
        assert file_rec["path"] == str(single_file.resolve())
        assert file_rec["target_type"] == "file"

        # 改变 cwd 后相对路径仍以实例构造期 base_dir 锚定
        other_cwd = tmp_path / "other_cwd"
        other_cwd.mkdir()
        monkeypatch.chdir(other_cwd)
        with pytest.raises(TaskLiteError, match="already exists"):
            p.grant_authorization("future_outputs", target_type="directory")

        # 创建目标文件并在无 authorizer 且无 AUTO_APPROVE 下直接执行前删
        work_dir.mkdir()
        nested = work_dir / "nested.txt"
        nested.write_text("stale", encoding="utf-8")
        single_file.write_text("stale-single", encoding="utf-8")

        p.register_task("writer", _declare_then_write_handler)
        p.enqueue(Job("writer", "nested", payload={"path": str(nested), "content": "fresh-nested"}))
        p.enqueue(Job("writer", "single", payload={"path": str(single_file), "content": "fresh-single"}))
        with in_process_workers(p), inject_authorizer(p, None):
            p.run(install_signals=False)

        assert _jobs_by_status(p, "succeeded") == {"writer::nested", "writer::single"}
        assert nested.read_text(encoding="utf-8") == "fresh-nested"
        assert single_file.read_text(encoding="utf-8") == "fresh-single"

    if backend == "sqlite":
        monkeypatch.chdir(tmp_path)
        with TaskLite(backend="sqlite", state_path=state_path) as reopened:
            assert {(r["path"], r["target_type"]) for r in reopened.list_authorizations()} == {
                (str(work_dir.resolve()), "directory"),
                (str(single_file.resolve()), "file"),
            }


def test_grant_authorization_guards_protected_paths_and_conflicts(tmp_path, monkeypatch):
    """grant_authorization 严守 Run/关闭守卫、受保护 5 路径、根路径、链接与冲突矩阵。"""
    import subprocess
    import sys

    monkeypatch.chdir(tmp_path)
    state_dir = tmp_path / "state_region"
    state_dir.mkdir()
    state_path = state_dir / "engine.db"
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    p = TaskLite(backend="sqlite", state_path=state_path)
    try:
        # 关键字参数与非法 target_type
        with pytest.raises(TypeError):
            p.grant_authorization(work_dir / "a.txt", "file")  # type: ignore[misc]
        for bad_type in ("symlink", "", None, 123, True):
            with pytest.raises(TaskLiteError, match="target_type"):
                p.grant_authorization(work_dir / "a.txt", target_type=bad_type)  # type: ignore[arg-type]

        # Run 期守卫
        with running(p):
            with pytest.raises(TaskLiteError, match="is only allowed outside run"):
                p.grant_authorization(work_dir / "a.txt", target_type="file")

        # 全部 5 项受保护内部路径、父目录与文件系统根一律拒绝
        for forbidden in (
            state_path,
            Path(f"{state_path}-wal"),
            Path(f"{state_path}-shm"),
            Path(f"{state_path}-journal"),
            Path(f"{state_path}.lock"),
            state_dir,
            tmp_path,
            Path(tmp_path.anchor),
        ):
            with pytest.raises(TaskLiteError):
                p.grant_authorization(forbidden, target_type="file")
            with pytest.raises(TaskLiteError):
                p.grant_authorization(forbidden, target_type="directory")

        # Windows NTFS Junction（免特权）硬拦截
        if sys.platform == "win32":
            junc = work_dir / "junc_target"
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(junc), str(state_dir)],
                check=True,
                capture_output=True,
            )
            try:
                with pytest.raises(TaskLiteError, match="junction|reparse"):
                    p.grant_authorization(junc, target_type="directory")
            finally:
                if os.path.lexists(junc):
                    os.rmdir(junc)

        # 重复授权与父子类型冲突矩阵
        parent_file = work_dir / "parent_file"
        p.grant_authorization(parent_file, target_type="file")
        with pytest.raises(TaskLiteError, match="already exists"):
            p.grant_authorization(parent_file, target_type="file")
        with pytest.raises(TaskLiteError, match="already exists"):
            p.grant_authorization(parent_file, target_type="directory")
        with pytest.raises(TaskLiteError, match="existing file authorization"):
            p.grant_authorization(parent_file / "child.txt", target_type="file")
        with pytest.raises(TaskLiteError, match="existing file authorization"):
            p.grant_authorization(parent_file / "subdir", target_type="directory")

        # 已存在子授权时禁止把祖先授权为 file
        nested_dir = work_dir / "tree" / "branch"
        p.grant_authorization(nested_dir, target_type="directory")
        with pytest.raises(TaskLiteError, match="descendant authorization"):
            p.grant_authorization(work_dir / "tree", target_type="file")
        # 但允许把祖先授权为 directory，且两者独立存取与撤销
        p.grant_authorization(work_dir / "tree", target_type="directory")
        assert p.revoke_authorization(nested_dir) is True
        assert p.revoke_authorization(work_dir / "tree") is True

        # Windows 大小写等价重复拒绝与跨大小写撤销
        if os.name == "nt":
            case_file = work_dir / "CaseTarget.txt"
            p.grant_authorization(case_file, target_type="file")
            with pytest.raises(TaskLiteError, match="already exists"):
                p.grant_authorization(work_dir / "casetarget.txt", target_type="file")
            assert p.revoke_authorization(work_dir / "CASETARGET.TXT") is True
    finally:
        p.close()

    # 关闭后守卫
    with pytest.raises(TaskLiteError, match="closed"):
        p.grant_authorization(work_dir / "after_close.txt", target_type="file")


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal", ".lock"])
def test_runtime_forbids_deleting_sqlite_sidecar_and_lock_files(tmp_path, monkeypatch, suffix):
    """运行期文件清理同样严禁触碰 state_path 的 -wal/-shm/-journal/.lock 伴随文件。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    state_path = tmp_path / "engine.db"
    sidecar = Path(f"{state_path}{suffix}")
    with TaskLite(backend="sqlite", state_path=state_path) as p:
        p.register_task("work", _quick_handler)
        p.enqueue(Job(
            "work",
            "one",
            files={"bad": FileSpec(sidecar, role="output", delete_existing=True)},
        ))
        with in_process_workers(p):
            p.run(install_signals=False)
        assert "work::one" in _jobs_by_status(p, "failed")
        assert p.list_authorizations() == []
