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
    TaskLite,
    TaskLiteError,
    TaskResult,
)
from tasklite.resource import CapacityResource


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
    ctx.declare_file(
        "clean_target",
        FileSpec("clean_me.txt", role="output", delete_on_success=True),
    )
    (Path(ctx.base_dir) / "clean_me.txt").write_text("content", encoding="utf-8")
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
        time.sleep(0.01)
    raise AssertionError("condition did not become true before timeout")


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
        time.sleep(0.01)
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
        time.sleep(0.01)
    if not gate.exists():
        raise RuntimeError("quick handler gate timed out")
    return TaskResult.success(metadata={"quick": True})


def test_worker_declaration_wakes_main_loop_wait(tmp_path, monkeypatch, caplog):
    """主循环等待在途事件时，真实 worker 的动态声明必须立即推进授权与日志。"""
    monkeypatch.chdir(tmp_path)
    caplog.set_level("INFO", logger="tasklite")
    target = tmp_path / "waiting-declare.txt"
    target.write_text("old", encoding="utf-8")
    entered = tmp_path / "worker-entered"
    trigger = tmp_path / "worker-trigger"

    p = TaskLite(
        backend="sqlite",
        state_path=tmp_path / "declaration-wake.db",
        max_workers=1,
    )
    authorizer = _ControlledAuthorizer(decision=True)
    p.authorizer = authorizer
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

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        deadline = time.monotonic() + 10.0
        while not entered.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert entered.exists()
        assert wait_observed.wait(10.0), "主循环应已在等待在途完成"
        trigger.write_text("go", encoding="utf-8")
        assert authorizer.first_call.wait(10.0)
        runner.join(timeout=15.0)
        assert not runner.is_alive()
        assert errors == []
    finally:
        trigger.touch(exist_ok=True)
        p.stop()
        runner.join(timeout=5.0)

    assert any(timeout is None or timeout > 5.0 for timeout in wait_timeouts)
    assert target.read_text(encoding="utf-8") == "new"
    assert {
        "declared::d1",
        "follow::f1",
    } <= set(p._runtime.store.backend.load_successes())
    assert any("FILE DECLARATION:" in record.message for record in caplog.records)
    assert any(
        "FILE AUTHORIZATION APPROVED:" in record.message
        for record in caplog.records
    )
    p.close()


def test_worker_wait_authorization_pauses_dispatch_while_other_worker_settles(
    tmp_path, monkeypatch,
):
    """真实双 worker 下，授权等待暂停新派发，但不阻塞另一在途 worker 结算。"""
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "pending_auth.txt"
    target.write_text("old", encoding="utf-8")
    third_marker = tmp_path / "third-started"
    quick_gate = tmp_path / "quick-gate"

    p = TaskLite(backend="sqlite", state_path=tmp_path / "t_auth_wait.db", max_workers=2)
    authorizer = _ControlledAuthorizer(decision=None)
    p.authorizer = authorizer
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
        if any(
            handle.waiting_auth
            for handle in p._runtime.in_flight.active_handles()
        ):
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

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        assert authorizer.first_call.wait(10.0)
        quick_gate.write_text("go", encoding="utf-8")
        _wait_for(lambda: "quick::q1" in p._runtime.store.backend.load_successes())
        assert "slow_decl::s1" not in p._runtime.store.backend.load_successes()
        assert not third_marker.exists()
        assert any(handle.waiting_auth for handle in p._runtime.in_flight.active_handles())
        gate_observed.clear()
        assert gate_observed.wait(10.0)
        assert not third_spawned.is_set()

        authorizer.decision = True
        runner.join(timeout=20.0)
        assert not runner.is_alive()
        assert errors == []
    finally:
        p.stop()
        runner.join(timeout=5.0)

    assert set(p._runtime.store.backend.load_successes()) == {
        "slow_decl::s1",
        "quick::q1",
        "third::t1",
    }
    assert target.read_text(encoding="utf-8") == "updated"
    assert third_marker.exists()
    p.close()


def test_authorization_wait_does_not_consume_execution_timeout(tmp_path, monkeypatch):
    """等待越过原执行截止后批准，恢复剩余时限并让 worker 正常完成。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "paused-timeout.txt"
    target.write_text("old", encoding="utf-8")

    p = TaskLite(backend="sqlite", state_path=tmp_path / "paused-timeout.db")
    authorizer = _BlockingAuthorizer()
    p.authorizer = authorizer
    deadline_passed = False
    reap_after_deadline = threading.Event()
    original_reap = p._runtime.channel.reap_completed

    def tracked_reap(handles):
        nonlocal deadline_passed
        result = original_reap(handles)
        if deadline_passed:
            reap_after_deadline.set()
        return result

    monkeypatch.setattr(p._runtime.channel, "reap_completed", tracked_reap)
    p.register_task("work", _declare_then_write_handler, timeout_seconds=30)
    p.enqueue(Job(
        "work",
        "one",
        payload={"path": str(target), "content": "updated"},
    ))

    errors = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        assert authorizer.first_call.wait(10.0)
        _wait_for(
            lambda: any(
                handle.waiting_auth
                for handle in p._runtime.in_flight.active_handles()
            )
        )
        handle = next(
            handle
            for handle in p._runtime.in_flight.active_handles()
            if handle.waiting_auth
        )
        assert handle.paused_remaining is not None
        handle.timeout_at_monotonic = time.monotonic() - 1.0
        deadline_passed = True
        authorizer.release.set()
        assert reap_after_deadline.wait(10.0)

        authorizer.decision = True
        runner.join(timeout=20.0)
        assert not runner.is_alive()
        assert errors == []
    finally:
        authorizer.release.set()
        p.stop()
        runner.join(timeout=5.0)

    assert "work::one" in p._runtime.store.backend.load_successes()
    assert target.read_text(encoding="utf-8") == "updated"
    p.close()


def test_directory_authorization_reusable_and_subpath_revocation(tmp_path, monkeypatch):
    """目录授权可复用，撤销子路径不产生父目录例外，实际清理错误与拒绝有不同去向。"""
    monkeypatch.chdir(tmp_path)
    p = TaskLite(backend="sqlite", state_path=tmp_path / "t_dir_auth.db")

    dir_path = (tmp_path / "managed_dir").resolve()
    sub_file = (dir_path / "sub" / "item.txt").resolve()
    sub_file.parent.mkdir(parents=True, exist_ok=True)
    sub_file.write_text("sub content", encoding="utf-8")

    # 1. 预先授予目录授权
    p.authorizer = None
    p.register_task("dir_task", _dir_handler)
    p.enqueue(Job("dir_task", "d1"))

    # 尚未授权时拒绝
    p.run()
    assert "dir_task::d1" not in p._runtime.store.backend.load_successes()

    # 记录目录授权
    p._runtime.store.record_authorization(dir_path, "directory")
    authorizations = p.list_authorizations()
    assert any(a["path"] == str(dir_path) and a["target_type"] == "directory" for a in authorizations)
    p.close()

    # 2. SQLite 重开后目录授权仍可复用；撤销子路径不能覆盖父目录授权
    reopened = TaskLite(backend="sqlite", state_path=tmp_path / "t_dir_auth.db")
    reopened.register_task("dir_task", _dir_handler)
    revoked = reopened.revoke_authorization(sub_file)
    assert revoked is False
    assert reopened._runtime.store.is_path_authorized(sub_file) is True

    # 3. 运行：目录授权可复用，子路径声明执行成功
    reopened.run()
    assert "dir_task::d1" in reopened._runtime.store.backend.load_successes()

    # 4. 精确撤销父目录授权，子路径不再被授权
    assert reopened.revoke_authorization(dir_path) is True
    assert reopened._runtime.store.is_path_authorized(sub_file) is False
    reopened.close()


def test_cleanup_failure_vs_authorization_denial_destinations(tmp_path, monkeypatch):
    """实际清理错误记为 failed 档案，授权拒绝回到 pending 且不计入 failed。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    p = TaskLite(backend="sqlite", state_path=tmp_path / "t_diff_dest.db")

    p.register_task("clean_task", _cleanup_fail_handler)
    p.enqueue(Job("clean_task", "c1"))

    # 构造实际清理错误（磁盘 IO 失败）
    with patch("tasklite.utils.artifact.ArtifactPathPolicy.cleanup", side_effect=OSError("Read-only file system")):
        p.run()

    # 清理错误落入 failed 档案
    failed = p._runtime.store.backend.load_failed()
    assert "clean_task::c1" in failed
    assert "Success cleanup failed" in failed["clean_task::c1"].get("error_message", "")
    assert "clean_task::c1" not in p._runtime.store.backend.load_successes()


@pytest.mark.parametrize("deferred", [False, True], ids=["immediate", "after-wait"])
def test_authorization_denial_settles_attempt_and_keeps_job_pending(
    tmp_path, monkeypatch, deferred,
):
    """即时与等待后拒绝都先结束 worker，再原子登记失败 Attempt 并回 pending。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    state_path = tmp_path / "denial.db"
    target = tmp_path / "target.txt"
    target.write_text("old data", encoding="utf-8")

    p = TaskLite(backend="sqlite", state_path=state_path, max_workers=2)
    authorizer = _ControlledAuthorizer(decision=False if not deferred else None)
    p.authorizer = authorizer
    p.register_task("target_task", _declare_then_write_handler)
    p.register_task("middle", _quick_handler)
    p.register_task("leaf", _quick_handler)
    p.register_task("independent", _quick_handler)

    target_job = Job(
        "target_task",
        "t1",
        payload={"path": str(target), "content": "new data"},
        trigger="always",
    )
    middle_job = Job(
        "middle",
        "m1",
        depends_on=["target_task::t1"],
        trigger="always",
    )
    leaf_job = Job(
        "leaf",
        "l1",
        depends_on=["middle::m1"],
        trigger="always",
    )
    for historical_job in (target_job, middle_job, leaf_job):
        p.set_job_status(historical_job, "succeeded", recursive=False)
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

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        assert authorizer.first_call.wait(10.0)
        if deferred:
            authorizer.decision = False
        runner.join(timeout=20.0)
        assert not runner.is_alive()
        assert errors == []
    finally:
        p.stop()
        runner.join(timeout=5.0)

    assert target.read_text(encoding="utf-8") == "old data"
    assert "independent::i1" in p._runtime.store.backend.load_successes()
    assert set(p._runtime.store.backend.load_failed()) == set()
    pending_uids = {
        row["job_uid"]
        for row in p._runtime.store.backend.load_queue_jobs(status="pending")
    }
    assert {"target_task::t1", "middle::m1", "leaf::l1"} <= pending_uids
    assert {
        "target_task::t1",
        "middle::m1",
        "leaf::l1",
    } <= p._runtime._temporary_excluded_uids
    assert {
        "target_task::t1",
        "middle::m1",
        "leaf::l1",
    } <= set(p._runtime.store.backend.load_successes())
    assert p._runtime.store.backend.load_settled_attempts("middle::m1") == []
    assert p._runtime.store.backend.load_settled_attempts("leaf::l1") == []
    attempt, = p._runtime.store.backend.load_settled_attempts("target_task::t1")
    assert attempt["outcome"] == "failure"
    assert "authorization denied for" in attempt["error_message"]
    assert attempt["result"]["annotations"]["authorization_status"] == "denied"
    assert p._runtime.session.retry_counts.get("target_task::t1", 0) == 0
    p.close()

    reopened = TaskLite(backend="sqlite", state_path=state_path, max_workers=2)
    reopened.authorizer = _MutableAuthorizer(allowed=True)
    reopened.register_task("target_task", _declare_then_write_handler)
    reopened.register_task("middle", _quick_handler)
    reopened.register_task("leaf", _quick_handler)
    reopened.run(install_signals=False)

    assert {
        "target_task::t1",
        "middle::m1",
        "leaf::l1",
    } <= set(reopened._runtime.store.backend.load_successes())
    attempts = reopened._runtime.store.backend.load_settled_attempts("target_task::t1")
    assert [record["outcome"] for record in attempts] == ["failure", "success"]
    assert target.read_text(encoding="utf-8") == "new data"
    reopened.close()


def test_stop_during_authorization_wait_records_stopped_reason_without_exclusion(
    tmp_path, monkeypatch,
):
    """stop 解除授权等待，记录停止原因，关闭全 Run 派发而不排除单个 Job。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "stopped.txt"
    target.write_text("old data", encoding="utf-8")
    stopped_marker = tmp_path / "stopped-marker"
    quick_gate = tmp_path / "quick-gate"

    p = TaskLite(backend="sqlite", state_path=tmp_path / "stop-auth.db", max_workers=2)
    release_observations: list[tuple[set[str], list[dict]]] = []

    class ObservingCapacity(CapacityResource):
        def release(self, amount: float) -> None:
            release_observations.append((
                {
                    row["job_uid"]
                    for row in p._runtime.store.backend.load_queue_jobs(status="running")
                },
                p._runtime.store.backend.load_settled_attempts("waiting::w1"),
            ))
            super().release(amount)

    p.register_resource(ObservingCapacity("slot", max_capacity=1.0))
    authorizer = _BlockingAuthorizer()
    p.authorizer = authorizer
    p.register_task("waiting", _declare_then_write_handler)
    p.register_task("quick", _wait_for_gate_then_quick_handler)
    p.register_task("after_stop", _marker_handler)
    p.enqueue(Job(
        "waiting",
        "w1",
        payload={"path": str(target), "content": "new data"},
        resources={"slot": 1.0},
    ))
    p.enqueue(Job("quick", "q1", payload={"gate": str(quick_gate)}))
    p.enqueue(Job(
        "after_stop",
        "a1",
        payload={"marker": str(stopped_marker)},
    ))

    errors = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        assert authorizer.first_call.wait(10.0)
        quick_gate.write_text("go", encoding="utf-8")
        _wait_for(
            lambda: any(
                handle.waiting_auth
                for handle in p._runtime.in_flight.active_handles()
            )
        )
        p.stop()
        running = p._runtime.store.backend.load_queue_jobs(status="running")
        assert "waiting::w1" in {row["job_uid"] for row in running}
        authorizer.release.set()
        runner.join(timeout=20.0)
        assert not runner.is_alive()
        assert errors == []
    finally:
        authorizer.release.set()
        p.stop()
        runner.join(timeout=5.0)

    assert target.read_text(encoding="utf-8") == "old data"
    assert p._runtime.store.backend.load_failed() == {}
    assert p._runtime.store.backend.load_queue_jobs(status="pending")[0]["job_uid"] == "waiting::w1"
    attempt, = p._runtime.store.backend.load_settled_attempts("waiting::w1")
    assert attempt["outcome"] == "failure"
    assert "authorization wait stopped for" in attempt["error_message"]
    assert attempt["result"]["annotations"]["authorization_status"] == "stopped"
    assert "waiting::w1" not in p._runtime._temporary_excluded_uids
    assert p._runtime.session.retry_counts.get("waiting::w1", 0) == 0
    assert p._runtime.in_flight.active_handles() == []
    assert any(
        "waiting::w1" in running_uids and not attempts
        for running_uids, attempts in release_observations
    )
    assert not stopped_marker.exists()
    pending_uids = {
        row["job_uid"]
        for row in p._runtime.store.backend.load_queue_jobs(status="pending")
    }
    assert {"waiting::w1", "after_stop::a1"} <= pending_uids

    authorizer.decision = True
    p.run(install_signals=False)
    assert "waiting::w1" in p._runtime.store.backend.load_successes()
    assert "after_stop::a1" in p._runtime.store.backend.load_successes()
    assert stopped_marker.exists()
    assert target.read_text(encoding="utf-8") == "new data"
    p.close()


def test_auto_approve_does_not_authorize_out_of_root_dynamic_target(
    tmp_path, monkeypatch,
):
    """硬性输出范围校验先于自动授权记录，根外目标保持原样并结算失败。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = tmp_path / "outside.txt"
    target.write_text("valuable", encoding="utf-8")

    p = TaskLite(
        backend="sqlite",
        state_path=tmp_path / "outside-root.db",
        output_roots=allowed,
    )
    p.register_task("work", _declare_then_write_handler)
    p.enqueue(Job(
        "work",
        "one",
        payload={"path": str(target), "content": "overwrite"},
    ))
    p.run(install_signals=False)

    assert target.read_text(encoding="utf-8") == "valuable"
    assert p.list_authorizations() == []
    failure = p._runtime.store.backend.load_failed()["work::one"]
    assert "outside output roots" in failure["error_message"]
    attempt, = p._runtime.store.backend.load_settled_attempts("work::one")
    assert attempt["outcome"] == "failure"
    assert "work::one" not in p._runtime._temporary_excluded_uids
    p.close()


def test_changed_dynamic_target_cannot_use_prior_wait_to_bypass_protection(
    tmp_path, monkeypatch,
):
    """等待期间将普通目标替换为受保护文件的链接时，后续决策仍先校验实际目标。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = allowed / "target.txt"
    target.write_text("old", encoding="utf-8")
    state_path = tmp_path / "changed-target.db"

    p = TaskLite(
        backend="sqlite",
        state_path=state_path,
        output_roots=allowed,
    )
    authorizer = _ControlledAuthorizer(decision=None)
    p.authorizer = authorizer
    p.register_task("work", _declare_then_write_handler)
    p.enqueue(Job(
        "work",
        "one",
        payload={"path": str(target), "content": "overwrite"},
    ))

    errors = []

    def run_pipeline():
        try:
            p.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        assert authorizer.first_call.wait(10.0)
        target.unlink()
        target.symlink_to(state_path)
        authorizer.decision = True
        runner.join(timeout=20.0)
        assert not runner.is_alive()
        assert errors == []
    finally:
        p.stop()
        runner.join(timeout=5.0)

    assert target.is_symlink()
    assert target.resolve() == state_path.resolve()
    assert p.list_authorizations() == []
    assert p._runtime.store.backend.load_successes() == {}
    failure = p._runtime.store.backend.load_failed()["work::one"]
    assert "symlink" in failure["error_message"]
    attempt, = p._runtime.store.backend.load_settled_attempts("work::one")
    assert attempt["outcome"] == "failure"
    assert "work::one" not in p._runtime._temporary_excluded_uids
    p.close()


def test_auto_approve_persists_actual_path_and_forbids_protected_roots(tmp_path, monkeypatch):
    """TASKLITE_AUTO_APPROVE=1 只自动同意实际请求路径，持久记录，保护路径仍禁止。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    state_path = tmp_path / "t_auto_approve.db"
    p = TaskLite(backend="sqlite", state_path=state_path)

    p.register_task("good_task", _good_handler)
    p.register_task("bad_task", _protected_path_handler)

    p.enqueue(Job("good_task", "g1"))
    (tmp_path / "my_out.txt").write_text("old", encoding="utf-8")
    p.run()

    # 1. 实际请求路径被自动同意并持久记录
    assert "good_task::g1" in p._runtime.store.backend.load_successes()
    auths = p.list_authorizations()
    expected_path = str((tmp_path / "my_out.txt").resolve())
    assert any(
        a["path"] == expected_path and a["target_type"] == "file"
        for a in auths
    )
    # 不会预先全量授权未请求的路径或整个根目录
    unrelated_path = str((tmp_path / "unrelated.txt").resolve())
    assert not any(a["path"] == unrelated_path for a in auths)
    p.close()

    # 2. SQLite 重开后精确文件授权继续复用，无需再次询问
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    reopened = TaskLite(backend="sqlite", state_path=state_path)
    reopened.register_task("good_task", _good_handler)
    reopened.register_task("bad_task", _protected_path_handler)
    assert any(
        a["path"] == expected_path and a["target_type"] == "file"
        for a in reopened.list_authorizations()
    )
    reopened.enqueue(Job("good_task", "g2"))
    reopened.run()
    assert "good_task::g2" in reopened._runtime.store.backend.load_successes()

    # 3. 保护路径（例如 state 数据库文件）即使自动同意也严禁授权
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    reopened.enqueue(Job("bad_task", "b1"))
    reopened.run()
    assert "bad_task::b1" not in reopened._runtime.store.backend.load_successes()
    assert not reopened._runtime.store.is_path_authorized(state_path)
    reopened.close()


def test_list_and_revoke_authorizations_surface_and_run_guard(tmp_path, monkeypatch):
    """list_authorizations 与 revoke_authorization 规范格式、精确匹配与 Run 守卫。"""
    monkeypatch.chdir(tmp_path)
    p = TaskLite(backend="sqlite", state_path=tmp_path / "t_mgmt_guard.db")

    f1 = tmp_path / "doc.txt"
    f1.write_text("data", encoding="utf-8")

    # 1. 空授权查询
    assert p.list_authorizations() == []

    # 2. 记录一条授权并验证结构字段样本
    p._runtime.store.record_authorization(f1, "file")
    records = p.list_authorizations()
    assert len(records) == 1
    rec = records[0]
    assert rec["path"] == str(f1.resolve())
    assert rec["target_type"] == "file"
    assert isinstance(rec["authorized_at"], str)

    # 3. 精确撤销
    assert p.revoke_authorization(f1) is True
    # 再次撤销不存在的记录返回 False
    assert p.revoke_authorization(f1) is False
    assert p.list_authorizations() == []

    # 4. Run 运行期守卫：运行中调用 list/revoke 明确拒绝
    with patch.object(type(p), "is_running", new_callable=lambda: property(lambda self: True)):
        with pytest.raises(TaskLiteError, match="is only allowed outside run"):
            p.list_authorizations()

        with pytest.raises(TaskLiteError, match="is only allowed outside run"):
            p.revoke_authorization("foo.txt")


@pytest.mark.parametrize("declaration", ["static", "dynamic"])
@pytest.mark.parametrize("approval", ["interactive", "automatic"])
@pytest.mark.parametrize("operation", ["delete_existing", "delete_on_success", "delete_on_failure"])
@pytest.mark.parametrize("forbidden", ["outside", "protected", "output-root", "filesystem-root", "symlink"])
def test_forbidden_file_operations_never_request_or_persist_authorization(
    tmp_path, monkeypatch, declaration, approval, operation, forbidden,
):
    """禁止目标在静态与动态入口均先拒绝，不询问、不保存授权、不进入删除。"""
    monkeypatch.chdir(tmp_path)
    if approval == "automatic":
        monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    else:
        monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    state_path = allowed / "state.db"
    valuable = tmp_path / "valuable"
    valuable.write_text("valuable", encoding="utf-8")
    targets = {
        "outside": valuable,
        "protected": state_path,
        "output-root": allowed,
        "filesystem-root": Path(allowed.anchor),
        "symlink": allowed / "linked",
    }
    targets["symlink"].symlink_to(valuable)
    target = targets[forbidden]
    calls = []
    removals = []
    with TaskLite(backend="sqlite", state_path=state_path, output_roots=allowed) as p:
        p.authorizer = lambda *args: calls.append(args) or True
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
        p.run(install_signals=False)
        assert calls == []
        assert p.list_authorizations() == []
        assert removals == []
        assert valuable.read_text(encoding="utf-8") == "valuable"
        assert target.exists()
        assert "work::one" in p._runtime.store.backend.load_failed()
        attempts = p._runtime.store.backend.load_settled_attempts("work::one")
        assert len(attempts) == (1 if declaration == "dynamic" else 0)


def _declare_spec_handler(job, ctx):
    ctx.declare_file("target", FileSpec.from_dict(job.payload["spec"]))
    return TaskResult.success()


def test_static_forbidden_declaration_prevents_authorizing_other_targets(tmp_path, monkeypatch):
    """同批预声明全部校验完成后才开始询问，禁止目标不会留下部分授权。"""
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    good = allowed / "good"
    bad = tmp_path / "bad"
    good.write_text("good", encoding="utf-8")
    bad.write_text("bad", encoding="utf-8")
    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db", output_roots=allowed) as p:
        calls = []
        p.authorizer = lambda *args: calls.append(args) or True
        p.register_task("work", _quick_handler)
        p.enqueue(Job("work", "one", files={
            "good": FileSpec(good, role="output", delete_existing=True),
            "bad": FileSpec(bad, role="output", delete_existing=True),
        }))
        p.run(install_signals=False)
        assert calls == []
        assert p.list_authorizations() == []
        assert good.read_text(encoding="utf-8") == "good"
        assert bad.read_text(encoding="utf-8") == "bad"


@pytest.mark.parametrize("changed", ["waiting", "other-declaration"])
def test_static_authorization_wait_revalidates_target_before_asking_again(tmp_path, monkeypatch, changed):
    """待决后目标变为链接，下一次询问前拒绝且保留目标。"""
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
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

    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db") as p:
        p.authorizer = authorize
        p.register_task("work", _quick_handler)
        p.enqueue(Job("work", "one", files={
            "target": FileSpec(target, role="output", delete_existing=True),
            "other": FileSpec(other, role="output", delete_on_success=True),
        }))
        p.run(install_signals=False)
        assert len(calls) == 1
        assert p.list_authorizations() == []
        assert changed_target.is_symlink()
        assert valuable.read_text(encoding="utf-8") == "valuable"
        assert "symlink" in p._runtime.store.backend.load_failed()["work::one"]["error_message"]
        assert p._runtime.store.backend.load_settled_attempts("work::one") == []


@pytest.mark.parametrize("declaration", ["static", "dynamic"])
def test_broken_symlink_preparation_authorizes_and_removes_only_the_link(
    tmp_path, monkeypatch, declaration,
):
    """前删断链时授权其目录项路径，不授权或创建缺失的链接目标。"""
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    missing = tmp_path / "missing"
    link = tmp_path / "link"
    link.symlink_to(missing)
    spec = FileSpec(link, role="cache", delete_existing=True)
    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db") as p:
        calls = []
        p.authorizer = lambda *args: calls.append(args) or True
        p.register_task("work", _declare_spec_handler if declaration == "dynamic" else _quick_handler)
        p.enqueue(Job(
            "work", "one", payload={"spec": spec.to_dict()},
            files={"target": spec} if declaration == "static" else {},
        ))
        p.run(install_signals=False)
        assert calls == [(str(link), "file")]
        authorization, = p.list_authorizations()
        assert authorization["path"] == str(link)
        assert authorization["target_type"] == "file"
        assert not os.path.lexists(link)
        assert not missing.exists()
        assert not p._runtime.store.is_path_authorized(missing)
        assert p.revoke_authorization(missing) is False
        assert p.revoke_authorization(link) is True
        assert p.list_authorizations() == []
        assert "work::one" in p._runtime.store.backend.load_successes()


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


@pytest.mark.parametrize("backend", ["sqlite", "memory"])
@pytest.mark.parametrize("declaration", ["static", "dynamic"])
def test_allowed_file_operations_keep_instance_path_base(
    tmp_path, monkeypatch, backend, declaration,
):
    """主进程和 worker 改变目录后，允许的相对路径仍按实例创建基准授权与准备。"""
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    base = tmp_path / "base"
    allowed = tmp_path / "allowed"
    other = tmp_path / "other"
    for directory in (base, allowed, other):
        directory.mkdir()
    target = allowed / "result"
    target.write_text("old", encoding="utf-8")
    monkeypatch.chdir(base)
    kwargs = {"state_path": base / "state.db"} if backend == "sqlite" else {}
    with TaskLite(backend=backend, output_roots="../allowed", **kwargs) as p:
        calls = []
        p.authorizer = lambda *args: calls.append(args) or True
        p.register_task("work", _fixed_base_file_handler)
        monkeypatch.chdir(other)
        p.enqueue(Job(
            "work", "one", payload={"worker_cwd": str(other), "declaration": declaration},
            files={"target": FileSpec("../allowed/result", role="output", delete_existing=True)}
            if declaration == "static" else {},
        ))
        p.run(install_signals=False)
        assert calls == [(str(target), "file")]
        authorization, = p.list_authorizations()
        assert authorization["path"] == str(target)
        assert authorization["target_type"] == "file"
        assert target.read_text(encoding="utf-8") == "updated"
        assert "work::one" in p._runtime.store.backend.load_successes()


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


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("outcome", ["success", "failure", "retry"])
@pytest.mark.parametrize("initial,decision", [
    (initial, decision)
    for initial in ("file", "missing")
    for decision in ("approve", "deny", "wait-approve", "wait-deny", "stop")
] + [("directory", "approve")])
def test_worker_target_type_change_requires_cleanup_scope_confirmation(
    tmp_path, monkeypatch, backend, initial, outcome, decision,
):
    """真实 worker 改变目标类型后，清理授权等待、拒绝和停止均保持结算语义。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    if initial == "file":
        target.write_text("initial", encoding="utf-8")
    elif initial == "directory":
        target.mkdir()
    p = TaskLite(backend=backend, state_path=tmp_path / "scope.db")
    calls = []
    waiting = threading.Event()
    final_decision = [None if decision.startswith("wait") or decision == "stop" else decision == "approve"]

    def authorize(path, kind):
        calls.append((path, kind))
        if len(calls) == 1:
            return True
        waiting.set()
        return final_decision[0]

    p.authorizer = authorize
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

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        if initial != "directory" and (decision.startswith("wait") or decision == "stop"):
            assert waiting.wait(10)
            _wait_for(lambda: p._runtime._completion.waiting_authorization)
            assert target.joinpath("child").read_text(encoding="utf-8") == "business data"
            assert p._runtime.store.backend.load_settled_attempts("replace::one") == []
            assert p._runtime.store.backend.load_queue_jobs(status="running")
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
    attempts = p._runtime.store.backend.load_settled_attempts("replace::one")
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
        assert {row["job_uid"] for row in p._runtime.store.backend.load_queue_jobs(status="pending")} == {"replace::one", "downstream::one"}
        assert p._runtime.store.backend.load_failed() == {}
        assert p._runtime.store.backend.load_successes() == {}
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
            time.sleep(0.01)

    threading.Thread(target=linger, daemon=False).start()
    return result


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("decision", [True, False])
def test_cleanup_wait_preserves_result_and_settles_other_worker_before_single_completion(
    tmp_path, monkeypatch, backend, decision,
):
    """清理待决时继续结算在途结果、暂停新派发；只调用一次收尾且拒绝先终止进程。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("initial", encoding="utf-8")
    independent_gate, exit_gate = tmp_path / "independent", tmp_path / "exit"
    p = TaskLite(backend=backend, state_path=tmp_path / "lifecycle.db", max_workers=2)
    requested = threading.Event()
    decided = [None]
    calls = []

    def authorize(path, kind):
        calls.append(kind)
        if kind == "file":
            return True
        requested.set()
        return decided[0]

    p.authorizer = authorize
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

    runner = threading.Thread(target=run_pipeline)
    runner.start()
    try:
        assert requested.wait(10)
        _wait_for(lambda: p._runtime._completion.waiting_authorization)
        entry = p._runtime.in_flight["replace::one"]
        assert entry.handle.process.is_alive()
        entry.handle.timeout_at_monotonic = time.monotonic() - 1
        independent_gate.touch()
        _wait_for(lambda: "independent::one" in p._runtime.store.backend.load_successes())
        assert not marker.exists()
        assert complete_calls == ["independent::one"]
        assert target.joinpath("child").exists()
        decided[0] = decision
        if decision:
            _wait_for(lambda: "replace::one" in p._runtime.store.backend.load_successes())
            assert entry.handle.process.is_alive()
            assert not target.exists()
            exit_gate.touch()
        runner.join(20)
        assert not runner.is_alive()
        assert errors == []
        assert complete_calls.count("replace::one") == 1
        assert len(p._runtime.store.backend.load_settled_attempts("replace::one")) == 1
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


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_worker_leaf_link_replacement_never_expands_file_authorization(tmp_path, monkeypatch, backend):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("initial", encoding="utf-8")
    destination = tmp_path / "business"
    destination.mkdir()
    child = destination / "child"
    child.write_text("preserve", encoding="utf-8")
    p = TaskLite(backend=backend, state_path=tmp_path / "link.db")
    calls = []
    p.authorizer = lambda *args: calls.append(args) or True
    p.register_task("replace", _replace_file_with_link_handler)
    p.enqueue(Job("replace", "one", payload={"destination": str(destination)},
                  files={"target": FileSpec(target, role="cache", delete_on_success=True)}))
    p.run()
    assert target.is_symlink()
    assert child.read_text(encoding="utf-8") == "preserve"
    assert calls == [(str(target), "file")]
    assert p.list_authorizations()[0]["target_type"] == "file"
    attempt, = p._runtime.store.backend.load_settled_attempts("replace::one")
    assert attempt["outcome"] == "failure"
    assert "symlink" in attempt["result"]["message"]
    p.close()


def _replace_second_target_handler(job, ctx):
    second = ctx.file_path("second")
    second.unlink()
    second.mkdir()
    (second / "child").write_text("second data", encoding="utf-8")
    return TaskResult.success()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("approve_first_tree", [True, False])
def test_later_authorization_callback_cannot_expand_previously_checked_target(
    tmp_path, monkeypatch, backend, approve_first_tree,
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    first, second = tmp_path / "first", tmp_path / "second"
    for path in (first, second):
        path.write_text("initial", encoding="utf-8")
    p = TaskLite(backend=backend, state_path=tmp_path / "changed-during-query.db")
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

    p.authorizer = authorize
    p.register_task("replace", _replace_second_target_handler)
    p.enqueue(Job("replace", "one", files={
        "first": FileSpec(first, role="cache", delete_on_success=True),
        "second": FileSpec(second, role="cache", delete_on_success=True),
    }))
    p.run()
    assert calls == [(str(first), "file"), (str(second), "file"),
                     (str(second), "directory"), (str(first), "directory")]
    if approve_first_tree:
        assert not first.exists() and not second.exists()
    else:
        assert first.joinpath("child").read_text(encoding="utf-8") == "first data"
        assert second.joinpath("child").read_text(encoding="utf-8") == "second data"
        assert p._runtime.store.backend.load_failed() == {}
        assert p._runtime.store.backend.load_queue_jobs(status="pending")
    p.close()


def _failure_handler(job, ctx):
    return TaskResult.failure("business failure")


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("timing", ["delete_existing", "delete_on_success", "delete_on_failure"])
def test_type_change_at_deletion_snapshot_fails_without_directory_permission(
    tmp_path, monkeypatch, backend, timing,
):
    """最后的对象快照发现范围已扩大时明确失败，不递归删除未授权目录。"""
    from tasklite.utils.artifact import ArtifactPathPolicy

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("initial", encoding="utf-8")
    p = TaskLite(backend=backend, state_path=tmp_path / "snapshot.db")
    calls = []
    p.authorizer = lambda *args: calls.append(args) or True
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
    p.run()
    assert target.joinpath("child").read_text(encoding="utf-8") == "preserve"
    assert calls == [(str(target), "file")]
    failed = p._runtime.store.backend.load_failed()["work::one"]
    assert "authorization scope changed before deletion" in str(failed)
    p.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("cached", [False, True], ids=["channel-result", "cleanup-result"])
@pytest.mark.parametrize("decision", [True, False, None])
def test_run_fault_leaves_uncommitted_cleanup_result_running_for_recovery(
    tmp_path, monkeypatch, backend, cached, decision,
):
    """收到有效结果不等于已提交；异常退出不补结算，下次按原身份恢复失败。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("initial", encoding="utf-8")
    p = TaskLite(backend=backend, state_path=tmp_path / "abort-result.db")
    final_decision = [None if cached else decision]
    calls = []

    def authorize(path, kind):
        calls.append((path, kind))
        return True if kind == "file" else final_decision[0]

    p.authorizer = authorize
    p.register_task("replace", _replace_file_with_tree_handler)
    p.register_task("committed", _quick_handler)
    p.enqueue(Job("committed", "one"))
    p.run(install_signals=False)
    committed = p._runtime.store.backend.load_successes()["committed::one"]
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
    with pytest.raises(RuntimeError) as caught:
        p.run(install_signals=False)
    assert caught.value is fault
    assert calls == [(str(target), "file")] + ([(str(target), "directory")] if cached else [])
    assert target.joinpath("child").read_text(encoding="utf-8") == "business data"
    assert runtime.store.backend.load_settled_attempts("replace::one") == []
    running, = runtime.store.backend.load_queue_jobs(status="running")
    assert running["job_uid"] == "replace::one"
    assert running["attempt_no"] == 1
    assert runtime.store.backend.load_failed() == {}
    assert runtime.store.backend.load_successes()["committed::one"] == committed
    assert p.get_cursor("tree-result") is None
    assert not runtime._completion.waiting_authorization
    assert not runtime.in_flight
    monkeypatch.undo()
    p.run(install_signals=False)
    attempt, = runtime.store.backend.load_settled_attempts("replace::one")
    assert attempt["outcome"] == "failure"
    assert attempt["run_id"] == running["run_id"]
    assert attempt["attempt_no"] == running["attempt_no"]
    assert runtime.store.backend.load_queue_jobs(status="running") == []
    assert "replace::one" in runtime.store.backend.load_failed()
    assert runtime.store.backend.load_successes()["committed::one"] == committed
    assert target.joinpath("child").read_text(encoding="utf-8") == "business data"
    p.run(install_signals=False)
    assert runtime.store.backend.load_settled_attempts("replace::one") == [attempt]
    p.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_success_cleanup_error_does_not_run_unapproved_failure_cleanup(tmp_path, monkeypatch, backend):
    from tasklite.utils.artifact import ArtifactPathPolicy

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    first, second = tmp_path / "first", tmp_path / "second"
    for path in (first, second):
        path.write_text("initial", encoding="utf-8")
    p = TaskLite(backend=backend, state_path=tmp_path / "cleanup-outcome.db")
    calls = []
    p.authorizer = lambda *args: calls.append(args) or True
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
    p.run()
    assert first.read_text(encoding="utf-8") == "initial"
    assert second.joinpath("child").read_text(encoding="utf-8") == "second data"
    assert calls == [(str(first), "file"), (str(second), "file")]
    assert "replace::one" in p._runtime.store.backend.load_failed()
    p.close()
