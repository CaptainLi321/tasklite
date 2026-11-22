"""真实 spawn 子进程隔离与瞬时 Worker Pipe 生命周期集成测试。

聚焦真实 OS 子进程边界：
- 真实子进程超时强杀（看门狗）、IntervalResource 限速、SystemExit 归因；
- 原子成功事务（inputs / 具名 artifacts / spawned_jobs / cursor / attempts）
  与 SQLite 重开恢复、once 去重跳过；
- 子进程被信号击杀（SIGKILL）、异常退出（os._exit）、超时击杀的单次失败与产物清理；
- 产物清理 PermissionError 阻断终态提交并保留原队列快照；
- 正常停止排空与非法管道帧（损坏 JSON / 未知 kind / job_uid / execution_id 不匹配）
  的零伪成功防御与遗留 running 恢复。
"""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
import ctypes
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

from tasklite import FileSpec, Job, StateSnapshot, TaskLite, TaskResult
from tasklite.resource import CapacityResource, IntervalResource
from tasklite.testing import authorizer, cleanup_hook
from tasklite.utils.jsonutil import dumps, loads


def _wait_until(predicate, timeout: float = 10.0, interval: float = 0.01) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


def _hung_handler(job, ctx):
    time.sleep(3.0)
    return TaskResult.success()


def _plain_gated_handler(job, ctx):
    return TaskResult.success(metadata={"ran_after_recovery": True})


def _system_exit_handler(job, ctx):
    raise SystemExit(1)


def success_handler(job: Job, ctx) -> TaskResult:
    output = Path(ctx.declare_file("report", FileSpec(ctx._base_dir / "output" / "published.txt", role="output")))
    output.write_text("已发布", encoding="utf-8")
    ctx.declare_file("source", FileSpec(output, role="input"))
    ctx.set_cursor("position", "42")
    ctx.spawn(Job("child", "child"))
    assert not hasattr(ctx, "suspend_resource")
    return TaskResult.success(metadata={"worker_pid": os.getpid()})


def child_handler(job: Job, ctx) -> TaskResult:
    return TaskResult.success(metadata={"child": True})


def interrupted_handler(job: Job, ctx) -> TaskResult:
    if job.attempt_no > 1:
        return TaskResult.success(metadata={"recovered": True})
    for filename, allowed in (("authorized.txt", True), ("retained.txt", False)):
        path = Path(ctx.declare_file(filename, FileSpec(ctx._base_dir / "output" / filename, role="output", delete_on_failure=allowed)))
        path.write_text("半成品", encoding="utf-8")
    sync_dir = Path(job.payload["sync_dir"])
    (sync_dir / "ready").write_text(str(os.getpid()), encoding="utf-8")
    time.sleep(5.0)
    return TaskResult.success(metadata={"unreachable": True})


def crash_handler(job: Job, ctx) -> TaskResult:
    os._exit(7)


def timeout_handler(job: Job, ctx) -> TaskResult:
    time.sleep(5.0)
    return TaskResult.success(metadata={"unreachable": True})


def cleanup_handler(job: Job, ctx) -> TaskResult:
    path = Path(ctx.declare_file("temporary", FileSpec(ctx._base_dir / "output" / "temporary.txt", role="cache", delete_on_success=True, delete_on_failure=True)))
    path.write_text("临时产物", encoding="utf-8")
    return TaskResult.success(metadata={"uncommitted": True})


def controlled_handler(job: Job, ctx) -> TaskResult:
    sync_dir = Path(job.payload["sync_dir"])
    kept = Path(ctx.declare_file("kept", FileSpec(ctx._base_dir / "output" / "kept.txt", role="output")))
    kept.write_text("保留产物", encoding="utf-8")
    temporary = Path(ctx.declare_file("temporary", FileSpec(ctx._base_dir / "output" / "temporary.txt", role="cache", delete_on_success=True, delete_on_failure=True)))
    temporary.write_text("临时产物", encoding="utf-8")
    (sync_dir / "ready").write_text(str(os.getpid()), encoding="utf-8")
    ctx.declare_file("source", FileSpec(kept, role="input"))
    assert not hasattr(ctx, "suspend_resource")
    assert _wait_until(lambda: (sync_dir / "release").exists(), timeout=15.0)
    return TaskResult.success(metadata={"completed": True})


def invalid_frame_handler(job: Job, ctx) -> TaskResult:
    for filename, allowed in (("authorized.txt", True), ("retained.txt", False)):
        path = Path(ctx.declare_file(filename, FileSpec(ctx._base_dir / "output" / filename, role="output", delete_on_failure=allowed)))
        path.write_text("错误消息之前声明的产物", encoding="utf-8")
    fault = job.payload["fault"]
    sender = ctx._pipe_sender
    assert sender is not None
    if fault == "json":
        sender._conn.send_bytes(b"{corrupt-json-frame")
    else:
        envelope = {
            "job_uid": "invalid::wrong" if fault == "job_uid" else sender.job_uid,
            "execution_id": "wrong.1" if fault == "execution_id" else sender.execution_id,
            "kind": "unexpected" if fault == "kind" else "result",
            "payload": {"status": "success", "raw_result": {"untrusted": True}},
        }
        sender._conn.send_bytes(dumps(envelope).encode("utf-8"))
    return TaskResult.success(metadata={"untrusted": True})


@pytest.fixture
def pipelines(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    created: list[TaskLite] = []
    stack = ExitStack()

    def create(backend="sqlite") -> TaskLite:
        options = {"state_path": tmp_path / "state.db"} if backend == "sqlite" else {}
        pipeline = TaskLite(backend=backend, **options, max_workers=1)
        (tmp_path / "output").mkdir(exist_ok=True)
        stack.enter_context(authorizer(pipeline, lambda *args: True))
        pipeline.register_resource(CapacityResource("api", max_capacity=1.0))
        pipeline.register_task("child", child_handler)
        created.append(pipeline)
        return pipeline

    try:
        yield create
    finally:
        stack.close()
        for pipeline in created:
            try:
                pipeline.stop()
            finally:
                pipeline.close()
    assert all(not pipeline._runtime.channel.has_active_workers for pipeline in created)
    assert not [t for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")]


class TestRealSubprocessFaultScenarios:
    """真实子进程故障场景：看门狗超时击杀、限速推进与 SystemExit 归因。"""

    def test_real_subprocess_timeout_killed_by_watchdog(self, tmp_path):
        p = TaskLite(backend="sqlite", state_path=tmp_path / "wd.db")
        p.register_task("hung", _hung_handler)
        p.enqueue(Job("hung", "item_watchdog", timeout_seconds=0.2, max_retries_per_run=0))
        p.run(install_signals=False)
        p.stop()

        with StateSnapshot.open(p) as snap:
            rec = snap.get_job("hung::item_watchdog")
            assert rec["status"] == "failed"
            assert "timed out" in (rec["result"]["message"] or "")
            attempts = list(snap.iter_attempts(job_uid="hung::item_watchdog"))
            assert [a["outcome"] for a in attempts] == ["failure"]
        p.close()

    def test_real_subprocess_interval_resource_paces(self, tmp_path):
        p = TaskLite(backend="sqlite", state_path=tmp_path / "pace.db")
        p.register_resource(IntervalResource("api", interval_seconds=0.25))
        p.register_task("gated", _plain_gated_handler, resources={"api": 1.0})
        p.register_task("gated_plain", _plain_gated_handler, resources={"api": 1.0})
        p.enqueue(Job("gated", "holder"))
        p.enqueue(Job("gated_plain", "waiter", depends_on=["gated::holder"]))
        res = p.run(install_signals=False)
        p.stop()

        assert res is None
        with StateSnapshot.open(p) as snap:
            assert snap.get_job("gated::holder")["status"] == "succeeded"
            assert snap.get_job("gated_plain::waiter")["status"] == "succeeded"
        p.close()

    def test_real_subprocess_system_exit_attributed(self, tmp_path):
        p = TaskLite(backend="sqlite", state_path=tmp_path / "sysexit.db")
        p.register_task("sysexit", _system_exit_handler)
        p.enqueue(Job("sysexit", "item_sysexit", max_retries_per_run=0))
        p.run(install_signals=False)
        p.stop()

        with StateSnapshot.open(p) as snap:
            rec = snap.get_job("sysexit::item_sysexit")
            assert rec["status"] == "failed"
            assert "SystemExit" in (rec["result"]["message"] or "")
            attempts = list(snap.iter_attempts(job_uid="sysexit::item_sysexit"))
            assert len(attempts) == 1 and attempts[0]["outcome"] == "failure"
        p.close()


def test_spawn_and_declarations_ignore_directory_environment(tmp_path, monkeypatch):
    ipc_path = tmp_path / "unused" / "ipc"
    monkeypatch.setenv("TASKLITE_IPC_DIR", str(ipc_path))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.chdir(workspace)
    output = workspace / "output"
    output.mkdir()
    with TaskLite(backend="sqlite", state_path=workspace / "state.db", max_workers=1) as pipeline:
        pipeline.register_task("parent", success_handler)
        pipeline.register_task("child", child_handler)
        channel = pipeline._runtime.channel
        reply_senders = []
        execution_ids = []
        original_release = channel.release_spawn

        def capture_reply_sender(prepared):
            handle = original_release(prepared)
            execution_ids.append(handle.execution_id)
            reply_senders.append(channel.get_reply_sender(handle.execution_id))
            return handle

        monkeypatch.setattr(channel, "release_spawn", capture_reply_sender)
        pipeline.enqueue(Job("parent", "parent"))
        pipeline.run(install_signals=False)
        assert len(reply_senders) == 2
        assert all(channel.get_reply_sender(eid) is None for eid in execution_ids)
        assert all(sender._closed and sender._conn.closed for sender in reply_senders)

        with StateSnapshot.open(pipeline) as snap:
            parent = snap.get_job("parent::parent")
            assert parent["result"]["metadata"]["worker_pid"] != os.getpid()
            assert parent["files"]["report"]["path"] == str(output / "published.txt")
            assert parent["files"]["source"]["path"] == str(output / "published.txt")
            assert {r["job_uid"] for r in snap.iter_jobs(status="succeeded")} == {"parent::parent", "child::child"}
            attempt, = snap.iter_attempts(job_uid="parent::parent")
            assert attempt["outcome"] == "success"
        assert pipeline.get_cursor("position") == "42"
        pipeline.stop()
        assert not ipc_path.parent.exists()
        assert not pipeline._runtime.channel.has_active_workers
    assert not [t for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")]


def test_success_transaction_and_reopen_without_execution(pipelines, monkeypatch, tmp_path):
    pipeline = pipelines()
    pipeline.register_task("parent", success_handler)
    pipeline.enqueue(Job("parent", "parent"))
    backend = pipeline._runtime.store.backend
    get_conn = backend._get_conn
    observations = []

    @contextmanager
    def observe_before_commit():
        with get_conn() as connection:
            yield connection
            staged = connection.execute(
                "SELECT metadata FROM successes WHERE job_uid='parent::parent'"
            ).fetchone()
            if staged is not None:
                with sqlite3.connect(backend.path) as outside:
                    visible = outside.execute(
                        "SELECT metadata FROM successes WHERE job_uid='parent::parent'"
                    ).fetchone()
                    if visible is None:
                        assert outside.execute("SELECT value FROM cursors WHERE key='position'").fetchone() is None
                        assert outside.execute("SELECT job_uid FROM queue WHERE job_uid='child::child'").fetchone() is None
                        assert outside.execute("SELECT outcome FROM attempts WHERE job_uid='parent::parent'").fetchone() is None
                if visible is None:
                    assert connection.in_transaction
                    observations.append((
                        loads(staged[0]),
                        connection.execute("SELECT outcome, settled_at FROM attempts WHERE job_uid='parent::parent'").fetchone(),
                        connection.execute("SELECT value FROM cursors WHERE key='position'").fetchone(),
                        connection.execute("SELECT job_uid FROM queue").fetchall(),
                    ))

    monkeypatch.setattr(backend, "_get_conn", observe_before_commit)
    assert pipeline.run(install_signals=False) is None
    assert len(observations) == 1
    metadata, attempt, cursor, queue = observations[0]
    assert metadata["artifacts"]["report"]["path"] == str(tmp_path / "output" / "published.txt")
    assert metadata["inputs"] == [metadata["artifacts"]["report"]["path"]]
    assert attempt[0] == "success" and attempt[1] is not None
    assert cursor == ("42",)
    assert queue == [("child::child",)]
    pid = metadata["result"]["metadata"]["worker_pid"]
    assert pid != os.getpid()
    pipeline.stop()

    reopened = pipelines()
    reopened.register_task("parent", success_handler)
    assert reopened.run(install_signals=False) is None
    with StateSnapshot.open(reopened) as snap:
        assert len(list(snap.iter_attempts(job_uid="parent::parent"))) == 1
        assert snap.get_job("parent::parent")["result"]["metadata"]["worker_pid"] == pid
    assert reopened.get_cursor("position") == "42"
    reopened.enqueue(Job("parent", "parent"))
    assert reopened.run(install_signals=False) is None
    with StateSnapshot.open(reopened) as snap:
        assert [a["outcome"] for a in snap.iter_attempts(job_uid="parent::parent")] == ["success"]


@pytest.mark.parametrize("mode", ["kill", "crash", "timeout"])
def test_process_death_is_single_failure_without_implicit_retry(pipelines, tmp_path, mode):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir(parents=True, exist_ok=True)
    pipeline = pipelines()
    handler = {"kill": interrupted_handler, "crash": crash_handler, "timeout": timeout_handler}[mode]
    pipeline.register_task("work", handler)
    pipeline.enqueue(
        Job(
            "work",
            mode,
            payload={"sync_dir": str(sync_dir)},
            max_retries_per_run=3,
            timeout_seconds=0.25 if mode == "timeout" else 10.0,
        )
    )
    killer = None
    if mode == "kill":
        def kill_ready_worker():
            assert _wait_until(lambda: (sync_dir / "ready").exists())
            handles = pipeline._runtime.channel.active_handles()
            assert len(handles) == 1
            handles[0].process.kill()

        killer = threading.Thread(target=kill_ready_worker)
        killer.start()

    try:
        res = pipeline.run(install_signals=False)
    finally:
        if killer is not None:
            killer.join(timeout=10.0)

    assert res is None
    assert killer is None or not killer.is_alive()
    with StateSnapshot.open(pipeline) as snap:
        rec = snap.get_job("work::" + mode)
        assert rec is not None and rec["status"] == "failed"
        attempts = list(snap.iter_attempts(job_uid="work::" + mode))
        assert len(attempts) == 1 and attempts[0]["attempt_no"] == 1 and attempts[0]["outcome"] == "failure"
        assert list(snap.iter_jobs(status="succeeded")) == []
        result = rec["result"]
    if mode == "kill":
        if os.name == "nt":
            assert "terminated" in result["message"]
            assert result["annotations"]["terminated"] is True
            assert result["annotations"]["exitcode"] == 1
        else:
            assert "signal" in result["message"]
            assert result["annotations"]["exitcode"] == -9
    elif mode == "timeout":
        assert "timed out" in result["message"]
        assert result["annotations"]["timed_out"] is True
    else:
        assert "code 7" in result["message"]
        assert result["annotations"]["exitcode"] == 7
    assert result["metadata"] == {}
    assert result["exception"] is None
    assert "oom_hint" not in result["annotations"]
    if mode == "kill":
        assert not (tmp_path / "output" / "authorized.txt").exists()
        assert (tmp_path / "output" / "retained.txt").exists()


def test_cleanup_error_fails_without_publishing_success(pipelines, monkeypatch, tmp_path):
    pipeline = pipelines()
    pipeline.register_task("work", cleanup_handler)
    pipeline.enqueue(Job("work", "cleanup", max_retries_per_run=3))

    with cleanup_hook(pipeline, PermissionError("授权清理不可用")):
        pipeline.run(install_signals=False)
    with StateSnapshot.open(pipeline) as snap:
        assert list(snap.iter_jobs(status="succeeded")) == []
        failed = snap.get_job("work::cleanup")
        assert failed["status"] == "failed"
        assert "授权清理不可用" in (failed["result"]["message"] or "")
        assert list(snap.iter_jobs(status="pending")) == []
        attempt, = snap.iter_attempts(job_uid="work::cleanup")
        assert attempt["outcome"] == "failure"
    assert (tmp_path / "output" / "temporary.txt").exists()


def test_stop_drains_received_messages_and_workers(pipelines, monkeypatch, tmp_path):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir(parents=True, exist_ok=True)
    pipeline = pipelines("sqlite")
    pipeline.register_task("controlled", controlled_handler)
    pipeline.enqueue(Job("controlled", "active", payload={"sync_dir": str(sync_dir)}))
    pipeline.enqueue(Job("child", "waiting"))
    channel = pipeline._runtime.channel
    requested: list[str] = []
    orig_reap = channel.reap_completed

    def reap_with_stop_trigger(handles):
        for handle in handles:
            channel._receive_available(handle)
            if (
                not requested
                and (sync_dir / "ready").exists()
                and len(pipeline._runtime._in_flight[handle.job_uid].active_files) == 3
            ):
                requested.append(handle.execution_id or "")
                pipeline.stop()
                (sync_dir / "release").write_text("go", encoding="utf-8")
        return orig_reap(handles)

    monkeypatch.setattr(channel, "reap_completed", reap_with_stop_trigger)
    assert pipeline.run(install_signals=False) is None
    assert requested
    assert not (tmp_path / "output" / "temporary.txt").exists()
    assert (tmp_path / "output" / "kept.txt").exists()
    with StateSnapshot.open(pipeline) as snap:
        assert [j["job_uid"] for j in snap.iter_jobs(status="pending")] == ["child::waiting"]
        active = snap.get_job("controlled::active")
        assert active["status"] == "succeeded"
        assert active["files"]["source"]["path"] == str(tmp_path / "output" / "kept.txt")


def test_stop_commits_complete_result_already_received(pipelines, monkeypatch):
    pipeline = pipelines()
    pipeline.enqueue(Job("child", "done"))
    channel = pipeline._runtime.channel
    orig_accept = channel._accept_message

    def accept_and_stop(handle, msg):
        orig_accept(handle, msg)
        if msg.kind == "result":
            pipeline.stop()

    monkeypatch.setattr(channel, "_accept_message", accept_and_stop)
    assert pipeline.run(install_signals=False) is None
    with StateSnapshot.open(pipeline) as snap:
        rec = snap.get_job("child::done")
        assert rec["status"] == "succeeded"
        assert rec["result"]["metadata"]["child"] is True
        assert list(snap.iter_jobs(status="pending")) == []
        assert [a["outcome"] for a in snap.iter_attempts(job_uid="child::done")] == ["success"]


@pytest.mark.parametrize(
    "backend,fault",
    [
        ("memory", "json"),
        ("memory", "kind"),
        ("memory", "job_uid"),
        ("memory", "execution_id"),
        ("sqlite", "json"),
    ],
)
def test_invalid_message_cannot_register_success(pipelines, tmp_path, fault, backend):
    pipeline = pipelines(backend)
    pipeline.register_task("invalid", invalid_frame_handler)
    pipeline.enqueue(Job("invalid", "invalid", payload={"fault": fault}))
    with pytest.raises(ValueError):
        pipeline.run(install_signals=False)
    with StateSnapshot.open(pipeline) as snap:
        assert list(snap.iter_jobs(status="succeeded")) == []
        assert list(snap.iter_jobs(status="failed")) == []
        row = snap.get_job("invalid::invalid")
        assert row["status"] == "running" and row["attempt_no"] == 1
        assert list(snap.iter_attempts(job_uid="invalid::invalid")) == []
    assert (tmp_path / "output" / "authorized.txt").exists()
    assert (tmp_path / "output" / "retained.txt").exists()

    pipeline.run(install_signals=False)
    with StateSnapshot.open(pipeline) as snap:
        attempt, = snap.iter_attempts(job_uid="invalid::invalid")
        assert attempt["outcome"] == "failure"
        assert attempt["run_id"] == row["run_id"]
        assert attempt["attempt_no"] == row["attempt_no"]
        assert list(snap.iter_jobs(status="pending")) == []
        assert list(snap.iter_jobs(status="succeeded")) == []
        assert snap.get_job("invalid::invalid")["status"] == "failed"
    assert (tmp_path / "output" / "authorized.txt").exists()
    assert (tmp_path / "output" / "retained.txt").exists()
    pipeline.run(install_signals=False)
    with StateSnapshot.open(pipeline) as snap:
        assert list(snap.iter_attempts(job_uid="invalid::invalid")) == [attempt]


def _is_pid_alive(pid: int) -> bool:
    """跨平台判定指定 PID 是否仍在运行。"""
    if pid <= 0:
        return False
    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetExitCodeProcess.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL

    proc_handle = kernel.OpenProcess(0x1000, False, pid)
    if not proc_handle:
        return False
    try:
        exit_code = wintypes.DWORD()
        if not kernel.GetExitCodeProcess(proc_handle, ctypes.byref(exit_code)):
            return False
        return int(exit_code.value) == 259
    finally:
        kernel.CloseHandle(proc_handle)


def spawn_tree_handler(job: Job, ctx) -> TaskResult:
    """派生两层后代进程（worker -> child -> grandchild），写出全部 PID 后阻塞等待。"""
    sync_dir = Path(job.payload["sync_dir"])
    grandchild_code = (
        "import os, pathlib, time; "
        f"pathlib.Path({str(sync_dir / 'grandchild.pid')!r}).write_text(str(os.getpid()), encoding='utf-8'); "
        "time.sleep(60)"
    )
    child_code = (
        "import os, pathlib, subprocess, sys, time; "
        f"pathlib.Path({str(sync_dir / 'child.pid')!r}).write_text(str(os.getpid()), encoding='utf-8'); "
        f"subprocess.Popen([sys.executable, '-c', {grandchild_code!r}]); "
        "time.sleep(60)"
    )
    (sync_dir / "worker.pid").write_text(str(os.getpid()), encoding="utf-8")
    subprocess.Popen([sys.executable, "-c", child_code])
    time.sleep(60)
    return TaskResult.success()


@pytest.mark.parametrize("mode", ["timeout", "abort"])
def test_multilevel_descendant_tree_cleaned_on_timeout_and_abort(
    pipelines, monkeypatch, tmp_path, mode
):
    """超时与异常中止均必须原子回收 worker 及其多层派生孙进程树，零残留孤儿。"""
    sync_dir = tmp_path / f"tree_{mode}"
    sync_dir.mkdir(parents=True, exist_ok=True)
    pipeline = pipelines("sqlite")
    pipeline.register_task("tree", spawn_tree_handler)
    pipeline.enqueue(
        Job(
            "tree",
            mode,
            payload={"sync_dir": str(sync_dir)},
            timeout_seconds=1.5 if mode == "timeout" else 30.0,
            max_retries_per_run=0,
        )
    )

    if mode == "abort":
        channel = pipeline._runtime.channel
        original_reap = channel.reap_completed
        triggered = False

        def trigger_abort_once(handles):
            nonlocal triggered
            if not triggered and (sync_dir / "grandchild.pid").exists():
                triggered = True
                raise KeyboardInterrupt("simulated scheduler abort")
            return original_reap(handles)

        monkeypatch.setattr(channel, "reap_completed", trigger_abort_once)
        with pytest.raises(KeyboardInterrupt):
            pipeline.run(install_signals=False)
    else:
        pipeline.run(install_signals=False)

    worker_pid = int((sync_dir / "worker.pid").read_text(encoding="utf-8").strip())
    child_pid = int((sync_dir / "child.pid").read_text(encoding="utf-8").strip())
    grandchild_pid = int((sync_dir / "grandchild.pid").read_text(encoding="utf-8").strip())
    assert len({os.getpid(), worker_pid, child_pid, grandchild_pid}) == 4

    assert _wait_until(
        lambda: (
            not _is_pid_alive(worker_pid)
            and not _is_pid_alive(child_pid)
            and not _is_pid_alive(grandchild_pid)
        ),
        timeout=10.0,
    ), (
        f"进程树未彻底回收: worker={_is_pid_alive(worker_pid)}, "
        f"child={_is_pid_alive(child_pid)}, grandchild={_is_pid_alive(grandchild_pid)}"
    )
    assert not pipeline._runtime.channel.has_active_workers


@pytest.mark.skipif(os.name != "nt", reason="Job Object 绑定校验仅在 Windows 平台执行")
def test_windows_job_object_assign_failure_fails_loud_and_kills_shell():
    """Windows 下 AssignProcessToJobObject 失败必须立即强杀空壳、关闭 hJob 并抛 OSError。"""
    from tasklite.engine.launcher import WindowsProcessTreeAdapter, _get_win_kernel

    real_kernel, ext_limit_cls = _get_win_kernel()
    closed_handles: list[int] = []

    class _FailingAssignKernel:
        def CreateJobObjectW(self, sec, name):
            return real_kernel.CreateJobObjectW(sec, name)

        def SetInformationJobObject(self, h_job, info_class, ptr, size):
            return real_kernel.SetInformationJobObject(h_job, info_class, ptr, size)

        def AssignProcessToJobObject(self, h_job, h_proc):
            ctypes.set_last_error(5)  # ERROR_ACCESS_DENIED
            return 0

        def CloseHandle(self, h_job):
            closed_handles.append(int(h_job))
            return real_kernel.CloseHandle(h_job)

    adapter = WindowsProcessTreeAdapter(
        kernel=_FailingAssignKernel(),
        extended_limit_cls=ext_limit_cls,
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        **adapter.popen_kwargs(),
    )
    try:
        with pytest.raises(OSError, match="AssignProcessToJobObject"):
            adapter.attach_and_wrap(proc)
        assert len(closed_handles) == 1
        assert proc.poll() is not None
        assert _wait_until(lambda: not _is_pid_alive(proc.pid), timeout=5.0)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)


@pytest.mark.skipif(os.name != "nt", reason="Job Object 跳板进程绑定校验仅在 Windows 平台执行")
def test_windows_job_object_assign_failure_kills_trampoline_and_real_worker(tmp_path):
    """Windows 跳板进程（worker_pid != proc.pid）绑定 Job 失败时须同时强杀真实 worker 与跳板壳。"""
    from tasklite.engine.launcher import WindowsProcessTreeAdapter, _get_win_kernel

    real_kernel, ext_limit_cls = _get_win_kernel()
    closed_handles: list[int] = []
    terminated_procs: list[int] = []

    class _FailingWorkerAssignKernel:
        def CreateJobObjectW(self, sec, name):
            return real_kernel.CreateJobObjectW(sec, name)

        def SetInformationJobObject(self, h_job, info_class, ptr, size):
            return real_kernel.SetInformationJobObject(h_job, info_class, ptr, size)

        def OpenProcess(self, access, inherit, pid):
            return real_kernel.OpenProcess(access, inherit, pid)

        def AssignProcessToJobObject(self, h_job, h_proc):
            ctypes.set_last_error(5)  # ERROR_ACCESS_DENIED
            return 0

        def TerminateProcess(self, h_proc, exit_code):
            terminated_procs.append(int(h_proc))
            return real_kernel.TerminateProcess(h_proc, exit_code)

        def TerminateJobObject(self, h_job, exit_code):
            return real_kernel.TerminateJobObject(h_job, exit_code)

        def CloseHandle(self, h_obj):
            closed_handles.append(int(h_obj))
            return real_kernel.CloseHandle(h_obj)

    adapter = WindowsProcessTreeAdapter(
        kernel=_FailingWorkerAssignKernel(),
        extended_limit_cls=ext_limit_cls,
    )
    real_worker_pid_file = tmp_path / "real_worker.pid"
    real_worker_code = (
        "import os, pathlib, time; "
        f"pathlib.Path({str(real_worker_pid_file)!r}).write_text(str(os.getpid()), encoding='utf-8'); "
        "time.sleep(60)"
    )
    trampoline_code = (
        "import subprocess, sys, time; "
        f"subprocess.Popen([sys.executable, '-c', {real_worker_code!r}]); "
        "time.sleep(60)"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", trampoline_code],
        **adapter.popen_kwargs(),
    )
    real_worker_pid = 0
    try:
        assert _wait_until(lambda: real_worker_pid_file.exists(), timeout=10.0)
        real_worker_pid = int(real_worker_pid_file.read_text(encoding="utf-8").strip())
        assert real_worker_pid != proc.pid
        assert _is_pid_alive(real_worker_pid)

        with pytest.raises(OSError, match="AssignProcessToJobObject"):
            adapter.attach_and_wrap(proc, worker_pid=real_worker_pid)

        assert len(terminated_procs) == 1
        assert len(closed_handles) >= 2  # h_worker_proc 与 h_worker_job 均已关闭
        assert _wait_until(
            lambda: not _is_pid_alive(proc.pid) and not _is_pid_alive(real_worker_pid),
            timeout=5.0,
        ), (
            f"绑定失败后仍有残留进程: trampoline={_is_pid_alive(proc.pid)}, "
            f"real_worker={_is_pid_alive(real_worker_pid)}"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)
        if real_worker_pid > 0 and _is_pid_alive(real_worker_pid):
            h = real_kernel.OpenProcess(0x0001, False, real_worker_pid)
            if h:
                real_kernel.TerminateProcess(h, 1)
                real_kernel.CloseHandle(h)


def test_worker_bootstrap_fork_bomb_guard_reports_structured_failure(tmp_path):
    """未写 if __name__ == '__main__' 守卫的脚本在子进程引导时触发重入守卫并结构化失败。"""
    state_path = tmp_path / "guard.db"
    enqueued_marker = tmp_path / "enqueued"
    marker_path = tmp_path / "parent_done"
    project_root = Path(__file__).resolve().parents[2]
    script = tmp_path / "unguarded_script.py"
    script.write_text(
        f"""
from pathlib import Path
from tasklite import Job, TaskLite, TaskResult


def top_handler(job, ctx):
    return TaskResult.success()


pipeline = TaskLite(backend="sqlite", state_path={str(state_path)!r}, max_workers=1)
pipeline.register_task("work", top_handler)
enqueued_marker = Path({str(enqueued_marker)!r})
if not enqueued_marker.exists():
    enqueued_marker.write_text("1", encoding="utf-8")
    pipeline.enqueue(Job("work", "j1", max_retries_per_run=0))
pipeline.run(install_signals=False)
pipeline.close()
Path({str(marker_path)!r}).write_text("done", encoding="utf-8")
""",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)
    completed = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, f"stdout={completed.stdout}\nstderr={completed.stderr}"
    assert marker_path.exists()
    with StateSnapshot.open(state_path) as snap:
        rec = snap.get_job("work::j1")
        assert rec is not None
        assert rec["status"] == "failed"
        message = rec["result"]["message"] or ""
        assert "Worker bootstrap failed" in message
        assert "Task handler" in message
        assert rec["result"]["exception"]["type"] == "RuntimeError"
