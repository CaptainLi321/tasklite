"""瞬时 Worker Pipe 执行生命周期多进程端到端集成测试。

覆盖真实 spawn 子进程下：
- 原子成功事务（inputs / 具名 artifacts / spawned_jobs / cursor /
  动态声明 / attempts）与重开恢复、once 去重跳过；
- 子进程被信号击杀（SIGKILL）、异常退出（os._exit）、看门狗超时的
  单次失败与产物清理；
- 产物清理 PermissionError 阻断终态提交并保留原队列快照；
- 正常停止排空与框架异常退出后的遗留执行恢复；
- 非法管道帧（损坏 JSON / 未知 kind / job_uid 不匹配 / execution_id 不匹配）
  的零伪成功防御与遗留 running 恢复。
"""

from __future__ import annotations

from contextlib import contextmanager
import multiprocessing as mp
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time

import pytest

from tasklite import FileSpec, Job, TaskLite, TaskResult
from tasklite.resource import CapacityResource
from tasklite.utils.artifact import ArtifactPathPolicy
from tasklite.utils.jsonutil import dumps, loads


def _wait_until(predicate, timeout: float = 10.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


def success_handler(job: Job, ctx) -> TaskResult:
    output = Path(ctx.declare_file("report", FileSpec(ctx.output_roots / "published.txt", role="output")))
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
        path = Path(ctx.declare_file(filename, FileSpec(ctx.output_roots / filename, role="output", delete_on_failure=allowed)))
        path.write_text("半成品", encoding="utf-8")
    sync_dir = Path(job.payload["sync_dir"])
    (sync_dir / "ready").write_text(str(os.getpid()), encoding="utf-8")
    time.sleep(30.0)
    return TaskResult.success(metadata={"unreachable": True})


def crash_handler(job: Job, ctx) -> TaskResult:
    os._exit(7)


def timeout_handler(job: Job, ctx) -> TaskResult:
    time.sleep(30.0)
    return TaskResult.success(metadata={"unreachable": True})


def cleanup_handler(job: Job, ctx) -> TaskResult:
    path = Path(ctx.declare_file("temporary", FileSpec(ctx.output_roots / "temporary.txt", role="cache", delete_on_success=True, delete_on_failure=True)))
    path.write_text("临时产物", encoding="utf-8")
    return TaskResult.success(metadata={"uncommitted": True})


def controlled_handler(job: Job, ctx) -> TaskResult:
    sync_dir = Path(job.payload["sync_dir"])
    kept = Path(ctx.declare_file("kept", FileSpec(ctx.output_roots / "kept.txt", role="output")))
    kept.write_text("保留产物", encoding="utf-8")
    temporary = Path(ctx.declare_file("temporary", FileSpec(ctx.output_roots / "temporary.txt", role="cache", delete_on_success=True, delete_on_failure=True)))
    temporary.write_text("临时产物", encoding="utf-8")
    (sync_dir / "ready").write_text(str(os.getpid()), encoding="utf-8")
    ctx.declare_file("source", FileSpec(kept, role="input"))
    assert not hasattr(ctx, "suspend_resource")
    assert _wait_until(lambda: (sync_dir / "release").exists(), timeout=20.0)
    return TaskResult.success(metadata={"completed": True})


def invalid_frame_handler(job: Job, ctx) -> TaskResult:
    for filename, allowed in (("authorized.txt", True), ("retained.txt", False)):
        path = Path(ctx.declare_file(filename, FileSpec(ctx.output_roots / filename, role="output", delete_on_failure=allowed)))
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
def pipelines(tmp_path):
    created: list[TaskLite] = []

    def create(backend="sqlite") -> TaskLite:
        options = {"state_path": tmp_path / "state.db"} if backend == "sqlite" else {}
        pipeline = TaskLite(
            backend=backend,
            **options,
            output_roots=tmp_path / "output",
            max_workers=1,
        )
        (tmp_path / "output").mkdir(exist_ok=True)
        pipeline.authorizer = lambda *args: True
        pipeline.register_resource(CapacityResource("api", max_capacity=1.0))
        pipeline.register_task("child", child_handler)
        created.append(pipeline)
        return pipeline

    yield create
    for pipeline in created:
        try:
            pipeline.stop()
        finally:
            pipeline.close()
    assert not mp.active_children()
    active_recv_threads = [
        t for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")
    ]
    assert not active_recv_threads


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_spawn_and_declarations_ignore_directory_environment(tmp_path, monkeypatch, backend):
    ipc_path = tmp_path / "unused" / "ipc"
    monkeypatch.setenv("TASKLITE_IPC_DIR", str(ipc_path))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.chdir(workspace)
    output = workspace / "output"
    output.mkdir()
    options = {"state_path": workspace / "state.db"} if backend == "sqlite" else {}
    with TaskLite(backend=backend, output_roots=output, max_workers=1, **options) as pipeline:
        pipeline.register_task("parent", success_handler)
        pipeline.register_task("child", child_handler)
        pipeline.enqueue(Job("parent", "parent"))
        pipeline.run()
        state = pipeline._runtime.store.backend
        success = state.load_successes()["parent::parent"]
        assert success["result"]["metadata"]["worker_pid"] != os.getpid()
        assert success["inputs"] == [str(output / "published.txt")]
        assert success["artifacts"]["report"]["path"] == str(output / "published.txt")
        assert state.load_cursors()["position"] == "42"
        assert set(state.load_successes()) == {"parent::parent", "child::child"}
        attempt, = state.load_settled_attempts("parent::parent")
        assert attempt["outcome"] == "success"
        pipeline.stop()
        assert pipeline._runtime.channel._handles == {}
        assert pipeline._runtime.channel._receivers == {}
        assert not ipc_path.parent.exists()
    assert not ipc_path.parent.exists()
    assert not mp.active_children()
    assert not [t for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")]
    if backend == "memory":
        assert set(workspace.iterdir()) == {output}


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
                    if visible is None and staged is not None:
                        assert (
                            outside.execute(
                                "SELECT value FROM cursors WHERE key='position'"
                            ).fetchone()
                            is None
                        )
                        assert (
                            outside.execute(
                                "SELECT job_uid FROM queue WHERE job_uid='child::child'"
                            ).fetchone()
                            is None
                        )
                        expected_outside = None
                        assert outside.execute(
                            "SELECT outcome FROM attempts WHERE job_uid='parent::parent'"
                        ).fetchone() == expected_outside
                if visible is None:
                    assert connection.in_transaction
                    observations.append((
                        loads(staged[0]),
                        connection.execute(
                            "SELECT outcome, settled_at FROM attempts WHERE job_uid='parent::parent'"
                        ).fetchone(),
                        connection.execute(
                            "SELECT value FROM cursors WHERE key='position'"
                        ).fetchone(),
                        connection.execute("SELECT job_uid FROM queue").fetchall(),
                    ))

    monkeypatch.setattr(backend, "_get_conn", observe_before_commit)
    res = pipeline.run()
    assert res is None
    assert len(observations) == 1
    metadata, attempt, cursor, queue = observations[0]
    assert metadata["artifacts"]["report"]["path"] == str(
        tmp_path / "output" / "published.txt"
    )
    assert metadata["inputs"] == [metadata["artifacts"]["report"]["path"]]
    assert attempt[0] == "success" and attempt[1] is not None
    assert cursor == ("42",)
    assert queue == [("child::child",)]
    assert backend.load_queue() == []
    pid = metadata["result"]["metadata"]["worker_pid"]
    assert pid != os.getpid()
    pipeline.stop()

    reopened = pipelines()
    reopened.register_task("parent", success_handler)
    result = reopened.run()
    assert result is None
    assert len(reopened._runtime.store.backend.load_settled_attempts("parent::parent")) == 1
    assert reopened._runtime.store.backend.load_successes()["parent::parent"]["result"]["metadata"]["worker_pid"] == pid
    assert reopened._runtime.store.backend.load_cursors()["position"] == "42"
    reopened.enqueue(Job("parent", "parent"))
    assert reopened.run() is None
    expected_attempts = ["success"]
    assert [a["outcome"] for a in reopened._runtime.store.backend.load_settled_attempts("parent::parent")] == expected_attempts


@pytest.mark.parametrize("mode", ["kill", "crash", "timeout"])
def test_process_death_is_single_failure_without_implicit_retry(
    pipelines, tmp_path, mode
):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir(parents=True, exist_ok=True)
    pipeline = pipelines()
    handler = {
        "kill": interrupted_handler,
        "crash": crash_handler,
        "timeout": timeout_handler,
    }[mode]
    pipeline.register_task("work", handler)
    pipeline.enqueue(
        Job(
            "work",
            mode,
            payload={"sync_dir": str(sync_dir)},
            max_retries_per_run=3,
            timeout_seconds=0.3 if mode == "timeout" else 10.0,
        )
    )
    killer = None
    if mode == "kill":
        def kill_ready_worker():
            assert _wait_until(lambda: (sync_dir / "ready").exists())
            handles = list(pipeline._runtime.channel._handles.values())
            assert len(handles) == 1
            handles[0].process.kill()

        killer = threading.Thread(target=kill_ready_worker)
        killer.start()

    try:
        res = pipeline.run()
    finally:
        if killer is not None:
            killer.join(timeout=10.0)

    assert res is None
    assert killer is None or not killer.is_alive()
    failed = pipeline._runtime.store.backend.load_failed()
    assert list(failed) == ["work::" + mode]
    attempts = pipeline._runtime.store.backend.load_settled_attempts("work::" + mode)
    assert len(attempts) == 1
    assert attempts[0]["attempt_no"] == 1
    assert attempts[0]["outcome"] == "failure"
    assert pipeline._runtime.store.backend.load_successes() == {}
    expected = {
        "kill": "signal",
        "crash": "code 7",
        "timeout": "timed out",
    }[mode]
    result = failed["work::" + mode]["result"]
    assert expected in result["message"]
    assert result["metadata"] == {}
    assert result["exception"] is None
    assert "oom_hint" not in result["annotations"]
    if mode == "timeout":
        assert result["annotations"]["timed_out"] is True
    else:
        assert result["annotations"]["exitcode"] == (-9 if mode == "kill" else 7)
    if mode == "kill":
        assert not (tmp_path / "output" / "authorized.txt").exists()
        assert (tmp_path / "output" / "retained.txt").exists()
    assert pipeline._runtime.store.backend.load_queue() == []


def test_cleanup_error_fails_without_publishing_success(pipelines, monkeypatch, tmp_path):
    pipeline = pipelines()
    pipeline.register_task("work", cleanup_handler)
    pipeline.enqueue(Job("work", "cleanup", max_retries_per_run=3))

    def unavailable(self, declaration, *, success):
        raise PermissionError("授权清理不可用")

    with monkeypatch.context() as failure:
        failure.setattr(ArtifactPathPolicy, "cleanup", unavailable)
        pipeline.run()
    assert pipeline._runtime.store.backend.load_successes() == {}
    failed = pipeline._runtime.store.backend.load_failed()["work::cleanup"]
    assert "授权清理不可用" in failed["error_message"]
    assert pipeline._runtime.store.backend.load_queue_jobs() == []
    attempt, = pipeline._runtime.store.backend.load_settled_attempts("work::cleanup")
    assert attempt["outcome"] == "failure"
    assert (tmp_path / "output" / "temporary.txt").exists()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_stop_drains_received_messages_and_workers(pipelines, monkeypatch, tmp_path, backend):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir(parents=True, exist_ok=True)
    pipeline = pipelines(backend)
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
    res = pipeline.run()
    assert res is None
    assert requested
    queue = pipeline._runtime.store.backend.load_queue()
    assert not (tmp_path / "output" / "temporary.txt").exists()
    assert (tmp_path / "output" / "kept.txt").exists()
    assert [j["job_id"] for j in queue] == ["waiting"]
    assert pipeline._runtime.store.backend.load_successes()["controlled::active"]["inputs"] == [
        str(tmp_path / "output" / "kept.txt")
    ]


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
    res = pipeline.run()
    assert res is None
    assert pipeline._runtime.store.backend.load_successes()["child::done"]["result"]["metadata"]["child"] is True
    assert pipeline._runtime.store.backend.load_queue() == []
    assert pipeline._runtime.store.backend.load_settled_attempts("child::done")[0]["outcome"] == "success"


@pytest.mark.parametrize("fault", ["json", "kind", "job_uid", "execution_id"])
@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_invalid_message_cannot_register_success(pipelines, tmp_path, fault, backend):
    pipeline = pipelines(backend)
    pipeline.register_task("invalid", invalid_frame_handler)
    pipeline.enqueue(Job("invalid", "invalid", payload={"fault": fault}))
    with pytest.raises(ValueError):
        pipeline.run()
    assert pipeline._runtime.store.backend.load_successes() == {}
    assert pipeline._runtime.store.backend.load_failed() == {}
    row, = pipeline._runtime.store.backend.load_queue_jobs()
    assert row["status"] == "running" and row["attempt_no"] == 1
    assert pipeline._runtime.store.backend.load_settled_attempts("invalid::invalid") == []
    assert (tmp_path / "output" / "authorized.txt").exists()
    assert (tmp_path / "output" / "retained.txt").exists()
    pipeline.run()
    attempt, = pipeline._runtime.store.backend.load_settled_attempts("invalid::invalid")
    assert attempt["outcome"] == "failure"
    assert attempt["run_id"] == row["run_id"]
    assert attempt["attempt_no"] == row["attempt_no"]
    assert pipeline._runtime.store.backend.load_queue_jobs() == []
    assert pipeline._runtime.store.backend.load_successes() == {}
    assert "invalid::invalid" in pipeline._runtime.store.backend.load_failed()
    assert (tmp_path / "output" / "authorized.txt").exists()
    assert (tmp_path / "output" / "retained.txt").exists()
    pipeline.run()
    assert pipeline._runtime.store.backend.load_settled_attempts("invalid::invalid") == [attempt]
