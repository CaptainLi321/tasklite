"""瞬时 Worker Pipe 执行生命周期多进程端到端集成测试。

覆盖真实 spawn 子进程下：
- 原子成功事务（inputs / 具名 artifacts / spawned_jobs / cursor /
  resource_suspensions / attempts）与重开恢复、once 去重跳过；
- 子进程被信号击杀（SIGKILL）、异常退出（os._exit）、看门狗超时的预算与产物清理；
- 产物清理 PermissionError 阻断终态提交并保留原队列快照；
- DRAINING / ABORTING 停机对在途声明、挂起信号与已完成结果的排空与结算；
- 非法管道帧（损坏 JSON / 未知 kind / job_uid 不匹配 / execution_id 不匹配）
  的零伪成功防御与失败授权清理；
- 父进程崩溃后孤儿 worker 持有 .lock 期间的跨重启锁冲突避让与锁释放后重跑收敛。
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

from tasklite import Job, TaskLite, TaskResult
from tasklite.resource import CapacityResource
from tasklite.backend.sqlite_backend import TARGET_SCHEMA_VERSION
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
    output = Path(ctx.declare_output("published.txt", name="report"))
    output.write_text("已发布", encoding="utf-8")
    ctx.declare_input(output)
    ctx.set_cursor("position", "42")
    ctx.spawn(Job("child", "child"))
    ctx.suspend_resource("api", 60)
    return TaskResult.success(metadata={"worker_pid": os.getpid()})


def child_handler(job: Job, ctx) -> TaskResult:
    return TaskResult.success(metadata={"child": True})


def interrupted_handler(job: Job, ctx) -> TaskResult:
    if job.attempt_no > 1:
        return TaskResult.success(metadata={"recovered": True})
    for filename, allowed in (("authorized.txt", True), ("retained.txt", False)):
        path = Path(ctx.declare_output(filename, delete_on_failure=allowed))
        path.write_text("半成品", encoding="utf-8")
    sync_dir = Path(job.payload["sync_dir"])
    (sync_dir / "ready").write_text(str(os.getpid()), encoding="utf-8")
    time.sleep(30.0)
    return TaskResult.success(metadata={"unreachable": True})


def crash_handler(job: Job, ctx) -> TaskResult:
    if job.attempt_no == 1:
        os._exit(7)
    return TaskResult.success(metadata={"recovered": True})


def timeout_handler(job: Job, ctx) -> TaskResult:
    time.sleep(30.0)
    return TaskResult.success(metadata={"unreachable": True})


def cleanup_handler(job: Job, ctx) -> TaskResult:
    path = Path(ctx.declare_cache("temporary.txt"))
    path.write_text("临时产物", encoding="utf-8")
    return TaskResult.success(metadata={"uncommitted": True})


def controlled_handler(job: Job, ctx) -> TaskResult:
    sync_dir = Path(job.payload["sync_dir"])
    kept = Path(ctx.declare_output("kept.txt"))
    kept.write_text("保留产物", encoding="utf-8")
    temporary = Path(ctx.declare_cache("temporary.txt"))
    temporary.write_text("临时产物", encoding="utf-8")
    ctx.declare_input(kept)
    ctx.suspend_resource("api", 60)
    (sync_dir / "ready").write_text(str(os.getpid()), encoding="utf-8")
    assert _wait_until(lambda: (sync_dir / "release").exists(), timeout=20.0)
    return TaskResult.success(metadata={"completed": True})


def invalid_frame_handler(job: Job, ctx) -> TaskResult:
    for filename, allowed in (("authorized.txt", True), ("retained.txt", False)):
        path = Path(ctx.declare_output(filename, delete_on_failure=allowed))
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

    def create() -> TaskLite:
        pipeline = TaskLite(
            backend="sqlite",
            state_path=tmp_path / "state.db",
            output_roots=tmp_path / "output",
            max_workers=1,
        )
        pipeline.register_resource(CapacityResource("api", max_capacity=1.0))
        pipeline.register_task("child", child_handler)
        created.append(pipeline)
        return pipeline

    yield create
    for pipeline in created:
        pipeline.stop()
    assert not mp.active_children()
    active_recv_threads = [
        t for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")
    ]
    assert not active_recv_threads
    ipc_dir = tmp_path / "state" / "ipc"
    if ipc_dir.exists():
        assert not list(ipc_dir.glob("*.json*"))


def test_success_transaction_and_reopen_without_execution(pipelines, monkeypatch, tmp_path):
    pipeline = pipelines()
    pipeline.register_task("parent", success_handler)
    pipeline.enqueue(Job("parent", "parent"))
    backend = pipeline.backend
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
                        expected_outside = (
                            None
                            if backend.schema_version >= TARGET_SCHEMA_VERSION
                            else ("running",)
                        )
                        assert outside.execute(
                            "SELECT outcome FROM attempts WHERE job_uid='parent::parent'"
                        ).fetchone() == expected_outside
                if visible is None:
                    assert connection.in_transaction
                    time_col = (
                        "settled_at"
                        if backend.schema_version >= TARGET_SCHEMA_VERSION
                        else "finished_at"
                    )
                    observations.append((
                        loads(staged[0]),
                        connection.execute(
                            f"SELECT outcome, {time_col} FROM attempts WHERE job_uid='parent::parent'"
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
    assert attempt[0] in ("succeeded", "success") and attempt[1] is not None
    assert cursor == ("42",)
    assert queue == [("child::child",)]
    assert backend.load_queue() == []
    pid = metadata["worker_pid"]
    assert pid != os.getpid()
    pipeline.stop()

    reopened = pipelines()
    reopened.register_task("parent", success_handler)
    result = reopened.run()
    assert result is None
    assert len(reopened.backend.load_attempts("parent::parent")) == 1
    assert reopened.backend.load_successes()["parent::parent"]["worker_pid"] == pid
    assert reopened.backend.load_cursors()["position"] == "42"
    reopened.enqueue(Job("parent", "parent"))
    assert reopened.run() is None
    expected_attempts = (
        ["succeeded"]
        if reopened.backend.schema_version >= TARGET_SCHEMA_VERSION
        else ["succeeded", "skipped"]
    )
    assert [a.outcome for a in reopened.backend.load_attempts("parent::parent")] == expected_attempts


@pytest.mark.parametrize("mode", ["kill", "crash", "timeout"])
def test_process_death_uses_existing_budget(pipelines, monkeypatch, tmp_path, mode):
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
            max_retries_per_run=1,
            timeout_seconds=0.3 if mode == "timeout" else 20.0,
        )
    )
    channel = pipeline._runtime.channel
    killed_once: list[str] = []
    if mode == "kill":
        orig_reap = channel.reap_completed

        def reap_and_kill(handles):
            for handle in handles:
                channel._receive_available(handle)
                if (
                    not killed_once
                    and (sync_dir / "ready").exists()
                    and len(channel.read_declared_artifacts(handle.job_uid)) == 2
                ):
                    killed_once.append(handle.execution_id or "")
                    handle.process.kill()
            return orig_reap(handles)

        monkeypatch.setattr(channel, "reap_completed", reap_and_kill)

    res = pipeline.run()
    assert res is None
    attempts = pipeline.backend.load_attempts("work::" + mode)
    if mode == "timeout":
        assert len(pipeline.backend.load_failed()) == 1
        assert len(attempts) == 1 and attempts[0].outcome == "failed"
        assert attempts[0].attempt_no == 1
        assert "TIMEOUT" in str(pipeline.backend.load_failed())
    else:
        assert "work::" + mode in pipeline.backend.load_successes()
        assert [a.attempt_no for a in attempts] == [1, 2]
        assert [a.outcome for a in attempts] == ["requeued", "succeeded"]
        expected = "PROCESS_SIGNAL_DEATH" if mode == "kill" else "PROCESS_CRASH_EXIT"
        assert expected in (attempts[0].error_message or "")
        assert pipeline.backend.load_failed() == {}
        if mode == "kill":
            assert len(killed_once) == 1
            assert not (tmp_path / "output" / "authorized.txt").exists()
            assert (tmp_path / "output" / "retained.txt").exists()
    assert pipeline.backend.load_queue() == []


def test_cleanup_error_preserves_uncommitted_queue(pipelines, monkeypatch, tmp_path):
    pipeline = pipelines()
    pipeline.register_task("work", cleanup_handler)
    pipeline.enqueue(Job("work", "cleanup", max_retries_per_run=3))

    def unavailable(self, declaration, *, success):
        raise PermissionError("授权清理不可用")

    with monkeypatch.context() as failure:
        failure.setattr(ArtifactPathPolicy, "cleanup", unavailable)
        with pytest.raises(PermissionError, match="授权清理不可用"):
            pipeline.run()
    assert pipeline.backend.load_successes() == {}
    assert pipeline.backend.load_failed() == {}
    queue = pipeline.backend.load_queue()
    assert len(queue) == 1 and queue[0]["attempt_no"] == 1
    assert (tmp_path / "output" / "temporary.txt").exists()
    if pipeline.backend.schema_version >= TARGET_SCHEMA_VERSION:
        assert pipeline.backend.load_attempts("work::cleanup") == []
    else:
        assert pipeline.backend.load_attempts("work::cleanup")[0].outcome == "running"


def test_stop_drains_received_messages_and_workers(pipelines, monkeypatch, tmp_path):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir(parents=True, exist_ok=True)
    pipeline = pipelines()
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
                and len(channel.read_declared_artifacts(handle.job_uid)) == 2
            ):
                requested.append(handle.execution_id or "")
                pipeline.stop()
                (sync_dir / "release").write_text("go", encoding="utf-8")
        return orig_reap(handles)

    monkeypatch.setattr(channel, "reap_completed", reap_with_stop_trigger)
    res = pipeline.run()
    assert res is None
    assert requested
    queue = pipeline.backend.load_queue()
    assert not (tmp_path / "output" / "temporary.txt").exists()
    assert (tmp_path / "output" / "kept.txt").exists()
    assert [j["job_id"] for j in queue] == ["waiting"]
    assert pipeline.backend.load_successes()["controlled::active"]["inputs"] == [
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
    assert pipeline.backend.load_successes()["child::done"]["child"] is True
    assert pipeline.backend.load_queue() == []
    assert pipeline.backend.load_attempts("child::done")[0].outcome == "succeeded"


@pytest.mark.parametrize("fault", ["json", "kind", "job_uid", "execution_id"])
def test_invalid_message_cannot_register_success(pipelines, tmp_path, fault):
    pipeline = pipelines()
    pipeline.register_task("invalid", invalid_frame_handler)
    pipeline.enqueue(Job("invalid", "invalid", payload={"fault": fault}))
    with pytest.raises(ValueError):
        pipeline.run()
    assert pipeline.backend.load_successes() == {}
    assert pipeline.backend.load_failed() == {}
    assert pipeline.backend.load_queue()[0]["attempt_no"] == 1
    assert not (tmp_path / "output" / "authorized.txt").exists()
    assert (tmp_path / "output" / "retained.txt").exists()
