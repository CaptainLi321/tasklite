"""声明环境故障沿执行结果返回，业务异常仍按任务错误处理。"""

import multiprocessing as mp

import pytest

from tasklite.engine.channel import WorkerLaunchSpec, _decode_ipc_result, _worker_process_entry
from tasklite.models.artifact import FileSpec
from tasklite.models.context import JobContext
from tasklite.models.job import Job
from tasklite.utils.ipc import (
    PIPE_MSG_RESULT,
    WorkerPipeMessage,
    create_worker_pipe,
    create_worker_reply_pipe,
)


def _declaration_fault(job, ctx):
    from tasklite.exceptions import _ArtifactIOError

    try:
        raise _ArtifactIOError("declaration write denied")
    except Exception:
        return {"swallowed": True}


def _handler_io_fault(job, ctx):
    raise OSError("external tool read failed")


def _drain_result_payload(receiver) -> dict:
    messages = receiver.drain_to_eof(timeout=5.0)
    receiver.close()
    res_msgs = [
        m.payload
        for m in messages
        if isinstance(m, WorkerPipeMessage) and m.kind == PIPE_MSG_RESULT
    ]
    assert len(res_msgs) == 1
    return res_msgs[0]


@pytest.mark.parametrize(
    "handler, infrastructure",
    [(_declaration_fault, True), (_handler_io_fault, False)],
)
def test_spawn_worker_preserves_error_boundary(tmp_path, handler, infrastructure):
    job = Job("transport", "one")
    ipc_dir = tmp_path / "ipc"
    ipc_dir.mkdir()
    ctx = JobContext(job, set(), set(), {}, ipc_dir=str(ipc_dir))
    mp_ctx = mp.get_context("spawn")
    receiver, sender = create_worker_pipe(
        mp_ctx, job_uid=job.uid, execution_id="session.1"
    )
    spec = WorkerLaunchSpec(
        handler, job, ctx, "session.1", str(ipc_dir), 10.0, pipe_sender=sender
    )
    process = mp_ctx.Process(target=_worker_process_entry, args=(spec,))
    process.start()
    sender.close()
    try:
        process.join(timeout=10)
        assert not process.is_alive()
        assert process.exitcode == 0
        payload = _drain_result_payload(receiver)
        result = _decode_ipc_result(payload, process, job)
        assert result.success is False
        if infrastructure:
            assert payload["status"] == "artifact_error"
            assert result.artifact_error == "declaration write denied"
            assert result.retry_requested is False
            assert result.result_metadata == {}
        else:
            assert result.artifact_error is None
            assert "external tool read failed" in str(
                result.message or result.result_metadata or result.retry_error
            )
    finally:
        receiver.close()
        if process.is_alive():
            process.kill()
            process.join()
        process.close()


def test_success_decode_does_not_read_artifact_declarations(tmp_path):
    job = Job("transport", "one")
    result = _decode_ipc_result(
        {"status": "success", "metadata": {}}, None, job, str(tmp_path)
    )
    assert result.success is True


def test_worker_cannot_swallow_declaration_environment_fault(tmp_path):
    job = Job("transport", "inline")
    ctx = JobContext(job, set(), set(), {})
    receiver, sender = create_worker_pipe(job_uid=job.uid, execution_id="session.1")
    spec = WorkerLaunchSpec(
        _declaration_fault,
        job,
        ctx,
        "session.1",
        str(tmp_path),
        10.0,
        pipe_sender=sender,
    )
    _worker_process_entry(spec)
    payload = _drain_result_payload(receiver)
    assert payload["status"] == "artifact_error"
    result = _decode_ipc_result(payload, None, job)
    assert result.artifact_error == "declaration write denied"
    assert not result.retry_requested


def _write_declaration_with_io_fault(job, ctx):
    from unittest.mock import patch

    from tasklite.utils.ipc import WorkerPipeSender

    kind = job.payload["kind"]
    target_method = "send_declare" if kind == "output" else "send_input"

    def denied_send(self, *args, **kwargs):
        from tasklite.exceptions import _ArtifactIOError

        raise _ArtifactIOError("declaration write denied")

    with patch.object(WorkerPipeSender, target_method, denied_send):
        try:
            if kind == "output":
                ctx.declare_file("output", FileSpec(job.payload["path"], role="output"))
            else:
                ctx.declare_input(job.payload["path"])
        except Exception:
            return {"swallowed": True}


@pytest.mark.parametrize("kind", ["input", "output"])
def test_public_declaration_write_fault_bypasses_handler_exception(tmp_path, kind):
    path = tmp_path / "data"
    if kind == "input":
        path.write_text("input")
    job = Job("transport", "inline", payload={"kind": kind, "path": str(path)})
    ipc = tmp_path / "ipc"
    ipc.mkdir()
    ctx = JobContext(job, set(), set(), {}, output_roots=tmp_path, ipc_dir=str(ipc))
    receiver, sender = create_worker_pipe(job_uid=job.uid, execution_id="session.1")
    reply_receiver, reply_sender = create_worker_reply_pipe(job_uid=job.uid, execution_id="session.1")
    spec = WorkerLaunchSpec(
        _write_declaration_with_io_fault,
        job,
        ctx,
        "session.1",
        str(ipc),
        10.0,
        pipe_sender=sender,
        reply_receiver=reply_receiver,
    )
    _worker_process_entry(spec)
    reply_sender.close()
    payload = _drain_result_payload(receiver)
    assert payload["status"] == "artifact_error"
    result = _decode_ipc_result(payload, None, job)
    assert result.artifact_error == "declaration write denied"
    assert not result.success
    assert not result.retry_requested


@pytest.mark.parametrize("kind", ["input", "output"])
def test_run_declaration_write_fault_preserves_original_queue(tmp_path, kind):
    from tasklite import TaskLite
    from tasklite.exceptions import _ArtifactIOError

    output_root = tmp_path / "output"
    output_root.mkdir()
    path = output_root / "data"
    if kind == "input":
        path.write_text("input")
    pipeline = TaskLite(
        backend="sqlite",
        state_path=tmp_path / "transport.db",
        output_roots=output_root,
    )
    pipeline.register_task("transport", _write_declaration_with_io_fault)
    job = Job._restore(
        "transport",
        "one",
        payload={"kind": kind, "path": str(path)},
        max_retries_per_run=3,
        attempt_no=2,
    )
    try:
        pipeline.enqueue(job)
        with pytest.raises(_ArtifactIOError, match="declaration write denied"):
            pipeline.run()
        rows = pipeline.backend.load_queue_jobs()
        assert rows[0]["status"] == "running"
        queue = [row["job"] for row in rows]
        assert len(queue) == 1
        assert queue[0]["task_type"] == job.task_type
        assert queue[0]["job_id"] == job.job_id
        assert queue[0]["attempt_no"] == 2
        assert queue[0]["max_retries_per_run"] == 3
        assert pipeline.backend.load_successes() == {}
        assert pipeline.backend.load_failed() == {}
    finally:
        pipeline.stop()
