"""声明环境故障沿执行结果返回，业务异常仍按任务错误处理。"""

import json
import multiprocessing as mp

import pytest

from tasklite.engine.channel import WorkerLaunchSpec, _decode_ipc_result, _worker_process_entry
from tasklite.models.context import JobContext
from tasklite.models.job import Job
from tasklite.utils.ipc import ArtifactJournal


def _declaration_fault(job, ctx):
    from tasklite.exceptions import _ArtifactIOError

    try:
        raise _ArtifactIOError("declaration write denied")
    except Exception:
        return {"swallowed": True}


def _handler_io_fault(job, ctx):
    raise OSError("external tool read failed")


@pytest.mark.parametrize("handler, infrastructure", [(_declaration_fault, True), (_handler_io_fault, False)])
def test_spawn_worker_preserves_error_boundary(tmp_path, handler, infrastructure):
    job = Job("transport", "one")
    ipc_dir = tmp_path / "ipc"
    ipc_dir.mkdir()
    ctx = JobContext(job, set(), set(), {}, ipc_dir=str(ipc_dir))
    spec = WorkerLaunchSpec(handler, job, ctx, "session.1", str(ipc_dir), 10.0, "token")
    process = mp.get_context("spawn").Process(target=_worker_process_entry, args=(spec,))
    process.start()
    try:
        process.join(timeout=10)
        assert not process.is_alive()
        assert process.exitcode == 0
        payload = json.loads(ArtifactJournal(ipc_dir).result_path(job.uid, "session.1").read_text())
        assert payload["auth"] == "token"
        result = _decode_ipc_result(payload, process, job)
        assert result.success is False
        if infrastructure:
            assert payload["status"] == "artifact_error"
            assert result.artifact_error == "declaration write denied"
            assert result.retry_requested is False
            assert result.result_metadata == {}
        else:
            assert result.artifact_error is None
            assert "external tool read failed" in str(result.result_metadata or result.retry_error)
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        process.close()


def test_success_decode_does_not_read_artifact_declarations(tmp_path):
    job = Job("transport", "one")
    ArtifactJournal(tmp_path).outputs_path(job.uid).mkdir()
    result = _decode_ipc_result({"status": "success", "raw_result": True}, None, job, str(tmp_path))
    assert result.success is True


def test_worker_cannot_swallow_declaration_environment_fault(tmp_path):
    job = Job("transport", "inline")
    ctx = JobContext(job, set(), set(), {})
    spec = WorkerLaunchSpec(_declaration_fault, job, ctx, "session.1", str(tmp_path), 10.0)
    _worker_process_entry(spec)
    payload = json.loads(ArtifactJournal(tmp_path).result_path(job.uid, "session.1").read_text())
    assert payload["status"] == "artifact_error"
    result = _decode_ipc_result(payload, None, job)
    assert result.artifact_error == "declaration write denied"
    assert not result.retry_requested


def _write_declaration_with_io_fault(job, ctx):
    import builtins
    from pathlib import Path
    from unittest.mock import patch

    kind = job.payload["kind"]
    journal = ArtifactJournal(ctx.ipc_dir)
    declaration_path = (
        journal.outputs_path(job.uid) if kind == "output" else journal.inputs_path(job.uid)
    )
    original_open = builtins.open

    def denied_open(file, mode="r", *args, **kwargs):
        if Path(file) == declaration_path and "a" in mode:
            raise PermissionError("declaration write denied")
        return original_open(file, mode, *args, **kwargs)

    with patch("builtins.open", denied_open):
        try:
            if kind == "output":
                ctx.declare_output(job.payload["path"])
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
    spec = WorkerLaunchSpec(_write_declaration_with_io_fault, job, ctx, "session.1", str(ipc), 10.0)
    _worker_process_entry(spec)
    payload = json.loads(ArtifactJournal(ipc).result_path(job.uid, "session.1").read_text())
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
    finished = []
    pipeline = TaskLite(
        "transport", state_dir=tmp_path / "state", output_roots=output_root,
        on_attempt_finished=lambda *args, **kwargs: finished.append((args, kwargs)),
    )
    pipeline.register_task("transport", _write_declaration_with_io_fault)
    job = Job("transport", "one", payload={"kind": kind, "path": str(path)},
              max_retries=3, attempt_no=2)
    try:
        pipeline.enqueue(job)
        with pytest.raises(_ArtifactIOError, match="declaration write denied"):
            pipeline.run()
        queue = pipeline.backend.load_queue()
        assert len(queue) == 1
        assert queue[0]["task_type"] == job.task_type
        assert queue[0]["job_id"] == job.job_id
        assert queue[0]["attempt_no"] == 2
        assert queue[0]["max_retries"] == 3
        assert pipeline.backend.load_successes() == {}
        assert pipeline.backend.load_failed() == {}
        assert finished == []
    finally:
        pipeline.stop()
