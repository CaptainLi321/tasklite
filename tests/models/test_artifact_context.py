"""公开产物声明与直接依赖引用契约。"""
from pathlib import Path

import pytest

from tasklite import FileSpec, Job, TaskLiteError
from tasklite.testing import fake_ctx
from tasklite.utils.ipc import create_worker_pipe, create_worker_reply_pipe


def test_named_output_is_idempotent_without_deleting_written_content(tmp_path):
    ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
    spec = FileSpec("result.txt", role="output", delete_existing=True)
    path = ctx.declare_file("report", spec)
    path.write_text("retained", encoding="utf-8")
    assert ctx.declare_file("report", spec) == path
    assert path.read_text(encoding="utf-8") == "retained"
    assert set(ctx._files) == {"report"}


def test_getter_reads_direct_dependency_without_declaring_input(tmp_path):
    folder = tmp_path / "bundle"
    folder.mkdir()
    (folder / "report.txt").write_text("result", encoding="utf-8")
    ctx = fake_ctx(
        Job("consume", "one", depends_on=["build::one"]), tmp_root=tmp_path,
        successes=["build::one"],
        artifacts={"build::one": {"bundle": {"path": str(folder)}}},
    )
    assert ctx.get_artifact_path(producer_job_uid="build::one", artifact_name="bundle") == str(folder)
    assert ctx._journal.read_inputs(ctx.job.uid) == []
    report = ctx.declare_input(folder / "report.txt")
    assert ctx._journal.read_inputs(ctx.job.uid) == [report]


@pytest.mark.parametrize("role", ["output", "cache"])
@pytest.mark.parametrize("attributes", [{}, {"delete_on_failure": True}, {"delete_on_success": True}])
def test_existing_path_requires_explicit_delete_request(tmp_path, role, attributes):
    path = tmp_path / "existing"
    path.write_text("valuable", encoding="utf-8")
    ctx = fake_ctx(Job("build", "one"))
    assert ctx.declare_file("keep", FileSpec(path, role=role, **attributes)) == path
    assert path.read_text(encoding="utf-8") == "valuable"
    assert ctx.declare_file("delete", FileSpec(path, role=role, delete_existing=True, **attributes)) == path
    assert not path.exists()


@pytest.mark.parametrize("change", [
    {"delete_existing": False}, {"delete_on_failure": True},
    {"delete_on_success": True}, {"role": "cache"},
])
def test_changed_named_declaration_rejected_before_deleting_content(tmp_path, change):
    ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
    attributes = {"role": "output", "delete_existing": True}
    path = ctx.declare_file("report", FileSpec("report", **attributes))
    path.write_text("valuable", encoding="utf-8")
    with pytest.raises(TaskLiteError, match="conflicts"):
        ctx.declare_file("report", FileSpec(path, **(attributes | change)))
    assert path.read_text(encoding="utf-8") == "valuable"


@pytest.mark.parametrize("first, second", [("folder", "folder/report"), ("folder/report", "folder"), ("same", "same")])
def test_different_names_allow_overlapping_paths(tmp_path, first, second):
    ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
    assert ctx.declare_file("first", FileSpec(first, role="output")) == tmp_path / first
    assert ctx.declare_file("second", FileSpec(second, role="output")) == tmp_path / second


def test_same_name_cannot_move_to_another_path(tmp_path):
    ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
    ctx.declare_file("report", FileSpec("first", role="output"))
    with pytest.raises(TaskLiteError, match="conflicts"):
        ctx.declare_file("report", FileSpec("second", role="output"))


@pytest.mark.parametrize("successes, dependencies, artifacts, expected", [
    (["build::one"], [], {"bundle": {"path": "/missing"}}, ValueError),
    ([], ["build::one"], {"bundle": {"path": "/missing"}}, LookupError),
    (["build::one"], ["build::one"], {}, KeyError),
    (["build::one"], ["build::one"], {"bundle": {"path": "/missing"}}, FileNotFoundError),
])
def test_getter_distinguishes_missing_dependency_success_name_and_path(
    successes, dependencies, artifacts, expected,
):
    ctx = fake_ctx(Job("consume", "one", depends_on=dependencies),
                   successes=successes, artifacts={"build::one": artifacts})
    with pytest.raises(expected) as caught:
        ctx.get_artifact_path(producer_job_uid="build::one", artifact_name="bundle")
    assert "build::one" in str(caught.value) and "bundle" in str(caught.value)


def test_injected_artifacts_are_dispatch_snapshot(tmp_path):
    path = tmp_path / "result"
    path.write_text("result", encoding="utf-8")
    artifacts = {"build::one": {"report": {"path": str(path)}}}
    ctx = fake_ctx(Job("consume", "one", depends_on=["build::one"]),
                   successes=["build::one"], artifacts=artifacts)
    artifacts["build::one"].clear()
    assert ctx.get_artifact_path(producer_job_uid="build::one", artifact_name="report") == str(path)


def test_declaration_write_error_propagates(tmp_path):
    ipc = tmp_path / "ipc"
    ctx = fake_ctx(Job("build", "one"), ipc_dir=str(ipc))
    receiver, sender = create_worker_pipe(
        job_uid=ctx.job.uid,
        execution_id="exec_err",
    )
    sender.close()
    receiver.close()
    reply_receiver, reply_sender = create_worker_reply_pipe(job_uid=ctx.job.uid, execution_id="exec_err")
    ctx._bind_pipe_sender(sender, reply_receiver)
    try:
        with pytest.raises(OSError):
            ctx.declare_file("result", FileSpec(tmp_path / "result", role="output"))
    finally:
        reply_receiver.close()
        reply_sender.close()


def test_getter_propagates_access_error_with_reference_context(tmp_path, monkeypatch):
    import errno
    import os

    path = tmp_path / "report"
    path.write_text("result", encoding="utf-8")
    ctx = fake_ctx(Job("consume", "one", depends_on=["build::one"]), successes=["build::one"],
                   artifacts={"build::one": {"report": {"path": str(path)}}})
    real_stat = os.stat

    def inaccessible(target, *args, **kwargs):
        if target == str(path):
            raise PermissionError(errno.EACCES, "denied", str(path))
        return real_stat(target, *args, **kwargs)

    monkeypatch.setattr(os, "stat", inaccessible)
    with pytest.raises(PermissionError) as caught:
        ctx.get_artifact_path(producer_job_uid="build::one", artifact_name="report")
    assert "build::one" in str(caught.value) and str(path) in str(caught.value)


def test_output_path_io_fault_is_environment_error(tmp_path, monkeypatch):
    import os
    from tasklite.exceptions import _ArtifactIOError

    ctx = fake_ctx(Job('build', 'one'), tmp_root=tmp_path)
    target = tmp_path / 'result'
    target.write_text("valuable")
    original = os.lstat

    def denied(path, *args, **kwargs):
        if Path(path) == target:
            raise PermissionError('output access denied')
        return original(path, *args, **kwargs)

    monkeypatch.setattr(os, 'lstat', denied)
    with pytest.raises(PermissionError, match='output access denied'):
        ctx.declare_file('result', FileSpec(target, role='output', delete_existing=True))
