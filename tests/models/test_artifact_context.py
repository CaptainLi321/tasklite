"""公开产物声明与直接依赖引用契约。"""
from pathlib import Path

import pytest

from tasklite import Job
from tasklite.testing import fake_ctx
from tasklite.utils.ipc import create_worker_pipe


def test_named_output_is_idempotent_without_deleting_written_content(tmp_path):
    ctx = fake_ctx(Job("build", "one"), tmp_root=tmp_path)
    path = ctx.declare_output("result.txt", name="report", delete_existing=True)
    Path(path).write_text("retained", encoding="utf-8")
    assert ctx.declare_output("result.txt", name="report", delete_existing=True) == path
    assert Path(path).read_text(encoding="utf-8") == "retained"
    assert len(ctx._journal.read_artifacts("build::one")) == 1


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


@pytest.mark.parametrize("attributes", [
    {}, {"delete_on_failure": True}, {"delete_on_success": True},
])
def test_existing_path_requires_independent_front_delete(tmp_path, attributes):
    path = tmp_path / "existing"
    path.write_text("valuable", encoding="utf-8")
    ctx = fake_ctx(Job("build", "one"))
    with pytest.raises(FileExistsError):
        ctx.declare_output(path, **attributes)
    assert path.read_text(encoding="utf-8") == "valuable"
    assert ctx.declare_output(path, delete_existing=True, **attributes) == str(path)
    assert not path.exists()


@pytest.mark.parametrize("change", [
    {"name": "alias"}, {"delete_existing": False}, {"delete_on_failure": True},
    {"delete_on_success": True, "name": None}, {"sandbox": False},
])
def test_changed_declaration_rejected_before_deleting_content(tmp_path, change):
    ctx = fake_ctx(Job("build", "one"), tmp_root=tmp_path)
    attributes = {"name": "report", "delete_existing": True}
    path = Path(ctx.declare_output("report", **attributes))
    path.write_text("valuable", encoding="utf-8")
    with pytest.raises(ValueError):
        ctx.declare_output(path, **(attributes | change))
    assert path.read_text(encoding="utf-8") == "valuable"


@pytest.mark.parametrize("first, second", [("folder", "folder/report"), ("folder/report", "folder")])
def test_overlapping_paths_rejected(tmp_path, first, second):
    ctx = fake_ctx(Job("build", "one"), tmp_root=tmp_path)
    ctx.declare_output(first)
    with pytest.raises(ValueError, match="overlapping"):
        ctx.declare_output(second, delete_existing=True)


def test_same_name_cannot_move_to_another_path(tmp_path):
    ctx = fake_ctx(Job("build", "one"), tmp_root=tmp_path)
    ctx.declare_output("first", name="report")
    with pytest.raises(ValueError, match="name already declared"):
        ctx.declare_output("second", name="report")


def test_cache_uses_anonymous_output_authorizations(tmp_path):
    ctx = fake_ctx(Job("build", "one"), tmp_root=tmp_path)
    path = ctx.declare_cache("scratch", delete_existing=True)
    declaration, = ctx._journal.read_artifacts(ctx.job.uid)
    assert declaration.to_dict() == {
        "path": path, "name": None, "delete_existing": True,
        "delete_on_failure": True, "delete_on_success": True, "sandbox": True,
    }
    with pytest.raises(TypeError):
        ctx.declare_cache("other", name="forbidden")
    with pytest.raises(ValueError):
        ctx.declare_output("other", name="forbidden", delete_on_success=True)
    with pytest.raises(TypeError):
        ctx.declare_output("other", cleanup_on_fail=True)


@pytest.mark.parametrize("relative", ["", "nested", "../"])
def test_sandbox_exemption_does_not_allow_state_paths(tmp_path, relative):
    state = tmp_path / "state"
    state.mkdir()
    ctx = fake_ctx(Job("build", "one"), protected_roots=(state,))
    with pytest.raises(ValueError, match="protected root"):
        ctx.declare_output(state / relative, sandbox=False, delete_existing=True)
    assert state.is_dir()


@pytest.mark.parametrize("target", ["root", "ipc", "leaf_link", "dangling_link"])
def test_protected_paths_and_leaf_links_rejected(tmp_path, target):
    ctx = fake_ctx(Job("build", "one"), tmp_root=tmp_path)
    real = tmp_path / "real"
    real.write_text("valuable", encoding="utf-8")
    leaf = tmp_path / "link"
    leaf.symlink_to(real if target != "dangling_link" else tmp_path / "missing")
    path = {"root": ctx.output_roots, "ipc": Path(ctx.ipc_dir),
            "leaf_link": leaf, "dangling_link": leaf}[target]
    with pytest.raises(ValueError):
        ctx.declare_output(path, sandbox=False, delete_existing=True)
    assert real.read_text(encoding="utf-8") == "valuable"


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
    assert ctx.has_succeeded("build::one") == bool(successes)


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
    ctx._bind_pipe_sender(sender)
    from tasklite.exceptions import _ArtifactIOError

    with pytest.raises(_ArtifactIOError):
        ctx.declare_output(tmp_path / "result")


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
    original = os.lstat

    def denied(path, *args, **kwargs):
        if Path(path) == target:
            raise PermissionError('output access denied')
        return original(path, *args, **kwargs)

    monkeypatch.setattr(os, 'lstat', denied)
    with pytest.raises(_ArtifactIOError, match='output access denied'):
        ctx.declare_output(target)
