"""公开文件声明、固定路径基准与上游产物引用契约。"""


import pytest

from tasklite import FileSpec, Job, TaskLiteError
from tasklite.testing import fake_ctx
from tasklite.utils.ipc import create_worker_pipe, create_worker_reply_pipe


def test_legacy_file_reader_and_input_entry_are_absent():
    ctx = fake_ctx(Job("build", "one"))
    assert not hasattr(ctx, "declare_input")
    assert not hasattr(ctx, "get_artifact_path")


def test_named_output_is_idempotent_without_deleting_written_content(tmp_path):
    ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
    spec = FileSpec("result.txt", role="output", delete_existing=True)
    path = ctx.declare_file("report", spec)
    path.write_text("retained", encoding="utf-8")
    assert ctx.declare_file("report", spec) == path
    assert path.read_text(encoding="utf-8") == "retained"
    assert set(ctx._files) == {"report"}


def test_predeclared_output_reference_is_read_through_file_path(tmp_path):
    folder = tmp_path / "bundle"
    folder.mkdir()
    (folder / "report.txt").write_text("result", encoding="utf-8")
    job = Job(
        "consume",
        "one",
        depends_on=["build::one"],
        files={
            "bundle": FileSpec.from_output("build::one", "bundle", role="input"),
        },
    )
    ctx = fake_ctx(
        job,
        successes=["build::one"],
        artifacts={"build::one": {"bundle": {"path": str(folder)}}},
    )
    assert ctx.file_path("bundle") == folder


def test_predeclared_reference_reused_by_same_dynamic_declaration(tmp_path):
    report = tmp_path / "report.txt"
    report.write_text("published", encoding="utf-8")
    source = FileSpec.from_output("build::one", "report", role="input")
    job = Job(
        "consume",
        "one",
        depends_on=["build::one"],
        files={"source": source},
    )
    ctx = fake_ctx(
        job,
        successes=["build::one"],
        artifacts={"build::one": {"report": {"path": str(report)}}},
    )
    assert ctx.declare_file("source", source) == report


@pytest.mark.parametrize("attributes", [{}, {"delete_on_failure": True}, {"delete_on_success": True}])
def test_test_context_never_executes_file_deletion_without_reply_path(tmp_path, attributes):
    path = tmp_path / "existing"
    path.write_text("valuable", encoding="utf-8")
    ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
    assert ctx.declare_file(
        "keep",
        FileSpec(path, role="output", **attributes),
    ) == path
    assert path.read_text(encoding="utf-8") == "valuable"


@pytest.mark.parametrize(
    "change",
    [
        {"delete_existing": False},
        {"delete_on_failure": True},
        {"delete_on_success": True},
        {"role": "cache"},
    ],
)
def test_changed_named_declaration_rejected_before_deleting_content(tmp_path, change):
    ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
    attributes = {"role": "output", "delete_existing": True}
    path = ctx.declare_file("report", FileSpec("report", **attributes))
    path.write_text("valuable", encoding="utf-8")
    with pytest.raises(TaskLiteError, match="conflicts"):
        ctx.declare_file("report", FileSpec(path, **(attributes | change)))
    assert path.read_text(encoding="utf-8") == "valuable"


@pytest.mark.parametrize(
    "first, second",
    [("folder", "folder/report"), ("folder/report", "folder"), ("same", "same")],
)
def test_different_names_allow_overlapping_paths(tmp_path, first, second):
    ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
    assert ctx.declare_file("first", FileSpec(first, role="output")) == tmp_path / first
    assert ctx.declare_file("second", FileSpec(second, role="output")) == tmp_path / second


def test_same_name_cannot_move_to_another_path(tmp_path):
    ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
    ctx.declare_file("report", FileSpec("first", role="output"))
    with pytest.raises(TaskLiteError, match="conflicts"):
        ctx.declare_file("report", FileSpec("second", role="output"))


def test_dynamic_output_reference_requires_direct_dependency(tmp_path):
    report = tmp_path / "report.txt"
    report.write_text("result", encoding="utf-8")
    ctx = fake_ctx(
        Job("consume", "one"),
        successes=["build::one"],
        artifacts={"build::one": {"report": {"path": str(report)}}},
    )
    with pytest.raises(TaskLiteError, match="not in explicit direct depends_on"):
        ctx.declare_file(
            "source",
            FileSpec.from_output("build::one", "report", role="input"),
        )


def test_unpublished_dynamic_reference_fails_without_recording(tmp_path):
    ctx = fake_ctx(
        Job("consume", "one", depends_on=["build::one"]),
        successes=["build::one"],
        artifacts={"build::one": {}},
    )
    with pytest.raises(TaskLiteError, match="did not publish output"):
        ctx.declare_file(
            "source",
            FileSpec.from_output("build::one", "report", role="input"),
        )
    assert "source" not in ctx._files


def test_declaration_write_error_propagates(tmp_path):
    ctx = fake_ctx(Job("build", "one"))
    receiver, sender = create_worker_pipe(
        job_uid=ctx.job.uid,
        execution_id="exec_err",
    )
    sender.close()
    receiver.close()
    reply_receiver, reply_sender = create_worker_reply_pipe(
        job_uid=ctx.job.uid,
        execution_id="exec_err",
    )
    ctx._bind_pipe_sender(sender, reply_receiver)
    try:
        with pytest.raises(OSError):
            ctx.declare_file(
                "result",
                FileSpec(tmp_path / "result", role="output"),
            )
    finally:
        reply_receiver.close()
        reply_sender.close()
