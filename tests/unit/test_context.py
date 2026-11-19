"""JobContext 单元测试：spawn 预检、文件声明契约、固定路径基准与游标语义。"""
from __future__ import annotations

import pytest

from tasklite import FileSpec, Job, TaskLiteError
from tasklite.testing import cursor_updates, fake_ctx, spawned_jobs
from tasklite.utils.ipc import create_worker_pipe, create_worker_reply_pipe


class TestContextCursorsAndSurface:
    """游标读写与精简公开面检查。"""

    def test_default_state_and_removed_methods(self):
        ctx = fake_ctx(Job("fetch", "j1"))
        for removed in (
            "has_succeeded",
            "has_failed",
            "succeeded_or_failed_job_uids",
            "declare_input",
            "get_artifact_path",
            "declare_output",
            "declare_cache",
            "suspend_resource",
            "resource_suspensions",
        ):
            assert not hasattr(ctx, removed)
        assert ctx.get_cursor("any") is None

    def test_cursors_read_update_delete_and_guards(self):
        ctx = fake_ctx(Job("fetch", "j1"), cursors={"high_water": "2026-01-01", "kept": "2"})
        assert ctx.get_cursor("high_water") == "2026-01-01"
        ctx.set_cursor("high_water", "2026-02-01")
        assert cursor_updates(ctx)["high_water"] == "2026-02-01"
        assert ctx.get_cursor("high_water") == "2026-02-01"

        ctx.set_cursor("high_water", None)
        assert ctx.get_cursor("high_water") is None
        assert cursor_updates(ctx)["high_water"] is None
        assert ctx.get_cursor("kept") == "2"

        ctx.set_cursor("empty_val", "")
        assert ctx.get_cursor("empty_val") == ""
        assert cursor_updates(ctx)["empty_val"] == ""

        with pytest.raises(TaskLiteError, match="cursor key must be a non-empty str"):
            ctx.set_cursor(123, "v")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="cursor key must be a non-empty str"):
            ctx.set_cursor("", "v")
        with pytest.raises(TaskLiteError, match="cursor value must be str or None"):
            ctx.set_cursor("k", 456)  # type: ignore[arg-type]


class TestContextSpawn:
    """spawn 预检与子作业登记隔离。"""

    def test_spawn_appends_and_isolates_payload(self):
        ctx = fake_ctx(Job("fetch", "j1"))
        payload = {"items": [None, True, False, 3, 1.25, "中文", {"": []}]}
        child = Job("fetch", "ok", payload=payload)
        ctx.spawn(child)
        child.payload["items"].append("Job 变异")
        payload["items"].append("外部变异")
        jobs = spawned_jobs(ctx)
        assert len(jobs) == 1
        assert jobs[0].payload == {
            "items": [None, True, False, 3, 1.25, "中文", {"": []}]
        }

    def test_spawn_rejects_mutated_invalid_payload(self):
        def my_func():
            pass

        ctx = fake_ctx(Job("fetch", "j1"))
        bad_fn = Job("fetch", "bad")
        bad_fn.payload["fn"] = my_func
        with pytest.raises(TaskLiteError, match="payload.fn"):
            ctx.spawn(bad_fn)
        assert spawned_jobs(ctx) == []

        bad_inf = Job("fetch", "inf")
        bad_inf.payload["v"] = float("inf")
        with pytest.raises(TaskLiteError, match="finite float"):
            ctx.spawn(bad_inf)
        assert spawned_jobs(ctx) == []


class TestContextFilesAndReferences:
    """动态与预声明 FileSpec 路径基准、幂等复用与上游产物引用。"""

    def test_resolves_against_fixed_base_without_creating_parents(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
        other = tmp_path / "other"
        other.mkdir()
        monkeypatch.chdir(other)

        final = ctx.declare_file("out", FileSpec("sub/dir/out.json", role="output"))
        assert final == tmp_path / "sub/dir/out.json"
        assert not final.parent.exists()
        assert ctx.file_path("out") == final

    def test_named_output_idempotent_without_deleting_written_content(self, tmp_path):
        ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
        spec = FileSpec("result.txt", role="output", delete_existing=True)
        path = ctx.declare_file("report", spec)
        path.write_text("retained", encoding="utf-8")
        assert ctx.declare_file("report", spec) == path
        assert ctx.file_path("report") == path
        assert path.read_text(encoding="utf-8") == "retained"

    @pytest.mark.parametrize("attributes", [{}, {"delete_on_failure": True}, {"delete_on_success": True}])
    def test_test_context_never_executes_file_deletion_without_reply_path(self, tmp_path, attributes):
        path = tmp_path / "existing"
        path.write_text("valuable", encoding="utf-8")
        ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
        assert ctx.declare_file("keep", FileSpec(path, role="output", **attributes)) == path
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
    def test_changed_named_declaration_rejected_before_deleting_content(self, tmp_path, change):
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
    def test_different_names_allow_overlapping_paths(self, tmp_path, first, second):
        ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
        assert ctx.declare_file("first", FileSpec(first, role="output")) == tmp_path / first
        assert ctx.declare_file("second", FileSpec(second, role="output")) == tmp_path / second

    def test_same_name_cannot_move_to_another_path(self, tmp_path):
        ctx = fake_ctx(Job("build", "one"), base_dir=tmp_path)
        ctx.declare_file("report", FileSpec("first", role="output"))
        with pytest.raises(TaskLiteError, match="conflicts"):
            ctx.declare_file("report", FileSpec("second", role="output"))

    def test_predeclared_and_dynamic_output_references(self, tmp_path):
        folder = tmp_path / "bundle"
        folder.mkdir()
        (folder / "report.txt").write_text("result", encoding="utf-8")
        source = FileSpec.from_output("build::one", "bundle", role="input")
        job = Job("consume", "one", depends_on=["build::one"], files={"bundle": source})
        ctx = fake_ctx(
            job,
            successes=["build::one"],
            artifacts={"build::one": {"bundle": {"path": str(folder)}}},
        )
        assert ctx.file_path("bundle") == folder
        assert ctx.declare_file("bundle", source) == folder

        no_dep_ctx = fake_ctx(
            Job("consume", "two"),
            successes=["build::one"],
            artifacts={"build::one": {"bundle": {"path": str(folder)}}},
        )
        with pytest.raises(TaskLiteError, match="not in explicit direct depends_on"):
            no_dep_ctx.declare_file("source", source)

        unpub_ctx = fake_ctx(
            Job("consume", "three", depends_on=["build::one"]),
            successes=["build::one"],
            artifacts={"build::one": {}},
        )
        with pytest.raises(TaskLiteError, match="did not publish output"):
            unpub_ctx.declare_file("source", source)
        with pytest.raises(TaskLiteError, match="No file declared"):
            unpub_ctx.file_path("source")

    def test_input_role_existence_and_relative_base(self, tmp_path, monkeypatch):
        ctx = fake_ctx(Job("read", "one"), base_dir=tmp_path)
        with pytest.raises(FileNotFoundError, match="Missing input file"):
            ctx.declare_file("source", FileSpec(tmp_path / "ghost.bin", role="input"))
        with pytest.raises(TaskLiteError, match="No file declared"):
            ctx.file_path("source")

        (tmp_path / "rel.txt").write_text("x", encoding="utf-8")
        other = tmp_path / "other"
        other.mkdir()
        monkeypatch.chdir(other)
        assert ctx.declare_file("source", FileSpec("rel.txt", role="input")) == tmp_path / "rel.txt"

        folder = tmp_path / "in_dir"
        folder.mkdir()
        assert ctx.declare_file("dir_in", FileSpec(folder, role="input")) == folder

    def test_declaration_write_error_propagates(self, tmp_path):
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
