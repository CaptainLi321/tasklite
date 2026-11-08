"""具名产物在真实 spawn、历史成功及目录消费中的公开契约。"""
import multiprocessing as mp
from pathlib import Path

import pytest

from tasklite import Job, TaskLite, TaskResult


@pytest.fixture(autouse=True)
def _actual_spawn(_patch_mp_context, monkeypatch):
    monkeypatch.setattr(mp, "get_context", lambda method=None: mp.context.SpawnContext())


def build_bundle(job, ctx):
    folder = Path(ctx.declare_output("bundle", name="bundle", delete_existing=True))
    folder.mkdir()
    (folder / "report.txt").write_text("published", encoding="utf-8")
    return TaskResult.success()


def consume_bundle(job, ctx):
    folder = Path(ctx.get_artifact_path(producer_job_uid="build::one", artifact_name="bundle"))
    report = Path(ctx.declare_input(folder / "report.txt"))
    return TaskResult.success(metadata={"content": report.read_text(encoding="utf-8")})


def illegal_state_output(job, ctx):
    ctx.declare_output(job.payload["path"], sandbox=False, delete_existing=True)
    return TaskResult.success()


def _pipeline(tmp_path):
    return TaskLite("artifacts", state_dir=tmp_path / "state", output_roots=tmp_path / "output")


def test_direct_dependency_publishes_directory_for_actual_file_input(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("build", build_bundle)
    pipeline.register_task("consume", consume_bundle)
    try:
        pipeline.enqueue(Job("consume", "one", depends_on=["build::one"]))
        pipeline.enqueue(Job("build", "one"))
        result = pipeline.run()
        assert result is None
        assert len(pipeline.backend.load_successes()) == 2
        successes = pipeline.backend.load_successes()
        assert successes["build::one"]["artifacts"] == {"bundle": {"path": str(tmp_path / "output" / "bundle")}}
        assert successes["consume::one"]["inputs"] == [str(tmp_path / "output" / "bundle" / "report.txt")]
        assert successes["consume::one"]["content"] == "published"
    finally:
        pipeline.stop()


def test_history_can_be_read_and_missing_disk_does_not_rerun_producer(tmp_path):
    first = _pipeline(tmp_path)
    first.register_task("build", build_bundle)
    try:
        first.enqueue(Job("build", "one"))
        first.run()
        original = first.backend.load_successes()["build::one"]
    finally:
        first.stop()
    second = _pipeline(tmp_path)
    second.register_task("consume", consume_bundle)
    try:
        second.enqueue(Job("consume", "historical", depends_on=["build::one"]))
        assert second.run() is None
        assert "consume::historical" in second.backend.load_successes()
        (tmp_path / "output" / "bundle" / "report.txt").unlink()
        (tmp_path / "output" / "bundle").rmdir()
        second.enqueue(Job("consume", "missing", depends_on=["build::one"]))
        second.run()
        assert second.backend.load_successes()["build::one"] == original
        assert "consume::missing" in second.backend.load_failed()
        assert not (tmp_path / "output" / "bundle").exists()
    finally:
        second.stop()


def test_dispatch_protects_complete_state_directory(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("unsafe", illegal_state_output)
    try:
        pipeline.enqueue(Job("unsafe", "one", payload={"path": str(tmp_path / "state")}))
        pipeline.run()
        assert "unsafe::one" in pipeline.backend.load_failed()
        assert (tmp_path / "state").is_dir()
    finally:
        pipeline.stop()
