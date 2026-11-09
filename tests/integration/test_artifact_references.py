"""具名产物在真实 spawn、历史成功及目录消费中的公开契约。"""
import multiprocessing as mp

import pytest

from tasklite import FileSpec, Job, TaskLite, TaskResult


@pytest.fixture(autouse=True)
def _actual_spawn(_patch_mp_context, monkeypatch):
    monkeypatch.setattr(mp, "get_context", lambda method=None: mp.context.SpawnContext())


def build_bundle(job, ctx):
    folder = ctx.file_path("bundle")
    folder.mkdir()
    (folder / "report.txt").write_text("published", encoding="utf-8")
    return TaskResult.success()


def consume_bundle(job, ctx):
    report = ctx.file_path("source") / "report.txt"
    return TaskResult.success(metadata={"content": report.read_text(encoding="utf-8")})


def illegal_state_output(job, ctx):
    ctx.declare_file("protected", FileSpec(job.payload["path"], role="output", delete_existing=True))
    return TaskResult.success()


def _pipeline(tmp_path):
    out = tmp_path / "output"
    out.mkdir(parents=True, exist_ok=True)
    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "artifacts.db", output_roots=out)
    pipeline.authorizer = lambda *args: True
    return pipeline


def test_direct_dependency_publishes_directory_for_actual_file_input(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("build", build_bundle)
    pipeline.register_task("consume", consume_bundle)
    try:
        pipeline.enqueue(Job(
            "build",
            "one",
            files={
                "bundle": FileSpec(
                    tmp_path / "output" / "bundle",
                    role="output",
                    delete_existing=True,
                ),
            },
        ))
        pipeline.enqueue(Job(
            "consume",
            "one",
            depends_on=["build::one"],
            files={
                "source": FileSpec.from_output(
                    "build::one", "bundle", role="input"
                ),
            },
        ))
        result = pipeline.run()
        assert result is None
        assert len(pipeline._runtime.store.backend.load_successes()) == 2
        successes = pipeline._runtime.store.backend.load_successes()
        assert successes["build::one"]["artifacts"] == {"bundle": {"path": str(tmp_path / "output" / "bundle")}}
        assert successes["consume::one"]["inputs"] == [
            str(tmp_path / "output" / "bundle")
        ]
        assert successes["consume::one"]["content"] == "published"
    finally:
        pipeline.stop()


def test_history_can_be_read_and_missing_disk_does_not_rerun_producer(tmp_path):
    first = _pipeline(tmp_path)
    first.register_task("build", build_bundle)
    try:
        first.enqueue(Job(
            "build",
            "one",
            files={
                "bundle": FileSpec(
                    tmp_path / "output" / "bundle",
                    role="output",
                    delete_existing=True,
                ),
            },
        ))
        first.run()
        original = first._runtime.store.backend.load_successes()["build::one"]
    finally:
        first.stop()
    second = _pipeline(tmp_path)
    second.register_task("consume", consume_bundle)
    try:
        consumer_files = {
            "source": FileSpec.from_output("build::one", "bundle", role="input"),
        }
        second.enqueue(Job(
            "consume",
            "historical",
            depends_on=["build::one"],
            files=consumer_files,
        ))
        assert second.run() is None
        assert "consume::historical" in second._runtime.store.backend.load_successes()
        (tmp_path / "output" / "bundle" / "report.txt").unlink()
        (tmp_path / "output" / "bundle").rmdir()
        second.enqueue(Job(
            "consume",
            "missing",
            depends_on=["build::one"],
            files=consumer_files,
        ))
        second.run()
        assert second._runtime.store.backend.load_successes()["build::one"] == original
        assert "consume::missing" in second._runtime.store.backend.load_failed()
        assert not (tmp_path / "output" / "bundle").exists()
    finally:
        second.stop()


def test_dispatch_protects_complete_state_directory(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("unsafe", illegal_state_output)
    try:
        pipeline.enqueue(Job("unsafe", "one", payload={"path": str(pipeline.state_path)}))
        pipeline.run()
        failure = pipeline._runtime.store.backend.load_failed()["unsafe::one"]
        assert "outside output roots" in failure["error_message"]
        assert pipeline._runtime.store.backend.load_successes() == {}
        assert pipeline._runtime.store.backend.load_queue_jobs() == []
        attempt, = pipeline._runtime.store.backend.load_settled_attempts("unsafe::one")
        assert attempt["outcome"] == "failure"
        assert pipeline.state_path.exists()
    finally:
        pipeline.stop()
