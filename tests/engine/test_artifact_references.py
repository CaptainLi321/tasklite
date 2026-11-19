"""具名产物在历史成功、目录消费及受保护状态目录中的公开契约。"""
from __future__ import annotations

from contextlib import contextmanager

from tasklite import FileSpec, Job, StateSnapshot, TaskLite, TaskResult
from tasklite.testing import authorizer, in_process_workers


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


@contextmanager
def _pipeline(tmp_path, *, backend="sqlite"):
    out = tmp_path / "output"
    out.mkdir(parents=True, exist_ok=True)
    kwargs = {"state_path": tmp_path / "artifacts.db"} if backend == "sqlite" else {}
    pipeline = TaskLite(backend=backend, **kwargs)
    try:
        with authorizer(pipeline, lambda *args: True):
            yield pipeline
    finally:
        try:
            pipeline.stop()
        finally:
            pipeline.close()


def test_direct_dependency_publishes_directory_for_actual_file_input(tmp_path):
    with _pipeline(tmp_path, backend="memory") as pipeline:
        pipeline.register_task("build", build_bundle)
        pipeline.register_task("consume", consume_bundle)
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
                "source": FileSpec.from_output("build::one", "bundle", role="input"),
            },
        ))
        with in_process_workers(pipeline):
            result = pipeline.run(install_signals=False)
        assert result is None
        with StateSnapshot.open(pipeline) as snap:
            b_rec = snap.get_job("build::one")
            c_rec = snap.get_job("consume::one")
        assert b_rec is not None and b_rec["status"] == "succeeded"
        assert b_rec["files"]["bundle"]["path"] == str(tmp_path / "output" / "bundle")
        assert c_rec is not None and c_rec["status"] == "succeeded"
        assert c_rec["files"]["source"]["path"] == str(tmp_path / "output" / "bundle")
        assert c_rec["result"]["metadata"]["content"] == "published"


def test_history_can_be_read_and_missing_disk_does_not_rerun_producer(tmp_path):
    with _pipeline(tmp_path) as first:
        first.register_task("build", build_bundle)
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
        with in_process_workers(first):
            first.run(install_signals=False)
        with StateSnapshot.open(first) as snap:
            original = snap.get_job("build::one")
    with _pipeline(tmp_path) as second:
        second.register_task("consume", consume_bundle)
        consumer_files = {
            "source": FileSpec.from_output("build::one", "bundle", role="input"),
        }
        second.enqueue(Job(
            "consume",
            "historical",
            depends_on=["build::one"],
            files=consumer_files,
        ))
        with in_process_workers(second):
            assert second.run(install_signals=False) is None
        with StateSnapshot.open(second) as snap:
            assert snap.get_job("consume::historical")["status"] == "succeeded"

        (tmp_path / "output" / "bundle" / "report.txt").unlink()
        (tmp_path / "output" / "bundle").rmdir()
        second.enqueue(Job(
            "consume",
            "missing",
            depends_on=["build::one"],
            files=consumer_files,
        ))
        with in_process_workers(second):
            second.run(install_signals=False)
        with StateSnapshot.open(second) as snap:
            assert snap.get_job("build::one") == original
            assert snap.get_job("consume::missing")["status"] == "failed"
        assert not (tmp_path / "output" / "bundle").exists()


def test_dispatch_protects_complete_state_directory(tmp_path):
    with _pipeline(tmp_path) as pipeline:
        pipeline.register_task("unsafe", illegal_state_output)
        pipeline.enqueue(Job("unsafe", "one", payload={"path": str(pipeline._state_path)}))
        # 动态声明受保护路径会被强制终止，使用真实子进程
        pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as snap:
            failure = snap.get_job("unsafe::one")
            assert failure is not None and failure["status"] == "failed"
            assert "protected root" in (failure["result"]["message"] or "")
            assert list(snap.iter_jobs(status="succeeded")) == []
            assert list(snap.iter_jobs(status="pending")) == []
            attempt, = snap.iter_attempts(job_uid="unsafe::one")
            assert attempt["outcome"] == "failure"
        assert pipeline._state_path.exists()
