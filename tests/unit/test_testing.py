"""tasklite.testing 官方测试辅助工具单元测试。"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from tasklite import FileSpec, Job, JobContext, StateSnapshot, TaskLite, TaskLiteError, TaskResult
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import ImmediateRequeuePolicy, TriggerRegistry
from tasklite.engine.channel import ExecutionChannel
from tasklite.engine.config import RunConfig
from tasklite.engine.governor import DeadlockGovernor
from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.engine.runtime import EngineRuntime
from tasklite.models.job import WORKER_RESOURCE
from tasklite.models.task import TaskRegistry
from tasklite.testing import (
    authorizer,
    cursor_updates,
    fake_ctx,
    in_process_workers,
    running,
    spawned_jobs,
)
from tasklite.models.trigger import BUILTIN_TRIGGER_FUNCTIONS


def _job(task_type="fetch", job_id="j1"):
    return Job(task_type, job_id)


def _make_runtime() -> EngineRuntime:
    """最小 EngineRuntime 装配（InMemory 后端零 IO）。"""
    resources = ResourceManager()
    resources[WORKER_RESOURCE] = CapacityResource(WORKER_RESOURCE, max_capacity=2.0)
    return EngineRuntime(
        config=RunConfig.resolve(
            name="testing_helper",
            backend=InMemoryStateBackend(),
            resources=resources,
            tasks=TaskRegistry(),
            channel=ExecutionChannel(),
            governor=DeadlockGovernor(),
            trigger_registry=TriggerRegistry(functions=BUILTIN_TRIGGER_FUNCTIONS),
            requeue_policy=ImmediateRequeuePolicy(),
        )
    )


class TestFakeCtxBasics:
    """默认构造与游标语义。"""

    def test_default_state_empty(self):
        job = _job()
        ctx = fake_ctx(job)
        assert ctx.job is job
        assert not hasattr(ctx, "has_succeeded")
        assert not hasattr(ctx, "has_failed")
        assert not hasattr(ctx, "succeeded_or_failed_job_uids")
        assert ctx.get_cursor("any") is None

    def test_cursors_mapping_visible_and_isolated(self):
        ctx = fake_ctx(_job(), cursors={"high_water": "2026-01-01"})
        assert ctx.get_cursor("high_water") == "2026-01-01"
        ctx.set_cursor("high_water", "2026-02-01")
        updates = cursor_updates(ctx)
        assert updates["high_water"] == "2026-02-01"
        updates["mutated"] = "external"
        assert "mutated" not in cursor_updates(ctx)

    def test_spawn_appends_and_returns_shallow_copy(self):
        ctx = fake_ctx(_job())
        child = _job("fetch", "child")
        ctx.spawn(child)
        jobs = spawned_jobs(ctx)
        assert jobs == [child]
        jobs.append(_job("fetch", "extra"))
        assert spawned_jobs(ctx) == [child]

    def test_helper_type_guards(self):
        with pytest.raises(TypeError, match="JobContext"):
            spawned_jobs("not_ctx")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="JobContext"):
            cursor_updates("not_ctx")  # type: ignore[arg-type]


class TestFakeCtxTmpRootAndBaseDir:
    """tmp_root 与 base_dir 模式提供隔离临时基准路径。"""

    def test_tmp_root_creates_isolated_subdirs(self, tmp_path):
        ctx1 = fake_ctx(_job(), tmp_root=tmp_path)
        ctx2 = fake_ctx(_job(), tmp_root=tmp_path)
        p1 = ctx1.declare_file("out", FileSpec("out.json", role="output"))
        p2 = ctx2.declare_file("out", FileSpec("out.json", role="output"))
        assert p1.parent.parent == tmp_path
        assert p2.parent.parent == tmp_path
        assert p1.parent != p2.parent
        assert not (p1.parent / "ipc").exists()

    def test_resources_keyword_is_rejected(self):
        with pytest.raises(TypeError, match="resources"):
            fake_ctx(_job(), resources=["api_known"])  # type: ignore[call-arg]

    def test_direct_base_dir_forwarded(self, tmp_path):
        root = tmp_path / "root"
        ctx = fake_ctx(_job(), base_dir=str(root))
        assert ctx.declare_file("out", FileSpec("a.txt", role="output")) == root / "a.txt"


class TestRunningHandle:
    """running 句柄：置位/复位/目标解析。"""

    def test_sets_and_clears_flag_on_runtime(self):
        rt = _make_runtime()
        assert rt.is_running is False
        with running(rt) as yielded:
            assert yielded is rt
            assert rt.is_running is True
        assert rt.is_running is False

    def test_unwraps_wrapper_to_runtime(self):
        rt = _make_runtime()
        holder = SimpleNamespace(_runtime=rt, is_running=rt.is_running)
        with running(holder) as yielded:
            assert yielded is rt
            assert rt.is_running is True
        assert rt.is_running is False

    def test_flag_cleared_on_exception(self):
        rt = _make_runtime()
        with pytest.raises(RuntimeError, match="boom"):
            with running(rt):
                raise RuntimeError("boom")
        assert rt.is_running is False


class TestFakeCtxFilesAndResults:
    """文件声明、游标与结果测试支持。"""

    def test_predeclared_files_and_file_path(self, tmp_path):
        src_path = tmp_path / "input.txt"
        src_path.write_text("hello", encoding="utf-8")
        job = Job("t", "j1", files={"src": FileSpec("input.txt", role="input")})
        ctx = fake_ctx(job, base_dir=tmp_path)
        assert ctx.file_path("src") == src_path

    def test_declare_file_and_file_path(self, tmp_path):
        job = Job("t", "j1")
        ctx = fake_ctx(job, base_dir=tmp_path)
        p = ctx.declare_file("out", FileSpec("output.csv", role="output"))
        assert p == tmp_path / "output.csv"
        assert ctx.file_path("out") == p

    def test_minimal_handler_usage_with_fake_ctx(self, tmp_path):
        in_file = tmp_path / "source.json"
        in_file.write_text('{"records": 42}', encoding="utf-8")

        def sample_handler(j: Job, c: JobContext) -> TaskResult:
            f = c.file_path("input_data")
            cur = c.get_cursor("step")
            c.set_cursor("step", "2")
            return TaskResult.success(metadata={"file_size": f.stat().st_size, "prev_step": cur})

        job = Job("process", "1", files={"input_data": FileSpec(str(in_file), role="input")})
        ctx = fake_ctx(job, cursors={"step": "1"})
        result = sample_handler(job, ctx)
        assert result.metadata["file_size"] > 0
        assert result.metadata["prev_step"] == "1"
        assert cursor_updates(ctx)["step"] == "2"

    def test_missing_input_raises_file_not_found_error(self, tmp_path):
        job = Job("read", "1", files={"src": FileSpec(tmp_path / "missing.txt", role="input")})
        with pytest.raises(FileNotFoundError, match="Missing input file"):
            fake_ctx(job, base_dir=tmp_path)

        ctx = fake_ctx(Job("read", "2"), base_dir=tmp_path)
        with pytest.raises(FileNotFoundError, match="Missing input file"):
            ctx.declare_file("src", FileSpec("missing.txt", role="input"))


class TestFakeCtxAuthorizationAndInvariants:
    """测试 fake 不绕过授权与状态事实。"""

    def test_unauthorized_artifact_reference_rejected(self):
        spec = FileSpec.from_output(producer_uid="prod::1", output_name="report", role="input")
        job = Job("consume", "c1", depends_on=[])
        ctx = fake_ctx(job)
        with pytest.raises(TaskLiteError, match="not in explicit direct depends_on"):
            ctx.declare_file("input_art", spec)

    def test_predeclared_artifact_reference_without_depends_on_rejected(self, tmp_path):
        out_file = tmp_path / "report.txt"
        out_file.write_text("ok", encoding="utf-8")
        spec = FileSpec.from_output(producer_uid="prod::1", output_name="report", role="input")
        job = Job("consume", "c1", depends_on=[], files={"input_art": spec})
        with pytest.raises(TaskLiteError, match="not in explicit direct depends_on"):
            fake_ctx(
                job,
                successes=["prod::1"],
                artifacts={"prod::1": {"report": {"path": str(out_file)}}},
            )

    def test_test_context_does_not_delete_in_protected_roots(self, tmp_path):
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        target_file = state_dir / "db.sqlite"
        target_file.write_text("protected", encoding="utf-8")

        ctx = fake_ctx(Job("clean", "1"), protected_roots=(state_dir,))
        with pytest.raises(TaskLiteError, match="protected root"):
            ctx.declare_file(
                "del",
                FileSpec(target_file, role="output", delete_existing=True),
            )
        assert target_file.read_text(encoding="utf-8") == "protected"

        predeclared = Job(
            "clean",
            "2",
            files={"out": FileSpec(target_file, role="output")},
        )
        with pytest.raises(TaskLiteError, match="protected root"):
            fake_ctx(predeclared, protected_roots=(state_dir,))

        outside = tmp_path / "work" / "out.txt"
        allowed_ctx = fake_ctx(Job("clean", "3"), protected_roots=(state_dir,), base_dir=tmp_path)
        assert allowed_ctx.declare_file("out", FileSpec(outside, role="output")) == outside

    def test_unregistered_spawn_dependency_rejected(self):
        ctx = fake_ctx(Job("parent", "p1"))
        bad_child = Job("child", "c1", depends_on=["other::nonexistent"])
        with pytest.raises(TaskLiteError, match="non-existent, future, or cross-worker dependency"):
            ctx.spawn(bad_child)

    def test_unserializable_spawn_payload_rejected(self):
        ctx = fake_ctx(Job("parent", "p1"))
        bad_child = Job("child", "c1")
        bad_child.payload["func"] = lambda x: x
        with pytest.raises(TaskLiteError, match="payload.func"):
            ctx.spawn(bad_child)


def _echo_worker_handler(job: Job, ctx: JobContext) -> TaskResult:
    return TaskResult.success(metadata={"job_id": job.job_id})


class TestAuthorizerAndInProcessWorkersHandles:
    """authorizer 与 in_process_workers 官方测试句柄。"""

    def test_authorizer_restores_original_on_exit_and_exception(self):
        with TaskLite(backend="memory") as pipeline:
            initial = lambda p, k: False  # noqa: E731
            with authorizer(pipeline, initial) as store:
                assert store.authorizer is initial
                with pytest.raises(RuntimeError, match="boom"):
                    with authorizer(pipeline, lambda p, k: True):
                        assert store.authorizer("x", "file") is True
                        raise RuntimeError("boom")
                assert store.authorizer is initial
            with authorizer(pipeline, None) as restored_store:
                assert restored_store.authorizer is None

    def test_in_process_workers_runs_and_restores_context(self):
        with TaskLite(backend="memory", max_workers=1) as pipeline:
            assert not hasattr(pipeline, "_process_context")
            assert not hasattr(pipeline._channel, "_process_context")
            orig_launcher = pipeline._channel._launcher
            pipeline.register_task("echo", _echo_worker_handler)
            pipeline.enqueue(Job("echo", "item"))
            with in_process_workers(pipeline):
                assert pipeline._channel._launcher is not orig_launcher
                pipeline.run(install_signals=False)
            assert pipeline._channel._launcher is orig_launcher
            with StateSnapshot.open(pipeline) as snap:
                record = snap.get_job("echo::item")
                assert record is not None
                assert record["status"] == "succeeded"

    def test_in_process_workers_accepts_custom_launcher_and_rejects_non_launcher(self):
        from tasklite.testing import InProcessWorkerLauncher

        with TaskLite(backend="memory", max_workers=1) as pipeline:
            orig_launcher = pipeline._channel._launcher
            custom = InProcessWorkerLauncher()
            with in_process_workers(pipeline, custom):
                assert pipeline._channel._launcher is custom
            assert pipeline._channel._launcher is orig_launcher

            with pytest.raises(TypeError, match="WorkerLauncher"):
                with in_process_workers(pipeline, object):  # type: ignore[arg-type]
                    pass
            assert pipeline._channel._launcher is orig_launcher
