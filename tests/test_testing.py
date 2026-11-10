"""v2 测试辅助工具回归（fake_ctx / running）。

契约：successes/failed/cursors 的内部容器结构由构造器兜底装配（调用方传
任意可迭代/映射即可）；tmp_root 模式仅提供沙盒 output_roots；运行句柄
保证 run 期旗子复位（漏复位污染同会话后续用例）。
"""
from __future__ import annotations

from tasklite.triggers import BUILTIN_TRIGGER_FUNCTIONS

from pathlib import Path
from types import SimpleNamespace

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.runtime import EngineRuntime
from tasklite import FileSpec, Job, JobContext, TaskLiteError, TaskResult
from tasklite.testing import fake_ctx, running


def _job(task_type="fetch", job_id="j1"):
    return Job(task_type, job_id)


def _make_runtime(tmp_path) -> EngineRuntime:
    """最小 EngineRuntime 装配（InMemory 后端零 IO）。"""
    from tasklite.engine.admission import (
        ImmediateRequeuePolicy,
        TriggerRegistry,
    )
    from tasklite.engine.channel import ExecutionChannel
    from tasklite.engine.config import RunConfig
    from tasklite.engine.errorclass import ErrorClassifier
    from tasklite.engine.governor import DeadlockGovernor
    from tasklite.engine.resource import CapacityResource, ResourceManager
    from tasklite.models.job import WORKER_RESOURCE
    from tasklite.models.task import TaskRegistry

    ipc_dir = str(tmp_path / "ipc")
    Path(ipc_dir).mkdir(parents=True, exist_ok=True)
    resources = ResourceManager()
    resources[WORKER_RESOURCE] = CapacityResource(WORKER_RESOURCE, max_capacity=2.0)
    return EngineRuntime(
        config=RunConfig.resolve(
            name="testing_helper",
            ipc_dir=ipc_dir,
            backend=InMemoryStateBackend(),
            resources=resources,
            tasks=TaskRegistry(),
            channel=ExecutionChannel(ipc_dir),
            classifier=ErrorClassifier(),
            governor=DeadlockGovernor(),
            trigger_registry=TriggerRegistry(functions=BUILTIN_TRIGGER_FUNCTIONS),
            requeue_policy=ImmediateRequeuePolicy(),
            output_roots=tmp_path,
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

    def test_cursors_mapping_visible(self):
        ctx = fake_ctx(_job(), cursors={"high_water": "2026-01-01"})
        assert ctx.get_cursor("high_water") == "2026-01-01"
        ctx.set_cursor("high_water", "2026-02-01")
        assert ctx.cursor_updates["high_water"] == "2026-02-01"

    def test_spawn_appends_new_jobs(self):
        ctx = fake_ctx(_job())
        child = _job("fetch", "child")
        ctx.spawn(child)
        assert ctx.spawned_jobs == [child]


class TestFakeCtxTmpRoot:
    """tmp_root 模式只提供沙盒 output_roots。"""

    def test_output_root_under_tmp_root_without_fake_ipc_dir(self, tmp_path):
        ctx = fake_ctx(_job(), tmp_root=tmp_path)
        assert Path(ctx.output_roots).parent == tmp_path
        assert ctx.ipc_dir is None
        assert not (Path(ctx.output_roots) / "ipc").exists()

    def test_declare_file_uses_explicit_output_path(self, tmp_path):
        ctx = fake_ctx(_job(), tmp_root=tmp_path)
        from tasklite import FileSpec

        final = ctx.declare_file("out", FileSpec(ctx.output_roots / "out.json", role="output"))
        assert str(tmp_path) in str(final)
        assert final.is_relative_to(ctx.output_roots)

    def test_two_ctxs_get_isolated_dirs(self, tmp_path):
        ctx1 = fake_ctx(_job(), tmp_root=tmp_path)
        ctx2 = fake_ctx(_job(), tmp_root=tmp_path)
        assert Path(ctx1.output_roots) != Path(ctx2.output_roots)


class TestFakeCtxNoSuspensionSimulation:
    """fake_ctx 不提供资源许可模拟参数。"""

    def test_resources_keyword_is_rejected(self):
        with pytest.raises(TypeError, match="resources"):
            fake_ctx(_job(), resources=["api_known"])  # type: ignore[call-arg]


class TestFakeCtxExtendedParams:
    """keyword-only 扩展参数。"""

    def test_direct_ipc_dir_created(self, tmp_path):
        ipc = str(tmp_path / "my_ipc")
        ctx = fake_ctx(_job(), ipc_dir=ipc)
        assert ctx.ipc_dir == ipc
        assert Path(ipc).is_dir()

    def test_direct_output_root_forwarded(self, tmp_path):
        root = tmp_path / "root"
        ctx = fake_ctx(_job(), output_roots=str(root))
        assert str(ctx.output_roots) == str(root)

    def test_tmp_root_with_explicit_ipc_dir_keeps_ipc(self, tmp_path):
        ipc = str(tmp_path / "custom_ipc")
        ctx = fake_ctx(_job(), tmp_root=tmp_path, ipc_dir=ipc)
        assert ctx.ipc_dir == ipc


class TestRunningHandle:
    """running 句柄：置位/复位/目标解析。"""

    def test_sets_and_clears_flag_on_runtime(self, tmp_path):
        rt = _make_runtime(tmp_path)
        assert rt.is_running is False
        with running(rt) as yielded:
            assert yielded is rt
            assert rt.is_running is True
        assert rt.is_running is False

    def test_unwraps_wrapper_to_runtime(self, tmp_path):
        rt = _make_runtime(tmp_path)
        holder = SimpleNamespace(_runtime=rt, is_running=rt.is_running)
        with running(holder) as yielded:
            assert yielded is rt
            assert rt.is_running is True
        assert rt.is_running is False

    def test_flag_cleared_on_exception(self, tmp_path):
        rt = _make_runtime(tmp_path)
        with pytest.raises(RuntimeError, match="boom"):
            with running(rt):
                raise RuntimeError("boom")
        assert rt.is_running is False


class TestFakeCtxFilesAndResults:
    """新 ctx 文件声明、游标与结果测试支持。"""

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
        assert ctx.cursor_updates["step"] == "2"


class TestFakeCtxAuthorizationAndInvariants:
    """测试 fake 不绕过授权与状态事实（严格校验不可破坏的不变式）。"""

    def test_unauthorized_artifact_reference_rejected(self):
        spec = FileSpec.from_output(producer_uid="prod::1", output_name="report", role="input")
        job = Job("consume", "c1", depends_on=[])  # 未在 depends_on 显式声明 producer
        ctx = fake_ctx(job)
        with pytest.raises(TaskLiteError, match="not in explicit direct depends_on"):
            ctx.declare_file("input_art", spec)

    def test_test_context_does_not_delete_in_protected_roots(self, tmp_path):
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        target_file = state_dir / "db.sqlite"
        target_file.write_text("protected", encoding="utf-8")

        ctx = fake_ctx(Job("clean", "1"), protected_roots=(state_dir,))
        assert ctx.declare_file(
            "del",
            FileSpec(target_file, role="output", delete_existing=True),
        ) == target_file
        assert target_file.read_text(encoding="utf-8") == "protected"

    def test_unregistered_spawn_dependency_rejected(self):
        ctx = fake_ctx(Job("parent", "p1"))
        bad_child = Job("child", "c1", depends_on=["other::nonexistent"])
        with pytest.raises(ValueError, match="non-existent, future, or cross-worker dependency"):
            ctx.spawn(bad_child)

    def test_unserializable_spawn_payload_rejected(self):
        ctx = fake_ctx(Job("parent", "p1"))
        bad_child = Job("child", "c1")
        bad_child.payload["func"] = lambda x: x
        with pytest.raises(TypeError, match="payload.func"):
            ctx.spawn(bad_child)

