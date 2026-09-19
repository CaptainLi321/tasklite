"""fake_ctx 官方测试构造器回归。

契约：wall/failed/cursors 的内部容器结构由构造器兜底装配（调用方传
任意可迭代/映射即可），tmp_root 模式提供沙盒 output_root 与可记录的
ipc 目录，resources 名单启用 suspend_resource 的 fail-loud 校验。
"""
import pytest

from pathlib import Path

from tasklite.models.job import Job
from tasklite.testing import fake_ctx


def _job(task_type="fetch", job_id="j1"):
    return Job(task_type, job_id)


class TestFakeCtxBasics:
    """默认构造与集合/游标语义。"""

    def test_default_state_empty(self):
        job = _job()
        ctx = fake_ctx(job)
        assert ctx.job is job
        assert not ctx.is_completed("fetch::j1")
        assert not ctx.is_failed("fetch::j1")
        assert ctx.get_cursor("any") is None

    def test_wall_failed_accept_any_iterable(self):
        ctx = fake_ctx(_job(), wall=["a::1", "a::2"], failed=("b::1",))
        assert ctx.is_completed("a::1")
        assert ctx.is_completed("a::2")
        assert ctx.is_failed("b::1")
        assert ctx.attempted_uids() == frozenset({"a::1", "a::2", "b::1"})

    def test_cursors_mapping_visible(self):
        ctx = fake_ctx(_job(), cursors={"high_water": "2026-01-01"})
        assert ctx.get_cursor("high_water") == "2026-01-01"
        ctx.set_cursor("high_water", "2026-02-01")
        assert ctx.cursor_updates["high_water"] == "2026-02-01"

    def test_spawn_appends_new_jobs(self):
        ctx = fake_ctx(_job())
        child = _job("fetch", "child")
        ctx.spawn(child)
        assert ctx.new_jobs == [child]


class TestFakeCtxTmpRoot:
    """tmp_root 模式：沙盒 output_root + 可记录 ipc 目录。"""

    def test_output_root_and_ipc_under_tmp_root(self, tmp_path):
        ctx = fake_ctx(_job(), tmp_root=tmp_path)
        assert Path(ctx.output_root).parent == tmp_path
        assert Path(ctx.ipc_dir).parent == Path(ctx.output_root)
        assert Path(ctx.ipc_dir).is_dir()

    def test_declare_output_resolved_inside_sandbox(self, tmp_path):
        ctx = fake_ctx(_job(), tmp_root=tmp_path)
        final = ctx.declare_output("out.json")
        assert str(tmp_path) in str(final)
        assert final.startswith(str(ctx.output_root))

    def test_two_ctxs_get_isolated_dirs(self, tmp_path):
        ctx1 = fake_ctx(_job(), tmp_root=tmp_path)
        ctx2 = fake_ctx(_job(), tmp_root=tmp_path)
        assert Path(ctx1.output_root) != Path(ctx2.output_root)


class TestFakeCtxSuspendGuard:
    """suspend_resource 恒按已注册名单 fail-loud 校验（空名单即全拒绝）。"""

    def test_unknown_resource_rejected(self):
        ctx = fake_ctx(_job(), resources=["api_known"])
        with pytest.raises(ValueError, match="Unknown resource"):
            ctx.suspend_resource("api_typo", 60.0)

    def test_registered_resource_accepted(self):
        ctx = fake_ctx(_job(), resources=["api_known"])
        ctx.suspend_resource("api_known", 60.0)
        assert ctx.resource_suspensions == [("api_known", 60.0)]

    def test_no_resources_rejects_all(self):
        ctx = fake_ctx(_job())
        with pytest.raises(ValueError, match="Unknown resource"):
            ctx.suspend_resource("anything", 60.0)


class TestFakeCtxExtendedParams:
    """keyword-only 扩展参数：不改变既有调用形态（非破坏性扩展）。"""

    def test_fatal_and_transient_exceptions_forwarded(self):
        class Boom(Exception):
            pass

        ctx = fake_ctx(_job(), fatal_exceptions=(Boom,), transient_exceptions=(ValueError,))
        assert ctx.fatal_exceptions == (Boom,)
        assert ctx.transient_exceptions == (ValueError,)

    def test_transient_registry_forwarded(self):
        ctx = fake_ctx(_job(), transient_registry=(RuntimeError,))
        assert ctx.transient_registry == (RuntimeError,)

    def test_direct_ipc_dir_created(self, tmp_path):
        ipc = str(tmp_path / "my_ipc")
        ctx = fake_ctx(_job(), ipc_dir=ipc)
        assert ctx.ipc_dir == ipc
        assert Path(ipc).is_dir()

    def test_direct_output_root_forwarded(self, tmp_path):
        root = tmp_path / "root"
        ctx = fake_ctx(_job(), output_root=str(root))
        assert str(ctx.output_root) == str(root)

    def test_tmp_root_with_explicit_ipc_dir_keeps_ipc(self, tmp_path):
        ipc = str(tmp_path / "custom_ipc")
        ctx = fake_ctx(_job(), tmp_root=tmp_path, ipc_dir=ipc)
        assert ctx.ipc_dir == ipc
