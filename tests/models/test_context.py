"""v2 JobContext 契约测试：spawn 预检、产物/输入声明沙盒与游标。

"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tasklite import FileSpec
from tasklite.models.context import JobContext
from tasklite.models.job import Job


def _make_ctx(
    job: Job | None = None,
    *,
    successes: set[str] | None = None,
    failed: set[str] | None = None,
    cursors: dict[str, str] | None = None,
    output_roots: Path | None = None,
    ipc_dir: str | None = None,
) -> JobContext:
    """构造测试用 JobContext（内部容器由本工厂兜底装配）。"""
    return JobContext(
        job if job is not None else Job("fetch", "j1"),
        success_job_uids=successes if successes is not None else set(),
        failed_job_uids=failed if failed is not None else set(),
        cursors=cursors if cursors is not None else {},
        output_roots=output_roots,
        ipc_dir=ipc_dir,
    )


class TestSnapshotSemantics:
    """游标读写与成败查询删除核查。"""

    def test_default_state_empty(self):
        ctx = _make_ctx()
        assert not hasattr(ctx, "has_succeeded")
        assert not hasattr(ctx, "has_failed")
        assert not hasattr(ctx, "succeeded_or_failed_job_uids")
        assert ctx.get_cursor("any") is None

    def test_cursors_visible_and_updatable(self):
        ctx = _make_ctx(cursors={"high_water": "2026-01-01"})
        assert ctx.get_cursor("high_water") == "2026-01-01"
        ctx.set_cursor("high_water", "2026-02-01")
        assert ctx.cursor_updates["high_water"] == "2026-02-01"
        assert ctx.get_cursor("high_water") == "2026-02-01"

    def test_set_cursor_none_deletes(self):
        """value=None 表示删除游标（与后端 None-删除语义对齐）。"""
        ctx = _make_ctx(cursors={"gone": "1", "kept": "2"})
        ctx.set_cursor("gone", None)
        assert ctx.get_cursor("gone") is None
        assert ctx.cursor_updates["gone"] is None
        assert ctx.get_cursor("kept") == "2"

    def test_set_cursor_empty_string_is_valid(self):
        ctx = _make_ctx()
        ctx.set_cursor("empty_val", "")
        assert ctx.get_cursor("empty_val") == ""
        assert ctx.cursor_updates["empty_val"] == ""

    def test_set_cursor_type_guards(self):
        ctx = _make_ctx()
        with pytest.raises(TypeError, match="cursor key must be a non-empty str"):
            ctx.set_cursor(123, "v")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="cursor key must be a non-empty str"):
            ctx.set_cursor("", "v")
        with pytest.raises(TypeError, match="cursor value must be str or None"):
            ctx.set_cursor("k", 456)  # type: ignore[arg-type]


class TestSpawn:
    """spawn 预检与子 job 登记。"""

    def test_spawn_appends_new_jobs(self):
        ctx = _make_ctx()
        child = Job("fetch", "child")
        ctx.spawn(child)
        assert ctx.spawned_jobs == [child]

    def test_spawn_rejects_non_serializable_payload(self):
        """坏 payload 在子进程内立即预检拒绝（父作业进失败档案，而非
        commit 阶段崩掉整个 run）。"""

        def my_func():
            pass

        ctx = _make_ctx()
        with pytest.raises(ValueError, match="not JSON-serializable") as exc_info:
            ctx.spawn(Job("fetch", "bad", payload={"fn": my_func}))
        assert "fetch::bad" in str(exc_info.value)
        assert ctx.spawned_jobs == []

    def test_spawn_rejects_nan_payload(self):
        """allow_nan=False：float('inf') 不得通过预检产出非标准 JSON。"""
        ctx = _make_ctx()
        with pytest.raises(ValueError, match="not JSON-serializable"):
            ctx.spawn(Job("fetch", "inf", payload={"v": float("inf")}))

    def test_spawn_accepts_normal_payload(self):
        ctx = _make_ctx()
        ctx.spawn(Job("fetch", "ok", payload={"url": "https://example.com/a"}))
        assert len(ctx.spawned_jobs) == 1


class TestDeclareFile:
    """动态 FileSpec 路径使用固定基准，不隐式创建业务目录。"""

    @pytest.mark.parametrize("method", ["declare_output", "declare_cache"])
    def test_removed_declaration_entry_is_absent(self, method):
        assert not hasattr(_make_ctx(), method)

    def test_resolves_against_base_without_creating_parents(self, tmp_path):
        ctx = _make_ctx()
        ctx.base_dir = tmp_path
        final = ctx.declare_file("out", FileSpec("sub/dir/out.json", role="output"))
        assert final == tmp_path / "sub/dir/out.json"
        assert not final.parent.exists()
        assert ctx.file_path("out") == final

    def test_captured_base_is_independent_of_worker_cwd(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        ctx = _make_ctx()
        other = tmp_path / "other"
        other.mkdir()
        monkeypatch.chdir(other)
        assert ctx.declare_file("out", FileSpec("relative", role="output")) == tmp_path / "relative"

    def test_existing_path_remains_without_delete_request(self, tmp_path):
        target = tmp_path / "valuable"
        target.write_text("valuable")
        ctx = _make_ctx()
        assert ctx.declare_file("out", FileSpec(target, role="output")) == target
        assert target.read_text() == "valuable"


class TestCacheFileSpec:
    """cache 的清理策略只由显式 FileSpec 标志表达。"""

    @pytest.mark.parametrize("cleanup", [False, True])
    def test_cache_preserves_explicit_cleanup_flags(self, tmp_path, cleanup):
        ctx = _make_ctx()
        spec = FileSpec(tmp_path / "cache", role="cache", delete_on_success=cleanup, delete_on_failure=cleanup)
        final = ctx.declare_file("cache", spec)
        assert final == tmp_path / "cache"
        assert ctx._files["cache"].delete_on_success is cleanup
        assert ctx._files["cache"].delete_on_failure is cleanup


class TestInputFileSpec:
    """输入存在性和固定基准统一由 FileSpec 声明表达。"""

    def test_missing_input_fails_without_recording(self, tmp_path):
        ctx = _make_ctx(output_roots=tmp_path)
        with pytest.raises(FileNotFoundError, match="Missing input file"):
            ctx.declare_file(
                "source",
                FileSpec(tmp_path / "ghost.bin", role="input"),
            )
        assert "source" not in ctx._files

    def test_relative_input_uses_instance_base(self, tmp_path, monkeypatch):
        (tmp_path / "rel.txt").write_text("x", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        ctx = _make_ctx()
        other = tmp_path / "other"
        other.mkdir()
        monkeypatch.chdir(other)
        assert ctx.declare_file(
            "source",
            FileSpec("rel.txt", role="input"),
        ) == tmp_path / "rel.txt"

    def test_directory_input_accepted(self, tmp_path):
        folder = tmp_path / "in_dir"
        folder.mkdir()
        ctx = _make_ctx()
        assert ctx.declare_file(
            "source",
            FileSpec(folder, role="input"),
        ) == folder


class TestRemovedResourceSuspension:
    """资源挂起接口不属于稳定 JobContext 公开面。"""

    def test_suspend_resource_is_absent(self):
        ctx = _make_ctx()
        assert not hasattr(ctx, "suspend_resource")
        assert not hasattr(ctx, "resource_suspensions")


