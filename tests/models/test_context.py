"""v2 JobContext 契约测试：spawn 预检、产物/输入声明沙盒、游标与资源挂起。

"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tasklite.models.artifact import ArtifactDeclaration
from tasklite.models.context import JobContext
from tasklite.models.job import Job
from tasklite.utils.ipc import (
    ArtifactJournal,
    PIPE_MSG_SUSPEND,
    WorkerPipeMessage,
    create_worker_pipe,
)


def _make_ctx(
    job: Job | None = None,
    *,
    successes: set[str] | None = None,
    failed: set[str] | None = None,
    cursors: dict[str, str] | None = None,
    resources: frozenset | None = None,
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
        resource_names=resources,
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


class TestDeclareOutputSandbox:
    """declare_output：沙盒校验、父目录创建与声明落盘。"""

    def test_resolves_within_root_and_creates_parents(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_roots=root, ipc_dir=str(tmp_path / "ipc"))
        final = ctx.declare_output("sub/dir/out.json")
        assert final.startswith(str(root))
        assert Path(final).parent.is_dir()  # 父目录自动创建
        outputs = ctx._journal.read_artifacts("fetch::j1")
        assert outputs == (ArtifactDeclaration(final),)

    def test_cleanup_flag_persisted(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_roots=root, ipc_dir=ipc)
        keep = ctx.declare_output("keep.bin", delete_on_failure=False)
        drop = ctx.declare_output("drop.bin", delete_on_failure=True)
        recorded = {d.path: d for d in ctx._journal.read_artifacts("fetch::j1")}
        assert recorded[keep] == ArtifactDeclaration(keep)
        assert recorded[drop] == ArtifactDeclaration(drop, delete_on_failure=True)

    def test_cleanup_flag_is_keyword_only(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_roots=root)
        with pytest.raises(TypeError, match="positional argument"):
            ctx.declare_output("a.bin", False)  # type: ignore[misc]

    def test_path_traversal_rejected(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_roots=root)
        (tmp_path / "secret.txt").write_text("secret")
        with pytest.raises(ValueError, match="outside output roots"):
            ctx.declare_output("../secret.txt")
        with pytest.raises(ValueError, match="outside output roots"):
            ctx.declare_output(str(tmp_path / "elsewhere" / "f.bin"))

    def test_absolute_within_root_accepted(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_roots=root)
        final = ctx.declare_output(str(root / "abs.json"))
        assert final == str((root / "abs.json").resolve())

    def test_sandbox_false_exempts_single_path(self, tmp_path):
        """sandbox=False 逐路径豁免（跨盘输出），其余声明仍受沙盒约束。"""
        root = tmp_path / "out"
        root.mkdir()
        elsewhere = tmp_path / "other_root"
        elsewhere.mkdir()
        ctx = _make_ctx(output_roots=root)
        final = ctx.declare_output(str(elsewhere / "cross.bin"), sandbox=False)
        assert final.startswith(str(elsewhere))
        with pytest.raises(ValueError, match="outside output roots"):
            ctx.declare_output(str(elsewhere / "cross2.bin"))

    def test_no_output_root_allows_any(self, tmp_path):
        ctx = _make_ctx()
        final = ctx.declare_output(str(tmp_path / "free.json"))
        assert Path(final).is_absolute()

    def test_null_byte_rejected(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_roots=root)
        with pytest.raises(ValueError, match="null byte"):
            ctx.declare_output("bad\x00name")

    def test_relative_path_relocated_to_root_not_cwd(self, tmp_path, monkeypatch):
        """相对路径按 output_roots 重定位（而非进程 CWD），写入与校验位置
        一致。"""
        root = tmp_path / "out"
        root.mkdir()
        other_cwd = tmp_path / "cwd"
        other_cwd.mkdir()
        monkeypatch.chdir(other_cwd)
        ctx = _make_ctx(output_roots=root)
        final = ctx.declare_output("rel.json")
        assert final.startswith(str(root))
        assert not (other_cwd / "rel.json").exists()


class TestMultiRootSandbox:
    """多根 output_roots：路径属于任一根即通过沙盒校验。"""

    def test_path_in_any_root_accepted(self, tmp_path):
        root_a = tmp_path / "disk_a"
        root_b = tmp_path / "disk_b"
        root_a.mkdir()
        root_b.mkdir()
        resolved = ArtifactJournal.resolve_and_validate_path(
            str(root_b / "f.bin"), [root_a, root_b], sandbox=True
        )
        assert resolved.startswith(str(root_b))

    def test_path_outside_all_roots_rejected(self, tmp_path):
        root_a = tmp_path / "disk_a"
        outside = tmp_path / "outside"
        root_a.mkdir()
        outside.mkdir()
        with pytest.raises(ValueError, match="resolves outside output_roots"):
            ArtifactJournal.resolve_and_validate_path(
                str(outside / "f.bin"), [root_a], sandbox=True
            )

    def test_relative_relocates_to_first_root(self, tmp_path):
        root_a = tmp_path / "disk_a"
        root_b = tmp_path / "disk_b"
        root_a.mkdir()
        root_b.mkdir()
        resolved = ArtifactJournal.resolve_and_validate_path(
            "rel.bin", [root_a, root_b], sandbox=True
        )
        assert resolved.startswith(str(root_a))


class TestDeclareCache:
    """declare_cache：匿名成败清理授权。"""

    def test_cache_resolves_within_root(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_roots=root, ipc_dir=str(tmp_path / "ipc"))
        final = ctx.declare_cache("./v.mp4.part")
        assert final.startswith(str(root))

    def test_cache_kind_persisted_with_cleanup_true(self, tmp_path):
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_roots=tmp_path, ipc_dir=ipc)
        final = ctx.declare_cache("part.bin")
        assert ctx._journal.read_artifacts("fetch::j1") == (
            ArtifactDeclaration(final, delete_on_failure=True, delete_on_success=True),
        )

    def test_cache_sandbox_rejected_by_default(self, tmp_path):
        root = tmp_path / "out"
        elsewhere = tmp_path / "other"
        root.mkdir()
        elsewhere.mkdir()
        ctx = _make_ctx(output_roots=root)
        with pytest.raises(ValueError, match="outside output roots"):
            ctx.declare_cache(str(elsewhere / "c.part"))

    def test_cache_sandbox_false_exempts(self, tmp_path):
        root = tmp_path / "out"
        elsewhere = tmp_path / "other"
        root.mkdir()
        elsewhere.mkdir()
        ctx = _make_ctx(output_roots=root)
        final = ctx.declare_cache(str(elsewhere / "c.part"), sandbox=False)
        assert final.startswith(str(elsewhere))


class TestDeclareInput:
    """declare_input：声明点校验存在性，仅记录规范路径。"""

    def test_path_recorded_without_fingerprint(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"payload")
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_roots=tmp_path, ipc_dir=ipc)
        resolved = ctx.declare_input(src)
        assert resolved == str(src.resolve())
        assert ctx._journal.read_inputs("fetch::j1") == [resolved]

    def test_directory_input_accepted(self, tmp_path):
        folder = tmp_path / "in_dir"
        folder.mkdir()
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_roots=tmp_path, ipc_dir=ipc)
        assert ctx.declare_input(folder) == str(folder.resolve())

    def test_relative_input_resolves_against_cwd_not_sandbox(self, tmp_path, monkeypatch):
        """declare_input 无沙盒语义（有意设计：框架不写输入文件）——相对
        路径按进程 CWD 解析为规范绝对路径。"""
        (tmp_path / "rel.txt").write_text("x")
        monkeypatch.chdir(tmp_path)
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_roots=tmp_path / "out", ipc_dir=ipc)
        resolved = ctx.declare_input("rel.txt")
        assert resolved == str((tmp_path / "rel.txt").resolve())

    def test_missing_input_fails_loud_without_recording(self, tmp_path):
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_roots=tmp_path, ipc_dir=ipc)
        with pytest.raises(FileNotFoundError):
            ctx.declare_input(tmp_path / "ghost.bin")
        assert ctx._journal.read_inputs("fetch::j1") == []

    def test_uri_declaration_removed(self):
        assert not hasattr(_make_ctx(), "declare_input_uri")

    def test_no_ipc_dir_declarations_are_noops(self, tmp_path):
        """ipc_dir=None：声明调用不落盘也不报错（无清单场景）。"""
        existing = tmp_path / "c.txt"
        existing.write_text("x")
        ctx = _make_ctx(output_roots=None, ipc_dir=None)
        ctx.declare_output("a.bin")
        ctx.declare_cache("b.part")
        ctx.declare_input(existing)


class TestSuspendResource:
    """suspend_resource：注册名单 fail-loud 与数值校验。"""

    def test_unknown_resource_rejected(self):
        ctx = _make_ctx(resources=frozenset({"api_known"}))
        with pytest.raises(ValueError, match="Unknown resource"):
            ctx.suspend_resource("api_typo", 60.0)

    def test_registered_resource_accepted(self, tmp_path):
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(resources=frozenset({"api_known"}), ipc_dir=ipc)
        receiver, sender = create_worker_pipe(
            job_uid=ctx.job.uid,
            execution_id="exec_susp_1",
        )
        ctx._bind_pipe_sender(sender)
        ctx.suspend_resource("api_known", 60.0)
        sender.close()
        assert ctx.resource_suspensions == [("api_known", 60.0)]
        messages = receiver.drain_to_eof(timeout=3.0)
        receiver.close()
        assert len(messages) == 1
        assert isinstance(messages[0], WorkerPipeMessage)
        assert messages[0].kind == PIPE_MSG_SUSPEND
        assert messages[0].payload == ["api_known", 60.0]

    def test_no_resources_rejects_all(self):
        ctx = _make_ctx()
        with pytest.raises(ValueError, match="Unknown resource"):
            ctx.suspend_resource("anything", 60.0)

    def test_empty_resource_name_rejected(self):
        ctx = _make_ctx(resources=frozenset({""}))
        with pytest.raises(TypeError, match="resource_name must be a non-empty str"):
            ctx.suspend_resource("", 60.0)

    def test_seconds_matrix_rejected(self):
        """非数值/bool/NaN/Inf/非正/超大 int 拒绝（超大 int 收敛
        ValueError 而非裸 OverflowError）。"""
        ctx = _make_ctx(resources=frozenset({"api"}))
        with pytest.raises(TypeError, match="seconds must be a number"):
            ctx.suspend_resource("api", "60")
        with pytest.raises(TypeError, match="seconds must be a number"):
            ctx.suspend_resource("api", True)
        for bad in (float("nan"), float("inf"), 0, -5.0, 10**400):
            with pytest.raises(ValueError, match="seconds must be finite and > 0"):
                ctx.suspend_resource("api", bad)
        assert ctx.resource_suspensions == []


