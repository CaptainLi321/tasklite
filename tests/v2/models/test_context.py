"""v2 JobContext 契约测试：spawn 预检、产物/输入声明沙盒、游标与资源挂起。

移植 v1 TaskContext 相关测试（fake_ctx/sandbox/declare_cache/
declare_input）并按 v2 API 改写；布尔参数 cleanup_on_fail/sandbox
keyword-only 化。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tasklite.v2.models.context import JobContext
from tasklite.v2.models.job import Job
from tasklite.v2.utils.encoding import safe_uid_filename
from tasklite.v2.utils.ipc import ArtifactJournal


def _make_ctx(
    job: Job | None = None,
    *,
    wall: set[str] | None = None,
    failed: set[str] | None = None,
    cursors: dict[str, str] | None = None,
    resources: frozenset | None = None,
    output_root: Path | None = None,
    ipc_dir: str | None = None,
) -> JobContext:
    """构造测试用 JobContext（内部容器由本工厂兜底装配）。"""
    return JobContext(
        job if job is not None else Job("fetch", "j1"),
        wall if wall is not None else set(),
        failed if failed is not None else set(),
        cursors if cursors is not None else {},
        output_root=output_root,
        ipc_dir=ipc_dir,
        resource_names=resources,
    )


class TestSnapshotSemantics:
    """wall/failed 快照与游标读写。"""

    def test_default_state_empty(self):
        ctx = _make_ctx()
        assert not ctx.is_completed("fetch::j1")
        assert not ctx.is_failed("fetch::j1")
        assert ctx.get_cursor("any") is None
        assert ctx.attempted_uids() == frozenset()

    def test_wall_failed_snapshot_visible(self):
        ctx = _make_ctx(wall={"a::one", "a::two"}, failed={"b::one"})
        assert ctx.is_completed("a::one")
        assert ctx.is_completed("a::two")
        assert ctx.is_failed("b::one")
        assert ctx.attempted_uids() == frozenset({"a::one", "a::two", "b::one"})

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

    def test_set_cursor_type_guards(self):
        ctx = _make_ctx()
        with pytest.raises(TypeError, match="cursor key must be str"):
            ctx.set_cursor(123, "v")
        with pytest.raises(TypeError, match="cursor value must be str"):
            ctx.set_cursor("k", 456)


class TestSpawn:
    """spawn 预检与子 job 登记。"""

    def test_spawn_appends_new_jobs(self):
        ctx = _make_ctx()
        child = Job("fetch", "child")
        ctx.spawn(child)
        assert ctx.new_jobs == [child]

    def test_spawn_rejects_non_serializable_payload(self):
        """坏 payload 在子进程内立即预检拒绝（父作业进失败档案，而非
        commit 阶段崩掉整个 run）。"""

        def my_func():
            pass

        ctx = _make_ctx()
        with pytest.raises(ValueError, match="not JSON-serializable") as exc_info:
            ctx.spawn(Job("fetch", "bad", payload={"fn": my_func}))
        assert "fetch::bad" in str(exc_info.value)
        assert ctx.new_jobs == []

    def test_spawn_rejects_nan_payload(self):
        """allow_nan=False：float('inf') 不得通过预检产出非标准 JSON。"""
        ctx = _make_ctx()
        with pytest.raises(ValueError, match="not JSON-serializable"):
            ctx.spawn(Job("fetch", "inf", payload={"v": float("inf")}))

    def test_spawn_accepts_normal_payload(self):
        ctx = _make_ctx()
        ctx.spawn(Job("fetch", "ok", payload={"url": "https://example.com/a"}))
        assert len(ctx.new_jobs) == 1


class TestDeclareOutputSandbox:
    """declare_output：沙盒校验、父目录创建与声明落盘。"""

    def test_resolves_within_root_and_creates_parents(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_root=root, ipc_dir=str(tmp_path / "ipc"))
        final = ctx.declare_output("sub/dir/out.json")
        assert final.startswith(str(root))
        assert Path(final).parent.is_dir()  # 父目录自动创建
        outputs = ArtifactJournal(str(tmp_path / "ipc")).read_outputs("fetch::j1")
        assert outputs == [(final, True, "output")]

    def test_cleanup_flag_persisted(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_root=root, ipc_dir=ipc)
        keep = ctx.declare_output("keep.bin", cleanup_on_fail=False)
        drop = ctx.declare_output("drop.bin")
        recorded = {p: (c, k) for p, c, k in ArtifactJournal(ipc).read_outputs("fetch::j1")}
        assert recorded[keep] == (False, "output")
        assert recorded[drop] == (True, "output")

    def test_cleanup_flag_is_keyword_only(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_root=root)
        with pytest.raises(TypeError, match="positional argument"):
            ctx.declare_output("a.bin", False)  # type: ignore[misc]

    def test_path_traversal_rejected(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_root=root)
        (tmp_path / "secret.txt").write_text("secret")
        with pytest.raises(ValueError, match="resolves outside output_root"):
            ctx.declare_output("../secret.txt")
        with pytest.raises(ValueError, match="resolves outside output_root"):
            ctx.declare_output(str(tmp_path / "elsewhere" / "f.bin"))

    def test_absolute_within_root_accepted(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_root=root)
        final = ctx.declare_output(str(root / "abs.json"))
        assert final == str((root / "abs.json").resolve())

    def test_sandbox_false_exempts_single_path(self, tmp_path):
        """sandbox=False 逐路径豁免（跨盘输出），其余声明仍受沙盒约束。"""
        root = tmp_path / "out"
        root.mkdir()
        elsewhere = tmp_path / "other_root"
        elsewhere.mkdir()
        ctx = _make_ctx(output_root=root)
        final = ctx.declare_output(str(elsewhere / "cross.bin"), sandbox=False)
        assert final.startswith(str(elsewhere))
        with pytest.raises(ValueError, match="resolves outside output_root"):
            ctx.declare_output(str(elsewhere / "cross2.bin"))

    def test_no_output_root_allows_any(self, tmp_path):
        ctx = _make_ctx()
        final = ctx.declare_output(str(tmp_path / "free.json"))
        assert Path(final).is_absolute()

    def test_null_byte_rejected(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_root=root)
        with pytest.raises(ValueError, match="null byte"):
            ctx.declare_output("bad\x00name")

    def test_relative_path_relocated_to_root_not_cwd(self, tmp_path, monkeypatch):
        """相对路径按 output_root 重定位（而非进程 CWD），写入与校验位置
        一致。"""
        root = tmp_path / "out"
        root.mkdir()
        other_cwd = tmp_path / "cwd"
        other_cwd.mkdir()
        monkeypatch.chdir(other_cwd)
        ctx = _make_ctx(output_root=root)
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
        with pytest.raises(ValueError, match="resolves outside output_root"):
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
    """declare_cache：kind=cache 声明与恒定 cleanup 语义。"""

    def test_cache_resolves_within_root(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        ctx = _make_ctx(output_root=root, ipc_dir=str(tmp_path / "ipc"))
        final = ctx.declare_cache("./v.mp4.part")
        assert final.startswith(str(root))

    def test_cache_kind_persisted_with_cleanup_true(self, tmp_path):
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_root=tmp_path, ipc_dir=ipc)
        final = ctx.declare_cache("part.bin")
        assert ArtifactJournal(ipc).read_outputs("fetch::j1") == [(final, True, "cache")]

    def test_cache_sandbox_rejected_by_default(self, tmp_path):
        root = tmp_path / "out"
        elsewhere = tmp_path / "other"
        root.mkdir()
        elsewhere.mkdir()
        ctx = _make_ctx(output_root=root)
        with pytest.raises(ValueError, match="resolves outside output_root"):
            ctx.declare_cache(str(elsewhere / "c.part"))

    def test_cache_sandbox_false_exempts(self, tmp_path):
        root = tmp_path / "out"
        elsewhere = tmp_path / "other"
        root.mkdir()
        elsewhere.mkdir()
        ctx = _make_ctx(output_root=root)
        final = ctx.declare_cache(str(elsewhere / "c.part"), sandbox=False)
        assert final.startswith(str(elsewhere))


class TestDeclareInput:
    """declare_input：stat 指纹采集与 URI 声明。"""

    def test_fingerprint_collected(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"payload")
        st = src.stat()
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_root=tmp_path, ipc_dir=ipc)
        resolved = ctx.declare_input(src)
        assert resolved == str(src.resolve())
        entries = ArtifactJournal(ipc).read_inputs("fetch::j1")
        assert entries == [{
            "path": resolved, "kind": "file",
            "size": st.st_size, "mtime_ns": st.st_mtime_ns,
        }]

    def test_relative_input_resolves_against_cwd_not_sandbox(self, tmp_path, monkeypatch):
        """declare_input 无沙盒语义（有意设计：框架不写输入文件）——相对
        路径按进程 CWD 解析为规范绝对路径。"""
        (tmp_path / "rel.txt").write_text("x")
        monkeypatch.chdir(tmp_path)
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_root=tmp_path / "out", ipc_dir=ipc)
        resolved = ctx.declare_input("rel.txt")
        assert resolved == str((tmp_path / "rel.txt").resolve())

    def test_missing_file_records_path_only(self, tmp_path):
        """stat 失败记录 path 但不带指纹（on_input_change 比对时视为变化
        → 重跑，避免误跳过）。"""
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(output_root=tmp_path, ipc_dir=ipc)
        resolved = ctx.declare_input(tmp_path / "ghost.bin")
        entries = ArtifactJournal(ipc).read_inputs("fetch::j1")
        assert entries == [{"path": resolved, "kind": "file"}]

    def test_declare_input_uri_recorded(self, tmp_path):
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(ipc_dir=ipc)
        url = ctx.declare_input_uri("https://example.com/feed", uri_fingerprint="etag-abc")
        assert url == "https://example.com/feed"
        entries = ArtifactJournal(ipc).read_inputs("fetch::j1")
        assert entries == [{
            "path": "https://example.com/feed", "kind": "uri",
            "uri_fingerprint": "etag-abc",
        }]

    def test_declare_input_uri_without_fingerprint(self, tmp_path):
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(ipc_dir=ipc)
        ctx.declare_input_uri(Path("/feeds/daily.xml"))
        entries = ArtifactJournal(ipc).read_inputs("fetch::j1")
        assert entries == [{"path": "/feeds/daily.xml", "kind": "uri"}]

    def test_declare_input_uri_fingerprint_type_guard(self):
        ctx = _make_ctx()
        with pytest.raises(TypeError, match="uri_fingerprint must be a str or None"):
            ctx.declare_input_uri("https://example.com", uri_fingerprint=123)

    def test_no_ipc_dir_declarations_are_noops(self):
        """ipc_dir=None：声明调用不落盘也不报错（无清单场景）。"""
        ctx = _make_ctx(output_root=None, ipc_dir=None)
        ctx.declare_output("a.bin")
        ctx.declare_cache("b.part")
        ctx.declare_input("c.txt")


class TestSuspendResource:
    """suspend_resource：注册名单 fail-loud 与数值校验。"""

    def test_unknown_resource_rejected(self):
        ctx = _make_ctx(resources=frozenset({"api_known"}))
        with pytest.raises(ValueError, match="Unknown resource"):
            ctx.suspend_resource("api_typo", 60.0)

    def test_registered_resource_accepted(self, tmp_path):
        ipc = str(tmp_path / "ipc")
        ctx = _make_ctx(resources=frozenset({"api_known"}), ipc_dir=ipc)
        ctx.suspend_resource("api_known", 60.0)
        assert ctx.resource_suspensions == [("api_known", 60.0)]
        # 信号立即落盘：进程被 kill 后文件仍在，信号不丢
        signals_file = Path(ipc) / f"{safe_uid_filename('fetch::j1')}.signals.jsonl"
        assert signals_file.exists()
        lines = [json.loads(line) for line in signals_file.read_text().splitlines()]
        assert lines == [{"suspend": ["api_known", 60.0]}]

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


class TestRegistrySnapshots:
    """异常分类声明快照：None 与空元组的区别必须保真。"""

    def test_transient_registry_forwarded(self):
        ctx = _make_ctx()
        assert ctx.transient_registry == ()

        class Boom(Exception):
            pass

        ctx_two = JobContext(
            Job("fetch", "j1"), set(), set(), {}, transient_registry=(Boom,)
        )
        assert ctx_two.transient_registry == (Boom,)

    def test_none_vs_empty_tuple_distinction_preserved(self):
        """None=未声明（回退默认启发式），空元组=显式清空——误存会让
        子进程静默关闭全部默认启发式。"""
        ctx_default = _make_ctx()
        assert ctx_default.fatal_exceptions is None
        assert ctx_default.transient_exceptions is None

        ctx_empty = JobContext(
            Job("fetch", "j1"), set(), set(), {},
            fatal_exceptions=(), transient_exceptions=(),
        )
        assert ctx_empty.fatal_exceptions == ()
        assert ctx_empty.transient_exceptions == ()

    def test_declared_tuples_forwarded(self):
        class Boom(Exception):
            pass

        ctx = JobContext(
            Job("fetch", "j1"), set(), set(), {},
            fatal_exceptions=(Boom,), transient_exceptions=(ValueError,),
        )
        assert ctx.fatal_exceptions == (Boom,)
        assert ctx.transient_exceptions == (ValueError,)
