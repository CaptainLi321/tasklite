"""R3输出体系测试 —— declare_cache 与多根沙盒。

覆盖：
- declare_cache 语义（成功跳过存在性校验 + 无条件清理；失败/abort 删半成品）
- 原子产出模式端到端（.part + os.replace）
- 多根 output_root（任一根内放行，根外拒绝）
- declare_output(sandbox=False) 显式豁免
- 防御：null byte / 根外路径拒绝
"""

import os
from pathlib import Path

import pytest
from tasklite import TaskLite, Job
from tasklite.models.context import TaskContext
from tasklite.models.job import Job as J


def _ctx(output_root, job_id="j1"):
    return TaskContext(
        J("test", job_id, payload={}),
        set(), set(), {},
        output_root=output_root,
    )


# ══════════════════════════════════════════════════════════════════════
# declare_cache 语义（ctx 直调：声明 + 落盘格式）
# ══════════════════════════════════════════════════════════════════════


class TestDeclareCacheCtx:
    def test_cache_resolves_within_root(self, tmp_path):
        ctx = _ctx(tmp_path)
        resolved = ctx.declare_cache("out/tmp.part")
        assert resolved == str(tmp_path / "out/tmp.part")
        assert (tmp_path / "out").is_dir()

    def test_cache_kind_persisted(self, tmp_path):
        """cache 声明落盘带 kind='cache'；output 声明 kind='output'。"""
        from tasklite.utils.ipc import ArtifactJournal
        ctx = _ctx(tmp_path, job_id="j9")
        ctx.ipc_dir = str(tmp_path / "ipc")
        ctx.declare_cache("tmp.part")
        ctx.declare_output("final.jpg")

        outputs = ArtifactJournal(ctx.ipc_dir).read_outputs("test::j9")
        kinds = {k for _, _, k in outputs}
        assert kinds == {"cache", "output"}

    def test_cache_null_byte_rejected(self, tmp_path):
        ctx = _ctx(tmp_path)
        with pytest.raises(ValueError, match="null byte"):
            ctx.declare_cache("bad\x00.part")

    def test_output_sandbox_false_exempts(self, tmp_path):
        """declare_output(sandbox=False) 显式豁免沙盒（跨盘场景）。"""
        ctx = _ctx(tmp_path)
        outside = tmp_path.parent / "elsewhere" / "x.jpg"
        resolved = ctx.declare_output(str(outside), sandbox=False)
        assert resolved == str(outside)

    def test_output_sandbox_default_still_rejects(self, tmp_path):
        ctx = _ctx(tmp_path)
        outside = tmp_path.parent / "elsewhere" / "x.jpg"
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output(str(outside))

    def test_cache_sandbox_false_exempts(self, tmp_path):
        """declare_cache(sandbox=False) 显式豁免沙盒——docstring 声称
        支持但签名缺参数（pre-fix：TypeError unexpected keyword）。
        跨盘缓存与 declare_output 同语义，逐路径声明。"""
        ctx = _ctx(tmp_path)
        outside = tmp_path.parent / "elsewhere" / "x.part"
        resolved = ctx.declare_cache(str(outside), sandbox=False)
        assert resolved == str(outside)

    def test_cache_sandbox_default_still_rejects(self, tmp_path):
        """declare_cache 默认 sandbox=True——根外路径仍拒绝。"""
        ctx = _ctx(tmp_path)
        outside = tmp_path.parent / "elsewhere" / "x.part"
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_cache(str(outside))


# ══════════════════════════════════════════════════════════════════════
# 多根沙盒
# ══════════════════════════════════════════════════════════════════════


class TestMultiRootSandbox:
    def test_path_in_any_root_accepted(self, tmp_path):
        root_a = tmp_path / "a"
        root_b = tmp_path / "b"
        ctx = _ctx([root_a, root_b])
        r1 = ctx.declare_output("in_a.jpg")  # 相对路径按第一个根
        r2 = ctx.declare_output(str(root_b / "in_b.jpg"))
        assert r1 == str(root_a / "in_a.jpg")
        assert r2 == str(root_b / "in_b.jpg")

    def test_path_outside_all_roots_rejected(self, tmp_path):
        ctx = _ctx([tmp_path / "a", tmp_path / "b"])
        outside = tmp_path / "c" / "x.jpg"
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output(str(outside))

    def test_single_root_still_works(self, tmp_path):
        ctx = _ctx(tmp_path)
        assert ctx.declare_output("x.jpg") == str(tmp_path / "x.jpg")


# ══════════════════════════════════════════════════════════════════════
# 原子产出模式端到端（真实子进程）
# ══════════════════════════════════════════════════════════════════════


def _atomic_producer(job, ctx):
    """标准原子产出模式：写 .part → os.replace 到最终路径。"""
    tmp = ctx.declare_cache("./result.bin.part")
    final = ctx.declare_output("./result.bin")
    Path(tmp).write_bytes(b"data")
    os.replace(tmp, final)
    return True


def _atomic_producer_fail(job, ctx):
    """失败场景：.part 写了但未 replace（handler 抛异常）。"""
    tmp = ctx.declare_cache("./result.bin.part")
    Path(tmp).write_bytes(b"half")
    raise RuntimeError("boom after writing part")


def _atomic_producer_ok(job, ctx):
    tmp = ctx.declare_cache("./result.bin.part")
    Path(tmp).write_bytes(b"half")
    return True  # 成功但 .part 未 rename → cache 应被清理


def _make_pipeline(tmp_path, name="r3"):
    return TaskLite(name=name, state_dir=tmp_path / "state",
                        output_root=tmp_path / "out", backend="sqlite")


def test_atomic_producer_success(tmp_path):
    """原子产出成功：.part 已被 replace（不存在）、最终产物存在且通过校验。"""
    p = _make_pipeline(tmp_path)
    p.register_handler("atomic", _atomic_producer)
    p.enqueue([Job("atomic", "a1")])
    p.run()
    assert (tmp_path / "out/result.bin").exists()
    assert not (tmp_path / "out/result.bin.part").exists()
    assert "atomic::a1" in p.backend.load_wall()


def test_atomic_producer_failure_cleans_part(tmp_path):
    """失败：.part 半成品被清理，final 不存在。"""
    p = _make_pipeline(tmp_path)
    p.register_handler("atomic", _atomic_producer_fail)
    p.enqueue([Job("atomic", "a2")])
    p.run()
    failed = p.backend.load_failed()
    assert "atomic::a2" in failed
    assert not (tmp_path / "out/result.bin.part").exists(), "cache 半成品必须清理"
    assert not (tmp_path / "out/result.bin").exists()


def test_success_with_unrenamed_cache_cleaned(tmp_path):
    """成功但 cache 未 rename：cache 仍被清理（语义：任务结束文件不应存在）。"""
    p = _make_pipeline(tmp_path)
    p.register_handler("atomic", _atomic_producer_ok)
    p.enqueue([Job("atomic", "a3")])
    p.run()
    assert "atomic::a3" in p.backend.load_wall()
    assert not (tmp_path / "out/result.bin.part").exists(), "成功路径 cache 也应清理"


def _cache_only_handler(job, ctx):
    ctx.declare_cache("./never_created.part")
    return True


def test_cache_missing_does_not_fail_verification(tmp_path):
    """cache 声明但从未创建：成功校验不因 cache 缺失失败（output 才校验）。"""
    p = _make_pipeline(tmp_path)
    p.register_handler("h", _cache_only_handler)
    p.enqueue([Job("h", "x")])
    p.run()
    assert "h::x" in p.backend.load_wall(), "cache 缺失不得导致 Missing output"
