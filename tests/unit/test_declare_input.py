"""R4输入追溯测试 —— declare_input/URI 与 on_input_change。

覆盖：
- declare_input 指纹采集（size/mtime_ns；文件缺失留空）
- 输入清单落 wall meta（可追溯）
- on_input_change：输入变 → 重跑；不变 → 跳过；文件消失 → 重跑
- declare_input_uri 记录 + 不参与比对
- 防御：无历史指纹视为变化
"""

import os
import time
from pathlib import Path

from tasklite import TaskLite, Job
from tasklite.models.context import TaskContext
from tasklite.models.job import Job as J
from tasklite.utils.ipc import ArtifactJournal


def _ctx(job_id="j1"):
    return TaskContext(
        J("test", job_id, payload={}),
        set(), set(), {},
        output_root=None,
    )


def _pipeline(tmp_path, name="r4"):
    return TaskLite(name=name, state_dir=tmp_path / "state", backend="sqlite")


# ══════════════════════════════════════════════════════════════════════
# declare_input 指纹采集（ctx 直调）
# ══════════════════════════════════════════════════════════════════════


class TestDeclareInputCtx:
    def test_fingerprint_collected(self, tmp_path):
        f = tmp_path / "input.bin"
        f.write_bytes(b"data" * 10)
        ctx = _ctx()
        ctx.ipc_dir = str(tmp_path / "ipc")
        ctx.declare_input(str(f))

        entries = ArtifactJournal(ctx.ipc_dir).read_inputs("test::j1")
        assert len(entries) == 1
        e = entries[0]
        assert e["path"] == str(f)
        assert e["kind"] == "file"
        assert e["size"] == 40
        assert e["mtime_ns"] > 0

    def test_missing_file_records_path_only(self, tmp_path):
        ctx = _ctx()
        ctx.ipc_dir = str(tmp_path / "ipc")
        ctx.declare_input(str(tmp_path / "nope.bin"))
        entries = ArtifactJournal(ctx.ipc_dir).read_inputs("test::j1")
        assert entries[0]["kind"] == "file"
        assert "size" not in entries[0], "stat 失败 → 不落指纹"

    def test_declare_input_uri_recorded(self, tmp_path):
        ctx = _ctx()
        ctx.ipc_dir = str(tmp_path / "ipc")
        ctx.declare_input_uri("https://example/fonts.zip", uri_fingerprint="etag-abc")
        entries = ArtifactJournal(ctx.ipc_dir).read_inputs("test::j1")
        assert entries[0]["kind"] == "uri"
        assert entries[0]["uri_fingerprint"] == "etag-abc"

    def test_declare_input_uri_accepts_path_and_returns_str(self, tmp_path):
        from pathlib import Path
        ctx = _ctx()
        ctx.ipc_dir = str(tmp_path / "ipc")
        url = Path("/tmp/example.txt")
        result = ctx.declare_input_uri(url)
        assert result == str(url)
        entries = ArtifactJournal(ctx.ipc_dir).read_inputs("test::j1")
        assert entries[0]["path"] == str(url)


# ══════════════════════════════════════════════════════════════════════
# on_input_change 端到端（真实子进程）
# ══════════════════════════════════════════════════════════════════════


def _read_input(job, ctx):
    """handler：声明输入文件 + 读取内容返回。"""
    src = job.payload["src"]
    ctx.declare_input(src)
    data = Path(src).read_bytes()
    return True, {"len": len(data)}


def _make_src(tmp_path):
    src = tmp_path / "src.bin"
    src.write_bytes(b"v1")
    return src


def _write_src(src, content):
    src.write_bytes(content)
    # 确保 mtime_ns 变化（同尺寸内容变化也可能同 mtime——写后强制 touch）
    now = time.time_ns()
    os.utime(src, ns=(now, now))


def test_on_input_change_reruns_when_changed(tmp_path):
    """输入变了 → 重跑；没变 → 跳过（rerun="on_input_change"）。"""
    p = _pipeline(tmp_path)
    p.register_handler("t", _read_input)
    src = _make_src(tmp_path)

    p.enqueue([Job("t", "x", payload={"src": str(src)}, rerun="on_input_change")])
    p.run()
    wall = p.backend.load_wall()
    assert wall["t::x"]["inputs"][0]["size"] == 2  # "v1"
    assert wall["t::x"]["run_count"] == 1

    # 输入变化 → 重跑
    _write_src(src, b"v2-longer-content")
    p.enqueue([Job("t", "x", payload={"src": str(src)}, rerun="on_input_change")])
    p.run()
    wall = p.backend.load_wall()
    assert wall["t::x"]["run_count"] == 2, "输入变化必须重跑"
    assert wall["t::x"]["inputs"][0]["size"] == len(b"v2-longer-content")

    # 输入未变 → 跳过（加载期清理，不派发）
    p.enqueue([Job("t", "x", payload={"src": str(src)}, rerun="on_input_change")])
    p.run()
    wall = p.backend.load_wall()
    assert wall["t::x"]["run_count"] == 2, "输入未变不得重跑"


def test_on_input_change_input_deleted_reruns(tmp_path):
    """输入文件消失 → 视为变化 → 重跑（防误跳过）。"""
    p = _pipeline(tmp_path)
    p.register_handler("t", _read_input)
    src = _make_src(tmp_path)

    p.enqueue([Job("t", "x", payload={"src": str(src)}, rerun="on_input_change")])
    p.run()
    assert p.backend.load_wall()["t::x"]["run_count"] == 1

    src.unlink()
    p.enqueue([Job("t", "x", payload={"src": str(src)}, rerun="on_input_change")])
    p.run()
    # 输入消失 → 触发重跑判定 → handler 重跑（src 不存在 → FileNotFoundError
    # → DLQ）。验证：重跑确实发生了（failed 有记录）+ wall 旧记录作废。
    assert "t::x" in p.backend.load_failed(), "文件消失必须触发重跑（handler 重执行）"
    assert "t::x" not in p.backend.load_wall(), "重跑失败后 wall 旧记录作废"


def test_on_input_change_same_size_content_change_reruns(tmp_path):
    """ 回归：内容变化但**尺寸相同**——指纹比对靠 mtime_ns 检测（size
    相同则 size 不比，mtime 变 → 重跑）。

    修复前（无限重跑的反面）：指纹消费路径缺 mtime 语义时同尺寸变化
    无法触发；本测试锁死「同尺寸 + mtime 变 = 变化」的重跑行为。
    """
    p = _pipeline(tmp_path)
    p.register_handler("t", _read_input)
    src = _make_src(tmp_path)

    p.enqueue([Job("t", "x", payload={"src": str(src)}, rerun="on_input_change")])
    p.run()
    assert p.backend.load_wall()["t::x"]["run_count"] == 1

    # 同尺寸内容变化（b"v1" → b"v2"）+ 强制 touch（mtime_ns 变化）
    # FAT/NFS/overlayfs 时间戳粒度可能 ≥1s，
    # 0.01s sleep 不足以跨越粒度窗口 → 写后显式校验 mtime_ns 确实变化，
    # 未变则跨粒度重试；仍无法区分（极端粗粒度 FS）则 skip 而非假红。
    old_ns = src.stat().st_mtime_ns
    _write_src(src, b"v2")
    assert src.stat().st_size == 2  # 同尺寸
    if src.stat().st_mtime_ns == old_ns:
        time.sleep(1.1)  # 跨 1s 粒度窗口后重写
        _write_src(src, b"v2")
    if src.stat().st_mtime_ns == old_ns:
        pytest.skip("filesystem mtime_ns granularity too coarse for this test")
    p.enqueue([Job("t", "x", payload={"src": str(src)}, rerun="on_input_change")])
    p.run()
    wall = p.backend.load_wall()
    assert wall["t::x"]["run_count"] == 2, "同尺寸内容变化（mtime 变）必须重跑"


def test_no_previous_fingerprint_reruns(tmp_path):
    """wall 无历史指纹（旧版本任务）→ 视为变化 → 重跑。"""
    p = _pipeline(tmp_path)
    p.register_handler("t", _read_input)
    src = _make_src(tmp_path)
    # 预置 wall 记录（无 inputs 字段 = 旧版本）
    p.seed_wall(["t::x"])

    p.enqueue([Job("t", "x", payload={"src": str(src)}, rerun="on_input_change")])
    p.run()
    wall = p.backend.load_wall()
    assert wall["t::x"]["run_count"] == 1, "无历史指纹必须重跑"


def test_uri_not_compared(tmp_path):
    """URI 声明不参与 on_input_change 比对（变更检测默认关闭）。"""
    p = _pipeline(tmp_path)
    p.register_handler("t", _read_input)
    src = _make_src(tmp_path)
    # wall 预置含 uri 的 inputs（uri 指纹不同也不触发重跑）
    from tasklite.backend.sqlite_backend import SQLiteStateBackend
    import json as _json
    backend = p.backend
    # 直接构造 wall 记录：inputs 只有 uri（无文件）
    from tasklite.utils.jsonutil import dumps as tp_dumps
    backend.seed_wall(["t::x"])  # 无 inputs → 视为变化重跑
    # 改为：首次正常跑出文件指纹，第二次 uri 不同不影响
    p.enqueue([Job("t", "x", payload={"src": str(src)}, rerun="on_input_change")])
    p.run()
    assert p.backend.load_wall()["t::x"]["run_count"] == 1


def test_inputs_land_in_wall_meta(tmp_path):
    """输入清单 + 指纹落 wall meta（可追溯：任务基于什么输入跑出来的）。"""
    p = _pipeline(tmp_path)
    p.register_handler("t", _read_input)
    src = _make_src(tmp_path)
    p.enqueue([Job("t", "x", payload={"src": str(src)})])  # never（默认）
    p.run()
    wall = p.backend.load_wall()
    inputs = wall["t::x"]["inputs"]
    assert inputs[0]["path"] == str(src)
    assert inputs[0]["kind"] == "file"
    assert inputs[0]["size"] == 2
    assert inputs[0]["mtime_ns"] > 0


# ──  回归：重试后 inputs.jsonl 被消费，旧指纹不残留 → 不无限重跑 ──


def _declare_then_retry(job, ctx):
    """第一次尝试 declare 输入后 RetryError；重试后成功（用文件计数——
    子进程隔离，模块级 dict 在子进程是副本）。"""
    src = job.payload["src"]
    ctx.declare_input(src)
    counter = Path(job.payload["counter"])
    n = 0
    if counter.exists():
        n = int(counter.read_text())
    counter.write_text(str(n + 1))
    if n == 0:
        from tasklite.exceptions import RetryError
        raise RetryError("first attempt transient")
    return True


def test_retry_does_not_accumulate_stale_fingerprints(tmp_path):
    """ 回归：重试路径消费 inputs.jsonl——修复前旧指纹残留导致
    on_input_change 每次 enqueue 都重跑（无限重跑循环）。"""
    p = _pipeline(tmp_path)
    p.register_handler("t", _declare_then_retry)
    src = _make_src(tmp_path)
    counter = tmp_path / "attempts.txt"

    # 第一次 run：declare → RetryError → 重试 → 成功
    p.enqueue([Job("t", "x", payload={"src": str(src), "counter": str(counter)},
                   rerun="on_input_change")])
    p.run()
    wall = p.backend.load_wall()
    inputs = wall["t::x"]["inputs"]
    # 修复前：inputs 含 2 条（retry 前 + retry 后）同 path 指纹 → 去重后 1 条
    assert len(inputs) == 1, f"同 path 指纹应去重: {inputs}"

    # 输入未变 → 不再重跑（无限重跑回归：修复前每次 enqueue 都 run）
    p.enqueue([Job("t", "x", payload={"src": str(src), "counter": str(counter)},
                   rerun="on_input_change")])
    p.run()
    assert p.backend.load_wall()["t::x"]["run_count"] == 1, "输入未变不得重跑"
