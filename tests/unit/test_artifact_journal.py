"""ArtifactJournal 产物清单与文件级 IPC 深模块独立单元测试套件。"""

import json
import os
from pathlib import Path
import pytest

from tasklite.utils.ipc import (
    ArtifactCleanupMode,
    ArtifactJournal,
)


class TestArtifactJournalPathAndSandbox:
    def test_path_construction(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "t::job_1"
        assert journal.signals_path(uid) == tmp_path / "t%3A%3Ajob_1.signals.jsonl"
        assert journal.outputs_path(uid) == tmp_path / "t%3A%3Ajob_1.outputs.jsonl"
        assert journal.inputs_path(uid) == tmp_path / "t%3A%3Ajob_1.inputs.jsonl"

    def test_null_byte_rejection(self, tmp_path):
        with pytest.raises(ValueError, match="null byte"):
            ArtifactJournal.resolve_and_validate_path("bad\x00path.txt", tmp_path)

    def test_sandbox_single_root_valid_relative(self, tmp_path):
        root = tmp_path / "outputs"
        root.mkdir()
        resolved = ArtifactJournal.resolve_and_validate_path("a/b/file.txt", root, sandbox=True)
        assert Path(resolved).is_relative_to(root.resolve())

    def test_sandbox_multi_root_matching(self, tmp_path):
        root1 = tmp_path / "root1"
        root2 = tmp_path / "root2"
        root1.mkdir()
        root2.mkdir()
        file2 = root2 / "dest.csv"
        resolved = ArtifactJournal.resolve_and_validate_path(str(file2), [root1, root2], sandbox=True)
        assert Path(resolved) == file2.resolve()

    def test_sandbox_escape_rejected(self, tmp_path):
        root = tmp_path / "outputs"
        root.mkdir()
        outside = tmp_path / "outside.txt"
        with pytest.raises(ValueError, match="outside output_root"):
            ArtifactJournal.resolve_and_validate_path(str(outside), root, sandbox=True)

    def test_sandbox_bypass(self, tmp_path):
        root = tmp_path / "outputs"
        root.mkdir()
        outside = tmp_path / "outside.txt"
        resolved = ArtifactJournal.resolve_and_validate_path(str(outside), root, sandbox=False)
        assert Path(resolved) == outside.resolve()


class TestArtifactJournalDeclarations:
    def test_record_and_verify_outputs(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "job_out"
        out_file = tmp_path / "final.data"
        cache_file = tmp_path / "temp.cache"

        journal.record_output(uid, str(out_file), cleanup=True, kind="output")
        journal.record_output(uid, str(cache_file), cleanup=True, kind="cache")

        # 产物未创建时校验失败
        ok, err = journal.verify_outputs(uid)
        assert ok is False
        assert "Missing output" in err

        # 创建正式产物后，尽管 cache_file 缺失，校验仍应通过（cache 文件被跳过）
        out_file.write_text("done")
        ok, err = journal.verify_outputs(uid)
        assert ok is True
        assert err is None

    def test_record_and_read_input_file_and_uri(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "job_in"
        data_file = tmp_path / "input.txt"
        data_file.write_text("hello input")

        entry_file = journal.record_input_file(uid, str(data_file))
        assert entry_file["kind"] == "file"
        assert entry_file["size"] == len("hello input")
        assert "mtime_ns" in entry_file

        entry_uri = journal.record_input_uri(uid, "https://example.com/data", uri_fingerprint="etag123")
        assert entry_uri["kind"] == "uri"
        assert entry_uri["uri_fingerprint"] == "etag123"

        inputs = journal.read_inputs(uid)
        assert len(inputs) == 2
        assert inputs[0]["path"] == str(data_file)
        assert inputs[1]["path"] == "https://example.com/data"

    def test_signal_record_and_drain(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "job_sig"
        journal.record_signal(uid, "gpu", 30.0)
        journal.record_signal(uid, "api", 5.0)

        signals = journal.drain_signals(uid)
        assert signals == [("gpu", 30.0), ("api", 5.0)]

        # 排空语义：再次调用返回空列表
        signals_after = journal.drain_signals(uid)
        assert signals_after == []

        # 排空后无任何残留（规范名与摘除临时名均不存在）
        assert not journal.signals_path(uid).exists()
        assert list(tmp_path.glob("*.draining")) == []

    def test_drain_signals_keeps_signal_appended_during_read(self, tmp_path, monkeypatch):
        """排空读取期间的并发追加不得被抹除（摘除式排空契约）。

        注入点：解析首行时向同名信号文件追加一条新信号（模拟活跃 worker
        的 O_APPEND 追加与排空读并发）。摘除式排空下，追加经按名新建落入
        新文件、下轮排空必须读回；「读后 truncate」旧实现会把该追加连同
        原内容一并抹除（丢信号）。
        """
        import tasklite.utils.ipc as ipc_module

        journal = ArtifactJournal(tmp_path)
        uid = "job_sig_race"
        journal.record_signal(uid, "gpu", 30.0)

        real_loads = ipc_module.loads
        signals_path = journal.signals_path(uid)
        appended = {"done": False}

        def loads_with_concurrent_append(line):
            data = real_loads(line)
            if not appended["done"]:
                appended["done"] = True
                with open(signals_path, "a", encoding="utf-8") as f:
                    f.write('{"suspend": ["api", 5.0]}\n')
                    f.flush()
            return data

        monkeypatch.setattr(ipc_module, "loads", loads_with_concurrent_append)

        assert journal.drain_signals(uid) == [("gpu", 30.0)]

        # 排空期间追加的信号在下一轮排空捞回（不丢）
        assert journal.drain_signals(uid) == [("api", 5.0)]

    def test_drain_signals_salvages_orphan_draining_file(self, tmp_path, monkeypatch):
        """排空中途读者死亡遗留的 .draining 孤儿必须在下次排空回收（先读后删）。

        注入点：排空的 unlink 抛 OSError，模拟读者进程在 rename 后、unlink
        前被 SIGKILL/断电（同一窗口）。此刻信号内容已脱离规范命名空间、困在
        孤儿文件中——回收协议必须先读取分发孤儿内容再删除，直接丢弃会把
        崩溃窗口放大成永久丢信号；孤儿文件本身不得在 ipc_dir 永久累积。
        """
        journal = ArtifactJournal(tmp_path)
        uid = "job_sig_orphan"
        journal.record_signal(uid, "api", 30.0)

        def dying_unlink(self, *args, **kwargs):
            raise OSError("reader died between rename and unlink")

        monkeypatch.setattr(Path, "unlink", dying_unlink)
        # 本次读取成功，但 unlink 失败遗留孤儿
        assert journal.drain_signals(uid) == [("api", 30.0)]
        monkeypatch.undo()

        orphans = list(tmp_path.glob("*.draining"))
        assert len(orphans) == 1, f"unlink 失败应遗留恰好一个孤儿，实际: {orphans}"

        # 重启后的下一轮排空：孤儿信号捞回且孤儿清除（不丢不重）
        assert journal.drain_signals(uid) == [("api", 30.0)]
        assert list(tmp_path.glob("*.draining")) == []
        assert journal.drain_signals(uid) == []

    def test_drain_signals_merges_orphan_and_live_signals(self, tmp_path):
        """孤儿与活跃信号文件并存时两者都必须读到，孤儿先于活跃内容。

        排空读者死亡后 worker 按名新建规范信号文件继续追加，重启后的
        首轮排空须同时回收孤儿内容与活跃文件内容，且不得遗留任何文件。
        """
        import os as os_mod
        import time as time_mod

        journal = ArtifactJournal(tmp_path)
        uid = "job_sig_mixed"
        signals_path = journal.signals_path(uid)
        journal.record_signal(uid, "gpu", 30.0)
        orphan = signals_path.with_name(
            f"{signals_path.name}.{os_mod.getpid()}.{time_mod.monotonic_ns()}.draining"
        )
        os_mod.rename(signals_path, orphan)
        journal.record_signal(uid, "api", 5.0)

        assert journal.drain_signals(uid) == [("gpu", 30.0), ("api", 5.0)]
        assert list(tmp_path.glob("*.draining")) == []
        assert not signals_path.exists()
        assert journal.drain_signals(uid) == []

    def test_drain_signals_orphan_salvage_is_uid_scoped(self, tmp_path):
        """孤儿回收按 uid 严格定界，不得回收前缀重叠的其他 uid 的孤儿。"""
        import os as os_mod
        import time as time_mod

        journal = ArtifactJournal(tmp_path)
        uid_a, uid_b = "job::a", "job::a::sibling"
        journal.record_signal(uid_b, "gpu", 2.0)
        b_path = journal.signals_path(uid_b)
        orphan_b = b_path.with_name(
            f"{b_path.name}.{os_mod.getpid()}.{time_mod.monotonic_ns()}.draining"
        )
        os_mod.rename(b_path, orphan_b)

        # a 的排空不得触及 b 的孤儿
        assert journal.drain_signals(uid_a) == []
        assert orphan_b.exists()

        # b 的排空回收自己的孤儿
        assert journal.drain_signals(uid_b) == [("gpu", 2.0)]
        assert not orphan_b.exists()

        # 前缀恰好重叠的 uid（job::a.signals.jsonl）孤儿不得被 a 误回收
        intruder = tmp_path / "job%3A%3Aa.signals.jsonl.signals.jsonl.7.8.draining"
        intruder.write_text('{"suspend": ["api", 9.0]}\n', encoding="utf-8")
        assert journal.drain_signals(uid_a) == []
        assert intruder.exists()
        assert journal.drain_signals("job::a.signals.jsonl") == [("api", 9.0)]
        assert not intruder.exists()


class TestArtifactJournalResidueSweep:
    """启动期残留信号清扫：ipc_dir 全量排空（先读分发后删除，含 .draining 孤儿）。

    跨 run 崩溃可能遗留「已落盘信号却无任何再消费路径」的残留——后续
    派发预检清理与完成收尾清理都会未读删除信号文件，启动期统一清扫
    是该不变式的兜底回收点。
    """

    def test_drain_all_signals_recovers_residue_and_orphans(self, tmp_path):
        import os as os_mod
        import time as time_mod

        journal = ArtifactJournal(tmp_path)
        journal.record_signal("t::a", "api", 30.0)
        # b 的信号文件已摘除为孤儿（排空中途读者死亡，无同名活跃文件）
        journal.record_signal("t::b", "gpu", 5.0)
        b_path = journal.signals_path("t::b")
        orphan = b_path.with_name(
            f"{b_path.name}.{os_mod.getpid()}.{time_mod.monotonic_ns()}.draining"
        )
        os_mod.rename(b_path, orphan)

        got = journal.drain_all_signals()

        assert ("t::a", "api", 30.0) in got
        assert ("t::b", "gpu", 5.0) in got
        assert list(tmp_path.iterdir()) == []
        # 幂等：二次清扫为空
        assert journal.drain_all_signals() == []

    def test_drain_all_signals_skips_names_outside_injective_image(self, tmp_path):
        """文件名还原不出合法 uid（safe_uid_filename 像集外）时不触碰。

        裸冒号不可能由 safe_uid_filename 产生（必被转义为 %3A），
        该形态不在协议域内，清扫不得误删。
        """
        journal = ArtifactJournal(tmp_path)
        foreign = tmp_path / "weird:uid.signals.jsonl"
        foreign.write_text('{"suspend": ["api", 1.0]}\n', encoding="utf-8")

        assert journal.drain_all_signals() == []
        assert foreign.exists()


class TestResultParseContainment:
    """结果/声明文件读取容灾的捕获面完整性：解析失控异常按损坏降级。

    深嵌套 JSON 使 json 解析器抛 RecursionError——它不是 ValueError 子类，
    容灾 except 家族漏接会让「损坏返回 None」承诺被击穿，异常沿认领/
    收割链穿透至主循环崩溃（残留文件 claim 前即抛未及 unlink，重启后
    崩溃循环）。
    """

    def test_read_result_deep_nesting_returns_none(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        deep = tmp_path / "deep.result.json"
        deep.write_text("[" * 200000 + "]" * 200000, encoding="utf-8")

        assert journal.read_result(deep) is None, "解析失控必须按损坏结果降级为 None"

    def test_read_outputs_deep_nesting_line_skipped(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "t::deep_outputs"
        op = journal.outputs_path(uid)
        good = tmp_path / "ok.txt"
        op.write_text(
            json.dumps({"path": str(good), "cleanup": True, "kind": "output"}) + "\n"
            + json.dumps({"path": "[" * 200000, "cleanup": True, "kind": "output"}) + "\n"
            + "[" * 200000 + "\n",
            encoding="utf-8",
        )

        outputs = journal.read_outputs(uid)

        assert len(outputs) == 2, "深嵌套行按坏行跳过，其余行正常解析"
        assert outputs[0][0] == str(good)
        assert outputs[1][1] is False or outputs[1][0]  # 第二行 path 字符串形态保留

    def test_read_inputs_deep_nesting_line_skipped(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "t::deep_inputs"
        ip = journal.inputs_path(uid)
        ip.write_text(
            json.dumps({"path": "/tmp/a", "kind": "file"}) + "\n"
            + "[" * 200000 + "\n",
            encoding="utf-8",
        )

        entries = journal.read_inputs(uid)

        assert len(entries) == 1, "深嵌套行按坏行跳过"
        assert entries[0]["path"] == "/tmp/a"

    def test_read_suspend_lines_deep_nesting_line_skipped(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "t::deep_signals"
        sp = journal.signals_path(uid)
        sp.write_text(
            json.dumps({"suspend": ["gpu", 5.0]}) + "\n"
            + "[" * 200000 + "\n",
            encoding="utf-8",
        )

        assert journal.drain_signals(uid) == [("gpu", 5.0)], "深嵌套行按坏行跳过，合法信号保留"


class TestCleanupSandboxRecheck:
    """清理消费侧的沙盒归属复检：outputs 声明是 ipc_dir 上的不可信输入。

    声明侧的沙盒校验可被伪造的 `.outputs.jsonl` 声明绕过，任何删除动作
    （unlink / rmtree）执行前必须复检路径归属——解析符号链接与 `..` 后
    落在 output_roots 或 ipc_dir 之外的一律拒绝清理，绝不 rmtree。
    """

    def _make_journal(self, tmp_path, with_output_root=True):
        ipc_dir = tmp_path / "ipc"
        ipc_dir.mkdir()
        roots = None
        if with_output_root:
            roots = tmp_path / "outputs"
            roots.mkdir()
        return ArtifactJournal(ipc_dir, output_roots=roots), ipc_dir, roots

    def test_failure_cleanup_refuses_out_of_sandbox_tree(self, tmp_path):
        """失败清理对沙盒外声明路径拒绝删除（含整树 rmtree），声明文件照常清。"""
        journal, _, _ = self._make_journal(tmp_path)
        victim = tmp_path / "victim_dir"
        victim.mkdir()
        (victim / "precious.txt").write_text("data")
        uid = "t::poison"
        journal.record_output(uid, str(victim), cleanup=True, kind="output")

        journal.cleanup(uid, ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert victim.exists(), "沙盒外目录绝不能被 rmtree"
        assert (victim / "precious.txt").exists()
        assert not journal.outputs_path(uid).exists(), "损坏声明文件本身照常清理"

    def test_failure_cleanup_still_removes_in_sandbox_entries(self, tmp_path):
        """沙盒内合法声明的半成品文件与目录照常清理（复检不误伤合法清理）。"""
        journal, _, roots = self._make_journal(tmp_path)
        broken_file = roots / "broken.txt"
        broken_file.write_text("half")
        broken_dir = roots / "half_baked"
        broken_dir.mkdir()
        (broken_dir / "sub.txt").write_text("sub")
        uid = "t::normal"
        journal.record_output(uid, str(broken_file), cleanup=True, kind="output")
        journal.record_output(uid, str(broken_dir), cleanup=True, kind="output")

        journal.cleanup(uid, ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert not broken_file.exists()
        assert not broken_dir.exists()

    def test_success_cleanup_refuses_out_of_sandbox_cache(self, tmp_path):
        """成功清理对沙盒外 cache 声明拒绝 unlink。"""
        journal, _, _ = self._make_journal(tmp_path)
        victim = tmp_path / "outside.cache"
        victim.write_text("data")
        uid = "t::cache"
        journal.record_output(uid, str(victim), cleanup=True, kind="cache")

        journal.cleanup(uid, ArtifactCleanupMode.SUCCESS)

        assert victim.exists(), "沙盒外 cache 文件绝不能被 unlink"

    def test_cleanup_without_output_roots_confines_to_ipc_dir(self, tmp_path):
        """未注入 output_roots 时沙盒退化为 ipc_dir 自身（缺信任根即默认拒绝）。"""
        journal, ipc_dir, _ = self._make_journal(tmp_path, with_output_root=False)
        inside = ipc_dir / "scratch.tmp"
        inside.write_text("data")
        victim = tmp_path / "outside.txt"
        victim.write_text("data")
        uid = "t::noipcroots"
        journal.record_output(uid, str(inside), cleanup=True, kind="cache")
        journal.record_output(uid, str(victim), cleanup=True, kind="cache")

        journal.cleanup(uid, ArtifactCleanupMode.SUCCESS)

        assert not inside.exists(), "ipc_dir 内声明路径照常清理"
        assert victim.exists(), "无信任根时沙盒外路径必须拒绝清理"

    def test_failure_cleanup_rejects_dotdot_escape_and_symlink(self, tmp_path):
        """`..` 穿越与符号链接间接逃逸的声明路径同样拒绝清理。"""
        journal, _, roots = self._make_journal(tmp_path)
        target = tmp_path / "real_target"
        target.mkdir()
        (target / "f.txt").write_text("data")
        link = roots / "link"
        os.symlink(target, link)
        uid = "t::escape"
        journal.record_output(uid, str(roots / "link" / ".." / ".." / "real_target"), cleanup=True, kind="output")

        journal.cleanup(uid, ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert target.exists(), "经符号链接与 .. 逃逸出沙盒的路径必须拒绝清理"


class TestArtifactJournalCleanupModes:
    def test_cleanup_pre_submit(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "job_clean_pre"
        journal.record_signal(uid, "gpu", 1.0)
        journal.record_output(uid, "/tmp/out", True)
        journal.record_input_file(uid, "/tmp/in")

        assert journal.signals_path(uid).exists()
        assert journal.outputs_path(uid).exists()
        assert journal.inputs_path(uid).exists()

        journal.cleanup(uid, ArtifactCleanupMode.PRE_SUBMIT)

        assert not journal.signals_path(uid).exists()
        assert not journal.outputs_path(uid).exists()
        assert not journal.inputs_path(uid).exists()

    def test_cleanup_success_mode(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "job_clean_succ"
        cache_file = tmp_path / "temp.cache"
        cache_file.write_text("cache")
        final_file = tmp_path / "final.txt"
        final_file.write_text("final")

        journal.record_output(uid, str(cache_file), cleanup=True, kind="cache")
        journal.record_output(uid, str(final_file), cleanup=True, kind="output")

        journal.cleanup(uid, ArtifactCleanupMode.SUCCESS)

        assert not cache_file.exists(), "cache file must be cleaned on success"
        assert final_file.exists(), "product file must be retained on success"
        assert not journal.outputs_path(uid).exists()

    def test_cleanup_failure_mode(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "job_clean_fail"
        broken_file = tmp_path / "broken.txt"
        broken_file.write_text("half written")
        broken_dir = tmp_path / "broken_dir"
        broken_dir.mkdir()
        (broken_dir / "sub.txt").write_text("sub")
        keep_file = tmp_path / "keep.txt"
        keep_file.write_text("preserve")

        journal.record_output(uid, str(broken_file), cleanup=True, kind="output")
        journal.record_output(uid, str(broken_dir), cleanup=True, kind="output")
        journal.record_output(uid, str(keep_file), cleanup=False, kind="output")

        journal.cleanup(uid, ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert not broken_file.exists(), "cleanup=True file must be unlinked"
        assert not broken_dir.exists(), "cleanup=True directory must be rmtree'd"
        assert keep_file.exists(), "cleanup=False file must be kept"
        assert not journal.outputs_path(uid).exists()
