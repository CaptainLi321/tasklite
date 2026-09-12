"""ArtifactJournal 产物清单与文件级 IPC 深模块独立单元测试套件。"""

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
