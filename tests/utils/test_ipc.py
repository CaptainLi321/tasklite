"""v2 ArtifactJournal 产物清单与文件级 IPC 深模块测试。

覆盖：路径与沙盒、声明追加/读取、摘除式信号排空（孤儿回收 + uid 定界）、
启动期残留清扫、结果原子写/两级降级/认领取舍、残留路径锚定（点后缀
兄弟 uid 不误认）、解析容灾与清理消费侧沙盒复检。
"""

import json
import os
from pathlib import Path

import pytest

from tasklite.utils.encoding import safe_uid_filename
from tasklite.utils.ipc import (
    ArtifactCleanupMode,
    ArtifactJournal,
    decode_raw_result,
    encode_raw_result,
)

_INCARNATION_FIRST = "deadbeefdeadbeefdeadbeefdeadbeef.1"


class TestArtifactJournalPathAndSandbox:
    def test_path_construction(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "t::job_1"
        assert journal.signals_path(uid) == tmp_path / "t%3A%3Ajob_1.signals.jsonl"
        assert journal.outputs_path(uid) == tmp_path / "t%3A%3Ajob_1.outputs.jsonl"
        assert journal.inputs_path(uid) == tmp_path / "t%3A%3Ajob_1.inputs.jsonl"
        assert (
            journal.result_path(uid, "abc.1")
            == tmp_path / "t%3A%3Ajob_1.abc.1.result.json"
        )
        assert (
            journal.result_tmp_path(uid, "abc.1")
            == tmp_path / "t%3A%3Ajob_1.abc.1.result.json.tmp"
        )

    def test_none_ipc_dir_path_builders_fail_loud(self):
        """ipc_dir 缺省时全部路径构造入口 fail-loud。

        若任一路径构造改为隐式 str(None) 兜底，写入侧会向进程 CWD 的
        字面 ``None/`` 目录落盘测试碎屑（跨 cwd 漂移、永不清理）——
        本测试锁定 ValueError 契约，杜绝该退化形态。
        """
        journal = ArtifactJournal(None)
        assert journal.ipc_dir is None
        for build in (
            lambda: journal.signals_path("t::x"),
            lambda: journal.outputs_path("t::x"),
            lambda: journal.inputs_path("t::x"),
            lambda: journal.result_path("t::x", _INCARNATION_FIRST),
            lambda: journal.result_tmp_path("t::x", _INCARNATION_FIRST),
        ):
            with pytest.raises(ValueError, match="ipc_dir"):
                build()

    def test_null_byte_rejection(self, tmp_path):
        with pytest.raises(ValueError, match="null byte"):
            ArtifactJournal.resolve_and_validate_path("bad\x00path.txt", tmp_path)

    def test_sandbox_single_root_valid_relative(self, tmp_path):
        root = tmp_path / "outputs"
        root.mkdir()
        resolved = ArtifactJournal.resolve_and_validate_path("a/b/file.txt", root, sandbox=True)
        assert Path(resolved).is_relative_to(root.resolve())

    def test_sandbox_multi_root_matching(self, tmp_path):
        first_root = tmp_path / "root_first"
        second_root = tmp_path / "root_second"
        first_root.mkdir()
        second_root.mkdir()
        file_in_second = second_root / "dest.csv"
        resolved = ArtifactJournal.resolve_and_validate_path(
            str(file_in_second), [first_root, second_root], sandbox=True
        )
        assert Path(resolved) == file_in_second.resolve()

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

        # 创建正式产物后，尽管 cache 缺失，校验仍通过（cache 跳过存在性校验）
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

        entry_uri = journal.record_input_uri(
            uid, "https://example.com/data", uri_fingerprint="etag123"
        )
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
        新文件、下轮排空必须读回；「读后 truncate」会把该追加连同原内容
        一并抹除（丢信号）。
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
        """孤儿与活跃信号文件并存时两者都必须读到，孤儿先于活跃内容。"""
        journal = ArtifactJournal(tmp_path)
        uid = "job_sig_mixed"
        signals_path = journal.signals_path(uid)
        journal.record_signal(uid, "gpu", 30.0)
        orphan = signals_path.with_name(
            f"{signals_path.name}.{os.getpid()}.1234567890.draining"
        )
        os.rename(signals_path, orphan)
        journal.record_signal(uid, "api", 5.0)

        assert journal.drain_signals(uid) == [("gpu", 30.0), ("api", 5.0)]
        assert list(tmp_path.glob("*.draining")) == []
        assert not signals_path.exists()
        assert journal.drain_signals(uid) == []

    def test_drain_signals_orphan_salvage_is_uid_scoped(self, tmp_path):
        """孤儿回收按 uid 严格定界，不得回收前缀重叠的其他 uid 的孤儿。"""
        journal = ArtifactJournal(tmp_path)
        uid_a, uid_b = "job::a", "job::a::sibling"
        journal.record_signal(uid_b, "gpu", 2.0)
        b_path = journal.signals_path(uid_b)
        orphan_b = b_path.with_name(
            f"{b_path.name}.{os.getpid()}.1234567891.draining"
        )
        os.rename(b_path, orphan_b)

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
    """启动期残留信号清扫：ipc_dir 全量排空（先读分发后删除，含孤儿）。

    跨 run 崩溃可能遗留「已落盘信号却无任何再消费路径」的残留——后续
    派发预检清理与完成收尾清理都会未读删除信号文件，启动期统一清扫
    是该不变式的兜底回收点。
    """

    def test_drain_all_signals_recovers_residue_and_orphans(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        journal.record_signal("t::a", "api", 30.0)
        # b 的信号文件已摘除为孤儿（排空中途读者死亡，无同名活跃文件）
        journal.record_signal("t::b", "gpu", 5.0)
        b_path = journal.signals_path("t::b")
        orphan = b_path.with_name(
            f"{b_path.name}.{os.getpid()}.1234567892.draining"
        )
        os.rename(b_path, orphan)

        got = journal.drain_all_signals()

        assert ("t::a", "api", 30.0) in got
        assert ("t::b", "gpu", 5.0) in got
        assert list(tmp_path.iterdir()) == []
        # 幂等：二次清扫为空
        assert journal.drain_all_signals() == []

    def test_drain_all_signals_skips_names_outside_injective_image(self, tmp_path):
        """文件名还原不出合法 uid（safe_uid_filename 像集外）时不触碰。

        裸冒号不可能由 safe_uid_filename 产生（必被转义），该形态不在
        协议域内，清扫不得误删。
        """
        journal = ArtifactJournal(tmp_path)
        foreign = tmp_path / "weird:uid.signals.jsonl"
        foreign.write_text('{"suspend": ["api", 1.0]}\n', encoding="utf-8")

        assert journal.drain_all_signals() == []
        assert foreign.exists()


class TestResultWriteAndRead:
    def test_write_and_read_result_roundtrip(self, tmp_path):
        """结果原子写 → 可读回（无部分消息问题）。"""
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "t::j1", {"status": "success", "x": 1}, incarnation=_INCARNATION_FIRST
        )
        assert journal.result_path("t::j1", _INCARNATION_FIRST).exists()
        data = journal.read_result(journal.result_path("t::j1", _INCARNATION_FIRST))
        assert data == {"status": "success", "x": 1}

    def test_write_result_no_tmp_leftover(self, tmp_path):
        """原子写后 .tmp 文件被 rename 掉，不留残留。"""
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "t::j1", {"status": "success"}, incarnation=_INCARNATION_FIRST
        )
        assert not journal.result_tmp_path("t::j1", _INCARNATION_FIRST).exists()

    def test_read_missing_returns_none(self, tmp_path):
        """结果文件不存在 → None（父进程轮询未完成状态）。"""
        journal = ArtifactJournal(tmp_path)
        assert journal.read_result(journal.result_path("t::ghost", _INCARNATION_FIRST)) is None

    def test_read_corrupt_returns_none(self, tmp_path):
        """损坏的结果文件 → None（宁可重跑，不可崩）。"""
        journal = ArtifactJournal(tmp_path)
        p = journal.result_path("t::j1", _INCARNATION_FIRST)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("not json {{{")
        assert journal.read_result(p) is None

    def test_write_result_atomic_rejects_nan(self, tmp_path):
        """写含 NaN 的结果必须抛（不落非标准 JSON）。"""
        journal = ArtifactJournal(tmp_path)
        with pytest.raises(ValueError):
            journal.write_result_atomic(
                "t::j1",
                {"status": "success", "raw_result": {"v": float("nan")}},
                incarnation=_INCARNATION_FIRST,
            )
        # 原子写失败 → 无残留文件（tmp 被清理）
        assert not journal.result_path("t::j1", _INCARNATION_FIRST).exists()

    def test_cleanup_removes_all_files(self, tmp_path):
        """cleanup 删除结果 + 信号 + 临时文件。"""
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "t::j1", {"status": "success"}, incarnation=_INCARNATION_FIRST
        )
        journal.record_signal("t::j1", "api", 1.0)
        journal.cleanup_ipc_files("t::j1")
        assert list(Path(tmp_path).iterdir()) == []


class TestResultWriteDegradation:
    """两级降级：完整写两连败 → 降级写最小结果（success 改写 retry）。"""

    def test_success_payload_degrades_to_retry(self, tmp_path, monkeypatch):
        """success 完整写失败 → 降级写 retry 结果（重跑而非静默丢失）。"""
        real_write = ArtifactJournal.write_result_atomic
        calls: list[dict] = []

        def flaky_write(journal_self, uid, result_dict, incarnation=None):
            calls.append(result_dict)
            # 完整 payload 模拟磁盘满（大文件写不下）；降级 payload 写成功
            if "raw_result" in result_dict:
                raise OSError(28, "No space left on device")
            return real_write(journal_self, uid, result_dict, incarnation=incarnation)

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", flaky_write)
        journal = ArtifactJournal(tmp_path)

        journal.write_result_with_degradation(
            "h::a", {"status": "success", "raw_result": True}, incarnation=_INCARNATION_FIRST
        )

        res = journal.read_result("h::a", incarnation=_INCARNATION_FIRST)
        assert res is not None, "降级写必须成功落盘（小 payload 可写）"
        assert res["status"] == "retry", (
            f"success 降级必须改写 retry（重跑而非丢失）: {res}"
        )
        assert "IPC_RESULT_WRITE_DEGRADED" in res["error"]
        full_attempts = [c for c in calls if "raw_result" in c]
        assert len(full_attempts) == 2, "完整写应先试两次再降级"
        assert len(calls) == 3, "降级写只补一次（calls[2] 为最小 retry 结果）"

    def test_transient_kind_and_auth_inherited_by_degraded_write(self, tmp_path, monkeypatch):
        """降级写必须继承 transient_kind 与认证令牌（瞬态保真 + 认证不误拒）。"""
        real_write = ArtifactJournal.write_result_atomic
        token = "b" * 64

        def flaky_write(journal_self, uid, result_dict, incarnation=None):
            # 以降级标记区分：降级 payload（error 带 DEGRADED 标记）放行
            if "IPC_RESULT_WRITE_DEGRADED" not in str(result_dict.get("error", "")):
                raise OSError(28, "No space left on device")
            return real_write(journal_self, uid, result_dict, incarnation=incarnation)

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", flaky_write)
        journal = ArtifactJournal(tmp_path)

        journal.write_result_with_degradation(
            "h::a",
            {"status": "retry", "transient_kind": "lock_conflict", "auth": token},
            incarnation=_INCARNATION_FIRST,
        )

        res = journal.read_result("h::a", incarnation=_INCARNATION_FIRST)
        assert res is not None
        assert res["status"] == "retry"
        assert res.get("transient_kind") == "lock_conflict", (
            f"降级写必须保留 transient_kind 结构化字段: {res}"
        )
        assert res.get("auth") == token, "降级结果丢失令牌会被读取侧误拒"

    def test_all_writes_fail_silently_gives_up(self, tmp_path, monkeypatch):
        """完整写 + 降级写全部失败 → 不抛 OSError、无结果文件。"""

        def refusing_write(journal_self, uid, result_dict, incarnation=None):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", refusing_write)
        journal = ArtifactJournal(tmp_path)

        journal.write_result_with_degradation(
            "h::a", {"status": "success", "raw_result": True}, incarnation=_INCARNATION_FIRST
        )
        assert not journal.result_path("h::a", _INCARNATION_FIRST).exists()


class TestStaleResultClaim:
    """残留结果认领：freshness 决胜、.tmp 忽略、坏形状丢弃、兄弟 uid 锚定。"""

    def test_claim_prefers_latest_incarnation_same_instant(self, tmp_path):
        """同刻多执行代残留按 (mtime_ns, incarnation_seq) 决胜。

        秒级 mtime 键在同秒内排序不稳定，可能消费较旧执行代的结果。
        把所有文件的 mtime 钉到同一纳秒，只有 seq 能区分。
        """
        run_id = "a" * 32
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "t::x",
            {"status": "success", "raw_result": encode_raw_result((True, {"v": 1}))},
            incarnation=f"{run_id}.1",
        )
        journal.write_result_atomic(
            "t::x",
            {"status": "success", "raw_result": encode_raw_result((True, {"v": 2}))},
            incarnation=f"{run_id}.2",
        )
        for p in tmp_path.glob("*.result.json"):
            os.utime(p, ns=(1234567890, 1234567890))

        payload = journal.claim_stale_result_payload("t::x")
        assert payload is not None, "应认领最新执行代的成功结果"
        assert decode_raw_result(payload["raw_result"]) == (True, {"v": 2}), (
            "同刻冲突必须取最新执行代"
        )

    def test_claim_ignores_tmp_leftover(self, tmp_path):
        """认领必须忽略 .tmp 残留（孤儿 worker 正在写，读半成品无意义且危险）。"""
        journal = ArtifactJournal(tmp_path)
        base = safe_uid_filename("h::a")
        tmp_file = tmp_path / f"{base}.result.json.tmp"
        tmp_file.write_text('{"status": "suc')  # 部分 JSON

        assert journal.claim_stale_result_payload("h::a") is None
        assert tmp_file.exists(), "认领不得 unlink 正在写的 .tmp 文件"

    def test_claim_discards_non_result_shapes(self, tmp_path):
        """无 status 键 / 非 dict 的残留 → 丢弃返回 None（宁可重跑）。"""
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "h::a", {"not": "a result"}, incarnation=_INCARNATION_FIRST
        )
        assert journal.claim_stale_result_payload("h::a") is None
        assert not journal.result_path("h::a", _INCARNATION_FIRST).exists(), (
            "丢弃的残留文件必须被清理（防重启后重复判定）"
        )

    def test_claim_consumes_and_cleans_all_incarnations(self, tmp_path):
        """认领消费最新残留后，其余执行代残留一并清理。"""
        run_id = "c" * 32
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "h::a", {"status": "retry", "error": "old"}, incarnation=f"{run_id}.1"
        )
        journal.write_result_atomic(
            "h::a", {"status": "retry", "error": "new"}, incarnation=f"{run_id}.2"
        )

        payload = journal.claim_stale_result_payload("h::a")
        assert payload is not None and payload["error"] == "new"
        assert journal.iter_stale_result_paths("h::a") == []

    def test_sibling_uid_dot_suffix_not_matched(self, tmp_path):
        """点后缀兄弟 uid 的结果文件不得被误认成本 uid 的残留。"""
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "h::page.1",
            {"status": "success", "raw_result": True},
            incarnation=_INCARNATION_FIRST,
        )
        assert journal.iter_stale_result_paths("h::page") == [], (
            "点后缀兄弟 uid 的结果文件不得被误判为本 uid 的残留"
        )

    def test_sibling_uid_deep_dot_suffix_not_matched(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "h::a.b.c",
            {"status": "success", "raw_result": True},
            incarnation="deadbeefdeadbeefdeadbeefdeadbeef.7",
        )
        assert journal.iter_stale_result_paths("h::a") == []

    def test_same_uid_incarnation_still_matched(self, tmp_path):
        """本 uid 自己的 incarnation 残留仍须被枚举（锚定修复不误伤正常路径）。"""
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "h::a",
            {"status": "success", "raw_result": True},
            incarnation="deadbeefdeadbeefdeadbeefdeadbeef.3",
        )
        found = journal.iter_stale_result_paths("h::a")
        assert len(found) == 1, f"本 uid 残留应被枚举: {found}"

    def test_cleanup_does_not_delete_sibling_result(self, tmp_path):
        """cleanup_ipc_files 不得删除点后缀兄弟 uid 的结果文件（全链路）。"""
        incarnation = "deadbeefdeadbeefdeadbeefdeadbeef.5"
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "h::page.1",
            {"status": "success", "raw_result": True},
            incarnation=incarnation,
        )
        journal.cleanup_ipc_files("h::page", incarnation)
        assert journal.result_path("h::page.1", incarnation).exists(), (
            "cleanup 不得删除兄弟 uid 的结果文件"
        )


class TestRawResultCodec:
    """tuple 哨兵编解码：合法编码往返、哨兵碰撞原样返回不崩。"""

    def test_tuple_roundtrip(self):
        assert decode_raw_result(encode_raw_result((True, {"v": 1}))) == (True, {"v": 1})
        assert decode_raw_result(encode_raw_result(None)) is None
        assert decode_raw_result(encode_raw_result({"a": 1})) == {"a": 1}

    def test_sentinel_collision_non_sequence_marks_failed_not_crash(self):
        """哨兵碰撞防御：还原段非 list/tuple → 原样返回，不抛 TypeError。"""
        for bad in (123, {"a": 1}, "text", None):
            out = decode_raw_result(["__tl_tuple_v1", bad])
            assert out == ["__tl_tuple_v1", bad], "原样返回，不抛异常"
        assert decode_raw_result(["__tl_tuple_v1", ["x", "y"]]) == ("x", "y")


class TestResultParseContainment:
    """结果/声明文件读取容灾的捕获面完整性：解析失控异常按损坏降级。

    深嵌套 JSON 使 json 解析器抛 RecursionError——它不是 ValueError 子类，
    容灾 except 家族漏接会让「损坏返回 None」承诺被击穿，异常沿认领/
    收割链穿透至主循环崩溃。
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
        assert outputs[1][0] == "[" * 200000

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

        assert journal.drain_signals(uid) == [("gpu", 5.0)], (
            "深嵌套行按坏行跳过，合法信号保留"
        )

    def test_read_outputs_skips_bad_line_keeps_good_ones(self, tmp_path):
        """坏行 + 后随好行 → 坏行跳过、好行保留（continue 语义）。"""
        journal = ArtifactJournal(tmp_path)
        uid = "t::a"
        p = tmp_path / "t%3A%3Aa.outputs.jsonl"
        p.write_text(
            '{"path": "/ok_first", "cleanup": true, "kind": "output"}\n'
            'not-json-line\n'
            '{"path": "/ok_second", "cleanup": true, "kind": "output"}\n',
            encoding="utf-8",
        )
        outputs = journal.read_outputs(uid)
        assert len(outputs) == 2, "坏行应被跳过而非终止读取"
        assert outputs[0][0] == "/ok_first" and outputs[1][0] == "/ok_second"

    def test_read_outputs_skips_empty_line(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "t::a"
        p = tmp_path / "t%3A%3Aa.outputs.jsonl"
        p.write_text(
            '{"path": "/a", "cleanup": true, "kind": "output"}\n'
            '\n'
            '{"path": "/b", "cleanup": true, "kind": "output"}\n',
            encoding="utf-8",
        )
        outputs = journal.read_outputs(uid)
        assert [o[0] for o in outputs] == ["/a", "/b"]

    def test_read_inputs_skips_bad_line_keeps_good_ones(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "t::a"
        p = tmp_path / "t%3A%3Aa.inputs.jsonl"
        p.write_text(
            '{"path": "/in_first", "kind": "file", "size": 1, "mtime_ns": 1}\n'
            'not-json\n'
            '{"path": "/in_second", "kind": "file", "size": 2, "mtime_ns": 2}\n',
            encoding="utf-8",
        )
        entries = journal.read_inputs(uid)
        assert len(entries) == 2, "坏行应被跳过而非终止读取"
        assert entries[0]["path"] == "/in_first" and entries[1]["path"] == "/in_second"


class TestCleanupSandboxRecheck:
    """清理消费侧的沙盒归属复检：outputs 声明是 ipc_dir 上的不可信输入。

    声明侧的沙盒校验可被伪造的 ``.outputs.jsonl`` 声明绕过，任何删除动作
    （unlink / rmtree）执行前必须复检路径归属——解析符号链接与 ``..`` 后
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
        uid = "t::no_roots"
        journal.record_output(uid, str(inside), cleanup=True, kind="cache")
        journal.record_output(uid, str(victim), cleanup=True, kind="cache")

        journal.cleanup(uid, ArtifactCleanupMode.SUCCESS)

        assert not inside.exists(), "ipc_dir 内声明路径照常清理"
        assert victim.exists(), "无信任根时沙盒外路径必须拒绝清理"

    def test_failure_cleanup_rejects_dotdot_escape_and_symlink(self, tmp_path):
        """``..`` 穿越与符号链接间接逃逸的声明路径同样拒绝清理。"""
        journal, _, roots = self._make_journal(tmp_path)
        target = tmp_path / "real_target"
        target.mkdir()
        (target / "f.txt").write_text("data")
        os.symlink(target, roots / "link")
        uid = "t::escape"
        journal.record_output(
            uid, str(roots / "link" / ".." / ".." / "real_target"),
            cleanup=True, kind="output",
        )

        journal.cleanup(uid, ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert target.exists(), "经符号链接与 .. 逃逸出沙盒的路径必须拒绝清理"


class TestArtifactJournalCleanupModes:
    def test_cleanup_pre_submit(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "job_clean_pre"
        journal.record_signal(uid, "gpu", 1.0)
        journal.record_output(uid, "/tmp/out", cleanup=True)
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
