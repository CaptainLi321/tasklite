"""_decode_ipc_result 损坏结果防御分支触发测试。

防御矩阵（触发输入）：
- new_jobs 坏 dict                    → res["new_jobs"] 含缺 job_id 的 dict
- cursor_updates 值非法               → int 值
- cursor_updates 类型非法             → cursor_updates 非 dict
- resource_suspensions 元素非法       → [("api", "x")] 值非数字
- resource_suspensions 类型非法       → resource_suspensions 非 list
- fatal 状态                          → status="fatal"
- 未知 status                         → status="weird" / 进程 exitcode != 0
- Missing output                      → 声明输出文件不存在
"""

import json

import pytest

from tasklite.engine.channel import (
    _decode_ipc_result,
)
from tasklite.models.job import Job
from tasklite.utils.ipc import ArtifactJournal

# 合法的成功结果基线（其余字段全部正常，只变异目标字段）
_BASE_OK = {
    "status": "success",
    "raw_result": {"ok": True},
    "new_jobs": [],
    "cursor_updates": {},
    "resource_suspensions": [],
}


def _decode(res, p=None, ipc_dir=None):
    return _decode_ipc_result(res, p, Job("t", "a"), ipc_dir)


class TestDecodeIpcsResultDefense:
    """A 组：_decode_ipc_result 损坏结果防御分支的变异体击杀测试。"""

    def test_missing_raw_result_key_marks_failed(self):
        """防御分支：status=success 但缺 raw_result（损坏/外来文件）→ 失败。"""
        res = dict(_BASE_OK)
        del res["raw_result"]
        result = _decode(res)
        assert result.success is False
        assert "CORRUPT_RESULT_FILE" in result.result_meta["error"]

    def test_sentinel_collision_non_sequence_marks_failed_not_crash(self):
        """哨兵碰撞防御：用户 handler 返回 ["__tl_tuple_v1", 123] 撞哨兵
        且还原段非 list/tuple → 原样返回 list，下游以 invalid-return-type
        明确失败进 DLQ——绝不 tuple(123) 抛 TypeError 穿透 drain 崩 run。"""
        from tasklite.engine.channel import _decode_raw_result
        for bad in (123, {"a": 1}, "text", None):
            out = _decode_raw_result(["__tl_tuple_v1", bad])
            assert out == ["__tl_tuple_v1", bad] # 原样返回，不抛异常
        # 合法编码不受影响
        assert _decode_raw_result(["__tl_tuple_v1", ["x", "y"]]) == ("x", "y")

    def test_success_decode_crash_converted_to_corrupt_result(self, monkeypatch):
        """解码异常兜底：success 分支解码/归一化段任何意外异常 → 转
        CORRUPT_RESULT_FILE 失败结果，不穿透 drain 崩 run（防御矩阵最后
        一级）。KeyboardInterrupt/SystemExit 不被吞（只捕 Exception）。"""
        res = dict(_BASE_OK)
        monkeypatch.setattr(
            "tasklite.engine.channel._normalize_handler_result",
            lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        result = _decode(res)
        assert result.success is False
        assert result.result_meta["error"].startswith("CORRUPT_RESULT_FILE: decode failed")
        assert "traceback" in result.result_meta

    def test_malformed_spawned_job_marks_failed(self):
        """防御分支（mutmut_44/45）：new_jobs 含坏 dict（缺 job_id）→ 失败
        而非上抛崩 run（宁可 DLQ，不可崩）；坏条目**不阻断**后续 cursor/
        suspension 校验。"""
        res = dict(_BASE_OK, new_jobs=[{"task_type": "t"}])  # 缺 job_id
        result = _decode(res)
        assert result.success is False
        assert "invalid spawned job dict" in result.result_meta["error"]

    def test_cursor_updates_type_not_dict_marks_failed(self):
        """防御分支（mutmut_82）：cursor_updates 非 dict（损坏文件注入
        list/str）→ 失败。"""
        res = dict(_BASE_OK, cursor_updates=["k", "v"])
        result = _decode(res)
        assert result.success is False
        assert "invalid cursor_updates type" in result.result_meta["error"]

    def test_cursor_updates_value_not_str_marks_failed(self):
        """防御分支（mutmut_72/81）：cursor_updates 值非 str/None（int 等）
        → 失败（防 state.cursors 内存/磁盘类型不一致）。"""
        res = dict(_BASE_OK, cursor_updates={"k": 123})
        result = _decode(res)
        assert result.success is False
        assert "invalid cursor_updates value" in result.result_meta["error"]

    def test_resource_suspensions_bad_element_marks_failed(self):
        """防御分支（mutmut_99/100）：resource_suspensions 元素形状非法
        （值非数字）→ 失败（防 _apply_result 解包崩溃）。"""
        res = dict(_BASE_OK, resource_suspensions=[("api", "not-a-number")])
        result = _decode(res)
        assert result.success is False
        assert "invalid resource_suspension entry" in result.result_meta["error"]

    def test_resource_suspensions_not_list_marks_failed(self):
        """防御分支（mutmut_106/107）：resource_suspensions 非 list →
        失败。"""
        res = dict(_BASE_OK, resource_suspensions={"api": 1.0})
        result = _decode(res)
        assert result.success is False
        assert "invalid resource_suspensions type" in result.result_meta["error"]

    def test_fatal_status_marks_failed(self):
        """防御分支（mutmut_137）：status=fatal → 失败且 meta 带 fatal 标记。"""
        res = {"status": "fatal", "error": "boom", "traceback": "tb"}
        result = _decode(res)
        assert result.success is False
        assert result.result_meta["fatal"] is True
        assert result.result_meta["error"] == "boom"

    def test_unknown_status_marks_failed(self):
        """防御分支（mutmut_161/185）：未知 status → 失败（不静默放行）。"""
        res = {"status": "weird"}
        result = _decode(res)
        assert result.success is False
        assert "unknown status" in result.result_meta["error"]

    def test_crash_exitcode_marks_failed(self):
        """防御分支（mutmut_186）：res 无 status 键（损坏/外来文件）且进程
        exitcode != 0 → PROCESS_CRASH 分类失败（mutmut_186 变异的
        success=False→True 分支点）。"""
        class _P:
            exitcode = -9
        res = {"raw": "not-a-result-dict"}  # 无 status 键 → 走 exitcode 分支
        result = _decode(res, p=_P())
        assert result.success is False
        assert "PROCESS_CRASH_EXITCODE_-9" in result.result_meta["error"]

    def test_missing_output_marks_failed(self, tmp_path):
        """防御分支（mutmut_208）：成功但声明输出文件不存在 → Missing output
        失败（输出存在性校验）。"""
        ipc = str(tmp_path)
        uid = "t::a"
        ArtifactJournal(ipc).record_output(uid, str(tmp_path / "missing.jpg"), True)
        result = _decode(dict(_BASE_OK), ipc_dir=ipc)
        assert result.success is False
        assert "Missing output" in result.result_meta["error"]

    def test_present_output_passes(self, tmp_path):
        """对偶路径：声明输出文件存在 → 校验通过（防「缺失→失败」误伤
        正常路径）。"""
        ipc = str(tmp_path)
        uid = "t::a"
        out = tmp_path / "present.jpg"
        out.write_bytes(b"x")
        ArtifactJournal(ipc).record_output(uid, str(out), True)
        result = _decode(dict(_BASE_OK), ipc_dir=ipc)
        assert result.success is True

    def test_cache_output_skips_existence_check(self, tmp_path):
        """对偶路径：kind=cache 的临时文件跳过存在性校验（原子产出的 .part
        已被 os.replace，校验必然失败——跳过是设计而非漏洞）。"""
        ipc = str(tmp_path)
        uid = "t::a"
        ArtifactJournal(ipc).record_output(uid, str(tmp_path / "cache.part"), True, kind="cache")
        result = _decode(dict(_BASE_OK), ipc_dir=ipc)
        assert result.success is True


class TestBuildTerminalFailureDefense:
    """B 组：_build_terminal_failure 终止分类防御分支（README「重构与稳定决策」/ CHANGELOG（mutmut 基线历史）
    2.2 节 B 组，mutmut_11/17/34）——变异体存活的触发测试。"""

    def test_timeout_transient_requests_retry(self):
        """防御分支（mutmut_11/17）：timeout_is_transient=True 时超时 → 按
        瞬态重试（success=False + retry_requested=True）而非直接 DLQ。"""
        from tasklite.engine.channel import (
            ExecutionChannel, JobHandle,
        )

        class _P:
            exitcode = None

        class _Job:
            timeout_is_transient = True

        handle = JobHandle(
            uid="t::a", process=_P(), deadline=1.0, timeout=5.0,
            job=_Job(), ipc_dir="/tmp",
        )
        result = ExecutionChannel._build_terminal_failure(
            _P(), handle, is_timeout=True,
        )
        assert result.success is False
        assert result.retry_requested is True
        assert "TIMEOUT" in result.retry_error

    def test_timeout_not_transient_marks_failed(self):
        """对偶路径：timeout_is_transient=False（默认）时超时 → 直接失败
        （TIMEOUT 错误，不重试）。"""
        from tasklite.engine.channel import (
            ExecutionChannel, JobHandle,
        )

        class _P:
            exitcode = None

        class _Job:
            timeout_is_transient = False

        handle = JobHandle(
            uid="t::a", process=_P(), deadline=1.0, timeout=5.0,
            job=_Job(), ipc_dir="/tmp",
        )
        result = ExecutionChannel._build_terminal_failure(
            _P(), handle, is_timeout=True,
        )
        assert result.success is False
        assert result.retry_requested is False
        assert "TIMEOUT" in result.result_meta["error"]

    def test_crash_exitcode_classified(self):
        """对偶路径：非超时且 exitcode != 0 → PROCESS_CRASH 分类（mutmut_34
        的 success=False→None 变异点）。"""
        from tasklite.engine.channel import (
            ExecutionChannel, JobHandle,
        )

        class _P:
            exitcode = -11

        class _Job:
            timeout_is_transient = False

        handle = JobHandle(
            uid="t::a", process=_P(), deadline=1.0, timeout=5.0,
            job=_Job(), ipc_dir="/tmp",
        )
        result = ExecutionChannel._build_terminal_failure(
            _P(), handle, is_timeout=False,
        )
        assert result.success is False
        assert "PROCESS_CRASH_EXITCODE_-11" in result.result_meta["error"]

    def test_no_ipc_result_classified(self):
        """对偶路径：非超时、exitcode 为 0/None → NO_IPC_RESULT（mutmut_34
        的 success=False→None 另一变异点）。"""
        from tasklite.engine.channel import (
            ExecutionChannel, JobHandle,
        )

        class _P:
            exitcode = 0

        class _Job:
            timeout_is_transient = False

        handle = JobHandle(
            uid="t::a", process=_P(), deadline=1.0, timeout=5.0,
            job=_Job(), ipc_dir="/tmp",
        )
        result = ExecutionChannel._build_terminal_failure(
            _P(), handle, is_timeout=False,
        )
        assert result.success is False
        assert "NO_IPC_RESULT" in result.result_meta["error"]


class TestReadDeclarationsSkipBadLines:
    """C 组：read_outputs/read_inputs/read_signals 的坏行跳过语义（docs/
    README「重构与稳定决策」/ CHANGELOG（mutmut 2.2 节） C 组，mutmut 的 continue→break 变异）——
    「坏行后仍有合法声明」时 continue 必须跳过而非终止读取。"""

    def test_read_outputs_skips_bad_line_keeps_good_ones(self, tmp_path):
        """坏行 + 后随好行 → 坏行跳过、好行保留（continue→break 变异点）。"""
        ipc = str(tmp_path)
        uid = "t::a"
        p = tmp_path / f"t%3A%3Aa.outputs.jsonl"
        p.write_text(
            '{"path": "/ok1", "cleanup": true, "kind": "output"}\n'
            'not-json-line\n'                      # 坏行
            '{"path": "/ok2", "cleanup": true, "kind": "output"}\n',  # 坏行后的好行
            encoding="utf-8",
        )
        outputs = ArtifactJournal(ipc).read_outputs(uid)
        assert len(outputs) == 2, "坏行应被跳过而非终止读取"
        assert outputs[0][0] == "/ok1" and outputs[1][0] == "/ok2"

    def test_read_outputs_skips_empty_line(self, tmp_path):
        """空行跳过（continue→break 的另一变异点：空行后仍有数据）。"""
        ipc = str(tmp_path)
        uid = "t::a"
        p = tmp_path / f"t%3A%3Aa.outputs.jsonl"
        p.write_text(
            '{"path": "/a", "cleanup": true, "kind": "output"}\n'
            '\n'
            '{"path": "/b", "cleanup": true, "kind": "output"}\n',
            encoding="utf-8",
        )
        outputs = ArtifactJournal(ipc).read_outputs(uid)
        assert [o[0] for o in outputs] == ["/a", "/b"]

    def test_read_signals_skips_bad_line_keeps_good_ones(self, tmp_path):
        """signals 坏行跳过 + 后随好行保留（continue→break 变异点）。"""
        ipc = str(tmp_path)
        uid = "t::a"
        p = tmp_path / f"t%3A%3Aa.signals.jsonl"
        p.write_text(
            '{"suspend": ["api", 1.0]}\n'
            'garbage\n'
            '{"suspend": ["api", 2.0]}\n',
            encoding="utf-8",
        )
        signals = ArtifactJournal(ipc).drain_signals(uid)
        assert len(signals) == 2, "坏行应被跳过而非终止读取"
        assert signals[0] == ("api", 1.0) and signals[1] == ("api", 2.0)

    def test_read_inputs_skips_bad_line_keeps_good_ones(self, tmp_path):
        """inputs 坏行跳过 + 后随好行保留（continue→break 变异点）。"""
        ipc = str(tmp_path)
        uid = "t::a"
        p = tmp_path / f"t%3A%3Aa.inputs.jsonl"
        p.write_text(
            '{"path": "/in1", "kind": "file", "size": 1, "mtime_ns": 1}\n'
            'not-json\n'
            '{"path": "/in2", "kind": "file", "size": 2, "mtime_ns": 2}\n',
            encoding="utf-8",
        )
        entries = ArtifactJournal(ipc).read_inputs(uid)
        assert len(entries) == 2, "坏行应被跳过而非终止读取"
        assert entries[0]["path"] == "/in1" and entries[1]["path"] == "/in2"


class TestMainLoopContinuesAfterDirectHandle:
    """C 组：主循环 `entry is None → continue`（README「重构与稳定决策」/ CHANGELOG（mutmut 基线历史） 2.2
    节 C 组，主循环扫描块 mutmut_35 的 continue→break 变异）——派发
    预检关直接处理（NO_HANDLER/依赖失败/payload 校验失败）后必须**继续
    填池**处理后续 job，而非 break 提前终止本轮。"""

    def test_no_handler_then_valid_job_both_processed(self, tmp_path, monkeypatch):
        """NO_HANDLER job 与正常 job 混合入队 → 两者都被处理（NO_HANDLER 进
        DLQ、正常 job 完成），证明派发 None 后 continue 而非 break。"""
        from tests.helpers import make_fake_process_class, patch_multiprocessing_for_fakes
        from tasklite import TaskLite
        from tasklite.models.job import Job

        pipeline = TaskLite(
            name="t", state_dir=str(tmp_path / "state"), max_workers=1,
        )
        pipeline.register_handler("good", lambda j, c: (True, {}))
        pipeline.enqueue([
            Job("no_handler", "j1"),
            Job("good", "j2"),
        ])
        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        pipeline.run()
        failed = pipeline.backend.load_failed()
        wall = pipeline.backend.load_wall()
        assert "no_handler::j1" in failed, "NO_HANDLER 应进 DLQ"
        assert "good::j2" in wall, "NO_HANDLER 处理后的正常 job 仍应被派发执行"
