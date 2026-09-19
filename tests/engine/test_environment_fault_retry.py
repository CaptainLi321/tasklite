"""环境故障（正退出码崩溃 / 无结果文件）可重试契约测试。

不变式：子进程执行体的环境性死亡（非零退出码、正常退出但无结果文件）
与信号死亡同属瞬态故障——result 文件缺失/半写通常是解释器启动失败、
导入段崩溃、OOM 前兆等环境问题，按瞬态走重试预算而非零重试直判 DLQ
（与信号死亡路径对称）。
"""
from types import SimpleNamespace

import pytest

from tasklite.engine.channel import ExecutionChannel, ExecutionResult, JobHandle, _decode_ipc_result
from tasklite.models.job import Job
from tests.helpers import ok_handler


class _Job:
    timeout_is_transient = False


def _handle(tmp_path):
    return JobHandle(
        uid="t::a", process=None, deadline=0.0, timeout=10,
        job=Job("t", "a"), ipc_dir=str(tmp_path),
    )


class TestDecodeEnvFaultRetry:
    """_decode_ipc_result：损坏/缺失结果文件场景归为可重试。"""

    def test_nonzero_exitcode_without_status_requests_retry(self):
        """res 无 status 键且进程正退出码（如 sys.exit(1)）→ 可重试。"""
        res = {"raw": "not-a-result-dict"}
        result = _decode_ipc_result(res, SimpleNamespace(exitcode=1), _Job())
        assert result.success is False
        assert result.retry_requested is True
        assert "PROCESS_CRASH_EXITCODE_1" in result.result_meta["error"]
        assert "PROCESS_CRASH_EXIT" in (result.retry_error or "")

    def test_missing_status_without_process_requests_retry(self):
        """claim 路径（p=None）读到无 status 的残留文件 → NO_IPC_RESULT 可重试。"""
        result = _decode_ipc_result({}, None, _Job())
        assert result.success is False
        assert result.retry_requested is True
        assert "NO_IPC_RESULT" in result.result_meta["error"]


class TestTerminalFailureRetry:
    """_build_terminal_failure：进程死亡但无结果文件的环境故障可重试。"""

    def test_nonzero_exitcode_requests_retry(self, tmp_path):
        """正退出码（handler sys.exit(1)/环境崩溃）→ 可重试，不再零重试直判 DLQ。"""
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=1), _handle(tmp_path), is_timeout=False)
        assert result.success is False
        assert result.retry_requested is True
        assert "PROCESS_CRASH_EXITCODE_1" in result.result_meta["error"]
        assert result.retry_error

    def test_clean_exit_without_result_requests_retry(self, tmp_path):
        """正常退出（exitcode=0）但无结果文件 → NO_IPC_RESULT 可重试。"""
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=0), _handle(tmp_path), is_timeout=False)
        assert result.success is False
        assert result.retry_requested is True
        assert "NO_IPC_RESULT" in result.result_meta["error"]
        assert result.retry_error

    def test_signal_death_still_requests_retry(self, tmp_path):
        """对照：信号死亡路径维持可重试（对称性基准）。"""
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=-9), _handle(tmp_path), is_timeout=False)
        assert result.retry_requested is True


class TestEnvFaultEndToEnd:
    """端到端：正退出码崩溃走重试预算，耗尽后 DLQ 携带终态原因。"""

    def test_nonzero_exitcode_exhausts_budget_then_dlq(self, tmp_path, monkeypatch):
        from tasklite.taxonomy import ERR_MAX_RETRIES

        from tests.helpers import make_ipc_process_class, make_pipeline, patch_multiprocessing_for_fakes

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("crash", ok_handler)
        # max_retries=0：首次正退出码崩溃即耗尽预算 → 验证重试耗尽终态
        pipeline.enqueue([Job("crash", "j1", payload={}, max_retries=0)])

        CrashProcess = make_ipc_process_class(exitcode=1)
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=CrashProcess)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "crash::j1" in failed
        entry = failed["crash::j1"]
        assert entry["error"] == ERR_MAX_RETRIES
        assert "PROCESS_CRASH_EXIT" in entry.get("retry_error", "")
