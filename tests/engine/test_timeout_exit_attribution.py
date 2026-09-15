"""超时窗口死亡归因契约测试。

不变式：deadline 后 join 窗口内已自然退出的执行体，死亡归因全权交
exitcode 分簇（0 → `NO_IPC_RESULT` 瞬态、<0 信号死亡、>0 正码崩溃），
与 deadline 前同因事件同果——环境故障不得仅因跨过 deadline 就从
「可重试」分裂为「零重试直落 DLQ」；仅「kill 后仍存活/被击杀」的
执行体才归 `TIMEOUT`。
"""
import time
from types import SimpleNamespace

from tasklite.engine.channel import ExecutionChannel, JobHandle
from tasklite.models.job import Job


def _fake_process(exitcode, *, dies_on_join=True):
    """模拟收割时序：deadline 检查时存活，join(0.5) 后死亡或持续挂起。"""
    state = {"alive": True}

    def join(timeout=None):
        if dies_on_join:
            state["alive"] = False

    def is_alive():
        return state["alive"]

    return SimpleNamespace(
        is_alive=is_alive, exitcode=exitcode, join=join, kill=lambda: None,
        close=lambda: None,
    )


def _handle(ipc_dir, process, *, expired):
    deadline = time.monotonic() - 1 if expired else time.monotonic() + 9999
    return JobHandle(
        uid="t::1", process=process, deadline=deadline, timeout=3600,
        job=Job("t", "1", payload={}), ipc_dir=str(ipc_dir), incarnation="x.1",
    )


def _reap(channel, handle):
    return channel.reap_completed([handle])[0][1]


class TestTimeoutWindowDeathAttribution:
    """reap_completed 超时分支：死亡分类与 deadline 前路径对称。"""

    def test_clean_exit_after_deadline_gets_transient_retry(self, tmp_path):
        """超时后 join 窗口内自然退出（exitcode=0）且无结果 → NO_IPC_RESULT 可重试。"""
        channel = ExecutionChannel(ipc_dir=tmp_path)
        handle = _handle(tmp_path, _fake_process(0), expired=True)
        result = _reap(channel, handle)
        assert result.success is False
        assert "NO_IPC_RESULT" in result.result_meta["error"]
        assert result.retry_requested is True
        assert result.retry_error

    def test_signal_death_after_deadline_gets_transient_retry(self, tmp_path):
        """对照：同窗口信号死亡（exitcode=-9）维持可重试（对称性基准）。"""
        channel = ExecutionChannel(ipc_dir=tmp_path)
        handle = _handle(tmp_path, _fake_process(-9), expired=True)
        result = _reap(channel, handle)
        assert result.retry_requested is True
        assert "PROCESS_CRASH_EXITCODE_-9" in result.result_meta["error"]

    def test_nonzero_exit_after_deadline_gets_transient_retry(self, tmp_path):
        """对照：同窗口正退出码崩溃（exitcode=1）维持可重试。"""
        channel = ExecutionChannel(ipc_dir=tmp_path)
        handle = _handle(tmp_path, _fake_process(1), expired=True)
        result = _reap(channel, handle)
        assert result.retry_requested is True
        assert "PROCESS_CRASH_EXITCODE_1" in result.result_meta["error"]

    def test_kill_survivor_still_reports_timeout(self, tmp_path):
        """真超时：join 后仍存活被击杀 → 维持 TIMEOUT 归因不变。"""
        channel = ExecutionChannel(ipc_dir=tmp_path)
        handle = _handle(
            tmp_path, _fake_process(None, dies_on_join=False), expired=True,
        )
        result = _reap(channel, handle)
        assert "TIMEOUT" in result.result_meta["error"]
