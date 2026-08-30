"""错误分类模型（Transient/Fatal/Unknown + 崩溃循环防护）回归测试。

覆盖：
- TRANSIENT_EXCEPTIONS：连接断开/超时等瞬态异常自动重试（无需包 RetryError）
- register_transient_exception：业务自有异常注册为瞬态
- 崩溃循环防护：连续 commit 失败达阈值 → DLQ 而非无限 crash（兜底）
"""

import pytest

from tasklite.exceptions import (
    TRANSIENT_EXCEPTIONS, FATAL_EXCEPTIONS,
    TransientRegistry, is_transient_exception,
    RetryError,
)
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite
from tests.helpers import make_fake_process_class, patch_multiprocessing_for_fakes


class _ModuleLevelNetworkError(Exception):
    """模块级异常类（spawn 子进程可 pickle 的要求）。"""


class TestTransientClassification:
    def test_connection_errors_are_transient(self):
        """连接类异常被自动归类为瞬态（无需包 RetryError）。"""
        assert is_transient_exception(ConnectionResetError("conn reset"))
        assert is_transient_exception(TimeoutError("took too long"))
        assert is_transient_exception(BrokenPipeError("pipe"))
        assert is_transient_exception(RetryError("explicit"))

    def test_fatal_and_unknown_not_transient(self):
        """确定性 bug 类与未知异常不是瞬态。"""
        assert not is_transient_exception(KeyError("k"))
        assert not is_transient_exception(TypeError("t"))
        assert not is_transient_exception(RuntimeError("unclassified"))

    def test_register_custom_transient(self):
        """per-pipeline 注册表：模块级业务异常注册为瞬态。"""
        registry = TransientRegistry()
        registry.register(_ModuleLevelNetworkError)
        assert is_transient_exception(_ModuleLevelNetworkError("down"), registry.snapshot())
        # 注册后立即生效，且幂等
        registry.register(_ModuleLevelNetworkError)
        assert is_transient_exception(_ModuleLevelNetworkError("down"), registry.snapshot())
        # 隔离语义（架构根治）：未注册的 registry 不受影响
        other = TransientRegistry()
        assert not is_transient_exception(_ModuleLevelNetworkError("down"), other.snapshot())

    def test_register_rejects_function_scope_class(self):
        """ fail-loud：函数作用域定义的类无法随 spawn 下发 → 注册即 TypeError。"""
        class LocalError(Exception):
            pass

        with pytest.raises(TypeError, match="module-level"):
            TransientRegistry().register(LocalError)

    def test_register_rejects_non_exception(self):
        """register_transient_exception 拒绝非 Exception 类。"""
        with pytest.raises(TypeError):
            TransientRegistry().register(int)  # type: ignore


class TestTransientAutoRetry:
    def test_connection_error_auto_retries_then_succeeds(self, tmp_path, monkeypatch):
        """真实子进程路径：handler 抛 ConnectionResetError → 自动重试 → 成功后进 wall。

        用 FakeProcess 模拟两次 IPC 结果：先 retry（瞬态），后 success。
        """
        from tests.helpers import make_ipc_process_class
        patch_multiprocessing_for_fakes(
            monkeypatch,
            fake_process_class=make_ipc_process_class(
                results=[
                    {"status": "retry", "error": "ConnectionResetError: conn reset"},
                    {"status": "success", "raw_result": True,
                     "new_jobs": [], "resource_suspensions": [], "cursor_updates": {}},
                ],
            ),
        )
        pipeline = TaskLite(
            name="test_transient", state_dir=tmp_path / "state", backend="sqlite", max_workers=1,
        )
        pipeline.register_handler("t", lambda j, c: True)
        pipeline.enqueue([Job("t", "j1", payload={}, backoff_base=0.01)])
        pipeline.run()

        # 重试后成功：job 进 wall，不在 failed
        assert "t::j1" in pipeline.backend.load_wall()
        assert "t::j1" not in pipeline.backend.load_failed()
        assert pipeline.stats["retried"] >= 1, "瞬态异常必须自动重试"


class TestCommitFailureDLQGuard:
    def test_commit_failure_guard_dlqs_after_threshold(self, tmp_path, monkeypatch):
        """崩溃循环防护：连续 commit 失败达阈值 → DLQ，而非无限 crash。

        模拟 job_dict 已带 _commit_failures=2（前两次已失败），本次再次
        commit 失败 → 达阈值 → 进 DLQ（不再 crash）。
        """
        from tasklite.models.state import PipelineState
        from tasklite.backend.sqlite_backend import SQLiteStateBackend
        import json, sqlite3

        pipeline = TaskLite(
            name="test_dlq_guard", state_dir=tmp_path / "state", backend="sqlite",
        )
        # 直接构造一个 _commit_failures=2 的 job 进磁盘队列
        jd = Job("t", "j1", payload={}).to_dict()
        jd["runtime"] = {"_commit_failures": 2}
        conn = sqlite3.connect(tmp_path / "state" / "test_dlq_guard_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json.dumps(jd),))
        conn.commit()
        conn.close()

        # 让 commit 失败：monkeypatch backend.commit_job_success 返回 False
        def fake_commit(*a, **kw):
            return False
        monkeypatch.setattr(pipeline.backend, "commit_job_success", fake_commit)

        patch_multiprocessing_for_fakes(
            monkeypatch, fake_process_class=make_fake_process_class("success"),
        )

        # 达阈值 → 走 DLQ 而非 crash
        pipeline.run()
        assert "t::j1" in pipeline.backend.load_failed(), \
            "连续 commit 失败达阈值必须 DLQ 而非无限 crash"
        assert "t::j1" not in pipeline.backend.load_wall()
