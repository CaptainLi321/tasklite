"""RunSession 单元测试——exit_reason 唯一推导与生命周期复位语义锁定。"""

from __future__ import annotations

import pytest

from tasklite.engine.session import RunSession
from tasklite.engine.types import ExitReason, StopMode, TaskStats


class TestExitReasonDerivation:
    """exit_reason(exc) 唯一推导：异常类型优先，无异常按 stop_mode 三态。"""

    def test_normal_completion(self):
        session = RunSession()
        assert session.exit_reason() == ExitReason.COMPLETED

    def test_stop_draining_then_drained(self):
        # 锁定 case：stop() 后正常排空 → STOPPED_DRAINING
        session = RunSession()
        session.request_stop(force=False)
        assert session.stop_mode == StopMode.DRAINING
        assert session.exit_reason() == ExitReason.STOPPED_DRAINING

    def test_stop_aborting(self):
        session = RunSession()
        session.request_stop(force=True)
        assert session.exit_reason() == ExitReason.STOPPED_ABORTING

    def test_keyboard_interrupt_beats_stop_mode(self):
        # 锁定 case：二次信号后 KeyboardInterrupt——信号 handler 已把
        # stop_mode 升到 ABORTING，仍以异常类型为准报 INTERRUPTED
        session = RunSession()
        session.request_stop(force=False)
        session.request_stop(force=True)
        assert session.stop_mode == StopMode.ABORTING
        assert session.exit_reason(KeyboardInterrupt()) == ExitReason.INTERRUPTED

    def test_arbitrary_exception_beats_stop_mode(self):
        session = RunSession()
        session.request_stop(force=False)
        assert session.exit_reason(RuntimeError("backend died")) == ExitReason.ERROR


class TestSessionLifecycle:
    def test_begin_resets_everything(self):
        session = RunSession()
        session.begin("run-1")
        session.next_dispatch_seq()
        session.next_dispatch_seq()
        session.stats["completed"] = 3
        session.request_stop(force=True)
        session.fire_run_end("error")  # 置幂等标志

        session.begin("run-2")
        assert session.run_id == "run-2"
        assert session.dispatch_seq == 0
        assert session.stop_mode == StopMode.NONE
        assert isinstance(session.stats, TaskStats)
        assert session.stats["completed"] == 0
        # begin 后 fire_run_end 重新可发（幂等标志已复位）
        fired = []
        session.on_run_end = fired.append
        session.fire_run_end("completed")
        assert fired == ["completed"]

    def test_request_stop_monotonic(self):
        session = RunSession()
        assert session.request_stop(False) == StopMode.DRAINING
        assert session.request_stop(False) == StopMode.ABORTING
        assert session.request_stop(False) == StopMode.ABORTING

    def test_dispatch_seq_monotonic(self):
        session = RunSession()
        assert session.next_dispatch_seq() == 1
        assert session.next_dispatch_seq() == 2
        assert session.dispatch_seq == 2

    def test_fire_run_end_idempotent(self):
        fired = []
        session = RunSession(on_run_end=fired.append)
        session.fire_run_end("completed")
        session.fire_run_end("completed")
        assert fired == ["completed"]

    def test_hook_error_counted_not_raised(self):
        def boom(uid, meta, success, retry):
            raise ValueError("hook is untrusted")

        session = RunSession(on_job_completed=boom)
        session.fire_job_completed("t::1", {}, True, False)
        assert session.stats["hook_errors"] == 1
