"""v2 RunSession 单元与属性测试——生命周期、停机幂等性与重试预算。

锁定不变式（对任意生成的调用/操作序列成立）：
- stop_mode 单调转移且幂等（NONE→DRAINING，唯一入口 request_stop，不升级强停）；
- begin() 复位全部生命周期状态（run_id、序号、停机模式、候选来源与每 Run 重试预算）；
- dispatch_no 单调递增；
- 每 Run 重试计数独立记录并在 begin() 复位。
"""

from __future__ import annotations

import pytest
from hypothesis import example, given, settings, strategies as st

from tasklite.engine.session import RunSession
from tasklite.engine.types import ExitReason, StopMode


class TestExitReasonDerivation:
    """exit_reason(exc) 唯一推导：异常类型优先，无异常按 stop_mode。"""

    def test_normal_completion(self):
        session = RunSession()
        assert session.exit_reason() == ExitReason.COMPLETED

    def test_stop_draining_then_drained(self):
        session = RunSession()
        session.request_stop()
        assert session.stop_mode == StopMode.DRAINING
        assert session.exit_reason() == ExitReason.STOPPED_DRAINING

    def test_keyboard_interrupt_beats_stop_mode(self):
        session = RunSession()
        session.request_stop()
        assert session.stop_mode == StopMode.DRAINING
        assert session.exit_reason(KeyboardInterrupt()) == ExitReason.INTERRUPTED

    def test_arbitrary_exception_beats_stop_mode(self):
        session = RunSession()
        session.request_stop()
        assert session.exit_reason(RuntimeError("backend died")) == ExitReason.ERROR


class TestSessionLifecycle:
    def test_begin_resets_everything(self):
        session = RunSession()
        session.begin("run-1")
        session.next_dispatch_no()
        session.next_dispatch_no()
        session.record_retry("t::j1")
        session.mark_retry_candidate("t::j1")
        session.request_stop()

        assert session.get_retry_count("t::j1") == 1
        assert session.is_retry_candidate("t::j1") is True
        assert session.stop_mode == StopMode.DRAINING

        session.begin("run-2")
        assert session.run_id == "run-2"
        assert session.dispatch_no == 0
        assert session.stop_mode == StopMode.NONE
        assert session.get_retry_count("t::j1") == 0
        assert session.is_retry_candidate("t::j1") is False

    def test_request_stop_idempotent_no_abort_upgrade(self):
        session = RunSession()
        assert session.stop_mode == StopMode.NONE
        assert session.request_stop() == StopMode.DRAINING
        assert session.request_stop() == StopMode.DRAINING
        assert session.request_stop() == StopMode.DRAINING
        assert session.stop_mode == StopMode.DRAINING

    def test_dispatch_no_monotonic(self):
        session = RunSession()
        assert session.next_dispatch_no() == 1
        assert session.next_dispatch_no() == 2
        assert session.dispatch_no == 2

    def test_retry_budget_per_run(self):
        session = RunSession()
        assert session.get_retry_count("t::1") == 0
        assert session.record_retry("t::1") == 1
        assert session.record_retry("t::1") == 2
        assert session.get_retry_count("t::1") == 2
        assert session.get_retry_count("t::2") == 0

    def test_retry_candidate_is_independent_from_accumulated_budget(self):
        session = RunSession()
        session.record_retry("t::1")
        session.mark_retry_candidate("t::1")
        assert session.is_retry_candidate("t::1") is True

        session.clear_retry_candidate("t::1")
        assert session.is_retry_candidate("t::1") is False
        assert session.get_retry_count("t::1") == 1


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@given(stops=st.integers(min_value=1, max_value=20))
def test_request_stop_always_draining_under_arbitrary_calls(stops):
    """多次调用 request_stop 恒为 DRAINING，绝不升级为强停。"""
    session = RunSession()
    for _ in range(stops):
        mode = session.request_stop()
        assert mode == StopMode.DRAINING
    assert session.stop_mode == StopMode.DRAINING
