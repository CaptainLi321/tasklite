"""v2 RunSession 单元与属性测试——生命周期、停机单调性与钩子单一出口。

锁定不变式（对任意生成的调用/操作序列成立）：
- stop_mode 单调转移（NONE→DRAINING→ABORTING，唯一入口 request_stop）；
- fire_run_end 幂等：任意多次调用，钩子至多触发一次且取首次 reason；
- 钩子异常不外溢、仅计数（hook_errors）；
- fire_attempt_finished 以 AttemptFinish 值对象原样路由到钩子；
- begin() 复位全部生命周期状态（幂等标志、统计、停机模式、序号）。
"""

from __future__ import annotations

import pytest
from hypothesis import example, given, settings, strategies as st

from tasklite.engine.session import RunSession
from tasklite.engine.types import AttemptFinish, ExitReason, StopMode, TaskStats

# 抛异常钩子可用的异常类型（均为 Exception 子类，模拟不可信钩子代码）
_HOSTILE_EXCEPTIONS = [ValueError, RuntimeError, KeyError, TypeError, IndexError]

_STOP_ORDER = {StopMode.NONE: 0, StopMode.DRAINING: 1, StopMode.ABORTING: 2}


# ── exit_reason 唯一推导与生命周期复位 ─────────────────────────────────


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

    def test_begin_generates_result_token(self):
        session = RunSession()
        assert session.result_token is None
        session.begin()
        assert isinstance(session.result_token, str) and session.result_token
        first = session.result_token
        session.begin()
        assert session.result_token != first, "每次 begin 必须轮换结果认证令牌"

    def test_request_stop_monotonic(self):
        session = RunSession()
        assert session.request_stop(force=False) == StopMode.DRAINING
        assert session.request_stop(force=False) == StopMode.ABORTING
        assert session.request_stop(force=False) == StopMode.ABORTING

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
        def boom(uid, *, outcome):
            raise ValueError("hook is untrusted")

        session = RunSession(on_attempt_finished=boom)
        session.fire_attempt_finished(
            "t::1", outcome=AttemptFinish(success=True, going_to_retry=False, meta={}),
        )
        assert session.stats["hook_errors"] == 1


# ── fire_run_end 幂等性（hypothesis）────────────────────────────────────


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(reasons=[])
@example(reasons=["completed", "completed", "error"])
@given(reasons=st.lists(st.text(max_size=20), max_size=12))
def test_fire_run_end_fires_exactly_once_with_first_reason(reasons):
    """任意 n 次调用：记录型钩子恰好在首次调用触发一次，reason 取首个。"""
    fired = []
    session = RunSession(on_run_end=fired.append)
    for reason in reasons:
        session.fire_run_end(reason)
    expected = [reasons[0]] if reasons else []
    assert fired == expected


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(reasons=["completed", "error"])
@given(reasons=st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=12))
def test_fire_run_end_idempotent_even_when_hook_raises(reasons):
    """幂等标志在钩子调用前置位：即使钩子每次都抛异常，也只进入钩子一次。"""
    calls = []

    def hostile(reason):
        calls.append(reason)
        raise ValueError("钩子不可信")

    session = RunSession(on_run_end=hostile)
    for reason in reasons:
        session.fire_run_end(reason)  # 异常不得外溢
    assert calls == [reasons[0]]
    assert session.stats["hook_errors"] == 1


# ── 钩子异常隔离与计数（hypothesis）────────────────────────────────────


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@given(
    exc_cls=st.sampled_from(_HOSTILE_EXCEPTIONS),
    n=st.integers(min_value=1, max_value=10),
    which=st.sampled_from(["run_start", "attempt_finished"]),
)
def test_hook_exception_swallowed_and_counted(exc_cls, n, which):
    """run_start / attempt_finished 钩子连抛 n 次：异常全部吞掉，hook_errors 恰为 n。"""
    calls = []

    def hostile(*args, **kwargs):
        calls.append((args, kwargs))
        raise exc_cls("钩子不可信")

    session = RunSession(
        on_run_start=hostile if which == "run_start" else None,
        on_attempt_finished=hostile if which == "attempt_finished" else None,
    )
    for i in range(n):
        if which == "run_start":
            session.fire_run_start()
        else:
            session.fire_attempt_finished(
                f"t::{i}",
                outcome=AttemptFinish(success=False, going_to_retry=False, meta={}),
            )
    assert len(calls) == n
    assert session.stats["hook_errors"] == n


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@given(uid=st.text(min_size=1, max_size=30), success=st.booleans(), retry=st.booleans())
def test_attempt_finished_hook_receives_value_object(uid, success, retry):
    """fire_attempt_finished 把 (uid, outcome=AttemptFinish) 原样路由给钩子。"""
    captured = []
    session = RunSession(
        on_attempt_finished=lambda u, *, outcome: captured.append((u, outcome))
    )
    outcome = AttemptFinish(success=success, going_to_retry=retry, meta={"k": 1})
    session.fire_attempt_finished(uid, outcome=outcome)
    assert captured == [(uid, outcome)]
    assert captured[0][1].success is success
    assert captured[0][1].going_to_retry is retry
    assert session.stats["hook_errors"] == 0


# ── request_stop 单调性（hypothesis）────────────────────────────────────


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(forces=[])
@example(forces=[False, False, True])
@given(forces=st.lists(st.booleans(), max_size=20))
def test_request_stop_is_monotone_under_random_force_sequences(forces):
    """任意 force 序列：stop_mode 只升不降，且与逐步模型精确一致。"""
    session = RunSession()
    last = StopMode.NONE
    for force in forces:
        mode = session.request_stop(force=force)
        if force or last == StopMode.DRAINING:
            expected = StopMode.ABORTING
        elif last == StopMode.NONE:
            expected = StopMode.DRAINING
        else:
            expected = last
        assert mode == expected == session.stop_mode
        assert _STOP_ORDER[mode] >= _STOP_ORDER[last]
        last = mode


# ── 任意操作序列下的组合不变式（hypothesis）────────────────────────────


_ACTIONS = [
    "begin",
    "run_start",
    "run_end",
    "attempt_ok",
    "attempt_fail",
    "attempt_retry",
    "stop",
    "stop_force",
]

_EXPECTED_EXIT_BY_STOP = {
    StopMode.NONE: ExitReason.COMPLETED,
    StopMode.DRAINING: ExitReason.STOPPED_DRAINING,
    StopMode.ABORTING: ExitReason.STOPPED_ABORTING,
}


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(actions=[])
@example(actions=["run_end", "begin", "run_end"])
@example(actions=["attempt_ok", "stop", "attempt_retry", "run_end"])
@given(actions=st.lists(st.sampled_from(_ACTIONS), max_size=30))
def test_lifecycle_invariants_hold_for_arbitrary_action_sequences(actions):
    """任意操作序列：幂等标志、异常计数、停机单调性与 exit_reason 推导全部自洽。

    全部钩子为抛异常的敌意实现——异常不外溢由「序列循环未被打破」证明，
    计数由窗口内模型精确对账。
    """
    session = RunSession(
        on_run_start=_raise_always,
        on_attempt_finished=_raise_always,
        on_run_end=_raise_always,
    )
    # 当前 begin 窗口内的模型记账
    run_end_fired = False
    hook_errors_model = 0
    attempt_hook_calls = 0
    last_stop = StopMode.NONE

    for action in actions:
        if action == "begin":
            session.begin()
            run_end_fired = False
            hook_errors_model = 0
            attempt_hook_calls = 0
            last_stop = StopMode.NONE
        elif action == "run_start":
            session.fire_run_start()
            hook_errors_model += 1
        elif action == "run_end":
            session.fire_run_end("completed")
            if not run_end_fired:
                hook_errors_model += 1
            run_end_fired = True
        elif action in ("attempt_ok", "attempt_fail", "attempt_retry"):
            success = action == "attempt_ok"
            going_to_retry = action == "attempt_retry"
            session.fire_attempt_finished(
                "t::1",
                outcome=AttemptFinish(
                    success=success, going_to_retry=going_to_retry, meta={},
                ),
            )
            attempt_hook_calls += 1
            hook_errors_model += 1
        elif action == "stop":
            last_stop = session.request_stop(force=False)
        elif action == "stop_force":
            last_stop = session.request_stop(force=True)

        assert session.stats["hook_errors"] == hook_errors_model
        assert session.stop_mode == last_stop

    # 窗口内 run_end 至多触发一次；attempt 钩子逐次触发
    assert attempt_hook_calls >= 0 and hook_errors_model >= attempt_hook_calls
    assert session.exit_reason() == _EXPECTED_EXIT_BY_STOP[session.stop_mode]


def _raise_always(*_args, **_kwargs):
    """敌意钩子：无论签名如何一律抛异常（模拟不可信钩子代码）。"""
    raise RuntimeError("钩子不可信")
