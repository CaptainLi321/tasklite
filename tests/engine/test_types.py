"""引擎停机状态与执行选项值对象。"""

import dataclasses

import pytest

from tasklite.engine.types import (
    ExecutionOptions,
    ExitReason,
    StopMode,
    StepOutcome,
)


class TestStopMode:
    """停机状态机值对象。"""

    def test_member_values(self):
        assert {member.name for member in StopMode} == {"NONE", "DRAINING"}
        assert StopMode.NONE.value == "none"
        assert StopMode.DRAINING.value == "draining"


class TestExitReason:
    """引擎退出原因枚举。"""

    def test_member_values(self):
        assert {member.name for member in ExitReason} == {
            "COMPLETED",
            "STOPPED_DRAINING",
            "INTERRUPTED",
            "ERROR",
        }
        assert ExitReason.COMPLETED.value == "completed"
        assert ExitReason("stopped_draining") is ExitReason.STOPPED_DRAINING


class TestExecutionOptions:
    """execute() 动态选项默认值与不可变性。"""

    def test_defaults(self):
        options = ExecutionOptions()
        assert options.install_signals is True

    def test_frozen(self):
        options = ExecutionOptions()
        with pytest.raises(dataclasses.FrozenInstanceError):
            options.install_signals = False


class TestStepOutcome:
    """step() 单步产物：必填字段 + 尾部默认。"""

    @staticmethod
    def _outcome() -> StepOutcome:
        return StepOutcome(
            dispatched_count=1,
            settled_count=1,
            is_idle=False,
            should_wait=True,
            wait_time=0.5,
            deadlock_detected=False,
            stop_mode=StopMode.NONE,
        )

    def test_tail_defaults(self):
        outcome = self._outcome()
        assert outcome.should_terminate is False
        assert outcome.exit_reason is None

    def test_frozen(self):
        outcome = self._outcome()
        with pytest.raises(dataclasses.FrozenInstanceError):
            outcome.dispatched_count = 9

    def test_explicit_tail_fields(self):
        base = dataclasses.asdict(self._outcome())
        outcome = StepOutcome(
            **{**base, "should_terminate": True, "exit_reason": "error"},
        )
        assert outcome.should_terminate is True
        assert outcome.exit_reason == "error"
