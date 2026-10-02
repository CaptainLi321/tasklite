"""v2 引擎值对象不变式与瞬态信号登记表完备性锁定测试。

移植 v1 test_transient_signal_rules.py 的登记表层断言（kind 集合穷举 +
统计键属公开监控面）并按 v2 词汇改写；军规四性（不烧预算 / 豁免失败档案
/ 零污染）的行为级断言由后续 store/completion 单元的测试承接，此处锁定
登记表与值对象本身的形状。
"""

import dataclasses

import pytest

from tasklite.engine.types import (
    EMPTY_STATS,
    TRANSIENT_KIND_STAT_KEYS,
    ExecutionOptions,
    ExitReason,
    RunSummary,
    StopMode,
    StepOutcome,
    TaskStats,
)

_EXPECTED_STAT_KEYS = {
    "completed",
    "failed",
    "retried",
    "skipped",
    "hook_errors",
    "deferred_orphan",
    "interrupted_reruns",
    "rate_limited_reruns",
    "cascade_failed",
}


class TestEmptyStats:
    """EMPTY_STATS 基准形状：九键全零，TaskStats 以其为初始快照而非别名。"""

    def test_nine_keys_all_zero(self):
        assert set(EMPTY_STATS) == _EXPECTED_STAT_KEYS
        assert all(value == 0 for value in EMPTY_STATS.values())

    def test_task_stats_initial_snapshot(self):
        stats = TaskStats()
        assert dict(stats) == EMPTY_STATS

    def test_task_stats_mutation_does_not_alias_template(self):
        """不变式：TaskStats 是 EMPTY_STATS 的拷贝初始化——实例计数不得
        写穿模板，否则并发 run 之间统计互污。"""
        stats = TaskStats()
        stats["completed"] += 1
        stats["failed"] = 7
        assert EMPTY_STATS["completed"] == 0
        assert EMPTY_STATS["failed"] == 0
        assert TaskStats()["completed"] == 0


class TestTransientKindRegistry:
    """瞬态信号登记表完备性（v1 同源断言按 v2 词汇移植）。

    登记表是瞬态军规的展开点：wire 产生端的全部 kind 必须在此登记一行，
    且统计键必须落在公开监控面（EMPTY_STATS）内——漏登记的信号会静默
    落入普通失败路径（烧预算 + 落失败档案）。
    """

    @pytest.mark.parametrize("stat_key", sorted(TRANSIENT_KIND_STAT_KEYS.values()))
    def test_stat_keys_are_public_api(self, stat_key):
        """kind→统计键映射不得指向公开 EMPTY_STATS 九键之外的键
        （统计键逐键文档化，改键名即破坏监控面）。"""
        assert stat_key in EMPTY_STATS

    def test_known_kinds_are_all_registered(self):
        """wire 产生端全部 kind 均已登记（decode 接受集合 == 注册表键集）。"""
        assert set(TRANSIENT_KIND_STAT_KEYS) == {
            "interrupted",
            "lock_conflict",
            "rate_limited",
        }

    def test_stat_key_per_kind_is_distinct(self):
        """每种 kind 独立计数：统计键一一对应（重复键会让两种信号互吞计数）。"""
        values = list(TRANSIENT_KIND_STAT_KEYS.values())
        assert len(set(values)) == len(values)


class TestStopMode:
    """停机状态机三态值对象。"""

    def test_member_values(self):
        assert {member.name for member in StopMode} == {"NONE", "DRAINING", "ABORTING"}
        assert StopMode.NONE.value == "none"
        assert StopMode.DRAINING.value == "draining"
        assert StopMode.ABORTING.value == "aborting"


class TestExitReason:
    """引擎退出原因枚举。"""

    def test_member_values(self):
        assert {member.name for member in ExitReason} == {
            "COMPLETED",
            "STOPPED_DRAINING",
            "STOPPED_ABORTING",
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
        assert options.acquire_run_lock is True

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
            completed_count=1,
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


class TestRunSummary:
    """运行摘要契约：异常通道与摘要通道互斥。"""

    @staticmethod
    def _summary() -> RunSummary:
        return RunSummary(
            exit_reason=ExitReason.COMPLETED,
            stats=TaskStats(),
            run_id="run_seed.1",
            duration_seconds=1.25,
        )

    def test_unhandled_exception_defaults_none(self):
        """execute() 自身永不回填 unhandled_exception（恒 None）——未处理
        异常一律经 raise 通道原样上抛，该字段只由包装层显式回填。"""
        summary = self._summary()
        assert summary.unhandled_exception is None

    def test_frozen(self):
        summary = self._summary()
        with pytest.raises(dataclasses.FrozenInstanceError):
            summary.run_id = "other"

    def test_explicit_unhandled_exception_backfill(self):
        exc = RuntimeError("wrapper observed crash")
        summary = RunSummary(
            exit_reason=ExitReason.ERROR,
            stats=TaskStats(),
            run_id="run_seed.2",
            duration_seconds=0.0,
            unhandled_exception=exc,
        )
        assert summary.unhandled_exception is exc
