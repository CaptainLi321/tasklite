"""DeadlockGovernor 死锁治理深模块单元与状态机测试。"""

from tasklite import Job
from tasklite.engine.governor import DeadlockGovernor
from tasklite.models.state import PipelineState
from tasklite.taxonomy import (
    ERR_DEADLOCK_GAP,
    ERR_DEPENDENCY_DEADLOCK,
    ERR_JOB_DEPENDENCY,
    ERR_MALFORMED_JOB,
    ERR_RESOURCE_DEADLOCK,
)


def test_deadlock_governor_reset():
    gov = DeadlockGovernor(dep_grace_seconds=10.0, deadlock_gap_max_rounds=3)
    gov.dep_grace_deadline = 100.0
    gov.dep_grace_missing = frozenset(["t::j1"])
    gov.deadlock_gap_rounds = 2
    gov.reset()
    assert gov.dep_grace_deadline is None
    assert gov.dep_grace_missing is None
    assert gov.deadlock_gap_rounds == 0


def test_deadlock_governor_gap_escalation():
    gov = DeadlockGovernor(deadlock_gap_max_rounds=3)
    assert not gov.check_gap_or_escalate("Gap prefix")
    assert gov.deadlock_gap_rounds == 1
    assert not gov.check_gap_or_escalate("Gap prefix")
    assert gov.deadlock_gap_rounds == 2
    assert gov.check_gap_or_escalate("Gap prefix")
    assert gov.deadlock_gap_rounds == 3


def test_deadlock_governor_grace_period(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    gov = DeadlockGovernor(dep_grace_seconds=5.0)
    # job a depends on missing b, but job c has no missing dependencies
    state = PipelineState(
        wall={},
        failed={},
        cursors={},
        queue=[
            Job("t", "a", depends_on=["t::b"]).to_dict(),
            Job("t", "c").to_dict(),
        ],
    )
    # First check: grants grace
    in_grace = gov.check_dependency_grace(state, ["t::b"], now=100.0)
    assert in_grace is True
    assert gov.dep_grace_deadline == 105.0
    assert gov.dep_grace_missing == frozenset(["t::b"])

    # Before expiry: still in grace
    assert gov.check_dependency_grace(state, ["t::b"], now=102.0) is True

    # After expiry: grace expired
    assert gov.check_dependency_grace(state, ["t::b"], now=106.0) is False


def test_deadlock_governor_spawner_fast_path(monkeypatch):
    """验证提供 has_potential_spawners 时直接依据单趟事实裁决，零扫描 state.queue。"""
    monkeypatch.setattr("time.sleep", lambda s: None)
    gov = DeadlockGovernor(dep_grace_seconds=5.0)
    # 构造空队列或不含可运行 job 的 state
    state = PipelineState(wall={}, failed={}, cursors={}, queue=[])

    # 1. has_potential_spawners=False -> 立即返回 False
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=False, now=100.0) is False
    assert gov.dep_grace_deadline is None

    # 2. has_potential_spawners=True -> 进入宽限
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=100.0) is True
    assert gov.dep_grace_deadline == 105.0

    # 3. 宽限期内持续返回 True
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=102.0) is True

    # 4. 超时返回 False
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=106.0) is False

