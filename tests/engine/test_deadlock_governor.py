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


def test_deadlock_governor_grace_period():
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


def test_deadlock_governor_spawner_fast_path():
    """验证提供 has_potential_spawners 时直接依据单趟事实裁决，零扫描 state.queue。"""
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


def test_deadlock_decision_structure():
    """DeadlockDecision 纯值对象属性与布尔兼容性验证。"""
    from tasklite.engine.governor import DeadlockDecision

    d1 = DeadlockDecision(action="grace_waiting", should_terminate=False, wait_time=0.5)
    assert not d1
    assert d1.action == "grace_waiting"
    assert d1.wait_time == 0.5
    assert d1.failed_uids == []

    d2 = DeadlockDecision(
        action="resolved", should_terminate=True, wait_time=0.0, failed_uids=["t::a"]
    )
    assert d2
    assert d2.action == "resolved"
    assert d2.should_terminate is True
    assert d2.failed_uids == ["t::a"]


def test_deadlock_governor_arbitrate(tmp_path):
    """验证 arbitrate 门面对于 None、正常等待、无限等待死锁、有限等待拓扑成环的统一裁决。"""
    import types
    from unittest.mock import MagicMock
    from tasklite.engine.governor import DeadlockGovernor, DeadlockDecision

    gov = DeadlockGovernor(dep_grace_seconds=5.0)
    mock_store = MagicMock()

    # 1. sched 为 None -> action="none"
    d_none = gov.arbitrate(None, mock_store)
    assert d_none.action == "none"
    assert d_none.should_terminate is False
    assert d_none.wait_time == 0.0

    # 2. 正常有限等待（非成环）-> action="none"
    state = PipelineState(
        wall={}, failed={}, cursors={},
        queue=[Job("t", "a", depends_on=["t::b"]).to_dict()],
    )
    sched_backoff = types.SimpleNamespace(
        min_wait=2.0,
        waiting_for_dependency=False,
    )
    d_backoff = gov.arbitrate(sched_backoff, mock_store, state=state)
    assert d_backoff.action == "none"
    assert d_backoff.should_terminate is False

    # 3. 有限等待但拓扑成环（掩盖死锁）-> 触发 resolve_deadlock
    mock_store.apply_bulk_failure.return_value = types.SimpleNamespace(
        failed_uids=["t::a", "t::b"], cascaded_uids=[]
    )
    cycle_state = PipelineState(
        wall={}, failed={}, cursors={},
        queue=[
            Job("t", "a", depends_on=["t::b"]).to_dict(),
            Job("t", "b", depends_on=["t::a"]).to_dict(),
        ],
    )
    sched_cycle = types.SimpleNamespace(
        min_wait=1.5,
        waiting_for_dependency=True,
    )
    d_cycle = gov.arbitrate(sched_cycle, mock_store, state=cycle_state)
    assert d_cycle.action == "resolved"
    assert "t::a" in d_cycle.failed_uids
    assert mock_store.apply_bulk_failure.called

