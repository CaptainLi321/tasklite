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
