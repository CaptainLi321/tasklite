"""DeadlockGovernor 死锁治理深模块单元与状态机测试。"""

from tasklite import Job
from tasklite.engine.governor import DeadlockGovernor
from tasklite.engine.scheduler import DeadlockAttribution, StandstillFacts
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


def test_gap_rounds_reset_on_normal_wait_arbitration():
    """缺口升级计数只统计连续轮次：仲裁回到正常等待结论即终结 gap episode。

    缺口（分类盲区）轮次间队列若恢复为有限等待/退避（仲裁裁决 none），
    说明上一缺口已消解——计数跨 episode 累积会让全新成因的缺口第 1 轮
    即升级整队列 DLQ，恢复窗口承诺（连续 N 轮才升级）失真。
    """
    import types
    from unittest.mock import MagicMock

    gov = DeadlockGovernor(deadlock_gap_max_rounds=5)
    mock_store = MagicMock()
    sched_gap = StandstillFacts(min_wait=float("inf"))
    sched_normal = StandstillFacts(min_wait=2.0, waiting_for_dependency=False)

    # episode 1：4 轮缺口（未达阈值）
    for _ in range(4):
        assert gov.arbitrate(sched_gap, mock_store).action == "gap_retrying"
    assert gov.deadlock_gap_rounds == 4

    # 队列恢复为正常有限等待（仲裁裁决 none）→ 上一缺口 episode 终结
    assert gov.arbitrate(sched_normal, mock_store).action == "none"
    assert gov.deadlock_gap_rounds == 0

    # episode 2：全新成因的缺口第 1 轮必须从零计数，不得立即升级
    decision = gov.arbitrate(sched_gap, mock_store)
    assert decision.action == "gap_retrying"
    assert gov.deadlock_gap_rounds == 1


def test_gap_rounds_reset_on_dispatch_progress():
    """派发前进信号终结 gap episode：缺口轮次间队列有作业被派发即重新计数。

    缺口消解后仲裁可能不再被触发（队列恢复可运行、持续派发），计数若
    不随前进信号清零，长运行中两次不相关缺口会累计触发整队列误 DLQ。
    """
    gov = DeadlockGovernor(deadlock_gap_max_rounds=5)
    for _ in range(4):
        assert gov.check_gap_or_escalate("gap") is False
    assert gov.deadlock_gap_rounds == 4

    # 缺口消解：队列恢复派发前进
    gov.note_dispatch_progress()
    assert gov.deadlock_gap_rounds == 0

    # 全新缺口 episode 第 1 轮不升级
    assert gov.check_gap_or_escalate("gap") is False
    assert gov.deadlock_gap_rounds == 1


def test_gap_rounds_reset_on_grace_waiting_conclusion():
    """缺口轮次间的依赖宽限裁决（非缺口结论）同样终结 gap episode。"""
    import types
    from unittest.mock import MagicMock

    gov = DeadlockGovernor(dep_grace_seconds=60.0, deadlock_gap_max_rounds=5)
    state = PipelineState(
        wall={}, failed={}, cursors={},
        queue=[Job("t", "b", depends_on=["t::x"]).to_dict()],
    )
    mock_store = MagicMock()
    mock_store.state = state
    sched_gap = StandstillFacts(min_wait=float("inf"))
    sched_missing = StandstillFacts(
        min_wait=float("inf"),
        attribution=DeadlockAttribution(missing_dependency_uids=("t::b",)),
        has_potential_spawners=True,
    )

    for _ in range(4):
        assert gov.arbitrate(sched_gap, mock_store).action == "gap_retrying"

    # 缺口转为可归因的缺失依赖等待（宽限裁决，非缺口结论）→ 计数终结
    assert gov.resolve_deadlock(sched_missing, mock_store).action == "grace_waiting"
    assert gov.deadlock_gap_rounds == 0

    # 宽限到期后缺口复发 → 从零计数，不立即升级
    assert gov.arbitrate(sched_gap, mock_store).action == "gap_retrying"
    assert gov.deadlock_gap_rounds == 1


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
    sched_backoff = StandstillFacts(
        min_wait=2.0,
        waiting_for_dependency=False,
    )
    mock_store.state = state
    d_backoff = gov.arbitrate(sched_backoff, mock_store)
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
    sched_cycle = StandstillFacts(
        min_wait=1.5,
        waiting_for_dependency=True,
    )
    mock_store.state = cycle_state
    d_cycle = gov.arbitrate(sched_cycle, mock_store)
    assert d_cycle.action == "resolved"
    assert "t::a" in d_cycle.failed_uids
    assert mock_store.apply_bulk_failure.called



def test_deadlock_governor_grace_episode_ends_on_expiry():
    """宽限超时即终结 episode：过期 deadline 不泄漏进同缺失集的新等待者。

    同缺失依赖（同 uid 集合，如 retry 原样 requeue）的新等待者面对全新
    spawner 时必须获得全新宽限，不得继承已过期 deadline 被零宽限立即
    误判死锁 DLQ。
    """
    gov = DeadlockGovernor(dep_grace_seconds=5.0)
    state = PipelineState(
        wall={}, failed={}, cursors={},
        queue=[
            Job("t", "b", depends_on=["t::x"]).to_dict(),
            Job("t", "sp").to_dict(),
        ],
    )

    # episode 1：授予宽限并到期
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=100.0) is True
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=106.0) is False
    assert gov.dep_grace_deadline is None
    assert gov.dep_grace_missing is None

    # episode 2：同缺失集的新等待者 + 全新 spawner → 全新宽限（非零宽限误判）
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=107.0) is True
    assert gov.dep_grace_deadline == 112.0
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=110.0) is True


def test_deadlock_governor_grace_episode_ends_on_resolution():
    """宽限成功消解（等待者转为可运行、仲裁停摆）同样终结 episode。

    消解路径无 False 裁决出口，残留 deadline 以过期形态存活；同 uid 集合
    复发（如 every_run 同 uid 重入队等待新缺失依赖）且全新 spawner 在场时
    必须重新授予完整宽限，而非继承残留 deadline 被零宽限批量误判 DLQ。
    """
    gov = DeadlockGovernor(dep_grace_seconds=5.0)
    state = PipelineState(
        wall={}, failed={}, cursors={},
        queue=[Job("t", "b", depends_on=["t::x"]).to_dict()],
    )

    # episode：授予宽限并持续在宽限中，随后依赖产出、等待者消解（无裁决出口）
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=100.0) is True
    assert gov.dep_grace_deadline == 105.0
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=101.0) is True

    # 同 uid 集合远超宽限窗后复发 + 全新 spawner → 全新宽限（非零宽限误判）
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=500.0) is True
    assert gov.dep_grace_deadline == 505.0
    # 新 episode 内正常计时并按期超时终结
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=503.0) is True
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=506.0) is False
    assert gov.dep_grace_deadline is None


def test_deadlock_governor_grace_episode_ends_on_no_spawner():
    """无潜在 spawner 的立即裁决同样终结 episode，重启后重新授予完整宽限。"""
    gov = DeadlockGovernor(dep_grace_seconds=5.0)
    state = PipelineState(
        wall={}, failed={}, cursors={},
        queue=[Job("t", "b", depends_on=["t::x"]).to_dict()],
    )

    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=100.0) is True
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=False, now=101.0) is False
    assert gov.dep_grace_deadline is None
    assert gov.dep_grace_missing is None

    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=102.0) is True
    assert gov.dep_grace_deadline == 107.0


def test_deadlock_governor_grace_expiry_clears_deadline_fallback_path():
    """兼容回退路径（无 spawner 快道事实）的超时裁决同样终结 episode。"""
    gov = DeadlockGovernor(dep_grace_seconds=5.0)
    state = PipelineState(
        wall={}, failed={}, cursors={},
        queue=[
            Job("t", "b", depends_on=["t::x"]).to_dict(),
            Job("t", "sp").to_dict(),
        ],
    )

    assert gov.check_dependency_grace(state, ["t::b"], now=100.0) is True
    assert gov.check_dependency_grace(state, ["t::b"], now=106.0) is False
    assert gov.dep_grace_deadline is None
    # 同缺失集新 episode 重新授予完整宽限
    assert gov.check_dependency_grace(state, ["t::b"], now=107.0) is True
    assert gov.dep_grace_deadline == 112.0


def test_deadlock_governor_grace_episode_ends_on_dispatch_progress():
    """派发前进信号终结宽限 episode：盲区 (deadline, deadline+宽限窗] 内的
    同缺失集复发必须获得完整新宽限，不得继承残留 deadline 被零宽限批量误杀。

    消解路径（等待者获得依赖、转为可运行被派发）无任何 False 裁决出口，
    deadline 以过期形态残留；复发时刻距残留 deadline 不超过一个宽限窗时，
    时间启发式无法区分「刚过期的残留」与「活跃 episode」。任何成功派发
    都证明等待者已消解，episode 应当终结，复发自然是新 episode。
    """
    gov = DeadlockGovernor(dep_grace_seconds=60.0)
    state = PipelineState(
        wall={}, failed={}, cursors={},
        queue=[Job("t", "b", depends_on=["t::x"]).to_dict()],
    )

    # episode：授予宽限（deadline=160），随后等待者消解并被派发
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=100.0) is True
    assert gov.dep_grace_deadline == 160.0
    gov.note_dispatch_progress()
    assert gov.dep_grace_deadline is None
    assert gov.dep_grace_missing is None

    # 同缺失集在 (160, 220] 盲区内复发 + spawner 在场 → 完整新宽限而非误杀
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=170.0) is True
    assert gov.dep_grace_deadline == 230.0
    # 新 episode 内正常计时并按期超时终结（超时语义不变）
    assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=290.0) is False
    assert gov.dep_grace_deadline is None


def test_resolve_deadlock_relapse_after_progress_gets_full_grace():
    """死锁仲裁路径同样受益于前进终结点：盲区复发裁决为 grace_waiting 而非批量 DLQ。"""
    import time as time_mod
    import types
    from unittest.mock import MagicMock

    gov = DeadlockGovernor(dep_grace_seconds=60.0)
    state = PipelineState(
        wall={}, failed={}, cursors={},
        queue=[Job("t", "b", depends_on=["t::x"]).to_dict()],
    )
    mock_store = MagicMock()
    mock_store.state = state
    sched = StandstillFacts(
        min_wait=float("inf"),
        attribution=DeadlockAttribution(missing_dependency_uids=("t::b",)),
        has_potential_spawners=True,
    )

    # episode 授予宽限后消解（时钟锚定真实单调时钟，复发落在真实盲区内）
    t0 = time_mod.monotonic()
    assert gov.check_dependency_grace(state, {"t::b"}, has_potential_spawners=True, now=t0) is True
    assert gov.dep_grace_deadline == t0 + 60.0

    # 等待者消解被派发 → 前进信号终结 episode → 仲裁授予完整新宽限
    gov.note_dispatch_progress()
    decision = gov.resolve_deadlock(sched, mock_store)
    assert decision.action == "grace_waiting"
    assert decision.wait_time > 0
    assert not mock_store.apply_bulk_failure.called


def test_runtime_step_dispatch_progress_ends_grace_episode(tmp_path, monkeypatch):
    """step() 观测到派发前进即终结宽限 episode（runtime 接线契约）。"""
    import types

    from tests.helpers import make_runtime

    runtime = make_runtime(tmp_path, name="test_grace_progress")
    gov = runtime.governor
    gov.dep_grace_deadline = 160.0
    gov.dep_grace_missing = frozenset({"t::x"})

    from tasklite.models.state import PipelineState

    runtime.store.set_state(PipelineState(
        wall={}, failed={}, cursors={},
        queue=[Job("t", "b", depends_on=["t::x"]).to_dict()],
    ))
    calls = {"n": 0}

    from tasklite.engine.dispatch import DispatchOutcome
    from tasklite.engine.scheduler import StandstillFacts

    def fake_dispatch_next():
        calls["n"] += 1
        if calls["n"] == 1:
            return DispatchOutcome(
                entry=object(), has_runnable=True, worker_wait=0.0,
                standstill=StandstillFacts(min_wait=0.0), should_continue=True,
            )
        return DispatchOutcome(
            entry=None, has_runnable=False, worker_wait=0.0,
            standstill=StandstillFacts(min_wait=float("inf")), should_continue=False,
        )

    monkeypatch.setattr(runtime._dispatch, "dispatch_next", fake_dispatch_next)

    outcome = runtime.step()
    assert outcome.dispatched_count == 1
    assert gov.dep_grace_deadline is None
    assert gov.dep_grace_missing is None
