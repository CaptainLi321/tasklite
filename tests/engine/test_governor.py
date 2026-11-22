"""v2 死锁治理器测试：归因、缺口升级、排除停摆剥离与深链仲裁（全量对接真实 StateStore）。"""

from __future__ import annotations

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.governor import (
    DEADLOCK_GAP_MAX_ROUNDS,
    DeadlockDecision,
    DeadlockGovernor,
    partition_standstill,
)
from tasklite.engine.resource import IntervalResource
from tasklite.engine.scheduler import DeadlockAttribution, JobScheduler, StandstillFacts
from tasklite.engine.session import RunSession
from tasklite.engine.store import StateStore
from tasklite.models.job import Job
from tasklite.models.state import PipelineState, job_uid_from_snapshot
from tasklite.selection import Exclude

INF = float("inf")
# 超出解释器默认递归限制（1000），保证场景真实落在递归深度敏感区
CHAIN_N = 1500


def _make_store(
    queue,
    *,
    successes=None,
    failed=None,
    exclude: Exclude | None = None,
) -> StateStore:
    """构造对接真实 InMemoryStateBackend 的 StateStore（磁盘与内存队列保持一致）。"""
    backend = InMemoryStateBackend()
    rows = list(queue)
    if rows:
        backend.enqueue_jobs(rows)
    session = RunSession()
    if exclude is not None:
        session.clear_exclusions(exclude)
    state = PipelineState(
        successes=successes or {},
        failed=failed or {},
        cursors={},
        queue=rows,
    )
    return StateStore(backend, state=state, session=session)


def _chain_queue(n, tail_deps=None):
    """n 节点线性链 j0→j1→…→j_{n-1}，链尾可追加额外依赖。"""
    queue = []
    for i in range(n):
        deps = [f"t::j{i+1}"] if i + 1 < n else list(tail_deps or ())
        queue.append(Job("t", f"j{i}", depends_on=deps).to_dict())
    return queue


def _spy_cycle_detection(monkeypatch):
    """包装环检测统计调用次数——防测试空转：断言仲裁确实执行了检测。"""
    calls = {"count": 0}
    original = PipelineState.find_dependency_cycles

    def spy(self, **kwargs):
        calls["count"] += 1
        return original(self, **kwargs)

    monkeypatch.setattr(PipelineState, "find_dependency_cycles", spy)
    return calls


class TestGovernorBasics:
    def test_reset_clears_all_round_state(self):
        gov = DeadlockGovernor(deadlock_gap_max_rounds=3)
        gov.deadlock_gap_rounds = 2
        gov.reset()
        assert gov.deadlock_gap_rounds == 0

    def test_default_constants(self):
        assert DEADLOCK_GAP_MAX_ROUNDS == 5

    def test_decision_value_structure(self):
        d_resolved = DeadlockDecision(
            action="resolved", should_terminate=True, wait_time=0.0, failed_uids=["t::a"]
        )
        assert d_resolved.should_terminate is True
        assert d_resolved.failed_uids == ["t::a"]


class TestGapEscalation:
    def test_gap_escalation_counts_consecutive_rounds(self):
        gov = DeadlockGovernor(deadlock_gap_max_rounds=3)
        assert not gov.check_gap_or_escalate("Gap prefix")
        assert gov.deadlock_gap_rounds == 1
        assert not gov.check_gap_or_escalate("Gap prefix")
        assert gov.deadlock_gap_rounds == 2
        assert gov.check_gap_or_escalate("Gap prefix")
        assert gov.deadlock_gap_rounds == 3

    def test_gap_rounds_reset_on_normal_wait_arbitration(self):
        """缺口升级计数只统计连续轮次：仲裁回到正常等待结论即终结 gap episode。"""
        gov = DeadlockGovernor(deadlock_gap_max_rounds=5)
        store = _make_store([])
        sched_gap = StandstillFacts(min_wait=INF)
        sched_normal = StandstillFacts(min_wait=2.0, waiting_for_dependency=False)

        for _ in range(4):
            assert gov.arbitrate(sched_gap, store).action == "gap_retrying"
        assert gov.deadlock_gap_rounds == 4

        # 队列恢复为正常有限等待（仲裁裁决 none）→ 上一缺口 episode 终结
        assert gov.arbitrate(sched_normal, store).action == "none"
        assert gov.deadlock_gap_rounds == 0

        # 全新成因的缺口第 1 轮必须从零计数，不得立即升级
        decision = gov.arbitrate(sched_gap, store)
        assert decision.action == "gap_retrying"
        assert gov.deadlock_gap_rounds == 1

    def test_gap_rounds_reset_on_dispatch_progress(self):
        """派发前进信号终结 gap episode：缺口轮次间队列有作业被派发即重新计数。"""
        gov = DeadlockGovernor(deadlock_gap_max_rounds=5)
        for _ in range(4):
            assert gov.check_gap_or_escalate("gap") is False
        assert gov.deadlock_gap_rounds == 4

        gov.record_dispatch_progress()
        assert gov.deadlock_gap_rounds == 0

        assert gov.check_gap_or_escalate("gap") is False
        assert gov.deadlock_gap_rounds == 1


class TestArbitrateFacade:
    def test_none_facts_returns_none(self):
        gov = DeadlockGovernor()
        store = _make_store([])
        decision = gov.arbitrate(None, store)
        assert decision.action == "none"
        assert decision.should_terminate is False
        assert decision.wait_time == 0.0

    def test_finite_wait_without_cycle_returns_none(self):
        gov = DeadlockGovernor()
        store = _make_store([Job("t", "a", depends_on=["t::b"]).to_dict()])
        sched = StandstillFacts(min_wait=2.0, waiting_for_dependency=False)
        decision = gov.arbitrate(sched, store)
        assert decision.action == "none"
        assert decision.should_terminate is False
        assert store.backend.load_failed() == {}

    def test_finite_wait_with_cycle_resolves(self):
        """有限等待但拓扑成环（掩盖死锁）→ 触发 resolve_deadlock 熔断环成员。"""
        gov = DeadlockGovernor()
        store = _make_store(
            [
                Job("t", "a", depends_on=["t::b"]).to_dict(),
                Job("t", "b", depends_on=["t::a"]).to_dict(),
            ]
        )
        sched = StandstillFacts(min_wait=1.5, waiting_for_dependency=True)
        decision = gov.arbitrate(sched, store)
        assert decision.action == "resolved"
        assert set(decision.failed_uids) == {"t::a", "t::b"}
        assert set(store.backend.load_failed()) == {"t::a", "t::b"}

    def test_infinite_wait_without_attribution_enters_gap_path(self):
        gov = DeadlockGovernor(deadlock_gap_max_rounds=2)
        store = _make_store([])
        sched = StandstillFacts(min_wait=INF)
        assert gov.arbitrate(sched, store).action == "gap_retrying"
        decision = gov.arbitrate(sched, store)
        assert decision.action == "resolved"
        assert decision.should_terminate is False


class TestResolveDeadlockAttribution:
    """三类归因：malformed / unknown / impossible + 环与缺口升级。"""

    def test_malformed_attribution(self):
        gov = DeadlockGovernor()
        store = _make_store([{"task_type": "t"}])
        malformed_uid = job_uid_from_snapshot({"task_type": "t"})
        facts = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(malformed_uids=(malformed_uid,)),
        )
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert store.state.failed[malformed_uid]["error_message"] == "Queued job configuration is invalid"
        assert store.state.failed[malformed_uid]["root_cause"] is True
        assert malformed_uid in store.backend.load_failed()

    def test_unknown_resource_attribution(self):
        gov = DeadlockGovernor()
        store = _make_store([Job("t", "x", resources={"ghost": 1.0}).to_dict()])
        facts = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(unknown_resource_uids=("t::x",)),
        )
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert store.state.failed["t::x"]["error_message"] == "Resource requirements cannot be satisfied"
        assert "t::x" in store.backend.load_failed()

    def test_impossible_resource_attribution(self):
        gov = DeadlockGovernor()
        store = _make_store([Job("t", "x", resources={"gpu": 99.0}).to_dict()])
        facts = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(impossible_resource_uids=("t::x",)),
        )
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert store.state.failed["t::x"]["error_message"] == "Resource requirements cannot be satisfied"
        assert "t::x" in store.backend.load_failed()

    def test_cycle_members_failed_outside_kept(self):
        gov = DeadlockGovernor()
        store = _make_store(
            [
                Job("t", "a", depends_on=["t::b"]).to_dict(),
                Job("t", "b", depends_on=["t::a"]).to_dict(),
                Job("t", "c").to_dict(),
            ]
        )
        facts = StandstillFacts(min_wait=INF, waiting_for_dependency=True)
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert set(store.state.failed) == {"t::a", "t::b"}
        assert store.state.failed["t::a"]["error_message"] == "Dependency cycle prevents execution"
        assert [job_uid_from_snapshot(jd) for jd in store.state.queue] == ["t::c"]
        assert [row["job_uid"] for row in store.backend.load_queue_jobs(status="pending")] == ["t::c"]

    def test_unclassifiable_gap_escalates_whole_queue(self):
        """缺口升级到阈值 → 全队列进失败档案且队列清空 → should_terminate。"""
        gov = DeadlockGovernor(deadlock_gap_max_rounds=2)
        store = _make_store([Job("t", "a").to_dict(), Job("t", "b").to_dict()])
        facts = StandstillFacts(min_wait=INF)
        assert gov.resolve_deadlock(facts, store=store).action == "gap_retrying"
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert decision.should_terminate is True
        assert set(store.state.failed) == {"t::a", "t::b"}
        assert store.state.failed["t::a"]["error_message"] == "Queue cannot make progress for an undetermined reason"
        assert store.state.queue == []
        assert store.backend.load_queue_jobs() == []

    def test_partial_failure_keeps_queue_not_terminating(self):
        """部分熔断（队列仍有剩余）→ should_terminate=False。"""
        gov = DeadlockGovernor()
        store = _make_store(
            [
                Job("t", "a", resources={"gpu": 99.0}).to_dict(),
                Job("t", "c").to_dict(),
            ]
        )
        facts = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(impossible_resource_uids=("t::a",)),
        )
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert decision.should_terminate is False
        assert set(store.state.failed) == {"t::a"}
        assert [job_uid_from_snapshot(jd) for jd in store.state.queue] == ["t::c"]


class TestGovernorArbitrateDeepChain:
    def test_finite_wait_deep_acyclic_chain_returns_none(self, monkeypatch):
        """有限等待 + 无环深链：仲裁正常完成返回 none，环检测不得崩溃。"""
        calls = _spy_cycle_detection(monkeypatch)
        gov = DeadlockGovernor()
        store = _make_store(_chain_queue(CHAIN_N))
        sched = StandstillFacts(min_wait=1.0, waiting_for_dependency=True)

        decision = gov.arbitrate(sched, store=store)

        assert calls["count"] >= 1, "仲裁未执行环检测，测试空转"
        assert decision.action == "none"
        assert decision.should_terminate is False

    def test_finite_wait_deep_chain_with_cycle_resolves_only_cycle_members(self, monkeypatch):
        """有限等待 + 深链伴随真环：熔断且仅环成员进失败档案，链保留。"""
        calls = _spy_cycle_detection(monkeypatch)
        queue = _chain_queue(CHAIN_N)
        queue += [
            Job("t", "c0", depends_on=["t::c1"]).to_dict(),
            Job("t", "c1", depends_on=["t::c0"]).to_dict(),
        ]
        store = _make_store(queue)
        gov = DeadlockGovernor()
        sched = StandstillFacts(min_wait=1.0, waiting_for_dependency=True)

        decision = gov.arbitrate(sched, store=store)

        assert calls["count"] >= 1, "仲裁未执行环检测，测试空转"
        assert decision.action == "resolved"
        assert set(decision.failed_uids) == {"t::c0", "t::c1"}
        assert set(store.state.queue_uids) == {f"t::j{i}" for i in range(CHAIN_N)}
        # 持久层同步：仅环成员落失败档案，磁盘队列保留全链
        assert set(store.backend.load_failed()) == {"t::c0", "t::c1"}
        assert len(store.backend.load_queue_jobs(status="pending")) == CHAIN_N


class TestSelectionClosedLoopArbitration:
    """DeadlockGovernor.arbitrate 基于 RunSession 闭环处理执行选择停摆与零归档测试。"""

    def test_arbitrate_returns_selection_blocked_when_only_exclusion_deferred(self):
        gov = DeadlockGovernor()
        store = _make_store(
            [
                Job("ex_type", "1").to_dict(),
                Job("normal", "2", depends_on=["ex_type::1"]).to_dict(),
            ],
            exclude=Exclude(task_type="ex_type"),
        )
        sched = StandstillFacts(min_wait=INF, attribution=DeadlockAttribution())

        decision = gov.arbitrate(sched, store=store)
        assert decision.action == "selection_blocked"
        assert decision.should_terminate is True
        assert decision.standstill is not None
        assert decision.standstill.blocked_uids == ("normal::2",)

    def test_excluded_self_loop_and_cross_exclude_cycle_leave_allowed_work_runnable(self):
        exclude = Exclude(task_type="excluded")
        store = _make_store(
            [
                Job("excluded", "e", depends_on=["excluded::e", "allowed::a"]).to_dict(),
                Job("allowed", "a", depends_on=["excluded::e"]).to_dict(),
                Job("allowed", "downstream", depends_on=["excluded::e"]).to_dict(),
                Job("resource", "waiter", resources={"api": 1}).to_dict(),
            ],
            exclude=exclude,
        )
        resource = IntervalResource("api", interval_seconds=30.0)
        resource.acquire(1.0)
        scheduler = JobScheduler({"api": resource}, session=store.session)
        facts = scheduler.scan_next_runnable(store.state, frozenset()).standstill_facts()
        assert 0 < facts.min_wait <= 30
        assert facts.waiting_for_dependency
        assert not facts.attribution.has_deadlock_causes

        standstill = partition_standstill(store.state, store.session)
        assert standstill is not None
        assert standstill.cycle_uids == ()
        assert set(standstill.blocked_uids) == {"allowed::a", "allowed::downstream"}
        assert standstill.unblocked_uids == ("resource::waiter",)

        decision = DeadlockGovernor().arbitrate(facts, store=store)
        assert decision.action == "none"
        assert not decision.should_terminate
        assert len(store.state.queue) == 4
        assert not store.state.failed

    def test_missing_dependency_settles_before_remaining_allowed_cycle(self):
        exclude = Exclude(task_type="other")
        store = _make_store(
            [
                Job("t", "a", depends_on=["t::b"]).to_dict(),
                Job("t", "b", depends_on=["t::a"]).to_dict(),
                Job("t", "gap", depends_on=["t::missing"]).to_dict(),
                Job("t", "downstream", depends_on=["t::gap"]).to_dict(),
            ],
            exclude=exclude,
        )
        scheduler = JobScheduler({}, session=store.session)
        governor = DeadlockGovernor()
        standstill = partition_standstill(store.state, store.session)
        assert standstill is not None
        assert set(standstill.cycle_uids) == {"t::a", "t::b"}

        candidate = scheduler.scan_next_runnable(store.state, frozenset())
        assert candidate.kind == "dep_missing"
        assert candidate.pending_dep_missing == "t::missing"
        assert candidate.runnable_idx is not None
        gap = store.state.pop_job(candidate.runnable_idx)
        store.apply_failure(
            "t::gap",
            {
                "error_message": "Dependency cycle prevents execution",
                "missing_dependency": "t::missing",
            },
            job_snapshot=gap,
        )
        assert set(store.state.failed) == {"t::gap", "t::downstream"}
        assert {job_uid_from_snapshot(row) for row in store.state.queue} == {
            "t::a",
            "t::b",
        }

        facts = scheduler.scan_next_runnable(store.state, frozenset()).standstill_facts()
        assert not facts.attribution.has_deadlock_causes
        decision = governor.arbitrate(facts, store=store)
        assert decision.action == "resolved"
        assert set(decision.failed_uids) == {"t::a", "t::b"}
        assert store.state.is_empty
