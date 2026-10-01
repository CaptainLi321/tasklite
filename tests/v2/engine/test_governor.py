"""v2 死锁治理器测试：四分类归因、宽限 episode 状态机、缺口升级与深链仲裁。"""

from __future__ import annotations

from types import SimpleNamespace

from tasklite.v2.backend.memory import InMemoryStateBackend
from tasklite.v2.engine.errorclass import (
    ERR_DEADLOCK_GAP,
    ERR_DEPENDENCY_DEADLOCK,
    ERR_MALFORMED_JOB,
    ERR_RESOURCE_DEADLOCK,
)
from tasklite.v2.engine.governor import (
    DEADLOCK_GAP_MAX_ROUNDS,
    DEP_GRACE_SECONDS,
    DeadlockDecision,
    DeadlockGovernor,
)
from tasklite.v2.engine.scheduler import DeadlockAttribution, StandstillFacts
from tasklite.v2.models.job import Job
from tasklite.v2.models.state import PipelineState, uid_from_job_dict

INF = float("inf")
# 超出解释器默认递归限制（1000），保证场景真实落在递归深度敏感区
CHAIN_N = 1500


class FakeArbitrationStore:
    """仲裁 store 契约的最小真实实现：内存状态与内存后端双推进。

    批量失败语义对齐未来 StateStore：失败档案按 uid 落库（先持久层、
    后内存）、队列按 uid 精准删除（剩余条目保序）、批量路径不做级联
    （级联收敛在单点失败出口）。
    """

    def __init__(self, state: PipelineState, backend: InMemoryStateBackend | None = None):
        self.state = state
        self.backend = backend
        self.bulk_calls: list[list[tuple[str, dict]]] = []

    def apply_bulk_failure(self, uids_metas):
        self.bulk_calls.append(list(uids_metas))
        if self.backend is not None:
            assert self.backend.commit_bulk_failure(list(uids_metas)) is True
        for uid, meta in uids_metas:
            self.state.mark_failed(uid, dict(meta))
        fail_set = {uid for uid, _ in uids_metas}
        remaining = [jd for jd in self.state.queue if uid_from_job_dict(jd) not in fail_set]
        self.state.replace_queue(remaining)
        return SimpleNamespace(failed_uids=[uid for uid, _ in uids_metas], cascaded_uids=[])


def _state(queue, wall=None, failed=None):
    return PipelineState(
        wall=wall or {}, failed=failed or {}, cursors={}, queue=list(queue)
    )


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

    def spy(self):
        calls["count"] += 1
        return original(self)

    monkeypatch.setattr(PipelineState, "find_dependency_cycles", spy)
    return calls


class TestGovernorBasics:
    def test_reset_clears_all_round_state(self):
        gov = DeadlockGovernor(dep_grace_seconds=10.0, deadlock_gap_max_rounds=3)
        gov.dep_grace_deadline = 100.0
        gov.dep_grace_missing = frozenset(["t::j1"])
        gov.deadlock_gap_rounds = 2
        gov.reset()
        assert gov.dep_grace_deadline is None
        assert gov.dep_grace_missing is None
        assert gov.deadlock_gap_rounds == 0

    def test_default_constants(self):
        assert DEP_GRACE_SECONDS == 60.0
        assert DEADLOCK_GAP_MAX_ROUNDS == 5

    def test_decision_value_structure(self):
        d_grace = DeadlockDecision(action="grace_waiting", should_terminate=False, wait_time=0.5)
        assert d_grace.action == "grace_waiting"
        assert d_grace.should_terminate is False
        assert d_grace.wait_time == 0.5
        assert d_grace.failed_uids == []

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
        store = FakeArbitrationStore(_state([]))
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

    def test_gap_rounds_reset_on_grace_waiting_conclusion(self):
        """缺口轮次间的依赖宽限裁决（非缺口结论）同样终结 gap episode。"""
        gov = DeadlockGovernor(dep_grace_seconds=60.0, deadlock_gap_max_rounds=5)
        state = _state([Job("t", "b", depends_on=["t::x"]).to_dict()])
        store = FakeArbitrationStore(state)
        sched_gap = StandstillFacts(min_wait=INF)
        sched_missing = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(missing_dependency_uids=("t::b",)),
            has_potential_spawners=True,
        )

        for _ in range(4):
            assert gov.arbitrate(sched_gap, store).action == "gap_retrying"

        # 缺口转为可归因的缺失依赖等待（宽限裁决，非缺口结论）→ 计数终结
        assert gov.resolve_deadlock(sched_missing, store=store).action == "grace_waiting"
        assert gov.deadlock_gap_rounds == 0

        # 宽限到期后缺口复发 → 从零计数，不立即升级
        assert gov.arbitrate(sched_gap, store).action == "gap_retrying"
        assert gov.deadlock_gap_rounds == 1


class TestDependencyGrace:
    def test_grace_period_fallback_path(self):
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        state = _state(
            [
                Job("t", "a", depends_on=["t::b"]).to_dict(),
                Job("t", "c").to_dict(),
            ]
        )
        # 首次裁决：授予宽限
        assert gov.check_dependency_grace(state, ["t::b"], now=100.0) is True
        assert gov.dep_grace_deadline == 105.0
        assert gov.dep_grace_missing == frozenset(["t::b"])

        # 到期前：仍在宽限
        assert gov.check_dependency_grace(state, ["t::b"], now=102.0) is True

        # 到期后：宽限过期
        assert gov.check_dependency_grace(state, ["t::b"], now=106.0) is False

    def test_spawner_fast_path(self):
        """提供 has_potential_spawners 时直接依据单趟事实裁决，零扫描 state.queue。"""
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        state = _state([])

        assert gov.check_dependency_grace(
            state, ["t::b"], has_potential_spawners=False, now=100.0
        ) is False
        assert gov.dep_grace_deadline is None

        assert gov.check_dependency_grace(
            state, ["t::b"], has_potential_spawners=True, now=100.0
        ) is True
        assert gov.dep_grace_deadline == 105.0

        assert gov.check_dependency_grace(
            state, ["t::b"], has_potential_spawners=True, now=102.0
        ) is True

        assert gov.check_dependency_grace(
            state, ["t::b"], has_potential_spawners=True, now=106.0
        ) is False

    def test_grace_episode_ends_on_expiry(self):
        """宽限超时即终结 episode：过期 deadline 不泄漏进同缺失集的新等待者。"""
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        state = _state(
            [
                Job("t", "b", depends_on=["t::x"]).to_dict(),
                Job("t", "sp").to_dict(),
            ]
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

    def test_grace_episode_ends_on_resolution(self):
        """宽限成功消解（等待者转为可运行、仲裁停摆）同样终结 episode。

        消解路径无 False 裁决出口，残留 deadline 以过期形态存活；同 uid 集合
        复发（如 every_run 同 uid 重入队等待新缺失依赖）且全新 spawner 在场时
        必须重新授予完整宽限，而非继承残留 deadline 被零宽限批量误判。
        """
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        state = _state([Job("t", "b", depends_on=["t::x"]).to_dict()])

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

    def test_grace_episode_ends_on_no_spawner(self):
        """无潜在 spawner 的立即裁决同样终结 episode，重启后重新授予完整宽限。"""
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        state = _state([Job("t", "b", depends_on=["t::x"]).to_dict()])

        assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=100.0) is True
        assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=False, now=101.0) is False
        assert gov.dep_grace_deadline is None
        assert gov.dep_grace_missing is None

        assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=102.0) is True
        assert gov.dep_grace_deadline == 107.0

    def test_grace_expiry_clears_deadline_fallback_path(self):
        """回退路径（无 spawner 快道事实）的超时裁决同样终结 episode。"""
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        state = _state(
            [
                Job("t", "b", depends_on=["t::x"]).to_dict(),
                Job("t", "sp").to_dict(),
            ]
        )

        assert gov.check_dependency_grace(state, ["t::b"], now=100.0) is True
        assert gov.check_dependency_grace(state, ["t::b"], now=106.0) is False
        assert gov.dep_grace_deadline is None
        # 同缺失集新 episode 重新授予完整宽限
        assert gov.check_dependency_grace(state, ["t::b"], now=107.0) is True
        assert gov.dep_grace_deadline == 112.0

    def test_grace_episode_ends_on_dispatch_progress(self):
        """派发前进信号终结宽限 episode：盲区 (deadline, deadline+宽限窗] 内的
        同缺失集复发必须获得完整新宽限，不得继承残留 deadline 被零宽限批量误杀。

        消解路径（等待者获得依赖、转为可运行被派发）无任何 False 裁决出口，
        deadline 以过期形态残留；复发时刻距残留 deadline 不超过一个宽限窗时，
        时间启发式无法区分「刚过期的残留」与「活跃 episode」。任何成功派发
        都证明等待者已消解，episode 应当终结，复发自然是新 episode。
        """
        gov = DeadlockGovernor(dep_grace_seconds=60.0)
        state = _state([Job("t", "b", depends_on=["t::x"]).to_dict()])

        # episode：授予宽限（deadline=160），随后等待者消解并被派发
        assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=100.0) is True
        assert gov.dep_grace_deadline == 160.0
        gov.record_dispatch_progress()
        assert gov.dep_grace_deadline is None
        assert gov.dep_grace_missing is None

        # 同缺失集在 (160, 220] 盲区内复发 + spawner 在场 → 完整新宽限而非误杀
        assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=170.0) is True
        assert gov.dep_grace_deadline == 230.0
        # 新 episode 内正常计时并按期超时终结（超时语义不变）
        assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=290.0) is False
        assert gov.dep_grace_deadline is None

    def test_missing_set_change_starts_new_episode(self):
        """缺失集变化即新 episode：deadline 重置、不继承旧窗口。"""
        gov = DeadlockGovernor(dep_grace_seconds=10.0)
        state = _state([Job("t", "b", depends_on=["t::x"]).to_dict()])

        assert gov.check_dependency_grace(state, ["t::b"], has_potential_spawners=True, now=100.0) is True
        assert gov.dep_grace_deadline == 110.0

        # 缺失集变化 → deadline 清零重授
        assert gov.check_dependency_grace(
            state, ["t::b", "t::c"], has_potential_spawners=True, now=103.0
        ) is True
        assert gov.dep_grace_deadline == 113.0

    def test_no_missing_uids_returns_false(self):
        gov = DeadlockGovernor()
        state = _state([Job("t", "a").to_dict()])
        assert gov.check_dependency_grace(state, [], now=100.0) is False
        # 越界整型下标被忽略（不产 missing）
        assert gov.check_dependency_grace(state, [7], now=100.0) is False


class TestArbitrateFacade:
    def test_none_facts_returns_none(self):
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        store = FakeArbitrationStore(_state([]))
        decision = gov.arbitrate(None, store)
        assert decision.action == "none"
        assert decision.should_terminate is False
        assert decision.wait_time == 0.0

    def test_finite_wait_without_cycle_returns_none(self):
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        state = _state([Job("t", "a", depends_on=["t::b"]).to_dict()])
        store = FakeArbitrationStore(state)
        sched = StandstillFacts(min_wait=2.0, waiting_for_dependency=False)
        decision = gov.arbitrate(sched, store)
        assert decision.action == "none"
        assert decision.should_terminate is False
        assert store.bulk_calls == []

    def test_finite_wait_with_cycle_resolves(self):
        """有限等待但拓扑成环（掩盖死锁）→ 触发 resolve_deadlock 熔断环成员。"""
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        state = _state(
            [
                Job("t", "a", depends_on=["t::b"]).to_dict(),
                Job("t", "b", depends_on=["t::a"]).to_dict(),
            ]
        )
        store = FakeArbitrationStore(state)
        sched = StandstillFacts(min_wait=1.5, waiting_for_dependency=True)
        decision = gov.arbitrate(sched, store)
        assert decision.action == "resolved"
        assert set(decision.failed_uids) == {"t::a", "t::b"}
        assert store.bulk_calls, "成环熔断必须提交批量失败"

    def test_infinite_wait_without_attribution_enters_gap_path(self):
        gov = DeadlockGovernor(deadlock_gap_max_rounds=2)
        store = FakeArbitrationStore(_state([]))
        sched = StandstillFacts(min_wait=INF)
        assert gov.arbitrate(sched, store).action == "gap_retrying"
        decision = gov.arbitrate(sched, store)
        assert decision.action == "resolved"
        assert decision.should_terminate is False


class TestResolveDeadlockAttribution:
    """四分类归因：malformed / unknown / missing / impossible + 环与缺口升级。"""

    def test_malformed_attribution(self):
        gov = DeadlockGovernor()
        state = _state([{"task_type": "t"}])
        store = FakeArbitrationStore(state)
        facts = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(malformed_uids=(uid_from_job_dict({"task_type": "t"}),)),
        )
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        malformed_uid = uid_from_job_dict({"task_type": "t"})
        assert state.failed[malformed_uid]["error"] == ERR_MALFORMED_JOB
        assert state.failed[malformed_uid]["root_cause"] is True

    def test_unknown_resource_attribution(self):
        gov = DeadlockGovernor()
        state = _state([Job("t", "x", resources={"ghost": 1.0}).to_dict()])
        store = FakeArbitrationStore(state)
        facts = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(unknown_resource_uids=("t::x",)),
        )
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert state.failed["t::x"]["error"] == ERR_RESOURCE_DEADLOCK

    def test_impossible_resource_attribution(self):
        gov = DeadlockGovernor()
        state = _state([Job("t", "x", resources={"gpu": 99.0}).to_dict()])
        store = FakeArbitrationStore(state)
        facts = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(impossible_resource_uids=("t::x",)),
        )
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert state.failed["t::x"]["error"] == ERR_RESOURCE_DEADLOCK

    def test_missing_dependency_without_spawner_fails_after_grace(self):
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        state = _state([Job("t", "a", depends_on=["t::ghost"]).to_dict()])
        store = FakeArbitrationStore(state)
        facts = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(missing_dependency_uids=("t::a",)),
            has_potential_spawners=False,
        )
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert state.failed["t::a"]["error"] == ERR_DEPENDENCY_DEADLOCK

    def test_missing_dependency_with_spawner_graces(self):
        gov = DeadlockGovernor(dep_grace_seconds=5.0)
        state = _state([Job("t", "a", depends_on=["t::ghost"]).to_dict()])
        store = FakeArbitrationStore(state)
        facts = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(missing_dependency_uids=("t::a",)),
            has_potential_spawners=True,
        )
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "grace_waiting"
        assert decision.wait_time > 0
        assert store.bulk_calls == []
        assert state.failed == {}

    def test_cycle_members_failed_outside_kept(self):
        gov = DeadlockGovernor()
        state = _state(
            [
                Job("t", "a", depends_on=["t::b"]).to_dict(),
                Job("t", "b", depends_on=["t::a"]).to_dict(),
                Job("t", "c").to_dict(),
            ]
        )
        store = FakeArbitrationStore(state)
        facts = StandstillFacts(min_wait=INF, waiting_for_dependency=True)
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert set(state.failed) == {"t::a", "t::b"}
        assert state.failed["t::a"]["error"] == ERR_DEPENDENCY_DEADLOCK
        assert [uid_from_job_dict(jd) for jd in state.queue] == ["t::c"]

    def test_unclassifiable_gap_escalates_whole_queue(self):
        """缺口升级到阈值 → 全队列进失败档案且队列清空 → should_terminate。"""
        gov = DeadlockGovernor(deadlock_gap_max_rounds=2)
        state = _state([Job("t", "a").to_dict(), Job("t", "b").to_dict()])
        store = FakeArbitrationStore(state)
        facts = StandstillFacts(min_wait=INF)
        assert gov.resolve_deadlock(facts, store=store).action == "gap_retrying"
        decision = gov.resolve_deadlock(facts, store=store)
        assert decision.action == "resolved"
        assert decision.should_terminate is True
        assert set(state.failed) == {"t::a", "t::b"}
        assert state.failed["t::a"]["error"] == ERR_DEADLOCK_GAP
        assert state.queue == []

    def test_partial_failure_keeps_queue_not_terminating(self):
        """部分熔断（队列仍有剩余）→ should_terminate=False。"""
        gov = DeadlockGovernor()
        state = _state(
            [
                Job("t", "a", depends_on=["t::ghost"]).to_dict(),
                Job("t", "c").to_dict(),
            ]
        )
        store = FakeArbitrationStore(state)
        facts = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(missing_dependency_uids=("t::a",)),
            has_potential_spawners=True,
        )
        # 先耗尽宽限（无 spawner 事实切换），再仲裁
        assert gov.resolve_deadlock(facts, store=store).action == "grace_waiting"
        facts_no_spawner = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(missing_dependency_uids=("t::a",)),
            has_potential_spawners=False,
        )
        decision = gov.resolve_deadlock(facts_no_spawner, store=store)
        assert decision.action == "resolved"
        assert decision.should_terminate is False
        assert set(state.failed) == {"t::a"}
        assert [uid_from_job_dict(jd) for jd in state.queue] == ["t::c"]


class TestGovernorArbitrateDeepChain:
    def test_finite_wait_deep_acyclic_chain_returns_none(self, monkeypatch):
        """有限等待 + 无环深链：仲裁正常完成返回 none，环检测不得崩溃。"""
        calls = _spy_cycle_detection(monkeypatch)
        gov = DeadlockGovernor()
        state = _state(_chain_queue(CHAIN_N))
        store = FakeArbitrationStore(state)
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
        backend = InMemoryStateBackend()
        backend.enqueue_jobs(queue)
        state = _state(queue)
        store = FakeArbitrationStore(state, backend=backend)
        gov = DeadlockGovernor()
        sched = StandstillFacts(min_wait=1.0, waiting_for_dependency=True)

        decision = gov.arbitrate(sched, store=store)

        assert calls["count"] >= 1, "仲裁未执行环检测，测试空转"
        assert decision.action == "resolved"
        assert set(decision.failed_uids) == {"t::c0", "t::c1"}
        assert set(store.state.queue_uids) == {f"t::j{i}" for i in range(CHAIN_N)}
        # 持久层同步：仅环成员落失败档案，磁盘队列保留全链
        assert set(backend.load_failed()) == {"t::c0", "t::c1"}
        assert len(backend.load_queue()) == CHAIN_N


class TestGraceRelapseAfterProgress:
    def test_resolve_relapse_after_progress_gets_full_grace(self):
        """死锁仲裁路径同样受益于前进终结点：盲区复发裁决为宽限而非批量熔断。"""
        gov = DeadlockGovernor(dep_grace_seconds=60.0)
        state = _state([Job("t", "b", depends_on=["t::x"]).to_dict()])
        store = FakeArbitrationStore(state)
        sched = StandstillFacts(
            min_wait=INF,
            attribution=DeadlockAttribution(missing_dependency_uids=("t::b",)),
            has_potential_spawners=True,
        )

        # episode 授予宽限（deadline=160）
        assert gov.check_dependency_grace(
            state, {"t::b"}, has_potential_spawners=True, now=100.0
        ) is True
        assert gov.dep_grace_deadline == 160.0

        # 等待者消解被派发 → 前进信号终结 episode → 仲裁授予完整新宽限
        gov.record_dispatch_progress()
        decision = gov.resolve_deadlock(sched, store=store)
        assert decision.action == "grace_waiting"
        assert decision.wait_time > 0
        assert store.bulk_calls == []
