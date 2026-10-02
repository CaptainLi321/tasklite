"""v2 只读扫描调度器测试：缓存一致性、畸形捕获、依赖兜底与 FIFO 默认序。"""

from __future__ import annotations

import pytest

from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.engine.scheduler import (
    DeadlockAttribution,
    FifoOrderingPolicy,
    JobFacts,
    JobScheduler,
    OrderingPolicy,
    StandstillFacts,
)
from tasklite.models.job import Job
from tasklite.models.state import PipelineState

INF = float("inf")


def _state(queue, wall=None, failed=None):
    return PipelineState(
        wall=wall or {}, failed=failed or {}, cursors={}, queue=list(queue)
    )


class TestJobFactsProjection:
    def test_from_job_dict_freezes_sched_fields(self):
        job = Job("t", "j", resources={"b": 2.0, "a": 1.0}, depends_on=["t::x"])
        facts = JobFacts.from_job_dict(job.to_dict())
        assert facts.uid == "t::j"
        assert facts.task_type == "t"
        assert facts.resources == (("a", 1.0), ("b", 2.0))
        assert facts.depends_on == ("t::x",)
        assert facts.resource_map() == {"a": 1.0, "b": 2.0}

    def test_frozen_projection_is_immutable(self):
        facts = JobFacts.from_job_dict(Job("t", "j").to_dict())
        with pytest.raises(Exception):
            facts.uid = "other::uid"  # type: ignore[misc]


class TestSchedFieldsMatchWhitelist:
    """缓存一致性白名单：调度只读字段 = depends_on + resources。"""

    def setup_method(self):
        self.scheduler = JobScheduler(resources={})

    def test_same_content_matches(self):
        job = Job("t", "j", rerun="every_run", resources={"a": 1.0}, depends_on=["other::x"])
        jd = job.to_dict()
        assert self.scheduler._sched_fields_match(job, jd) is True

    def test_resource_change_invalidates(self):
        job = Job("t", "j", resources={"a": 1.0}, depends_on=["other::x"])
        jd = dict(job.to_dict())
        jd["resources"] = {"b": 2.0}
        assert self.scheduler._sched_fields_match(job, jd) is False

    def test_depends_change_invalidates(self):
        job = Job("t", "j", resources={"a": 1.0}, depends_on=["other::x"])
        jd = dict(job.to_dict())
        jd["depends_on"] = []
        assert self.scheduler._sched_fields_match(job, jd) is False

    def test_non_whitelisted_fields_do_not_invalidate(self):
        """白名单外字段（实例位/策略位/负载）变化不触发缓存 miss。"""
        job = Job("t", "j", resources={"a": 1.0}, depends_on=["other::x"])
        jd = dict(job.to_dict())
        jd["attempt_no"] = 4
        jd["activation_no"] = 2
        jd["rerun"] = "every_run"
        jd["payload"] = {"k": "v2"}
        assert self.scheduler._sched_fields_match(job, jd) is True

    def test_null_semantics_aligned_with_from_dict(self):
        """null 容忍：resources/depends_on 为 None 时不得抛 TypeError。"""
        job = Job("t", "j", resources=None, depends_on=None)
        jd = {"task_type": "t", "job_id": "j", "resources": None, "depends_on": None}
        assert self.scheduler._sched_fields_match(job, jd) is True

    def test_malformed_value_returns_false_without_raising(self):
        job = Job("t", "j")
        jd = dict(job.to_dict())
        jd["resources"] = "not-a-dict"
        assert self.scheduler._sched_fields_match(job, jd) is False


class TestCachedJobContentKey:
    def test_same_dict_reuses_cached_object(self):
        scheduler = JobScheduler(resources={})
        jd = Job("t", "j", resources={"a": 1.0}).to_dict()
        first = scheduler.cached_job(jd)
        second = scheduler.cached_job(dict(jd))
        assert first is second

    def test_same_uid_changed_content_reparses(self):
        scheduler = JobScheduler(resources={})
        jd = Job("t", "j", resources={"a": 1.0}).to_dict()
        first = scheduler.cached_job(jd)
        jd_changed = dict(jd)
        jd_changed["resources"] = {"nosuch": 1.0}
        second = scheduler.cached_job(jd_changed)
        assert second is not first
        assert second.resource_map() == {"nosuch": 1.0}

    def test_begin_round_clears_cache(self):
        scheduler = JobScheduler(resources={})
        jd = Job("t", "j").to_dict()
        first = scheduler.cached_job(jd)
        scheduler.begin_round()
        assert scheduler.cached_job(jd) is not first

    def test_malformed_dict_bypasses_cache_and_raises(self):
        scheduler = JobScheduler(resources={})
        with pytest.raises(KeyError):
            scheduler.cached_job({"task_type": "t"})


class TestScanResultSemantics:
    def test_runnable_job_selected(self):
        scheduler = JobScheduler(resources={})
        state = _state([Job("t", "j1").to_dict()])
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.is_runnable is True
        assert result.is_dep_failed is False
        assert result.has_candidate is True
        assert result.candidate_uid == "t::j1"
        assert result.runnable_idx == 0
        assert result.min_wait == INF

    def test_dep_failed_fallback(self):
        scheduler = JobScheduler(resources={})
        state = _state(
            [Job("t", "j2", depends_on=["t::failed_parent"]).to_dict()],
            failed={"t::failed_parent": {"error": "fatal"}},
        )
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.is_runnable is False
        assert result.is_dep_failed is True
        assert result.has_candidate is True
        assert result.candidate_uid == "t::j2"
        assert result.pending_dep_failure == "t::failed_parent"

    def test_runnable_after_dep_failed_clears_pending_failure(self):
        """真正可运行 job 排在 dep-failed 之后 → 兜底字段清空，不得误判。"""
        scheduler = JobScheduler(resources={})
        state = _state(
            [
                Job("t", "bad", depends_on=["t::failed_parent"]).to_dict(),
                Job("t", "good").to_dict(),
            ],
            failed={"t::failed_parent": {"error": "fatal"}},
        )
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.is_runnable is True
        assert result.candidate_uid == "t::good"
        assert result.pending_dep_failure is None

    def test_empty_queue_returns_none(self):
        scheduler = JobScheduler(resources={})
        result = scheduler.scan_next_runnable(_state([]), frozenset())
        assert result.is_runnable is False
        assert result.is_dep_failed is False
        assert result.has_candidate is False
        assert result.candidate_uid is None
        assert result.kind == "none"

    def test_standstill_facts_projection(self):
        scheduler = JobScheduler(resources={})
        state = _state([Job("t", "a", depends_on=["t::missing"]).to_dict()])
        result = scheduler.scan_next_runnable(state, frozenset())
        facts = result.standstill_facts()
        assert isinstance(facts, StandstillFacts)
        assert facts.waiting_for_dependency is True
        assert facts.attribution.missing_dependency_uids == ("t::a",)
        assert facts.min_wait == result.min_wait


class TestScanAttribution:
    def test_unknown_resource_attribution_forces_inf(self):
        scheduler = JobScheduler(resources={})
        state = _state([Job("t", "x", resources={"ghost": 1.0}).to_dict()])
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.attribution.unknown_resource_uids == ("t::x",)
        assert result.min_wait == INF

    def test_impossible_resource_attribution(self):
        cap = CapacityResource("r", 1.0)
        scheduler = JobScheduler({"r": cap})
        state = _state([Job("t", "x", resources={"r": 99.0}).to_dict()])
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.attribution.impossible_resource_uids == ("t::x",)
        assert result.min_wait == INF

    def test_malformed_job_captured_without_crash(self):
        scheduler = JobScheduler(resources={})
        state = _state([{"task_type": "t"}])
        result = scheduler.scan_next_runnable(state, frozenset())
        assert len(result.attribution.malformed_uids) == 1
        assert result.attribution.malformed_uids[0].startswith("_unknown::")
        assert result.min_wait == INF

    def test_malformed_forces_inf_over_finite_resource_wait(self):
        """畸形归因不被其它 job 的有限资源等待遮蔽：min_wait 必须强制 inf。"""
        gpu = CapacityResource("gpu", 4.0)
        gpu.suspend(30.0)
        scheduler = JobScheduler({"gpu": gpu})
        state = _state(
            [
                {"task_type": "t"},
                Job("t", "waiting", resources={"gpu": 1.0}).to_dict(),
            ]
        )
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.attribution.malformed_uids and result.attribution.malformed_uids[0].startswith("_unknown::")
        assert result.min_wait == INF, "有限资源等待不得遮蔽畸形死锁归因"

    def test_finite_resource_wait_reported(self):
        gpu = CapacityResource("gpu", 4.0)
        gpu.suspend(20.0)
        scheduler = JobScheduler({"gpu": gpu})
        state = _state([Job("t", "w", resources={"gpu": 1.0}).to_dict()])
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.kind == "none"
        assert 0.0 < result.min_wait <= 20.0

    def test_missing_dependency_attribution(self):
        scheduler = JobScheduler(resources={})
        state = _state([Job("t", "a", depends_on=["t::missing"]).to_dict()])
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.attribution.missing_dependency_uids == ("t::a",)
        assert result.waiting_for_dependency is True
        assert result.has_potential_spawners is False

    def test_in_flight_dependency_not_counted_missing(self):
        """依赖正在执行的 job 不算 missing（待 commit 到 wall 自然解锁）。"""
        scheduler = JobScheduler(resources={})
        state = _state([Job("t", "a", depends_on=["t::running"]).to_dict()])
        result = scheduler.scan_next_runnable(state, frozenset({"t::running"}))
        assert result.attribution.missing_dependency_uids == ()
        assert result.waiting_for_dependency is True

    def test_has_potential_spawners_with_resource_blocked_job(self):
        gpu = CapacityResource("gpu", 1.0)
        gpu.used = 1.0
        scheduler = JobScheduler({"gpu": gpu})
        state = _state(
            [
                Job("t", "a", depends_on=["t::missing"]).to_dict(),
                Job("t", "spawner", resources={"gpu": 1.0}).to_dict(),
            ]
        )
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.runnable_idx is None
        assert result.has_potential_spawners is True

    def test_scan_stops_at_first_runnable(self):
        """首中即停：选中可运行 job 后不再扫描后续条目（惰性归因）。"""
        scheduler = JobScheduler(resources={})
        state = _state(
            [
                Job("t", "first").to_dict(),
                Job("t", "ghost", resources={"nosuch": 1.0}).to_dict(),
            ]
        )
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.is_runnable is True
        assert result.candidate_uid == "t::first"
        assert result.attribution.unknown_resource_uids == ()


class TestOrderingPolicySeam:
    def test_fifo_default_selects_first_runnable_in_seq_order(self):
        scheduler = JobScheduler(resources={})
        assert isinstance(scheduler.ordering, FifoOrderingPolicy)
        state = _state([Job("t", "a").to_dict(), Job("t", "b").to_dict()])
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.candidate_uid == "t::a"

    def test_fifo_visit_order_is_seq_ascending(self):
        policy = FifoOrderingPolicy()
        assert list(policy.visit_order([{}, {}, {}])) == [0, 1, 2]
        assert list(policy.visit_order([])) == []

    def test_custom_ordering_policy_pluggable(self):
        """接缝插入位：自定义访问序策略可替换默认实现，核心不改。"""

        class ReverseOrderingPolicy(OrderingPolicy):
            def visit_order(self, queue):
                return reversed(range(len(queue)))

        scheduler = JobScheduler(resources={}, ordering=ReverseOrderingPolicy())
        state = _state([Job("t", "a").to_dict(), Job("t", "b").to_dict()])
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.candidate_uid == "t::b"

    def test_custom_ordering_governs_dep_failed_fallback(self):
        """dep-failed 兜底同样按访问序取首个（倒序时取队列末尾的 dep-failed）。"""

        class ReverseOrderingPolicy(OrderingPolicy):
            def visit_order(self, queue):
                return reversed(range(len(queue)))

        scheduler = JobScheduler(resources={}, ordering=ReverseOrderingPolicy())
        state = _state(
            [
                Job("t", "head", depends_on=["t::failed_parent"]).to_dict(),
                Job("t", "tail", depends_on=["t::other_parent"]).to_dict(),
            ],
            failed={"t::failed_parent": {"error": "x"}, "t::other_parent": {"error": "y"}},
        )
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.is_dep_failed is True
        assert result.candidate_uid == "t::tail"
        assert result.pending_dep_failure == "t::other_parent"

    def test_scheduler_accepts_resource_manager_directly(self):
        gpu = CapacityResource("gpu", 4.0)
        rm = ResourceManager({"gpu": gpu})
        scheduler = JobScheduler(rm)
        assert scheduler.resource_mgr is rm
        state = _state([Job("t", "a", resources={"gpu": 1.0}).to_dict()])
        result = scheduler.scan_next_runnable(state, frozenset())
        assert result.is_runnable is True


class TestDeadlockAttributionValue:
    def test_has_deadlock_causes(self):
        assert DeadlockAttribution().has_deadlock_causes is False
        assert DeadlockAttribution(malformed_uids=("t::x",)).has_deadlock_causes is True
        assert DeadlockAttribution(missing_dependency_uids=("t::x",)).has_deadlock_causes is True
        assert DeadlockAttribution(unknown_resource_uids=("t::x",)).has_deadlock_causes is True
        assert DeadlockAttribution(impossible_resource_uids=("t::x",)).has_deadlock_causes is True
