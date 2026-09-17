"""PreflightPolicy 策略深模块单元测试套件。"""

import math
import os
import time
import pytest

from tasklite.engine.policy import (
    BackoffSchedule,
    DecisionReason,
    PreflightAction,
    PreflightDecision,
    PreflightPolicy,
)


class TestPreflightEvaluationMatrix:
    def test_fresh_job_always_runs(self):
        policy = PreflightPolicy()
        dec = policy.evaluate({"task_type": "t", "rerun": "never"}, is_wall=False, is_failed=False)
        assert dec.should_run
        assert not dec.should_skip
        assert dec.reason == DecisionReason.FRESH
        assert dec.action == PreflightAction.RUN

    def test_never_strategy_blocks_wall_and_failed(self):
        policy = PreflightPolicy()
        dec_wall = policy.evaluate({"task_type": "t", "rerun": "never"}, is_wall=True, is_failed=False)
        assert dec_wall.should_skip
        assert dec_wall.reason == DecisionReason.WALL_BLOCKED

        dec_failed = policy.evaluate({"task_type": "t", "rerun": "never"}, is_wall=False, is_failed=True)
        assert dec_failed.should_skip
        assert dec_failed.reason == DecisionReason.FAILED_BLOCKED

    def test_every_run_strategy_allows_both(self):
        policy = PreflightPolicy()
        dec_wall = policy.evaluate({"task_type": "t", "rerun": "every_run"}, is_wall=True, is_failed=False)
        assert dec_wall.should_run
        assert dec_wall.reason == DecisionReason.EVERY_RUN

        dec_failed = policy.evaluate({"task_type": "t", "rerun": "every_run"}, is_wall=False, is_failed=True)
        assert dec_failed.should_run
        assert dec_failed.reason == DecisionReason.EVERY_RUN

    def test_on_failure_strategy_allows_failed_blocks_wall(self):
        policy = PreflightPolicy()
        dec_failed = policy.evaluate({"task_type": "t", "rerun": "on_failure"}, is_wall=False, is_failed=True)
        assert dec_failed.should_run
        assert dec_failed.reason == DecisionReason.ON_FAILURE_MATCH

        dec_wall = policy.evaluate({"task_type": "t", "rerun": "on_failure"}, is_wall=True, is_failed=False)
        assert dec_wall.should_skip
        assert dec_wall.reason == DecisionReason.WALL_BLOCKED

    def test_admit_narrow_interface(self):
        """测试 AdmissionPolicy.admit 统一极窄接口。"""
        from types import SimpleNamespace
        from tasklite.engine.policy import AdmissionPolicy

        policy = AdmissionPolicy()
        store_mock = SimpleNamespace(
            wall={"t::wall_job": {}},
            failed={"t::failed_job": {}},
        )

        # 1. 未命中历史 -> fresh -> run
        d1 = policy.admit({"task_type": "t", "job_id": "new_job", "rerun": "never"}, store_mock)
        assert d1.should_run
        assert d1.reason == DecisionReason.FRESH

        # 2. 命中 wall 且 never -> wall_blocked -> skip
        d2 = policy.admit({"task_type": "t", "job_id": "wall_job", "rerun": "never"}, store_mock)
        assert d2.should_skip
        assert d2.reason == DecisionReason.WALL_BLOCKED

        # 3. 命中 failed 且 on_failure -> on_failure_match -> run
        d3 = policy.admit({"task_type": "t", "job_id": "failed_job", "rerun": "on_failure"}, store_mock)
        assert d3.should_run
        assert d3.reason == DecisionReason.ON_FAILURE_MATCH


class TestInputChangeAndStatCache:
    def test_input_changed_detects_mtime_and_size(self, tmp_path):
        f = tmp_path / "data.txt"
        f.write_text("hello")
        st = f.stat()

        policy = PreflightPolicy()
        wall_meta = {"inputs": [{"path": str(f), "size": st.st_size, "mtime_ns": st.st_mtime_ns}]}
        assert not policy.check_input_changed(wall_meta)

        # 修改内容
        f.write_text("hello world")
        policy.clear_stat_cache()
        assert policy.check_input_changed(wall_meta)

    def test_input_changed_stat_cache_avoids_repeated_syscalls(self, tmp_path):
        f = tmp_path / "data.txt"
        f.write_text("hello")
        st = f.stat()

        policy = PreflightPolicy(enable_stat_cache=True)
        wall_meta = {"inputs": [{"path": str(f), "size": st.st_size, "mtime_ns": st.st_mtime_ns}]}
        assert not policy.check_input_changed(wall_meta)

        # 文件被删但 cache 命中
        f.unlink()
        assert not policy.check_input_changed(wall_meta)

        # 清除 cache 后感知到文件缺失
        policy.clear_stat_cache()
        assert policy.check_input_changed(wall_meta)

    def test_input_changed_corrupt_data_fail_safe(self):
        policy = PreflightPolicy()
        # 非 dict、非 list、损坏元素均安全判定为 True（放行重跑）
        assert policy.check_input_changed(None)
        assert policy.check_input_changed({})
        assert policy.check_input_changed({"inputs": "corrupt_string"})
        assert policy.check_input_changed({"inputs": [42, "bar"]})
        assert policy.check_input_changed({"inputs": [{"path": 12345}]})


class TestDiscoveryPolicyNormalization:
    def test_normalize_injects_default_only_when_none(self):
        policy = PreflightPolicy({"scan": "every_run"})

        j1 = {"task_type": "scan", "rerun": None}
        assert policy.normalize_job_dict(j1, "scan")
        assert j1["rerun"] == "every_run"

        j2 = {"task_type": "scan", "rerun": "never"}
        assert not policy.normalize_job_dict(j2, "scan")
        assert j2["rerun"] == "never"

        j3 = {"task_type": "scan", "rerun": "on_failure"}
        assert not policy.normalize_job_dict(j3, "scan")
        assert j3["rerun"] == "on_failure"


class TestBackoffScheduleInvariants:
    def test_compute_backoff_basic_exponential(self):
        policy = PreflightPolicy()
        assert policy.compute_backoff(-1) == 0.0
        assert policy.compute_backoff(0) == 0.0

        for r in range(1, 10):
            d = policy.compute_backoff(r, backoff_base=2.0, backoff_max=300.0)
            assert d >= 0.0

    def test_compute_backoff_overflow_capping(self):
        policy = PreflightPolicy()
        # retries >= 1025 不会 OverflowError
        d = policy.compute_backoff(2000, backoff_base=2.0, backoff_max=300.0)
        assert 0.0 <= d <= 375.0

    def test_compute_backoff_nan_inf_sanitizer(self):
        policy = PreflightPolicy()
        d1 = policy.compute_backoff(1, backoff_base=float("nan"))
        assert 0.0 <= d1 <= 5.0
        d2 = policy.compute_backoff(1, backoff_max=float("inf"))
        assert 0.0 <= d2 <= 5.0

    def test_compute_backoff_schedule_dual_clock_alignment(self):
        policy = PreflightPolicy()
        now_mono = 100.0
        now_wall = 1700000000.0
        sched = policy.compute_backoff_schedule(
            1, backoff_base=2.0, backoff_max=10.0, now_mono=now_mono, now_wall=now_wall
        )
        assert math.isclose(sched.backoff_until - now_mono, sched.delay, abs_tol=1e-6)
        assert math.isclose(sched.wall_deadline - now_wall, sched.delay, abs_tol=1e-6)

        rt = {}
        sched.populate_runtime(rt)
        assert rt["_backoff_until"] == sched.backoff_until
        assert rt["_backoff_wall_deadline"] == sched.wall_deadline

    def test_compute_orphan_schedule(self):
        policy = PreflightPolicy()
        now_mono = 200.0
        now_wall = 1700000500.0
        sched = policy.compute_orphan_schedule(now_mono=now_mono, now_wall=now_wall)
        assert 0.75 <= sched.delay <= 1.0
        assert math.isclose(sched.backoff_until - now_mono, sched.delay, abs_tol=1e-6)
        assert math.isclose(sched.wall_deadline - now_wall, sched.delay, abs_tol=1e-6)


class TestRetryPlanStateMachine:
    def test_plan_retry_within_budget(self):
        from tasklite.engine.policy import ExecutionPolicy
        from tasklite.models.job import Job

        policy = ExecutionPolicy()
        job = Job("t", "1", max_retries=3, retries=0)
        job_dict = job.to_dict()

        plan = policy.plan_retry(job, job_dict, retry_error="transient network")
        assert plan.going_to_retry is True
        assert job.retries == 1
        assert plan.delay > 0.0
        assert plan.retry_dict is not None
        assert plan.retry_dict["retries"] == 1
        assert plan.retry_dict["runtime"]["_last_retry_error"] == "transient network"
        assert "_backoff_until" in plan.retry_dict["runtime"]
        assert "_backoff_wall_deadline" in plan.retry_dict["runtime"]

    def test_plan_retry_exceeded_budget_returns_dlq_meta(self):
        from tasklite.engine.policy import ExecutionPolicy
        from tasklite.models.job import Job

        policy = ExecutionPolicy()
        job = Job("t", "1", max_retries=3, retries=3)
        job_dict = job.to_dict()
        job_dict["runtime"] = {"_last_retry_error": "prev error"}

        plan = policy.plan_retry(job, job_dict, retry_error="final error")
        assert plan.going_to_retry is False
        assert job.retries == 3
        assert plan.fail_meta is not None
        assert plan.fail_meta["error"] == "MAX_RETRIES_EXCEEDED"
        assert plan.fail_meta["last_retry_error"] == "prev error"
        assert plan.fail_meta["retry_error"] == "final error"

    def test_plan_retry_interrupted_and_lock_conflict_exempt_from_dlq(self):
        from tasklite.engine.policy import ExecutionPolicy
        from tasklite.models.job import Job

        policy = ExecutionPolicy()
        job = Job("t", "1", max_retries=3, retries=3)
        job_dict = job.to_dict()

        # 即使 retries == 3，interrupted 也豁免 DLQ 且不消耗重试预算
        plan_int = policy.plan_retry(job, job_dict, transient_kind="interrupted")
        assert plan_int.going_to_retry is True
        assert plan_int.transient_kind == "interrupted"
        assert job.retries == 3  # 不增加
        assert 0.75 <= plan_int.delay <= 1.0

        # lock_conflict 同样豁免 DLQ 且不消耗重试预算
        plan_lock = policy.plan_retry(job, job_dict, transient_kind="lock_conflict")
        assert plan_lock.going_to_retry is True
        assert plan_lock.transient_kind == "lock_conflict"
        assert job.retries == 3  # 不增加
        assert 0.75 <= plan_lock.delay <= 1.0

    def test_plan_retry_preserves_top_level_custom_fields(self):
        """重试 = 原作业原样重入队：job_dict 顶层自定义字段随重试往返保留。

        enqueue 对用户入队 dict 全量保留（自定义顶层字段合法落盘），重试
        字典若只从 Job 固定 schema 序列化重建，自定义字段即静默丢失，落盘
        的持久化队列行与用户原始入队不再一致。
        """
        from tasklite.engine.policy import ExecutionPolicy
        from tasklite.models.job import Job

        policy = ExecutionPolicy()
        job_dict = {
            "task_type": "t",
            "job_id": "1",
            "payload": {"k": "v"},
            "resources": {"__workers__": 1.0},
            "custom_field": "keepme",
            "priority_hint": 7,
        }
        job = Job.from_dict(job_dict)

        plan = policy.plan_retry(job, job_dict, retry_error="boom")
        assert plan.going_to_retry is True
        rd = plan.retry_dict
        # 受管键以 job 权威状态为准（retries 已递增）
        assert rd["retries"] == 1
        assert rd["task_type"] == "t" and rd["job_id"] == "1"
        # 顶层自定义字段原样保留
        assert rd["custom_field"] == "keepme"
        assert rd["priority_hint"] == 7

    def test_plan_orphan_defer_populates_runtime(self):
        from tasklite.engine.policy import ExecutionPolicy

        policy = ExecutionPolicy()
        job_dict = {"task_type": "t", "job_id": "1"}
        sched = policy.plan_orphan_defer(job_dict)
        assert 0.75 <= sched.delay <= 1.0
        assert "_backoff_until" in job_dict["runtime"]
        assert "_backoff_wall_deadline" in job_dict["runtime"]

