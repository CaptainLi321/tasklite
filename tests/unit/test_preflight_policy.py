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
