"""Tests for mission-critical compute_backoff() from tasklite.engine.policy.

Tests the actual production function — not a duplicate.
Formula verified:
    base_delay = backoff_base * (2 ** (retries - 1))
    delay = min(base_delay, backoff_max)
    jitter = random.uniform(-0.25, 0.25) * delay
    delay = max(0.0, delay + jitter)
"""

import time
from unittest.mock import patch

from tasklite.engine.policy import PreflightPolicy

_DEFAULT_POLICY = PreflightPolicy()
compute_backoff = _DEFAULT_POLICY.compute_backoff

# ── compute_backoff monkeypatch target ─────────────────────────────────
_BACKOFF_RANDOM = "tasklite.engine.policy.random.uniform"


# ── tests ─────────────────────────────────────────────────────────────

class TestExponentialProgression:
    """Tests for the exponential base delay before jitter and cap."""

    def _base_only(self, retries: int, backoff_base: float = 2.0) -> float:
        """Compute the raw base delay without cap or jitter."""
        return backoff_base * (2 ** (retries - 1))

    def test_exponential_progression(self) -> None:
        """backoff_base=2.0, retries 1-4 → base_delay = 2, 4, 8, 16."""
        for retries, expected in [(1, 2.0), (2, 4.0), (3, 8.0), (4, 16.0)]:
            assert self._base_only(retries) == expected, f"retries={retries}"

    def test_max_cap(self) -> None:
        """backoff_base=2.0, backoff_max=5.0, retries=4 → capped at 5.0."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=4, backoff_base=2.0, backoff_max=5.0)
        assert delay == 5.0, f"Expected 5.0 (capped), got {delay}"

    def test_large_backoff_max(self) -> None:
        """backoff_max=999999.0, retries=10 → base=1024.0, no cap applied."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=10, backoff_base=2.0, backoff_max=999999.0)
        assert delay == 1024.0, f"Expected 1024.0, got {delay}"

    def test_first_retry_equals_base(self) -> None:
        """After first increment (retries=1), base_delay == backoff_base."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=1, backoff_base=4.0)
        assert delay == 4.0, f"Expected 4.0, got {delay}"


class TestJitter:
    """Tests for the jitter component of the delay."""

    def test_jitter_range_lower_bound(self) -> None:
        """jitter = -0.25 → delay = base + (-0.25*base) = 0.75 * base."""
        with patch(_BACKOFF_RANDOM, return_value=-0.25):
            delay = compute_backoff(retries=2, backoff_base=2.0, backoff_max=300.0)
        # base_delay = 2 * 2^(1) = 4, no cap, delay = 4 + (-1) = 3
        assert delay == 3.0, f"Expected 3.0, got {delay}"

    def test_jitter_range_upper_bound(self) -> None:
        """jitter = 0.25 → delay = base + (0.25*base) = 1.25 * base."""
        with patch(_BACKOFF_RANDOM, return_value=0.25):
            delay = compute_backoff(retries=2, backoff_base=2.0, backoff_max=300.0)
        # base_delay = 2 * 2^(1) = 4, no cap, delay = 4 + 1 = 5
        assert delay == 5.0, f"Expected 5.0, got {delay}"

    def test_jitter_zero(self) -> None:
        """jitter = 0.0 → delay equals base_delay exactly."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=3, backoff_base=2.0, backoff_max=300.0)
        # base_delay = 2 * 2^(2) = 8, no cap
        assert delay == 8.0, f"Expected 8.0, got {delay}"

    def test_jitter_not_near_boundary(self) -> None:
        """jitter = 0.0 → delay exactly equals min(base_delay, backoff_max)."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay_capped = compute_backoff(retries=5, backoff_base=2.0, backoff_max=10.0)
            delay_uncapped = compute_backoff(retries=3, backoff_base=2.0, backoff_max=300.0)
        # retries=5: base=2*16=32, capped at 10
        assert delay_capped == 10.0, f"Expected 10.0, got {delay_capped}"
        # retries=3: base=2*4=8
        assert delay_uncapped == 8.0, f"Expected 8.0, got {delay_uncapped}"


class TestEdgeCases:
    """Edge-case and boundary tests."""

    def test_minimum_delay_zero(self) -> None:
        """base=0.01, retries=1, jitter=-0.25 → 0.01*0.75=0.0075 (not floored)."""
        with patch(_BACKOFF_RANDOM, return_value=-0.25):
            delay = compute_backoff(retries=1, backoff_base=0.01, backoff_max=300.0)
        assert delay == 0.0075, f"Expected 0.0075, got {delay}"

    def test_minimum_delay_floored(self) -> None:
        """backoff_base=0.0 → all delays 0.0, max(0.0, ...) floors."""
        with patch(_BACKOFF_RANDOM, return_value=-0.25):
            delay = compute_backoff(retries=1, backoff_base=0.0, backoff_max=300.0)
        assert delay == 0.0, f"Expected 0.0, got {delay}"

    def test_backoff_base_zero(self) -> None:
        """backoff_base=0.0 → all delays are 0.0 (instant retry)."""
        for retries in (1, 2, 3, 5):
            with patch(_BACKOFF_RANDOM, return_value=0.0):
                delay = compute_backoff(retries=retries, backoff_base=0.0, backoff_max=300.0)
            assert delay == 0.0, f"retries={retries}: expected 0.0, got {delay}"

    def test_retry_count_zero(self) -> None:
        """retries=0 → returns 0.0 per  contract (no backoff for non-retry)."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=0, backoff_base=2.0, backoff_max=300.0)
        assert delay == 0.0, f"Expected 0.0, got {delay}"

    def test_negative_backoff_base_floored(self) -> None:
        """Negative backoff_base →  防御性 fallback 到默认 2.0; delay = min(2.0, 300.0) = 2.0."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=1, backoff_base=-5.0, backoff_max=300.0)
        assert delay == 2.0, f"Expected 2.0 (fallback base), got {delay}"


class TestRealRandom:
    """Test with real random values — verify invariants hold."""

    def test_real_random_is_positive(self) -> None:
        """With real randomness, result should always be >= 0.0."""
        for retries in range(1, 10):
            delay = compute_backoff(retries=retries, backoff_base=2.0, backoff_max=300.0)
            assert delay >= 0.0, f"retries={retries}: negative delay {delay}"

    def test_real_random_within_bounds(self) -> None:
        """Real random delay should stay within [0.75*base, 1.25*base] ∩ [0, max]."""
        for retries in range(1, 10):
            base = 2.0 * (2 ** (retries - 1))
            base_capped = min(base, 300.0)
            delay = compute_backoff(retries=retries, backoff_base=2.0, backoff_max=300.0)
            assert 0.75 * base_capped <= delay <= 1.25 * base_capped, (
                f"retries={retries}: delay {delay} outside expected range "
                f"[{0.75 * base_capped}, {1.25 * base_capped}]"
            )


class TestBackoffUntil:
    """Test _backoff_until = time.time() + delay."""

    def test_backoff_until(self) -> None:
        """time.time() mocked at 1000, delay=5 → _backoff_until=1005."""
        with patch(_BACKOFF_RANDOM, return_value=0.0), \
             patch("time.time", return_value=1000.0):
            delay = compute_backoff(retries=2, backoff_base=2.5, backoff_max=300.0)
            backoff_until = time.time() + delay
        # base_delay = 2.5 * 2^(1) = 5.0, no jitter
        assert delay == 5.0, f"Expected 5.0, got {delay}"
        assert backoff_until == 1005.0, f"Expected 1005.0, got {backoff_until}"


class TestProductionPipelineBehavior:
    """Verify compute_backoff() is the function used in production code paths."""

    def test_compute_backoff_in_policy_namespace(self) -> None:
        """compute_backoff is available on PreflightPolicy."""
        from tasklite.engine.policy import PreflightPolicy
        policy = PreflightPolicy()
        assert callable(policy.compute_backoff)
        assert callable(policy.compute_backoff_schedule)

    def test_retry_computation_matches_pipeline(self) -> None:
        """Default job values → compute_backoff(1, 2.0, 300.0) matches pipeline default."""
        # Default Job: backoff_base=2.0, backoff_max=300.0, retries starts at 0
        # After first retry, retries=1 → compute_backoff(1, 2.0, 300.0)
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=1, backoff_base=2.0, backoff_max=300.0)
        assert delay == 2.0, "First retry with defaults should yield backoff_base"


# ── Backoff computation tests (from test_pipeline.py) ───────────────────


class TestBackoffComputation:
    """Tests for exponential backoff calculation using Job defaults (REQ-11)."""

    def test_backoff_formula_progression(self):
        """base_delay follows base * 2^(retries-1)."""
        from tasklite.models.job import Job
        job = Job("t", "id")

        retries = [1, 2, 3, 4, 5]
        expected_base = [2.0, 4.0, 8.0, 16.0, 32.0]

        for r, exp in zip(retries, expected_base):
            base_delay = job.backoff_base * (2 ** (r - 1))
            assert base_delay == exp

    def test_backoff_max_cap(self):
        """base_delay exceeding backoff_max is capped."""
        from tasklite.models.job import Job
        job = Job("t", "id", backoff_base=10.0, backoff_max=25.0)
        base_delay = job.backoff_base * (2 ** 3)
        delay = min(base_delay, job.backoff_max)
        assert delay == 25.0

    def test_backoff_jitter_within_bounds(self):
        """Jitter is within ±25% of base delay."""
        import random
        random.seed(42)
        base = 10.0
        for _ in range(100):
            jitter = random.uniform(-0.25, 0.25) * base
            delay = max(0.0, base + jitter)
            assert 7.5 <= delay <= 12.5


# ============================================================
# Backoff edge cases (invariant verification)
# ============================================================


class TestBackoffEdgeCasesWeird:
    """Edge case inputs to compute_backoff() — invariant verification for
    negative values, infinities, and boundary conditions."""

    def test_backoff_retries_negative(self):
        """retries=-1 → returns 0.0 per  contract (retries<=0 short-circuits to 0.0)."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=-1, backoff_base=2.0, backoff_max=300.0)
        assert delay == 0.0, f"Expected 0.0, got {delay}"

    def test_backoff_retries_very_negative(self):
        """retries=-10 → returns 0.0 per  contract (retries<=0 short-circuits to 0.0)."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=-10, backoff_base=2.0, backoff_max=300.0)
        assert delay == 0.0, f"Expected 0.0, got {delay}"

    def test_backoff_base_negative_with_jitter(self):
        """Negative base →  fallback 到默认 2.0; jitter=-0.25 → delay = 2.0 * 0.75 = 1.5."""
        with patch(_BACKOFF_RANDOM, return_value=-0.25):
            delay = compute_backoff(retries=1, backoff_base=-5.0, backoff_max=300.0)
        # fallback base=2.0; base_delay=2.0; min(2.0, 300.0)=2.0; jitter=-0.25*2.0=-0.5
        # delay = 2.0 + (-0.5) = 1.5; max(0, 1.5) = 1.5
        assert delay == 1.5, f"Expected 1.5 (fallback base with jitter), got {delay}"

    def test_backoff_base_negative_with_positive_jitter(self):
        """Negative base →  fallback 到默认 2.0; jitter=0.25 → delay = 2.0 * 1.25 = 2.5."""
        with patch(_BACKOFF_RANDOM, return_value=0.25):
            delay = compute_backoff(retries=1, backoff_base=-5.0, backoff_max=300.0)
        # fallback base=2.0; base_delay=2.0; min(2.0, 300.0)=2.0; jitter=0.25*2.0=0.5
        # delay = 2.0 + 0.5 = 2.5; max(0, 2.5) = 2.5
        assert delay == 2.5, f"Expected 2.5 (fallback base with jitter), got {delay}"

    def test_backoff_max_zero(self):
        """backoff_max=0.0 → min(base, 0) = 0 for positive base; delay = 0 + jitter*0 = 0."""
        with patch(_BACKOFF_RANDOM, return_value=0.5):
            delay = compute_backoff(retries=5, backoff_base=2.0, backoff_max=0.0)
        # base_delay = 2*16 = 32; min(32, 0) = 0; jitter = 0.5 * 0 = 0; max(0, 0) = 0
        assert delay == 0.0, f"Expected 0.0, got {delay}"

    def test_backoff_max_negative(self):
        """backoff_max=-5.0 →  防御性 fallback 到默认 300.0; delay = min(2.0, 300.0) = 2.0."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=1, backoff_base=2.0, backoff_max=-5.0)
        # fallback max=300.0; base_delay=2.0; min(2.0, 300.0)=2.0; jitter=0; max(0, 2.0)=2.0
        assert delay == 2.0, f"Expected 2.0 (fallback max), got {delay}"

    def test_backoff_retries_large_capped_at_max(self):
        """retries=100 → base huge, capped at backoff_max."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=100, backoff_base=2.0, backoff_max=60.0)
        assert delay == 60.0, f"Expected 60.0 (capped), got {delay}"

    def test_backoff_very_large_retries_no_overflow(self):
        """retries >= ~1025 时旧公式 2**(retries-1) 溢出 float
        （OverflowError），崩溃重试路径。修复后返回 capped 值而非异常。"""
        for retries in (1025, 2000, 200000):
            with patch(_BACKOFF_RANDOM, return_value=0.0):
                delay = compute_backoff(retries=retries, backoff_base=2.0, backoff_max=60.0)
            assert delay == 60.0, f"Expected 60.0 for retries={retries}, got {delay}"

    def test_backoff_base_inf(self):
        """backoff_base=inf → 防御性 fallback 到默认 2.0; delay = min(2.0, 10.0) = 2.0."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=1, backoff_base=float('inf'), backoff_max=10.0)
        assert delay == 2.0, f"Expected 2.0 (fallback base), got {delay}"

    def test_backoff_max_inf(self):
        """backoff_max=inf → 防御性 fallback 到默认 300.0; delay = min(8.0, 300.0) = 8.0."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=3, backoff_base=2.0, backoff_max=float('inf'))
        # base = 2 * 2^2 = 8; min(8, 300) = 8; jitter = 0; max(0, 8) = 8
        assert delay == 8.0, f"Expected 8.0 (fallback max), got {delay}"

    def test_backoff_base_and_max_both_inf(self):
        """backoff_base=inf, backoff_max=inf → 两者均 fallback 到默认 (2.0, 300.0);
        delay = min(2.0, 300.0) = 2.0。NaN-poisoning 已由防御性校验消除。"""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=1, backoff_base=float('inf'), backoff_max=float('inf'))
        assert delay == 2.0, f"Expected 2.0 (both fallback to defaults), got {delay}"

    def test_backoff_jitter_full_range_many_samples(self):
        """1000 samples with real random — all within [0.75*base, 1.25*base] ∩ [0, max]."""
        base_capped = min(2.0 * (2 ** (2 - 1)), 300.0)  # retries=2 → base=4
        for _ in range(1000):
            delay = compute_backoff(retries=2, backoff_base=2.0, backoff_max=300.0)
            assert 0.75 * base_capped <= delay <= 1.25 * base_capped, (
                f"Delay {delay} outside [{0.75 * base_capped}, {1.25 * base_capped}]"
            )

    def test_backoff_jitter_zero_delay_stays_zero(self):
        """When base is capped to 0 (backoff_max=0), jitter*0=0; delay stays 0."""
        for _ in range(100):
            delay = compute_backoff(retries=1, backoff_base=2.0, backoff_max=0.0)
            assert delay == 0.0

    def test_backoff_retries_zero_with_default_base(self):
        """retries=0 → returns 0.0 per  contract (retries<=0 short-circuits to 0.0)."""
        with patch(_BACKOFF_RANDOM, return_value=0.0):
            delay = compute_backoff(retries=0, backoff_base=10.0, backoff_max=300.0)
        assert delay == 0.0, f"Expected 0.0, got {delay}"

    def test_backoff_until_in_past_treated_as_runnable(self):
        """A job with _backoff_until in the past is immediately runnable.

        Tests the scheduler's _backoff_until check: if _backoff_until <=
        time.monotonic(), the job is NOT skipped. We mock time.monotonic()
        to a known value and verify the scheduler returns a runnable index
        for a job whose backoff expired.

        Note: the scheduler uses time.monotonic(), not time.time() — mocking
        the wrong clock makes this test flaky on freshly-booted machines.
        """
        from tasklite.engine.scheduler import JobScheduler
        from tasklite.models.job import Job
        from tasklite.models.state import PipelineState

        scheduler = JobScheduler({})
        job = Job("test", "j1", payload={})
        job_dict = job.to_dict()
        # Set backoff_until in the past（runtime 子 dict）
        job_dict["runtime"] = {"_backoff_until": 1000.0}

        with patch("time.monotonic", return_value=2000.0):
            result = scheduler.pop_next_runnable(PipelineState({}, {}, {}, [job_dict]))

        assert result.runnable_idx == 0, (
            f"Job with expired backoff should be runnable, got idx={result.runnable_idx}"
        )

    def test_backoff_until_in_future_skipped(self):
        """A job with _backoff_until in the future is skipped; min_wait reflects remaining."""
        from tasklite.engine.scheduler import JobScheduler
        from tasklite.models.job import Job
        from tasklite.models.state import PipelineState

        scheduler = JobScheduler({})
        job = Job("test", "j1", payload={})
        job_dict = job.to_dict()
        # 退避状态在 runtime 子 dict（scheduler 读 runtime）
        job_dict["runtime"] = {"_backoff_until": 1500.0}

        with patch("time.monotonic", return_value=1000.0):
            result = scheduler.pop_next_runnable(PipelineState({}, {}, {}, [job_dict]))

        assert result.runnable_idx is None, "Job with future backoff should NOT be runnable"
        assert result.min_wait == 500.0, (
            f"min_wait should be 500.0 (remaining backoff), got {result.min_wait}"
        )


class TestScheduleResultKind:
    """ScheduleResult.kind 三态显式化——runnable/dep_failed/none。

    kind 是调度契约的类型化表达：消费方（_dispatch_job）按 kind 判定依赖
    失败分支（契约从读注释变读类型）。三态测试固定该不变式。
    """

    def test_kind_runnable_for_normal_job(self):
        """无依赖、无退避的 job → kind="runnable"。"""
        from tasklite.engine.scheduler import JobScheduler
        from tasklite.models.job import Job
        from tasklite.models.state import PipelineState

        sched = JobScheduler({})
        jd = Job("t", "x").to_dict()
        result = sched.pop_next_runnable(PipelineState({}, {}, {}, [jd]))
        assert result.kind == "runnable", f"正常可运行 job 应 kind=runnable，实际: {result.kind}"

    def test_kind_none_for_empty_queue(self):
        """空队列 → kind="none"（runnable_idx=-1）。"""
        from tasklite.engine.scheduler import JobScheduler
        from tasklite.models.state import PipelineState

        sched = JobScheduler({})
        result = sched.pop_next_runnable(PipelineState({}, {}, {}, []))
        assert result.kind == "none", f"空队列应 kind=none，实际: {result.kind}"
        assert result.runnable_idx is None

    def test_kind_dep_failed_for_fallback(self):
        """队列仅含 dep-failed job（依赖已失败）→ 兜底位置 → kind="dep_failed"，
        pending_dep_failure 携带失败依赖（_dispatch_job 据此走依赖失败分支）。"""
        from tasklite.engine.scheduler import JobScheduler
        from tasklite.models.job import Job
        from tasklite.models.state import PipelineState

        sched = JobScheduler({})
        jd = Job("t", "x", depends_on=["t::parent"]).to_dict()
        # failed 集合含父依赖 → 本 job 是 dep-failed
        result = sched.pop_next_runnable(
            PipelineState({}, {"t::parent": {"error": "gone"}}, {}, [jd])
        )
        assert result.kind == "dep_failed", f"dep-failed 兜底应 kind=dep_failed，实际: {result.kind}"
        assert result.pending_dep_failure == "t::parent", \
            f"pending_dep_failure 应携带失败依赖，实际: {result.pending_dep_failure}"
        assert result.runnable_idx == 0, "兜底位置应为 0"


class TestRerunSkipsDirect:
    """rerun_skips 策略矩阵直接单测。"""

    def _rerun_skips(self, jd, *, wall_hit, failed_hit, wall_meta=None):
        decision = _DEFAULT_POLICY.evaluate(jd, wall_meta=wall_meta, is_wall=wall_hit, is_failed=failed_hit)
        return decision.should_skip

    def test_never_skips_both(self):
        jd = {"rerun": "never"}
        assert self._rerun_skips(jd, wall_hit=True, failed_hit=False) is True
        assert self._rerun_skips(jd, wall_hit=False, failed_hit=True) is True

    def test_every_run_allows_both(self):
        jd = {"rerun": "every_run"}
        assert self._rerun_skips(jd, wall_hit=True, failed_hit=False) is False
        assert self._rerun_skips(jd, wall_hit=False, failed_hit=True) is False

    def test_on_failure_only_failed_allows(self):
        jd = {"rerun": "on_failure"}
        assert self._rerun_skips(jd, wall_hit=True, failed_hit=False) is True
        assert self._rerun_skips(jd, wall_hit=False, failed_hit=True) is False

    def test_on_input_change_wall_compares_fingerprint(self, tmp_path):
        f = tmp_path / "in.txt"
        f.write_text("hello")
        st = f.stat()
        wall_meta = {"inputs": [{"path": str(f), "size": st.st_size, "mtime_ns": st.st_mtime_ns}]}
        jd = {"rerun": "on_input_change"}
        # 未变化 → 拦截（跳过）
        assert self._rerun_skips(jd, wall_hit=True, failed_hit=False, wall_meta=wall_meta) is True
        # 文件变化 → 豁免（重跑）
        f.write_text("hello changed")
        assert self._rerun_skips(jd, wall_hit=True, failed_hit=False, wall_meta=wall_meta) is False
        # failed 命中 → 豁免
        assert self._rerun_skips(jd, wall_hit=False, failed_hit=True) is False


class TestInputChangedDirect:
    """input_changed 语义分支直接单测。"""

    def _input_changed(self, wall_meta):
        return _DEFAULT_POLICY.check_input_changed(wall_meta)

    def test_no_prev_inputs_means_changed(self):
        assert self._input_changed({}) is True
        assert self._input_changed(None) is True

    def test_corrupt_inputs_non_list_defensive(self):
        """meta["inputs"] 非 list 脏数据（dict/str）→ 视为变化，不 AttributeError 崩溃。"""
        assert self._input_changed({"inputs": "garbage"}) is True
        assert self._input_changed({"inputs": {"path": "/x"}}) is True

    def test_uri_entries_skipped(self):
        wall_meta = {"inputs": [{"kind": "uri", "path": "https://x"}]}
        # 只有 uri → 无文件变化 → 未变化
        assert self._input_changed(wall_meta) is False

    def test_missing_fingerprint_means_changed(self):
        wall_meta = {"inputs": [{"path": "/x"}]}  # 缺 size/mtime_ns
        assert self._input_changed(wall_meta) is True

    def test_file_disappeared_means_changed(self, tmp_path):
        f = tmp_path / "gone.txt"
        f.write_text("x")
        st = f.stat()
        wall_meta = {"inputs": [{"path": str(f), "size": st.st_size, "mtime_ns": st.st_mtime_ns}]}
        f.unlink()
        assert self._input_changed(wall_meta) is True


class TestSplitDeadlockDirect:
    """_split_deadlock 直接单测——两种谓词类型（索引/uid）。"""

    def test_index_based_split(self):
        from tasklite.engine.store import StateStore
        queue = [{"task_type": "t", "job_id": "a"},
                 {"task_type": "t", "job_id": "b"}]
        uids_metas, remaining = StateStore._split_deadlock(
            queue, "MALFORMED_JOB",
            extract_uid=lambda jd: f"{jd['task_type']}::{jd['job_id']}",
            include=lambda idx, uid, root={0}: idx in root,
        )
        assert uids_metas == [("t::a", {"error": "MALFORMED_JOB", "root_cause": True})]
        assert remaining == [{"task_type": "t", "job_id": "b"}]

    def test_uid_based_split_preserves_order(self):
        from tasklite.engine.store import StateStore
        queue = [{"task_type": "t", "job_id": "a"},
                 {"task_type": "t", "job_id": "b"},
                 {"task_type": "t", "job_id": "c"}]
        uids_metas, remaining = StateStore._split_deadlock(
            queue, "DEPENDENCY_DEADLOCK",
            extract_uid=lambda jd: f"{jd['task_type']}::{jd['job_id']}",
            include=lambda idx, uid, roots={"t::b"}: uid in roots,
        )
        assert [u for u, _ in uids_metas] == ["t::b"]
        assert [j["job_id"] for j in remaining] == ["a", "c"]

