"""瞬态信号军规回归测试（transient_kind：interrupted / lock_conflict / rate_limited
「不烧重试预算 + 零污染」）。

不变式（红线 8）：瞬态信号经 ExecutionResult.transient_kind 单值识别——
- 不消耗重试预算（job.retries 不递增）；
- 即使已达 max_retries 也豁免 DLQ；
- 短退避 [0.75, 1.0]s 回队自恢复（限流的实际等待由资源挂起 TTL 承担）；
- 不污染 last_retry_error；
- 无 kind 时不误判（默认分支）；
- TRANSIENT_KIND_STAT_KEYS 注册表每种 kind 全部通过军规四性（新增 kind
  漏登记即红——变异锁定）。
"""

import pytest

from tasklite.engine.channel import ExecutionResult
from tasklite.engine.policy import ExecutionPolicy
from tasklite.engine.types import TRANSIENT_KIND_STAT_KEYS, EMPTY_STATS
from tasklite.models.job import Job


def _job(max_retries: int = 3, retries: int = 0) -> Job:
    return Job("t", "1", max_retries=max_retries, retries=retries)


class TestTransientSignalViaResultObject:
    def test_interrupted_result_preserves_budget_and_clean_runtime(self):
        """interrupted 结果对象：预算不烧、豁免 DLQ、错误不落 last_retry_error。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=1)
        res = ExecutionResult(retry_error="signal hit", transient_kind="interrupted")
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True, "瞬态信号必须豁免 DLQ 继续重试"
        assert plan.transient_kind == "interrupted"
        assert job.retries == 1, "瞬态重试不得消耗重试预算"
        assert 0.75 <= plan.delay <= 1.0, "瞬态重试必须走短退避"
        rt = plan.retry_dict["runtime"]
        assert not rt.get("_last_retry_error"), \
            f"瞬态信号不得污染 last_retry_error: {rt.get('_last_retry_error')!r}"

    def test_lock_conflict_result_preserves_budget(self):
        """lock_conflict 结果对象：同样不烧预算、短退避。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=2)
        res = ExecutionResult(transient_kind="lock_conflict")
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True
        assert plan.transient_kind == "lock_conflict"
        assert job.retries == 2, "锁冲突重试不得消耗重试预算"
        assert 0.75 <= plan.delay <= 1.0

    def test_rate_limited_result_preserves_budget_and_clean_runtime(self):
        """rate_limited 结果对象：不烧预算、豁免 DLQ、零污染、短退避。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=2)
        res = ExecutionResult(retry_error="HTTP 429 RateLimit hit", transient_kind="rate_limited")
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True, "限流瞬态信号必须豁免 DLQ 继续重试"
        assert plan.transient_kind == "rate_limited"
        assert job.retries == 2, "限流重试不得消耗重试预算"
        assert 0.75 <= plan.delay <= 1.0, "限流重试必须走短退避（等待由资源挂起承担）"
        rt = plan.retry_dict["runtime"]
        assert not rt.get("_last_retry_error"), \
            f"限流信号不得污染 last_retry_error: {rt.get('_last_retry_error')!r}"

    def test_rate_limited_exhausted_budget_still_exempt(self):
        """retries 已达上限时限流结果对象仍豁免 DLQ（429 风暴不误终结）。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=3)
        res = ExecutionResult(retry_error="HTTP 429 RateLimit hit", transient_kind="rate_limited")
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True, "限流达预算上限也必须豁免 DLQ（持续 429 应挂起等待）"
        assert plan.fail_meta is None

    def test_rate_limited_keyword_form_preserves_budget(self):
        """plan_retry 显式 transient_kind 关键字：与结果对象属性同语义。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=3)
        plan = policy.plan_retry(
            job, job.to_dict(), retry_error="HTTP 429 RateLimit hit",
            transient_kind="rate_limited",
        )
        assert plan.going_to_retry is True
        assert plan.transient_kind == "rate_limited"
        assert job.retries == 3, "关键字形态的限流豁免同样不得消耗预算"

    def test_exhausted_budget_still_exempt_via_result_object(self):
        """retries 已达上限时，瞬态结果对象仍豁免 DLQ（识别链贯通）。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=3)
        res = ExecutionResult(transient_kind="interrupted")
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True, "瞬态信号达上限也必须豁免 DLQ"
        assert plan.fail_meta is None

    def test_plain_result_without_transient_flags_is_normal_retry(self):
        """无瞬态 kind 的结果对象必须走正常重试：烧预算 + 记录错误（默认分支）。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=1)
        res = ExecutionResult(retry_error="real failure")
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True
        assert plan.transient_kind is None
        assert job.retries == 2, "普通失败重试必须消耗重试预算"
        assert plan.retry_dict["runtime"]["_last_retry_error"] == "real failure"


class TestTransientKindRegistry:
    """注册表完备性：登记表内每种 kind 军规四性全过 + 统计键均属公开面。"""

    @pytest.mark.parametrize("kind", sorted(TRANSIENT_KIND_STAT_KEYS))
    def test_every_registered_kind_passes_the_warranty(self, kind):
        policy = ExecutionPolicy()
        job = _job(max_retries=2, retries=2)
        res = ExecutionResult(retry_error="transient hit", transient_kind=kind)
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True, f"{kind} 必须豁免 DLQ"
        assert plan.transient_kind == kind
        assert job.retries == 2, f"{kind} 不得烧预算"
        assert 0.75 <= plan.delay <= 1.0, f"{kind} 必须短退避"
        assert not plan.retry_dict["runtime"].get("_last_retry_error"), \
            f"{kind} 不得污染 last_retry_error"

    @pytest.mark.parametrize("stat_key", sorted(TRANSIENT_KIND_STAT_KEYS.values()))
    def test_stat_keys_are_public_api(self, stat_key):
        """kind→统计键映射不得指向公开 EMPTY_STATS 九键之外的键
        （API_GUIDE 逐键文档化，改键名即破坏监控面）。"""
        assert stat_key in EMPTY_STATS

    def test_known_kinds_are_all_registered(self):
        """wire 产生端全部 kind 均已登记（decode 接受集合 == 注册表键集）。"""
        assert set(TRANSIENT_KIND_STAT_KEYS) == {
            "interrupted", "lock_conflict", "rate_limited",
        }


class TestExhaustedBudgetTerminalShape:
    def test_exhausted_budget_dlq_plan_has_zero_delay(self):
        """预算耗尽的终局计划：零延迟 + DLQ 元数据（瞬态豁免的对照组）。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=2, retries=2)
        plan = policy.plan_retry(job, job.to_dict(), retry_error="final")
        assert plan.going_to_retry is False
        assert plan.delay == 0.0, "终局计划不得携带重试延迟"
        assert plan.fail_meta["error"] == "MAX_RETRIES_EXCEEDED"
        assert plan.retry_dict is None
