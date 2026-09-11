"""瞬态信号军规回归测试（interrupted / lock_conflict「不烧重试预算 + 零污染」）。

不变式（红线 8）：外部中断与孤儿锁冲突为瞬态信号——
- 经 ExecutionResult 结果对象属性识别（生产路径唯一形态）；
- 不消耗重试预算（job.retries 不递增）；
- 即使已达 max_retries 也豁免 DLQ；
- 短退避 [0.75, 1.0]s 回队自恢复；
- 不污染 last_retry_error；
- 无瞬态属性时不误判（默认 False 分支）。
"""

from tasklite.engine.channel import ExecutionResult
from tasklite.engine.policy import ExecutionPolicy
from tasklite.models.job import Job


def _job(max_retries: int = 3, retries: int = 0) -> Job:
    return Job("t", "1", max_retries=max_retries, retries=retries)


class TestTransientSignalViaResultObject:
    def test_interrupted_result_preserves_budget_and_clean_runtime(self):
        """interrupted 结果对象：预算不烧、豁免 DLQ、错误不落 last_retry_error。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=1)
        res = ExecutionResult(retry_error="signal hit", interrupted=True)
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True, "瞬态信号必须豁免 DLQ 继续重试"
        assert plan.is_interrupted is True
        assert job.retries == 1, "瞬态重试不得消耗重试预算"
        assert 0.75 <= plan.delay <= 1.0, "瞬态重试必须走短退避"
        rt = plan.retry_dict["runtime"]
        assert not rt.get("_last_retry_error"), \
            f"瞬态信号不得污染 last_retry_error: {rt.get('_last_retry_error')!r}"

    def test_lock_conflict_result_preserves_budget(self):
        """lock_conflict 结果对象：同样不烧预算、短退避。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=2)
        res = ExecutionResult(lock_conflict=True)
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True
        assert plan.is_lock_conflict is True
        assert job.retries == 2, "锁冲突重试不得消耗重试预算"
        assert 0.75 <= plan.delay <= 1.0

    def test_exhausted_budget_still_exempt_via_result_object(self):
        """retries 已达上限时，瞬态结果对象仍豁免 DLQ（识别链贯通）。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=3)
        res = ExecutionResult(interrupted=True)
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True, "瞬态信号达上限也必须豁免 DLQ"
        assert plan.fail_meta is None

    def test_plain_result_without_transient_flags_is_normal_retry(self):
        """无瞬态标志的结果对象必须走正常重试：烧预算 + 记录错误（默认分支）。"""
        policy = ExecutionPolicy()
        job = _job(max_retries=3, retries=1)
        res = ExecutionResult(retry_error="real failure")
        plan = policy.plan_retry(job, job.to_dict(), res)
        assert plan.going_to_retry is True
        assert plan.is_interrupted is False
        assert plan.is_lock_conflict is False
        assert job.retries == 2, "普通失败重试必须消耗重试预算"
        assert plan.retry_dict["runtime"]["_last_retry_error"] == "real failure"


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
