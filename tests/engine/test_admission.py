"""v2 准入与重入队策略深模块测试：trigger 矩阵、输入 mtime 与可信基线判据、节奏接缝。"""

from __future__ import annotations

import os
from types import SimpleNamespace

from tasklite.engine.admission import (
    ImmediateRequeuePolicy,
    RequeuePlan,
    RequeuePolicy,
    TriggerRegistry,
    extract_trusted_metadata_timestamp_us,
    is_trusted_success_metadata,
)


def _trusted_meta(
    inputs: list[str],
    *,
    success_count: object = 1,
    last_succeeded_at: str = "2026-05-01T00:00:00+00:00",
) -> dict:
    return {
        "success_count": success_count,
        "last_succeeded_at": last_succeeded_at,
        "last_success_run_id": "run_seed",
        "inputs": inputs,
    }


class TestTrustedMetadataFilter:
    def test_seed_empty_and_incomplete_metadata_untrusted(self):
        assert extract_trusted_metadata_timestamp_us(None) is None
        assert extract_trusted_metadata_timestamp_us({}) is None
        assert not is_trusted_success_metadata({"last_succeeded_at": "2026-05-01T00:00:00+00:00"})
        assert not is_trusted_success_metadata(_trusted_meta([], success_count=0))
        assert not is_trusted_success_metadata(_trusted_meta([], success_count=True))
        assert not is_trusted_success_metadata(_trusted_meta([], last_succeeded_at="garbage"))
        assert not is_trusted_success_metadata(_trusted_meta([], last_succeeded_at="2026-05-01T00:00:00"))
        bad_run = _trusted_meta([])
        bad_run["last_success_run_id"] = ""
        assert not is_trusted_success_metadata(bad_run)

    def test_complete_framework_metadata_trusted_in_microseconds(self):
        meta = _trusted_meta([], last_succeeded_at="1970-01-01T00:00:01.000002+00:00")
        assert extract_trusted_metadata_timestamp_us(meta) == 1_000_002


class TestImmediateRequeuePolicy:
    def test_default_plan_is_immediate_front_insert(self):
        policy = ImmediateRequeuePolicy()
        plan = policy.plan_requeue({"task_type": "t", "job_id": "j"})
        assert isinstance(plan, RequeuePlan)
        assert plan.front is True
        assert plan.delay_seconds == 0.0


    def test_seam_accepts_alternate_strategy(self):
        """接缝多态：自定义节奏策略可替换默认实现，核心不改。"""

        class TailingRequeuePolicy(RequeuePolicy):
            """尾插节奏桩：验证接缝只约定形状、不绑定默认实现。"""

            def plan_requeue(self, job_snapshot) -> RequeuePlan:
                return RequeuePlan(front=False, delay_seconds=2.5)

        policy: RequeuePolicy = TailingRequeuePolicy()
        plan = policy.plan_requeue({"task_type": "t", "job_id": "j"})
        assert plan.front is False
        assert plan.delay_seconds == 2.5
