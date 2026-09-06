"""重试与退避向后兼容适配器（已收敛至 PreflightPolicy 深模块）。

推荐直接使用 ``tasklite.engine.policy.PreflightPolicy``。
"""

import logging
import math
import os
import random
from typing import Optional

from .policy import (
    BackoffSchedule,
    DecisionReason,
    PreflightAction,
    PreflightDecision,
    PreflightPolicy,
)

logger = logging.getLogger("tasklite")
_DEFAULT_POLICY = PreflightPolicy()


def compute_backoff(
    retries: int,
    backoff_base: float = 2.0,
    backoff_max: float = 300.0,
) -> float:
    """Compute exponential backoff delay with ±25% jitter."""
    if retries < 0:
        logger.warning(f"compute_backoff called with negative retries={retries}, treating as 0")
        return 0.0
    if retries == 0:
        return 0.0
    if not math.isfinite(backoff_base) or backoff_base < 0:
        backoff_base = 2.0
    if not math.isfinite(backoff_max) or backoff_max < 0:
        backoff_max = 300.0
    exp = min(retries - 1, 60)
    base_delay = backoff_base * (2 ** exp)
    delay = min(base_delay, backoff_max)
    jitter = random.uniform(-0.25, 0.25) * delay
    return max(0.0, delay + jitter)


def input_changed(wall_meta: dict) -> bool:
    """比对 wall 输入指纹与磁盘文件。"""
    return _DEFAULT_POLICY.check_input_changed(wall_meta)


def rerun_skips(
    job_dict: dict,
    *,
    wall_hit: bool,
    failed_hit: bool,
    wall_meta: Optional[dict] = None,
) -> bool:
    """rerun 策略决定 wall/failed 命中是否拦截（True=跳过，False=放行重跑）。"""
    decision = _DEFAULT_POLICY.evaluate(
        job_dict,
        wall_meta=wall_meta,
        is_wall=wall_hit,
        is_failed=failed_hit,
    )
    return decision.should_skip


def apply_discovery_rerun(
    job_dict: dict,
    task_type: str,
    discovery_rerun: dict,
) -> bool:
    """discovery 默认 rerun 注入单点。"""
    policy = PreflightPolicy(discovery_rerun)
    return policy.normalize_job_dict(job_dict, task_type)


__all__ = [
    "PreflightAction",
    "DecisionReason",
    "PreflightDecision",
    "BackoffSchedule",
    "PreflightPolicy",
    "compute_backoff",
    "input_changed",
    "rerun_skips",
    "apply_discovery_rerun",
]
