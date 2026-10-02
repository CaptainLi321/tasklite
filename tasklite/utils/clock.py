"""UTC 时钟单点：引擎全部落盘时间戳的唯一生成出口。

所有持久化时间戳（first_enqueued_at / started_at / finished_at /
last_run_at / failed_at 等）统一经 ``utc_now_iso`` 生成——UTC 意识
时区 + ISO 8601 全格式。格式漂移会破坏跨 run 的时间比较与追溯链
排序，收敛为单点。

归置本模块而非 jsonutil：时钟生成是独立于 JSON 序列化的关注点，
序列化实现的替换不得连带时间戳语义；引擎层（dispatch/store/
errorclass）与模型层共用，utils 是两侧共同的下沉层。
"""
from __future__ import annotations

import datetime


def utc_now_iso() -> str:
    """当前时刻的 UTC ISO 8601 串（落盘时间戳唯一格式）。"""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


__all__ = [
    "utc_now_iso",
]
