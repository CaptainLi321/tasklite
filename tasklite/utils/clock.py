"""UTC 时钟单点：引擎全部落盘时间戳的唯一生成出口。

所有持久化时间戳（started_at / finished_at /
last_succeeded_at / failed_at 等）统一经 ``utc_now_iso`` 生成——UTC 意识
时区 + ISO 8601 全格式。格式漂移会破坏跨 run 的时间比较与追溯链
排序，收敛为单点。

归置本模块而非 jsonutil：时钟生成是独立于 JSON 序列化的关注点，
序列化实现的替换不得连带时间戳语义；引擎层（dispatch/store/
errorclass）与模型层共用，utils 是两侧共同的下沉层。
"""
from __future__ import annotations

import datetime

_UTC_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def utc_now_iso() -> str:
    """当前时刻的 UTC ISO 8601 串（落盘时间戳唯一格式）。"""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def parse_utc_iso_to_us(iso_str: str) -> int:
    """将带时区的 UTC ISO 8601 字符串转换为自 Unix Epoch 起的整数微秒（无浮点精度损耗）。"""
    if not isinstance(iso_str, str):
        raise TypeError(f"iso_str must be a str, got {type(iso_str).__name__}")
    if not iso_str:
        raise ValueError("iso_str must be a non-empty UTC ISO 8601 string")
    normalized = f"{iso_str[:-1]}+00:00" if iso_str.endswith(("Z", "z")) else iso_str
    dt = datetime.datetime.fromisoformat(normalized)
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"Timestamp must be timezone-aware ISO 8601, got {iso_str!r}")
    dt_utc = dt.astimezone(datetime.timezone.utc)
    delta = dt_utc - _UTC_EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def mtime_ns_to_us(mtime_ns: int) -> int:
    """将 os.stat 的纳秒整数 mtime_ns 截断转换为整数微秒（消除亚微秒精度差）。"""
    if not isinstance(mtime_ns, int) or isinstance(mtime_ns, bool):
        raise TypeError(f"mtime_ns must be an int, got {type(mtime_ns).__name__}")
    return mtime_ns // 1_000


__all__ = [
    "mtime_ns_to_us",
    "parse_utc_iso_to_us",
    "utc_now_iso",
]
