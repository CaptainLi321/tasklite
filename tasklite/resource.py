"""tasklite.resource: 内置资源公开模块。

导出 CapacityResource 与 IntervalResource 配置规格。
内部调度协议（can_acquire/acquire/release 与通知机制）由引擎内部管理，
不作为公开接口暴露。
"""
from __future__ import annotations

import math

from .exceptions import TaskLiteError


def _validate_resource_name(name: str) -> str:
    if not isinstance(name, str) or not name:
        raise TaskLiteError(f"Resource name must be a non-empty string, got {name!r}")
    return name


def _validate_interval_seconds(interval_seconds: float) -> float:
    if not isinstance(interval_seconds, (int, float)) or isinstance(interval_seconds, bool):
        raise TaskLiteError(
            f"interval_seconds must be a number, got {type(interval_seconds).__name__}"
        )
    if not math.isfinite(interval_seconds) or interval_seconds <= 0:
        raise TaskLiteError(
            f"interval_seconds must be finite and > 0, got {interval_seconds!r}"
        )
    return float(interval_seconds)


def _validate_max_capacity(max_capacity: float) -> float:
    if not isinstance(max_capacity, (int, float)) or isinstance(max_capacity, bool):
        raise TaskLiteError(
            f"max_capacity must be a number, got {type(max_capacity).__name__} ({max_capacity!r})"
        )
    if not math.isfinite(max_capacity) or max_capacity < 0:
        raise TaskLiteError(f"max_capacity must be finite and >= 0, got {max_capacity!r}")
    return float(max_capacity)


class IntervalResource:
    """间隔限流资源规格：每次获准后，下次获取至少等待 ``interval_seconds * amount``。"""

    def __init__(self, name: str, *, interval_seconds: float) -> None:
        self._name = _validate_resource_name(name)
        self._interval_seconds = _validate_interval_seconds(interval_seconds)

    @property
    def name(self) -> str:
        return self._name

    @property
    def interval_seconds(self) -> float:
        return self._interval_seconds

    def __repr__(self) -> str:
        return (
            f"IntervalResource(name={self._name!r}, "
            f"interval_seconds={self._interval_seconds})"
        )


class CapacityResource:
    """容量并发资源规格：控制最大同时占用上限（如并发数或显存配额）。"""

    def __init__(self, name: str, *, max_capacity: float) -> None:
        self._name = _validate_resource_name(name)
        self._max_capacity = _validate_max_capacity(max_capacity)

    @property
    def name(self) -> str:
        return self._name

    @property
    def max_capacity(self) -> float:
        return self._max_capacity

    def __repr__(self) -> str:
        return (
            f"CapacityResource(name={self._name!r}, "
            f"max_capacity={self._max_capacity})"
        )


__all__ = [
    "CapacityResource",
    "IntervalResource",
]
