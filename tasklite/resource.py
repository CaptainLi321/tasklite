"""tasklite.resource: 内置资源公开模块。

导出 CapacityResource 与 IntervalResource。
"""
from __future__ import annotations

from .engine.resource import CapacityResource, IntervalResource

__all__ = [
    "CapacityResource",
    "IntervalResource",
]
