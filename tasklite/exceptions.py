"""v2 异常体系：三分类异常与框架内部崩溃信号。

分类契约（与错误分类模块的决策表同源）：
- ``FatalError``：非瞬态缺陷——代码级 bug（非法配置、永不出现的缺失文件、
  逻辑错误），不消耗重试预算，直接进入失败档案（failed）；
- ``RetryError``：瞬态失败——handler 内抛出即把 job 推回队列重试；
- ``RateLimitHit``：HTTP 429 信号——``RetryError`` 子类，裸抛即按瞬态
  重试，且遵守瞬态信号军规（不烧重试预算 + 降级写盘 + 零污染）。
"""
from __future__ import annotations


class PipelineError(Exception):
    """v2 框架异常的公共基类。"""

    pass


class RetryError(PipelineError):
    """瞬态失败信号：handler 抛出后 job 被推回队列重试。"""

    pass


class FatalError(PipelineError):
    """非瞬态错误：不消耗重试预算，直接进入失败档案（failed）。

    适用于代码级 bug——非法配置、永不出现的缺失文件、逻辑错误。
    """

    pass


class RateLimitHit(RetryError):
    """HTTP 429 限流信号。

    继承 ``RetryError``：裸抛即按瞬态重试处理，不烧重试预算。
    """

    pass


class _CommitCrashSignal(BaseException):
    """内部信号：commit 失败后已重入队，需立即崩溃。

    不变式：继承 ``BaseException``（而非 ``Exception``）——防止用户
    handler 的 ``except Exception`` 误吞崩溃信号。
    """

    pass


class _JobTerminated(BaseException):
    """内部信号：当前 job 已终结（失败档案阈值命中），需立即停止处理。"""

    pass


__all__ = [
    "PipelineError",
    "RetryError",
    "FatalError",
    "RateLimitHit",
    "_CommitCrashSignal",
    "_JobTerminated",
]
