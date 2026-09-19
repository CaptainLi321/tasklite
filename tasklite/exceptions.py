"""Exception types for tasklite."""

from __future__ import annotations


class PipelineError(Exception):
    """Base class for all tasklite framework exceptions."""
    pass


class RetryError(PipelineError):
    """Raise this in a handler to signal a transient failure. The job will be pushed back to the queue."""
    pass


class FatalError(PipelineError):
    """Non-retryable error. Goes directly to DLQ without consuming retries.

    Raise this for code-level bugs (invalid config, missing files that will
    never appear, logic errors).
    """
    pass


class RateLimitHit(RetryError):
    """HTTP 429 信号。

    RateLimitHit 是 RetryError 子类：裸抛即按瞬态退避重试。
    """
    pass


class _CommitCrashSignal(BaseException):
    """内部信号：commit 失败后已 requeue，需立即崩溃。

    继承 BaseException（而非 Exception）防止用户 handler 的 except Exception 误吞崩溃信号。
    """
    pass


class _JobTerminated(BaseException):
    """内部信号：当前 job 已终结（DLQ 阈值命中），需立即停止处理。"""
    pass


__all__ = [
    "PipelineError",
    "RetryError",
    "FatalError",
    "RateLimitHit",
    "_CommitCrashSignal",
    "_JobTerminated",
]

