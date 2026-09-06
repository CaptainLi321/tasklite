"""Exception types for tasklite."""

from typing import Optional, Sequence, Tuple, Type


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


from .taxonomy import (
    FATAL_EXCEPTIONS,
    TRANSIENT_EXCEPTIONS,
    ErrorTaxonomy,
    _DEFAULT_TAXONOMY,
)


def _validate_transient_class(exception_cls: type) -> None:
    """注册入口校验（fail-loud）——per-pipeline 注册表与外部直调共用。"""
    if not isinstance(exception_cls, type) or not issubclass(exception_cls, Exception):
        raise TypeError(
            f"register_transient_exception requires an Exception subclass, got {exception_cls!r}"
        )
    if issubclass(exception_cls, (RetryError, FatalError)):
        raise TypeError(
            f"register_transient_exception cannot register a "
            f"{'RetryError' if issubclass(exception_cls, RetryError) else 'FatalError'} "
            f"subclass ({exception_cls!r}) — these have dedicated except branches "
            f"that bypass the registry; registration would silently no-op."
        )
    try:
        import pickle
        pickle.dumps(exception_cls)
    except (pickle.PicklingError, AttributeError, TypeError) as e:
        raise TypeError(
            f"register_transient_exception requires a module-level (picklable) "
            f"Exception class for spawn-subprocess propagation, got {exception_cls!r}: {e}"
        ) from e


class TransientRegistry:
    """per-pipeline 瞬态异常注册表。"""

    def __init__(self) -> None:
        self._classes: list = []

    def register(self, exception_cls: type) -> None:
        """把业务自有异常类注册为瞬态（自动重试），幂等。"""
        _validate_transient_class(exception_cls)
        if exception_cls not in self._classes:
            self._classes.append(exception_cls)

    def snapshot(self) -> tuple:
        """返回不可变注册表快照（随 ctx 显式下发子进程）。"""
        return tuple(self._classes)

    def matches(self, exc: BaseException) -> bool:
        """exc 是否命中本注册表。"""
        return any(isinstance(exc, cls) for cls in self._classes)


def _matches_registry(exc: BaseException, registry) -> bool:
    """纯判定：exc 是否命中给定注册表快照。"""
    classes = registry.snapshot() if isinstance(registry, TransientRegistry) else tuple(registry or ())
    return any(isinstance(exc, cls) for cls in classes)


def classify_exception(
    exc: BaseException,
    registry=(),
    *,
    fatal_exceptions: Optional[tuple] = None,
    transient_exceptions: Optional[tuple] = None,
) -> str:
    """异常三分类的生产语义，返回 retry/fatal/error。"""
    classes = registry.snapshot() if isinstance(registry, TransientRegistry) else tuple(registry or ())
    taxonomy = ErrorTaxonomy(
        fatal_exceptions=fatal_exceptions,
        transient_exceptions=transient_exceptions,
        transient_registry=classes,
    )
    cl = taxonomy.classify(exc)
    if cl.is_retry or cl.is_transient:
        return "retry"
    if cl.is_fatal:
        return "fatal"
    return "error"


def is_transient_exception(exc: BaseException, registry=()) -> bool:
    """判断异常是否属于瞬态（应自动重试）。"""
    return classify_exception(exc, registry) == "retry"


class _CommitCrashSignal(BaseException):
    """内部信号：commit 失败后已 requeue，需立即崩溃。

    继承 BaseException（而非 Exception）防止用户 handler 的 except Exception 误吞崩溃信号。
    """
    pass


class _JobTerminated(BaseException):
    """内部信号：当前 job 已终结（DLQ 阈值命中），需立即停止处理。"""
    pass
