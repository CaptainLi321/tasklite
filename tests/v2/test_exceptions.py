"""v2 异常体系契约测试：继承层级与 str 透传。

锁定契约：
- 三分类公开异常以 ``PipelineError`` 为唯一基类（调用方 ``except
  PipelineError`` 一网打尽框架异常）；
- ``RateLimitHit`` 是 ``RetryError`` 子类（裸抛即瞬态重试，不烧预算）；
- 内部崩溃信号继承 ``BaseException``，用户 handler 的 ``except
  Exception`` 不得误吞（锁生命周期与崩溃语义依赖此不变式）；
- 异常消息经 ``str()`` 原样透传（失败档案落盘串即用户消息）。
"""

from __future__ import annotations

import pytest

from tasklite.v2.exceptions import (
    _CommitCrashSignal,
    _JobTerminated,
    FatalError,
    PipelineError,
    RateLimitHit,
    RetryError,
)


class TestExceptionHierarchy:
    """继承层级契约。"""

    def test_fatal_and_retry_share_pipeline_base(self):
        """FatalError / RetryError 都是 PipelineError 子类。"""
        assert issubclass(FatalError, PipelineError)
        assert issubclass(RetryError, PipelineError)

    def test_rate_limit_hit_is_retry_subclass(self):
        """RateLimitHit 是 RetryError 子类：裸抛即按瞬态重试。"""
        assert issubclass(RateLimitHit, RetryError)
        assert issubclass(RateLimitHit, PipelineError)

    def test_fatal_is_not_retry(self):
        """FatalError 与 RetryError 互不继承（分类语义不得混淆）。"""
        assert not issubclass(FatalError, RetryError)
        assert not issubclass(RetryError, FatalError)


class TestInternalSignalsEscapeExceptException:
    """内部崩溃信号必须逃逸 ``except Exception``。"""

    @pytest.mark.parametrize(
        "signal_cls", [_CommitCrashSignal, _JobTerminated], ids=lambda c: c.__name__
    )
    def test_signals_inherit_base_exception(self, signal_cls):
        """内部信号继承 BaseException 而非 Exception——用户 handler 的
        ``except Exception`` 不得误吞框架崩溃信号。"""
        assert issubclass(signal_cls, BaseException)
        assert not issubclass(signal_cls, Exception)

    @pytest.mark.parametrize(
        "signal_cls", [_CommitCrashSignal, _JobTerminated], ids=lambda c: c.__name__
    )
    def test_signals_not_swallowed_by_except_exception(self, signal_cls):
        """except Exception 守卫下裸抛内部信号必须向外传播。"""

        def guarded_raise() -> None:
            try:
                raise signal_cls("framework internal signal")
            except Exception:  # noqa: SIM105 —— 契约本身：不得捕获
                pytest.fail("内部信号被 except Exception 误吞")

        with pytest.raises(signal_cls):
            guarded_raise()


class TestStrContract:
    """str(e) 消息透传契约（失败档案 error 串即用户消息）。"""

    @pytest.mark.parametrize(
        "exc_cls",
        [PipelineError, RetryError, FatalError, RateLimitHit],
        ids=lambda c: c.__name__,
    )
    def test_str_passes_message_through(self, exc_cls):
        msg = "boom: 网络抖动"
        assert str(exc_cls(msg)) == msg

    @pytest.mark.parametrize(
        "exc_cls",
        [PipelineError, RetryError, FatalError, RateLimitHit],
        ids=lambda c: c.__name__,
    )
    def test_raise_and_catch_roundtrip(self, exc_cls):
        """raise → except 同类捕获且消息不漂移。"""
        with pytest.raises(exc_cls, match="disk full") as exc_info:
            raise exc_cls("disk full")
        assert str(exc_info.value) == "disk full"

    def test_catch_via_pipeline_error_base(self):
        """任意三分类异常可经 PipelineError 基类统一捕获。"""
        with pytest.raises(PipelineError):
            raise RateLimitHit("429 too many requests")
