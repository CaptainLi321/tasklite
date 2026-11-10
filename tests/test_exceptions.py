"""v2 异常体系契约测试：公开 TaskLiteError、内部崩溃信号与已移除旧异常面验证。"""

from __future__ import annotations

import pytest

import tasklite
import tasklite.exceptions as exc_mod
from tasklite.exceptions import (
    TaskLiteError,
    _ArtifactIOError,
)


class TestPublicExceptionSurface:
    """公开异常仅保留 TaskLiteError，旧异常与错误分类已从公开面移除。"""

    def test_tasklite_error_is_exception_subclass(self):
        assert issubclass(TaskLiteError, Exception)
        assert TaskLiteError is tasklite.TaskLiteError

    @pytest.mark.parametrize(
        "removed_name",
        [
            "PipelineError",
            "RetryError",
            "FatalError",
            "RateLimitHit",
            "ErrorClassifier",
            "ErrorCategory",
            "ErrorClassification",
            "ERR_MAX_RETRIES",
            "ERR_NO_HANDLER",
            "FATAL_EXCEPTIONS",
            "TRANSIENT_EXCEPTIONS",
        ],
    )
    def test_removed_symbols_not_in_public_surface(self, removed_name: str):
        assert not hasattr(tasklite, removed_name)
        assert removed_name not in tasklite.__all__
        assert not hasattr(exc_mod, removed_name)


class TestInternalSignalsEscapeExceptException:
    """内部崩溃信号必须逃逸 ``except Exception``。"""

    @pytest.mark.parametrize(
        "signal_cls",
        [_ArtifactIOError],
        ids=lambda c: c.__name__,
    )
    def test_signals_inherit_base_exception(self, signal_cls):
        """内部信号继承 BaseException 而非 Exception——用户 handler 的
        ``except Exception`` 不得误吞框架崩溃信号。"""
        assert issubclass(signal_cls, BaseException)
        assert not issubclass(signal_cls, Exception)

    @pytest.mark.parametrize(
        "signal_cls",
        [_ArtifactIOError],
        ids=lambda c: c.__name__,
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
    """str(e) 消息透传契约。"""

    def test_str_passes_message_through(self):
        msg = "boom: 非法契约参数"
        assert str(TaskLiteError(msg)) == msg

    def test_raise_and_catch_roundtrip(self):
        with pytest.raises(TaskLiteError, match="invalid uid") as exc_info:
            raise TaskLiteError("invalid uid")
        assert str(exc_info.value) == "invalid uid"
