"""Unit tests for ErrorTaxonomy deep module."""

from __future__ import annotations

import typing
from typing_extensions import TypedDict
import pytest

from tasklite.exceptions import FatalError, RateLimitHit, RetryError, TransientRegistry
from tasklite.taxonomy import (
    ERR_COMMIT_FAILURE_DLQ,
    ERR_DEPENDENCY_DEADLOCK,
    ERR_DISPATCH_FAILURE,
    ERR_JOB_DEPENDENCY,
    ERR_MAX_RETRIES,
    ERR_NO_HANDLER,
    ERR_PAYLOAD_VALIDATION,
    ERR_RESOURCE_DEADLOCK,
    ERROR_TYPE_COMMIT_FAILURE,
    ERROR_TYPE_DEADLOCK,
    ERROR_TYPE_DEPENDENCY,
    ERROR_TYPE_DISPATCH,
    ERROR_TYPE_FATAL,
    ERROR_TYPE_NO_HANDLER,
    ERROR_TYPE_TRANSIENT_EXHAUSTED,
    ERROR_TYPE_UNKNOWN,
    ERROR_TYPE_VALIDATION,
    ErrorCategory,
    ErrorClassification,
    ErrorTaxonomy,
    ValidationResult,
)


class TestErrorTaxonomyClassification:
    """测试统一多态错误分类契约。"""

    def test_classify_standard_exceptions(self):
        taxonomy = ErrorTaxonomy()

        # RetryError
        cl = taxonomy.classify(RetryError("temporary"))
        assert cl.is_retry is True
        assert cl.is_transient is True
        assert cl.is_fatal is False
        assert cl.category == ErrorCategory.TRANSIENT_EXHAUSTED
        assert cl.dlq_error_type == ERROR_TYPE_TRANSIENT_EXHAUSTED

        # RateLimitHit (RetryError subclass)
        cl_rate = taxonomy.classify(RateLimitHit("429"))
        assert cl_rate.is_retry is True
        assert cl_rate.is_transient is True

        # FatalError
        cl_fatal = taxonomy.classify(FatalError("bad config"))
        assert cl_fatal.is_fatal is True
        assert cl_fatal.is_transient is False
        assert cl_fatal.category == ErrorCategory.FATAL
        assert cl_fatal.dlq_error_type == ERROR_TYPE_FATAL

        # 内置启发式 Fatal (TypeError, KeyError, etc.)
        cl_type = taxonomy.classify(TypeError("bad type"))
        assert cl_type.is_fatal is True
        assert cl_type.category == ErrorCategory.FATAL

        # 内置启发式 Transient (ConnectionResetError, TimeoutError, etc.)
        cl_conn = taxonomy.classify(ConnectionResetError("peer reset"))
        assert cl_conn.is_transient is True
        assert cl_conn.is_fatal is False
        assert cl_conn.category == ErrorCategory.TRANSIENT_EXHAUSTED

        # KeyboardInterrupt
        cl_intr = taxonomy.classify(KeyboardInterrupt())
        assert cl_intr.is_interrupted is True
        assert cl_intr.category == ErrorCategory.INTERRUPTED

        # 未知异常 (ValueError, RuntimeError)
        cl_unk = taxonomy.classify(ValueError("unknown format"))
        assert cl_unk.is_fatal is False
        assert cl_unk.is_transient is False
        assert cl_unk.category == ErrorCategory.UNKNOWN

    def test_classify_user_transient_registry(self):
        class CustomNetworkError(Exception):
            pass

        taxonomy = ErrorTaxonomy(transient_registry=[CustomNetworkError])
        cl = taxonomy.classify(CustomNetworkError("custom drop"))
        assert cl.is_transient is True
        assert cl.is_retry is True
        assert cl.category == ErrorCategory.TRANSIENT_EXHAUSTED

    def test_classify_exitcodes(self):
        taxonomy = ErrorTaxonomy()

        # OOM (-9)
        cl_oom = taxonomy.classify(None, exitcode=-9)
        assert cl_oom.is_transient is True
        assert cl_oom.category == ErrorCategory.TRANSIENT_EXHAUSTED

        # SIGSEGV (-11)
        cl_segv = taxonomy.classify(None, exitcode=-11)
        assert cl_segv.is_transient is False
        assert cl_segv.category == ErrorCategory.FATAL

        # 超时被标记为瞬态
        cl_timeout = taxonomy.classify(None, exitcode=-9, timeout_is_transient=True)
        assert cl_timeout.is_transient is True

    def test_classify_meta_dictionaries_and_ipc(self):
        taxonomy = ErrorTaxonomy()

        # IPC retry 字典
        cl_retry = taxonomy.classify({"status": "retry", "error": "retry me", "lock_conflict": True})
        assert cl_retry.is_retry is True
        assert cl_retry.lock_conflict is True
        assert cl_retry.dlq_error_type == ERROR_TYPE_TRANSIENT_EXHAUSTED

        # IPC fatal 字典
        cl_fatal = taxonomy.classify({"status": "fatal", "error": "fatal crash", "traceback": "..."})
        assert cl_fatal.is_fatal is True
        assert cl_fatal.dlq_error_type == ERROR_TYPE_FATAL

        # DLQ meta 字典
        cl_dlq = taxonomy.classify({"error": ERR_DEPENDENCY_DEADLOCK})
        assert cl_dlq.category == ErrorCategory.DEADLOCK
        assert cl_dlq.dlq_error_type == ERROR_TYPE_DEADLOCK

        cl_no_h = taxonomy.classify({"error": ERR_NO_HANDLER})
        assert cl_no_h.category == ErrorCategory.NO_HANDLER
        assert cl_no_h.dlq_error_type == ERROR_TYPE_NO_HANDLER

    def test_classification_invariance_roundtrip(self):
        """不变式：异常 -> Classification -> DLQ Meta -> Classification 分类恒等。"""
        taxonomy = ErrorTaxonomy()
        exc = ConnectionResetError("network peer dropped")
        cl1 = taxonomy.classify(exc)

        meta = cl1.to_dlq_meta(attempt=2)
        cl2 = taxonomy.classify(meta)

        assert cl1.dlq_error_type == cl2.dlq_error_type == ERROR_TYPE_TRANSIENT_EXHAUSTED
        assert cl1.is_fatal == cl2.is_fatal is False
        assert meta["_attempt"] == 2

    def test_never_raise_contract(self):
        """Never-Raise 契约测试：传入任何脏数据均返回结构化对象，绝不上抛异常。"""
        taxonomy = ErrorTaxonomy()
        for garbage in [None, 42, "unknown string", [], object(), float("nan")]:
            cl = taxonomy.classify(garbage)
            assert isinstance(cl, ErrorClassification)
            norm = taxonomy.normalize_dlq_meta(garbage)
            assert isinstance(norm, dict)
            assert "error_type" in norm
            assert "failed_at" in norm


class TestPayloadValidation:
    """测试载荷结构化校验契约。"""

    class ExampleSchema(TypedDict):
        age: int
        name: str
        tag: typing.Optional[str]

    def test_valid_payload(self):
        taxonomy = ErrorTaxonomy()
        res = taxonomy.validate_payload({"age": 25, "name": "Alice", "tag": None}, self.ExampleSchema)
        assert res.is_valid is True
        assert len(res.errors) == 0

    def test_missing_and_unexpected_fields(self):
        taxonomy = ErrorTaxonomy()
        res = taxonomy.validate_payload({"age": 25, "extra": "extra_val"}, self.ExampleSchema)
        assert res.is_valid is False
        codes = {e.code for e in res.errors}
        assert "MISSING_REQUIRED_FIELD" in codes
        assert "UNEXPECTED_FIELD" in codes

    def test_bool_rejected_for_int(self):
        """不变式：bool 是 int 子类，但必须被显式拒绝以防类型错位。"""
        taxonomy = ErrorTaxonomy()
        res = taxonomy.validate_payload({"age": True, "name": "Bob"}, self.ExampleSchema)
        assert res.is_valid is False
        assert any(e.code == "BOOL_FORBIDDEN_FOR_INT" for e in res.errors)

    def test_non_dict_payload(self):
        taxonomy = ErrorTaxonomy()
        res = taxonomy.validate_payload("not a dict", self.ExampleSchema)
        assert res.is_valid is False
        assert res.errors[0].code == "INVALID_PAYLOAD_TYPE"

    def test_validation_result_as_error_strings_and_diagnostics(self):
        taxonomy = ErrorTaxonomy()
        res = taxonomy.validate_payload({"age": True}, self.ExampleSchema)
        assert isinstance(res.as_error_strings(), list)
        diag = res.to_diagnostic_meta()
        assert "details" in diag
        assert diag["error_count"] > 0


class TestResourceValidation:
    """测试资源 amount 数值校验。"""

    def test_valid_resources(self):
        taxonomy = ErrorTaxonomy()
        res = taxonomy.validate_resources({"cpu": 2, "memory": 1024.5})
        assert res.is_valid is True

    def test_invalid_resource_types_and_values(self):
        taxonomy = ErrorTaxonomy()

        # bool rejected
        res = taxonomy.validate_resources({"cpu": True})
        assert res.is_valid is False
        assert res.errors[0].code == "BOOL_FORBIDDEN"

        # negative rejected
        res = taxonomy.validate_resources({"cpu": -1})
        assert res.is_valid is False
        assert res.errors[0].code == "NEGATIVE_AMOUNT"

        # NaN / Inf rejected
        res = taxonomy.validate_resources({"cpu": float("nan")})
        assert res.is_valid is False
        assert res.errors[0].code == "NOT_FINITE"

        # Overflow (10**400)
        res = taxonomy.validate_resources({"cpu": 10**400})
        assert res.is_valid is False
        assert res.errors[0].code == "OVERFLOW"

    def test_ensure_resources_valid_exceptions(self):
        taxonomy = ErrorTaxonomy()
        with pytest.raises(TypeError, match="must be a number"):
            taxonomy.ensure_resources_valid({"cpu": True})
        with pytest.raises(ValueError, match="must be non-negative"):
            taxonomy.ensure_resources_valid({"cpu": -5})
        with pytest.raises(ValueError, match="is too large"):
            taxonomy.ensure_resources_valid({"cpu": 10**400})
