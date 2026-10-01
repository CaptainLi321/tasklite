"""v2 错误分类模块契约测试：分类 / 死亡归因 / 载荷校验 / 失败档案元数据规范化。

保留 v1 分类决策语义并按 v2 API 改写：分类器更名 ErrorClassifier、归因
入口更名 classify_process_death、失败档案元数据方法更名 to_failed_meta /
normalize_failed_meta；资源数额校验单点在 models/task.py，此处不再覆盖。
注意：本文件不启用 ``from __future__ import annotations``——字符串化注解
会让 get_type_hints 丢失嵌套局部 TypedDict 的定义域。
"""
import typing
from typing import Literal, TypedDict

import pytest

from tasklite.v2.exceptions import FatalError, RateLimitHit, RetryError
from tasklite.v2.engine.errorclass import (
    ERR_COMMIT_FAILURE,
    ERR_DEADLOCK_GAP,
    ERR_DEPENDENCY_DEADLOCK,
    ERR_DISPATCH_FAILURE,
    ERR_JOB_DEPENDENCY,
    ERR_MALFORMED_JOB,
    ERR_MAX_RETRIES,
    ERR_NO_HANDLER,
    ERR_NO_IPC_RESULT,
    ERR_PAYLOAD_VALIDATION,
    ERR_PROCESS_CRASH_PREFIX,
    ERR_PROCESS_SIGNAL_DEATH,
    ERR_RESOURCE_DEADLOCK,
    ERR_TIMEOUT_PREFIX,
    DeathAttribution,
    ErrorCategory,
    ErrorClassification,
    ErrorClassifier,
    ValidationResult,
    classify_error_type,
    classify_exception,
    is_transient_exception,
    validate_payload_errors,
)


class _CustomNetworkError(Exception):
    """模块级瞬态注册样本类（构造/注册路径均要求可 pickle 的模块级类）。"""


class _BrokenStrError(Exception):
    """``__str__`` 必然抛异常的异常实例样本。"""

    def __str__(self):
        raise RuntimeError("broken __str__")


class _BrokenStrObject:
    """``__str__`` 必然抛异常的普通对象样本。"""

    def __str__(self):
        raise RuntimeError("broken __str__")


class _BrokenStrSchema:
    """``__str__`` 必然抛异常的 schema 对象样本。"""

    def __str__(self):
        raise RuntimeError("broken __str__")


class TestClassifierStandardExceptionShapes:
    """统一多态错误分类契约（异常实例路径）。"""

    def test_classify_standard_exceptions(self):
        classifier = ErrorClassifier()

        # RetryError
        cl = classifier.classify(RetryError("temporary"))
        assert cl.is_retry is True
        assert cl.is_transient is True
        assert cl.is_fatal is False
        assert cl.category == ErrorCategory.TRANSIENT_EXHAUSTED
        assert cl.failed_error_type == ErrorCategory.TRANSIENT_EXHAUSTED.value

        # RateLimitHit（RetryError 子类）
        cl_rate = classifier.classify(RateLimitHit("429"))
        assert cl_rate.is_retry is True
        assert cl_rate.is_transient is True

        # FatalError
        cl_fatal = classifier.classify(FatalError("bad config"))
        assert cl_fatal.is_fatal is True
        assert cl_fatal.is_transient is False
        assert cl_fatal.category == ErrorCategory.FATAL
        assert cl_fatal.failed_error_type == ErrorCategory.FATAL.value

        # 内置启发式 Fatal（TypeError, KeyError 等）
        cl_type = classifier.classify(TypeError("bad type"))
        assert cl_type.is_fatal is True
        assert cl_type.category == ErrorCategory.FATAL

        # 内置启发式 Transient（ConnectionResetError, TimeoutError 等）
        cl_conn = classifier.classify(ConnectionResetError("peer reset"))
        assert cl_conn.is_transient is True
        assert cl_conn.is_fatal is False
        assert cl_conn.category == ErrorCategory.TRANSIENT_EXHAUSTED

        # KeyboardInterrupt
        cl_intr = classifier.classify(KeyboardInterrupt())
        assert cl_intr.is_interrupted is True
        assert cl_intr.category == ErrorCategory.INTERRUPTED

        # 未知异常（ValueError, RuntimeError）
        cl_unk = classifier.classify(ValueError("unknown format"))
        assert cl_unk.is_fatal is False
        assert cl_unk.is_transient is False
        assert cl_unk.category == ErrorCategory.UNKNOWN

    def test_classify_user_transient_registry(self):
        classifier = ErrorClassifier(transient_registry=[_CustomNetworkError])
        cl = classifier.classify(_CustomNetworkError("custom drop"))
        assert cl.is_transient is True
        assert cl.is_retry is True
        assert cl.category == ErrorCategory.TRANSIENT_EXHAUSTED

    def test_classify_string_error_codes(self):
        classifier = ErrorClassifier()
        cl = classifier.classify(ERR_JOB_DEPENDENCY)
        assert cl.category == ErrorCategory.DEPENDENCY
        assert cl.error_code == ERR_JOB_DEPENDENCY
        cl_unk = classifier.classify("some handler message")
        assert cl_unk.category == ErrorCategory.UNKNOWN

    def test_classify_no_exitcode_parameter(self):
        """进程死亡归因不经 classify——收割侧统一走 classify_process_death
        决策表（TestDeathAttributionTable 锁定全组合）。"""
        classifier = ErrorClassifier()
        with pytest.raises(TypeError):
            classifier.classify(None, exitcode=-9)  # type: ignore[misc]


class TestClassifierMetaDictionaries:
    """IPC 状态字典与失败档案 meta 字典的分类契约。"""

    def test_ipc_status_dictionaries(self):
        classifier = ErrorClassifier()

        # IPC retry 字典
        cl_retry = classifier.classify(
            {"status": "retry", "error": "retry me"}
        )
        assert cl_retry.is_retry is True
        assert cl_retry.failed_error_type == ErrorCategory.TRANSIENT_EXHAUSTED.value

        # IPC fatal 字典
        cl_fatal = classifier.classify(
            {"status": "fatal", "error": "fatal crash", "traceback": "..."}
        )
        assert cl_fatal.is_fatal is True
        assert cl_fatal.failed_error_type == ErrorCategory.FATAL.value

        # IPC interrupted 字典：档案视图按 unknown 归档（中断不落档案）
        cl_intr = classifier.classify({"status": "interrupted", "error": "ctrl-c"})
        assert cl_intr.is_interrupted is True
        assert cl_intr.category == ErrorCategory.INTERRUPTED
        assert cl_intr.failed_error_type == ErrorCategory.UNKNOWN.value

    def test_failed_meta_dictionaries(self):
        classifier = ErrorClassifier()

        cl_deadlock = classifier.classify({"error": ERR_DEPENDENCY_DEADLOCK})
        assert cl_deadlock.category == ErrorCategory.DEADLOCK
        assert cl_deadlock.failed_error_type == ErrorCategory.DEADLOCK.value

        cl_no_handler = classifier.classify({"error": ERR_NO_HANDLER})
        assert cl_no_handler.category == ErrorCategory.NO_HANDLER
        assert cl_no_handler.failed_error_type == ErrorCategory.NO_HANDLER.value

        cl_fatal_flag = classifier.classify({"error": "boom", "fatal": True})
        assert cl_fatal_flag.is_fatal is True
        assert cl_fatal_flag.failed_error_type == ErrorCategory.FATAL.value

        # meta 的 error_type 为 unknown 视同未设置：回退错误串映射结论
        cl_typed = classifier.classify(
            {"error": ERR_JOB_DEPENDENCY, "error_type": ErrorCategory.UNKNOWN.value}
        )
        assert cl_typed.failed_error_type == ErrorCategory.DEPENDENCY.value

    def test_classification_roundtrip_via_failed_meta(self):
        """不变式：异常 → 分类 → 失败档案 meta → 分类，结论恒等。"""
        classifier = ErrorClassifier()
        exc = ConnectionResetError("network peer dropped")
        cl_first = classifier.classify(exc)

        meta = cl_first.to_failed_meta(attempt=2)
        cl_second = classifier.classify(meta)

        assert (
            cl_first.failed_error_type
            == cl_second.failed_error_type
            == ErrorCategory.TRANSIENT_EXHAUSTED.value
        )
        assert cl_second.is_fatal is False
        assert meta["_attempt"] == 2

    def test_to_failed_meta_fields(self):
        cl = ErrorClassification(
            category=ErrorCategory.FATAL,
            error_code="",
            failed_error_type=ErrorCategory.FATAL.value,
            is_transient=False,
            is_fatal=True,
            is_retry=False,
            raw_error="ValueError: boom",
            traceback_str="tb-frame",
        )
        meta = cl.to_failed_meta(attempt=3)
        assert meta == {
            "error": "ValueError: boom",
            "error_type": "fatal",
            "fatal": True,
            "traceback": "tb-frame",
            "_attempt": 3,
        }
        # error_code 与 raw_error 双空 → UNKNOWN_ERROR 兜底
        empty = ErrorClassification(
            category=ErrorCategory.UNKNOWN,
            error_code="",
            failed_error_type=ErrorCategory.UNKNOWN.value,
            is_transient=False,
            is_fatal=False,
            is_retry=False,
        )
        assert empty.to_failed_meta()["error"] == "UNKNOWN_ERROR"
        assert "_attempt" not in empty.to_failed_meta()


class TestNeverRaiseContract:
    """Never-Raise 契约：任何脏输入都返回结构化对象，绝不上抛。"""

    def test_classify_garbage_targets(self):
        classifier = ErrorClassifier()
        for garbage in [
            None,
            42,
            "unknown string",
            [],
            object(),
            float("nan"),
            _BrokenStrError("x"),
            _BrokenStrObject(),
        ]:
            cl = classifier.classify(garbage)
            assert isinstance(cl, ErrorClassification)

    def test_normalize_failed_meta_garbage_inputs(self):
        classifier = ErrorClassifier()
        for garbage in [None, 42, [1, 2], _BrokenStrObject()]:
            norm = classifier.normalize_failed_meta(garbage)
            assert isinstance(norm, dict)
            assert "error_type" in norm
            assert "failed_at" in norm
            assert "_attempt" in norm

    def test_normalize_failed_meta_dict_paths(self):
        classifier = ErrorClassifier()
        # dict 缺 error_type → 由分类推导；缺 failed_at → 填 UTC 时间戳
        norm = classifier.normalize_failed_meta({"error": ERR_DEPENDENCY_DEADLOCK})
        assert norm["error_type"] == ErrorCategory.DEADLOCK.value
        assert norm["failed_at"]
        assert norm["_attempt"] == 0

        # 显式 attempt 覆盖；既有 error_type / _attempt 保留
        norm_two = classifier.normalize_failed_meta(
            {"error": "x", "error_type": ErrorCategory.DISPATCH.value, "_attempt": 5},
            attempt=9,
        )
        assert norm_two["error_type"] == ErrorCategory.DISPATCH.value
        assert norm_two["_attempt"] == 9

        # 非 dict meta 的 default_error 兜底
        norm_default = classifier.normalize_failed_meta(
            None, default_error="FALLBACK_ERR"
        )
        assert norm_default["error"] == "FALLBACK_ERR"
        assert norm_default["error_type"] == ErrorCategory.UNKNOWN.value


class TestNeverRaiseAgainstBrokenStr:
    """``__str__`` 会抛异常的对象不得击穿 Never-Raise 契约。

    classify()/validate_payload()/normalize_failed_meta() 的 raw_error/
    expected 等字段对不可信对象取 ``str()`` 时，用户 ``__str__`` 的任意
    异常必须被吞掉并降级为占位串——分类边界内的取串失败不得让 worker
    携 traceback 崩溃（无结果文件、失败被误归因为环境故障）。
    """

    def test_classify_swallows_broken_str_across_all_target_shapes(self):
        classifier = ErrorClassifier()

        # 异常实例路径（_classify_exception）
        cl = classifier.classify(_BrokenStrError("x"))
        assert isinstance(cl, ErrorClassification)
        assert cl.raw_error
        # 普通对象路径（未知标量兜底分支）
        cl_obj = classifier.classify(_BrokenStrObject())
        assert isinstance(cl_obj, ErrorClassification)
        assert cl_obj.category == ErrorCategory.UNKNOWN
        # 字典路径（IPC/档案 meta 的 error 值不可信）
        cl_dict = classifier.classify(
            {"status": "retry", "error": _BrokenStrObject()}
        )
        assert isinstance(cl_dict, ErrorClassification)
        assert cl_dict.is_retry is True
        cl_failed = classifier.classify({"error": _BrokenStrError("x"), "fatal": True})
        assert isinstance(cl_failed, ErrorClassification)
        assert cl_failed.is_fatal is True

    def test_validate_payload_swallows_broken_str_schema(self):
        classifier = ErrorClassifier()
        res = classifier.validate_payload({}, _BrokenStrSchema())
        assert isinstance(res, ValidationResult)
        assert res.is_valid is False
        assert res.errors[0].expected

    def test_normalize_failed_meta_swallows_broken_str_meta(self):
        classifier = ErrorClassifier()
        norm = classifier.normalize_failed_meta(_BrokenStrObject())
        assert isinstance(norm, dict)
        assert norm["error"]
        assert norm["error_type"] == ErrorCategory.UNKNOWN.value


class TestPayloadValidation:
    """载荷结构化校验契约（实例方法与模块级错误串出口）。"""

    class ExampleSchema(TypedDict):
        age: int
        name: str
        tag: str | None

    def test_valid_payload(self):
        classifier = ErrorClassifier()
        res = classifier.validate_payload(
            {"age": 25, "name": "Alice", "tag": None}, self.ExampleSchema
        )
        assert res.is_valid is True
        assert len(res.errors) == 0

    def test_missing_and_unexpected_fields(self):
        classifier = ErrorClassifier()
        res = classifier.validate_payload(
            {"age": 25, "extra": "extra_val"}, self.ExampleSchema
        )
        assert res.is_valid is False
        codes = {e.code for e in res.errors}
        assert "MISSING_REQUIRED_FIELD" in codes
        assert "UNEXPECTED_FIELD" in codes

    def test_bool_rejected_for_int(self):
        """不变式：bool 是 int 子类，但必须被显式拒绝以防类型错位。"""
        classifier = ErrorClassifier()
        res = classifier.validate_payload(
            {"age": True, "name": "Bob"}, self.ExampleSchema
        )
        assert res.is_valid is False
        assert any(e.code == "BOOL_FORBIDDEN_FOR_INT" for e in res.errors)

    def test_non_dict_payload(self):
        classifier = ErrorClassifier()
        res = classifier.validate_payload("not a dict", self.ExampleSchema)
        assert res.is_valid is False
        assert res.errors[0].code == "INVALID_PAYLOAD_TYPE"

    def test_result_exports_and_diagnostics(self):
        classifier = ErrorClassifier()
        res = classifier.validate_payload({"age": True}, self.ExampleSchema)
        assert isinstance(res.to_error_strings(), list)
        diag = res.to_diagnostic_meta()
        assert "details" in diag
        assert diag["error_count"] > 0

    def test_module_level_errors_export(self):
        errors = validate_payload_errors(
            {"age": "thirty", "name": "Charlie", "tag": None}, self.ExampleSchema
        )
        assert errors == ["field 'age' expected int, got str"]

    def test_module_level_missing_field_message(self):
        errors = validate_payload_errors(
            {"name": "Bob", "tag": None}, self.ExampleSchema
        )
        assert errors == ["missing required field 'age'"]

    def test_schema_resolution_failure_is_structured(self):
        classifier = ErrorClassifier()
        res = classifier.validate_payload({"age": 1}, _BrokenStrSchema())
        assert res.is_valid is False
        assert res.errors[0].code == "SCHEMA_RESOLUTION_ERROR"


class TestPayloadValidationMatrix:
    """复杂与对抗性 schema/payload 组合的不变式验证。"""

    class SimpleSchema(TypedDict):
        name: str
        age: int

    class MixedSchema(TypedDict):
        active: bool
        score: float

    class EmptySchema(TypedDict):
        pass

    class OptionalSchema(TypedDict):
        name: str
        nickname: typing.Optional[str]

    class TriSchema(TypedDict):
        val: str | int | None

    def test_wrong_type_message(self):
        errors = validate_payload_errors(
            {"name": "Charlie", "age": "thirty"}, self.SimpleSchema
        )
        assert errors == ["field 'age' expected int, got str"]

    def test_unexpected_field_message(self):
        errors = validate_payload_errors(
            {"name": "Dan", "age": 25, "extra": "oops"}, self.SimpleSchema
        )
        assert errors == ["unexpected field 'extra'"]

    def test_multiple_errors_at_once(self):
        errors = validate_payload_errors({"extra": 1}, self.SimpleSchema)
        assert "missing required field 'name'" in errors
        assert "missing required field 'age'" in errors
        assert "unexpected field 'extra'" in errors
        assert len(errors) == 3

    def test_empty_schema_accepts_empty_and_rejects_extra(self):
        assert validate_payload_errors({}, self.EmptySchema) == []
        assert validate_payload_errors(
            {"anything": 123}, self.EmptySchema
        ) == ["unexpected field 'anything'"]

    def test_bool_field_type_rules(self):
        assert validate_payload_errors(
            {"active": True, "score": 3.14}, self.MixedSchema
        ) == []
        errors = validate_payload_errors({"active": 1, "score": 3.14}, self.MixedSchema)
        assert errors == ["field 'active' expected bool, got int"]

    def test_none_value_for_non_optional_rejected(self):
        errors = validate_payload_errors(
            {"name": None, "age": 30}, self.SimpleSchema
        )
        assert errors == ["field 'name' expected str, got NoneType"]

    def test_optional_field_rules(self):
        schema = self.OptionalSchema
        assert validate_payload_errors({"name": "Alice", "nickname": None}, schema) == []
        assert validate_payload_errors({"name": "Alice", "nickname": "Ali"}, schema) == []
        # Optional 字段仍必填（缺键报 missing），None 是合法值
        errors = validate_payload_errors({"nickname": None}, schema)
        assert "missing required field 'name'" in errors
        # 非法值类型：错误串携带联合类型名与实际类型
        errors_bad = validate_payload_errors(
            {"name": "Alice", "nickname": 42}, schema
        )
        assert len(errors_bad) == 1
        assert errors_bad[0].startswith("field 'nickname' expected ")
        assert "got int" in errors_bad[0]

    def test_new_style_three_way_union(self):
        schema = self.TriSchema
        assert validate_payload_errors({"val": "hello"}, schema) == []
        assert validate_payload_errors({"val": 42}, schema) == []
        assert validate_payload_errors({"val": None}, schema) == []
        errors = validate_payload_errors({"val": 3.14}, schema)
        assert len(errors) == 1
        assert "got float" in errors[0]

    def test_container_type_fields(self):
        class ListSchema(TypedDict):
            items: list

        class DictSchema(TypedDict):
            data: dict

        class GenSchema(TypedDict):
            tags: list[str]
            metadata: dict[str, int]

        assert "expected list" in validate_payload_errors(
            {"items": "not_a_list"}, ListSchema
        )[0]
        assert "expected dict" in validate_payload_errors(
            {"data": [1, 2]}, DictSchema
        )[0]
        assert (
            validate_payload_errors(
                {"tags": ["a", "b"], "metadata": {"x": 1}}, GenSchema
            )
            == []
        )
        assert "expected list" in validate_payload_errors(
            {"tags": "not_a_list"}, GenSchema
        )[0]

    def test_tuple_set_frozenset_bytes_fields(self):
        class TupleSchema(TypedDict):
            coords: tuple

        class SetSchema(TypedDict):
            tags: set

        class FsSchema(TypedDict):
            immutable: frozenset

        class BytesSchema(TypedDict):
            data: bytes

        assert validate_payload_errors({"coords": (1, 2)}, TupleSchema) == []
        assert "got list" in validate_payload_errors(
            {"coords": [1, 2]}, TupleSchema
        )[0]
        assert validate_payload_errors({"tags": {1, 2, 3}}, SetSchema) == []
        assert "got list" in validate_payload_errors({"tags": [1, 2, 3]}, SetSchema)[0]
        assert validate_payload_errors(
            {"immutable": frozenset([1, 2])}, FsSchema
        ) == []
        assert "got set" in validate_payload_errors({"immutable": {1, 2}}, FsSchema)[0]
        assert validate_payload_errors({"data": b"hello"}, BytesSchema) == []
        assert "got str" in validate_payload_errors({"data": "hello"}, BytesSchema)[0]

    def test_literal_fields(self):
        class LitSchema(TypedDict):
            mode: Literal["a", "b"]

        class LitIntSchema(TypedDict):
            code: Literal[1, 2]

        assert validate_payload_errors({"mode": "a"}, LitSchema) == []
        errors = validate_payload_errors({"mode": "c"}, LitSchema)
        assert len(errors) == 1
        assert "Literal" in errors[0]
        assert validate_payload_errors({"code": 2}, LitIntSchema) == []
        assert len(validate_payload_errors({"code": 3}, LitIntSchema)) == 1

    def test_literal_inside_union_fallback(self):
        """联合内含 Literal 成员：isinstance 不支持的组合走 Literal 兜底。"""

        class UnionLitSchema(TypedDict):
            mode: typing.Union[Literal["a", "b"], int]

        assert validate_payload_errors({"mode": "a"}, UnionLitSchema) == []
        assert validate_payload_errors({"mode": 7}, UnionLitSchema) == []
        errors = validate_payload_errors({"mode": 3.14}, UnionLitSchema)
        assert len(errors) == 1
        assert "got float" in errors[0]

    def test_bool_in_int_union_rejected(self):
        """联合含 int 且不含 bool：bool 值不得借 int 子类身份通过。"""

        class IntUnionSchema(TypedDict):
            amount: int | None

        errors = validate_payload_errors({"amount": True}, IntUnionSchema)
        assert len(errors) == 1
        assert "got bool" in errors[0]

    def test_nested_typeddict_skipped_not_crashing(self):
        """嵌套 TypedDict 字段不递归校验（浅层校验），不炸裂管线。"""

        class Inner(TypedDict):
            x: int

        class Outer(TypedDict):
            inner: Inner

        assert validate_payload_errors({"inner": {"x": 1}}, Outer) == []
        assert validate_payload_errors({"inner": {"x": "not_int"}}, Outer) == []

    def test_regular_class_schema(self):
        class RegularSchema:
            name: str
            age: int

        assert validate_payload_errors({"name": "Alice", "age": 30}, RegularSchema) == []
        assert "expected int" in validate_payload_errors(
            {"name": "Alice", "age": "old"}, RegularSchema
        )[0]

    def test_schema_without_annotations_all_unexpected(self):
        class NoHints:
            pass

        errors = validate_payload_errors({"anything": 1}, NoHints)
        assert len(errors) == 1
        assert "unexpected field" in errors[0]

    def test_underscore_keys_not_exempt(self):
        """``_`` 前缀键不豁免：与普通未知字段一样拒绝。"""
        errors = validate_payload_errors(
            {"name": "Alice", "age": 30, "_internal": 42}, self.SimpleSchema
        )
        assert errors == ["unexpected field '_internal'"]

    def test_private_schema_field_still_required(self):
        class PrivSchema(TypedDict):
            _private: str
            public: int

        errors = validate_payload_errors({"public": 1}, PrivSchema)
        assert "missing required field '_private'" in errors

    def test_non_dict_payloads_report_single_clear_error(self):
        assert validate_payload_errors(None, self.SimpleSchema) == [
            "payload must be a dict, got NoneType"
        ]
        assert validate_payload_errors([1, 2, 3], self.SimpleSchema) == [
            "payload must be a dict, got list"
        ]

    def test_idempotent_validation(self):
        payload = {"name": "Alice", "age": "old"}
        first = validate_payload_errors(payload, self.SimpleSchema)
        second = validate_payload_errors(payload, self.SimpleSchema)
        assert first == second

    def test_large_schema_payload(self):
        class BigSchema:
            pass

        BigSchema.__annotations__ = {f"f{i}": int for i in range(1000)}
        payload = {f"f{i}": i for i in range(1000)}
        assert validate_payload_errors(payload, BigSchema) == []


class TestConstructorPolicyValidation:
    """构造器策略序列入口校验：与注册/声明路径同规（fail-loud）。

    不变式：classify() 标注 Never-Raise 契约，注册表/fatal/transient
    序列若混入非 type 成员，matches() 的 isinstance 会让 TypeError 从
    classify() 逸出——构造期即拒绝。
    """

    def test_transient_registry_rejects_non_type(self):
        with pytest.raises(TypeError, match="transient_registry"):
            ErrorClassifier(transient_registry=[42])

    def test_fatal_exceptions_rejects_non_type(self):
        with pytest.raises(TypeError, match="fatal_exceptions"):
            ErrorClassifier(fatal_exceptions=["ConnectionError"])

    def test_transient_exceptions_rejects_non_exception_subclass(self):
        with pytest.raises(TypeError, match="transient_exceptions"):
            ErrorClassifier(transient_exceptions=[int])

    def test_valid_sequences_still_accepted(self):
        classifier = ErrorClassifier(
            fatal_exceptions=(KeyError,),
            transient_exceptions=(TimeoutError,),
            transient_registry=(ConnectionError,),
        )
        assert classifier.classify(KeyError("k")).is_fatal is True
        assert classifier.classify(TimeoutError("t")).is_transient is True
        assert classifier.classify(ConnectionError("c")).is_retry is True

    def test_generator_and_iterator_inputs_materialize_once(self):
        """生成器/一次性迭代器入参与序列语义一致：校验物化结果复用于赋值。

        若校验先行消费迭代器而赋值处二次物化，用户策略会静默变空且不回退
        内置默认，classify 将本应 fatal 的异常误判为可重试、白烧重试预算。
        """
        classifier = ErrorClassifier(
            fatal_exceptions=(c for c in [KeyError]),
            transient_exceptions=iter([TimeoutError]),
        )
        assert classifier.fatal_exceptions == (KeyError,)
        assert classifier.transient_exceptions == (TimeoutError,)
        assert classifier.classify(KeyError("k")).is_fatal is True
        assert classifier.classify(TimeoutError("t")).is_transient is True

    def test_transient_registry_accepts_one_shot_iterator(self):
        registry = ErrorClassifier(transient_registry=iter([ConnectionError]))
        assert registry.snapshot() == (ConnectionError,)
        assert registry.matches(ConnectionError("c")) is True

    def test_list_input_and_generator_member_validation(self):
        assert ErrorClassifier(fatal_exceptions=[ValueError]).fatal_exceptions == (
            ValueError,
        )
        # 生成器入参的非法成员仍在构造期 fail-loud，校验语义不变
        with pytest.raises(TypeError, match="transient_registry"):
            ErrorClassifier(transient_registry=(c for c in [42]))


class TestConstructorRejectsDedicatedAndUnpicklableClasses:
    """构造路径与注册/声明路径同规：专用分支异常类与不可 pickle 类构造期拒绝。

    RetryError/FatalError 拥有专用 except 分支且 ``_classify_exception`` 中
    注册表命中判定先于 fatal 判定——构造器若接受其子类进入注册表，
    fatal 异常会被翻转为可重试（白烧重试预算）；函数作用域类不可随
    ctx pickle 下发，spawn 派发期才失败的滞后错误必须在构造期消灭。
    """

    def test_transient_registry_rejects_retry_error_subclass(self):
        class MyRetry(RetryError):
            pass

        with pytest.raises(TypeError, match="transient_registry.*RetryError"):
            ErrorClassifier(transient_registry=[MyRetry])

    def test_transient_registry_rejects_fatal_error_subclass(self):
        class MyFatal(FatalError):
            pass

        with pytest.raises(TypeError, match="transient_registry.*FatalError"):
            ErrorClassifier(transient_registry=[MyFatal])

    def test_fatal_exceptions_rejects_retry_error_subclass(self):
        class MyRetry(RetryError):
            pass

        with pytest.raises(TypeError, match="fatal_exceptions.*RetryError"):
            ErrorClassifier(fatal_exceptions=(MyRetry,))

    def test_transient_exceptions_rejects_fatal_error_subclass(self):
        class MyFatal(FatalError):
            pass

        with pytest.raises(TypeError, match="transient_exceptions.*FatalError"):
            ErrorClassifier(transient_exceptions=(MyFatal,))

    def test_transient_registry_rejects_unpicklable_class(self):
        class LocalError(Exception):
            pass

        with pytest.raises(TypeError, match="module-level"):
            ErrorClassifier(transient_registry=[LocalError])

    def test_fatal_exceptions_rejects_unpicklable_class(self):
        class LocalError(Exception):
            pass

        with pytest.raises(TypeError, match="module-level"):
            ErrorClassifier(fatal_exceptions=(LocalError,))

    def test_classify_keeps_fatal_priority_for_legal_policies(self):
        """合法策略下 fatal 判定不受注册表污染：FatalError 子类恒 fatal，
        不得因注册表命中先于 fatal 判定而翻转为可重试。"""
        classifier = ErrorClassifier(transient_registry=(ConnectionError,))
        assert classifier.classify(FatalError("f")).is_fatal is True
        assert classifier.classify(FatalError("f")).is_retry is False
        assert classifier.classify(ConnectionError("c")).is_retry is True


class TestTransientRegistryManagement:
    """per-pipeline 瞬态注册表：注册、幂等与隔离。"""

    def test_register_transient_idempotent_and_isolated(self):
        registry = ErrorClassifier()
        registry.register_transient(_CustomNetworkError)
        assert is_transient_exception(_CustomNetworkError("down"), registry.snapshot())
        # 注册后立即生效，且幂等
        registry.register_transient(_CustomNetworkError)
        assert registry.snapshot() == (_CustomNetworkError,)
        # 隔离语义：未注册的实例不受影响
        other = ErrorClassifier()
        assert not is_transient_exception(_CustomNetworkError("down"), other.snapshot())

    def test_register_rejects_function_scope_class(self):
        """fail-loud：函数作用域定义的类无法随 spawn 下发 → 注册即 TypeError。"""

        class LocalError(Exception):
            pass

        with pytest.raises(TypeError, match="module-level"):
            ErrorClassifier().register_transient(LocalError)

    def test_register_rejects_non_exception(self):
        with pytest.raises(TypeError):
            ErrorClassifier().register_transient(int)  # type: ignore[arg-type]

    def test_builtin_transient_and_fatal_heuristics(self):
        """内置启发式：连接类瞬态自动重试，bug 类与未知异常不重试。"""
        assert is_transient_exception(ConnectionResetError("conn reset"))
        assert is_transient_exception(TimeoutError("took too long"))
        assert is_transient_exception(BrokenPipeError("pipe"))
        assert is_transient_exception(RetryError("explicit"))
        assert not is_transient_exception(KeyError("k"))
        assert not is_transient_exception(TypeError("t"))
        assert not is_transient_exception(RuntimeError("unclassified"))


class TestClassifyExceptionKwargsContract:
    """classify_exception 的 registry 形态与显式 kwargs 契约。"""

    def test_classifier_with_explicit_kwargs_rejected(self):
        """ErrorClassifier 已持完整分类策略：显式 kwargs 与其互斥，fail-loud
        而非静默清零——静默忽略会让同一调用仅因 registry 形态不同语义翻转。"""
        classifier = ErrorClassifier()
        with pytest.raises(TypeError, match="fatal_exceptions"):
            classify_exception(
                ValueError("boom"), classifier, fatal_exceptions=(ValueError,)
            )
        with pytest.raises(TypeError, match="transient_exceptions"):
            classify_exception(
                ValueError("boom"), classifier, transient_exceptions=(ValueError,)
            )

    def test_classifier_without_kwargs_uses_its_policy(self):
        classifier = ErrorClassifier()
        assert classify_exception(ValueError("boom"), classifier) == "error"
        assert classify_exception(KeyError("k"), classifier) == "fatal"

    def test_tuple_registry_with_kwargs_still_effective(self):
        assert (
            classify_exception(ValueError("boom"), (), fatal_exceptions=(ValueError,))
            == "fatal"
        )


class TestErrorCodeStringMapping:
    """错误码常量 → 大类映射的穷举锁定（与 v1 决策表同源）。"""

    @pytest.mark.parametrize(
        "code, category",
        [
            (ERR_DEPENDENCY_DEADLOCK, ErrorCategory.DEADLOCK),
            (ERR_RESOURCE_DEADLOCK, ErrorCategory.DEADLOCK),
            (ERR_MALFORMED_JOB, ErrorCategory.DEADLOCK),
            (ERR_DEADLOCK_GAP, ErrorCategory.DEADLOCK),
            (ERR_JOB_DEPENDENCY, ErrorCategory.DEPENDENCY),
            (ERR_MAX_RETRIES, ErrorCategory.TRANSIENT_EXHAUSTED),
            (ERR_NO_HANDLER, ErrorCategory.NO_HANDLER),
            (ERR_PAYLOAD_VALIDATION, ErrorCategory.VALIDATION),
            (ERR_COMMIT_FAILURE, ErrorCategory.COMMIT_FAILURE),
            (ERR_DISPATCH_FAILURE, ErrorCategory.DISPATCH),
        ],
    )
    def test_error_code_maps_category(self, code, category):
        classifier = ErrorClassifier()
        cl = classifier.classify(code)
        assert cl.category == category
        assert cl.failed_error_type == category.value

    def test_prefixed_error_codes_match(self):
        """带上下文后缀的错误串按前缀归因（失败档案落盘串回读场景）。"""
        classifier = ErrorClassifier()
        cl = classifier.classify(f"{ERR_DISPATCH_FAILURE}: spawn failed hard")
        assert cl.category == ErrorCategory.DISPATCH
        assert cl.error_code == ERR_DISPATCH_FAILURE


@pytest.fixture()
def classifier():
    return ErrorClassifier()


class TestDeathAttributionTable:
    """classify_process_death 决策表的完备性表驱动测试。

    锁定不变式：
    1. exitcode × timed_out × timeout_is_transient 全组合的 meta 形状 /
       错误串 / 瞬态语义 / failed_error_type；
    2. 决策表产出的错误串族必须被 _classify_string 映射消费（死亡串不落
       unknown）；
    3. 瞬态军规：环境死亡烧预算（retry_requested=True）、非瞬态超时终局。
    """

    def test_transient_timeout_retries_without_meta(self, classifier):
        att = classifier.classify_process_death(
            None, timed_out=True, timeout_seconds=5.0, timeout_is_transient=True
        )
        assert att.retry_requested is True
        assert att.result_meta == {}
        assert att.retry_error == f"{ERR_TIMEOUT_PREFIX}5.0s)"
        assert att.failed_error_type == ErrorCategory.TRANSIENT_EXHAUSTED.value

    def test_non_transient_timeout_is_terminal_fatal(self, classifier):
        att = classifier.classify_process_death(
            None, timed_out=True, timeout_seconds=3.0, timeout_is_transient=False
        )
        assert att.retry_requested is False
        assert att.result_meta == {"error": f"{ERR_TIMEOUT_PREFIX}3.0s)"}
        assert att.retry_error is None
        assert att.failed_error_type == ErrorCategory.FATAL.value

    @pytest.mark.parametrize("code", [-9, -11, -15])
    def test_negative_exitcode_is_signal_death(self, classifier, code):
        att = classifier.classify_process_death(
            code, timed_out=False, timeout_is_transient=False
        )
        assert att.retry_requested is True
        assert att.result_meta["error"] == f"{ERR_PROCESS_CRASH_PREFIX}{code}"
        assert "signal" in att.result_meta
        assert att.result_meta["oom_hint"] is (code == -9)
        assert att.retry_error.startswith(ERR_PROCESS_SIGNAL_DEATH)
        assert att.failed_error_type == ErrorCategory.TRANSIENT_EXHAUSTED.value

    def test_signal_name_resolved_for_known_signal(self, classifier):
        att = classifier.classify_process_death(
            -9, timed_out=False, timeout_is_transient=False
        )
        assert att.result_meta["signal"] == "SIGKILL"

    def test_unknown_signal_number_falls_back_to_signo(self, classifier):
        att = classifier.classify_process_death(
            -99, timed_out=False, timeout_is_transient=False
        )
        assert att.result_meta["signal"] == "SIGNO99"

    @pytest.mark.parametrize("code", [1, 2, 42])
    def test_positive_exitcode_is_env_crash(self, classifier, code):
        att = classifier.classify_process_death(
            code, timed_out=False, timeout_is_transient=False
        )
        assert att.retry_requested is True
        assert att.result_meta == {"error": f"{ERR_PROCESS_CRASH_PREFIX}{code}"}
        assert "signal" not in att.result_meta
        assert "oom_hint" not in att.result_meta
        assert att.retry_error == f"PROCESS_CRASH_EXIT: worker exited with code {code}"
        assert att.failed_error_type == ErrorCategory.TRANSIENT_EXHAUSTED.value

    @pytest.mark.parametrize("code", [None, 0])
    def test_zero_or_missing_exitcode_is_no_ipc_result(self, classifier, code):
        att = classifier.classify_process_death(
            code, timed_out=False, timeout_is_transient=False
        )
        assert att.retry_requested is True
        assert att.result_meta == {"error": ERR_NO_IPC_RESULT}
        assert att.retry_error == (
            f"{ERR_NO_IPC_RESULT}: worker exited without writing a result file"
        )
        assert att.failed_error_type == ErrorCategory.TRANSIENT_EXHAUSTED.value

    def test_timed_out_takes_priority_over_exitcode(self, classifier):
        """timed_out 优先裁决：超时窗口内被看门狗 kill（exitcode=-9）仍按
        超时归因，不落入信号死亡分支。"""
        att = classifier.classify_process_death(
            -9, timed_out=True, timeout_seconds=2.0, timeout_is_transient=True
        )
        assert att.retry_requested is True
        assert att.retry_error == f"{ERR_TIMEOUT_PREFIX}2.0s)"


class TestDeathStringClassification:
    """决策表产出的串族必须经 classify/档案分类链得到非 unknown 分类。"""

    @pytest.mark.parametrize(
        "error_str, expected_type",
        [
            (f"{ERR_TIMEOUT_PREFIX}5s)", ErrorCategory.FATAL.value),
            (
                f"{ERR_PROCESS_CRASH_PREFIX}-9",
                ErrorCategory.TRANSIENT_EXHAUSTED.value,
            ),
            (ERR_NO_IPC_RESULT, ErrorCategory.TRANSIENT_EXHAUSTED.value),
            (
                f"{ERR_PROCESS_SIGNAL_DEATH}: killed by SIGKILL (-9)",
                ErrorCategory.TRANSIENT_EXHAUSTED.value,
            ),
        ],
    )
    def test_death_strings_classified(self, classifier, error_str, expected_type):
        cl = classifier.classify({"error": error_str})
        assert cl.failed_error_type == expected_type
        assert classify_error_type({"error": error_str}) == expected_type

    def test_arbitrary_business_string_still_unknown(self, classifier):
        assert classify_error_type({"error": "some handler message"}) == "unknown"

    def test_timeout_string_marks_fatal(self, classifier):
        cl = classifier.classify({"error": f"{ERR_TIMEOUT_PREFIX}5s)"})
        assert cl.is_fatal is True
        assert cl.category == ErrorCategory.FATAL

    def test_decision_table_rows_carry_consistent_failed_types(self, classifier):
        """决策表每行的 failed_error_type 与其错误串的分类结论一致——两表同源。"""
        rows = [
            classifier.classify_process_death(
                None, timed_out=True, timeout_seconds=1.0, timeout_is_transient=True
            ),
            classifier.classify_process_death(
                None, timed_out=True, timeout_seconds=1.0, timeout_is_transient=False
            ),
            classifier.classify_process_death(-9, timed_out=False),
            classifier.classify_process_death(1, timed_out=False),
            classifier.classify_process_death(None, timed_out=False),
        ]
        for att in rows:
            assert isinstance(att, DeathAttribution)
            error = att.result_meta.get("error")
            if error:
                assert classify_error_type({"error": error}) == att.failed_error_type
