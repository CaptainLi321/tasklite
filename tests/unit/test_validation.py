from typing import Literal, Optional, TypedDict, Union
from tasklite.utils import validate_payload

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
    nickname: Optional[str]

# Tests

def test_valid_payload(tmp_path):
    """All required fields present with correct types → no errors."""
    errors = validate_payload({"name": "Alice", "age": 30}, SimpleSchema)
    assert errors == []

def test_missing_required_field(tmp_path):
    """Missing a required field → error with 'missing required field'."""
    errors = validate_payload({"name": "Bob"}, SimpleSchema)
    assert errors == ["missing required field 'age'"]

def test_wrong_type(tmp_path):
    """Field present but wrong type → error about type mismatch."""
    errors = validate_payload({"name": "Charlie", "age": "thirty"}, SimpleSchema)
    assert errors == [
        "field 'age' expected int, got str"
    ]

def test_unexpected_field(tmp_path):
    """Extra field not in schema → error about unexpected field."""
    errors = validate_payload(
        {"name": "Dan", "age": 25, "extra": "oops"}, SimpleSchema
    )
    assert errors == ["unexpected field 'extra'"]

def test_multiple_errors(tmp_path):
    """Empty payload against schema with ≥2 required fields → multiple errors."""
    errors = validate_payload({}, SimpleSchema)
    assert "missing required field 'name'" in errors
    assert "missing required field 'age'" in errors
    assert len(errors) == 2

def test_empty_schema_accepts_any(tmp_path):
    """EmptySchema has no hints → any payload is valid."""
    errors = validate_payload({}, EmptySchema)
    assert errors == []

def test_empty_schema_extra_is_unexpected(tmp_path):
    """EmptySchema has no expected fields → any payload key is unexpected."""
    errors = validate_payload({"anything": 123}, EmptySchema)
    assert errors == ["unexpected field 'anything'"]

def test_bool_type(tmp_path):
    """bool fields with bool values pass."""
    errors = validate_payload(
        {"active": True, "score": 3.14}, MixedSchema
    )
    assert errors == []

def test_bool_wrong_type(tmp_path):
    """bool field with int 1 (truthy but wrong type) → type error."""
    errors = validate_payload(
        {"active": 1, "score": 3.14}, MixedSchema
    )
    assert errors == [
        "field 'active' expected bool, got int"
    ]

def test_none_value(tmp_path):
    """None value for a non-optional str field → type error (NoneType not str)."""
    errors = validate_payload({"name": None, "age": 30}, SimpleSchema)
    assert errors == [
        "field 'name' expected str, got NoneType"
    ]

def test_optional_field_accepts_none(tmp_path):
    """Optional[str] field accepts None value without error."""
    errors = validate_payload({"name": "Alice", "nickname": None}, OptionalSchema)
    assert errors == []

def test_optional_field_accepts_string(tmp_path):
    """Optional[str] field accepts string value without error."""
    errors = validate_payload({"name": "Alice", "nickname": "Ali"}, OptionalSchema)
    assert errors == []

def test_optional_field_rejects_wrong_type(tmp_path):
    """Optional[str | None] field rejects int value — verifies Union type name in error."""
    errors = validate_payload({"name": "Alice", "nickname": 42}, OptionalSchema)
    assert len(errors) == 1
    assert errors[0].startswith("field 'nickname' expected ")
    assert "got int" in errors[0]

def test_validate_payload_all_errors_at_once(tmp_path):
    errors = validate_payload({"extra": 1}, SimpleSchema)
    assert "missing required field 'name'" in errors
    assert "missing required field 'age'" in errors
    assert "unexpected field 'extra'" in errors
    assert len(errors) == 3

def test_validate_payload_list_type_rejected(tmp_path):
    class ListSchema(TypedDict):
        items: list
    errors = validate_payload({"items": "not_a_list"}, ListSchema)
    assert len(errors) == 1
    assert "expected list" in errors[0]
    assert "got str" in errors[0]

def test_validate_payload_dict_type_rejected(tmp_path):
    class DictSchema(TypedDict):
        data: dict
    errors = validate_payload({"data": [1, 2]}, DictSchema)
    assert len(errors) == 1
    assert "expected dict" in errors[0]
    assert "got list" in errors[0]

def test_validate_payload_underscore_keys_ignored(tmp_path):
    """Underscore-prefixed keys are not exempt (restricted prefix policy);
    _seen_ids exemption was removed with the old discovery."""
    errors = validate_payload(
        {"name": "Alice", "age": 30, "_seen_ids": ["a", "b"], "_internal": 42},
        SimpleSchema,
    )
    # 旧 discovery 的 _seen_ids 保留键已移除 → 与普通未知字段一样拒绝
    assert any("_seen_ids" in e for e in errors), f"expected rejection: {errors}"
    assert any("_internal" in e for e in errors)

def test_validate_payload_non_underscore_unexpected_still_rejected(tmp_path):
    """Non-underscore extra fields are still rejected."""
    errors = validate_payload(
        {"name": "Alice", "age": 30, "extra_field": "oops"},
        SimpleSchema,
    )
    assert len(errors) == 1
    assert "unexpected field 'extra_field'" in errors[0]

def test_validate_payload_parameterized_generic_accepted(tmp_path):
    """Parameterized generic (list[str]) should validate correctly via get_origin()."""
    class GenSchema(TypedDict):
        tags: list[str]
        metadata: dict[str, int]
    errors = validate_payload({"tags": ["a", "b"], "metadata": {"x": 1}}, GenSchema)
    assert errors == []

def test_validate_payload_parameterized_generic_wrong_type(tmp_path):
    """Parameterized generic with wrong concrete type → error."""
    class GenSchema(TypedDict):
        tags: list[str]
    errors = validate_payload({"tags": "not_a_list"}, GenSchema)
    assert len(errors) == 1
    assert "expected list" in errors[0]
    assert "got str" in errors[0]

# Schema complexity & edge case payloads (invariant verification)

class TestSchemaComplexity:
    """Complex and adversarial schema/payload combinations."""
    def test_nested_typeddict_field_not_validated(self, tmp_path):
        """TypedDict as a field type → validator skips it instead of crashing.
        Previously this raised TypeError because TypedDict does not support
        isinstance checks. The validator now catches such TypeErrors and falls
        back to a lenient check to avoid blowing up the pipeline.
        """
        class Inner(TypedDict):
            x: int
        class Outer(TypedDict):
            inner: Inner
        assert validate_payload({"inner": {"x": 1}}, Outer) == []
    def test_nested_typeddict_wrong_inner_type_not_caught(self, tmp_path):
        """Nested TypedDict inner content is not recursively validated."""
        class Inner(TypedDict):
            x: int
        class Outer(TypedDict):
            inner: Inner
        # Validation is shallow; inner TypedDict checks are skipped.
        assert validate_payload({"inner": {"x": "not_int"}}, Outer) == []
    def test_schema_not_typeddict_regular_class(self, tmp_path):
        """Schema is a regular class with __annotations__ → get_type_hints still works."""
        class RegularSchema:
            name: str
            age: int
        errors = validate_payload({"name": "Alice", "age": 30}, RegularSchema)
        assert errors == []
    def test_schema_not_typeddict_regular_class_wrong_type(self, tmp_path):
        """Regular class schema with wrong type → error reported."""
        class RegularSchema:
            name: str
            age: int
        errors = validate_payload({"name": "Alice", "age": "old"}, RegularSchema)
        assert len(errors) == 1
        assert "expected int" in errors[0]
    def test_optional_fields_still_required_in_typeddict(self, tmp_path):
        """Optional[str] doesn't make a field optional in TypedDict — still required.
        The field is required, but None is an accepted value.
        """
        class OptSchema(TypedDict):
            name: Optional[str]
            age: Optional[int]
        # Empty payload → both fields reported as missing
        errors = validate_payload({}, OptSchema)
        assert len(errors) == 2
        assert "missing required field 'name'" in errors
        assert "missing required field 'age'" in errors
    def test_optional_fields_accept_none(self, tmp_path):
        """Optional fields accept None as a value."""
        class OptSchema(TypedDict):
            name: Optional[str]
            age: Optional[int]
        errors = validate_payload({"name": None, "age": None}, OptSchema)
        assert errors == []
    def test_payload_with_all_none_values_non_optional_rejected(self, tmp_path):
        """Non-optional fields with None values → type error."""
        class Schema(TypedDict):
            name: str
            age: int
        errors = validate_payload({"name": None, "age": None}, Schema)
        assert len(errors) == 2
        assert any("expected str, got NoneType" in e for e in errors)
        assert any("expected int, got NoneType" in e for e in errors)
    def test_optional_union_three_types(self, tmp_path):
        """field: Optional[Union[str, int]] accepts str, int, None; rejects float."""
        class TriSchema(TypedDict):
            val: Optional[Union[str, int]]
        # All accepted
        assert validate_payload({"val": "hello"}, TriSchema) == []
        assert validate_payload({"val": 42}, TriSchema) == []
        assert validate_payload({"val": None}, TriSchema) == []
        # float rejected
        errors = validate_payload({"val": 3.14}, TriSchema)
        assert len(errors) == 1
        assert "got float" in errors[0]
    def test_tuple_type_field(self, tmp_path):
        """field: tuple accepts tuple, rejects list."""
        class TupleSchema(TypedDict):
            coords: tuple
        assert validate_payload({"coords": (1, 2)}, TupleSchema) == []
        errors = validate_payload({"coords": [1, 2]}, TupleSchema)
        assert len(errors) == 1
        assert "expected tuple" in errors[0]
        assert "got list" in errors[0]
    def test_set_type_field(self, tmp_path):
        """field: set accepts set, rejects list."""
        class SetSchema(TypedDict):
            tags: set
        assert validate_payload({"tags": {1, 2, 3}}, SetSchema) == []
        errors = validate_payload({"tags": [1, 2, 3]}, SetSchema)
        assert len(errors) == 1
        assert "expected set" in errors[0]
    def test_frozenset_type_field(self, tmp_path):
        """field: frozenset accepts frozenset, rejects set."""
        class FsSchema(TypedDict):
            immutable: frozenset
        assert validate_payload({"immutable": frozenset([1, 2])}, FsSchema) == []
        errors = validate_payload({"immutable": {1, 2}}, FsSchema)
        assert len(errors) == 1
        assert "expected frozenset" in errors[0]
    def test_bytes_type_field(self, tmp_path):
        """field: bytes accepts bytes, rejects str."""
        class BytesSchema(TypedDict):
            data: bytes
        assert validate_payload({"data": b"hello"}, BytesSchema) == []
        errors = validate_payload({"data": "hello"}, BytesSchema)
        assert len(errors) == 1
        assert "expected bytes" in errors[0]
        assert "got str" in errors[0]
    def test_very_large_payload(self, tmp_path):
        """1000 fields — performance/sanity check."""
        # Build a TypedDict dynamically is hard; instead use a regular class.
        class BigSchema:
            pass
        hints = {f"f{i}": int for i in range(1000)}
        BigSchema.__annotations__ = hints
        payload = {f"f{i}": i for i in range(1000)}
        errors = validate_payload(payload, BigSchema)
        assert errors == []
    def test_validate_payload_idempotent(self, tmp_path):
        """Calling validate twice produces the same result."""
        class Schema(TypedDict):
            name: str
            age: int
        payload = {"name": "Alice", "age": "old"}
        errors1 = validate_payload(payload, Schema)
        errors2 = validate_payload(payload, Schema)
        assert errors1 == errors2
    def test_field_name_with_underscore_only_required(self, tmp_path):
        """_private: str in schema — _-prefixed keys ARE checked in required-field loop."""
        class PrivSchema(TypedDict):
            _private: str
            public: int
        # Missing _private → still reported as missing (required loop checks all hints)
        errors = validate_payload({"public": 1}, PrivSchema)
        assert "missing required field '_private'" in errors
    def test_field_name_with_underscore_only_not_unexpected(self, tmp_path):
        """_-prefixed keys in payload are flagged as unexpected unless
        framework-reserved (_seen_ids, _backoff_*). Restricted prefix policy."""
        class PrivSchema(TypedDict):
            public: int
        errors = validate_payload({"public": 1, "_internal": "x"}, PrivSchema)
        assert errors == ["unexpected field '_internal'"], \
            "non-reserved _-prefixed keys should be flagged as unexpected"
    def test_payload_with_empty_dict_against_empty_schema(self, tmp_path):
        """Empty payload {} against EmptySchema → valid (no required fields)."""
        class EmptySchema(TypedDict):
            pass
        errors = validate_payload({}, EmptySchema)
        assert errors == []
    def test_none_payload_raises_attributeerror(self, tmp_path):
        """payload=None → 返回明确错误串（不再抛 TypeError 崩 run）。"""
        class Schema(TypedDict):
            name: str
        errors = validate_payload(None, Schema)
        assert errors == ["payload must be a dict, got NoneType"]
    def test_list_payload_reports_unexpected_fields(self, tmp_path):
        """payload=[] (non-dict) → 返回明确错误串，不崩溃、不逐元素误报。
        修复：旧行为对非 dict payload 逐元素报 unexpected field（依赖
        'for key in payload' 对 list 迭代的偶然行为），且 None/str 会抛
        TypeError 崩掉整个 run。现在入口统一返回一条清晰错误。
        """
        class Schema(TypedDict):
            name: str
        errors = validate_payload([1, 2, 3], Schema)
        assert len(errors) == 1
        assert errors[0] == "payload must be a dict, got list"
    def test_schema_with_no_annotations_raises_attributeerror(self, tmp_path):
        """Schema with no __annotations__ → get_type_hints returns {} → all keys unexpected."""
        class NoHints:
            pass
        errors = validate_payload({"anything": 1}, NoHints)
        assert len(errors) == 1
        assert "unexpected field" in errors[0]
    def test_bool_is_not_int_subtype(self, tmp_path):
        """bool field rejects int (even though bool is subclass of int, type names differ)."""
        class BSchema(TypedDict):
            flag: bool
        # int 1 IS an instance of bool? No — bool is subclass of int, not vice versa.
        # isinstance(1, bool) is False, so int value → error.
        errors = validate_payload({"flag": 1}, BSchema)
        assert len(errors) == 1
        assert "expected bool" in errors[0]
        assert "got int" in errors[0]
    def test_int_accepted_for_float_field(self, tmp_path):
        """int is NOT accepted for float field (isinstance(1, float) is False)."""
        class FSchema(TypedDict):
            score: float
        errors = validate_payload({"score": 1}, FSchema)
        assert len(errors) == 1
        assert "expected float" in errors[0]
        assert "got int" in errors[0]
    def test_literal_field_accepts_allowed_value(self, tmp_path):
        """Literal[\"a\", \"b\"] 接受允许值，拒绝其他值。"""
        class LitSchema(TypedDict):
            mode: Literal["a", "b"]
        assert validate_payload({"mode": "a"}, LitSchema) == []
        errors = validate_payload({"mode": "c"}, LitSchema)
        assert len(errors) == 1
        assert "Literal" in errors[0]
    def test_literal_field_accepts_int_values(self, tmp_path):
        """Literal[1, 2] 接受允许整数值。"""
        class LitSchema(TypedDict):
            code: Literal[1, 2]
        assert validate_payload({"code": 2}, LitSchema) == []
        errors = validate_payload({"code": 3}, LitSchema)
        assert len(errors) == 1
