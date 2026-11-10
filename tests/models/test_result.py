"""TaskResult 显式返回结果模型契约测试。"""

from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from enum import IntEnum
from pathlib import Path
from typing import Any

import pytest

from tasklite import TaskLiteError

import tasklite
from tasklite import TaskResult
from tasklite.models import TaskResult as ModelsTaskResult
from tasklite.models.result import (
    TaskResult as DirectTaskResult,
    validate_strict_json_object,
)


class _CustomStr(str):
    pass


class _CustomInt(int):
    pass


class _CustomFloat(float):
    pass


class _CustomList(list):  # type: ignore[type-arg]
    pass


class _CustomDict(dict):  # type: ignore[type-arg]
    pass


class _SampleIntEnum(IntEnum):
    ONE = 1


class TestTaskResultExportsAndConstruction:
    """导出面、禁止直接构造与仅关键字参数契约。"""

    def test_exported_from_root_and_models(self) -> None:
        assert TaskResult is ModelsTaskResult
        assert TaskResult is DirectTaskResult
        assert "TaskResult" in tasklite.__all__

    def test_direct_instantiation_forbidden(self) -> None:
        with pytest.raises(TaskLiteError, match="cannot be instantiated directly"):
            TaskResult()
        with pytest.raises(TaskLiteError, match="cannot be instantiated directly"):
            TaskResult("success")
        with pytest.raises(TaskLiteError, match="cannot be instantiated directly"):
            TaskResult(status="success")

    def test_positional_arguments_rejected(self) -> None:
        with pytest.raises(TypeError):
            TaskResult.success("done")  # type: ignore[misc]
        with pytest.raises(TypeError):
            TaskResult.failure("err")  # type: ignore[misc]
        with pytest.raises(TypeError):
            TaskResult.retry("later")  # type: ignore[misc]

    def test_success_defaults_and_fields(self) -> None:
        res = TaskResult.success()
        assert res.status == "success"
        assert res.message is None
        assert res.metadata == {}
        assert res.consume_budget is False

        res_with_args = TaskResult.success(
            message="downloaded",
            metadata={"count": 3, "items": ["a", "b"], "nested": {"ok": True}},
        )
        assert res_with_args.status == "success"
        assert res_with_args.message == "downloaded"
        assert res_with_args.metadata == {
            "count": 3,
            "items": ["a", "b"],
            "nested": {"ok": True},
        }
        assert res_with_args.consume_budget is False

    def test_failure_defaults_and_fields(self) -> None:
        res = TaskResult.failure()
        assert res.status == "failure"
        assert res.message is None
        assert res.metadata == {}
        assert res.consume_budget is False

        res_with_args = TaskResult.failure(
            message="checksum mismatch",
            metadata={"expected": "abc", "actual": "def"},
        )
        assert res_with_args.status == "failure"
        assert res_with_args.message == "checksum mismatch"
        assert res_with_args.metadata == {"expected": "abc", "actual": "def"}
        assert res_with_args.consume_budget is False

    def test_retry_defaults_and_consume_budget(self) -> None:
        res_default = TaskResult.retry()
        assert res_default.status == "retry"
        assert res_default.message is None
        assert res_default.metadata == {}
        assert res_default.consume_budget is True

        res_no_budget = TaskResult.retry(
            message="rate limited",
            metadata={"retry_after": 30.0},
            consume_budget=False,
        )
        assert res_no_budget.status == "retry"
        assert res_no_budget.message == "rate limited"
        assert res_no_budget.metadata == {"retry_after": 30.0}
        assert res_no_budget.consume_budget is False

    def test_consume_budget_only_on_retry_and_strict_bool(self) -> None:
        with pytest.raises(TypeError):
            TaskResult.success(consume_budget=False)  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            TaskResult.failure(consume_budget=False)  # type: ignore[call-arg]

        for bad_budget in (1, 0, None, "true", _CustomInt(1)):
            with pytest.raises(TaskLiteError, match="consume_budget"):
                TaskResult.retry(consume_budget=bad_budget)  # type: ignore[arg-type]


class TestTaskResultValidationAndImmutability:
    """message 与严格 JSON 对象 metadata 校验及防御性深拷贝。"""

    @pytest.mark.parametrize("bad_msg", [123, b"bytes", ["msg"], _CustomStr("sub")])
    def test_message_rejects_non_exact_str(self, bad_msg: Any) -> None:
        with pytest.raises(TaskLiteError, match="message"):
            TaskResult.success(message=bad_msg)

    def test_message_rejects_lone_surrogate(self) -> None:
        with pytest.raises(TaskLiteError, match="UTF-8"):
            TaskResult.failure(message="\ud800")

    @pytest.mark.parametrize(
        "bad_top",
        [
            [],
            ("a", 1),
            "not-a-dict",
            42,
            OrderedDict([("a", 1)]),
            _CustomDict({"a": 1}),
        ],
    )
    def test_metadata_top_level_must_be_exact_dict_or_none(self, bad_top: Any) -> None:
        with pytest.raises(TaskLiteError, match="metadata"):
            TaskResult.success(metadata=bad_top)

    @pytest.mark.parametrize("bad_key", [1, None, ("a",), _CustomStr("k")])
    def test_metadata_keys_must_be_exact_str(self, bad_key: Any) -> None:
        with pytest.raises(TaskLiteError, match="keys must be exact str"):
            TaskResult.success(metadata={bad_key: "v"})  # type: ignore[dict-item]

    def test_metadata_key_rejects_lone_surrogate(self) -> None:
        with pytest.raises(TaskLiteError, match="UTF-8"):
            TaskResult.success(metadata={"\ud800": 1})

    @pytest.mark.parametrize(
        "bad_val",
        [
            (1, 2),
            {1, 2},
            b"raw",
            Path("/tmp/a"),
            datetime.now(),
            object(),
            _CustomStr("s"),
            _CustomInt(1),
            _CustomFloat(1.5),
            _SampleIntEnum.ONE,
            _CustomList([1]),
            _CustomDict({"k": "v"}),
            OrderedDict([("k", "v")]),
        ],
    )
    def test_metadata_rejects_non_json_types_and_subclasses(self, bad_val: Any) -> None:
        with pytest.raises(TaskLiteError):
            TaskResult.success(metadata={"bad": bad_val})
        with pytest.raises(TaskLiteError):
            TaskResult.success(metadata={"nested": [bad_val]})

    @pytest.mark.parametrize("non_finite", [float("nan"), float("inf"), float("-inf")])
    def test_metadata_rejects_non_finite_floats(self, non_finite: float) -> None:
        with pytest.raises(TaskLiteError, match="finite float"):
            TaskResult.success(metadata={"score": non_finite})
        with pytest.raises(TaskLiteError, match="finite float"):
            TaskResult.retry(metadata={"nested": {"list": [non_finite]}})

    def test_metadata_defensive_copy_and_immutability(self) -> None:
        source: dict[str, Any] = {"nested": {"items": [1, 2]}}
        res = TaskResult.success(metadata=source)
        source["nested"]["items"].append(3)
        assert res.metadata == {"nested": {"items": [1, 2]}}

        read_copy = res.metadata
        read_copy["nested"]["items"].append(99)
        assert res.metadata == {"nested": {"items": [1, 2]}}

        with pytest.raises(AttributeError, match="immutable"):
            res.status = "failure"  # type: ignore[misc]
        with pytest.raises(AttributeError, match="immutable"):
            del res.message  # type: ignore[misc]

    def test_equality_and_repr(self) -> None:
        r1 = TaskResult.retry(message="wait", metadata={"k": 1}, consume_budget=False)
        r2 = TaskResult.retry(message="wait", metadata={"k": 1}, consume_budget=False)
        r3 = TaskResult.retry(message="wait", metadata={"k": 1}, consume_budget=True)
        assert r1 == r2
        assert r1 != r3
        assert "TaskResult.retry" in repr(r1)
        assert "TaskResult.success" in repr(TaskResult.success(message="ok"))
        assert validate_strict_json_object(None) == {}
