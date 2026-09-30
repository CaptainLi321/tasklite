"""v2 Task 规格与 TaskRegistry 契约测试。

锁定契约：
- Task 为 frozen 规格：字段不可就地变异，默认值对齐 v2 Job 默认
  （max_retries=3 / timeout=3600 / timeout_is_transient=False）；
- 校验拆分后的入口拒绝矩阵：task_type / handler / payload_schema /
  default_resources / max_retries / timeout / timeout_is_transient；
- TaskRegistry：register/lookup 往返、重复注册显式报错、未注册 lookup
  显式报错。
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from tasklite.v2.models.task import Task, TaskRegistry


def _handler(job: Any, ctx: Any) -> bool:
    return True


class TestTaskCreation:
    """Task 构造与默认值。"""

    def test_create_with_minimal_args(self):
        """仅必填项构造：默认值对齐 v2 Job 默认。"""
        task = Task(task_type="download", handler=_handler)
        assert task.task_type == "download"
        assert task.handler is _handler
        assert task.default_resources == {}
        assert task.payload_schema is None
        assert task.max_retries == 3
        assert task.timeout == 3600
        assert task.timeout_is_transient is False

    def test_create_with_all_fields(self):
        """全字段显式构造。"""
        task = Task(
            task_type="encode",
            handler=_handler,
            default_resources={"gpu": 1.0},
            payload_schema=dict,
            max_retries=5,
            timeout=7200,
            timeout_is_transient=True,
        )
        assert task.default_resources == {"gpu": 1.0}
        assert task.payload_schema is dict
        assert task.max_retries == 5
        assert task.timeout == 7200
        assert task.timeout_is_transient is True

    def test_frozen_fields_reject_assignment(self):
        """Task 为 frozen 规格：字段赋值显式拒绝。"""
        task = Task(task_type="t", handler=_handler)
        with pytest.raises(FrozenInstanceError):
            task.max_retries = 9  # type: ignore[misc]

    def test_default_resources_copied_and_isolated(self):
        """默认资源浅拷贝隔离：注册后的外部就地变异不漂移规格。"""
        res = {"api": 1.0}
        task = Task(task_type="t", handler=_handler, default_resources=res)
        res["api"] = 99.0
        assert task.default_resources == {"api": 1.0}

    def test_default_resources_none_normalizes_to_empty(self):
        task = Task(task_type="t", handler=_handler, default_resources=None)
        assert task.default_resources == {}

    def test_equality_and_hash_on_spec_fields(self):
        """同规格 Task 相等（frozen dataclass 语义）。"""
        a = Task(task_type="t", handler=_handler)
        b = Task(task_type="t", handler=_handler)
        assert a == b
        assert a != Task(task_type="u", handler=_handler)


class TestTaskValidation:
    """Task 入口校验矩阵（fail-loud）。"""

    def test_task_type_rejected(self):
        """task_type 拒绝非 str / 空 / 含 '::'。"""
        with pytest.raises(TypeError, match="task_type must be a str"):
            Task(task_type=123, handler=_handler)
        with pytest.raises(ValueError, match="non-empty str"):
            Task(task_type="", handler=_handler)
        with pytest.raises(ValueError, match="must not contain '::'"):
            Task(task_type="a::b", handler=_handler)

    def test_handler_must_be_callable(self):
        with pytest.raises(TypeError, match="handler must be callable"):
            Task(task_type="t", handler="not_callable")

    def test_payload_schema_must_be_type_or_none(self):
        """payload_schema 传实例/字符串在运行时校验会静默失效，入口拒绝。"""
        with pytest.raises(TypeError, match="payload_schema must be a type"):
            Task(task_type="t", handler=_handler, payload_schema={"k": str})
        with pytest.raises(TypeError, match="payload_schema must be a type"):
            Task(task_type="t", handler=_handler, payload_schema="TypedDictName")

    def test_max_retries_matrix_rejected(self):
        """max_retries 拒绝非 int / bool / 负值。"""
        with pytest.raises(TypeError, match="max_retries must be an int"):
            Task(task_type="t", handler=_handler, max_retries="3")
        with pytest.raises(TypeError, match="max_retries must be an int"):
            Task(task_type="t", handler=_handler, max_retries=True)
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            Task(task_type="t", handler=_handler, max_retries=-1)

    def test_max_retries_zero_allowed(self):
        """max_retries=0：任何失败直接进失败档案（合法边界）。"""
        task = Task(task_type="t", handler=_handler, max_retries=0)
        assert task.max_retries == 0

    def test_timeout_matrix_rejected(self):
        """timeout 拒绝非数值 / bool / 零 / 负值 / NaN / Inf / 超大 int。"""
        with pytest.raises(TypeError, match="timeout must be a number"):
            Task(task_type="t", handler=_handler, timeout="60")
        with pytest.raises(TypeError, match="timeout must be a number"):
            Task(task_type="t", handler=_handler, timeout=True)
        for bad in (0, -1, float("nan"), float("inf"), 10**400):
            with pytest.raises(ValueError, match="timeout must be finite and > 0"):
                Task(task_type="t", handler=_handler, timeout=bad)

    def test_timeout_float_accepted(self):
        task = Task(task_type="t", handler=_handler, timeout=1.5)
        assert task.timeout == 1.5

    def test_timeout_is_transient_must_be_bool(self):
        """非 bool 真值会静默改变超时归类语义，入口拒绝。"""
        with pytest.raises(TypeError, match="timeout_is_transient must be a bool"):
            Task(task_type="t", handler=_handler, timeout_is_transient="yes")
        with pytest.raises(TypeError, match="timeout_is_transient must be a bool"):
            Task(task_type="t", handler=_handler, timeout_is_transient=1)

    def test_default_resources_amount_matrix_rejected(self):
        """默认资源 amount 拒绝矩阵——bool/NaN/Inf/负值/超大 int/非数值。"""
        with pytest.raises(TypeError, match="must be a number"):
            Task(task_type="t", handler=_handler, default_resources={"cpu": True})
        with pytest.raises(TypeError, match="must be a number"):
            Task(task_type="t", handler=_handler, default_resources={"cpu": "high"})
        with pytest.raises(ValueError, match="must be finite|too large"):
            Task(task_type="t", handler=_handler, default_resources={"cpu": float("nan")})
        with pytest.raises(ValueError, match="must be finite|too large"):
            Task(task_type="t", handler=_handler, default_resources={"cpu": float("inf")})
        with pytest.raises(ValueError, match="must be finite|too large"):
            Task(task_type="t", handler=_handler, default_resources={"cpu": 10**400})
        with pytest.raises(ValueError, match="must be non-negative"):
            Task(task_type="t", handler=_handler, default_resources={"cpu": -1.0})

    def test_default_resources_container_and_name_rejected(self):
        """默认资源非 dict / 非 str 资源名 / 空资源名拒绝。"""
        with pytest.raises(TypeError, match="must be a dict"):
            Task(task_type="t", handler=_handler, default_resources=["cpu", 1.0])
        with pytest.raises(TypeError, match="must be a non-empty str"):
            Task(task_type="t", handler=_handler, default_resources={1: 2.0})
        with pytest.raises(TypeError, match="must be a non-empty str"):
            Task(task_type="t", handler=_handler, default_resources={"": 2.0})


class TestTaskRegistry:
    """TaskRegistry 注册/查询契约。"""

    def test_register_and_lookup_roundtrip(self):
        reg = TaskRegistry()
        task = Task(task_type="scan", handler=_handler)
        reg.register(task)
        assert reg.lookup("scan") is task

    def test_duplicate_registration_rejected(self):
        """重复注册显式报错：静默覆盖会让已入队 Job 挂到新 handler。"""
        reg = TaskRegistry()
        reg.register(Task(task_type="scan", handler=_handler))
        with pytest.raises(ValueError, match="already registered"):
            reg.register(Task(task_type="scan", handler=_handler))

    def test_unknown_lookup_raises_keyerror(self):
        """未注册 lookup 显式 KeyError：拼错 task_type 的 Job 不得静默无人处理。"""
        reg = TaskRegistry()
        with pytest.raises(KeyError, match="unknown task_type"):
            reg.lookup("ghost")

    def test_lookup_rejects_non_str(self):
        reg = TaskRegistry()
        with pytest.raises(TypeError, match="task_type must be a str"):
            reg.lookup(123)

    def test_register_rejects_non_task(self):
        reg = TaskRegistry()
        with pytest.raises(TypeError, match="must be a Task instance"):
            reg.register(("scan", _handler))

    def test_contains_and_task_types(self):
        reg = TaskRegistry()
        reg.register(Task(task_type="a", handler=_handler))
        reg.register(Task(task_type="b", handler=_handler))
        assert "a" in reg
        assert "b" in reg
        assert "c" not in reg
        assert 123 not in reg
        assert reg.task_types() == ("a", "b")

    def test_distinct_registrations_coexist(self):
        reg = TaskRegistry()
        first = Task(task_type="a", handler=_handler)
        second = Task(task_type="b", handler=_handler, max_retries=0)
        reg.register(first)
        reg.register(second)
        assert reg.lookup("a") is first
        assert reg.lookup("b") is second
        assert reg.lookup("b").max_retries == 0
