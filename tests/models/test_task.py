"""v2 Task 规格与 TaskRegistry 契约测试。

锁定契约：
- Task 为 frozen 规格：字段不可就地变异，默认值对齐 v2 Job 默认
  （max_retries=3 / timeout_seconds=3600 / timeout_is_transient=False）；
- 校验拆分后的入口拒绝矩阵：task_type / handler / payload_schema /
  resources / max_retries / timeout_seconds / timeout_is_transient；
- TaskRegistry：register/lookup 往返、重复注册显式报错、未注册 lookup
  显式报错。
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from tasklite.exceptions import TaskLiteError
from tasklite.models.task import Task, TaskRegistry


def _handler(job: Any, ctx: Any) -> bool:
    return True


class TestTaskCreation:
    """Task 构造与默认值。"""

    def test_create_with_minimal_args(self):
        """仅必填项构造：默认值对齐 Task 规格默认。"""
        task = Task(task_type="download", handler=_handler)
        assert task.task_type == "download"
        assert task.handler is _handler
        assert task.resources == {}
        assert task.max_retries_per_run == 3
        assert task.timeout_seconds == 3600
        assert task.trigger == "once"
        assert not hasattr(task, "payload_schema")
        assert not hasattr(task, "timeout_is_transient")

    def test_create_with_all_fields(self):
        """全字段显式构造。"""
        task = Task(
            task_type="encode",
            handler=_handler,
            resources={"gpu": 1.0},
            max_retries_per_run=5,
            timeout_seconds=7200,
            trigger="always",
        )
        assert task.resources == {"gpu": 1.0}
        assert task.max_retries_per_run == 5
        assert task.timeout_seconds == 7200
        assert task.trigger == "always"

    def test_config_fields_are_keyword_only(self):
        """task_type 与 handler 之后的配置参数全部 keyword-only，位置传参被拒。"""
        with pytest.raises(TypeError):
            Task("t", _handler, {"gpu": 1.0})  # type: ignore[misc]
        with pytest.raises(TypeError):
            Task("t", _handler, None, 3, 3600, "once")  # type: ignore[misc]

    def test_removed_and_job_only_fields_rejected(self):
        """已移除字段与仅属于 Job 的字段在 Task 构造时抛出 TypeError。"""
        for kw in (
            {"payload_schema": dict},
            {"max_retries": 3},
            {"timeout_is_transient": True},
            {"payload": {}},
            {"depends_on": []},
            {"files": {}},
        ):
            with pytest.raises(TypeError):
                Task("t", _handler, **kw)  # type: ignore[arg-type]

    def test_frozen_fields_reject_assignment(self):
        """Task 为 frozen 规格：字段赋值显式拒绝。"""
        task = Task(task_type="t", handler=_handler)
        with pytest.raises(FrozenInstanceError):
            task.max_retries_per_run = 9  # type: ignore[misc]

    def test_resources_copied_and_isolated(self):
        """默认资源浅拷贝隔离：注册后的外部就地变异不漂移规格。"""
        res = {"api": 1.0}
        task = Task(task_type="t", handler=_handler, resources=res)
        res["api"] = 99.0
        assert task.resources == {"api": 1.0}

    def test_resources_none_normalizes_to_empty(self):
        task = Task(task_type="t", handler=_handler, resources=None)
        assert task.resources == {}

    def test_equality_and_hash_on_spec_fields(self):
        """同规格 Task 相等（frozen dataclass 语义）。"""
        a = Task(task_type="t", handler=_handler)
        b = Task(task_type="t", handler=_handler)
        assert a == b
        assert a != Task(task_type="u", handler=_handler)


class TestTaskValidation:
    """Task 入口校验矩阵（fail-loud）。"""

    def test_task_type_rejected(self):
        """task_type 拒绝非 str / 空；含 ':' 或 '::' 保留原值。"""
        with pytest.raises(TaskLiteError, match="task_type must be a str"):
            Task(task_type=123, handler=_handler)
        with pytest.raises(TaskLiteError, match="non-empty str"):
            Task(task_type="", handler=_handler)
        task = Task(task_type="a::b", handler=_handler)
        assert task.task_type == "a::b"

    def test_handler_must_be_callable(self):
        with pytest.raises(TypeError, match="handler must be callable"):
            Task(task_type="t", handler="not_callable")

    def test_max_retries_per_run_matrix_rejected(self):
        """max_retries_per_run 拒绝非 int / bool / 负值。"""
        with pytest.raises(TypeError, match="max_retries_per_run must be an int"):
            Task(task_type="t", handler=_handler, max_retries_per_run="3")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="max_retries_per_run must be an int"):
            Task(task_type="t", handler=_handler, max_retries_per_run=True)
        with pytest.raises(ValueError, match="max_retries_per_run must be >= 0"):
            Task(task_type="t", handler=_handler, max_retries_per_run=-1)

    def test_max_retries_per_run_zero_allowed(self):
        """max_retries_per_run=0：任何失败直接进失败档案（合法边界）。"""
        task = Task(task_type="t", handler=_handler, max_retries_per_run=0)
        assert task.max_retries_per_run == 0

    def test_timeout_matrix_rejected(self):
        """timeout_seconds 拒绝非数值 / bool / 零 / 负值 / NaN / Inf / 超大 int。"""
        with pytest.raises(TypeError, match="timeout_seconds must be a number"):
            Task(task_type="t", handler=_handler, timeout_seconds="60")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="timeout_seconds must be a number"):
            Task(task_type="t", handler=_handler, timeout_seconds=True)
        for bad in (0, -1, float("nan"), float("inf"), float("-inf"), 10**400):
            with pytest.raises(ValueError, match="timeout_seconds must be finite and > 0"):
                Task(task_type="t", handler=_handler, timeout_seconds=bad)

    def test_timeout_float_accepted(self):
        task = Task(task_type="t", handler=_handler, timeout_seconds=1.5)
        assert task.timeout_seconds == 1.5

    def test_resources_amount_matrix_rejected(self):
        """默认资源 amount 拒绝矩阵——bool/NaN/Inf/负值/超大 int/非数值。"""
        with pytest.raises(TypeError, match="must be a number"):
            Task(task_type="t", handler=_handler, resources={"cpu": True})
        with pytest.raises(TypeError, match="must be a number"):
            Task(task_type="t", handler=_handler, resources={"cpu": "high"})
        with pytest.raises(ValueError, match="must be finite|too large"):
            Task(task_type="t", handler=_handler, resources={"cpu": float("nan")})
        with pytest.raises(ValueError, match="must be finite|too large"):
            Task(task_type="t", handler=_handler, resources={"cpu": float("inf")})
        with pytest.raises(ValueError, match="must be finite|too large"):
            Task(task_type="t", handler=_handler, resources={"cpu": 10**400})
        with pytest.raises(ValueError, match="must be non-negative"):
            Task(task_type="t", handler=_handler, resources={"cpu": -1.0})

    def test_resources_container_and_name_rejected(self):
        """默认资源非 dict / 非 str 资源名 / 空资源名拒绝。"""
        with pytest.raises(TypeError, match="must be a dict"):
            Task(task_type="t", handler=_handler, resources=["cpu", 1.0])
        with pytest.raises(TypeError, match="must be a non-empty str"):
            Task(task_type="t", handler=_handler, resources={1: 2.0})
        with pytest.raises(TypeError, match="must be a non-empty str"):
            Task(task_type="t", handler=_handler, resources={"": 2.0})


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
        second = Task(task_type="b", handler=_handler, max_retries_per_run=0)
        reg.register(first)
        reg.register(second)
        assert reg.lookup("a") is first
        assert reg.lookup("b") is second
        assert reg.lookup("b").max_retries_per_run == 0
