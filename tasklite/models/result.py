"""TaskResult：任务执行体显式返回结果值对象。

不变式：
- 禁止直接调用 ``TaskResult(...)`` 构造，唯一合法入口为三个仅关键字静态构造器：
  ``TaskResult.success``、``TaskResult.failure``、``TaskResult.retry``；
- ``metadata`` 顶层与递归结构必须满足严格 JSON 对象契约，拒绝隐式类型转换与非有限浮点数；
- ``consume_budget`` 仅在 ``TaskResult.retry(...)`` 上暴露，默认 ``True``。
"""

from __future__ import annotations

import copy
import math
from typing import Any, Literal

TaskResultStatus = Literal["success", "failure", "retry"]

_CONSTRUCT_TOKEN = object()
_VALID_STATUSES: frozenset[str] = frozenset({"success", "failure", "retry"})


def validate_strict_json_value(value: Any, *, path: str) -> Any:
    """递归校验严格 JSON 值并返回深拷贝副本。

    仅允许 ``str``、``int``、有限 ``float``、``bool``、``None``、``list``、``dict`` 精确类型；
    拒绝 ``tuple``、``set``、``bytes``、``Path``、``datetime``、自定义对象及内置类型子类。
    """
    if value is None:
        return None
    value_type = type(value)
    if value_type is bool:
        return value
    if value_type is int:
        return value
    if value_type is float:
        if not math.isfinite(value):
            raise ValueError(
                f"{path} must be a finite float; got {value!r}"
            )
        return value
    if value_type is str:
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError(
                f"{path} must be valid UTF-8 string"
            ) from exc
        return value
    if value_type is list:
        return [
            validate_strict_json_value(item, path=f"{path}[{idx}]")
            for idx, item in enumerate(value)
        ]
    if value_type is dict:
        out: dict[str, Any] = {}
        for k, v in value.items():
            if type(k) is not str:
                raise TypeError(
                    f"{path} keys must be exact str, got {type(k).__name__}"
                )
            try:
                k.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ValueError(
                    f"{path} key must be valid UTF-8 string"
                ) from exc
            out[k] = validate_strict_json_value(v, path=f"{path}.{k}")
        return out
    raise TypeError(
        f"{path} must be a strict JSON value (str, int, finite float, bool, None, list, dict); "
        f"got {value_type.__name__}"
    )


def validate_strict_json_object(
    metadata: Any, *, field_name: str = "metadata"
) -> dict[str, Any]:
    """校验顶层严格 JSON 对象（``None`` 归一为 ``{}``）并返回深拷贝字典。"""
    if metadata is None:
        return {}
    if type(metadata) is not dict:
        raise TypeError(
            f"{field_name} must be a dict or None, got {type(metadata).__name__}"
        )
    result: dict[str, Any] = validate_strict_json_value(metadata, path=field_name)
    return result


def _validate_message(message: Any) -> str | None:
    """校验可选结果消息（必须为 ``None`` 或精确 ``str`` 且合法 UTF-8）。"""
    if message is None:
        return None
    if type(message) is not str:
        raise TypeError(
            f"message must be a str or None, got {type(message).__name__}"
        )
    try:
        message.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("message must be a valid UTF-8 string") from exc
    return message


class TaskResult:
    """任务 handler 的唯一合法返回类型。"""

    __slots__ = ("_status", "_message", "_metadata", "_consume_budget")

    _status: TaskResultStatus
    _message: str | None
    _metadata: dict[str, Any]
    _consume_budget: bool

    def __init__(
        self,
        _token: object = None,
        *,
        status: TaskResultStatus = "success",
        message: str | None = None,
        metadata: dict[str, Any] | None = None,
        consume_budget: bool = False,
    ) -> None:
        if _token is not _CONSTRUCT_TOKEN:
            raise TypeError(
                "TaskResult cannot be instantiated directly; "
                "use TaskResult.success(), TaskResult.failure(), or TaskResult.retry()"
            )
        if status not in _VALID_STATUSES:
            raise ValueError(f"Invalid TaskResult status: {status!r}")
        if type(consume_budget) is not bool:
            raise TypeError(
                f"consume_budget must be a bool, got {type(consume_budget).__name__}"
            )
        validated_msg = _validate_message(message)
        validated_meta = validate_strict_json_object(metadata, field_name="metadata")
        object.__setattr__(self, "_status", status)
        object.__setattr__(self, "_message", validated_msg)
        object.__setattr__(self, "_metadata", validated_meta)
        object.__setattr__(self, "_consume_budget", consume_budget)

    @classmethod
    def success(
        cls,
        *,
        message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "TaskResult":
        """构造成功结果。"""
        return cls(
            _CONSTRUCT_TOKEN,
            status="success",
            message=message,
            metadata=metadata,
            consume_budget=False,
        )

    @classmethod
    def failure(
        cls,
        *,
        message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "TaskResult":
        """构造永久失败结果（不自动重试）。"""
        return cls(
            _CONSTRUCT_TOKEN,
            status="failure",
            message=message,
            metadata=metadata,
            consume_budget=False,
        )

    @classmethod
    def retry(
        cls,
        *,
        message: str | None = None,
        metadata: dict[str, Any] | None = None,
        consume_budget: bool = True,
    ) -> "TaskResult":
        """构造显式重试结果（``consume_budget`` 默认 ``True``）。"""
        return cls(
            _CONSTRUCT_TOKEN,
            status="retry",
            message=message,
            metadata=metadata,
            consume_budget=consume_budget,
        )

    @property
    def status(self) -> TaskResultStatus:
        """结果状态（``"success"`` | ``"failure"`` | ``"retry"``）。"""
        return self._status

    @property
    def message(self) -> str | None:
        """可选人类可读说明。"""
        return self._message

    @property
    def metadata(self) -> dict[str, Any]:
        """业务结果元数据副本（严格 JSON 对象，不含任何框架保留键）。"""
        return copy.deepcopy(self._metadata)

    @property
    def consume_budget(self) -> bool:
        """本次重试是否消耗当前 Run 的重试预算（非 retry 恒为 ``False``）。"""
        return self._consume_budget

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("TaskResult is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("TaskResult is immutable")

    def __eq__(self, other: object) -> bool:
        if type(other) is not TaskResult:
            return NotImplemented
        return (
            self._status == other._status
            and self._message == other._message
            and self._metadata == other._metadata
            and self._consume_budget == other._consume_budget
        )

    def __repr__(self) -> str:
        if self._status == "retry":
            return (
                f"TaskResult.retry(message={self._message!r}, "
                f"metadata={self._metadata!r}, "
                f"consume_budget={self._consume_budget!r})"
            )
        return (
            f"TaskResult.{self._status}(message={self._message!r}, "
            f"metadata={self._metadata!r})"
        )


__all__ = [
    "TaskResult",
    "TaskResultStatus",
    "validate_strict_json_object",
    "validate_strict_json_value",
]
