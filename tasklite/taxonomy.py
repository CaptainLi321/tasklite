"""统一错误分类、异常判定、载荷校验与 DLQ 元数据规范化的深模块。

本模块是整个框架对于「错误与异常」的单一真相源（Single Source of Truth），
将散落在异常继承体系、DLQ 错误码匹配与载荷校验中的规则收敛为高局部性、高杠杆的深模块。
"""

from __future__ import annotations

import datetime
import math
import signal
import types
import typing
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type, Union

from .exceptions import FatalError, RateLimitHit, RetryError

_UnionType = getattr(types, "UnionType", None)

# ── 框架错误码常量 ────────────────────────────────────────────────────────
ERR_DEPENDENCY_DEADLOCK = "DEPENDENCY_DEADLOCK"
ERR_JOB_DEPENDENCY = "JOB_DEPENDENCY"
ERR_PAYLOAD_VALIDATION = "PAYLOAD_VALIDATION_FAILED"
ERR_MAX_RETRIES = "MAX_RETRIES_EXCEEDED"
ERR_NO_HANDLER = "NO_HANDLER"
ERR_RESOURCE_DEADLOCK = "RESOURCE_DEADLOCK"
ERR_MALFORMED_JOB = "MALFORMED_JOB"
ERR_COMMIT_FAILURE_DLQ = "COMMIT_FAILURE_DLQ"
ERR_DISPATCH_FAILURE = "DISPATCH_FAILURE"
ERR_DEADLOCK_GAP = "DEADLOCK_CLASSIFICATION_GAP"

# ── 子进程死亡归因错误串前缀（attribute_process_death 决策表产出，
#    与 _classify_string 映射同源；DLQ 落盘串按前缀匹配分类） ─────────────
ERR_TIMEOUT_PREFIX = "TIMEOUT ("
ERR_PROCESS_CRASH_PREFIX = "PROCESS_CRASH_EXITCODE_"
ERR_NO_IPC_RESULT = "NO_IPC_RESULT"
ERR_PROCESS_SIGNAL_DEATH = "PROCESS_SIGNAL_DEATH"
ERR_IPC_WRITE_DEGRADED_PREFIX = "IPC_RESULT_WRITE_DEGRADED"

# ── DLQ error_type 分类常量 ──────────────────────────────────────────────
ERROR_TYPE_FATAL = "fatal"
ERROR_TYPE_TRANSIENT_EXHAUSTED = "transient_exhausted"
ERROR_TYPE_DEPENDENCY = "dependency"
ERROR_TYPE_DEADLOCK = "deadlock"
ERROR_TYPE_NO_HANDLER = "no_handler"
ERROR_TYPE_VALIDATION = "validation"
ERROR_TYPE_COMMIT_FAILURE = "commit_failure"
ERROR_TYPE_DISPATCH = "dispatch"
ERROR_TYPE_UNKNOWN = "unknown"

# ── 默认内置启发式异常元组 ────────────────────────────────────────────────
FATAL_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    TypeError,
    KeyError,
    AttributeError,
    IndexError,
    StopIteration,
    ArithmeticError,
    ImportError,
    NotImplementedError,
    RecursionError,
)

TRANSIENT_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    ConnectionRefusedError,
    ConnectionResetError,
    ConnectionAbortedError,
    BrokenPipeError,
)


class ErrorCategory(str, Enum):
    """错误大类（权威单一事实来源）。"""
    FATAL = "fatal"
    TRANSIENT_EXHAUSTED = "transient_exhausted"
    DEPENDENCY = "dependency"
    DEADLOCK = "deadlock"
    NO_HANDLER = "no_handler"
    VALIDATION = "validation"
    COMMIT_FAILURE = "commit_failure"
    DISPATCH = "dispatch"
    INTERRUPTED = "interrupted"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ValidationErrorItem:
    """单个校验缺陷的结构化不可变描述。"""
    field_path: str
    code: str
    expected: str
    actual: str
    message: str


@dataclass(frozen=True)
class ValidationResult:
    """载荷/资源输入校验结果的不可变值对象。"""
    is_valid: bool
    errors: Tuple[ValidationErrorItem, ...] = field(default_factory=tuple)

    def as_error_strings(self) -> List[str]:
        """向后兼容：导出 list[str] 错误字符串格式。"""
        return [e.message for e in self.errors]

    def to_diagnostic_meta(self) -> Dict[str, Any]:
        """导出结构化 DLQ 诊断字典。"""
        return {
            "error_count": len(self.errors),
            "details": [
                {
                    "field": e.field_path,
                    "code": e.code,
                    "expected": e.expected,
                    "actual": e.actual,
                    "message": e.message,
                }
                for e in self.errors
            ],
            "summary": self.as_error_strings(),
        }


@dataclass(frozen=True)
class ErrorClassification:
    """错误多维分类决策的不可变值对象（单一真相源）。"""
    category: ErrorCategory
    error_code: str
    dlq_error_type: str
    is_transient: bool
    is_fatal: bool
    is_retry: bool
    is_interrupted: bool = False
    lock_conflict: bool = False
    retry_error: Optional[str] = None
    traceback_str: Optional[str] = None
    raw_error: str = ""
    diagnostic_details: Optional[Dict[str, Any]] = None

    def to_dlq_meta(self, *, attempt: Optional[int] = None) -> Dict[str, Any]:
        """构建落入 failed_dlq 表的标准元数据字典。"""
        meta: Dict[str, Any] = {
            "error": self.error_code or self.raw_error or "UNKNOWN_ERROR",
            "error_type": self.dlq_error_type,
            "fatal": self.is_fatal,
        }
        if self.diagnostic_details:
            meta.update(self.diagnostic_details)
        if self.traceback_str:
            meta["traceback"] = self.traceback_str
        if attempt is not None:
            meta["_attempt"] = attempt
        return meta


@dataclass(frozen=True)
class DeathAttribution:
    """子进程死亡归因决策表的行值（不可变）。

    attribute_process_death 是「exitcode × 超时 × timeout_is_transient →
    瞬态/终局、result_meta、retry_error、dlq_error_type」的单一裁决点；
    收割路径（有结果文件解码、无结果文件终局构造、stale 认领）对同一
    根因必须经本表得到相同形状。
    """

    retry_requested: bool
    result_meta: Dict[str, Any]
    retry_error: Optional[str]
    dlq_error_type: str


def _safe_str(obj: Any) -> str:
    """``str()`` 的 Never-Raise 变体：``__str__`` 抛异常的对象降级为类型占位串。

    不变式：classify()/validate_payload()/normalize_dlq_meta() 标注
    Never-Raise 契约，raw_error/expected 等字段对不可信对象取串必须吞掉
    用户 ``__str__`` 的任意异常——分类边界内取串失败不得逸出
    （子进程分类路径无外层兜底，逸出即 worker 无结果文件崩溃）。
    """
    try:
        return str(obj)
    except Exception:
        return f"<unprintable {type(obj).__name__}>"


def _ensure_exception_class(exception_cls: type, api_name: str) -> None:
    """分类序列成员底座校验（fail-loud）：必须是 Exception 子类。"""
    if not isinstance(exception_cls, type) or not issubclass(exception_cls, Exception):
        raise TypeError(
            f"{api_name} requires an Exception subclass, got {exception_cls!r}"
        )


def _validate_policy_exception_class(exception_cls: type, api_name: str) -> None:
    """异常分类声明入口校验（fail-loud）——注册表与构造器元组两条声明路径共用。

    不变式：声明类必须可 pickle——分类决策在 spawn 子进程发生，
    声明元组随 ctx pickle 下发，非模块级类会让 spawn 派发整体失败。
    """
    _ensure_exception_class(exception_cls, api_name)
    if issubclass(exception_cls, (RetryError, FatalError)):
        raise TypeError(
            f"{api_name} cannot register a "
            f"{'RetryError' if issubclass(exception_cls, RetryError) else 'FatalError'} "
            f"subclass ({exception_cls!r}) — these have dedicated except branches "
            f"that bypass the registry; registration would silently no-op."
        )
    try:
        import pickle
        pickle.dumps(exception_cls)
    except (pickle.PicklingError, AttributeError, TypeError) as e:
        raise TypeError(
            f"{api_name} requires a module-level (picklable) "
            f"Exception class for spawn-subprocess propagation, got {exception_cls!r}: {e}"
        ) from e


def _validate_transient_class(exception_cls: type) -> None:
    """注册入口校验（fail-loud）——per-pipeline 注册表与外部直调共用。"""
    _validate_policy_exception_class(exception_cls, "register_transient_exception")


def validate_declared_exception_classes(
    classes: Optional[Sequence[type]], param_name: str
) -> None:
    """构造器声明的 fatal/transient 异常元组入口校验（fail-loud）。

    与注册表路径同规：非 Exception / RetryError-FatalError 子类 /
    不可 pickle 类在构造期即拒绝，而非滞后到 spawn 派发才失败。
    """
    for exception_cls in tuple(classes) if classes is not None else ():
        _validate_policy_exception_class(exception_cls, param_name)


class ErrorTaxonomy:
    """错误归因、异常判定、输入防御与元数据规范化的深模块。"""

    def __init__(
        self,
        *,
        fatal_exceptions: Optional[Sequence[Type[BaseException]]] = None,
        transient_exceptions: Optional[Sequence[Type[BaseException]]] = None,
        transient_registry: Optional[Sequence[Type[BaseException]]] = None,
    ) -> None:
        # 构造期 fail-loud（Never-Raise 契约前置）：与注册路径同规——
        # 专用分支异常类（RetryError/FatalError 子类，注册表命中先于
        # fatal 判定会把 fatal 翻转为可重试）与不可 pickle 类（声明元组
        # 随 ctx pickle 下发，spawn 派发期才失败）在构造期即拒绝。
        # 不变式：入参先一次性物化，物化结果复用于校验与赋值——若校验先行
        # 消费一次性迭代器（生成器/iterator）而赋值处二次物化，用户策略会
        # 静默变空且不回退内置默认。
        fatal_seq = tuple(fatal_exceptions) if fatal_exceptions is not None else None
        transient_seq = (
            tuple(transient_exceptions) if transient_exceptions is not None else None
        )
        registry_seq = (
            list(transient_registry) if transient_registry is not None else None
        )
        validate_declared_exception_classes(fatal_seq, "fatal_exceptions")
        validate_declared_exception_classes(transient_seq, "transient_exceptions")
        validate_declared_exception_classes(registry_seq, "transient_registry")
        self._fatal_exceptions: Tuple[Type[BaseException], ...] = (
            fatal_seq if fatal_seq is not None else FATAL_EXCEPTIONS
        )
        self._transient_exceptions: Tuple[Type[BaseException], ...] = (
            transient_seq if transient_seq is not None else TRANSIENT_EXCEPTIONS
        )
        self._registry: List[Type[BaseException]] = (
            registry_seq if registry_seq is not None else []
        )

    # ── 1. 统一分类接缝 ──────────────────────────────────────────────────

    def classify(
        self,
        target: Union[BaseException, ValidationResult, Dict[str, Any], str, None],
    ) -> ErrorClassification:
        """错误分类单一入口（Never-Raise 契约保证）。

        进程死亡归因不经本入口——收割侧统一走 attribute_process_death
        决策表（exitcode 归因与异常/字符串分类是两套正交裁决）。
        """
        # 1. 校验失败对象
        if isinstance(target, ValidationResult):
            return ErrorClassification(
                category=ErrorCategory.VALIDATION,
                error_code=ERR_PAYLOAD_VALIDATION,
                dlq_error_type=ERROR_TYPE_VALIDATION,
                is_transient=False,
                is_fatal=False,
                is_retry=False,
                raw_error=f"Validation failed: {len(target.errors)} issue(s)",
                diagnostic_details=target.to_diagnostic_meta(),
            )

        # 2. Python 异常实例
        if isinstance(target, BaseException):
            return self._classify_exception(target)

        # 3. 字典（IPC 字典或 DLQ meta 字典）
        if isinstance(target, dict):
            return self._classify_dict(target)

        # 5. 字符串（错误码或通用文本）
        if isinstance(target, str):
            return self._classify_string(target)

        # 6. None 或其他未知标量
        return ErrorClassification(
            category=ErrorCategory.UNKNOWN,
            error_code="",
            dlq_error_type=ERROR_TYPE_UNKNOWN,
            is_transient=False,
            is_fatal=False,
            is_retry=False,
            raw_error=_safe_str(target) if target is not None else "",
        )

    def _classify_exception(self, exc: BaseException) -> ErrorClassification:
        msg = _safe_str(exc)
        raw_err = f"{type(exc).__name__}: {msg}" if msg else type(exc).__name__
        is_retry = isinstance(exc, RetryError)
        is_fatal = isinstance(exc, FatalError)
        is_interrupted = isinstance(exc, KeyboardInterrupt)

        # 用户注册瞬态 > Fatal 启发式 > Transient 启发式
        in_reg = self.matches(exc)
        in_fatal = isinstance(exc, self._fatal_exceptions)
        in_transient = isinstance(exc, self._transient_exceptions)

        if is_interrupted:
            return ErrorClassification(
                category=ErrorCategory.INTERRUPTED,
                error_code="INTERRUPTED",
                dlq_error_type=ERROR_TYPE_UNKNOWN,
                is_transient=False,
                is_fatal=False,
                is_retry=False,
                is_interrupted=True,
                raw_error=raw_err,
            )

        if is_retry or in_reg:
            return ErrorClassification(
                category=ErrorCategory.TRANSIENT_EXHAUSTED,
                error_code=ERR_MAX_RETRIES,
                dlq_error_type=ERROR_TYPE_TRANSIENT_EXHAUSTED,
                is_transient=True,
                is_fatal=False,
                is_retry=True,
                raw_error=raw_err,
            )

        if is_fatal or in_fatal:
            return ErrorClassification(
                category=ErrorCategory.FATAL,
                error_code="",
                dlq_error_type=ERROR_TYPE_FATAL,
                is_transient=False,
                is_fatal=True,
                is_retry=False,
                raw_error=raw_err,
            )

        if in_transient:
            return ErrorClassification(
                category=ErrorCategory.TRANSIENT_EXHAUSTED,
                error_code=ERR_MAX_RETRIES,
                dlq_error_type=ERROR_TYPE_TRANSIENT_EXHAUSTED,
                is_transient=True,
                is_fatal=False,
                is_retry=False,
                raw_error=raw_err,
            )

        return ErrorClassification(
            category=ErrorCategory.UNKNOWN,
            error_code="",
            dlq_error_type=ERROR_TYPE_UNKNOWN,
            is_transient=False,
            is_fatal=False,
            is_retry=False,
            raw_error=raw_err,
        )

    def attribute_process_death(
        self,
        exitcode: Optional[int],
        *,
        timed_out: bool,
        timeout_seconds: float = 0.0,
        timeout_is_transient: bool = False,
    ) -> DeathAttribution:
        """子进程死亡归因决策表（Never-Raise 契约）。

        收割侧的唯一裁决点：有结果文件解码路径与无结果文件终局构造路径
        对同一 exitcode 必须得到相同的 meta 形状与错误串。决策列：

        - timed_out ∧ timeout_is_transient → 瞬态重试（meta 留空，仅 retry_error）
        - timed_out ∧ ¬timeout_is_transient → 终局（DLQ 落 fatal 型）
        - exitcode < 0 → 信号死亡（signal/oom_hint 结构化键，烧预算瞬态）
        - exitcode > 0 → 解释器/导入段崩溃（烧预算瞬态）
        - exitcode ∈ {None, 0} → 无结果文件（烧预算瞬态）
        """
        if timed_out:
            if timeout_is_transient:
                return DeathAttribution(
                    retry_requested=True,
                    result_meta={},
                    retry_error=f"{ERR_TIMEOUT_PREFIX}{timeout_seconds}s)",
                    dlq_error_type=ERROR_TYPE_TRANSIENT_EXHAUSTED,
                )
            return DeathAttribution(
                retry_requested=False,
                result_meta={"error": f"{ERR_TIMEOUT_PREFIX}{timeout_seconds}s)"},
                retry_error=None,
                dlq_error_type=ERROR_TYPE_FATAL,
            )
        if exitcode is not None and exitcode < 0:
            try:
                sig_name = signal.Signals(-exitcode).name
            except (ValueError, AttributeError):
                sig_name = f"SIGNO{-exitcode}"
            return DeathAttribution(
                retry_requested=True,
                result_meta={
                    "error": f"{ERR_PROCESS_CRASH_PREFIX}{exitcode}",
                    "signal": sig_name,
                    "oom_hint": -exitcode == int(signal.SIGKILL),
                },
                retry_error=f"{ERR_PROCESS_SIGNAL_DEATH}: killed by {sig_name} ({exitcode})",
                dlq_error_type=ERROR_TYPE_TRANSIENT_EXHAUSTED,
            )
        if exitcode:
            return DeathAttribution(
                retry_requested=True,
                result_meta={"error": f"{ERR_PROCESS_CRASH_PREFIX}{exitcode}"},
                retry_error=f"PROCESS_CRASH_EXIT: worker exited with code {exitcode}",
                dlq_error_type=ERROR_TYPE_TRANSIENT_EXHAUSTED,
            )
        return DeathAttribution(
            retry_requested=True,
            result_meta={"error": ERR_NO_IPC_RESULT},
            retry_error=f"{ERR_NO_IPC_RESULT}: worker exited without writing a result file",
            dlq_error_type=ERROR_TYPE_TRANSIENT_EXHAUSTED,
        )

    def _classify_dict(self, meta: Dict[str, Any]) -> ErrorClassification:
        # IPC 状态字典
        status = meta.get("status")
        if status == "retry":
            return ErrorClassification(
                category=ErrorCategory.TRANSIENT_EXHAUSTED,
                error_code=ERR_MAX_RETRIES,
                dlq_error_type=ERROR_TYPE_TRANSIENT_EXHAUSTED,
                is_transient=True,
                is_fatal=False,
                is_retry=True,
                lock_conflict=bool(meta.get("lock_conflict")),
                raw_error=_safe_str(meta.get("error", "")),
                traceback_str=meta.get("traceback"),
            )
        if status == "fatal":
            return ErrorClassification(
                category=ErrorCategory.FATAL,
                error_code="",
                dlq_error_type=ERROR_TYPE_FATAL,
                is_transient=False,
                is_fatal=True,
                is_retry=False,
                raw_error=_safe_str(meta.get("error", "")),
                traceback_str=meta.get("traceback"),
            )
        if status == "interrupted":
            return ErrorClassification(
                category=ErrorCategory.INTERRUPTED,
                error_code="INTERRUPTED",
                dlq_error_type=ERROR_TYPE_UNKNOWN,
                is_transient=False,
                is_fatal=False,
                is_retry=False,
                is_interrupted=True,
                raw_error=_safe_str(meta.get("error", "")),
            )

        # DLQ meta 字典
        is_fatal = bool(meta.get("fatal"))
        error_str = _safe_str(meta.get("error", ""))
        dlq_type = meta.get("error_type")

        if is_fatal:
            return ErrorClassification(
                category=ErrorCategory.FATAL,
                error_code=error_str,
                dlq_error_type=ERROR_TYPE_FATAL,
                is_transient=False,
                is_fatal=True,
                is_retry=False,
                raw_error=error_str,
                traceback_str=meta.get("traceback"),
            )

        # 映射错误码
        cl = self._classify_string(error_str)
        if dlq_type and dlq_type != ERROR_TYPE_UNKNOWN:
            return ErrorClassification(
                category=cl.category,
                error_code=error_str,
                dlq_error_type=dlq_type,
                is_transient=cl.is_transient,
                is_fatal=cl.is_fatal,
                is_retry=cl.is_retry,
                raw_error=error_str,
                traceback_str=meta.get("traceback"),
            )
        return cl

    def _classify_string(self, error: str) -> ErrorClassification:
        mapping: Tuple[Tuple[str, ErrorCategory, str, bool, bool], ...] = (
            (ERR_DEPENDENCY_DEADLOCK, ErrorCategory.DEADLOCK, ERROR_TYPE_DEADLOCK, False, False),
            (ERR_RESOURCE_DEADLOCK, ErrorCategory.DEADLOCK, ERROR_TYPE_DEADLOCK, False, False),
            (ERR_MALFORMED_JOB, ErrorCategory.DEADLOCK, ERROR_TYPE_DEADLOCK, False, False),
            (ERR_DEADLOCK_GAP, ErrorCategory.DEADLOCK, ERROR_TYPE_DEADLOCK, False, False),
            (ERR_JOB_DEPENDENCY, ErrorCategory.DEPENDENCY, ERROR_TYPE_DEPENDENCY, False, False),
            (ERR_MAX_RETRIES, ErrorCategory.TRANSIENT_EXHAUSTED, ERROR_TYPE_TRANSIENT_EXHAUSTED, False, False),
            (ERR_NO_HANDLER, ErrorCategory.NO_HANDLER, ERROR_TYPE_NO_HANDLER, False, True),
            (ERR_PAYLOAD_VALIDATION, ErrorCategory.VALIDATION, ERROR_TYPE_VALIDATION, False, False),
            # 子进程死亡族（与 attribute_process_death 产出串同源）——
            # 非瞬态超时终局落 fatal 型；其余死亡/降级串落瞬态型
            (ERR_TIMEOUT_PREFIX, ErrorCategory.FATAL, ERROR_TYPE_FATAL, False, True),
            (ERR_PROCESS_CRASH_PREFIX, ErrorCategory.TRANSIENT_EXHAUSTED, ERROR_TYPE_TRANSIENT_EXHAUSTED, True, False),
            (ERR_NO_IPC_RESULT, ErrorCategory.TRANSIENT_EXHAUSTED, ERROR_TYPE_TRANSIENT_EXHAUSTED, True, False),
            (ERR_PROCESS_SIGNAL_DEATH, ErrorCategory.TRANSIENT_EXHAUSTED, ERROR_TYPE_TRANSIENT_EXHAUSTED, True, False),
            (ERR_IPC_WRITE_DEGRADED_PREFIX, ErrorCategory.TRANSIENT_EXHAUSTED, ERROR_TYPE_TRANSIENT_EXHAUSTED, True, False),
        )
        for code, cat, etype, trans, fatal in mapping:
            if error == code or error.startswith(code):
                return ErrorClassification(
                    category=cat,
                    error_code=code,
                    dlq_error_type=etype,
                    is_transient=trans,
                    is_fatal=fatal,
                    is_retry=False,
                    raw_error=error,
                )

        if error.startswith(ERR_COMMIT_FAILURE_DLQ):
            return ErrorClassification(
                category=ErrorCategory.COMMIT_FAILURE,
                error_code=ERR_COMMIT_FAILURE_DLQ,
                dlq_error_type=ERROR_TYPE_COMMIT_FAILURE,
                is_transient=False,
                is_fatal=False,
                is_retry=False,
                raw_error=error,
            )

        if error.startswith(ERR_DISPATCH_FAILURE):
            return ErrorClassification(
                category=ErrorCategory.DISPATCH,
                error_code=ERR_DISPATCH_FAILURE,
                dlq_error_type=ERROR_TYPE_DISPATCH,
                is_transient=False,
                is_fatal=False,
                is_retry=False,
                raw_error=error,
            )

        return ErrorClassification(
            category=ErrorCategory.UNKNOWN,
            error_code="",
            dlq_error_type=ERROR_TYPE_UNKNOWN,
            is_transient=False,
            is_fatal=False,
            is_retry=False,
            raw_error=error,
        )

    # ── 2. 载荷与资源输入校验接缝 ────────────────────────────────────────

    def validate_payload(self, payload: Any, schema: Any) -> ValidationResult:
        """结构化载荷校验（Never-Raise 契约）。"""
        try:
            return self._validate_payload_impl(payload, schema)
        except Exception as e:
            item = ValidationErrorItem(
                field_path="__root__",
                code="SCHEMA_RESOLUTION_ERROR",
                expected=_safe_str(schema),
                actual=type(payload).__name__,
                message=f"schema error: {type(e).__name__}: {e}",
            )
            return ValidationResult(is_valid=False, errors=(item,))

    def _validate_payload_impl(self, payload: Any, schema: Any) -> ValidationResult:
        if not isinstance(payload, dict):
            item = ValidationErrorItem(
                field_path="__root__",
                code="INVALID_PAYLOAD_TYPE",
                expected="dict",
                actual=type(payload).__name__,
                message=f"payload must be a dict, got {type(payload).__name__}",
            )
            return ValidationResult(is_valid=False, errors=(item,))

        try:
            hints = typing.get_type_hints(schema)
        except Exception as e:
            item = ValidationErrorItem(
                field_path="__root__",
                code="SCHEMA_RESOLUTION_ERROR",
                expected=_safe_str(schema),
                actual="unresolvable",
                message=f"schema resolution failed: {e}",
            )
            return ValidationResult(is_valid=False, errors=(item,))

        errors: List[ValidationErrorItem] = []
        required_keys = getattr(schema, "__required_keys__", None)

        for key, expected_type in hints.items():
            if key not in payload:
                if required_keys is not None and key not in required_keys:
                    continue
                errors.append(
                    ValidationErrorItem(
                        field_path=key,
                        code="MISSING_REQUIRED_FIELD",
                        expected=str(expected_type),
                        actual="missing",
                        message=f"missing required field '{key}'",
                    )
                )
            else:
                value = payload[key]
                origin = typing.get_origin(expected_type)

                if origin is typing.Literal:
                    if not any(
                        value == allowed and type(value) is type(allowed)
                        for allowed in typing.get_args(expected_type)
                    ):
                        type_name = str(expected_type)
                        errors.append(
                            ValidationErrorItem(
                                field_path=key,
                                code="LITERAL_MISMATCH",
                                expected=type_name,
                                actual=type(value).__name__,
                                message=f"field '{key}' expected {type_name}, got {type(value).__name__}",
                            )
                        )
                    continue

                if origin is None and expected_type is int and isinstance(value, bool):
                    errors.append(
                        ValidationErrorItem(
                            field_path=key,
                            code="BOOL_FORBIDDEN_FOR_INT",
                            expected="int",
                            actual="bool",
                            message=f"field '{key}' expected int, got bool",
                        )
                    )
                    continue

                is_union = (
                    origin is typing.Union or (_UnionType is not None and origin is _UnionType)
                )
                if is_union:
                    check_type = tuple(
                        (typing.get_origin(a) or a) for a in typing.get_args(expected_type)
                    )
                else:
                    check_type = origin or expected_type

                try:
                    valid = isinstance(value, check_type)
                except TypeError:
                    literal_members = [
                        a for a in typing.get_args(expected_type)
                        if typing.get_origin(a) is typing.Literal
                    ]
                    if literal_members:
                        non_literal = [
                            (typing.get_origin(a) or a) for a in typing.get_args(expected_type)
                            if typing.get_origin(a) is not typing.Literal
                        ]
                        literal_ok = any(
                            value == allowed and type(value) is type(allowed)
                            for m in literal_members
                            for allowed in typing.get_args(m)
                        )
                        isinst_ok = True
                        for t in non_literal:
                            if t is typing.Any or isinstance(t, typing.TypeVar):
                                continue
                            try:
                                if not isinstance(value, t):
                                    isinst_ok = False
                                    break
                            except TypeError:
                                continue
                        valid = literal_ok or isinst_ok
                    else:
                        valid = True

                if valid and is_union and isinstance(value, bool):
                    member_types = tuple(
                        typing.get_origin(a) or a for a in typing.get_args(expected_type)
                    )
                    if int in member_types and bool not in member_types:
                        valid = False

                if not valid:
                    type_name = getattr(expected_type, "__name__", str(expected_type))
                    errors.append(
                        ValidationErrorItem(
                            field_path=key,
                            code="TYPE_MISMATCH",
                            expected=type_name,
                            actual=type(value).__name__,
                            message=f"field '{key}' expected {type_name}, got {type(value).__name__}",
                        )
                    )

        for key in payload:
            if key not in hints:
                errors.append(
                    ValidationErrorItem(
                        field_path=key,
                        code="UNEXPECTED_FIELD",
                        expected="none",
                        actual=type(payload[key]).__name__,
                        message=f"unexpected field '{key}'",
                    )
                )

        return ValidationResult(is_valid=len(errors) == 0, errors=tuple(errors))

    def validate_resources(self, resources: Any, where: str = "resources") -> ValidationResult:
        """结构化资源数值校验。"""
        if not isinstance(resources, dict):
            item = ValidationErrorItem(
                field_path=where,
                code="INVALID_RESOURCE_CONTAINER",
                expected="dict",
                actual=type(resources).__name__,
                message=f"resources in {where} must be a dict, got {type(resources).__name__}",
            )
            return ValidationResult(is_valid=False, errors=(item,))

        errors: List[ValidationErrorItem] = []
        for res_name, amount in resources.items():
            # 资源名必须为非空 str——非 str 键在调度侧判为未知资源（永不可跑）
            # 且经 JSON 落盘后键强转为 str，同一作业跨重启从死锁翻转为可跑，
            # 身份与账目漂移；与 suspend_resource 的资源名校验对称。
            if not isinstance(res_name, str) or not res_name:
                errors.append(
                    ValidationErrorItem(
                        field_path=f"{where}.{res_name!r}",
                        code="INVALID_RESOURCE_NAME",
                        expected="non-empty str",
                        actual=type(res_name).__name__,
                        message=f"resource name in {where} must be a non-empty str, "
                        f"got {type(res_name).__name__} ({res_name!r})",
                    )
                )
                continue
            if not isinstance(amount, (int, float)) or isinstance(amount, bool):
                errors.append(
                    ValidationErrorItem(
                        field_path=f"{where}.{res_name}",
                        code="BOOL_FORBIDDEN" if isinstance(amount, bool) else "NOT_A_NUMBER",
                        expected="number",
                        actual=type(amount).__name__,
                        message=f"resource '{res_name}' amount in {where} must be a number, got {type(amount).__name__} ({amount!r})",
                    )
                )
                continue

            try:
                finite = math.isfinite(amount)
            except OverflowError:
                errors.append(
                    ValidationErrorItem(
                        field_path=f"{where}.{res_name}",
                        code="OVERFLOW",
                        expected="finite float",
                        actual="overflow",
                        message=f"resource '{res_name}' amount in {where} is too large (exceeds float range), got {amount!r}",
                    )
                )
                continue

            if not finite:
                errors.append(
                    ValidationErrorItem(
                        field_path=f"{where}.{res_name}",
                        code="NOT_FINITE",
                        expected="finite number",
                        actual=str(amount),
                        message=f"resource '{res_name}' amount in {where} must be finite, got {amount!r}",
                    )
                )
                continue

            if amount < 0:
                errors.append(
                    ValidationErrorItem(
                        field_path=f"{where}.{res_name}",
                        code="NEGATIVE_AMOUNT",
                        expected="non-negative number",
                        actual=str(amount),
                        message=f"resource '{res_name}' amount in {where} must be non-negative, got {amount!r}",
                    )
                )

        return ValidationResult(is_valid=len(errors) == 0, errors=tuple(errors))

    def ensure_resources_valid(self, resources: Any, where: str = "resources") -> None:
        """断言模式资源校验（供 Job.__init__ 与注册入口直接抛异常）。"""
        res = self.validate_resources(resources, where)
        if not res.is_valid:
            first_err = res.errors[0]
            if first_err.code in (
                "NOT_A_NUMBER", "BOOL_FORBIDDEN", "INVALID_RESOURCE_CONTAINER",
                "INVALID_RESOURCE_NAME",
            ):
                raise TypeError(first_err.message)
            raise ValueError(first_err.message)

    # ── 3. DLQ 元数据清洗与规范化接缝 ───────────────────────────────────

    def normalize_dlq_meta(
        self,
        meta: Any,
        *,
        attempt: Optional[int] = None,
        default_error: str = "UNKNOWN_ERROR",
    ) -> Dict[str, Any]:
        """DLQ 元数据清洗与规范化的唯一出口。"""
        if not isinstance(meta, dict):
            raw_err = _safe_str(meta) if meta is not None else default_error
            return {
                "error": raw_err,
                "error_type": ERROR_TYPE_UNKNOWN,
                "failed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "_attempt": attempt if attempt is not None else 0,
            }

        sanitized = dict(meta)
        if "error_type" not in sanitized or not sanitized["error_type"]:
            cl = self.classify(sanitized)
            sanitized["error_type"] = cl.dlq_error_type

        if "failed_at" not in sanitized:
            sanitized["failed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

        if attempt is not None:
            sanitized["_attempt"] = attempt
        elif "_attempt" not in sanitized:
            sanitized["_attempt"] = 0

        return sanitized

    # ── 4. 瞬态注册表管理 ────────────────────────────────────────────────

    def register_transient(self, exception_cls: type) -> None:
        """注册业务自有异常类为瞬态（Fail-loud 校验）。"""
        _validate_transient_class(exception_cls)
        if exception_cls not in self._registry:
            self._registry.append(exception_cls)

    def register(self, exception_cls: type) -> None:
        """register_transient 的快捷别名方法。"""
        self.register_transient(exception_cls)

    def snapshot(self) -> Tuple[Type[BaseException], ...]:
        """生成不可变 tuple 快照。"""
        return tuple(self._registry)

    def matches(self, exc: BaseException) -> bool:
        """exc 是否命中注册表。"""
        return any(isinstance(exc, cls) for cls in self._registry)

    @property
    def fatal_exceptions(self) -> Tuple[Type[BaseException], ...]:
        """已解析 fatal 启发式元组（构造器声明或内置默认）——派发侧下发子进程的唯一读口。"""
        return self._fatal_exceptions

    @property
    def transient_exceptions(self) -> Tuple[Type[BaseException], ...]:
        """已解析 transient 启发式元组（构造器声明或内置默认）——派发侧下发子进程的唯一读口。"""
        return self._transient_exceptions


_DEFAULT_TAXONOMY = ErrorTaxonomy()


def validate_resource_amounts(resources: dict, where: str = "resources") -> None:
    """校验资源 amount 数值（Job.__init__ 与 pipeline 注册路径共用单点）。"""
    _DEFAULT_TAXONOMY.ensure_resources_valid(resources, where)


def validate_payload(payload: dict, schema: Any) -> list:
    """Validate payload against a TypedDict schema using runtime type hints."""
    return _DEFAULT_TAXONOMY.validate_payload(payload, schema).as_error_strings()


def classify_error_type(meta: dict) -> str:
    """从 DLQ meta 推导结构化 error_type（list_dlq() 查询与 _write_dlq_row 落库共用）。"""
    return _DEFAULT_TAXONOMY.classify(meta).dlq_error_type


def classify_exception(
    exc: BaseException,
    registry: Union[ErrorTaxonomy, Sequence[type]] = (),
    *,
    fatal_exceptions: Optional[Tuple[type, ...]] = None,
    transient_exceptions: Optional[Tuple[type, ...]] = None,
) -> str:
    """异常三分类的生产语义，返回 retry/fatal/error。"""
    if isinstance(registry, ErrorTaxonomy):
        # 不变式：taxonomy 实例已持完整分类策略，显式 kwargs 与其互斥——
        # 静默忽略会让同一调用仅因 registry 形态不同而语义翻转。
        if fatal_exceptions is not None or transient_exceptions is not None:
            raise TypeError(
                "fatal_exceptions/transient_exceptions cannot be combined with an "
                "ErrorTaxonomy instance — it already carries the resolved policy; "
                "pass a registry snapshot tuple instead"
            )
        cl = registry.classify(exc)
    elif fatal_exceptions is None and transient_exceptions is None and not registry:
        cl = _DEFAULT_TAXONOMY.classify(exc)
    else:
        taxonomy = ErrorTaxonomy(
            fatal_exceptions=fatal_exceptions,
            transient_exceptions=transient_exceptions,
            transient_registry=tuple(registry or ()),
        )
        cl = taxonomy.classify(exc)
    if cl.is_retry or cl.is_transient:
        return "retry"
    if cl.is_fatal:
        return "fatal"
    return "error"


def is_transient_exception(
    exc: BaseException,
    registry: Union[ErrorTaxonomy, Sequence[type]] = (),
) -> bool:
    """判断异常是否属于瞬态（应自动重试）。"""
    return classify_exception(exc, registry) == "retry"


__all__ = [
    # 错误码常量
    "ERR_DEPENDENCY_DEADLOCK",
    "ERR_JOB_DEPENDENCY",
    "ERR_PAYLOAD_VALIDATION",
    "ERR_MAX_RETRIES",
    "ERR_NO_HANDLER",
    "ERR_RESOURCE_DEADLOCK",
    "ERR_MALFORMED_JOB",
    "ERR_COMMIT_FAILURE_DLQ",
    "ERR_DISPATCH_FAILURE",
    "ERR_DEADLOCK_GAP",
    # DLQ error_type 常量
    "ERROR_TYPE_FATAL",
    "ERROR_TYPE_TRANSIENT_EXHAUSTED",
    "ERROR_TYPE_DEPENDENCY",
    "ERROR_TYPE_DEADLOCK",
    "ERROR_TYPE_NO_HANDLER",
    "ERROR_TYPE_VALIDATION",
    "ERROR_TYPE_COMMIT_FAILURE",
    "ERROR_TYPE_DISPATCH",
    "ERROR_TYPE_UNKNOWN",
    # 默认异常元组
    "FATAL_EXCEPTIONS",
    "TRANSIENT_EXCEPTIONS",
    # 类与值对象
    "ErrorCategory",
    "ValidationErrorItem",
    "ValidationResult",
    "ErrorClassification",
    "ErrorTaxonomy",
    "_DEFAULT_TAXONOMY",
    # 模块级工具函数
    "_validate_transient_class",
    "validate_declared_exception_classes",
    "validate_resource_amounts",
    "validate_payload",
    "classify_error_type",
    "classify_exception",
    "is_transient_exception",
]

