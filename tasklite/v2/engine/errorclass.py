"""v2 错误分类深模块：统一错误分类、进程死亡归因、载荷校验与失败档案元数据规范化。

本模块是 v2 框架对于「错误与异常」的单一真相源（Single Source of Truth）：
异常继承体系判定、失败档案 error_type 推导、子进程死亡归因与载荷结构化
校验中的规则收敛为高局部性、高杠杆的深模块。资源数额校验不在此处——
单点位于 ``models/task.py`` 的 ``validate_resource_amounts``。
"""
from __future__ import annotations

import datetime
import pickle
import signal
import types
import typing
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Sequence

from ..exceptions import FatalError, RateLimitHit, RetryError

_UnionType = getattr(types, "UnionType", None)

# ── 框架错误码常量（持久化协议：失败档案与重试账目按这些串归因，
#    串值属跨进程数据契约，不做词汇改写） ──────────────────────────────
ERR_DEPENDENCY_DEADLOCK = "DEPENDENCY_DEADLOCK"
ERR_JOB_DEPENDENCY = "JOB_DEPENDENCY"
ERR_PAYLOAD_VALIDATION = "PAYLOAD_VALIDATION_FAILED"
ERR_MAX_RETRIES = "MAX_RETRIES_EXCEEDED"
ERR_NO_HANDLER = "NO_HANDLER"
ERR_RESOURCE_DEADLOCK = "RESOURCE_DEADLOCK"
ERR_MALFORMED_JOB = "MALFORMED_JOB"
ERR_COMMIT_FAILURE = "COMMIT_FAILURE"
ERR_DISPATCH_FAILURE = "DISPATCH_FAILURE"
ERR_DEADLOCK_GAP = "DEADLOCK_CLASSIFICATION_GAP"

# ── 子进程死亡归因错误串前缀（classify_process_death 决策表产出，
#    与 _classify_string 映射同源；失败档案落盘串按前缀匹配分类） ──────
ERR_TIMEOUT_PREFIX = "TIMEOUT ("
ERR_PROCESS_CRASH_PREFIX = "PROCESS_CRASH_EXITCODE_"
ERR_NO_IPC_RESULT = "NO_IPC_RESULT"
ERR_PROCESS_SIGNAL_DEATH = "PROCESS_SIGNAL_DEATH"
ERR_IPC_WRITE_DEGRADED_PREFIX = "IPC_RESULT_WRITE_DEGRADED"

# ── 默认内置启发式异常元组 ───────────────────────────────────────────────
FATAL_EXCEPTIONS: tuple[type[BaseException], ...] = (
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

TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    ConnectionRefusedError,
    ConnectionResetError,
    ConnectionAbortedError,
    BrokenPipeError,
)


class ErrorCategory(str, Enum):
    """错误大类（权威单一事实来源；档案 error_type 取成员值）。"""

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
    """载荷输入校验结果的不可变值对象。"""

    is_valid: bool
    errors: tuple[ValidationErrorItem, ...] = field(default_factory=tuple)

    def to_error_strings(self) -> list[str]:
        """导出 list[str] 错误字符串格式（兼容 handler 报错通道）。"""
        return [e.message for e in self.errors]

    def to_diagnostic_meta(self) -> dict[str, Any]:
        """导出结构化诊断字典（失败档案 details 视图）。"""
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
            "summary": self.to_error_strings(),
        }


@dataclass(frozen=True)
class ErrorClassification:
    """错误多维分类决策的不可变值对象（单一真相源）。

    failed_error_type 是失败档案 meta ``error_type`` 键的取值（与
    ErrorCategory 成员值同源）；中断分类按 unknown 归档——中断是瞬态
    信号不落失败档案，档案值域不为其单列成员。
    """

    category: ErrorCategory
    error_code: str
    failed_error_type: str
    is_transient: bool
    is_fatal: bool
    is_retry: bool
    is_interrupted: bool = False
    lock_conflict: bool = False
    retry_error: str | None = None
    traceback_str: str | None = None
    raw_error: str = ""
    diagnostic_details: dict[str, Any] | None = None

    def to_failed_meta(self, *, attempt: int | None = None) -> dict[str, Any]:
        """构建落入 failed 表的标准元数据字典。"""
        meta: dict[str, Any] = {
            "error": self.error_code or self.raw_error or "UNKNOWN_ERROR",
            "error_type": self.failed_error_type,
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

    classify_process_death 是「exitcode × 超时 × timeout_is_transient →
    瞬态/终局、result_meta、retry_error、failed_error_type」的单一裁决点；
    收割路径（有结果文件解码、无结果文件终局构造、stale 认领）对同一
    根因必须经本表得到相同形状。
    """

    retry_requested: bool
    result_meta: dict[str, Any]
    retry_error: str | None
    failed_error_type: str


def _safe_str(obj: Any) -> str:
    """``str()`` 的 Never-Raise 变体：``__str__`` 抛异常的对象降级为类型占位串。

    不变式：classify()/validate_payload()/normalize_failed_meta() 标注
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
    classes: Sequence[type] | None, param_name: str
) -> None:
    """构造器声明的 fatal/transient 异常元组入口校验（fail-loud）。

    与注册表路径同规：非 Exception / RetryError-FatalError 子类 /
    不可 pickle 类在构造期即拒绝，而非滞后到 spawn 派发才失败。
    """
    for exception_cls in tuple(classes) if classes is not None else ():
        _validate_policy_exception_class(exception_cls, param_name)


class ErrorClassifier:
    """错误归因、异常判定、载荷校验与失败档案元数据规范化的深模块。"""

    def __init__(
        self,
        *,
        fatal_exceptions: Sequence[type[BaseException]] | None = None,
        transient_exceptions: Sequence[type[BaseException]] | None = None,
        transient_registry: Sequence[type[BaseException]] | None = None,
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
        self._fatal_exceptions: tuple[type[BaseException], ...] = (
            fatal_seq if fatal_seq is not None else FATAL_EXCEPTIONS
        )
        self._transient_exceptions: tuple[type[BaseException], ...] = (
            transient_seq if transient_seq is not None else TRANSIENT_EXCEPTIONS
        )
        self._registry: list[type[BaseException]] = (
            registry_seq if registry_seq is not None else []
        )

    # ── 1. 统一分类接缝 ──────────────────────────────────────────────────

    def classify(
        self,
        target: BaseException | ValidationResult | dict[str, Any] | str | None,
    ) -> ErrorClassification:
        """错误分类单一入口（Never-Raise 契约保证）。

        进程死亡归因不经本入口——收割侧统一走 classify_process_death
        决策表（exitcode 归因与异常/字符串分类是两套正交裁决）。
        """
        # 1. 校验失败对象
        if isinstance(target, ValidationResult):
            return ErrorClassification(
                category=ErrorCategory.VALIDATION,
                error_code=ERR_PAYLOAD_VALIDATION,
                failed_error_type=ErrorCategory.VALIDATION.value,
                is_transient=False,
                is_fatal=False,
                is_retry=False,
                raw_error=f"Validation failed: {len(target.errors)} issue(s)",
                diagnostic_details=target.to_diagnostic_meta(),
            )

        # 2. Python 异常实例
        if isinstance(target, BaseException):
            return self._classify_exception(target)

        # 3. 字典（IPC 状态字典或失败档案 meta 字典）
        if isinstance(target, dict):
            return self._classify_dict(target)

        # 4. 字符串（错误码或通用文本）
        if isinstance(target, str):
            return self._classify_string(target)

        # 5. None 或其他未知标量
        return ErrorClassification(
            category=ErrorCategory.UNKNOWN,
            error_code="",
            failed_error_type=ErrorCategory.UNKNOWN.value,
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

        # 判定优先级：中断 > RetryError 专用分支 > 用户注册瞬态 >
        # FatalError 专用分支 > Fatal 启发式 > Transient 启发式。
        in_reg = self.matches(exc)
        in_fatal = isinstance(exc, self._fatal_exceptions)
        in_transient = isinstance(exc, self._transient_exceptions)

        if is_interrupted:
            return ErrorClassification(
                category=ErrorCategory.INTERRUPTED,
                error_code="INTERRUPTED",
                failed_error_type=ErrorCategory.UNKNOWN.value,
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
                failed_error_type=ErrorCategory.TRANSIENT_EXHAUSTED.value,
                is_transient=True,
                is_fatal=False,
                is_retry=True,
                raw_error=raw_err,
            )

        if is_fatal or in_fatal:
            return ErrorClassification(
                category=ErrorCategory.FATAL,
                error_code="",
                failed_error_type=ErrorCategory.FATAL.value,
                is_transient=False,
                is_fatal=True,
                is_retry=False,
                raw_error=raw_err,
            )

        if in_transient:
            return ErrorClassification(
                category=ErrorCategory.TRANSIENT_EXHAUSTED,
                error_code=ERR_MAX_RETRIES,
                failed_error_type=ErrorCategory.TRANSIENT_EXHAUSTED.value,
                is_transient=True,
                is_fatal=False,
                is_retry=False,
                raw_error=raw_err,
            )

        return ErrorClassification(
            category=ErrorCategory.UNKNOWN,
            error_code="",
            failed_error_type=ErrorCategory.UNKNOWN.value,
            is_transient=False,
            is_fatal=False,
            is_retry=False,
            raw_error=raw_err,
        )

    def classify_process_death(
        self,
        exitcode: int | None,
        *,
        timed_out: bool,
        timeout_seconds: float = 0.0,
        timeout_is_transient: bool = False,
    ) -> DeathAttribution:
        """子进程死亡归因决策表（Never-Raise 契约）。

        收割侧的唯一裁决点：有结果文件解码路径与无结果文件终局构造路径
        对同一 exitcode 必须得到相同的 meta 形状与错误串。决策列：

        - timed_out ∧ timeout_is_transient → 瞬态重试（meta 留空，仅 retry_error）
        - timed_out ∧ ¬timeout_is_transient → 终局（失败档案落 fatal 型）
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
                    failed_error_type=ErrorCategory.TRANSIENT_EXHAUSTED.value,
                )
            return DeathAttribution(
                retry_requested=False,
                result_meta={"error": f"{ERR_TIMEOUT_PREFIX}{timeout_seconds}s)"},
                retry_error=None,
                failed_error_type=ErrorCategory.FATAL.value,
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
                failed_error_type=ErrorCategory.TRANSIENT_EXHAUSTED.value,
            )
        if exitcode:
            return DeathAttribution(
                retry_requested=True,
                result_meta={"error": f"{ERR_PROCESS_CRASH_PREFIX}{exitcode}"},
                retry_error=f"PROCESS_CRASH_EXIT: worker exited with code {exitcode}",
                failed_error_type=ErrorCategory.TRANSIENT_EXHAUSTED.value,
            )
        return DeathAttribution(
            retry_requested=True,
            result_meta={"error": ERR_NO_IPC_RESULT},
            retry_error=f"{ERR_NO_IPC_RESULT}: worker exited without writing a result file",
            failed_error_type=ErrorCategory.TRANSIENT_EXHAUSTED.value,
        )

    def _classify_dict(self, meta: dict[str, Any]) -> ErrorClassification:
        # IPC 状态字典
        status = meta.get("status")
        if status == "retry":
            return ErrorClassification(
                category=ErrorCategory.TRANSIENT_EXHAUSTED,
                error_code=ERR_MAX_RETRIES,
                failed_error_type=ErrorCategory.TRANSIENT_EXHAUSTED.value,
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
                failed_error_type=ErrorCategory.FATAL.value,
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
                failed_error_type=ErrorCategory.UNKNOWN.value,
                is_transient=False,
                is_fatal=False,
                is_retry=False,
                is_interrupted=True,
                raw_error=_safe_str(meta.get("error", "")),
            )

        # 失败档案 meta 字典
        is_fatal = bool(meta.get("fatal"))
        error_str = _safe_str(meta.get("error", ""))
        failed_type = meta.get("error_type")

        if is_fatal:
            return ErrorClassification(
                category=ErrorCategory.FATAL,
                error_code=error_str,
                failed_error_type=ErrorCategory.FATAL.value,
                is_transient=False,
                is_fatal=True,
                is_retry=False,
                raw_error=error_str,
                traceback_str=meta.get("traceback"),
            )

        # 映射错误码
        cl = self._classify_string(error_str)
        if failed_type and failed_type != ErrorCategory.UNKNOWN.value:
            return ErrorClassification(
                category=cl.category,
                error_code=error_str,
                failed_error_type=failed_type,
                is_transient=cl.is_transient,
                is_fatal=cl.is_fatal,
                is_retry=cl.is_retry,
                raw_error=error_str,
                traceback_str=meta.get("traceback"),
            )
        return cl

    def _classify_string(self, error: str) -> ErrorClassification:
        # (错误码/前缀, 大类, 瞬态, 致命)；档案 error_type 取大类成员值
        mapping: tuple[tuple[str, ErrorCategory, bool, bool], ...] = (
            (ERR_DEPENDENCY_DEADLOCK, ErrorCategory.DEADLOCK, False, False),
            (ERR_RESOURCE_DEADLOCK, ErrorCategory.DEADLOCK, False, False),
            (ERR_MALFORMED_JOB, ErrorCategory.DEADLOCK, False, False),
            (ERR_DEADLOCK_GAP, ErrorCategory.DEADLOCK, False, False),
            (ERR_JOB_DEPENDENCY, ErrorCategory.DEPENDENCY, False, False),
            (ERR_MAX_RETRIES, ErrorCategory.TRANSIENT_EXHAUSTED, False, False),
            (ERR_NO_HANDLER, ErrorCategory.NO_HANDLER, False, True),
            (ERR_PAYLOAD_VALIDATION, ErrorCategory.VALIDATION, False, False),
            # 子进程死亡族（与 classify_process_death 产出串同源）——
            # 非瞬态超时终局落 fatal 型；其余死亡/降级串落瞬态型
            (ERR_TIMEOUT_PREFIX, ErrorCategory.FATAL, False, True),
            (ERR_PROCESS_CRASH_PREFIX, ErrorCategory.TRANSIENT_EXHAUSTED, True, False),
            (ERR_NO_IPC_RESULT, ErrorCategory.TRANSIENT_EXHAUSTED, True, False),
            (ERR_PROCESS_SIGNAL_DEATH, ErrorCategory.TRANSIENT_EXHAUSTED, True, False),
            (ERR_IPC_WRITE_DEGRADED_PREFIX, ErrorCategory.TRANSIENT_EXHAUSTED, True, False),
        )
        for code, cat, trans, fatal in mapping:
            if error == code or error.startswith(code):
                return ErrorClassification(
                    category=cat,
                    error_code=code,
                    failed_error_type=cat.value,
                    is_transient=trans,
                    is_fatal=fatal,
                    is_retry=False,
                    raw_error=error,
                )

        if error.startswith(ERR_COMMIT_FAILURE):
            return ErrorClassification(
                category=ErrorCategory.COMMIT_FAILURE,
                error_code=ERR_COMMIT_FAILURE,
                failed_error_type=ErrorCategory.COMMIT_FAILURE.value,
                is_transient=False,
                is_fatal=False,
                is_retry=False,
                raw_error=error,
            )

        if error.startswith(ERR_DISPATCH_FAILURE):
            return ErrorClassification(
                category=ErrorCategory.DISPATCH,
                error_code=ERR_DISPATCH_FAILURE,
                failed_error_type=ErrorCategory.DISPATCH.value,
                is_transient=False,
                is_fatal=False,
                is_retry=False,
                raw_error=error,
            )

        return ErrorClassification(
            category=ErrorCategory.UNKNOWN,
            error_code="",
            failed_error_type=ErrorCategory.UNKNOWN.value,
            is_transient=False,
            is_fatal=False,
            is_retry=False,
            raw_error=error,
        )

    # ── 2. 载荷结构化校验接缝 ───────────────────────────────────────────

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
        """载荷校验编排：类型门卫 → hints 解析 → 逐字段校验 → 意外字段。"""
        if not isinstance(payload, dict):
            return ValidationResult(
                is_valid=False, errors=(_invalid_payload_type_item(payload),)
            )

        hints, resolve_error = _resolve_schema_hints(schema)
        if resolve_error is not None:
            return ValidationResult(is_valid=False, errors=(resolve_error,))

        errors: list[ValidationErrorItem] = []
        required_keys = getattr(schema, "__required_keys__", None)
        for key, expected_type in hints.items():
            if key not in payload:
                if required_keys is not None and key not in required_keys:
                    continue
                errors.append(_missing_field_item(key, expected_type))
                continue
            errors.extend(_validate_payload_field(payload, key, expected_type))
        errors.extend(_unexpected_field_items(payload, hints))
        return ValidationResult(is_valid=len(errors) == 0, errors=tuple(errors))

    # ── 3. 失败档案元数据规范化接缝 ─────────────────────────────────────

    def normalize_failed_meta(
        self,
        meta: Any,
        *,
        attempt: int | None = None,
        default_error: str = "UNKNOWN_ERROR",
    ) -> dict[str, Any]:
        """失败档案元数据规范化的唯一出口。"""
        if not isinstance(meta, dict):
            raw_err = _safe_str(meta) if meta is not None else default_error
            return {
                "error": raw_err,
                "error_type": ErrorCategory.UNKNOWN.value,
                "failed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "_attempt": attempt if attempt is not None else 0,
            }

        normalized = dict(meta)
        if not normalized.get("error_type"):
            cl = self.classify(normalized)
            normalized["error_type"] = cl.failed_error_type

        if "failed_at" not in normalized:
            normalized["failed_at"] = datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat()

        if attempt is not None:
            normalized["_attempt"] = attempt
        else:
            normalized.setdefault("_attempt", 0)

        return normalized

    # ── 4. 瞬态注册表管理 ────────────────────────────────────────────────

    def register_transient(self, exception_cls: type) -> None:
        """注册业务自有异常类为瞬态（Fail-loud 校验）。"""
        _validate_transient_class(exception_cls)
        if exception_cls not in self._registry:
            self._registry.append(exception_cls)

    def snapshot(self) -> tuple[type[BaseException], ...]:
        """生成不可变 tuple 快照。"""
        return tuple(self._registry)

    def matches(self, exc: BaseException) -> bool:
        """exc 是否命中注册表。"""
        return any(isinstance(exc, cls) for cls in self._registry)

    @property
    def fatal_exceptions(self) -> tuple[type[BaseException], ...]:
        """已解析 fatal 启发式元组（构造器声明或内置默认）——派发侧下发子进程的唯一读口。"""
        return self._fatal_exceptions

    @property
    def transient_exceptions(self) -> tuple[type[BaseException], ...]:
        """已解析 transient 启发式元组（构造器声明或内置默认）——派发侧下发子进程的唯一读口。"""
        return self._transient_exceptions


# ── 载荷字段级校验函数组（纯函数，无 classifier 状态依赖） ────────────────


def _invalid_payload_type_item(payload: Any) -> ValidationErrorItem:
    """非 dict payload 的门卫缺陷项。"""
    return ValidationErrorItem(
        field_path="__root__",
        code="INVALID_PAYLOAD_TYPE",
        expected="dict",
        actual=type(payload).__name__,
        message=f"payload must be a dict, got {type(payload).__name__}",
    )


def _resolve_schema_hints(
    schema: Any,
) -> tuple[dict[str, Any], ValidationErrorItem | None]:
    """解析 schema 类型 hints；失败时返回结构化缺陷项（不抛异常）。"""
    try:
        return typing.get_type_hints(schema), None
    except Exception as e:
        return {}, ValidationErrorItem(
            field_path="__root__",
            code="SCHEMA_RESOLUTION_ERROR",
            expected=_safe_str(schema),
            actual="unresolvable",
            message=f"schema resolution failed: {e}",
        )


def _missing_field_item(key: str, expected_type: Any) -> ValidationErrorItem:
    """缺失必填字段的缺陷项。"""
    return ValidationErrorItem(
        field_path=key,
        code="MISSING_REQUIRED_FIELD",
        expected=str(expected_type),
        actual="missing",
        message=f"missing required field '{key}'",
    )


def _validate_payload_field(
    payload: dict[str, Any], key: str, expected_type: Any
) -> list[ValidationErrorItem]:
    """单字段类型校验：Literal → bool-for-int → 常规/联合 isinstance 判定。"""
    value = payload[key]
    origin = typing.get_origin(expected_type)

    if origin is typing.Literal:
        return _check_literal_field(key, value, expected_type)

    if origin is None and expected_type is int and isinstance(value, bool):
        return [_bool_forbidden_for_int_item(key)]

    is_union = origin is typing.Union or (_UnionType is not None and origin is _UnionType)
    check_type = _resolve_check_type(expected_type, origin, is_union)
    valid = _isinstance_with_literal_fallback(value, expected_type, check_type)
    if valid and is_union:
        valid = not _bool_in_int_only_union(value, expected_type)
    if not valid:
        return [_type_mismatch_item(key, value, expected_type)]
    return []


def _check_literal_field(
    key: str, value: Any, expected_type: Any
) -> list[ValidationErrorItem]:
    """Literal 字段校验：值与类型须同时等于某一允许成员。"""
    if any(
        value == allowed and type(value) is type(allowed)
        for allowed in typing.get_args(expected_type)
    ):
        return []
    type_name = str(expected_type)
    return [
        ValidationErrorItem(
            field_path=key,
            code="LITERAL_MISMATCH",
            expected=type_name,
            actual=type(value).__name__,
            message=f"field '{key}' expected {type_name}, got {type(value).__name__}",
        )
    ]


def _bool_forbidden_for_int_item(key: str) -> ValidationErrorItem:
    """int 字段收到 bool 的缺陷项（bool 是 int 子类，须显式拒绝）。"""
    return ValidationErrorItem(
        field_path=key,
        code="BOOL_FORBIDDEN_FOR_INT",
        expected="int",
        actual="bool",
        message=f"field '{key}' expected int, got bool",
    )


def _resolve_check_type(expected_type: Any, origin: Any, is_union: bool) -> Any:
    """isinstance 检查目标：联合拍平为成员元组，其余取 origin 或原类型。"""
    if not is_union:
        return origin or expected_type
    return tuple((typing.get_origin(a) or a) for a in typing.get_args(expected_type))


def _isinstance_with_literal_fallback(
    value: Any, expected_type: Any, check_type: Any
) -> bool:
    """isinstance 判定；TypedDict 等不支持 isinstance 的目标降级宽松判定。

    降级规则：类型表达式含 Literal 成员时按「Literal 命中或其余成员
    isinstance 全通过」判定；不含 Literal（如嵌套 TypedDict）时通过
    （校验是浅层校验，嵌套结构不递归、不炸裂管线）。
    """
    try:
        return isinstance(value, check_type)
    except TypeError:
        pass
    members = typing.get_args(expected_type)
    literal_members = [m for m in members if typing.get_origin(m) is typing.Literal]
    if not literal_members:
        return True
    if any(
        value == allowed and type(value) is type(allowed)
        for m in literal_members
        for allowed in typing.get_args(m)
    ):
        return True
    return _isinstance_all_non_literal(value, members)


def _isinstance_all_non_literal(value: Any, members: tuple[Any, ...]) -> bool:
    """非 Literal 成员的 isinstance 全通过判定（Any/TypeVar 视为通过）。"""
    for m in members:
        if typing.get_origin(m) is typing.Literal:
            continue
        member_type = typing.get_origin(m) or m
        if member_type is typing.Any or isinstance(member_type, typing.TypeVar):
            continue
        try:
            if not isinstance(value, member_type):
                return False
        except TypeError:
            continue
    return True


def _bool_in_int_only_union(value: Any, expected_type: Any) -> bool:
    """联合含 int 且不含 bool 时，bool 值须判不通过（bool 是 int 子类）。"""
    if not isinstance(value, bool):
        return False
    member_types = tuple(
        typing.get_origin(a) or a for a in typing.get_args(expected_type)
    )
    return int in member_types and bool not in member_types


def _type_mismatch_item(key: str, value: Any, expected_type: Any) -> ValidationErrorItem:
    """类型不匹配的缺陷项。"""
    type_name = getattr(expected_type, "__name__", str(expected_type))
    return ValidationErrorItem(
        field_path=key,
        code="TYPE_MISMATCH",
        expected=type_name,
        actual=type(value).__name__,
        message=f"field '{key}' expected {type_name}, got {type(value).__name__}",
    )


def _unexpected_field_items(
    payload: dict[str, Any], hints: dict[str, Any]
) -> list[ValidationErrorItem]:
    """schema 未声明的 payload 键的缺陷项列表。"""
    return [
        ValidationErrorItem(
            field_path=key,
            code="UNEXPECTED_FIELD",
            expected="none",
            actual=type(payload[key]).__name__,
            message=f"unexpected field '{key}'",
        )
        for key in payload
        if key not in hints
    ]


_DEFAULT_CLASSIFIER = ErrorClassifier()


def validate_payload_errors(payload: dict, schema: Any) -> list:
    """按 TypedDict schema 的运行时类型 hints 校验 payload，返回错误串列表。"""
    return _DEFAULT_CLASSIFIER.validate_payload(payload, schema).to_error_strings()


def classify_error_type(meta: dict) -> str:
    """从失败档案 meta 推导结构化 error_type（list_failures 查询与落库共用）。"""
    return _DEFAULT_CLASSIFIER.classify(meta).failed_error_type


def classify_exception(
    exc: BaseException,
    registry: ErrorClassifier | Sequence[type] = (),
    *,
    fatal_exceptions: tuple[type, ...] | None = None,
    transient_exceptions: tuple[type, ...] | None = None,
) -> str:
    """异常三分类的生产语义，返回 retry/fatal/error。"""
    if isinstance(registry, ErrorClassifier):
        # 不变式：classifier 实例已持完整分类策略，显式 kwargs 与其互斥——
        # 静默忽略会让同一调用仅因 registry 形态不同而语义翻转。
        if fatal_exceptions is not None or transient_exceptions is not None:
            raise TypeError(
                "fatal_exceptions/transient_exceptions cannot be combined with an "
                "ErrorClassifier instance — it already carries the resolved policy; "
                "pass a registry snapshot tuple instead"
            )
        cl = registry.classify(exc)
    elif fatal_exceptions is None and transient_exceptions is None and not registry:
        cl = _DEFAULT_CLASSIFIER.classify(exc)
    else:
        classifier = ErrorClassifier(
            fatal_exceptions=fatal_exceptions,
            transient_exceptions=transient_exceptions,
            transient_registry=tuple(registry or ()),
        )
        cl = classifier.classify(exc)
    if cl.is_retry or cl.is_transient:
        return "retry"
    if cl.is_fatal:
        return "fatal"
    return "error"


def is_transient_exception(
    exc: BaseException,
    registry: ErrorClassifier | Sequence[type] = (),
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
    "ERR_COMMIT_FAILURE",
    "ERR_DISPATCH_FAILURE",
    "ERR_DEADLOCK_GAP",
    # 死亡归因错误串前缀
    "ERR_TIMEOUT_PREFIX",
    "ERR_PROCESS_CRASH_PREFIX",
    "ERR_NO_IPC_RESULT",
    "ERR_PROCESS_SIGNAL_DEATH",
    "ERR_IPC_WRITE_DEGRADED_PREFIX",
    # 默认异常元组
    "FATAL_EXCEPTIONS",
    "TRANSIENT_EXCEPTIONS",
    # 类与值对象
    "ErrorCategory",
    "ValidationErrorItem",
    "ValidationResult",
    "ErrorClassification",
    "DeathAttribution",
    "ErrorClassifier",
    # 模块级工具函数
    "validate_declared_exception_classes",
    "validate_payload_errors",
    "classify_error_type",
    "classify_exception",
    "is_transient_exception",
]
