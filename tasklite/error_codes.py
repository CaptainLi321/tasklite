"""框架错误码 —— DLQ error 字段与 error_type 分类的单一事实来源。

本模块是公开契约与薄适配层，内部核心逻辑委托至 ErrorTaxonomy 深模块。
"""

from .taxonomy import (
    ERR_COMMIT_FAILURE_DLQ,
    ERR_DEADLOCK_GAP,
    ERR_DEPENDENCY_DEADLOCK,
    ERR_DISPATCH_FAILURE,
    ERR_JOB_DEPENDENCY,
    ERR_MALFORMED_JOB,
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
    _DEFAULT_TAXONOMY,
)


def classify_error_type(meta: dict) -> str:
    """从 DLQ meta 推导结构化 error_type（list_dlq() 查询与 _write_dlq_row 落库共用）。"""
    return _DEFAULT_TAXONOMY.classify(meta).dlq_error_type
