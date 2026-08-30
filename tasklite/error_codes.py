"""框架错误码 —— DLQ error 字段与 error_type 分类的单一事实来源。
``pipeline.py``（构造 DLQ meta）与 ``backend/base.py``（error_type 分类）
共同引用，避免两处字符串字面量漂移。错误码是公开契约（README/AGENTS
文档化）：任何新增的 DLQ 写入路径必须先在此登记错误码，再决定分类。
"""
# DLQ meta["error"] 字段值（框架写入的错误码） -------------------------
ERR_DEPENDENCY_DEADLOCK = "DEPENDENCY_DEADLOCK"
ERR_JOB_DEPENDENCY = "JOB_DEPENDENCY"
ERR_PAYLOAD_VALIDATION = "PAYLOAD_VALIDATION_FAILED"
ERR_MAX_RETRIES = "MAX_RETRIES_EXCEEDED"
ERR_NO_HANDLER = "NO_HANDLER"
ERR_RESOURCE_DEADLOCK = "RESOURCE_DEADLOCK"
ERR_MALFORMED_JOB = "MALFORMED_JOB"
ERR_COMMIT_FAILURE_DLQ = "COMMIT_FAILURE_DLQ"
# dispatch 阶段失败（submit 的 pickle/启动报错、资源
# acquire 校验失败）连续达阈值转 DLQ 的错误码——不可 pickle 的 lambda
# handler 会触发无限崩溃重启循环（无 3-strike 兜底）
ERR_DISPATCH_FAILURE = "DISPATCH_FAILURE"
# 死锁分类缺口（环检测空 / 不可归因）连续多轮未分类 →
# 升级为整队列 DLQ 时的专属错误码（区别于正常归因的 DEPENDENCY/RESOURCE_DEADLOCK）
ERR_DEADLOCK_GAP = "DEADLOCK_CLASSIFICATION_GAP"
# DLQ error_type 分类值（list_dlq 查询与 _write_dlq_row 落库共用） --------
ERROR_TYPE_FATAL = "fatal"
ERROR_TYPE_TRANSIENT_EXHAUSTED = "transient_exhausted"
ERROR_TYPE_DEPENDENCY = "dependency"
ERROR_TYPE_DEADLOCK = "deadlock"
ERROR_TYPE_NO_HANDLER = "no_handler"
ERROR_TYPE_VALIDATION = "validation"
ERROR_TYPE_COMMIT_FAILURE = "commit_failure"
# dispatch 失败（submit pickle/启动报错、资源 acquire 校验
# 失败）的专属 error_type——list_dlq 查询可按「派发失败」过滤，与
# 错误码集中登记的精神一致。
ERROR_TYPE_DISPATCH = "dispatch"
ERROR_TYPE_UNKNOWN = "unknown"


def classify_error_type(meta: dict) -> str:
    """从 DLQ meta 推导结构化 error_type（list_dlq() 查询与 _write_dlq_row 落库共用）。

    分类优先级：fatal 标志最高（FatalError 确定性失败）；其次按 error 错误码
    精确/前缀匹配；其余归 unknown。error 字段是自由字符串，前缀匹配只对框架错误码生效。
    """
    if not isinstance(meta, dict):
        return ERROR_TYPE_UNKNOWN
    if meta.get("fatal"):
        return ERROR_TYPE_FATAL
    error = str(meta.get("error", ""))
    mapping = (
        (ERR_DEPENDENCY_DEADLOCK, ERROR_TYPE_DEADLOCK),
        (ERR_RESOURCE_DEADLOCK, ERROR_TYPE_DEADLOCK),
        (ERR_MALFORMED_JOB, ERROR_TYPE_DEADLOCK),
        (ERR_DEADLOCK_GAP, ERROR_TYPE_DEADLOCK),
        (ERR_JOB_DEPENDENCY, ERROR_TYPE_DEPENDENCY),
        (ERR_MAX_RETRIES, ERROR_TYPE_TRANSIENT_EXHAUSTED),
        (ERR_NO_HANDLER, ERROR_TYPE_NO_HANDLER),
        (ERR_PAYLOAD_VALIDATION, ERROR_TYPE_VALIDATION),
    )
    for code, etype in mapping:
        if error == code or error.startswith(code):
            return etype
    if error.startswith(ERR_COMMIT_FAILURE_DLQ):
        return ERROR_TYPE_COMMIT_FAILURE
    if error.startswith(ERR_DISPATCH_FAILURE):
        return ERROR_TYPE_DISPATCH
    return ERROR_TYPE_UNKNOWN

