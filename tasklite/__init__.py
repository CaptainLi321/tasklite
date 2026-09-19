"""TaskLite — 零外部依赖、物理进程隔离的轻量高可靠任务编排引擎。

A lightweight, battle-hardened task orchestration engine with zero external
dependencies, process isolation, and ACID persistence.
"""

__version__ = "1.3.1"


# Public API - Core
from .models.context import TaskContext
from .exceptions import (
    FatalError, PipelineError, RateLimitHit, RetryError,
)
from .models.job import Job, JobRuntimeState
from .hooks import job_ref, progress_hook, slice_list
from .pipeline import DLQEntry, TaskLite, WORKER_RESOURCE
from .utils.injective import (
    content_fingerprint,
    escape_injective,
    safe_uid_filename,
    sanitize_content_id,
    sanitize_identifier,
    sanitize_job_component,
)

# Public API - Taxonomy & Validation
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
    FATAL_EXCEPTIONS,
    TRANSIENT_EXCEPTIONS,
    ErrorCategory,
    ErrorClassification,
    ErrorTaxonomy,
    ValidationErrorItem,
    ValidationResult,
    classify_error_type,
    classify_exception,
    is_transient_exception,
    validate_payload,
    validate_resource_amounts,
)

# Public API - Contrib ecosystem
from . import contrib

# Public API - Resources
from .engine.resource import CapacityResource, RateLimitResource, Resource

__all__ = [
    # Core
    "TaskLite",
    "Job",
    "JobRuntimeState",
    "TaskContext",
    "RetryError",
    "FatalError",
    "PipelineError",
    "RateLimitHit",
    "FATAL_EXCEPTIONS",
    "TRANSIENT_EXCEPTIONS",
    "classify_exception",
    "is_transient_exception",
    "WORKER_RESOURCE",
    "DLQEntry",
    "job_ref",
    "progress_hook",
    "slice_list",
    # Taxonomy, Errors & Validation
    "ErrorTaxonomy",
    "ErrorCategory",
    "ErrorClassification",
    "ValidationErrorItem",
    "ValidationResult",
    "classify_error_type",
    "validate_payload",
    "validate_resource_amounts",
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
    "ERROR_TYPE_FATAL",
    "ERROR_TYPE_TRANSIENT_EXHAUSTED",
    "ERROR_TYPE_DEPENDENCY",
    "ERROR_TYPE_DEADLOCK",
    "ERROR_TYPE_NO_HANDLER",
    "ERROR_TYPE_VALIDATION",
    "ERROR_TYPE_COMMIT_FAILURE",
    "ERROR_TYPE_DISPATCH",
    "ERROR_TYPE_UNKNOWN",
    # Injective & Fingerprint
    "content_fingerprint",
    "escape_injective",
    "sanitize_identifier",
    "sanitize_job_component",
    "sanitize_content_id",
    "safe_uid_filename",
    # Contrib
    "contrib",
    # Resources
    "Resource",
    "RateLimitResource",
    "CapacityResource",
    # Version
    "__version__",
]


