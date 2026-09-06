"""TaskLite — 零外部依赖、物理进程隔离的轻量高可靠任务编排引擎。

A lightweight, battle-hardened task orchestration engine with zero external
dependencies, process isolation, and ACID persistence.
"""

__version__ = "1.1.0"


# Public API - Core
from .models.context import TaskContext
from .exceptions import (
    FatalError, FATAL_EXCEPTIONS, PipelineError, RateLimitHit, RetryError,
    TRANSIENT_EXCEPTIONS, TransientRegistry, classify_exception,
    is_transient_exception,
)
from .models.job import Job, JobRuntimeState
from .pipeline import DLQEntry, TaskLite, WORKER_RESOURCE, job_ref, progress_hook, slice_list
from .utils.injective import (
    content_fingerprint,
    escape_injective,
    safe_uid_filename,
    sanitize_content_id,
    sanitize_identifier,
    sanitize_job_component,
)

# Public API - Contrib ecosystem
from . import contrib

# Public API - Generic pipeline scaffolding (run/revive/job-id/progress)
from . import pipeline_util

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
    "TransientRegistry",
    "classify_exception",
    "is_transient_exception",
    "WORKER_RESOURCE",
    "DLQEntry",
    "job_ref",
    "progress_hook",
    "slice_list",
    # Injective & Fingerprint
    "content_fingerprint",
    "escape_injective",
    "sanitize_identifier",
    "sanitize_job_component",
    "sanitize_content_id",
    "safe_uid_filename",
    # Contrib
    "contrib",
    # Generic pipeline scaffolding
    "pipeline_util",
    # Resources
    "Resource",
    "RateLimitResource",
    "CapacityResource",
    # Version
    "__version__",
]

