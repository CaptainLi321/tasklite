import sys
import tempfile
import time
from pathlib import Path

# 确保仓库根目录在 sys.path 中
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# 1. 基础模块与类型求值验证
import tasklite
from tasklite import (
    TaskLite, Job, TaskContext, RetryError, FatalError, PipelineError, RateLimitHit,
    CapacityResource, RateLimitResource, Resource, WORKER_RESOURCE, DLQEntry,
    job_ref, progress_hook, slice_list, content_fingerprint,
    sanitize_job_component, sanitize_identifier, sanitize_content_id, safe_uid_filename
)
from tasklite.engine.policy import ExecutionPolicy, BackoffSchedule, PreflightDecision, PreflightAction, DecisionReason
from tasklite.engine.resource import ResourceManager, ResourceEvaluation
from tasklite.engine.inflight import InFlightTracker, InFlightJob
from tasklite.models.state import PipelineState
from tasklite.backend.sqlite_backend import SQLiteStateBackend

def handler_a(job, ctx):
    val = job.payload.get("val")
    return {"result": f"processed_{val}"}


def handler_retry(job, ctx):
    if job.retries < 1:
        raise RetryError("transient failure")
    return True


def main():
    print(f"Executing with Python {sys.version}")

    # 1. 基础模块与类型求值验证
    import typing
    hints = typing.get_type_hints(TaskLite.run_graceful)
    assert "return" in hints
    print("✓ Type hints evaluated successfully.")

    # 2. 端到端管线运行验证（多进程、依赖拓扑、重试退避、资源管理、持久化）
    with tempfile.TemporaryDirectory() as tmpdir:
        pipeline = TaskLite("matrix_verify_pipeline", state_dir=tmpdir, max_workers=2)
        pipeline.add_resource(CapacityResource("gpu", 2.0))
        pipeline.add_resource(RateLimitResource("api", 0.05))

        pipeline.register_handler("task_a", handler_a, default_resources={"gpu": 1.0})
        pipeline.register_handler("task_retry", handler_retry)

        j1 = Job("task_a", "job_1", {"val": 42}, resources={"api": 1.0})
        j2 = Job("task_retry", "job_2", {}, max_retries=2, depends_on=["task_a::job_1"])

        pipeline.enqueue([j1, j2])
        pipeline.run()
        stats = pipeline.stats

        assert stats is not None
        assert stats.completed == 2, f"Expected 2 completed, got {stats.completed}"
        assert stats.failed == 0, f"Expected 0 failed, got {stats.failed}"
        assert stats.retried == 1, f"Expected 1 retried, got {stats.retried}"
        assert stats.skipped == 0

        wall = pipeline.backend.load_wall()
        assert "task_a::job_1" in wall
        assert "task_retry::job_2" in wall
        assert wall["task_a::job_1"]["result"] == "processed_42"
        assert len(pipeline.list_dlq()) == 0

    print("✓ End-to-end multi-process execution passed.")


if __name__ == "__main__":
    main()
