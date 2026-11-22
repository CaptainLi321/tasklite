"""周期限速、并发槽位配额与重试/失败结算的可运行示例。

展示内容：
    1. ``IntervalResource``（周期调用节流）与 ``CapacityResource``（并发槽位上限）联合约束；
    2. ``TaskResult.retry(...)`` 触发瞬时故障自动重试，直至成功结算；
    3. ``max_retries_per_run`` 预算耗尽后自动落盘至失败档案（``failed``）；
    4. 通过 ``StateSnapshot.iter_attempts(job_uid=...)`` 观测完整物理执行轨迹。

运行方式：
    python examples/rate_limit_and_retry.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from tasklite import (
    Job,
    JobContext,
    StateSnapshot,
    Task,
    TaskLite,
    TaskResult,
)
from tasklite.resource import CapacityResource, IntervalResource


def call_remote_api(job: Job, ctx: JobContext) -> TaskResult:
    """模拟受限外部接口调用：按磁盘计数模拟前 N 次瞬时故障。"""
    counter_path = Path(job.payload["counter_path"])
    count = int(counter_path.read_text(encoding="utf-8")) + 1 if counter_path.exists() else 1
    counter_path.write_text(str(count), encoding="utf-8")

    fail_times = int(job.payload.get("fail_times", 0))
    if count <= fail_times:
        return TaskResult.retry(
            message=f"模拟瞬时限流（第 {count} 次尝试），等待重入队重试",
            metadata={"attempt_seen": count},
        )
    return TaskResult.success(
        metadata={"item": job.job_id, "attempts_used": count},
    )


def run_demo() -> int:
    with tempfile.TemporaryDirectory(prefix="tasklite_demo_rate_retry_") as tmp:
        root = Path(tmp)
        state_file = root / "state.db"
        counters = root / "counters"
        counters.mkdir(parents=True, exist_ok=True)

        job_ok = Job(
            "api_call",
            "steady_item",
            payload={"counter_path": str(counters / "steady.txt"), "fail_times": 0},
        )
        job_flaky_ok = Job(
            "api_call",
            "flaky_then_ok",
            payload={"counter_path": str(counters / "flaky.txt"), "fail_times": 2},
            max_retries_per_run=3,
        )
        job_exhausted = Job(
            "api_call",
            "always_503",
            payload={"counter_path": str(counters / "exhausted.txt"), "fail_times": 5},
            max_retries_per_run=1,
        )

        with TaskLite(backend="sqlite", state_path=state_file, max_workers=3) as pipeline:
            # 周期节流（0.05s/次）与最大并发槽位（2 个）同时生效
            pipeline.register_resource(IntervalResource("api_rate", interval_seconds=0.05))
            pipeline.register_resource(CapacityResource("api_slots", max_capacity=2))
            pipeline.register_task(
                Task(
                    "api_call",
                    call_remote_api,
                    resources={"api_rate": 1.0, "api_slots": 1.0},
                    max_retries_per_run=3,
                )
            )
            pipeline.enqueue(job_ok)
            pipeline.enqueue(job_flaky_ok)
            pipeline.enqueue(job_exhausted)
            pipeline.run(install_signals=False)

        with StateSnapshot.open(state_file, mode="copy") as snap:
            ok_rec = snap.get_job(job_ok.uid)
            flaky_rec = snap.get_job(job_flaky_ok.uid)
            exhaust_rec = snap.get_job(job_exhausted.uid)

            assert ok_rec is not None and ok_rec["status"] == "succeeded"
            assert flaky_rec is not None and flaky_rec["status"] == "succeeded"
            assert exhaust_rec is not None and exhaust_rec["status"] == "failed"

            flaky_outcomes = [
                a["outcome"] for a in snap.iter_attempts(job_uid=job_flaky_ok.uid)
            ]
            exhaust_outcomes = [
                a["outcome"] for a in snap.iter_attempts(job_uid=job_exhausted.uid)
            ]

            assert flaky_outcomes == ["retry", "retry", "success"], flaky_outcomes
            assert exhaust_outcomes == ["retry", "failure"], exhaust_outcomes

        print(f"flaky_then_ok 物理轨迹: {' -> '.join(flaky_outcomes)}")
        print(f"always_503    物理轨迹: {' -> '.join(exhaust_outcomes)} (预算耗尽落盘 failed)")
        print("[OK] 周期限速、并发配额与重试/失败结算验证通过。")
        return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    return run_demo()


if __name__ == "__main__":
    raise SystemExit(main())
