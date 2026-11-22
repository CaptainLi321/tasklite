"""迁移示例 4：局部调大 timeout_seconds 并级联重跑（set_status + replace）。

典型场景：
    上游大作业 ``heavy_train::exp1`` 因默认 ``timeout_seconds`` 偏小而超时失败，
    导致整棵下游（20 个评估/汇总/绘图节点）被级联标记为 ``failed``。
    现在只需要把上游超时调大并把该分支一键打回 ``pending`` 重跑，
    **20 个下游完全不需要删掉重新构造入队**。

核心原语与组合技巧：
    1. ``tx.set_status(up_uid, "pending", recursive=True)``：
       沿着正向 ``depends_on`` 闭包，将上游及全部下游同步重置为 ``pending``
       （并自动清除它们的历史 ``successes / failed`` 终态档案，避免下次 ``run()``
       被 ``trigger="once"`` 跳过）；
    2. ``tx.replace(up_uid, new_up_job, status="pending")``：
       原地更新上游的 ``timeout_seconds``（或资源/payload 配置）。

运行方式：
    python examples/edit_migrations/04_bump_timeout_and_rerun.py
    python examples/edit_migrations/04_bump_timeout_and_rerun.py --state-path ./state.db --uid heavy_train::exp1 --timeout 7200 [--apply]
"""
from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

from tasklite import (
    Job,
    StateSnapshot,
    Task,
    TaskLite,
    TaskLiteError,
    TaskResult,
)


# ── 1. 业务任务定义 ──────────────────────────────────────────────────


def heavy_train(job: Job, ctx) -> TaskResult:
    # 模拟：只有 timeout_seconds >= 60 时才视为预算充足
    if job.payload.get("require_large_timeout") and (job.timeout_seconds or 0) < 60:
        return TaskResult.failure(message="simulated timeout: increase timeout_seconds")
    return TaskResult.success(metadata={"used_timeout": job.timeout_seconds})


def downstream_eval(job: Job, ctx) -> TaskResult:
    return TaskResult.success(metadata={"evaluated": job.job_id})


def register_all_tasks(pipeline: TaskLite) -> None:
    pipeline.register_task(Task("heavy_train", heavy_train, max_retries_per_run=0, timeout_seconds=10))
    pipeline.register_task(Task("downstream_eval", downstream_eval))


# ── 2. 原子调参重排核心逻辑 ──────────────────────────────────────────


def migrate_bump_timeout_and_rerun(
    state_path: Path,
    *,
    upstream_uid: str,
    new_timeout_seconds: float,
    apply: bool,
) -> int:
    pipeline = TaskLite(backend="sqlite", state_path=state_path)
    register_all_tasks(pipeline)
    try:
        with pipeline.edit(
            dry_run=not apply,
            note=f"bump {upstream_uid} timeout_seconds -> {new_timeout_seconds} and rerun subtree",
        ) as tx:
            if upstream_uid not in tx:
                raise TaskLiteError(f"目标节点不存在：{upstream_uid}")

            # 从基准快照读出原上游配置，只修改 timeout_seconds
            orig = tx.before.get_job(upstream_uid)
            assert orig is not None
            old_cfg = orig["job"]
            updated_up_job = Job(
                old_cfg["task_type"],
                old_cfg["job_id"],
                payload=old_cfg["payload"],
                depends_on=old_cfg["depends_on"],
                resources={k: v for k, v in old_cfg["resources"].items() if k != "worker"} or None,
                max_retries_per_run=old_cfg["max_retries_per_run"],
                timeout_seconds=new_timeout_seconds,
                trigger=old_cfg["trigger"],
            )

            # 第一步：将上游及全部下游一键打回 pending（自动清理历史 failed/succeeded 档案）
            tx.set_status(upstream_uid, "pending", recursive=True)
            # 第二步：仅原地替换上游配置（显式 status="pending" 保持待执行状态）
            tx.replace(upstream_uid, updated_up_job, status="pending")

            print(tx.diff())

        if apply:
            print("\n=== 调参重排前后快照报告 ===")
            print(StateSnapshot.report(tx.before, tx.after))
        return 0
    except TaskLiteError as exc:
        print(f"\n调参重排失败并已回滚：\n{exc}")
        return 1
    finally:
        pipeline.close()


# ── 3. 自包含演示入口 ────────────────────────────────────────────────


def _run_self_demo() -> int:
    with tempfile.TemporaryDirectory(prefix="tasklite_demo_timeout_") as tmp:
        state_path = Path(tmp) / "state.db"

        # 步骤 A：首次运行——上游失败，5 个下游自动级联失败
        p = TaskLite(backend="sqlite", state_path=state_path, max_workers=1)
        register_all_tasks(p)
        try:
            up = Job(
                "heavy_train",
                "exp1",
                payload={"require_large_timeout": True},
                timeout_seconds=10,
            )
            p.enqueue(up)
            for i in range(1, 6):
                p.enqueue(Job("downstream_eval", f"exp1_metric_{i}", depends_on=[up.uid]))
            p.run(install_signals=False)
        finally:
            p.close()

        with StateSnapshot.open(state_path, mode="copy") as before_snap:
            assert before_snap.get_job("heavy_train::exp1")["status"] == "failed"  # type: ignore[index]
            assert before_snap.get_job("downstream_eval::exp1_metric_5")["status"] == "failed"  # type: ignore[index]

        # 步骤 B：dry_run 预览 -> --apply 调大超时并级联打回 pending
        print("--- [步骤 1] dry_run=True 预览调大 timeout_seconds 并级联重排 ---")
        migrate_bump_timeout_and_rerun(
            state_path, upstream_uid="heavy_train::exp1", new_timeout_seconds=3600, apply=False
        )

        print("\n--- [步骤 2] dry_run=False 原子落盘 ---")
        rc = migrate_bump_timeout_and_rerun(
            state_path, upstream_uid="heavy_train::exp1", new_timeout_seconds=3600, apply=True
        )
        if rc != 0:
            return rc

        # 步骤 C：直接 run()，整棵 6 节点子树全部跑通，且保留前一次失败 Attempt 审计记录
        print("\n--- [步骤 3] 重新 run() 验证整棵子树跑通 ---")
        p = TaskLite(backend="sqlite", state_path=state_path, max_workers=1)
        register_all_tasks(p)
        try:
            p.run(install_signals=False)
        finally:
            p.close()

        with StateSnapshot.open(state_path, mode="copy") as after_snap:
            for item in after_snap.iter_jobs():
                assert item["status"] == "succeeded", f"未成功节点: {item['job_uid']}"
            attempts = list(after_snap.iter_attempts(job_uid="heavy_train::exp1"))
            assert [a["outcome"] for a in attempts] == ["failure", "success"]
        print("[OK] 局部调大 timeout 级联重跑验证通过：下游零重入队、6 节点全绿、历史失败 Attempt 完整保留。")
        return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="场景 4：局部调大 timeout_seconds 级联重跑脚本")
    parser.add_argument("--state-path", type=Path, help="已有 SQLite 状态库路径（留空则运行自包含演示）")
    parser.add_argument("--uid", default="heavy_train::exp1", help="需要调整超时的上游 UID")
    parser.add_argument("--timeout", type=float, default=3600.0, help="新的 timeout_seconds 值")
    parser.add_argument("--apply", action="store_true", help="真正提交落盘（默认 dry_run=True 仅预览）")
    args = parser.parse_args()

    if args.state_path is None:
        return _run_self_demo()
    return migrate_bump_timeout_and_rerun(
        args.state_path,
        upstream_uid=args.uid,
        new_timeout_seconds=args.timeout,
        apply=args.apply,
    )


if __name__ == "__main__":
    raise SystemExit(main())
