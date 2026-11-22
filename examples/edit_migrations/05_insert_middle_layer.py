"""迁移示例 5：4 层 DAG 拓扑中间插层扩为 5 层（add + replace 接管 from_output）。

典型场景：
    原有 4 层流水线：``L1(ingest) -> L2(clean) -> L3(train) -> L4(report)``。
    其中 ``L1``、``L2`` 已运行成功；现在需求演进，要在 ``L1`` 与 ``L2`` 之间插入
    一层 ``L1b(calibrate)`` 扩为 5 层（且对于老数据，已由离线脚本补好 ``L1b`` 的
    磁盘产出，或者以无产物逻辑层占位）。
    需要原子完成：
    1. ``tx.add(l1b_job, status="succeeded")`` 插入新中间层并验盘发布产出；
    2. ``tx.replace(l2_uid, updated_l2_job)`` 将 ``L2`` 的 ``depends_on`` 与
       ``FileSpec.from_output`` 改接至 ``L1b``（若老数据 ``L2`` 结果仍有效可保留
       ``succeeded``；若需按新校准重算可同时 ``tx.set_status(l2_uid, "pending", recursive=True)``）。

核心安全保证：
    - 提交期 Error 3 一次性校验整条 5 层链路的所有 ``FileSpec.from_output(p, key)``：
      要求 ``p in depends_on``、``p`` 存在且为已发布保留型 ``output``；
    - 提交期 Error 2 迭代式环检测确保插层后 DAG 100% 无环。

运行方式：
    python examples/edit_migrations/05_insert_middle_layer.py
    python examples/edit_migrations/05_insert_middle_layer.py --state-path ./state.db --data-root ./data [--rerun-downstream] [--apply]
"""
from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

from tasklite import (
    FileSpec,
    Job,
    StateSnapshot,
    Task,
    TaskLite,
    TaskLiteError,
    TaskResult,
)


# ── 1. 业务任务定义（5 层拓扑）──────────────────────────────────────


def l1_ingest(job: Job, ctx) -> TaskResult:
    raw = ctx.file_path("raw")
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text("raw_v1\n", encoding="utf-8")
    return TaskResult.success()


def l1b_calibrate(job: Job, ctx) -> TaskResult:
    raw = ctx.file_path("raw")
    cal = ctx.file_path("calibrated")
    cal.parent.mkdir(parents=True, exist_ok=True)
    cal.write_text(f"calibrated({raw.read_text(encoding='utf-8').strip()})\n", encoding="utf-8")
    return TaskResult.success()


def l2_clean(job: Job, ctx) -> TaskResult:
    inp = ctx.file_path("in_data")
    clean = ctx.file_path("clean")
    clean.parent.mkdir(parents=True, exist_ok=True)
    clean.write_text(f"clean({inp.read_text(encoding='utf-8').strip()})\n", encoding="utf-8")
    return TaskResult.success()


def l3_train(job: Job, ctx) -> TaskResult:
    clean = ctx.file_path("clean")
    model = ctx.file_path("model")
    model.parent.mkdir(parents=True, exist_ok=True)
    model.write_text(f"model({clean.read_text(encoding='utf-8').strip()})\n", encoding="utf-8")
    return TaskResult.success()


def l4_report(job: Job, ctx) -> TaskResult:
    model = ctx.file_path("model")
    rep = ctx.file_path("report")
    rep.parent.mkdir(parents=True, exist_ok=True)
    rep.write_text(f"report({model.read_text(encoding='utf-8').strip()})\n", encoding="utf-8")
    return TaskResult.success()


def register_all_tasks(pipeline: TaskLite) -> None:
    pipeline.register_task(Task("l1_ingest", l1_ingest))
    pipeline.register_task(Task("l1b_calibrate", l1b_calibrate))
    pipeline.register_task(Task("l2_clean", l2_clean))
    pipeline.register_task(Task("l3_train", l3_train))
    pipeline.register_task(Task("l4_report", l4_report))


# ── 2. 原子插层核心逻辑 ──────────────────────────────────────────────


def migrate_insert_middle_layer(
    state_path: Path,
    *,
    data_root: Path,
    item_id: str,
    rerun_downstream: bool,
    apply: bool,
) -> int:
    pipeline = TaskLite(backend="sqlite", state_path=state_path)
    register_all_tasks(pipeline)
    try:
        l1_uid = f"l1_ingest::{item_id}"
        l2_uid = f"l2_clean::{item_id}"

        mid_job = Job(
            "l1b_calibrate",
            item_id,
            depends_on=[l1_uid],
            files={
                "raw": FileSpec.from_output(l1_uid, "raw", role="input"),
                "calibrated": FileSpec(data_root / item_id / "calibrated.txt", role="output"),
            },
        )
        updated_l2 = Job(
            "l2_clean",
            item_id,
            depends_on=[mid_job.uid],
            files={
                "in_data": FileSpec.from_output(mid_job.uid, "calibrated", role="input"),
                "clean": FileSpec(data_root / item_id / "clean.txt", role="output"),
            },
        )

        with pipeline.edit(
            dry_run=not apply,
            note=f"insert l1b_calibrate between {l1_uid} and {l2_uid}",
        ) as tx:
            # 1. 插入中间层为 succeeded（将强制验盘 calibrated.txt 并发布 artifacts）
            if mid_job.uid not in tx:
                tx.add(mid_job, status="succeeded")
            # 2. 更新 L2 的依赖与 from_output 指向新中间层
            tx.replace(l2_uid, updated_l2)
            # 3. 可选：若希望 L2 及下游按新校准层重算，一键级联打回 pending
            if rerun_downstream:
                tx.set_status(l2_uid, "pending", recursive=True)

            print(tx.diff())

        if apply:
            print("\n=== 4 层扩 5 层前后快照报告 ===")
            print(StateSnapshot.report(tx.before, tx.after))
        return 0
    except TaskLiteError as exc:
        print(f"\n插层迁移被拒绝并已回滚：\n{exc}")
        return 1
    finally:
        pipeline.close()


# ── 3. 自包含演示入口 ────────────────────────────────────────────────


def _run_self_demo() -> int:
    item_id = "case_01"
    with tempfile.TemporaryDirectory(prefix="tasklite_demo_insert_layer_") as tmp:
        root = Path(tmp)
        state_path = root / "state.db"
        data_root = root / "data"

        # 步骤 A：构建并运行原 4 层拓扑中的前 2 层（L1、L2 succeeded；L3、L4 pending）
        p = TaskLite(backend="sqlite", state_path=state_path, max_workers=1)
        register_all_tasks(p)
        try:
            j1 = Job(
                "l1_ingest",
                item_id,
                files={"raw": FileSpec(data_root / item_id / "raw.txt", role="output")},
            )
            j2 = Job(
                "l2_clean",
                item_id,
                depends_on=[j1.uid],
                files={
                    "in_data": FileSpec.from_output(j1.uid, "raw", role="input"),
                    "clean": FileSpec(data_root / item_id / "clean.txt", role="output"),
                },
            )
            j3 = Job(
                "l3_train",
                item_id,
                depends_on=[j2.uid],
                files={
                    "clean": FileSpec.from_output(j2.uid, "clean", role="input"),
                    "model": FileSpec(data_root / item_id / "model.txt", role="output"),
                },
            )
            j4 = Job(
                "l4_report",
                item_id,
                depends_on=[j3.uid],
                files={
                    "model": FileSpec.from_output(j3.uid, "model", role="input"),
                    "report": FileSpec(data_root / item_id / "report.txt", role="output"),
                },
            )
            p.enqueue(j1)
            p.enqueue(j2)
            p.run(install_signals=False)
            p.enqueue(j3)
            p.enqueue(j4)
        finally:
            p.close()

        # 步骤 B：在磁盘准备新中间层 L1b 的产出文件
        (data_root / item_id / "calibrated.txt").write_text("calibrated(raw_v1)\n", encoding="utf-8")

        print("--- [步骤 1] dry_run=True 预览 4 层中间插层扩为 5 层 ---")
        migrate_insert_middle_layer(
            state_path, data_root=data_root, item_id=item_id, rerun_downstream=False, apply=False
        )

        print("\n--- [步骤 2] dry_run=False 原子落盘（保留 L2 succeeded，继续跑 L3/L4）---")
        rc = migrate_insert_middle_layer(
            state_path, data_root=data_root, item_id=item_id, rerun_downstream=False, apply=True
        )
        if rc != 0:
            return rc

        # 步骤 C：继续运行未完成的 L3、L4，验证整条 5 层链路闭合
        print("\n--- [步骤 3] 运行剩余 L3 -> L4 ---")
        p = TaskLite(backend="sqlite", state_path=state_path, max_workers=1)
        register_all_tasks(p)
        try:
            p.run(install_signals=False)
        finally:
            p.close()

        with StateSnapshot.open(state_path, mode="copy") as snap:
            all_jobs = {j["job_uid"]: j for j in snap.iter_jobs()}
            assert len(all_jobs) == 5
            assert all(j["status"] == "succeeded" for j in all_jobs.values())
            assert all_jobs[f"l2_clean::{item_id}"]["job"]["depends_on"] == [f"l1b_calibrate::{item_id}"]
        print("[OK] 4 层插层扩 5 层验证通过：5 个节点全部 succeeded，from_output 契约完整闭合。")
        return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="场景 5：4 层 DAG 拓扑中间插层扩为 5 层迁移脚本")
    parser.add_argument("--state-path", type=Path, help="已有 SQLite 状态库路径（留空则运行自包含演示）")
    parser.add_argument("--data-root", type=Path, help="产物根目录（与 --state-path 同用）")
    parser.add_argument("--item-id", default="case_01", help="目标样本 ID")
    parser.add_argument(
        "--rerun-downstream",
        action="store_true",
        help="插层后将 L2 及下游一并打回 pending 按新校准结果重算",
    )
    parser.add_argument("--apply", action="store_true", help="真正提交落盘（默认 dry_run=True 仅预览）")
    args = parser.parse_args()

    if args.state_path is None:
        return _run_self_demo()
    if args.data_root is None:
        parser.error("指定 --state-path 时必须同时指定 --data-root")
    return migrate_insert_middle_layer(
        args.state_path,
        data_root=args.data_root,
        item_id=args.item_id,
        rerun_downstream=args.rerun_downstream,
        apply=args.apply,
    )


if __name__ == "__main__":
    raise SystemExit(main())
