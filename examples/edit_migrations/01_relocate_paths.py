"""迁移示例 1：换盘符 / 多盘符目录搬迁（原地 replace 继承原态）。

典型场景：
    磁盘空间不足，已通过操作系统命令（mv / rsync / robocopy）将全部或部分
    数据产物目录从旧路径（如 ``old_disk/``）搬迁至新路径（如 ``new_disk/``）。
    需要把状态库中对应 Job 的 ``FileSpec`` 路径同步指向新目录，同时完整保留
    它们原有的 ``succeeded / failed / pending`` 状态与历史 ``attempts`` 轨迹。

核心原语：
    - ``if job.uid in tx:`` 判断节点是否已在库中；
    - ``tx.replace(job.uid, job)``（``status=None``）原地更新配置并自动继承原态；
    - 提交期 Error 4 自动对所有替换后仍为 ``succeeded`` 的保留型 ``output`` 做
      磁盘存在性与安全路径核验：只要有 1 个文件漏搬，整单事务自动回滚。

运行方式：
    1. 零依赖自演示（在临时目录构造旧库并完整演示 dry-run 与落盘）：
       python examples/edit_migrations/01_relocate_paths.py
    2. 对真实状态库执行预览 / 落盘：
       python examples/edit_migrations/01_relocate_paths.py --state-path ./state.db --new-root /mnt/disk2
       python examples/edit_migrations/01_relocate_paths.py --state-path ./state.db --new-root /mnt/disk2 --apply
"""
from __future__ import annotations

import argparse
import shutil
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
    make_job_uid,
)


# ── 1. 业务任务定义（代码即规格）──────────────────────────────────────


def extract_features(job: Job, ctx) -> TaskResult:
    out_path = ctx.file_path("features")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(f"features:{job.payload['dataset']}\n", encoding="utf-8")
    return TaskResult.success(metadata={"dataset": job.payload["dataset"]})


def train_model(job: Job, ctx) -> TaskResult:
    feat_path = ctx.file_path("features")
    ckpt_path = ctx.file_path("checkpoint")
    ckpt_path.parent.mkdir(parents=True, exist_ok=True)
    ckpt_path.write_text(
        f"model_from({feat_path.read_text(encoding='utf-8').strip()})\n",
        encoding="utf-8",
    )
    return TaskResult.success()


def register_all_tasks(pipeline: TaskLite) -> None:
    pipeline.register_task(Task("extract", extract_features))
    pipeline.register_task(Task("train", train_model))


def build_jobs(data_root: Path, datasets: list[str]) -> list[Job]:
    """按目标数据根目录构造新版拓扑 Job 列表。"""
    jobs: list[Job] = []
    for ds in datasets:
        extract_job = Job(
            "extract",
            ds,
            payload={"dataset": ds},
            files={"features": FileSpec(data_root / ds / "features.txt", role="output")},
        )
        train_job = Job(
            "train",
            ds,
            payload={"dataset": ds},
            depends_on=[extract_job.uid],
            files={
                "features": FileSpec.from_output(extract_job.uid, "features", role="input"),
                "checkpoint": FileSpec(data_root / ds / "model.pt", role="output"),
            },
        )
        jobs.extend([extract_job, train_job])
    return jobs


# ── 2. 原子迁移核心逻辑 ──────────────────────────────────────────────


def migrate_relocate_paths(
    state_path: Path,
    *,
    new_root: Path,
    datasets: list[str],
    apply: bool,
) -> int:
    pipeline = TaskLite(backend="sqlite", state_path=state_path)
    register_all_tasks(pipeline)
    try:
        with pipeline.edit(dry_run=not apply, note=f"relocate data_root -> {new_root}") as tx:
            for job in build_jobs(new_root, datasets):
                if job.uid in tx:
                    # status=None 自动继承原 succeeded / failed / pending 状态
                    tx.replace(job.uid, job)

            print(tx.diff())

        if apply:
            print("\n=== 落盘前后快照报告 ===")
            print(StateSnapshot.report(tx.before, tx.after))
        return 0
    except TaskLiteError as exc:
        print(f"\n迁移被拒绝并已完整回滚（零落盘）：\n{exc}")
        return 1
    finally:
        pipeline.close()


# ── 3. 自包含演示入口 ────────────────────────────────────────────────


def _run_self_demo() -> int:
    datasets = ["ds_alpha", "ds_beta"]
    with tempfile.TemporaryDirectory(prefix="tasklite_demo_relocate_") as tmp:
        root = Path(tmp)
        state_path = root / "state.db"
        old_disk = root / "old_disk"
        new_disk = root / "new_disk"

        # 步骤 A：在旧盘上跑完 ds_alpha，ds_beta 留在 pending（模拟中途搬迁）
        p = TaskLite(backend="sqlite", state_path=state_path, max_workers=1)
        register_all_tasks(p)
        try:
            for j in build_jobs(old_disk, ["ds_alpha"]):
                p.enqueue(j)
            p.run(install_signals=False)
            for j in build_jobs(old_disk, ["ds_beta"]):
                p.enqueue(j)
        finally:
            p.close()

        # 步骤 B：模拟漏搬 ds_alpha/model.pt -> 校验必失败并整单回滚
        (new_disk / "ds_alpha").mkdir(parents=True, exist_ok=True)
        shutil.copy2(old_disk / "ds_alpha" / "features.txt", new_disk / "ds_alpha" / "features.txt")
        print("--- [演练 1] 模拟漏搬 1 个产物文件（应被 Error 4 拦截回滚）---")
        migrate_relocate_paths(state_path, new_root=new_disk, datasets=datasets, apply=False)

        # 步骤 C：补齐 OS 搬迁 -> dry_run 预览 -> --apply 正式落盘
        shutil.copy2(old_disk / "ds_alpha" / "model.pt", new_disk / "ds_alpha" / "model.pt")
        print("\n--- [演练 2] 全部文件就位后执行 dry_run=True 预览 ---")
        migrate_relocate_paths(state_path, new_root=new_disk, datasets=datasets, apply=False)

        print("\n--- [演练 3] 执行 dry_run=False 原子落盘 ---")
        rc = migrate_relocate_paths(state_path, new_root=new_disk, datasets=datasets, apply=True)
        if rc != 0:
            return rc

        # 验证：新库中 ds_alpha 保持 succeeded 且产出指向 new_disk，ds_beta 保持 pending
        with StateSnapshot.open(state_path, mode="copy") as snap:
            train_alpha = snap.get_job(make_job_uid("train", "ds_alpha"))
            extract_beta = snap.get_job(make_job_uid("extract", "ds_beta"))
            assert train_alpha is not None and train_alpha["status"] == "succeeded"
            assert str(new_disk.resolve()) in train_alpha["files"]["checkpoint"]["path"]
            assert extract_beta is not None and extract_beta["status"] == "pending"
        print("\n[OK] 换盘符迁移验证通过：已完成节点保持 succeeded，待办节点保持 pending。")
        return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="场景 1：换盘符 / 多盘符目录搬迁迁移脚本")
    parser.add_argument("--state-path", type=Path, help="已有 SQLite 状态库路径（留空则运行自包含演示）")
    parser.add_argument("--new-root", type=Path, help="搬迁后的新数据根目录（与 --state-path 同用）")
    parser.add_argument("--datasets", nargs="+", default=["ds_alpha", "ds_beta"], help="待迁移数据集列表")
    parser.add_argument("--apply", action="store_true", help="真正提交落盘（默认 dry_run=True 仅预览）")
    args = parser.parse_args()

    if args.state_path is None:
        return _run_self_demo()
    if args.new_root is None:
        parser.error("指定 --state-path 时必须同时指定 --new-root")
    return migrate_relocate_paths(
        args.state_path,
        new_root=args.new_root,
        datasets=args.datasets,
        apply=args.apply,
    )


if __name__ == "__main__":
    raise SystemExit(main())
