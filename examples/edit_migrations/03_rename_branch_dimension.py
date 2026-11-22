"""迁移示例 3：新增消融维度致老枝条改名与批量顺移（replace / replace_many）。

典型场景：
    实验原先只有 ``train::ds1``（默认无正则化），现在新增正则化维度，
    需要把老枝条规范化改名为 ``train::ds1_l2_0``（同时已用 OS 把对应产物目录
    改名），并把下游 ``eval::ds1`` 改名/更新依赖指向 ``train::ds1_l2_0``；
    或者存在 ``stage::1 -> stage::2``、``stage::2 -> stage::3`` 编号顺移/互换。

核心原语与安全保证：
    - ``tx.replace_many(mapping)`` 先统一摘除全部旧键（异名替换会彻底清空旧键
      三表条目，不留幽灵旧节点），再统一放入新 Job，天然支持编号顺移与互换；
    - 只要漏迁任意一个下游或汇总节点，提交期 Error 1 / Error 2 孤儿检测立即
      整单拦截并列出全部断裂边，绝不写出脏库；
    - 既有 ``attempts`` 历史记录保持不可变，原样归属旧 UID 不重写。

运行方式：
    python examples/edit_migrations/03_rename_branch_dimension.py
    python examples/edit_migrations/03_rename_branch_dimension.py --state-path ./state.db --data-root ./data [--apply]
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
)


# ── 1. 业务任务定义 ──────────────────────────────────────────────────


def train_model(job: Job, ctx) -> TaskResult:
    ckpt = ctx.file_path("ckpt")
    ckpt.parent.mkdir(parents=True, exist_ok=True)
    ckpt.write_text(f"ckpt:{job.job_id}\n", encoding="utf-8")
    return TaskResult.success()


def eval_model(job: Job, ctx) -> TaskResult:
    ckpt = ctx.file_path("ckpt")
    metrics = ctx.file_path("metrics")
    metrics.parent.mkdir(parents=True, exist_ok=True)
    metrics.write_text(f"score({ckpt.read_text(encoding='utf-8').strip()})\n", encoding="utf-8")
    return TaskResult.success()


def register_all_tasks(pipeline: TaskLite) -> None:
    pipeline.register_task(Task("train", train_model))
    pipeline.register_task(Task("eval", eval_model))


def build_rename_mapping(data_root: Path, *, include_downstream: bool = True) -> dict[str, Job]:
    """构造旧 UID -> 新 Job 的演进映射。"""
    new_train = Job(
        "train",
        "ds1_l2_0",
        payload={"dataset": "ds1", "l2": 0.0},
        files={"ckpt": FileSpec(data_root / "ds1_l2_0" / "model.pt", role="output")},
    )
    mapping: dict[str, Job] = {"train::ds1": new_train}
    if include_downstream:
        mapping["eval::ds1"] = Job(
            "eval",
            "ds1_l2_0",
            payload={"dataset": "ds1", "l2": 0.0},
            depends_on=[new_train.uid],
            files={
                "ckpt": FileSpec.from_output(new_train.uid, "ckpt", role="input"),
                "metrics": FileSpec(data_root / "ds1_l2_0" / "metrics.txt", role="output"),
            },
        )
    return mapping


# ── 2. 原子改名核心逻辑 ──────────────────────────────────────────────


def migrate_rename_branch(
    state_path: Path,
    *,
    data_root: Path,
    include_downstream: bool,
    apply: bool,
) -> int:
    pipeline = TaskLite(backend="sqlite", state_path=state_path)
    register_all_tasks(pipeline)
    try:
        mapping = build_rename_mapping(data_root, include_downstream=include_downstream)
        with pipeline.edit(dry_run=not apply, note="add l2 ablation dimension to uid") as tx:
            present_mapping = {old_uid: new_job for old_uid, new_job in mapping.items() if old_uid in tx}
            tx.replace_many(present_mapping)
            print(tx.diff())

        if apply:
            print("\n=== 改名前后快照报告 ===")
            print(StateSnapshot.report(tx.before, tx.after))
        return 0
    except TaskLiteError as exc:
        print(f"\n改名被拒绝并已完整回滚：\n{exc}")
        return 1
    finally:
        pipeline.close()


# ── 3. 自包含演示入口 ────────────────────────────────────────────────


def _run_self_demo() -> int:
    with tempfile.TemporaryDirectory(prefix="tasklite_demo_rename_") as tmp:
        root = Path(tmp)
        state_path = root / "state.db"
        data_root = root / "data"

        # 步骤 A：在旧命名体系下跑完 train::ds1，eval::ds1 仍在 pending
        p = TaskLite(backend="sqlite", state_path=state_path, max_workers=1)
        register_all_tasks(p)
        try:
            old_train = Job(
                "train",
                "ds1",
                payload={"dataset": "ds1"},
                files={"ckpt": FileSpec(data_root / "ds1" / "model.pt", role="output")},
            )
            old_eval = Job(
                "eval",
                "ds1",
                payload={"dataset": "ds1"},
                depends_on=[old_train.uid],
                files={
                    "ckpt": FileSpec.from_output(old_train.uid, "ckpt", role="input"),
                    "metrics": FileSpec(data_root / "ds1" / "metrics.txt", role="output"),
                },
            )
            p.enqueue(old_train)
            p.run(install_signals=False)
            p.enqueue(old_eval)
        finally:
            p.close()

        # 步骤 B：OS 层先将目录改名 ds1 -> ds1_l2_0
        shutil.move(str(data_root / "ds1"), str(data_root / "ds1_l2_0"))

        # 步骤 C：故意漏迁下游 eval::ds1 -> 必被孤儿检测拦截
        print("--- [演练 1] 故意漏迁下游 eval::ds1（应被孤儿检测拦下）---")
        rc_bad = migrate_rename_branch(
            state_path, data_root=data_root, include_downstream=False, apply=False
        )
        assert rc_bad == 1

        # 步骤 D：上下游成对改名 -> dry_run 预览 -> --apply 正式落盘
        print("\n--- [演练 2] 上下游成对改名（dry_run=True 预览）---")
        migrate_rename_branch(state_path, data_root=data_root, include_downstream=True, apply=False)

        print("\n--- [演练 3] 执行 dry_run=False 原子落盘 ---")
        rc = migrate_rename_branch(state_path, data_root=data_root, include_downstream=True, apply=True)
        if rc != 0:
            return rc

        # 验证：老键彻底消失，新键继承状态，直接 run() 即可无缝跑通 eval::ds1_l2_0
        p = TaskLite(backend="sqlite", state_path=state_path, max_workers=1)
        register_all_tasks(p)
        try:
            p.run(install_signals=False)
        finally:
            p.close()

        with StateSnapshot.open(state_path, mode="copy") as snap:
            assert snap.get_job("train::ds1") is None
            assert snap.get_job("eval::ds1") is None
            new_eval = snap.get_job("eval::ds1_l2_0")
            assert new_eval is not None and new_eval["status"] == "succeeded"
            # 旧 UID 的历史 Attempt 审计轨迹依然完整保留
            assert len(list(snap.iter_attempts(job_uid="train::ds1"))) == 1
        print("\n[OK] 分支改名迁移验证通过：漏迁拦截生效，改名后下游无缝衔接且旧 Attempt 完好保留。")
        return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="场景 3：新增维度致老枝条改名迁移脚本")
    parser.add_argument("--state-path", type=Path, help="已有 SQLite 状态库路径（留空则运行自包含演示）")
    parser.add_argument("--data-root", type=Path, help="产物根目录（与 --state-path 同用）")
    parser.add_argument("--apply", action="store_true", help="真正提交落盘（默认 dry_run=True 仅预览）")
    args = parser.parse_args()

    if args.state_path is None:
        return _run_self_demo()
    if args.data_root is None:
        parser.error("指定 --state-path 时必须同时指定 --data-root")
    return migrate_rename_branch(
        args.state_path, data_root=args.data_root, include_downstream=True, apply=args.apply
    )


if __name__ == "__main__":
    raise SystemExit(main())
