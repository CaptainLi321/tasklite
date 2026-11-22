"""迁移示例 2：历史游离缓存纳管（add(..., status="succeeded")）。

典型场景：
    在引入 TaskLite 之前，磁盘上已有若干通过手工脚本跑完并保存在规范路径下的
    大型缓存产物（例如耗时数小时的预处理特征或外部下载模型权重）。
    希望把它们直接登记为 ``succeeded`` 节点，让后续新入队的下游任务通过
    ``FileSpec.from_output`` 直接复用，而无需重算。

核心原语与安全保证：
    - ``tx.add(cache_job, status="succeeded")``：
      1. 防覆盖：若同 UID 已存在于库中，立即抛 ``TaskLiteError``；
      2. 强制验盘：提交期 Error 4 校验所有保留型 ``role="output"`` 路径真实在场，
         并自动将规范绝对路径双写至外层 ``files`` 与 ``artifacts``；
      3. 零删除副作用：完全忽略 ``delete_existing`` 等标志、不触碰磁盘、不要求路径授权；
      4. 审计隔离：不伪造 ``Attempt`` 记录，``annotations.completion_source == "manual"``。

运行方式：
    python examples/edit_migrations/02_adopt_legacy_cache.py
    python examples/edit_migrations/02_adopt_legacy_cache.py --state-path ./state.db --cache-dir ./legacy_cache [--apply]
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


# ── 1. 业务任务定义 ──────────────────────────────────────────────────


def preprocess(job: Job, ctx) -> TaskResult:
    out_path = ctx.file_path("cleaned")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(f"fresh:{job.job_id}\n", encoding="utf-8")
    return TaskResult.success()


def evaluate(job: Job, ctx) -> TaskResult:
    in_path = ctx.file_path("cleaned")
    report_path = ctx.file_path("report")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    content = in_path.read_text(encoding="utf-8").strip()
    report_path.write_text(f"evaluated({content})\n", encoding="utf-8")
    return TaskResult.success(metadata={"source": content})


def register_all_tasks(pipeline: TaskLite) -> None:
    pipeline.register_task(Task("preprocess", preprocess))
    pipeline.register_task(Task("evaluate", evaluate))


def build_legacy_cache_jobs(cache_dir: Path, sample_ids: list[str]) -> list[Job]:
    """为历史上已手工算好的缓存产物构造待纳管 Job。"""
    return [
        Job(
            "preprocess",
            sid,
            payload={"sample_id": sid},
            files={
                # 即便声明了 delete_existing=True，edit() 纳管期间也保证零磁盘删除
                "cleaned": FileSpec(
                    cache_dir / f"{sid}.parquet",
                    role="output",
                    delete_existing=True,
                ),
            },
        )
        for sid in sample_ids
    ]


# ── 2. 原子纳管核心逻辑 ──────────────────────────────────────────────


def migrate_adopt_legacy_cache(
    state_path: Path,
    *,
    cache_dir: Path,
    sample_ids: list[str],
    apply: bool,
) -> int:
    pipeline = TaskLite(backend="sqlite", state_path=state_path)
    register_all_tasks(pipeline)
    try:
        with pipeline.edit(dry_run=not apply, note="adopt precomputed legacy cache") as tx:
            for cache_job in build_legacy_cache_jobs(cache_dir, sample_ids):
                # 只纳管尚未登记的节点；若强行对已有节点调 tx.add() 会抛 TaskLiteError 防覆盖
                if cache_job.uid not in tx:
                    tx.add(cache_job, status="succeeded")

            print(tx.diff())

        if apply:
            print("\n=== 纳管前后快照报告 ===")
            print(StateSnapshot.report(tx.before, tx.after))
        return 0
    except TaskLiteError as exc:
        print(f"\n纳管被拒绝并已回滚：\n{exc}")
        return 1
    finally:
        pipeline.close()


# ── 3. 自包含演示入口 ────────────────────────────────────────────────


def _run_self_demo() -> int:
    sample_ids = ["s001", "s002"]
    with tempfile.TemporaryDirectory(prefix="tasklite_demo_adopt_") as tmp:
        root = Path(tmp)
        state_path = root / "state.db"
        cache_dir = root / "legacy_cache"
        report_dir = root / "reports"
        cache_dir.mkdir(parents=True, exist_ok=True)

        # 初始化空状态库，并在磁盘放好历史手工算出的产物文件
        p = TaskLite(backend="sqlite", state_path=state_path)
        p.close()
        for sid in sample_ids:
            (cache_dir / f"{sid}.parquet").write_text(f"legacy_cached_{sid}\n", encoding="utf-8")

        print("--- [步骤 1] dry_run=True 预览历史缓存纳管 ---")
        migrate_adopt_legacy_cache(state_path, cache_dir=cache_dir, sample_ids=sample_ids, apply=False)

        print("\n--- [步骤 2] dry_run=False 原子纳管 ---")
        rc = migrate_adopt_legacy_cache(state_path, cache_dir=cache_dir, sample_ids=sample_ids, apply=True)
        if rc != 0:
            return rc

        # 步骤 3：下游直接通过 FileSpec.from_output 消费刚纳管的历史缓存并执行 run()
        print("\n--- [步骤 3] 下游作业直接通过 from_output 消费纳管产物 ---")
        p = TaskLite(backend="sqlite", state_path=state_path, max_workers=1)
        register_all_tasks(p)
        try:
            for sid in sample_ids:
                p.enqueue(
                    Job(
                        "evaluate",
                        sid,
                        depends_on=[f"preprocess::{sid}"],
                        files={
                            "cleaned": FileSpec.from_output(f"preprocess::{sid}", "cleaned", role="input"),
                            "report": FileSpec(report_dir / f"{sid}.txt", role="output"),
                        },
                    )
                )
            p.run(install_signals=False)
        finally:
            p.close()

        with StateSnapshot.open(state_path, mode="copy") as snap:
            pre_s1 = snap.get_job("preprocess::s001")
            eval_s1 = snap.get_job("evaluate::s001")
            assert pre_s1 is not None and pre_s1["status"] == "succeeded"
            assert pre_s1["result"]["annotations"]["completion_source"] == "manual"
            assert list(snap.iter_attempts(job_uid="preprocess::s001")) == []
            assert eval_s1 is not None and eval_s1["status"] == "succeeded"
            assert (report_dir / "s001.txt").read_text(encoding="utf-8").strip() == "evaluated(legacy_cached_s001)"
        print("[OK] 游离缓存纳管验证通过：零 Attempt 伪造、零磁盘删除、下游 from_output 无缝运行。")
        return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="场景 2：历史游离缓存纳管迁移脚本")
    parser.add_argument("--state-path", type=Path, help="已有 SQLite 状态库路径（留空则运行自包含演示）")
    parser.add_argument("--cache-dir", type=Path, help="历史缓存文件目录（与 --state-path 同用）")
    parser.add_argument("--samples", nargs="+", default=["s001", "s002"], help="待纳管样本 ID 列表")
    parser.add_argument("--apply", action="store_true", help="真正提交落盘（默认 dry_run=True 仅预览）")
    args = parser.parse_args()

    if args.state_path is None:
        return _run_self_demo()
    if args.cache_dir is None:
        parser.error("指定 --state-path 时必须同时指定 --cache-dir")
    return migrate_adopt_legacy_cache(
        args.state_path,
        cache_dir=args.cache_dir,
        sample_ids=args.samples,
        apply=args.apply,
    )


if __name__ == "__main__":
    raise SystemExit(main())
