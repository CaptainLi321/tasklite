"""零句柄快照前后对比报告、选择性运行排除与输入变更触发器的可运行示例。

展示内容：
    1. ``StateSnapshot.open(..., mode="copy")`` 在 ``run()`` 前后零连接占用捕获快照，
       并打印 ``StateSnapshot.report(before, after)`` 对比报告；
    2. ``pipeline.run(exclude=Exclude(task_type="heavy_report"))`` 本轮跳过重型下游并干净留队，
       下一轮放开执行；
    3. ``trigger="on_input_change"``：输入文件未变时自动跳过，修改输入文件后自动触发重算。

运行方式：
    python examples/snapshot_report_and_exclude.py
"""
from __future__ import annotations

import hashlib
import sys
import tempfile
from pathlib import Path

from tasklite import (
    FileSpec,
    Job,
    JobContext,
    StateSnapshot,
    Task,
    TaskLite,
    TaskResult,
)
from tasklite.selection import Exclude


def prepare_data(job: Job, ctx: JobContext) -> TaskResult:
    """轻量前置任务：产出供重型报告读取的中间数据。"""
    out_path = ctx.file_path("prepared")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(f"prepared:{job.job_id}\n", encoding="utf-8")
    return TaskResult.success()


def build_heavy_report(job: Job, ctx: JobContext) -> TaskResult:
    """重型下游报告任务：读取前置产物并写出报告。"""
    src_path = ctx.file_path("prepared")
    report_path = ctx.file_path("report")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    body = src_path.read_text(encoding="utf-8").strip()
    report_path.write_text(f"REPORT({body})\n", encoding="utf-8")
    return TaskResult.success(metadata={"source": body})


def digest_input(job: Job, ctx: JobContext) -> TaskResult:
    """输入变更敏感任务：计算输入文件摘要。"""
    in_path = ctx.file_path("source")
    raw = in_path.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()[:12]
    return TaskResult.success(metadata={"sha256_prefix": sha, "bytes": len(raw)})


def run_demo() -> int:
    with tempfile.TemporaryDirectory(prefix="tasklite_demo_snapshot_exclude_") as tmp:
        root = Path(tmp)
        state_file = root / "state.db"
        work_dir = root / "work"
        work_dir.mkdir(parents=True, exist_ok=True)

        input_file = work_dir / "config.txt"
        input_file.write_text("version=1\n", encoding="utf-8", newline="\n")

        job_prepare = Job(
            "prepare",
            "batch_1",
            files={"prepared": FileSpec(work_dir / "prepared.txt", role="output")},
        )
        job_report = Job(
            "heavy_report",
            "batch_1",
            depends_on=[job_prepare.uid],
            files={
                "prepared": FileSpec.from_output(job_prepare.uid, "prepared", role="input"),
                "report": FileSpec(work_dir / "report.txt", role="output"),
            },
        )
        job_digest = Job(
            "digest",
            "config",
            files={"source": FileSpec(input_file, role="input")},
        )

        with TaskLite(backend="sqlite", state_path=state_file, max_workers=2) as pipeline:
            pipeline.register_task(Task("prepare", prepare_data))
            pipeline.register_task(Task("heavy_report", build_heavy_report))
            pipeline.register_task(
                Task("digest", digest_input, trigger="on_input_change")
            )

            pipeline.enqueue(job_prepare)
            pipeline.enqueue(job_report)
            pipeline.enqueue(job_digest)

            # ── 阶段 1：排除 heavy_report，先跑前置与首次摘要 ──
            before = StateSnapshot.open(state_file, mode="copy")
            pipeline.run(
                exclude=Exclude(task_type="heavy_report"),
                install_signals=False,
            )
            after_round1 = StateSnapshot.open(state_file, mode="copy")

            print("=== 第 1 轮对比报告（排除 heavy_report）===")
            print(StateSnapshot.report(before, after_round1))

            assert after_round1.get_job(job_prepare.uid)["status"] == "succeeded"
            assert after_round1.get_job(job_report.uid)["status"] == "pending"
            assert list(after_round1.iter_attempts(job_uid=job_report.uid)) == []
            assert len(list(after_round1.iter_attempts(job_uid=job_digest.uid))) == 1

            # ── 阶段 2：放开 heavy_report；同时验证输入未变时 digest 自动跳过 ──
            pipeline.enqueue(job_digest)
            pipeline.run(install_signals=False)
            after_round2 = StateSnapshot.open(state_file, mode="copy")

            print("\n=== 第 2 轮对比报告（放开 heavy_report，输入未变跳过 digest）===")
            print(StateSnapshot.report(after_round1, after_round2))

            assert after_round2.get_job(job_report.uid)["status"] == "succeeded"
            assert len(list(after_round2.iter_attempts(job_uid=job_digest.uid))) == 1

            # ── 阶段 3：修改输入文件内容，验证 on_input_change 自动触发重算 ──
            input_file.write_text("version=2-changed\n", encoding="utf-8", newline="\n")
            pipeline.enqueue(job_digest)
            pipeline.run(install_signals=False)
            after_round3 = StateSnapshot.open(state_file, mode="copy")

            digest_attempts = list(after_round3.iter_attempts(job_uid=job_digest.uid))
            assert len(digest_attempts) == 2
            digest_rec = after_round3.get_job(job_digest.uid)
            assert digest_rec is not None and digest_rec["status"] == "succeeded"
            assert digest_rec["result"]["metadata"]["bytes"] == len("version=2-changed\n")

            before.close()
            after_round1.close()
            after_round2.close()
            after_round3.close()

        print("\n[OK] 快照对比报告、Exclude 选择性留队与 on_input_change 触发器验证通过。")
        return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    return run_demo()


if __name__ == "__main__":
    raise SystemExit(main())
