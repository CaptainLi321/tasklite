"""DAG 依赖链、具名产物传递与 Run 外显式预授权的可运行示例。

展示内容：
    1. 使用 ``depends_on`` 声明上下游 DAG 依赖链；
    2. 使用 ``FileSpec`` 与 ``FileSpec.from_output(..., role="input")`` 传递具名产物，
       并在 handler 中经 ``ctx.file_path(...)`` 获取规范绝对路径；
    3. 在 Run 外显式调用 ``pipeline.grant_authorization(out_dir, target_type="directory")``，
       使 ``delete_existing=True`` 在非交互模式下安全清理既有旧文件；
    4. 使用 ``StateSnapshot.open(..., mode="copy")`` 零句柄验收最终状态与业务元数据。

运行方式：
    1. 零参数开箱自演示（在临时目录运行并自动校验）：
       python examples/trigger_chain.py
    2. 指定独立运行目录：
       python examples/trigger_chain.py --root ./tmp_chain_demo
"""
from __future__ import annotations

import argparse
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


def produce_data(job: Job, ctx: JobContext) -> TaskResult:
    """上游生产者：向预声明的输出路径写入业务文本。"""
    out_path = ctx.file_path("source")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    content = str(job.payload.get("content", "default data"))
    out_path.write_text(content, encoding="utf-8")
    return TaskResult.success(metadata={"size": len(content)})


def consume_data(job: Job, ctx: JobContext) -> TaskResult:
    """下游消费者：读取上游具名产物并生成转换后的新产物。"""
    in_path = ctx.file_path("source")
    out_path = ctx.file_path("result")
    text = in_path.read_text(encoding="utf-8")
    upper = text.upper()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(upper, encoding="utf-8")
    return TaskResult.success(metadata={"uppercase": upper})


def _ensure_dir_authorized(pipeline: TaskLite, target_dir: Path) -> None:
    """若目标目录尚未持久预授权，则在 Run 外显式授予目录清理权限。"""
    resolved = str(target_dir.resolve())
    existing = {
        str(Path(item["path"]).resolve())
        for item in pipeline.list_authorizations()
    }
    if resolved not in existing:
        pipeline.grant_authorization(target_dir, target_type="directory")


def run_demo(root: Path) -> int:
    root.mkdir(parents=True, exist_ok=True)
    state_file = root / "state.db"
    out_dir = root / "files"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 预先写入一份旧文件，用于验证 delete_existing=True 在预授权下自动清理旧产物
    stale_source = out_dir / "source.txt"
    stale_source.write_text("stale content before run", encoding="utf-8")

    job_produce = Job(
        "produce",
        "item_1",
        payload={"content": "hello tasklite"},
        files={
            "source": FileSpec(
                out_dir / "source.txt",
                role="output",
                delete_existing=True,
            ),
        },
    )
    job_consume = Job(
        "consume",
        "item_1",
        depends_on=[job_produce.uid],
        files={
            "source": FileSpec.from_output(
                job_produce.uid,
                "source",
                role="input",
            ),
            "result": FileSpec(
                out_dir / "upper.txt",
                role="output",
                delete_existing=True,
            ),
        },
    )

    with TaskLite(backend="sqlite", state_path=state_file, max_workers=2) as pipeline:
        # 含 delete_existing=True 的作业需主进程授权；非交互流水线推荐 Run 外显式预授权
        _ensure_dir_authorized(pipeline, out_dir)
        pipeline.register_task(Task("produce", produce_data))
        pipeline.register_task(Task("consume", consume_data))
        pipeline.enqueue(job_produce)
        pipeline.enqueue(job_consume)
        pipeline.run(install_signals=False)

    # 使用 mode="copy" 只读快照验收终态与产物，不长期占用 SQLite 连接
    with StateSnapshot.open(state_file, mode="copy") as snap:
        prod_rec = snap.get_job(job_produce.uid)
        cons_rec = snap.get_job(job_consume.uid)
        assert prod_rec is not None and prod_rec["status"] == "succeeded"
        assert cons_rec is not None and cons_rec["status"] == "succeeded"
        assert prod_rec["result"]["metadata"] == {"size": len("hello tasklite")}
        assert cons_rec["result"]["metadata"] == {"uppercase": "HELLO TASKLITE"}

    assert (out_dir / "source.txt").read_text(encoding="utf-8") == "hello tasklite"
    assert (out_dir / "upper.txt").read_text(encoding="utf-8") == "HELLO TASKLITE"
    print(
        "[OK] DAG 依赖链与具名产物传递验收通过："
        f"{job_produce.uid} -> {job_consume.uid} -> {(out_dir / 'upper.txt').name}"
    )
    return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="DAG 依赖链与具名产物传递示例")
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="可选的独立状态与产物目录（不传时在临时目录自包含运行）",
    )
    args = parser.parse_args()

    if args.root is not None:
        return run_demo(args.root)
    with tempfile.TemporaryDirectory(prefix="tasklite_demo_trigger_chain_") as tmp:
        return run_demo(Path(tmp))


if __name__ == "__main__":
    raise SystemExit(main())
