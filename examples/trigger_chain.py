"""稳定 Job 身份与 DAG 拓扑依赖链的最小可运行示例。"""
from __future__ import annotations

import argparse
from pathlib import Path

from tasklite import (
    FileSpec,
    Job,
    Task,
    TaskLite,
    TaskResult,
)


def produce_data(job, ctx):
    """上游生产者：声明输出文件并写入业务数据。"""
    out_path = ctx.declare_file(
        "output_file",
        FileSpec("source.txt", role="output", delete_existing=True),
    )
    content = job.payload.get("content", "default data")
    out_path.write_text(content, encoding="utf-8")
    return TaskResult.success(metadata={"size": len(content)})


def consume_data(job, ctx):
    """下游消费者：读取上游生成的文件并进行处理。"""
    in_path = Path(ctx.base_dir) / "source.txt"
    text = in_path.read_text(encoding="utf-8")
    return TaskResult.success(metadata={"uppercase": text.upper()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="独立的示例状态与产物目录")
    args = parser.parse_args()

    args.root.mkdir(parents=True, exist_ok=True)
    state_file = args.root / "state.db"
    out_dir = args.root / "files"
    out_dir.mkdir(parents=True, exist_ok=True)

    pipeline = TaskLite(
        backend="sqlite",
        state_path=state_file,
        output_roots=out_dir,
    )
    pipeline.authorizer = lambda path, target_type: True
    pipeline.register_task(Task("produce", produce_data))
    pipeline.register_task(Task("consume", consume_data))

    try:
        job_produce = Job("produce", "item_1", payload={"content": "hello tasklite"})
        job_consume = Job("consume", "item_1", depends_on=["produce::item_1"])

        pipeline.enqueue(job_produce)
        pipeline.enqueue(job_consume)
        pipeline.run(install_signals=False)
        print("Pipeline executed successfully!")
    finally:
        pipeline.stop()
        pipeline.close()


if __name__ == "__main__":
    main()
