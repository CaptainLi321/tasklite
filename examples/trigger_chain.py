"""稳定 Job 身份与按输入修改时间重跑的最小可运行示例。"""
from __future__ import annotations

import argparse
from pathlib import Path

from tasklite import Job, TaskLite


def update_source(job, ctx):
    path = ctx.output_roots / "source.txt"
    content = job.payload["content"]
    changed = not path.exists() or path.read_text(encoding="utf-8") != content
    if changed:
        path = Path(ctx.declare_output(path))
        path.write_text(content, encoding="utf-8")
    return {"changed": changed}


def read_source(job, ctx):
    path = Path(ctx.declare_input(ctx.output_roots / "source.txt"))
    return {"text": path.read_text(encoding="utf-8").upper()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="独立的示例状态与产物目录")
    args = parser.parse_args()
    pipeline = TaskLite("trigger_example", state_dir=args.root / "state",
                        output_roots=args.root / "files")
    pipeline.register_task("source", update_source, trigger="always")
    pipeline.register_task("read", read_source, trigger="on_input_change")
    try:
        for content in ("alpha", "alpha", "beta"):
            pipeline.enqueue([
                Job("read", "document", depends_on=["source::document"]),
                Job("source", "document", payload={"content": content}),
            ])
            summary = pipeline.run()
            text = pipeline.backend.load_successes()["read::document"]["text"]
            print(f"输入={content} 成功={summary.stats['succeeded']} "
                  f"跳过={summary.stats['skipped']} 结果={text}")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
