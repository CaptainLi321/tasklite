"""动态衍生子作业、事务性增量游标与两轮分页发现的可运行示例。

展示内容：
    1. ``ctx.spawn()`` 动态衍生下游子作业（父任务成功提交时原子入队）；
    2. ``ctx.get_cursor()`` / ``ctx.set_cursor()`` 事务性增量游标持久化；
    3. ``tasklite.wrappers.discovery.register_discovery`` 两轮增量分页扫描：
       第 1 轮抓取首批条目并推进已见游标，第 2 轮仅抓取新增条目并在整页命中时自动刹车。

运行方式：
    python examples/dynamic_spawn_and_cursor.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from tasklite import (
    Job,
    JobContext,
    StateSnapshot,
    Task,
    TaskLite,
    TaskResult,
    make_job_uid,
)
from tasklite.wrappers.discovery import register_discovery


# ── 1. 动态衍生与业务游标示例 handler ─────────────────────────────────


def sync_batch(job: Job, ctx: JobContext) -> TaskResult:
    """读取高水位游标，仅衍生高于游标的新记录并推进游标。"""
    cursor_key = "orders_watermark"
    watermark = int(ctx.get_cursor(cursor_key) or "0")
    records: list[dict[str, Any]] = job.payload["records"]

    spawned = 0
    max_seq = watermark
    for rec in records:
        seq = int(rec["seq"])
        if seq <= watermark:
            continue
        ctx.spawn(
            Job(
                "process_order",
                f"order_{seq}",
                payload={"seq": seq, "title": rec["title"]},
            )
        )
        spawned += 1
        max_seq = max(max_seq, seq)

    if max_seq > watermark:
        ctx.set_cursor(cursor_key, str(max_seq))
    return TaskResult.success(
        metadata={"prev_watermark": watermark, "new_watermark": max_seq, "spawned": spawned}
    )


def process_order(job: Job, ctx: JobContext) -> TaskResult:
    """处理动态衍生的下游订单任务。"""
    return TaskResult.success(
        metadata={"seq": job.payload["seq"], "title": str(job.payload["title"]).upper()}
    )


# ── 2. register_discovery 两轮增量分页扫描回调（须模块级可 pickle）─────


def fetch_feed_page(job: Any, ctx: Any, page: int) -> list[dict[str, str]]:
    """按时间倒序分页读取模拟外部源，并记录实际拉取页码。"""
    feed_path = Path(job.payload["feed_path"])
    log_path = Path(job.payload["fetch_log_path"])
    page_size = int(job.payload.get("page_size", 2))

    # 记录每轮实际拉取的页码，便于验证第 2 轮在整页命中时提前刹车
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(f"{page}\n")

    items: list[dict[str, str]] = json.loads(feed_path.read_text(encoding="utf-8"))
    start = (page - 1) * page_size
    return items[start : start + page_size]


def extract_post_id(item: dict[str, str]) -> str:
    return str(item["id"])


def spawn_post_task(job: Any, ctx: Any, item: dict[str, str], content_id: str) -> None:
    ctx.spawn(
        Job(
            "process_post",
            content_id,
            payload={"id": item["id"], "title": item["title"]},
        )
    )


def process_post(job: Job, ctx: JobContext) -> TaskResult:
    return TaskResult.success(metadata={"title": job.payload["title"]})


def _write_feed(feed_path: Path, items: list[dict[str, str]]) -> None:
    feed_path.write_text(json.dumps(items, ensure_ascii=False), encoding="utf-8")


def run_demo() -> int:
    with tempfile.TemporaryDirectory(prefix="tasklite_demo_spawn_cursor_") as tmp:
        root = Path(tmp)
        state_file = root / "state.db"
        feed_path = root / "feed.json"
        round1_log = root / "round1_pages.log"
        round2_log = root / "round2_pages.log"

        # 第 1 轮源数据（时间倒序：最新在前，每页 2 条，共 2 页）
        round1_feed = [
            {"id": "post_4", "title": "第四篇"},
            {"id": "post_3", "title": "第三篇"},
            {"id": "post_2", "title": "第二篇"},
            {"id": "post_1", "title": "第一篇"},
        ]
        _write_feed(feed_path, round1_feed)

        with TaskLite(backend="sqlite", state_path=state_file, max_workers=2) as pipeline:
            pipeline.register_task(Task("sync_batch", sync_batch, trigger="always"))
            pipeline.register_task(Task("process_order", process_order))
            pipeline.register_task(Task("process_post", process_post))
            register_discovery(
                pipeline,
                task_type="discover_feed",
                fetch_func=fetch_feed_page,
                id_func=extract_post_id,
                process_item_func=spawn_post_task,
                process_task_type="process_post",
            )

            # 第 1 轮：执行游标批次同步 + 首轮全量发现
            pipeline.enqueue(
                Job(
                    "sync_batch",
                    "main",
                    payload={
                        "records": [
                            {"seq": 101, "title": "alpha"},
                            {"seq": 102, "title": "beta"},
                        ]
                    },
                )
            )
            pipeline.enqueue(
                Job(
                    "discover_feed",
                    "timeline",
                    payload={
                        "feed_path": str(feed_path),
                        "fetch_log_path": str(round1_log),
                        "page_size": 2,
                    },
                )
            )
            pipeline.run(install_signals=False)

            assert pipeline.get_cursor("orders_watermark") == "102"

            # 第 2 轮：源头部新增 2 条（post_6、post_5），旧条目顺延至第 2、3 页
            round2_feed = [
                {"id": "post_6", "title": "第六篇"},
                {"id": "post_5", "title": "第五篇"},
                *round1_feed,
            ]
            _write_feed(feed_path, round2_feed)

            pipeline.enqueue(
                Job(
                    "sync_batch",
                    "main",
                    payload={
                        "records": [
                            {"seq": 102, "title": "beta-already-seen"},
                            {"seq": 103, "title": "gamma"},
                        ]
                    },
                )
            )
            pipeline.enqueue(
                Job(
                    "discover_feed",
                    "timeline",
                    payload={
                        "feed_path": str(feed_path),
                        "fetch_log_path": str(round2_log),
                        "page_size": 2,
                    },
                )
            )
            pipeline.run(install_signals=False)

            assert pipeline.get_cursor("orders_watermark") == "103"

        round1_pages = [int(x) for x in round1_log.read_text(encoding="utf-8").splitlines() if x]
        round2_pages = [int(x) for x in round2_log.read_text(encoding="utf-8").splitlines() if x]
        # 第 1 轮扫到第 3 页（空页）结束；第 2 轮第 2 页整页命中已见集合，自动刹车不翻第 3 页
        assert round1_pages == [1, 2, 3], round1_pages
        assert round2_pages == [1, 2], round2_pages

        with StateSnapshot.open(state_file, mode="copy") as snap:
            orders = list(snap.iter_jobs(task_type="process_order"))
            posts = list(snap.iter_jobs(task_type="process_post"))
            assert len(orders) == 3 and all(j["status"] == "succeeded" for j in orders)
            assert len(posts) == 6 and all(j["status"] == "succeeded" for j in posts)
            # 确认旧条目未在第 2 轮重复执行
            for pid in ("post_1", "post_2", "post_3", "post_4"):
                attempts = list(snap.iter_attempts(job_uid=make_job_uid("process_post", pid)))
                assert len(attempts) == 1, (pid, len(attempts))

        print(f"第 1 轮发现拉取页码: {round1_pages}（扫至空页）")
        print(f"第 2 轮发现拉取页码: {round2_pages}（第 2 页整页命中自动刹车）")
        print("[OK] 动态衍生、事务性游标与两轮增量分页发现验证通过。")
        return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    return run_demo()


if __name__ == "__main__":
    raise SystemExit(main())
