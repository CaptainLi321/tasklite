"""面向业务方的展示与切片工具。

提供 job_ref / progress_hook / slice_list 三个便利工具，经
``tasklite/__init__.py`` 统一导出。progress_hook 按
``on_attempt_finished(uid, *, outcome)`` 钩子契约适配——布尔语义经
AttemptFinish 值对象承载，不裸传四个散装参数。
"""
from __future__ import annotations

from typing import Any

from .engine.types import AttemptFinish

__all__ = [
    "job_ref",
    "progress_hook",
    "slice_list",
]


def job_ref(meta: Any) -> str:
    """从 result_meta 提取人类可读引用（#作品id / 画师名 / 任意 id 字段）。"""
    if isinstance(meta, dict):
        if "post_id" in meta:
            return f"#{meta['post_id']}"
        if "artist" in meta:
            return str(meta["artist"])
        if "id" in meta:
            return str(meta["id"])
    return ""


def progress_hook(uid: str, *, outcome: AttemptFinish) -> None:
    """控制台每 attempt 终结回调：一行进度输出（on_attempt_finished 标准适配器）。

    重试文案为「重入队」而非任何延迟字眼——v2 重试节奏唯一经
    RequeuePolicy seam 表达，默认立即重入队，标准适配器不预设延迟语义。
    """
    task_type = uid.split("::", 1)[0]
    ref = job_ref(outcome.meta)
    label = f"{task_type} {ref}" if ref else task_type
    if outcome.going_to_retry:
        print(f"  ⟳ {label} 失败，重入队重试", flush=True)
    elif outcome.success:
        print(f"  ✓ {label}", flush=True)
    else:
        print(f"  ✗ {label} → 失败档案", flush=True)


def slice_list(
    items: list[Any],
    *,
    start: int | None = None,
    count: int | None = None,
    limit: int | None = None,
) -> list[Any]:
    """分批切片：先 limit 总量截断，再取 [start : start+count] 窗口。

    参数纪律：start / count / limit 一律 keyword-only——三者语义易混
    （``start+count`` 是窗口定位、``limit`` 是总量截断，先 limit 后窗口），
    裸位置调用会把 limit 错传成 start 而静默得到错误切片。
    """
    if limit is not None:
        items = items[:limit]
    if start is not None or count is not None:
        s = start if start is not None else 0
        c = count if count is not None else len(items)
        items = items[s:s + c]
    return items
