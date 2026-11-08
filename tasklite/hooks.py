"""面向业务方的展示与切片工具。

提供 job_ref / slice_list 便利工具，经 ``tasklite/__init__.py`` 统一导出。
"""
from __future__ import annotations

from typing import Any

from .utils.encoding import parse_job_uid

__all__ = [
    "job_ref",
    "slice_list",
]


def job_ref(metadata: Any) -> str:
    """从 result_metadata 提取人类可读引用（#作品id / 画师名 / 任意 id 字段）。"""
    if isinstance(metadata, dict):
        if "post_id" in metadata:
            return f"#{metadata['post_id']}"
        if "artist" in metadata:
            return str(metadata["artist"])
        if "id" in metadata:
            return str(metadata["id"])
    return ""


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
