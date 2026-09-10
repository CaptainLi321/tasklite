"""用户工具函数（从 pipeline.py 门面迁出）。

提供 job_ref / progress_hook / slice_list 三个面向业务方的便利工具，
保持对外公共 API 面不变（经 tasklite/__init__.py 统一导出）。
"""
from __future__ import annotations

from typing import Any, List, Optional


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


def progress_hook(uid: str, meta: Any, success: bool, going_to_retry: bool) -> None:
    """控制台每 job 终结回调：一行进度输出（标准钩子适配器）。"""
    task_type = uid.split("::", 1)[0]
    ref = job_ref(meta)
    label = f"{task_type} {ref}" if ref else task_type
    if going_to_retry:
        print(f"  ⟳ {label} 失败，退避重试", flush=True)
    elif success:
        print(f"  ✓ {label}", flush=True)
    else:
        print(f"  ✗ {label} → DLQ", flush=True)


def slice_list(
    items: List[Any],
    start: Optional[int],
    count: Optional[int],
    limit: Optional[int],
) -> List[Any]:
    """分批切片：先 limit，再 [start:start+count]。"""
    if limit is not None:
        items = items[:limit]
    if start is not None or count is not None:
        s = start if start is not None else 0
        c = count if count is not None else len(items)
        items = items[s:s + c]
    return items
