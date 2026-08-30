"""框架默认的**管线脚手架**通用工具（纯数据转换与进度展示）。

涵盖：
- ``content_fingerprint`` 与 ``sanitize_job_component``：
  内容任务 job_id 的通用派生原语（单射、无随机后缀、确定性）；
- ``job_ref`` / ``progress_hook`` / ``slice_list``：控制台输出与批次切片工具。
"""

import hashlib
import logging
from typing import Any, Iterable, List, Optional, Sequence, Union

from .utils.injective import sanitize_identifier

logger = logging.getLogger(__name__)


def sanitize_job_component(value: Any, *, max_len: int = 120) -> str:
    r"""把任意字符串**转义**净化为可作 job_id 成分的串（单射，不删字符）。

    单射转义与长度截断统一委托底层 ``utils.injective.sanitize_identifier``。
    """
    return sanitize_identifier(value, max_len=max_len)


def content_fingerprint(parts: Iterable[Union[str, int, float]], *, version: str = "") -> str:
    """内容指纹 → 确定性 job_id 片段（sha1，截 16 位，可读）。

    任何输入变化（含 bump ``version`` 盐）→ 指纹变化 → wall 不命中 → 重跑。
    与“job_id 确定性、禁止随机后缀”红线一致：给定输入恒同指纹。
    """
    h = hashlib.sha1()
    if version:
        h.update(str(version).encode("utf-8", errors="replace"))
        h.update(b"\x00")
    for p in parts:
        h.update(str(p).encode("utf-8", errors="replace"))
        h.update(b"\x00")
    return h.hexdigest()[:16]


def job_ref(meta: Any) -> str:
    """从 result_meta 提取人类可读引用（#作品id / 画师名 / 任意 id 字段）。"""
    if isinstance(meta, dict):
        if "post_id" in meta:
            return f"#{meta['post_id']}"
        if "artist" in meta:
            return meta["artist"]
        if "id" in meta:
            return str(meta["id"])
    return ""


def progress_hook(uid: str, meta: Any, success: bool, going_to_retry: bool) -> None:
    """父进程侧每 job 终结回调：一行进度输出（钩子）。"""
    task_type = uid.split("::", 1)[0]
    ref = job_ref(meta)
    label = f"{task_type} {ref}" if ref else task_type
    if going_to_retry:
        print(f"  ⟳ {label} 失败，退避重试", flush=True)
    elif success:
        print(f"  ✓ {label}", flush=True)
    else:
        print(f"  ✗ {label} → DLQ", flush=True)


def slice_list(items: List, start: Optional[int], count: Optional[int], limit: Optional[int]) -> List:
    """分批切片：先 limit，再 [start:start+count]。"""
    if limit is not None:
        items = items[:limit]
    if start is not None or count is not None:
        s = start if start is not None else 0
        c = count if count is not None else len(items)
        items = items[s:s + c]
    return items
