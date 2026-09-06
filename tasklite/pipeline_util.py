"""TaskLite 通用脚手架与工具函数（向下兼容模块路径）。

提供任务指纹、单射净化、进度展示钩子与切片辅助。
推荐直接从顶级包导入：``from tasklite import progress_hook, job_ref, ...``。
"""

from .pipeline import job_ref, progress_hook, slice_list
from .utils.injective import content_fingerprint, sanitize_job_component

__all__ = [
    "sanitize_job_component",
    "content_fingerprint",
    "progress_hook",
    "job_ref",
    "slice_list",
]
