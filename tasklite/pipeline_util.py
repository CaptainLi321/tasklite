"""通用管线脚手架兼容适配器（向后兼容重导出）。

推荐直接从根命名空间或相应深模块导入：
- 标识符与指纹：``from tasklite.utils.injective import sanitize_job_component, content_fingerprint``
- 控制台钩子与切片：``from tasklite.pipeline import job_ref, progress_hook, slice_list``
- 或统一从根包导入：``from tasklite import content_fingerprint, progress_hook, ...``
"""

from .pipeline import job_ref, progress_hook, slice_list
from .utils.injective import content_fingerprint, sanitize_identifier, sanitize_job_component

__all__ = [
    "sanitize_job_component",
    "sanitize_identifier",
    "content_fingerprint",
    "job_ref",
    "progress_hook",
    "slice_list",
]

