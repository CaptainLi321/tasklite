"""v2 异常体系：公开契约异常与框架内部崩溃信号。"""
from __future__ import annotations


class TaskLiteError(Exception):
    """框架主动发现调用条件不成立时抛出的公开异常。"""

    pass


class _ArtifactIOError(BaseException):
    """声明读写或授权清理的环境故障，需停止当前运行。

    继承 BaseException，防止 handler 的 except Exception 误吞后发布成功。
    """


__all__ = [
    "TaskLiteError",
    "_ArtifactIOError",
]
