"""v2 异常体系：公开契约异常与框架内部崩溃信号。"""
from __future__ import annotations


class TaskLiteError(Exception):
    """框架主动发现调用条件不成立时抛出的公开异常。"""

    pass


class _ArtifactIOError(BaseException):
    """声明读写或授权清理的环境故障，需停止当前运行。

    继承 BaseException，防止 handler 的 except Exception 误吞后发布成功。
    """


class _CommitCrashSignal(BaseException):
    """内部信号：commit 失败后已重入队，需立即崩溃。

    不变式：继承 ``BaseException``（而非 ``Exception``）——防止用户
    handler 的 ``except Exception`` 误吞崩溃信号。
    """

    pass


class _JobTerminated(BaseException):
    """内部信号：当前 job 已终结（失败档案阈值命中），需立即停止处理。"""

    pass


__all__ = [
    "TaskLiteError",
    "_ArtifactIOError",
    "_CommitCrashSignal",
    "_JobTerminated",
]
