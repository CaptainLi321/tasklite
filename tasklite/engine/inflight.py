"""in-flight job 数据类（dispatch / completion / recovery 三机器共享）。"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

from ..models.job import Job
from .executor import JobHandle


@dataclass
class InFlightJob:
    """一个已派发到子进程、尚未 commit 的 job 的上下文。

    由 DispatchMachine 创建，传给 CompletionMachine 处理结果。
    ``acquired`` 记录已 acquire 的资源，``complete_job`` 的 finally 块释放。
    ``handle is None`` 表示伪 entry（崩溃恢复/abort 消费路径），此时
    ``expect_in_flight=False``、``acquired=[]``。
    """

    uid: str
    job_dict: dict
    job: Job
    acquired: List[Tuple[str, float]]
    handle: Optional[JobHandle]
    job_start: Optional[float]
