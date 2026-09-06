"""在途作业与生命周期跟踪深模块（Deep Execution Lifecycle Module）。"""

from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import (
    Dict, Iterator, List, Mapping, Optional, Tuple, TYPE_CHECKING
)

if TYPE_CHECKING:
    from ..models.job import Job
    from ..models.state import PipelineState
    from .executor import JobHandle
    from .resource import ResourceManager, ResourceLease

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
    lease: Optional["ResourceLease"] = None

    @property
    def is_pseudo(self) -> bool:
        """是否为伪条目（崩溃恢复或 abort 消费路径）。"""
        return self.handle is None

    def release_resources(self) -> None:
        """释放关联的资源租约或 acquired 列表。"""
        if self.lease is not None:
            self.lease.release()


class InFlightTracker(MutableMapping[str, InFlightJob]):
    """统一在途任务生命周期跟踪器深模块。

    统一内敛：
    1. 在途任务条目管理与 PipelineState 状态同步（原子登记/注销，防身份真空）；
    2. 活动子进程句柄批量提取与轮询；
    3. 异常/停机时的统一资源安全释放与伪条目合成；
    4. 字典语义 MutableMapping 兼容。
    """

    def __init__(self, entries: Optional[Mapping[str, InFlightJob]] = None) -> None:
        self._entries: Dict[str, InFlightJob] = dict(entries) if entries is not None else {}

    def __getitem__(self, uid: str) -> InFlightJob:
        return self._entries[uid]

    def __setitem__(self, uid: str, entry: InFlightJob) -> None:
        self._entries[uid] = entry

    def __delitem__(self, uid: str) -> None:
        del self._entries[uid]

    def __iter__(self) -> Iterator[str]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, uid: object) -> bool:
        return uid in self._entries

    def get(self, uid: str, default: Optional[InFlightJob] = None) -> Optional[InFlightJob]:
        return self._entries.get(uid, default)

    def pop(self, uid: str, default: Optional[InFlightJob] = None) -> Optional[InFlightJob]:
        return self._entries.pop(uid, default)

    def clear(self) -> None:
        self._entries.clear()

    def register(
        self,
        entry: InFlightJob,
        *,
        state: Optional["PipelineState"] = None,
    ) -> None:
        """原子登记在途任务到内存与 PipelineState 索引。"""
        self._entries[entry.uid] = entry
        if state is not None:
            state.register_in_flight(entry.uid)

    def unregister(
        self,
        uid: str,
        *,
        state: Optional["PipelineState"] = None,
    ) -> Optional[InFlightJob]:
        """注销在途任务。"""
        entry = self._entries.pop(uid, None)
        if state is not None:
            state.unregister_in_flight(uid)
        return entry

    def active_handles(self) -> List[JobHandle]:
        """收集所有活动的真实子进程句柄（排除 handle=None 的伪条目）。"""
        return [
            entry.handle for entry in self._entries.values()
            if entry.handle is not None
        ]

    def release_all_acquired(
        self,
        resource_mgr: "ResourceManager",
    ) -> None:
        """释放所有在途任务已占用的资源（防泄漏）。"""
        for entry in self._entries.values():
            if entry.acquired:
                resource_mgr.release_all(entry.acquired, uid=entry.uid)

    @staticmethod
    def create_pseudo_entry(
        uid: str,
        job_dict: dict,
        job: Job,
    ) -> InFlightJob:
        """合成安全伪条目（用于崩溃恢复或 abort 结果提交）。"""
        return InFlightJob(
            uid=uid,
            job_dict=job_dict,
            job=job,
            acquired=[],
            handle=None,
            job_start=None,
        )


__all__ = [
    "InFlightJob",
    "InFlightTracker",
]

