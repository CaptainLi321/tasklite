"""在途作业与生命周期跟踪深模块（Deep Execution Lifecycle Module）。"""
from __future__ import annotations

from collections.abc import MutableMapping
from dataclasses import dataclass, field
from typing import (
    Any, Dict, Iterator, List, Mapping, Optional, Tuple, Union, TYPE_CHECKING
)

if TYPE_CHECKING:
    from ..models.job import Job
    from ..models.state import PipelineState
    from .channel import JobHandle
    from .resource import ResourceManager, ResourceLease
    from .store import StateStore

from ..models.job import Job
from .channel import JobHandle


@dataclass
class InFlightJob:
    """一个已派发到子进程、尚未 commit 的 job 的上下文。

    由 DispatchMachine 创建，传给 CompletionMachine 处理结果。
    内敛持有 ResourceLease 或 acquired 列表，提供自归还的 release_resources 接缝。
    ``handle is None`` 表示伪 entry（崩溃恢复/abort 消费路径），此时
    ``expect_in_flight=False``、``acquired=[]``。
    """

    uid: str
    job_dict: dict
    job: Job
    acquired: List[Tuple[str, float]] = field(default_factory=list)
    handle: Optional[JobHandle] = None
    job_start: Optional[float] = None
    lease: Optional["ResourceLease"] = None

    def __post_init__(self) -> None:
        if self.lease is not None and not self.acquired:
            self.acquired = list(self.lease.acquired)

    @property
    def is_pseudo(self) -> bool:
        """是否为伪条目（崩溃恢复或 abort 消费路径）。"""
        return self.handle is None

    def release_resources(self, resource_mgr: Optional["ResourceManager"] = None) -> None:
        """释放关联的资源租约或 acquired 列表（幂等归还）。"""
        if self.lease is not None:
            self.lease.release()
            self.acquired = []
        elif self.acquired:
            if resource_mgr is not None:
                resource_mgr.release_all(self.acquired, uid=self.uid)
            self.acquired = []


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
        state: Optional[Union["PipelineState", "StateStore", Any]] = None,
    ) -> None:
        """原子登记在途任务到内存与 PipelineState 索引。"""
        self._entries[entry.uid] = entry
        if state is not None:
            state.register_in_flight(entry.uid)

    def dispatch(
        self,
        entry: InFlightJob,
        *,
        state: Optional[Union["PipelineState", "StateStore", Any]] = None,
    ) -> InFlightJob:
        """语义化派发接缝：原子登记在途任务并同步内存状态。"""
        self.register(entry, state=state)
        return entry

    def unregister(
        self,
        uid: str,
        *,
        state: Optional[Union["PipelineState", "StateStore", Any]] = None,
    ) -> Optional[InFlightJob]:
        """注销在途任务。"""
        entry = self._entries.pop(uid, None)
        if state is not None:
            state.unregister_in_flight(uid)
        return entry

    def settle(
        self,
        uid: str,
        *,
        state: Optional[Union["PipelineState", "StateStore", Any]] = None,
    ) -> Optional[InFlightJob]:
        """语义化结算接缝：注销在途任务并同步内存状态。"""
        return self.unregister(uid, state=state)

    def active_handles(self) -> List[JobHandle]:
        """收集所有活动的真实子进程句柄（排除 handle=None 的伪条目）。"""
        return [
            entry.handle for entry in self._entries.values()
            if entry.handle is not None
        ]

    def active_uids(self) -> List[str]:
        """收集所有在途任务的 uid 列表。"""
        return list(self._entries.keys())

    def classify_aborted(
        self, completed_map: Mapping[str, Any]
    ) -> Tuple[List[InFlightJob], List[Tuple[InFlightJob, Any]]]:
        """将当前在途任务分类为（已取消待重入队列表, 已完成待提交列表）。"""
        cancelled_entries: List[InFlightJob] = []
        done_entries: List[Tuple[InFlightJob, Any]] = []
        for entry in self._entries.values():
            if entry.uid in completed_map:
                done_entries.append((entry, completed_map[entry.uid]))
            else:
                cancelled_entries.append(entry)
        return cancelled_entries, done_entries

    def release_all_resources(
        self,
        resource_mgr: Optional["ResourceManager"] = None,
    ) -> None:
        """释放所有在途任务已占用的资源（防泄漏并清空 acquired 列表以防二次释放）。"""
        for entry in self._entries.values():
            entry.release_resources(resource_mgr)

    def release_all_acquired(
        self,
        resource_mgr: "ResourceManager",
    ) -> None:
        """向后兼容别名：释放所有在途任务已占用的资源。"""
        self.release_all_resources(resource_mgr)

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
            lease=None,
        )


__all__ = [
    "InFlightJob",
    "InFlightTracker",
]
