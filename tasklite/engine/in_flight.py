"""v2 在途作业与生命周期跟踪深模块。

统一内敛：
1. 在途任务条目管理与 PipelineState 状态同步（原子登记/注销，防身份真空）；
2. 活动子进程句柄批量提取与轮询；
3. 异常/停机时的统一资源安全释放与伪条目合成；
4. 字典语义 MutableMapping 兼容。
"""
from __future__ import annotations

from collections.abc import MutableMapping
from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.state import PipelineState
    from .resource import ResourceManager

    class InFlightStateIndex(Protocol):
        """在途登记/注销同步的内存状态索引契约。

        PipelineState（直持索引）满足；注册与注销是六集合互斥不变式的
        单一维护点（调用方传 ``store.state``）。
        """

        @property
        def in_flight_uids(self) -> frozenset[str]: ...

        def register_in_flight(self, job_uid: str) -> None: ...

        def unregister_in_flight(self, job_uid: str) -> None: ...

from ..models.job import Job
from .resource import NullResourceLease, ResourceLease
from .types import JobHandle


@dataclass
class InFlightJob:
    """已进入 running、尚未提交结果的执行上下文。

    资源租约或 acquired 列表由 release_resources 幂等归还。
    handle 为 None 时没有 worker，可携带前置操作失败时已获取的资源；
    有 worker 时以 started_monotonic 和句柄期限驱动超时判定。
    """

    job_uid: str
    job_snapshot: dict
    job: Job
    acquired: list[tuple[str, float]] = field(default_factory=list)
    handle: JobHandle | None = None
    started_monotonic: float | None = None
    lease: ResourceLease | None = None
    # 完整执行身份随 running 登记，结算时用于追加 Attempt。
    execution_id: str | None = None
    attempt_no: int = 1
    run_id: str = ""
    started_at: str | None = None
    known_job_uids: frozenset[str] = field(default_factory=frozenset)
    active_files: dict[str, Any] = field(default_factory=dict)
    active_file_sources: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.lease is None:
            if not self.acquired:
                self.lease = NullResourceLease(job_uid=self.job_uid)
        elif not self.acquired:
            self.acquired = list(self.lease.acquired)

    @property
    def is_pseudo(self) -> bool:
        """是否没有关联的 worker 句柄。"""
        return self.handle is None

    def release_resources(self, resource_mgr: ResourceManager | None = None) -> None:
        """释放关联的资源租约或 acquired 列表（幂等归还）。"""
        if self.lease is not None:
            self.lease.release()
            self.acquired = []
        elif self.acquired:
            acquired, self.acquired = self.acquired, []
            if resource_mgr is not None:
                resource_mgr.release_all(acquired, job_uid=self.job_uid)


class InFlightTracker(MutableMapping[str, InFlightJob]):
    """统一在途任务生命周期跟踪器深模块。"""

    def __init__(self, entries: Mapping[str, InFlightJob] | None = None) -> None:
        self._entries: dict[str, InFlightJob] = dict(entries) if entries is not None else {}

    def __getitem__(self, job_uid: str) -> InFlightJob:
        return self._entries[job_uid]

    def __setitem__(self, job_uid: str, entry: InFlightJob) -> None:
        self._entries[job_uid] = entry

    def __delitem__(self, job_uid: str) -> None:
        del self._entries[job_uid]

    def __iter__(self) -> Iterator[str]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, job_uid: object) -> bool:
        return job_uid in self._entries

    def get(self, job_uid: str, default: InFlightJob | None = None) -> InFlightJob | None:  # type: ignore[override]
        return self._entries.get(job_uid, default)

    def pop(self, job_uid: str, default: InFlightJob | None = None) -> InFlightJob | None:  # type: ignore[override]
        return self._entries.pop(job_uid, default)

    @property
    def uids(self) -> frozenset[str]:
        """当前在途作业 UID 集合的不可变快照（单一真相源）。"""
        return frozenset(self._entries.keys())

    def clear(self) -> None:
        self._entries.clear()

    def track(
        self,
        entry: InFlightJob,
        *,
        state: "InFlightStateIndex | None" = None,
    ) -> InFlightJob:
        """登记在途作业条目（单一真相源入口）。

        ``state`` 为内存状态索引（``InFlightStateIndex`` 契约，
        PipelineState 满足，调用方传 ``store.state``），非 None 时同步
        内存 in-flight 索引。
        """
        self._entries[entry.job_uid] = entry
        if state is not None:
            state.register_in_flight(entry.job_uid)
        return entry

    def settle(
        self,
        job_uid: str,
        *,
        state: "InFlightStateIndex | None" = None,
    ) -> InFlightJob | None:
        """语义化结算接缝：注销在途任务并同步内存状态（单一真相源出口）。"""
        entry = self._entries.pop(job_uid, None)
        if state is not None:
            # 收尾机器的提交路径内部已注销 in-flight（重试分支注销后立即
            # 重入队并重建 trigger 豁免）；此处仅在 uid 仍在途索引时才兜底
            # 转发注销，防止二次注销误删刚重建的豁免（安全网语义不变）。
            if job_uid in state.in_flight_uids:
                state.unregister_in_flight(job_uid)
        return entry

    def active_handles(self) -> list[JobHandle]:
        """收集所有活动的真实子进程句柄（排除 handle=None 的伪条目）。"""
        return [
            entry.handle for entry in self._entries.values()
            if entry.handle is not None
        ]

    def active_uids(self) -> list[str]:
        """收集所有在途任务的 uid 列表。"""
        return list(self._entries.keys())

    def classify_aborted(
        self, completed_map: Mapping[str, Any]
    ) -> tuple[list[InFlightJob], list[tuple[InFlightJob, Any]]]:
        """将当前在途任务分类为（已取消列表, 已完成待提交列表）。"""
        cancelled_entries: list[InFlightJob] = []
        done_entries: list[tuple[InFlightJob, Any]] = []
        for entry in self._entries.values():
            if entry.job_uid in completed_map:
                done_entries.append((entry, completed_map[entry.job_uid]))
            else:
                cancelled_entries.append(entry)
        return cancelled_entries, done_entries

    def release_all_resources(
        self,
        resource_mgr: ResourceManager | None = None,
    ) -> None:
        """释放所有在途任务已占用的资源（防泄漏并清空 acquired 列表以防二次释放）。"""
        for entry in self._entries.values():
            entry.release_resources(resource_mgr)

    @staticmethod
    def create_pseudo_entry(
        job_uid: str,
        job_snapshot: dict,
        job: Job,
    ) -> InFlightJob:
        """合成安全伪条目（用于崩溃恢复或 abort 结果提交）。"""
        return InFlightJob(
            job_uid=job_uid,
            job_snapshot=job_snapshot,
            job=job,
            acquired=[],
            handle=None,
            started_monotonic=None,
            lease=NullResourceLease(job_uid=job_uid),
        )


__all__ = [
    "InFlightJob",
    "InFlightTracker",
]
