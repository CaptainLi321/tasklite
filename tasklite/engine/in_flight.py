"""v2 在途作业与生命周期跟踪深模块。

统一内敛：
1. 在途任务条目管理与 PipelineState 状态同步（原子登记/注销/清空，防身份真空）；
2. 活动子进程句柄批量提取；
3. 异常/停机时的统一资源安全释放；
4. 只读 Mapping 视图（禁止绕过 PipelineState 同步的裸写入口）。
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.state import PipelineState
    from .resource import ResourceManager

from ..models.job import Job
from .types import JobHandle


@dataclass
class InFlightJob:
    """已进入 running、尚未提交结果的执行上下文。

    acquired 列表由 release_resources 幂等归还。
    handle 为 None 时没有 worker，可携带前置操作失败时已获取的资源；
    有 worker 时以 started_monotonic 和句柄期限驱动超时判定。
    """

    job_uid: str
    job_snapshot: dict
    job: Job
    acquired: list[tuple[str, float]] = field(default_factory=list)
    handle: JobHandle | None = None
    started_monotonic: float | None = None
    # 完整执行身份随 running 登记，结算时用于追加 Attempt。
    execution_id: str | None = None
    attempt_no: int = 1
    run_id: str = ""
    started_at: str | None = None
    known_job_uids: frozenset[str] = field(default_factory=frozenset)
    active_files: dict[str, Any] = field(default_factory=dict)
    active_file_sources: dict[str, Any] = field(default_factory=dict)

    def release_resources(self, resource_mgr: ResourceManager | None = None) -> None:
        """释放关联的已获取资源列表（幂等归还）。"""
        if self.acquired:
            acquired, self.acquired = self.acquired, []
            if resource_mgr is not None:
                resource_mgr.release_all(acquired, job_uid=self.job_uid)


class InFlightTracker(Mapping[str, InFlightJob]):
    """统一在途任务生命周期跟踪器深模块（对外只读 Mapping，写操作必经同步出口）。"""

    def __init__(self, entries: Mapping[str, InFlightJob] | None = None) -> None:
        self._entries: dict[str, InFlightJob] = dict(entries) if entries is not None else {}

    def __getitem__(self, job_uid: str) -> InFlightJob:
        return self._entries[job_uid]

    def __iter__(self) -> Iterator[str]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, job_uid: object) -> bool:
        return job_uid in self._entries

    def get(self, job_uid: str, default: InFlightJob | None = None) -> InFlightJob | None:  # type: ignore[override]
        return self._entries.get(job_uid, default)

    @property
    def uids(self) -> frozenset[str]:
        """当前在途作业 UID 集合的不可变快照（单一真相源）。"""
        return frozenset(self._entries.keys())

    def clear_all(self, *, state: "PipelineState") -> None:
        """清空全部在途条目并同步注销内存状态中的在途索引。"""
        for uid in list(self._entries.keys()):
            if uid in state.in_flight_uids:
                state.unregister_in_flight(uid)
        self._entries.clear()

    def track(
        self,
        entry: InFlightJob,
        *,
        state: "PipelineState | None" = None,
    ) -> InFlightJob:
        """登记在途作业条目（单一真相源入口）。

        ``state`` 非 None 时同步登记内存 in-flight 索引，维护六集合互斥不变式。
        """
        self._entries[entry.job_uid] = entry
        if state is not None:
            state.register_in_flight(entry.job_uid)
        return entry

    def settle(
        self,
        job_uid: str,
        *,
        state: "PipelineState | None" = None,
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
        """收集所有活动的真实子进程句柄（排除 handle=None 的无 worker 条目）。"""
        return [
            entry.handle for entry in self._entries.values()
            if entry.handle is not None
        ]

    def active_uids(self) -> list[str]:
        """收集所有在途任务的 uid 列表。"""
        return list(self._entries.keys())

    def release_all_resources(
        self,
        resource_mgr: ResourceManager | None = None,
    ) -> None:
        """释放所有在途任务已占用的资源（防泄漏并清空 acquired 列表以防二次释放）。"""
        for entry in self._entries.values():
            entry.release_resources(resource_mgr)


__all__ = [
    "InFlightJob",
    "InFlightTracker",
]
