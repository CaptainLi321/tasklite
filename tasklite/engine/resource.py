"""v2 资源体系：CapacityResource / IntervalResource / ResourceManager 与事件等待。

ResourceManager 是统一资源管理器深模块：资源注册表（Dict-like）、
Task 默认资源单点合并、三态可用性评估（可用、暂不可用、明确不可满足）、
串行连续获取与释放、每 Run 通知器绑定与唤醒。
"""
from __future__ import annotations

import logging
import math
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ..models.job import WORKER_RESOURCE
from ..models.task import TaskRegistry

logger = logging.getLogger("tasklite")

_REAL_SLEEP = time.sleep


@dataclass(frozen=True)
class ResourceQueryResult:
    """资源可用性三态查询结果（不可变值对象）。

    三态定义：
    1. is_available=True: 资源当前可用；
    2. is_available=False, is_impossible=False: 暂不可用（带 wait_seconds 或 None 等待通知）；
    3. is_impossible=True: 明确不可满足（附带 reason，收敛失败不执行）。
    """

    is_available: bool
    wait_seconds: float | None = None
    is_impossible: bool = False
    reason: str | None = None

    def __iter__(self) -> Iterator[Any]:
        """兼容 (is_available, wait_time) 解包语法。"""
        yield self.is_available
        yield self.wait_seconds if self.wait_seconds is not None else float("inf")


class Resource(ABC):
    """统一内部资源基类。"""

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(f"Resource name must be a non-empty string, got {name!r}")
        self.name = name
        self._notifier: Any = None

    def bind_notifier(self, notifier: Any) -> None:
        """绑定当前 Run 的唤醒通知器。"""
        self._notifier = notifier

    def unbind_notifier(self) -> None:
        """解除通知器绑定（不清空自身资源状态）。"""
        self._notifier = None

    def _notify(self) -> None:
        if self._notifier is not None:
            self._notifier.notify()

    def notify(self) -> None:
        """主动通知可用性变化（线程安全，供外部适配器调用）。"""
        self._notify()

    @abstractmethod
    def can_acquire(self, amount: float) -> ResourceQueryResult:
        """返回资源可用性查询结果（三态：可用、暂不可用、明确不可能）。"""

    @abstractmethod
    def acquire(self, amount: float) -> None:
        """占用 amount 个单位的资源。"""

    @abstractmethod
    def release(self, amount: float) -> None:
        """归还 amount 个单位的资源（job 终结时由框架调用）。"""


class IntervalResource(Resource):
    """控制操作频率，每次获准后下次至少等待 interval_seconds * amount。

    本次获取获准后，下次获取至少等待 interval_seconds * amount，任务结束不返还时间配额。
    首次可立即获取；用量影响后续等待。
    """

    def __init__(self, name: str, *, interval_seconds: float) -> None:
        super().__init__(name)
        if not isinstance(interval_seconds, (int, float)) or isinstance(
            interval_seconds, bool
        ):
            raise TypeError(
                f"interval_seconds must be a number, got {type(interval_seconds).__name__}"
            )
        if not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError(
                f"interval_seconds must be finite and > 0, got {interval_seconds!r}"
            )
        self.interval_seconds = float(interval_seconds)
        self.next_available = time.monotonic()

    def can_acquire(self, amount: float) -> ResourceQueryResult:
        if amount < 0:
            raise ValueError(f"amount must be non-negative, got {amount}")
        now = time.monotonic()
        if now >= self.next_available:
            return ResourceQueryResult(is_available=True, wait_seconds=0.0)
        return ResourceQueryResult(
            is_available=False,
            wait_seconds=self.next_available - now,
            is_impossible=False,
        )

    def acquire(self, amount: float) -> None:
        if amount < 0:
            raise ValueError(f"amount must be non-negative, got {amount}")
        now = time.monotonic()
        base_time = max(now, self.next_available)
        self.next_available = base_time + (self.interval_seconds * amount)

    def release(self, amount: float) -> None:
        """时间片消费不可撤销，完成不退时间配额（no-op）。"""

    def __repr__(self) -> str:
        return f"IntervalResource(name={self.name!r}, interval_seconds={self.interval_seconds})"


class CapacityResource(Resource):
    """控制并发量，如最多 4 个并发 worker 或 8000 MB 显存。"""

    def __init__(self, name: str, *, max_capacity: float) -> None:
        super().__init__(name)
        if not isinstance(max_capacity, (int, float)) or isinstance(max_capacity, bool):
            raise TypeError(
                f"max_capacity must be a number, got {type(max_capacity).__name__} ({max_capacity!r})"
            )
        if not math.isfinite(max_capacity) or max_capacity < 0:
            raise ValueError(f"max_capacity must be finite and >= 0, got {max_capacity!r}")
        self.max_capacity = float(max_capacity)
        self.used = 0.0
        self.release_overruns = 0

    def can_acquire(self, amount: float) -> ResourceQueryResult:
        if amount < 0:
            raise ValueError(f"amount must be non-negative, got {amount}")
        if amount > self.max_capacity:
            return ResourceQueryResult(
                is_available=False,
                wait_seconds=None,
                is_impossible=True,
                reason=f"amount {amount} exceeds resource capacity {self.max_capacity}",
            )
        if self.used + amount <= self.max_capacity:
            return ResourceQueryResult(is_available=True, wait_seconds=0.0)
        # 容量不足，等待其他任务释放通知（无周期轮询）
        return ResourceQueryResult(is_available=False, wait_seconds=None, is_impossible=False)

    def acquire(self, amount: float) -> None:
        if amount < 0:
            raise ValueError(f"amount must be non-negative, got {amount}")
        if amount > self.max_capacity:
            raise ValueError(
                f"amount {amount} exceeds resource capacity {self.max_capacity}"
            )
        if self.used + amount > self.max_capacity:
            raise RuntimeError(
                f"Resource '{self.name}' capacity exceeded: {self.used} + {amount} > {self.max_capacity}"
            )
        self.used += amount

    def release(self, amount: float) -> None:
        if amount < 0:
            raise ValueError(f"amount must be non-negative, got {amount}")
        if amount > self.used:
            self.release_overruns += 1
            logger.warning(
                f"Resource '{self.name}' underflow: release({amount}) > used({self.used}). "
                f"Possible double-release. Clamping to 0."
            )
        self.used = max(0.0, self.used - amount)
        self._notify()

    def __repr__(self) -> str:
        return (
            f"CapacityResource(name={self.name!r}, used={self.used}, "
            f"max_capacity={self.max_capacity})"
        )


@dataclass(frozen=True)
class ResourceEvaluation:
    """资源可用性与死锁归因评估结果（不可变值对象）。"""

    is_available: bool
    wait_time: float
    is_unknown: bool = False
    is_impossible: bool = False
    unknown_name: str | None = None
    impossible_name: str | None = None
    reason: str | None = None


class LeaseStatus(str, Enum):
    """资源租约状态（兼容保留）。"""

    RESERVED = "reserved"
    CLAIMED = "claimed"
    RELEASED = "released"


@dataclass
class ResourceLease:
    """资源租约（兼容保留）。"""

    manager: ResourceManager | None = None
    job_uid: str = ""
    acquired: list[tuple[str, float]] = field(default_factory=list)
    rate_limits: list[tuple[str, float]] = field(default_factory=list)
    status: LeaseStatus = LeaseStatus.CLAIMED

    def claim(self) -> None:
        self.status = LeaseStatus.CLAIMED

    def release(self) -> None:
        if self.status == LeaseStatus.RELEASED:
            return
        self.status = LeaseStatus.RELEASED
        if self.manager is not None and self.acquired:
            self.manager.release_all(self.acquired, job_uid=self.job_uid)

    def cancel(self) -> None:
        self.release()

    def __enter__(self) -> ResourceLease:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None and self.status != LeaseStatus.RELEASED:
            self.release()


@dataclass
class NullResourceLease(ResourceLease):
    """空资源租约。"""

    manager: Any = None
    job_uid: str = ""
    acquired: list[tuple[str, float]] = field(default_factory=list)
    rate_limits: list[tuple[str, float]] = field(default_factory=list)
    status: LeaseStatus = LeaseStatus.RELEASED

    def claim(self) -> None:
        pass

    def release(self) -> None:
        pass

    def cancel(self) -> None:
        pass


class RunNotifier:
    """内部运行期唤醒通知器。

    保证 check-to-wait 间通知不丢失：版本序号（generation counter）与 threading.Event。
    """

    def __init__(self) -> None:
        self._event = threading.Event()
        self._seq = 0
        self._lock = threading.Lock()

    def notify(self) -> None:
        """从任意线程安全调用，唤醒主控制线程。"""
        with self._lock:
            self._seq += 1
            self._event.set()

    def snapshot(self) -> int:
        """在开始检查调度/资源条件前记录当前通知序号。"""
        with self._lock:
            return self._seq

    def wait(self, last_seq: int, timeout: float | None = None) -> bool:
        """如果快照后已有新通知，立即返回 True；否则进入等待，直到到期或被新通知唤醒。"""
        with self._lock:
            if self._seq != last_seq:
                return True
            self._event.clear()
        if time.sleep is not _REAL_SLEEP and timeout is not None and timeout > 0:
            time.sleep(timeout)
        return self._event.wait(timeout=timeout)

    def clear(self) -> None:
        with self._lock:
            self._event.clear()


class ResourceManager(MutableMapping[str, Resource]):
    """统一资源管理器深模块。

    统一负责：
    1. 资源注册表与生命周期管理（Dict-like 访问）；
    2. Task 默认资源与 Job 声明资源的单点合并；
    3. 三态可用性与合法性评估（可用、暂不可用、明确不可满足）；
    4. 串行连续获取与异常安全释放；
    5. 每 Run 通知机制绑定与唤醒。
    """

    def __init__(
        self,
        resources: Mapping[str, Resource] | None = None,
        tasks: TaskRegistry | Mapping[str, Any] | None = None,
    ) -> None:
        self._resources: dict[str, Resource] = (
            dict(resources) if resources is not None else {}
        )
        self.tasks: TaskRegistry | Mapping[str, Any] = (
            tasks if tasks is not None else {}
        )
        self._notifier: RunNotifier | None = None

    def bind_notifier(self, notifier: RunNotifier) -> None:
        """按 Run 绑定通知器，旧通知不作用于新 Run。"""
        self._notifier = notifier
        for res in self._resources.values():
            res.bind_notifier(notifier)

    def unbind_notifier(self) -> None:
        """解除通知器绑定（不清空资源内部状态）。"""
        self._notifier = None
        for res in self._resources.values():
            res.unbind_notifier()

    def notify(self, name: str | None = None) -> None:
        """主动触发主进程唤醒。"""
        if self._notifier is not None:
            self._notifier.notify()

    def __getitem__(self, key: str) -> Resource:
        return self._resources[key]

    def __setitem__(self, key: str, value: Resource) -> None:
        if self._notifier is not None:
            value.bind_notifier(self._notifier)
        self._resources[key] = value

    def __delitem__(self, key: str) -> None:
        del self._resources[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._resources)

    def __len__(self) -> int:
        return len(self._resources)

    def __contains__(self, key: object) -> bool:
        return key in self._resources

    def effective_resources(
        self,
        task_type: str,
        declared_resources: Mapping[str, float]
        | Iterable[tuple[str, float]]
        | None = None,
    ) -> dict[str, float]:
        """合并 Task 默认资源与 Job 声明资源（声明值覆盖默认值）。"""
        if declared_resources is None or isinstance(declared_resources, Mapping):
            base: dict[str, float] = dict(declared_resources or {})
        else:
            base = dict(declared_resources)
        task = self.tasks.get(task_type)
        defaults = (
            getattr(task, "resources", None) if task is not None else None
        )
        if not defaults:
            return base
        return {**defaults, **base}

    def _resolve_runtime_resources(
        self,
        task_type: str,
        declared_resources: Mapping[str, float]
        | Iterable[tuple[str, float]]
        | None = None,
    ) -> dict[str, float]:
        if declared_resources is None or isinstance(declared_resources, Mapping):
            base: dict[str, float] = dict(declared_resources or {})
        else:
            base = dict(declared_resources)
        if WORKER_RESOURCE in base:
            return base
        return self.effective_resources(task_type, base)

    def evaluate(
        self,
        task_type: str,
        declared_resources: Mapping[str, float]
        | Iterable[tuple[str, float]]
        | None = None,
    ) -> ResourceEvaluation:
        """评估作业所需资源的可用性与合法性（三态：可用、暂不可用、明确不可满足）。"""
        eff = self._resolve_runtime_resources(task_type, declared_resources)
        min_wait = 0.0
        has_waiting = False
        for res_name, amount in eff.items():
            res = self._resources.get(res_name)
            if res is None:
                return ResourceEvaluation(
                    is_available=False,
                    wait_time=float("inf"),
                    is_unknown=True,
                    is_impossible=True,
                    unknown_name=res_name,
                    impossible_name=res_name,
                    reason=f"Resource '{res_name}' is not registered",
                )
            query = res.can_acquire(amount)
            if isinstance(query, tuple):
                query = ResourceQueryResult(
                    is_available=bool(query[0]),
                    wait_seconds=query[1] if len(query) > 1 else None,
                )
            if query.is_impossible:
                return ResourceEvaluation(
                    is_available=False,
                    wait_time=float("inf"),
                    is_impossible=True,
                    impossible_name=res_name,
                    reason=query.reason or f"Resource '{res_name}' impossible",
                )
            if not query.is_available:
                has_waiting = True
                if query.wait_seconds is not None:
                    min_wait = max(min_wait, query.wait_seconds)

        if has_waiting:
            return ResourceEvaluation(
                is_available=False,
                wait_time=min_wait if min_wait > 0.0 else float("inf"),
            )
        return ResourceEvaluation(
            is_available=True,
            wait_time=0.0,
        )

    def acquire_all(
        self,
        task_type: str,
        declared_resources: Mapping[str, float]
        | Iterable[tuple[str, float]]
        | None = None,
        *,
        job_uid: str | None = None,
    ) -> list[tuple[str, float]]:
        """主控制线程连续逐项获取资源（异常属 Run 级框架故障直接上抛）。"""
        eff = self._resolve_runtime_resources(task_type, declared_resources)
        acquired: list[tuple[str, float]] = []
        for res_name, amount in eff.items():
            res = self._resources.get(res_name)
            if res is None:
                raise KeyError(f"Resource '{res_name}' not registered")
            res.acquire(amount)
            acquired.append((res_name, amount))
        return acquired

    def release_all(
        self,
        acquired: Iterable[tuple[str, float]],
        *,
        job_uid: str | None = None,
    ) -> None:
        """主控制线程连续释放已获取资源（异常属 Run 级框架故障直接上抛）。"""
        for item in acquired:
            try:
                res_name, amount = item
            except (TypeError, ValueError):
                continue
            res = self._resources.get(res_name)
            if res is None:
                continue
            res.release(amount)

    def reserve(
        self,
        task_type: str,
        declared_resources: Mapping[str, float]
        | Iterable[tuple[str, float]]
        | None = None,
        *,
        job_uid: str | None = None,
    ) -> ResourceLease:
        """租约封装（直接连续获取，不再使用两阶段预扣）。"""
        acquired = self.acquire_all(task_type, declared_resources, job_uid=job_uid)
        return ResourceLease(manager=self, job_uid=job_uid or "", acquired=acquired)

    def can_acquire_worker(self, amount: float = 1.0) -> ResourceQueryResult:
        """检查工作者槽位可用性。"""
        worker_res = self._resources.get(WORKER_RESOURCE)
        if worker_res is None:
            return ResourceQueryResult(is_available=True, wait_seconds=0.0)
        return worker_res.can_acquire(amount)


__all__ = [
    "Resource",
    "IntervalResource",
    "CapacityResource",
    "ResourceQueryResult",
    "ResourceEvaluation",
    "ResourceManager",
    "RunNotifier",
    "ResourceLease",
    "NullResourceLease",
    "LeaseStatus",
]
