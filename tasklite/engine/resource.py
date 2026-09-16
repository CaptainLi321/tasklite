from __future__ import annotations

import logging
import math
import time
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from dataclasses import dataclass, field
from typing import (
    Any, Dict, Iterable, Iterator, List, Mapping, Optional,
    Tuple, Union
)

logger = logging.getLogger("tasklite")

from ..models.job import WORKER_RESOURCE  # noqa: E402
from ..utils.jsonutil import dumps  # noqa: E402

# 单次 suspend 的上限（秒）：防止子进程传入 1e12 等超大值永久停摆管线
_MAX_SUSPEND_SECONDS = 86400.0  # 24h

# 资源挂起截止时刻在 meta 表的持久化键
META_RESOURCE_SUSPENDS = "resource_suspends"


class RateLimitUnavailable(RuntimeError):
    """reserve 预检发现 RateLimitResource 处于限流等待窗（瞬态信号）。

    语义边界：只表达「此刻不可预约、稍后自动恢复」——等待由资源挂起
    TTL / 令牌窗承担，调用方必须按瞬态信号处理（短退避回队、不烧
    重试预算），不得与派发故障混流计崩溃计数。
    """


def persist_resource_suspensions(backend: Any, resource_mgr: "ResourceManager") -> None:
    """把资源挂起截止时刻原子落盘到 meta 表（completion/recovery 共享助手）。

    挂起的应用点即时持久化，消除 kill -9/OOM 时挂起丢失窗口。
    """
    deadlines = resource_mgr.collect_suspensions()
    try:
        backend.set_meta(META_RESOURCE_SUSPENDS, dumps(deadlines))
    except Exception as e:
        logger.error(f"Failed to persist resource suspends to meta: {e}")


class Resource(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def can_acquire(self, amount: float) -> Tuple[bool, float]:
        """Returns (is_available, seconds_to_wait_if_not)"""
        pass

    @abstractmethod
    def acquire(self, amount: float) -> None:
        """占用 amount 个单位的资源。"""
        pass

    @abstractmethod
    def release(self, amount: float) -> None:
        """归还 amount 个单位的资源（job 终结时由框架调用）。

        契约说明：无用量计数的资源类型（如 RateLimitResource——时间片
        消费不可撤销）**允许实现为 no-op**；调用方不得假设 release 一定
        释放可复用容量。
        """
        pass

    @abstractmethod
    def suspend(self, seconds: float) -> None:
        """挂起资源 seconds 秒。

        语义是「设置最早可用截止」：重复挂起取 max（幂等去重），**不是
        累加**——suspend(30) 后再 suspend(60)，总挂起到 now+60 而非
        now+90。与 acquire 的推进语义（多次调用线性累加）不同。
        """
        pass

    def suspended_until(self) -> Optional[float]:
        """挂起截止的协议访问器——monotonic 时钟
        时刻，无挂起/无限速等待返回 None。

        需要持久化挂起/等待状态的子类应覆写（默认实现返回 None，不持久
        化）——协议方法优于 getattr duck-typing：后者探测各实现的私有属
        性名，新增实现会静默漏持久化 429 状态。
        """
        return None

    def _sanitize_suspend(self, seconds: float) -> float:
        """校验并钳制 suspend 秒数：非有限/负值忽略，超上限钳制。

        子进程（handler）可经 ``ctx.suspend_resource`` 任意传值——无校验时
        1e12 秒的 suspend 会让整个管线永久停摆（min_wait 有限→主循环无限
        sleep，不触发死锁处理）。返回钳制后的有效秒数。

        大整数溢出防御：超大 int（如 ``10**400``）通过 isinstance 检查后，
        ``math.isfinite`` 转 float 抛 OverflowError 击穿本防御直达 suspend
        调用点（worker 内未捕获 → 进程崩溃）。溢出 int 必然超出上限：正值
        钳制、负值按无效忽略。
        """
        if not isinstance(seconds, (int, float)) or isinstance(seconds, bool):
            logger.warning(f"Resource '{self.name}' suspend: non-numeric seconds {seconds!r}, ignoring")
            return 0.0
        try:
            finite = math.isfinite(seconds)
        except OverflowError:
            if seconds > 0:
                logger.warning(
                    f"Resource '{self.name}' suspend: {seconds} overflows float, "
                    f"clamping to max {_MAX_SUSPEND_SECONDS}s"
                )
                return float(_MAX_SUSPEND_SECONDS)
            logger.warning(f"Resource '{self.name}' suspend: invalid seconds {seconds!r}, ignoring")
            return 0.0
        if not finite or seconds <= 0:
            logger.warning(f"Resource '{self.name}' suspend: invalid seconds {seconds!r}, ignoring")
            return 0.0
        if seconds > _MAX_SUSPEND_SECONDS:
            logger.warning(
                f"Resource '{self.name}' suspend: {seconds}s exceeds max {_MAX_SUSPEND_SECONDS}s, clamping"
            )
            return _MAX_SUSPEND_SECONDS
        return seconds

class RateLimitResource(Resource):
    """Controls frequency of operations, e.g., 1 request per 5 seconds.

    直觉警告：`acquire(amount)` 的 ``amount`` 是「消费多少个时间片」，
    **不是请求次数**——一般限流器的直觉是传请求数、间隔内部管理，本类
    相反（amount 直接缩放间隔）。绝大多数调用方应保持默认 1.0。
    For example, with interval_seconds=2.0:
    - acquire(amount=1.0) advances next_available by 2 seconds (default)
    - acquire(amount=0.5) advances next_available by 1 second (half interval)
    - acquire(amount=2.0) advances next_available by 4 seconds (two intervals)

    Note: `can_acquire()` only checks if the resource is currently available (now >= next_available).
    It does NOT validate whether the requested amount would block the resource for too long.
    Callers should use reasonable amounts (typically 1.0) to avoid accidentally blocking.
    """
    def __init__(self, name: str, interval_seconds: float):
        super().__init__(name)
        # interval 必须为有限正数——负值会把 next_available 推向过去使
        # can_acquire 恒 True，静默禁用限速（CapacityResource 的容量校验
        # 是 fail-loud，两者不对称，此处需显式校验）。
        if not isinstance(interval_seconds, (int, float)) or isinstance(interval_seconds, bool):
            raise TypeError(
                f"interval_seconds must be a number, got {type(interval_seconds).__name__}"
            )
        if not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError(
                f"interval_seconds must be finite and > 0, got {interval_seconds!r}"
            )
        self.interval = interval_seconds
        self.next_available = time.monotonic()

    def can_acquire(self, amount: float) -> Tuple[bool, float]:
        now = time.monotonic()
        if now >= self.next_available:
            return True, 0.0
        return False, self.next_available - now

    def acquire(self, amount: float) -> None:
        if amount < 0:
            raise ValueError(f"amount must be non-negative, got {amount}")
        now = time.monotonic()
        base_time = max(now, self.next_available)
        self.next_available = base_time + (self.interval * amount)

    def release(self, amount: float) -> None:
        pass

    def suspend(self, seconds: float) -> None:
        now = time.monotonic()
        seconds = self._sanitize_suspend(seconds)
        self.next_available = max(self.next_available, now + seconds)

    def suspended_until(self) -> Optional[float]:
        # 限速的挂起语义 = 下次可用时刻（next_available 为 monotonic）。
        # 仅当存在真实等待/挂起（未来时刻）时返回；否则返回 None，与
        # CapacityResource 的“无挂起返回 None”语义对齐。
        if self.next_available > time.monotonic():
            return self.next_available
        return None

    def __repr__(self) -> str:
        return f"RateLimitResource(name={self.name!r}, interval={self.interval})"


class CapacityResource(Resource):
    """Controls concurrent volume, e.g., max 8000 MB VRAM."""
    _CAPACITY_POLL_INTERVAL: float = 0.5

    def __init__(self, name: str, max_capacity: float):
        super().__init__(name)
        # 构造期校验 max_capacity——与 RateLimitResource 的 interval 校验
        # 对称。
        # NaN 让 can_acquire 的 ``amount > NaN`` 恒 False、``used + amount
        # <= NaN`` 恒 False → 永远返回 (False, 0.5) → 主循环无限 sleep
        # （livelock，死锁检测只认 inf 不认 NaN）；负值让 acquire 永远
        # 抛 ValueError（amount > capacity 恒 True）→ 全灭。入口即拒。
        if not isinstance(max_capacity, (int, float)) or isinstance(max_capacity, bool):
            raise TypeError(
                f"max_capacity must be a number, got {type(max_capacity).__name__} ({max_capacity!r})"
            )
        if not math.isfinite(max_capacity) or max_capacity < 0:
            raise ValueError(
                f"max_capacity must be finite and >= 0, got {max_capacity!r}"
            )
        self.capacity = max_capacity
        self.used = 0.0
        self.suspend_until = 0.0
        # 可观测性增强：release 超量（双重释放/记错账）计数——保持 clamp 行为不变，
        # 用计数器提升可观测性（双重释放是资源泄漏的早期信号）。
        self.release_overruns = 0

    def can_acquire(self, amount: float) -> Tuple[bool, float]:
        if amount > self.capacity:
            # Deadlock safeguard: impossible request
            return False, float('inf')

        now = time.monotonic()
        if now < self.suspend_until:
            return False, self.suspend_until - now

        if self.used + amount <= self.capacity:
            return True, 0.0

        # Wait a small poll interval until other tasks release it
        return False, self._CAPACITY_POLL_INTERVAL

    def acquire(self, amount: float) -> None:
        """Acquire ``amount`` units of capacity.

        ``amount=0`` is valid: it does not occupy capacity, only validates
        that ``capacity >= 0``. Used by jobs that declare a resource but
        don't consume slots.
        """
        if amount < 0:
            raise ValueError(f"amount must be non-negative, got {amount}")
        if amount > self.capacity:
            raise ValueError(
                f"amount {amount} exceeds resource capacity {self.capacity}"
            )
        self.used += amount

    def release(self, amount: float) -> None:
        if amount < 0:
            raise ValueError(f"amount must be non-negative, got {amount}")
        if amount > self.used:
            # release 超量（双重释放/记错账）时保持 clamp 行为（release
            # 常在 finally，抛异常会遮蔽原异常），以计数 + 告警提升可观测性。
            # 该计数仅用于本条日志内联展示（管线侧不汇总读取，仅作排障
            # 线索）。
            self.release_overruns += 1
            logger.warning(
                f"Resource '{self.name}' underflow: release({amount}) > used({self.used}). "
                f"Possible double-release. Clamping to 0. (overruns={self.release_overruns})"
            )
        self.used = max(0.0, self.used - amount)

    def suspend(self, seconds: float) -> None:
        now = time.monotonic()
        seconds = self._sanitize_suspend(seconds)
        self.suspend_until = max(self.suspend_until, now + seconds)

    def suspended_until(self) -> Optional[float]:
        # 仅在存在有效挂起（未来截止时刻）时返回；过期或无挂起返回 None
        return self.suspend_until if self.suspend_until > time.monotonic() else None

    def __repr__(self) -> str:
        return f"CapacityResource(name={self.name!r}, used={self.used}, capacity={self.capacity})"


from enum import Enum


class LeaseStatus(str, Enum):
    """资源租约状态。"""
    RESERVED = "reserved"
    CLAIMED = "claimed"
    RELEASED = "released"


@dataclass(frozen=True)
class ResourceEvaluation:
    """资源可用性与死锁归因评估结果（不可变值对象）。"""

    is_available: bool
    wait_time: float
    is_unknown: bool = False
    is_impossible: bool = False
    unknown_name: Optional[str] = None
    impossible_name: Optional[str] = None


@dataclass
class ResourceLease:
    """两阶段资源租约。

    封装原子预扣（Capacity 占用 / RateLimit 校验）与正式兑现（RateLimit 推进）。
    支持上下文管理器：派发阶段遇异常自动回滚释放。
    """

    manager: "ResourceManager"
    job_uid: str
    acquired: List[Tuple[str, float]]
    rate_limits: List[Tuple[str, float]]
    status: LeaseStatus = LeaseStatus.RESERVED

    def claim(self) -> None:
        """正式兑现租约（子进程真正派发时调用）。

        推进速率限制资源的时间片。幂等保护：仅在 RESERVED 状态下生效。
        """
        if self.status != LeaseStatus.RESERVED:
            return
        for res_name, amount in self.rate_limits:
            res = self.manager.get(res_name)
            if res is not None:
                res.acquire(amount)
        self.status = LeaseStatus.CLAIMED

    def release(self) -> None:
        """归还已占用的资源（任务完成或异常时调用）。

        幂等保护：仅在 RESERVED 或 CLAIMED 状态下执行一次释放。
        """
        if self.status == LeaseStatus.RELEASED:
            return
        self.manager.release_all(self.acquired, uid=self.job_uid)
        self.status = LeaseStatus.RELEASED

    def cancel(self) -> None:
        """取消租约并释放已占资源（等价于 release）。"""
        self.release()

    def __enter__(self) -> "ResourceLease":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None and self.status != LeaseStatus.RELEASED:
            self.release()


@dataclass
class NullResourceLease(ResourceLease):
    """空资源租约（用于伪条目或无资源占用的作业，所有生命周期操作均为 no-op）。"""

    manager: Any = None
    job_uid: str = ""
    acquired: List[Tuple[str, float]] = field(default_factory=list)
    rate_limits: List[Tuple[str, float]] = field(default_factory=list)
    status: LeaseStatus = LeaseStatus.RELEASED

    def claim(self) -> None:
        pass

    def release(self) -> None:
        pass

    def cancel(self) -> None:
        pass


class ResourceManager(MutableMapping[str, Resource]):
    """统一资源管理器深模块。

    统一内敛：
    1. 资源注册表与生命周期管理（支持 Dict-like 访问，向下完全兼容）；
    2. Handler 默认资源的动态单点合并（消除调度与派发双重维护）；
    3. 细粒度资源合法性与可用性评估（unknown、impossible、wait_time）；
    4. 事务性原子获取（acquire_effective）与安全幂等释放（release_all）；
    5. 统一挂起与跨崩溃状态持久化/恢复。
    """

    def __init__(
        self,
        resources: Optional[Mapping[str, Resource]] = None,
        handlers: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self._resources: Dict[str, Resource] = dict(resources) if resources is not None else {}
        self.handlers: Mapping[str, Any] = handlers if handlers is not None else {}

    def __getitem__(self, key: str) -> Resource:
        return self._resources[key]

    def __setitem__(self, key: str, value: Resource) -> None:
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
        declared_resources: Optional[Union[Mapping[str, float], Iterable[Tuple[str, float]]]] = None,
    ) -> Dict[str, float]:
        """合并 Handler 默认资源与 Job 声明资源。"""
        if declared_resources is None:
            base: Dict[str, float] = {}
        elif isinstance(declared_resources, Mapping):
            base = dict(declared_resources)
        else:
            base = dict(declared_resources)
        entry = self.handlers.get(task_type)
        defaults = getattr(entry, "default_resources", None) if entry is not None else None
        if not defaults:
            return base
        return {**defaults, **base}

    def evaluate(
        self,
        task_type: str,
        declared_resources: Optional[Union[Mapping[str, float], Iterable[Tuple[str, float]]]] = None,
    ) -> ResourceEvaluation:
        """评估作业所需资源的可用性与合法性。"""
        eff = self.effective_resources(task_type, declared_resources)
        min_wait = 0.0
        for res_name, amount in eff.items():
            res = self._resources.get(res_name)
            if res is None:
                return ResourceEvaluation(
                    is_available=False,
                    wait_time=float("inf"),
                    is_unknown=True,
                    unknown_name=res_name,
                )
            ok, wait_time = res.can_acquire(amount)
            if not ok:
                if wait_time == float("inf"):
                    return ResourceEvaluation(
                        is_available=False,
                        wait_time=float("inf"),
                        is_impossible=True,
                        impossible_name=res_name,
                    )
                min_wait = max(min_wait, wait_time)

        if min_wait > 0.0:
            return ResourceEvaluation(
                is_available=False,
                wait_time=min_wait,
            )
        return ResourceEvaluation(
            is_available=True,
            wait_time=0.0,
        )

    def reserve(
        self,
        task_type: str,
        declared_resources: Optional[Union[Mapping[str, float], Iterable[Tuple[str, float]]]] = None,
        *,
        uid: Optional[str] = None,
    ) -> ResourceLease:
        """两阶段原子预约：立即占用 CapacityResource，预检 RateLimitResource。"""
        eff = self.effective_resources(task_type, declared_resources)
        acquired: List[Tuple[str, float]] = []
        rate_limits: List[Tuple[str, float]] = []
        try:
            for res_name, amount in eff.items():
                res = self._resources.get(res_name)
                if res is None:
                    raise KeyError(f"Resource '{res_name}' not registered")
                if isinstance(res, RateLimitResource):
                    ok, wait_time = res.can_acquire(amount)
                    if not ok:
                        raise RateLimitUnavailable(
                            f"RateLimitResource '{res_name}' not available (wait {wait_time:.2f}s)"
                        )
                    rate_limits.append((res_name, amount))
                else:
                    res.acquire(amount)
                    acquired.append((res_name, amount))
            return ResourceLease(
                manager=self,
                job_uid=uid or "",
                acquired=acquired,
                rate_limits=rate_limits,
                status=LeaseStatus.RESERVED,
            )
        except BaseException:
            self.release_all(acquired, uid=uid)
            raise

    def try_reserve(
        self,
        task_type: str,
        declared_resources: Optional[Union[Mapping[str, float], Iterable[Tuple[str, float]]]] = None,
        *,
        uid: Optional[str] = None,
    ) -> Optional[ResourceLease]:
        """试探性预约资源。不可用时返回 None（不抛异常、不产生副作用）。"""
        eval_res = self.evaluate(task_type, declared_resources)
        if not eval_res.is_available:
            return None
        try:
            return self.reserve(task_type, declared_resources, uid=uid)
        except Exception:
            return None

    def acquire_effective(
        self,
        task_type: str,
        declared_resources: Optional[Union[Mapping[str, float], Iterable[Tuple[str, float]]]] = None,
    ) -> List[Tuple[str, float]]:
        """事务性 acquire 所有合并后的资源（异常时自动释放已获取的部分）。"""
        eff = self.effective_resources(task_type, declared_resources)
        acquired: List[Tuple[str, float]] = []
        try:
            for res_name, amount in eff.items():
                res = self._resources.get(res_name)
                if res is None:
                    raise KeyError(f"Resource '{res_name}' not registered")
                res.acquire(amount)
                acquired.append((res_name, amount))
            return acquired
        except BaseException:
            self.release_all(acquired)
            raise

    def release_all(
        self,
        acquired: Iterable[Tuple[str, float]],
        *,
        uid: Optional[str] = None,
    ) -> None:
        """释放已获取的资源集合（异常安全）。"""
        for item in acquired:
            try:
                res_name, amount = item
            except (TypeError, ValueError):
                continue
            res = self._resources.get(res_name)
            if res is None:
                continue
            try:
                res.release(amount)
            except Exception as e:
                logger.warning(
                    f"Error releasing resource '{res_name}' for {uid or 'unknown'}: {e}"
                )

    def suspend_resource(self, name: str, seconds: float) -> bool:
        """挂起指定资源。"""
        res = self._resources.get(name)
        if res is None:
            logger.warning(f"Cannot suspend unknown resource '{name}'")
            return False
        res.suspend(seconds)
        return True

    def can_acquire_worker(self, amount: float = 1.0) -> Tuple[bool, float]:
        """检查工作者槽位可用性。"""
        worker_res = self._resources.get(WORKER_RESOURCE)
        if worker_res is None:
            return True, 0.0
        return worker_res.can_acquire(amount)

    def collect_suspensions(
        self,
        now_mono: Optional[float] = None,
        now_wall: Optional[float] = None,
    ) -> Dict[str, float]:
        """收集当前所有资源的挂起截止（挂钟时间戳，用于跨崩溃持久化）。"""
        if now_mono is None:
            now_mono = time.monotonic()
        if now_wall is None:
            now_wall = time.time()
        suspensions: Dict[str, float] = {}
        for res_name, res in self._resources.items():
            deadline = res.suspended_until()
            if deadline is not None and deadline > now_mono:
                remaining = deadline - now_mono
                suspensions[res_name] = now_wall + remaining
        return suspensions

    def restore_suspensions(
        self,
        suspensions: Mapping[str, float],
        now_wall: Optional[float] = None,
    ) -> None:
        """从挂钟时间戳恢复资源挂起状态。"""
        if now_wall is None:
            now_wall = time.time()
        for name, wall_deadline in suspensions.items():
            if not isinstance(wall_deadline, (int, float)) or isinstance(wall_deadline, bool):
                logger.warning(f"Suspension restore: invalid non-numeric deadline for {name!r}, ignoring")
                continue
            if not math.isfinite(wall_deadline):
                logger.warning(f"Suspension restore: non-finite deadline for {name!r}, ignoring")
                continue
            remaining = wall_deadline - now_wall
            if remaining <= 0:
                continue
            if name not in self._resources:
                logger.warning(
                    f"Suspension restore: resource '{name}' not found on pipeline, ignoring"
                )
                continue
            self._resources[name].suspend(remaining)


__all__ = [
    "Resource",
    "RateLimitResource",
    "CapacityResource",
    "ResourceEvaluation",
    "ResourceManager",
    "LeaseStatus",
    "ResourceLease",
    "NullResourceLease",
    "WORKER_RESOURCE",
]

