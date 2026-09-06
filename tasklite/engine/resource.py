import logging
import math
import time
from abc import ABC, abstractmethod
from typing import Optional, Tuple

logger = logging.getLogger("tasklite")

# 单次 suspend 的上限（秒）：防止子进程传入 1e12 等超大值永久停摆管线
_MAX_SUSPEND_SECONDS = 86400.0  # 24h


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
