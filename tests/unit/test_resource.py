"""Tests for RateLimitResource and CapacityResource using a controllable FakeClock."""

import pytest

from tasklite.engine.resource import RateLimitResource, CapacityResource

# ── monkeypatch target: resource.py does `import time` at module level ──
_RESOURCE_TIME = "tasklite.engine.resource.time.monotonic"


class FakeClock:
    """A controllable clock for mocking time.monotonic()."""

    def __init__(self, start: float = 0.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# ────────────────────────────────────────────────────────────────
# RateLimitResource tests
# ────────────────────────────────────────────────────────────────

def test_rate_acquire_advances_time(monkeypatch):
    """acquire(1.0) pushes next_available by 2.0s; advance past it → ready."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(1.0)

    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 2.0) < 0.01

    clock.advance(2.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_rate_can_acquire_immediate(monkeypatch):
    """First can_acquire without acquire → (True, 0.0)."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_rate_suspend_extends(monkeypatch):
    """acquire then suspend(10.0): still blocked after 2s, freed after full 10s."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(1.0)
    r.suspend(10.0)

    # After 2s — normally would be ready, but suspension blocks
    clock.advance(2.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 8.0) < 0.01  # 8s remaining

    # After full 10s — released
    clock.advance(8.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_rate_suspended_until_none_when_idle(monkeypatch):
    """无挂起/无限速等待时 suspended_until 返回 None（与 CapacityResource 对齐）。"""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    assert r.suspended_until() is None

    r.acquire(1.0)
    # 有真实限速等待时应返回未来时刻
    assert r.suspended_until() is not None

    clock.advance(2.0)
    assert r.suspended_until() is None


def test_capacity_suspended_until_none_when_idle(monkeypatch):
    """CapacityResource 无挂起或挂起过期时 suspended_until 返回 None。"""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=10.0)
    assert r.suspended_until() is None

    r.suspend(5.0)
    assert r.suspended_until() is not None

    clock.advance(5.0)
    assert r.suspended_until() is None


def test_rate_amount_multiplies_interval(monkeypatch):
    """acquire(3.0) pushes next_available by 6.0s (interval * amount)."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(3.0)

    clock.advance(5.9)
    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 0.1) < 0.02

    clock.advance(0.1)
    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_rate_zero_interval_rejected():
    """interval=0.0 → 构造时拒绝（0 间隔 = 无限速，无意义配置）。"""
    import pytest
    with pytest.raises(ValueError, match="interval_seconds"):
        RateLimitResource("test", interval_seconds=0.0)


def test_rate_multiple_acquires_accumulate(monkeypatch):
    """Two acquires of 1.0 each → next_available pushed by 4.0s total."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(1.0)
    r.acquire(1.0)

    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 4.0) < 0.02

    clock.advance(4.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_rate_release_noop(monkeypatch):
    """release() is a no-op for RateLimitResource (no error)."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(1.0)
    r.release(1.0)  # Should not raise or change state

    ok, wait = r.can_acquire(1.0)
    # Still blocked because release does nothing
    assert ok is False
    assert abs(wait - 2.0) < 0.01


def test_rate_limit_exact_wait_boundary(monkeypatch):
    """Advance exactly interval after acquire → (True, 0.0)."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(1.0)

    clock.advance(2.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


# ────────────────────────────────────────────────────────────────
# CapacityResource tests
# ────────────────────────────────────────────────────────────────

def test_capacity_basic_enforcement(monkeypatch):
    """acquire(3) → can_acquire(3) blocked (0.5s wait); release(3) → full capacity."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.acquire(3.0)

    ok, wait = r.can_acquire(3.0)
    assert ok is False
    assert wait == 0.5

    r.release(3.0)
    ok, wait = r.can_acquire(5.0)
    assert ok is True
    assert wait == 0.0


def test_capacity_impossible_request(monkeypatch):
    """Requesting more than max_capacity → (False, inf)."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    ok, wait = r.can_acquire(6.0)
    assert ok is False
    assert wait == float("inf")


def test_capacity_suspend_blocks(monkeypatch):
    """suspend(10.0) → blocked for 10s regardless of capacity."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.suspend(10.0)

    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 10.0) < 0.01

    clock.advance(9.9)
    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 0.1) < 0.02

    clock.advance(0.1)
    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_capacity_release_floors_zero(monkeypatch):
    """release(10.0) when used=0 → used stays 0 (not negative)."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.release(10.0)
    assert r.used == 0.0


def test_capacity_rapid_cycle(monkeypatch):
    """acquire(5) → release(5) → acquire(5) → no state leak."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.acquire(5.0)
    r.release(5.0)
    r.acquire(5.0)

    ok, wait = r.can_acquire(1.0)
    assert ok is False  # used=5, capacity=5
    assert wait == 0.5


def test_capacity_release_does_not_overflow(monkeypatch):
    """acquire(3) + release(5) → used=0, can_acquire(5) → True."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.acquire(3.0)
    r.release(5.0)
    assert r.used == 0.0
    # 超量释放计数器（观测性设计）必须有断言——
    # 否则变异体（删掉 +=1）存活。
    assert r.release_overruns == 1, f"overrun counter should record 1, got {r.release_overruns}"

    ok, wait = r.can_acquire(5.0)
    assert ok is True
    assert wait == 0.0


def test_rate_amount_zero_never_blocks(monkeypatch):
    """acquire(0.0) pushes next_available by 0.0 — immediate access after."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(0.0)  # amount=0 → next_available = max(now, next_available) + 0

    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_capacity_amount_zero_always_available(monkeypatch):
    """amount=0.0 → no resource cost, always available even at full capacity."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.acquire(5.0)  # Fill capacity

    ok, wait = r.can_acquire(0.0)
    assert ok is True
    assert wait == 0.0


def test_rate_multiple_zero_acquires(monkeypatch):
    """Multiple acquire(0.0) calls do not advance next_available."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(0.0)
    r.acquire(0.0)
    r.acquire(0.0)

    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_rate_suspend_resource_method(monkeypatch):
    """suspend_resource() on RateLimitResource extends next_available."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(1.0)

    clock.advance(1.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is False

    r.suspend(10.0)

    clock.advance(1.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 9.0) < 0.01


def test_capacity_suspend_resource_method(monkeypatch):
    """suspend_resource() on CapacityResource blocks until suspension expires."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.suspend(5.0)

    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 5.0) < 0.01

    clock.advance(5.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_capacity_suspend_takes_longer_than_capacity_wait(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.acquire(5.0)
    r.suspend(10.0)

    clock.advance(1.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 9.0) < 0.01

    r.release(5.0)
    clock.advance(8.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 1.0) < 0.02

    clock.advance(1.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_capacity_suspend_extended_by_acquire(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.suspend(5.0)
    r.suspend(10.0)

    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - 10.0) < 0.01

    clock.advance(10.0)
    ok, wait = r.can_acquire(1.0)
    assert ok is True


# ────────────────────────────────────────────────────────────────
# Edge / adversarial resource states (invariant verification)
# ────────────────────────────────────────────────────────────────


def test_capacity_max_zero_blocks_all_nonzero(monkeypatch):
    """max_capacity=0.0 → only amount=0.0 can acquire; anything else is impossible."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=0.0)
    ok, wait = r.can_acquire(0.0)
    assert ok is True
    assert wait == 0.0

    ok, wait = r.can_acquire(0.1)
    assert ok is False
    assert wait == float("inf")  # impossible request


def test_capacity_negative_amount_raises(monkeypatch):
    """acquire(-1.0) raises ValueError instead of silently clamping."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    with pytest.raises(ValueError, match="amount must be non-negative"):
        r.acquire(-1.0)

    # can_acquire still works
    ok, _ = r.can_acquire(1.0)
    assert ok is True


def test_rate_negative_interval_rejected():
    """interval_seconds=-1.0 → 构造时拒绝（负间隔静默禁用限速，真 bug）。"""
    with pytest.raises(ValueError, match="interval_seconds"):
        RateLimitResource("test", interval_seconds=-1.0)


def test_rate_negative_amount_raises(monkeypatch):
    """acquire(-1.0) raises ValueError instead of silently clamping."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    with pytest.raises(ValueError, match="amount must be non-negative"):
        r.acquire(-1.0)

    ok, _ = r.can_acquire(1.0)
    assert ok is True


def test_capacity_release_negative_amount_raises(monkeypatch):
    """release(-1.0) raises ValueError instead of silently clamping."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.acquire(3.0)
    with pytest.raises(ValueError, match="amount must be non-negative"):
        r.release(-1.0)
    assert r.used == 3.0


def test_capacity_acquire_exactly_max_then_zero_ok(monkeypatch):
    """acquire(max) fills capacity; can_acquire(0.0) still True, epsilon blocked."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.acquire(5.0)

    ok, wait = r.can_acquire(0.0)
    assert ok is True
    assert wait == 0.0

    ok, wait = r.can_acquire(0.0001)
    assert ok is False
    assert wait == 0.5  # poll interval


def test_rate_suspend_zero_seconds_is_noop(monkeypatch):
    """suspend(0.0) on rate limit is effectively a no-op."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(1.0)  # next_available = 2.0
    r.suspend(0.0)  # next_available = max(2.0, 0.0 + 0.0) = 2.0

    assert r.next_available == 2.0


def test_capacity_suspend_zero_seconds_is_noop(monkeypatch):
    """suspend(0.0) on capacity is effectively a no-op."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    r.suspend(0.0)  # suspend_until = max(0.0, 0.0 + 0.0) = 0.0

    ok, wait = r.can_acquire(1.0)
    assert ok is True
    assert wait == 0.0


def test_rate_suspend_negative_seconds_no_effect_when_already_blocked(monkeypatch):
    """suspend(-5.0) when next_available is in future → no effect (max keeps future)."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(1.0)  # next_available = 2.0
    r.suspend(-5.0)  # next_available = max(2.0, 0.0 - 5.0) = max(2.0, -5.0) = 2.0

    assert r.next_available == 2.0


def test_capacity_no_drift_after_many_acquire_release_cycles(monkeypatch):
    """1000 acquire/release cycles leave used at exactly 0.0 (no float drift)."""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=10.0)
    for _ in range(1000):
        r.acquire(3.3)
        r.release(3.3)
    assert r.used == 0.0


def test_capacity_acquire_exceeds_capacity_raises(monkeypatch):
    """直接调用 acquire 时，amount 超过 capacity 应抛出 ValueError。"""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = CapacityResource("test", max_capacity=5.0)
    with pytest.raises(ValueError, match="amount 6.0 exceeds resource capacity 5.0"):
        r.acquire(6.0)


def test_rate_suspend_oversized_clamped(monkeypatch):
    """suspend(1e12) 被钳制到 _MAX_SUSPEND_SECONDS，防止子进程永久停摆管线。"""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    from tasklite.engine.resource import _MAX_SUSPEND_SECONDS
    r = RateLimitResource("test", interval_seconds=2.0)
    r.suspend(1e12)

    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - _MAX_SUSPEND_SECONDS) < 0.01


def test_capacity_suspend_oversized_clamped(monkeypatch):
    """CapacityResource.suspend(1e12) 同样被钳制到上限。"""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    from tasklite.engine.resource import _MAX_SUSPEND_SECONDS
    r = CapacityResource("test", max_capacity=5.0)
    r.suspend(1e12)

    ok, wait = r.can_acquire(1.0)
    assert ok is False
    assert abs(wait - _MAX_SUSPEND_SECONDS) < 0.01


def test_rate_suspend_non_numeric_ignored(monkeypatch):
    """suspend('30')（字符串）被拒绝，不改变 next_available。"""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(1.0)  # next_available = 2.0
    r.suspend("30")  # 非数值 → 忽略

    assert r.next_available == 2.0


def test_rate_suspend_nan_ignored(monkeypatch):
    """suspend(nan) 被拒绝，不改变 next_available。"""
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=2.0)
    r.acquire(1.0)
    r.suspend(float("nan"))

    assert r.next_available == 2.0


def test_suspend_overflow_int_clamped(monkeypatch):
    """超大 int（10**400）不再击穿 isfinite 防御。

    超大浮点数校验防止 OverflowError 穿透至调度器：
    worker 崩溃。修复后正值钳制到上限、负值按无效忽略。
    """
    clock = FakeClock()
    monkeypatch.setattr(_RESOURCE_TIME, clock)

    r = RateLimitResource("test", interval_seconds=1.0)
    r.acquire(1.0)
    r.suspend(10 ** 400) # 修复前：OverflowError 击穿
    assert r.next_available == 86400.0, "正值钳制到 _MAX_SUSPEND_SECONDS（max 语义）"

    r2 = RateLimitResource("test2", interval_seconds=1.0)
    r2.acquire(1.0)
    r2.suspend(-(10 ** 400))
    assert r2.next_available == 1.0, "负值超大 int 按无效忽略"
