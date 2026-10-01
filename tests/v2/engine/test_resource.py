"""v2 资源体系契约测试：资源类 / 管理器 / 两阶段租约 / 挂起与持久化。

移植 v1 test_resource.py / test_resource_manager.py /
test_apply_suspend_signals.py / test_resource_persistence.py（资源层可
测面；管线级重启场景由后续 recovery 单元承接）并按 v2 术语改写：
挂起统一 suspensions、Task 规格替代 handler 条目、meta 键
``resource_suspensions``。
"""

import json
import math
import time

import pytest

from tasklite.v2.backend import InMemoryStateBackend
from tasklite.v2.engine.resource import (
    META_RESOURCE_SUSPENSIONS,
    CapacityResource,
    RateLimitResource,
    RateLimitUnavailable,
    ResourceManager,
    ResourceEvaluation,
    apply_suspend_signals,
    persist_resource_suspensions,
)
from tasklite.v2.models.job import WORKER_RESOURCE
from tasklite.v2.models.task import Task

# monkeypatch 目标：resource.py 模块级 ``import time``
_RESOURCE_TIME = "tasklite.v2.engine.resource.time.monotonic"


class FakeClock:
    """可控时钟（mock time.monotonic()）。"""

    def __init__(self, start: float = 0.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _ok_handler(job, ctx):
    """模块级 handler 样本（Task 规格要求可调用对象）。"""
    return True


class TestRateLimitResource:
    """限速资源：时间片推进语义（amount 缩放间隔，非请求次数）。"""

    def test_acquire_advances_time(self, monkeypatch):
        """acquire(1.0) 把 next_available 推进 2.0s；越过后可获取。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(1.0)

        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 2.0) < 0.01

        clock.advance(2.0)
        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_can_acquire_immediate(self, monkeypatch):
        """未 acquire 前首次 can_acquire → (True, 0.0)。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_amount_multiplies_interval(self, monkeypatch):
        """acquire(3.0) 推进 6.0s（interval × amount）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(3.0)

        clock.advance(5.9)
        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 0.1) < 0.02

        clock.advance(0.1)
        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_multiple_acquires_accumulate(self, monkeypatch):
        """两次 acquire(1.0) → next_available 累计推进 4.0s。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(1.0)
        res.acquire(1.0)

        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 4.0) < 0.02

        clock.advance(4.0)
        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_exact_wait_boundary(self, monkeypatch):
        """acquire 后恰好推进一个 interval → (True, 0.0)。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(1.0)

        clock.advance(2.0)
        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_release_noop(self, monkeypatch):
        """时间片消费不可撤销：release() 不改变可用性（基类契约）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(1.0)
        res.release(1.0)

        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 2.0) < 0.01

    def test_amount_zero_never_blocks(self, monkeypatch):
        """acquire(0.0) 推进 0 秒——立即可用。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(0.0)

        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_multiple_zero_acquires(self, monkeypatch):
        """多次 acquire(0.0) 不推进 next_available。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(0.0)
        res.acquire(0.0)
        res.acquire(0.0)

        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_negative_amount_raises(self, monkeypatch):
        """acquire(-1.0) 显式 ValueError，不静默钳制。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        with pytest.raises(ValueError, match="amount must be non-negative"):
            res.acquire(-1.0)

        ok, _ = res.can_acquire(1.0)
        assert ok is True

    @pytest.mark.parametrize("bad_interval", [0.0, -1.0, float("nan"), float("inf")])
    def test_invalid_interval_rejected(self, bad_interval):
        """非有限正 interval 构造期拒绝（负间隔静默禁用限速）。"""
        with pytest.raises(ValueError, match="interval_seconds"):
            RateLimitResource("test", interval_seconds=bad_interval)

    def test_non_numeric_interval_rejected(self):
        with pytest.raises(TypeError, match="interval_seconds"):
            RateLimitResource("test", interval_seconds=True)  # type: ignore[arg-type]

    def test_suspended_until_none_when_idle(self, monkeypatch):
        """无挂起/无限速等待时 suspended_until 返回 None（与容量资源对齐）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        assert res.suspended_until() is None

        res.acquire(1.0)
        assert res.suspended_until() is not None

        clock.advance(2.0)
        assert res.suspended_until() is None


class TestRateLimitSuspension:
    """限速资源的挂起语义：max 合并（幂等去重，非累加）。"""

    def test_suspend_extends(self, monkeypatch):
        """acquire 后 suspend(10.0)：2s 后仍阻塞，10s 全程结束才放行。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(1.0)
        res.suspend(10.0)

        clock.advance(2.0)
        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 8.0) < 0.01

        clock.advance(8.0)
        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_suspend_via_manager_extends_next_available(self, monkeypatch):
        """ResourceManager.suspend_resource 对限速资源推进 next_available。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(1.0)

        clock.advance(1.0)
        ok, _ = res.can_acquire(1.0)
        assert ok is False

        res.suspend(10.0)

        clock.advance(1.0)
        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 9.0) < 0.01

    def test_suspend_zero_seconds_is_noop(self, monkeypatch):
        """suspend(0.0) 无效果（max 语义保持原截止）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(1.0)  # next_available = 2.0
        res.suspend(0.0)

        assert res.next_available == 2.0

    def test_suspend_negative_seconds_no_effect_when_already_blocked(self, monkeypatch):
        """suspend(-5.0) 在已有未来截止时无效果（max 保留未来时刻）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(1.0)  # next_available = 2.0
        res.suspend(-5.0)

        assert res.next_available == 2.0

    def test_suspend_oversized_clamped(self, monkeypatch):
        """suspend(1e12) 钳制到上限，防止子进程超大值永久停摆管线。"""
        from tasklite.v2.engine.resource import _MAX_SUSPEND_SECONDS

        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.suspend(1e12)

        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - _MAX_SUSPEND_SECONDS) < 0.01

    def test_suspend_non_numeric_ignored(self, monkeypatch):
        """suspend("30")（字符串）被拒绝，不改变 next_available。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(1.0)
        res.suspend("30")  # type: ignore[arg-type]

        assert res.next_available == 2.0

    def test_suspend_nan_ignored(self, monkeypatch):
        """suspend(nan) 被拒绝，不改变 next_available。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=2.0)
        res.acquire(1.0)
        res.suspend(float("nan"))

        assert res.next_available == 2.0

    def test_suspend_overflow_int_clamped(self, monkeypatch):
        """超大 int（10**400）不得击穿 isfinite 防御：正值钳制、负值忽略。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = RateLimitResource("test", interval_seconds=1.0)
        res.acquire(1.0)
        res.suspend(10**400)
        assert res.next_available == 86400.0, "正值钳制到上限（max 语义）"

        other = RateLimitResource("test2", interval_seconds=1.0)
        other.acquire(1.0)
        other.suspend(-(10**400))
        assert other.next_available == 1.0, "负值超大 int 按无效忽略"


class TestCapacityResource:
    """容量资源：并发量控制与不可能请求死锁防御。"""

    def test_basic_enforcement(self, monkeypatch):
        """acquire(3) → can_acquire(3) 阻塞（轮询等待）；release 后满额可用。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.acquire(3.0)

        ok, wait = res.can_acquire(3.0)
        assert ok is False
        assert wait == 0.5

        res.release(3.0)
        ok, wait = res.can_acquire(5.0)
        assert ok is True
        assert wait == 0.0

    def test_impossible_request(self, monkeypatch):
        """申请超过 max_capacity → (False, inf)（不可能满足，死锁防御）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        ok, wait = res.can_acquire(6.0)
        assert ok is False
        assert wait == float("inf")

    def test_max_zero_blocks_all_nonzero(self, monkeypatch):
        """max_capacity=0 → 仅 amount=0 可获取，其余为不可能请求。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=0.0)
        ok, wait = res.can_acquire(0.0)
        assert ok is True
        assert wait == 0.0

        ok, wait = res.can_acquire(0.1)
        assert ok is False
        assert wait == float("inf")

    def test_rapid_cycle_no_state_leak(self, monkeypatch):
        """acquire(5) → release(5) → acquire(5) 快速循环无状态泄漏。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.acquire(5.0)
        res.release(5.0)
        res.acquire(5.0)

        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert wait == 0.5

    def test_acquire_exceeds_capacity_raises(self, monkeypatch):
        """直接 acquire 超容量 → ValueError（不静默放行）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        with pytest.raises(ValueError, match="amount 6.0 exceeds resource capacity 5.0"):
            res.acquire(6.0)

    def test_acquire_exactly_max_then_zero_ok(self, monkeypatch):
        """占满容量后 amount=0 仍可用（零占用声明语义）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.acquire(5.0)

        ok, wait = res.can_acquire(0.0)
        assert ok is True
        assert wait == 0.0

        ok, wait = res.can_acquire(0.0001)
        assert ok is False
        assert wait == 0.5

    def test_no_drift_after_many_cycles(self, monkeypatch):
        """1000 次 acquire/release 循环后 used 恰为 0.0（无浮点漂移）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=10.0)
        for _ in range(1000):
            res.acquire(3.3)
            res.release(3.3)
        assert res.used == 0.0

    @pytest.mark.parametrize("bad_capacity", [-1.0, float("nan"), float("inf")])
    def test_invalid_capacity_rejected(self, bad_capacity):
        """负值/NaN/Inf 容量构造期拒绝（NaN 致 livelock、负值致全灭）。"""
        with pytest.raises(ValueError, match="max_capacity"):
            CapacityResource("test", max_capacity=bad_capacity)

    def test_non_numeric_capacity_rejected(self):
        with pytest.raises(TypeError, match="max_capacity"):
            CapacityResource("test", max_capacity=True)  # type: ignore[arg-type]


class TestCapacityReleaseGuards:
    """容量释放守卫：floor 零、负值拒绝与超量计数。"""

    def test_release_floors_zero(self, monkeypatch):
        """used=0 时 release(10.0) → used 保持 0（不为负）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.release(10.0)
        assert res.used == 0.0

    def test_release_over_capacity_clamps_and_counts(self, monkeypatch):
        """acquire(3) + release(5) → used=0；超量释放计数器记录 1
        （变异锁定：删掉计数递增即红）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.acquire(3.0)
        res.release(5.0)
        assert res.used == 0.0
        assert res.release_overruns == 1

        ok, wait = res.can_acquire(5.0)
        assert ok is True
        assert wait == 0.0

    def test_release_negative_amount_raises(self, monkeypatch):
        """release(-1.0) 显式 ValueError，used 不变。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.acquire(3.0)
        with pytest.raises(ValueError, match="amount must be non-negative"):
            res.release(-1.0)
        assert res.used == 3.0

    def test_negative_amount_acquire_raises(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        with pytest.raises(ValueError, match="amount must be non-negative"):
            res.acquire(-1.0)

        ok, _ = res.can_acquire(1.0)
        assert ok is True


class TestCapacitySuspension:
    """容量资源的挂起语义。"""

    def test_suspend_blocks(self, monkeypatch):
        """suspend(10.0) → 10s 内不可获取（与容量无关）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.suspend(10.0)

        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 10.0) < 0.01

        clock.advance(9.9)
        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 0.1) < 0.02

        clock.advance(0.1)
        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_suspend_dominates_capacity_wait(self, monkeypatch):
        """容量已释放但挂起未到 → 仍按挂起截止等待。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.acquire(5.0)
        res.suspend(10.0)

        clock.advance(1.0)
        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 9.0) < 0.01

        res.release(5.0)
        clock.advance(8.0)
        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 1.0) < 0.02

        clock.advance(1.0)
        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_repeated_suspend_takes_max(self, monkeypatch):
        """suspend(5) 后 suspend(10) → 总挂起 10s（max 语义，非累加）。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.suspend(5.0)
        res.suspend(10.0)

        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - 10.0) < 0.01

        clock.advance(10.0)
        ok, _ = res.can_acquire(1.0)
        assert ok is True

    def test_suspend_zero_seconds_is_noop(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.suspend(0.0)

        ok, wait = res.can_acquire(1.0)
        assert ok is True
        assert wait == 0.0

    def test_suspended_until_none_when_idle_or_expired(self, monkeypatch):
        """无挂起或挂起过期时 suspended_until 返回 None。"""
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        assert res.suspended_until() is None

        res.suspend(5.0)
        assert res.suspended_until() is not None

        clock.advance(5.0)
        assert res.suspended_until() is None

    def test_suspend_oversized_clamped(self, monkeypatch):
        from tasklite.v2.engine.resource import _MAX_SUSPEND_SECONDS

        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = CapacityResource("test", max_capacity=5.0)
        res.suspend(1e12)

        ok, wait = res.can_acquire(1.0)
        assert ok is False
        assert abs(wait - _MAX_SUSPEND_SECONDS) < 0.01


class TestResourceManagerMapping:
    """Dict-like 注册表兼容面。"""

    def test_dict_like_crud(self):
        mgr = ResourceManager()
        gpu = CapacityResource("gpu", 10.0)
        mgr["gpu"] = gpu

        assert "gpu" in mgr
        assert len(mgr) == 1
        assert mgr["gpu"] is gpu
        assert list(mgr.keys()) == ["gpu"]
        assert list(mgr.values()) == [gpu]

        del mgr["gpu"]
        assert "gpu" not in mgr
        assert len(mgr) == 0


class TestEffectiveResources:
    """Task 默认资源与 Job 声明资源的单点合并。"""

    def test_without_task_defaults(self):
        mgr = ResourceManager()
        eff = mgr.effective_resources("train", {"gpu": 2.0})
        assert eff == {"gpu": 2.0}

    def test_task_defaults_merged(self):
        tasks = {
            "train": Task(
                task_type="train",
                handler=_ok_handler,
                default_resources={"gpu": 1.0, "ram": 4.0},
            )
        }
        mgr = ResourceManager(tasks=tasks)

        # 声明值覆盖同名默认，未声明项保留默认
        eff = mgr.effective_resources("train", {"gpu": 2.0})
        assert eff == {"gpu": 2.0, "ram": 4.0}

        # 未声明资源 → 完全使用默认
        eff_default = mgr.effective_resources("train", None)
        assert eff_default == {"gpu": 1.0, "ram": 4.0}

    def test_pair_sequence_declared_resources(self):
        mgr = ResourceManager()
        eff = mgr.effective_resources("train", [("gpu", 2.0), ("ram", 8.0)])
        assert eff == {"gpu": 2.0, "ram": 8.0}


class TestResourceManagerEvaluation:
    """可用性与死锁归因评估（unknown / impossible / wait）。"""

    def test_evaluate_unknown_resource(self):
        mgr = ResourceManager()
        eval_res = mgr.evaluate("task", {"unknown_res": 1.0})
        assert not eval_res.is_available
        assert eval_res.is_unknown
        assert eval_res.unknown_name == "unknown_res"
        assert eval_res.wait_time == float("inf")

    def test_evaluate_impossible_resource(self):
        mgr = ResourceManager({"gpu": CapacityResource("gpu", 4.0)})
        eval_res = mgr.evaluate("task", {"gpu": 8.0})
        assert not eval_res.is_available
        assert eval_res.is_impossible
        assert eval_res.impossible_name == "gpu"
        assert eval_res.wait_time == float("inf")

    def test_evaluate_available_then_waiting(self):
        mgr = ResourceManager({"gpu": CapacityResource("gpu", 4.0)})
        eval_res = mgr.evaluate("task", {"gpu": 2.0})
        assert eval_res.is_available
        assert eval_res.wait_time == 0.0

        mgr["gpu"].acquire(4.0)
        eval_again = mgr.evaluate("task", {"gpu": 2.0})
        assert not eval_again.is_available
        assert not eval_again.is_unknown
        assert not eval_again.is_impossible
        assert eval_again.wait_time > 0.0

    def test_evaluation_value_object_defaults(self):
        eval_res = ResourceEvaluation(is_available=True, wait_time=0.0)
        assert eval_res.is_unknown is False
        assert eval_res.is_impossible is False
        assert eval_res.unknown_name is None
        assert eval_res.impossible_name is None


class TestReleaseAllSafety:
    """批量释放幂等与异常安全。"""

    def test_release_all_rollback_after_reserve_acquire(self):
        """reserve 预扣的容量可经 release_all 全量归还（事务性回滚通道）。"""
        gpu = CapacityResource("gpu", 10.0)
        mgr = ResourceManager({"gpu": gpu})
        lease = mgr.reserve("task", {"gpu": 4.0})
        assert gpu.used == 4.0

        mgr.release_all(lease.acquired)
        assert gpu.used == 0.0

    def test_release_all_skips_malformed_items(self):
        """畸形条目（非二元组）跳过不打断批释放（异常安全）。"""
        gpu = CapacityResource("gpu", 4.0)
        mgr = ResourceManager({"gpu": gpu})
        mgr.release_all([("gpu", 2.0), "garbage", ("nope", 1.0)])
        assert gpu.used == 0.0


class TestResourceLease:
    """两阶段租约：预扣 → 兑现 → 释放。"""

    def test_two_phase_lease_capacity_and_rate_limit(self):
        gpu = CapacityResource("gpu", 4.0)
        api = RateLimitResource("api", interval_seconds=10.0)
        mgr = ResourceManager({"gpu": gpu, "api": api})

        # 预约阶段：gpu 立即扣减，api 暂不推进 next_available
        before = api.next_available
        lease = mgr.reserve("task", {"gpu": 2.0, "api": 1.0}, uid="t::j1")
        assert lease.status.value == "reserved"
        assert gpu.used == 2.0
        assert api.next_available == before

        # 兑现阶段：api 时间片推进
        lease.claim()
        assert lease.status.value == "claimed"
        assert api.next_available > before

        # 释放阶段：gpu 容量归还；幂等释放不再二次归还
        lease.release()
        assert lease.status.value == "released"
        assert gpu.used == 0.0
        lease.release()
        assert gpu.used == 0.0

    def test_reserve_rate_limit_unavailable_raises_and_rolls_back(self):
        """限速等待窗内预约 → RateLimitUnavailable 且已占容量回滚。"""
        gpu = CapacityResource("gpu", 4.0)
        api = RateLimitResource("api", interval_seconds=10.0)
        mgr = ResourceManager({"gpu": gpu, "api": api})
        api.acquire(1.0)  # 制造限速等待窗

        with pytest.raises(RateLimitUnavailable, match="api"):
            mgr.reserve("task", {"gpu": 2.0, "api": 1.0}, uid="t::j2")
        assert gpu.used == 0.0, "预约失败必须回滚已占容量"

    def test_reserve_unknown_resource_rolls_back(self):
        gpu = CapacityResource("gpu", 4.0)
        mgr = ResourceManager({"gpu": gpu})
        with pytest.raises(KeyError, match="not registered"):
            mgr.reserve("task", {"gpu": 2.0, "nope": 1.0})
        assert gpu.used == 0.0

    def test_lease_context_manager_auto_rollback_on_exception(self):
        gpu = CapacityResource("gpu", 4.0)
        mgr = ResourceManager({"gpu": gpu})

        with pytest.raises(RuntimeError):
            with mgr.reserve("task", {"gpu": 3.0}, uid="t::err") as lease:
                assert gpu.used == 3.0
                assert lease.status.value == "reserved"
                raise RuntimeError("dispatch failed")

        assert gpu.used == 0.0


class TestWorkerSlots:
    """worker 槽位查询（未注册视为不设限）。"""

    def test_can_acquire_worker(self):
        worker = CapacityResource(WORKER_RESOURCE, 2.0)
        mgr = ResourceManager({WORKER_RESOURCE: worker})
        ok, wait = mgr.can_acquire_worker(1.0)
        assert ok
        assert wait == 0.0

        worker.acquire(2.0)
        ok_again, wait_again = mgr.can_acquire_worker(1.0)
        assert not ok_again
        assert wait_again > 0.0

    def test_worker_slot_unregistered_means_unlimited(self):
        mgr = ResourceManager()
        ok, wait = mgr.can_acquire_worker()
        assert ok is True
        assert wait == 0.0


class TestSuspensionCollectAndRestore:
    """挂起收集（monotonic→挂钟换算）与恢复。"""

    def test_collect_and_restore_roundtrip(self):
        gpu = CapacityResource("gpu", 10.0)
        mgr = ResourceManager({"gpu": gpu})

        assert mgr.suspend_resource("gpu", 30.0)
        assert not mgr.suspend_resource("unknown", 30.0)

        now_mono = time.monotonic()
        now_wall = time.time()
        gpu.suspend_until = now_mono + 20.0
        suspensions = mgr.collect_suspensions(now_mono=now_mono, now_wall=now_wall)
        assert "gpu" in suspensions
        assert math.isclose(suspensions["gpu"], now_wall + 20.0, abs_tol=1e-1)

        # 模拟新进程实例上恢复
        gpu_fresh = CapacityResource("gpu", 10.0)
        mgr_fresh = ResourceManager({"gpu": gpu_fresh})
        mgr_fresh.restore_suspensions(suspensions, now_wall=now_wall)
        assert gpu_fresh.suspend_until > time.monotonic()

    def test_restore_expired_deadline_ignored(self):
        """挂钟截止已过 → 恢复后资源不暂停。"""
        api = RateLimitResource("api", interval_seconds=1.0)
        mgr = ResourceManager({"api": api})
        mgr.restore_suspensions({"api": time.time() - 100.0})
        assert api.suspended_until() is None

    def test_restore_invalid_deadlines_ignored(self):
        """非数值/bool/非有限截止逐条降级跳过，不抛异常。"""
        api = RateLimitResource("api", interval_seconds=1.0)
        mgr = ResourceManager({"api": api})
        mgr.restore_suspensions(
            {"api": "soon", "other": True, "third": float("inf")}
        )
        assert api.suspended_until() is None

    def test_restore_unregistered_resource_ignored(self):
        """挂起表中资源不在本管线注册表 → 告警跳过。"""
        api = RateLimitResource("api", interval_seconds=1.0)
        mgr = ResourceManager({"api": api})
        mgr.restore_suspensions(
            {"api": time.time() + 50.0, "gone": time.time() + 50.0}
        )
        assert api.suspended_until() is not None


class TestApplySuspendSignals:
    """``apply_suspend_signals`` 共享助手：应用成功才持久化，坏信号降级跳过。"""

    class _MetaRecorder:
        """记录 set_meta 调用的假后端（只实现助手依赖的最小面）。"""

        def __init__(self):
            self.calls: list[tuple[str, str]] = []

        def set_meta(self, key: str, value: str) -> None:
            self.calls.append((key, value))

    @staticmethod
    def _mgr() -> ResourceManager:
        return ResourceManager(
            resources={"api": RateLimitResource("api", interval_seconds=1.0)}
        )

    def test_applied_signal_persists_once(self):
        backend = self._MetaRecorder()
        mgr = self._mgr()
        apply_suspend_signals([("t::j1", "api", 300.0)], backend, mgr, origin="from ")
        assert len(backend.calls) == 1
        key, raw = backend.calls[0]
        assert key == META_RESOURCE_SUSPENSIONS
        assert "api" in json.loads(raw)
        assert mgr["api"].suspended_until() is not None

    def test_unknown_resource_skipped_without_persist(self):
        backend = self._MetaRecorder()
        mgr = self._mgr()
        apply_suspend_signals([("t::j1", "nope", 10.0)], backend, mgr)
        assert backend.calls == []
        assert mgr["api"].suspended_until() is None

    def test_mixed_batch_applies_valid_and_persists(self):
        backend = self._MetaRecorder()
        mgr = self._mgr()
        apply_suspend_signals(
            [("t::j1", "nope", 10.0), ("t::j2", "api", 60.0)], backend, mgr
        )
        assert len(backend.calls) == 1
        assert mgr["api"].suspended_until() is not None

    def test_empty_batch_no_persist(self):
        backend = self._MetaRecorder()
        apply_suspend_signals([], backend, self._mgr())
        assert backend.calls == []


class TestSuspensionPersistenceAcrossCrash:
    """挂起截止跨崩溃持久化（新 meta 键 ``resource_suspensions``）。

    handler 对 "api" 挂起 300 秒（限流）后进程崩溃——若不落盘，挂起随
    进程消失，重启即放行。管线级「run 尾部落盘 + 启动加载」编排由后续
    recovery 单元承接，此处锁定资源层换算与落盘键。
    """

    def test_persist_writes_wall_clock_deadline_under_new_key(self):
        backend = InMemoryStateBackend()
        mgr = ResourceManager(
            resources={"api": RateLimitResource("api", interval_seconds=1.0)}
        )
        mgr.suspend_resource("api", 300.0)

        persist_resource_suspensions(backend, mgr)

        raw = backend.get_meta(META_RESOURCE_SUSPENSIONS)
        assert raw is not None
        deadlines = json.loads(raw)
        assert "api" in deadlines
        # 挂钟截止 ≈ 当前时刻 + 300s（落盘时刻与断言时刻之间的微小漂移容差）
        assert 299.0 < deadlines["api"] - time.time() <= 300.5

    def test_persist_backend_failure_degrades_to_log(self):
        """后端落盘失败降级告警，不向挂起应用方抛异常。"""

        class _FailingBackend:
            def set_meta(self, key: str, value: str) -> None:
                raise RuntimeError("disk gone")

        mgr = ResourceManager(
            resources={"api": RateLimitResource("api", interval_seconds=1.0)}
        )
        mgr.suspend_resource("api", 60.0)
        # 不抛异常即契约成立
        persist_resource_suspensions(_FailingBackend(), mgr)

    def test_persist_then_restore_keeps_remaining_wait(self):
        """落盘 → 模拟崩溃 → 新实例恢复：剩余等待 ≈ 原挂起时长（±1s）。"""
        backend = InMemoryStateBackend()
        mgr = ResourceManager(
            resources={"api": RateLimitResource("api", interval_seconds=1.0)}
        )
        mgr.suspend_resource("api", 300.0)
        persist_resource_suspensions(backend, mgr)

        fresh = ResourceManager(
            resources={"api": RateLimitResource("api", interval_seconds=1.0)}
        )
        fresh.restore_suspensions(json.loads(backend.get_meta(META_RESOURCE_SUSPENSIONS)))
        remaining = fresh["api"].next_available - time.monotonic()
        assert abs(remaining - 300.0) <= 1.0

    def test_no_active_suspension_persists_empty_snapshot(self):
        backend = InMemoryStateBackend()
        mgr = ResourceManager(
            resources={"api": RateLimitResource("api", interval_seconds=1.0)}
        )
        persist_resource_suspensions(backend, mgr)
        assert json.loads(backend.get_meta(META_RESOURCE_SUSPENSIONS)) == {}
