"""v2 资源体系契约测试：资源类 / 管理器 / 三态查询 / 通知机制。

契约基线来自 ADR-0011：
1. 内置公开资源从 tasklite.resource 导入（CapacityResource / IntervalResource）；
2. 构造参数配置为关键字参数，仅名称支持位置参数；
3. 三态查询：可用、暂不可用、明确不可满足；
4. 主控制线程串行连续获取与释放，无 reserve/claim 两阶段与 isinstance 具体类型特判；
5. 不采用周期轮询，结合计时与通知实现可唤醒等待，check-to-wait 间通知不丢失。
"""

import math
import threading
import time

import pytest

from tasklite import TaskLiteError

from tasklite.engine.resource import (
    CapacityResource,
    IntervalResource,
    Resource,
    ResourceEvaluation,
    ResourceManager,
    ResourceQueryResult,
    RunNotifier,
)
from tasklite.models.job import WORKER_RESOURCE
from tasklite.models.task import Task
from tasklite.resource import (
    CapacityResource as PublicCapacityResource,
    IntervalResource as PublicIntervalResource,
)

# monkeypatch 目标：resource.py 模块级 ``import time``
_RESOURCE_TIME = "tasklite.engine.resource.time.monotonic"


class FakeClock:
    """可控时钟（mock time.monotonic()）。"""

    def __init__(self, start: float = 0.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _ok_handler(job, ctx):
    return True


class TestPublicImport:
    """公开导入面契约。"""

    def test_imports_match(self):
        assert PublicCapacityResource is CapacityResource
        assert PublicIntervalResource is IntervalResource


class TestIntervalResource:
    """间隔资源：时间片推进语义（amount 缩放间隔，完成不退时间配额）。"""

    def test_keyword_only_constructor(self):
        with pytest.raises(TypeError):
            IntervalResource("test", 2.0)  # type: ignore[misc]
        res = IntervalResource("test", interval_seconds=2.0)
        assert res.name == "test"
        assert res.interval_seconds == 2.0

    def test_acquire_advances_time(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = IntervalResource("test", interval_seconds=2.0)
        res.acquire(1.0)

        query = res.can_acquire(1.0)
        assert query.is_available is False
        assert query.is_impossible is False
        assert query.wait_seconds is not None
        assert abs(query.wait_seconds - 2.0) < 0.01

        # 解包兼容测试
        ok, wait = query
        assert ok is False
        assert abs(wait - 2.0) < 0.01

        clock.advance(2.0)
        query = res.can_acquire(1.0)
        assert query.is_available is True
        assert query.wait_seconds == 0.0

    def test_can_acquire_immediate(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = IntervalResource("test", interval_seconds=2.0)
        query = res.can_acquire(1.0)
        assert query.is_available is True
        assert query.wait_seconds == 0.0

    def test_amount_multiplies_interval(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = IntervalResource("test", interval_seconds=2.0)
        res.acquire(3.0)

        clock.advance(5.9)
        query = res.can_acquire(1.0)
        assert query.is_available is False
        assert query.wait_seconds is not None
        assert abs(query.wait_seconds - 0.1) < 0.02

        clock.advance(0.1)
        query = res.can_acquire(1.0)
        assert query.is_available is True

    def test_multiple_acquires_accumulate(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = IntervalResource("test", interval_seconds=2.0)
        res.acquire(1.0)
        res.acquire(1.0)

        query = res.can_acquire(1.0)
        assert query.is_available is False
        assert query.wait_seconds is not None
        assert abs(query.wait_seconds - 4.0) < 0.02

        clock.advance(4.0)
        query = res.can_acquire(1.0)
        assert query.is_available is True

    def test_release_noop(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = IntervalResource("test", interval_seconds=2.0)
        res.acquire(1.0)
        res.release(1.0)

        query = res.can_acquire(1.0)
        assert query.is_available is False
        assert query.wait_seconds is not None
        assert abs(query.wait_seconds - 2.0) < 0.01

    def test_negative_amount_raises(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_RESOURCE_TIME, clock)

        res = IntervalResource("test", interval_seconds=2.0)
        with pytest.raises(TaskLiteError, match="amount must be non-negative"):
            res.acquire(-1.0)
        with pytest.raises(TaskLiteError, match="amount must be non-negative"):
            res.can_acquire(-1.0)

    @pytest.mark.parametrize("bad_interval", [0.0, -1.0, float("nan"), float("inf")])
    def test_invalid_interval_rejected(self, bad_interval):
        with pytest.raises(TaskLiteError, match="interval_seconds"):
            IntervalResource("test", interval_seconds=bad_interval)

    def test_non_numeric_interval_rejected(self):
        with pytest.raises(TaskLiteError, match="interval_seconds"):
            IntervalResource("test", interval_seconds=True)  # type: ignore[arg-type]


class TestCapacityResource:
    """容量资源：并发量控制与明确不可能判定。"""

    def test_keyword_only_constructor(self):
        with pytest.raises(TypeError):
            CapacityResource("test", 5.0)  # type: ignore[misc]
        res = CapacityResource("test", max_capacity=5.0)
        assert res.name == "test"
        assert res.max_capacity == 5.0

    def test_basic_enforcement(self):
        res = CapacityResource("test", max_capacity=5.0)
        res.acquire(3.0)

        # 再次申请 3.0：3+3=6 > 5，容量不足（暂不可用，无恢复时间，不周期轮询）
        query = res.can_acquire(3.0)
        assert query.is_available is False
        assert query.is_impossible is False
        assert query.wait_seconds is None  # 无周期轮询，等待释放通知

        # 解包兼容测试：未知等待解包为 inf
        ok, wait = query
        assert ok is False
        assert wait == float("inf")

        # 释放 3.0
        res.release(3.0)
        query = res.can_acquire(5.0)
        assert query.is_available is True
        assert query.wait_seconds == 0.0

    def test_impossible_request(self):
        res = CapacityResource("test", max_capacity=5.0)
        query = res.can_acquire(6.0)
        assert query.is_available is False
        assert query.is_impossible is True
        assert query.reason is not None

        # 解包为 (False, inf)
        ok, wait = query
        assert ok is False
        assert wait == float("inf")

    def test_acquire_exceeds_capacity_raises(self):
        res = CapacityResource("test", max_capacity=5.0)
        with pytest.raises(TaskLiteError, match="exceeds resource capacity"):
            res.acquire(6.0)

    def test_acquire_beyond_available_is_rejected(self):
        res = CapacityResource("test", max_capacity=5.0)
        res.acquire(4.0)
        with pytest.raises(TaskLiteError, match="capacity exceeded"):
            res.acquire(2.0)

    def test_release_notifies_bound_notifier(self):
        notifier = RunNotifier()
        res = CapacityResource("test", max_capacity=2.0)
        res.bind_notifier(notifier)

        res.acquire(2.0)
        assert notifier.snapshot() == 0

        res.release(1.0)
        assert notifier.snapshot() == 1, "释放容量必须通知 RunNotifier"

    @pytest.mark.parametrize("bad_capacity", [-1.0, float("nan"), float("inf")])
    def test_invalid_capacity_rejected(self, bad_capacity):
        with pytest.raises(TaskLiteError, match="max_capacity"):
            CapacityResource("test", max_capacity=bad_capacity)


class TestRunNotifier:
    """RunNotifier 唤醒与防丢通知。"""

    def test_notify_increments_sequence_and_sets_event(self):
        notifier = RunNotifier()
        assert notifier.snapshot() == 0
        notifier.notify()
        assert notifier.snapshot() == 1

    def test_lost_wakeup_immunity(self):
        """check-to-wait 间通知不丢失：快照后有通知，wait 立即返回 True。"""
        notifier = RunNotifier()
        seq = notifier.snapshot()

        # 模拟在 check 之后、wait 之前发生通知
        notifier.notify()

        # 此时 wait(seq) 必须立即返回 True，不等待超时期
        start = time.monotonic()
        woken = notifier.wait(seq, timeout=10.0)
        duration = time.monotonic() - start

        assert woken is True
        assert duration < 0.2, "快照后的通知必须立即使 wait 返回，无须等待 timeout"

    def test_external_notify_wakes_early(self):
        """额外通知早于原计时唤醒。"""
        notifier = RunNotifier()
        seq = notifier.snapshot()

        def wake_soon():
            time.sleep(0.05)
            notifier.notify()

        t = threading.Thread(target=wake_soon)
        t.start()

        start = time.monotonic()
        woken = notifier.wait(seq, timeout=10.0)
        duration = time.monotonic() - start
        t.join()

        assert woken is True
        assert duration < 1.0, "额外通知必须早于 10.0s 唤醒主线程"


class TestResourceManager:
    """ResourceManager 三态评估与连续获取/释放。"""

    def test_effective_resources_merge(self):
        tasks = {"task_a": Task("task_a", _ok_handler, resources={"cpu": 1.0, "mem": 2.0})}
        mgr = ResourceManager(tasks=tasks)

        merged = mgr.effective_resources("task_a", {"mem": 4.0, "gpu": 1.0})
        assert merged == {"cpu": 1.0, "mem": 4.0, "gpu": 1.0}

    def test_evaluate_three_states(self):
        cap = CapacityResource("slots", max_capacity=2.0)
        mgr = ResourceManager({"slots": cap})

        # 1. 可用
        ev = mgr.evaluate("t", {"slots": 1.0})
        assert ev.is_available is True
        assert ev.is_impossible is False

        # 2. 暂不可用（未知恢复时间）
        cap.acquire(2.0)
        ev = mgr.evaluate("t", {"slots": 1.0})
        assert ev.is_available is False
        assert ev.is_impossible is False

        # 3. 明确不可满足（超容量）
        ev = mgr.evaluate("t", {"slots": 5.0})
        assert ev.is_available is False
        assert ev.is_impossible is True

        # 4. 明确不可满足（未注册资源）
        ev = mgr.evaluate("t", {"unregistered": 1.0})
        assert ev.is_available is False
        assert ev.is_impossible is True
        assert ev.is_unknown is True

    def test_acquire_all_and_release_all(self):
        c1 = CapacityResource("c1", max_capacity=2.0)
        c2 = CapacityResource("c2", max_capacity=4.0)
        mgr = ResourceManager({"c1": c1, "c2": c2})

        acquired = mgr.acquire_all("t", {"c1": 1.0, "c2": 2.0})
        assert len(acquired) == 2
        assert c1.used == 1.0
        assert c2.used == 2.0

        mgr.release_all(acquired)
        assert c1.used == 0.0
        assert c2.used == 0.0

    def test_acquire_all_exception_propagates(self):
        c1 = CapacityResource("c1", max_capacity=1.0)
        c1.acquire(1.0)
        mgr = ResourceManager({"c1": c1})

        with pytest.raises(TaskLiteError):
            mgr.acquire_all("t", {"c1": 1.0})

    def test_release_all_exception_propagates(self):
        class FaultyResource(Resource):
            def can_acquire(self, amount: float) -> ResourceQueryResult:
                return ResourceQueryResult(is_available=True)

            def acquire(self, amount: float) -> None:
                pass

            def release(self, amount: float) -> None:
                raise RuntimeError("Release failed!")

        mgr = ResourceManager({"faulty": FaultyResource("faulty")})
        with pytest.raises(RuntimeError, match="Release failed!"):
            mgr.release_all([("faulty", 1.0)])
