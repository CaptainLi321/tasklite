"""ResourceManager 统一资源管理器深模块单元测试套件。"""

import math
import time
import pytest

from tasklite.engine.resource import (
    CapacityResource,
    RateLimitResource,
    ResourceEvaluation,
    ResourceManager,
    WORKER_RESOURCE,
)
from tasklite.pipeline import HandlerEntry


class TestResourceManagerMappingCompatibility:
    def test_dict_like_crud_operations(self):
        rm = ResourceManager()
        r1 = CapacityResource("gpu", 10.0)
        rm["gpu"] = r1

        assert "gpu" in rm
        assert len(rm) == 1
        assert rm["gpu"] is r1
        assert list(rm.keys()) == ["gpu"]
        assert list(rm.values()) == [r1]

        del rm["gpu"]
        assert "gpu" not in rm
        assert len(rm) == 0


class TestResourceManagerEffectiveResources:
    def test_effective_resources_without_handler_defaults(self):
        rm = ResourceManager()
        eff = rm.effective_resources("train", {"gpu": 2.0})
        assert eff == {"gpu": 2.0}

    def test_effective_resources_with_handler_defaults(self):
        handlers = {
            "train": HandlerEntry(
                func=lambda j, c: True,
                default_resources={"gpu": 1.0, "ram": 4.0},
                payload_schema=None,
            )
        }
        rm = ResourceManager(handlers=handlers)

        # 仅声明部分资源，与默认资源合并
        eff = rm.effective_resources("train", {"gpu": 2.0})
        assert eff == {"gpu": 2.0, "ram": 4.0}

        # 未声明资源，完全使用默认资源
        eff_default = rm.effective_resources("train", None)
        assert eff_default == {"gpu": 1.0, "ram": 4.0}


class TestResourceManagerEvaluation:
    def test_evaluate_unknown_resource(self):
        rm = ResourceManager()
        eval_res = rm.evaluate("task", {"unknown_res": 1.0})
        assert not eval_res.is_available
        assert eval_res.is_unknown
        assert eval_res.unknown_name == "unknown_res"
        assert eval_res.wait_time == float("inf")

    def test_evaluate_impossible_resource(self):
        rm = ResourceManager({"gpu": CapacityResource("gpu", 4.0)})
        eval_res = rm.evaluate("task", {"gpu": 8.0})
        assert not eval_res.is_available
        assert eval_res.is_impossible
        assert eval_res.impossible_name == "gpu"
        assert eval_res.wait_time == float("inf")

    def test_evaluate_available_and_waiting(self):
        rm = ResourceManager({"gpu": CapacityResource("gpu", 4.0)})
        # 可用
        eval_res = rm.evaluate("task", {"gpu": 2.0})
        assert eval_res.is_available
        assert eval_res.wait_time == 0.0

        # 占用全部容量后变为等待
        rm["gpu"].acquire(4.0)
        eval_res2 = rm.evaluate("task", {"gpu": 2.0})
        assert not eval_res2.is_available
        assert not eval_res2.is_unknown
        assert not eval_res2.is_impossible
        assert eval_res2.wait_time > 0.0


class TestResourceManagerTransactionalAcquireAndRelease:
    def test_acquire_effective_success_and_release(self):
        gpu = CapacityResource("gpu", 10.0)
        rm = ResourceManager({"gpu": gpu})
        acquired = rm.acquire_effective("task", {"gpu": 4.0})
        assert acquired == [("gpu", 4.0)]
        assert gpu.used == 4.0

        rm.release_all(acquired)
        assert gpu.used == 0.0

    def test_acquire_effective_rollback_on_failure(self):
        gpu = CapacityResource("gpu", 4.0)
        cpu = CapacityResource("cpu", 2.0)
        rm = ResourceManager({"gpu": gpu, "cpu": cpu})

        # cpu 仅有 2.0，申请 5.0 会在第二项抛出异常
        with pytest.raises(ValueError):
            rm.acquire_effective("task", {"gpu": 2.0, "cpu": 5.0})

        # 已获取的 gpu 必须被自动回滚释放
        assert gpu.used == 0.0
        assert cpu.used == 0.0


class TestResourceManagerSuspensionAndWorker:
    def test_suspend_and_restore(self):
        gpu = CapacityResource("gpu", 10.0)
        rm = ResourceManager({"gpu": gpu})

        assert rm.suspend_resource("gpu", 30.0)
        assert not rm.suspend_resource("unknown", 30.0)

        now_mono = time.monotonic()
        now_wall = time.time()
        gpu.suspend_until = now_mono + 20.0
        suspensions = rm.collect_suspensions(now_mono=now_mono, now_wall=now_wall)
        assert "gpu" in suspensions
        assert math.isclose(suspensions["gpu"], now_wall + 20.0, abs_tol=1e-1)

        # 模拟在新实例上恢复
        gpu2 = CapacityResource("gpu", 10.0)
        rm2 = ResourceManager({"gpu": gpu2})
        rm2.restore_suspensions(suspensions, now_wall=now_wall)
        assert gpu2.suspend_until > time.monotonic()

    def test_can_acquire_worker(self):
        worker = CapacityResource(WORKER_RESOURCE, 2.0)
        rm = ResourceManager({WORKER_RESOURCE: worker})
        ok, wait = rm.can_acquire_worker(1.0)
        assert ok
        assert wait == 0.0

        worker.acquire(2.0)
        ok2, wait2 = rm.can_acquire_worker(1.0)
        assert not ok2
        assert wait2 > 0.0


class TestResourceLeaseAndReservation:
    def test_two_phase_lease_capacity_and_rate_limit(self):
        gpu = CapacityResource("gpu", 4.0)
        api = RateLimitResource("api", interval_seconds=10.0)
        rm = ResourceManager({"gpu": gpu, "api": api})

        # 1. 预约阶段：gpu 立即扣减，api 暂不推进 next_available
        t_before = api.next_available
        lease = rm.reserve("task", {"gpu": 2.0, "api": 1.0}, uid="j1")
        assert lease.status.value == "reserved"
        assert gpu.used == 2.0
        assert api.next_available == t_before

        # 2. 兑现阶段：api 时间片推进
        lease.claim()
        assert lease.status.value == "claimed"
        assert api.next_available > t_before

        # 3. 释放阶段：gpu 容量归还
        lease.release()
        assert lease.status.value == "released"
        assert gpu.used == 0.0

        # 幂等释放
        lease.release()
        assert gpu.used == 0.0

    def test_try_reserve_returns_none_when_unavailable(self):
        gpu = CapacityResource("gpu", 2.0)
        rm = ResourceManager({"gpu": gpu})

        lease1 = rm.try_reserve("task", {"gpu": 2.0})
        assert lease1 is not None
        assert gpu.used == 2.0

        # 容量耗尽，try_reserve 返回 None，不抛出异常
        lease2 = rm.try_reserve("task", {"gpu": 1.0})
        assert lease2 is None
        assert gpu.used == 2.0

        lease1.release()
        assert gpu.used == 0.0

    def test_lease_context_manager_auto_rollback_on_exception(self):
        gpu = CapacityResource("gpu", 4.0)
        rm = ResourceManager({"gpu": gpu})

        with pytest.raises(RuntimeError):
            with rm.reserve("task", {"gpu": 3.0}, uid="err_job"):
                assert gpu.used == 3.0
                raise RuntimeError("Dispatch failed")

        # 退出上下文后应自动 release
        assert gpu.used == 0.0
