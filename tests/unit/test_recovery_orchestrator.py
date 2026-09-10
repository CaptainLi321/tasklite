"""RecoveryOrchestrator 故障恢复编排深模块单元测试套件。"""

import math
import time
from unittest.mock import MagicMock
import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.policy import PreflightPolicy
from tasklite.engine.recovery import RecoveryOrchestrator, RecoveryMachine
from tasklite.engine.resource import (
    META_RESOURCE_SUSPENDS,
    RateLimitResource,
    ResourceManager,
)
from tasklite.engine.runtime import (
    RT_BACKOFF_UNTIL,
    RT_BACKOFF_WALL_DEADLINE,
)
from tasklite.engine.store import StateStore
from tasklite.models.job import Job
from tasklite.models.state import PipelineState


def make_orchestrator(backend, state=None, resource_mgr=None):
    """显式装配 RecoveryOrchestrator 的窄依赖集合。"""
    state = state or PipelineState({}, {}, {}, [])
    return RecoveryOrchestrator(
        store=StateStore(backend, state=state),
        backend=backend,
        channel=MagicMock(),
        resources=resource_mgr or ResourceManager(),
        in_flight=MagicMock(),
        policy=PreflightPolicy(),
        completion=MagicMock(),
    )


class TestRecoveryOrchestratorQueueRepair:
    def test_repair_queue_backoff_conversion(self):
        backend = InMemoryStateBackend()
        orchestrator = make_orchestrator(backend)

        now_wall = time.time()
        # 1. 过去已过期截止时间 -> 应当被清除
        expired_job = {"task_type": "t", "job_id": "j1", "runtime": {RT_BACKOFF_WALL_DEADLINE: now_wall - 10.0}}
        # 2. 未来截止时间 -> 应当被换算为 _backoff_until
        future_job = {"task_type": "t", "job_id": "j2", "runtime": {RT_BACKOFF_WALL_DEADLINE: now_wall + 5.0}}
        # 3. 重复 UID 作业 -> 应当保留第一条
        duplicate_job = {"task_type": "t", "job_id": "j1", "runtime": {}}

        q_data = [expired_job, future_job, duplicate_job]
        repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})

        assert len(repaired) == 2
        assert RT_BACKOFF_WALL_DEADLINE not in repaired[0]["runtime"]
        assert RT_BACKOFF_UNTIL not in repaired[0]["runtime"]

        assert RT_BACKOFF_UNTIL in repaired[1]["runtime"]
        assert repaired[1]["runtime"][RT_BACKOFF_UNTIL] > time.monotonic()

    def test_repair_queue_filters_wall_and_failed_unless_rerun(self):
        backend = InMemoryStateBackend()
        orchestrator = make_orchestrator(backend)

        # 在 wall 中且 rerun="never" -> 过滤
        job_wall = {"task_type": "t", "job_id": "w1", "rerun": "never"}
        # 在 wall 中但 rerun="every_run" -> 保留
        job_every_run = {"task_type": "t", "job_id": "w2", "rerun": "every_run"}
        # 新作业 -> 保留
        job_fresh = {"task_type": "t", "job_id": "f1"}

        wall = {"t::w1": {}, "t::w2": {}}
        repaired = orchestrator.repair_queue_on_load(
            [job_wall, job_every_run, job_fresh], wall=wall, failed={}
        )

        uids = [j["task_type"] + "::" + j["job_id"] for j in repaired]
        assert "t::w1" not in uids
        assert "t::w2" in uids
        assert "t::f1" in uids


class TestRecoveryOrchestratorCrashSafeSave:
    def test_crash_safe_save_recovers_missing_jobs_from_disk(self):
        backend = InMemoryStateBackend()
        # 磁盘上有 j1, j2
        backend.save_queue([
            {"task_type": "t", "job_id": "j1"},
            {"task_type": "t", "job_id": "j2"},
        ])

        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "j2"}])

        orchestrator = make_orchestrator(backend, state=state)

        orchestrator.save_queue_crash_safe()

        disk_q = backend.load_queue()
        disk_uids = [j["task_type"] + "::" + j["job_id"] for j in disk_q]
        # j1 必须被补回队首
        assert disk_uids == ["t::j1", "t::j2"]

    def test_crash_safe_save_skips_overwrite_if_disk_load_fails(self):
        backend = InMemoryStateBackend()
        backend.save_queue([{"task_type": "t", "job_id": "safe_on_disk"}])

        # 模拟磁盘读取异常
        backend.load_queue = MagicMock(side_effect=IOError("Disk corruption"))

        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "in_mem"}])
        orchestrator = make_orchestrator(backend, state=state)

        # 不应抛出异常，也不应覆盖磁盘
        orchestrator.save_queue_crash_safe()


class TestRecoveryOrchestratorSuspendPersistence:
    def test_apply_pending_signals_persists_suspensions(self):
        backend = InMemoryStateBackend()
        rm = ResourceManager({"api": RateLimitResource("api", 1.0)})
        in_flight = MagicMock()
        in_flight.active_uids.return_value = ["t::j1"]
        channel = MagicMock()
        # channel.drain_active_signals 返回一条挂起信号
        channel.drain_active_signals.return_value = [("t::j1", "api", 30.0)]

        orchestrator = RecoveryOrchestrator(
            store=StateStore(backend),
            backend=backend,
            channel=channel,
            resources=rm,
            in_flight=in_flight,
            policy=PreflightPolicy(),
            completion=MagicMock(),
        )

        orchestrator.apply_pending_signals()

        assert backend.get_meta(META_RESOURCE_SUSPENDS) is not None


class TestRecoveryOrchestratorCompatibility:
    def test_alias_equivalence(self):
        assert RecoveryMachine is RecoveryOrchestrator
