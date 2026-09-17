"""动态 spawn 子作业 JSON 序列化预检回归测试。

handler 返回的 new_jobs 经完成机器动态 spawn 管道入队。payload 不可 JSON
序列化的坏子作业必须在提交前被预检拦截并独立登记失败终态（进 DLQ），
父作业的成功提交不受连坐——否则 SQLite 后端落盘 ``dumps`` 时失败返回
False，已成功的父作业被推入 3-strike 崩溃契约（整 run 崩溃重启、handler
副作用重复后误标 ``ERR_COMMIT_FAILURE_DLQ``）；Memory 后端则静默接受坏
payload 到队列，延迟到派发阶段才失败（双后端行为分歧）。
"""

from __future__ import annotations

import types

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.channel import ExecutionChannel, ExecutionResult
from tasklite.engine.completion import CompletionMachine
from tasklite.engine.inflight import InFlightJob, InFlightTracker
from tasklite.engine.policy import ExecutionPolicy
from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.engine.session import RunSession
from tasklite.engine.store import StateStore
from tasklite.engine.types import TaskStats
from tasklite.models.job import Job
from tasklite.models.state import PipelineState
from tasklite.taxonomy import ErrorTaxonomy


def _make_machine(tmp_path, backend):
    """显式装配 CompletionMachine 的窄依赖集合。"""
    gpu = CapacityResource("gpu", 10.0)
    rm = ResourceManager({"gpu": gpu})
    channel = ExecutionChannel(str(tmp_path / "ipc"))
    stats = TaskStats()
    policy = ExecutionPolicy()
    store = StateStore(
        backend,
        state=PipelineState({}, {}, {}, []),
        taxonomy=ErrorTaxonomy(),
        stats=stats,
        policy=policy,
    )
    session = RunSession()
    session.run_id = "test_run"
    in_flight = InFlightTracker()
    completion = CompletionMachine(
        store=store,
        policy=policy,
        channel=channel,
        resources=rm,
        in_flight=in_flight,
        session=session,
    )
    ctx = types.SimpleNamespace(
        store=store, stats=stats, resource_mgr=rm, in_flight=in_flight,
        channel=channel, session=session,
    )
    return ctx, completion


@pytest.mark.parametrize(
    "backend_factory",
    [
        lambda tmp_path: InMemoryStateBackend(),
        lambda tmp_path: SQLiteStateBackend(tmp_path / "state.db"),
    ],
    ids=["memory", "sqlite"],
)
class TestSpawnedJobSerializationPrecheck:
    def test_unserializable_child_isolated_to_dlq_parent_still_succeeds(
        self, tmp_path, backend_factory
    ):
        ctx, completion = _make_machine(tmp_path, backend_factory(tmp_path))

        lease = ctx.resource_mgr.reserve("t", {"gpu": 1.0}, uid="t::parent")
        job = Job("t", "parent", resources={"gpu": 1.0})
        entry = InFlightJob(
            uid="t::parent", job_dict=job.to_dict(), job=job, lease=lease
        )
        ctx.in_flight.track(entry, state=ctx.store)

        result = ExecutionResult(
            success=True,
            result_meta={"v": 1},
            new_jobs=[
                Job("t", "bad", payload={"k": {1, 2}}),  # set：不可 JSON 序列化
                Job("t", "good", payload={"k": "ok"}),
            ],
        )

        # 修复前：SQLite 在此抛 _CommitCrashSignal（父作业被 requeue + 崩溃）
        completion.complete_job(entry, result)

        # 父作业照常成功提交，不被坏子作业连坐；资源租约正常归还
        assert "t::parent" in ctx.store.wall
        assert ctx.resource_mgr["gpu"].used == 0.0

        # 坏子作业独立登记失败终态（DLQ），不滞留队列
        assert "t::bad" in ctx.store.failed
        assert "t::bad" not in ctx.store.queue_uids
        assert "not JSON-serializable" in ctx.store.failed["t::bad"]["error"]

        # 好子作业正常入队
        assert "t::good" in ctx.store.queue_uids


class TestRejectedSpawnedJobSingleExit:
    def test_completion_hook_fires_exactly_once_per_job(self, tmp_path):
        """拒绝路径与正常路径共用 complete_job 单一出口。

        父作业（成功）与坏子作业（INVALID_SPAWNED_JOB 终结）各恰好
        触发一次 on_job_completed，success 标志与实际终态一致——
        「每个 job 终结时钩子恰好调用一次」是完成事件的明文承诺。
        """
        events: list = []

        def hook(uid, meta, success, going_to_retry):
            events.append((uid, success, going_to_retry))

        gpu = CapacityResource("gpu", 10.0)
        rm = ResourceManager({"gpu": gpu})
        channel = ExecutionChannel(str(tmp_path / "ipc"))
        stats = TaskStats()
        policy = ExecutionPolicy()
        backend = InMemoryStateBackend()
        store = StateStore(
            backend,
            state=PipelineState({}, {}, {}, []),
            taxonomy=ErrorTaxonomy(),
            stats=stats,
            policy=policy,
        )
        session = RunSession(on_job_completed=hook)
        session.run_id = "test_run"
        completion = CompletionMachine(
            store=store,
            policy=policy,
            channel=channel,
            resources=rm,
            in_flight=InFlightTracker(),
            session=session,
        )

        lease = rm.reserve("t", {"gpu": 1.0}, uid="t::parent")
        job = Job("t", "parent", resources={"gpu": 1.0})
        entry = InFlightJob(uid="t::parent", job_dict=job.to_dict(), job=job, lease=lease)
        completion.complete_job(
            entry,
            ExecutionResult(
                success=True,
                result_meta={"v": 1},
                new_jobs=[
                    Job("t", "bad", payload={"k": {1, 2}}),
                ],
            ),
        )

        assert events == [("t::bad", False, False), ("t::parent", True, False)]
