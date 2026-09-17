"""Unit tests for DispatchMachine dispatch_next deep seam."""

from __future__ import annotations

import types

import pytest

from tasklite.engine.dispatch import DispatchMachine, DispatchOutcome
from tasklite.engine.completion import CompletionMachine
from tasklite.engine.inflight import InFlightTracker
from tasklite.engine.scheduler import JobScheduler
from tasklite.engine.session import RunSession
from tasklite.engine.policy import ExecutionPolicy
from tasklite.engine.resource import ResourceManager, CapacityResource
from tasklite.engine.channel import ExecutionChannel
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.store import StateStore
from tasklite.engine.types import TaskStats
from tasklite.taxonomy import ERR_JOB_DEPENDENCY, ERR_NO_HANDLER, ErrorTaxonomy


def make_env(tmp_path, capacity=2, extra_resources=None, handlers=None):
    """显式装配 DispatchMachine 的窄依赖集合。"""
    backend = InMemoryStateBackend()
    resources = {"__workers__": CapacityResource("__workers__", capacity)}
    if extra_resources:
        resources.update(extra_resources)
    resource_mgr = ResourceManager(resources)
    scheduler = JobScheduler(resource_mgr)
    channel = ExecutionChannel(tmp_path / "ipc")
    taxonomy = ErrorTaxonomy()
    stats = TaskStats()
    policy = ExecutionPolicy()
    store = StateStore(backend, taxonomy=taxonomy, stats=stats, policy=policy)
    session = RunSession()
    session.run_id = "test_run"
    in_flight = InFlightTracker()

    completion = CompletionMachine(
        store=store,
        policy=policy,
        channel=channel,
        resources=resource_mgr,
        in_flight=in_flight,
        session=session,
    )
    dispatch = DispatchMachine(
        store=store,
        scheduler=scheduler,
        policy=policy,
        resources=resource_mgr,
        channel=channel,
        in_flight=in_flight,
        session=session,
        completion=completion,
        handlers=handlers if handlers is not None else {},
        taxonomy=taxonomy,
        output_root=str(tmp_path / "out"),
        ipc_dir=str(tmp_path / "ipc"),
        commit_failure_dlq_threshold=3,
    )
    ctx = types.SimpleNamespace(store=store, stats=stats)
    return ctx, dispatch, completion


def test_dispatch_next_empty_queue(tmp_path):
    ctx, dispatch, _ = make_env(tmp_path, capacity=1)

    outcome = dispatch.dispatch_next()
    assert outcome.entry is None
    assert outcome.should_continue is False
    assert outcome.standstill.min_wait == float("inf")


def test_dispatch_next_workers_exhausted(tmp_path):
    ctx, dispatch, _ = make_env(tmp_path, capacity=0)

    outcome = dispatch.dispatch_next()
    assert outcome.entry is None
    assert outcome.should_continue is False
    assert outcome.worker_wait > 0


def test_dispatch_preflight_dedup_skip(tmp_path):
    ctx, dispatch, _ = make_env(tmp_path)

    ctx.store.apply_success("demo::1", {"done": True})
    job_dict = {"task_type": "demo", "job_id": "1", "payload": {}}

    handled = dispatch.dispatch_dedup(ctx.store, "demo::1", job_dict)
    assert handled is True
    assert ctx.stats["skipped"] == 1


def test_dispatch_preflight_no_handler(tmp_path):
    ctx, dispatch, _ = make_env(tmp_path)

    job_dict = {"task_type": "unregistered", "job_id": "1", "payload": {}}
    handled = dispatch.dispatch_no_handler("unregistered::1", job_dict, "unregistered")
    assert handled is True
    assert ctx.stats["failed"] == 1
    assert "unregistered::1" in ctx.store.failed
    assert ctx.store.failed["unregistered::1"]["error"] == ERR_NO_HANDLER


def test_dispatch_preflight_dep_failed(tmp_path):
    ctx, dispatch, _ = make_env(tmp_path)

    job_dict = {"task_type": "child", "job_id": "1", "payload": {}, "depends_on": ["parent::1"]}
    handled = dispatch.dispatch_dep_failed("child::1", job_dict, "parent::1")
    assert handled is True
    assert ctx.stats["cascade_failed"] == 1
    assert "child::1" in ctx.store.failed
    assert ctx.store.failed["child::1"]["error"] == ERR_JOB_DEPENDENCY


def test_dispatch_job_rate_limit_recheck_defers_transiently(tmp_path, monkeypatch):
    """调度评估与 reserve 预约之间应用限流挂起时，二次检查失败按瞬态信号 defer。

    残留挂起信号排空恰落在调度器 evaluate 之后、reserve 预约之前——
    二次检查失败表达的是「稍后自动恢复的限流等待」：必须零预算短退避
    回队（实际等待由资源挂起 TTL 承担），不得计入派发失败 3-strike，
    更不得上抛击穿事件泵杀掉整个 run。
    """
    import time as time_mod

    from tasklite.engine.resource import RateLimitResource
    from tasklite.models.job import Job, JobRuntimeState

    rate_limit = RateLimitResource("api", 3600.0)
    ctx, dispatch, _ = make_env(
        tmp_path,
        extra_resources={"api": rate_limit},
        handlers={"demo": lambda job, c: True},
    )

    job = Job("demo", "1", resources={"api": 1.0})
    ctx.store.requeue_jobs([job.to_dict()])
    # 评估时点资源可用（调度器据此判可运行），随后挂起信号在预约前被排空应用
    assert dispatch._resources.evaluate("demo", {"api": 1.0}).is_available
    monkeypatch.setattr(
        dispatch._channel,
        "drain_active_signals",
        lambda uids: [("demo::1", "api", 30.0)],
    )

    sched = dispatch._scheduler.pop_next_runnable(ctx.store, ctx.store.in_flight_uids)
    assert sched.runnable_idx is not None
    entry = dispatch.dispatch_job(sched)

    assert entry is None
    assert "demo::1" in ctx.store.queue_uids, "瞬态 defer 后作业必须降级写盘回队"
    rt = JobRuntimeState.from_dict(ctx.store.state.queue[0].get("runtime"))
    assert rt.dispatch_failures == 0, "限流二次检查失败不得计入派发失败 3-strike"
    assert rt.last_retry_error == "", "瞬态信号不得污染 last_retry_error"
    assert 0.0 < rt.remaining_backoff(time_mod.monotonic()) <= 1.0, \
        "瞬态 defer 必须走短退避（限流等待由资源挂起 TTL 承担）"
    assert ctx.stats["rate_limited_reruns"] == 1
