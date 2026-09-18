"""Unit tests for DispatchMachine dispatch_next deep seam."""

from __future__ import annotations

import types

import pytest

from tasklite.engine.dispatch import DispatchOutcome
from tests.machines import make_machines
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


def test_dispatch_next_empty_queue(tmp_path):
    env = make_machines(tmp_path, capacity=1)

    outcome = env.dispatch.dispatch_next()
    assert outcome.entry is None
    assert outcome.should_continue is False
    assert outcome.standstill.min_wait == float("inf")


def test_dispatch_next_workers_exhausted(tmp_path):
    env = make_machines(tmp_path, capacity=0)

    outcome = env.dispatch.dispatch_next()
    assert outcome.entry is None
    assert outcome.should_continue is False
    assert outcome.worker_wait > 0


def test_dispatch_preflight_dedup_skip(tmp_path):
    env = make_machines(tmp_path)

    env.store.apply_success("demo::1", {"done": True})
    job_dict = {"task_type": "demo", "job_id": "1", "payload": {}}

    handled = env.dispatch.dispatch_dedup(env.store, "demo::1", job_dict)
    assert handled is True
    assert env.stats["skipped"] == 1


def test_dispatch_preflight_no_handler(tmp_path):
    env = make_machines(tmp_path)

    job_dict = {"task_type": "unregistered", "job_id": "1", "payload": {}}
    handled = env.dispatch.dispatch_no_handler("unregistered::1", job_dict, "unregistered")
    assert handled is True
    assert env.stats["failed"] == 1
    assert "unregistered::1" in env.store.failed
    assert env.store.failed["unregistered::1"]["error"] == ERR_NO_HANDLER


def test_dispatch_preflight_dep_failed(tmp_path):
    env = make_machines(tmp_path)

    job_dict = {"task_type": "child", "job_id": "1", "payload": {}, "depends_on": ["parent::1"]}
    handled = env.dispatch.dispatch_dep_failed("child::1", job_dict, "parent::1")
    assert handled is True
    assert env.stats["cascade_failed"] == 1
    assert "child::1" in env.store.failed
    assert env.store.failed["child::1"]["error"] == ERR_JOB_DEPENDENCY


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
    env = make_machines(
        tmp_path,
        extra_resources={"api": rate_limit},
        handlers={"demo": lambda job, c: True},
    )

    job = Job("demo", "1", resources={"api": 1.0})
    env.store.requeue_jobs([job.to_dict()])
    # 评估时点资源可用（调度器据此判可运行），随后挂起信号在预约前被排空应用
    assert env.dispatch._resources.evaluate("demo", {"api": 1.0}).is_available
    monkeypatch.setattr(
        env.dispatch._channel,
        "drain_active_signals",
        lambda uids: [("demo::1", "api", 30.0)],
    )

    sched = env.dispatch._scheduler.pop_next_runnable(env.store, env.store.in_flight_uids)
    assert sched.runnable_idx is not None
    entry = env.dispatch.dispatch_job(sched)

    assert entry is None
    assert "demo::1" in env.store.queue_uids, "瞬态 defer 后作业必须降级写盘回队"
    rt = JobRuntimeState.from_dict(env.store.state.queue[0].get("runtime"))
    assert rt.dispatch_failures == 0, "限流二次检查失败不得计入派发失败 3-strike"
    assert rt.last_retry_error == "", "瞬态信号不得污染 last_retry_error"
    assert 0.0 < rt.remaining_backoff(time_mod.monotonic()) <= 1.0, \
        "瞬态 defer 必须走短退避（限流等待由资源挂起 TTL 承担）"
    assert env.stats["rate_limited_reruns"] == 1
