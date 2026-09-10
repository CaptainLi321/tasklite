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


def make_env(tmp_path, capacity=2):
    """显式装配 DispatchMachine 的窄依赖集合。"""
    backend = InMemoryStateBackend()
    resources = {"__workers__": CapacityResource("__workers__", capacity)}
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
        handlers={},
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
    assert outcome.sched is not None
    assert outcome.sched.runnable_idx is None


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
