"""Unit tests for DispatchMachine dispatch_next deep seam."""

from __future__ import annotations

import pytest

from tasklite.engine.dispatch import DispatchMachine, DispatchOutcome
from tasklite.engine.runtime import RunContext
from tasklite.engine.completion import CompletionMachine
from tasklite.engine.scheduler import JobScheduler
from tasklite.engine.resource import ResourceManager, CapacityResource
from tasklite.engine.channel import ExecutionChannel
from tasklite.backend.memory import InMemoryStateBackend


def test_dispatch_next_empty_queue(tmp_path):
    backend = InMemoryStateBackend()
    resources = {"__workers__": CapacityResource("__workers__", 1)}
    resource_mgr = ResourceManager(resources)
    scheduler = JobScheduler(resource_mgr)
    channel = ExecutionChannel(tmp_path / "ipc")

    ctx = RunContext(
        name="test_pipe",
        backend=backend,
        scheduler=scheduler,
        resources=resources,
        handlers={},
        executor=channel.executor,
        ipc_dir=str(tmp_path / "ipc"),
        output_root=str(tmp_path / "out"),
    )
    completion = CompletionMachine(ctx)
    dispatch = DispatchMachine(ctx, completion)

    outcome = dispatch.dispatch_next()
    assert outcome.entry is None
    assert outcome.should_continue is False
    assert outcome.sched is not None
    assert outcome.sched.runnable_idx is None


def test_dispatch_next_workers_exhausted(tmp_path):
    backend = InMemoryStateBackend()
    resources = {"__workers__": CapacityResource("__workers__", 0)}
    resource_mgr = ResourceManager(resources)
    scheduler = JobScheduler(resource_mgr)
    channel = ExecutionChannel(tmp_path / "ipc")

    ctx = RunContext(
        name="test_pipe",
        backend=backend,
        scheduler=scheduler,
        resources=resources,
        handlers={},
        executor=channel.executor,
        ipc_dir=str(tmp_path / "ipc"),
        output_root=str(tmp_path / "out"),
    )
    completion = CompletionMachine(ctx)
    dispatch = DispatchMachine(ctx, completion)

    outcome = dispatch.dispatch_next()
    assert outcome.entry is None
    assert outcome.should_continue is False
    assert outcome.worker_wait > 0
