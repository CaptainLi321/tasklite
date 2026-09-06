"""Unit tests for ExecutionChannel deep module."""

from __future__ import annotations

import multiprocessing as mp
import time
from pathlib import Path
import pytest

from tasklite.engine.channel import (
    ArtifactCleanupMode,
    ExecutionChannel,
    ExecutionHandle,
    ExecutionResult,
)
from tasklite.models.context import TaskContext
from tasklite.models.job import Job
from tasklite.utils.ipc import append_output, append_signal, outputs_path


def dummy_success_handler(job: Job, ctx: TaskContext) -> dict:
    out = ctx.declare_output("/tmp/dummy_out.txt", cleanup_on_fail=True, sandbox=False)
    Path(out).write_text("dummy", encoding="utf-8")
    return {"status": "ok"}


def dummy_error_handler(job: Job, ctx: TaskContext):
    raise ValueError("handler exploded")


def dummy_signal_handler(job: Job, ctx: TaskContext):
    ctx.suspend_resource("rate_limit_api", 15.0)
    return {"status": "suspended"}


class TestExecutionChannelLifecycle:
    """测试 ExecutionChannel 核心生命周期与看门狗。"""

    def test_spawn_and_poll_success(self, tmp_path):
        mp_ctx = mp.get_context("spawn")
        channel = ExecutionChannel(ipc_dir=tmp_path, mp_ctx=mp_ctx)

        job = Job("test", "1")
        ctx = TaskContext(
            job=job,
            wall_keys=set(),
            failed_keys=set(),
            cursors={},
            ipc_dir=str(tmp_path),
            incarnation="run1.1",
        )

        handle = channel.spawn(dummy_success_handler, job, ctx, timeout=5.0)
        assert isinstance(handle, ExecutionHandle)
        assert handle.uid == "test::1"

        # 轮询直到完成
        completed = []
        for _ in range(50):
            completed = channel.poll_completed([handle])
            if completed:
                break
            time.sleep(0.05)

        assert len(completed) == 1
        h, res = completed[0]
        assert h.uid == "test::1"
        assert res.success is True

    def test_spawn_and_poll_failure(self, tmp_path):
        mp_ctx = mp.get_context("spawn")
        channel = ExecutionChannel(ipc_dir=tmp_path, mp_ctx=mp_ctx)

        job = Job("test", "2")
        ctx = TaskContext(
            job=job,
            wall_keys=set(),
            failed_keys=set(),
            cursors={},
            ipc_dir=str(tmp_path),
            incarnation="run1.1",
        )

        handle = channel.spawn(dummy_error_handler, job, ctx, timeout=5.0)
        completed = []
        for _ in range(50):
            completed = channel.poll_completed([handle])
            if completed:
                break
            time.sleep(0.05)

        assert len(completed) == 1
        _, res = completed[0]
        assert res.success is False

    def test_probe_orphan_lock(self, tmp_path):
        channel = ExecutionChannel(ipc_dir=tmp_path)
        assert channel.probe_orphan_lock("orphan::1") is True

    def test_drain_signals(self, tmp_path):
        channel = ExecutionChannel(ipc_dir=tmp_path)
        append_signal(str(tmp_path), "job::1", "api_limit", 30.0)

        signals = channel.drain_active_signals(["job::1"])
        assert len(signals) == 1
        assert signals[0] == ("job::1", "api_limit", 30.0)


class TestExecutionChannelArtifactCleanup:
    """测试产物与声明生命周期清理。"""

    def test_cleanup_pre_submit(self, tmp_path):
        channel = ExecutionChannel(ipc_dir=tmp_path)
        p = outputs_path(str(tmp_path), "job::1")
        p.write_text("{}", encoding="utf-8")
        assert p.exists()

        channel.cleanup_artifacts("job::1", mode=ArtifactCleanupMode.PRE_SUBMIT)
        assert not p.exists()

    def test_cleanup_success_preserves_output_cleans_cache(self, tmp_path):
        channel = ExecutionChannel(ipc_dir=tmp_path)

        out_file = tmp_path / "final.txt"
        out_file.write_text("data", encoding="utf-8")
        cache_file = tmp_path / "temp.part"
        cache_file.write_text("part", encoding="utf-8")

        append_output(str(tmp_path), "job::1", str(out_file), cleanup=False, kind="output")
        append_output(str(tmp_path), "job::1", str(cache_file), cleanup=True, kind="cache")

        channel.cleanup_artifacts("job::1", mode=ArtifactCleanupMode.SUCCESS)

        # final output 保留，cache 临时文件与 IPC 清理
        assert out_file.exists()
        assert not cache_file.exists()
        assert not outputs_path(str(tmp_path), "job::1").exists()

    def test_cleanup_failure_removes_cleanup_targets(self, tmp_path):
        channel = ExecutionChannel(ipc_dir=tmp_path)

        broken_out = tmp_path / "broken.txt"
        broken_out.write_text("incomplete", encoding="utf-8")

        append_output(str(tmp_path), "job::1", str(broken_out), cleanup=True, kind="output")

        channel.cleanup_artifacts("job::1", mode=ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert not broken_out.exists()
        assert not outputs_path(str(tmp_path), "job::1").exists()
