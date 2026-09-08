"""ExecutionChannel 子进程执行通道深模块单元测试套件。"""

import json
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


class TestExecutionChannelBasics:
    def test_init_and_directory_creation(self, tmp_path):
        ipc_dir = tmp_path / "ipc"
        assert not ipc_dir.exists()
        channel = ExecutionChannel(ipc_dir)
        assert ipc_dir.exists()
        assert channel.ipc_dir == str(ipc_dir)

    def test_probe_orphan_lock(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        # 无锁占用时 probe 返回 True
        assert channel.probe_orphan_lock("job_1") is True

    def test_drain_active_signals(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        channel.journal.record_signal("job_1", "gpu", 15.0)
        channel.journal.record_signal("job_1", "api", 3.0)

        signals = channel.drain_active_signals(["job_1", "job_2"])
        assert ("job_1", "gpu", 15.0) in signals
        assert ("job_1", "api", 3.0) in signals

    def test_read_declared_inputs(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        channel.journal.record_input_entry("job_1", {"kind": "file", "path": "/tmp/a.txt", "size": 100, "mtime_ns": 1000})
        inputs = channel.read_declared_inputs("job_1")
        assert len(inputs) == 1
        assert inputs[0]["path"] == "/tmp/a.txt"
        assert inputs[0]["size"] == 100


class TestExecutionChannelArtifactCleanup:
    def test_cleanup_pre_submit(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        uid = "job_clean"
        channel.journal.record_input_entry(uid, {"kind": "file", "path": "/tmp/x"})
        channel.journal.record_output(uid, "/tmp/y", False, kind="file")
        channel.journal.record_signal(uid, "gpu", 10.0)

        assert channel.journal.inputs_path(uid).exists()
        assert channel.journal.outputs_path(uid).exists()
        assert channel.journal.signals_path(uid).exists()

        channel.cleanup_artifacts(uid, mode=ArtifactCleanupMode.PRE_SUBMIT)

        assert not channel.journal.inputs_path(uid).exists()
        assert not channel.journal.outputs_path(uid).exists()
        assert not channel.journal.signals_path(uid).exists()

    def test_cleanup_success_deletes_cache_files_and_declarations(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        uid = "job_success"
        cache_file = tmp_path / "cache.tmp"
        cache_file.write_text("temporary data")
        output_file = tmp_path / "final.txt"
        output_file.write_text("final data")

        channel.journal.record_output(uid, str(cache_file), False, kind="cache")
        channel.journal.record_output(uid, str(output_file), True, kind="file")

        channel.cleanup_artifacts(uid, mode=ArtifactCleanupMode.SUCCESS)

        # cache 临时文件被删除，正式产物被保留，声明文件被删除
        assert not cache_file.exists()
        assert output_file.exists()
        assert not channel.journal.outputs_path(uid).exists()

    def test_cleanup_failure_deletes_cleanup_on_fail_outputs(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        uid = "job_fail"
        fail_file = tmp_path / "temp_product.txt"
        fail_file.write_text("to delete")
        keep_file = tmp_path / "keep.txt"
        keep_file.write_text("to keep")

        channel.journal.record_output(uid, str(fail_file), True, kind="file")
        channel.journal.record_output(uid, str(keep_file), False, kind="file")

        channel.cleanup_artifacts(uid, mode=ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert not fail_file.exists()
        assert keep_file.exists()
        assert not channel.journal.outputs_path(uid).exists()


class TestExecutionChannelStaleResultClaim:
    def test_claim_stale_result(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "j1", payload={"data": 123})
        res_file = channel.journal.result_path(job.uid, f"{'a' * 32}.1")
        res_data = {
            "status": "success",
            "raw_result": {"status": "ok"},
            "new_jobs": [],
            "resource_suspensions": [],
            "cursor_updates": {},
        }
        res_file.write_text(json.dumps(res_data))

        result = channel.claim_stale_result(job.uid, job)
        assert result is not None
        assert result.success is True
        assert result.result_meta == {"status": "ok"}


class TestExecutionChannelAbortInFlight:
    def test_abort_with_completed_and_cancelled(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        job1 = Job("task", "j1")
        job2 = Job("task", "j2")

        # j1 has written result
        res1 = channel.journal.result_path(job1.uid, "inc1")
        res1.write_text(json.dumps({"status": "success", "raw_result": {"done": 1}}))

        # create mock handles
        class MockProcess:
            def is_alive(self):
                return False
            def kill(self):
                pass
            def join(self, timeout=None):
                pass

        h1 = ExecutionHandle(uid=job1.uid, process=MockProcess(), deadline=time.monotonic() + 10, timeout=10, job=job1, ipc_dir=str(tmp_path), incarnation="inc1")
        h2 = ExecutionHandle(uid=job2.uid, process=MockProcess(), deadline=time.monotonic() + 10, timeout=10, job=job2, ipc_dir=str(tmp_path), incarnation="inc2")

        outcome = channel.abort_in_flight([h1, h2])
        assert len(outcome.completed) == 1
        assert outcome.completed[0][0].uid == job1.uid
        assert outcome.completed[0][1].success is True
        assert outcome.completed[0][1].result_meta == {"done": 1}

        assert len(outcome.cancelled) == 1
        assert outcome.cancelled[0].uid == job2.uid

