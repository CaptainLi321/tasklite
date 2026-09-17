"""ExecutionChannel 子进程执行通道深模块单元测试套件。"""

import json
import time
from pathlib import Path
import pytest

from tasklite.engine.channel import (
    ArtifactCleanupMode,
    ExecutionChannel,
    ExecutionResult,
    JobHandle,
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

        h1 = JobHandle(uid=job1.uid, process=MockProcess(), deadline=time.monotonic() + 10, timeout=10, job=job1, ipc_dir=str(tmp_path), incarnation="inc1")
        h2 = JobHandle(uid=job2.uid, process=MockProcess(), deadline=time.monotonic() + 10, timeout=10, job=job2, ipc_dir=str(tmp_path), incarnation="inc2")

        outcome = channel.abort_in_flight([h1, h2])
        assert len(outcome.completed) == 1
        assert outcome.completed[0][0].uid == job1.uid
        assert outcome.completed[0][1].success is True
        assert outcome.completed[0][1].result_meta == {"done": 1}

        assert len(outcome.cancelled) == 1
        assert outcome.cancelled[0].uid == job2.uid

    def test_abort_salvages_signal_written_before_kill(self, tmp_path):
        """杀进程后补排空：「先排空后杀」窗口内终前写入的 suspend 信号捞回。

        受控注入：stub 进程在 kill() 时向信号文件追加一条信号（模拟 worker
        在上层预排空之后、被终止之前落盘的 suspend）。abort 的最终排空必须
        把它带回 AbortOutcome.salvaged_signals，且不得被随后的半成品清理
        一并删除（旧实现 cancelled 分支直接清 IPC 文件，信号静默丢失）。
        """
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "j9")

        class KillWritingProcess:
            """kill() 时写入 suspend 信号的 stub：确定性复现终前写窗口。"""

            def __init__(self):
                self._alive = True

            def is_alive(self):
                return self._alive

            def kill(self):
                self._alive = False
                channel.journal.record_signal(job.uid, "gpu", 30.0)

            def join(self, timeout=None):
                pass

        h = JobHandle(
            uid=job.uid, process=KillWritingProcess(),
            deadline=time.monotonic() + 10, timeout=10, job=job,
            ipc_dir=str(tmp_path), incarnation="inc9",
        )

        outcome = channel.abort_in_flight([h])

        assert [h.uid for h in outcome.cancelled] == [job.uid]
        assert outcome.salvaged_signals == [(job.uid, "gpu", 30.0)]
        # 信号已被消费带走，半成品清理照常执行（不留残留文件）
        assert not channel.journal.signals_path(job.uid).exists()

    def test_abort_reprobe_completed_merges_post_drain_signals(self, tmp_path):
        """重探测完成分支：终前写入的结果与信号都被消费。

        stub 进程在 kill() 时同时写结果文件与 suspend 信号（worker 终前
        恰好完成的时序）。重探测按结果分类为已完成，最终排空捞回的信号
        并入 ExecutionResult.resource_suspensions（与常规收割路径
        _collect_outcome 的 salvage 行为对齐），随 complete_job 提交链应用。
        """
        import json

        channel = ExecutionChannel(tmp_path)
        job = Job("task", "j10")

        class FinishOnKillProcess:
            """kill() 时写最终结果 + suspend 信号的 stub。"""

            def __init__(self):
                self._alive = True

            def is_alive(self):
                return self._alive

            def kill(self):
                self._alive = False
                channel.journal.write_result_atomic(
                    job.uid,
                    {"status": "success", "raw_result": {"done": 1}},
                    incarnation="inc10",
                )
                channel.journal.record_signal(job.uid, "api", 5.0)

            def join(self, timeout=None):
                pass

        h = JobHandle(
            uid=job.uid, process=FinishOnKillProcess(),
            deadline=time.monotonic() + 10, timeout=10, job=job,
            ipc_dir=str(tmp_path), incarnation="inc10",
        )

        outcome = channel.abort_in_flight([h])

        assert [h.uid for h, _ in outcome.completed] == [job.uid]
        assert outcome.completed[0][1].success is True
        assert outcome.completed[0][1].resource_suspensions == [("api", 5.0)]
        assert outcome.salvaged_signals == []

