"""Pipeline error handling tests: timeout, fatal, retry, crashes, output, keyboard interrupt."""

import os
import time
import types
import multiprocessing
from pathlib import Path

import pytest

from tasklite.pipeline import TaskLite
from tasklite.models.job import Job
from tasklite.models.state import PipelineState
from tasklite.engine.resource import CapacityResource
from tests.helpers import (
    _write_fake_result,
    FakeManager,
    make_fake_process_class,
    make_ipc_process_class,
    make_pipeline,
    patch_multiprocessing_for_fakes,
    patch_pipeline_manager,
)


class TestErrorHandling:
    """Tests for error handling: timeout, fatal, retry (REQ-4, REQ-11)."""

    def test_timeout_kills_process(self, tmp_path, monkeypatch):
        """Handler hangs (process stays alive) → kill() called → DLQ with TIMEOUT."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("hang", lambda j, c: (True, {}))
        pipeline.enqueue([Job("hang", "j1", payload={}, timeout=1)])

        TimeoutFakeProcess = make_ipc_process_class(stay_alive=True)
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=TimeoutFakeProcess)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "hang::j1" in failed
        assert "TIMEOUT" in str(failed["hang::j1"].get("error", ""))

    def test_fatal_error_direct_dlq(self, tmp_path, monkeypatch):
        """FatalError → direct DLQ without retry."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("fatal_test", lambda j, c: (True, {}))

        job = Job("fatal_test", "j1", payload={}, max_retries=5, retries=0)
        pipeline.enqueue([job])

        FakeP = make_fake_process_class("fatal")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "fatal_test::j1" in failed
        assert "FatalError" in failed["fatal_test::j1"].get("error", "")
        assert failed["fatal_test::j1"].get("fatal")

    def test_fatal_exception_detected(self, tmp_path, monkeypatch):
        """TypeError/KeyError/AttributeError/ValueError → direct DLQ with fatal."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("typeerr", lambda j, c: (True, {}))
        pipeline.enqueue([Job("typeerr", "j1", payload={})])

        FakeP = make_fake_process_class("fatal_exception")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "typeerr::j1" in failed
        assert failed["typeerr::j1"].get("fatal")

    def test_retry_max_exceeded_dlq(self, tmp_path, monkeypatch):
        """RetryError after max_retries → DLQ with MAX_RETRIES_EXCEEDED."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("retry_max", lambda j, c: (True, {}))
        pipeline.enqueue([Job("retry_max", "j1", payload={}, retries=3, max_retries=3)])

        FakeP = make_fake_process_class("retry")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "retry_max::j1" in failed
        assert failed["retry_max::j1"]["error"] == "MAX_RETRIES_EXCEEDED"


class TestOutputVerification:
    """Tests for output file declaration and verification (REQ-8)."""

    def test_declare_output_missing_file_fails(self, tmp_path, monkeypatch):
        """Handler claims output exists but file is missing → job fails."""
        pipeline = make_pipeline(tmp_path)

        fake_file = tmp_path / "output" / "missing.txt"
        fake_file.parent.mkdir(parents=True, exist_ok=True)
        # Do NOT create fake_file — it stays missing

        # Fake Manager that returns regular Python lists
        # FakeManager imported from tests.helpers
        monkeypatch.setattr("multiprocessing.Manager", lambda: FakeManager())

        # FakeProcess that adds the missing file to shared_outputs
        class MissingOutputProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                ctx = self.args[2]
                ctx.declare_output(str(Path(fake_file)), cleanup_on_fail=True)
                _write_fake_result(self.args[3], self.args[1].uid, {
                    "status": "success", "raw_result": True,
                    "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
                }, incarnation=getattr(self.args[2], "incarnation", None))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=MissingOutputProcess)

        pipeline.register_handler("output_test", lambda j, c: None)
        pipeline.enqueue([Job("output_test", "j1", payload={})])
        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "output_test::j1" in failed
        assert "Missing output" in str(failed["output_test::j1"])

    def test_declare_output_cleanup_on_fail(self, tmp_path, monkeypatch):
        """Failure with cleanup_on_fail=True → partially written file deleted."""
        pipeline = make_pipeline(tmp_path)

        cleanup_file = tmp_path / "output" / "cleanup_test.txt"
        cleanup_file.parent.mkdir(parents=True, exist_ok=True)
        cleanup_file.write_text("partial data")

        # FakeManager imported from tests.helpers
        monkeypatch.setattr("multiprocessing.Manager", lambda: FakeManager())

        class FailWithCleanup:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                ctx = self.args[2]
                ctx.declare_output(str(cleanup_file), cleanup_on_fail=True)
                _write_fake_result(self.args[3], self.args[1].uid, {"status": "error", "error": "crash", "traceback": "tb"}, incarnation=getattr(self.args[2], "incarnation", None))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FailWithCleanup)

        pipeline.register_handler("cleanup_test", lambda j, c: None)
        pipeline.enqueue([Job("cleanup_test", "j1", payload={})])
        pipeline.run()

        assert not cleanup_file.exists()


class TestPayloadValidation:
    """Tests for payload schema validation (REQ-9)."""

    def test_payload_validation_before_exec(self, tmp_path, monkeypatch):
        """Invalid payload → DLQ with PAYLOAD_VALIDATION_FAILED before handler runs."""
        from typing import TypedDict

        class MySchema(TypedDict):
            name: str
            age: int

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("validated", lambda j, c: (True, {}), payload_schema=MySchema)

        # Payload missing 'age' field
        pipeline.enqueue([Job("validated", "j1", payload={"name": "test"})])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "validated::j1" in failed
        assert failed["validated::j1"]["error"] == "PAYLOAD_VALIDATION_FAILED"


class TestProcessErrorPaths:
    """Tests for error handling in the run main loop."""

    def test_process_crash_nonzero_exitcode(self, tmp_path, monkeypatch):
        """Child exits with non-zero exitcode (not killed) → DLQ."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("crash", lambda j, c: (True, {}))
        pipeline.enqueue([Job("crash", "j1", payload={})])

        CrashProcess = make_ipc_process_class(exitcode=1)  # non-zero, not killed
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=CrashProcess)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "crash::j1" in failed
        assert "PROCESS_CRASH_EXITCODE_1" in failed["crash::j1"]["error"]

    def test_no_ipc_result_empty_queue(self, tmp_path, monkeypatch):
        """Child exits cleanly (exitcode=0) but Queue empty → DLQ."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("noresult", lambda j, c: (True, {}))
        pipeline.enqueue([Job("noresult", "j1", payload={})])

        NoResultProcess = make_ipc_process_class()  # clean exit, no IPC put
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=NoResultProcess)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "noresult::j1" in failed
        assert "NO_IPC_RESULT" in failed["noresult::j1"]["error"]

    def test_capacity_impossible_request_deadlock(self, tmp_path):
        """Job requests > max_capacity → pipeline detects deadlock, job to DLQ."""
        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("gpu", max_capacity=5.0))
        pipeline.register_handler("heavy", lambda j, c: (True, {}))

        job = Job("heavy", "j1", payload={}, resources={"gpu": 10.0})
        pipeline.enqueue([job])

        pipeline.run()

        # Pipeline breaks without hanging. Queue cleared — job moved to DLQ.
        queue = pipeline.backend.load_queue()
        assert len(queue) == 0

        failed = pipeline.backend.load_failed()
        assert "heavy::j1" in failed
        assert failed["heavy::j1"]["error"] == "RESOURCE_DEADLOCK"

    def test_resource_release_failure_not_fatal(self, tmp_path, monkeypatch):
        """Resource.release() raises → caught by finally block, pipeline continues."""
        from tasklite.engine.resource import CapacityResource

        pipeline = make_pipeline(tmp_path)
        res = CapacityResource("slot", max_capacity=1.0)
        pipeline.add_resource(res)
        pipeline.register_handler("test", lambda j, c: (True, {}),
                                  default_resources={"slot": 1.0})

        # Inject broken release
        original_release = res.release
        res.release = lambda amount: (_ for _ in ()).throw(RuntimeError("release failed"))

        pipeline.enqueue([Job("test", "j1", payload={})])

        FakeP = make_fake_process_class("error")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        # Should not crash
        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "test::j1" in failed

        # Restore for cleanup
        res.release = original_release

    def test_output_cleanup_failure_not_fatal(self, tmp_path, monkeypatch):
        """File unlink during cleanup raises → caught, pipeline continues, job in DLQ."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("cleanup", lambda j, c: (True, {}))

        # FakeManager imported from tests.helpers
        monkeypatch.setattr("multiprocessing.Manager", lambda: FakeManager())

        class FailWithCleanupBrokenUnlink:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                ctx = self.args[2]
                dummy = tmp_path / "output" / "doomed.txt"
                dummy.parent.mkdir(parents=True, exist_ok=True)
                dummy.write_text("temp")
                ctx.declare_output(str(dummy), cleanup_on_fail=True)
                _write_fake_result(self.args[3], self.args[1].uid, {"status": "error", "error": "boom", "traceback": "tb"}, incarnation=getattr(self.args[2], "incarnation", None))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        # Monkeypatch Path.unlink to raise — exercises the except BaseException handler
        original_unlink = Path.unlink
        monkeypatch.setattr(Path, "unlink", lambda self: (_ for _ in ()).throw(OSError("unlink failed")))
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FailWithCleanupBrokenUnlink)

        pipeline.enqueue([Job("cleanup", "j1", payload={})])
        pipeline.run()

        # Job should still go to DLQ (cleanup failure is logged, not fatal)
        failed = pipeline.backend.load_failed()
        assert "cleanup::j1" in failed

        # Restore for other tests
        monkeypatch.setattr(Path, "unlink", original_unlink)


class TestKeyboardInterrupt:
    """KeyboardInterrupt during pipeline execution tests."""

    def test_keyboard_interrupt_saves_queue_sqlite(self, tmp_path, monkeypatch):
        """KeyboardInterrupt during job start → queue saved with all jobs, run() re-raises."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))
        pipeline.enqueue([Job("test", "j1", payload={}), Job("test", "j2", payload={})])

        class InterruptProcess:
            def __init__(self, target=None, args=(), **kwargs): pass
            def start(self): raise KeyboardInterrupt()
            def join(self, timeout=None): pass
            def is_alive(self): return False
            def kill(self): pass

        # FakeManager imported from tests.helpers
        monkeypatch.setattr("multiprocessing.Manager", lambda: FakeManager())
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=InterruptProcess)

        # pipeline uses self._mp_ctx (spawn context). conftest's autouse
        # _patch_mp_context fixture makes get_context return the global mp module,
        # so the monkeypatches above take effect. run saves the queue then
        # re-raises KeyboardInterrupt.
        with pytest.raises(KeyboardInterrupt):
            pipeline.run()

        # Verify queue was saved with all jobs (j1 re-inserted at front).
        queue = pipeline.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in queue]
        assert "test::j1" in uids
        assert "test::j2" in uids


class TestOutputRootFailure:
    """output_root.mkdir failure propagation tests."""

    def test_output_root_mkdir_permission_error_propagates(self, tmp_path):
        """output_root creation fails in read-only parent → propagates."""
        if getattr(os, "geteuid", None) and os.geteuid() == 0:
            pytest.skip("POSIX root user bypasses directory permission checks")
        parent = tmp_path / "noperm"
        parent.mkdir()
        parent.chmod(0o444)  # read-only

        with pytest.raises(PermissionError):
            TaskLite(name="test", state_dir=tmp_path / "state", backend="sqlite",
                         output_root=parent / "output")


class TestKeyboardInterruptDuringSleep:
    """Regression: KeyboardInterrupt during time.sleep should not create duplicate queue entries."""

    def test_no_duplicate_on_interrupt_during_sleep(self, tmp_path, monkeypatch):
        """Interrupt during scheduler sleep → job not duplicated in queue."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))
        # Place the job in a retry backoff window so the scheduler sleeps
        # waiting for the backoff to expire. (Unknown resources now trigger an
        # immediate RESOURCE_DEADLOCK instead of sleeping.)
        pipeline.enqueue([Job("test", "j1", payload={})])
        queue = pipeline.backend.load_queue()
        # 持久化格式为 wall-clock 绝对截止 wall_deadline
        # （monotonic _backoff_until 仅内存使用，_run_body 加载时换算）。
        queue[0]["runtime"]["_backoff_wall_deadline"] = time.time() + 100.0
        pipeline.backend.save_queue(queue)

        sleep_count = [0]
        def fake_sleep(seconds):
            sleep_count[0] += 1
            if sleep_count[0] == 1:
                raise KeyboardInterrupt()

        monkeypatch.setattr("tasklite.pipeline.time.sleep", fake_sleep)

        # run saves the queue then re-raises KeyboardInterrupt.
        with pytest.raises(KeyboardInterrupt):
            pipeline.run()

        queue = pipeline.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in queue]
        # Job should appear exactly once (no duplicate from re-insert)
        assert uids.count("test::j1") == 1, (
            f"Expected exactly 1 occurrence of 'test::j1' in queue, got {uids.count('test::j1')}. "
            f"Queue: {uids}"
        )


# ─── retry-then-success & backoff waiting ───────────────────


class TestRetryThenSuccess:
    """REQ-11 happy path: a job that retries once and then succeeds."""

    def test_retry_then_success(self, tmp_path, monkeypatch):
        """Handler raises RetryError first, returns True second → job in wall, retries=1."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("flaky", lambda j, c: (True, {}))
        pipeline.enqueue([Job("flaky", "j1", payload={}, retries=0, max_retries=3)])

        RetryThenSuccessProcess = make_ipc_process_class(results=[
            {"status": "retry", "error": "transient"},
            {"status": "success", "raw_result": True,
             "new_jobs": [], "resource_suspensions": [], "cursor_updates": {}},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=RetryThenSuccessProcess)
        # Backoff delay = 0 so the job is immediately runnable after retry.
        monkeypatch.setattr("tasklite.engine.policy.PreflightPolicy.compute_backoff", lambda *a, **k: 0.0)
        # Don't actually sleep.
        monkeypatch.setattr("tasklite.pipeline.time.sleep", lambda s: None)

        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "flaky::j1" in wall, "Job should succeed after retry"
        assert RetryThenSuccessProcess.call_count[0] == 2, "Handler should have been invoked twice"
        # Queue should be empty
        assert pipeline.backend.load_queue() == []

    def test_backoff_until_in_future_skips_then_runs(self, tmp_path, monkeypatch):
        """Job with future wall_deadline is skipped; after clock advances, runs."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("delayed", lambda j, c: (True, {}))

        # Controllable wall clock: backoff wall_deadline at 100s after now
        wall_clock = [time.time()]
        # Controllable monotonic clock: start at t=1000, backoff until t=1100
        clock = [1000.0]
        monkeypatch.setattr("tasklite.engine.scheduler.time.monotonic", lambda: clock[0])
        monkeypatch.setattr("tasklite.pipeline.time.monotonic", lambda: clock[0])
        monkeypatch.setattr("tasklite.pipeline.time.time", lambda: wall_clock[0])
        # When the pipeline sleeps, advance the clock so the job becomes runnable.
        def fake_sleep(seconds):
            clock[0] += seconds
            wall_clock[0] += seconds
        monkeypatch.setattr("tasklite.pipeline.time.sleep", fake_sleep)

        job = Job("delayed", "j1", payload={}, retries=0, max_retries=3)
        job_dict = job.to_dict()
        # 持久化格式为 wall-clock 绝对截止（monotonic _backoff_until
        # 是加载期换算产物——直接注入 monotonic 值会被 _run_body 当作残留清除）。
        job_dict["runtime"]["_backoff_wall_deadline"] = wall_clock[0] + 100.0
        pipeline.backend.save_queue([job_dict])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        patch_pipeline_manager(pipeline, monkeypatch)

        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "delayed::j1" in wall, "Job should have run after backoff expired"
        assert clock[0] >= 1100.0, "Clock should have advanced past backoff_until"

    def test_retry_error_empty_message(self, tmp_path, monkeypatch):
        """RetryError with empty string message → retry status, no crash."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("empty_retry", lambda j, c: (True, {}))
        pipeline.enqueue([Job("empty_retry", "j1", payload={}, retries=3, max_retries=3)])

        EmptyMessageRetryProcess = make_ipc_process_class(results=[
            {"status": "retry", "error": ""},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=EmptyMessageRetryProcess)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "empty_retry::j1" in failed
        # Empty message but still MAX_RETRIES_EXCEEDED (retries already at max)
        assert failed["empty_retry::j1"]["error"] == "MAX_RETRIES_EXCEEDED"


class TestExceptionSubclassesAndEmpty:
    """Edge cases for exception types and messages."""

    def test_fatal_error_empty_message(self, tmp_path, monkeypatch):
        """FatalError with empty message → DLQ with fatal=True."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("empty_fatal", lambda j, c: (True, {}))
        pipeline.enqueue([Job("empty_fatal", "j1", payload={})])

        EmptyFatalProcess = make_ipc_process_class(results=[
            {"status": "fatal", "error": "", "traceback": ""},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=EmptyFatalProcess)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "empty_fatal::j1" in failed
        assert failed["empty_fatal::j1"].get("fatal") is True

    def test_retry_error_subclass_treated_as_retry(self, tmp_path, monkeypatch):
        """A subclass of RetryError → status 'retry' (isinstance check)."""
        from tasklite.exceptions import RetryError as _RE

        class CustomRetry(_RE):
            pass

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("sub_retry", lambda j, c: (True, {}))
        pipeline.enqueue([Job("sub_retry", "j1", payload={}, retries=3, max_retries=3)])

        SubclassRetryProcess = make_ipc_process_class(results=[
            {"status": "retry", "error": "custom retry"},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=SubclassRetryProcess)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "sub_retry::j1" in failed
        assert failed["sub_retry::j1"]["error"] == "MAX_RETRIES_EXCEEDED"

    def test_fatal_error_subclass_treated_as_fatal(self, tmp_path, monkeypatch):
        """A subclass of FatalError → status 'fatal' (isinstance check)."""
        from tasklite.exceptions import FatalError as _FE

        class CustomFatal(_FE):
            pass

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("sub_fatal", lambda j, c: (True, {}))
        pipeline.enqueue([Job("sub_fatal", "j1", payload={})])

        SubclassFatalProcess = make_ipc_process_class(results=[
            {"status": "fatal", "error": "custom fatal", "traceback": ""},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=SubclassFatalProcess)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "sub_fatal::j1" in failed
        assert failed["sub_fatal::j1"].get("fatal") is True

    def test_process_negative_exitcode_sigkill(self, tmp_path, monkeypatch):
        """exitcode=-9 (SIGKILL) → 结构化信号重试路径。

        旧行为：SIGKILL 死亡与确定性 bug 同判，Unknown 直接 DLQ（error 即
        PROCESS_CRASH_EXITCODE_-9、无重试）。新契约：按瞬态处理走正常计数
        退避；本用例 max_retries=0 使首次信号死亡即耗尽预算，终态应为
        「MAX_RETRIES_EXCEEDED + retry_error 记录信号死亡」而非旧的无重试
        Unknown 判死。
        """
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("killed", lambda j, c: (True, {}))
 # max_retries=0：信号死亡消耗一次预算即耗尽 → 验证重试耗尽终态
        pipeline.enqueue([Job("killed", "j1", payload={}, max_retries=0)])

        SigkillProcess = make_ipc_process_class(exitcode=-9)  # SIGKILL, no IPC put
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=SigkillProcess)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "killed::j1" in failed
        entry = failed["killed::j1"]
 # 新契约：走「瞬态重试耗尽」而非旧的 Unknown 直判——耗尽的 DLQ 行
 # 携带信号死亡信息（max_retries=0 时首次即耗尽，历史
 # _last_retry_error 为空属预期，retry_error 记录本次终态原因）
        assert entry["error"] == "MAX_RETRIES_EXCEEDED"
        assert entry.get("retry_error") == (
            "PROCESS_SIGNAL_DEATH: killed by SIGKILL (-9)")

    def test_process_negative_exitcode_sigkill_structured_decode(
        self, tmp_path,
    ):
        """主断言（decode 层）：SIGKILL 死亡 → retry_requested +
        结构化 signal/oom_hint meta；确定性崩溃（exitcode=1）保持判死不变。"""
        from types import SimpleNamespace

        from tasklite.engine.channel import (
            JobHandle, ExecutionChannel,
        )

        handle = JobHandle(uid="t::s", process=None, deadline=0.0, timeout=10,
                           job=Job("t", "s"), ipc_dir=str(tmp_path))
 # 信号死亡：环境性瞬态 → 重试
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=-9), handle, is_timeout=False)
        assert result.retry_requested is True
        assert result.result_meta["signal"] == "SIGKILL"
        assert result.result_meta["oom_hint"] is True
        assert "PROCESS_SIGNAL_DEATH" in (result.retry_error or "")
 # 对照：确定性退出码（handler sys.exit(1)/异常退出）维持判死
        result1 = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=1), handle, is_timeout=False)
        assert result1.retry_requested is False
        assert "signal" not in result1.result_meta

    def test_systemexit_in_subprocess_crash(self, tmp_path, monkeypatch):
        """SystemExit (BaseException, not Exception) → subprocess dies, crash exitcode."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("sysexit", lambda j, c: (True, {}))
        pipeline.enqueue([Job("sysexit", "j1", payload={})])

        SystemExitProcess = make_ipc_process_class(exitcode=1)  # no IPC result
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=SystemExitProcess)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "sysexit::j1" in failed
        assert "PROCESS_CRASH_EXITCODE_1" in failed["sysexit::j1"]["error"]


# ─── output cleanup variants ────────────────────────────────


class TestOutputCleanupVariants:
    """REQ-8 variants: cleanup_on_fail=False, directory cleanup, multiple outputs."""

    def _fake_manager(self, monkeypatch):
        # FakeManager imported from tests.helpers
        monkeypatch.setattr("multiprocessing.Manager", lambda: FakeManager())

    def test_cleanup_on_fail_false_keeps_file(self, tmp_path, monkeypatch):
        """cleanup_on_fail=False on failure → file is NOT deleted."""
        pipeline = make_pipeline(tmp_path)
        self._fake_manager(monkeypatch)

        keep_file = tmp_path / "output" / "keep.txt"
        keep_file.parent.mkdir(parents=True, exist_ok=True)
        keep_file.write_text("should survive")

        class FailWithCleanupFalse:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                ctx = self.args[2]
                ctx.declare_output(str(keep_file), cleanup_on_fail=False)  # cleanup=False
                _write_fake_result(self.args[3], self.args[1].uid, {"status": "error", "error": "boom", "traceback": "tb"}, incarnation=getattr(self.args[2], "incarnation", None))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FailWithCleanupFalse)

        pipeline.register_handler("keep_test", lambda j, c: None)
        pipeline.enqueue([Job("keep_test", "j1", payload={})])
        pipeline.run()

        assert keep_file.exists(), "File with cleanup_on_fail=False must survive failure"
        assert keep_file.read_text() == "should survive"

    def test_directory_output_cleaned_on_fail(self, tmp_path, monkeypatch):
        """Output declared as a directory → shutil.rmtree on failure."""
        pipeline = make_pipeline(tmp_path)
        self._fake_manager(monkeypatch)

        out_dir = tmp_path / "output" / "subdir"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "file1.txt").write_text("a")
        (out_dir / "file2.txt").write_text("b")

        class FailWithDirOutput:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                ctx = self.args[2]
                ctx.declare_output(str(out_dir), cleanup_on_fail=True)  # cleanup=True
                _write_fake_result(self.args[3], self.args[1].uid, {"status": "error", "error": "boom", "traceback": "tb"}, incarnation=getattr(self.args[2], "incarnation", None))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FailWithDirOutput)

        pipeline.register_handler("dir_test", lambda j, c: None)
        pipeline.enqueue([Job("dir_test", "j1", payload={})])
        pipeline.run()

        assert not out_dir.exists(), "Directory output should be removed on failure"

    def test_multiple_outputs_partial_missing_fails(self, tmp_path, monkeypatch):
        """Job declares 2 outputs; one missing on success → job fails, existing one cleaned."""
        pipeline = make_pipeline(tmp_path)
        self._fake_manager(monkeypatch)

        existing_file = tmp_path / "output" / "exists.txt"
        missing_file = tmp_path / "output" / "missing.txt"
        existing_file.parent.mkdir(parents=True, exist_ok=True)
        existing_file.write_text("present")
        # missing_file deliberately NOT created

        class MultiOutputProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                ctx = self.args[2]
                ctx.declare_output(str(existing_file), cleanup_on_fail=True)
                ctx.declare_output(str(missing_file), cleanup_on_fail=True)
                # Handler claims success, but missing_file doesn't exist
                _write_fake_result(self.args[3], self.args[1].uid, {
                    "status": "success", "raw_result": True,
                    "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
                }, incarnation=getattr(self.args[2], "incarnation", None))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=MultiOutputProcess)

        pipeline.register_handler("multi_out", lambda j, c: None)
        pipeline.enqueue([Job("multi_out", "j1", payload={})])
        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "multi_out::j1" in failed, "Job should fail due to missing output"
        assert "Missing output" in str(failed["multi_out::j1"])
        # existing_file should be cleaned (cleanup=True, failure path)
        assert not existing_file.exists(), "Existing output should be cleaned on failure"

    def test_multiple_outputs_all_present_succeeds(self, tmp_path, monkeypatch):
        """Job declares 2 outputs; both present on success → job succeeds."""
        pipeline = make_pipeline(tmp_path)
        self._fake_manager(monkeypatch)

        file_a = tmp_path / "output" / "a.txt"
        file_b = tmp_path / "output" / "b.txt"
        file_a.parent.mkdir(parents=True, exist_ok=True)
        file_a.write_text("a")
        file_b.write_text("b")

        class AllPresentProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                ctx = self.args[2]
                ctx.declare_output(str(file_a), cleanup_on_fail=True)
                ctx.declare_output(str(file_b), cleanup_on_fail=True)
                _write_fake_result(self.args[3], self.args[1].uid, {
                    "status": "success", "raw_result": True,
                    "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
                }, incarnation=getattr(self.args[2], "incarnation", None))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=AllPresentProcess)

        pipeline.register_handler("all_present", lambda j, c: None)
        pipeline.enqueue([Job("all_present", "j1", payload={})])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "all_present::j1" in wall
        # Both files survive on success
        assert file_a.exists()
        assert file_b.exists()


# ─── 死锁兜底分支保守化 ─────────────────────────


class TestDeadlockFallbackConservative:
    """死锁兜底分支（环检测空 / 不可归因）不再清空整队列。

    修复前：waiting_for_dependency 但 find_dependency_cycles 返回空时，兜底把
    **整个队列**标为环成员 DLQ；分类链全空（else）时把**整条队列** DLQ 并清空。
    保守后：记录 + 短退避，返回 False 下一轮再判——绝不误杀全部在途 job。
    """

    def _sched(self, **kwargs):
        import types
        base = dict(
            malformed_uids=(), unknown_resource_uids=(),
            missing_dependency_uids=(), impossible_resource_uids=(),
            waiting_for_dependency=False,
        )
        base.update(kwargs)
        return types.SimpleNamespace(**base)

    def _queue_two_jobs(self, tmp_path):
        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("slot", max_capacity=1.0))
        pipeline.register_handler("t", lambda j, c: True)
        pipeline.enqueue([Job("t", "a"), Job("t", "b")])
        state = PipelineState(
            pipeline.backend.load_wall(),
            pipeline.backend.load_failed(),
            pipeline.backend.load_cursors(),
            pipeline.backend.load_queue(),
        )
        pipeline._runtime.ctx.set_state(state)
        return pipeline

    def test_cycle_gap_fallback_keeps_queue(self, tmp_path):
        """环检测空兜底：队列原样保留、不误杀；返回 False（非终态）。"""
        pipeline = self._queue_two_jobs(tmp_path)
        pipeline._runtime.state.find_dependency_cycles = lambda: []
        sched = self._sched(waiting_for_dependency=True)
        decision = pipeline.store.handle_deadlock(sched)
        assert not decision
        assert decision.should_terminate is False
        assert decision.wait_time == 0.5
        assert len(pipeline._runtime.state.queue) == 2
        assert pipeline.backend.load_failed() == {}

    def test_unclassifiable_fallback_keeps_queue(self, tmp_path):
        """分类链全空（不可归因）兜底：队列原样保留、不误杀；返回 False。"""
        pipeline = self._queue_two_jobs(tmp_path)
        sched = self._sched()
        decision = pipeline.store.handle_deadlock(sched)
        assert not decision
        assert decision.should_terminate is False
        assert decision.wait_time == 0.5
        assert len(pipeline._runtime.state.queue) == 2
        assert pipeline.backend.load_failed() == {}


class TestDeadlockGapEscalation:
    """死锁分类缺口升级计数——主循环级终止性验证。

    保守兜底（环检测空 / 不可归因）在冻结状态上「重试下一轮」是空的：
    队列/in-flight 不变 → 分类结果必逐位相同 → 无限 sleep(0.5) + ERROR 刷屏。
    修复：连续轮次计数，达 _DEADLOCK_GAP_MAX_ROUNDS 升级为整队列 DLQ
    （专属错误码 DEADLOCK_CLASSIFICATION_GAP），恢复 run() 终止性。
    """

    def test_cycle_gap_escalates_to_dlq_and_run_terminates(self, tmp_path, monkeypatch):
        """真环 A↔B 但 find_dependency_cycles 返回空（分类缺口）→ 5 轮后
        升级整队列 DLQ，run() 正常返回（不永久挂起）。"""
        import types as types_mod
        from tasklite.models.state import PipelineState

        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("slot", max_capacity=2.0))
        pipeline.register_handler("t", lambda j, c: True)
        pipeline.enqueue([
            Job("t", "a", payload={}, depends_on=["t::b"]),
            Job("t", "b", payload={}, depends_on=["t::a"]),
        ])
        # run 内创建 state——monkeypatch 类方法使所有实例生效
        monkeypatch.setattr(PipelineState, "find_dependency_cycles", lambda self: [])
        monkeypatch.setattr("time.sleep", lambda s: None)  # 防 0.5s×5 慢
        pipeline.run()

        # run 正常终止（未挂起）→ 整队列 DLQ + 专属错误码
        failed = pipeline.backend.load_failed()
        assert "t::a" in failed and "t::b" in failed
        for uid, meta in failed.items():
            assert meta["error"] == "DEADLOCK_CLASSIFICATION_GAP", \
                f"{uid} 应带专属错误码，实际: {meta}"
        assert pipeline.backend.load_queue() == []
        assert pipeline.stats["failed"] == 2

    def test_cycle_gap_below_threshold_keeps_queue(self, tmp_path):
        """未达阈值前保持保守（不误杀）：单轮 _handle_deadlock 调用返回 False、
        队列保留——升级只在连续达阈值后发生。"""
        import types
        from tasklite.models.state import PipelineState

        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("slot", max_capacity=2.0))
        pipeline.register_handler("t", lambda j, c: True)
        pipeline.enqueue([Job("t", "a", payload={}, depends_on=["t::b"]),
                          Job("t", "b", payload={}, depends_on=["t::a"])])
        # 手动加载 state（_run_body 的加载逻辑）
        state = PipelineState(
            pipeline.backend.load_wall(), pipeline.backend.load_failed(),
            pipeline.backend.load_cursors(), pipeline.backend.load_queue(),
        )
        pipeline._runtime.ctx.set_state(state)
        state.find_dependency_cycles = lambda: []
        sched = types.SimpleNamespace(
            malformed_uids=(), unknown_resource_uids=(),
            missing_dependency_uids=(), impossible_resource_uids=(),
            waiting_for_dependency=True,
        )
        decision = pipeline.store.handle_deadlock(sched)
        assert not decision
        assert decision.should_terminate is False
        assert decision.wait_time == 0.5
        assert len(pipeline._runtime.state.queue) == 2
        assert pipeline.backend.load_failed() == {}
