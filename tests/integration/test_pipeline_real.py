"""Real multiprocessing subprocess tests — no mocking of mp.Process or mp.Queue."""

import os
import sys
from pathlib import Path

from tasklite.pipeline import TaskLite
from tasklite.models.job import Job
from tasklite.exceptions import RetryError
from tasklite.models.context import TaskContext


# ─── Real Subprocess Handlers (module-level for pickleability) ──────────


def _spawn_test_handler(job, ctx):
    """Handler that returns success with metadata — real subprocess test."""
    return True, {"spawn_ok": True, "uid": job.uid}


def _spawn_failing_handler(job, ctx):
    """Handler that raises RuntimeError — exercise real subprocess DLQ path."""
    raise RuntimeError("handler explosion")


def _spawn_sysexit_handler(job, ctx):
    """Handler that raises SystemExit — exercise WORKER_INTERRUPTED classification.

    SystemExit 是 BaseException 而非 Exception，
    真实子进程中 worker 的 ``except (KeyboardInterrupt, SystemExit)`` 分支
    负责把它写成 WORKER_INTERRUPTED 结果。
    """
    raise SystemExit(1)


def _spawn_retry_handler(job, ctx):
    """Handler that raises RetryError — exercise real subprocess retry IPC path."""
    raise RetryError("transient network failure")


def _spawn_output_success(job, ctx):
    """Handler that declares output file and succeeds — real subprocess test."""
    out = Path(ctx.output_root) / "success_output.txt"
    ctx.declare_output(str(out))
    out.write_text("all good")
    return True, {}


def _spawn_output_failure(job, ctx):
    """Handler that declares output file then raises — cleanup on failure."""
    out = Path(ctx.output_root) / "fail_output.txt"
    ctx.declare_output(str(out))
    out.write_text("partial")
    raise RuntimeError("boom")


def _spawn_hung_handler(job, ctx):
    """Handler that sleeps longer than timeout to test real subprocess SIGKILL."""
    import time
    time.sleep(5.0)
    return True, {}


def _spawn_record_pid_handler(job, ctx):
    """Handler that records its PID and does brief work to test real concurrency."""
    import os, time
    pid = os.getpid()
    if ctx.output_root:
        out = Path(ctx.output_root) / f"{job.job_id}.pid"
        ctx.declare_output(str(out), cleanup_on_fail=False)
        out.write_text(str(pid))
    time.sleep(0.05)
    return True, {"pid": pid}


# ─── Edge case handlers (module-level for pickleability) ──────────────


def _spawn_none_handler(job, ctx):
    """Handler that returns None → should be treated as success."""
    return None


def _spawn_dict_handler(job, ctx):
    """Handler that returns a dict → success with metadata."""
    return {"processed": True, "count": 42}


def _spawn_false_handler(job, ctx):
    """Handler that returns False → failure (DLQ)."""
    return False


def _spawn_keyerror_handler(job, ctx):
    """Handler that raises KeyError — a FATAL_EXCEPTION → fatal DLQ."""
    raise KeyError("missing_key")


def _spawn_valueerror_handler(job, ctx):
    """Handler that raises ValueError — Unknown classification → DLQ (not fatal)."""
    raise ValueError("bad value")


def _spawn_child_handler(job, ctx):
    """Handler that spawns a child job via ctx.spawn."""
    child = Job("real_child", "child_1", payload={"parent": job.uid})
    ctx.spawn(child)
    return True, {"spawned_child": True}


def _spawn_child_target_handler(job, ctx):
    """Target handler for spawned child jobs — simple success."""
    return True, {"child_ran": True}


def _spawn_cursor_handler(job, ctx):
    """Handler that sets a cursor via ctx.set_cursor."""
    ctx.set_cursor("last_position", "5000")
    return True, {"cursor_set": True}


def _spawn_suspend_handler(job, ctx):
    """Handler that suspends a resource via ctx.suspend_resource."""
    ctx.suspend_resource("api", 10.0)
    return True, {"suspended": True}


def _spawn_cleanup_false_handler(job, ctx):
    """Handler that declares output with cleanup_on_fail=False then raises.

    The output file should NOT be cleaned up because cleanup_on_fail=False.
    """
    out = Path(ctx.output_root) / "kept_output.txt"
    ctx.declare_output(str(out), cleanup_on_fail=False)
    out.write_text("should survive")
    raise RuntimeError("intentional failure with cleanup_on_fail=False")


def _spawn_directory_output_handler(job, ctx):
    """Handler that declares a directory output then raises — tests rmtree cleanup."""
    out_dir = Path(ctx.output_root) / "cleanup_dir"
    ctx.declare_output(str(out_dir))
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "file1.txt").write_text("content1")
    (out_dir / "file2.txt").write_text("content2")
    raise RuntimeError("dir failure")


class TestRealSubprocess:
    """Real mp.Process spawn + IPC (no mocking)."""

    def test_real_subprocess_spawn_and_ipc(self, tmp_path, monkeypatch):
        """Handler function runs in real spawned subprocess, result via IPC."""
        import os, sys
        project_root = str(Path(__file__).resolve().parent.parent.parent)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        monkeypatch.setenv("PYTHONPATH", project_root)

        pipeline = TaskLite(
            name="real_sp",
            state_dir=tmp_path / "state",
            backend="sqlite",
        )
        pipeline.register_handler("real_handler", _spawn_test_handler)
        pipeline.enqueue([Job("real_handler", "j1", payload={})])

        # NO monkeypatch — real mp.Process, real mp.Queue
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "real_handler::j1" in wall, (
            f"Job should be in wall after real subprocess execution. "
            f"Wall keys: {list(wall.keys())}"
        )
        assert wall["real_handler::j1"].get("spawn_ok") is True
        assert wall["real_handler::j1"].get("uid") == "real_handler::j1"

    def test_real_subprocess_handler_raises_exception(self, tmp_path):
        """Handler raises Exception → job goes to DLQ via real subprocess."""
        pipeline = TaskLite(
            name="real_sp_fail",
            state_dir=tmp_path / "state",
            backend="sqlite",
        )
        pipeline.register_handler("fail_handler", _spawn_failing_handler)
        pipeline.enqueue([Job("fail_handler", "j1", payload={})])

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "fail_handler::j1" in failed, (
            f"Failing job should be in DLQ. Failed keys: {list(failed.keys())}"
        )
        wall = pipeline.backend.load_wall()
        assert "fail_handler::j1" not in wall, (
            "Failed handler should NOT be in wall"
        )
        err = failed["fail_handler::j1"].get("error", "")
        assert "handler explosion" in err, (
            f"Error should contain 'handler explosion', got: {err}"
        )

    def test_real_subprocess_retry_error(self, tmp_path):
        """Handler raises RetryError → job retries via real subprocess IPC."""
        pipeline = TaskLite(
            name="real_sp_retry",
            state_dir=tmp_path / "state",
            backend="sqlite",
        )
        pipeline.register_handler("retry_handler", _spawn_retry_handler)
        pipeline.enqueue([Job("retry_handler", "j1", payload={}, max_retries=2, backoff_base=0.01)])

        pipeline.run()

        failed = pipeline.backend.load_failed()
        wall = pipeline.backend.load_wall()
        assert "retry_handler::j1" in failed, (
            f"Job must be in DLQ after retry exhaustion. "
            f"Failed: {list(failed.keys())}, Wall: {list(wall.keys())}"
        )
        assert "retry_handler::j1" not in wall, (
            "Retry-exhausted job should NOT be in wall (it always fails)"
        )
        assert failed["retry_handler::j1"]["error"] == "MAX_RETRIES_EXCEEDED"

    def test_real_subprocess_systemexit_interrupted(self, tmp_path):
        """Handler raises SystemExit → worker classifies as WORKER_INTERRUPTED.

        走真实子进程，让 worker 的 ``except (KeyboardInterrupt, SystemExit)``
        分类分支真正执行。
        """
        pipeline = TaskLite(
            name="real_sp_sysexit",
            state_dir=tmp_path / "state",
            backend="sqlite",
        )
        pipeline.register_handler("sysexit_handler", _spawn_sysexit_handler)
        pipeline.enqueue([Job("sysexit_handler", "j1", payload={})])

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "sysexit_handler::j1" in failed, (
            f"SystemExit job should be in DLQ. Failed keys: {list(failed.keys())}"
        )
        err = failed["sysexit_handler::j1"].get("error", "")
        assert "WORKER_INTERRUPTED" in err, (
            f"Error should contain 'WORKER_INTERRUPTED' (worker classification "
            f"branch), got: {err}"
        )

    def test_real_subprocess_declare_output_and_cleanup(self, tmp_path):
        """Handler declares output → file persists on success, cleaned on failure."""
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        success_file = output_dir / "success_output.txt"

        pipeline_success = TaskLite(
            name="real_sp_output",
            state_dir=tmp_path / "state1",
            backend="sqlite",
            output_root=output_dir,
        )
        pipeline_success.register_handler("success_handler", _spawn_output_success)
        pipeline_success.enqueue([Job("success_handler", "j1", payload={})])
        pipeline_success.run()

        wall = pipeline_success.backend.load_wall()
        assert "success_handler::j1" in wall
        assert success_file.exists(), "Declared output should exist after success"
        assert success_file.read_text() == "all good"

        pipeline_fail = TaskLite(
            name="real_sp_fail2",
            state_dir=tmp_path / "state2",
            backend="sqlite",
            output_root=output_dir,
        )
        pipeline_fail.register_handler("fail_handler2", _spawn_output_failure)
        pipeline_fail.enqueue([Job("fail_handler2", "j1", payload={})])
        pipeline_fail.run()

        fail_file = output_dir / "fail_output.txt"
        assert not fail_file.exists(), (
            "Declared output should be cleaned up after failure"
        )
        failed = pipeline_fail.backend.load_failed()
        assert "fail_handler2::j1" in failed, "Failed job must be in DLQ"
        wall = pipeline_fail.backend.load_wall()
        assert "fail_handler2::j1" not in wall, "Failed job must NOT be in wall"


# ============================================================
# Real subprocess edge cases (invariant verification)
# ============================================================


class TestRealSubprocessWeirdCases:
    """Real subprocess tests for handler return-value normalization, fatal
    exception detection, spawn/cursor/suspend IPC, and cleanup_on_fail=False.

    All handlers are module-level functions for pickleability under the
    'spawn' multiprocessing start method.
    """

    @staticmethod
    def _setup_path(monkeypatch):
        """Ensure project root is on sys.path / PYTHONPATH for subprocess imports."""
        project_root = str(Path(__file__).resolve().parent.parent.parent)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        monkeypatch.setenv("PYTHONPATH", project_root)

    def test_real_subprocess_handler_returns_none(self, tmp_path, monkeypatch):
        """Handler returns None → succeeds (None is a valid success signal per _normalize_handler_result)."""
        self._setup_path(monkeypatch)
        pipeline = TaskLite(
            name="real_none", state_dir=tmp_path / "state", backend="sqlite"
        )
        pipeline.register_handler("none_handler", _spawn_none_handler)
        pipeline.enqueue([Job("none_handler", "j1", payload={})])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "none_handler::j1" in wall, f"None-returning handler should succeed. Wall: {list(wall.keys())}"

    def test_real_subprocess_handler_returns_dict(self, tmp_path, monkeypatch):
        """Handler returns a dict → success with metadata preserved."""
        self._setup_path(monkeypatch)
        pipeline = TaskLite(
            name="real_dict", state_dir=tmp_path / "state", backend="sqlite"
        )
        pipeline.register_handler("dict_handler", _spawn_dict_handler)
        pipeline.enqueue([Job("dict_handler", "j1", payload={})])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "dict_handler::j1" in wall
        assert wall["dict_handler::j1"].get("processed") is True
        assert wall["dict_handler::j1"].get("count") == 42

    def test_real_subprocess_handler_returns_false(self, tmp_path, monkeypatch):
        """Handler returns False → failure (DLQ), not retry."""
        self._setup_path(monkeypatch)
        pipeline = TaskLite(
            name="real_false", state_dir=tmp_path / "state", backend="sqlite"
        )
        pipeline.register_handler("false_handler", _spawn_false_handler)
        pipeline.enqueue([Job("false_handler", "j1", payload={})])
        pipeline.run()

        failed = pipeline.backend.load_failed()
        wall = pipeline.backend.load_wall()
        assert "false_handler::j1" in failed, "False return should go to DLQ"
        assert "false_handler::j1" not in wall, "False return should NOT be in wall"

    def test_real_subprocess_handler_raises_keyerror(self, tmp_path, monkeypatch):
        """Handler raises KeyError → fatal DLQ (auto-detected FATAL_EXCEPTION)."""
        self._setup_path(monkeypatch)
        pipeline = TaskLite(
            name="real_keyerr", state_dir=tmp_path / "state", backend="sqlite"
        )
        pipeline.register_handler("ke_handler", _spawn_keyerror_handler)
        pipeline.enqueue([Job("ke_handler", "j1", payload={})])
        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "ke_handler::j1" in failed, "KeyError should go to fatal DLQ"
        err_str = str(failed["ke_handler::j1"])
        assert "KeyError" in err_str or "missing_key" in err_str, (
            f"Error should mention KeyError, got: {failed['ke_handler::j1']}"
        )

    def test_real_subprocess_handler_raises_valueerror(self, tmp_path, monkeypatch):
        """Handler raises ValueError → Unknown classification → DLQ (not fatal)."""
        self._setup_path(monkeypatch)
        pipeline = TaskLite(
            name="real_verr", state_dir=tmp_path / "state", backend="sqlite"
        )
        pipeline.register_handler("ve_handler", _spawn_valueerror_handler)
        pipeline.enqueue([Job("ve_handler", "j1", payload={})])
        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "ve_handler::j1" in failed, "ValueError should go to DLQ (Unknown classification, not fatal)"
        err_str = str(failed["ve_handler::j1"])
        assert "ValueError" in err_str or "bad value" in err_str, (
            f"Error should mention ValueError, got: {failed['ve_handler::j1']}"
        )

    def test_real_subprocess_spawn_child(self, tmp_path, monkeypatch):
        """Handler calls ctx.spawn(Job(...)) → child appears in queue and runs."""
        self._setup_path(monkeypatch)
        pipeline = TaskLite(
            name="real_spawn", state_dir=tmp_path / "state", backend="sqlite"
        )
        pipeline.register_handler("real_parent", _spawn_child_handler)
        pipeline.register_handler("real_child", _spawn_child_target_handler)
        pipeline.enqueue([Job("real_parent", "parent_1", payload={})])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "real_parent::parent_1" in wall, "Parent should succeed"
        assert wall["real_parent::parent_1"].get("spawned_child") is True
        assert "real_child::child_1" in wall, (
            f"Spawned child should also succeed. Wall: {list(wall.keys())}"
        )

    def test_real_subprocess_cursor_update(self, tmp_path, monkeypatch):
        """Handler calls ctx.set_cursor → cursor persisted to state backend."""
        self._setup_path(monkeypatch)
        pipeline = TaskLite(
            name="real_cursor", state_dir=tmp_path / "state", backend="sqlite"
        )
        pipeline.register_handler("cursor_handler", _spawn_cursor_handler)
        pipeline.enqueue([Job("cursor_handler", "j1", payload={})])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "cursor_handler::j1" in wall
        cursors = pipeline.backend.load_cursors()
        assert cursors.get("last_position") == "5000", (
            f"Cursor should be persisted, got: {cursors}"
        )

    def test_real_subprocess_suspend_resource(self, tmp_path, monkeypatch):
        """Handler calls ctx.suspend_resource → resource suspension applied globally."""
        from tasklite.engine.resource import RateLimitResource

        self._setup_path(monkeypatch)
        pipeline = TaskLite(
            name="real_suspend", state_dir=tmp_path / "state", backend="sqlite"
        )
        api_resource = RateLimitResource("api", interval_seconds=1.0)
        pipeline.add_resource(api_resource)
        pipeline.register_handler("suspend_handler", _spawn_suspend_handler)
        pipeline.enqueue([Job("suspend_handler", "j1", payload={}, resources={"api": 1.0})])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "suspend_handler::j1" in wall
        # After suspension, next_available should be in the future (10s suspension)
        assert api_resource.next_available > 0, (
            "Resource should be suspended (next_available > 0)"
        )

    def test_real_subprocess_declare_output_cleanup_on_fail_false(self, tmp_path, monkeypatch):
        """declare_output(cleanup_on_fail=False) + failure → file REMAINS on disk."""
        self._setup_path(monkeypatch)
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        pipeline = TaskLite(
            name="real_cleanup_false",
            state_dir=tmp_path / "state",
            backend="sqlite",
            output_root=output_dir,
        )
        pipeline.register_handler("cleanup_false_handler", _spawn_cleanup_false_handler)
        pipeline.enqueue([Job("cleanup_false_handler", "j1", payload={})])
        pipeline.run()

        kept_file = output_dir / "kept_output.txt"
        assert kept_file.exists(), (
            "File declared with cleanup_on_fail=False should SURVIVE failure"
        )
        assert kept_file.read_text() == "should survive"
        failed = pipeline.backend.load_failed()
        assert "cleanup_false_handler::j1" in failed

    def test_real_subprocess_directory_output_cleaned_on_fail(self, tmp_path, monkeypatch):
        """declare_output pointing at a directory + failure → shutil.rmtree invoked."""
        self._setup_path(monkeypatch)
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        pipeline = TaskLite(
            name="real_dir_cleanup",
            state_dir=tmp_path / "state",
            backend="sqlite",
            output_root=output_dir,
        )
        pipeline.register_handler("dir_fail_handler", _spawn_directory_output_handler)
        pipeline.enqueue([Job("dir_fail_handler", "j1", payload={})])
        pipeline.run()

        cleaned_dir = output_dir / "cleanup_dir"
        assert not cleaned_dir.exists(), (
            "Directory output should be rmtree'd on failure (cleanup_on_fail=True default)"
        )
        failed = pipeline.backend.load_failed()
        assert "dir_fail_handler::j1" in failed

    def test_real_subprocess_timeout_killed(self, tmp_path, monkeypatch):
        """Job with small timeout running hung handler → killed by watchdog and moved to DLQ."""
        self._setup_path(monkeypatch)
        pipeline = TaskLite(
            name="real_timeout",
            state_dir=tmp_path / "state",
            backend="sqlite",
        )
        pipeline.register_handler("hung_handler", _spawn_hung_handler)
        pipeline.enqueue([Job("hung_handler", "j1", payload={}, timeout=0.2)])
        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "hung_handler::j1" in failed, "Hung job should fail into DLQ"
        err = failed["hung_handler::j1"].get("error", "")
        assert "TIMEOUT" in err or "CRASH" in err or "EXITCODE" in err, (
            f"Expected timeout/crash error, got: {err}"
        )

    def test_real_subprocess_concurrent_workers(self, tmp_path, monkeypatch):
        """Multiple jobs executed concurrently by real subprocess workers with distinct PIDs."""
        self._setup_path(monkeypatch)
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        pipeline = TaskLite(
            name="real_concurrent",
            state_dir=tmp_path / "state",
            backend="sqlite",
            output_root=output_dir,
            max_workers=3,
        )
        pipeline.register_handler("pid_handler", _spawn_record_pid_handler)
        pipeline.enqueue([
            Job("pid_handler", "j1", payload={}),
            Job("pid_handler", "j2", payload={}),
            Job("pid_handler", "j3", payload={}),
        ])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert len(wall) == 3, f"Expected 3 completed jobs in wall, got {len(wall)}"
        pids = {wall[f"pid_handler::j{i}"]["pid"] for i in range(1, 4)}
        # Each job ran in a separate subprocess, distinct from main process PID
        parent_pid = os.getpid()
        assert parent_pid not in pids, "Handlers should run in subprocesses, not parent process"

