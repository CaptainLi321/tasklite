"""v2 执行通道测试：spawn 兜底、看门狗、结果认证、解码防御、fencing 认领与中止。

以受控进程桩与内联 worker 包装确定性复现时序（无真实多进程竞速）；
端到端（派发机器/完成机器装配后的 run 主循环）由后续门面阶段承接。
"""

import json
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from tasklite.engine.channel import (
    ExecutionChannel,
    WorkerLaunchSpec,
    _decode_ipc_result,
    _worker_process_entry,
)
from tasklite.engine.types import JobHandle
from tasklite.models.context import JobContext
from tasklite.models.job import Job
from tasklite.models.result import TaskResult
from tasklite.utils.ipc import (
    WorkerPipeSender,
    create_worker_pipe,
)

_EXECUTION_ID = "deadbeefdeadbeefdeadbeefdeadbeef.1"


def make_ctx(job: Job) -> JobContext:
    """最小 JobContext 装配（声明/挂起默认空，ipc_dir 由 spawn 回填）。"""
    return JobContext(job, success_job_uids=set(), failed_job_uids=set(), cursors={})


def make_spec(
    job: Job,
    handler,
    ipc_dir,
    execution_id: str | None = _EXECUTION_ID,
    *,
    pipe_sender: WorkerPipeSender | None = None,
    job_context: JobContext | None = None,
) -> WorkerLaunchSpec:
    return WorkerLaunchSpec(
        handler=handler,
        job=job,
        job_context=job_context if job_context is not None else make_ctx(job),
        execution_id=execution_id,  # type: ignore[arg-type]
        ipc_dir=str(ipc_dir) if ipc_dir is not None else None,
        timeout_seconds=60.0,
        pipe_sender=pipe_sender,
    )


class TestExecutionChannelBasics:
    def test_init_and_directory_creation(self, tmp_path):
        ipc_dir = tmp_path / "ipc"
        assert not ipc_dir.exists()
        channel = ExecutionChannel(ipc_dir)
        assert ipc_dir.exists()
        assert channel.ipc_dir == str(ipc_dir)

    def test_drain_active_signals(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        channel._pending_signals.append(("job_signals", "gpu", 15.0))
        channel._pending_signals.append(("job_signals", "api", 3.0))

        signals = channel.drain_active_signals(["job_signals", "job_other"])
        assert ("job_signals", "gpu", 15.0) in signals
        assert ("job_signals", "api", 3.0) in signals

class TestWorkerWrapper:
    """worker 执行体入口：Pipe 结果发送、锁冲突/环境故障降级、瞬态三 kind 回传。"""

    @staticmethod
    def _run_worker(
        job: Job,
        handler,
        ipc_dir,
        execution_id: str = _EXECUTION_ID,
        *,
        job_context: JobContext | None = None,
    ) -> dict:
        receiver, sender = create_worker_pipe(job_uid=job.uid, execution_id=execution_id)
        try:
            spec = make_spec(
                job,
                handler,
                ipc_dir,
                execution_id=execution_id,
                pipe_sender=sender,
                job_context=job_context,
            )
            _worker_process_entry(spec)
            messages = receiver.drain_to_eof(timeout=3.0)
        finally:
            sender.close()
            receiver.close()
        results = [
            m.payload
            for m in messages
            if not isinstance(m, Exception) and m.kind == "result"
        ]
        assert len(results) == 1
        return results[0]

    def test_worker_writes_success_over_pipe(self, tmp_path):
        job = Job("t", "x")
        data = self._run_worker(job, lambda j, c: TaskResult.success(metadata={"ok": 1}), tmp_path)
        assert data["status"] == "success"
        assert data["metadata"] == {"ok": 1}

    def test_worker_error_payloads_sent_over_pipe(self, tmp_path):
        """retry/failure 各状态通道的 payload 均经 Pipe 发送。"""
        def return_retry(job, ctx):
            return TaskResult.retry(message="t")

        def raise_system_exit(job, ctx):
            raise SystemExit(2)

        for handler, want_status, job_id in (
            (return_retry, "retry", "yretry"),
            (raise_system_exit, "failure", "yexit"),
        ):
            job = Job("t", job_id)
            data = self._run_worker(job, handler, tmp_path)
            assert data["status"] == want_status

    def test_worker_rejects_missing_execution_id(self, tmp_path):
        """execution_id 缺失 fail-loud（fencing 契约破坏不得静默产出无名结果）。"""
        job = Job("t", "x")
        with pytest.raises(RuntimeError, match="execution_id"):
            _worker_process_entry(make_spec(job, lambda j, c: TaskResult.success(), tmp_path, execution_id=None))

    def test_retry_carries_consume_budget(self, tmp_path):
        """retry 结果携带 consume_budget 经 Pipe 传递。"""
        def zero_budget(job, ctx):
            return TaskResult.retry(message="no burn", consume_budget=False)

        def burn_budget(job, ctx):
            return TaskResult.retry(message="burn", consume_budget=True)

        for handler, job_id, want_consume in (
            (zero_budget, "zb", False),
            (burn_budget, "bb", True),
        ):
            job = Job("h", job_id)
            res = self._run_worker(job, handler, tmp_path)
            assert res["status"] == "retry"
            assert res["consume_budget"] is want_consume

    def test_keyboard_interrupt_writes_interrupted(self, tmp_path):
        def interrupted(job, ctx):
            raise KeyboardInterrupt()

        job = Job("h", "ki")
        res = self._run_worker(job, interrupted, tmp_path)
        assert res["status"] == "interrupted"
        assert res["error_message"].startswith("WORKER_INTERRUPTED:")

    def test_unhandled_exception_writes_failure_with_exception_dict(self, tmp_path):
        """未捕获异常统一结算为 failure，记录 type/message/traceback，不自动重试。"""
        job = Job("h", "err")
        res = self._run_worker(
            job, lambda j, c: (_ for _ in ()).throw(ValueError("boom")), tmp_path,
        )
        assert res["status"] == "failure"
        assert res["message"] == "ValueError: boom"
        assert res["exception"]["type"] == "ValueError"
        assert res["exception"]["message"] == "boom"
        assert "traceback" in res["exception"]

    def test_pipe_send_failure_exits_without_crash(self, tmp_path, monkeypatch):
        """Pipe 发送结果遇到 OSError 时，worker 不抛未捕获异常退出。"""
        def refusing_send(sender_self, payload):
            raise BrokenPipeError("pipe broken")

        monkeypatch.setattr(WorkerPipeSender, "send_result", refusing_send)
        job = Job("h", "a")
        receiver, sender = create_worker_pipe(job_uid=job.uid, execution_id=_EXECUTION_ID)
        try:
            _worker_process_entry(make_spec(
                job, lambda j, c: (True, {}), tmp_path, pipe_sender=sender
            ))
            assert receiver.drain_to_eof(timeout=3.0) == []
        finally:
            sender.close()
            receiver.close()


class TestEnvFallbackIpcDirCoherence:
    """env 兜底 ipc_dir：解析成功必须回写实例属性，收割路径同源可用。"""

    def test_env_fallback_spawn_reap_probe_share_same_ipc_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TASKLITE_IPC_DIR", str(tmp_path))

        class ResultWritingProcess:
            """start() 时经 spec.pipe_sender 发送 success 结果的进程桩。"""

            def __init__(self, target=None, args=(), kwargs=None, **_kw):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                spec = self.args[0]
                self._alive = True
                assert spec.pipe_sender is not None
                spec.pipe_sender.send_result(
                    {
                        "status": "success",
                        "metadata": {},
                        "spawned_jobs": [],
                        "resource_suspensions": [],
                        "cursor_updates": {},
                    }
                )

            def join(self, timeout=None):
                self._alive = False

            def is_alive(self):
                return self._alive

            def kill(self):
                self._alive = False

        fake_process_context = type("Ctx", (), {"Process": ResultWritingProcess})()
        channel = ExecutionChannel(process_context=fake_process_context)

        job = Job("t", "x")
        try:
            handle = channel.spawn(make_spec(job, lambda j, c: TaskResult.success(), None))

            # 实例属性与 handle 的派发路径同源
            assert channel.ipc_dir == str(tmp_path), (
                "env 兜底解析成功后必须回写实例属性（收割路径的事实源）"
            )
            assert handle.ipc_dir == str(tmp_path)

            completed = channel.reap_completed([handle])
            assert [(h.job_uid, r.success) for h, r in completed] == [("t::x", True)]
        finally:
            channel.close()


class TestSpawnFailureCleanup:
    def test_submit_start_failure_reraises(self, tmp_path):
        class ExplodingCtx:
            def Process(self, *args, **kwargs):
                raise RuntimeError("process start failed")

        channel = ExecutionChannel(process_context=ExplodingCtx(), ipc_dir=str(tmp_path))
        job = Job("t", "j1")
        with pytest.raises(RuntimeError, match="process start failed"):
            channel.spawn(make_spec(job, lambda j, c: TaskResult.success(), tmp_path))

    def test_submit_without_ipc_dir(self, monkeypatch):
        monkeypatch.delenv("TASKLITE_IPC_DIR", raising=False)
        started = []
        class DummyProcess:
            def start(self):
                started.append(True)
        channel = ExecutionChannel(
            process_context=type("Ctx", (), {"Process": lambda *a, **k: DummyProcess()})()
        )
        job = Job("t", "j1")
        try:
            handle = channel.spawn(make_spec(job, lambda j, c: TaskResult.success(), None))
            assert handle is not None
            assert started == [True]
        finally:
            channel.close()


class TestHandlerInvalidReturnTypes:
    """None/bool/dict/tuple 等非 TaskResult 返回值一律收敛为任务级失败。"""

    @pytest.mark.parametrize(
        "bad_return",
        [
            None,
            True,
            False,
            {"ok": True},
            (True, {"a": 1}),
            42,
            "success",
        ],
    )
    def test_non_task_result_rejected_as_failure(self, tmp_path, bad_return):
        job = Job("t", "bad")
        data = TestWorkerWrapper._run_worker(
            job, lambda j, c: bad_return, tmp_path
        )
        assert data["status"] == "failure"
        assert "invalid handler return type: expected TaskResult" in data["message"]


_BASE_OK = {
    "status": "success",
    "metadata": {"ok": True},
    "spawned_jobs": [],
    "cursor_updates": {},
    "resource_suspensions": [],
}


def _decode(res, p=None, ipc_dir=None):
    return _decode_ipc_result(res, p, Job("t", "a"), ipc_dir)


class TestDecodeIpcResultDefense:
    """损坏结果防御分支：任何异常形态收敛为任务级失败，不穿透崩 run。"""

    def test_missing_metadata_key_marks_failed(self):
        res = dict(_BASE_OK)
        del res["metadata"]
        result = _decode(res)
        assert result.success is False
        assert "CORRUPT_RESULT_FILE: missing metadata" in (result.message or "")

    def test_success_decode_crash_converted_to_corrupt_result(self, monkeypatch):
        """解码异常兜底：success 分支解码/归一化段意外异常 → 转损坏结果。"""
        monkeypatch.setattr(
            "tasklite.engine.channel.validate_strict_json_object",
            lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        result = _decode(dict(_BASE_OK))
        assert result.success is False
        assert (result.message or "").startswith("CORRUPT_RESULT_FILE: decode failed")

    def test_new_jobs_scalar_marks_failed_not_crash(self):
        for scalar in (7, 3.5, True):
            result = _decode(dict(_BASE_OK, spawned_jobs=scalar))
            assert result.success is False
            assert "invalid spawned_jobs type" in (result.message or "")

    def test_malformed_spawned_job_marks_failed(self):
        res = dict(_BASE_OK, spawned_jobs=[{"task_type": "t"}])  # 缺 job_id
        result = _decode(res)
        assert result.success is False
        assert "invalid spawned job dict" in (result.message or "")

    def test_valid_spawned_jobs_parsed(self):
        res = dict(_BASE_OK, spawned_jobs=[
            {"task_type": "t", "job_id": "child", "payload": {"v": 1}},
        ])
        result = _decode(res)
        assert result.success is True
        assert [j.uid for j in result.spawned_jobs] == ["t::child"]

    def test_cursor_updates_type_not_dict_marks_failed(self):
        res = dict(_BASE_OK, cursor_updates=["k", "v"])
        result = _decode(res)
        assert result.success is False
        assert "invalid cursor_updates type" in (result.message or "")

    def test_cursor_updates_value_not_str_marks_failed(self):
        res = dict(_BASE_OK, cursor_updates={"k": 123})
        result = _decode(res)
        assert result.success is False
        assert "invalid cursor_updates value" in (result.message or "")

    def test_resource_suspensions_bad_element_marks_failed(self):
        res = dict(_BASE_OK, resource_suspensions=[("api", "not-a-number")])
        result = _decode(res)
        assert result.success is False
        assert "invalid resource_suspension entry" in (result.message or "")

    def test_resource_suspensions_not_list_marks_failed(self):
        res = dict(_BASE_OK, resource_suspensions={"api": 1.0})
        result = _decode(res)
        assert result.success is False
        assert "invalid resource_suspensions type" in (result.message or "")

    def test_retry_status_extracts_transient_kind(self):
        result = _decode({"status": "retry", "error_message": "x", "transient_kind": "rate_limited"})
        assert result.retry_requested is True
        assert result.transient_kind == "rate_limited"

    def test_retry_status_non_string_kind_ignored(self):
        result = _decode({"status": "retry", "error_message": "x", "transient_kind": 7})
        assert result.retry_requested is True
        assert result.transient_kind is None

    def test_interrupted_status_forces_transient_kind(self):
        result = _decode({"status": "interrupted", "error_message": "sig"})
        assert result.retry_requested is True
        assert result.transient_kind == "interrupted"

    def test_fatal_status_marks_failed(self):
        res = {"status": "failure", "message": "boom", "metadata": {"attempt": 1}}
        result = _decode(res)
        assert result.success is False
        assert result.message == "boom"
        assert result.result_metadata == {"attempt": 1}

    def test_unknown_status_marks_failed(self):
        result = _decode({"status": "weird"})
        assert result.success is False
        assert "unknown status" in (result.message or "")

    def test_crash_exitcode_marks_failed(self):
        class _P:
            exitcode = -9

        res = {"raw": "not-a-result-dict"}  # 无 status 键 → 走 exitcode 归因
        result = _decode(res, p=_P())
        assert result.success is False
        assert "PROCESS_CRASH_EXITCODE_-9" in result.result_metadata["error_message"]

    def test_missing_status_without_process_requests_retry(self):
        """认领路径（p=None）读到无 status 的残留文件 → NO_IPC_RESULT 可重试。"""
        result = _decode({}, None)
        assert result.success is False
        assert result.retry_requested is True
        assert "NO_IPC_RESULT" in result.result_metadata["error_message"]

    def test_nonzero_exitcode_without_status_requests_retry(self):
        res = {"raw": "not-a-result-dict"}
        result = _decode(res, SimpleNamespace(exitcode=1))
        assert result.success is False
        assert result.retry_requested is True
        assert "PROCESS_CRASH_EXITCODE_1" in result.result_metadata["error_message"]
        assert "PROCESS_CRASH_EXIT" in (result.retry_error or "")

    def test_decode_leaves_output_validation_to_completion(self, tmp_path):
        result = _decode(dict(_BASE_OK), ipc_dir=str(tmp_path))
        assert result.success is True


class TestDecodeConvergencePaths:
    """收割路径收敛：损坏结果归一为任务级结果，绝不以 TypeError 崩 run。"""

    _SCALAR_CORRUPT = {
        "status": "success",
        "metadata": {},
        "spawned_jobs": 7,
        "cursor_updates": {},
        "resource_suspensions": [],
    }

    class _DeadProcess:
        exitcode = 0

        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

        def kill(self):
            pass

        def close(self):
            pass

    def test_reap_completed_contains_scalar_new_jobs_corruption(self, tmp_path):
        job = Job("t", "a")
        channel = ExecutionChannel(tmp_path)
        channel._results[_EXECUTION_ID] = dict(self._SCALAR_CORRUPT)
        handle = JobHandle(
            job_uid=job.uid, process=self._DeadProcess(), timeout_at_monotonic=1.0, timeout_seconds=5.0,
            job=job, ipc_dir=str(tmp_path), execution_id=_EXECUTION_ID,
        )
        completed = channel.reap_completed([handle])
        assert len(completed) == 1, "损坏任务必须被收割而非打断收割循环"
        result = completed[0][1]
        assert result.success is False
        assert "invalid spawned_jobs type" in (result.message or "")
        assert _EXECUTION_ID not in channel._results


class TestBuildTerminalFailureDefense:
    """无结果消息的终局构造：超时瞬态/终局、崩溃、无结果分类。"""

    class _JobStub:
        def __init__(self, timeout_is_transient):
            self.timeout_is_transient = timeout_is_transient

    @staticmethod
    def _handle(timeout_is_transient):
        return JobHandle(
            job_uid="t::a",
            process=SimpleNamespace(exitcode=None),
            timeout_at_monotonic=1.0,
            timeout_seconds=5.0,
            job=TestBuildTerminalFailureDefense._JobStub(timeout_is_transient),
            ipc_dir="/tmp",
        )

    def test_timeout_transient_requests_retry(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=None), self._handle(True), is_timeout=True,
        )
        assert result.success is False
        assert result.retry_requested is True
        assert "TIMEOUT" in (result.retry_error or "")

    def test_timeout_not_transient_marks_failed(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=None), self._handle(False), is_timeout=True,
        )
        assert result.success is False
        assert result.retry_requested is False
        assert "TIMEOUT" in result.result_metadata["error_message"]

    def test_crash_exitcode_classified(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=-11), self._handle(False), is_timeout=False,
        )
        assert result.success is False
        assert result.retry_requested is True
        assert "PROCESS_CRASH_EXITCODE_-11" in result.result_metadata["error_message"]

    def test_clean_exit_without_result_classified(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=0), self._handle(False), is_timeout=False,
        )
        assert result.success is False
        assert result.retry_requested is True
        assert "NO_IPC_RESULT" in result.result_metadata["error_message"]
        assert result.retry_error


class TestWatchdogReap:
    """看门狗：timeout_at_monotonic 驱动 kill；join 窗口自然退出按 exitcode 归因。"""

    class _HungProcess:
        """kill 前恒活的进程桩（模拟死锁/卡死）。"""

        def __init__(self):
            self._alive = True
            self.exitcode = None

        def is_alive(self):
            return self._alive

        def join(self, timeout=None):
            pass

        def kill(self):
            self._alive = False
            self.exitcode = -9

        def close(self):
            pass

    class _DyingDuringJoinProcess:
        """join 窗口内自然退出的进程桩（无结果消息，exitcode 可注入）。"""

        def __init__(self, exitcode: int = 0):
            self._alive = True
            self.exitcode = exitcode

        def is_alive(self):
            return self._alive

        def join(self, timeout=None):
            self._alive = False

        def kill(self):
            self._alive = False

        def close(self):
            pass

    def test_deadline_kill_attributed_to_timeout(self, tmp_path):
        job = Job("t", "hang")
        handle = JobHandle(
            job_uid=job.uid, process=self._HungProcess(),
            timeout_at_monotonic=time.monotonic() - 1, timeout_seconds=2.0,
            job=job, ipc_dir=str(tmp_path), execution_id=_EXECUTION_ID,
        )
        completed = ExecutionChannel(tmp_path).reap_completed([handle])
        assert len(completed) == 1
        result = completed[0][1]
        assert result.success is False
        assert result.result_metadata["error_message"].startswith("TIMEOUT")
        assert result.retry_requested is False, (
            "timeout_is_transient=False 的超时是终局，不烧重试预算"
        )

    def test_join_window_exit_attributed_by_exitcode(self, tmp_path):
        """timeout_at_monotonic 触发 join 后自然退出 → 按 exitcode 归因（NO_IPC_RESULT），不折入 TIMEOUT。"""
        job = Job("t", "natural_exit")
        handle = JobHandle(
            job_uid=job.uid, process=self._DyingDuringJoinProcess(),
            timeout_at_monotonic=time.monotonic() - 1, timeout_seconds=2.0,
            job=job, ipc_dir=str(tmp_path), execution_id=_EXECUTION_ID,
        )
        completed = ExecutionChannel(tmp_path).reap_completed([handle])
        result = completed[0][1]
        assert "NO_IPC_RESULT" in result.result_metadata["error_message"], (
            "join 窗口内自然退出且无结果 → NO_IPC_RESULT 瞬态，非 TIMEOUT"
        )
        assert result.retry_requested is True

    @pytest.mark.parametrize("exitcode", [-9, 1])
    def test_join_window_death_cluster_attributed_symmetrically(
        self, tmp_path, exitcode
    ):
        """对称性基准：join 窗口内信号死亡（-9）/正码崩溃（1）按 exitcode
        分簇归因并维持可重试。

        不变式：timeout_at_monotonic 后 join 窗口内已自然退出的执行体，死亡归因全权
        交 exitcode 分簇，与 timeout_at_monotonic 前同因事件同果——环境故障不得仅因
        跨过 timeout_at_monotonic 就从「可重试」分裂为「零重试直落失败档案」；仅
        kill 后仍存活的执行体才归 TIMEOUT。
        """
        job = Job("t", "cluster_death")
        handle = JobHandle(
            job_uid=job.uid, process=self._DyingDuringJoinProcess(exitcode),
            timeout_at_monotonic=time.monotonic() - 1, timeout_seconds=2.0,
            job=job, ipc_dir=str(tmp_path), execution_id=_EXECUTION_ID,
        )
        completed = ExecutionChannel(tmp_path).reap_completed([handle])
        result = completed[0][1]
        assert result.success is False
        assert f"PROCESS_CRASH_EXITCODE_{exitcode}" in result.result_metadata["error_message"], (
            "join 窗口内死亡必须按 exitcode 分簇归因，而非折入 TIMEOUT"
        )
        assert result.retry_requested is True, "同窗口死亡维持瞬态可重试（对称性）"
        assert result.retry_error

    def test_completed_result_reaped_before_deadline(self, tmp_path):
        """结果已到达且进程仍活 → 直接按结果收割（不等 timeout_at_monotonic）。"""
        job = Job("t", "fast")
        channel = ExecutionChannel(tmp_path)
        channel._results[_EXECUTION_ID] = {
            "status": "success",
            "metadata": {},
            "spawned_jobs": [],
            "resource_suspensions": [],
            "cursor_updates": {},
        }
        handle = JobHandle(
            job_uid=job.uid, process=self._HungProcess(),
            timeout_at_monotonic=time.monotonic() + 60, timeout_seconds=60.0,
            job=job, ipc_dir=str(tmp_path), execution_id=_EXECUTION_ID,
        )
        completed = channel.reap_completed([handle])
        assert [(h.job_uid, r.success) for h, r in completed] == [("t::fast", True)]


class TestFinalizeProcessKillRace:
    def test_kill_race_still_joins_and_closes(self):
        """kill 抛 ProcessLookupError（is_alive 与 kill 之间退出）时 join/close 不跳过。"""

        class RaceProcess:
            def __init__(self):
                self.joined = False
                self.closed = False

            def is_alive(self):
                return True

            def kill(self):
                raise ProcessLookupError("no such process")

            def join(self, timeout=None):
                self.joined = True

            def close(self):
                self.closed = True

        p = RaceProcess()
        ExecutionChannel._finalize_process(p)
        assert p.joined, "join must still be called after kill race (reap zombie)"
        assert p.closed, "close must still be called after kill race"


class TestAbortInFlight:
    class MockProcess:
        def __init__(self):
            self._alive = False
            self.exitcode = 0

        def is_alive(self):
            return self._alive

        def kill(self):
            pass

        def join(self, timeout=None):
            pass

        def close(self):
            pass

    def test_abort_with_completed_and_cancelled(self, tmp_path):
        ipc_dir = tmp_path / "ipc"
        ipc_dir.mkdir()
        channel = ExecutionChannel(ipc_dir, output_roots=tmp_path)
        job_done = Job("task", "done")
        job_pending = Job("task", "pending")
        unfinished = tmp_path / "unfinished"
        unfinished.write_text("partial output")

        channel._results["inc_done"] = {
            "status": "success",
            "metadata": {"done": 1},
        }

        h_done = JobHandle(
            job_uid=job_done.uid, process=self.MockProcess(),
            timeout_at_monotonic=time.monotonic() + 10, timeout_seconds=10, job=job_done,
            ipc_dir=str(ipc_dir), execution_id="inc_done",
        )
        h_pending = JobHandle(
            job_uid=job_pending.uid, process=self.MockProcess(),
            timeout_at_monotonic=time.monotonic() + 10, timeout_seconds=10, job=job_pending,
            ipc_dir=str(ipc_dir), execution_id="inc_pending",
        )

        outcome = channel.abort_in_flight([h_done, h_pending])
        assert len(outcome.completed) == 1
        assert outcome.completed[0][0].job_uid == job_done.uid
        assert outcome.completed[0][1].success is True
        assert outcome.completed[0][1].result_metadata == {"done": 1}

        assert len(outcome.cancelled) == 1
        assert outcome.cancelled[0].job_uid == job_pending.uid
        assert unfinished.exists()

    def test_abort_salvages_signal_written_before_kill(self, tmp_path):
        """杀进程后补排空：「先排空后杀」窗口内终前写入的 suspend 信号捞回。"""
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "salvage")

        class KillWritingProcess:
            """kill() 时写入 suspend 信号的 stub：确定性复现终前写窗口。"""

            def __init__(self):
                self._alive = True

            def is_alive(self):
                return self._alive

            def kill(self):
                self._alive = False
                channel._pending_signals.append((job.uid, "gpu", 30.0))

            def join(self, timeout=None):
                pass

            def close(self):
                pass

        h = JobHandle(
            job_uid=job.uid, process=KillWritingProcess(),
            timeout_at_monotonic=time.monotonic() + 10, timeout_seconds=10, job=job,
            ipc_dir=str(tmp_path), execution_id="inc_salvage",
        )

        outcome = channel.abort_in_flight([h])

        assert [handle.job_uid for handle in outcome.cancelled] == [job.uid]
        assert outcome.salvaged_signals == [(job.uid, "gpu", 30.0)]
        assert channel._pending_signals == []

    def test_abort_reprobe_completed_merges_post_drain_signals(self, tmp_path):
        """重探测完成分支：终前写入的结果与信号都被消费。"""
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "reprobe")

        class FinishOnKillProcess:
            """kill() 时写最终结果 + suspend 信号的 stub。"""

            def __init__(self):
                self._alive = True

            def is_alive(self):
                return self._alive

            def kill(self):
                self._alive = False
                channel._results["inc_reprobe"] = {
                    "status": "success",
                    "metadata": {"done": 1},
                }
                channel._pending_signals.append((job.uid, "api", 5.0))

            def join(self, timeout=None):
                pass

            def close(self):
                pass

        h = JobHandle(
            job_uid=job.uid, process=FinishOnKillProcess(),
            timeout_at_monotonic=time.monotonic() + 10, timeout_seconds=10, job=job,
            ipc_dir=str(tmp_path), execution_id="inc_reprobe",
        )

        outcome = channel.abort_in_flight([h])

        assert [handle.job_uid for handle, _ in outcome.completed] == [job.uid]
        assert outcome.completed[0][1].success is True
        assert outcome.completed[0][1].resource_suspensions == [("api", 5.0)]
        assert outcome.salvaged_signals == []

    def test_abort_interrupted_result_not_treated_as_terminal(self, tmp_path):
        """interrupted 报告按「未完成」处理：不进 completed，走 cancelled。"""
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "interrupted")
        channel._results["inc_interrupted"] = {
            "status": "interrupted",
            "error_message": "WORKER_INTERRUPTED: KeyboardInterrupt",
        }
        h = JobHandle(
            job_uid=job.uid, process=self.MockProcess(),
            timeout_at_monotonic=time.monotonic() + 10, timeout_seconds=10, job=job,
            ipc_dir=str(tmp_path), execution_id="inc_interrupted",
        )

        outcome = channel.abort_in_flight([h])

        assert outcome.completed == [], "被中断报告不得作为终态提交"
        assert [handle.job_uid for handle in outcome.cancelled] == [job.uid]

    def test_abort_empty_handles_returns_empty_outcome(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        outcome = channel.abort_in_flight([])
        assert outcome.completed == []
        assert outcome.cancelled == []
        assert outcome.salvaged_signals == []


class TestCleanupInFlight:
    def test_cleanup_in_flight_kills_and_clears_channel_state(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "residual")
        channel._results[_EXECUTION_ID] = {"status": "success"}
        channel._pending_signals.append((job.uid, "api", 1.0))
        handle = JobHandle(
            job_uid=job.uid, process=TestAbortInFlight.MockProcess(),
            timeout_at_monotonic=time.monotonic() + 10, timeout_seconds=10, job=job,
            ipc_dir=str(tmp_path), execution_id=_EXECUTION_ID,
        )

        channel.cleanup_in_flight([handle])

        assert _EXECUTION_ID not in channel._results
        assert channel._pending_signals == []
