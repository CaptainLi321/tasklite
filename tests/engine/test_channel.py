"""v2 执行通道测试：结果协议、退出证据、看门狗、解码防御与中止。

以受控进程桩与内联 worker 包装确定性复现时序（无真实多进程竞速）；
端到端（派发机器/完成机器装配后的 run 主循环）由后续门面阶段承接。
"""

import multiprocessing as mp
import threading
import time
from types import SimpleNamespace

import pytest

from tasklite.engine.channel import (
    ExecutionChannel,
    ExecutionResult,
    WorkerLaunchSpec,
)
from tasklite.engine.types import AuthorizationOutcome, AuthorizationStatus, JobHandle
from tasklite.models.context import JobContext
from tasklite.models.job import Job
from tasklite.models.result import TaskResult
from tasklite.utils.ipc import (
    PIPE_MSG_RESULT,
    WorkerPipeMessage,
    WorkerPipeSender,
    create_worker_reply_pipe,
)

_EXECUTION_ID = "deadbeefdeadbeefdeadbeefdeadbeef.1"


def make_ctx(job: Job) -> JobContext:
    """最小 JobContext 装配（声明默认空）。"""
    return JobContext(job, success_job_uids=set(), failed_job_uids=set(), cursors={})


def make_spec(
    job: Job,
    handler,
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
        timeout_seconds=60.0,
        pipe_sender=pipe_sender,
    )


class TestExecutionChannelBasics:
    def test_duplicate_result_frame_keeps_first_payload(self, tmp_path):
        channel = ExecutionChannel()
        job = Job("t", "duplicate")
        handle = JobHandle(
            job_uid=job.uid,
            process=SimpleNamespace(exitcode=None),
            timeout_at_monotonic=time.monotonic() + 10,
            timeout_seconds=10,
            job=job,
            execution_id=_EXECUTION_ID,
        )
        first = {
            "status": "success",
            "metadata": {"frame": 1},
            "spawned_jobs": [],
            "cursor_updates": {},
        }
        second = {
            "status": "failure",
            "message": "late frame",
            "metadata": {},
            "exception": None,
        }
        for payload in (first, second):
            channel._accept_message(
                handle,
                WorkerPipeMessage(
                    job_uid=job.uid,
                    execution_id=_EXECUTION_ID,
                    kind=PIPE_MSG_RESULT,
                    payload=payload,
                ),
            )

        assert channel._results[_EXECUTION_ID] == first

class _ThreadWorker:
    def __init__(self, *, target, args=(), kwargs=None):
        self._target = target
        self._args = args
        self._kwargs = dict(kwargs or {})
        self.exitcode: int | None = None
        self._thread: threading.Thread | None = None

    def _run(self) -> None:
        try:
            self._target(*self._args, **self._kwargs)
            if self.exitcode is None:
                self.exitcode = 0
        except BaseException:
            if self.exitcode is None:
                self.exitcode = 1

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def join(self, timeout=None) -> None:
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def kill(self) -> None:
        if self.exitcode is None:
            self.exitcode = -9

    def close(self) -> None:
        self._thread = None


class _ThreadSpawnContext:
    def __init__(self) -> None:
        self._ctx = mp.get_context("spawn")
        self.Process = _ThreadWorker

    def Pipe(self, *args, **kwargs):
        return self._ctx.Pipe(*args, **kwargs)


class TestWorkerWrapper:
    """worker 执行通道端到端：显式结果、异常失败与 Pipe 收割。"""

    @staticmethod
    def _run_worker(
        job: Job,
        handler,
        execution_id: str = _EXECUTION_ID,
        *,
        job_context: JobContext | None = None,
    ) -> ExecutionResult:
        channel = ExecutionChannel(process_context=_ThreadSpawnContext())
        try:
            spec = make_spec(
                job,
                handler,
                execution_id=execution_id,
                job_context=job_context,
            )
            handle = channel.spawn(spec)
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                completed = channel.reap_completed([handle])
                if completed:
                    return completed[0][1]
                time.sleep(0.005)
            raise AssertionError("worker did not complete in time")
        finally:
            channel.close()

    def test_worker_writes_success_over_pipe(self):
        job = Job("t", "x")
        res = self._run_worker(job, lambda j, c: TaskResult.success(metadata={"ok": 1}))
        assert res.success is True
        assert res.result_metadata == {"ok": 1}

    def test_worker_error_payloads_sent_over_pipe(self):
        """retry/failure 各状态通道的 payload 均经 Pipe 发送与收割。"""
        def return_retry(job, ctx):
            return TaskResult.retry(message="t")

        def raise_system_exit(job, ctx):
            raise SystemExit(2)

        res_retry = self._run_worker(Job("t", "yretry"), return_retry)
        assert res_retry.retry_requested is True
        assert res_retry.success is False

        res_exit = self._run_worker(Job("t", "yexit"), raise_system_exit)
        assert res_exit.success is False
        assert res_exit.retry_requested is False
        assert res_exit.exception is not None and res_exit.exception["type"] == "SystemExit"

    def test_worker_rejects_missing_execution_id(self):
        """execution_id 缺失 fail-loud，handler 不得执行且不得产出成功结果。"""
        called: list[bool] = []
        channel = ExecutionChannel(process_context=_ThreadSpawnContext())
        handle = None
        try:
            handle = channel.spawn(
                make_spec(
                    Job("t", "x"),
                    lambda j, c: called.append(True) or TaskResult.success(),
                    execution_id=None,
                )
            )
            deadline = time.monotonic() + 5.0
            completed = []
            while time.monotonic() < deadline and not completed:
                completed = channel.reap_completed([handle])
                if not completed:
                    time.sleep(0.005)
            assert len(completed) == 1
            assert completed[0][1].success is False
            assert called == []
        finally:
            for mapping in (
                channel._in_process_senders,
                channel._reply_senders,
            ):
                item = mapping.pop(None, None)  # type: ignore[arg-type]
                if item is not None:
                    item.close()
            recv = channel._receivers.pop(None, None)  # type: ignore[arg-type]
            if recv is not None:
                recv.close(timeout=1.0)
            stop = channel._health_stops.pop(None, None)  # type: ignore[arg-type]
            if stop is not None:
                stop.set()
            channel.close()

    def test_retry_carries_consume_budget(self):
        """retry 结果携带 consume_budget 经 Pipe 传递。"""
        def zero_budget(job, ctx):
            return TaskResult.retry(message="no burn", consume_budget=False)

        def burn_budget(job, ctx):
            return TaskResult.retry(message="burn", consume_budget=True)

        for handler, job_id, want_consume in (
            (zero_budget, "zb", False),
            (burn_budget, "bb", True),
        ):
            res = self._run_worker(Job("h", job_id), handler)
            assert res.retry_requested is True
            assert res.consume_budget is want_consume

    def test_keyboard_interrupt_writes_failure(self):
        def interrupted(job, ctx):
            raise KeyboardInterrupt()

        res = self._run_worker(Job("h", "ki"), interrupted)
        assert res.success is False
        assert res.message == "KeyboardInterrupt"
        assert res.exception is not None and res.exception["type"] == "KeyboardInterrupt"

    def test_unhandled_exception_writes_failure_with_exception_dict(self):
        """未捕获异常统一结算为 failure，记录 type/message/traceback，不自动重试。"""
        res = self._run_worker(
            Job("h", "err"), lambda j, c: (_ for _ in ()).throw(ValueError("boom")),
        )
        assert res.success is False
        assert res.message == "ValueError: boom"
        assert res.exception is not None
        assert res.exception["type"] == "ValueError"
        assert res.exception["message"] == "boom"
        assert "traceback" in res.exception

    def test_pipe_send_failure_exits_without_crash(self, monkeypatch):
        """Pipe 发送结果遇到 OSError 时，worker 不抛未捕获异常退出，通道按无结果失败收割。"""
        def refusing_send(sender_self, payload):
            raise BrokenPipeError("pipe broken")

        monkeypatch.setattr(WorkerPipeSender, "send_result", refusing_send)
        res = self._run_worker(Job("h", "a"), lambda j, c: TaskResult.success())
        assert res.success is False
        assert "without a result" in (res.message or "")


class TestSpawnFailureCleanup:
    def test_submit_start_failure_reraises(self, tmp_path):
        class ExplodingCtx:
            def Process(self, *args, **kwargs):
                raise RuntimeError("process start failed")

        channel = ExecutionChannel(process_context=ExplodingCtx())
        job = Job("t", "j1")
        with pytest.raises(RuntimeError, match="process start failed"):
            channel.spawn(make_spec(job, lambda j, c: TaskResult.success()))

    def test_submit_starts_process(self):
        started = []
        class DummyProcess:
            def start(self):
                started.append(True)
        channel = ExecutionChannel(
            process_context=type("Ctx", (), {"Process": lambda *a, **k: DummyProcess()})()
        )
        job = Job("t", "j1")
        try:
            handle = channel.spawn(make_spec(job, lambda j, c: TaskResult.success()))
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
        res = TestWorkerWrapper._run_worker(
            job, lambda j, c: bad_return
        )
        assert res.success is False
        assert "invalid handler return type: expected TaskResult" in (res.message or "")


_BASE_OK = {
    "status": "success",
    "metadata": {"ok": True},
    "spawned_jobs": [],
    "cursor_updates": {},
}


def _decode(res):
    job = Job("t", "a")
    channel = ExecutionChannel()
    channel._results[_EXECUTION_ID] = res
    handle = JobHandle(
        job_uid=job.uid,
        process=TestDecodeConvergencePaths._DeadProcess(),
        timeout_at_monotonic=time.monotonic() + 10.0,
        timeout_seconds=10.0,
        job=job,
        execution_id=_EXECUTION_ID,
    )
    completed = channel.reap_completed([handle])
    assert len(completed) == 1
    return completed[0][1]


class TestDecodeIpcResultDefense:
    """损坏结果防御分支：任何异常形态收敛为任务级失败，不穿透崩 run。"""

    def test_missing_metadata_key_marks_failed(self):
        res = dict(_BASE_OK)
        del res["metadata"]
        result = _decode(res)
        assert result.success is False
        assert "Invalid worker result: missing metadata" in (result.message or "")

    def test_success_decode_crash_converted_to_corrupt_result(self, monkeypatch):
        """解码异常兜底：success 分支解码/归一化段意外异常 → 转损坏结果。"""
        monkeypatch.setattr(
            "tasklite.engine.channel.validate_strict_json_object",
            lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        result = _decode(dict(_BASE_OK))
        assert result.success is False
        assert (result.message or "").startswith("Invalid worker result: decode failed")

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

    def test_retry_status_keeps_explicit_budget_flag(self):
        result = _decode({
            "status": "retry",
            "message": "x",
            "metadata": {},
            "consume_budget": False,
        })
        assert result.retry_requested is True
        assert result.consume_budget is False
        assert result.message == "x"

    def test_interrupted_status_is_corrupt_failure(self):
        result = _decode({"status": "interrupted", "error_message": "sig"})
        assert result.success is False
        assert result.retry_requested is False
        assert result.result_metadata == {}
        assert "unknown status 'interrupted'" in (result.message or "")

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

    def test_missing_status_marks_failed(self):
        result = _decode({"raw": "not-a-result-dict"})
        assert result.success is False
        assert result.retry_requested is False
        assert result.result_metadata == {}
        assert "missing status" in (result.message or "")

    def test_empty_payload_marks_failed_without_retry(self):
        result = _decode({})
        assert result.success is False
        assert result.retry_requested is False
        assert result.result_metadata == {}
        assert "missing status" in (result.message or "")

    def test_result_payload_rejects_unknown_fields(self):
        result = _decode(dict(_BASE_OK, unexpected_field=[]))
        assert result.success is False
        assert result.retry_requested is False
        assert result.result_metadata == {}
        assert "unsupported fields" in (result.message or "")

    def test_decode_leaves_output_validation_to_completion(self):
        result = _decode(dict(_BASE_OK))
        assert result.success is True


class TestDecodeConvergencePaths:
    """收割路径收敛：损坏结果归一为任务级结果，绝不以 TypeError 崩 run。"""

    _SCALAR_CORRUPT = {
        "status": "success",
        "metadata": {},
        "spawned_jobs": 7,
        "cursor_updates": {},
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
        channel = ExecutionChannel()
        channel._results[_EXECUTION_ID] = dict(self._SCALAR_CORRUPT)
        handle = JobHandle(
            job_uid=job.uid, process=self._DeadProcess(), timeout_at_monotonic=1.0, timeout_seconds=5.0,
            job=job, execution_id=_EXECUTION_ID,
        )
        completed = channel.reap_completed([handle])
        assert len(completed) == 1, "损坏任务必须被收割而非打断收割循环"
        result = completed[0][1]
        assert result.success is False
        assert "invalid spawned_jobs type" in (result.message or "")
        assert _EXECUTION_ID not in channel._results


class TestBuildTerminalFailureDefense:
    """无结果消息的终局构造：超时、崩溃与无结果均保留失败证据。"""

    @staticmethod
    def _handle():
        return JobHandle(
            job_uid="t::a",
            process=SimpleNamespace(exitcode=None),
            timeout_at_monotonic=1.0,
            timeout_seconds=5.0,
            job=Job("t", "a"),
        )

    def test_timeout_marks_failed_without_retry(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=None), self._handle(), is_timeout=True,
        )
        assert result.success is False
        assert result.retry_requested is False
        assert result.result_metadata == {}
        assert "timed out" in (result.message or "")
        assert result.annotations["timed_out"] is True

    def test_signal_death_preserves_evidence(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=-11), self._handle(), is_timeout=False,
        )
        assert result.success is False
        assert result.retry_requested is False
        assert result.result_metadata == {}
        assert "SIGSEGV" in (result.message or "")
        assert result.annotations["signal"] == "SIGSEGV"
        assert result.annotations["exitcode"] == -11

    def test_clean_exit_without_result_preserves_evidence(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=0), self._handle(), is_timeout=False,
        )
        assert result.success is False
        assert result.retry_requested is False
        assert result.result_metadata == {}
        assert "without a result" in (result.message or "")
        assert result.annotations["exitcode"] == 0


class TestWatchdogReap:
    """看门狗：截止时刻的未完成执行统一归因超时。"""

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
            job=job, execution_id=_EXECUTION_ID,
        )
        completed = ExecutionChannel().reap_completed([handle])
        assert len(completed) == 1
        result = completed[0][1]
        assert result.success is False
        assert "timed out" in (result.message or "")
        assert result.retry_requested is False
        assert result.result_metadata == {}

    def test_join_window_exit_remains_timeout(self, tmp_path):
        """截止时刻无结果即归因超时，进程在 join 窗口退出不改变原因。"""
        job = Job("t", "natural_exit")
        handle = JobHandle(
            job_uid=job.uid, process=self._DyingDuringJoinProcess(),
            timeout_at_monotonic=time.monotonic() - 1, timeout_seconds=2.0,
            job=job, execution_id=_EXECUTION_ID,
        )
        completed = ExecutionChannel().reap_completed([handle])
        result = completed[0][1]
        assert "timed out" in (result.message or "")
        assert result.retry_requested is False
        assert result.result_metadata == {}

    @pytest.mark.parametrize("exitcode", [-9, 1])
    def test_join_window_death_does_not_restore_retry(
        self, tmp_path, exitcode
    ):
        """截止后尚无结果时，退出码不恢复隐式重试语义。"""
        job = Job("t", "cluster_death")
        handle = JobHandle(
            job_uid=job.uid, process=self._DyingDuringJoinProcess(exitcode),
            timeout_at_monotonic=time.monotonic() - 1, timeout_seconds=2.0,
            job=job, execution_id=_EXECUTION_ID,
        )
        completed = ExecutionChannel().reap_completed([handle])
        result = completed[0][1]
        assert result.success is False
        assert result.retry_requested is False
        assert result.result_metadata == {}
        assert "timed out" in (result.message or "")

    def test_completed_result_reaped_before_deadline(self, tmp_path):
        """结果已到达且进程仍活 → 直接按结果收割（不等 timeout_at_monotonic）。"""
        job = Job("t", "fast")
        channel = ExecutionChannel()
        channel._results[_EXECUTION_ID] = {
            "status": "success",
            "metadata": {},
            "spawned_jobs": [],
            "cursor_updates": {},
        }
        handle = JobHandle(
            job_uid=job.uid, process=self._HungProcess(),
            timeout_at_monotonic=time.monotonic() + 60, timeout_seconds=60.0,
            job=job, execution_id=_EXECUTION_ID,
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
        channel = ExecutionChannel()
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
            execution_id="inc_done",
        )
        h_pending = JobHandle(
            job_uid=job_pending.uid, process=self.MockProcess(),
            timeout_at_monotonic=time.monotonic() + 10, timeout_seconds=10, job=job_pending,
            execution_id="inc_pending",
        )

        outcome = channel.abort_in_flight([h_done, h_pending])
        assert len(outcome.completed) == 1
        assert outcome.completed[0][0].job_uid == job_done.uid
        assert outcome.completed[0][1].success is True
        assert outcome.completed[0][1].result_metadata == {"done": 1}

        assert len(outcome.cancelled) == 1
        assert outcome.cancelled[0].job_uid == job_pending.uid
        assert unfinished.exists()

    def test_abort_reprobe_consumes_result_written_on_kill(self, tmp_path):
        """重探测完成分支：终前写入的结果仍被消费。"""
        channel = ExecutionChannel()
        job = Job("task", "reprobe")

        class FinishOnKillProcess:
            """kill() 时写最终结果的 stub。"""

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

            def join(self, timeout=None):
                pass

            def close(self):
                pass

        h = JobHandle(
            job_uid=job.uid, process=FinishOnKillProcess(),
            timeout_at_monotonic=time.monotonic() + 10, timeout_seconds=10, job=job,
            execution_id="inc_reprobe",
        )

        outcome = channel.abort_in_flight([h])

        assert [handle.job_uid for handle, _ in outcome.completed] == [job.uid]
        assert outcome.completed[0][1].success is True

    def test_abort_interrupted_status_settles_failure(self, tmp_path):
        """不支持的 interrupted 状态按一次损坏结果失败结算。"""
        channel = ExecutionChannel()
        job = Job("task", "interrupted")
        channel._results["inc_interrupted"] = {
            "status": "interrupted",
            "error_message": "WORKER_INTERRUPTED: KeyboardInterrupt",
        }
        h = JobHandle(
            job_uid=job.uid, process=self.MockProcess(),
            timeout_at_monotonic=time.monotonic() + 10, timeout_seconds=10, job=job,
            execution_id="inc_interrupted",
        )

        outcome = channel.abort_in_flight([h])

        assert [handle.job_uid for handle, _ in outcome.completed] == [job.uid]
        result = outcome.completed[0][1]
        assert result.success is False
        assert result.retry_requested is False
        assert result.result_metadata == {}
        assert "unknown status 'interrupted'" in (result.message or "")
        assert outcome.cancelled == []

    def test_abort_empty_handles_returns_empty_outcome(self, tmp_path):
        channel = ExecutionChannel()
        outcome = channel.abort_in_flight([])
        assert outcome.completed == []
        assert outcome.cancelled == []


class TestCleanupInFlight:
    def test_cleanup_in_flight_kills_and_clears_channel_state(self, tmp_path):
        channel = ExecutionChannel()
        job = Job("task", "residual")
        channel._results[_EXECUTION_ID] = {"status": "success"}
        handle = JobHandle(
            job_uid=job.uid, process=TestAbortInFlight.MockProcess(),
            timeout_at_monotonic=time.monotonic() + 10, timeout_seconds=10, job=job,
            execution_id=_EXECUTION_ID,
        )

        channel.cleanup_in_flight([handle])

        assert _EXECUTION_ID not in channel._results


@pytest.mark.parametrize("status", ["success", "failure", "retry"])
def test_business_error_fields_are_not_framework_evidence(status):
    metadata = {
        "error_message": "业务错误", "traceback": "业务堆栈",
        "exitcode": "业务退出码", "timed_out": "业务超时",
        "signal": "业务信号", "annotations": {"timed_out": True},
    }
    payload = {"status": status, "metadata": metadata}
    if status == "retry":
        payload["consume_budget"] = True
    result = _decode(payload)
    assert result.result_metadata == metadata
    assert result.error == ""
    assert result.traceback is None
    assert result.annotations == {}
    result.message = "实际说明"
    result.exception = {"type": "ValueError", "message": "异常", "traceback": "实际堆栈"}
    assert result.error == "实际说明"
    assert result.traceback == "实际堆栈"


@pytest.mark.parametrize("exitcode", [None, False, 0, 42, -9, -99])
def test_terminal_failure_only_records_observed_exit_evidence(exitcode):
    result = ExecutionChannel._build_terminal_failure(
        SimpleNamespace(exitcode=exitcode), TestBuildTerminalFailureDefense._handle(),
        is_timeout=False,
    )
    assert result.success is False
    assert result.retry_requested is False
    assert result.result_metadata == {}
    assert result.exception is None
    assert "oom_hint" not in result.annotations
    if type(exitcode) is int:
        assert result.annotations["exitcode"] == exitcode
    else:
        assert "exitcode" not in result.annotations
    if type(exitcode) is int and exitcode < 0:
        assert result.annotations["signal"]
    else:
        assert "signal" not in result.annotations


@pytest.mark.parametrize("status", ["success", "failure", "retry"])
@pytest.mark.parametrize("alive", [False, True])
def test_result_releases_reply_pipe_before_worker_exit(status, alive):
    channel = ExecutionChannel()
    job = Job("task", "reply")
    process = TestWatchdogReap._HungProcess()
    process._alive = alive
    process.joined = False

    def join(timeout=None):
        process.joined = True

    process.join = join
    handle = JobHandle(
        job_uid=job.uid, process=process,
        timeout_at_monotonic=time.monotonic() + 60, timeout_seconds=60,
        job=job, execution_id=_EXECUTION_ID,
    )
    reply_receiver, reply_sender = create_worker_reply_pipe(
        job_uid=job.uid, execution_id=_EXECUTION_ID,
    )
    receiver_closes = []
    receiver = SimpleNamespace(
        take_messages=lambda: [], eof=False,
        close=lambda timeout: receiver_closes.append(timeout),
    )
    channel._receivers[_EXECUTION_ID] = receiver
    channel._handles[_EXECUTION_ID] = handle
    channel._reply_senders[_EXECUTION_ID] = reply_sender
    channel._results[_EXECUTION_ID] = {
        "status": status, "metadata": {"trusted": True},
    }
    if status == "retry":
        channel._results[_EXECUTION_ID]["consume_budget"] = True
    try:
        completed = channel.reap_completed([handle])
        assert completed[0][1].result_metadata == {"trusted": True}
        assert completed[0][1].success is (status == "success")
        assert completed[0][1].retry_requested is (status == "retry")
        assert channel._reply_senders == {}
        assert reply_sender._closed
        assert reply_sender._conn.closed
        if alive:
            assert process.is_alive()
            assert not process.joined
            assert channel._lingering_workers == [(_EXECUTION_ID, process, receiver)]
            assert receiver_closes == []
        channel.close()
        channel.close()
        assert channel._lingering_workers == []
        assert channel._reply_senders == {}
    finally:
        channel.close()
        reply_sender.close()
        reply_receiver.close()


@pytest.mark.parametrize("route", ["crash", "timeout", "error", "authorization", "abort", "cleanup", "close"])
def test_terminal_paths_release_reply_pipe(route):
    channel = ExecutionChannel()
    job = Job("task", "terminal")
    process = TestWatchdogReap._HungProcess()
    process._alive = route == "timeout"
    handle = JobHandle(
        job_uid=job.uid, process=process,
        timeout_at_monotonic=time.monotonic() - 1, timeout_seconds=1,
        job=job, execution_id=_EXECUTION_ID,
    )
    reply_receiver, reply_sender = create_worker_reply_pipe(
        job_uid=job.uid, execution_id=_EXECUTION_ID,
    )
    channel._handles[_EXECUTION_ID] = handle
    channel._reply_senders[_EXECUTION_ID] = reply_sender
    try:
        if route == "error":
            error = OSError("broken worker frame")
            channel._errors[_EXECUTION_ID] = error
            with pytest.raises(OSError) as raised:
                channel.reap_completed([handle])
            assert raised.value is error
        elif route == "authorization":
            handle.authorization_outcome = AuthorizationOutcome(
                status=AuthorizationStatus.DENIED, message="未获授权", path="output",
            )
            result, = channel.reap_completed([handle])
            assert result[1].authorization_outcome is handle.authorization_outcome
        elif route == "abort":
            assert channel.abort_in_flight([handle]).cancelled == [handle]
        elif route == "cleanup":
            channel.cleanup_in_flight([handle])
        elif route == "close":
            channel.close()
        else:
            result, = channel.reap_completed([handle])
            assert not result[1].success
        assert channel._reply_senders == {}
        assert reply_sender._closed
        assert reply_sender._conn.closed
        channel.close()
        assert channel._reply_senders == {}
    finally:
        channel.close()
        reply_sender.close()
        reply_receiver.close()
