"""v2 执行通道测试：结果协议、退出证据、看门狗、解码防御与中止。

以受控进程桩与内联 worker 包装确定性复现时序（无真实多进程竞速）；
端到端（派发机器/完成机器装配后的 run 主循环）由后续门面阶段承接。
"""

import json
import time
from datetime import datetime
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
    PIPE_MSG_RESULT,
    WorkerPipeMessage,
    WorkerPipeSender,
    create_worker_pipe,
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

class TestWorkerWrapper:
    """worker 执行体入口：显式结果、异常失败与 Pipe 发送。"""

    @staticmethod
    def _run_worker(
        job: Job,
        handler,
        execution_id: str = _EXECUTION_ID,
        *,
        job_context: JobContext | None = None,
    ) -> dict:
        receiver, sender = create_worker_pipe(job_uid=job.uid, execution_id=execution_id)
        try:
            spec = make_spec(
                job,
                handler,
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
        data = self._run_worker(job, lambda j, c: TaskResult.success(metadata={"ok": 1}))
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
            data = self._run_worker(job, handler)
            assert data["status"] == want_status

    def test_worker_rejects_missing_execution_id(self, tmp_path):
        """execution_id 缺失 fail-loud（fencing 契约破坏不得静默产出无名结果）。"""
        job = Job("t", "x")
        with pytest.raises(RuntimeError, match="execution_id"):
            _worker_process_entry(make_spec(job, lambda j, c: TaskResult.success(), execution_id=None))

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
            res = self._run_worker(job, handler)
            assert res["status"] == "retry"
            assert res["consume_budget"] is want_consume

    def test_keyboard_interrupt_writes_failure(self, tmp_path):
        def interrupted(job, ctx):
            raise KeyboardInterrupt()

        job = Job("h", "ki")
        res = self._run_worker(job, interrupted)
        assert res["status"] == "failure"
        assert res["message"] == "KeyboardInterrupt"
        assert res["exception"]["type"] == "KeyboardInterrupt"

    def test_unhandled_exception_writes_failure_with_exception_dict(self, tmp_path):
        """未捕获异常统一结算为 failure，记录 type/message/traceback，不自动重试。"""
        job = Job("h", "err")
        res = self._run_worker(
            job, lambda j, c: (_ for _ in ()).throw(ValueError("boom")),
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
                job, lambda j, c: (True, {}), pipe_sender=sender
            ))
            assert receiver.drain_to_eof(timeout=3.0) == []
        finally:
            sender.close()
            receiver.close()


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
        data = TestWorkerWrapper._run_worker(
            job, lambda j, c: bad_return
        )
        assert data["status"] == "failure"
        assert "invalid handler return type: expected TaskResult" in data["message"]


_BASE_OK = {
    "status": "success",
    "metadata": {"ok": True},
    "spawned_jobs": [],
    "cursor_updates": {},
}


def _decode(res):
    return _decode_ipc_result(res, Job("t", "a"))


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
        channel = ExecutionChannel(output_roots=tmp_path)
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
