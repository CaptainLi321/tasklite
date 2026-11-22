"""v2 执行通道测试：线性两阶段启动、单一会话状态机、结果协议、看门狗、解码防御与中止。

通过标准 ``WorkerLauncher`` / ``ManagedWorkerHandle`` 接缝与真实 Pipe 契约验证通道行为，
不直接探测或改写通道私有字典。
"""

from __future__ import annotations

import ast
import os
import pathlib
import threading
import time
from collections.abc import Callable
from multiprocessing.connection import Listener
from types import SimpleNamespace
from typing import Any

import pytest

from tasklite.engine.channel import (
    ExecutionChannel,
    ExecutionResult,
    WorkerLaunchSpec,
)
from tasklite.engine.launcher import (
    PosixManagedWorkerHandle,
    PreparedWorker,
    SubprocessWorkerLauncher,
    WindowsManagedWorkerHandle,
    WorkerLauncher,
    _HandshakeAcceptor,
    _WorkerLaunchCancelled,
    _make_ipc_address,
)
from tasklite.engine.types import AuthorizationOutcome, AuthorizationStatus, JobHandle
from tasklite.models.context import JobContext
from tasklite.models.job import Job
from tasklite.models.result import TaskResult
from tasklite.testing import InProcessWorkerLauncher
from tasklite.utils.ipc import (
    PIPE_MSG_RESULT,
    WorkerPipeMessage,
    WorkerPipeSender,
    create_worker_pipe,
    create_worker_reply_pipe,
)

_EXECUTION_ID = "deadbeefdeadbeefdeadbeefdeadbeef.1"
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def make_ctx(job: Job) -> JobContext:
    """最小 JobContext 装配（声明默认空）。"""
    return JobContext(job, success_job_uids=set(), failed_job_uids=set(), cursors={})


def make_spec(
    job: Job,
    handler: Callable[[Job, JobContext], Any],
    execution_id: str | None = _EXECUTION_ID,
    *,
    pipe_sender: WorkerPipeSender | None = None,
    job_context: JobContext | None = None,
    timeout_seconds: float = 60.0,
) -> WorkerLaunchSpec:
    return WorkerLaunchSpec(
        handler=handler,
        job=job,
        job_context=job_context if job_context is not None else make_ctx(job),
        execution_id=execution_id,  # type: ignore[arg-type]
        timeout_seconds=timeout_seconds,
        pipe_sender=pipe_sender,
    )


class _ControlledWorkerHandle:
    """可编程受控句柄（直接实现 ManagedWorkerHandle，并遵循退出/强杀即关写端契约）。"""

    def __init__(
        self,
        *,
        pipe_sender: WorkerPipeSender | None = None,
        reply_receiver: Any = None,
        alive: bool = False,
        exitcode: int | None = 0,
        die_on_join: bool = False,
        on_kill: Callable[[_ControlledWorkerHandle], None] | None = None,
    ) -> None:
        self._pipe_sender = pipe_sender
        self._reply_receiver = reply_receiver
        self._alive = alive
        self._exitcode = exitcode
        self._die_on_join = die_on_join
        self._on_kill = on_kill
        self._was_force_killed = False
        self._closed = False
        self.joined = False
        self.killed = False
        self.closed = False

    def _close_pipes(self) -> None:
        if self._pipe_sender is not None:
            try:
                self._pipe_sender.close()
            except OSError:
                pass
            self._pipe_sender = None
        if self._reply_receiver is not None:
            try:
                self._reply_receiver.close()
            except OSError:
                pass
            self._reply_receiver = None

    @property
    def pid(self) -> int | None:
        return None

    @property
    def exitcode(self) -> int | None:
        return self._exitcode

    @property
    def was_force_killed(self) -> bool:
        return self._was_force_killed

    def is_alive(self) -> bool:
        return self._alive and not self._closed

    def join(self, timeout: float | None = None) -> None:
        self.joined = True
        if self._die_on_join:
            self._alive = False
        if not self._alive:
            self._close_pipes()

    def kill_tree(self) -> None:
        self._was_force_killed = True
        self.killed = True
        if self._on_kill is not None:
            self._on_kill(self)
        self._alive = False
        if self._exitcode is None:
            self._exitcode = -9
        self._close_pipes()

    def kill(self) -> None:
        self.kill_tree()

    def close(self) -> None:
        self._closed = True
        self.closed = True
        self._alive = False
        self._close_pipes()


class _ScriptedLauncher(WorkerLauncher):
    """按脚本向真实 Pipe 写入帧或模拟存活/挂起/强杀窗口的标准 WorkerLauncher。"""

    def __init__(
        self,
        *,
        on_release: Callable[[WorkerLaunchSpec, WorkerPipeSender, _ControlledWorkerHandle], None] | None = None,
        alive: bool = False,
        exitcode: int | None = 0,
        die_on_join: bool = False,
        on_kill: Callable[[WorkerPipeSender, _ControlledWorkerHandle], None] | None = None,
        close_sender_on_release: bool = True,
    ) -> None:
        self._on_release = on_release
        self._alive = alive
        self._exitcode = exitcode
        self._die_on_join = die_on_join
        self._on_kill = on_kill
        self._close_sender_on_release = close_sender_on_release
        self.handles: list[_ControlledWorkerHandle] = []
        self.reply_receivers: list[Any] = []
        self.abort_count = 0

    def prepare(
        self,
        spec: WorkerLaunchSpec,
        *,
        on_message: Callable[[], None] | None,
        cancel_event: threading.Event,
    ) -> PreparedWorker:
        if cancel_event.is_set():
            raise _WorkerLaunchCancelled("cancelled before prepare")
        receiver, sender = create_worker_pipe(
            job_uid=spec.job.uid,
            execution_id=spec.execution_id,
            run_id=spec.run_id or None,
            attempt_no=spec.attempt_no,
            on_message=on_message,
        )
        reply_receiver, reply_sender = create_worker_reply_pipe(
            job_uid=spec.job.uid,
            execution_id=spec.execution_id,
            run_id=spec.run_id or None,
            attempt_no=spec.attempt_no,
        )
        self.reply_receivers.append(reply_receiver)

        def _kill_cb(h: _ControlledWorkerHandle) -> None:
            if self._on_kill is not None:
                self._on_kill(sender, h)

        handle = _ControlledWorkerHandle(
            pipe_sender=sender,
            reply_receiver=reply_receiver,
            alive=self._alive,
            exitcode=self._exitcode,
            die_on_join=self._die_on_join,
            on_kill=_kill_cb if self._on_kill is not None else None,
        )
        self.handles.append(handle)

        def _release() -> None:
            if self._on_release is not None:
                self._on_release(spec, sender, handle)
            if self._close_sender_on_release and not handle.is_alive():
                handle._close_pipes()

        def _record_abort() -> None:
            self.abort_count += 1

        return PreparedWorker(
            handle=handle,
            receiver=receiver,
            reply_sender=reply_sender,
            on_release=_release,
            on_abort=_record_abort,
        )


def _wait_for_reap(
    channel: ExecutionChannel,
    handles: list[JobHandle],
    timeout: float = 5.0,
) -> list[tuple[JobHandle, ExecutionResult]]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        completed = channel.reap_completed(handles)
        if completed:
            return completed
        time.sleep(0.005)
    raise AssertionError("worker did not complete in time")


class TestExecutionChannelBasics:
    def test_duplicate_result_frame_keeps_first_payload(self):
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

        def send_two(_spec, sender, _handle):
            sender.send_result(first)
            sender.send_result(second)

        channel = ExecutionChannel(launcher=_ScriptedLauncher(on_release=send_two))
        try:
            handle = channel.spawn(make_spec(Job("t", "duplicate"), lambda j, c: TaskResult.success()))
            completed = _wait_for_reap(channel, [handle])
            assert len(completed) == 1
            assert completed[0][1].success is True
            assert completed[0][1].result_metadata == {"frame": 1}
            assert not channel.has_active_workers
        finally:
            channel.close()


class TestWorkerWrapper:
    """worker 执行通道端到端：显式结果、异常失败与 Pipe 收割。"""

    @staticmethod
    def _run_worker(
        job: Job,
        handler: Callable[[Job, JobContext], Any],
        execution_id: str = _EXECUTION_ID,
        *,
        job_context: JobContext | None = None,
    ) -> ExecutionResult:
        channel = ExecutionChannel(launcher=InProcessWorkerLauncher())
        try:
            spec = make_spec(
                job,
                handler,
                execution_id=execution_id,
                job_context=job_context,
            )
            handle = channel.spawn(spec)
            completed = _wait_for_reap(channel, [handle])
            return completed[0][1]
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
        """execution_id 缺失 fail-loud，handler 不得执行且通道自动回收会话。"""
        called: list[bool] = []
        channel = ExecutionChannel(launcher=InProcessWorkerLauncher())
        try:
            handle = channel.spawn(
                make_spec(
                    Job("t", "x"),
                    lambda j, c: called.append(True) or TaskResult.success(),
                    execution_id=None,
                )
            )
            completed = _wait_for_reap(channel, [handle])
            assert len(completed) == 1
            assert completed[0][1].success is False
            assert called == []
            assert not channel.has_active_workers
        finally:
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


class TestSpawnFailureAndTwoPhaseContract:
    def test_prepare_failure_reraises_without_leaking_session(self):
        class _ExplodingLauncher(WorkerLauncher):
            def prepare(self, spec, *, on_message, cancel_event):
                raise RuntimeError("process start failed")

        channel = ExecutionChannel(launcher=_ExplodingLauncher())
        job = Job("t", "j1")
        with pytest.raises(RuntimeError, match="process start failed"):
            channel.spawn(make_spec(job, lambda j, c: TaskResult.success()))
        assert not channel.has_active_workers
        assert channel.active_handles() == []

    def test_two_phase_abort_before_release_never_runs_and_cleans_up(self):
        """prepare_spawn 后若在放行前调用 abort_spawn：不得放行执行且零泄漏回收句柄与连接。"""
        released: list[bool] = []
        launcher = _ScriptedLauncher(
            on_release=lambda _spec, _sender, _handle: released.append(True),
            alive=True,
            close_sender_on_release=False,
        )
        channel = ExecutionChannel(launcher=launcher)
        try:
            prepared = channel.prepare_spawn(
                make_spec(Job("t", "j_abort"), lambda j, c: TaskResult.success())
            )
            assert channel.has_active_workers is True
            assert channel.active_handles() == []

            channel.abort_spawn(prepared)
            channel.abort_spawn(prepared)  # 幂等

            assert released == []
            assert launcher.abort_count == 1
            assert launcher.handles[0].killed is True
            assert launcher.handles[0].closed is True
            assert channel.has_active_workers is False
            assert channel.active_handles() == []
        finally:
            channel.close()

    def test_release_failure_aborts_prepared_worker_and_clears_session(self):
        """release_spawn 阶段异常必须自动 abort 并清理会话。"""
        launcher = _ScriptedLauncher(
            on_release=lambda _spec, _sender, _handle: (_ for _ in ()).throw(
                BrokenPipeError("gate send failed")
            ),
            alive=True,
            close_sender_on_release=False,
        )
        channel = ExecutionChannel(launcher=launcher)
        try:
            prepared = channel.prepare_spawn(
                make_spec(Job("t", "j_rel_fail"), lambda j, c: TaskResult.success())
            )
            with pytest.raises(BrokenPipeError, match="gate send failed"):
                channel.release_spawn(prepared)
            assert launcher.abort_count == 1
            assert launcher.handles[0].closed is True
            assert channel.has_active_workers is False
            assert channel.active_handles() == []
        finally:
            channel.close()

    def test_submit_starts_worker_and_registers_active_handle(self):
        started: list[bool] = []
        launcher = _ScriptedLauncher(
            on_release=lambda _spec, _sender, _handle: started.append(True),
            alive=True,
            close_sender_on_release=False,
        )
        channel = ExecutionChannel(launcher=launcher)
        job = Job("t", "j1")
        try:
            handle = channel.spawn(make_spec(job, lambda j, c: TaskResult.success()))
            assert handle is not None
            assert started == [True]
            assert channel.active_handles() == [handle]
        finally:
            channel.close()
        assert not channel.has_active_workers


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
    def test_non_task_result_rejected_as_failure(self, bad_return):
        job = Job("t", "bad")
        res = TestWorkerWrapper._run_worker(job, lambda j, c: bad_return)
        assert res.success is False
        assert "invalid handler return type: expected TaskResult" in (res.message or "")


_BASE_OK = {
    "status": "success",
    "metadata": {"ok": True},
    "spawned_jobs": [],
    "cursor_updates": {},
}


def _decode(res: dict[str, Any]) -> ExecutionResult:
    """通过真实 Pipe + ExecutionChannel.spawn/reap_completed 契约解码单条结果负载。"""
    job = Job("t", "a")
    launcher = _ScriptedLauncher(
        on_release=lambda _spec, sender, _handle: sender.send_result(res),
        alive=False,
        exitcode=0,
    )
    channel = ExecutionChannel(launcher=launcher)
    try:
        handle = channel.spawn(make_spec(job, lambda j, c: TaskResult.success()))
        completed = _wait_for_reap(channel, [handle])
        assert len(completed) == 1
        assert not channel.has_active_workers
        return completed[0][1]
    finally:
        channel.close()


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

    def test_reap_completed_contains_scalar_new_jobs_corruption(self):
        job = Job("t", "a")
        launcher = _ScriptedLauncher(
            on_release=lambda _spec, sender, _handle: sender.send_result(dict(self._SCALAR_CORRUPT)),
            alive=False,
            exitcode=0,
        )
        channel = ExecutionChannel(launcher=launcher)
        try:
            handle = channel.spawn(make_spec(job, lambda j, c: TaskResult.success()))
            completed = _wait_for_reap(channel, [handle])
            assert len(completed) == 1, "损坏任务必须被收割而非打断收割循环"
            result = completed[0][1]
            assert result.success is False
            assert "invalid spawned_jobs type" in (result.message or "")
            assert not channel.has_received_result(_EXECUTION_ID)
            assert not channel.has_active_workers
        finally:
            channel.close()


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

    def test_force_killed_positive_exitcode_records_terminated_and_real_exitcode(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=1, was_force_killed=True),
            self._handle(),
            is_timeout=False,
        )
        assert result.success is False
        assert result.retry_requested is False
        assert "terminated" in (result.message or "")
        assert result.annotations["terminated"] is True
        assert result.annotations["exitcode"] == 1
        assert "signal" not in result.annotations


class TestWatchdogReap:
    """看门狗：截止时刻的未完成执行统一归因超时。"""

    def test_deadline_kill_attributed_to_timeout(self):
        job = Job("t", "hang")
        launcher = _ScriptedLauncher(alive=True, exitcode=None, close_sender_on_release=False)
        channel = ExecutionChannel(launcher=launcher)
        try:
            handle = channel.spawn(
                make_spec(job, lambda j, c: TaskResult.success(), timeout_seconds=-1.0)
            )
            completed = channel.reap_completed([handle])
            assert len(completed) == 1
            result = completed[0][1]
            assert result.success is False
            assert "timed out" in (result.message or "")
            assert result.retry_requested is False
            assert result.result_metadata == {}
            assert not channel.has_active_workers
        finally:
            channel.close()

    def test_join_window_exit_remains_timeout(self):
        """截止时刻无结果即归因超时，进程在 join 窗口退出不改变原因。"""
        job = Job("t", "natural_exit")
        launcher = _ScriptedLauncher(
            alive=True, exitcode=0, die_on_join=True, close_sender_on_release=False
        )
        channel = ExecutionChannel(launcher=launcher)
        try:
            handle = channel.spawn(
                make_spec(job, lambda j, c: TaskResult.success(), timeout_seconds=-1.0)
            )
            completed = channel.reap_completed([handle])
            result = completed[0][1]
            assert "timed out" in (result.message or "")
            assert result.retry_requested is False
            assert result.result_metadata == {}
            assert not channel.has_active_workers
        finally:
            channel.close()

    @pytest.mark.parametrize("exitcode", [-9, 1])
    def test_join_window_death_does_not_restore_retry(self, exitcode):
        """截止后尚无结果时，退出码不恢复隐式重试语义。"""
        job = Job("t", "cluster_death")
        launcher = _ScriptedLauncher(
            alive=True, exitcode=exitcode, die_on_join=True, close_sender_on_release=False
        )
        channel = ExecutionChannel(launcher=launcher)
        try:
            handle = channel.spawn(
                make_spec(job, lambda j, c: TaskResult.success(), timeout_seconds=-1.0)
            )
            completed = channel.reap_completed([handle])
            result = completed[0][1]
            assert result.success is False
            assert result.retry_requested is False
            assert result.result_metadata == {}
            assert "timed out" in (result.message or "")
        finally:
            channel.close()

    def test_completed_result_reaped_before_deadline(self):
        """结果已到达且进程仍活 → 直接按结果收割并转入 lingering（不等 timeout_at_monotonic）。"""
        job = Job("t", "fast")
        launcher = _ScriptedLauncher(
            on_release=lambda _spec, sender, _handle: sender.send_result(dict(_BASE_OK)),
            alive=True,
            exitcode=None,
            close_sender_on_release=False,
        )
        channel = ExecutionChannel(launcher=launcher)
        try:
            handle = channel.spawn(
                make_spec(job, lambda j, c: TaskResult.success(), timeout_seconds=60.0)
            )
            completed = _wait_for_reap(channel, [handle])
            assert [(h.job_uid, r.success) for h, r in completed] == [("t::fast", True)]
            assert channel.active_handles() == []
            assert channel.has_active_workers is True  # lingering 阶段仍保活
            launcher.handles[0]._alive = False
            assert channel.has_active_workers is False
            assert launcher.handles[0].closed is True
        finally:
            channel.close()


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


class TestCleanupInFlight:
    def test_cleanup_in_flight_kills_and_clears_channel_state(self):
        job = Job("task", "residual")
        launcher = _ScriptedLauncher(
            on_release=lambda _spec, sender, _handle: sender.send_result({"status": "success"}),
            alive=True,
            exitcode=0,
            close_sender_on_release=False,
        )
        channel = ExecutionChannel(launcher=launcher)
        try:
            handle = channel.spawn(make_spec(job, lambda j, c: TaskResult.success()))
            channel.cleanup_in_flight([handle])
            assert not channel.has_received_result(_EXECUTION_ID)
            assert channel.active_handles() == []
            assert not channel.has_active_workers
            assert launcher.handles[0].closed is True
        finally:
            channel.close()


@pytest.mark.parametrize("status", ["success", "failure", "retry"])
def test_business_error_fields_are_not_framework_evidence(status):
    metadata = {
        "error_message": "业务错误", "traceback": "业务堆栈",
        "exitcode": "业务退出码", "timed_out": "业务超时",
        "signal": "业务信号", "annotations": {"timed_out": True},
    }
    payload: dict[str, Any] = {"status": status, "metadata": metadata}
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
    payload: dict[str, Any] = {"status": status, "metadata": {"trusted": True}}
    if status == "retry":
        payload["consume_budget"] = True
    launcher = _ScriptedLauncher(
        on_release=lambda _spec, sender, _handle: sender.send_result(payload),
        alive=alive,
        exitcode=None if alive else 0,
        close_sender_on_release=not alive,
    )
    channel = ExecutionChannel(launcher=launcher)
    try:
        handle = channel.spawn(
            make_spec(Job("task", "reply"), lambda j, c: TaskResult.success(), timeout_seconds=60.0)
        )
        reply_sender = channel.get_reply_sender(_EXECUTION_ID)
        assert reply_sender is not None

        completed = _wait_for_reap(channel, [handle])
        assert completed[0][1].result_metadata == {"trusted": True}
        assert completed[0][1].success is (status == "success")
        assert completed[0][1].retry_requested is (status == "retry")
        assert channel.get_reply_sender(_EXECUTION_ID) is None
        assert reply_sender._closed
        assert reply_sender._conn.closed
        assert channel.active_handles() == []

        proc = launcher.handles[0]
        if alive:
            assert proc.is_alive()
            assert not proc.joined
            assert channel.has_active_workers is True
        else:
            assert channel.has_active_workers is False

        channel.close()
        channel.close()
        assert channel.has_active_workers is False
        assert channel.get_reply_sender(_EXECUTION_ID) is None
        assert proc.closed is True
    finally:
        channel.close()


@pytest.mark.parametrize("route", ["crash", "timeout", "error", "authorization", "cleanup", "close"])
def test_terminal_paths_release_reply_pipe_and_session(route):
    def emit_corrupt_frame(_spec, sender, _handle):
        sender._conn.send_bytes(b"{corrupt-json-frame")

    launcher = _ScriptedLauncher(
        on_release=emit_corrupt_frame if route == "error" else None,
        alive=(route == "timeout"),
        exitcode=None if route == "timeout" else 1,
        close_sender_on_release=(route not in ("timeout",)),
    )
    channel = ExecutionChannel(launcher=launcher)
    try:
        handle = channel.spawn(
            make_spec(
                Job("task", "terminal"),
                lambda j, c: TaskResult.success(),
                timeout_seconds=-1.0 if route == "timeout" else 60.0,
            )
        )
        reply_sender = channel.get_reply_sender(_EXECUTION_ID)
        assert reply_sender is not None

        if route == "error":
            with pytest.raises(ValueError):
                _wait_for_reap(channel, [handle])
        elif route == "authorization":
            expected_outcome = AuthorizationOutcome(
                status=AuthorizationStatus.DENIED, message="未获授权", path="output",
            )
            channel.set_authorization_queries(
                get_outcome=lambda h: expected_outcome if h is handle else None
            )
            result, = channel.reap_completed([handle])
            assert result[1].authorization_outcome is expected_outcome
        elif route == "cleanup":
            channel.cleanup_in_flight([handle])
        elif route == "close":
            channel.close()
        else:
            result, = channel.reap_completed([handle])
            assert not result[1].success

        assert channel.get_reply_sender(_EXECUTION_ID) is None
        assert channel.active_handles() == []
        assert channel.has_active_workers is False
        assert reply_sender._closed
        assert reply_sender._conn.closed
        channel.close()
        assert channel.has_active_workers is False
    finally:
        channel.close()


def test_collect_outcome_authorization_clears_active_session():
    """直接经 _collect_outcome 处理 authorization_outcome 时不得遗留活跃会话或句柄引用。"""
    launcher = _ScriptedLauncher(alive=True, exitcode=None, close_sender_on_release=False)
    channel = ExecutionChannel(launcher=launcher)
    try:
        handle = channel.spawn(make_spec(Job("task", "auth_collect"), lambda j, c: TaskResult.success()))
        expected_outcome = AuthorizationOutcome(
            status=AuthorizationStatus.STOPPED, message="stopped", path="out",
        )
        channel.set_authorization_queries(
            get_outcome=lambda h: expected_outcome if h is handle else None
        )
        outcome = channel._collect_outcome(handle)
        assert outcome.authorization_outcome is expected_outcome
        assert channel.active_handles() == []
        assert channel.get_reply_sender(_EXECUTION_ID) is None
        assert channel.has_active_workers is False
        assert launcher.handles[0].closed is True
    finally:
        channel.close()


def test_posix_managed_worker_handle_kill_tree_calls_killpg_before_wait(monkeypatch):
    """POSIX 进程组强杀必须严格先 os.killpg(pgid, SIGKILL)，再在 join() 中 proc.wait()。"""
    import tasklite.engine.launcher as launcher_module

    events: list[tuple[str, Any]] = []

    class _FakePopen:
        pid = 43210
        returncode: int | None = None

        def poll(self):
            events.append(("poll", None))
            return self.returncode

        def wait(self, timeout=None):
            events.append(("wait", timeout))
            self.returncode = -9
            return self.returncode

        def kill(self):
            events.append(("kill", None))

    def fake_killpg(pgid, sig):
        events.append(("killpg", pgid))

    monkeypatch.setattr(launcher_module.os, "killpg", fake_killpg, raising=False)
    handle = PosixManagedWorkerHandle(_FakePopen())  # type: ignore[arg-type]
    handle.kill_tree()
    handle.join(timeout=2.0)
    handle.close()

    assert handle.was_force_killed is True
    assert handle.exitcode == -9
    assert events == [("killpg", 43210), ("wait", 2.0)]


def test_windows_managed_worker_handle_keeps_hjob_until_close():
    """WindowsManagedWorkerHandle 在 kill_tree 后保活 hJob，仅在 close() 时幂等关闭。"""
    events: list[tuple[str, int]] = []

    class _FakeKernel:
        def TerminateJobObject(self, h_job: int, code: int) -> int:
            events.append(("terminate_job", h_job))
            return 1

        def CloseHandle(self, h_job: int) -> int:
            events.append(("close_handle", h_job))
            return 1

    class _FakePopen:
        pid = 9001
        returncode: int | None = None

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            self.returncode = 1
            return 1

        def kill(self):
            self.returncode = 1

    handle = WindowsManagedWorkerHandle(_FakePopen(), [101, 202], _FakeKernel())  # type: ignore[arg-type]
    handle.kill_tree()
    assert handle.was_force_killed is True
    assert events == [("terminate_job", 101), ("terminate_job", 202)]

    handle.join(timeout=1.0)
    assert handle.exitcode == 1
    assert ("close_handle", 101) not in events

    handle.close()
    handle.close()  # 幂等
    assert events == [
        ("terminate_job", 101),
        ("terminate_job", 202),
        ("close_handle", 101),
        ("close_handle", 202),
    ]


def test_cancel_pending_launches_wakes_handshake_and_cleans_up():
    """在冷启动/握手阻塞期间调用 cancel_pending_launches() 必须即时唤醒并零残留。"""
    entered = threading.Event()
    cleaned = threading.Event()

    class _BlockingHandshakeLauncher(WorkerLauncher):
        def prepare(self, spec, *, on_message, cancel_event):
            entered.set()
            assert cancel_event.wait(timeout=5.0)
            cleaned.set()
            raise _WorkerLaunchCancelled("cancelled during handshake")

    channel = ExecutionChannel(launcher=_BlockingHandshakeLauncher())
    errors: list[BaseException] = []

    def do_spawn() -> None:
        try:
            channel.spawn(make_spec(Job("t", "pending"), lambda j, c: TaskResult.success()))
        except BaseException as exc:
            errors.append(exc)

    t = threading.Thread(target=do_spawn, daemon=True)
    t.start()
    assert entered.wait(timeout=5.0)
    assert channel.has_active_workers is True
    channel.cancel_pending_launches()
    t.join(timeout=5.0)

    assert cleaned.is_set()
    assert len(errors) == 1 and isinstance(errors[0], _WorkerLaunchCancelled)
    assert channel.has_active_workers is False


def test_handshake_acceptor_cancel_and_join_never_deadlocks_after_done():
    """_HandshakeAcceptor 在 _done 已置位或阻塞于 accept() 时 cancel_and_join 均不死锁。"""
    addr_done, family_done = _make_ipc_address()
    authkey_done = os.urandom(32)
    listener_done = Listener(address=addr_done, family=family_done, authkey=authkey_done)
    try:
        acceptor_done = _HandshakeAcceptor(
            listener_done,
            address=str(listener_done.address),
            family=family_done,
            authkey=authkey_done,
        )
        acceptor_done._done.set()
        acceptor_done._thread = SimpleNamespace(  # type: ignore[assignment]
            is_alive=lambda: True,
            join=lambda timeout=None: None,
        )
        t0 = time.monotonic()
        acceptor_done.cancel_and_join(timeout=1.0)
        assert time.monotonic() - t0 < 0.5
    finally:
        listener_done.close()
        SubprocessWorkerLauncher._cleanup_socket_file(addr_done, family_done)

    addr_live, family_live = _make_ipc_address()
    authkey_live = os.urandom(32)
    listener_live = Listener(address=addr_live, family=family_live, authkey=authkey_live)
    try:
        acceptor_live = _HandshakeAcceptor(
            listener_live,
            address=str(listener_live.address),
            family=family_live,
            authkey=authkey_live,
        )
        acceptor_live.start()
        time.sleep(0.05)
        t1 = time.monotonic()
        acceptor_live.cancel_and_join(timeout=2.0)
        assert time.monotonic() - t1 < 1.5
        assert acceptor_live._done.is_set()
        assert not acceptor_live._thread.is_alive()
    finally:
        listener_live.close()
        SubprocessWorkerLauncher._cleanup_socket_file(addr_live, family_live)


def _collect_imported_modules(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                imported.add(node.module or "")
            else:
                parts = path.relative_to(_REPO_ROOT).with_suffix("").parts[:-1]
                base = ".".join(parts[: len(parts) - (node.level - 1)])
                imported.add(f"{base}.{node.module}" if node.module else base)
    return imported


def test_worker_bootstrap_and_runtime_do_not_import_parent_scheduler_tree():
    """纯 worker 侧模块严禁导入 ExecutionChannel 或父进程调度树，launcher 严禁反向导入 channel 或测试双体。"""
    forbidden_for_worker = {
        "tasklite.engine.channel",
        "tasklite.engine.launcher",
        "tasklite.engine.dispatch",
        "tasklite.engine.runtime",
        "tasklite.engine.completion",
        "tasklite.engine.recovery",
        "tasklite.engine.scheduler",
        "tasklite.engine.store",
        "tasklite.engine.governor",
        "tasklite.pipeline",
        "tasklite.testing",
    }
    for rel_path in (
        "tasklite/engine/_worker_bootstrap.py",
        "tasklite/engine/_worker_runtime.py",
    ):
        imports = _collect_imported_modules(_REPO_ROOT / rel_path)
        leaked = sorted(imports & forbidden_for_worker)
        assert not leaked, f"{rel_path} 不得导入父进程模块: {leaked}"

    launcher_path = _REPO_ROOT / "tasklite/engine/launcher.py"
    launcher_imports = _collect_imported_modules(launcher_path)
    assert "tasklite.engine.channel" not in launcher_imports
    assert "tasklite.testing" not in launcher_imports
    launcher_src = launcher_path.read_text(encoding="utf-8")
    assert "InProcessWorkerLauncher" not in launcher_src
    assert "_InProcessWorker" not in launcher_src
