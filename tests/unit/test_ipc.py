"""Worker 单向管道与主进程双向回复管道通信原语单元测试。

收敛原 utils/test_ipc.py、utils/test_pipe_ipc.py、utils/test_reply_pipe.py：
锁定信封协议、身份绑定、有界队列反压、4 MiB 大帧、半包崩溃隔离与双向响应等待。
"""
from __future__ import annotations

import json
import multiprocessing as mp
import os
import struct
import sys
import threading
import time

import pytest

from tasklite.models.artifact import FileSpec
from tasklite.utils.ipc import (
    PIPE_MSG_DECLARE,
    PIPE_MSG_INPUT,
    PIPE_MSG_RESULT,
    VALID_PIPE_MESSAGE_KINDS,
    WorkerPipeMessage,
    create_worker_pipe,
    create_worker_reply_pipe,
    decode_worker_pipe_message,
)


def _sequential_sender_worker(sender) -> None:
    try:
        sender.send_declare(
            "request",
            "out",
            FileSpec("/tmp/out.txt", role="output", delete_on_failure=True).to_dict(),
        )
        sender.send_result({"status": "success", "result": 42})
    finally:
        sender.close()


def _flood_sender_worker(sender, count: int, payload_size: int) -> None:
    blob = "x" * payload_size
    try:
        for idx in range(count):
            sender.send_declare(
                f"req-{idx}",
                "out",
                {
                    "path": f"/tmp/{idx}_{blob}",
                    "role": "output",
                    "delete_existing": False,
                    "delete_on_failure": False,
                    "delete_on_success": False,
                },
            )
        sender.send_result({"status": "success", "count": count})
    finally:
        sender.close()


def _large_frame_worker(sender, size_bytes: int) -> None:
    try:
        sender.send_result({"status": "success", "blob": "a" * size_bytes})
    finally:
        sender.close()


def _partial_frame_crash_worker(sender) -> None:
    fd = sender._conn.fileno()
    header = struct.pack("!i", 65536)
    os.write(fd, header + b"partial-corrupt!")
    os._exit(1)


def _partial_frame_hold_open_worker(sender, ready_evt, release_evt) -> None:
    fd = sender._conn.fileno()
    header = struct.pack("!i", 65536)
    os.write(fd, header + b"partial-hold")
    ready_evt.set()
    release_evt.wait(timeout=10.0)
    os._exit(0)


class TestWorkerPipeWireProtocol:
    """信封身份绑定、已移除旧消息类型拒绝与 JSON 帧校验。"""

    def test_declare_frame_roundtrips_with_execution_identity(self):
        receiver, sender = create_worker_pipe(
            job_uid="build::one",
            execution_id="run.1",
            run_id="run",
            attempt_no=1,
        )
        try:
            sender.send_declare(
                "req-1",
                "report",
                {
                    "path": "/work/report.txt",
                    "role": "output",
                    "delete_existing": False,
                    "delete_on_failure": False,
                    "delete_on_success": False,
                },
            )
            messages = receiver.drain_to_eof(timeout=1.0)
            assert len(messages) == 1
            message = messages[0]
            assert isinstance(message, WorkerPipeMessage)
            assert message.job_uid == "build::one"
            assert message.execution_id == "run.1"
            assert message.run_id == "run"
            assert message.attempt_no == 1
            assert message.kind == PIPE_MSG_DECLARE
            assert message.payload["name"] == "report"
            assert message.payload["request_id"] == "req-1"
        finally:
            sender.close()
            receiver.close()

    def test_legacy_input_and_suspend_kinds_not_in_wire_protocol(self):
        assert PIPE_MSG_INPUT not in VALID_PIPE_MESSAGE_KINDS
        assert "suspend" not in VALID_PIPE_MESSAGE_KINDS
        receiver, sender = create_worker_pipe(job_uid="build::one", execution_id="run.1")
        try:
            assert not hasattr(sender, "send_input")
            assert not hasattr(sender, "send_suspend")
            for bad_kind in (PIPE_MSG_INPUT, "suspend", "bogus"):
                with pytest.raises(ValueError, match="unknown worker pipe message kind"):
                    sender.send_message(bad_kind, {"path": "/work/input.txt"})
                raw = json.dumps(
                    {
                        "job_uid": "build::one",
                        "execution_id": "run.1",
                        "kind": bad_kind,
                        "payload": {},
                    }
                ).encode("utf-8")
                with pytest.raises(ValueError, match="unknown worker pipe message kind"):
                    decode_worker_pipe_message(
                        raw,
                        expected_job_uid="build::one",
                        expected_execution_id="run.1",
                    )
        finally:
            sender.close()
            receiver.close()

    def test_decode_rejects_bad_frames_and_identity_mismatches(self):
        with pytest.raises(ValueError, match="invalid worker pipe JSON frame"):
            decode_worker_pipe_message(b"{bad json", expected_job_uid="t::1", expected_execution_id="e1")
        with pytest.raises(ValueError, match="must decode to a JSON object"):
            decode_worker_pipe_message(b"[1, 2, 3]", expected_job_uid="t::1", expected_execution_id="e1")

        cases = [
            ({"job_uid": "other::job", "execution_id": "run.2", "kind": PIPE_MSG_DECLARE, "payload": {}}, "job_uid mismatch"),
            ({"job_uid": "build::one", "execution_id": "run.0", "kind": PIPE_MSG_DECLARE, "payload": {}}, "execution_id mismatch"),
            ({"job_uid": "build::one", "execution_id": "run.2", "run_id": "run-old", "attempt_no": 2, "kind": PIPE_MSG_DECLARE, "payload": {}}, "run_id mismatch"),
            ({"job_uid": "build::one", "execution_id": "run.2", "run_id": "run", "attempt_no": 1, "kind": PIPE_MSG_DECLARE, "payload": {}}, "attempt_no mismatch"),
        ]
        for frame, pattern in cases:
            with pytest.raises(ValueError, match=pattern):
                decode_worker_pipe_message(
                    json.dumps(frame).encode("utf-8"),
                    expected_job_uid="build::one",
                    expected_execution_id="run.2",
                    expected_run_id="run",
                    expected_attempt_no=2,
                )

        receiver, sender = create_worker_pipe(job_uid="t::closed", execution_id="exec_closed")
        sender.close()
        with pytest.raises(OSError):
            sender.send_declare("request", "out", FileSpec("/tmp/b", role="output").to_dict())
        receiver.close()


class TestWorkerPipeSpawnTransport:
    """跨进程单向 Pipe 收发、反压、大帧与异常边界。"""

    def test_sequential_delivery_and_idempotent_close(self):
        ctx = mp.get_context("spawn")
        receiver, sender = create_worker_pipe(ctx, job_uid="t::seq", execution_id="exec_seq_1")
        proc = ctx.Process(target=_sequential_sender_worker, args=(sender,))
        proc.start()
        sender.close()

        proc.join(timeout=10.0)
        assert proc.exitcode == 0

        messages = receiver.drain_to_eof(timeout=5.0)
        receiver.close()
        receiver.close()
        sender.close()

        assert len(messages) == 2
        assert all(isinstance(m, WorkerPipeMessage) for m in messages)
        assert [m.kind for m in messages if isinstance(m, WorkerPipeMessage)] == [
            PIPE_MSG_DECLARE,
            PIPE_MSG_RESULT,
        ]
        res_msg = messages[-1]
        assert isinstance(res_msg, WorkerPipeMessage)
        assert res_msg.payload == {"status": "success", "result": 42}

    def test_backpressure_and_resume(self):
        ctx = mp.get_context("spawn")
        receiver, sender = create_worker_pipe(
            ctx,
            job_uid="t::bp",
            execution_id="exec_bp_1",
            max_queued_messages=2,
        )
        proc = ctx.Process(target=_flood_sender_worker, args=(sender, 30, 8192))
        proc.start()
        sender.close()

        collected: list[WorkerPipeMessage | Exception] = []
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            collected.extend(receiver.take_messages())
            if not proc.is_alive() and receiver.eof:
                collected.extend(receiver.drain_to_eof(timeout=2.0))
                break
            time.sleep(0.01)

        proc.join(timeout=5.0)
        receiver.close()

        assert proc.exitcode == 0
        assert len(collected) == 31
        last = collected[-1]
        assert isinstance(last, WorkerPipeMessage)
        assert last.kind == PIPE_MSG_RESULT
        assert last.payload["count"] == 30

    def test_four_mib_large_frame(self):
        ctx = mp.get_context("spawn")
        receiver, sender = create_worker_pipe(ctx, job_uid="t::large", execution_id="exec_large_1")
        target_bytes = 4 * 1024 * 1024
        proc = ctx.Process(target=_large_frame_worker, args=(sender, target_bytes))
        proc.start()
        sender.close()

        messages = receiver.drain_to_eof(timeout=15.0)
        proc.join(timeout=5.0)
        receiver.close()

        assert proc.exitcode == 0
        assert len(messages) == 1
        msg = messages[0]
        assert isinstance(msg, WorkerPipeMessage)
        assert msg.kind == PIPE_MSG_RESULT
        assert len(msg.payload["blob"]) == target_bytes

    def test_kill_sender_when_buffer_full_recovers_cleanly(self):
        ctx = mp.get_context("spawn")
        receiver, sender = create_worker_pipe(
            ctx,
            job_uid="t::kill_full",
            execution_id="exec_kill_full_1",
            max_queued_messages=1,
        )
        proc = ctx.Process(target=_flood_sender_worker, args=(sender, 500, 32768))
        proc.start()
        sender.close()

        time.sleep(0.2)
        assert proc.is_alive()

        proc.kill()
        proc.join(timeout=5.0)
        receiver.close(timeout=3.0)
        assert not receiver._thread.is_alive()

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX pipe fd semantics")
    def test_partial_frame_sender_exit_reports_error_or_eof_without_hang(self):
        ctx = mp.get_context("spawn")
        receiver, sender = create_worker_pipe(ctx, job_uid="t::partial", execution_id="exec_partial_1")
        proc = ctx.Process(target=_partial_frame_crash_worker, args=(sender,))
        proc.start()
        sender.close()

        proc.join(timeout=5.0)
        items = receiver.drain_to_eof(timeout=3.0)
        receiver.close(timeout=3.0)

        assert proc.exitcode == 1
        assert all(isinstance(item, Exception) for item in items)

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX pipe fd semantics")
    def test_close_raises_runtime_error_when_writer_still_alive_on_partial_frame(self):
        ctx = mp.get_context("spawn")
        receiver, sender = create_worker_pipe(ctx, job_uid="t::hold", execution_id="exec_hold_1")
        ready_evt = ctx.Event()
        release_evt = ctx.Event()
        proc = ctx.Process(
            target=_partial_frame_hold_open_worker,
            args=(sender, ready_evt, release_evt),
        )
        proc.start()
        sender.close()

        assert ready_evt.wait(timeout=5.0)
        try:
            with pytest.raises(RuntimeError, match="did not exit"):
                receiver.close(timeout=0.2)
        finally:
            release_evt.set()
            proc.join(timeout=5.0)
            if proc.is_alive():
                proc.kill()
                proc.join(timeout=5.0)
            receiver._thread.join(timeout=3.0)
            try:
                receiver._conn.close()
            except OSError:
                pass
            active_recv_threads = [
                t.name for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")
            ]
            assert not active_recv_threads


class TestWorkerReplyPipe:
    """Worker 与主进程双向响应管道往返、陈旧帧过滤与 EOF 唤醒。"""

    def test_reply_pipe_roundtrip(self):
        ctx = mp.get_context("spawn")
        w_recv, m_send = create_worker_reply_pipe(
            ctx,
            job_uid="job-1",
            execution_id="run-1.1",
            run_id="run-1",
            attempt_no=1,
        )
        try:
            m_send.send_reply({"request_id": "req-123", "status": "ok", "resolved_path": "/tmp/out.csv"})
            reply = w_recv.wait_reply("req-123", timeout=2.0)
            assert reply["status"] == "ok"
            assert reply["resolved_path"] == "/tmp/out.csv"
            assert reply["request_id"] == "req-123"
        finally:
            w_recv.close()
            m_send.close()

    def test_reply_pipe_ignores_stale_identity_and_request_then_accepts_correct_reply(self):
        ctx = mp.get_context("spawn")
        w_recv, m_send = create_worker_reply_pipe(
            ctx,
            job_uid="job-1",
            execution_id="run-1.2",
            run_id="run-1",
            attempt_no=2,
        )
        try:
            original = (m_send.job_uid, m_send.execution_id, m_send.run_id, m_send.attempt_no)

            m_send.job_uid = "job-other"
            m_send.send_reply({"request_id": "req-good", "status": "ok"})
            m_send.job_uid = original[0]

            m_send.execution_id = "run-1.1"
            m_send.send_reply({"request_id": "req-good", "status": "ok"})
            m_send.execution_id = original[1]

            m_send.run_id = "run-0"
            m_send.send_reply({"request_id": "req-good", "status": "ok"})
            m_send.run_id = original[2]

            m_send.attempt_no = 1
            m_send.send_reply({"request_id": "req-good", "status": "ok"})
            m_send.attempt_no = original[3]

            m_send.send_reply({"request_id": "req-stale", "status": "ok", "resolved_path": "/tmp/stale.csv"})
            m_send.send_reply({"request_id": "req-good", "status": "ok", "resolved_path": "/tmp/good.csv"})

            reply = w_recv.wait_reply("req-good", timeout=2.0)
            assert reply["request_id"] == "req-good"
            assert reply["resolved_path"] == "/tmp/good.csv"
        finally:
            w_recv.close()
            m_send.close()

    def test_reply_pipe_default_wait_has_no_private_deadline(self):
        ctx = mp.get_context("spawn")
        w_recv, m_send = create_worker_reply_pipe(
            ctx,
            job_uid="job-1",
            execution_id="run-1.3",
            run_id="run-1",
            attempt_no=3,
        )
        started = threading.Event()
        finished = threading.Event()
        replies = []

        def wait_for_reply():
            started.set()
            replies.append(w_recv.wait_reply("req-wait"))
            finished.set()

        worker = threading.Thread(target=wait_for_reply)
        worker.start()
        try:
            assert started.wait(1.0)
            assert not finished.wait(0.1)
            m_send.send_reply({"request_id": "req-wait", "status": "ok", "resolved_path": "/tmp/late.csv"})
            assert finished.wait(2.0)
            assert replies[0]["resolved_path"] == "/tmp/late.csv"
        finally:
            w_recv.close()
            m_send.close()
            worker.join(timeout=2.0)

    def test_reply_pipe_eof_wakes_default_wait(self):
        ctx = mp.get_context("spawn")
        w_recv, m_send = create_worker_reply_pipe(
            ctx,
            job_uid="job-1",
            execution_id="run-1.4",
            run_id="run-1",
            attempt_no=4,
        )
        errors = []
        finished = threading.Event()

        def wait_for_reply():
            try:
                w_recv.wait_reply("req-eof")
            except OSError as exc:
                errors.append(exc)
            finally:
                finished.set()

        worker = threading.Thread(target=wait_for_reply)
        worker.start()
        try:
            m_send.close()
            assert finished.wait(2.0)
            assert errors and "closed" in str(errors[0])
        finally:
            w_recv.close()
            worker.join(timeout=2.0)
