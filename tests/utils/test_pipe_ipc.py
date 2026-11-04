"""瞬时 Worker 管道通信原语单元测试。

锁定单向 multiprocessing.Pipe 信封协议、有界队列反压、4 MiB 大帧传输、
半包崩溃隔离、写端未关闭违约检测与身份/JSON 校验语义。
"""

from __future__ import annotations

import multiprocessing as mp
import os
import struct
import threading
import time

import pytest

from tasklite.exceptions import _ArtifactIOError
from tasklite.models.artifact import ArtifactDeclaration
from tasklite.utils.ipc import (
    PIPE_MSG_INPUT,
    PIPE_MSG_OUTPUT,
    PIPE_MSG_RESULT,
    PIPE_MSG_SUSPEND,
    WorkerPipeMessage,
    create_worker_pipe,
    decode_worker_pipe_message,
)


def _sequential_sender_worker(sender) -> None:
    try:
        sender.send_input("/tmp/in.txt")
        sender.send_output(
            ArtifactDeclaration(
                path="/tmp/out.txt",
                name="out",
                delete_existing=False,
                delete_on_success=False,
                delete_on_failure=True,
                sandbox=False,
            )
        )
        sender.send_suspend("api", 2.5)
        sender.send_result({"status": "success", "result": 42})
    finally:
        sender.close()


def _flood_sender_worker(sender, count: int, payload_size: int) -> None:
    blob = "x" * payload_size
    try:
        for idx in range(count):
            sender.send_message(PIPE_MSG_INPUT, {"path": f"/tmp/{idx}_{blob}"})
        sender.send_result({"status": "success", "count": count})
    finally:
        sender.close()


def _large_frame_worker(sender, size_bytes: int) -> None:
    try:
        sender.send_result({"status": "success", "blob": "a" * size_bytes})
    finally:
        sender.close()


def _partial_frame_crash_worker(sender) -> None:
    # 直接向底层 fd 写入声明 64 KiB 长度的帧头但只写 16 字节负载后 _exit
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


class TestWorkerPipePrimitives:
    """测试单向 Pipe 收发端、反压、大帧与异常边界。"""

    def test_sequential_delivery_and_idempotent_close(self):
        ctx = mp.get_context("spawn")
        receiver, sender = create_worker_pipe(
            ctx,
            job_uid="t::seq",
            execution_id="exec_seq_1",
        )
        proc = ctx.Process(target=_sequential_sender_worker, args=(sender,))
        proc.start()
        sender.close()

        proc.join(timeout=10.0)
        assert proc.exitcode == 0

        messages = receiver.drain_to_eof(timeout=5.0)
        receiver.close()
        receiver.close()
        sender.close()

        assert len(messages) == 4
        assert all(isinstance(m, WorkerPipeMessage) for m in messages)
        kinds = [m.kind for m in messages if isinstance(m, WorkerPipeMessage)]
        assert kinds == [
            PIPE_MSG_INPUT,
            PIPE_MSG_OUTPUT,
            PIPE_MSG_SUSPEND,
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
        receiver, sender = create_worker_pipe(
            ctx,
            job_uid="t::large",
            execution_id="exec_large_1",
        )
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

        # 等待子进程塞满有界队列与 OS 管道缓冲区
        time.sleep(0.2)
        assert proc.is_alive()

        proc.kill()
        proc.join(timeout=5.0)
        receiver.close(timeout=3.0)
        assert not receiver._thread.is_alive()

    def test_partial_frame_sender_exit_reports_error_or_eof_without_hang(self):
        ctx = mp.get_context("spawn")
        receiver, sender = create_worker_pipe(
            ctx,
            job_uid="t::partial",
            execution_id="exec_partial_1",
        )
        proc = ctx.Process(target=_partial_frame_crash_worker, args=(sender,))
        proc.start()
        sender.close()

        proc.join(timeout=5.0)
        items = receiver.drain_to_eof(timeout=3.0)
        receiver.close(timeout=3.0)

        assert proc.exitcode == 1
        assert all(isinstance(item, Exception) for item in items)

    def test_close_raises_runtime_error_when_writer_still_alive_on_partial_frame(self):
        ctx = mp.get_context("spawn")
        receiver, sender = create_worker_pipe(
            ctx,
            job_uid="t::hold",
            execution_id="exec_hold_1",
        )
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

    def test_envelope_validation_and_sender_closed_errors(self):
        with pytest.raises(ValueError, match="invalid worker pipe JSON frame"):
            decode_worker_pipe_message(
                b"{bad json",
                expected_job_uid="t::1",
                expected_execution_id="e1",
            )
        with pytest.raises(ValueError, match="must decode to a JSON object"):
            decode_worker_pipe_message(
                b"[1, 2, 3]",
                expected_job_uid="t::1",
                expected_execution_id="e1",
            )
        with pytest.raises(ValueError, match="job_uid mismatch"):
            decode_worker_pipe_message(
                b'{"job_uid":"t::other","execution_id":"e1","kind":"result","payload":{}}',
                expected_job_uid="t::1",
                expected_execution_id="e1",
            )
        with pytest.raises(ValueError, match="execution_id mismatch"):
            decode_worker_pipe_message(
                b'{"job_uid":"t::1","execution_id":"e2","kind":"result","payload":{}}',
                expected_job_uid="t::1",
                expected_execution_id="e1",
            )
        with pytest.raises(ValueError, match="unknown worker pipe message kind"):
            decode_worker_pipe_message(
                b'{"job_uid":"t::1","execution_id":"e1","kind":"bogus","payload":{}}',
                expected_job_uid="t::1",
                expected_execution_id="e1",
            )

        receiver, sender = create_worker_pipe(
            job_uid="t::closed",
            execution_id="exec_closed",
        )
        sender.close()
        with pytest.raises(_ArtifactIOError):
            sender.send_input("/tmp/a")
        with pytest.raises(_ArtifactIOError):
            sender.send_output(
                ArtifactDeclaration(
                    path="/tmp/b",
                    name="b",
                    delete_existing=False,
                    delete_on_success=False,
                    delete_on_failure=True,
                    sandbox=False,
                )
            )
        receiver.close()

        active_recv_threads = [
            t.name for t in threading.enumerate() if t.name.startswith("tasklite-pipe-recv-")
        ]
        assert not active_recv_threads
