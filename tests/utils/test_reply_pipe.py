"""Worker 与主进程双向响应管道往返与执行身份绑定测试。"""
import multiprocessing as mp
import threading

from tasklite.utils.ipc import (
    PIPE_MSG_DECLARE,
    create_worker_pipe,
    create_worker_reply_pipe,
)


def test_reply_pipe_roundtrip():
    ctx = mp.get_context("spawn")
    w_recv, m_send = create_worker_reply_pipe(
        ctx,
        job_uid="job-1",
        execution_id="run-1.1",
        run_id="run-1",
        attempt_no=1,
    )
    try:
        m_send.send_reply({
            "request_id": "req-123",
            "status": "ok",
            "resolved_path": "/tmp/out.csv",
        })
        reply = w_recv.wait_reply("req-123", timeout=2.0)
        assert reply["status"] == "ok"
        assert reply["resolved_path"] == "/tmp/out.csv"
        assert reply["request_id"] == "req-123"
    finally:
        w_recv.close()
        m_send.close()


def test_reply_pipe_ignores_stale_identity_and_request_then_accepts_correct_reply():
    ctx = mp.get_context("spawn")
    w_recv, m_send = create_worker_reply_pipe(
        ctx,
        job_uid="job-1",
        execution_id="run-1.2",
        run_id="run-1",
        attempt_no=2,
    )
    try:
        original = (
            m_send.job_uid,
            m_send.execution_id,
            m_send.run_id,
            m_send.attempt_no,
        )

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

        m_send.send_reply({
            "request_id": "req-stale",
            "status": "ok",
            "resolved_path": "/tmp/stale.csv",
        })
        m_send.send_reply({
            "request_id": "req-good",
            "status": "ok",
            "resolved_path": "/tmp/good.csv",
        })

        reply = w_recv.wait_reply("req-good", timeout=2.0)
        assert reply["request_id"] == "req-good"
        assert reply["resolved_path"] == "/tmp/good.csv"
    finally:
        w_recv.close()
        m_send.close()


def test_reply_pipe_default_wait_has_no_private_deadline():
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
        m_send.send_reply({
            "request_id": "req-wait",
            "status": "ok",
            "resolved_path": "/tmp/late.csv",
        })
        assert finished.wait(2.0)
        assert replies[0]["resolved_path"] == "/tmp/late.csv"
    finally:
        w_recv.close()
        m_send.close()
        worker.join(timeout=2.0)


def test_reply_pipe_eof_wakes_default_wait():
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


def test_worker_pipe_sender_send_declare():
    ctx = mp.get_context("spawn")
    receiver, sender = create_worker_pipe(ctx, job_uid="job-2", execution_id="run-2.1")
    try:
        sender.send_declare("req-456", "out", {"path": "/tmp/f.txt", "role": "output"})
        msgs = receiver.drain_to_eof(timeout=1.0)
        assert len(msgs) == 1
        msg = msgs[0]
        assert msg.kind == PIPE_MSG_DECLARE
        assert msg.payload["request_id"] == "req-456"
        assert msg.payload["name"] == "out"
        assert msg.payload["spec"]["path"] == "/tmp/f.txt"
    finally:
        sender.close()
        receiver.close()
