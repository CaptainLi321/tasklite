"""Worker 与主进程双向响应管道往返与执行身份绑定测试。"""
import multiprocessing as mp
import time
import pytest

from tasklite.utils.ipc import (
    PIPE_MSG_DECLARE,
    create_worker_pipe,
    create_worker_reply_pipe,
)


def test_reply_pipe_roundtrip():
    ctx = mp.get_context("spawn")
    w_recv, m_send = create_worker_reply_pipe(ctx, job_uid="job-1", execution_id="run-1.1")
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


def test_reply_pipe_rejects_mismatched_execution_id():
    ctx = mp.get_context("spawn")
    w_recv, m_send = create_worker_reply_pipe(ctx, job_uid="job-1", execution_id="run-1.2")
    try:
        # 发送过期/旧 execution_id 的回复
        m_send.execution_id = "run-1.1"
        m_send.send_reply({
            "request_id": "req-stale",
            "status": "ok",
            "resolved_path": "/tmp/stale.csv",
        })
        with pytest.raises(TimeoutError):
            w_recv.wait_reply("req-stale", timeout=0.2)
    finally:
        w_recv.close()
        m_send.close()


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
