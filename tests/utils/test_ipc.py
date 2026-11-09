"""Worker 单向管道信封协议与动态声明传输测试。"""

import json

import pytest

from tasklite.utils.ipc import (
    PIPE_MSG_DECLARE,
    PIPE_MSG_INPUT,
    VALID_PIPE_MESSAGE_KINDS,
    create_worker_pipe,
    decode_worker_pipe_message,
)


def test_declare_frame_roundtrips_with_execution_identity():
    receiver, sender = create_worker_pipe(
        job_uid="build::one",
        execution_id="run.1",
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
        assert message.job_uid == "build::one"
        assert message.execution_id == "run.1"
        assert message.kind == PIPE_MSG_DECLARE
        assert message.payload["name"] == "report"
        assert message.payload["request_id"] == "req-1"
    finally:
        sender.close()
        receiver.close()


def test_legacy_input_kind_is_not_part_of_wire_protocol():
    assert PIPE_MSG_INPUT not in VALID_PIPE_MESSAGE_KINDS

    raw = json.dumps(
        {
            "job_uid": "build::one",
            "execution_id": "run.1",
            "kind": PIPE_MSG_INPUT,
            "payload": {"path": "/work/input.txt"},
        }
    ).encode("utf-8")
    with pytest.raises(ValueError, match="unknown worker pipe message kind"):
        decode_worker_pipe_message(
            raw,
            expected_job_uid="build::one",
            expected_execution_id="run.1",
        )


def test_legacy_input_frame_cannot_be_sent_through_public_sender():
    receiver, sender = create_worker_pipe(
        job_uid="build::one",
        execution_id="run.1",
    )
    try:
        assert not hasattr(sender, "send_input")
        with pytest.raises(ValueError, match="unknown worker pipe message kind"):
            sender.send_message(PIPE_MSG_INPUT, {"path": "/work/input.txt"})
    finally:
        sender.close()
        receiver.close()


def test_decode_rejects_mismatched_job_and_execution_identity():
    raw = json.dumps(
        {
            "job_uid": "other::job",
            "execution_id": "run.1",
            "kind": PIPE_MSG_DECLARE,
            "payload": {},
        }
    ).encode("utf-8")
    with pytest.raises(ValueError, match="job_uid mismatch"):
        decode_worker_pipe_message(
            raw,
            expected_job_uid="build::one",
            expected_execution_id="run.1",
        )

    raw = json.dumps(
        {
            "job_uid": "build::one",
            "execution_id": "run.0",
            "kind": PIPE_MSG_DECLARE,
            "payload": {},
        }
    ).encode("utf-8")
    with pytest.raises(ValueError, match="execution_id mismatch"):
        decode_worker_pipe_message(
            raw,
            expected_job_uid="build::one",
            expected_execution_id="run.1",
        )
