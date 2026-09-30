"""v2 AttemptRecord 轨迹契约测试：词汇表、字段校验与序列化往返。"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from tasklite.v2.models.attempt import ATTEMPT_OUTCOMES, AttemptRecord


def _running_record(**overrides) -> AttemptRecord:
    """派发即插行的标准形态（outcome=running，无收尾时刻/错误）。"""
    defaults = {
        "job_uid": "encode::vid_001",
        "activation_no": 1,
        "attempt_no": 1,
        "incarnation": "run_ab12cd34.7",
        "run_id": "run_ab12cd34",
        "started_at": "2026-05-01T00:00:00+00:00",
        "outcome": "running",
    }
    defaults.update(overrides)
    return AttemptRecord(**defaults)


class TestAttemptRecordCreation:
    """构造与词汇表。"""

    def test_outcome_vocabulary(self):
        assert ATTEMPT_OUTCOMES == (
            "running", "succeeded", "failed", "requeued", "skipped",
        )

    def test_running_record_defaults(self):
        rec = _running_record()
        assert rec.job_uid == "encode::vid_001"
        assert rec.activation_no == 1
        assert rec.attempt_no == 1
        assert rec.incarnation == "run_ab12cd34.7"
        assert rec.run_id == "run_ab12cd34"
        assert rec.outcome == "running"
        assert rec.finished_at is None
        assert rec.error is None
        assert rec.is_running

    def test_all_outcomes_accepted(self):
        for outcome in ATTEMPT_OUTCOMES:
            rec = _running_record(
                outcome=outcome, finished_at="2026-05-01T00:00:05+00:00"
            )
            assert rec.outcome == outcome
            assert rec.is_running == (outcome == "running")

    def test_failed_outcome_carries_error(self):
        rec = _running_record(
            outcome="failed",
            finished_at="2026-05-01T00:00:05+00:00",
            error="MAX_RETRIES_EXCEEDED",
        )
        assert rec.error == "MAX_RETRIES_EXCEEDED"

    def test_frozen_rejects_mutation(self):
        rec = _running_record()
        with pytest.raises(FrozenInstanceError):
            rec.outcome = "succeeded"  # type: ignore[misc]

    def test_equality_and_hash_on_all_fields(self):
        assert _running_record() == _running_record()
        assert hash(_running_record()) == hash(_running_record())
        assert _running_record() != _running_record(attempt_no=2)


class TestAttemptRecordValidation:
    """字段校验矩阵（fail-loud）。"""

    def test_job_uid_matrix(self):
        with pytest.raises(TypeError, match="job_uid must be a str"):
            _running_record(job_uid=123)
        with pytest.raises(ValueError, match="job_uid must be a non-empty str"):
            _running_record(job_uid="")

    def test_counters_matrix(self):
        for field in ("activation_no", "attempt_no"):
            with pytest.raises(TypeError, match=f"{field} must be an int"):
                _running_record(**{field: "2"})
            with pytest.raises(TypeError, match=f"{field} must be an int"):
                _running_record(**{field: True})
            with pytest.raises(ValueError, match=f"{field} must be >= 1"):
                _running_record(**{field: 0})

    def test_identity_strings_matrix(self):
        for field in ("incarnation", "run_id", "started_at"):
            with pytest.raises(TypeError, match=f"{field} must be a str"):
                _running_record(**{field: 42})
            with pytest.raises(ValueError, match=f"{field} must be a non-empty str"):
                _running_record(**{field: ""})

    def test_optional_strings_matrix(self):
        for field in ("finished_at", "error"):
            with pytest.raises(TypeError, match=f"{field} must be a str"):
                _running_record(**{field: 99})
            with pytest.raises(ValueError, match=f"{field} must be a non-empty str"):
                _running_record(**{field: ""})

    def test_outcome_rejected(self):
        """拼错/非字符串 outcome 会静默漏出追溯查询，入口统一 ValueError。"""
        for bad in ("success", "", None, 123):
            with pytest.raises(ValueError, match="outcome must be one of"):
                _running_record(outcome=bad)


class TestAttemptRecordSerialization:
    """to_dict/from_dict 往返与缺键语义。"""

    def test_roundtrip_running(self):
        rec = _running_record()
        assert AttemptRecord.from_dict(rec.to_dict()) == rec

    def test_roundtrip_terminal_with_error(self):
        rec = _running_record(
            activation_no=2,
            attempt_no=3,
            outcome="requeued",
            finished_at="2026-05-01T00:00:05+00:00",
            error="RateLimitHit: 429",
        )
        assert AttemptRecord.from_dict(rec.to_dict()) == rec

    def test_to_dict_key_set(self):
        keys = set(_running_record().to_dict())
        assert keys == {
            "job_uid", "activation_no", "attempt_no", "incarnation",
            "run_id", "started_at", "outcome", "finished_at", "error",
        }

    def test_from_dict_missing_optional_keys_default_none(self):
        data = _running_record().to_dict()
        data.pop("finished_at")
        data.pop("error")
        rec = AttemptRecord.from_dict(data)
        assert rec.finished_at is None
        assert rec.error is None

    def test_from_dict_missing_required_key_raises(self):
        data = _running_record().to_dict()
        for required in ("job_uid", "activation_no", "attempt_no",
                         "incarnation", "run_id", "started_at", "outcome"):
            broken = dict(data)
            broken.pop(required)
            with pytest.raises(KeyError):
                AttemptRecord.from_dict(broken)

    def test_from_dict_revalidates(self):
        """重建走同一校验链：脏数据按同一规则拒绝。"""
        data = _running_record().to_dict()
        data["outcome"] = "vanished"
        with pytest.raises(ValueError, match="outcome must be one of"):
            AttemptRecord.from_dict(data)
