"""jsonutil 与严格 JSON 对象校验单一矩阵测试。

收敛原散落于 jsonutil、payload_intake、result_metadata、Job/TaskResult 模型中的
严格 JSON 校验契约，确保输入、结果元数据、持久化与跨进程边界统一执行同一规则。
"""
from __future__ import annotations

from collections import OrderedDict
import copy
from datetime import datetime
from enum import IntEnum
import io
import json
from pathlib import Path
from typing import Any

import pytest

from tasklite import Job, StateSnapshot, TaskLite, TaskLiteError, TaskResult
from tasklite.models.result import validate_strict_json_object
from tasklite.testing import in_process_workers
from tasklite.utils.jsonutil import dump, dumps, load, loads


class _CustomStr(str):
    pass


class _CustomInt(int):
    pass


class _CustomFloat(float):
    pass


class _CustomList(list):  # type: ignore[type-arg]
    pass


class _CustomDict(dict):  # type: ignore[type-arg]
    pass


class _SampleIntEnum(IntEnum):
    ONE = 1


def _accept_payload(job: Job, ctx: Any) -> TaskResult:
    return TaskResult.success(metadata=job.payload)


def _metadata_handler(job: Job, ctx: Any) -> TaskResult:
    factory = TaskResult.success if job.payload["success"] else TaskResult.failure
    return factory(message=job.payload["message"], metadata=job.payload["metadata"])


class TestLoadsRejectsAsJSONDecodeError:
    """loads 的损坏数据拒绝必须落在 JSONDecodeError。"""

    def test_oversized_int_literal_rejected_as_json_decode_error(self):
        raw = "[" + "9" * 4301 + "]"
        with pytest.raises(json.JSONDecodeError):
            loads(raw)

    def test_normal_ints_and_reject_channels_unchanged(self):
        assert loads("[-1, 0, 4300]") == [-1, 0, 4300]
        with pytest.raises(json.JSONDecodeError):
            loads("NaN")
        with pytest.raises(json.JSONDecodeError):
            loads("1e400")

    def test_large_but_within_limit_int_roundtrip(self):
        big = 10**4200
        assert loads(dumps([big])) == [big]

    def test_overflow_float_rejected(self):
        for bad in ('{"v": 1e400}', '{"v": -1e400}', '{"v": 2e308}', "[1e999]"):
            with pytest.raises(json.JSONDecodeError):
                loads(bad)
        assert loads('{"a": 1.5, "b": 1e10, "c": -0.001}') == {
            "a": 1.5,
            "b": 1e10,
            "c": -0.001,
        }
        with pytest.raises(json.JSONDecodeError):
            loads("[NaN, Infinity]")

    def test_nan_and_infinity_constants_rejected(self):
        for bad in ("NaN", "Infinity", "-Infinity", "[NaN]", "[Infinity, -Infinity]"):
            with pytest.raises(json.JSONDecodeError):
                loads(bad)


class TestDumpsRejectsNonFinite:
    """dumps 强制 allow_nan=False。"""

    def test_dumps_rejects_nan_and_inf(self):
        with pytest.raises(ValueError):
            dumps({"x": float("nan")})
        with pytest.raises(ValueError):
            dumps({"x": float("inf")})
        with pytest.raises(ValueError):
            dumps({"x": float("-inf")})

    def test_dumps_keeps_chinese_readable(self):
        assert dumps({"名": "任务"}) == '{"名": "任务"}'

    def test_roundtrip_preserves_payload(self):
        payload = {"k": [1, 2.5, "中文", None, True], "nested": {"a": {}}}
        assert loads(dumps(payload)) == payload


class TestFileObjectApi:
    """dump/load 文件对象接口与字符串接口同契约。"""

    def test_dump_and_load_roundtrip(self, tmp_path):
        target = tmp_path / "payload.json"
        payload = {"items": [1, 2], "note": "落盘"}
        with open(target, "w", encoding="utf-8") as fp:
            dump(payload, fp)
        with open(target, encoding="utf-8") as fp:
            assert load(fp) == payload

    def test_load_rejects_overflow_float_from_file(self):
        with pytest.raises(json.JSONDecodeError):
            load(io.StringIO('{"v": 1e400}'))

    def test_load_rejects_oversized_int_from_file(self):
        raw = "[" + "9" * 4301 + "]"
        with pytest.raises(json.JSONDecodeError):
            load(io.StringIO(raw))


class TestStrictJsonValidationMatrix:
    """严格 JSON 对象校验单一矩阵：覆盖校验函数、Job、TaskResult 与后端摄入。"""

    @pytest.mark.parametrize(
        "bad_top",
        [
            [],
            ("a", 1),
            "not-a-dict",
            42,
            OrderedDict([("a", 1)]),
            _CustomDict({"a": 1}),
        ],
    )
    def test_top_level_must_be_exact_dict_or_none(self, bad_top: Any):
        assert validate_strict_json_object(None) == {}
        with pytest.raises(TaskLiteError, match="metadata"):
            validate_strict_json_object(bad_top)
        with pytest.raises(TaskLiteError, match="metadata"):
            TaskResult.success(metadata=bad_top)
        with pytest.raises(TaskLiteError, match="payload"):
            Job("t", "id", payload=bad_top)

    @pytest.mark.parametrize("bad_key", [1, None, ("a",), _CustomStr("k"), False])
    def test_keys_must_be_exact_str(self, bad_key: Any):
        with pytest.raises(TaskLiteError, match="keys must be exact str"):
            validate_strict_json_object({bad_key: "v"})
        with pytest.raises(TaskLiteError, match="keys must be exact str"):
            Job("t", "id", payload={"nested": [{bad_key: "v"}]})
        with pytest.raises(TaskLiteError, match="keys must be exact str"):
            TaskResult.success(metadata={bad_key: "v"})  # type: ignore[dict-item]

    def test_lone_surrogate_rejected_in_key_and_value(self):
        with pytest.raises(TaskLiteError, match="UTF-8"):
            validate_strict_json_object({"\ud800": 1})
        with pytest.raises(TaskLiteError, match="UTF-8"):
            validate_strict_json_object({"k": "\ud800"})

    @pytest.mark.parametrize(
        "bad_val",
        [
            (1, 2),
            {1, 2},
            b"raw",
            Path("/tmp/a"),
            datetime.now(),
            object(),
            _CustomStr("s"),
            _CustomInt(1),
            _CustomFloat(1.5),
            _SampleIntEnum.ONE,
            _CustomList([1]),
            _CustomDict({"k": "v"}),
            OrderedDict([("k", "v")]),
            float("nan"),
            float("inf"),
            float("-inf"),
        ],
    )
    def test_rejects_non_json_types_subclasses_and_non_finite_floats(self, bad_val: Any):
        for candidate in ({"bad": bad_val}, {"nested": [bad_val]}):
            with pytest.raises(TaskLiteError):
                validate_strict_json_object(candidate)
            with pytest.raises(TaskLiteError):
                Job("t", "id", payload=candidate)
            with pytest.raises(TaskLiteError):
                TaskResult.retry(metadata=candidate)

    @pytest.mark.parametrize("backend", ["memory", "sqlite"])
    @pytest.mark.parametrize("intake", ["enqueue", "manual"])
    def test_mutated_invalid_payload_rejected_at_pipeline_intake_without_records(
        self, tmp_path, backend: str, intake: str
    ):
        with TaskLite(backend=backend, state_path=tmp_path / "state.db") as pipeline:
            pipeline.register_task("t", _accept_payload)
            for bad_payload in (
                {1: "integer"},
                {1: "integer", "1": "string"},
                {"nested": [{None: "null"}]},
                {"nested": [{"items": (1, 2)}]},
            ):
                job = Job("t", "bad")
                job.payload = bad_payload  # 模拟绕过构造期校验后的就地变异
                with pytest.raises(TaskLiteError, match="payload"):
                    if intake == "enqueue":
                        pipeline.enqueue(job)
                    else:
                        with pipeline.edit(dry_run=False) as tx:
                            tx.add(job, status="succeeded")
            with StateSnapshot.open(pipeline) as snapshot:
                assert list(snapshot.iter_jobs()) == []
                assert list(snapshot.iter_attempts()) == []

    @pytest.mark.parametrize("backend", ["memory", "sqlite"])
    def test_json_payload_is_complete_idempotent_and_isolated(self, tmp_path, backend: str):
        payload = {
            "items": [None, True, False, 0, -7, 1.25, "中文", {"": "empty key"}],
            "object": {},
            "array": [],
        }
        expected = copy.deepcopy(payload)
        with TaskLite(backend=backend, state_path=tmp_path / "state.db") as pipeline:
            pipeline.register_task("t", _accept_payload)
            job = Job("t", "ok", payload=payload)
            assert pipeline.enqueue(job) is True
            assert pipeline.enqueue(job) is False
            assert pipeline.enqueue(Job("t", "ok", payload=expected)) is False
            payload["items"].append("外部变异")
            job.payload["items"].append("Job 变异")
            with StateSnapshot.open(pipeline) as snapshot:
                row = snapshot.get_job(job.uid)
                assert row is not None
                assert row["job"]["payload"] == expected
                row["job"]["payload"]["items"].append("查询变异")
                row_again = snapshot.get_job(job.uid)
                assert row_again is not None and row_again["job"]["payload"] == expected
            with in_process_workers(pipeline):
                pipeline.run(install_signals=False)
            with StateSnapshot.open(pipeline) as snapshot:
                settled = snapshot.get_job(job.uid)
                assert settled is not None
                assert settled["job"]["payload"] == expected
                assert settled["result"] is not None
                assert settled["result"]["metadata"] == expected

    def test_spawn_process_preserves_json_payload_and_metadata_roundtrip(self, tmp_path):
        """保留单次真实 spawn 跨进程序列化端到端验证。"""
        payload = {"items": [None, True, False, 0, -7, 1.25, "中文", {"": "k"}]}
        with TaskLite(backend="sqlite", state_path=tmp_path / "spawn.db") as pipeline:
            pipeline.register_task("t", _accept_payload)
            job = Job("t", "spawn_ok", payload=payload)
            assert pipeline.enqueue(job) is True
            pipeline.run(install_signals=False)
            with StateSnapshot.open(pipeline) as snapshot:
                settled = snapshot.get_job(job.uid)
                assert settled is not None
                assert settled["status"] == "succeeded"
                assert settled["job"]["payload"] == payload
                assert settled["result"]["metadata"] == payload

    @pytest.mark.parametrize("backend", ["memory", "sqlite"])
    def test_manual_job_captures_json_payload_without_attempts(self, tmp_path, backend: str):
        expected = {"items": [None, False, 1, 1.25, "中文", {"": []}]}
        with TaskLite(backend=backend, state_path=tmp_path / "manual.db") as pipeline:
            pipeline.register_task("t", _accept_payload)
            job = Job("t", "manual", payload=expected)
            with pipeline.edit(dry_run=False) as tx:
                tx.add(job, status="succeeded")
            with pipeline.edit(dry_run=False) as tx:
                tx.replace(job.uid, Job("t", "manual", payload=expected), status="succeeded")
            job.payload["items"].clear()
            with StateSnapshot.open(pipeline) as snapshot:
                row = snapshot.get_job(job.uid)
                assert row is not None and row["job"]["payload"] == expected
                assert list(snapshot.iter_attempts()) == []

    @pytest.mark.parametrize("backend", ["memory", "sqlite"])
    @pytest.mark.parametrize("success", [True, False])
    @pytest.mark.parametrize(
        ("message", "annotations"),
        [
            (None, "业务字符串"),
            ("业务说明", {"completion_source": "manual", "note": "业务备注"}),
        ],
    )
    def test_business_keys_do_not_control_execution_records(
        self, tmp_path, backend: str, success: bool, message: str | None, annotations: Any
    ):
        metadata = {
            "annotations": annotations,
            "result": "业务结果",
            "job_snapshot": "业务快照",
            "files": "业务文件",
            "metadata": {"nested": [None, True, 1, "中文"]},
            "error_message": "JOB_DEPENDENCY",
            "error_category": "manual",
            "traceback": "业务堆栈",
            "exitcode": 42,
            "timed_out": True,
            "signal": "业务信号",
            "message": "业务消息键",
            "exception": {"type": "业务异常"},
            "run_id": "业务运行",
            "attempt_no": 999,
            "started_at": "业务开始",
            "settled_at": "业务结束",
            "artifacts": "业务产物",
            "inputs": "业务输入",
            "success_count": 999,
            "last_succeeded_at": "业务时间",
            "last_success_run_id": "业务身份",
            "cleanup_error": "业务清理",
        }
        expected = copy.deepcopy(metadata)
        with TaskLite(backend=backend, state_path=tmp_path / "meta.db") as pipeline:
            pipeline.register_task("t", _metadata_handler)
            job = Job("t", "executed", payload={"success": success, "message": message, "metadata": metadata})
            pipeline.enqueue(job)
            with in_process_workers(pipeline):
                pipeline.run(install_signals=False)

            with StateSnapshot.open(pipeline) as snapshot:
                row = snapshot.get_job(job.uid)
                assert row is not None
                assert row["status"] == ("succeeded" if success else "failed")
                assert row["run_id"] and row["run_id"] != "业务运行"
                assert row["attempt_no"] == 1
                assert row["started_at"] and row["settled_at"]
                assert row["files"] == {}
                assert row["job"]["payload"] == job.payload
                assert row["result"]["metadata"] == expected
                assert row["result"]["message"] == message
                assert row["result"]["exception"] is None
                assert row["result"]["annotations"]["completion_source"] == "execution"
                assert list(snapshot.iter_jobs()) == [row]
                (attempt,) = snapshot.iter_attempts()
                assert attempt["result"] == row["result"]
                row["result"]["metadata"]["metadata"]["nested"].append("查询变异")
                reloaded = snapshot.get_job(job.uid)
                assert reloaded is not None and reloaded["result"]["metadata"] == expected

            for status in ("failed", "succeeded", "pending"):
                with pipeline.edit(dry_run=False, note="人工说明") as tx:
                    if status == "succeeded":
                        tx.replace(job.uid, job, status="succeeded")
                    else:
                        tx.set_status(job.uid, status, recursive=False)
                with StateSnapshot.open(pipeline) as snapshot:
                    row = snapshot.get_job(job.uid)
                    assert row is not None
                    assert row["status"] == status
                    assert row["run_id"] is None and row["attempt_no"] is None
                    assert len(list(snapshot.iter_attempts())) == 1
                    if status == "pending":
                        assert row["result"] is None
                    else:
                        assert row["result"]["metadata"] == {}
                        assert row["result"]["annotations"]["completion_source"] == "manual"
                        assert row["started_at"] is None and row["settled_at"]
                        if status == "succeeded":
                            assert row["result"]["annotations"]["note"] == "人工说明"
                        else:
                            assert row["result"]["message"] == "人工说明"
