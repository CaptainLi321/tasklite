"""Task 规格、TaskRegistry、Job 模型与快照序列化边界单元测试。"""
from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any

import pytest

from tasklite.exceptions import TaskLiteError
from tasklite.models.artifact import FileSpec, resolve_job_files_config
from tasklite.models.job import (
    BUILTIN_TRIGGERS,
    Job,
    inject_worker_resource,
)
from tasklite.models.task import Task, TaskRegistry
from tasklite.utils.encoding import make_job_uid, parse_job_uid


def _handler(job: Any, ctx: Any) -> bool:
    return True


class TestTaskCreationAndValidation:
    """Task 规格构造、frozen 不可变性与入口校验。"""

    def test_create_with_minimal_and_all_fields(self):
        task = Task(task_type="download", handler=_handler)
        assert task.task_type == "download"
        assert task.handler is _handler
        assert task.resources == {}
        assert task.max_retries_per_run == 3
        assert task.timeout_seconds == 3600
        assert task.trigger == "once"
        assert not hasattr(task, "payload_schema")
        assert not hasattr(task, "timeout_is_transient")

        full = Task(
            task_type="encode",
            handler=_handler,
            resources={"gpu": 1.0},
            max_retries_per_run=5,
            timeout_seconds=7200,
            trigger="always",
        )
        assert full.resources == {"gpu": 1.0}
        assert full.max_retries_per_run == 5
        assert full.timeout_seconds == 7200
        assert full.trigger == "always"

    def test_config_fields_keyword_only_and_removed_fields_rejected(self):
        with pytest.raises(TypeError):
            Task("t", _handler, {"gpu": 1.0})  # type: ignore[misc]
        with pytest.raises(TypeError):
            Task("t", _handler, None, 3, 3600, "once")  # type: ignore[misc]
        for kw in (
            {"payload_schema": dict},
            {"max_retries": 3},
            {"timeout_is_transient": True},
            {"payload": {}},
            {"depends_on": []},
            {"files": {}},
        ):
            with pytest.raises(TypeError):
                Task("t", _handler, **kw)  # type: ignore[arg-type]

    def test_frozen_isolation_and_equality(self):
        res = {"api": 1.0}
        task = Task(task_type="t", handler=_handler, resources=res)
        res["api"] = 99.0
        assert task.resources == {"api": 1.0}
        assert Task(task_type="t", handler=_handler, resources=None).resources == {}
        with pytest.raises(FrozenInstanceError):
            task.max_retries_per_run = 9  # type: ignore[misc]
        assert Task(task_type="t", handler=_handler) == Task(task_type="t", handler=_handler)
        assert task != Task(task_type="u", handler=_handler)

    def test_task_validation_errors(self):
        with pytest.raises(TaskLiteError, match="task_type must be a str"):
            Task(task_type=123, handler=_handler)  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="non-empty str"):
            Task(task_type="", handler=_handler)
        assert Task(task_type="a::b", handler=_handler).task_type == "a::b"
        with pytest.raises(TaskLiteError, match="handler must be callable"):
            Task(task_type="t", handler="not_callable")  # type: ignore[arg-type]

        with pytest.raises(TaskLiteError, match="max_retries_per_run must be an int"):
            Task(task_type="t", handler=_handler, max_retries_per_run="3")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="max_retries_per_run must be an int"):
            Task(task_type="t", handler=_handler, max_retries_per_run=True)
        with pytest.raises(TaskLiteError, match="max_retries_per_run must be >= 0"):
            Task(task_type="t", handler=_handler, max_retries_per_run=-1)
        assert Task(task_type="t", handler=_handler, max_retries_per_run=0).max_retries_per_run == 0

        with pytest.raises(TaskLiteError, match="timeout_seconds must be a number"):
            Task(task_type="t", handler=_handler, timeout_seconds="60")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="timeout_seconds must be a number"):
            Task(task_type="t", handler=_handler, timeout_seconds=True)
        for bad in (0, -1, float("nan"), float("inf"), float("-inf"), 10**400):
            with pytest.raises(TaskLiteError, match="timeout_seconds must be finite and > 0"):
                Task(task_type="t", handler=_handler, timeout_seconds=bad)
        assert Task(task_type="t", handler=_handler, timeout_seconds=1.5).timeout_seconds == 1.5

        for bad_res in (
            {"cpu": True},
            {"cpu": "high"},
            {"cpu": float("nan")},
            {"cpu": float("inf")},
            {"cpu": 10**400},
            {"cpu": -1.0},
            ["cpu", 1.0],
            {1: 2.0},
            {"": 2.0},
        ):
            with pytest.raises(TaskLiteError):
                Task(task_type="t", handler=_handler, resources=bad_res)  # type: ignore[arg-type]


class TestTaskRegistry:
    """TaskRegistry 注册/查询契约。"""

    def test_register_lookup_contains_and_errors(self):
        reg = TaskRegistry()
        first = Task(task_type="a", handler=_handler)
        second = Task(task_type="b", handler=_handler, max_retries_per_run=0)
        reg.register(first)
        reg.register(second)
        assert reg.lookup("a") is first
        assert reg.lookup("b") is second
        assert "a" in reg and "b" in reg and "c" not in reg and 123 not in reg
        assert reg.task_types() == ("a", "b")

        with pytest.raises(TaskLiteError, match="already registered"):
            reg.register(Task(task_type="a", handler=_handler))
        with pytest.raises(TaskLiteError, match="unknown task_type"):
            reg.lookup("ghost")
        with pytest.raises(TaskLiteError, match="task_type must be a str"):
            reg.lookup(123)  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="must be a Task instance"):
            reg.register(("scan", _handler))  # type: ignore[arg-type]


class TestJobCreationAndIdentity:
    """Job 构造、默认值、只读位与身份转义。"""

    def test_minimal_and_full_construction(self):
        job = Job("scan", "scan_001")
        assert job.task_type == "scan"
        assert job.job_id == "scan_001"
        assert job.payload == {}
        assert job.depends_on == []
        assert job.files == {}
        assert job.resources is None
        assert job.max_retries_per_run is None
        assert job.timeout_seconds is None
        assert job.trigger is None
        assert job.attempt_no == 1
        for removed in ("max_retries", "timeout_is_transient", "activation_no", "runtime", "first_enqueued_at"):
            assert not hasattr(job, removed)

        with pytest.raises(AttributeError):
            job.attempt_no = 7  # type: ignore[misc]

        out_spec = FileSpec("tmp/1.jpg", role="output")
        full = Job(
            task_type="download",
            job_id="img_001",
            payload={"url": "https://example.com/1.jpg"},
            depends_on=["parent_job"],
            files={"output": out_spec},
            resources={"api": 1.0, "disk": 2.5},
            max_retries_per_run=5,
            timeout_seconds=7200,
            trigger="until_success",
        )
        assert full.payload == {"url": "https://example.com/1.jpg"}
        assert full.depends_on == ["parent_job"]
        assert full.files == {"output": out_spec}
        assert full.resources == {"api": 1.0, "disk": 2.5}
        assert full.max_retries_per_run == 5
        assert full.timeout_seconds == 7200
        assert full.trigger == "until_success"

    def test_defaults_copies_and_keyword_only_contract(self):
        assert Job("fetch", "j1", payload=None).payload == {}
        assert Job("fetch", "j1", files=None).files == {}
        assert Job("fetch", "j1", resources=None).resources is None
        assert Job("fetch", "j1", depends_on=None).depends_on == []

        spec = FileSpec("out/1.jpg", role="output")
        src_files = {"out": spec}
        res = {"gpu": 1.0}
        nested_list = [1, 2, 3]
        job = Job("fetch", "j1", payload={"items": nested_list}, files=src_files, resources=res)
        src_files["extra"] = FileSpec("out/2.jpg", role="output")
        res["gpu"] = 2.0
        nested_list.append(4)
        assert job.files == {"out": spec}
        assert job.resources == {"gpu": 1.0}
        assert job.payload["items"] == [1, 2, 3]

        with pytest.raises(TypeError, match="positional argument"):
            Job("t", "id", {"a": 1})  # type: ignore[misc]
        for kw in (
            {"attempt_no": 2},
            {"activation_no": 2},
            {"first_enqueued_at": "2026-01-01T00:00:00+00:00"},
            {"runtime": {}},
            {"max_retries": 3},
            {"timeout_is_transient": True},
        ):
            with pytest.raises(TypeError):
                Job("t", "id", **kw)  # type: ignore[arg-type]

        float_job = Job("t", "id", timeout_seconds=1.5)
        assert float_job.timeout_seconds == 1.5 and isinstance(float_job.timeout_seconds, float)

    def test_identity_validation_uid_and_equality(self):
        for bad in (42, 3.14, True, b"jid"):
            with pytest.raises(TaskLiteError, match="job_id must be a str"):
                Job("download", bad)  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="task_type must be a str"):
            Job(123, "id")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="task_type must be a non-empty str"):
            Job("", "id")
        with pytest.raises(TaskLiteError, match="job_id must be a non-empty str"):
            Job("type", "")

        job_t = Job("a::b", "c")
        assert job_t.uid == make_job_uid("a::b", "c") == "a%3A%3Ab::c"
        assert parse_job_uid(job_t.uid) == ("a::b", "c")

        job_id = Job("a", "b::c")
        assert job_id.uid == make_job_uid("a", "b::c") == "a::b%3A%3Ac"
        assert parse_job_uid(job_id.uid) == ("a", "b::c")

        long_id = "x" * 400
        assert Job("t", long_id).uid == f"t::{long_id}"
        assert Job("tag", "日本语_テスト").uid == "tag::日本语_テスト"
        assert Job("path", "a/b/c").uid == "path::a/b/c"

        a = Job("t", "j", payload={"x": 1})
        b = Job("t", "j", payload={"x": 2}, timeout_seconds=99)
        assert a == b and hash(a) == hash(b)
        assert a != Job("t", "other")
        assert a != "t::j"


class TestJobFieldValidation:
    """Job 字段校验（resources / retries / timeout / files / depends_on / trigger / attempt_no）。"""

    def test_resources_validation(self):
        with pytest.raises(TaskLiteError, match="resources must be a dict"):
            Job("t", "id", resources=["cpu", 1.0])  # type: ignore[arg-type]
        for bad_res in (
            {"cpu": True},
            {"cpu": float("nan")},
            {"cpu": float("inf")},
            {"cpu": -1.0},
            {"cpu": 10**400},
            {"cpu": "high"},
            {1: 2.0},
            {"": 2.0},
        ):
            with pytest.raises(TaskLiteError):
                Job("t", "id", resources=bad_res)  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError):
            Job.from_dict({"task_type": "t", "job_id": "id", "resources": {1: 2.0}})

    def test_retries_and_timeout_validation(self):
        with pytest.raises(TaskLiteError, match="max_retries_per_run must be >= 0"):
            Job("t", "id", max_retries_per_run=-1)
        with pytest.raises(TaskLiteError, match="max_retries_per_run must be an int"):
            Job("t", "id", max_retries_per_run="3")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="max_retries_per_run must be an int"):
            Job("t", "id", max_retries_per_run=True)
        assert Job("t", "id", max_retries_per_run=0).max_retries_per_run == 0

        with pytest.raises(TaskLiteError, match="timeout_seconds must be a number"):
            Job("t", "id", timeout_seconds="60")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="timeout_seconds must be a number"):
            Job("t", "id", timeout_seconds=True)
        for bad in (0, -1, float("nan"), float("inf"), float("-inf"), 10**400):
            with pytest.raises(TaskLiteError, match="timeout_seconds must be finite and > 0"):
                Job("t", "id", timeout_seconds=bad)

    def test_files_depends_on_and_trigger_validation(self):
        spec = FileSpec("a.txt", role="output")
        with pytest.raises(TaskLiteError, match="files must be a dict"):
            Job("t", "id", files=["a"])  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="file key in files must be a non-empty str"):
            Job("t", "id", files={"": spec})
        with pytest.raises(TaskLiteError, match="file key in files must be a non-empty str"):
            Job("t", "id", files={1: spec})  # type: ignore[dict-item]
        with pytest.raises(TaskLiteError, match="file spec in files must be a FileSpec"):
            Job("t", "id", files={"out": "a.txt"})  # type: ignore[dict-item]
        with pytest.raises(TaskLiteError, match="file spec in files must be a FileSpec"):
            Job("t", "id", files={"out": spec.to_dict()})  # type: ignore[dict-item]

        with pytest.raises(TaskLiteError, match="depends_on must be a list of strings"):
            Job("t", "j1", depends_on="parent::id")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="depends_on must contain only strings"):
            Job("t", "j1", depends_on=[123])  # type: ignore[list-item]
        with pytest.raises(TaskLiteError, match="depends_on must contain only strings"):
            Job("t", "j1", depends_on=["a::b", None])  # type: ignore[list-item]
        assert Job("t", "id", depends_on=[""]).depends_on == [""]
        assert "t::j1" in Job("t", "j1", depends_on=["t::j1"]).depends_on

        assert BUILTIN_TRIGGERS == ("once", "until_success", "always", "on_input_change")
        for value in BUILTIN_TRIGGERS:
            assert Job("t", "id", trigger=value).trigger == value
        assert Job("t", "id").trigger is None
        assert Job("t", "id", trigger="custom").trigger == "custom"
        with pytest.raises(TaskLiteError, match="trigger must be a non-empty str"):
            Job("t", "id", trigger="")
        with pytest.raises(TaskLiteError, match="trigger must be a non-empty str"):
            Job("t", "id", trigger=12)  # type: ignore[arg-type]

    @pytest.mark.parametrize("bad_attempt_no", ["2", True, False, 0, -1, 1.5])
    def test_from_dict_rejects_invalid_attempt_no(self, bad_attempt_no):
        with pytest.raises(TaskLiteError, match="attempt_no must be"):
            Job.from_dict({"task_type": "t", "job_id": "j", "attempt_no": bad_attempt_no})


class TestJobSerializationAndWorkerResource:
    """Job.to_dict / Job.from_dict 往返、结构字段边界与 worker 资源注入。"""

    def test_to_dict_and_from_dict_roundtrip_and_isolation(self):
        result = Job("scan", "s1").to_dict()
        assert set(result.keys()) == {
            "task_type", "job_id", "payload", "depends_on", "files", "resources",
            "max_retries_per_run", "timeout_seconds", "trigger",
        }
        assert "attempt_no" not in result

        minimal = Job.from_dict({"task_type": "t", "job_id": "j"})
        assert minimal.payload == {} and minimal.depends_on == [] and minimal.files == {}
        assert minimal.resources is None and minimal.attempt_no == 1

        explicit_att = Job.from_dict({"task_type": "t", "job_id": "j", "attempt_no": 3})
        assert explicit_att.attempt_no == 3

        user = {
            "timeout": 17,
            "error": {"new_jobs": [], "fatal": True},
            "timeout_seconds": 29,
            "runtime": {"_commit_failures": 8},
            "activation_no": 4,
            "first_enqueued_at": "业务日期",
        }
        original = Job(
            task_type="download",
            job_id="img_001",
            payload=user,
            depends_on=["parent_job"],
            files={
                "out": FileSpec("tmp/1.jpg", role="output", delete_existing=True),
                "src": FileSpec.from_output("parent_job", "raw", role="input"),
            },
            resources={"api": 1.0, "disk": 2.5},
            max_retries_per_run=5,
            timeout_seconds=7200,
            trigger="on_input_change",
        )
        d = original.to_dict()
        assert "timeout" not in d and "attempt_no" not in d
        d["depends_on"].append("mutated")
        assert original.depends_on == ["parent_job"]

        recreated = Job.from_dict(original.to_dict())
        assert recreated.task_type == original.task_type
        assert recreated.job_id == original.job_id
        assert recreated.payload == user
        assert recreated.depends_on == original.depends_on
        assert recreated.files == original.files
        assert recreated.resources == original.resources
        assert recreated.max_retries_per_run == 5
        assert recreated.timeout_seconds == 7200
        assert recreated.trigger == "on_input_change"
        assert recreated.attempt_no == 1

    def test_from_dict_required_and_removed_keys(self):
        with pytest.raises(KeyError):
            Job.from_dict({"job_id": "j"})
        with pytest.raises(KeyError):
            Job.from_dict({"task_type": "t"})
        for legacy_key, current_fields in (
            ("rerun", {"trigger": "always"}),
            ("timeout", {"timeout_seconds": 9}),
            ("max_retries", {"max_retries_per_run": 3}),
            ("timeout_is_transient", {"timeout_seconds": 9}),
        ):
            with pytest.raises(TaskLiteError, match=legacy_key):
                Job.from_dict({"task_type": "t", "job_id": "j", legacy_key: 1, **current_fields})

    def test_job_files_snapshot_and_output_ref_isolation(self, tmp_path):
        base = tmp_path / "work"
        consumer = Job(
            "process",
            "report-001",
            depends_on=["generate::report-001"],
            files={
                "source": FileSpec.from_output("generate::report-001", "report", role="input"),
                "result": FileSpec(Path("results/processed.csv"), role="output"),
            },
        )
        snapshot = consumer.to_dict()
        snapshot["files"] = resolve_job_files_config(
            snapshot["files"], base_dir=base, depends_on=snapshot["depends_on"]
        )
        consumer.files["extra"] = FileSpec("results/extra.csv", role="cache")
        consumer.depends_on.append("other::1")

        assert snapshot["files"] == {
            "source": {
                "producer_uid": "generate::report-001",
                "output_name": "report",
                "role": "input",
                "delete_existing": False,
                "delete_on_failure": False,
                "delete_on_success": False,
            },
            "result": {
                "path": str(base / "results" / "processed.csv"),
                "role": "output",
                "delete_existing": False,
                "delete_on_failure": False,
                "delete_on_success": False,
            },
        }
        restored = Job.from_dict(snapshot)
        assert restored.files["source"] == FileSpec.from_output(
            "generate::report-001", "report", role="input"
        )
        assert restored.files["result"] == FileSpec(
            base / "results" / "processed.csv", role="output"
        )

    def test_inject_worker_resource(self):
        job_snapshot = Job("t", "id").to_dict()
        inject_worker_resource(job_snapshot)
        assert job_snapshot["resources"]["__workers__"] == 1.0

        explicit = Job("t", "id", resources={"__workers__": 2.0}).to_dict()
        inject_worker_resource(explicit)
        assert explicit["resources"]["__workers__"] == 2.0

        job = Job("t", "id")
        inject_worker_resource(job.to_dict())
        assert job.resources is None
