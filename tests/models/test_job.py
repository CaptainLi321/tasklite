"""Job 配置、执行观察与单射身份契约。"""

from __future__ import annotations

import pytest

from tasklite.exceptions import TaskLiteError
from tasklite.models.artifact import FileSpec
from tasklite.models.job import (
    Job,
    BUILTIN_TRIGGERS,
    inject_worker_resource,
)
from tasklite.utils.encoding import make_job_uid, parse_job_uid


class TestJobCreation:
    """Job 构造与默认值。"""

    def test_create_with_minimal_args(self):
        """仅必填项构造：规格位默认值 + 实例位初值。"""
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
        assert not hasattr(job, "max_retries")
        assert not hasattr(job, "timeout_is_transient")
        # 编号仅供观察，首次执行由后端分配。
        assert job.attempt_no == 1
        assert not hasattr(job, "activation_no")
        assert not hasattr(job, "runtime")
        assert not hasattr(job, "first_enqueued_at")

    def test_execution_number_is_read_only(self):
        job = Job("t", "id")
        with pytest.raises(AttributeError):
            job.attempt_no = 7

    def test_create_with_all_fields(self):
        """公开构造全字段显式构造。"""
        out_spec = FileSpec("/tmp/1.jpg", role="output")
        job = Job(
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
        assert job.payload == {"url": "https://example.com/1.jpg"}
        assert job.depends_on == ["parent_job"]
        assert job.files == {"output": out_spec}
        assert job.resources == {"api": 1.0, "disk": 2.5}
        assert job.max_retries_per_run == 5
        assert job.timeout_seconds == 7200
        assert job.trigger == "until_success"

    def test_restore_with_execution_number(self):
        """内部恢复入口挂载当前执行编号。"""
        out_spec = FileSpec("/tmp/1.jpg", role="output")
        job = Job._restore(
            task_type="download",
            job_id="img_001",
            payload={"url": "https://example.com/1.jpg"},
            depends_on=["parent_job"],
            files={"output": out_spec.to_dict()},
            resources={"api": 1.0, "disk": 2.5},
            max_retries_per_run=5,
            timeout_seconds=7200,
            trigger="until_success",
            attempt_no=2,
        )
        assert job.files == {"output": out_spec}
        assert job.attempt_no == 2

    def test_public_constructor_rejects_internal_and_removed_params(self):
        """公开构造拒绝内部运行态参数与已删除参数。"""
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

    def test_payload_none_defaults_to_empty_dict(self):
        assert Job("fetch", "j1", payload=None).payload == {}

    def test_files_none_defaults_to_empty_dict_and_copied(self):
        assert Job("fetch", "j1", files=None).files == {}
        spec = FileSpec("out/1.jpg", role="output")
        src_files = {"out": spec}
        job = Job("fetch", "j1", files=src_files)
        src_files["extra"] = FileSpec("out/2.jpg", role="output")
        assert job.files == {"out": spec}

    def test_resources_none_defaults_to_empty_dict(self):
        assert Job("fetch", "j1", resources=None).resources is None

    def test_resources_shallow_copied(self):
        """Job.resources 初始化浅拷贝，防外部就地变异。"""
        res = {"gpu": 1.0}
        job = Job("scan", "s1", resources=res)
        res["gpu"] = 2.0
        assert job.resources == {"gpu": 1.0}

    def test_depends_on_none_defaults_to_empty_list(self):
        assert Job("fetch", "j1", depends_on=None).depends_on == []

    def test_all_config_after_job_id_is_keyword_only(self):
        """job_id 之后的全部配置参数为 keyword-only，位置传参显式报错。"""
        with pytest.raises(TypeError, match="positional argument"):
            Job("t", "id", {"a": 1})  # type: ignore[misc]
        with pytest.raises(TypeError, match="positional argument"):
            Job("t", "id", {}, {}, 3, [], 3600, True)  # type: ignore[misc]

    def test_float_timeout_accepted(self):
        job = Job("t", "id", timeout_seconds=1.5)
        assert job.timeout_seconds == 1.5
        assert isinstance(job.timeout_seconds, float)


class TestJobIdentityValidation:
    """task_type / job_id 身份校验与透明转义。"""

    def test_job_id_rejects_non_str(self):
        """非 str job_id 显式抛 TaskLiteError 拒绝。"""
        for bad in (42, 3.14, True, b"jid"):
            with pytest.raises(TaskLiteError, match="job_id must be a str"):
                Job("download", bad)

    def test_non_str_task_type_rejected(self):
        with pytest.raises(TaskLiteError, match="task_type must be a str"):
            Job(123, "id")

    def test_empty_string_task_type(self):
        with pytest.raises(TaskLiteError, match="task_type must be a non-empty str"):
            Job("", "id")

    def test_empty_string_job_id(self):
        with pytest.raises(TaskLiteError, match="job_id must be a non-empty str"):
            Job("type", "")

    def test_double_colon_in_task_type_transparently_escaped(self):
        """task_type 含 '::' 由框架透明转义，原始属性保留原文。"""
        job = Job("a::b", "c")
        assert job.task_type == "a::b"
        assert job.job_id == "c"
        assert job.uid == "a%3A%3Ab::c"
        assert job.uid == make_job_uid("a::b", "c")
        assert parse_job_uid(job.uid) == ("a::b", "c")

    def test_double_colon_in_job_id_transparently_escaped(self):
        """job_id 含 '::' 由框架透明转义，原始属性保留原文。"""
        job = Job("a", "b::c")
        assert job.task_type == "a"
        assert job.job_id == "b::c"
        assert job.uid == "a::b%3A%3Ac"
        assert job.uid == make_job_uid("a", "b::c")
        assert parse_job_uid(job.uid) == ("a", "b::c")

    def test_long_job_id_accepted_without_filename_truncation(self):
        """移除业务身份的文件名长度约束，长 job_id 原样保留且可逆。"""
        long_id = "x" * 400
        job = Job("t", long_id)
        assert job.job_id == long_id
        assert job.uid == f"t::{long_id}"
        assert parse_job_uid(job.uid) == ("t", long_id)


class TestJobPayloadAndResources:
    """payload / resources 容器与数值校验。"""

    def test_non_dict_payload_rejected(self):
        """非 dict payload 在模型入口拒绝。"""
        with pytest.raises(TaskLiteError, match="payload must be a dict"):
            Job("t", "id", payload="abc")

    def test_non_dict_resources_rejected(self):
        with pytest.raises(TaskLiteError, match="resources must be a dict"):
            Job("t", "id", resources=["cpu", 1.0])

    def test_bool_resource_amount_rejected(self):
        """bool 是 int 子类，isinstance 会放行；与 Task 默认资源校验对齐，
        入口拒绝。"""
        with pytest.raises(TaskLiteError, match="must be a number"):
            Job("t", "id", resources={"cpu": True})

    def test_resource_amount_matrix_rejected(self):
        """resource amount 拒绝矩阵——NaN/Inf/负值/超大 int/非数值全覆盖。"""
        with pytest.raises(TaskLiteError, match="must be finite|too large"):
            Job("t", "id", resources={"cpu": float("nan")})
        with pytest.raises(TaskLiteError, match="must be finite|too large"):
            Job("t", "id", resources={"cpu": float("inf")})
        with pytest.raises(TaskLiteError, match="must be non-negative"):
            Job("t", "id", resources={"cpu": -1.0})
        # 超大整数与非有限值采用相同拒绝规则。
        with pytest.raises(TaskLiteError, match="too large"):
            Job("t", "id", resources={"cpu": 10**400})
        with pytest.raises(TaskLiteError, match="must be a number"):
            Job("t", "id", resources={"cpu": "high"})

    def test_non_str_resource_name_rejected(self):
        """非 str 资源名：调度侧判为未知资源（永不可跑），且 JSON 落盘后
        键强转为 str，同一作业跨重启身份与账目漂移；构造期入口拒绝。"""
        with pytest.raises(TaskLiteError, match="must be a non-empty str"):
            Job("t", "id", resources={1: 2.0})
        with pytest.raises(TaskLiteError, match="must be a non-empty str"):
            Job("t", "id", resources={"": 2.0})

    def test_non_str_resource_name_rejected_via_from_dict(self):
        data = {"task_type": "t", "job_id": "id", "resources": {1: 2.0}}
        with pytest.raises(TaskLiteError):
            Job.from_dict(data)


class TestJobSpecFieldValidation:
    """max_retries_per_run / timeout_seconds / depends_on / files / trigger 校验。"""

    def test_negative_max_retries_per_run(self):
        with pytest.raises(TaskLiteError, match="max_retries_per_run must be >= 0"):
            Job("t", "id", max_retries_per_run=-1)

    def test_non_int_max_retries_per_run(self):
        with pytest.raises(TaskLiteError, match="max_retries_per_run must be an int"):
            Job("t", "id", max_retries_per_run="3")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="max_retries_per_run must be an int"):
            Job("t", "id", max_retries_per_run=True)

    def test_zero_max_retries_per_run_allowed(self):
        """max_retries_per_run=0：任何失败直接进失败档案。"""
        job = Job("t", "id", max_retries_per_run=0)
        assert job.max_retries_per_run == 0

    def test_timeout_matrix_rejected(self):
        """timeout_seconds 拒绝零/负/NaN/Inf/超大 int/bool/非数值。"""
        with pytest.raises(TaskLiteError, match="timeout_seconds must be a number"):
            Job("t", "id", timeout_seconds="60")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="timeout_seconds must be a number"):
            Job("t", "id", timeout_seconds=True)
        for bad in (0, -1, float("nan"), float("inf"), float("-inf"), 10**400):
            with pytest.raises(TaskLiteError, match="timeout_seconds must be finite and > 0"):
                Job("t", "id", timeout_seconds=bad)

    def test_files_validation(self):
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

    def test_depends_on_non_list_rejected(self):
        """先校验外层类型再迭代——str 会被肢解为字符列表。"""
        with pytest.raises(TaskLiteError, match="depends_on must be a list of strings"):
            Job("t", "j1", depends_on="parent::id")

    def test_depends_on_non_string_elements_rejected(self):
        with pytest.raises(TaskLiteError, match="depends_on must contain only strings"):
            Job("t", "j1", depends_on=[123])
        with pytest.raises(TaskLiteError, match="depends_on must contain only strings"):
            Job("t", "j1", depends_on=["a::b", None])

    def test_depends_on_empty_string_and_self_allowed(self):
        """'' 与自依赖可构造（运行期由依赖图治理，构造期不越权）。"""
        assert Job("t", "id", depends_on=[""]).depends_on == [""]
        job = Job("t", "j1", depends_on=["t::j1"])
        assert job.uid in job.depends_on

    def test_trigger_names_and_unspecified_sentinel(self):
        """trigger 四策略 + None 哨兵语义：合法值全接受，非法值拒绝。"""
        assert BUILTIN_TRIGGERS == (
            "once",
            "until_success",
            "always",
            "on_input_change",
        )
        for value in BUILTIN_TRIGGERS:
            assert Job("t", "id", trigger=value).trigger == value
        assert Job("t", "id").trigger is None
        assert Job("t", "id", trigger="custom").trigger == "custom"
        with pytest.raises(TaskLiteError, match="trigger must be a non-empty str"):
            Job("t", "id", trigger="")
        with pytest.raises(TaskLiteError, match="trigger must be a non-empty str"):
            Job("t", "id", trigger=12)


class TestJobInstanceFieldValidation:
    """实例位字段校验（经私有恢复入口 _restore）。"""

    def test_attempt_no_matrix(self):
        with pytest.raises(TypeError, match="attempt_no must be an int"):
            Job._restore("t", "id", attempt_no="2")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="attempt_no must be an int"):
            Job._restore("t", "id", attempt_no=True)
        with pytest.raises(ValueError, match="attempt_no must be >= 1"):
            Job._restore("t", "id", attempt_no=0)


class TestJobUid:
    """uid 派生与身份等价。"""

    def test_uid_returns_task_type_double_colon_job_id(self):
        assert Job("download", "img_001").uid == "download::img_001"

    def test_uid_with_unicode_in_job_id(self):
        assert Job("tag", "日本语_テスト").uid == "tag::日本语_テスト"

    def test_uid_with_slashes_in_job_id(self):
        assert Job("path", "a/b/c").uid == "path::a/b/c"

    def test_uid_with_colons_in_job_id(self):
        job = Job("t", "id:with:colons")
        assert job.uid == "t::id%3Awith%3Acolons"
        assert job.uid == make_job_uid("t", "id:with:colons")
        assert parse_job_uid(job.uid) == ("t", "id:with:colons")

    def test_uid_identity_after_roundtrip(self):
        original = Job("work", "w_42", payload={"x": 1})
        recreated = Job.from_dict(original.to_dict())
        assert recreated.uid == original.uid == "work::w_42"

    def test_equality_and_hash_by_uid_only(self):
        """__eq__/__hash__ 仅按 uid：规格位不同的同 uid Job 仍等价。"""
        a = Job("t", "j", payload={"x": 1})
        b = Job("t", "j", payload={"x": 2}, timeout_seconds=99)
        assert a == b
        assert hash(a) == hash(b)
        assert a != Job("t", "other")
        assert a != "t::j"  # 与非 Job 比较恒不等


class TestJobToDictFromDict:
    """序列化往返与缺键默认。"""

    def test_to_dict_contains_all_keys(self):
        result = Job("scan", "s1").to_dict()
        expected_keys = {
            "task_type", "job_id", "payload", "depends_on", "files", "resources",
            "max_retries_per_run", "timeout_seconds", "trigger", "attempt_no",
        }
        assert set(result.keys()) == expected_keys

    def test_to_dict_values_reflect_constructor_args(self):
        spec = FileSpec("/tmp/v", role="output")
        job = Job._restore(
            task_type="process",
            job_id="p_99",
            payload={"key": "val"},
            depends_on=["a", "b"],
            files={"f": spec},
            resources={"cpu": 0.5},
            max_retries_per_run=4,
            timeout_seconds=100,
            trigger="always",
            attempt_no=2,
        )
        d = job.to_dict()
        assert d["task_type"] == "process"
        assert d["job_id"] == "p_99"
        assert d["payload"] == {"key": "val"}
        assert d["depends_on"] == ["a", "b"]
        assert d["files"] == {"f": spec.to_dict()}
        assert d["resources"] == {"cpu": 0.5}
        assert d["max_retries_per_run"] == 4
        assert d["timeout_seconds"] == 100
        assert d["trigger"] == "always"
        assert d["attempt_no"] == 2

    def test_from_dict_missing_optional_keys_uses_defaults(self):
        job = Job.from_dict({"task_type": "t", "job_id": "j"})
        assert job.payload == {}
        assert job.depends_on == []
        assert job.files == {}
        assert job.resources is None
        assert job.max_retries_per_run is None
        assert job.timeout_seconds is None
        assert job.trigger is None
        assert job.attempt_no == 1

    def test_from_dict_required_keys(self):
        with pytest.raises(KeyError):
            Job.from_dict({"job_id": "j"})
        with pytest.raises(KeyError):
            Job.from_dict({"task_type": "t"})

    @pytest.mark.parametrize(
        ("legacy_key", "current_fields"),
        [
            ("rerun", {"trigger": "always"}),
            ("timeout", {"timeout_seconds": 9}),
            ("max_retries", {"max_retries_per_run": 3}),
            ("timeout_is_transient", {"timeout_seconds": 9}),
        ],
    )
    def test_from_dict_rejects_removed_keys(self, legacy_key, current_fields):
        with pytest.raises(TaskLiteError, match=legacy_key):
            Job.from_dict({"task_type": "t", "job_id": "j", legacy_key: 1, **current_fields})

    def test_roundtrip_preserves_all_fields(self):
        original = Job._restore(
            task_type="download",
            job_id="img_001",
            payload={"url": "https://example.com/1.jpg"},
            depends_on=["parent_job"],
            files={
                "out": FileSpec("/tmp/1.jpg", role="output", delete_existing=True),
                "src": FileSpec.from_output("parent_job", "raw", role="input"),
            },
            resources={"api": 1.0, "disk": 2.5},
            max_retries_per_run=5,
            timeout_seconds=7200,
            trigger="on_input_change",
            attempt_no=3,
        )
        recreated = Job.from_dict(original.to_dict())
        assert recreated.task_type == original.task_type
        assert recreated.job_id == original.job_id
        assert recreated.payload == original.payload
        assert recreated.depends_on == original.depends_on
        assert recreated.files == original.files
        assert recreated.resources == original.resources
        assert recreated.max_retries_per_run == original.max_retries_per_run
        assert recreated.timeout_seconds == original.timeout_seconds
        assert recreated.trigger == original.trigger
        assert recreated.attempt_no == original.attempt_no

    def test_roundtrip_nested_payload(self):
        nested = {
            "level_one": {
                "level_two": {"level_three": [1, 2, 3], "flag": True},
                "name": "deep",
            },
            "count": 42,
        }
        original = Job("work", "deep_1", payload=nested)
        assert Job.from_dict(original.to_dict()).payload == nested

    def test_to_dict_returns_shallow_copies(self):
        """to_dict 的容器值是浅拷贝：外部变异不回流内部状态。"""
        job = Job("t", "id", payload={"a": 1}, depends_on=["x"])
        d = job.to_dict()
        d["payload"]["a"] = 99
        d["depends_on"].append("y")
        assert job.payload == {"a": 1}
        assert job.depends_on == ["x"]


class TestJobWorkerResource:
    """worker 槽位注入（resources 缺省补 1.0，显式值不覆盖）。"""

    def test_inject_defaults_when_missing(self):
        job_snapshot = Job("t", "id").to_dict()
        inject_worker_resource(job_snapshot)
        assert job_snapshot["resources"]["__workers__"] == 1.0

    def test_inject_keeps_explicit_amount(self):
        job_snapshot = Job("t", "id", resources={"__workers__": 2.0}).to_dict()
        inject_worker_resource(job_snapshot)
        assert job_snapshot["resources"]["__workers__"] == 2.0

    def test_inject_does_not_mutate_source_job(self):
        job = Job("t", "id")
        inject_worker_resource(job.to_dict())
        assert job.resources is None


class TestJobWeirdInputs:
    """对抗性构造与深拷贝隔离。"""

    def test_payload_with_nested_mutables_deep_copied(self):
        """payload 深拷贝隔离——外部嵌套可变对象的后续变异不污染 Job。"""
        nested_list = [1, 2, 3]
        job = Job("t", "id", payload={"items": nested_list})
        nested_list.append(4)
        assert job.payload["items"] == [1, 2, 3]

    @pytest.mark.parametrize("payload", [
        {1: "integer", "1": "string"},
        {"nested": {False: "boolean"}},
        {"nested": [{"items": (1, 2)}]},
        {"value": object()},
    ])
    def test_payload_rejects_implicit_json_conversion(self, payload):
        with pytest.raises(TaskLiteError, match="payload"):
            Job("t", "id", payload=payload)
