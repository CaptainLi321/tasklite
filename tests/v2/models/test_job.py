"""v2 Job 模型契约测试：字段归位、构造校验、序列化往返与 uid 身份。

移植 v1 job 构造校验类测试并按 v2 字段面改写：retries/backoff_* 构造
参数废除，新增实例位 attempt_no / activation_no / first_enqueued_at。
"""

from __future__ import annotations

import pytest

from tasklite.v2.models.job import (
    Job,
    JobRuntimeState,
    RERUN_VALUES,
    inject_worker_resource,
)
from tasklite.v2.utils.encoding import safe_uid_filename


class TestJobCreation:
    """Job 构造与默认值。"""

    def test_create_with_minimal_args(self):
        """仅必填项构造：规格位默认值 + 实例位初值。"""
        job = Job("scan", "scan_001")
        assert job.task_type == "scan"
        assert job.job_id == "scan_001"
        assert job.payload == {}
        assert job.resources == {}
        assert job.max_retries == 3
        assert job.depends_on == []
        assert job.timeout == 3600
        assert job.timeout_is_transient is False
        assert job.rerun is None
        # 实例位初值：首次激活、首次尝试、尚未入队
        assert job.attempt_no == 1
        assert job.activation_no == 1
        assert job.first_enqueued_at is None

    def test_create_with_all_fields(self):
        """全字段显式构造（规格位 + 实例位）。"""
        job = Job(
            task_type="download",
            job_id="img_001",
            payload={"url": "https://example.com/1.jpg"},
            resources={"api": 1.0, "disk": 2.5},
            max_retries=5,
            depends_on=["parent_job"],
            timeout=7200,
            timeout_is_transient=True,
            rerun="on_failure",
            attempt_no=2,
            activation_no=3,
            first_enqueued_at="2026-01-01T00:00:00+00:00",
        )
        assert job.payload == {"url": "https://example.com/1.jpg"}
        assert job.resources == {"api": 1.0, "disk": 2.5}
        assert job.max_retries == 5
        assert job.depends_on == ["parent_job"]
        assert job.timeout == 7200
        assert job.timeout_is_transient is True
        assert job.rerun == "on_failure"
        assert job.attempt_no == 2
        assert job.activation_no == 3
        assert job.first_enqueued_at == "2026-01-01T00:00:00+00:00"

    def test_payload_none_defaults_to_empty_dict(self):
        assert Job("fetch", "j1", payload=None).payload == {}

    def test_resources_none_defaults_to_empty_dict(self):
        assert Job("fetch", "j1", resources=None).resources == {}

    def test_resources_shallow_copied(self):
        """Job.resources 初始化浅拷贝，防外部就地变异。"""
        res = {"gpu": 1.0}
        job = Job("scan", "s1", resources=res)
        res["gpu"] = 2.0
        assert job.resources == {"gpu": 1.0}

    def test_depends_on_none_defaults_to_empty_list(self):
        assert Job("fetch", "j1", depends_on=None).depends_on == []

    def test_timeout_is_transient_is_keyword_only(self):
        """布尔参数 keyword-only：第 8 个位置参数（timeout_is_transient 位）
        显式报错。"""
        with pytest.raises(TypeError, match="positional argument"):
            Job("t", "id", {}, {}, 3, [], 3600, True)  # type: ignore[misc]

    def test_float_timeout_accepted(self):
        job = Job("t", "id", timeout=1.5)
        assert job.timeout == 1.5
        assert isinstance(job.timeout, float)


class TestJobIdentityValidation:
    """task_type / job_id 身份校验（uid 分隔符与类型门卫）。"""

    def test_job_id_rejects_non_str(self):
        """Job("t", 1.0).uid == Job("t", "1.0").uid 静默碰撞合并任务，
        入口拒绝。"""
        for bad in (42, 3.14, True, b"jid"):
            with pytest.raises(TypeError, match="job_id must be a str"):
                Job("download", bad)

    def test_non_str_task_type_rejected(self):
        with pytest.raises(TypeError, match="task_type must be a str"):
            Job(123, "id")

    def test_empty_string_task_type(self):
        with pytest.raises(ValueError, match="task_type must be a non-empty str"):
            Job("", "id")

    def test_empty_string_job_id(self):
        with pytest.raises(ValueError, match="job_id must be a non-empty str"):
            Job("type", "")

    def test_double_colon_in_task_type_rejected(self):
        """task_type 含 '::' 抛 ValueError，避免 uid 分隔符碰撞。"""
        with pytest.raises(ValueError, match="task_type must not contain '::'"):
            Job("a::b", "c")

    def test_double_colon_in_job_id_rejected(self):
        """job_id 含 '::' 抛 ValueError，避免 uid 分隔符碰撞。"""
        with pytest.raises(ValueError, match="job_id must not contain '::'"):
            Job("a", "b::c")

    def test_oversized_job_id_rejected(self):
        """uid 派生 IPC 文件名（锁/结果）超上限 → ENAMETOOLONG → 派发
        livelock（永远重入队且不进失败档案），入口按转义后字节数拒绝。"""
        long_id = "x" * 400
        assert len(safe_uid_filename(f"t::{long_id}")) > 199
        with pytest.raises(ValueError, match="too long"):
            Job("t", long_id)

    def test_uid_filename_boundary_accepted(self):
        """转义后 ≤199 字节的 uid 仍可构造（边界不误杀）。"""
        # 干净 job_id：safe_uid_filename 只把 :: 转义为 %3A%3A（+4 字节）
        ok_id = "k" * 190
        assert len(safe_uid_filename(f"t::{ok_id}")) <= 199
        Job("t", ok_id)  # 不抛


class TestJobPayloadAndResources:
    """payload / resources 容器与数值校验。"""

    def test_non_dict_payload_rejected(self):
        """非 dict payload：入口显式拒绝（完整序列化预检在 enqueue 侧）。"""
        with pytest.raises(TypeError, match="payload must be a dict"):
            Job("t", "id", payload="abc")

    def test_non_dict_resources_rejected(self):
        with pytest.raises(TypeError, match="resources must be a dict"):
            Job("t", "id", resources=["cpu", 1.0])

    def test_bool_resource_amount_rejected(self):
        """bool 是 int 子类，isinstance 会放行；与 Task 默认资源校验对齐，
        入口拒绝。"""
        with pytest.raises(TypeError, match="must be a number"):
            Job("t", "id", resources={"cpu": True})

    def test_resource_amount_matrix_rejected(self):
        """resource amount 拒绝矩阵——NaN/Inf/负值/超大 int/非数值全覆盖。"""
        with pytest.raises(ValueError, match="must be finite|too large"):
            Job("t", "id", resources={"cpu": float("nan")})
        with pytest.raises(ValueError, match="must be finite|too large"):
            Job("t", "id", resources={"cpu": float("inf")})
        with pytest.raises(ValueError, match="must be non-negative"):
            Job("t", "id", resources={"cpu": -1.0})
        # 超大 int 溢出收敛 ValueError，不裸抛 OverflowError
        with pytest.raises(ValueError, match="too large"):
            Job("t", "id", resources={"cpu": 10**400})
        with pytest.raises(TypeError, match="must be a number"):
            Job("t", "id", resources={"cpu": "high"})

    def test_non_str_resource_name_rejected(self):
        """非 str 资源名：调度侧判为未知资源（永不可跑），且 JSON 落盘后
        键强转为 str，同一作业跨重启身份与账目漂移；构造期入口拒绝。"""
        with pytest.raises(TypeError, match="must be a non-empty str"):
            Job("t", "id", resources={1: 2.0})
        with pytest.raises(TypeError, match="must be a non-empty str"):
            Job("t", "id", resources={"": 2.0})

    def test_non_str_resource_name_rejected_via_from_dict(self):
        data = {"task_type": "t", "job_id": "id", "resources": {1: 2.0}}
        with pytest.raises(TypeError):
            Job.from_dict(data)


class TestJobSpecFieldValidation:
    """max_retries / timeout / depends_on / rerun 校验。"""

    def test_negative_max_retries(self):
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            Job("t", "id", max_retries=-1)

    def test_non_int_max_retries(self):
        with pytest.raises(TypeError, match="max_retries must be an int"):
            Job("t", "id", max_retries="3")
        with pytest.raises(TypeError, match="max_retries must be an int"):
            Job("t", "id", max_retries=True)

    def test_zero_max_retries_allowed(self):
        """max_retries=0：任何失败直接进失败档案（允许执行条件
        attempt_no <= 0 + 1 仅剩首次执行）。"""
        job = Job("t", "id", max_retries=0)
        assert job.max_retries == 0

    def test_timeout_matrix_rejected(self):
        """timeout 拒绝零/负/NaN/Inf/超大 int/bool/非数值。"""
        with pytest.raises(TypeError, match="timeout must be a number"):
            Job("t", "id", timeout="60")
        with pytest.raises(TypeError, match="timeout must be a number"):
            Job("t", "id", timeout=True)
        for bad in (0, -1, float("nan"), float("inf"), 10**400):
            with pytest.raises(ValueError, match="timeout must be finite and > 0"):
                Job("t", "id", timeout=bad)

    def test_non_bool_timeout_is_transient_rejected(self):
        """非 bool 真值会静默改变超时归类语义，入口拒绝。"""
        with pytest.raises(TypeError, match="timeout_is_transient must be a bool"):
            Job("t", "id", timeout_is_transient="yes")
        with pytest.raises(TypeError, match="timeout_is_transient must be a bool"):
            Job("t", "id", timeout_is_transient=1)

    def test_depends_on_non_list_rejected(self):
        """先校验外层类型再迭代——str 会被肢解为字符列表。"""
        with pytest.raises(TypeError, match="depends_on must be a list of strings"):
            Job("t", "j1", depends_on="parent::id")

    def test_depends_on_non_string_elements_rejected(self):
        with pytest.raises(TypeError, match="depends_on must contain only strings"):
            Job("t", "j1", depends_on=[123])
        with pytest.raises(TypeError, match="depends_on must contain only strings"):
            Job("t", "j1", depends_on=["a::b", None])

    def test_depends_on_empty_string_and_self_allowed(self):
        """'' 与自依赖可构造（运行期由依赖图治理，构造期不越权）。"""
        assert Job("t", "id", depends_on=[""]).depends_on == [""]
        job = Job("t", "j1", depends_on=["t::j1"])
        assert job.uid in job.depends_on

    def test_rerun_legal_value_set(self):
        """rerun 四策略 + None 哨兵语义：合法值全接受，非法值拒绝。"""
        assert RERUN_VALUES == ("never", "on_failure", "every_run", "on_input_change")
        for value in RERUN_VALUES:
            assert Job("t", "id", rerun=value).rerun == value
        assert Job("t", "id").rerun is None
        with pytest.raises(ValueError, match="rerun must be None"):
            Job("t", "id", rerun="always")
        with pytest.raises(ValueError, match="rerun must be None"):
            Job("t", "id", rerun="never ")


class TestJobInstanceFieldValidation:
    """实例位字段校验（attempt_no / activation_no / first_enqueued_at）。"""

    def test_attempt_no_matrix(self):
        with pytest.raises(TypeError, match="attempt_no must be an int"):
            Job("t", "id", attempt_no="2")
        with pytest.raises(TypeError, match="attempt_no must be an int"):
            Job("t", "id", attempt_no=True)
        with pytest.raises(ValueError, match="attempt_no must be >= 1"):
            Job("t", "id", attempt_no=0)

    def test_activation_no_matrix(self):
        with pytest.raises(TypeError, match="activation_no must be an int"):
            Job("t", "id", activation_no=1.5)
        with pytest.raises(ValueError, match="activation_no must be >= 1"):
            Job("t", "id", activation_no=0)

    def test_first_enqueued_at_matrix(self):
        with pytest.raises(TypeError, match="first_enqueued_at must be a str or None"):
            Job("t", "id", first_enqueued_at=12345)
        with pytest.raises(ValueError, match="non-empty"):
            Job("t", "id", first_enqueued_at="")

    def test_budget_rule_alignment(self):
        """预算判定对齐：允许执行条件 attempt_no <= max_retries + 1。"""
        job = Job("t", "id", max_retries=3)
        assert job.attempt_no <= job.max_retries + 1
        retrying = Job("t", "id", max_retries=3, attempt_no=4)
        assert retrying.attempt_no == retrying.max_retries + 1


class TestJobUid:
    """uid 派生与身份等价。"""

    def test_uid_returns_task_type_double_colon_job_id(self):
        assert Job("download", "img_001").uid == "download::img_001"

    def test_uid_with_unicode_in_job_id(self):
        assert Job("tag", "日本语_テスト").uid == "tag::日本语_テスト"

    def test_uid_with_slashes_in_job_id(self):
        assert Job("path", "a/b/c").uid == "path::a/b/c"

    def test_uid_with_colons_in_job_id(self):
        assert Job("t", "id:with:colons").uid == "t::id:with:colons"

    def test_uid_identity_after_roundtrip(self):
        original = Job("work", "w_42", payload={"x": 1})
        recreated = Job.from_dict(original.to_dict())
        assert recreated.uid == original.uid == "work::w_42"

    def test_equality_and_hash_by_uid_only(self):
        """__eq__/__hash__ 仅按 uid：规格位不同的同 uid Job 仍等价。"""
        a = Job("t", "j", payload={"x": 1})
        b = Job("t", "j", payload={"x": 2}, timeout=99)
        assert a == b
        assert hash(a) == hash(b)
        assert a != Job("t", "other")
        assert a != "t::j"  # 与非 Job 比较恒不等


class TestJobToDictFromDict:
    """序列化往返与缺键默认。"""

    def test_to_dict_contains_all_keys(self):
        result = Job("scan", "s1").to_dict()
        expected_keys = {
            "task_type", "job_id", "payload", "resources",
            "max_retries", "depends_on", "timeout", "timeout_is_transient",
            "rerun", "runtime", "attempt_no", "activation_no",
            "first_enqueued_at",
        }
        assert set(result.keys()) == expected_keys

    def test_to_dict_values_reflect_constructor_args(self):
        job = Job(
            task_type="process",
            job_id="p_99",
            payload={"key": "val"},
            resources={"cpu": 0.5},
            max_retries=4,
            depends_on=["a", "b"],
            timeout=100,
            rerun="every_run",
            attempt_no=2,
            activation_no=1,
            first_enqueued_at="2026-02-02T00:00:00+00:00",
        )
        d = job.to_dict()
        assert d["task_type"] == "process"
        assert d["job_id"] == "p_99"
        assert d["payload"] == {"key": "val"}
        assert d["resources"] == {"cpu": 0.5}
        assert d["max_retries"] == 4
        assert d["depends_on"] == ["a", "b"]
        assert d["timeout"] == 100
        assert d["rerun"] == "every_run"
        assert d["attempt_no"] == 2
        assert d["activation_no"] == 1
        assert d["first_enqueued_at"] == "2026-02-02T00:00:00+00:00"

    def test_from_dict_missing_optional_keys_uses_defaults(self):
        job = Job.from_dict({"task_type": "t", "job_id": "j"})
        assert job.payload == {}
        assert job.resources == {}
        assert job.max_retries == 3
        assert job.depends_on == []
        assert job.timeout == 3600
        assert job.timeout_is_transient is False
        assert job.rerun is None
        assert job.attempt_no == 1
        assert job.activation_no == 1
        assert job.first_enqueued_at is None
        assert job.runtime.to_dict() == {}

    def test_from_dict_required_keys(self):
        with pytest.raises(KeyError):
            Job.from_dict({"job_id": "j"})
        with pytest.raises(KeyError):
            Job.from_dict({"task_type": "t"})

    def test_roundtrip_preserves_all_fields(self):
        original = Job(
            task_type="download",
            job_id="img_001",
            payload={"url": "https://example.com/1.jpg"},
            resources={"api": 1.0, "disk": 2.5},
            max_retries=5,
            depends_on=["parent_job"],
            timeout=7200,
            timeout_is_transient=True,
            rerun="on_input_change",
            attempt_no=3,
            activation_no=2,
            first_enqueued_at="2026-03-03T00:00:00+00:00",
        )
        recreated = Job.from_dict(original.to_dict())
        assert recreated.task_type == original.task_type
        assert recreated.job_id == original.job_id
        assert recreated.payload == original.payload
        assert recreated.resources == original.resources
        assert recreated.max_retries == original.max_retries
        assert recreated.depends_on == original.depends_on
        assert recreated.timeout == original.timeout
        assert recreated.timeout_is_transient == original.timeout_is_transient
        assert recreated.rerun == original.rerun
        assert recreated.attempt_no == original.attempt_no
        assert recreated.activation_no == original.activation_no
        assert recreated.first_enqueued_at == original.first_enqueued_at

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
        job_dict = Job("t", "id").to_dict()
        inject_worker_resource(job_dict)
        assert job_dict["resources"]["__workers__"] == 1.0

    def test_inject_keeps_explicit_amount(self):
        job_dict = Job("t", "id", resources={"__workers__": 2.0}).to_dict()
        inject_worker_resource(job_dict)
        assert job_dict["resources"]["__workers__"] == 2.0

    def test_inject_does_not_mutate_source_job(self):
        job = Job("t", "id")
        inject_worker_resource(job.to_dict())
        assert "__workers__" not in job.resources


class TestJobRuntimeState:
    """JobRuntimeState 精简版：计数字段、extra 无损往返与映射协议。"""

    def test_default_empty(self):
        st = JobRuntimeState()
        assert st.commit_failures == 0
        assert st.dispatch_failures == 0
        assert st.last_retry_error == ""
        assert st.extra == {}
        assert st.to_dict() == {}

    def test_roundtrip_dict(self):
        data = {
            "_commit_failures": 2,
            "_dispatch_failures": 1,
            "_last_retry_error": "Connection reset",
        }
        st = JobRuntimeState.from_dict(data)
        assert st.commit_failures == 2
        assert st.dispatch_failures == 1
        assert st.last_retry_error == "Connection reset"
        assert st.to_dict() == data

    def test_none_safe(self):
        st = JobRuntimeState.from_dict(None)
        assert st.commit_failures == 0
        assert st.to_dict() == {}

    def test_record_counters_and_retry_error(self):
        st = JobRuntimeState()
        assert st.record_commit_failure() == 1
        assert st.record_commit_failure() == 2
        assert st.record_dispatch_failure() == 1
        st.record_retry_error("boom")
        assert st.last_retry_error == "boom"
        st.record_retry_error(None)
        assert st.last_retry_error == ""

    def test_extra_same_name_keys_stay_lossless(self):
        # runtime 命名空间仅 `_` 前缀名为框架字段；与字段同名的无下划线键
        # 属用户 extra 数据——非数值不静默丢弃、数值不被劫持为框架字段
        data = {"commit_failures": "note", "note": "x"}
        st = JobRuntimeState.from_dict(data)
        assert st.commit_failures == 0
        assert st.extra == data
        assert st.to_dict() == data

        st_two = JobRuntimeState.from_dict({"dispatch_failures": 7})
        assert st_two.dispatch_failures == 0
        assert st_two.extra == {"dispatch_failures": 7}
        assert st_two.to_dict() == {"dispatch_failures": 7}

    def test_stale_loose_same_name_key_cannot_revive_field(self):
        # 同时含规范键与无下划线同名键：字段只认规范键；规范键清空后，
        # 滞留的同名 extra 键不得在下次反序列化时复活陈旧框架值
        st = JobRuntimeState.from_dict({"_commit_failures": 2, "commit_failures": 99})
        assert st.commit_failures == 2
        assert st.extra == {"commit_failures": 99}

        st.commit_failures = 0
        back = st.to_dict()
        assert back == {"commit_failures": 99}
        st_two = JobRuntimeState.from_dict(back)
        assert st_two.commit_failures == 0

    def test_from_dict_tolerates_dirty_counters(self):
        """持久化容灾：脏计数（非数值/None）降级为 0，不抛异常。"""
        st = JobRuntimeState.from_dict({
            "_commit_failures": "many",
            "_dispatch_failures": None,
            "_last_retry_error": 42,
        })
        assert st.commit_failures == 0
        assert st.dispatch_failures == 0
        assert st.last_retry_error == "42"

    def test_runtime_setter_accepts_dict_and_instance(self):
        job = Job("t", "id", runtime={"_commit_failures": 1, "k": "v"})
        assert job.runtime.commit_failures == 1
        assert job.runtime.extra == {"k": "v"}
        shared = JobRuntimeState(commit_failures=5)
        job.runtime = shared
        assert job.runtime is shared


class TestJobWeirdInputs:
    """对抗性构造（当前行为锁定）。"""

    def test_payload_with_nested_mutables_shared_reference(self):
        """payload 不做深拷贝（浅拷贝语义）——嵌套可变对象的变异可见。"""
        nested_list = [1, 2, 3]
        job = Job("t", "id", payload={"items": nested_list})
        nested_list.append(4)
        assert job.payload["items"] == [1, 2, 3, 4]

    def test_to_dict_with_non_serializable_payload(self):
        """to_dict 本身不做 JSON 序列化（预检在 enqueue 侧）。"""

        def my_func():
            pass

        job = Job("t", "id", payload={"fn": my_func})
        d = job.to_dict()
        assert d["payload"]["fn"] is my_func
