"""Unit tests for the Job class serialization and deserialization."""

import pytest
from tasklite.testing import fake_ctx
from tasklite.models.job import Job, JobRuntimeState


class TestJobCreation:
    """Tests for Job object creation."""

    def test_create_with_all_fields(self):
        """Job creation with all 10 fields explicitly set."""
        job = Job(
            task_type="download",
            job_id="img_001",
            payload={"url": "https://example.com/1.jpg"},
            resources={"api": 1.0, "disk": 2.5},
            retries=2,
            max_retries=5,
            depends_on=["parent_job"],
            timeout=7200,
            backoff_base=3.0,
            backoff_max=600.0,
        )

        assert job.task_type == "download"
        assert job.job_id == "img_001"
        assert job.payload == {"url": "https://example.com/1.jpg"}
        assert job.resources == {"api": 1.0, "disk": 2.5}
        assert job.retries == 2
        assert job.max_retries == 5
        assert job.depends_on == ["parent_job"]
        assert job.timeout == 7200
        assert job.backoff_base == 3.0
        assert job.backoff_max == 600.0

    def test_create_with_minimal_args(self):
        """Job creation with only required args (task_type, job_id) uses correct defaults."""
        job = Job("scan", "scan_001")

        assert job.task_type == "scan"
        assert job.job_id == "scan_001"
        assert job.payload == {}
        assert job.resources == {}
        assert job.retries == 0
        assert job.max_retries == 3
        assert job.depends_on == []
        assert job.timeout == 3600
        assert job.backoff_base == 2.0
        assert job.backoff_max == 300.0

    def test_job_id_rejects_non_str(self):
        """job_id 必须显式要求为 str：
        Job("t",1.0).uid == Job("t","1.0").uid 静默碰撞合并任务。"""
        with pytest.raises(TypeError, match="job_id"):
            Job("download", 42)

    def test_payload_none_defaults_to_empty_dict(self):
        """When payload is None, it defaults to an empty dict."""
        job = Job("fetch", "j1", payload=None)
        assert job.payload == {}

    def test_resources_none_defaults_to_empty_dict(self):
        """When resources is None, it defaults to an empty dict."""
        job = Job("fetch", "j1", resources=None)
        assert job.resources == {}

    def test_resources_shallow_copied(self):
        """Job.resources 初始化时进行浅拷贝，防止外部就地变异。"""
        res = {"gpu": 1.0}
        job = Job("scan", "s1", resources=res)
        res["gpu"] = 2.0
        assert job.resources == {"gpu": 1.0}

    def test_depends_on_none_defaults_to_empty_list(self):
        """When depends_on is None, it defaults to an empty list."""
        job = Job("fetch", "j1", depends_on=None)
        assert job.depends_on == []



class TestJobToDict:
    """Tests for Job.to_dict() serialization."""

    def test_to_dict_returns_dict(self):
        """to_dict() returns a dict."""
        job = Job("scan", "s1")
        result = job.to_dict()
        assert isinstance(result, dict)

    def test_to_dict_contains_all_keys(self):
        """to_dict() returns dict with all 13 expected keys ( 含 runtime)。"""
        job = Job("scan", "s1")
        result = job.to_dict()

        expected_keys = {
            "task_type", "job_id", "payload", "resources",
            "retries", "max_retries", "depends_on",
            "timeout", "backoff_base", "backoff_max",
            "timeout_is_transient", "rerun", "runtime",
        }
        assert set(result.keys()) == expected_keys

    def test_to_dict_values_reflect_constructor_args(self):
        """to_dict() values match what was set via constructor."""
        job = Job(
            task_type="process",
            job_id="p99",
            payload={"key": "val"},
            resources={"cpu": 0.5},
            retries=1,
            max_retries=4,
            depends_on=["a", "b"],
            timeout=100,
            backoff_base=1.5,
            backoff_max=60.0,
        )
        d = job.to_dict()

        assert d["task_type"] == "process"
        assert d["job_id"] == "p99"
        assert d["payload"] == {"key": "val"}
        assert d["resources"] == {"cpu": 0.5}
        assert d["retries"] == 1
        assert d["max_retries"] == 4
        assert d["depends_on"] == ["a", "b"]
        assert d["timeout"] == 100
        assert d["backoff_base"] == 1.5
        assert d["backoff_max"] == 60.0


class TestJobFromDict:
    """Tests for Job.from_dict() deserialization."""

    def test_from_dict_with_all_fields(self):
        """from_dict() with all fields reconstructs a Job correctly."""
        data = {
            "task_type": "download",
            "job_id": "img_001",
            "payload": {"url": "https://example.com/1.jpg"},
            "resources": {"api": 1.0, "disk": 2.5},
            "retries": 2,
            "max_retries": 5,
            "depends_on": ["parent_job"],
            "timeout": 7200,
            "backoff_base": 3.0,
            "backoff_max": 600.0,
        }
        job = Job.from_dict(data)

        assert job.task_type == "download"
        assert job.job_id == "img_001"
        assert job.payload == {"url": "https://example.com/1.jpg"}
        assert job.resources == {"api": 1.0, "disk": 2.5}
        assert job.retries == 2
        assert job.max_retries == 5
        assert job.depends_on == ["parent_job"]
        assert job.timeout == 7200
        assert job.backoff_base == 3.0
        assert job.backoff_max == 600.0

    def test_from_dict_missing_optional_keys_uses_defaults(self):
        """from_dict() with only task_type and job_id fills defaults correctly."""
        data = {"task_type": "t", "job_id": "j"}
        job = Job.from_dict(data)

        assert job.task_type == "t"
        assert job.job_id == "j"
        assert job.payload == {}
        assert job.resources == {}
        assert job.retries == 0
        assert job.max_retries == 3
        assert job.depends_on == []
        assert job.timeout == 3600
        assert job.backoff_base == 2.0
        assert job.backoff_max == 300.0

    def test_from_dict_task_type_required(self):
        """from_dict() raises KeyError when task_type is missing."""
        with pytest.raises(KeyError):
            Job.from_dict({"job_id": "j"})

    def test_from_dict_job_id_required(self):
        """from_dict() raises KeyError when job_id is missing."""
        with pytest.raises(KeyError):
            Job.from_dict({"task_type": "t"})


class TestJobRoundtrip:
    """Tests for serialize → deserialize roundtrip."""

    def test_roundtrip_preserves_all_fields(self):
        """to_dict() → from_dict() roundtrip preserves all field values."""
        original = Job(
            task_type="download",
            job_id="img_001",
            payload={"url": "https://example.com/1.jpg"},
            resources={"api": 1.0, "disk": 2.5},
            retries=2,
            max_retries=5,
            depends_on=["parent_job"],
            timeout=7200,
            backoff_base=3.0,
            backoff_max=600.0,
        )
        recreated = Job.from_dict(original.to_dict())

        assert recreated.task_type == original.task_type
        assert recreated.job_id == original.job_id
        assert recreated.payload == original.payload
        assert recreated.resources == original.resources
        assert recreated.retries == original.retries
        assert recreated.max_retries == original.max_retries
        assert recreated.depends_on == original.depends_on
        assert recreated.timeout == original.timeout
        assert recreated.backoff_base == original.backoff_base
        assert recreated.backoff_max == original.backoff_max

    def test_roundtrip_nested_payload(self):
        """A deeply nested dict in payload survives to_dict → from_dict roundtrip."""
        nested = {
            "level1": {
                "level2": {
                    "level3": [1, 2, 3],
                    "flag": True,
                },
                "name": "deep",
            },
            "count": 42,
        }
        original = Job("work", "deep_1", payload=nested)
        recreated = Job.from_dict(original.to_dict())

        assert recreated.payload == nested
        assert recreated.payload["level1"]["level2"]["level3"] == [1, 2, 3]

    def test_roundtrip_depends_on_list(self):
        """depends_on list survives to_dict → from_dict roundtrip."""
        original = Job("build", "b1", depends_on=["step_a", "step_b", "step_c"])
        recreated = Job.from_dict(original.to_dict())

        assert recreated.depends_on == ["step_a", "step_b", "step_c"]
        assert len(recreated.depends_on) == 3

    def test_roundtrip_empty_depends_on(self):
        """Empty depends_on list survives roundtrip."""
        original = Job("build", "b1", depends_on=[])
        recreated = Job.from_dict(original.to_dict())

        assert recreated.depends_on == []

    def test_roundtrip_float_backoff_values(self):
        """Float backoff values survive roundtrip exactly."""
        original = Job("test", "t1", backoff_base=1.7, backoff_max=45.5)
        recreated = Job.from_dict(original.to_dict())

        assert recreated.backoff_base == 1.7
        assert recreated.backoff_max == 45.5
        assert isinstance(recreated.backoff_base, float)
        assert isinstance(recreated.backoff_max, float)


class TestJobUid:
    """Tests for Job.uid."""

    def test_uid_returns_task_type_double_colon_job_id(self):
        """uid() returns 'task_type::job_id' format."""
        job = Job("download", "img_001")
        assert job.uid == "download::img_001"

    def test_uid_with_unicode_in_job_id(self):
        """uid() handles Unicode characters in job_id."""
        job = Job("tag", "日本语_テスト")
        assert job.uid == "tag::日本语_テスト"

    def test_uid_with_slashes_in_job_id(self):
        """uid() handles forward slashes in job_id."""
        job = Job("path", "a/b/c")
        assert job.uid == "path::a/b/c"

    def test_uid_with_colons_in_job_id(self):
        """uid() handles colons in job_id (beyond the :: separator)."""
        job = Job("t", "id:with:colons")
        assert job.uid == "t::id:with:colons"

    def test_uid_with_int_job_id(self):
        """job_id 拒绝非 str（不再 str() 强转）。"""
        with pytest.raises(TypeError, match="job_id"):
            Job("counter", 100)

    def test_uid_identity_after_roundtrip(self):
        """uid() is preserved across to_dict → from_dict roundtrip."""
        original = Job("work", "w_42", payload={"x": 1})
        recreated = Job.from_dict(original.to_dict())
        assert recreated.uid == original.uid
        assert recreated.uid == "work::w_42"


class TestJobEdgeCases:
    """Bizarre edge case tests for Job creation."""

    def test_empty_string_task_type(self):
        with pytest.raises(ValueError, match="task_type must be a non-empty str"):
            Job("", "id")

    def test_empty_string_job_id(self):
        with pytest.raises(ValueError, match="job_id must be a non-empty str"):
            Job("type", "")

    def test_empty_string_both(self):
        with pytest.raises(ValueError):
            Job("", "")

    def test_none_payload_explicit(self):
        job = Job("t", "id", payload=None)
        assert job.payload == {}

    def test_non_str_task_type_rejected(self):
        """非 str task_type：入口显式拒绝。"""
        with pytest.raises(TypeError, match="task_type must be a str"):
            Job(123, "id")

    def test_non_dict_payload_rejected(self):
        """非 dict payload：入口显式拒绝。"""
        with pytest.raises(TypeError, match="payload must be a dict"):
            Job("t", "id", payload="abc")

    def test_bool_resource_amount_rejected(self):
        """bool 资源 amount：bool 是 int 子类，isinstance 会放行；与
        pipeline._validate_resource_amounts 对齐，入口拒绝。"""
        with pytest.raises(TypeError, match="must be a number"):
            Job("t", "id", resources={"cpu": True})

    def test_resource_amount_matrix_rejected(self):
        """resource amount 拒绝矩阵——NaN/Inf/负值/超大 int
        全覆盖（此前只有 bool 分支有测试，Rule 4 对称路径缺口）。"""
        import math
        with pytest.raises(ValueError, match="must be finite"):
            Job("t", "id", resources={"cpu": float("nan")})
        with pytest.raises(ValueError, match="must be finite"):
            Job("t", "id", resources={"cpu": float("inf")})
        with pytest.raises(ValueError, match="must be non-negative"):
            Job("t", "id", resources={"cpu": -1.0})
        # OverflowError 防御：超大 int（10**400）让 math.isfinite 抛
        # OverflowError → 应转标准 ValueError（不逃逸 malformed 兜底）
        with pytest.raises(ValueError, match="too large"):
            Job("t", "id", resources={"cpu": 10 ** 400})
        with pytest.raises(TypeError, match="must be a number"):
            Job("t", "id", resources={"cpu": "high"})

    def test_resource_amount_dict_guard(self):
        """：resources 非 dict 类型入口拒绝（Job.__init__ 守卫
        在 validate_resource_amounts 之前）——与 pipeline 侧 register_handler
        的 default_resources dict 守卫对称。"""
        with pytest.raises(TypeError, match="resources must be a dict"):
            Job("t", "id", resources=["cpu", 1.0])

    def test_non_str_resource_name_rejected(self):
        """非 str 资源名：调度侧判为未知资源（永不可跑），且 JSON 落盘后
        键强转为 str，同一作业跨重启从死锁翻转为可跑（身份与账目漂移）；
        构造期入口拒绝，与 suspend_resource 的资源名校验对称。"""
        with pytest.raises(TypeError, match="must be a non-empty str"):
            Job("t", "id", resources={1: 2.0})
        with pytest.raises(TypeError, match="must be a non-empty str"):
            Job("t", "id", resources={"": 2.0})

    def test_non_str_resource_name_rejected_via_from_dict(self):
        data = {
            "task_type": "t", "job_id": "id", "payload": {},
            "resources": {1: 2.0}, "retries": 0, "max_retries": 3,
            "depends_on": [], "timeout": 60, "backoff_base": 2.0, "backoff_max": 300.0,
        }
        with pytest.raises(TypeError):
            Job.from_dict(data)

    def test_zero_timeout_raises_valueerror(self):
        """timeout=0 now raises ValueError per  contract (timeout must be > 0)."""
        with pytest.raises(ValueError, match="timeout must be finite and > 0"):
            Job("t", "id", timeout=0)

    def test_nan_timeout_raises_valueerror(self):
        """timeout=NaN/Inf 必须被拒绝——NaN <= 0 为 False，旧校验放行 →
        deadline=NaN → 看门狗永不触发。"""
        import math
        with pytest.raises(ValueError, match="timeout must be finite"):
            Job("t", "id", timeout=float("nan"))
        with pytest.raises(ValueError, match="timeout must be finite"):
            Job("t", "id", timeout=float("inf"))

    def test_bool_timeout_raises_typeerror(self):
        """timeout=True（bool 是 int 子类）必须被拒绝，而非静默当 1 秒。"""
        with pytest.raises(TypeError, match="timeout must be a number"):
            Job("t", "id", timeout=True)

    def test_backoff_base_invalid_types_rejected(self):
        """防御触发：backoff_base 非数值/bool/NaN/负值拒绝——
        timeout 参数类型严格校验：
        TypeError、True 被静默接受为 1.0。"""
        with pytest.raises(TypeError, match="backoff_base must be a number"):
            Job("t", "id", backoff_base="2.0")
        with pytest.raises(TypeError, match="backoff_base must be a number"):
            Job("t", "id", backoff_base=True)
        with pytest.raises(ValueError, match="backoff_base must be finite"):
            Job("t", "id", backoff_base=float("nan"))
        with pytest.raises(ValueError, match="backoff_base must be finite"):
            Job("t", "id", backoff_base=float("inf"))
        with pytest.raises(ValueError, match="backoff_base must be finite"):
            Job("t", "id", backoff_base=-1.0)

    def test_backoff_max_invalid_types_rejected(self):
        """backoff_max 非数值/bool/NaN/负值拒绝。"""
        with pytest.raises(TypeError, match="backoff_max must be a number"):
            Job("t", "id", backoff_max="300.0")
        with pytest.raises(TypeError, match="backoff_max must be a number"):
            Job("t", "id", backoff_max=True)
        with pytest.raises(ValueError, match="backoff_max must be finite"):
            Job("t", "id", backoff_max=float("nan"))
        with pytest.raises(ValueError, match="backoff_max must be finite"):
            Job("t", "id", backoff_max=-5.0)


# ── Package integrity ────────────────────────────────────────────────────


class TestPackageExports:
    """Verify __init__.py re-exports match actual imports (lint guard)."""

    def test_all_matches_imports(self):
        """__all__ list must exactly match the symbols imported or assigned at module level."""
        import ast
        import tasklite

        init_path = tasklite.__file__
        with open(init_path) as f:
            source = f.read()

        tree = ast.parse(source)

        defined_names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    defined_names.add(alias.name)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id != "__all__":
                        defined_names.add(target.id)
            elif isinstance(node, ast.AnnAssign):
                if isinstance(node.target, ast.Name) and node.target.id != "__all__":
                    defined_names.add(node.target.id)

        exported = set(tasklite.__all__)
        assert defined_names == exported, (
            f"__all__ mismatch:\n"
            f"  In imports/assigns but not __all__: {defined_names - exported}\n"
            f"  In __all__ but not imported/assigned: {exported - defined_names}"
        )


# ── FATAL_EXCEPTIONS + Job defaults (from test_pipeline.py) ─────────────


class TestFatalExceptionTypes:
    """Verify FATAL_EXCEPTIONS tuple contains correct types.

    异常分类调整：ValueError 已从 FATAL 移除（过宽，含 JSONDecodeError
    等瞬态子类，应走 Transient 分类）；连接类瞬态异常归入 TRANSIENT_EXCEPTIONS。
    """

    def test_fatal_exceptions_tuple(self):
        """FATAL_EXCEPTIONS 含确定性 bug 类，不含 ValueError。"""
        from tasklite.taxonomy import FATAL_EXCEPTIONS
        assert TypeError in FATAL_EXCEPTIONS
        assert KeyError in FATAL_EXCEPTIONS
        assert AttributeError in FATAL_EXCEPTIONS
        assert ValueError not in FATAL_EXCEPTIONS, \
            "ValueError 过宽（含 JSONDecodeError 等瞬态子类），已移出 FATAL"

    def test_transient_exceptions_tuple(self):
        """TRANSIENT_EXCEPTIONS 含连接/超时类瞬态异常。"""
        from tasklite.taxonomy import TRANSIENT_EXCEPTIONS
        assert ConnectionError in TRANSIENT_EXCEPTIONS
        assert TimeoutError in TRANSIENT_EXCEPTIONS
        assert ConnectionResetError in TRANSIENT_EXCEPTIONS
        assert BrokenPipeError in TRANSIENT_EXCEPTIONS


class TestJobDefaults:
    """Verify Job default values."""

    def test_job_defaults(self):
        job = Job("t", "id")
        assert job.retries == 0
        assert job.max_retries == 3
        assert job.timeout == 3600
        assert job.backoff_base == 2.0
        assert job.backoff_max == 300.0
        assert job.payload == {}
        assert job.resources == {}
        assert job.depends_on == []


# ── Edge case / adversarial Job construction (invariant verification) ───


class TestJobWeirdInputs:
    """Bizarre and adversarial Job construction cases.

    These tests pin down the current (often permissive) behavior of the Job
    data model so that any future tightening of validation is a visible,
    intentional change rather than a silent regression.
    """

    def test_negative_max_retries(self):
        """max_retries=-1 now raises ValueError ( fix: must be >= 0)."""
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            Job("t", "id", max_retries=-1)

    def test_negative_retries(self):
        """retries 负值被入口拒绝（此前零延迟重试浪费多轮）。"""
        with pytest.raises(ValueError, match="retries must be >= 0"):
            Job("t", "id", retries=-2)

    def test_retries_exceeds_max_retries(self):
        """Inconsistent state retries=5, max_retries=3 is preserved verbatim."""
        job = Job("t", "id", retries=5, max_retries=3)
        assert job.retries == 5
        assert job.max_retries == 3
        assert job.retries > job.max_retries

    def test_float_timeout_accepted(self):
        """Type hint says int, but Python dynamism allows float timeout."""
        job = Job("t", "id", timeout=1.5)
        assert job.timeout == 1.5
        assert isinstance(job.timeout, float)

    def test_negative_timeout_raises_valueerror(self):
        """Negative timeout now raises ValueError per  contract (timeout must be > 0)."""
        with pytest.raises(ValueError, match="timeout must be finite and > 0"):
            Job("t", "id", timeout=-1)

    def test_zero_max_retries(self):
        """max_retries=0 means any failure goes straight to DLQ."""
        job = Job("t", "id", max_retries=0)
        assert job.max_retries == 0
        # retries(0) >= max_retries(0) → already at limit
        assert job.retries >= job.max_retries

    def test_boolean_job_id_rejected(self):
        """job_id=True 不再被 str() 强转为 'True'——拒绝。"""
        with pytest.raises(TypeError, match="job_id"):
            Job("t", True)

    def test_float_job_id_rejected(self):
        """job_id=3.14 不再被 str() 强转为 '3.14'——拒绝。"""
        with pytest.raises(TypeError, match="job_id"):
            Job("t", 3.14)

    def test_very_long_job_id(self):
        """ 起超长 job_id 被拒（旧行为 10000 字符保留 → uid 派生 IPC 文件名
        超 255 字节 → ENAMETOOLONG → 派发 livelock）。合法长 id 仍保留格式。"""
        from tasklite.utils.lockfile import safe_uid_filename
        # 转义后 ≤199 字节：干净 id 上限 = 190（"t::"→"t%3A%3A" 膨胀 +4，3+6+190=199）
        long_but_ok = "x" * 190
        job = Job("t", long_but_ok)
        assert job.job_id == long_but_ok
        assert job.uid == f"t::{long_but_ok}"
        assert len(safe_uid_filename(job.uid)) <= 199
        # 超长：入口 fail-fast（pre-fix 构造成功 → run 时 ENAMETOOLONG livelock）
        with pytest.raises(ValueError, match="too long"):
            Job("t", "x" * 10000)

    def test_payload_with_nested_mutables_shared_reference(self):
        """Documents that payload is NOT deep-copied — mutations leak.

        This is intentional Python behavior; the test pins it so a future
        defensive copy is a visible change.
        """
        nested_list = [1, 2, 3]
        job = Job("t", "id", payload={"items": nested_list})
        nested_list.append(4)
        # The payload reflects the mutation because no copy is made.
        assert job.payload["items"] == [1, 2, 3, 4]

    def test_to_dict_with_non_serializable_payload(self):
        """to_dict() itself does NOT json-serialize — only the backend does.

        A payload containing a function is fine for to_dict(); it would only
        blow up later when the backend calls json.dumps.
        """
        def my_func():
            pass
        job = Job("t", "id", payload={"fn": my_func})
        d = job.to_dict()
        assert d["payload"]["fn"] is my_func
        # Sanity: json.dumps would indeed fail — but to_dict must not.
        import json
        with pytest.raises(TypeError):
            json.dumps(d)

    def test_depends_on_with_empty_string(self):
        """depends_on=[''] is accepted — '' is a valid (if unusual) uid string."""
        job = Job("t", "id", depends_on=[""])
        assert job.depends_on == [""]

    def test_depends_on_self(self):
        """Self-dependency is constructible (deadlock detected at runtime)."""
        job = Job("t", "j1", depends_on=["t::j1"])
        assert job.depends_on == ["t::j1"]
        assert job.uid in job.depends_on

    def test_depends_on_non_string_raises_typeerror(self):
        """depends_on 元素非字符串时立即抛出 TypeError，避免静默死锁。"""
        with pytest.raises(TypeError, match="depends_on must contain only strings"):
            Job("t", "j1", depends_on=[123])

    def test_depends_on_mixed_types_raises_typeerror(self):
        """depends_on 包含一个非字符串也抛出 TypeError。"""
        with pytest.raises(TypeError, match="depends_on must contain only strings"):
            Job("t", "j1", depends_on=["a::b", None])

    def test_uid_with_double_colon_in_task_type_rejected(self):
        """task_type 包含 '::' 时抛出 ValueError，避免 uid() 分隔符碰撞。"""
        with pytest.raises(ValueError, match="task_type must not contain '::'"):
            Job("a::b", "c")

    def test_uid_with_double_colon_in_job_id_rejected(self):
        """job_id 包含 '::' 时抛出 ValueError，避免 uid() 分隔符碰撞。"""
        with pytest.raises(ValueError, match="job_id must not contain '::'"):
            Job("a", "b::c")

    def test_oversized_job_id_rejected(self):
        """uid 派生的 IPC 文件名（锁/结果）超 255 字节 → ENAMETOOLONG →
        派发 livelock（永远 requeue 且不进 DLQ）。入口按转义后字节数拒绝。

        pre-fix：Job("t", "x"*1000) 构造成功 → run 时 executor 创建
        {uid}.lock 抛 ENAMETOOLONG → 未分类异常 → 崩溃重启循环。
        """
        from tasklite.utils.lockfile import safe_uid_filename
        long_id = "x" * 400
        assert len(safe_uid_filename(f"t::{long_id}")) > 199  # 变异说明：确认超限
        with pytest.raises(ValueError, match="too long"):
            Job("t", long_id)

    def test_uid_filename_boundary_accepted(self):
        """转义后 ≤199 字节的 uid 仍可构造（边界不误杀）。"""
        from tasklite.utils.lockfile import safe_uid_filename
        # 干净 job_id：safe_uid_filename 只把 :: 转义为 %3A%3A（+4 字节）
        ok_id = "k" * 190
        assert len(safe_uid_filename(f"t::{ok_id}")) <= 199
        Job("t", ok_id)  # 不抛


class TestJobRuntimeState:
    """JobRuntimeState 强类型状态测试。"""

    def test_default_empty(self):
        st = JobRuntimeState()
        assert st.backoff_until is None
        assert st.backoff_wall_deadline is None
        assert st.commit_failures == 0
        assert st.dispatch_failures == 0
        assert st.last_retry_error == ""
        assert st.to_dict() == {}

    def test_roundtrip_dict(self):
        data = {
            "_backoff_until": 123.45,
            "_backoff_wall_deadline": 678.90,
            "_commit_failures": 2,
            "_dispatch_failures": 1,
            "_last_retry_error": "Connection reset",
        }
        st = JobRuntimeState.from_dict(data)
        assert st.backoff_until == 123.45
        assert st.backoff_wall_deadline == 678.90
        assert st.commit_failures == 2
        assert st.dispatch_failures == 1
        assert st.last_retry_error == "Connection reset"
        assert st.to_dict() == data

    def test_none_safe(self):
        st = JobRuntimeState.from_dict(None)
        assert st.commit_failures == 0
        assert st.to_dict() == {}

    def test_extra_same_name_keys_stay_lossless(self):
        # runtime 命名空间仅 `_` 前缀名为框架字段；与字段同名的无下划线键
        # 属用户 extra 数据——非数值不静默丢弃、数值不被劫持为框架字段
        data = {"backoff_until": "user-data", "commit_failures": "note", "note": "x"}
        st = JobRuntimeState.from_dict(data)
        assert st.backoff_until is None
        assert st.commit_failures == 0
        assert st.extra == data
        assert st.to_dict() == data

        st2 = JobRuntimeState.from_dict({"backoff_until": 123.5})
        assert st2.backoff_until is None
        assert st2.extra == {"backoff_until": 123.5}
        # 映射协议读取同名键直达 extra，不被字段别名遮蔽
        assert st2["backoff_until"] == 123.5
        st2["backoff_until"] = 1
        assert st2.backoff_until is None
        assert st2.extra["backoff_until"] == 1

    def test_stale_loose_same_name_key_cannot_revive_field(self):
        # 同时含规范键与无下划线同名键：字段只认规范键；规范键清空后，
        # 滞留的同名 extra 键不得在下次反序列化时复活陈旧框架值
        st = JobRuntimeState.from_dict({"_backoff_until": 5.0, "backoff_until": 99.0})
        assert st.backoff_until == 5.0
        assert st.extra == {"backoff_until": 99.0}

        st.backoff_until = None
        back = st.to_dict()
        assert back == {"backoff_until": 99.0}
        st2 = JobRuntimeState.from_dict(back)
        assert st2.backoff_until is None


class TestOversizedIntNumberValidation:
    """超出 float 范围的超大 int（如 10**400）走既有 ValueError 通道明确报错，
    不得以裸 OverflowError 逃逸（ArithmeticError 子类会命中 FATAL 启发式）。"""

    def test_job_number_fields_reject_oversized_int_as_value_error(self):
        huge = 10 ** 400
        for field in ("timeout", "backoff_base", "backoff_max"):
            with pytest.raises(ValueError):
                Job("t", "j", **{field: huge})

    def test_runtime_state_and_suspend_resource_reject_oversized_int(self):
        from tasklite.models.context import TaskContext

        huge = 10 ** 400
        # 持久化恢复容灾：溢出值按非有限降级为 None，与 NaN/inf 同路
        st = JobRuntimeState.from_dict({"_backoff_until": huge, "_backoff_wall_deadline": huge})
        assert st.backoff_until is None
        assert st.backoff_wall_deadline is None
        ctx = fake_ctx(Job("t", "j"))
        with pytest.raises(ValueError):
            ctx.suspend_resource("api", huge)
