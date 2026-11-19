"""TaskResult 显式返回结果与 FileSpec 值语义/合并单元测试。"""
from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

import tasklite
from tasklite import FileSpec, TaskLiteError, TaskResult
from tasklite.models import TaskResult as ModelsTaskResult
from tasklite.models.artifact import (
    FILE_ROLES,
    ArtifactDeclaration,
    merge_named_file_spec,
    resolve_job_files_config,
)
from tasklite.models.result import TaskResult as DirectTaskResult
from tasklite.utils import jsonutil


class TestTaskResultModel:
    """TaskResult 导出面、静态构造器、message 校验与不可变性。"""

    def test_exported_from_root_and_models(self):
        assert TaskResult is ModelsTaskResult
        assert TaskResult is DirectTaskResult
        assert "TaskResult" in tasklite.__all__

    def test_direct_instantiation_and_positional_args_rejected(self):
        for bad_call in (
            lambda: TaskResult(),
            lambda: TaskResult("success"),
            lambda: TaskResult(status="success"),
        ):
            with pytest.raises(TaskLiteError, match="cannot be instantiated directly"):
                bad_call()
        with pytest.raises(TypeError):
            TaskResult.success("done")  # type: ignore[misc]
        with pytest.raises(TypeError):
            TaskResult.failure("err")  # type: ignore[misc]
        with pytest.raises(TypeError):
            TaskResult.retry("later")  # type: ignore[misc]

    def test_factories_defaults_fields_and_consume_budget(self):
        s_def = TaskResult.success()
        assert (s_def.status, s_def.message, s_def.metadata, s_def.consume_budget) == (
            "success", None, {}, False
        )
        s_full = TaskResult.success(
            message="downloaded",
            metadata={"count": 3, "items": ["a", "b"], "nested": {"ok": True}},
        )
        assert s_full.status == "success"
        assert s_full.message == "downloaded"
        assert s_full.metadata == {"count": 3, "items": ["a", "b"], "nested": {"ok": True}}

        f_def = TaskResult.failure()
        assert (f_def.status, f_def.message, f_def.metadata, f_def.consume_budget) == (
            "failure", None, {}, False
        )
        f_full = TaskResult.failure(message="checksum mismatch", metadata={"expected": "abc"})
        assert f_full.status == "failure" and f_full.message == "checksum mismatch"

        r_def = TaskResult.retry()
        assert (r_def.status, r_def.message, r_def.metadata, r_def.consume_budget) == (
            "retry", None, {}, True
        )
        r_no_budget = TaskResult.retry(
            message="rate limited",
            metadata={"retry_after": 30.0},
            consume_budget=False,
        )
        assert r_no_budget.consume_budget is False

        with pytest.raises(TypeError):
            TaskResult.success(consume_budget=False)  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            TaskResult.failure(consume_budget=False)  # type: ignore[call-arg]

        class _BoolLikeInt(int):
            pass

        for bad_budget in (1, 0, None, "true", _BoolLikeInt(1)):
            with pytest.raises(TaskLiteError, match="consume_budget"):
                TaskResult.retry(consume_budget=bad_budget)  # type: ignore[arg-type]

    def test_message_validation_defensive_copy_and_immutability(self):
        class _SubStr(str):
            pass

        for bad_msg in (123, b"bytes", ["msg"], _SubStr("sub")):
            with pytest.raises(TaskLiteError, match="message"):
                TaskResult.success(message=bad_msg)  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="UTF-8"):
            TaskResult.failure(message="\ud800")

        source: dict[str, Any] = {"nested": {"items": [1, 2]}}
        res = TaskResult.success(metadata=source)
        source["nested"]["items"].append(3)
        assert res.metadata == {"nested": {"items": [1, 2]}}
        res.metadata["nested"]["items"].append(99)
        assert res.metadata == {"nested": {"items": [1, 2]}}

        with pytest.raises(AttributeError, match="immutable"):
            res.status = "failure"  # type: ignore[misc]
        with pytest.raises(AttributeError, match="immutable"):
            del res.message  # type: ignore[misc]

        r1 = TaskResult.retry(message="wait", metadata={"k": 1}, consume_budget=False)
        r2 = TaskResult.retry(message="wait", metadata={"k": 1}, consume_budget=False)
        r3 = TaskResult.retry(message="wait", metadata={"k": 1}, consume_budget=True)
        assert r1 == r2 and r1 != r3
        assert "TaskResult.retry" in repr(r1)
        assert "TaskResult.success" in repr(TaskResult.success(message="ok"))


class TestFileSpecRolesAndCleanup:
    """三 role 与删除选项组合契约。"""

    @pytest.mark.parametrize("role", FILE_ROLES)
    def test_defaults_all_cleanup_flags_false(self, role):
        spec = FileSpec("data/item.txt", role=role)
        assert spec.path == "data/item.txt"
        assert spec.producer_uid is None
        assert spec.output_name is None
        assert spec.is_output_ref is False
        assert spec.role == role
        assert spec.delete_existing is False
        assert spec.delete_on_failure is False
        assert spec.delete_on_success is False

    @pytest.mark.parametrize("delete_on_failure", [False, True])
    @pytest.mark.parametrize("delete_on_success", [False, True])
    def test_input_allows_post_cleanup_but_forbids_delete_existing(
        self, delete_on_failure, delete_on_success
    ):
        spec = FileSpec(
            "in/raw.csv",
            role="input",
            delete_existing=False,
            delete_on_failure=delete_on_failure,
            delete_on_success=delete_on_success,
        )
        assert spec.delete_on_failure is delete_on_failure
        assert spec.delete_on_success is delete_on_success

        ref = FileSpec.from_output(
            "gen::1",
            "raw",
            role="input",
            delete_existing=False,
            delete_on_failure=delete_on_failure,
            delete_on_success=delete_on_success,
        )
        assert ref.delete_on_failure is delete_on_failure
        assert ref.delete_on_success is delete_on_success

        with pytest.raises(TaskLiteError, match="delete_existing"):
            FileSpec("in/raw.csv", role="input", delete_existing=True)
        with pytest.raises(TaskLiteError, match="delete_existing"):
            FileSpec.from_output("gen::1", "raw", role="input", delete_existing=True)

    @pytest.mark.parametrize("role", ["output", "cache"])
    @pytest.mark.parametrize("delete_existing", [False, True])
    @pytest.mark.parametrize("delete_on_failure", [False, True])
    @pytest.mark.parametrize("delete_on_success", [False, True])
    def test_output_and_cache_allow_all_cleanup_combinations(
        self, role, delete_existing, delete_on_failure, delete_on_success
    ):
        spec = FileSpec(
            "out/file.bin",
            role=role,
            delete_existing=delete_existing,
            delete_on_failure=delete_on_failure,
            delete_on_success=delete_on_success,
        )
        assert spec.role == role
        assert spec.delete_existing is delete_existing
        assert spec.delete_on_failure is delete_on_failure
        assert spec.delete_on_success is delete_on_success

    def test_invalid_args_rejected(self):
        with pytest.raises(TypeError, match="positional argument"):
            FileSpec("a.txt", "output")  # type: ignore[misc]
        with pytest.raises(TypeError, match="role"):
            FileSpec("a.txt")  # type: ignore[call-arg]
        with pytest.raises(TaskLiteError, match="role must be a str"):
            FileSpec("a.txt", role=123)  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="role must be one of"):
            FileSpec("a.txt", role="temp")  # type: ignore[arg-type]
        for field in ("delete_existing", "delete_on_failure", "delete_on_success"):
            with pytest.raises(TaskLiteError, match="bool"):
                FileSpec("a.txt", role="output", **{field: 1})  # type: ignore[arg-type]

        with pytest.raises(TaskLiteError, match="str or Path"):
            FileSpec(123, role="output")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="non-empty"):
            FileSpec("", role="output")
        with pytest.raises(TaskLiteError, match="null bytes"):
            FileSpec("a\x00b", role="output")
        with pytest.raises(TaskLiteError, match="producer_uid must be a str"):
            FileSpec.from_output(1, "out", role="input")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="producer_uid must be a non-empty str"):
            FileSpec.from_output("", "out", role="input")
        with pytest.raises(TaskLiteError, match="output_name must be a str"):
            FileSpec.from_output("p::1", None, role="input")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="output_name must be a non-empty str"):
            FileSpec.from_output("p::1", "", role="input")


class TestFileSpecSerializationAndMerging:
    """FileSpec 序列化恢复、零磁盘副作用与同名合并。"""

    def test_path_and_from_output_roundtrip_via_json(self, tmp_path):
        path_obj = tmp_path / "nested" / ".." / "results" / "report.csv"
        spec = FileSpec(
            path_obj,
            role="output",
            delete_existing=True,
            delete_on_failure=True,
            delete_on_success=False,
        )
        assert spec.path == str(tmp_path / "results" / "report.csv")
        restored = FileSpec.from_dict(jsonutil.loads(jsonutil.dumps(spec.to_dict())))
        assert restored == spec
        with pytest.raises(FrozenInstanceError):
            spec.path = "/other"  # type: ignore[misc]

        ref = FileSpec.from_output("generate::report-001", "report", role="input", delete_on_success=True)
        assert ref.is_output_ref is True and ref.path is None
        assert FileSpec.from_dict(jsonutil.loads(jsonutil.dumps(ref.to_dict()))) == ref

        valid_path = FileSpec("/tmp/a.txt", role="output").to_dict()
        with pytest.raises(TaskLiteError, match="Invalid FileSpec dict keys"):
            FileSpec.from_dict({**valid_path, "sandbox": True})
        with pytest.raises(TaskLiteError, match="Invalid FileSpec dict keys"):
            FileSpec.from_dict({**valid_path, "producer_uid": "p::1", "output_name": "o"})

    def test_construction_and_resolution_have_no_filesystem_side_effects(self, tmp_path):
        existing_file = tmp_path / "existing.txt"
        existing_file.write_text("keep-me", encoding="utf-8")
        missing_dir_target = tmp_path / "nonexistent_dir" / "sub" / "out.csv"

        resolved = resolve_job_files_config(
            {
                "a": FileSpec(existing_file, role="output", delete_existing=True, delete_on_failure=True, delete_on_success=True),
                "b": FileSpec("nonexistent_dir/sub/out.csv", role="output", delete_existing=True),
            },
            base_dir=tmp_path,
        )
        assert resolved["a"]["path"] == str(existing_file)
        assert resolved["b"]["path"] == str(missing_dir_target)
        assert existing_file.read_text(encoding="utf-8") == "keep-me"
        assert not (tmp_path / "nonexistent_dir").exists()

    def test_same_name_reusable_and_conflicts_rejected(self, tmp_path):
        first = FileSpec("results/./report.csv", role="output", delete_existing=True)
        second = FileSpec(tmp_path / "results" / "report.csv", role="output", delete_existing=True)
        merged, is_new = merge_named_file_spec({}, "report", first, base_dir=tmp_path)
        assert is_new is True
        merged_again, is_new_again = merge_named_file_spec(merged, "report", second, base_dir=tmp_path)
        assert is_new_again is False and merged_again == merged

        ref_a = FileSpec.from_output("build::one", "report", role="input")
        ref_b = FileSpec.from_output("build::one", "report", role="input")
        m_ref, is_new_ref = merge_named_file_spec({}, "source", ref_a, base_dir=tmp_path, depends_on=["build::one"])
        assert is_new_ref is True
        _, is_new_ref2 = merge_named_file_spec(m_ref, "source", ref_b, base_dir=tmp_path, depends_on=["build::one"])
        assert is_new_ref2 is False

        for conflicting in (
            FileSpec("results/other.csv", role="output", delete_existing=True),
            FileSpec("results/report.csv", role="cache", delete_existing=True),
            FileSpec("results/report.csv", role="output", delete_existing=False),
            FileSpec.from_output("p::1", "report", role="output", delete_existing=True),
        ):
            with pytest.raises(TaskLiteError, match="conflicts"):
                merge_named_file_spec(merged, "report", conflicting, base_dir=tmp_path, depends_on=["p::1"])

    def test_different_names_allow_overlapping_paths(self, tmp_path):
        shared = tmp_path / "work" / "data.csv"
        files: dict[str, FileSpec] = {}
        files, _ = merge_named_file_spec(files, "raw_in", FileSpec(shared, role="input"), base_dir=tmp_path)
        files, _ = merge_named_file_spec(files, "in_place_out", FileSpec(shared, role="output", delete_existing=True), base_dir=tmp_path)
        files, _ = merge_named_file_spec(files, "work_dir", FileSpec(tmp_path / "work", role="cache"), base_dir=tmp_path)
        files, _ = merge_named_file_spec(files, "nested_part", FileSpec(tmp_path / "work" / "sub" / "part.bin", role="output"), base_dir=tmp_path)
        assert set(files.keys()) == {"raw_in", "in_place_out", "work_dir", "nested_part"}


class TestArtifactDeclarationGuards:
    """内部 ArtifactDeclaration 规范化路径与布尔标志守卫。"""

    def test_named_declaration_and_rejections(self, tmp_path):
        declaration = ArtifactDeclaration(
            str(tmp_path / "result"),
            name="result",
            delete_on_success=True,
        )
        assert declaration.name == "result"
        assert declaration.delete_on_success is True

        for suffix in ("nested/../result", "./result", "nested//result", "result/"):
            with pytest.raises(ValueError, match="normalized"):
                ArtifactDeclaration(f"{tmp_path}/{suffix}")

        for field in ("delete_existing", "delete_on_failure", "delete_on_success", "sandbox"):
            with pytest.raises(TypeError, match="bool"):
                ArtifactDeclaration(str(tmp_path / "result"), **{field: "false"})
