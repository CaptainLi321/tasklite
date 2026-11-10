"""FileSpec 值语义、配置序列化与同执行产物声明测试。"""
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from tasklite import FileSpec, TaskLiteError
from tasklite.utils import jsonutil
from tasklite.models.artifact import (
    FILE_ROLES,
    ArtifactDeclaration,
    merge_named_file_spec,
    resolve_job_files_config,
)


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
    def test_input_allows_failure_and_success_deletion_but_forbids_delete_existing(
        self, delete_on_failure, delete_on_success
    ):
        spec = FileSpec(
            "in/raw.csv",
            role="input",
            delete_existing=False,
            delete_on_failure=delete_on_failure,
            delete_on_success=delete_on_success,
        )
        assert spec.role == "input"
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
        assert ref.role == "input"
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

    def test_invalid_role_or_non_boolean_flags_rejected(self):
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

    def test_invalid_path_and_from_output_args_rejected(self):
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


class TestFileSpecSerializationAndFilesystemSafety:
    """Path 输入序列化恢复、不可变性与零磁盘副作用。"""

    def test_path_input_serializes_and_roundtrips_via_json(self, tmp_path):
        path_obj = tmp_path / "nested" / ".." / "results" / "report.csv"
        spec = FileSpec(
            path_obj,
            role="output",
            delete_existing=True,
            delete_on_failure=True,
            delete_on_success=False,
        )
        assert spec.path == str(tmp_path / "results" / "report.csv")
        payload = jsonutil.dumps(spec.to_dict())
        restored = FileSpec.from_dict(jsonutil.loads(payload))
        assert restored == spec
        assert restored.to_dict() == {
            "path": str(tmp_path / "results" / "report.csv"),
            "role": "output",
            "delete_existing": True,
            "delete_on_failure": True,
            "delete_on_success": False,
        }
        with pytest.raises(FrozenInstanceError):
            spec.path = "/other"  # type: ignore[misc]

    def test_from_output_serializes_and_roundtrips_via_json(self):
        ref = FileSpec.from_output(
            "generate::report-001",
            "report",
            role="input",
            delete_on_success=True,
        )
        assert ref.is_output_ref is True
        assert ref.path is None
        assert ref.producer_uid == "generate::report-001"
        assert ref.output_name == "report"
        data = ref.to_dict()
        assert data == {
            "producer_uid": "generate::report-001",
            "output_name": "report",
            "role": "input",
            "delete_existing": False,
            "delete_on_failure": False,
            "delete_on_success": True,
        }
        restored = FileSpec.from_dict(jsonutil.loads(jsonutil.dumps(data)))
        assert restored == ref

    def test_from_dict_rejects_unknown_or_mixed_keys(self):
        valid_path = FileSpec("/tmp/a.txt", role="output").to_dict()
        with pytest.raises(TaskLiteError, match="Invalid FileSpec dict keys"):
            FileSpec.from_dict({**valid_path, "sandbox": True})
        with pytest.raises(TaskLiteError, match="Invalid FileSpec dict keys"):
            FileSpec.from_dict({**valid_path, "producer_uid": "p::1", "output_name": "o"})

    def test_construction_and_serialization_never_create_dirs_or_delete_files(self, tmp_path):
        existing_file = tmp_path / "existing.txt"
        existing_file.write_text("keep-me", encoding="utf-8")
        missing_dir_target = tmp_path / "nonexistent_dir" / "sub" / "out.csv"

        spec_existing = FileSpec(
            existing_file,
            role="output",
            delete_existing=True,
            delete_on_failure=True,
            delete_on_success=True,
        )
        spec_missing = FileSpec(
            "nonexistent_dir/sub/out.csv",
            role="output",
            delete_existing=True,
        )
        resolved = resolve_job_files_config(
            {"a": spec_existing, "b": spec_missing},
            base_dir=tmp_path,
        )
        assert resolved["a"]["path"] == str(existing_file)
        assert resolved["b"]["path"] == str(missing_dir_target)
        assert existing_file.read_text(encoding="utf-8") == "keep-me"
        assert not (tmp_path / "nonexistent_dir").exists()


class TestNamedFileSpecComparisonAndOverlap:
    """同名规范声明幂等复用与不同名重叠路径允许。"""

    def test_same_name_canonical_declaration_is_reusable(self, tmp_path):
        first = FileSpec("results/./report.csv", role="output", delete_existing=True)
        second = FileSpec(tmp_path / "results" / "report.csv", role="output", delete_existing=True)

        merged, is_new = merge_named_file_spec({}, "report", first, base_dir=tmp_path)
        assert is_new is True
        assert merged["report"].path == str(tmp_path / "results" / "report.csv")

        merged_again, is_new_again = merge_named_file_spec(
            merged, "report", second, base_dir=tmp_path
        )
        assert is_new_again is False
        assert merged_again == merged

    def test_same_output_reference_is_reusable(self, tmp_path):
        first = FileSpec.from_output("build::one", "report", role="input")
        second = FileSpec.from_output("build::one", "report", role="input")
        merged, is_new = merge_named_file_spec(
            {}, "source", first, base_dir=tmp_path, depends_on=["build::one"]
        )
        assert is_new is True
        merged_again, is_new_again = merge_named_file_spec(
            merged,
            "source",
            second,
            base_dir=tmp_path,
            depends_on=["build::one"],
        )
        assert is_new_again is False
        assert merged_again["source"] == first

    @pytest.mark.parametrize(
        "conflicting",
        [
            FileSpec("results/other.csv", role="output", delete_existing=True),
            FileSpec("results/report.csv", role="cache", delete_existing=True),
            FileSpec("results/report.csv", role="output", delete_existing=False),
            FileSpec.from_output("p::1", "report", role="output", delete_existing=True),
        ],
    )
    def test_same_name_different_declaration_raises_conflict(self, tmp_path, conflicting):
        base = FileSpec("results/report.csv", role="output", delete_existing=True)
        merged, _ = merge_named_file_spec({}, "report", base, base_dir=tmp_path)
        with pytest.raises(TaskLiteError, match="conflicts"):
            merge_named_file_spec(
                merged, "report", conflicting, base_dir=tmp_path, depends_on=["p::1"]
            )

    def test_different_names_allow_same_path_overlapping_dirs_and_input_output_roles(
        self, tmp_path
    ):
        shared = tmp_path / "work" / "data.csv"
        parent_dir = tmp_path / "work"
        child_file = tmp_path / "work" / "sub" / "part.bin"

        files: dict[str, FileSpec] = {}
        files, is_new = merge_named_file_spec(
            files, "raw_in", FileSpec(shared, role="input"), base_dir=tmp_path
        )
        assert is_new is True
        files, is_new = merge_named_file_spec(
            files,
            "in_place_out",
            FileSpec(shared, role="output", delete_existing=True),
            base_dir=tmp_path,
        )
        assert is_new is True
        files, is_new = merge_named_file_spec(
            files, "work_dir", FileSpec(parent_dir, role="cache"), base_dir=tmp_path
        )
        assert is_new is True
        files, is_new = merge_named_file_spec(
            files, "nested_part", FileSpec(child_file, role="output"), base_dir=tmp_path
        )
        assert is_new is True
        assert set(files.keys()) == {"raw_in", "in_place_out", "work_dir", "nested_part"}


def test_named_internal_cleanup_description_allows_success_deletion(tmp_path):
    declaration = ArtifactDeclaration(
        str(tmp_path / "result"),
        name="result",
        delete_on_success=True,
    )
    assert declaration.name == "result"
    assert declaration.delete_on_success is True


@pytest.mark.parametrize("suffix", ["nested/../result", "./result", "nested//result", "result/"])
def test_declaration_rejects_non_normalized_absolute_paths(tmp_path, suffix):
    with pytest.raises(ValueError, match="normalized"):
        ArtifactDeclaration(f"{tmp_path}/{suffix}")


@pytest.mark.parametrize("field", ["delete_existing", "delete_on_failure", "delete_on_success", "sandbox"])
def test_truthy_non_boolean_authorization_is_not_accepted(tmp_path, field):
    with pytest.raises(TypeError, match="bool"):
        ArtifactDeclaration(str(tmp_path / "result"), **{field: "false"})
