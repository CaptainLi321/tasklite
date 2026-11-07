"""FileSpec 值语义、配置序列化与同执行产物声明测试。"""
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from tasklite import FileSpec, TaskLiteError, jsonutil
from tasklite.models.artifact import (
    FILE_ROLES,
    ArtifactDeclaration,
    merge_named_file_spec,
    resolve_job_files_config,
    validate_artifact_declarations,
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

        with pytest.raises(ValueError, match="delete_existing"):
            FileSpec("in/raw.csv", role="input", delete_existing=True)
        with pytest.raises(ValueError, match="delete_existing"):
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
        with pytest.raises(TypeError, match="role must be a str"):
            FileSpec("a.txt", role=123)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="role must be one of"):
            FileSpec("a.txt", role="temp")  # type: ignore[arg-type]
        for field in ("delete_existing", "delete_on_failure", "delete_on_success"):
            with pytest.raises(TypeError, match="bool"):
                FileSpec("a.txt", role="output", **{field: 1})  # type: ignore[arg-type]

    def test_invalid_path_and_from_output_args_rejected(self):
        with pytest.raises(TypeError, match="str or Path"):
            FileSpec(123, role="output")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="non-empty"):
            FileSpec("", role="output")
        with pytest.raises(ValueError, match="null bytes"):
            FileSpec("a\x00b", role="output")
        with pytest.raises(TypeError, match="producer_uid must be a str"):
            FileSpec.from_output(1, "out", role="input")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="producer_uid must be a non-empty str"):
            FileSpec.from_output("", "out", role="input")
        with pytest.raises(TypeError, match="output_name must be a str"):
            FileSpec.from_output("p::1", None, role="input")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="output_name must be a non-empty str"):
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
        with pytest.raises(ValueError, match="Invalid FileSpec dict keys"):
            FileSpec.from_dict({**valid_path, "sandbox": True})
        with pytest.raises(ValueError, match="Invalid FileSpec dict keys"):
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


def test_named_artifact_cannot_request_success_deletion(tmp_path):
    with pytest.raises(ValueError, match="name"):
        ArtifactDeclaration(str(tmp_path / "result"), name="result", delete_on_success=True)


def test_repeating_same_declaration_is_idempotent(tmp_path):
    item = ArtifactDeclaration(str(tmp_path / "result"), name="result")
    assert validate_artifact_declarations([item, item]) == (item,)


@pytest.mark.parametrize("case", ["same_name", "alias", "rename", "permission", "nested"])
def test_conflicting_declarations_are_rejected(tmp_path, case):
    first = ArtifactDeclaration(str(tmp_path / "result"), name="result")
    other = {
        "same_name": ArtifactDeclaration(str(tmp_path / "another"), name="result"),
        "alias": ArtifactDeclaration(first.path, name="alias"),
        "rename": ArtifactDeclaration(first.path),
        "permission": ArtifactDeclaration(first.path, name="result", delete_existing=True),
        "nested": ArtifactDeclaration(str(Path(first.path) / "child")),
    }[case]
    with pytest.raises(ValueError):
        validate_artifact_declarations([first, other])


def test_declaration_roundtrip_contains_only_description_and_authorization(tmp_path):
    declaration = ArtifactDeclaration(str(tmp_path / "result"), name="result",
                                      delete_existing=True, delete_on_failure=True, sandbox=False)
    data = declaration.to_dict()
    assert data == {"path": str(tmp_path / "result"), "name": "result", "delete_existing": True,
                    "delete_on_failure": True, "delete_on_success": False, "sandbox": False}
    assert ArtifactDeclaration.from_dict(data) == declaration
    with pytest.raises(TypeError):
        ArtifactDeclaration.from_dict({**data, "accepted": True})


@pytest.mark.parametrize("suffix", ["nested/../result", "./result", "nested//result", "result/"])
def test_declaration_rejects_non_normalized_absolute_paths(tmp_path, suffix):
    with pytest.raises(ValueError, match="normalized"):
        ArtifactDeclaration(f"{tmp_path}/{suffix}")


@pytest.mark.parametrize("field", ["delete_existing", "delete_on_failure", "delete_on_success", "sandbox"])
def test_truthy_non_boolean_authorization_is_not_accepted(tmp_path, field):
    with pytest.raises(TypeError, match="bool"):
        ArtifactDeclaration(str(tmp_path / "result"), **{field: "false"})
