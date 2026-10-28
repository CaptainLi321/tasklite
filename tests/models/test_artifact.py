"""产物授权值与同执行声明的一致性。"""
from pathlib import Path

import pytest

from tasklite.models.artifact import ArtifactDeclaration, validate_artifact_declarations


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
