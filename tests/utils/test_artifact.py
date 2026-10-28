"""产物路径与独立删除授权的文件系统行为。"""
from pathlib import Path

import pytest

from tasklite.models.artifact import ArtifactDeclaration
from tasklite.utils.artifact import ArtifactPathPolicy


def test_existing_output_requires_its_own_deletion_authorization(tmp_path):
    root = tmp_path / "output"
    root.mkdir()
    target = root / "result"
    target.write_text("existing", encoding="utf-8")
    policy = ArtifactPathPolicy(output_roots=[root])
    declaration = ArtifactDeclaration(str(target), delete_on_failure=True)
    with pytest.raises(FileExistsError):
        policy.remove_existing(declaration)
    assert target.read_text(encoding="utf-8") == "existing"
    policy.remove_existing(ArtifactDeclaration(str(target), delete_existing=True))
    assert not target.exists()


@pytest.mark.parametrize("is_directory", [False, True])
@pytest.mark.parametrize("success", [False, True])
def test_cleanup_obeys_only_the_selected_phase_authorization(tmp_path, is_directory, success):
    root = tmp_path / "output"
    root.mkdir()
    target = root / "result"
    if is_directory:
        target.mkdir()
        (target / "child").write_text("data", encoding="utf-8")
    else:
        target.write_text("data", encoding="utf-8")
    policy = ArtifactPathPolicy(output_roots=root)
    other_phase = ArtifactDeclaration(str(target), delete_existing=True,
                                      delete_on_success=not success, delete_on_failure=success)
    policy.cleanup(other_phase, success=success)
    assert target.exists()
    authorized = ArtifactDeclaration(str(target), delete_on_success=success,
                                     delete_on_failure=not success)
    policy.cleanup(authorized, success=success)
    assert not target.exists()
    policy.cleanup(authorized, success=success)


@pytest.mark.parametrize("link_target_exists", [False, True])
def test_leaf_symlinks_are_rejected_without_touching_target(tmp_path, link_target_exists):
    root = tmp_path / "output"
    root.mkdir()
    outside = tmp_path / "outside"
    if link_target_exists:
        outside.write_text("precious", encoding="utf-8")
    link = root / "link"
    link.symlink_to(outside)
    policy = ArtifactPathPolicy(output_roots=root)
    with pytest.raises(ValueError, match="symlink"):
        policy.resolve(link)
    assert link.is_symlink()
    if link_target_exists:
        assert outside.read_text(encoding="utf-8") == "precious"


def test_safe_parent_link_returns_canonical_path_but_escape_is_rejected(tmp_path):
    root = tmp_path / "output"
    root.mkdir()
    real = root / "real"
    real.mkdir()
    (root / "inside").symlink_to(real, target_is_directory=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "escape").symlink_to(outside, target_is_directory=True)
    policy = ArtifactPathPolicy(output_roots=root)
    assert policy.resolve("inside/result") == str(real / "result")
    with pytest.raises(ValueError):
        policy.resolve("escape/result")


@pytest.mark.parametrize("sandbox", [False, True])
def test_roots_and_engine_regions_are_protected_even_with_sandbox_disabled(tmp_path, sandbox):
    root = tmp_path / "output"
    state = tmp_path / "state"
    root.mkdir()
    state.mkdir()
    policy = ArtifactPathPolicy(output_roots=root, protected_roots=[state])
    for protected in [Path(root.anchor), tmp_path, root, state, state / "ipc" / "file"]:
        with pytest.raises(ValueError):
            policy.resolve(protected, sandbox=sandbox)
    assert policy.resolve(root / "dedicated", sandbox=sandbox) == str(root / "dedicated")


def test_directory_cleanup_unlinks_nested_link_without_following_it(tmp_path):
    root = tmp_path / "output"
    target = root / "directory"
    target.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "precious").write_text("keep", encoding="utf-8")
    (target / "link").symlink_to(outside, target_is_directory=True)
    policy = ArtifactPathPolicy(output_roots=root)
    policy.cleanup(ArtifactDeclaration(str(target), delete_on_success=True), success=True)
    assert not target.exists()
    assert (outside / "precious").read_text(encoding="utf-8") == "keep"


def test_disappearing_parent_during_directory_walk_is_not_reported_as_success(tmp_path, monkeypatch):
    import os
    root = tmp_path / "output"
    target = root / "directory"
    target.mkdir(parents=True)
    (target / "child").write_text("keep", encoding="utf-8")
    original_listdir = os.listdir
    def move_parent(fd):
        names = original_listdir(fd)
        root.rename(tmp_path / "moved")
        return names
    monkeypatch.setattr(os, "listdir", move_parent)
    monkeypatch.setattr(os, "supports_fd", os.supports_fd | {move_parent})
    policy = ArtifactPathPolicy(output_roots=root)
    with pytest.raises((OSError, ValueError)):
        policy.cleanup(ArtifactDeclaration(str(target), delete_on_failure=True), success=False)
    assert (tmp_path / "moved" / "directory" / "child").read_text(encoding="utf-8") == "keep"


def test_same_device_bind_mount_with_escaped_path_blocks_directory_cleanup(tmp_path, monkeypatch):
    root = tmp_path / "output"
    target = root / "directory"
    mounted = target / "bind space\\slash"
    mounted.mkdir(parents=True)
    (mounted / "precious").write_text("keep", encoding="utf-8")
    real_read = Path.read_text
    escaped = f"{target}/bind\\040space\\134slash"
    def mount_table(path, *args, **kwargs):
        if path == Path("/proc/self/mountinfo"):
            return f"1 0 0:1 / / rw - ext4 /dev/test rw\n2 1 0:1 / {escaped} rw - ext4 /dev/test rw\n"
        return real_read(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", mount_table)
    assert mounted.stat().st_dev == target.stat().st_dev
    policy = ArtifactPathPolicy(output_roots=root)
    with pytest.raises(ValueError, match="mount"):
        policy.cleanup(ArtifactDeclaration(str(target), delete_on_failure=True), success=False)
    assert (mounted / "precious").read_text(encoding="utf-8") == "keep"


def test_unknown_mount_boundaries_refuse_directory_deletion(tmp_path, monkeypatch):
    root = tmp_path / "output"
    target = root / "directory"
    target.mkdir(parents=True)
    (target / "precious").write_text("keep", encoding="utf-8")
    real_read = Path.read_text
    def denied_mount_table(path, *args, **kwargs):
        if path == Path("/proc/self/mountinfo"):
            raise PermissionError("mount information unavailable")
        return real_read(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", denied_mount_table)
    with pytest.raises(NotImplementedError):
        ArtifactPathPolicy(output_roots=root).cleanup(
            ArtifactDeclaration(str(target), delete_on_success=True), success=True)
    assert (target / "precious").read_text(encoding="utf-8") == "keep"


def test_inaccessible_output_is_reported_instead_of_treated_as_missing(tmp_path):
    import os
    if not hasattr(os, "geteuid") or os.geteuid() == 0:
        pytest.skip("需要普通用户的目录权限检查")
    root = tmp_path / "output"
    private = root / "private"
    private.mkdir(parents=True)
    target = private / "result"
    target.write_text("keep", encoding="utf-8")
    private.chmod(0)
    try:
        with pytest.raises(PermissionError):
            ArtifactPathPolicy(output_roots=root).cleanup(
                ArtifactDeclaration(str(target), delete_on_failure=True), success=False)
    finally:
        private.chmod(0o700)
    assert target.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize("use_link", [False, True])
def test_parent_replaced_during_open_is_rejected_without_deleting_replacement(tmp_path, monkeypatch, use_link):
    import os
    root = tmp_path / "output"
    parent = root / "parent"
    parent.mkdir(parents=True)
    target = parent / "result"
    target.write_text("original", encoding="utf-8")
    replacement = tmp_path / "replacement"
    replacement.mkdir()
    (replacement / "result").write_text("precious", encoding="utf-8")
    real_open = os.open
    changed = False
    def replace_parent(path, flags, *args, **kwargs):
        nonlocal changed
        if not changed and str(path) == str(parent) and "dir_fd" not in kwargs:
            changed = True
            parent.rename(root / "old_parent")
            if use_link:
                parent.symlink_to(replacement, target_is_directory=True)
            else:
                replacement.rename(parent)
        return real_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(os, "open", replace_parent)
    monkeypatch.setattr(os, "supports_dir_fd", os.supports_dir_fd | {replace_parent})
    with pytest.raises((ValueError, OSError)):
        ArtifactPathPolicy(output_roots=root).cleanup(
            ArtifactDeclaration(str(target), delete_on_failure=True), success=False)
    precious = replacement / "result" if use_link else parent / "result"
    assert precious.read_text(encoding="utf-8") == "precious"
    assert (root / "old_parent" / "result").read_text(encoding="utf-8") == "original"


def test_normal_leaf_replace_before_cleanup_is_allowed(tmp_path):
    import os
    root = tmp_path / "output"
    root.mkdir()
    target = root / "result"
    target.write_text("first", encoding="utf-8")
    declaration = ArtifactDeclaration(str(target), delete_on_success=True)
    temporary = root / "temporary"
    temporary.write_text("replacement", encoding="utf-8")
    os.replace(temporary, target)
    ArtifactPathPolicy(output_roots=root).cleanup(declaration, success=True)
    assert not target.exists()


def test_missing_non_following_capability_refuses_file_deletion(tmp_path, monkeypatch):
    import os
    root = tmp_path / "output"
    root.mkdir()
    target = root / "result"
    target.write_text("keep", encoding="utf-8")
    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    with pytest.raises(NotImplementedError):
        ArtifactPathPolicy(output_roots=root).cleanup(
            ArtifactDeclaration(str(target), delete_on_failure=True), success=False)
    assert target.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize("data", ["", "not a mount table", "1 0 0:1 / / rw\n"])
def test_malformed_mount_table_does_not_authorize_recursion(tmp_path, monkeypatch, data):
    root = tmp_path / "output"
    target = root / "directory"
    target.mkdir(parents=True)
    (target / "precious").write_text("keep", encoding="utf-8")
    real_read = Path.read_text
    def mount_table(path, *args, **kwargs):
        return data if path == Path("/proc/self/mountinfo") else real_read(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", mount_table)
    with pytest.raises(NotImplementedError):
        ArtifactPathPolicy(output_roots=root).cleanup(
            ArtifactDeclaration(str(target), delete_on_failure=True), success=False)
    assert (target / "precious").read_text(encoding="utf-8") == "keep"
