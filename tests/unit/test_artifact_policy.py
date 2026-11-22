"""统一路径策略 PathPolicy、平台适配器与不跟随链接的安全删除原语单元测试。"""
from __future__ import annotations

import os
import stat
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

import pytest

from tasklite.utils.artifact import (
    EntryInspection,
    FilesystemAdapter,
    PathPolicy,
    TargetType,
    _classify_windows_stat,
    derive_protected_paths,
    evaluate_authorization,
)


# ── 跨平台共享测试（真实文件系统，POSIX 与 Windows 均执行）───────────────


def test_delete_respects_authorization_callback_and_removes_when_approved(tmp_path):
    root = tmp_path / "output"
    root.mkdir()
    target = root / "result"
    target.write_text("existing", encoding="utf-8")
    policy = PathPolicy(base_dir=root)
    with pytest.raises(PermissionError, match="authorization scope changed"):
        policy.delete(str(target), authorize=lambda _path, _kind: False)
    assert target.read_text(encoding="utf-8") == "existing"
    assert policy.delete(str(target), authorize=lambda _path, kind: kind == "file") is True
    assert not target.exists()


def _try_symlink(link: Path, target: Path, *, target_is_directory: bool = False) -> None:
    try:
        link.symlink_to(target, target_is_directory=target_is_directory)
    except OSError as exc:
        if sys.platform == "win32" and getattr(exc, "winerror", None) == 1314:
            pytest.skip("当前 Windows 账户未启用符号链接权限")
        raise


def test_pre_delete_removes_broken_symlink_without_following_target(tmp_path):
    root = tmp_path / "output"
    root.mkdir()
    link = root / "broken"
    _try_symlink(link, tmp_path / "missing-target")
    policy = PathPolicy(base_dir=root)

    with pytest.raises(ValueError, match="symlink"):
        policy.delete(str(link), allow_broken_leaf_symlink=False)
    assert os.path.lexists(link)

    assert policy.delete(str(link), allow_broken_leaf_symlink=True) is True
    assert not os.path.lexists(link)


def test_pre_delete_rejects_live_symlink_without_touching_target(tmp_path):
    root = tmp_path / "output"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("precious", encoding="utf-8")
    link = root / "live"
    _try_symlink(link, outside)
    policy = PathPolicy(base_dir=root)

    with pytest.raises(ValueError, match="symlink"):
        policy.delete(str(link), allow_broken_leaf_symlink=True)

    assert link.is_symlink()
    assert outside.read_text(encoding="utf-8") == "precious"


def test_pre_delete_missing_target_is_idempotent(tmp_path):
    root = tmp_path / "output"
    root.mkdir()
    target = root / "missing"
    policy = PathPolicy(base_dir=root)

    assert policy.delete(str(target), allow_broken_leaf_symlink=True) is False
    assert not target.exists()


@pytest.mark.parametrize("is_directory", [False, True])
def test_delete_removes_file_and_directory_idempotently(tmp_path, is_directory):
    root = tmp_path / "output"
    root.mkdir()
    target = root / "result"
    if is_directory:
        target.mkdir()
        (target / "child").write_text("data", encoding="utf-8")
    else:
        target.write_text("data", encoding="utf-8")
    policy = PathPolicy(base_dir=root)
    assert policy.delete(str(target)) is True
    assert not target.exists()
    assert policy.delete(str(target)) is False


@pytest.mark.parametrize("link_target_exists", [False, True])
def test_leaf_symlinks_are_rejected_without_touching_target(tmp_path, link_target_exists):
    root = tmp_path / "output"
    root.mkdir()
    outside = tmp_path / "outside"
    if link_target_exists:
        outside.write_text("precious", encoding="utf-8")
    link = root / "link"
    _try_symlink(link, outside)
    policy = PathPolicy(base_dir=root)
    with pytest.raises(ValueError, match="symlink"):
        policy.inspect(link, mode="strict")
    assert link.is_symlink()
    if link_target_exists:
        assert outside.read_text(encoding="utf-8") == "precious"


def test_safe_parent_link_returns_canonical_path_but_escape_is_rejected(tmp_path, monkeypatch):
    root = tmp_path / "output"
    root.mkdir()
    real = root / "real"
    real.mkdir()
    _try_symlink(root / "inside", real, target_is_directory=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    _try_symlink(root / "escape", outside, target_is_directory=True)
    monkeypatch.chdir(root)
    policy = PathPolicy(protected_roots=[outside])
    assert policy.inspect("inside/result", mode="strict").canonical_path == str(real / "result")
    with pytest.raises(ValueError, match="parent path changed"):
        policy.inspect("inside/result", mode="strict", verify_unchanged=True)
    with pytest.raises(ValueError):
        policy.inspect("escape/result", mode="strict")


def test_roots_and_engine_regions_are_protected(tmp_path):
    root = tmp_path / "output"
    state = tmp_path / "state"
    root.mkdir()
    state.mkdir()
    policy = PathPolicy(protected_roots=[state])
    for protected in [Path(root.anchor), tmp_path, state, state / "ipc" / "file"]:
        with pytest.raises(ValueError):
            policy.inspect(protected, mode="strict")
    assert policy.inspect(root / "dedicated", mode="strict").canonical_path == str(root / "dedicated")


def test_directory_cleanup_unlinks_nested_link_without_following_it(tmp_path):
    root = tmp_path / "output"
    target = root / "directory"
    target.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "precious").write_text("keep", encoding="utf-8")
    _try_symlink(target / "link", outside, target_is_directory=True)
    policy = PathPolicy(base_dir=root)
    assert policy.delete(str(target)) is True
    assert not target.exists()
    assert (outside / "precious").read_text(encoding="utf-8") == "keep"


def test_normal_leaf_replace_before_cleanup_is_allowed(tmp_path):
    root = tmp_path / "output"
    root.mkdir()
    target = root / "result"
    target.write_text("first", encoding="utf-8")
    temporary = root / "temporary"
    temporary.write_text("replacement", encoding="utf-8")
    os.replace(temporary, target)
    assert PathPolicy(base_dir=root).delete(str(target)) is True
    assert not target.exists()


# ── PathPolicy 纯内存规则与受保护 5 路径派生测试 ──────────────────────────


def test_derive_protected_paths_covers_all_five_state_files(tmp_path):
    state_db = tmp_path / "engine" / "state.db"
    derived = derive_protected_paths(state_db)
    assert derived == (
        state_db,
        Path(f"{state_db}-wal"),
        Path(f"{state_db}-shm"),
        Path(f"{state_db}-journal"),
        Path(f"{state_db}.lock"),
    )

    policy = PathPolicy(base_dir=tmp_path, state_path=state_db)
    for forbidden in (
        state_db,
        Path(f"{state_db}-wal"),
        Path(f"{state_db}-shm"),
        Path(f"{state_db}-journal"),
        Path(f"{state_db}.lock"),
        state_db.parent,
        tmp_path,
        Path(tmp_path.anchor),
    ):
        with pytest.raises(ValueError):
            policy.inspect(forbidden, mode="strict")
        if forbidden != Path(tmp_path.anchor):
            assert policy.is_protected(forbidden) is True

    # 同级前缀相似文件不得误判为受保护重叠（禁止 str.startswith 误伤）
    for allowed in (
        tmp_path / "engine" / "state.db.bak",
        tmp_path / "engine" / "state.db-wal2",
        tmp_path / "engine_other" / "state.db",
    ):
        inspected = policy.inspect(allowed, mode="strict")
        assert inspected.kind == "missing"
        assert policy.is_protected(allowed) is False


def test_evaluate_authorization_matrix_posix_and_windows():
    # Windows 大小写不敏感 + PurePath.parts 段元组语义
    existing_win: list[tuple[str, TargetType]] = [
        ("C:\\Work\\Data", "directory"),
        ("C:\\Work\\Single.txt", "file"),
    ]
    # 大小写等价重复拒绝 & exact_path 命中原主键
    dup_file = evaluate_authorization(existing_win, "c:\\work\\single.txt", "file", is_windows=True)
    assert dup_file.covered is True
    assert dup_file.exact_path == "C:\\Work\\Single.txt"
    assert dup_file.grant_conflict is not None

    dup_diff_type = evaluate_authorization(existing_win, "c:\\work\\single.txt", "directory", is_windows=True)
    assert dup_diff_type.covered is False
    assert dup_diff_type.exact_path == "C:\\Work\\Single.txt"
    assert dup_diff_type.grant_conflict is not None

    # 目录覆盖同路径 file（用于重试时目标缺失/改回文件）与后代，但不同级前缀不误覆盖
    same_dir_as_file = evaluate_authorization(existing_win, "c:\\work\\data", "file", is_windows=True)
    assert same_dir_as_file.covered is True
    assert same_dir_as_file.grant_conflict is not None

    nested_dir = evaluate_authorization(existing_win, "c:\\work\\data\\sub", "directory", is_windows=True)
    assert nested_dir.covered is True
    assert nested_dir.grant_conflict is None

    nested_file = evaluate_authorization(existing_win, "c:\\work\\data\\sub\\a.bin", "file", is_windows=True)
    assert nested_file.covered is True
    assert nested_file.grant_conflict is None

    sibling = evaluate_authorization(existing_win, "c:\\work\\data_other\\a.bin", "file", is_windows=True)
    assert sibling.covered is False
    assert sibling.grant_conflict is None

    # file 祖先冲突 & 候选 file 与已有后代冲突
    under_file = evaluate_authorization(existing_win, "c:\\work\\single.txt\\child", "file", is_windows=True)
    assert under_file.covered is False
    assert under_file.grant_conflict is not None

    parent_as_file = evaluate_authorization(existing_win, "c:\\work", "file", is_windows=True)
    assert parent_as_file.covered is False
    assert parent_as_file.grant_conflict is not None

    parent_as_dir = evaluate_authorization(existing_win, "c:\\work", "directory", is_windows=True)
    assert parent_as_dir.covered is False
    assert parent_as_dir.grant_conflict is None

    # POSIX 大小写敏感
    existing_posix: list[tuple[str, TargetType]] = [("/work/Data", "directory")]
    assert evaluate_authorization(existing_posix, "/work/data/a", "file", is_windows=False).covered is False
    assert evaluate_authorization(existing_posix, "/work/Data/a", "file", is_windows=False).covered is True


# ── FakeFilesystemAdapter 确定性测试（TOCTOU / 大小写 / 词法 / 重解析点）──


class FakeFilesystemAdapter(FilesystemAdapter):
    """纯内存可控适配器，用于跨平台确定性验证词法、TOCTOU、大小写与重解析点策略。"""

    def __init__(self, *, is_windows: bool = True) -> None:
        self.is_windows = is_windows
        self.entries: dict[str, EntryInspection] = {}
        self.realpaths: dict[str, str] = {}
        self.leaf_cases: dict[str, str] = {}
        self.deleted: list[str] = []
        self.on_delete: Callable[[str], None] | None = None

    def _norm(self, path: str) -> str:
        return path.lower() if self.is_windows else path

    def path_lexists(self, path: str) -> bool:
        key = self._norm(path)
        return key in self.entries and self.entries[key].entry_class != "missing"

    def realpath(self, path: str) -> str:
        return self.realpaths.get(self._norm(path), path)

    def inspect_entry(self, path: str) -> EntryInspection:
        return self.entries.get(self._norm(path), EntryInspection("missing"))

    def recover_leaf_case(self, canonical_parent: str, leaf_name: str) -> str:
        sep = "\\" if self.is_windows else "/"
        candidate = f"{canonical_parent.rstrip(sep)}{sep}{leaf_name}"
        return self.leaf_cases.get(self._norm(candidate), candidate)

    def delete_target(
        self,
        target: str,
        *,
        allow_broken_leaf_symlink: bool,
        revalidate: Callable[[], None],
        authorize: Callable[[str, TargetType], bool] | None,
    ) -> bool:
        if self.on_delete is not None:
            self.on_delete(target)
        revalidate()
        entry = self.inspect_entry(target)
        if entry.entry_class == "missing":
            return False
        if entry.entry_class == "live_symlink" or (
            entry.entry_class == "broken_symlink" and not allow_broken_leaf_symlink
        ):
            raise ValueError(f"artifact leaf must not be a symlink: {target}")
        if entry.entry_class in ("mount_point", "unsafe_reparse"):
            raise ValueError(f"artifact contains an unsafe reparse point: {target}")
        actual_type: TargetType = "directory" if entry.entry_class == "directory" else "file"
        if authorize is not None and not authorize(target, actual_type):
            raise PermissionError(f"artifact authorization scope changed before deletion: {target}")
        self.deleted.append(target)
        self.entries[self._norm(target)] = EntryInspection("missing")
        return True


def test_fake_adapter_windows_lexical_rejections_and_case_stability():
    adapter = FakeFilesystemAdapter(is_windows=True)
    adapter.entries["c:\\work"] = EntryInspection("directory")
    adapter.entries["c:\\work\\output.txt"] = EntryInspection("file")
    adapter.leaf_cases["c:\\work\\output.txt"] = "C:\\work\\Output.TXT"
    policy = PathPolicy(base_dir="c:\\work", adapter=adapter)

    for bad in ("", "a\x00b", "C:rel\\file.txt", "C:", "\\root_no_drive", "\\\\.\\PhysicalDrive0", "\\??\\C:\\x", "\\\\?\\Volume{abc}\\x"):
        with pytest.raises(ValueError):
            policy.inspect(bad, mode="strict")
        with pytest.raises(ValueError):
            policy.inspect(bad, mode="lookup")

    # 盘符小写与叶大小写差异归一化为真实叶大小写，且 comparison_key 一致不误报漂移
    inspected = policy.inspect("c:\\work\\output.txt", mode="strict")
    assert inspected.canonical_path == "C:\\work\\Output.TXT"
    assert inspected.comparison_key == "c:\\work\\output.txt"
    assert policy.delete("c:\\work\\output.txt") is True
    assert adapter.deleted == ["C:\\work\\Output.TXT"]


def test_fake_adapter_toctou_parent_drift_and_scope_expansion_blocked():
    adapter = FakeFilesystemAdapter(is_windows=True)
    adapter.entries["c:\\work"] = EntryInspection("directory")
    adapter.entries["c:\\work\\item"] = EntryInspection("file")
    policy = PathPolicy(base_dir="c:\\work", adapter=adapter)

    # 删除瞬间父目录漂移到其他真实路径 -> 必须抛 ValueError 且不删除
    def drift_parent(_target: str) -> None:
        adapter.realpaths["c:\\work"] = "C:\\other"

    adapter.on_delete = drift_parent
    with pytest.raises(ValueError, match="parent path changed"):
        policy.delete("C:\\work\\item")
    assert adapter.deleted == []

    # 恢复父目录后，文件在删除瞬间变为目录且 authorize 仅授权 file -> 必须抛 PermissionError
    adapter.realpaths.clear()

    def swap_to_directory(_target: str) -> None:
        adapter.entries["c:\\work\\item"] = EntryInspection("directory")

    adapter.on_delete = swap_to_directory
    with pytest.raises(PermissionError, match="authorization scope changed"):
        policy.delete("C:\\work\\item", authorize=lambda path, kind: kind == "file")
    assert adapter.deleted == []


def test_fake_adapter_reparse_point_classification_rules():
    # 直接测试 Windows 重解析点标签白名单与 fail-closed 拦截
    reg_mode = stat.S_IFREG | 0o644
    dir_mode = stat.S_IFDIR | 0o755

    # OneDrive Cloud / Dedup / WOF 常规文件放行
    for safe_tag in (0x9000001A, 0x9000101A, 0x80000013, 0x80000017):
        st_file = SimpleNamespace(st_mode=reg_mode, st_file_attributes=0x400, st_reparse_tag=safe_tag)
        assert _classify_windows_stat(st_file, "C:\\cloud.bin").entry_class == "file"  # type: ignore[arg-type]
        st_dir = SimpleNamespace(st_mode=dir_mode, st_file_attributes=0x410, st_reparse_tag=safe_tag)
        assert _classify_windows_stat(st_dir, "C:\\cloud_dir").entry_class == "unsafe_reparse"  # type: ignore[arg-type]

    # Junction / Mount point (0xa0000003) 永不放行
    st_junc = SimpleNamespace(st_mode=dir_mode, st_file_attributes=0x410, st_reparse_tag=0xa0000003)
    assert _classify_windows_stat(st_junc, "C:\\junc").entry_class == "mount_point"  # type: ignore[arg-type]

    # 未知名称代理或未知重解析标签一律 fail-closed 拒绝
    for unsafe_tag in (0xa000ffff, 0x8000ffff, 0x20000001):
        st_bad = SimpleNamespace(st_mode=reg_mode, st_file_attributes=0x400, st_reparse_tag=unsafe_tag)
        assert _classify_windows_stat(st_bad, "C:\\bad").entry_class == "unsafe_reparse"  # type: ignore[arg-type]


def test_is_protected_detects_leaf_symlink_and_junction_targeting_protected_region():
    adapter = FakeFilesystemAdapter(is_windows=True)
    adapter.entries["c:\\work"] = EntryInspection("directory")
    adapter.entries["c:\\state"] = EntryInspection("directory")
    adapter.entries["c:\\work\\link_to_db"] = EntryInspection("live_symlink")
    adapter.realpaths["c:\\work\\link_to_db"] = "C:\\state\\engine.db"
    adapter.entries["c:\\work\\junc_to_state"] = EntryInspection("mount_point")
    adapter.realpaths["c:\\work\\junc_to_state"] = "C:\\state"
    adapter.entries["c:\\work\\safe_link"] = EntryInspection("live_symlink")
    adapter.realpaths["c:\\work\\safe_link"] = "C:\\outside\\file.txt"

    policy = PathPolicy(base_dir="c:\\work", state_path="C:\\state\\engine.db", adapter=adapter)
    assert policy.is_protected("C:\\work\\link_to_db") is True
    assert policy.is_protected("C:\\work\\junc_to_state") is True
    assert policy.is_protected("C:\\work\\safe_link") is False


# ── Windows 真实 OS 行为测试（免管理员权限可执行）──────────────────────────


@pytest.mark.skipif(sys.platform != "win32", reason="仅在 Windows 验证 Win32 CreateFileW 与 NTFS Junction")
class TestWindowsFilesystemAdapter:
    def test_createfilew_pins_parent_directory_against_rename(self, tmp_path, monkeypatch):
        """清理期间父目录被省略 FILE_SHARE_DELETE 钉锁，OS 内核级阻止 rename/移走。"""
        root = tmp_path / "pinned_parent"
        target = root / "directory"
        target.mkdir(parents=True)
        (target / "child.txt").write_text("data", encoding="utf-8")

        real_listdir = os.listdir
        rename_blocked = False

        def attempt_move_parent(path):
            nonlocal rename_blocked
            if Path(path) == target:
                with pytest.raises(PermissionError):
                    root.rename(tmp_path / "moved_parent")
                rename_blocked = True
            return real_listdir(path)

        monkeypatch.setattr(os, "listdir", attempt_move_parent)
        policy = PathPolicy(base_dir=tmp_path)
        assert policy.delete(str(target)) is True
        assert rename_blocked is True
        assert root.exists()
        assert not target.exists()

    def test_mklink_junction_rejected_top_level_and_nested_without_touching_target(self, tmp_path):
        """免特权 NTFS Junction 在顶层（strict/pre_delete）和目录递归内均 fail-closed 拒绝。"""
        outside = tmp_path / "outside_precious"
        outside.mkdir()
        precious = outside / "keep.txt"
        precious.write_text("do-not-delete", encoding="utf-8")

        top_junction = tmp_path / "top_junction"
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(top_junction), str(outside)],
            check=True,
            capture_output=True,
        )
        policy = PathPolicy(base_dir=tmp_path)
        try:
            with pytest.raises(ValueError, match="junction|reparse"):
                policy.inspect(top_junction, mode="strict")
            with pytest.raises(ValueError, match="junction|reparse"):
                policy.inspect(top_junction, mode="pre_delete")
            with pytest.raises(ValueError, match="junction|reparse"):
                policy.delete(str(top_junction), allow_broken_leaf_symlink=True)
            assert precious.read_text(encoding="utf-8") == "do-not-delete"
        finally:
            if os.path.lexists(top_junction):
                os.rmdir(top_junction)

        # 目录递归内含 Junction 时也必须 fail-closed 拒绝且绝不删除 Junction 目标内容
        container = tmp_path / "container"
        container.mkdir()
        nested_junction = container / "inner_junc"
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(nested_junction), str(outside)],
            check=True,
            capture_output=True,
        )
        try:
            with pytest.raises(ValueError, match="junction|reparse"):
                policy.delete(str(container))
            assert precious.read_text(encoding="utf-8") == "do-not-delete"
        finally:
            if os.path.lexists(nested_junction):
                os.rmdir(nested_junction)

    def test_open_file_sharing_violation_and_readonly_propagate_errors(self, tmp_path):
        """文件被占用（WinError 32）与只读位（WinError 5）不吞错、不自动改属性。"""
        policy = PathPolicy(base_dir=tmp_path)

        locked_file = tmp_path / "in_use.txt"
        locked_file.write_text("busy", encoding="utf-8")
        with locked_file.open("r+b"):
            with pytest.raises(PermissionError):
                policy.delete(str(locked_file))
        assert locked_file.read_text(encoding="utf-8") == "busy"

        readonly_file = tmp_path / "readonly.txt"
        readonly_file.write_text("immutable", encoding="utf-8")
        os.chmod(readonly_file, stat.S_IREAD)
        try:
            with pytest.raises(PermissionError):
                policy.delete(str(readonly_file))
            assert readonly_file.exists()
            assert not (os.stat(readonly_file).st_mode & stat.S_IWRITE)
        finally:
            os.chmod(readonly_file, stat.S_IWRITE)

    def test_windows_recovers_true_leaf_case_and_case_insensitive_key(self, tmp_path):
        real_file = tmp_path / "MixedCase.Txt"
        real_file.write_text("hello", encoding="utf-8")
        policy = PathPolicy(base_dir=tmp_path)
        inspected = policy.inspect(tmp_path / "mixedcase.txt", mode="strict")
        assert inspected.canonical_path == str(real_file)
        assert inspected.comparison_key == str(real_file).lower()

    def test_windows_is_protected_short_circuits_junction_to_state_dir(self, tmp_path):
        from tasklite.backend.memory import InMemoryStateBackend
        from tasklite.engine.store import StateStore
        from tasklite.testing import authorizer

        state_dir = tmp_path / "state_dir"
        state_dir.mkdir()
        state_db = state_dir / "engine.db"
        work_dir = tmp_path / "work"
        work_dir.mkdir()
        junc = work_dir / "alias_state"
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(junc), str(state_dir)],
            check=True,
            capture_output=True,
        )
        try:
            policy = PathPolicy(base_dir=work_dir, state_path=state_db)
            assert policy.is_protected(junc) is True

            from tasklite.engine.authorization import AuthorizationCoordinator

            store = StateStore(backend=InMemoryStateBackend(), base_dir=work_dir, path_policy=policy)
            coord = AuthorizationCoordinator(store=store)
            calls: list[tuple[str, str]] = []
            with authorizer(coord, lambda p, t: calls.append((p, t)) or True):
                assert coord.check_or_request_authorization(junc) is False
            assert calls == []
            assert store.list_authorizations() == []
        finally:
            if os.path.lexists(junc):
                os.rmdir(junc)


# ── POSIX / Linux 专属安全边界测试（在 Linux CI 完整执行）─────────────────


@pytest.mark.skipif(os.name != "posix", reason="仅在 POSIX 验证 dir_fd 期间父目录被移走")
def test_disappearing_parent_during_directory_walk_is_not_reported_as_success(tmp_path, monkeypatch):
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
    policy = PathPolicy(base_dir=root)
    with pytest.raises((OSError, ValueError)):
        policy.delete(str(target))
    assert (tmp_path / "moved" / "directory" / "child").read_text(encoding="utf-8") == "keep"


@pytest.mark.skipif(sys.platform != "linux", reason="仅在 Linux 验证 /proc/self/mountinfo bind mount")
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
    policy = PathPolicy(base_dir=root)
    with pytest.raises(ValueError, match="mount"):
        policy.delete(str(target))
    assert (mounted / "precious").read_text(encoding="utf-8") == "keep"


@pytest.mark.skipif(sys.platform != "linux", reason="仅在 Linux 验证 /proc/self/mountinfo 不可读")
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
        PathPolicy(base_dir=root).delete(str(target))
    assert (target / "precious").read_text(encoding="utf-8") == "keep"


def test_inaccessible_output_is_reported_instead_of_treated_as_missing(tmp_path):
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
            PathPolicy(base_dir=root).delete(str(target))
    finally:
        private.chmod(0o700)
    assert target.read_text(encoding="utf-8") == "keep"


@pytest.mark.skipif(os.name != "posix", reason="仅在 POSIX 验证 dir_fd open 瞬间父目录替换")
@pytest.mark.parametrize("use_link", [False, True])
def test_parent_replaced_during_open_is_rejected_without_deleting_replacement(
    tmp_path, monkeypatch, use_link
):
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
        PathPolicy(base_dir=root).delete(str(target))
    precious = replacement / "result" if use_link else parent / "result"
    assert precious.read_text(encoding="utf-8") == "precious"
    assert (root / "old_parent" / "result").read_text(encoding="utf-8") == "original"


@pytest.mark.skipif(os.name != "posix", reason="仅在 POSIX 验证缺失 O_NOFOLLOW 拒绝删除")
def test_missing_non_following_capability_refuses_file_deletion(tmp_path, monkeypatch):
    root = tmp_path / "output"
    root.mkdir()
    target = root / "result"
    target.write_text("keep", encoding="utf-8")
    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    with pytest.raises(NotImplementedError):
        PathPolicy(base_dir=root).delete(str(target))
    assert target.read_text(encoding="utf-8") == "keep"


@pytest.mark.skipif(sys.platform != "linux", reason="仅在 Linux 验证异常 /proc/self/mountinfo 格式")
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
        PathPolicy(base_dir=root).delete(str(target))
    assert (target / "precious").read_text(encoding="utf-8") == "keep"
