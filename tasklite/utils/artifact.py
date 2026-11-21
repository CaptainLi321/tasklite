"""跨平台统一路径策略（PathPolicy）与不跟随链接的安全清理原语。

本模块是全引擎路径词法校验、前缀规范化、受保护路径派生与双向重叠拦截、
授权层级冲突求值以及平台安全文件/目录清理的唯一真相源。

平台安全边界：
- POSIX 适配器：依赖 ``dir_fd`` / ``O_NOFOLLOW`` / ``O_DIRECTORY``、
  ``(st_dev, st_ino)`` 身份校验及 Linux ``/proc/self/mountinfo`` 挂载边界。
- Windows 适配器：仅调用 3 个无特权 ``kernel32`` 函数（``CreateFileW`` 省略
  ``FILE_SHARE_DELETE`` 内核级钉锁父目录及递归子目录、
  ``GetFileInformationByHandle`` 复核卷与文件索引、``CloseHandle``），结合
  ``os.lstat`` 原生 ``st_file_attributes`` / ``st_reparse_tag`` 区分 symlink、
  fail-closed 拒绝 NTFS Junction/卷挂载点与未知重解析点；不隐式移除只读位、
  不吞并文件占用错误（WinError 32 / WinError 5 原样传播）。
"""
from __future__ import annotations

import ctypes
import ntpath
import os
import posixpath
import re
import stat
import sys
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from typing import Any, Literal

from .lockfile import lock_path_for

PathKind = Literal["missing", "file", "directory", "broken_symlink"]
TargetType = Literal["file", "directory"]
InspectMode = Literal["strict", "pre_delete", "lookup"]
EntryClass = Literal[
    "missing",
    "file",
    "directory",
    "broken_symlink",
    "live_symlink",
    "mount_point",
    "unsafe_reparse",
    "unsupported",
]

_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
)

# Windows 文件属性与重解析点常量
_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_IO_REPARSE_TAG_MOUNT_POINT = 0xa0000003
_IO_REPARSE_TAG_SYMLINK = 0xa000000c
_IO_REPARSE_TAG_NAME_SURROGATE_MASK = 0x20000000
_IO_REPARSE_TAG_DEDUP = 0x80000013
_IO_REPARSE_TAG_WOF = 0x80000017
_IO_REPARSE_TAG_CLOUD_MASK = 0xFFFF0000
_IO_REPARSE_TAG_CLOUD_PREFIX = 0x90000000

_FILE_READ_ATTRIBUTES = 0x00000080
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_OPEN_EXISTING = 3
_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value


@dataclass(frozen=True)
class InspectedPath:
    """路径策略检查结果值对象。"""

    canonical_path: str
    comparison_key: str
    kind: PathKind

    @property
    def exists(self) -> bool:
        return self.kind != "missing"

    @property
    def target_type(self) -> TargetType:
        return "directory" if self.kind == "directory" else "file"


@dataclass(frozen=True)
class AuthorizationMatch:
    """纯内存授权匹配与冲突求值结果。"""

    covered: bool
    grant_conflict: str | None
    exact_path: str | None


@dataclass(frozen=True)
class EntryInspection:
    """适配器对单个目录项（绝不跟随叶链接）的底层探测结果。"""

    entry_class: EntryClass
    is_dir_symlink: bool = False


# ── 跨平台纯词法与路径段元组工具 ──────────────────────────────────────────


def _is_safe_windows_data_reparse_tag(tag: int) -> bool:
    """仅放行非名称代理的已知数据重解析点（OneDrive Cloud / Dedup / WOF）。"""
    if tag & _IO_REPARSE_TAG_NAME_SURROGATE_MASK:
        return False
    if tag in (_IO_REPARSE_TAG_DEDUP, _IO_REPARSE_TAG_WOF):
        return True
    if (tag & _IO_REPARSE_TAG_CLOUD_MASK) == _IO_REPARSE_TAG_CLOUD_PREFIX:
        return True
    return False


def _strip_windows_extended_prefix(path_str: str) -> str:
    """剥离 Win32 扩展长度前缀 ``\\\\?\\`` 并规范盘符为大写。"""
    result = path_str
    if result.startswith("\\\\?\\"):
        body = result[4:]
        if body.upper().startswith("UNC\\"):
            result = "\\\\" + body[4:]
        else:
            result = body
    if len(result) >= 2 and result[1] == ":" and result[0].isalpha():
        result = result[0].upper() + result[1:]
    return result


def is_platform_abs_path(
    norm_path: str,
    *,
    is_windows: bool | None = None,
) -> bool:
    """判断词法规范化后的路径是否为目标平台绝对路径（Windows 需盘符根或 UNC）。"""
    win = (os.name == "nt") if is_windows is None else is_windows
    if win:
        drive, tail = ntpath.splitdrive(norm_path)
        return bool(drive) and tail.startswith("\\")
    return norm_path.startswith("/")


def validate_and_normalize_lexical(
    path: str | Path,
    *,
    is_windows: bool | None = None,
) -> str:
    """校验路径词法边界（拒空串/空字节/Windows 盘符相对/无盘符根相对/设备路径）。"""
    win = (os.name == "nt") if is_windows is None else is_windows
    if not isinstance(path, (str, os.PathLike)):
        raise ValueError("artifact path must be a non-empty path without null bytes")
    raw = os.fspath(path)
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        raise ValueError("artifact path must be a non-empty path without null bytes")

    if not win:
        norm = posixpath.normpath(raw)
        if norm.startswith("//"):
            norm = "/" + norm.lstrip("/")
        return norm

    slash_norm = raw.replace("/", "\\")
    lower = slash_norm.lower()
    if lower.startswith("\\\\.\\") or lower.startswith("\\??\\"):
        raise ValueError(f"device path is not allowed: {raw!r}")
    if lower.startswith("\\\\?\\"):
        body = slash_norm[4:]
        if body.lower().startswith("unc\\"):
            slash_norm = "\\\\" + body[4:]
        elif len(body) >= 3 and body[0].isalpha() and body[1] == ":" and body[2] == "\\":
            slash_norm = body
        else:
            raise ValueError(f"device path is not allowed: {raw!r}")

    drive, tail = ntpath.splitdrive(slash_norm)
    if drive:
        if not drive.startswith("\\\\") and not tail.startswith("\\"):
            raise ValueError(f"drive-relative path is not allowed: {raw!r}")
    elif slash_norm.startswith("\\"):
        raise ValueError(f"root-relative path without drive is not allowed: {raw!r}")

    norm = ntpath.normpath(slash_norm)
    return _strip_windows_extended_prefix(norm)


def resolve_lexical_path(
    path: str | Path,
    base_dir: str | Path,
    *,
    is_windows: bool | None = None,
) -> str:
    """按固定基准目录将路径解析为词法规范绝对路径（不触碰磁盘）。"""
    win = (os.name == "nt") if is_windows is None else is_windows
    norm = validate_and_normalize_lexical(path, is_windows=win)
    base_norm = validate_and_normalize_lexical(base_dir, is_windows=win)
    if win:
        path_drive, path_tail = ntpath.splitdrive(norm)
        if path_drive.startswith("\\\\") and not path_tail:
            return f"{path_drive}\\"
        if not is_platform_abs_path(base_norm, is_windows=True):
            base_norm = _strip_windows_extended_prefix(
                ntpath.normpath(os.path.abspath(base_norm).replace("/", "\\"))
            )
        if is_platform_abs_path(norm, is_windows=True):
            return norm
        return _join_platform_path(base_norm, (norm,), is_windows=True)

    if not is_platform_abs_path(base_norm, is_windows=False):
        base_norm = posixpath.normpath(os.path.abspath(base_norm))
        if base_norm.startswith("//"):
            base_norm = "/" + base_norm.lstrip("/")
    if is_platform_abs_path(norm, is_windows=False):
        return norm
    return _join_platform_path(base_norm, (norm,), is_windows=False)


def _comparison_key_for_path(path_str: str, *, is_windows: bool) -> str:
    """生成跨调用一致的平台比较键（Windows 统一小写与反斜杠，POSIX 保原样）。"""
    if is_windows:
        normalized = _strip_windows_extended_prefix(ntpath.normpath(path_str.replace("/", "\\")))
        return normalized.lower()
    normalized = posixpath.normpath(path_str)
    if normalized.startswith("//"):
        normalized = "/" + normalized.lstrip("/")
    return normalized


def _path_parts(comparison_key: str, *, is_windows: bool) -> tuple[str, ...]:
    """按平台 PurePath 拆分路径段元组，禁止字符串前缀误匹配。"""
    if is_windows:
        return PureWindowsPath(comparison_key).parts
    return PurePosixPath(comparison_key).parts


def _parts_overlap(first: tuple[str, ...], second: tuple[str, ...]) -> bool:
    """双向段元组前缀重叠判定（相等、父子、子父均返回 True）。"""
    if not first or not second:
        return False
    shorter = min(len(first), len(second))
    return first[:shorter] == second[:shorter]


def _is_root_path(path_str: str, *, is_windows: bool) -> bool:
    """判断路径是否为文件系统根、Windows 盘符根或 UNC 共享根。"""
    pure: PurePath = PureWindowsPath(path_str) if is_windows else PurePosixPath(path_str)
    return pure.parent == pure


def _join_platform_path(base: str, parts: Sequence[str], *, is_windows: bool) -> str:
    if is_windows:
        return _strip_windows_extended_prefix(ntpath.normpath(ntpath.join(base, *parts)))
    joined = posixpath.normpath(posixpath.join(base, *parts))
    if joined.startswith("//"):
        joined = "/" + joined.lstrip("/")
    return joined


def _split_parent_and_name(path_str: str, *, is_windows: bool) -> tuple[str, str]:
    pure: PurePath = PureWindowsPath(path_str) if is_windows else PurePosixPath(path_str)
    return str(pure.parent), pure.name


def derive_protected_paths(
    state_path: str | Path | None,
    protected_roots: Sequence[str | Path] = (),
) -> tuple[Path, ...]:
    """由状态库路径单点派生全部 5 项受保护内部路径并合并额外受保护根。"""
    derived: list[Path] = []
    if state_path is not None:
        sp = Path(state_path)
        sp_str = str(sp)
        derived.extend(
            (
                sp,
                Path(f"{sp_str}-wal"),
                Path(f"{sp_str}-shm"),
                Path(f"{sp_str}-journal"),
                lock_path_for(sp),
            )
        )
    for root in protected_roots:
        derived.append(Path(root))
    return tuple(derived)


def evaluate_authorization(
    existing: Sequence[tuple[str, TargetType]],
    target: str | InspectedPath,
    target_type: TargetType | None = None,
    *,
    is_windows: bool | None = None,
) -> AuthorizationMatch:
    """纯内存授权匹配与冲突求值（基于平台比较键的 PurePath.parts 元组）。

    规则：
    - ``exact_path``：命中的既有存储主键 ``path``（用于精确撤销及 Windows 大小写原地覆盖）。
    - ``covered``：同键且同类型，或存在任意 ``"directory"`` 祖先。
    - ``grant_conflict``：同键重复（无论类型是否相同）、存在 ``"file"`` 祖先、
      或候选为 ``"file"`` 且存在任意子路径授权；父子目录嵌套及目录下文件授权均允许。
    """
    win = (os.name == "nt") if is_windows is None else is_windows
    if isinstance(target, InspectedPath):
        target_key = target.comparison_key
        effective_type: TargetType | None = (
            target_type if target_type is not None else target.target_type
        )
    else:
        target_key = _comparison_key_for_path(os.fspath(target), is_windows=win)
        effective_type = target_type

    target_parts = _path_parts(target_key, is_windows=win)
    covered = False
    grant_conflict: str | None = None
    exact_path: str | None = None

    for existing_path, existing_type in existing:
        ex_key = _comparison_key_for_path(existing_path, is_windows=win)
        ex_parts = _path_parts(ex_key, is_windows=win)

        if ex_parts == target_parts:
            exact_path = existing_path
            if effective_type is not None and (
                existing_type == effective_type or existing_type == "directory"
            ):
                covered = True
            if grant_conflict is None:
                grant_conflict = (
                    f"Authorization already exists for path {existing_path!r} "
                    f"(target_type={existing_type!r})"
                )
        elif len(ex_parts) < len(target_parts) and target_parts[: len(ex_parts)] == ex_parts:
            if existing_type == "directory":
                covered = True
            elif existing_type == "file" and grant_conflict is None:
                grant_conflict = (
                    f"Cannot authorize path under existing file authorization "
                    f"{existing_path!r}"
                )
        elif len(target_parts) < len(ex_parts) and ex_parts[: len(target_parts)] == target_parts:
            if effective_type == "file" and grant_conflict is None:
                grant_conflict = (
                    f"Cannot authorize path as 'file' because descendant authorization "
                    f"{existing_path!r} already exists"
                )

    return AuthorizationMatch(
        covered=covered,
        grant_conflict=grant_conflict,
        exact_path=exact_path,
    )


# ── 内部平台文件系统 Seam（POSIX / Windows / 测试 Fake Adapter）───────────


class FilesystemAdapter(ABC):
    """路径规范化与安全清理的内部平台 Seam。"""

    is_windows: bool

    @abstractmethod
    def path_lexists(self, path: str) -> bool:
        """非跟随判断路径是否存在（权限错误必须原样传播）。"""

    @abstractmethod
    def realpath(self, path: str) -> str:
        """解析已存在前缀的真实规范路径（展开父级链接/Junction/8.3 短名并剥离前缀）。"""

    @abstractmethod
    def inspect_entry(self, path: str) -> EntryInspection:
        """非跟随探测单个路径的形态分类（缺失返回 missing，权限错误原样传播）。"""

    @abstractmethod
    def recover_leaf_case(self, canonical_parent: str, leaf_name: str) -> str:
        """在叶已存在且非链接/重解析点时回收平台真实大小写。"""

    @abstractmethod
    def delete_target(
        self,
        target: str,
        *,
        allow_broken_leaf_symlink: bool,
        revalidate: Callable[[], None],
        authorize: Callable[[str, TargetType], bool] | None,
    ) -> bool:
        """在父目录锁保护下核验身份、复核授权并执行非跟随单对象或安全递归删除。"""


class PosixFilesystemAdapter(FilesystemAdapter):
    """POSIX / Linux 适配器：完整保留 dir_fd / O_NOFOLLOW / O_DIRECTORY 与 mountinfo。"""

    is_windows = False

    def path_lexists(self, path: str) -> bool:
        try:
            os.lstat(path)
            return True
        except (FileNotFoundError, NotADirectoryError):
            return False

    def realpath(self, path: str) -> str:
        try:
            resolved = os.path.realpath(path)
        except RuntimeError as error:
            raise ValueError(f"cannot resolve artifact path: {path}") from error
        if resolved.startswith("//"):
            resolved = "/" + resolved.lstrip("/")
        return resolved

    def inspect_entry(self, path: str) -> EntryInspection:
        try:
            st = os.lstat(path)
        except (FileNotFoundError, NotADirectoryError):
            return EntryInspection("missing")
        return self._classify_stat(st, path)

    @staticmethod
    def _classify_stat(st: os.stat_result, path: str) -> EntryInspection:
        if stat.S_ISLNK(st.st_mode):
            try:
                os.stat(path)
            except FileNotFoundError:
                return EntryInspection("broken_symlink")
            except OSError as error:
                raise ValueError(f"cannot inspect artifact leaf: {path}") from error
            return EntryInspection("live_symlink")
        if getattr(st, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT:
            return EntryInspection("unsafe_reparse")
        if stat.S_ISDIR(st.st_mode):
            return EntryInspection("directory")
        if stat.S_ISREG(st.st_mode):
            return EntryInspection("file")
        return EntryInspection("unsupported")

    def recover_leaf_case(self, canonical_parent: str, leaf_name: str) -> str:
        return _join_platform_path(canonical_parent, (leaf_name,), is_windows=False)

    @staticmethod
    def _require_relative_operations() -> None:
        if not all(
            operation in os.supports_dir_fd
            for operation in (os.open, os.stat, os.unlink, os.rmdir)
        ):
            raise NotImplementedError("safe directory-relative artifact deletion is unavailable")
        if os.listdir not in os.supports_fd:
            raise NotImplementedError("safe directory enumeration is unavailable")
        if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
            raise NotImplementedError("non-following artifact deletion is unavailable")

    @staticmethod
    def _same_object(first: os.stat_result, second: os.stat_result) -> bool:
        return (first.st_dev, first.st_ino) == (second.st_dev, second.st_ino)

    @staticmethod
    def _mount_points() -> tuple[Path, ...]:
        """Linux 挂载表包含同设备 bind mount；无法确认边界时拒绝递归。"""
        if sys.platform != "linux":
            raise NotImplementedError("safe artifact directory deletion requires Linux mountinfo")
        try:
            lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
            if not lines:
                raise ValueError("empty mount table")
            points = []
            for line in lines:
                if " - " not in line:
                    raise ValueError("invalid mount table separator")
                fields = line.split(" - ", 1)[0].split()
                if len(fields) < 6 or not fields[0].isdigit() or not fields[1].isdigit():
                    raise ValueError("invalid mount table")
                raw = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields[4])
                point = Path(raw)
                if not point.is_absolute():
                    raise ValueError("non-absolute mount target")
                points.append(point)
            return tuple(points)
        except (OSError, UnicodeError, ValueError) as error:
            raise NotImplementedError("cannot establish artifact mount boundaries") from error

    def delete_target(
        self,
        target: str,
        *,
        allow_broken_leaf_symlink: bool,
        revalidate: Callable[[], None],
        authorize: Callable[[str, TargetType], bool] | None,
    ) -> bool:
        self._require_relative_operations()
        target_path = Path(target)
        before = target_path.parent.stat(follow_symlinks=False)
        parent_fd = os.open(target_path.parent, _DIRECTORY_OPEN_FLAGS)
        try:
            if not self._same_object(before, os.fstat(parent_fd)):
                raise ValueError(f"artifact parent identity changed: {target_path.parent}")
            revalidate()
            try:
                entry = os.stat(target_path.name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return False

            target_type: TargetType = "directory" if stat.S_ISDIR(entry.st_mode) else "file"
            if authorize is not None and not authorize(str(target_path), target_type):
                raise PermissionError(
                    f"artifact authorization scope changed before deletion: {target_path}"
                )

            if stat.S_ISDIR(entry.st_mode):
                if any(point.is_relative_to(target_path) for point in self._mount_points()):
                    raise ValueError(f"artifact directory contains a mount: {target_path}")

            def guard() -> None:
                revalidate()
                if not self._same_object(before, target_path.parent.stat(follow_symlinks=False)):
                    raise ValueError(f"artifact parent identity changed: {target_path.parent}")
                current = os.stat(target_path.name, dir_fd=parent_fd, follow_symlinks=False)
                if not self._same_object(entry, current):
                    raise ValueError(f"artifact identity changed during deletion: {target_path}")

            self._remove_at(
                parent_fd,
                target_path,
                guard,
                allow_symlink=allow_broken_leaf_symlink,
            )
            return True
        finally:
            os.close(parent_fd)

    def _remove_at(
        self,
        parent_fd: int,
        target: Path,
        guard: Callable[[], None],
        *,
        allow_symlink: bool,
    ) -> None:
        guard()
        parent_now = target.parent.stat(follow_symlinks=False)
        if not self._same_object(parent_now, os.fstat(parent_fd)):
            raise ValueError(f"artifact directory identity changed: {target.parent}")
        entry = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
        if stat.S_ISLNK(entry.st_mode):
            if not allow_symlink:
                raise ValueError(f"artifact leaf must not be a symlink: {target}")
        elif getattr(entry, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT:
            raise ValueError(f"artifact contains an unsafe reparse point: {target}")
        elif stat.S_ISDIR(entry.st_mode):
            if entry.st_dev != os.fstat(parent_fd).st_dev or os.path.ismount(target):
                raise ValueError(f"artifact deletion must not cross a mount: {target}")
            child_fd = os.open(target.name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_fd)
            try:
                if not self._same_object(entry, os.fstat(child_fd)):
                    raise ValueError(f"artifact directory identity changed: {target}")
                for name in os.listdir(child_fd):
                    self._remove_at(child_fd, target / name, guard, allow_symlink=True)
                guard()
                if not self._same_object(
                    entry, os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
                ):
                    raise ValueError(f"artifact directory identity changed: {target}")
                os.rmdir(target.name, dir_fd=parent_fd)
            finally:
                os.close(child_fd)
            return
        elif not stat.S_ISREG(entry.st_mode):
            raise ValueError(f"unsupported artifact object: {target}")
        guard()
        current = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
        if not self._same_object(entry, current):
            raise ValueError(f"artifact identity changed during deletion: {target}")
        os.unlink(target.name, dir_fd=parent_fd)


# ── Windows 适配器（仅 3 个无特权 kernel32 函数，显式 64 位安全签名）────


if sys.platform == "win32":
    from ctypes import wintypes

    class _FILETIME(ctypes.Structure):
        _fields_ = [
            ("dwLowDateTime", wintypes.DWORD),
            ("dwHighDateTime", wintypes.DWORD),
        ]

    class _BY_HANDLE_FILE_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("dwFileAttributes", wintypes.DWORD),
            ("ftCreationTime", _FILETIME),
            ("ftLastAccessTime", _FILETIME),
            ("ftLastWriteTime", _FILETIME),
            ("dwVolumeSerialNumber", wintypes.DWORD),
            ("nFileSizeHigh", wintypes.DWORD),
            ("nFileSizeLow", wintypes.DWORD),
            ("nNumberOfLinks", wintypes.DWORD),
            ("nFileIndexHigh", wintypes.DWORD),
            ("nFileIndexLow", wintypes.DWORD),
        ]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    _kernel32.CreateFileW.restype = wintypes.HANDLE

    _kernel32.GetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_BY_HANDLE_FILE_INFORMATION),
    ]
    _kernel32.GetFileInformationByHandle.restype = wintypes.BOOL

    _kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    _kernel32.CloseHandle.restype = wintypes.BOOL


def _classify_windows_stat(st: os.stat_result, path: str) -> EntryInspection:
    """按 Windows st_file_attributes 与 st_reparse_tag 分类单个目录项。"""
    attrs = getattr(st, "st_file_attributes", 0)
    reparse_tag = getattr(st, "st_reparse_tag", 0)
    is_dir_attr = bool(attrs & _FILE_ATTRIBUTE_DIRECTORY)

    if attrs & _FILE_ATTRIBUTE_REPARSE_POINT:
        if reparse_tag == _IO_REPARSE_TAG_SYMLINK:
            try:
                os.stat(path)
            except FileNotFoundError:
                return EntryInspection("broken_symlink", is_dir_symlink=is_dir_attr)
            except OSError as error:
                raise ValueError(f"cannot inspect artifact leaf: {path}") from error
            return EntryInspection("live_symlink", is_dir_symlink=is_dir_attr)
        if reparse_tag == _IO_REPARSE_TAG_MOUNT_POINT:
            return EntryInspection("mount_point")
        if (
            _is_safe_windows_data_reparse_tag(reparse_tag)
            and stat.S_ISREG(st.st_mode)
            and not is_dir_attr
        ):
            return EntryInspection("file")
        return EntryInspection("unsafe_reparse")

    if stat.S_ISLNK(st.st_mode):
        try:
            os.stat(path)
        except FileNotFoundError:
            return EntryInspection("broken_symlink", is_dir_symlink=is_dir_attr)
        except OSError as error:
            raise ValueError(f"cannot inspect artifact leaf: {path}") from error
        return EntryInspection("live_symlink", is_dir_symlink=is_dir_attr)

    if stat.S_ISDIR(st.st_mode):
        return EntryInspection("directory")
    if stat.S_ISREG(st.st_mode):
        return EntryInspection("file")
    return EntryInspection("unsupported")


class WindowsFilesystemAdapter(FilesystemAdapter):
    """Windows 适配器：CreateFileW 省略 FILE_SHARE_DELETE 内核钉锁父目录 + lstat 分类。"""

    is_windows = True

    def path_lexists(self, path: str) -> bool:
        try:
            os.lstat(path)
            return True
        except (FileNotFoundError, NotADirectoryError):
            return False

    def realpath(self, path: str) -> str:
        try:
            resolved = os.path.realpath(path)
        except RuntimeError as error:
            raise ValueError(f"cannot resolve artifact path: {path}") from error
        return _strip_windows_extended_prefix(ntpath.normpath(resolved))

    def inspect_entry(self, path: str) -> EntryInspection:
        try:
            st = os.lstat(path)
        except (FileNotFoundError, NotADirectoryError):
            return EntryInspection("missing")
        return _classify_windows_stat(st, path)

    def recover_leaf_case(self, canonical_parent: str, leaf_name: str) -> str:
        candidate = _join_platform_path(canonical_parent, (leaf_name,), is_windows=True)
        try:
            resolved = self.realpath(candidate)
        except (OSError, ValueError):
            return candidate
        res_parent, res_name = _split_parent_and_name(resolved, is_windows=True)
        if _comparison_key_for_path(res_parent, is_windows=True) == _comparison_key_for_path(
            canonical_parent, is_windows=True
        ):
            return _join_platform_path(canonical_parent, (res_name,), is_windows=True)
        return candidate

    @staticmethod
    def _same_lstat(first: os.stat_result, second: os.stat_result) -> bool:
        return (first.st_dev, first.st_ino) == (second.st_dev, second.st_ino)

    @staticmethod
    def _open_pinned_directory(path: str) -> int:
        """以省略 FILE_SHARE_DELETE 的句柄钉锁目录，防止清理期间被重命名或移走。"""
        if sys.platform != "win32":
            raise NotImplementedError("WindowsFilesystemAdapter requires Windows")
        handle = _kernel32.CreateFileW(
            path,
            _FILE_READ_ATTRIBUTES,
            _FILE_SHARE_READ | _FILE_SHARE_WRITE,
            None,
            _OPEN_EXISTING,
            _FILE_FLAG_BACKUP_SEMANTICS | _FILE_FLAG_OPEN_REPARSE_POINT,
            None,
        )
        if handle is None or handle == _INVALID_HANDLE_VALUE:
            err = ctypes.get_last_error()
            raise ctypes.WinError(err)
        return int(handle)

    @staticmethod
    def _close_handle(handle: int) -> None:
        if sys.platform == "win32":
            _kernel32.CloseHandle(handle)

    @staticmethod
    def _get_handle_info(handle: int) -> tuple[int, int, int]:
        """读取句柄的 (dwFileAttributes, dwVolumeSerialNumber, nFileIndex64)。"""
        if sys.platform != "win32":
            raise NotImplementedError("WindowsFilesystemAdapter requires Windows")
        info = _BY_HANDLE_FILE_INFORMATION()
        if not _kernel32.GetFileInformationByHandle(handle, ctypes.byref(info)):
            err = ctypes.get_last_error()
            raise ctypes.WinError(err)
        file_index = (info.nFileIndexHigh << 32) | info.nFileIndexLow
        return (int(info.dwFileAttributes), int(info.dwVolumeSerialNumber), int(file_index))

    @classmethod
    def _verify_pinned_dir(
        cls,
        handle: int,
        expected_lstat: os.stat_result,
        path: str,
    ) -> None:
        """校验钉锁句柄仍指向同一常规非重解析目录。

        注：Python 3.12+ 在 Windows 上经 FileIdInfo 读取 64 位卷序列号，其低 32 位与
        BY_HANDLE_FILE_INFORMATION.dwVolumeSerialNumber 一致。
        """
        attrs, vol_serial, file_index = cls._get_handle_info(handle)
        if attrs & _FILE_ATTRIBUTE_REPARSE_POINT:
            raise ValueError(f"artifact parent contains an unsafe reparse point: {path}")
        if not (attrs & _FILE_ATTRIBUTE_DIRECTORY):
            raise ValueError(f"artifact parent is not a directory: {path}")
        if (
            (expected_lstat.st_dev & 0xFFFFFFFF) != vol_serial
            or (expected_lstat.st_ino & 0xFFFFFFFFFFFFFFFF) != file_index
        ):
            raise ValueError(f"artifact parent identity changed: {path}")

    def delete_target(
        self,
        target: str,
        *,
        allow_broken_leaf_symlink: bool,
        revalidate: Callable[[], None],
        authorize: Callable[[str, TargetType], bool] | None,
    ) -> bool:
        parent, _ = _split_parent_and_name(target, is_windows=True)
        before_parent = os.lstat(parent)
        parent_cls = _classify_windows_stat(before_parent, parent)
        if parent_cls.entry_class != "directory":
            raise ValueError(f"artifact parent is not a regular directory: {parent}")

        parent_handle = self._open_pinned_directory(parent)
        try:
            self._verify_pinned_dir(parent_handle, before_parent, parent)
            revalidate()

            try:
                entry_st = os.lstat(target)
            except FileNotFoundError:
                return False

            entry_info = _classify_windows_stat(entry_st, target)
            if entry_info.entry_class == "live_symlink":
                raise ValueError(f"artifact leaf must not be a symlink: {target}")
            if entry_info.entry_class == "broken_symlink" and not allow_broken_leaf_symlink:
                raise ValueError(f"artifact leaf must not be a symlink: {target}")
            if entry_info.entry_class == "mount_point":
                raise ValueError(
                    f"artifact contains an unsafe reparse point (junction or mount point): {target}"
                )
            if entry_info.entry_class == "unsafe_reparse":
                raise ValueError(f"artifact contains an unsafe reparse point: {target}")
            if entry_info.entry_class == "unsupported":
                raise ValueError(f"unsupported artifact object: {target}")

            actual_type: TargetType = (
                "directory" if entry_info.entry_class == "directory" else "file"
            )
            if authorize is not None and not authorize(target, actual_type):
                raise PermissionError(
                    f"artifact authorization scope changed before deletion: {target}"
                )

            def guard() -> None:
                revalidate()
                self._verify_pinned_dir(parent_handle, before_parent, parent)
                parent_now = os.lstat(parent)
                if not self._same_lstat(before_parent, parent_now):
                    raise ValueError(f"artifact parent identity changed: {parent}")
                current_st = os.lstat(target)
                if not self._same_lstat(entry_st, current_st):
                    raise ValueError(f"artifact identity changed during deletion: {target}")

            if entry_info.entry_class == "directory":
                self._remove_dir_recursive(target, entry_st, guard)
                return True

            guard()
            current_st = os.lstat(target)
            if not self._same_lstat(entry_st, current_st):
                raise ValueError(f"artifact identity changed during deletion: {target}")
            if entry_info.entry_class == "broken_symlink" and entry_info.is_dir_symlink:
                os.rmdir(target)
            else:
                os.unlink(target)
            return True
        finally:
            self._close_handle(parent_handle)

    def _remove_dir_recursive(
        self,
        directory: str,
        expected_st: os.stat_result,
        guard: Callable[[], None],
    ) -> None:
        """钉锁当前子目录并递归清空其内容，随后释放子句柄、复验身份并 rmdir。"""
        guard()
        child_handle = self._open_pinned_directory(directory)
        closed = False
        try:
            self._verify_pinned_dir(child_handle, expected_st, directory)
            for name in os.listdir(directory):
                guard()
                child_path = _join_platform_path(directory, (name,), is_windows=True)
                child_st = os.lstat(child_path)
                child_info = _classify_windows_stat(child_st, child_path)

                if child_info.entry_class == "mount_point":
                    raise ValueError(
                        f"artifact contains an unsafe reparse point (junction or mount point): {child_path}"
                    )
                if child_info.entry_class == "unsafe_reparse":
                    raise ValueError(f"artifact contains an unsafe reparse point: {child_path}")
                if child_info.entry_class == "unsupported":
                    raise ValueError(f"unsupported artifact object: {child_path}")

                if child_info.entry_class in ("live_symlink", "broken_symlink"):
                    # 目录递归内仅删链接本体，绝不跟随进入链接目标。
                    if child_info.is_dir_symlink:
                        os.rmdir(child_path)
                    else:
                        os.unlink(child_path)
                elif child_info.entry_class == "directory":
                    self._remove_dir_recursive(child_path, child_st, guard)
                else:
                    os.unlink(child_path)

            # 仍持有上层父目录钉锁的前提下关闭当前子目录句柄，再复验身份并删除空目录。
            self._close_handle(child_handle)
            closed = True
            guard()
            current_st = os.lstat(directory)
            current_info = _classify_windows_stat(current_st, directory)
            if current_info.entry_class != "directory" or not self._same_lstat(
                expected_st, current_st
            ):
                raise ValueError(f"artifact directory identity changed: {directory}")
            os.rmdir(directory)
        finally:
            if not closed:
                self._close_handle(child_handle)


def _default_adapter() -> FilesystemAdapter:
    if sys.platform == "win32":
        return WindowsFilesystemAdapter()
    return PosixFilesystemAdapter()


# ── 统一路径策略深模块 PathPolicy ────────────────────────────────────────


class PathPolicy:
    """跨平台统一路径规范化、受保护路径拦截、安全清理与授权判定深模块。"""

    def __init__(
        self,
        *,
        base_dir: str | Path | None = None,
        state_path: str | Path | None = None,
        protected_roots: Sequence[str | Path] = (),
        adapter: FilesystemAdapter | None = None,
    ) -> None:
        self._adapter: FilesystemAdapter = adapter if adapter is not None else _default_adapter()
        self._is_windows: bool = self._adapter.is_windows

        if base_dir is not None:
            self._base_dir_fixed = True
            self._base_dir: str | None = self._canonicalize_anchor(base_dir)
        else:
            self._base_dir_fixed = False
            self._base_dir = None

        raw_protected = derive_protected_paths(state_path, protected_roots)
        canonical_protected: list[Path] = []
        protected_parts: list[tuple[str, ...]] = []
        seen_keys: set[str] = set()
        for item in raw_protected:
            inspected = self._canonicalize_path(item)
            if inspected.comparison_key not in seen_keys:
                seen_keys.add(inspected.comparison_key)
                canonical_protected.append(Path(inspected.canonical_path))
                protected_parts.append(_path_parts(inspected.comparison_key, is_windows=self._is_windows))
        self._protected_paths: tuple[Path, ...] = tuple(canonical_protected)
        self._protected_parts: tuple[tuple[str, ...], ...] = tuple(protected_parts)

    @property
    def protected_paths(self) -> tuple[Path, ...]:
        """全部已规范化的受保护路径集合（含 state_path 5 项及附加受保护根）。"""
        return self._protected_paths

    @property
    def adapter(self) -> FilesystemAdapter:
        return self._adapter

    @property
    def fixed_base_dir(self) -> str | None:
        """构造期显式固定的规范基准目录；未固定时返回 None。"""
        return self._base_dir if self._base_dir_fixed else None

    def _effective_base_dir(self) -> str:
        if self._base_dir_fixed and self._base_dir is not None:
            return self._base_dir
        return self._canonicalize_anchor(os.getcwd())

    def _canonicalize_anchor(self, anchor: str | Path) -> str:
        raw = validate_and_normalize_lexical(anchor, is_windows=self._is_windows)
        if not self._is_absolute(raw):
            cwd = validate_and_normalize_lexical(os.getcwd(), is_windows=self._is_windows)
            raw = _join_platform_path(cwd, (raw,), is_windows=self._is_windows)
        if self._adapter.path_lexists(raw):
            return self._adapter.realpath(raw)
        return raw

    def _is_absolute(self, norm_path: str) -> bool:
        return is_platform_abs_path(norm_path, is_windows=self._is_windows)

    def comparison_key_for(self, path: str | Path) -> str:
        """为已锚定或词法路径生成纯内存平台比较键。"""
        norm = validate_and_normalize_lexical(path, is_windows=self._is_windows)
        if not self._is_absolute(norm):
            norm = _join_platform_path(self._effective_base_dir(), (norm,), is_windows=self._is_windows)
        return _comparison_key_for_path(norm, is_windows=self._is_windows)

    def _canonicalize_path(self, path: str | Path) -> InspectedPath:
        """词法校验 + 锚定 base_dir + 展开最长已存在父前缀（绝不跟随叶链接）。"""
        norm = validate_and_normalize_lexical(path, is_windows=self._is_windows)
        if not self._is_absolute(norm):
            abs_str = _join_platform_path(
                self._effective_base_dir(), (norm,), is_windows=self._is_windows
            )
        else:
            abs_str = norm

        if _is_root_path(abs_str, is_windows=self._is_windows):
            canonical = abs_str
            entry = self._adapter.inspect_entry(canonical)
            kind: PathKind = "directory" if entry.entry_class == "directory" else "missing"
            return InspectedPath(
                canonical_path=canonical,
                comparison_key=_comparison_key_for_path(canonical, is_windows=self._is_windows),
                kind=kind,
            )

        parent, leaf_name = _split_parent_and_name(abs_str, is_windows=self._is_windows)
        tail_parts: list[str] = [leaf_name]
        cursor = parent
        while not self._adapter.path_lexists(cursor):
            if _is_root_path(cursor, is_windows=self._is_windows):
                break
            next_parent, seg = _split_parent_and_name(cursor, is_windows=self._is_windows)
            tail_parts.append(seg)
            cursor = next_parent

        if self._adapter.path_lexists(cursor):
            resolved_prefix = self._adapter.realpath(cursor)
        else:
            resolved_prefix = cursor

        tail_parts.reverse()
        if len(tail_parts) == 1:
            # 父目录存在：仅非跟随探测叶节点，非链接/重解析点时回收叶真实大小写。
            canonical_parent = resolved_prefix
            candidate = _join_platform_path(
                canonical_parent, (leaf_name,), is_windows=self._is_windows
            )
            entry = self._adapter.inspect_entry(candidate)
            if entry.entry_class in ("file", "directory"):
                canonical = self._adapter.recover_leaf_case(canonical_parent, leaf_name)
            else:
                canonical = candidate
        else:
            canonical = _join_platform_path(
                resolved_prefix, tail_parts, is_windows=self._is_windows
            )
            entry = EntryInspection("missing")

        kind_map: dict[EntryClass, PathKind] = {
            "missing": "missing",
            "file": "file",
            "directory": "directory",
            "broken_symlink": "broken_symlink",
            "live_symlink": "file",
            "mount_point": "directory",
            "unsafe_reparse": "file",
            "unsupported": "file",
        }
        return InspectedPath(
            canonical_path=canonical,
            comparison_key=_comparison_key_for_path(canonical, is_windows=self._is_windows),
            kind=kind_map[entry.entry_class],
        )

    def _overlaps_protected(self, comparison_key: str) -> bool:
        target_parts = _path_parts(comparison_key, is_windows=self._is_windows)
        return any(_parts_overlap(target_parts, prot) for prot in self._protected_parts)

    def is_protected(self, path: str | Path) -> bool:
        """判断路径是否与已配置的受保护路径双向重叠（含叶链接/重解析点目标，异常时 fail-closed 返回 True）。"""
        try:
            inspected = self.inspect(path, mode="lookup")
            if self._overlaps_protected(inspected.comparison_key):
                return True
            entry = self._adapter.inspect_entry(inspected.canonical_path)
            if entry.entry_class in ("live_symlink", "mount_point", "unsafe_reparse"):
                resolved = self._adapter.realpath(inspected.canonical_path)
                resolved_key = _comparison_key_for_path(resolved, is_windows=self._is_windows)
                if self._overlaps_protected(resolved_key):
                    return True
        except Exception:
            return True
        return False

    def inspect(
        self,
        path: str | Path,
        *,
        mode: InspectMode = "strict",
        verify_unchanged: bool = False,
    ) -> InspectedPath:
        """统一路径规范化与安全检查入口（strict / pre_delete / lookup）。"""
        if mode not in ("strict", "pre_delete", "lookup"):
            raise ValueError(f"Unsupported inspect mode: {mode!r}")

        inspected = self._canonicalize_path(path)
        if verify_unchanged and inspected.comparison_key != self.comparison_key_for(path):
            raise ValueError(f"artifact parent path changed: {path}")
        if mode == "lookup":
            return inspected

        if _is_root_path(inspected.canonical_path, is_windows=self._is_windows):
            raise ValueError(f"filesystem root cannot be an artifact: {inspected.canonical_path}")

        if self._overlaps_protected(inspected.comparison_key):
            raise ValueError(f"artifact overlaps a protected root: {inspected.canonical_path}")

        entry = self._adapter.inspect_entry(inspected.canonical_path)
        if entry.entry_class == "live_symlink":
            raise ValueError(f"artifact leaf must not be a symlink: {inspected.canonical_path}")
        if entry.entry_class == "broken_symlink":
            if mode != "pre_delete":
                raise ValueError(
                    f"artifact leaf must not be a symlink: {inspected.canonical_path}"
                )
            return InspectedPath(
                canonical_path=inspected.canonical_path,
                comparison_key=inspected.comparison_key,
                kind="broken_symlink",
            )
        if entry.entry_class == "mount_point":
            raise ValueError(
                f"artifact contains an unsafe reparse point (junction or mount point): "
                f"{inspected.canonical_path}"
            )
        if entry.entry_class == "unsafe_reparse":
            raise ValueError(
                f"artifact contains an unsafe reparse point: {inspected.canonical_path}"
            )
        return inspected

    def delete(
        self,
        target: str | InspectedPath,
        *,
        allow_broken_leaf_symlink: bool = False,
        authorize: Callable[[str, TargetType], bool] | None = None,
    ) -> bool:
        """校验路径未漂移后经平台适配器执行非跟随安全删除（缺失返回 False）。"""
        mode: InspectMode = "pre_delete" if allow_broken_leaf_symlink else "strict"
        if isinstance(target, InspectedPath):
            expected_key = target.comparison_key
            raw_target = target.canonical_path
        else:
            expected_key = self.comparison_key_for(target)
            raw_target = target

        inspected = self.inspect(raw_target, mode=mode)
        if inspected.comparison_key != expected_key:
            raise ValueError(f"artifact parent path changed: {raw_target}")
        if not inspected.exists:
            return False

        def revalidate() -> None:
            current = self.inspect(inspected.canonical_path, mode=mode)
            if current.comparison_key != expected_key:
                raise ValueError(f"artifact parent path changed: {raw_target}")

        return self._adapter.delete_target(
            inspected.canonical_path,
            allow_broken_leaf_symlink=allow_broken_leaf_symlink,
            revalidate=revalidate,
            authorize=authorize,
        )

    def evaluate_authorization(
        self,
        existing: Sequence[tuple[str, TargetType]],
        target: str | InspectedPath,
        target_type: TargetType | None = None,
    ) -> AuthorizationMatch:
        """委托纯内存授权匹配与冲突求值（绑定当前策略平台语义）。"""
        return evaluate_authorization(
            existing,
            target,
            target_type,
            is_windows=self._is_windows,
        )


__all__ = [
    "AuthorizationMatch",
    "EntryInspection",
    "FilesystemAdapter",
    "InspectMode",
    "InspectedPath",
    "PathKind",
    "PathPolicy",
    "PosixFilesystemAdapter",
    "TargetType",
    "WindowsFilesystemAdapter",
    "derive_protected_paths",
    "evaluate_authorization",
    "is_platform_abs_path",
    "resolve_lexical_path",
    "validate_and_normalize_lexical",
]
