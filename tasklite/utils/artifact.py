"""磁盘产物路径校验与不跟随链接的删除原语。

删除要求 fd 相对操作能力；目录额外要求 Linux mountinfo。身份校验仅覆盖
单次删除操作，不保存声明期身份，也不承诺识别此前已完成的普通父目录替换。
"""
from __future__ import annotations

import os
import re
import stat
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Sequence

from ..models.artifact import ArtifactDeclaration

_DIRECTORY_OPEN_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)


def _execute_pre_deletes(
    policy: "ArtifactPathPolicy",
    declarations: Sequence[ArtifactDeclaration],
) -> None:
    """按声明顺序执行前置删除；缺失目标由路径原语幂等吸收。"""
    for declaration in declarations:
        policy.remove_existing(declaration)


class ArtifactPathPolicy:
    """路径范围配置；不登记全局所有权或持久执行阶段。"""

    def __init__(
        self,
        *,
        output_roots: str | Path | Sequence[str | Path] | None = None,
        protected_roots: Sequence[str | Path] = (),
        authorization_check: Callable[[str, str], bool] | None = None,
    ) -> None:
        if isinstance(output_roots, (str, Path)):
            output_roots = [output_roots]
        self.output_roots = tuple(Path(root).resolve() for root in (output_roots or ()))
        self.protected_roots = tuple(Path(root).resolve() for root in protected_roots)
        self._authorization_check = authorization_check

    def resolve(
        self, path: str | Path, *, sandbox: bool = True, delete_existing: bool = False,
    ) -> str:
        """返回受允许的规范路径；仅前删可接受断链，不授权链接指向的目标。"""
        return self._resolve(path, sandbox=sandbox, allow_leaf_symlink=delete_existing)

    def _resolve(
        self,
        path: str | Path,
        *,
        sandbox: bool,
        allow_leaf_symlink: bool,
    ) -> str:
        raw = os.fspath(path)
        if not isinstance(raw, str) or not raw or "\x00" in raw:
            raise ValueError("artifact path must be a non-empty path without null bytes")
        target = Path(raw)
        if not target.is_absolute() and sandbox and self.output_roots:
            target = self.output_roots[0] / target
        target = Path(os.path.abspath(target))
        try:
            leaf = os.lstat(target)
        except FileNotFoundError:
            leaf = None
        if leaf is not None and stat.S_ISLNK(leaf.st_mode):
            if not allow_leaf_symlink:
                raise ValueError(f"artifact leaf must not be a symlink: {target}")
            try:
                os.stat(target)
            except FileNotFoundError:
                pass
            except OSError as error:
                raise ValueError(f"cannot inspect artifact leaf: {target}") from error
            else:
                raise ValueError(f"artifact leaf must not be a symlink: {target}")
            try:
                target = target.parent.resolve() / target.name
            except RuntimeError as error:
                raise ValueError(f"cannot resolve artifact path: {target}") from error
        else:
            try:
                target = target.resolve()
            except RuntimeError as error:
                raise ValueError(f"cannot resolve artifact path: {target}") from error
        if sandbox and self.output_roots and not any(
            target.is_relative_to(root) for root in self.output_roots
        ):
            raise ValueError(f"artifact path is outside output roots: {target}")
        if target.parent == target:
            raise ValueError(f"filesystem root cannot be an artifact: {target}")
        for root in self.output_roots:
            if root.is_relative_to(target):
                raise ValueError(f"artifact must not cover an output root: {target}")
        for root in self.protected_roots:
            if root.is_relative_to(target) or target.is_relative_to(root):
                raise ValueError(f"artifact overlaps a protected root: {target}")
        return str(target)

    def remove_existing(self, declaration: ArtifactDeclaration) -> None:
        """仅以前删授权删除已有对象，其他清理权限不代替该授权。"""
        target = self._target(declaration, allow_leaf_symlink=True)
        if not self._exists(target):
            return
        if not declaration.delete_existing:
            raise FileExistsError(str(target))
        self._remove(target, declaration, allow_leaf_symlink=True)

    def cleanup(self, declaration: ArtifactDeclaration, *, success: bool) -> None:
        """只应用指定结局的清理授权，缺失路径视为已移除。"""
        if type(success) is not bool:
            raise TypeError("success must be a bool")
        authorized = declaration.delete_on_success if success else declaration.delete_on_failure
        if authorized:
            target = self._target(declaration)
            if self._exists(target):
                self._remove(target, declaration)

    def entry_exists(self, declaration: ArtifactDeclaration) -> bool:
        """以非跟随方式判断声明目标是否存在；访问错误不降级为缺失。"""
        return self._exists(self._target(declaration))

    @staticmethod
    def _exists(target: Path) -> bool:
        try:
            os.lstat(target)
            return True
        except FileNotFoundError:
            return False

    def _target(
        self,
        declaration: ArtifactDeclaration,
        *,
        allow_leaf_symlink: bool = False,
    ) -> Path:
        target = self._resolve(
            declaration.path,
            sandbox=declaration.sandbox,
            allow_leaf_symlink=allow_leaf_symlink,
        )
        if target != declaration.path:
            raise ValueError(f"artifact parent path changed: {declaration.path}")
        return Path(target)

    @staticmethod
    def _require_relative_operations() -> None:
        if not all(operation in os.supports_dir_fd for operation in (os.open, os.stat, os.unlink, os.rmdir)):
            raise NotImplementedError("safe directory-relative artifact deletion is unavailable")
        if os.listdir not in os.supports_fd:
            raise NotImplementedError("safe directory enumeration is unavailable")
        if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
            raise NotImplementedError("non-following artifact deletion is unavailable")

    @staticmethod
    def _same_object(first: os.stat_result, second: os.stat_result) -> bool:
        return (first.st_dev, first.st_ino) == (second.st_dev, second.st_ino)

    def _remove(
        self,
        target: Path,
        declaration: ArtifactDeclaration,
        *,
        allow_leaf_symlink: bool = False,
    ) -> None:
        self._require_relative_operations()
        before = target.parent.stat(follow_symlinks=False)
        parent_fd = os.open(target.parent, _DIRECTORY_OPEN_FLAGS)
        try:
            if not self._same_object(before, os.fstat(parent_fd)):
                raise ValueError(f"artifact parent identity changed: {target.parent}")
            self._target(declaration, allow_leaf_symlink=allow_leaf_symlink)
            try:
                entry = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return

            target_type = "directory" if stat.S_ISDIR(entry.st_mode) else "file"
            if self._authorization_check is not None and not self._authorization_check(
                str(target), target_type,
            ):
                raise PermissionError(f"artifact authorization scope changed before deletion: {target}")

            if stat.S_ISDIR(entry.st_mode):
                if any(point.is_relative_to(target) for point in self._mount_points()):
                    raise ValueError(f"artifact directory contains a mount: {target}")

            def guard() -> None:
                self._target(declaration, allow_leaf_symlink=allow_leaf_symlink)
                if not self._same_object(before, target.parent.stat(follow_symlinks=False)):
                    raise ValueError(f"artifact parent identity changed: {target.parent}")
                current = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
                if not self._same_object(entry, current):
                    raise ValueError(f"artifact identity changed during deletion: {target}")

            self._remove_at(
                parent_fd,
                target,
                guard,
                allow_symlink=allow_leaf_symlink,
            )
        finally:
            os.close(parent_fd)

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

    def _remove_at(
        self, parent_fd: int, target: Path, guard: Callable[[], None], *, allow_symlink: bool,
    ) -> None:
        guard()
        parent_now = target.parent.stat(follow_symlinks=False)
        if not self._same_object(parent_now, os.fstat(parent_fd)):
            raise ValueError(f"artifact directory identity changed: {target.parent}")
        entry = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
        if stat.S_ISLNK(entry.st_mode):
            if not allow_symlink:
                raise ValueError(f"artifact leaf must not be a symlink: {target}")
        elif getattr(entry, "st_file_attributes", 0) & 0x400:
            raise ValueError(f"artifact contains an unsafe reparse point: {target}")
        elif stat.S_ISDIR(entry.st_mode):
            if entry.st_dev != os.fstat(parent_fd).st_dev or os.path.ismount(target):
                raise ValueError(f"artifact deletion must not cross a mount: {target}")
            child_fd = os.open(target.name, _DIRECTORY_OPEN_FLAGS,
                               dir_fd=parent_fd)
            try:
                if not self._same_object(entry, os.fstat(child_fd)):
                    raise ValueError(f"artifact directory identity changed: {target}")
                for name in os.listdir(child_fd):
                    self._remove_at(child_fd, target / name, guard, allow_symlink=True)
                guard()
                if not self._same_object(entry, os.stat(target.name, dir_fd=parent_fd,
                                                        follow_symlinks=False)):
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
