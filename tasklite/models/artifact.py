"""磁盘产物声明值：仅描述路径、名称与独立删除授权。"""
from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class ArtifactDeclaration:
    """同执行的授权值；路径仅校验词法规范，物理归属由路径策略核验。"""

    path: str
    name: str | None = None
    delete_existing: bool = False
    delete_on_failure: bool = False
    delete_on_success: bool = False
    sandbox: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.path, str):
            raise TypeError("artifact path must be a str")
        if not self.path or "\x00" in self.path or not Path(self.path).is_absolute():
            raise ValueError("artifact path must be a non-empty absolute path without null bytes")
        if os.path.normpath(self.path) != self.path or (os.name == "posix" and self.path.startswith("//")):
            raise ValueError("artifact path must be lexically normalized")
        if self.name is not None:
            if not isinstance(self.name, str):
                raise TypeError("artifact name must be a str or None")
            if not self.name:
                raise ValueError("artifact name must be non-empty")
        for field in ("delete_existing", "delete_on_failure", "delete_on_success", "sandbox"):
            if type(getattr(self, field)) is not bool:
                raise TypeError(f"{field} must be a bool")
        if self.name is not None and self.delete_on_success:
            raise ValueError("artifact name cannot be used with delete_on_success")

    def to_dict(self) -> dict[str, Any]:
        """导出声明数据，不记录执行阶段。"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ArtifactDeclaration:
        """严格读取声明字段，未知键不作为框架状态接纳。"""
        return cls(**data)


def validate_artifact_declarations(
    declarations: Iterable[ArtifactDeclaration],
) -> tuple[ArtifactDeclaration, ...]:
    """校验同执行声明并保序收敛完全一致的重复。"""
    accepted: list[ArtifactDeclaration] = []
    for declaration in declarations:
        if not isinstance(declaration, ArtifactDeclaration):
            raise TypeError("declaration must be an ArtifactDeclaration")
        for previous in accepted:
            if previous == declaration:
                break
            path, old_path = Path(declaration.path), Path(previous.path)
            if path == old_path:
                raise ValueError(f"artifact path already declared with different attributes: {path}")
            if declaration.name is not None and declaration.name == previous.name:
                raise ValueError(f"artifact name already declared: {declaration.name!r}")
            if path.is_relative_to(old_path) or old_path.is_relative_to(path):
                raise ValueError(f"overlapping artifact paths: {old_path}, {path}")
        else:
            accepted.append(declaration)
    return tuple(accepted)


def append_artifact_declaration(
    previous: tuple[ArtifactDeclaration, ...], declaration: ArtifactDeclaration,
) -> tuple[ArtifactDeclaration, ...]:
    """仅比较新增声明与已有声明；完整集合在成功边界复核。"""
    for old in previous:
        if old == declaration:
            return previous
        validate_artifact_declarations((old, declaration))
    return (*previous, declaration)
