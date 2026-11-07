"""磁盘文件声明值与配置解析：FileSpec 统一声明值对象及提交期快照规范化。"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
import os
from pathlib import Path
from typing import Any, Literal, cast

from ..exceptions import TaskLiteError

FILE_ROLES: tuple[str, ...] = ("input", "output", "cache")
FileRole = Literal["input", "output", "cache"]

_PATH_SPEC_KEYS = frozenset({
    "path",
    "role",
    "delete_existing",
    "delete_on_failure",
    "delete_on_success",
})
_OUTPUT_REF_SPEC_KEYS = frozenset({
    "producer_uid",
    "output_name",
    "role",
    "delete_existing",
    "delete_on_failure",
    "delete_on_success",
})


def _normalize_lexical_path(path: str | Path) -> str:
    """校验并词法规范化路径字符串（不触碰磁盘，保留相对或绝对形态）。"""
    if not isinstance(path, (str, Path)):
        raise TypeError(
            f"FileSpec path must be a str or Path, got {type(path).__name__}"
        )
    if isinstance(path, str) and not path:
        raise ValueError("FileSpec path must be non-empty")
    raw = os.fspath(path)
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        raise ValueError("FileSpec path must be a non-empty path without null bytes")
    norm = os.path.normpath(raw)
    if os.name == "posix" and norm.startswith("//"):
        norm = "/" + norm.lstrip("/")
    return norm


def resolve_path_against_base(path: str | Path, base_dir: str | Path) -> str:
    """按固定基准目录将路径解析为词法规范的绝对路径（不创建目录或访问磁盘）。"""
    norm = _normalize_lexical_path(path)
    base_norm = _normalize_lexical_path(base_dir)
    if not os.path.isabs(base_norm):
        base_norm = os.path.abspath(base_norm)
    if os.path.isabs(norm):
        resolved = norm
    else:
        resolved = os.path.normpath(os.path.join(base_norm, norm))
    if os.name == "posix" and resolved.startswith("//"):
        resolved = "/" + resolved.lstrip("/")
    return resolved


def _validate_role_and_cleanup(
    role: Any,
    delete_existing: Any,
    delete_on_failure: Any,
    delete_on_success: Any,
) -> FileRole:
    if not isinstance(role, str):
        raise TypeError(f"FileSpec role must be a str, got {type(role).__name__}")
    if role not in FILE_ROLES:
        raise ValueError(f"FileSpec role must be one of {FILE_ROLES}, got {role!r}")
    for field_name, value in (
        ("delete_existing", delete_existing),
        ("delete_on_failure", delete_on_failure),
        ("delete_on_success", delete_on_success),
    ):
        if type(value) is not bool:
            raise TypeError(f"{field_name} must be a bool, got {type(value).__name__}")
    if role == "input" and delete_existing:
        raise ValueError("input FileSpec cannot set delete_existing=True")
    return cast(FileRole, role)


@dataclass(frozen=True, init=False)
class FileSpec:
    """统一文件声明值对象（普通路径声明或上游具名产物引用）。"""

    path: str | None
    producer_uid: str | None
    output_name: str | None
    role: FileRole
    delete_existing: bool
    delete_on_failure: bool
    delete_on_success: bool

    def __init__(
        self,
        path: str | Path,
        *,
        role: FileRole,
        delete_existing: bool = False,
        delete_on_failure: bool = False,
        delete_on_success: bool = False,
    ) -> None:
        norm_path = _normalize_lexical_path(path)
        validated_role = _validate_role_and_cleanup(
            role, delete_existing, delete_on_failure, delete_on_success
        )
        object.__setattr__(self, "path", norm_path)
        object.__setattr__(self, "producer_uid", None)
        object.__setattr__(self, "output_name", None)
        object.__setattr__(self, "role", validated_role)
        object.__setattr__(self, "delete_existing", delete_existing)
        object.__setattr__(self, "delete_on_failure", delete_on_failure)
        object.__setattr__(self, "delete_on_success", delete_on_success)

    @classmethod
    def from_output(
        cls,
        producer_uid: str,
        output_name: str,
        *,
        role: FileRole,
        delete_existing: bool = False,
        delete_on_failure: bool = False,
        delete_on_success: bool = False,
    ) -> FileSpec:
        """构造指向显式直接依赖上游具名 output 产物的引用声明。"""
        if not isinstance(producer_uid, str):
            raise TypeError(
                f"producer_uid must be a str, got {type(producer_uid).__name__}"
            )
        if not producer_uid or "\x00" in producer_uid:
            raise ValueError("producer_uid must be a non-empty str")
        if not isinstance(output_name, str):
            raise TypeError(
                f"output_name must be a str, got {type(output_name).__name__}"
            )
        if not output_name or "\x00" in output_name:
            raise ValueError("output_name must be a non-empty str")
        validated_role = _validate_role_and_cleanup(
            role, delete_existing, delete_on_failure, delete_on_success
        )
        instance = object.__new__(cls)
        object.__setattr__(instance, "path", None)
        object.__setattr__(instance, "producer_uid", producer_uid)
        object.__setattr__(instance, "output_name", output_name)
        object.__setattr__(instance, "role", validated_role)
        object.__setattr__(instance, "delete_existing", delete_existing)
        object.__setattr__(instance, "delete_on_failure", delete_on_failure)
        object.__setattr__(instance, "delete_on_success", delete_on_success)
        return instance

    @property
    def is_output_ref(self) -> bool:
        """是否为上游具名产物引用（from_output）。"""
        return self.producer_uid is not None

    def resolve_against(self, base_dir: str | Path) -> FileSpec:
        """按固定基准目录解析普通路径为绝对路径；上游引用保持原样。"""
        _normalize_lexical_path(base_dir)
        if self.is_output_ref:
            return self
        assert self.path is not None
        abs_path = resolve_path_against_base(self.path, base_dir)
        if abs_path == self.path:
            return self
        return FileSpec(
            abs_path,
            role=self.role,
            delete_existing=self.delete_existing,
            delete_on_failure=self.delete_on_failure,
            delete_on_success=self.delete_on_success,
        )

    def bind_output_path(
        self,
        resolved_path: str | Path,
        *,
        base_dir: str | Path | None = None,
    ) -> FileSpec:
        """将上游引用或声明绑定为具体绝对路径 FileSpec（保留当前声明的 role 与清理开关）。"""
        if base_dir is not None:
            abs_path = resolve_path_against_base(resolved_path, base_dir)
        else:
            abs_path = _normalize_lexical_path(resolved_path)
            if not os.path.isabs(abs_path):
                raise ValueError(
                    f"resolved_path must be an absolute path when base_dir is None, got {resolved_path!r}"
                )
        return FileSpec(
            abs_path,
            role=self.role,
            delete_existing=self.delete_existing,
            delete_on_failure=self.delete_on_failure,
            delete_on_success=self.delete_on_success,
        )

    def to_dict(self) -> dict[str, Any]:
        """导出统一 JSON 字典表示（路径型或上游引用型）。"""
        if self.is_output_ref:
            return {
                "producer_uid": self.producer_uid,
                "output_name": self.output_name,
                "role": self.role,
                "delete_existing": self.delete_existing,
                "delete_on_failure": self.delete_on_failure,
                "delete_on_success": self.delete_on_success,
            }
        return {
            "path": self.path,
            "role": self.role,
            "delete_existing": self.delete_existing,
            "delete_on_failure": self.delete_on_failure,
            "delete_on_success": self.delete_on_success,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FileSpec:
        """从统一 JSON 字典表示严格还原 FileSpec，拒绝未知键或混用形态。"""
        if not isinstance(data, Mapping):
            raise TypeError(
                f"FileSpec data must be a dict, got {type(data).__name__}"
            )
        keys = set(data.keys())
        if keys == _PATH_SPEC_KEYS:
            raw_path = data["path"]
            if not isinstance(raw_path, str):
                raise TypeError(
                    f"FileSpec JSON path must be a str, got {type(raw_path).__name__}"
                )
            return cls(
                raw_path,
                role=data["role"],
                delete_existing=data["delete_existing"],
                delete_on_failure=data["delete_on_failure"],
                delete_on_success=data["delete_on_success"],
            )
        if keys == _OUTPUT_REF_SPEC_KEYS:
            return cls.from_output(
                data["producer_uid"],
                data["output_name"],
                role=data["role"],
                delete_existing=data["delete_existing"],
                delete_on_failure=data["delete_on_failure"],
                delete_on_success=data["delete_on_success"],
            )
        raise ValueError(
            f"Invalid FileSpec dict keys: {sorted(keys)!r}; expected "
            f"{sorted(_PATH_SPEC_KEYS)!r} or {sorted(_OUTPUT_REF_SPEC_KEYS)!r}"
        )


def validate_file_name(name: Any) -> str:
    """校验文件声明名称：非空且不含空字节的 str。"""
    if not isinstance(name, str) or not name:
        raise TypeError(
            f"file key in files must be a non-empty str, "
            f"got {type(name).__name__} ({name!r})"
        )
    if "\x00" in name:
        raise ValueError("file key in files must not contain null bytes")
    return name


def coerce_file_spec(value: Any, *, allow_dict: bool = False) -> FileSpec:
    """校验并转换单个 FileSpec（公开构造仅接受 FileSpec，内部恢复可选接受 dict）。"""
    if isinstance(value, FileSpec):
        return value
    if allow_dict and isinstance(value, Mapping):
        return FileSpec.from_dict(value)
    raise TypeError(
        f"file spec in files must be a FileSpec, got {type(value).__name__} ({value!r})"
    )


def parse_job_files_config(
    files: Mapping[str, Any] | None,
    *,
    allow_dict: bool = True,
) -> dict[str, FileSpec]:
    """将 files 映射解析为独立的 {name: FileSpec} 字典（供 Job 构造/恢复与运行时读取）。"""
    if files is None:
        return {}
    if not isinstance(files, dict):
        raise TypeError(
            f"files must be a dict or None, got {type(files).__name__} ({files!r})"
        )
    parsed: dict[str, FileSpec] = {}
    for raw_name, raw_spec in files.items():
        name = validate_file_name(raw_name)
        parsed[name] = coerce_file_spec(raw_spec, allow_dict=allow_dict)
    return parsed


def resolve_job_files_config(
    files: Mapping[str, Any] | None,
    *,
    base_dir: str | Path,
    depends_on: Sequence[str] = (),
) -> dict[str, dict[str, Any]]:
    """提交期解析并校验 Job.files 配置快照（enqueue / spawn / 恢复规范化唯一入口）。

    - 普通路径声明按固定 base_dir 解析为词法规范绝对路径；
    - from_output 保留引用配置，要求 producer_uid 属于当前 Job 的显式直接依赖
      depends_on，不在提交时绑定历史产物；缺失直接依赖时抛 TaskLiteError；
    - 返回可直接 JSON 序列化的独立字典。
    """
    parsed = parse_job_files_config(files, allow_dict=True)
    direct_deps = set(depends_on)
    resolved: dict[str, dict[str, Any]] = {}
    for name, spec in parsed.items():
        if spec.is_output_ref:
            if spec.producer_uid not in direct_deps:
                raise TaskLiteError(
                    f"FileSpec {name!r} references producer {spec.producer_uid!r} "
                    f"which is not in explicit direct depends_on {list(depends_on)!r}"
                )
            resolved_spec = spec
        else:
            resolved_spec = spec.resolve_against(base_dir)
        resolved[name] = resolved_spec.to_dict()
    return resolved


def merge_named_file_spec(
    existing: Mapping[str, FileSpec],
    name: str,
    spec: FileSpec,
    *,
    base_dir: str | Path | None = None,
    depends_on: Sequence[str] | None = None,
) -> tuple[dict[str, FileSpec], bool]:
    """按名称合并/校验 FileSpec 声明（用于预声明与动态声明并集及同名复用判断）。

    返回 ``(merged_dict, is_new)``：
    - 同名且规范化后声明完全相同：幂等复用，返回 ``(拷贝, False)``；
    - 同名但声明不同：抛 ``TaskLiteError``；
    - 不同名称指向同路径、父子重叠目录或分属 input/output：允许，返回 ``(新字典, True)``。
    """
    validated_name = validate_file_name(name)
    if not isinstance(spec, FileSpec):
        raise TypeError(
            f"spec must be a FileSpec, got {type(spec).__name__} ({spec!r})"
        )
    if spec.is_output_ref and depends_on is not None:
        if spec.producer_uid not in set(depends_on):
            raise TaskLiteError(
                f"FileSpec {validated_name!r} references producer {spec.producer_uid!r} "
                f"which is not in explicit direct depends_on {list(depends_on)!r}"
            )
    canonical = spec.resolve_against(base_dir) if base_dir is not None else spec
    merged: dict[str, FileSpec] = {}
    for raw_key, raw_val in existing.items():
        k = validate_file_name(raw_key)
        v = coerce_file_spec(raw_val, allow_dict=True)
        merged[k] = v.resolve_against(base_dir) if base_dir is not None else v
    if validated_name in merged:
        previous = merged[validated_name]
        if previous == canonical:
            return merged, False
        raise TaskLiteError(
            f"File declaration {validated_name!r} conflicts with existing declaration: "
            f"{previous!r} vs {canonical!r}"
        )
    merged[validated_name] = canonical
    return merged, True


# ── 运行时产物声明值 ──────────────────────────────────────────────────

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


__all__ = [
    "FILE_ROLES",
    "FileRole",
    "FileSpec",
    "coerce_file_spec",
    "merge_named_file_spec",
    "parse_job_files_config",
    "resolve_job_files_config",
    "resolve_path_against_base",
    "validate_file_name",
    "ArtifactDeclaration",
    "append_artifact_declaration",
    "validate_artifact_declarations",
]
