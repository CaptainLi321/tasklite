from __future__ import annotations

import os
from typing import TYPE_CHECKING, Sequence

from ..exceptions import TaskLiteError
from ..models.artifact import FileSpec

if TYPE_CHECKING:
    from .store import StateStore


class FileSpecResolver:
    """内部 FileSpec 共同解析知识深化：普通路径解析、上游产物引用绑定与输入存在性校验。"""

    def __init__(self, store: StateStore) -> None:
        self._store = store

    def resolve(self, spec: FileSpec, *, depends_on: Sequence[str]) -> FileSpec:
        """将声明值对象解析为绝对路径绑定的 FileSpec。

        普通路径基于 store.base_dir 绝对化；
        上游引用校验直接依赖 producer 并从 store.state.successes 绑定路径；
        role="input" 时校验物理文件存在性。
        """
        store = self._store
        path_policy = store.path_policy
        if spec.is_output_ref:
            assert spec.producer_uid is not None and spec.output_name is not None
            if spec.producer_uid not in depends_on:
                raise TaskLiteError(
                    f"Producer {spec.producer_uid!r} not in explicit direct depends_on {list(depends_on)!r}"
                )
            producer_artifacts = store.state.successes.get(
                spec.producer_uid, {}
            ).get("artifacts", {})
            desc = producer_artifacts.get(spec.output_name)
            if not desc or "path" not in desc:
                raise TaskLiteError(
                    f"Producer {spec.producer_uid!r} did not publish output {spec.output_name!r}"
                )
            try:
                resolved_path = path_policy.inspect(desc["path"], mode="lookup").canonical_path
            except ValueError as exc:
                raise TaskLiteError(str(exc)) from exc
            if spec.role == "input" and not os.path.exists(resolved_path):
                raise FileNotFoundError(f"Missing input file: {resolved_path}")
            return spec._bind_output_path(resolved_path)

        assert spec.path is not None
        try:
            resolved_path = path_policy.inspect(spec.path, mode="lookup").canonical_path
        except ValueError as exc:
            raise TaskLiteError(str(exc)) from exc
        bound_spec = spec._bind_output_path(resolved_path)
        if spec.role == "input" and not os.path.exists(resolved_path):
            raise FileNotFoundError(f"Missing input file: {resolved_path}")
        return bound_spec


__all__ = ["FileSpecResolver"]
