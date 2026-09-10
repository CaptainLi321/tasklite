"""OpsConsole：run() 外纯运维接缝。

从 StateStore 剥离管理段逻辑（list_dlq / clear_dlq / clear_history / seed_wall /
seed_cursor），供门面 TaskLite 管理 API 委托。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Union

from .store import DLQEntry, StateStore
from ..backend.base import AbstractStateBackend
from ..taxonomy import ErrorTaxonomy, _DEFAULT_TAXONOMY


class OpsConsole:
    """run() 外的纯运维接缝——管理段操作委托目标。

    构造依赖 (backend, store, taxonomy)：list_dlq 分类依赖 taxonomy.classify。
    """

    def __init__(
        self,
        backend: AbstractStateBackend,
        store: StateStore,
        taxonomy: ErrorTaxonomy,
    ) -> None:
        self._backend = backend
        self._store = store
        self._taxonomy = taxonomy

    # ── 只读查询 ──────────────────────────────────────────────────────

    def list_dlq(self) -> List[DLQEntry]:
        """只读查询 DLQ，返回结构化条目（uid / error_type / error / attempts / failed_at / meta）。"""
        failed = self._backend.load_failed()
        entries: List[DLQEntry] = []
        for uid, meta in sorted(failed.items()):
            if not isinstance(meta, dict):
                entries.append(
                    DLQEntry(
                        uid=uid,
                        error_type=self._taxonomy.classify(meta).dlq_error_type,
                        error="",
                        attempts=0,
                        failed_at=None,
                        meta={},
                    )
                )
                continue
            attempts = meta.get("_attempt", 0)
            if not isinstance(attempts, int):
                attempts = 0
            entries.append(
                DLQEntry(
                    uid=uid,
                    error_type=self._taxonomy.classify(meta).dlq_error_type,
                    error=str(meta.get("error", "")),
                    attempts=attempts,
                    failed_at=meta.get("failed_at"),
                    meta=dict(meta),
                )
            )
        return entries

    # ── 变更操作 ──────────────────────────────────────────────────────

    def clear_dlq(
        self,
        task_types: Optional[Sequence[str]] = None,
        *,
        keep_fatal: bool = True,
    ) -> int:
        """从 DLQ 删除匹配条目（默认保留 fatal=true 的确定性失败），返回删除数。"""
        if task_types is not None:
            if not isinstance(task_types, (list, tuple)):
                raise TypeError(
                    f"task_types must be a list/tuple of str or None, "
                    f"got {type(task_types).__name__}"
                )
            for t in task_types:
                if not isinstance(t, str) or not t:
                    raise TypeError(
                        f"task_types must contain only non-empty str, got {t!r}"
                    )
            task_types = list(task_types)
        failed = self._backend.load_failed()
        to_delete = [
            uid for uid, meta in failed.items()
            if (task_types is None or any(uid.startswith(t + "::") for t in task_types))
            and not (keep_fatal and isinstance(meta, dict) and meta.get("fatal"))
        ]
        if not to_delete:
            return 0
        deleted_count = self._backend.delete_failed(to_delete)
        state = self._store.state
        for uid in to_delete:
            state.discard_failed(uid)
        return deleted_count

    def clear_history(
        self,
        targets: Union[str, Sequence[str]],
        *,
        where: Sequence[str] = ("wall", "failed"),
    ) -> int:
        """从 wall 和/或 DLQ 删除条目。"""
        if isinstance(targets, str):
            patterns = [targets]
        elif isinstance(targets, (list, tuple)):
            patterns = list(targets)
        else:
            raise TypeError(
                f"targets must be a str or a list/tuple of str, "
                f"got {type(targets).__name__}"
            )
        for p in patterns:
            if not isinstance(p, str):
                raise TypeError(
                    f"targets must contain only str, got {type(p).__name__} ({p!r})"
                )
        if not isinstance(where, (list, tuple)):
            raise TypeError(
                f"where must be a sequence of 'wall'/'failed', got {type(where).__name__}"
            )
        where_set = set(where)
        unknown = where_set - {"wall", "failed"}
        if unknown:
            raise ValueError(
                f"where contains unknown target(s): {sorted(unknown)!r}; "
                f"allowed: 'wall', 'failed'"
            )

        def _matches(uid: str) -> bool:
            return any(
                uid == p or (p.endswith("::") and uid.startswith(p))
                for p in patterns
            )

        state = self._store.state
        total = 0
        if "wall" in where:
            wall = self._backend.load_wall()
            matched = [u for u in wall if _matches(u)]
            if matched:
                total += self._backend.delete_wall(matched)
                for u in matched:
                    state.discard_wall(u)
        if "failed" in where:
            failed = self._backend.load_failed()
            matched = [u for u in failed if _matches(u)]
            if matched:
                total += self._backend.delete_failed(matched)
                for u in matched:
                    state.discard_failed(u)
        return total

    def seed_wall(self, uids: Sequence[str]) -> int:
        """把 uid 批量写入 wall（存档迁移标记「已处理」），返回实际写入数。"""
        if not isinstance(uids, (list, tuple)):
            raise TypeError(
                f"uids must be a list/tuple of str, got {type(uids).__name__}"
            )
        for u in uids:
            if not isinstance(u, str) or u.count("::") != 1:
                raise ValueError(
                    f"seed_wall uid must be 'task_type::job_id' str with exactly "
                    f"one '::' separator, got {u!r}"
                )
            task_type, job_id = u.split("::", 1)
            if not task_type or not job_id:
                raise ValueError(
                    f"seed_wall uid must have non-empty task_type and job_id, got {u!r}"
                )
        written = self._backend.seed_wall(list(uids))
        state = self._store.state
        for u in uids:
            state.add_wall(u, {})
        return written

    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（幂等）——存档迁移/进度书签恢复。"""
        self._backend.seed_cursor(key, value)
        self._store.state.set_cursor(key, value)
