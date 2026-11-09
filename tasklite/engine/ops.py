"""Run 外管理接缝。"""

from __future__ import annotations

from typing import Collection, Literal

from .store import StateStore


class OpsConsole:
    """把公开管理调用委托给 StateStore 的薄接缝。"""

    def __init__(self, store: StateStore) -> None:
        self._store = store

    def clear_history(
        self,
        uid: str,
        *,
        records: Collection[Literal["successes", "failures", "attempts"]],
    ) -> int:
        return self._store.clear_history(uid, records)


__all__ = ["OpsConsole"]
