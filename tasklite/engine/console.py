"""OpsConsole：run() 外纯运维接缝。

从 StateStore 剥离管理段逻辑（list_dlq / clear_dlq / clear_history / seed_wall /
seed_cursor / list_suspends），供门面 TaskLite 管理 API 委托。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Union

from .store import DLQEntry, StateStore
from .resource import META_RESOURCE_SUSPENDS
from ..backend.base import AbstractStateBackend
from ..models.state import uid_from_job_dict
from ..taxonomy import ErrorTaxonomy, _DEFAULT_TAXONOMY
from ..utils.jsonutil import loads

logger = logging.getLogger("tasklite")


@dataclass(frozen=True)
class SuspendEntry:
    """资源挂起条目（只读运维视图值对象）。

    ``resume_at`` 为挂钟时间戳（epoch 秒），``remaining_seconds`` 为查询
    时刻快照——耗时操作后应以 ``resume_at`` 为准重新换算。
    """

    resource: str
    resume_at: float
    remaining_seconds: float


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

    def set_backend(self, backend: AbstractStateBackend) -> None:
        """重新绑定持久化后端（与 StateStore.set_backend 同步调用）。

        不变式：凡持有后端引用的组件必须随换库同步重绑定，否则管理 API
        静默读写旧库。
        """
        self._backend = backend

    # ── 只读查询 ──────────────────────────────────────────────────────

    def list_suspends(self) -> List[SuspendEntry]:
        """只读查询当前仍生效的资源挂起（跨重启持久化的限流/离线等待）。

        真相源是 meta 表（``resource_suspends``）而非内存 ResourceManager——
        挂起仅在 ``run()`` 启动时恢复进内存，run() 外查询内存态恒为空。
        已过期（解封时刻早于查询时刻）的条目过滤不返回；坏数据降级为
        告警 + 跳过，与启动期恢复路径的 fail-soft 语义一致。

        Returns:
            List[SuspendEntry]: 按 ``resume_at`` 升序（最先解封在前）。
        """
        try:
            raw = self._backend.get_meta(META_RESOURCE_SUSPENDS)
        except Exception as e:
            logger.warning(f"Failed to load resource suspends from meta: {e}")
            return []
        if raw is None:
            return []
        try:
            deadlines = loads(raw)
        except (ValueError, TypeError) as e:
            logger.warning(f"Corrupted resource_suspends meta, ignoring: {e}")
            return []
        if not isinstance(deadlines, dict):
            logger.warning("resource_suspends meta is not a dict, ignoring")
            return []

        now_wall = time.time()
        entries: List[SuspendEntry] = []
        for name, deadline in deadlines.items():
            # loads 契约保证键恒为 str、数值恒有限——仅需防字符串/布尔
            # 等合法 JSON 但语义非法的值（bool 是 int 子类，须先判）。
            if isinstance(deadline, bool) or not isinstance(deadline, (int, float)):
                logger.warning(f"Suspension query: non-numeric deadline for {name!r}, skipping")
                continue
            remaining = deadline - now_wall
            if remaining <= 0:
                continue
            entries.append(SuspendEntry(
                resource=name,
                resume_at=float(deadline),
                remaining_seconds=float(remaining),
            ))
        entries.sort(key=lambda e: e.resume_at)
        return entries

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
        """把 uid 批量写入 wall（存档迁移标记「已处理」），返回实际写入数。

        不变式（六集合互斥）：已在 failed（DLQ）或驻留队列的 uid 拒绝
        种子。failed 与 queue 两腿均以后端持久层为真相（run 前内存 state
        是空占位，仅查内存腿会漏掉落盘队列驻留）。预检先于任何写入
        （backend 持久层与内存 state 双腿零写入），冲突整体拒绝——静默
        写入会留下 wall∩failed / wall∩queue 非豁免重叠，令下一次状态
        变更的一致性断言崩溃；DLQ 记录须先经 clear_dlq/clear_history
        显式清除，队列驻留须待 run 排空或显式清理后再种子。
        """
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
        failed = self._backend.load_failed()
        queue_uids = set(self._store.queue_uids)
        queue_uids.update(uid_from_job_dict(jd) for jd in self._backend.load_queue())
        conflict_failed = sorted({u for u in uids if u in failed})
        conflict_queue = sorted({u for u in uids if u in queue_uids})
        if conflict_failed:
            raise ValueError(
                f"seed_wall refuses uid(s) already in failed (DLQ): {conflict_failed}; "
                f"wall/failed must stay disjoint. Clear the DLQ entries first "
                f"(clear_dlq/clear_history) if archiving is intended."
            )
        if conflict_queue:
            raise ValueError(
                f"seed_wall refuses uid(s) still resident in the queue: "
                f"{conflict_queue}; wall/queue must stay disjoint. Drain or "
                f"clear the queue entries first if archiving is intended."
            )
        written = self._backend.seed_wall(list(uids))
        state = self._store.state
        for u in uids:
            state.add_wall(u, {})
        return written

    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（幂等）——存档迁移/进度书签恢复。

        入口即校验（与 SQLiteStateBackend.seed_cursor 同契约）：key 非空
        str、value 必须 str。委托前拒绝，保证持久层与内存镜像双腿零写入
        且双写值一致。
        """
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        if not isinstance(value, str):
            raise TypeError(f"cursor value must be str, got {type(value).__name__}")
        self._backend.seed_cursor(key, value)
        self._store.state.set_cursor(key, value)
