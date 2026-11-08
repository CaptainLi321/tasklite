"""OpsConsole：run() 外纯运维接缝。

从 StateStore 剥离管理段逻辑（list_failures / clear_failures / retry_
failure / clear_history / seed_successes / seed_cursor / list_suspensions /
filter_jobs_without_success），供门面管理 API 委托。不变式：管理 API 仅限 run() 外
调用（改变 is_known 判定基础，与 enqueue 同纪律）。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Collection, Literal, Sequence

from .errorclass import ErrorClassifier
from .store import FailureEntry, StateStore
from ..exceptions import TaskLiteError
from ..models.job import Job
from ..models.state import job_uid_from_snapshot
from ..utils.encoding import parse_job_uid
from ..utils.jsonutil import loads

logger = logging.getLogger("tasklite")

ENGINE_METADATA_RESOURCE_SUSPENSIONS = "resource_suspensions"


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

    构造依赖 (store, classifier)：持久化后端经 ``store.backend`` 只读
    派生（所有权唯一锚定 StateStore，换库随锚点自然跟随，本类不独立
    持库）；list_failures 的 error_category 归档视图依赖 classifier.classify。
    """

    def __init__(
        self,
        store: StateStore,
        classifier: ErrorClassifier,
    ) -> None:
        self._store = store
        self._classifier = classifier

    # ── 只读查询 ──────────────────────────────────────────────────────

    def list_suspensions(self) -> list[SuspendEntry]:
        """只读查询当前仍生效的资源挂起（跨重启持久化的限流/离线等待）。

        真相源是 engine_metadata 表（``resource_suspensions``）而非内存
        ResourceManager——挂起仅在 ``run()`` 启动时恢复进内存，run() 外
        查询内存态恒为空。已过期（解封时刻早于查询时刻）的条目过滤不
        返回；坏数据降级为告警 + 跳过，与启动期恢复路径的 fail-soft
        语义一致。

        Returns:
            list[SuspendEntry]: 按 ``resume_at`` 升序（最先解封在前）。
        """
        try:
            raw = self._store.backend.get_engine_metadata(ENGINE_METADATA_RESOURCE_SUSPENSIONS)
        except Exception as e:
            logger.warning(f"Failed to load resource suspensions from engine_metadata: {e}")
            return []
        if raw is None:
            return []
        try:
            deadlines = loads(raw)
        except (ValueError, TypeError) as e:
            logger.warning(f"Corrupted resource_suspensions engine_metadata, ignoring: {e}")
            return []
        if not isinstance(deadlines, dict):
            logger.warning("resource_suspensions engine_metadata is not a dict, ignoring")
            return []

        now_wall = time.time()
        entries: list[SuspendEntry] = []
        for name, deadline in deadlines.items():
            # loads 契约保证键恒为 str、数值恒有限——仅需防字符串/布尔
            # 等合法 JSON 但语义非法的值（bool 是 int 子类，须先判）。
            if isinstance(deadline, bool) or not isinstance(deadline, (int, float)):
                logger.warning(
                    f"Suspension query: non-numeric deadline for {name!r}, skipping"
                )
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

    def list_failures(self) -> list[FailureEntry]:
        """只读查询失败档案，返回结构化条目（job_uid / error_message / metadata /
        payload）。

        meta 视图缺 error_category（带外登记的裸行）时经 classifier 就地归档
        （unknown 兜底）；损坏行（非 dict）归空视图，绝不让单条脏数据
        打断整条查询。
        """
        failed = self._store.backend.load_failed()
        payloads = self._store.backend.load_failure_payloads()
        entries: list[FailureEntry] = []
        for uid, meta in sorted(failed.items()):
            if not isinstance(meta, dict):
                meta = {}
            meta_view = dict(meta)
            if not meta_view.get("error_category"):
                meta_view["error_category"] = (
                    self._classifier.classify(meta).error_category
                )
            entries.append(FailureEntry(
                job_uid=uid,
                error_message=str(meta_view.get("error_message", "")),
                metadata=meta_view,
                payload=payloads.get(uid),
            ))
        return entries

    def filter_jobs_without_success(self, jobs: Sequence[Job]) -> list[Job]:
        """过滤出尚未成功完成的 job（uid 不在 successes 的子集，保留输入顺序）。

        回溯/增量场景的入队前过滤辅助。边界语义：
        - 只按 successes（成功历史）过滤；失败档案（failed）中的 job **不**
          排除——重新入队后是否重跑由 ``Job(trigger=...)`` 在派发层裁决
          （默认 ``once`` 静默跳过，``on_failure`` 重跑），此处抢先过滤
          会吞掉重跑策略的豁免语义；
        - 已驻留队列的重复 uid 不排除——队列去重是 enqueue 自身的职责。
        """
        successes = self._store.backend.load_successes()
        return [j for j in jobs if j.uid not in successes]

    # ── 变更操作 ──────────────────────────────────────────────────────

    def clear_history(
        self,
        uid: str,
        *,
        records: Collection[Literal["successes", "failures", "attempts"]],
    ) -> int:
        """从指定档案和/或 Attempt 历史中删除单个 job 的记录。"""
        if not isinstance(uid, str) or not uid:
            raise TypeError(f"uid must be a non-empty str, got {uid!r}")
        try:
            parse_job_uid(uid)
        except Exception as exc:
            raise ValueError(
                f"uid must be a valid canonical 'task_type::job_id' str, got {uid!r}"
            ) from exc
        if not isinstance(records, (list, tuple, set, frozenset)):
            raise TypeError(
                f"records must be a collection of 'successes', 'failures', 'attempts', "
                f"got {type(records).__name__}"
            )
        records_set = set(records)
        if not records_set:
            raise ValueError("records must not be empty")
        allowed = {"successes", "failures", "attempts"}
        unknown = records_set - allowed
        if unknown:
            raise ValueError(
                f"records contains unknown target(s): {sorted(unknown)!r}; "
                f"allowed: {sorted(allowed)!r}"
            )
        return self._store.clear_history(uid, records_set)


__all__ = [
    "OpsConsole",
    "SuspendEntry",
]
