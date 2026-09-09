"""DeadlockGovernor: 死锁归因、依赖宽限与缺口升级治理深模块。

统一内敛：
1. 依赖缺失宽限期管理（Dependency Grace Deadline & Missing Sets）；
2. 死锁分类缺口连续轮次升级治理（Deadlock Gap Escalation Rounds）；
3. 细粒度死锁归因分析与批量事务熔断（Cycle Detection, Missing Dep, Unknown/Impossible Resource）。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import (
    Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Sequence, Set, Tuple, Union, TYPE_CHECKING
)

if TYPE_CHECKING:
    from .store import StateStore
    from ..backend.base import AbstractStateBackend

from ..models.job import Job
from ..models.state import PipelineState, uid_from_job_dict
from ..taxonomy import (
    ERR_DEADLOCK_GAP,
    ERR_DEPENDENCY_DEADLOCK,
    ERR_JOB_DEPENDENCY,
    ERR_MALFORMED_JOB,
    ERR_RESOURCE_DEADLOCK,
    ErrorTaxonomy,
    _DEFAULT_TAXONOMY,
)

logger = logging.getLogger("tasklite")

DEP_GRACE_SECONDS = 60.0
DEADLOCK_GAP_MAX_ROUNDS = 5


@dataclass(frozen=True)
class DeadlockDecision:
    """死锁治理裁决结果（纯值对象，无阻塞副作用）。

    - action: "resolved" (已归因并移入 DLQ) | "grace_waiting" (正在依赖宽限期中) | "gap_retrying" (分类缺口重试中) | "none" (未检测到死锁)
    - should_terminate: bool (是否应终止主循环，如全队列 DLQ 完毕且队列清空)
    - wait_time: float (建议外层事件泵等待的时延秒数，如宽限或重试时为 0.5s，否则为 0.0s)
    - failed_uids: List[str] (本轮判定失败的 UID 列表)
    - cascaded_uids: List[str] (本轮级联失败的 UID 列表)
    """

    action: str
    should_terminate: bool
    wait_time: float = 0.0
    failed_uids: List[str] = field(default_factory=list)
    cascaded_uids: List[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        """保持向后兼容布尔求值 (bool(decision) == should_terminate)。"""
        return self.should_terminate


@dataclass
class DeadlockGovernor:
    """统一死锁归因、依赖宽限与缺口升级治理深模块。"""

    dep_grace_seconds: float = DEP_GRACE_SECONDS
    deadlock_gap_max_rounds: int = DEADLOCK_GAP_MAX_ROUNDS
    dep_grace_deadline: Optional[float] = None
    dep_grace_missing: Optional[FrozenSet[str]] = None
    deadlock_gap_rounds: int = 0

    def reset(self) -> None:
        """重置跨轮治理状态。"""
        self.dep_grace_deadline = None
        self.dep_grace_missing = None
        self.deadlock_gap_rounds = 0

    def check_dependency_grace(
        self,
        state: PipelineState,
        missing_identifiers: Union[Sequence[Union[int, str]], Set[str], Sequence[str]],
        *,
        has_potential_spawners: Optional[bool] = None,
        scheduler: Optional[Any] = None,
        now: Optional[float] = None,
        grace_seconds: Optional[float] = None,
    ) -> bool:
        """评估缺失依赖的作业是否应授予宽限期（等待潜在 spawner 产出而非立即 DLQ）。

        返回 True 表示正在宽限中（主循环应继续等待）；False 表示无候选或宽限已超时。
        （纯逻辑计算，不产生 sleep 副作用）。
        """
        missing_uids: Set[str] = set()
        for item in missing_identifiers:
            if isinstance(item, str):
                missing_uids.add(item)
            elif isinstance(item, int) and 0 <= item < len(state.queue):
                try:
                    missing_uids.add(uid_from_job_dict(state.queue[item]))
                except (KeyError, TypeError, ValueError):
                    pass

        if not missing_uids:
            return False

        effective_grace = (
            float(grace_seconds) if grace_seconds is not None else self.dep_grace_seconds
        )

        # 若缺失 UID 集合发生变化，重置宽限截止时间（新 episode）
        if self.dep_grace_missing is not None and self.dep_grace_missing != missing_uids:
            self.dep_grace_deadline = None
        self.dep_grace_missing = frozenset(missing_uids)

        now_mono = time.monotonic() if now is None else now

        # 若调度器已单趟给出潜在 spawner 裁决，直接复用事实（避免二次扫描队列及重复反序列化）
        if has_potential_spawners is not None:
            if not has_potential_spawners:
                return False
            deadline = self.dep_grace_deadline
            if deadline is None:
                deadline = now_mono + effective_grace
                self.dep_grace_deadline = deadline
                logger.warning(
                    f"DEPENDENCY GRACE: {len(missing_uids)} job(s) waiting "
                    f"on missing deps; granting {effective_grace}s "
                    f"grace (runnable job(s) may spawn them)."
                )
            if now_mono < deadline:
                return True
            logger.error(
                f"DEPENDENCY GRACE EXPIRED: {len(missing_uids)} job(s) "
                f"still waiting on missing deps after "
                f"{effective_grace}s; treating as deadlock (DLQ)."
            )
            return False

        # 兼容回退：检查队列中是否存在「全部已知依赖已在 wall 中」的潜在可运行作业
        for jd in state.queue:
            uid = uid_from_job_dict(jd)
            if uid in missing_uids:
                continue
            try:
                if scheduler is not None and hasattr(scheduler, "cached_job"):
                    job = scheduler.cached_job(jd)
                else:
                    job = Job.from_dict(jd)
            except (KeyError, TypeError, ValueError):
                continue
            if all(dep in state.wall for dep in job.depends_on):
                deadline = self.dep_grace_deadline
                if deadline is None:
                    deadline = now_mono + effective_grace
                    self.dep_grace_deadline = deadline
                    logger.warning(
                        f"DEPENDENCY GRACE: {len(missing_uids)} job(s) waiting "
                        f"on missing deps; granting {effective_grace}s "
                        f"grace (runnable job(s) may spawn them)."
                    )
                if now_mono < deadline:
                    return True
                logger.error(
                    f"DEPENDENCY GRACE EXPIRED: {len(missing_uids)} job(s) "
                    f"still waiting on missing deps after "
                    f"{effective_grace}s; treating as deadlock (DLQ)."
                )
                return False
        return False

    def check_gap_or_escalate(
        self,
        log_prefix: str,
        *,
        max_rounds: Optional[int] = None,
    ) -> bool:
        """死锁分类缺口的连续轮次升级逻辑。

        返回 True 表示已达上限需升级为全队列 DLQ；False 表示未达上限，等待下一轮重试。
        （纯计数逻辑，不产生 sleep 副作用）。
        """
        effective_max = (
            int(max_rounds) if max_rounds is not None else self.deadlock_gap_max_rounds
        )
        self.deadlock_gap_rounds += 1
        if self.deadlock_gap_rounds < effective_max:
            logger.error(
                f"{log_prefix}: refusing to fail the whole queue, retrying next round "
                f"({self.deadlock_gap_rounds}/{effective_max})."
            )
            return False
        logger.critical(
            f"{log_prefix} persisted for {effective_max} rounds; "
            f"escalating to whole-queue DLQ ({ERR_DEADLOCK_GAP})."
        )
        return True

    def _extract_deadlock_uids(
        self,
        sched: Any,
        field_name: str,
    ) -> Set[str]:
        """统一提取归因 UID 集合。"""
        attr = getattr(sched, "attribution", None) or getattr(sched, "deadlock_attribution", None)
        if attr is not None and hasattr(attr, field_name):
            uids = getattr(attr, field_name)
            if uids:
                return set(uids)
        if hasattr(sched, field_name):
            uids = getattr(sched, field_name)
            if uids:
                return set(uids)
        return set()

    @staticmethod
    def _split_deadlock_by_uids(
        queue: List[Dict[str, Any]],
        target_uids: Set[str],
        error: str,
    ) -> Tuple[List[Tuple[str, Dict[str, Any]]], List[Dict[str, Any]]]:
        """把队列拆分为「进 DLQ 的肇事者」与「保留的剩余队列」。"""
        uids_metas: List[Tuple[str, Dict[str, Any]]] = []
        remaining_queue: List[Dict[str, Any]] = []
        for jd in queue:
            uid = uid_from_job_dict(jd)
            if uid in target_uids:
                uids_metas.append((uid, {"error": error, "root_cause": True}))
            else:
                remaining_queue.append(jd)
        return uids_metas, remaining_queue

    def resolve_deadlock(
        self,
        sched: Any,
        store: "StateStore",
        *,
        state: Optional[PipelineState] = None,
        scheduler: Optional[Any] = None,
        ctx: Optional[Any] = None,
        dep_grace_seconds: Optional[float] = None,
        deadlock_gap_max_rounds: Optional[int] = None,
    ) -> DeadlockDecision:
        """处理死锁：细粒度归因 + bulk_failure + cascade（纯计算求值，无阻塞副作用）。"""
        effective_state = state or store.state
        effective_grace = dep_grace_seconds if dep_grace_seconds is not None else self.dep_grace_seconds
        effective_gap_max = deadlock_gap_max_rounds if deadlock_gap_max_rounds is not None else self.deadlock_gap_max_rounds

        malformed_uids = self._extract_deadlock_uids(sched, "malformed_uids")
        unknown_uids = self._extract_deadlock_uids(sched, "unknown_resource_uids")
        missing_uids = self._extract_deadlock_uids(sched, "missing_dependency_uids")
        impossible_uids = self._extract_deadlock_uids(sched, "impossible_resource_uids")

        if malformed_uids:
            logger.error(f"Deadlock: {len(malformed_uids)} job(s) have malformed dict (unparseable).")
            uids_metas, remaining_queue = self._split_deadlock_by_uids(
                list(effective_state.queue), malformed_uids, ERR_MALFORMED_JOB
            )
        elif unknown_uids:
            logger.error(f"Deadlock: {len(unknown_uids)} job(s) reference unknown resource(s).")
            uids_metas, remaining_queue = self._split_deadlock_by_uids(
                list(effective_state.queue), unknown_uids, ERR_RESOURCE_DEADLOCK
            )
        elif missing_uids:
            has_spawners = getattr(sched, "has_potential_spawners", None)
            if self.check_dependency_grace(
                effective_state,
                missing_uids,
                has_potential_spawners=has_spawners,
                scheduler=scheduler,
                grace_seconds=effective_grace,
            ):
                return DeadlockDecision(
                    action="grace_waiting",
                    should_terminate=False,
                    wait_time=0.5,
                )
            logger.error(f"Deadlock: {len(missing_uids)} job(s) have unresolvable (missing) dependencies.")
            uids_metas, remaining_queue = self._split_deadlock_by_uids(
                list(effective_state.queue), missing_uids, ERR_DEPENDENCY_DEADLOCK
            )
        elif impossible_uids:
            logger.error(f"Deadlock: {len(impossible_uids)} job(s) request impossible resource amounts (exceeds capacity).")
            uids_metas, remaining_queue = self._split_deadlock_by_uids(
                list(effective_state.queue), impossible_uids, ERR_RESOURCE_DEADLOCK
            )
        elif getattr(sched, "waiting_for_dependency", False):
            cycle_uids = set(effective_state.find_dependency_cycles())
            if not cycle_uids:
                escalated = self.check_gap_or_escalate(
                    "Deadlock classification gap (waiting_for_dependency without cycle)",
                    max_rounds=effective_gap_max,
                )
                if not escalated:
                    return DeadlockDecision(
                        action="gap_retrying",
                        should_terminate=False,
                        wait_time=0.5,
                    )
                uids_metas = [
                    (uid_from_job_dict(jd),
                     {"error": ERR_DEADLOCK_GAP, "root_cause": True})
                    for jd in effective_state.queue
                ]
                remaining_queue = []
            else:
                logger.error(
                    f"Deadlock detected: dependency cycle among {len(cycle_uids)} job(s): "
                    f"{sorted(cycle_uids)}"
                )
                uids_metas, remaining_queue = self._split_deadlock_by_uids(
                    list(effective_state.queue), cycle_uids, ERR_DEPENDENCY_DEADLOCK
                )
        else:
            escalated = self.check_gap_or_escalate(
                "Deadlock: unclassifiable deadlock (no known root cause)",
                max_rounds=effective_gap_max,
            )
            if not escalated:
                return DeadlockDecision(
                    action="gap_retrying",
                    should_terminate=False,
                    wait_time=0.5,
                )
            uids_metas = [
                (uid_from_job_dict(jd),
                 {"error": ERR_DEADLOCK_GAP, "root_cause": True})
                for jd in effective_state.queue
            ]
            remaining_queue = []

        # 提交批量死锁失败
        outcome = store.apply_bulk_failure(uids_metas, remaining_queue=remaining_queue)
        self.deadlock_gap_rounds = 0
        should_terminate = len(outcome.failed_uids) > 0 and len(effective_state.queue) == 0
        return DeadlockDecision(
            action="resolved",
            should_terminate=should_terminate,
            wait_time=0.0,
            failed_uids=outcome.failed_uids,
            cascaded_uids=outcome.cascaded_uids,
        )


# 向后兼容别名
EpisodeState = DeadlockGovernor

__all__ = [
    "DeadlockDecision",
    "DeadlockGovernor",
    "EpisodeState",
    "DEP_GRACE_SECONDS",
    "DEADLOCK_GAP_MAX_ROUNDS",
]
