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
from typing import Any, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from .store import StateStore
    from ..backend.base import AbstractStateBackend

from ..models.job import Job
from ..models.state import PipelineState, uid_from_job_dict
from .scheduler import StandstillFacts
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
    - failed_uids: list[str] (本轮判定失败的 UID 列表)
    - cascaded_uids: list[str] (本轮级联失败的 UID 列表)
    """

    action: str
    should_terminate: bool
    wait_time: float = 0.0
    failed_uids: list[str] = field(default_factory=list)
    cascaded_uids: list[str] = field(default_factory=list)


@dataclass
class DeadlockGovernor:
    """统一死锁归因、依赖宽限与缺口升级治理深模块。"""

    dep_grace_seconds: float = DEP_GRACE_SECONDS
    deadlock_gap_max_rounds: int = DEADLOCK_GAP_MAX_ROUNDS
    dep_grace_deadline: float | None = None
    dep_grace_missing: frozenset[str] | None = None
    deadlock_gap_rounds: int = 0

    def reset(self) -> None:
        """重置跨轮治理状态。"""
        self.dep_grace_deadline = None
        self.dep_grace_missing = None
        self.deadlock_gap_rounds = 0

    def _end_grace_episode(self) -> None:
        """终结当前宽限 episode（deadline 与缺失集快照一并清除）。

        不变式：过期/残留 deadline 严禁泄漏进后续 episode——否则同缺失依赖
        （同 uid 集合，如 retry 原样 requeue）的新等待者继承过期 deadline
        被零宽限立即误判死锁 DLQ，即使面对全新 spawner 也无等待机会。
        """
        self.dep_grace_deadline = None
        self.dep_grace_missing = None

    def note_dispatch_progress(self) -> None:
        """派发前进信号终结当前宽限 episode（消解侧唯一终结点）。

        不变式：任何成功派发都证明此前的等待者已消解，残留 deadline 严禁
        泄漏进同缺失集合的后续 episode——复发落在 (deadline, deadline+宽限窗]
        内时时间启发式无法区分残留与活跃 episode，零宽限批量误杀只能靠
        消解侧终结点消除。缺口升级计数同理随前进清零（缺口升级只统计
        连续轮次，见 check_gap_or_escalate）。
        """
        if self.dep_grace_deadline is not None:
            self._end_grace_episode()
        self.deadlock_gap_rounds = 0

    def check_dependency_grace(
        self,
        state: PipelineState,
        missing_identifiers: Sequence[int | str] | set[str] | Sequence[str],
        *,
        has_potential_spawners: bool | None = None,
        scheduler: Any | None = None,
        now: float | None = None,
        grace_seconds: float | None = None,
    ) -> bool:
        """评估缺失依赖的作业是否应授予宽限期（等待潜在 spawner 产出而非立即 DLQ）。

        返回 True 表示正在宽限中（主循环应继续等待）；False 表示无候选或宽限已超时。
        （纯逻辑计算，不产生 sleep 副作用）。
        """
        missing_uids: set[str] = set()
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

        # 不变式：存活 deadline 一旦过期超过一个完整宽限窗，必为轮询中断后
        # 的残留（等待者已消解、仲裁停摆，episode 无裁决出口终结）——严禁
        # 复用，按新 episode 重新计账，防同 uid 集合复发时零宽限批量误杀。
        if (
            self.dep_grace_deadline is not None
            and now_mono - self.dep_grace_deadline > effective_grace
        ):
            self._end_grace_episode()

        # 若调度器已单趟给出潜在 spawner 裁决，直接复用事实（避免二次扫描队列及重复反序列化）
        if has_potential_spawners is not None:
            if not has_potential_spawners:
                self._end_grace_episode()
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
            self._end_grace_episode()
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
                self._end_grace_episode()
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
        max_rounds: int | None = None,
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

    @staticmethod
    def _split_deadlock_by_uids(
        queue: list[dict[str, Any]],
        target_uids: set[str],
        error: str,
    ) -> tuple[list[tuple[str, dict[str, Any]]], list[dict[str, Any]]]:
        """把队列拆分为「进 DLQ 的肇事者」与「保留的剩余队列」。"""
        uids_metas: list[tuple[str, dict[str, Any]]] = []
        remaining_queue: list[dict[str, Any]] = []
        for jd in queue:
            uid = uid_from_job_dict(jd)
            if uid in target_uids:
                uids_metas.append((uid, {"error": error, "root_cause": True}))
            else:
                remaining_queue.append(jd)
        return uids_metas, remaining_queue

    def arbitrate(
        self,
        facts: StandstillFacts | None,
        store: "StateStore",
    ) -> DeadlockDecision:
        """自闭环死锁仲裁单一入口。

        评估停摆事实：
        1. 若 min_wait == inf，直接触发 resolve_deadlock 进行细粒度归因与批量熔断；
        2. 若 min_wait 为有限值，但处于 waiting_for_dependency 且存在拓扑成环（find_dependency_cycles），
           自动识别隐式死锁并触发 resolve_deadlock；
        3. 否则认定为正常等待或背压，返回 action="none"。
        """
        if facts is None:
            self.deadlock_gap_rounds = 0
            return DeadlockDecision(action="none", should_terminate=False, wait_time=0.0)

        if facts.min_wait == float("inf"):
            return self.resolve_deadlock(facts, store=store)

        if facts.waiting_for_dependency and store.state is not None:
            cycle_uids = store.state.find_dependency_cycles()
            if cycle_uids:
                logger.error(
                    f"Deadlock detected during backoff/wait: dependency cycle "
                    f"{sorted(set(cycle_uids))} masked by finite min_wait."
                )
                return self.resolve_deadlock(facts, store=store)

        # 正常等待/背压结论（有限等待且无环）证明上一缺口 episode 已消解——
        # 缺口升级只统计连续轮次，非连续缺口从零计数
        self.deadlock_gap_rounds = 0
        return DeadlockDecision(action="none", should_terminate=False, wait_time=0.0)

    def resolve_deadlock(
        self,
        facts: StandstillFacts,
        store: "StateStore",
        *,
        scheduler: Any | None = None,
    ) -> DeadlockDecision:
        """处理死锁：细粒度归因 + bulk_failure + cascade（纯计算求值，无阻塞副作用）。"""
        effective_state = store.state
        effective_grace = self.dep_grace_seconds
        effective_gap_max = self.deadlock_gap_max_rounds

        attr = facts.attribution
        malformed_uids = set(attr.malformed_uids)
        unknown_uids = set(attr.unknown_resource_uids)
        missing_uids = set(attr.missing_dependency_uids)
        impossible_uids = set(attr.impossible_resource_uids)

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
            has_spawners = facts.has_potential_spawners
            if self.check_dependency_grace(
                effective_state,
                missing_uids,
                has_potential_spawners=has_spawners,
                scheduler=scheduler,
                grace_seconds=effective_grace,
            ):
                # 可归因的宽限等待（非缺口结论）终结上一缺口 episode
                self.deadlock_gap_rounds = 0
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
        elif facts.waiting_for_dependency:
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

__all__ = [
    "DeadlockDecision",
    "DeadlockGovernor",
    "DEP_GRACE_SECONDS",
    "DEADLOCK_GAP_MAX_ROUNDS",
]
