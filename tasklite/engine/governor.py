"""v2 死锁治理深模块：归因、依赖宽限与缺口升级。

统一内敛：
1. 依赖缺失宽限期管理（宽限截止时刻与缺失集快照）；
2. 死锁分类缺口连续轮次升级治理；
3. 细粒度死锁归因分析与批量事务熔断（环检测、缺失依赖、
   未知/超容资源、畸形条目）。

术语口径：死锁熔断的终态落点是失败档案（failed）——按 uid 唯一
终态、与 successes 全局互斥，由 store 的批量失败出口收敛。
"""
from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..exceptions import TaskLiteError
from ..models.job import Job
from ..models.state import PipelineState, job_uid_from_snapshot
from .errorclass import (
    ERR_DEADLOCK_GAP,
    ERR_DEPENDENCY_DEADLOCK,
    ERR_MALFORMED_JOB,
    ERR_RESOURCE_DEADLOCK,
)
from .scheduler import JobFacts, StandstillFacts
from .selection import SelectionStandstill

logger = logging.getLogger("tasklite")

DEP_GRACE_SECONDS: float = 3.0
DEADLOCK_GAP_MAX_ROUNDS = 5


class ScanCache(Protocol):
    """依赖宽限回退路径复用的调度投影缓存契约（JobScheduler 自动满足）。

    单趟扫描已反序列化的 Job 投影按内容键缓存复用——宽限判定逐行
    读 ``depends_on`` 时免二次全量反序列化。
    """

    def cached_job(self, job_snapshot: dict) -> JobFacts: ...


class BulkFailureOutcome(Protocol):
    """批量失败转移结果的鸭子形状（失败与级联 UID 列表）。"""

    @property
    def failed_uids(self) -> list[str]: ...

    @property
    def cascaded_uids(self) -> list[str]: ...


class ArbitrationStore(Protocol):
    """死锁仲裁所需的 store 契约。

    ``state`` 提供只读事实（queue / successes / 环检测）；
    ``apply_bulk_failure`` 是批量失败档案写入的单一出口（原子提交
    失败档案行、按 uid 删除队列行并级联下游，返回转移结果）。
    """

    @property
    def state(self) -> PipelineState: ...

    def apply_bulk_failure(
        self, uids_metas: Sequence[tuple[str, dict[str, Any]]]
    ) -> BulkFailureOutcome: ...


class SelectionStandstillSource(Protocol):
    """死锁仲裁消费的执行选择停摆事实源契约（SelectionMachine 自动满足）。"""

    @property
    def is_active(self) -> bool: ...

    def partition_standstill(self) -> SelectionStandstill | None: ...


@dataclass(frozen=True)
class DeadlockDecision:
    """死锁治理裁决结果（纯值对象，无阻塞副作用）。

    - action: "resolved" (已归因并移入失败档案) | "selection_blocked" (队列仅因执行选择排除而延迟)
      | "grace_waiting" (正在依赖宽限期中) | "gap_retrying" (分类缺口重试中) | "none" (未检测到死锁)
    - should_terminate: 是否应终止主循环（如全队列熔断完毕且队列清空，或选择排除干净收敛）
    - wait_time: 建议外层事件泵等待的时延秒数（宽限或重试时为 0.5s，否则 0.0s）
    - failed_uids: 本轮判定失败的 UID 列表
    - cascaded_uids: 本轮级联失败的 UID 列表
    - standstill: 选择停摆事实快照（仅在 action="selection_blocked" 时附带）
    """

    action: str
    should_terminate: bool
    wait_time: float = 0.0
    failed_uids: list[str] = field(default_factory=list)
    cascaded_uids: list[str] = field(default_factory=list)
    standstill: SelectionStandstill | None = None


@dataclass
class DeadlockGovernor:
    """统一死锁归因与缺口升级治理深模块。"""

    dependency_grace_seconds: float = DEP_GRACE_SECONDS
    deadlock_gap_max_rounds: int = DEADLOCK_GAP_MAX_ROUNDS
    deadlock_gap_rounds: int = 0

    def reset(self) -> None:
        """重置跨轮治理状态。"""
        self.deadlock_gap_rounds = 0

    def record_dispatch_progress(self) -> None:
        """派发前进信号重置缺口计数。"""
        self.deadlock_gap_rounds = 0

    def check_gap_or_escalate(
        self,
        log_prefix: str,
        *,
        max_rounds: int | None = None,
    ) -> bool:
        """死锁分类缺口的连续轮次升级逻辑。

        返回 True 表示已达上限需升级为全队列熔断；False 表示未达上限，等待下一轮重试。
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
            f"escalating to whole-queue failure archive ({ERR_DEADLOCK_GAP})."
        )
        return True

    @staticmethod
    def _split_deadlock_by_uids(
        queue: list[dict[str, Any]],
        target_uids: set[str] | None,
        error: str,
    ) -> list[tuple[str, dict[str, Any]]]:
        """从队列拆出「进失败档案的肇事者」（uid → 错误元数据）。

        ``target_uids=None`` 表示缺口升级的全队列兜底熔断（全部行标记
        root_cause）。队列剩余行的去留由 store 的批量失败出口按 uid
        精准删除收敛，本方法只负责归因装配。
        """
        return [
            (job_uid_from_snapshot(jd), {"error_message": error, "root_cause": True})
            for jd in queue
            if target_uids is None or job_uid_from_snapshot(jd) in target_uids
        ]

    def arbitrate(
        self,
        facts: StandstillFacts | None,
        store: ArbitrationStore,
        *,
        selection_machine: SelectionStandstillSource | None = None,
    ) -> DeadlockDecision:
        """自闭环死锁仲裁单一入口。

        评估停摆事实：
        0. 若传入 selection_machine 且激活，且无已知死锁成因，先做 Kahn 拓扑消减；
           若判定为「仅因 Exclude 阻塞」，闭环返回 action="selection_blocked"；
        1. 若 min_wait == inf，直接触发 resolve_deadlock 进行细粒度归因与批量熔断；
        2. 若 min_wait 为有限值，但处于 waiting_for_dependency 且存在拓扑成环
           （find_dependency_cycles），自动识别隐式死锁并触发 resolve_deadlock；
        3. 否则认定为正常等待或背压，返回 action="none"。
        """
        if facts is None:
            self.deadlock_gap_rounds = 0
            return DeadlockDecision(action="none", should_terminate=False, wait_time=0.0)

        selection_active = selection_machine is not None and selection_machine.is_active
        selection_standstill = (
            selection_machine.partition_standstill()
            if selection_active and selection_machine is not None
            else None
        )

        # 0. 执行选择先验判定：无明确死锁成因时先检查是否仅因 Exclude 阻塞
        if (
            selection_active
            and selection_machine is not None
            and not facts.attribution.has_deadlock_causes
        ):
            if (
                selection_standstill is not None
                and selection_standstill.only_blocked_by_exclusion
            ):
                self.deadlock_gap_rounds = 0
                return DeadlockDecision(
                    action="selection_blocked",
                    should_terminate=True,
                    wait_time=0.0,
                    standstill=selection_standstill,
                )

        if facts.attribution.has_deadlock_causes:
            return self._resolve_deadlock(
                facts,
                store=store,
                selection_machine=selection_machine,
                selection_standstill=selection_standstill,
                scheduler=None,
            )

        if facts.waiting_for_dependency and store.state is not None:
            cycle_uids: Sequence[str]
            if selection_active:
                cycle_uids = (
                    selection_standstill.cycle_uids
                    if selection_standstill is not None
                    else ()
                )
            else:
                cycle_uids = store.state.find_dependency_cycles()
            if cycle_uids or facts.min_wait == float("inf"):
                if cycle_uids:
                    logger.error(
                        f"Deadlock detected during wait: dependency cycle "
                        f"{sorted(set(cycle_uids))} masked by finite min_wait."
                    )
                return self._resolve_deadlock(
                    facts,
                    store=store,
                    selection_machine=selection_machine,
                    selection_standstill=selection_standstill,
                    scheduler=None,
                )


        # 3. 资源等待（包含有限时间等待与未知时长通知等待）：不视为死锁缺口
        if facts.has_resource_waiters:
            self.deadlock_gap_rounds = 0
            return DeadlockDecision(action="none", should_terminate=False, wait_time=0.0)

        # 4. 无成因且无任何资源等待的无限停摆 → 进入缺口重试或升级归因
        if facts.min_wait == float("inf"):
            return self._resolve_deadlock(
                facts,
                store=store,
                selection_machine=selection_machine,
                selection_standstill=selection_standstill,
                scheduler=None,
            )

        # 正常等待/背压结论（有限等待且无环）证明上一缺口 episode 已消解——
        # 缺口升级只统计连续轮次，非连续缺口从零计数
        self.deadlock_gap_rounds = 0
        return DeadlockDecision(action="none", should_terminate=False, wait_time=0.0)

    def resolve_deadlock(
        self,
        facts: StandstillFacts,
        store: ArbitrationStore,
        *,
        scheduler: ScanCache | None = None,
        selection_machine: SelectionStandstillSource | None = None,
    ) -> DeadlockDecision:
        """直接执行死锁归因，并在入口读取当前选择停摆事实。"""
        selection_standstill = (
            selection_machine.partition_standstill()
            if selection_machine is not None and selection_machine.is_active
            else None
        )
        return self._resolve_deadlock(
            facts,
            store=store,
            scheduler=scheduler,
            selection_machine=selection_machine,
            selection_standstill=selection_standstill,
        )

    def _resolve_deadlock(
        self,
        facts: StandstillFacts,
        store: ArbitrationStore,
        *,
        scheduler: ScanCache | None,
        selection_machine: SelectionStandstillSource | None,
        selection_standstill: SelectionStandstill | None,
    ) -> DeadlockDecision:
        """以同一停摆快照完成归因与状态提交。"""
        effective_state = store.state
        effective_grace = self.dependency_grace_seconds
        effective_gap_max = self.deadlock_gap_max_rounds
        selection_active = selection_machine is not None and selection_machine.is_active

        attr = facts.attribution
        malformed_uids = set(attr.malformed_uids)
        unknown_uids = set(attr.unknown_resource_uids)
        missing_uids = set(attr.missing_dependency_uids)
        impossible_uids = set(attr.impossible_resource_uids)

        if malformed_uids:
            logger.error(f"Deadlock: {len(malformed_uids)} job(s) have malformed dict (unparseable).")
            uids_metas = self._split_deadlock_by_uids(
                list(effective_state.queue), malformed_uids, ERR_MALFORMED_JOB
            )
        elif unknown_uids:
            logger.error(f"Deadlock: {len(unknown_uids)} job(s) reference unknown resource(s).")
            uids_metas = self._split_deadlock_by_uids(
                list(effective_state.queue), unknown_uids, ERR_RESOURCE_DEADLOCK
            )
        elif missing_uids:
            logger.error(f"Deadlock: {len(missing_uids)} job(s) have unresolvable (missing) dependencies.")
            uids_metas = self._split_deadlock_by_uids(
                list(effective_state.queue), missing_uids, ERR_DEPENDENCY_DEADLOCK
            )
        elif impossible_uids:
            logger.error(f"Deadlock: {len(impossible_uids)} job(s) request impossible resource amounts (exceeds capacity).")
            uids_metas = self._split_deadlock_by_uids(
                list(effective_state.queue), impossible_uids, ERR_RESOURCE_DEADLOCK
            )
        elif facts.waiting_for_dependency:
            if selection_active:
                cycle_uids = set(
                    selection_standstill.cycle_uids
                    if selection_standstill is not None
                    else ()
                )
            else:
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
                uids_metas = self._split_deadlock_by_uids(
                    list(effective_state.queue), None, ERR_DEADLOCK_GAP
                )
            else:
                logger.error(
                    f"Deadlock detected: dependency cycle among {len(cycle_uids)} job(s): "
                    f"{sorted(cycle_uids)}"
                )
                uids_metas = self._split_deadlock_by_uids(
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
            uids_metas = self._split_deadlock_by_uids(
                list(effective_state.queue), None, ERR_DEADLOCK_GAP
            )

        # 提交批量死锁失败（失败档案单一出口）
        outcome = store.apply_bulk_failure(uids_metas)
        self.deadlock_gap_rounds = 0

        # resolved 但零归档时的选择收敛闭环（如归档全部被选择过滤）
        if (
            not outcome.failed_uids
            and selection_machine is not None
            and selection_machine.is_active
        ):
            # 批量写入可能改变队列；归档为零时再基于当前队列收敛。
            standstill = selection_machine.partition_standstill()
            if standstill is not None and standstill.only_blocked_by_exclusion:
                return DeadlockDecision(
                    action="selection_blocked",
                    should_terminate=True,
                    wait_time=0.0,
                    standstill=standstill,
                )
            if standstill is not None and standstill.stuck_uids:
                logger.error(
                    "Deadlock arbitration resolved without archiving while allowed "
                    f"jobs remain stuck ({list(standstill.stuck_uids)}); attributing "
                    f"them to {ERR_DEADLOCK_GAP} to guarantee queue reduction."
                )
                fallback_outcome = store.apply_bulk_failure(
                    [
                        (uid, {"error_message": ERR_DEADLOCK_GAP, "root_cause": True})
                        for uid in standstill.stuck_uids
                    ]
                )
                if fallback_outcome.failed_uids:
                    return DeadlockDecision(
                        action="resolved",
                        should_terminate=len(effective_state.queue) == 0,
                        wait_time=0.0,
                        failed_uids=fallback_outcome.failed_uids,
                        cascaded_uids=fallback_outcome.cascaded_uids,
                    )
            logger.critical(
                "Deadlock arbitration reported 'resolved' without failing any job; "
                "terminating the run instead of spinning (queue preserved on disk)."
            )
            return DeadlockDecision(
                action="resolved",
                should_terminate=True,
                wait_time=0.0,
            )

        should_terminate = len(outcome.failed_uids) > 0 and len(effective_state.queue) == 0
        return DeadlockDecision(
            action="resolved",
            should_terminate=should_terminate,
            wait_time=0.0,
            failed_uids=outcome.failed_uids,
            cascaded_uids=outcome.cascaded_uids,
        )


__all__ = [
    "ArbitrationStore",
    "BulkFailureOutcome",
    "DEADLOCK_GAP_MAX_ROUNDS",
    "DEP_GRACE_SECONDS",
    "DeadlockDecision",
    "DeadlockGovernor",
    "ScanCache",
    "SelectionStandstill",
]
