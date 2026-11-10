"""v2 运行排除拓扑机器（SelectionMachine）：Exclude 与临时排除的停摆剥离。

深模块契约：顶层 tasklite/selection.py 仅保留 Exclude 值对象；本模块负责
Kahn 排除停摆投影，依赖环检测复用 PipelineState 的统一实现。

单向依赖约束：本模块仅依赖 Exclude 与 StateStore，避免反向耦合 JobScheduler。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..exceptions import TaskLiteError
from ..models.job import Job
from ..selection import Exclude

if TYPE_CHECKING:
    from .store import StateStore

logger = logging.getLogger("tasklite")


@dataclass(frozen=True)
class SelectionStandstill:
    """排除规则与临时排除下的停摆拓扑判定（Kahn 入度消减结果）。

    以 ``deferred_uids``（Exclude 命中行 + 运行期临时排除行）为延迟波前，
    对剩余「允许执行」作业做入度消减：

    - ``stuck_uids``：入度无法消减为 0 的允许作业，可能包含环成员、缺口
      及其下游；
    - ``cycle_uids``：仅从 stuck 子图中由 PipelineState 的依赖环检测器识别
      的真实环成员，供 governor 沿用既有环归因；
    - ``blocked_uids``：入度可消减、且传递依赖最终落在 deferred 波前上的
      允许作业——本次仅因上游被排除而无法完成，必须留队等待下次 run；
    - ``unblocked_uids``：入度可消减但仍有余量（如资源等待）的允许作业，
      存在本 run 继续推进的可能，不得据此终止运行。

    结论 ``only_blocked_by_exclusion`` 为真当且仅当没有任何允许作业可能
    在本 run 完成（无 stuck、无 unblocked）：此时本 run 干净终止，队列保留。
    """

    excluded_uids: tuple[str, ...] = ()
    blocked_uids: tuple[str, ...] = ()
    unblocked_uids: tuple[str, ...] = ()
    stuck_uids: tuple[str, ...] = ()
    cycle_uids: tuple[str, ...] = ()
    blocked_by: tuple[tuple[str, str], ...] = ()

    @property
    def only_blocked_by_exclusion(self) -> bool:
        """剩余队列是否全部为「排除 + 仅被其阻塞」的作业。"""
        return not self.stuck_uids and not self.unblocked_uids


class SelectionMachine:
    """排除机器：统筹 Exclude 规则与运行期临时排除的停摆判定。"""

    def __init__(
        self,
        store: StateStore,
        *,
        exclude: Exclude | None = None,
    ):
        self._store = store
        self._exclude: Exclude | None = exclude
        self._temporary_excluded_uids: set[str] = set()

    @property
    def is_active(self) -> bool:
        return self._exclude is not None or bool(self._temporary_excluded_uids)

    def set_exclusions(
        self,
        exclude: Exclude | None,
        temporary_uids: set[str] | None = None,
    ) -> None:
        self._exclude = exclude
        if temporary_uids is not None:
            self._temporary_excluded_uids = set(temporary_uids)

    def temporarily_exclude(self, job_uid: str) -> None:
        """内部排除入口：登记单个临时排除 UID（供授权拒绝等复用）。"""
        self._temporary_excluded_uids.add(job_uid)

    def is_excluded(self, job_uid: str, task_type: str) -> bool:
        """判断作业是否被排除（用户 exclude 或运行期临时排除）。"""
        if job_uid in self._temporary_excluded_uids:
            return True
        if self._exclude is not None and self._exclude.matches_task_type(task_type):
            return True
        return False

    def reset_run(self, exclude: Exclude | None = None) -> None:
        """每 run 开始时复位运行期状态。"""
        self._exclude = exclude
        self._temporary_excluded_uids.clear()

    def partition_standstill(self) -> SelectionStandstill | None:
        """以 Kahn 入度消减剥离选择停摆：区分真死锁与「仅被排除阻塞」。

        返回 None 表示队列存在畸形行（无可信拓扑事实），交既有
        配置无效归因。纯计算、无状态变更。
        """
        if not self.is_active:
            return None
        state = self._store.state
        queue = state.queue
        if not queue:
            return SelectionStandstill()
        facts: dict[str, tuple[str, tuple[str, ...]]] = {}
        for job_snapshot in queue:
            try:
                parsed = Job.from_dict(job_snapshot)
                uid, task_type, depends_on = (
                    parsed.uid,
                    parsed.task_type,
                    tuple(parsed.depends_on),
                )
            except (KeyError, TypeError, ValueError, TaskLiteError):
                return None
            facts[uid] = (task_type, tuple(depends_on))

        deferred = {
            uid for uid, (task_type, _deps) in facts.items()
            if self.is_excluded(uid, task_type)
        }
        excluded = set(deferred)
        allowed = set(facts) - deferred

        # Kahn：只有允许作业参与消减；deferred 与 successes 同为已满足波前。
        indegree: dict[str, int] = {}
        dependents: dict[str, list[str]] = {}
        for uid in allowed:
            count = 0
            for dep in facts[uid][1]:
                if dep in deferred or state.dependency_ready(dep):
                    continue
                count += 1
                dependents.setdefault(dep, []).append(uid)
            indegree[uid] = count
        ready = [uid for uid, count in indegree.items() if count == 0]
        reduced: set[str] = set()
        while ready:
            uid = ready.pop()
            reduced.add(uid)
            for child in dependents.get(uid, ()):
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
        stuck = allowed - reduced
        cycle_uids = (
            state.find_dependency_cycles(node_uids=stuck) if stuck else []
        )

        # 仅因排除受阻：从 deferred 沿依赖边反向闭包，再与可消减集求交。
        children: dict[str, list[str]] = {}
        for uid in allowed:
            for dep in facts[uid][1]:
                children.setdefault(dep, []).append(uid)
        root_of: dict[str, str] = {}
        blocked: set[str] = set()
        stack: list[str] = list(deferred)
        while stack:
            cur = stack.pop()
            for child in children.get(cur, ()):
                if child in deferred or child in blocked:
                    continue
                blocked.add(child)
                root_of.setdefault(child, root_of.get(cur, cur))
                stack.append(child)
        blocked &= reduced
        return SelectionStandstill(
            excluded_uids=tuple(sorted(excluded)),
            blocked_uids=tuple(sorted(blocked)),
            unblocked_uids=tuple(sorted(reduced - blocked)),
            stuck_uids=tuple(sorted(stuck)),
            cycle_uids=tuple(cycle_uids),
            blocked_by=tuple(
                sorted((uid, root_of.get(uid, "")) for uid in blocked)
            ),
        )


__all__ = ["SelectionMachine", "SelectionStandstill"]
