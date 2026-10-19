"""v2 执行选择机器（SelectionMachine）：统筹 Skip 闭包驱逐与 Exclude 停摆剥离。

深模块契约：顶层 tasklite/selection.py 仅保留声明式规则与协议；本模块负责
Skip 闭包与 Kahn 选择停摆投影，依赖环检测复用 PipelineState 的统一实现。

单向依赖约束：本模块仅依赖 Selection（规则集）与 StateStore（状态仓），
避免反向耦合 JobScheduler。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from ..models.job import Job
from ..selection import Selection
from .errorclass import ERR_DEPENDENCY_SKIPPED, ERR_SKIPPED

if TYPE_CHECKING:
    from .store import StateStore

logger = logging.getLogger("tasklite")


@dataclass(frozen=True)
class SelectionStandstill:
    """执行选择下的停摆拓扑判定（Kahn 入度消减结果）。

    以 ``wall_uids | deferred_uids``（Exclude 命中行 + 防御性的 Skip 残留行）
    为初始波前，对剩余「允许执行」作业做入度消减：

    - ``stuck_uids``：入度无法消减为 0 的允许作业，可能包含环成员、缺口
      及其下游；
    - ``cycle_uids``：仅从 stuck 子图中由 PipelineState 的依赖环检测器识别
      的真实环成员，供 governor 沿用既有环归因；
    - ``blocked_uids``：入度可消减、且传递依赖最终落在 deferred 波前上的
      允许作业——本次仅因上游被排除/跳过而无法完成，必须留队等待下次 run；
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
        """剩余队列是否全部为「排除/跳过 + 仅被其阻塞」的作业。"""
        return not self.stuck_uids and not self.unblocked_uids


class SelectionMachine:
    """执行选择深模块：统筹 Skip 闭包驱逐与 Exclude 停摆剥离。"""

    def __init__(self, selection: Selection, store: StateStore):
        self._selection = selection
        self._store = store
        self._skipped_uids: set[str] = set()

    @property
    def is_active(self) -> bool:
        return self._selection.is_active

    @property
    def has_skip_rules(self) -> bool:
        return self._selection.has_skip_rules

    def reset_run(self) -> None:
        """每 run 开始时复位运行期状态。"""
        self._skipped_uids.clear()

    def apply_skips(self, *, run_id: str | None, seq_supplier: Callable[[], int]) -> int:
        """终态跳过：Skip 命中行移出 queue，下游整链级联跳过并留审计轨迹。

        Skip 是永久语义（不进入 wall/失败档案，下次不再重试），审计落
        attempts（outcome=skipped）。删除序为下游优先：崩溃中断后 Skip 根
        仍在队列，下次 run 可重新得到同一闭包（幂等）。
        """
        if not self._selection.has_skip_rules:
            return 0
        store = self._store
        queue = store.state.queue
        if not queue:
            return 0
        rows: dict[str, dict] = {}
        facts: dict[str, tuple[str, tuple[str, ...]]] = {}
        for job_dict in queue:
            try:
                parsed = Job.from_dict(job_dict)
            except (KeyError, TypeError, ValueError):
                continue  # 畸形行走既有 MALFORMED_JOB 归因
            rows[parsed.uid] = job_dict
            facts[parsed.uid] = (parsed.task_type, tuple(parsed.depends_on))
        if not facts:
            return 0
        dependents: dict[str, list[str]] = {}
        for uid, (_task_type, depends_on) in facts.items():
            for dep in depends_on:
                dependents.setdefault(dep, []).append(uid)
        seeds = {
            uid
            for uid, (task_type, depends_on) in facts.items()
            if self._selection.is_skipped(task_type)
            or any(dep in self._skipped_uids for dep in depends_on)
        }
        if not seeds:
            return 0
        closure = set(seeds)
        depth: dict[str, int] = {uid: 0 for uid in seeds}
        root_of: dict[str, str] = {uid: uid for uid in seeds}
        stack = list(seeds)
        while stack:
            cur = stack.pop()
            for child in dependents.get(cur, ()):
                if child in closure:
                    continue
                closure.add(child)
                depth[child] = depth[cur] + 1
                root_of[child] = root_of.get(cur, cur)
                stack.append(child)
        skipped = 0
        for uid in sorted(closure, key=lambda item: (-depth.get(item, 0), item)):
            queued_row = rows.get(uid)
            if queued_row is None:
                continue
            try:
                job = Job.from_dict(queued_row)
            except (KeyError, TypeError, ValueError):
                continue
            if uid in seeds and self._selection.is_skipped(job.task_type):
                error = ERR_SKIPPED
            else:
                error = (
                    f"{ERR_DEPENDENCY_SKIPPED} "
                    f"(upstream: {root_of.get(uid, '?')})"
                )
            store.apply_skip(uid, job_dict=queued_row)
            self._skipped_uids.add(uid)
            store.record_attempt_skip(
                job,
                error,
                run_id=run_id,
                seq=seq_supplier(),
            )
            skipped += 1
            logger.info(f"SELECTION: job {uid} skipped terminal ({error}).")
        return skipped

    def partition_standstill(self) -> SelectionStandstill | None:
        """以 Kahn 入度消减剥离选择停摆：区分真死锁与「仅被排除阻塞」。

        返回 None 表示队列存在畸形行（无可信拓扑事实），交既有
        MALFORMED_JOB 归因。纯计算、无状态变更。
        """
        if not self._selection.is_active:
            return None
        state = self._store.state
        queue = state.queue
        if not queue:
            return SelectionStandstill()
        facts: dict[str, tuple[str, tuple[str, ...]]] = {}
        for job_dict in queue:
            try:
                parsed = Job.from_dict(job_dict)
                uid, task_type, depends_on = (
                    parsed.uid,
                    parsed.task_type,
                    tuple(parsed.depends_on),
                )
            except (KeyError, TypeError, ValueError):
                return None
            facts[uid] = (task_type, tuple(depends_on))

        deferred = {
            uid for uid, (task_type, _deps) in facts.items()
            if not self._selection.allows_type(task_type)
        }
        excluded = {
            uid for uid, (task_type, _deps) in facts.items()
            if self._selection.is_excluded(task_type)
        }
        allowed = set(facts) - deferred

        # Kahn：只有允许作业参与消减；deferred 与 wall 同为已满足波前。
        indegree: dict[str, int] = {}
        dependents: dict[str, list[str]] = {}
        for uid in allowed:
            count = 0
            for dep in facts[uid][1]:
                if dep in deferred or dep in state.wall_uids:
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
