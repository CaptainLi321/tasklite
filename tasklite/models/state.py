"""v2 可变管线状态容器（successes / failed / cursors / queue / in-flight）。

单线程事件循环，无需不可变化。三条一致性契约：
  1. 内存队列 = 落盘队列 − in-flight 作业。
  2. successes/failed/cursors 只在后端 commit 返回 True 之后推进。
  3. 加载期由恢复编排层过滤 queue 对 successes/failed 的残留漂移
     （at-least-once 兜底）。

所有变更原地进行；队列变更只经受控方法，``_queue_uids`` 永不漂移。
"""
from __future__ import annotations

import copy
from collections.abc import Mapping, Set as AbstractSet
from typing import Any

from ..exceptions import TaskLiteError
from ..utils.encoding import make_job_uid
from .job import Job


def job_uid_from_snapshot(job_snapshot: dict) -> str:
    """从 job dict 中直接提取 uid，避免完整的 Job.from_dict 反序列化开销。"""
    task_type = job_snapshot.get("task_type")
    job_id = job_snapshot.get("job_id")
    if isinstance(task_type, str) and task_type and isinstance(job_id, str) and job_id:
        try:
            return make_job_uid(task_type, job_id)
        except TaskLiteError:
            pass
    try:
        return Job.from_dict(job_snapshot).uid
    except (KeyError, TypeError, ValueError, TaskLiteError):
        import hashlib, json
        h = hashlib.sha256(json.dumps(job_snapshot, sort_keys=True, default=str).encode()).hexdigest()
        return f"_unknown::{h}"


class PipelineState:
    """可变管线状态：successes/failed/cursors 三个 dict + queue 列表。

    所有操作原地进行，O(1)/O(k)。维护 ``_queue_uids``/``_success_uids``/
    ``_failed_uids`` 增量 uid 索引。所有状态转移必须经由受控方法。
    """

    def __init__(
        self,
        successes: Mapping[str, dict[str, Any]],
        failed: Mapping[str, dict[str, Any]],
        cursors: Mapping[str, str],
        queue: list[dict[str, Any]],
    ):
        self._successes: dict[str, dict[str, Any]] = dict(successes)
        self._failed: dict[str, dict[str, Any]] = dict(failed)
        self._cursors: dict[str, str] = dict(cursors)
        self._queue: list[dict] = list(queue)
        self._success_uids: set[str] = set(self._successes)
        self._failed_uids: set[str] = set(self._failed)
        self._queue_uids: set[str] = {job_uid_from_snapshot(j) for j in self._queue}
        self._in_flight_uids: set[str] = set()
        self._trigger_skipped_uids: set[str] = set()

    @property
    def successes(self) -> dict[str, dict[str, Any]]:
        return self._successes

    @property
    def failed(self) -> dict[str, dict[str, Any]]:
        return self._failed

    @property
    def cursors(self) -> dict[str, str]:
        return self._cursors

    @property
    def queue(self) -> list[dict]:
        return self._queue

    @queue.setter
    def queue(self, value: list[dict]) -> None:
        self.replace_queue(value)

    # 队列变更（唯一修改 _queue + _queue_uids 的路径）----------------

    def pop_job(self, idx: int) -> dict:
        """弹出并返回 job dict，同步从 _queue_uids 移除其 uid。"""
        job_snapshot = self._queue.pop(idx)
        job_uid = job_uid_from_snapshot(job_snapshot)
        self._queue_uids.discard(job_uid)
        if __debug__:
            self._assert_uids_consistent()
        return job_snapshot

    def spawn_jobs(self, job_snapshots: list[dict[str, Any]], front: bool = True) -> None:
        """批量入队（spawn 默认队首插队，保序），逐条同步 uid。"""
        if front:
            self._queue[0:0] = list(job_snapshots)
        else:
            self._queue.extend(job_snapshots)
        self._trigger_skipped_uids.difference_update(job_uid_from_snapshot(j) for j in job_snapshots)
        self._queue_uids.update(job_uid_from_snapshot(j) for j in job_snapshots)
        if __debug__:
            self._assert_uids_consistent()

    def remove_queued_uid(self, job_uid: str) -> None:
        """按 uid 清除队列行。"""
        if job_uid not in self._queue_uids:
            return
        self._queue[:] = [
            job_snapshot for job_snapshot in self._queue
            if job_uid_from_snapshot(job_snapshot) != job_uid
        ]
        self._queue_uids.discard(job_uid)
        if __debug__:
            self._assert_uids_consistent()

    def requeue_jobs(self, job_snapshots: list[dict[str, Any]], front: bool = True) -> None:
        """重入队（崩溃恢复/重试），语义同 spawn_jobs 队首插入。"""
        self.spawn_jobs(job_snapshots, front=front)

    # in-flight 集合（唯一修改 _in_flight_uids 的路径）-------------

    def register_in_flight(self, job_uid: str) -> None:
        """登记一个已派发到子进程、尚未 commit 的作业 uid。"""
        self._in_flight_uids.add(job_uid)
        if __debug__:
            self._assert_state_consistent()

    def unregister_in_flight(self, job_uid: str) -> None:
        """注销一个已完成/失败/崩溃回收的 in-flight 作业 uid。"""
        self._in_flight_uids.discard(job_uid)
        if __debug__:
            self._assert_state_consistent()

    def clear_in_flight(self) -> None:
        """清空 in-flight 集合（中止回收路径使用）。"""
        self._in_flight_uids.clear()
        if __debug__:
            self._assert_state_consistent()

    def replace_queue(self, job_snapshots: list[dict[str, Any]]) -> None:
        """整体替换队列（死锁批量移除肇事者场景），重建 uid 索引。"""
        self._queue = list(job_snapshots)
        self._queue_uids = {job_uid_from_snapshot(j) for j in self._queue}
        self._trigger_skipped_uids.difference_update(self._queue_uids)
        if __debug__:
            self._assert_uids_consistent()

    # 级联失败（按需计算反向依赖）-------------------------------

    def _build_dependents(self) -> dict[str, set[str]]:
        """按需从当前 _queue 构建反向依赖索引 dependents[dep_uid] -> {job_uid...}。"""
        dependents: dict[str, set[str]] = {}
        for job_snapshot in self._queue:
            try:
                job = Job.from_dict(job_snapshot)
            except (KeyError, TypeError, ValueError, TaskLiteError):
                continue
            job_uid = job.uid
            for dep_uid in job.depends_on:
                dependents.setdefault(dep_uid, set()).add(job_uid)
        return dependents

    def cascade_fail(
        self,
        failed_uid: str,
        *,
        excluded_uids: frozenset[str] = frozenset(),
    ) -> list[str]:
        """沿反向依赖递归标记下游为级联失败。

        ``excluded_uids`` 是传播终止点（执行选择排除的作业）：既不加入级联
        集合，也不向其下游继续传播——这些作业原样留队，由后续 run 消费。
        """
        dependents = self._build_dependents()
        cascade: list[str] = []
        stack = [failed_uid]
        seen = {failed_uid}
        while stack:
            cur = stack.pop()
            for downstream in dependents.get(cur, ()):
                if downstream in excluded_uids:
                    continue
                if downstream not in seen and downstream not in self._successes \
                        and downstream not in self._failed:
                    seen.add(downstream)
                    cascade.append(downstream)
                    stack.append(downstream)
        return cascade

    def find_dependency_cycles(
        self, *, node_uids: AbstractSet[str] | None = None
    ) -> list[str]:
        """在当前 _queue 的依赖图中找出所有依赖环成员。

        ``node_uids`` 限定参与构图的队列节点；节点范围外的依赖边不参与环检测。

        不变式：返回值为每次回边命中时「DFS 当前路径自闭点起的切片 + 闭点重复」
        按遍历序的拼接，死锁治理器的归因依赖该精确口径（成员、顺序、重复闭点）。
        实现为显式栈仿真调用栈：栈帧持有节点的依赖迭代器，弹帧即回溯置黑，
        遍历序与递归形式逐一致且深度不受解释器递归上限约束——深链队列
        （深度超递归限制）不得因环检测本身崩溃。
        """
        edges: dict[str, set[str]] = {}
        for job_snapshot in self._queue:
            try:
                job = Job.from_dict(job_snapshot)
            except (KeyError, TypeError, ValueError, TaskLiteError):
                continue
            if node_uids is not None and job.uid not in node_uids:
                continue
            edges[job.uid] = set(job.depends_on)

        color: dict[str, int] = {}
        cycle_members: list[str] = []
        path_stack: list[str] = []
        frame_deps: list[Any] = []

        for root in list(edges):
            if color.get(root, 0) != 0:
                continue
            color[root] = 1
            path_stack.append(root)
            frame_deps.append(iter(edges.get(root, ())))
            while frame_deps:
                try:
                    dep = next(frame_deps[-1])
                except StopIteration:
                    frame_deps.pop()
                    color[path_stack.pop()] = 2
                    continue
                if dep not in edges:
                    continue
                dep_color = color.get(dep, 0)
                if dep_color == 1:
                    try:
                        start = path_stack.index(dep)
                    except ValueError:
                        start = 0
                    cycle_members.extend(path_stack[start:])
                    cycle_members.append(dep)
                elif dep_color == 0:
                    color[dep] = 1
                    path_stack.append(dep)
                    frame_deps.append(iter(edges.get(dep, ())))
        return cycle_members

    # 完成/失败记录 ------------------------------------------------

    def mark_success(self, job_uid: str, metadata: dict[str, Any]) -> None:
        """终态转移：uid → successes（成功）。单一入口维护 successes/failed 索引。"""
        self._trigger_skipped_uids.discard(job_uid)
        self._failed.pop(job_uid, None)
        self._failed_uids.discard(job_uid)
        self._successes[job_uid] = copy.deepcopy(metadata)
        self._success_uids.add(job_uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def mark_failed(self, job_uid: str, metadata: dict[str, Any]) -> None:
        """终态转移：uid → failed（失败档案）。单一入口维护 successes/failed 索引。"""
        self._trigger_skipped_uids.discard(job_uid)
        self._successes.pop(job_uid, None)
        self._success_uids.discard(job_uid)
        self._failed[job_uid] = copy.deepcopy(metadata)
        self._failed_uids.add(job_uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def update_cursors(self, updates: Mapping[str, str | None]) -> None:
        """合并游标更新（覆盖语义）。"""
        for k, v in updates.items():
            if v is None:
                self._cursors.pop(k, None)
            else:
                self._cursors[k] = v

    # 运维管理受控变更（OpsConsole 公共接缝）--------------------------

    def discard_success(self, job_uid: str) -> None:
        """从 successes 集合移除指定 uid（运维接缝，幂等）。"""
        self._successes.pop(job_uid, None)
        self._success_uids.discard(job_uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def discard_failed(self, job_uid: str) -> None:
        """从 failed 集合移除指定 uid（运维接缝，幂等）。"""
        self._failed.pop(job_uid, None)
        self._failed_uids.discard(job_uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def add_success(self, job_uid: str, metadata: dict[str, Any]) -> None:
        """向 successes 集合添加 uid（运维接缝，seed_successes 场景）。"""
        self._successes[job_uid] = metadata
        self._success_uids.add(job_uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def set_cursor(self, key: str, value: str) -> None:
        """设置单个游标键值（运维接缝，seed_cursor 场景）。"""
        self._cursors[key] = value

    # 只读访问 ----------------------------------------------------

    @property
    def queue_uids(self) -> set[str]:
        """队列 uid 的活索引。"""
        return self._queue_uids

    @property
    def trigger_skipped_uids(self) -> frozenset[str]:
        """本次 run 中触发条件判定无需执行的依赖就绪标记。"""
        return frozenset(self._trigger_skipped_uids)

    def mark_trigger_skipped(self, job_uid: str) -> None:
        """仅在跳过提交成功后记录；不伪造历史成功。"""
        self._trigger_skipped_uids.add(job_uid)

    def dependency_ready(self, job_uid: str) -> bool:
        """活跃候选优先于历史终态与本批无需执行的判定。"""
        return (
            job_uid not in self._queue_uids
            and job_uid not in self._in_flight_uids
            and (job_uid in self._success_uids or job_uid in self._trigger_skipped_uids)
        )

    @property
    def success_uids(self) -> set[str]:
        """successes 终态 uid 的活索引。"""
        return self._success_uids

    @property
    def failed_uids(self) -> set[str]:
        """failed 终态 uid 的活索引。"""
        return self._failed_uids

    @property
    def in_flight_uids(self) -> frozenset[str]:
        """in-flight 作业 uid 集合。"""
        return frozenset(self._in_flight_uids)

    @property
    def is_empty(self) -> bool:
        """队列是否为空。"""
        return not self._queue

    def is_known(self, job_uid: str) -> bool:
        """作业是否在系统中的统一谓词。"""
        return (
            job_uid in self._successes
            or job_uid in self._failed
            or job_uid in self._queue_uids
            or job_uid in self._in_flight_uids
        )

    # 内部 ---------------------------------------------------------

    def _assert_uids_consistent(self) -> None:
        """DEBUG 不变式：_queue_uids 与 _queue 完全一致。"""
        recomputed = {job_uid_from_snapshot(j) for j in self._queue}
        assert self._queue_uids == recomputed, (
            f"_queue_uids drift: cached={self._queue_uids!r} actual={recomputed!r}"
        )

    def _assert_terminal_uids_consistent(self) -> None:
        """DEBUG 不变式：successes/failed 增量索引与 dict 完全一致。"""
        assert self._success_uids == set(self._successes), (
            f"_success_uids drift: cached={self._success_uids!r} actual={set(self._successes)!r}"
        )
        assert self._failed_uids == set(self._failed), (
            f"_failed_uids drift: cached={self._failed_uids!r} actual={set(self._failed)!r}"
        )

    def _assert_state_consistent(self) -> None:
        """DEBUG 不变式：核心互斥不变式。
        1. queue ∩ in_flight == ∅
        2. successes ∩ failed == ∅
        """
        overlap = self._queue_uids & self._in_flight_uids
        assert not overlap, (
            f"uid in both queue and in_flight: {overlap!r} "
            f"(queue={self._queue_uids!r} in_flight={self._in_flight_uids!r})"
        )
        success_failed = self._success_uids & self._failed_uids
        assert not success_failed, (
            f"uid in both successes and failed: {success_failed!r}"
        )
        self._assert_uids_consistent()
        self._assert_terminal_uids_consistent()


__all__ = [
    "PipelineState",
    "job_uid_from_snapshot",
]
