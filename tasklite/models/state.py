"""Mutable pipeline state container.

Single-threaded event loop; immutability is unnecessary. Three contracts
govern consistency:
  1. In-memory queue = on-disk queue minus in-flight jobs.
  2. wall/failed/cursors advance only after backend commit returns True.
  3. ``_run_body`` repairs residual drift by filtering queue against
     wall/failed on load (at-least-once backstop).

All mutations are in-place; queue changes go through the three methods
(``pop_job``/``spawn_jobs``/``requeue_jobs``) so ``_queue_uids`` never drifts.
"""
from __future__ import annotations

import copy
from typing import Any, Dict, FrozenSet, List, Mapping

from .job import Job, RERUN_EXEMPT_VALUES


def _is_rerun_exempt(job_dict: dict) -> bool:
    """rerun 策略允许越过终态重跑的作业可合法落在 wall/failed ∩ 队列
    交集——六集合互斥断言对此类作业豁免。"""
    return job_dict.get("rerun") in RERUN_EXEMPT_VALUES


def uid_from_job_dict(job_dict: dict) -> str:
    """从 job dict 中直接提取 uid，避免完整的 Job.from_dict 反序列化开销。"""
    task_type = job_dict.get("task_type")
    job_id = job_dict.get("job_id")
    if task_type is not None and job_id is not None:
        return f"{task_type}::{job_id}"
    try:
        return Job.from_dict(job_dict).uid
    except (KeyError, TypeError, ValueError):
        import hashlib, json
        h = hashlib.sha256(json.dumps(job_dict, sort_keys=True, default=str).encode()).hexdigest()
        return f"_unknown::{h}"


class PipelineState:
    """Mutable pipeline state: wall/failed/cursors dicts + queue list.

    All operations are in-place, O(1)/O(k)。维护 ``_queue_uids``/``_wall_uids``/
    ``_failed_uids`` 增量 uid 索引。所有状态转移必须经由受控方法。
    """

    def __init__(
        self,
        wall: Mapping[str, Dict[str, Any]],
        failed: Mapping[str, Dict[str, Any]],
        cursors: Mapping[str, str],
        queue: List[Dict[str, Any]],
    ):
        self._wall: Dict[str, Dict[str, Any]] = dict(wall)
        self._failed: Dict[str, Dict[str, Any]] = dict(failed)
        self._cursors: Dict[str, str] = dict(cursors)
        self._queue: List[dict] = list(queue)
        self._wall_uids: set = set(self._wall)
        self._failed_uids: set = set(self._failed)
        self._queue_uids: set = {uid_from_job_dict(j) for j in self._queue}
        self._in_flight_uids: set = set()
        self._rerun_active_uids: set = set()
        for j in self._queue:
            if _is_rerun_exempt(j):
                self._rerun_active_uids.add(uid_from_job_dict(j))

    @property
    def wall(self) -> Dict[str, Dict[str, Any]]:
        return self._wall

    @property
    def failed(self) -> Dict[str, Dict[str, Any]]:
        return self._failed

    @property
    def cursors(self) -> Dict[str, str]:
        return self._cursors

    @property
    def queue(self) -> List[dict]:
        return self._queue

    @queue.setter
    def queue(self, value: List[dict]) -> None:
        self.replace_queue(value)

    # 队列变更（唯一修改 _queue + _queue_uids 的路径）----------------

    def pop_job(self, idx: int) -> dict:
        """弹出并返回 job dict，同步从 _queue_uids 移除其 uid。"""
        job_dict = self._queue.pop(idx)
        uid = uid_from_job_dict(job_dict)
        if _is_rerun_exempt(job_dict):
            self._rerun_active_uids.add(uid)
        self._queue_uids.discard(uid)
        if __debug__:
            self._assert_uids_consistent()
        return job_dict

    def spawn_jobs(self, job_dicts: List[Dict[str, Any]], front: bool = True) -> None:
        """批量入队（spawn 默认队首，preserving order），逐条同步 uid。"""
        if front:
            self._queue[0:0] = list(job_dicts)
        else:
            self._queue.extend(job_dicts)
        self._queue_uids.update(uid_from_job_dict(j) for j in job_dicts)
        for j in job_dicts:
            if _is_rerun_exempt(j):
                self._rerun_active_uids.add(uid_from_job_dict(j))
        if __debug__:
            self._assert_uids_consistent()

    def requeue_jobs(self, job_dicts: List[Dict[str, Any]], front: bool = True) -> None:
        """重入队（崩溃恢复/重试），语义同 spawn_jobs 队首插入。"""
        self.spawn_jobs(job_dicts, front=front)

    def mark_rerun_active(self, uid: str) -> None:
        """登记派发期准入放行的重跑豁免 uid。

        不变式：豁免集合与准入判定同源——字面 rerun 键由 pop_job/
        spawn_jobs 登记，准入层按含 discovery 默认策略的有效策略放行的
        重跑由此登记；缺任一登记，in-flight 登记的全量互斥断言都会把
        合法重跑误判为 wall/failed ∩ in-flight 违例。
        """
        self._rerun_active_uids.add(uid)

    # in-flight 集合（唯一修改 _in_flight_uids 的路径）-------------

    def register_in_flight(self, uid: str) -> None:
        """登记一个已派发到子进程、尚未 commit 的作业 uid。"""
        self._in_flight_uids.add(uid)
        if __debug__:
            self._assert_state_consistent()

    def unregister_in_flight(self, uid: str) -> None:
        """注销一个已完成/失败/崩溃回收的 in-flight 作业 uid。"""
        self._in_flight_uids.discard(uid)
        self._rerun_active_uids.discard(uid)
        if __debug__:
            self._assert_state_consistent()

    def clear_in_flight(self) -> None:
        """清空 in-flight 集合（_abort_in_flight 使用）。"""
        self._in_flight_uids.clear()
        self._rerun_active_uids = {
            uid_from_job_dict(j) for j in self._queue
            if _is_rerun_exempt(j)
        }
        if __debug__:
            self._assert_state_consistent()

    def replace_queue(self, job_dicts: List[Dict[str, Any]]) -> None:
        """整体替换队列（死锁批量移除肇事者场景），重建 uid 索引。"""
        self._queue = list(job_dicts)
        self._queue_uids = {uid_from_job_dict(j) for j in self._queue}
        self._rerun_active_uids = {
            uid_from_job_dict(j) for j in self._queue
            if _is_rerun_exempt(j)
        }
        if __debug__:
            self._assert_uids_consistent()

    # 级联失败（按需计算反向依赖）-------------------------------

    def _build_dependents(self) -> Dict[str, set]:
        """按需从当前 _queue 构建反向依赖索引 dependents[dep_uid] -> {job_uid...}。"""
        dependents: Dict[str, set] = {}
        for jd in self._queue:
            try:
                job = Job.from_dict(jd)
            except (KeyError, TypeError, ValueError):
                continue
            job_uid = job.uid
            for dep_uid in job.depends_on:
                dependents.setdefault(dep_uid, set()).add(job_uid)
        return dependents

    def fail_cascade(self, failed_uid: str) -> List[str]:
        """沿反向依赖递归标记下游为级联失败。"""
        dependents = self._build_dependents()
        cascade: List[str] = []
        stack = [failed_uid]
        seen = {failed_uid}
        while stack:
            cur = stack.pop()
            for downstream in dependents.get(cur, ()):
                if downstream not in seen and downstream not in self._wall \
                        and downstream not in self._failed:
                    seen.add(downstream)
                    cascade.append(downstream)
                    stack.append(downstream)
        return cascade

    def find_dependency_cycles(self) -> List[str]:
        """在当前 _queue 的依赖图中找出所有依赖环成员。

        不变式：返回值为每次回边命中时「DFS 当前路径自闭点起的切片 + 闭点重复」
        按遍历序的拼接，governor 死锁归因依赖该精确口径（成员、顺序、重复闭点）。
        实现为显式栈仿真调用栈：栈帧持有节点的依赖迭代器，弹帧即回溯置黑，
        遍历序与递归形式逐一致且深度不受解释器递归上限约束——深链队列
        （深度超递归限制）不得因环检测本身崩溃。
        """
        edges: Dict[str, set] = {}
        for jd in self._queue:
            try:
                job = Job.from_dict(jd)
            except (KeyError, TypeError, ValueError):
                continue
            edges[job.uid] = set(job.depends_on)

        color: Dict[str, int] = {}
        cycle_members: List[str] = []
        path_stack: List[str] = []
        frame_deps: List[Any] = []

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

    def mark_success(self, uid: str, meta: Dict[str, Any]) -> None:
        """终态转移：uid → wall（成功）。单一入口维护 wall/failed 索引。"""
        self._failed.pop(uid, None)
        self._failed_uids.discard(uid)
        self._wall[uid] = copy.deepcopy(meta)
        self._wall_uids.add(uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def mark_failed(self, uid: str, meta: Dict[str, Any]) -> None:
        """终态转移：uid → failed（DLQ）。单一入口维护 wall/failed 索引。"""
        self._wall.pop(uid, None)
        self._wall_uids.discard(uid)
        self._failed[uid] = copy.deepcopy(meta)
        self._failed_uids.add(uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def update_cursors(self, updates: Mapping[str, str]) -> None:
        """合并游标更新（覆盖语义）。"""
        for k, v in updates.items():
            if v is None:
                self._cursors.pop(k, None)
            else:
                self._cursors[k] = v

    # 运维管理受控变更（OpsConsole 公共接缝）--------------------------

    def discard_wall(self, uid: str) -> None:
        """从 wall 集合移除指定 uid（运维接缝，幂等）。"""
        self._wall.pop(uid, None)
        self._wall_uids.discard(uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def discard_failed(self, uid: str) -> None:
        """从 failed 集合移除指定 uid（运维接缝，幂等）。"""
        self._failed.pop(uid, None)
        self._failed_uids.discard(uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def add_wall(self, uid: str, meta: Dict[str, Any]) -> None:
        """向 wall 集合添加 uid（运维接缝，seed_wall 场景）。"""
        self._wall[uid] = meta
        self._wall_uids.add(uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def set_cursor(self, key: str, value: str) -> None:
        """设置单个游标键值（运维接缝，seed_cursor 场景）。"""
        self._cursors[key] = value

    # 只读访问 ----------------------------------------------------

    @property
    def queue_uids(self) -> set:
        """队列 uid 的活索引。"""
        return self._queue_uids

    @property
    def wall_uids(self) -> set:
        """wall 终态 uid 的活索引。"""
        return self._wall_uids

    @property
    def failed_uids(self) -> set:
        """failed 终态 uid 的活索引。"""
        return self._failed_uids

    @property
    def attempted_uids(self) -> set:
        """wall∪failed 终态 uid 活索引。"""
        return self._wall_uids | self._failed_uids

    @property
    def in_flight_uids(self) -> FrozenSet[str]:
        """in-flight 作业 uid 集合。"""
        return frozenset(self._in_flight_uids)

    @property
    def is_empty(self) -> bool:
        """队列是否为空。"""
        return not self._queue

    def is_known(self, uid: str) -> bool:
        """作业是否在系统中的统一谓词。"""
        return (
            uid in self._wall
            or uid in self._failed
            or uid in self._queue_uids
            or uid in self._in_flight_uids
        )

    # 内部 ---------------------------------------------------------

    def _rerun_exempt_uids_from_queue(self) -> set:
        """从 queue 事实源直接取重跑豁免集合，不依赖派生缓存
        ``_rerun_active_uids``（缓存被瞬态路径误删时断言不得误报）。"""
        return {
            uid_from_job_dict(j) for j in self._queue
            if _is_rerun_exempt(j)
        }

    def _assert_uids_consistent(self) -> None:
        """DEBUG 不变式：_queue_uids 与 _queue 完全一致。"""
        recomputed = {uid_from_job_dict(j) for j in self._queue}
        assert self._queue_uids == recomputed, (
            f"_queue_uids drift: cached={self._queue_uids!r} actual={recomputed!r}"
        )

    def _assert_terminal_uids_consistent(self) -> None:
        """DEBUG 不变式：wall/failed 增量索引与 dict 完全一致。"""
        assert self._wall_uids == set(self._wall), (
            f"_wall_uids drift: cached={self._wall_uids!r} actual={set(self._wall)!r}"
        )
        assert self._failed_uids == set(self._failed), (
            f"_failed_uids drift: cached={self._failed_uids!r} actual={set(self._failed)!r}"
        )

    def _assert_state_consistent(self) -> None:
        """DEBUG 不变式：作业身份六集合互斥。"""
        overlap = self._queue_uids & self._in_flight_uids
        assert not overlap, (
            f"uid in both queue and in_flight: {overlap!r} "
            f"(queue={self._queue_uids!r} in_flight={self._in_flight_uids!r})"
        )
        exempt = self._rerun_active_uids | self._rerun_exempt_uids_from_queue()
        done_overlap = (set(self._wall) | set(self._failed)) & self._in_flight_uids
        assert not (done_overlap - exempt), (
            f"uid in wall/failed and in_flight: {done_overlap - exempt!r}"
        )
        wall_failed = set(self._wall) & set(self._failed)
        assert not wall_failed, (
            f"uid in both wall and failed: {wall_failed!r}"
        )
        done_queue = (set(self._wall) | set(self._failed)) & self._queue_uids
        assert not (done_queue - exempt), (
            f"uid in wall/failed and queue: {done_queue - exempt!r}"
        )
        self._assert_uids_consistent()
        self._assert_terminal_uids_consistent()
