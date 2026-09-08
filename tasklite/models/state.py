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

from .job import Job


def uid_from_job_dict(job_dict: dict) -> str:
    """从 job dict 中直接提取 uid，避免完整的 Job.from_dict 反序列化开销。"""
    task_type = job_dict.get("task_type")
    job_id = job_dict.get("job_id")
    if task_type is not None and job_id is not None:
        return f"{task_type}::{job_id}"
    # fallback: 对非标准 dict（测试中可能使用），尝试完整反序列化
    try:
        return Job.from_dict(job_dict).uid
    except (KeyError, TypeError, ValueError):
        # 最终 fallback: 基于 dict 内容生成稳定 hash uid（完整 64 字符降低碰撞概率）
        import hashlib, json
        h = hashlib.sha256(json.dumps(job_dict, sort_keys=True, default=str).encode()).hexdigest()
        return f"_unknown::{h}"


class PipelineState:
    """Mutable pipeline state: wall/failed/cursors dicts + queue list.

    All operations are in-place, O(1)/O(k)。除 ``_queue_uids``（由
    ``pop_job``/``spawn_jobs`` 独占维护）外，另维护 ``_wall_uids``/
    ``_failed_uids`` 两个增量 uid 索引——所有 wall/failed 终态转移必须经
    ``mark_success``/``mark_failed``，
    直接改 dict 会使索引漂移并被 DEBUG 断言捕获。

    所有权契约：构造时对 wall/failed 做顶层浅拷贝（dict(...)），value
    （meta dict）与调用方共享引用——生产路径的 value 来自 backend
    反序列化的新鲜对象；调用方构造后不得变异传入的 value。终态写入经
    ``mark_success``/``mark_failed``（内部深拷贝），不受外部别名影响。
    """

    def __init__(
        self,
        wall: Mapping[str, Dict[str, Any]],
        failed: Mapping[str, Dict[str, Any]],
        cursors: Mapping[str, str],
        queue: List[Dict[str, Any]],
    ):
        # 顶层浅拷贝而非深拷贝：生产路径的 wall/failed 来自 backend 反序列化
        # 的新鲜对象（每次 run 重建），deepcopy 是纯双倍开销——百万级 wall
        # 时启动时间与内存 ×2。所有权契约：value（meta dict）共享引用，
        # 调用方构造后不得变异传入的 value；终态写入只经 mark_success/
        # mark_failed（内部对 meta 深拷贝后整体替换），不受外部别名影响。
        self.wall: Dict[str, Dict[str, Any]] = dict(wall)
        self.failed: Dict[str, Dict[str, Any]] = dict(failed)
        self.cursors: Dict[str, str] = dict(cursors)
        self.queue: List[dict] = list(queue)
        self._wall_uids: set = set(self.wall)
        self._failed_uids: set = set(self.failed)
        self._queue_uids: set = {uid_from_job_dict(j) for j in self.queue}
        # in-flight 作业 uid 集合纳入 state，作为
        # 「作业是否在系统中」的统一事实源之一。由 register/unregister 维护，
        # 与 _queue_uids 同款模式（派发即出队 → 二者互斥）。
        self._in_flight_uids: set = set()
        # rerun 策略任务（every_run/on_failure/on_input_change）
        # 的 uid——其「同时属于 wall/failed（历史）与 queue/in-flight（重跑中）」
        # 是合法语义，DEBUG 互斥断言对此豁免（身份非真空仍成立：始终 ≥1 集合）。
        # 生命周期：spawn/requeue/入队时加入，任务终结（unregister）时移除。
        self._rerun_active_uids: set = set()
        for j in self.queue:
            if j.get("rerun") in ("every_run", "on_failure", "on_input_change"):
                self._rerun_active_uids.add(uid_from_job_dict(j))

    # 队列变更（唯一修改 queue + _queue_uids 的路径）----------------

    def pop_job(self, idx: int) -> dict:
        """弹出并返回 job dict，同步从 _queue_uids 移除其 uid。"""
        job_dict = self.queue.pop(idx)
        uid = uid_from_job_dict(job_dict)
        if job_dict.get("rerun") in ("every_run", "on_failure", "on_input_change"):
            self._rerun_active_uids.add(uid)
        self._queue_uids.discard(uid)
        if __debug__:
            self._assert_uids_consistent()
        return job_dict

    def spawn_jobs(self, job_dicts: List[Dict[str, Any]], front: bool = True) -> None:
        """批量入队（spawn 默认队首，preserving order），逐条同步 uid。"""
        if front:
            self.queue[0:0] = list(job_dicts)
        else:
            self.queue.extend(job_dicts)
        self._queue_uids.update(uid_from_job_dict(j) for j in job_dicts)
        for j in job_dicts:
            if j.get("rerun") in ("every_run", "on_failure", "on_input_change"):
                self._rerun_active_uids.add(uid_from_job_dict(j))
        if __debug__:
            self._assert_uids_consistent()

    def requeue_jobs(self, job_dicts: List[Dict[str, Any]], front: bool = True) -> None:
        """重入队（崩溃恢复/重试），语义同 spawn_jobs 队首插入。"""
        self.spawn_jobs(job_dicts, front=front)

    # in-flight 集合（唯一修改 _in_flight_uids 的路径）-------------

    def register_in_flight(self, uid: str) -> None:
        """登记一个已派发到子进程、尚未 commit 的作业 uid。"""
        self._in_flight_uids.add(uid)
        if __debug__:
            self._assert_state_consistent()

    def unregister_in_flight(self, uid: str) -> None:
        """注销一个已完成/失败/崩溃回收的 in-flight 作业 uid。"""
        self._in_flight_uids.discard(uid)
        self._rerun_active_uids.discard(uid)  # 任务终结，重跑豁免结束
        if __debug__:
            self._assert_state_consistent()

    def clear_in_flight(self) -> None:
        """清空 in-flight 集合（_abort_in_flight 使用）。

        不整体清空 ``_rerun_active_uids``——abort 时先
        requeue（rerun 任务回到 queue 且仍在 wall 有历史记录）再清空豁免
        集合会让 DEBUG 互斥断言崩（wall∩queue 无豁免）。重建为当前 queue
        中的 rerun 任务：requeue 后的 rerun 任务保留豁免。
        """
        self._in_flight_uids.clear()
        self._rerun_active_uids = {
            uid_from_job_dict(j) for j in self.queue
            if j.get("rerun") in ("every_run", "on_failure", "on_input_change")
        }
        if __debug__:
            self._assert_state_consistent()

    def replace_queue(self, job_dicts: List[Dict[str, Any]]) -> None:
        """整体替换队列（死锁批量移除肇事者场景），重建 uid 索引。"""
        self.queue = list(job_dicts)
        self._queue_uids = {uid_from_job_dict(j) for j in self.queue}
        self._rerun_active_uids = {
            uid_from_job_dict(j) for j in self.queue
            if j.get("rerun") in ("every_run", "on_failure", "on_input_change")
        }
        if __debug__:
            self._assert_uids_consistent()

    # 级联失败（按需计算反向依赖）-------------------------------

    def _build_dependents(self) -> Dict[str, set]:
        """按需从当前 queue 构建反向依赖索引 dependents[dep_uid] -> {job_uid...}。

        级联是失败路径上的罕见操作，按需 O(N·k) 构建比
        在 pop/spawn/mark_success/replace 四条变更路径上增量维护索引更简单——
        eager 索引存在同步漏维护风险（级联漏标）。
        """
        dependents: Dict[str, set] = {}
        for jd in self.queue:
            try:
                job = Job.from_dict(jd)
            except (KeyError, TypeError, ValueError):
                continue  # 畸形 job 无法解析依赖，跳过
            job_uid = job.uid
            for dep_uid in job.depends_on:
                dependents.setdefault(dep_uid, set()).add(job_uid)
        return dependents

    def fail_cascade(self, failed_uid: str) -> List[str]:
        """沿反向依赖递归标记下游为级联失败。

        父 job 失败后，所有（直接/间接）依赖它的 job 都不可能满足依赖，
        应被级联标记。返回本次级联标记的 uid 列表（供调用方持久化）。

        一次性 O(下游数) 标记，消除调度循环逐轮扫描的级联检测延迟。
        排除 wall（已成功）与 failed（已失败，避免重复级联）。
        """
        dependents = self._build_dependents()
        cascade: List[str] = []
        stack = [failed_uid]
        seen = {failed_uid}
        while stack:
            cur = stack.pop()
            for downstream in dependents.get(cur, ()):
                if downstream not in seen and downstream not in self.wall \
                        and downstream not in self.failed:
                    seen.add(downstream)
                    cascade.append(downstream)
                    stack.append(downstream)
        return cascade

    def find_dependency_cycles(self) -> List[str]:
        """在当前 queue 的依赖图中找出所有依赖环成员。

        返回**环内成员 uid 集合**（扁平 list，含每个环的成员）。采用
        DFS + 三色标记（0=未访问, 1=在路径上, 2=已结束）检测有向环。

        我们只需要「环成员集合」用于细粒度失败，不需要断环回溯删除单个
        成员（我们的环=确定性死锁，全部环成员都标 DEPENDENCY_DEADLOCK）。

        Returns:
            环内成员 uid 列表（可能有重复，调用方用 set 吸收）。
        """
        # 依赖边：job_uid -> set(dep_uids)
        edges: Dict[str, set] = {}
        for jd in self.queue:
            try:
                job = Job.from_dict(jd)
            except (KeyError, TypeError, ValueError):
                continue
            edges[job.uid] = set(job.depends_on)

        color: Dict[str, int] = {}  # 0/缺失=未访问, 1=在路径上, 2=完成
        cycle_members: List[str] = []
        path_stack: List[str] = []

        def dfs(uid: str) -> None:
            color[uid] = 1
            path_stack.append(uid)
            for dep in edges.get(uid, ()):
                # 依赖不在当前队列中（在 wall/in_flight/缺失）→ 不算环
                if dep not in edges:
                    continue
                if color.get(dep, 0) == 1:
                    # 找到环：从 dep 到当前路径末尾都是环成员
                    try:
                        start = path_stack.index(dep)
                    except ValueError:
                        start = 0
                    cycle_members.extend(path_stack[start:])
                    cycle_members.append(dep)  # 闭环
                elif color.get(dep, 0) == 0:
                    dfs(dep)
            path_stack.pop()
            color[uid] = 2

        for uid in list(edges):
            if color.get(uid, 0) == 0:
                dfs(uid)
        return cycle_members

    # 完成/失败记录 ------------------------------------------------

    def mark_success(self, uid: str, meta: Dict[str, Any]) -> None:
        """终态转移：uid → wall（成功）。单一入口维护 wall/failed 索引。

        「最终状态唯一」在这里结构保证：进入 wall 的同时清除 failed 残行
        （rerun 任务重跑成功时磁盘同事务已删 DLQ 行，内存必须镜像），
        任何路径都不得再手工 ``failed.pop`` + ``mark_success`` 复制该出口逻辑。
        对新 meta 做防御性深拷贝。反向依赖索引不做增量维护——级联按需
        计算（``_build_dependents``），job 成功与否由 wall/failed 排除防御。
        """
        self.failed.pop(uid, None)
        self._failed_uids.discard(uid)
        self.wall[uid] = copy.deepcopy(meta)
        self._wall_uids.add(uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def mark_failed(self, uid: str, meta: Dict[str, Any]) -> None:
        """终态转移：uid → failed（DLQ）。单一入口维护 wall/failed 索引。

        与 ``mark_success`` 对称：进入 failed 的同时清除 wall 旧成功记录
        （rerun 任务重跑失败时 wall 旧记录必须作废），杜绝 wall∩failed
        并存。对新 meta 做防御性深拷贝。
        """
        self.wall.pop(uid, None)
        self._wall_uids.discard(uid)
        self.failed[uid] = copy.deepcopy(meta)
        self._failed_uids.add(uid)
        if __debug__:
            self._assert_terminal_uids_consistent()

    def update_cursors(self, updates: Mapping[str, str]) -> None:
        """合并游标更新（覆盖语义）。

        与后端语义对齐——值为 None 表示删除该游标（pop），
        非 None 值才写入（直接 ``dict.update`` 会把 {key: None} 写进
        cursors，偏离 None-删除语义）。
        """
        for k, v in updates.items():
            if v is None:
                self.cursors.pop(k, None)
            else:
                self.cursors[k] = v

    # 只读访问 ----------------------------------------------------

    @property
    def queue_uids(self) -> set:
        """队列 uid 的**活索引**（O(1) 去重/依赖判定；性能敏感路径直读免拷贝）。

        返回活引用而非 frozenset 副本：调度扫描与 spawn 去重每轮消费，
        O(N) 拷贝在万级队列下纯烧 CPU。调用方不得变异返回值——索引由
        ``pop_job``/``spawn_jobs``/``replace_queue`` 独占维护。
        """
        return self._queue_uids

    @property
    def wall_uids(self) -> set:
        """wall 终态 uid 的**活索引**（O(1) 构建；仅供派发快照/内部判定）。

        返回活引用而非 frozenset 副本：``_dispatch_job`` 构造 ctx 时随即
        pickle 下发，单线程派发路径上 state 不会并发变化。调用方不得修改
        返回值——索引由 ``mark_success``/``mark_failed`` 独占维护。
        """
        return self._wall_uids

    @property
    def failed_uids(self) -> set:
        """failed 终态 uid 的活索引（语义同 ``wall_uids``）。"""
        return self._failed_uids

    @property
    def attempted_uids(self) -> set:
        """wall∪failed 终态 uid 活索引（= ``ctx.attempted_uids()`` 的父进程侧视图）。

        命名即语义：「已尝试过」（wall 成功 ∪ failed 失败）——队列中待跑的
        不算；与四集合并集 ``is_known(uid)``（含 queue/in-flight）区分。
        """
        return self._wall_uids | self._failed_uids

    @property
    def in_flight_uids(self) -> FrozenSet[str]:
        """in-flight 作业 uid 集合。"""
        return frozenset(self._in_flight_uids)

    @property
    def is_empty(self) -> bool:
        """队列是否为空。"""
        return not self.queue

    def is_known(self, uid: str) -> bool:
        """作业是否「在系统中」的统一谓词。

        作业身份的唯一事实源 = wall ∪ failed ∪ 队列 ∪ in-flight。
        所有去重判定（spawn 去重、dispatch 去重、依赖判定）必须调用本
        谓词，而不是自行查某个集合——曾因「调用点自己拼集合」漏查
        in-flight 导致双重执行。
        """
        return (
            uid in self.wall
            or uid in self.failed
            or uid in self._queue_uids
            or uid in self._in_flight_uids
        )

    # 内部 ---------------------------------------------------------

    def _assert_uids_consistent(self) -> None:
        """DEBUG 不变式：_queue_uids 与 queue 完全一致。"""
        recomputed = {uid_from_job_dict(j) for j in self.queue}
        assert self._queue_uids == recomputed, (
            f"_queue_uids drift: cached={self._queue_uids!r} actual={recomputed!r}"
        )

    def _assert_terminal_uids_consistent(self) -> None:
        """DEBUG 不变式：wall/failed 增量索引与 dict 完全一致。

        捕获「绕开 mark_success/mark_failed 直接改 dict」的索引漂移——
        这正是 ctx 派发快照与 is_known 的事实源，漂移会让子进程看到过期
        终态集合（身份判定失真）。
        """
        assert self._wall_uids == set(self.wall), (
            f"_wall_uids drift: cached={self._wall_uids!r} actual={set(self.wall)!r}"
        )
        assert self._failed_uids == set(self.failed), (
            f"_failed_uids drift: cached={self._failed_uids!r} actual={set(self.failed)!r}"
        )

    def _assert_state_consistent(self) -> None:
        """DEBUG 不变式：作业身份六集合互斥（C(4,2)=6 对两两无交集）。

        - 派发即出队：_queue_uids 与 _in_flight_uids 互斥（同一 uid 不可能
          既在队列又 in-flight）。
        - wall/failed 与活动集合互斥：已完成的作业不在队列/不在 in-flight。
        - wall/failed 与彼此互斥：同一 uid 不可能既成功又失败。该互斥的
          后果落点：已 DLQ 的 job 若因控制流异常继续执行并
          commit_job_success，会同时进入 wall 和 failed——本条捕获。
        - 无重复：in-flight 集合本身无重复（set 语义保证）。
        """
        overlap = self._queue_uids & self._in_flight_uids
        assert not overlap, (
            f"uid in both queue and in_flight: {overlap!r} "
            f"(queue={self._queue_uids!r} in_flight={self._in_flight_uids!r})"
        )
        # rerun 策略任务（every_run/on_failure）重跑期间
        # uid 合法地同时属于 wall/failed（历史记录）与 queue/in-flight（重跑
        # 中）——互斥断言对此豁免（身份非真空仍成立）。非 rerun 任务的
        # wall/failed∩in-flight 仍是异常状态（已 DLQ 的 job 继续执行）的捕获网。
        done_overlap = (set(self.wall) | set(self.failed)) & self._in_flight_uids
        assert not (done_overlap - self._rerun_active_uids), (
            f"uid in wall/failed and in_flight: {done_overlap - self._rerun_active_uids!r}"
        )
        # 显式断言剩余互斥对——wall∩queue、failed∩queue、wall∩failed：
        # 「已 DLQ 的 job 被继续执行并 commit 成功」会让 uid 同时进入
        # wall 和 failed，wall/failed 与队列的漂移也在此一并显式捕获。
        wall_failed = set(self.wall) & set(self.failed)
        assert not wall_failed, (
            f"uid in both wall and failed: {wall_failed!r}"
        )
        done_queue = (set(self.wall) | set(self.failed)) & self._queue_uids
        assert not (done_queue - self._rerun_active_uids), (
            f"uid in wall/failed and queue: {done_queue - self._rerun_active_uids!r}"
        )
        self._assert_uids_consistent()
        self._assert_terminal_uids_consistent()
