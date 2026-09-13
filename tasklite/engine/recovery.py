"""恢复机器（崩溃安全保存 / suspend 信号排空 / abort 分类消费）。

``abort_in_flight`` 是异常退出与强制停机的统一收尾：先排空信号，再按
「结果文件是否已原子落盘」分类——已完成走完成机器提交（不重跑），
未完成 kill + 清半成品 + requeue（at-least-once）。依赖以显式窄清单注入
（无共享袋）、经 CompletionMachine 复用收尾契约，不反向引用 TaskLite。
"""

from __future__ import annotations

import logging
import math
import time
from typing import Any, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .channel import ExecutionChannel
    from .completion import CompletionMachine
    from .inflight import InFlightTracker
    from .policy import ExecutionPolicy
    from .resource import ResourceManager
    from .store import StateStore

from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.job import Job, JobRuntimeState
from ..models.state import uid_from_job_dict
from ..utils.jsonutil import loads
from .inflight import InFlightJob, InFlightTracker
from .resource import META_RESOURCE_SUSPENDS, persist_resource_suspensions

logger = logging.getLogger("tasklite")


class RecoveryOrchestrator:
    """统一故障恢复、启动修复与在途任务收尾编排深模块（RecoveryOrchestrator）。

    统一内敛：
    1. 启动期队列修复（repair_queue_on_load：退避换算、残留过滤、去重整理）；
    2. 资源挂起状态加载与持久化恢复（load_resource_suspends / persist_resource_suspends）；
    3. 崩溃/异常路径安全保存队列（save_queue_crash_safe：以磁盘真相合并内存队列）；
    4. 实时 suspend 信号排空与应用（apply_pending_signals）；
    5. 异常/停机在途任务 TOCTOU 闭环中止与收尾（abort_in_flight）。
    """

    def __init__(
        self,
        *,
        store: "StateStore",
        channel: "ExecutionChannel",
        resources: "ResourceManager",
        in_flight: "InFlightTracker",
        policy: "ExecutionPolicy",
        completion: "CompletionMachine",
    ) -> None:
        self._store = store
        self._channel = channel
        self._resources = resources
        self._in_flight = in_flight
        self._policy = policy
        self._completion = completion

    def repair_queue_on_load(
        self, q_data: list, wall: dict, failed: dict
    ) -> list:
        """加载期队列整理：磁盘合并重读 + 退避换算 + 过滤残留 + UID 去重，差量定向清理。

        1. 磁盘合并重读：捕获「load 之后、repair 期间」并发进程合法入队的
           作业，补进本次 run 的内存队列；合并以磁盘真相序为权威序（窗口期
           front 入队作业保持其磁盘队首位置，快照独有行让位队尾）。重读失败
           仅放弃合并并告警，清理正确性不受影响；不基于重读结果做任何写盘。
        2. 退避换算：以 wall_deadline 换算为单调时钟 monotonic _backoff_until。
           monotonic 值跨进程/重启无意义（每次加载由持久化 wall_deadline
           重推导），保留行无需回写磁盘——这正是消除覆盖窗口的前提。
        3. 残留过滤：过滤已在 wall/failed 中且不符合 rerun 策略的残留。
        4. UID 去重：同一 UID 重复条目保留首条；磁盘重读与快照的同 uid
           重逢属合并预期内的镜像行（静默收敛），仅快照内部的真实异常重复告警。
        5. 差量落盘：仅对残留行按 uid 定向 DELETE（delete_queue_uids）。
           不变式：修复基准是加载时刻的陈旧快照，严禁以其全表 save_queue
           重写——重写会静默吞掉窗口期他进程已应答成功的入队（磁盘与内存
           双失、永不再现，at-least-once 被击穿）；定向 DELETE 的删除集不含
           并发新行，无覆盖窗口，且删除失败时磁盘保持原状、下次加载重判（幂等）。
        """
        now = time.monotonic()
        wall_now = time.time()

        try:
            disk_q = self._store.backend.load_queue()
        except Exception as e:
            logger.warning(
                f"Failed to reload queue for repair merge; "
                f"proceeding with load-time snapshot only: {e}"
            )
            disk_q = []

        seen_uid: set = set()
        clean_q = []
        dropped_uids: list = []

        # 合并以磁盘真相序为权威序（与 save_queue_crash_safe 的「磁盘独有行
        # 补回队首」语义对齐）：窗口期他进程 front 入队的行保持其磁盘队首
        # 位置，不得 append 到内存队尾后被停机落盘固化（磁盘/内存序分叉）。
        # 镜像行（快照∩磁盘）经集合合并收敛、不进告警分支；快照独有行
        # （窗口期被他进程消费的行）保底追加队尾维持 at-least-once，不丢行。
        disk_uids: set = set()
        merged_rows: list = []
        for jd in disk_q:
            u = uid_from_job_dict(jd)
            if u not in disk_uids:  # uid 主键下磁盘重复不可达，防御性收敛
                disk_uids.add(u)
                merged_rows.append(jd)
        for jd in q_data:
            if uid_from_job_dict(jd) not in disk_uids:
                merged_rows.append(jd)

        for jd in merged_rows:
            # 1. 退避换算（委托强类型 JobRuntimeState 对齐双时钟）
            rt_state = JobRuntimeState.from_dict(jd.get("runtime"))
            rt_state.align_wall_clock(now, wall_now)
            jd["runtime"] = rt_state.to_dict()

            # 2. 过滤残留
            u = uid_from_job_dict(jd)
            wall_hit = u in wall
            failed_hit = u in failed
            if wall_hit or failed_hit:
                decision = self._policy.evaluate(
                    jd,
                    wall_meta=wall.get(u),
                    is_wall=wall_hit,
                    is_failed=failed_hit,
                )
                if decision.should_skip:
                    dropped_uids.append(u)
                    continue
                # 不变式：放行保留的重跑行必须落字面 rerun 键——PipelineState
                # 构造期豁免登记以行内键为事实源，discovery 默认晚于入队注册
                # 的动态兜底放行不得在加载态丢失豁免（否则任意其他作业的
                # in-flight 登记即触发互斥断言）。
                jd["rerun"] = decision.effective_rerun

            # 3. 去重（保留首条）。集合合并已收敛镜像行，此分支仅剩快照内部
            # 的真实重复告警。去重命中项不参与磁盘删除：uid 主键约束下
            # 磁盘不存在重复行，按 uid 删除会连同保留首条一并误删。
            if u in seen_uid:
                logger.warning(
                    f"Loading duplicate uid {u} in queue; keeping first occurrence "
                    f"(dropping {len(clean_q)}-th)."
                )
                continue
            seen_uid.add(u)
            clean_q.append(jd)

        # 4. 差量落盘：只删除需要移除的残留行，其余行原样保留
        if dropped_uids:
            self._store.backend.delete_queue_uids(dropped_uids)
        return clean_q

    def load_resource_suspends(self) -> None:
        """从 meta 表恢复资源 suspend 状态（换算回 monotonic 挂起时刻）。"""
        try:
            raw = self._store.backend.get_meta(META_RESOURCE_SUSPENDS)
        except Exception as e:
            logger.warning(f"Failed to load resource suspends from meta: {e}")
            return
        if raw is None:
            return
        try:
            deadlines = loads(raw)
        except (ValueError, TypeError) as e:
            logger.warning(f"Corrupted resource_suspends meta, ignoring: {e}")
            return
        if not isinstance(deadlines, dict):
            logger.warning("resource_suspends meta is not a dict, ignoring")
            return
        self._resources.restore_suspensions(deadlines)

    def persist_resource_suspends(self) -> None:
        """run 结束时把资源挂起截止持久化到 meta 表。

        挂起的应用点即时持久化（消除 kill -9/OOM 时挂起丢失窗口）
        经 resource.persist_resource_suspensions 共享助手落盘。
        """
        persist_resource_suspensions(self._store.backend, self._resources)


    def save_queue_crash_safe(self) -> None:
        """崩溃路径保存队列：以「磁盘真相」合并「内存队列」，避免丢失作业。

        崩溃处理器不能盲目用内存队列全量覆盖磁盘——内存队列在以下窗口会
        缺失磁盘上仍存在的作业：
          - ``_dispatch_job`` 的 pop 之后、try 之前（作业既不在内存也不在 in-flight）；
          - ``_complete_job`` 的 commit 成功之后、apply 到内存之前（spawned 子任务、
            retry 重入队已在磁盘提交但内存未同步）。

        读真相 → 合并 → 写回必须收敛进单个写事务
        （``replace_queue_atomic``）：跨进程 enqueue 与本保存并发时，其
        要么先于事务提交（进入磁盘真相、被合并保留），要么等事务提交后
        再落盘——两段式独立 load/save 的中间态会把窗口期他进程已应答
        成功的入队静默抹除（磁盘内存双失、永不再现，at-least-once 击穿）。

        保存失败（读/写/合并任一）由原语整体回滚保持磁盘原状，此处降级
        告警且不打断停机收尾序列——内存独有作业的丢失由下次启动的
        at-least-once 重扫吸收。
        """
        def _merge(disk_q: list) -> list:
            mem_q = self._store.queue
            mem_uids = {uid_from_job_dict(jd) for jd in mem_q}
            # 磁盘有而内存没有的作业（commit/pop 窗口内丢失的）补回队首
            extra = [jd for jd in disk_q if uid_from_job_dict(jd) not in mem_uids]
            if extra:
                logger.warning(
                    f"Crash-safe save: recovered {len(extra)} job(s) from disk "
                    f"that were missing in memory."
                )
            # 内存内按 uid 去重（异常路径可能重复 requeue 同一作业）
            seen: set = set()
            dedup_mem = []
            for jd in mem_q:
                u = uid_from_job_dict(jd)
                if u in seen:
                    continue
                seen.add(u)
                dedup_mem.append(jd)
            return extra + dedup_mem

        try:
            self._store.backend.replace_queue_atomic(_merge)
        except Exception as e:
            logger.error(
                f"Crash-safe queue save failed; disk preserved as-is, "
                f"in-memory-only jobs will be re-run by at-least-once: {e}"
            )


    def apply_pending_signals(self) -> None:
        """读取所有 in-flight job 的 suspend 信号文件，即时应用。

        handler 在子进程中调用 ``ctx.suspend_resource()`` 时，信号追加到
        ``{ipc_dir}/{uid}.signals.jsonl``（落盘 flush）。此方法在主循环
        drain 阶段读取并**删除**各 job 的信号文件（排空语义）——即使
        handler 随后崩溃/超时，落盘的限流信息也不丢失（文件仍在）。

        ``suspend()`` 使用 ``max`` 语义，重复应用同一信号是幂等的。
        """
        signals = self._channel.drain_active_signals(
            self._in_flight.active_uids()
        )
        applied = False
        for uid, r_name, secs in signals:
            if self._resources.suspend_resource(r_name, secs):
                logger.info(f"Applied suspend signal from {uid}: {r_name} for {secs}s")
                applied = True
            else:
                logger.warning(
                    f"Skipping suspend signal for unregistered resource "
                    f"{r_name!r} (from {uid})"
                )
        if applied:
            persist_resource_suspensions(self._store.backend, self._resources)




    # ── 派发机器薄转发（实现见 engine/dispatch.py）──────────

    def abort_in_flight(self) -> None:
        """异常/强制停机时：kill 进行中的子进程、消费已完成的结果、requeue。

        在 ``_run_loop`` 的 except 块和 ABORTING 停机路径调用。
        委托 channel 执行底层 TOCTOU 闭环中止（kill、重查、清理），
        本方法收敛状态机编排：排空信号 -> 释放资源 -> 重入队未完成 -> 伪 entry 提交已完成。
        """
        if not self._in_flight:
            return

        # 1. 消费所有 in-flight 的 suspend 信号
        self.apply_pending_signals()

        # 2. 释放已占用的资源（务必在 clear 前）
        self._in_flight.release_all_resources(self._resources)

        # 3. 委托 channel 执行底层 TOCTOU 闭环中止（kill、重查、清理）
        handles = self._in_flight.active_handles()
        outcome = self._channel.abort_in_flight(handles)

        # 3.5 杀进程后补排空的应用点：cancelled 任务的信号文件已随半成品
        # 清理删除，channel 捞回的 suspend 信号只能经 AbortOutcome 带回；
        # suspend 为 max 语义，与「先排空」阶段已应用项幂等合并。
        applied = False
        for uid, r_name, secs in outcome.salvaged_signals:
            if self._resources.suspend_resource(r_name, secs):
                logger.info(
                    f"Applied suspend signal salvaged from aborted {uid}: "
                    f"{r_name} for {secs}s"
                )
                applied = True
            else:
                logger.warning(
                    f"Skipping suspend signal for unregistered resource "
                    f"{r_name!r} (from aborted {uid})"
                )
        if applied:
            persist_resource_suspensions(self._store.backend, self._resources)

        completed_map = {h.uid: res for h, res in outcome.completed}
        cancelled_entries, done_entries = self._in_flight.classify_aborted(completed_map)

        # 4. 委托 CompletionMachine 统一结算已取消与已完成条目
        self._completion.settle_aborted(cancelled_entries, done_entries)


# 向下兼容别名
RecoveryMachine = RecoveryOrchestrator

__all__ = [
    "RecoveryOrchestrator",
    "RecoveryMachine",
]
