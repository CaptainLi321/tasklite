"""恢复机器（崩溃安全保存 / suspend 信号排空 / abort 分类消费）。

``abort_in_flight`` 是异常退出与强制停机的统一收尾：先排空信号，再按
「结果文件是否已原子落盘」分类——已完成走完成机器提交（不重跑），
未完成 kill + 清半成品 + requeue（at-least-once）。依赖经 RunContext
注入、经 CompletionMachine 复用收尾契约，不反向引用 TaskLite。
"""

from __future__ import annotations

import logging
import math
import time
from typing import List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import RunContext
    from .completion import CompletionMachine
    from .store import RecoveryView

from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.job import Job, JobRuntimeState
from ..models.state import uid_from_job_dict
from ..utils.jsonutil import loads
from .inflight import InFlightJob, InFlightTracker
from .runtime import META_RESOURCE_SUSPENDS

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

    def __init__(self, ctx: "RunContext", completion: "CompletionMachine") -> None:
        self._ctx = ctx
        self._completion = completion

    def repair_queue_on_load(
        self, q_data: list, wall: dict, failed: dict
    ) -> list:
        """加载期队列整理：退避换算 + 过滤残留 + UID 去重，并执行单次原子持久化。

        1. 退避换算：以 wall_deadline 换算为单调时钟 monotonic _backoff_until。
        2. 残留过滤：过滤已在 wall/failed 中且不符合 rerun 策略的残留。
        3. UID 去重：同一 UID 重复条目保留首条。
        4. 变更持久化：若有过滤或去重发生，单次原子落盘，避免多次保存出现中间状态。
        """
        now = time.monotonic()
        wall_now = time.time()
        seen_uid: set = set()
        clean_q = []

        for jd in q_data:
            # 1. 退避换算（委托强类型 JobRuntimeState 对齐双时钟）
            rt_state = JobRuntimeState.from_dict(jd.get("runtime"))
            rt_state.align_wall_clock(now, wall_now)
            jd["runtime"] = rt_state.to_dict()

            # 2. 过滤残留
            u = uid_from_job_dict(jd)
            wall_hit = u in wall
            failed_hit = u in failed
            if wall_hit or failed_hit:
                decision = self._ctx.policy.evaluate(
                    jd,
                    wall_meta=wall.get(u),
                    is_wall=wall_hit,
                    is_failed=failed_hit,
                )
                if decision.should_skip:
                    continue


            # 3. 去重（保留首条）
            if u in seen_uid:
                logger.warning(
                    f"Loading duplicate uid {u} in queue; keeping first occurrence "
                    f"(dropping {len(clean_q)}-th)."
                )
                continue
            seen_uid.add(u)
            clean_q.append(jd)

        # 4. 单次原子落盘
        if len(clean_q) < len(q_data):
            self._ctx.backend.save_queue(clean_q)
        return clean_q

    def load_resource_suspends(self) -> None:
        """从 meta 表恢复资源 suspend 状态（换算回 monotonic 挂起时刻）。"""
        try:
            raw = self._ctx.backend.get_meta(META_RESOURCE_SUSPENDS)
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
        self._ctx.resource_mgr.restore_suspensions(deadlines)

    def persist_resource_suspends(self) -> None:
        """run 结束时把资源挂起截止持久化到 meta 表。

        薄转发到 RunContext.persist_resource_suspends_now——挂起的每个
        应用点即时持久化（消除 kill -9/OOM 时挂起丢失窗口）见该方法；
        本入口保留 run 收尾调用面与既有契约测试的兼容。
        """
        self._ctx.persist_resource_suspends_now()


    def save_queue_crash_safe(self) -> None:
        """崩溃路径保存队列：以「磁盘真相」合并「内存队列」，避免丢失作业。

        崩溃处理器不能盲目用内存队列全量覆盖磁盘——在以下窗口内，内存队列
        会缺失磁盘上仍存在的作业：
          - ``_dispatch_job`` 的 pop 之后、try 之前（作业既不在内存也不在 in-flight）；
          - ``_complete_job`` 的 commit 成功之后、apply 到内存之前（spawned 子任务、
            retry 重入队已在磁盘提交但内存未同步）。

        此方法重新加载磁盘队列，把「磁盘有而内存没有」的作业补回队首，
        再按 uid 去重保存（含内存自身的重复，来自异常路径的双 requeue）。
        """
        try:
            disk_q = self._ctx.backend.load_queue()
        except Exception as e:
            # load 失败不得用空列表继续覆盖——磁盘上「已
            # commit 但内存未同步」的作业会在此次保存中被永久抹除（覆盖
            # 用空 disk 基准）。磁盘至少是上次成功保存的状态，保留原样
            # 比覆盖更安全；内存丢失由 at-least-once 重跑吸收。
            logger.error(
                f"Failed to reload queue from disk for crash-safe save; "
                f"skipping overwrite to preserve disk truth: {e}"
            )
            return
        mem_q = self._ctx.store.queue
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
        self._ctx.backend.save_queue(extra + dedup_mem)


    def apply_pending_signals(self) -> None:
        """读取所有 in-flight job 的 suspend 信号文件，即时应用。

        handler 在子进程中调用 ``ctx.suspend_resource()`` 时，信号追加到
        ``{ipc_dir}/{uid}.signals.jsonl``（落盘 flush）。此方法在主循环
        drain 阶段读取并**删除**各 job 的信号文件（排空语义）——即使
        handler 随后崩溃/超时，落盘的限流信息也不丢失（文件仍在）。

        ``suspend()`` 使用 ``max`` 语义，重复应用同一信号是幂等的。
        """
        signals = self._ctx.channel.drain_active_signals(
            self._ctx.in_flight.active_uids()
        )
        applied = False
        for uid, r_name, secs in signals:
            if self._ctx.resource_mgr.suspend_resource(r_name, secs):
                logger.info(f"Applied suspend signal from {uid}: {r_name} for {secs}s")
                applied = True
            else:
                logger.warning(
                    f"Skipping suspend signal for unregistered resource "
                    f"{r_name!r} (from {uid})"
                )
        if applied:
            self._ctx.persist_resource_suspends_now()




    # ── 派发机器薄转发（实现见 engine/dispatch.py）──────────

    def abort_in_flight(self) -> None:
        """异常/强制停机时：kill 进行中的子进程、消费已完成的结果、requeue。

        在 ``_run_loop`` 的 except 块和 ABORTING 停机路径调用。
        委托 channel 执行底层 TOCTOU 闭环中止（kill、重查、清理），
        本方法收敛状态机编排：排空信号 -> 释放资源 -> 重入队未完成 -> 伪 entry 提交已完成。
        """
        if not self._ctx.in_flight:
            return

        # 1. 消费所有 in-flight 的 suspend 信号
        self.apply_pending_signals()

        # 2. 释放已占用的资源（务必在 clear 前）
        self._ctx.in_flight.release_all_resources(self._ctx.resource_mgr)

        # 3. 委托 channel 执行底层 TOCTOU 闭环中止（kill、重查、清理）
        handles = self._ctx.in_flight.active_handles()
        outcome = self._ctx.channel.abort_in_flight(handles)

        completed_map = {h.uid: res for h, res in outcome.completed}
        cancelled_entries, done_entries = self._ctx.in_flight.classify_aborted(completed_map)

        # 4. 委托 CompletionMachine 统一结算已取消与已完成条目
        self._completion.settle_aborted(cancelled_entries, done_entries)


# 向下兼容别名
RecoveryMachine = RecoveryOrchestrator

__all__ = [
    "RecoveryOrchestrator",
    "RecoveryMachine",
]
