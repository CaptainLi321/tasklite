"""v2 恢复编排器：启动修复、崩溃安全保存、挂起恢复与 TOCTOU 安全中止。

RecoveryOrchestrator 统一内敛：

1. 启动期队列修复（repair_queue_on_load：磁盘真相合并、残留过滤、
   去重整理、差量定向落盘——加载快照严禁全表重写磁盘）；
2. 资源挂起状态加载与持久化恢复（load_resource_suspensions /
   persist_resource_suspensions，注入 ResourceManager）；
3. 崩溃/异常路径安全保存队列（save_queue_crash_safe：以磁盘真相合并
   内存队列，读-改-写收敛进单个写事务）；
4. 实时 suspend 信号排空与应用（apply_pending_signals）与启动期残留
   信号回收（salvage_residue_signals）；
5. 异常/停机在途任务 TOCTOU 闭环中止与收尾（abort_in_flight）；
6. 陈旧结果恢复（restore_stale_result：派发前消费上次 run 遗留的
   已落盘结果，避免双重执行窗口）。

依赖以显式窄清单注入（无共享袋）；``store`` 与 ``completion`` 两个
协作机器按具名 Protocol 消费——``RecoveryStore``（backend 活引用 /
queue 内存队列视图 / set_state 落位口，StateStore 自动满足）与
``RecoveryCompletion``（complete_job(entry, result) 与
settle_aborted(cancelled, done)，Job 终结唯一经由完成机器收尾，
CompletionMachine 自动满足）。
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ..backend.base import AbstractStateBackend
    from .admission import RerunPolicy
    from .channel import ExecutionChannel, ExecutionResult
    from .in_flight import InFlightJob, InFlightTracker
    from .resource import ResourceManager

    class RecoveryStore(Protocol):
        """恢复编排消费的状态仓库契约（StateStore 自动满足）。

        ``backend`` 是持久化后端活引用（加载/修复/差量落盘不经 facade
        中转）；``queue`` 为内存队列视图（崩溃安全保存的合并侧）；
        ``set_state`` 为装载修复结果的落位口。
        """

        @property
        def backend(self) -> AbstractStateBackend: ...

        @property
        def queue(self) -> list[dict[str, Any]]: ...

        def set_state(self, state: PipelineState | None) -> None: ...

    class RecoveryCompletion(Protocol):
        """恢复编排消费的完成机器契约（CompletionMachine 自动满足）。

        Job 终结唯一经由完成机器收尾：残留认领走 ``complete_job`` 同一
        出口，中止分类结算走 ``settle_aborted``。
        """

        def complete_job(
            self, entry: InFlightJob, result: ExecutionResult
        ) -> None: ...

        def settle_aborted(
            self,
            cancelled_entries: Sequence[InFlightJob],
            done_entries: Sequence[tuple[InFlightJob, ExecutionResult]],
        ) -> None: ...

from ..exceptions import _JobTerminated
from ..models.job import Job
from ..models.state import PipelineState, uid_from_job_dict
from ..utils.jsonutil import loads
from .resource import (
    META_RESOURCE_SUSPENSIONS,
    apply_suspend_signals,
    persist_resource_suspensions,
)

logger = logging.getLogger("tasklite")


class RecoveryOrchestrator:
    """统一故障恢复、启动修复与在途任务收尾编排深模块。"""

    def __init__(
        self,
        *,
        store: "RecoveryStore",
        channel: "ExecutionChannel",
        resources: "ResourceManager",
        in_flight: "InFlightTracker",
        policy: "RerunPolicy",
        completion: "RecoveryCompletion",
    ) -> None:
        self._store = store
        self._channel = channel
        self._resources = resources
        self._in_flight = in_flight
        self._policy = policy
        self._completion = completion

    def load_and_repair(self) -> PipelineState:
        """启动期持久化状态装载与修复（backend 四连加载 → 修复 → 装配）。

        backend 四连加载 → 终态交集收敛 → 队列修复 → 资源挂起恢复 →
        残留信号回收 → PipelineState 构造并落 store。全部依赖已在构造
        清单内（backend 经 store 活引用），零新增依赖。
        """
        backend = self._store.backend
        wall = backend.load_wall()
        failed = backend.load_failed()
        cursors = backend.load_cursors()
        q_data = backend.load_queue()

        # 启动期队列整理与资源挂起加载（终态交集先收敛，后续修复与
        # 六集合互斥断言都依赖 wall/failed 互斥前提）
        self.converge_terminal_overlap(wall, failed)
        q_data = self.repair_queue_on_load(q_data, wall, failed)
        self.load_resource_suspensions()
        self.salvage_residue_signals()

        state = PipelineState(wall, failed, cursors, q_data)
        self._store.set_state(state)
        return state

    def repair_queue_on_load(
        self, q_data: list, wall: dict, failed: dict
    ) -> list:
        """加载期队列整理：磁盘合并重读 + 残留过滤 + UID 去重，差量定向清理。

        1. 磁盘合并重读：捕获「load 之后、repair 期间」并发进程合法入队
           的作业，补进本次 run 的内存队列；合并以磁盘真相序为权威序（窗口期
           front 入队作业保持其磁盘队首位置，快照独有行让位队尾）。重读失败
           仅放弃合并并告警，清理正确性不受影响；不基于重读结果做任何写盘。
        2. 残留过滤：过滤已在 wall/failed 中且不符合 rerun 策略的残留；
           放行保留的重跑行落字面 rerun 键（有效策略口径，豁免登记以行内
           键为事实源）。
        3. UID 去重：同一 UID 重复条目保留首条；磁盘重读与快照的同 uid
           重逢属合并预期内的镜像行（静默收敛），仅快照内部的真实异常重复
           告警。
        4. 差量落盘：仅对残留行按 uid 定向 DELETE（delete_queue_uids）。
           不变式：修复基准是加载时刻的陈旧快照，严禁以其全表 save_queue
           重写——重写会静默吞掉窗口期他进程已应答成功的入队（磁盘与内存
           双失、永不再现，at-least-once 被击穿）；定向 DELETE 的删除集不含
           并发新行，无覆盖窗口，且删除失败时磁盘保持原状、下次加载重判
           （幂等）。
        """
        try:
            disk_q = self._store.backend.load_queue()
        except Exception as e:
            logger.warning(
                f"Failed to reload queue for repair merge; "
                f"proceeding with load-time snapshot only: {e}"
            )
            disk_q = []

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

        seen_uid: set = set()
        clean_q: list[dict] = []
        dropped_uids: list = []

        for jd in merged_rows:
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
                # 构造期豁免登记以行内键为事实源，动态兜底放行不得在加载态
                # 丢失豁免（否则任意其他作业的 in-flight 登记即触发互斥断言）。
                jd["rerun"] = decision.effective_rerun

            # 去重（保留首条）。集合合并已收敛镜像行，此分支仅剩快照内部
            # 的真实重复告警。去重命中项不参与磁盘删除：uid 主键约束下
            # 磁盘不存在重复行，按 uid 删除会连同保留首条一并误删。
            if u in seen_uid:
                logger.warning(
                    f"Loading duplicate uid {u} in queue; keeping first occurrence"
                )
                continue
            seen_uid.add(u)
            clean_q.append(jd)

        # 差量落盘：只删除需要移除的残留行，其余行原样保留。
        # 落盘失败仅降级告警——内存态已收敛，本次 run 不受影响，磁盘保持
        # 原状、下次加载重判（幂等），与 converge_terminal_overlap 同策略。
        if dropped_uids:
            try:
                self._store.backend.delete_queue_uids(dropped_uids)
            except Exception as e:
                logger.warning(
                    f"Failed to persist queue repair deletions to backend; "
                    f"in-memory state is converged, disk will re-converge on next load: {e}"
                )
        return clean_q

    def converge_terminal_overlap(self, wall: dict, failed: dict) -> None:
        """加载期收敛存量 wall∩failed 终态交集（failed 优先，内存 + 磁盘同步）。

        不变式：wall/failed 全局互斥；交集属终态违例数据，不收敛则首次
        派发的六集合互斥断言对任意作业全局触发。收敛语义与失败档案写入
        侧清 wall 一致（failed 保留失败证据，可人工核查重跑，优于静默
        视为成功）。落盘为按 uid 定向 ``delete_wall`` 差量删除（磁盘其余
        行原样保留），失败仅降级告警——内存态已收敛，本次 run 不受影响，
        磁盘保持原状、下次加载重判（幂等）。
        """
        overlap = sorted(set(wall) & set(failed))
        if not overlap:
            return
        for uid in overlap:
            del wall[uid]
        logger.warning(
            f"Loaded {len(overlap)} uid(s) present in both wall and failed "
            f"(terminal-state violation); converged to failed and removed "
            f"from wall: {overlap}"
        )
        try:
            self._store.backend.delete_wall(overlap)
        except Exception as e:
            logger.warning(
                f"Failed to persist terminal-overlap convergence to backend; "
                f"in-memory state is converged, disk will re-converge on next load: {e}"
            )

    def load_resource_suspensions(self) -> None:
        """从 meta 表恢复资源挂起状态（挂钟截止换算回挂起秒数注入 ResourceManager）。"""
        try:
            raw = self._store.backend.get_meta(META_RESOURCE_SUSPENSIONS)
        except Exception as e:
            logger.warning(f"Failed to load resource suspensions from meta: {e}")
            return
        if raw is None:
            return
        try:
            deadlines = loads(raw)
        except (ValueError, TypeError) as e:
            logger.warning(f"Corrupted resource_suspensions meta, ignoring: {e}")
            return
        if not isinstance(deadlines, dict):
            logger.warning("resource_suspensions meta is not a dict, ignoring")
            return
        self._resources.restore_suspensions(deadlines)

    def persist_resource_suspensions(self) -> None:
        """run 结束时把资源挂起截止持久化到 meta 表。

        挂起的应用点即时持久化（消除 kill -9/OOM 时挂起丢失窗口）
        经 resource.persist_resource_suspensions 共享助手落盘。
        """
        persist_resource_suspensions(self._store.backend, self._resources)

    def salvage_residue_signals(self) -> None:
        """启动期回收 ipc_dir 全部跨 run 残留 suspend 信号并即时应用。

        主进程在 worker ``record_signal`` 之后、同 run 任一排空点之前
        崩溃时，信号文件存活但同 run 再无排空触点；后续派发预检清理与
        完成收尾清理都会未读删除信号文件。启动期全量清扫（先读分发后
        删除）是该不变式的兜底回收点；``suspend()`` 的 max 语义保证与
        meta 恢复的挂起幂等合并。清扫原语异常只降级告警，不打断启动。
        """
        try:
            signals = self._channel.drain_all_signals()
        except Exception as e:
            logger.warning(f"Failed to salvage residue signals: {e}")
            return
        apply_suspend_signals(
            signals,
            self._store.backend,
            self._resources,
            origin="salvaged from residue of ",
        )

    def save_queue_crash_safe(self) -> None:
        """崩溃路径保存队列：以「磁盘真相」合并「内存队列」，避免丢失作业。

        崩溃处理器不能盲目用内存队列全量覆盖磁盘——内存队列在以下窗口会
        缺失磁盘上仍存在的作业：
          - 派发机器 pop 之后、spawn 之前（作业既不在内存也不在 in-flight）；
          - commit 成功之后、apply 到内存之前（spawned 子任务、重试重入队
            已在磁盘提交但内存未同步）。

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
            mem_q = self._store.state.queue
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
        排空阶段读取并**删除**各 job 的信号文件（排空语义）——即使
        handler 随后崩溃/超时，落盘的限流信息也不丢失（文件仍在）。

        ``suspend()`` 使用 max 语义，重复应用同一信号是幂等的。
        """
        signals = self._channel.drain_active_signals(
            self._in_flight.active_uids()
        )
        apply_suspend_signals(
            signals, self._store.backend, self._resources, origin="from "
        )

    def restore_stale_result(
        self,
        uid: str,
        job: Job,
        job_dict: dict,
        *,
        attempt_id: int | None = None,
    ) -> bool:
        """崩溃恢复：派发子进程前消费该 uid 的残留结果文件。

        上次 run 主进程 SIGKILL/OOM/断电崩溃时，子进程可能已写好结果
        文件但未及 commit。本方法在派发机器的 acquire/spawn 之前调用：
        若有残留结果，直接构造伪 entry 交完成机器按同一收尾契约提交
        （不启动子进程），避免「新子进程已启动、却被旧结果文件误判完成
        而 kill」的双重执行窗口。``attempt_id`` 是本次派发已开的轨迹行，
        随伪 entry 透传给完成机器按认领结果收尾（残留行不悬空为 running）。

        Returns:
            True 表示已消费残留（job 已提交/重入队，调用方不再派发子进程）；
            False 表示无残留（含认证拒绝的跨 run/伪造残留），照常派发。
        """
        result = self._channel.claim_stale_result(uid, job)
        if result is None:
            return False
        logger.info(f"RESTORE: {uid} (stale result from previous run, no subprocess)")
        # （单一出口）：构造安全伪 entry 走完成机器同一条路径
        # （acquired=[] 无资源、handle=None 无进程），复用完整的收尾契约：
        # 失败输出清理、IPC 文件清理、身份注销。
        entry = self._in_flight.create_pseudo_entry(uid, job_dict, job)
        entry.attempt_id = attempt_id
        try:
            self._completion.complete_job(entry, result)
        except _JobTerminated:
            # 残留结果提交时 commit 连续失败达阈值 → job 已失败档案终结。
            # 不 re-raise（主循环继续），消费语义视为完成（返回 True）。
            pass
        return True

    def abort_in_flight(self) -> None:
        """异常/强制停机时：kill 进行中的子进程、消费已完成的结果、requeue。

        在 run 主循环的 except 块和 ABORTING 停机路径调用。
        委托 channel 执行底层 TOCTOU 闭环中止（kill、重查、清理），本方法
        收敛状态机编排：排空信号 → 释放资源 → 中止分类 → 挂起补应用 →
        统一结算。
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
        apply_suspend_signals(
            outcome.salvaged_signals,
            self._store.backend,
            self._resources,
            origin="salvaged from aborted ",
        )

        completed_map = {h.uid: res for h, res in outcome.completed}
        cancelled_entries, done_entries = self._in_flight.classify_aborted(completed_map)

        # 4. 委托完成机器统一结算已取消与已完成条目
        self._completion.settle_aborted(cancelled_entries, done_entries)


__all__ = [
    "RecoveryOrchestrator",
]
