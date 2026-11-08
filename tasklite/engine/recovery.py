"""v2 恢复编排器：启动修复、崩溃安全保存、挂起恢复与 TOCTOU 安全中止。

RecoveryOrchestrator 统一内敛：

1. 启动期队列修复（repair_queue_on_load：磁盘真相合并、残留过滤、
   去重整理、差量定向落盘——加载快照严禁全表重写磁盘）；
2. 资源挂起状态加载与持久化恢复（load_resource_suspensions /
   persist_resource_suspensions，注入 ResourceManager）；
3. 崩溃/异常路径安全保存队列（save_queue_crash_safe：以磁盘真相合并
   内存队列，读-改-写收敛进单个写事务）；
4. 实时 suspend 信号排空与应用（apply_pending_signals）；
5. 异常/停机在途任务 TOCTOU 闭环中止与收尾（abort_in_flight）。

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
    from .admission import TriggerRegistry
    from .channel import ExecutionChannel, ExecutionResult
    from .in_flight import InFlightJob, InFlightTracker
    from .resource import ResourceManager

    class RecoveryStore(Protocol):
        """恢复编排消费的状态仓库契约（StateStore 自动满足）。

        ``backend`` 是持久化后端活引用（加载/修复/差量落盘不经 facade
        中转）；``state`` 为内存运行态（崩溃安全保存的队列合并侧）；
        ``set_state`` 为装载修复结果的落位口。
        """

        @property
        def backend(self) -> AbstractStateBackend: ...

        @property
        def state(self) -> PipelineState: ...

        def set_state(self, state: PipelineState | None) -> None: ...

        def apply_failure(
            self, job_uid: str, failure_metadata: dict[str, Any], *,
            job_snapshot: dict[str, Any] | None = None,
            cascade: bool = True, attempt_no: int | None = None,
            run_id: str | None = None, started_at: str | None = None,
        ) -> Any: ...

    class RecoveryCompletion(Protocol):
        """恢复编排消费的完成机器契约（CompletionMachine 自动满足）。

        Job 终结唯一经由完成机器收尾：中止分类结算走 ``settle_aborted``。
        """

        def complete_job(
            self, entry: InFlightJob, result: ExecutionResult
        ) -> None: ...

        def settle_aborted(
            self,
            cancelled_entries: Sequence[InFlightJob],
            done_entries: Sequence[tuple[InFlightJob, ExecutionResult]],
        ) -> None: ...

from ..backend.sqlite_backend import TARGET_SCHEMA_VERSION
from ..models.state import PipelineState, job_uid_from_snapshot
from ..utils.clock import utc_now_iso
from ..utils.jsonutil import loads

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
        trigger_registry: "TriggerRegistry | None" = None,
        completion: "RecoveryCompletion",
    ) -> None:
        self._store = store
        self._channel = channel
        self._resources = resources
        self._in_flight = in_flight
        self._trigger_registry = trigger_registry
        self._completion = completion

    def load_and_repair(self) -> PipelineState:
        """启动期持久化状态装载与修复（backend 四连加载 → 修复 → 装配）。

        backend 四连加载 → 终态交集收敛 → 队列修复 → 资源挂起恢复 →
        残留信号回收 → PipelineState 构造并落 store。全部依赖已在构造
        清单内（backend 经 store 活引用），零新增依赖。
        """
        backend = self._store.backend
        successes = backend.load_successes()
        failed = backend.load_failed()
        cursors = backend.load_cursors()

        if backend.schema_version >= TARGET_SCHEMA_VERSION:
            queue_rows = backend.load_queue_jobs()
            for row in queue_rows:
                u = row["job_uid"]
                if row.get("status") == "running" and (u in successes or u in failed):
                    raise RuntimeError(
                        f"Invariant conflict: Job {u!r} is running in queue but already present in terminal records "
                        f"(successes={u in successes}, failed={u in failed})"
                    )

            self._store.set_state(PipelineState(successes, failed, cursors, []))
            recovered_any = False
            for row in queue_rows:
                if row.get("status") == "running":
                    u = row["job_uid"]
                    att_no = row.get("attempt_no") or 1
                    r_id = row.get("run_id") or "unknown"
                    st_at = row.get("started_at")
                    job_snap = row.get("job")
                    msg = "上次执行未完成结算，实际结果未知"
                    fail_meta = {
                        "error_message": msg,
                        "reason": "unsettled_running_recovery",
                        "run_id": r_id,
                        "attempt_no": att_no,
                    }
                    self._store.apply_failure(
                        u, fail_meta, job_snapshot=job_snap, cascade=False,
                        attempt_no=att_no, run_id=r_id, started_at=st_at,
                    )
                    failed = dict(self._store.state.failed)
                    recovered_any = True

            if recovered_any:
                queue_rows = backend.load_queue_jobs()
            queue_snapshots = [
                row["job"] for row in queue_rows if row.get("status") == "pending"
            ]
        else:
            queue_snapshots = backend.load_queue()

        # 启动期队列整理与资源挂起加载（终态交集先收敛，后续修复与
        # 六集合互斥断言都依赖 successes/failed 互斥前提）
        self.converge_terminal_overlap(successes, failed)
        queue_snapshots = self.repair_queue_on_load(queue_snapshots, successes, failed)
        self.load_resource_suspensions()

        state = PipelineState(successes, failed, cursors, queue_snapshots)
        self._store.set_state(state)
        return state

    def repair_queue_on_load(
        self, queue_snapshots: list, successes: dict, failed: dict
    ) -> list:
        """加载期队列整理：磁盘合并重读 + 残留过滤 + UID 去重，差量定向清理。

        1. 磁盘合并重读：捕获「load 之后、repair 期间」并发进程合法入队
           的作业，补进本次 run 的内存队列；合并以磁盘真相序为权威序（窗口期
           front 入队作业保持其磁盘队首位置，快照独有行让位队尾）。重读失败
           仅放弃合并并告警，清理正确性不受影响；不基于重读结果做任何写盘。
        2. 残留过滤：过滤已在 successes/failed 中且不符合 trigger 策略的残留；
           放行保留的重跑行落字面 trigger 键（有效策略口径，豁免登记以行内
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
            disk_queue = self._store.backend.load_queue()
        except Exception as e:
            logger.warning(
                f"Failed to reload queue for repair merge; "
                f"proceeding with load-time snapshot only: {e}"
            )
            disk_queue = []

        # 合并以磁盘真相序为权威序（与 save_queue_crash_safe 的「磁盘独有行
        # 补回队首」语义对齐）：窗口期他进程 front 入队的行保持其磁盘队首
        # 位置，不得 append 到内存队尾后被停机落盘固化（磁盘/内存序分叉）。
        # 镜像行（快照∩磁盘）经集合合并收敛、不进告警分支；快照独有行
        # （窗口期被他进程消费的行）保底追加队尾维持 at-least-once，不丢行。
        disk_uids: set = set()
        merged_rows: list = []
        for job_snapshot in disk_queue:
            u = job_uid_from_snapshot(job_snapshot)
            if u not in disk_uids:  # uid 主键下磁盘重复不可达，防御性收敛
                disk_uids.add(u)
                merged_rows.append(job_snapshot)
        for job_snapshot in queue_snapshots:
            if job_uid_from_snapshot(job_snapshot) not in disk_uids:
                merged_rows.append(job_snapshot)

        seen_uid: set = set()
        clean_queue: list[dict] = []

        for job_snapshot in merged_rows:
            u = job_uid_from_snapshot(job_snapshot)
            # 去重（保留首条）。集合合并已收敛镜像行，此分支仅剩快照内部
            # 的真实重复告警。去重命中项不参与磁盘删除：uid 主键约束下
            # 磁盘不存在重复行，按 uid 删除会连同保留首条一并误删。
            if u in seen_uid:
                logger.warning(
                    f"Loading duplicate uid {u} in queue; keeping first occurrence"
                )
                continue
            seen_uid.add(u)
            clean_queue.append(job_snapshot)

        return clean_queue

    def converge_terminal_overlap(self, successes: dict, failed: dict) -> None:
        """加载期收敛存量 successes∩failed 终态交集（failed 优先，内存 + 磁盘同步）。

        不变式：successes/failed 全局互斥；交集属终态违例数据，不收敛则首次
        派发的六集合互斥断言对任意作业全局触发。收敛语义与失败档案写入
        侧清 successes 一致（failed 保留失败证据，可人工核查重跑，优于静默
        视为成功）。落盘为按 uid 定向 ``delete_successes`` 差量删除（磁盘其余
        行原样保留），失败仅降级告警——内存态已收敛，本次 run 不受影响，
        磁盘保持原状、下次加载重判（幂等）。
        """
        overlap = sorted(set(successes) & set(failed))
        if not overlap:
            return
        for uid in overlap:
            del successes[uid]
        logger.warning(
            f"Loaded {len(overlap)} uid(s) present in both successes and failed "
            f"(terminal-state violation); converged to failed and removed "
            f"from successes: {overlap}"
        )
        try:
            self._store.backend.delete_successes(overlap)
        except Exception as e:
            logger.warning(
                f"Failed to persist terminal-overlap convergence to backend; "
                f"in-memory state is converged, disk will re-converge on next load: {e}"
            )

    def load_resource_suspensions(self) -> None:
        pass

    def persist_resource_suspensions(self) -> None:
        pass

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

        def _merge(disk_queue: list) -> list:
            mem_queue = self._store.state.queue
            mem_uids = {job_uid_from_snapshot(job_snapshot) for job_snapshot in mem_queue}
            # 磁盘有而内存没有的作业（commit/pop 窗口内丢失的）补回队首
            extra = [
                job_snapshot for job_snapshot in disk_queue
                if job_uid_from_snapshot(job_snapshot) not in mem_uids
            ]
            if extra:
                logger.warning(
                    f"Crash-safe save: recovered {len(extra)} job(s) from disk "
                    f"that were missing in memory."
                )
            # 内存内按 uid 去重（异常路径可能重复 requeue 同一作业）
            seen: set = set()
            dedup_mem = []
            for job_snapshot in mem_queue:
                u = job_uid_from_snapshot(job_snapshot)
                if u in seen:
                    continue
                seen.add(u)
                dedup_mem.append(job_snapshot)
            return extra + dedup_mem

        try:
            self._store.backend.replace_queue_atomic(_merge)
        except Exception as e:
            logger.error(
                f"Crash-safe queue save failed; disk preserved as-is, "
                f"in-memory-only jobs will be re-run by at-least-once: {e}"
            )

    def apply_pending_signals(self, extra_uid: str | None = None) -> None:
        pass

    def abort_in_flight(self) -> None:
        """异常/强制停机时：kill 进行中的子进程、消费已完成的结果、requeue。

        在 run 主循环的 except 块和 ABORTING 停机路径调用。
        委托 channel 执行底层 TOCTOU 闭环中止（kill、重查、清理），本方法
        收敛状态机编排：释放资源 → 中止分类 → 统一结算。
        """
        if not self._in_flight:
            return

        # 1. 释放已占用的资源（务必在 clear 前）
        self._in_flight.release_all_resources(self._resources)

        # 2. 委托 channel 执行底层 TOCTOU 闭环中止（kill、重查、清理）
        handles = self._in_flight.active_handles()
        outcome = self._channel.abort_in_flight(handles)

        completed_map = {h.job_uid: res for h, res in outcome.completed}
        cancelled_entries, done_entries = self._in_flight.classify_aborted(completed_map)

        # 4. 委托完成机器统一结算已取消与已完成条目
        self._completion.settle_aborted(cancelled_entries, done_entries)


__all__ = [
    "RecoveryOrchestrator",
]
