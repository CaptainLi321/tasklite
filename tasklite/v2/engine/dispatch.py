"""v2 派发机器：五关预检、两阶段资源租约与子进程 spawn 编排。

五关顺序即契约（顺序即时序约束）：dedup → dep-failed → no-handler →
orphan-probe → stale-restore。依赖以显式窄清单注入（无共享袋），经
``store``/``recovery`` 复用状态仓库与恢复编排深模块，不反向引用门面。

``session`` 与 ``tasks`` 按具名 Protocol 消费（装配契约见本模块的
``DispatchSession`` 与 ``TaskSource``：RunSession / TaskRegistry 真实
类型自动满足）；``output_root`` 与 config 声明同型
（``Path | Sequence[Path] | None``，多根输出场景）。

派发即插 attempts 轨迹行（outcome=running）：incarnation
（``run_id.dispatch_seq``）在本模块生成并随行落表——seq 每次派发递增，
同 uid 重试再派发也获得新 incarnation，与上次尝试的结果文件隔离。
"""

from __future__ import annotations

import enum
import logging
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from ..models.state import PipelineState
    from ..models.task import Task
    from .admission import RerunPolicy
    from .channel import ExecutionChannel
    from .errorclass import ErrorClassifier
    from .in_flight import InFlightTracker
    from .recovery import RecoveryOrchestrator
    from .resource import ResourceManager
    from .scheduler import JobScheduler
    from .store import StateStore

    class DispatchSession(Protocol):
        """派发/完成机器消费的运行会话装配契约（RunSession 自动满足）。

        ``run_id`` 是 incarnation 与 wall meta 的 run 身份事实源；
        ``result_token`` 随 WorkerLaunchSpec 下发做结果认证；
        ``next_dispatch_seq`` 为 incarnation fencing 提供单调递增序号。
        """

        run_id: str | None
        result_token: str | None

        def next_dispatch_seq(self) -> int: ...

    class TaskSource(Protocol):
        """派发机器消费的任务规格来源契约（TaskRegistry 自动满足）。"""

        def lookup(self, task_type: str) -> "Task": ...

        def __contains__(self, task_type: object) -> bool: ...

from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.attempt import ATTEMPT_REQUEUED, ATTEMPT_RUNNING, AttemptRecord
from ..models.context import JobContext
from ..models.job import Job, JobRuntimeState
from .channel import ArtifactCleanupMode, WorkerLaunchSpec
from .errorclass import (
    ERR_DISPATCH_FAILURE,
    ERR_JOB_DEPENDENCY,
    ERR_NO_HANDLER,
    ERR_PAYLOAD_VALIDATION,
    validate_payload_errors,
)
from .in_flight import InFlightJob
from .resource import RateLimitUnavailable, apply_suspend_signals
from .scheduler import ScheduleResult, StandstillFacts
from .types import AttemptFinish
from .wait import IN_FLIGHT_POLL_SECONDS
from ..utils.clock import utc_now_iso

logger = logging.getLogger("tasklite.v2")


class DispatchKind(enum.Enum):
    """一次调度派发周期的结局分类（单一事实，无交叠布尔）。"""

    SPAWNED = "spawned"                    # 子进程已启动并登记在飞
    HANDLED_NO_SUBPROCESS = "handled_no_subprocess"  # 五关/校验拦截，无需子进程即终结
    WORKER_SATURATED = "worker_saturated"  # 工人槽位耗尽，退出填池等待
    NO_CANDIDATE = "no_candidate"          # 队列无可运行候选（停摆事实随附）


@dataclass(frozen=True)
class DispatchOutcome:
    """一次调度派发周期的不可变决策结果。

    ``kind`` 是结局的唯一判据（调用方按枚举分派，无布尔交叠）；
    ``standstill`` 是调度结果的停摆投影（governor 仲裁输入唯一形状），
    构造期一次性投影——等待决策与死锁仲裁从同一份事实读取。
    """

    kind: DispatchKind
    entry: InFlightJob | None = None
    worker_wait: float = 0.0
    standstill: StandstillFacts = field(default_factory=StandstillFacts)


class DispatchMachine:
    """派发预检 + 资源租约 + 轨迹开行 + 子进程 spawn 的编排器。"""

    def __init__(
        self,
        *,
        store: "StateStore",
        scheduler: "JobScheduler",
        rerun_policy: "RerunPolicy",
        resources: "ResourceManager",
        channel: "ExecutionChannel",
        in_flight: "InFlightTracker",
        session: "DispatchSession",
        recovery: "RecoveryOrchestrator",
        tasks: "TaskSource",
        classifier: "ErrorClassifier",
        output_root: "Path | Sequence[Path] | None",
        ipc_dir: str,
        commit_failure_threshold: int,
    ) -> None:
        self._store = store
        self._scheduler = scheduler
        self._rerun_policy = rerun_policy
        self._resources = resources
        self._channel = channel
        self._in_flight = in_flight
        self._session = session
        self._recovery = recovery
        self._tasks = tasks
        self._classifier = classifier
        self._output_root = output_root
        self._ipc_dir = ipc_dir
        self._commit_failure_threshold = commit_failure_threshold

    # ── 扫描与派发接缝 ────────────────────────────────────────────────

    def dispatch_next(self) -> DispatchOutcome:
        """统一扫描与派发接缝：工人槽位预检 → 队列扫描 → 五关预检 →
        资源租约 → 子进程派发。

        返回 DispatchOutcome（kind 单一判据）：
        - SPAWNED：entry 携带在飞条目，调用方继续收割；
        - HANDLED_NO_SUBPROCESS：作业被五关/校验拦截并已终结（跳过/
          依赖失败/无 handler/校验失败/孤儿延迟/残留恢复），调用方可
          继续填池；
        - WORKER_SATURATED：工人槽位耗尽（worker_wait 为建议等待），
          调用方应退出填池循环；
        - NO_CANDIDATE：无可运行作业，停摆事实随 standstill 供仲裁。
        """
        store = self._store
        ok, worker_wait = self._resources.can_acquire_worker(1.0)
        if not ok:
            if worker_wait <= 0:
                worker_wait = IN_FLIGHT_POLL_SECONDS
            return DispatchOutcome(
                kind=DispatchKind.WORKER_SATURATED,
                worker_wait=worker_wait,
                standstill=StandstillFacts(min_wait=0.0),
            )

        sched = self._scheduler.scan_next_runnable(store.state, store.state.in_flight_uids)
        if sched.runnable_idx is None:
            return DispatchOutcome(
                kind=DispatchKind.NO_CANDIDATE,
                standstill=sched.standstill_facts(),
            )

        entry = self.dispatch_job(sched)
        if entry is None:
            return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)
        return DispatchOutcome(kind=DispatchKind.SPAWNED, entry=entry)

    def dispatch_job(self, sched: ScheduleResult) -> InFlightJob | None:
        """统一派发单个作业：出队 → 五关预检 → 轨迹开行 → 两阶段资源
        租约 → 子进程启动 → 在飞原子登记。

        返回 InFlightJob 条目；被五关预检或载荷校验拦截（已终结/已回队）
        时返回 None。
        """
        store = self._store
        if sched.runnable_idx is None:
            return None
        pending_dep_failure = (
            sched.pending_dep_failure if sched.kind == "dep_failed" else None
        )
        job_dict = store.state.pop_job(sched.runnable_idx)
        job = Job.from_dict(job_dict)
        uid = job.uid

        # 预检五关以调用顺序表达时序契约，每关返回 True=已处理。
        if self.dispatch_dedup(uid, job, job_dict):
            return None
        attempt_id, incarnation = self._open_attempt(job)
        if pending_dep_failure is not None:
            if self.dispatch_dep_failed(
                uid, job_dict, pending_dep_failure, attempt_id=attempt_id
            ):
                return None
        if job.task_type not in self._tasks:
            if self.dispatch_no_handler(
                uid, job_dict, job.task_type, attempt_id=attempt_id
            ):
                return None
        if self.dispatch_orphan_probe(uid, job_dict, attempt_id=attempt_id):
            return None
        if self.dispatch_stale_restore(uid, job, job_dict, attempt_id=attempt_id):
            return None

        try:
            return self._spawn_entry(uid, job, job_dict, attempt_id, incarnation)
        except _CommitCrashSignal:
            # _CommitCrashSignal 继承 BaseException——「commit 失败需崩溃」
            # 的信号不会被 except Exception 兜底误吞。re-raise 让它穿透到
            # 运行循环的崩溃处理分支（已 requeue 当前作业，不在此二次处理）。
            raise
        except AssertionError:
            # DEBUG 断言是引擎不变式破坏信号，不是作业坏输入——不得计入
            # 派发失败预算（3-strike 会把可正常执行的作业误送失败档案），
            # 原样穿透交运行循环崩溃网统一收尾。
            raise
        except KeyboardInterrupt:
            return self._handle_interrupt(uid, job_dict)
        except RateLimitUnavailable as e:
            return self._handle_rate_limit_defer(uid, job_dict, attempt_id, e)
        except Exception as e:
            return self._handle_dispatch_failure(uid, job_dict, attempt_id, e)

    # ── 预检五关（顺序即时序契约）────────────────────────────────────

    def dispatch_dedup(self, uid: str, job: Job, job_dict: dict) -> bool:
        """派发预检关 1——去重（is_known 命中 → rerun 策略 → 跳过/放行）。

        返回 True = 已处理（job 被跳过，跳过同样落轨迹行 skipped）；
        返回 False = 未命中或准入放行（调用方继续后续预检关）。统一
        is_known 谓词；rerun 策略豁免 every_run/on_failure/on_input_change
        的 wall/failed 命中重跑。
        """
        store = self._store
        if not store.state.is_known(uid):
            return False
        decision = self._rerun_policy.admit(job_dict, store.state)
        if decision.should_skip:
            attempt_id, _ = self._open_attempt(job)
            store.apply_skip(uid, job_dict=job_dict, attempt_id=attempt_id)
            return True
        if decision.should_run and (uid in store.state.wall_uids or uid in store.state.failed_uids):
            # 豁免登记与准入判定同源：pop_job 只按字面 rerun 键登记豁免，
            # 队列行无键而由动态兜底放行的重跑必须在此补登记，否则
            # in-flight 登记的全量互斥断言会击落本关放行的作业。
            store.state.mark_rerun_active(uid)
            # 不变式：准入层放行的重跑，其有效策略必须落为行内字面键——
            # 豁免事实随行持久，abort/retry/崩溃回滚的按字面键豁免重建
            # （requeue/clear_in_flight/replace_queue）才不丢失动态放行事实。
            job_dict["rerun"] = decision.effective_rerun
            # 激活推进：从 wall/failed 拦截点放行重跑 = 新激活代——
            # activation_no + 1、attempt_no 重置 1（本次执行是新激活的
            # 首次尝试，轨迹行按新代号落表）。
            job.activation_no = job.activation_no + 1
            job.attempt_no = 1
            job_dict["activation_no"] = job.activation_no
            job_dict["attempt_no"] = 1
        return False

    def dispatch_dep_failed(
        self,
        uid: str,
        job_dict: dict,
        pending_dep_failure: str,
        *,
        attempt_id: int | None = None,
    ) -> bool:
        """派发预检关 2——依赖失败（父任务已进失败档案 → 本任务依赖失败）。

        返回 True = 已处理（依赖失败直接 commit，含级联下游 + 钩子）；
        仅当 pending_dep_failure 非空时调用。commit 失败走 3-strike /
        崩溃路径。
        """
        if pending_dep_failure is None:
            return False
        logger.warning(f"SKIP: {uid} (Dependency {pending_dep_failure} failed)")
        fail_meta: dict[str, Any] = {
            "error": ERR_JOB_DEPENDENCY,
            "failed_dependency": pending_dep_failure,
        }
        # 依赖父失败的级联下游计入 cascade_failed 而非 failed——
        # 「真实业务失败率」统计不被级联稀释。
        self._reject_and_commit(
            uid, job_dict, fail_meta, count_as="cascade_failed", attempt_id=attempt_id
        )
        return True

    def dispatch_no_handler(
        self,
        uid: str,
        job_dict: dict,
        task_type: str,
        *,
        attempt_id: int | None = None,
    ) -> bool:
        """派发预检关 3——无对应 Task 注册（配置级错误 → fatal 进失败档案）。

        返回 True = 已处理（直接 commit 到失败档案并级联下游）；仅当
        task_type not in tasks 时调用。commit 失败走 3-strike / 崩溃路径。
        """
        logger.error(f"SKIP: {uid} (No handler for task_type '{task_type}')")
        fail_meta = {"error": ERR_NO_HANDLER, "fatal": True}
        self._reject_and_commit(uid, job_dict, fail_meta, attempt_id=attempt_id)
        return True

    def dispatch_orphan_probe(
        self,
        uid: str,
        job_dict: dict,
        *,
        attempt_id: int | None = None,
    ) -> bool:
        """派发预检关 4——孤儿探测（probe 先于 restore）。

        主进程仅探测（非阻塞试锁）。锁生命周期 = 执行体生命周期：主进程
        崩溃不释放 worker 的锁，探测失败 = 同 uid 孤儿执行体仍持锁 →
        lock_conflict 瞬态信号降级回队（零预算、队首重入、节奏经
        RequeuePolicy 唯一出口）本轮不派发，下轮孤儿死后正常执行。
        probe 必须先于 stale-restore 与声明清理——孤儿存活时提前
        return，绝不删孤儿实时声明。返回 True = 已处理（孤儿存活 defer）。
        """
        try:
            probe_ok = self._channel.probe_orphan_lock(uid)
        except OSError as e:
            # 锁文件环境故障（权限/目录占位/路径超长）与「锁被占」语义
            # 不同，但同属非作业自身故障：按孤儿锁冲突瞬态信号同构 defer
            # （零预算、降级写盘回队、零污染），绝不放大为整 run 崩溃。
            logger.warning(
                f"Lock probe environment fault for {uid} "
                f"(errno={getattr(e, 'errno', None)}): {e}; deferring."
            )
            probe_ok = False
        if not probe_ok:
            logger.warning(
                f"Deferring {uid}: orphan execution body still holds lock."
            )
            self._store.requeue_transient(
                job_dict,
                transient_kind="lock_conflict",
                attempt_id=attempt_id,
                error="orphan lock conflict",
            )
            return True
        return False

    def dispatch_stale_restore(
        self,
        uid: str,
        job: Job,
        job_dict: dict,
        *,
        attempt_id: int | None = None,
    ) -> bool:
        """派发预检关 5——陈旧结果认领与派发前残留清理（stale-restore）。

        1. 存在上一轮崩溃遗留的结果文件：经恢复编排层认领并按完成机器
           单一出口提交（返回 True，不派发子进程）。
        2. 无残留结果文件：清理已死孤儿残留声明与信号文件，准备派发新
           子进程（返回 False）。

        不变式：两条分支的收尾（完成机器清理 / PRE_SUBMIT 清理）都会
        未读删除信号文件，残留的 suspend 信号必须先排空应用。调用前提：
        关 4 探测已通过（锁空闲 ⇒ 无活跃追加写者），排空无损。本次派发
        已开的轨迹行随认领结果在完成机器侧收尾（attempt_id 透传）。
        """
        self._salvage_residue_signals(uid)
        if self._recovery.restore_stale_result(uid, job, job_dict, attempt_id=attempt_id):
            return True
        self._channel.cleanup_artifacts(uid, mode=ArtifactCleanupMode.PRE_SUBMIT)
        return False

    def _salvage_residue_signals(self, uid: str) -> None:
        """排空并应用单个 uid 跨 run 崩溃残留的 suspend 信号（先读后删）。"""
        try:
            signals = self._channel.drain_active_signals([uid])
        except Exception as e:
            logger.warning(f"Failed to salvage residue signals for {uid}: {e}")
            return
        apply_suspend_signals(
            signals, self._store.backend, self._resources, origin="residue of "
        )

    # ── 轨迹开行与 spawn ──────────────────────────────────────────────

    def _open_attempt(self, job: Job) -> tuple[int | None, str]:
        """派发即插 running 轨迹行，返回 (轨迹行 id, incarnation)。

        incarnation fencing 归属派发点：``{run_id}.{dispatch_seq}`` 生成
        于此并随轨迹落表。轨迹插入失败降级为 (None, incarnation)——
        attempts 是旁路观测面，观测缺口不得击落可正常执行的派发。
        """
        incarnation = f"{self._session.run_id}.{self._session.next_dispatch_seq()}"
        record = AttemptRecord(
            job_uid=job.uid,
            activation_no=job.activation_no,
            attempt_no=job.attempt_no,
            incarnation=incarnation,
            run_id=self._session.run_id,
            started_at=utc_now_iso(),
            outcome=ATTEMPT_RUNNING,
        )
        try:
            attempt_id = self._store.backend.append_attempt(record)
        except Exception as e:
            logger.warning(
                f"Attempt trace append degraded for {job.uid}: {e}; "
                f"dispatching without trace row"
            )
            return None, incarnation
        return attempt_id, incarnation

    def _spawn_entry(
        self,
        uid: str,
        job: Job,
        job_dict: dict,
        attempt_id: int | None,
        incarnation: str,
    ) -> InFlightJob | None:
        """两阶段资源租约 → 载荷校验 → 执行上下文组装 → spawn → 在飞登记。

        载荷校验失败不走子进程：释放租约后按派发拒绝语义直接 commit。
        spawn 已启动而后续步骤异常时（entry 尚未登记在飞），此处清理
        子进程句柄后原样上抛——崩溃信号与断言信号不清理（交由运行循环
        崩溃网/断言网处置，且子进程可能正在写结果文件）。
        """
        store = self._store
        handle = None
        lease = self._resources.reserve(job.task_type, job.resources, uid=uid)
        try:
            with lease:
                task = self._tasks.lookup(job.task_type)
                if task.payload_schema is not None:
                    errors = validate_payload_errors(job.payload, task.payload_schema)
                    if errors:
                        logger.error(f"Payload validation failed for {uid}: {errors}")
                        lease.release()
                        fail_meta: dict[str, Any] = {
                            "error": ERR_PAYLOAD_VALIDATION,
                            "details": errors,
                        }
                        self._reject_and_commit(
                            uid, job_dict, fail_meta, attempt_id=attempt_id
                        )
                        return None

                ctx = self._build_context(job, store.state)
                logger.info(f"RUN: {uid}")
                job_start = time.monotonic()
                handle = self._channel.spawn(WorkerLaunchSpec(
                    handler=task.handler,
                    job=job,
                    task_ctx=ctx,
                    incarnation=incarnation,
                    ipc_dir=self._ipc_dir,
                    timeout=job.timeout,
                    result_token=self._session.result_token,
                ))
                lease.claim()

                entry = InFlightJob(
                    uid=uid,
                    job_dict=job_dict,
                    job=job,
                    handle=handle,
                    job_start=job_start,
                    lease=lease,
                    attempt_id=attempt_id,
                )
                # 在 return 前原子登记到 in_flight 与 state 索引，避免时序真空
                self._in_flight.track(entry, state=store.state)
                return entry
        except (_CommitCrashSignal, AssertionError):
            raise
        except BaseException:
            if handle is not None:
                self._channel.cleanup_in_flight([handle])
            raise

    def _build_context(self, job: Job, state: "PipelineState") -> JobContext:
        """组装执行上下文（快照契约：注册表/启发式/资源名随 ctx 下发子进程）。"""
        # 注册表快照契约：per-pipeline 瞬态异常注册表快照随 ctx pickle
        # 下发——分类决策在子进程，注册表必须显式传递（不可依赖父进程
        # 作用域，更不存在模块级可变全局）。
        # 构造器声明的 fatal/transient 启发式元组同一契约随 ctx 下发
        # （取已解析形态，与注册表快照在 worker 侧并集生效）；元组与
        # 注册表同源于本 classifier 实例，父子两侧分类语义一致。
        # 资源名注册集快照随 ctx 下发——suspend_resource 对未注册名
        # fail-loud（typo 不静默失效）。
        # 不变式：wall/failed 活索引在快照边界冻结为 frozenset——
        # JobContext 持有的必须是派发时刻快照，绝不引用活集合。
        return JobContext(
            job,
            frozenset(state.wall_uids),
            frozenset(state.failed_uids),
            dict(state.cursors),
            output_root=self._output_root,
            ipc_dir=self._ipc_dir,
            transient_registry=self._classifier.snapshot(),
            fatal_exceptions=self._classifier.fatal_exceptions,
            transient_exceptions=self._classifier.transient_exceptions,
            resource_names=frozenset(self._resources),
        )

    # ── 拒绝与异常分支 ────────────────────────────────────────────────

    def _reject_and_commit(
        self,
        uid: str,
        job_dict: dict,
        meta: dict,
        *,
        count_as: str = "failed",
        attempt_id: int | None = None,
    ) -> None:
        """拒绝作业的统一出口：委托 StateStore.apply_failure 原子终态转移。

        四处拒绝路径（依赖失败 / 无 Task / 载荷校验失败 / 派发 3-strike）
        共用本方法，彻底收敛 commit + mark_failed_memory + cascade_fail +
        收尾钩子至 StateStore。正常返回 = 处理完成（commit 成功或 3-strike
        失败档案成功）；_CommitCrashSignal 穿透上抛 = 后端环境故障（由
        运行循环崩溃处理）。
        """
        try:
            outcome = self._store.apply_failure(
                uid, meta, job_dict=job_dict, count_as=count_as,
                cascade=True, attempt_id=attempt_id,
            )
            self._store.fire_attempt_finished(
                uid,
                outcome=AttemptFinish(
                    success=False, going_to_retry=False, meta=dict(outcome.error_meta)
                ),
            )
        except _JobTerminated:
            # 3-strike 失败档案成功——作业已终结（StateStore 内部已计入
            # 指标并触发钩子）
            return

    def _handle_interrupt(self, uid: str, job_dict: dict) -> None:
        """派发中断分支：entry 已登记交运行循环统一 abort，否则回队后上抛。"""
        logger.warning(f"Pipeline interrupted while dispatching {uid}.")
        if uid in self._in_flight:
            # entry 已注册到 in_flight：此处不清理/不 requeue/不 release，
            # 全部交给运行循环的中止收尾统一处理，避免对同一 entry 二次
            # 释放资源、二次 requeue 同一作业。
            raise KeyboardInterrupt
        self._store.state.requeue_jobs([job_dict], front=True)
        # 不在此保存队列：内存此刻缺其他 in-flight 作业，交给运行循环的
        # 崩溃安全保存合并磁盘真相后统一保存。
        raise KeyboardInterrupt

    def _handle_rate_limit_defer(
        self,
        uid: str,
        job_dict: dict,
        attempt_id: int | None,
        exc: RateLimitUnavailable,
    ) -> None:
        """限速二次检查失败分支：瞬态信号降级回队（零预算 + 零污染）。

        调度评估与预约之间应用的限速挂起（关 5 残留信号排空）使二次
        检查失败——瞬态等待信号：零预算 + 降级写盘回队 + 零污染，实际
        等待由资源挂起 TTL 承担，绝不计入 3-strike 崩溃计数。
        """
        logger.warning(
            f"Deferring {uid}: rate limit re-check failed at reserve ({exc})."
        )
        self._store.requeue_transient(
            job_dict,
            transient_kind="rate_limited",
            attempt_id=attempt_id,
            error=str(exc),
        )
        return None

    def _handle_dispatch_failure(
        self,
        uid: str,
        job_dict: dict,
        attempt_id: int | None,
        exc: Exception,
    ) -> None:
        """派发异常分支：3-strike 派发失败预算收敛，未达阈值回队后上抛。"""
        store = self._store
        logger.error(f"Error dispatching job {uid}: {exc}\n{traceback.format_exc()}")
        rt_state = JobRuntimeState.from_dict(job_dict.get("runtime"))
        failures = rt_state.record_dispatch_failure()
        job_dict["runtime"] = rt_state.to_dict()
        if failures >= self._commit_failure_threshold:
            logger.critical(
                f"Dispatch failed {failures} times for {uid} ({exc}); "
                f"treating as deterministic bad input (e.g. unpickleable "
                f"handler), sending to failure archive."
            )
            fail_meta = {
                "error": ERR_DISPATCH_FAILURE,
                "failures": failures,
                "detail": str(exc)[:200],
            }
            self._in_flight.settle(uid, state=store.state)
            self._reject_and_commit(uid, job_dict, fail_meta, attempt_id=attempt_id)
            return None
        self._in_flight.settle(uid, state=store.state)
        store.state.requeue_jobs([job_dict], front=True)
        store.finish_attempt(
            attempt_id, outcome=ATTEMPT_REQUEUED, error=f"dispatch failure: {exc}"
        )
        # 不在此保存队列：内存此刻缺其他 in-flight 作业，交给运行循环的
        # 崩溃安全保存合并磁盘真相后统一保存。
        raise


__all__ = [
    "DispatchKind",
    "DispatchMachine",
    "DispatchOutcome",
]
