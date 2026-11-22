"""v2 派发机器：预检、两阶段资源租约、阶段回滚守卫与子进程 spawn 编排。

预检顺序即契约（顺序即时序约束）：dep-failed/dep-missing → 预声明解析 →
admit/dedup → no-handler → 预授权 → 资源重检 → 落 running → 获取资源 →
前置清理 → 锁外冷启动 → 锁内微秒放行。依赖以显式窄清单注入，经 ``store``
复用状态仓库深模块，不反向引用门面。

进入 running 时分配编号，结算时追加 Attempt：execution_id
（``run_id.dispatch_no``）在本模块生成并随行落表——dispatch_no 每次派发递增，
同 uid 重试再派发也获得新 execution_id，与上次尝试的执行身份严格隔离。
"""

from __future__ import annotations

import enum
import logging
import threading
import time
import traceback
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.state import PipelineState
    from ..models.task import TaskRegistry
    from .admission import TriggerRegistry
    from .channel import ExecutionChannel, PreparedLaunch
    from .completion import CompletionMachine
    from .in_flight import InFlightTracker
    from .resource import ResourceManager
    from .scheduler import JobScheduler
    from .session import RunSession
    from .store import StateStore

from ..models.artifact import FileSpec
from ..models.context import JobContext
from ..models.job import Job
from ..utils.clock import utc_now_iso
from .authorization import AuthorizationCoordinator
from .channel import ExecutionResult, WorkerLaunchSpec
from .in_flight import InFlightJob
from .launcher import _WorkerLaunchCancelled
from .scheduler import ScheduleResult, StandstillFacts
from .types import AuthorizationStatus, JobHandle, StopMode

logger = logging.getLogger("tasklite")


class DispatchKind(enum.Enum):
    """一次调度派发周期的结局分类（单一事实，无交叠布尔）。"""

    SPAWNED = "spawned"                              # 子进程已启动并登记在飞
    HANDLED_NO_SUBPROCESS = "handled_no_subprocess"  # 预检/校验拦截，无需子进程即终结
    AUTHORIZATION_PENDING = "authorization_pending"  # 派发前路径授权待决
    WORKER_SATURATED = "worker_saturated"            # 工人槽位耗尽，退出填池等待
    NO_CANDIDATE = "no_candidate"                    # 队列无可运行候选（停摆事实随附）


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


class _DispatchStopped(Exception):
    """派发阶段内检测到停机请求，触发未启动状态回退的内部信号。"""


class _DispatchStageGuard:
    """派发阶段回滚守卫：按已越过阶段精确回退。

    - 停机路径（``rollback_for_stop``）：中止未放行启动、释放资源，若已落 running
      则回退 pending 且不写 Attempt，否则原位插回队首；
    - 故障路径（``cleanup_on_fault``）：回收未放行启动/已提交句柄/已占资源；未落
      running 前回队，已落 running 后保留磁盘 running 供下次启动恢复。
    """

    def __init__(
        self,
        machine: "DispatchMachine",
        *,
        job_uid: str,
        job_snapshot: dict[str, Any],
    ) -> None:
        self._machine = machine
        self.job_uid = job_uid
        self.job_snapshot = job_snapshot
        self.execution_id: str | None = None
        self.attempt_no: int | None = None
        self.acquired: list[tuple[str, float]] = []
        self.prepared: "PreparedLaunch | None" = None
        self.committed_handle: JobHandle | None = None
        self._finalized: bool = False

    def ensure_allowed(self) -> None:
        """停机语义屏障：若已请求停机则立即退出本次派发并触发阶段回退。"""
        if not self._machine._dispatch_allowed():
            raise _DispatchStopped

    def commit(self) -> None:
        """移交在飞追踪或完成机器接管，后续退出不再触发回滚。"""
        self._finalized = True

    def rollback_for_stop(self) -> None:
        """停机回退：按已越过阶段逆序清理并将未启动作业退回 pending（不写 Attempt）。"""
        if self._finalized:
            return
        self._finalized = True
        if self.prepared is not None:
            self._machine._channel.abort_spawn(self.prepared)
            self.prepared = None
        if self.acquired:
            acquired, self.acquired = self.acquired, []
            self._machine._resources.release_all(acquired, job_uid=self.job_uid)
        if self.execution_id is not None and self.attempt_no is not None:
            self._machine._store.apply_pending(
                self.job_uid,
                self.job_snapshot,
                execution_id=self.execution_id,
                attempt_no=self.attempt_no,
                run_id=self._machine._session.run_id or "run",
                started_at=None,
                error_message="stop requested before worker launch",
                record_attempt=False,
            )
        else:
            self._machine._store.state.requeue_jobs([self.job_snapshot], front=True)

    def cleanup_on_fault(self) -> None:
        """框架故障清理：回收启动/通道/资源；未入 running 前回队，已入 running 留待恢复。"""
        if self._finalized:
            return
        self._finalized = True
        if self.prepared is not None:
            self._machine._channel.abort_spawn(self.prepared)
            self.prepared = None
        if self.committed_handle is not None:
            self._machine._channel.cleanup_in_flight([self.committed_handle])
            self.committed_handle = None
        if self.acquired:
            acquired, self.acquired = self.acquired, []
            self._machine._resources.release_all(acquired, job_uid=self.job_uid)
        if self.execution_id is None:
            self._machine._store.state.requeue_jobs([self.job_snapshot], front=True)


class DispatchMachine:
    """派发预检 + 资源租约 + running 编号 + 子进程 spawn 的编排器。"""

    def __init__(
        self,
        *,
        store: "StateStore",
        scheduler: "JobScheduler",
        trigger_registry: "TriggerRegistry",
        resources: "ResourceManager",
        channel: "ExecutionChannel",
        in_flight: "InFlightTracker",
        session: "RunSession",
        completion: "CompletionMachine",
        tasks: "TaskRegistry",
        authorization: AuthorizationCoordinator | None = None,
        dispatch_start_lock: Any | None = None,
    ) -> None:
        self._store = store
        self._scheduler = scheduler
        self._trigger_registry = trigger_registry
        self._resources = resources
        self._channel = channel
        self._in_flight = in_flight
        self._session = session
        self._completion = completion
        self._tasks = tasks
        self._authorization = (
            authorization
            if authorization is not None
            else AuthorizationCoordinator(
                store=store,
                channel=channel,
                in_flight=in_flight,
                session=session,
            )
        )
        self._dispatch_start_lock = dispatch_start_lock or threading.RLock()

    # ── 扫描与派发接缝 ────────────────────────────────────────────────

    def _dispatch_allowed(self) -> bool:
        """新派发的唯一停止边界。"""
        return self._session.stop_mode is StopMode.NONE

    def dispatch_next(self) -> DispatchOutcome:
        """统一扫描与派发接缝：派发前授权复查 → 队列扫描 → 工人槽位预检 →
        预检关卡 → 资源租约 → 子进程派发。

        返回 DispatchOutcome（kind 单一判据）：
        - SPAWNED：entry 携带在飞条目，调用方继续收割；
        - HANDLED_NO_SUBPROCESS：作业被预检/校验拦截并已终结，调用方可继续填池；
        - AUTHORIZATION_PENDING：派发前授权待决，由 LoopFacts.authorization_wait 驱动复查；
        - WORKER_SATURATED：工人槽位耗尽（worker_wait 为建议等待），调用方退出填池；
        - NO_CANDIDATE：无可运行作业，停摆事实随 standstill 供仲裁。
        """
        store = self._store
        auth_status = self._authorization.poll_dispatch_authorization()
        if auth_status is AuthorizationStatus.STOPPED:
            return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)
        if auth_status is AuthorizationStatus.PENDING:
            return DispatchOutcome(kind=DispatchKind.AUTHORIZATION_PENDING)
        if auth_status in (
            AuthorizationStatus.DENIED,
            AuthorizationStatus.PREPARATION_FAILED,
        ):
            return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)

        sched = self._scheduler.scan_next_runnable(store.state, store.state.in_flight_uids)
        if sched.runnable_idx is None:
            return DispatchOutcome(
                kind=DispatchKind.NO_CANDIDATE,
                standstill=sched.standstill_facts(),
            )

        if sched.kind not in ("dep_failed", "dep_missing"):
            ok, worker_wait = self._resources.can_acquire_worker(1.0)
            if not ok:
                return DispatchOutcome(
                    kind=DispatchKind.WORKER_SATURATED,
                    worker_wait=worker_wait,
                    standstill=StandstillFacts(min_wait=0.0),
                )

        if not self._dispatch_allowed():
            return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)
        entry = self.dispatch_job(sched)
        if self._authorization.has_dispatch_pending:
            return DispatchOutcome(kind=DispatchKind.AUTHORIZATION_PENDING)
        if entry is None:
            return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)
        return DispatchOutcome(kind=DispatchKind.SPAWNED, entry=entry)

    def dispatch_job(self, sched: ScheduleResult) -> InFlightJob | None:
        """统一派发单个作业：依赖失败处理 / 统一准入求值 → 出队 → running 编号 →
        两阶段资源租约 → 前置清理 → 锁外冷启动 → 锁内放行与在飞原子登记。

        返回 InFlightJob 条目；被预检拦截（已终结/跳过/已回队）时返回 None。
        """
        store = self._store
        if sched.runnable_idx is None or not self._dispatch_allowed():
            return None

        # 1. 依赖终态分支：优先于去重/准入求值（失败或缺失阻断下游）。
        if sched.kind == "dep_failed":
            job_snapshot = store.state.pop_job(sched.runnable_idx)
            job_uid = Job.from_dict(job_snapshot).uid
            self.dispatch_dep_failed(job_uid, job_snapshot, sched.pending_dep_failure)
            return None
        if sched.kind == "dep_missing":
            job_snapshot = store.state.pop_job(sched.runnable_idx)
            job_uid = Job.from_dict(job_snapshot).uid
            self.dispatch_dep_missing(job_uid, job_snapshot, sched.pending_dep_missing)
            return None

        # 2. 候选就绪：求值前不 pop 丢队，异常原位留队
        peek_snapshot = store.state.queue[sched.runnable_idx]
        if peek_snapshot.get("trigger") is None:
            store.finalize_intake_snapshot(peek_snapshot, peek_snapshot["task_type"])
        job = Job.from_dict(peek_snapshot)
        job_uid = job.uid

        # 依赖就绪后、Trigger 判断前解析预声明文件
        resolved_files: dict[str, FileSpec] = {}
        if job.files:
            try:
                resolved_files = self._resolve_job_files(job)
            except Exception as err:
                job_snapshot = store.state.pop_job(sched.runnable_idx)
                self._reject_and_commit(
                    job_uid,
                    job_snapshot,
                    {
                        "error_message": str(err),
                        "exception": {
                            "type": type(err).__name__,
                            "message": str(err),
                            "traceback": traceback.format_exc(),
                        },
                    },
                )
                return None

        # 准入求值（仅显式 retry 登记的同 Run 候选跳过触发器）
        if not self._session.is_retry_candidate(job_uid):
            should_run = self._trigger_registry.admit(
                peek_snapshot, store.state, baseline_lookup=store.trusted_last_succeeded_at_us
            )
            if not should_run:
                store.apply_skip(job_uid, job_snapshot=peek_snapshot)
                return None

        if job.task_type not in self._tasks:
            job_snapshot = store.state.pop_job(sched.runnable_idx)
            if self.dispatch_no_handler(job_uid, job_snapshot, job.task_type):
                return None

        # 必要路径授权（未 running 不造 Attempt，授权拒绝本 Run 临时排除该作业及下游）
        auth_status = self._authorization.authorize_dispatch_files(job_uid, resolved_files)
        if auth_status is not AuthorizationStatus.APPROVED:
            return None

        # 再次检查全部资源（授权后的全部资源检查到连续获取完成之间，不插入其他分配或回收）
        eval_res = self._resources.evaluate(job.task_type, job.resources)
        if not self._dispatch_allowed():
            return None
        if eval_res.is_impossible:
            job_snapshot = store.state.pop_job(sched.runnable_idx)
            self._reject_and_commit(
                job_uid,
                job_snapshot,
                {
                    "error_message": "Resource requirements cannot be satisfied",
                    "error_details": eval_res.reason,
                },
            )
            return None
        if not eval_res.is_available:
            return None

        if not self._dispatch_allowed():
            return None
        job_snapshot = store.state.pop_job(sched.runnable_idx)
        guard = _DispatchStageGuard(self, job_uid=job_uid, job_snapshot=job_snapshot)
        try:
            # 屏障 1：出队后、落 running 前
            guard.ensure_allowed()

            running_at = utc_now_iso()
            effective_run_id = self._session.run_id or "run"
            execution_id = f"{effective_run_id}.{self._session.next_dispatch_no()}"
            try:
                attempt_no = store.mark_job_running(
                    job_uid,
                    execution_id=execution_id,
                    run_id=effective_run_id,
                    started_at=running_at,
                )
            except Exception as e:
                logger.critical(
                    f"Failed to mark job running for {job_uid}: {e}; "
                    f"requeueing and aborting dispatch without starting handler."
                )
                raise
            guard.execution_id = execution_id
            guard.attempt_no = attempt_no
            job._attempt_no = attempt_no
            job_snapshot["attempt_no"] = attempt_no
            job_snapshot["run_id"] = effective_run_id
            job_snapshot["started_at"] = running_at

            # 屏障 2：落 running 后、获取资源前
            guard.ensure_allowed()

            # 逐项获取资源（串行连续获取，异常属 Run 级框架故障直接上抛）
            guard.acquired = self._resources.acquire_all(
                job.task_type, job.resources, job_uid=job_uid
            )

            # 屏障 3：获取资源后、前置删除与启动准备前
            guard.ensure_allowed()

            policy = self._store.path_policy
            authorize_cb = self._store.backend.is_path_authorized
            try:
                for fspec in resolved_files.values():
                    if fspec.delete_existing and fspec.path:
                        policy.delete(
                            fspec.path,
                            allow_broken_leaf_symlink=True,
                            authorize=authorize_cb,
                        )
            except (OSError, ValueError, NotImplementedError) as exc:
                acquired = guard.acquired
                guard.acquired = []
                guard.commit()
                entry = InFlightJob(
                    job_uid=job_uid,
                    job_snapshot=job_snapshot,
                    job=job,
                    acquired=acquired,
                    execution_id=execution_id,
                    attempt_no=attempt_no,
                    run_id=effective_run_id,
                    active_files=dict(resolved_files),
                    active_file_sources=dict(job.files),
                )
                self._in_flight.track(entry, state=store.state)
                self._completion.settle_reaped(
                    [
                        (
                            entry,
                            ExecutionResult(
                                message=f"Pre-deletion failed: {exc}",
                                exception={
                                    "type": type(exc).__name__,
                                    "message": str(exc),
                                    "traceback": traceback.format_exc(),
                                },
                            ),
                        )
                    ]
                )
                return None

            return self._spawn_entry(
                guard,
                job=job,
                effective_run_id=effective_run_id,
                resolved_files=resolved_files,
            )
        except (_DispatchStopped, _WorkerLaunchCancelled):
            guard.rollback_for_stop()
            return None
        except BaseException:
            guard.cleanup_on_fault()
            raise

    # ── 预检关卡 ──────────────────────────────────────────────────────

    def dispatch_dep_failed(
        self,
        job_uid: str,
        job_snapshot: dict,
        pending_dep_failure: str | None,
    ) -> bool:
        """派发预检关 2——依赖失败（父任务已进失败档案 → 本任务依赖失败）。

        返回 True = 已处理（依赖失败直接 commit，含级联下游）；
        仅当 pending_dep_failure 非空时调用。事务异常原样传播。
        """
        if pending_dep_failure is None:
            return False
        logger.warning(f"SKIP: {job_uid} (Dependency {pending_dep_failure} failed)")
        failure_metadata: dict[str, Any] = {
            "error_message": "Dependency failed or has no record",
            "failed_dependency": pending_dep_failure,
        }
        self._reject_and_commit(job_uid, job_snapshot, failure_metadata)
        return True

    def dispatch_dep_missing(
        self,
        job_uid: str,
        job_snapshot: dict,
        pending_dep_missing: str | None,
    ) -> bool:
        """依赖记录缺失时直接结算失败并级联下游，不启动 worker。"""
        if pending_dep_missing is None:
            return False
        logger.warning(
            f"SKIP: {job_uid} (Dependency {pending_dep_missing} has no record)"
        )
        failure_metadata: dict[str, Any] = {
            "error_message": "Dependency failed or has no record",
            "failed_dependency": pending_dep_missing,
            "missing_dependency": pending_dep_missing,
        }
        self._reject_and_commit(job_uid, job_snapshot, failure_metadata)
        return True

    def dispatch_no_handler(
        self,
        job_uid: str,
        job_snapshot: dict,
        task_type: str,
    ) -> bool:
        """派发预检关 3——无对应 Task 注册（配置缺失直接进入失败档案）。

        返回 True = 已处理（直接 commit 到失败档案并级联下游）；仅当
        task_type not in tasks 时调用。事务异常原样传播。
        """
        logger.error(f"SKIP: {job_uid} (No handler for task_type '{task_type}')")
        failure_metadata = {"error_message": "No registered handler for task type"}
        self._reject_and_commit(job_uid, job_snapshot, failure_metadata)
        return True

    # ── worker 启动 ──────────────────────────────────────────────

    def _spawn_entry(
        self,
        guard: _DispatchStageGuard,
        *,
        job: Job,
        effective_run_id: str,
        resolved_files: dict[str, FileSpec],
    ) -> InFlightJob:
        """锁外冷启动准备（停在放行门前）→ 微秒级锁内放行与在飞原子登记。"""
        store = self._store
        assert guard.execution_id is not None and guard.attempt_no is not None
        task = self._tasks.lookup(job.task_type)
        ctx = self._build_context(job, store.state, files=resolved_files)
        logger.info(f"RUN: {guard.job_uid}")
        guard.ensure_allowed()

        spec = WorkerLaunchSpec(
            handler=task.handler,
            job=job,
            job_context=ctx,
            execution_id=guard.execution_id,
            timeout_seconds=float(
                job.timeout_seconds if job.timeout_seconds is not None else 3600.0
            ),
            run_id=effective_run_id,
            attempt_no=guard.attempt_no,
        )

        # 阶段一（锁外）：冷启动子进程、握手与进程树绑定，停在 b"GO" 栅栏前
        guard.prepared = self._channel.prepare_spawn(spec)

        # 屏障 4（微秒级短持锁）：与 stop() 同步二次校验，放行并原子登记在飞
        with self._dispatch_start_lock:
            guard.ensure_allowed()
            started_monotonic = time.monotonic()
            handle = self._channel.release_spawn(guard.prepared)
            guard.prepared = None
            guard.committed_handle = handle
            entry = InFlightJob(
                job_uid=guard.job_uid,
                job_snapshot=guard.job_snapshot,
                job=job,
                handle=handle,
                started_monotonic=started_monotonic,
                acquired=guard.acquired,
                execution_id=guard.execution_id,
                attempt_no=guard.attempt_no,
                run_id=effective_run_id,
                started_at=handle.started_at,
                known_job_uids=frozenset(
                    store.state.queue_uids
                    | store.state.in_flight_uids
                    | store.state.success_uids
                    | store.state.failed_uids
                ),
                active_files=dict(resolved_files),
                active_file_sources=dict(job.files),
            )
            self._in_flight.track(entry, state=store.state)
            guard.commit()
            return entry

    def _build_context(
        self,
        job: Job,
        state: "PipelineState",
        files: Mapping[str, FileSpec] | None = None,
    ) -> JobContext:
        """组装执行上下文（已完成状态快照随 ctx 下发子进程）。"""
        # 不变式：successes/failed 活索引在快照边界冻结为 frozenset——
        # JobContext 持有的必须是派发时刻快照，绝不引用活集合。
        return JobContext(
            job,
            frozenset(state.success_uids),
            frozenset(state.failed_uids),
            dict(state.cursors),
            protected_roots=self._store.path_policy.protected_paths,
            base_dir=self._store.base_dir,
            known_job_uids=frozenset(
                state.queue_uids
                | state.in_flight_uids
                | state.success_uids
                | state.failed_uids
            ),
            artifact_descriptions={
                uid: state.successes[uid].get("artifacts", {})
                for uid in job.depends_on
                if uid in state.success_uids
            },
            files=files,
        )

    def _resolve_job_files(self, job: Job) -> dict[str, FileSpec]:
        """准入前基于统一 PathPolicy 单次原子解析预声明文件。"""
        return {
            name: self._store.resolve_file_spec(spec, depends_on=job.depends_on)
            for name, spec in job.files.items()
        }

    # ── 拒绝与异常分支 ────────────────────────────────────────────────

    def _reject_and_commit(
        self,
        job_uid: str,
        job_snapshot: dict,
        meta: dict,
    ) -> None:
        """执行前拒绝统一登记失败；事务异常原样传播。"""
        self._store.apply_failure(
            job_uid,
            meta,
            job_snapshot=job_snapshot,
            cascade=True,
        )


__all__ = [
    "DispatchKind",
    "DispatchMachine",
    "DispatchOutcome",
]
