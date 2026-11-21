"""v2 派发机器：四关预检、两阶段资源租约与子进程 spawn 编排。

预检顺序即契约（顺序即时序约束）：dep-failed → admit/dedup → no-handler →
pre-submit-cleanup。依赖以显式窄清单注入（无共享袋），经
``store`` 复用状态仓库深模块，不反向引用门面。

``session`` 直接使用内部 RunSession；``tasks`` 按具名 Protocol
``TaskSource`` 消费（TaskRegistry 自动满足）。``output_roots`` 与 config 声明同型
（``Path | Sequence[Path] | None``，多根输出场景）。

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
from dataclasses import dataclass, field
from typing import Any, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.state import PipelineState
    from ..models.task import Task
    from .admission import TriggerRegistry
    from .channel import ExecutionChannel
    from .completion import CompletionMachine
    from .in_flight import InFlightTracker
    from .resource import ResourceManager
    from .scheduler import JobScheduler
    from .store import StateStore
    from .session import RunSession

    class TaskSource(Protocol):
        """派发机器消费的任务规格来源契约（TaskRegistry 自动满足）。"""

        def lookup(self, task_type: str) -> "Task": ...

        def __contains__(self, task_type: object) -> bool: ...

from collections.abc import Mapping

from ..models.artifact import FileSpec
from ..models.context import JobContext
from ..models.job import Job
from .channel import ExecutionResult, WorkerLaunchSpec
from .launcher import _WorkerLaunchCancelled
from .in_flight import InFlightJob
from .scheduler import ScheduleResult, StandstillFacts
from .types import StopMode
from .wait import AUTHORIZATION_RECHECK_SECONDS
from ..utils.clock import utc_now_iso

logger = logging.getLogger("tasklite")


class DispatchKind(enum.Enum):
    """一次调度派发周期的结局分类（单一事实，无交叠布尔）。"""

    SPAWNED = "spawned"                    # 子进程已启动并登记在飞
    HANDLED_NO_SUBPROCESS = "handled_no_subprocess"  # 五关/校验拦截，无需子进程即终结
    AUTHORIZATION_PENDING = "authorization_pending"
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
        tasks: "TaskSource",
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
        self._authorization_pending: tuple[str | None, str, str] | None = None
        self._dispatch_start_lock = dispatch_start_lock or threading.RLock()

    # ── 扫描与派发接缝 ────────────────────────────────────────────────

    def _dispatch_allowed(self) -> bool:
        """新派发的唯一停止边界。"""
        return self._session.stop_mode is StopMode.NONE

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
        if not self._dispatch_allowed():
            self._authorization_pending = None
            return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)

        pending = self._authorization_pending
        if pending is not None:
            run_id, pending_uid, name = pending
            self._authorization_pending = None
            if run_id == self._session.run_id and pending_uid in store.state.queue_uids:
                pending_idx = next(
                    index for index, snapshot in enumerate(store.state.queue)
                    if Job.from_dict(snapshot).uid == pending_uid
                )
                try:
                    pending_files = self._resolve_job_files(
                        Job.from_dict(store.state.queue[pending_idx])
                    )
                    path = pending_files[name].path
                    assert path is not None
                except Exception as exc:
                    snapshot = store.state.pop_job(pending_idx)
                    self._reject_and_commit(
                        pending_uid, snapshot, {
                            "error_message": str(exc),
                            "exception": {
                                "type": type(exc).__name__, "message": str(exc),
                                "traceback": traceback.format_exc(),
                            },
                        },
                    )
                    return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)
                authorized = store.check_or_request_authorization(path)
                if not self._dispatch_allowed():
                    return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)
                if authorized is None:
                    self._authorization_pending = pending
                    return DispatchOutcome(
                        kind=DispatchKind.AUTHORIZATION_PENDING,
                        worker_wait=AUTHORIZATION_RECHECK_SECONDS,
                    )
                if authorized is False:
                    self._session.temporarily_exclude_chain(pending_uid, store.state)
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
        if self._authorization_pending:
            return DispatchOutcome(
                kind=DispatchKind.AUTHORIZATION_PENDING,
                worker_wait=AUTHORIZATION_RECHECK_SECONDS,
            )
        if entry is None:
            return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)
        return DispatchOutcome(kind=DispatchKind.SPAWNED, entry=entry)

    def dispatch_job(self, sched: ScheduleResult) -> InFlightJob | None:
        """统一派发单个作业：依赖失败处理 / 统一准入求值 → 出队 → running 编号 →
        两阶段资源租约 → 子进程启动 → 在飞原子登记。

        返回 InFlightJob 条目；被预检拦截（已终结/跳过/已回队）时返回 None。
        """
        store = self._store
        if sched.runnable_idx is None or not self._dispatch_allowed():
            return None

        # 1. 依赖终态分支：优先于去重/准入求值（失败或缺失阻断下游）。
        if sched.kind == "dep_failed":
            pending_dep_failure = sched.pending_dep_failure
            job_snapshot = store.state.pop_job(sched.runnable_idx)
            job = Job.from_dict(job_snapshot)
            job_uid = job.uid
            self.dispatch_dep_failed(
                job_uid, job_snapshot, pending_dep_failure
            )
            return None
        if sched.kind == "dep_missing":
            job_snapshot = store.state.pop_job(sched.runnable_idx)
            job_uid = Job.from_dict(job_snapshot).uid
            self.dispatch_dep_missing(
                job_uid,
                job_snapshot,
                sched.pending_dep_missing,
            )
            return None

        # 2. 候选就绪：求值前不 pop 丢队，异常原位留队
        peek_snapshot = store.state.queue[sched.runnable_idx]
        if peek_snapshot.get("trigger") is None:
            self._trigger_registry.normalize_job_snapshot(peek_snapshot, peek_snapshot["task_type"])
        job = Job.from_dict(peek_snapshot)
        job_uid = job.uid

        # 依赖就绪后、Trigger判断前解析预声明文件
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
                            "type": type(err).__name__, "message": str(err),
                            "traceback": traceback.format_exc(),
                        },
                    },
                )
                return None

        # 准入求值（仅显式 retry 登记的同 Run 候选跳过触发器）
        is_session_retry = self._session.is_retry_candidate(job_uid)
        if not is_session_retry:
            should_run = self._trigger_registry.admit(
                peek_snapshot, store.state, baseline_lookup=store.trusted_last_succeeded_at_us
            )
            if not should_run:
                job_snapshot = peek_snapshot
                store.apply_skip(job_uid, job_snapshot=job_snapshot)
                store.state.mark_trigger_skipped(job_uid)
                return None

        if job.task_type not in self._tasks:
            job_snapshot = store.state.pop_job(sched.runnable_idx)
            if self.dispatch_no_handler(
                job_uid, job_snapshot, job.task_type
            ):
                return None

        # 必要路径授权（未 running 不造 Attempt，授权拒绝本 Run 临时排除该作业及下游）
        for fname, fspec in resolved_files.items():
            if fspec.delete_existing or fspec.delete_on_success or fspec.delete_on_failure:
                assert fspec.path is not None
                authorized = store.check_or_request_authorization(fspec.path)
                if not self._dispatch_allowed():
                    return None
                if authorized is None:
                    self._authorization_pending = (self._session.run_id, job_uid, fname)
                    return None
                if authorized is False:
                    self._session.temporarily_exclude_chain(job_uid, store.state)
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
                {"error_message": 'Resource requirements cannot be satisfied', "error_details": eval_res.reason},
            )
            return None
        if not eval_res.is_available:
            return None

        if not self._dispatch_allowed():
            return None
        job_snapshot = store.state.pop_job(sched.runnable_idx)
        if not self._dispatch_allowed():
            store.state.requeue_jobs([job_snapshot], front=True)
            return None
        running_at = utc_now_iso()
        attempt_no = 1

        effective_run_id = self._session.run_id or "run"
        execution_id = f"{effective_run_id}.{self._session.next_dispatch_no()}"
        if not self._dispatch_allowed():
            store.state.requeue_jobs([job_snapshot], front=True)
            return None
        try:
            attempt_no = store.mark_job_running(
                job_uid,
                execution_id=execution_id,
                run_id=effective_run_id,
                started_at=running_at,
            )
            job._attempt_no = attempt_no
            job_snapshot["attempt_no"] = attempt_no
            job_snapshot["run_id"] = effective_run_id
            job_snapshot["started_at"] = running_at
        except Exception as e:
            logger.critical(
                f"Failed to mark job running for {job_uid}: {e}; "
                f"requeueing and aborting dispatch without starting handler."
            )
            store.state.requeue_jobs([job_snapshot], front=True)
            raise

        if not self._dispatch_allowed():
            self._rollback_unstarted_running(
                job_uid=job_uid,
                job_snapshot=job_snapshot,
                execution_id=execution_id,
                attempt_no=attempt_no,
            )
            return None

        # 逐项获取资源（串行连续获取，异常属 Run 级框架故障直接上抛）
        acquired = self._resources.acquire_all(job.task_type, job.resources, job_uid=job_uid)
        if not self._dispatch_allowed():
            self._resources.release_all(acquired)
            self._rollback_unstarted_running(
                job_uid=job_uid,
                job_snapshot=job_snapshot,
                execution_id=execution_id,
                attempt_no=attempt_no,
            )
            return None

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
            entry = InFlightJob(
                job_uid=job_uid,
                job_snapshot=job_snapshot,
                job=job,
                acquired=acquired,
                execution_id=execution_id,
                attempt_no=attempt_no,
                run_id=self._session.run_id or "run",
                active_files=dict(resolved_files),
                active_file_sources=dict(job.files),
            )
            self._in_flight.track(entry, state=store.state)
            self._completion.settle_reaped([(entry, ExecutionResult(
                message=f"Pre-deletion failed: {exc}",
                exception={
                    "type": type(exc).__name__, "message": str(exc),
                    "traceback": traceback.format_exc(),
                },
            ))])
            return None
        except BaseException:
            self._resources.release_all(acquired)
            raise

        if not self._dispatch_allowed():
            self._resources.release_all(acquired)
            self._rollback_unstarted_running(
                job_uid=job_uid,
                job_snapshot=job_snapshot,
                execution_id=execution_id,
                attempt_no=attempt_no,
            )
            return None

        try:
            return self._spawn_entry(
                job_uid,
                job,
                job_snapshot,
                execution_id,
                acquired,
                attempt_no=attempt_no,
                resolved_files=resolved_files,
            )
        except BaseException:
            self._resources.release_all(acquired)
            raise

    def _rollback_unstarted_running(
        self,
        *,
        job_uid: str,
        job_snapshot: dict,
        execution_id: str,
        attempt_no: int,
    ) -> None:
        """停止门越过 running 提交时，将未启动执行回退为 pending 且不写 Attempt。"""
        self._store.apply_pending(
            job_uid,
            job_snapshot,
            execution_id=execution_id,
            attempt_no=attempt_no,
            run_id=self._session.run_id or "run",
            started_at=None,
            error_message="stop requested before worker launch",
            record_attempt=False,
        )

    # ── 预检关卡 ──────────────────────────────────────────────────────

    def dispatch_dep_failed(
        self,
        job_uid: str,
        job_snapshot: dict,
        pending_dep_failure: str | None,
    ) -> bool:
        """派发预检关 2——依赖失败（父任务已进失败档案 → 本任务依赖失败）。

        返回 True = 已处理（依赖失败直接 commit，含级联下游 + 钩子）；
        仅当 pending_dep_failure 非空时调用。事务异常原样传播。
        """
        if pending_dep_failure is None:
            return False
        logger.warning(f"SKIP: {job_uid} (Dependency {pending_dep_failure} failed)")
        failure_metadata: dict[str, Any] = {
            "error_message": 'Dependency failed or has no record',
            "failed_dependency": pending_dep_failure,
        }
        # 依赖父失败的级联下游计入 cascade_failed 而非 failed——
        # 「真实业务失败率」统计不被级联稀释。
        self._reject_and_commit(
            job_uid, job_snapshot, failure_metadata
        )
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
            "error_message": 'Dependency failed or has no record',
            "failed_dependency": pending_dep_missing,
            "missing_dependency": pending_dep_missing,
        }
        self._reject_and_commit(
            job_uid,
            job_snapshot,
            failure_metadata,
        )
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
        failure_metadata = {"error_message": 'No registered handler for task type'}
        self._reject_and_commit(job_uid, job_snapshot, failure_metadata)
        return True

    # ── worker 启动 ──────────────────────────────────────────────

    def _spawn_entry(
        self,
        job_uid: str,
        job: Job,
        job_snapshot: dict,
        execution_id: str,
        acquired: list[tuple[str, float]],
        *,
        attempt_no: int = 1,
        resolved_files: dict[str, FileSpec] | None = None,
    ) -> InFlightJob | None:
        """载荷校验 → 锁外冷启动准备（停在放行门前）→ 微秒级锁内放行与在飞登记。

        Popen、双连接握手、进程树绑定及载荷预送均在 ``_dispatch_start_lock``
        之外完成，避免冷启动阻塞并发 ``stop()``；最终仅微秒级短持锁完成停机
        二次校验、发送放行门与原子在飞登记。任意未放行异常/取消路径显式调用
        ``abort_spawn`` 立即回收进程树与双工连接。
        """
        store = self._store
        effective_run_id = self._session.run_id or "run"
        prepared = None
        committed_handle = None
        try:
            task = self._tasks.lookup(job.task_type)
            ctx = self._build_context(job, store.state, files=resolved_files)
            if not self._dispatch_allowed():
                self._resources.release_all(acquired)
                self._rollback_unstarted_running(
                    job_uid=job_uid,
                    job_snapshot=job_snapshot,
                    execution_id=execution_id,
                    attempt_no=attempt_no,
                )
                return None
            logger.info(f"RUN: {job_uid}")
            if not self._dispatch_allowed():
                self._resources.release_all(acquired)
                self._rollback_unstarted_running(
                    job_uid=job_uid,
                    job_snapshot=job_snapshot,
                    execution_id=execution_id,
                    attempt_no=attempt_no,
                )
                return None

            spec = WorkerLaunchSpec(
                handler=task.handler,
                job=job,
                job_context=ctx,
                execution_id=execution_id,
                timeout_seconds=float(
                    job.timeout_seconds if job.timeout_seconds is not None else 3600.0
                ),
                run_id=effective_run_id,
                attempt_no=attempt_no,
            )

            # 阶段一（锁外）：冷启动子进程、握手与进程树绑定，停在 b"GO" 栅栏前
            prepared = self._channel.prepare_spawn(spec)

            # 阶段二（微秒级短持锁）：与 stop() 同步二次校验，放行并原子登记在飞
            with self._dispatch_start_lock:
                if not self._dispatch_allowed():
                    raise _WorkerLaunchCancelled(
                        f"stop requested before worker commit for {job_uid}"
                    )
                started_monotonic = time.monotonic()
                handle = self._channel.release_spawn(prepared)
                prepared = None
                committed_handle = handle
                entry = InFlightJob(
                    job_uid=job_uid,
                    job_snapshot=job_snapshot,
                    job=job,
                    handle=handle,
                    started_monotonic=started_monotonic,
                    acquired=acquired,
                    execution_id=execution_id,
                    attempt_no=attempt_no,
                    run_id=effective_run_id,
                    started_at=handle.started_at,
                    known_job_uids=frozenset(
                        store.state.queue_uids
                        | store.state.in_flight_uids
                        | store.state.success_uids
                        | store.state.failed_uids
                    ),
                    active_files=dict(resolved_files or {}),
                    active_file_sources=dict(job.files),
                )
                self._in_flight.track(entry, state=store.state)
                return entry
        except _WorkerLaunchCancelled:
            if prepared is not None:
                self._channel.abort_spawn(prepared)
            self._resources.release_all(acquired)
            self._rollback_unstarted_running(
                job_uid=job_uid,
                job_snapshot=job_snapshot,
                execution_id=execution_id,
                attempt_no=attempt_no,
            )
            return None
        except BaseException:
            if prepared is not None:
                self._channel.abort_spawn(prepared)
            if committed_handle is not None:
                self._channel.cleanup_in_flight([committed_handle])
            raise

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
            base_dir=self._trigger_registry.base_dir,
            known_job_uids=frozenset(
                state.queue_uids
                | state.in_flight_uids
                | state.success_uids
                | state.failed_uids
            ),
            artifact_descriptions={
                uid: state.successes[uid].get("artifacts", {})
                for uid in job.depends_on if uid in state.success_uids
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
            job_uid, meta, job_snapshot=job_snapshot, cascade=True,
        )


__all__ = [
    "DispatchKind",
    "DispatchMachine",
    "DispatchOutcome",
]
