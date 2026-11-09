"""v2 派发机器：四关预检、两阶段资源租约与子进程 spawn 编排。

预检顺序即契约（顺序即时序约束）：dep-failed → admit/dedup → no-handler →
pre-submit-cleanup。依赖以显式窄清单注入（无共享袋），经
``store`` 复用状态仓库深模块，不反向引用门面。

``session`` 与 ``tasks`` 按具名 Protocol 消费（装配契约见本模块的
``DispatchSession`` 与 ``TaskSource``：RunSession / TaskRegistry 真实
类型自动满足）；``output_roots`` 与 config 声明同型
（``Path | Sequence[Path] | None``，多根输出场景）。

派发即插 attempts 轨迹行（outcome=running）：execution_id
（``run_id.dispatch_no``）在本模块生成并随行落表——dispatch_no 每次派发递增，
同 uid 重试再派发也获得新 execution_id，与上次尝试的执行身份严格隔离。
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
    from .admission import TriggerRegistry
    from .channel import ExecutionChannel
    from .errorclass import ErrorClassifier
    from .in_flight import InFlightTracker
    from .recovery import RecoveryOrchestrator
    from .resource import ResourceManager
    from .scheduler import JobScheduler
    from .store import StateStore

    class DispatchSession(Protocol):
        """派发/完成机器消费的运行会话装配契约（RunSession 自动满足）。

        ``run_id`` 是 execution_id 与 successes meta 的 run 身份事实源；
        ``next_dispatch_no`` 为 execution_id fencing 提供单调递增序号。
        """

        run_id: str | None
        stop_mode: StopMode

        def next_dispatch_no(self) -> int: ...

    class TaskSource(Protocol):
        """派发机器消费的任务规格来源契约（TaskRegistry 自动满足）。"""

        def lookup(self, task_type: str) -> "Task": ...

        def __contains__(self, task_type: object) -> bool: ...

import os
from collections.abc import Mapping, Sequence
from pathlib import Path

from ..backend.sqlite_backend import TARGET_SCHEMA_VERSION
from ..exceptions import TaskLiteError, _CommitCrashSignal, _JobTerminated
from ..models.artifact import ArtifactDeclaration, FileSpec
from ..models.attempt import ATTEMPT_REQUEUED
from ..models.context import JobContext
from ..models.job import Job, JobRuntimeState
from ..utils.artifact import ArtifactPathPolicy, _execute_pre_deletes
from .channel import WorkerLaunchSpec
from .errorclass import (
    ERR_DISPATCH_FAILURE,
    ERR_JOB_DEPENDENCY,
    ERR_NO_HANDLER,
    ERR_RESOURCE_DEADLOCK,
)
from .in_flight import InFlightJob
from .scheduler import ScheduleResult, StandstillFacts
from .types import AttemptOutcome, StopMode
from .wait import IN_FLIGHT_POLL_SECONDS
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
    """派发预检 + 资源租约 + 轨迹开行 + 子进程 spawn 的编排器。"""

    def __init__(
        self,
        *,
        store: "StateStore",
        scheduler: "JobScheduler",
        trigger_registry: "TriggerRegistry",
        resources: "ResourceManager",
        channel: "ExecutionChannel",
        in_flight: "InFlightTracker",
        session: "DispatchSession",
        recovery: "RecoveryOrchestrator",
        tasks: "TaskSource",
        output_roots: "Path | Sequence[Path] | None",
        ipc_dir: str | None = None,
        commit_failure_threshold: int = 10,
        protected_roots: tuple[Path, ...] = (),
        classifier: "ErrorClassifier | None" = None,
    ) -> None:
        self._store = store
        self._scheduler = scheduler
        self._trigger_registry = trigger_registry
        self._resources = resources
        self._channel = channel
        self._in_flight = in_flight
        self._session = session
        self._recovery = recovery
        self._tasks = tasks
        self._classifier = classifier
        self._output_roots = output_roots
        self._ipc_dir = ipc_dir
        self._protected_roots = protected_roots
        self._commit_failure_threshold = commit_failure_threshold
        self._authorization_pending: tuple[str | None, str, str] | None = None

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
            run_id, pending_uid, path = pending
            self._authorization_pending = None
            if run_id == self._session.run_id and pending_uid in store.state.queue_uids:
                authorized = store.check_or_request_authorization(path)
                if not self._dispatch_allowed():
                    return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)
                if authorized is None:
                    self._authorization_pending = pending
                    return DispatchOutcome(
                        kind=DispatchKind.AUTHORIZATION_PENDING,
                        worker_wait=IN_FLIGHT_POLL_SECONDS,
                    )
                if authorized is False:
                    exclude_fn = getattr(self._session, "temporarily_exclude_chain", None)
                    if callable(exclude_fn):
                        exclude_fn(pending_uid)
                    return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)

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

        if not self._dispatch_allowed():
            return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)
        entry = self.dispatch_job(sched)
        if self._authorization_pending:
            return DispatchOutcome(
                kind=DispatchKind.AUTHORIZATION_PENDING,
                worker_wait=IN_FLIGHT_POLL_SECONDS,
            )
        if entry is None:
            return DispatchOutcome(kind=DispatchKind.HANDLED_NO_SUBPROCESS)
        return DispatchOutcome(kind=DispatchKind.SPAWNED, entry=entry)

    def dispatch_job(self, sched: ScheduleResult) -> InFlightJob | None:
        """统一派发单个作业：依赖失败处理 / 统一准入求值 → 出队 → 轨迹开行 →
        两阶段资源租约 → 子进程启动 → 在飞原子登记。

        返回 InFlightJob 条目；被预检拦截（已终结/跳过/已回队）时返回 None。
        """
        store = self._store
        if sched.runnable_idx is None or not self._dispatch_allowed():
            return None

        # 1. 依赖已失败分支：优先于去重/准入求值（上游失败阻断下游，不得借旧成功跳过）
        if sched.kind == "dep_failed":
            pending_dep_failure = sched.pending_dep_failure
            job_snapshot = store.state.pop_job(sched.runnable_idx)
            job = Job.from_dict(job_snapshot)
            job_uid = job.uid
            attempt_id = None
            if store.backend.schema_version < TARGET_SCHEMA_VERSION:
                try:
                    attempt_id, _ = self._open_attempt(job)
                except Exception as e:
                    logger.warning(
                        f"Attempt trace append degraded for dependency failed {job_uid}: {e}"
                    )
                    attempt_id = None
            self.dispatch_dep_failed(
                job_uid, job_snapshot, pending_dep_failure, attempt_id=attempt_id
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
                    {"error_message": str(err)},
                    count_as="failed",
                    attempt_id=None,
                )
                return None

        # 准入求值（同 Run 内由 TaskResult.retry 产生的再次尝试不重复求值；新 Run 重新求值）
        is_session_retry = (
            getattr(self._session, "get_retry_count", lambda _: 0)(job_uid) > 0
            or (store.backend.schema_version < TARGET_SCHEMA_VERSION and job.attempt_no > 1)
        )
        if not is_session_retry:
            should_run = self._trigger_registry.admit(
                peek_snapshot, store.state, baseline_lookup=store.trusted_last_succeeded_at_us
            )
            if not should_run:
                job_snapshot = store.state.pop_job(sched.runnable_idx)
                attempt_id = None
                if store.backend.schema_version < TARGET_SCHEMA_VERSION:
                    try:
                        attempt_id, _ = self._open_attempt(job)
                    except Exception as e:
                        logger.warning(
                            f"Attempt trace append degraded for skipped {job_uid}: {e}"
                        )
                        attempt_id = None
                store.apply_skip(job_uid, job_snapshot=job_snapshot, attempt_id=attempt_id)
                store.state.mark_trigger_skipped(job_uid)
                return None

            if store.backend.schema_version < TARGET_SCHEMA_VERSION and job.attempt_no <= 1:
                job.activation_no = max(
                    (record.activation_no for record in store.backend.load_attempts(job_uid)),
                    default=0,
                ) + 1
                peek_snapshot["activation_no"] = job.activation_no

        if job.task_type not in self._tasks:
            job_snapshot = store.state.pop_job(sched.runnable_idx)
            attempt_id = None
            if store.backend.schema_version < TARGET_SCHEMA_VERSION:
                try:
                    attempt_id, _ = self._open_attempt(job)
                except Exception as e:
                    logger.warning(
                        f"Attempt trace append degraded for missing handler {job_uid}: {e}"
                    )
                    attempt_id = None
            if self.dispatch_no_handler(
                job_uid, job_snapshot, job.task_type, attempt_id=attempt_id
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
                    self._authorization_pending = (self._session.run_id, job_uid, fspec.path)
                    return None
                if authorized is False:
                    exclude_fn = getattr(self._session, "temporarily_exclude_chain", None)
                    if callable(exclude_fn):
                        exclude_fn(job_uid)
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
                {"error_message": ERR_RESOURCE_DEADLOCK, "error_details": eval_res.reason},
                count_as="failed",
                attempt_id=None,
            )
            return None
        if not eval_res.is_available:
            return None

        job_snapshot = store.state.pop_job(sched.runnable_idx)
        if store.backend.schema_version < TARGET_SCHEMA_VERSION:
            job_snapshot["activation_no"] = job.activation_no
        started_at = utc_now_iso()
        attempt_id = None
        attempt_no = 1

        if store.backend.schema_version >= TARGET_SCHEMA_VERSION:
            execution_id = f"{self._session.run_id}.{self._session.next_dispatch_no()}"
            try:
                attempt_no = store.mark_job_running(
                    job_uid, run_id=self._session.run_id or "run", started_at=started_at
                )
                job.attempt_no = attempt_no
                job_snapshot["attempt_no"] = attempt_no
                job_snapshot["run_id"] = self._session.run_id
                job_snapshot["started_at"] = started_at
            except (_CommitCrashSignal, Exception) as e:
                logger.critical(
                    f"Failed to mark job running for {job_uid}: {e}; "
                    f"requeueing and aborting dispatch without starting handler."
                )
                store.state.requeue_jobs([job_snapshot], front=True)
                if isinstance(e, _CommitCrashSignal):
                    raise
                raise _CommitCrashSignal(
                    f"Failed to mark job running for {job_uid} ({e}). "
                    f"Job preserved in queue without starting handler."
                ) from e
        else:
            try:
                attempt_id, execution_id = self._open_attempt(job)
            except Exception as e:
                logger.critical(
                    f"Failed to open attempt row for {job_uid}: {e}; "
                    f"requeueing and aborting dispatch without starting handler."
                )
                store.state.requeue_jobs([job_snapshot], front=True)
                raise _CommitCrashSignal(
                    f"Failed to append attempt row for {job_uid} ({e}). "
                    f"Job preserved in queue without starting handler."
                ) from e
            except BaseException:
                store.state.requeue_jobs([job_snapshot], front=True)
                raise

        # 逐项获取资源（串行连续获取，异常属 Run 级框架故障直接上抛）
        acquired = self._resources.acquire_all(job.task_type, job.resources, job_uid=job_uid)

        pre_deletes = [
            ArtifactDeclaration(fspec.path, delete_existing=True)
            for fspec in resolved_files.values()
            if fspec.delete_existing and fspec.path
        ]
        policy = ArtifactPathPolicy(
            output_roots=self._output_roots,
            protected_roots=self._protected_roots,
        )
        try:
            _execute_pre_deletes(policy, pre_deletes)
        except (OSError, ValueError, NotImplementedError) as exc:
            self._resources.release_all(acquired)
            message = f"Pre-deletion failed: {exc}"
            exception = {
                "type": type(exc).__name__, "message": str(exc),
                "traceback": traceback.format_exc(),
            }
            self._store.apply_failure(
                job_uid,
                {"error_message": message, "exception": exception},
                job_snapshot=job_snapshot,
                cascade=True,
                attempt_no=attempt_no,
                run_id=self._session.run_id or "run",
                started_at=started_at,
                files={k: v.to_dict() for k, v in resolved_files.items()},
                result={"message": message, "exception": exception},
            )
            return None

        try:
            return self._spawn_entry(
                job_uid,
                job,
                job_snapshot,
                attempt_id,
                execution_id,
                acquired,
                attempt_no=attempt_no,
                started_at=started_at,
                resolved_files=resolved_files,
            )
        except _CommitCrashSignal:
            self._resources.release_all(acquired)
            raise
        except AssertionError:
            self._resources.release_all(acquired)
            raise
        except KeyboardInterrupt:
            self._resources.release_all(acquired)
            self._handle_interrupt(job_uid, job_snapshot)
            return None
        except Exception as e:
            self._resources.release_all(acquired)
            if store.backend.schema_version >= TARGET_SCHEMA_VERSION:
                raise
            self._handle_dispatch_failure(job_uid, job_snapshot, attempt_id, e)
            return None

    # ── 预检关卡 ──────────────────────────────────────────────────────

    def dispatch_dep_failed(
        self,
        job_uid: str,
        job_snapshot: dict,
        pending_dep_failure: str | None,
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
        logger.warning(f"SKIP: {job_uid} (Dependency {pending_dep_failure} failed)")
        failure_metadata: dict[str, Any] = {
            "error_message": ERR_JOB_DEPENDENCY,
            "failed_dependency": pending_dep_failure,
        }
        # 依赖父失败的级联下游计入 cascade_failed 而非 failed——
        # 「真实业务失败率」统计不被级联稀释。
        self._reject_and_commit(
            job_uid, job_snapshot, failure_metadata, count_as="cascade_failed", attempt_id=attempt_id
        )
        return True

    def dispatch_no_handler(
        self,
        job_uid: str,
        job_snapshot: dict,
        task_type: str,
        *,
        attempt_id: int | None = None,
    ) -> bool:
        """派发预检关 3——无对应 Task 注册（配置级错误 → fatal 进失败档案）。

        返回 True = 已处理（直接 commit 到失败档案并级联下游）；仅当
        task_type not in tasks 时调用。commit 失败走 3-strike / 崩溃路径。
        """
        logger.error(f"SKIP: {job_uid} (No handler for task_type '{task_type}')")
        failure_metadata = {"error_message": ERR_NO_HANDLER, "is_fatal": True}
        self._reject_and_commit(job_uid, job_snapshot, failure_metadata, attempt_id=attempt_id)
        return True

    # ── 轨迹开行与 spawn ──────────────────────────────────────────────

    def _drain_signals_before_reserve(self, job_uid: str) -> None:
        """在 reserve 二次检查前排空在途 worker 的挂起信号。"""
        self._recovery.apply_pending_signals(job_uid)

    def _open_attempt(self, job: Job) -> tuple[int | None, str]:
        """派发即插 running 轨迹行，返回 (轨迹行 id, execution_id)。

        execution_id fencing 归属派发点：``{run_id}.{dispatch_no}`` 生成
        于此并随轨迹落表；开行失败由调用方捕获以保队并中止派发。
        """
        execution_id = f"{self._session.run_id}.{self._session.next_dispatch_no()}"
        attempt_id = self._store.record_attempt_dispatch(
            job,
            execution_id=execution_id,
            run_id=self._session.run_id,
        )
        return attempt_id, execution_id

    def _spawn_entry(
        self,
        job_uid: str,
        job: Job,
        job_snapshot: dict,
        attempt_id: int | None,
        execution_id: str,
        acquired: list[tuple[str, float]],
        *,
        attempt_no: int = 1,
        started_at: str | None = None,
        resolved_files: dict[str, FileSpec] | None = None,
    ) -> InFlightJob | None:
        """载荷校验 → 执行上下文组装 → spawn → 在飞登记。

        spawn 已启动而后续步骤异常时（entry 尚未登记在飞），此处清理
        子进程句柄后原样上抛——崩溃信号与断言信号不清理（交由运行循环
        崩溃网/断言网处置）。
        """
        store = self._store
        handle = None
        try:
            task = self._tasks.lookup(job.task_type)
            ctx = self._build_context(job, store.state, files=resolved_files)
            logger.info(f"RUN: {job_uid}")
            started_monotonic = time.monotonic()
            handle = self._channel.spawn(WorkerLaunchSpec(
                handler=task.handler,
                job=job,
                job_context=ctx,
                execution_id=execution_id,
                ipc_dir=self._ipc_dir,
                timeout_seconds=float(
                    job.timeout_seconds if job.timeout_seconds is not None else 3600.0
                ),
                run_id=self._session.run_id or "run",
                attempt_no=attempt_no,
            ))

            entry = InFlightJob(
                job_uid=job_uid,
                job_snapshot=job_snapshot,
                job=job,
                handle=handle,
                started_monotonic=started_monotonic,
                acquired=acquired,
                attempt_id=attempt_id,
                execution_id=execution_id,
                attempt_no=attempt_no,
                run_id=self._session.run_id or "run",
                started_at=started_at,
                known_job_uids=frozenset(
                    store.state.queue_uids
                    | store.state.in_flight_uids
                    | store.state.success_uids
                    | store.state.failed_uids
                ),
                active_files=dict(resolved_files or {}),
                active_file_sources=dict(job.files),
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

    def _build_context(
        self,
        job: Job,
        state: "PipelineState",
        files: Mapping[str, FileSpec] | None = None,
    ) -> JobContext:
        """组装执行上下文（资源名与已完成状态快照随 ctx 下发子进程）。"""
        # 资源名注册集快照随 ctx 下发——suspend_resource 对未注册名
        # fail-loud（typo 不静默失效）。
        # 不变式：successes/failed 活索引在快照边界冻结为 frozenset——
        # JobContext 持有的必须是派发时刻快照，绝不引用活集合。
        return JobContext(
            job,
            frozenset(state.success_uids),
            frozenset(state.failed_uids),
            dict(state.cursors),
            output_roots=self._output_roots,
            ipc_dir=self._ipc_dir,
            resource_names=frozenset(self._resources),
            protected_roots=self._protected_roots,
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
        """准入前解析预声明文件（普通路径绝对化、上游产物引用解析与输入存在性校验）。"""
        store = self._store
        resolved: dict[str, FileSpec] = {}
        for name, spec in job.files.items():
            if spec.is_output_ref:
                assert spec.producer_uid is not None and spec.output_name is not None
                if spec.producer_uid not in job.depends_on:
                    raise TaskLiteError(
                        f"Producer {spec.producer_uid!r} not in explicit direct depends_on {list(job.depends_on)!r}"
                    )
                producer_artifacts = store.state.successes.get(spec.producer_uid, {}).get("artifacts", {})
                desc = producer_artifacts.get(spec.output_name)
                if not desc or "path" not in desc:
                    raise TaskLiteError(
                        f"Producer {spec.producer_uid!r} did not publish output {spec.output_name!r}"
                    )
                resolved_path = desc["path"]
                if spec.role == "input" and not os.path.exists(resolved_path):
                    raise FileNotFoundError(f"Missing input file: {resolved_path}")
                bound_spec = spec.bind_output_path(resolved_path, base_dir=self._trigger_registry.base_dir)
            else:
                bound_spec = spec.resolve_against(self._trigger_registry.base_dir)
                assert bound_spec.path is not None
                resolved_path = bound_spec.path
                if spec.role == "input" and not os.path.exists(resolved_path):
                    raise FileNotFoundError(f"Missing input file: {resolved_path}")
            resolved[name] = bound_spec
        return resolved

    # ── 拒绝与异常分支 ────────────────────────────────────────────────

    def _reject_and_commit(
        self,
        job_uid: str,
        job_snapshot: dict,
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
                job_uid, meta, job_snapshot=job_snapshot, count_as=count_as,
                cascade=True, attempt_id=attempt_id,
            )
            self._store.fire_attempt_finished(
                job_uid,
                outcome=AttemptOutcome(
                    success=False, will_retry=False, metadata=dict(outcome.failure_metadata)
                ),
            )
        except _JobTerminated:
            # 3-strike 失败档案成功——作业已终结（StateStore 内部已计入
            # 指标并触发钩子）
            return

    def _handle_interrupt(self, job_uid: str, job_snapshot: dict) -> None:
        """派发中断分支：entry 已登记交运行循环统一 abort，否则回队后上抛。"""
        logger.warning(f"Pipeline interrupted while dispatching {job_uid}.")
        if job_uid in self._in_flight:
            # entry 已注册到 in_flight：此处不清理/不 requeue/不 release，
            # 全部交给运行循环的中止收尾统一处理，避免对同一 entry 二次
            # 释放资源、二次 requeue 同一作业。
            raise KeyboardInterrupt
        self._store.state.requeue_jobs([job_snapshot], front=True)
        # 不在此保存队列：内存此刻缺其他 in-flight 作业，交给运行循环的
        # 崩溃安全保存合并磁盘真相后统一保存。
        raise KeyboardInterrupt

    def _handle_dispatch_failure(
        self,
        job_uid: str,
        job_snapshot: dict,
        attempt_id: int | None,
        exc: Exception,
    ) -> None:
        """派发异常分支：3-strike 派发失败预算收敛，未达阈值回队后上抛。"""
        store = self._store
        logger.error(f"Error dispatching job {job_uid}: {exc}\n{traceback.format_exc()}")
        rt_state = JobRuntimeState.from_dict(job_snapshot.get("runtime"))
        failures = rt_state.record_dispatch_failure()
        job_snapshot["runtime"] = rt_state.to_dict()
        if failures >= self._commit_failure_threshold:
            logger.critical(
                f"Dispatch failed {failures} times for {job_uid} ({exc}); "
                f"treating as deterministic bad input (e.g. unpickleable "
                f"handler), sending to failure archive."
            )
            failure_metadata = {
                "error_message": ERR_DISPATCH_FAILURE,
                "failures": failures,
                "detail": str(exc)[:200],
            }
            self._in_flight.settle(job_uid, state=store.state)
            self._reject_and_commit(job_uid, job_snapshot, failure_metadata, attempt_id=attempt_id)
            return None
        self._in_flight.settle(job_uid, state=store.state)
        store.state.requeue_jobs([job_snapshot], front=True)
        store.finish_attempt(
            attempt_id, outcome=ATTEMPT_REQUEUED, error_message=f"dispatch failure: {exc}"
        )
        # 不在此保存队列：内存此刻缺其他 in-flight 作业，交给运行循环的
        # 崩溃安全保存合并磁盘真相后统一保存。
        raise


__all__ = [
    "DispatchKind",
    "DispatchMachine",
    "DispatchOutcome",
]
