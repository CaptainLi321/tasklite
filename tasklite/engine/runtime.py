"""EngineRuntime：v2 核心运行期深模块（装配 + 薄事件泵）。

统一聚合主循环事件泵（四相：填池派发 → 死锁仲裁 → 回收结算 → 等待
决策）、五关预检派发、结果收敛事务、崩溃/停机恢复与在途追踪。装配
快照见 RunConfig（engine/config.py）；一次 run 的生命周期状态
见 RunSession（engine/session.py）。
"""
from __future__ import annotations

import logging
import os
import pickle
import signal
import threading
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import Any

from .config import RunConfig
from .dispatch import DispatchKind, DispatchMachine, DispatchOutcome
from .execution_access import exclusive_state_access
from .channel import ExecutionChannel
from .completion import CompletionMachine
from .resource import RunNotifier
from .selection import SelectionMachine, SelectionStandstill
from .in_flight import InFlightTracker
from .recovery import RecoveryOrchestrator
from .scheduler import DeadlockAttribution, JobScheduler
from .session import RunSession
from .store import StateStore
from .types import (
    AuthorizationOutcome,
    AuthorizationStatus,
    ExecutionOptions,
    ExitReason,
    JobHandle,
    PendingDeclaration,
    StepOutcome,
    StopMode,
)
from .wait import LoopFacts, decide_wait
from ..backend.base import AbstractStateBackend
from ..exceptions import TaskLiteError
from ..models.artifact import (
    ArtifactDeclaration,
    FileSpec,
    merge_named_file_spec,
    validate_file_name,
)
from ..models.job import Job
from ..models.state import PipelineState, job_uid_from_snapshot
from ..selection import Exclude
from ..utils.artifact import ArtifactPathPolicy, _execute_pre_deletes

logger = logging.getLogger("tasklite")

_REAL_SLEEP = time.sleep

# step() 单步派发上限缺省（显式上限由调用方传入）
_UNBOUNDED_DISPATCH_LIMIT = 1_000_000


@dataclass(frozen=True)
class DeadlockArbitration:
    """无可运行候选时死锁仲裁的命名结果。

    - ``deadlock_detected``: 本拍是否判定并已熔断死锁（进失败档案）；
    - ``should_terminate``: governor 独立终止信号（全队列熔断且队列清空）；
    - ``wait_seconds``: 缺口重试的建议等待秒数（否则 0）。
    """

    deadlock_detected: bool
    should_terminate: bool
    wait_seconds: float


class EngineRuntime:
    """v2 核心运行期深模块：装配机器群并驱动四相事件泵。"""

    def __init__(self, config: RunConfig) -> None:
        self.config = config
        self.tasks = config.tasks
        self.scheduler = JobScheduler(
            resources=config.resources, ordering_policy=config.ordering_policy
        )
        self.governor = config.governor

        # 持久会话：execute() 经 begin() 复位（语义等价于每次新建会话，
        # 同时保证机器持有的 session 引用跨 run 稳定）。
        self._session = RunSession()

        # 构造期即建 StateStore（enqueue 与管理写操作在 run 前可用）。
        # backend 所有权唯一锚定 StateStore（RunConfig.backend 仅为构造
        # 期装配快照的搬运通道），运行期一切读写经 store.backend 派生。
        self.store: StateStore = StateStore(
            config.backend,
            trigger_registry=config.trigger_registry,
            requeue_policy=config.requeue_policy,
            protected_roots=config.protected_roots,
        )

        self.channel: ExecutionChannel = config.channel
        self._in_flight: InFlightTracker = InFlightTracker()
        self._notifier: RunNotifier = RunNotifier()

        # 构建机器依赖拓扑（recovery 依赖 completion；dispatch 依赖 recovery）
        self._completion = CompletionMachine(
            store=self.store,
            trigger_registry=config.trigger_registry,
            channel=self.channel,
            resources=config.resources,
            in_flight=self._in_flight,
            session=self._session,
        )
        self._recovery = RecoveryOrchestrator(
            store=self.store,
            channel=self.channel,
            resources=config.resources,
            in_flight=self._in_flight,
            completion=self._completion,
        )
        self._dispatch_start_lock = threading.RLock()
        self._dispatch = DispatchMachine(
            store=self.store,
            scheduler=self.scheduler,
            trigger_registry=config.trigger_registry,
            resources=config.resources,
            channel=self.channel,
            in_flight=self._in_flight,
            session=self._session,
            recovery=self._recovery,
            completion=self._completion,
            tasks=config.tasks,
            output_roots=config.output_roots,
            protected_roots=config.protected_roots,
            dispatch_start_lock=self._dispatch_start_lock,
        )

        self._is_running: bool = False
        self._exclude: Exclude | None = None
        self._temporary_excluded_uids: set[str] = set()
        self.selection_machine: SelectionMachine = SelectionMachine(self.store)
        self.channel.set_declaration_handler(self._handle_declaration)
        self.channel.set_wakeup_callback(self._notifier.notify)
        self._session.bind_exclude_chain_handler(self.temporarily_exclude_chain)

    def temporarily_exclude(self, job_uid: str) -> None:
        """内部排除入口：登记单个临时排除 UID（供授权拒绝等复用）。"""
        self._temporary_excluded_uids.add(job_uid)
        self.scheduler.temporarily_exclude(job_uid)
        self.store.temporarily_exclude(job_uid)
        self.selection_machine.temporarily_exclude(job_uid)

    def temporarily_exclude_chain(self, job_uid: str) -> None:
        """临时排除指定作业及其所有直接和间接下游作业。"""
        self.temporarily_exclude(job_uid)
        for down_uid in self.store.state.find_all_downstream(job_uid):
            self.temporarily_exclude(down_uid)

    def _handle_declaration(
        self, handle: JobHandle, payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        """主进程动态文件声明仲裁出口：验证引用、授权检查、前置清理并回送路径。"""
        raw_req_id = payload.get("request_id")
        if not isinstance(raw_req_id, str) or not raw_req_id:
            return {
                "request_id": "",
                "status": "error",
                "error": "request_id must be a non-empty str",
            }
        req_id = raw_req_id
        name = payload.get("name", "")
        spec_data = payload.get("spec", {})
        entry = self._in_flight.get(handle.job_uid)
        if entry is None or not self._declaration_identity_matches(handle, entry):
            return {
                "request_id": req_id,
                "status": "error",
                "error": f"Job {handle.job_uid} not in flight",
            }
        if handle.authorization_outcome is not None:
            return {
                "request_id": req_id,
                "status": "error",
                "error": f"Job {handle.job_uid} execution is no longer accepting declarations",
            }
        if handle.pending_declaration is not None:
            return {
                "request_id": req_id,
                "status": "error",
                "error": f"Job {handle.job_uid} already has a pending declaration",
            }

        try:
            spec = FileSpec.from_dict(spec_data)
            validated_name = validate_file_name(name)
        except Exception as exc:
            return {"request_id": req_id, "status": "error", "error": str(exc)}

        # 1. 幂等与冲突检查
        try:
            merged, is_new = merge_named_file_spec(
                entry.active_file_sources,
                validated_name,
                spec,
                base_dir=self.store.base_dir,
                depends_on=handle.job.depends_on,
            )
        except Exception as exc:
            return {"request_id": req_id, "status": "error", "error": str(exc)}
        source_spec = merged[validated_name]
        if not is_new:
            existing = entry.active_files.get(validated_name)
            if existing is None or existing.path is None:
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"File declaration {validated_name!r} has no resolved path",
                }
            return {
                "request_id": req_id,
                "status": "ok",
                "resolved_path": existing.path,
            }

        # 2. 上游产物引用解析
        if source_spec.is_output_ref:
            assert source_spec.producer_uid is not None and source_spec.output_name is not None
            if source_spec.producer_uid not in handle.job.depends_on:
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"Producer {source_spec.producer_uid} not in explicit direct depends_on",
                }
            producer_artifacts = self.store.state.successes.get(
                source_spec.producer_uid, {}
            ).get("artifacts", {})
            desc = producer_artifacts.get(source_spec.output_name)
            if not desc or "path" not in desc:
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": (
                        f"Producer {source_spec.producer_uid} did not publish "
                        f"output {source_spec.output_name}"
                    ),
                }
            resolved_path = desc["path"]
            if source_spec.role == "input" and not os.path.exists(resolved_path):
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"Missing input file: {resolved_path}",
                }
            bound_spec = source_spec.bind_output_path(resolved_path)
        else:
            bound_spec = source_spec.resolve_against(self.store.base_dir)
            assert bound_spec.path is not None
            resolved_path = bound_spec.path
            if source_spec.role == "input" and not os.path.exists(resolved_path):
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"Missing input file: {resolved_path}",
                }

        needs_authorization = (
            bound_spec.delete_existing
            or bound_spec.delete_on_success
            or bound_spec.delete_on_failure
        )
        logger.info(
            f"FILE DECLARATION: {handle.job_uid} requested "
            f"{validated_name!r} at {resolved_path}"
        )
        request = PendingDeclaration(
            request_id=req_id,
            name=validated_name,
            source=source_spec,
            bound=bound_spec,
            resolved_path=resolved_path,
        )
        if not needs_authorization:
            self._record_declaration(entry, request)
            request.status = AuthorizationStatus.APPROVED
            logger.info(
                f"FILE DECLARATION APPROVED: {handle.job_uid} {resolved_path}"
            )
            return self._declaration_reply(request)

        handle.pending_declaration = request
        handle.pause_timeout()
        validated_path, error = self._validate_declaration_target(request)
        if error is not None:
            return self._finish_authorization(
                handle,
                entry,
                request,
                AuthorizationStatus.PREPARATION_FAILED,
                error["message"],
                error,
            )
        request.bound = request.bound.bind_output_path(validated_path)
        request.resolved_path = validated_path
        if self._session.stop_mode is not StopMode.NONE:
            self._finish_authorization(
                handle,
                entry,
                request,
                AuthorizationStatus.STOPPED,
                f"authorization wait stopped for {validated_path}",
            )
            return None
        logger.info(
            f"FILE AUTHORIZATION WAIT: {handle.job_uid} {validated_path}"
        )
        authorized = self.store.check_or_request_authorization(validated_path)
        if authorized is None:
            return None
        return self._apply_authorization_decision(handle, entry, request, authorized)

    def _declaration_identity_matches(self, handle: JobHandle, entry: Any) -> bool:
        """请求句柄必须与当前在飞执行使用同一 job、run、attempt 与 execution 身份。"""
        if entry.handle is not handle or entry.job_uid != handle.job_uid:
            return False
        if not handle.execution_id or entry.execution_id != handle.execution_id:
            return False
        if handle.run_id is not None and entry.run_id != handle.run_id:
            return False
        if handle.attempt_no is not None and entry.attempt_no != handle.attempt_no:
            return False
        if self._session.run_id is not None and entry.run_id != self._session.run_id:
            return False
        return True

    def _record_declaration(self, entry: Any, request: PendingDeclaration) -> None:
        entry.active_file_sources[request.name] = request.source
        entry.active_files[request.name] = request.bound

    def _validate_declaration_target(
        self, request: PendingDeclaration
    ) -> tuple[str, dict[str, str] | None]:
        policy = ArtifactPathPolicy(
            output_roots=self.config.output_roots,
            protected_roots=self.config.protected_roots,
        )
        try:
            return policy.resolve(
                request.resolved_path, delete_existing=request.bound.delete_existing,
            ), None
        except (OSError, ValueError, NotImplementedError) as exc:
            return request.resolved_path, {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            }

    def _apply_authorization_decision(
        self,
        handle: JobHandle,
        entry: Any,
        request: PendingDeclaration,
        authorized: bool,
    ) -> dict[str, Any] | None:
        if (
            handle.pending_declaration is not request
            or request.status is not AuthorizationStatus.PENDING
            or handle.authorization_outcome is not None
        ):
            return None
        if self._session.stop_mode is not StopMode.NONE:
            message = f"authorization wait stopped for {request.resolved_path}"
            self._finish_authorization(
                handle, entry, request, AuthorizationStatus.STOPPED, message
            )
            return {
                "request_id": request.request_id,
                "status": "stopped",
                "error": message,
            }
        if authorized is False:
            message = f"authorization denied for {request.resolved_path}"
            self._finish_authorization(
                handle, entry, request, AuthorizationStatus.DENIED, message
            )
            return {
                "request_id": request.request_id,
                "status": "denied",
                "error": message,
            }

        if request.bound.delete_existing:
            policy = ArtifactPathPolicy(
                output_roots=self.config.output_roots,
                protected_roots=self.config.protected_roots,
                authorization_check=lambda path, kind: self.store.backend.is_path_authorized(path, kind),
            )
            try:
                _execute_pre_deletes(
                    policy,
                    [ArtifactDeclaration(request.resolved_path, delete_existing=True)],
                )
            except (OSError, ValueError, NotImplementedError) as exc:
                error = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                }
                message = f"Artifact preparation failed for {request.resolved_path}: {exc}"
                self._finish_authorization(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.PREPARATION_FAILED,
                    message,
                    error,
                )
                return None

        request.status = AuthorizationStatus.APPROVED
        handle.pending_declaration = None
        handle.resume_timeout()
        self._record_declaration(entry, request)
        logger.info(
            f"FILE AUTHORIZATION APPROVED: {handle.job_uid} {request.resolved_path}"
        )
        return self._declaration_reply(request)

    def _finish_authorization(
        self,
        handle: JobHandle,
        entry: Any,
        request: PendingDeclaration,
        status: AuthorizationStatus,
        message: str,
        exception: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        if (
            handle.pending_declaration is not request
            or handle.authorization_outcome is not None
        ):
            return None
        request.status = status
        handle.pending_declaration = None
        handle.authorization_outcome = AuthorizationOutcome(
            status=status,
            message=message,
            path=request.resolved_path,
            exception=exception,
        )
        handle.resume_timeout()
        self._record_declaration(entry, request)
        logger.info(
            f"FILE AUTHORIZATION {status.value.upper()}: "
            f"{handle.job_uid} {request.resolved_path} ({message})"
        )
        self.channel.terminate_process(handle)
        return None

    @staticmethod
    def _declaration_reply(request: PendingDeclaration) -> dict[str, Any]:
        return {
            "request_id": request.request_id,
            "status": "ok",
            "resolved_path": request.resolved_path,
        }

    def is_job_excluded(self, job_uid: str, task_type: str) -> bool:
        """统一判断作业是否被排除（用户 exclude 规则或运行期临时排除）。"""
        if job_uid in self._temporary_excluded_uids:
            return True
        if self._exclude is not None and self._exclude.matches_task_type(task_type):
            return True
        return False

    @property
    def session(self) -> RunSession:
        return self._session

    @property
    def backend(self) -> AbstractStateBackend:
        """持久化后端只读派生引用（所有权唯一锚定 StateStore）。

        换库唯一经 ``store.set_backend`` 单点，本属性每次读取实时派生，
        不持有独立可变引用——新增需要后端的机器一律经本接缝取用。
        """
        return self.store.backend

    @property
    def in_flight(self) -> InFlightTracker:
        return self._in_flight

    @property
    def stop_mode(self) -> StopMode:
        return self._session.stop_mode

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def state(self) -> PipelineState:
        return self.store.state

    def request_stop(self) -> StopMode:
        """停机请求接口（单调状态转移，委托 RunSession，并唤醒运行循环）。"""
        with self._dispatch_start_lock:
            if not self._is_running:
                return self._session.stop_mode
            mode = self._session.request_stop()
        for handle in self._in_flight.active_handles():
            request = handle.pending_declaration
            entry = self._in_flight.get(handle.job_uid)
            if request is not None and entry is not None:
                self._finish_authorization(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.STOPPED,
                    f"authorization wait stopped for {request.resolved_path}",
                )
        self._notifier.notify()
        return mode

    def execute(self, options: ExecutionOptions | None = None) -> None:
        """完整执行管线生命周期。"""
        opts = options if options is not None else ExecutionOptions()

        # 会话复位与活跃发布必须原子完成；接纳 stop 后本 Run 不再复位会话。
        with self._dispatch_start_lock:
            if self._is_running:
                raise TaskLiteError("Pipeline run() already in progress on this instance.")
            self._session.begin()
            self._is_running = True

        old_sigterm = None
        old_sigint = None
        try:
            self._exclude = opts.exclude
            self._temporary_excluded_uids.clear()
            self.scheduler.reset_run(opts.exclude)
            self.store.set_run_exclusions(opts.exclude, self._temporary_excluded_uids)
            self.selection_machine.set_exclusions(opts.exclude, self._temporary_excluded_uids)

            with exclusive_state_access(self.store.backend):
                try:
                    # 主线程安装信号处理器；失败仍由作用域归还执行权。
                    if opts.install_signals:
                        old_sigterm = signal.signal(signal.SIGTERM, self._handle_signal)
                        old_sigint = signal.signal(signal.SIGINT, self._handle_signal)

                    self._preflight_picklable_handlers()
                    self.governor.reset()
                    self._run_body()
                finally:
                    self.config.resources.unbind_notifier()
        finally:
            try:
                try:
                    if old_sigterm is not None:
                        signal.signal(signal.SIGTERM, old_sigterm)
                finally:
                    if old_sigint is not None:
                        signal.signal(signal.SIGINT, old_sigint)
            finally:
                with self._dispatch_start_lock:
                    self._is_running = False

    # ── 四相事件泵 ────────────────────────────────────────────────────

    def _terminal_outcome(self) -> StepOutcome:
        """三个终态早退（ABORTING / 排空完毕 / 空闲完成）的统一形状：
        全零计数 + should_terminate=True + 当前停机态与退出原因透传。"""
        return StepOutcome(
            dispatched_count=0,
            settled_count=0,
            is_idle=True,
            should_wait=False,
            wait_time=0.0,
            deadlock_detected=False,
            stop_mode=self._session.stop_mode,
            should_terminate=True,
            exit_reason=self._session.exit_reason().value,
        )

    def _fill_dispatch_pool(
        self, limit: int, draining: bool
    ) -> tuple[DispatchOutcome | None, float, int]:
        """相一·填池派发（仅非 DRAINING 且未超单步上限）：逐个 dispatch_next，
        SPAWNED / HANDLED_NO_SUBPROCESS 继续填池，WORKER_SATURATED /
        NO_CANDIDATE 断流即停；worker_wait 聚合取 min（多次资源挂起恢复
        取最早者）。

        Returns:
            (最后一次派发结果或 None, worker_wait 最小值或 0, 实派发计数)
        """
        last_outcome: DispatchOutcome | None = None
        worker_wait = 0.0
        dispatched = 0
        if not draining:
            if self._completion.waiting_authorization or any(
                h.waiting_auth for h in self._in_flight.active_handles()
            ):
                return last_outcome, worker_wait, dispatched
            while dispatched < limit and self._session.stop_mode is StopMode.NONE:
                outcome = self._dispatch.dispatch_next()
                last_outcome = outcome
                if outcome.worker_wait > 0:
                    worker_wait = (
                        outcome.worker_wait
                        if worker_wait <= 0
                        else min(worker_wait, outcome.worker_wait)
                    )
                if outcome.kind is DispatchKind.SPAWNED:
                    dispatched += 1
                    continue
                if outcome.kind is DispatchKind.HANDLED_NO_SUBPROCESS:
                    continue
                break
        return last_outcome, worker_wait, dispatched

    def _arbitrate_deadlock(
        self, last_outcome: DispatchOutcome | None, store: StateStore
    ) -> DeadlockArbitration:
        """相二·无可运行候选时的死锁归因（委托 DeadlockGovernor 自闭环仲裁）。"""
        deadlock_detected = False
        should_terminate = False
        wait_seconds = 0.0
        if last_outcome is not None and last_outcome.kind is DispatchKind.NO_CANDIDATE:
            if store.state.is_empty and not self._in_flight:
                should_terminate = True
            elif not self._in_flight:
                if self.selection_machine is not None:
                    try:
                        decision = self.governor.arbitrate(
                            last_outcome.standstill,
                            store=self.store,
                            selection_machine=self.selection_machine,
                        )
                    except TypeError:
                        decision = self.governor.arbitrate(
                            last_outcome.standstill,
                            store=self.store,
                        )
                else:
                    decision = self.governor.arbitrate(
                        last_outcome.standstill,
                        store=self.store,
                    )
                if decision.action in ("resolved", "selection_blocked"):
                    if decision.action == "selection_blocked":
                        if decision.standstill is not None:
                            self._report_selection_blocked(decision.standstill)
                    else:
                        deadlock_detected = True
                    if decision.should_terminate:
                        should_terminate = True
                elif decision.action in ("grace_waiting", "gap_retrying"):
                    if decision.wait_time > 0:
                        wait_seconds = decision.wait_time
        return DeadlockArbitration(
            deadlock_detected=deadlock_detected,
            should_terminate=should_terminate,
            wait_seconds=wait_seconds,
        )

    def _report_selection_blocked(self, standstill: SelectionStandstill) -> None:
        """记录「允许执行但仅因 Exclude 而本次无法完成」的下游（留队待下次）。"""
        upstream = dict(standstill.blocked_by)
        for uid in standstill.blocked_uids:
            logger.warning(
                f"SELECTION: allowed job {uid} blocked by excluded upstream "
                f"{upstream.get(uid, '?')} (dependency excluded); kept in "
                f"queue for a later run."
            )
        logger.info(
            "Remaining queued jobs are deferred by selection; finishing run as "
            "completed with the queue preserved."
        )

    def _poll_pending_authorizations(self) -> None:
        """轮询处于等待中的文件声明授权；若已决策则恢复执行或终止。"""
        is_stopping = self._session.stop_mode is not StopMode.NONE
        for handle in self._in_flight.active_handles():
            request = handle.pending_declaration
            if request is None or not handle.waiting_auth:
                continue
            entry = self._in_flight.get(handle.job_uid)
            if entry is None or not self._declaration_identity_matches(handle, entry):
                continue

            if is_stopping:
                self._finish_authorization(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.STOPPED,
                    f"authorization wait stopped for {request.resolved_path}",
                )
                continue

            validated_path, error = self._validate_declaration_target(request)
            if error is not None:
                self._finish_authorization(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.PREPARATION_FAILED,
                    error["message"],
                    error,
                )
                continue
            request.bound = request.bound.bind_output_path(validated_path)
            request.resolved_path = validated_path

            authorized = self.store.check_or_request_authorization(validated_path)
            if authorized is not None:
                reply = self._apply_authorization_decision(
                    handle, entry, request, authorized
                )
                if reply is None:
                    continue
                reply_sender = self.channel.get_reply_sender(handle.execution_id)
                if reply_sender is not None:
                    try:
                        reply_sender.send_reply(reply)
                    except OSError:
                        pass

    def _drain_and_settle(self) -> int:
        """相三·回收在途结果并统一结算，返回本轮完成计数（无在途为 0）。"""
        if not self._in_flight:
            return 0
        handles = [
            h for h in self._in_flight.active_handles()
            if not self._completion.has_pending_result(h)
        ]
        completed = self.channel.reap_completed(handles)
        return self._completion.settle_reaped(completed)

    def _next_in_flight_wait(self) -> float | None:
        """返回最近执行超时；授权等待已暂停计时，不计入截止点。"""
        deadlines = [
            handle.timeout_at_monotonic
            for handle in self._in_flight.active_handles()
            if not handle.waiting_auth and not self._completion.has_pending_result(handle)
        ]
        if not deadlines:
            return None
        return max(0.0, min(deadlines) - time.monotonic())

    def step(self, max_dispatch: int | None = None) -> StepOutcome:
        """非阻塞单步推进事件泵（主循环与单步测试共用的统一事件泵）。

        run 身份（run_id/空态装载）唯一负责者是 prepare_run_state（execute()
        启动屏障）——store.state 恒非 None（构造与 set_state 双点归一化），
        经 run() 外直达 step 的路径由测试显式 set_state 装载，无需惰性引导。
        """
        store = self.store

        # 0. 停机门：DRAINING 排空完毕与空闲完成早退
        draining = False
        if self._session.stop_mode is not StopMode.NONE:
            if self._in_flight:
                draining = True
            else:
                logger.info("Pipeline drained.")
                return self._terminal_outcome()

        self._poll_pending_authorizations()

        if store.state.is_empty and not self._in_flight:
            return self._terminal_outcome()

        # 1. 先处理 worker IPC 与结算，避免旧槽位释放抢在授权等待登记前派发。
        settled_count = self._drain_and_settle()

        # 2. 填池派发（仅非 DRAINING 状态且未超过单步限制）
        limit = max_dispatch if max_dispatch is not None else _UNBOUNDED_DISPATCH_LIMIT
        last_outcome, worker_wait, dispatched = self._fill_dispatch_pool(limit, draining)

        # 前进信号终结缺口 episode（须先于死锁仲裁）。
        if dispatched > 0:
            self.governor.record_dispatch_progress()

        # 3. 处理无可运行 job 与死锁判定
        arbitration = self._arbitrate_deadlock(last_outcome, store)

        # 4. 等待/空闲决策（唯一实现见 wait.decide_wait）
        is_idle = store.state.is_empty and not self._in_flight
        authorization_wait = self._completion.waiting_authorization or any(
            handle.waiting_auth
            for handle in self._in_flight.active_handles()
        )
        decision = decide_wait(LoopFacts(
            stop_mode=self._session.stop_mode, has_in_flight=bool(self._in_flight),
            store_empty=store.state.is_empty, dispatched=dispatched, settled=settled_count,
            has_runnable=(last_outcome.kind is not DispatchKind.NO_CANDIDATE
                          if last_outcome is not None else True),
            min_wait=(last_outcome.standstill.min_wait
                      if last_outcome is not None else float("inf")),
            in_flight_wait=self._next_in_flight_wait(),
            authorization_wait=authorization_wait,
            worker_wait=worker_wait, deadlock_wait=arbitration.wait_seconds,
            should_terminate=arbitration.should_terminate,
            deadlock_detected=arbitration.deadlock_detected,
        ))

        exit_reason = (
            self._session.exit_reason().value if (is_idle or arbitration.should_terminate)
            else None
        )

        return StepOutcome(
            dispatched_count=dispatched, settled_count=settled_count,
            is_idle=is_idle, should_wait=decision.should_wait,
            wait_time=decision.wait_time,
            deadlock_detected=arbitration.deadlock_detected,
            stop_mode=self._session.stop_mode,
            should_terminate=arbitration.should_terminate or is_idle,
            exit_reason=exit_reason,
        )

    # ── 主循环与承重网 ────────────────────────────────────────────────

    def _wait(self, timeout: float | None, last_seq: int | None = None) -> None:
        """运行循环调度等待：支持测试 monkeypatch 捕获，同时由 _notifier 实现跨线程与 check-to-wait 唤醒。"""
        if last_seq is not None:
            self._notifier.wait(last_seq, timeout=timeout)
        else:
            if time.sleep is not _REAL_SLEEP and timeout is not None and timeout > 0:
                time.sleep(timeout)
            self._notifier._event.wait(timeout=timeout)
            self._notifier._event.clear()

    def run_loop(self) -> None:
        """事件驱动主循环：以 step() 统一驱动填池、回收与等待（裸循环）。"""
        store = self.store
        self._in_flight.clear()
        self._notifier.clear()
        while not store.state.is_empty or self._in_flight:
            last_seq = self._notifier.snapshot()
            outcome = self.step()
            if outcome.should_terminate:
                break
            if outcome.should_wait:
                wait_time = outcome.wait_time
                self._wait(
                    wait_time if wait_time != float("inf") else None,
                    last_seq=last_seq,
                )
            elif store.state.is_empty and not self._in_flight:
                break

        logger.info(f"Pipeline {self.config.name} finished.")
        self.channel.wait_for_lingering_workers()
        self._in_flight.clear()

    def prepare_run_state(self) -> PipelineState:
        """恢复持久化状态，再登记本次 Run 身份。"""
        self.config.resources.bind_notifier(self._notifier)
        self.scheduler.begin_round()

        state = self._recovery.load_and_recover()
        self._session.run_id = uuid.uuid4().hex
        self._session.dispatch_no = 0
        try:
            self.backend.set_engine_metadata("last_run_id", self._session.run_id)
        except Exception as e:
            logger.critical(f"Failed to persist run_id to meta table: {e}")
            raise

        return state

    def _run_with_crash_net(self) -> None:
        """运行主事件循环与统一异常承重网。"""
        try:
            self.run_loop()
        except BaseException as e:
            # 单点崩溃网：所有异常同构处理（在途清扫并原样上抛）；
            # KI 与非 Exception（如 SystemExit）
            # 不附 traceback，其余附。
            self._crash_log(e)
            try:
                self._recovery.abort_in_flight()
            except BaseException as cleanup_error:
                logger.error(f"Abort cleanup failed: {cleanup_error}")
            raise

    @staticmethod
    def _crash_log(e: BaseException) -> None:
        """承重网的逐类型日志分派（KI 与非 Exception 不附 traceback）。"""
        if isinstance(e, KeyboardInterrupt):
            logger.warning("Pipeline interrupted by user.")
        elif isinstance(e, Exception):
            logger.critical(
                f"Pipeline scheduler crashed with unhandled exception: {e}\n"
                f"{traceback.format_exc()}"
            )
        else:
            logger.critical(f"Pipeline terminated by {type(e).__name__}: {e}")

    def _run_body(self) -> None:
        """主执行体：启动屏障（装载 + fencing）→ 带承重网的主循环。"""
        self.prepare_run_state()
        self._skip_dirty = True
        self._run_with_crash_net()

    def _handle_signal(self, signum: int, frame: Any) -> None:
        if self._session.stop_mode is StopMode.NONE:
            logger.info(f"收到信号 {signum}，停止新派发并等待在途任务完成……")
            self.request_stop()
        else:
            logger.info(f"再次收到信号 {signum}，正在等待在途任务完成，不强制终止。")
            self._notifier.notify()

    def _preflight_picklable_handlers(self) -> None:
        """预检：全部已注册 Task 的 handler 必须可 pickle。

        spawn 进程模型下不可 pickle 的 handler 会在每次派发时崩溃——
        入口拒绝（fail-loud），而非运行期逐作业失败。
        """
        for task_type in self.tasks.task_types():
            task = self.tasks.lookup(task_type)
            try:
                pickle.dumps(task.handler)
            except Exception as e:
                raise TaskLiteError(
                    f"Handler for task_type '{task_type}' is not picklable: "
                    f"{task.handler!r} ({e}). Functions must be module-level."
                ) from e

    def close(self) -> None:
        """释放运行期通道资源。"""
        self.channel.close()


__all__ = [
    "DeadlockArbitration",
    "EngineRuntime",
]
