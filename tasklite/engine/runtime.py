"""EngineRuntime：v2 核心运行期深模块（装配 + 薄事件泵）。

统一聚合主循环事件泵（四相：填池派发 → 死锁仲裁 → 回收结算 → 等待
决策）、五关预检派发、结果收敛事务、崩溃/停机恢复与在途追踪。装配
快照见 RunConfig（engine/config.py）；一次 run 的生命周期状态与钩子
单一出口见 RunSession（engine/session.py）。
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
from .channel import ExecutionChannel
from .completion import CompletionMachine
from .errorclass import (
    ERR_DEPENDENCY_EXCLUDED,
    ErrorClassifier,
)
from .resource import RunNotifier
from .selection import SelectionMachine, SelectionStandstill
from .in_flight import InFlightTracker
from .recovery import RecoveryOrchestrator
from .scheduler import DeadlockAttribution, JobScheduler
from .session import RunSession
from .store import StateStore
from .types import (
    ExecutionOptions,
    ExitReason,
    JobHandle,
    StepOutcome,
    StopMode,
)
from .wait import IN_FLIGHT_POLL_SECONDS, LoopFacts, decide_wait
from ..backend.base import AbstractStateBackend
from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.artifact import (
    ArtifactDeclaration,
    FileSpec,
    merge_named_file_spec,
    validate_file_name,
)
from ..models.job import Job
from ..models.state import PipelineState, job_uid_from_snapshot
from ..selection import Exclude
from ..utils.artifact import ArtifactPathPolicy
from ..utils.lockfile import release_lock, try_acquire_lock

logger = logging.getLogger("tasklite")

_REAL_SLEEP = time.sleep

# step() 单步派发上限缺省（显式上限由调用方传入）
_UNBOUNDED_DISPATCH_LIMIT = 1_000_000


@dataclass(frozen=True)
class DeadlockArbitration:
    """无可运行候选时死锁仲裁的命名结果。

    - ``deadlock_detected``: 本拍是否判定并已熔断死锁（进失败档案）；
    - ``should_terminate``: governor 独立终止信号（全队列熔断且队列清空）；
    - ``wait_seconds``: 宽限/缺口重试的建议等待秒数（否则 0）。
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

        # 构造期即建 StateStore（enqueue/OpsConsole 在 run 前可用）。
        # backend 所有权唯一锚定 StateStore（RunConfig.backend 仅为构造
        # 期装配快照的搬运通道），运行期一切读写经 store.backend 派生。
        self.store: StateStore = StateStore(
            config.backend,
            commit_failure_threshold=config.commit_failure_threshold,
            classifier=config.classifier,
            trigger_registry=config.trigger_registry,
            requeue_policy=config.requeue_policy,
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
            trigger_registry=config.trigger_registry,
            completion=self._completion,
        )
        self._dispatch = DispatchMachine(
            store=self.store,
            scheduler=self.scheduler,
            trigger_registry=config.trigger_registry,
            resources=config.resources,
            channel=self.channel,
            in_flight=self._in_flight,
            session=self._session,
            recovery=self._recovery,
            tasks=config.tasks,
            classifier=config.classifier,
            output_roots=config.output_roots,
            ipc_dir=config.ipc_dir,
            protected_roots=config.protected_roots,
            commit_failure_threshold=config.commit_failure_threshold,
        )

        self._run_lock_fd: int | None = None
        self._is_running: bool = False
        self._exclude: Exclude | None = None
        self._temporary_excluded_uids: set[str] = set()
        self.selection_machine: SelectionMachine = SelectionMachine(self.store)
        self.channel.set_declaration_handler(self._handle_declaration)
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
        req_id = payload.get("request_id", "") or payload.get("req_id", "")
        name = payload.get("name", "")
        spec_data = payload.get("spec", {})
        entry = self._in_flight.get(handle.job_uid)
        if entry is None:
            return {"request_id": req_id, "status": "error", "error": f"Job {handle.job_uid} not in flight"}

        try:
            spec = FileSpec.from_dict(spec_data)
            validated_name = validate_file_name(name)
        except Exception as exc:
            return {"request_id": req_id, "status": "error", "error": str(exc)}

        # 1. 幂等与冲突检查
        if validated_name in entry.active_files:
            existing = entry.active_files[validated_name]
            try:
                merged, should_delete = merge_named_file_spec(
                    entry.active_files,
                    validated_name,
                    spec,
                    base_dir=self.store.base_dir,
                    depends_on=handle.job.depends_on,
                )
                return {
                    "request_id": req_id,
                    "status": "ok",
                    "resolved_path": existing.path,
                }
            except Exception as exc:
                return {"request_id": req_id, "status": "error", "error": str(exc)}

        # 2. 上游产物引用解析
        if spec.is_output_ref:
            assert spec.producer_uid is not None and spec.output_name is not None
            if spec.producer_uid not in handle.job.depends_on:
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"Producer {spec.producer_uid} not in explicit direct depends_on",
                }
            producer_artifacts = self.store.state.successes.get(spec.producer_uid, {}).get("artifacts", {})
            desc = producer_artifacts.get(spec.output_name)
            if not desc or "path" not in desc:
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"Producer {spec.producer_uid} did not publish output {spec.output_name}",
                }
            resolved_path = desc["path"]
            if spec.role == "input" and not os.path.exists(resolved_path):
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"Missing input file: {resolved_path}",
                }
            bound_spec = spec.bind_output_path(resolved_path, base_dir=self.store.base_dir)
        else:
            bound_spec = spec.resolve_against(self.store.base_dir)
            assert bound_spec.path is not None
            resolved_path = bound_spec.path
            if spec.role == "input" and not os.path.exists(resolved_path):
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"Missing input file: {resolved_path}",
                }

        # 3. 授权检查
        if bound_spec.delete_existing or bound_spec.delete_on_success or bound_spec.delete_on_failure:
            handle.pause_timeout()
            try:
                authorized = self.store.check_or_request_authorization(resolved_path)
            finally:
                handle.resume_timeout()
            if not authorized:
                handle.auth_denied = True
                self.channel.terminate_process(handle)
                return {"request_id": req_id, "status": "denied"}

        # 4. 前置删除（若声明要求且目标存在）
        if bound_spec.delete_existing and os.path.exists(resolved_path):
            policy = ArtifactPathPolicy(
                output_roots=self.config.output_roots,
                protected_roots=self.config.protected_roots,
            )
            try:
                policy.remove_existing(ArtifactDeclaration(resolved_path, delete_existing=True))
            except OSError as exc:
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"Pre-deletion failed: {exc}",
                }

        entry.active_files[validated_name] = bound_spec
        return {
            "request_id": req_id,
            "status": "ok",
            "resolved_path": resolved_path,
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

    @property
    def classifier(self) -> ErrorClassifier:
        return self.config.classifier

    def request_stop(self) -> StopMode:
        """停机请求接口（单调状态转移，委托 RunSession，并唤醒运行循环）。"""
        mode = self._session.request_stop()
        self._notifier.notify()
        return mode

    def execute(self, options: ExecutionOptions | None = None) -> None:
        """完整执行管线生命周期。"""
        opts = options if options is not None else ExecutionOptions()

        # 不变式：run() 重入守卫——同一实例并发 execute 必须 fail-loud 拒绝
        if self._is_running:
            raise RuntimeError("Pipeline run() already in progress on this instance.")
        self._is_running = True
        self._exclude = opts.exclude
        self._temporary_excluded_uids.clear()
        self.scheduler.reset_run(opts.exclude)
        self.store.set_run_exclusions(opts.exclude, self._temporary_excluded_uids)
        self.selection_machine.set_exclusions(opts.exclude, self._temporary_excluded_uids)

        old_sigterm = None
        old_sigint = None
        try:
            # 1. 单运行排他文件锁（try_acquire_lock 契约：仅「锁被占」返回
            #    None，权限/磁盘满等环境故障抛 OSError，语义必须上抛不吞）。
            self._run_lock_fd = try_acquire_lock(
                self.config.ipc_dir, "__pipeline_run__", timeout=0
            )
            if self._run_lock_fd is None:
                raise RuntimeError(
                    f"Another run() is in progress for state_dir "
                    f"{self.config.ipc_dir}; concurrent runs on the same "
                    f"state are forbidden."
                )

            # 2. 信号陷阱（主线程安装，失败抛出不静默忽略）
            if opts.install_signals:
                old_sigterm = signal.signal(signal.SIGTERM, self._handle_signal)
                old_sigint = signal.signal(signal.SIGINT, self._handle_signal)
        except BaseException:
            if old_sigterm is not None:
                signal.signal(signal.SIGTERM, old_sigterm)
            if old_sigint is not None:
                signal.signal(signal.SIGINT, old_sigint)
            if self._run_lock_fd is not None:
                release_lock(self._run_lock_fd)
                self._run_lock_fd = None
            self._is_running = False
            raise

        try:
            self._preflight_picklable_handlers()
            self._session.begin()
            self.governor.reset()

            self._run_body()

        finally:
            self.config.resources.unbind_notifier()
            if self._run_lock_fd is not None:
                release_lock(self._run_lock_fd)
                self._run_lock_fd = None
            self._is_running = False

            if old_sigterm is not None:
                signal.signal(signal.SIGTERM, old_sigterm)
            if old_sigint is not None:
                signal.signal(signal.SIGINT, old_sigint)

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
                f"{upstream.get(uid, '?')} ({ERR_DEPENDENCY_EXCLUDED}); kept in "
                f"queue for a later run."
            )
        logger.info(
            "Remaining queued jobs are deferred by selection; finishing run as "
            "completed with the queue preserved."
        )

    def _drain_and_settle(self) -> int:
        """相三·回收在途结果并统一结算，返回本轮完成计数（无在途为 0）。"""
        if not self._in_flight:
            return 0
        self._recovery.apply_pending_signals()
        handles = self._in_flight.active_handles()
        completed = self.channel.reap_completed(handles)
        return self._completion.settle_reaped(completed)

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
                logger.info("Pipeline drained. Saving queue and exiting.")
                self._recovery.save_queue_crash_safe()
                return self._terminal_outcome()

        if store.state.is_empty and not self._in_flight:
            return self._terminal_outcome()

        # 1. 填池派发（仅非 DRAINING 状态且未超过单步限制）
        limit = max_dispatch if max_dispatch is not None else _UNBOUNDED_DISPATCH_LIMIT
        last_outcome, worker_wait, dispatched = self._fill_dispatch_pool(limit, draining)

        # 前进信号终结宽限 episode（须先于死锁仲裁）：任何成功派发都证明
        # 等待者已消解，同缺失集合复发按新 episode 重新授予完整宽限
        if dispatched > 0:
            self.governor.record_dispatch_progress()

        # 2. 处理无可运行 job 与死锁判定
        arbitration = self._arbitrate_deadlock(last_outcome, store)

        # 3. Drain 回收在途结果并统一结算
        settled_count = self._drain_and_settle()

        # 4. 等待/空闲决策（唯一实现见 wait.decide_wait）
        is_idle = store.state.is_empty and not self._in_flight
        decision = decide_wait(LoopFacts(
            stop_mode=self._session.stop_mode, has_in_flight=bool(self._in_flight),
            store_empty=store.state.is_empty, dispatched=dispatched, settled=settled_count,
            has_runnable=(last_outcome.kind is not DispatchKind.NO_CANDIDATE
                          if last_outcome is not None else True),
            min_wait=(last_outcome.standstill.min_wait
                      if last_outcome is not None else float("inf")),
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
                if wait_time > 0:
                    self._wait(wait_time if wait_time != float("inf") else None, last_seq=last_seq)
                elif bool(self._in_flight):
                    self._wait(IN_FLIGHT_POLL_SECONDS, last_seq=last_seq)
                elif store.state.is_empty:
                    break

        logger.info(f"Pipeline {self.config.name} finished.")
        self._in_flight.clear()

    def prepare_run_state(self) -> PipelineState:
        """加载持久化状态、初始化 run_id 屏障、执行恢复修复并构建内存
        PipelineState。

        编排层只负责 run 生命周期操作：调度轮复位与 fencing 屏障（run
        身份是 RunSession 属物，meta 持久化与 channel 同步是横切副作用，
        不下沉机器以免 session 依赖回流）；装载与修复序列归
        RecoveryOrchestrator.load_and_repair。
        """
        self.config.resources.bind_notifier(self._notifier)
        self.scheduler.begin_round()

        self._session.run_id = uuid.uuid4().hex
        self._session.dispatch_no = 0
        try:
            self.backend.set_engine_metadata("last_run_id", self._session.run_id)
        except Exception as e:
            logger.critical(f"Failed to persist run_id to meta table: {e}")
            raise

        return self._recovery.load_and_repair()

    def _run_with_crash_net(self) -> None:
        """运行主事件循环与统一异常承重网。"""
        try:
            self.run_loop()
        except BaseException as e:
            # 单点崩溃网：所有异常同构处理（在途清扫 + 崩溃保队 + 原样上抛）；
            # KI 与非 Exception（_JobTerminated / _CommitCrashSignal / SystemExit）
            # 不附 traceback，其余附。
            self._crash_log(e)
            try:
                self._recovery.abort_in_flight()
            except BaseException as cleanup_error:
                logger.error(f"Abort cleanup failed: {cleanup_error}")
            finally:
                self._recovery.save_queue_crash_safe()
            raise
        finally:
            pass

    @staticmethod
    def _crash_log(e: BaseException) -> None:
        """承重网的逐类型日志分派（KI 与非 Exception 不附 traceback）。"""
        if isinstance(e, KeyboardInterrupt):
            logger.warning("Pipeline interrupted by user.")
        elif isinstance(e, _JobTerminated):
            logger.critical(f"Job terminated outside expected handlers: {e}")
        elif isinstance(e, _CommitCrashSignal):
            logger.critical(f"Backend commit failure; aborting in-flight jobs: {e}")
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
        """strict 预检：全部已注册 Task 的 handler 必须可 pickle。

        spawn 进程模型下不可 pickle 的 handler 会在每次派发时崩溃——
        构造期入口拒绝（fail-loud），而非运行期逐作业失败。
        """
        if not self.config.strict_picklable:
            return
        for task_type in self.tasks.task_types():
            task = self.tasks.lookup(task_type)
            try:
                pickle.dumps(task.handler)
            except Exception as e:
                raise TypeError(
                    f"Handler for task_type '{task_type}' is not picklable: "
                    f"{task.handler!r} ({e}). Functions must be module-level."
                ) from e


__all__ = [
    "DeadlockArbitration",
    "EngineRuntime",
]
