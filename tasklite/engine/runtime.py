"""EngineRuntime：v2 核心运行期深模块（装配 + 薄四相事件泵）。

统一聚合主循环事件泵（四相：回收结算 → 填池派发 → 死锁仲裁 → 等待决策）、
机器群装配、停机信号管理与异常承重网。主进程文件声明与三类授权等待统一
下沉至 ``AuthorizationCoordinator``；一次 run 的生命周期与排除状态归属
``RunSession``。
"""
from __future__ import annotations

import logging
import pickle
import signal
import threading
import time
import traceback
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from typing import Any

from ..backend.base import AbstractStateBackend
from ..exceptions import TaskLiteError
from ..models.state import PipelineState
from ._worker_runtime import stop_signals
from .authorization import AuthorizationCoordinator
from .channel import ExecutionChannel
from .completion import CompletionMachine
from .config import RunConfig
from .dispatch import DispatchKind, DispatchMachine, DispatchOutcome
from .governor import SelectionStandstill
from .in_flight import InFlightTracker
from .recovery import RecoveryOrchestrator
from .resource import RunNotifier
from .scheduler import JobScheduler
from .session import RunSession
from .store import StateStore
from .types import ExecutionOptions, JobHandle, StepOutcome, StopMode
from .wait import LoopFacts, decide_wait

logger = logging.getLogger("tasklite")

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
        self.governor = config.governor

        # 持久会话：execute() 经 begin() 复位（语义等价于每次新建会话，
        # 同时保证机器持有的 session 引用跨 run 稳定）。
        self._session = RunSession()
        self.scheduler = JobScheduler(
            resources=config.resources,
            ordering_policy=config.ordering_policy,
            session=self._session,
        )

        # 构造期即建 StateStore（enqueue 与管理写操作在 run 前可用）。
        # backend 所有权唯一锚定 StateStore，运行期一切读写经 store.backend 派生。
        self.store: StateStore = StateStore(
            config.backend,
            tasks=config.tasks,
            trigger_registry=config.trigger_registry,
            requeue_policy=config.requeue_policy,
            path_policy=config.path_policy,
            session=self._session,
        )

        self.channel: ExecutionChannel = config.channel
        self._in_flight: InFlightTracker = InFlightTracker()
        self._notifier: RunNotifier = RunNotifier()
        self._authorization = AuthorizationCoordinator(
            store=self.store,
            in_flight=self._in_flight,
            session=self._session,
        )

        # 构建机器依赖拓扑（recovery 依赖 completion，三者共享授权协调器）
        self._completion = CompletionMachine(
            store=self.store,
            channel=self.channel,
            resources=config.resources,
            in_flight=self._in_flight,
            session=self._session,
            authorization=self._authorization,
        )
        self._recovery = RecoveryOrchestrator(
            store=self.store,
            channel=self.channel,
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
            completion=self._completion,
            tasks=config.tasks,
            authorization=self._authorization,
            dispatch_start_lock=self._dispatch_start_lock,
        )

        self._is_running: bool = False
        self.channel.set_declaration_handler(self._handle_declaration)
        self.channel.set_authorization_queries(
            is_timeout_paused=self._authorization.is_timeout_paused,
            get_outcome=self._authorization.get_declaration_outcome,
            get_timeout_at=self._authorization.effective_timeout_at,
        )
        self.channel.set_wakeup_callback(self._notifier.notify)

    def _handle_declaration(
        self, handle: JobHandle, payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        """驱动动态声明仲裁并将终止/回复指令交由执行通道落地。"""
        action = self._authorization.handle_declaration(handle, payload)
        if action.terminate:
            self.channel.terminate_process(action.handle)
        return action.reply

    @property
    def session(self) -> RunSession:
        return self._session

    @property
    def authorization(self) -> AuthorizationCoordinator:
        """主进程文件声明与三类授权等待的统一协调器。"""
        return self._authorization

    @property
    def backend(self) -> AbstractStateBackend:
        """持久化后端只读派生引用（所有权唯一锚定 StateStore）。"""
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

    @staticmethod
    def _stop_signals() -> tuple[signal.Signals, ...]:
        """返回当前平台需安装的优雅停机信号列表（委托唯一信号表）。"""
        return stop_signals(signal)

    @contextmanager
    def install_stop_signals(self) -> Iterator[None]:
        """安装平台停机信号并按 LIFO 逆序恢复；安装中途失败自动回滚已装信号。"""
        installed: list[tuple[signal.Signals, Any]] = []
        try:
            for sig in self._stop_signals():
                previous = signal.signal(sig, self._handle_signal)
                installed.append((sig, previous))
        except BaseException:
            for sig, previous in reversed(installed):
                if previous is None:
                    continue
                try:
                    signal.signal(sig, previous)
                except Exception:
                    pass
            raise
        try:
            yield
        finally:
            first_error: BaseException | None = None
            for sig, previous in reversed(installed):
                if previous is None:
                    continue
                try:
                    signal.signal(sig, previous)
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
            if first_error is not None:
                raise first_error

    def request_stop(self) -> StopMode:
        """停机请求接口（单调状态转移，委托 RunSession，取消未提交启动并唤醒运行循环）。"""
        with self._dispatch_start_lock:
            if not self._is_running:
                return self._session.stop_mode
            mode = self._session.request_stop()
        self.channel.cancel_pending_launches()
        for handle in self._authorization.notify_stop():
            self.channel.terminate_process(handle)
        self._notifier.notify()
        return mode

    def execute(self, options: ExecutionOptions | None = None) -> None:
        """完整执行管线生命周期。"""
        opts = options if options is not None else ExecutionOptions()

        # 会话复位与活跃发布必须原子完成；接纳 stop 后本 Run 不再复位会话。
        with self._dispatch_start_lock:
            if self._is_running:
                raise TaskLiteError("Pipeline run() already in progress on this instance.")
            self._session.begin(exclude=opts.exclude)
            self._is_running = True

        try:
            self._authorization.clear_all()
            self.scheduler.begin_round()

            with self.store.exclusive_access():
                try:
                    # 主线程安装信号处理器；安装失败自动回滚，退出按 LIFO 逆序恢复。
                    with (
                        self.install_stop_signals()
                        if opts.install_signals
                        else nullcontext()
                    ):
                        self._preflight_picklable_handlers()
                        self.governor.reset()
                        self._run_body()
                finally:
                    self.config.resources.unbind_notifier()
        finally:
            with self._dispatch_start_lock:
                self._is_running = False

    # ── 四相事件泵 ────────────────────────────────────────────────────

    def _terminal_outcome(self) -> StepOutcome:
        """终态早退（排空完毕 / 空闲完成）的统一形状。"""
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
        """相二·填池派发（仅非 DRAINING、无在途授权等待且未超单步上限）。"""
        last_outcome: DispatchOutcome | None = None
        worker_wait = 0.0
        dispatched = 0
        if not draining:
            if self._authorization.has_in_flight_authorization_wait:
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
        """相三·无可运行候选时的死锁归因（委托 DeadlockGovernor 自闭环仲裁）。"""
        deadlock_detected = False
        should_terminate = False
        wait_seconds = 0.0
        if last_outcome is not None and last_outcome.kind is DispatchKind.NO_CANDIDATE:
            if store.state.is_empty and not self._in_flight:
                should_terminate = True
            elif not self._in_flight:
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
                elif decision.action == "gap_retrying":
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

    def _drain_and_settle(self) -> int:
        """相一·回收在途结果并统一结算，返回本轮完成计数（无在途为 0）。"""
        if not self._in_flight:
            return 0
        handles = [
            h
            for h in self._in_flight.active_handles()
            if not self._authorization.has_pending_cleanup(h.job_uid)
        ]
        completed = self.channel.reap_completed(handles)
        return self._completion.settle_reaped(completed)

    def _next_in_flight_wait(self) -> float | None:
        """返回最近执行超时；授权等待已暂停计时，不计入截止点。"""
        deadlines = [
            self._authorization.effective_timeout_at(handle)
            for handle in self._in_flight.active_handles()
            if not self._authorization.is_timeout_paused(handle)
            and not self._authorization.has_pending_cleanup(handle.job_uid)
        ]
        if not deadlines:
            return None
        return max(0.0, min(deadlines) - time.monotonic())

    def step(self, max_dispatch: int | None = None) -> StepOutcome:
        """非阻塞单步推进事件泵（主循环与单步测试共用的统一事件泵）。"""
        store = self.store

        # 0. 停机门：DRAINING 排空完毕与空闲完成早退
        draining = False
        if self._session.stop_mode is not StopMode.NONE:
            if self._in_flight:
                draining = True
            else:
                logger.info("Pipeline drained.")
                return self._terminal_outcome()

        for action in self._authorization.poll_pending_declarations():
            if action.terminate:
                self.channel.terminate_process(action.handle)
            if action.reply is not None:
                self.channel.send_declaration_reply(
                    action.handle.execution_id, action.reply
                )

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
        decision = decide_wait(
            LoopFacts(
                stop_mode=self._session.stop_mode,
                has_in_flight=bool(self._in_flight),
                store_empty=store.state.is_empty,
                dispatched=dispatched,
                settled=settled_count,
                has_runnable=(
                    last_outcome.kind is not DispatchKind.NO_CANDIDATE
                    if last_outcome is not None
                    else True
                ),
                min_wait=(
                    last_outcome.standstill.min_wait
                    if last_outcome is not None
                    else float("inf")
                ),
                in_flight_wait=self._next_in_flight_wait(),
                authorization_wait=self._authorization.has_pending_authorization,
                worker_wait=worker_wait,
                deadlock_wait=arbitration.wait_seconds,
                should_terminate=arbitration.should_terminate,
                deadlock_detected=arbitration.deadlock_detected,
            )
        )

        exit_reason = (
            self._session.exit_reason().value
            if (is_idle or arbitration.should_terminate)
            else None
        )

        return StepOutcome(
            dispatched_count=dispatched,
            settled_count=settled_count,
            is_idle=is_idle,
            should_wait=decision.should_wait,
            wait_time=decision.wait_time,
            deadlock_detected=arbitration.deadlock_detected,
            stop_mode=self._session.stop_mode,
            should_terminate=arbitration.should_terminate or is_idle,
            exit_reason=exit_reason,
        )

    # ── 主循环与承重网 ────────────────────────────────────────────────

    def _wait(self, timeout: float | None, last_seq: int | None = None) -> None:
        """运行循环调度等待：统一经 _notifier 实现跨线程与 check-to-wait 唤醒。"""
        seq = self._notifier.snapshot() if last_seq is None else last_seq
        self._notifier.wait(seq, timeout=timeout)

    def run_loop(self) -> None:
        """事件驱动主循环：以 step() 统一驱动填池、回收与等待（裸循环）。"""
        store = self.store
        self._in_flight.clear_all(state=store.state)
        self._authorization.clear_all()
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

        logger.info("Pipeline finished.")
        self.channel.wait_for_lingering_workers()
        self._in_flight.clear_all(state=store.state)
        self._authorization.clear_all()

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
            # KI 与非 Exception（如 SystemExit）不附 traceback，其余附。
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
