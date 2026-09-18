"""EngineRuntime：TaskLite 核心运行期深模块（装配 + 薄事件泵）。

统一聚合主循环事件泵、五关预检派发、结果收敛事务、崩溃/停机恢复与在途追踪。
装配快照见 RunConfig（engine/config.py）；一次 run 的生命周期状态与钩子
单一出口见 RunSession（engine/session.py）。
"""
from __future__ import annotations

import logging
import pickle
import secrets
import signal
import time
import traceback
import uuid
from typing import Any, Optional, Tuple

logger = logging.getLogger("tasklite")

# 引擎公共值对象单一真相源见 engine/types.py；此处 re-export 维持历史导入路径。
from .types import (  # noqa: E402
    EMPTY_STATS,
    ExecutionOptions,
    ExitReason,
    HandlerEntry,
    RunSummary,
    StepOutcome,
    StopMode,
    TaskStats,
)

# 资源与运行时常量（提前定义避免模块环形导入）
RT_BACKOFF_UNTIL = "_backoff_until"
RT_BACKOFF_WALL_DEADLINE = "_backoff_wall_deadline"
RT_COMMIT_FAILURES = "_commit_failures"

from .config import RunConfig  # noqa: E402
from .pacing import LoopFacts, decide_wait  # noqa: E402
from .governor import (  # noqa: E402
    DEADLOCK_GAP_MAX_ROUNDS,
    DEP_GRACE_SECONDS,
    DeadlockGovernor,
)
from .store import (  # noqa: E402
    COMMIT_FAILURE_DLQ_THRESHOLD,
    StateStore,
)
from .channel import ExecutionChannel  # noqa: E402
from .inflight import InFlightTracker  # noqa: E402
from .policy import ExecutionPolicy  # noqa: E402
from .resource import (  # noqa: E402
    CapacityResource,
    Resource,
    ResourceManager,
)
from .scheduler import JobScheduler  # noqa: E402
from .dispatch import DispatchMachine, DispatchOutcome  # noqa: E402
from .session import RunSession  # noqa: E402
from ..backend.base import AbstractStateBackend  # noqa: E402
from ..exceptions import _CommitCrashSignal, _JobTerminated  # noqa: E402
from ..models.job import Job, JobRuntimeState, WORKER_RESOURCE, inject_worker_resource  # noqa: E402,F401
from ..models.state import PipelineState  # noqa: E402
from ..taxonomy import ErrorTaxonomy  # noqa: E402
from ..utils.lockfile import release_lock, try_acquire_lock  # noqa: E402


class EngineRuntime:
    """TaskLite 核心运行期深模块。

    统一聚合主循环事件泵、五关预检派发、结果收敛事务、崩溃/停机恢复与在途追踪。
    """

    def __init__(self, config: RunConfig) -> None:
        from .completion import CompletionMachine
        from .recovery import RecoveryMachine

        self.config = config
        self.backend = config.backend
        self.handlers = config.handlers
        self.scheduler = JobScheduler(resources=config.resources, handlers=config.handlers)
        self.governor = config.governor

        # 持久会话：execute() 经 begin() 复位（语义等价于每次新建会话，
        # 同时保证机器持有的 session 引用跨 run 稳定）。
        self._session = RunSession(
            on_run_start=config.on_run_start,
            on_job_completed=config.on_job_completed,
            on_run_end=config.on_run_end,
        )

        # 构造期即建 StateStore（enqueue/OpsConsole 在 run 前可用）；
        # on_job_completed 绑定会话方法——钩子后置变更即时生效。
        self.store: StateStore = StateStore(
            self.backend,
            commit_failure_dlq_threshold=config.commit_failure_dlq_threshold,
            taxonomy=config.taxonomy,
            on_job_completed=self._session.fire_job_completed,
            governor=config.governor,
            stats=self._session.stats,
            policy=config.policy,
        )

        self.channel: ExecutionChannel = config.channel
        self._in_flight: InFlightTracker = InFlightTracker()

        # 构建机器依赖拓扑
        self._completion = CompletionMachine(
            store=self.store,
            policy=config.policy,
            channel=self.channel,
            resources=config.resources,
            in_flight=self._in_flight,
            session=self._session,
        )
        self._dispatch = DispatchMachine(
            store=self.store,
            scheduler=self.scheduler,
            policy=config.policy,
            resources=config.resources,
            channel=self.channel,
            in_flight=self._in_flight,
            session=self._session,
            completion=self._completion,
            handlers=config.handlers,
            taxonomy=config.taxonomy,
            output_root=config.output_root,
            ipc_dir=config.ipc_dir,
            commit_failure_dlq_threshold=config.commit_failure_dlq_threshold,
        )
        self._recovery = RecoveryMachine(
            store=self.store,
            channel=self.channel,
            resources=config.resources,
            in_flight=self._in_flight,
            policy=config.policy,
            completion=self._completion,
        )

        self._run_lock_fd: Optional[int] = None
        self._is_running: bool = False

    @property
    def session(self) -> RunSession:
        return self._session

    @property
    def in_flight(self) -> InFlightTracker:
        return self._in_flight

    @property
    def stats(self) -> TaskStats:
        return self._session.stats

    @property
    def stop_mode(self) -> StopMode:
        return self._session.stop_mode

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def state(self) -> Optional[PipelineState]:
        return self.store.state

    @property
    def taxonomy(self) -> ErrorTaxonomy:
        return self.config.taxonomy

    def request_stop(self, force: bool = False) -> StopMode:
        """停机请求接口（单调状态转移，委托 RunSession）。"""
        return self._session.request_stop(force=force)

    def execute(self, options: Optional[ExecutionOptions] = None) -> RunSummary:
        """完整执行管线生命周期。"""
        opts = options or ExecutionOptions()
        start_time = time.monotonic()
        exit_reason = ExitReason.COMPLETED

        if self._is_running:
            raise RuntimeError("Pipeline run() already in progress on this instance.")
        self._is_running = True

        old_sigterm = None
        old_sigint = None
        # 不变式：任何离开 execute() 的路径都必须复位 _is_running 并释放已
        # 获取的锁 fd。初始化段（锁获取 + 信号陷阱）与 begin 前预检
        # （strict_picklable）故障同样走该收尾，但不触发 fire_run_end——
        # run 尚未 begin，on_run_end 只属于已开始的 run（与 on_run_start
        # 起止对称）。
        session_begun = False
        try:
            # 1. 单运行排他文件锁（try_acquire_lock 契约：仅「锁被占」返回
            #    None，权限/磁盘满等环境故障抛 OSError，语义必须上抛不吞）。
            #    fd 返回值立即注册（None 注册无害）：注册前的 KI 窗口是解释器
            #    字节码级、纯 Python 无法消除，此处收敛到最小。
            if opts.acquire_run_lock:
                self._run_lock_fd = try_acquire_lock(
                    self.config.ipc_dir, "__pipeline_run__", timeout=0
                )
                if self._run_lock_fd is None:
                    raise RuntimeError(
                        f"Another run() is in progress for state_dir {self.config.ipc_dir}; "
                        f"concurrent runs on the same state are forbidden."
                    )

            # 2. 信号陷阱
            if opts.install_signals:
                try:
                    old_sigterm = signal.signal(signal.SIGTERM, self._handle_signal)
                except (ValueError, OSError):
                    pass
                try:
                    old_sigint = signal.signal(signal.SIGINT, self._handle_signal)
                except (ValueError, OSError):
                    pass
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
            self._preflight_picklable_callbacks()
            # 新 run 复位：统计/序号/停机态/幂等标志归零；store 记账换新 stats。
            self._session.begin()
            session_begun = True
            self.store.set_stats(self._session.stats)
            self.governor.reset()

            # on_run_start 钩子（单一出口在 RunSession）
            self._session.fire_run_start()

            self._run_body()

            exit_reason = self._session.exit_reason()

        except KeyboardInterrupt as e:
            exit_reason = self._session.exit_reason(e)
            raise
        except BaseException as e:
            exit_reason = self._session.exit_reason(e)
            raise
        finally:
            # on_run_end 仅在对应 on_run_start 已可能触发的 run（已 begin）
            # 上触发：begin 前失败不发 end，杜绝起止不对称。
            if session_begun:
                self._session.fire_run_end(exit_reason.value)
            if self._run_lock_fd is not None:
                release_lock(self._run_lock_fd)
                self._run_lock_fd = None
            self._is_running = False

            if old_sigterm is not None:
                signal.signal(signal.SIGTERM, old_sigterm)
            if old_sigint is not None:
                signal.signal(signal.SIGINT, old_sigint)

        duration = time.monotonic() - start_time
        return RunSummary(
            exit_reason=exit_reason,
            stats=self._session.stats,
            run_id=self._session.run_id or "",
            duration_seconds=duration,
        )

    def _terminal_outcome(self) -> StepOutcome:
        """三个终态早退（ABORTING / 排空完毕 / 空闲完成）的统一形状：
        全零计数 + should_terminate=True + 当前停机态与退出原因透传。"""
        return StepOutcome(
            dispatched_count=0,
            completed_count=0,
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
    ) -> Tuple[Optional[DispatchOutcome], float, int]:
        """填池派发（仅非 DRAINING 且未超单步上限）：逐个 dispatch_next，
        有候选则计数继续，断流/无候选即停；worker_wait 聚合取 min
        （多次资源挂起恢复取最早者）。

        Returns:
            (最后一次派发结果或 None, worker_wait 最小值或 0, 派发计数)
        """
        last_outcome: Optional[DispatchOutcome] = None
        worker_wait = 0.0
        dispatched = 0
        if not draining:
            while dispatched < limit:
                outcome = self._dispatch.dispatch_next()
                last_outcome = outcome
                if outcome.worker_wait > 0:
                    worker_wait = outcome.worker_wait if worker_wait <= 0 else min(worker_wait, outcome.worker_wait)
                if outcome.entry is not None:
                    dispatched += 1
                    continue
                if not outcome.should_continue:
                    break
        return last_outcome, worker_wait, dispatched

    def _arbitrate_deadlock(
        self, last_outcome: Optional[DispatchOutcome], store: StateStore
    ) -> Tuple[bool, bool, float]:
        """无可运行候选时的死锁归因（仅无 in-flight 时仲裁生效）。

        Returns:
            (是否判定死锁, 是否应终止, 宽限/间隙重试等待秒数)
        """
        deadlock_detected = False
        should_terminate = False
        deadlock_wait = 0.0
        if last_outcome is not None and not last_outcome.has_runnable:
            if store.is_empty and not self._in_flight:
                should_terminate = True
            elif not self._in_flight:
                decision = self.governor.arbitrate(last_outcome.standstill, store=self.store)
                if decision.action == "resolved":
                    deadlock_detected = True
                    if decision.should_terminate:
                        should_terminate = True
                elif decision.action in ("grace_waiting", "gap_retrying"):
                    if decision.wait_time > 0:
                        deadlock_wait = decision.wait_time
        return deadlock_detected, should_terminate, deadlock_wait

    def _drain_and_settle(self) -> int:
        """Drain 回收在途结果并统一结算，返回本轮完成计数（无在途为 0）。"""
        if not self._in_flight:
            return 0
        self._recovery.apply_pending_signals()
        handles = self._in_flight.active_handles()
        completed = self.channel.reap_completed(handles)
        return self._completion.settle_reaped(completed)

    def step(self, max_dispatch: Optional[int] = None) -> StepOutcome:
        """非阻塞单步推进事件泵（主循环与单步测试共用的统一事件泵）。

        run 身份（run_id/result_token/空态装载）唯一负责者是
        prepare_run_state（execute() 启动屏障）——store.state 恒非 None
        （构造与 set_state 双点归一化），经 run() 外直达 step 的路径由
        测试显式 set_state 装载，无需惰性引导。
        """
        store = self.store

        # 0. 停机门：ABORTING 强杀在途、DRAINING 排空完毕、空闲完成三早退
        draining = False
        if self._session.stop_mode is not StopMode.NONE:
            if self._session.stop_mode is StopMode.ABORTING:
                logger.warning("Force abort requested. Killing in-flight jobs.")
                self._recovery.abort_in_flight()
                self._recovery.save_queue_crash_safe()
                return self._terminal_outcome()
            if self._in_flight:
                draining = True
            else:
                logger.info("Pipeline drained. Saving queue and exiting.")
                self._recovery.save_queue_crash_safe()
                return self._terminal_outcome()
        if store.is_empty and not self._in_flight:
            return self._terminal_outcome()

        # 1. 填池派发（仅非 DRAINING 状态且未超过单步限制）
        limit = max_dispatch if max_dispatch is not None else 1000000
        last_outcome, worker_wait, dispatched = self._fill_dispatch_pool(limit, draining)

        # 前进信号终结宽限 episode（须先于死锁仲裁）：任何成功派发都证明
        # 等待者已消解，同缺失集合复发按新 episode 重新授予完整宽限
        if dispatched > 0:
            self.governor.note_dispatch_progress()

        # 2. 处理无可运行 job 与死锁判定
        deadlock_detected, should_terminate, deadlock_wait = (
            self._arbitrate_deadlock(last_outcome, store)
        )

        # 3. Drain 回收在途结果并统一结算
        completed_count = self._drain_and_settle()

        # 4. 等待/空闲决策（唯一实现见 pacing.decide_wait）
        is_idle = store.is_empty and not self._in_flight
        decision = decide_wait(LoopFacts(
            stop_mode=self._session.stop_mode, has_in_flight=bool(self._in_flight),
            store_empty=store.is_empty, dispatched=dispatched, completed=completed_count,
            has_runnable=last_outcome.has_runnable if last_outcome is not None else True,
            min_wait=(last_outcome.standstill.min_wait
                      if last_outcome is not None else float("inf")),
            worker_wait=worker_wait, deadlock_wait=deadlock_wait,
            should_terminate=should_terminate,
        ))

        exit_reason = (
            self._session.exit_reason().value if (is_idle or should_terminate) else None
        )

        return StepOutcome(
            dispatched_count=dispatched, completed_count=completed_count,
            is_idle=is_idle, should_wait=decision.should_wait,
            wait_time=decision.wait_time, deadlock_detected=deadlock_detected,
            stop_mode=self._session.stop_mode,
            should_terminate=should_terminate or is_idle, exit_reason=exit_reason,
        )

    def run_loop_impl(self) -> None:
        """事件驱动主循环：以 step() 统一驱动填池、回收与等待。"""
        store = self.store
        self._in_flight.clear()
        while not store.is_empty or self._in_flight:
            outcome = self.step()
            if outcome.should_terminate:
                break
            if outcome.should_wait and outcome.wait_time > 0:
                time.sleep(outcome.wait_time)

        logger.info(f"Pipeline {self.config.name} finished.")
        self._in_flight.clear()

    def prepare_run_state(self) -> PipelineState:
        """加载持久化状态、初始化 run_id 屏障、执行恢复修复并构建内存 PipelineState。"""
        self.scheduler.begin_round()
        wall = self.backend.load_wall()
        failed = self.backend.load_failed()
        cursors = self.backend.load_cursors()
        q_data = self.backend.load_queue()

        # Fencing 屏障（结果认证令牌与 run_id 同生命周期：每 run 轮换并
        # 同步到执行通道，收割/认领/中止三条读取路径共用同一信任锚）
        self._session.run_id = uuid.uuid4().hex
        self._session.result_token = secrets.token_hex(32)
        self.channel.result_token = self._session.result_token
        self._session.dispatch_seq = 0
        try:
            self.backend.set_meta("last_run_id", self._session.run_id)
        except Exception as e:
            logger.critical(f"Failed to persist run_id to meta table: {e}")
            raise

        # 启动期队列整理与资源挂起加载（终态交集先收敛，后续 repair 与
        # 六集合互斥断言都依赖 wall/failed 互斥前提）
        self._recovery.converge_terminal_overlap(wall, failed)
        q_data = self._recovery.repair_queue_on_load(q_data, wall, failed)
        self._recovery.load_resource_suspends()
        self._recovery.salvage_residue_signals()

        state = PipelineState(wall, failed, cursors, q_data)
        self.store.set_state(state)
        return state

    def _run_loop(self) -> None:
        """运行主事件循环与统一异常承重网。"""
        exit_reason = self._session.exit_reason().value
        try:
            self.run_loop_impl()
            exit_reason = self._session.exit_reason().value
        except BaseException as e:
            # 单点崩溃网：所有异常同构处理（exit_reason + 在途清扫 + 崩溃保队
            # + 原样上抛）；五类历史分支仅日志文案/级别不同——KI 与
            # 非 Exception（_JobTerminated/_CommitCrashSignal/SystemExit）
            # 不附 traceback，其余附。_CommitCrashSignal 防误吞不变式在
            # 类继承（BaseException）与 dispatch 的早置 raise，不在此处。
            self._crash_log(e)
            exit_reason = self._session.exit_reason(e).value
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        finally:
            try:
                self._recovery.persist_resource_suspends()
            except Exception as e:
                logger.warning(f"Failed to persist resource suspends: {e}")
            self._session.fire_run_end(exit_reason)

    @staticmethod
    def _crash_log(e: BaseException) -> None:
        """承重网的逐类型日志分派（级别与文案对齐历史行为）。"""
        if isinstance(e, KeyboardInterrupt):
            logger.warning("Pipeline interrupted by user.")
        elif isinstance(e, _JobTerminated):
            logger.critical(f"Job terminated outside expected handlers: {e}")
        elif isinstance(e, _CommitCrashSignal):
            logger.critical(f"Backend commit failure; aborting in-flight jobs: {e}")
        elif isinstance(e, Exception):
            logger.critical(f"Pipeline scheduler crashed with unhandled exception: {e}\n{traceback.format_exc()}")
        else:
            logger.critical(f"Pipeline terminated by {type(e).__name__}: {e}")

    def _run_body(self) -> None:
        """主执行体。"""
        self.prepare_run_state()
        self._run_loop()

    def _handle_signal(self, signum: int, frame: Any) -> None:
        logger.info(f"收到信号 {signum}，更新停机状态机……")
        self.request_stop()

    def _preflight_picklable_callbacks(self) -> None:
        if not self.config.strict_picklable:
            return
        for task_type, entry in self.handlers.items():
            try:
                pickle.dumps(entry.func)
            except Exception as e:
                raise TypeError(
                    f"Handler for task_type '{task_type}' is not picklable: {entry.func!r} ({e}). "
                    f"Functions must be module-level."
                ) from e
