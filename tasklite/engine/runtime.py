from __future__ import annotations

import enum
import logging
import pickle
import signal
import time
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Sequence, Set, Tuple, Union

logger = logging.getLogger("tasklite")

# 资源与运行时常量（提前定义避免模块环形导入）
WORKER_RESOURCE = "__workers__"
META_RESOURCE_SUSPENDS = "resource_suspends"
RT_BACKOFF_UNTIL = "_backoff_until"
RT_BACKOFF_WALL_DEADLINE = "_backoff_wall_deadline"
RT_COMMIT_FAILURES = "_commit_failures"

EMPTY_STATS = {
    "completed": 0,
    "failed": 0,
    "retried": 0,
    "skipped": 0,
    "hook_errors": 0,
    "deferred_orphan": 0,
    "interrupted_reruns": 0,
    "cascade_failed": 0,
}


def inject_worker_resource(job_dict: dict) -> None:
    """给 job_dict 的 resources 注入默认 worker 槽位。"""
    resources = dict(job_dict.get("resources", {}))
    if WORKER_RESOURCE not in resources:
        resources[WORKER_RESOURCE] = 1.0
    job_dict["resources"] = resources


class StopMode(enum.Enum):
    """停机状态机三态。"""
    NONE = "none"
    DRAINING = "draining"
    ABORTING = "aborting"


class ExitReason(str, enum.Enum):
    """引擎退出原因。"""
    COMPLETED = "completed"
    STOPPED_DRAINING = "stopped_draining"
    STOPPED_ABORTING = "stopped_aborting"
    INTERRUPTED = "interrupted"
    ERROR = "error"


class TaskStats(dict):
    """运行统计字典。"""

    def __init__(self) -> None:
        super().__init__(EMPTY_STATS)

    @property
    def completed(self) -> int:
        return self["completed"]

    @property
    def failed(self) -> int:
        return self["failed"]

    @property
    def retried(self) -> int:
        return self["retried"]

    @property
    def skipped(self) -> int:
        return self["skipped"]

    @property
    def hook_errors(self) -> int:
        return self["hook_errors"]

    @property
    def deferred_orphan(self) -> int:
        return self["deferred_orphan"]

    @property
    def interrupted_reruns(self) -> int:
        return self["interrupted_reruns"]

    @property
    def cascade_failed(self) -> int:
        return self["cascade_failed"]


from .governor import (
    DEADLOCK_GAP_MAX_ROUNDS,
    DEP_GRACE_SECONDS,
    DeadlockGovernor,
    EpisodeState,
)
from .store import (
    COMMIT_FAILURE_DLQ_THRESHOLD,
    StateStore,
)
from .channel import ArtifactCleanupMode, ExecutionChannel, ExecutionResult, JobHandle
from .inflight import InFlightJob, InFlightTracker
from .policy import ExecutionPolicy, PreflightPolicy
from .resource import CapacityResource, Resource, ResourceManager
from .scheduler import DeadlockAttribution, JobScheduler, ScheduleResult
from ..backend.base import AbstractStateBackend
from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.context import TaskContext
from ..models.job import Job, JobRuntimeState
from ..models.state import PipelineState, uid_from_job_dict
from ..taxonomy import ErrorTaxonomy
from ..utils.jsonutil import dumps, loads
from ..utils.lockfile import release_lock, try_acquire_lock


@dataclass(frozen=True)
class RuntimeConfig:
    """EngineRuntime 的静态装配配置规范。"""
    name: str
    ipc_dir: str
    output_root: Optional[Union[Path, Sequence[Path]]] = None
    strict_picklable: bool = False
    dep_grace_seconds: float = DEP_GRACE_SECONDS
    commit_failure_dlq_threshold: int = COMMIT_FAILURE_DLQ_THRESHOLD
    deadlock_gap_max_rounds: int = DEADLOCK_GAP_MAX_ROUNDS
    fatal_exceptions: Optional[Tuple[type, ...]] = None
    transient_exceptions: Optional[Tuple[type, ...]] = None
    on_run_start: Optional[Callable[[], None]] = None
    on_run_end: Optional[Callable[[str], None]] = None
    on_job_completed: Optional[Callable[[str, Dict[str, Any], bool, bool], None]] = None


@dataclass(frozen=True)
class ExecutionOptions:
    """execute() 单次运行的动态选项。"""
    install_signals: bool = True
    acquire_run_lock: bool = True


@dataclass(frozen=True)
class StepOutcome:
    """step() 单步事件泵的执行产物。"""
    dispatched_count: int
    completed_count: int
    is_idle: bool
    should_wait: bool
    wait_time: float
    deadlock_detected: bool
    stop_mode: StopMode
    should_terminate: bool = False
    exit_reason: Optional[str] = None


@dataclass(frozen=True)
class RunSummary:
    """引擎执行完成后的不可变运行摘要。"""
    exit_reason: ExitReason
    stats: "TaskStats"
    run_id: str
    duration_seconds: float
    unhandled_exception: Optional[BaseException] = None


class RunContext:
    """一次 run() 的运行上下文容器。"""

    def __init__(
        self,
        *,
        name: str,
        backend: AbstractStateBackend,
        scheduler: JobScheduler,
        resources: Union[ResourceManager, Dict[str, Resource]],
        handlers: Dict[str, Any],
        channel: Optional[ExecutionChannel] = None,
        ipc_dir: str,
        output_root: Optional[Union[str, Path, Sequence[Path]]] = None,
        on_run_start: Optional[Callable[[], None]] = None,
        on_job_completed: Optional[Callable[[str, dict, bool, bool], None]] = None,
        on_run_end: Optional[Callable[[str], None]] = None,
        transient_registry: Any = None,
        discovery_rerun: Optional[Dict[str, str]] = None,
        fatal_exceptions: Optional[Tuple[type, ...]] = None,
        transient_exceptions: Optional[Tuple[type, ...]] = None,
        dep_grace_seconds: Optional[float] = None,
        commit_failure_dlq_threshold: Optional[int] = None,
        deadlock_gap_max_rounds: Optional[int] = None,
    ) -> None:
        self.name = name
        self._backend = backend
        self.scheduler = scheduler
        self.handlers = handlers
        if isinstance(resources, ResourceManager):
            self.resource_mgr = resources
            self.resources = resources
        else:
            self.resource_mgr = ResourceManager(resources, handlers=handlers)
            self.resources = self.resource_mgr
        self.ipc_dir = ipc_dir
        self.channel = channel if channel is not None else ExecutionChannel(self.ipc_dir)
        self.output_root = output_root
        self.fatal_exceptions = tuple(fatal_exceptions) if fatal_exceptions is not None else None
        self.transient_exceptions = tuple(transient_exceptions) if transient_exceptions is not None else None
        if isinstance(transient_registry, ErrorTaxonomy):
            self.taxonomy = transient_registry
        else:
            self.taxonomy = ErrorTaxonomy(
                fatal_exceptions=self.fatal_exceptions,
                transient_exceptions=self.transient_exceptions,
                transient_registry=transient_registry.snapshot() if hasattr(transient_registry, "snapshot") else transient_registry,
            )
        self.transient_registry = self.taxonomy
        self.discovery_rerun = discovery_rerun if discovery_rerun is not None else {}
        self.policy: ExecutionPolicy = ExecutionPolicy(self.discovery_rerun)

        self.dep_grace_seconds: float = (
            float(dep_grace_seconds) if dep_grace_seconds is not None else DEP_GRACE_SECONDS
        )
        self.commit_failure_dlq_threshold: int = (
            int(commit_failure_dlq_threshold)
            if commit_failure_dlq_threshold is not None else COMMIT_FAILURE_DLQ_THRESHOLD
        )
        self.deadlock_gap_max_rounds: int = (
            int(deadlock_gap_max_rounds)
            if deadlock_gap_max_rounds is not None else DEADLOCK_GAP_MAX_ROUNDS
        )

        self.governor: DeadlockGovernor = DeadlockGovernor(
            dep_grace_seconds=self.dep_grace_seconds,
            deadlock_gap_max_rounds=self.deadlock_gap_max_rounds,
        )
        self.episode: EpisodeState = self.governor

        self.on_run_start = on_run_start
        self.on_job_completed = on_job_completed
        self.on_run_end = on_run_end

        self._stats: TaskStats = TaskStats()
        self.store: StateStore = StateStore(
            self._backend,
            commit_failure_dlq_threshold=self.commit_failure_dlq_threshold,
            taxonomy=self.taxonomy,
            on_job_completed=lambda uid, meta, s, r: self.fire_job_completed(uid, meta, s, r),
            ctx=self,
            governor=self.governor,
            stats=self._stats,
        )

        self._in_flight: InFlightTracker = InFlightTracker()
        self.run_id: Optional[str] = None
        self.dispatch_seq: int = 0
        self.stop_mode: StopMode = StopMode.NONE
        self._run_end_fired = False

    @property
    def stats(self) -> TaskStats:
        return self._stats

    @stats.setter
    def stats(self, value: TaskStats) -> None:
        self._stats = value
        if hasattr(self, "store") and self.store is not None:
            self.store.set_stats(value)

    @property
    def state(self) -> PipelineState:
        return self.store.state

    @state.setter
    def state(self, value: Optional[PipelineState]) -> None:
        self.store.set_state(value)

    @property
    def backend(self) -> AbstractStateBackend:
        return self._backend

    @backend.setter
    def backend(self, value: AbstractStateBackend) -> None:
        self._backend = value
        self.store.set_backend(value)

    @property
    def in_flight(self) -> InFlightTracker:
        return self._in_flight

    @in_flight.setter
    def in_flight(self, value: Any) -> None:
        if isinstance(value, InFlightTracker):
            self._in_flight = value
        elif isinstance(value, Mapping):
            self._in_flight.clear()
            self._in_flight.update(value)
        else:
            self._in_flight.clear()

    @property
    def dep_grace_deadline(self) -> Optional[float]:
        return self.governor.dep_grace_deadline

    @dep_grace_deadline.setter
    def dep_grace_deadline(self, value: Optional[float]) -> None:
        self.governor.dep_grace_deadline = value

    @property
    def dep_grace_missing(self) -> Optional[FrozenSet[str]]:
        return self.governor.dep_grace_missing

    @dep_grace_missing.setter
    def dep_grace_missing(self, value: Optional[FrozenSet[str]]) -> None:
        self.governor.dep_grace_missing = value

    @property
    def deadlock_gap_rounds(self) -> int:
        return self.governor.deadlock_gap_rounds

    @deadlock_gap_rounds.setter
    def deadlock_gap_rounds(self, value: int) -> None:
        self.governor.deadlock_gap_rounds = value

    def reset_episode(self) -> None:
        self.governor.reset()

    def set_state(self, state: PipelineState) -> None:
        self.state = state
        self.store.set_state(state)

    def persist_resource_suspends_now(self) -> None:
        deadlines = self.resource_mgr.collect_suspensions()
        try:
            self.backend.set_meta(META_RESOURCE_SUSPENDS, dumps(deadlines))
        except Exception as e:
            logger.error(f"Failed to persist resource suspends to meta: {e}")

    def fire_job_completed(
        self, uid: str, meta: Dict[str, Any], success: bool, going_to_retry: bool,
    ) -> None:
        if self.on_job_completed is None:
            return
        try:
            self.on_job_completed(uid, meta, success, going_to_retry)
        except Exception as e:
            logger.warning(f"on_job_completed hook raised for {uid}: {e}")
            self.stats["hook_errors"] += 1

    def reset_run_end_fired(self) -> None:
        self._run_end_fired = False

    def fire_run_end(self, reason: str) -> None:
        if self._run_end_fired:
            return
        self._run_end_fired = True
        if self.on_run_end is None:
            return
        try:
            self.on_run_end(reason)
        except Exception as e:
            logger.warning(f"on_run_end hook raised: {e}")
            self.stats["hook_errors"] += 1


class EngineRuntime:
    """TaskLite 核心运行期深模块。

    统一聚合主循环事件泵、五关预检派发、结果收敛事务、崩溃/停机恢复与在途追踪。
    """

    def __init__(
        self,
        config: RuntimeConfig,
        backend: AbstractStateBackend,
        resources: Union[ResourceManager, Dict[str, Resource]],
        handlers: Dict[str, Any],
        transient_registry: Any,
        discovery_rerun: Dict[str, str],
        channel: Optional[ExecutionChannel] = None,
        executor: Any = None,
    ) -> None:
        from .completion import CompletionMachine
        from .dispatch import DispatchMachine
        from .recovery import RecoveryMachine

        self.config = config
        self.backend = backend
        self.handlers = handlers
        self.discovery_rerun = discovery_rerun
        self.transient_registry = transient_registry

        # 构建调度器
        self.scheduler = JobScheduler(resources=resources, handlers=self.handlers)

        # 构建运行上下文
        self._ctx = RunContext(
            name=config.name,
            backend=self.backend,
            scheduler=self.scheduler,
            resources=resources,
            handlers=self.handlers,
            channel=channel,
            ipc_dir=config.ipc_dir,
            output_root=config.output_root,
            on_run_start=config.on_run_start,
            on_job_completed=config.on_job_completed,
            on_run_end=config.on_run_end,
            transient_registry=transient_registry,
            discovery_rerun=discovery_rerun,
            fatal_exceptions=config.fatal_exceptions,
            transient_exceptions=config.transient_exceptions,
            dep_grace_seconds=config.dep_grace_seconds,
            commit_failure_dlq_threshold=config.commit_failure_dlq_threshold,
            deadlock_gap_max_rounds=config.deadlock_gap_max_rounds,
        )

        # 构建机器依赖拓扑
        self._completion = CompletionMachine(self._ctx)
        self._dispatch = DispatchMachine(self._ctx, self._completion)
        self._recovery = RecoveryMachine(self._ctx, self._completion)

        self._run_lock_fd: Optional[int] = None
        self._is_running: bool = False

    @property
    def ctx(self) -> RunContext:
        return self._ctx

    @property
    def channel(self) -> ExecutionChannel:
        return self._ctx.channel

    @property
    def stats(self) -> TaskStats:
        return self._ctx.stats

    @property
    def stop_mode(self) -> StopMode:
        return self._ctx.stop_mode

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def state(self) -> Optional[PipelineState]:
        return self._ctx.state

    @property
    def store(self) -> StateStore:
        return self._ctx.store

    @property
    def taxonomy(self) -> ErrorTaxonomy:
        return self._ctx.taxonomy

    def request_stop(self, force: bool = False) -> StopMode:
        """停机请求接口（单调状态转移）。"""
        if force or self._ctx.stop_mode == StopMode.DRAINING:
            self._ctx.stop_mode = StopMode.ABORTING
            logger.info("停机状态升级为 ABORTING（强制终止在途任务）")
        elif self._ctx.stop_mode == StopMode.NONE:
            self._ctx.stop_mode = StopMode.DRAINING
            logger.info("停机状态设置为 DRAINING（等待在途任务完成）")
        return self._ctx.stop_mode

    def execute(self, options: Optional[ExecutionOptions] = None) -> RunSummary:
        """完整执行管线生命周期。"""
        opts = options or ExecutionOptions()
        start_time = time.monotonic()
        exit_reason = ExitReason.COMPLETED
        unhandled_exc: Optional[BaseException] = None

        if self._is_running:
            raise RuntimeError("Pipeline run() already in progress on this instance.")
        self._is_running = True

        # 1. 单运行排他文件锁
        if opts.acquire_run_lock:
            lock_fd = try_acquire_lock(self._ctx.ipc_dir, "__pipeline_run__", timeout=0)
            if lock_fd is None:
                self._is_running = False
                raise RuntimeError(
                    f"Another run() is in progress for state_dir {self._ctx.ipc_dir}; "
                    f"concurrent runs on the same state are forbidden."
                )
            self._run_lock_fd = lock_fd

        # 2. 信号陷阱
        old_sigterm = None
        old_sigint = None
        if opts.install_signals:
            try:
                old_sigterm = signal.signal(signal.SIGTERM, self._handle_signal)
            except (ValueError, OSError):
                pass
            try:
                old_sigint = signal.signal(signal.SIGINT, self._handle_signal)
            except (ValueError, OSError):
                pass

        try:
            self._preflight_picklable_callbacks()
            self._ctx.stats = TaskStats()
            self._ctx.reset_episode()
            self._ctx.reset_run_end_fired()
            self._ctx.stop_mode = StopMode.NONE

            # on_run_start 钩子
            if self.config.on_run_start is not None:
                try:
                    self.config.on_run_start()
                except Exception as e:
                    logger.warning(f"on_run_start hook raised: {e}")
                    self._ctx.stats["hook_errors"] += 1

            self._run_body()

            if self._ctx.stop_mode == StopMode.ABORTING:
                exit_reason = ExitReason.STOPPED_ABORTING
            elif self._ctx.stop_mode == StopMode.DRAINING:
                exit_reason = ExitReason.STOPPED_DRAINING
            else:
                exit_reason = ExitReason.COMPLETED

        except KeyboardInterrupt as e:
            exit_reason = ExitReason.INTERRUPTED
            unhandled_exc = e
            raise
        except BaseException as e:
            exit_reason = ExitReason.ERROR
            unhandled_exc = e
            raise
        finally:
            self._ctx.fire_run_end(exit_reason.value)
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
            stats=self._ctx.stats,
            run_id=self._ctx.run_id or "",
            duration_seconds=duration,
            unhandled_exception=unhandled_exc,
        )

    def step(self, max_dispatch: Optional[int] = None) -> StepOutcome:
        """非阻塞单步推进事件泵（主循环与单步测试共用的统一事件泵）。"""
        if self._ctx.state is None:
            self._ctx.run_id = self._ctx.run_id or uuid.uuid4().hex
            self._ctx.set_state(PipelineState({}, {}, {}, []))

        store = self._ctx.store
        if store is None:
            return StepOutcome(
                dispatched_count=0,
                completed_count=0,
                is_idle=True,
                should_wait=False,
                wait_time=0.0,
                deadlock_detected=False,
                stop_mode=self._ctx.stop_mode,
                should_terminate=True,
                exit_reason="completed",
            )

        # 0. 停机模式检查
        draining = False
        if self._ctx.stop_mode is not StopMode.NONE:
            if self._ctx.stop_mode is StopMode.ABORTING:
                logger.warning("Force abort requested. Killing in-flight jobs.")
                self._recovery.abort_in_flight()
                self._recovery.save_queue_crash_safe()
                return StepOutcome(
                    dispatched_count=0,
                    completed_count=0,
                    is_idle=True,
                    should_wait=False,
                    wait_time=0.0,
                    deadlock_detected=False,
                    stop_mode=self._ctx.stop_mode,
                    should_terminate=True,
                    exit_reason="stopped_aborting",
                )
            if self._ctx.in_flight:
                draining = True
            else:
                logger.info("Pipeline drained. Saving queue and exiting.")
                self._recovery.save_queue_crash_safe()
                return StepOutcome(
                    dispatched_count=0,
                    completed_count=0,
                    is_idle=True,
                    should_wait=False,
                    wait_time=0.0,
                    deadlock_detected=False,
                    stop_mode=self._ctx.stop_mode,
                    should_terminate=True,
                    exit_reason="stopped_draining",
                )

        if store.is_empty and not self._ctx.in_flight:
            return StepOutcome(
                dispatched_count=0,
                completed_count=0,
                is_idle=True,
                should_wait=False,
                wait_time=0.0,
                deadlock_detected=False,
                stop_mode=self._ctx.stop_mode,
                should_terminate=True,
                exit_reason="completed",
            )

        # 1. 填池派发（仅非 DRAINING 状态且未超过单步限制）
        last_outcome: Optional[Any] = None
        worker_wait = 0.0
        dispatched = 0
        limit = max_dispatch if max_dispatch is not None else 1000000
        if not draining:
            while dispatched < limit:
                outcome = self._dispatch.dispatch_next()
                last_outcome = outcome
                if outcome.worker_wait > 0:
                    worker_wait = outcome.worker_wait
                if outcome.entry is not None:
                    dispatched += 1
                    continue
                if not outcome.should_continue:
                    break

        # 2. 处理无可运行 job 与死锁判定
        deadlock_detected = False
        should_terminate = False
        deadlock_wait = 0.0
        if last_outcome is not None and not last_outcome.has_runnable:
            if store.is_empty and not self._ctx.in_flight:
                should_terminate = True
            elif not self._ctx.in_flight:
                decision = self._ctx.governor.arbitrate(
                    last_outcome,
                    store=self._ctx.store,
                )
                if decision.action == "resolved":
                    deadlock_detected = True
                    if decision.should_terminate:
                        should_terminate = True
                elif decision.action in ("grace_waiting", "gap_retrying"):
                    if decision.wait_time > 0:
                        deadlock_wait = decision.wait_time

        # 3. Drain 回收在途结果并统一结算
        completed_count = 0
        if self._ctx.in_flight:
            self._recovery.apply_pending_signals()
            handles = self._ctx.in_flight.active_handles()
            completed = self._ctx.channel.reap_completed(handles)
            completed_count = self._completion.settle_reaped(completed)

        # 4. 计算等待时延与空闲状态
        is_idle = store.is_empty and not self._ctx.in_flight
        if is_idle or should_terminate:
            wait_time = 0.0
            should_wait = False
        elif self._ctx.in_flight:
            if completed_count == 0:
                wait_time = 0.05
                should_wait = True
            else:
                wait_time = 0.0
                should_wait = False
        elif deadlock_wait > 0 and not self._ctx.in_flight:
            wait_time = min(deadlock_wait, 1.0)
            should_wait = True
        elif last_outcome is not None and not last_outcome.has_runnable and last_outcome.min_wait != float("inf"):
            wait_time = min(last_outcome.min_wait, 1.0)
            should_wait = True
        elif worker_wait > 0 and not self._ctx.in_flight:
            wait_time = min(worker_wait, 1.0)
            should_wait = True
        else:
            wait_time = 0.0
            should_wait = not is_idle and dispatched == 0 and completed_count == 0

        exit_reason = None
        if is_idle or should_terminate:
            if self._ctx.stop_mode is StopMode.ABORTING:
                exit_reason = "stopped_aborting"
            elif self._ctx.stop_mode is StopMode.DRAINING:
                exit_reason = "stopped_draining"
            else:
                exit_reason = "completed"

        return StepOutcome(
            dispatched_count=dispatched,
            completed_count=completed_count,
            is_idle=is_idle,
            should_wait=should_wait,
            wait_time=wait_time,
            deadlock_detected=deadlock_detected,
            stop_mode=self._ctx.stop_mode,
            should_terminate=should_terminate or is_idle,
            exit_reason=exit_reason,
        )

    def run_loop_impl(self) -> None:
        """事件驱动主循环：以 step() 统一驱动填池、回收与等待。"""
        store = self._ctx.store
        self._ctx.in_flight.clear()
        while not store.is_empty or self._ctx.in_flight:
            outcome = self.step()
            if outcome.should_terminate:
                break
            if outcome.should_wait and outcome.wait_time > 0:
                time.sleep(outcome.wait_time)

        logger.info(f"Pipeline {self._ctx.name} finished.")
        self._ctx.in_flight.clear()

    def prepare_run_state(self) -> PipelineState:
        """加载持久化状态、初始化 run_id 屏障、执行恢复修复并构建内存 PipelineState。"""
        self.scheduler.begin_round()
        wall = self.backend.load_wall()
        failed = self.backend.load_failed()
        cursors = self.backend.load_cursors()
        q_data = self.backend.load_queue()

        # Fencing 屏障
        self._ctx.run_id = uuid.uuid4().hex
        self._ctx.dispatch_seq = 0
        try:
            self.backend.set_meta("last_run_id", self._ctx.run_id)
        except Exception as e:
            logger.critical(f"Failed to persist run_id to meta table: {e}")
            raise

        # 启动期队列整理与资源挂起加载
        q_data = self._recovery.repair_queue_on_load(q_data, wall, failed)
        self._recovery.load_resource_suspends()

        state = PipelineState(wall, failed, cursors, q_data)
        self._ctx.set_state(state)
        return state

    def _run_loop(self) -> None:
        """运行主事件循环与统一异常承重网。"""
        exit_reason = "completed"
        try:
            self.run_loop_impl()
            if self._ctx.stop_mode is StopMode.ABORTING:
                exit_reason = "stopped_aborting"
            elif self._ctx.stop_mode is StopMode.DRAINING:
                exit_reason = "stopped_draining"
        except _JobTerminated as e:
            logger.critical(f"Job terminated outside expected handlers: {e}")
            exit_reason = "error"
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted by user.")
            exit_reason = "interrupted"
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        except _CommitCrashSignal as e:
            logger.critical(f"Backend commit failure; aborting in-flight jobs: {e}")
            exit_reason = "error"
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        except Exception as e:
            logger.critical(f"Pipeline scheduler crashed with unhandled exception: {e}\n{traceback.format_exc()}")
            exit_reason = "error"
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        except BaseException as e:
            logger.critical(f"Pipeline terminated by {type(e).__name__}: {e}")
            exit_reason = "error"
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        finally:
            try:
                self._recovery.persist_resource_suspends()
            except Exception as e:
                logger.warning(f"Failed to persist resource suspends: {e}")
            self._ctx.fire_run_end(exit_reason)

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
