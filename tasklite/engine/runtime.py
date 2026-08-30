"""一次 run() 的运行上下文（RunContext）——一次 run 内跨职责共享的
可变运行态真相源。

把「一次 run 内跨职责共享的可变运行态」从 TaskLite 宿主属性收敛为
独立的 ``RunContext``：调度循环 / 派发 / 完成 / 失败机器从同一个
``RunContext`` 取依赖，不依赖宿主类杂散属性。

归属划分：
- 常驻服务引用（构造注入，跨 run 存活）：``backend``/``scheduler``/
  ``resources``/``handlers``/``executor``/``ipc_dir``/``output_root``/
  ``transient_registry``/``discovery_rerun``。
- 每-run 可变运行态（``run()`` 建立）：``state``（PipelineState）、
  ``in_flight``、``run_id``/``dispatch_seq``（fencing）、``stop_mode``
  （停机状态机，单枚举）、``stats``（计数）、episode 态
  ``dep_grace_*``/``deadlock_gap_rounds``。
- 钩子单一出口：``fire_job_completed``（异常隔离 + hook_errors 计数）。
"""

import enum
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING, Tuple, Union

if TYPE_CHECKING:
    from pathlib import Path
    from ..backend.base import AbstractStateBackend
    from .scheduler import Scheduler
    from ..models.resource import Resource
    from ..models.handler import HandlerEntry
    from .executor import SubprocessExecutor
    from ..models.exceptions_registry import TransientExceptionRegistry

from .failure import (
    COMMIT_FAILURE_DLQ_THRESHOLD,
    DEADLOCK_GAP_MAX_ROUNDS,
    DEP_GRACE_SECONDS,
)
from ..models.state import PipelineState
from ..utils.jsonutil import dumps

logger = logging.getLogger("tasklite")


@dataclass
class EpisodeState:
    """一次 run 内跨轮累计的治理状态（死锁宽限与缺口升级跟踪）。"""

    dep_grace_deadline: Optional[float] = None
    dep_grace_missing: Optional[frozenset] = None
    deadlock_gap_rounds: int = 0

    def reset(self) -> None:
        """重置所有 episode 状态。"""
        self.dep_grace_deadline = None
        self.dep_grace_missing = None
        self.deadlock_gap_rounds = 0


class StopMode(enum.Enum):
    """停机状态机三态。

    - NONE：正常运行；
    - DRAINING：请求优雅停机——不派发新 job，等 in-flight 自然完成后退出
      （stop / 首次 SIGTERM/SIGINT）；
    - ABORTING：强制停机——分类消费在途任务（已完成者照常提交），进行中
      者 kill + 清半成品 + requeue（stop(force=True) / 二次信号）。
    """

    NONE = "none"
    DRAINING = "draining"
    ABORTING = "aborting"


# 内部 worker 资源名：每个 job 默认占用 1 个 worker 槽位（放 runtime
# 避免 dispatch/completion 反向 import pipeline）。
WORKER_RESOURCE = "__workers__"

# 资源 suspend 状态在 meta 表的存储键（{资源名: wall-clock 截止时刻}）。
META_RESOURCE_SUSPENDS = "resource_suspends"

# 运行统计初始值（放 runtime 避免循环 import；hook_errors/deferred_orphan
# 是运行期动态键，预置后累加不依赖拼写正确）。
# job_dict["runtime"] 框架寄生键（常量集中管理：字符串字面量散落各处
# 易拼错且无 IDE 提示，统一常量并集中于此）。下划线前缀=框架内部命名空间，
# 业务不得读写。
RT_BACKOFF_UNTIL = "_backoff_until" # monotonic 退避截止（内存态）
RT_BACKOFF_WALL_DEADLINE = "_backoff_wall_deadline" # wall-clock 退避截止（持久化）
RT_COMMIT_FAILURES = "_commit_failures" # commit 失败 3-strike 计数

EMPTY_STATS = {"completed": 0, "failed": 0, "retried": 0, "skipped": 0,
               "hook_errors": 0, "deferred_orphan": 0, "interrupted_reruns": 0,
               # 因上游失败被级联阻断的下游数（JOB_DEPENDENCY）——与真正
               # 执行失败的 failed 分开统计，DLQ 规模不被无辜下游膨胀。
               "cascade_failed": 0}


class TaskStats(dict):
    """运行统计的 dict 子类（仍是 dict，但提供类型化只读属性）。

    ``stats["completed"]`` 与 ``stats.completed`` 两种写法并存（属性
    改善可读性与 IDE 提示）；值始终为 int。
    """

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


def inject_worker_resource(job_dict: dict) -> None:
    """给 job_dict 的 resources 注入默认 worker 槽位（若未显式声明）。

    这样 ``__workers__`` 进入 job_dict 的 resources，scheduler 的
    ``can_acquire`` 检查和 DispatchMachine 的 acquire/release 自然处理它。
    用户可显式声明 ``resources={"__workers__": 2.0}`` 占多槽位。

    ``Job.to_dict()`` 返回的 ``resources`` 是原 Job 对象的引用，这里必须
    用 ``dict(...)`` 拷贝后替换，避免污染传入的 Job 对象。
    """
    resources = dict(job_dict.get("resources", {}))
    if WORKER_RESOURCE not in resources:
        resources[WORKER_RESOURCE] = 1.0
    job_dict["resources"] = resources


class RunContext:
    """一次 run() 的运行上下文：跨职责共享的可变运行态容器。"""

    def __init__(
        self,
        *,
        name: str,
        backend: "AbstractStateBackend",
        scheduler: "Scheduler",
        resources: Dict[str, "Resource"],
        handlers: Dict[str, "HandlerEntry"],
        executor: "SubprocessExecutor",
        ipc_dir: str,
        output_root: Optional[Union[str, "Path"]],
        on_run_start: Optional[Callable[[], None]],
        on_job_completed: Optional[Callable[[str, dict, bool, bool], None]],
        on_run_end: Optional[Callable[[str], None]],
        transient_registry: "TransientExceptionRegistry",
        discovery_rerun: Dict[str, str],
        fatal_exceptions: Optional[Tuple[type, ...]] = None,
        transient_exceptions: Optional[Tuple[type, ...]] = None,
        dep_grace_seconds: Optional[float] = None,
        commit_failure_dlq_threshold: Optional[int] = None,
        deadlock_gap_max_rounds: Optional[int] = None,
    ) -> None:
        # ── 常驻服务引用（构造注入，跨 run 存活）──────────────────
        self.name = name
        self.backend = backend
        self.scheduler = scheduler
        self.resources = resources
        self.handlers = handlers
        self.executor = executor
        self.ipc_dir = ipc_dir
        self.output_root = output_root
        self.transient_registry = transient_registry
        # per-pipeline 异常分类覆盖（None=用 exceptions 模块默认元组）。
        # 与瞬态注册表同纪律：快照随 ctx pickle 下发子进程，分类决策
        # 不读模块级可变全局。
        self.fatal_exceptions: Optional[tuple] = (
            tuple(fatal_exceptions) if fatal_exceptions is not None else None)
        self.transient_exceptions: Optional[tuple] = (
            tuple(transient_exceptions) if transient_exceptions is not None else None)
        self.discovery_rerun = discovery_rerun
        # 引擎阈值可配（支持外部配置：None=沿用 failure.py 模块默认）——
        # 不同业务对「依赖宽限时长 / commit 崩溃容忍度 / 死锁观察轮数」
        # 的合理值差异很大，不应硬编码。
        self.dep_grace_seconds: float = (
            float(dep_grace_seconds) if dep_grace_seconds is not None else DEP_GRACE_SECONDS)
        self.commit_failure_dlq_threshold: int = (
            int(commit_failure_dlq_threshold)
            if commit_failure_dlq_threshold is not None else COMMIT_FAILURE_DLQ_THRESHOLD)
        self.deadlock_gap_max_rounds: int = (
            int(deadlock_gap_max_rounds)
            if deadlock_gap_max_rounds is not None else DEADLOCK_GAP_MAX_ROUNDS)
        # ── 钩子（单一出口 fire_job_completed/fire_run_end，异常隔离）──
        self.on_run_start = on_run_start
        self.on_job_completed = on_job_completed
        self.on_run_end = on_run_end
        # ── 每-run 可变运行态（run 建立）────────────────────
        self.state: Optional[PipelineState] = None
        self.in_flight: Dict[str, Any] = {}
        self.run_id: Optional[str] = None
        self.dispatch_seq: int = 0
        # 停机状态机：单枚举——双独立 bool 可组合出非法状态（force 而未
        # 请求停机），枚举从类型上保证状态合法且互斥。
        self.stop_mode: StopMode = StopMode.NONE
        self.stats: TaskStats = TaskStats()
        # episode 态（一次 run 内跨轮累计，独立容器存储）
        self.episode: EpisodeState = EpisodeState()
        # on_run_end 幂等标志——run 的 finally 与 LoopRunner 都可能触发，
        # 保证整个 run 只调用一次。
        self._run_end_fired = False

    @property
    def dep_grace_deadline(self) -> Optional[float]:
        return self.episode.dep_grace_deadline

    @dep_grace_deadline.setter
    def dep_grace_deadline(self, value: Optional[float]) -> None:
        self.episode.dep_grace_deadline = value

    @property
    def dep_grace_missing(self) -> Optional[frozenset]:
        return self.episode.dep_grace_missing

    @dep_grace_missing.setter
    def dep_grace_missing(self, value: Optional[frozenset]) -> None:
        self.episode.dep_grace_missing = value

    @property
    def deadlock_gap_rounds(self) -> int:
        return self.episode.deadlock_gap_rounds

    @deadlock_gap_rounds.setter
    def deadlock_gap_rounds(self, value: int) -> None:
        self.episode.deadlock_gap_rounds = value

    def reset_episode(self) -> None:
        """重置 episode 态（新 run 开始时调用）。"""
        self.episode.reset()

    def set_state(self, state: PipelineState) -> None:
        """装载本次 run 的 PipelineState（``_run_body`` 加载修复后调用）。"""
        self.state = state

    def persist_resource_suspends_now(self) -> None:
        """把资源挂起截止**即时**持久化到 meta 表。

        挂起的应用点（completion.apply_result /
        recovery.apply_pending_signals）调用本方法即时落盘——仅靠 run
        收尾持久化的话，kill -9/OOM/断电时运行中累计的挂起全部丢失，
        重启后全速重打正在限流本机的 API（README 主打的崩溃安全场景
        失效）。幂等 UPSERT，成本一次小事务，仅在确有挂起时写入。
        RecoveryMachine.persist_resource_suspends（run 收尾单点）保留为
        薄转发以兼容既有调用面与既有契约测试。

        suspend_until/next_available 是 monotonic 时钟（系统重启归零），
        换算为 wall-clock 截止（time.time + 剩余秒），加载时反向换算；
        全部无挂起时写空映射清除旧数据。失败 error 级日志（与
        last_run_id 的 fail-loud 策略对齐——静默丢失正是要消除的）。
        """
        now = time.monotonic()
        wall_now = time.time()
        deadlines = {}
        for res_name, res in self.resources.items():
            # 统一协议访问器 suspended_until（防新增 Resource 实现
            # 静默漏持久化）
            deadline = res.suspended_until()
            if deadline is None:
                continue
            remaining = deadline - now
            if remaining <= 0:
                continue
            deadlines[res_name] = wall_now + remaining
        try:
            self.backend.set_meta(META_RESOURCE_SUSPENDS, dumps(deadlines))
        except Exception as e:
            logger.error(f"Failed to persist resource suspends to meta: {e}")

    def fire_job_completed(
        self, uid: str, meta: Dict[str, Any], success: bool, going_to_retry: bool,
    ) -> None:
        """钩子单一出口：所有 job 终结路径都经此触发。

        钩子按不可信代码对待：抛异常 catch + ``stats["hook_errors"]`` 计数，
        绝不影响主循环。``on_job_completed is None`` 时直接返回。
        """
        if self.on_job_completed is None:
            return
        try:
            self.on_job_completed(uid, meta, success, going_to_retry)
        except Exception as e:
            logger.warning(f"on_job_completed hook raised for {uid}: {e}")
            self.stats["hook_errors"] += 1

    def reset_run_end_fired(self) -> None:
        """重置 on_run_end 幂等标志（新 run 开始时由宿主 pipeline 调用）。

        幂等标志是 RunContext 的私有运行态——跨模块直写私有属性属
        违规访问，经本方法收敛为公开出口。
        """
        self._run_end_fired = False

    def fire_run_end(self, reason: str) -> None:
        """on_run_end 钩子单一出口（run 正常/中断/崩溃统一触发，幂等）。"""
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
