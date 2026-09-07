import logging
import multiprocessing as mp
import pickle
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Sequence, Tuple, Union

from .backend.base import AbstractStateBackend, classify_error_type
from .backend.memory import InMemoryStateBackend
from .backend.sqlite_backend import SQLiteStateBackend
from .engine.channel import ExecutionChannel
from .engine.executor import MultiprocessingExecutor
from .engine.inflight import InFlightJob as _InFlightJob, InFlightTracker
from .engine.resource import CapacityResource, Resource, ResourceManager
from .engine.runtime import (
    EngineRuntime,
    RuntimeConfig,
    TaskStats,
    WORKER_RESOURCE,
    inject_worker_resource,
)
from .engine.scheduler import JobScheduler
from .engine.store import StateStore
from .exceptions import TransientRegistry, _CommitCrashSignal, _JobTerminated
from .models.context import TaskContext
from .models.job import Job
from .models.state import PipelineState, uid_from_job_dict
from .taxonomy import validate_resource_amounts
from .utils.jsonutil import dumps, loads

logger = logging.getLogger("tasklite")


class DLQEntry(NamedTuple):
    """DLQ 只读查询（``pipeline.list_dlq()``）返回的结构化条目。

    - ``error_type``：结构化分类（fatal / dependency / deadlock /
      transient_exhausted / no_handler / validation / commit_failure /
      dispatch / unknown）。
    - ``error``：原始错误消息/错误码。
    - ``attempts``：写入 DLQ 的次数（``_attempt`` 计数）。
    - ``failed_at``：最近一次失败时间（UTC ISO 8601；历史行可能为 None）。
    - ``meta``：完整 DLQ payload（只读视图）。
    """

    uid: str
    error_type: str
    error: str
    attempts: int
    failed_at: Optional[str]
    meta: Dict[str, Any]


class HandlerEntry(NamedTuple):
    """注册 handler 的结构化条目——调度器与派发路径按字段名访问。"""
    func: Callable
    default_resources: Dict[str, float]
    payload_schema: Optional[type]

_BACKEND_SQLITE = "sqlite"

# 内部 worker 资源名：每个 job 默认占用 1 个 worker 槽位。
# 通过 CapacityResource 实现，复用现有资源调度逻辑控制并发度。
# 常量本体在 engine.runtime（机器模块不反向 import pipeline）。
_DEFAULT_MAX_WORKERS = 4


class TaskLite:
    """Main pipeline orchestrator for task execution."""

    def __init__(
        self,
        name: str,
        state_dir: Union[str, Path],
        backend: Union[str, AbstractStateBackend] = "sqlite",
        output_root: Union[str, Path, Sequence[Union[str, Path]], None] = None,
        max_workers: int = _DEFAULT_MAX_WORKERS,
        on_run_start: Optional[Callable[[], None]] = None,
        on_run_end: Optional[Callable[[str], None]] = None,
        on_job_completed: Optional[Callable[[str, dict, bool, bool], None]] = None,
        strict_picklable: bool = False,
        fatal_exceptions: Optional[tuple] = None,
        transient_exceptions: Optional[tuple] = None,
        dep_grace_seconds: Optional[float] = None,
        commit_failure_dlq_threshold: Optional[int] = None,
        deadlock_gap_max_rounds: Optional[int] = None,
    ):
        """Initialize the pipeline.

        Args:
            name: Pipeline name, used for state file naming (e.g. ``{name}_state.db``).
            state_dir: Directory for persistent state files. Created if not exists.
            backend: ``"sqlite"`` (default) or an ``AbstractStateBackend`` instance.
                Production must use ``"sqlite"`` for ACID guarantees.
            output_root: Root directory for declared outputs. If set, ``declare_output``
                paths are sandboxed under this root. If ``None``, no sandboxing.
            max_workers: Max concurrent subprocesses. Implemented as internal
                ``CapacityResource("__workers__", N)``; override via ``add_resource``.
            on_run_start: 生命周期钩子——run() 开始前同步调用（无参数）。
            on_run_end: 生命周期钩子——run() 结束时调用（统一 finally，
                覆盖正常/中断/崩溃全部退出路径），参数为 exit_reason:
                ``"completed"`` | ``"stopped_draining"``（stop 请求，等完
                在途后退出）| ``"stopped_aborting"``（stop(force)/二次信号，
                强杀在途立即退出）| ``"interrupted"`` | ``"error"``。
            on_job_completed: 生命周期钩子——每次 attempt 完成时同步调用
                （同一 job 跨重试生命周期会触发多次），参数 (uid, result_meta,
                success, going_to_retry)；``going_to_retry=True`` 表示将退避重试、
                ``False`` 才是终局（成功/DLQ）。在 stats 更新之后、下一 job
                派发之前调用；钩子内读 stats 保证一致。
            strict_picklable: 代码级预检——True 时 run() 前对全部
                handler 做 pickle 预检（fail-loud），False 为默认（保留单测
                lambda 兼容）。

            钩子契约（与防御层同原则）：同步、主线程执行、必须轻量
            非阻塞（重活业务方自丢线程池）；抛异常 → catch + warning +
            ``stats["hook_errors"]`` 计数，**绝不影响主循环**——钩子按不可信
            代码对待。多方订阅由业务自封装分发器，框架不维护监听器列表。
        """
        self.name = name
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self._mp_ctx = mp.get_context("spawn")

        if output_root is not None:
            # 多根支持——list 时声明路径属于任一根即通过
            # 沙盒校验（跨盘输出场景）。
            if isinstance(output_root, (list, tuple)):
                self.output_root = [Path(r).resolve() for r in output_root]
                for r in self.output_root:
                    r.mkdir(parents=True, exist_ok=True)
            else:
                self.output_root = Path(output_root).resolve()
                self.output_root.mkdir(parents=True, exist_ok=True)
        else:
            self.output_root = None

        if isinstance(backend, str):
            self.backend_type = backend
            if backend == _BACKEND_SQLITE:
                try:
                    self._backend = SQLiteStateBackend(self.state_dir / f"{name}_state.db")
                except Exception as e:
                    logger.critical(
                        f"Failed to initialize SQLite backend for '{name}' at "
                        f"{self.state_dir}: {e}. "
                        f"Check disk space, directory permissions, and filesystem health."
                    )
                    raise RuntimeError(
                        f"SQLite backend initialization failed for '{name}': {e}"
                    ) from e
            elif backend == "memory":
                self._backend = InMemoryStateBackend()
            else:
                raise ValueError(
                    f"Unknown backend: {backend!r}. Supported backends are 'sqlite' and 'memory'."
                )
        elif isinstance(backend, AbstractStateBackend):
            # 命名标准化：内置 SQLite/Memory 实例使用标准名称
            self.backend_type = (
                "sqlite" if isinstance(backend, SQLiteStateBackend)
                else "memory" if isinstance(backend, InMemoryStateBackend)
                else backend.__class__.__name__
            )
            self._backend = backend
        else:
            raise TypeError(
                f"backend must be 'sqlite', 'memory', or AbstractStateBackend instance, "
                f"got {type(backend).__name__}"
            )




        # handler -> (func, default_resources, payload_schema)
        self.handlers: Dict[str, HandlerEntry] = {}
        # discovery task_type -> 默认 rerun 策略——enqueue/spawn
        # 时经 apply_discovery_rerun 注入（见 enqueue docstring），使「固定
        # uid 每会话重扫」成为默认。
        self._discovery_rerun: Dict[str, str] = {}
        # 瞬态异常注册表是 **per-pipeline 实例态**——不跨 pipeline/run
        # 累积；子进程只消费 ctx 携带的不可变快照（见
        # register_transient_exception / _dispatch_job）。
        self.transient_registry = TransientRegistry()
        # per-pipeline 异常分类覆盖（None=用 exceptions 模块内置元组）：
        # 确定性/瞬态启发式的成员集合可按 pipeline 定制，快照经 ctx 下发
        # 子进程——与瞬态注册表同一作用域纪律。
        self._fatal_exceptions: Optional[tuple] = (
            tuple(fatal_exceptions) if fatal_exceptions is not None else None)
        self._transient_exceptions: Optional[tuple] = (
            tuple(transient_exceptions) if transient_exceptions is not None else None)
        self.resources: ResourceManager = ResourceManager(handlers=self.handlers)
        # 内部 worker 资源：控制并发度。每个 job 默认占用 1 个 worker 槽位，
        # CapacityResource.used 实时反映 in-flight 占用，scheduler 的
        # can_acquire 自然阻止过度派发。用户可通过 add_resource 覆盖。
        # 防御 bool 类型：bool 是 int 子类，True<1 为 False 会静默通过
        # 再被 float(True) 变成 1 worker——显式拒绝 bool 保持类型严格。
        if (not isinstance(max_workers, int) or isinstance(max_workers, bool)
                or max_workers < 1):
            raise ValueError(f"max_workers must be an int >= 1, got {max_workers!r}")
        self.resources[WORKER_RESOURCE] = CapacityResource(
            WORKER_RESOURCE, float(max_workers)
        )
        # IPC 落盘目录（state_dir/ipc）——子进程结果/信号写文件，
        # 主进程轮询文件存在（无 mp.Queue 伪阻塞点）。
        self.ipc_dir = str(self.state_dir / "ipc")
        Path(self.ipc_dir).mkdir(parents=True, exist_ok=True)
        self.executor = MultiprocessingExecutor(mp_ctx=self._mp_ctx, ipc_dir=self.ipc_dir)
        # 传入 handlers 引用，调度器按「handler 默认资源 ∪ job 资源」检查
        # 可用性，与 _dispatch_job 的实际 acquire 一致（堵住限速/容量绕过）。
        self.scheduler = JobScheduler(self.resources, self.handlers)

        self.strict_picklable = strict_picklable

        # 构建 EngineRuntime 静态装配配置
        self.runtime_config = RuntimeConfig(
            name=self.name,
            ipc_dir=self.ipc_dir,
            output_root=self.output_root,
            strict_picklable=strict_picklable,
            dep_grace_seconds=dep_grace_seconds if dep_grace_seconds is not None else 60.0,
            commit_failure_dlq_threshold=commit_failure_dlq_threshold if commit_failure_dlq_threshold is not None else 3,
            deadlock_gap_max_rounds=deadlock_gap_max_rounds if deadlock_gap_max_rounds is not None else 3,
            fatal_exceptions=self._fatal_exceptions,
            transient_exceptions=self._transient_exceptions,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            on_job_completed=on_job_completed,
        )

        # 核心运行期深模块
        self._runtime = EngineRuntime(
            config=self.runtime_config,
            backend=self._backend,
            resources=self.resources,
            handlers=self.handlers,
            executor=self.executor,
            transient_registry=self.transient_registry,
            discovery_rerun=self._discovery_rerun,
        )

        # 运行上下文与拓扑机器均由 EngineRuntime 统一装配
        self._ctx = self._runtime.ctx
        self.scheduler = self._runtime.scheduler
        self._completion = self._runtime._completion
        self._dispatch = self._runtime._dispatch
        self._recovery = self._runtime._recovery
        self._loop = self._runtime._loop

    # ── 核心深模块与 RunContext 代理属性 ────────────────────────────
    @property
    def runtime(self) -> EngineRuntime:
        """核心运行期深模块接缝。"""
        return self._runtime

    @property
    def store(self) -> StateStore:
        """状态与事务深模块。"""
        return self._ctx.store

    @property
    def channel(self) -> ExecutionChannel:
        """执行通道深模块。"""
        return self._ctx.channel

    @property
    def _failure(self) -> StateStore:
        """向后兼容属性：委托给 StateStore。"""
        return self._ctx.store

    @property
    def _run_started(self) -> bool:
        return self._runtime.is_running

    @_run_started.setter
    def _run_started(self, value: bool) -> None:
        self._runtime._is_running = value
    # 运行态真相源在 self._ctx；这些属性代理保留 TaskLite 的调用面
    # （生产方法、测试直调、monkeypatch 赋值），代理写入即时同步真相源。
    @property
    def backend(self):
        return self._backend

    @backend.setter
    def backend(self, value) -> None:
        # 测试/运维可能替换 backend（如注入 FailingBackend）——RunContext
        # 是运行时真相源，必须同步，避免失败机器仍持旧引用。
        self._backend = value
        if hasattr(self, "_ctx"):
            self._ctx.backend = value
        if hasattr(self, "_runtime"):
            self._runtime.backend = value

    @property
    def stats(self) -> TaskStats:
        return self._ctx.stats

    @stats.setter
    def stats(self, value: dict) -> None:
        self._ctx.stats = value

    @property
    def _state(self):
        return self._ctx.state

    @_state.setter
    def _state(self, value) -> None:
        self._ctx.state = value

    @property
    def _in_flight(self) -> InFlightTracker:
        return self._ctx.in_flight

    @_in_flight.setter
    def _in_flight(self, value: Any) -> None:
        if isinstance(value, InFlightTracker):
            self._ctx.in_flight = value
        else:
            self._ctx.in_flight.clear()
            if value:
                self._ctx.in_flight.update(value)

    @property
    def _deadlock_gap_rounds(self) -> int:
        return self._ctx.deadlock_gap_rounds

    @_deadlock_gap_rounds.setter
    def _deadlock_gap_rounds(self, value: int) -> None:
        self._ctx.deadlock_gap_rounds = value

    @property
    def _dep_grace_missing(self):
        return self._ctx.dep_grace_missing

    @_dep_grace_missing.setter
    def _dep_grace_missing(self, value) -> None:
        self._ctx.dep_grace_missing = value

    @property
    def _dep_grace_deadline(self):
        return self._ctx.dep_grace_deadline

    @_dep_grace_deadline.setter
    def _dep_grace_deadline(self, value) -> None:
        self._ctx.dep_grace_deadline = value

    # 停机状态机经 self._ctx.stop_mode（StopMode 枚举）直接读写。

    @property
    def _run_id(self):
        return self._ctx.run_id

    @_run_id.setter
    def _run_id(self, value) -> None:
        self._ctx.run_id = value

    @property
    def _dispatch_seq(self) -> int:
        return self._ctx.dispatch_seq

    @_dispatch_seq.setter
    def _dispatch_seq(self, value: int) -> None:
        self._ctx.dispatch_seq = value

    @property
    def on_run_start(self):
        return self._ctx.on_run_start

    @on_run_start.setter
    def on_run_start(self, value) -> None:
        self._ctx.on_run_start = value

    @property
    def on_job_completed(self):
        return self._ctx.on_job_completed

    @on_job_completed.setter
    def on_job_completed(self, value) -> None:
        self._ctx.on_job_completed = value

    @property
    def on_run_end(self):
        return self._ctx.on_run_end

    @on_run_end.setter
    def on_run_end(self, value) -> None:
        self._ctx.on_run_end = value

    def _ensure_not_running(self, api_name: str) -> None:
        """管理/入队 API 的 run 期间守卫（把文档限制变成代码级 RuntimeError）。"""
        if self._run_started:
            raise RuntimeError(
                f"{api_name}() is only allowed outside run(); "
                f"current run is in progress. See README『开发与 Agent 约束』."
            )

    def _preflight_picklable_callbacks(self) -> None:
        """strict_picklable=True 时，run 前校验全部 handler 可 pickle（fail-loud）。"""
        if not self.strict_picklable:
            return
        for task_type, entry in self.handlers.items():
            try:
                pickle.dumps(entry.func)
            except Exception as e:
                raise TypeError(
                    f"strict_picklable: handler for task_type '{task_type}' is not "
                    f"module-level picklable: {e}"
                ) from e

    def add_resource(self, resource: Resource) -> None:
        """Add a resource scheduler to the pipeline.

        If ``resource.name`` already exists (including the internal ``__workers__``),
        it is overwritten with a warning. Overriding ``__workers__`` changes the
        max concurrency of the pipeline.
        """
        if not isinstance(resource, Resource):
            raise TypeError(
                f"add_resource expects a Resource instance, "
                f"got {type(resource).__name__}"
            )
        if resource.name in self.resources:
            logger.warning(f"Overwriting existing resource '{resource.name}'")
        self.resources[resource.name] = resource

    def register_handler(
        self,
        task_type: str,
        handler_func: Callable[[Job, TaskContext], Any],
        default_resources: Optional[Dict[str, float]] = None,
        payload_schema: Optional[type] = None,
    ) -> None:
        """
        Register a handler for a task type.

        Handler can return:
        - None (Implies success)
        - True / False
        - dict (Metadata for success)
        - Tuple[bool, dict]
        Raise RetryError to push back to queue.
        Raise FatalError for non-retryable bugs (direct DLQ, no retries).
        Raise Exception to fail and push to DLQ.

        payload_schema: Optional TypedDict class for runtime payload validation.
        Validated BEFORE forking the subprocess. Validation failures go directly to DLQ.
        """
        if not isinstance(task_type, str) or not task_type:
            raise TypeError(
                f"task_type must be a non-empty str, got {type(task_type).__name__} ({task_type!r})"
            )
        if "::" in task_type:
            raise ValueError(f"task_type must not contain '::', got {task_type!r}")
        if not callable(handler_func):
            raise TypeError(f"handler_func must be callable, got {type(handler_func).__name__}")
        if task_type in self.handlers:
            logger.warning(f"Overwriting existing handler for task_type '{task_type}'")
        # 与 Job.__init__ 同级校验 handler 默认资源——负值/NaN 会绕过 Job 构造
        # 校验，在派发时导致 acquire 崩溃或 NaN 污染调度（管线无限空转）；
        # 非 dict 类型与 Job.__init__ 的显式 dict 守卫（job.py）对称，入口拒绝。
        if default_resources is not None and not isinstance(default_resources, dict):
            raise TypeError(
                f"default_resources must be a dict or None, "
                f"got {type(default_resources).__name__}"
            )
        if default_resources:
            self._validate_resource_amounts(default_resources, "default_resources")
        if payload_schema is not None and not isinstance(payload_schema, type):
            # 代码级限制：文档约定 payload_schema 必须是 TypedDict 类；
            # 传实例/字符串等会在运行时校验时静默失效，入口 fail-loud。
            raise TypeError(
                f"payload_schema must be a type (e.g. TypedDict class), "
                f"got {type(payload_schema).__name__}"
            )
        self.handlers[task_type] = HandlerEntry(handler_func, default_resources or {}, payload_schema)

    def set_discovery_rerun(self, task_type: str, rerun: str) -> None:
        """登记 discovery task_type 的默认 rerun 策略。

        ``wrappers.discovery.register_discovery`` 的框架无关适配经本公开方法
        写入默认 rerun——宿主实现细节（`_discovery_rerun` 私有字典）不
        暴露给 discovery 模块。enqueue/spawn 时若 job 未指定 rerun
        （Job.rerun=None 哨兵）则经
        ``apply_discovery_rerun`` 注入该默认值，使固定 uid 的 discovery
        job 每会话重扫；显式值（含 "never"）一律尊重。
        """
        if not isinstance(task_type, str) or not task_type:
            raise TypeError(
                f"task_type must be a non-empty str, got {type(task_type).__name__} ({task_type!r})"
            )
        if "::" in task_type:
            raise ValueError(f"task_type must not contain '::', got {task_type!r}")
        if rerun not in ("never", "on_failure", "every_run", "on_input_change"):
            raise ValueError(
                f"rerun must be one of 'never'/'on_failure'/'every_run'/"
                f"'on_input_change', got {rerun!r}"
            )
        if task_type in self._discovery_rerun:
            logger.warning(
                f"Overwriting discovery rerun for task_type '{task_type}'"
            )
        self._discovery_rerun[task_type] = rerun

    def register_transient_exception(self, exception_cls: type) -> None:
        """把业务自有异常类注册为瞬态（自动重试），**per-pipeline 语义**。

        注册表是本 pipeline 实例态——不同 pipeline 的注册互不可见、
        跨 run 不累积。分类决策发生在子进程，因此类必须为模块级
        可 pickle（入口 fail-loud 预检）；注册表快照随 ``TaskContext``
        显式下发子进程。
        """
        self.transient_registry.register(exception_cls)

    def register_transient_exceptions(self, classes: Sequence[type]) -> None:
        """批量注册瞬态异常类。"""
        for cls in classes:
            self.register_transient_exception(cls)

    def register_file_transients(
        self,
        classes: Sequence[type] = (PermissionError, BlockingIOError, ConnectionResetError),
    ) -> None:
        """把常见文件系统环境异常批量注册为瞬态（可重试）。"""
        self.register_transient_exceptions(classes)


    def enqueue(self, jobs: Union[Job, Sequence[Job]], front: bool = False) -> None:
        """Add jobs to the queue.

        Args:
            jobs: Job 或 Job 列表（单个 Job 会自动包成列表）。重复 uid 静默跳过。
            front: If ``True``, insert at queue head (for requeue-style usage);
                else append to tail (default).

        Note:
            Not thread-safe. Do not call concurrently with ``run()``（见
            README 已知限制）. Use ``ctx.spawn()`` for runtime
            child job generation inside handlers.

        写入走后端 ``enqueue_jobs`` 增量 API（单事务原子插入，不做
        DELETE 全表重写）——与 run() 的 delta commit 并发时互不覆盖。

        discovery task_type 的 job 在入队时注入该 discovery
        注册的默认 rerun（通常 "every_run"）——用户**未指定**（Job.rerun=None，
        哨兵语义）时注入 discovery 默认，使「固定 uid 每会话重扫」成为
        默认姿势；显式指定（含 "never"）一律尊重，不覆盖、不告警。
        """
        self._ensure_not_running("enqueue")
        # 兼容单个 Job 与 list/tuple——与 add_resource/
        # ctx.spawn 的单数语义对齐，避免“必须包一层 []”的非直觉用法。
        if isinstance(jobs, Job):
            jobs_list = [jobs]
        elif isinstance(jobs, (list, tuple)):
            jobs_list = list(jobs)
        else:
            raise TypeError(
                "enqueue() expects a Job or a list of Job objects, "
                f"got {type(jobs).__name__}"
            )
        if not jobs_list:
            return

        jobs_dicts = []
        for j in jobs_list:
            if not isinstance(j, Job):
                raise TypeError(
                    "enqueue() expects Job objects, "
                    f"got {type(j).__name__}"
                )
            # 预检 payload JSON 可序列化，避免子进程 IPC 时崩溃。
            # allow_nan=False 与 ctx.spawn 的 JSON 序列化预检对齐——默认
            # allow_nan=True 会让 float('nan') 通过预检，产出非标准 JSON
            # "Infinity"，下游 json.loads 反序列化出 NaN 污染计算。
            try:
                dumps(j.payload)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"Payload for job {j.uid} is not JSON-serializable: {e}"
                ) from e
            # enqueue 不合并 handler 默认 resources——合并会把注册时的
            # 默认值烤进持久化 job_dict（handler 默认资源变更后磁盘留旧值）；
            # 运行时由 scheduler 的 _effective_resources（扫描可见性）与
            # _dispatch_job（acquire 实际值）两处合并。浅拷贝避免修改传入的 Job 对象。
            job_dict = j.to_dict()
            # discovery 默认 rerun 规范化（策略深模块；None=未指定
            # 哨兵才注入，显式值含 "never" 一律尊重）
            self._ctx.policy.normalize_job_dict(job_dict, j.task_type)
            self._inject_worker_resource(job_dict)
            jobs_dicts.append(job_dict)



        if not jobs_dicts:
            return

        # 增量入队（后端在单事务内去重 + 插入），返回实际插入 uid
        inserted = self.backend.enqueue_jobs(jobs_dicts, front=front)
        skipped = len(jobs_dicts) - len(inserted)
        if skipped:
            logger.info(f"Enqueued {len(inserted)} job(s), skipped {skipped} duplicate(s).")
        elif inserted:
            logger.info(f"Enqueued {len(inserted)} job(s).")

    def list_dlq(self) -> List[DLQEntry]:
        """只读查询 DLQ，返回结构化条目（uid / error_type / error / attempts / failed_at / meta）。

        error_type 分类：fatal（FatalError）/ dependency（级联）/
        deadlock / transient_exhausted（重试耗尽）/ no_handler /
        validation / commit_failure / dispatch / unknown（见 ``backend.base.classify_error_type``）。
        只读，不改变任何状态；仅限 run() 之外调用。
        """
        self._ensure_not_running("list_dlq")
        failed = self.backend.load_failed()
        entries: List[DLQEntry] = []
        for uid, meta in sorted(failed.items()):
            if not isinstance(meta, dict):
                # 与 classify_error_type 同款防御：手改/遗留损坏行不炸掉整个
                # 排障工具——合法 JSON 标量（"boom"/42/true
                # 等）也要兜底：dict(标量) 会抛 TypeError/ValueError，此处
                # 统一按未知分类展示，排障者可修复。
                entries.append(DLQEntry(
                    uid=uid,
                    error_type=classify_error_type(meta),
                    error="",
                    attempts=0,
                    failed_at=None,
                    meta={},
                ))
                continue
            attempts = meta.get("_attempt", 0)
            if not isinstance(attempts, int):
                attempts = 0  # 非 int 的 _attempt（手改/遗留行）不炸 list_dlq()
            entries.append(DLQEntry(
                uid=uid,
                error_type=classify_error_type(meta),
                error=str(meta.get("error", "")),
                attempts=attempts,
                failed_at=meta.get("failed_at"),
                meta=dict(meta),
            ))
        return entries

    def clear_dlq(
        self,
        task_types: Optional[Sequence[str]] = None,
        *,
        keep_fatal: bool = True,
    ) -> int:
        """从 DLQ 删除匹配条目（默认保留 fatal=true 的确定性失败），返回删除数。

        清除 = 删 DLQ + 调用方随后 enqueue 同名任务重跑（is_known 不再
        把该 uid 算「已知」）。``task_types`` 过滤只删这些 task_type 前缀的
        条目（str 列表/tuple）；None = 全部。``keep_fatal=False`` 连 FatalError
        条目一并删除。仅限 run() 之外调用（改变 is_known 判定基础，与 enqueue 同纪律）。
        """
        self._ensure_not_running("clear_dlq")
        if task_types is not None:
            if not isinstance(task_types, (list, tuple)):
                raise TypeError(
                    f"task_types must be a list/tuple of str or None, "
                    f"got {type(task_types).__name__}"
                )
            for t in task_types:
                if not isinstance(t, str) or not t:
                    raise TypeError(
                        f"task_types must contain only non-empty str, got {t!r}"
                    )
            task_types = list(task_types)
        failed = self.backend.load_failed()
        to_delete = [
            uid for uid, meta in failed.items()
            if (task_types is None
                or any(uid.startswith(t + "::") for t in task_types))
            # 非 dict 损坏行（合法 JSON 标量）不炸 revive——
            # 无 fatal 标志可读，按「可删除」处理（删除本身就是修复手段）。
            and not (keep_fatal and isinstance(meta, dict) and meta.get("fatal"))
        ]
        if not to_delete:
            return 0
        return self.backend.delete_failed(to_delete)

    def clear_history(
        self,
        targets: Union[str, Sequence[str]],
        *,
        where: Sequence[str] = ("wall", "failed"),
    ) -> int:
        """从 wall 和/或 DLQ 删除条目——「误删文件强制重下」「历史垃圾清理」的官方通道。

        ``targets``：str 或 str 列表。完整 uid 精确删除；**以 ``::``
        结尾的字符串按前缀匹配**（如 ``"download::"`` 删全部 download 任务）
        ——防止 ``"download"`` 误匹配 ``"downloads::"``（前缀误匹配痛点 ）。
        ``where``：含 ``"wall"`` / ``"failed"`` 的序列，默认两者都清。
        返回删除总数。仅限 run() 之外调用（改变 is_known 判定基础）。
        """
        self._ensure_not_running("clear_history")
        if isinstance(targets, str):
            patterns = [targets]
        elif isinstance(targets, (list, tuple)):
            patterns = list(targets)
        else:
            raise TypeError(
                f"targets must be a str or a list/tuple of str, "
                f"got {type(targets).__name__}"
            )
        for p in patterns:
            if not isinstance(p, str):
                raise TypeError(
                    f"targets must contain only str, got {type(p).__name__} ({p!r})"
                )
        if not isinstance(where, (list, tuple)):
            raise TypeError(
                f"where must be a sequence of 'wall'/'failed', got {type(where).__name__}"
            )
        where_set = set(where)
        unknown = where_set - {"wall", "failed"}
        if unknown:
            raise ValueError(
                f"where contains unknown target(s): {sorted(unknown)!r}; "
                f"allowed: 'wall', 'failed'"
            )

        def _matches(uid: str) -> bool:
            return any(
                uid == p or (p.endswith("::") and uid.startswith(p))
                for p in patterns
            )

        total = 0
        if "wall" in where:
            wall = self.backend.load_wall()
            matched = [u for u in wall if _matches(u)]
            if matched:
                total += self.backend.delete_wall(matched)
        if "failed" in where:
            failed = self.backend.load_failed()
            matched = [u for u in failed if _matches(u)]
            if matched:
                total += self.backend.delete_failed(matched)
        return total

    def seed_wall(self, uids: Sequence[str]) -> int:
        """把 uid 批量写入 wall（存档迁移标记「已处理」），返回实际写入数。

        媒体/数据资产存档迁移（硬链接 + wall 种子）从此不用裸 SQL。
        uid 必须为 ``"task_type::job_id"`` 形式 str，且 task_type/job_id 均
        非空、不含额外 ``::``；meta 为空 dict。仅限 run() 之外调用。
        """
        self._ensure_not_running("seed_wall")
        if not isinstance(uids, (list, tuple)):
            raise TypeError(
                f"uids must be a list/tuple of str, got {type(uids).__name__}"
            )
        for u in uids:
            if not isinstance(u, str) or u.count("::") != 1:
                raise ValueError(
                    f"seed_wall uid must be 'task_type::job_id' str with exactly "
                    f"one '::' separator, got {u!r}"
                )
            task_type, job_id = u.split("::", 1)
            if not task_type or not job_id:
                raise ValueError(
                    f"seed_wall uid must have non-empty task_type and job_id, got {u!r}"
                )
        return self.backend.seed_wall(list(uids))

    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（幂等）——存档迁移/进度书签恢复。

        注意：discovery 的已见判定走 wall/failed（见
        ``wrappers/discovery.py`` 头部），不再使用 cursor——「已见预填」请用
        ``seed_wall``（把 process 任务的 uid 写入 wall）。本方法服务
        通用业务 cursor（``ctx.get_cursor`` 可读）。仅限 run() 之外调用。
        """
        self._ensure_not_running("seed_cursor")
        self.backend.seed_cursor(key, value)

    def _validate_resource_amounts(self, resources: Dict[str, float], where: str) -> None:
        """数值校验转发（与 Job.__init__ 共用 taxonomy 单点）。

        handler 默认资源在注册时即校验，防止负值/NaN/Inf 绕过 Job 构造
        校验后在派发时引发 acquire 崩溃（负值）或调度 NaN 污染（无限空转）。
        """
        validate_resource_amounts(resources, where)

    def _inject_worker_resource(self, job_dict: dict) -> None:
        """给 job_dict 注入默认 worker 槽位（单点实现见 engine.runtime）。"""
        inject_worker_resource(job_dict)

    def stop(self, force: bool = False) -> None:
        """请求管线停止（停机状态机）。

        - ``stop(force=False)``（默认）：**DRAINING**——不再派发新 job，
          允许当前 in-flight 自然完成后再退出（优雅停机）。
        - ``stop(force=True)``：**ABORTING**——分类消费在途任务：**已完成**
          （结果文件已落盘、只差 drain 回收）的 job 走 ``_complete_job`` 提交
          （进 wall/failed，不 kill、不删产出、不 requeue），仅对**进行中**
          的 job 执行 kill + 清半成品 + requeue 后退出。
        """
        self._runtime.request_stop(force=force)

    def _handle_stop_signal(self, signum, frame) -> None:
        """SIGTERM/SIGINT 信号处理器：请求 DRAINING 优雅停机；二次强制 ABORTING。"""
        self._runtime._handle_signal(signum, frame)

    def run(self) -> None:
        """Start the pipeline and run until queue is empty (or a drain/abort stop is requested)."""
        logger.info(f"=== Starting Pipeline: {self.name} (Backend: {self.backend_type}) ===")
        self._runtime.execute()

    def run_graceful(self) -> None:
        """统一 run + 优雅停机包装：捕获 KeyboardInterrupt 并触发 DRAINING 优雅停机。

        库函数不改变退出码（无 sys.exit）。收到 KeyboardInterrupt 时请求 stop()
        转为优雅停机，等待在途任务完成并收尾。
        """
        try:
            self.run()
        except KeyboardInterrupt:
            logger.info("收到 KeyboardInterrupt，请求优雅停机（DRAINING）……")
        finally:
            try:
                self.stop()
            except Exception:
                logger.exception("run_graceful 收尾 stop 失败")

    def _run_body(self) -> None:
        """run() 的实际执行体，由 run() 包裹在 SIGTERM 安装/恢复之间调用。"""
        self._runtime.prepare_run_state()
        self._run_loop()

    # ── 内部组件委托方法（供测试与生命周期直调）─────────────────

    def _run_loop(self) -> None:
        return self._runtime._run_loop()

    def _save_queue_crash_safe(self) -> None:
        return self._recovery.save_queue_crash_safe()

    def _abort_in_flight(self) -> None:
        return self._recovery.abort_in_flight()

    def _release_acquired(self, acquired, uid=None) -> None:
        return self._completion.release_acquired(acquired, uid=uid)

    def _dispatch_job(self, sched):
        return self._dispatch.dispatch_job(sched)

    def _apply_result(self, uid, job, job_dict, result, job_start=None, expect_in_flight=True):
        return self._completion.apply_result(
            uid, job, job_dict, result, job_start=job_start, expect_in_flight=expect_in_flight,
        )


def job_ref(meta: Any) -> str:
    """从 result_meta 提取人类可读引用（#作品id / 画师名 / 任意 id 字段）。"""
    if isinstance(meta, dict):
        if "post_id" in meta:
            return f"#{meta['post_id']}"
        if "artist" in meta:
            return str(meta["artist"])
        if "id" in meta:
            return str(meta["id"])
    return ""


def progress_hook(uid: str, meta: Any, success: bool, going_to_retry: bool) -> None:
    """控制台每 job 终结回调：一行进度输出（标准钩子适配器）。"""
    task_type = uid.split("::", 1)[0]
    ref = job_ref(meta)
    label = f"{task_type} {ref}" if ref else task_type
    if going_to_retry:
        print(f"  ⟳ {label} 失败，退避重试", flush=True)
    elif success:
        print(f"  ✓ {label}", flush=True)
    else:
        print(f"  ✗ {label} → DLQ", flush=True)


def slice_list(
    items: List[Any],
    start: Optional[int],
    count: Optional[int],
    limit: Optional[int],
) -> List[Any]:
    """分批切片：先 limit，再 [start:start+count]。"""
    if limit is not None:
        items = items[:limit]
    if start is not None or count is not None:
        s = start if start is not None else 0
        c = count if count is not None else len(items)
        items = items[s:s + c]
    return items



