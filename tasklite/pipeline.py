"""v2 TaskLite 门面：用户唯一入口与六步标准调用顺序。

六步契约（顺序本身是设计的一部分）：初始化 → ``register_resource`` →
``register_task`` / ``register_discovery`` / ``register_trigger`` /
``register_transient_exception`` →
``enqueue`` → ``run`` → ``stop``；管理 API 仅限 ``run()`` 外调用。

装配分区：构造器只做参数编排，backend / 分类器 / 资源 / IPC 通道 /
运行配置 / 运维控制台各自下沉到私有装配方法（每段可独立读测）；
EngineRuntime 静态装配快照见 RunConfig（engine/config.py），run 期
生命周期状态与钩子单一出口见 RunSession（engine/session.py）。
"""
from __future__ import annotations

import logging
import multiprocessing as mp
from pathlib import Path
from typing import Any, Callable, Sequence

from .backend.base import AbstractStateBackend
from .backend.memory import InMemoryStateBackend
from .backend.sqlite_backend import SQLiteStateBackend
from .engine.admission import RequeuePolicy, TriggerRegistry
from .models.trigger import TriggerContext
from .engine.channel import ExecutionChannel
from .engine.config import RunConfig, resolve_tuning
from .engine.errorclass import ErrorClassifier, validate_declared_exception_classes
from .engine.governor import DeadlockGovernor
from .engine.ops import OpsConsole, SuspendEntry
from .engine.resource import CapacityResource, Resource, ResourceManager
from .engine.runtime import EngineRuntime
from .engine.scheduler import OrderingPolicy
from .engine.store import FailureEntry
from .engine.types import ExecutionOptions, RunSummary, RunStats
from .models.job import Job, WORKER_RESOURCE
from .models.task import Task, TaskRegistry
from .selection import Rule, Selection

logger = logging.getLogger("tasklite")

_BACKEND_SQLITE = "sqlite"
_BACKEND_MEMORY = "memory"

# 内部 worker 资源：每个 job 默认占用 1 个 worker 槽位。经
# CapacityResource 实现，复用资源调度逻辑控制并发度；常量本体在
# models/job.py（全仓库唯一定义点）。
_DEFAULT_MAX_WORKERS = 4


def _validate_pipeline_name(name: Any) -> str:
    """管线名入口校验：非空 str 且不含路径分隔符。

    name 派生状态库文件名（{name}_state.db）——含路径分隔符时 SQLite
    库会逃逸 state_dir（持久态与 ipc/ 分离，备份/巡检漏库）；校验先于
    任何目录/库文件副作用，与 task_type 的入口校验惯例同规。
    """
    if not isinstance(name, str) or not name:
        raise TypeError(
            f"name must be a non-empty str, got {type(name).__name__} ({name!r})"
        )
    if "/" in name or "\\" in name:
        raise ValueError(f"name must not contain path separators, got {name!r}")
    return name


_UNSET: Any = object()


class TaskLite:
    """v2 管线编排门面（Task/Job/Attempt 三层模型，六步调用契约）。

    Args:
        name: 管线名，派生状态库文件名（``{name}_state.db``）。
        state_dir: 持久化状态目录（不存在则创建）。
        backend: ``"sqlite"``（默认，ACID 保证）/"memory" 或
            AbstractStateBackend 实例。
        output_roots: 产物根目录（单根或多根序列）；设置后
            declare_output 声明路径被沙盒约束在根内。
        max_workers: 最大并发子进程数——内部 ``__workers__`` 容量资源
            实现，可经 ``register_resource`` 覆盖。
        on_run_start: run() 开始前同步调用（无参数）。
        on_run_end: run() 结束时统一 finally 调用（覆盖正常/中断/崩溃
            全部退出路径），参数为 exit_reason（completed /
            stopped_draining / stopped_aborting / interrupted / error）。
        on_attempt_finished: 每 attempt 收尾时同步调用（同一 job 跨
            重试触发多次），签名 ``(job_uid, *, outcome: AttemptOutcome)``；
            在 stats 更新之后、下一 job 派发之前调用。布尔语义经
            AttemptOutcome 值对象承载（success 与 will_retry 正交）。
        strict_picklable: run() 前对全部已注册 Task 的 handler 做
            pickle 预检（fail-loud）——spawn 上下文要求 handler 模块级
            可 pickle，预检把派发期才爆发的序列化失败提前到入口。
        fatal_exceptions / transient_exceptions: 异常分类声明元组
            （构造期 fail-loud 校验，随执行上下文快照下发子进程）。
        ordering_policy: 队列访问序策略（默认 None → FIFO，唯一解析点在
            RunConfig.resolve）；核心不含排序计算，自定义策略经
            OrderingPolicy 接缝插入，``scan_next_runnable`` 唯一
            选择点结构不变。
        requeue_policy: 重试节奏策略（默认 None → 立即重入队，唯一
            解析点在 RunConfig.resolve）；重入队位置与节奏唯一经
            RequeuePolicy 出口规划。
        dependency_grace_seconds / commit_failure_threshold / deadlock_gap_max_rounds:
            调优标量（可空透传，默认值唯一解析点在 resolve_tuning）。

    钩子契约：同步、主线程执行、必须轻量非阻塞（重活业务方自丢线程
    池）；抛异常只计数（``stats["hook_errors"]``）绝不影响主循环——
    钩子按不可信代码对待。多方订阅由业务自封装分发器，框架不维护
    监听器列表。
    """

    output_roots: Path | list[Path] | None = None

    def __init__(
        self,
        name: str,
        state_dir: str | Path,
        backend: str | AbstractStateBackend = "sqlite",
        output_roots: str | Path | Sequence[str | Path] | None = None,
        max_workers: int = _DEFAULT_MAX_WORKERS,
        *,
        on_run_start: Callable[[], None] | None = None,
        on_run_end: Callable[[str], None] | None = None,
        on_attempt_finished: Callable[..., None] | None = None,
        strict_picklable: bool = True,
        fatal_exceptions: tuple | None = None,
        transient_exceptions: tuple | None = None,
        ordering_policy: OrderingPolicy | None = None,
        requeue_policy: RequeuePolicy | None = None,
        dependency_grace_seconds: float | None = None,
        commit_failure_threshold: int | None = None,
        deadlock_gap_max_rounds: int | None = None,
    ) -> None:
        self.name = _validate_pipeline_name(name)
        self.state_dir = self._prepare_state_dir(state_dir)
        self.output_roots = self._resolve_output_roots(output_roots)
        # 构造期局部搬运：backend 实例只向 RunConfig（装配快照）与
        # StateStore（运行期所有权锚点）各交接一次，门面自身不独立持库。
        backend_instance, self.backend_type = self._build_backend(backend)

        # 注册表按引用共享；运行期守卫限制配置变更窗口。
        self.tasks = TaskRegistry()
        from .triggers import BUILTIN_TRIGGER_FUNCTIONS
        self._triggers = TriggerRegistry(tasks=self.tasks, functions=BUILTIN_TRIGGER_FUNCTIONS)
        self.classifier = self._build_classifier(fatal_exceptions, transient_exceptions)
        self.resources = self._build_resources(self.tasks, max_workers)
        self.ipc_dir, self._process_context = self._prepare_ipc_dir()
        self.strict_picklable = strict_picklable
        self.runtime_config = self._build_run_config(
            backend_instance,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            on_attempt_finished=on_attempt_finished,
            ordering_policy=ordering_policy,
            requeue_policy=requeue_policy,
            dependency_grace_seconds=dependency_grace_seconds,
            commit_failure_threshold=commit_failure_threshold,
            deadlock_gap_max_rounds=deadlock_gap_max_rounds,
        )

        # 核心运行期深模块（StateStore 于其构造期创建，run 前即可经
        # 门面使用 enqueue/管理面）
        self._runtime = EngineRuntime(config=self.runtime_config)

        # 管理段运维接缝（run() 外管理 API 委托目标；backend 经
        # store.backend 派生，不独立持库）
        self._console = OpsConsole(
            store=self._runtime.store,
            classifier=self.classifier,
        )

    # ── 构造期装配分区（各自可独立读测）────────────────────────────

    @staticmethod
    def _prepare_state_dir(state_dir: str | Path) -> Path:
        """状态目录装配：物化 Path 并创建。"""
        path = Path(state_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _resolve_output_roots(
        output_roots: str | Path | Sequence[str | Path] | None,
    ) -> Path | list[Path] | None:
        """产物根装配：单根/多根（多根时声明路径属任一根即过沙盒）。"""
        if output_roots is None:
            return None
        if isinstance(output_roots, (list, tuple)):
            roots = [Path(r).resolve() for r in output_roots]
            for r in roots:
                r.mkdir(parents=True, exist_ok=True)
            return roots
        if isinstance(output_roots, (str, Path)):
            root = Path(output_roots).resolve()
            root.mkdir(parents=True, exist_ok=True)
            return root
        raise TypeError(
            f"output_roots must be str/Path or a sequence thereof, "
            f"got {type(output_roots).__name__}"
        )

    def _build_backend(
        self, backend: str | AbstractStateBackend
    ) -> tuple[AbstractStateBackend, str]:
        """持久化后端装配：字符串名或实例 → (backend, 标准名)。"""
        if isinstance(backend, str):
            if backend == _BACKEND_SQLITE:
                try:
                    return (
                        SQLiteStateBackend(self.state_dir / f"{self.name}_state.db"),
                        _BACKEND_SQLITE,
                    )
                except Exception as e:
                    logger.critical(
                        f"Failed to initialize SQLite backend for '{self.name}' "
                        f"at {self.state_dir}: {e}. Check disk space, directory "
                        f"permissions, and filesystem health."
                    )
                    raise RuntimeError(
                        f"SQLite backend initialization failed for "
                        f"'{self.name}': {e}"
                    ) from e
            if backend == _BACKEND_MEMORY:
                return InMemoryStateBackend(), _BACKEND_MEMORY
            raise ValueError(
                f"Unknown backend: {backend!r}. Supported backends are "
                f"'sqlite' and 'memory'."
            )
        if isinstance(backend, AbstractStateBackend):
            # 命名标准化：内置 SQLite/Memory 实例使用标准名
            if isinstance(backend, SQLiteStateBackend):
                backend_type = _BACKEND_SQLITE
            elif isinstance(backend, InMemoryStateBackend):
                backend_type = _BACKEND_MEMORY
            else:
                backend_type = backend.__class__.__name__
            return backend, backend_type
        raise TypeError(
            f"backend must be 'sqlite', 'memory', or AbstractStateBackend "
            f"instance, got {type(backend).__name__}"
        )

    @staticmethod
    def _build_classifier(
        fatal_exceptions: tuple | None, transient_exceptions: tuple | None
    ) -> ErrorClassifier:
        """错误分类器装配：声明元组物化 + 构造期 fail-loud 校验。

        分类声明是 per-pipeline 实例态（不跨 pipeline/run 累积）；声明
        元组随执行上下文 pickle 下发子进程——非 Exception / 不可 pickle
        类与注册表路径同规在构造期拒绝，不滞后到 spawn 派发才失败。
        """
        fatal = tuple(fatal_exceptions) if fatal_exceptions is not None else None
        transient = (
            tuple(transient_exceptions) if transient_exceptions is not None else None
        )
        validate_declared_exception_classes(fatal, "fatal_exceptions")
        validate_declared_exception_classes(transient, "transient_exceptions")
        return ErrorClassifier(
            fatal_exceptions=fatal, transient_exceptions=transient
        )

    @staticmethod
    def _build_resources(tasks: TaskRegistry, max_workers: int) -> ResourceManager:
        """资源管理器装配：注入 Task 默认资源读口与 worker 容量资源。

        防御 bool 类型：bool 是 int 子类，True < 1 为 False 会静默通过
        再被 float(True) 变成 1 worker——显式拒绝保持类型严格。
        """
        if (
            not isinstance(max_workers, int)
            or isinstance(max_workers, bool)
            or max_workers < 1
        ):
            raise ValueError(
                f"max_workers must be an int >= 1, got {max_workers!r}"
            )
        resources = ResourceManager(tasks=tasks)
        resources[WORKER_RESOURCE] = CapacityResource(
            WORKER_RESOURCE, float(max_workers)
        )
        return resources

    def _prepare_ipc_dir(self) -> tuple[str, mp.context.BaseContext]:
        """IPC 落盘目录装配：state_dir/ipc（子进程结果/信号写文件，
        主进程轮询文件存在，无 mp.Queue 伪阻塞点）。"""
        ipc_dir = str(self.state_dir / "ipc")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)
        return ipc_dir, mp.get_context("spawn")

    def _build_run_config(
        self,
        backend: AbstractStateBackend,
        *,
        on_run_start: Callable[[], None] | None,
        on_run_end: Callable[[str], None] | None,
        on_attempt_finished: Callable[..., None] | None,
        ordering_policy: OrderingPolicy | None,
        requeue_policy: RequeuePolicy | None,
        dependency_grace_seconds: float | None,
        commit_failure_threshold: int | None,
        deadlock_gap_max_rounds: int | None,
    ) -> RunConfig:
        """RunConfig 静态装配快照：调优唯一解析点 + 机器群前置件。

        装配件按引用共享（冻结引用而非拷贝）；调优标量一律可空透传，
        默认值唯一解析点在 resolve_tuning；调度策略（ordering_policy /
        requeue_policy）同为可空透传，None → FIFO / 立即重入队的默认
        收敛在 RunConfig.resolve。backend 在此仅为构造期搬运——运行期
        所有权唯一锚定 StateStore（EngineRuntime 构造 store 时交接）。
        """
        tuning = resolve_tuning(
            dependency_grace_seconds=dependency_grace_seconds,
            commit_failure_threshold=commit_failure_threshold,
            deadlock_gap_max_rounds=deadlock_gap_max_rounds,
        )
        channel = ExecutionChannel(
            self.ipc_dir,
            process_context=self._process_context,
            output_roots=self.output_roots,
            protected_roots=(self.state_dir,),
        )
        return RunConfig.resolve(
            name=self.name,
            ipc_dir=self.ipc_dir,
            protected_roots=(self.state_dir,),
            backend=backend,
            resources=self.resources,
            tasks=self.tasks,
            channel=channel,
            classifier=self.classifier,
            governor=DeadlockGovernor(
                dependency_grace_seconds=tuning.dependency_grace_seconds,
                deadlock_gap_max_rounds=tuning.deadlock_gap_max_rounds,
            ),
            trigger_registry=self._triggers,
            requeue_policy=requeue_policy,
            ordering_policy=ordering_policy,
            output_roots=self.output_roots,
            strict_picklable=self.strict_picklable,
            dependency_grace_seconds=tuning.dependency_grace_seconds,
            commit_failure_threshold=tuning.commit_failure_threshold,
            deadlock_gap_max_rounds=tuning.deadlock_gap_max_rounds,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            on_attempt_finished=on_attempt_finished,
        )

    # ── 运行期只读接缝 ─────────────────────────────────────────────

    @property
    def is_running(self) -> bool:
        """当前管线是否正在执行中。"""
        return self._runtime.is_running

    @property
    def backend(self) -> AbstractStateBackend:
        """当前持久化后端（只读派生自 StateStore 锚点）。"""
        return self._runtime.store.backend

    @backend.setter
    def backend(self, value: AbstractStateBackend) -> None:
        # 入口即校验：仅接受 AbstractStateBackend 实例，非后端对象（后端
        # 名字符串、None、只带个别方法的鸭子对象）一律 fail-loud。鸭子
        # 接受会让契约缺口的伪造后端延迟到运行期才以 AttributeError
        # 半路爆发——契约面必须单一（显式子类化，缺方法在类构造期暴露）。
        if not isinstance(value, AbstractStateBackend):
            raise TypeError(
                f"backend must be an AbstractStateBackend instance "
                f"(use the `backend` constructor argument for "
                f"'sqlite'/'memory'), got {type(value).__name__}"
            )
        # 换库属管理段操作，仅限 run() 外调用（对齐 register_task /
        # register_resource 守卫惯例）：运行期换库会撕裂主循环已装配的
        # 后端引用。换库唯一动作 = 换 StateStore 锚点——runtime / 门面 /
        # OpsConsole 等一切持有者经 store.backend 派生，必然同步。
        self._ensure_not_running("backend")
        self._runtime.store.set_backend(value)

    @property
    def stats(self) -> RunStats:
        return self._runtime.stats

    # 钩子只读直达 RunSession（钩子仅在构造期参数传入）。
    @property
    def on_run_start(self):
        return self._runtime.session.on_run_start

    @property
    def on_attempt_finished(self):
        return self._runtime.session.on_attempt_finished

    @property
    def on_run_end(self):
        return self._runtime.session.on_run_end

    def _ensure_not_running(self, api_name: str) -> None:
        """管理/入队 API 的 run 期间守卫（把文档限制变成代码级 RuntimeError）。"""
        if self.is_running:
            raise RuntimeError(
                f"{api_name}() is only allowed outside run(); "
                f"current run is in progress."
            )

    # ── 注册面（六步之二/三）───────────────────────────────────────

    def register_resource(self, resource: Resource) -> None:
        """注册一个资源调度器。

        ``resource.name`` 已存在（含内部 ``__workers__``）时覆盖并告警；
        覆盖 ``__workers__`` 即改变管线最大并发。
        """
        self._ensure_not_running("register_resource")
        if not isinstance(resource, Resource):
            raise TypeError(
                f"register_resource expects a Resource instance, "
                f"got {type(resource).__name__}"
            )
        if resource.name in self.resources:
            logger.warning(f"Overwriting existing resource '{resource.name}'")
        self.resources[resource.name] = resource

    def register_task(
        self,
        task_or_type: "Task | str",
        handler: Callable[[Job, Any], Any] | None = None,
        *,
        resources: dict[str, float] | None = _UNSET,
        max_retries_per_run: int = _UNSET,
        timeout_seconds: int | float = _UNSET,
        trigger: str = _UNSET,
    ) -> None:
        """注册一个 Task 规格（Task 实例）或按简式参数就地构造规格。

        规格式：首参传 Task 实例，其余参数不得传入——规格字段以 Task
        实例为准，旁置参数静默忽略会让调用方误以为覆盖生效，入口拒绝。

        简式：首参传 task_type str，handler 必填；resources /
        max_retries_per_run / timeout_seconds / trigger 全部 keyword-only，
        经 Task 构造面校验（模型层校验单点，门面不重复实现）。

        重复注册同一 task_type 显式报错（TaskRegistry 契约）——静默覆盖
        会让已入队 Job 挂到新 handler 上，规格与实例错配。
        """
        self._ensure_not_running("register_task")
        if isinstance(task_or_type, Task):
            self._reject_spec_overrides(
                handler=handler,
                resources=resources,
                max_retries_per_run=max_retries_per_run,
                timeout_seconds=timeout_seconds,
                trigger=trigger,
            )
            task = task_or_type
        else:
            task_type = task_or_type
            if handler is None:
                raise TypeError(
                    "handler is required when registering by task_type "
                    f"(got task_type={task_type!r}, handler=None)"
                )
            task = Task(
                task_type,
                handler,
                resources=None if resources is _UNSET else resources,
                max_retries_per_run=3 if max_retries_per_run is _UNSET else max_retries_per_run,
                timeout_seconds=3600 if timeout_seconds is _UNSET else timeout_seconds,
                trigger="once" if trigger is _UNSET else trigger,
            )
        self.tasks.register(task)

    @staticmethod
    def _reject_spec_overrides(
        *,
        handler: Callable[[Job, Any], Any] | None,
        resources: object,
        max_retries_per_run: object,
        timeout_seconds: object,
        trigger: object,
    ) -> None:
        """规格式注册的旁置参数门卫（额外参数一律 ValueError）。"""
        if (
            handler is not None
            or resources is not _UNSET
            or max_retries_per_run is not _UNSET
            or timeout_seconds is not _UNSET
            or trigger is not _UNSET
        ):
            raise ValueError(
                "register_task(Task, ...) takes the spec as-is; extra spec "
                "arguments alongside a Task instance are rejected (they "
                "would be silently ignored). Pass overrides inside the Task."
            )

    def selection(self, *rules: Rule) -> "TaskLite":
        """设置执行选择规则集（pipeline 属性，不是 run() 参数）。

        每次调用**整体替换**规则集，0 参数 = 重置为默认全执行；返回 self
        支持链式。两类规则语义严格区分：

        - ``Exclude(task_type=...)``：本次不执行、原样留队（不写 attempt、不进
          successes/失败档案、不消耗资源与预算），下游同样留队，下一次 run 自动
          恢复执行；仅因排除而停摆时 `run()` 以 COMPLETED 收敛并保留队列。
        - ``Skip(task_type=...)``：终态跳过（移出 queue、不进 successes/失败档案），
          下游整链级联跳过（attempt 记 ``DEPENDENCY_SKIPPED``），下次不再
          执行。

        同一 task_type 同时命中时 Skip 优先。
        """
        self._ensure_not_running("selection")
        self._runtime.set_selection(Selection(rules))
        return self

    def register_trigger(self, name: str, fn: Callable[["TriggerContext"], bool]) -> None:
        """注册主进程触发条件；内置名称与重复名称不允许覆盖。"""
        self._ensure_not_running("register_trigger")
        self._triggers.register(name, fn)

    def register_transient_exception(
        self, exception_cls: "type | Sequence[type]"
    ) -> None:
        """把业务自有异常类注册为瞬态（自动重试），per-pipeline 语义。

        接受单个异常类或类序列（序列即批量注册，等价于逐个调用）；分类
        决策发生在子进程，类必须模块级可 pickle（classifier 入口
        fail-loud 预检）；注册表快照随 JobContext 显式下发子进程。
        """
        self._ensure_not_running("register_transient_exception")
        classes = (
            (exception_cls,)
            if isinstance(exception_cls, type)
            else tuple(exception_cls)
        )
        for cls in classes:
            self.classifier.register_transient(cls)

    # ── 入队与过滤（六步之四前置）──────────────────────────────────

    def filter_jobs_without_success(self, jobs: Sequence[Job]) -> list[Job]:
        """过滤出尚未成功完成的 job（uid 不在 successes 的子集，保留输入顺序）。

        委托 OpsConsole；边界语义（failed 不排除、队列驻留不排除）见其
        docstring。标准姿势：``pipeline.enqueue(pipeline.filter_jobs_without_success(jobs))``。
        """
        self._ensure_not_running("filter_jobs_without_success")
        return self._console.filter_jobs_without_success(jobs)

    def enqueue(self, job: Job) -> bool:
        """入队单个作业，捕获独立快照并校验同 UID 幂等/冲突。

        首次加入队列返回 True；同 UID 已排队且有效配置相同返回 False，
        不同配置抛出 TaskLiteError。
        """
        self._ensure_not_running("enqueue")
        if not isinstance(job, Job):
            raise TypeError(
                f"enqueue() expects a Job, got {type(job).__name__}"
            )
        inserted = self._runtime.store.enqueue_job(job)
        if inserted:
            logger.info("Enqueued 1 job(s).")
        else:
            logger.info("Enqueued 0 job(s), skipped 1 duplicate(s).")
        return inserted

    # ── 管理面（run() 外运维委托）──────────────────────────────────

    def list_suspensions(self) -> list[SuspendEntry]:
        """只读查询当前仍生效的资源挂起。委托 OpsConsole。"""
        self._ensure_not_running("list_suspensions")
        return self._console.list_suspensions()

    def list_failures(self) -> list[FailureEntry]:
        """只读查询失败档案，返回结构化条目。委托 OpsConsole。"""
        self._ensure_not_running("list_failures")
        return self._console.list_failures()

    def clear_failures(
        self,
        task_types: Sequence[str] | None = None,
        *,
        keep_fatal: bool = True,
    ) -> int:
        """从失败档案删除匹配条目。委托 OpsConsole。"""
        self._ensure_not_running("clear_failures")
        return self._console.clear_failures(task_types, keep_fatal=keep_fatal)

    def retry_failure(self, job_uid: str) -> bool:
        """把失败档案中的单个作业移出档案并重新入队（人工补跑）。"""
        self._ensure_not_running("retry_failure")
        return self._console.retry_failure(job_uid)

    def clear_history(
        self,
        targets: str | Sequence[str],
        *,
        where: Sequence[str] = ("successes", "failed"),
        predicate: Callable[[str], bool] | None = None,
    ) -> int:
        """从 successes 和/或失败档案删除条目。委托 OpsConsole。"""
        self._ensure_not_running("clear_history")
        return self._console.clear_history(targets, where=where, predicate=predicate)

    def seed_successes(self, job_uids: Sequence[str]) -> int:
        """把 uid 批量写入 successes（存档迁移标记「已处理」）。委托 OpsConsole。"""
        self._ensure_not_running("seed_successes")
        return self._console.seed_successes(job_uids)

    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（幂等）。委托 OpsConsole。"""
        self._ensure_not_running("seed_cursor")
        self._console.seed_cursor(key, value)

    # ── 运行与停机（六步之五/六）───────────────────────────────────

    def stop(self, *, force: bool = False) -> None:
        """请求管线停止（停机状态机）。

        - ``stop()``（默认 DRAINING）：不再派发新 job，允许当前在途
          自然完成后再退出（优雅停机）。
        - ``stop(force=True)``（ABORTING）：分类消费在途任务——已完成
          （结果文件已落盘、只差 drain 回收）的 job 走完成机器提交（进
          successes/failed，不 kill、不删产出、不 requeue），仅对进行中的
          job 执行 kill + 清半成品 + requeue 后退出。
        """
        self._runtime.request_stop(force=force)

    def run(
        self,
        *,
        install_signals: bool = True,
        acquire_run_lock: bool = True,
    ) -> RunSummary:
        """运行管线直到队列排空（或收到排空/强停请求），返回运行摘要。

        install_signals / acquire_run_lock 逐参透传引擎 ExecutionOptions
        （缺省与引擎一致、默认行为不变）：install_signals=False 不安装
        SIGTERM/SIGINT 停机信号陷阱（宿主进程自管信号时使用）；
        acquire_run_lock=False 跳过 state_dir 级单运行排他文件锁
        （同库多实例的互斥协调移交宿主）。

        未处理异常一律经 raise 通道原样上抛（摘要仅无异常终结时语义
        完整）。
        """
        logger.info(
            f"=== Starting Pipeline: {self.name} "
            f"(Backend: {self.backend_type}) ==="
        )
        return self._runtime.execute(
            ExecutionOptions(
                install_signals=install_signals,
                acquire_run_lock=acquire_run_lock,
            )
        )

    def run_graceful(self) -> RunSummary | None:
        """统一 run + 优雅停机包装：捕获 KeyboardInterrupt 转 DRAINING。

        库函数不改变退出码（无 sys.exit）。正常完成返回 RunSummary；
        收到 KeyboardInterrupt 时请求 stop() 转为优雅停机并返回 None。
        """
        try:
            return self.run()
        except KeyboardInterrupt:
            logger.info("收到 KeyboardInterrupt，请求优雅停机（DRAINING）……")
            return None
        finally:
            try:
                self.stop()
            except Exception:
                logger.exception("run_graceful 收尾 stop 失败")


__all__ = [
    "TaskLite",
]
