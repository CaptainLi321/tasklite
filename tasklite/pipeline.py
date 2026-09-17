from __future__ import annotations

import logging
import multiprocessing as mp
import time  # 模块内零调用：tests 以 tasklite.pipeline.time 为锚点 monkeypatch sleep/monotonic，保留为补丁接缝
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from .backend.base import AbstractStateBackend
from .backend.memory import InMemoryStateBackend
from .backend.sqlite_backend import SQLiteStateBackend
from .engine.channel import ExecutionChannel
from .engine.config import RunConfig, resolve_tuning
from .engine.policy import ExecutionPolicy
from .engine.governor import DeadlockGovernor
from .engine.resource import CapacityResource, Resource, ResourceManager
from .engine.runtime import (
    EngineRuntime,
    TaskStats,
)
from .engine.store import DLQEntry
from .engine.console import OpsConsole, SuspendEntry
from .engine.types import HandlerEntry
from .models.context import TaskContext
from .models.job import Job, WORKER_RESOURCE
from .taxonomy import (
    ErrorTaxonomy,
    validate_declared_exception_classes,
    validate_resource_amounts,
)

logger = logging.getLogger("tasklite")


_BACKEND_SQLITE = "sqlite"

# 内部 worker 资源名：每个 job 默认占用 1 个 worker 槽位。
# 通过 CapacityResource 实现，复用现有资源调度逻辑控制并发度。
# 常量本体在 models/job.py（全仓库唯一定义点）。
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
                Must be a non-empty str without path separators (fail-loud).
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
        # name 派生状态库文件名（{name}_state.db）——含路径分隔符时
        # SQLite 库逃逸 state_dir（持久态与 ipc/ 分离，备份/巡检漏库）；
        # 校验先于任何目录/库文件副作用，与 task_type 的入口校验惯例同规。
        if not isinstance(name, str) or not name:
            raise TypeError(
                f"name must be a non-empty str, got {type(name).__name__} ({name!r})"
            )
        if "/" in name or "\\" in name:
            raise ValueError(f"name must not contain path separators, got {name!r}")
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
        # 错误分类与瞬态注册是 **per-pipeline 实例态**——不跨 pipeline/run
        # 累积；子进程只消费 ctx 携带的不可变快照（见
        # register_transient_exception / _dispatch_job）。
        self._fatal_exceptions: Optional[tuple] = (
            tuple(fatal_exceptions) if fatal_exceptions is not None else None)
        self._transient_exceptions: Optional[tuple] = (
            tuple(transient_exceptions) if transient_exceptions is not None else None)
        # 声明元组随 ctx pickle 下发子进程——非 Exception / 不可 pickle 类
        # 与注册表路径同规在构造期 fail-loud，不滞后到 spawn 派发才失败。
        validate_declared_exception_classes(
            self._fatal_exceptions, "fatal_exceptions")
        validate_declared_exception_classes(
            self._transient_exceptions, "transient_exceptions")
        self.taxonomy = ErrorTaxonomy(
            fatal_exceptions=self._fatal_exceptions,
            transient_exceptions=self._transient_exceptions,
        )
        self.transient_registry = self.taxonomy
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
        channel = ExecutionChannel(
            mp_ctx=self._mp_ctx, ipc_dir=self.ipc_dir, output_roots=self.output_root
        )

        self.strict_picklable = strict_picklable

        # 构造期即建 governor/policy（装配快照前置件）；默认值唯一解析点
        # 在 config.resolve_tuning（含 deadlock_gap 以 governor 常量为准，
        # 纠正早期门面字面量 3 与常量 5 的双默认值冲突）。
        tuning = resolve_tuning(
            dep_grace_seconds=dep_grace_seconds,
            commit_failure_dlq_threshold=commit_failure_dlq_threshold,
            deadlock_gap_max_rounds=deadlock_gap_max_rounds,
        )
        governor = DeadlockGovernor(
            dep_grace_seconds=tuning.dep_grace_seconds,
            deadlock_gap_max_rounds=tuning.deadlock_gap_max_rounds,
        )
        policy = ExecutionPolicy(self._discovery_rerun)

        # EngineRuntime 静态装配快照（冻结引用而非拷贝）
        self.runtime_config = RunConfig.resolve(
            name=self.name,
            ipc_dir=self.ipc_dir,
            backend=self._backend,
            resources=self.resources,
            handlers=self.handlers,
            channel=channel,
            taxonomy=self.taxonomy,
            discovery_rerun=self._discovery_rerun,
            governor=governor,
            policy=policy,
            output_root=self.output_root,
            strict_picklable=strict_picklable,
            dep_grace_seconds=tuning.dep_grace_seconds,
            commit_failure_dlq_threshold=tuning.commit_failure_dlq_threshold,
            deadlock_gap_max_rounds=tuning.deadlock_gap_max_rounds,
            fatal_exceptions=self._fatal_exceptions,
            transient_exceptions=self._transient_exceptions,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            on_job_completed=on_job_completed,
        )

        # 核心运行期深模块（StateStore 于其构造期创建，run 前即可经门面使用）
        self._runtime = EngineRuntime(config=self.runtime_config)

        # 管理段运维接缝（run() 外管理 API 委托目标）
        self._console = OpsConsole(
            backend=self._backend,
            store=self._runtime.store,
            taxonomy=self.taxonomy,
        )

    # ── 运行期只读接缝 ─────────────────────────────────────────────
    #
    # 深模块（store / scheduler / governor / channel / state / in_flight）
    # 为内部实现，经 self._runtime 直达（手册 §4.1 去留表已登记移除）。

    @property
    def is_running(self) -> bool:
        """检查当前管线是否正在执行中。"""
        return self._runtime.is_running

    @property
    def backend(self) -> AbstractStateBackend:
        return self._backend

    @backend.setter
    def backend(self, value: AbstractStateBackend) -> None:
        # 入口即校验：非后端对象（后端名字符串、None 等）fail-loud，失败
        # 不延迟到管理 API 调用才以 AttributeError 爆发。除 AbstractStateBackend
        # 实例外接受提供读写核心方法的鸭子类型对象（崩溃注入测试的部分
        # 伪造后端 seam，见 tests/engine/test_crash_recovery_regressions.py）。
        if not (
            isinstance(value, AbstractStateBackend)
            or (
                callable(getattr(value, "load_queue", None))
                and callable(getattr(value, "commit_job_success", None))
            )
        ):
            raise TypeError(
                f"backend must be an AbstractStateBackend instance "
                f"(use the `backend` constructor argument for 'sqlite'/'memory'), "
                f"got {type(value).__name__}"
            )
        if hasattr(self, "_runtime"):
            # 换库属管理段操作，仅限 run() 外调用（对齐 add_resource /
            # register_handler 守卫惯例）：运行期换库会撕裂主循环已装配的
            # 后端引用。
            self._ensure_not_running("backend")
            self._runtime.backend = value
            self._runtime.store.set_backend(value)
        self._backend = value
        if hasattr(self, "_console"):
            # 不变式：凡持有后端引用的组件必须随换库同步重绑定，漏绑会使
            # 管理 API（list_dlq/clear_dlq/clear_history/seed_wall/seed_cursor）
            # 静默读写旧库。
            self._console.set_backend(value)

    @property
    def stats(self) -> TaskStats:
        return self._runtime.stats

    # 钩子只读直达 RunSession（钩子仅在构造期参数传入）。
    @property
    def on_run_start(self):
        return self._runtime.session.on_run_start

    @property
    def on_job_completed(self):
        return self._runtime.session.on_job_completed

    @property
    def on_run_end(self):
        return self._runtime.session.on_run_end

    def _ensure_not_running(self, api_name: str) -> None:
        """管理/入队 API 的 run 期间守卫（把文档限制变成代码级 RuntimeError）。"""
        if self.is_running:
            raise RuntimeError(
                f"{api_name}() is only allowed outside run(); "
                f"current run is in progress. See README『开发与 Agent 约束』."
            )

    def add_resource(self, resource: Resource) -> None:
        """Add a resource scheduler to the pipeline.

        If ``resource.name`` already exists (including the internal ``__workers__``),
        it is overwritten with a warning. Overriding ``__workers__`` changes the
        max concurrency of the pipeline.
        """
        self._ensure_not_running("add_resource")
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
        self._ensure_not_running("register_handler")
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
            validate_resource_amounts(default_resources, "default_resources")
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
        self._ensure_not_running("set_discovery_rerun")
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
        self._ensure_not_running("register_transient_exception")
        self.taxonomy.register_transient(exception_cls)

    def register_transient_exceptions(self, classes: Sequence[type]) -> None:
        """批量注册瞬态异常类。"""
        self._ensure_not_running("register_transient_exceptions")
        for cls in classes:
            self.register_transient_exception(cls)

    def register_file_transients(
        self,
        classes: Sequence[type] = (PermissionError, BlockingIOError, ConnectionResetError),
    ) -> None:
        """把常见文件系统环境异常批量注册为瞬态（可重试）。"""
        self.register_transient_exceptions(classes)


    def uncompleted(self, jobs: Sequence[Job]) -> List[Job]:
        """过滤出尚未成功完成的 job（uid 不在 wall 的子集，保留输入顺序）。

        回溯/增量场景的入队前过滤辅助——调用方自行查 wall 过滤时，漏掉
        这一步的 job 会整批撞 wall 以 skipped 统计空转。标准姿势：
        ``pipeline.enqueue(pipeline.uncompleted(jobs))``。

        边界语义：
        - 只按 wall（成功历史）过滤；DLQ（failed）中的 job **不**排除——
          重新入队后是否重跑由 ``Job(rerun=...)`` 在派发层裁决（默认
          ``never`` 静默跳过，``on_failure`` 重跑），此处抢先过滤会吞掉
          重跑策略的豁免语义；
        - 已驻留队列的重复 uid 不排除——队列去重是 ``enqueue`` 自身的
          职责（重复 uid 静默跳过）。
        """
        self._ensure_not_running("uncompleted")
        wall = self._backend.load_wall()
        return [j for j in jobs if j.uid not in wall]

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
        if isinstance(jobs, Job):
            jobs_list = [jobs]
        elif isinstance(jobs, (list, tuple)):
            for j in jobs:
                if not isinstance(j, Job):
                    raise TypeError(
                        f"enqueue() expects Job objects, got {type(j).__name__}"
                    )
            jobs_list = list(jobs)
        else:
            raise TypeError(
                "enqueue() expects a Job or a list of Job objects, "
                f"got {type(jobs).__name__}"
            )
        if not jobs_list:
            return

        inserted = self._runtime.store.enqueue_jobs(jobs_list, front=front)
        skipped = len(jobs_list) - len(inserted)
        if skipped:
            logger.info(f"Enqueued {len(inserted)} job(s), skipped {skipped} duplicate(s).")
        elif inserted:
            logger.info(f"Enqueued {len(inserted)} job(s).")

    def list_suspends(self) -> List[SuspendEntry]:
        """只读查询当前仍生效的资源挂起。委托 OpsConsole。

        Returns:
            List[SuspendEntry]: 每条含 ``resource`` / ``resume_at``（epoch
            秒）/ ``remaining_seconds``（查询时刻快照），按解封时刻升序。
        """
        self._ensure_not_running("list_suspends")
        return self._console.list_suspends()

    def list_dlq(self) -> List[DLQEntry]:
        """只读查询 DLQ，返回结构化条目。委托 OpsConsole。"""
        self._ensure_not_running("list_dlq")
        return self._console.list_dlq()

    def clear_dlq(
        self,
        task_types: Optional[Sequence[str]] = None,
        *,
        keep_fatal: bool = True,
    ) -> int:
        """从 DLQ 删除匹配条目。委托 OpsConsole。"""
        self._ensure_not_running("clear_dlq")
        return self._console.clear_dlq(task_types, keep_fatal=keep_fatal)

    def clear_history(
        self,
        targets: Union[str, Sequence[str]],
        *,
        where: Sequence[str] = ("wall", "failed"),
    ) -> int:
        """从 wall 和/或 DLQ 删除条目。委托 OpsConsole。"""
        self._ensure_not_running("clear_history")
        return self._console.clear_history(targets, where=where)

    def seed_wall(self, uids: Sequence[str]) -> int:
        """把 uid 批量写入 wall。委托 OpsConsole。"""
        self._ensure_not_running("seed_wall")
        return self._console.seed_wall(uids)

    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor。委托 OpsConsole。"""
        self._ensure_not_running("seed_cursor")
        self._console.seed_cursor(key, value)

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
