"""TaskLite 门面：用户唯一入口与标准调用顺序。

标准调用顺序：初始化 → ``register_resource`` →
``register_task`` / ``register_trigger`` →
``enqueue`` → ``run`` → ``stop``；管理 API 仅限 ``run()`` 外调用。
"""
from __future__ import annotations

import contextlib
import logging
import multiprocessing as mp
import os
import threading
from pathlib import Path
from typing import Any, Callable, Collection, Literal, Sequence

from .backend.base import AbstractStateBackend
from .backend.memory import InMemoryStateBackend
from .backend.sqlite_backend import SQLiteStateBackend, TARGET_SCHEMA_VERSION
from .engine.admission import TriggerRegistry
from .models.trigger import TriggerContext
from .engine.channel import ExecutionChannel
from .engine.config import RunConfig, resolve_tuning
from .engine.errorclass import ErrorClassifier
from .engine.governor import DeadlockGovernor
from .engine.ops import OpsConsole
from .engine.resource import CapacityResource, Resource, ResourceManager
from .engine.runtime import EngineRuntime
from .engine.types import ExecutionOptions
from .exceptions import TaskLiteError
from .models.job import Job, WORKER_RESOURCE
from .models.task import Task, TaskRegistry
from .selection import Exclude
from .utils.lockfile import release_lock, try_acquire_lock

logger = logging.getLogger("tasklite")

_BACKEND_SQLITE = "sqlite"
_BACKEND_MEMORY = "memory"

# 内部 worker 资源：每个 job 默认占用 1 个 worker 槽位。经
# CapacityResource 实现，复用资源调度逻辑控制并发度；常量本体在
# models/job.py（全仓库唯一定义点）。
_DEFAULT_MAX_WORKERS = 4

_UNSET: Any = object()


class TaskLite:
    """管线编排门面（Task/Job/Attempt 三层模型，标准调用契约）。

    Args:
        backend: "sqlite"（ACID 保证）或 "memory"（内存状态）。
        state_path: SQLite 数据库文件路径（backend="sqlite" 时必填，memory 默认 None）。
        max_workers: 最大并发子进程数（默认 4）。
        output_roots: 产物根目录（单根或多根序列）；设置后沙盒校验在根内，构造不创建目录。
    """

    output_roots: Path | list[Path] | None = None

    def __init__(
        self,
        *,
        backend: Literal["sqlite", "memory"],
        state_path: str | Path | None = None,
        max_workers: int = _DEFAULT_MAX_WORKERS,
        output_roots: str | Path | Sequence[str | Path] | None = None,
    ) -> None:
        if backend not in (_BACKEND_SQLITE, _BACKEND_MEMORY):
            if isinstance(backend, str):
                raise TaskLiteError(
                    f"Unknown backend: {backend!r}. Supported backends are 'sqlite' and 'memory'."
                )
            raise TaskLiteError(
                f"backend must be 'sqlite' or 'memory', got {type(backend).__name__}"
            )

        if backend == _BACKEND_SQLITE:
            if state_path is None or (isinstance(state_path, str) and not state_path.strip()):
                raise TaskLiteError("state_path is required when backend is 'sqlite'")
            self.state_path: Path | None = Path(state_path).resolve()
        else:
            self.state_path = None

        if (
            not isinstance(max_workers, int)
            or isinstance(max_workers, bool)
            or max_workers < 1
        ):
            raise TaskLiteError(
                f"max_workers must be an int >= 1, got {max_workers!r}"
            )

        self._closed: bool = False
        self.base_dir: Path = Path(os.path.abspath(os.getcwd()))
        self.output_roots = self._resolve_output_roots(output_roots)
        self.backend_type: str = backend
        self._backend_handle: AbstractStateBackend | None = None

        try:
            self._backend_handle = self._build_backend(backend)
            self.tasks = TaskRegistry()
            from .triggers import BUILTIN_TRIGGER_FUNCTIONS
            self._triggers = TriggerRegistry(
                tasks=self.tasks,
                functions=BUILTIN_TRIGGER_FUNCTIONS,
                base_dir=self.base_dir,
            )
            self.classifier = ErrorClassifier()
            self.resources = self._build_resources(self.tasks, max_workers)
            self._process_context = mp.get_context("spawn")
            self._channel = ExecutionChannel(
                process_context=self._process_context,
                output_roots=self.output_roots,
                protected_roots=((self.state_path,) if self.state_path is not None else ()),
            )
            self.runtime_config = self._build_run_config(self._backend_handle)
            self._runtime = EngineRuntime(config=self.runtime_config)
            self._console = OpsConsole(self._runtime.store)
        except BaseException:
            self._cleanup_failed_init()
            raise

    def _cleanup_failed_init(self) -> None:
        """初始化失败时释放本次创建的资源，不删除原有数据库文件。"""
        self._closed = True
        if hasattr(self, "_channel") and self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                pass
        if hasattr(self, "_runtime") and self._runtime is not None:
            try:
                self._runtime.close()
            except Exception:
                pass
        if self._backend_handle is not None:
            try:
                self._backend_handle.close()
            except Exception:
                pass

    def __enter__(self) -> TaskLite:
        if self._closed:
            raise TaskLiteError("Cannot enter context of a closed TaskLite instance.")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def close(self) -> None:
        """释放资源且运行期拒绝调用，重复关闭无操作。"""
        if self.is_running:
            raise TaskLiteError("Cannot close TaskLite while run() is in progress.")
        if self._closed:
            return
        self._closed = True
        if hasattr(self, "_channel") and self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                pass
        if hasattr(self, "_runtime") and self._runtime is not None:
            try:
                self._runtime.close()
            except Exception:
                pass
        if self._backend_handle is not None:
            try:
                self._backend_handle.close()
            except Exception:
                pass

    # ── 构造期装配分区 ────────────────────────────────────────────

    @staticmethod
    def _resolve_output_roots(
        output_roots: str | Path | Sequence[str | Path] | None,
    ) -> Path | list[Path] | None:
        """产物根装配：单根/多根（多根时声明路径属任一根即过沙盒），不创建目录。"""
        if output_roots is None:
            return None
        if isinstance(output_roots, (list, tuple)):
            return [Path(r).resolve() for r in output_roots]
        if isinstance(output_roots, (str, Path)):
            return Path(output_roots).resolve()
        raise TaskLiteError(
            f"output_roots must be str/Path or a sequence thereof, "
            f"got {type(output_roots).__name__}"
        )

    def _build_backend(self, backend: str) -> AbstractStateBackend:
        """持久化后端装配。"""
        if backend == _BACKEND_SQLITE:
            assert self.state_path is not None
            return SQLiteStateBackend(
                self.state_path,
                schema_version=TARGET_SCHEMA_VERSION,
            )
        return InMemoryStateBackend(schema_version=TARGET_SCHEMA_VERSION)

    @staticmethod
    def _build_resources(tasks: TaskRegistry, max_workers: int) -> ResourceManager:
        """资源管理器装配：注入 Task 默认资源读口与 worker 容量资源。"""
        resources = ResourceManager(tasks=tasks)
        resources[WORKER_RESOURCE] = CapacityResource(
            WORKER_RESOURCE, max_capacity=float(max_workers)
        )
        return resources

    def _build_run_config(
        self,
        backend: AbstractStateBackend,
    ) -> RunConfig:
        """RunConfig 静态装配快照。"""
        tuning = resolve_tuning()
        return RunConfig.resolve(
            name="tasklite",
            ipc_dir=None,
            protected_roots=((self.state_path,) if self.state_path is not None else ()),
            backend=backend,
            resources=self.resources,
            tasks=self.tasks,
            channel=self._channel,
            classifier=self.classifier,
            governor=DeadlockGovernor(
                deadlock_gap_max_rounds=tuning.deadlock_gap_max_rounds,
            ),
            trigger_registry=self._triggers,
            output_roots=self.output_roots,
        )

    # ── 运行期只读接缝 ─────────────────────────────────────────────

    @property
    def is_running(self) -> bool:
        """当前管线是否正在执行中。"""
        return self._runtime.is_running

    def _snapshot_backend(self) -> AbstractStateBackend:
        """向只读快照提供受控实例桥接，不开放为门面属性。"""
        backend = self._backend_handle
        if backend is None:
            raise TaskLiteError("TaskLite backend is unavailable")
        return backend

    def _ensure_active(self, api_name: str) -> None:
        """管理/入队/注册 API 的状态守卫（关闭或运行期间拒绝）。"""
        if self._closed:
            raise TaskLiteError(
                f"Cannot call {api_name}() on a closed TaskLite instance."
            )
        if self.is_running:
            raise TaskLiteError(
                f"{api_name}() is only allowed outside run(); "
                f"current run is in progress."
            )

    @contextlib.contextmanager
    def _management_lock(self):
        """Run 外写操作库级互斥保护。"""
        if self.state_path is not None:
            fd = try_acquire_lock(self.state_path, timeout=0)
            if fd is None:
                raise TaskLiteError(
                    f"Another run() or management operation is in progress for "
                    f"{self.state_path}; concurrent operations on the same state are forbidden."
                )
            try:
                yield
            finally:
                release_lock(fd)
        else:
            backend = self._snapshot_backend()
            if not hasattr(backend, "_exec_lock"):
                backend._exec_lock = threading.Lock()
            if not backend._exec_lock.acquire(blocking=False):
                raise TaskLiteError(
                    "Another run() or management operation is in progress; "
                    "concurrent operations on the same state are forbidden."
                )
            try:
                yield
            finally:
                if backend._exec_lock.locked():
                    backend._exec_lock.release()

    # ── 注册面（六步之二/三）───────────────────────────────────────

    def register_resource(self, resource: Resource) -> None:
        """注册一个资源调度器。

        同名资源重复注册明确拒绝，内部 worker 容量不能通过资源注册覆盖，
        最大并发只通过 max_workers 配置。
        """
        self._ensure_active("register_resource")
        if not isinstance(resource, Resource):
            raise TaskLiteError(
                f"register_resource expects a Resource instance, "
                f"got {type(resource).__name__}"
            )
        if resource.name == WORKER_RESOURCE:
            raise TaskLiteError(
                f"Cannot register internal worker resource {WORKER_RESOURCE!r}; "
                f"worker concurrency must be configured via max_workers."
            )
        if resource.name in self.resources:
            raise TaskLiteError(f"Resource {resource.name!r} already registered")
        self.resources[resource.name] = resource

    def register_task(
        self,
        task_or_type: "Task | str",
        handler: Callable[[Job, Any], Any] | None = _UNSET,
        *,
        resources: dict[str, float] | None = _UNSET,
        max_retries_per_run: int = _UNSET,
        timeout_seconds: int | float = _UNSET,
        trigger: str = _UNSET,
    ) -> None:
        """注册一个 Task 规格（Task 实例）或按简式参数就地构造规格。"""
        self._ensure_active("register_task")
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
            if handler is _UNSET or handler is None:
                raise TaskLiteError(
                    "handler is required when registering by task_type "
                    f"(got task_type={task_type!r}, handler=None)"
                )
            assert handler is not None and handler is not _UNSET
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
        """规格式注册的旁置参数校验（拒绝额外参数）。"""
        if (
            handler is not _UNSET
            or resources is not _UNSET
            or max_retries_per_run is not _UNSET
            or timeout_seconds is not _UNSET
            or trigger is not _UNSET
        ):
            raise TaskLiteError(
                "register_task(Task, ...) takes the spec as-is; extra spec "
                "arguments alongside a Task instance are rejected (they "
                "would be silently ignored). Pass overrides inside the Task."
            )

    def register_trigger(self, name: str, fn: Callable[["TriggerContext"], bool]) -> None:
        """注册主进程触发条件；内置名称与重复名称不允许覆盖。"""
        self._ensure_active("register_trigger")
        self._triggers.register(name, fn)

    # ── 入队（六步之四）──────────────────────────────────────────

    def enqueue(self, job: Job) -> bool:
        """入队单个作业，捕获独立快照并校验同 UID 幂等/冲突。

        首次加入队列返回 True；同 UID 已排队且有效配置相同返回 False，
        不同配置抛出 TaskLiteError。
        """
        self._ensure_active("enqueue")
        if not isinstance(job, Job):
            raise TaskLiteError(
                f"enqueue() expects a Job, got {type(job).__name__}"
            )
        with self._management_lock():
            inserted = self._runtime.store.enqueue_job(job)
            if inserted:
                logger.info("Enqueued 1 job(s).")
            else:
                logger.info("Enqueued 0 job(s), skipped 1 duplicate(s).")
            return inserted

    # ── 管理面（run() 外运维委托）──────────────────────────────────

    def set_job_status(
        self,
        job_or_uid: Job | str,
        status: Literal["pending", "succeeded", "failed"],
        *,
        recursive: bool,
        note: str | None = None,
    ) -> None:
        """人工设置作业状态（pending/succeeded/failed），限 Run 外。"""
        self._ensure_active("set_job_status")
        with self._management_lock():
            self._runtime.store.set_job_status(
                job_or_uid, status, recursive=recursive, note=note
            )

    def clear_history(
        self,
        uid: str,
        *,
        records: Collection[Literal["successes", "failures", "attempts"]],
    ) -> int:
        """从指定档案和/或 Attempt 历史中删除单个 job 的记录，限 Run 外。"""
        self._ensure_active("clear_history")
        with self._management_lock():
            return self._console.clear_history(uid, records=records)

    def get_cursor(self, key: str) -> str | None:
        """读取指定游标键的最新已提交值，限 Run 外。"""
        self._ensure_active("get_cursor")
        return self._runtime.store.get_cursor(key)

    def set_cursor(self, key: str, value: str | None) -> None:
        """设置或删除游标键值，写入直接持久化提交，限 Run 外。"""
        self._ensure_active("set_cursor")
        with self._management_lock():
            self._runtime.store.set_cursor(key, value)

    @property
    def authorizer(self) -> Callable[[str, str], bool | None] | None:
        """内部文件操作授权判定回调 (path, target_type) -> bool | None。"""
        self._ensure_active("authorizer")
        return self._runtime.store.authorizer

    @authorizer.setter
    def authorizer(self, value: Callable[[str, str], bool | None] | None) -> None:
        self._ensure_active("authorizer")
        self._runtime.store.authorizer = value

    def list_authorizations(self) -> list[dict[str, Any]]:
        """只读查询全部已持久化的授权记录，限 Run 外。"""
        self._ensure_active("list_authorizations")
        return self._runtime.store.list_authorizations()

    def revoke_authorization(self, path: str | Path) -> bool:
        """按规范绝对路径精确撤销授权，限 Run 外。"""
        self._ensure_active("revoke_authorization")
        with self._management_lock():
            return self._runtime.store.revoke_authorization(path)

    # ── 运行与停机（六步之五/六）───────────────────────────────────

    def stop(self) -> None:
        """请求管线停止当前 Run。

        只发出停止当前 Run 新派发的请求，由 run() 等待在途结算，
        重复调用幂等，无活跃 Run 时无操作，不影响下一次 Run。
        """
        if self._closed:
            raise TaskLiteError("Cannot call stop() on a closed TaskLite instance.")
        if not self.is_running:
            return
        self._runtime.request_stop()

    def run(
        self,
        *,
        exclude: Exclude | None = None,
        install_signals: bool = True,
    ) -> None:
        """运行管线直到队列排空（或收到排空请求）。"""
        self._ensure_active("run")
        if exclude is not None and not isinstance(exclude, Exclude):
            raise TaskLiteError(
                f"exclude must be an Exclude instance or None, got {type(exclude).__name__}"
            )
        logger.info(f"=== Starting Pipeline (Backend: {self.backend_type}) ===")
        self._runtime.execute(
            ExecutionOptions(
                install_signals=install_signals,
                exclude=exclude,
            )
        )


__all__ = [
    "TaskLite",
]
