"""v2 JobContext：handler 执行上下文（属于一次 job 执行）。

提供 spawn、产物/输入声明、游标与资源挂起 API。快照语义：每个 job
派发时构造一次，successes/failed 键集合为该时刻快照——同一 run 内后派发
的 job 能看到先前完成的任务，不反映构造之后其他并发 in-flight job
的完成/失败。
"""
from __future__ import annotations

import copy
import logging
import math
import os
import stat
from pathlib import Path

from ..utils.artifact import ArtifactPathPolicy
from ..utils.ipc import ArtifactJournal
from ..utils.jsonutil import dumps
from .artifact import ArtifactDeclaration, append_artifact_declaration
from .job import Job

logger = logging.getLogger("tasklite")


class JobContext:
    """传给 handler 的执行上下文：spawn / 产物声明 / 游标 / 资源挂起。"""

    def __init__(
        self,
        job: Job,
        success_job_uids: set | frozenset,
        failed_job_uids: set | frozenset,
        cursors: dict[str, str],
        output_roots: Path | None = None,
        ipc_dir: str | None = None,
        transient_registry: tuple[type[BaseException], ...] = (),
        fatal_exceptions: tuple[type[BaseException], ...] | None = None,
        transient_exceptions: tuple[type[BaseException], ...] | None = None,
        resource_names: frozenset | set | None = None,
        artifact_descriptions: dict[str, dict[str, dict[str, str]]] | None = None,
        protected_roots: tuple[str | Path, ...] = (),
    ):
        self._artifact_descriptions = copy.deepcopy(artifact_descriptions or {})
        self._protected_roots = tuple(protected_roots)
        self._local_artifacts: tuple[ArtifactDeclaration, ...] = ()
        self.job = job
        self.spawned_jobs: list[Job] = []
        self.resource_suspensions: list[tuple[str, float]] = []
        self._success_job_uids = success_job_uids
        self._failed_job_uids = failed_job_uids
        self._cursors = cursors
        self.cursor_updates: dict[str, str | None] = {}
        # 已注册资源名快照——suspend_resource 据此 fail-loud。
        # 不变式：恒为冻结集合；空集意味着无已注册资源，任何资源名的
        # suspend 请求一律入口拒绝——未注册名永不静默放行。
        self._resource_names: frozenset = frozenset(resource_names or ())
        # output_roots 支持多根（跨盘输出场景）——解析时
        # 路径属于**任一**根即通过沙盒校验。
        self.output_roots = output_roots
        # 注册表快照契约：瞬态异常注册表快照随 ctx pickle 下发子进程——
        # spawn 子进程不继承父进程函数作用域的注册，分类决策在子进程
        # 发生，快照在此携带、worker 入口重放。
        self.transient_registry = tuple(transient_registry)
        # 启发式异常元组快照契约：与 transient_registry 同构随 ctx 下发。
        # 不变式：必须保真 None 与空元组的区别——None 表示未声明（worker
        # 回退内置默认启发式），空元组表示「整体替换为空」（关闭该侧启发
        # 式）；None 被误存为空元组会让子进程静默关闭全部默认启发式。
        self.fatal_exceptions = (
            tuple(fatal_exceptions) if fatal_exceptions is not None else None
        )
        self.transient_exceptions = (
            tuple(transient_exceptions) if transient_exceptions is not None else None
        )
        # suspend 信号走落盘文件（{ipc_dir}/{uid}.signals.jsonl）——
        # 进程被 kill 后文件仍在，信号不丢。
        self.ipc_dir = ipc_dir
        # fencing 的执行代标识（{run_id}.{dispatch_no}）不经 JobContext 携带——
        # 归 WorkerLaunchSpec（进程 seam 具名契约），worker 据它构造带
        # execution_id 的结果文件名，孤儿进程的旧 execution_id 文件不被
        # 新 run 看见。
        self._journal_instance: ArtifactJournal | None = None
        self._journal_instance_dir: str | None = None

    @property
    def _journal(self) -> ArtifactJournal | None:
        """当前 ipc_dir 对应的产物清单深模块实例（按目录缓存单例）。"""
        if self.ipc_dir is None:
            return None
        if self._journal_instance is None or self._journal_instance_dir != self.ipc_dir:
            self._journal_instance = ArtifactJournal(self.ipc_dir, self.output_roots, self._protected_roots)
            self._journal_instance_dir = self.ipc_dir
        return self._journal_instance

    def spawn(self, job: Job) -> None:
        """入队一个子 job。

        在子进程内立即预检 JSON 可序列化性（与 enqueue 的序列化预检对齐）。
        坏 payload 在此抛 ValueError → handler 异常 → 父作业进失败档案，
        而不是在 commit 阶段才崩掉整个 run。
        """
        try:
            # allow_nan=False——默认 allow_nan=True 会让
            # float('inf')/float('nan') 通过预检，产出非标准 JSON "Infinity"。
            dumps(job.to_dict())
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Payload for spawned job {job.uid} is not JSON-serializable: {e}"
            ) from e
        self.spawned_jobs.append(job)

    def declare_output(
        self, path: str | Path, *, name: str | None = None,
        delete_existing: bool = False, delete_on_failure: bool = False,
        delete_on_success: bool = False, sandbox: bool = True,
    ) -> str:
        """声明文件或目录并返回规范绝对路径；三项删除授权相互独立。

        声明必须早于首次写入。完全一致的重复声明幂等，不会再次删除；
        调用方负责跨 Job 的路径独占，框架不维护全局所有权。
        """
        policy = ArtifactPathPolicy(
            output_roots=self.output_roots,
            protected_roots=(*self._protected_roots, *((self.ipc_dir,) if self.ipc_dir else ())),
        )
        resolved = policy.resolve(path, sandbox=sandbox)
        declaration = ArtifactDeclaration(
            resolved, name=name, delete_existing=delete_existing,
            delete_on_failure=delete_on_failure, delete_on_success=delete_on_success,
            sandbox=sandbox,
        )
        journal = self._journal
        if journal is not None:
            journal.record_artifact(self.job.uid, declaration, prepare=True)
        else:
            declarations = append_artifact_declaration(self._local_artifacts, declaration)
            if len(declarations) > len(self._local_artifacts):
                policy.remove_existing(declaration)
                Path(resolved).parent.mkdir(parents=True, exist_ok=True)
                self._local_artifacts = declarations
        return resolved

    def declare_cache(
        self, path: str | Path, *, delete_existing: bool = False, sandbox: bool = True,
    ) -> str:
        """声明成败均清理的匿名临时磁盘产物；已有路径仍须前删授权。"""
        return self.declare_output(
            path, delete_existing=delete_existing, delete_on_failure=True,
            delete_on_success=True, sandbox=sandbox,
        )

    def declare_input(self, path: str | Path) -> str:
        """声明一个本地输入文件或目录：校验路径存在、记录规范绝对路径并返回该路径字符串。

        本方法无 ``sandbox`` 参数——框架不写或清理输入文件，相对路径按当前工作目录解析。
        在声明点立即执行 ``os.stat(resolved)`` 校验路径存在（缺失或断链立即抛 ``FileNotFoundError`` / ``OSError``）；
        成功提交时框架把去重保序的规范路径字符串列表 ``list[str]`` 写入 ``successes.metadata["inputs"]``。
        """
        raw = str(path)
        resolved = ArtifactJournal.resolve_and_validate_path(raw, self.output_roots, sandbox=False)
        os.stat(resolved)
        if self._journal is not None:
            self._journal.record_input_file(self.job.uid, resolved)
        return resolved

    def get_artifact_path(self, *, producer_job_uid: str, artifact_name: str) -> str:
        """查询直接依赖的已成功产物；不自动声明输入或触发生产者。"""
        for label, value in (("producer_job_uid", producer_job_uid), ("artifact_name", artifact_name)):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{label} must be a non-empty str")
        reference = f"producer {producer_job_uid!r}, artifact {artifact_name!r}"
        if producer_job_uid not in self.job.depends_on:
            raise ValueError(f"{reference}: producer must be a direct dependency")
        if producer_job_uid not in self._success_job_uids:
            raise LookupError(f"{reference}: no successful producer in dispatch snapshot")
        descriptions = self._artifact_descriptions.get(producer_job_uid, {})
        if artifact_name not in descriptions:
            raise KeyError(f"{reference}: artifact name was not published")
        description = descriptions[artifact_name]
        if not isinstance(description, dict) or set(description) != {"path"}:
            raise ValueError(f"{reference}: invalid artifact description")
        path = description["path"]
        try:
            ArtifactDeclaration(path, name=artifact_name)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{reference}: invalid artifact path {path!r}") from error
        try:
            info = os.stat(path)
        except OSError as error:
            raise OSError(error.errno, f"{reference}: {error.strerror}", path) from error
        if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
            raise ValueError(f"{reference}: artifact is not a file or directory: {path}")
        if Path(path).is_symlink() or str(Path(path).resolve()) != path:
            raise ValueError(f"{reference}: artifact path changed: {path}")
        return path

    def has_succeeded(self, job_uid: str) -> bool:
        """查询 job_uid 是否命中派发时的成功档案快照。"""
        return job_uid in self._success_job_uids

    def has_failed(self, job_uid: str) -> bool:
        """查询 job_uid 是否命中派发时的失败档案快照。"""
        return job_uid in self._failed_job_uids

    def succeeded_or_failed_job_uids(self) -> frozenset:
        """返回派发时成功与失败档案 UID 的只读并集，不包含 skipped 或执行轨迹。"""
        return frozenset(self._success_job_uids) | frozenset(self._failed_job_uids)

    def get_cursor(self, key: str) -> str | None:
        """读取高水位游标的当前值。"""
        return self._cursors.get(key)

    def set_cursor(self, key: str, value: str | None) -> None:
        """更新游标，job 成功时原子提交。

        key 与 value 都必须是 str；``value=None`` 表示**删除**该游标
        （与后端 None-删除语义对齐）。
        """
        if not isinstance(key, str):
            raise TypeError(f"cursor key must be str, got {type(key).__name__}")
        if value is not None and not isinstance(value, str):
            raise TypeError(f"cursor value must be str, got {type(value).__name__}")
        self.cursor_updates[key] = value
        if value is None:
            self._cursors.pop(key, None)
        else:
            self._cursors[key] = value

    def suspend_resource(self, resource_name: str, seconds: float) -> None:
        """请求全局挂起一个资源（如 429 限速）。

        API 边界即校验（与 set_cursor 的严格校验对称）——非数值/非有限/
        非正值在入口拒绝，而非延迟到挂起清洗层静默忽略，避免调用方
        误以为 suspend 已生效。

        信号立即追加到 ``{ipc_dir}/{uid}.signals.jsonl``（落盘 flush），
        即使 handler 随后崩溃/超时，限流信息也不丢失。同时保留
        resource_suspensions list 供正常完成路径的结果文件携带（去重
        由挂起的 max 语义保证）。
        """
        if not isinstance(resource_name, str) or not resource_name:
            raise TypeError(f"resource_name must be a non-empty str, got {resource_name!r}")
        # 未注册资源名 fail-loud——typo（如 "api_v_two" vs "api"）会静默
        # 追加，消费端按 `in resources` 静默跳过 → 用户以为限流已生效、
        # 管线继续猛打目标 API。生产派发路径携带注册集快照，入口即拒绝。
        if resource_name not in self._resource_names:
            raise ValueError(
                f"Unknown resource {resource_name!r} (registered: "
                f"{sorted(self._resource_names)}); suspend request ignored"
            )
        if not isinstance(seconds, (int, float)) or isinstance(seconds, bool):
            raise TypeError(f"seconds must be a number, got {type(seconds).__name__} ({seconds!r})")
        # 超大 int（如 10**400）使 isfinite 转 float 溢出抛 OverflowError
        # （ArithmeticError 子类，会命中 fatal 启发式）——与非法值同路收敛
        # 为 ValueError 明确报错，而非异常类型漂移
        try:
            finite = math.isfinite(seconds)
        except OverflowError:
            finite = False
        if not finite or seconds <= 0:
            raise ValueError(f"seconds must be finite and > 0, got {seconds!r}")
        self.resource_suspensions.append((resource_name, seconds))
        if self._journal is not None:
            self._journal.record_signal(self.job.uid, resource_name, seconds)
        logger.info(f"Job requested global suspension of resource '{resource_name}' for {seconds}s.")


__all__ = [
    "JobContext",
]
