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
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..exceptions import TaskLiteError, _ArtifactIOError
from ..utils.artifact import ArtifactPathPolicy
from ..utils.ipc import (
    ArtifactJournal,
    WorkerPipeSender,
    WorkerReplyReceiver,
)
from ..utils.jsonutil import dumps
from .artifact import (
    ArtifactDeclaration,
    FileSpec,
    coerce_file_spec,
    resolve_job_files_config,
    validate_file_name,
)
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
        output_roots: str | Path | Sequence[str | Path] | None = None,
        ipc_dir: str | None = None,
        resource_names: frozenset | set | None = None,
        artifact_descriptions: dict[str, dict[str, dict[str, str]]] | None = None,
        protected_roots: tuple[str | Path, ...] = (),
        base_dir: str | Path | None = None,
        known_job_uids: frozenset[str] | set[str] | None = None,
        files: Mapping[str, Any] | None = None,
    ):
        self._artifact_descriptions = copy.deepcopy(artifact_descriptions or {})
        self._protected_roots = tuple(protected_roots)
        self.base_dir: Path = (
            Path(os.path.abspath(os.fspath(base_dir)))
            if base_dir is not None
            else Path(os.path.abspath(os.getcwd()))
        )
        self._pipe_sender: WorkerPipeSender | None = None
        self._reply_receiver: WorkerReplyReceiver | None = None
        self.job = job
        self.spawned_jobs: list[Job] = []
        self.resource_suspensions: list[tuple[str, float]] = []
        self._files: dict[str, FileSpec] = {}
        init_files = files if files is not None else getattr(job, "files", None)
        if init_files:
            for raw_k, raw_v in init_files.items():
                k = validate_file_name(raw_k)
                spec = coerce_file_spec(raw_v, allow_dict=True)
                if not spec.is_output_ref and spec.path is not None and not os.path.isabs(str(spec.path)):
                    spec = spec.resolve_against(self.base_dir)
                self._files[k] = spec
        self._success_job_uids = success_job_uids
        self._failed_job_uids = failed_job_uids
        self._known_job_uids: frozenset[str] = (
            frozenset(known_job_uids)
            if known_job_uids is not None
            else (frozenset(success_job_uids) | frozenset(failed_job_uids))
        )
        self._cursors = cursors
        self.cursor_updates: dict[str, str | None] = {}
        # 已注册资源名快照——suspend_resource 据此 fail-loud。
        # 不变式：恒为冻结集合；空集意味着无已注册资源，任何资源名的
        # suspend 请求一律入口拒绝——未注册名永不静默放行。
        self._resource_names: frozenset = frozenset(resource_names or ())
        # output_roots 支持多根（跨盘输出场景）——解析时
        # 路径属于**任一**根即通过沙盒校验。
        self.output_roots = output_roots
        self.ipc_dir = ipc_dir
        self._journal_instance: ArtifactJournal | None = None
        self._journal_instance_dir: str | None = None

    @property
    def _journal(self) -> ArtifactJournal:
        """当前上下文对应的产物清单深模块实例。"""
        if self._journal_instance is None or self._journal_instance_dir != self.ipc_dir:
            self._journal_instance = ArtifactJournal(
                self.ipc_dir,
                self.output_roots,
                self._protected_roots,
                pipe_sender=self._pipe_sender,
            )
            self._journal_instance_dir = self.ipc_dir
        return self._journal_instance

    def _bind_pipe_sender(
        self,
        sender: WorkerPipeSender | None,
        reply_receiver: WorkerReplyReceiver | None = None,
    ) -> None:
        """绑定或解绑当前执行体的瞬时管道发送端与回复接收端。"""
        self._pipe_sender = sender
        self._reply_receiver = reply_receiver
        if self._journal_instance is not None:
            self._journal_instance.bind_pipe_sender(sender)

    def file_path(self, name: str) -> Path:
        """返回当前 Job 已声明文件（预声明或已完成动态声明）的规范绝对 Path。"""
        validated_name = validate_file_name(name)
        if validated_name not in self._files:
            raise KeyError(f"No file declared with name {validated_name!r}")
        spec = self._files[validated_name]
        if spec.path is None:
            raise TaskLiteError(f"FileSpec {validated_name!r} has no resolved path")
        return Path(spec.path)

    def declare_file(self, name: str, spec: FileSpec) -> Path:
        """动态登记文件声明，等待主进程完成必要授权与准备操作后返回规范绝对 Path。"""
        validated_name = validate_file_name(name)
        if not isinstance(spec, FileSpec):
            raise TypeError(f"declare_file expects a FileSpec, got {type(spec).__name__}")

        canonical_spec = spec if spec.is_output_ref else spec.resolve_against(self.base_dir)
        if validated_name in self._files:
            existing = self._files[validated_name]
            if existing.is_output_ref and spec.is_output_ref:
                if (
                    existing.producer_uid == spec.producer_uid
                    and existing.output_name == spec.output_name
                    and existing.role == spec.role
                    and existing.delete_existing == spec.delete_existing
                    and existing.delete_on_failure == spec.delete_on_failure
                    and existing.delete_on_success == spec.delete_on_success
                ):
                    assert existing.path is not None
                    return Path(existing.path)
            elif not existing.is_output_ref and not spec.is_output_ref:
                if (
                    existing.path == canonical_spec.path
                    and existing.role == canonical_spec.role
                    and existing.delete_existing == canonical_spec.delete_existing
                    and existing.delete_on_failure == canonical_spec.delete_on_failure
                    and existing.delete_on_success == canonical_spec.delete_on_success
                ):
                    assert existing.path is not None
                    return Path(existing.path)
            raise TaskLiteError(
                f"File declaration {validated_name!r} conflicts with existing declaration: "
                f"{existing!r} vs {spec!r}"
            )

        if spec.is_output_ref:
            assert spec.producer_uid is not None
            if spec.producer_uid not in self.job.depends_on:
                raise TaskLiteError(
                    f"FileSpec {validated_name!r} references producer {spec.producer_uid!r} "
                    f"which is not in explicit direct depends_on {list(self.job.depends_on)!r}"
                )

        if self._pipe_sender is not None and self._reply_receiver is not None:
            req_id = f"req-{uuid.uuid4().hex[:8]}"
            self._pipe_sender.send_declare(req_id, validated_name, spec.to_dict())
            reply = self._reply_receiver.wait_reply(req_id)
            status = reply.get("status")
            if status == "ok":
                resolved_path = str(reply["resolved_path"])
                bound_spec = (
                    spec.bind_output_path(resolved_path, base_dir=self.base_dir)
                    if spec.is_output_ref
                    else spec.resolve_against(self.base_dir)
                )
                self._files[validated_name] = bound_spec
                return Path(resolved_path)
            elif status == "error":
                raise TaskLiteError(reply.get("error", "Declaration failed"))
            elif status == "denied":
                raise TaskLiteError("authorization denied")
            else:
                raise TaskLiteError(f"Unknown reply status: {status!r}")

        # 本地 / 无管道回退（用于直接单测与调试）
        if spec.is_output_ref:
            assert spec.producer_uid is not None and spec.output_name is not None
            desc = self._artifact_descriptions.get(spec.producer_uid, {}).get(spec.output_name)
            if not desc or "path" not in desc:
                raise TaskLiteError(f"producer {spec.producer_uid!r} did not publish output {spec.output_name!r}")
            resolved_path = desc["path"]
            if spec.role == "input" and not os.path.exists(resolved_path):
                raise FileNotFoundError(f"Missing input file: {resolved_path}")
            bound_spec = spec.bind_output_path(resolved_path, base_dir=self.base_dir)
        else:
            bound_spec = spec.resolve_against(self.base_dir)
            assert bound_spec.path is not None
            resolved_path = bound_spec.path
            if spec.role == "input" and not os.path.exists(resolved_path):
                raise FileNotFoundError(f"Missing input file: {resolved_path}")

        if bound_spec.delete_existing and os.path.exists(resolved_path):
            policy = ArtifactPathPolicy(output_roots=self.output_roots, protected_roots=self._protected_roots)
            policy.remove_existing(ArtifactDeclaration(resolved_path, delete_existing=True))

        self._files[validated_name] = bound_spec
        return Path(resolved_path)

    def spawn(self, job: Job) -> None:
        """入队一个子 job（捕获固定路径基准下的独立快照并预检依赖引用与 JSON 序列化性）。"""
        if not isinstance(job, Job):
            raise TypeError(
                f"spawn() expects a Job, got {type(job).__name__}"
            )
        # 依赖闭合：只能引用派发时已知的 Job、当前父 Job，以及本次先前声明的 Job
        allowed_uids = self._known_job_uids | {self.job.uid} | {j.uid for j in self.spawned_jobs}
        for dep in job.depends_on:
            if dep not in allowed_uids:
                raise ValueError(
                    f"Spawned job {job.uid!r} references non-existent, future, or cross-worker dependency {dep!r}"
                )
        job_dict = job.to_dict()
        job_dict["files"] = resolve_job_files_config(
            job_dict.get("files"),
            base_dir=self.base_dir,
            depends_on=job_dict.get("depends_on") or (),
        )
        try:
            # allow_nan=False——默认 allow_nan=True 会让
            # float('inf')/float('nan') 通过预检，产出非标准 JSON "Infinity"。
            dumps(job_dict)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Payload for spawned job {job.uid} is not JSON-serializable: {e}"
            ) from e
        self.spawned_jobs.append(Job.from_dict(job_dict))

    def declare_input(self, path: str | Path) -> str:
        """声明一个本地输入文件或目录：校验路径存在、记录规范绝对路径并返回该路径字符串。

        本方法无 ``sandbox`` 参数——框架不写或清理输入文件，相对路径按当前工作目录解析。
        在声明点立即执行 ``os.stat(resolved)`` 校验路径存在（缺失或断链立即抛 ``FileNotFoundError`` / ``OSError``）；
        成功提交时框架把去重保序的规范路径字符串列表 ``list[str]`` 写入 ``successes.metadata["inputs"]``。
        """
        raw = str(path)
        resolved = ArtifactJournal.resolve_and_validate_path(raw, self.output_roots, sandbox=False)
        os.stat(resolved)
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

    def get_cursor(self, key: str) -> str | None:
        """读取高水位游标的当前值（含自身暂存写入）。"""
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        return self._cursors.get(key)

    def set_cursor(self, key: str, value: str | None) -> None:
        """暂存游标更新，job 成功时随事务原子提交。

        key 必须为非空 str；value 必须为 str 或 None（None 表示删除游标，
        空字符串为合法值）。
        """
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        if value is not None and not isinstance(value, str):
            raise TypeError(f"cursor value must be str or None, got {type(value).__name__}")
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

        挂起信号经瞬时管道实时发送至父进程，同时保留 resource_suspensions
        供正常完成结果携带（去重由挂起的 max 语义保证）。
        """
        if not isinstance(resource_name, str) or not resource_name:
            raise TypeError(f"resource_name must be a non-empty str, got {resource_name!r}")
        if resource_name not in self._resource_names:
            raise ValueError(
                f"Unknown resource {resource_name!r} (registered: "
                f"{sorted(self._resource_names)}); suspend request ignored"
            )
        if not isinstance(seconds, (int, float)) or isinstance(seconds, bool):
            raise TypeError(f"seconds must be a number, got {type(seconds).__name__} ({seconds!r})")
        try:
            finite = math.isfinite(seconds)
        except OverflowError:
            finite = False
        if not finite or seconds <= 0:
            raise ValueError(f"seconds must be finite and > 0, got {seconds!r}")
        self.resource_suspensions.append((resource_name, seconds))
        if self._pipe_sender is not None:
            try:
                self._pipe_sender.send_suspend(resource_name, seconds)
            except OSError:
                pass
        logger.info(f"Job requested global suspension of resource '{resource_name}' for {seconds}s.")


__all__ = [
    "JobContext",
]
