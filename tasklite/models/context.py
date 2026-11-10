"""v2 JobContext：handler 执行上下文（属于一次 job 执行）。

提供 spawn、产物/输入声明与游标 API。快照语义：每个 job
派发时构造一次，successes/failed 键集合为该时刻快照——同一 run 内后派发
的 job 能看到先前完成的任务，不反映构造之后其他并发 in-flight job
的完成/失败。
"""
from __future__ import annotations

import copy
import os
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..exceptions import TaskLiteError, _ArtifactIOError
from ..utils.ipc import (
    WorkerPipeSender,
    WorkerReplyReceiver,
)
from .artifact import (
    FileSpec,
    coerce_file_spec,
    resolve_job_files_config,
    validate_file_name,
)
from .job import Job


class JobContext:
    """传给 handler 的执行上下文：spawn / 产物声明 / 游标。"""

    def __init__(
        self,
        job: Job,
        success_job_uids: set | frozenset,
        failed_job_uids: set | frozenset,
        cursors: dict[str, str],
        output_roots: str | Path | Sequence[str | Path] | None = None,
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
        self._file_sources: dict[str, FileSpec] = {}
        self._files: dict[str, FileSpec] = {}
        job_files = getattr(job, "files", {}) or {}
        resolved_files = files or {}
        init_names = dict.fromkeys((*job_files, *resolved_files))
        for raw_name in init_names:
            name = validate_file_name(raw_name)
            raw_source = job_files.get(name, resolved_files.get(name))
            source = self._canonical_source(
                coerce_file_spec(raw_source, allow_dict=True)
            )
            resolved = (
                coerce_file_spec(resolved_files[name], allow_dict=True)
                if name in resolved_files
                else self._resolve_local_file(source)
            )
            self._file_sources[name] = source
            self._files[name] = resolved
        self._success_job_uids = success_job_uids
        self._failed_job_uids = failed_job_uids
        self._known_job_uids: frozenset[str] = (
            frozenset(known_job_uids)
            if known_job_uids is not None
            else (frozenset(success_job_uids) | frozenset(failed_job_uids))
        )
        self._cursors = cursors
        self.cursor_updates: dict[str, str | None] = {}
        # output_roots 支持多根（跨盘输出场景）——解析时
        # 路径属于**任一**根即通过沙盒校验。
        self.output_roots = output_roots

    def _bind_pipe_sender(
        self,
        sender: WorkerPipeSender | None,
        reply_receiver: WorkerReplyReceiver | None = None,
    ) -> None:
        """绑定或解绑当前执行体的瞬时管道发送端与回复接收端。"""
        self._pipe_sender = sender
        self._reply_receiver = reply_receiver

    def _canonical_source(self, spec: FileSpec) -> FileSpec:
        """将普通路径声明按固定基准绝对化；上游引用保留配置身份。"""
        return spec if spec.is_output_ref else spec.resolve_against(self.base_dir)

    def _resolve_local_file(self, spec: FileSpec) -> FileSpec:
        """无回复通道时解析声明供隔离测试使用；不执行任何文件删除。"""
        if spec.is_output_ref:
            assert spec.producer_uid is not None and spec.output_name is not None
            desc = self._artifact_descriptions.get(spec.producer_uid, {}).get(spec.output_name)
            if not desc or "path" not in desc:
                raise TaskLiteError(
                    f"producer {spec.producer_uid!r} did not publish "
                    f"output {spec.output_name!r}"
                )
            resolved = spec.bind_output_path(desc["path"])
        else:
            resolved = self._canonical_source(spec)
        if resolved.path is None:
            raise TaskLiteError("FileSpec resolved without a path")
        if resolved.role == "input" and not os.path.exists(resolved.path):
            raise TaskLiteError(f"Missing input file: {resolved.path}")
        return resolved

    def file_path(self, name: str) -> Path:
        """返回当前 Job 已声明文件（预声明或已完成动态声明）的规范绝对 Path。"""
        validated_name = validate_file_name(name)
        if validated_name not in self._files:
            raise TaskLiteError(f"No file declared with name {validated_name!r}")
        spec = self._files[validated_name]
        if spec.path is None:
            raise TaskLiteError(f"FileSpec {validated_name!r} has no resolved path")
        return Path(spec.path)

    def declare_file(self, name: str, spec: FileSpec) -> Path:
        """动态登记文件声明，等待主进程完成必要授权与准备操作后返回规范绝对 Path。"""
        validated_name = validate_file_name(name)
        if not isinstance(spec, FileSpec):
            raise TaskLiteError(f"declare_file expects a FileSpec, got {type(spec).__name__}")

        if spec.is_output_ref:
            assert spec.producer_uid is not None
            if spec.producer_uid not in self.job.depends_on:
                raise TaskLiteError(
                    f"FileSpec {validated_name!r} references producer {spec.producer_uid!r} "
                    f"which is not in explicit direct depends_on {list(self.job.depends_on)!r}"
                )

        canonical_source = self._canonical_source(spec)
        if validated_name in self._file_sources:
            existing = self._file_sources[validated_name]
            if existing == canonical_source:
                resolved = self._files[validated_name]
                if resolved.path is None:
                    raise TaskLiteError(f"FileSpec {validated_name!r} has no resolved path")
                return Path(resolved.path)
            raise TaskLiteError(
                f"File declaration {validated_name!r} conflicts with existing declaration: "
                f"{existing!r} vs {canonical_source!r}"
            )

        if self._pipe_sender is not None and self._reply_receiver is not None:
            req_id = f"req-{uuid.uuid4().hex[:8]}"
            self._pipe_sender.send_declare(
                req_id, validated_name, canonical_source.to_dict()
            )
            reply = self._reply_receiver.wait_reply(req_id)
            status = reply.get("status")
            if status == "ok":
                resolved_path = str(reply["resolved_path"])
                bound_spec = canonical_source.bind_output_path(resolved_path)
                self._file_sources[validated_name] = canonical_source
                self._files[validated_name] = bound_spec
                return Path(resolved_path)
            elif status == "error":
                raise TaskLiteError(reply.get("error", "Declaration failed"))
            elif status == "denied":
                raise TaskLiteError(reply.get("error", "authorization denied"))
            else:
                raise TaskLiteError(f"Unknown reply status: {status!r}")

        bound_spec = self._resolve_local_file(canonical_source)
        assert bound_spec.path is not None
        resolved_path = bound_spec.path
        self._file_sources[validated_name] = canonical_source
        self._files[validated_name] = bound_spec
        return Path(resolved_path)

    def spawn(self, job: Job) -> None:
        """入队一个子 job（捕获固定路径基准下的独立快照并校验依赖引用与 JSON 对象契约）。"""
        if not isinstance(job, Job):
            raise TaskLiteError(
                f"spawn() expects a Job, got {type(job).__name__}"
            )
        # 依赖闭合：只能引用派发时已知的 Job、当前父 Job，以及本次先前声明的 Job
        allowed_uids = self._known_job_uids | {self.job.uid} | {j.uid for j in self.spawned_jobs}
        for dep in job.depends_on:
            if dep not in allowed_uids:
                raise TaskLiteError(
                    f"Spawned job {job.uid!r} references non-existent, future, or cross-worker dependency {dep!r}"
                )
        job_dict = job.to_dict()
        job_dict["files"] = resolve_job_files_config(
            job_dict.get("files"),
            base_dir=self.base_dir,
            depends_on=job_dict.get("depends_on") or (),
        )
        self.spawned_jobs.append(Job.from_dict(job_dict))

    def get_cursor(self, key: str) -> str | None:
        """读取高水位游标的当前值（含自身暂存写入）。"""
        if not isinstance(key, str) or not key:
            raise TaskLiteError(f"cursor key must be a non-empty str, got {key!r}")
        return self._cursors.get(key)

    def set_cursor(self, key: str, value: str | None) -> None:
        """暂存游标更新，job 成功时随事务原子提交。

        key 必须为非空 str；value 必须为 str 或 None（None 表示删除游标，
        空字符串为合法值）。
        """
        if not isinstance(key, str) or not key:
            raise TaskLiteError(f"cursor key must be a non-empty str, got {key!r}")
        if value is not None and not isinstance(value, str):
            raise TaskLiteError(f"cursor value must be str or None, got {type(value).__name__}")
        self.cursor_updates[key] = value
        if value is None:
            self._cursors.pop(key, None)
        else:
            self._cursors[key] = value

__all__ = [
    "JobContext",
]
