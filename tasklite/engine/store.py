"""v2 StateStore: 统一内存状态机与后端持久化事务的深模块。

内敛原子双写一致性、六集合全局互斥与
依赖图级联分析。单一出口纪律：

- Job 终结事务唯一经由本模块的 apply_* 出口族收敛（成功 / 失败 / 重试 /
  跳过 / 批量失败 / 级联）——backend commit + 内存镜像 + 统计 + 级联
  在同一出口内对齐，内存永不领先磁盘；
- 失败终态内存登记唯一经由 ``mark_failed_memory``（successes/failed 互斥的
  单点维护）；
- Attempt 为追加式执行轨迹，与对应结果同事务结算；事务异常原样传播。
"""

from __future__ import annotations

from contextlib import AbstractContextManager
import copy
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Sequence

from ..backend.base import AbstractStateBackend, GraphEditCommit
from ..exceptions import TaskLiteError
from ..models.artifact import FileSpec, resolve_file_spec
from ..models.job import Job, inject_worker_resource
from ..models.result import validate_strict_json_object
from ..models.state import PipelineState, job_uid_from_snapshot
from ..selection import Exclude
from ..utils.artifact import InspectedPath, PathPolicy
from ..utils.clock import parse_utc_iso_to_us, utc_now_iso
from ..utils.jsonutil import dumps
from .admission import (
    ImmediateRequeuePolicy,
    RequeuePolicy,
    TriggerRegistry,
    extract_effective_job_config,
    extract_trusted_metadata_timestamp_us,
    is_trusted_success_metadata,
)
from .execution_access import exclusive_state_access
from .session import RunSession

logger = logging.getLogger("tasklite")


@dataclass(frozen=True)
class SuccessOutcome:
    """apply_success 原子转移结果。"""
    job_uid: str
    success_metadata: dict[str, Any]
    spawned_uids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class FailureOutcome:
    """apply_failure 原子转移结果。"""
    job_uid: str
    failure_metadata: dict[str, Any]
    cascaded_uids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RetryOutcome:
    """apply_retry 原子转移结果。"""
    job_uid: str
    retry_job_snapshot: dict[str, Any]


@dataclass(frozen=True)
class SkipOutcome:
    """apply_skip 原子转移结果。"""
    job_uid: str
    was_known: bool


@dataclass(frozen=True)
class BulkFailureOutcome:
    """apply_bulk_failure 原子转移结果。"""
    failed_uids: list[str]
    cascaded_uids: list[str] = field(default_factory=list)
    remaining_queue_count: int = 0


class StateStore:
    """统一内存状态机与后端持久化事务的深模块。"""

    def __init__(
        self,
        backend: AbstractStateBackend,
        state: PipelineState | None = None,
        *,
        trigger_registry: TriggerRegistry | None = None,
        requeue_policy: RequeuePolicy | None = None,
        base_dir: str | Path | None = None,
        state_path: str | Path | None = None,
        protected_roots: Sequence[str | Path] = (),
        path_policy: PathPolicy | None = None,
        session: RunSession | None = None,
    ) -> None:
        self._backend = backend
        self._state = state if state is not None else PipelineState({}, {}, {}, [])
        self._trigger_registry = (
            trigger_registry
            if trigger_registry is not None
            else TriggerRegistry(base_dir=base_dir)
        )
        # 重试节奏唯一出口：核心不做任何节奏计算，默认立即重入队。
        self._requeue_policy = (
            requeue_policy if requeue_policy is not None else ImmediateRequeuePolicy()
        )
        self._session = session if session is not None else RunSession()
        if path_policy is not None:
            if protected_roots or state_path is not None:
                merged_roots = (*path_policy.protected_paths, *protected_roots)
                effective_base: str | Path = (
                    path_policy.fixed_base_dir
                    if path_policy.fixed_base_dir is not None
                    else self._trigger_registry.base_dir
                )
                self._path_policy = PathPolicy(
                    base_dir=effective_base,
                    state_path=state_path,
                    protected_roots=merged_roots,
                    adapter=path_policy.adapter,
                )
            else:
                self._path_policy = path_policy
        else:
            self._path_policy = PathPolicy(
                base_dir=self._trigger_registry.base_dir,
                state_path=state_path,
                protected_roots=protected_roots,
            )
        self.authorizer: Callable[[str, str], bool | None] | None = None

    @property
    def protected_roots(self) -> tuple[Path, ...]:
        """受保护路径集合（派生自统一 PathPolicy，单一事实源）。"""
        return self._path_policy.protected_paths

    @property
    def path_policy(self) -> PathPolicy:
        """统一路径规范化与受保护路径策略实例。"""
        return self._path_policy

    def resolve_file_spec(
        self,
        spec: FileSpec,
        *,
        depends_on: Sequence[str],
        enforce_safety: bool | None = None,
    ) -> FileSpec:
        """经统一 PathPolicy 单次原子解析 FileSpec（上游引用、安全边界、存在性判定与路径绑定）。"""
        def _lookup(producer_uid: str, output_name: str) -> Mapping[str, Any] | None:
            artifacts = self._state.successes.get(producer_uid, {}).get("artifacts", {})
            return artifacts.get(output_name) if isinstance(artifacts, Mapping) else None

        return resolve_file_spec(
            spec,
            path_policy=self._path_policy,
            depends_on=depends_on,
            artifact_lookup=_lookup,
            enforce_safety=enforce_safety,
        )

    @property
    def base_dir(self) -> Path:
        """当前实例固定的相对路径解析基准目录。"""
        return self._trigger_registry.base_dir

    @property
    def session(self) -> RunSession:
        """当前关联的运行期会话（单次 Run 排除规则与生命周期状态真相源）。"""
        return self._session

    def set_run_exclusions(
        self,
        exclude: Exclude | None,
        temporary_uids: set[str] | None = None,
    ) -> None:
        """内部注入面：设置本 Run 排除规则与临时排除集合。"""
        self._session.exclude = exclude
        self._session.temporary_excluded_uids = (
            set(temporary_uids) if temporary_uids is not None else set()
        )

    def temporarily_exclude(self, job_uid: str) -> None:
        """登记单个临时排除 UID。"""
        self._session.temporarily_exclude(job_uid)

    def _selection_excluded_uids(self) -> frozenset[str]:
        """当前队列中被排除拦截（Exclude 或临时排除）的合法作业 UID 集合。"""
        exclude = self._session.exclude
        temporary_uids = self._session.temporary_excluded_uids
        if exclude is None and not temporary_uids:
            return frozenset()
        excluded: set[str] = set(temporary_uids)
        if exclude is not None:
            for job_snapshot in self._state.queue:
                try:
                    job = Job.from_dict(job_snapshot)
                except (KeyError, TypeError, ValueError, TaskLiteError):
                    continue
                if exclude.matches_task_type(job.task_type):
                    excluded.add(job.uid)
        return frozenset(excluded)


    @staticmethod
    def _attempt_error(metadata: Mapping[str, Any]) -> str | None:
        """从失败 meta 提取轨迹行错误串（空串收敛为 None）。"""
        error_message = str(metadata.get("error_message") or "")
        return error_message or None

    # ── 游标（get_cursor / set_cursor）─────────────────────────────

    def get_cursor(self, key: str) -> str | None:
        """查询游标最新已提交值。"""
        if not isinstance(key, str) or not key:
            raise TaskLiteError(f"cursor key must be a non-empty str, got {key!r}")
        return self._backend.get_cursor(key)

    def set_cursor(self, key: str, value: str | None) -> None:
        """设置或删除单个游标键值（直接提交写后端并更新内存镜像）。"""
        if not isinstance(key, str) or not key:
            raise TaskLiteError(f"cursor key must be a non-empty str, got {key!r}")
        if value is not None and not isinstance(value, str):
            raise TaskLiteError(f"cursor value must be str or None, got {type(value).__name__}")
        self._backend.set_cursor(key, value)
        self._state.set_cursor(key, value)

    def exclusive_access(self) -> AbstractContextManager[None]:
        """获取底层状态库的独占执行权上下文（供 EditTransaction 等受控协作方调用）。"""
        return exclusive_state_access(self._backend)

    def load_consistent_snapshot(self) -> dict[str, Any]:
        """在单一读事务或单锁内原子捕获全部核心表一致快照。"""
        return self._backend.load_consistent_snapshot()

    def apply_graph_edit(self, commit: GraphEditCommit) -> None:
        """原子提交图修整事务并在落盘成功后同步内存状态镜像。"""
        if (
            not commit.queue_writes
            and not commit.success_writes
            and not commit.failed_writes
            and not commit.recovery_attempts
        ):
            return
        self._backend.commit_graph_edit(commit)
        self._state = PipelineState(
            self._backend.load_successes(),
            self._backend.load_failed(),
            self._backend.load_cursors(),
            self._backend.load_queue(),
        )

    # ── 摄入管道（enqueue / spawn）──────────────────────────────────

    def _require_registered_task(self, task_type: str) -> None:
        """新声明必须对应本实例已注册的 Task。"""
        tasks = self._trigger_registry.tasks
        if tasks is None or task_type not in tasks:
            raise TaskLiteError(
                f"unknown task_type {task_type!r} (register the task before "
                f"submitting its jobs)"
            )

    def normalize_and_validate_job(self, job: Job | dict[str, Any]) -> dict[str, Any]:
        """捕获完整作业的独立快照并落实摄入规则。"""
        if isinstance(job, Job):
            job_snapshot = copy.deepcopy(job.to_dict())
        elif isinstance(job, dict):
            if "task_type" not in job or "job_id" not in job:
                raise TaskLiteError("Job dict must contain 'task_type' and 'job_id'")
            job_snapshot = copy.deepcopy(job)
        else:
            raise TaskLiteError(
                f"Expected Job or dict, got {type(job).__name__}"
            )

        self.finalize_intake_snapshot(job_snapshot, str(job_snapshot["task_type"]))
        return job_snapshot

    def normalize_new_job_snapshot(self, job: Job, *, caller: str = "enqueue") -> dict[str, Any]:
        """解析并校验新声明的有效配置，要求 Task 已注册。"""
        if not isinstance(job, Job):
            raise TaskLiteError(
                f"{caller}() expects a Job, got {type(job).__name__}"
            )
        self._require_registered_task(job.task_type)
        job_snapshot = copy.deepcopy(job.to_dict())
        self.finalize_intake_snapshot(job_snapshot, job.task_type)
        return job_snapshot

    def normalize_spawned_job(self, job: Job) -> dict[str, Any]:
        """动态子作业与 enqueue 共用摄入规则，非法声明由父结算整体拒绝。"""
        self._require_registered_task(job.task_type)
        job_snapshot = job.to_dict()
        self.finalize_intake_snapshot(job_snapshot, job.task_type)
        return job_snapshot

    def finalize_intake_snapshot(self, job_snapshot: dict[str, Any], task_type: str) -> None:
        """校验并隔离 payload 后落实 Trigger 与资源。"""
        job_snapshot["payload"] = validate_strict_json_object(
            job_snapshot.get("payload"), field_name="payload"
        )
        self._trigger_registry.normalize_job_snapshot(job_snapshot, task_type)
        inject_worker_resource(job_snapshot)

    def enqueue_job(self, job: Job) -> bool:
        """单作业入队摄入管道：解析有效配置、捕获独立快照并校验同 UID 幂等/冲突。"""
        job_snapshot = self.normalize_new_job_snapshot(job, caller="enqueue")
        target_uid = job.uid

        for queued in self._backend.load_queue():
            try:
                queued_uid = job_uid_from_snapshot(queued)
            except (KeyError, TypeError, ValueError, TaskLiteError):
                continue
            if queued_uid == target_uid:
                if extract_effective_job_config(queued) == extract_effective_job_config(job_snapshot):
                    return False
                raise TaskLiteError(
                    f"Job {target_uid!r} is already queued with different effective configuration"
                )

        # 依赖引用存在性在提交时检查：只能引用已存在的 Job（队列或当前终态档案）
        if job.depends_on:
            queued_snapshots = self._backend.load_queue()
            existing_uids = {job_uid_from_snapshot(q) for q in queued_snapshots}
            existing_uids.update(self._backend.load_successes().keys())
            existing_uids.update(self._backend.load_failed().keys())
            for dep in job.depends_on:
                if dep not in existing_uids:
                    raise TaskLiteError(
                        f"Job {job.uid!r} references non-existent dependency {dep!r} in depends_on"
                    )
        inserted = self._backend.enqueue_jobs([job_snapshot])
        if inserted and target_uid not in self._state.queue_uids:
            self._state.spawn_jobs([copy.deepcopy(job_snapshot)], front=False)
        return bool(inserted)

    def mark_job_running(
        self, job_uid: str, *, execution_id: str, run_id: str, started_at: str
    ) -> int:
        """记录作业进入 running 状态并推进编号（事务性单一出口）。"""
        try:
            next_no = self._backend.mark_job_running(
                job_uid,
                execution_id=execution_id,
                run_id=run_id,
                started_at=started_at,
            )
            for item in self._state.queue:
                if job_uid_from_snapshot(item) == job_uid:
                    item["status"] = "running"
                    item["run_id"] = run_id
                    item["attempt_no"] = next_no
                    item["started_at"] = started_at
                    break
            return next_no
        except Exception as e:
            logger.critical(f"Failed to mark job running for {job_uid}: {e}")
            raise

    def trusted_last_succeeded_at_us(
        self, job_uid: str, *, success_metadata: Mapping[str, Any] | None = None
    ) -> int | None:
        """可信成功基线（整数微秒）：无可信档案则返回 None（调用方保守执行）。

        不变式：基线只取自同事务提交的 attempts succeeded 行；先以 successes
        档案框架元数据（seed 的空字典、缺键或坏值一律不可信）为闸门，且校验
        successes 框架时间戳与 attempts 实际完成时间戳严格一致，防止
        移除 successes 后残留的旧 attempts 成功行被拼接成伪基线。
        """
        if success_metadata is None:
            success_metadata = self._state.successes.get(job_uid)
        meta_ts = extract_trusted_metadata_timestamp_us(success_metadata)
        if meta_ts is None:
            return None
        finished_at = self._backend.load_latest_succeeded_finished_at(job_uid)
        if finished_at is None:
            return None
        try:
            attempt_ts = parse_utc_iso_to_us(finished_at)
        except (TypeError, ValueError):
            return None
        if meta_ts != attempt_ts:
            return None
        return attempt_ts

    # ── 内存状态受控接缝 ─────────────────────────────────────────────

    def mark_failed_memory(
        self, job_uid: str, metadata: dict[str, Any], *, unregister: bool = True
    ) -> None:
        """失败终态内存登记的唯一尾段（successes/failed 互斥单点维护）。

        独立复制框架档案，再落 failed 集合——state.mark_failed
        同步摘除 successes 同名行（成功/失败互删语义）；``unregister=False``
        供批量崩溃契约路径先登记、由调用方统一处置在途集合。
        """
        normalized = copy.deepcopy(metadata)
        self._state.mark_failed(job_uid, normalized)
        if unregister:
            self._state.unregister_in_flight(job_uid)

    @property
    def state(self) -> PipelineState:
        """底层 PipelineState 引用（内存状态唯一访问路径，供调度器/
        恢复编排层/机器群直读）。"""
        return self._state

    @property
    def backend(self) -> AbstractStateBackend:
        """底层持久化后端引用（所有权唯一锚点，换库经 set_backend 单点）。"""
        return self._backend

    def set_state(self, state: PipelineState | None) -> None:
        """重新设置内存状态（run 启动加载期使用）。"""
        self._state = state if state is not None else PipelineState({}, {}, {}, [])

    def set_backend(self, backend: AbstractStateBackend) -> None:
        """换库唯一锚点：backend 所有权归 StateStore 单持。

        不变式：本方法是全引擎唯一的后端重绑定点——其余组件（runtime /
        门面管理面）一律经 ``store.backend`` 只读派生，换库一处
        动作、全部持有者必然同步。
        """
        self._backend = backend

    # ── 路径授权管理与判定 ──────────────────────────────────────────

    def _inspect_lookup(self, path: str | Path) -> InspectedPath:
        """经统一 PathPolicy 以 lookup 模式解析路径（保留叶目录项不跟随）。"""
        try:
            return self._path_policy.inspect(path, mode="lookup")
        except ValueError as exc:
            raise TaskLiteError(str(exc)) from exc

    def _authorization_target(self, path: str | Path) -> Path:
        """固定基准解析父路径，保留叶目录项以免断链授权漂移到缺失目标。"""
        return Path(self._inspect_lookup(path).canonical_path)

    def is_path_protected(self, path: str | Path) -> bool:
        """检查路径是否与受保护路径集合（含 state_path 5 项）双向重叠。"""
        return self._path_policy.is_protected(path)

    def is_path_authorized(self, path: str | Path) -> bool:
        """检查指定规范绝对路径是否命中已有授权。"""
        inspected = self._inspect_lookup(path)
        return self._backend.is_path_authorized(inspected.canonical_path, inspected.target_type)

    def grant_authorization(
        self,
        path: str | Path,
        *,
        target_type: Literal["file", "directory"],
    ) -> dict[str, Any]:
        """Run 外显式预授权单个文件或目录（重复或父子类型冲突抛 TaskLiteError）。"""
        if type(target_type) is not str or target_type not in ("file", "directory"):
            raise TaskLiteError(
                f"target_type must be 'file' or 'directory', got {target_type!r}"
            )
        try:
            inspected = self._path_policy.inspect(path, mode="strict")
        except ValueError as exc:
            raise TaskLiteError(str(exc)) from exc
        return self._backend.grant_authorization(inspected.canonical_path, target_type)

    def record_authorization(
        self,
        path: str | Path,
        target_type: str,
        *,
        authorized_at: str | None = None,
    ) -> None:
        """持久化记录一条文件系统路径授权（运行期支持 file -> directory 升级）。"""
        inspected = self._inspect_lookup(path)
        self._backend.record_authorization(
            inspected.canonical_path, target_type, authorized_at=authorized_at
        )

    def list_authorizations(self) -> list[dict[str, Any]]:
        """查询全部已持久化的授权记录。"""
        return self._backend.list_authorizations()

    def revoke_authorization(self, path: str | Path) -> bool:
        """按规范绝对路径精确撤销授权。"""
        inspected = self._inspect_lookup(path)
        return self._backend.revoke_authorization(inspected.canonical_path)

    def check_or_request_authorization(self, path: str | Path) -> bool | None:
        """按实际目标范围查询、询问并持久化授权；None 表示等待决策。"""
        if self.is_path_protected(path):
            return False
        while True:
            inspected = self._inspect_lookup(path)
            norm_path = inspected.canonical_path
            effective_type = inspected.target_type
            if self._backend.is_path_authorized(norm_path, effective_type):
                return True
            if os.environ.get("TASKLITE_DANGEROUS_AUTO_APPROVE") == "1":
                decision: bool | None = True
            elif self.authorizer is not None:
                decision = self.authorizer(norm_path, effective_type)
            else:
                return False
            if decision is not True:
                return None if decision is None else False
            self._backend.record_authorization(norm_path, effective_type)
            # 询问期间文件可能变成目录，保存的文件授权不能扩大为目录树。
            after = self._inspect_lookup(path)
            if effective_type == "directory" or after.target_type != "directory":
                return True

    # ── 事务性原子终态转移（apply_* 出口族）──────────────────────────

    def apply_success(
        self,
        job_uid: str,
        *,
        result: dict[str, Any],
        spawned_job_snapshots: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
        declared_inputs: Sequence[str] = (),
        declared_artifacts: Mapping[str, FileSpec | str] | None = None,
        run_id: str | None = None,
        job_snapshot: dict[str, Any] | None = None,
        attempt_no: int = 1,
        started_at: str | None = None,
        files: dict[str, Any] | None = None,
        execution_id: str | None = None,
        finished_at: str | None = None,
    ) -> SuccessOutcome:
        """原子终态转移：成功（同事务终结对应 attempts 行 + successes 落盘 + 清失败档案残行 + 子任务/cursor）。"""
        effective_finished_at = finished_at if finished_at is not None else utc_now_iso()
        success_metadata = self._execution_record(result, files)
        success_metadata.update(
            run_id=run_id or "run", attempt_no=attempt_no,
            started_at=started_at, settled_at=effective_finished_at,
        )
        artifacts_manifest: dict[str, dict[str, str]] = {}
        if declared_artifacts:
            for name, spec_or_path in declared_artifacts.items():
                path_val = spec_or_path.path if isinstance(spec_or_path, FileSpec) else spec_or_path
                if isinstance(name, str) and name and isinstance(path_val, str) and path_val:
                    artifacts_manifest[name] = {"path": path_val}
        success_metadata["artifacts"] = artifacts_manifest
        success_metadata["last_succeeded_at"] = effective_finished_at
        if run_id is not None:
            success_metadata["last_success_run_id"] = run_id
        if job_snapshot is not None:
            success_metadata["job_snapshot"] = copy.deepcopy(job_snapshot)
        if declared_inputs:
            input_paths = list(dict.fromkeys(
                path for path in declared_inputs if isinstance(path, str) and path
            ))
            if input_paths:
                success_metadata["inputs"] = input_paths

        # 增量记录 success_count（防御非 int 脏数据；重跑成功从对侧终态续数）
        prev_meta = self._state.successes.get(job_uid) or self._state.failed.get(job_uid)
        prev_count = 0
        if isinstance(prev_meta, dict):
            try:
                prev_count = int(prev_meta.get("success_count", 0))
            except (TypeError, ValueError):
                prev_count = 0
        success_metadata["success_count"] = prev_count + 1

        spawned_snapshots = list(spawned_job_snapshots)
        self._backend.commit_job_settlement(
            job_uid=job_uid,
            outcome="success",
            attempt_no=attempt_no,
            execution_id=execution_id or "",
            run_id=run_id or "run",
            settled_at=effective_finished_at,
            started_at=started_at,
            job_snapshot=job_snapshot,
            files=files,
            result=success_metadata["result"],
            error_message=None,
            success_metadata=success_metadata,
            spawned_job_snapshots=spawned_snapshots,
            cursor_updates=cursor_updates,
        )

        if spawned_snapshots:
            self._state.spawn_jobs(spawned_snapshots, front=True)
        if cursor_updates:
            self._state.update_cursors(cursor_updates)
        self._state.mark_success(job_uid, success_metadata)
        self._state.unregister_in_flight(job_uid)
        spawned_uids = [job_uid_from_snapshot(j) for j in spawned_snapshots]
        return SuccessOutcome(job_uid=job_uid, success_metadata=success_metadata, spawned_uids=spawned_uids)

    def apply_failure(
        self,
        job_uid: str,
        failure_metadata: dict[str, Any],
        *,
        job_snapshot: dict[str, Any] | None = None,
        cascade: bool = True,
        attempt_no: int | None = None,
        run_id: str | None = None,
        started_at: str | None = None,
        settled_at: str | None = None,
        files: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        execution_id: str | None = None,
    ) -> FailureOutcome:
        """原子终态转移：永久失败（进入失败档案 + successes 互删 + 级联下游）。"""
        normalized_meta = self._failure_record(failure_metadata, job_snapshot)
        effective_settled_at = settled_at if settled_at is not None else normalized_meta["settled_at"]
        normalized_meta["settled_at"] = effective_settled_at
        if result is not None or attempt_no is not None:
            normalized_meta.update(self._execution_record(
                result if result is not None else normalized_meta["result"],
                files if files is not None else normalized_meta["files"],
            ))
        if attempt_no is not None:
            normalized_meta.update(
                run_id=run_id or "run", attempt_no=attempt_no,
                started_at=started_at, execution_id=execution_id,
            )
        payload = self._extract_job_payload(job_snapshot)
        if attempt_no is not None:
            self._backend.commit_job_settlement(
                job_uid=job_uid,
                outcome="failure",
                attempt_no=attempt_no,
                execution_id=execution_id or "",
                run_id=run_id or "run",
                settled_at=effective_settled_at,
                started_at=started_at,
                job_snapshot=job_snapshot,
                files=files,
                result=normalized_meta.get("result"),
                error_message=self._attempt_error(normalized_meta),
                failure_metadata=normalized_meta,
                payload=payload,
            )
        else:
            self._backend.commit_job_failure(job_uid, normalized_meta, payload)

        self.mark_failed_memory(job_uid, normalized_meta)
        cascaded_uids: list[str] = []
        if cascade:
            cascaded_uids = self.cascade_fail(job_uid)
        return FailureOutcome(job_uid=job_uid, failure_metadata=normalized_meta, cascaded_uids=cascaded_uids)

    @staticmethod
    def _failure_record(
        metadata: dict[str, Any], job_snapshot: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """当前失败档案自足；无对应执行时不得借用历史 Attempt 的事实。"""
        record = copy.deepcopy(metadata)
        record.update(
            settled_at=utc_now_iso(), run_id=None, attempt_no=None, started_at=None,
            files=copy.deepcopy((job_snapshot or {}).get("files") or {}),
            result={
                "message": metadata.get("error_message") or metadata.get("message"),
                "exception": copy.deepcopy(metadata.get("exception")),
                "metadata": {},
                "annotations": copy.deepcopy(metadata.get("annotations") or {}),
            },
        )
        if job_snapshot is not None:
            record["job_snapshot"] = copy.deepcopy(job_snapshot)
        return record

    @staticmethod
    def _execution_record(
        result: dict[str, Any], files: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """业务数据只存在于 result.metadata；档案根部仅容纳框架字段。"""
        result = copy.deepcopy(result)
        result["annotations"] = {
            **result.get("annotations", {}), "completion_source": "execution",
        }
        return {
            "result": result,
            "annotations": {"completion_source": "execution"},
            "files": copy.deepcopy(files or {}),
        }

    @staticmethod
    def _extract_job_payload(job_snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
        """提取可 JSON 序列化的业务 payload 快照（失败档案落盘用）。

        不可序列化时降级为 None 并告警，不阻断失败提交。
        """
        if not isinstance(job_snapshot, dict):
            return None
        payload = job_snapshot.get("payload")
        if not isinstance(payload, dict):
            return None
        try:
            dumps(payload)
        except (TypeError, ValueError):
            logger.warning(
                f"Job payload for {job_snapshot.get('job_id', '?')} is not "
                f"JSON-serializable; failure archive payload snapshot skipped."
            )
            return None
        return payload

    def apply_retry(
        self,
        job_uid: str,
        job_snapshot: dict[str, Any],
        retry_job_snapshot: dict[str, Any],
        *,
        attempt_no: int = 1,
        run_id: str | None = None,
        started_at: str | None = None,
        files: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        error_message: str | None = None,
        execution_id: str | None = None,
    ) -> RetryOutcome:
        """原子状态转移：重试重入队（节奏经 RequeuePolicy 唯一出口）。"""
        plan = self._requeue_policy.plan_requeue(retry_job_snapshot)
        self._backend.commit_job_settlement(
            job_uid=job_uid,
            outcome="retry",
            attempt_no=attempt_no,
            execution_id=execution_id or "",
            run_id=run_id or "run",
            settled_at=utc_now_iso(),
            started_at=started_at,
            job_snapshot=job_snapshot,
            files=files,
            result=result,
            error_message=error_message,
            requeued_job_snapshot=retry_job_snapshot,
        )

        self._state.unregister_in_flight(job_uid)
        self._state.requeue_jobs([retry_job_snapshot], front=plan.front)
        return RetryOutcome(job_uid=job_uid, retry_job_snapshot=retry_job_snapshot)

    def apply_pending(
        self,
        job_uid: str,
        job_snapshot: dict[str, Any],
        *,
        attempt_no: int = 1,
        run_id: str | None = None,
        started_at: str | None = None,
        files: dict[str, Any] | None = None,
        error_message: str | None = None,
        result: dict[str, Any] | None = None,
        execution_id: str | None = None,
        record_attempt: bool = True,
    ) -> bool:
        """受阻或停机执行回滚到待派发队列（写盘重置为 pending，释放在途并还原内存队列）。"""
        settled_at = utc_now_iso()
        self._backend.commit_job_settlement(
            job_uid=job_uid,
            outcome="pending",
            attempt_no=attempt_no,
            execution_id=execution_id or "",
            run_id=run_id or "run",
            settled_at=settled_at,
            started_at=started_at,
            job_snapshot=job_snapshot,
            files=files,
            result=result,
            error_message=error_message,
            record_attempt=record_attempt,
        )
        self._state.unregister_in_flight(job_uid)
        self._state.requeue_jobs([job_snapshot], front=True)
        return True

    def apply_skip(
        self,
        job_uid: str,
        *,
        job_snapshot: dict[str, Any] | None = None,
    ) -> SkipOutcome:
        """原子移除候选（清除磁盘与内存队列，不写 successes/failed）。"""
        self._backend.commit_skip(job_uid)
        self._state.remove_queued_uid(job_uid)
        self._state.unregister_in_flight(job_uid)
        return SkipOutcome(job_uid=job_uid, was_known=True)

    def apply_bulk_failure(
        self,
        uids_metas: Sequence[tuple[str, dict[str, Any]]],
    ) -> BulkFailureOutcome:
        """原子批量失败（死锁归因或批量熔断）；剩余队列由失败集单点派生。

        执行选择激活时按队列行 task_type 过滤被排除/跳过 uid：它们绝不进入
        失败档案（过滤后为空则直接返回空结果，由运行期选择仲裁收敛）。
        """
        excluded_uids = self._selection_excluded_uids()
        if excluded_uids:
            uids_metas = [
                (job_uid, metadata) for job_uid, metadata in uids_metas if job_uid not in excluded_uids
            ]
            if not uids_metas:
                return BulkFailureOutcome(
                    failed_uids=[], remaining_queue_count=len(self._state.queue)
                )
        queued = {job_uid_from_snapshot(j): j for j in self._state.queue}
        normalized_metas = [
            (job_uid, self._failure_record(metadata, queued.get(job_uid)))
            for job_uid, metadata in uids_metas
        ]
        self._backend.commit_job_failures(normalized_metas)

        failed_set = {u for u, _ in normalized_metas}
        remaining_queue = [
            jd for jd in self._state.queue
            if job_uid_from_snapshot(jd) not in failed_set
        ]
        self._state.replace_queue(list(remaining_queue))
        for job_uid, metadata in normalized_metas:
            self.mark_failed_memory(job_uid, metadata)
        return BulkFailureOutcome(
            failed_uids=[u for u, _ in normalized_metas],
            remaining_queue_count=len(self._state.queue),
        )

    def cascade_fail(self, failed_uid: str) -> list[str]:
        """父作业失败后 O(1) 级联标记全部下游为依赖失败。

        执行选择激活时，被排除作业是传播终止点：不进入级联失败集合，也不
        向其下游继续传播（下游保持待依赖留队，由后续 run 消费）。
        """
        excluded_uids = self._selection_excluded_uids()
        cascade_uids = self._state.cascade_fail(
            failed_uid, excluded_uids=excluded_uids
        )
        if excluded_uids:
            # 兜底过滤：终态写入出口绝不接受被排除 uid（与 DFS 终止点双保险）。
            cascade_uids = [job_uid for job_uid in cascade_uids if job_uid not in excluded_uids]
        if not cascade_uids:
            return []
        queued = {job_uid_from_snapshot(j): j for j in self._state.queue}
        cascade_metas = [
            (job_uid, self._failure_record(
                {"error_message": 'Dependency failed or has no record', "failed_dependency": failed_uid},
                queued.get(job_uid),
            ))
            for job_uid in cascade_uids
        ]
        self._backend.commit_job_failures(cascade_metas)
        cascade_set = set(cascade_uids)
        remaining = [
            jd for jd in self._state.queue
            if job_uid_from_snapshot(jd) not in cascade_set
        ]
        self._state.replace_queue(remaining)
        for job_uid, metadata in cascade_metas:
            self.mark_failed_memory(job_uid, metadata)
        return cascade_uids


__all__ = [
    "BulkFailureOutcome",
    "FailureOutcome",
    "RetryOutcome",
    "SkipOutcome",
    "StateStore",
    "SuccessOutcome",
]
