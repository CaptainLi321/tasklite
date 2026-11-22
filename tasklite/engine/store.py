"""v2 StateStore: 统一内存状态机与后端持久化事务的深模块。

内敛原子双写一致性、六集合全局互斥与依赖图级联分析。单一出口纪律：

- Job 终结事务唯一经由本模块的 apply_* 出口族收敛（成功 / 失败 / 重试 /
  跳过 / 批量失败 / 级联）——backend commit + 内存镜像 + 级联
  在同一出口内对齐，内存永不领先磁盘；
- 失败终态内存登记唯一经由 ``mark_failed_memory``（successes/failed 互斥的
  单点维护）；
- Attempt 为追加式执行轨迹，与对应结果同事务结算；事务异常原样传播。
"""

from __future__ import annotations

import copy
import logging
import os
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

from ..backend.base import AbstractStateBackend, GraphEditCommit
from ..exceptions import TaskLiteError
from ..models.artifact import FileSpec, resolve_file_spec
from ..models.job import Job
from ..models.records import (
    build_execution_failure_record,
    build_execution_success_record,
    extract_effective_job_config,
    extract_serializable_payload,
    normalize_job_intake_snapshot,
)
from ..models.state import PipelineState, job_uid_from_snapshot
from ..models.task import TaskRegistry
from ..utils.artifact import InspectedPath, PathPolicy
from ..utils.clock import parse_utc_iso_to_us, utc_now_iso
from .admission import (
    ImmediateRequeuePolicy,
    RequeuePolicy,
    TriggerRegistry,
    extract_trusted_metadata_timestamp_us,
)
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
        tasks: TaskRegistry | None = None,
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
        self._tasks = tasks
        self._trigger_registry = (
            trigger_registry if trigger_registry is not None else TriggerRegistry()
        )
        # 重试节奏唯一出口：核心不做任何节奏计算，默认立即重入队。
        self._requeue_policy = (
            requeue_policy if requeue_policy is not None else ImmediateRequeuePolicy()
        )
        self._session = session if session is not None else RunSession()

        fallback_base: str | Path = (
            base_dir if base_dir is not None else Path(os.path.abspath(os.getcwd()))
        )
        if path_policy is not None:
            if protected_roots or state_path is not None or path_policy.fixed_base_dir is None:
                merged_roots = (*path_policy.protected_paths, *protected_roots)
                effective_base: str | Path = (
                    path_policy.fixed_base_dir
                    if path_policy.fixed_base_dir is not None
                    else fallback_base
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
                base_dir=fallback_base,
                state_path=state_path,
                protected_roots=protected_roots,
            )

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
        """当前实例固定的相对路径解析基准目录（直接派生自 PathPolicy）。"""
        assert self._path_policy.fixed_base_dir is not None
        return Path(self._path_policy.fixed_base_dir)

    @property
    def session(self) -> RunSession:
        """当前关联的运行期会话（单次 Run 排除规则与生命周期状态真相源）。"""
        return self._session

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
        """获取底层状态库的独占执行权上下文（委托后端独占执行权契约）。"""
        return self._backend.exclusive_access()

    def load_consistent_snapshot(self) -> dict[str, Any]:
        """在单一读事务或单锁内原子捕获全部核心表一致快照。"""
        return self._backend.load_consistent_snapshot()

    def apply_graph_edit(self, commit: GraphEditCommit) -> None:
        """原子提交图修整事务并在落盘成功后以单次一致快照同步内存状态镜像。"""
        if (
            not commit.queue_writes
            and not commit.success_writes
            and not commit.failed_writes
            and not commit.recovery_attempts
        ):
            return
        self._backend.commit_graph_edit(commit)
        snap = self._backend.load_consistent_snapshot()
        self._state = PipelineState(
            snap["successes"],
            snap["failed"],
            snap["cursors"],
            [row["job"] for row in snap["queue_jobs"] if row["status"] == "pending"],
        )

    # ── 摄入管道（enqueue / spawn / edit add-replace 单一入口）────────

    def _require_registered_task(self, task_type: str) -> None:
        """新声明必须对应本实例已注册的 Task。"""
        if self._tasks is None or task_type not in self._tasks:
            raise TaskLiteError(
                f"unknown task_type {task_type!r} (register the task before "
                f"submitting its jobs)"
            )

    def normalize_new_job_snapshot(self, job: Job, *, caller: str = "enqueue") -> dict[str, Any]:
        """摄入唯一入口：校验 Task 已注册，捕获独立快照并落实规格继承、FileSpec 解析与触发器校验。"""
        if not isinstance(job, Job):
            raise TaskLiteError(
                f"{caller}() expects a Job, got {type(job).__name__}"
            )
        self._require_registered_task(job.task_type)
        job_snapshot = copy.deepcopy(job.to_dict())
        self.finalize_intake_snapshot(job_snapshot, job.task_type)
        return job_snapshot

    def finalize_intake_snapshot(self, job_snapshot: dict[str, Any], task_type: str) -> None:
        """落实 Task→Job 规格继承、FileSpec 解析、worker 槽位注入与已注册 Trigger 校验。"""
        task = self._tasks.get(task_type) if self._tasks is not None else None
        normalize_job_intake_snapshot(job_snapshot, task=task, base_dir=self.base_dir)
        self._trigger_registry.require_registered(job_snapshot.get("trigger"))

    def enqueue_job(self, job: Job) -> bool:
        """单作业入队摄入管道：单次一致快照校验全队列（含 pending/running）同 UID 幂等/冲突与依赖存在性。"""
        job_snapshot = self.normalize_new_job_snapshot(job, caller="enqueue")
        target_uid = job.uid

        queue_rows = self._backend.load_queue_jobs()

        for row in queue_rows:
            if row.get("job_uid") != target_uid:
                continue
            queued = row.get("job")
            if (
                isinstance(queued, Mapping)
                and extract_effective_job_config(queued) == extract_effective_job_config(job_snapshot)
            ):
                return False
            raise TaskLiteError(
                f"Job {target_uid!r} is already queued with different effective configuration"
            )

        # 依赖引用存在性按最小读集查验：允许引用全队列（含遗留 running）及终态档案，
        # 避免扫描无关的 attempts/cursors 表。
        if job.depends_on:
            existing_uids: set[str] = {
                row["job_uid"] for row in queue_rows if isinstance(row.get("job_uid"), str)
            }
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
            return self._backend.mark_job_running(
                job_uid,
                execution_id=execution_id,
                run_id=run_id,
                started_at=started_at,
            )
        except Exception as e:
            logger.critical(f"Failed to mark job running for {job_uid}: {e}")
            raise

    def trusted_last_succeeded_at_us(
        self, job_uid: str, *, success_metadata: Mapping[str, Any] | None = None
    ) -> int | None:
        """可信成功基线（整数微秒）：无可信档案则返回 None（调用方保守执行）。"""
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
        """失败终态内存登记的唯一尾段（successes/failed 互斥单点维护）。"""
        normalized = copy.deepcopy(metadata)
        self._state.mark_failed(job_uid, normalized)
        if unregister:
            self._state.unregister_in_flight(job_uid)

    @property
    def state(self) -> PipelineState:
        """底层 PipelineState 引用（内存状态唯一访问路径）。"""
        return self._state

    @property
    def backend(self) -> AbstractStateBackend:
        """底层持久化后端引用（所有权唯一锚点，换库经 set_backend 单点）。"""
        return self._backend

    def set_state(self, state: PipelineState | None) -> None:
        """重新设置内存状态（run 启动加载期使用）。"""
        self._state = state if state is not None else PipelineState({}, {}, {}, [])

    def set_backend(self, backend: AbstractStateBackend) -> None:
        """换库唯一锚点：backend 所有权归 StateStore 单持。"""
        self._backend = backend

    # ── 路径授权记录持久化存取 ───────────────────────────────────────

    def inspect_lookup(self, path: str | Path) -> InspectedPath:
        """经统一 PathPolicy 以 lookup 模式解析路径（保留叶目录项不跟随）。"""
        try:
            return self._path_policy.inspect(path, mode="lookup")
        except ValueError as exc:
            raise TaskLiteError(str(exc)) from exc

    def is_path_protected(self, path: str | Path) -> bool:
        """检查路径是否与受保护路径集合（含 state_path 5 项）双向重叠。"""
        return self._path_policy.is_protected(path)

    def is_path_authorized(
        self,
        path: str | Path,
        target_type: str | None = None,
        *,
        canonical: bool = False,
    ) -> bool:
        """检查指定路径是否命中已有授权；``canonical=True`` 复用调用方已解析结果。"""
        if canonical:
            if not isinstance(path, str) or target_type is None:
                raise TaskLiteError("canonical authorization check requires str path and target_type")
            return self._backend.is_path_authorized(path, target_type)
        inspected = self.inspect_lookup(path)
        effective_type = target_type if target_type is not None else inspected.target_type
        return self._backend.is_path_authorized(inspected.canonical_path, effective_type)

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
        canonical: bool = False,
    ) -> None:
        """持久化记录一条文件系统路径授权（运行期支持 file -> directory 升级）。"""
        norm_path = path if canonical and isinstance(path, str) else self.inspect_lookup(path).canonical_path
        self._backend.record_authorization(
            norm_path, target_type, authorized_at=authorized_at
        )

    def list_authorizations(self) -> list[dict[str, Any]]:
        """查询全部已持久化的授权记录。"""
        return self._backend.list_authorizations()

    def revoke_authorization(self, path: str | Path) -> bool:
        """按规范绝对路径精确撤销授权。"""
        inspected = self.inspect_lookup(path)
        return self._backend.revoke_authorization(inspected.canonical_path)

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
        prev_meta = self._state.successes.get(job_uid) or self._state.failed.get(job_uid)
        success_metadata = build_execution_success_record(
            result=result,
            files=files,
            run_id=run_id,
            attempt_no=attempt_no,
            started_at=started_at,
            settled_at=effective_finished_at,
            declared_inputs=declared_inputs,
            declared_artifacts=declared_artifacts,
            job_snapshot=job_snapshot,
            prev_meta=prev_meta,
        )

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
        effective_settled_at = settled_at if settled_at is not None else utc_now_iso()
        normalized_meta = build_execution_failure_record(
            failure_metadata,
            job_snapshot=job_snapshot,
            settled_at=effective_settled_at,
            attempt_no=attempt_no,
            run_id=run_id,
            started_at=started_at,
            files=files,
            result=result,
            execution_id=execution_id,
        )
        payload = extract_serializable_payload(job_snapshot)
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
            self._backend.commit_job_failures([(job_uid, normalized_meta, payload)])

        self.mark_failed_memory(job_uid, normalized_meta)
        cascaded_uids: list[str] = []
        if cascade:
            cascaded_uids = self.cascade_fail(job_uid)
        return FailureOutcome(job_uid=job_uid, failure_metadata=normalized_meta, cascaded_uids=cascaded_uids)

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

    def apply_skip(self, job_uid: str) -> SkipOutcome:
        """原子移除候选（清除磁盘与内存队列，不写 successes/failed）。"""
        self._backend.commit_skip(job_uid)
        self._state.remove_queued_uid(job_uid)
        self._state.unregister_in_flight(job_uid)
        return SkipOutcome(job_uid=job_uid, was_known=True)

    def apply_bulk_failure(
        self,
        uids_metas: Sequence[tuple[str, dict[str, Any]]],
    ) -> BulkFailureOutcome:
        """原子批量失败（死锁归因或批量熔断）；剩余队列由失败集单点派生。"""
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
            (
                job_uid,
                build_execution_failure_record(
                    metadata,
                    job_snapshot=queued.get(job_uid),
                    settled_at=utc_now_iso(),
                ),
            )
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
        """父作业失败后级联标记全部下游为依赖失败。"""
        excluded_uids = self._selection_excluded_uids()
        cascade_uids = self._state.cascade_fail(
            failed_uid, excluded_uids=excluded_uids
        )
        if excluded_uids:
            cascade_uids = [job_uid for job_uid in cascade_uids if job_uid not in excluded_uids]
        if not cascade_uids:
            return []
        queued = {job_uid_from_snapshot(j): j for j in self._state.queue}
        cascade_metas = [
            (
                job_uid,
                build_execution_failure_record(
                    {"error_message": 'Dependency failed or has no record', "failed_dependency": failed_uid},
                    job_snapshot=queued.get(job_uid),
                    settled_at=utc_now_iso(),
                ),
            )
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
