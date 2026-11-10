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

import copy
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Collection, Mapping, Sequence

from ..backend.base import AbstractStateBackend
from ..exceptions import TaskLiteError
from ..models.artifact import ArtifactDeclaration, resolve_path_against_base
from ..models.job import Job, inject_worker_resource
from ..models.result import validate_strict_json_object
from ..models.state import PipelineState, job_uid_from_snapshot
from ..selection import Exclude
from ..utils.clock import parse_utc_iso_to_us, utc_now_iso
from ..utils.encoding import parse_job_uid
from ..utils.jsonutil import dumps, loads
from .admission import (
    ImmediateRequeuePolicy,
    RequeuePolicy,
    TriggerRegistry,
    extract_effective_job_config,
    extract_trusted_metadata_timestamp_us,
    is_trusted_success_metadata,
)

logger = logging.getLogger("tasklite")


@dataclass(frozen=True)
class FailureEntry:
    """失败档案只读查询的结构化不可变条目（运维查询面的载荷形状）。

    - ``job_uid``: 作业唯一标识 (task_type::job_id)
    - ``error_message``: 失败说明
    - ``metadata``: 完整失败档案 meta 字典视图
    - ``payload``: 原始业务 payload 快照（失败落盘无可存快照时为 None）
    """

    job_uid: str
    error_message: str
    metadata: dict[str, Any]
    payload: dict[str, Any] | None = None


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
        stats: Any | None = None,
        trigger_registry: TriggerRegistry | None = None,
        requeue_policy: RequeuePolicy | None = None,
        base_dir: str | Path | None = None,
        protected_roots: Sequence[str | Path] = (),
    ) -> None:
        self._backend = backend
        self._state = state if state is not None else PipelineState({}, {}, {}, [])
        self._stats = stats
        self._trigger_registry = (
            trigger_registry
            if trigger_registry is not None
            else TriggerRegistry(base_dir=base_dir)
        )
        # 重试节奏唯一出口：核心不做任何节奏计算，默认立即重入队。
        self._requeue_policy = (
            requeue_policy if requeue_policy is not None else ImmediateRequeuePolicy()
        )
        self._exclude: Exclude | None = None
        self._temporary_excluded_uids: set[str] = set()
        self.protected_roots: tuple[Path, ...] = tuple(
            Path(root).resolve() for root in protected_roots
        )
        self.authorizer: Callable[[str, str], bool | None] | None = None

    @property
    def base_dir(self) -> Path:
        """当前实例固定的相对路径解析基准目录。"""
        return self._trigger_registry.base_dir

    # ── 内部统计 ────────────────────────────────────────────────

    def _record_stat(self, key: str, amount: int = 1) -> None:
        """更新统计指标（单一真相源）。"""
        if self._stats is not None:
            self._stats[key] = self._stats.get(key, 0) + amount

    @property
    def stats(self) -> Any:
        """统计字典引用（可能为 None）。"""
        return self._stats


    def set_run_exclusions(
        self,
        exclude: Exclude | None,
        temporary_uids: set[str] | None = None,
    ) -> None:
        """内部注入面：设置本 Run 排除规则与临时排除集合。"""
        self._exclude = exclude
        self._temporary_excluded_uids = set(temporary_uids) if temporary_uids is not None else set()

    def temporarily_exclude(self, job_uid: str) -> None:
        """登记单个临时排除 UID。"""
        self._temporary_excluded_uids.add(job_uid)

    def _selection_excluded_uids(self) -> frozenset[str]:
        """当前队列中被排除拦截（Exclude 或临时排除）的合法作业 UID 集合。"""
        if self._exclude is None and not self._temporary_excluded_uids:
            return frozenset()
        excluded: set[str] = set(self._temporary_excluded_uids)
        if self._exclude is not None:
            for job_snapshot in self._state.queue:
                try:
                    job = Job.from_dict(job_snapshot)
                except (KeyError, TypeError, ValueError, TaskLiteError):
                    continue
                if self._exclude.matches_task_type(job.task_type):
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

    def get_job_snapshot(self, uid: str) -> dict[str, Any] | None:
        """查找指定 uid 的完整作业快照（依次检索队列、成功档案、失败档案与 Attempt 轨迹）。"""
        return self._backend.load_known_job_snapshot(uid)

    def get_all_known_jobs(self) -> dict[str, dict[str, Any]]:
        """检索当前已知的全部作业完整快照映射（queue/successes/failed/attempts 并集）。"""
        return self._backend.load_known_job_snapshots()

    def _find_downstream_uids(self, root_uid: str, all_known: dict[str, dict[str, Any]]) -> list[str]:
        """按已知记录的 depends_on 拓扑构建反向依赖图，遍历收集全部下游作业 UID。"""
        adjacency: dict[str, set[str]] = defaultdict(set)
        for u, snap in all_known.items():
            for dep in snap.get("depends_on") or ():
                adjacency[dep].add(u)
        downstream: list[str] = []
        visited = {root_uid}
        queue = [root_uid]
        while queue:
            curr = queue.pop(0)
            for child in sorted(adjacency.get(curr, ())):
                if child not in visited:
                    visited.add(child)
                    queue.append(child)
                    downstream.append(child)
        return downstream

    def set_job_status(
        self,
        job_or_uid: Job | str,
        status: str,
        *,
        recursive: bool,
        note: str | None = None,
    ) -> None:
        """人工设置作业状态（pending/succeeded/failed），原子处理目标及递归下游。"""
        if status not in ("pending", "succeeded", "failed"):
            raise TaskLiteError(
                f"status must be one of 'pending', 'succeeded', 'failed', got {status!r}"
            )
        if not isinstance(recursive, bool):
            raise TaskLiteError(f"recursive must be a bool, got {type(recursive).__name__}")
        if note is not None and not isinstance(note, str):
            raise TaskLiteError(f"note must be a str or None, got {type(note).__name__}")

        if isinstance(job_or_uid, Job):
            target_job = job_or_uid
            target_uid = job_or_uid.uid
        elif isinstance(job_or_uid, str):
            if not job_or_uid:
                raise TaskLiteError("job_or_uid cannot be empty")
            parse_job_uid(job_or_uid)
            target_job = None
            target_uid = job_or_uid
        else:
            raise TaskLiteError(
                f"job_or_uid must be a Job or str, got {type(job_or_uid).__name__}"
            )

        if target_job is not None:
            intake_snap = self.normalize_and_validate_job(target_job)
            existing_snap = self.get_job_snapshot(target_uid)
            if existing_snap is not None:
                if extract_effective_job_config(existing_snap) != extract_effective_job_config(intake_snap):
                    raise TaskLiteError(
                        f"Job configuration conflict for UID {target_uid!r}: "
                        f"passed Job does not match existing effective configuration"
                    )
                target_snap = existing_snap
            else:
                target_snap = intake_snap
        else:
            found = self.get_job_snapshot(target_uid)
            if found is None:
                raise TaskLiteError(f"No existing snapshot found for job UID {target_uid!r}")
            target_snap = found

        all_known = self.get_all_known_jobs()
        all_known[target_uid] = target_snap

        affected: list[tuple[str, dict[str, Any]]] = [(target_uid, target_snap)]
        if recursive:
            downstream_uids = self._find_downstream_uids(target_uid, all_known)
            for child_uid in downstream_uids:
                child_snap = all_known.get(child_uid)
                if child_snap is not None:
                    affected.append((child_uid, child_snap))

        updates: list[tuple[str, str, dict[str, Any], dict[str, Any] | None]] = []
        for u, snap in affected:
            if status == "pending":
                updates.append((u, "pending", snap, None))
            elif status == "succeeded":
                annotations = {"completion_source": "manual"}
                if note is not None:
                    annotations["note"] = note
                succ_meta: dict[str, Any] = {
                    "job_snapshot": snap,
                    "annotations": annotations,
                    "settled_at": utc_now_iso(),
                    "run_id": None,
                    "attempt_no": None,
                    "started_at": None,
                    "artifacts": {},
                    "inputs": [],
                }
                updates.append((u, "succeeded", snap, succ_meta))
            elif status == "failed":
                fail_meta: dict[str, Any] = {
                    "job_snapshot": snap,
                    "annotations": {"completion_source": "manual"},
                    "error_message": note or "Manual status set to failed",
                    "settled_at": utc_now_iso(),
                    "run_id": None,
                    "attempt_no": None,
                }
                if note is not None:
                    fail_meta["note"] = note
                updates.append((u, "failed", snap, fail_meta))

        self._backend.apply_manual_status(updates)
        for u, st, snap, up_meta in updates:
            if st == "pending":
                self._state.discard_success(u)
                self._state.discard_failed(u)
                self._state.remove_queued_uid(u)
                self._state.spawn_jobs([snap], front=False)
            elif st == "succeeded":
                self._state.remove_queued_uid(u)
                self._state.mark_success(u, up_meta or {})
            elif st == "failed":
                self._state.remove_queued_uid(u)
                self._state.mark_failed(u, up_meta or {})

    def clear_history(self, uid: str, records: Collection[str]) -> int:
        """从 successes/failed/attempts 清除指定 job_uid 的记录，返回删除条数。"""
        if not isinstance(uid, str) or not uid:
            raise TaskLiteError(f"uid must be a non-empty str, got {uid!r}")
        try:
            parse_job_uid(uid)
        except Exception as exc:
            raise TaskLiteError(
                f"uid must be a valid canonical 'task_type::job_id' str, got {uid!r}"
            ) from exc
        if not isinstance(records, (list, tuple, set, frozenset)):
            raise TaskLiteError(
                "records must be a collection of 'successes', 'failures', "
                f"'attempts', got {type(records).__name__}"
            )
        record_set = set(records)
        allowed = {"successes", "failures", "attempts"}
        unknown = record_set - allowed
        if not record_set:
            raise TaskLiteError("records must not be empty")
        if unknown:
            raise TaskLiteError(
                f"records contains unknown target(s): {sorted(unknown)!r}; "
                f"allowed: {sorted(allowed)!r}"
            )
        total = self._backend.clear_history(uid, record_set)
        if "successes" in record_set:
            self._state.discard_success(uid)
        if "failures" in record_set:
            self._state.discard_failed(uid)
        return total

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

    def _normalize_new_job_snapshot(self, job: Job) -> dict[str, Any]:
        """解析并校验新声明的有效配置，要求 Task 已注册。"""
        if not isinstance(job, Job):
            raise TaskLiteError(
                f"enqueue() expects a Job, got {type(job).__name__}"
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
        if not isinstance(job, Job):
            raise TaskLiteError(
                f"enqueue() expects a Job, got {type(job).__name__}"
            )
        job_snapshot = self._normalize_new_job_snapshot(job)
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
        clear_history 后残留的旧 attempts 成功行被拼接成伪基线。
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

    def _authorization_target(self, path: str | Path) -> Path:
        """固定基准解析父路径，保留叶目录项以免断链授权漂移到缺失目标。"""
        target = Path(resolve_path_against_base(path, self.base_dir))
        return target.parent.resolve() / target.name

    def is_path_protected(self, path: str | Path) -> bool:
        """检查路径是否与受保护根目录（如状态目录）重叠。"""
        try:
            raw = Path(os.fspath(path))
            target = (self.base_dir / raw).resolve() if not raw.is_absolute() else raw.resolve()
        except Exception:
            return True
        for root in self.protected_roots:
            try:
                if root.is_relative_to(target) or target.is_relative_to(root):
                    return True
            except (ValueError, TypeError):
                pass
        return False

    def is_path_authorized(self, path: str | Path) -> bool:
        """检查指定规范绝对路径是否命中已有授权。"""
        norm_path = str(self._authorization_target(path))
        target_type = "directory" if os.path.isdir(norm_path) else "file"
        return self._backend.is_path_authorized(norm_path, target_type)

    def record_authorization(
        self,
        path: str | Path,
        target_type: str,
        *,
        authorized_at: str | None = None,
    ) -> None:
        """持久化记录一条文件系统路径授权。"""
        norm_path = str(self._authorization_target(path))
        self._backend.record_authorization(norm_path, target_type, authorized_at=authorized_at)

    def list_authorizations(self) -> list[dict[str, Any]]:
        """查询全部已持久化的授权记录。"""
        return self._backend.list_authorizations()

    def revoke_authorization(self, path: str | Path) -> bool:
        """按规范绝对路径精确撤销授权。"""
        norm_path = str(self._authorization_target(path))
        return self._backend.revoke_authorization(norm_path)

    def check_or_request_authorization(self, path: str | Path) -> bool | None:
        """按实际目标范围查询、询问并持久化授权；None 表示等待决策。"""
        if self.is_path_protected(path):
            return False
        norm_path = str(self._authorization_target(path))
        while True:
            effective_type = "directory" if os.path.isdir(norm_path) else "file"
            if self._backend.is_path_authorized(norm_path, effective_type):
                return True
            if os.environ.get("TASKLITE_AUTO_APPROVE") == "1":
                decision: bool | None = True
            elif self.authorizer is not None:
                decision = self.authorizer(norm_path, effective_type)
            else:
                return False
            if decision is not True:
                return None if decision is None else False
            self._backend.record_authorization(norm_path, effective_type)
            # 询问期间文件可能变成目录，保存的文件授权不能扩大为目录树。
            if effective_type == "directory" or not os.path.isdir(norm_path):
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
        declared_artifacts: Sequence[ArtifactDeclaration] = (),
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
        declarations = tuple(declared_artifacts)
        success_metadata["artifacts"] = {
            declaration.name: {"path": declaration.path}
            for declaration in declarations if declaration.name is not None
        }
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
        self._record_stat("succeeded", 1)
        spawned_uids = [job_uid_from_snapshot(j) for j in spawned_snapshots]
        return SuccessOutcome(job_uid=job_uid, success_metadata=success_metadata, spawned_uids=spawned_uids)

    def apply_failure(
        self,
        job_uid: str,
        failure_metadata: dict[str, Any],
        *,
        job_snapshot: dict[str, Any] | None = None,
        count_as: str = "failed",
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
        self._record_stat(count_as, 1)
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
        self._record_stat("retried", 1)
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
        self._record_stat("skipped", 1)
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
        self._record_stat("failed", len(normalized_metas))
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
        self._record_stat("cascade_failed", len(cascade_uids))
        for job_uid, metadata in cascade_metas:
            self.mark_failed_memory(job_uid, metadata)
        return cascade_uids


__all__ = [
    "BulkFailureOutcome",
    "FailureEntry",
    "FailureOutcome",
    "RetryOutcome",
    "SkipOutcome",
    "StateStore",
    "SuccessOutcome",
]
