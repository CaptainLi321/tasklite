"""v2 完成机器：结果提交、输出清理、资源释放与在飞结算。

``complete_job`` 是已进入 running 的执行结果的唯一收尾入口，``apply_result`` 承载
retry/success/failure 三态事务提交。重试预算只统计本 Run 中显式
``TaskResult.retry`` 的消耗；候选来源与累计数量分开登记。

``session`` 按具名 Protocol ``DispatchSession`` 消费（``run_id`` 是
successes meta 的 last_run_id 事实源；契约定义见 dispatch 模块）。
"""

from __future__ import annotations

import copy
import logging
import time
import traceback
from dataclasses import dataclass
from typing import Any, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from .admission import TriggerRegistry
    from .channel import ExecutionChannel
    from .dispatch import DispatchSession
    from .in_flight import InFlightTracker
    from .resource import ResourceManager
    from .store import StateStore

from ..exceptions import TaskLiteError, _ArtifactIOError
from ..models.job import Job, JobRuntimeState
from ..models.artifact import ArtifactDeclaration, FileSpec
from ..utils.artifact import ArtifactPathPolicy
from ..utils.clock import utc_now_iso
from .admission import extract_effective_job_config
from .channel import ExecutionResult, JobHandle
from .errorclass import ERR_MAX_RETRIES
from .in_flight import InFlightJob, InFlightTracker
from .types import AttemptOutcome, AuthorizationOutcome, AuthorizationStatus, StopMode

logger = logging.getLogger("tasklite")
_CLEANUP_ERRORS = (OSError, ValueError, NotImplementedError)


@dataclass
class _PreparedCompletion:
    active_files: dict[str, FileSpec]
    spawned_job_snapshots: list[dict[str, Any]]


class _CleanupAuthorizationPending(Exception):
    """清理尚待授权，保留有效结果及在途归属。"""


class CompletionMachine:
    """结果提交 / 输出清理 / 资源释放 / 在飞结算的完成侧机器。"""

    def __init__(
        self,
        *,
        store: "StateStore",
        trigger_registry: "TriggerRegistry | None" = None,
        channel: "ExecutionChannel",
        resources: "ResourceManager",
        in_flight: "InFlightTracker",
        session: "DispatchSession",
    ) -> None:
        self._store = store
        self._trigger_registry = trigger_registry
        self._channel = channel
        self._resources = resources
        self._in_flight = in_flight
        self._session = session
        self._pending_cleanup: dict[str, tuple[InFlightJob, ExecutionResult, _PreparedCompletion]] = {}
        self._fallback_retry_counts: dict[str, int] = {}
        self._fallback_retry_candidates: set[str] = set()

    def _get_retry_count(self, uid: str) -> int:
        getter = getattr(self._session, "get_retry_count", None)
        if callable(getter):
            return int(getter(uid))
        return self._fallback_retry_counts.get(uid, 0)

    def _record_retry(self, uid: str) -> int:
        recorder = getattr(self._session, "record_retry", None)
        if callable(recorder):
            return int(recorder(uid))
        updated = self._fallback_retry_counts.get(uid, 0) + 1
        self._fallback_retry_counts[uid] = updated
        return updated

    def _mark_retry_candidate(self, uid: str) -> None:
        marker = getattr(self._session, "mark_retry_candidate", None)
        if callable(marker):
            marker(uid)
        else:
            self._fallback_retry_candidates.add(uid)

    def _clear_retry_candidate(self, uid: str) -> None:
        clearer = getattr(self._session, "clear_retry_candidate", None)
        if callable(clearer):
            clearer(uid)
        else:
            self._fallback_retry_candidates.discard(uid)

    @staticmethod
    def _cleanup_exception(exc: BaseException) -> dict[str, str]:
        return {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }

    @classmethod
    def _attach_cleanup_failure(
        cls,
        result: ExecutionResult,
        exc: BaseException,
    ) -> None:
        details = cls._cleanup_exception(exc)
        result.annotations["cleanup_error"] = str(exc)
        result.annotations["cleanup_exception"] = details
        if result.exception is None:
            result.exception = details

    @classmethod
    def _record_failure_cleanup_error(
        cls,
        result: ExecutionResult,
        exc: BaseException,
    ) -> None:
        cls._attach_cleanup_failure(result, exc)
        if result.retry_requested:
            original_message = result.message
            if original_message:
                result.annotations["cleanup_original_message"] = original_message
            result.retry_requested = False
            result.message = f"Retry cleanup failed: {exc}"
            if original_message:
                result.message = f"{result.message} (original: {original_message})"
            return
        if result.message:
            result.message = f"{result.message} (cleanup failed: {exc})"
        else:
            result.message = f"Cleanup failed: {exc}"

    # ── 唯一收尾出口 ──────────────────────────────────────────────────

    @property
    def waiting_authorization(self) -> bool:
        return bool(self._pending_cleanup)

    def has_pending_result(self, handle: JobHandle) -> bool:
        return handle.job_uid in self._pending_cleanup

    def _authorize_cleanup(
        self, entry: InFlightJob | None, result: ExecutionResult,
        active_files: dict[str, FileSpec],
    ) -> None:
        policy = ArtifactPathPolicy(
            output_roots=getattr(self._channel, "output_roots", None),
            protected_roots=self._channel.protected_roots,
        )
        for spec in active_files.values():
            requested = spec.delete_on_success if result.success else spec.delete_on_failure
            if not requested or spec.path is None:
                continue
            try:
                if not policy.entry_exists(ArtifactDeclaration(spec.path)):
                    continue
            except _CLEANUP_ERRORS:
                continue
            stopped = getattr(self._session, "stop_mode", StopMode.NONE) is not StopMode.NONE
            waiting = entry is not None and entry.job_uid in self._pending_cleanup
            if stopped and waiting:
                status = AuthorizationStatus.STOPPED
                message = f"authorization wait stopped for {spec.path}"
            else:
                decision = self._store.check_or_request_authorization(spec.path)
                stopped = getattr(self._session, "stop_mode", StopMode.NONE) is not StopMode.NONE
                if stopped and (waiting or decision is None):
                    status = AuthorizationStatus.STOPPED
                    message = f"authorization wait stopped for {spec.path}"
                else:
                    if decision is True:
                        continue
                    if decision is None:
                        raise _CleanupAuthorizationPending
                    status = AuthorizationStatus.DENIED
                    message = f"authorization denied for {spec.path}"
            result.authorization_outcome = AuthorizationOutcome(status, message, spec.path)
            if entry is not None and entry.handle is not None:
                self._channel.terminate_process(entry.handle)
            raise _CleanupAuthorizationPending
        # 后续询问回调可能改变此前目标，整组通过后再交付实际清理。
        for spec in active_files.values():
            requested = spec.delete_on_success if result.success else spec.delete_on_failure
            if requested and spec.path and not self._store.is_path_authorized(spec.path):
                try:
                    present = policy.entry_exists(ArtifactDeclaration(spec.path))
                except _CLEANUP_ERRORS:
                    continue
                if present:
                    raise _CleanupAuthorizationPending

    def complete_job(
        self, entry: InFlightJob, result: ExecutionResult,
        *, prepared: _PreparedCompletion | None = None,
    ) -> None:
        """处理一个在飞作业的完成结果（唯一收尾出口）。

        事务性提交与内存 apply 见 ``apply_result``（两条提交路径共用）；
        本方法额外负责在飞注销时机下推、资源释放、失败输出清理与收尾
        钩子恰好一次。

        身份非真空：在飞注销**不在本方法开头**统一执行——那会造成
        「四集合真空」窗口（uid 不在 successes/failed/queue/in-flight 任一
        集合），使 handler 自 spawn 同 uid 子任务时被 ``is_known`` 漏判。
        注销时机下推到 ``apply_result`` 内部分支（retry 在重入队前、
        success 在 mark_success 后、failure 在 mark_failed 后），保证
        uid 在 ``is_known`` 可达的代码段内始终属于至少一个集合。

        声明只在业务结算完成后删除；提交前异常保留声明以便诊断。
        """
        uid = entry.job_uid
        try:
            self.apply_result(
                uid,
                entry.job,
                entry.job_snapshot,
                result,
                started_monotonic=entry.started_monotonic,
                expect_in_flight=(entry.handle is not None),
                execution_id=entry.execution_id if entry.execution_id is not None else (
                    entry.handle.execution_id if entry.handle is not None else None
                ),
                attempt_no=entry.attempt_no,
                run_id=entry.run_id or getattr(self._session, "run_id", "run"),
                started_at=entry.started_at,
                entry=entry,
                prepared=prepared,
            )
        finally:
            # 资源释放（无论成功/失败/重试/崩溃，由 entry 自归还）
            entry.release_resources(self._resources)

        # 收尾钩子在统计更新之后（apply_result 已 +1）触发——钩子内读统计
        # 一致；单一出口经 store.fire_attempt_finished（异常隔离 + 计数），
        # 正常提交与伪 entry 都经此触发。
        self._store.fire_attempt_finished(
            uid,
            outcome=AttemptOutcome(
                success=bool(result.success),
                will_retry=result.will_retry,
                metadata=dict(result.result_metadata),
            ),
        )

    def _prepare_completion(
        self, uid: str, job: Job, result: ExecutionResult,
        entry: InFlightJob | None,
    ) -> _PreparedCompletion:
        store = self._store
        active_files: dict[str, FileSpec] = {}
        if entry is not None and getattr(entry, "active_files", None):
            active_files = dict(entry.active_files)
        elif job.files:
            active_files = {
                k: v.resolve_against(store.base_dir) if not v.is_output_ref else v
                for k, v in job.files.items()
            }

        if result.authorization_outcome is not None:
            return _PreparedCompletion(active_files, [])
        if result.artifact_error is not None:
            raise _ArtifactIOError(result.artifact_error)
        if result.success:
            policy = ArtifactPathPolicy(
                output_roots=getattr(self._channel, "output_roots", None),
                protected_roots=self._channel.protected_roots,
            )
            for fname, spec in active_files.items():
                if spec.role != "output":
                    continue
                if spec.path is None:
                    result.success = False
                    result.message = f"Missing output path for {fname!r}"
                    break
                try:
                    present = policy.entry_exists(
                        ArtifactDeclaration(spec.path, sandbox=False)
                    )
                except _CLEANUP_ERRORS as exc:
                    result.success = False
                    result.message = f"Output verification failed: {exc}"
                    self._attach_cleanup_failure(result, exc)
                    break
                if not present:
                    result.success = False
                    result.message = f"Missing output {spec.path}"
                    break

        spawned_job_snapshots: list[dict[str, Any]] = []
        if result.success and result.spawned_jobs:
            try:
                spawned_job_snapshots = self._collect_spawned_job_snapshots(
                    uid, result.spawned_jobs, entry=entry
                )
            except Exception as err:
                logger.error(f"Spawned jobs validation failed for {uid}: {err}")
                result.success = False
                result.message = f"Invalid spawned job: {err}"
                spawned_job_snapshots = []

        if result.success and result.cursor_updates:
            for k, v in result.cursor_updates.items():
                if not isinstance(k, str) or not k or (v is not None and not isinstance(v, str)):
                    result.success = False
                    result.message = f"Invalid cursor update: {k!r}={v!r}"
                    break

        return _PreparedCompletion(active_files, spawned_job_snapshots)

    def apply_result(
        self,
        uid: str,
        job: Job,
        job_snapshot: dict[str, Any],
        result: ExecutionResult,
        *,
        started_monotonic: float | None = None,
        expect_in_flight: bool = True,
        execution_id: str | None = None,
        attempt_no: int = 1,
        run_id: str | None = None,
        started_at: str | None = None,
        entry: InFlightJob | None = None,
        prepared: _PreparedCompletion | None = None,
    ) -> None:
        """事务性提交一个执行结果到后端并同步内存。

        先提交后端（唯一真相源），commit 成功后才 apply 到内存 state。
        三态分发：
          - retry_requested -> ``_apply_retry``
          - success -> ``_apply_success``
          - 其余 -> ``_apply_failure``
        """
        store = self._store
        if expect_in_flight:
            assert uid in store.state.in_flight_uids, (
                f"identity vacuity violation: {uid} not in-flight "
                f"at apply_result entry"
            )

        if prepared is None:
            prepared = self._prepare_completion(uid, job, result, entry)
            if result.authorization_outcome is None:
                self._authorize_cleanup(entry, result, prepared.active_files)
        active_files = prepared.active_files

        authorization_outcome = result.authorization_outcome
        if (
            authorization_outcome is not None
            and authorization_outcome.status
            in (AuthorizationStatus.DENIED, AuthorizationStatus.STOPPED)
        ):
            # 不变式：资源释放先于 running 回退事务，Release 回调仍须观察 running。
            if entry is not None:
                entry.release_resources(self._resources)
            self._store.apply_pending(
                job_uid=uid,
                job_snapshot=job_snapshot,
                attempt_no=attempt_no,
                execution_id=execution_id,
                run_id=run_id or getattr(self._session, "run_id", "run"),
                started_at=started_at,
                error_message=authorization_outcome.message,
                files={k: v.to_dict() for k, v in active_files.items()},
                result={
                    "message": authorization_outcome.message,
                    "exception": None,
                    "metadata": {},
                    "annotations": {
                        "authorization_status": authorization_outcome.status.value,
                        "path": authorization_outcome.path,
                    },
                },
            )
            if authorization_outcome.status is AuthorizationStatus.DENIED:
                exclude_fn = getattr(self._session, "temporarily_exclude_chain", None)
                if callable(exclude_fn):
                    exclude_fn(uid)
            result.will_retry = False
            return

        spawned_job_snapshots = prepared.spawned_job_snapshots

        # 声明式产物主进程清理
        if active_files:
            policy = ArtifactPathPolicy(
                output_roots=getattr(self._channel, "output_roots", None),
                protected_roots=self._channel.protected_roots,
                authorization_check=lambda path, kind: self._store.backend.is_path_authorized(path, kind),
            )
            if result.success:
                for fname, spec in active_files.items():
                    if not (spec.delete_on_success and spec.path):
                        continue
                    declaration = ArtifactDeclaration(spec.path, delete_on_success=True)
                    try:
                        present = policy.entry_exists(declaration)
                    except _CLEANUP_ERRORS as exc:
                        result.success = False
                        result.message = f"Success cleanup failed: {exc}"
                        self._attach_cleanup_failure(result, exc)
                        break
                    if not present:
                        continue
                    try:
                        policy.cleanup(declaration, success=True)
                    except _CLEANUP_ERRORS as exc:
                        result.success = False
                        result.message = f"Success cleanup failed: {exc}"
                        self._attach_cleanup_failure(result, exc)
                        break
                if not result.success:
                    spawned_job_snapshots = []
                    result.cursor_updates = {}
            else:
                for fname, spec in active_files.items():
                    if not (spec.delete_on_failure and spec.path):
                        continue
                    declaration = ArtifactDeclaration(spec.path, delete_on_failure=True)
                    try:
                        present = policy.entry_exists(declaration)
                    except _CLEANUP_ERRORS as exc:
                        self._record_failure_cleanup_error(result, exc)
                        continue
                    if not present:
                        continue
                    try:
                        policy.cleanup(declaration, success=False)
                    except _CLEANUP_ERRORS as exc:
                        self._record_failure_cleanup_error(result, exc)

        # 释放资源（必须在事务提交之前完成）
        if entry is not None:
            entry.release_resources(self._resources)

        # 2. 状态分发
        if result.retry_requested:
            self._apply_retry(
                uid,
                job,
                job_snapshot,
                result,
                execution_id=execution_id,
                attempt_no=attempt_no,
                run_id=run_id,
                started_at=started_at,
                active_files=active_files,
            )
        elif result.success:
            self._apply_success(
                uid,
                job_snapshot,
                result,
                started_monotonic=started_monotonic,
                execution_id=execution_id,
                attempt_no=attempt_no,
                run_id=run_id,
                started_at=started_at,
                spawned_job_snapshots=spawned_job_snapshots,
                active_files=active_files,
            )
        else:
            self._apply_failure(
                uid,
                job_snapshot,
                result,
                started_monotonic=started_monotonic,
                execution_id=execution_id,
                attempt_no=attempt_no,
                run_id=run_id,
                started_at=started_at,
                active_files=active_files,
            )

    # ── 三态分支 ──────────────────────────────────────────────────────

    def _apply_retry(
        self,
        uid: str,
        job: Job,
        job_snapshot: dict[str, Any],
        result: ExecutionResult,
        *,
        execution_id: str | None = None,
        attempt_no: int = 1,
        run_id: str | None = None,
        started_at: str | None = None,
        active_files: dict[str, FileSpec] | None = None,
    ) -> None:
        """重试分支：预算判定、快照推进与来源登记。

        ``consume_budget=True`` 时消耗本 Run 的 uid 级预算；耗尽则转为失败。
        ``consume_budget=False`` 不消耗预算，但仍推进 attempt_no。
        """
        consume_budget = result.consume_budget
        retry_error = result.message
        max_retries = job.max_retries_per_run if job.max_retries_per_run is not None else 3

        if consume_budget:
            consumed = self._get_retry_count(uid)
            if consumed >= max_retries:
                logger.error(
                    f"FAIL: {uid} exceeded max retries ({max_retries}). "
                    f"Sent to failure archive."
                )
                failure_metadata: dict[str, Any] = {
                    "error_message": ERR_MAX_RETRIES,
                }
                if result.message is not None:
                    failure_metadata["message"] = result.message
                rt_state = JobRuntimeState.from_dict(job_snapshot.get("runtime"))
                if rt_state.last_retry_error:
                    failure_metadata["last_retry_error"] = rt_state.last_retry_error
                if retry_error:
                    failure_metadata["retry_error"] = retry_error
                self._apply_failure(
                    uid,
                    job_snapshot,
                    result,
                    execution_id=execution_id,
                    attempt_no=attempt_no,
                    run_id=run_id,
                    started_at=started_at,
                    failure_record_override=failure_metadata,
                    active_files=active_files,
                )
                result.will_retry = False
                return
            self._record_retry(uid)

        retry_job_snapshot = self._build_retry_job_snapshot(
            job, job_snapshot, retry_error=retry_error
        )
        files_dict = (
            {k: v.to_dict() for k, v in active_files.items()}
            if active_files
            else job_snapshot.get("files")
        )
        logger.info(f"RETRY: {uid} (attempt {attempt_no})")
        self._store.apply_retry(
            uid,
            job_snapshot,
            retry_job_snapshot,
            execution_id=execution_id,
            attempt_no=attempt_no,
            run_id=run_id or self._session.run_id,
            started_at=started_at,
            error_message=retry_error,
            files=files_dict,
            result={
                "message": result.message,
                "exception": result.exception,
                "metadata": dict(result.result_metadata or {}),
                "annotations": dict(result.annotations or {}),
            },
        )
        result.will_retry = True
        self._mark_retry_candidate(uid)

    @staticmethod
    def _build_retry_job_snapshot(
        job: Job,
        job_snapshot: dict[str, Any],
        *,
        retry_error: str | None,
    ) -> dict[str, Any]:
        """重试字典组装：保留原快照，只推进 attempt_no 与重试说明。"""
        retry_job_snapshot = dict(job_snapshot)
        job.attempt_no = job.attempt_no + 1
        retry_job_snapshot.update(job.to_dict())
        retry_job_snapshot["resources"] = dict(job_snapshot.get("resources", {}))
        retry_state = JobRuntimeState.from_dict(job_snapshot.get("runtime"))
        if retry_error:
            retry_state.record_retry_error(retry_error)
        retry_job_snapshot["runtime"] = retry_state.to_dict()
        return retry_job_snapshot

    def _apply_success(
        self,
        uid: str,
        job_snapshot: dict[str, Any],
        result: ExecutionResult,
        *,
        started_monotonic: float | None = None,
        execution_id: str | None = None,
        attempt_no: int = 1,
        run_id: str | None = None,
        started_at: str | None = None,
        spawned_job_snapshots: Sequence[dict[str, Any]] = (),
        active_files: dict[str, FileSpec] | None = None,
    ) -> None:
        """成功分支：子任务去重规范化、successes 记录与 cursor 推进。"""
        duration = (time.monotonic() - started_monotonic) if started_monotonic is not None else 0.0
        logger.info(f"SUCCESS: {uid} (duration {duration:.2f}s)")

        files_dict = (
            {k: v.to_dict() for k, v in active_files.items()}
            if active_files
            else job_snapshot.get("files")
        )
        effective_declarations: list[ArtifactDeclaration] = []
        effective_inputs: list[str] = []
        if active_files:
            for fname, fspec in active_files.items():
                if fspec.role == "output" and fspec.path is not None:
                    effective_declarations.append(
                        ArtifactDeclaration(fspec.path, name=fname)
                    )
                elif fspec.role == "input" and fspec.path is not None:
                    effective_inputs.append(fspec.path)

        self._store.apply_success(
            uid,
            spawned_job_snapshots=spawned_job_snapshots,
            cursor_updates=result.cursor_updates,
            declared_inputs=effective_inputs,
            declared_artifacts=effective_declarations,
            run_id=run_id or self._session.run_id,
            job_snapshot=job_snapshot,
            execution_id=execution_id,
            attempt_no=attempt_no,
            started_at=started_at,
            files=files_dict,
            result={
                "message": result.message,
                "exception": None,
                "metadata": dict(result.result_metadata or {}),
                "annotations": dict(result.annotations or {}),
            },
        )
        result.will_retry = False
        self._clear_retry_candidate(uid)

    def _collect_spawned_job_snapshots(
        self,
        parent_uid: str,
        spawned_jobs: Sequence[Job],
        *,
        entry: InFlightJob | None = None,
    ) -> list[dict[str, Any]]:
        """动态子任务声明顺序校验、去重与规范化。"""
        store = self._store
        spawned_job_snapshots: list[dict[str, Any]] = []
        if not spawned_jobs:
            return spawned_job_snapshots

        # 依赖引用校验：只能引用派发时已知的 Job、当前父 Job，以及本次先前声明的 Job
        if entry is not None:
            allowed_uids: set[str] = set(entry.known_job_uids)
        elif store.state is not None:
            allowed_uids = (
                set(store.state.queue_uids)
                | set(store.state.in_flight_uids)
                | set(store.state.success_uids)
                | set(store.state.failed_uids)
            )
        else:
            allowed_uids = set()
        allowed_uids.add(parent_uid)

        for spawned_job in spawned_jobs:
            for dep in spawned_job.depends_on:
                if dep not in allowed_uids:
                    raise ValueError(
                        f"Spawned job {spawned_job.uid!r} references non-existent, future, "
                        f"or cross-worker dependency {dep!r}"
                    )
            allowed_uids.add(spawned_job.uid)

        busy_snapshots = {
            Job.from_dict(snapshot).uid: snapshot for snapshot in store.state.queue
        }
        busy_snapshots.update({
            uid: active.job_snapshot for uid, active in self._in_flight.items()
        })
        accepted: dict[str, dict[str, Any]] = {}
        for spawned_job in spawned_jobs:
            spawned_uid = spawned_job.uid
            snapshot = store.normalize_spawned_job(spawned_job)
            previous = accepted.get(spawned_uid, busy_snapshots.get(spawned_uid))
            if previous is not None:
                if extract_effective_job_config(previous) != extract_effective_job_config(snapshot):
                    raise TaskLiteError(
                        f"Spawned job {spawned_uid!r} conflicts with existing effective configuration"
                    )
                continue
            accepted[spawned_uid] = snapshot
            spawned_job_snapshots.append(snapshot)
        logger.debug(f"Spawned {len(spawned_job_snapshots)} jobs for {parent_uid}.")
        return spawned_job_snapshots

    def _apply_failure(
        self,
        uid: str,
        job_snapshot: dict[str, Any],
        result: ExecutionResult,
        *,
        started_monotonic: float | None = None,
        execution_id: str | None = None,
        attempt_no: int | None = None,
        run_id: str | None = None,
        started_at: str | None = None,
        failure_record_override: dict[str, Any] | None = None,
        active_files: dict[str, FileSpec] | None = None,
    ) -> None:
        """永久失败分支：写入失败档案与级联阻断下游（不污染 result.result_metadata）。"""
        duration = (time.monotonic() - started_monotonic) if started_monotonic is not None else 0.0
        logger.error(
            f"FAIL: {uid} (duration {duration:.2f}s, sent to failure archive). "
            f"Message: {result.message}, Meta: {result.result_metadata}"
        )
        failure_record: dict[str, Any] = copy.deepcopy(failure_record_override) if failure_record_override is not None else {}
        if failure_record_override is None:
            if result.message is not None:
                failure_record["error_message"] = result.message

        files_dict = (
            {k: v.to_dict() for k, v in active_files.items()}
            if active_files
            else job_snapshot.get("files")
        )

        self._store.apply_failure(
            uid,
            failure_record,
            job_snapshot=job_snapshot,
            cascade=True,
            execution_id=execution_id,
            attempt_no=attempt_no,
            run_id=run_id or self._session.run_id,
            started_at=started_at,
            files=files_dict,
            result={
                "message": result.message,
                "exception": result.exception,
                "metadata": dict(result.result_metadata or {}),
                "annotations": dict(result.annotations or {}),
            },
        )
        result.will_retry = False
        self._clear_retry_candidate(uid)

    # ── 批量结算 ──────────────────────────────────────────────────────

    def settle_reaped(
        self, reaped: Sequence[tuple[JobHandle | InFlightJob, ExecutionResult]]
    ) -> int:
        """统一结算通道结果与未启动 worker 的执行失败。

        无 worker 的条目直接携带执行身份，不制造句柄或启动超时计时。

        为每个已完成作业原子执行：
        1. 提取在飞条目；
        2. complete_job 事务提交、释放资源租约、清理产物、触发收尾钩子；
        3. 确保在 finally 中从 InFlightTracker 与 StateStore 中注销；
        返回成功结算的作业数。
        """
        store = self._store
        settled_count = 0
        completion_error: BaseException | None = None
        pending_results = list(self._pending_cleanup.values())
        candidates = [*pending_results, *((h, r, None) for h, r in reaped)]
        for source, result, prepared in candidates:
            entry = self._in_flight.get(source.job_uid)
            if entry is None:
                continue
            handle = entry.handle
            deferred = False
            completion_started = False
            try:
                if prepared is None:
                    prepared = self._prepare_completion(entry.job_uid, entry.job, result, entry)
                if result.authorization_outcome is None:
                    try:
                        self._authorize_cleanup(entry, result, prepared.active_files)
                    except _CleanupAuthorizationPending:
                        self._pending_cleanup[entry.job_uid] = (entry, result, prepared)
                        if handle is not None:
                            handle.pause_timeout()
                        deferred = True
                        continue
                elif handle is not None and entry.job_uid in self._pending_cleanup:
                    try:
                        if handle.process.is_alive():
                            deferred = True
                            continue
                    except ValueError:
                        # 有效结果交付后，非阻塞回收可能已关闭进程句柄。
                        pass
                completion_started = True
                self.complete_job(entry, result, prepared=prepared)
                settled_count += 1
            except BaseException as error:
                if not completion_started:
                    entry.release_resources(self._resources)
                completion_error = completion_error or error
            finally:
                if not deferred:
                    self._pending_cleanup.pop(entry.job_uid, None)
                    self._in_flight.settle(entry.job_uid, state=store.state)
        if completion_error is not None:
            raise completion_error
        return settled_count

    def settle_aborted(
        self,
        cancelled_entries: Sequence[InFlightJob],
        done_entries: Sequence[tuple[InFlightJob, ExecutionResult]],
    ) -> None:
        """异常退出只释放内存归属，未提交的 running 留待下次恢复。"""
        cleanup_error: BaseException | None = None
        entries = [*cancelled_entries, *(entry for entry, _ in done_entries)]
        for entry in entries:
            try:
                entry.release_resources(self._resources)
            except BaseException as error:
                cleanup_error = cleanup_error or error
            finally:
                self._in_flight.settle(entry.job_uid, state=self._store.state)
        self._pending_cleanup.clear()
        if cleanup_error is not None:
            raise cleanup_error


__all__ = [
    "CompletionMachine",
]
