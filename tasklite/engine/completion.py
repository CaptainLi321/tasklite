"""v2 完成机器：结果提交、输出清理、资源释放与在飞结算。

``complete_job`` 是已进入 running 的执行结果的唯一收尾入口，``apply_result`` 承载
retry/success/failure 三态事务提交。重试预算只统计本 Run 中显式
``TaskResult.retry`` 的消耗；候选来源与累计数量分开登记。

收尾清理期授权等待由 ``AuthorizationCoordinator`` 统一承接，本模块不自建
暂挂队列或控制流异常，亦不穿透访问底层进程句柄。
"""

from __future__ import annotations

import copy
import logging
import time
import traceback
from dataclasses import dataclass
from typing import Any, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from .channel import ExecutionChannel
    from .resource import ResourceManager
    from .session import RunSession
    from .store import StateStore

from ..exceptions import TaskLiteError, _ArtifactIOError
from ..models.artifact import FileSpec
from ..models.job import Job
from ..models.records import build_result_record, extract_effective_job_config
from .authorization import AuthorizationCoordinator, CleanupAuthorizationDecision
from .channel import ExecutionResult, JobHandle
from .in_flight import InFlightJob, InFlightTracker
from .types import AuthorizationStatus

logger = logging.getLogger("tasklite")
_CLEANUP_ERRORS = (OSError, ValueError, NotImplementedError)


@dataclass
class _PreparedCompletion:
    active_files: dict[str, FileSpec]
    spawned_job_snapshots: list[dict[str, Any]]


class CompletionMachine:
    """结果提交 / 输出清理 / 资源释放 / 在飞结算的完成侧机器。"""

    def __init__(
        self,
        *,
        store: "StateStore",
        channel: "ExecutionChannel",
        resources: "ResourceManager",
        in_flight: "InFlightTracker",
        session: "RunSession",
        authorization: AuthorizationCoordinator | None = None,
    ) -> None:
        self._store = store
        self._channel = channel
        self._resources = resources
        self._in_flight = in_flight
        self._session = session
        self._authorization = (
            authorization
            if authorization is not None
            else AuthorizationCoordinator(
                store=store,
                channel=channel,
                in_flight=in_flight,
                session=session,
            )
        )

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

    def complete_job(
        self,
        entry: InFlightJob,
        result: ExecutionResult,
        *,
        prepared: _PreparedCompletion | None = None,
    ) -> None:
        """处理一个在飞作业的完成结果（唯一收尾出口）。

        事务性提交与内存 apply 见 ``apply_result``（两条提交路径共用）；
        本方法在提交成功后注销在途条目并清除授权状态，资源释放在 ``apply_result``
        内部恰好执行一次（提交前或异常兜底）。

        身份非真空：在飞注销下推到 ``apply_result`` 内部分支（retry 在重入队前、
        success 在 mark_success 后、failure 在 mark_failed 后），保证
        uid 在 ``is_known`` 可达的代码段内始终属于至少一个集合。
        """
        uid = entry.job_uid
        exec_id = (
            entry.execution_id
            if entry.execution_id is not None
            else (entry.handle.execution_id if entry.handle is not None else None)
        )
        settled = self.apply_result(
            uid,
            entry.job,
            entry.job_snapshot,
            result,
            started_monotonic=entry.started_monotonic,
            expect_in_flight=(entry.handle is not None),
            execution_id=exec_id,
            attempt_no=entry.attempt_no,
            run_id=entry.run_id or self._session.run_id,
            started_at=entry.started_at,
            entry=entry,
            prepared=prepared,
        )
        if settled:
            self._in_flight.settle(uid, state=self._store.state)
            self._authorization.clear_execution(exec_id, job_uid=uid)

    def _prepare_completion(
        self,
        uid: str,
        job: Job,
        result: ExecutionResult,
        entry: InFlightJob | None,
    ) -> _PreparedCompletion:
        store = self._store
        active_files: dict[str, FileSpec] = {}
        if entry is not None and getattr(entry, "active_files", None):
            active_files = dict(entry.active_files)
        elif job.files:
            active_files = {
                k: v._resolve_against(store.base_dir) if not v.is_output_ref else v
                for k, v in job.files.items()
            }

        if result.authorization_outcome is not None:
            return _PreparedCompletion(active_files, [])
        if result.artifact_error is not None:
            raise _ArtifactIOError(result.artifact_error)
        if result.success:
            policy = self._store.path_policy
            for fname, spec in active_files.items():
                if spec.role != "output":
                    continue
                if spec.path is None:
                    result.success = False
                    result.message = f"Missing output path for {fname!r}"
                    break
                try:
                    present = policy.inspect(
                        spec.path, mode="strict", verify_unchanged=True
                    ).exists
                except _CLEANUP_ERRORS as exc:
                    result.success = False
                    result.message = f"Output verification failed: {exc}"
                    self._attach_cleanup_failure(result, exc)
                    break
                if not present:
                    # strict inspect 已执行根、受保护路径与符号链接边界防护。若声明了成功删除，
                    # 契约允许工作进程在返回前原子 rename 移走该临时输出，缺失视为已移除。
                    if not spec.delete_on_success:
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
    ) -> bool:
        """事务性提交一个执行结果到后端并同步内存；若清理授权仍待决返回 False。

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

        released = False
        try:
            if prepared is None:
                prepared = self._prepare_completion(uid, job, result, entry)
                decision = self._authorization.evaluate_cleanup(
                    entry, result, prepared.active_files
                )
                if decision is CleanupAuthorizationDecision.PENDING:
                    if entry is not None:
                        self._authorization.defer_cleanup(entry, result, prepared)
                    return False
            active_files = prepared.active_files

            authorization_outcome = result.authorization_outcome
            if (
                authorization_outcome is not None
                and authorization_outcome.status
                in (AuthorizationStatus.DENIED, AuthorizationStatus.STOPPED)
            ):
                # 不变式：资源释放先于 running 回退事务，Release 回调仍须观察 running。
                if entry is not None:
                    released = True
                    entry.release_resources(self._resources)
                self._store.apply_pending(
                    job_uid=uid,
                    job_snapshot=job_snapshot,
                    attempt_no=attempt_no,
                    execution_id=execution_id,
                    run_id=run_id or self._session.run_id,
                    started_at=started_at,
                    error_message=authorization_outcome.message,
                    files={k: v.to_dict() for k, v in active_files.items()},
                    result=build_result_record(
                        message=authorization_outcome.message,
                        annotations={
                            "authorization_status": authorization_outcome.status.value,
                            "path": authorization_outcome.path,
                        },
                    ),
                )
                if authorization_outcome.status is AuthorizationStatus.DENIED:
                    self._session.temporarily_exclude_chain(uid, store.state)
                result.will_retry = False
                return True

            spawned_job_snapshots = prepared.spawned_job_snapshots

            # 声明式产物主进程清理
            if active_files:
                policy = self._store.path_policy
                authorize_cb = self._store.backend.is_path_authorized
                if result.success:
                    for _fname, spec in active_files.items():
                        if not (spec.delete_on_success and spec.path):
                            continue
                        try:
                            policy.delete(spec.path, authorize=authorize_cb)
                        except _CLEANUP_ERRORS as exc:
                            result.success = False
                            result.message = f"Success cleanup failed: {exc}"
                            self._attach_cleanup_failure(result, exc)
                            break
                    if not result.success:
                        spawned_job_snapshots = []
                        result.cursor_updates = {}
                else:
                    for _fname, spec in active_files.items():
                        if not (spec.delete_on_failure and spec.path):
                            continue
                        try:
                            policy.delete(spec.path, authorize=authorize_cb)
                        except _CLEANUP_ERRORS as exc:
                            self._record_failure_cleanup_error(result, exc)

            # 释放资源（必须在事务提交之前完成）
            if entry is not None:
                released = True
                entry.release_resources(self._resources)
        except BaseException:
            if entry is not None and not released:
                entry.release_resources(self._resources)
            raise

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
        return True

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
        新执行编号仅在下次进入 running 时分配。
        """
        consume_budget = result.consume_budget
        retry_error = result.message
        max_retries = job.max_retries_per_run if job.max_retries_per_run is not None else 3

        if consume_budget:
            consumed = self._session.get_retry_count(uid)
            if consumed >= max_retries:
                logger.error(
                    f"FAIL: {uid} exceeded max retries ({max_retries}). "
                    f"Sent to failure archive."
                )
                failure_metadata: dict[str, Any] = {
                    "error_message": "Retry budget exhausted",
                }
                if result.message is not None:
                    failure_metadata["message"] = result.message
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
            self._session.record_retry(uid)

        retry_job_snapshot = copy.deepcopy(job_snapshot)
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
            result=build_result_record(
                message=result.message,
                exception=result.exception,
                metadata=result.result_metadata,
                annotations=result.annotations,
            ),
        )
        result.will_retry = True
        self._session.mark_retry_candidate(uid)

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
        effective_outputs: dict[str, FileSpec] = {}
        effective_inputs: list[str] = []
        if active_files:
            for fname, fspec in active_files.items():
                if fspec.role == "output" and fspec.path is not None:
                    effective_outputs[fname] = fspec
                elif fspec.role == "input" and fspec.path is not None:
                    effective_inputs.append(fspec.path)

        self._store.apply_success(
            uid,
            spawned_job_snapshots=spawned_job_snapshots,
            cursor_updates=result.cursor_updates,
            declared_inputs=effective_inputs,
            declared_artifacts=effective_outputs,
            run_id=run_id or self._session.run_id,
            job_snapshot=job_snapshot,
            execution_id=execution_id,
            attempt_no=attempt_no,
            started_at=started_at,
            files=files_dict,
            result=build_result_record(
                message=result.message,
                metadata=result.result_metadata,
                annotations=result.annotations,
            ),
        )
        result.will_retry = False
        self._session.clear_retry_candidate(uid)

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
            snapshot = store.normalize_new_job_snapshot(spawned_job, caller="spawn")
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
        failure_record: dict[str, Any] = (
            copy.deepcopy(failure_record_override)
            if failure_record_override is not None
            else {}
        )
        if failure_record_override is None and result.message is not None:
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
            result=build_result_record(
                message=result.message,
                exception=result.exception,
                metadata=result.result_metadata,
                annotations=result.annotations,
            ),
        )
        result.will_retry = False
        self._session.clear_retry_candidate(uid)

    # ── 批量结算与异常清扫 ────────────────────────────────────────────

    def settle_reaped(
        self,
        reaped: Sequence[tuple[JobHandle | InFlightJob, ExecutionResult]],
    ) -> int:
        """统一结算通道结果与未启动 worker 的执行失败。

        无 worker 的条目直接携带执行身份，不制造句柄或启动超时计时。
        清理授权待决或受阻进程待退出时统一交由 ``AuthorizationCoordinator`` 暂挂，
        待放行或进程退出后仅执行一次收尾提交。
        """
        settled_count = 0
        pending_results = [
            (item.entry, item.result, item.prepared)
            for item in self._authorization.pending_cleanups()
        ]
        candidates: list[tuple[JobHandle | InFlightJob, ExecutionResult, _PreparedCompletion | None]] = [
            *pending_results,
            *((h, r, None) for h, r in reaped),
        ]
        for source, result, prepared in candidates:
            entry = self._in_flight.get(source.job_uid)
            if entry is None:
                continue
            if prepared is None:
                prepared = self._prepare_completion(entry.job_uid, entry.job, result, entry)
            decision = self._authorization.evaluate_cleanup(
                entry, result, prepared.active_files
            )
            if decision is CleanupAuthorizationDecision.PENDING:
                self._authorization.defer_cleanup(entry, result, prepared)
                continue
            # 框架异常立即上抛，未提交条目保留归属，由异常回收统一处理。
            self.complete_job(entry, result, prepared=prepared)
            settled_count += 1
        return settled_count

    def discard_in_flight(self) -> None:
        """异常退出单口径清扫：统一释放资源、同步注销在途并清空授权队列，不补提交结果。"""
        cleanup_error: BaseException | None = None
        for entry in list(self._in_flight.values()):
            try:
                entry.release_resources(self._resources)
            except BaseException as error:
                cleanup_error = cleanup_error or error
            finally:
                self._in_flight.settle(entry.job_uid, state=self._store.state)
        self._authorization.clear_all()
        if cleanup_error is not None:
            raise cleanup_error


__all__ = [
    "CompletionMachine",
]
