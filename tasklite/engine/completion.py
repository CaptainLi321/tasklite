"""v2 完成机器：结果提交、输出清理、资源释放与在飞结算。

``complete_job`` 是子进程结果的唯一收尾入口（成功/失败/重试全经此）；
``apply_result`` 承载 retry/success/failure 三态事务提交。崩溃残留认领
（stale-restore）归恢复编排层，经伪 entry 回流到本机器的同一收尾契约。
依赖以显式窄清单注入（无共享袋），经 ``store`` 复用状态仓库事务深模块
（3-strike/级联/轨迹收尾），不反向引用门面。

重试预算判定在 ``_apply_retry``：允许执行条件 ``attempt_no <=
max_retries + 1``，业务失败重试推进 ``attempt_no + 1``；瞬态信号
（interrupted / lock_conflict / rate_limited）零预算——attempt_no 不推进、
last_retry_error 不污染、预算耗尽也豁免失败档案。重试节奏唯一经
StateStore.apply_retry 内的 RequeuePolicy（默认立即队首重入）。

``session`` 按具名 Protocol ``DispatchSession`` 消费（``run_id`` 是
successes meta 的 last_run_id 事实源；契约定义见 dispatch 模块）。
"""

from __future__ import annotations

import copy
import logging
import os
import time
from typing import Any, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from .admission import TriggerRegistry
    from .channel import ExecutionChannel
    from .dispatch import DispatchSession
    from .in_flight import InFlightTracker
    from .resource import ResourceManager
    from .store import StateStore

from ..backend.sqlite_backend import TARGET_SCHEMA_VERSION
from ..exceptions import TaskLiteError, _ArtifactIOError, _CommitCrashSignal, _JobTerminated
from ..models.job import Job, JobRuntimeState
from ..models.artifact import ArtifactDeclaration, FileSpec
from ..utils.artifact import ArtifactPathPolicy
from ..utils.clock import utc_now_iso
from .admission import extract_effective_job_config
from .channel import ArtifactCleanupMode, ExecutionResult, JobHandle
from .errorclass import ERR_MAX_RETRIES
from .in_flight import InFlightJob, InFlightTracker
from .types import AttemptOutcome

logger = logging.getLogger("tasklite")


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
        self._fallback_retry_counts: dict[str, int] = {}

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

    # ── 唯一收尾出口 ──────────────────────────────────────────────────

    def complete_job(self, entry: InFlightJob, result: ExecutionResult) -> None:
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
        terminated = False
        try:
            self.apply_result(
                uid,
                entry.job,
                entry.job_snapshot,
                result,
                started_monotonic=entry.started_monotonic,
                expect_in_flight=(entry.handle is not None),
                attempt_id=entry.attempt_id,
                execution_id=entry.execution_id if entry.execution_id is not None else (
                    entry.handle.execution_id if entry.handle is not None else None
                ),
                attempt_no=entry.attempt_no,
                run_id=entry.run_id or getattr(self._session, "run_id", "run"),
                started_at=entry.started_at,
                entry=entry,
            )
        except _JobTerminated:
            # commit 连续失败达阈值 → job 已失败档案终结（正常终态）。
            # store 的 3-strike 分支已触发钩子（带完整熔断 meta），此处
            # 标记终结并跳过尾部触发——否则同一 job 触发两次钩子（第二次
            # success 标志还与实际终态矛盾）。「每个 attempt 收尾钩子恰好
            # 调用一次」是收尾出口的明文承诺。
            result.will_retry = False
            terminated = True
        finally:
            # 资源释放（无论成功/失败/重试/崩溃，由 entry 自归还）
            entry.release_resources(self._resources)

        # 收尾钩子在统计更新之后（apply_result 已 +1）触发——钩子内读统计
        # 一致；单一出口经 store.fire_attempt_finished（异常隔离 + 计数），
        # 正常提交与伪 entry 都经此触发。
        if not terminated:
            self._store.fire_attempt_finished(
                uid,
                outcome=AttemptOutcome(
                    success=bool(result.success),
                    will_retry=result.will_retry,
                    metadata=dict(result.result_metadata),
                ),
            )
        self._channel.journal.clear_declarations(uid)

    def apply_result(
        self,
        uid: str,
        job: Job,
        job_snapshot: dict[str, Any],
        result: ExecutionResult,
        *,
        started_monotonic: float | None = None,
        expect_in_flight: bool = True,
        attempt_id: int | None = None,
        execution_id: str | None = None,
        attempt_no: int = 1,
        run_id: str | None = None,
        started_at: str | None = None,
        entry: InFlightJob | None = None,
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

        active_files: dict[str, FileSpec] = {}
        if entry is not None and getattr(entry, "active_files", None):
            active_files = dict(entry.active_files)
        elif job.files:
            active_files = {
                k: v.resolve_against(store.base_dir) if not v.is_output_ref else v
                for k, v in job.files.items()
            }

        if result.transient_kind == "authorization_denied":
            self._store.apply_pending(
                job_uid=uid,
                job_snapshot=job_snapshot,
                attempt_no=attempt_no,
                run_id=run_id or getattr(self._session, "run_id", "run"),
                started_at=started_at,
                error_message="authorization denied",
                files={k: v.to_dict() for k, v in active_files.items()},
            )
            if entry is not None:
                entry.release_resources(self._resources)
            exclude_fn = getattr(self._session, "temporarily_exclude_chain", None)
            if callable(exclude_fn):
                exclude_fn(uid)
            return

        if result.artifact_error is not None:
            raise _ArtifactIOError(result.artifact_error)
        declarations = self._channel.read_declared_artifacts(uid)
        inputs = self._channel.read_declared_inputs(uid) if result.success else ()
        if result.success:
            missing_output = self._channel.journal.verify_artifacts(declarations)
            if missing_output is not None:
                result.success = False
                result.message = missing_output

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

        self._channel.journal.cleanup_declared_artifacts(declarations, success=result.success)

        # 声明式产物主进程清理
        if active_files:
            policy = ArtifactPathPolicy(
                output_roots=getattr(self._channel, "output_roots", None),
                protected_roots=self._channel.protected_roots,
            )
            if result.success:
                for fname, spec in active_files.items():
                    if spec.delete_on_success and spec.path and os.path.exists(spec.path):
                        if not store.check_or_request_authorization(spec.path):
                            result.success = False
                            result.message = f"Success cleanup authorization denied: {spec.path}"
                            break
                        try:
                            policy.cleanup(
                                ArtifactDeclaration(spec.path, delete_on_success=True),
                                success=True,
                            )
                        except OSError as exc:
                            result.success = False
                            result.message = f"Success cleanup failed: {exc}"
                            break
                if not result.success:
                    spawned_job_snapshots = []
                    result.cursor_updates = {}
            else:
                for fname, spec in active_files.items():
                    if spec.delete_on_failure and spec.path and os.path.exists(spec.path):
                        if store.check_or_request_authorization(spec.path):
                            try:
                                policy.cleanup(
                                    ArtifactDeclaration(spec.path, delete_on_failure=True),
                                    success=False,
                                )
                            except OSError as exc:
                                if result.retry_requested:
                                    result.retry_requested = False
                                    result.message = f"Retry cleanup failed: {exc}"
                                else:
                                    result.annotations["cleanup_error"] = str(exc)
                                    if result.message:
                                        result.message = f"{result.message} (cleanup failed: {exc})"
                                    else:
                                        result.message = f"Cleanup failed: {exc}"

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
                attempt_id=attempt_id,
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
                attempt_id=attempt_id,
                execution_id=execution_id,
                declared_artifacts=declarations,
                declared_inputs=inputs,
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
                attempt_id=attempt_id,
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
        attempt_id: int | None = None,
        attempt_no: int = 1,
        run_id: str | None = None,
        started_at: str | None = None,
        active_files: dict[str, FileSpec] | None = None,
    ) -> None:
        """重试分支：预算判定 → retry_job_snapshot 组装 → store.apply_retry。

        重试预算按单次 Run、按 uid 独立计数（不与历史 attempt_no 挂钩）：
        - ``consume_budget=True`` 的业务重试检查本 Run 已消耗预算，达到
          ``max_retries_per_run`` 即转入失败终态，否则记账 +1 并推进
          ``attempt_no + 1``；
        - ``consume_budget=False`` 的显式重试不烧本 Run 重试预算，仍推进
          ``attempt_no + 1`` 并记录 Attempt；
        - 内部瞬态信号（``transient_kind`` 非 None）零预算且不推进 ``attempt_no``。
        """
        transient_kind = result.transient_kind
        is_transient = transient_kind is not None
        consume_budget = bool(result.consume_budget) and not is_transient
        retry_error = result.message if result.message is not None else result.retry_error
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
                    "metadata": copy.deepcopy(result.result_metadata),
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
                    attempt_id=attempt_id,
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
            job, job_snapshot, retry_error=retry_error, is_transient=is_transient
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
            transient_kind=transient_kind,
            attempt_id=attempt_id,
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

    @staticmethod
    def _build_retry_job_snapshot(
        job: Job,
        job_snapshot: dict[str, Any],
        *,
        retry_error: str | None,
        is_transient: bool,
    ) -> dict[str, Any]:
        """重试字典组装：原作业原样重入队，受管键以 job 权威状态覆写。

        不变式（与 Job 实例位语义对齐）：瞬态信号零预算——attempt_no
        不推进、last_retry_error 不写；业务失败重试 attempt_no+1 并记录
        最近重试错误。job_snapshot 顶层自定义字段随重试往返保留；resources
        以原 job_snapshot 为准（保留已注入的 ``__workers__`` 槽位）；
        first_enqueued_at 原样保留（重试不刷新首次入队时间）。
        """
        retry_job_snapshot = dict(job_snapshot)
        if not is_transient:
            job.attempt_no = job.attempt_no + 1
        retry_job_snapshot.update(job.to_dict())
        retry_job_snapshot["resources"] = dict(job_snapshot.get("resources", {}))
        retry_state = JobRuntimeState.from_dict(job_snapshot.get("runtime"))
        if retry_error and not is_transient:
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
        attempt_id: int | None = None,
        execution_id: str | None = None,
        declared_artifacts: Sequence[ArtifactDeclaration] = (),
        declared_inputs: Sequence[str] = (),
        attempt_no: int = 1,
        run_id: str | None = None,
        started_at: str | None = None,
        spawned_job_snapshots: Sequence[dict[str, Any]] = (),
        active_files: dict[str, FileSpec] | None = None,
    ) -> None:
        """成功分支：子任务去重规范化、successes 记录与 cursor 推进。"""
        duration = (time.monotonic() - started_monotonic) if started_monotonic is not None else 0.0
        logger.info(f"SUCCESS: {uid} (duration {duration:.2f}s)")

        success_metadata = dict(result.result_metadata or {})
        files_dict = (
            {k: v.to_dict() for k, v in active_files.items()}
            if active_files
            else job_snapshot.get("files")
        )
        effective_declarations = list(declared_artifacts)
        effective_inputs = list(declared_inputs)
        if active_files:
            for fname, fspec in active_files.items():
                if fspec.role == "output" and fspec.path is not None:
                    effective_declarations.append(ArtifactDeclaration(fspec.path, name=fname))
                elif fspec.role == "input" and fspec.path is not None:
                    effective_inputs.append(fspec.path)

        self._store.apply_success(
            uid,
            success_metadata,
            spawned_job_snapshots=spawned_job_snapshots,
            cursor_updates=result.cursor_updates,
            declared_inputs=effective_inputs,
            declared_artifacts=effective_declarations,
            run_id=run_id or self._session.run_id,
            job_snapshot=job_snapshot,
            attempt_id=attempt_id,
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
        if entry is not None and entry.known_job_uids:
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
                effective_previous = store.normalize_spawned_job(Job.from_dict(previous))
                if extract_effective_job_config(effective_previous) != extract_effective_job_config(snapshot):
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
        attempt_id: int | None = None,
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
        failure_record: dict[str, Any] = failure_record_override if failure_record_override is not None else copy.deepcopy(result.result_metadata)
        if failure_record_override is None:
            if result.message is not None and "error_message" not in failure_record:
                failure_record["error_message"] = result.message
            if result.message is not None and "message" not in failure_record:
                failure_record["message"] = result.message
            if result.exception is not None and "exception" not in failure_record:
                failure_record["exception"] = copy.deepcopy(result.exception)
            if "metadata" not in failure_record:
                failure_record["metadata"] = copy.deepcopy(result.result_metadata)

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
            attempt_id=attempt_id,
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

    # ── 批量结算 ──────────────────────────────────────────────────────

    def settle_reaped(
        self, reaped: Sequence[tuple[JobHandle, ExecutionResult]]
    ) -> int:
        """统一结算从执行通道收割的已完成作业列表。

        为每个已完成作业原子执行：
        1. 提取在飞条目；
        2. complete_job 事务提交、释放资源租约、清理产物、触发收尾钩子；
        3. 确保在 finally 中从 InFlightTracker 与 StateStore 中注销；
        返回成功结算的作业数。
        """
        store = self._store
        settled_count = 0
        completion_error: BaseException | None = None
        for handle, result in reaped:
            entry = self._in_flight.get(handle.job_uid)
            if entry is None:
                continue
            try:
                self.complete_job(entry, result)
                settled_count += 1
            except BaseException as error:
                completion_error = completion_error or error
            finally:
                self._requeue_uncommitted(entry)
                self._in_flight.settle(handle.job_uid, state=store.state)
        if completion_error is not None:
            raise completion_error
        return settled_count

    def settle_aborted(
        self,
        cancelled_entries: Sequence[InFlightJob],
        done_entries: Sequence[tuple[InFlightJob, ExecutionResult]],
    ) -> None:
        """统一结算异常或停机时分类的在飞作业。

        1. 未完成任务：注销在飞并回滚重入队到队首；
        2. 已完成任务：通过 complete_job 事务提交（不重跑、不误杀）；
        3. 彻底清空在飞集合并透传可能的 commit 崩溃信号。
        """
        store = self._store
        cleanup_error: BaseException | None = None
        for entry in cancelled_entries:
            try:
                self._channel.cleanup_artifacts(
                    entry.job_uid, mode=ArtifactCleanupMode.FAILURE_OR_RETRY,
                )
                policy = ArtifactPathPolicy(
                    output_roots=getattr(self._channel, "output_roots", None),
                    protected_roots=self._channel.protected_roots,
                )
                for spec in entry.active_files.values():
                    if (spec.delete_on_failure and spec.path
                            and store.check_or_request_authorization(spec.path) is True):
                        policy.cleanup(
                            ArtifactDeclaration(spec.path, delete_on_failure=True),
                            success=False,
                        )
            except BaseException as error:
                cleanup_error = cleanup_error or error
            finally:
                entry.release_resources(self._resources)
                self._requeue_uncommitted(entry)
                self._in_flight.settle(entry.job_uid, state=store.state)

        for entry, result in done_entries:
            try:
                self.complete_job(entry, result)
            except BaseException as error:
                cleanup_error = cleanup_error or error
            finally:
                self._requeue_uncommitted(entry)
                self._in_flight.settle(entry.job_uid, state=store.state)

        if cleanup_error is not None:
            raise cleanup_error

    def _requeue_uncommitted(self, entry: InFlightJob) -> None:
        """只回队尚未提交的原快照，已提交事实不能因尾部清理失败而重跑。"""
        if self._store.backend.schema_version >= TARGET_SCHEMA_VERSION:
            # 未提交的执行只保留持久化 running，不能回队自动重跑副作用。
            return
        state = self._store.state
        if entry.job_uid in state.in_flight_uids:
            state.unregister_in_flight(entry.job_uid)
            state.requeue_jobs([entry.job_snapshot], front=True)


__all__ = [
    "CompletionMachine",
]
