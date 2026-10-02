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
wall meta 的 last_run_id 事实源；契约定义见 dispatch 模块）。
"""

from __future__ import annotations

import logging
import time
from typing import Any, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from .admission import RerunPolicy
    from .channel import ExecutionChannel
    from .dispatch import DispatchSession
    from .in_flight import InFlightTracker
    from .resource import ResourceManager
    from .store import StateStore

from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.job import Job, JobRuntimeState
from .channel import ArtifactCleanupMode, ExecutionResult, JobHandle
from .errorclass import ERR_MAX_RETRIES
from .in_flight import InFlightJob, InFlightTracker
from .resource import persist_resource_suspensions
from .types import AttemptFinish

logger = logging.getLogger("tasklite")


class CompletionMachine:
    """结果提交 / 输出清理 / 资源释放 / 在飞结算的完成侧机器。"""

    def __init__(
        self,
        *,
        store: "StateStore",
        rerun_policy: "RerunPolicy",
        channel: "ExecutionChannel",
        resources: "ResourceManager",
        in_flight: "InFlightTracker",
        session: "DispatchSession",
    ) -> None:
        self._store = store
        self._rerun_policy = rerun_policy
        self._channel = channel
        self._resources = resources
        self._in_flight = in_flight
        self._session = session

    # ── 唯一收尾出口 ──────────────────────────────────────────────────

    def complete_job(self, entry: InFlightJob, result: ExecutionResult) -> None:
        """处理一个在飞作业的完成结果（唯一收尾出口）。

        事务性提交与内存 apply 见 ``apply_result``（两条提交路径共用）；
        本方法额外负责在飞注销时机下推、资源释放、失败输出清理与收尾
        钩子恰好一次。

        身份非真空：在飞注销**不在本方法开头**统一执行——那会造成
        「四集合真空」窗口（uid 不在 wall/failed/queue/in-flight 任一
        集合），使 handler 自 spawn 同 uid 子任务时被 ``is_known`` 漏判。
        注销时机下推到 ``apply_result`` 内部分支（retry 在重入队前、
        success 在 mark_success 后、failure 在 mark_failed 后），保证
        uid 在 ``is_known`` 可达的代码段内始终属于至少一个集合。

        单一出口：IPC 文件生命周期集中在本方法 finally——正常路径（收割
        已删 result/signals）与恢复路径（伪 entry）统一走清理模式分派。
        """
        uid = entry.uid
        terminated = False
        try:
            self.apply_result(
                uid,
                entry.job,
                entry.job_dict,
                result,
                job_start=entry.job_start,
                expect_in_flight=(entry.handle is not None),
                attempt_id=entry.attempt_id,
            )
        except _JobTerminated:
            # commit 连续失败达阈值 → job 已失败档案终结（正常终态）。
            # store 的 3-strike 分支已触发钩子（带完整熔断 meta），此处
            # 标记终结并跳过尾部触发——否则同一 job 触发两次钩子（第二次
            # success 标志还与实际终态矛盾）。「每个 attempt 收尾钩子恰好
            # 调用一次」是收尾出口的明文承诺。
            result.going_to_retry = False
            terminated = True
        finally:
            # 资源释放（无论成功/失败/重试/崩溃，由 entry 自归还）
            entry.release_resources(self._resources)

            # 经 channel 深模块清理产物与 IPC 工件
            cleanup_mode = (
                ArtifactCleanupMode.SUCCESS if result.success
                else ArtifactCleanupMode.FAILURE_OR_RETRY
            )
            self._channel.cleanup_artifacts(uid, mode=cleanup_mode)

        # 收尾钩子在统计更新之后（apply_result 已 +1）触发——钩子内读统计
        # 一致；单一出口经 store.fire_attempt_finished（异常隔离 + 计数），
        # 正常提交与伪 entry 都经此触发。
        if not terminated:
            self._store.fire_attempt_finished(
                uid,
                outcome=AttemptFinish(
                    success=bool(result.success),
                    going_to_retry=result.going_to_retry,
                    meta=dict(result.result_meta),
                ),
            )

    def apply_result(
        self,
        uid: str,
        job: Job,
        job_dict: dict[str, Any],
        result: ExecutionResult,
        *,
        job_start: float | None = None,
        expect_in_flight: bool = True,
        attempt_id: int | None = None,
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

        # 1. 全局应用资源挂起
        applied_suspension = False
        for r_name, secs in result.resource_suspensions:
            if self._resources.suspend_resource(r_name, secs):
                applied_suspension = True
            else:
                logger.warning(
                    f"Skipping suspend for unregistered resource {r_name!r} "
                    f"(requested by {uid})"
                )
        if applied_suspension:
            persist_resource_suspensions(self._store.backend, self._resources)

        # 2. 状态分发
        if result.retry_requested:
            self._apply_retry(uid, job, job_dict, result, attempt_id=attempt_id)
        elif result.success:
            self._apply_success(
                uid, job_dict, result, job_start=job_start, attempt_id=attempt_id
            )
        else:
            self._apply_failure(
                uid, job_dict, result, job_start=job_start, attempt_id=attempt_id
            )

    # ── 三态分支 ──────────────────────────────────────────────────────

    def _apply_retry(
        self,
        uid: str,
        job: Job,
        job_dict: dict[str, Any],
        result: ExecutionResult,
        *,
        attempt_id: int | None = None,
    ) -> None:
        """重试分支：预算判定 → retry_dict 组装 → store.apply_retry。

        瞬态信号（TRANSIENT_KIND_STAT_KEYS 登记的三类）零预算：即使
        attempt_no 已超预算也豁免失败档案、attempt_no 不推进、
        last_retry_error 不污染；业务失败重试推进 attempt_no+1 并记录
        最近重试错误。节奏（何时/何处重入队）由 RequeuePolicy 唯一出口
        承担，本分支零节奏计算。
        """
        transient_kind = result.transient_kind
        is_transient = transient_kind is not None
        retry_error = result.retry_error

        if not is_transient and job.attempt_no > job.max_retries:
            logger.error(
                f"FAIL: {uid} exceeded max retries ({job.max_retries}). "
                f"Sent to failure archive."
            )
            fail_meta: dict[str, Any] = {"error": ERR_MAX_RETRIES}
            rt_state = JobRuntimeState.from_dict(job_dict.get("runtime"))
            if rt_state.last_retry_error:
                fail_meta["last_retry_error"] = rt_state.last_retry_error
            if retry_error:
                fail_meta["retry_error"] = retry_error
            self._store.apply_failure(
                uid, fail_meta, job_dict=job_dict, cascade=True,
                attempt_id=attempt_id,
            )
            result.going_to_retry = False
            return

        retry_dict = self._build_retry_dict(
            job, job_dict, retry_error=retry_error, is_transient=is_transient
        )
        next_attempt = job.attempt_no if is_transient else job.attempt_no + 1
        logger.info(f"RETRY: {uid} (attempt {next_attempt}/{job.max_retries + 1})")
        self._store.apply_retry(
            uid,
            job_dict,
            retry_dict,
            transient_kind=transient_kind,
            attempt_id=attempt_id,
            error=retry_error,
        )
        result.going_to_retry = True

    @staticmethod
    def _build_retry_dict(
        job: Job,
        job_dict: dict[str, Any],
        *,
        retry_error: str | None,
        is_transient: bool,
    ) -> dict[str, Any]:
        """重试字典组装：原作业原样重入队，受管键以 job 权威状态覆写。

        不变式（与 Job 实例位语义对齐）：瞬态信号零预算——attempt_no
        不推进、last_retry_error 不写；业务失败重试 attempt_no+1 并记录
        最近重试错误。job_dict 顶层自定义字段随重试往返保留；resources
        以原 job_dict 为准（保留已注入的 ``__workers__`` 槽位）；
        first_enqueued_at 原样保留（重试不刷新首次入队时间）。
        """
        retry_dict = dict(job_dict)
        if not is_transient:
            job.attempt_no = job.attempt_no + 1
        retry_dict.update(job.to_dict())
        retry_dict["resources"] = dict(job_dict.get("resources", {}))
        retry_state = JobRuntimeState.from_dict(job_dict.get("runtime"))
        if retry_error and not is_transient:
            retry_state.record_retry_error(retry_error)
        retry_dict["runtime"] = retry_state.to_dict()
        return retry_dict

    def _apply_success(
        self,
        uid: str,
        job_dict: dict[str, Any],
        result: ExecutionResult,
        *,
        job_start: float | None = None,
        attempt_id: int | None = None,
    ) -> None:
        """成功分支：子任务去重规范化、wall 记录与 cursor 推进。"""
        duration = (time.monotonic() - job_start) if job_start is not None else 0.0
        logger.info(f"SUCCESS: {uid} (duration {duration:.2f}s)")

        spawned_dicts = self._collect_spawned_dicts(uid, result.new_jobs)
        wall_meta = dict(result.result_meta or {})
        declared_inputs = self._channel.read_declared_inputs(uid)

        self._store.apply_success(
            uid,
            wall_meta,
            spawned_jobs=spawned_dicts,
            cursor_updates=result.cursor_updates,
            declared_inputs=declared_inputs,
            run_id=self._session.run_id,
            job_dict=job_dict,
            attempt_id=attempt_id,
        )
        result.going_to_retry = False

    def _collect_spawned_dicts(
        self, parent_uid: str, new_jobs: Sequence[Job]
    ) -> list[dict[str, Any]]:
        """动态子任务去重与规范化。

        去重规则：批内同 uid 只留首个；queue/in-flight 命中无条件拦截
        （同轮不重复派发/并发双跑，优先于 rerun 豁免）；wall/failed 命中
        经 RerunPolicy 准入判定。规范化经 store.normalize_spawned_job
        （策略注入 + 资源注入 + 首入队时间 + 序列化预检）；不可序列化的
        坏子作业独立登记失败终态（拒绝语义），不连坐父作业的成功提交、
        不触发 commit 崩溃契约。
        """
        store = self._store
        spawned_dicts: list[dict[str, Any]] = []
        if not new_jobs:
            return spawned_dicts

        seen_in_batch: set[str] = set()
        unique_new_jobs: list[Job] = []
        for nj in new_jobs:
            nj_uid = nj.uid
            if nj_uid in seen_in_batch:
                logger.debug(f"Skipping duplicate spawn for {nj_uid}.")
                continue
            if store.state.is_known(nj_uid):
                if nj_uid in store.state.queue_uids or nj_uid in store.state.in_flight_uids:
                    continue
                decision = self._rerun_policy.admit(nj.to_dict(), store.state)
                if decision.should_skip:
                    continue
            seen_in_batch.add(nj_uid)
            unique_new_jobs.append(nj)

        for nj in unique_new_jobs:
            try:
                spawned_dicts.append(store.normalize_spawned_job(nj))
            except ValueError as err:
                self._reject_spawned_job(nj, err)
        logger.debug(f"Spawned {len(spawned_dicts)} jobs for {parent_uid}.")
        return spawned_dicts

    def _reject_spawned_job(self, job: Job, err: Exception) -> None:
        """坏子作业（不可 JSON 序列化）经 ``complete_job`` 单一出口终结。

        伪 entry（无资源租约、无进程、未派发故无 IPC 产物与轨迹行）承载
        派发拒绝语义的失败结果，与崩溃残留恢复共用完整收尾契约：失败
        终态登记（``apply_failure`` 收敛、级联下游）与收尾钩子恰好一次。
        ``_CommitCrashSignal``（后端环境故障）穿透上抛，交由崩溃契约处理。
        """
        logger.error(
            f"Spawned job {job.uid} rejected: not JSON-serializable: {err}"
        )
        entry = InFlightTracker.create_pseudo_entry(job.uid, job.to_dict(), job)
        self.complete_job(
            entry,
            ExecutionResult(
                success=False,
                result_meta={
                    "error": f"INVALID_SPAWNED_JOB: job dict is not "
                    f"JSON-serializable: {err}"
                },
            ),
        )

    def _apply_failure(
        self,
        uid: str,
        job_dict: dict[str, Any],
        result: ExecutionResult,
        *,
        job_start: float | None = None,
        attempt_id: int | None = None,
    ) -> None:
        """永久失败分支：写入失败档案与级联阻断下游。"""
        duration = (time.monotonic() - job_start) if job_start is not None else 0.0
        logger.error(
            f"FAIL: {uid} (duration {duration:.2f}s, sent to failure archive). "
            f"Meta: {result.result_meta}"
        )
        self._store.apply_failure(
            uid, result.result_meta, job_dict=job_dict, cascade=True,
            attempt_id=attempt_id,
        )
        result.going_to_retry = False

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
        completed_count = 0
        for handle, result in reaped:
            entry = self._in_flight.get(handle.uid)
            if entry is None:
                continue
            try:
                self.complete_job(entry, result)
                completed_count += 1
            finally:
                self._in_flight.settle(handle.uid, state=store.state)
        return completed_count

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
        # 1. 未完成任务注销并重入队
        for pending_entry in cancelled_entries:
            self._in_flight.settle(pending_entry.uid, state=store.state)
        job_dicts = [entry.job_dict for entry in cancelled_entries]
        if job_dicts:
            store.state.requeue_jobs(job_dicts, front=True)

        # 2. 已完成任务提交
        commit_crash: BaseException | None = None
        for entry, result in done_entries:
            try:
                self.complete_job(entry, result)
            except _JobTerminated:
                pass
            except _CommitCrashSignal as e:
                commit_crash = e
            finally:
                self._in_flight.settle(entry.uid, state=store.state)

        self._in_flight.clear()
        store.state.clear_in_flight()
        if commit_crash is not None:
            raise commit_crash


__all__ = [
    "CompletionMachine",
]
