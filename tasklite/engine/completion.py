"""完成机器：结果提交、输出清理、资源释放与崩溃恢复族。

``complete_job`` 是子进程结果的唯一收尾入口；``apply_result`` 承载
retry/success/failure 三态事务提交；restore/cleanup/release 是崩溃恢复与
资源释放的共享助手。依赖经 RunContext 注入，经 ``self.store`` 复用
状态与事务深模块（3-strike/级联），不反向引用 TaskLite。
"""
from __future__ import annotations

import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import RunContext
    from .store import CommitView

from ..taxonomy import ERR_MAX_RETRIES as _ERR_MAX_RETRIES
from ..exceptions import _JobTerminated
from ..models.job import Job
from .channel import ArtifactCleanupMode, ExecutionResult
from .inflight import InFlightJob
from .runtime import inject_worker_resource


logger = logging.getLogger("tasklite")


class CompletionMachine:
    """结果提交 / 输出清理 / 资源释放 / 崩溃恢复的完成侧机器。"""

    def __init__(self, ctx: "RunContext") -> None:
        self._ctx = ctx

    def complete_job(self, entry: InFlightJob, result: ExecutionResult) -> None:
        """处理一个 in-flight job 的完成结果（薄包装）。

        事务性提交与内存 apply 见 ``_apply_result``（两条提交路径共用）；
        本方法额外负责 in-flight 注销、资源释放、失败输出清理。

        身份非真空：in-flight 注销**不在本方法开头**统一
        执行——那会造成「四集合真空」窗口（uid 不在 wall/failed/queue/
        in-flight 任一集合），使 handler 自 spawn 同 uid 子任务时被
        ``is_known`` 漏判。注销时机下推到 ``_apply_result`` 内部分支
        （retry 在 requeue 前、success 在 mark_success 后、failure 在
        mark_failed 后），保证 uid 在 ``is_known`` 可达的代码段内始终
        属于至少一个集合。

        单一出口：IPC 文件生命周期集中在本方法 finally——
        正常路径（drain 已删 result/signals）与恢复路径（``_restore_stale_result``
        伪 entry）统一走 ``cleanup_ipc_files``。
        """
        uid = entry.uid
        terminated = False
        try:
            self.apply_result(
                uid, entry.job, entry.job_dict, result, job_start=entry.job_start,
                expect_in_flight=(entry.handle is not None),
            )
        except _JobTerminated:
            # commit 连续失败达阈值 → job 已 DLQ 终结（正常终态）。
            # _apply_result 已把该 job 标记为 failed；不 re-raise（主循环
            # 继续处理队列中其余 job），finally 仍释放资源。
            result.going_to_retry = False  # DLQ 阈值终结，非重试
            # _commit_failed_crash 的 3-strike DLQ 分支
            # 已触发钩子（带完整 COMMIT_FAILURE_DLQ meta），此处标记终结，
            # 跳过尾部 _fire_job_completed——否则同一 job 触发两次钩子
            # （第二次 success 标志还与实际 DLQ 矛盾）。「每个 job 终结时
            # 钩子恰好调用一次」是 _fire_job_completed docstring 的明文承诺。
            terminated = True
        finally:
            # Release resources (无论成功/失败/重试/崩溃，由 entry 自归还)
            entry.release_resources(self._ctx.resource_mgr)

            # Cleanup outputs and IPC artifacts via channel deep module
            cleanup_mode = (
                ArtifactCleanupMode.SUCCESS if result.success
                else ArtifactCleanupMode.FAILURE_OR_RETRY
            )
            self._ctx.channel.cleanup_artifacts(uid, mode=cleanup_mode)

        # on_job_completed 在 stats 更新之后（_apply_result
        # 已 +1）调用——钩子内读 stats 保证一致。单一出口：正常提交与
        # restore 伪 entry 都经此触发（_dispatch_job 的「不走子进程」直接
        # commit 路径也调用同一 helper）。异常隔离：
        # 钩子抛异常 catch + 计数，绝不影响主循环。
        # 3-strike 终结路径（except _JobTerminated）
        # 的钩子已在 _commit_failed_crash 内触发过，此处跳过防双触发。
        if not terminated:
            self._ctx.fire_job_completed(
                uid, dict(result.result_meta),
                bool(result.success), bool(result.going_to_retry),
            )

    def apply_result(
        self,
        uid: str,
        job: Job,
        job_dict: dict,
        result: ExecutionResult,
        job_start: Optional[float] = None,
        expect_in_flight: bool = True,
    ) -> None:
        """事务性提交一个执行结果到后端并同步内存。

        先提交后端（唯一真相源），commit 成功后才 apply 到内存 state。
        三态分发：
          - retry_requested -> _apply_retry
          - success -> _apply_success
          - failure -> _apply_failure
        """
        store = self._ctx.store
        if expect_in_flight:
            assert uid in store.in_flight_uids, (
                f"identity vacuity violation: {uid} not in-flight at _apply_result entry"
            )

        # 1. 全局应用资源挂起
        applied_suspension = False
        for r_name, secs in result.resource_suspensions:
            if self._ctx.resource_mgr.suspend_resource(r_name, secs):
                applied_suspension = True
            else:
                logger.warning(
                    f"Skipping suspend for unregistered resource {r_name!r} "
                    f"(requested by {uid})"
                )
        if applied_suspension:
            self._ctx.persist_resource_suspends_now()

        # 2. 状态分发
        if result.retry_requested:
            self._apply_retry(uid, job, job_dict, result)
        elif result.success:
            self._apply_success(uid, job_dict, result, job_start)
        else:
            self._apply_failure(uid, job_dict, result, job_start)

    def _apply_retry(
        self, uid: str, job: Job, job_dict: dict, result: ExecutionResult
    ) -> None:
        """处理重试分支：委托策略深模块规划重试并同步存储与统计。"""
        plan = self._ctx.policy.plan_retry(job, job_dict, result)
        if not plan.going_to_retry:
            logger.error(f"FAIL: {uid} exceeded max retries ({job.max_retries}). Sent to DLQ.")
            outcome = self._ctx.store.apply_failure(
                uid, plan.fail_meta or {"error": _ERR_MAX_RETRIES}, job_dict=job_dict, cascade=True
            )
            self._ctx.stats["failed"] += 1
            if outcome.cascaded_uids:
                self._ctx.stats["cascade_failed"] += len(outcome.cascaded_uids)
            result.going_to_retry = False
            return

        if plan.is_interrupted:
            self._ctx.stats["interrupted_reruns"] += 1
        elif plan.is_lock_conflict:
            self._ctx.stats["deferred_orphan"] += 1

        logger.info(f"RETRY: {uid} (attempt {job.retries}/{job.max_retries}, backoff {plan.delay:.1f}s)")
        assert plan.retry_dict is not None
        self._ctx.store.apply_retry(uid, job_dict, plan.retry_dict, front=False)
        self._ctx.stats["retried"] += 1
        result.going_to_retry = True

    def _apply_success(
        self, uid: str, job_dict: dict, result: ExecutionResult,
        job_start: Optional[float] = None
    ) -> None:
        """处理成功分支：子任务去重、wall 记录与 cursor 推进。"""
        store = self._ctx.store
        duration = (time.monotonic() - job_start) if job_start is not None else 0.0
        logger.info(f"SUCCESS: {uid} (duration {duration:.2f}s)")

        # 动态子任务去重与规范化
        spawned_dicts: List[Dict[str, Any]] = []
        new_jobs = result.new_jobs
        if new_jobs:
            seen_in_batch = set()
            unique_new_jobs = []
            for nj in new_jobs:
                nj_uid = nj.uid
                if nj_uid in seen_in_batch:
                    logger.debug(f"Skipping duplicate spawn for {nj_uid}.")
                    continue
                if store.is_known(nj_uid):
                    if nj_uid in store.queue_uids or nj_uid in store.in_flight_uids:
                        continue
                    wall_hit = nj_uid in store.wall
                    failed_hit = nj_uid in store.failed
                    if wall_hit or failed_hit:
                        decision = self._ctx.policy.evaluate(
                            nj.to_dict(),
                            wall_meta=store.wall.get(nj_uid),
                            is_wall=wall_hit,
                            is_failed=failed_hit,
                        )
                        if decision.should_skip:
                            continue
                seen_in_batch.add(nj_uid)
                unique_new_jobs.append(nj)

            for nj in unique_new_jobs:
                jd = nj.to_dict()
                self._ctx.policy.normalize_job_dict(jd, nj.task_type)
                inject_worker_resource(jd)
                spawned_dicts.append(jd)
            logger.debug(f"Spawned {len(spawned_dicts)} jobs for {uid}.")

        # 构建 wall meta
        wall_meta = dict(result.result_meta or {})
        declared_inputs = self._ctx.channel.read_declared_inputs(uid)

        self._ctx.store.apply_success(
            uid,
            wall_meta,
            spawned_jobs=spawned_dicts,
            cursor_updates=result.cursor_updates,
            declared_inputs=declared_inputs,
            run_id=self._ctx.run_id,
            job_dict=job_dict,
        )
        self._ctx.stats["completed"] += 1
        result.going_to_retry = False

    def _apply_failure(
        self, uid: str, job_dict: dict, result: ExecutionResult,
        job_start: Optional[float] = None
    ) -> None:
        """处理永久失败分支：写入 DLQ 与级联阻断下游。"""
        duration = (time.monotonic() - job_start) if job_start is not None else 0.0
        logger.error(f"FAIL: {uid} (duration {duration:.2f}s, Sent to DLQ). Meta: {result.result_meta}")
        outcome = self._ctx.store.apply_failure(
            uid, result.result_meta, job_dict=job_dict, cascade=True
        )
        self._ctx.stats["failed"] += 1
        if outcome.cascaded_uids:
            self._ctx.stats["cascade_failed"] += len(outcome.cascaded_uids)
        result.going_to_retry = False



    def restore_stale_result(self, uid: str, job: Job, job_dict: dict) -> bool:
        """崩溃恢复：派发子进程前消费该 uid 的残留结果文件。

        上次 run 主进程 SIGKILL/OOM/断电 崩溃时，子进程可能已写好结果文件
        但未及 commit。本方法在 ``_dispatch_job`` 的 acquire/submit 之前
        调用：若有残留结果，直接经 ``_apply_result`` 提交（不启动子进程），
        避免「新子进程已启动、却被旧结果文件误判完成而 kill」的双重执行窗口。

        Returns:
            True 表示已消费残留（job 已提交/重入队，调用方应返回 None，
            不再派发子进程）；False 表示无残留，照常派发。
        """
        result = self._ctx.channel.consume_stale_result(uid, job)
        if result is None:
            return False
        logger.info(f"RESTORE: {uid} (stale result from previous run, no subprocess)")
        # （单一出口）：构造伪 entry 走 _complete_job 同一条路径
        # （acquired=[] 无资源、handle=None 无进程），复用完整的收尾契约：
        # 失败输出清理（_cleanup_outputs）、IPC 文件清理（cleanup_ipc_files）、
        # 身份注销。
        # expect_in_flight=False：uid 从未注册 in-flight（restore 在派发前），
        # _apply_result 内的 unregister 对未注册 uid 是 no-op，断言跳过。
        entry = InFlightJob(
            uid=uid,
            job_dict=job_dict,
            job=job,
            acquired=[],
            handle=None,
            job_start=None,
        )
        try:
            self.complete_job(entry, result)
        except _JobTerminated:
            # 残留结果提交时 commit 连续失败达阈值 → job 已 DLQ 终结。
            # 不 re-raise（主循环继续），消费语义视为完成（返回 True）。
            pass
        return True

    def cleanup_outputs(self, uid: str) -> None:
        """清理失败/中断 job 的半成品输出（向后兼容委托给 channel）。"""
        self._ctx.channel.cleanup_artifacts(uid, mode=ArtifactCleanupMode.FAILURE_OR_RETRY)

    def release_acquired(
        self,
        acquired_or_entry: Union[InFlightJob, List[Tuple[str, float]], Any],
        uid: Optional[str] = None,
    ) -> None:
        """释放已 acquire 的资源（支持 InFlightJob 或元组列表）。"""
        if isinstance(acquired_or_entry, InFlightJob):
            acquired_or_entry.release_resources(self._ctx.resource_mgr)
        else:
            self._ctx.resource_mgr.release_all(acquired_or_entry, uid=uid)

