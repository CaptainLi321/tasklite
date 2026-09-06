"""完成机器：结果提交、输出清理、资源释放与崩溃恢复族。

``complete_job`` 是子进程结果的唯一收尾入口；``apply_result`` 承载
retry/success/failure 三态事务提交；restore/cleanup/release 是崩溃恢复与
资源释放的共享助手。依赖经 RunContext 注入，经 ``self._failure`` 复用
失败机器（3-strike/级联），不反向引用 TaskLite。
"""

import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import RunContext
    from .failure import FailureMachine

from ..error_codes import ERR_MAX_RETRIES as _ERR_MAX_RETRIES
from ..exceptions import _JobTerminated
from ..models.job import Job
from .executor import (
    ExecutionResult, cleanup_ipc_files, read_inputs, read_outputs,
)
from .inflight import InFlightJob
from .policy import BackoffSchedule
from .retry import compute_backoff
from .runtime import RT_BACKOFF_UNTIL, RT_BACKOFF_WALL_DEADLINE, inject_worker_resource
from ..utils.ipc import inputs_path, outputs_path


logger = logging.getLogger("tasklite")


class CompletionMachine:
    """结果提交 / 输出清理 / 资源释放 / 崩溃恢复的完成侧机器。"""

    def __init__(self, ctx: "RunContext", failure: "FailureMachine") -> None:
        self._ctx = ctx
        self._failure = failure

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
        state = self._ctx.state
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
            # Release resources (无论成功/失败/重试/崩溃，都释放)
            self.release_acquired(entry.acquired, uid=uid)

            # Cleanup outputs on failure AND retry (intermediate products
            # from a failed retry attempt should not persist)。
            # result 总是非 None（_complete_job 只在有结果时调用）。
            # 输出从落盘 outputs.jsonl 读取（handler 声明时已落盘）。
            if not result.success:
                self.cleanup_outputs(uid)
            else:
                # 成功路径：不删输出文件（cleanup_on_fail 只对失败生效），
                # 仅删除落盘声明文件（outputs.jsonl 生命周期结束）。
                # kind=="cache" 的临时文件在成功路径也要
                # 清理（语义：任务结束时该文件不应存在；.part 已 rename 则
                # no-op）。outputs.jsonl 最后删除，先读完再删。
                try:
                    for out_path, _, kind in read_outputs(self._ctx.ipc_dir, uid):
                        if kind == "cache":
                            out_obj = Path(out_path)
                            if out_obj.exists():
                                out_obj.unlink()
                                logger.info(f"Cleaned cache file: {out_obj}")
                    op = outputs_path(self._ctx.ipc_dir, uid)
                    if op.exists():
                        op.unlink()
                    # inputs.jsonl 生命周期与 outputs.jsonl 一致——
                    # 成功路径已读入 wall meta，落盘文件可删。失败/重试路径
                    # 同样删除：不删则重试时 handler 重新 append，旧指纹残留
                    # → input_changed 误判无限重跑（见 _cleanup_outputs 的 finally）。
                    ip = inputs_path(self._ctx.ipc_dir, uid)
                    if ip.exists():
                        ip.unlink()
                except OSError:
                    pass

            # （单一出口）：IPC 文件（result/signals/tmp）生命周期集中
            # 在此 finally——正常路径 drain 已删（重复删除无害，FileNotFound
            # 忽略），恢复路径（_restore_stale_result 伪 entry）也经此
            # 清理，两条路径一致，孤儿 signals 文件不残留。
            try:
                cleanup_ipc_files(self._ctx.ipc_dir, uid)
            except Exception:
                pass

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
        state = self._ctx.state
        if expect_in_flight:
            assert uid in state.in_flight_uids, (
                f"identity vacuity violation: {uid} not in-flight at _apply_result entry"
            )

        # 1. 全局应用资源挂起
        applied_suspension = False
        for r_name, secs in result.resource_suspensions:
            if r_name in self._ctx.resources:
                self._ctx.resources[r_name].suspend(secs)
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
        """处理重试分支：max_retries 预算检查、指数退避计算与队尾重入队。"""
        state = self._ctx.state
        # 中断信号与孤儿锁冲突都不消耗预算：中断源于外部信号，锁冲突源于
        # 同 uid 孤儿执行体仍持锁（handler 未执行，孤儿死后重跑本可成功）。
        # 二者即使撞上已耗尽的重试预算也豁免 DLQ，走下方零计数短退避回队
        # 自恢复；其余超限则进入 DLQ。
        if job.retries >= job.max_retries and not (result.interrupted or result.lock_conflict):
            logger.error(f"FAIL: {uid} exceeded max retries ({job.max_retries}). Sent to DLQ.")
            fail_meta = {"error": _ERR_MAX_RETRIES}
            raw_rt = job_dict.get("runtime")
            last_retry_error = raw_rt.get("_last_retry_error") if isinstance(raw_rt, dict) else None
            if last_retry_error:
                fail_meta["last_retry_error"] = last_retry_error
            if result.retry_error:
                fail_meta["retry_error"] = result.retry_error
            committed = self._ctx.backend.commit_job_failure(uid, fail_meta)
            if committed:
                self._failure.apply_failed(uid, fail_meta)
                self._failure.cascade_fail(uid)
                result.going_to_retry = False
                return
            self._failure.commit_failed_crash(uid, "commit_job_failure", job_dict)

        lock_conflict = result.lock_conflict
        if result.interrupted or lock_conflict:
            sched = self._ctx.policy.compute_orphan_schedule()
            if result.interrupted:
                self._ctx.stats["interrupted_reruns"] += 1
            else:
                self._ctx.stats["deferred_orphan"] += 1
        else:
            job.retries += 1
            delay = compute_backoff(
                job.retries, job.backoff_base, job.backoff_max
            )
            sched = BackoffSchedule.from_delay(delay)

        logger.info(f"RETRY: {uid} (attempt {job.retries}/{job.max_retries}, backoff {sched.delay:.1f}s)")
        retry_dict = job.to_dict()
        retry_dict["resources"] = dict(job_dict.get("resources", {}))
        raw_rt = job_dict.get("runtime")
        retry_dict["runtime"] = dict(raw_rt) if isinstance(raw_rt, dict) else {}
        retry_rt = retry_dict["runtime"]
        retry_rt.setdefault("_last_retry_error", "")
        if result.retry_error and not (lock_conflict or result.interrupted):
            retry_rt["_last_retry_error"] = result.retry_error
        sched.populate_runtime(retry_rt)

        committed = self._ctx.backend.commit_retry(uid, retry_dict, front=False)
        if not committed:
            self._failure.commit_failed_crash(uid, "commit_retry", job_dict)

        state.unregister_in_flight(uid)
        state.requeue_jobs([retry_dict], front=False)
        self._ctx.stats["retried"] += 1
        result.going_to_retry = True

    def _apply_success(
        self, uid: str, job_dict: dict, result: ExecutionResult,
        job_start: Optional[float] = None
    ) -> None:
        """处理成功分支：子任务去重、wall 记录与 cursor 推进。"""
        state = self._ctx.state
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
                if state.is_known(nj_uid):
                    if nj_uid in state.queue_uids or nj_uid in state.in_flight_uids:
                        continue
                    wall_hit = nj_uid in state.wall
                    failed_hit = nj_uid in state.failed
                    if wall_hit or failed_hit:
                        decision = self._ctx.policy.evaluate(
                            nj.to_dict(),
                            wall_meta=state.wall.get(nj_uid),
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
        prev = state.wall.get(uid)
        raw_count = prev.get("run_count", 0) if isinstance(prev, dict) else 0
        try:
            prev_count = int(raw_count)
        except (TypeError, ValueError):
            logger.warning(
                f"Corrupt run_count for {uid} in wall meta ({raw_count!r}); treating as 0"
            )
            prev_count = 0
        wall_meta["run_count"] = prev_count + 1
        wall_meta["last_run_at"] = datetime.now(timezone.utc).isoformat()
        wall_meta["last_run_id"] = self._ctx.run_id
        try:
            declared_inputs = read_inputs(self._ctx.ipc_dir, uid)
            deduped: dict = {}
            for entry in declared_inputs:
                deduped[entry.get("path")] = entry
            if deduped:
                wall_meta["inputs"] = list(deduped.values())
        except Exception:
            pass

        committed = self._ctx.backend.commit_job_success(
            uid, wall_meta,
            spawned_jobs=spawned_dicts,
            cursor_updates=result.cursor_updates,
        )
        if committed:
            if spawned_dicts:
                state.spawn_jobs(spawned_dicts, front=True)
            if result.cursor_updates:
                state.update_cursors(result.cursor_updates)
            state.mark_success(uid, wall_meta)
            state.unregister_in_flight(uid)
            self._ctx.stats["completed"] += 1
            result.going_to_retry = False
            return
        self._failure.commit_failed_crash(uid, "commit_job_success", job_dict)

    def _apply_failure(
        self, uid: str, job_dict: dict, result: ExecutionResult,
        job_start: Optional[float] = None
    ) -> None:
        """处理永久失败分支：写入 DLQ 与级联阻断下游。"""
        duration = (time.monotonic() - job_start) if job_start is not None else 0.0
        logger.error(f"FAIL: {uid} (duration {duration:.2f}s, Sent to DLQ). Meta: {result.result_meta}")
        committed = self._ctx.backend.commit_job_failure(
            uid, result.result_meta
        )
        if committed:
            self._failure.apply_failed(uid, result.result_meta)
            self._failure.cascade_fail(uid)
            result.going_to_retry = False
            return
        self._failure.commit_failed_crash(uid, "commit_job_failure", job_dict)



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
        result = self._ctx.executor.consume_stale_result(uid, job)
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
        """清理失败/中断 job 的半成品输出（abort 路径复用）。

        遍历 handler 声明的输出（从落盘 ``{uid}.outputs.jsonl`` 读取，
        handler 崩溃/kill 后声明仍可读），对 ``cleanup_on_fail=True`` 且
        物理存在的路径执行删除（文件 unlink / 目录 rmtree）。
        读后**删除落盘文件**（消费语义，与 read_signals 一致）——
        outputs.jsonl 生命周期在此结束，drain 的 cleanup_ipc_files
        不覆盖它。

        ``_complete_job`` 与 ``_abort_in_flight`` 共用同一清理：被 kill 的
        in-flight job 半成品输出若不清理，重启后 handler 若「文件已存在
        则跳过」会读到半残文件。
        """
        outputs = read_outputs(self._ctx.ipc_dir, uid)
        try:
            for out_path, cleanup, kind in outputs:
                # kind=="cache" 的临时文件**无条件删除**
                # （语义：任务结束时该文件不应存在；成功路径 rename 已发生
                # 则是 no-op）。kind=="output" 按 cleanup_on_fail 删除。
                if kind == "cache" or cleanup:
                    out_path_obj = Path(out_path)
                    if out_path_obj.exists():
                        if out_path_obj.is_dir():
                            shutil.rmtree(out_path_obj)
                        else:
                            out_path_obj.unlink()
                        logger.info(f"Cleaned broken output: {out_path_obj}")
        except Exception as e:
            logger.error(f"Could not remove outputs for {uid}: {e}")
        finally:
            try:
                # 兼容测试对 Path.unlink 的无参 monkeypatch：
                # 避免 missing_ok keyword 触发 TypeError。
                p = outputs_path(self._ctx.ipc_dir, uid)
                if p.exists():
                    p.unlink()
                # inputs.jsonl 生命周期与 outputs.jsonl 一致——
                # 失败/重试路径也消费删除：不删则重试时 handler 重新 append，
                # 旧指纹残留 → input_changed 任一 entry 不匹配 →
                # on_input_change 无限重跑。输入文件本身不是框架产物（不删），
                # 但声明文件（inputs.jsonl）属于本次执行。
                ip = inputs_path(self._ctx.ipc_dir, uid)
                if ip.exists():
                    ip.unlink()
            except OSError:
                pass

    def release_acquired(
        self, acquired: List[Tuple[str, float]], uid: Optional[str] = None
    ) -> None:
        """释放已 acquire 的资源列表。

        逐资源 try/except：单个资源的 ``release()`` 失败不中断循环，
        确保其余资源也被释放（否则资源计数器永久抬高，形成泄漏）。
        """
        for res_name, amount in acquired:
            try:
                self._ctx.resources[res_name].release(amount)
            except Exception as e:
                logger.error(f"Error releasing resource '{res_name}' for {uid}: {e}")

