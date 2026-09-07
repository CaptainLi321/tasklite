"""派发机器：派发预检与 submit 编排。

五关顺序即契约（顺序即时序约束）：dedup → dep-failed → no-handler →
orphan-probe → stale-restore。依赖经 RunContext（``self._ctx``）注入，
经 ``self.store``/``self._completion`` 复用状态深模块与完成机器，
不反向引用 TaskLite。
"""

import logging
import random
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import RunContext
    from .completion import CompletionMachine

from ..taxonomy import (
    ERR_DISPATCH_FAILURE as _ERR_DISPATCH_FAILURE,
    ERR_JOB_DEPENDENCY as _ERR_JOB_DEPENDENCY,
    ERR_NO_HANDLER as _ERR_NO_HANDLER,
    ERR_PAYLOAD_VALIDATION as _ERR_PAYLOAD_VALIDATION,
    validate_payload,
)
from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.context import TaskContext
from ..models.job import Job, JobRuntimeState
from .channel import ArtifactCleanupMode, JobHandle
from .inflight import InFlightJob
from .scheduler import DeadlockAttribution

logger = logging.getLogger("tasklite")


@dataclass(frozen=True)
class DispatchOutcome:
    """一次调度派发周期的不可变决策结果（彻底解耦整型下标与底层调度 DTO）。"""
    entry: Optional[InFlightJob] = None
    has_runnable: bool = False
    should_continue: bool = True
    worker_wait: float = 0.0
    min_wait: float = 0.0
    waiting_for_dependency: bool = False
    deadlock_attribution: DeadlockAttribution = field(default_factory=DeadlockAttribution)
    dispatched: bool = False
    handled: bool = False
    sched: Optional[Any] = None

    @property
    def attribution(self) -> DeadlockAttribution:
        """死锁归因值对象属性别名（兼容 StateStore.handle_deadlock 读取）。"""
        return self.deadlock_attribution


class DispatchMachine:
    """派发预检 + 资源 acquire + 子进程 submit 的编排器。"""

    def __init__(
        self, ctx: "RunContext", completion: "CompletionMachine"
    ) -> None:
        self._ctx = ctx
        self._completion = completion

    def dispatch_next(self) -> DispatchOutcome:
        """统一扫描与派发接缝：工人资源预检 -> 队列扫描 -> 预检五关 -> 资源锁定 -> 子进程派发。

        返回 DispatchOutcome：
        - entry 非 None：成功派发子进程并登记 in-flight（has_runnable=True, dispatched=True, handled=True）；
        - entry 为 None 且 should_continue 为 True：处理了无需子进程的作业（去重/依赖失败/无 handler/校验失败，has_runnable=True, handled=True），调用方可继续填池；
        - should_continue 为 False：工人资源耗尽或无可运行作业，调用方应退出填池循环。
        """
        store = self._ctx.store
        ok, worker_wait = self._ctx.resource_mgr.can_acquire_worker(1.0)
        if not ok:
            if worker_wait <= 0:
                worker_wait = 0.05
            return DispatchOutcome(
                entry=None,
                has_runnable=True,
                should_continue=False,
                worker_wait=worker_wait,
                min_wait=0.0,
                dispatched=False,
                handled=False,
                sched=None,
            )

        in_flight_uids = store.in_flight_uids
        sched = self._ctx.scheduler.pop_next_runnable(store, in_flight_uids)
        if sched.runnable_idx is None:
            return DispatchOutcome(
                entry=None,
                has_runnable=False,
                should_continue=False,
                worker_wait=0.0,
                min_wait=sched.min_wait,
                waiting_for_dependency=sched.waiting_for_dependency,
                deadlock_attribution=sched.attribution,
                dispatched=False,
                handled=False,
                sched=sched,
            )

        entry = self.dispatch_job(sched)
        if entry is None:
            return DispatchOutcome(
                entry=None,
                has_runnable=True,
                should_continue=True,
                worker_wait=0.0,
                min_wait=0.0,
                dispatched=False,
                handled=True,
                sched=sched,
            )

        return DispatchOutcome(
            entry=entry,
            has_runnable=True,
            should_continue=True,
            worker_wait=0.0,
            min_wait=0.0,
            dispatched=True,
            handled=True,
            sched=sched,
        )

    def _reject_and_commit(
        self, uid: str, job_dict: dict, meta: dict, *,
        count_as: str = "failed",
    ) -> None:
        """拒绝 job 的统一出口：委托 StateStore.apply_failure 原子终态转移。

        四处拒绝路径（依赖失败 / no-handler / payload 校验失败 / dispatch 3-strike）
        共用本方法，彻底收敛 commit + apply_failed + cascade_fail + fire_hook 至 StateStore。

        正常返回 = 处理完成（commit 成功或 3-strike DLQ 成功）；
        _CommitCrashSignal 穿透上抛 = 后端环境故障（由 run_loop 崩溃处理）。
        """
        try:
            outcome = self._ctx.store.apply_failure(
                uid, meta, job_dict=job_dict, count_as=count_as, cascade=True
            )
            self._ctx.stats[count_as] += 1
            if outcome.cascaded_uids:
                self._ctx.stats["cascade_failed"] += len(outcome.cascaded_uids)
            self._ctx.fire_job_completed(uid, outcome.error_meta, False, False)
        except _JobTerminated:
            # 3-strike DLQ 成功——job 已终结，统计递增后正常返回即可
            self._ctx.stats[count_as] += 1
            return

    def dispatch_dedup(self, store, uid: str, job_dict: dict) -> bool:
        """派发预检关 1——去重（is_known 命中 → rerun 策略 → skip/放行）。

        返回 True = 已处理（job 被 skip 或放行后本关终结）；返回 False
        表示未命中（调用方继续后续预检关）。统一 is_known 谓词；
        rerun 策略豁免 every_run/on_failure 的 wall/failed 命中。
        """
        if store.is_known(uid):
            decision = self._ctx.policy.evaluate(
                job_dict,
                wall_meta=store.wall.get(uid),
                is_wall=(uid in store.wall),
                is_failed=(uid in store.failed),
            )
            if decision.should_skip:
                self._ctx.store.apply_skip(uid, job_dict)
                self._ctx.stats["skipped"] += 1
                return True
        return False


    def dispatch_dep_failed(self, uid: str, job_dict: dict, pending_dep_failure: str) -> bool:
        """派发预检关 2——依赖失败（父任务已进 DLQ → 本任务 JOB_DEPENDENCY）。

        返回 True = 已处理（依赖失败直接 commit，含级联下游 + 钩子）；
        仅当 pending_dep_failure 非空时调用。commit 失败走 3-strike / 崩溃路径。
        """
        if pending_dep_failure is not None:
            logger.warning(f"SKIP: {uid} (Dependency {pending_dep_failure} failed)")
            fail_meta = {"error": _ERR_JOB_DEPENDENCY,
                         "failed_dependency": pending_dep_failure}
            # 依赖父失败的级联下游计入 cascade_failed 而非 failed，
            # 保证「真实业务失败率」统计不被级联稀释。
            self._reject_and_commit(
                uid, job_dict, fail_meta, count_as="cascade_failed",
            )
            return True
        return False


    def dispatch_no_handler(self, uid: str, job_dict: dict, task_type: str) -> bool:
        """派发预检关 3——无对应 handler 注册（配置级错误 → FatalError DLQ）。

        返回 True = 已处理（直接 commit 到 DLQ 并 cascade 下游）；
        仅当 task_type not in handlers 时调用。commit 失败走 3-strike / 崩溃路径。
        """
        if task_type not in self._ctx.handlers:
            logger.error(f"SKIP: {uid} (No handler for task_type '{task_type}')")
            fail_meta = {"error": _ERR_NO_HANDLER, "fatal": True}
            self._reject_and_commit(uid, job_dict, fail_meta)
            return True
        return False


    def dispatch_orphan_probe(self, store, uid: str, job_dict: dict) -> bool:
        """派发预检关 4——孤儿探测（probe 先于 restore）。

        主进程仅探测（非阻塞试锁）。锁被占 = 同 uid 孤儿 worker 仍持锁
        → requeue + 短退避（上限 ~1s 防热循环）本轮不派发。
        probe 必须**先于** _restore_stale_result 与残留声明清理——
        孤儿存活时提前 return，绝不删孤儿实时声明。返回 True = 已处理
        （孤儿存活 defer）；否则调用方继续 restore/submit。
        """
        # 主进程仅探测——非阻塞试锁，成功即释放。
        # 锁生命周期 = 执行体生命周期：主进程崩溃不释放 worker 的锁，
        # 探测失败 = 同 uid 孤儿 worker 仍持锁 → requeue + 短退避
        # （上限 ~1s 防热循环）本轮 defer，下轮孤儿死后正常执行。
        # probe 必须**先于** restore 与声明清理——
        # 孤儿存活时提前 return，绝不删孤儿实时声明；probe 通过后
        # restore 消费孤儿残留结果 → 不派发（无双跑）。
        if not self._ctx.channel.probe_orphan_lock(uid):
            logger.warning(
                f"Deferring {uid}: orphan execution body still holds lock; "
                f"requeue with short backoff."
            )
            self._ctx.stats["deferred_orphan"] += 1
            self._ctx.policy.plan_orphan_defer(job_dict)
            store.requeue_jobs([job_dict], front=True)
            return True
        return False


    def dispatch_stale_restore(self, uid: str, job: Job, job_dict: dict) -> bool:
        """派发预检关 5——陈旧结果认领与派发前残留清理（stale-restore）。

        1. 若存在上一轮崩溃遗留的结果文件，直接通过完成机器提交（返回 True，不派发子进程）。
        2. 若无残留结果文件，清理已死孤儿残留声明与信号文件，准备派发新子进程（返回 False）。
        """
        if self._completion.restore_stale_result(uid, job, job_dict):
            return True
        self._ctx.channel.cleanup_artifacts(uid, mode=ArtifactCleanupMode.PRE_SUBMIT)
        return False


    def dispatch_job(self, sched: Any) -> Optional[InFlightJob]:
        """统一派发单个作业：出队 -> 五关预检 -> 两阶段资源租约 -> 子进程启动 -> in-flight 原子登记。

        参数 sched 可以为 ScheduleResult、Job 实例或 job_dict 字典。
        返回 InFlightJob 条目；若被预检五关拦截（去重/依赖失败/无handler/孤儿延迟/残留恢复）或校验失败，返回 None。
        """
        store = self._ctx.store
        if isinstance(sched, Job):
            job = sched
            job_dict = job.to_dict()
            pending_dep_failure = None
        elif isinstance(sched, dict):
            job_dict = sched
            job = Job.from_dict(job_dict)
            pending_dep_failure = None
        else:
            runnable_idx = getattr(sched, "runnable_idx", 0)
            if runnable_idx is None:
                return None
            kind = getattr(sched, "kind", "runnable")
            pending_dep_failure = getattr(sched, "pending_dep_failure", None) if kind == "dep_failed" else None
            job_dict = store.pop_job(runnable_idx)
            job = Job.from_dict(job_dict)

        uid = job.uid
        # 预检五关以调用顺序表达时序契约，每关返回 True=已处理。
        # 关 1-4（dedup/dep-failed/no-handler/orphan-probe）+ 关 5（stale-restore）。
        if self.dispatch_dedup(store, uid, job_dict):
            return None
        if pending_dep_failure is not None:
            if self.dispatch_dep_failed(uid, job_dict, pending_dep_failure):
                return None
        if job.task_type not in self._ctx.handlers:
            if self.dispatch_no_handler(uid, job_dict, job.task_type):
                return None
        if self.dispatch_orphan_probe(store, uid, job_dict):
            return None
        if self.dispatch_stale_restore(uid, job, job_dict):
            return None

        # Acquire resources via two-phase lease
        handle: Optional[JobHandle] = None
        try:
            lease = self._ctx.resource_mgr.reserve(
                job.task_type, job.resources, uid=uid
            )
            with lease:
                # Payload validation
                _handler_entry = self._ctx.handlers[job.task_type]
                _payload_schema = _handler_entry.payload_schema
                if _payload_schema is not None:
                    _errors = validate_payload(job.payload, _payload_schema)
                    if _errors:
                        logger.error(f"Payload validation failed for {uid}: {_errors}")
                        # 校验失败不走子进程，release 租约后直接 commit
                        lease.release()
                        fail_meta = {"error": _ERR_PAYLOAD_VALIDATION, "details": _errors}
                        self._reject_and_commit(uid, job_dict, fail_meta)
                        return None
                # Build context + submit (non-blocking)
                wall_keys = store.wall_uids
                failed_keys = store.failed_uids
                # 输出声明走落盘 outputs.jsonl——handler 子进程内声明的
                # 输出经落盘文件传回主进程。
                # fencing：分配本 job 的执行代标识（run_id.seq）。
                # seq 每次 submit 递增——同 uid 重试再派发也获得新 incarnation，
                # 与上次尝试的结果文件隔离（旧尝试的残留不被本次 drain 看见）。
                self._ctx.dispatch_seq += 1
                incarnation = f"{self._ctx.run_id}.{self._ctx.dispatch_seq}"
                # 注册表快照契约：per-pipeline 瞬态异常注册表快照随 ctx pickle
                # 下发——分类决策在子进程，注册表必须显式传递（不可依赖父进程
                # 作用域，更不存在模块级可变全局）。
                ctx = TaskContext(
                    job, wall_keys, failed_keys, dict(store.cursors),
                    output_root=self._ctx.output_root,
                    ipc_dir=self._ctx.ipc_dir, incarnation=incarnation,
                    transient_registry=self._ctx.transient_registry.snapshot(),
                    # 资源名注册集快照随 ctx 下发——
                    # suspend_resource 对未注册名 fail-loud（typo 不静默失效）。
                    resource_names=frozenset(self._ctx.resources),
                )

                logger.info(f"RUN: {uid}")
                job_start = time.monotonic()
                handle = self._ctx.channel.submit(
                    self._ctx.handlers[job.task_type].func, job, ctx, job.timeout,
                    ipc_dir=self._ctx.ipc_dir,
                )
                lease.claim()

                entry = InFlightJob(
                    uid=uid,
                    job_dict=job_dict,
                    job=job,
                    handle=handle,
                    job_start=job_start,
                    lease=lease,
                )
                # 在 return 前原子登记到 in_flight 与 state 索引，避免时序真空
                self._ctx.in_flight.dispatch(entry, state=store)
                return entry

        except _CommitCrashSignal:
            # _CommitCrashSignal 继承 BaseException——「commit 失败需崩溃」
            # 的信号不会被 except Exception 兜底误吞。re-raise 让它穿透到
            # run_loop 的崩溃处理分支（已 requeue 当前 job，不在此二次处理）。
            raise
        except KeyboardInterrupt:
            logger.warning(f"Pipeline interrupted while dispatching {uid}.")
            if uid in self._ctx.in_flight:
                # entry 已注册到 _in_flight：此处不清理/不 requeue/不 release，
                # 全部交给 _run_loop 的 _abort_in_flight 统一处理，
                # 避免对同一 entry 二次释放资源、二次 requeue 同一作业。
                raise
            if handle is not None:
                self._ctx.channel.cleanup([handle])
            store.requeue_jobs([job_dict], front=True)
            # 不在此 save_queue：内存此刻缺其他 in-flight 作业，
            # 交给 _run_loop 的 _save_queue_crash_safe 合并磁盘真相后统一保存。
            raise
        except Exception as e:
            logger.error(f"Error dispatching job {uid}: {e}\n{traceback.format_exc()}")
            if handle is not None:
                self._ctx.channel.cleanup([handle])
            rt_state = JobRuntimeState.from_dict(job_dict.get("runtime"))
            failures = rt_state.record_dispatch_failure()
            job_dict["runtime"] = rt_state.to_dict()
            if failures >= self._ctx.commit_failure_dlq_threshold:
                logger.critical(
                    f"Dispatch failed {failures} times for {uid} ({e}); "
                    f"treating as deterministic bad input (e.g. unpickleable "
                    f"handler), sending to DLQ."
                )
                fail_meta = {
                    "error": _ERR_DISPATCH_FAILURE,
                    "failures": failures,
                    "detail": str(e)[:200],
                }
                self._ctx.in_flight.pop(uid, None)
                self._reject_and_commit(uid, job_dict, fail_meta)
                return None
            self._ctx.in_flight.pop(uid, None)
            store.unregister_in_flight(uid)
            store.requeue_jobs([job_dict], front=True)
            # 不在此 save_queue：内存此刻缺其他 in-flight 作业，
            # 交给 _run_loop 的 _save_queue_crash_safe 合并磁盘真相后统一保存。
            raise

