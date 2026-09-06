"""派发机器：派发预检与 submit 编排。

五关顺序即契约（顺序即时序约束）：dedup → dep-failed → no-handler →
orphan-probe → stale-restore。依赖经 RunContext（``self._ctx``）注入，
经 ``self._failure``/``self._completion`` 复用失败机器与完成机器，
不反向引用 TaskLite。
"""

import logging
import random
import time
import traceback
from typing import List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import RunContext
    from .failure import FailureMachine
    from .completion import CompletionMachine

from ..error_codes import (
    ERR_DISPATCH_FAILURE as _ERR_DISPATCH_FAILURE,
    ERR_JOB_DEPENDENCY as _ERR_JOB_DEPENDENCY,
    ERR_NO_HANDLER as _ERR_NO_HANDLER,
    ERR_PAYLOAD_VALIDATION as _ERR_PAYLOAD_VALIDATION,
)
from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.context import TaskContext
from ..models.job import Job
from .runtime import RT_BACKOFF_UNTIL, RT_BACKOFF_WALL_DEADLINE
from .executor import JobHandle
from .inflight import InFlightJob
from ..utils.ipc import inputs_path, outputs_path, signals_path

from ..utils.lockfile import probe_lock
from ..utils.validation import validate_payload

logger = logging.getLogger("tasklite")


class DispatchMachine:
    """派发预检 + 资源 acquire + 子进程 submit 的编排器。"""

    def __init__(
        self, ctx: "RunContext", failure: "FailureMachine", completion: "CompletionMachine"
    ) -> None:
        self._ctx = ctx
        self._failure = failure
        self._completion = completion

    def _reject_and_commit(
        self, uid: str, job_dict: dict, meta: dict, *,
        count_as: str = "failed",
    ) -> None:
        """拒绝 job 的统一出口：commit 失败 → 三连收敛 → 级联 → 钩子。

        三处拒绝路径（依赖失败 / no-handler / payload 校验失败）共用本方法，
        消除重复的 commit + apply_failed + cascade_fail + fire_hook 样板。

        正常返回 = 处理完成（commit 成功或 3-strike DLQ 成功）；
        _CommitCrashSignal 穿透上抛 = 后端环境故障（由 run_loop 崩溃处理）。
        """
        committed = self._ctx.backend.commit_job_failure(uid, meta)
        if committed:
            self._failure.apply_failed(uid, meta, count_as=count_as)
            self._failure.cascade_fail(uid)
            self._ctx.fire_job_completed(uid, meta, False, False)
            return
        # commit 失败 → 3-strike / 崩溃路径。commit_failed_crash 要么抛
        # _JobTerminated（3-strike DLQ 成功，job 已终结），要么抛
        # _CommitCrashSignal（后端环境故障，需崩溃重启）。
        try:
            self._failure.commit_failed_crash(
                uid, meta.get("error", "unknown"), job_dict,
            )
        except _JobTerminated:
            # 3-strike DLQ 成功——job 已终结，正常返回即可
            return
        # _CommitCrashSignal 继承 BaseException，不被上方 except 捕获，
        # 自动穿透上抛到 run_loop 的崩溃处理分支。

    def dispatch_dedup(self, state, uid: str, job_dict: dict) -> bool:
        """派发预检关 1——去重（is_known 命中 → rerun 策略 → skip/放行）。

        返回 True = 已处理（job 被 skip 或放行后本关终结）；返回 False
        表示未命中（调用方继续后续预检关）。统一 is_known 谓词；
        rerun 策略豁免 every_run/on_failure 的 wall/failed 命中。
        """
        # Dedup check (job already completed or failed since queue was loaded)
        # 统一 is_known 谓词。pop 自 queue 的 uid
        # 不可能在 queue/in-flight（已出队）。命中时对后端做 commit_skip——
        # 否则磁盘条目残留，每次 run 重复 pop→skip→drift。
        # rerun 策略豁免——every_run/on_failure 任务命中
        # wall/failed 时**放行重跑**（不 skip；磁盘残留由后续 commit 清理）。
        if state.is_known(uid):
            decision = self._ctx.policy.evaluate(
                job_dict,
                wall_meta=state.wall.get(uid),
                is_wall=(uid in state.wall),
                is_failed=(uid in state.failed),
            )
            if decision.should_skip:
                self._ctx.stats["skipped"] += 1
                committed = self._ctx.backend.commit_skip(uid)
                if not committed:
                    # commit_skip 终态模型：skip 命中 = uid 已有
                    # wall/failed 终态，此处只是清理磁盘残留。commit_skip 失败
                    # 是环境故障——走「requeue + 崩溃」契约，**绝不走 3-strike
                    # DLQ**（那会把 wall 成功记录翻转成失败）。下一次 run 的
                    # 加载期过滤或 commit_skip 重试消化残留。
                    self._failure.commit_skip_crash(uid, job_dict)
                    # 与 dep-failed/no-handler 分支同款：
                    # _commit_skip_crash 契约上永不正常返回；fall-through 仅当
                    # 契约被破坏时可达——fail-loud 优于静默「装作已处理」。
                    raise AssertionError(
                        "_commit_skip_crash unexpectedly returned normally"
                    )
                else:
                    # pop_job 对 rerun 任务把 uid 加入
                    # _rerun_active_uids（豁免集合），skip 成功路径配对
                    # discard——否则豁免集合泄漏并永久弱化 DEBUG 互斥断言。
                    # 与 dep-failure/no-handler 直接 commit 路径的 discard
                    # 对称；对未注册 uid 是 no-op。
                    state.unregister_in_flight(uid)
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


    def dispatch_orphan_probe(self, state, uid: str, job_dict: dict) -> bool:
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
        if not probe_lock(self._ctx.ipc_dir, uid):
            logger.warning(
                f"Deferring {uid}: orphan execution body still holds lock; "
                f"requeue with short backoff."
            )
            self._ctx.stats["deferred_orphan"] += 1
            # 策略深模块生成孤儿退避时间表并原子填充 runtime
            sched = self._ctx.policy.compute_orphan_schedule()
            rt = job_dict.setdefault("runtime", {})
            sched.populate_runtime(rt)
            state.requeue_jobs([job_dict], front=True)
            return True
        return False


    def dispatch_job(self, sched) -> Optional[InFlightJob]:
        """Pop job → 预检查 → acquire 资源 → submit 到子进程。直接操作 self._ctx.state。

        返回 entry。entry 为 None 表示 job 走了「不走子进程」路径
        （dedup/依赖失败/no-handler/payload 校验失败），已直接 commit 并返回。
        entry 非 None 表示已 submit，需由 ``_complete_job`` 处理结果。
        """
        state = self._ctx.state
        runnable_idx = sched.runnable_idx
        # kind 显式表达调度契约——"dep_failed" 表示 runnable_idx 指向
        # dep-failed 兜底位置（pending_dep_failure 携带失败依赖），"runnable"
        # 表示真正可运行的 job。消费方按 kind 走分支。
        pending_dep_failure = sched.pending_dep_failure if sched.kind == "dep_failed" else None

        job_dict = state.pop_job(runnable_idx)
        job = Job.from_dict(job_dict)
        uid = job.uid
        # 预检五关以调用顺序表达时序契约，每关返回 True=已处理。
        # 关 1-4（dedup/dep-failed/no-handler/orphan-probe）+ 关 5（stale-restore）。
        if self.dispatch_dedup(state, uid, job_dict):
            return None
        if pending_dep_failure is not None:
            if self.dispatch_dep_failed(uid, job_dict, pending_dep_failure):
                return None
        if job.task_type not in self._ctx.handlers:
            if self.dispatch_no_handler(uid, job_dict, job.task_type):
                return None
        if self.dispatch_orphan_probe(state, uid, job_dict):
            return None
        # 关 5：stale-restore（崩溃残留消费）

        # 崩溃恢复：派发子进程前先消费该 uid 的残留结果文件
        # （上次 run 崩溃前已执行完成但未 commit）。有残留 → 直接提交，不派发。
        # restore 在 probe **之后**——probe 通过意味着
        # 无存活孤儿（锁生命周期 = 执行体生命周期，孤儿已死才会释放锁），
        # 此时残留结果文件若存在必然来自已死执行的孤儿，消费它即吸收
        # 「孤儿已完成但主进程崩溃未 commit」的执行成果，不派发（无双跑）。
        if self._completion.restore_stale_result(uid, job, job_dict):
            return None

        # 崩溃残留声明文件污染——崩溃时序「drain 消费
        # 结果后 _complete_job finally 前 SIGKILL」残留 outputs.jsonl，重派发
        # 时新 worker **append** 声明 → [旧声明, 新声明] → 成功路径存在性
        # 校验读到旧声明 → "Missing output" 假 DLQ。此处 _restore_stale_result
        # 已返回 False（无残留结果可消费，只剩崩溃残留声明）→ submit 前
        # 清残留；与 _complete_job finally 的防御一致（try/except OSError，
        # FileNotFound 忽略），正常路径无残留时幂等 no-op。
        # 清理块**必须在 probe 之后**——probe 失败（孤儿
        # 存活）时已提前 return，绝不会走到这里删孤儿实时声明；probe 通过
        # + restore 无果 = 无存活执行体且无残留结果，声明文件只可能是
        # 已死执行留下的崩溃残留。
        for _stale_decl in (
            outputs_path(self._ctx.ipc_dir, uid), inputs_path(self._ctx.ipc_dir, uid),
            # signals 文件同属已死执行的崩溃残留——上一执行
            # 体写了 suspend 信号但主进程未及消费即崩，残留信号会在此刻
            # （新 incarnation 派发前）被 apply_pending_signals 误应用到本轮
            # 上下文（资源挂起与新执行体无关）。随声明文件一并清理。
            signals_path(self._ctx.ipc_dir, uid),
        ):
            try:
                if _stale_decl.exists():
                    _stale_decl.unlink()
            except OSError:
                pass

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
                # 增量 uid 索引（PipelineState._wall_uids/_failed_uids）避免
                # 每次派发 ``set(state.wall.keys())`` 的 O(W) 重建；这里是单线程
                # 派发路径，活引用在 submit 内立即 pickle 为子进程快照。
                wall_keys = state.wall_uids
                failed_keys = state.failed_uids
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
                    job, wall_keys, failed_keys, dict(state.cursors),
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
                    acquired=lease.acquired,
                    handle=handle,
                    job_start=job_start,
                    lease=lease,
                )
                # 在 return 前原子登记到 in_flight 与 state 索引，避免时序真空
                self._ctx.in_flight.register(entry, state=state)
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
            state.requeue_jobs([job_dict], front=True)
            # 不在此 save_queue：内存此刻缺其他 in-flight 作业，
            # 交给 _run_loop 的 _save_queue_crash_safe 合并磁盘真相后统一保存。
            raise
        except Exception as e:
            logger.error(f"Error dispatching job {uid}: {e}\n{traceback.format_exc()}")
            if handle is not None:
                self._ctx.channel.cleanup([handle])
            # dispatch 阶段失败（submit 的 pickle/启动报错、
            # 资源 acquire 校验失败）：确定性失败（如不可 pickle 的
            # lambda handler）若只 requeue + 崩溃会触发**无限重启循环**。
            # 独立 `_dispatch_failures` 计数——不复用
            # `_commit_failures`（混用计数的话，dispatch 失败几次后任意一次
            # commit 失败即达阈值进 DLQ，错误码 COMMIT_FAILURE_DLQ 误导
            raw_rt = job_dict.get("runtime")
            if not isinstance(raw_rt, dict):
                raw_rt = {}
                job_dict["runtime"] = raw_rt
            rt = raw_rt
            failures = rt.get("_dispatch_failures", 0) + 1
            rt["_dispatch_failures"] = failures
            if failures >= self._ctx.commit_failure_dlq_threshold:
                logger.critical(
                    f"Dispatch failed {failures} times for {uid} ({e}); "
                    f"treating as deterministic bad input (e.g. unpickleable "
                    f"handler), sending to DLQ."
                )
                committed = self._ctx.backend.commit_job_failure(
                    uid, {"error": _ERR_DISPATCH_FAILURE,
                          "failures": failures, "detail": str(e)[:200]},
                )
                if committed:
                    # 与下方 requeue 分支对称——DLQ 分支
                    # return 前也移除 entry：若 entry 已注册（register_in_flight
                    # 的 DEBUG 断言失败时可达），残留的 entry 会在后续 drain
                    # 中被当 in-flight 处理（死 handle → 二次 _complete_job →
                    # 重复 DLQ / 断言崩）。未注册时 pop/unregister 均安全。
                    self._ctx.in_flight.pop(uid, None)
                    # dispatch 失败达阈值视为业务侧确定性坏输入，下游自动级联跳过。
                    self._failure.apply_failed(
                        uid, {"error": _ERR_DISPATCH_FAILURE,
                              "failures": failures, "detail": str(e)[:200]}
                    )
                    self._failure.cascade_fail(uid)
                    # dispatch 3-strike 终态触发钩子——
                    # 承诺「每个 job 终结时钩子恰好调用一次」（否则监控
                    # 漏报该类失败）。与 _commit_failed_crash /
                    # payload 校验等直接 commit 路径对称。
                    self._ctx.fire_job_completed(
                        uid, {"error": _ERR_DISPATCH_FAILURE,
                              "failures": failures, "detail": str(e)[:200]},
                        False, False,
                    )
                    return
                # DLQ 也失败（环境故障）→ 走 crash 路径
            # 与 KeyboardInterrupt 分支对称——若 entry 已
            # 注册到 _in_flight（仅 register_in_flight 的 DEBUG 断言失败时
            # 可达，此时状态已损坏），requeue 后 raise 会被 _run_loop 的
            # _abort_in_flight 对该 entry **二次 requeue**（内存队列重复 uid）。
            # 先移除 entry（未注册时 pop/unregister 均安全），requeue 交给
            # 下方统一执行。
            self._ctx.in_flight.pop(uid, None)
            state.unregister_in_flight(uid)
            state.requeue_jobs([job_dict], front=True)
            # 不在此 save_queue：内存此刻缺其他 in-flight 作业，
            # 交给 _run_loop 的 _save_queue_crash_safe 合并磁盘真相后统一保存。
            raise

