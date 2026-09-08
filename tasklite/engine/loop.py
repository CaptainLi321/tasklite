"""主循环机器：run loop 与异常承重网。

事件驱动主循环：填池 → drain → 等待。``run_loop`` 是全部退出路径的
承重网（normal/interrupt/commit-crash/unknown 四分支清理对称 + suspend
持久化 + on_run_end 单一出口）；``run_loop_impl`` 是纯调度编排。
依赖经 RunContext 注入，经 recovery/dispatch/completion/failure 机器
协同，不反向引用 TaskLite。
"""
from __future__ import annotations

import logging
import time
import traceback
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import RunContext
    from .recovery import RecoveryMachine
    from .dispatch import DispatchMachine
    from .completion import CompletionMachine

from ..exceptions import _CommitCrashSignal, _JobTerminated
from .runtime import StepOutcome, StopMode, WORKER_RESOURCE

logger = logging.getLogger("tasklite")


class LoopRunner:
    """填池/回收/等待的主循环 + 四退出路径承重网。"""

    def __init__(
        self,
        ctx: "RunContext",
        recovery: "RecoveryMachine",
        dispatch: "DispatchMachine",
        completion: "CompletionMachine",
    ) -> None:
        self._ctx = ctx
        self._recovery = recovery
        self._dispatch = dispatch
        self._completion = completion

    def run_loop(self) -> None:
        """Run loop with exception handling.

        异常退出时先 ``_abort_in_flight`` kill 所有 in-flight 子进程并 requeue
        未 commit 的 job，再 ``_save_queue_crash_safe``（以磁盘为基准合并内存，
        避免 commit/pop 窗口内丢失作业），然后 re-raise。

        兜底契约：``_JobTerminated`` 逃逸至此属承重网
        路径（正常路径已被 _dispatch_job/_complete_job 内层承接，本分支实际
        不可达）——但**同样**先 abort + save 再 re-raise，与其他三分支对称：
        逃逸意味着契约被破坏，in-flight 子进程/内存队列不可信，必须先清理。
        只有 ``_CommitCrashSignal`` 与未知异常走相同的崩溃契约。
        """
        # on_run_end 覆盖全部退出路径——正常/中断/崩溃统一
        # 在 finally 触发，签名带 exit_reason（监控最不能丢的是崩溃事件）。
        exit_reason = "completed"
        try:
            self.run_loop_impl()
            # stop 请求的退出不是「completed」——监控/
            # on_run_end 消费方据此区分自然排空与人为停机（draining=等完
            # 在途后退出；aborting=强杀在途立即退出）。
            if self._ctx.stop_mode is StopMode.ABORTING:
                exit_reason = "stopped_aborting"
            elif self._ctx.stop_mode is StopMode.DRAINING:
                exit_reason = "stopped_draining"
        except _JobTerminated as e:
            # 正常路径内层已捕获（_dispatch_job/
            # _complete_job 各自 `except _JobTerminated` 承接，job 已终结 →
            # 返回 None，主循环继续），本分支**实际不可达**，属防御性
            # fail-loud 承重网：若未来改动新增 `_commit_failed_crash` 直调点
            # 而漏包 except，`_JobTerminated` 逃逸至此**上抛暴露**（run
            # 崩溃可见）而非静默吞掉（静默会掩盖类漏洞——已 DLQ 的
            # job 被 except Exception requeue 复发）。
            # 与其他三分支对称执行清理——逃逸
            # 时 in-flight 子进程仍在跑（资源占用/副作用未释放），必须先
            # abort + save 再上抛，避免「崩溃可见但资源泄漏 + 内存队列丢失」。
            logger.critical(f"Job terminated outside expected handlers: {e}")
            exit_reason = "error"
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted by user.")
            exit_reason = "interrupted"
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        except _CommitCrashSignal as e:
            # commit 失败崩溃契约（设计内路径）：清理 in-flight 后按序退出。
            logger.critical(f"Backend commit failure; aborting in-flight jobs: {e}")
            exit_reason = "error"
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        except Exception as e:
            logger.critical(f"Pipeline scheduler crashed with unhandled exception: {e}\n{traceback.format_exc()}")
            exit_reason = "error"
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        except BaseException as e:
            # 兜底分支——非 KI 的 BaseException（handler/
            # 钩子主动 raise SystemExit、自定义取消异常等）逃逸时同样执行
            # 清理契约（abort + crash-safe save），不留泄漏子进程/内存队列
            # 丢失；不吞异常，清理后 re-raise 保持崩溃可见。
            logger.critical(
                f"Pipeline terminated by {type(e).__name__}: {e}"
            )
            exit_reason = "error"
            self._recovery.abort_in_flight()
            self._recovery.save_queue_crash_safe()
            raise
        finally:
            # 单一出口：资源挂起持久化挂统一 finally——正常退出、
            # KeyboardInterrupt、_CommitCrashSignal、未知异常、BaseException
            # 兜底五路径一致持久化。
            # 崩溃路径若不在此持久化，429
            # 限流状态丢失 → 重启后恢复对 API 的猛打。
            # 自身 try/except + warning，不遮蔽原异常（原异常由上面 re-raise）。
            try:
                self._recovery.persist_resource_suspends()
            except Exception as e:
                logger.warning(f"Failed to persist resource suspends: {e}")
            # on_run_end 同 finally 触发（含崩溃路径），
            # 异常隔离——钩子按不可信代码对待，不遮蔽原异常。
            self._ctx.fire_run_end(exit_reason)

    def step(self, max_dispatch: Optional[int] = None) -> StepOutcome:
        """非阻塞单步推进事件泵（主循环与单步测试共用的统一事件泵）。"""
        store = self._ctx.store
        if store is None:
            return StepOutcome(
                dispatched_count=0,
                completed_count=0,
                is_idle=True,
                should_wait=False,
                wait_time=0.0,
                deadlock_detected=False,
                stop_mode=self._ctx.stop_mode,
                should_terminate=True,
                exit_reason="completed",
            )

        # 0. 停机模式检查
        draining = False
        if self._ctx.stop_mode is not StopMode.NONE:
            if self._ctx.stop_mode is StopMode.ABORTING:
                logger.warning("Force abort requested. Killing in-flight jobs.")
                self._recovery.abort_in_flight()
                self._recovery.save_queue_crash_safe()
                return StepOutcome(
                    dispatched_count=0,
                    completed_count=0,
                    is_idle=True,
                    should_wait=False,
                    wait_time=0.0,
                    deadlock_detected=False,
                    stop_mode=self._ctx.stop_mode,
                    should_terminate=True,
                    exit_reason="stopped_aborting",
                )
            if self._ctx.in_flight:
                draining = True
            else:
                logger.info("Pipeline drained. Saving queue and exiting.")
                self._recovery.save_queue_crash_safe()
                return StepOutcome(
                    dispatched_count=0,
                    completed_count=0,
                    is_idle=True,
                    should_wait=False,
                    wait_time=0.0,
                    deadlock_detected=False,
                    stop_mode=self._ctx.stop_mode,
                    should_terminate=True,
                    exit_reason="stopped_draining",
                )

        if store.is_empty and not self._ctx.in_flight:
            return StepOutcome(
                dispatched_count=0,
                completed_count=0,
                is_idle=True,
                should_wait=False,
                wait_time=0.0,
                deadlock_detected=False,
                stop_mode=self._ctx.stop_mode,
                should_terminate=True,
                exit_reason="completed",
            )

        # 1. 填池派发（仅非 DRAINING 状态且未超过单步限制）
        last_outcome: Optional[Any] = None
        worker_wait = 0.0
        dispatched = 0
        limit = max_dispatch if max_dispatch is not None else 1000000
        if not draining:
            while dispatched < limit:
                outcome = self._dispatch.dispatch_next()
                last_outcome = outcome
                if outcome.worker_wait > 0:
                    worker_wait = outcome.worker_wait
                if outcome.entry is not None:
                    dispatched += 1
                    continue
                if not outcome.should_continue:
                    break

        # 2. 处理无可运行 job 与死锁判定
        deadlock_detected = False
        should_terminate = False
        if last_outcome is not None and not last_outcome.has_runnable:
            if store.is_empty and not self._ctx.in_flight:
                should_terminate = True
            elif last_outcome.min_wait == float("inf"):
                if not self._ctx.in_flight:
                    deadlock_detected = True
                    should_break = self._ctx.store.handle_deadlock(last_outcome, ctx=self._ctx)
                    if should_break:
                        should_terminate = True

        # 3. Drain 回收在途结果
        completed_count = 0
        if self._ctx.in_flight:
            self._recovery.apply_pending_signals()
            handles = self._ctx.in_flight.active_handles()
            completed = self._ctx.channel.reap_completed(handles)
            completed_count = len(completed)
            for handle, result in completed:
                entry = self._ctx.in_flight.get(handle.uid)
                try:
                    self._completion.complete_job(entry, result)
                finally:
                    self._ctx.in_flight.pop(handle.uid, None)

        # 4. 计算等待时延与空闲状态
        is_idle = store.is_empty and not self._ctx.in_flight
        if is_idle or should_terminate:
            wait_time = 0.0
            should_wait = False
        elif self._ctx.in_flight:
            if completed_count == 0:
                wait_time = 0.05
                should_wait = True
            else:
                wait_time = 0.0
                should_wait = False
        elif last_outcome is not None and not last_outcome.has_runnable and last_outcome.min_wait != float("inf"):
            if not self._ctx.in_flight and last_outcome.waiting_for_dependency:
                cycle_uids = store.find_dependency_cycles()
                if cycle_uids:
                    logger.error(
                        f"Deadlock detected during backoff/wait: dependency cycle "
                        f"{sorted(set(cycle_uids))} masked by finite min_wait."
                    )
                    deadlock_detected = True
                    should_break = self._ctx.store.handle_deadlock(last_outcome, ctx=self._ctx)
                    if should_break:
                        should_terminate = True
                        wait_time = 0.0
                        should_wait = False
                    else:
                        wait_time = 0.0
                        should_wait = False
                else:
                    wait_time = min(last_outcome.min_wait, 1.0)
                    should_wait = True
            else:
                wait_time = min(last_outcome.min_wait, 1.0)
                should_wait = True
        elif worker_wait > 0 and not self._ctx.in_flight:
            wait_time = min(worker_wait, 1.0)
            should_wait = True
        else:
            wait_time = 0.0
            should_wait = not is_idle and dispatched == 0 and completed_count == 0

        exit_reason = None
        if is_idle or should_terminate:
            if self._ctx.stop_mode is StopMode.ABORTING:
                exit_reason = "stopped_aborting"
            elif self._ctx.stop_mode is StopMode.DRAINING:
                exit_reason = "stopped_draining"
            else:
                exit_reason = "completed"

        return StepOutcome(
            dispatched_count=dispatched,
            completed_count=completed_count,
            is_idle=is_idle,
            should_wait=should_wait,
            wait_time=wait_time,
            deadlock_detected=deadlock_detected,
            stop_mode=self._ctx.stop_mode,
            should_terminate=should_terminate or is_idle,
            exit_reason=exit_reason,
        )

    def run_loop_impl(self) -> None:
        """事件驱动主循环：以 step() 统一驱动填池、回收与等待。"""
        store = self._ctx.store
        self._ctx.in_flight.clear()
        while not store.is_empty or self._ctx.in_flight:
            outcome = self.step()
            if outcome.should_terminate:
                break
            if outcome.should_wait and outcome.wait_time > 0:
                time.sleep(outcome.wait_time)

        logger.info(f"Pipeline {self._ctx.name} finished.")
        self._ctx.in_flight.clear()

