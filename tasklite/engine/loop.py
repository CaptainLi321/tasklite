"""主循环机器：run loop 与异常承重网。

事件驱动主循环：填池 → drain → 等待。``run_loop`` 是全部退出路径的
承重网（normal/interrupt/commit-crash/unknown 四分支清理对称 + suspend
持久化 + on_run_end 单一出口）；``run_loop_impl`` 是纯调度编排。
依赖经 RunContext 注入，经 recovery/dispatch/completion/failure 机器
协同，不反向引用 TaskLite。
"""

import logging
import time
import traceback
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import RunContext
    from .recovery import RecoveryMachine
    from .dispatch import DispatchMachine
    from .failure import FailureMachine
    from .completion import CompletionMachine

from ..exceptions import _CommitCrashSignal, _JobTerminated
from .runtime import StopMode, WORKER_RESOURCE

logger = logging.getLogger("tasklite")


class LoopRunner:
    """填池/回收/等待的主循环 + 四退出路径承重网。"""

    def __init__(
        self,
        ctx: "RunContext",
        recovery: "RecoveryMachine",
        dispatch: "DispatchMachine",
        failure: "FailureMachine",
        completion: "CompletionMachine",
    ) -> None:
        self._ctx = ctx
        self._recovery = recovery
        self._dispatch = dispatch
        self._failure = failure
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


    def run_loop_impl(self) -> None:
        """事件驱动主循环：填池 → drain → 等待。直接操作 self._ctx.state。

        ``self._ctx.in_flight`` 记录已派发到子进程、尚未 commit 的 job。
        循环直到队列空且 in-flight 空。
        """
        state = self._ctx.state
        self._ctx.in_flight.clear()
        while not state.is_empty or self._ctx.in_flight:
            # 调度缓存为 run 生命周期（_run_body
            # 加载期清空），主循环不每轮清空——阻塞/慢 job 阶段不重复反
            # 序列化全队列。
            draining = False
            if self._ctx.stop_mode is not StopMode.NONE:
                if self._ctx.stop_mode is StopMode.ABORTING:
                    # ABORTING：kill 全部 in-flight + 清理半成品输出 + requeue
                    logger.warning("Force abort requested. Killing in-flight jobs.")
                    self._recovery.abort_in_flight()
                    self._recovery.save_queue_crash_safe()
                    break
                if self._ctx.in_flight:
                    # DRAINING：等 in-flight 自然完成——本轮不派发新 job，
                    # 也不 kill，落到下方 drain 回收结果；队列中未派发的
                    # job 保留到下次 run。
                    draining = True
                else:
                    # DRAINING 且 in-flight 已空：保存队列退出
                    logger.info("Pipeline drained. Saving queue and exiting.")
                    self._recovery.save_queue_crash_safe()
                    break

            # 1. 填池：循环派发直到 __workers__ 资源耗尽或无 runnable
            # DRAINING 时跳过填池——不再派发新 job，只等 in-flight 回收。
            sched = None
            worker_wait = 0.0
            if not draining:
                while True:
                    # workers 耗尽预检——本轮不可能
                    # 有可派发 job（需 worker 槽的），跳过全队列扫描
                    # （N=10 万时每轮 ~300ms CPU 空烧，慢 job 阶段 20Hz
                    # 轮询 ~85% 单核）。no-subprocess 路径（dedup/dep-failed/
                    # no-handler）的清理延迟至 worker 释放，最终仍会处理。
                    ok, worker_wait = self._ctx.resource_mgr.can_acquire_worker(1.0)
                    if not ok:
                        # 忙循环护栏：第三方自定义资源在
                        # 不可用态可能返回 wait=0（内置 CapacityResource /
                        # RateLimitResource 恒返回 >0，不受影响）。若此处不
                        # 钳制下界，下方 `elif worker_wait > 0` 睡眠分支全部
                        # 不命中 → 无限忙循环空烧 CPU（stop 响应也下降）。
                        # 钳制为最小轮询间隔保证至少有一次睡眠。
                        if worker_wait <= 0:
                            worker_wait = 0.05
                        break
                    # in_flight 以 state 集合为事实源（与 is_known 一致）
                    in_flight_uids = state.in_flight_uids
                    sched = self._ctx.scheduler.pop_next_runnable(
                        state, in_flight_uids
                    )
                    if sched.runnable_idx is None:
                        break
                    entry = self._dispatch.dispatch_job(sched)
                    if entry is None:
                        continue  # 依赖失败/no-handler/payload 校验失败：已直接处理
                    # entry 已在 _dispatch_job 内注册到 _in_flight（避免窗口泄漏）
                    # __workers__ 资源耗尽时下一轮扫描无 runnable，自然退出

            # 处理无可运行 job 的情况
            if sched is not None and sched.runnable_idx is None:
                if state.is_empty and not self._ctx.in_flight:
                    # 队列已排空且无 in-flight：正常完成，非死锁。
                    # 走「不走子进程」路径（stale 恢复/校验失败）清空队列时，
                    # 外层 while 条件只在循环头检查，此处需显式退出，
                    # 避免空队列被误判为死锁（假日志 + 空操作 break）。
                    break
                if sched.min_wait == float('inf'):
                    # 死锁判定：仅当 in_flight 为空时才是真死锁。
                    # in_flight 非空时资源可能被释放解锁，不判死锁，落到 drain 等待。
                    if not self._ctx.in_flight:
                        should_break = self._failure.handle_deadlock(sched)
                        if should_break:
                            break
                        continue
                # min_wait 有限（资源暂不可用/backoff）或 in_flight 非空：drain 等待

            # 2. drain：非阻塞收集已完成结果
            if self._ctx.in_flight:
                # 读取 in-flight 的 suspend 信号文件，即时应用
                # （handler 崩溃/超时也不丢失限流信息——文件落盘）
                self._recovery.apply_pending_signals()
                handles = self._ctx.in_flight.active_handles()
                completed = self._ctx.channel.reap_completed(handles)
                for handle, result in completed:
                    # 先 complete 再 pop：complete 与 pop 之间被
                    # Ctrl+C/KI 打断时 entry 仍在 in_flight——abort_in_flight
                    # 会正确释放资源并 requeue（job 未 commit，at-least-once）。
                    # 若先 pop，entry 已离开 in_flight 而
                    # CapacityResource.used 要到 complete_job 内部才释放
                    # → 跨 run 泄漏 worker 槽位，管线静默活锁。
                    # pop 放 try/finally：commit 失败路径 complete_job 内
                    # commit_failed_crash 已自行 requeue 并抛
                    # _CommitCrashSignal——若不 pop，stale entry 会被 abort
                    # 二次 requeue（无 wall 记录可吸收，同一 job 双重入队）。
                    entry = self._ctx.in_flight.get(handle.uid)
                    try:
                        self._completion.complete_job(entry, result)
                    finally:
                        self._ctx.in_flight.pop(handle.uid, None)

                # 3. 无新完成且仍有 in-flight → 短轮询等待。
                # 用短间隔（50ms）而非 sched.min_wait，因为 job 可能在任意时刻
                # 完成需要及时回收；长 sleep 会抵消并发收益。
                if not completed and self._ctx.in_flight:
                    time.sleep(0.05)
            elif sched is not None and sched.runnable_idx is None and sched.min_wait != float('inf'):
                # 退避/资源等待可能遮蔽依赖环死锁——环成员处于退避时
                # min_wait 有限，主循环无限 sleep，死锁判定被推迟到退避结束
                # （指数退避可拖数十分钟）。
                # 无 in-flight 且存在等待依赖时，即使 min_wait 有限也先做环检测；
                # 确认有环才走死锁处理（只失败环成员），否则是合法依赖链尾退避，照常等待。
                if not self._ctx.in_flight and sched.waiting_for_dependency:
                    cycle_uids = state.find_dependency_cycles()
                    if cycle_uids:
                        logger.error(
                            f"Deadlock detected during backoff/wait: dependency cycle "
                            f"{sorted(set(cycle_uids))} masked by finite min_wait."
                        )
                        sched.min_wait = float('inf')
                        should_break = self._failure.handle_deadlock(sched)
                        if should_break:
                            break
                        continue
                # 无 in-flight 但需等待（backoff / 资源限流释放）：sleep(min_wait)。
                # 支持 backoff 作业的等待路径。
                # cap 1.0s 避免 backoff 时间过长时无法响应停机请求。
                time.sleep(min(sched.min_wait, 1.0))
            elif worker_wait > 0 and not self._ctx.in_flight:
                # workers 预检 break 且无 in-flight 可
                # drain——__workers__ 被 suspend（ctx.suspend_resource 或
                # 持久化恢复）或用户覆盖为 capacity<1 / RateLimitResource 时，
                # can_acquire 返回 False 且队列无在途 job：无此分支则
                # sched=None 使两个等待分支都跳过 → 无限忙循环。
                # cap 1.0s 保持停机请求响应性。
                time.sleep(min(worker_wait, 1.0))

        logger.info(f"Pipeline {self._ctx.name} finished.")
        # run 结束时统一持久化一次资源挂起状态——由 _run_loop 的
        # finally 统一执行（正常/崩溃路径一致，见 _run_loop）。
        self._ctx.in_flight.clear()

