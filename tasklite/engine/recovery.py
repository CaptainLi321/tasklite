"""恢复机器（崩溃安全保存 / suspend 信号排空 / abort 分类消费）。

``abort_in_flight`` 是异常退出与强制停机的统一收尾：先排空信号，再按
「结果文件是否已原子落盘」分类——已完成走完成机器提交（不重跑），
未完成 kill + 清半成品 + requeue（at-least-once）。依赖经 RunContext
注入、经 CompletionMachine 复用收尾契约，不反向引用 TaskLite。
"""

import logging
import math
import time
from typing import List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import RunContext
    from .completion import CompletionMachine

from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.state import uid_from_job_dict
from ..utils.jsonutil import loads
from .executor import (
    _decode_ipc_result, cleanup_ipc_files, read_result_file, read_signals,
    result_path,
)
from .inflight import InFlightJob
from .runtime import (
    META_RESOURCE_SUSPENDS,
    RT_BACKOFF_UNTIL,
    RT_BACKOFF_WALL_DEADLINE,
)

logger = logging.getLogger("tasklite")


class RecoveryMachine:
    """崩溃恢复 + 强制停机收尾 + 启动期修复。"""

    def __init__(self, ctx: "RunContext", completion: "CompletionMachine") -> None:
        self._ctx = ctx
        self._completion = completion

    def repair_queue_on_load(
        self, q_data: list, wall: dict, failed: dict
    ) -> list:
        """加载期队列整理：退避换算 + 过滤残留 + UID 去重，并执行单次原子持久化。

        1. 退避换算：以 wall_deadline 换算为单调时钟 monotonic _backoff_until。
        2. 残留过滤：过滤已在 wall/failed 中且不符合 rerun 策略的残留。
        3. UID 去重：同一 UID 重复条目保留首条。
        4. 变更持久化：若有过滤或去重发生，单次原子落盘，避免多次保存出现中间状态。
        """
        now = time.monotonic()
        wall_now = time.time()
        seen_uid: set = set()
        clean_q = []

        for jd in q_data:
            # 1. 退避换算
            rt = jd.setdefault("runtime", {})
            if not isinstance(rt, dict):
                rt = {}
                jd["runtime"] = rt
            wall_deadline = rt.get(RT_BACKOFF_WALL_DEADLINE)
            if (
                isinstance(wall_deadline, (int, float))
                and not isinstance(wall_deadline, bool)
                and math.isfinite(wall_deadline)
            ):
                if wall_now >= wall_deadline:
                    rt.pop(RT_BACKOFF_WALL_DEADLINE, None)
                    rt.pop(RT_BACKOFF_UNTIL, None)
                else:
                    rt[RT_BACKOFF_UNTIL] = now + (wall_deadline - wall_now)
            else:
                rt.pop(RT_BACKOFF_WALL_DEADLINE, None)
                rt.pop(RT_BACKOFF_UNTIL, None)

            # 2. 过滤残留
            u = uid_from_job_dict(jd)
            wall_hit = u in wall
            failed_hit = u in failed
            if wall_hit or failed_hit:
                decision = self._ctx.policy.evaluate(
                    jd,
                    wall_meta=wall.get(u),
                    is_wall=wall_hit,
                    is_failed=failed_hit,
                )
                if decision.should_skip:
                    continue


            # 3. 去重（保留首条）
            if u in seen_uid:
                logger.warning(
                    f"Loading duplicate uid {u} in queue; keeping first occurrence "
                    f"(dropping {len(clean_q)}-th)."
                )
                continue
            seen_uid.add(u)
            clean_q.append(jd)

        # 4. 单次原子落盘
        if len(clean_q) < len(q_data):
            self._ctx.backend.save_queue(clean_q)
        return clean_q

    def load_resource_suspends(self) -> None:
        """从 meta 表恢复资源 suspend 状态（换算回 monotonic 挂起时刻）。"""
        try:
            raw = self._ctx.backend.get_meta(META_RESOURCE_SUSPENDS)
        except Exception as e:
            logger.warning(f"Failed to load resource suspends from meta: {e}")
            return
        if raw is None:
            return
        try:
            deadlines = loads(raw)
        except (ValueError, TypeError) as e:
            logger.warning(f"Corrupted resource_suspends meta, ignoring: {e}")
            return
        if not isinstance(deadlines, dict):
            logger.warning("resource_suspends meta is not a dict, ignoring")
            return
        now = time.time()
        for name, deadline in deadlines.items():
            try:
                if name not in self._ctx.resources:
                    logger.warning(
                        f"Skipping persisted suspend for unknown resource '{name}'"
                    )
                    continue
                if (
                    not isinstance(deadline, (int, float))
                    or isinstance(deadline, bool)
                    or not math.isfinite(deadline)
                ):
                    logger.warning(
                        f"Skipping invalid suspend deadline for resource '{name}'"
                    )
                    continue
                remaining = deadline - now
                if remaining <= 0:
                    continue  # 已过期：放行
                self._ctx.resources[name].suspend(remaining)
            except Exception as e:
                logger.warning(
                    f"Skipping persisted suspend for resource '{name}' "
                    f"(corrupt entry {deadline!r}): {e}"
                )

    def persist_resource_suspends(self) -> None:
        """run 结束时把资源挂起截止持久化到 meta 表。

        薄转发到 RunContext.persist_resource_suspends_now——挂起的每个
        应用点即时持久化（消除 kill -9/OOM 时挂起丢失窗口）见该方法；
        本入口保留 run 收尾调用面与既有契约测试的兼容。
        """
        self._ctx.persist_resource_suspends_now()


    def save_queue_crash_safe(self) -> None:
        """崩溃路径保存队列：以「磁盘真相」合并「内存队列」，避免丢失作业。

        崩溃处理器不能盲目用内存队列全量覆盖磁盘——在以下窗口内，内存队列
        会缺失磁盘上仍存在的作业：
          - ``_dispatch_job`` 的 pop 之后、try 之前（作业既不在内存也不在 in-flight）；
          - ``_complete_job`` 的 commit 成功之后、apply 到内存之前（spawned 子任务、
            retry 重入队已在磁盘提交但内存未同步）。

        此方法重新加载磁盘队列，把「磁盘有而内存没有」的作业补回队首，
        再按 uid 去重保存（含内存自身的重复，来自异常路径的双 requeue）。
        """
        try:
            disk_q = self._ctx.backend.load_queue()
        except Exception as e:
            # load 失败不得用空列表继续覆盖——磁盘上「已
            # commit 但内存未同步」的作业会在此次保存中被永久抹除（覆盖
            # 用空 disk 基准）。磁盘至少是上次成功保存的状态，保留原样
            # 比覆盖更安全；内存丢失由 at-least-once 重跑吸收。
            logger.error(
                f"Failed to reload queue from disk for crash-safe save; "
                f"skipping overwrite to preserve disk truth: {e}"
            )
            return
        mem_q = self._ctx.state.queue
        mem_uids = {uid_from_job_dict(jd) for jd in mem_q}
        # 磁盘有而内存没有的作业（commit/pop 窗口内丢失的）补回队首
        extra = [jd for jd in disk_q if uid_from_job_dict(jd) not in mem_uids]
        if extra:
            logger.warning(
                f"Crash-safe save: recovered {len(extra)} job(s) from disk "
                f"that were missing in memory."
            )
        # 内存内按 uid 去重（异常路径可能重复 requeue 同一作业）
        seen: set = set()
        dedup_mem = []
        for jd in mem_q:
            u = uid_from_job_dict(jd)
            if u in seen:
                continue
            seen.add(u)
            dedup_mem.append(jd)
        self._ctx.backend.save_queue(extra + dedup_mem)


    def apply_pending_signals(self) -> None:
        """读取所有 in-flight job 的 suspend 信号文件，即时应用。

        handler 在子进程中调用 ``ctx.suspend_resource()`` 时，信号追加到
        ``{ipc_dir}/{uid}.signals.jsonl``（落盘 flush）。此方法在主循环
        drain 阶段读取并**删除**各 job 的信号文件（排空语义）——即使
        handler 随后崩溃/超时，落盘的限流信息也不丢失（文件仍在）。

        ``suspend()`` 使用 ``max`` 语义，重复应用同一信号是幂等的。
        """
        for entry in self._ctx.in_flight.values():
            signals = read_signals(self._ctx.ipc_dir, entry.uid)
            for r_name, secs in signals:
                if r_name in self._ctx.resources:
                    self._ctx.resources[r_name].suspend(secs)
                    logger.info(f"Applied suspend signal from {entry.uid}: {r_name} for {secs}s")
                    # 挂起应用后即时持久化——kill -9/OOM 时已应用的
                    # 限流不丢（否则重启后全速重打正在限流的 API）。幂等
                    # UPSERT，仅在确有应用动作时触发（本循环体内有应用即写，
                    # 无应用零开销）。
                    self._ctx.persist_resource_suspends_now()
                else:
                    # 纵深防御对称：未注册资源名告警跳过
                    # （入口已 fail-loud，此为结果文件/旧信号文件通道兜底）。
                    logger.warning(
                        f"Skipping suspend signal for unregistered resource "
                        f"{r_name!r} (from {entry.uid})"
                    )




    # ── 派发机器薄转发（实现见 engine/dispatch.py）──────────

    def abort_in_flight(self) -> None:
        """异常/强制停机时：kill 进行中的子进程、消费已完成的结果、requeue。

        在 ``_run_loop`` 的 except 块和 ABORTING 停机路径调用。
        kill 子进程避免泄漏 fd；清理半成品输出（与 ``_complete_job``
        finally 共用 ``_cleanup_outputs``，被 kill job 的输出不残留）；
        requeue 保证未 commit 的 job 不丢
        （磁盘 queue 本就含它们，这里同步内存状态便于 save_queue）。
        务必在 clear() 前释放已 acquire 的资源，避免资源计数器永久抬高。

        时序契约：**先 kill 子进程、再清理输出**——子进程仍可能
        正在写 declared_outputs，先清理会与写入并发交错（读到半截文件/
        删除后被重新创建）。与正常路径（``_complete_job``：先等子进程死、
        再清理）时序对齐。

        按「当前 incarnation 的结果文件是否存在」分类：
        「已完成」entry（结果文件已原子落盘、只差 drain 轮询回收）不得按
        失败处理——若走「kill + 删结果文件 + 删已成功产出的物理文件 +
        requeue」→ 重启必重跑，非幂等副作用（API POST/外发邮件）被重复执行
        （窗口「job 完成至下一次 drain ≤50ms + 本轮调度」，在受控中止路径
        确定性可命中）。结果文件存在的 entry 不 kill（进程已自然退出或
        即将退出），改走 ``_complete_job`` 伪 entry 提交（与
        ``_restore_stale_result`` 同模式）消费结果；只有无结果文件的 entry
        才执行 kill + 清半成品 + requeue。该时序只适用于进行中 entry。

        分类（读结果文件）与 kill 之间存在竞态窗口：分类读到无结果
        （归 pending）→ worker 恰在 kill 前完成 ``write_result_atomic``
        （结果文件出现）——若 kill 后立即删结果文件 + 删成功产出 +
        requeue，重启必重跑，非幂等副作用被重复执行。由「kill 后重查」
        兜底：先对全部 pending 只 kill + join（``executor.finalize_processes``，
        不删结果文件），随后**重查** pending 的结果文件——kill 后新出现的
        entry（worker 恰在分类与 kill 之间完成）移入 done 消费（走
        ``_complete_job`` 伪 entry，不删成功输出、不 requeue），与 drain()
        超时路径的「kill 后重读」（``executor._collect_outcome``）对称；
        剩余 pending 才是真正「未完成」——此刻写入已因 kill/join 停止，
        再统一 ``cleanup_ipc_files`` + ``_cleanup_outputs`` + requeue。
        时序（先 kill 再清理）不变。
        """
        if not self._ctx.in_flight:
            return
        # abort 前先消费所有 in-flight 的 suspend 信号——
        # _apply_pending_signals 是 read_signals 的唯一消费点，只在正常主循环
        # drain 阶段调用；abort 路径若直接 kill + cleanup_ipc_files（targets
        # 含 signals 文件），未被读取的 suspend 限流信息会随文件删除丢失，
        # 违反「崩溃/强制停机也要保留 429 限流状态」的已文档化承诺。在
        # 分类/kill 前统一 drain 一次（排空语义：读后删，幂等），被杀 job
        # 的挂起状态在 apply_pending_signals 内应用后即时持久化
        # （RunContext.persist_resource_suspends_now 保证即时持久化）。
        self.apply_pending_signals()
        # 分类——结果文件已落盘 = 执行已完成，只差提交。
        # 只读一次并缓存 result dict（复用消费，避免双读 TOCTOU）；损坏/
        # 非标准结果（read_result_file 返回 None 或无 "status" 键）按「未完成」
        # 处理（与 consume_stale_result 的「损坏 = 无残留」语义一致：宁可重跑，不可
        # 误判；且 _decode_ipc_result 遇无 status 的 dict 会访问 p.exitcode
        # 崩 run——分类保证进入消费的 res 必为合法结果 dict）。
        done_entries: List[Tuple[InFlightJob, dict]] = []
        pending_entries: List[InFlightJob] = []
        for entry in self._ctx.in_flight.values():
            res_path = result_path(
                entry.handle.ipc_dir, entry.uid, entry.handle.incarnation
            )
            if res_path.exists():
                res = read_result_file(res_path)
                # status=="interrupted"（worker 被 Ctrl+C/
                # SIGTERM 中断）不视为已完成——abort 语义是「全部进行中
                # job 视为未完成」，interrupted 归 pending requeue（不进
                # DLQ、不级联），与 README「仅进行中 job requeue」一致。
                if (res is not None and isinstance(res, dict) and "status" in res
                        and res.get("status") != "interrupted"):
                    done_entries.append((entry, res))
                    continue
            pending_entries.append(entry)
        # 先释放资源（务必在 clear 前）
        for entry in self._ctx.in_flight.values():
            self._completion.release_acquired(entry.acquired, uid=entry.uid)
        # 先 kill 全部**进行中**子进程（确保输出写入停止），
        # 再清理半成品输出（与 _complete_job 的正常路径时序对齐）。
        # 「已完成」entry 不 kill——结果已落盘，子进程已自然退出或即将退出。
        # **先只 kill + join、不删 IPC 文件**
        # （executor.finalize_processes）——分类（上方读结果文件判 done/
        # pending）与 kill 之间，worker 可能恰好完成 write_result_atomic：
        # 若 kill 后立即 cleanup_ipc_files 删结果文件，刚写好的成功结果被
        # 清掉 → 成功 job 被误判未完成 → requeue → 重启必重跑，非幂等
        # 副作用被重复执行。kill/join 后进程写入已停止，此刻重查结果文件
        # 才无竞态。
        handles = [entry.handle for entry in pending_entries]
        self._ctx.executor.finalize_processes(handles)
        # kill 后重查 pending 的结果文件——worker 恰在分类与 kill
        # 之间完成写结果的 entry（结果文件此刻才出现）移入 done 消费
        # （不删成功输出、不 requeue）；与 drain 超时路径的「kill 后
        # 重读」对称。
        still_pending: List[InFlightJob] = []
        for entry in pending_entries:
            res_path = result_path(
                entry.handle.ipc_dir, entry.uid, entry.handle.incarnation
            )
            if res_path.exists():
                res = read_result_file(res_path)
                # 同初查——interrupted 结果归 pending requeue
                if (res is not None and isinstance(res, dict) and "status" in res
                        and res.get("status") != "interrupted"):
                    done_entries.append((entry, res))
                    continue
            still_pending.append(entry)
        pending_entries = still_pending
        # 此刻进程已 kill/join（写入停止），对真正「未完成」的 entry
        # 统一清理 IPC 文件（结果/signals/tmp——cleanup_ipc_files 语义）
        # 与半成品输出。清理动作与 executor.cleanup 相同，但推迟到
        # 重查之后，消除 TOCTOU 窗口。
        for entry in pending_entries:
            try:
                cleanup_ipc_files(entry.handle.ipc_dir, entry.uid)
            except Exception:
                pass
        # 清理被 kill job 的半成品输出（与 _complete_job 一致）。
        # 输出从落盘 outputs.jsonl 读取（kill 前 handler 已落盘声明）。
        for entry in pending_entries:
            self._completion.cleanup_outputs(entry.uid)
        # self._ctx.state 的 queue 不含 in-flight job（已 pop），requeue 后含它们，
        # 与磁盘一致（commit 才删，它们未 commit）。requeue 到队首。
        # 注意：只 requeue 进行中的 job——「已完成」entry 的结果将被
        # _complete_job 提交到 wall/failed，不 requeue（否则重启必重跑，
        # 非幂等副作用被重复执行，避免重复提交造成副作用重复）。
        job_dicts = [entry.job_dict for entry in pending_entries]
        # requeue 前先把 pending 从 in-flight 集合注销——
        # 否则 pending 同时属于 queue（requeue 后）与 _in_flight_uids，随后
        # done entry 消费（_complete_job → unregister_in_flight）触发
        # `_assert_state_consistent` 的「queue ∩ in_flight 互斥」断言崩溃，
        # 恢复路径被替换原异常（身份非真空的集合互斥被破坏）。
        # 此处 unregister 的 pending 在 requeue 前仅在 in-flight（不在
        # queue），断言通过；requeue 后 pending 只在 queue，互斥保持。rerun
        # 任务经 spawn_jobs 重新登记 _rerun_active_uids，不泄漏豁免。
        for pentry in pending_entries:
            self._ctx.state.unregister_in_flight(pentry.uid)
        self._ctx.state.requeue_jobs(job_dicts, front=True)
        # 消费「已完成」entry 的结果——走 _complete_job 伪
        # entry 提交（acquired=[] 防二次释放、handle=None 使
        # expect_in_flight=False 跳过 DEBUG 断言），复用单一出口收尾
        # （成功进 wall / 失败进 DLQ / retry 退避重入队；失败清理与 IPC
        # 文件清理由 _complete_job finally 统一执行）。
        # commit 连续失败达阈值时 _complete_job 内层已转 _JobTerminated
        # （job 已 DLQ 终结，消费语义视为完成）；_CommitCrashSignal（未达
        # 阈值）保留到 abort 收尾后重抛——requeue 已由 _commit_failed_crash
        commit_crash: Optional[BaseException] = None
        for entry, res in done_entries:
            result = _decode_ipc_result(
                res, None, entry.job, entry.handle.ipc_dir
            )
            pseudo = InFlightJob(
                uid=entry.uid,
                job_dict=entry.job_dict,
                job=entry.job,
                acquired=[],
                handle=None,
                job_start=None,
            )
            try:
                self._completion.complete_job(pseudo, result)
            except _JobTerminated:
                # commit 连续失败达阈值 → job 已 DLQ 终结（正常终态）。
                # 与 _restore_stale_result 的消费语义一致：视为完成，继续收尾。
                pass
            except _CommitCrashSignal as e:
                # 完成 abort 收尾（clear 后续 entry）后再重抛，
                # 避免中途 raise 使 _in_flight 残留、其余 entry 不处理。
                commit_crash = e
        self._ctx.in_flight.clear()
        # 同步清空 state 的 in-flight 集合
        self._ctx.state.clear_in_flight()
        if commit_crash is not None:
            raise commit_crash
