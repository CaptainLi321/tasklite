"""失败机器：3-strike / 级联 / 死锁归因的失败处理族。

持有失败处理的七个方法——它们自成一个子语言（3-strike、级联、死锁归因），
聚到一起后「每条 DLQ 路径是否触发钩子」「是否对称」一眼可查。
保持单一出口：``fire_job_completed``（钩子出口，经 RunContext 访问）与
``_complete_job`` 伪 entry 是既有收敛点，本模块不复制出口逻辑。

设计：``FailureMachine`` **不持有宿主 pipeline 引用**，
只依赖注入的 ``RunContext``（``self._ctx``）——state/backend/stats/
fire_job_completed/scheduler/episode 态均从上下文读取，避免
FailureMachine ↔ TaskLite 双向引用。TaskLite 保留同名薄
转发方法（委托给 ``self._failure``），既有调用点不变。episode 状态
（``dep_grace_*``/``deadlock_gap_rounds``）由 RunContext 按 run 重置。

注意：``_handle_deadlock`` 返回 ``should_break`` 被主循环
消费；``_commit_failed_crash``/``_commit_bulk_failed_crash`` 抛
``_JobTerminated``/``_CommitCrashSignal`` 需穿越回 pipeline 的 except 块
——这些信号是 BaseException，跨对象传播无碍（捕获点仍在 pipeline）。
"""

import logging
import time
from typing import List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import RunContext

from ..error_codes import (
    ERR_COMMIT_FAILURE_DLQ,
    ERR_DEADLOCK_GAP as _ERR_DEADLOCK_GAP,
    ERR_DEPENDENCY_DEADLOCK as _ERR_DEPENDENCY_DEADLOCK,
    ERR_JOB_DEPENDENCY as _ERR_JOB_DEPENDENCY,
    ERR_MALFORMED_JOB as _ERR_MALFORMED_JOB,
    ERR_RESOURCE_DEADLOCK as _ERR_RESOURCE_DEADLOCK,
)
from ..exceptions import _CommitCrashSignal, _JobTerminated
from ..models.job import Job
from ..models.state import uid_from_job_dict

logger = logging.getLogger("tasklite")

# 失败机器的模块级常量。
# pipeline._dispatch_job 的 dispatch 3-strike 也读 COMMIT_FAILURE_DLQ_THRESHOLD
# pipeline 经 `from .engine.failure import ...` 引用本常量。
COMMIT_FAILURE_DLQ_THRESHOLD = 3
DEP_GRACE_SECONDS = 60.0
DEADLOCK_GAP_MAX_ROUNDS = 5


class FailureMachine:
    """失败处理族：3-strike 崩溃契约 / 级联 / 死锁归因 / 宽限。

    依赖**注入的 RunContext**（``self._ctx``）而非宿主 pipeline——
    ``state`` 在 run() 时才装载，backend/scheduler/hooks/episode 态均为
    上下文常驻属性。
    """

    def __init__(self, ctx: "RunContext") -> None:
        self._ctx = ctx

    def _requeue_and_crash(self, uid: str, job_dict: dict, reason: str) -> None:
        """单一出口：所有「commit 失败 → requeue 内存 + 崩溃」路径的收敛点。

        3-strike 的 DLQ 分支（``_commit_failed_crash``）与清理型终态
        （``_commit_skip_crash``）最终都走到这里。前置条件：job 已不在
        in-flight（调用方先 unregister），否则 ``_abort_in_flight`` 会二次
        requeue 同一 uid。
        """
        self._ctx.state.unregister_in_flight(uid)
        self._ctx.state.requeue_jobs([job_dict], front=True)
        raise _CommitCrashSignal(
            f"Backend commit returned False for {uid} ({reason}). "
            f"On-disk queue preserved; crashing to avoid unbounded retry loop."
        )

    def commit_skip_crash(self, uid: str, job_dict: dict) -> None:
        """``commit_skip`` 失败时的终态：**只 requeue + 崩溃，绝不写 DLQ**。

        状态模型边界：skip 命中意味着 uid 已有一个成功/失败终态
        （wall 或 failed），本次只是清理磁盘队列里的残留条目——「清理失败」
        是环境故障，不是业务失败。若复用 ``_commit_failed_crash`` 的 3-strike，
        第 3 次会把 wall 里的成功记录翻转成 DLQ 失败（从未重跑过的任务被
        判死），违反「最终状态唯一 + 终态不可伪造」。正确的收敛是回到
        「wall∩queue 残留」这一可修复的崩溃前状态，由下次 ``_run_body``
        的加载期过滤或下次 ``commit_skip`` 重试消化。
        """
        self._requeue_and_crash(uid, job_dict, "commit_skip")

    def commit_failed_crash(self, uid: str, reason: str, job_dict: dict) -> None:
        """commit 返回 False 时：requeue 保持内存一致，然后崩溃。"""
        self._ctx.store.commit_failed_crash(uid, reason, job_dict)

    def mark_failed(self, uid: str, meta: dict) -> None:
        """统一失败登记：清 wall 旧记录 + mark_failed。"""
        self._ctx.state.mark_failed(uid, meta)

    def apply_failed(self, uid: str, meta: dict, *, unregister: bool = True,
                     count_as: str = "failed") -> None:
        """失败登记的内存尾段。"""
        self._ctx.store.apply_failed(uid, meta, unregister=unregister)
        self._ctx.stats[count_as] += 1

    def commit_bulk_failed_crash(
        self, reason: str, uids_metas: List[Tuple[str, dict]], queue_job_dicts: List[dict]
    ) -> Tuple[List[dict], bool]:
        """bulk commit 失败的 3-strike 处理。"""
        return self._ctx.store.commit_bulk_failed_crash(reason, uids_metas, queue_job_dicts)

    def cascade_fail(self, failed_uid: str) -> None:
        """父 job 失败后 O(1) 级联标记全部下游为依赖失败。"""
        cascaded = self._ctx.store.cascade_fail(failed_uid)
        if cascaded:
            self._ctx.stats["cascade_failed"] += len(cascaded)

    def _dependency_grace(self, missing_indices) -> bool:
        """宽限：缺失依赖的 job 是否应等待而非立即 DLQ。

        宽限条件：队列中存在**可运行的候选 job**（依赖全部在 wall 或
        无依赖）——它一旦运行可能 spawn 出缺失的依赖（如转码项目的 scan
        driver）。仅当全部 job 都在等缺失依赖、或其余 job 都是等待者
        （依赖链尾，如「依赖缺失者的下游」）时才判死锁。
        防无限等待：宽限总时长上限（``DEP_GRACE_SECONDS``，monotonic），
        超时后不再宽限（DLQ + 醒目日志）。返回 True = 宽限，False = 判死锁。
        """
        missing_set = set(missing_indices)
        state = self._ctx.state
        # 宽限截止按 episode 重置——episode 判定必须用
        # **uid 集合**而非索引集合：索引随队列位移变化（dispatch pop 前面的
        # job 会前移后续索引），同索引不同身份会误判同 episode（B 组缺失 job
        # 恰好占据 A 组解决后的同位置 → 集合相同 → 不重置 → 复用 A 组已
        # 过期 deadline → 零宽限立即 DLQ）。uid 是稳定身份键。
        missing_uids: set = set()
        for i in missing_indices:
            if 0 <= i < len(state.queue):
                try:
                    missing_uids.add(Job.from_dict(state.queue[i]).uid)
                except (KeyError, TypeError, ValueError):
                    pass  # 畸形条目由 malformed 分支处理，此处跳过
        if self._ctx.dep_grace_missing is not None and self._ctx.dep_grace_missing != missing_uids:
            self._ctx.dep_grace_deadline = None
        self._ctx.dep_grace_missing = frozenset(missing_uids)
        for i, jd in enumerate(state.queue):
            if i in missing_set:
                continue
            try:
                # （性能优化）：复用调度器内容键缓存而非裸
                # Job.from_dict——死锁宽限阶段（min_wait=inf，全队列无
                # 可运行）每轮两次全量反序列化（N=10 万 ≈ 240ms/轮）
                job = self._ctx.scheduler.cached_job(jd)
            except (KeyError, TypeError, ValueError):
                continue
            if all(dep in state.wall for dep in job.depends_on):
                # 存在可运行候选（潜在 spawner）→ 宽限，等它 spawn 出依赖
                now = time.monotonic()
                if self._ctx.dep_grace_deadline is None:
                    self._ctx.dep_grace_deadline = now + self._ctx.dep_grace_seconds
                    logger.warning(
                        f"DEPENDENCY GRACE: {len(missing_indices)} job(s) waiting "
                        f"on missing deps; granting {self._ctx.dep_grace_seconds}s "
                        f"grace (runnable job(s) may spawn them)."
                    )
                if now < self._ctx.dep_grace_deadline:
                    # 防忙循环：宽限等待期间给主循环喘息（候选 job 可能在退避/等资源）
                    time.sleep(0.5)
                    return True
                logger.error(
                    f"DEPENDENCY GRACE EXPIRED: {len(missing_indices)} job(s) "
                    f"still waiting on missing deps after "
                    f"{self._ctx.dep_grace_seconds}s; treating as deadlock (DLQ)."
                )
                return False
        # 无可运行候选（其余都是等待者/死锁类）→ 真死锁，无宽限
        return False

    def _deadlock_gap_or_escalate(self, log_prefix: str) -> bool:
        """死锁分类缺口（环检测空 / 不可归因）的连续轮次升级逻辑。

        两处保守兜底（waiting_for_dependency 无环 / 无已知根因）共用——
        保守动作在冻结状态上「重试下一轮」是空的（队列/in-flight
        不变 → 分类结果必逐位相同），会永久挂起 + 日志刷屏。加连续轮次计数
        （``_deadlock_gap_rounds``），达阈值升级整队列 DLQ（专属错误码
        ``_ERR_DEADLOCK_GAP``），恢复终止性。

        返回 True = 已升级（调用方应构造整队列 uids_metas 走 bulk DLQ）；
        返回 False = 未达阈值（调用方应 return False 退避重试）。
        """
        self._ctx.deadlock_gap_rounds += 1
        if self._ctx.deadlock_gap_rounds < self._ctx.deadlock_gap_max_rounds:
            logger.error(
                f"{log_prefix}: refusing to fail the whole queue, retrying "
                f"next round "
                f"({self._ctx.deadlock_gap_rounds}/{self._ctx.deadlock_gap_max_rounds})."
            )
            time.sleep(0.5)
            return False
        logger.critical(
            f"{log_prefix} persisted for {self._ctx.deadlock_gap_max_rounds} rounds; "
            f"escalating to whole-queue DLQ ({_ERR_DEADLOCK_GAP})."
        )
        return True

    @staticmethod
    def _split_deadlock(
        queue: List[dict],
        error: str,
        *,
        extract_uid,
        include,
    ) -> Tuple[List[Tuple[str, dict]], List[dict]]:
        """把队列拆分为「进 DLQ 的肇事者」与「保留的剩余队列」。"""
        uids_metas = []
        remaining_queue = []
        for idx, jd in enumerate(queue):
            uid = extract_uid(jd)
            if include(idx, uid):
                uids_metas.append((uid, {"error": error, "root_cause": True}))
            else:
                remaining_queue.append(jd)
        return uids_metas, remaining_queue

    def handle_deadlock(self, sched) -> bool:
        """处理死锁：细粒度归因 + bulk_failure + cascade。原位操作 self._ctx.state。

        返回 should_break。True 表示终态（commit 失败或剩余队列空），主循环应退出。
        """
        state = self._ctx.state
        if sched.malformed_indices:
            # 畸形 job dict 优先处理 — 无法反序列化的 job 直接入 DLQ
            logger.error(f"Deadlock: {len(sched.malformed_indices)} job(s) have malformed dict (unparseable).")
            root = set(sched.malformed_indices)
            uids_metas, remaining_queue = self._split_deadlock(
                list(state.queue),
                _ERR_MALFORMED_JOB,
                extract_uid=uid_from_job_dict,
                include=lambda idx, uid, root=root: idx in root,
            )
        elif sched.unknown_resource_indices:
            logger.error(f"Deadlock: {len(sched.unknown_resource_indices)} job(s) reference unknown resource(s).")
            root = set(sched.unknown_resource_indices)
            uids_metas, remaining_queue = self._split_deadlock(
                list(state.queue),
                _ERR_RESOURCE_DEADLOCK,
                extract_uid=lambda jd: Job.from_dict(jd).uid,
                include=lambda idx, uid, root=root: idx in root,
            )
        elif sched.missing_dependency_indices:
            # 宽限语义：缺失依赖的 job——若队列中还有
            # 其他 job（可能是 spawner，未来会 spawn 出依赖）→ 宽限等待
            # 而非立即 DLQ；仅当全部 job 都在等缺失依赖或宽限超时才判死锁。
            if self._dependency_grace(sched.missing_dependency_indices):
                return False  # 宽限中：主循环继续（等 spawner 产出依赖）
            logger.error(f"Deadlock: {len(sched.missing_dependency_indices)} job(s) have unresolvable (missing) dependencies.")
            root = set(sched.missing_dependency_indices)
            uids_metas, remaining_queue = self._split_deadlock(
                list(state.queue),
                _ERR_DEPENDENCY_DEADLOCK,
                extract_uid=lambda jd: Job.from_dict(jd).uid,
                include=lambda idx, uid, root=root: idx in root,
            )
        elif sched.impossible_resource_indices:
            # 不可达资源（amount > capacity）→ 只失败肇事者，其他 job 继续。
            # 置于 waiting_for_dependency（依赖环）之前：资源量不可达是确定的
            # 根因，比"依赖环"归因更准确，且只失败肇事者、让被阻断者走 cascade。
            logger.error(f"Deadlock: {len(sched.impossible_resource_indices)} job(s) request impossible resource amounts (exceeds capacity).")
            root = set(sched.impossible_resource_indices)
            uids_metas, remaining_queue = self._split_deadlock(
                list(state.queue),
                _ERR_RESOURCE_DEADLOCK,
                extract_uid=lambda jd: Job.from_dict(jd).uid,
                include=lambda idx, uid, root=root: idx in root,
            )
        elif sched.waiting_for_dependency:
            # 依赖全在队列中但无法运行 → 依赖环。
            # find_dependency_cycles 精确定位环内成员，只失败环内；环外
            # job 保留（其依赖若在环内，会经 fail_cascade 在环成员 commit
            # 失败后被级联标记）——整队列标 DEPENDENCY_DEADLOCK 会连带
            # 杀死环外无关 job。
            cycle_uids = set(state.find_dependency_cycles())
            if not cycle_uids:
                # 理论不可达（waiting_for_dependency 意味着
                # 依赖在队列/在途但无法运行；in-flight 为空时依赖必在队列 →
                # 图算法应找到环）。保守动作：记录 + 短退避，下一轮再判，
                # 绝不整队列清空。保守动作在冻结状态上「重试下一轮」是空的
                # （队列/in-flight 不变 → 分类结果必逐位相同），会永久挂起 +
                # 日志刷屏。加连续轮次计数：达阈值升级为整队列 DLQ（专属
                # 错误码），恢复终止性——保留「首轮不误杀」意图，又不让未知
                # 死锁变成僵尸进程。（与不可归因分支共用 gap 升级逻辑）
                escalated = self._deadlock_gap_or_escalate(
                    "Deadlock classification gap (waiting_for_dependency without cycle)"
                )
                if not escalated:
                    return False
                uids_metas = [
                    (Job.from_dict(jd).uid,
                     {"error": _ERR_DEADLOCK_GAP, "root_cause": True})
                    for jd in state.queue
                ]
                remaining_queue = []
            else:
                logger.error(
                    f"Deadlock detected: dependency cycle among {len(cycle_uids)} job(s): "
                    f"{sorted(cycle_uids)}"
                )
                uids_metas, remaining_queue = self._split_deadlock(
                    list(state.queue),
                    _ERR_DEPENDENCY_DEADLOCK,
                    extract_uid=lambda jd: Job.from_dict(jd).uid,
                    include=lambda idx, uid, roots=cycle_uids: uid in roots,
                )
        else:
            # 理论不可达（scheduler 的分类链应覆盖全部死锁
            # 归因；未来新增归因类别若漏进分类链，整队列 DLQ 会误杀全部
            # 在途任务）。保守动作：记录 + 短退避，下一轮再判。同环空分支
            # ——连续轮次计数，达阈值升级为整队列 DLQ（专属错误码），
            # 防永久挂起。（与环空分支共用 gap 升级逻辑）
            escalated = self._deadlock_gap_or_escalate(
                "Deadlock: unclassifiable deadlock (no known root cause)"
            )
            if not escalated:
                return False
            uids_metas = [
                (Job.from_dict(jd).uid,
                 {"error": _ERR_DEADLOCK_GAP, "root_cause": True})
                for jd in state.queue
            ]
            remaining_queue = []

        committed = self._ctx.backend.commit_bulk_failure(uids_metas)
        if committed:
            # 成功分类并 DLQ 落地 → 重置分类缺口计数（保守分支未达
            # 阈值时保持累计，达阈值升级后 run 随即终止，计数随 run 重建）。
            self._ctx.deadlock_gap_rounds = 0
            for uid, meta in uids_metas:
                # 死锁批量：uid 在队列中，豁免集合由下方 replace_queue 重建
                self.apply_failed(uid, meta, unregister=False)
                # 死锁批量 DLQ 终态触发钩子——
                # on_job_completed 承诺「每个 job 终结」应覆盖批量终态。
                self._ctx.fire_job_completed(uid, meta, False, False)
            state.replace_queue(remaining_queue)
            return not remaining_queue
        # 3-strike：逐 job 计数，达阈值转 DLQ，未达保留。
        # 计数持久化于 job_dict（_commit_failures），重启后继续累计——
        # 持久性 DB 故障下不无限崩溃循环。
        queue, kept = self.commit_bulk_failed_crash(
            "commit_bulk_failure", uids_metas, list(state.queue)
        )
        state.replace_queue(queue)
        if kept:
            raise _CommitCrashSignal(
                f"Backend commit_bulk_failure returned False for "
                f"{len(uids_metas)} deadlock job(s); {len(queue)} kept in queue "
                f"with incremented _commit_failures (3-strike will DLQ them). "
                f"Crashing to retry; on-disk queue preserved."
            )
        # 全部死锁 job 已达阈值且单条 DLQ 成功 → 终局，不崩溃
        return not queue

