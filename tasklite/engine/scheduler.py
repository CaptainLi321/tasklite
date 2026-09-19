"""Job scheduler for tasklite.

The scheduler performs a read-only scan over the queue to find the next
runnable job. It does NOT acquire resources (only ``can_acquire``); the
actual acquisition happens in the pipeline after the job is popped.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from .types import HandlerEntry

from .resource import Resource, ResourceManager, ResourceEvaluation
from ..models.job import Job, JobRuntimeState
from ..models.state import PipelineState, uid_from_job_dict

logger = logging.getLogger("tasklite")


@dataclass(frozen=True)
class JobFacts:
    """不可变调度投影（scheduler 缓存唯一持有的 Job 视图）。

    调度扫描只读四个事实：uid / task_type / resources / depends_on。
    frozen dataclass + tuple/tuple 让 `_job_cache` 命中返回的对象无法被
    任何调用点原地改写——「缓存不透出可变对象」由类型直接保证。
    """

    uid: str
    task_type: str
    resources: tuple[tuple[str, float], ...]
    depends_on: tuple[str, ...]

    @classmethod
    def from_job_dict(cls, job_dict: dict) -> "JobFacts":
        """完整走 Job.from_dict 校验，然后冻结调度所需字段。"""
        job = Job.from_dict(job_dict)
        return cls(
            uid=job.uid,
            task_type=job.task_type,
            resources=tuple(sorted(job.resources.items())),
            depends_on=tuple(job.depends_on),
        )

    def resource_map(self) -> dict[str, float]:
        """解冻 resources 供 can_acquire/merge 使用（只读消费）。"""
        return dict(self.resources)


@dataclass(frozen=True)
class DeadlockAttribution:
    """不可变死锁归因值对象，记录导致死锁的作业 UID 分类（解耦队列整型下标）。"""
    unknown_resource_uids: tuple[str, ...] = ()
    missing_dependency_uids: tuple[str, ...] = ()
    malformed_uids: tuple[str, ...] = ()
    impossible_resource_uids: tuple[str, ...] = ()

    @property
    def has_deadlock_causes(self) -> bool:
        """是否存在死锁归因原因。"""
        return bool(
            self.unknown_resource_uids
            or self.missing_dependency_uids
            or self.malformed_uids
            or self.impossible_resource_uids
        )


@dataclass(frozen=True)
class StandstillFacts:
    """停摆投影值对象——死锁仲裁输入的唯一形状（LoopFacts 模式）。

    只携带仲裁评估所需事实（min_wait / 依赖等待 / 潜在派生者 / 归因），
    不含派发侧字段；DispatchOutcome 构造期从 ScheduleResult 一次性投影，
    governor 不再鸭子解包多态入参形状。等待决策仍归 pacing.decide_wait，
    本对象只承载事实不产出决策。
    """

    min_wait: float = float("inf")
    waiting_for_dependency: bool = False
    has_potential_spawners: bool = False
    attribution: DeadlockAttribution = field(default_factory=DeadlockAttribution)


@dataclass
class ScheduleResult:
    """Outcome of a read-only scan over the queue.

    ``kind`` 显式表达 runnable_idx + pending_dep_failure + dep_failed_idx
    的兜底关系：

    - ``kind == "runnable"``：runnable_idx 指向真正可运行的 job；
    - ``kind == "dep_failed"``：runnable_idx 指向 dep-failed 兜底位置
      （pending_dep_failure 携带失败依赖，_dispatch_job 走依赖失败分支）；
    - ``kind == "none"``：无可运行 job（本轮无 job 可派发）。
    """
    runnable_idx: int | None = None
    pending_dep_failure: str | None = None
    min_wait: float = float('inf')
    waiting_for_dependency: bool = False
    has_potential_spawners: bool = False
    # 结果类型（runnable / dep_failed / none）
    kind: str = "none"
    # 死锁归因值对象（UID 集合）
    attribution: DeadlockAttribution = field(default_factory=DeadlockAttribution)
    candidate_uid: str | None = None

    @property
    def is_runnable(self) -> bool:
        """是否选出了真正可运行的作业。"""
        return self.kind == "runnable"

    @property
    def is_dep_failed(self) -> bool:
        """是否选出了依赖已失败待拒绝的作业。"""
        return self.kind == "dep_failed"

    @property
    def has_candidate(self) -> bool:
        """是否存在选中的候选作业下标。"""
        return self.runnable_idx is not None

    @property
    def unknown_resource_uids(self) -> tuple[str, ...]:
        return self.attribution.unknown_resource_uids

    @property
    def missing_dependency_uids(self) -> tuple[str, ...]:
        return self.attribution.missing_dependency_uids

    @property
    def malformed_uids(self) -> tuple[str, ...]:
        return self.attribution.malformed_uids

    @property
    def impossible_resource_uids(self) -> tuple[str, ...]:
        return self.attribution.impossible_resource_uids

    def standstill_facts(self) -> StandstillFacts:
        """投影为 governor 仲裁输入的停摆事实（剔除派发侧字段）。"""
        return StandstillFacts(
            min_wait=self.min_wait,
            waiting_for_dependency=self.waiting_for_dependency,
            has_potential_spawners=self.has_potential_spawners,
            attribution=self.attribution,
        )


class JobScheduler:
    """Read-only scanner that locates the next runnable job in the queue."""

    resources: dict[str, Resource] | ResourceManager
    handlers: Mapping[str, HandlerEntry] | dict[str, HandlerEntry]

    def __init__(
        self,
        resources: dict[str, "Resource"] | ResourceManager,
        handlers: Mapping[str, HandlerEntry] | None = None,
    ):
        if isinstance(resources, ResourceManager):
            self.resource_mgr = resources
            self.resources = resources
        else:
            self.resources = resources
            self.resource_mgr = ResourceManager(resources, handlers=handlers)
        self.handlers = handlers if handlers is not None else getattr(self.resource_mgr, "handlers", {})
        self._job_cache: dict[tuple[str, str], JobFacts] = {}
        self._JOB_CACHE_MAX = 100_000

    def begin_round(self) -> None:
        """清空调度缓存（**run 生命周期**，由 ``_run_body`` 加载期调用）。

        内容键 + 只读字段（depends_on/task_type/resources/uid）
        使缓存跨轮安全：retries/runtime/backoff 从 live job_dict 读取，
        retry 路径用 retry_dict["resources"] 恢复原始资源与缓存一致，
        spawn 去重防同 uid 冲突——阻塞/退避/慢 job 阶段主循环 ~20Hz 轮询
        时避免每轮全量反序列化队列（N=10 万 ≈ 240ms/轮 CPU 空烧）。
        跨 run 陈旧（clear_history + 重新 enqueue 同 uid 不同内容）由
        加载期清空覆盖。
        """
        self._job_cache.clear()

    def cached_job(self, job_dict: dict) -> JobFacts:
        """返回 job_dict 的不可变调度投影（内容键缓存复用）。

        公开方法：失败机器的依赖宽限路径跨模块复用同一缓存（免二次
        全量反序列化），调度扫描本身也是唯一内部消费方。
        """
        task_type = job_dict.get("task_type")
        job_id = job_dict.get("job_id")
        if task_type is None or job_id is None:
            # 畸形 dict（缺 task_type/job_id）无法用内容键——直接解析不缓存
            return JobFacts.from_job_dict(job_dict)
        key = (task_type, job_id)
        facts = self._job_cache.get(key)
        # 缓存一致性校验：缓存命中时校验「调度只读字段」一致——rerun="every_run"
        # 的 job 在 wall 命中时被 spawn 去重豁免（文档化「每会话重扫」语义），
        # 可在**同一 run 内**让同 uid 以不同 resources/depends_on 二次入队。
        # 若直接返回陈旧缓存，调度器（缓存投影）与派发器（Job.from_dict 重新
        # 解析的 fresh Job）看到不同字段 → 未知资源/依赖判定背离，以裸 KeyError
        # 击穿整条 run。字段不一致视为 miss 重新解析；同 uid 同内容
        # （retry/常规重跑/requeue）仍命中缓存，保留缓存复用的性能收益。
        if facts is not None and self._sched_fields_match(facts, job_dict):
            return facts
        facts = JobFacts.from_job_dict(job_dict)
        if len(self._job_cache) >= self._JOB_CACHE_MAX:
            self._job_cache.clear()
        self._job_cache[key] = facts
        return facts

    @staticmethod
    def _sched_fields_match(job, job_dict: dict) -> bool:
        """校验缓存投影（或 Job）与 job_dict 的调度只读字段一致。

        ``job`` 接受 ``JobFacts``（生产缓存路径）或 ``Job``（测试直调）——
        两者都只读 depends_on/resources；``dict(...)`` 归一化让
        ``tuple[(k,v),...]``（JobFacts）与 ``dict``（Job）在此等价。

        内容键 (task_type, job_id) 假设同 uid 内容不变；every_run 重 spawn 打破
        该假设。仅 depends_on/resources 影响调度判定（unknown/missing/依赖/
        can_acquire），比对二者即可判定能否安全复用缓存。

        null 语义与 ``Job.from_dict`` 对齐：``resources=None``→``{}``、
        ``depends_on=None``→``[]``（否则 ``dict(None)``/``list(None)`` 抛
        TypeError，把**本可正常解析执行**的合法 job 误判为畸形进 DLQ——
        避免将合法任务误判为畸形）。非 dict/非 list 的畸形值视为不一致，
        返回 False（重新解析，``Job.from_dict`` 会对畸形正确抛错归位）。
        """
        raw_res = job_dict.get("resources")
        raw_dep = job_dict.get("depends_on")
        try:
            job_res = dict(raw_res) if raw_res is not None else {}
            job_dep = list(raw_dep) if raw_dep is not None else []
        except (TypeError, ValueError):
            return False  # 畸形值 → 不一致，重新解析（Job.from_dict 会拒）
        cached_res = dict(job.resources) if not isinstance(job.resources, dict) else job.resources
        return list(job.depends_on) == job_dep and cached_res == job_res

    def _effective_resources(self, job) -> dict[str, float]:
        """job 实际会 acquire 的资源集（委托给 ResourceManager 单点真相源）。"""
        if isinstance(job, JobFacts):
            job_resources = job.resource_map()
        else:
            job_resources = dict(job.resources)
        return self.resource_mgr.effective_resources(job.task_type, job_resources)

    def pop_next_runnable(
        self,
        state: PipelineState,
        in_flight_uids: frozenset[str] = frozenset(),
    ) -> ScheduleResult:
        """Scan queue read-only. Returns index and wait info. Does NOT acquire resources.

        ``state`` 提供队列只读事实（queue/wall/failed/queue_uids——
        ``queue_uids`` 返回活索引引用，提供 O(1) 索引，无 O(N) 拷贝）。

        ``in_flight_uids`` 是当前正在子进程中执行（已 pop 但未 commit）的 job uid
        集合。在 missing dependency 判定时，依赖正在运行的 job 不算 missing
        （待其完成 commit 到 wall 后自然解锁），避免并发模型下误判死锁。
        """
        q_data = state.queue
        wall_data = state.wall
        failed_data = state.failed
        queue_uids = state.queue_uids
        pending_or_running = queue_uids | set(in_flight_uids)

        runnable_idx = None
        min_wait = float('inf')
        waiting_for_dependency = False
        has_potential_spawners = False
        pending_dep_failure: str | None = None
        dep_failed_idx: int = -1  # 首个 dep-failed job 的索引（兜底）
        unknown_resource_uids: list[str] = []
        missing_dependency_uids: list[str] = []
        malformed_uids: list[str] = []
        impossible_resource_uids: list[str] = []

        now = time.monotonic()

        for i, job_dict in enumerate(q_data):
            # 捕获畸形 job dict（缺 task_type/job_id 等），记录 UID 避免整个扫描崩溃
            try:
                job = self.cached_job(job_dict)
            except (KeyError, TypeError, ValueError) as e:
                logger.error(f"Malformed job dict at index {i}: {e}")
                malformed_uids.append(uid_from_job_dict(job_dict))
                continue
            can_run = True
            failed_dependency = None

            # 1. Failed dependency: job is runnable (will be marked as dependency failure)
            if job.depends_on:
                for dep_uid in job.depends_on:
                    if dep_uid in failed_data:
                        failed_dependency = dep_uid
                        can_run = False
                        break

            if failed_dependency:
                # 记录首个 pending_dep_failure，继续扫描优先挑出可运行 job
                if pending_dep_failure is None:
                    pending_dep_failure = failed_dependency
                    dep_failed_idx = i
                continue

            # 2. Missing dependency: not runnable
            has_missing_dep = False
            if job.depends_on:
                for dep_uid in job.depends_on:
                    if dep_uid not in wall_data:
                        can_run = False
                        waiting_for_dependency = True
                        has_missing_dep = True
                        if dep_uid not in pending_or_running:
                            missing_dependency_uids.append(job.uid)

            if not has_missing_dep:
                has_potential_spawners = True

            # 3. 资源评估（unknown / impossible / wait_time 由 ResourceManager 深模块统一裁决）
            eval_res = self.resource_mgr.evaluate(job.task_type, job.resources)
            if eval_res.is_unknown:
                logger.error(f"Job {job.uid} references unknown resource '{eval_res.unknown_name}'.")
                unknown_resource_uids.append(job.uid)
                min_wait = float('inf')  # 死锁：未知资源
                can_run = False
            elif eval_res.is_impossible:
                impossible_resource_uids.append(job.uid)
                can_run = False
            elif not eval_res.is_available:
                can_run = False
                min_wait = min(min_wait, eval_res.wait_time)

            if not can_run:
                continue

            # 4. Backoff — 复用循环顶部的 now 值，避免双重 time.monotonic 调用
            raw_rt = job_dict.get("runtime")
            rt_state = (
                raw_rt
                if isinstance(raw_rt, JobRuntimeState)
                else (JobRuntimeState.from_dict(raw_rt) if isinstance(raw_rt, dict) else None)
            )
            if rt_state is not None and rt_state.is_backed_off(now):
                min_wait = min(min_wait, rt_state.remaining_backoff(now))
                continue

            if can_run:
                runnable_idx = i
                break

        # 兜底：整轮没有可运行 job 但存在 dep-failed job →
        # runnable_idx 落回首个 dep-failed 位置（_dispatch_job 会处理它）。
        if runnable_idx is None and pending_dep_failure is not None:
            runnable_idx = dep_failed_idx

        # pending_dep_failure 只在 runnable_idx 落回 dep-failed 兜底位置时才有效。
        # 若 runnable_idx 指向真正可运行的 job（排在 dep-failed job 之后扫描选出的），
        # 必须清空 pending_dep_failure，防止将依赖失败误判给可运行作业。
        if runnable_idx != dep_failed_idx:
            pending_dep_failure = None

        attribution = DeadlockAttribution(
            unknown_resource_uids=tuple(unknown_resource_uids),
            missing_dependency_uids=tuple(missing_dependency_uids),
            malformed_uids=tuple(malformed_uids),
            impossible_resource_uids=tuple(impossible_resource_uids),
        )

        # 死锁归因类集合（impossible/unknown/missing/malformed）任一非空时
        # 强制 min_wait=inf：这些类别的判定不依赖任何等待——impossible/
        # unknown 是永久性死锁，missing 由宽限逻辑单独裁决，malformed 无法
        # 反序列化、永远不可能变为可运行。若被队列中另一 job 的有限退避/
        # 资源等待覆盖 min_wait，死锁判定被逐轮推迟到该等待终结（退避逐轮
        # 放大时可拖数十分钟，管线表现为卡死无日志）。
        if attribution.has_deadlock_causes and min_wait != float('inf'):
            min_wait = float('inf')

        # 显式表达结果类型——兜底后 runnable_idx 与 pending_dep_failure
        # 的关系决定 kind。
        if runnable_idx is not None and pending_dep_failure is not None:
            kind = "dep_failed"
        elif runnable_idx is not None:
            kind = "runnable"
        else:
            kind = "none"

        candidate_uid: str | None = None
        if runnable_idx is not None and runnable_idx < len(q_data):
            candidate_uid = uid_from_job_dict(q_data[runnable_idx])

        return ScheduleResult(
            runnable_idx=runnable_idx,
            pending_dep_failure=pending_dep_failure,
            min_wait=min_wait,
            waiting_for_dependency=waiting_for_dependency,
            has_potential_spawners=has_potential_spawners,
            kind=kind,
            attribution=attribution,
            candidate_uid=candidate_uid,
        )
