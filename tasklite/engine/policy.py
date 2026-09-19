"""执行策略深模块（Deep Execution Policy Module）。

负责统一管理作业运行前后全生命周期策略：
1. Rerun 策略矩阵评估（never / on_failure / every_run / on_input_change）；
2. 磁盘文件输入指纹（stat）比对与 StatCache 缓存；
3. Discovery 默认策略兜底与规范化；
4. 指数退避抖动时延与双时钟截止时间计算；
5. 重试状态机规划（plan_retry / plan_orphan_defer）与 DLQ 归因装配。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging
import math
import os
import random
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from ..models.job import Job, JobRuntimeState, RT_BACKOFF_UNTIL, RT_BACKOFF_WALL_DEADLINE
from ..models.state import uid_from_job_dict
from ..taxonomy import ERR_MAX_RETRIES as _ERR_MAX_RETRIES

logger = logging.getLogger("tasklite")


class PreflightAction(str, Enum):
    """预检判定行动。"""

    RUN = "run"    # 放行：新作业运行，或策略豁免重跑
    SKIP = "skip"  # 拦截：命中历史终态且无豁免，跳过执行


class DecisionReason(str, Enum):
    """预检判定决策归因。"""

    FRESH = "fresh"                        # 全新作业（未命中 Wall 亦未命中 DLQ）
    EVERY_RUN = "every_run"                # 显式/默认 every_run 策略豁免重跑
    ON_FAILURE_MATCH = "on_failure_match"  # 命中 DLQ 且策略为 on_failure 放行重跑
    INPUT_CHANGED = "input_changed"        # 命中 Wall 但输入文件指纹发生变化，放行重跑
    INPUT_UNCHANGED = "input_unchanged"    # 命中 Wall 且输入文件指纹无变化，拦截跳过
    WALL_BLOCKED = "wall_blocked"          # 命中 Wall 且策略为 never/on_failure，拦截跳过
    FAILED_BLOCKED = "failed_blocked"      # 命中 DLQ 且策略为 never，拦截跳过


@dataclass(frozen=True)
class PreflightDecision:
    """预检策略评估结果（不可变值对象）。"""

    action: PreflightAction
    reason: DecisionReason
    effective_rerun: str
    input_changed: Optional[bool] = None

    @property
    def should_skip(self) -> bool:
        """是否应当跳过（拦截）。"""
        return self.action == PreflightAction.SKIP

    @property
    def should_run(self) -> bool:
        """是否应当放行（运行或重跑）。"""
        return self.action == PreflightAction.RUN


@dataclass(frozen=True)
class BackoffSchedule:
    """退避时间表（封装时延与双时钟截止时间）。"""

    delay: float
    backoff_until: float        # Monotonic 时钟截止点（主循环内存调度使用）
    wall_deadline: float        # Wall-clock 时钟截止点（跨崩溃持久化使用）

    @classmethod
    def from_delay(cls, delay: float) -> "BackoffSchedule":
        now_mono = time.monotonic()
        now_wall = time.time()
        return cls(
            delay=delay,
            backoff_until=now_mono + delay,
            wall_deadline=now_wall + delay,
        )

    def populate_runtime(self, runtime_obj: Union[dict, JobRuntimeState]) -> None:
        """将双时钟截止时间原子写入 Job 的 runtime（支持 dict 或 JobRuntimeState）。"""
        if isinstance(runtime_obj, JobRuntimeState):
            runtime_obj.backoff_until = self.backoff_until
            runtime_obj.backoff_wall_deadline = self.wall_deadline
        elif isinstance(runtime_obj, dict):
            runtime_obj[RT_BACKOFF_UNTIL] = self.backoff_until
            runtime_obj[RT_BACKOFF_WALL_DEADLINE] = self.wall_deadline


@dataclass(frozen=True)
class RetryPlan:
    """重试规划不可变值对象（封装是否重试、退避时延、预组装的字典与 DLQ 元数据）。"""

    going_to_retry: bool
    delay: float = 0.0
    retry_dict: Optional[Dict[str, Any]] = None
    fail_meta: Optional[Dict[str, Any]] = None
    transient_kind: Optional[str] = None
    schedule: Optional[BackoffSchedule] = None


class AdmissionPolicy:
    """准入与预检策略深模块（统一负责 Rerun 矩阵、输入指纹比对与 Discovery 注入）。"""

    def __init__(
        self,
        discovery_rerun: Optional[Mapping[str, str]] = None,
        *,
        enable_stat_cache: bool = False,
    ) -> None:
        self.discovery_rerun: Mapping[str, str] = (
            discovery_rerun if discovery_rerun is not None else {}
        )
        self.enable_stat_cache: bool = enable_stat_cache
        self._stat_cache: Dict[str, Optional[os.stat_result]] = {}

    def clear_stat_cache(self) -> None:
        """清空实例级文件 stat 缓存。"""
        self._stat_cache.clear()

    def normalize_job_dict(self, job_dict: dict, task_type: str) -> bool:
        """入队与 Spawn 时的 Job 字典规范化（注入 Discovery 默认策略）。

        仅在 job_dict['rerun'] is None 时注入默认策略；显式指定的 'never'
        或其它策略一律尊重。
        """
        disc_rerun = self.discovery_rerun.get(task_type)
        if disc_rerun and job_dict.get("rerun") is None:
            job_dict["rerun"] = disc_rerun
            return True
        return False

    def check_input_changed(self, wall_meta: Optional[dict]) -> bool:
        """比对 Wall 历史输入指纹与当前磁盘文件状态（带 StatCache 与脏数据容灾）。

        任一文件输入 size/mtime_ns 变化（或文件消失、或 wall 无历史指纹）
        → 视为变化（True）→ 重跑。URI 声明不参与比对。
        """
        prev_inputs = wall_meta.get("inputs") if isinstance(wall_meta, dict) else None
        if not isinstance(prev_inputs, list):
            return True
        for entry in prev_inputs:
            if not isinstance(entry, dict):
                return True
            if entry.get("kind") == "uri":
                continue
            path = entry.get("path")
            if not path or not isinstance(path, str):
                return True
            size = entry.get("size")
            mtime_ns = entry.get("mtime_ns")
            if size is None or mtime_ns is None:
                return True
            try:
                if self.enable_stat_cache:
                    if path not in self._stat_cache:
                        try:
                            self._stat_cache[path] = os.stat(path)
                        except OSError:
                            self._stat_cache[path] = None
                    st = self._stat_cache[path]
                    if st is None:
                        return True
                else:
                    st = os.stat(path)
            except OSError:
                return True
            if st.st_size != size or st.st_mtime_ns != mtime_ns:
                return True
        return False

    def evaluate(
        self,
        job_dict_or_rerun: Union[dict, Optional[str]],
        *,
        task_type: Optional[str] = None,
        wall_meta: Optional[dict] = None,
        is_wall: bool = False,
        is_failed: bool = False,
    ) -> PreflightDecision:
        """评估作业是否应当被拦截或放行重跑。"""
        if isinstance(job_dict_or_rerun, dict):
            rerun_raw = job_dict_or_rerun.get("rerun")
            actual_task_type = task_type or job_dict_or_rerun.get("task_type")
        else:
            rerun_raw = job_dict_or_rerun
            actual_task_type = task_type

        # Discovery 默认策略兜底（未显式指定时）
        if rerun_raw is None and actual_task_type and actual_task_type in self.discovery_rerun:
            rerun = self.discovery_rerun[actual_task_type]
        else:
            rerun = rerun_raw or "never"

        # 未命中历史集合
        if not is_wall and not is_failed:
            return PreflightDecision(
                action=PreflightAction.RUN,
                reason=DecisionReason.FRESH,
                effective_rerun=rerun,
            )

        if rerun == "every_run":
            return PreflightDecision(
                action=PreflightAction.RUN,
                reason=DecisionReason.EVERY_RUN,
                effective_rerun=rerun,
            )

        if rerun == "on_failure":
            if is_failed:
                return PreflightDecision(
                    action=PreflightAction.RUN,
                    reason=DecisionReason.ON_FAILURE_MATCH,
                    effective_rerun=rerun,
                )
            return PreflightDecision(
                action=PreflightAction.SKIP,
                reason=DecisionReason.WALL_BLOCKED,
                effective_rerun=rerun,
            )

        if rerun == "on_input_change":
            if is_failed:
                return PreflightDecision(
                    action=PreflightAction.RUN,
                    reason=DecisionReason.ON_FAILURE_MATCH,
                    effective_rerun=rerun,
                )
            if is_wall:
                changed = self.check_input_changed(wall_meta)
                if changed:
                    return PreflightDecision(
                        action=PreflightAction.RUN,
                        reason=DecisionReason.INPUT_CHANGED,
                        effective_rerun=rerun,
                        input_changed=True,
                    )
                return PreflightDecision(
                    action=PreflightAction.SKIP,
                    reason=DecisionReason.INPUT_UNCHANGED,
                    effective_rerun=rerun,
                    input_changed=False,
                )
            return PreflightDecision(
                action=PreflightAction.RUN,
                reason=DecisionReason.FRESH,
                effective_rerun=rerun,
            )

        # 默认 "never"
        if is_wall:
            return PreflightDecision(
                action=PreflightAction.SKIP,
                reason=DecisionReason.WALL_BLOCKED,
                effective_rerun=rerun,
            )
        return PreflightDecision(
            action=PreflightAction.SKIP,
            reason=DecisionReason.FAILED_BLOCKED,
            effective_rerun=rerun,
        )

    def admit(
        self,
        job_dict: dict,
        store_or_state: Any,
    ) -> PreflightDecision:
        """统一极窄准入判定入口：自动从 store/state 提取历史上下文并执行评估。"""
        uid = uid_from_job_dict(job_dict)
        wall = getattr(store_or_state, "wall", {})
        failed = getattr(store_or_state, "failed", {})
        wall_hit = uid in wall
        failed_hit = uid in failed
        wall_meta = wall.get(uid) if wall_hit else None
        return self.evaluate(
            job_dict,
            wall_meta=wall_meta,
            is_wall=wall_hit,
            is_failed=failed_hit,
        )


class BackoffGovernor:
    """退避与重试规划治理深模块（统一负责指数退避计算、双时钟调度与重试状态机）。"""

    def compute_backoff(
        self,
        retries: int,
        backoff_base: float = 2.0,
        backoff_max: float = 300.0,
    ) -> float:
        """计算指数退避抖动时延（秒）。"""
        if retries < 0:
            logger.warning(f"compute_backoff called with negative retries={retries}, treating as 0")
            return 0.0
        if retries == 0:
            return 0.0
        if not math.isfinite(backoff_base) or backoff_base < 0:
            backoff_base = 2.0
        if not math.isfinite(backoff_max) or backoff_max < 0:
            backoff_max = 300.0

        exp = min(retries - 1, 60)
        base_delay = backoff_base * (2 ** exp)
        delay = min(base_delay, backoff_max)
        jitter = random.uniform(-0.25, 0.25) * delay
        return max(0.0, delay + jitter)

    def compute_backoff_schedule(
        self,
        retries: int,
        backoff_base: float = 2.0,
        backoff_max: float = 300.0,
        *,
        now_mono: Optional[float] = None,
        now_wall: Optional[float] = None,
    ) -> BackoffSchedule:
        """计算指数退避抖动时延，并生成对齐的双时钟截止时间。"""
        delay = self.compute_backoff(retries, backoff_base, backoff_max)
        mono = time.monotonic() if now_mono is None else now_mono
        wall = time.time() if now_wall is None else now_wall
        return BackoffSchedule(
            delay=delay,
            backoff_until=mono + delay,
            wall_deadline=wall + delay,
        )

    def compute_orphan_schedule(
        self,
        *,
        now_mono: Optional[float] = None,
        now_wall: Optional[float] = None,
    ) -> BackoffSchedule:
        """计算孤儿锁冲突时的短退避时间表（[0.75, 1.0]s 抖动）。"""
        delay = random.uniform(0.75, 1.0)
        mono = time.monotonic() if now_mono is None else now_mono
        wall = time.time() if now_wall is None else now_wall
        return BackoffSchedule(
            delay=delay,
            backoff_until=mono + delay,
            wall_deadline=wall + delay,
        )

    def plan_orphan_defer(self, job_dict: dict) -> BackoffSchedule:
        """为孤儿锁探测 defer 生成短退避计划，原子填充 job_dict['runtime']。"""
        sched = self.compute_orphan_schedule()
        rt_state = JobRuntimeState.from_dict(job_dict.get("runtime"))
        sched.populate_runtime(rt_state)
        job_dict["runtime"] = rt_state.to_dict()
        return sched

    def plan_retry(
        self,
        job: Job,
        job_dict: dict,
        retry_error_or_result: Any = None,
        *,
        retry_error: Optional[str] = None,
        transient_kind: Optional[str] = None,
    ) -> RetryPlan:
        """规划重试决策与退避状态机（单一出口：计算预算、退避时延与双时钟状态）。

        契约：
        1. 瞬态信号（transient_kind 登记于 engine.types.TRANSIENT_KIND_STAT_KEYS：
           interrupted / lock_conflict / rate_limited）：
           - 不消耗重试预算（不递增 job.retries）；
           - 即使 job.retries 已达 max_retries 也豁免 DLQ；
           - 采用 [0.75, 1.0]s 短退避回队自恢复（限流的实际等待由资源
             挂起 TTL 承担，退避只负责回队节奏）；
           - 不污染 last_retry_error。
        2. 正常业务失败重试：
           - 检查 job.retries >= job.max_retries：超限则装配 DLQ fail_meta 并返回 going_to_retry=False；
           - 未超限则 job.retries += 1，按指数退避计算 BackoffSchedule；
           - 记录 retry_error 到 last_retry_error。
        3. 组装待入队的 retry_dict 并对齐双时钟截止时间。
        """
        # 支持直接传入 ExecutionResult 结构体
        if retry_error_or_result is not None:
            if hasattr(retry_error_or_result, "retry_error"):
                retry_error = getattr(retry_error_or_result, "retry_error", retry_error)
            elif isinstance(retry_error_or_result, str):
                retry_error = retry_error_or_result
            kind = getattr(retry_error_or_result, "transient_kind", None)
            if kind:
                transient_kind = kind

        transient = transient_kind is not None

        if job.retries >= job.max_retries and not transient:
            fail_meta: Dict[str, Any] = {"error": _ERR_MAX_RETRIES}
            rt_state = JobRuntimeState.from_dict(job_dict.get("runtime"))
            if rt_state.last_retry_error:
                fail_meta["last_retry_error"] = rt_state.last_retry_error
            if retry_error:
                fail_meta["retry_error"] = retry_error
            return RetryPlan(
                going_to_retry=False,
                delay=0.0,
                fail_meta=fail_meta,
                transient_kind=transient_kind,
            )

        if transient:
            sched = self.compute_orphan_schedule()
        else:
            job.retries += 1
            sched = self.compute_backoff_schedule(
                job.retries, job.backoff_base, job.backoff_max
            )

        # 不变式：重试 = 原作业原样重入队——job_dict 顶层自定义字段随重试
        # 往返保留；受管键以 job 权威状态覆写（retries 已递增等），runtime
        # 单独重建。
        retry_dict = dict(job_dict)
        retry_dict.update(job.to_dict())
        retry_dict["resources"] = dict(job_dict.get("resources", {}))
        retry_state = JobRuntimeState.from_dict(job_dict.get("runtime"))
        if retry_error and not transient:
            retry_state.last_retry_error = retry_error
        sched.populate_runtime(retry_state)
        retry_dict["runtime"] = retry_state.to_dict()

        return RetryPlan(
            going_to_retry=True,
            delay=sched.delay,
            retry_dict=retry_dict,
            transient_kind=transient_kind,
            schedule=sched,
        )


class ExecutionPolicy(AdmissionPolicy, BackoffGovernor):
    """执行与预检深模块组合门面（统合准入矩阵、文件指纹、指数退避与重试规划）。"""

    def __init__(
        self,
        discovery_rerun: Optional[Mapping[str, str]] = None,
        *,
        enable_stat_cache: bool = False,
    ) -> None:
        super().__init__(discovery_rerun=discovery_rerun, enable_stat_cache=enable_stat_cache)


__all__ = [
    "AdmissionPolicy",
    "BackoffGovernor",
    "BackoffSchedule",
    "DecisionReason",
    "ExecutionPolicy",
    "PreflightAction",
    "PreflightDecision",
    "RetryPlan",
]
