"""v2 准入与重入队策略深模块。

统一内敛：
1. rerun 四策略矩阵评估（never / on_failure / every_run / on_input_change）
   ——策略只作用于 wall/failed 拦截点，queue/in-flight 永远算数；
2. 磁盘文件输入指纹（stat）比对与实例级 stat 缓存；
3. 任务级默认策略（discovery 注入面）的入队规范化；
4. 重入队节奏接缝（RequeuePolicy）：重试节奏的唯一出口，默认实现为
   立即重入队——核心不出现任何节奏计算，未来延迟类策略经此插入。
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from ..models.state import uid_from_job_dict


class PreflightAction(str, Enum):
    """预检判定行动。"""

    RUN = "run"    # 放行：新作业运行，或策略豁免重跑
    SKIP = "skip"  # 拦截：命中历史终态且无豁免，跳过执行


class DecisionReason(str, Enum):
    """预检判定决策归因。"""

    FRESH = "fresh"                        # 全新作业（未命中 wall 亦未命中失败档案）
    EVERY_RUN = "every_run"                # 显式/默认 every_run 策略豁免重跑
    ON_FAILURE_MATCH = "on_failure_match"  # 命中失败档案且策略为 on_failure 放行重跑
    INPUT_CHANGED = "input_changed"        # 命中 wall 但输入文件指纹发生变化，放行重跑
    INPUT_UNCHANGED = "input_unchanged"    # 命中 wall 且输入文件指纹无变化，拦截跳过
    WALL_BLOCKED = "wall_blocked"          # 命中 wall 且策略为 never/on_failure，拦截跳过
    FAILED_BLOCKED = "failed_blocked"      # 命中失败档案且策略为 never，拦截跳过


@dataclass(frozen=True)
class PreflightDecision:
    """预检策略评估结果（不可变值对象）。"""

    action: PreflightAction
    reason: DecisionReason
    effective_rerun: str
    input_changed: bool | None = None

    @property
    def should_skip(self) -> bool:
        """是否应当跳过（拦截）。"""
        return self.action == PreflightAction.SKIP

    @property
    def should_run(self) -> bool:
        """是否应当放行（运行或重跑）。"""
        return self.action == PreflightAction.RUN


class RerunPolicy:
    """rerun 策略矩阵深模块（评估、输入指纹比对与任务级默认策略注入）。

    策略只作用于 wall/failed 拦截点：命中历史终态的作业按矩阵判定拦截
    或豁免重跑；queue/in-flight 身份永远算数（同一轮内不重复派发）。
    """

    def __init__(
        self,
        discovery_rerun: Mapping[str, str] | None = None,
        *,
        enable_stat_cache: bool = False,
    ) -> None:
        self.discovery_rerun: Mapping[str, str] = (
            discovery_rerun if discovery_rerun is not None else {}
        )
        self.enable_stat_cache: bool = enable_stat_cache
        self._stat_cache: dict[str, os.stat_result | None] = {}

    def clear_stat_cache(self) -> None:
        """清空实例级文件 stat 缓存。"""
        self._stat_cache.clear()

    def normalize_job_dict(self, job_dict: dict, task_type: str) -> bool:
        """入队与 Spawn 时的 Job 字典规范化（注入任务级默认策略）。

        仅在 job_dict['rerun'] is None 时注入默认策略；显式指定的 'never'
        或其它策略一律尊重。
        """
        disc_rerun = self.discovery_rerun.get(task_type)
        if disc_rerun and job_dict.get("rerun") is None:
            job_dict["rerun"] = disc_rerun
            return True
        return False

    def check_input_changed(self, wall_meta: dict | None) -> bool:
        """比对 wall 历史输入指纹与当前磁盘文件状态（带 stat 缓存与脏数据容灾）。

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
        job_dict_or_rerun: dict | str | None,
        *,
        task_type: str | None = None,
        wall_meta: dict | None = None,
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

        # 任务级默认策略兜底（未显式指定时）
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


@dataclass(frozen=True)
class RequeuePlan:
    """重入队节奏规划（不可变值对象）。

    ``front`` 为 True 表示插队首（同轮内优先重扫，重试不被新入队作业
    排挤到饥饿尾部）；``delay_seconds`` 是延迟类策略的表达位——默认
    实现恒为 0（立即），当前核心无任何延迟消费方，未来节奏策略经
    RequeuePolicy 接缝填充此字段即可生效，无需改动调用方。
    """

    front: bool
    delay_seconds: float = 0.0


class RequeuePolicy(ABC):
    """重试节奏唯一出口的接缝契约。

    核心引擎不做任何节奏计算：需要把一个失败/瞬态作业放回队列时，
    一律经 ``plan_requeue`` 取得节奏规划（何时、何处）。瞬态信号
    （interrupted / lock_conflict / rate_limited）不烧重试预算的军规
    由「立即重入队」默认策略自然成立——零等待、零预算消耗。
    """

    @abstractmethod
    def plan_requeue(
        self,
        job_dict: dict,
        *,
        transient_kind: str | None = None,
    ) -> RequeuePlan:
        """为一个待重入队的作业规划节奏。

        ``transient_kind`` 非 None 表示瞬态信号（引擎值对象
        TRANSIENT_KIND_STAT_KEYS 登记的三类）——预算豁免语境下策略
        不得因预算耗尽拒绝重入队；返回值表达节奏与位置。
        """


class ImmediateRequeuePolicy(RequeuePolicy):
    """立即重入队策略（默认）：零延迟、插队首。

    语义与瞬态信号的队首重入队路径一致（锁冲突/限流/中断的降级回队
    均为 front 插队）；业务失败重试同享该节奏——预算判定在收尾机器
    侧先行，走到本策略的作业都已是「决定重试」的作业。
    """

    def plan_requeue(
        self,
        job_dict: dict,
        *,
        transient_kind: str | None = None,
    ) -> RequeuePlan:
        return RequeuePlan(front=True, delay_seconds=0.0)


__all__ = [
    "DecisionReason",
    "ImmediateRequeuePolicy",
    "PreflightAction",
    "PreflightDecision",
    "RequeuePlan",
    "RequeuePolicy",
    "RerunPolicy",
]
