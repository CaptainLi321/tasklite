"""v2 运行循环等待决策纯函数。

事件泵每拍把现场收敛为 LoopFacts 快照，经 decide_wait 求得
WaitDecision——等待/空闲语义的唯一实现（单一真相源），表驱动单测锁定。
等待来源包括授权决策复查、在途超时、死锁宽限/缺口等待、候选资源
等待与 worker 槽位等待。除授权决策外均按已知截止点或通知推进，不做
固定周期扫描。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .types import StopMode

# 异步 authorizer 返回 None 后无外部通知入口，只复查授权决策，不扫描资源。
AUTHORIZATION_RECHECK_SECONDS = 0.05


@dataclass(frozen=True)
class LoopFacts:
    """一拍事件泵的纯数据快照。"""

    stop_mode: StopMode
    has_in_flight: bool
    store_empty: bool
    dispatched: int
    settled: int
    # 本拍是否存在「无可运行候选」信号；无调度扫描（DRAINING）时视为 True
    # （不触发无候选等待分支）
    has_runnable: bool = True
    # 候选最早可运行时刻（资源等待聚合）；inf = 无候选，或死锁成因被
    # 调度器强制置 inf（双语义）
    min_wait: float = field(default_factory=lambda: float("inf"))
    # 最近一个未暂停执行超时；None 表示在途只有授权等待，无执行截止点
    in_flight_wait: float | None = None
    # 存在等待外部 authorizer 再次决策的执行
    authorization_wait: bool = False
    # 资源挂起最早恢复时刻，按 min 聚合各次派发结果中的正值
    worker_wait: float = 0.0
    # governor 宽限/gap 裁决等待
    deadlock_wait: float = 0.0
    # governor 仲裁的独立终止信号（区别于 is_idle）
    should_terminate: bool = False
    # 本拍是否刚熔断/解决死锁（已产生状态迁移，下一拍立即推进）
    deadlock_detected: bool = False


@dataclass(frozen=True)
class WaitDecision:
    """一拍等待决策结果。"""

    should_wait: bool
    wait_time: float


def decide_wait(facts: LoopFacts) -> WaitDecision:
    """等待/空闲决策唯一实现（分支次序即优先级契约）。"""
    is_idle = facts.store_empty and not facts.has_in_flight
    if is_idle or facts.should_terminate or facts.deadlock_detected:
        return WaitDecision(should_wait=False, wait_time=0.0)
    if facts.settled > 0:
        return WaitDecision(should_wait=False, wait_time=0.0)

    deadlines: list[float] = []
    if facts.deadlock_wait > 0:
        deadlines.append(facts.deadlock_wait)
    if not facts.has_runnable and facts.min_wait != float("inf"):
        deadlines.append(facts.min_wait)
    if facts.worker_wait > 0:
        deadlines.append(facts.worker_wait)
    if facts.in_flight_wait is not None:
        deadlines.append(max(0.0, facts.in_flight_wait))
    if facts.authorization_wait:
        deadlines.append(AUTHORIZATION_RECHECK_SECONDS)
    if deadlines:
        return WaitDecision(should_wait=True, wait_time=min(deadlines))
    if facts.has_in_flight or (not facts.has_runnable and not facts.store_empty):
        return WaitDecision(should_wait=True, wait_time=float("inf"))
    if facts.dispatched == 0:
        return WaitDecision(should_wait=True, wait_time=float("inf"))
    return WaitDecision(should_wait=False, wait_time=0.0)
