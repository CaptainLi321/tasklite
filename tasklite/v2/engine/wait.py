"""v2 运行循环等待决策纯函数。

事件泵每拍把现场收敛为 LoopFacts 快照，经 decide_wait 求得
WaitDecision——等待/空闲语义的唯一实现（单一真相源），表驱动单测锁定。
等待来源只有四类：在途轮询、死锁宽限/缺口等待、候选资源等待、
worker 槽位等待。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .types import StopMode

# 在途但本拍无新完成时的轮询间隔
_IN_FLIGHT_POLL_SECONDS = 0.05
# 各类受控等待的单拍上限
_WAIT_CAP_SECONDS = 1.0


@dataclass(frozen=True)
class LoopFacts:
    """一拍事件泵的纯数据快照。"""

    stop_mode: StopMode
    has_in_flight: bool
    store_empty: bool
    dispatched: int
    completed: int
    # 本拍是否存在「无可运行候选」信号；无调度扫描（DRAINING）时视为 True
    # （不触发无候选等待分支）
    has_runnable: bool = True
    # 候选最早可运行时刻（资源等待聚合）；inf = 无候选，或死锁成因被
    # 调度器强制置 inf（双语义）
    min_wait: float = field(default_factory=lambda: float("inf"))
    # 资源挂起最早恢复时刻，按 min 聚合各次派发结果中的正值
    worker_wait: float = 0.0
    # governor 宽限/gap 裁决等待
    deadlock_wait: float = 0.0
    # governor 仲裁的独立终止信号（区别于 is_idle）
    should_terminate: bool = False


@dataclass(frozen=True)
class WaitDecision:
    """一拍等待决策结果。"""

    should_wait: bool
    wait_time: float


def decide_wait(facts: LoopFacts) -> WaitDecision:
    """等待/空闲决策唯一实现（分支次序即优先级契约）。"""
    is_idle = facts.store_empty and not facts.has_in_flight
    if is_idle or facts.should_terminate:
        return WaitDecision(should_wait=False, wait_time=0.0)
    if facts.has_in_flight:
        if facts.completed == 0:
            return WaitDecision(should_wait=True, wait_time=_IN_FLIGHT_POLL_SECONDS)
        return WaitDecision(should_wait=False, wait_time=0.0)
    if facts.deadlock_wait > 0:
        return WaitDecision(should_wait=True, wait_time=min(facts.deadlock_wait, _WAIT_CAP_SECONDS))
    if not facts.has_runnable and facts.min_wait != float("inf"):
        return WaitDecision(should_wait=True, wait_time=min(facts.min_wait, _WAIT_CAP_SECONDS))
    if facts.worker_wait > 0:
        return WaitDecision(should_wait=True, wait_time=min(facts.worker_wait, _WAIT_CAP_SECONDS))
    return WaitDecision(should_wait=(facts.dispatched == 0 and facts.completed == 0), wait_time=0.0)
