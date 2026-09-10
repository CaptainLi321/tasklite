"""pacing.decide_wait 表驱动单测——等待/空闲决策唯一实现的语义锁定。"""

from __future__ import annotations

import pytest

from tasklite.engine.pacing import LoopFacts, WaitDecision, decide_wait
from tasklite.engine.types import StopMode

INF = float("inf")


def facts(**overrides) -> LoopFacts:
    base = dict(
        stop_mode=StopMode.NONE,
        has_in_flight=False,
        store_empty=False,
        dispatched=0,
        completed=0,
        has_runnable=True,
        min_wait=INF,
        worker_wait=0.0,
        deadlock_wait=0.0,
        should_terminate=False,
    )
    base.update(overrides)
    return LoopFacts(**base)


TABLE = [
    # (名称, facts, 期望)
    ("空闲：store 空且无在途 → 不等待", facts(store_empty=True), (False, 0.0)),
    ("governor 终止信号 → 不等待", facts(should_terminate=True), (False, 0.0)),
    ("在途无新完成 → 0.05 轮询", facts(has_in_flight=True), (True, 0.05)),
    ("在途有新完成 → 立即下一拍", facts(has_in_flight=True, completed=2), (False, 0.0)),
    ("死锁宽限等待 → 封顶 1.0", facts(deadlock_wait=7.0), (True, 1.0)),
    ("死锁宽限短等待 → 原值", facts(deadlock_wait=0.4), (True, 0.4)),
    ("无候选且有限 min_wait → 等待", facts(has_runnable=False, min_wait=0.3), (True, 0.3)),
    ("无候选且 min_wait=inf（无候选语义）→ 跳过该分支", facts(has_runnable=False, min_wait=INF, worker_wait=0.2), (True, 0.2)),
    ("死锁成因强制 inf：has_runnable=False+inf → 落到 worker_wait",
     facts(has_runnable=False, min_wait=INF, worker_wait=5.0), (True, 1.0)),
    ("资源挂起恢复等待 → 封顶 1.0", facts(worker_wait=30.0), (True, 1.0)),
    ("DRAINING 无扫描（has_runnable=True）→ 兜底分支短等待让出",
     facts(stop_mode=StopMode.DRAINING, dispatched=0, completed=0), (True, 0.0)),
    ("兜底分支：本拍有派发 → 不等待", facts(dispatched=3), (False, 0.0)),
    ("兜底分支：无派发无完成 → 短等待让出", facts(dispatched=0, completed=0), (True, 0.0)),
]


class TestDecideWaitTable:
    @pytest.mark.parametrize("name, f, expected", TABLE, ids=[t[0] for t in TABLE])
    def test_table(self, name, f, expected):
        decision = decide_wait(f)
        assert isinstance(decision, WaitDecision)
        assert (decision.should_wait, decision.wait_time) == expected

    def test_in_flight_poll_exact_value(self):
        decision = decide_wait(facts(has_in_flight=True))
        assert decision.wait_time == 0.05

    def test_branch_priority_deadlock_over_min_wait(self):
        # 死锁宽限优先于候选 min_wait（分支次序即优先级契约）
        decision = decide_wait(facts(deadlock_wait=0.8, has_runnable=False, min_wait=0.2))
        assert (decision.should_wait, decision.wait_time) == (True, 0.8)
