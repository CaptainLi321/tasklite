"""decide_wait 表驱动单测——等待/空闲决策唯一实现的语义锁定。"""

from __future__ import annotations

import pytest

from tasklite.engine.types import StopMode
from tasklite.engine.wait import (
    AUTHORIZATION_RECHECK_SECONDS,
    LoopFacts,
    WaitDecision,
    decide_wait,
)

INF = float("inf")


def facts(**overrides) -> LoopFacts:
    base = dict(
        stop_mode=StopMode.NONE,
        has_in_flight=False,
        store_empty=False,
        dispatched=0,
        settled=0,
        has_runnable=True,
        min_wait=INF,
        in_flight_wait=None,
        authorization_wait=False,
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
    ("在途无新完成且无截止点 → 等通知", facts(has_in_flight=True), (True, INF)),
    ("在途有执行超时 → 等到最近超时", facts(has_in_flight=True, in_flight_wait=0.7), (True, 0.7)),
    ("在途有新完成 → 立即下一拍", facts(has_in_flight=True, settled=2), (False, 0.0)),
    ("授权决策等待 → 仅授权复查节拍", facts(authorization_wait=True), (True, AUTHORIZATION_RECHECK_SECONDS)),
    ("死锁缺口等待 → 使用完整截止点", facts(deadlock_wait=7.0), (True, 7.0)),
    ("死锁缺口短等待 → 原值", facts(deadlock_wait=0.4), (True, 0.4)),
    ("无候选且有限 min_wait → 等待", facts(has_runnable=False, min_wait=0.3), (True, 0.3)),
    ("无候选且 min_wait=inf（无候选语义）→ 跳过该分支",
     facts(has_runnable=False, min_wait=INF, worker_wait=0.2), (True, 0.2)),
    ("死锁成因强制 inf：has_runnable=False+inf → 落到 worker_wait",
     facts(has_runnable=False, min_wait=INF, worker_wait=5.0), (True, 5.0)),
    ("资源挂起恢复等待 → 使用完整截止点", facts(worker_wait=30.0), (True, 30.0)),
    ("DRAINING 无扫描且未知 → 等通知",
     facts(stop_mode=StopMode.DRAINING, dispatched=0, settled=0), (True, INF)),
    ("本拍有派发且无截止点 → 立即下一拍", facts(dispatched=3), (False, 0.0)),
    ("无派发且未知 → 等通知", facts(dispatched=0, settled=0), (True, INF)),
]


class TestDecideWaitTable:
    @pytest.mark.parametrize("name, f, expected", TABLE, ids=[t[0] for t in TABLE])
    def test_table(self, name, f, expected):
        decision = decide_wait(f)
        assert isinstance(decision, WaitDecision)
        assert (decision.should_wait, decision.wait_time) == expected

    def test_earliest_deadline_wins(self):
        decision = decide_wait(facts(
            has_in_flight=True,
            in_flight_wait=4.0,
            deadlock_wait=0.8,
            has_runnable=False,
            min_wait=0.2,
        ))
        assert (decision.should_wait, decision.wait_time) == (True, 0.2)

    def test_idle_wins_even_with_wait_sources(self):
        # 空闲判定优先于一切等待源：无在途且队列空 → 不等待
        decision = decide_wait(facts(store_empty=True, worker_wait=9.0, deadlock_wait=0.6))
        assert (decision.should_wait, decision.wait_time) == (False, 0.0)
