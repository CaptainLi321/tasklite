"""深链场景下死锁仲裁与运行主循环的确定性回归测试。

不变式：
- governor 每轮仲裁对全队列调用环检测，环检测不得因深链抛 RecursionError
  沿运行主循环上抛（否则整轮 run 以崩溃退出，队列已持久化、重启复现）；
- 无环深链处于有限退避遮蔽下，仲裁必须正常完成并返回 none（正常等待）；
- 深链伴随真环时，仲裁只熔断环成员，链上作业保留在队列。
"""

import time
import types

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.governor import DeadlockGovernor
from tasklite.engine.store import StateStore
from tasklite.models.job import Job
from tasklite.models.state import PipelineState
from tests.helpers import make_fake_process_class, make_pipeline, patch_multiprocessing_for_fakes

# 超出解释器默认递归限制（1000），保证场景真实落在递归深度敏感区
CHAIN_N = 1500


def _chain_queue(n, tail_deps=None):
    """n 节点线性链 j0→j1→…→j_{n-1}，链尾可追加额外依赖。"""
    queue = []
    for i in range(n):
        deps = [f"t::j{i+1}"] if i + 1 < n else list(tail_deps or ())
        queue.append(Job("t", f"j{i}", payload={}, depends_on=deps).to_dict())
    return queue


def _spy_cycle_detection(monkeypatch):
    """包装环检测统计调用次数——防测试空转：断言仲裁确实执行了检测。"""
    calls = {"count": 0}
    original = PipelineState.find_dependency_cycles

    def spy(self):
        calls["count"] += 1
        return original(self)

    monkeypatch.setattr(PipelineState, "find_dependency_cycles", spy)
    return calls


class TestGovernorArbitrateDeepChain:
    def test_finite_wait_deep_acyclic_chain_arbitration_returns_none(self, monkeypatch):
        """有限退避 + 无环深链：仲裁正常完成返回 none，环检测不得崩溃。"""
        calls = _spy_cycle_detection(monkeypatch)
        gov = DeadlockGovernor()
        state = PipelineState({}, {}, {}, _chain_queue(CHAIN_N))
        store = StateStore(InMemoryStateBackend(), state=state)
        from tasklite.engine.scheduler import StandstillFacts
        sched = StandstillFacts(min_wait=1.0, waiting_for_dependency=True)

        decision = gov.arbitrate(sched, store=store)

        assert calls["count"] >= 1, "仲裁未执行环检测，测试空转"
        assert decision.action == "none"
        assert decision.should_terminate is False

    def test_finite_wait_deep_chain_with_cycle_resolves_only_cycle_members(self, monkeypatch):
        """有限退避 + 深链伴随真环：仲裁熔断且仅环成员进 DLQ，链保留。"""
        calls = _spy_cycle_detection(monkeypatch)
        queue = _chain_queue(CHAIN_N)
        queue += [
            Job("t", "c0", payload={}, depends_on=["t::c1"]).to_dict(),
            Job("t", "c1", payload={}, depends_on=["t::c0"]).to_dict(),
        ]
        store = StateStore(InMemoryStateBackend(), state=PipelineState({}, {}, {}, queue))
        gov = DeadlockGovernor()
        from tasklite.engine.scheduler import StandstillFacts
        sched = StandstillFacts(min_wait=1.0, waiting_for_dependency=True)

        decision = gov.arbitrate(sched, store=store)

        assert calls["count"] >= 1, "仲裁未执行环检测，测试空转"
        assert decision.action == "resolved"
        assert set(decision.failed_uids) == {"t::c0", "t::c1"}
        assert set(store.state.queue_uids) == {f"t::j{i}" for i in range(CHAIN_N)}


class TestRunLoopDeepChainNoCrash:
    def test_run_completes_with_deep_chain_waiting_on_backoff_job(self, tmp_path, monkeypatch):
        """深链等退避作业的全链路：仲裁（环检测）→ 退避到期 → 致命失败级联
        整链进 DLQ → 队列清空正常退出，全程不得因环检测崩溃。"""
        calls = _spy_cycle_detection(monkeypatch)
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: (True, {}))

        # 链尾依赖退避中的 c：仲裁期 min_wait 有限且 waiting_for_dependency
        queue = _chain_queue(CHAIN_N, tail_deps=["t::c"])
        backoff = Job("t", "c", payload={}).to_dict()
        backoff["runtime"] = {"_backoff_until": time.monotonic() + 0.3}
        queue.append(backoff)

        pipeline._runtime.store.set_state(PipelineState({}, {}, {}, queue))
        patch_multiprocessing_for_fakes(
            monkeypatch, fake_process_class=make_fake_process_class("fatal")
        )
        pipeline._runtime._run_loop()

        assert calls["count"] >= 1, "仲裁未执行环检测，测试空转"
        failed = pipeline.backend.load_failed()
        wall = pipeline.backend.load_wall()
        assert "t::c" in failed and failed["t::c"].get("fatal")
        for i in range(CHAIN_N):
            assert f"t::j{i}" in failed, f"t::j{i} 未随级联进 DLQ"
            assert failed[f"t::j{i}"]["error"] == "JOB_DEPENDENCY"
        assert wall == {}, f"深链场景不得有作业成功进 wall: {sorted(wall)}"
