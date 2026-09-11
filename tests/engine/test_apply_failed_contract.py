"""apply_failed 失败终态唯一出口的联动契约回归测试。

不变式（红线「失败终态登记唯一收敛」）：
- 默认调用：uid 进 failed 集合，同时必须注销 in-flight
  （失败终态与在途集合联动，残留即阻塞后续派发判定）；
- 显式 unregister=False：登记 failed 但保留 in-flight
  （批量崩溃契约路径先登记、由调用方统一处置在途集合）。
"""

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.store import StateStore
from tasklite.models.state import PipelineState


def _make_store() -> tuple[StateStore, PipelineState]:
    state = PipelineState({}, {}, {}, [])
    return StateStore(InMemoryStateBackend(), state), state


class TestApplyFailedContract:
    def test_default_registers_failed_and_unregisters_in_flight(self):
        store, state = _make_store()
        state.register_in_flight("encode::a")
        store.apply_failed("encode::a", {"error": "boom"})
        assert "encode::a" in state.failed_uids, "失败终态必须登记进 failed 集合"
        assert "encode::a" not in state.in_flight_uids, \
            "默认调用必须注销 in-flight（失败终态联动）"
        assert state.failed["encode::a"]["error"] == "boom", \
            "失败元数据必须经规范化后落入 failed 记录"

    def test_explicit_unregister_false_keeps_in_flight(self):
        store, state = _make_store()
        state.register_in_flight("encode::a")
        store.apply_failed("encode::a", {"error": "boom"}, unregister=False)
        assert "encode::a" in state.failed_uids, "显式 False 仍必须登记 failed"
        assert "encode::a" in state.in_flight_uids, \
            "显式 unregister=False 必须保留在途（批量崩溃契约依赖）"

    def test_failed_takes_over_from_wall(self):
        """wall/failed 互斥：失败登记必须把 uid 从 wall 摘除（唯一出口维护互斥）。"""
        store, state = _make_store()
        state.add_wall("encode::a", {"prev": "ok"})
        store.apply_failed("encode::a", {"error": "boom"})
        assert "encode::a" in state.failed_uids
        assert "encode::a" not in state.wall_uids, \
            "失败登记必须将 uid 从 wall 摘除（wall/failed 互斥）"
