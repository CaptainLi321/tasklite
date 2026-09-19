"""回归：瞬态重试与 wall 历史行并存的 every_run 任务不得崩主循环。

不变式：queue ∩ (wall∪failed) = ∅，豁免仅限处于重跑生命周期的 rerun 作业。
in-flight 结算是「complete_job 意外未注销」的兜底安全网，不得对已走完
三态提交（retry 分支已注销并重建豁免）的作业做二次注销，否则豁免被误删、
断言误报。
"""

import pytest

from tasklite import Job
from tests.helpers import (
    make_ipc_process_class,
    make_pipeline,
    patch_multiprocessing_for_fakes,
)


class TestEveryRunRetryWithWallHistory:
    def test_transient_retry_then_success_does_not_crash(self, tmp_path, monkeypatch):
        """主用例：every_run + wall 历史 + 首次 RetryError → 重试成功。

        修复前：apply_retry 注销后 requeue 重建豁免，settle 的 finally
        无条件二次注销把豁免删掉 → 主循环 AssertionError
        （uid in wall/failed and queue）。
        """
        p = make_pipeline(tmp_path)
        p.register_handler("t", lambda j, c: (True, {}))
        p.seed_wall(["t::scan"])
        p.enqueue([Job("t", "scan", rerun="every_run")])

        FlakyProcess = make_ipc_process_class(results=[
            {"status": "retry", "error": "transient proxy blip"},
            {"status": "success", "raw_result": True,
             "new_jobs": [], "resource_suspensions": [], "cursor_updates": {}},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FlakyProcess)
        monkeypatch.setattr(
            "tasklite.engine.policy.ExecutionPolicy.compute_backoff", lambda *a, **k: 0.0)
        monkeypatch.setattr("tasklite.engine.runtime.time.sleep", lambda s: None)

        p.run()

        assert FlakyProcess.call_count[0] == 2, "瞬态失败后必须恰好重试一次"
        wall = p.backend.load_wall()
        assert "t::scan" in wall, "重试成功后 wall 行必须 REPLACE"
        assert "t::scan" not in p.backend.load_failed()


class TestInFlightSettleConditionalForwarding:
    """InFlightTracker.settle 条件转发分支单元锁定。"""

    def _make_state(self, queue=()):
        from tasklite.models.state import PipelineState
        return PipelineState({}, {}, {}, list(queue))

    def test_settle_skips_unregister_when_uid_not_in_flight(self):
        """uid 已不在 in-flight（三态提交已注销）→ 只 pop 条目，不转发注销。"""
        from tasklite.engine.inflight import InFlightTracker, InFlightJob

        state = self._make_state()
        entry = InFlightJob(uid="t::x", job_dict={}, job=None)
        tracker = InFlightTracker({"t::x": entry})
        # 模拟三态提交（apply_retry/apply_success）已注销：uid 不在途
        popped = tracker.settle("t::x", state=state)
        assert popped is entry
        assert "t::x" not in state.in_flight_uids
        assert len(tracker) == 0

    def test_settle_forwards_unregister_when_uid_still_in_flight(self):
        """uid 仍在途（complete_job 意外未注销的安全网场景）→ 照常转发注销。"""
        from tasklite.engine.inflight import InFlightTracker, InFlightJob

        state = self._make_state()
        state.register_in_flight("t::y")
        entry = InFlightJob(uid="t::y", job_dict={}, job=None)
        tracker = InFlightTracker({"t::y": entry})
        popped = tracker.settle("t::y", state=state)
        assert popped is entry
        assert "t::y" not in state.in_flight_uids

    def test_settle_preserves_rerun_exemption_of_retried_job(self):
        """重试路径复核：注销后 requeue 重建豁免，settle 不得再删豁免。"""
        from tasklite.engine.inflight import InFlightTracker, InFlightJob

        state = self._make_state()
        state.register_in_flight("t::x")
        tracker = InFlightTracker({"t::x": InFlightJob(uid="t::x", job_dict={}, job=None)})
        # 模拟 apply_retry 顺序：注销 → requeue（豁免重建）→ uid 离开在途
        state.unregister_in_flight("t::x")
        state.requeue_jobs([Job("t", "x", rerun="every_run").to_dict()], front=True)
        tracker.settle("t::x", state=state)
        assert "t::x" in state.rerun_active_uids, "豁免不得被结算二次注销误删"


class TestStateAssertionFactSource:
    """断言豁免判定直接取 queue 事实源（rerun 字段），不依赖派生缓存。"""

    def _make_state(self, queue=(), wall=()):
        from tasklite.models.state import PipelineState
        return PipelineState(dict.fromkeys(wall, {}), {}, {}, list(queue))

    def test_rerun_exempt_even_if_cache_drained(self):
        """缓存 _rerun_active_uids 被误删时，queue 中 rerun 作业仍豁免不误报。"""
        state = self._make_state(queue=[Job("t", "x", rerun="every_run").to_dict()],
                                wall=["t::x"])
        state._rerun_active_uids.clear()
        state._assert_state_consistent()  # 不抛即通过

    def test_non_rerun_collision_still_asserts(self):
        """非 rerun 作业撞 queue∩wall → 必须断言失败（门禁不得被弱化）。"""
        state = self._make_state(queue=[Job("t", "y").to_dict()], wall=["t::y"])
        with pytest.raises(AssertionError, match="uid in wall/failed and queue"):
            state._assert_state_consistent()

    def test_rerun_collision_with_cache_ok_but_direct_gate(self):
        """缓存失效时非 rerun 作业仍被门禁拦截（事实源兜底）。"""
        state = self._make_state(queue=[Job("t", "z").to_dict()], wall=["t::z"])
        with pytest.raises(AssertionError, match="uid in wall/failed and queue"):
            state._assert_state_consistent()
