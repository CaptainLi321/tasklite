"""v2 准入与重入队策略深模块测试：rerun 矩阵、输入指纹与节奏接缝。"""

from __future__ import annotations

from types import SimpleNamespace

from tasklite.v2.engine.admission import (
    DecisionReason,
    ImmediateRequeuePolicy,
    PreflightAction,
    RequeuePlan,
    RequeuePolicy,
    RerunPolicy,
)


class TestRerunEvaluationMatrix:
    def test_fresh_job_always_runs(self):
        policy = RerunPolicy()
        dec = policy.evaluate({"task_type": "t", "rerun": "never"}, is_wall=False, is_failed=False)
        assert dec.should_run
        assert not dec.should_skip
        assert dec.reason == DecisionReason.FRESH
        assert dec.action == PreflightAction.RUN

    def test_never_strategy_blocks_wall_and_failed(self):
        policy = RerunPolicy()
        dec_wall = policy.evaluate({"task_type": "t", "rerun": "never"}, is_wall=True, is_failed=False)
        assert dec_wall.should_skip
        assert dec_wall.reason == DecisionReason.WALL_BLOCKED

        dec_failed = policy.evaluate({"task_type": "t", "rerun": "never"}, is_wall=False, is_failed=True)
        assert dec_failed.should_skip
        assert dec_failed.reason == DecisionReason.FAILED_BLOCKED

    def test_every_run_strategy_allows_both(self):
        policy = RerunPolicy()
        dec_wall = policy.evaluate({"task_type": "t", "rerun": "every_run"}, is_wall=True, is_failed=False)
        assert dec_wall.should_run
        assert dec_wall.reason == DecisionReason.EVERY_RUN

        dec_failed = policy.evaluate({"task_type": "t", "rerun": "every_run"}, is_wall=False, is_failed=True)
        assert dec_failed.should_run
        assert dec_failed.reason == DecisionReason.EVERY_RUN

    def test_on_failure_strategy_allows_failed_blocks_wall(self):
        policy = RerunPolicy()
        dec_failed = policy.evaluate(
            {"task_type": "t", "rerun": "on_failure"}, is_wall=False, is_failed=True
        )
        assert dec_failed.should_run
        assert dec_failed.reason == DecisionReason.ON_FAILURE_MATCH

        dec_wall = policy.evaluate({"task_type": "t", "rerun": "on_failure"}, is_wall=True, is_failed=False)
        assert dec_wall.should_skip
        assert dec_wall.reason == DecisionReason.WALL_BLOCKED

    def test_unspecified_rerun_defaults_to_never(self):
        policy = RerunPolicy()
        dec = policy.evaluate({"task_type": "t", "rerun": None}, is_wall=True, is_failed=False)
        assert dec.should_skip
        assert dec.reason == DecisionReason.WALL_BLOCKED
        assert dec.effective_rerun == "never"

    def test_on_input_change_matrix(self, tmp_path):
        target = tmp_path / "data.bin"
        target.write_bytes(b"v1")
        st = target.stat()
        policy = RerunPolicy()
        meta = {"inputs": [{"path": str(target), "size": st.st_size, "mtime_ns": st.st_mtime_ns}]}

        # wall 命中且指纹一致 → 拦截
        dec_unchanged = policy.evaluate(
            {"task_type": "t", "rerun": "on_input_change"},
            wall_meta=meta, is_wall=True, is_failed=False,
        )
        assert dec_unchanged.should_skip
        assert dec_unchanged.reason == DecisionReason.INPUT_UNCHANGED
        assert dec_unchanged.input_changed is False

        # wall 命中且指纹变化 → 放行重跑
        target.write_bytes(b"v2-content-longer")
        policy.clear_stat_cache()
        dec_changed = policy.evaluate(
            {"task_type": "t", "rerun": "on_input_change"},
            wall_meta=meta, is_wall=True, is_failed=False,
        )
        assert dec_changed.should_run
        assert dec_changed.reason == DecisionReason.INPUT_CHANGED
        assert dec_changed.input_changed is True

        # 失败档案命中 → 放行重跑（不比对指纹）
        dec_failed = policy.evaluate(
            {"task_type": "t", "rerun": "on_input_change"}, is_wall=False, is_failed=True
        )
        assert dec_failed.should_run
        assert dec_failed.reason == DecisionReason.ON_FAILURE_MATCH

        # 无历史命中 → 全新运行
        dec_fresh = policy.evaluate(
            {"task_type": "t", "rerun": "on_input_change"}, is_wall=False, is_failed=False
        )
        assert dec_fresh.should_run
        assert dec_fresh.reason == DecisionReason.FRESH


class TestAdmitNarrowInterface:
    def test_admit_extracts_history_from_store(self):
        policy = RerunPolicy()
        store_mock = SimpleNamespace(
            wall={"t::wall_job": {}},
            failed={"t::failed_job": {}},
        )

        d_fresh = policy.admit({"task_type": "t", "job_id": "new_job", "rerun": "never"}, store_mock)
        assert d_fresh.should_run
        assert d_fresh.reason == DecisionReason.FRESH

        d_wall = policy.admit({"task_type": "t", "job_id": "wall_job", "rerun": "never"}, store_mock)
        assert d_wall.should_skip
        assert d_wall.reason == DecisionReason.WALL_BLOCKED

        d_failed = policy.admit(
            {"task_type": "t", "job_id": "failed_job", "rerun": "on_failure"}, store_mock
        )
        assert d_failed.should_run
        assert d_failed.reason == DecisionReason.ON_FAILURE_MATCH


class TestInputChangeAndStatCache:
    def test_input_changed_detects_mtime_and_size(self, tmp_path):
        data_file = tmp_path / "data.txt"
        data_file.write_text("hello")
        st = data_file.stat()

        policy = RerunPolicy()
        wall_meta = {"inputs": [{"path": str(data_file), "size": st.st_size, "mtime_ns": st.st_mtime_ns}]}
        assert not policy.check_input_changed(wall_meta)

        data_file.write_text("hello world")
        policy.clear_stat_cache()
        assert policy.check_input_changed(wall_meta)

    def test_input_changed_stat_cache_avoids_repeated_syscalls(self, tmp_path):
        data_file = tmp_path / "data.txt"
        data_file.write_text("hello")
        st = data_file.stat()

        policy = RerunPolicy(enable_stat_cache=True)
        wall_meta = {"inputs": [{"path": str(data_file), "size": st.st_size, "mtime_ns": st.st_mtime_ns}]}
        assert not policy.check_input_changed(wall_meta)

        # 文件被删但缓存命中：沿用缓存事实判定未变化
        data_file.unlink()
        assert not policy.check_input_changed(wall_meta)

        # 清除缓存后感知到文件缺失
        policy.clear_stat_cache()
        assert policy.check_input_changed(wall_meta)

    def test_input_changed_corrupt_data_fail_safe(self):
        policy = RerunPolicy()
        # 非 dict、非 list、损坏元素均安全判定为 True（放行重跑）
        assert policy.check_input_changed(None)
        assert policy.check_input_changed({})
        assert policy.check_input_changed({"inputs": "corrupt_string"})
        assert policy.check_input_changed({"inputs": [42, "bar"]})
        assert policy.check_input_changed({"inputs": [{"path": 12345}]})

    def test_input_changed_skips_uri_entries(self):
        policy = RerunPolicy()
        meta = {"inputs": [{"kind": "uri", "path": "http://example.com/a", "size": None, "mtime_ns": None}]}
        assert policy.check_input_changed(meta) is False


class TestDiscoveryPolicyNormalization:
    def test_normalize_injects_default_only_when_none(self):
        policy = RerunPolicy({"scan": "every_run"})

        job_unspecified = {"task_type": "scan", "rerun": None}
        assert policy.normalize_job_dict(job_unspecified, "scan")
        assert job_unspecified["rerun"] == "every_run"

        job_never = {"task_type": "scan", "rerun": "never"}
        assert not policy.normalize_job_dict(job_never, "scan")
        assert job_never["rerun"] == "never"

        job_on_failure = {"task_type": "scan", "rerun": "on_failure"}
        assert not policy.normalize_job_dict(job_on_failure, "scan")
        assert job_on_failure["rerun"] == "on_failure"

    def test_evaluate_uses_discovery_default_when_rerun_missing(self):
        policy = RerunPolicy({"scan": "every_run"})
        dec = policy.evaluate({"task_type": "scan"}, is_wall=True, is_failed=False)
        assert dec.should_run
        assert dec.reason == DecisionReason.EVERY_RUN
        assert dec.effective_rerun == "every_run"


class TestImmediateRequeuePolicy:
    def test_default_plan_is_immediate_front_insert(self):
        policy = ImmediateRequeuePolicy()
        plan = policy.plan_requeue({"task_type": "t", "job_id": "j"})
        assert isinstance(plan, RequeuePlan)
        assert plan.front is True
        assert plan.delay_seconds == 0.0

    def test_transient_kind_keeps_immediate_zero_budget_rhythm(self):
        """瞬态信号军规在立即策略下自然成立：零延迟、不因预算耗尽拒收。"""
        policy = ImmediateRequeuePolicy()
        job_dict = {"task_type": "t", "job_id": "j", "max_retries": 0, "attempt_no": 1}
        for kind in ("interrupted", "lock_conflict", "rate_limited"):
            plan = policy.plan_requeue(job_dict, transient_kind=kind)
            assert plan.front is True
            assert plan.delay_seconds == 0.0

    def test_seam_accepts_alternate_strategy(self):
        """接缝多态：自定义节奏策略可替换默认实现，核心不改。"""

        class TailingRequeuePolicy(RequeuePolicy):
            """尾插节奏桩：验证接缝只约定形状、不绑定默认实现。"""

            def plan_requeue(self, job_dict, *, transient_kind=None) -> RequeuePlan:
                return RequeuePlan(front=False, delay_seconds=2.5)

        policy: RequeuePolicy = TailingRequeuePolicy()
        plan = policy.plan_requeue({"task_type": "t", "job_id": "j"})
        assert plan.front is False
        assert plan.delay_seconds == 2.5
