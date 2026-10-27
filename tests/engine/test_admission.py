"""v2 准入与重入队策略深模块测试：rerun 矩阵、输入 mtime 与可信基线判据、节奏接缝。"""

from __future__ import annotations

import os
from types import SimpleNamespace

from tasklite.engine.admission import (
    DecisionReason,
    ImmediateRequeuePolicy,
    PreflightAction,
    RequeuePlan,
    RequeuePolicy,
    RerunPolicy,
    extract_trusted_metadata_timestamp_us,
    is_trusted_success_metadata,
)


def _trusted_meta(
    inputs: list[str],
    *,
    success_count: object = 1,
    last_succeeded_at: str = "2026-05-01T00:00:00+00:00",
) -> dict:
    return {
        "success_count": success_count,
        "last_succeeded_at": last_succeeded_at,
        "last_success_run_id": "run_seed",
        "inputs": inputs,
    }


class TestRerunEvaluationMatrix:
    def test_fresh_job_always_runs(self):
        policy = RerunPolicy()
        dec = policy.evaluate({"task_type": "t", "rerun": "never"}, is_success=False, is_failed=False)
        assert dec.should_run
        assert not dec.should_skip
        assert dec.reason == DecisionReason.FRESH
        assert dec.action == PreflightAction.RUN

    def test_never_strategy_blocks_wall_and_failed(self):
        policy = RerunPolicy()
        dec_wall = policy.evaluate({"task_type": "t", "rerun": "never"}, is_success=True, is_failed=False)
        assert dec_wall.should_skip
        assert dec_wall.reason == DecisionReason.SUCCESS_BLOCKED
        assert dec_wall.reason.value == "success_blocked"

        dec_failed = policy.evaluate({"task_type": "t", "rerun": "never"}, is_success=False, is_failed=True)
        assert dec_failed.should_skip
        assert dec_failed.reason == DecisionReason.FAILED_BLOCKED
        assert dec_failed.reason.value == "failed_blocked"

    def test_every_run_strategy_allows_both(self):
        policy = RerunPolicy()
        dec_wall = policy.evaluate({"task_type": "t", "rerun": "every_run"}, is_success=True, is_failed=False)
        assert dec_wall.should_run
        assert dec_wall.reason == DecisionReason.EVERY_RUN

        dec_failed = policy.evaluate({"task_type": "t", "rerun": "every_run"}, is_success=False, is_failed=True)
        assert dec_failed.should_run
        assert dec_failed.reason == DecisionReason.EVERY_RUN

    def test_on_failure_strategy_allows_failed_blocks_wall(self):
        policy = RerunPolicy()
        dec_failed = policy.evaluate(
            {"task_type": "t", "rerun": "on_failure"}, is_success=False, is_failed=True
        )
        assert dec_failed.should_run
        assert dec_failed.reason == DecisionReason.ON_FAILURE_MATCH

        dec_wall = policy.evaluate({"task_type": "t", "rerun": "on_failure"}, is_success=True, is_failed=False)
        assert dec_wall.should_skip
        assert dec_wall.reason == DecisionReason.SUCCESS_BLOCKED

    def test_unspecified_rerun_defaults_to_never(self):
        policy = RerunPolicy()
        dec = policy.evaluate({"task_type": "t", "rerun": None}, is_success=True, is_failed=False)
        assert dec.should_skip
        assert dec.reason == DecisionReason.SUCCESS_BLOCKED
        assert dec.effective_rerun == "never"

    def test_on_input_change_matrix(self, tmp_path):
        target = tmp_path / "data.bin"
        target.write_bytes(b"v1")
        mtime_us = target.stat().st_mtime_ns // 1_000
        policy = RerunPolicy()
        meta = _trusted_meta([str(target)])

        # successes 命中且输入 mtime 不晚于可信基线 → 拦截
        dec_unchanged = policy.evaluate(
            {"task_type": "t", "rerun": "on_input_change"},
            success_metadata=meta, is_success=True, is_failed=False,
            baseline_lookup=lambda: mtime_us,
        )
        assert dec_unchanged.should_skip
        assert dec_unchanged.reason == DecisionReason.INPUT_UNCHANGED
        assert dec_unchanged.input_changed is False

        # 输入 mtime 严格晚于基线 → 放行重跑
        dec_changed = policy.evaluate(
            {"task_type": "t", "rerun": "on_input_change"},
            success_metadata=meta, is_success=True, is_failed=False,
            baseline_lookup=lambda: mtime_us - 1,
        )
        assert dec_changed.should_run
        assert dec_changed.reason == DecisionReason.INPUT_CHANGED
        assert dec_changed.input_changed is True

        # 无可信基线 → 保守放行
        dec_no_baseline = policy.evaluate(
            {"task_type": "t", "rerun": "on_input_change"},
            success_metadata=meta, is_success=True, is_failed=False,
            baseline_lookup=lambda: None,
        )
        assert dec_no_baseline.should_run

        # 失败档案命中 → 放行重跑（不查询基线）
        def _must_not_call():
            raise AssertionError("baseline lookup must be lazy")

        dec_failed = policy.evaluate(
            {"task_type": "t", "rerun": "on_input_change"}, is_success=False, is_failed=True,
            baseline_lookup=_must_not_call,
        )
        assert dec_failed.should_run
        assert dec_failed.reason == DecisionReason.ON_FAILURE_MATCH

        # 无历史命中 → 全新运行
        dec_fresh = policy.evaluate(
            {"task_type": "t", "rerun": "on_input_change"}, is_success=False, is_failed=False,
            baseline_lookup=_must_not_call,
        )
        assert dec_fresh.should_run
        assert dec_fresh.reason == DecisionReason.FRESH


class TestAdmitNarrowInterface:
    def test_admit_extracts_history_from_store(self):
        policy = RerunPolicy()
        store_mock = SimpleNamespace(
            successes={"t::wall_job": {}},
            failed={"t::failed_job": {}},
        )

        d_fresh = policy.admit({"task_type": "t", "job_id": "new_job", "rerun": "never"}, store_mock)
        assert d_fresh.should_run
        assert d_fresh.reason == DecisionReason.FRESH

        d_wall = policy.admit({"task_type": "t", "job_id": "wall_job", "rerun": "never"}, store_mock)
        assert d_wall.should_skip
        assert d_wall.reason == DecisionReason.SUCCESS_BLOCKED

        d_failed = policy.admit(
            {"task_type": "t", "job_id": "failed_job", "rerun": "on_failure"}, store_mock
        )
        assert d_failed.should_run
        assert d_failed.reason == DecisionReason.ON_FAILURE_MATCH


class TestInputChangeAndStatCache:
    def test_equal_microsecond_does_not_trigger_strictly_later_does(self, tmp_path):
        data_file = tmp_path / "data.txt"
        data_file.write_text("hello")
        base_ns = 1_700_000_000_123_456_000
        os.utime(data_file, ns=(base_ns, base_ns + 999))
        baseline_us = base_ns // 1_000
        policy = RerunPolicy()
        meta = _trusted_meta([str(data_file)])
        # 同一微秒内的亚微秒差（999ns）截断后相等 → 不触发
        assert not policy.check_input_changed(meta, last_succeeded_at_us=baseline_us)
        os.utime(data_file, ns=(base_ns, base_ns + 1_000))
        assert policy.check_input_changed(meta, last_succeeded_at_us=baseline_us)

    def test_size_only_change_with_same_mtime_does_not_trigger(self, tmp_path):
        data_file = tmp_path / "data.txt"
        data_file.write_text("hello")
        base_ns = 1_700_000_000_000_000_000
        os.utime(data_file, ns=(base_ns, base_ns))
        policy = RerunPolicy()
        meta = _trusted_meta([str(data_file)])
        data_file.write_text("hello world, longer")
        os.utime(data_file, ns=(base_ns, base_ns))
        assert not policy.check_input_changed(meta, last_succeeded_at_us=base_ns // 1_000)

    def test_directory_input_stats_directory_itself_only(self, tmp_path):
        folder = tmp_path / "dir"
        folder.mkdir()
        base_ns = 1_700_000_000_000_000_000
        os.utime(folder, ns=(base_ns, base_ns))
        policy = RerunPolicy()
        meta = _trusted_meta([str(folder)])
        nested = tmp_path / "dir_sub"
        nested.mkdir()
        assert not policy.check_input_changed(meta, last_succeeded_at_us=base_ns // 1_000)
        (folder / "new.txt").write_text("x")
        os.utime(folder, ns=(base_ns + 5_000, base_ns + 5_000))
        assert policy.check_input_changed(meta, last_succeeded_at_us=base_ns // 1_000)

    def test_default_baseline_extracted_from_trusted_metadata(self, tmp_path):
        data_file = tmp_path / "data.txt"
        data_file.write_text("hello")
        base_ns = 1_700_000_000_000_000_000
        os.utime(data_file, ns=(base_ns, base_ns))
        policy = RerunPolicy()
        meta = _trusted_meta([str(data_file)], last_succeeded_at="2023-11-14T22:13:20+00:00")
        assert not policy.check_input_changed(meta)
        assert policy.check_input_changed({"inputs": [str(data_file)]})

    def test_input_changed_stat_cache_avoids_repeated_syscalls(self, tmp_path):
        data_file = tmp_path / "data.txt"
        data_file.write_text("hello")
        baseline_us = data_file.stat().st_mtime_ns // 1_000

        policy = RerunPolicy(enable_stat_cache=True)
        meta = _trusted_meta([str(data_file)])
        assert not policy.check_input_changed(meta, last_succeeded_at_us=baseline_us)

        # 文件被删但缓存命中：沿用缓存事实判定未变化
        data_file.unlink()
        assert not policy.check_input_changed(meta, last_succeeded_at_us=baseline_us)

        # 清除缓存后感知到文件缺失
        policy.clear_stat_cache()
        assert policy.check_input_changed(meta, last_succeeded_at_us=baseline_us)

    def test_input_changed_corrupt_data_fail_safe(self):
        policy = RerunPolicy()
        # 缺基线、非 list、空 list、非 str 元素均安全判定为 True（放行重跑）
        assert policy.check_input_changed(None)
        assert policy.check_input_changed({})
        assert policy.check_input_changed({"inputs": "corrupt"}, last_succeeded_at_us=1)
        assert policy.check_input_changed({"inputs": []}, last_succeeded_at_us=1)
        assert policy.check_input_changed({"inputs": [42]}, last_succeeded_at_us=1)
        assert policy.check_input_changed({"inputs": [{"path": "/x"}]}, last_succeeded_at_us=1)
        assert policy.check_input_changed({"inputs": ["/x"]}, last_succeeded_at_us=True)


class TestTrustedMetadataFilter:
    def test_seed_empty_and_incomplete_metadata_untrusted(self):
        assert extract_trusted_metadata_timestamp_us(None) is None
        assert extract_trusted_metadata_timestamp_us({}) is None
        assert not is_trusted_success_metadata({"last_succeeded_at": "2026-05-01T00:00:00+00:00"})
        assert not is_trusted_success_metadata(_trusted_meta([], success_count=0))
        assert not is_trusted_success_metadata(_trusted_meta([], success_count=True))
        assert not is_trusted_success_metadata(_trusted_meta([], last_succeeded_at="garbage"))
        assert not is_trusted_success_metadata(_trusted_meta([], last_succeeded_at="2026-05-01T00:00:00"))
        bad_run = _trusted_meta([])
        bad_run["last_success_run_id"] = ""
        assert not is_trusted_success_metadata(bad_run)

    def test_complete_framework_metadata_trusted_in_microseconds(self):
        meta = _trusted_meta([], last_succeeded_at="1970-01-01T00:00:01.000002+00:00")
        assert extract_trusted_metadata_timestamp_us(meta) == 1_000_002


class TestDiscoveryPolicyNormalization:
    def test_normalize_injects_default_only_when_none(self):
        policy = RerunPolicy({"scan": "every_run"})

        job_unspecified = {"task_type": "scan", "rerun": None}
        assert policy.normalize_job_snapshot(job_unspecified, "scan")
        assert job_unspecified["rerun"] == "every_run"

        job_never = {"task_type": "scan", "rerun": "never"}
        assert not policy.normalize_job_snapshot(job_never, "scan")
        assert job_never["rerun"] == "never"

        job_on_failure = {"task_type": "scan", "rerun": "on_failure"}
        assert not policy.normalize_job_snapshot(job_on_failure, "scan")
        assert job_on_failure["rerun"] == "on_failure"

    def test_evaluate_uses_discovery_default_when_rerun_missing(self):
        policy = RerunPolicy({"scan": "every_run"})
        dec = policy.evaluate({"task_type": "scan"}, is_success=True, is_failed=False)
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
        job_snapshot = {"task_type": "t", "job_id": "j", "max_retries": 0, "attempt_no": 1}
        for kind in ("interrupted", "lock_conflict", "rate_limited"):
            plan = policy.plan_requeue(job_snapshot, transient_kind=kind)
            assert plan.front is True
            assert plan.delay_seconds == 0.0

    def test_seam_accepts_alternate_strategy(self):
        """接缝多态：自定义节奏策略可替换默认实现，核心不改。"""

        class TailingRequeuePolicy(RequeuePolicy):
            """尾插节奏桩：验证接缝只约定形状、不绑定默认实现。"""

            def plan_requeue(self, job_snapshot, *, transient_kind=None) -> RequeuePlan:
                return RequeuePlan(front=False, delay_seconds=2.5)

        policy: RequeuePolicy = TailingRequeuePolicy()
        plan = policy.plan_requeue({"task_type": "t", "job_id": "j"})
        assert plan.front is False
        assert plan.delay_seconds == 2.5
