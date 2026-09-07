"""Pipeline cascade tests: DAG cascading failures, child isolation, resource suspend, discovery schema."""

import time
import multiprocessing
from typing import TypedDict

import pytest
from tasklite.pipeline import TaskLite
from tasklite.models.job import Job
from tasklite.engine.resource import RateLimitResource, CapacityResource
from tasklite.engine.scheduler import JobScheduler
from tests.helpers import make_fake_process_class, make_ipc_process_class, make_pipeline, patch_multiprocessing_for_fakes


class TestIndirectCascade:
    """REQ-7.2: Indirect (grandchild) cascading dependency."""

    def test_indirect_cascade_grandchild(self, tmp_path, monkeypatch):
        """A→DLQ → B→JOB_DEPENDENCY → C→JOB_DEPENDENCY (B depends on A, C depends on B)."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("cascade", lambda j, c: (True, {}))

        job_a = Job("cascade", "a", payload={})
        job_b = Job("cascade", "b", payload={}, depends_on=[job_a.uid])
        job_c = Job("cascade", "c", payload={}, depends_on=[job_b.uid])

        pipeline.backend.commit_job_failure(job_a.uid, {"error": "injected"})
        pipeline.enqueue([job_a, job_b, job_c])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert job_b.uid in failed
        assert failed[job_b.uid]["error"] == "JOB_DEPENDENCY"
        assert job_c.uid in failed
        assert failed[job_c.uid]["error"] == "JOB_DEPENDENCY"


class TestChildFailureIsolation:
    """REQ-1.2: Child failure does not block parent chain."""

    def test_child_failure_does_not_block_parent(self, tmp_path, monkeypatch):
        """Parent spawns child via ctx.spawn(); child fails; parent still succeeds."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("parent", lambda j, c: (True, {}))
        pipeline.register_handler("child", lambda j, c: (True, {}))

        StatefulFakeProcess = make_ipc_process_class(results=[
            {"status": "success", "raw_result": True,
             "new_jobs": [Job("child", "c1").to_dict()],
             "resource_suspensions": [], "cursor_updates": {}},
            {"status": "error", "error": "child crashed", "traceback": "tb"},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=StatefulFakeProcess)

        pipeline.enqueue([Job("parent", "p1", payload={})])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        failed = pipeline.backend.load_failed()
        assert "parent::p1" in wall, "Parent should succeed despite child failure"
        assert "child::c1" in failed, "Child that crashed should be in DLQ"


class TestSuspendResource:
    """REQ-6.3: ctx.suspend_resource() end-to-end pipeline effect."""

    def test_suspend_resource_blocks_next_job(self, tmp_path, monkeypatch):
        """Handler suspends resource → resource state reflects suspension."""
        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(RateLimitResource("api", interval_seconds=5.0))
        pipeline.register_handler("blocker", lambda j, c: (True, {}),
                                  default_resources={"api": 1.0})

        pipeline.enqueue([Job("blocker", "j1", payload={}, resources={"api": 1.0})])

        SuspendProcess = make_ipc_process_class(results=[{
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [("api", 3600.0)],
            "cursor_updates": {},
        }])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=SuspendProcess)
        monkeypatch.setattr("time.sleep", lambda s: None)

        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "blocker::j1" in wall
        res = pipeline.resources["api"]
        assert res.next_available > time.monotonic()


class TestPayloadSchemaUnexpectedField:
    """validate_payload 对未知字段的拒绝行为（框架保留字段豁免已随旧
    discovery 移除）。"""

    def test_non_internal_unexpected_field_still_rejected(self, tmp_path):
        """Non-underscore unexpected fields are still rejected."""
        from typing import TypedDict

        class SimpleSchema(TypedDict):
            name: str
            age: int

        from tasklite.utils import validate_payload
        errors = validate_payload(
            {"name": "x", "age": 1, "bogus": True}, SimpleSchema
        )
        assert len(errors) == 1
        assert "unexpected field 'bogus'" in errors[0]

    def test_seen_ids_now_rejected(self, tmp_path):
        """旧 discovery 的 _seen_ids 保留字段已移除 → 现在按未知字段拒绝。"""
        from typing import TypedDict

        class ArtistSchema(TypedDict):
            artist_id: str
            page: int

        from tasklite.utils import validate_payload
        errors = validate_payload(
            {"artist_id": "a1", "page": 1, "_seen_ids": ["id1", "id2"]},
            ArtistSchema,
        )
        assert any("_seen_ids" in e for e in errors), f"expected rejection: {errors}"


# ─── DAG edge case topologies (diamond, self-dep, missing dep, multi-level) ──


class TestDAGWeirdTopologies:
    """REQ-7 edge case DAG shapes: diamond, self-dep, missing dep, multi-level cascade."""

    def _setup(self, tmp_path, monkeypatch):
        """Shared setup: pipeline + success FakeProcess mocks."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("dag", lambda j, c: (True, {}))

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        return pipeline

    def test_diamond_dependency_all_succeed(self, tmp_path, monkeypatch):
        """Diamond A→B, A→C, B→D, C→D: all succeed → all 4 in wall."""
        pipeline = self._setup(tmp_path, monkeypatch)

        a = Job("dag", "a", payload={})
        b = Job("dag", "b", payload={}, depends_on=[a.uid])
        c = Job("dag", "c", payload={}, depends_on=[a.uid])
        d = Job("dag", "d", payload={}, depends_on=[b.uid, c.uid])
        pipeline.enqueue([a, b, c, d])

        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert a.uid in wall
        assert b.uid in wall
        assert c.uid in wall
        assert d.uid in wall

    def test_diamond_dependency_root_fails_cascades(self, tmp_path, monkeypatch):
        """Diamond: A fails → B, C, D all JOB_DEPENDENCY."""
        pipeline = self._setup(tmp_path, monkeypatch)

        a = Job("dag", "a", payload={})
        b = Job("dag", "b", payload={}, depends_on=[a.uid])
        c = Job("dag", "c", payload={}, depends_on=[a.uid])
        d = Job("dag", "d", payload={}, depends_on=[b.uid, c.uid])

        # Pre-fail A so run loads it from failed_data
        pipeline.backend.commit_job_failure(a.uid, {"error": "injected"})
        pipeline.enqueue([b, c, d])

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert b.uid in failed
        assert failed[b.uid]["error"] == "JOB_DEPENDENCY"
        assert c.uid in failed
        assert failed[c.uid]["error"] == "JOB_DEPENDENCY"
        assert d.uid in failed
        assert failed[d.uid]["error"] == "JOB_DEPENDENCY"

    def test_diamond_dependency_middle_fails(self, tmp_path, monkeypatch):
        """Diamond: A succeeds, B fails → C succeeds, D JOB_DEPENDENCY (B failed)."""
        pipeline = self._setup(tmp_path, monkeypatch)

        a = Job("dag", "a", payload={})
        b = Job("dag", "b", payload={}, depends_on=[a.uid])
        c = Job("dag", "c", payload={}, depends_on=[a.uid])
        d = Job("dag", "d", payload={}, depends_on=[b.uid, c.uid])

        # Pre-succeed A, pre-fail B
        pipeline.backend.commit_job_success(a.uid, {}, cursor_updates={})
        pipeline.backend.commit_job_failure(b.uid, {"error": "injected"})
        pipeline.enqueue([c, d])

        pipeline.run()

        wall = pipeline.backend.load_wall()
        failed = pipeline.backend.load_failed()
        # C should succeed (A is in wall)
        assert c.uid in wall
        # D should JOB_DEPENDENCY because B failed
        assert d.uid in failed
        assert failed[d.uid]["error"] == "JOB_DEPENDENCY"

    def test_self_dependency_deadlock(self, tmp_path, monkeypatch):
        """Job depends on its own uid → DEPENDENCY_DEADLOCK (waiting_for_dependency + min_wait=inf)."""
        pipeline = self._setup(tmp_path, monkeypatch)

        # Self-dependency: job's uid is "dag::self1", depends_on itself
        job = Job("dag", "self1", payload={}, depends_on=["dag::self1"])
        pipeline.enqueue([job])

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert job.uid in failed
        assert failed[job.uid]["error"] == "DEPENDENCY_DEADLOCK"

    def test_dependency_on_nonexistent_job_deadlock(self, tmp_path, monkeypatch):
        """depends_on a uid not in wall/failed → DEPENDENCY_DEADLOCK."""
        pipeline = self._setup(tmp_path, monkeypatch)

        job = Job("dag", "waiter", payload={}, depends_on=["dag::ghost"])
        pipeline.enqueue([job])

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert job.uid in failed
        assert failed[job.uid]["error"] == "DEPENDENCY_DEADLOCK"

    def test_multi_level_cascade_four_deep(self, tmp_path, monkeypatch):
        """A→B→C→D linear chain; A fails → B, C, D all JOB_DEPENDENCY."""
        pipeline = self._setup(tmp_path, monkeypatch)

        a = Job("dag", "a", payload={})
        b = Job("dag", "b", payload={}, depends_on=[a.uid])
        c = Job("dag", "c", payload={}, depends_on=[b.uid])
        d = Job("dag", "d", payload={}, depends_on=[c.uid])

        pipeline.backend.commit_job_failure(a.uid, {"error": "injected"})
        pipeline.enqueue([b, c, d])

        pipeline.run()

        failed = pipeline.backend.load_failed()
        for j in (b, c, d):
            assert j.uid in failed
            assert failed[j.uid]["error"] == "JOB_DEPENDENCY"

    def test_dependency_already_in_wall_runs_immediately(self, tmp_path, monkeypatch):
        """B depends on A, A already in wall → B runs without waiting."""
        pipeline = self._setup(tmp_path, monkeypatch)

        a = Job("dag", "a", payload={})
        b = Job("dag", "b", payload={}, depends_on=[a.uid])

        # Pre-populate wall with A
        pipeline.backend.commit_job_success(a.uid, {}, cursor_updates={})
        pipeline.enqueue([b])

        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert b.uid in wall, "B should run immediately since A is already in wall"

    def test_dependency_already_in_failed_cascades(self, tmp_path, monkeypatch):
        """B depends on A, A already in failed → B JOB_DEPENDENCY immediately."""
        pipeline = self._setup(tmp_path, monkeypatch)

        a = Job("dag", "a", payload={})
        b = Job("dag", "b", payload={}, depends_on=[a.uid])

        pipeline.backend.commit_job_failure(a.uid, {"error": "pre-injected"})
        pipeline.enqueue([b])

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert b.uid in failed
        assert failed[b.uid]["error"] == "JOB_DEPENDENCY"
        assert failed[b.uid].get("failed_dependency") == a.uid

    def test_diamond_plus_linear_in_same_queue(self, tmp_path, monkeypatch):
        """Mix diamond (A→B, A→C, B→D, C→D) with linear (E→F) in one queue."""
        pipeline = self._setup(tmp_path, monkeypatch)

        a = Job("dag", "a", payload={})
        b = Job("dag", "b", payload={}, depends_on=[a.uid])
        c = Job("dag", "c", payload={}, depends_on=[a.uid])
        d = Job("dag", "d", payload={}, depends_on=[b.uid, c.uid])
        e = Job("dag", "e", payload={})
        f = Job("dag", "f", payload={}, depends_on=[e.uid])

        pipeline.enqueue([a, b, c, d, e, f])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        for j in (a, b, c, d, e, f):
            assert j.uid in wall

    def test_dependency_on_multiple_parents_one_fails(self, tmp_path, monkeypatch):
        """D depends on [B, C]; B fails, C succeeds → D JOB_DEPENDENCY."""
        pipeline = self._setup(tmp_path, monkeypatch)

        b = Job("dag", "b", payload={})
        c = Job("dag", "c", payload={})
        d = Job("dag", "d", payload={}, depends_on=[b.uid, c.uid])

        # Pre-fail B, pre-succeed C
        pipeline.backend.commit_job_failure(b.uid, {"error": "injected"})
        pipeline.backend.commit_job_success(c.uid, {}, cursor_updates={})
        pipeline.enqueue([d])

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert d.uid in failed
        assert failed[d.uid]["error"] == "JOB_DEPENDENCY"
        # The first failed dependency encountered should be B
        assert failed[d.uid].get("failed_dependency") == b.uid

    def test_empty_depends_on_list_runs_immediately(self, tmp_path, monkeypatch):
        """depends_on=[] (explicit empty) is equivalent to no dependency."""
        pipeline = self._setup(tmp_path, monkeypatch)

        job = Job("dag", "free", payload={}, depends_on=[])
        pipeline.enqueue([job])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert job.uid in wall


class TestSpawnDeduplication:
    """动态生成的子任务去重。"""

    def test_spawn_duplicate_job_is_skipped(self, tmp_path, monkeypatch):
        """Handler spawn 已存在的作业时，不应重复加入队列。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("parent", lambda j, c: (True, {}))
        pipeline.register_handler("child", lambda j, c: (True, {}))

        SpawnDuplicateProcess = make_ipc_process_class(results=[{
            "status": "success", "raw_result": True,
            "new_jobs": [Job("child", "c1").to_dict(), Job("child", "c2").to_dict()],
            "resource_suspensions": [],
            "cursor_updates": {},
        }])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=SpawnDuplicateProcess)

        pipeline.enqueue([Job("parent", "p1", payload={}), Job("child", "c1", payload={})])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "parent::p1" in wall
        # 队列中已有该 job，spawn 的重复项被去重，原 job 正常执行
        assert "child::c1" in wall
        assert "child::c2" in wall

    def test_spawn_rerun_job_in_queue_is_blocked(self, tmp_path):
        """spawn 去重时 queue/in-flight 命中**优先于** rerun 豁免。

        修复前：every_run 任务同时属于 wall（历史成功）与 queue（重跑中）时，
        旧逻辑先查 wall 命中的 rerun 豁免（every_run 放行）→ 绕过 queue 检查 →
        同 uid 重复入队 → commit 时 INSERT 冲突 → rowcount 守卫崩溃循环。
        修复后：queue/in-flight 命中无条件拦截（同轮不重复派发）。"""
        from tasklite.engine.channel import ExecutionResult
        from tasklite.models.state import PipelineState

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("parent", lambda j, c: (True, {}))
        pipeline.register_handler("child", lambda j, c: (True, {}))
        # X 历史成功（wall）
        pipeline.backend.commit_job_success("child::x", {"run_count": 1}, cursor_updates={})
        # X 重跑中（queue，every_run 放行重跑）
        pipeline.enqueue([Job("child", "x", payload={}, rerun="every_run")])
        state = PipelineState(
            pipeline.backend.load_wall(), pipeline.backend.load_failed(),
            pipeline.backend.load_cursors(), pipeline.backend.load_queue(),
        )
        pipeline.runtime.ctx.set_state(state)

        # parent::p1 成功并 spawn 同 uid X
        from tasklite.engine.runtime import inject_worker_resource
        job = Job("parent", "p1", payload={})
        job_dict = job.to_dict()
        inject_worker_resource(job_dict)
        result = ExecutionResult(
            success=True,
            new_jobs=[Job("child", "x", payload={}, rerun="every_run")],
        )
        pipeline.runtime._completion.apply_result("parent::p1", job, job_dict, result, expect_in_flight=False)

        q = pipeline.backend.load_queue()
        x_count = sum(1 for jd in q
                      if f"{jd['task_type']}::{jd['job_id']}" == "child::x")
        assert x_count == 1, f"spawn 必须被 queue 命中拦截，实际 queue: {q}"
        wall = pipeline.backend.load_wall()
        assert "parent::p1" in wall


class TestSchedulerDeadlockClassification:
    """调度器死锁分类。"""

    def test_dependency_plus_unknown_resource_reports_resource_deadlock(self):
        """同时存在缺失依赖和未知资源时，应报告 RESOURCE_DEADLOCK。"""
        scheduler = JobScheduler(resources={})
        # 该作业依赖缺失，且引用未知资源
        job = Job("t", "j1", payload={}, depends_on=["missing::dep"], resources={"ghost": 1.0})
        from tasklite.models.state import PipelineState
        result = scheduler.pop_next_runnable(
            PipelineState({}, {}, {}, [job.to_dict()]),
        )
        assert result.runnable_idx is None
        assert len(result.unknown_resource_indices) == 1
        assert result.waiting_for_dependency is True


class TestOnJobCompletedBatchPaths:
    """on_job_completed 钩子覆盖批量 DLQ 终态路径。

    修复前：级联批量 DLQ（_cascade_fail）、死锁批量 DLQ（_handle_deadlock）
    不触发钩子——docstring 承诺「每个 job 终结」但实现只覆盖直接 commit
    路径。以下测试锁定补全后的覆盖。"""

    def _recording_pipeline(self, tmp_path):
        calls = []
        pipeline = make_pipeline(tmp_path)
        pipeline.on_job_completed = (
            lambda uid, meta, success, going_to_retry: calls.append((uid, success, going_to_retry))
        )
        return pipeline, calls

    def test_cascade_dlq_fires_hook(self, tmp_path, monkeypatch):
        """父失败 → 级联 DLQ 下游：钩子必须收到每个级联 job 的终态调用
        （c 经 _cascade_fail 批量路径；b 经 pending_dep_failure 直接 commit 路径）。
        dedup skip（a 本就在 failed）不算终结，不触发钩子。"""
        pipeline, calls = self._recording_pipeline(tmp_path)
        pipeline.register_handler("cascade", lambda j, c: (True, {}))
        pipeline.backend.commit_job_failure("cascade::a", {"error": "injected"})
        pipeline.enqueue([
            Job("cascade", "a", payload={}),
            Job("cascade", "b", payload={}, depends_on=["cascade::a"]),
            Job("cascade", "c", payload={}, depends_on=["cascade::b"]),
        ])
        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        pipeline.run()

        by_uid = {uid: (success, going_to_retry) for uid, success, going_to_retry in calls}
        assert by_uid["cascade::b"] == (False, False)
        assert by_uid["cascade::c"] == (False, False)  # 级联批量路径（修复前不触发）
        assert "cascade::a" not in by_uid  # dedup skip 非终结，不触发

    def test_deadlock_dlq_fires_hook(self, tmp_path):
        """impossible resource 死锁 → 批量 DLQ（_handle_deadlock）：
        钩子必须收到肇事者终态调用；stats["failed"] 同步递增（修复：
        死锁批量路径此前缺 stats 递增，违反「钩子在 stats 更新后调用」契约）。"""
        pipeline, calls = self._recording_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("gpu", max_capacity=5.0))
        pipeline.register_handler("heavy", lambda j, c: (True, {}))
        pipeline.enqueue([Job("heavy", "j1", payload={}, resources={"gpu": 10.0})])
        pipeline.run()

        by_uid = {uid: (success, going_to_retry) for uid, success, going_to_retry in calls}
        assert by_uid["heavy::j1"] == (False, False)
        assert pipeline.stats["failed"] == 1

    def test_deadlock_dlq_hook_isolates_errors(self, tmp_path):
        """钩子抛异常必须被隔离（契约 2）——批量 DLQ 路径下异常不阻断主循环，
        stats["hook_errors"] 计数。"""
        pipeline, calls = self._recording_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("gpu", max_capacity=5.0))
        pipeline.register_handler("heavy", lambda j, c: (True, {}))

        def _boom(uid, meta, success, going_to_retry):
            calls.append((uid, success, going_to_retry))
            raise RuntimeError("hook failure")

        pipeline.on_job_completed = _boom
        pipeline.enqueue([Job("heavy", "j1", payload={}, resources={"gpu": 10.0})])
        pipeline.run()

        assert len(calls) == 1
        assert pipeline.stats.get("hook_errors", 0) == 1

    def test_3strike_subprocess_dlq_fires_hook_exactly_once(self, tmp_path, monkeypatch):
        """3-strike commit 失败 DLQ（子进程路径）：钩子恰好调用一次。

         曾引入双触发：_commit_failed_crash 的 3-strike 分支触发钩子后
        raise _JobTerminated → _complete_job 承接 → 尾部 _fire_job_completed
        再触发一次（第二次 success 标志还与实际 DLQ 矛盾）。 修复：
        except 分支标记终结、跳过尾部触发。本测试锁死「恰好一次」+ meta 完整。
        """
        pipeline, _ = self._recording_pipeline(tmp_path)
        calls = []
        pipeline.on_job_completed = (
            lambda uid, meta, success, going_to_retry: calls.append((uid, meta, success, going_to_retry))
        )
        pipeline.register_handler("t", lambda j, c: (True, {"size": 1}))

        # 注入 _commit_failures=2：本次 commit_job_success 失败即达 3-strike
        jd = Job("t", "j1", payload={}).to_dict()
        jd["runtime"] = {"_commit_failures": 2}
        import sqlite3 as sqlite3_mod
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (__import__("json").dumps(jd),))
        conn.commit()
        conn.close()

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        # 第 1 次 commit_job_success 返回 False（触发 _commit_failed_crash → 3-strike）
        real_success = pipeline.backend.commit_job_success
        call_count = [0]
        def fake_success(*a, **kw):
            call_count[0] += 1
            if call_count[0] >= 2:
                return real_success(*a, **kw)
            return False
        monkeypatch.setattr(pipeline.backend, "commit_job_success", fake_success)

        pipeline.run()

        assert len(calls) == 1, f"钩子必须恰好调用一次，实际 {len(calls)} 次: {calls}"
        uid, meta, success, going_to_retry = calls[0]
        assert uid == "t::j1"
        assert "COMMIT_FAILURE_DLQ" in str(meta.get("error", "")), \
            f"meta 应含 COMMIT_FAILURE_DLQ，实际: {meta}"
        assert success is False, "3-strike 终结不是成功"


class TestCascadeFailedIndependentCounter:
    """级联下游独立计数：下游 JOB_DEPENDENCY 失败计 cascade_failed，不占 failed。

    防回归点：apply_failed 的 count_as 桶参数——若级联路径误用默认 "failed"，
    failed 会虚高（父 1 + 下游 N），监控「真实业务失败率」失真。
    """

    def test_cascade_counts_separate_from_failed(self, tmp_path, monkeypatch):
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("cascade", lambda j, c: (True, {}))

        job_a = Job("cascade", "a", payload={})
        job_b = Job("cascade", "b", payload={}, depends_on=[job_a.uid])
        job_c = Job("cascade", "c", payload={}, depends_on=[job_b.uid])

        pipeline.backend.commit_job_failure(job_a.uid, {"error": "injected"})
        pipeline.enqueue([job_a, job_b, job_c])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        stats = pipeline.stats
        # 父 A 为历史失败（enqueue 前已 commit_failure，不经本轮
        # apply_failed）；两个下游各占 cascade_failed——级联不稀释 failed
        assert stats["failed"] == 0, f"级联下游不得计入 failed: {stats}"
        assert stats["cascade_failed"] == 2, f"两个下游各计一次 cascade_failed: {stats}"
        assert stats["completed"] == 0
        # DLQ/failed 表含全部三条终态（父 error + 下游 JOB_DEPENDENCY×2）
        failed_table = pipeline.backend.load_failed()
        assert {job_a.uid, job_b.uid, job_c.uid} <= set(failed_table)

    def test_empty_stats_carries_cascade_failed_key(self):
        from tasklite.engine.runtime import EMPTY_STATS
        assert "cascade_failed" in EMPTY_STATS
        assert EMPTY_STATS["cascade_failed"] == 0
