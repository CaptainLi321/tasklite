"""Core pipeline integration tests: scheduling, resources, dependencies, enqueue, normalization.

Tests extracted from the original monolithic test file into:
- test_pipeline_error.py (timeout, fatal, retry, crashes, output, keyboard interrupt)
- test_pipeline_cascade.py (DAG cascading, child isolation, resource suspend, discovery schema)
- test_pipeline_backend.py (JSON backend lifecycle, backoff persistence)
- test_pipeline_real.py (real mp.Process subprocess tests)
- test_backoff.py (backoff computation + defaults)
- test_job.py (FATAL_EXCEPTIONS tuple, Job defaults)
- test_sandbox.py (path sandbox)
"""

import json
import time
import queue
import multiprocessing

import pytest

from tasklite.pipeline import TaskLite
from tasklite.models.job import Job
from tasklite.engine.resource import RateLimitResource, CapacityResource
from tests.helpers import _write_fake_result, make_fake_process_class, make_ipc_process_class, make_pipeline, patch_multiprocessing_for_fakes


# ─── Helpers ────────────────────────────────────────────────────────────────

def _register_simple_handler(pipeline, task_type="test", succeed=True, meta=None):
    """Register a handler that returns success/failure."""
    def handler(job, ctx):
        if not succeed:
            raise Exception("failure")
        return True, meta or {}
    pipeline.register_handler(task_type, handler)


# ─── Tests ──────────────────────────────────────────────────────────────────

class TestEmptyQueue:
    """Tests for empty or trivial queue states."""

    def test_empty_queue_finishes(self, tmp_path):
        """Pipeline with empty queue completes immediately."""
        pipeline = make_pipeline(tmp_path)
        pipeline.run()
        # Should not raise, just finish

    def test_unknown_backend_raises_valueerror(self, tmp_path):
        """非法 backend 字符串应抛出 ValueError，而不是静默回退到 JSON。"""
        with pytest.raises(ValueError, match="Unknown backend: 'postgres'"):
            TaskLite(name="bad", state_dir=tmp_path, backend="postgres")

    def test_enqueue_empty_list_noop(self, tmp_path):
        """enqueue([]) does not mutate queue or raise."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))
        pipeline.enqueue([])
        assert pipeline.backend.load_queue() == []

    def test_register_and_enqueue_emit_no_deprecation_warning(self, tmp_path):
        """标准六步调用序列（构造→注册→入队）在用户调用点不得收到
        DeprecationWarning——内部路径不得触达过渡期门面属性。"""
        import warnings as warnings_mod

        with warnings_mod.catch_warnings(record=True) as caught:
            warnings_mod.simplefilter("always")
            pipeline = make_pipeline(tmp_path)
            pipeline.register_handler("test", lambda j, c: (True, {}))
            pipeline.enqueue([Job("test", "j1", payload={})])

        deprecations = [w for w in caught
                        if issubclass(w.category, DeprecationWarning)]
        assert deprecations == [], (
            "register_handler/enqueue 泄漏 DeprecationWarning 至用户调用点: "
            f"{[str(w.message) for w in deprecations]}"
        )

    def test_run_called_twice_idempotent(self, tmp_path, monkeypatch):
        """Calling run() twice on a finished pipeline is a no-op.

        原断言只查 wall/queue 不变——若第二次 run()
        仍派发执行（副作用），只要最终状态未变测试照样绿。FakeProcess 不执行
        handler（直接写 canned 结果），故用「进程创建计数」锁定零派发：
        第二次 run 必须不创建任何执行体。
        """
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))

        FakeP = make_fake_process_class("success")
        created = []

        class CountingFakeProcess(FakeP):
            def __init__(self, *a, **kw):
                super().__init__(*a, **kw)
                created.append(1)

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=CountingFakeProcess)
        pipeline.enqueue([Job("test", "j1", payload={})])

        pipeline.run()
        wall_after_first = dict(pipeline.backend.load_wall())
        queue_after_first = pipeline.backend.load_queue()
        assert len(created) == 1, f"first run must dispatch exactly once, got {len(created)}"

        pipeline.run()  # second run should be no-op
        assert pipeline.backend.load_wall() == wall_after_first
        assert pipeline.backend.load_queue() == queue_after_first
        assert len(created) == 1, (
            f"second run must NOT dispatch (zero new processes), got {len(created)}"
        )

    def test_handler_not_found_breaks(self, tmp_path, monkeypatch):
        """Enqueueing job with unregistered task_type → sent to DLQ, queue continues."""
        pipeline = make_pipeline(tmp_path)
        pipeline.enqueue([Job("no_such_handler", "j1", payload={})])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        remaining = pipeline.backend.load_queue()
        assert len(remaining) == 0
        failed = pipeline.backend.load_failed()
        assert "no_such_handler::j1" in failed
        assert failed["no_such_handler::j1"]["error"] == "NO_HANDLER"

    def test_wall_dedup_skips_completed(self, tmp_path):
        """Already-completed job UID in wall → removed from queue at run() start."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("dedup_test", lambda j, c: (True, {}))

        # Inject a wall entry into the DB
        pipeline.backend.commit_job_success("dedup_test::j1", {"ok": True}, cursor_updates={})
        # Enqueue the same job
        job = Job("dedup_test", "j1", payload={})
        pipeline.enqueue([job])

        # Verify the job IS in the queue before run
        before = [Job.from_dict(j).uid for j in pipeline.backend.load_queue()]
        assert "dedup_test::j1" in before

        # Now run loads wall from DB, detects dup, and cleans queue
        pipeline.run()
        after = pipeline.backend.load_queue()
        # dedup_test::j1 should have been cleaned
        uids = [Job.from_dict(j).uid for j in after]
        assert "dedup_test::j1" not in uids


class TestResources:
    """Tests for resource scheduling (REQ-6)."""

    def test_unknown_resource_deadlock_detected(self, tmp_path):
        """Job requires unknown resource → deadlock detected, job to DLQ, queue cleared."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))
        job = Job("test", "j1", payload={}, resources={"nonexistent": 1.0})
        pipeline.enqueue([job])

        pipeline.run()

        # After deadlock, the job should be in the DLQ with RESOURCE_DEADLOCK error
        queue_after = pipeline.backend.load_queue()
        assert len(queue_after) == 0  # queue cleared — jobs moved to DLQ

        failed_after = pipeline.backend.load_failed()
        assert "test::j1" in failed_after
        assert failed_after["test::j1"]["error"] == "RESOURCE_DEADLOCK"

    def test_deadlock_only_fails_unknown_resource_job(self, tmp_path, monkeypatch):
        """单个未知资源任务不应连累其他正常任务。
        队列含 1 个 unknown-resource job + 1 个 normal job，
        只 fail 前者，后者正常完成。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))
        pipeline.register_handler("bad", lambda j, c: (True, {}))
        pipeline.enqueue([
            Job("bad", "b1", payload={}, resources={"ghost": 1.0}),
            Job("test", "j1", payload={}),
        ])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "bad::b1" in failed, "unknown-resource job should be in DLQ"
        assert failed["bad::b1"]["error"] == "RESOURCE_DEADLOCK"
        assert "test::j1" not in failed, "normal job should NOT be in DLQ"

        wall = pipeline.backend.load_wall()
        assert "test::j1" in wall, "normal job should complete successfully"

        queue_after = pipeline.backend.load_queue()
        assert len(queue_after) == 0, "queue should be empty after all jobs processed"

    def test_deadlock_missing_dependency_only_fails_root_cause(self, tmp_path):
        """缺失依赖的肇事者被 fail 为 DEPENDENCY_DEADLOCK，
        依赖它的下游任务走 cascade 标记为 JOB_DEPENDENCY。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))
        # j_missing 依赖不存在的 missing::dep
        # j_downstream 依赖 j_missing（j_missing 失败后走 cascade）
        pipeline.enqueue([
            Job("test", "j_missing", payload={}, depends_on=["missing::dep"]),
            Job("test", "j_downstream", payload={}, depends_on=["test::j_missing"]),
        ])

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "test::j_missing" in failed, "root-cause job should be in DLQ"
        assert failed["test::j_missing"]["error"] == "DEPENDENCY_DEADLOCK"
        assert "test::j_downstream" in failed, "downstream job should cascade to DLQ"
        assert failed["test::j_downstream"]["error"] == "JOB_DEPENDENCY"

    def test_resource_acquired_and_released(self, tmp_path, monkeypatch):
        """Resource acquire before job, release after (even on failure)."""
        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("slots", max_capacity=1.0))
        pipeline.register_handler(
            "test",
            lambda j, c: (True, {}),
            default_resources={"slots": 1.0},
        )
        pipeline.enqueue([Job("test", "j1", payload={})])

        FakeP = make_fake_process_class("error")  # Simulates failure
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        # After failure, resource should be released
        res = pipeline.resources["slots"]
        assert res.used == 0.0

    def test_impossible_resource_only_fails_offender(self, tmp_path, monkeypatch):
        """请求超过容量的 job 只失败自身，不连累其他正常 job。

        队列含 1 个 impossible-resource job（请求 200，容量 100）+ 1 个 normal job
        （请求 50，容量 100）。前者进 DLQ，后者正常完成。
        """
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))
        pipeline.add_resource(CapacityResource("mem", max_capacity=100.0))

        # Job A: 不可达（请求 200，容量 100）
        # Job B: 正常（请求 50，容量 100）
        pipeline.enqueue([
            Job("test", "impossible", payload={}, resources={"mem": 200.0}),
            Job("test", "normal", payload={}, resources={"mem": 50.0}),
        ])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        wall = pipeline.backend.load_wall()
        # 不可达 job 应在 failed（细粒度失败，只 fail 肇事者）
        assert "test::impossible" in failed
        assert failed["test::impossible"]["error"] == "RESOURCE_DEADLOCK"
        # 正常 job 应成功（在 wall）
        assert "test::normal" in wall


class TestDependencies:
    """Tests for DAG dependencies and cascading failures (REQ-7)."""

    def test_dependency_waits_for_parent(self, tmp_path, monkeypatch):
        """Job B depends_on=[A.uid] → B waits until A succeeds."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("dep_test", lambda j, c: (True, {}))

        job_a = Job("dep_test", "a", payload={})
        job_b = Job("dep_test", "b", payload={}, depends_on=[job_a.uid])
        pipeline.enqueue([job_a, job_b])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        # Both should be in wall (B waited, A completed, then B ran)
        wall = pipeline.backend.load_wall()
        assert job_a.uid in wall
        assert job_b.uid in wall

    def test_cascading_failure_job_dependency(self, tmp_path, monkeypatch):
        """Parent A fails → child B marked JOB_DEPENDENCY without execution."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("fail_test", lambda j, c: (True, {}))

        job_b = Job("fail_test", "b", payload={}, depends_on=["fail_test::a"])

        # Pre-fail A by putting it in failed DLQ (so run loads it)
        pipeline.backend.commit_job_failure("fail_test::a", {"error": "injected"})

        pipeline.enqueue([job_b])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert job_b.uid in failed
        assert failed[job_b.uid]["error"] == "JOB_DEPENDENCY"

    def test_dependency_deadlock_cycle(self, tmp_path, monkeypatch):
        """Circular dependency → deadlock detected → all to DLQ."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("cycle_test", lambda j, c: (True, {}))

        job_a = Job("cycle_test", "a", payload={}, depends_on=["cycle_test::b"])
        job_b = Job("cycle_test", "b", payload={}, depends_on=["cycle_test::a"])
        pipeline.enqueue([job_a, job_b])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert job_a.uid in failed
        assert job_b.uid in failed
        assert failed[job_a.uid]["error"] == "DEPENDENCY_DEADLOCK"


class TestMiscellaneous:
    """Miscellaneous pipeline behavior tests."""

    def test_enqueue_applies_default_resources(self, tmp_path):
        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("slots", max_capacity=10.0))
        pipeline.register_handler(
            "with_defaults", lambda j, c: (True, {}),
            default_resources={"slots": 2.0},
        )
        job = Job("with_defaults", "j1", payload={})
        pipeline.enqueue([job])
        # Fix 3: enqueue 不再修改传入的 Job 对象
        assert job.resources == {}  # Job 对象保持不变
        # enqueue 不再把 handler 默认资源烤进持久化 job_dict——
        # 磁盘只存 job 自身资源（+ 自动注入的 __workers__），运行时由
        # scheduler._effective_resources / _dispatch_job 合并兜底（语义等价，
        # 且 handler 默认资源变更后磁盘不留旧值）。
        q_data = pipeline.backend.load_queue()
        assert len(q_data) == 1
        assert q_data[0]["resources"] == {"__workers__": 1.0}

    def test_default_resources_applied_atruntime(self, tmp_path, monkeypatch):
        """enqueue 不烤默认值后，运行时仍正确合并 handler
        默认资源——跑一轮确认 job 用上默认资源并成功完成。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("slots", max_capacity=10.0))
        pipeline.register_handler(
            "with_defaults", lambda j, c: (True, {}),
            default_resources={"slots": 2.0},
        )
        pipeline.enqueue([Job("with_defaults", "j1", payload={})])
        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        pipeline.run()
        wall = pipeline.backend.load_wall()
        assert "with_defaults::j1" in wall

    def test_enqueue_does_not_override_explicit_resources(self, tmp_path):
        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("slots", max_capacity=10.0))
        pipeline.register_handler(
            "with_defaults", lambda j, c: (True, {}),
            default_resources={"slots": 2.0},
        )
        job = Job("with_defaults", "j1", payload={}, resources={"slots": 5.0})
        pipeline.enqueue([job])
        assert job.resources == {"slots": 5.0}

    def test_enqueue_no_handler_skips_default_resources(self, tmp_path):
        pipeline = make_pipeline(tmp_path)
        job = Job("unregistered", "j1", payload={})
        pipeline.enqueue([job])
        assert job.resources == {}

    def test_retry_dict_does_not_bake_handler_defaults(self, tmp_path, monkeypatch):
        """retry requeue 不把 handler 默认资源烤进持久化。

        恢复 pop 时的原始 resources，避免 handler 默认资源变更后被旧值覆盖。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("mem", max_capacity=10.0))
        pipeline.register_handler("flaky", lambda j, c: True, default_resources={"mem": 1.0})
        pipeline.enqueue([Job("flaky", "j1", payload={}, max_retries=1)])

        captured = {}
        real_commit_retry = pipeline.backend.commit_retry

        def fake_commit_retry(uid, retry_dict, front=False):
            captured["retry_dict"] = retry_dict
            return real_commit_retry(uid, retry_dict, front=front)
        monkeypatch.setattr(pipeline.backend, "commit_retry", fake_commit_retry)

        RetryProcess = make_ipc_process_class(results=[{"status": "retry", "error": "transient"}])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=RetryProcess)
        monkeypatch.setattr("time.sleep", lambda s: None)  # 跳过退避 sleep

        pipeline.run()

        assert "retry_dict" in captured, "commit_retry 必须被调用（重试 requeue）"
        # 修复前：烤入合并后的 {"mem": 1.0, "__workers__": 1.0}；修复后：只含 worker
        assert captured["retry_dict"]["resources"] == {"__workers__": 1.0}, \
            f"retry_dict 不应烤入 handler 默认资源，实际: {captured['retry_dict']['resources']}"

    def test_lock_conflict_retry_does_not_consume_budget(self, tmp_path, monkeypatch):
        """LOCK_CONFLICT（框架锁冲突，handler 未执行）不
        消耗 max_retries 预算——与 _dispatch_job 的 probe defer 语义对齐。
        修复前：锁冲突与业务瞬态失败混计数 → 孤儿持锁期间烧光预算 → 虚假 DLQ。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: (True, {}))
        pipeline.enqueue([Job("t", "j1", payload={}, max_retries=1)])

        captured = {}
        real_commit_retry = pipeline.backend.commit_retry

        def fake_commit_retry(uid, retry_dict, front=False):
            captured["retry_dict"] = retry_dict
            return real_commit_retry(uid, retry_dict, front=front)
        monkeypatch.setattr(pipeline.backend, "commit_retry", fake_commit_retry)

        # 第一次：LOCK_CONFLICT retry（零计数）；第二次：成功（孤儿死后自愈）
        LockRetryProcess = make_ipc_process_class(results=[
            {"status": "retry",
             "lock_conflict": True,  # 结构化字段（判定端不再 startswith 前缀）
             "error": "LOCK_CONFLICT: another execution body holds t::j1 lock"},
            {"status": "success", "raw_result": True,
             "new_jobs": [], "resource_suspensions": [], "cursor_updates": {}},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=LockRetryProcess)
        monkeypatch.setattr("time.sleep", lambda s: None)  # 跳过退避

        pipeline.run()

        assert "retry_dict" in captured, "LOCK_CONFLICT 应触发 requeue（重试）"
        # 零计数：retries 保持 0（业务重试此时已 +1）
        assert captured["retry_dict"]["retries"] == 0, \
            f"LOCK_CONFLICT 不得消耗 retry 预算，实际 retries: {captured['retry_dict']['retries']}"
        wall = pipeline.backend.load_wall()
        assert "t::j1" in wall, "孤儿死后 LOCK_CONFLICT 自愈重跑应成功"

    def test_business_retry_error_with_lock_conflict_prefix_counts_budget(
            self, tmp_path, monkeypatch):
        """业务 RetryError 消息恰好以 "LOCK_CONFLICT" 开头
        （如 "LOCK_CONFLICT: my business quota exceeded"）必须是**正常计数重试**
        ——retries 消耗 max_retries 预算、退避正常、_last_retry_error 被覆盖。

        修复前：判定端 `retry_error.startswith("LOCK_CONFLICT")` 把业务消息
        误判为框架锁冲突 → 零计数重试 + 不覆盖 _last_retry_error（与
        test_lock_conflict_retry_does_not_consume_budget 为零计数的对偶路径）。
        修复后：判定端只读结构化 lock_conflict 字段（本测试的结果 dict 故意
        **不带**该字段）→ 业务撞前缀不再误判。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: (True, {}))
        pipeline.enqueue([Job("t", "j1", payload={}, max_retries=1)])

        captured = {}
        real_commit_retry = pipeline.backend.commit_retry

        def fake_commit_retry(uid, retry_dict, front=False):
            captured["retry_dict"] = retry_dict
            return real_commit_retry(uid, retry_dict, front=front)
        monkeypatch.setattr(pipeline.backend, "commit_retry", fake_commit_retry)

        # 业务 RetryError 消息撞 "LOCK_CONFLICT" 前缀，但**不带**结构化字段
        # lock_conflict（区别于框架锁冲突）→ 必须走正常计数重试。
        backoff_calls = []

        def fake_backoff(*args, **kwargs):
            backoff_calls.append(args)
            return 0.0
        monkeypatch.setattr("tasklite.engine.policy.PreflightPolicy.compute_backoff", fake_backoff)

        BusinessPrefixProcess = make_ipc_process_class(results=[
            {"status": "retry", "error": "LOCK_CONFLICT: my business quota exceeded"},
            {"status": "success", "raw_result": True,
             "new_jobs": [], "resource_suspensions": [], "cursor_updates": {}},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=BusinessPrefixProcess)
        monkeypatch.setattr("time.sleep", lambda s: None)  # 跳过退避 sleep

        pipeline.run()

        assert "retry_dict" in captured, "业务撞前缀 RetryError 必须触发 requeue"
        # 计数重试：retries 消耗预算（0 → 1）；修复前被误判为 0
        assert captured["retry_dict"]["retries"] == 1, \
            f"业务撞 LOCK_CONFLICT 前缀必须走计数重试，实际 retries: {captured['retry_dict']['retries']}"
        # _last_retry_error 被覆盖为业务消息（修复前误判 lock_conflict 不覆盖）
        assert captured["retry_dict"]["runtime"].get("_last_retry_error") == \
            "LOCK_CONFLICT: my business quota exceeded", \
            f"_last_retry_error 应被业务消息覆盖（runtime），实际: {captured['retry_dict']['runtime'].get('_last_retry_error')!r}"
        # 退避正常：走 compute_backoff（修复前误判 lock_conflict 走固定 1s）
        assert backoff_calls, "业务撞前缀重试必须走 compute_backoff 正常退避"
        wall = pipeline.backend.load_wall()
        assert "t::j1" in wall, "计数重试后成功应进 wall"

    def test_cursor_update_on_success(self, tmp_path, monkeypatch):
        """Worker sends cursor_updates via IPC → persisted in backend."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("cursor_test", lambda j, c: (True, {}))

        CursorUpdateProcess = make_ipc_process_class(results=[{
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [],
            "cursor_updates": {"key1": "val1", "key2": "42"},
        }])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=CursorUpdateProcess)

        pipeline.enqueue([Job("cursor_test", "j1", payload={})])
        pipeline.run()

        cursors = pipeline.backend.load_cursors()
        assert cursors == {"key1": "val1", "key2": "42"}

        wall = pipeline.backend.load_wall()
        assert "cursor_test::j1" in wall

    def test_enqueue_front(self, tmp_path):
        """enqueue with front=True → job at front of queue."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))

        pipeline.enqueue([Job("test", "j1", payload={}), Job("test", "j2", payload={})])
        pipeline.enqueue([Job("test", "j0", payload={})], front=True)

        queue_data = pipeline.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in queue_data]
        assert uids[0] == "test::j0"

    def test_enqueue_front_deduplicates_existing_jobs(self, tmp_path):
        """enqueue with front=True 时也应跳过已存在的重复作业。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("test", lambda j, c: (True, {}))

        pipeline.enqueue([Job("test", "j1", payload={})])
        pipeline.enqueue([Job("test", "j1", payload={})], front=True)

        queue_data = pipeline.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in queue_data]
        assert uids == ["test::j1"]

    def test_multiple_jobs_process(self, tmp_path, monkeypatch):
        """Multiple enqueued jobs all get processed."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("multi", lambda j, c: (True, {"id": j.payload["id"]}))
        jobs = [Job("multi", f"j{i}", payload={"id": i}) for i in range(5)]
        pipeline.enqueue(jobs)

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        wall = pipeline.backend.load_wall()
        for i in range(5):
            assert f"multi::j{i}" in wall


class TestNormalizeHandlerResult:
    """_normalize_handler_result 归一化契约（参数化，覆盖全部返回类型）。

    直接测试 executor 的模块级纯函数，无需构造 pipeline。
    """

    @pytest.mark.parametrize(
        "raw, expected",
        [
            # ── 成功/失败路径（合法返回类型）──
            (None, (True, {})),
            (True, (True, {})),
            (False, (False, {})),
            ({"k": "v"}, (True, {"k": "v"})),
            ({"outer": {"inner": [1, 2, 3]}}, (True, {"outer": {"inner": [1, 2, 3]}})),
            ((True, {"a": 1}), (True, {"a": 1})),
            ((False, {"b": 2}), (False, {"b": 2})),
            # ── 非法 tuple（(bool, dict) 契约违反）──
            ((True,), (False, "error")),
            (("not_bool", "not_dict"), (False, "error")),
            ((0, {}), (False, "error")),
            (("ok", 42), (False, "error")),
            ((None, {"k": "v"}), (False, "error")),
            # ── 未识别返回类型（handler bug）──
            ([1, 2, 3], (False, "error")),
            (42, (False, "error")),
            ("hello", (False, "error")),
            ((1, 2, 3), (False, "error")),
            ((), (False, "error")),
        ],
    )
    def test_normalize_contract(self, raw, expected):
        from tasklite.engine.channel import _normalize_handler_result

        success, meta = _normalize_handler_result(raw)
        exp_success, exp_meta = expected
        assert success is exp_success
        if exp_meta == "error":
            assert "error" in meta
        else:
            assert meta == exp_meta

    def test_ordered_dict_and_job_instance(self):
        """OrderedDict 视作 dict；Job 实例视作未识别类型。"""
        from collections import OrderedDict
        from tasklite.engine.channel import _normalize_handler_result
        from tasklite.models.job import Job as _J

        success, meta = _normalize_handler_result(OrderedDict([("a", 1), ("b", 2)]))
        assert success is True
        assert meta == {"a": 1, "b": 2}

        success, meta = _normalize_handler_result(_J("x", "y"))
        assert success is False
        assert "error" in meta


# ─── Core pipeline edge case / adversarial cases ───────────────────────


class TestPipelineWeirdCases:
    """Edge-case pipeline behaviors that complement the happy-path tests."""

    def test_enqueue_empty_list_is_noop(self, tmp_path):
        """enqueue([]) returns immediately without touching the queue."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: (True, {}))
        pipeline.enqueue([Job("t", "j1", payload={})])
        before = len(pipeline.backend.load_queue())

        pipeline.enqueue([])  # noop

        after = len(pipeline.backend.load_queue())
        assert before == after == 1

    def test_enqueue_duplicate_uids_same_call(self, tmp_path):
        """Two jobs with same uid enqueued in one call — first stored, second skipped (dedup at enqueue)."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: (True, {}))
        pipeline.enqueue([Job("t", "dup", payload={}), Job("t", "dup", payload={})])

        queue = pipeline.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in queue]
        assert uids.count("t::dup") == 1  # dedup at enqueue: second skipped


    def test_handler_returns_none_succeeds_integration(self, tmp_path, monkeypatch):
        """Integration: handler returns None → success in wall (not just normalize unit)."""
        pipeline = make_pipeline(tmp_path)

        NoneResultProcess = make_ipc_process_class(results=[{
            "status": "success", "raw_result": None,
            "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
        }])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=NoneResultProcess)

        pipeline.register_handler("none_handler", lambda j, c: None)
        pipeline.enqueue([Job("none_handler", "j1", payload={})])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        assert "none_handler::j1" in wall

    def test_multiple_resources_single_job(self, tmp_path, monkeypatch):
        """Job with 2 resources → both acquired before run, both released after."""
        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("gpu", max_capacity=4.0))
        pipeline.add_resource(RateLimitResource("api", interval_seconds=1.0))
        pipeline.register_handler(
            "multi_res", lambda j, c: (True, {}),
            default_resources={"gpu": 2.0, "api": 1.0},
        )
        pipeline.enqueue([Job("multi_res", "j1", payload={})])

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        pipeline.run()

        # Both resources released after success
        assert pipeline.resources["gpu"].used == 0.0
        wall = pipeline.backend.load_wall()
        assert "multi_res::j1" in wall

    def test_spawned_child_jobs_prepend_to_queue(self, tmp_path, monkeypatch):
        """ctx.spawn() children are prepended → run before pre-existing queue items."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("parent", lambda j, c: (True, {}))
        pipeline.register_handler("child", lambda j, c: (True, {}))

        # Pre-load the queue with a "later" job
        pipeline.enqueue([Job("child", "later", payload={})])

        # First Process call (parent) spawns "earlier"; subsequent calls (children) succeed plainly.
        spawn_result = {
            "status": "success", "raw_result": True,
            "new_jobs": [Job("child", "earlier", payload={}).to_dict()],
            "resource_suspensions": [], "cursor_updates": {},
        }
        plain_success = {
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
        }
        HybridProcess = make_ipc_process_class(results=[spawn_result, plain_success])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=HybridProcess)

        pipeline.enqueue([Job("parent", "p1", payload={})])
        pipeline.run()

        wall = pipeline.backend.load_wall()
        # All three should succeed
        assert "parent::p1" in wall
        assert "child::earlier" in wall
        assert "child::later" in wall

    def test_enqueue_front_multiple_calls(self, tmp_path):
        """Two front-enqueues: second call's job ends up at index 0."""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: (True, {}))

        pipeline.enqueue([Job("t", "base", payload={})])
        pipeline.enqueue([Job("t", "first_front", payload={})], front=True)
        pipeline.enqueue([Job("t", "second_front", payload={})], front=True)

        queue = pipeline.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in queue]
        assert uids[0] == "t::second_front"
        assert uids[1] == "t::first_front"
        assert uids[2] == "t::base"

    def test_cursor_updates_visible_to_subsequent_jobs(self, tmp_path, monkeypatch):
        """Job A sets cursor → Job B's ctx.get_cursor() sees it within same run."""
        pipeline = make_pipeline(tmp_path)
        observed_cursor = [None]

        class CursorSetterProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                spec = self.args[0]
                _write_fake_result(spec.ipc_dir, spec.job.uid, {
                    "status": "success", "raw_result": True,
                    "new_jobs": [], "resource_suspensions": [],
                    "cursor_updates": {"shared_key": "shared_val"},
                }, incarnation=spec.incarnation,
                   auth_token=getattr(spec, "result_token", None))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        class CursorReaderProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                spec = self.args[0]
                observed_cursor[0] = spec.task_ctx.get_cursor("shared_key")
                _write_fake_result(spec.ipc_dir, spec.job.uid, {
                    "status": "success", "raw_result": True,
                    "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
                }, incarnation=spec.incarnation,
                   auth_token=getattr(spec, "result_token", None))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        call_idx = [0]
        class HybridProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0
            def start(self):
                self._alive = True
                call_idx[0] += 1
                if call_idx[0] == 1:
                    CursorSetterProcess.start(self)
                else:
                    CursorReaderProcess.start(self)
            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=HybridProcess)

        pipeline.register_handler("setter", lambda j, c: (True, {}))
        pipeline.register_handler("reader", lambda j, c: (True, {}))
        # reader 显式依赖 setter：并发模型下无依赖的 job 不保证执行顺序，
        # 加 depends_on 确保 B 在 A commit cursor 后才 dispatch。
        pipeline.enqueue([
            Job("setter", "a", payload={}),
            Job("reader", "b", payload={}, depends_on=["setter::a"]),
        ])
        pipeline.run()

        assert observed_cursor[0] == "shared_val"


class TestRegisterHandlerResourceGuard:
    """register_handler 的 default_resources 拒绝矩阵——
    pipeline 入口此前零测试（Rule 4 对称路径缺口，仅 Job 入口有覆盖）。"""

    def test_default_resources_rejection_matrix(self, tmp_path):
        """bool/NaN/Inf/负值/超大 int 全覆盖拒绝（与 Job.__init__ 对称）。"""
        import math
        from tasklite import TaskLite
        from tasklite.models.job import Job

        p = TaskLite(name="g6_guard", state_dir=tmp_path / "state")
        ok_handler = lambda j, c: True

        with pytest.raises(TypeError, match="must be a number"):
            p.register_handler("t1", ok_handler, default_resources={"api": True})
        with pytest.raises(ValueError, match="must be finite"):
            p.register_handler("t2", ok_handler, default_resources={"api": float("nan")})
        with pytest.raises(ValueError, match="must be finite"):
            p.register_handler("t3", ok_handler, default_resources={"api": float("inf")})
        with pytest.raises(ValueError, match="must be non-negative"):
            p.register_handler("t4", ok_handler, default_resources={"api": -1.0})
        with pytest.raises(ValueError, match="too large"):
            p.register_handler("t5", ok_handler, default_resources={"api": 10 ** 400})
        # dict 类型守卫（truthy 非 dict 此前抛原始 AttributeError）
        with pytest.raises(TypeError, match="default_resources must be a dict"):
            p.register_handler("t6", ok_handler, default_resources="abc")
        with pytest.raises(TypeError, match="default_resources must be a dict"):
            p.register_handler("t7", ok_handler, default_resources=["api", 1.0])

    def test_default_resources_none_and_empty_ok(self, tmp_path):
        """None 与空 dict 合法（不校验、不报错）。"""
        from tasklite import TaskLite

        p = TaskLite(name="g6_none", state_dir=tmp_path / "state")
        p.register_handler("t1", lambda j, c: True, default_resources=None)
        p.register_handler("t2", lambda j, c: True, default_resources={})
        assert "t1" in p.handlers and "t2" in p.handlers
