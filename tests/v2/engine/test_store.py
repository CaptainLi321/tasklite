"""v2 StateStore 状态仓库测试：六出口事务、失败档案、3-strike 与摄入管道。

覆盖：apply_* 原子终态转移（内存/后端镜像一致）、wall/failed 互斥性质
（hypothesis 任意操作序列）、mark_failed_memory 联动契约、commit 失败
3-strike 收敛、enqueue 摄入三步（策略注入 / 工人资源注入 / 首入队时间
填充）、瞬态降级回队与 attempts 旁路轨迹收尾。
"""

from __future__ import annotations

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from tasklite.v2.backend.memory import InMemoryStateBackend
from tasklite.v2.engine.admission import RerunPolicy
from tasklite.v2.engine.errorclass import (
    ERR_COMMIT_FAILURE,
    ERR_JOB_DEPENDENCY,
)
from tasklite.v2.engine.store import (
    COMMIT_FAILURE_THRESHOLD,
    BulkFailureOutcome,
    FailureEntry,
    FailureOutcome,
    RetryOutcome,
    SkipOutcome,
    StateStore,
    SuccessOutcome,
)
from tasklite.v2.engine.types import AttemptFinish, TaskStats
from tasklite.v2.exceptions import _CommitCrashSignal, _JobTerminated
from tasklite.v2.models.job import Job, WORKER_RESOURCE
from tasklite.v2.models.state import PipelineState


class CommitFailureBackend:
    """commit 故障注入 backend：指定方法恒返 False，其余委托真实后端。

    ``archive_pass_through`` 时 commit_job_failure 对 3-strike 熔断 meta
    放行 True（收敛路径需要失败档案写入成功才能走到终态分支）。
    """

    def __init__(self, real_backend, *, failing=(), archive_pass_through=False):
        self._real = real_backend
        self._failing = frozenset(failing)
        self._archive_pass_through = archive_pass_through

    def commit_job_success(self, *a, **k):
        if "commit_job_success" in self._failing:
            return False
        return self._real.commit_job_success(*a, **k)

    def commit_job_failure(self, uid, meta, *a, **k):
        if "commit_job_failure" in self._failing:
            if self._archive_pass_through and meta.get("error") == ERR_COMMIT_FAILURE:
                return self._real.commit_job_failure(uid, meta, *a, **k)
            return False
        return self._real.commit_job_failure(uid, meta, *a, **k)

    def commit_retry(self, *a, **k):
        if "commit_retry" in self._failing:
            return False
        return self._real.commit_retry(*a, **k)

    def commit_bulk_failure(self, *a, **k):
        if "commit_bulk_failure" in self._failing:
            return False
        return self._real.commit_bulk_failure(*a, **k)

    def commit_skip(self, *a, **k):
        if "commit_skip" in self._failing:
            return False
        return self._real.commit_skip(*a, **k)

    def __getattr__(self, name):
        return getattr(self._real, name)


def _make_store(**kwargs) -> tuple[StateStore, PipelineState, InMemoryStateBackend]:
    backend = InMemoryStateBackend()
    state = PipelineState({}, {}, {}, [])
    store = StateStore(backend, state, **kwargs)
    return store, state, backend


class TestApplySuccess:
    """apply_success 原子终态转移。"""

    def test_basic_wall_meta(self):
        store, state, _ = _make_store()
        state.register_in_flight("process::1")

        outcome = store.apply_success(
            "process::1",
            {"score": 100},
            run_id="run_abc",
            declared_inputs=[{"path": "/tmp/a.txt", "kind": "file"}],
        )

        assert isinstance(outcome, SuccessOutcome)
        assert outcome.uid == "process::1"
        assert store.wall["process::1"]["score"] == 100
        assert store.wall["process::1"]["last_run_id"] == "run_abc"
        assert store.wall["process::1"]["run_count"] == 1
        assert "last_run_at" in store.wall["process::1"]
        assert store.wall["process::1"]["inputs"][0]["path"] == "/tmp/a.txt"
        assert "process::1" not in store.in_flight_uids

    def test_run_count_continues_from_opposite_terminal(self):
        """重跑成功从失败档案对侧终态续数（run_count 跨终态递增）。"""
        store, state, _ = _make_store()
        state.register_in_flight("t::1")
        store.apply_failure("t::1", {"error": "boom", "run_count": 4})
        # 终态 uid 重入 in-flight 须经重跑豁免登记（六集合互斥协议）
        store.mark_rerun_active("t::1")
        state.register_in_flight("t::1")
        store.apply_success("t::1", {})
        assert store.wall["t::1"]["run_count"] == 5
        assert "t::1" not in store.failed

    def test_spawn_and_cursor_updates(self):
        store, _, _ = _make_store()
        store.register_in_flight("root::1")
        spawned = [
            {"task_type": "child", "job_id": "c1"},
            {"task_type": "child", "job_id": "c2"},
        ]

        outcome = store.apply_success(
            "root::1",
            {},
            spawned_jobs=spawned,
            cursor_updates={"batch_pos": "100", "old_cursor": None},
        )

        assert outcome.spawned_uids == ["child::c1", "child::c2"]
        assert store.cursors["batch_pos"] == "100"
        assert "old_cursor" not in store.cursors
        assert [jd["job_id"] for jd in store.queue] == ["c1", "c2"]
        assert store.stats is None  # 未注入统计不炸

    def test_declared_inputs_deduped_by_path(self):
        store, _, _ = _make_store()
        store.apply_success(
            "t::1",
            {},
            declared_inputs=[
                {"path": "/a", "size": 1},
                {"path": "/a", "size": 2},
                {"not_path": True},
                "junk",
            ],
        )
        inputs = store.wall["t::1"]["inputs"]
        assert len(inputs) == 1
        assert inputs[0]["size"] == 2


class TestApplyFailureAndCascade:
    """apply_failure 与依赖级联。"""

    def test_failure_with_transitive_cascade(self):
        store, state, _ = _make_store()
        child = Job("child", "1", depends_on=["parent::1"]).to_dict()
        grandchild = Job("grandchild", "1", depends_on=["child::1"]).to_dict()
        state.replace_queue(
            [{"task_type": "parent", "job_id": "1"}, child, grandchild]
        )
        state.pop_job(0)
        state.register_in_flight("parent::1")

        outcome = store.apply_failure(
            "parent::1",
            {"error": "crash"},
            job_dict={"task_type": "parent", "job_id": "1"},
        )

        assert isinstance(outcome, FailureOutcome)
        assert set(outcome.cascaded_uids) == {"child::1", "grandchild::1"}
        for uid in ("parent::1", "child::1", "grandchild::1"):
            assert uid in store.failed
            assert uid in store.backend.load_failed()
        assert store.failed["child::1"]["error"] == ERR_JOB_DEPENDENCY
        assert store.failed["child::1"]["failed_dependency"] == "parent::1"
        assert "parent::1" not in store.in_flight_uids

    def test_failure_payload_snapshot_skipped_when_unserializable(self, caplog):
        """payload 快照不可序列化时降级 None——快照失败不得误触 3-strike。"""
        store, _, backend = _make_store()
        job_dict = {
            "task_type": "t",
            "job_id": "1",
            "payload": {"bad": object()},
        }
        store.apply_failure("t::1", {"error": "x"}, job_dict=job_dict)
        assert "t::1" in store.failed
        assert "t::1" not in backend.load_failed_payloads()

    def test_failure_clears_wall_on_both_sides(self):
        """失败终态与成功终态互删对侧（内存与后端镜像同步）。"""
        store, _, backend = _make_store()
        store.register_in_flight("t::1")
        store.apply_success("t::1", {})
        store.mark_rerun_active("t::1")
        store.register_in_flight("t::1")
        store.apply_failure("t::1", {"error": "again"})

        assert "t::1" not in store.wall
        assert "t::1" in store.failed
        assert "t::1" not in backend.load_wall()
        assert "t::1" in backend.load_failed()


class TestApplyRetry:
    """apply_retry 重试重入队。"""

    def _retry_dict(self) -> dict:
        return Job("t", "1", attempt_no=2).to_dict()

    def test_commit_path_requeues_front_and_records_stats(self):
        stats = TaskStats()
        store, state, _ = _make_store(stats=stats)
        state.replace_queue([])
        store.register_in_flight("t::1")

        outcome = store.apply_retry(
            "t::1",
            {"task_type": "t", "job_id": "1"},
            self._retry_dict(),
            transient_kind="interrupted",
            error="transient blip",
        )

        assert isinstance(outcome, RetryOutcome)
        assert outcome.transient_kind == "interrupted"
        assert store.queue[0]["job_id"] == "1"
        assert "t::1" not in store.in_flight_uids
        assert stats["retried"] == 1
        assert stats["interrupted_reruns"] == 1

    def test_commit_failure_raises_crash_signal(self):
        backend = InMemoryStateBackend()
        failing = CommitFailureBackend(backend, failing=("commit_retry",))
        state = PipelineState({}, {}, {}, [])
        store = StateStore(failing, state, stats=TaskStats())

        with pytest.raises(_CommitCrashSignal, match="commit_retry"):
            store.apply_retry("t::1", {"task_type": "t", "job_id": "1"}, self._retry_dict())
        assert "t::1" in store.queue_uids


class TestApplySkip:
    """apply_skip 去重跳过。"""

    def test_skip_records_stat_and_clears_queue(self):
        stats = TaskStats()
        backend = InMemoryStateBackend()
        backend.save_queue([{"task_type": "t", "job_id": "1"}])
        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "1"}])
        store = StateStore(backend, state, stats=stats)
        state.pop_job(0)

        outcome = store.apply_skip("t::1", job_dict={"task_type": "t", "job_id": "1"})

        assert isinstance(outcome, SkipOutcome)
        assert outcome.was_known is True
        assert stats["skipped"] == 1
        assert store.is_empty
        assert backend.load_queue() == []

    def test_skip_commit_failure_crashes_without_archive(self):
        """commit_skip 失败：只 requeue + 崩溃，绝不写失败档案。"""
        backend = InMemoryStateBackend()
        failing = CommitFailureBackend(backend, failing=("commit_skip",))
        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "1"}])
        store = StateStore(failing, state, stats=TaskStats())
        state.pop_job(0)

        with pytest.raises(_CommitCrashSignal, match="commit_skip"):
            store.apply_skip("t::1", job_dict={"task_type": "t", "job_id": "1"})
        assert "t::1" in store.queue_uids
        assert "t::1" not in store.failed


class TestMarkFailedMemoryContract:
    """mark_failed_memory 失败终态内存尾段契约。"""

    def test_default_registers_failed_and_unregisters_in_flight(self):
        store, state, _ = _make_store()
        state.register_in_flight("encode::a")
        store.mark_failed_memory("encode::a", {"error": "boom"})
        assert "encode::a" in state.failed_uids
        assert "encode::a" not in state.in_flight_uids
        assert state.failed["encode::a"]["error"] == "boom"

    def test_explicit_unregister_false_keeps_in_flight(self):
        store, state, _ = _make_store()
        state.register_in_flight("encode::a")
        store.mark_failed_memory("encode::a", {"error": "boom"}, unregister=False)
        assert "encode::a" in state.failed_uids
        assert "encode::a" in state.in_flight_uids

    def test_failed_takes_over_from_wall(self):
        store, state, _ = _make_store()
        state.add_wall("encode::a", {"prev": "ok"})
        store.mark_failed_memory("encode::a", {"error": "boom"})
        assert "encode::a" in state.failed_uids
        assert "encode::a" not in state.wall_uids


class TestCommitFailureStrike:
    """commit 失败 3-strike 收敛契约。"""

    def _strike_store(self, hooks: list[tuple[str, AttemptFinish]]) -> StateStore:
        backend = CommitFailureBackend(
            InMemoryStateBackend(),
            failing=("commit_job_success", "commit_job_failure"),
            archive_pass_through=True,
        )
        return StateStore(
            backend,
            PipelineState({}, {}, {}, []),
            commit_failure_threshold=3,
            stats=TaskStats(),
            on_attempt_finished=lambda uid, *, outcome: hooks.append((uid, outcome)),
        )

    def test_threshold_retry_then_archive(self):
        store = self._strike_store([])
        job_dict = {"task_type": "task", "job_id": "1"}

        store.register_in_flight("task::1")
        with pytest.raises(_CommitCrashSignal):
            store.apply_success("task::1", {}, job_dict=job_dict)
        assert job_dict["runtime"]["_commit_failures"] == 1
        assert "task::1" in store.queue_uids

        store.pop_job(0)
        store.register_in_flight("task::1")
        with pytest.raises(_CommitCrashSignal):
            store.apply_success("task::1", {}, job_dict=job_dict)
        assert job_dict["runtime"]["_commit_failures"] == 2

        store.pop_job(0)
        store.register_in_flight("task::1")
        with pytest.raises(_JobTerminated, match="permanently failed"):
            store.apply_success("task::1", {}, job_dict=job_dict)
        assert "task::1" in store.failed
        assert store.failed["task::1"]["fatal"] is True
        assert store.failed["task::1"]["error"] == ERR_COMMIT_FAILURE

    def test_strike_archive_fires_hook_exactly_once(self):
        hooks: list[tuple[str, AttemptFinish]] = []
        store = self._strike_store(hooks)
        job_dict = {"task_type": "task", "job_id": "1"}
        for _ in range(2):
            store.register_in_flight("task::1")
            with pytest.raises(_CommitCrashSignal):
                store.apply_success("task::1", {}, job_dict=job_dict)
            store.pop_job(0)
        store.register_in_flight("task::1")
        with pytest.raises(_JobTerminated):
            store.apply_success("task::1", {}, job_dict=job_dict)

        assert len(hooks) == 1
        uid, fin = hooks[0]
        assert uid == "task::1"
        assert fin.success is False
        assert fin.going_to_retry is False
        assert fin.meta["error"] == ERR_COMMIT_FAILURE
        assert store.stats["failed"] == 1

    def test_default_threshold_constant(self):
        assert COMMIT_FAILURE_THRESHOLD == 3


class TestHookSingleExit:
    """fire_attempt_finished 单一出口与异常隔离。"""

    def test_hook_exception_isolated_and_counted(self):
        stats = TaskStats()
        store, _, _ = _make_store(stats=stats)

        def boom(uid, *, outcome):
            raise RuntimeError("hook failure")

        store.set_on_attempt_finished(boom)
        store.fire_attempt_finished(
            "t::1", outcome=AttemptFinish(success=True, going_to_retry=False, meta={})
        )
        assert stats["hook_errors"] == 1

    def test_no_callback_is_noop(self):
        store, _, _ = _make_store()
        store.fire_attempt_finished(
            "t::1", outcome=AttemptFinish(success=True, going_to_retry=False, meta={})
        )

    def test_set_on_attempt_finished_rebinds(self):
        captured: list[str] = []
        store, _, _ = _make_store()
        store.set_on_attempt_finished(lambda uid, *, outcome: captured.append(uid))
        store.fire_attempt_finished(
            "t::1", outcome=AttemptFinish(success=False, going_to_retry=True, meta={})
        )
        assert captured == ["t::1"]

    def test_bulk_failure_fires_hook_per_uid(self):
        stats = TaskStats()
        hooks: list[tuple[str, AttemptFinish]] = []
        backend = InMemoryStateBackend()
        store = StateStore(
            backend,
            PipelineState({}, {}, {}, []),
            stats=stats,
            on_attempt_finished=lambda uid, *, outcome: hooks.append((uid, outcome)),
        )

        store.apply_bulk_failure(
            [("cycle::1", {"error": "DEPENDENCY_DEADLOCK"}),
             ("cycle::2", {"error": "DEPENDENCY_DEADLOCK"})]
        )
        assert {uid for uid, _ in hooks} == {"cycle::1", "cycle::2"}
        assert all(fin.success is False and fin.going_to_retry is False for _, fin in hooks)
        assert stats["failed"] == 2

    def test_cascade_fires_hook_per_downstream(self):
        hooks: list[tuple[str, AttemptFinish]] = []
        backend = InMemoryStateBackend()
        state = PipelineState(
            {}, {}, {},
            [
                {"task_type": "parent", "job_id": "1"},
                Job("child", "1", depends_on=["parent::1"]).to_dict(),
            ],
        )
        store = StateStore(
            backend, state,
            on_attempt_finished=lambda uid, *, outcome: hooks.append((uid, outcome)),
        )
        state.pop_job(0)

        outcome = store.apply_failure("parent::1", {"error": "crash"})
        assert outcome.cascaded_uids == ["child::1"]
        assert [uid for uid, _ in hooks] == ["child::1"]


class TestApplyBulkFailure:
    """apply_bulk_failure 批量失败。"""

    def test_bulk_failure_replaces_queue(self):
        store, state, _ = _make_store()
        state.replace_queue([
            {"task_type": "cycle", "job_id": "1"},
            {"task_type": "cycle", "job_id": "2"},
            {"task_type": "safe", "job_id": "1"},
        ])

        outcome = store.apply_bulk_failure(
            [
                ("cycle::1", {"error": "DEPENDENCY_DEADLOCK"}),
                ("cycle::2", {"error": "DEPENDENCY_DEADLOCK"}),
            ],
            remaining_queue=[{"task_type": "safe", "job_id": "1"}],
        )

        assert isinstance(outcome, BulkFailureOutcome)
        assert outcome.failed_uids == ["cycle::1", "cycle::2"]
        assert outcome.remaining_queue_count == 1
        assert "cycle::1" in store.failed
        assert "safe::1" in store.queue_uids

    def test_bulk_failure_keeps_wall_deleted_on_backend(self):
        """批量失败与单条失败对称：wall 同名行随事务删除。"""
        store, _, backend = _make_store()
        backend.seed_wall(["cycle::1"])
        store.apply_bulk_failure([("cycle::1", {"error": "DEPENDENCY_DEADLOCK"})])
        assert "cycle::1" not in backend.load_wall()

    def test_bulk_commit_failure_below_threshold_crashes(self):
        backend = InMemoryStateBackend()
        failing = CommitFailureBackend(
            backend,
            failing=("commit_bulk_failure", "commit_job_failure"),
        )
        state = PipelineState({}, {}, {}, [{"task_type": "cycle", "job_id": "1"}])
        store = StateStore(failing, state, commit_failure_threshold=3)

        with pytest.raises(_CommitCrashSignal):
            store.apply_bulk_failure([("cycle::1", {"error": "DEPENDENCY_DEADLOCK"})])
        assert store.queue[0]["runtime"]["_commit_failures"] == 1
        assert "cycle::1" not in store.failed


class TestIntakePipeline:
    """enqueue 摄入管道三步 + 首入队时间。"""

    def test_enqueue_fills_first_enqueued_at_once(self):
        store, _, backend = _make_store()
        inserted = store.enqueue_jobs(Job("t", "1"))
        assert inserted == ["t::1"]
        row = backend.load_queue()[0]
        assert row["first_enqueued_at"]
        assert row["resources"][WORKER_RESOURCE] == 1.0

    def test_enqueue_does_not_refresh_first_enqueued_at(self):
        store, _, _ = _make_store()
        job = Job("t", "1", first_enqueued_at="2026-01-01T00:00:00+00:00")
        jd = store.normalize_and_validate_job(job)
        assert jd["first_enqueued_at"] == "2026-01-01T00:00:00+00:00"

    def test_enqueue_injects_discovery_rerun_for_unspecified(self):
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            rerun_policy=RerunPolicy(discovery_rerun={"scan": "every_run"}),
        )
        jd = store.normalize_and_validate_job(Job("scan", "1"))
        assert jd["rerun"] == "every_run"
        jd_explicit = store.normalize_and_validate_job(Job("scan", "2", rerun="never"))
        assert jd_explicit["rerun"] == "never"

    def test_enqueue_rejects_unserializable_payload(self):
        store, _, _ = _make_store()
        with pytest.raises(ValueError, match="not JSON-serializable"):
            store.enqueue_jobs(Job("t", "1", payload={"bad": object()}))
        with pytest.raises(ValueError, match="not JSON-serializable"):
            store.enqueue_jobs({"task_type": "t", "job_id": "2", "payload": {"bad": object()}})

    def test_enqueue_rejects_non_job_non_dict(self):
        store, _, _ = _make_store()
        with pytest.raises(TypeError):
            store.enqueue_jobs("not-a-job")
        with pytest.raises(ValueError, match="task_type"):
            store.enqueue_jobs({"job_id": "1"})

    def test_enqueue_dict_deep_copied(self):
        """dict 入参深拷贝——调用方就地变异不得穿透后端镜像。"""
        store, _, backend = _make_store()
        src = {"task_type": "t", "job_id": "1", "payload": {"k": 1}}
        store.enqueue_jobs(src)
        src["payload"]["k"] = 999
        assert backend.load_queue()[0]["payload"]["k"] == 1

    def test_normalize_spawned_job_matches_intake_rules(self):
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            rerun_policy=RerunPolicy(discovery_rerun={"child": "on_failure"}),
        )
        jd = store.normalize_spawned_job(Job("child", "c1"))
        assert jd["rerun"] == "on_failure"
        assert jd["resources"][WORKER_RESOURCE] == 1.0
        assert jd["first_enqueued_at"]

    def test_normalize_spawned_job_rejects_unserializable(self):
        store, _, _ = _make_store()
        with pytest.raises(ValueError, match="Spawned job"):
            store.normalize_spawned_job(Job("t", "1", payload={"bad": object()}))


class TestRequeueTransient:
    """瞬态信号降级回队单一出口（军规）。"""

    @pytest.mark.parametrize(
        ("kind", "stat_key"),
        [
            ("interrupted", "interrupted_reruns"),
            ("lock_conflict", "deferred_orphan"),
            ("rate_limited", "rate_limited_reruns"),
        ],
    )
    def test_requeue_transient_records_stat_and_fronts(self, kind, stat_key):
        stats = TaskStats()
        store, state, _ = _make_store(stats=stats)
        jd = Job("t", "1").to_dict()

        store.requeue_transient(jd, transient_kind=kind)

        assert state.queue[0]["job_id"] == "1"
        assert stats[stat_key] == 1

    def test_requeue_transient_leaves_runtime_untouched(self):
        """零污染：不烧 commit/dispatch 失败预算、不写 last_retry_error。"""
        stats = TaskStats()
        store, _, _ = _make_store(stats=stats)
        jd = Job("t", "1").to_dict()

        store.requeue_transient(jd, transient_kind="rate_limited")

        assert jd["runtime"] == {}
        assert stats["retried"] == 0


class TestAttemptTraceFinalization:
    """apply_* 终态的 attempts 旁路轨迹收尾。"""

    def _open_running(self, backend, uid="t::1"):
        from tasklite.v2.models.attempt import AttemptRecord

        record = AttemptRecord(
            job_uid=uid,
            activation_no=1,
            attempt_no=1,
            incarnation="r" * 32 + ".1",
            run_id="r" * 32,
            started_at="2026-01-01T00:00:00+00:00",
            outcome="running",
        )
        return backend.append_attempt(record)

    def test_success_failure_retry_skip_finalize_trace(self):
        backend = InMemoryStateBackend()
        state = PipelineState({}, {}, {}, [])
        store = StateStore(backend, state)

        aid_ok = self._open_running(backend, "t::ok")
        store.apply_success("t::ok", {}, attempt_id=aid_ok)
        assert backend.load_attempts("t::ok")[0].outcome == "succeeded"
        assert backend.load_attempts("t::ok")[0].finished_at

        aid_fail = self._open_running(backend, "t::fail")
        store.apply_failure("t::fail", {"error": "boom"}, attempt_id=aid_fail)
        rec = backend.load_attempts("t::fail")[0]
        assert rec.outcome == "failed"
        assert rec.error == "boom"

        aid_retry = self._open_running(backend, "t::retry")
        store.apply_retry(
            "t::retry",
            {"task_type": "t", "job_id": "retry"},
            Job("t", "retry").to_dict(),
            attempt_id=aid_retry,
            error="transient",
        )
        rec = backend.load_attempts("t::retry")[0]
        assert rec.outcome == "requeued"
        assert rec.error == "transient"

        aid_skip = self._open_running(backend, "t::skip")
        store.apply_skip("t::skip", attempt_id=aid_skip)
        assert backend.load_attempts("t::skip")[0].outcome == "skipped"

    def test_trace_update_failure_degrades_without_blocking(self):
        """轨迹收尾失败（旁路观测面）降级告警，不阻断主事务。"""
        backend = InMemoryStateBackend()
        state = PipelineState({}, {}, {}, [])

        class TraceBreakingBackend(CommitFailureBackend):
            def update_attempt(self, *a, **k):
                raise RuntimeError("trace backend down")

        store = StateStore(TraceBreakingBackend(backend), state)
        aid = self._open_running(backend, "t::1")

        store.apply_success("t::1", {}, attempt_id=aid)
        assert "t::1" in store.wall

    def test_attempt_id_none_is_noop(self):
        store, _, _ = _make_store()
        store.apply_success("t::1", {}, attempt_id=None)
        assert "t::1" in store.wall


class TestSnapshotAndMembership:
    """uid 快照契约与成员判定接缝。"""

    def _probe_store(self) -> StateStore:
        state = PipelineState(
            {"w::1": {}},
            {"f::1": {}},
            {"c": "v"},
            [{"task_type": "q", "job_id": "1"}],
        )
        return StateStore(InMemoryStateBackend(), state)

    def test_membership_delegation(self):
        store = self._probe_store()
        assert store.is_completed("w::1")
        assert store.is_failed("f::1")
        assert store.is_known("w::1")
        assert store.is_known("q::1")
        assert not store.is_known("unknown::1")
        assert store.cursors == {"c": "v"}

    @pytest.mark.parametrize(
        "prop", ["wall_uids", "failed_uids", "queue_uids", "in_flight_uids"]
    )
    def test_uid_properties_return_frozenset(self, prop):
        store = self._probe_store()
        assert isinstance(getattr(store, prop), frozenset)

    def test_snapshot_isolation(self):
        store = self._probe_store()
        snapshot = store.queue_uids
        store.pop_job(0)
        assert "q::1" in snapshot
        assert "q::1" not in store.queue_uids

    def test_set_state_and_backend_reset(self):
        store, _, _ = _make_store()
        fresh = PipelineState({}, {}, {}, [])
        store.set_state(fresh)
        assert store.state is fresh
        store.set_state(None)
        assert store.is_empty
        new_backend = InMemoryStateBackend()
        store.set_backend(new_backend)
        assert store.backend is new_backend


class TestFailureEntryShape:
    """FailureEntry 数据类形状（运维查询面 4e 的载荷契约）。"""

    def test_frozen_shape(self):
        entry = FailureEntry(
            uid="t::1", error="boom", meta={"error": "boom"}, job_payload={"k": 1}
        )
        assert entry.uid == "t::1"
        assert entry.error == "boom"
        assert entry.meta == {"error": "boom"}
        assert entry.job_payload == {"k": 1}
        with pytest.raises(Exception):
            entry.uid = "t::2"  # type: ignore[misc]

    def test_payload_defaults_none(self):
        entry = FailureEntry(uid="t::1", error="x", meta={})
        assert entry.job_payload is None


# ── wall/failed 互斥性质（hypothesis 任意操作序列）─────────────────────

_UID_POOL = [
    "encode::1",
    "encode::2",
    "render::1",
    "render::2",
]


def _job_dict(uid: str) -> dict:
    task_type, job_id = uid.split("::", 1)
    return {"task_type": task_type, "job_id": job_id}


op_strategy = st.lists(
    st.tuples(
        st.sampled_from(
            ["enqueue", "success", "failure", "mark_failed", "retry", "skip", "bulk_failure"]
        ),
        st.integers(min_value=0, max_value=len(_UID_POOL) - 1),
    ),
    max_size=24,
)


class _MutexProbe:
    """对一组随机操作执行真实状态机协议，并在每步后断言互斥不变式。"""

    def __init__(self) -> None:
        self.backend = InMemoryStateBackend()
        self.state = PipelineState({}, {}, {}, [])
        self.store = StateStore(self.backend, self.state)

    def _pop_from_memory_queue(self, uid: str) -> None:
        for idx, jd in enumerate(self.store.queue):
            if f"{jd.get('task_type')}::{jd.get('job_id')}" == uid:
                self.store.pop_job(idx)
                return

    def apply(self, action: str, i: int) -> None:
        uid = _UID_POOL[i]
        if action == "enqueue":
            if self.store.is_known(uid) or uid in self.store.queue_uids:
                return
            self.store.spawn_jobs([_job_dict(uid)], front=False)
        elif action == "success":
            self._pop_from_memory_queue(uid)
            self.store.apply_success(uid, {"score": i})
        elif action == "failure":
            self._pop_from_memory_queue(uid)
            self.store.apply_failure(uid, {"error": "boom"}, job_dict=_job_dict(uid))
        elif action == "mark_failed":
            # mark_failed_memory 是失败登记的内存尾段：先完成后端提交再登记
            self._pop_from_memory_queue(uid)
            if self.backend.commit_job_failure(uid, {"error": "tail"}):
                self.store.mark_failed_memory(uid, {"error": "tail"})
        elif action == "retry":
            if self.store.is_completed(uid) or self.store.is_failed(uid):
                return
            self._pop_from_memory_queue(uid)
            self.store.apply_retry(uid, _job_dict(uid), _job_dict(uid))
        elif action == "skip":
            self._pop_from_memory_queue(uid)
            self.store.apply_skip(uid)
        elif action == "bulk_failure":
            self._pop_from_memory_queue(uid)
            self.store.apply_bulk_failure([(uid, {"error": "deadlock"})])
        self.assert_mutex()

    def assert_mutex(self) -> None:
        """核心不变式：wall ∩ failed = ∅（内存与后端镜像一致），终态不留在队列。"""
        mem_wall = set(self.store.wall_uids)
        mem_failed = set(self.store.failed_uids)
        assert mem_wall & mem_failed == set(), f"内存互斥破坏: {mem_wall & mem_failed}"
        disk_wall = set(self.backend.load_wall())
        disk_failed = set(self.backend.load_failed())
        assert disk_wall & disk_failed == set(), f"后端互斥破坏: {disk_wall & disk_failed}"
        assert mem_wall == disk_wall, f"wall 镜像漂移: {mem_wall} vs {disk_wall}"
        assert mem_failed == disk_failed, f"failed 镜像漂移: {mem_failed} vs {disk_failed}"
        assert mem_wall | mem_failed <= set(_UID_POOL)
        for uid in mem_wall | mem_failed:
            assert uid not in self.store.queue_uids, f"终态 {uid} 残留在队列"


@pytest.mark.hypothesis
@settings(max_examples=40, deadline=None)
@example(ops=[])
@example(ops=[("success", 0), ("failure", 0), ("success", 0)])
@example(ops=[("failure", 1), ("enqueue", 1), ("retry", 1)])
@given(ops=op_strategy)
def test_wall_failed_mutex_holds_for_arbitrary_operation_sequences(ops):
    """任意操作序列后（及每一步后），wall/failed 全局互斥且镜像一致。"""
    probe = _MutexProbe()
    probe.assert_mutex()
    for action, i in ops:
        probe.apply(action, i)
    probe.assert_mutex()


@pytest.mark.hypothesis
@settings(max_examples=40, deadline=None)
@given(i=st.integers(min_value=0, max_value=len(_UID_POOL) - 1))
def test_terminal_transitions_move_uid_exactly_out_of_opposite_set(i):
    """成功/失败互逆转移：任一终态写入都把 uid 从对侧终态集合精确移出。"""
    uid = _UID_POOL[i]
    probe = _MutexProbe()
    probe._pop_from_memory_queue(uid)

    probe.store.apply_failure(uid, {"error": "boom"}, job_dict=_job_dict(uid))
    assert probe.store.is_failed(uid)
    assert not probe.store.is_completed(uid)

    probe.store.apply_success(uid, {"score": 1})
    assert probe.store.is_completed(uid)
    assert not probe.store.is_failed(uid)

    probe.store.apply_failure(uid, {"error": "again"}, job_dict=_job_dict(uid))
    assert probe.store.is_failed(uid)
    assert not probe.store.is_completed(uid)
    probe.assert_mutex()
