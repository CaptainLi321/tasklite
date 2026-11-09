"""v2 StateStore 状态仓库测试：六出口事务、失败档案、3-strike 与摄入管道。

覆盖：apply_* 原子终态转移（内存/后端镜像一致）、successes/failed 互斥性质
（hypothesis 任意操作序列）、mark_failed_memory 联动契约、commit 失败
3-strike 收敛、enqueue 摄入三步（策略注入 / 工人资源注入 / 首入队时间
填充）、瞬态降级回队与 attempts 旁路轨迹收尾。
"""

from __future__ import annotations

from collections import defaultdict

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import TriggerRegistry
from tasklite.engine.errorclass import (
    ERR_COMMIT_FAILURE,
    ERR_JOB_DEPENDENCY,
)
from tasklite.engine.store import (
    COMMIT_FAILURE_THRESHOLD,
    BulkFailureOutcome,
    FailureEntry,
    FailureOutcome,
    RetryOutcome,
    SkipOutcome,
    StateStore,
    SuccessOutcome,
)
from tasklite.engine.types import AttemptOutcome
from tasklite.exceptions import _CommitCrashSignal, _JobTerminated
from tasklite.models.job import Job, WORKER_RESOURCE
from tasklite.models.state import PipelineState
from tasklite.models.task import TaskRegistry


def _task_specs(task_type, trigger):
    from tasklite import Task
    from tasklite.models.task import TaskRegistry
    tasks = TaskRegistry()
    tasks.register(Task(task_type, lambda job, ctx: True, trigger=trigger))
    return tasks


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

    def commit_job_failure(self, job_uid, meta, *a, **k):
        if "commit_job_failure" in self._failing:
            if self._archive_pass_through and meta.get("error_message") == ERR_COMMIT_FAILURE:
                return self._real.commit_job_failure(job_uid, meta, *a, **k)
            return False
        return self._real.commit_job_failure(job_uid, meta, *a, **k)

    def commit_retry(self, *a, **k):
        if "commit_retry" in self._failing:
            return False
        return self._real.commit_retry(*a, **k)

    def commit_job_failures(self, *a, **k):
        if "commit_job_failures" in self._failing:
            return False
        return self._real.commit_job_failures(*a, **k)

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


def _open_running_attempt(backend: InMemoryStateBackend, uid: str = "t::1", execution_id: str = "run_abc.1", run_id: str = "run_abc") -> int:
    from tasklite.models.attempt import AttemptRecord

    return backend.append_attempt(
        AttemptRecord(
            job_uid=uid,
            activation_no=1,
            attempt_no=1,
            execution_id=execution_id,
            run_id=run_id,
            started_at="2026-01-01T00:00:00+00:00",
            outcome="running",
        )
    )


class TestApplySuccess:
    """apply_success 原子终态转移。"""

    def test_basic_wall_meta(self):
        store, state, backend = _make_store()
        state.register_in_flight("process::1")
        aid = _open_running_attempt(backend, "process::1")

        outcome = store.apply_success(
            "process::1",
            {"score": 100},
            run_id="run_abc",
            declared_inputs=["/tmp/a.txt"],
            attempt_id=aid,
            execution_id="run_abc.1",
        )

        assert isinstance(outcome, SuccessOutcome)
        assert outcome.job_uid == "process::1"
        assert store.state.successes["process::1"]["score"] == 100
        assert store.state.successes["process::1"]["last_success_run_id"] == "run_abc"
        assert store.state.successes["process::1"]["success_count"] == 1
        assert "last_succeeded_at" in store.state.successes["process::1"]
        assert store.state.successes["process::1"]["inputs"] == ["/tmp/a.txt"]
        assert "process::1" not in store.state.in_flight_uids
        (rec,) = backend.load_attempts("process::1")
        assert rec.outcome == "succeeded"
        assert rec.finished_at == store.state.successes["process::1"]["last_succeeded_at"]

    def test_run_count_continues_from_opposite_terminal(self):
        """重跑成功从失败档案对侧终态续数（run_count 跨终态递增）。"""
        store, state, backend = _make_store()
        state.register_in_flight("t::1")
        store.apply_failure("t::1", {"error_message": "boom", "success_count": 4})
        state.register_in_flight("t::1")
        aid = _open_running_attempt(backend, "t::1")
        store.apply_success("t::1", {}, attempt_id=aid)
        assert store.state.successes["t::1"]["success_count"] == 5
        assert "t::1" not in store.state.failed

    def test_spawn_and_cursor_updates(self):
        store, _, backend = _make_store()
        store.state.register_in_flight("root::1")
        aid = _open_running_attempt(backend, "root::1")
        spawned = [
            {"task_type": "child", "job_id": "c1"},
            {"task_type": "child", "job_id": "c2"},
        ]

        outcome = store.apply_success(
            "root::1",
            {},
            spawned_job_snapshots=spawned,
            cursor_updates={"batch_pos": "100", "old_cursor": None},
            attempt_id=aid,
        )

        assert outcome.spawned_uids == ["child::c1", "child::c2"]
        assert store.state.cursors["batch_pos"] == "100"
        assert "old_cursor" not in store.state.cursors
        assert [jd["job_id"] for jd in store.state.queue] == ["c1", "c2"]
        assert store.stats is None  # 未注入统计不炸

    def test_declared_inputs_deduped_preserving_order(self):
        store, _, backend = _make_store()
        aid = _open_running_attempt(backend, "t::1")
        store.apply_success(
            "t::1",
            {"inputs": ["/user/spoofed"]},
            declared_inputs=["/b", "/a", "/b", "", 42, {"path": "/c"}],
            attempt_id=aid,
        )
        assert store.state.successes["t::1"]["inputs"] == ["/b", "/a"]

    def test_user_inputs_key_dropped_without_declarations(self):
        store, _, backend = _make_store()
        aid = _open_running_attempt(backend, "t::1")
        store.apply_success("t::1", {"inputs": ["/user/spoofed"]}, attempt_id=aid)
        assert "inputs" not in store.state.successes["t::1"]


class TestTrustedLastSucceededAtUs:
    """可信成功基线查询（trusted_last_succeeded_at_us）判定矩阵。"""

    def test_missing_successes_returns_none_even_with_attempts_row(self):
        store, _, backend = _make_store()
        aid = _open_running_attempt(backend, "t::1")
        backend.update_attempt(aid, outcome="succeeded", finished_at="2026-05-01T00:00:05+00:00")
        assert store.trusted_last_succeeded_at_us("t::1") is None

    def test_manual_succeeded_empty_metadata_returns_none(self):
        store, state, backend = _make_store()
        aid = _open_running_attempt(backend, "t::1")
        backend.update_attempt(aid, outcome="succeeded", finished_at="2026-05-01T00:00:05+00:00")
        state.add_success("t::1", {})
        assert store.trusted_last_succeeded_at_us("t::1") is None

    def test_clear_history_then_seed_rejects_old_attempts_row(self):
        store, state, backend = _make_store()
        aid = _open_running_attempt(backend, "t::1")
        store.apply_success("t::1", {}, attempt_id=aid, finished_at="2026-05-01T00:00:05+00:00")
        assert store.trusted_last_succeeded_at_us("t::1") is not None

        # 模拟清空 successes 历史后人工种子
        state.discard_success("t::1")
        assert store.trusted_last_succeeded_at_us("t::1") is None
        state.add_success("t::1", {})
        assert store.trusted_last_succeeded_at_us("t::1") is None

    def test_incomplete_or_corrupt_metadata_returns_none(self):
        store, state, backend = _make_store()
        aid = _open_running_attempt(backend, "t::1")
        backend.update_attempt(aid, outcome="succeeded", finished_at="2026-05-01T00:00:05+00:00")

        # 缺 success_count / 非整数
        state.add_success("t::1", {"last_succeeded_at": "2026-05-01T00:00:05+00:00"})
        assert store.trusted_last_succeeded_at_us("t::1") is None
        state.add_success("t::1", {"last_succeeded_at": "2026-05-01T00:00:05+00:00", "success_count": 0})
        assert store.trusted_last_succeeded_at_us("t::1") is None
        state.add_success("t::1", {"last_succeeded_at": "2026-05-01T00:00:05+00:00", "success_count": True})
        assert store.trusted_last_succeeded_at_us("t::1") is None

        # 坏时间戳
        state.add_success("t::1", {"last_succeeded_at": "corrupt", "success_count": 1})
        assert store.trusted_last_succeeded_at_us("t::1") is None

        # 空 last_success_run_id
        state.add_success(
            "t::1",
            {"last_succeeded_at": "2026-05-01T00:00:05+00:00", "success_count": 1, "last_success_run_id": ""},
        )
        assert store.trusted_last_succeeded_at_us("t::1") is None

    def test_valid_success_returns_exact_integer_microseconds(self):
        store, _, backend = _make_store()
        aid = _open_running_attempt(backend, "t::1")
        store.apply_success(
            "t::1",
            {},
            run_id="run_valid",
            attempt_id=aid,
            finished_at="1970-01-01T00:00:01.123456+00:00",
        )
        assert store.trusted_last_succeeded_at_us("t::1") == 1_123_456

    def test_mismatched_finished_at_between_successes_and_attempts_returns_none(self):
        """successes 的 last_succeeded_at 与 attempts 最新 succeeded 行时间不一致时不采信。"""
        store, state, backend = _make_store()
        aid = _open_running_attempt(backend, "t::1")
        backend.update_attempt(aid, outcome="succeeded", finished_at="2026-05-01T00:00:01+00:00")
        state.add_success(
            "t::1",
            {
                "success_count": 1,
                "last_succeeded_at": "2026-05-01T00:00:09+00:00",
                "last_success_run_id": "run_1",
            },
        )
        # 即使两者各自合法，只要不相等就不采信为基线
        assert store.trusted_last_succeeded_at_us("t::1") is None


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
            {"error_message": "crash"},
            job_snapshot={"task_type": "parent", "job_id": "1"},
        )

        assert isinstance(outcome, FailureOutcome)
        assert set(outcome.cascaded_uids) == {"child::1", "grandchild::1"}
        for uid in ("parent::1", "child::1", "grandchild::1"):
            assert uid in store.state.failed
            assert uid in store.backend.load_failed()
        assert store.state.failed["child::1"]["error_message"] == ERR_JOB_DEPENDENCY
        assert store.state.failed["child::1"]["failed_dependency"] == "parent::1"
        assert "parent::1" not in store.state.in_flight_uids

    def test_failure_payload_snapshot_skipped_when_unserializable(self, caplog):
        """payload 快照不可序列化时降级 None——快照失败不得误触 3-strike。"""
        store, _, backend = _make_store()
        job_snapshot = {
            "task_type": "t",
            "job_id": "1",
            "payload": {"bad": object()},
        }
        store.apply_failure("t::1", {"error_message": "x"}, job_snapshot=job_snapshot)
        assert "t::1" in store.state.failed
        assert "t::1" not in backend.load_failure_payloads()

    def test_failure_clears_wall_on_both_sides(self):
        """失败终态与成功终态互删对侧（内存与后端镜像同步）。"""
        store, _, backend = _make_store()
        store.state.register_in_flight("t::1")
        aid = _open_running_attempt(backend, "t::1")
        store.apply_success("t::1", {}, attempt_id=aid)
        store.state.register_in_flight("t::1")
        store.apply_failure("t::1", {"error_message": "again"})

        assert "t::1" not in store.state.successes
        assert "t::1" in store.state.failed
        assert "t::1" not in backend.load_successes()
        assert "t::1" in backend.load_failed()


class TestApplyRetry:
    """apply_retry 重试重入队。"""

    def _retry_job_snapshot(self) -> dict:
        return Job._restore("t", "1", attempt_no=2).to_dict()

    def test_commit_path_requeues_front_and_records_stats(self):
        stats = defaultdict(int)
        store, state, _ = _make_store(stats=stats)
        state.replace_queue([])
        store.state.register_in_flight("t::1")

        outcome = store.apply_retry(
            "t::1",
            {"task_type": "t", "job_id": "1"},
            self._retry_job_snapshot(),
            transient_kind="interrupted",
            error_message="transient blip",
        )

        assert isinstance(outcome, RetryOutcome)
        assert outcome.transient_kind == "interrupted"
        assert store.state.queue[0]["job_id"] == "1"
        assert "t::1" not in store.state.in_flight_uids
        assert stats["retried"] == 1
        assert stats["interrupted_reruns"] == 1

    def test_commit_failure_raises_crash_signal(self):
        backend = InMemoryStateBackend()
        failing = CommitFailureBackend(backend, failing=("commit_retry",))
        state = PipelineState({}, {}, {}, [])
        store = StateStore(failing, state, stats={})

        with pytest.raises(_CommitCrashSignal, match="commit_retry"):
            store.apply_retry("t::1", {"task_type": "t", "job_id": "1"}, self._retry_job_snapshot())
        assert "t::1" in store.state.queue_uids


class TestApplySkip:
    """apply_skip 去重跳过。"""

    @pytest.mark.parametrize("dispatched", [False, True])
    def test_skip_records_stat_and_clears_queue(self, dispatched):
        stats = defaultdict(int)
        backend = InMemoryStateBackend()
        backend.save_queue([{"task_type": "t", "job_id": "1"}])
        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "1"}])
        store = StateStore(backend, state, stats=stats)
        if dispatched:
            state.pop_job(0)

        outcome = store.apply_skip("t::1", job_snapshot={"task_type": "t", "job_id": "1"})

        assert isinstance(outcome, SkipOutcome)
        assert outcome.was_known is True
        assert stats["skipped"] == 1
        assert store.state.is_empty
        assert backend.load_queue() == []

    def test_skip_preserves_other_in_flight_rerun_exemption(self):
        store, state, backend = _make_store()
        aid = _open_running_attempt(backend, "t::running")
        store.apply_success("t::running", {}, attempt_id=aid)
        state.register_in_flight("t::running")
        pending = Job("t", "skip").to_dict()
        state.spawn_jobs([pending])
        backend.save_queue([pending])

        store.apply_skip("t::skip", job_snapshot=pending)

        assert state.is_empty
        assert state.in_flight_uids == {"t::running"}
        state._assert_state_consistent()

    def test_skip_commit_failure_crashes_without_archive(self):
        """commit_skip 失败：只 requeue + 崩溃，绝不写失败档案。"""
        backend = InMemoryStateBackend()
        failing = CommitFailureBackend(backend, failing=("commit_skip",))
        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "1"}])
        store = StateStore(failing, state, stats={})
        state.pop_job(0)

        with pytest.raises(_CommitCrashSignal, match="commit_skip"):
            store.apply_skip("t::1", job_snapshot={"task_type": "t", "job_id": "1"})
        assert "t::1" in store.state.queue_uids
        assert "t::1" not in store.state.failed


class TestMarkFailedMemoryContract:
    """mark_failed_memory 失败终态内存尾段契约。"""

    def test_default_registers_failed_and_unregisters_in_flight(self):
        store, state, _ = _make_store()
        state.register_in_flight("encode::a")
        store.mark_failed_memory("encode::a", {"error_message": "boom"})
        assert "encode::a" in state.failed_uids
        assert "encode::a" not in state.in_flight_uids
        assert state.failed["encode::a"]["error_message"] == "boom"

    def test_explicit_unregister_false_keeps_in_flight(self):
        store, state, _ = _make_store()
        state.register_in_flight("encode::a")
        store.mark_failed_memory("encode::a", {"error_message": "boom"}, unregister=False)
        assert "encode::a" in state.failed_uids
        assert "encode::a" in state.in_flight_uids

    def test_failed_takes_over_from_wall(self):
        store, state, _ = _make_store()
        state.add_success("encode::a", {"prev": "ok"})
        store.mark_failed_memory("encode::a", {"error_message": "boom"})
        assert "encode::a" in state.failed_uids
        assert "encode::a" not in state.success_uids


class TestCommitFailureStrike:
    """commit 失败 3-strike 收敛契约。"""

    def _strike_store(self, hooks: list[tuple[str, AttemptOutcome]]) -> StateStore:
        backend = CommitFailureBackend(
            InMemoryStateBackend(),
            failing=("commit_job_success", "commit_job_failure"),
            archive_pass_through=True,
        )
        return StateStore(
            backend,
            PipelineState({}, {}, {}, []),
            commit_failure_threshold=3,
            stats={},
            on_attempt_finished=lambda uid, *, outcome: hooks.append((uid, outcome)),
        )

    def test_threshold_retry_then_archive(self):
        store = self._strike_store([])
        job_snapshot = {"task_type": "task", "job_id": "1"}

        store.state.register_in_flight("task::1")
        with pytest.raises(_CommitCrashSignal):
            store.apply_success("task::1", {}, job_snapshot=job_snapshot)
        assert job_snapshot["runtime"]["_commit_failures"] == 1
        assert "task::1" in store.state.queue_uids

        store.state.pop_job(0)
        store.state.register_in_flight("task::1")
        with pytest.raises(_CommitCrashSignal):
            store.apply_success("task::1", {}, job_snapshot=job_snapshot)
        assert job_snapshot["runtime"]["_commit_failures"] == 2

        store.state.pop_job(0)
        store.state.register_in_flight("task::1")
        with pytest.raises(_JobTerminated, match="permanently failed"):
            store.apply_success("task::1", {}, job_snapshot=job_snapshot)
        assert "task::1" in store.state.failed
        assert store.state.failed["task::1"]["is_fatal"] is True
        assert store.state.failed["task::1"]["error_message"] == ERR_COMMIT_FAILURE

    def test_strike_archive_fires_hook_exactly_once(self):
        hooks: list[tuple[str, AttemptOutcome]] = []
        store = self._strike_store(hooks)
        job_snapshot = {"task_type": "task", "job_id": "1"}
        for _ in range(2):
            store.state.register_in_flight("task::1")
            with pytest.raises(_CommitCrashSignal):
                store.apply_success("task::1", {}, job_snapshot=job_snapshot)
            store.state.pop_job(0)
        store.state.register_in_flight("task::1")
        with pytest.raises(_JobTerminated):
            store.apply_success("task::1", {}, job_snapshot=job_snapshot)

        assert len(hooks) == 1
        uid, fin = hooks[0]
        assert uid == "task::1"
        assert fin.success is False
        assert fin.will_retry is False
        assert fin.metadata["error_message"] == ERR_COMMIT_FAILURE
        assert store.stats["failed"] == 1

    def test_default_threshold_constant(self):
        assert COMMIT_FAILURE_THRESHOLD == 3


class TestHookSingleExit:
    """fire_attempt_finished 单一出口与异常隔离。"""

    def test_hook_exception_isolated_and_counted(self):
        stats = defaultdict(int)
        store, _, _ = _make_store(stats=stats)

        def boom(uid, *, outcome):
            raise RuntimeError("hook failure")

        store.set_on_attempt_finished(boom)
        store.fire_attempt_finished(
            "t::1", outcome=AttemptOutcome(success=True, will_retry=False, metadata={})
        )
        assert stats["hook_errors"] == 1

    def test_no_callback_is_noop(self):
        store, _, _ = _make_store()
        store.fire_attempt_finished(
            "t::1", outcome=AttemptOutcome(success=True, will_retry=False, metadata={})
        )

    def test_set_on_attempt_finished_rebinds(self):
        captured: list[str] = []
        store, _, _ = _make_store()
        store.set_on_attempt_finished(lambda uid, *, outcome: captured.append(uid))
        store.fire_attempt_finished(
            "t::1", outcome=AttemptOutcome(success=False, will_retry=True, metadata={})
        )
        assert captured == ["t::1"]

    def test_bulk_failure_fires_hook_per_uid(self):
        stats = defaultdict(int)
        hooks: list[tuple[str, AttemptOutcome]] = []
        backend = InMemoryStateBackend()
        store = StateStore(
            backend,
            PipelineState({}, {}, {}, []),
            stats=stats,
            on_attempt_finished=lambda uid, *, outcome: hooks.append((uid, outcome)),
        )

        store.apply_bulk_failure(
            [("cycle::1", {"error_message": "DEPENDENCY_DEADLOCK"}),
             ("cycle::2", {"error_message": "DEPENDENCY_DEADLOCK"})]
        )
        assert {uid for uid, _ in hooks} == {"cycle::1", "cycle::2"}
        assert all(fin.success is False and fin.will_retry is False for _, fin in hooks)
        assert stats["failed"] == 2

    def test_cascade_fires_hook_per_downstream(self):
        hooks: list[tuple[str, AttemptOutcome]] = []
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

        outcome = store.apply_failure("parent::1", {"error_message": "crash"})
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
                ("cycle::1", {"error_message": "DEPENDENCY_DEADLOCK"}),
                ("cycle::2", {"error_message": "DEPENDENCY_DEADLOCK"}),
            ],
        )

        assert isinstance(outcome, BulkFailureOutcome)
        assert outcome.failed_uids == ["cycle::1", "cycle::2"]
        assert outcome.remaining_queue_count == 1
        assert "cycle::1" in store.state.failed
        assert "safe::1" in store.state.queue_uids

    def test_bulk_failure_keeps_wall_deleted_on_backend(self):
        """批量失败与单条失败对称：successes 同名行随事务删除。"""
        store, _, backend = _make_store()
        backend.commit_job_success("cycle::1", {})
        store.apply_bulk_failure([("cycle::1", {"error_message": "DEPENDENCY_DEADLOCK"})])
        assert "cycle::1" not in backend.load_successes()

    def test_bulk_commit_failure_below_threshold_crashes(self):
        backend = InMemoryStateBackend()
        failing = CommitFailureBackend(
            backend,
            failing=("commit_job_failures", "commit_job_failure"),
        )
        state = PipelineState({}, {}, {}, [{"task_type": "cycle", "job_id": "1"}])
        store = StateStore(failing, state, commit_failure_threshold=3)

        with pytest.raises(_CommitCrashSignal):
            store.apply_bulk_failure([("cycle::1", {"error_message": "DEPENDENCY_DEADLOCK"})])
        assert store.state.queue[0]["runtime"]["_commit_failures"] == 1
        assert "cycle::1" not in store.state.failed


class TestBulkStrikeConvergence:
    """批量 3-strike 两路（死锁批量 / 级联批量）收敛语义分叉锁定。"""

    def _strike_bulk_store(
        self, hooks: list, queue_rows: list, *, threshold: int = 3
    ) -> StateStore:
        backend = CommitFailureBackend(
            InMemoryStateBackend(),
            failing=("commit_job_failures",),
            archive_pass_through=True,
        )
        state = PipelineState({}, {}, {}, queue_rows)
        return StateStore(
            backend,
            state,
            commit_failure_threshold=threshold,
            stats={},
            on_attempt_finished=lambda uid, *, outcome: hooks.append((uid, outcome)),
        )

    def test_deadlock_and_cascade_strike_meta_converged(self):
        """死锁批量与级联批量的熔断 meta 同规（归因统一 ERR_COMMIT_FAILURE）。"""
        hooks_deadlock: list[tuple[str, AttemptOutcome]] = []
        store_deadlock = self._strike_bulk_store(
            hooks_deadlock,
            [{"task_type": "cycle", "job_id": "1", "runtime": {"_commit_failures": 2}}],
        )
        outcome = store_deadlock.apply_bulk_failure(
            [("cycle::1", {"error_message": "DEPENDENCY_DEADLOCK"})]
        )
        assert outcome.failed_uids == ["cycle::1"]
        deadlock_meta = store_deadlock.state.failed["cycle::1"]

        hooks_cascade: list[tuple[str, AttemptOutcome]] = []
        store_cascade = self._strike_bulk_store(
            hooks_cascade,
            [
                {"task_type": "parent", "job_id": "1"},
                {
                    "task_type": "child",
                    "job_id": "1",
                    "depends_on": ["parent::1"],
                    "runtime": {"_commit_failures": 2},
                },
            ],
        )
        store_cascade.state.pop_job(0)
        cascade_outcome = store_cascade.apply_failure("parent::1", {"error_message": "boom"})
        assert cascade_outcome.cascaded_uids == ["child::1"]
        cascade_meta = store_cascade.state.failed["child::1"]

        for meta in (deadlock_meta, cascade_meta):
            assert meta["error_message"] == ERR_COMMIT_FAILURE
            assert meta["is_fatal"] is True
            assert meta["commit_failures"] == 3
        assert [uid for uid, _ in hooks_deadlock] == ["cycle::1"]
        assert [uid for uid, _ in hooks_cascade] == ["child::1"]
        for _, fin in hooks_deadlock + hooks_cascade:
            assert fin.success is False and fin.will_retry is False
            assert fin.metadata["error_message"] == ERR_COMMIT_FAILURE

    def test_out_of_queue_uid_still_finalized_and_accounted(self):
        """归因中的队列外 uid 同规计数与熔断（记账绝不静默丢失）。"""
        hooks: list[tuple[str, AttemptOutcome]] = []
        store = self._strike_bulk_store(
            hooks,
            [{"task_type": "cycle", "job_id": "1"}],
            threshold=1,
        )

        outcome = store.apply_bulk_failure(
            [
                ("cycle::1", {"error_message": "DEPENDENCY_DEADLOCK"}),
                ("ghost::9", {"error_message": "DEPENDENCY_DEADLOCK"}),
            ]
        )

        assert outcome.failed_uids == ["cycle::1", "ghost::9"]
        assert outcome.remaining_queue_count == 0
        assert store.state.failed["cycle::1"]["error_message"] == ERR_COMMIT_FAILURE
        assert store.state.failed["ghost::9"]["error_message"] == ERR_COMMIT_FAILURE
        assert "ghost::9" not in store.state.queue_uids
        assert sorted(uid for uid, _ in hooks) == ["cycle::1", "ghost::9"]
        assert store.stats["failed"] == 2


class TestIntakePipeline:
    """enqueue 摄入管道三步 + 首入队时间。"""

    def test_enqueue_fills_first_enqueued_at_once(self):
        backend = InMemoryStateBackend()
        store = StateStore(
            backend,
            PipelineState({}, {}, {}, []),
            trigger_registry=TriggerRegistry(tasks=_task_specs("t", "once")),
        )
        assert store.enqueue_job(Job("t", "1")) is True
        row = backend.load_queue()[0]
        assert row["first_enqueued_at"]
        assert row["resources"][WORKER_RESOURCE] == 1.0

    def test_enqueue_does_not_refresh_first_enqueued_at(self):
        store, _, _ = _make_store()
        job = Job._restore("t", "1", first_enqueued_at="2026-01-01T00:00:00+00:00")
        jd = store.normalize_and_validate_job(job)
        assert jd["first_enqueued_at"] == "2026-01-01T00:00:00+00:00"

    def test_enqueue_injects_discovery_trigger_for_unspecified(self):
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            trigger_registry=TriggerRegistry(tasks=_task_specs("scan", "always")),
        )
        jd = store.normalize_and_validate_job(Job("scan", "1"))
        assert jd["trigger"] == "always"
        jd_explicit = store.normalize_and_validate_job(Job("scan", "2", trigger="once"))
        assert jd_explicit["trigger"] == "once"

    def test_enqueue_rejects_unserializable_payload(self):
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            trigger_registry=TriggerRegistry(tasks=_task_specs("t", "once")),
        )
        with pytest.raises(ValueError, match="not JSON-serializable"):
            store.enqueue_job(Job("t", "1", payload={"bad": object()}))

    def test_enqueue_rejects_non_job(self):
        store, _, _ = _make_store()
        with pytest.raises(TypeError):
            store.enqueue_job("not-a-job")  # type: ignore[arg-type]

    def test_enqueue_rejects_unregistered_task(self):
        from tasklite import TaskLiteError

        backend = InMemoryStateBackend()
        store = StateStore(
            backend,
            PipelineState({}, {}, {}, []),
            trigger_registry=TriggerRegistry(tasks=TaskRegistry()),
        )
        job = Job("missing", "1")
        with pytest.raises(TaskLiteError, match="unknown task_type"):
            store.enqueue_job(job)
        assert backend.load_queue() == []
        assert backend.load_attempts(job.uid) == []

    def test_enqueue_job_same_uid_idempotent_vs_conflict(self):
        from tasklite import Task, TaskLiteError
        from tasklite.models.task import TaskRegistry

        tasks = TaskRegistry()
        tasks.register(
            Task(
                "t",
                lambda job, ctx: True,
                resources={"gpu": 1.0},
                max_retries_per_run=4,
                timeout_seconds=120,
                trigger="once",
            )
        )
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            trigger_registry=TriggerRegistry(tasks=tasks),
        )
        assert store.enqueue_job(Job("t", "1")) is True
        # 显式传入与继承后的有效配置完全等价（且空 dict 不清空 Task 资源）→ 返回 False
        assert (
            store.enqueue_job(
                Job(
                    "t",
                    "1",
                    payload={},
                    depends_on=[],
                    files={},
                    resources={},
                    max_retries_per_run=4,
                    timeout_seconds=120,
                    trigger="once",
                )
            )
            is False
        )
        # 有效配置不同 → 抛 TaskLiteError
        with pytest.raises(TaskLiteError):
            store.enqueue_job(Job("t", "1", payload={"diff": 1}))
        with pytest.raises(TaskLiteError):
            store.enqueue_job(Job("t", "1", max_retries_per_run=0))

    def test_normalize_spawned_job_matches_intake_rules(self):
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            trigger_registry=TriggerRegistry(tasks=_task_specs("child", "on_failure")),
        )
        jd = store.normalize_spawned_job(Job("child", "c1"))
        assert jd["trigger"] == "on_failure"
        assert jd["resources"][WORKER_RESOURCE] == 1.0
        assert jd["first_enqueued_at"]

    def test_normalize_spawned_job_rejects_unserializable(self):
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            trigger_registry=TriggerRegistry(tasks=_task_specs("t", "once")),
        )
        with pytest.raises(ValueError, match="Spawned job"):
            store.normalize_spawned_job(Job("t", "1", payload={"bad": object()}))

    def test_normalize_spawned_job_rejects_unregistered_task(self):
        from tasklite import TaskLiteError

        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            trigger_registry=TriggerRegistry(tasks=TaskRegistry()),
        )
        with pytest.raises(TaskLiteError, match="unknown task_type"):
            store.normalize_spawned_job(Job("missing", "1"))


class TestRequeueTransient:
    """瞬态信号降级回队单一出口（军规）。"""

    @pytest.mark.parametrize(
        ("kind", "stat_key"),
        [
            ("interrupted", "interrupted_reruns"),
            ("lock_conflict", "lock_conflict_reruns"),
            ("rate_limited", "rate_limited_reruns"),
        ],
    )
    def test_requeue_transient_records_stat_and_fronts(self, kind, stat_key):
        stats = defaultdict(int)
        store, state, _ = _make_store(stats=stats)
        jd = Job("t", "1").to_dict()

        store.requeue_transient(jd, transient_kind=kind)

        assert state.queue[0]["job_id"] == "1"
        assert stats[stat_key] == 1

    def test_requeue_transient_leaves_runtime_untouched(self):
        """零污染：不烧 commit/dispatch 失败预算、不写 last_retry_error。"""
        stats = defaultdict(int)
        store, _, _ = _make_store(stats=stats)
        jd = Job("t", "1").to_dict()

        store.requeue_transient(jd, transient_kind="rate_limited")

        assert jd["runtime"] == {}
        assert stats["retried"] == 0


class TestAttemptTraceFinalization:
    """apply_* 终态的 attempts 旁路轨迹收尾。"""

    def _open_running(self, backend, uid="t::1"):
        from tasklite.models.attempt import AttemptRecord

        record = AttemptRecord(
            job_uid=uid,
            activation_no=1,
            attempt_no=1,
            execution_id="r" * 32 + ".1",
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
        store.apply_failure("t::fail", {"error_message": "boom"}, attempt_id=aid_fail)
        rec = backend.load_attempts("t::fail")[0]
        assert rec.outcome == "failed"
        assert rec.error_message == "boom"

        aid_retry = self._open_running(backend, "t::retry")
        store.apply_retry(
            "t::retry",
            {"task_type": "t", "job_id": "retry"},
            Job("t", "retry").to_dict(),
            attempt_id=aid_retry,
            error_message="transient",
        )
        rec = backend.load_attempts("t::retry")[0]
        assert rec.outcome == "requeued"
        assert rec.error_message == "transient"

        aid_skip = self._open_running(backend, "t::skip")
        store.apply_skip("t::skip", attempt_id=aid_skip)
        assert backend.load_attempts("t::skip")[0].outcome == "skipped"

    def test_trace_update_failure_degrades_without_blocking(self):
        """旁路轨迹收尾失败（失败/重试/跳过路径）降级告警，不阻断主事务。"""
        backend = InMemoryStateBackend()
        state = PipelineState({}, {}, {}, [])

        class TraceBreakingBackend(CommitFailureBackend):
            def update_attempt(self, *a, **k):
                raise RuntimeError("trace backend down")

        store = StateStore(TraceBreakingBackend(backend), state)
        aid = self._open_running(backend, "t::1")

        store.apply_failure("t::1", {"error_message": "boom"}, attempt_id=aid)
        assert "t::1" in store.state.failed

    def test_success_missing_or_invalid_attempt_rolls_back_and_keeps_state_clean(self):
        """成功提交缺少有效 running attempt 行时整体失败回滚，内存状态/统计/钩子不提前变化。"""
        stats = defaultdict(int)
        hooks: list[tuple[str, AttemptOutcome]] = []
        store, state, backend = _make_store(
            stats=stats,
            on_attempt_finished=lambda uid, *, outcome: hooks.append((uid, outcome)),
        )
        job_snapshot = {"task_type": "t", "job_id": "1"}
        state.register_in_flight("t::1")

        with pytest.raises(_CommitCrashSignal, match="commit_job_success"):
            store.apply_success("t::1", {"score": 1}, job_snapshot=job_snapshot, attempt_id=None)

        assert "t::1" not in store.state.successes
        assert "t::1" not in backend.load_successes()
        assert "t::1" in store.state.queue_uids
        assert stats["succeeded"] == 0
        assert hooks == []


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

    def test_membership_predicates(self):
        """成员判定经 store.state 单一访问路径（六集合统一谓词）。"""
        store = self._probe_store()
        assert "w::1" in store.state.successes
        assert "f::1" in store.state.failed
        assert store.state.is_known("w::1")
        assert store.state.is_known("q::1")
        assert not store.state.is_known("unknown::1")
        assert store.state.cursors == {"c": "v"}

    def test_uid_index_liveness_contract(self):
        """uid 索引形态契约：in_flight 恒 frozenset，successes/failed/queue 为
        活索引（调用方需要快照时在边界显式 frozenset 冻结）。"""
        store = self._probe_store()
        assert isinstance(store.state.in_flight_uids, frozenset)
        assert isinstance(store.state.success_uids, set)
        assert isinstance(store.state.failed_uids, set)
        assert isinstance(store.state.queue_uids, set)

    def test_snapshot_isolation(self):
        """显式冻结的快照与活索引隔离（快照边界在调用方）。"""
        store = self._probe_store()
        snapshot = frozenset(store.state.queue_uids)
        store.state.pop_job(0)
        assert "q::1" in snapshot
        assert "q::1" not in store.state.queue_uids

    def test_set_state_and_backend_reset(self):
        store, _, _ = _make_store()
        fresh = PipelineState({}, {}, {}, [])
        store.set_state(fresh)
        assert store.state is fresh
        store.set_state(None)
        assert store.state.is_empty
        new_backend = InMemoryStateBackend()
        store.set_backend(new_backend)
        assert store.backend is new_backend


class TestFailureEntryShape:
    """FailureEntry 数据类形状（运维查询面 4e 的载荷契约）。"""

    def test_frozen_shape(self):
        entry = FailureEntry(
            job_uid="t::1", error_message="boom", metadata={"error_message": "boom"}, payload={"k": 1}
        )
        assert entry.job_uid == "t::1"
        assert entry.error_message == "boom"
        assert entry.metadata == {"error_message": "boom"}
        assert entry.payload == {"k": 1}
        with pytest.raises(Exception):
            entry.job_uid = "t::2"  # type: ignore[misc]

    def test_payload_defaults_none(self):
        entry = FailureEntry(job_uid="t::1", error_message="x", metadata={})
        assert entry.payload is None


# ── successes/failed 互斥性质（hypothesis 任意操作序列）─────────────────────

_UID_POOL = [
    "encode::1",
    "encode::2",
    "render::1",
    "render::2",
]


def _job_snapshot(uid: str) -> dict:
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
        for idx, jd in enumerate(self.store.state.queue):
            if f"{jd.get('task_type')}::{jd.get('job_id')}" == uid:
                self.store.state.pop_job(idx)
                return

    def apply(self, action: str, i: int) -> None:
        uid = _UID_POOL[i]
        if action == "enqueue":
            if self.store.state.is_known(uid) or uid in self.store.state.queue_uids:
                return
            self.store.state.spawn_jobs([_job_snapshot(uid)], front=False)
        elif action == "success":
            self._pop_from_memory_queue(uid)
            aid = _open_running_attempt(self.backend, uid)
            self.store.apply_success(uid, {"score": i}, attempt_id=aid)
        elif action == "failure":
            self._pop_from_memory_queue(uid)
            self.store.apply_failure(uid, {"error_message": "boom"}, job_snapshot=_job_snapshot(uid))
        elif action == "mark_failed":
            # mark_failed_memory 是失败登记的内存尾段：先完成后端提交再登记
            self._pop_from_memory_queue(uid)
            if self.backend.commit_job_failure(uid, {"error_message": "tail"}):
                self.store.mark_failed_memory(uid, {"error_message": "tail"})
        elif action == "retry":
            if uid in self.store.state.successes or uid in self.store.state.failed:
                return
            self._pop_from_memory_queue(uid)
            self.store.apply_retry(uid, _job_snapshot(uid), _job_snapshot(uid))
        elif action == "skip":
            self._pop_from_memory_queue(uid)
            self.store.apply_skip(uid)
        elif action == "bulk_failure":
            self._pop_from_memory_queue(uid)
            self.store.apply_bulk_failure([(uid, {"error_message": "deadlock"})])
        self.assert_mutex()

    def assert_mutex(self) -> None:
        """核心不变式：successes ∩ failed = ∅（内存与后端镜像一致），终态不留在队列。"""
        mem_wall = set(self.store.state.success_uids)
        mem_failed = set(self.store.state.failed_uids)
        assert mem_wall & mem_failed == set(), f"内存互斥破坏: {mem_wall & mem_failed}"
        disk_wall = set(self.backend.load_successes())
        disk_failed = set(self.backend.load_failed())
        assert disk_wall & disk_failed == set(), f"后端互斥破坏: {disk_wall & disk_failed}"
        assert mem_wall == disk_wall, f"wall 镜像漂移: {mem_wall} vs {disk_wall}"
        assert mem_failed == disk_failed, f"failed 镜像漂移: {mem_failed} vs {disk_failed}"
        assert mem_wall | mem_failed <= set(_UID_POOL)
        for uid in mem_wall | mem_failed:
            assert uid not in self.store.state.queue_uids, f"终态 {uid} 残留在队列"


@pytest.mark.hypothesis
@settings(max_examples=40, deadline=None)
@example(ops=[])
@example(ops=[("success", 0), ("failure", 0), ("success", 0)])
@example(ops=[("failure", 1), ("enqueue", 1), ("retry", 1)])
@given(ops=op_strategy)
def test_wall_failed_mutex_holds_for_arbitrary_operation_sequences(ops):
    """任意操作序列后（及每一步后），successes/failed 全局互斥且镜像一致。"""
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

    probe.store.apply_failure(uid, {"error_message": "boom"}, job_snapshot=_job_snapshot(uid))
    assert uid in probe.store.state.failed
    assert uid not in probe.store.state.successes

    aid = _open_running_attempt(probe.backend, uid)
    probe.store.apply_success(uid, {"score": 1}, attempt_id=aid)
    assert uid in probe.store.state.successes
    assert uid not in probe.store.state.failed

    probe.store.apply_failure(uid, {"error_message": "again"}, job_snapshot=_job_snapshot(uid))
    assert uid in probe.store.state.failed
    assert uid not in probe.store.state.successes
    probe.assert_mutex()


class TestRecordAttemptSeam:
    """StateStore 对 attempt 轨迹写入的统一持久化接缝测试。"""

    def test_record_attempt_dispatch_success_and_propagates_failure(self):
        backend = InMemoryStateBackend()
        store = StateStore(backend)
        job = Job("fetch", "1")

        # 正常记录 dispatch
        attempt_id = store.record_attempt_dispatch(
            job, execution_id="run1.1", run_id="run1"
        )
        assert attempt_id is not None
        records = backend.load_attempts("fetch::1")
        assert len(records) == 1
        assert records[0].outcome == "running"
        assert records[0].execution_id == "run1.1"

        # run_id 为空时不写轨迹
        assert store.record_attempt_dispatch(job, execution_id="x", run_id=None) is None

        # 开行写入失败时向上传播异常（不静默返回 None）
        def fail_append(_record):
            raise RuntimeError("append_attempt failed")

        backend.append_attempt = fail_append  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="append_attempt failed"):
            store.record_attempt_dispatch(job, execution_id="run1.2", run_id="run1")

    def test_record_attempt_skip_success_and_degraded(self):
        backend = InMemoryStateBackend()
        store = StateStore(backend)
        job = Job("skip_type", "2")

        # 正常记录 skip
        store.record_attempt_skip(job, "SKIPPED", run_id="run1", dispatch_no=2)
        records = backend.load_attempts("skip_type::2")
        assert len(records) == 1
        assert records[0].outcome == "skipped"
        assert records[0].error_message == "SKIPPED"
        assert records[0].execution_id == "run1.skip.2"

        # run_id 为空时不写轨迹
        store.record_attempt_skip(job, "SKIPPED", run_id=None, dispatch_no=3)
        assert len(backend.load_attempts("skip_type::2")) == 1


def test_success_metadata_preserves_user_keys_and_overwrites_framework_keys():
    store, state, backend = _make_store()
    metadata = {"run_count": 73, "last_run_at": "业务时间", "last_run_id": "业务身份",
                "success_count": 99, "last_succeeded_at": "自定义时间",
                "last_success_run_id": "自定义身份", "nested": {"error": "业务"}}
    state.register_in_flight("t::j")
    aid = _open_running_attempt(backend, "t::j", execution_id="actual.1", run_id="actual")
    store.apply_success(
        "t::j",
        metadata,
        run_id="actual",
        attempt_id=aid,
        execution_id="actual.1",
        finished_at="2026-05-01T00:00:09+00:00",
    )
    stored = backend.load_successes()["t::j"]
    assert stored == {
        **metadata,
        "artifacts": {},
        "success_count": 1,
        "last_succeeded_at": "2026-05-01T00:00:09+00:00",
        "last_success_run_id": "actual",
    }
    (rec,) = backend.load_attempts("t::j")
    assert rec.outcome == "succeeded"
    assert rec.finished_at == "2026-05-01T00:00:09+00:00"
    metadata["nested"]["error"] = "changed"
    assert backend.load_successes()["t::j"]["nested"] == {"error": "业务"}


def test_failure_normalization_keeps_user_keys_and_unknown_attempt_sentinel():
    from tasklite.engine.errorclass import ErrorClassifier

    metadata = {"error_message": ERR_JOB_DEPENDENCY, "error": "业务旧键", "fatal": "业务",
                "_attempt": 7, "nested": {"error_type": "业务分类", "fatal": False}}
    normalized = ErrorClassifier().normalize_failure_metadata(metadata)
    assert normalized["error_category"] == "dependency"
    assert normalized["attempt_no"] == 0
    assert normalized["error"] == "业务旧键"
    assert normalized["fatal"] == "业务"
    assert normalized["_attempt"] == 7
    assert normalized["nested"] == metadata["nested"]
    assert "error_category" not in metadata
    assert "attempt_no" not in metadata


@pytest.mark.parametrize("backend_kind", ["memory", "sqlite"])
def test_artifact_manifest_replaces_as_part_of_success_transaction(tmp_path, backend_kind):
    from tasklite.backend import SQLiteStateBackend
    from tasklite.models.artifact import ArtifactDeclaration

    backend = InMemoryStateBackend() if backend_kind == "memory" else SQLiteStateBackend(tmp_path / "state.db")
    state = PipelineState({}, {}, {}, [])
    store = StateStore(backend, state)
    uid = "t::artifacts"
    declaration = ArtifactDeclaration(str(tmp_path / "report"), name="report")
    anonymous = ArtifactDeclaration(str(tmp_path / "private"))
    old = {"artifacts": {"old": {"path": str(tmp_path / "old")}}}
    seed_aid = _open_running_attempt(backend, uid, execution_id="seed.1")
    assert backend.commit_job_success(uid, old, attempt_id=seed_aid, finished_at="2026-01-01T00:00:01+00:00")
    state.mark_success(uid, old)
    aid = _open_running_attempt(backend, uid)
    store.apply_success(
        uid, {"artifacts": {"forged": {"path": "/fake"}}, "payload": {"artifacts": "business"}},
        declared_artifacts=[declaration, anonymous], attempt_id=aid,
    )
    expected = {"report": {"path": declaration.path}}
    assert state.successes[uid]["artifacts"] == expected
    assert backend.load_successes()[uid]["artifacts"] == expected
    assert state.successes[uid]["payload"] == {"artifacts": "business"}
    aid = _open_running_attempt(backend, uid, execution_id="run_abc.2")
    store.apply_success(uid, {"artifacts": expected}, attempt_id=aid)
    assert state.successes[uid]["artifacts"] == {}
    assert backend.load_successes()[uid]["artifacts"] == {}


def test_artifact_manifest_does_not_change_before_success_commit(tmp_path):
    from tasklite.models.artifact import ArtifactDeclaration

    store, state, backend = _make_store()
    old = {"artifacts": {"old": {"path": str(tmp_path / "old")}}}
    seed_aid = _open_running_attempt(backend, execution_id="seed.1")
    assert backend.commit_job_success("t::1", old, attempt_id=seed_aid, finished_at="2026-01-01T00:00:01+00:00")
    state.mark_success("t::1", old)
    store.set_backend(CommitFailureBackend(backend, failing=["commit_job_success"]))
    aid = _open_running_attempt(backend)
    with pytest.raises(_CommitCrashSignal):
        store.apply_success(
            "t::1", {}, declared_artifacts=[ArtifactDeclaration(str(tmp_path / "new"), name="new")],
            attempt_id=aid,
        )
    assert state.successes["t::1"] == old
    assert backend.load_successes()["t::1"] == old


def test_sqlite_artifact_publication_rolls_back_with_cursor_transaction(tmp_path):
    import sqlite3
    from tasklite.backend import SQLiteStateBackend
    from tasklite.models.artifact import ArtifactDeclaration

    db_path = tmp_path / "state.db"
    backend = SQLiteStateBackend(db_path)
    old = {"artifacts": {"old": {"path": str(tmp_path / "old")}}}
    seed_aid = _open_running_attempt(backend, execution_id="seed.1")
    assert backend.commit_job_success("t::1", old, attempt_id=seed_aid, finished_at="2026-01-01T00:00:01+00:00")
    state = PipelineState(backend.load_successes(), {}, {}, [])
    store = StateStore(backend, state)
    aid = _open_running_attempt(backend)
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TRIGGER reject_cursor BEFORE INSERT ON cursors BEGIN SELECT RAISE(ABORT, 'cursor unavailable'); END")
    with pytest.raises(_CommitCrashSignal):
        store.apply_success(
            "t::1", {}, declared_artifacts=[ArtifactDeclaration(str(tmp_path / "new"), name="new")],
            attempt_id=aid, cursor_updates={"batch": "2"},
            spawned_job_snapshots=[Job("child", "1").to_dict()],
        )
    assert backend.load_successes()["t::1"] == old
    assert state.successes["t::1"] == old
    assert backend.load_cursors() == {}
    assert backend.load_queue() == []
