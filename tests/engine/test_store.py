"""StateStore 当前结算、摄入与内存／持久化互斥测试。"""

from __future__ import annotations

import pytest

from tasklite import TaskLiteError
from hypothesis import example, given, settings
from hypothesis import strategies as st

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import TriggerRegistry
from tasklite.engine.store import (
    BulkFailureOutcome,
    FailureOutcome,
    RetryOutcome,
    SkipOutcome,
    StateStore,
    SuccessOutcome,
)
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
    """批量失败及候选移除的真实异常注入。"""

    def __init__(self, real_backend, *, failing=()):
        self._real = real_backend
        self._failing = frozenset(failing)

    def commit_job_failures(self, *a, **k):
        if "commit_job_failures" in self._failing:
            raise OSError("批量事务写入失败")
        return self._real.commit_job_failures(*a, **k)

    def commit_skip(self, *a, **k):
        if "commit_skip" in self._failing:
            raise OSError("候选移除事务失败")
        return self._real.commit_skip(*a, **k)

    def __getattr__(self, name):
        return getattr(self._real, name)


def _make_store(**kwargs) -> tuple[StateStore, PipelineState, InMemoryStateBackend]:
    backend = InMemoryStateBackend()
    state = PipelineState({}, {}, {}, [])
    store = StateStore(backend, state, **kwargs)
    return store, state, backend


def _start_execution(backend, uid="t::1", execution_id=None, run_id="run_abc"):
    task_type, job_id = uid.split("::", 1)
    backend.enqueue_jobs([Job(task_type, job_id).to_dict()])
    if execution_id is None:
        execution_id = f"{run_id}.{backend.load_consistent_snapshot()['job_sequences'].get(uid, 0) + 1}"
    if not execution_id.startswith(run_id + "."):
        run_id = execution_id.rsplit(".", 1)[0]
    attempt_no = backend.mark_job_running(
        uid, execution_id=execution_id, run_id=run_id,
        started_at="2026-01-01T00:00:00Z",
    )
    return {"execution_id": execution_id, "run_id": run_id, "attempt_no": attempt_no}


class TestApplySuccess:
    """apply_success 原子终态转移。"""

    def test_basic_wall_meta(self):
        store, state, backend = _make_store()
        state.register_in_flight("process::1")
        aid = _start_execution(backend, "process::1")

        outcome = store.apply_success(
            "process::1",
            result={"message": None, "exception": None, "metadata": {"score": 100}, "annotations": {}},
            declared_inputs=["/tmp/a.txt"],
            **aid,
        )

        assert isinstance(outcome, SuccessOutcome)
        assert outcome.job_uid == "process::1"
        assert store.state.successes["process::1"]["result"]["metadata"]["score"] == 100
        assert store.state.successes["process::1"]["last_success_run_id"] == "run_abc"
        assert store.state.successes["process::1"]["success_count"] == 1
        assert "last_succeeded_at" in store.state.successes["process::1"]
        assert store.state.successes["process::1"]["inputs"] == ["/tmp/a.txt"]
        assert "process::1" not in store.state.in_flight_uids
        (rec,) = [a for a in backend.load_consistent_snapshot()["attempts"] if a["job_uid"] == "process::1"]
        assert rec["outcome"] == "success"
        assert rec["settled_at"] == store.state.successes["process::1"]["last_succeeded_at"]

    def test_run_count_continues_from_opposite_terminal(self):
        """重跑成功从失败档案对侧终态续数（run_count 跨终态递增）。"""
        store, state, backend = _make_store()
        state.register_in_flight("t::1")
        store.apply_failure("t::1", {"error_message": "boom", "success_count": 4})
        state.register_in_flight("t::1")
        aid = _start_execution(backend, "t::1")
        store.apply_success("t::1", result={"message": None, "exception": None, "metadata": {}, "annotations": {}}, **aid)
        assert store.state.successes["t::1"]["success_count"] == 5
        assert "t::1" not in store.state.failed

    def test_spawn_and_cursor_updates(self):
        store, _, backend = _make_store()
        store.state.register_in_flight("root::1")
        aid = _start_execution(backend, "root::1")
        spawned = [
            {"task_type": "child", "job_id": "c1"},
            {"task_type": "child", "job_id": "c2"},
        ]

        outcome = store.apply_success(
            "root::1",
            result={"message": None, "exception": None, "metadata": {}, "annotations": {}},
            spawned_job_snapshots=spawned,
            cursor_updates={"batch_pos": "100", "old_cursor": None},
            **aid,
        )

        assert outcome.spawned_uids == ["child::c1", "child::c2"]
        assert store.state.cursors["batch_pos"] == "100"
        assert "old_cursor" not in store.state.cursors
        assert [jd["job_id"] for jd in store.state.queue] == ["c1", "c2"]

    def test_declared_inputs_deduped_preserving_order(self):
        store, _, backend = _make_store()
        aid = _start_execution(backend, "t::1")
        store.apply_success(
            "t::1",
            result={"message": None, "exception": None, "metadata": {"inputs": ["/user/spoofed"]}, "annotations": {}},
            declared_inputs=["/b", "/a", "/b", "", 42, {"path": "/c"}],
            **aid,
        )
        assert store.state.successes["t::1"]["inputs"] == ["/b", "/a"]

    def test_user_inputs_key_stays_in_result_without_declarations(self):
        store, _, backend = _make_store()
        aid = _start_execution(backend, "t::1")
        store.apply_success("t::1", result={"message": None, "exception": None, "metadata": {"inputs": ["/user/spoofed"]}, "annotations": {}}, **aid)
        assert "inputs" not in store.state.successes["t::1"]
        assert store.state.successes["t::1"]["result"]["metadata"] == {"inputs": ["/user/spoofed"]}


class TestTrustedLastSucceededAtUs:
    """可信成功基线查询（trusted_last_succeeded_at_us）判定矩阵。"""


    def test_removed_success_then_seed_rejects_old_attempts_row(self):
        store, state, backend = _make_store()
        aid = _start_execution(backend, "t::1")
        store.apply_success("t::1", result={"message": None, "exception": None, "metadata": {}, "annotations": {}}, **aid, finished_at="2026-05-01T00:00:05+00:00")
        assert store.trusted_last_succeeded_at_us("t::1") is not None

        # 模拟清空 successes 历史后人工种子
        state.successes.pop("t::1", None)
        state.success_uids.discard("t::1")
        assert store.trusted_last_succeeded_at_us("t::1") is None
        state.mark_success("t::1", {})
        assert store.trusted_last_succeeded_at_us("t::1") is None


    def test_valid_success_returns_exact_integer_microseconds(self):
        store, _, backend = _make_store()
        aid = _start_execution(backend, "t::1")
        store.apply_success(
            "t::1",
            result={"message": None, "exception": None, "metadata": {}, "annotations": {}},
            # 执行身份来自实际 running 提交。
            **aid,
            finished_at="1970-01-01T00:00:01.123456+00:00",
        )
        assert store.trusted_last_succeeded_at_us("t::1") == 1_123_456


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
        assert store.state.failed["child::1"]["error_message"] == 'Dependency failed or has no record'
        assert store.state.failed["child::1"]["failed_dependency"] == "parent::1"
        assert "parent::1" not in store.state.in_flight_uids

    def test_failure_payload_snapshot_skipped_when_unserializable(self, caplog):
        """payload 快照不可序列化时降级 None，不阻断失败提交。"""
        store, _, backend = _make_store()
        job_snapshot = {
            "task_type": "t",
            "job_id": "1",
            "payload": {"bad": object()},
        }
        store.apply_failure("t::1", {"error_message": "x"}, job_snapshot=job_snapshot)
        assert "t::1" in store.state.failed
        assert "t::1" not in backend.load_consistent_snapshot()["failure_payloads"]

    def test_failure_clears_wall_on_both_sides(self):
        """失败终态与成功终态互删对侧（内存与后端镜像同步）。"""
        store, _, backend = _make_store()
        store.state.register_in_flight("t::1")
        aid = _start_execution(backend, "t::1")
        store.apply_success("t::1", result={"message": None, "exception": None, "metadata": {}, "annotations": {}}, **aid)
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

    def test_commit_path_requeues_front(self):
        store, state, _ = _make_store()
        state.replace_queue([])
        store.state.register_in_flight("t::1")

        identity = _start_execution(store.backend)
        outcome = store.apply_retry(
            "t::1",
            {"task_type": "t", "job_id": "1"},
            self._retry_job_snapshot(),
            **identity,
            error_message="transient blip",
        )

        assert isinstance(outcome, RetryOutcome)
        assert store.state.queue[0]["job_id"] == "1"
        assert "t::1" not in store.state.in_flight_uids


class TestApplySkip:
    """apply_skip 去重跳过。"""

    @pytest.mark.parametrize("dispatched", [False, True])
    def test_skip_clears_queue(self, dispatched):
        backend = InMemoryStateBackend()
        backend.enqueue_jobs([{"task_type": "t", "job_id": "1"}])
        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "1"}])
        store = StateStore(backend, state)
        if dispatched:
            state.pop_job(0)

        outcome = store.apply_skip("t::1", job_snapshot={"task_type": "t", "job_id": "1"})

        assert isinstance(outcome, SkipOutcome)
        assert outcome.was_known is True
        assert store.state.is_empty
        assert backend.load_queue_jobs(status="pending") == []

    def test_skip_preserves_other_in_flight_rerun_exemption(self):
        store, state, backend = _make_store()
        aid = _start_execution(backend, "t::running")
        store.apply_success("t::running", result={"message": None, "exception": None, "metadata": {}, "annotations": {}}, **aid)
        state.register_in_flight("t::running")
        pending = Job("t", "skip").to_dict()
        state.spawn_jobs([pending])
        backend.enqueue_jobs([pending])

        store.apply_skip("t::skip", job_snapshot=pending)

        assert state.is_empty
        assert state.in_flight_uids == {"t::running"}
        state._assert_state_consistent()

    def test_skip_fault_preserves_queue_without_archive(self):
        backend = InMemoryStateBackend()
        failing = CommitFailureBackend(backend, failing=("commit_skip",))
        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "1"}])
        store = StateStore(failing, state)
        with pytest.raises(OSError, match="候选移除事务失败"):
            store.apply_skip("t::1")
        assert "t::1" in state.queue_uids
        assert state.failed == {}


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
        state.mark_success("encode::a", {"prev": "ok"})
        store.mark_failed_memory("encode::a", {"error_message": "boom"})
        assert "encode::a" in state.failed_uids
        assert "encode::a" not in state.success_uids


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
                ("cycle::1", {"error_message": "Dependency cycle prevents execution"}),
                ("cycle::2", {"error_message": "Dependency cycle prevents execution"}),
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
        identity = _start_execution(backend, "cycle::1")
        store.apply_success("cycle::1", result={"message": None, "exception": None, "metadata": {}, "annotations": {}}, **identity)
        assert "cycle::1" in backend.load_successes()
        store.apply_bulk_failure([("cycle::1", {"error_message": "Dependency cycle prevents execution"})])
        assert "cycle::1" not in backend.load_successes()

    @pytest.mark.parametrize("operation", ["bulk", "cascade"])
    def test_bulk_fault_preserves_state_without_failure_accounting(self, operation):
        backend = InMemoryStateBackend()
        failing = CommitFailureBackend(backend, failing=("commit_job_failures",))
        job = Job("child", "one", depends_on=["parent::one"]).to_dict()
        state = PipelineState({}, {}, {}, [job])
        store = StateStore(failing, state)
        with pytest.raises(OSError, match="批量事务写入失败"):
            if operation == "bulk":
                store.apply_bulk_failure([("child::one", {"error_message": "deadlock"})])
            else:
                store.cascade_fail("parent::one")
        assert state.queue == [job]
        assert state.failed == {}


class TestIntakePipeline:
    """enqueue 摄入校验、Trigger 与资源配置。"""


    def test_enqueue_injects_discovery_trigger_for_unspecified(self):
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            tasks=_task_specs("scan", "always"),
        )
        jd = store.normalize_new_job_snapshot(Job("scan", "1"))
        assert jd["trigger"] == "always"
        jd_explicit = store.normalize_new_job_snapshot(Job("scan", "2", trigger="once"))
        assert jd_explicit["trigger"] == "once"

    def test_enqueue_rejects_unserializable_payload(self):
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            tasks=_task_specs("t", "once"),
        )
        job = Job("t", "1")
        job.payload["bad"] = object()
        with pytest.raises(TaskLiteError, match="payload.bad"):
            store.enqueue_job(job)

    def test_enqueue_job_syncs_memory_queue_with_backend(self):
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            tasks=_task_specs("t", "once"),
        )
        assert store.enqueue_job(Job("t", "first")) is True
        assert store.enqueue_job(Job("t", "first")) is False
        assert [row["job_id"] for row in store.state.queue] == ["first"]
        assert store.state.queue_uids == {"t::first"}
        assert [row["job"]["job_id"] for row in store.backend.load_queue_jobs(status="pending")] == ["first"]

    def test_enqueue_rejects_non_job(self):
        store, _, _ = _make_store()
        with pytest.raises(TaskLiteError):
            store.enqueue_job("not-a-job")  # type: ignore[arg-type]

    def test_enqueue_rejects_unregistered_task(self):
        from tasklite import TaskLiteError

        backend = InMemoryStateBackend()
        store = StateStore(
            backend,
            PipelineState({}, {}, {}, []),
            tasks=TaskRegistry(),
        )
        job = Job("missing", "1")
        with pytest.raises(TaskLiteError, match="unknown task_type"):
            store.enqueue_job(job)
        assert backend.load_queue_jobs(status="pending") == []
        assert [a for a in backend.load_consistent_snapshot()["attempts"] if a["job_uid"] == job.uid] == []

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
            tasks=tasks,
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
            tasks=_task_specs("child", "until_success"),
        )
        jd = store.normalize_new_job_snapshot(Job("child", "c1"), caller="spawn")
        assert jd["trigger"] == "until_success"
        assert jd["resources"][WORKER_RESOURCE] == 1.0

    def test_normalize_spawned_job_rejects_unserializable(self):
        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            tasks=_task_specs("t", "once"),
        )
        job = Job("t", "1")
        job.payload["bad"] = object()
        with pytest.raises(TaskLiteError, match="payload.bad"):
            store.normalize_new_job_snapshot(job, caller="spawn")

    def test_normalize_spawned_job_rejects_unregistered_task(self):
        from tasklite import TaskLiteError

        store = StateStore(
            InMemoryStateBackend(),
            PipelineState({}, {}, {}, []),
            tasks=TaskRegistry(),
        )
        with pytest.raises(TaskLiteError, match="unknown task_type"):
            store.normalize_new_job_snapshot(Job("missing", "1"), caller="spawn")

    @pytest.mark.parametrize("backend_kind", ["memory", "sqlite"])
    def test_enqueue_handles_legacy_running_conflict_and_downstream_dependency(self, tmp_path, backend_kind):
        """遗留 running 作业参与入队幂等/冲突判定与下游依赖存在性校验（双后端回归）。"""
        from tasklite.backend import SQLiteStateBackend

        backend = (
            InMemoryStateBackend()
            if backend_kind == "memory"
            else SQLiteStateBackend(tmp_path / "legacy_running.db")
        )
        try:
            tasks = _task_specs("work", "once")
            store = StateStore(backend, PipelineState({}, {}, {}, []), tasks=tasks)
            upstream = Job("work", "a", payload={"v": 1})
            assert store.enqueue_job(upstream) is True
            backend.mark_job_running(
                upstream.uid,
                execution_id="crashed_run.1",
                run_id="crashed_run",
                started_at="2026-05-01T00:00:00Z",
            )

            # 1. 同 UID 配置一致仍幂等返回 False；配置不同必抛 TaskLiteError（不再漏判静默 False）
            assert store.enqueue_job(Job("work", "a", payload={"v": 1})) is False
            with pytest.raises(TaskLiteError, match="already queued with different effective configuration"):
                store.enqueue_job(Job("work", "a", payload={"v": 2}))

            # 2. 下游依赖遗留 running 上游允许入队（不再误拒为依赖不存在）
            downstream = Job("work", "b", depends_on=[upstream.uid])
            assert store.enqueue_job(downstream) is True
            rows = {row["job_uid"]: row["status"] for row in backend.load_queue_jobs()}
            assert rows == {"work::a": "running", "work::b": "pending"}
        finally:
            backend.close()


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
        """成员判定经 store.state 单一访问路径（各集合活索引一致）。"""
        store = self._probe_store()
        assert "w::1" in store.state.successes
        assert "f::1" in store.state.failed
        assert "q::1" in store.state.queue_uids
        assert "unknown::1" not in store.state.success_uids
        assert "unknown::1" not in store.state.failed_uids
        assert "unknown::1" not in store.state.queue_uids
        assert "unknown::1" not in store.state.in_flight_uids
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
            if (
                uid in self.store.state.success_uids
                or uid in self.store.state.failed_uids
                or uid in self.store.state.queue_uids
                or uid in self.store.state.in_flight_uids
            ):
                return
            self.store.state.spawn_jobs([_job_snapshot(uid)], front=False)
        elif action == "success":
            self._pop_from_memory_queue(uid)
            aid = _start_execution(self.backend, uid)
            self.store.apply_success(uid, result={"message": None, "exception": None, "metadata": {"score": i}, "annotations": {}}, **aid)
        elif action == "failure":
            self._pop_from_memory_queue(uid)
            self.store.apply_failure(uid, {"error_message": "boom"}, job_snapshot=_job_snapshot(uid))
        elif action == "mark_failed":
            # mark_failed_memory 是失败登记的内存尾段：先完成后端提交再登记
            self._pop_from_memory_queue(uid)
            self.backend.commit_job_failures([(uid, {"error_message": "tail"})])
            self.store.mark_failed_memory(uid, {"error_message": "tail"})
        elif action == "retry":
            if uid in self.store.state.successes or uid in self.store.state.failed:
                return
            self._pop_from_memory_queue(uid)
            self.store.apply_retry(uid, _job_snapshot(uid), _job_snapshot(uid), **_start_execution(self.backend, uid))
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

    aid = _start_execution(probe.backend, uid)
    probe.store.apply_success(uid, result={"message": None, "exception": None, "metadata": {"score": 1}, "annotations": {}}, **aid)
    assert uid in probe.store.state.successes
    assert uid not in probe.store.state.failed

    probe.store.apply_failure(uid, {"error_message": "again"}, job_snapshot=_job_snapshot(uid))
    assert uid in probe.store.state.failed
    assert uid not in probe.store.state.successes
    probe.assert_mutex()


def test_success_metadata_is_separate_from_framework_keys():
    store, state, backend = _make_store()
    metadata = {"run_count": 73, "last_run_at": "业务时间", "last_run_id": "业务身份",
                "success_count": 99, "last_succeeded_at": "自定义时间",
                "last_success_run_id": "自定义身份", "nested": {"error": "业务"}}
    state.register_in_flight("t::j")
    aid = _start_execution(backend, "t::j", execution_id="actual.1", run_id="actual")
    store.apply_success(
        "t::j",
        result={"message": None, "exception": None, "metadata": metadata, "annotations": {}},
        **aid,
        finished_at="2026-05-01T00:00:09+00:00",
    )
    stored = backend.load_successes()["t::j"]
    assert stored["result"]["metadata"] == metadata
    assert stored["artifacts"] == {}
    assert stored["success_count"] == 1
    assert stored["last_succeeded_at"] == "2026-05-01T00:00:09+00:00"
    assert stored["last_success_run_id"] == "actual"
    (rec,) = [a for a in backend.load_consistent_snapshot()["attempts"] if a["job_uid"] == "t::j"]
    assert rec["outcome"] == "success"
    assert rec["settled_at"] == "2026-05-01T00:00:09+00:00"
    metadata["nested"]["error"] = "changed"
    assert backend.load_successes()["t::j"]["result"]["metadata"]["nested"] == {"error": "业务"}


@pytest.mark.parametrize("backend_kind", ["memory", "sqlite"])
def test_artifact_manifest_replaces_as_part_of_success_transaction(tmp_path, backend_kind):
    from tasklite.backend import SQLiteStateBackend
    from tasklite.models.artifact import FileSpec

    backend = InMemoryStateBackend() if backend_kind == "memory" else SQLiteStateBackend(tmp_path / "state.db")
    state = PipelineState({}, {}, {}, [])
    store = StateStore(backend, state)
    uid = "t::artifacts"
    declaration = FileSpec(str(tmp_path / "report"), role="output")
    seed_aid = _start_execution(backend, uid, execution_id="seed.1")
    store.apply_success(
        uid, result={"message": None, "exception": None, "metadata": {}, "annotations": {}},
        declared_artifacts={"old": FileSpec(str(tmp_path / "old"), role="output")},
        **seed_aid,
    )
    assert state.successes[uid]["artifacts"] == backend.load_successes()[uid]["artifacts"]
    aid = _start_execution(backend, uid)
    store.apply_success(
        uid, result={"message": None, "exception": None, "metadata": {"artifacts": {"forged": {"path": "/fake"}}, "payload": {"artifacts": "business"}}, "annotations": {}},
        declared_artifacts={"report": declaration}, **aid,
    )
    expected = {"report": {"path": declaration.path}}
    assert state.successes[uid]["artifacts"] == expected
    assert backend.load_successes()[uid]["artifacts"] == expected
    assert state.successes[uid]["result"]["metadata"]["payload"] == {"artifacts": "business"}
    aid = _start_execution(backend, uid, execution_id="run_abc.2")
    store.apply_success(uid, result={"message": None, "exception": None, "metadata": {"artifacts": expected}, "annotations": {}}, **aid)
    assert state.successes[uid]["artifacts"] == {}
    assert backend.load_successes()[uid]["artifacts"] == {}


@pytest.mark.parametrize("metadata", [
    {},
    {"last_succeeded_at": "2026-05-01T00:00:05Z"},
    {"last_succeeded_at": "2026-05-01T00:00:05Z", "success_count": 0},
    {"last_succeeded_at": "2026-05-01T00:00:05Z", "success_count": True},
    {"last_succeeded_at": "corrupt", "success_count": 1},
    {"last_succeeded_at": "2026-05-01T00:00:05Z", "success_count": 1, "last_success_run_id": ""},
    {"last_succeeded_at": "2026-05-01T00:00:09Z", "success_count": 1, "last_success_run_id": "run_abc"},
])
def test_trusted_baseline_rejects_untrusted_or_mismatched_metadata(metadata):
    store, state, backend = _make_store()
    identity = _start_execution(backend)
    store.apply_success(
        "t::1", result={"message": None, "exception": None, "metadata": {}, "annotations": {}},
        finished_at="2026-05-01T00:00:05Z", **identity,
    )
    assert store.trusted_last_succeeded_at_us("t::1") is not None
    state.successes.pop("t::1", None)
    state.success_uids.discard("t::1")
    assert store.trusted_last_succeeded_at_us("t::1") is None
    state.mark_success("t::1", metadata)
    assert store.trusted_last_succeeded_at_us("t::1") is None


@pytest.mark.parametrize("backend_kind", ["memory", "sqlite"])
def test_artifact_manifest_stays_committed_when_settlement_fails(tmp_path, monkeypatch, backend_kind):
    from tasklite.backend import SQLiteStateBackend
    from tasklite.models.artifact import FileSpec

    backend = InMemoryStateBackend() if backend_kind == "memory" else SQLiteStateBackend(tmp_path / "state.db")
    store = StateStore(backend)
    result = {"message": None, "exception": None, "metadata": {}, "annotations": {}}
    old = FileSpec(str(tmp_path / "old"), role="output")
    store.apply_success("t::1", result=result, declared_artifacts={"old": old}, **_start_execution(backend))
    before = backend.load_successes()
    identity = _start_execution(backend)
    error = OSError("结算不可写")
    def fail(**kwargs):
        raise error
    monkeypatch.setattr(backend, "commit_job_settlement", fail)
    with pytest.raises(OSError) as caught:
        store.apply_success(
            "t::1", result=result,
            declared_artifacts={"new": FileSpec(str(tmp_path / "new"), role="output")},
            **identity,
        )
    assert caught.value is error
    assert store.state.successes == backend.load_successes() == before
    assert len([a for a in backend.load_consistent_snapshot()["attempts"] if a["job_uid"] == "t::1"]) == 1
    assert backend.load_queue_jobs()[0]["status"] == "running"
    backend.close()


def test_sqlite_artifact_publication_rolls_back_with_cursor_transaction(tmp_path):
    import sqlite3
    from tasklite.backend import SQLiteStateBackend
    from tasklite.models.artifact import FileSpec

    backend = SQLiteStateBackend(tmp_path / "state.db")
    store = StateStore(backend)
    result = {"message": None, "exception": None, "metadata": {}, "annotations": {}}
    store.apply_success(
        "t::1", result=result,
        declared_artifacts={"old": FileSpec(str(tmp_path / "old"), role="output")},
        **_start_execution(backend),
    )
    before = backend.load_successes()
    identity = _start_execution(backend)
    with backend._get_conn() as connection:
        connection.execute("CREATE TRIGGER reject_cursor BEFORE INSERT ON cursors BEGIN SELECT RAISE(ABORT, 'cursor unavailable'); END")
    with pytest.raises(sqlite3.IntegrityError, match="cursor unavailable"):
        store.apply_success(
            "t::1", result=result,
            declared_artifacts={"new": FileSpec(str(tmp_path / "new"), role="output")},
            cursor_updates={"batch": "2"}, spawned_job_snapshots=[Job("child", "1").to_dict()],
            **identity,
        )
    assert store.state.successes == backend.load_successes() == before
    snap = backend.load_consistent_snapshot()
    assert snap["cursors"] == {}
    assert len([a for a in snap["attempts"] if a["job_uid"] == "t::1"]) == 1
    (running,) = backend.load_queue_jobs()
    assert running["job_uid"] == "t::1" and running["status"] == "running"
    backend.close()


def _store_test_ok_handler(job, ctx):
    from tasklite import TaskResult
    return TaskResult.success()


@pytest.mark.parametrize("backend_kind", ["memory", "sqlite"])
def test_pipeline_enqueue_against_legacy_running_and_run_recovery(tmp_path, backend_kind):
    """端到端双后端回归：库内存在遗留 running 时，同 UID 冲突拦截、下游允许入队且后续 run() 按 DAG 收敛。"""
    from tasklite import StateSnapshot, TaskLite

    kwargs = {"state_path": tmp_path / "legacy_run.db"} if backend_kind == "sqlite" else {}
    with TaskLite(backend=backend_kind, **kwargs) as pipeline:
        pipeline.register_task("work", _store_test_ok_handler)
        assert pipeline.enqueue(Job("work", "a", payload={"v": 1})) is True
        pipeline._runtime.store.backend.mark_job_running(
            "work::a",
            execution_id="crashed.1",
            run_id="crashed",
            started_at="2026-05-01T00:00:00Z",
        )

        # 同 UID 同配置幂等返回 False；不同配置抛出 TaskLiteError
        assert pipeline.enqueue(Job("work", "a", payload={"v": 1})) is False
        with pytest.raises(TaskLiteError, match="already queued with different effective configuration"):
            pipeline.enqueue(Job("work", "a", payload={"v": 2}))

        # 下游依赖遗留 running 允许入队
        assert pipeline.enqueue(Job("work", "b", depends_on=["work::a"])) is True

        # run() 恢复遗留 running 为失败，随后下游按依赖失败收敛
        pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as snap:
            a_rec = snap.get_job("work::a")
            b_rec = snap.get_job("work::b")
            assert a_rec is not None and a_rec["status"] == "failed"
            assert b_rec is not None and b_rec["status"] == "failed"
            assert list(snap.iter_jobs(status="pending")) == []


def test_manual_failure_archive_omits_redundant_root_note_and_reads_legacy_root_note():
    """人工失败档案仅在 annotations 记录 note（根部不再多写），读路径仍向后兼容旧库根部 note。"""
    from tasklite import StateSnapshot, TaskLite, TaskResult
    from tasklite.models.records import extract_archive_result

    def _ok(job, ctx):
        return TaskResult.success()

    with TaskLite(backend="memory") as pipeline:
        pipeline.register_task("work", _ok)
        with pipeline.edit(dry_run=False, note="人工标记失败") as tx:
            tx.add(Job("work", "f1"), status="failed")

        raw_meta = pipeline._runtime.store.backend.load_failed()["work::f1"]
        assert "note" not in raw_meta
        assert raw_meta["annotations"]["note"] == "人工标记失败"
        assert raw_meta["result"]["annotations"]["note"] == "人工标记失败"

        with StateSnapshot.open(pipeline) as snap:
            rec = snap.get_job("work::f1")
            assert rec is not None
            assert rec["result"]["annotations"]["note"] == "人工标记失败"

    # 读路径向后兼容存量 Schema 6 根部 note 字段
    legacy_result = extract_archive_result(
        {"error_message": "legacy fail", "note": "旧库说明"},
        is_failure=True,
    )
    assert legacy_result is not None
    assert legacy_result["annotations"]["note"] == "旧库说明"
