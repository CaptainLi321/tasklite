"""状态与执行结算集成回归测试套件。

验证 ADR-0011 核心验收契约：
1. running 提交前/后、结果事务中/后的崩溃窗口有确定性覆盖；恢复不重复生成记录。
2. 父成功与派生/游标共同接受，非法子项不允许父成功；失败/retry 丢弃暂存效果。
3. 每次尝试含零预算重试推进 attempt_no，跨 Run 不重用；手工状态不制造 Attempt。
4. 启动时拒绝同一次执行既处于 running 又已结算。
"""
from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import os
import sqlite3
import threading
import time
import pytest

from tasklite import FileSpec, Job, StateSnapshot, TaskLite, TaskResult
from tasklite.backend.base import QUEUE_EXECUTION_ID_KEY
from tasklite.resource import CapacityResource
from tasklite.backend.sqlite_backend import SQLiteStateBackend, TARGET_SCHEMA_VERSION


@pytest.mark.parametrize("outcome", ["success", "failure", "retry", "pending"])
def test_startup_invariant_conflict_raises_runtime_error(tmp_path: Path, outcome):
    """同一次执行不能同时处于 running 与已结算。"""
    db_path = tmp_path / "conflict_state.db"
    backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
    backend.enqueue_jobs([{"task_type": "t", "job_id": "1"}])
    backend.mark_job_running(
        "t::1",
        execution_id="run_old.1",
        run_id="run_old",
        started_at="2026-01-01T00:00:00+00:00",
    )
    backend.commit_job_settlement(
        job_uid="t::1",
        outcome=outcome,
        attempt_no=1,
        execution_id="run_old.1",
        run_id="run_old",
        settled_at="2026-01-01T00:00:01+00:00",
        success_metadata={"score": 100},
        failure_metadata={"error_message": "failed"},
    )
    with backend._get_conn() as conn:
        conn.execute("DELETE FROM queue WHERE job_uid = 't::1'")
        conn.execute(
            "INSERT INTO queue (job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at) "
            "VALUES ('t::1', 0, '{\"task_type\":\"t\",\"job_id\":\"1\"}', 'running', 'run_old', 1, '2026-01-01T00:00:00+00:00')"
        )

    # 排在冲突记录前的合法遗留执行也必须保持原样。
    backend.enqueue_jobs([{"task_type": "t", "job_id": "orphan"}], front=True)
    backend.mark_job_running(
        "t::orphan",
        execution_id="run_old.2",
        run_id="run_old",
        started_at="2026-01-01T00:00:00Z",
    )
    assert_startup_conflict_preserves_database(db_path)


def assert_startup_conflict_preserves_database(db_path):
    with closing(sqlite3.connect(db_path)) as observer:
        before = list(observer.iterdump())
        with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
            with pytest.raises(RuntimeError, match="Invariant conflict"):
                pipeline.run()
        assert list(observer.iterdump()) == before


@pytest.mark.parametrize(
    "corruption",
    [
        "UPDATE job_sequences SET last_attempt_no = 0 WHERE job_uid = 't::one'",
        '''UPDATE queue SET job_snapshot = '{"task_type":"t","job_id":"other"}' WHERE job_uid = 't::one' ''',
    ],
    ids=["sequence", "snapshot-uid"],
)
def test_startup_rejects_running_identity_conflict(tmp_path, corruption):
    db_path = tmp_path / "state.db"
    backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
    backend.enqueue_jobs([Job("t", "one").to_dict()])
    backend.mark_job_running(
        "t::one",
        execution_id="old.1",
        run_id="old",
        started_at="2026-01-01T00:00:00Z",
    )
    with backend._get_conn() as conn:
        conn.execute(corruption)
    assert_startup_conflict_preserves_database(db_path)


def test_startup_rejects_running_execution_id_mismatch(tmp_path):
    db_path = tmp_path / "state.db"
    backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
    backend.enqueue_jobs([Job("t", "one").to_dict()])
    backend.mark_job_running(
        "t::one",
        execution_id="old.1",
        run_id="old",
        started_at="2026-01-01T00:00:00Z",
    )
    with backend._get_conn() as conn:
        raw = conn.execute(
            "SELECT job_snapshot FROM queue WHERE job_uid = 't::one'"
        ).fetchone()[0]
        stored = json.loads(raw)
        stored[QUEUE_EXECUTION_ID_KEY] = "other.1"
        conn.execute(
            "UPDATE queue SET job_snapshot = ? WHERE job_uid = 't::one'",
            (json.dumps(stored),),
        )

    assert_startup_conflict_preserves_database(db_path)


def test_startup_terminal_overlap_is_a_conflict(tmp_path):
    db_path = tmp_path / "overlap.db"
    backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
    backend.apply_manual_status([
        ("t::overlap", "succeeded", Job("t", "overlap").to_dict(), None),
    ])
    backend.append_failed("t::overlap", {"error_message": "conflict"})
    assert_startup_conflict_preserves_database(db_path)


def transaction_effect_handler(job, ctx):
    with Path(job.payload["effect"]).open("a") as stream:
        stream.write(f"{os.getpid()}\n")
    ctx.spawn(Job("child", "unpublished"))
    ctx.set_cursor("offset", "unpublished")
    return TaskResult.success()


@pytest.mark.parametrize("old_outcome", ["success", "failure"])
def test_recovery_replaces_previous_terminal_without_replaying(tmp_path, old_outcome):
    db_path = tmp_path / "state.db"
    backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
    uid = "work::one"
    first = Job("work", "one").to_dict()
    backend.enqueue_jobs([first])
    backend.mark_job_running(
        uid,
        execution_id="first.1",
        run_id="first",
        started_at="2026-01-01T00:00:00Z",
    )
    assert backend.commit_job_settlement(
        job_uid=uid, outcome=old_outcome, attempt_no=1,
        execution_id="first.1", run_id="first",
        settled_at="2026-01-01T00:00:01Z", job_snapshot=first,
        success_metadata={"job_snapshot": first}, failure_metadata={"job_snapshot": first},
    )
    backend.set_cursor("offset", "original")
    history = backend.load_settled_attempts(uid)
    effect = tmp_path / "worker.txt"

    def register(pipeline):
        pipeline.register_task("work", transaction_effect_handler, trigger="always")
        pipeline.register_task("child", child_consumer_handler)

    with closing(sqlite3.connect(db_path)) as observer:
        observer.execute(
            "CREATE TRIGGER reject_cursor BEFORE INSERT ON cursors "
            "BEGIN SELECT RAISE(ABORT, 'synthetic settlement failure'); END"
        )
        with TaskLite(backend="sqlite", state_path=db_path, max_workers=1) as pipeline:
            register(pipeline)
            pipeline.enqueue(Job("work", "one", payload={"effect": str(effect)}))
            pipeline.enqueue(Job("child", "dependent", depends_on=[uid]))
            pipeline.enqueue(Job("child", "independent"))
            current = backend.load_queue_jobs()[0]["job"]
            with pytest.raises(RuntimeError, match="synthetic settlement failure") as error:
                pipeline.run()
            assert isinstance(error.value.__cause__, sqlite3.IntegrityError)

        worker_pid = int(effect.read_text().strip())
        assert worker_pid != os.getpid()
        queue = backend.load_queue_jobs()
        running = queue[0]
        assert (running["job_uid"], running["status"], running["attempt_no"]) == (uid, "running", 2)
        assert running["job"] == current
        assert [(row["job_uid"], row["status"]) for row in queue[1:]] == [
            ("child::dependent", "pending"), ("child::independent", "pending"),
        ]
        assert backend.load_settled_attempts(uid) == history
        assert (uid in backend.load_successes()) == (old_outcome == "success")
        assert (uid in backend.load_failed()) == (old_outcome == "failure")
        assert backend.load_cursors() == {"offset": "original"}

        observer.execute("DROP TRIGGER reject_cursor")
        observer.execute(
            "CREATE TRIGGER reject_failure BEFORE INSERT ON failed "
            "BEGIN SELECT RAISE(ABORT, 'synthetic recovery failure'); END"
        )
        before = list(observer.iterdump())
        with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
            register(pipeline)
            with pytest.raises(RuntimeError, match="synthetic recovery failure") as error:
                pipeline.run()
            assert isinstance(error.value.__cause__, sqlite3.IntegrityError)
        assert list(observer.iterdump()) == before
        observer.execute("DROP TRIGGER reject_failure")

    for _ in range(2):
        with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
            register(pipeline)
            pipeline.run()

    attempts = backend.load_settled_attempts(uid)
    assert attempts[:1] == history
    assert [(a["run_id"], a["attempt_no"], a["outcome"]) for a in attempts[1:]] == [
        (running["run_id"], 2, "failure"),
    ]
    assert attempts[1]["job"] == current
    assert attempts[1]["started_at"] == running["started_at"]
    failures = backend.load_failed()
    assert failures[uid]["job_snapshot"] == current
    assert failures[uid]["reason"] == "unsettled_running_recovery"
    assert failures["child::dependent"]["failed_dependency"] == uid
    assert backend.load_settled_attempts("child::dependent") == []
    assert set(backend.load_successes()) == {"child::independent"}
    assert backend.load_cursors() == {"offset": "original"}
    assert effect.read_text() == f"{worker_pid}\n"


def test_leftover_running_recovery_and_idempotence(tmp_path: Path):
    """崩溃窗口：running 提交后未完成结算，下次启动单次收敛为 failed，幂等不重复记行。"""
    db_path = tmp_path / "recovery_state.db"
    backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
    backend.enqueue_jobs([{"task_type": "work", "job_id": "orphan"}])
    att_no = backend.mark_job_running(
        "work::orphan",
        execution_id="run_crash.1",
        run_id="run_crash",
        started_at="2026-01-01T00:00:00+00:00",
    )
    assert att_no == 1

    pipeline = TaskLite(backend="sqlite", state_path=db_path)
    # 启动时恢复
    pipeline.run()

    failed = pipeline._runtime.store.backend.load_failed()
    assert "work::orphan" in failed
    assert failed["work::orphan"]["error_message"] == "上次执行未完成结算，实际结果未知"
    assert failed["work::orphan"]["run_id"] == "run_crash"

    attempts = pipeline._runtime.store.backend.load_attempts("work::orphan")
    assert len(attempts) == 1
    assert attempts[0].outcome == "failed"
    assert attempts[0].attempt_no == 1
    assert attempts[0].run_id == "run_crash"
    assert pipeline._runtime.store.backend.load_queue() == []

    # 再次启动，幂等不产生重复 attempts
    pipeline2 = TaskLite(backend="sqlite", state_path=db_path)
    pipeline2.run()
    attempts2 = pipeline2._runtime.store.backend.load_attempts("work::orphan")
    assert len(attempts2) == 1


def parent_spawn_and_cursor_handler(job, ctx):
    ctx.set_cursor("offset", "100")
    ctx.spawn(Job("child", "c1", payload={"data": 1}))
    return TaskResult.success(metadata={"result": "parent_ok"})


def child_consumer_handler(job, ctx):
    return TaskResult.success(metadata={"data": job.payload.get("data")})


def test_parent_success_atomic_with_spawned_and_cursor(tmp_path: Path):
    """父作业成功与动态派生及游标同事务提交；子作业入队为 pending，游标已落盘。"""
    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "spawn_cursor.db")
    pipeline.register_task("parent", parent_spawn_and_cursor_handler)
    pipeline.register_task("child", child_consumer_handler)
    pipeline.enqueue(Job("parent", "p1"))
    pipeline.run()

    assert "parent::p1" in pipeline._runtime.store.backend.load_successes()
    assert "child::c1" in pipeline._runtime.store.backend.load_successes()
    assert pipeline._runtime.store.backend.load_cursors().get("offset") == "100"


class BadPayload:
    pass


def bad_parent_handler(job, ctx):
    ctx.set_cursor("offset", "999")
    ctx.spawn(Job("child", "bad", payload={"bad": BadPayload()}))
    return TaskResult.success(metadata={"ok": 1})


def test_invalid_spawned_job_rejects_parent_and_discards_effects(tmp_path: Path):
    """非法派生子作业导致父作业失败，所有暂存派生任务和游标更新整体丢弃。"""
    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "bad_spawn.db")
    pipeline.register_task("parent", bad_parent_handler)
    pipeline.enqueue(Job("parent", "p1"))
    pipeline.run()

    assert "parent::p1" not in pipeline._runtime.store.backend.load_successes()
    assert "parent::p1" in pipeline._runtime.store.backend.load_failed()
    assert "not JSON-serializable" in pipeline._runtime.store.backend.load_failed()["parent::p1"]["error_message"]
    assert "offset" not in pipeline._runtime.store.backend.load_cursors()
    assert pipeline._runtime.store.backend.load_queue() == []


def file_counter_retry_handler(job, ctx):
    counter_path = Path(job.payload["counter_file"])
    count = int(counter_path.read_text()) if counter_path.exists() else 0
    count += 1
    counter_path.write_text(str(count))
    if count == 1:
        # 零预算重试
        return TaskResult.retry(message="try again", consume_budget=False)
    return TaskResult.success(metadata={"runs": count})


def test_attempt_no_monotonic_advance_including_zero_budget_retry(tmp_path: Path):
    """attempt_no 单调递增：每次尝试（含零预算重试）推进编号，跨 Run 不重用。"""
    counter_file = tmp_path / "runs.txt"
    counter_file.write_text("0")

    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "monotonic.db")
    pipeline.register_task("worker", file_counter_retry_handler)
    pipeline.enqueue(Job("worker", "m1", payload={"counter_file": str(counter_file)}))
    pipeline.run()

    attempts = pipeline._runtime.store.backend.load_attempts("worker::m1")
    assert len(attempts) == 2
    assert [a.attempt_no for a in attempts] == [1, 2]
    assert [a.outcome for a in attempts] == ["requeued", "succeeded"]


@pytest.mark.parametrize("raises", [False, True])
def test_recovery_settlement_fault_preserves_running(tmp_path, monkeypatch, raises):
    """恢复提交失败时保留 running 与完整快照，不登记内存失败。"""
    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
    backend = pipeline._runtime.store.backend
    snapshot = {"task_type": "work", "job_id": "orphan", "payload": {"key": "value"}}
    backend.enqueue_jobs([snapshot])
    backend.mark_job_running(
        "work::orphan",
        execution_id="old.1",
        run_id="old",
        started_at="2026-01-01T00:00:00+00:00",
    )

    def fail_settlement(**kwargs):
        if raises:
            raise OSError("无法写入结算")
        return False

    monkeypatch.setattr(backend, "commit_job_settlement", fail_settlement)
    with pytest.raises(BaseException, match="无法写入结算|Failed to commit failure"):
        pipeline.run()
    assert backend.load_queue_jobs()[0]["status"] == "running"
    assert backend.load_queue_jobs()[0]["job"] == snapshot
    assert backend.load_failed() == {}
    assert backend.load_attempts("work::orphan") == []
    assert pipeline._runtime.store.state.failed == {}
    pipeline.close()


def unsettled_effect_handler(job, ctx):
    with Path(job.payload["effect"]).open("a") as stream:
        stream.write("effect\n")
    return TaskResult.success()


def pending_after_effect_handler(job, ctx):
    effect = Path(job.payload["effect"])
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        if effect.exists() and effect.read_text(encoding="utf-8") == "effect\n":
            return TaskResult.success()
        time.sleep(0.02)
    raise TimeoutError("work effect was not committed before pending settlement")


@pytest.mark.parametrize("raises", [False, True])
def test_uncommitted_execution_is_not_automatically_replayed(tmp_path, monkeypatch, raises):
    """结算写入故障保留 running，重开只恢复失败，不重复业务副作用。"""
    db_path = tmp_path / "state.db"
    effect = tmp_path / "effect.txt"
    pipeline = TaskLite(backend="sqlite", state_path=db_path)
    pipeline.register_task("work", unsettled_effect_handler)
    pipeline.register_task("pending", pending_after_effect_handler)
    pipeline.enqueue(Job("work", "one", payload={"effect": str(effect)}))
    pipeline.enqueue(Job("pending", "two", payload={"effect": str(effect)}))

    def fail_settlement(**kwargs):
        if raises:
            raise RuntimeError("结算失败")
        return False

    with monkeypatch.context() as patch:
        patch.setattr(
            pipeline._runtime.store.backend,
            "commit_job_settlement",
            fail_settlement,
        )
        with pytest.raises(BaseException, match="结算失败|commit_job_settlement returned False"):
            pipeline.run()
    rows = pipeline._runtime.store.backend.load_queue_jobs()
    assert rows[0]["status"] == "running"
    assert pipeline._runtime.store.backend.load_settled_attempts("work::one") == []
    assert effect.read_text() == "effect\n"
    pipeline.close()

    with TaskLite(backend="sqlite", state_path=db_path) as reopened:
        reopened.register_task("work", unsettled_effect_handler)
        reopened.register_task("pending", pending_after_effect_handler)
        reopened.run()
        attempts = reopened._runtime.store.backend.load_settled_attempts("work::one")
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failure"
        assert "work::one" in reopened._runtime.store.backend.load_failed()
        assert effect.read_text() == "effect\n"


def duplicate_spawn_handler(job, ctx):
    ctx.spawn(Job("child", "fresh"))
    child_files = None
    if "child_output" in job.payload:
        child_files = {
            "result": FileSpec(job.payload["child_output"], role="output"),
        }
    if job.payload["mode"] == "batch":
        ctx.spawn(Job("child", "same", payload={"version": 1}, files=child_files))
    ctx.spawn(Job(
        "child",
        "same",
        payload={"version": 2 if job.payload["conflict"] else 1},
        files=child_files,
    ))
    ctx.set_cursor("offset", "unpublished")
    return TaskResult.success()


@pytest.mark.parametrize("mode", ["batch", "pending"])
@pytest.mark.parametrize("conflict", [False, True])
def test_spawn_duplicate_configuration_checked_before_atomic_publication(tmp_path, mode, conflict):
    """同批或忙时重复必须比较有效配置，冲突拒绝父成功及全部暂存效果。"""
    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db", max_workers=1) as p:
        p.register_task("parent", duplicate_spawn_handler)
        p.register_task("child", child_consumer_handler)
        p.enqueue(Job("parent", "one", payload={"mode": mode, "conflict": conflict}))
        if mode == "pending":
            p.enqueue(Job("child", "same", payload={"version": 1}))
        p.run()
        successes = p._runtime.store.backend.load_successes()
        if conflict:
            assert "parent::one" in p._runtime.store.backend.load_failed()
            assert "configuration" in p._runtime.store.backend.load_failed()["parent::one"]["error_message"]
            assert "child::fresh" not in successes
            assert p.get_cursor("offset") is None
            assert ("child::same" in successes) == (mode == "pending")
        else:
            assert {"parent::one", "child::same", "child::fresh"} <= successes.keys()
            assert len(p._runtime.store.backend.load_settled_attempts("child::same")) == 1
            assert p.get_cursor("offset") == "unpublished"


@pytest.mark.parametrize("template_changed", [False, True])
def test_spawn_uses_persisted_effective_configuration(tmp_path, template_changed):
    db_path = tmp_path / "state.db"
    child_output = tmp_path / "child-output.txt"
    original_payload = {"version": 1}
    original_files = {"result": FileSpec(child_output, role="output")}
    expected_files = {
        name: spec.to_dict()
        for name, spec in original_files.items()
    }
    with TaskLite(backend="sqlite", state_path=db_path, max_workers=1) as pipeline:
        pipeline.register_task("parent", duplicate_spawn_handler)
        pipeline.register_task("child", child_consumer_handler)
        pipeline.enqueue(Job(
            "parent",
            "one",
            payload={
                "mode": "pending",
                "conflict": False,
                "child_output": str(child_output),
            },
        ))
        pipeline.enqueue(Job(
            "child",
            "same",
            payload=original_payload,
            files=original_files,
        ))
        pipeline.set_cursor("offset", "original")

    with TaskLite(backend="sqlite", state_path=db_path, max_workers=1) as pipeline:
        pipeline.register_resource(CapacityResource("disk", max_capacity=1))
        pipeline.register_task(
            "child", child_consumer_handler, resources={"disk": 1} if template_changed else {},
        )
        pipeline.register_task("parent", duplicate_spawn_handler)
        pipeline.run()
        with StateSnapshot.open(pipeline) as state:
            attempts = list(state.iter_attempts(job_uid="parent::one"))
            assert len(attempts) == 1
            same = list(state.iter_attempts(job_uid="child::same"))
            assert len(same) == 1
            assert "disk" not in same[0]["job"]["resources"]
            assert same[0]["job"]["payload"] == original_payload
            assert same[0]["job"]["files"] == expected_files
            assert same[0]["files"] == expected_files
            if template_changed:
                assert attempts[0]["outcome"] == "failure"
                assert state.get_job("child::fresh") is None
                assert list(state.iter_attempts(job_uid="child::fresh")) == []
                assert pipeline.get_cursor("offset") == "original"
            else:
                assert attempts[0]["outcome"] == "success"
                fresh = state.get_job("child::fresh")
                assert fresh is not None and fresh["status"] == "succeeded"
                assert pipeline.get_cursor("offset") == "unpublished"


def unknown_spawn_handler(job, ctx):
    ctx.spawn(Job("child", "sibling", payload={"version": 1}))
    ctx.spawn(Job("missing", "unknown"))
    ctx.set_cursor("offset", "unpublished")
    return TaskResult.success()


def test_spawn_unregistered_task_fails_parent_atomically_and_survives_reopen(tmp_path):
    db_path = tmp_path / "state.db"
    with TaskLite(backend="sqlite", state_path=db_path, max_workers=1) as pipeline:
        pipeline.register_task("parent", unknown_spawn_handler)
        pipeline.register_task("child", child_consumer_handler)
        pipeline.set_cursor("offset", "original")
        pipeline.enqueue(Job("parent", "one"))
        pipeline.run()

        with StateSnapshot.open(pipeline) as state:
            assert state.get_job("parent::one")["status"] == "failed"
            parent_attempts = list(state.iter_attempts(job_uid="parent::one"))
            assert len(parent_attempts) == 1
            assert parent_attempts[0]["outcome"] == "failure"
            for uid in ("child::sibling", "missing::unknown"):
                assert state.get_job(uid) is None
                assert list(state.iter_attempts(job_uid=uid)) == []
            assert pipeline.get_cursor("offset") == "original"

    with TaskLite(backend="sqlite", state_path=db_path, max_workers=1) as reopened:
        reopened.register_task("parent", unknown_spawn_handler)
        reopened.register_task("child", child_consumer_handler)
        reopened.run()
        assert len(reopened._runtime.store.backend.load_settled_attempts("parent::one")) == 1
        assert reopened._runtime.store.backend.load_settled_attempts("child::sibling") == []
        assert reopened._runtime.store.backend.load_settled_attempts("missing::unknown") == []
        assert reopened.get_cursor("offset") == "original"


def _wait_for_path(path: str | Path, *, timeout_seconds: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    target = Path(path)
    while not target.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out waiting for {target}")
        time.sleep(0.02)


def blocked_duplicate_child_handler(job, ctx):
    Path(job.payload["started"]).write_text("started", encoding="utf-8")
    _wait_for_path(job.payload["release"])
    return TaskResult.success(metadata={"version": job.payload["version"]})


def blocked_duplicate_parent_handler(job, ctx):
    _wait_for_path(job.payload["child_started"])
    ctx.set_cursor("offset", "published")
    ctx.spawn(Job(
        "child",
        "same",
        payload={
            "started": job.payload["child_started"],
            "release": job.payload["release"],
            "version": 2 if job.payload["conflict"] else 1,
        },
    ))
    return TaskResult.success()


@pytest.mark.parametrize("conflict", [False, True])
def test_inflight_duplicate_spawn_uses_real_process_barrier(tmp_path, conflict):
    db_path = tmp_path / "state.db"
    child_started = tmp_path / "child-started"
    child_release = tmp_path / "release-child"
    errors: list[str] = []

    def release_after_parent_settlement():
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            try:
                with StateSnapshot.open(db_path) as state:
                    attempts = list(state.iter_attempts(job_uid="parent::one"))
                    if any(
                        attempt["outcome"] in {"success", "failure"}
                        for attempt in attempts
                    ):
                        child_release.write_text("release", encoding="utf-8")
                        return
            except (OSError, RuntimeError):
                pass
            time.sleep(0.02)
        errors.append("parent did not settle before barrier timeout")

    with TaskLite(backend="sqlite", state_path=db_path, max_workers=2) as pipeline:
        pipeline.register_task("parent", blocked_duplicate_parent_handler)
        pipeline.register_task("child", blocked_duplicate_child_handler)
        child_payload = {
            "started": str(child_started),
            "release": str(child_release),
            "version": 1,
        }
        pipeline.enqueue(Job("child", "same", payload=child_payload))
        pipeline.enqueue(Job(
            "parent",
            "one",
            payload={
                "child_started": str(child_started),
                "release": str(child_release),
                "conflict": conflict,
            },
        ))
        pipeline.set_cursor("offset", "original")

        controller = threading.Thread(target=release_after_parent_settlement)
        controller.start()
        try:
            pipeline.run()
        finally:
            child_release.write_text("release", encoding="utf-8")
            controller.join(timeout=5)

        assert not controller.is_alive()
        assert errors == []
        parent_attempts = list(pipeline._runtime.store.backend.load_settled_attempts("parent::one"))
        assert len(parent_attempts) == 1
        child_attempts = list(pipeline._runtime.store.backend.load_settled_attempts("child::same"))
        assert len(child_attempts) == 1
        assert child_attempts[0]["outcome"] == "success"
        assert child_attempts[0]["job"]["payload"] == child_payload
        if conflict:
            assert parent_attempts[0]["outcome"] == "failure"
            assert "configuration" in parent_attempts[0]["error_message"]
            assert pipeline.get_cursor("offset") == "original"
        else:
            assert parent_attempts[0]["outcome"] == "success"
            assert pipeline.get_cursor("offset") == "published"


def late_child_handler(job, ctx):
    return TaskResult.success(metadata={"published": True})


def late_producer_handler(job, ctx):
    _wait_for_path(job.payload["parent_started"])
    ctx.spawn(Job("late_child", "target"))
    return TaskResult.success()


def late_parent_handler(job, ctx):
    Path(job.payload["parent_started"]).write_text("started", encoding="utf-8")
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        with StateSnapshot.open(job.payload["db_path"]) as state:
            attempts = list(state.iter_attempts(job_uid="late_child::target"))
            if any(attempt["outcome"] == "success" for attempt in attempts):
                break
        time.sleep(0.02)
    else:
        raise TimeoutError("late child did not succeed")

    ctx.set_cursor("offset", "unpublished")
    ctx.spawn(Job("summary", "one", depends_on=["late_child::target"]))
    return TaskResult.success()


def test_dependency_on_job_published_after_parent_dispatch_is_rejected(tmp_path):
    db_path = tmp_path / "state.db"
    parent_started = tmp_path / "parent-started"
    with TaskLite(backend="sqlite", state_path=db_path, max_workers=2) as pipeline:
        pipeline.register_task("producer", late_producer_handler)
        pipeline.register_task("parent", late_parent_handler)
        pipeline.register_task("late_child", late_child_handler)
        pipeline.register_task("summary", child_consumer_handler)
        pipeline.set_cursor("offset", "original")
        pipeline.enqueue(Job(
            "producer",
            "one",
            payload={"parent_started": str(parent_started)},
        ))
        pipeline.enqueue(Job(
            "parent",
            "one",
            payload={
                "parent_started": str(parent_started),
                "db_path": str(db_path),
            },
        ))
        pipeline.run()

        assert "late_child::target" in pipeline._runtime.store.backend.load_successes()
        assert "parent::one" in pipeline._runtime.store.backend.load_failed()
        assert len(pipeline._runtime.store.backend.load_settled_attempts("parent::one")) == 1
        assert pipeline._runtime.store.backend.load_settled_attempts("summary::one") == []
        assert pipeline.get_cursor("offset") == "original"
