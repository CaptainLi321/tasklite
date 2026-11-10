"""数据库故障保留原始异常，事务失败不发布内存状态。"""
from __future__ import annotations

import copy
from contextlib import contextmanager
import sqlite3

import pytest

from tasklite import Job, TaskLite
from tasklite.backend import InMemoryStateBackend, SQLiteStateBackend, TARGET_SCHEMA_VERSION
from tasklite.engine.store import StateStore
from tasklite.models.state import PipelineState


STARTED = "2026-01-01T00:00:00Z"


def _record_connection_errors(monkeypatch, backend):
    original_get_conn = backend._get_conn
    errors = []

    @contextmanager
    def observed_connection():
        try:
            with original_get_conn() as connection:
                yield connection
        except BaseException as error:
            original_traceback = error.__traceback__
            while original_traceback.tb_next is not None:
                original_traceback = original_traceback.tb_next
            errors.append((error, original_traceback))
            raise

    monkeypatch.setattr(backend, "_get_conn", observed_connection)
    return errors


def _assert_original_error(error, errors):
    assert len(errors) == 1
    original, original_traceback = errors[0]
    assert error is original
    assert error.__cause__ is None
    tracebacks = []
    current = error.__traceback__
    while current is not None:
        tracebacks.append(current)
        current = current.tb_next
    assert original_traceback in tracebacks
    assert any(frame.tb_frame.f_code.co_name == "commit_job_settlement"
               or frame.tb_frame.f_code.co_name == "commit_job_failure"
               or frame.tb_frame.f_code.co_name == "mark_job_running"
               for frame in tracebacks)


def _memory_facts(state):
    return copy.deepcopy((state.queue, state.successes, state.failed, state.cursors,
                          state._in_flight_uids))


@pytest.mark.parametrize("operation", ["success", "failure", "retry", "pending", "pre_failure", "running"])
@pytest.mark.parametrize("fault_kind", ["sqlite", "os"])
def test_sqlite_store_fault_preserves_error_and_all_state(tmp_path, monkeypatch, operation, fault_kind):
    backend = SQLiteStateBackend(tmp_path / "state.db", schema_version=TARGET_SCHEMA_VERSION)
    job = Job("work", "one", payload={"key": "value"}).to_dict()
    backend.enqueue_jobs([job, Job("work", "tail").to_dict()])
    backend.set_cursor("offset", "original")
    if operation not in ("pre_failure", "running"):
        backend.mark_job_running("work::one", execution_id="old.1", run_id="old", started_at=STARTED)
    state = PipelineState(backend.load_successes(), backend.load_failed(),
                          backend.load_cursors(), backend.load_queue())
    store = StateStore(backend, state)
    if operation not in ("pre_failure", "running"):
        state.pop_job(0)
        state.register_in_flight("work::one")
    trigger_action = {
        "success": "BEFORE INSERT ON cursors",
        "failure": "BEFORE DELETE ON queue",
        "pre_failure": "BEFORE DELETE ON queue",
        "retry": "BEFORE UPDATE ON queue WHEN NEW.status = 'pending'",
        "pending": "BEFORE UPDATE ON queue WHEN NEW.status = 'pending'",
        "running": "BEFORE UPDATE ON queue WHEN NEW.status = 'running'",
    }[operation]
    with backend._get_conn() as connection:
        if fault_kind == "sqlite":
            connection.execute(f"CREATE TRIGGER reject_write {trigger_action} "
                               "BEGIN SELECT RAISE(ABORT, 'synthetic write failure'); END")
        before_disk = list(connection.iterdump())
    before_memory = _memory_facts(state)
    original = OSError("synthetic write failure")
    original_get_conn = backend._get_conn
    if fault_kind == "os":
        @contextmanager
        def failed_before_commit():
            with original_get_conn() as connection:
                yield connection
                raise original
        monkeypatch.setattr(backend, "_get_conn", failed_before_commit)
    errors = _record_connection_errors(monkeypatch, backend)
    identity = dict(attempt_no=1, execution_id="old.1", run_id="old", started_at=STARTED)
    error_type = sqlite3.IntegrityError if fault_kind == "sqlite" else OSError
    with pytest.raises(error_type, match="synthetic write failure") as caught:
        if operation == "success":
            store.apply_success("work::one", result={"message": None, "exception": None, "metadata": {}, "annotations": {}}, job_snapshot=job,
                                spawned_job_snapshots=[Job("child", "one").to_dict()],
                                cursor_updates={"offset": "unpublished"}, **identity)
        elif operation == "failure":
            store.apply_failure("work::one", {"message": "failed"}, job_snapshot=job,
                                cascade=False, **identity)
        elif operation == "retry":
            store.apply_retry("work::one", job, job, **identity)
        elif operation == "pending":
            store.apply_pending("work::one", job, **identity)
        elif operation == "pre_failure":
            store.apply_failure("work::one", {"message": "rejected"}, job_snapshot=job, cascade=False)
        else:
            store.mark_job_running("work::one", execution_id="old.1", run_id="old", started_at=STARTED)
    _assert_original_error(caught.value, errors)
    if fault_kind == "os":
        assert caught.value is original
    assert _memory_facts(state) == before_memory
    monkeypatch.setattr(backend, "_get_conn", original_get_conn)
    with backend._get_conn() as connection:
        assert list(connection.iterdump()) == before_disk
    assert backend.load_settled_attempts("work::one") == []


class _CopyFailure:
    def __init__(self, error):
        self.error = error

    def __deepcopy__(self, memo):
        raise self.error


@pytest.mark.parametrize("operation", ["success", "failure", "retry", "pending", "pre_failure"])
def test_memory_transaction_fault_preserves_original_error_and_all_state(operation):
    backend = InMemoryStateBackend(schema_version=TARGET_SCHEMA_VERSION)
    job = Job("work", "one").to_dict()
    backend.enqueue_jobs([job])
    if operation != "pre_failure":
        backend.mark_job_running("work::one", execution_id="old.1", run_id="old", started_at=STARTED)
    before = copy.deepcopy(backend.__dict__ | {"_lock": None, "_exec_lock": None})
    original = OSError("无法复制事务数据")
    bad = {"value": _CopyFailure(original)}
    with pytest.raises(OSError) as caught:
        if operation == "pre_failure":
            backend.commit_job_failure("work::one", bad)
        else:
            kwargs = dict(job_uid="work::one", outcome=operation, attempt_no=1,
                          execution_id="old.1", run_id="old", settled_at=STARTED)
            if operation == "success":
                kwargs["success_metadata"] = bad
            elif operation == "failure":
                kwargs["failure_metadata"] = bad
            elif operation == "retry":
                kwargs["requeued_job_snapshot"] = job | bad
            else:
                kwargs["result"] = bad
            backend.commit_job_settlement(**kwargs)
    assert caught.value is original
    assert backend.__dict__ | {"_lock": None, "_exec_lock": None} == before


def _unexpected_handler(job, ctx):
    raise AssertionError("执行前拒绝不能启动 worker")


@pytest.mark.parametrize("operation", ["pre_failure", "running"])
def test_predispatch_database_fault_propagates_without_attempt(tmp_path, monkeypatch, operation):
    path = tmp_path / "state.db"
    with TaskLite(backend="sqlite", state_path=path) as pipeline:
        pipeline.register_task("work", _unexpected_handler)
        upstream = Job("work", "upstream")
        pipeline.set_job_status(upstream, "succeeded", recursive=False)
        pipeline.enqueue(Job("work", "downstream", depends_on=[upstream.uid]))
        if operation == "pre_failure":
            pipeline.clear_history(upstream.uid, records=["successes"])
        backend = pipeline._runtime.store.backend
        with backend._get_conn() as connection:
            action = ("BEFORE INSERT ON failed" if operation == "pre_failure"
                      else "BEFORE UPDATE ON queue WHEN NEW.status = 'running'")
            connection.execute(f"CREATE TRIGGER reject_failure {action} "
                               "BEGIN SELECT RAISE(ABORT, 'synthetic rejection failure'); END")
            before = list(connection.iterdump())
        errors = _record_connection_errors(monkeypatch, backend)
        with pytest.raises(sqlite3.IntegrityError, match="synthetic rejection failure") as caught:
            pipeline.run(install_signals=False)
        _assert_original_error(caught.value, errors)
        with backend._get_conn() as connection:
            after = list(connection.iterdump())
        # Run 身份在派发前独立提交，不属于失败结算事务。
        assert [line for line in after if not line.startswith('INSERT INTO "engine_metadata"')] == before
        assert backend.load_queue_jobs()[0]["status"] == "pending"
        assert backend.load_settled_attempts("work::downstream") == []
        assert pipeline._runtime.store.state.failed == {}


def _transaction_result_handler(job, ctx):
    from tasklite import TaskResult

    ctx.spawn(Job("child", "unpublished"))
    ctx.set_cursor("offset", "unpublished")
    if job.payload["outcome"] == "failure":
        return TaskResult.failure(message="业务失败")
    if job.payload["outcome"] == "retry":
        return TaskResult.retry(consume_budget=False)
    return TaskResult.success()


@pytest.mark.parametrize("outcome", ["success", "failure", "retry"])
def test_run_settlement_fault_preserves_original_identity_and_recovers(tmp_path, monkeypatch, outcome):
    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db", max_workers=1) as pipeline:
        pipeline.register_task("work", _transaction_result_handler)
        pipeline.register_task("child", _unexpected_handler)
        job = Job("work", "one", payload={"outcome": outcome})
        pipeline.enqueue(job)
        pipeline.set_cursor("offset", "original")
        backend = pipeline._runtime.store.backend
        submitted = backend.load_queue_jobs()[0]["job"]
        action = {
            "success": "BEFORE INSERT ON cursors",
            "failure": "BEFORE DELETE ON queue",
            "retry": "BEFORE UPDATE ON queue WHEN NEW.status = 'pending'",
        }[outcome]
        with backend._get_conn() as connection:
            connection.execute(f"CREATE TRIGGER reject_settlement {action} "
                               "BEGIN SELECT RAISE(ABORT, 'synthetic settlement failure'); END")
        errors = _record_connection_errors(monkeypatch, backend)
        with pytest.raises(sqlite3.IntegrityError, match="synthetic settlement failure") as caught:
            pipeline.run(install_signals=False)
        _assert_original_error(caught.value, errors)
        running, = backend.load_queue_jobs()
        assert running["status"] == "running"
        assert running["job"] == submitted
        assert backend.load_settled_attempts(job.uid) == []
        assert backend.load_successes() == backend.load_failed() == {}
        assert backend.load_cursors() == {"offset": "original"}
        state = pipeline._runtime.store.state
        assert state.successes == state.failed == {}
        assert state.cursors == {"offset": "original"}
        with backend._get_conn() as connection:
            connection.execute("DROP TRIGGER reject_settlement")
        pipeline.run(install_signals=False)
        attempt, = backend.load_settled_attempts(job.uid)
        assert (attempt["outcome"], attempt["run_id"], attempt["attempt_no"], attempt["job"]) == (
            "failure", running["run_id"], running["attempt_no"], running["job"],
        )
        pipeline.run(install_signals=False)
        assert backend.load_settled_attempts(job.uid) == [attempt]
        assert backend.load_cursors() == {"offset": "original"}
