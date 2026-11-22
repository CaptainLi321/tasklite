"""只读一致状态快照与 JSON 查询测试（聚焦 StateSnapshot 公开查询与一致性语义）。"""
from __future__ import annotations

from contextlib import closing
import sqlite3
import pytest

from tasklite import (
    FileSpec,
    Job,
    StateSnapshot,
    Task,
    TaskLite,
    TaskLiteError,
    TaskResult,
)
from tasklite.backend.sqlite_backend import SQLiteStateBackend, TARGET_SCHEMA_VERSION
from tasklite.testing import authorizer, in_process_workers


def dummy_handler(job, ctx):
    return TaskResult.success(metadata={"echo": 42})


def retry_then_succeed_handler(job, ctx):
    if job.attempt_no < 3:
        return TaskResult.retry(message=f"attempt {job.attempt_no} failed")
    return TaskResult.success(metadata={"final": True})


@pytest.fixture(params=["sqlite", "memory"])
def pipeline_fixture(request, tmp_path):
    backend_type = request.param
    state_path = (tmp_path / f"state_{backend_type}.db") if backend_type == "sqlite" else None
    p = TaskLite(backend=backend_type, state_path=state_path)
    p.register_task(Task("process", dummy_handler))
    p.register_task(Task("generate", dummy_handler))
    return p


class TestStateSnapshotOpeningAndClosing:
    """快照生命周期与打开/关闭行为。"""

    def test_open_via_path_and_tasklite_instance(self, tmp_path):
        db_path = tmp_path / "custom # % space.db"
        backend = SQLiteStateBackend(db_path)
        backend.enqueue_jobs([Job("process", "j1").to_dict()])

        with StateSnapshot.open(db_path) as s1:
            assert s1.source == db_path
            assert not s1.closed
            job = s1.get_job("process::j1")
            assert job is not None
            assert job["status"] == "pending"

        assert s1.closed is True
        assert not hasattr(s1, "is_closed")
        with pytest.raises(TaskLiteError, match="source must be"):
            StateSnapshot.open(backend)
        with TaskLite(backend="sqlite", state_path=db_path) as sqlite_pipeline:
            with StateSnapshot.open(sqlite_pipeline) as sqlite_snapshot:
                assert sqlite_snapshot.get_job("process::j1") == job

        pipeline = TaskLite(backend="memory")
        pipeline.register_task(Task("process", dummy_handler))
        pipeline.enqueue(Job("process", "m1"))

        with StateSnapshot.open(pipeline) as s2:
            assert not s2.closed
            m_job = s2.get_job("process::m1")
            assert m_job is not None
            assert m_job["status"] == "pending"

        assert s2.closed is True
        assert pipeline.get_cursor("nonexistent") is None

    def test_operations_on_closed_snapshot_raise_error(self, tmp_path):
        db_path = tmp_path / "closed.db"
        SQLiteStateBackend(db_path)
        s = StateSnapshot.open(db_path)
        s.close()
        s.close()  # 幂等

        with pytest.raises(TaskLiteError, match="closed"):
            s.get_job("process::j1")
        with pytest.raises(TaskLiteError, match="closed"):
            list(s.iter_jobs())
        with pytest.raises(TaskLiteError, match="closed"):
            list(s.iter_attempts())

    def test_invalid_source_and_missing_file_rejected(self, tmp_path):
        with pytest.raises(TaskLiteError):
            StateSnapshot.open(tmp_path / "nonexistent.db")
        assert not list(tmp_path.iterdir())

        with pytest.raises(TaskLiteError, match="source must be"):
            StateSnapshot.open(12345)  # type: ignore[arg-type]

        empty_file = tmp_path / "empty.db"
        empty_file.touch()
        with pytest.raises(TaskLiteError):
            StateSnapshot.open(empty_file)
        empty_file.unlink()

    @pytest.mark.parametrize(("version", "change", "message"), [
        (5, None, "Unsupported database schema version 5"),
        (TARGET_SCHEMA_VERSION, "DROP TABLE job_sequences", "for 'job_sequences'"),
        (TARGET_SCHEMA_VERSION, "DROP TABLE authorizations", "for 'authorizations'"),
        (TARGET_SCHEMA_VERSION, "ALTER TABLE authorizations ADD COLUMN extra TEXT", "for 'authorizations'"),
        (TARGET_SCHEMA_VERSION, "ALTER TABLE attempts ADD COLUMN extra TEXT", "for 'attempts'"),
    ])
    def test_invalid_schema_closes_connection_and_preserves_database(
        self, tmp_path, monkeypatch, version, change, message,
    ):
        db_path = tmp_path / "invalid.db"
        backend = SQLiteStateBackend(db_path)
        backend.enqueue_jobs([Job("process", "precious").to_dict()])
        with closing(sqlite3.connect(db_path)) as conn:
            conn.execute(f"PRAGMA user_version={version}")
        if change is not None:
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(change)

        real_connect = sqlite3.connect
        connections = []

        def connect(database, *args, **kwargs):
            assert kwargs.get("uri") is True
            assert database == db_path.resolve().as_uri() + "?mode=ro"
            connection = real_connect(database, *args, **kwargs)
            connections.append(connection)
            return connection

        monkeypatch.setattr(sqlite3, "connect", connect)
        before = db_path.read_bytes()
        with pytest.raises(TaskLiteError, match=message):
            StateSnapshot.open(db_path)
        assert len(connections) == 1
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connections[0].execute("SELECT 1")
        assert db_path.read_bytes() == before
        db_path.unlink()

    def test_corrupt_database_is_preserved(self, tmp_path):
        db_path = tmp_path / "corrupt.db"
        original = b"not a SQLite database"
        db_path.write_bytes(original)

        with pytest.raises(sqlite3.DatabaseError, match="not a database"):
            StateSnapshot.open(db_path)

        assert db_path.read_bytes() == original
        db_path.unlink()


class TestStateSnapshotIsolationAndConsistency:
    """快照隔离与多次读取一致性。"""

    def test_readonly_snapshot_sees_active_wal_and_pins_first_version(self, tmp_path):
        db_path = tmp_path / "state.db"
        first = Job("process", "first")
        second = Job("process", "second")
        with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
            pipeline.register_task(Task("process", dummy_handler))
            with closing(sqlite3.connect(db_path)) as keeper:
                assert keeper.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone() == (0, 0, 0)
                before = db_path.read_bytes()
                pipeline.enqueue(first)
                assert db_path.with_name(db_path.name + "-wal").stat().st_size > 0
                assert db_path.read_bytes() == before

                with StateSnapshot.open(db_path) as previous:
                    first_record = previous.get_job(first.uid)
                    assert first_record is not None
                    assert first_record["status"] == "pending"
                    assert previous.get_job(second.uid) is None

                    pipeline.enqueue(second)

                    assert previous.get_job(first.uid) == first_record
                    assert previous.get_job(second.uid) is None
                    with StateSnapshot.open(pipeline) as current:
                        assert current.get_job(first.uid) == first_record
                        assert {job["job_uid"] for job in current.iter_jobs()} == {
                            first.uid, second.uid,
                        }
                assert db_path.read_bytes() == before

    def test_snapshot_remains_isolated_across_concurrent_commits(self, pipeline_fixture):
        p = pipeline_fixture
        p.enqueue(Job("process", "j1", payload={"v": 1}))

        with StateSnapshot.open(p) as s1:
            p.enqueue(Job("process", "j2", payload={"v": 2}))
            with in_process_workers(p):
                p.run(install_signals=False)

            j1 = s1.get_job("process::j1")
            assert j1 is not None and j1["status"] == "pending"
            assert s1.get_job("process::j2") is None
            jobs_s1 = list(s1.iter_jobs())
            assert [j["job_uid"] for j in jobs_s1] == ["process::j1"]

        with StateSnapshot.open(p) as s2:
            assert s2.get_job("process::j1")["status"] == "succeeded"
            assert s2.get_job("process::j2")["status"] == "succeeded"

    def test_modifying_returned_dictionary_does_not_mutate_framework(self, pipeline_fixture):
        p = pipeline_fixture
        p.enqueue(Job("process", "j1", payload={"nested": {"count": 10}}))

        with StateSnapshot.open(p) as s:
            record = s.get_job("process::j1")
            assert record is not None
            record["status"] = "corrupted"
            record["job"]["payload"]["nested"]["count"] = 999

            second = s.get_job("process::j1")
            assert second is not None
            assert second["status"] == "pending"
            assert second["job"]["payload"]["nested"]["count"] == 10


class TestJobRecordStructureAndPriority:
    """Job 与 Attempt 记录结构分层、FileSpec 样本与优先级。"""

    def test_record_structure_and_filespec_sample(self, tmp_path):
        p = TaskLite(backend="memory")
        p.register_task(Task("generate", dummy_handler))
        p.register_task(Task("process", dummy_handler))

        result_path = str((tmp_path / "results" / "processed.csv").resolve())
        report_path = str((tmp_path / "results" / "report.csv").resolve())

        producer = Job(
            "generate",
            "report-001",
            files={"report": FileSpec(report_path, role="output")},
        )
        consumer = Job(
            "process",
            "consume-001",
            depends_on=[producer.uid],
            files={
                "source": FileSpec.from_output(producer.uid, "report", role="input"),
                "result": FileSpec(result_path, role="output"),
            },
        )
        p.enqueue(producer)
        p.enqueue(consumer)

        with StateSnapshot.open(p) as s:
            rec = s.get_job(consumer.uid)
            assert rec is not None
            assert rec["job_uid"] == consumer.uid
            assert rec["status"] == "pending"
            assert all(rec[k] is None for k in ("result", "run_id", "attempt_no", "started_at", "settled_at"))

            files_cfg = rec["job"]["files"]
            assert files_cfg["source"] == {
                "producer_uid": producer.uid,
                "output_name": "report",
                "role": "input",
                "delete_existing": False,
                "delete_on_failure": False,
                "delete_on_success": False,
            }
            assert files_cfg["result"] == {
                "path": result_path,
                "role": "output",
                "delete_existing": False,
                "delete_on_failure": False,
                "delete_on_success": False,
            }
            assert "result" in rec["files"]
            assert rec["files"]["result"]["path"] == result_path
            assert "source" not in rec["files"]

    def test_queue_pending_and_running_priority_over_old_terminal_status(self, pipeline_fixture):
        p = pipeline_fixture
        p.enqueue(Job("process", "j1"))
        with in_process_workers(p):
            p.run(install_signals=False)

        with StateSnapshot.open(p) as s1:
            assert s1.get_job("process::j1")["status"] == "succeeded"

        with p.edit(dry_run=False) as tx:
            tx.set_status("process::j1", "pending", recursive=False)

        with StateSnapshot.open(p) as s2:
            j1_requeued = s2.get_job("process::j1")
            assert j1_requeued is not None
            assert j1_requeued["status"] == "pending"
            assert all(j1_requeued[k] is None for k in ("result", "run_id", "attempt_no", "started_at", "settled_at"))

    def test_manual_success_and_failure_attributions(self, pipeline_fixture):
        p = pipeline_fixture
        p.enqueue(Job("process", "j_manual_succ"))
        p.enqueue(Job("process", "j_manual_fail"))

        with p.edit(dry_run=False, note="manual approval") as tx:
            tx.replace("process::j_manual_succ", Job("process", "j_manual_succ"), status="succeeded")
        with p.edit(dry_run=False, note="manual rejection") as tx:
            tx.set_status("process::j_manual_fail", "failed", recursive=False)

        with StateSnapshot.open(p) as s:
            succ = s.get_job("process::j_manual_succ")
            assert succ["status"] == "succeeded"
            assert all(succ[k] is None for k in ("run_id", "attempt_no", "started_at"))
            assert succ["settled_at"] is not None
            assert succ["result"]["annotations"]["completion_source"] == "manual"
            assert succ["result"]["annotations"]["note"] == "manual approval"

            fail = s.get_job("process::j_manual_fail")
            assert fail["status"] == "failed"
            assert all(fail[k] is None for k in ("run_id", "attempt_no", "started_at"))
            assert fail["settled_at"] is not None
            assert fail["result"]["annotations"]["completion_source"] == "manual"
            assert fail["result"]["message"] == "manual rejection"

    def test_nonexistent_job_returns_none(self, pipeline_fixture):
        with StateSnapshot.open(pipeline_fixture) as s:
            assert s.get_job("process::does_not_exist") is None
            assert s.get_job("invalid_format") is None


class TestQueriesAndFilters:
    """iter_jobs 与 iter_attempts 的过滤与排序。"""

    def test_sqlite_iter_jobs_streams_cursor_rows(self, tmp_path, monkeypatch):
        db_path = tmp_path / "state.db"
        with TaskLite(backend="sqlite", state_path=db_path) as p:
            p.register_task(Task("process", dummy_handler))
            for job_id in ("succ", "pend", "fail"):
                p.enqueue(Job("process", job_id))
            with in_process_workers(p):
                p.run(install_signals=False)
            with p.edit(dry_run=False) as tx:
                tx.set_status("process::pend", "pending", recursive=False)
                tx.set_status("process::fail", "failed", recursive=False)

        tracked_cursors: list[Any] = []
        real_connect = sqlite3.connect

        class TrackedCursor(sqlite3.Cursor):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.steps = 0

            def __next__(self):
                row = super().__next__()
                self.steps += 1
                return row

            def fetchall(self):
                raise AssertionError("lazy mode iter_jobs must not materialize SQLite rows via fetchall()")

        class TrackedConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                cursor = self.cursor(factory=TrackedCursor)
                cursor.execute(sql, parameters)
                tracked_cursors.append(cursor)
                return cursor

        def wrapped_connect(*args, **kwargs):
            kwargs["factory"] = TrackedConnection
            return real_connect(*args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", wrapped_connect)
        with StateSnapshot.open(db_path, mode="lazy") as snapshot:
            jobs = list(snapshot.iter_jobs())
            assert {job["job_uid"] for job in jobs} == {"process::succ", "process::pend", "process::fail"}

            before = sum(cursor.steps for cursor in tracked_cursors)
            iterator = snapshot.iter_jobs()
            first = next(iterator)
            assert first["job_uid"] == "process::pend"
            assert sum(cursor.steps for cursor in tracked_cursors) == before + 1
            iterator.close()

    def test_iter_jobs_filtering_and_uniqueness(self, pipeline_fixture):
        p = pipeline_fixture
        p.enqueue(Job("process", "j_succ"))
        p.enqueue(Job("process", "j_pend"))
        p.enqueue(Job("generate", "g_fail"))

        with in_process_workers(p):
            p.run(install_signals=False)

        with p.edit(dry_run=False) as tx:
            tx.set_status("process::j_pend", "pending", recursive=False)
            tx.set_status("generate::g_fail", "failed", recursive=False)

        with StateSnapshot.open(p) as s:
            assert {j["job_uid"] for j in s.iter_jobs()} == {"process::j_succ", "process::j_pend", "generate::g_fail"}
            assert [j["job_uid"] for j in s.iter_jobs(status="pending")] == ["process::j_pend"]
            assert [j["job_uid"] for j in s.iter_jobs(status="succeeded")] == ["process::j_succ"]
            assert [j["job_uid"] for j in s.iter_jobs(status="failed")] == ["generate::g_fail"]
            assert {j["job_uid"] for j in s.iter_jobs(task_type="process")} == {"process::j_succ", "process::j_pend"}

            with pytest.raises(TaskLiteError, match="status must be one of"):
                list(s.iter_jobs(status="unknown"))  # type: ignore[arg-type]

    @pytest.mark.parametrize("backend", ["sqlite", "memory"])
    def test_iter_attempts_ordered_by_attempt_no(self, tmp_path, backend):
        p = TaskLite(backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None)
        p.register_task(Task("process", retry_then_succeed_handler))
        p.enqueue(Job("process", "j_retry", max_retries_per_run=5))
        with in_process_workers(p):
            p.run(install_signals=False)

        with StateSnapshot.open(p) as s:
            attempts = list(s.iter_attempts(job_uid="process::j_retry"))
            assert [a["attempt_no"] for a in attempts] == [1, 2, 3]
            assert [a["outcome"] for a in attempts] == ["retry", "retry", "success"]
            assert attempts[2]["result"]["metadata"] == {"final": True}
            for a in attempts:
                assert a["job_uid"] == "process::j_retry"
                assert set(a) == {
                    "job_uid", "outcome", "job", "files", "result", "run_id",
                    "attempt_no", "started_at", "settled_at",
                }
                assert set(a["job"]) == {
                    "task_type", "job_id", "payload", "depends_on", "files",
                    "resources", "max_retries_per_run", "timeout_seconds", "trigger",
                }
                assert set(a["result"]) == {"message", "exception", "metadata", "annotations"}
                assert a["settled_at"] is not None
                assert a["run_id"] is not None

        with p.edit(dry_run=False) as tx:
            assert tx.remove("process::j_retry", entries=("successes", "failures"), recursive=False) == 1
        with StateSnapshot.open(p) as s:
            assert s.get_job("process::j_retry") is None
            assert len(list(s.iter_attempts(job_uid="process::j_retry"))) == 3
        p.enqueue(Job("process", "j_retry", max_retries_per_run=5))
        with StateSnapshot.open(p) as s:
            pending = s.get_job("process::j_retry")
            assert pending["status"] == "pending"
            assert all(pending[key] is None for key in ("result", "run_id", "attempt_no", "started_at", "settled_at"))
        with in_process_workers(p):
            p.run(install_signals=False)
        with StateSnapshot.open(p) as s:
            current = list(s.iter_attempts(job_uid="process::j_retry"))
            assert [a["attempt_no"] for a in current] == [1, 2, 3, 4]
            assert current[-1]["attempt_no"] == 4
            assert current[-1]["run_id"] != attempts[-1]["run_id"]
            assert current[-1]["job"] == attempts[-1]["job"]
            assert current[-1]["outcome"] == "success"
        p.close()


def terminal_result_handler(job, ctx):
    mode = job.payload["mode"]
    if mode == "retry":
        return TaskResult.retry(message="本次重试", metadata={"mode": mode})
    if mode == "failure":
        return TaskResult.failure(message="本次失败", metadata={"mode": mode})
    return TaskResult.success(metadata={"mode": mode})


def assert_current_sources(pipeline, uid, expected, attempts):
    sources = [pipeline]
    if pipeline._state_path is not None:
        sources.append(pipeline._state_path)
    for source in sources:
        with StateSnapshot.open(source) as snapshot:
            assert snapshot.get_job(uid) == expected
            assert list(snapshot.iter_jobs(status=expected["status"])) == [expected]
            assert list(snapshot.iter_attempts(job_uid=uid)) == attempts


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("prior,authorization_pending", [
    (None, False),
    ("success", False),
    ("failure", True),
])
def test_predispatch_failure_owns_current_result_and_configuration(
    tmp_path, monkeypatch, backend, prior, authorization_pending,
):
    source = tmp_path / "input"
    source.write_text("输入")
    with TaskLite(backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None) as p:
        p.register_task("process", terminal_result_handler, trigger="always")
        attempts = []
        if prior is not None:
            p.enqueue(Job("process", "one", payload={"mode": prior}))
            with in_process_workers(p):
                p.run(install_signals=False)
            with StateSnapshot.open(p) as snapshot:
                attempts = list(snapshot.iter_attempts(job_uid="process::one"))
            assert len(attempts) == 1
        files = {"input": FileSpec(source, role="input")}
        if authorization_pending:
            def defer_authorization(path, target_type):
                source.unlink()
                return None

            files["out"] = FileSpec(tmp_path / "out", role="output", delete_existing=True)
            p.enqueue(Job("process", "one", payload={"mode": "current"}, files=files, timeout_seconds=7))
            monkeypatch.setattr("tasklite.engine.store.utc_now_iso", lambda: "2030-01-02T03:04:05+00:00")
            with in_process_workers(p), authorizer(p, defer_authorization):
                p.run(install_signals=False)
        else:
            source.unlink()
            p.enqueue(Job("process", "one", payload={"mode": "current"}, files=files, timeout_seconds=7))
            monkeypatch.setattr("tasklite.engine.store.utc_now_iso", lambda: "2030-01-02T03:04:05+00:00")
            with in_process_workers(p):
                p.run(install_signals=False)
        with StateSnapshot.open(p) as snapshot:
            current = snapshot.get_job("process::one")
        assert current["status"] == "failed"
        assert "Missing input file" in current["result"]["message"]
        assert current["result"]["metadata"] == {}
        exception = current["result"]["exception"]
        assert exception["type"] == "FileNotFoundError"
        assert "_resolve_job_files" in exception["traceback"]
        assert all(current[key] is None for key in ("run_id", "attempt_no", "started_at"))
        assert current["settled_at"] == "2030-01-02T03:04:05Z"
        assert current["job"]["payload"] == {"mode": "current"}
        assert current["job"]["timeout_seconds"] == 7
        assert current["files"]["input"]["path"] == str(source)
        assert_current_sources(p, "process::one", current, attempts)
        with p.edit(dry_run=False) as tx:
            assert tx.remove("process::one", entries=("failures",), recursive=False) == 1
        with StateSnapshot.open(p) as snapshot:
            assert snapshot.get_job("process::one") is None
            assert list(snapshot.iter_attempts(job_uid="process::one")) == attempts


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("mode", ["failure", "retry"])
def test_executed_failure_retains_own_facts_after_attempt_cleanup(tmp_path, backend, mode):
    with TaskLite(backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None) as p:
        p.register_task("process", terminal_result_handler, trigger="always")
        output = tmp_path / "output"
        p.enqueue(Job("process", "one", payload={"mode": mode}, max_retries_per_run=0,
                      files={"output": FileSpec(output, role="output")}))
        with in_process_workers(p):
            p.run(install_signals=False)
        with StateSnapshot.open(p) as snapshot:
            current = snapshot.get_job("process::one")
            attempts = list(snapshot.iter_attempts(job_uid="process::one"))
        assert len(attempts) == 1
        attempt = attempts[0]
        assert current["status"] == "failed"
        for key in ("job", "files", "result", "run_id", "attempt_no", "started_at", "settled_at"):
            assert current[key] == attempt[key]
        assert current["result"]["metadata"] == {"mode": mode}
        assert current["attempt_no"] == 1
        assert_current_sources(p, "process::one", current, attempts)
        with p.edit(dry_run=False) as tx:
            assert tx.remove("process::one", entries=("failures",), recursive=False) == 1
        with StateSnapshot.open(p) as snapshot:
            assert snapshot.get_job("process::one") is None
            assert list(snapshot.iter_attempts(job_uid="process::one")) == attempts


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_cascaded_failure_keeps_current_config_and_settlement(tmp_path, monkeypatch, backend):
    with TaskLite(backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None) as p:
        p.register_task("process", terminal_result_handler)
        parent = Job("process", "parent", payload={"mode": "failure"})
        child = Job("process", "child", payload={"mode": "success"}, depends_on=[parent.uid])
        p.enqueue(parent)
        p.enqueue(child)
        monkeypatch.setattr("tasklite.engine.store.utc_now_iso", lambda: "2030-01-02T03:04:05+00:00")
        with in_process_workers(p):
            p.run(install_signals=False)
        for source in [p] + ([p._state_path] if p._state_path is not None else []):
            with StateSnapshot.open(source) as snapshot:
                current = snapshot.get_job(child.uid)
                assert current["status"] == "failed"
                assert current["job"]["payload"] == child.payload
                assert current["job"]["depends_on"] == [parent.uid]
                assert current["result"]["message"] == "Dependency failed or has no record"
                assert current["result"]["exception"] is None
                assert current["settled_at"] == "2030-01-02T03:04:05Z"
                assert all(current[key] is None for key in ("run_id", "attempt_no", "started_at"))
                assert list(snapshot.iter_attempts(job_uid=child.uid)) == []


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_manual_failure_and_record_deletion_do_not_reuse_execution_history(tmp_path, backend):
    with TaskLite(backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None) as p:
        p.register_task("process", terminal_result_handler)
        job = Job("process", "one", payload={"mode": "success"})
        p.enqueue(job)
        with in_process_workers(p):
            p.run(install_signals=False)
        with p.edit(dry_run=False, note="人工否决") as tx:
            tx.set_status(job.uid, "failed", recursive=False)
        with StateSnapshot.open(p) as snapshot:
            current = snapshot.get_job(job.uid)
            attempts = list(snapshot.iter_attempts(job_uid=job.uid))
        assert len(attempts) == 1
        assert current["result"]["message"] == "人工否决"
        assert current["result"]["annotations"]["completion_source"] == "manual"
        assert current["result"]["metadata"] == {}
        assert all(current[key] is None for key in ("run_id", "attempt_no", "started_at"))
        assert_current_sources(p, job.uid, current, attempts)
        with p.edit(dry_run=False) as tx:
            assert tx.remove(job.uid, entries=("failures",), recursive=False) == 1
        with StateSnapshot.open(p) as snapshot:
            assert snapshot.get_job(job.uid) is None
            assert list(snapshot.iter_attempts(job_uid=job.uid)) == attempts


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_running_recovery_current_failure_is_self_contained(tmp_path, backend):
    with TaskLite(backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None) as p:
        p.register_task("process", terminal_result_handler)
        job = Job("process", "one", payload={"mode": "success"},
                  files={"out": FileSpec(tmp_path / "out", role="output")})
        p.enqueue(job)
        p._runtime.store.backend.mark_job_running(
            job.uid, execution_id="old.1", run_id="old", started_at="2026-01-01T00:00:00Z",
        )
        with in_process_workers(p):
            p.run(install_signals=False)
        with StateSnapshot.open(p) as snapshot:
            current = snapshot.get_job(job.uid)
            attempts = list(snapshot.iter_attempts(job_uid=job.uid))
        assert len(attempts) == 1
        assert current["result"]["message"] == "上次执行未完成结算，实际结果未知"
        assert current["run_id"] == "old"
        assert current["attempt_no"] == 1
        assert current["started_at"] == attempts[0]["started_at"]
        assert current["settled_at"] is not None
        assert current["job"]["payload"] == job.payload
        assert current["files"]["out"]["path"] == str(tmp_path / "out")
        assert current["result"] == attempts[0]["result"]
        assert_current_sources(p, job.uid, current, attempts)


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_bulk_predispatch_failure_preserves_config_and_current_settlement(tmp_path, backend):
    from tasklite.resource import CapacityResource

    with TaskLite(backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None) as p:
        p.register_resource(CapacityResource("capacity", max_capacity=1))
        p.register_task("process", terminal_result_handler)
        job = Job("process", "one", payload={"mode": "success"}, resources={"capacity": 2})
        p.enqueue(job)
        with in_process_workers(p):
            p.run(install_signals=False)
        with StateSnapshot.open(p) as snapshot:
            current = snapshot.get_job(job.uid)
        assert current["status"] == "failed"
        assert current["job"]["payload"] == job.payload
        assert current["job"]["resources"]["capacity"] == 2
        assert current["result"]["exception"] is None
        assert current["result"]["message"]
        assert current["result"]["metadata"] == {}
        assert current["settled_at"] is not None
        assert all(current[key] is None for key in ("run_id", "attempt_no", "started_at"))
        assert_current_sources(p, job.uid, current, [])


def authorization_stopped_result_handler(job, ctx):
    if job.payload.get("block"):
        ctx.spawn(Job("process", "child", payload={"mode": "success"}))
        ctx.set_cursor("stopped", "unpublished")
        ctx.declare_file("cache", FileSpec(
            job.payload["path"], role="cache", delete_existing=True,
        ))
    else:
        ctx.declare_file("output", FileSpec(job.payload["path"], role="output"))
    if job.payload["mode"] == "failure":
        return TaskResult.failure(message="可信失败", metadata={"business": "failure"})
    return TaskResult.success(message="可信成功", metadata={"business": "success"})


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("prior", ["success", "failure"])
def test_current_terminal_record_ignores_later_authorization_attempt(
    tmp_path, monkeypatch, backend, prior,
):
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "business"
    target.write_text("业务数据")
    with TaskLite(backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None) as p:
        gate = [True]
        p.register_trigger("gate", lambda ctx: gate[0])
        p.register_task("process", authorization_stopped_result_handler, trigger="gate")
        uid = "process::one"
        p.enqueue(Job("process", "one", payload={"mode": prior, "path": str(target)},
                      timeout_seconds=17, max_retries_per_run=2))
        with in_process_workers(p):
            p.run(install_signals=False)
        with StateSnapshot.open(p) as snapshot:
            original = snapshot.get_job(uid)
            attempts = list(snapshot.iter_attempts(job_uid=uid))
        assert len(attempts) == 1
        for key in ("job", "files", "result", "run_id", "attempt_no", "started_at", "settled_at"):
            assert original[key] == attempts[0][key]
        assert original["files"]["output"]["path"] == str(target)
        assert original["result"]["metadata"] == {"business": prior}

        def defer_authorization(path, target_type):
            p.stop()
            return None

        p.enqueue(Job("process", "one", payload={"mode": "success", "block": True, "path": str(target)},
                      timeout_seconds=19, max_retries_per_run=0))
        # 动态声明被 stop 终止需强制终止阻塞在 wait_reply 的 worker，使用真实子进程
        with authorizer(p, defer_authorization):
            p.run(install_signals=False)
        with StateSnapshot.open(p) as snapshot:
            assert snapshot.get_job(uid)["status"] == "pending"
            later = list(snapshot.iter_attempts(job_uid=uid))
        assert [a["outcome"] for a in later] == [prior, "failure"]
        assert [a["attempt_no"] for a in later] == [1, 2]
        assert later[0] == attempts[0]
        assert later[1]["result"]["annotations"]["authorization_status"] == "stopped"
        assert later[1]["run_id"] != original["run_id"]
        assert p.get_cursor("stopped") is None
        assert target.read_text() == "业务数据"
        gate[0] = False
        with in_process_workers(p):
            p.run(install_signals=False)
        assert_current_sources(p, uid, original, later)
        with StateSnapshot.open(p) as snapshot:
            assert list(snapshot.iter_jobs()) == [original]
            assert snapshot.get_job("process::child") is None
        gate[0] = True
        p.enqueue(Job("process", "one", payload={"mode": "success", "path": str(target)}))
        with in_process_workers(p):
            p.run(install_signals=False)
        with StateSnapshot.open(p) as snapshot:
            current = snapshot.get_job(uid)
            attempts = list(snapshot.iter_attempts(job_uid=uid))
        assert current["attempt_no"] == 3
        assert [a["attempt_no"] for a in attempts] == [1, 2, 3]
        assert_current_sources(p, uid, current, attempts)
        with p.edit(dry_run=False) as tx:
            assert tx.remove(uid, entries=("successes",), recursive=False) == 1
        for source in [p] + ([p._state_path] if p._state_path is not None else []):
            with StateSnapshot.open(source) as snapshot:
                assert snapshot.get_job(uid) is None
                assert list(snapshot.iter_jobs()) == []
                assert list(snapshot.iter_attempts(job_uid=uid)) == attempts


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_manual_success_does_not_reuse_execution_facts(tmp_path, backend):
    with TaskLite(backend=backend, state_path=tmp_path / "state.db" if backend == "sqlite" else None) as p:
        p.register_task("process", terminal_result_handler)
        job = Job("process", "one", payload={"mode": "success"})
        p.enqueue(job)
        with in_process_workers(p):
            p.run(install_signals=False)
        with StateSnapshot.open(p) as snapshot:
            attempts = list(snapshot.iter_attempts(job_uid=job.uid))
        with p.edit(dry_run=False, note="人工确认") as tx:
            tx.replace(job.uid, job, status="succeeded")
        with StateSnapshot.open(p) as snapshot:
            current = snapshot.get_job(job.uid)
        assert current["status"] == "succeeded"
        assert current["result"]["metadata"] == {}
        assert current["result"]["annotations"] == {"completion_source": "manual", "note": "人工确认"}
        assert all(current[key] is None for key in ("run_id", "attempt_no", "started_at"))
        assert current["settled_at"] is not None
        assert_current_sources(p, job.uid, current, attempts)


class TestQueueStartedAtProjection:
    """queue.started_at 在 running / pending 态下的投影契约（双后端与双模式对等）。"""

    @pytest.mark.parametrize("backend", ["memory", "sqlite"])
    @pytest.mark.parametrize("mode", ["lazy", "copy"])
    def test_running_projects_started_at_and_pending_is_always_none(self, tmp_path, backend, mode):
        db_path = tmp_path / f"started_{backend}_{mode}.db"
        with TaskLite(
            backend=backend,
            state_path=db_path if backend == "sqlite" else None,
        ) as p:
            p.register_task("process", dummy_handler)
            j_run = Job("process", "running_one")
            j_pend = Job("process", "pending_one")
            p.enqueue(j_run)
            p.enqueue(j_pend)
            p._runtime.store.backend.mark_job_running(
                j_run.uid,
                execution_id="run_x.1",
                run_id="run_x",
                started_at="2026-06-01T12:34:56+00:00",
            )
            # 即使底层队列行残留 started_at，pending 态也必须投影为 None
            if backend == "sqlite":
                with closing(sqlite3.connect(db_path)) as conn:
                    conn.execute(
                        "UPDATE queue SET started_at = ? WHERE job_uid = ?",
                        ("2026-06-01T10:00:00+00:00", j_pend.uid),
                    )
                    conn.commit()
            else:
                for item in p._runtime.store.backend._queue:
                    if item.job_uid == j_pend.uid:
                        item.started_at = "2026-06-01T10:00:00+00:00"

            sources = [p] + ([db_path] if backend == "sqlite" else [])
            for src in sources:
                with StateSnapshot.open(src, mode=mode) as snap:
                    running_rec = snap.get_job(j_run.uid)
                    assert running_rec is not None
                    assert running_rec["status"] == "running"
                    assert running_rec["run_id"] == "run_x"
                    assert running_rec["attempt_no"] == 1
                    assert running_rec["started_at"] == "2026-06-01T12:34:56Z"
                    assert running_rec["result"] is None
                    assert running_rec["settled_at"] is None
                    assert list(snap.iter_jobs(status="running")) == [running_rec]

                    pending_rec = snap.get_job(j_pend.uid)
                    assert pending_rec is not None
                    assert pending_rec["status"] == "pending"
                    assert pending_rec["started_at"] is None
                    assert pending_rec["run_id"] is None
                    assert pending_rec["attempt_no"] is None
                    assert list(snap.iter_jobs(status="pending")) == [pending_rec]


class TestStateSnapshotCopyMode:
    """StateSnapshot mode='copy' 零句柄占用、惰性反序列化、隔离性与双模式同形。"""

    @pytest.mark.parametrize("bad_mode", ["invalid", "", "LAZY", 123, None, True])
    def test_invalid_mode_raises_tasklite_error(self, tmp_path, bad_mode):
        db_path = tmp_path / "not_created.db"
        with pytest.raises(TaskLiteError, match="mode must be"):
            StateSnapshot.open(db_path, mode=bad_mode)  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="mode must be"):
            StateSnapshot(db_path, mode=bad_mode)  # type: ignore[arg-type]
        assert not db_path.exists()

    def test_sqlite_copy_mode_closes_connection_immediately_and_releases_file(
        self, tmp_path, monkeypatch
    ):
        db_path = tmp_path / "copy_release.db"
        with TaskLite(backend="sqlite", state_path=db_path) as p:
            p.register_task("process", dummy_handler)
            p.enqueue(Job("process", "j1", payload={"v": 1}))

            real_connect = sqlite3.connect
            opened_conns: list[sqlite3.Connection] = []

            def tracking_connect(database, *args, **kwargs):
                assert kwargs.get("uri") is True
                assert database == db_path.resolve().as_uri() + "?mode=ro"
                conn = real_connect(database, *args, **kwargs)
                opened_conns.append(conn)
                return conn

            monkeypatch.setattr(sqlite3, "connect", tracking_connect)
            before = StateSnapshot.open(db_path, mode="copy")
            monkeypatch.setattr(sqlite3, "connect", real_connect)

            # 打开返回前即已关闭底层 SQLite 连接
            assert len(opened_conns) == 1
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                opened_conns[0].execute("SELECT 1")

            # 不调用 before.close()，跨 run() 正常写入新状态
            p.enqueue(Job("process", "j2", payload={"v": 2}))
            with in_process_workers(p):
                p.run(install_signals=False)

            after = StateSnapshot.open(p, mode="copy")

        # 甚至关闭 pipeline 后立即删除磁盘上的库文件，未关闭的 copy 快照仍可完整读取
        db_path.unlink()
        for suffix in ("-wal", "-shm"):
            sidecar = db_path.with_name(db_path.name + suffix)
            if sidecar.exists():
                sidecar.unlink()

        try:
            assert before.get_job("process::j1")["status"] == "pending"
            assert before.get_job("process::j2") is None
            assert {j["job_uid"] for j in before.iter_jobs()} == {"process::j1"}

            assert after.get_job("process::j1")["status"] == "succeeded"
            assert after.get_job("process::j2")["status"] == "succeeded"
            assert {j["job_uid"] for j in after.iter_jobs()} == {"process::j1", "process::j2"}
        finally:
            before.close()
            after.close()

        with pytest.raises(TaskLiteError, match="closed"):
            before.get_job("process::j1")
        with pytest.raises(TaskLiteError, match="closed"):
            list(after.iter_jobs())
        with pytest.raises(TaskLiteError, match="closed"):
            list(after.iter_attempts())

    def test_sqlite_copy_mode_lazily_deserializes_json_columns(self, tmp_path, monkeypatch):
        import tasklite.snapshot as snapshot_mod

        db_path = tmp_path / "lazy_json.db"
        with TaskLite(backend="sqlite", state_path=db_path) as p:
            p.register_task("process", terminal_result_handler)
            for idx in range(5):
                p.enqueue(Job("process", f"ok_{idx}", payload={"mode": "success", "n": idx}))
            p.enqueue(Job("process", "bad_0", payload={"mode": "failure"}))
            with in_process_workers(p):
                p.run(install_signals=False)
            p.enqueue(Job("process", "pend_0", payload={"mode": "success"}))

        real_loads = snapshot_mod.loads
        load_calls = 0

        def counting_loads(text):
            nonlocal load_calls
            load_calls += 1
            return real_loads(text)

        monkeypatch.setattr(snapshot_mod, "loads", counting_loads)
        with StateSnapshot.open(db_path, mode="copy") as snap:
            # open 阶段零反序列化
            assert load_calls == 0

            # get_job 命中单个 pending 作业，仅反序列化 1 次 job_snapshot
            pend = snap.get_job("process::pend_0")
            assert pend is not None and pend["status"] == "pending"
            assert load_calls == 1

            # iter_jobs(status="failed") 仅反序列化 1 条 failed（metadata + payload = 2 次），跳过 5 条 succeeded 与 1 条 pending
            load_calls = 0
            failed = list(snap.iter_jobs(status="failed"))
            assert len(failed) == 1 and failed[0]["job_uid"] == "process::bad_0"
            assert load_calls == 2

            # iter_attempts(job_uid="process::bad_0") 仅反序列化命中的 1 条 attempt（最多 3 个 JSON 列），跳过其他 5 条
            load_calls = 0
            atts = list(snap.iter_attempts(job_uid="process::bad_0"))
            assert len(atts) == 1
            assert 1 <= load_calls <= 3

    @pytest.mark.parametrize("backend", ["memory", "sqlite"])
    def test_copy_and_lazy_modes_are_100_percent_identical(self, tmp_path, backend):
        db_path = tmp_path / f"parity_{backend}.db"
        with TaskLite(
            backend=backend,
            state_path=db_path if backend == "sqlite" else None,
        ) as p:
            p.register_task("process", retry_then_succeed_handler)
            p.register_task("generate", terminal_result_handler)
            p.enqueue(Job("process", "ok", max_retries_per_run=5))
            p.enqueue(Job("generate", "bad", payload={"mode": "failure"}))
            with in_process_workers(p):
                p.run(install_signals=False)
            p.enqueue(Job("process", "pend"))
            p.enqueue(Job("generate", "run_item"))
            p._runtime.store.backend.mark_job_running(
                "generate::run_item",
                execution_id="run_live.1",
                run_id="run_live",
                started_at="2026-07-01T08:00:00+00:00",
            )

            sources = [p] + ([db_path] if backend == "sqlite" else [])
            for src in sources:
                with (
                    StateSnapshot.open(src, mode="lazy") as lazy_snap,
                    StateSnapshot.open(src, mode="copy") as copy_snap,
                ):
                    for uid in ("process::ok", "generate::bad", "process::pend", "generate::run_item", "process::missing"):
                        assert copy_snap.get_job(uid) == lazy_snap.get_job(uid)
                    for st in (None, "pending", "running", "succeeded", "failed"):
                        assert list(copy_snap.iter_jobs(status=st)) == list(lazy_snap.iter_jobs(status=st))
                    for tt in (None, "process", "generate"):
                        assert list(copy_snap.iter_jobs(task_type=tt)) == list(lazy_snap.iter_jobs(task_type=tt))
                        assert list(copy_snap.iter_attempts(task_type=tt)) == list(lazy_snap.iter_attempts(task_type=tt))
                    assert list(copy_snap.iter_attempts(job_uid="process::ok")) == list(
                        lazy_snap.iter_attempts(job_uid="process::ok")
                    )

    def test_memory_shallow_attempts_isolation_across_mutations_and_edit(self):
        with TaskLite(backend="memory") as p:
            p.register_task("process", retry_then_succeed_handler)
            p.enqueue(Job("process", "j1", payload={"nested": {"k": 1}}, max_retries_per_run=5))
            with in_process_workers(p):
                p.run(install_signals=False)

            snap = StateSnapshot.open(p, mode="copy")
            try:
                attempts_first = list(snap.iter_attempts(job_uid="process::j1"))
                assert len(attempts_first) == 3
                # 篡改返回字典不得污染快照或底层后端
                attempts_first[0]["job"]["payload"]["nested"]["k"] = 999
                attempts_first[0]["result"]["metadata"]["injected"] = True

                # 即使后续 edit() 移除成功档案并追加新执行，已打开快照仍完全隔离
                with p.edit(dry_run=False) as tx:
                    tx.remove("process::j1", entries=("successes",), recursive=False)
                p.enqueue(Job("process", "j1", payload={"nested": {"k": 2}}, max_retries_per_run=5))
                with in_process_workers(p):
                    p.run(install_signals=False)

                attempts_second = list(snap.iter_attempts(job_uid="process::j1"))
                assert len(attempts_second) == 3
                assert attempts_second[0]["job"]["payload"]["nested"]["k"] == 1
                assert "injected" not in attempts_second[0]["result"]["metadata"]
            finally:
                snap.close()

    def test_sqlite_copy_mode_survives_source_db_deletion_and_replacement(self, tmp_path):
        """mode='copy' 打开后立刻释放句柄，删除或覆盖源 DB 文件后快照仍完整可读。"""
        db_path = tmp_path / "replaceable.db"
        with TaskLite(backend="sqlite", state_path=db_path) as p:
            p.register_task("process", retry_then_succeed_handler)
            p.enqueue(Job("process", "item", max_retries_per_run=5))
            with in_process_workers(p):
                p.run(install_signals=False)

        snap = StateSnapshot.open(db_path, mode="copy")
        try:
            # 立刻删除源库及 WAL/SHM 旁路文件，并用全新内容覆盖，验证无文件句柄占用
            for target in (
                db_path,
                db_path.with_name(f"{db_path.name}-wal"),
                db_path.with_name(f"{db_path.name}-shm"),
            ):
                if target.exists():
                    target.unlink()
            db_path.write_bytes(b"replaced-with-unrelated-content")

            rec = snap.get_job("process::item")
            assert rec is not None and rec["status"] == "succeeded"
            assert [j["job_uid"] for j in snap.iter_jobs()] == ["process::item"]
            attempts = list(snap.iter_attempts(job_uid="process::item"))
            assert [a["attempt_no"] for a in attempts] == [1, 2, 3]
            report = StateSnapshot.report(snap)
            assert "process" in report
        finally:
            snap.close()

        assert snap.closed is True
        snap.close()
        with pytest.raises(TaskLiteError, match="closed"):
            snap.get_job("process::item")

    def test_empty_attempt_result_projection_identical_across_backends_modes_and_edit(self, tmp_path):
        """空 Attempt.result（result=None, error_message=None/str）在 SQLite(lazy/copy)、Memory 与 EditTransaction 下 100% 同形。"""
        expected_empty_result = {
            "message": None,
            "exception": None,
            "metadata": {},
            "annotations": {},
        }
        expected_err_result = {
            "message": "raw error fallback",
            "exception": None,
            "metadata": {},
            "annotations": {},
        }

        collected_all: list[list[dict]] = []
        collected_by_uid: list[list[dict]] = []
        for backend_kind in ("sqlite", "memory"):
            db_path = tmp_path / f"empty_result_{backend_kind}.db"
            with TaskLite(
                backend=backend_kind,
                state_path=db_path if backend_kind == "sqlite" else None,
            ) as p:
                p.register_task("process", dummy_handler)
                j1 = Job("process", "null_both")
                j2 = Job("process", "null_with_err")
                p.enqueue(j1)
                p.enqueue(j2)
                b = p._runtime.store.backend
                b.mark_job_running(j1.uid, execution_id="r1.1", run_id="r1", started_at="2026-08-01T00:00:00Z")
                b.commit_job_settlement(
                    job_uid=j1.uid,
                    outcome="success",
                    attempt_no=1,
                    execution_id="r1.1",
                    run_id="r1",
                    started_at="2026-08-01T00:00:00Z",
                    settled_at="2026-08-01T00:00:01Z",
                    job_snapshot=j1.to_dict(),
                    result=None,
                    error_message=None,
                    success_metadata={"job_snapshot": j1.to_dict()},
                )
                b.mark_job_running(j2.uid, execution_id="r1.2", run_id="r1", started_at="2026-08-01T00:00:02Z")
                b.commit_job_settlement(
                    job_uid=j2.uid,
                    outcome="failure",
                    attempt_no=1,
                    execution_id="r1.2",
                    run_id="r1",
                    started_at="2026-08-01T00:00:02Z",
                    settled_at="2026-08-01T00:00:03Z",
                    job_snapshot=j2.to_dict(),
                    result=None,
                    error_message="raw error fallback",
                    failure_metadata={"job_snapshot": j2.to_dict()},
                )

                sources = [p] + ([db_path] if backend_kind == "sqlite" else [])
                for src in sources:
                    for mode in ("lazy", "copy"):
                        with StateSnapshot.open(src, mode=mode) as snap:
                            all_atts = list(snap.iter_attempts())
                            uid_atts = list(snap.iter_attempts(job_uid=j1.uid))
                            assert [a["job_uid"] for a in all_atts] == [j1.uid, j2.uid]
                            assert all_atts[0]["result"] == expected_empty_result
                            assert all_atts[1]["result"] == expected_err_result
                            assert uid_atts[0]["result"] == expected_empty_result
                            collected_all.append(all_atts)
                            collected_by_uid.append(uid_atts)

                with p.edit(dry_run=True) as tx:
                    for tx_snap in (tx.before, tx.after):
                        all_atts = list(tx_snap.iter_attempts())
                        uid_atts = list(tx_snap.iter_attempts(job_uid=j1.uid))
                        assert [a["job_uid"] for a in all_atts] == [j1.uid, j2.uid]
                        assert all_atts[0]["result"] == expected_empty_result
                        assert all_atts[1]["result"] == expected_err_result
                        collected_all.append(all_atts)
                        collected_by_uid.append(uid_atts)

        for entry in collected_all[1:]:
            assert entry == collected_all[0]
        for entry in collected_by_uid[1:]:
            assert entry == collected_by_uid[0]


class TestStateSnapshotReport:
    """StateSnapshot.report 单快照与双快照对比报告验收。"""

    def test_report_validation_rejects_invalid_and_closed_snapshots(self, tmp_path):
        with TaskLite(backend="memory") as p:
            open_snap = StateSnapshot.open(p, mode="copy")
            closed_snap = StateSnapshot.open(p, mode="copy")
            closed_snap.close()
            try:
                with pytest.raises(TaskLiteError, match="first must be a StateSnapshot"):
                    StateSnapshot.report("not_a_snapshot")  # type: ignore[arg-type]
                with pytest.raises(TaskLiteError, match="closed"):
                    StateSnapshot.report(closed_snap)
                with pytest.raises(TaskLiteError, match="second must be a StateSnapshot"):
                    StateSnapshot.report(open_snap, "bad")  # type: ignore[arg-type]
                with pytest.raises(TaskLiteError, match="closed"):
                    StateSnapshot.report(open_snap, closed_snap)
            finally:
                open_snap.close()

    @pytest.mark.parametrize("backend", ["memory", "sqlite"])
    def test_report_empty_database_renders_safely(self, tmp_path, backend):
        db_path = tmp_path / f"empty_{backend}.db"
        with TaskLite(
            backend=backend,
            state_path=db_path if backend == "sqlite" else None,
        ) as p:
            with StateSnapshot.open(p, mode="copy") as snap:
                text = StateSnapshot.report(snap)
                assert "Summary (total: 0):" in text
                assert "pending:   0" in text
                assert "running:   0" in text
                assert "succeeded: 0 (execution: 0, manual: 0)" in text
                assert "failed:    0" in text
                assert "By Task Type:\n  (none)" in text
                assert "Running Jobs (0):\n  (none)" in text
                assert "Failed Jobs (0):\n  (none)" in text

    def test_single_snapshot_report_metrics_eta_manual_and_truncation(self, tmp_path, monkeypatch):
        import tasklite._report as report_mod

        db_path = tmp_path / "report_single.db"
        with TaskLite(backend="sqlite", state_path=db_path) as p:
            p.register_task("train", dummy_handler)
            p.register_task("eval", dummy_handler)

            # 1) 构造 3 个 train 历史成功 Attempt（耗时分别为 10s、20s、60s -> Avg=30.0s, P50=20.0s, Max=60.0s）
            #    以及 1 个时钟回拨样本（settled < started -> 0.00s，不产生负数）和 1 个缺失 started_at 样本（跳过）
            backend = p._runtime.store.backend
            for job_id, start_ts, end_ts in (
                ("s1", "2026-08-01T00:00:00Z", "2026-08-01T00:00:10Z"),
                ("s2", "2026-08-01T00:01:00Z", "2026-08-01T00:01:20Z"),
                ("s3", "2026-08-01T00:02:00Z", "2026-08-01T00:03:00Z"),
            ):
                job = Job("train", job_id)
                p.enqueue(job)
                attempt_no = backend.mark_job_running(
                    job.uid, execution_id=f"r_{job_id}.1", run_id=f"r_{job_id}", started_at=start_ts,
                )
                backend.commit_job_settlement(
                    job_uid=job.uid,
                    outcome="success",
                    attempt_no=attempt_no,
                    execution_id=f"r_{job_id}.1",
                    run_id=f"r_{job_id}",
                    started_at=start_ts,
                    settled_at=end_ts,
                    job_snapshot=job.to_dict(),
                    result={"message": None, "exception": None, "metadata": {}, "annotations": {}},
                    success_metadata={
                        "job_snapshot": job.to_dict(),
                        # 刻意置空 annotations 以验证旧记录兼容归类为 execution 且不抛 KeyError
                        "result": {"message": None, "exception": None, "metadata": {}, "annotations": {}},
                        "run_id": f"r_{job_id}",
                        "attempt_no": attempt_no,
                        "started_at": start_ts,
                        "settled_at": end_ts,
                    },
                )

            # 缺失 started_at 的成功 Attempt（仅测试跳过逻辑，不污染 train 的 P50）
            j_nostart = Job("eval", "e_nostart")
            p.enqueue(j_nostart)
            att_no = backend.mark_job_running(
                j_nostart.uid, execution_id="r_ns.1", run_id="r_ns", started_at="2026-08-01T00:00:00Z",
            )
            backend.commit_job_settlement(
                job_uid=j_nostart.uid,
                outcome="success",
                attempt_no=att_no,
                execution_id="r_ns.1",
                run_id="r_ns",
                started_at=None,
                settled_at="2026-08-01T00:00:05Z",
                job_snapshot=j_nostart.to_dict(),
                result={"message": None, "exception": None, "metadata": {}, "annotations": {}},
                success_metadata={
                    "job_snapshot": j_nostart.to_dict(),
                    "result": {"message": None, "exception": None, "metadata": {}, "annotations": {}},
                    "run_id": "r_ns",
                    "attempt_no": att_no,
                    "started_at": None,
                    "settled_at": "2026-08-01T00:00:05Z",
                },
            )

            # 时钟回拨样本（eval_skew）验证 max(0.0, ...)
            j_skew = Job("eval", "e_skew")
            p.enqueue(j_skew)
            att_skew = backend.mark_job_running(
                j_skew.uid, execution_id="r_sk.1", run_id="r_sk", started_at="2026-08-01T00:05:00Z",
            )
            backend.commit_job_settlement(
                job_uid=j_skew.uid,
                outcome="success",
                attempt_no=att_skew,
                execution_id="r_sk.1",
                run_id="r_sk",
                started_at="2026-08-01T00:05:10Z",
                settled_at="2026-08-01T00:05:00Z",
                job_snapshot=j_skew.to_dict(),
                result={"message": None, "exception": None, "metadata": {}, "annotations": {}},
                success_metadata={
                    "job_snapshot": j_skew.to_dict(),
                    "result": {"message": None, "exception": None, "metadata": {}, "annotations": {}},
                    "run_id": "r_sk",
                    "attempt_no": att_skew,
                    "started_at": "2026-08-01T00:05:10Z",
                    "settled_at": "2026-08-01T00:05:00Z",
                },
            )

            # 2) 手工标记 succeeded（manual）
            j_manual = Job("train", "manual_ok")
            p.enqueue(j_manual)
            with p.edit(dry_run=False, note="verified") as tx:
                tx.replace(j_manual.uid, j_manual, status="succeeded")

            # 3) 待运行 pending
            p.enqueue(Job("train", "p1"))

            # 4) 在途 running：一个有 train 历史基线（已跑 5s，P50=20s -> ETA ~15.0s）；一个无历史基线（fresh_type -> ETA=N/A）
            p.register_task("fresh_type", dummy_handler)
            j_run_known = Job("train", "r_known")
            j_run_unknown = Job("fresh_type", "r_unknown")
            p.enqueue(j_run_known)
            p.enqueue(j_run_unknown)
            backend.mark_job_running(
                j_run_known.uid, execution_id="r_live.1", run_id="r_live", started_at="2026-08-01T01:00:00Z",
            )
            backend.mark_job_running(
                j_run_unknown.uid, execution_id="r_live.2", run_id="r_live", started_at="2026-08-01T01:00:02Z",
            )

            # 5) 失败作业（带超长多行错误信息测试单行截断）
            long_msg = "line1\n" + ("E" * 200)
            j_fail = Job("train", "f1")
            p.enqueue(j_fail)
            with p.edit(dry_run=False, note=long_msg) as tx:
                tx.set_status(j_fail.uid, "failed", recursive=False)

            # 固定报告当前时钟为 2026-08-01T01:00:05Z
            monkeypatch.setattr(report_mod, "utc_now_iso", lambda: "2026-08-01T01:00:05Z")

            with StateSnapshot.open(db_path, mode="copy") as snap:
                report = StateSnapshot.report(snap)

            assert "Summary (total: 10):" in report
            assert "pending:   1" in report
            assert "running:   2" in report
            assert "succeeded: 6 (execution: 5, manual: 1)" in report
            assert "failed:    1" in report
            # train 历史耗时（n=3, Avg=30.0s, P50=20.0s, Max=1m 00.0s）
            assert "train: pending=1, running=1, succeeded=4, failed=1 (total=7)" in report
            assert "success duration (n=3): Avg=30.0s, P50=20.0s, Max=1m 00.0s" in report
            # eval 时钟回拨取 0.00s 且缺失 started_at 被跳过（n=1）
            assert "eval: pending=0, running=0, succeeded=2, failed=0 (total=2)" in report
            assert "success duration (n=1): Avg=0.00s, P50=0.00s, Max=0.00s" in report
            # fresh_type 无历史基线
            assert "fresh_type: pending=0, running=1, succeeded=0, failed=0 (total=1) | success duration: N/A" in report
            # running ETA
            assert "train::r_known (task_type=train, attempt=1): elapsed=5.00s, ETA=~15.0s (P50=20.0s)" in report
            assert "fresh_type::r_unknown (task_type=fresh_type, attempt=1): elapsed=3.00s, ETA=N/A (no history)" in report
            # failed 截断摘要
            assert "train::f1:" in report
            assert "..." in report
            assert ("E" * 200) not in report

    @pytest.mark.parametrize("backend", ["memory", "sqlite"])
    def test_dual_snapshot_comparison_report(self, tmp_path, backend):
        db_path = tmp_path / f"report_diff_{backend}.db"
        with TaskLite(
            backend=backend,
            state_path=db_path if backend == "sqlite" else None,
        ) as p:
            p.register_task("process", retry_then_succeed_handler)
            p.register_task("worker", terminal_result_handler)

            # 预先制造 1 个旧 failed、1 个将被移除的 succeeded、2 个待运行的 pending
            p.enqueue(Job("worker", "old_fail", payload={"mode": "failure"}))
            p.enqueue(Job("worker", "to_remove", payload={"mode": "success"}))
            with in_process_workers(p):
                p.run(install_signals=False)

            p.enqueue(Job("process", "will_retry_ok", max_retries_per_run=5))
            p.enqueue(Job("worker", "will_fail", payload={"mode": "failure"}))

            # 运行前使用 mode="copy" 拍快照（不持锁不占句柄）
            before = StateSnapshot.open(p, mode="copy")
            try:
                # 区间变更：新增 1 个 pending，移除 to_remove，并执行 run()
                with p.edit(dry_run=False) as tx:
                    tx.remove("worker::to_remove", entries=("successes",), recursive=False)
                p.enqueue(Job("worker", "new_ok", payload={"mode": "success"}))
                with in_process_workers(p):
                    p.run(install_signals=False)
                # 再新增 1 个留队 pending
                p.enqueue(Job("worker", "new_pending", payload={"mode": "success"}))

                after = StateSnapshot.open(p, mode="copy")
                try:
                    diff_report = StateSnapshot.report(before, after)

                    assert "Comparison: before -> after" in diff_report
                    # before: total=4 (pending=2, running=0, succeeded=1, failed=1)
                    # after:  total=5 (pending=1, running=0, succeeded=2, failed=2)
                    assert "Summary (total: 5, +1):" in diff_report
                    assert "pending:   1 (-1)" in diff_report
                    assert "succeeded: 2 (+1)" in diff_report
                    assert "failed:    2 (+1)" in diff_report

                    # 状态转移汇总
                    assert "pending -> succeeded: 1" in diff_report
                    assert "pending -> failed: 1" in diff_report
                    assert "(new) -> succeeded: 1" in diff_report
                    assert "(new) -> pending: 1" in diff_report
                    assert "succeeded -> (removed): 1" in diff_report

                    # 新增 Attempt 统计：will_retry_ok(2 retry + 1 success) + will_fail(1 failure) + new_ok(1 success) = 5
                    assert "New Attempts (5): success=2, failure=1, retry=2" in diff_report
                    assert "success duration (n=2): total=" in diff_report

                    # 本次区间内新进入 failed 仅包含 worker::will_fail，不包含旧的 worker::old_fail
                    newly_failed_section = diff_report.split("Newly Failed Jobs (1):")[1].split("By Task Type:")[0]
                    assert "worker::will_fail (pending -> failed): 本次失败" in newly_failed_section
                    assert "worker::old_fail" not in newly_failed_section

                    # 底部完整 Failed Jobs 列表则包含两个当前失败作业
                    all_failed_section = diff_report.split("Failed Jobs (2):")[1]
                    assert "worker::old_fail" in all_failed_section
                    assert "worker::will_fail" in all_failed_section

                    # 自对比报告应全为零增量
                    same_report = StateSnapshot.report(after, after)
                    assert "Summary (total: 5, +0):" in same_report
                    assert "(no state transitions)" in same_report
                    assert "New Attempts (0): success=0, failure=0, retry=0 | success duration: N/A" in same_report
                    assert "Newly Failed Jobs (0):" in same_report
                finally:
                    after.close()
            finally:
                before.close()

