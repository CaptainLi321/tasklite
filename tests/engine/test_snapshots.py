"""只读一致状态快照与 JSON 查询测试。"""
from __future__ import annotations

from contextlib import closing
import os
from pathlib import Path
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
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend, TARGET_SCHEMA_VERSION


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
    p = TaskLite(
        backend=backend_type,
        state_path=state_path,
    )
    p.register_task(Task("process", dummy_handler))
    p.register_task(Task("generate", dummy_handler))
    return p


class TestStateSnapshotOpeningAndClosing:
    """快照生命周期与打开/关闭行为。"""

    def test_open_via_path_and_tasklite_instance(self, tmp_path):
        db_path = tmp_path / "custom # % space.db"
        backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
        backend.enqueue_jobs([Job("process", "j1").to_dict()])

        # 从 Path 打开
        with StateSnapshot.open(db_path) as s1:
            assert s1.source == db_path
            assert not s1.closed
            assert not s1.is_closed
            job = s1.get_job("process::j1")
            assert job is not None
            assert job["status"] == "pending"

        assert s1.closed is True
        assert s1.is_closed is True
        with pytest.raises(TaskLiteError, match="source must be"):
            StateSnapshot.open(backend)
        with TaskLite(backend="sqlite", state_path=db_path) as sqlite_pipeline:
            with StateSnapshot.open(sqlite_pipeline) as sqlite_snapshot:
                assert sqlite_snapshot.get_job("process::j1") == job

        # 从 TaskLite 实例打开
        pipeline = TaskLite(backend="memory")
        pipeline.register_task(Task("process", dummy_handler))
        pipeline.enqueue(Job("process", "m1"))

        with StateSnapshot.open(pipeline) as s2:
            assert not s2.closed
            m_job = s2.get_job("process::m1")
            assert m_job is not None
            assert m_job["status"] == "pending"

        # 关闭快照不影响 TaskLite 实例
        assert s2.closed is True
        assert pipeline.get_cursor("nonexistent") is None

    def test_operations_on_closed_snapshot_raise_error(self, tmp_path):
        db_path = tmp_path / "closed.db"
        backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
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
        (TARGET_SCHEMA_VERSION, "ALTER TABLE attempts ADD COLUMN extra TEXT", "for 'attempts'"),
    ])
    def test_invalid_schema_closes_connection_and_preserves_database(
        self, tmp_path, monkeypatch, version, change, message,
    ):
        db_path = tmp_path / "invalid.db"
        backend = SQLiteStateBackend(db_path, schema_version=version)
        backend.enqueue_jobs([Job("process", "precious").to_dict()])
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

        # 在提交前打开快照 1
        with StateSnapshot.open(p) as s1:
            # 外部提交新任务与推进状态
            p.enqueue(Job("process", "j2", payload={"v": 2}))
            p.run()

            # s1 两次读取间即使外部已提交完成，仍观察打开时的初始快照
            j1 = s1.get_job("process::j1")
            assert j1 is not None
            assert j1["status"] == "pending"
            assert s1.get_job("process::j2") is None

            # 全量遍历同样只看到快照 1 打开时的任务
            jobs_s1 = list(s1.iter_jobs())
            assert len(jobs_s1) == 1
            assert jobs_s1[0]["job_uid"] == "process::j1"

        # 重新打开快照 2 观察新提交
        with StateSnapshot.open(p) as s2:
            j1_new = s2.get_job("process::j1")
            assert j1_new is not None
            assert j1_new["status"] == "succeeded"
            j2_new = s2.get_job("process::j2")
            assert j2_new is not None
            assert j2_new["status"] == "succeeded"

    def test_modifying_returned_dictionary_does_not_mutate_framework(self, pipeline_fixture):
        p = pipeline_fixture
        p.enqueue(Job("process", "j1", payload={"nested": {"count": 10}}))

        with StateSnapshot.open(p) as s:
            record = s.get_job("process::j1")
            assert record is not None
            # 变异外层与嵌套配置字典
            record["status"] = "corrupted"
            record["job"]["payload"]["nested"]["count"] = 999

            # 再次查询不受污染
            second = s.get_job("process::j1")
            assert second is not None
            assert second["status"] == "pending"
            assert second["job"]["payload"]["nested"]["count"] == 10


class TestJobRecordStructureAndPriority:
    """Job 与 Attempt 记录结构分层、FileSpec 样本与优先级。"""

    def test_record_structure_and_filespec_sample(self, tmp_path):
        p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        p.register_task(Task("generate", dummy_handler))
        p.register_task(Task("process", dummy_handler))

        result_path = str((tmp_path / "results" / "processed.csv").resolve())
        report_path = str((tmp_path / "results" / "report.csv").resolve())

        # 生产者
        producer = Job(
            "generate",
            "report-001",
            files={"report": FileSpec(report_path, role="output")},
        )
        # 消费者：上游具名产物引用 + 本地输出
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
            # 顶层字段分层
            assert rec["job_uid"] == consumer.uid
            assert rec["status"] == "pending"
            assert rec["result"] is None
            assert rec["run_id"] is None
            assert rec["attempt_no"] is None
            assert rec["started_at"] is None
            assert rec["settled_at"] is None

            # job["files"] 配置快照保留上游引用配置与绝对化普通路径
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

            # 外层 files 仅包含具有绝对 path 的已掌握声明
            assert "result" in rec["files"]
            assert rec["files"]["result"]["path"] == result_path
            assert "source" not in rec["files"]  # 尚未解析运行，无绝对 path

    def test_queue_pending_and_running_priority_over_old_terminal_status(self, pipeline_fixture):
        p = pipeline_fixture
        p.enqueue(Job("process", "j1"))
        p.run()

        # 已成功
        with StateSnapshot.open(p) as s1:
            j1 = s1.get_job("process::j1")
            assert j1 is not None
            assert j1["status"] == "succeeded"
            assert j1["result"] is not None

        # 重新入队进入 pending 态
        p.set_job_status("process::j1", "pending", recursive=False)

        with StateSnapshot.open(p) as s2:
            j1_requeued = s2.get_job("process::j1")
            assert j1_requeued is not None
            # 当前 pending 态优先于旧终态
            assert j1_requeued["status"] == "pending"
            # pending 没有旧 result，不附带上次执行身份
            assert j1_requeued["result"] is None
            assert j1_requeued["run_id"] is None
            assert j1_requeued["attempt_no"] is None
            assert j1_requeued["started_at"] is None
            assert j1_requeued["settled_at"] is None

    def test_manual_success_and_failure_attributions(self, pipeline_fixture):
        p = pipeline_fixture
        p.enqueue(Job("process", "j_manual_succ"))
        p.enqueue(Job("process", "j_manual_fail"))

        p.set_job_status(
            "process::j_manual_succ",
            "succeeded",
            recursive=False,
            note="manual approval",
        )
        p.set_job_status(
            "process::j_manual_fail",
            "failed",
            recursive=False,
            note="manual rejection",
        )

        with StateSnapshot.open(p) as s:
            succ = s.get_job("process::j_manual_succ")
            assert succ is not None
            assert succ["status"] == "succeeded"
            assert succ["run_id"] is None
            assert succ["attempt_no"] is None
            assert succ["started_at"] is None
            assert succ["settled_at"] is not None
            assert succ["result"]["annotations"]["completion_source"] == "manual"
            assert succ["result"]["annotations"]["note"] == "manual approval"

            fail = s.get_job("process::j_manual_fail")
            assert fail is not None
            assert fail["status"] == "failed"
            assert fail["run_id"] is None
            assert fail["attempt_no"] is None
            assert fail["started_at"] is None
            assert fail["settled_at"] is not None
            assert fail["result"]["annotations"]["completion_source"] == "manual"
            assert fail["result"]["message"] == "manual rejection"

    def test_nonexistent_job_returns_none(self, pipeline_fixture):
        with StateSnapshot.open(pipeline_fixture) as s:
            assert s.get_job("process::does_not_exist") is None
            assert s.get_job("invalid_format") is None


class TestQueriesAndFilters:
    """iter_jobs 与 iter_attempts 的过滤与排序。"""

    def test_sqlite_iter_jobs_streams_cursor_rows(self, tmp_path):
        p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        p.register_task(Task("process", dummy_handler))
        for job_id in ("succ", "pend", "fail"):
            p.enqueue(Job("process", job_id))
        p.run()
        p.set_job_status("process::pend", "pending", recursive=False)
        p.set_job_status("process::fail", "failed", recursive=False)

        class TrackedCursor:
            def __init__(self, cursor):
                self._cursor = cursor
                self.steps = 0

            def __iter__(self):
                return self

            def __next__(self):
                row = next(self._cursor)
                self.steps += 1
                return row

            def fetchone(self):
                return self._cursor.fetchone()

            def fetchall(self):
                raise AssertionError("iter_jobs must not materialize SQLite rows")

        class TrackedConnection:
            def __init__(self, conn):
                self._conn = conn
                self.cursors = []

            def execute(self, sql, params=()):
                cursor = TrackedCursor(self._conn.execute(sql, params))
                self.cursors.append(cursor)
                return cursor

        with StateSnapshot.open(p) as snapshot:
            tracked = TrackedConnection(snapshot._reader._conn)
            snapshot._reader._conn = tracked

            jobs = list(snapshot.iter_jobs())
            assert {job["job_uid"] for job in jobs} == {
                "process::succ",
                "process::pend",
                "process::fail",
            }

            before = sum(cursor.steps for cursor in tracked.cursors)
            iterator = snapshot.iter_jobs()
            first = next(iterator)
            assert first["job_uid"] == "process::pend"
            assert sum(cursor.steps for cursor in tracked.cursors) == before + 1
            iterator.close()

    def test_iter_jobs_filtering_and_uniqueness(self, pipeline_fixture):
        p = pipeline_fixture
        p.enqueue(Job("process", "j_succ"))
        p.enqueue(Job("process", "j_pend"))
        p.enqueue(Job("generate", "g_fail"))

        p.run()  # j_succ, j_pend, g_fail all run and succeed

        # 调整状态为不同终态与待处理
        p.set_job_status("process::j_pend", "pending", recursive=False)
        p.set_job_status("generate::g_fail", "failed", recursive=False)

        with StateSnapshot.open(p) as s:
            all_jobs = list(s.iter_jobs())
            assert len(all_jobs) == 3
            uids = {j["job_uid"] for j in all_jobs}
            assert uids == {"process::j_succ", "process::j_pend", "generate::g_fail"}

            pending_jobs = list(s.iter_jobs(status="pending"))
            assert len(pending_jobs) == 1
            assert pending_jobs[0]["job_uid"] == "process::j_pend"

            succeeded_jobs = list(s.iter_jobs(status="succeeded"))
            assert len(succeeded_jobs) == 1
            assert succeeded_jobs[0]["job_uid"] == "process::j_succ"

            failed_jobs = list(s.iter_jobs(status="failed"))
            assert len(failed_jobs) == 1
            assert failed_jobs[0]["job_uid"] == "generate::g_fail"

            process_jobs = list(s.iter_jobs(task_type="process"))
            assert {j["job_uid"] for j in process_jobs} == {
                "process::j_succ",
                "process::j_pend",
            }

            with pytest.raises(TaskLiteError, match="status must be one of"):
                list(s.iter_jobs(status="unknown"))  # type: ignore[arg-type]

    def test_iter_attempts_ordered_by_attempt_no(self, tmp_path):
        p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        p.register_task(Task("process", retry_then_succeed_handler))
        p.enqueue(Job("process", "j_retry", max_retries_per_run=5))
        p.run()

        with StateSnapshot.open(p) as s:
            attempts = list(s.iter_attempts(job_uid="process::j_retry"))
            assert len(attempts) == 3
            # 同 Job Attempt 编号严格升序
            attempt_nos = [a["attempt_no"] for a in attempts]
            assert attempt_nos == [1, 2, 3]

            assert attempts[0]["outcome"] == "retry"
            assert attempts[0]["result"]["message"] == "attempt 1 failed"
            assert attempts[1]["outcome"] == "retry"
            assert attempts[1]["result"]["message"] == "attempt 2 failed"
            assert attempts[2]["outcome"] == "success"
            assert attempts[2]["result"]["metadata"] == {"final": True}

            # 结构体统一含有时间、配置、文件分层
            for a in attempts:
                assert a["job_uid"] == "process::j_retry"
                assert "job" in a
                assert "files" in a
                assert a["settled_at"] is not None
                assert a["run_id"] is not None
