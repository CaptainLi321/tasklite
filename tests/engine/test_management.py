"""Run 外管理与只读快照验收。"""

from __future__ import annotations

import multiprocessing as mp
from pathlib import Path
import sqlite3

import pytest

import tasklite.backend.sqlite_backend as sqlite_backend_module
from tasklite import Job, StateSnapshot, TaskLite, TaskLiteError, TaskResult
from tasklite.testing import running
from tasklite.utils.lockfile import release_lock, try_acquire_lock


def _dummy_handler(job, ctx):
    return TaskResult.success()


def _make_app(backend: str, tmp_path: Path) -> TaskLite:
    state_path = tmp_path / "state.db" if backend == "sqlite" else None
    app = TaskLite(backend=backend, state_path=state_path)
    app.register_task("work", _dummy_handler)
    return app


@pytest.fixture(params=["sqlite", "memory"])
def app(request, tmp_path):
    instance = _make_app(request.param, tmp_path)
    try:
        yield instance
    finally:
        instance.close()


def _status(app: TaskLite, uid: str) -> dict:
    with StateSnapshot.open(app) as snapshot:
        record = snapshot.get_job(uid)
        assert record is not None
        return record


def _blocked_management_worker(
    state_path: str,
    ready: object,
    result_path: str,
) -> None:
    app = TaskLite(backend="sqlite", state_path=state_path)
    try:
        app.register_task("work", _dummy_handler)
        ready.set()
        try:
            with app.edit(dry_run=False) as tx:
                tx.set_status("work::a", "failed", recursive=False)
        except BaseException as exc:
            Path(result_path).write_text(type(exc).__name__, encoding="utf-8")
        else:
            Path(result_path).write_text("no-error", encoding="utf-8")
    finally:
        app.close()


class TestEditStatusAndAtomicity:
    """图修整事务状态流转、递归闭包与异常原子性。"""

    @pytest.mark.parametrize("target_status", ["pending", "failed"])
    def test_recursive_three_level_chain_is_symmetric(self, app, target_status):
        jobs = [
            Job("work", "a"),
            Job("work", "b", depends_on=["work::a"]),
            Job("work", "c", depends_on=["work::b"]),
        ]
        for job in jobs:
            assert app.enqueue(job)

        with app.edit(dry_run=False) as tx:
            tx.replace("work::a", jobs[0], status="succeeded")
            tx.set_status("work::b", "failed", recursive=False)
            tx.replace("work::c", jobs[2], status="succeeded")

        with app.edit(dry_run=False) as tx:
            tx.set_status("work::a", target_status, recursive=True)

        assert [_status(app, job.uid)["status"] for job in jobs] == [
            target_status,
            target_status,
            target_status,
        ]
        with StateSnapshot.open(app) as snapshot:
            assert list(snapshot.iter_attempts()) == []

    def test_snapshot_read_failure_aborts_without_mutation(self, app, monkeypatch):
        app.enqueue(Job("work", "a"))

        def fail_read():
            raise OSError("management snapshot unavailable")

        monkeypatch.setattr(
            app._runtime.store.backend,
            "load_consistent_snapshot",
            fail_read,
        )
        with pytest.raises(OSError, match="snapshot unavailable"):
            with app.edit(dry_run=False) as tx:
                tx.set_status("work::a", "failed", recursive=True)

        monkeypatch.undo()
        assert _status(app, "work::a")["status"] == "pending"

    def test_unknown_uid_and_duplicate_add_raise_public_error(self, app):
        with pytest.raises(TaskLiteError, match="does not exist"):
            with app.edit(dry_run=False) as tx:
                tx.set_status("work::missing", "failed", recursive=False)

        with app.edit(dry_run=False) as tx:
            tx.add(Job("work", "same", payload={"version": 1}), status="succeeded")

        with pytest.raises(TaskLiteError, match="already exists"):
            with app.edit(dry_run=False) as tx:
                tx.add(Job("work", "same", payload={"version": 2}), status="succeeded")

    def test_invalid_arguments_are_rejected(self, app):
        with pytest.raises(TaskLiteError, match="dry_run must be a bool"):
            app.edit(dry_run="yes")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError, match="note must be a str or None"):
            app.edit(note=123)  # type: ignore[arg-type]

        app.enqueue(Job("work", "a"))
        with app.edit(dry_run=True) as tx:
            with pytest.raises(TaskLiteError, match="set_status"):
                tx.set_status("work::a", "succeeded", recursive=False)  # type: ignore[arg-type]
            with pytest.raises(TaskLiteError, match="recursive must be a bool"):
                tx.set_status("work::a", "failed", recursive="yes")  # type: ignore[arg-type]

    def test_sqlite_apply_abort_rolls_back_entire_recursive_batch(
        self,
        tmp_path,
        monkeypatch,
    ):
        app = _make_app("sqlite", tmp_path)
        try:
            for job in (
                Job("work", "a"),
                Job("work", "b", depends_on=["work::a"]),
                Job("work", "c", depends_on=["work::b"]),
            ):
                app.enqueue(job)

            real_dumps = sqlite_backend_module.dumps
            calls = {"count": 0}

            def abort_third_dump(value):
                calls["count"] += 1
                if calls["count"] == 3:
                    raise RuntimeError("injected SQLite abort")
                return real_dumps(value)

            monkeypatch.setattr(sqlite_backend_module, "dumps", abort_third_dump)
            with pytest.raises(RuntimeError, match="injected SQLite abort"):
                with app.edit(dry_run=False) as tx:
                    tx.set_status("work::a", "failed", recursive=True)
            monkeypatch.undo()

            with StateSnapshot.open(app) as snapshot:
                assert {
                    job["job_uid"]: job["status"]
                    for job in snapshot.iter_jobs()
                } == {
                    "work::a": "pending",
                    "work::b": "pending",
                    "work::c": "pending",
                }
        finally:
            app.close()


def _snapshot_attempts(app: TaskLite):
    with StateSnapshot.open(app) as snapshot:
        yield from snapshot.iter_attempts()


class TestCursor:
    """游标始终以后端最新提交值为准。"""

    def test_empty_string_and_deletion_use_latest_commit(self, tmp_path):
        first = _make_app("sqlite", tmp_path)
        second = _make_app("sqlite", tmp_path)
        try:
            first.set_cursor("checkpoint", "")
            assert second.get_cursor("checkpoint") == ""

            second.set_cursor("checkpoint", None)
            assert first.get_cursor("checkpoint") is None

            first.set_cursor("checkpoint", "42")
            assert second.get_cursor("checkpoint") == "42"
        finally:
            first.close()
            second.close()

    def test_management_guard_rejects_cursor_access_during_run(self, tmp_path):
        app = _make_app("memory", tmp_path)
        try:
            with running(app):
                with pytest.raises(TaskLiteError, match="outside run"):
                    app.get_cursor("key")
                with pytest.raises(TaskLiteError, match="outside run"):
                    app.set_cursor("key", "value")
        finally:
            app.close()


class TestEditRemoveAndSequence:
    """移除所选分区条目不触碰队列（未选时）、游标、attempts 或编号进度。"""

    def test_sqlite_cross_table_delete_rolls_back_on_trigger_error(self, tmp_path):
        app = _make_app("sqlite", tmp_path)
        try:
            app.enqueue(Job("work", "item"))
            app.run()
            app.enqueue(Job("work", "other"))
            assert app._state_path is not None
            with sqlite3.connect(app._state_path) as conn:
                conn.execute(
                    "CREATE TRIGGER reject_success_delete "
                    "BEFORE DELETE ON successes "
                    "WHEN OLD.job_uid = 'work::item' "
                    "BEGIN "
                    "SELECT RAISE(ABORT, 'injected successes delete failure'); "
                    "END"
                )

            with pytest.raises(
                sqlite3.IntegrityError, match="injected successes delete failure"
            ):
                with app.edit(dry_run=False) as tx:
                    tx.remove("work::other", entries=("queue",), recursive=False)
                    tx.remove("work::item", entries=("successes",), recursive=False)

            with StateSnapshot.open(app) as snapshot:
                job = snapshot.get_job("work::item")
                assert job is not None and job["status"] == "succeeded"
                other = snapshot.get_job("work::other")
                assert other is not None and other["status"] == "pending"
                assert len(list(snapshot.iter_attempts(job_uid="work::item"))) == 1
        finally:
            app.close()

    def test_granular_remove_preserves_attempts_cursors_and_sequence(self, app):
        app.set_cursor("cursor", "value")
        app.enqueue(Job("work", "item"))
        app.run()
        first_attempt_no = next(iter(_snapshot_attempts(app)))["attempt_no"]

        with app.edit(dry_run=False) as tx:
            deleted = tx.remove("work::item", entries=("successes",), recursive=False)
        assert deleted == 1
        assert app.get_cursor("cursor") == "value"
        with StateSnapshot.open(app) as snapshot:
            # attempts 严格 append-only，不受 remove 影响
            assert len(list(snapshot.iter_attempts(job_uid="work::item"))) == 1
            assert list(snapshot.iter_jobs(status="succeeded")) == []

        app.enqueue(Job("work", "item"))
        app.run()
        attempts = list(_snapshot_attempts(app))
        assert [a["attempt_no"] for a in attempts] == [first_attempt_no, first_attempt_no + 1]

    def test_validation_is_stable(self, app):
        with app.edit(dry_run=True) as tx:
            for entries, error, match in (
                ([], TaskLiteError, "entries must not be empty"),
                (["unknown"], TaskLiteError, "unknown or forbidden"),
                (["attempts"], TaskLiteError, "unknown or forbidden"),
            ):
                with pytest.raises(error, match=match):
                    tx.remove("work::item", entries=entries, recursive=False)  # type: ignore[arg-type]
            with pytest.raises(TaskLiteError, match="uid"):
                tx.remove("", entries=("successes",), recursive=False)
            with pytest.raises(TaskLiteError, match="canonical"):
                tx.remove("bad_uid", entries=("successes",), recursive=False)


class TestManagementAuthorizationSurface:
    """授权查询与撤销沿用同一 Run 外管理边界。"""

    def test_list_and_revoke_authorization(self, tmp_path):
        app = _make_app("sqlite", tmp_path)
        try:
            path = tmp_path / "approved.txt"
            app._runtime.store.record_authorization(path, "file")
            assert [item["path"] for item in app.list_authorizations()] == [
                str(path.resolve())
            ]
            assert app.revoke_authorization(path) is True
            assert app.list_authorizations() == []
        finally:
            app.close()

    def test_authorization_management_is_rejected_during_run(self, tmp_path):
        app = _make_app("memory", tmp_path)
        try:
            with running(app):
                with pytest.raises(TaskLiteError, match="outside run"):
                    app.list_authorizations()
                with pytest.raises(TaskLiteError, match="outside run"):
                    app.revoke_authorization("approved.txt")
        finally:
            app.close()


class TestCrossProcessManagementMutex:
    """库级锁必须排斥其他进程的管理写，并在释放后立即可重试。"""

    def test_other_process_is_rejected_then_released_lock_allows_write(self, tmp_path):
        app = _make_app("sqlite", tmp_path)
        result_path = tmp_path / "worker-result.txt"
        fd = None
        process = None
        try:
            app.enqueue(Job("work", "a"))
            assert app._state_path is not None
            fd = try_acquire_lock(app._state_path, timeout=0)
            assert fd is not None

            context = mp.get_context("spawn")
            ready = context.Event()
            process = context.Process(
                target=_blocked_management_worker,
                args=(str(app._state_path), ready, str(result_path)),
            )
            process.start()
            assert ready.wait(timeout=10)
            process.join(timeout=10)
            assert process.exitcode == 0
            assert result_path.read_text(encoding="utf-8") == "TaskLiteError"

            release_lock(fd)
            fd = None
            with app.edit(dry_run=False) as tx:
                tx.set_status("work::a", "failed", recursive=False)
            assert _status(app, "work::a")["status"] == "failed"
        finally:
            if fd is not None:
                release_lock(fd)
            if process is not None and process.is_alive():
                process.terminate()
                process.join(timeout=5)
            app.close()
