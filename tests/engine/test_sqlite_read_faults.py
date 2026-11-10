"""SQLite 初始化与读取故障在公开入口保留原始证据。"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tasklite import Job, TaskLite, TaskResult
from tasklite.backend import SQLiteStateBackend


def _observe_sqlite(monkeypatch, denied=None):
    connect = sqlite3.connect
    errors = []
    calls = []

    class Connection(sqlite3.Connection):
        def execute(self, *args, **kwargs):
            try:
                return super().execute(*args, **kwargs)
            except sqlite3.Error as error:
                errors.append((error, error.__traceback__, error.__cause__, error.__context__))
                raise

    def observed_connect(*args, **kwargs):
        calls.append((args, kwargs.copy()))
        kwargs['factory'] = Connection
        connection = connect(*args, **kwargs)
        if denied is not None:
            connection.set_authorizer(
                lambda action, first, second, database, source:
                sqlite3.SQLITE_DENY if denied(action, first, second) else sqlite3.SQLITE_OK
            )
        return connection

    monkeypatch.setattr(sqlite3, 'connect', observed_connect)
    return errors, calls


def _assert_original(caught, errors):
    assert len(errors) == 1
    original, traceback, cause, context = errors[0]
    assert caught.value is original
    assert caught.value.__cause__ is cause
    assert caught.value.__context__ is context
    frames = []
    current = caught.value.__traceback__
    while current is not None:
        frames.append(current)
        current = current.tb_next
    assert traceback in frames


def _success(job, ctx):
    return TaskResult.success()


@pytest.mark.parametrize('stage', ['preflight', 'schema_create', 'wal', 'corrupt'])
def test_constructor_sqlite_fault_is_original_and_preserves_database(tmp_path, monkeypatch, stage):
    path = tmp_path / 'state.db'
    if stage == 'preflight':
        SQLiteStateBackend(path)
    elif stage == 'corrupt':
        path.write_bytes(b'precious non-sqlite bytes')
    before = path.read_bytes() if path.exists() else None
    denied = {
        'preflight': lambda action, first, second: action == sqlite3.SQLITE_PRAGMA and first == 'user_version',
        'schema_create': lambda action, first, second: action == sqlite3.SQLITE_CREATE_INDEX,
        'wal': lambda action, first, second: action == sqlite3.SQLITE_PRAGMA and first == 'journal_mode',
        'corrupt': None,
    }[stage]
    errors, calls = _observe_sqlite(monkeypatch, denied)
    with pytest.raises(sqlite3.DatabaseError) as caught:
        TaskLite(backend='sqlite', state_path=path)
    _assert_original(caught, errors)
    if before is not None:
        assert path.read_bytes() == before
        assert len(calls) == 1
        assert calls[0][1]['uri'] is True
        assert 'mode=ro' in calls[0][0][0]
    else:
        with sqlite3.connect(path) as connection:
            assert connection.execute('SELECT name FROM sqlite_master').fetchall() == []


def test_constructor_parent_file_preserves_original_filesystem_error(tmp_path, monkeypatch):
    blocker = tmp_path / 'business'
    blocker.write_bytes(b'precious')
    mkdir = Path.mkdir
    errors = []

    def observed_mkdir(path, *args, **kwargs):
        try:
            return mkdir(path, *args, **kwargs)
        except OSError as error:
            errors.append((error, error.__traceback__, error.__cause__, error.__context__))
            raise

    monkeypatch.setattr(Path, 'mkdir', observed_mkdir)
    with pytest.raises(FileExistsError) as caught:
        TaskLite(backend='sqlite', state_path=blocker / 'state.db')
    _assert_original(caught, errors)
    assert blocker.read_bytes() == b'precious'


@pytest.mark.parametrize('entry,table', [
    ('run', 'successes'), ('run', 'failed'), ('run', 'cursors'),
    ('enqueue', 'queue'), ('enqueue_dependency', 'successes'), ('enqueue_dependency', 'failed'),
    ('trusted_baseline', 'attempts'),
])
def test_public_read_fault_is_original_without_task_failure(tmp_path, monkeypatch, entry, table):
    with TaskLite(backend='sqlite', state_path=tmp_path / 'state.db') as pipeline:
        pipeline.register_task('work', _success)
        job = Job('work', 'one', trigger='always')
        if entry == 'trusted_baseline':
            pipeline.enqueue(job)
            pipeline.run(install_signals=False)
            pipeline.enqueue(job)
        backend = pipeline._runtime.store.backend
        before_queue = backend.load_queue_jobs()
        before_successes = backend.load_successes()
        errors, _ = _observe_sqlite(
            monkeypatch,
            lambda action, first, second: action == sqlite3.SQLITE_READ and first == table,
        )
        with pytest.raises(sqlite3.DatabaseError) as caught:
            if entry in ('run', 'trusted_baseline'):
                pipeline.run(install_signals=False)
            elif entry == 'enqueue':
                pipeline.enqueue(job)
            else:
                pipeline.enqueue(Job('work', 'child', depends_on=['work::parent']))
        _assert_original(caught, errors)
        monkeypatch.undo()
        assert backend.load_queue_jobs() == before_queue
        assert backend.load_successes() == before_successes
        assert backend.load_failed() == {}
        assert len(backend.load_settled_attempts(job.uid)) == (1 if entry == 'trusted_baseline' else 0)


@pytest.mark.parametrize('reader,table', [
    ('load_successes', 'successes'), ('load_failed', 'failed'),
    ('load_failure_payloads', 'failed'), ('load_cursors', 'cursors'),
    ('load_queue', 'queue'), ('load_latest_succeeded_finished_at', 'attempts'),
    ('get_engine_metadata', 'engine_metadata'),
])
def test_backend_read_fault_preserves_original_error(tmp_path, monkeypatch, reader, table):
    backend = SQLiteStateBackend(tmp_path / 'state.db')
    errors, _ = _observe_sqlite(
        monkeypatch,
        lambda action, first, second: action == sqlite3.SQLITE_READ and first == table,
    )
    args = ('work::one',) if reader in ('load_latest_succeeded_finished_at', 'get_engine_metadata') else ()
    with pytest.raises(sqlite3.DatabaseError) as caught:
        getattr(backend, reader)(*args)
    _assert_original(caught, errors)


@pytest.mark.parametrize('stage', ['stat', 'readonly_connect', 'read_connect'])
def test_filesystem_fault_preserves_existing_exception_chain(tmp_path, monkeypatch, stage):
    path = tmp_path / 'state.db'
    backend = SQLiteStateBackend(path)
    cause = LookupError('底层已有原因')
    original = PermissionError('无法读取状态库')
    original.__cause__ = cause
    errors = []

    def denied(*args, **kwargs):
        try:
            raise original
        except OSError as error:
            errors.append((error, error.__traceback__, error.__cause__, error.__context__))
            raise

    if stage == 'stat':
        stat = Path.stat

        def denied_stat(target, *args, **kwargs):
            if target == path:
                return denied()
            return stat(target, *args, **kwargs)

        monkeypatch.setattr(Path, 'stat', denied_stat)
    else:
        monkeypatch.setattr(sqlite3, 'connect', denied)
    with pytest.raises(PermissionError) as caught:
        if stage == 'read_connect':
            backend.load_successes()
        elif stage == 'stat':
            backend._preflight_schema()
        else:
            TaskLite(backend='sqlite', state_path=path)
    _assert_original(caught, errors)
    assert caught.value.__cause__ is cause


@pytest.mark.parametrize('reader,table,column', [
    ('load_successes', 'successes', 'metadata'),
    ('load_failed', 'failed', 'metadata'),
    ('load_failure_payloads', 'failed', 'payload'),
    ('load_queue', 'queue', 'job_snapshot'),
])
def test_corrupt_json_read_preserves_decoder_error(tmp_path, monkeypatch, reader, table, column):
    import json
    import tasklite.backend.sqlite_backend as sqlite_backend

    backend = SQLiteStateBackend(tmp_path / 'state.db')
    backend.enqueue_jobs([Job('work', 'one').to_dict()])
    backend.commit_job_failure('work::failed', {'message': '业务失败'}, {'key': 'value'})
    with backend._get_conn() as connection:
        if table == 'successes':
            connection.execute("INSERT INTO successes VALUES ('work::success', 'broken')")
        else:
            connection.execute(f"UPDATE {table} SET {column} = 'broken'")
    loads = sqlite_backend.loads
    errors = []

    def observed_loads(value):
        try:
            return loads(value)
        except json.JSONDecodeError as error:
            errors.append((error, error.__traceback__, error.__cause__, error.__context__))
            raise

    monkeypatch.setattr(sqlite_backend, 'loads', observed_loads)
    with pytest.raises(json.JSONDecodeError) as caught:
        getattr(backend, reader)()
    _assert_original(caught, errors)
