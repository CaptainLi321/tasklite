"""公开拒绝与底层自然异常的传播契约。"""
from pathlib import Path
import sqlite3
import traceback

import pytest

from tasklite import Job, StateSnapshot, TaskLite, TaskLiteError, TaskResult
import tasklite.snapshot as snapshot_module


def _handle(job, ctx):
    return TaskResult.success()


def _assert_original_error(caught, original, last_frame):
    assert caught.value is original
    assert traceback.extract_tb(caught.value.__traceback__)[-1].name == last_frame
    assert caught.value.__cause__ is None


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("error_type", [TypeError, ValueError, KeyError])
def test_user_trigger_exception_keeps_identity_and_traceback(tmp_path, backend, error_type):
    original = error_type("用户触发器异常")

    def trigger(ctx):
        raise original

    with TaskLite(backend=backend, state_path=tmp_path / "state.db") as pipeline:
        pipeline.register_trigger("custom", trigger)
        pipeline.register_task("t", _handle, trigger="custom")
        pipeline.enqueue(Job("t", "a"))
        with pytest.raises(error_type) as caught:
            pipeline.run(install_signals=False)
        _assert_original_error(caught, original, "trigger")
        with StateSnapshot(pipeline) as snapshot:
            assert snapshot.get_job("t::a")["status"] == "pending"
            assert list(snapshot.iter_attempts()) == []


@pytest.mark.parametrize("error_type", [sqlite3.OperationalError, RuntimeError])
@pytest.mark.parametrize("operation", ["connect", "schema"])
def test_snapshot_natural_exception_keeps_identity_and_traceback(tmp_path, monkeypatch, error_type, operation):
    path = tmp_path / "state.db"
    with TaskLite(backend="sqlite", state_path=path):
        pass
    original = error_type("读取故障")

    def fail(*args, **kwargs):
        raise original

    if operation == "connect":
        monkeypatch.setattr(snapshot_module.sqlite3, "connect", fail)
    else:
        monkeypatch.setattr(snapshot_module, "_check_schema", fail)
    with pytest.raises(error_type) as caught:
        StateSnapshot(path)
    _assert_original_error(caught, original, "fail")


def test_management_sqlite_exception_keeps_identity_and_traceback(tmp_path, monkeypatch):
    original = sqlite3.OperationalError("游标读取故障")

    def fail(key):
        raise original

    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db") as pipeline:
        monkeypatch.setattr(pipeline._runtime.store.backend, "get_cursor", fail)
        with pytest.raises(sqlite3.OperationalError) as caught:
            pipeline.get_cursor("watermark")
        _assert_original_error(caught, original, "fail")


def test_python_binding_path_conversion_and_mapping_errors_remain_natural():
    with pytest.raises(TypeError):
        Job("t", "a", unknown=True)
    with pytest.raises(TypeError):
        TaskLite(backend="sqlite", state_path=object())
    with TaskLite(backend="memory") as pipeline:
        with pytest.raises(TypeError):
            pipeline.enqueue()
        with pytest.raises(KeyError):
            Job.from_dict({})
    with pytest.raises(TypeError):
        Path(object())


def test_snapshot_active_configuration_rejections_use_public_error(tmp_path):
    with pytest.raises(TaskLiteError):
        StateSnapshot(tmp_path)
    with pytest.raises(TaskLiteError):
        StateSnapshot(tmp_path / "missing.db")
    empty = tmp_path / "empty.db"
    sqlite3.connect(empty).close()
    with pytest.raises(TaskLiteError):
        StateSnapshot(empty)


def test_input_declaration_rejection_and_natural_filesystem_error_are_distinct(tmp_path, monkeypatch):
    from tasklite import FileSpec
    from tasklite.testing import fake_ctx
    import tasklite.models.context as context_module

    ctx = fake_ctx(Job("t", "a"), base_dir=tmp_path)
    with pytest.raises(TaskLiteError, match="Missing input file"):
        ctx.declare_file("source", FileSpec("missing", role="input"))

    original = FileNotFoundError("文件系统读取故障")

    def fail(path):
        raise original

    monkeypatch.setattr(context_module.os.path, "exists", fail)
    with pytest.raises(FileNotFoundError) as caught:
        ctx.declare_file("source", FileSpec("missing", role="input"))
    _assert_original_error(caught, original, "fail")


@pytest.mark.parametrize("error_type", [TypeError, ValueError, KeyError])
@pytest.mark.parametrize("callback", ["fetch", "group"])
def test_discovery_callback_exception_keeps_identity_and_traceback(tmp_path, error_type, callback):
    from tasklite.testing import fake_ctx
    from tasklite.wrappers.discovery import DiscoveryHandler

    original = error_type("发现回调故障")

    def fail(*args):
        raise original

    handler = DiscoveryHandler(
        fail if callback == "fetch" else lambda *args: [],
        lambda item: item,
        lambda *args: None,
        "process",
        group_key_func=fail if callback == "group" else None,
    )
    job = Job("discover", "a")
    with pytest.raises(error_type) as caught:
        handler(job, fake_ctx(job, base_dir=tmp_path))
    _assert_original_error(caught, original, "fail")


@pytest.mark.parametrize("error_type", [TypeError, ValueError, KeyError])
def test_http_policy_exception_keeps_identity_and_traceback(error_type):
    from tasklite.wrappers.http import HttpPolicy, HttpResponse, http_guard

    original = error_type("策略回调故障")

    def fail(*args):
        raise original

    guard = http_guard(policy=HttpPolicy(status_classifier=fail))
    with pytest.raises(error_type) as caught:
        guard.check_response(HttpResponse(status_code=200, headers={}, body=b""))
    _assert_original_error(caught, original, "fail")
