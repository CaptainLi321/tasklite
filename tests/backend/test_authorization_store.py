"""持久化授权存储与判定接口契约测试。"""
import os
from pathlib import Path
import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.store import StateStore


@pytest.fixture(params=["sqlite", "memory"])
def backend(request, tmp_path):
    if request.param == "sqlite":
        db_path = tmp_path / "test_auth.db"
        return SQLiteStateBackend(db_path)
    else:
        return InMemoryStateBackend()


def test_record_and_list_authorizations(backend, tmp_path):
    f1 = str((tmp_path / "a.txt").resolve())
    d1 = str((tmp_path / "data").resolve())
    backend.record_authorization(f1, "file", authorized_at="2026-11-08T01:00:00Z")
    backend.record_authorization(d1, "directory", authorized_at="2026-11-08T02:00:00Z")

    records = backend.list_authorizations()
    assert len(records) == 2
    assert records[0]["path"] == f1
    assert records[0]["target_type"] == "file"
    assert records[0]["authorized_at"] == "2026-11-08T01:00:00Z"
    assert records[1]["path"] == d1
    assert records[1]["target_type"] == "directory"
    assert records[1]["authorized_at"] == "2026-11-08T02:00:00Z"


def test_is_path_authorized_file_and_directory(backend, tmp_path):
    f1 = str((tmp_path / "a.txt").resolve())
    d1 = str((tmp_path / "data").resolve())
    sub = str((tmp_path / "data" / "sub" / "file.csv").resolve())
    outside = str((tmp_path / "data_other" / "file.csv").resolve())

    assert not backend.is_path_authorized(f1, "file")
    assert not backend.is_path_authorized(sub, "file")

    backend.record_authorization(f1, "file")
    assert backend.is_path_authorized(f1, "file")
    # 子文件不能被文件授权覆盖
    assert not backend.is_path_authorized(f1 + ".bak", "file")

    backend.record_authorization(d1, "directory")
    assert backend.is_path_authorized(d1, "directory")
    assert backend.is_path_authorized(sub, "file")
    assert not backend.is_path_authorized(outside, "file")


def test_revoke_authorization(backend, tmp_path):
    f1 = str((tmp_path / "a.txt").resolve())
    backend.record_authorization(f1, "file")
    assert backend.is_path_authorized(f1, "file")

    assert backend.revoke_authorization(f1) is True
    assert not backend.is_path_authorized(f1, "file")
    assert backend.revoke_authorization(f1) is False


def test_store_check_or_request_authorization(backend, tmp_path, monkeypatch):
    store = StateStore(backend=backend, base_dir=tmp_path)
    target = str((tmp_path / "work" / "out.txt").resolve())

    # 1. 默认无授权且非自动同意 -> 拒绝
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    assert store.check_or_request_authorization(target) is False
    assert not store.is_path_authorized(target)

    # 2. authorizer 回调拒绝
    store.authorizer = lambda p, t: False
    assert store.check_or_request_authorization(target) is False
    assert not store.is_path_authorized(target)

    # 3. authorizer 回调同意 -> 持久化
    store.authorizer = lambda p, t: True
    assert store.check_or_request_authorization(target) is True
    assert store.is_path_authorized(target)

    # 4. 再次检查命中已有授权
    store.authorizer = None
    assert store.check_or_request_authorization(target) is True

    # 5. 自动同意环境变量生效
    target2 = str((tmp_path / "work" / "out2.txt").resolve())
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    assert store.check_or_request_authorization(target2) is True
    assert store.is_path_authorized(target2)


@pytest.mark.parametrize("path_form", ["relative", "parent-link", "broken-link", "current", "parent", "root"])
def test_store_authorization_paths_share_fixed_base_normalization(
    backend, tmp_path, monkeypatch, path_form,
):
    """授权询问、记录、查询和撤销共用固定基准及父路径规范化，文件系统根保持为根。"""
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    base = tmp_path / "base"
    other = tmp_path / "other"
    real = tmp_path / "real"
    for directory in (base, other, real):
        directory.mkdir()
    (base / "alias").symlink_to(real, target_is_directory=True)
    (base / "broken").symlink_to(real / "missing")
    cases = {
        "relative": ("nested/../file", base / "file"),
        "parent-link": ("alias/file", real / "file"),
        "broken-link": ("broken", base / "broken"),
        "current": (".", base),
        "parent": ("..", tmp_path),
        "root": (base.anchor, Path(base.anchor)),
    }
    path, expected = cases[path_form]
    store = StateStore(backend=backend, base_dir=base)
    monkeypatch.chdir(other)
    calls = []
    store.authorizer = lambda *args: calls.append(args) or True
    target_type = "directory" if expected.is_dir() else "file"
    assert store.check_or_request_authorization(path) is True
    assert calls == [(str(expected), target_type)]
    assert store.is_path_authorized(path)
    store.record_authorization(path, target_type)
    record, = store.list_authorizations()
    assert record["path"] == str(expected)
    assert store.revoke_authorization(path) is True
    assert store.list_authorizations() == []


@pytest.mark.parametrize("initial", ["file", "missing"])
def test_file_scope_requires_new_authorization_for_directory(backend, tmp_path, monkeypatch, initial):
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    if initial == "file":
        target.write_text("file", encoding="utf-8")
    store = StateStore(backend=backend, base_dir=tmp_path)
    calls = []
    store.authorizer = lambda *args: calls.append(args) or True
    assert store.check_or_request_authorization(target) is True
    if target.exists():
        target.unlink()
    target.mkdir()
    (target / "child").write_text("preserve", encoding="utf-8")
    assert not backend.is_path_authorized(str(target), "directory")
    assert not store.is_path_authorized(target)
    store.authorizer = lambda *args: calls.append(args) or None
    assert store.check_or_request_authorization(target) is None
    assert backend.list_authorizations()[0]["target_type"] == "file"
    store.authorizer = lambda *args: calls.append(args) or True
    assert store.check_or_request_authorization(target) is True
    assert [kind for _, kind in calls] == ["file", "directory", "directory"]
    assert store.is_path_authorized(target / "child")


def test_parent_link_change_does_not_reuse_previous_target_authorization(backend, tmp_path, monkeypatch):
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(first, target_is_directory=True)
    store = StateStore(backend=backend, base_dir=tmp_path)
    calls = []
    store.authorizer = lambda *args: calls.append(args) or True
    assert store.check_or_request_authorization(alias / "item") is True
    alias.unlink()
    alias.symlink_to(second, target_is_directory=True)
    assert not store.is_path_authorized(alias / "item")
    assert store.check_or_request_authorization(alias / "item") is True
    assert calls == [(str(first / "item"), "file"), (str(second / "item"), "file")]


def test_type_change_inside_authorizer_cannot_expand_recorded_file_scope(backend, tmp_path, monkeypatch):
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    store = StateStore(backend=backend, base_dir=tmp_path)
    calls = []

    def authorize(path, kind):
        calls.append((path, kind))
        if kind == "file":
            target.mkdir()
            return True
        return None

    store.authorizer = authorize
    assert store.check_or_request_authorization(target) is None
    assert calls == [(str(target), "file"), (str(target), "directory")]
    assert not store.is_path_authorized(target)
    assert store.list_authorizations()[0]["target_type"] == "file"


def test_auto_approval_records_directory_scope_after_file_replacement(backend, tmp_path, monkeypatch):
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    target = tmp_path / "target"
    target.write_text("file", encoding="utf-8")
    store = StateStore(backend=backend, base_dir=tmp_path)
    assert store.check_or_request_authorization(target) is True
    assert store.list_authorizations()[0]["target_type"] == "file"
    target.unlink()
    target.mkdir()
    assert store.check_or_request_authorization(target) is True
    assert store.list_authorizations()[0]["target_type"] == "directory"
