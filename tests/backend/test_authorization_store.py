"""持久化授权存储与判定接口契约测试。"""
import os
from pathlib import Path
import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend, TARGET_SCHEMA_VERSION
from tasklite.engine.store import StateStore


@pytest.fixture(params=["sqlite", "memory"])
def backend(request, tmp_path):
    if request.param == "sqlite":
        db_path = tmp_path / "test_auth.db"
        return SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
    else:
        return InMemoryStateBackend(schema_version=TARGET_SCHEMA_VERSION)


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

    assert not backend.is_path_authorized(f1)
    assert not backend.is_path_authorized(sub)

    backend.record_authorization(f1, "file")
    assert backend.is_path_authorized(f1)
    # 子文件不能被文件授权覆盖
    assert not backend.is_path_authorized(f1 + ".bak")

    backend.record_authorization(d1, "directory")
    assert backend.is_path_authorized(d1)
    assert backend.is_path_authorized(sub)
    assert not backend.is_path_authorized(outside)


def test_revoke_authorization(backend, tmp_path):
    f1 = str((tmp_path / "a.txt").resolve())
    backend.record_authorization(f1, "file")
    assert backend.is_path_authorized(f1)

    assert backend.revoke_authorization(f1) is True
    assert not backend.is_path_authorized(f1)
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
