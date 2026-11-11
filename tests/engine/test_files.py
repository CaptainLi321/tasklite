from __future__ import annotations

from pathlib import Path

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.files import FileSpecResolver
from tasklite.engine.store import StateStore
from tasklite.exceptions import TaskLiteError
from tasklite.models.artifact import ArtifactDeclaration, FileSpec
from tasklite.models.job import Job


def _record_success(store: StateStore, backend: InMemoryStateBackend, uid: str, artifacts: list[ArtifactDeclaration], *, run_id: str = "run_test"):
    task_type, job_id = uid.split("::", 1)
    backend.enqueue_jobs([Job(task_type, job_id).to_dict()])
    execution_id = f"{run_id}.{backend.get_attempt_sequence(uid) + 1}"
    attempt_no = backend.mark_job_running(
        uid,
        execution_id=execution_id,
        run_id=run_id,
        started_at="2026-01-01T00:00:00Z",
    )
    store.apply_success(
        uid,
        execution_id=execution_id,
        attempt_no=attempt_no,
        run_id=run_id,
        declared_artifacts=artifacts,
        result={},
    )


def test_resolve_plain_relative_and_absolute_paths(tmp_path):
    """普通相对路径基于 store.base_dir 解析，绝对路径保持原样。"""
    store = StateStore(InMemoryStateBackend(), base_dir=tmp_path / "base")
    resolver = FileSpecResolver(store)

    rel_spec = FileSpec("sub/file.txt", role="output")
    resolved_rel = resolver.resolve(rel_spec, depends_on=())
    assert resolved_rel.path == str((tmp_path / "base" / "sub" / "file.txt").resolve())
    assert resolved_rel.role == "output"

    abs_target = (tmp_path / "other" / "abs.txt").resolve()
    abs_spec = FileSpec(abs_target, role="cache", delete_on_success=True)
    resolved_abs = resolver.resolve(abs_spec, depends_on=())
    assert resolved_abs.path == str(abs_target)
    assert resolved_abs.role == "cache"
    assert resolved_abs.delete_on_success is True


def test_resolve_output_ref_success_and_preserves_attributes(tmp_path):
    """上游引用从直接依赖的 successes 中绑定路径并保留角色与清理开关。"""
    backend = InMemoryStateBackend()
    store = StateStore(backend, base_dir=tmp_path)
    out_file = tmp_path / "producer_out.csv"
    out_file.write_text("data", encoding="utf-8")

    _record_success(
        store,
        backend,
        "parent::p1",
        [ArtifactDeclaration(str(out_file), name="report")],
    )
    resolver = FileSpecResolver(store)

    ref = FileSpec.from_output(
        "parent::p1",
        "report",
        role="input",
        delete_on_success=True,
        delete_on_failure=True,
    )
    resolved = resolver.resolve(ref, depends_on=["parent::p1"])
    assert resolved.path == str(out_file)
    assert resolved.role == "input"
    assert resolved.delete_on_success is True
    assert resolved.delete_on_failure is True


def test_resolve_output_ref_rejects_missing_direct_dependency(tmp_path):
    """上游 producer 不在当前任务直接 depends_on 时拒绝。"""
    store = StateStore(InMemoryStateBackend(), base_dir=tmp_path)
    resolver = FileSpecResolver(store)
    ref = FileSpec.from_output("parent::p1", "report", role="input")

    with pytest.raises(TaskLiteError, match="not in explicit direct depends_on"):
        resolver.resolve(ref, depends_on=["other::p2"])


def test_resolve_output_ref_rejects_unpublished_output(tmp_path):
    """上游 producer 未发布指定具名产物或无成功记录时拒绝。"""
    store = StateStore(InMemoryStateBackend(), base_dir=tmp_path)
    resolver = FileSpecResolver(store)
    ref = FileSpec.from_output("parent::p1", "missing_out", role="input")

    with pytest.raises(TaskLiteError, match="did not publish output 'missing_out'"):
        resolver.resolve(ref, depends_on=["parent::p1"])


def test_resolve_input_role_missing_file_raises_file_not_found(tmp_path):
    """role='input' 无论来自普通路径还是 output_ref，磁盘缺失时均抛出 FileNotFoundError。"""
    backend = InMemoryStateBackend()
    store = StateStore(backend, base_dir=tmp_path)
    resolver = FileSpecResolver(store)

    plain_missing = FileSpec("not_exist.txt", role="input")
    with pytest.raises(FileNotFoundError, match="Missing input file"):
        resolver.resolve(plain_missing, depends_on=())

    missing_artifact = tmp_path / "deleted.csv"
    _record_success(
        store,
        backend,
        "parent::p1",
        [ArtifactDeclaration(str(missing_artifact), name="lost")],
    )
    ref_missing = FileSpec.from_output("parent::p1", "lost", role="input")
    with pytest.raises(FileNotFoundError, match="Missing input file"):
        resolver.resolve(ref_missing, depends_on=["parent::p1"])


def test_resolver_reflects_latest_store_state_without_stale_cache(tmp_path):
    """resolver 在 store 的 successes 动态更新后立即反映最新产物路径。"""
    backend = InMemoryStateBackend()
    store = StateStore(backend, base_dir=tmp_path)
    resolver = FileSpecResolver(store)
    ref = FileSpec.from_output("parent::p1", "dynamic_out", role="output")

    with pytest.raises(TaskLiteError, match="did not publish output"):
        resolver.resolve(ref, depends_on=["parent::p1"])

    first_path = tmp_path / "first.dat"
    first_path.write_text("v1", encoding="utf-8")
    _record_success(
        store,
        backend,
        "parent::p1",
        [ArtifactDeclaration(str(first_path), name="dynamic_out")],
    )
    resolved_first = resolver.resolve(ref, depends_on=["parent::p1"])
    assert resolved_first.path == str(first_path)

    second_path = tmp_path / "second.dat"
    second_path.write_text("v2", encoding="utf-8")
    _record_success(
        store,
        backend,
        "parent::p1",
        [ArtifactDeclaration(str(second_path), name="dynamic_out")],
    )
    resolved_second = resolver.resolve(ref, depends_on=["parent::p1"])
    assert resolved_second.path == str(second_path)
