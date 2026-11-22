"""基于统一 PathPolicy 的 FileSpec 单次原子解析测试（真实盘与 FakeFilesystemAdapter）。"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.store import StateStore
from tasklite.exceptions import TaskLiteError
from tasklite.models.artifact import FileSpec, resolve_file_spec
from tasklite.models.job import Job
from tasklite.utils.artifact import EntryInspection, FilesystemAdapter, PathPolicy, TargetType


def _record_success(
    store: StateStore,
    backend: InMemoryStateBackend,
    uid: str,
    artifacts: dict[str, FileSpec | str],
    *,
    run_id: str = "run_test",
):
    task_type, job_id = uid.split("::", 1)
    backend.enqueue_jobs([Job(task_type, job_id).to_dict()])
    execution_id = f"{run_id}.{backend.load_consistent_snapshot()['job_sequences'].get(uid, 0) + 1}"
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


class _MemoryAdapter(FilesystemAdapter):
    """纯内存适配器，用于验证 resolve_file_spec 100% 走 FilesystemAdapter、零直调 os.path.exists。"""

    def __init__(self, *, is_windows: bool = True) -> None:
        self.is_windows = is_windows
        self.entries: dict[str, EntryInspection] = {}
        self.inspect_calls: list[str] = []

    def _key(self, path: str) -> str:
        return path.lower() if self.is_windows else path

    def path_lexists(self, path: str) -> bool:
        entry = self.entries.get(self._key(path))
        return entry is not None and entry.entry_class != "missing"

    def realpath(self, path: str) -> str:
        return path

    def inspect_entry(self, path: str) -> EntryInspection:
        self.inspect_calls.append(path)
        return self.entries.get(self._key(path), EntryInspection("missing"))

    def recover_leaf_case(self, canonical_parent: str, leaf_name: str) -> str:
        sep = "\\" if self.is_windows else "/"
        return f"{canonical_parent.rstrip(sep)}{sep}{leaf_name}"

    def delete_target(
        self,
        target: str,
        *,
        allow_broken_leaf_symlink: bool,
        revalidate: Callable[[], None],
        authorize: Callable[[str, TargetType], bool] | None,
    ) -> bool:
        return False


def test_resolve_plain_relative_and_absolute_paths(tmp_path):
    """普通相对路径基于 store.base_dir 解析，绝对路径保持原样。"""
    store = StateStore(InMemoryStateBackend(), base_dir=tmp_path / "base")

    rel_spec = FileSpec("sub/file.txt", role="output")
    resolved_rel = store.resolve_file_spec(rel_spec, depends_on=())
    assert resolved_rel.path == str((tmp_path / "base" / "sub" / "file.txt").resolve())
    assert resolved_rel.role == "output"

    abs_target = (tmp_path / "other" / "abs.txt").resolve()
    abs_spec = FileSpec(abs_target, role="cache", delete_on_success=True)
    resolved_abs = store.resolve_file_spec(abs_spec, depends_on=())
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
        {"report": FileSpec(out_file, role="output")},
    )

    ref = FileSpec.from_output(
        "parent::p1",
        "report",
        role="input",
        delete_on_success=True,
        delete_on_failure=True,
    )
    resolved = store.resolve_file_spec(ref, depends_on=["parent::p1"])
    assert resolved.path == str(out_file)
    assert resolved.role == "input"
    assert resolved.delete_on_success is True
    assert resolved.delete_on_failure is True


def test_resolve_output_ref_rejects_missing_direct_dependency(tmp_path):
    """上游 producer 不在当前任务直接 depends_on 时拒绝。"""
    store = StateStore(InMemoryStateBackend(), base_dir=tmp_path)
    ref = FileSpec.from_output("parent::p1", "report", role="input")

    with pytest.raises(TaskLiteError, match="not in explicit direct depends_on"):
        store.resolve_file_spec(ref, depends_on=["other::p2"])


def test_resolve_output_ref_rejects_unpublished_output(tmp_path):
    """上游 producer 未发布指定具名产物或无成功记录时拒绝。"""
    store = StateStore(InMemoryStateBackend(), base_dir=tmp_path)
    ref = FileSpec.from_output("parent::p1", "missing_out", role="input")

    with pytest.raises(TaskLiteError, match="did not publish output 'missing_out'"):
        store.resolve_file_spec(ref, depends_on=["parent::p1"])


def test_resolve_input_role_missing_file_raises_file_not_found(tmp_path):
    """role='input' 无论来自普通路径还是 output_ref，磁盘缺失时均抛出 FileNotFoundError。"""
    backend = InMemoryStateBackend()
    store = StateStore(backend, base_dir=tmp_path)

    plain_missing = FileSpec("not_exist.txt", role="input")
    with pytest.raises(FileNotFoundError, match="Missing input file"):
        store.resolve_file_spec(plain_missing, depends_on=())

    missing_artifact = tmp_path / "deleted.csv"
    _record_success(
        store,
        backend,
        "parent::p1",
        {"lost": str(missing_artifact)},
    )
    ref_missing = FileSpec.from_output("parent::p1", "lost", role="input")
    with pytest.raises(FileNotFoundError, match="Missing input file"):
        store.resolve_file_spec(ref_missing, depends_on=["parent::p1"])


def test_resolve_reflects_latest_store_state_without_stale_cache(tmp_path):
    """store 的 successes 动态更新后立即反映最新产物路径。"""
    backend = InMemoryStateBackend()
    store = StateStore(backend, base_dir=tmp_path)
    ref = FileSpec.from_output("parent::p1", "dynamic_out", role="output")

    with pytest.raises(TaskLiteError, match="did not publish output"):
        store.resolve_file_spec(ref, depends_on=["parent::p1"])

    first_path = tmp_path / "first.dat"
    first_path.write_text("v1", encoding="utf-8")
    _record_success(
        store,
        backend,
        "parent::p1",
        {"dynamic_out": str(first_path)},
    )
    resolved_first = store.resolve_file_spec(ref, depends_on=["parent::p1"])
    assert resolved_first.path == str(first_path)

    second_path = tmp_path / "second.dat"
    second_path.write_text("v2", encoding="utf-8")
    _record_success(
        store,
        backend,
        "parent::p1",
        {"dynamic_out": str(second_path)},
    )
    resolved_second = store.resolve_file_spec(ref, depends_on=["parent::p1"])
    assert resolved_second.path == str(second_path)


def test_resolve_file_spec_uses_filesystem_adapter_without_os_path_exists(monkeypatch):
    """输入存在性判定与清理安全校验 100% 经由 FilesystemAdapter，绝不绕过调用 os.path.exists。"""
    import os

    monkeypatch.setattr(
        os.path,
        "exists",
        lambda _p: pytest.fail("resolve_file_spec 不得绕过 FilesystemAdapter 直调 os.path.exists"),
    )
    is_win = os.name == "nt"
    base = "C:\\virtual_work" if is_win else "/virtual_work"
    sep = "\\" if is_win else "/"
    adapter = _MemoryAdapter(is_windows=is_win)
    adapter.entries[adapter._key(base)] = EntryInspection("directory")
    adapter.entries[adapter._key(f"{base}{sep}in.csv")] = EntryInspection("file")
    adapter.entries[adapter._key(f"{base}{sep}broken.csv")] = EntryInspection("broken_symlink")
    policy = PathPolicy(base_dir=base, protected_roots=[f"{base}{sep}state"], adapter=adapter)

    # 虚拟存在文件作为 input 通过
    bound_in = resolve_file_spec(FileSpec("in.csv", role="input"), path_policy=policy)
    assert bound_in.path == f"{base}{sep}in.csv"

    # 虚拟断链作为 input 必须报 FileNotFoundError
    with pytest.raises(FileNotFoundError, match="Missing input file"):
        resolve_file_spec(FileSpec("broken.csv", role="input"), path_policy=policy)

    # 带清理开关指向受保护目录时在单次解析即拒绝
    with pytest.raises(ValueError, match="protected root"):
        resolve_file_spec(
            FileSpec("state/db.sqlite", role="output", delete_existing=True),
            path_policy=policy,
        )
    assert adapter.inspect_calls


def test_store_merges_explicit_path_policy_with_extra_protected_roots(tmp_path):
    """StateStore 同时接收 path_policy 与额外 protected_roots 时合并而不静默丢弃。"""
    state_db = tmp_path / "state.db"
    extra_root = tmp_path / "extra_protected"
    base_policy = PathPolicy(base_dir=tmp_path, state_path=state_db)
    store = StateStore(
        InMemoryStateBackend(),
        path_policy=base_policy,
        protected_roots=(extra_root,),
    )
    assert store.is_path_protected(state_db) is True
    assert store.is_path_protected(extra_root / "secret.bin") is True
