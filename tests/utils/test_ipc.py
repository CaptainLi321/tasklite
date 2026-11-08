"""v2 ArtifactJournal 产物清单与管道声明管理测试。

覆盖：路径与沙盒、声明登记与读取、管道实时上报、存在性校验、
清理消费侧沙盒复检与生命周期模式清理。
"""

import os
from pathlib import Path

import pytest

from tasklite.exceptions import _ArtifactIOError
from tasklite.models.artifact import ArtifactDeclaration
from tasklite.utils.ipc import (
    ArtifactCleanupMode,
    ArtifactJournal,
    PIPE_MSG_INPUT,
    WorkerPipeMessage,
    create_worker_pipe,
)


def _inject_raw_declaration(
    journal: ArtifactJournal,
    uid: str,
    path: str,
    *,
    delete_on_failure: bool = False,
    delete_on_success: bool = False,
    sandbox: bool = True,
) -> None:
    """注入原始声明对象（供消费侧沙盒复检测试验证越界拒绝）。"""
    decl = object.__new__(ArtifactDeclaration)
    object.__setattr__(decl, "path", path)
    object.__setattr__(decl, "name", None)
    object.__setattr__(decl, "delete_existing", False)
    object.__setattr__(decl, "delete_on_failure", delete_on_failure)
    object.__setattr__(decl, "delete_on_success", delete_on_success)
    object.__setattr__(decl, "sandbox", sandbox)
    prev = journal._artifacts.get(uid, ())
    journal._artifacts[uid] = (*prev, decl)


class TestArtifactJournalPathAndSandbox:
    def test_null_byte_rejection(self, tmp_path):
        with pytest.raises(ValueError, match="null byte"):
            ArtifactJournal.resolve_and_validate_path("bad\x00path.txt", tmp_path)

    def test_sandbox_single_root_valid_relative(self, tmp_path):
        root = tmp_path / "outputs"
        root.mkdir()
        resolved = ArtifactJournal.resolve_and_validate_path("a/b/file.txt", root, sandbox=True)
        assert Path(resolved).is_relative_to(root.resolve())

    def test_sandbox_multi_root_matching(self, tmp_path):
        first_root = tmp_path / "root_first"
        second_root = tmp_path / "root_second"
        first_root.mkdir()
        second_root.mkdir()
        file_in_second = second_root / "dest.csv"
        resolved = ArtifactJournal.resolve_and_validate_path(
            str(file_in_second), [first_root, second_root], sandbox=True
        )
        assert Path(resolved) == file_in_second.resolve()

    def test_sandbox_escape_rejected(self, tmp_path):
        root = tmp_path / "outputs"
        root.mkdir()
        outside = tmp_path / "outside.txt"
        with pytest.raises(ValueError, match="outside output_roots"):
            ArtifactJournal.resolve_and_validate_path(str(outside), root, sandbox=True)

    def test_sandbox_bypass(self, tmp_path):
        root = tmp_path / "outputs"
        root.mkdir()
        outside = tmp_path / "outside.txt"
        resolved = ArtifactJournal.resolve_and_validate_path(str(outside), root, sandbox=False)
        assert Path(resolved) == outside.resolve()


class TestArtifactJournalDeclarations:
    def test_record_and_verify_artifacts(self, tmp_path):
        journal = ArtifactJournal(tmp_path / "ipc", output_roots=tmp_path)
        uid = "job_out"
        out_file = tmp_path / "final.data"
        cache_file = tmp_path / "temp.cache"

        journal.record_artifact(
            uid, ArtifactDeclaration(str(out_file), delete_on_failure=True)
        )
        journal.record_artifact(
            uid,
            ArtifactDeclaration(
                str(cache_file), delete_on_failure=True, delete_on_success=True
            ),
        )

        # 产物未创建时校验失败
        declarations = journal.read_artifacts(uid)
        err = journal.verify_artifacts(declarations)
        assert err is not None and "Missing output" in err

        # 创建正式产物后，尽管 cache 缺失，校验仍通过（cache 跳过存在性校验）
        out_file.write_text("done")
        assert journal.verify_artifacts(declarations) is None

    def test_record_and_read_input_paths(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "job_in"
        data_file = tmp_path / "input.txt"
        data_file.write_text("hello input")

        assert journal.record_input_file(uid, str(data_file)) == str(data_file)
        journal.record_input_file(uid, "/other/path")

        assert journal.read_inputs(uid) == [str(data_file), "/other/path"]

    def test_pipe_sender_forwards_declarations(self, tmp_path):
        receiver, sender = create_worker_pipe(
            job_uid="t::pipe",
            execution_id="exec_pipe_1",
        )
        journal = ArtifactJournal(
            tmp_path / "ipc",
            output_roots=tmp_path,
            pipe_sender=sender,
        )
        out_path = str(tmp_path / "out.txt")
        in_path = str(tmp_path / "in.txt")
        decl = ArtifactDeclaration(out_path, name="report", delete_on_failure=True)
        journal.record_artifact("t::pipe", decl)
        journal.record_input_file("t::pipe", in_path)
        sender.close()

        messages = receiver.drain_to_eof(timeout=3.0)
        receiver.close()

        assert journal.read_artifacts("t::pipe") == (decl,)
        assert len(messages) == 1
        assert isinstance(messages[0], WorkerPipeMessage)
        assert messages[0].kind == PIPE_MSG_INPUT
        assert messages[0].payload == {"path": in_path}


class TestCleanupSandboxRecheck:
    """删除时复检声明范围，越界或保护目录冲突必须保留现场并报错。"""

    def _make_journal(self, tmp_path, with_output_root=True):
        ipc_dir = tmp_path / "ipc"
        ipc_dir.mkdir()
        roots = None
        if with_output_root:
            roots = tmp_path / "outputs"
            roots.mkdir()
        return ArtifactJournal(ipc_dir, output_roots=roots), ipc_dir, roots

    def test_failure_cleanup_refuses_out_of_sandbox_tree(self, tmp_path):
        """拒绝沙盒外目录并保留声明供诊断。"""
        journal, _, _ = self._make_journal(tmp_path)
        victim = tmp_path / "victim_dir"
        victim.mkdir()
        (victim / "precious.txt").write_text("data")
        uid = "t::poison"
        _inject_raw_declaration(journal, uid, str(victim), delete_on_failure=True)

        with pytest.raises(ValueError):
            journal.cleanup(uid, ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert victim.exists(), "沙盒外目录绝不能被 rmtree"
        assert (victim / "precious.txt").exists()
        assert len(journal.read_artifacts(uid)) == 1

    def test_failure_cleanup_still_removes_in_sandbox_entries(self, tmp_path):
        """沙盒内合法声明的半成品文件与目录照常清理（复检不误伤合法清理）。"""
        journal, _, roots = self._make_journal(tmp_path)
        broken_file = roots / "broken.txt"
        broken_file.write_text("half")
        broken_dir = roots / "half_baked"
        broken_dir.mkdir()
        (broken_dir / "sub.txt").write_text("sub")
        uid = "t::normal"
        _inject_raw_declaration(journal, uid, str(broken_file), delete_on_failure=True)
        _inject_raw_declaration(journal, uid, str(broken_dir), delete_on_failure=True)

        journal.cleanup(uid, ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert not broken_file.exists()
        assert not broken_dir.exists()

    def test_success_cleanup_refuses_out_of_sandbox_cache(self, tmp_path):
        """成功清理对沙盒外 cache 声明拒绝 unlink。"""
        journal, _, _ = self._make_journal(tmp_path)
        victim = tmp_path / "outside.cache"
        victim.write_text("data")
        uid = "t::cache"
        _inject_raw_declaration(
            journal, uid, str(victim), delete_on_failure=True, delete_on_success=True
        )

        with pytest.raises(ValueError):
            journal.cleanup(uid, ArtifactCleanupMode.SUCCESS)

        assert victim.exists(), "沙盒外 cache 文件绝不能被 unlink"

    def test_cleanup_rejects_protected_ipc_dir_without_output_roots(self, tmp_path):
        """IPC 目录始终受保护，即使没有配置输出根。"""
        journal, ipc_dir, _ = self._make_journal(tmp_path, with_output_root=False)
        inside = ipc_dir / "scratch.tmp"
        inside.write_text("data")
        victim = tmp_path / "outside.txt"
        victim.write_text("data")
        uid = "t::no_roots"
        _inject_raw_declaration(
            journal, uid, str(inside), delete_on_failure=True, delete_on_success=True
        )
        _inject_raw_declaration(
            journal, uid, str(victim), delete_on_failure=True, delete_on_success=True
        )

        with pytest.raises(ValueError, match="protected root"):
            journal.cleanup(uid, ArtifactCleanupMode.SUCCESS)

        assert inside.exists()
        assert victim.exists()

    def test_failure_cleanup_rejects_dotdot_escape_and_symlink(self, tmp_path):
        """``..`` 穿越与符号链接间接逃逸的声明路径同样拒绝清理。"""
        journal, _, roots = self._make_journal(tmp_path)
        target = tmp_path / "real_target"
        target.mkdir()
        (target / "f.txt").write_text("data")
        os.symlink(target, roots / "link")
        uid = "t::escape"
        _inject_raw_declaration(
            journal,
            uid,
            str(roots / "link" / ".." / ".." / "real_target"),
            delete_on_failure=True,
        )

        with pytest.raises(ValueError):
            journal.cleanup(uid, ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert target.exists(), "经符号链接与 .. 逃逸出沙盒的路径必须拒绝清理"


class TestArtifactJournalCleanupModes:
    def test_cleanup_pre_submit(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        uid = "job_clean_pre"
        _inject_raw_declaration(journal, uid, "/tmp/out", delete_on_failure=True)
        journal.record_input_file(uid, "/tmp/in")

        assert len(journal.read_artifacts(uid)) == 1
        assert journal.read_inputs(uid) == ["/tmp/in"]

        journal.cleanup(uid, ArtifactCleanupMode.PRE_SUBMIT)

        assert journal.read_artifacts(uid) == ()
        assert journal.read_inputs(uid) == []

    def test_cleanup_success_mode(self, tmp_path):
        journal = ArtifactJournal(tmp_path / "ipc", output_roots=tmp_path)
        uid = "job_clean_succ"
        cache_file = tmp_path / "temp.cache"
        cache_file.write_text("cache")
        final_file = tmp_path / "final.txt"
        final_file.write_text("final")

        journal.record_artifact(
            uid,
            ArtifactDeclaration(
                str(cache_file), delete_on_failure=True, delete_on_success=True
            ),
        )
        journal.record_artifact(
            uid, ArtifactDeclaration(str(final_file), delete_on_failure=True)
        )

        journal.cleanup(uid, ArtifactCleanupMode.SUCCESS)

        assert not cache_file.exists(), "cache file must be cleaned on success"
        assert final_file.exists(), "product file must be retained on success"
        assert journal.read_artifacts(uid) == ()

    def test_cleanup_failure_mode(self, tmp_path):
        journal = ArtifactJournal(tmp_path / "ipc", output_roots=tmp_path)
        uid = "job_clean_fail"
        broken_file = tmp_path / "broken.txt"
        broken_file.write_text("half written")
        broken_dir = tmp_path / "broken_dir"
        broken_dir.mkdir()
        (broken_dir / "sub.txt").write_text("sub")
        keep_file = tmp_path / "keep.txt"
        keep_file.write_text("preserve")

        journal.record_artifact(
            uid, ArtifactDeclaration(str(broken_file), delete_on_failure=True)
        )
        journal.record_artifact(
            uid, ArtifactDeclaration(str(broken_dir), delete_on_failure=True)
        )
        journal.record_artifact(
            uid, ArtifactDeclaration(str(keep_file), delete_on_failure=False)
        )

        journal.cleanup(uid, ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert not broken_file.exists(), "cleanup=True file must be unlinked"
        assert not broken_dir.exists(), "cleanup=True directory must be rmtree'd"
        assert keep_file.exists(), "cleanup=False file must be kept"
        assert journal.read_artifacts(uid) == ()


class TestTypedArtifactJournal:
    def test_authorizations_deduplicate_identical_declaration(self, tmp_path):
        out_root = tmp_path / "out"
        out_root.mkdir()
        journal = ArtifactJournal(tmp_path / "ipc", output_roots=out_root)
        declaration = ArtifactDeclaration(
            str(out_root / "report"),
            name="report",
            delete_existing=True,
            delete_on_failure=True,
        )
        journal.record_artifact("t::1", declaration)
        journal.record_artifact("t::1", declaration)
        assert journal.read_artifacts("t::1") == (declaration,)

    def test_typed_record_rejects_conflict_and_noncanonical_parent_link(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        (root / "real").mkdir()
        (root / "link").symlink_to(root / "real", target_is_directory=True)
        journal = ArtifactJournal(tmp_path / "ipc", output_roots=root)
        first = ArtifactDeclaration(str(root / "a"), name="a")
        journal.record_artifact("t::1", first)
        with pytest.raises(ValueError):
            journal.record_artifact("t::1", ArtifactDeclaration(str(root / "b"), name="a"))
        with pytest.raises(ValueError):
            journal.record_artifact("t::1", ArtifactDeclaration(str(root / "link" / "c")))
        assert journal.read_artifacts("t::1") == (first,)

    def test_named_publication_rechecks_path_without_following_leaf_link(self, tmp_path):
        root = tmp_path / "out"
        root.mkdir()
        report = root / "report"
        report.write_text("output")
        journal = ArtifactJournal(tmp_path / "ipc", output_roots=root)
        journal.record_artifact("t::1", ArtifactDeclaration(str(report), name="report"))
        report.unlink()
        report.symlink_to(tmp_path / "external")
        with pytest.raises(ValueError, match="symlink"):
            journal.read_artifacts("t::1")

    def test_typed_pipe_send_failure_propagates(self, tmp_path):
        receiver, sender = create_worker_pipe(
            job_uid="t::1",
            execution_id="exec_1",
        )
        sender.close()
        receiver.close()
        journal = ArtifactJournal(tmp_path / "ipc", pipe_sender=sender)
        declaration = ArtifactDeclaration(str(tmp_path / "report"), name="report")
        journal.record_artifact("t::1", declaration)
        assert journal.read_artifacts("t::1") == (declaration,)
        with pytest.raises(_ArtifactIOError):
            journal.record_input_file("t::1", str(tmp_path / "source"))


@pytest.mark.parametrize("success", [False, True])
def test_cleanup_custom_directory_obeys_explicit_authorization(tmp_path, success):
    journal = ArtifactJournal(tmp_path / "ipc", output_roots=tmp_path / "outputs")
    directory = tmp_path / "external" / "scratch"
    directory.mkdir(parents=True)
    (directory / "part").write_text("temporary")
    declaration = ArtifactDeclaration(
        str(directory), sandbox=False, delete_on_failure=True, delete_on_success=True,
    )
    journal.record_artifact("t::cleanup", declaration)

    mode = ArtifactCleanupMode.SUCCESS if success else ArtifactCleanupMode.FAILURE_OR_RETRY
    journal.cleanup("t::cleanup", mode)

    assert not directory.exists()
    assert journal.read_artifacts("t::cleanup") == ()


@pytest.mark.parametrize("success", [False, True])
def test_cleanup_io_failure_preserves_declarations(tmp_path, monkeypatch, success):
    from tasklite.utils.artifact import ArtifactPathPolicy

    journal = ArtifactJournal(tmp_path / "ipc", output_roots=tmp_path / "outputs")
    target = tmp_path / "outputs" / "scratch"
    target.parent.mkdir()
    target.write_text("temporary")
    declaration = ArtifactDeclaration(
        str(target), delete_on_failure=True, delete_on_success=True,
    )
    journal.record_artifact("t::cleanup", declaration)

    def denied(self, declaration, *, success):
        raise PermissionError("permission denied")

    monkeypatch.setattr(ArtifactPathPolicy, "cleanup", denied)
    mode = ArtifactCleanupMode.SUCCESS if success else ArtifactCleanupMode.FAILURE_OR_RETRY
    with pytest.raises(PermissionError, match="permission denied"):
        journal.cleanup("t::cleanup", mode)

    assert target.exists()
    assert journal.read_artifacts("t::cleanup") == (declaration,)
