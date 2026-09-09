"""Tests for TaskContext.declare_output() output path sandbox (REQ-10)."""
from pathlib import Path
import pytest
from tasklite.models.context import TaskContext
from tasklite.models.job import Job
from tasklite.utils.ipc import ArtifactJournal

class TestOutputSandbox:
    """Output path sandbox (REQ-10) tests."""

    # Helpers

    @staticmethod
    def _ctx(output_root, job_id="j1", ipc_dir=None):
        job = Job("test", job_id, payload={})
        return TaskContext(
            job,
            set(),
            set(),
            {},
            output_root=output_root,
            ipc_dir=ipc_dir,
        )

    # Tests

    def test_relative_within_root(self, tmp_path):
        """Relative path within output_root → resolved to output_root/path, dirs created."""
        ctx = self._ctx(tmp_path)
        resolved = ctx.declare_output("output/file.jpg")
        assert resolved == str(tmp_path / "output/file.jpg")
        # Parent directory created
        assert (tmp_path / "output").is_dir()
    def test_absolute_within_root(self, tmp_path):
        """Absolute path within output_root → accepted, dirs created."""
        ctx = self._ctx(tmp_path)
        target = tmp_path / "abs/file.jpg"
        resolved = ctx.declare_output(str(target))
        assert resolved == str(target)
        assert target.parent.is_dir()
    def test_path_traversal_rejected(self, tmp_path):
        """Path traversal '../../etc/passwd' → raises ValueError."""
        ctx = self._ctx(tmp_path)
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output("../../etc/passwd")
    def test_path_traversal_absolute_rejected(self, tmp_path):
        """Absolute path '/etc/passwd' → raises ValueError."""
        ctx = self._ctx(tmp_path)
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output("/etc/passwd")
    def test_no_output_root_allows_any(self, tmp_path):
        """output_root=None → any path accepted without restriction."""
        ctx = self._ctx(None)
        # Should not raise regardless of path
        resolved = ctx.declare_output("/tmp/anything")
        assert resolved == "/tmp/anything"
    def test_auto_makedirs_created(self, tmp_path):
        """Deeply nested path → all parent directories created."""
        ctx = self._ctx(tmp_path)
        ctx.declare_output("a/b/c/d/file.txt")
        expected_dir = tmp_path / "a" / "b" / "c" / "d"
        assert expected_dir.is_dir()
    def test_path_exactly_equals_root(self, tmp_path):
        """Path exactly equals output_root → accepted via equality check."""
        ctx = self._ctx(tmp_path)
        resolved = ctx.declare_output(str(tmp_path))
        assert resolved == str(tmp_path)
    def test_trailing_slash(self, tmp_path):
        """Path with trailing slash 'subdir/' → accepted (directory path)."""
        ctx = self._ctx(tmp_path)
        resolved = ctx.declare_output("subdir/")
        # Trailing slash is normalized away by pathlib
        assert resolved == str(tmp_path / "subdir")
    def test_cleanup_flag_stored(self, tmp_path):
        """cleanup_on_fail=False stored correctly in the persisted outputs.jsonl."""
        ipc_dir = tmp_path / "ipc"
        ctx = self._ctx(tmp_path, ipc_dir=str(ipc_dir))
        resolved = ctx.declare_output("file.txt", cleanup_on_fail=False)
        assert resolved == str(tmp_path / "file.txt")
        outputs = ArtifactJournal(ipc_dir).read_outputs(ctx.job.uid)
        assert [(p, cl, k) for p, cl, k in outputs] == [(str(tmp_path / "file.txt"), False, "output")]
    def test_unicode_path_accepted(self, tmp_path):
        """Unicode (Chinese) path within output_root → accepted."""
        ctx = self._ctx(tmp_path)
        ctx.declare_output("中文/路径/文件.jpg")
        assert (tmp_path / "中文" / "路径").is_dir()
    def test_resolve_failure_sandbox_still_enforced(self, tmp_path, monkeypatch):
        """When resolve() raises OSError, sandbox check uses abspath fallback — traversal still rejected."""
        ctx = self._ctx(tmp_path)
        original_resolve = Path.resolve
        def mock_resolve(self_instance, **kwargs):
            # 只对「路径穿越」场景抛错；其他调用（含 mutmut trampoline 内部的
            # Path.resolve(strict=True)）放行——否则全局 mock 会误伤 mutmut 自身。
            if "../" in str(self_instance):
                raise OSError("Cannot resolve path")
            return original_resolve(self_instance, **kwargs)
        monkeypatch.setattr(Path, "resolve", mock_resolve)
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output("../../etc/passwd")
        # Restore original for other tests
        monkeypatch.setattr(Path, "resolve", original_resolve)

# ── Path sandbox tests (from test_pipeline.py) ──────────────────────────

class TestPathSandbox:
    """Tests for output_root path sandbox (REQ-10)."""
    def test_output_root_rejects_traversal_in_sandbox(self, tmp_path):
        """Path traversal outside output_root → ValueError."""
        job = Job("test", "j1")
        ctx = TaskContext(job, set(), set(), {}, output_root=tmp_path)
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output("../../etc/passwd")
    def test_no_output_root_allows_any(self, tmp_path):
        """output_root=None → no path restriction."""
        job = Job("test", "j2")
        ctx = TaskContext(job, set(), set(), {}, output_root=None)
        # Should not raise
        ctx.declare_output(str(tmp_path / "anything.txt"))

# ─── Adversarial sandbox inputs (invariant: path containment holds) ────

class TestSandboxAdversarialInputs:
    """REQ-10 edge cases: null bytes, dotdot variants, symlinks, empty paths."""
    @staticmethod
    def _ctx(output_root, job_id="j1"):
        job = Job("test", job_id, payload={})
        return TaskContext(
            job,
            set(),
            set(),
            {},
            output_root=output_root,
        )
    def test_path_with_null_bytes_rejected(self, tmp_path):
        """Paths with embedded null bytes raise ValueError (Python filesystem safety)."""
        ctx = self._ctx(tmp_path)
        # Python raises ValueError for null bytes in path operations.
        with pytest.raises(ValueError):
            ctx.declare_output("a\x00b")
    def test_dotdot_in_middle_rejected(self, tmp_path):
        """'a/../../b' resolves outside root → rejected."""
        ctx = self._ctx(tmp_path)
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output("a/../../b")
    def test_path_exactly_dotdot_rejected(self, tmp_path):
        """'..' resolves to parent of root → rejected."""
        ctx = self._ctx(tmp_path)
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output("..")
    def test_path_with_trailing_dotdot_accepted(self, tmp_path):
        """'subdir/..' resolves to root itself → accepted."""
        ctx = self._ctx(tmp_path)
        # Create subdir first so resolve works
        (tmp_path / "subdir").mkdir(exist_ok=True)
        ctx.declare_output("subdir/..")
        # Should not raise; resolves to root
    def test_symlink_escape_rejected(self, tmp_path):
        """Symlink inside root pointing outside → resolved path outside root → rejected."""
        ctx = self._ctx(tmp_path)
        # Create an external target
        external = tmp_path.parent / "external_target.txt"
        external.write_text("secret")
        # Create symlink inside root pointing to external
        link = tmp_path / "escape_link"
        try:
            link.symlink_to(external)
        except (OSError, NotImplementedError):
            pytest.skip("Symlinks not supported on this platform")
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output("escape_link")
    def test_very_long_path_accepted(self, tmp_path):
        """A 500-char relative path is accepted (within root)."""
        ctx = self._ctx(tmp_path)
        long_name = "a" * 500
        resolved = ctx.declare_output(long_name)
        assert long_name in resolved
    def test_path_with_only_slashes_rejected(self, tmp_path):
        """'/' is absolute and outside root → rejected."""
        ctx = self._ctx(tmp_path)
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output("/")
    def test_path_with_windows_backslashes_treated_as_filename(self, tmp_path):
        """On Linux, 'subdir\\file' is a single filename (not a path separator)."""
        ctx = self._ctx(tmp_path)
        resolved = ctx.declare_output("subdir\\file.txt")
        # Should be accepted — backslash is part of the filename on Linux
        assert "subdir\\file.txt" in resolved
    def test_multiple_outputs_persisted(self, tmp_path):
        """Calling declare_output multiple times persists all paths to outputs.jsonl."""
        ipc_dir = tmp_path / "ipc"
        ctx = self._ctx(tmp_path)
        ctx.ipc_dir = str(ipc_dir)
        ctx.declare_output("file1.txt")
        ctx.declare_output("file2.txt")
        ctx.declare_output("file3.txt")
        outputs = ArtifactJournal(ipc_dir).read_outputs(ctx.job.uid)
        names = [Path(r).name for r, _, _ in outputs]
        assert set(names) == {"file1.txt", "file2.txt", "file3.txt"}
    def test_declare_output_path_object_within_root(self, tmp_path):
        """Passing a Path object (relative) → accepted, coerced to str."""
        ctx = self._ctx(tmp_path)
        resolved = ctx.declare_output(Path("path_obj_file.txt"))
        assert "path_obj_file.txt" in resolved
    def test_declare_output_empty_string_resolves_to_root(self, tmp_path):
        """declare_output('') → Path('') == Path('.') → resolves to root itself → accepted."""
        ctx = self._ctx(tmp_path)
        ctx.declare_output("")
    def test_declare_output_dot_only_accepted(self, tmp_path):
        """declare_output('.') → resolves to root itself → accepted."""
        ctx = self._ctx(tmp_path)
        ctx.declare_output(".")
    def test_no_output_root_relative_path_uses_absolute(self, tmp_path):
        """output_root=None + relative path → uses absolute() (resolves to CWD/path)."""
        ctx = self._ctx(None)
        # Should not raise; resolves to CWD/path
        resolved = ctx.declare_output("relative_no_root.txt")
        # Should be an absolute path
        assert Path(resolved).is_absolute()
    def test_nested_dotdot_deep_traversal_rejected(self, tmp_path):
        """Deep traversal 'a/b/../../../d' resolves outside root → rejected."""
        ctx = self._ctx(tmp_path)
        # a/b/../../d → ../d (outside root)
        with pytest.raises(ValueError, match="outside output_root"):
            ctx.declare_output("a/b/../../../d")
    def test_cleanup_flag_variants(self, tmp_path):
        """cleanup_on_fail True/False both persisted correctly."""
        ipc_dir = tmp_path / "ipc"
        ctx = self._ctx(tmp_path)
        ctx.ipc_dir = str(ipc_dir)
        ctx.declare_output("a.txt", cleanup_on_fail=True)
        ctx.declare_output("b.txt", cleanup_on_fail=False)
        outputs = ArtifactJournal(ipc_dir).read_outputs(ctx.job.uid)
        flags = [cl for _, cl, _ in outputs]
        assert flags == [True, False]
