"""跨平台单文件锁互斥回归测试（flock / msvcrt 成功、冲突、偏移归零、IO 故障区分、不删锁文件）。"""
from __future__ import annotations

import errno
import os
import sys
import time
from pathlib import Path

import pytest

from tasklite.utils import lockfile
from tasklite.utils.lockfile import lock_path_for, release_lock, try_acquire_lock


class TestFlockMutex:
    def test_lock_path_for_derivation_and_idempotency(self, tmp_path):
        state_db = tmp_path / "state.db"
        derived = lock_path_for(state_db)
        assert derived == Path(f"{state_db}.lock")
        assert lock_path_for(derived) == derived

    def test_second_acquire_while_held_fails(self, tmp_path):
        lock_name = "pipeline_run_test"
        fd_first = try_acquire_lock(str(tmp_path), lock_name)
        assert fd_first is not None, "无竞争时首次加锁必须成功"
        try:
            fd_other = try_acquire_lock(str(tmp_path), "pipeline_run_other")
            assert fd_other is not None, "不同名称加锁不得被无关持锁者阻塞"
            release_lock(fd_other)
            fd_second = try_acquire_lock(str(tmp_path), lock_name)
            assert fd_second is None, "持锁期间同名二次加锁必须失败（锁互斥）"
        finally:
            release_lock(fd_first)
        fd_third = try_acquire_lock(str(tmp_path), lock_name)
        assert fd_third is not None, "释放后必须能重新获取锁"
        release_lock(fd_third)

    def test_acquire_timeout_returns_none_while_held(self, tmp_path):
        lock_name = "render_timeout"
        fd = try_acquire_lock(str(tmp_path), lock_name)
        assert fd is not None
        try:
            started_at = time.monotonic()
            got = try_acquire_lock(str(tmp_path), lock_name, timeout=0.4)
            elapsed = time.monotonic() - started_at
            assert got is None, "持锁期间带超时加锁必须超时返回 None"
            assert elapsed >= 0.3, "deadline 前不得提前返回"
        finally:
            release_lock(fd)

    def test_lock_file_never_unlinked_after_release(self, tmp_path):
        lock_name = "keep_file"
        fd = try_acquire_lock(str(tmp_path), lock_name)
        assert fd is not None
        release_lock(fd)
        assert (tmp_path / f"{lock_name}.lock").exists()

    def test_lock_env_fault_raises_not_none(self, tmp_path):
        lock_name = "keep_file"
        (tmp_path / f"{lock_name}.lock").mkdir()
        with pytest.raises(OSError):
            try_acquire_lock(str(tmp_path), lock_name)

    def test_try_lock_fd_propagates_non_contention_oserror(self, tmp_path, monkeypatch):
        """非争用类 OSError（如 EIO / EBADF）必须原样向上传播，绝不伪装为锁被占用。"""
        closed_fds: list[int] = []
        real_close = os.close

        def tracked_close(fd: int) -> None:
            closed_fds.append(fd)
            real_close(fd)

        monkeypatch.setattr(os, "close", tracked_close)

        if sys.platform == "win32":
            import msvcrt

            def raise_io_error(fd: int, mode: int, nbytes: int) -> None:
                raise OSError(errno.EIO, "simulated disk I/O error")

            monkeypatch.setattr(msvcrt, "locking", raise_io_error)
        else:
            import fcntl

            def raise_io_error(fd: int, operation: int) -> None:
                raise OSError(errno.EIO, "simulated disk I/O error")

            monkeypatch.setattr(fcntl, "flock", raise_io_error)

        with pytest.raises(OSError) as exc_info:
            try_acquire_lock(tmp_path / "state.db")
        assert exc_info.value.errno == errno.EIO
        assert len(closed_fds) == 1, "异常路径不得泄漏已打开的文件描述符"

    def test_windows_lseek_resets_offset_before_locking(self, tmp_path):
        """Windows 下即使文件偏移不在 0，_try_lock_fd 也会先归零再加锁，确保始终锁第 0 字节。"""
        if sys.platform != "win32":
            pytest.skip("仅在 Windows 验证 msvcrt.locking 偏移归零行为")
        lock_file = tmp_path / "offset_test.lock"
        fd1 = os.open(str(lock_file), os.O_RDWR | os.O_CREAT, 0o644)
        fd2 = os.open(str(lock_file), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            os.write(fd1, b"abcdef")
            assert os.lseek(fd1, 0, os.SEEK_CUR) == 6
            assert lockfile._try_lock_fd(fd1) is True
            assert os.lseek(fd1, 0, os.SEEK_CUR) == 0

            os.lseek(fd2, 4, os.SEEK_SET)
            assert lockfile._try_lock_fd(fd2) is False
            assert os.lseek(fd2, 0, os.SEEK_CUR) == 0
        finally:
            release_lock(fd1)
            try:
                os.close(fd2)
            except OSError:
                pass

    def test_contention_error_classification_win32_vs_posix(self, monkeypatch):
        monkeypatch.setattr(lockfile.sys, "platform", "win32")
        assert lockfile._is_lock_contention(OSError(errno.EACCES, "locked")) is True
        assert lockfile._is_lock_contention(OSError(errno.EDEADLK, "deadlock")) is True
        win_lock_err = OSError("lock violation")
        win_lock_err.winerror = 33  # type: ignore[attr-defined]
        assert lockfile._is_lock_contention(win_lock_err) is True
        win_share_err = OSError("sharing violation")
        win_share_err.winerror = 32  # type: ignore[attr-defined]
        assert lockfile._is_lock_contention(win_share_err) is True
        assert lockfile._is_lock_contention(OSError(errno.EBADF, "bad fd")) is False
        assert lockfile._is_lock_contention(OSError(errno.EIO, "disk fault")) is False

        monkeypatch.setattr(lockfile.sys, "platform", "linux")
        assert lockfile._is_lock_contention(OSError(errno.EACCES, "locked")) is True
        assert lockfile._is_lock_contention(OSError(errno.EAGAIN, "again")) is True
        assert lockfile._is_lock_contention(OSError(errno.EWOULDBLOCK, "would block")) is True
        assert lockfile._is_lock_contention(OSError(errno.EBADF, "bad fd")) is False
        assert lockfile._is_lock_contention(OSError(errno.EIO, "disk fault")) is False
