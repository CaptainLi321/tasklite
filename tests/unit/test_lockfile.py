"""跨平台单文件锁互斥回归测试（flock 成功 / 冲突 / 超时 / 不删锁文件）。"""
from __future__ import annotations

import time
import pytest

from tasklite.utils.lockfile import release_lock, try_acquire_lock


class TestFlockMutex:
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
