"""v2 单文件锁互斥回归测试（flock 成功 / 冲突双路径）。

不变式：同一 uid 的锁文件在持锁期间对其他执行体不可获取（互斥），
释放后可重新获取；锁生命周期 = 执行体生命周期（worker 持锁、主进程
仅探测）的前提是 flock 返回值真实反映加锁成败。
"""

import time

from tasklite.utils.lockfile import probe_lock, release_lock, try_acquire_lock


class TestFlockMutex:
    def test_second_acquire_while_held_fails(self, tmp_path):
        """持锁期间同 uid 二次加锁必须失败，释放后可再获取。"""
        uid = "encode::j1"
        # 成功路径：无竞争首次加锁必得 fd
        fd_first = try_acquire_lock(str(tmp_path), uid)
        assert fd_first is not None, "无竞争时首次加锁必须成功"
        try:
            # 不同 uid 的锁互不干扰（uid→锁文件隔离）
            fd_other = try_acquire_lock(str(tmp_path), "encode::j2")
            assert fd_other is not None, "不同 uid 加锁不得被无关持锁者阻塞"
            release_lock(fd_other)
            # 冲突路径：持锁期间同 uid 二次加锁必须失败（lock_conflict 语义）
            fd_second = try_acquire_lock(str(tmp_path), uid)
            assert fd_second is None, "持锁期间同 uid 二次加锁必须失败（锁互斥）"
        finally:
            release_lock(fd_first)
        # 释放后可再获取：锁确已释放，而非锁文件残留导致的假冲突
        fd_third = try_acquire_lock(str(tmp_path), uid)
        assert fd_third is not None, "释放后必须能重新获取锁"
        release_lock(fd_third)

    def test_probe_lock_reflects_external_holder(self, tmp_path):
        """probe_lock 只反映「是否有他人持锁」：空闲通过、被占失败、释放恢复。"""
        uid = "render::probe"
        assert probe_lock(str(tmp_path), uid) is True, "无持锁者时探测必须通过"
        fd = try_acquire_lock(str(tmp_path), uid)
        assert fd is not None, "测试前置：本执行体持锁"
        try:
            assert probe_lock(str(tmp_path), uid) is False, "他人持锁时探测必须失败"
        finally:
            release_lock(fd)
        assert probe_lock(str(tmp_path), uid) is True, "释放后探测必须恢复通过"

    def test_acquire_timeout_returns_none_while_held(self, tmp_path):
        """timeout>0 时持锁阻塞必须在 deadline 处返回 None（超时语义）。"""
        uid = "render::timeout"
        fd = try_acquire_lock(str(tmp_path), uid)
        assert fd is not None, "测试前置：持锁制造竞争"
        try:
            started_at = time.monotonic()
            got = try_acquire_lock(str(tmp_path), uid, timeout=0.4)
            elapsed = time.monotonic() - started_at
            assert got is None, "持锁期间带超时加锁必须超时返回 None"
            assert elapsed >= 0.3, "deadline 前不得提前返回（timeout 参数被无视）"
        finally:
            release_lock(fd)

    def test_lock_file_never_unlinked_after_release(self, tmp_path):
        """锁文件创建后永不删除：释放后同名文件仍在（unlink-recreate 竞争防御）。"""
        from tasklite.utils.encoding import safe_uid_filename

        uid = "keep::file"
        fd = try_acquire_lock(str(tmp_path), uid)
        assert fd is not None
        release_lock(fd)
        assert (tmp_path / f"{safe_uid_filename(uid)}.lock").exists(), (
            "锁文件必须保留在磁盘（unlink 后新 inode 锁与旧持锁者不互斥）"
        )

    def test_lock_env_fault_raises_not_none(self, tmp_path):
        """锁路径环境故障（目录占位）必须上抛 OSError，不得伪装成「锁被占」。

        两种语义必须区分：环境故障静默混入 None 会让调用方把真实错误
        误当锁冲突重试吞掉。
        """
        import pytest

        from tasklite.utils.encoding import safe_uid_filename

        lock_name = f"{safe_uid_filename('keep::file')}.lock"
        (tmp_path / lock_name).mkdir()
        with pytest.raises(OSError):
            try_acquire_lock(str(tmp_path), "keep::file")
