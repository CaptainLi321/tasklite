""" flock 孤儿探测验收测试 +  生命周期钩子验收测试。

对应 / 决策的验收规格：
worker 持锁 + 主进程仅探测、锁文件永不删除、
    uid→文件名统一映射、跨平台锁封装。
三钩子集（on_run_start/on_run_end/on_job_completed）+ going_to_retry 合并条件
    + 同步/异常隔离/全退出路径/单 callable/时序（5 条契约）。
"""
import time
from pathlib import Path

import pytest

from tasklite.models.job import Job
from tasklite.pipeline import TaskLite
from tasklite.utils.lockfile import (
    release_lock, safe_uid_filename, try_acquire_lock,
)


def _v11_ok_handler(job, ctx):
    """模块级 handler（spawn 子进程要求可 pickle）。"""
    return True


def _v11_retry_then_fail(job, ctx):
    """模块级 handler：首试瞬态失败（重试），次试永久失败（DLQ）。"""
    from tasklite import RetryError
    if job.retries == 0:
        raise RetryError("transient")
    raise RuntimeError("permanent")



# flock 孤儿探测


class TestOrphanLockDetection:
    def test_safe_uid_filename_escapes_colons(self):
        """uid 的 :: 映射为可逆安全文件名（Windows 禁 :）。"""
        uid = "task_type::job_id"
        safe = safe_uid_filename(uid)
        assert ":" not in safe
        assert "::" not in safe
        # 可逆且无碰撞：不同 uid → 不同安全名
        assert safe_uid_filename("a::b") != safe_uid_filename("a__b")
        assert safe_uid_filename("a::b") == safe_uid_filename("a::b")

    def test_safe_uid_filename_injective(self):
        """safe_uid_filename 必须单射——job_id 可含 %，
        单射转义防止复合键分隔符碰撞：
        文件名（锁/信号/结果文件串扰）。先转义 % 为 %25 保证单射。"""
        assert safe_uid_filename("t::x::y") != safe_uid_filename("t::x_%3A%3A_y")
        # 合法 uid 里的 % 不会被编码序列混淆
        assert safe_uid_filename("a::b%3A%3A") != safe_uid_filename("a::b::")
        # 单射的逆：同一 uid 恒同映射
        assert safe_uid_filename("t::x_%3A%3A_y") == safe_uid_filename("t::x_%3A%3A_y")

    def test_safe_uid_filename_escapes_fs_dangerous_chars(self, tmp_path):
        """job_id 可含路径分隔符/glob 元字符——safe_uid_filename
        必须转义它们，否则 uid 派生路径逃逸 state_dir（任意文件创建/覆盖/删除）。"""
        # 路径分隔符 / 和 \ 必须转义（否则 .. 组件可逃逸目录）
        assert "/" not in safe_uid_filename("t::a/../../../etc/passwd")
        assert "\\" not in safe_uid_filename("t::a\\..\\..\\b")
        # NUL 必须转义（os.open 对含 NUL 路径抛 ValueError）
        assert "\x00" not in safe_uid_filename("t::a\x00b")
        # glob 元字符必须转义（防 _iter_stale_result_paths glob 注入跨 uid 匹配）
        for ch in "*?[]":
            assert ch not in safe_uid_filename(f"t::a{ch}b"), f"glob 元字符 {ch} 未转义"

    def test_safe_uid_filename_no_traversal(self, tmp_path):
        """恶意 job_id 派生的文件名解析后必须仍位于 ipc_dir 内（不逃逸）。"""
        ipc = tmp_path / "ipc"
        ipc.mkdir()
        malicious = "t::a/../../../b"
        safe = safe_uid_filename(malicious)
        # 转义后不含路径分隔符 → 作为单文件名拼在 ipc 下
        assert "/" not in safe and "\\" not in safe
        p = Path(ipc) / f"{safe}.lock"
        assert p.is_relative_to(ipc), f"派生路径逃逸 ipc_dir: {p}"

    def test_safe_uid_filename_still_injective_with_dangerous(self):
        """转义后仍单射——不同 uid（含危险字符）派生不同文件名。"""
        assert safe_uid_filename("t::a/b") != safe_uid_filename("t::a//b")
        assert safe_uid_filename("t::a/b") != safe_uid_filename("t::a%2Fb")
        assert safe_uid_filename("t::a*b") != safe_uid_filename("t::a%2Ab")

    def test_safe_uid_filename_escapes_single_colon(self):
        """job_id 可合法含单冒号（只禁 ::）——Windows 文件名禁 `:`，
        单冒号不转义则 Windows 派发路径非法。pre-fix：safe_uid_filename
        ("t::a:b") 保留冒号 → 派生 .lock 路径含 `:` → NTFS 拒绝创建。"""
        safe = safe_uid_filename("t::a:b")
        assert ":" not in safe, f"单冒号未转义: {safe}"
        # 单射保持：单冒号 → %3A 与字面 %3A 不混淆（% 先转义 %25）
        assert safe_uid_filename("t::a:b") != safe_uid_filename("t::a%3Ab")
        assert safe_uid_filename("t::a:b") == safe_uid_filename("t::a:b")

    def test_lock_acquire_release_roundtrip(self, tmp_path):
        """锁获取/释放/再获取（非阻塞互斥）。"""
        ipc = str(tmp_path)
        fd1 = try_acquire_lock(ipc, "t::a")
        assert fd1 is not None
        # 同 uid 第二把锁非阻塞失败
        fd2 = try_acquire_lock(ipc, "t::a")
        assert fd2 is None
        # 释放后重新可获取
        release_lock(fd1)
        fd3 = try_acquire_lock(ipc, "t::a")
        assert fd3 is not None
        release_lock(fd3)

    def test_lock_file_survives_cleanup_ipc_files(self, tmp_path, monkeypatch):
        """cleanup_ipc_files 不误删 .lock（unlink-recreate 竞争）。"""
        from tasklite.engine.channel import cleanup_ipc_files
        ipc = tmp_path
        fd = try_acquire_lock(str(ipc), "t::a")
        assert fd is not None
        lock_path = ipc / f"{safe_uid_filename('t::a')}.lock"
        assert lock_path.exists()
        # 模拟 job 完成后的 IPC 清理
        from tasklite.engine.channel import signals_path, result_path
        signals_path(str(ipc), "t::a").touch()
        result_path(str(ipc), "t::a", "abc").touch()
        cleanup_ipc_files(str(ipc), "t::a", "abc")
        # 结果/信号被清理，锁文件保留
        assert not signals_path(str(ipc), "t::a").exists()
        assert lock_path.exists(), "lock file must survive cleanup"
        release_lock(fd)

    def test_orphan_hold_defers_dispatch(self, tmp_path, monkeypatch):
        """孤儿 worker 持锁 → 主进程探测失败 → defer 而非执行。

        孤儿持锁期间 job 带退避、run 会等待（正确 at-least-once 语义），
        故不跑完整 run——直接验证单轮派发拦截 + 计数 + requeue + 释放后
        探测恢复。
        """
        p = TaskLite(name="t", state_dir=str(tmp_path / "st"),
                         backend="sqlite", max_workers=1, output_root=str(tmp_path / "out"))
        p.register_handler("t", _v11_ok_handler)
        # 先持锁模拟存活孤儿——锁在 pipeline 实际的 ipc_dir（state_dir/ipc）
        ipc = Path(p.ipc_dir)
        orphan_fd = try_acquire_lock(str(ipc), "t::a")
        assert orphan_fd is not None

        p.enqueue([Job("t", "a")])
        # 构造最小 state 直接验证 _dispatch_job 拦截
        from tasklite.models.state import PipelineState
        from tasklite.engine.scheduler import JobScheduler
        p._state = PipelineState(p.backend.load_wall(), p.backend.load_failed(),
                                 p.backend.load_cursors(), p.backend.load_queue())
        sched = JobScheduler(p.resources, p.handlers).pop_next_runnable(p._state, frozenset())
        assert sched.runnable_idx is not None, "job should be runnable (probe is the gate)"
        entry = p._dispatch_job(sched)
        assert entry is None, "orphan lock → dispatch must defer (no subprocess)"
        # (a) 拦截生效：计数增长、job 未执行
        assert p.stats.get("deferred_orphan", 0) >= 1
        assert "t::a" not in p.backend.load_wall(), "job must not execute while orphan holds lock"
        # (b) job 保留在队列（requeue，at-least-once 不丢）
        assert "t::a" in [Job.from_dict(j).uid for j in p._state.queue]
        # (c) 释放锁 → 探测恢复
        release_lock(orphan_fd)
        from tasklite.utils.lockfile import probe_lock
        assert probe_lock(str(ipc), "t::a") is True, "released lock → probe passes"

    def test_lock_semantics_platform_consistent(self, tmp_path):
        """同一 uid 两进程不可同时持锁（跨平台语义一致性）。"""
        # 在同一进程内用两个 fd 模拟两个进程的互斥语义（flock 同进程
        # 不同 fd 也互斥；msvcrt 依赖文件指针，同 fd 语义不同——用独立
        # 打开验证 POSIX 路径，Windows 由 CI runner 覆盖）。
        ipc = str(tmp_path)
        fd1 = try_acquire_lock(ipc, "t::mutex")
        # 独立打开的第二把锁必须失败（另一个"进程"持锁）
        import os
        path = Path(ipc) / f"{safe_uid_filename('t::mutex')}.lock"
        fd2 = os.open(str(path), os.O_RDWR)
        if os.name == "nt":
            pytest.skip("Windows lock semantics covered by CI runner")
        import fcntl
        with pytest.raises(OSError):
            fcntl.flock(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.close(fd2)
        release_lock(fd1)



# 生命周期钩子（API-5）


class TestLifecycleHooks:
    def test_hook_exception_isolated(self, tmp_path):
        """契约 2：钩子抛异常 → run 不受影响、hook_errors 计数。"""
        calls = {"start": 0, "end": 0, "job": 0}

        def bad_start():
            calls["start"] += 1
            raise RuntimeError("boom")

        def bad_job(*a):
            calls["job"] += 1
            raise RuntimeError("boom2")

        def ok_end(reason):
            calls["end"] += 1
            assert reason == "completed"

        p = TaskLite(name="t", state_dir=str(tmp_path / "s"),
                         backend="sqlite", max_workers=1,
                         on_run_start=bad_start, on_run_end=ok_end,
                         on_job_completed=bad_job)
        p.register_handler("t", _v11_ok_handler)
        p.enqueue([Job("t", "a")])
        p.run()
        assert calls["start"] == 1
        assert calls["end"] == 1
        assert calls["job"] == 1
        assert p.stats.get("hook_errors", 0) == 2, p.stats
        # run 正常完成
        assert "t::a" in p.backend.load_wall()

    def test_on_run_end_interrupted_reason(self, tmp_path, monkeypatch):
        """契约 3：崩溃路径（KeyboardInterrupt）→ on_run_end 以 interrupted 触发。"""
        reasons = []

        def record(reason):
            reasons.append(reason)

        p = TaskLite(name="t", state_dir=str(tmp_path / "s"),
                         backend="sqlite", max_workers=1, on_run_end=record)
        p.register_handler("t", _v11_ok_handler)
        p.enqueue([Job("t", "a")])
        # 注入 KeyboardInterrupt 在派发时（后派发实现迁至 DispatchMachine）
        def exploding(*a, **k):
            raise KeyboardInterrupt()
        monkeypatch.setattr(p._dispatch, "dispatch_job", exploding)
        import pytest as _pt
        with _pt.raises(KeyboardInterrupt):
            p.run()
        assert reasons == ["interrupted"], reasons

    def test_job_completed_going_to_retry_consistency(self, tmp_path):
        """合并条件：on_job_completed 的 success/going_to_retry 与最终去向一致。"""
        from tasklite import RetryError
        seen = []

        def record(uid, meta, success, going_to_retry):
            seen.append((uid, success, going_to_retry))

        p = TaskLite(name="t", state_dir=str(tmp_path / "s"),
                         backend="sqlite", max_workers=1,
                         on_job_completed=record)
        p.register_handler("t", _v11_retry_then_fail)
        p.enqueue([Job("t", "a", max_retries=1)])
        p.run()
        # 第一次：瞬态失败 → going_to_retry=True；第二次：终局失败 → going_to_retry=False
        assert len(seen) == 2, seen
        assert seen[0][1] is False and seen[0][2] is True, seen[0]  # retry
        assert seen[1][1] is False and seen[1][2] is False, seen[1]  # DLQ

    def test_job_completed_after_stats_update(self, tmp_path):
        """契约 5：钩子在 stats 更新后调用（钩内读 stats 一致）。"""
        observed = {}

        def record(uid, meta, success, going_to_retry):
            observed["stats"] = dict(p.stats)

        p = TaskLite(name="t", state_dir=str(tmp_path / "s"),
                         backend="sqlite", max_workers=1,
                         on_job_completed=record)
        p.register_handler("t", _v11_ok_handler)
        p.enqueue([Job("t", "a")])
        p.run()
        assert observed["stats"]["completed"] == 1, observed
        assert observed["stats"]["failed"] == 0, observed

    def test_job_completed_fires_on_direct_commit_paths(self, tmp_path):
        """契约 5 覆盖缺口：no-handler 等「不走子进程」直接 commit 路径
        也必须触发 on_job_completed（此前只在 _complete_job 触发）。"""
        seen = []

        def record(uid, meta, success, going_to_retry):
            seen.append((uid, success, going_to_retry))

        p = TaskLite(name="t", state_dir=str(tmp_path / "s"),
                         backend="sqlite", max_workers=1,
                         on_job_completed=record)
        # 不注册 handler → no-handler 直接 commit 路径
        p.enqueue([Job("nohandler", "x")])
        p.run()
        assert len(seen) == 1, seen
        assert seen[0] == ("nohandler::x", False, False), seen[0]
        assert "nohandler::x" in p.backend.load_failed()
