"""Shared mock infrastructure for pipeline integration tests.

Extracted from test_pipeline.py to avoid duplication across split test files.

IPC 落盘机制：FakeProcess 直接调用 ``write_result_atomic`` 写结果文件——executor 的
``drain()`` 统一走文件轮询，fake 与真实子进程共享同一代码路径。
"""

import os
import queue
import time
from pathlib import Path

from tasklite.pipeline import TaskLite


def _write_fake_result(ipc_dir, uid, result_dict, incarnation=None):
    """FakeProcess 用：把 IPC 结果原子写入结果文件（与真实子进程同路径）。

    incarnation 由 ctx 携带——fake 与真实子进程共享
    同一路径构造逻辑，保证 drain 能读到 fake 写的结果。
    """
    from tasklite.engine.channel import write_result_atomic
    write_result_atomic(ipc_dir, uid, result_dict, incarnation=incarnation)


def _ctx_incarnation(args):
    """从 FakeProcess args=(handler_func, job, ctx, ipc_dir) 提取 ctx.incarnation。"""
    ctx = args[2] if len(args) >= 3 else None
    return getattr(ctx, "incarnation", None) if ctx is not None else None


def make_fake_process_class(outcome="success"):
    """Factory for FakeProcess classes with controllable outcomes.

    ``start()`` 直接向结果文件写入 canned IPC 结果。
    """

    class FakeProcess:
        def __init__(self, target=None, args=(), kwargs=None, target_pipeline=None, **_kw):
            self.target = target
            self.args = args
            self.kwargs = kwargs or {}
            self._alive = False
            self._killed = False
            self._joined = False
            self.exitcode = 0
            if target_pipeline:
                setattr(self, '_pipeline', target_pipeline)

        def start(self):
            self._alive = True
            # args = (handler_func, job, ctx, ipc_dir)
            if len(self.args) >= 4:
                job = self.args[1]
                ipc_dir = self.args[3]
                incarnation = _ctx_incarnation(self.args)
                if outcome == "success":
                    _write_fake_result(ipc_dir, job.uid, {
                        "status": "success",
                        "raw_result": True,
                        "new_jobs": [],
                        "resource_suspensions": [],
                        "cursor_updates": {},
                    }, incarnation=incarnation)
                elif outcome == "retry":
                    _write_fake_result(ipc_dir, job.uid, {"status": "retry", "error": "transient"}, incarnation=incarnation)
                elif outcome == "fatal":
                    _write_fake_result(ipc_dir, job.uid, {
                        "status": "fatal",
                        "error": "FatalError: bug",
                        "traceback": "traceback",
                    }, incarnation=incarnation)
                elif outcome == "fatal_exception":
                    _write_fake_result(ipc_dir, job.uid, {
                        "status": "fatal",
                        "error": "TypeError: bad type",
                        "traceback": "traceback",
                    }, incarnation=incarnation)
                elif outcome == "error":
                    _write_fake_result(ipc_dir, job.uid, {
                        "status": "error",
                        "error": "something broke",
                        "traceback": "traceback",
                    }, incarnation=incarnation)

        def join(self, timeout=None):
            self._alive = False
            self._joined = True

        def is_alive(self):
            return self._alive

        def kill(self):
            self._alive = False
            self._joined = True
            self._killed = True
            self.exitcode = -9

    return FakeProcess


def make_ipc_process_class(results=(), exitcode=0, stay_alive=False):
    """Factory for FakeProcess classes that write a fixed sequence of IPC results.

    每次 ``start()`` 从 results 序列取一个结果写入结果文件。

    Args:
        results: iterable of IPC result dicts to write on each ``start()``.
            An entry of ``None`` skips the write (simulates no IPC result).
            The last entry repeats for subsequent calls.
        exitcode: process exit code (used by the crash-detection path).
        stay_alive: if True, ``is_alive()`` stays True until ``kill()`` —
            simulates a hung subprocess for the timeout path.
    """
    results = list(results)
    call_count = [0]
    _exposed_count = call_count  # 类属性需引用外部别名（类体不解析闭包）

    class FakeProcess:
        call_count = _exposed_count  # exposed for assertions like "started twice"

        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self.args = args
            self._alive = False
            self.exitcode = exitcode

        def start(self):
            self._alive = stay_alive
            if not results:
                return
            idx = min(call_count[0], len(results) - 1)
            call_count[0] += 1
            payload = results[idx]
            if payload is not None and len(self.args) >= 4:
                job = self.args[1]
                ipc_dir = self.args[3]
                incarnation = _ctx_incarnation(self.args)
                _write_fake_result(ipc_dir, job.uid, payload, incarnation=incarnation)

        def join(self, timeout=None):
            if not stay_alive:
                self._alive = False

        def is_alive(self):
            return self._alive

        def kill(self):
            self._alive = False
            self.exitcode = -9

    return FakeProcess


class FakeManager:
    """Drop-in replacement for multiprocessing.Manager() in tests.

    Provides a shared list() method; __exit__ is a no-op so no real server
    process is spawned. Use this whenever a test monkeypatches time.sleep or
    time.monotonic to avoid the real Manager's shutdown hanging.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def list(self):
        return []


def patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=None):
    """Install fake Process/Queue/Manager across the multiprocessing modules.

    Pipeline code uses both the global `multiprocessing` module and the spawn
    context returned by `mp.get_context("spawn")`. This helper patches all
    relevant targets so fake-based tests don't accidentally spawn real
    subprocesses.

    Args:
        monkeypatch: pytest monkeypatch fixture.
        fake_process_class: Fake Process class (e.g. from make_fake_process_class)
            or None to skip Process patching.
    """
    monkeypatch.setattr("multiprocessing.Manager", lambda: FakeManager())
    monkeypatch.setattr("tasklite.pipeline.mp.Manager", lambda: FakeManager())

    if fake_process_class is not None:
        monkeypatch.setattr("multiprocessing.Process", fake_process_class)
        monkeypatch.setattr("tasklite.pipeline.mp.Process", fake_process_class)


def patch_pipeline_manager(pipeline, monkeypatch):
    """Replace the spawn-context Manager on a pipeline instance with FakeManager."""
    monkeypatch.setattr(pipeline._mp_ctx, "Manager", lambda: FakeManager())


def make_pipeline(tmp_path, name="test_pipeline"):
    """Create a pipeline with sqlite backend and temp directory."""
    return TaskLite(name=name, state_dir=tmp_path / "state", backend="sqlite", output_root=tmp_path / "output")
