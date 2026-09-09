"""IPC 落盘 + 看门狗非阻塞回归测试。

覆盖：
- 挂起 job 被看门狗 TIMEOUT kill，正常并行 job 不受阻塞
- suspend 信号经文件落盘，handler 崩溃后信号不丢
-  崩溃恢复：stale 结果文件在派发子进程前被消费（不重复执行）
"""

import os
import time
from pathlib import Path

import pytest

from tasklite.pipeline import TaskLite
from tasklite.models.context import TaskContext
from tasklite.models.job import Job

from tests.helpers import make_fake_process_class, make_pipeline, patch_multiprocessing_for_fakes
from tasklite.engine.channel import (
    _normalize_handler_result,
    _encode_raw_result,
)
from tasklite.utils.ipc import ArtifactJournal


def _hang_handler(job, ctx):
    """真实子进程：无限挂起（模拟死锁/卡死），直到被看门狗 kill。"""
    while True:
        time.sleep(1)


def _quick_handler(job, ctx):
    return True


def _suspend_then_hang_handler(job, ctx):
    """真实子进程：先 suspend 资源再挂起（验证信号落盘不丢）。"""
    ctx.suspend_resource("api", 30.0)
    while True:
        time.sleep(1)


def _slow_ok_handler(job, ctx):
    time.sleep(0.5)
    return True


class TestWatchdogNonBlocking:
    def test_hung_job_timeout_kill_not_blocking(self, tmp_path, monkeypatch):
        """/挂起 job 超时被 kill，正常 job 并行完成，主循环不阻塞。

        基于落盘文件的信号传递机制：
        永远执行不到。新实现（落盘文件）：drain 只 poll 文件存在，挂起 job 由
        deadline 独立判定 → TIMEOUT kill。
        """
        import sys
        from pathlib import Path
        project_root = str(Path(__file__).resolve().parent.parent.parent)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        monkeypatch.setenv("PYTHONPATH", project_root)

        pipeline = TaskLite(
            name="watchdog", state_dir=tmp_path / "state", backend="sqlite", max_workers=2,
        )
        pipeline.register_handler("hang", _hang_handler)
        pipeline.register_handler("quick", _quick_handler)
        pipeline.enqueue([
            Job("hang", "h1", timeout=2),
            Job("quick", "q1", timeout=30),
        ])

        start = time.time()
        pipeline.run()
        elapsed = time.time() - start

        failed = pipeline.backend.load_failed()
        wall = pipeline.backend.load_wall()
        assert "hang::h1" in failed, "挂起 job 必须被看门狗 TIMEOUT kill"
        assert failed["hang::h1"].get("error", "").startswith("TIMEOUT")
        assert "quick::q1" in wall, "正常 job 必须并行完成"
        assert elapsed < 6, f"看门狗不得阻塞主循环: {elapsed:.1f}s（hang timeout=2s）"

    def test_suspend_signal_survives_hang(self, tmp_path, monkeypatch):
        """ 信号落盘：handler suspend 后挂起被 kill，信号仍被应用。"""
        import sys
        from pathlib import Path
        project_root = str(Path(__file__).resolve().parent.parent.parent)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        monkeypatch.setenv("PYTHONPATH", project_root)

        pipeline = TaskLite(
            name="suspend_hang", state_dir=tmp_path / "state", backend="sqlite", max_workers=1,
        )
        pipeline.add_resource(__import__(
            'tasklite.engine.resource', fromlist=['CapacityResource']
        ).CapacityResource("api", 10.0))
        pipeline.register_handler("suspend", _suspend_then_hang_handler)
        pipeline.enqueue([Job("suspend", "s1", timeout=2)])

        pipeline.run()

        # handler 先 suspend("api", 30) 再挂起 → 信号经文件落盘，kill 后不丢
        res = pipeline.resources["api"]
        assert res.suspend_until > time.monotonic(), \
            "挂起 job 的 suspend 信号必须经落盘文件被应用（不因 kill 丢失）"
        failed = pipeline.backend.load_failed()
        assert "suspend::s1" in failed

    def test_graceful_path_result_file_cleanup(self, tmp_path, monkeypatch):
        """正常完成路径：结果文件被读取并清理（ipc 目录不留残留）。"""
        import sys
        from pathlib import Path
        project_root = str(Path(__file__).resolve().parent.parent.parent)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        monkeypatch.setenv("PYTHONPATH", project_root)

        pipeline = TaskLite(
            name="cleanup_ipc", state_dir=tmp_path / "state", backend="sqlite", max_workers=1,
        )
        pipeline.register_handler("slow", _slow_ok_handler)
        pipeline.enqueue([Job("slow", "j1", timeout=30)])
        pipeline.run()

        assert "slow::j1" in pipeline.backend.load_wall()
        import os
        leftover = [f for f in os.listdir(pipeline.ipc_dir)
                    if f.endswith(".result.json") or f.endswith(".tmp")
                    or f.endswith(".signals.jsonl")]
        assert leftover == [], f"IPC 文件应被清理: {leftover}"


# ──  崩溃恢复：stale 结果文件在派发前被消费 ─────────────────────────


class TestStaleResultRestore:
    """ 修复：主进程 SIGKILL/OOM/断电 崩溃后，重启不重复执行已完成的 job。

    残留结果文件（{uid}.{incarnation}.result.json）在 ``_dispatch_job``
    派发子进程**之前**被 ``consume_stale_result`` 消费：成功 → 直接进 wall；
    retry → 重试计数+1 重入队；fatal/error → 直接 DLQ；损坏文件 → 丢弃并
    正常执行。handler 一律不再执行。
    """

    def test_stale_success_committed_without_subprocess(self, tmp_path, monkeypatch):
        """残留 success 结果 → job 直接进 wall，不启动子进程、不执行 handler。"""
        handler_calls = []
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: handler_calls.append(job.uid) or (True, {}))

        p.enqueue([Job("h", "a")])
        # 模拟上次崩溃：残留 success 结果文件（job 已执行完成但未 commit）
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        journal = ArtifactJournal(p.ipc_dir)
        journal.write_result_atomic("h::a", {
            "status": "success",
            "raw_result": True,
            "new_jobs": [],
            "resource_suspensions": [],
            "cursor_updates": {},
        }, incarnation=inc)

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        p.run()

        assert handler_calls == [], "stale 结果已消费，handler 不应再执行"
        assert "h::a" in p.backend.load_wall()
        assert not journal.result_path("h::a", inc).exists(), "残留文件消费后应删除"

    def test_stale_retry_increments_retries(self, tmp_path, monkeypatch):
        """残留 retry 结果 → job 重试计数 +1 重入队，退避后重新执行成功。"""
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: (True, {}))

        p.enqueue([Job("h", "a")])
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        journal = ArtifactJournal(p.ipc_dir)
        journal.write_result_atomic(
            "h::a", {"status": "retry", "error": "stale-retry"},
            incarnation=inc,
        )

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        p.run()

        # stale retry 被消费 → stats["retried"]=1；退避到期后重新执行 → 最终成功进 wall
        assert p.stats["retried"] == 1, \
            f"stale retry 应被消费并计入重试: {p.stats}"
        assert "h::a" in p.backend.load_wall(), "重试后应成功进 wall"
        assert "h::a" not in p.backend.load_failed()
        assert not journal.result_path("h::a", inc).exists(), "残留文件消费后应删除"

    def test_stale_fatal_goes_to_dlq(self, tmp_path, monkeypatch):
        """残留 fatal 结果 → job 直接进 DLQ，不启动子进程。"""
        handler_calls = []
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: handler_calls.append(job.uid) or (True, {}))

        p.enqueue([Job("h", "a")])
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        journal = ArtifactJournal(p.ipc_dir)
        journal.write_result_atomic("h::a", {
            "status": "fatal", "error": "TypeError: stale bug", "traceback": "tb",
        }, incarnation=inc)

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        p.run()

        assert handler_calls == [], "stale fatal 已消费，handler 不应再执行"
        failed = p.backend.load_failed()
        assert "h::a" in failed, "fatal 残留应进 DLQ"
        assert failed["h::a"]["fatal"] is True

    def test_stale_error_goes_to_dlq(self, tmp_path, monkeypatch):
        """残留 error 结果 → job 直接进 DLQ。"""
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: (True, {}))

        p.enqueue([Job("h", "a")])
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        journal = ArtifactJournal(p.ipc_dir)
        journal.write_result_atomic("h::a", {
            "status": "error", "error": "boom", "traceback": "tb",
        }, incarnation=inc)

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        p.run()

        failed = p.backend.load_failed()
        assert "h::a" in failed
        assert failed["h::a"]["error"] == "boom"

    def test_stale_corrupt_file_discarded_and_rerun(self, tmp_path, monkeypatch):
        """损坏/非结果格式残留 → 丢弃并正常派发执行（宁可重跑，不可误判）。"""
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: (True, {}))

        p.enqueue([Job("h", "a")])
        # 非标准结果文件：有内容但缺 status 键
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        journal = ArtifactJournal(p.ipc_dir)
        journal.write_result_atomic("h::a", {"not": "a result"}, incarnation=inc)

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        p.run()

        # 损坏残留被丢弃（日志含 "Discarding stale result file"），job 正常执行进 wall
        assert "h::a" in p.backend.load_wall(), "损坏残留应丢弃，job 应正常执行进 wall"
        assert p.stats["completed"] == 1
        assert not journal.result_path("h::a", inc).exists(), "损坏残留文件应被清理"

    def test_stale_success_missing_raw_result_does_not_crash(self, tmp_path, monkeypatch):
        """success 状态但缺 raw_result 键的残留文件不得崩掉整个 run。

        schema 防御：记为失败进 DLQ，不崩。
        """
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: (True, {}))

        p.enqueue([Job("h", "a")])
        # status=success 但缺 raw_result（模拟损坏/旧版本残留）
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        journal = ArtifactJournal(p.ipc_dir)
        journal.write_result_atomic("h::a", {"status": "success"}, incarnation=inc)

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        p.run()  # 不应抛异常

        failed = p.backend.load_failed()
        assert "h::a" in failed, "缺 raw_result 的残留应记为失败进 DLQ，而非崩 run"
        assert failed["h::a"]["error"] == "CORRUPT_RESULT_FILE: missing raw_result"

    def test_success_missing_raw_result_direct_decode_does_not_crash(self):
        """ 回归（drain 路径）：decode 直接收到缺 raw_result 的 dict 不抛 KeyError。"""
        from tasklite.engine.channel import _decode_ipc_result
        from tasklite.models.job import Job
        job = Job("h", "a")
        res = _decode_ipc_result({"status": "success"}, None, job, [])
        assert res.success is False
        assert res.result_meta["error"] == "CORRUPT_RESULT_FILE: missing raw_result"


# ══════════════════════════════════════════════════════════════════════
# IPC 文件生命周期边角防御
# ══════════════════════════════════════════════════════════════════════


class TestSignalsFileTruncateSemantics:
    def test_read_signals_truncates_before_unlink(self, tmp_path, monkeypatch):
        """unlink 失败时残留信号文件必须已截空。

        增量消费信号文件保障并发追加不丢失：
        丢失。返工版读后先 seek(0)+truncate(0) 再删——本用例模拟 unlink
        被拒，断言：a) 本轮信号完整读出；b) 残留文件为空（下轮 drain 不
        重复消费）。注意 open 必须是 "r+"——只读模式 truncate 抛
        io.UnsupportedOperation（OSError 子类）会被静默吞掉沦为死代码。
        """
        journal = ArtifactJournal(tmp_path)
        journal.record_signal("t::x", "api", 30.0)
        journal.record_signal("t::x", "db", 60.0)

        leftover = journal.signals_path("t::x")

        real_unlink_ref = leftover # noqa: F841（可读性：被测文件即下方断言对象）

        def refusing_unlink(self, *a, **k):
            raise OSError("permission denied (simulated)")

        monkeypatch.setattr(Path, "unlink", refusing_unlink)
        got = journal.drain_signals("t::x")
        assert ("api", 30.0) in got and ("db", 60.0) in got
        assert leftover.stat().st_size == 0, (
            f"截空失败，size={leftover.stat().st_size}——truncate 死代码复发"
        )
 # 截空后二次读取为空（不重复消费）
        assert journal.drain_signals("t::x") == []

    def test_drain_stale_prefers_latest_incarnation_same_instant(
        self, tmp_path, monkeypatch,
    ):
        """同刻多执行代残留按 (mtime_ns, incarnation_seq) 决胜。

        秒级 st_mtime 键在同秒内排序不稳定，可能消费较旧执行代的结果。
        用 monkeypatch 把所有文件的 mtime 钉到同一纳秒，只有 seq 能区分。
        """
        import os

        from tasklite.engine.channel import ExecutionChannel

        run_id = "a" * 32
        journal = ArtifactJournal(tmp_path)
 # worker 正常路径写的是 _encode_raw_result 编码后的 raw_result
        journal.write_result_atomic("t::x",
                            {"status": "success",
                             "raw_result": _encode_raw_result((True, {"v": 1}))},
                            incarnation=f"{run_id}.1")
        journal.write_result_atomic("t::x",
                            {"status": "success",
                             "raw_result": _encode_raw_result((True, {"v": 2}))},
                            incarnation=f"{run_id}.2")
 # 钉同一纳秒 mtime——决胜只剩 incarnation_seq
        for p in tmp_path.glob("*.result.json"):
            os.utime(p, ns=(1234567890, 1234567890))

        ex = ExecutionChannel.__new__(ExecutionChannel)
        ex.ipc_dir = str(tmp_path)
        job = Job("h", "x")
        result = ex.consume_stale_result("t::x", job)
        assert result is not None and result.success, (
            f"consume_stale_result 应消费最新执行代成功结果: {result.result_meta if result else None}")
 # 成功路径的 result_meta 即归一化后的 handler 返回值 dict
        assert result.result_meta == {"v": 2}, (
            f"同刻冲突必须取最新执行代: {result.result_meta}")

    def test_worker_rejects_missing_incarnation(self, tmp_path):
        """incarnation 缺失 fail-loud（防 .None.result.json 垃圾路径）。"""
        from tasklite.engine.channel import _mp_worker_wrapper

        ctx = TaskContext(Job("t", "x"), set, set, {}) # 无 incarnation
        with pytest.raises(RuntimeError, match="incarnation"):
            _mp_worker_wrapper(lambda j, c: (True, {}), Job("t", "x"), ctx,
                               str(tmp_path))
