"""停机状态机（DRAINING / ABORTING）回归测试。

覆盖：
- stop() 默认 DRAINING：不再派发新 job，允许 in-flight 自然完成
- stop(force=True) ABORTING：kill 全部 in-flight + 清理半成品输出
- DRAINING 后未派发 job 保留在队列（下次 run 继续）
- 已完成（结果文件落盘）的 in-flight job 在 ABORTING 时
  结果被消费进 wall、成功输出保留、不 requeue（非幂等副作用不重跑）
"""

import threading
import time

from tasklite.pipeline import TaskLite
from tasklite.models.job import Job
from tasklite.utils.ipc import ArtifactJournal
from tests.helpers import make_ipc_process_class, patch_multiprocessing_for_fakes

# ══════════════════════════════════════════════════════════════════════
# 模块级 handler（FakeProcess 内部用 ForkingPickler 校验可 pickle；
# FakeProcess 不执行 target，handler 体仅真实子进程场景下运行）。
# ══════════════════════════════════════════════════════════════════════


def _slow_handler(job, ctx):
    """慢 handler：真实子进程场景下运行约 5s 后自然完成（DRAINING 语义）。

    TaskContext 不暴露停机态（stop_mode 归主进程 runtime 所有，子进程
    handler 无从查询），故不做 stop 检查、仅按 deadline 自然完成；
    「DRAINING 等 in-flight 自然完成」的行为由 stay-alive fake 进程
    （_make_releasable_process_class）在测试中验证。
    """
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        time.sleep(0.01)
    return True, {"completed_naturally": True}


def _fast_handler(job, ctx):
    return True


def _make_releasable_process_class(release_event):
    """FakeProcess：start() 后保持 alive，release_event 置位后写成功结果并退出。

    DRAINING 测试专用：模拟「真实 handler 仍在运行、stop() 时 in-flight 非空」，
    之后由测试主动放行完成——验证 DRAINING 等 in-flight 自然完成并 commit。
    """
    from tests.helpers import _write_fake_result

    class ReleasableProcess:
        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self.args = args
            self._alive = True
            self._killed = False
            self.exitcode = 0
            self._written = False

        def start(self):
            self._alive = True

        def _complete_if_released(self):
            if not release_event.is_set() or self._written:
                return
            self._written = True
            if self.args:
                _spec = self.args[0]
                _write_fake_result(_spec.ipc_dir, _spec.job.uid, {
                    "status": "success",
                    "raw_result": True,
                    "new_jobs": [],
                    "resource_suspensions": [],
                    "cursor_updates": {},
                }, incarnation=_spec.incarnation, auth_token=getattr(_spec, "result_token", None))
            self._alive = False

        def is_alive(self):
            self._complete_if_released()
            return self._alive

        def join(self, timeout=None):
            self._complete_if_released()

        def kill(self):
            self._alive = False
            self._killed = True
            self.exitcode = -9

    return ReleasableProcess


class TestGracefulDraining:
    def test_stop_waits_for_inflight_to_complete(self, tmp_path, monkeypatch):
        """DRAINING：stop() 时 in-flight 仍在运行 → 不 kill，等它自然完成并 commit。

        旧测试用 make_fake_process_class("success")：start() 即写结果，job 在
        stop()（0.3s 后）之前已跑完，run() 早已返回——DRAINING 分支
        （stop_requested + in-flight 非空）从未执行（false positive）。
        新测试用可释放的 stay-alive 进程：stop() 在 in-flight 期间触发，
        随后放行让其自然完成。
        """
        import threading

        release = threading.Event()
        patch_multiprocessing_for_fakes(
            monkeypatch,
            fake_process_class=_make_releasable_process_class(release),
        )
        pipeline = TaskLite(
            name="test_drain", state_dir=tmp_path / "state", backend="sqlite", max_workers=1,
        )
        pipeline.register_handler("slow", _slow_handler)
        pipeline.enqueue([Job("slow", "j1", payload={})])

        observed = {}

        def controller():
            # 等 job 派发进 in-flight
            for _ in range(200):
                if pipeline._runtime.in_flight:
                    break
                time.sleep(0.01)
            observed["inflight_at_stop"] = len(pipeline._runtime.in_flight)
            entry = next(iter(pipeline._runtime.in_flight.values()))
            proc = entry.handle.process
            pipeline.stop()  # 默认 DRAINING：不 kill
            # DRAINING 窗口内 in-flight 进程必须仍存活（未被 kill）
            time.sleep(0.1)
            observed["alive_during_draining"] = proc.is_alive()
            # 放行：in-flight 自然完成
            release.set()

        t = threading.Thread(target=controller)
        t.start()
        pipeline.run()
        t.join()

        assert observed["inflight_at_stop"] == 1, "stop() 时 in-flight 必须非空（DRAINING 前提）"
        assert observed["alive_during_draining"] is True, "DRAINING 不得 kill in-flight 进程"
        # DRAINING：in-flight job 完成并进 wall（未 kill、结果已 commit）
        assert "slow::j1" in pipeline.backend.load_wall(), \
            "DRAINING 必须等 in-flight job 完成进 wall，而非 kill"

    def test_stop_keeps_undispatched_jobs_for_next_run(self, tmp_path, monkeypatch):
        """DRAINING：stop() 时未派发的 job 保留在磁盘队列，下次 run 继续。

        旧测试用即时完成 fake：stop()（0.3s 后）时 run() 早已结束，
        「DRAINING 期间不派发新 job」的行为从未被验证（false positive）。
        新测试：j1 在 stop() 时 in-flight 运行中，j2 未派发（max_workers=1
        槽位被 j1 占用）；DRAINING 等 j1 完成后不再派发 j2，j2 保留在队列。
        """
        import threading

        release = threading.Event()
        patch_multiprocessing_for_fakes(
            monkeypatch,
            fake_process_class=_make_releasable_process_class(release),
        )
        pipeline = TaskLite(
            name="test_drain_keep", state_dir=tmp_path / "state", backend="sqlite", max_workers=1,
        )
        pipeline.register_handler("fast", _fast_handler)
        pipeline.enqueue([Job("fast", "j1", payload={}), Job("fast", "j2", payload={})])

        observed = {}

        def controller():
            for _ in range(200):
                if pipeline._runtime.in_flight:
                    break
                time.sleep(0.01)
            pipeline.stop()
            time.sleep(0.1)
            observed["queue_during_draining"] = [
                j.get("job_id") for j in pipeline._runtime.state.queue
            ]
            release.set()

        t = threading.Thread(target=controller)
        t.start()
        pipeline.run()
        t.join()

        assert observed["queue_during_draining"] == ["j2"], \
            "DRAINING 期间不得派发 j2，j2 必须保留在队列"
        # run 退出后：j1 进 wall，j2 仍在磁盘队列（下次 run 继续）
        assert "fast::j1" in pipeline.backend.load_wall(), "j1 必须正常完成进 wall"
        assert "fast::j2" not in pipeline.backend.load_wall(), "j2 绝不得在本次 run 运行"
        remaining = pipeline.backend.load_queue()
        assert any(j.get("job_id") == "j2" for j in remaining), \
            "j2 必须保留在磁盘队列（下次 run 恢复）"


class TestForceAbort:
    def test_force_abort_cleans_partial_outputs_and_keeps_in_queue(self, tmp_path, monkeypatch):
        """ABORTING：stop(force=True) 清理被 kill 的 in-flight job 半成品输出。

        语义不变式：被 kill 的 job 产物（outputs.jsonl 声明的物理路径）必须
        被清理（防止半成品残留脏数据），job 未 commit 保留在磁盘队列。
        """
        # stay-alive fake：job 永远运行中，模拟被 kill 的长任务
        patch_multiprocessing_for_fakes(
            monkeypatch,
            fake_process_class=make_ipc_process_class(results=[None], stay_alive=True),
        )
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        pipeline = TaskLite(
            name="test_abort_clean", state_dir=tmp_path / "state", backend="sqlite",
            max_workers=1, output_root=str(out_dir),
        )
        pipeline.register_handler("out", _fast_handler)
        pipeline.enqueue([Job("out", "j1", payload={})])

        partial = out_dir / "partial.txt"
        partial.write_text("half-written")

        import threading
        injected = {}

        def inject_output():
            # 轮询派发完成（上限 5s），再注入 + stop，避免竞态失败。
            deadline = time.time() + 5.0
            while not pipeline._runtime.in_flight and time.time() < deadline:
                time.sleep(0.01)
            # 模拟 handler 副作用：把输出声明写入落盘 outputs.jsonl
            # （后真实 handler 的 declare_output 即落盘；FakeProcess
            # 不执行 handler，此处直接写文件模拟）。
            for entry in pipeline._runtime.in_flight.values():
                ArtifactJournal(pipeline.ipc_dir).record_output(entry.uid, str(partial), True)
                injected["done"] = True
            pipeline.stop(force=True)

        t = threading.Thread(target=inject_output)
        t.start()
        pipeline.run()
        t.join()

        assert injected.get("done"), "注入输出声明的线程应执行"
        # ABORTING：半成品输出被清理
        assert not partial.exists(), \
            "ABORTING 必须清理被 kill job 的半成品输出"
        # 作业未 commit（未进 wall），留在磁盘队列可重跑
        assert "out::j1" not in pipeline.backend.load_wall()
        remaining = pipeline.backend.load_queue()
        assert any(j.get("job_id") == "j1" for j in remaining), \
            "ABORTING 后未 commit 的 job 必须保留在队列（at-least-once）"

    def test_stop_mode_semantics(self, tmp_path):
        """stop 置 DRAINING；stop(force=True) 置 ABORTING（单枚举状态机）。"""
        from tasklite.engine.types import StopMode
        pipeline = TaskLite(name="t", state_dir=tmp_path / "state", backend="sqlite")
        assert pipeline._runtime.stop_mode is StopMode.NONE
        pipeline.stop()
        assert pipeline._runtime.stop_mode is StopMode.DRAINING

        pipeline2 = TaskLite(name="t2", state_dir=tmp_path / "s2", backend="sqlite")
        pipeline2.stop(force=True)
        assert pipeline2._runtime.stop_mode is StopMode.ABORTING

    def test_force_abort_during_inflight_kills_and_requeues(self, tmp_path, monkeypatch):
        """ABORTING：stop(force=True) 在 in-flight 期间 kill 子进程并 requeue。

        旧 DRAINING 测试的即时完成 fake 使 stop() 永远赶不上 in-flight 运行期，
        kill 路径无从命中。此测试用 stay-alive fake：job 在 in-flight 时
        stop(force=True) → 子进程被 kill、job 未 commit、保留在磁盘队列。
        """
        import threading

        patch_multiprocessing_for_fakes(
            monkeypatch,
            fake_process_class=make_ipc_process_class(results=[None], stay_alive=True),
        )
        pipeline = TaskLite(
            name="test_abort_kill", state_dir=tmp_path / "state", backend="sqlite", max_workers=1,
        )
        pipeline.register_handler("fast", _fast_handler)
        pipeline.enqueue([Job("fast", "j1", payload={})])

        observed = {}

        def controller():
            for _ in range(200):
                if pipeline._runtime.in_flight:
                    break
                time.sleep(0.01)
            observed["proc"] = next(iter(pipeline._runtime.in_flight.values())).handle.process
            pipeline.stop(force=True)

        t = threading.Thread(target=controller)
        t.start()
        pipeline.run()
        t.join()

        proc = observed["proc"]
        # _abort_in_flight → executor.cleanup 已 kill 该子进程
        assert proc.is_alive() is False, "ABORTING 必须 kill in-flight 子进程"
        assert proc.exitcode == -9, f"kill 后 exitcode 应为 -9, got {proc.exitcode}"
        # job 未 commit（未进 wall），保留在磁盘队列（at-least-once）
        assert "fast::j1" not in pipeline.backend.load_wall()
        remaining = pipeline.backend.load_queue()
        assert any(j.get("job_id") == "j1" for j in remaining), \
            "ABORTING 后未 commit 的 job 必须保留在队列"


class TestAbortConsumesCompletedResult:
    """已完成 in-flight job 在 ABORTING 时结果被消费：

    时序保证：``_abort_in_flight`` 先按当前 incarnation 的结果文件存在性分类——
    存在则走 ``_complete_job`` 伪 entry 提交消费（不 kill、不删成功输出、不 requeue）；
    只有无结果文件的 entry 才 kill + 清半成品 + requeue。
    """

    def _inject_completed_result(self, pipeline, out_file):
        """模拟 handler 已完成：物理输出 + 声明 + 当前 incarnation 成功结果落盘。"""
        journal = ArtifactJournal(pipeline.ipc_dir)
        entry = next(iter(pipeline._runtime.in_flight.values()))
        out_file.write_text("done")
        journal.record_output(entry.uid, str(out_file), True)
        journal.write_result_atomic(entry.uid, {
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
            # 真实 worker 经 spec 拿到的本 run 认证令牌（读取侧强校验的合法形态）
            "auth": getattr(pipeline._runtime.channel, "result_token", None),
        }, incarnation=entry.handle.incarnation)
        return journal.result_path(entry.uid, entry.handle.incarnation).exists()

    def test_force_abort_consumes_completed_result(self, tmp_path, monkeypatch):
        """ABORTING：job 完成（结果+输出存在）后、drain 前 stop(force=True)。

        变异体（删除 _abort_in_flight 的 done 消费分支——完成 entry 被当
        失败处理）下：结果文件被删、成功输出被清、job 被 requeue → 本断言
        失败（非幂等副作用将被重启重跑重复执行）。
        """
        patch_multiprocessing_for_fakes(
            monkeypatch,
            fake_process_class=make_ipc_process_class(results=[None], stay_alive=True),
        )
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        pipeline = TaskLite(
            name="test_abort_done", state_dir=tmp_path / "state", backend="sqlite",
            max_workers=1, output_root=str(out_dir),
        )
        pipeline.register_handler("fast", _fast_handler)
        pipeline.enqueue([Job("fast", "j1", payload={})])

        result_file = out_dir / "result.txt"
        observed = {}

        def controller():
            # 等 job 派发进 in-flight（drain 轮询前）
            for _ in range(300):
                if pipeline._runtime.in_flight:
                    break
                time.sleep(0.01)
            observed["result_at_abort"] = self._inject_completed_result(
                pipeline, result_file)
            pipeline.stop(force=True)

        t = threading.Thread(target=controller)
        t.start()
        pipeline.run()
        t.join()

        assert observed.get("result_at_abort") is True, \
            "前置：abort 前结果文件必须已落盘（模拟 job 已完成）"
        # 结果被消费：进 wall 而非失败/requeue
        assert "fast::j1" in pipeline.backend.load_wall(), \
            "已完成 job 的结果必须被 _abort_in_flight 消费提交（进 wall）"
        # 成功产出的物理输出保留（不得被 _cleanup_outputs 误删）
        assert result_file.exists(), "已完成 job 的成功输出必须保留"
        # 不 requeue：磁盘队列不应再有该 uid（重启不重跑）
        remaining = pipeline.backend.load_queue()
        remaining_uids = [Job.from_dict(j).uid for j in remaining]
        assert "fast::j1" not in remaining_uids, \
            f"已完成 job 不得 requeue（否则重启重跑、非幂等副作用双跑）: {remaining_uids}"

    def test_abort_consumes_completed_result_simulated(self, tmp_path):
        """模拟 abort（直接调 _abort_in_flight）：done entry 不 kill、结果消费。

        完全确定性的单测变体：手工构造「完成未 drain」窗口的 in-flight 状态
        （结果文件 + 声明 + 物理输出均已落盘、entry 已注册），直接触发
        _abort_in_flight。修复前：kill + 删结果文件 + 删成功输出 + requeue。
        """
        from tasklite.engine.channel import JobHandle
        from tasklite.models.state import PipelineState
        from tasklite.engine.inflight import InFlightJob as _InFlightJob

        pipeline = TaskLite(
            name="test_abort_sim", state_dir=tmp_path / "state", backend="sqlite",
            output_root=str(tmp_path / "out"),
        )
        state = PipelineState({}, {}, {}, [])
        pipeline._runtime.store.set_state(state)
        pipeline._runtime.in_flight.clear()

        job = Job("fast", "j1", payload={})
        job_dict = job.to_dict()
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        out_file = tmp_path / "out" / "result.txt"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("done")
        journal = ArtifactJournal(pipeline.ipc_dir)
        journal.record_output("fast::j1", str(out_file), True)
        journal.write_result_atomic("fast::j1", {
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
        }, incarnation=inc)

        killed = []

        class DoneProcess:
            """已完成 entry 的进程桩——abort 不得 kill 它（结果已落盘）。"""

            def __init__(self):
                self._alive = True
                self.exitcode = 0

            def is_alive(self):
                return self._alive

            def join(self, timeout=None):
                self._alive = False

            def kill(self):
                self._alive = False
                killed.append(1)

        handle = JobHandle(
            uid="fast::j1", process=DoneProcess(), deadline=0.0, timeout=60.0,
            job=job, ipc_dir=pipeline.ipc_dir, incarnation=inc,
        )
        entry = _InFlightJob(
            uid="fast::j1", job_dict=job_dict, job=job,
            acquired=[], handle=handle, job_start=None,
        )
        pipeline._runtime.in_flight["fast::j1"] = entry
        state.register_in_flight("fast::j1")

        pipeline._runtime._recovery.abort_in_flight()

        assert killed == [], "已完成 entry 不得被 kill（进程已自然退出或即将退出）"
        assert "fast::j1" in pipeline.backend.load_wall(), \
            "已完成 job 的结果必须被消费提交（进 wall）"
        assert out_file.exists(), "已完成 job 的成功输出必须保留"
        remaining = pipeline.backend.load_queue()
        assert all(Job.from_dict(j).uid != "fast::j1" for j in remaining), \
            "已完成 job 不得 requeue（否则重启重跑、非幂等副作用双跑）"
        assert not state.in_flight_uids and not pipeline._runtime.in_flight, \
            "abort 后 in-flight 必须清空（state + 内存 dict）"


class TestAbortTOCTOU:
    """分类与 kill 之间 worker 完成写结果的窗口防御：

    时序保证：kill 只做进程收割（``executor.finalize_processes``，不删 IPC
    文件），随后重查 pending 结果文件——kill 后新出现的 entry 移入 done
    消费（不删成功输出、不 requeue）；剩余 pending 才统一清理。
    """

    def test_abort_toctou_result_appears_during_kill(self, tmp_path):
        """确定性复现：分类时无结果 → finalize（kill/join）时结果文件出现。

        模拟「分类读到 pending → kill 窗口内 worker 恰好完成写结果」——
        结果文件在 finalize 之前不存在、finalize 之后才出现（在进程桩的
        join/kill 回调里写入）。变异体（删 kill 后重查逻辑）下：结果被删 +
        成功输出被清 + requeue → 本断言失败。
        """
        from tasklite.engine.channel import JobHandle
        from tasklite.models.state import PipelineState
        from tasklite.engine.inflight import InFlightJob as _InFlightJob

        pipeline = TaskLite(
            name="test_abort_toctou", state_dir=tmp_path / "state", backend="sqlite",
            output_root=str(tmp_path / "out"),
        )
        state = PipelineState({}, {}, {}, [])
        pipeline._runtime.store.set_state(state)
        pipeline._runtime.in_flight.clear()

        job = Job("fast", "j1", payload={})
        job_dict = job.to_dict()
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        out_file = tmp_path / "out" / "result.txt"
        out_file.parent.mkdir(parents=True, exist_ok=True)

        journal = ArtifactJournal(pipeline.ipc_dir)
        # 前置：输出已物理写完（handler 已完成产出），但结果文件**尚未**落盘
        # （worker 还在 write_result_atomic 之前）。声明也先落盘——模拟
        # handler 已执行完毕、只差结果写入的最后一步。
        journal.record_output("fast::j1", str(out_file), True)

        # 进程桩：kill/join 时（finalize_processes 收割窗口）才完成结果写入——
        # 精确复现「分类读到无结果 → kill 窗口内结果出现」的 TOCTOU。
        written_in_kill = []

        class KillWindowProcess:
            def __init__(self):
                self._alive = True
                self.exitcode = 0

            def is_alive(self):
                return self._alive

            def join(self, timeout=None):
                # 模拟 worker 在收割窗口内完成 write_result_atomic
                out_file.write_text("done")
                journal.write_result_atomic("fast::j1", {
                    "status": "success", "raw_result": True,
                    "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
                }, incarnation=inc)
                written_in_kill.append(1)
                self._alive = False

            def kill(self):
                self._alive = False

        handle = JobHandle(
            uid="fast::j1", process=KillWindowProcess(), deadline=0.0, timeout=60.0,
            job=job, ipc_dir=pipeline.ipc_dir, incarnation=inc,
        )
        entry = _InFlightJob(
            uid="fast::j1", job_dict=job_dict, job=job,
            acquired=[], handle=handle, job_start=None,
        )
        pipeline._runtime.in_flight["fast::j1"] = entry
        state.register_in_flight("fast::j1")

        # 前置断言：分类时（_abort_in_flight 内）结果文件尚不存在——
        # 保证入口确实走「pending → finalize → 重查」路径而非「直接 done」。
        assert not journal.result_path("fast::j1", inc).exists(), \
            "前置：结果文件必须在 abort 前不存在（TOCTOU 触发条件）"

        pipeline._runtime._recovery.abort_in_flight()

        assert written_in_kill == [1], "进程桩必须模拟 kill 窗口内的结果写入"
        # 结果被消费：进 wall 而非 requeue
        assert "fast::j1" in pipeline.backend.load_wall(), \
            "kill 后新出现的结果必须被重查消费提交（进 wall）"
        assert out_file.exists(), "kill 后出现的成功结果不得被 _cleanup_outputs 误删"
        remaining = pipeline.backend.load_queue()
        assert all(Job.from_dict(j).uid != "fast::j1" for j in remaining), \
            f"TOCTOU 窗口内完成的 job 不得 requeue（否则重启重跑）: {remaining}"

    def test_abort_mixed_done_and_pending_no_assert_crash(self, tmp_path):
        """abort 时 in-flight 同时含「已完成 done」与
        「未完成 pending」必须不崩溃。

        requeue 前先 unregister pending，全程保持活动集合互斥。
        """
        from tasklite.engine.channel import JobHandle
        from tasklite.models.state import PipelineState
        from tasklite.engine.inflight import InFlightJob as _InFlightJob

        pipeline = TaskLite(
            name="test_mixed_abort", state_dir=tmp_path / "state", backend="sqlite",
            output_root=str(tmp_path / "out"),
        )
        state = PipelineState({}, {}, {}, [])
        pipeline._runtime.store.set_state(state)
        pipeline._runtime.in_flight.clear()

        # ── done entry：结果已落盘（无结果文件则归 pending）──
        job_done = Job("a", "done", payload={})
        done_dict = job_done.to_dict()
        inc_done = "d0000000000000000000000000000000.1"
        ArtifactJournal(pipeline.ipc_dir).write_result_atomic("a::done", {
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
        }, incarnation=inc_done)

        class DoneProcess:
            def __init__(self):
                self._alive = True
                self.exitcode = 0
            def is_alive(self): return self._alive
            def join(self, timeout=None): self._alive = False
            def kill(self): self._alive = False

        handle_done = JobHandle(
            uid="a::done", process=DoneProcess(), deadline=0.0, timeout=60.0,
            job=job_done, ipc_dir=pipeline.ipc_dir, incarnation=inc_done,
        )
        entry_done = _InFlightJob(
            uid="a::done", job_dict=done_dict, job=job_done,
            acquired=[], handle=handle_done, job_start=None,
        )
        pipeline._runtime.in_flight["a::done"] = entry_done

        # ── pending entry：无结果文件（走 kill + requeue）──
        job_pend = Job("b", "pending", payload={})
        pend_dict = job_pend.to_dict()
        inc_pend = "0000000000000000000000000000000b.1"

        class PendingProcess:
            def __init__(self):
                self._alive = True
                self.exitcode = -1
            def is_alive(self): return self._alive
            def join(self, timeout=None): self._alive = False
            def kill(self): self._alive = False

        handle_pend = JobHandle(
            uid="b::pending", process=PendingProcess(), deadline=0.0, timeout=60.0,
            job=job_pend, ipc_dir=pipeline.ipc_dir, incarnation=inc_pend,
        )
        entry_pend = _InFlightJob(
            uid="b::pending", job_dict=pend_dict, job=job_pend,
            acquired=[], handle=handle_pend, job_start=None,
        )
        pipeline._runtime.in_flight["b::pending"] = entry_pend

        state.register_in_flight("a::done")
        state.register_in_flight("b::pending")

        # 修复前此调用抛 AssertionError（queue∩in_flight 互斥被破坏）
        pipeline._runtime._recovery.abort_in_flight()

        # done 被消费进 wall；pending 被 requeue（内存 queue）。
        # 注意：本测试用手工构造的 PipelineState（未走真实后端加载），
        # requeue 只进内存 state.queue；故用内存断言而非 backend.load_queue。
        assert "a::done" in pipeline.backend.load_wall(), \
            "done entry 的结果必须被消费提交（进 wall）"
        q_uids = [Job.from_dict(j).uid for j in pipeline._runtime.state.queue]
        assert "b::pending" in q_uids, \
            f"pending entry 必须被 requeue（at-least-once）: {q_uids}"
        assert "a::done" not in pipeline.backend.load_failed(), \
            "done entry（成功结果）不得被误判失败"

    def test_abort_preserves_suspend_signals(self, tmp_path):
        """项 1：abort 时必须先消费 in-flight 的 suspend 信号，
        否则被 kill job 落盘的限流信息随 signals 文件删除而丢失。

        修复前：_abort_in_flight 直接 kill + cleanup_ipc_files（删除
        signals 文件），`{uid}.signals.jsonl` 未被读取 → suspend 状态丢失，
        违背「崩溃/强制停机保留 429 限流状态」的文档承诺。
        修复后：abort 开头先 _apply_pending_signals 读到并应用，重启后由
        _persist_resource_suspends 持久化。
        """
        import time as time_mod
        from tasklite.engine.channel import JobHandle
        from tasklite.engine.resource import CapacityResource
        from tasklite.models.state import PipelineState
        from tasklite.engine.inflight import InFlightJob as _InFlightJob

        pipeline = TaskLite(
            name="test_abort_suspend", state_dir=tmp_path / "state", backend="sqlite",
            output_root=str(tmp_path / "out"),
        )
        pipeline.add_resource(CapacityResource("api", max_capacity=10.0))
        state = PipelineState({}, {}, {}, [])
        pipeline._runtime.store.set_state(state)
        pipeline._runtime.in_flight.clear()

        job = Job("x", "j1", payload={}, resources={"api": 1.0})
        job_dict = job.to_dict()
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"

        class PendingProcess:
            def __init__(self):
                self._alive = True
                self.exitcode = -1
            def is_alive(self): return self._alive
            def join(self, timeout=None): self._alive = False
            def kill(self): self._alive = False

        handle = JobHandle(
            uid="x::j1", process=PendingProcess(), deadline=0.0, timeout=60.0,
            job=job, ipc_dir=pipeline.ipc_dir, incarnation=inc,
        )
        entry = _InFlightJob(
            uid="x::j1", job_dict=job_dict, job=job,
            acquired=[], handle=handle, job_start=None,
        )
        pipeline._runtime.in_flight["x::j1"] = entry
        state.register_in_flight("x::j1")

        # handler 落盘的 suspend 信号（写文件、flush）
        ArtifactJournal(pipeline.ipc_dir).record_signal("x::j1", "api", 60.0)

        now = time_mod.monotonic()
        pipeline._runtime._recovery.abort_in_flight()

        # 资源必须已被挂起（信号被消费应用，而非随文件删除丢失）
        suspended_until = pipeline.resources["api"].suspended_until()
        assert suspended_until is not None and suspended_until > now, \
            f"abort 必须消费并应用 suspend 信号，实际 suspended_until={suspended_until}"


class TestAbortSalvagedSignalApplication:
    """abort 收尾「杀进程后补排空」的应用点契约。

    cancelled 任务的信号文件已随半成品清理删除，channel 捞回的 suspend
    信号只能经 AbortOutcome.salvaged_signals 带回；recovery 必须将其应用
    到 ResourceManager 并即时持久化（suspend 不丢承诺的最后一环）。
    """

    def test_salvaged_signals_applied_and_persisted(self):
        from types import SimpleNamespace

        from tasklite.backend.memory import InMemoryStateBackend
        from tasklite.engine.channel import AbortOutcome
        from tasklite.engine.inflight import InFlightJob, InFlightTracker
        from tasklite.engine.recovery import RecoveryOrchestrator
        from tasklite.engine.resource import (
            META_RESOURCE_SUSPENDS,
            CapacityResource,
            ResourceManager,
        )
        from tasklite.engine.store import StateStore
        from tasklite.engine.policy import ExecutionPolicy
        from tasklite.models.job import Job
        from tasklite.taxonomy import ErrorTaxonomy
        from tasklite.utils.jsonutil import loads

        backend = InMemoryStateBackend()
        store = StateStore(
            backend,
            commit_failure_dlq_threshold=3,
            taxonomy=ErrorTaxonomy(),
        )
        resources = ResourceManager()
        resources["gpu"] = CapacityResource("gpu", 1.0)

        job = Job("t", "j1")
        in_flight = InFlightTracker()
        in_flight.track(
            InFlightJob(
                uid=job.uid,
                job_dict=job.to_dict(),
                job=job,
            )
        )

        def fake_abort(handles):
            assert [h for h in handles] == []
            return AbortOutcome(
                completed=[],
                cancelled=[],
                salvaged_signals=[(job.uid, "gpu", 30.0)],
            )

        channel = SimpleNamespace(
            drain_active_signals=lambda uids: [],
            abort_in_flight=fake_abort,
        )
        settled = {}
        completion = SimpleNamespace(
            settle_aborted=lambda cancelled, done: settled.update(
                cancelled=[e.uid for e in cancelled]
            )
        )
        recovery = RecoveryOrchestrator(
            store=store,
            channel=channel,
            resources=resources,
            in_flight=in_flight,
            policy=ExecutionPolicy(),
            completion=completion,
        )

        recovery.abort_in_flight()

        # 信号已应用（资源挂起生效）并即时持久化到 meta
        assert "gpu" in resources.collect_suspensions()
        persisted = loads(backend.get_meta(META_RESOURCE_SUSPENDS))
        assert "gpu" in persisted
        # 收尾结算照常进行（cancelled 分支 requeue）
        assert settled["cancelled"] == [job.uid]

    def test_unregistered_resource_salvaged_signal_skipped(self):
        """未注册资源的捞回信号告警跳过，不中断 abort 收尾。"""
        from types import SimpleNamespace

        from tasklite.backend.memory import InMemoryStateBackend
        from tasklite.engine.channel import AbortOutcome
        from tasklite.engine.inflight import InFlightJob, InFlightTracker
        from tasklite.engine.recovery import RecoveryOrchestrator
        from tasklite.engine.resource import (
            META_RESOURCE_SUSPENDS,
            CapacityResource,
            ResourceManager,
        )
        from tasklite.engine.store import StateStore
        from tasklite.engine.policy import ExecutionPolicy
        from tasklite.models.job import Job
        from tasklite.taxonomy import ErrorTaxonomy

        backend = InMemoryStateBackend()
        store = StateStore(
            backend,
            commit_failure_dlq_threshold=3,
            taxonomy=ErrorTaxonomy(),
        )
        resources = ResourceManager()
        resources["gpu"] = CapacityResource("gpu", 1.0)

        job = Job("t", "j2")
        in_flight = InFlightTracker()
        in_flight.track(InFlightJob(uid=job.uid, job_dict=job.to_dict(), job=job))

        channel = SimpleNamespace(
            drain_active_signals=lambda uids: [],
            abort_in_flight=lambda handles: AbortOutcome(
                completed=[],
                cancelled=[],
                salvaged_signals=[(job.uid, "no_such_resource", 9.0)],
            ),
        )
        recovery = RecoveryOrchestrator(
            store=store,
            channel=channel,
            resources=resources,
            in_flight=in_flight,
            policy=ExecutionPolicy(),
            completion=SimpleNamespace(settle_aborted=lambda c, d: None),
        )

        recovery.abort_in_flight()

        assert backend.get_meta(META_RESOURCE_SUSPENDS) is None
        assert "gpu" not in resources.collect_suspensions()


class TestAbortInitialScanDonePairSignalDrain:
    """abort 初扫「结果已落盘」分类的信号排空契约。

    不变式：初扫检出结果文件 ⇒ 该执行体的信号追加必然全部早于结果原子
    落盘（record_signal 只发生在 handler 执行期内），此刻排空无并发写者、
    无损；缺此步时 done 对经 settle_aborted → complete_job 的收尾清理会把
    「abort 先排空阶段之后写入」的信号文件未读删除。
    """

    @staticmethod
    def _make_handle(tmp_path, uid="t::victim", inc="a" * 32 + ".1", alive=False):
        from tasklite.engine.channel import JobHandle

        class _Proc:
            def __init__(self):
                self._alive = bool(alive)
                self.exitcode = 0

            def is_alive(self):
                return self._alive

            def join(self, timeout=None):
                self._alive = False

            def kill(self):
                self._alive = False

            def close(self):
                pass

        return JobHandle(
            uid=uid, process=_Proc(), deadline=0.0, timeout=60.0,
            job=Job("t", uid.split("::", 1)[1]),
            ipc_dir=str(tmp_path), incarnation=inc,
        )

    def test_retry_done_pair_drains_signal_file(self, tmp_path):
        """retry done 对：结果 payload 不携带挂起，文件信号必须随初扫排空带出。"""
        from tasklite.engine.channel import ExecutionChannel

        ch = ExecutionChannel(ipc_dir=tmp_path)
        journal = ArtifactJournal(tmp_path)
        handle = self._make_handle(tmp_path)
        journal.record_signal(handle.uid, "api", 60.0)
        journal.write_result_atomic(handle.uid, {
            "status": "retry", "error": "HTTP 429", "transient_kind": "rate_limited",
        }, incarnation=handle.incarnation)

        outcome = ch.abort_in_flight([handle])

        assert len(outcome.completed) == 1
        suspensions = outcome.completed[0][1].resource_suspensions
        assert ("api", 60.0) in suspensions, (
            "初扫 done 对必须排空信号文件并合并进结果挂起"
        )
        # 排空先于收尾清理：信号文件不残留
        assert not journal.signals_path(handle.uid).exists()

    def test_pending_path_salvage_control(self, tmp_path):
        """对照：初扫无结果（pending）路径的杀后补排空不受影响。"""
        from tasklite.engine.channel import ExecutionChannel

        ch = ExecutionChannel(ipc_dir=tmp_path)
        journal = ArtifactJournal(tmp_path)
        handle = self._make_handle(tmp_path, alive=True)
        journal.record_signal(handle.uid, "api", 60.0)

        outcome = ch.abort_in_flight([handle])

        assert outcome.completed == []
        assert outcome.salvaged_signals == [(handle.uid, "api", 60.0)]

    def test_success_done_pair_merges_payload_and_file_signals(self, tmp_path):
        """success payload 挂起与文件信号并存时两者都带入（max 语义幂等）。"""
        from tasklite.engine.channel import ExecutionChannel

        ch = ExecutionChannel(ipc_dir=tmp_path)
        journal = ArtifactJournal(tmp_path)
        handle = self._make_handle(tmp_path)
        journal.record_signal(handle.uid, "api", 10.0)
        journal.write_result_atomic(handle.uid, {
            "status": "success", "raw_result": True, "new_jobs": [],
            "resource_suspensions": [["api", 60.0]], "cursor_updates": {},
        }, incarnation=handle.incarnation)

        outcome = ch.abort_in_flight([handle])

        suspensions = outcome.completed[0][1].resource_suspensions
        assert ("api", 60.0) in suspensions
        assert ("api", 10.0) in suspensions


class TestAbortDonePairSignalDrainOrchestration:
    """abort 编排层 done 对信号回收端到端。

    注入点取 ``channel.abort_in_flight`` 调用沿，确定性等价于「worker 在
    recovery 第 1 步排空之后、初扫之前完成 record_signal + 结果落盘」：
    该 handle 初扫即落 done 对；不排空则 settle_aborted → complete_job 的
    收尾清理把信号文件未读删除。对照（只写信号不写结果）走 pending 路径。
    """

    def _run_abort_scenario(self, tmp_path, monkeypatch, with_retry_result):
        from pathlib import Path
        from unittest import mock

        from tasklite.engine.resource import CapacityResource

        fake_cls = make_ipc_process_class(results=[None], stay_alive=True)
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=fake_cls)
        pipeline = TaskLite(
            name="abort_donepair", state_dir=tmp_path / "state",
            backend="sqlite", max_workers=2,
        )
        pipeline.add_resource(CapacityResource("api", max_capacity=1.0))
        pipeline.register_handler("t", _fast_handler)
        pipeline.enqueue([Job("t", "victim", payload={}, max_retries=3)])

        ctx = mock.MagicMock()
        ctx.Process = lambda target, args, **kw: fake_cls(target=target, args=args)
        patcher = mock.patch.object(pipeline._runtime.channel, "_mp_ctx", ctx)
        patcher.start()
        try:
            pipeline._runtime.prepare_run_state()
            pipeline._runtime.step()
            assert pipeline._runtime.in_flight, "dispatch failed"
            entry = next(iter(pipeline._runtime.in_flight.values()))
            uid, inc = entry.uid, entry.handle.incarnation

            journal = ArtifactJournal(pipeline.ipc_dir)
            real_abort = pipeline._runtime.channel.abort_in_flight

            def injecting_abort(handles):
                # 模拟 worker 在第 1 步排空之后的终前写入
                journal.record_signal(uid, "api", 60.0)
                if with_retry_result:
                    journal.write_result_atomic(uid, {
                        "status": "retry", "error": "HTTP 429", "transient_kind": "rate_limited",
                    }, incarnation=inc)
                return real_abort(handles)

            pipeline.stop(force=True)
            with mock.patch.object(
                pipeline._runtime.channel, "abort_in_flight", injecting_abort
            ):
                pipeline._runtime.step()

            deadline = pipeline.resources["api"].suspended_until()
            applied = deadline is not None and deadline > time.monotonic()
            leftover = list(Path(pipeline.ipc_dir).glob("*.signals.jsonl"))
            return applied, leftover
        finally:
            patcher.stop()

    def test_retry_done_pair_signal_applied_not_deleted(self, tmp_path, monkeypatch):
        applied, leftover = self._run_abort_scenario(
            tmp_path, monkeypatch, with_retry_result=True
        )
        assert applied, "abort done 对的 suspend 信号必须被应用而非随清理丢失"
        assert leftover == []

    def test_pending_path_control_salvaged(self, tmp_path, monkeypatch):
        applied, leftover = self._run_abort_scenario(
            tmp_path, monkeypatch, with_retry_result=False
        )
        assert applied
        assert leftover == []
