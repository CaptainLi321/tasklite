"""执行身份 fencing 回归测试。

孤儿进程 fencing（执行身份隔离）：主进程 OOM/SIGKILL 崩溃后，
spawn 的子进程不随父死成为孤儿继续运行。重启后新 run 若以同 uid 派发
新子进程，孤儿写出的结果文件若仍用旧命名（{uid}.result.json），会被
drain 阶段 1 误读——commit 孤儿上下文 + kill 正在运行的新子进程。

修复：结果文件名携带 incarnation（{uid}.{run_id}.{seq}.result.json），
drain 只查当前 incarnation 路径——孤儿（旧 run_id）写的文件不可见。
"""

import threading
import time

from tasklite.pipeline import TaskLite
from tasklite.models.job import Job
from tasklite.models.state import PipelineState

from tests.helpers import (
    make_fake_process_class, make_pipeline, patch_multiprocessing_for_fakes,
    _write_fake_result, _ctx_incarnation,
)
from tasklite.engine.executor import (
    write_result_atomic, result_path, _iter_stale_result_paths,
    append_output, append_input, outputs_path, inputs_path,
)


class TestIncarnationFencing:
    def test_drain_ignores_orphan_old_incarnation_result(self, tmp_path, monkeypatch):
        """孤儿（旧 incarnation）写的结果文件必须被 drain 忽略。

        模拟崩溃恢复时序：
        1. 上次 run 崩溃前，孤儿子进程以旧 run_id 开始执行 uid X（未完成）；
        2. 本次 run 重新派发 X（新 incarnation），孤儿**随后**写完结果；
        3. drain 只查新 incarnation 路径 → 孤儿文件不可见 → 不误杀新子进程、
           不 commit 孤儿上下文。

        本测试以「submit 后手工写入旧 incarnation 结果文件」模拟孤儿写入：
        新 handle 的 drain 读到的是新子进程自己写的结果，孤儿文件被忽略。
        """
        p = make_pipeline(tmp_path)
        handler_calls = []
        p.register_handler("h", lambda job, ctx: handler_calls.append(job.uid) or (True, {}))
        p.enqueue([Job("h", "a")])

        # 预先写入一条「旧 run_id」的残留结果（模拟上次 run 孤儿将写的文件，
        # 文件名格式 {uid}.{old_run_id}.{seq}.result.json——与当前 run 的
        # run_id 不同）。
        old_incarnation = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        write_result_atomic(p.ipc_dir, "h::a", {
            "status": "success",
            "raw_result": True,
            "new_jobs": [],
            "resource_suspensions": [],
            "cursor_updates": {},
        }, incarnation=old_incarnation)

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        p.run()

 # consume_stale_result 会消费旧 incarnation 残留（崩溃恢复语义：省一次重跑）
        assert "h::a" in p.backend.load_wall()
        assert handler_calls == [], "旧 incarnation 残留应在派发前被 consume_stale_result 消费"

    def test_orphan_file_ignored_when_stale_consumption_skipped(self, tmp_path, monkeypatch):
        """关键回归：孤儿文件在**派发之后**才写入 → 必须对 drain 不可见。

        模拟最危险的时序：consume_stale_result 检查时孤儿还没写完（无残留可消费）
        → 派发新子进程 → 孤儿此时才写完旧 incarnation 文件。若 drain 阶段 1
        读到该文件，会 commit 孤儿上下文并 kill 新子进程（造成错误提交与误杀危害）。
        """
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: (True, {}))

 # 手工触发一次派发（不消费残留）——通过让 consume_stale_result 找不到文件：
        # 先跑完一个正常 run，确认其结果文件使用**当前 run 的 incarnation**。
        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        p.enqueue([Job("h", "a")])
        p.run()

        # 现在 ipc_dir 中应无残留（正常路径消费后清理）
        assert _iter_stale_result_paths(p.ipc_dir, "h::a") == []

        # 模拟孤儿在 run 结束前悄悄写入旧 incarnation 文件
        old_incarnation = "deadbeefdeadbeefdeadbeefdeadbeef.99"
        write_result_atomic(p.ipc_dir, "h::a", {
            "status": "success",
            "raw_result": True,
            "new_jobs": [],
            "resource_suspensions": [],
            "cursor_updates": {},
        }, incarnation=old_incarnation)

        # 再次 run（uid 已在 wall，加载期过滤，不派发；孤儿文件不参与判定）
        p.run()
        assert "h::a" in p.backend.load_wall()

    def test_incarnation_in_file_name_and_handle(self, tmp_path, monkeypatch):
        """submit 写入的 JobHandle/结果文件名必须携带当前 run 的 incarnation。"""
        p = make_pipeline(tmp_path)
        captured = {}

        class CaptureProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                ctx = self.args[2]
                captured["incarnation"] = ctx.incarnation
                from tasklite.engine.executor import write_result_atomic
                write_result_atomic(self.args[3], self.args[1].uid, {
                    "status": "success", "raw_result": True,
                    "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
                }, incarnation=ctx.incarnation)

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=CaptureProcess)
        p.register_handler("h", lambda job, ctx: (True, {}))
        p.enqueue([Job("h", "a")])
        p.run()

        inc = captured.get("incarnation")
        assert inc is not None and "." in inc, f"ctx 必须携带 incarnation: {inc!r}"
        run_id, seq = inc.split(".")
        assert len(run_id) == 32, f"run_id 应为 32-hex uuid: {run_id!r}"
        assert int(seq) >= 1, f"seq 应从 1 递增: {seq!r}"
        # 结果文件应以该 incarnation 命名，且正常消费后应被清理（不留残留）
        assert result_path(p.ipc_dir, "h::a", inc).exists() is False, \
            "run 完成后当前 incarnation 的结果文件应被消费清理"
        assert _iter_stale_result_paths(p.ipc_dir, "h::a") == [], \
            "run 完成后 ipc 目录不应有残留结果文件"

    def test_meta_table_persists_run_id(self, tmp_path, monkeypatch):
        """run_id 应持久化到后端 meta 表（fencing 的运维可观测性）。"""
        p = make_pipeline(tmp_path)
        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)
        p.register_handler("h", lambda job, ctx: (True, {}))
        p.enqueue([Job("h", "a")])
        p.run()
        saved = p.backend.get_meta("last_run_id")
        assert saved is not None and len(saved) == 32, f"meta 表应记录 run_id: {saved!r}"

    def test_seq_increments_across_retries(self, tmp_path, monkeypatch):
        """同 uid 重试再派发必须获得新 incarnation（旧尝试结果隔离）。"""
        p = make_pipeline(tmp_path)
        seqs = []

        class SeqCaptureProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self._alive = True
                ctx = self.args[2]
                seqs.append(ctx.incarnation)
                from tasklite.engine.executor import write_result_atomic
                write_result_atomic(self.args[3], self.args[1].uid, {
                    "status": "retry", "error": "transient",
                }, incarnation=ctx.incarnation)

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=SeqCaptureProcess)
        p.register_handler("h", lambda job, ctx: (True, {}))
        p.enqueue([Job("h", "a", max_retries=1, backoff_base=0.01, backoff_max=0.01)])
        p.run()

        assert len(seqs) == 2, f"应派发两次（初始+重试）: {seqs}"
        assert seqs[0] != seqs[1], "重试派发必须获得新 incarnation"


class TestStalePathPrefixCollision:
    """_iter_stale_result_paths 不得把点后缀兄弟 uid 的
    结果文件误认成自己的残留——job_id 可含点，glob {uid}.* 会匹配
    {uid}.X 这类更长 uid 的文件，.search 未锚定导致误消费/误删除。"""

    def test_sibling_uid_dot_suffix_not_matched(self, tmp_path):
        """`h::page` 的残留枚举不得匹配 `h::page.1` 的结果文件。"""
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        write_result_atomic(str(tmp_path), "h::page.1", {
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
        }, incarnation=inc)
        # 兄弟 uid 的结果文件存在，但 h::page 的枚举必须为空
        assert _iter_stale_result_paths(str(tmp_path), "h::page") == [], \
            "点后缀兄弟 uid 的结果文件不得被误判为本 uid 的残留"

    def test_sibling_uid_deep_dot_suffix_not_matched(self, tmp_path):
        """多级点后缀（`h::a.b.c` vs `h::a`）同样不得误匹配。"""
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.7"
        write_result_atomic(str(tmp_path), "h::a.b.c", {
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
        }, incarnation=inc)
        assert _iter_stale_result_paths(str(tmp_path), "h::a") == []

    def test_same_uid_incarnation_still_matched(self, tmp_path):
        """本 uid 自己的 incarnation 残留仍须被枚举（修复不能误伤正常路径）。"""
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.3"
        write_result_atomic(str(tmp_path), "h::a", {
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
        }, incarnation=inc)
        found = _iter_stale_result_paths(str(tmp_path), "h::a")
        assert len(found) == 1, f"本 uid 残留应被枚举: {found}"

    def test_cleanup_does_not_delete_sibling_result(self, tmp_path):
        """cleanup_ipc_files 不得删除点后缀兄弟 uid 的结果文件（全链路）。"""
        from tasklite.engine.executor import cleanup_ipc_files
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.5"
        write_result_atomic(str(tmp_path), "h::page.1", {
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
        }, incarnation=inc)
        cleanup_ipc_files(str(tmp_path), "h::page", inc)
        # 兄弟 uid 的结果文件必须保留
        from tasklite.engine.executor import result_path
        assert result_path(str(tmp_path), "h::page.1", inc).exists(), \
            "cleanup 不得删除兄弟 uid 的结果文件"


class TestDrainStaleTmpIgnore:
    """/consume_stale_result 必须忽略 .tmp 残留（孤儿 worker 正在写）。

    变异体（删除 .tmp 过滤行）会消费 .tmp——读部分 JSON 返回 None 后 unlink
    正在写的文件 → 孤儿 os.replace 抛 FileNotFoundError → 写 error 结果 →
    下次派发消费 error → 成功执行的 job 虚假 DLQ。本测试锁定「忽略 .tmp」。"""

    def test_drain_stale_ignores_tmp_leftover(self, tmp_path):
        from pathlib import Path as _Path
        from tasklite.engine.executor import _RESULT_TMP_SUFFIX
        from tasklite.utils.lockfile import safe_uid_filename

        p = make_pipeline(tmp_path)
        job = Job("h", "a")
        # 模拟孤儿 worker 正在写结果：.tmp 残留（部分 JSON，dump/fsync 中途）
        base = safe_uid_filename(job.uid)
        tmp_file = _Path(p.ipc_dir) / f"{base}{_RESULT_TMP_SUFFIX}"
        tmp_file.write_text('{"status": "suc')  # 部分 JSON

        result = p.executor.consume_stale_result(job.uid, job)
        assert result is None, "consume_stale_result 必须忽略 .tmp（孤儿仍在写，无 final 可消费）"
        assert tmp_file.exists(), "consume_stale_result 不得 unlink 正在写的 .tmp 文件"


class TestStaleDeclarationCleanup:
    """ 修复（1）：崩溃残留声明文件污染 → 成功 job 假 DLQ。

    时序：run1 worker 声明输出 O1 落盘 outputs.jsonl → drain 消费结果后
    cleanup_ipc_files 删 result/signals 但不删 outputs.jsonl（生命周期归
    _complete_job 管）→ 主进程在 _complete_job finally 前 SIGKILL → 残留
    outputs.jsonl。run2 同 uid 重派发 → _restore_stale_result 无残留结果
    可消费 → 新 worker **append** 声明 → 文件变 [O1,O2] → 成功路径存在性
    校验（_decode_ipc_result）读到 O1（临时产物/已清理 → 不存在）→
    "Missing output" 假 DLQ。

    修复：_restore_stale_result 返回 False 后、submit 前，删除该 uid 的
    残留 outputs.jsonl/inputs.jsonl（幂等 no-op；正常路径 _complete_job
    finally 已删，此处只清崩溃残留）。
    """

    def test_stale_outputs_declaration_not_polluting_success(self, tmp_path, monkeypatch):
        """预置「旧声明指向不存在的文件」→ 未清理则成功路径校验必假 DLQ。

        submit 时（修复点之后）旧声明必须已被清——若未清，_decode_ipc_result
        读 outputs.jsonl 校验旧输出存在性失败 → "Missing output" 假 DLQ。
        """
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: (True, {}))
        p.enqueue([Job("h", "a")])

        # 预置崩溃残留：上一次执行声明了 /tmp/nonexistent_old_output.txt
        #（临时产物/已清理 → 物理不存在），残留至本次重派发。
        append_output(p.ipc_dir, "h::a", "/tmp/nonexistent_old_output.txt", cleanup=True)
        stale_op = outputs_path(p.ipc_dir, "h::a")
        assert stale_op.exists(), "预置崩溃残留声明文件"

        cleaned_at_submit = []

        class StaleCheckProcess:
            def __init__(self, target=None, args=(), **_kw):
                # submit 时（派发后、子进程 start 前）旧声明必须已被清——
                # 修复缺失时此处 outputs.jsonl 仍含旧声明，drain 后校验必失败。
                self.args = args
                self._alive = False
                self.exitcode = 0
                cleaned_at_submit.append(not stale_op.exists())

            def start(self):
                self._alive = True
                _write_fake_result(self.args[3], self.args[1].uid, {
                    "status": "success",
                    "raw_result": True,
                    "new_jobs": [],
                    "resource_suspensions": [],
                    "cursor_updates": {},
                }, incarnation=_ctx_incarnation(self.args))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=StaleCheckProcess)
        p.run()

        # a) submit 前残留 outputs.jsonl 已被清理（旧声明未参与校验）
        assert cleaned_at_submit == [True], \
            f"submit 前必须已清理崩溃残留声明文件: {cleaned_at_submit}"
        # b) 成功 job 不假 DLQ——进 wall 而非 failed
        assert "h::a" in p.backend.load_wall(), "成功 job 不得因旧声明污染假 DLQ"
        # c) run 结束后声明文件被消费删除（正常路径 _complete_job finally）
        assert not stale_op.exists(), "run 结束后 outputs.jsonl 不应残留"

    def test_stale_inputs_declaration_cleaned_before_resubmit(self, tmp_path, monkeypatch):
        """inputs.jsonl 与 outputs.jsonl 同生命周期（对称）——崩溃残留的
        输入声明同样在 submit 前清理（否则成功提交时 _apply_result 读
        read_inputs 把旧指纹并入 wall meta，on_input_change 比对读到陈旧指纹）。"""
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: (True, {}))
        p.enqueue([Job("h", "a")])

        append_input(p.ipc_dir, "h::a", {
            "path": "/tmp/stale_input.txt", "kind": "file",
            "size": 0, "mtime_ns": 0,
        })
        stale_ip = inputs_path(p.ipc_dir, "h::a")
        assert stale_ip.exists(), "预置崩溃残留输入声明文件"

        seen_at_submit = []

        class InputsCheckProcess:
            def __init__(self, target=None, args=(), **_kw):
                self.args = args
                self._alive = False
                self.exitcode = 0
                seen_at_submit.append(not stale_ip.exists())

            def start(self):
                self._alive = True
                _write_fake_result(self.args[3], self.args[1].uid, {
                    "status": "success",
                    "raw_result": True,
                    "new_jobs": [],
                    "resource_suspensions": [],
                    "cursor_updates": {},
                }, incarnation=_ctx_incarnation(self.args))

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=InputsCheckProcess)
        p.run()

        assert seen_at_submit == [True], \
            f"submit 前必须已清理崩溃残留 inputs.jsonl: {seen_at_submit}"
        assert "h::a" in p.backend.load_wall(), "成功 job 不得因旧输入声明污染假 DLQ"
        assert not stale_ip.exists(), "run 结束后 inputs.jsonl 不应残留"


class TestDispatchOrderFencing:
    """_dispatch_job 内 probe_lock 必须先于 restore 与清理：

    时序约束（probe → restore → 清理 → submit）：
    1) 孤儿存活 → probe 失败 defer（不清理孤儿实时声明）；
    2) 孤儿死后 → probe 通过 → restore 消费其残留结果 → 不派发（无双跑）；
    3) 无孤儿 → probe 通过 → restore 无果 → 清理崩溃残留声明 → submit。
    """

    def test_orphan_alive_probe_fail_keeps_live_declarations(self, tmp_path, monkeypatch):
        """孤儿存活（probe_lock 失败）→ defer，不得清理孤儿实时声明文件。

        变异体（清理块仍位于 probe 之前）：probe 失败前清理块已删掉
        孤儿实时声明的 outputs.jsonl/inputs.jsonl → 本断言失败。
        """
        from tasklite.utils.lockfile import try_acquire_lock, release_lock

        patch_multiprocessing_for_fakes(
            monkeypatch, fake_process_class=make_fake_process_class("success"))
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: (True, {}))
        p.enqueue([Job("h", "a")])

        # 模拟孤儿 worker：持有 {uid}.lock + 正在实时 append 的声明文件
        lock_fd = try_acquire_lock(p.ipc_dir, "h::a")
        assert lock_fd is not None, "测试前置：孤儿持锁"
        append_output(p.ipc_dir, "h::a", "/tmp/orphan_live_output.txt", True)
        append_input(p.ipc_dir, "h::a", {
            "path": "/tmp/orphan_live_input.txt", "kind": "file",
            "size": 0, "mtime_ns": 0,
        })
        op = outputs_path(p.ipc_dir, "h::a")
        ip = inputs_path(p.ipc_dir, "h::a")
        assert op.exists() and ip.exists(), "前置：孤儿实时声明文件已落盘"

        observed = {}

        def run_loop():
            p.run()

        t = threading.Thread(target=run_loop)
        t.start()
        try:
            # 等首次派发命中孤儿探测（probe 失败 → deferred_orphan 计数 +1）
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if p.stats.get("deferred_orphan", 0) >= 1:
                    break
                time.sleep(0.01)
            observed["deferred"] = p.stats.get("deferred_orphan", 0)
            observed["outputs_survive"] = op.exists()
            observed["inputs_survive"] = ip.exists()
        finally:
            # 释放锁 → 孤儿「死后」probe 通过 → 正常派发完成
            release_lock(lock_fd)
            t.join(timeout=10)

        assert observed.get("deferred", 0) >= 1, \
            "孤儿持锁时派发必须 defer（probe 失败）"
        assert observed.get("outputs_survive"), \
            "孤儿存活（probe 失败）时不得清理其实时 outputs.jsonl"
        assert observed.get("inputs_survive"), \
            "孤儿存活（probe 失败）时不得清理其实时 inputs.jsonl"
        # 孤儿死后 probe 通过 → 正常完成（清理残留后重写声明）
        assert "h::a" in p.backend.load_wall(), "孤儿死后 job 应正常完成"

    def test_orphan_dead_probe_pass_restore_consumes_no_dispatch(self, tmp_path):
        """孤儿死后（无锁）probe 通过 → restore 消费其残留结果 → 不派发。

        修复后顺序 probe → restore →  清理 → submit：直接驱动一次
        _dispatch_job，restore 命中残留结果即返回 None（无新 worker），
        残留结果提交进 wall、成功产出的物理文件保留（无双跑、无重跑）。
        """
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: (True, {}))
        p.enqueue([Job("h", "a")])
        # 直接驱动派发需要内存状态（enqueue 只写磁盘）——从后端装载
        p._state = PipelineState({}, {}, {}, p.backend.load_queue())
        p._dispatch_seq = 0

        # 模拟孤儿已死（无锁）：其成功结果文件 + 声明 + 物理输出均已落盘
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        out_file = tmp_path / "out" / "result.txt"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("done")
        append_output(p.ipc_dir, "h::a", str(out_file), True)
        write_result_atomic(p.ipc_dir, "h::a", {
            "status": "success", "raw_result": True,
            "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
        }, incarnation=inc)

        state = p._state
        sched = p.scheduler.pop_next_runnable(state, state.in_flight_uids)
        assert sched.runnable_idx is not None, "job 应可运行"
        entry = p._dispatch_job(sched)

        assert entry is None, "restore 消费残留结果后不得派发新 worker（无双跑）"
        assert p._dispatch_seq == 0, "不得 submit（无新 incarnation 分配）"
        assert "h::a" in p.backend.load_wall(), "残留结果应被消费提交到 wall"
        assert out_file.exists(), \
            "成功产出的物理输出必须保留（restore 消费非失败清理）"
