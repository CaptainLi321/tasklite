"""v2 端到端集成测试：真实子进程跑通全链路（无任何 fake）。

覆盖六步契约的运行段全链路：spawn 子进程执行 → IPC 结果落盘 → wall
终态（业务 meta + run_count/inputs 审计键）→ attempts 轨迹（append-only
追溯链：activation/attempt 序号、incarnation、outcome 流转）→ 失败
档案 → retry_failure 人工补跑 → run/stop 门面收尾。handler 全部模块级
（spawn 子进程经 pickle 接收）。

激活代双通道对照：拦截点豁免放行（every_run）推进 activation_no+1
并重置 attempt_no=1；失败档案人工补跑（retry_failure）同为 failed
拦截点放行——按 attempts 轨迹最大激活代 +1 重建、attempt_no=1。两
通道均不得与既有激活在 (activation_no, attempt_no) 逻辑键上撞号。

故障场景补充：环境崩溃（正退出码、无结果文件）耗尽预算、强制中止的
半成品清理与 at-least-once 回队、看门狗超时击杀、限速资源挂起恢复、
中断族信号（KeyboardInterrupt 瞬态 / SystemExit 归因）——环境崩溃
场景以受控进程桩驱动（确定性复现「进程死亡且无结果」时序），其余走
真实子进程。
"""
from __future__ import annotations

import multiprocessing as mp
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pytest

from tasklite import (
    ERR_MAX_RETRIES,
    CapacityResource,
    ExitReason,
    Job,
    RateLimitResource,
    TaskLite,
)

PROJECT_ROOT = str(Path(__file__).resolve().parents[3])


@pytest.fixture(autouse=True)
def _subprocess_pythonpath(monkeypatch):
    """子进程按限定名 import 本模块（handler 反序列化）所需的路径环境。"""
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    monkeypatch.setenv("PYTHONPATH", PROJECT_ROOT)


def _make_pipeline(tmp_path, **overrides) -> TaskLite:
    """真实子进程管线（sqlite 后端 + 产物根）。"""
    defaults: dict = dict(
        name="e2e_pipeline",
        state_dir=tmp_path / "state",
        backend="sqlite",
        output_root=tmp_path / "output",
    )
    defaults.update(overrides)
    return TaskLite(**defaults)


# ── 模块级 handler（spawn 子进程经 pickle 接收）───────────────────────


class TransientSampleError(Exception):
    """业务瞬态异常样本（模块级可 pickle，经注册表快照下发子进程）。"""


def ok_with_input_handler(job, ctx):
    """成功 + 声明输入文件 + 业务元数据（供 wall meta 审计键断言）。"""
    source = ctx.output_root / f"source_{job.job_id}.txt"
    source.write_text("seed data", encoding="utf-8")
    ctx.declare_input(source)
    return True, {"touched": job.job_id}


def explode_handler(job, ctx):
    """确定性业务异常 → 失败档案。"""
    raise RuntimeError("end-to-end explosion")


def flaky_first_attempt_handler(job, ctx):
    """首次执行抛瞬态异常，重试执行成功（磁盘标记跨子进程传递状态）。"""
    marker = ctx.output_root / f"flaky_marker_{job.job_id}"
    if marker.exists():
        return True, {"recovered": True}
    marker.write_text(job.job_id, encoding="utf-8")
    raise TransientSampleError("transient on first attempt")


def always_transient_handler(job, ctx):
    """恒瞬态失败 → 重试预算耗尽进失败档案。"""
    raise TransientSampleError("always transient")


def fail_once_then_ok_handler(job, ctx):
    """首激活失败并写标记；档案补跑激活读到标记即成功。"""
    marker = ctx.output_root / f"fail_once_marker_{job.job_id}"
    if marker.exists():
        return True, {"recovered": True}
    marker.write_text(job.job_id, encoding="utf-8")
    raise RuntimeError("fail once then await manual rerun")


def hung_handler(job, ctx):
    """睡眠远超 timeout，等看门狗击杀（真实子进程超时场景）。"""
    time.sleep(8.0)
    return True, {}


def suspend_and_complete_handler(job, ctx):
    """成功并挂起 api 资源 2 秒（挂起窗口延迟同资源的后续作业）。"""
    ctx.suspend_resource("api", 2.0)
    return True, {"suspended_by": job.job_id}


def plain_gated_handler(job, ctx):
    """无副作用成功（消费 api 资源的挂起恢复窗口）。"""
    return True, {"ran_after_recovery": True}


def interrupted_first_handler(job, ctx):
    """首次执行抛 KeyboardInterrupt；重跑（零预算瞬态）读到标记即成功。"""
    marker = ctx.output_root / f"interrupted_marker_{job.job_id}"
    if marker.exists():
        return True, {"recovered": True}
    marker.write_text(job.job_id, encoding="utf-8")
    raise KeyboardInterrupt()


def system_exit_handler(job, ctx):
    """抛 SystemExit：worker 捕获后以中断族串归因落 error 通道。"""
    raise SystemExit(1)


def partial_output_hang_handler(job, ctx):
    """声明半成品输出后挂起长睡（强中止场景：等 stop(force) 击杀）。

    ready 标记先于挂起落盘——测试侧轮询标记出现再请求强停，避免在
    handler 尚未声明输出前击杀（竞态会让清理断言假绿）。
    """
    partial = ctx.declare_output(ctx.output_root / f"partial_{job.job_id}.txt")
    Path(partial).write_text("half-written", encoding="utf-8")
    ready = ctx.output_root / f"ready_{job.job_id}"
    ready.write_text(job.job_id, encoding="utf-8")
    time.sleep(60.0)
    return True, {}


class TestEndToEnd:
    """真实子进程全链路。"""

    def test_success_lands_in_wall_with_attempt_trace(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", ok_with_input_handler)
        p.enqueue(Job("work", "item_alpha"))
        summary = p.run()
        p.stop()

        assert summary.exit_reason is ExitReason.COMPLETED
        assert summary.stats["completed"] == 1
        meta = p.backend.load_wall()["work::item_alpha"]
        # wall 审计键：业务 meta 原样落盘 + 框架审计键（run_count /
        # inputs 指纹清单 / 归属 run）
        assert meta["touched"] == "item_alpha"
        assert meta["run_count"] == 1
        assert meta["last_run_id"] == summary.run_id
        assert len(meta["inputs"]) == 1
        entry = meta["inputs"][0]
        assert entry["path"].endswith("source_item_alpha.txt")
        assert entry["size"] == len("seed data")
        assert "mtime_ns" in entry

        attempts = p.backend.load_attempts("work::item_alpha")
        assert len(attempts) == 1
        rec = attempts[0]
        assert rec.outcome == "succeeded"
        assert rec.attempt_no == 1
        assert rec.activation_no == 1
        assert rec.incarnation.startswith(rec.run_id)
        assert rec.finished_at is not None
        assert rec.error is None

    def test_failure_lands_in_archive_with_attempt_trace(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", explode_handler)
        p.enqueue(Job("work", "item_beta", max_retries=0))
        p.run()
        p.stop()

        failed = p.backend.load_failed()
        assert "work::item_beta" in failed
        assert "end-to-end explosion" in failed["work::item_beta"]["error"]
        # wall/failed 互斥：失败终态不得残留 wall 行
        assert "work::item_beta" not in p.backend.load_wall()

        attempts = p.backend.load_attempts("work::item_beta")
        assert len(attempts) == 1
        assert attempts[0].outcome == "failed"
        assert attempts[0].error is not None

    def test_transient_retry_exhaustion_trace(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", always_transient_handler)
        p.register_transient_exception(TransientSampleError)
        p.enqueue(Job("work", "item_gamma", max_retries=2))
        p.run()
        p.stop()

        meta = p.backend.load_failed()["work::item_gamma"]
        assert meta["error"] == ERR_MAX_RETRIES

        attempts = p.backend.load_attempts("work::item_gamma")
        assert [a.attempt_no for a in attempts] == [1, 2, 3]
        assert [a.outcome for a in attempts] == ["requeued", "requeued", "failed"]
        assert p.stats["retried"] == 2

    def test_transient_recovery_second_attempt_succeeds(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", flaky_first_attempt_handler)
        p.register_transient_exception(TransientSampleError)
        p.enqueue(Job("work", "item_delta", max_retries=3))
        summary = p.run()
        p.stop()

        assert "work::item_delta" in p.backend.load_wall()
        attempts = p.backend.load_attempts("work::item_delta")
        # 同一激活内重试推进：attempt_no 1→2，activation_no 不变
        assert [a.outcome for a in attempts] == ["requeued", "succeeded"]
        assert [a.attempt_no for a in attempts] == [1, 2]
        assert [a.activation_no for a in attempts] == [1, 1]
        assert summary.stats["completed"] == 1

    def test_retry_failure_manual_requeue_runs_to_success(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", fail_once_then_ok_handler)
        p.enqueue(
            Job("work", "item_eps", payload={"k": "v"}, max_retries=0)
        )
        p.run()

        # 档案结构化查询：uid/错误/payload 快照自洽（补跑无需外部反查）
        entries = p.list_failures()
        assert [e.uid for e in entries] == ["work::item_eps"]
        assert entries[0].job_payload == {"k": "v"}

        assert p.retry_failure("work::item_eps") is True
        assert p.list_failures() == []

        summary = p.run()
        p.stop()
        assert summary.stats["completed"] == 1
        meta = p.backend.load_wall()["work::item_eps"]
        assert meta["recovered"] is True
        assert meta["run_count"] == 1

        # 档案补跑通道：failed 拦截点放行语义——激活代按轨迹最大值 +1
        # 推进、attempt_no 归 1，与 every_run 豁免放行同构
        attempts = p.backend.load_attempts("work::item_eps")
        assert [a.outcome for a in attempts] == ["failed", "succeeded"]
        assert [a.attempt_no for a in attempts] == [1, 1]
        assert [a.activation_no for a in attempts] == [1, 2]
        # 撞号防回归：补跑放行不得复用首激活的 (activation_no, attempt_no)
        logical_keys = [(a.activation_no, a.attempt_no) for a in attempts]
        assert len(logical_keys) == len(set(logical_keys))

    def test_every_run_rerun_advances_activation_and_run_count(
        self, tmp_path
    ):
        p = _make_pipeline(tmp_path)
        p.register_task("work", ok_with_input_handler)
        p.enqueue(Job("work", "item_eta", rerun="every_run"))
        first = p.run()
        assert first.stats["completed"] == 1
        assert p.backend.load_wall()["work::item_eta"]["run_count"] == 1

        # 第二会话重扫：wall 命中但 every_run 豁免放行 → 新激活重跑，
        # 成功 REPLACE wall 行且 run_count 从对侧终态续数
        p.enqueue(Job("work", "item_eta", rerun="every_run"))
        second = p.run()
        p.stop()

        assert second.stats["completed"] == 1
        assert p.backend.load_wall()["work::item_eta"]["run_count"] == 2

        attempts = p.backend.load_attempts("work::item_eta")
        assert [a.outcome for a in attempts] == ["succeeded", "succeeded"]
        assert [a.attempt_no for a in attempts] == [1, 1]
        assert [a.activation_no for a in attempts] == [1, 2]

    def test_six_step_contract_with_resource_and_hooks(self, tmp_path):
        events: list = []

        def on_attempt_finished(uid, *, outcome):
            events.append((uid, outcome.success, outcome.going_to_retry))

        p = _make_pipeline(
            tmp_path,
            on_run_start=lambda: events.append("start"),
            on_run_end=lambda reason: events.append(f"end:{reason}"),
            on_attempt_finished=on_attempt_finished,
        )
        p.register_resource(CapacityResource("io_slots", 4.0))
        p.register_task("work", ok_with_input_handler)
        p.enqueue(
            [
                Job("work", "step_one"),
                Job("work", "step_two"),
                Job("work", "step_one"),  # 重复 uid 静默去重
            ]
        )
        summary = p.run()
        p.stop()

        assert events[0] == "start"
        assert events[-1] == "end:completed"
        assert sorted(events[1:-1]) == [
            ("work::step_one", True, False),
            ("work::step_two", True, False),
        ]
        assert summary.stats["completed"] == 2
        assert sorted(p.backend.load_wall()) == [
            "work::step_one",
            "work::step_two",
        ]
        assert p.uncompleted([Job("work", "step_one"), Job("work", "next")]) == [
            Job("work", "next")
        ]


def make_exit_crash_process_class():
    """环境崩溃进程桩：以 exitcode=1 退出且从不写结果文件。

    确定性复现「解释器/导入段崩溃、无结果落盘」时序（真实子进程下
    该时序不可控）；与真实子进程共享同一收割代码路径。
    """

    class ExitCrashProcess:
        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self._alive = False
            self.exitcode = 1

        def start(self):
            self._alive = False

        def join(self, timeout=None):
            self._alive = False

        def is_alive(self):
            return self._alive

        def kill(self):
            self._alive = False

        def close(self):
            pass

    return ExitCrashProcess


class TestEnvFaultExhaustion:
    """环境故障端到端：正退出码崩溃走重试预算，耗尽后档案携带终态原因。"""

    def test_nonzero_exitcode_exhausts_budget_then_archive(self, tmp_path, monkeypatch):
        """exitcode=1 崩溃 ×（max_retries=1）→ 失败档案 ERR_MAX_RETRIES
        + last_retry_error/retry_error 携带 PROCESS_CRASH_EXIT 归因。

        环境性死亡（结果文件缺失/半写通常是解释器启动失败、导入段崩溃
        等环境问题）与信号死亡同属瞬态烧预算路径——不得零重试直判失败
        档案。
        """
        p = _make_pipeline(tmp_path)
        p.register_task("crash", ok_with_input_handler)
        p.enqueue(Job("crash", "item_env_crash", max_retries=1))

        monkeypatch.setattr(mp, "Process", make_exit_crash_process_class())
        p.run()
        p.stop()

        failed = p.backend.load_failed()
        assert "crash::item_env_crash" in failed
        entry = failed["crash::item_env_crash"]
        assert entry["error"] == ERR_MAX_RETRIES
        assert "PROCESS_CRASH_EXIT" in entry.get("last_retry_error", "")
        assert "PROCESS_CRASH_EXIT" in entry.get("retry_error", "")

        attempts = p.backend.load_attempts("crash::item_env_crash")
        assert [a.attempt_no for a in attempts] == [1, 2]
        assert [a.outcome for a in attempts] == ["requeued", "failed"]
        assert "crash::item_env_crash" not in p.backend.load_wall()


class TestForceAbortCleanup:
    """强中止（stop(force=True)）：半成品清理与 at-least-once 回队。"""

    def test_force_abort_cleans_partial_outputs_and_keeps_in_queue(self, tmp_path):
        """ABORTING：被 kill 的 in-flight job 半成品输出必须清理、job 回队。

        语义不变式：被强杀作业的声明产物（物理路径）不得残留脏数据；
        未 commit 的作业必须保留在磁盘队列可重跑（at-least-once）。
        """
        p = _make_pipeline(tmp_path, max_workers=1)
        p.register_task("abortable", partial_output_hang_handler)
        p.enqueue(Job("abortable", "item_abort"))

        output_root = tmp_path / "output"
        partial = output_root / "partial_item_abort.txt"
        ready = output_root / "ready_item_abort"

        def request_force_abort_when_declared() -> None:
            # 轮询 handler 已落盘半成品声明（上限放宽至 15s 容忍满载
            # spawn 开销）再强停——避免击杀先于声明的竞态。
            deadline = time.time() + 15.0
            while not ready.exists() and time.time() < deadline:
                time.sleep(0.05)
            p.stop(force=True)

        requester = threading.Thread(target=request_force_abort_when_declared)
        requester.start()
        summary = p.run()
        requester.join(timeout=5.0)
        p.stop()

        assert summary.exit_reason is ExitReason.STOPPED_ABORTING
        # ABORTING：半成品输出被清理
        assert not partial.exists(), "强杀作业的半成品输出必须被清理"
        # 作业未 commit（未进 wall），留在磁盘队列可重跑
        assert "abortable::item_abort" not in p.backend.load_wall()
        remaining = p.backend.load_queue()
        assert any(
            j.get("job_id") == "item_abort" for j in remaining
        ), "强中止后未 commit 的作业必须保留在队列（at-least-once）"


class TestRealSubprocessFaultScenarios:
    """真实子进程故障场景：看门狗超时击杀、限速挂起恢复、中断族信号。"""

    def test_real_subprocess_timeout_killed_by_watchdog(self, tmp_path):
        """短 timeout + 挂起 handler → 看门狗击杀 → 超时归因进失败档案。"""
        p = _make_pipeline(tmp_path)
        p.register_task("hung", hung_handler)
        p.enqueue(Job("hung", "item_watchdog", timeout=0.2, max_retries=0))
        p.run()
        p.stop()

        failed = p.backend.load_failed()
        assert "hung::item_watchdog" in failed
        error = failed["hung::item_watchdog"]["error"]
        assert error.startswith("TIMEOUT"), f"应归因超时，got: {error}"
        assert "hung::item_watchdog" not in p.backend.load_wall()

        attempts = p.backend.load_attempts("hung::item_watchdog")
        assert [a.outcome for a in attempts] == ["failed"]
        assert attempts[0].error is not None and "TIMEOUT" in attempts[0].error

    def test_real_subprocess_suspend_resource_recovers(self, tmp_path):
        """RateLimitResource 挂起：首作业挂起资源，后续作业等恢复后完成。"""
        p = _make_pipeline(tmp_path)
        api = RateLimitResource("api", interval_seconds=1.0)
        p.register_resource(api)
        p.register_task(
            "gated", suspend_and_complete_handler, default_resources={"api": 1.0}
        )
        p.register_task(
            "gated_plain", plain_gated_handler, default_resources={"api": 1.0}
        )
        p.enqueue([Job("gated", "holder"), Job("gated_plain", "waiter")])
        summary = p.run()
        p.stop()

        wall = p.backend.load_wall()
        assert summary.stats["completed"] == 2
        assert "gated::holder" in wall
        assert "gated_plain::waiter" in wall

        # 挂起语义下界：waiter 的派发必须晚于 holder 终态 + 挂起窗口的
        # 宽松下界（挂起 2s，下界取 1s——满载只会更长，不会更短）
        holder = p.backend.load_attempts("gated::holder")[0]
        waiter = p.backend.load_attempts("gated_plain::waiter")[0]
        gap = (
            datetime.fromisoformat(waiter.started_at)
            - datetime.fromisoformat(holder.finished_at)
        ).total_seconds()
        assert gap >= 1.0, (
            f"waiter 必须等待资源挂起恢复后才派发，实测间隔 {gap:.2f}s"
        )

    def test_real_subprocess_keyboard_interrupt_is_transient(self, tmp_path):
        """KeyboardInterrupt → interrupted 瞬态：零预算重入队，重跑成功。

        瞬态军规端到端：attempt_no 不推进、不烧重试预算、不进失败档案。
        """
        p = _make_pipeline(tmp_path)
        p.register_task("interrupted", interrupted_first_handler)
        p.enqueue(Job("interrupted", "item_interrupted", max_retries=1))
        summary = p.run()
        p.stop()

        assert "interrupted::item_interrupted" in p.backend.load_wall()
        assert "interrupted::item_interrupted" not in p.backend.load_failed()
        attempts = p.backend.load_attempts("interrupted::item_interrupted")
        assert [a.outcome for a in attempts] == ["requeued", "succeeded"]
        assert [a.attempt_no for a in attempts] == [1, 1], (
            "interrupted 瞬态零预算：attempt_no 不得推进"
        )
        assert summary.stats["interrupted_reruns"] == 1

    def test_real_subprocess_system_exit_attributed(self, tmp_path):
        """SystemExit 在真实子进程被 worker 捕获，以中断族串归因进失败档案。"""
        p = _make_pipeline(tmp_path)
        p.register_task("sysexit", system_exit_handler)
        p.enqueue(Job("sysexit", "item_sysexit", max_retries=0))
        p.run()
        p.stop()

        failed = p.backend.load_failed()
        assert "sysexit::item_sysexit" in failed
        error = failed["sysexit::item_sysexit"]["error"]
        assert "WORKER_INTERRUPTED" in error, f"应含中断族归因串，got: {error}"
        assert "sysexit::item_sysexit" not in p.backend.load_wall()
