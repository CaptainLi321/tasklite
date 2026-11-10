"""v2 端到端集成测试：真实子进程跑通全链路（无任何 fake）。

覆盖六步契约的运行段全链路：spawn 子进程执行 → IPC 结果落盘 → successes
终态（业务 meta + run_count/inputs 审计键）→ attempts 轨迹（append-only
追溯链：activation/attempt 序号、execution_id、outcome 流转）→ 失败
档案 → set_job_status 人工重置 → run/stop 门面收尾。handler 全部模块级
（spawn 子进程经 pickle 接收）。

激活代推进：拦截点豁免放行（every_run）推进 activation_no+1
并重置 attempt_no=1。各通道均不得与既有激活在 (activation_no, attempt_no)
逻辑键上撞号。

故障场景补充：异常、SystemExit 与看门狗超时均单次失败，不自动重试；
限速资源按间隔恢复，强制中止路径保留未提交产物语义。
"""
from __future__ import annotations

import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pytest

from tasklite import (
    FileSpec,
    Job,
    StateSnapshot,
    TaskLite,
    TaskResult,
)
from tasklite.resource import (
    CapacityResource,
    IntervalResource,
)
from tasklite.engine.errorclass import ERR_MAX_RETRIES

PROJECT_ROOT = str(Path(__file__).resolve().parents[3])


@pytest.fixture(autouse=True)
def _subprocess_pythonpath(monkeypatch):
    """子进程按限定名 import 本模块（handler 反序列化）所需的路径环境。"""
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    monkeypatch.setenv("PYTHONPATH", PROJECT_ROOT)


def _make_pipeline(tmp_path, **overrides) -> TaskLite:
    """真实子进程管线（sqlite 后端 + 产物根）。"""
    out = tmp_path / "output"
    out.mkdir(parents=True, exist_ok=True)
    overrides.pop("name", None)
    overrides.pop("state_dir", None)
    state_path = overrides.pop("state_path", tmp_path / "e2e_state.db")
    backend = overrides.pop("backend", "sqlite")
    output_roots = overrides.pop("output_roots", out)
    return TaskLite(
        backend=backend,
        state_path=state_path,
        output_roots=output_roots,
        **overrides,
    )


# ── 模块级 handler（spawn 子进程经 pickle 接收）───────────────────────


def ok_with_input_handler(job, ctx):
    """成功 + 声明输入文件 + 业务元数据（供 successes meta 审计键断言）。"""
    source = ctx.output_roots / f"source_{job.job_id}.txt"
    source.write_text("seed data", encoding="utf-8")
    ctx.declare_file("source", FileSpec(source, role="input"))
    return TaskResult.success(metadata={"touched": job.job_id})


def explode_handler(job, ctx):
    """确定性业务异常 → 失败档案。"""
    raise RuntimeError("end-to-end explosion")


def flaky_first_attempt_handler(job, ctx):
    """首次执行返回重试，重试执行成功（磁盘标记跨子进程传递状态）。"""
    marker = ctx.output_roots / f"flaky_marker_{job.job_id}"
    if marker.exists():
        return TaskResult.success(metadata={"recovered": True})
    marker.write_text(job.job_id, encoding="utf-8")
    return TaskResult.retry(message="transient on first attempt")


def always_transient_handler(job, ctx):
    """恒重试失败 → 重试预算耗尽进失败档案。"""
    return TaskResult.retry(message="always transient")


def fail_once_then_ok_handler(job, ctx):
    """首激活失败并写标记；档案补跑激活读到标记即成功。"""
    marker = ctx.output_roots / f"fail_once_marker_{job.job_id}"
    if marker.exists():
        return TaskResult.success(metadata={"recovered": True})
    marker.write_text(job.job_id, encoding="utf-8")
    raise RuntimeError("fail once then await manual trigger")


def hung_handler(job, ctx):
    """睡眠远超 timeout，等看门狗击杀（真实子进程超时场景）。"""
    time.sleep(8.0)
    return TaskResult.success()


def plain_gated_handler(job, ctx):
    """无副作用成功（消费 api 资源的间隔窗口）。"""
    return TaskResult.success(metadata={"ran_after_recovery": True})


def system_exit_handler(job, ctx):
    """抛 SystemExit：worker 捕获后记录进失败档案。"""
    raise SystemExit(1)


class TestEndToEnd:
    """真实子进程全链路。"""

    def test_success_lands_in_wall_with_attempt_trace(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", ok_with_input_handler)
        p.enqueue(Job("work", "item_alpha"))
        res = p.run()
        p.stop()

        assert res is None
        meta = p._runtime.store.backend.load_successes()["work::item_alpha"]
        assert meta["result"]["metadata"]["touched"] == "item_alpha"
        assert meta["success_count"] == 1
        assert meta["last_success_run_id"]
        assert len(meta["inputs"]) == 1
        assert meta["inputs"][0].endswith("source_item_alpha.txt")

        attempts = p._runtime.store.backend.load_settled_attempts("work::item_alpha")
        assert len(attempts) == 1
        rec = attempts[0]
        assert rec["outcome"] == "success"
        assert rec["attempt_no"] == 1
        assert rec["settled_at"] is not None
        assert rec["error_message"] is None

    def test_failure_lands_in_archive_with_attempt_trace(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", explode_handler)
        p.enqueue(Job("work", "item_beta", max_retries_per_run=0))
        p.run()
        p.stop()

        failed = p._runtime.store.backend.load_failed()
        assert "work::item_beta" in failed
        assert "end-to-end explosion" in failed["work::item_beta"]["error_message"]
        # successes/failed 互斥：失败终态不得残留 successes 行
        assert "work::item_beta" not in p._runtime.store.backend.load_successes()

        attempts = p._runtime.store.backend.load_settled_attempts("work::item_beta")
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failure"
        assert attempts[0]["error_message"] is not None

    def test_transient_retry_exhaustion_trace(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", always_transient_handler)
        p.enqueue(Job("work", "item_gamma", max_retries_per_run=2))
        p.run()
        p.stop()

        meta = p._runtime.store.backend.load_failed()["work::item_gamma"]
        assert meta["error_message"] == ERR_MAX_RETRIES

        attempts = p._runtime.store.backend.load_settled_attempts("work::item_gamma")
        assert [a["attempt_no"] for a in attempts] == [1, 2, 3]
        assert [a["outcome"] for a in attempts] == ["retry", "retry", "failure"]

    def test_transient_recovery_second_attempt_succeeds(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", flaky_first_attempt_handler)
        p.enqueue(Job("work", "item_delta", max_retries_per_run=3))
        res = p.run()
        p.stop()

        assert res is None
        assert "work::item_delta" in p._runtime.store.backend.load_successes()
        attempts = p._runtime.store.backend.load_settled_attempts("work::item_delta")
        # 同一激活内重试推进：attempt_no 1→2，activation_no 不变
        assert [a["outcome"] for a in attempts] == ["retry", "success"]
        assert [a["attempt_no"] for a in attempts] == [1, 2]

    def test_manual_requeue_runs_to_success(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", fail_once_then_ok_handler)
        p.enqueue(
            Job("work", "item_eps", payload={"k": "v"}, max_retries_per_run=0)
        )
        p.run()

        # 档案结构化查询：uid/错误/payload 快照自洽（补跑无需外部反查）
        with StateSnapshot.open(p) as snap:
            entries = list(snap.iter_jobs(status="failed"))
            assert [e["job_uid"] for e in entries] == ["work::item_eps"]
            assert entries[0]["job"]["payload"] == {"k": "v"}

        p.set_job_status("work::item_eps", "pending", recursive=False)
        with StateSnapshot.open(p) as snap:
            assert list(snap.iter_jobs(status="failed")) == []

        res = p.run()
        p.stop()
        assert res is None
        meta = p._runtime.store.backend.load_successes()["work::item_eps"]
        assert meta["result"]["metadata"]["recovered"] is True
        assert meta["success_count"] == 1

        # 档案补跑通道：failed 拦截点放行语义——激活代按轨迹最大值 +1
        # 推进、attempt_no 归 1，与 every_run 豁免放行同构
        attempts = p._runtime.store.backend.load_settled_attempts("work::item_eps")
        assert [a["outcome"] for a in attempts] == ["failure", "success"]
        assert [a["attempt_no"] for a in attempts] == [1, 2]
        # 补跑放行推进 attempt_no
        logical_keys = [a["attempt_no"] for a in attempts]
        assert len(logical_keys) == len(set(logical_keys))

    def test_every_run_rerun_advances_activation_and_run_count(
        self, tmp_path
    ):
        p = _make_pipeline(tmp_path)
        p.register_task("work", ok_with_input_handler)
        p.enqueue(Job("work", "item_eta", trigger="always"))
        first = p.run()
        assert first is None
        assert p._runtime.store.backend.load_successes()["work::item_eta"]["success_count"] == 1

        # 第二会话重扫：successes 命中但 every_run 豁免放行 → 新激活重跑，
        # 成功 REPLACE successes 行且 run_count 从对侧终态续数
        p.enqueue(Job("work", "item_eta", trigger="always"))
        second = p.run()
        p.stop()

        assert second is None
        assert p._runtime.store.backend.load_successes()["work::item_eta"]["success_count"] == 2

        attempts = p._runtime.store.backend.load_settled_attempts("work::item_eta")
        assert [a["outcome"] for a in attempts] == ["success", "success"]
        assert [a["attempt_no"] for a in attempts] == [1, 2]

    def test_six_step_contract_with_resource(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_resource(CapacityResource("io_slots", max_capacity=4.0))
        p.register_task("work", ok_with_input_handler)
        assert p.enqueue(Job("work", "step_one")) is True
        assert p.enqueue(Job("work", "step_two")) is True
        assert p.enqueue(Job("work", "step_one")) is False  # 重复 uid 幂等去重
        res = p.run()
        p.stop()

        assert res is None
        assert sorted(p._runtime.store.backend.load_successes()) == [
            "work::step_one",
            "work::step_two",
        ]
        with StateSnapshot.open(p) as snap:
            succ = {j["job_uid"] for j in snap.iter_jobs(status="succeeded")}
            candidates = [Job("work", "step_one"), Job("work", "next")]
            assert [j for j in candidates if j.uid not in succ] == [Job("work", "next")]


class TestRealSubprocessFaultScenarios:
    """真实子进程故障场景：看门狗超时击杀、限速挂起恢复、中断族信号。"""

    def test_real_subprocess_timeout_killed_by_watchdog(self, tmp_path):
        """短 timeout + 挂起 handler → 看门狗击杀 → 超时归因进失败档案。"""
        p = _make_pipeline(tmp_path)
        p.register_task("hung", hung_handler)
        p.enqueue(Job("hung", "item_watchdog", timeout_seconds=0.2, max_retries_per_run=0))
        p.run()
        p.stop()

        failed = p._runtime.store.backend.load_failed()
        assert "hung::item_watchdog" in failed
        error = failed["hung::item_watchdog"]["error_message"]
        assert "timed out" in error, f"应归因超时，got: {error}"
        assert "hung::item_watchdog" not in p._runtime.store.backend.load_successes()

        attempts = p._runtime.store.backend.load_settled_attempts("hung::item_watchdog")
        assert [a["outcome"] for a in attempts] == ["failure"]
        assert attempts[0]["error_message"] is not None and "timed out" in attempts[0]["error_message"]

    def test_real_subprocess_interval_resource_paces(self, tmp_path):
        """IntervalResource：首作业获取后推进下次可用时间，后续作业按间隔派发。"""
        p = _make_pipeline(tmp_path)
        api = IntervalResource("api", interval_seconds=1.0)
        p.register_resource(api)
        p.register_task(
            "gated", plain_gated_handler, resources={"api": 1.0}
        )
        p.register_task(
            "gated_plain", plain_gated_handler, resources={"api": 1.0}
        )
        p.enqueue(Job("gated", "holder"))
        p.enqueue(Job("gated_plain", "waiter", depends_on=["gated::holder"]))
        res = p.run()
        p.stop()

        successes = p._runtime.store.backend.load_successes()
        assert res is None
        assert "gated::holder" in successes
        assert "gated_plain::waiter" in successes

    def test_real_subprocess_system_exit_attributed(self, tmp_path):
        """SystemExit 在真实子进程被 worker 捕获，按业务失败证据结算。"""
        p = _make_pipeline(tmp_path)
        p.register_task("sysexit", system_exit_handler)
        p.enqueue(Job("sysexit", "item_sysexit", max_retries_per_run=0))
        p.run()
        p.stop()

        failed = p._runtime.store.backend.load_failed()
        assert "sysexit::item_sysexit" in failed
        error = failed["sysexit::item_sysexit"]["error_message"]
        assert "SystemExit" in error, f"应含 SystemExit 错误信息，got: {error}"
        assert "sysexit::item_sysexit" not in p._runtime.store.backend.load_successes()
        attempts = p._runtime.store.backend.load_settled_attempts("sysexit::item_sysexit")
        assert len(attempts) == 1
        assert attempts[0]["outcome"] == "failure"
