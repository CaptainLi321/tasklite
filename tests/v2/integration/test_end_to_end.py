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
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from tasklite.v2 import (
    ERR_MAX_RETRIES,
    CapacityResource,
    ExitReason,
    Job,
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
