"""端到端核心生命周期测试：成功、失败、重试、手动重入队与管理操作。"""
from __future__ import annotations

import os
from pathlib import Path

from tasklite import (
    FileSpec,
    Job,
    StateSnapshot,
    TaskLite,
    TaskResult,
)
from tasklite.resource import CapacityResource
from tasklite.testing import in_process_workers


def _make_pipeline(tmp_path, **overrides) -> TaskLite:
    out = tmp_path / "output"
    out.mkdir(parents=True, exist_ok=True)
    backend = overrides.pop("backend", "memory")
    kwargs = {"state_path": tmp_path / "e2e_state.db"} if backend == "sqlite" else {}
    prev = os.getcwd()
    os.chdir(tmp_path)
    try:
        return TaskLite(backend=backend, **kwargs, **overrides)
    finally:
        os.chdir(prev)


def ok_with_input_handler(job, ctx):
    source = Path(job.payload["output_dir"]) / f"source_{job.job_id}.txt"
    source.write_text("seed data", encoding="utf-8")
    ctx.declare_file("source", FileSpec(source, role="input"))
    return TaskResult.success(metadata={"touched": job.job_id})


def explode_handler(job, ctx):
    raise RuntimeError("end-to-end explosion")


def flaky_first_attempt_handler(job, ctx):
    marker = Path(job.payload["output_dir"]) / f"flaky_marker_{job.job_id}"
    if marker.exists():
        return TaskResult.success(metadata={"recovered": True})
    marker.write_text(job.job_id, encoding="utf-8")
    return TaskResult.retry(message="transient on first attempt")


def attempt_aware_retry_handler(job, ctx):
    assert "attempt_no" not in job.to_dict()
    if job.attempt_no < 3:
        return TaskResult.retry(message=f"transient on attempt {job.attempt_no}")
    return TaskResult.success(metadata={"final_attempt": job.attempt_no})


def always_transient_handler(job, ctx):
    return TaskResult.retry(message="always transient")


def fail_once_then_ok_handler(job, ctx):
    marker = Path(job.payload["output_dir"]) / f"fail_once_marker_{job.job_id}"
    if marker.exists():
        return TaskResult.success(metadata={"recovered": True})
    marker.write_text(job.job_id, encoding="utf-8")
    raise RuntimeError("fail once then await manual trigger")


class TestEndToEnd:
    """核心管线端到端契约。"""

    def test_success_lands_in_wall_with_attempt_trace(self, tmp_path):
        p = _make_pipeline(tmp_path)
        out_dir = str(tmp_path / "output")
        p.register_task("work", ok_with_input_handler)
        p.enqueue(Job("work", "item_alpha", payload={"output_dir": out_dir}))
        with in_process_workers(p):
            res = p.run(install_signals=False)
        p.stop()

        assert res is None
        with StateSnapshot.open(p) as snap:
            rec = snap.get_job("work::item_alpha")
            assert rec is not None and rec["status"] == "succeeded"
            assert rec["result"]["metadata"]["touched"] == "item_alpha"
            assert rec["files"]["source"]["path"].endswith("source_item_alpha.txt")
            attempts = list(snap.iter_attempts(job_uid="work::item_alpha"))
            assert len(attempts) == 1
            assert attempts[0]["outcome"] == "success"
            assert attempts[0]["attempt_no"] == 1
            assert attempts[0]["settled_at"] is not None

    def test_failure_lands_in_archive_with_attempt_trace(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", explode_handler)
        p.enqueue(Job("work", "item_beta", max_retries_per_run=0))
        with in_process_workers(p):
            p.run(install_signals=False)
        p.stop()

        with StateSnapshot.open(p) as snap:
            rec = snap.get_job("work::item_beta")
            assert rec is not None and rec["status"] == "failed"
            assert "end-to-end explosion" in (rec["result"]["message"] or "")
            attempts = list(snap.iter_attempts(job_uid="work::item_beta"))
            assert len(attempts) == 1 and attempts[0]["outcome"] == "failure"

    def test_transient_retry_exhaustion_trace(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p.register_task("work", always_transient_handler)
        p.enqueue(Job("work", "item_gamma", max_retries_per_run=2))
        with in_process_workers(p):
            p.run(install_signals=False)
        p.stop()

        with StateSnapshot.open(p) as snap:
            rec = snap.get_job("work::item_gamma")
            assert rec["status"] == "failed"
            assert rec["result"]["message"] == "always transient"
            attempts = list(snap.iter_attempts(job_uid="work::item_gamma"))
            assert [a["attempt_no"] for a in attempts] == [1, 2, 3]
            assert [a["outcome"] for a in attempts] == ["retry", "retry", "failure"]

    def test_transient_recovery_second_attempt_succeeds(self, tmp_path):
        p = _make_pipeline(tmp_path)
        out_dir = str(tmp_path / "output")
        p.register_task("work", flaky_first_attempt_handler)
        p.enqueue(Job("work", "item_delta", payload={"output_dir": out_dir}, max_retries_per_run=3))
        with in_process_workers(p):
            res = p.run(install_signals=False)
        p.stop()

        assert res is None
        with StateSnapshot.open(p) as snap:
            assert snap.get_job("work::item_delta")["status"] == "succeeded"
            attempts = list(snap.iter_attempts(job_uid="work::item_delta"))
            assert [a["outcome"] for a in attempts] == ["retry", "success"]
            assert [a["attempt_no"] for a in attempts] == [1, 2]

    def test_cross_process_retry_attempt_no_observed_by_handler(self, tmp_path):
        """跨真实子进程重试序号回归验证：handler 观察到自增的 job.attempt_no，且 job.to_dict() 不含该键。"""
        p = _make_pipeline(tmp_path, backend="sqlite")
        p.register_task("work", attempt_aware_retry_handler)
        p.enqueue(Job("work", "item_retry", max_retries_per_run=3))
        res = p.run(install_signals=False)
        p.stop()

        assert res is None
        with StateSnapshot.open(p) as snap:
            rec = snap.get_job("work::item_retry")
            assert rec["status"] == "succeeded"
            assert rec["result"]["metadata"]["final_attempt"] == 3
            attempts = list(snap.iter_attempts(job_uid="work::item_retry"))
            assert [a["outcome"] for a in attempts] == ["retry", "retry", "success"]
            assert [a["attempt_no"] for a in attempts] == [1, 2, 3]

    def test_manual_requeue_runs_to_success(self, tmp_path):
        p = _make_pipeline(tmp_path)
        out_dir = str(tmp_path / "output")
        p.register_task("work", fail_once_then_ok_handler)
        p.enqueue(Job("work", "item_eps", payload={"k": "v", "output_dir": out_dir}, max_retries_per_run=0))
        with in_process_workers(p):
            p.run(install_signals=False)

        with StateSnapshot.open(p) as snap:
            entries = list(snap.iter_jobs(status="failed"))
            assert [e["job_uid"] for e in entries] == ["work::item_eps"]
            assert entries[0]["job"]["payload"]["k"] == "v"

        p.set_job_status("work::item_eps", "pending", recursive=False)
        with StateSnapshot.open(p) as snap:
            assert list(snap.iter_jobs(status="failed")) == []

        with in_process_workers(p):
            res = p.run(install_signals=False)
        p.stop()
        assert res is None
        with StateSnapshot.open(p) as snap:
            rec = snap.get_job("work::item_eps")
            assert rec["status"] == "succeeded"
            assert rec["result"]["metadata"]["recovered"] is True
            attempts = list(snap.iter_attempts(job_uid="work::item_eps"))
            assert [a["outcome"] for a in attempts] == ["failure", "success"]
            assert [a["attempt_no"] for a in attempts] == [1, 2]

    def test_always_rerun_advances_attempt_number_and_success_count(self, tmp_path):
        p = _make_pipeline(tmp_path)
        out_dir = str(tmp_path / "output")
        p.register_task("work", ok_with_input_handler)
        p.enqueue(Job("work", "item_eta", payload={"output_dir": out_dir}, trigger="always"))
        with in_process_workers(p):
            assert p.run(install_signals=False) is None
        assert p._runtime.store.backend.load_successes()["work::item_eta"]["success_count"] == 1

        p.enqueue(Job("work", "item_eta", payload={"output_dir": out_dir}, trigger="always"))
        with in_process_workers(p):
            assert p.run(install_signals=False) is None
        p.stop()

        assert p._runtime.store.backend.load_successes()["work::item_eta"]["success_count"] == 2
        with StateSnapshot.open(p) as snap:
            attempts = list(snap.iter_attempts(job_uid="work::item_eta"))
            assert [a["outcome"] for a in attempts] == ["success", "success"]
            assert [a["attempt_no"] for a in attempts] == [1, 2]

    def test_six_step_contract_with_resource(self, tmp_path):
        p = _make_pipeline(tmp_path)
        out_dir = str(tmp_path / "output")
        p.register_resource(CapacityResource("io_slots", max_capacity=4.0))
        p.register_task("work", ok_with_input_handler)
        assert p.enqueue(Job("work", "step_one", payload={"output_dir": out_dir})) is True
        assert p.enqueue(Job("work", "step_two", payload={"output_dir": out_dir})) is True
        assert p.enqueue(Job("work", "step_one", payload={"output_dir": out_dir})) is False
        with in_process_workers(p):
            res = p.run(install_signals=False)
        p.stop()

        assert res is None
        with StateSnapshot.open(p) as snap:
            succ = {j["job_uid"] for j in snap.iter_jobs(status="succeeded")}
            assert succ == {"work::step_one", "work::step_two"}
            candidates = [Job("work", "step_one"), Job("work", "next")]
            assert [j for j in candidates if j.uid not in succ] == [Job("work", "next")]
