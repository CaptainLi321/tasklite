"""依赖缺失在 SQLite 与并行 worker 下的结算契约。"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path

from tasklite import Job, StateSnapshot, TaskLite, TaskResult
from tasklite.testing import in_process_workers


def _pipeline(tmp_path):
    output = tmp_path / "output"
    output.mkdir(parents=True, exist_ok=True)
    prev = os.getcwd()
    os.chdir(tmp_path)
    try:
        return TaskLite(
            backend="sqlite",
            state_path=tmp_path / "dependency.db",
            max_workers=2,
        )
    finally:
        os.chdir(prev)


def _succeed(job, ctx):
    return TaskResult.success(metadata={"ok": True})


def _wait_for_release(job, ctx):
    output = Path(job.payload["output_dir"])
    (output / "dependency_worker_ready").write_text("ready", encoding="utf-8")
    deadline = time.monotonic() + 20
    while not (output / "dependency_worker_release").exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("dependency barrier timed out")
        time.sleep(0.005)
    return TaskResult.success(metadata={"ok": True})


def _wait_until_failed(pipeline, expected, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with StateSnapshot.open(pipeline) as snap:
            failed = {r["job_uid"] for r in snap.iter_jobs(status="failed")}
        if failed == expected:
            return
        time.sleep(0.005)
    raise TimeoutError(f"失败档案未收敛：{failed!r}")


def test_cleared_manual_success_settles_dependency_as_missing(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("control", _succeed)
    pipeline.register_task("source", _succeed)
    pipeline.register_task("dependent", _succeed)

    assert pipeline.enqueue(Job("control", "baseline")) is True
    assert pipeline.enqueue(Job("source", "prior")) is True
    with in_process_workers(pipeline):
        pipeline.run(install_signals=False)
    with StateSnapshot.open(pipeline) as snap:
        assert len(list(snap.iter_attempts(job_uid="source::prior"))) == 1
    from tasklite.backend.base import GraphEditCommit

    with pipeline.edit(dry_run=False) as tx:
        tx.add(Job("source", "up"), status="succeeded")
    assert pipeline.enqueue(Job("dependent", "down", depends_on=["source::up"])) is True
    pipeline._runtime.store.backend.commit_graph_edit(
        GraphEditCommit(success_writes={"source::up": None})
    )

    with in_process_workers(pipeline):
        pipeline.run(install_signals=False)
    pipeline.stop()

    with StateSnapshot.open(pipeline) as snap:
        assert {r["job_uid"] for r in snap.iter_jobs(status="failed")} == {"dependent::down"}
        failure = snap.get_job("dependent::down")
        assert failure["result"]["message"] == "Dependency failed or has no record"
        assert pipeline._runtime.store.backend.load_failed()["dependent::down"]["missing_dependency"] == "source::up"
        assert list(snap.iter_attempts(job_uid="dependent::down")) == []
        assert list(snap.iter_attempts(job_uid="source::up")) == []
        assert len(list(snap.iter_attempts(job_uid="control::baseline"))) == 1
        assert len(list(snap.iter_attempts(job_uid="source::prior"))) == 1
    pipeline.close()


def test_missing_dependency_settles_while_unrelated_worker_is_in_flight(tmp_path):
    pipeline = _pipeline(tmp_path)
    output = tmp_path / "output"
    pipeline.register_task("blocking", _wait_for_release, trigger="always")
    pipeline.register_task("source", _succeed)
    pipeline.register_task("dependent", _succeed)
    pipeline.enqueue(Job("blocking", "unrelated", payload={"output_dir": str(output)}))
    from tasklite.backend.base import GraphEditCommit

    with pipeline.edit(dry_run=False) as tx:
        tx.add(Job("source", "up"), status="succeeded")
    pipeline.enqueue(Job("dependent", "gap", depends_on=["source::up"]))
    pipeline.enqueue(Job("dependent", "down", depends_on=["dependent::gap"]))
    pipeline._runtime.store.backend.commit_graph_edit(
        GraphEditCommit(success_writes={"source::up": None})
    )

    errors = []

    def run_pipeline():
        try:
            with in_process_workers(pipeline):
                pipeline.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_pipeline, daemon=True)
    runner.start()
    try:
        ready = output / "dependency_worker_ready"
        deadline = time.monotonic() + 15
        while not ready.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("无关 worker 未达到事件屏障")
            time.sleep(0.005)

        _wait_until_failed(pipeline, {"dependent::gap", "dependent::down"})
        with StateSnapshot.open(pipeline) as snap:
            running = [r["job_uid"] for r in snap.iter_jobs(status="running")]
            assert running == ["blocking::unrelated"]
            assert list(snap.iter_attempts(job_uid="dependent::gap")) == []
            assert list(snap.iter_attempts(job_uid="dependent::down")) == []
    finally:
        (output / "dependency_worker_release").write_text("release", encoding="utf-8")
        runner.join(timeout=20)

    assert not runner.is_alive()
    assert errors == []
    with StateSnapshot.open(pipeline) as snap:
        assert snap.get_job("blocking::unrelated")["status"] == "succeeded"
        attempts = list(snap.iter_attempts(job_uid="blocking::unrelated"))
        assert [row["outcome"] for row in attempts] == ["success"]
        assert list(snap.iter_jobs(status="pending")) == []
    pipeline.close()
