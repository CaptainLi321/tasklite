"""依赖缺失在真实 SQLite 与并行 worker 下的结算契约。"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path

from tasklite import Job, TaskLite, TaskResult


def _pipeline(tmp_path):
    output = tmp_path / "output"
    output.mkdir(parents=True, exist_ok=True)
    return TaskLite(
        backend="sqlite",
        state_path=tmp_path / "dependency.db",
        output_roots=output,
        max_workers=2,
    )


def _succeed(job, ctx):
    return TaskResult.success(metadata={"pid": os.getpid()})


def _wait_for_release(job, ctx):
    output = Path(ctx.output_roots)
    (output / "dependency_worker_ready").write_text("ready", encoding="utf-8")
    deadline = time.monotonic() + 30
    while not (output / "dependency_worker_release").exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("dependency barrier timed out")
        time.sleep(0.01)
    return TaskResult.success(metadata={"pid": os.getpid()})


def _wait_until_failed(pipeline, expected, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if set(pipeline._runtime.store.backend.load_failed()) == expected:
            return
        time.sleep(0.01)
    raise TimeoutError(f"失败档案未收敛：{pipeline._runtime.store.backend.load_failed()!r}")


def test_cleared_manual_success_settles_dependency_as_missing(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("control", _succeed)
    pipeline.register_task("source", _succeed)
    pipeline.register_task("dependent", _succeed)

    assert pipeline.enqueue(Job("control", "baseline")) is True
    assert pipeline.enqueue(Job("source", "prior")) is True
    pipeline.run()
    assert len(pipeline._runtime.store.backend.load_attempts("source::prior")) == 1
    pipeline.set_job_status(Job("source", "up"), "succeeded", recursive=False)
    assert pipeline.enqueue(
        Job("dependent", "down", depends_on=["source::up"])
    ) is True
    pipeline.clear_history("source::up", records=["successes"])

    pipeline.run()
    pipeline.stop()

    assert set(pipeline._runtime.store.backend.load_failed()) == {"dependent::down"}
    failure = pipeline._runtime.store.backend.load_failed()["dependent::down"]
    assert failure["missing_dependency"] == "source::up"
    assert pipeline._runtime.store.backend.load_attempts("dependent::down") == []
    assert pipeline._runtime.store.backend.load_attempts("source::up") == []
    assert len(pipeline._runtime.store.backend.load_attempts("control::baseline")) == 1
    assert len(pipeline._runtime.store.backend.load_attempts("source::prior")) == 1


def test_missing_dependency_settles_while_unrelated_worker_is_in_flight(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("blocking", _wait_for_release, trigger="always")
    pipeline.register_task("source", _succeed)
    pipeline.register_task("dependent", _succeed)
    pipeline.enqueue(Job("blocking", "unrelated"))
    pipeline.set_job_status(Job("source", "up"), "succeeded", recursive=False)
    pipeline.enqueue(Job("dependent", "gap", depends_on=["source::up"]))
    pipeline.enqueue(Job("dependent", "down", depends_on=["dependent::gap"]))
    pipeline.clear_history("source::up", records=["successes"])

    errors = []

    def run_pipeline():
        try:
            pipeline.run(install_signals=False)
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_pipeline, daemon=True)
    runner.start()
    output = Path(pipeline.output_roots)
    try:
        ready = output / "dependency_worker_ready"
        deadline = time.monotonic() + 20
        while not ready.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("无关 worker 未达到事件屏障")
            time.sleep(0.01)

        _wait_until_failed(pipeline, {"dependent::gap", "dependent::down"})
        rows = {
            row["job_uid"]: row["status"]
            for row in pipeline._runtime.store.backend.load_queue_jobs()
        }
        assert rows == {"blocking::unrelated": "running"}
        assert pipeline._runtime.store.backend.load_attempts("dependent::gap") == []
        assert pipeline._runtime.store.backend.load_attempts("dependent::down") == []
    finally:
        (output / "dependency_worker_release").write_text(
            "release", encoding="utf-8"
        )
        runner.join(timeout=30)

    assert not runner.is_alive()
    assert errors == []
    assert "blocking::unrelated" in pipeline._runtime.store.backend.load_successes()
    attempts = pipeline._runtime.store.backend.load_attempts("blocking::unrelated")
    assert [row.outcome for row in attempts] == ["succeeded"]
    assert pipeline._runtime.store.backend.load_queue() == []
