"""触发器在真实 spawn、跨 run 与持久化恢复中的公开契约。"""
from __future__ import annotations

import multiprocessing as mp
import os
from pathlib import Path

import pytest

from tasklite import FileSpec, Job, TaskLite, TaskResult
from tasklite.triggers import on_input_change
from tasklite.utils.clock import parse_utc_iso_to_us


@pytest.fixture(autouse=True)
def _actual_spawn(_patch_mp_context, monkeypatch):
    """本模块使用生产 spawn 上下文，不继承父进程配置。"""
    monkeypatch.setattr(mp, "get_context", lambda method=None: mp.context.SpawnContext())
    assert mp.get_context("spawn").get_start_method() == "spawn"


def _pipeline(tmp_path, **overrides):
    out = tmp_path / "output"
    out.mkdir(parents=True, exist_ok=True)
    pipeline = TaskLite(backend="sqlite", state_path=tmp_path / "trigger_lifecycle.db",
                        output_roots=out, **overrides)
    pipeline.authorizer = lambda *args: True
    return pipeline


def produce_input(job, ctx):
    mode = job.payload.get("mode", "write")
    if mode == "fail":
        return TaskResult.failure(message="upstream failed in this batch")
    path = Path(ctx.declare_file("source", FileSpec(
        ctx.output_roots / "source.txt",
        role="output",
        delete_existing=mode == "write",
    )))
    if mode == "write":
        path.write_text(job.payload.get("content", "first"), encoding="utf-8")
        if "mtime_ns" in job.payload:
            stamp = job.payload["mtime_ns"]
            os.utime(path, ns=(stamp, stamp))
    return TaskResult.success(metadata={"pid": os.getpid(), "mode": mode})


def consume_input(job, ctx):
    path = ctx.file_path("source")
    return TaskResult.success(metadata={
        "content": path.read_text(encoding="utf-8"), "pid": os.getpid(),
    })


@pytest.mark.parametrize("mode", ["write", "unchanged", "fail"])
def test_reverse_batch_evaluates_input_after_upstream_settles(tmp_path, mode):
    first = _pipeline(tmp_path)
    first.register_task("produce", produce_input, trigger="always")
    first.register_task("consume", consume_input, trigger="on_input_change")
    first.enqueue(Job("produce", "source"))
    consumer_files = {
        "source": FileSpec.from_output(
            "produce::source", "source", role="input"
        ),
    }
    first.enqueue(Job(
        "consume",
        "result",
        depends_on=["produce::source"],
        files=consumer_files,
    ))
    assert first.run() is None
    first.stop()
    first_produce_run_id = first.backend.load_successes()["produce::source"]["last_success_run_id"]
    old_meta = first.backend.load_successes()["consume::result"]
    assert old_meta["pid"] != os.getpid()
    baseline_us = parse_utc_iso_to_us(old_meta["last_succeeded_at"])

    second = _pipeline(tmp_path)
    evaluated = []
    def observe_input(ctx):
        evaluated.append(Path(ctx.success_metadata["inputs"][0]).read_text(encoding="utf-8"))
        parent = second.backend.load_successes()["produce::source"]
        assert parent["last_success_run_id"] != first_produce_run_id
        return on_input_change(ctx)
    second.register_trigger("observe", observe_input)
    second.register_task("produce", produce_input, trigger="always")
    second.register_task("consume", consume_input, trigger="observe")
    second.enqueue(Job(
        "consume",
        "result",
        depends_on=["produce::source"],
        files=consumer_files,
    ))
    second.enqueue(Job("produce", "source", payload={"mode": mode, "content": "updated",
                                                     "mtime_ns": (baseline_us + 1) * 1000}))
    res = second.run()
    second.stop()
    assert res is None
    if mode == "fail":
        assert evaluated == []
        assert set(second.backend.load_failed()) == {"produce::source", "consume::result"}
        assert second.backend.load_successes() == {}
    else:
        assert evaluated == ["updated" if mode == "write" else "first"]
        current = second.backend.load_successes()["consume::result"]
        assert current["success_count"] == (2 if mode == "write" else 1)
        assert current["content"] == evaluated[0]
        assert second.backend.load_failed() == {}
    assert second.backend.load_queue() == []


def always_retry(job, ctx):
    return TaskResult.retry(message="retry until budget exhausted")


def _run_until_retry_committed(root, ready, release):
    """队列重试已提交的屏障供监督进程精确终止主引擎。"""
    def park_after_retry(uid, *, outcome):
        if outcome.will_retry:
            queued = pipeline.backend.load_queue()
            assert (queued[0].get("activation_no", 1), queued[0]["attempt_no"]) == (1, 2)
            ready.set()
            release.wait(timeout=30)
    pipeline = _pipeline(Path(root))
    pipeline._runtime.store.set_on_attempt_finished(park_after_retry)
    pipeline.register_trigger("gated", lambda ctx: True)
    pipeline.register_task("retry", always_retry, max_retries_per_run=1, trigger="gated")
    pipeline.enqueue(Job("retry", "item"))
    pipeline.run()


def test_killed_engine_recovers_retry_without_resetting_activation_or_budget(tmp_path):
    context = mp.get_context("spawn")
    ready, release = context.Event(), context.Event()
    engine = context.Process(target=_run_until_retry_committed,
                             args=(str(tmp_path), ready, release))
    engine.start()
    try:
        assert ready.wait(20), "主引擎必须到达已提交重试的屏障"
        engine.kill()
        engine.join(timeout=10)
        assert not engine.is_alive()
        assert engine.exitcode != 0
        if os.name == "posix":
            assert engine.exitcode < 0
    finally:
        if engine.is_alive():
            engine.kill()
            engine.join(timeout=10)
        engine.close()

    recovered = _pipeline(tmp_path)
    queued = recovered.backend.load_queue()
    assert len(queued) == 1
    recovered.register_trigger("gated", lambda ctx: True)
    recovered.register_task("retry", always_retry, max_retries_per_run=1, trigger="gated")
    recovered.run()
    recovered.stop()
    records = recovered.backend.load_attempts("retry::item")
    assert [(row.attempt_no, row.outcome) for row in records] == [
        (1, "requeued"), (2, "requeued"), (3, "failed")]
    assert len({row.run_id for row in records}) == 2
    assert recovered.backend.load_queue() == []
    assert recovered.backend.load_successes() == {}
    assert recovered.backend.load_failed()["retry::item"]["error_message"] == "MAX_RETRIES_EXCEEDED"


@pytest.mark.parametrize("seed_after_clear", [False, True])
def test_untrusted_success_reexecutes_without_reusing_old_attempt_baseline(tmp_path, seed_after_clear):
    first = _pipeline(tmp_path)
    first.register_task("produce", produce_input)
    first.register_task("consume", consume_input, trigger="on_input_change")
    first.enqueue(Job("produce", "source"))
    first.enqueue(Job(
        "consume",
        "result",
        depends_on=["produce::source"],
        files={
            "source": FileSpec.from_output(
                "produce::source", "source", role="input"
            ),
        },
    ))
    first.run()
    first.clear_history("consume::result", records=["successes"])
    if seed_after_clear:
        first.set_job_status(
            Job(
                "consume",
                "result",
                depends_on=["produce::source"],
                files={
                    "source": FileSpec.from_output(
                        "produce::source", "source", role="input"
                    ),
                },
            ),
            "succeeded",
            recursive=False,
        )
    first.stop()

    second = _pipeline(tmp_path)
    second.register_task("consume", consume_input, trigger="on_input_change")
    second.enqueue(Job(
        "consume",
        "result",
        depends_on=["produce::source"],
        files={
            "source": FileSpec.from_output(
                "produce::source", "source", role="input"
            ),
        },
    ))
    res = second.run()
    second.stop()
    assert res is None
    assert second.backend.load_successes()["consume::result"]["success_count"] == 1
    records = second.backend.load_attempts("consume::result")
    assert [row.attempt_no for row in records] == [1, 2]
    assert all(row.outcome == "succeeded" for row in records)


def discover_page(job, ctx, page):
    with (ctx.output_roots / "scan_calls").open("a", encoding="utf-8") as stream:
        stream.write(f"{page}:{os.getpid()}\n")
    return [{"id": "item"}] if page == 1 else []


def discover_id(item):
    return item["id"]


def spawn_discovered(job, ctx, item, content_id):
    ctx.spawn(Job("child", content_id))


def discovered_child(job, ctx):
    return TaskResult.success(metadata={"pid": os.getpid()})


def test_discovery_defaults_always_and_job_once_override_across_runs(tmp_path):
    from tasklite.wrappers.discovery import register_discovery
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("child", discovered_child)
    register_discovery(pipeline, "discover", discover_page, discover_id,
                       spawn_discovered, "child")
    pipeline.enqueue(Job("discover", "root"))
    pipeline.run()
    first_children = {uid: metadata for uid, metadata in pipeline.backend.load_successes().items()
                      if uid.startswith("child::")}
    assert len(first_children) == 1
    pipeline.enqueue(Job("discover", "root"))
    pipeline.run()
    calls = (tmp_path / "output" / "scan_calls").read_text(encoding="utf-8").splitlines()
    assert [line.split(":")[0] for line in calls] == ["1", "2", "1"]
    assert all(int(line.split(":")[1]) != os.getpid() for line in calls)
    assert pipeline.backend.load_successes()["discover::root"]["success_count"] == 2
    pipeline.enqueue(Job("discover", "root", trigger="once"))
    res = pipeline.run()
    pipeline.stop()
    assert res is None
    assert (tmp_path / "output" / "scan_calls").read_text(encoding="utf-8").splitlines() == calls
    assert {uid: metadata for uid, metadata in pipeline.backend.load_successes().items()
            if uid.startswith("child::")} == first_children


def _wait_for_file(path, timeout=20):
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size:
            return
        time.sleep(0.01)
    raise TimeoutError(f"未到达文件屏障：{path}")


def waiting_for_release(job, ctx):
    (ctx.output_roots / "worker_ready").write_text(str(os.getpid()), encoding="utf-8")
    _wait_for_file(ctx.output_roots / "worker_release")
    return TaskResult.success(metadata={"pid": os.getpid()})


def test_trigger_exception_aborts_real_worker_and_preserves_recovery_candidates(tmp_path):
    pipeline = _pipeline(tmp_path, max_workers=2)
    def explode_after_worker_ready(ctx):
        _wait_for_file(tmp_path / "output" / "worker_ready")
        raise RuntimeError("trigger boundary")
    pipeline.register_trigger("explode", explode_after_worker_ready)
    pipeline.register_task("waiting", waiting_for_release, max_retries_per_run=0, trigger="always")
    pipeline.register_task("later", discovered_child, trigger="explode")
    pipeline.enqueue(Job("waiting", "item"))
    pipeline.enqueue(Job("later", "item"))
    with pytest.raises(RuntimeError, match="trigger boundary"):
        pipeline.run()
    queue_rows = pipeline.backend.load_queue_jobs()
    assert {row["job_uid"]: row["status"] for row in queue_rows} == {
        "waiting::item": "running", "later::item": "pending",
    }
    rows = [row["job"] for row in queue_rows]
    assert {row["task_type"] for row in rows} == {"waiting", "later"}
    assert all(row["attempt_no"] == 1 and row["runtime"] == {} for row in rows)
    assert pipeline.backend.load_failed() == {}
    assert pipeline.backend.load_successes() == {}
    assert pipeline.backend.load_attempts("later::item") == []
    assert not mp.active_children()

    (tmp_path / "output" / "worker_release").write_text("release", encoding="utf-8")
    recovered = _pipeline(tmp_path)
    recovered.register_trigger("explode", lambda ctx: False)
    recovered.register_task("waiting", waiting_for_release, max_retries_per_run=0, trigger="always")
    recovered.register_task("later", discovered_child, trigger="explode")
    res = recovered.run()
    recovered.stop()
    assert res is None
    assert set(recovered.backend.load_failed()) == {"waiting::item"}
    assert recovered.backend.load_queue() == []
    attempts = recovered.backend.load_attempts("waiting::item")
    assert len(attempts) == 1
    assert attempts[0].outcome == "failed" and attempts[0].attempt_no == 1
