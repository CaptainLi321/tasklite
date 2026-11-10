"""触发器在真实 spawn、跨 run 与持久化恢复中的公开契约。"""
from __future__ import annotations

import multiprocessing as mp
import os
from pathlib import Path

import pytest

from tasklite import FileSpec, Job, TaskLite, TaskResult
from tasklite.engine.errorclass import ERR_MAX_RETRIES
from tasklite.triggers import on_input_change
from tasklite.utils.clock import parse_utc_iso_to_us


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
    first_produce_run_id = first._runtime.store.backend.load_successes()["produce::source"]["last_success_run_id"]
    old_meta = first._runtime.store.backend.load_successes()["consume::result"]
    assert old_meta["result"]["metadata"]["pid"] != os.getpid()
    baseline_us = parse_utc_iso_to_us(old_meta["last_succeeded_at"])

    second = _pipeline(tmp_path)
    evaluated = []
    def observe_input(ctx):
        evaluated.append(Path(ctx.success_metadata["inputs"][0]).read_text(encoding="utf-8"))
        parent = second._runtime.store.backend.load_successes()["produce::source"]
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
        assert set(second._runtime.store.backend.load_failed()) == {"produce::source", "consume::result"}
        assert second._runtime.store.backend.load_successes() == {}
    else:
        assert evaluated == ["updated" if mode == "write" else "first"]
        current = second._runtime.store.backend.load_successes()["consume::result"]
        assert current["success_count"] == (2 if mode == "write" else 1)
        assert current["result"]["metadata"]["content"] == evaluated[0]
        assert second._runtime.store.backend.load_failed() == {}
    assert second._runtime.store.backend.load_queue() == []


def always_retry(job, ctx):
    return TaskResult.retry(message="retry until budget exhausted")


def _record_child_call(ctx) -> int:
    calls = ctx.output_roots / "child_calls.txt"
    with calls.open("a", encoding="utf-8") as stream:
        stream.write(f"{ctx.job.attempt_no}\n")
    return len(calls.read_text(encoding="utf-8").splitlines())


def retry_budget_child(job, ctx):
    calls = _record_child_call(ctx)
    if calls == 1:
        return TaskResult.retry(message="first attempt retries")
    if calls >= 3 and job.trigger == "always":
        return TaskResult.retry(message="ordinary rerun shares run budget")
    return TaskResult.success(metadata={"calls": calls})


def zero_budget_child(job, ctx):
    calls = _record_child_call(ctx)
    if calls == 1:
        return TaskResult.retry(
            message="first attempt retries without budget",
            consume_budget=False,
        )
    return TaskResult.success(metadata={"calls": calls})


def spawn_same_child(job, ctx):
    ctx.spawn(Job(
        "child",
        "item",
        trigger=job.payload["child_trigger"],
        max_retries_per_run=job.payload["child_max_retries"],
    ))
    return TaskResult.success(metadata={"spawned": "child::item"})


def retry_again_then_succeed(job, ctx):
    if job.attempt_no == 2:
        return TaskResult.retry(message="second run has its own budget")
    return TaskResult.success(metadata={"resumed_at": job.attempt_no})


@pytest.mark.parametrize(
    ("child_trigger", "expected_outcomes", "expected_calls"),
    [
        ("once", ["requeued", "succeeded"], ["1", "2"]),
        ("always", ["requeued", "succeeded", "failed"], ["1", "2", "3"]),
    ],
)
def test_retry_candidate_and_later_ordinary_candidate_same_run(
    tmp_path, child_trigger, expected_outcomes, expected_calls
):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("child", retry_budget_child)
    pipeline.register_task("parent", spawn_same_child)
    child = Job(
        "child",
        "item",
        trigger=child_trigger,
        max_retries_per_run=1,
    )
    pipeline.enqueue(child)
    pipeline.enqueue(Job(
        "parent",
        "p",
        depends_on=[child.uid],
        payload={
            "child_trigger": child_trigger,
            "child_max_retries": 1,
        },
    ))

    assert pipeline.run() is None
    pipeline.stop()

    calls = (tmp_path / "output" / "child_calls.txt").read_text(
        encoding="utf-8"
    ).splitlines()
    assert calls == expected_calls
    records = pipeline._runtime.store.backend.load_attempts("child::item")
    assert [record.attempt_no for record in records] == [1, 2, 3][
        :len(expected_calls)
    ]
    assert [record.outcome for record in records] == expected_outcomes
    assert len({record.run_id for record in records}) == 1
    assert "parent::p" in pipeline._runtime.store.backend.load_successes()
    if child_trigger == "always":
        assert pipeline._runtime.store.backend.load_failed()["child::item"]["error_message"] == (
            ERR_MAX_RETRIES
        )
    else:
        assert pipeline._runtime.store.backend.load_failed() == {}
        assert pipeline._runtime.store.backend.load_successes()["child::item"]["result"]["metadata"]["calls"] == 2
    assert pipeline._runtime.store.backend.load_queue() == []


def test_zero_budget_retry_skips_gate_without_resetting_ordinary_candidate(tmp_path):
    pipeline = _pipeline(tmp_path)
    evaluated: list[tuple[int, bool]] = []

    def gate(ctx):
        evaluated.append((ctx.job.attempt_no, ctx.has_succeeded))
        return len(evaluated) == 1

    pipeline.register_trigger("gate", gate)
    pipeline.register_task("child", zero_budget_child)
    pipeline.register_task("parent", spawn_same_child)
    child = Job(
        "child",
        "item",
        trigger="gate",
        max_retries_per_run=0,
    )
    pipeline.enqueue(child)
    pipeline.enqueue(Job(
        "parent",
        "p",
        depends_on=[child.uid],
        payload={
            "child_trigger": "gate",
            "child_max_retries": 0,
        },
    ))

    assert pipeline.run() is None
    pipeline.stop()

    assert evaluated == [(1, False), (1, True)]
    records = pipeline._runtime.store.backend.load_attempts("child::item")
    assert [record.attempt_no for record in records] == [1, 2]
    assert [record.outcome for record in records] == ["requeued", "succeeded"]
    assert pipeline._runtime.store.backend.load_failed() == {}
    assert pipeline._runtime.store.backend.load_successes()["child::item"]["result"]["metadata"]["calls"] == 2
    assert pipeline._runtime.store.backend.load_queue() == []


def _run_until_retry_committed(root, ready, release):
    """队列重试已提交的屏障供监督进程精确终止主引擎。"""
    def park_after_retry(uid, *, outcome):
        if outcome.will_retry:
            queued = pipeline._runtime.store.backend.load_queue()
            assert queued[0]["attempt_no"] == 2
            ready.set()
            release.wait(timeout=30)
    pipeline = _pipeline(Path(root))
    pipeline._runtime.store.set_on_attempt_finished(park_after_retry)
    pipeline.register_trigger("gated", lambda ctx: True)
    pipeline.register_task("retry", always_retry, max_retries_per_run=1, trigger="gated")
    pipeline.enqueue(Job("retry", "item"))
    pipeline.run()


@pytest.mark.parametrize("gate_allows", [False, True])
def test_new_run_rechecks_retry_pending_and_resets_budget(tmp_path, gate_allows):
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
    queued = recovered._runtime.store.backend.load_queue()
    assert len(queued) == 1
    assert queued[0]["attempt_no"] == 2
    recovered.register_trigger("gated", lambda ctx: gate_allows)
    handler = retry_again_then_succeed if gate_allows else always_retry
    recovered.register_task("retry", handler, max_retries_per_run=1, trigger="gated")
    recovered.run()
    recovered.stop()
    records = recovered._runtime.store.backend.load_attempts("retry::item")
    assert recovered._runtime.store.backend.load_queue() == []
    if gate_allows:
        assert [(row.attempt_no, row.outcome) for row in records] == [
            (1, "requeued"),
            (2, "requeued"),
            (3, "succeeded"),
        ]
        assert len({row.run_id for row in records}) == 2
        assert recovered._runtime.store.backend.load_successes()["retry::item"]["result"]["metadata"]["resumed_at"] == 3
        assert recovered._runtime.store.backend.load_failed() == {}
    else:
        assert [(row.attempt_no, row.outcome) for row in records] == [
            (1, "requeued"),
        ]
        assert recovered._runtime.store.backend.load_successes() == {}
        assert recovered._runtime.store.backend.load_failed() == {}


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
    assert second._runtime.store.backend.load_successes()["consume::result"]["success_count"] == 1
    records = second._runtime.store.backend.load_attempts("consume::result")
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
    first_children = {uid: metadata for uid, metadata in pipeline._runtime.store.backend.load_successes().items()
                      if uid.startswith("child::")}
    assert len(first_children) == 1
    pipeline.enqueue(Job("discover", "root"))
    pipeline.run()
    calls = (tmp_path / "output" / "scan_calls").read_text(encoding="utf-8").splitlines()
    assert [line.split(":")[0] for line in calls] == ["1", "2", "1"]
    assert all(int(line.split(":")[1]) != os.getpid() for line in calls)
    assert pipeline._runtime.store.backend.load_successes()["discover::root"]["success_count"] == 2
    pipeline.enqueue(Job("discover", "root", trigger="once"))
    res = pipeline.run()
    pipeline.stop()
    assert res is None
    assert (tmp_path / "output" / "scan_calls").read_text(encoding="utf-8").splitlines() == calls
    assert {uid: metadata for uid, metadata in pipeline._runtime.store.backend.load_successes().items()
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
    queue_rows = pipeline._runtime.store.backend.load_queue_jobs()
    assert {row["job_uid"]: row["status"] for row in queue_rows} == {
        "waiting::item": "running", "later::item": "pending",
    }
    rows = [row["job"] for row in queue_rows]
    assert {row["task_type"] for row in rows} == {"waiting", "later"}
    assert all(row["attempt_no"] == 1 and row["runtime"] == {} for row in rows)
    assert pipeline._runtime.store.backend.load_failed() == {}
    assert pipeline._runtime.store.backend.load_successes() == {}
    assert pipeline._runtime.store.backend.load_attempts("later::item") == []
    assert not mp.active_children()

    (tmp_path / "output" / "worker_release").write_text("release", encoding="utf-8")
    recovered = _pipeline(tmp_path)
    recovered.register_trigger("explode", lambda ctx: False)
    recovered.register_task("waiting", waiting_for_release, max_retries_per_run=0, trigger="always")
    recovered.register_task("later", discovered_child, trigger="explode")
    res = recovered.run()
    recovered.stop()
    assert res is None
    assert set(recovered._runtime.store.backend.load_failed()) == {"waiting::item"}
    assert recovered._runtime.store.backend.load_queue() == []
    attempts = recovered._runtime.store.backend.load_attempts("waiting::item")
    assert len(attempts) == 1
    assert attempts[0].outcome == "failed" and attempts[0].attempt_no == 1


def _record_until_success_call(job, ctx):
    calls = ctx.output_roots / "until_success_calls.txt"
    with calls.open("a", encoding="utf-8") as stream:
        stream.write(f"{job.uid}\n")
    return TaskResult.success(metadata={"calls": len(calls.read_text().splitlines())})


def _fail_until_success(job, ctx):
    _record_until_success_call(job, ctx)
    return TaskResult.failure(message="still failing")


def _spawn_with_unknown_trigger(job, ctx):
    ctx.set_cursor("trigger/spawn", "must-not-publish")
    ctx.spawn(Job("child", "good", trigger="allowed"))
    ctx.spawn(Job("child", "bad", trigger="missing"))
    return TaskResult.success()


def test_until_success_uses_current_sqlite_terminal_history(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("work", _record_until_success_call, trigger="until_success")

    assert pipeline.enqueue(Job("work", "item")) is True
    pipeline.run()
    assert pipeline._runtime.store.backend.load_attempts("work::item")[0].outcome == "succeeded"

    assert pipeline.enqueue(Job("work", "item")) is True
    pipeline.run()
    assert len(pipeline._runtime.store.backend.load_attempts("work::item")) == 1

    pipeline.clear_history("work::item", records=["successes"])
    pipeline.set_job_status(
        Job("work", "item", trigger="until_success"),
        "failed",
        recursive=False,
    )
    assert pipeline.enqueue(Job("work", "item")) is True
    pipeline.run()
    assert len(pipeline._runtime.store.backend.load_attempts("work::item")) == 2
    pipeline.stop()


def test_until_success_recognizes_manual_success_without_attempt(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("work", _record_until_success_call, trigger="until_success")
    pipeline.set_job_status(
        Job("work", "manual", trigger="until_success"),
        "succeeded",
        recursive=False,
    )

    assert pipeline.enqueue(Job("work", "manual")) is True
    pipeline.run()
    pipeline.stop()

    assert pipeline._runtime.store.backend.load_attempts("work::manual") == []
    assert pipeline._runtime.store.backend.load_successes()["work::manual"]["annotations"][
        "completion_source"
    ] == "manual"


def test_until_success_retries_after_real_failure(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task(
        "work",
        _fail_until_success,
        trigger="until_success",
        max_retries_per_run=0,
    )

    assert pipeline.enqueue(Job("work", "item")) is True
    pipeline.run()
    assert len(pipeline._runtime.store.backend.load_attempts("work::item")) == 1

    assert pipeline.enqueue(Job("work", "item")) is True
    pipeline.run()
    pipeline.stop()

    assert [row.outcome for row in pipeline._runtime.store.backend.load_attempts("work::item")] == [
        "failed",
        "failed",
    ]
    calls = (tmp_path / "output" / "until_success_calls.txt").read_text(
        encoding="utf-8"
    ).splitlines()
    assert calls == ["work::item", "work::item"]


def test_legacy_trigger_pending_is_preserved_and_rejected_on_use(tmp_path):
    pipeline = _pipeline(tmp_path)
    legacy = Job("work", "legacy", trigger="on_failure").to_dict()
    pipeline._runtime.store.backend.save_queue([legacy])
    pipeline.close()

    reopened = _pipeline(tmp_path)
    reopened.register_task("work", _record_until_success_call)
    assert reopened._runtime.store.backend.load_queue() == [legacy]
    with pytest.raises(ValueError, match="unregistered trigger 'on_failure'"):
        reopened.run()
    reopened.stop()

    assert reopened._runtime.store.backend.load_queue() == [legacy]
    assert reopened._runtime.store.backend.load_attempts("work::legacy") == []
    assert reopened._runtime.store.backend.load_failed() == {}


def test_unknown_spawned_trigger_blocks_siblings_and_cursor_publication(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_trigger("allowed", lambda ctx: True)
    pipeline.register_task("child", _record_until_success_call)
    pipeline.register_task("parent", _spawn_with_unknown_trigger, trigger="always")
    pipeline.enqueue(Job("parent", "root"))

    pipeline.run()
    pipeline.stop()

    assert "parent::root" in pipeline._runtime.store.backend.load_failed()
    assert pipeline._runtime.store.backend.load_successes() == {}
    assert pipeline._runtime.store.backend.load_queue() == []
    assert pipeline.get_cursor("trigger/spawn") is None
    assert pipeline._runtime.store.backend.load_attempts("child::good") == []
    assert pipeline._runtime.store.backend.load_attempts("child::bad") == []
