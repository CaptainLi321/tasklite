"""触发器在跨 run、重试预算与持久化恢复中的公开契约。"""
from __future__ import annotations

from contextlib import contextmanager
import multiprocessing as mp
import os
from pathlib import Path

import pytest

from tasklite import FileSpec, Job, StateSnapshot, TaskLite, TaskLiteError, TaskResult
from tasklite.testing import authorizer, in_process_workers
from tasklite.models.trigger import on_input_change
from tasklite.utils.clock import parse_utc_iso_to_us


@contextmanager
def _pipeline(tmp_path, *, backend="sqlite", **overrides):
    out = tmp_path / "output"
    out.mkdir(parents=True, exist_ok=True)
    prev = os.getcwd()
    os.chdir(tmp_path)
    try:
        kwargs = {"state_path": tmp_path / "trigger_lifecycle.db"} if backend == "sqlite" else {}
        pipeline = TaskLite(backend=backend, **kwargs, **overrides)
    finally:
        os.chdir(prev)
    try:
        with authorizer(pipeline, lambda *args: True):
            yield pipeline
    finally:
        pipeline.close()


def produce_input(job, ctx):
    mode = job.payload.get("mode", "write")
    if mode == "fail":
        return TaskResult.failure(message="upstream failed in this batch")
    path = Path(ctx.declare_file("source", FileSpec(
        Path(job.payload["output_dir"]) / "source.txt",
        role="output",
        delete_existing=mode == "write",
    )))
    if mode == "write":
        path.write_text(job.payload.get("content", "first"), encoding="utf-8")
        if "mtime_ns" in job.payload:
            stamp = job.payload["mtime_ns"]
            os.utime(path, ns=(stamp, stamp))
    return TaskResult.success(metadata={"mode": mode})


def consume_input(job, ctx):
    path = ctx.file_path("source")
    return TaskResult.success(metadata={"content": path.read_text(encoding="utf-8")})


@pytest.mark.parametrize("mode", ["write", "unchanged", "fail"])
def test_reverse_batch_evaluates_input_after_upstream_settles(tmp_path, mode):
    output_dir = str(tmp_path / "output")
    consumer_files = {
        "source": FileSpec.from_output("produce::source", "source", role="input"),
    }
    with _pipeline(tmp_path) as first:
        first.register_task("produce", produce_input, trigger="always")
        first.register_task("consume", consume_input, trigger="on_input_change")
        first.enqueue(Job("produce", "source", payload={"output_dir": output_dir}))
        first.enqueue(Job(
            "consume",
            "result",
            depends_on=["produce::source"],
            files=consumer_files,
        ))
        with in_process_workers(first):
            assert first.run(install_signals=False) is None
        first.stop()
        first_produce_run_id = first._runtime.store.backend.load_successes()["produce::source"]["last_success_run_id"]
        old_meta = first._runtime.store.backend.load_successes()["consume::result"]
        baseline_us = parse_utc_iso_to_us(old_meta["last_succeeded_at"])

    with _pipeline(tmp_path) as second:
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
        second.enqueue(Job("produce", "source", payload={
            "output_dir": output_dir,
            "mode": mode,
            "content": "updated",
            "mtime_ns": (baseline_us + 1) * 1000,
        }))
        with in_process_workers(second):
            res = second.run(install_signals=False)
        second.stop()
        assert res is None
        with StateSnapshot.open(second) as snap:
            if mode == "fail":
                assert evaluated == []
                assert {r["job_uid"] for r in snap.iter_jobs(status="failed")} == {"produce::source", "consume::result"}
                assert list(snap.iter_jobs(status="succeeded")) == []
            else:
                assert evaluated == ["updated" if mode == "write" else "first"]
                current = snap.get_job("consume::result")
                assert current["result"]["metadata"]["content"] == evaluated[0]
                assert list(snap.iter_jobs(status="failed")) == []
            assert list(snap.iter_jobs(status="pending")) == []
        if mode != "fail":
            assert second._runtime.store.backend.load_successes()["consume::result"]["success_count"] == (
                2 if mode == "write" else 1
            )


def always_retry(job, ctx):
    return TaskResult.retry(message="retry until budget exhausted")


def _record_child_call(ctx) -> int:
    calls = Path(ctx.job.payload["calls_file"])
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
        return TaskResult.retry(message="first attempt retries without budget", consume_budget=False)
    return TaskResult.success(metadata={"calls": calls})


def spawn_same_child(job, ctx):
    ctx.spawn(Job(
        "child",
        "item",
        payload={"calls_file": job.payload["calls_file"]},
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
        ("once", ["retry", "success"], ["1", "2"]),
        ("always", ["retry", "success", "failure"], ["1", "2", "3"]),
    ],
)
def test_retry_candidate_and_later_ordinary_candidate_same_run(
    tmp_path, child_trigger, expected_outcomes, expected_calls
):
    calls_file = str(tmp_path / "output" / "child_calls.txt")
    with _pipeline(tmp_path, backend="memory") as pipeline:
        pipeline.register_task("child", retry_budget_child)
        pipeline.register_task("parent", spawn_same_child)
        child = Job(
            "child",
            "item",
            payload={"calls_file": calls_file},
            trigger=child_trigger,
            max_retries_per_run=1,
        )
        pipeline.enqueue(child)
        pipeline.enqueue(Job(
            "parent",
            "p",
            depends_on=[child.uid],
            payload={
                "calls_file": calls_file,
                "child_trigger": child_trigger,
                "child_max_retries": 1,
            },
        ))

        with in_process_workers(pipeline):
            assert pipeline.run(install_signals=False) is None
        pipeline.stop()

        calls = Path(calls_file).read_text(encoding="utf-8").splitlines()
        assert calls == expected_calls
        with StateSnapshot.open(pipeline) as snap:
            records = list(snap.iter_attempts(job_uid="child::item"))
            assert [r["attempt_no"] for r in records] == [1, 2, 3][:len(expected_calls)]
            assert [r["outcome"] for r in records] == expected_outcomes
            assert len({r["run_id"] for r in records}) == 1
            assert snap.get_job("parent::p")["status"] == "succeeded"
            if child_trigger == "always":
                assert snap.get_job("child::item")["result"]["message"] == "ordinary rerun shares run budget"
            else:
                assert list(snap.iter_jobs(status="failed")) == []
                assert snap.get_job("child::item")["result"]["metadata"]["calls"] == 2
            assert list(snap.iter_jobs(status="pending")) == []


def test_zero_budget_retry_skips_gate_without_resetting_ordinary_candidate(tmp_path):
    calls_file = str(tmp_path / "output" / "child_calls.txt")
    with _pipeline(tmp_path, backend="memory") as pipeline:
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
            payload={"calls_file": calls_file},
            trigger="gate",
            max_retries_per_run=0,
        )
        pipeline.enqueue(child)
        pipeline.enqueue(Job(
            "parent",
            "p",
            depends_on=[child.uid],
            payload={
                "calls_file": calls_file,
                "child_trigger": "gate",
                "child_max_retries": 0,
            },
        ))

        with in_process_workers(pipeline):
            assert pipeline.run(install_signals=False) is None
        pipeline.stop()

        assert evaluated == [(1, False), (1, True)]
        with StateSnapshot.open(pipeline) as snap:
            records = list(snap.iter_attempts(job_uid="child::item"))
            assert [r["attempt_no"] for r in records] == [1, 2]
            assert [r["outcome"] for r in records] == ["retry", "success"]
            assert list(snap.iter_jobs(status="failed")) == []
            assert snap.get_job("child::item")["result"]["metadata"]["calls"] == 2


def _run_until_retry_committed(root, ready, release):
    """队列重试已提交的屏障供监督进程精确终止主引擎。"""
    with _pipeline(Path(root)) as pipeline:
        backend = pipeline._runtime.store.backend
        original_commit = backend.commit_job_settlement

        def commit_then_park(**kwargs):
            original_commit(**kwargs)
            if kwargs["outcome"] == "retry":
                queued = [row["job"] for row in backend.load_queue_jobs(status="pending")]
                assert queued[0]["attempt_no"] == 1
                ready.set()
                release.wait(timeout=30)

        backend.commit_job_settlement = commit_then_park
        pipeline.register_trigger("gated", lambda ctx: True)
        pipeline.register_task("retry", always_retry, max_retries_per_run=1, trigger="gated")
        pipeline.enqueue(Job("retry", "item"))
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)


@pytest.mark.parametrize("gate_allows", [False, True])
def test_new_run_rechecks_retry_pending_and_resets_budget(tmp_path, gate_allows):
    context = mp.get_context("spawn")
    ready, release = context.Event(), context.Event()
    engine = context.Process(target=_run_until_retry_committed, args=(str(tmp_path), ready, release))
    engine.start()
    try:
        assert ready.wait(20), "主引擎必须到达已提交重试的屏障"
        engine.kill()
        engine.join(timeout=10)
        assert not engine.is_alive()
        assert engine.exitcode != 0
    finally:
        if engine.is_alive():
            engine.kill()
            engine.join(timeout=10)
        engine.close()

    with _pipeline(tmp_path) as recovered:
        with StateSnapshot.open(recovered) as snap:
            pending = list(snap.iter_jobs(status="pending"))
            assert len(pending) == 1 and pending[0]["attempt_no"] is None
        recovered.register_trigger("gated", lambda ctx: gate_allows)
        handler = retry_again_then_succeed if gate_allows else always_retry
        recovered.register_task("retry", handler, max_retries_per_run=1, trigger="gated")
        with in_process_workers(recovered):
            recovered.run(install_signals=False)
        recovered.stop()
        with StateSnapshot.open(recovered) as snap:
            records = list(snap.iter_attempts(job_uid="retry::item"))
            assert list(snap.iter_jobs(status="pending")) == []
            if gate_allows:
                assert [(r["attempt_no"], r["outcome"]) for r in records] == [(1, "retry"), (2, "retry"), (3, "success")]
                assert len({r["run_id"] for r in records}) == 2
                assert snap.get_job("retry::item")["result"]["metadata"]["resumed_at"] == 3
                assert list(snap.iter_jobs(status="failed")) == []
            else:
                assert [(r["attempt_no"], r["outcome"]) for r in records] == [(1, "retry")]
                assert list(snap.iter_jobs(status="succeeded")) == []
                assert list(snap.iter_jobs(status="failed")) == []


@pytest.mark.parametrize("seed_after_clear", [False, True])
def test_untrusted_success_reexecutes_without_reusing_old_attempt_baseline(tmp_path, seed_after_clear):
    output_dir = str(tmp_path / "output")
    with _pipeline(tmp_path) as first:
        first.register_task("produce", produce_input)
        first.register_task("consume", consume_input, trigger="on_input_change")
        first.enqueue(Job("produce", "source", payload={"output_dir": output_dir}))
        first.enqueue(Job(
            "consume",
            "result",
            depends_on=["produce::source"],
            files={
                "source": FileSpec.from_output("produce::source", "source", role="input"),
            },
        ))
        with in_process_workers(first):
            first.run(install_signals=False)
        with first.edit(dry_run=False) as tx:
            if seed_after_clear:
                tx.replace(
                    "consume::result",
                    Job(
                        "consume",
                        "result",
                        depends_on=["produce::source"],
                        files={
                            "source": FileSpec.from_output("produce::source", "source", role="input"),
                        },
                    ),
                    status="succeeded",
                )
            else:
                tx.remove("consume::result", entries=("successes",), recursive=False)
        first.stop()

    with _pipeline(tmp_path) as second:
        second.register_task("consume", consume_input, trigger="on_input_change")
        second.enqueue(Job(
            "consume",
            "result",
            depends_on=["produce::source"],
            files={
                "source": FileSpec.from_output("produce::source", "source", role="input"),
            },
        ))
        with in_process_workers(second):
            res = second.run(install_signals=False)
        second.stop()
        assert res is None
        assert second._runtime.store.backend.load_successes()["consume::result"]["success_count"] == 1
        with StateSnapshot.open(second) as snap:
            records = list(snap.iter_attempts(job_uid="consume::result"))
            assert [row["attempt_no"] for row in records] == [1, 2]
            assert all(row["outcome"] == "success" for row in records)


def discover_page(job, ctx, page):
    with Path(job.payload["scan_calls"]).open("a", encoding="utf-8") as stream:
        stream.write(f"{page}\n")
    return [{"id": "item"}] if page == 1 else []


def discover_id(item):
    return item["id"]


def spawn_discovered(job, ctx, item, content_id):
    ctx.spawn(Job("child", content_id))


def discovered_child(job, ctx):
    return TaskResult.success(metadata={"ok": True})


def test_discovery_defaults_always_and_job_once_override_across_runs(tmp_path):
    from tasklite.wrappers.discovery import register_discovery

    scan_calls = str(tmp_path / "output" / "scan_calls")
    with _pipeline(tmp_path) as pipeline:
        pipeline.register_task("child", discovered_child)
        register_discovery(pipeline, "discover", discover_page, discover_id, spawn_discovered, "child")
        pipeline.enqueue(Job("discover", "root", payload={"scan_calls": scan_calls}))
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as snap:
            first_children = [r["job_uid"] for r in snap.iter_jobs(task_type="child", status="succeeded")]
            assert len(first_children) == 1
        pipeline.enqueue(Job("discover", "root", payload={"scan_calls": scan_calls}))
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)
        calls = Path(scan_calls).read_text(encoding="utf-8").splitlines()
        assert calls == ["1", "2", "1"]
        assert pipeline._runtime.store.backend.load_successes()["discover::root"]["success_count"] == 2
        pipeline.enqueue(Job("discover", "root", payload={"scan_calls": scan_calls}, trigger="once"))
        with in_process_workers(pipeline):
            res = pipeline.run(install_signals=False)
        pipeline.stop()
        assert res is None
        assert Path(scan_calls).read_text(encoding="utf-8").splitlines() == calls


def _wait_for_file(path, timeout=15):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size:
            return
        time.sleep(0.005)
    raise TimeoutError(f"未到达文件屏障：{path}")


def waiting_for_release(job, ctx):
    out_dir = Path(job.payload["output_dir"])
    (out_dir / "worker_ready").write_text(str(os.getpid()), encoding="utf-8")
    _wait_for_file(out_dir / "worker_release")
    return TaskResult.success(metadata={"pid": os.getpid()})


def test_trigger_exception_aborts_real_worker_and_preserves_recovery_candidates(tmp_path):
    output_dir = str(tmp_path / "output")
    with _pipeline(tmp_path, max_workers=2) as pipeline:
        def explode_after_worker_ready(ctx):
            _wait_for_file(tmp_path / "output" / "worker_ready")
            raise RuntimeError("trigger boundary")

        pipeline.register_trigger("explode", explode_after_worker_ready)
        pipeline.register_task("waiting", waiting_for_release, max_retries_per_run=0, trigger="always")
        pipeline.register_task("later", discovered_child, trigger="explode")
        pipeline.enqueue(Job("waiting", "item", payload={"output_dir": output_dir}))
        pipeline.enqueue(Job("later", "item"))
        with pytest.raises(RuntimeError, match="trigger boundary"):
            pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as snap:
            assert snap.get_job("waiting::item")["status"] == "running"
            assert snap.get_job("waiting::item")["attempt_no"] == 1
            assert snap.get_job("later::item")["status"] == "pending"
            assert list(snap.iter_jobs(status="failed")) == []
            assert list(snap.iter_jobs(status="succeeded")) == []
            assert list(snap.iter_attempts(job_uid="later::item")) == []
        assert not mp.active_children()

    (tmp_path / "output" / "worker_release").write_text("release", encoding="utf-8")
    with _pipeline(tmp_path) as recovered:
        recovered.register_trigger("explode", lambda ctx: False)
        recovered.register_task("waiting", waiting_for_release, max_retries_per_run=0, trigger="always")
        recovered.register_task("later", discovered_child, trigger="explode")
        with in_process_workers(recovered):
            res = recovered.run(install_signals=False)
        recovered.stop()
        assert res is None
        with StateSnapshot.open(recovered) as snap:
            assert {r["job_uid"] for r in snap.iter_jobs(status="failed")} == {"waiting::item"}
            assert list(snap.iter_jobs(status="pending")) == []
            attempts = list(snap.iter_attempts(job_uid="waiting::item"))
            assert len(attempts) == 1
            assert attempts[0]["outcome"] == "failure" and attempts[0]["attempt_no"] == 1


def _record_until_success_call(job, ctx):
    calls = Path(job.payload["calls_file"])
    with calls.open("a", encoding="utf-8") as stream:
        stream.write(f"{job.uid}\n")
    return TaskResult.success(metadata={"calls": len(calls.read_text().splitlines())})


def _fail_until_success(job, ctx):
    _record_until_success_call(job, ctx)
    return TaskResult.failure(message="still failing")


def _spawn_with_unknown_trigger(job, ctx):
    ctx.set_cursor("trigger/spawn", "must-not-publish")
    ctx.spawn(Job("child", "good", payload=job.payload, trigger="allowed"))
    ctx.spawn(Job("child", "bad", payload=job.payload, trigger="missing"))
    return TaskResult.success()


def test_until_success_uses_current_sqlite_terminal_history(tmp_path):
    calls_file = str(tmp_path / "output" / "until_success_calls.txt")
    with _pipeline(tmp_path) as pipeline:
        pipeline.register_task("work", _record_until_success_call, trigger="until_success")

        assert pipeline.enqueue(Job("work", "item", payload={"calls_file": calls_file})) is True
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as snap:
            assert list(snap.iter_attempts(job_uid="work::item"))[0]["outcome"] == "success"

        assert pipeline.enqueue(Job("work", "item", payload={"calls_file": calls_file})) is True
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as snap:
            assert len(list(snap.iter_attempts(job_uid="work::item"))) == 1

        with pipeline.edit(dry_run=False) as tx:
            tx.set_status("work::item", "failed", recursive=False)
        assert pipeline.enqueue(Job("work", "item", payload={"calls_file": calls_file})) is True
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)
        with StateSnapshot.open(pipeline) as snap:
            assert len(list(snap.iter_attempts(job_uid="work::item"))) == 2
        pipeline.stop()


def test_until_success_recognizes_manual_success_without_attempt(tmp_path):
    calls_file = str(tmp_path / "output" / "until_success_calls.txt")
    with _pipeline(tmp_path) as pipeline:
        pipeline.register_task("work", _record_until_success_call, trigger="until_success")
        with pipeline.edit(dry_run=False) as tx:
            tx.add(Job("work", "manual", trigger="until_success"), status="succeeded")

        assert pipeline.enqueue(Job("work", "manual", payload={"calls_file": calls_file})) is True
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)
        pipeline.stop()

        with StateSnapshot.open(pipeline) as snap:
            assert list(snap.iter_attempts(job_uid="work::manual")) == []
            assert snap.get_job("work::manual")["result"]["annotations"]["completion_source"] == "manual"


def test_until_success_retries_after_real_failure(tmp_path):
    calls_file = str(tmp_path / "output" / "until_success_calls.txt")
    with _pipeline(tmp_path) as pipeline:
        pipeline.register_task("work", _fail_until_success, trigger="until_success", max_retries_per_run=0)

        assert pipeline.enqueue(Job("work", "item", payload={"calls_file": calls_file})) is True
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)

        assert pipeline.enqueue(Job("work", "item", payload={"calls_file": calls_file})) is True
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)
        pipeline.stop()

        with StateSnapshot.open(pipeline) as snap:
            assert [r["outcome"] for r in snap.iter_attempts(job_uid="work::item")] == ["failure", "failure"]
        assert Path(calls_file).read_text(encoding="utf-8").splitlines() == ["work::item", "work::item"]


def test_legacy_trigger_pending_is_preserved_and_rejected_on_use(tmp_path):
    with _pipeline(tmp_path) as pipeline:
        legacy = Job("work", "legacy", trigger="on_failure").to_dict()
        pipeline._runtime.store.backend.enqueue_jobs([legacy])
        pipeline.close()

    with _pipeline(tmp_path) as reopened:
        reopened.register_task("work", _record_until_success_call)
        assert [row["job"] for row in reopened._runtime.store.backend.load_queue_jobs(status="pending")] == [legacy]
        with pytest.raises(TaskLiteError, match="unregistered trigger 'on_failure'"):
            reopened.run(install_signals=False)
        reopened.stop()

        assert [row["job"] for row in reopened._runtime.store.backend.load_queue_jobs(status="pending")] == [legacy]
        with StateSnapshot.open(reopened) as snap:
            assert list(snap.iter_attempts(job_uid="work::legacy")) == []
            assert list(snap.iter_jobs(status="failed")) == []


def test_unknown_spawned_trigger_blocks_siblings_and_cursor_publication(tmp_path):
    calls_file = str(tmp_path / "output" / "until_success_calls.txt")
    with _pipeline(tmp_path, backend="memory") as pipeline:
        pipeline.register_trigger("allowed", lambda ctx: True)
        pipeline.register_task("child", _record_until_success_call)
        pipeline.register_task("parent", _spawn_with_unknown_trigger, trigger="always")
        pipeline.enqueue(Job("parent", "root", payload={"calls_file": calls_file}))

        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)
        pipeline.stop()

        with StateSnapshot.open(pipeline) as snap:
            assert snap.get_job("parent::root")["status"] == "failed"
            assert list(snap.iter_jobs(status="succeeded")) == []
            assert list(snap.iter_jobs(status="pending")) == []
            assert list(snap.iter_attempts(job_uid="child::good")) == []
            assert list(snap.iter_attempts(job_uid="child::bad")) == []
        assert pipeline.get_cursor("trigger/spawn") is None
