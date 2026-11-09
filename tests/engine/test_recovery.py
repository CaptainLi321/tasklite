"""异常退出的在途回收与结果分类测试。"""

import time
from unittest.mock import MagicMock

import pytest

from tasklite import TaskLite
from tasklite.backend.sqlite_backend import SQLiteStateBackend, TARGET_SCHEMA_VERSION
from tasklite.engine.channel import ExecutionChannel
from tasklite.engine.in_flight import InFlightJob, InFlightTracker
from tasklite.engine.recovery import RecoveryOrchestrator
from tasklite.engine.resource import (
    CapacityResource,
    ResourceManager,
)
from tasklite.engine.types import JobHandle
from tasklite.models.job import Job
from tasklite.utils.lockfile import release_lock, try_acquire_lock

_INCARNATION = "deadbeefdeadbeefdeadbeefdeadbeef.1"


def _write_artifact(journal, uid, path, *, delete_on_failure=False, delete_on_success=False):
    """注入产物声明到当前进程日志。"""
    from tasklite.models.artifact import ArtifactDeclaration

    prev = journal._artifacts.get(uid, ())
    journal._artifacts[uid] = (
        *prev,
        ArtifactDeclaration(
            path,
            delete_on_failure=delete_on_failure,
            delete_on_success=delete_on_success,
        ),
    )


def unexpected_worker(job, ctx):
    raise AssertionError("不应启动 worker")


def test_startup_recovers_under_exclusive_lock_and_preserves_pending_order(tmp_path):
    db_path = tmp_path / "state.db"
    backend = SQLiteStateBackend(db_path, schema_version=TARGET_SCHEMA_VERSION)
    backend.enqueue_jobs([Job("work", "tail").to_dict(), Job("work", "orphan").to_dict()])
    backend.enqueue_jobs([Job("work", "head").to_dict()], front=True)
    backend.mark_job_running("work::orphan", run_id="old", started_at="2026-01-01T00:00:00Z")
    before = backend.load_queue_jobs()
    seen = []

    def inspect_pending(ctx):
        seen.append(ctx.job.job_id)
        return False

    with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
        pipeline.register_trigger("inspect", inspect_pending)
        pipeline.register_task("work", unexpected_worker, trigger="inspect")
        lock = try_acquire_lock(db_path)
        assert lock is not None
        try:
            with pytest.raises(RuntimeError, match="concurrent runs"):
                pipeline.run()
            assert backend.load_queue_jobs() == before
            assert backend.load_settled_attempts("work::orphan") == []
        finally:
            release_lock(lock)
        pipeline.run()

    assert seen == ["head", "tail"]
    assert backend.load_queue_jobs() == []
    assert backend.load_failed().keys() == {"work::orphan"}
    assert backend.load_settled_attempts("work::head") == []
    assert backend.load_settled_attempts("work::tail") == []


class RecordingCompletion:
    """完成机器窄契约替身：记录结算调用供断言。"""

    def __init__(self):
        self.settled = []

    def settle_aborted(self, cancelled_entries, done_entries):
        self.settled.append((list(cancelled_entries), list(done_entries)))


class _DoneProcess:
    """已完成 entry 的进程桩——abort 不得 kill 它（结果已到达）。"""

    def __init__(self):
        self._alive = True
        self.exitcode = 0
        self.killed = False

    def is_alive(self):
        return self._alive

    def join(self, timeout=None):
        self._alive = False

    def kill(self):
        self._alive = False
        self.killed = True

    def close(self):
        pass


class _FinishOnKillProcess:
    """kill() 时通过通道回传最终结果 + suspend 信号的进程桩（TOCTOU 窗口复现）。"""

    def __init__(self, channel, uid, execution_id):
        self._alive = True
        self.exitcode = 0
        self.killed = False
        self._channel = channel
        self._uid = uid
        self._execution_id = execution_id

    def is_alive(self):
        return self._alive

    def join(self, timeout=None):
        pass

    def kill(self):
        self._alive = False
        self.killed = True
        self._channel._results[self._execution_id] = {
            "status": "success",
            "raw_result": {"done": 1},
        }
        self._channel._pending_signals.append((self._uid, "api", 5.0))

    def close(self):
        pass


class TestAbortInFlight:
    def _make(self, tmp_path, *, resource_mgr=None):
        ipc_dir = tmp_path / "ipc"
        ipc_dir.mkdir(parents=True, exist_ok=True)
        channel = ExecutionChannel(ipc_dir)
        resource_mgr = resource_mgr if resource_mgr is not None else ResourceManager(
            {"api": CapacityResource("api", max_capacity=1.0)}
        )
        completion = RecordingCompletion()
        orchestrator = RecoveryOrchestrator(
            store=MagicMock(), channel=channel, resources=resource_mgr,
            in_flight=InFlightTracker(),
            completion=completion,
        )
        return orchestrator, channel, completion, resource_mgr

    def test_abort_consumes_completed_result(self, tmp_path):
        """已完成未收割的 entry：结果被消费提交（进 done），不 kill、不取消。"""
        orchestrator, channel, completion, _ = self._make(tmp_path)
        job = Job("fast", "done_job")
        out_file = tmp_path / "out" / "result.txt"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("done")
        _write_artifact(channel.journal, job.uid, str(out_file), delete_on_failure=True)
        channel._results[_INCARNATION] = {
            "status": "success", "raw_result": True,
            "spawned_jobs": [], "resource_suspensions": [], "cursor_updates": {},
        }
        process = _DoneProcess()
        handle = JobHandle(
            job_uid=job.uid, process=process,
            timeout_at_monotonic=time.monotonic() + 60, timeout_seconds=60.0, job=job,
            ipc_dir=str(tmp_path / "ipc"), execution_id=_INCARNATION,
        )
        tracker = orchestrator._in_flight
        tracker.track(InFlightJob(job.uid, job.to_dict(), job, [], handle, None))

        orchestrator.abort_in_flight()

        assert process.killed is False, "已完成 entry 不得被 kill"
        cancelled, done = completion.settled[0]
        assert [e.job_uid for e, _ in done] == [job.uid], "已完成结果必须交完成机器提交"
        assert cancelled == []
        assert out_file.exists(), "成功产出的物理输出必须保留"

    def test_abort_toctou_result_appears_during_kill(self, tmp_path):
        """分类时无结果 → kill 窗口内 worker 写完结果 → 重探测移入 done 消费。"""
        orchestrator, channel, completion, _ = self._make(tmp_path)
        job = Job("fast", "toctou")
        process = _FinishOnKillProcess(channel, job.uid, _INCARNATION)
        handle = JobHandle(
            job_uid=job.uid, process=process,
            timeout_at_monotonic=time.monotonic() + 60, timeout_seconds=60.0, job=job,
            ipc_dir=str(tmp_path), execution_id=_INCARNATION,
        )
        tracker = orchestrator._in_flight
        tracker.track(InFlightJob(job.uid, job.to_dict(), job, [], handle, None))

        orchestrator.abort_in_flight()

        cancelled, done = completion.settled[0]
        assert [e.job_uid for e, _ in done] == [job.uid], (
            "kill 窗口内到达的结果必须被重探测消费（不删结果、不取消）"
        )
        assert cancelled == []
        assert done[0][1].resource_suspensions == [("api", 5.0)], (
            "终前写入的挂起信号并入结果（随完成提交链应用）"
        )

    def test_abort_releases_resources_before_settlement(self, tmp_path):
        """已占用资源在结算前统一释放（防泄漏）。"""
        orchestrator, channel, completion, resource_mgr = self._make(tmp_path)
        job = Job("fast", "release_job")
        capacity = resource_mgr["api"]
        capacity.acquire(1.0)
        process = _DoneProcess()
        handle = JobHandle(
            job_uid=job.uid, process=process,
            timeout_at_monotonic=time.monotonic() + 60, timeout_seconds=60.0, job=job,
            ipc_dir=str(tmp_path), execution_id=_INCARNATION,
        )
        tracker = orchestrator._in_flight
        tracker.track(InFlightJob(
            job.uid, job.to_dict(), job, [("api", 1.0)], handle, None,
        ))

        orchestrator.abort_in_flight()

        assert capacity.used == 0.0, "abort 必须释放在途占用的资源"

    def test_abort_with_empty_in_flight_is_noop(self, tmp_path):
        orchestrator, channel, completion, _ = self._make(tmp_path)

        orchestrator.abort_in_flight()

        assert completion.settled == []
