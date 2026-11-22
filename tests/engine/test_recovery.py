"""遗留 running 启动恢复与异常退出单口径清扫测试。"""

import time

import pytest

from tasklite import TaskLite, TaskLiteError
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.authorization import AuthorizationCoordinator
from tasklite.engine.channel import ExecutionChannel, WorkerLaunchSpec
from tasklite.engine.completion import CompletionMachine
from tasklite.engine.in_flight import InFlightJob, InFlightTracker
from tasklite.engine.launcher import PreparedWorker, WorkerLauncher
from tasklite.engine.recovery import RecoveryOrchestrator
from tasklite.engine.resource import CapacityResource, ResourceManager
from tasklite.engine.session import RunSession
from tasklite.engine.store import StateStore
from tasklite.engine.types import JobHandle
from tasklite.models.artifact import FileSpec
from tasklite.models.context import JobContext
from tasklite.models.job import Job
from tasklite.models.result import TaskResult
from tasklite.models.state import PipelineState
from tasklite.utils.ipc import create_worker_pipe, create_worker_reply_pipe
from tasklite.utils.lockfile import release_lock, try_acquire_lock

_RUN_ID = "deadbeefdeadbeefdeadbeefdeadbeef"
_EXECUTION_ID = f"{_RUN_ID}.1"


def unexpected_worker(job, ctx):
    raise AssertionError("不应启动 worker")


def test_startup_recovers_under_exclusive_lock_and_preserves_pending_order(tmp_path):
    db_path = tmp_path / "state.db"
    backend = SQLiteStateBackend(db_path)
    backend.enqueue_jobs([Job("work", "tail").to_dict(), Job("work", "orphan").to_dict()])
    backend.enqueue_jobs([Job("work", "head").to_dict()], front=True)
    backend.mark_job_running(
        "work::orphan",
        execution_id="old.1",
        run_id="old",
        started_at="2026-01-01T00:00:00Z",
    )
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
            with pytest.raises(TaskLiteError, match="concurrent runs"):
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


class _DoneProcess:
    """已产出结果的进程句柄桩。"""

    def __init__(self, pipe_sender=None, reply_receiver=None):
        self._alive = True
        self.exitcode = 0
        self.killed = False
        self.was_force_killed = False
        self.closed = False
        self.pid = None
        self._pipe_sender = pipe_sender
        self._reply_receiver = reply_receiver

    def _close_pipes(self):
        if self._pipe_sender is not None:
            self._pipe_sender.close()
            self._pipe_sender = None
        if self._reply_receiver is not None:
            self._reply_receiver.close()
            self._reply_receiver = None

    def is_alive(self):
        return self._alive

    def join(self, timeout=None):
        self._alive = False
        self._close_pipes()

    def kill_tree(self):
        self._alive = False
        self.killed = True
        self.was_force_killed = True
        self._close_pipes()

    def kill(self):
        self.kill_tree()

    def close(self):
        self._alive = False
        self.closed = True
        self._close_pipes()


class _FinishOnKillProcess:
    """kill_tree() 时通过真实 Pipe 回传结果的句柄桩（模拟强杀窗口内迟到结果）。"""

    def __init__(self, pipe_sender, reply_receiver):
        self._alive = True
        self.exitcode = 0
        self.killed = False
        self.was_force_killed = False
        self.closed = False
        self.pid = None
        self._pipe_sender = pipe_sender
        self._reply_receiver = reply_receiver

    def is_alive(self):
        return self._alive

    def join(self, timeout=None):
        pass

    def kill_tree(self):
        self._alive = False
        self.killed = True
        self.was_force_killed = True
        if self._pipe_sender is not None:
            self._pipe_sender.send_result(
                {
                    "status": "success",
                    "metadata": {"done": 1},
                    "spawned_jobs": [],
                    "cursor_updates": {},
                }
            )
            self._pipe_sender.close()
            self._pipe_sender = None
        if self._reply_receiver is not None:
            self._reply_receiver.close()
            self._reply_receiver = None

    def kill(self):
        self.kill_tree()

    def close(self):
        self._alive = False
        self.closed = True
        if self._pipe_sender is not None:
            self._pipe_sender.close()
            self._pipe_sender = None
        if self._reply_receiver is not None:
            self._reply_receiver.close()
            self._reply_receiver = None


class _RecoveryStubLauncher(WorkerLauncher):
    """复现管道已有未提交结果与强杀窗口迟到结果的最小 WorkerLauncher。"""

    def __init__(self, *, mode: str):
        self._mode = mode
        self.handles: list[object] = []

    def prepare(self, spec, *, on_message, cancel_event):
        receiver, sender = create_worker_pipe(
            job_uid=spec.job.uid,
            execution_id=spec.execution_id,
            run_id=spec.run_id or None,
            attempt_no=spec.attempt_no,
            on_message=on_message,
        )
        reply_receiver, reply_sender = create_worker_reply_pipe(
            job_uid=spec.job.uid,
            execution_id=spec.execution_id,
            run_id=spec.run_id or None,
            attempt_no=spec.attempt_no,
        )
        if self._mode == "done":
            handle = _DoneProcess(pipe_sender=sender, reply_receiver=reply_receiver)

            def _release():
                sender.send_result(
                    {
                        "status": "success",
                        "metadata": {},
                        "spawned_jobs": [],
                        "cursor_updates": {},
                    }
                )
        else:
            handle = _FinishOnKillProcess(pipe_sender=sender, reply_receiver=reply_receiver)

            def _release():
                pass

        self.handles.append(handle)

        def _abort():
            handle.kill_tree()
            handle.close()
            reply_sender.close()
            receiver.close(timeout=2.0)

        return PreparedWorker(
            handle=handle,
            receiver=receiver,
            reply_sender=reply_sender,
            on_release=_release,
            on_abort=_abort,
        )


def _make_launch_spec(job: Job) -> WorkerLaunchSpec:
    return WorkerLaunchSpec(
        handler=lambda j, c: TaskResult.success(),
        job=job,
        job_context=JobContext(job, frozenset(), frozenset(), {}),
        execution_id=_EXECUTION_ID,
        timeout_seconds=60.0,
        run_id=_RUN_ID,
        attempt_no=1,
    )


class TestAbortInFlight:
    """对接真实 CompletionMachine / StateStore 验证异常中止不补提交与遗留 running 恢复。"""

    def _make(self, *, resource_mgr=None, launcher=None):
        backend = InMemoryStateBackend()
        session = RunSession()
        session.begin(_RUN_ID)
        store = StateStore(backend, PipelineState({}, {}, {}, []), session=session)
        channel = ExecutionChannel(launcher=launcher)
        resource_mgr = (
            resource_mgr
            if resource_mgr is not None
            else ResourceManager({"api": CapacityResource("api", max_capacity=1.0)})
        )
        in_flight = InFlightTracker()
        authorization = AuthorizationCoordinator(
            store=store,
            channel=channel,
            in_flight=in_flight,
            session=session,
        )
        completion = CompletionMachine(
            store=store,
            channel=channel,
            resources=resource_mgr,
            in_flight=in_flight,
            session=session,
            authorization=authorization,
        )
        orchestrator = RecoveryOrchestrator(
            store=store,
            channel=channel,
            in_flight=in_flight,
            completion=completion,
        )
        return orchestrator, store, channel, authorization, resource_mgr

    @staticmethod
    def _mark_running(store: StateStore, job: Job) -> tuple[dict, int]:
        snapshot = job.to_dict()
        store.backend.enqueue_jobs([snapshot])
        attempt_no = store.backend.mark_job_running(
            job.uid,
            execution_id=_EXECUTION_ID,
            run_id=_RUN_ID,
            started_at="2026-01-01T00:00:00Z",
        )
        return snapshot, attempt_no

    @pytest.mark.parametrize("mode", ["done", "finish_on_kill"])
    def test_abort_does_not_commit_unsettled_result_and_leaves_running_for_recovery(
        self, tmp_path, mode
    ):
        """异常中止不补提交已到达或强杀窗口内到达的未结算结果，下次启动按原身份恢复失败。"""
        launcher = _RecoveryStubLauncher(mode=mode)
        orchestrator, store, channel, authorization, resource_mgr = self._make(launcher=launcher)
        job = Job("fast", f"job_{mode}")
        snapshot, attempt_no = self._mark_running(store, job)
        out_file = tmp_path / "out" / "result.txt"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("artifact", encoding="utf-8")
        active_files = {
            "out": FileSpec(out_file, role="output", delete_on_failure=True),
        }
        capacity = resource_mgr["api"]
        capacity.acquire(1.0)
        try:
            handle = channel.spawn(_make_launch_spec(job))
            process = launcher.handles[0]
            if mode == "done":
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    channel._receive_available(handle)
                    if channel.has_received_result(_EXECUTION_ID):
                        break
                    time.sleep(0.005)
                assert channel.has_received_result(_EXECUTION_ID)

            orchestrator._in_flight.track(
                InFlightJob(
                    job_uid=job.uid,
                    job_snapshot=snapshot,
                    job=job,
                    acquired=[("api", 1.0)],
                    handle=handle,
                    execution_id=_EXECUTION_ID,
                    attempt_no=attempt_no,
                    run_id=_RUN_ID,
                    active_files=active_files,
                ),
                state=store.state,
            )

            orchestrator.abort_in_flight()

            # 1. 绝不补提交结果，也不越权清理残留文件
            assert store.backend.load_successes() == {}
            assert store.backend.load_failed() == {}
            assert store.backend.load_settled_attempts(job.uid) == []
            assert out_file.exists()

            # 2. 通道/进程/内存追踪/资源均已完成单口径清理，零双重释放
            assert process.closed is True
            assert not channel.has_active_workers
            assert not orchestrator._in_flight
            assert store.state.in_flight_uids == frozenset()
            assert not authorization.has_pending_authorization
            assert capacity.used == 0.0
            assert capacity.release_overruns == 0

            # 3. 持久化队列保留 running，下次启动恢复为按原执行身份记录的失败 Attempt
            (running_row,) = store.backend.load_queue_jobs(status="running")
            assert running_row["job_uid"] == job.uid
            assert running_row["execution_id"] == _EXECUTION_ID

            recovered_state = orchestrator.load_and_recover()
            assert job.uid in recovered_state.failed
            assert job.uid not in recovered_state.successes
            assert store.backend.load_queue_jobs() == []
            (attempt,) = store.backend.load_settled_attempts(job.uid)
            assert attempt["outcome"] == "failure"
            assert attempt["run_id"] == _RUN_ID
            assert attempt["attempt_no"] == attempt_no
        finally:
            channel.close()

    def test_abort_releases_resources_once_without_double_release(self):
        """异常中止仅经完成机器释放一次资源，不发生重复归还。"""
        orchestrator, store, channel, _authorization, resource_mgr = self._make()
        job = Job("fast", "release_job")
        snapshot, attempt_no = self._mark_running(store, job)
        capacity = resource_mgr["api"]
        capacity.acquire(1.0)
        process = _DoneProcess()
        handle = JobHandle(
            job_uid=job.uid,
            process=process,
            timeout_at_monotonic=time.monotonic() + 60,
            timeout_seconds=60.0,
            job=job,
            execution_id=_EXECUTION_ID,
        )
        orchestrator._in_flight.track(
            InFlightJob(
                job_uid=job.uid,
                job_snapshot=snapshot,
                job=job,
                acquired=[("api", 1.0)],
                handle=handle,
                execution_id=_EXECUTION_ID,
                attempt_no=attempt_no,
                run_id=_RUN_ID,
            ),
            state=store.state,
        )

        orchestrator.abort_in_flight()

        assert capacity.used == 0.0
        assert capacity.release_overruns == 0
        assert not orchestrator._in_flight
        assert store.state.in_flight_uids == frozenset()
        assert process.closed is True

    def test_abort_with_empty_in_flight_is_safe_and_clears_authorization(self):
        orchestrator, store, _channel, authorization, _resource_mgr = self._make()
        authorization._dispatch_pending = object()  # type: ignore[assignment]

        orchestrator.abort_in_flight()

        assert not authorization.has_pending_authorization
        assert store.backend.load_failed() == {}
        assert store.backend.load_successes() == {}
