"""v2 TaskLite 门面回归：六步调用契约、注册面、入队与管理面委托。

装配分区行为等价验证：构造器拆分为 backend / 分类器 / 资源 / IPC /
RunConfig / 运维控制台各私有装配方法后，对外契约（校验、快照、守卫、
委托）与 v1 门面逐一对照。需要确定性的 run 路径显式安装进程替身，并在
真实单向 Pipe 边界发送结果。
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tasklite import TaskLiteError

from tasklite.backend.base import AbstractStateBackend
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import (
    ImmediateRequeuePolicy,
    RequeuePlan,
    RequeuePolicy,
)
from tasklite.resource import CapacityResource, IntervalResource
from tasklite.engine.scheduler import FifoOrderingPolicy, OrderingPolicy
from tasklite.engine.types import StopMode
from tasklite.models.job import Job
from tasklite.models.result import TaskResult
from tasklite.models.task import Task
from tasklite.pipeline import TaskLite
from tasklite.testing import running
from tests.helpers import install_fake_process_context


def ok_handler(job, ctx):
    """占位 handler：成功 + 空 meta（模块级可 pickle，满足 strict 预检）。"""
    return TaskResult.success()


class BizError(Exception):
    """业务瞬态异常样本（模块级可 pickle——注册表快照随 ctx 下发子进程）。"""


class OtherBizError(Exception):
    """第二类业务瞬态异常样本（批量注册用，同为模块级可 pickle）。"""


class NewestFirstPolicy(OrderingPolicy):
    """队尾优先访问序（门面注入用样本：反转 FIFO 出队顺序）。"""

    def visit_order(self, queue):
        return range(len(queue) - 1, -1, -1)


class TailRequeuePolicy(RequeuePolicy):
    """恒队尾重入队策略（门面注入用样本：front 恒 False、零延迟）。"""

    def plan_requeue(self, job_snapshot) -> RequeuePlan:
        return RequeuePlan(front=False, delay_seconds=0.0)


class DelegatingFakeBackend(AbstractStateBackend):
    """显式契约子类假后端：全部契约方法逐一转发内部真实后端。

    伪造后端的标准形态——契约缺口在类构造期即暴露（ABC 强制对齐），
    不依赖任何鸭子接受分支。
    """

    def __init__(self) -> None:
        self.inner = InMemoryStateBackend()

    def load_successes(self, *args, **kwargs):
        return self.inner.load_successes(*args, **kwargs)

    def load_failed(self, *args, **kwargs):
        return self.inner.load_failed(*args, **kwargs)

    def load_failure_payloads(self, *args, **kwargs):
        return self.inner.load_failure_payloads(*args, **kwargs)

    def load_cursors(self, *args, **kwargs):
        return self.inner.load_cursors(*args, **kwargs)

    def load_queue(self, *args, **kwargs):
        return self.inner.load_queue(*args, **kwargs)

    def load_known_job_snapshot(self, *args, **kwargs):
        return self.inner.load_known_job_snapshot(*args, **kwargs)

    def load_known_job_snapshots(self, *args, **kwargs):
        return self.inner.load_known_job_snapshots(*args, **kwargs)

    def save_queue(self, *args, **kwargs):
        return self.inner.save_queue(*args, **kwargs)

    def replace_queue_atomic(self, *args, **kwargs):
        return self.inner.replace_queue_atomic(*args, **kwargs)

    def commit_job_failure(self, *args, **kwargs):
        return self.inner.commit_job_failure(*args, **kwargs)

    def commit_job_failures(self, *args, **kwargs):
        return self.inner.commit_job_failures(*args, **kwargs)

    def commit_skip(self, *args, **kwargs):
        return self.inner.commit_skip(*args, **kwargs)

    def enqueue_jobs(self, *args, **kwargs):
        return self.inner.enqueue_jobs(*args, **kwargs)

    def delete_queue_uids(self, *args, **kwargs):
        return self.inner.delete_queue_uids(*args, **kwargs)

    def delete_failed(self, *args, **kwargs):
        return self.inner.delete_failed(*args, **kwargs)

    def delete_successes(self, *args, **kwargs):
        return self.inner.delete_successes(*args, **kwargs)

    def apply_manual_status(self, *args, **kwargs):
        return self.inner.apply_manual_status(*args, **kwargs)

    def clear_history(self, *args, **kwargs):
        return self.inner.clear_history(*args, **kwargs)

    def get_cursor(self, *args, **kwargs):
        return self.inner.get_cursor(*args, **kwargs)

    def set_cursor(self, *args, **kwargs):
        return self.inner.set_cursor(*args, **kwargs)

    def load_latest_succeeded_finished_at(self, *args, **kwargs):
        return self.inner.load_latest_succeeded_finished_at(*args, **kwargs)

    def get_engine_metadata(self, *args, **kwargs):
        return self.inner.get_engine_metadata(*args, **kwargs)

    def set_engine_metadata(self, *args, **kwargs):
        return self.inner.set_engine_metadata(*args, **kwargs)

    def mark_job_running(self, *args, **kwargs):
        return self.inner.mark_job_running(*args, **kwargs)

    def commit_job_settlement(self, *args, **kwargs):
        return self.inner.commit_job_settlement(*args, **kwargs)

    def get_attempt_sequence(self, *args, **kwargs):
        return self.inner.get_attempt_sequence(*args, **kwargs)

    def load_settled_attempts(self, *args, **kwargs):
        return self.inner.load_settled_attempts(*args, **kwargs)

    def clear_attempts(self, *args, **kwargs):
        return self.inner.clear_attempts(*args, **kwargs)

    def load_queue_jobs(self, *args, **kwargs):
        return self.inner.load_queue_jobs(*args, **kwargs)

    def record_authorization(self, *args, **kwargs):
        return self.inner.record_authorization(*args, **kwargs)

    def list_authorizations(self, *args, **kwargs):
        return self.inner.list_authorizations(*args, **kwargs)

    def is_path_authorized(self, *args, **kwargs):
        return self.inner.is_path_authorized(*args, **kwargs)

    def revoke_authorization(self, *args, **kwargs):
        return self.inner.revoke_authorization(*args, **kwargs)


# ── 显式进程替身 ──────────────────────────────────────────────────────


def make_instant_process_class(retry_uid: str | None = None, recorder: list | None = None):
    """FakeProcess：start() 经 Pipe 发送结果并退出。

    ``retry_uid`` 命中的 job 写 retry 结果（走失败重试收尾路径），其余
    写成功结果——重入队位置类用例经此驱动。
    """

    class InstantProcess:
        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self.args = args
            self._alive = False
            self.exitcode = 0

        def start(self):
            if self.args:
                spec = self.args[0]
                if recorder is not None:
                    recorder.append(spec.job.uid)
                if retry_uid is not None and spec.job.uid == retry_uid:
                    result = {
                        "status": "retry",
                        "message": "stub failure",
                        "metadata": {},
                        "consume_budget": True,
                    }
                else:
                    result = {
                        "status": "success",
                        "metadata": {},
                        "spawned_jobs": [],
                        "cursor_updates": {},
                    }
                assert spec.pipe_sender is not None
                spec.pipe_sender.send_result(result)
            self._alive = False

        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

        def kill(self):
            self.exitcode = -9

        def close(self):
            pass

    return InstantProcess


def make_pipeline(tmp_path=None, **overrides) -> TaskLite:
    """标准测试管线（memory 后端零 IO）。"""
    overrides.pop("name", None)
    overrides.pop("state_dir", None)
    backend = overrides.pop("backend", "memory")
    state_path = overrides.pop("state_path", None)
    if backend == "sqlite" and state_path is None:
        state_path = (tmp_path / "state.db") if tmp_path else Path("./state.db")
    return TaskLite(
        backend=backend,
        state_path=state_path,
        **overrides,
    )


# ── 构造与装配分区 ────────────────────────────────────────────────────


class TestConstruction:
    """构造参数、目录、后端、生命周期与互斥契约。"""

    def test_sqlite_requires_state_path(self, tmp_path):
        with pytest.raises(TaskLiteError, match="state_path"):
            TaskLite(backend="sqlite")
        with pytest.raises(TaskLiteError, match="state_path"):
            TaskLite(backend="sqlite", state_path="")

    def test_memory_requires_no_state_path_and_no_disk_side_effects(self, tmp_path):
        p = TaskLite(backend="memory")
        assert p.state_path is None
        assert p._backend_type == "memory"

    def test_output_root_resolved_without_creating_dir(self, tmp_path):
        root = tmp_path / "out_not_created"
        p = make_pipeline(tmp_path, output_roots=root)
        assert p.output_roots == root.resolve()
        assert not root.exists()

    def test_output_root_multi_root_resolved_without_creating_dir(self, tmp_path):
        roots = [tmp_path / "o1", tmp_path / "o2"]
        p = make_pipeline(tmp_path, output_roots=roots)
        assert p.output_roots == [r.resolve() for r in roots]
        assert not roots[0].exists()
        assert not roots[1].exists()

    def test_output_root_bad_type_rejected(self, tmp_path):
        with pytest.raises(TaskLiteError, match="output_roots"):
            make_pipeline(tmp_path, output_roots=42)

    def test_backend_sqlite_type_name(self, tmp_path):
        p = TaskLite(backend="sqlite", state_path=tmp_path / "test.db")
        assert p._backend_type == "sqlite"

    def test_backend_memory_type_name(self, tmp_path):
        p = make_pipeline(tmp_path)
        assert p._backend_type == "memory"

    def test_backend_unknown_string_rejected(self, tmp_path):
        with pytest.raises(TaskLiteError, match="Unknown backend"):
            make_pipeline(tmp_path, backend="redis")

    def test_backend_bad_type_rejected(self, tmp_path):
        with pytest.raises(TaskLiteError, match="backend must be"):
            make_pipeline(tmp_path, backend=object())

    def test_backend_instance_rejected(self, tmp_path):
        with pytest.raises(TaskLiteError, match="backend must be"):
            TaskLite(backend=InMemoryStateBackend())  # type: ignore[arg-type]

    def test_backend_object_is_not_public(self, tmp_path):
        p = make_pipeline(tmp_path)
        assert not hasattr(TaskLite, "backend")
        with pytest.raises(AttributeError):
            getattr(p, "backend")

    def test_sqlite_init_failure_wraps_runtime_error(
        self, tmp_path, monkeypatch
    ):
        import tasklite.pipeline as pipeline_mod

        def boom(_path, **kw):
            raise OSError("disk full")

        monkeypatch.setattr(pipeline_mod, "SQLiteStateBackend", boom)
        with pytest.raises(OSError, match="disk full"):
            TaskLite(backend="sqlite", state_path=tmp_path / "test.db")

    def test_init_failure_after_backend_creation_closes_owned_backend(
        self, tmp_path, monkeypatch
    ):
        import tasklite.pipeline as pipeline_mod

        backend = InMemoryStateBackend()
        closed: list[bool] = []
        monkeypatch.setattr(backend, "close", lambda: closed.append(True))
        monkeypatch.setattr(
            pipeline_mod,
            "InMemoryStateBackend",
            lambda **_kwargs: backend,
        )

        failure = OSError("runtime wiring failed")

        def fail_runtime(**_kwargs):
            raise failure

        monkeypatch.setattr(pipeline_mod, "EngineRuntime", fail_runtime)
        with pytest.raises(OSError) as caught:
            TaskLite(backend="memory")
        assert caught.value is failure
        assert closed == [True]

    def test_close_releases_owned_backend_once(self, tmp_path, monkeypatch):
        p = make_pipeline(tmp_path)
        backend = p._snapshot_backend()
        closed: list[bool] = []
        monkeypatch.setattr(backend, "close", lambda: closed.append(True))

        p.close()
        p.close()

        assert closed == [True]

    def test_init_failure_after_backend_creation_preserves_existing_state(
        self, tmp_path, monkeypatch
    ):
        import tasklite.pipeline as pipeline_mod

        db = tmp_path / "existing.db"
        with TaskLite(backend="sqlite", state_path=db) as first:
            first.register_task("work", ok_handler)
            first.enqueue(Job("work", "kept"))

        def fail_runtime(**_kwargs):
            raise OSError("runtime wiring failed")

        monkeypatch.setattr(pipeline_mod, "EngineRuntime", fail_runtime)
        with pytest.raises(OSError, match="runtime wiring failed"):
            TaskLite(backend="sqlite", state_path=db)

        from tasklite.snapshot import StateSnapshot

        with StateSnapshot.open(db) as snapshot:
            record = snapshot.get_job("work::kept")
            assert record is not None
            assert record["status"] == "pending"

    def test_max_workers_validation(self, tmp_path):
        for bad in (0, -1, True, "4", 2.5):
            with pytest.raises(TaskLiteError, match="max_workers"):
                make_pipeline(tmp_path, max_workers=bad)

    def test_worker_resource_registered_with_capacity(self, tmp_path):
        p = make_pipeline(tmp_path, max_workers=7)
        assert "__workers__" in p._resources
        assert p._resources["__workers__"].max_capacity == 7.0

    def test_removed_constructor_params_rejected(self, tmp_path):
        for bad_kwarg in (
            "name",
            "state_dir",
            "strict_picklable",
            "ordering_policy",
            "requeue_policy",
            "dependency_grace_seconds",
            "commit_failure_threshold",
            "deadlock_gap_max_rounds",
        ):
            with pytest.raises(TypeError):
                TaskLite(backend="memory", **{bad_kwarg: "val"})

    def test_close_and_context_manager_lifecycle(self, tmp_path):
        with TaskLite(backend="memory") as p:
            p.register_task("fast", ok_handler)
            p.enqueue(Job("fast", "j1"))
            assert not p._closed
        assert p._closed
        # close 幂等
        p.close()
        # 关闭后受限调用明确拒绝
        with pytest.raises(TaskLiteError, match="closed"):
            p.enqueue(Job("fast", "j2"))
        with pytest.raises(TaskLiteError, match="closed"):
            p.run()
        with pytest.raises(TaskLiteError, match="closed"):
            p.register_task("fast2", ok_handler)
        with pytest.raises(TaskLiteError, match="closed"):
            p.register_resource(CapacityResource("res", max_capacity=1.0))
        with pytest.raises(TaskLiteError, match="closed"):
            p.set_job_status("fast::j1", "succeeded", recursive=False)
        with pytest.raises(TaskLiteError, match="closed"):
            p.clear_history("fast::j1", records=["successes"])
        with pytest.raises(TaskLiteError, match="closed"):
            p.set_cursor("key", "val")
        with pytest.raises(TaskLiteError, match="closed"):
            p.get_cursor("key")
        with pytest.raises(TaskLiteError, match="closed"):
            p.stop()
        with pytest.raises(TaskLiteError, match="closed"):
            p.__enter__()

    def test_close_during_run_rejected(self, tmp_path):
        p = TaskLite(backend="memory")
        p.register_task("fast", ok_handler)
        p.enqueue(Job("fast", "j1"))
        with running(p):
            with pytest.raises(TaskLiteError, match="close.*run"):
                p.close()

    def test_state_snapshot_independent_lifecycle(self, tmp_path):
        from tasklite.snapshot import StateSnapshot

        db = tmp_path / "snap_test.db"
        p = TaskLite(backend="sqlite", state_path=db)
        p.register_task("t", ok_handler)
        p.enqueue(Job("t", "j1"))
        snap = StateSnapshot.open(p)
        p.close()
        # snap 独立于已关闭的实例，仍可正常查询
        assert snap.get_job("t::j1")["status"] == "pending"
        snap.close()
        # 在已关闭实例上打开快照被拒绝
        with pytest.raises(TaskLiteError, match="closed"):
            StateSnapshot.open(p)
        # 通过路径仍可正常打开
        with StateSnapshot.open(db) as snap2:
            assert snap2.get_job("t::j1")["status"] == "pending"

    def test_init_failure_leaves_existing_state_intact(self, tmp_path):
        db = tmp_path / "precious.db"
        db.write_text("precious data")
        with pytest.raises(Exception):
            TaskLite(backend="sqlite", state_path=db)
        assert db.read_text() == "precious data"

    def test_sqlite_and_path_snapshot_parity(self, tmp_path):
        from tasklite.snapshot import StateSnapshot

        db = tmp_path / "parity.db"
        p = TaskLite(backend="sqlite", state_path=db)
        p.register_task("t", ok_handler)
        p.enqueue(Job("t", "j1"))
        with StateSnapshot.open(p) as s1, StateSnapshot.open(db) as s2:
            assert s1.get_job("t::j1") == s2.get_job("t::j1")
        p.close()

    def test_mutual_exclusion_between_instances_and_management(self, tmp_path):
        from tasklite.snapshot import StateSnapshot
        from tasklite.utils.lockfile import release_lock, try_acquire_lock

        db = tmp_path / "mutex.db"
        p1 = TaskLite(backend="sqlite", state_path=db)
        p2 = TaskLite(backend="sqlite", state_path=db)
        p1.register_task("t", ok_handler)
        p2.register_task("t", ok_handler)
        p1.enqueue(Job("t", "j1"))
        lock_fd = try_acquire_lock(p1.state_path, timeout=0)
        assert lock_fd is not None
        try:
            with pytest.raises(TaskLiteError, match="Another run.*in progress"):
                p2.run()
            with pytest.raises(TaskLiteError, match="Another run.*in progress"):
                p2.set_job_status("t::j1", "succeeded", recursive=False)
            with pytest.raises(TaskLiteError, match="Another run.*in progress"):
                p2.enqueue(Job("t", "j2"))
            # 只读观察不受阻
            with StateSnapshot.open(db) as snap:
                assert snap.get_job("t::j1") is not None
        finally:
            release_lock(lock_fd)
        p1.close()
        p2.close()


# ── 注册面（六步之二/三）─────────────────────────────────────────────


class TestRegisterTask:
    """register_task 规格式/简式双形态与 TaskRegistry 契约。"""

    def test_register_task_spec_instance(self, tmp_path):
        p = make_pipeline(tmp_path)
        spec = Task("fetch", ok_handler, resources={"api": 1.0})
        p.register_task(spec)
        assert p._tasks.lookup("fetch") is spec

    def test_register_task_simple_form_builds_spec(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task(
            "fetch",
            ok_handler,
            resources={"api": 1.0},
            max_retries_per_run=1,
            timeout_seconds=30,
            trigger="always",
        )
        spec = p._tasks.lookup("fetch")
        assert spec.handler is ok_handler
        assert spec.resources == {"api": 1.0}
        assert spec.max_retries_per_run == 1
        assert spec.timeout_seconds == 30
        assert spec.trigger == "always"

    def test_simple_form_config_is_keyword_only(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError):
            p.register_task("fetch", ok_handler, {"api": 1.0})  # type: ignore[misc]

    def test_simple_form_requires_handler(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TaskLiteError, match="handler is required"):
            p.register_task("fetch")

    def test_simple_form_validates_task_type(self, tmp_path):
        from tasklite import TaskLiteError
        p = make_pipeline(tmp_path)
        with pytest.raises(TaskLiteError, match="non-empty str"):
            p.register_task("", ok_handler)
        p.register_task("fe::tch", ok_handler)
        assert p._tasks.lookup("fe::tch").task_type == "fe::tch"

    def test_spec_form_rejects_stray_arguments(self, tmp_path):
        p = make_pipeline(tmp_path)
        for handler in (ok_handler, None):
            with pytest.raises(TaskLiteError, match="extra spec"):
                p.register_task(Task("fetch", ok_handler), handler=handler)
        with pytest.raises(TaskLiteError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), resources={"a": 1})
        with pytest.raises(TaskLiteError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), max_retries_per_run=0)
        with pytest.raises(TaskLiteError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), max_retries_per_run=3)
        with pytest.raises(TaskLiteError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), timeout_seconds=60)
        with pytest.raises(TaskLiteError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), trigger="once")
        with pytest.raises(TypeError):
            p.register_task("fetch", ok_handler, payload_schema=dict)  # type: ignore[call-arg]

    def test_duplicate_registration_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        with pytest.raises(TaskLiteError, match="already registered"):
            p.register_task("fetch", ok_handler)

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(TaskLiteError, match="register_task"):
                p.register_task("fetch", ok_handler)


class TestRegisterResource:
    """register_resource：类型校验、同名拒绝、worker 并发不可覆盖。"""

    def test_registers_into_manager(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_resource(IntervalResource("api", interval_seconds=0.5))
        assert "api" in p._resources

    def test_rejects_non_resource(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TaskLiteError, match="Resource instance"):
            p.register_resource("api")

    def test_duplicate_registration_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_resource(CapacityResource("gpu", max_capacity=1.0))
        with pytest.raises(TaskLiteError, match="already registered"):
            p.register_resource(CapacityResource("gpu", max_capacity=2.0))

    def test_override_workers_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TaskLiteError, match="internal worker resource"):
            p.register_resource(CapacityResource("__workers__", max_capacity=9.0))

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(TaskLiteError, match="register_resource"):
                p.register_resource(CapacityResource("gpu", max_capacity=1.0))


class TestTaskTriggerDefaults:
    """触发器默认规格与显式覆盖。"""

    def test_default_trigger_injected_on_enqueue(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("discover", ok_handler, trigger="always")
        p.enqueue(Job("discover", "scan"))
        assert p._runtime.store.backend.load_queue()[0]["trigger"] == "always"

    def test_explicit_trigger_respected(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("discover", ok_handler, trigger="always")
        p.enqueue(Job("discover", "scan", trigger="once"))
        assert p._runtime.store.backend.load_queue()[0]["trigger"] == "once"

    def test_normal_task_defaults_once(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        p.enqueue(Job("fetch", "a"))
        assert p._runtime.store.backend.load_queue()[0]["trigger"] == "once"


class TestRegisterTransientExceptionRemoved:
    """register_transient_exception 已按 T04 从 TaskLite 门面移除。"""

    def test_method_not_present_on_pipeline(self, tmp_path):
        p = make_pipeline(tmp_path)
        assert not hasattr(p, "register_transient_exception")


# ── 入队与过滤（六步之四）────────────────────────────────────────────


class TestEnqueue:
    """enqueue 摄入：单 Job 入参、有效配置解析与幂等/冲突、快照隔离、successes 不拦。"""

    def test_single_job_returns_true_on_first_enqueue(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        assert p.enqueue(Job("fetch", "a")) is True
        assert len(p._runtime.store.backend.load_queue()) == 1

    @pytest.mark.parametrize("backend", ["memory", "sqlite"])
    def test_unregistered_task_is_rejected_without_side_effects(self, tmp_path, backend):
        from tasklite import TaskLiteError

        with make_pipeline(tmp_path, backend=backend) as p:
            job = Job("missing", "a")
            with pytest.raises(TaskLiteError, match="unknown task_type"):
                p.enqueue(job)
            assert p._runtime.store.backend.load_queue() == []
            assert p._runtime.store.backend.load_successes() == {}
            assert p._runtime.store.backend.load_failed() == {}
            assert p._runtime.store.backend.load_settled_attempts(job.uid) == []

            p.register_task("missing", ok_handler)
            assert p.enqueue(job) is True
            assert [row["job_id"] for row in p._runtime.store.backend.load_queue()] == ["a"]

    def test_unavailable_resource_is_allowed_at_submission(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler, resources={"gpu": 1.0})
        assert p.enqueue(Job("fetch", "a")) is True
        (row,) = p._runtime.store.backend.load_queue()
        assert row["resources"]["gpu"] == 1.0

    def test_list_or_non_job_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        for bad in ([Job("fetch", "a")], [], "bad", 42, {"task_type": "fetch", "job_id": "a"}):
            with pytest.raises(TaskLiteError, match="expects a Job"):
                p.enqueue(bad)  # type: ignore[arg-type]

    def test_same_uid_identical_effective_config_returns_false(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task(
            "fetch",
            ok_handler,
            resources={"gpu": 1.0},
            max_retries_per_run=5,
            timeout_seconds=120,
            trigger="once",
        )
        assert p.enqueue(Job("fetch", "a")) is True
        # 显式传入与继承后的有效配置完全等价（且空 {} 不清空 Task 资源）→ 返回 False
        assert (
            p.enqueue(
                Job(
                    "fetch",
                    "a",
                    payload={},
                    depends_on=[],
                    files={},
                    resources={},
                    max_retries_per_run=5,
                    timeout_seconds=120,
                    trigger="once",
                )
            )
            is False
        )
        assert len(p._runtime.store.backend.load_queue()) == 1

    def test_same_uid_different_effective_config_raises_tasklite_error(self, tmp_path):
        from tasklite import FileSpec, TaskLiteError

        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler, resources={"gpu": 1.0})
        assert p.enqueue(Job("fetch", "a", payload={"x": 1})) is True
        for conflicting_job in (
            Job("fetch", "a", payload={"x": 2}),
            Job("fetch", "a", payload={"x": 1}, depends_on=["fetch::dep"]),
            Job("fetch", "a", payload={"x": 1}, files={"out": FileSpec("p", role="output")}),
            Job("fetch", "a", payload={"x": 1}, resources={"gpu": 2.0}),
            Job("fetch", "a", payload={"x": 1}, max_retries_per_run=0),
            Job("fetch", "a", payload={"x": 1}, timeout_seconds=10),
            Job("fetch", "a", payload={"x": 1}, trigger="always"),
        ):
            with pytest.raises(TaskLiteError, match="already queued with different effective configuration"):
                p.enqueue(conflicting_job)
        assert len(p._runtime.store.backend.load_queue()) == 1

    def test_front_argument_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError):
            p.enqueue(Job("fetch", "a"), front=True)  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            p.enqueue(Job("fetch", "a"), True)  # type: ignore[misc]

    def test_mutating_submitted_payload_or_resources_does_not_pollute_snapshot(self, tmp_path):
        p = make_pipeline(tmp_path)
        task_res = {"cpu": 2.0}
        p.register_task("fetch", ok_handler, resources=task_res)
        src_payload = {"nested": {"items": [1, 2]}}
        src_resources = {"gpu": 1.0}
        job = Job("fetch", "a", payload=src_payload, resources=src_resources)
        assert p.enqueue(job) is True

        # 修改提交源字典、Task 源字典与 Job 实例属性均不得污染已接纳快照
        task_res["cpu"] = 99.0
        src_payload["nested"]["items"].append(3)
        src_resources["gpu"] = 88.0
        job.payload["nested"]["items"].append(4)
        assert job.resources is not None
        job.resources["gpu"] = 77.0

        (row,) = p._runtime.store.backend.load_queue()
        assert row["payload"] == {"nested": {"items": [1, 2]}}
        assert row["resources"] == {"cpu": 2.0, "gpu": 1.0, "__workers__": 1.0}

    def test_enqueue_resolves_relative_files_against_fixed_instance_base_dir(
        self, tmp_path, monkeypatch
    ):
        from tasklite import FileSpec

        dir_a = tmp_path / "workspace_a" / "proj"
        dir_b = tmp_path / "workspace_b"
        dir_a.mkdir(parents=True)
        dir_b.mkdir(parents=True)

        monkeypatch.chdir(dir_a)
        p = make_pipeline(tmp_path / "state")
        p.register_task("fetch", ok_handler)

        # 实例创建后改变主进程工作目录，enqueue 仍使用创建 TaskLite 时的固定基准
        monkeypatch.chdir(dir_b)
        job = Job(
            "fetch",
            "a",
            files={
                "out": FileSpec("results/out.csv", role="output"),
                "ext": FileSpec("../shared/in.csv", role="input"),
            },
        )
        assert p.enqueue(job) is True
        job.files["extra"] = FileSpec("other.txt", role="cache")

        (row,) = p._runtime.store.backend.load_queue()
        assert row["files"] == {
            "out": {
                "path": str(dir_a / "results" / "out.csv"),
                "role": "output",
                "delete_existing": False,
                "delete_on_failure": False,
                "delete_on_success": False,
            },
            "ext": {
                "path": str(dir_a.parent / "shared" / "in.csv"),
                "role": "input",
                "delete_existing": False,
                "delete_on_failure": False,
                "delete_on_success": False,
            },
        }
        # 等价的已解析绝对路径声明幂等去重返回 False
        equivalent = Job(
            "fetch",
            "a",
            files={
                "out": FileSpec(dir_a / "results" / "out.csv", role="output"),
                "ext": FileSpec(dir_a.parent / "shared" / "in.csv", role="input"),
            },
        )
        assert p.enqueue(equivalent) is False

    def test_from_output_requires_direct_dependency_and_keeps_reference_at_submission(
        self, tmp_path, monkeypatch
    ):
        from tasklite import FileSpec, JobContext, TaskLiteError

        p = make_pipeline(tmp_path)
        p.register_task("generate", ok_handler)
        p.register_task("process", ok_handler)
        # 即使存在历史成功产物，提交时仍保留引用配置而不绑定历史路径
        p.set_job_status(Job("generate", "001"), "succeeded", recursive=False)
        p._runtime.store.backend._successes["generate::001"]["artifacts"] = {
            "report": {"path": str(tmp_path / "old_report.csv")}
        }

        missing_dep = Job(
            "process",
            "001",
            files={"src": FileSpec.from_output("generate::001", "report", role="input")},
        )
        with pytest.raises(TaskLiteError, match="depends_on"):
            p.enqueue(missing_dep)
        assert p._runtime.store.backend.load_queue() == []

        valid_consumer = Job(
            "process",
            "001",
            depends_on=["generate::001"],
            files={"src": FileSpec.from_output("generate::001", "report", role="input")},
        )
        assert p.enqueue(valid_consumer) is True
        (queued,) = p._runtime.store.backend.load_queue()
        assert queued["files"] == {
            "src": {
                "producer_uid": "generate::001",
                "output_name": "report",
                "role": "input",
                "delete_existing": False,
                "delete_on_failure": False,
                "delete_on_success": False,
            }
        }

        # ctx.spawn 同样使用固定基准并校验直接依赖与快照隔离
        base_dir = tmp_path / "fixed_base"
        other_dir = tmp_path / "worker_cwd"
        base_dir.mkdir()
        other_dir.mkdir()
        ctx = JobContext(
            Job("generate", "001"),
            frozenset(),
            frozenset(),
            {},
            base_dir=base_dir,
        )
        monkeypatch.chdir(other_dir)
        with pytest.raises(TaskLiteError, match="depends_on"):
            ctx.spawn(
                Job(
                    "process",
                    "002",
                    files={"src": FileSpec.from_output("generate::001", "report", role="input")},
                )
            )
        assert ctx._spawned_jobs == []

        child = Job(
            "process",
            "002",
            depends_on=["generate::001"],
            files={
                "src": FileSpec.from_output("generate::001", "report", role="input"),
                "out": FileSpec("out/result.csv", role="output"),
            },
        )
        ctx.spawn(child)
        child.files["mutated"] = FileSpec("mut.txt", role="cache")
        assert len(ctx._spawned_jobs) == 1
        spawned = ctx._spawned_jobs[0]
        assert set(spawned.files.keys()) == {"src", "out"}
        assert spawned.files["src"] == FileSpec.from_output(
            "generate::001", "report", role="input"
        )
        assert spawned.files["out"] == FileSpec(
            base_dir / "out" / "result.csv", role="output"
        )

    def test_pending_recovery_across_runs_preserves_accepted_snapshot(self, tmp_path):
        """跨次恢复 pending 不套用新 Task 默认。"""
        db_path = tmp_path / "recover.db"
        p1 = TaskLite(backend="sqlite", state_path=db_path, max_workers=1)
        p1.register_resource(CapacityResource("gpu", max_capacity=2.0))
        p1.register_task(
            "fetch",
            ok_handler,
            resources={"gpu": 1.0},
            max_retries_per_run=1,
            timeout_seconds=15,
            trigger="once",
        )
        assert p1.enqueue(Job("fetch", "a")) is True
        p1.stop()

        observed: list[dict[str, Any]] = []

        class CapturingProcess(make_instant_process_class()):
            def __init__(self, *, args=(), **kwargs):
                spec = args[0]
                observed.append(spec.job.to_dict())
                super().__init__(args=args, **kwargs)

        p2 = TaskLite(backend="sqlite", state_path=db_path, max_workers=1)
        install_fake_process_context(p2, CapturingProcess)
        p2.register_resource(CapacityResource("gpu", max_capacity=2.0))
        # 新运行注册了完全不同的 Task 默认（甚至包含未注册资源 extra_vram）
        p2.register_task(
            "fetch",
            ok_handler,
            resources={"gpu": 2.0, "extra_vram": 99.0},
            max_retries_per_run=9,
            timeout_seconds=999,
            trigger="always",
        )
        p2.run()
        p2.stop()
        assert len(p2._runtime.store.backend.load_successes()) == 1
        assert len(observed) == 1
        assert observed[0]["resources"] == {"gpu": 1.0, "__workers__": 1.0}
        assert observed[0]["max_retries_per_run"] == 1
        assert observed[0]["timeout_seconds"] == 15
        assert observed[0]["trigger"] == "once"

    def test_wall_hit_not_blocked_at_enqueue(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        p.set_job_status(Job("fetch", "done"), "succeeded", recursive=False)
        assert p.enqueue(Job("fetch", "done")) is True
        assert len(p._runtime.store.backend.load_queue()) == 1

    def test_intake_pipeline_injections(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        p.enqueue(Job("fetch", "a", timeout_seconds=17, payload={"timeout": "业务时限"}))
        (row,) = p._runtime.store.backend.load_queue()
        assert row["resources"]["__workers__"] == 1.0
        assert row["timeout_seconds"] == 17
        assert "timeout" not in row
        restored = Job.from_dict(row)
        assert restored.timeout_seconds == 17
        assert restored.payload == {"timeout": "业务时限"}

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(TaskLiteError, match="enqueue"):
                p.enqueue(Job("fetch", "a"))


# ── 六步契约与运行门面 ────────────────────────────────────────────────


class TestSixStepContract:
    """初始化 → register_resource → register_task/register_transient
    → enqueue → run → stop 的完整链路（fake 进程驱动）。"""

    def test_full_six_step_flow(self, tmp_path):
        p = make_pipeline(tmp_path)

        p.register_resource(CapacityResource("gpu", max_capacity=2.0))
        p.register_task("fetch", ok_handler, resources={"gpu": 1.0})
        p.enqueue(Job("fetch", "a"))
        p.enqueue(Job("fetch", "b"))

        install_fake_process_context(p, make_instant_process_class())
        res = p.run()
        p.stop()

        assert res is None
        assert sorted(p._runtime.store.backend.load_successes()) == ["fetch::a", "fetch::b"]

    def test_run_reentry_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(TaskLiteError, match="run.*in progress"):
                p.run()

    def test_stop_transitions_session_state(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            p.stop()
            assert p._runtime.stop_mode is StopMode.DRAINING
            # 幂等无副作用
            p.stop()
            assert p._runtime.stop_mode is StopMode.DRAINING

    def test_is_running_flag_lifecycle(self, tmp_path):
        p = make_pipeline(tmp_path)
        assert p.is_running is False
        p.register_task("fetch", ok_handler)
        p.enqueue(Job("fetch", "a"))
        observed: list[bool] = []

        class ObservingProcess(make_instant_process_class()):
            """派发窗口内观察 run 期旗子（start() 发生在主循环体内）。"""

            def start(self):
                observed.append(p.is_running)
                super().start()

        install_fake_process_context(p, ObservingProcess)
        p.run()
        assert observed == [True]
        assert p.is_running is False


# ── 调度接缝构造参数（ordering_policy / requeue_policy）─────────────────────


class TestSchedulingSeamWiring:
    """调度策略不在公开构造签名开放，默认装配行为保持。"""

    def test_scheduling_constructor_args_rejected(self, tmp_path):
        with pytest.raises(TypeError):
            TaskLite(backend="memory", ordering_policy=NewestFirstPolicy())  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            TaskLite(backend="memory", requeue_policy=TailRequeuePolicy())  # type: ignore[call-arg]

    def test_ordering_default_wiring_and_fifo_equivalence(self, tmp_path):
        order: list[str] = []
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        for jid in ("a", "b", "c"):
            p.enqueue(Job("fetch", jid))
        install_fake_process_context(p, make_instant_process_class(recorder=order))
        p.run()

        assert order == ["fetch::a", "fetch::b", "fetch::c"]
        assert isinstance(p._runtime_config.ordering_policy, FifoOrderingPolicy)
        assert p._runtime.scheduler.ordering_policy is p._runtime_config.ordering_policy

    def test_requeue_default_wiring_and_front_equivalence(self, tmp_path):
        """默认立即重入队（front=True）：flaky 重试先于 later 结算。"""
        order: list[str] = []
        p = make_pipeline(
            tmp_path,
            max_workers=1,
        )
        p.register_task("fetch", ok_handler)
        p.enqueue(Job("fetch", "flaky", max_retries_per_run=1))
        p.enqueue(Job("fetch", "later"))
        install_fake_process_context(
            p,
            make_instant_process_class(retry_uid="fetch::flaky", recorder=order),
        )
        p.run()

        assert order == [
            "fetch::flaky", "fetch::flaky", "fetch::later",
        ]
        assert "fetch::later" in p._runtime.store.backend.load_successes()
        assert "fetch::flaky" in p._runtime.store.backend.load_failed()
        assert isinstance(p._runtime_config.requeue_policy, ImmediateRequeuePolicy)
        assert p._runtime.store._requeue_policy is p._runtime_config.requeue_policy


# ── 管理面委托（run() 外）────────────────────────────────────────────


class TestManageApis:
    """失败档案/挂起/种子/清史等运维 API 的委托与守卫。"""

    def test_set_job_status_and_requeue(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        p.set_job_status(Job("fetch", "bad"), "failed", recursive=False)
        assert len(p._runtime.store.backend.load_failed()) == 1
        p.set_job_status("fetch::bad", "pending", recursive=False)
        assert p._runtime.store.backend.load_failed() == {}
        (row,) = p._runtime.store.backend.load_queue()
        assert row["job_id"] == "bad"

    def test_set_job_status_unknown_uid_raises(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TaskLiteError, match="No existing snapshot found"):
            p.set_job_status("fetch::nope", "pending", recursive=False)

    def test_cursor_get_and_set(self, tmp_path):
        p = make_pipeline(tmp_path)
        assert p.get_cursor("page") is None
        p.set_cursor("page", "7")
        assert p.get_cursor("page") == "7"
        assert p._runtime.store.backend.load_cursors()["page"] == "7"
        p.set_cursor("page", None)
        assert p.get_cursor("page") is None
        assert p._runtime.store.backend.load_cursors() == {}

    def test_clear_history_removes_from_selected_records(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.set_job_status(Job("fetch", "ok"), "succeeded", recursive=False)
        p.set_job_status(Job("fetch", "bad"), "failed", recursive=False)
        removed_succ = p.clear_history("fetch::ok", records=["successes"])
        assert removed_succ == 1
        assert p._runtime.store.backend.load_successes() == {}
        removed_fail = p.clear_history("fetch::bad", records=["failures"])
        assert removed_fail == 1
        assert p._runtime.store.backend.load_failed() == {}

    def test_run_guards_across_manage_apis(self, tmp_path):
        guarded = [
            ("set_job_status", lambda p: p.set_job_status(Job("fetch", "a"), "pending", recursive=False)),
            ("clear_history", lambda p: p.clear_history("fetch::a", records=["successes"])),
            ("get_cursor", lambda p: p.get_cursor("k")),
            ("set_cursor", lambda p: p.set_cursor("k", "v")),
        ]
        for api_name, call in guarded:
            p = make_pipeline(tmp_path)
            with running(p):
                with pytest.raises(TaskLiteError, match=api_name):
                    call(p)


class TestRunExecutionOptions:
    """run() 运行参数透传：引擎可配的 ExecutionOptions 在公开面可达，
    缺省与引擎默认一致（默认行为不变）。"""

    def test_default_run_acquires_reentry_lock(self, tmp_path):
        """默认等价断言：run() 照常获取状态库单运行排他锁。"""
        from tasklite.utils.lockfile import release_lock, try_acquire_lock

        p = make_pipeline(tmp_path, backend="sqlite", state_path=tmp_path / "lock.db")
        lock_fd = try_acquire_lock(p.state_path, timeout=0)
        assert lock_fd is not None
        try:
            with pytest.raises(TaskLiteError, match="Another run.*in progress"):
                p.run()
        finally:
            release_lock(lock_fd)

    def test_install_signals_wiring(self, tmp_path, monkeypatch):
        """install_signals 接线：False 不安装信号陷阱，默认安装
        SIGTERM/SIGINT（记录器断言，不硬造信号场景）。"""
        import signal as signal_module

        import tasklite.engine.runtime as runtime_module

        installed: list[int] = []

        class _SignalRecorder:
            """signal 模块替身：只记录安装请求，不装真实 handler。"""

            SIGTERM = signal_module.SIGTERM
            SIGINT = signal_module.SIGINT

            @staticmethod
            def signal(signum, handler):
                installed.append(signum)
                return None

        monkeypatch.setattr(runtime_module, "signal", _SignalRecorder)
        p = make_pipeline(tmp_path)

        p.run(install_signals=False)
        assert installed == []

        res = p.run()
        assert res is None
        assert installed == [signal_module.SIGTERM, signal_module.SIGINT]


class TestBackendSetterDeleted:
    """backend setter 已删除，backend 为只读 property。"""

    def test_backend_setter_raises_attribute_error(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(AttributeError):
            p._runtime.store.backend = InMemoryStateBackend()
