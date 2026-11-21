"""TaskLite 门面单元测试：构造、注册面、入队、六步契约、管理 API 与运行选项。"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from tasklite import FileSpec, Job, JobContext, StateSnapshot, Task, TaskLite, TaskLiteError, TaskResult
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import RequeuePlan, RequeuePolicy
from tasklite.engine.scheduler import OrderingPolicy
from tasklite.engine.store import StateStore
from tasklite.resource import CapacityResource, IntervalResource
from tasklite.testing import in_process_workers, running, spawned_jobs
from tasklite.utils.lockfile import release_lock, try_acquire_lock


def ok_handler(job: Job, ctx: JobContext) -> TaskResult:
    """占位 handler：成功 + 空 meta（模块级可 pickle）。"""
    return TaskResult.success()


def publish_report_handler(job: Job, ctx: JobContext) -> TaskResult:
    """发布具名产物 report，供产物引用入队测试使用。"""
    report_path = ctx.declare_file("report", FileSpec("old_report.csv", role="output"))
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("published", encoding="utf-8")
    return TaskResult.success()


class NewestFirstPolicy(OrderingPolicy):
    """队尾优先访问序（构造拒绝测试样本）。"""

    def visit_order(self, queue):
        return range(len(queue) - 1, -1, -1)


class TailRequeuePolicy(RequeuePolicy):
    """恒队尾重入队策略（构造拒绝测试样本）。"""

    def plan_requeue(self, job_snapshot) -> RequeuePlan:
        return RequeuePlan(front=False, delay_seconds=0.0)


def make_instant_process_class(retry_uid: str | None = None, recorder: list | None = None):
    """FakeProcess：start() 经 Pipe 发送结果并退出。"""

    class InstantProcess:
        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self.args = args
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
    """标准测试管线（默认 memory 后端）。"""
    backend = overrides.pop("backend", "memory")
    state_path = overrides.pop("state_path", None)
    if backend == "sqlite" and state_path is None:
        state_path = (tmp_path / "state.db") if tmp_path else Path("./state.db")
    return TaskLite(
        backend=backend,
        state_path=state_path,
        **overrides,
    )


class TestConstruction:
    """构造参数、目录、后端、生命周期与互斥契约。"""

    def test_sqlite_requires_state_path(self):
        with pytest.raises(TaskLiteError, match="state_path"):
            TaskLite(backend="sqlite")
        with pytest.raises(TaskLiteError, match="state_path"):
            TaskLite(backend="sqlite", state_path="")

    def test_memory_requires_no_state_path_and_no_disk_side_effects(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with TaskLite(backend="memory") as p:
            p.register_task("t", ok_handler)
            p.enqueue(Job("t", "a"))
            with StateSnapshot.open(p) as snap:
                row = snap.get_job("t::a")
                assert row is not None and row["status"] == "pending"
        assert list(tmp_path.iterdir()) == []

    def test_backend_validation_and_not_public(self, tmp_path):
        with pytest.raises(TaskLiteError, match="Unknown backend"):
            make_pipeline(tmp_path, backend="redis")
        with pytest.raises(TaskLiteError, match="backend must be"):
            make_pipeline(tmp_path, backend=object())
        with pytest.raises(TaskLiteError, match="backend must be"):
            TaskLite(backend=InMemoryStateBackend())  # type: ignore[arg-type]

        with make_pipeline(tmp_path) as p:
            assert not hasattr(TaskLite, "backend")
            with pytest.raises(AttributeError):
                getattr(p, "backend")

    def test_sqlite_init_failure_and_owned_backend_cleanup(self, tmp_path, monkeypatch):
        import tasklite.pipeline as pipeline_mod

        def boom(_path, **kw):
            raise OSError("disk full")

        monkeypatch.setattr(pipeline_mod, "SQLiteStateBackend", boom)
        with pytest.raises(OSError, match="disk full"):
            TaskLite(backend="sqlite", state_path=tmp_path / "test.db")

        monkeypatch.undo()
        backend = InMemoryStateBackend()
        closed: list[bool] = []
        monkeypatch.setattr(backend, "close", lambda: closed.append(True))
        monkeypatch.setattr(pipeline_mod, "InMemoryStateBackend", lambda **_kwargs: backend)
        failure = OSError("runtime wiring failed")

        def fail_runtime(**_kwargs):
            raise failure

        monkeypatch.setattr(pipeline_mod, "EngineRuntime", fail_runtime)
        with pytest.raises(OSError) as caught:
            TaskLite(backend="memory")
        assert caught.value is failure
        assert closed == [True]

    def test_close_releases_owned_backend_once(self, tmp_path, monkeypatch):
        closed: list[bool] = []
        orig_close = InMemoryStateBackend.close

        def tracked_close(self):
            closed.append(True)
            orig_close(self)

        monkeypatch.setattr(InMemoryStateBackend, "close", tracked_close)
        p = make_pipeline(tmp_path)
        p.close()
        p.close()
        assert closed == [True]

    def test_init_failure_after_backend_creation_preserves_existing_state(self, tmp_path, monkeypatch):
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

        with StateSnapshot.open(db) as snapshot:
            record = snapshot.get_job("work::kept")
            assert record is not None
            assert record["status"] == "pending"

    def test_max_workers_validation_and_worker_capacity(self, tmp_path):
        for bad in (0, -1, True, "4", 2.5):
            with pytest.raises(TaskLiteError, match="max_workers"):
                make_pipeline(tmp_path, max_workers=bad)

        with make_pipeline(tmp_path, max_workers=7) as p:
            p.register_task("fit", ok_handler, resources={"__workers__": 7.0})
            p.register_task("over", ok_handler, resources={"__workers__": 8.0})
            p.enqueue(Job("fit", "ok"))
            p.enqueue(Job("over", "fail"))
            with in_process_workers(p, make_instant_process_class()):
                p.run(install_signals=False)
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("fit::ok")["status"] == "succeeded"  # type: ignore[index]
                assert snap.get_job("over::fail")["status"] == "failed"  # type: ignore[index]

    def test_removed_constructor_params_rejected(self):
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
                TaskLite(backend="memory", **{bad_kwarg: "val"})  # type: ignore[arg-type]

    def test_close_and_context_manager_lifecycle(self):
        with TaskLite(backend="memory") as p:
            p.register_task("fast", ok_handler)
            p.enqueue(Job("fast", "j1"))
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("fast::j1") is not None
        p.close()
        for call in (
            lambda: p.enqueue(Job("fast", "j2")),
            lambda: p.run(),
            lambda: p.register_task("fast2", ok_handler),
            lambda: p.register_resource(CapacityResource("res", max_capacity=1.0)),
            lambda: p.edit(),
            lambda: p.set_cursor("key", "val"),
            lambda: p.get_cursor("key"),
            lambda: p.stop(),
            lambda: p.__enter__(),
            lambda: StateSnapshot.open(p),
        ):
            with pytest.raises(TaskLiteError, match="closed"):
                call()

    def test_close_during_run_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fast", ok_handler)
        p.enqueue(Job("fast", "j1"))
        with running(p):
            with pytest.raises(TaskLiteError, match="close.*run"):
                p.close()
        p.close()

    def test_state_snapshot_independent_lifecycle_and_parity(self, tmp_path):
        db = tmp_path / "snap_test.db"
        p = TaskLite(backend="sqlite", state_path=db)
        p.register_task("t", ok_handler)
        p.enqueue(Job("t", "j1"))
        with StateSnapshot.open(p) as s1, StateSnapshot.open(db) as s2:
            assert s1.get_job("t::j1") == s2.get_job("t::j1")
        snap = StateSnapshot.open(p)
        p.close()
        assert snap.get_job("t::j1")["status"] == "pending"  # type: ignore[index]
        snap.close()
        with pytest.raises(TaskLiteError, match="closed"):
            StateSnapshot.open(p)
        with StateSnapshot.open(db) as snap2:
            assert snap2.get_job("t::j1")["status"] == "pending"  # type: ignore[index]

    def test_init_failure_leaves_existing_state_intact(self, tmp_path):
        db = tmp_path / "precious.db"
        db.write_text("precious data")
        with pytest.raises(Exception):
            TaskLite(backend="sqlite", state_path=db)
        assert db.read_text() == "precious data"

    def test_mutual_exclusion_between_instances_and_management(self, tmp_path):
        db = tmp_path / "mutex.db"
        p1 = TaskLite(backend="sqlite", state_path=db)
        p2 = TaskLite(backend="sqlite", state_path=db)
        p1.register_task("t", ok_handler)
        p2.register_task("t", ok_handler)
        p1.enqueue(Job("t", "j1"))
        lock_fd = try_acquire_lock(db, timeout=0)
        assert lock_fd is not None
        try:
            with pytest.raises(TaskLiteError, match="Another run.*in progress"):
                p2.run()
            with pytest.raises(TaskLiteError, match="Another run.*in progress"):
                with p2.edit(dry_run=False) as tx:
                    tx.set_status("t::j1", "failed", recursive=False)
            with pytest.raises(TaskLiteError, match="Another run.*in progress"):
                p2.enqueue(Job("t", "j2"))
            with StateSnapshot.open(db) as snap:
                assert snap.get_job("t::j1") is not None
        finally:
            release_lock(lock_fd)
        p1.close()
        p2.close()


class TestRegisterTaskAndResource:
    """register_task 规格式/简式双形态、register_resource 与守卫契约。"""

    def test_register_task_spec_and_simple_forms(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            spec = Task("fetch", ok_handler, resources={"api": 1.0})
            p.register_task(spec)
            p.register_task(
                "upload",
                ok_handler,
                resources={"api": 2.0},
                max_retries_per_run=1,
                timeout_seconds=30,
                trigger="always",
            )
            p.enqueue(Job("fetch", "f1"))
            p.enqueue(Job("upload", "u1"))
            with StateSnapshot.open(p) as snap:
                f_job = snap.get_job("fetch::f1")["job"]  # type: ignore[index]
                u_job = snap.get_job("upload::u1")["job"]  # type: ignore[index]
                assert f_job["resources"] == {"api": 1.0, "__workers__": 1.0}
                assert u_job["resources"] == {"api": 2.0, "__workers__": 1.0}
                assert u_job["max_retries_per_run"] == 1
                assert u_job["timeout_seconds"] == 30
                assert u_job["trigger"] == "always"

    def test_register_task_validation_and_guards(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            with pytest.raises(TypeError):
                p.register_task("fetch", ok_handler, {"api": 1.0})  # type: ignore[misc]
            with pytest.raises(TaskLiteError, match="handler is required"):
                p.register_task("fetch")
            with pytest.raises(TaskLiteError, match="non-empty str"):
                p.register_task("", ok_handler)
            p.register_task("fe::tch", ok_handler)
            assert p.enqueue(Job("fe::tch", "1")) is True

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

            p.register_task("fetch", ok_handler)
            with pytest.raises(TaskLiteError, match="already registered"):
                p.register_task("fetch", ok_handler)
            with running(p):
                with pytest.raises(TaskLiteError, match="register_task"):
                    p.register_task("another", ok_handler)

    def test_register_resource_validation_and_guards(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            p.register_resource(IntervalResource("api", interval_seconds=0.5))
            with pytest.raises(TaskLiteError, match="already registered"):
                p.register_resource(CapacityResource("api", max_capacity=2.0))
            with pytest.raises(TaskLiteError, match="Resource instance"):
                p.register_resource("api")  # type: ignore[arg-type]
            with pytest.raises(TaskLiteError, match="internal worker resource"):
                p.register_resource(CapacityResource("__workers__", max_capacity=9.0))
            with running(p):
                with pytest.raises(TaskLiteError, match="register_resource"):
                    p.register_resource(CapacityResource("gpu", max_capacity=1.0))

    def test_task_trigger_defaults_and_removed_register_transient(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            assert not hasattr(p, "register_transient_exception")
            p.register_task("discover", ok_handler, trigger="always")
            p.register_task("fetch", ok_handler)
            p.enqueue(Job("discover", "scan"))
            p.enqueue(Job("discover", "scan_once", trigger="once"))
            p.enqueue(Job("fetch", "a"))
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("discover::scan")["job"]["trigger"] == "always"  # type: ignore[index]
                assert snap.get_job("discover::scan_once")["job"]["trigger"] == "once"  # type: ignore[index]
                assert snap.get_job("fetch::a")["job"]["trigger"] == "once"  # type: ignore[index]


class TestEnqueue:
    """enqueue 摄入、幂等/冲突、快照隔离、相对路径解析与产物引用。"""

    @pytest.mark.parametrize("backend", ["memory", "sqlite"])
    def test_unregistered_task_rejected_without_side_effects(self, tmp_path, backend):
        with make_pipeline(tmp_path, backend=backend) as p:
            job = Job("missing", "a")
            with pytest.raises(TaskLiteError, match="unknown task_type"):
                p.enqueue(job)
            with StateSnapshot.open(p) as snap:
                assert list(snap.iter_jobs()) == []
                assert list(snap.iter_attempts()) == []

            p.register_task("missing", ok_handler)
            assert p.enqueue(job) is True
            with StateSnapshot.open(p) as snap:
                assert [row["job"]["job_id"] for row in snap.iter_jobs(status="pending")] == ["a"]

    def test_unavailable_resource_allowed_and_bad_args_rejected(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            p.register_task("fetch", ok_handler, resources={"gpu": 1.0})
            assert p.enqueue(Job("fetch", "a")) is True
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("fetch::a")["job"]["resources"]["gpu"] == 1.0  # type: ignore[index]

            for bad in ([Job("fetch", "a")], [], "bad", 42, {"task_type": "fetch", "job_id": "a"}):
                with pytest.raises(TaskLiteError, match="expects a Job"):
                    p.enqueue(bad)  # type: ignore[arg-type]
            with pytest.raises(TypeError):
                p.enqueue(Job("fetch", "b"), front=True)  # type: ignore[call-arg]
            with pytest.raises(TypeError):
                p.enqueue(Job("fetch", "b"), True)  # type: ignore[misc]

    def test_same_uid_idempotency_and_config_conflict(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            p.register_task(
                "fetch",
                ok_handler,
                resources={"gpu": 1.0},
                max_retries_per_run=5,
                timeout_seconds=120,
                trigger="once",
            )
            assert p.enqueue(Job("fetch", "a")) is True
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
            with StateSnapshot.open(p) as snap:
                assert len(list(snap.iter_jobs(status="pending"))) == 1

        with make_pipeline(tmp_path) as p2:
            p2.register_task("fetch", ok_handler, resources={"gpu": 1.0})
            assert p2.enqueue(Job("fetch", "a", payload={"x": 1})) is True
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
                    p2.enqueue(conflicting_job)
            with StateSnapshot.open(p2) as snap:
                assert len(list(snap.iter_jobs(status="pending"))) == 1

    def test_mutating_submitted_payload_or_resources_does_not_pollute_snapshot(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            task_res = {"cpu": 2.0}
            p.register_task("fetch", ok_handler, resources=task_res)
            src_payload = {"nested": {"items": [1, 2]}}
            src_resources = {"gpu": 1.0}
            job = Job("fetch", "a", payload=src_payload, resources=src_resources)
            assert p.enqueue(job) is True

            task_res["cpu"] = 99.0
            src_payload["nested"]["items"].append(3)
            src_resources["gpu"] = 88.0
            job.payload["nested"]["items"].append(4)
            assert job.resources is not None
            job.resources["gpu"] = 77.0

            with StateSnapshot.open(p) as snap:
                row = snap.get_job("fetch::a")["job"]  # type: ignore[index]
                assert row["payload"] == {"nested": {"items": [1, 2]}}
                assert row["resources"] == {"cpu": 2.0, "gpu": 1.0, "__workers__": 1.0}

    def test_enqueue_resolves_relative_files_against_fixed_instance_base_dir(
        self, tmp_path, monkeypatch
    ):
        dir_a = tmp_path / "workspace_a" / "proj"
        dir_b = tmp_path / "workspace_b"
        dir_a.mkdir(parents=True)
        dir_b.mkdir(parents=True)

        monkeypatch.chdir(dir_a)
        with make_pipeline(tmp_path / "state") as p:
            p.register_task("fetch", ok_handler)
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

            with StateSnapshot.open(p) as snap:
                queued = snap.get_job("fetch::a")
                assert queued is not None
                assert queued["job"]["files"] == {
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
        monkeypatch.chdir(tmp_path)
        with make_pipeline(tmp_path) as p:
            p.register_task("generate", publish_report_handler)
            p.register_task("process", ok_handler)
            p.enqueue(Job("generate", "001"))
            with in_process_workers(p):
                p.run(install_signals=False)

            missing_dep = Job(
                "process",
                "001",
                files={"src": FileSpec.from_output("generate::001", "report", role="input")},
            )
            with pytest.raises(TaskLiteError, match="depends_on"):
                p.enqueue(missing_dep)
            with StateSnapshot.open(p) as snap:
                assert list(snap.iter_jobs(status="pending")) == []

            valid_consumer = Job(
                "process",
                "001",
                depends_on=["generate::001"],
                files={"src": FileSpec.from_output("generate::001", "report", role="input")},
            )
            assert p.enqueue(valid_consumer) is True
            with StateSnapshot.open(p) as snap:
                queued = snap.get_job("process::001")
                assert queued is not None
                assert queued["job"]["files"] == {
                    "src": {
                        "producer_uid": "generate::001",
                        "output_name": "report",
                        "role": "input",
                        "delete_existing": False,
                        "delete_on_failure": False,
                        "delete_on_success": False,
                    }
                }

        base_dir = tmp_path / "fixed_base"
        other_dir = tmp_path / "worker_cwd"
        base_dir.mkdir()
        other_dir.mkdir()
        ctx = JobContext(Job("generate", "001"), frozenset(), frozenset(), {}, base_dir=base_dir)
        monkeypatch.chdir(other_dir)
        with pytest.raises(TaskLiteError, match="depends_on"):
            ctx.spawn(
                Job(
                    "process",
                    "002",
                    files={"src": FileSpec.from_output("generate::001", "report", role="input")},
                )
            )
        assert spawned_jobs(ctx) == []

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
        spawned_list = spawned_jobs(ctx)
        assert len(spawned_list) == 1
        assert spawned_list[0].files["src"] == FileSpec.from_output(
            "generate::001", "report", role="input"
        )
        assert spawned_list[0].files["out"] == FileSpec(
            base_dir / "out" / "result.csv", role="output"
        )

    def test_pending_recovery_across_runs_preserves_accepted_snapshot(self, tmp_path):
        """跨次恢复 pending 不套用新 Task 默认。"""
        db_path = tmp_path / "recover.db"
        with TaskLite(backend="sqlite", state_path=db_path, max_workers=1) as p1:
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

        with TaskLite(backend="sqlite", state_path=db_path, max_workers=1) as p2:
            p2.register_resource(CapacityResource("gpu", max_capacity=2.0))
            p2.register_task(
                "fetch",
                ok_handler,
                resources={"gpu": 2.0, "extra_vram": 99.0},
                max_retries_per_run=9,
                timeout_seconds=999,
                trigger="always",
            )
            with in_process_workers(p2, CapturingProcess):
                p2.run()
            with StateSnapshot.open(p2) as snap:
                assert snap.get_job("fetch::a")["status"] == "succeeded"  # type: ignore[index]
        assert len(observed) == 1
        assert observed[0]["resources"] == {"gpu": 1.0, "__workers__": 1.0}
        assert observed[0]["max_retries_per_run"] == 1
        assert observed[0]["timeout_seconds"] == 15
        assert observed[0]["trigger"] == "once"

    def test_wall_hit_intake_and_run_guard(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            p.register_task("fetch", ok_handler)
            with p.edit(dry_run=False) as tx:
                tx.add(Job("fetch", "done"), status="succeeded")
            assert p.enqueue(Job("fetch", "done")) is True
            assert p.enqueue(Job("fetch", "a", timeout_seconds=17, payload={"timeout": "业务时限"})) is True
            with StateSnapshot.open(p) as snap:
                assert len(list(snap.iter_jobs(status="pending"))) == 2
                row = snap.get_job("fetch::a")["job"]  # type: ignore[index]
                assert row["resources"]["__workers__"] == 1.0
                assert row["timeout_seconds"] == 17
                assert row["payload"] == {"timeout": "业务时限"}
            with running(p):
                with pytest.raises(TaskLiteError, match="enqueue"):
                    p.enqueue(Job("fetch", "blocked"))


class TestSixStepAndSchedulingContract:
    """六步契约、运行态标志与默认 FIFO/队首重入队调度行为。"""

    def test_full_six_step_flow_and_reentry_guard(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            p.register_resource(CapacityResource("gpu", max_capacity=2.0))
            p.register_task("fetch", ok_handler, resources={"gpu": 1.0})
            p.enqueue(Job("fetch", "a"))
            p.enqueue(Job("fetch", "b"))

            with in_process_workers(p, make_instant_process_class()):
                res = p.run()
            p.stop()
            assert res is None
            with StateSnapshot.open(p) as snap:
                succeeded_uids = sorted(row["job_uid"] for row in snap.iter_jobs(status="succeeded"))
                assert succeeded_uids == ["fetch::a", "fetch::b"]

            with running(p):
                with pytest.raises(TaskLiteError, match="run.*in progress"):
                    p.run()
                p.stop()
                p.stop()

    def test_is_running_flag_lifecycle(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            assert p.is_running is False
            p.register_task("fetch", ok_handler)
            p.enqueue(Job("fetch", "a"))
            observed: list[bool] = []

            class ObservingProcess(make_instant_process_class()):
                def start(self):
                    observed.append(p.is_running)
                    super().start()

            with in_process_workers(p, ObservingProcess):
                p.run()
            assert observed == [True]
            assert p.is_running is False

    def test_scheduling_constructor_args_rejected_and_default_fifo_front_requeue(self, tmp_path):
        with pytest.raises(TypeError):
            TaskLite(backend="memory", ordering_policy=NewestFirstPolicy())  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            TaskLite(backend="memory", requeue_policy=TailRequeuePolicy())  # type: ignore[call-arg]

        order: list[str] = []
        with make_pipeline(tmp_path) as p:
            p.register_task("fetch", ok_handler)
            for jid in ("a", "b", "c"):
                p.enqueue(Job("fetch", jid))
            with in_process_workers(p, make_instant_process_class(recorder=order)):
                p.run()
        assert order == ["fetch::a", "fetch::b", "fetch::c"]

        requeue_order: list[str] = []
        with make_pipeline(tmp_path, max_workers=1) as p2:
            p2.register_task("fetch", ok_handler)
            p2.enqueue(Job("fetch", "flaky", max_retries_per_run=1))
            p2.enqueue(Job("fetch", "later"))
            with in_process_workers(
                p2,
                make_instant_process_class(retry_uid="fetch::flaky", recorder=requeue_order),
            ):
                p2.run()
            with StateSnapshot.open(p2) as snap:
                assert snap.get_job("fetch::later")["status"] == "succeeded"  # type: ignore[index]
                assert snap.get_job("fetch::flaky")["status"] == "failed"  # type: ignore[index]
        assert requeue_order == ["fetch::flaky", "fetch::flaky", "fetch::later"]


class TestManageApisAndRunOptions:
    """run() 外管理 API、守卫、文件锁与信号安装。"""

    def test_edit_cursors_and_guards(self, tmp_path):
        with make_pipeline(tmp_path) as p:
            p.register_task("fetch", ok_handler)
            with p.edit(dry_run=False) as tx:
                tx.add(Job("fetch", "bad"), status="failed")
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("fetch::bad")["status"] == "failed"  # type: ignore[index]
            with p.edit(dry_run=False) as tx:
                tx.set_status("fetch::bad", "pending", recursive=False)
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("fetch::bad")["status"] == "pending"  # type: ignore[index]
            with pytest.raises(TaskLiteError, match="does not exist"):
                with p.edit(dry_run=False) as tx:
                    tx.set_status("fetch::nope", "pending", recursive=False)

            assert p.get_cursor("page") is None
            p.set_cursor("page", "7")
            assert p.get_cursor("page") == "7"
            p.set_cursor("page", None)
            assert p.get_cursor("page") is None

            with p.edit(dry_run=False) as tx:
                tx.add(Job("fetch", "ok"), status="succeeded")
                tx.add(Job("fetch", "bad_done"), status="failed")
            with p.edit(dry_run=False) as tx:
                assert tx.remove("fetch::ok", entries=("successes",), recursive=False) == 1
                assert tx.remove("fetch::bad_done", entries=("failures",), recursive=False) == 1
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("fetch::ok") is None
                assert snap.get_job("fetch::bad_done") is None

            guarded = [
                ("edit", lambda pl: pl.edit()),
                ("get_cursor", lambda pl: pl.get_cursor("k")),
                ("set_cursor", lambda pl: pl.set_cursor("k", "v")),
            ]
            for api_name, call in guarded:
                with running(p):
                    with pytest.raises(TaskLiteError, match=api_name):
                        call(p)

    def test_default_run_acquires_reentry_lock_and_installs_signals(self, tmp_path, monkeypatch):
        import signal as signal_module
        import tasklite.engine.runtime as runtime_module

        db = tmp_path / "lock.db"
        with make_pipeline(tmp_path, backend="sqlite", state_path=db) as p:
            lock_fd = try_acquire_lock(db, timeout=0)
            assert lock_fd is not None
            try:
                with pytest.raises(TaskLiteError, match="Another run.*in progress"):
                    p.run()
            finally:
                release_lock(lock_fd)

        installed: list[int] = []

        class _SignalRecorder:
            SIGTERM = signal_module.SIGTERM
            SIGINT = signal_module.SIGINT

            @staticmethod
            def signal(signum, handler):
                installed.append(signum)
                return None

        monkeypatch.setattr(runtime_module, "signal", _SignalRecorder)
        with make_pipeline(tmp_path) as p2:
            p2.run(install_signals=False)
            assert installed == []
            assert p2.run() is None
            assert installed == [signal_module.SIGTERM, signal_module.SIGINT]

    def test_state_store_backend_setter_raises_attribute_error(self):
        store = StateStore(InMemoryStateBackend())
        with pytest.raises(AttributeError):
            store.backend = InMemoryStateBackend()  # type: ignore[misc]
