"""v2 TaskLite 门面回归：六步调用契约、注册面、入队与管理面委托。

装配分区行为等价验证：构造器拆分为 backend / 分类器 / 资源 / IPC /
RunConfig / 运维控制台各私有装配方法后，对外契约（校验、快照、守卫、
委托）与 v1 门面逐一对照。run 路径以确定性 fake 进程驱动（写 IPC 结果
文件，与真实子进程共享同一收割代码路径）。
"""
from __future__ import annotations

import multiprocessing as mp
from pathlib import Path

import pytest

from tasklite.backend.base import AbstractStateBackend
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import (
    ImmediateRequeuePolicy,
    RequeuePlan,
    RequeuePolicy,
)
from tasklite.engine.resource import CapacityResource, RateLimitResource
from tasklite.engine.scheduler import FifoOrderingPolicy, OrderingPolicy
from tasklite.engine.types import AttemptOutcome, StopMode
from tasklite.models.job import Job
from tasklite.models.result import TaskResult
from tasklite.models.task import Task
from tasklite.pipeline import TaskLite
from tasklite.testing import running
from tasklite.utils.ipc import ArtifactJournal


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

    def plan_requeue(self, job_snapshot, *, transient_kind=None) -> RequeuePlan:
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

    def save_queue(self, *args, **kwargs):
        return self.inner.save_queue(*args, **kwargs)

    def replace_queue_atomic(self, *args, **kwargs):
        return self.inner.replace_queue_atomic(*args, **kwargs)

    def commit_job_success(self, *args, **kwargs):
        return self.inner.commit_job_success(*args, **kwargs)

    def commit_job_failure(self, *args, **kwargs):
        return self.inner.commit_job_failure(*args, **kwargs)

    def commit_retry(self, *args, **kwargs):
        return self.inner.commit_retry(*args, **kwargs)

    def commit_job_failures(self, *args, **kwargs):
        return self.inner.commit_job_failures(*args, **kwargs)

    def commit_skip(self, *args, **kwargs):
        return self.inner.commit_skip(*args, **kwargs)

    def append_failed(self, *args, **kwargs):
        return self.inner.append_failed(*args, **kwargs)

    def enqueue_jobs(self, *args, **kwargs):
        return self.inner.enqueue_jobs(*args, **kwargs)

    def delete_queue_uids(self, *args, **kwargs):
        return self.inner.delete_queue_uids(*args, **kwargs)

    def delete_failed(self, *args, **kwargs):
        return self.inner.delete_failed(*args, **kwargs)

    def delete_successes(self, *args, **kwargs):
        return self.inner.delete_successes(*args, **kwargs)

    def seed_successes(self, *args, **kwargs):
        return self.inner.seed_successes(*args, **kwargs)

    def seed_cursor(self, *args, **kwargs):
        return self.inner.seed_cursor(*args, **kwargs)

    def append_attempt(self, *args, **kwargs):
        return self.inner.append_attempt(*args, **kwargs)

    def update_attempt(self, *args, **kwargs):
        return self.inner.update_attempt(*args, **kwargs)

    def load_attempts(self, *args, **kwargs):
        return self.inner.load_attempts(*args, **kwargs)

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


# ── fake 进程（写 IPC 结果文件，与真实 worker 同路径）──────────────────


def make_instant_process_class(retry_uid: str | None = None, recorder: list | None = None):
    """FakeProcess：start() 即写结果并退出。

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
                result = {
                    "spawned_jobs": [],
                    "resource_suspensions": [],
                    "cursor_updates": {},
                }
                if retry_uid is not None and spec.job.uid == retry_uid:
                    result.update({"status": "retry", "message": "stub failure", "consume_budget": True})
                else:
                    result.update({"status": "success", "metadata": {}})
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


def make_pipeline(tmp_path, **overrides) -> TaskLite:
    """标准测试管线（memory 后端零 IO；钩子经构造期参数传入）。"""
    defaults: dict = dict(
        name="facade_test",
        state_dir=tmp_path / "state",
        backend="memory",
    )
    defaults.update(overrides)
    return TaskLite(**defaults)


# ── 构造与装配分区 ────────────────────────────────────────────────────


class TestConstruction:
    """name/目录/后端/资源/分类器各装配段的行为等价。"""

    def test_name_must_be_non_empty_str(self, tmp_path):
        with pytest.raises(TypeError, match="name must be a non-empty str"):
            TaskLite("", tmp_path / "state")
        with pytest.raises(TypeError, match="name must be a non-empty str"):
            TaskLite(123, tmp_path / "state")

    def test_name_rejects_path_separators(self, tmp_path):
        with pytest.raises(ValueError, match="path separators"):
            TaskLite("a/b", tmp_path / "state")
        with pytest.raises(ValueError, match="path separators"):
            TaskLite("a\\b", tmp_path / "state")

    def test_state_dir_created(self, tmp_path):
        state = tmp_path / "nested" / "state"
        TaskLite("p", state, backend="memory")
        assert state.is_dir()

    def test_ipc_dir_created_under_state_dir(self, tmp_path):
        p = make_pipeline(tmp_path)
        assert Path(p.ipc_dir) == p.state_dir / "ipc"
        assert Path(p.ipc_dir).is_dir()

    def test_output_root_single_created_and_resolved(self, tmp_path):
        root = tmp_path / "out"
        p = make_pipeline(tmp_path, output_roots=root)
        assert p.output_roots == root.resolve()
        assert p.output_roots.is_dir()

    def test_output_root_multi_root_all_created(self, tmp_path):
        roots = [tmp_path / "o1", tmp_path / "o2"]
        p = make_pipeline(tmp_path, output_roots=roots)
        assert p.output_roots == [r.resolve() for r in roots]
        assert all(r.is_dir() for r in p.output_roots)

    def test_output_root_bad_type_rejected(self, tmp_path):
        with pytest.raises(TypeError, match="output_roots"):
            make_pipeline(tmp_path, output_roots=42)

    def test_backend_sqlite_default_type_name(self, tmp_path):
        p = TaskLite("sp", tmp_path / "state")
        assert p.backend_type == "sqlite"

    def test_backend_memory_type_name(self, tmp_path):
        p = make_pipeline(tmp_path)
        assert p.backend_type == "memory"

    def test_backend_instance_normalized_names(self, tmp_path):
        p = TaskLite("i", tmp_path / "state", backend=InMemoryStateBackend())
        assert p.backend_type == "memory"

    def test_backend_unknown_string_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="Unknown backend"):
            make_pipeline(tmp_path, backend="redis")

    def test_backend_bad_type_rejected(self, tmp_path):
        with pytest.raises(TypeError, match="backend must be"):
            make_pipeline(tmp_path, backend=object())

    def test_sqlite_init_failure_wraps_runtime_error(
        self, tmp_path, monkeypatch
    ):
        import tasklite.pipeline as pipeline_mod

        def boom(_path):
            raise OSError("disk full")

        monkeypatch.setattr(pipeline_mod, "SQLiteStateBackend", boom)
        with pytest.raises(OSError, match="disk full"):
            TaskLite("sp", tmp_path / "state", backend="sqlite")

    def test_max_workers_validation(self, tmp_path):
        for bad in (0, -1, True, "4", 2.5):
            with pytest.raises(ValueError, match="max_workers"):
                make_pipeline(tmp_path, max_workers=bad)

    def test_worker_resource_registered_with_capacity(self, tmp_path):
        p = make_pipeline(tmp_path, max_workers=7)
        assert "__workers__" in p.resources
        assert p.resources["__workers__"].max_capacity == 7.0

    def test_removed_exception_constructor_params_rejected(self, tmp_path):
        with pytest.raises(TypeError):
            make_pipeline(tmp_path, fatal_exceptions=(int,))
        with pytest.raises(TypeError):
            make_pipeline(tmp_path, transient_exceptions=(BizError,))

    def test_strict_picklable_flag_forwarded(self, tmp_path):
        assert make_pipeline(tmp_path).strict_picklable is True
        assert make_pipeline(tmp_path, strict_picklable=False).strict_picklable is False


# ── 注册面（六步之二/三）─────────────────────────────────────────────


class TestRegisterTask:
    """register_task 规格式/简式双形态与 TaskRegistry 契约。"""

    def test_register_task_spec_instance(self, tmp_path):
        p = make_pipeline(tmp_path)
        spec = Task("fetch", ok_handler, resources={"api": 1.0})
        p.register_task(spec)
        assert p.tasks.lookup("fetch") is spec

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
        spec = p.tasks.lookup("fetch")
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
        with pytest.raises(TypeError, match="handler is required"):
            p.register_task("fetch")

    def test_simple_form_validates_task_type(self, tmp_path):
        from tasklite import TaskLiteError
        p = make_pipeline(tmp_path)
        with pytest.raises(TaskLiteError, match="non-empty str"):
            p.register_task("", ok_handler)
        p.register_task("fe::tch", ok_handler)
        assert p.tasks.lookup("fe::tch").task_type == "fe::tch"

    def test_spec_form_rejects_stray_arguments(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), handler=ok_handler)
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), resources={"a": 1})
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), max_retries_per_run=0)
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), max_retries_per_run=3)
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), timeout_seconds=60)
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), trigger="once")
        with pytest.raises(TypeError):
            p.register_task("fetch", ok_handler, payload_schema=dict)  # type: ignore[call-arg]

    def test_duplicate_registration_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        with pytest.raises(ValueError, match="already registered"):
            p.register_task("fetch", ok_handler)

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="register_task"):
                p.register_task("fetch", ok_handler)


class TestRegisterResource:
    """register_resource：类型校验、覆盖告警、worker 并发覆盖。"""

    def test_registers_into_manager(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_resource(RateLimitResource("api", interval_seconds=0.5))
        assert "api" in p.resources

    def test_rejects_non_resource(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="Resource instance"):
            p.register_resource("api")

    def test_overwrite_warns(self, tmp_path, caplog):
        p = make_pipeline(tmp_path)
        p.register_resource(CapacityResource("gpu", 1.0))
        with caplog.at_level("WARNING"):
            p.register_resource(CapacityResource("gpu", 2.0))
        assert p.resources["gpu"].max_capacity == 2.0
        assert any("Overwriting existing resource 'gpu'" in r.message for r in caplog.records)

    def test_override_workers_changes_concurrency(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_resource(CapacityResource("__workers__", 9.0))
        assert p.resources["__workers__"].max_capacity == 9.0

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="register_resource"):
                p.register_resource(CapacityResource("gpu", 1.0))


class TestTaskTriggerDefaults:
    """触发器默认规格与显式覆盖。"""

    def test_default_trigger_injected_on_enqueue(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("discover", ok_handler, trigger="always")
        p.enqueue(Job("discover", "scan"))
        assert p.backend.load_queue()[0]["trigger"] == "always"

    def test_explicit_trigger_respected(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("discover", ok_handler, trigger="always")
        p.enqueue(Job("discover", "scan", trigger="once"))
        assert p.backend.load_queue()[0]["trigger"] == "once"

    def test_normal_task_defaults_once(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        p.enqueue(Job("fetch", "a"))
        assert p.backend.load_queue()[0]["trigger"] == "once"


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
        assert p.enqueue(Job("fetch", "a")) is True
        assert len(p.backend.load_queue()) == 1

    def test_list_or_non_job_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        for bad in ([Job("fetch", "a")], [], "bad", 42, {"task_type": "fetch", "job_id": "a"}):
            with pytest.raises(TypeError, match="expects a Job"):
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
        assert len(p.backend.load_queue()) == 1

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
        assert len(p.backend.load_queue()) == 1

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

        (row,) = p.backend.load_queue()
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

        (row,) = p.backend.load_queue()
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
        p.backend.seed_successes(["generate::001"])
        p.backend._successes["generate::001"]["artifacts"] = {
            "report": {"path": str(tmp_path / "old_report.csv")}
        }

        missing_dep = Job(
            "process",
            "001",
            files={"src": FileSpec.from_output("generate::001", "report", role="input")},
        )
        with pytest.raises(TaskLiteError, match="depends_on"):
            p.enqueue(missing_dep)
        assert p.backend.load_queue() == []

        valid_consumer = Job(
            "process",
            "001",
            depends_on=["generate::001"],
            files={"src": FileSpec.from_output("generate::001", "report", role="input")},
        )
        assert p.enqueue(valid_consumer) is True
        (queued,) = p.backend.load_queue()
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
        assert ctx.spawned_jobs == []

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
        assert len(ctx.spawned_jobs) == 1
        spawned = ctx.spawned_jobs[0]
        assert set(spawned.files.keys()) == {"src", "out"}
        assert spawned.files["src"] == FileSpec.from_output(
            "generate::001", "report", role="input"
        )
        assert spawned.files["out"] == FileSpec(
            base_dir / "out" / "result.csv", role="output"
        )

    def test_pending_recovery_across_runs_preserves_accepted_snapshot(self, tmp_path, monkeypatch):
        """跨次恢复 pending 不套用新 Task 默认。"""
        p1 = TaskLite(name="recover", state_dir=str(tmp_path), max_workers=1)
        p1.register_resource(CapacityResource("gpu", 2.0))
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

        monkeypatch.setattr(mp, "Process", CapturingProcess)
        p2 = TaskLite(name="recover", state_dir=str(tmp_path), max_workers=1)
        p2.register_resource(CapacityResource("gpu", 2.0))
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
        assert len(p2.backend.load_successes()) == 1
        assert len(observed) == 1
        assert observed[0]["resources"] == {"gpu": 1.0, "__workers__": 1.0}
        assert observed[0]["max_retries_per_run"] == 1
        assert observed[0]["timeout_seconds"] == 15
        assert observed[0]["trigger"] == "once"

    def test_wall_hit_not_blocked_at_enqueue(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.seed_successes(["fetch::done"])
        assert p.enqueue(Job("fetch", "done")) is True
        assert len(p.backend.load_queue()) == 1

    def test_intake_pipeline_injections(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.enqueue(Job("fetch", "a", timeout_seconds=17, payload={"timeout": "业务时限"}))
        (row,) = p.backend.load_queue()
        assert row["resources"]["__workers__"] == 1.0
        assert row["first_enqueued_at"]
        assert row["timeout_seconds"] == 17
        assert "timeout" not in row
        restored = Job.from_dict(row)
        assert restored.timeout_seconds == 17
        assert restored.payload == {"timeout": "业务时限"}

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="enqueue"):
                p.enqueue(Job("fetch", "a"))


class TestFilterJobsWithoutSuccess:
    """successes 过滤语义与顺序保持（委托 OpsConsole）。"""

    def test_wall_hits_filtered_order_preserved(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.seed_successes(["fetch::done1", "fetch::done2"])
        jobs = [
            Job("fetch", "new"),
            Job("fetch", "done1"),
            Job("fetch", "new2"),
            Job("fetch", "done2"),
        ]
        result = p.filter_jobs_without_success(jobs)
        assert [j.uid for j in result] == ["fetch::new", "fetch::new2"]

    def test_empty_input_returns_empty(self, tmp_path):
        assert make_pipeline(tmp_path).filter_jobs_without_success([]) == []

    def test_failed_entries_not_excluded(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.backend.append_failed("fetch::broken", {"error_message": "x"})
        jobs = [Job("fetch", "broken"), Job("fetch", "fresh")]
        assert [j.uid for j in p.filter_jobs_without_success(jobs)] == [
            "fetch::broken",
            "fetch::fresh",
        ]

    def test_enqueue_composition_only_enqueues_jobs_without_success(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.seed_successes(["fetch::done"])
        for job in p.filter_jobs_without_success([Job("fetch", "done"), Job("fetch", "new")]):
            p.enqueue(job)
        queued = {f"{r['task_type']}::{r['job_id']}" for r in p.backend.load_queue()}
        assert queued == {"fetch::new"}

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="filter_jobs_without_success"):
                p.filter_jobs_without_success([])


# ── 六步契约与运行门面 ────────────────────────────────────────────────


class TestSixStepContract:
    """初始化 → register_resource → register_task/register_transient
    → enqueue → run → stop 的完整链路（fake 进程驱动）。"""

    def test_full_six_step_flow(self, tmp_path, monkeypatch):
        p = make_pipeline(tmp_path)

        p.register_resource(CapacityResource("gpu", 2.0))
        p.register_task("fetch", ok_handler, resources={"gpu": 1.0})
        p.enqueue(Job("fetch", "a"))
        p.enqueue(Job("fetch", "b"))

        monkeypatch.setattr(mp, "Process", make_instant_process_class())
        res = p.run()
        p.stop()

        assert res is None
        assert sorted(p.backend.load_successes()) == ["fetch::a", "fetch::b"]

    def test_run_reentry_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="already in progress"):
                p.run()

    def test_stop_transitions_session_state(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            p.stop()
            assert p._runtime.stop_mode is StopMode.DRAINING
            # 幂等无副作用
            p.stop()
            assert p._runtime.stop_mode is StopMode.DRAINING

    def test_is_running_flag_lifecycle(self, tmp_path, monkeypatch):
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

        monkeypatch.setattr(mp, "Process", ObservingProcess)
        p.run()
        assert observed == [True]
        assert p.is_running is False


# ── 调度接缝构造参数（ordering_policy / requeue_policy）─────────────────────


class TestSchedulingSeamWiring:
    """调度策略门面构造参数：注入生效、默认不传行为等价。"""

    def test_ordering_constructor_arg_reverses_visit_order(self, tmp_path, monkeypatch):
        order: list[str] = []
        p = make_pipeline(
            tmp_path,
            ordering_policy=NewestFirstPolicy(),
        )
        p.register_task("fetch", ok_handler)
        for jid in ("a", "b", "c"):
            p.enqueue(Job("fetch", jid))
        monkeypatch.setattr(mp, "Process", make_instant_process_class(recorder=order))
        p.run()

        assert order == ["fetch::c", "fetch::b", "fetch::a"]

    def test_ordering_default_wiring_and_fifo_equivalence(self, tmp_path, monkeypatch):
        order: list[str] = []
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        for jid in ("a", "b", "c"):
            p.enqueue(Job("fetch", jid))
        monkeypatch.setattr(mp, "Process", make_instant_process_class(recorder=order))
        p.run()

        assert order == ["fetch::a", "fetch::b", "fetch::c"]
        # 默认解析收敛在 RunConfig.resolve：门面不传 → FIFO 装配件直达调度器
        assert isinstance(p.runtime_config.ordering_policy, FifoOrderingPolicy)
        assert p._runtime.scheduler.ordering_policy is p.runtime_config.ordering_policy

    def test_requeue_policy_constructor_arg_controls_requeue_position(
        self, tmp_path, monkeypatch
    ):
        """队尾重入队注入：flaky 首试失败后排在 later 之后（默认应在之前）。"""
        order: list[str] = []
        p = make_pipeline(
            tmp_path,
            max_workers=1,
            requeue_policy=TailRequeuePolicy(),
        )
        p.register_task("fetch", ok_handler)
        p.enqueue(Job("fetch", "flaky", max_retries_per_run=1))
        p.enqueue(Job("fetch", "later"))
        monkeypatch.setattr(
            mp, "Process", make_instant_process_class(retry_uid="fetch::flaky", recorder=order)
        )
        p.run()

        assert order == [
            "fetch::flaky", "fetch::later", "fetch::flaky",
        ]
        assert "fetch::later" in p.backend.load_successes()
        assert "fetch::flaky" in p.backend.load_failed()

    def test_requeue_default_wiring_and_front_equivalence(self, tmp_path, monkeypatch):
        """默认立即重入队（front=True）：flaky 重试先于 later 结算。"""
        order: list[str] = []
        p = make_pipeline(
            tmp_path,
            max_workers=1,
        )
        p.register_task("fetch", ok_handler)
        p.enqueue(Job("fetch", "flaky", max_retries_per_run=1))
        p.enqueue(Job("fetch", "later"))
        monkeypatch.setattr(
            mp, "Process", make_instant_process_class(retry_uid="fetch::flaky", recorder=order)
        )
        p.run()

        assert order == [
            "fetch::flaky", "fetch::flaky", "fetch::later",
        ]
        assert isinstance(p.runtime_config.requeue_policy, ImmediateRequeuePolicy)
        assert p._runtime.store._requeue_policy is p.runtime_config.requeue_policy


# ── 管理面委托（run() 外）────────────────────────────────────────────


class TestManageApis:
    """失败档案/挂起/种子/清史等运维 API 的委托与守卫。"""

    def test_list_failures_returns_structured_entries(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.backend.append_failed("fetch::bad", {"error_message": "boom", "is_fatal": False})
        entries = p.list_failures()
        assert len(entries) == 1
        assert entries[0].job_uid == "fetch::bad"
        assert entries[0].error_message == "boom"

    def test_clear_failures_removes_non_fatal(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.backend.append_failed("fetch::bad", {"error_message": "boom"})
        p.backend.append_failed("fetch::bug", {"error_message": "x", "is_fatal": True})
        removed = p.clear_failures()
        assert removed == 1
        assert sorted(p.backend.load_failed()) == ["fetch::bug"]

    def test_retry_failure_requeues_job(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        p.backend.append_failed("fetch::bad", {"error_message": "boom"})
        assert p.retry_failure(job_uid="fetch::bad") is True
        assert "fetch::bad" not in p.backend.load_failed()
        (row,) = p.backend.load_queue()
        assert row["job_id"] == "bad"

    def test_retry_failure_unknown_uid_raises(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(KeyError, match="not in failure archive"):
            p.retry_failure("fetch::nope")

    def test_list_suspensions_empty_on_fresh_state(self, tmp_path):
        assert make_pipeline(tmp_path).list_suspensions() == []

    def test_seed_wall_and_seed_cursor(self, tmp_path):
        p = make_pipeline(tmp_path)
        assert p.seed_successes(["fetch::done"]) == 1
        assert "fetch::done" in p.backend.load_successes()
        p.seed_cursor("page", "7")
        assert p.backend.load_cursors()["page"] == "7"

    def test_clear_history_removes_from_both(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.seed_successes(["fetch::ok"])
        p.backend.append_failed("fetch::bad", {"error_message": "x", "is_fatal": False})
        removed = p.clear_history(["fetch::"])
        assert removed == 2
        assert p.backend.load_successes() == {}
        assert p.backend.load_failed() == {}

    def test_run_guards_across_manage_apis(self, tmp_path):
        guarded = [
            ("list_suspensions", lambda p: p.list_suspensions()),
            ("list_failures", lambda p: p.list_failures()),
            ("clear_failures", lambda p: p.clear_failures()),
            ("clear_history", lambda p: p.clear_history("fetch::")),
            ("seed_successes", lambda p: p.seed_successes(["fetch::a"])),
            ("seed_cursor", lambda p: p.seed_cursor("k", "v")),
        ]
        for api_name, call in guarded:
            p = make_pipeline(tmp_path)
            with running(p):
                with pytest.raises(RuntimeError, match=api_name):
                    call(p)


class TestRunExecutionOptions:
    """run() 运行参数透传：引擎可配的 ExecutionOptions 在公开面可达，
    缺省与引擎默认一致（默认行为不变）。"""

    def test_default_run_acquires_reentry_lock(self, tmp_path):
        """默认等价断言：run() 照常获取 state_dir 级单运行排他锁。"""
        from tasklite.utils.lockfile import release_lock, try_acquire_lock

        p = make_pipeline(tmp_path)
        lock_fd = try_acquire_lock(p.ipc_dir, "__pipeline_run__", timeout=0)
        assert lock_fd is not None
        try:
            with pytest.raises(RuntimeError, match="Another run.*in progress"):
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


class TestBackendSwap:
    """backend property/setter：换库唯一动作 = 换 StateStore 锚点。"""

    def test_backend_setter_rebinds_runtime_store_console(self, tmp_path):
        """换库后全部持有者看到同一新实例（锚点收敛回归锁）。

        不变式：backend 所有权唯一锚定 StateStore——pipeline setter 只做
        「换 store 锚点」一个动作，其余持有者（门面 property / runtime /
        OpsConsole 管理面 / enqueue 摄入面）一律经 store.backend 只读
        派生。新增持 backend 的机器必须加入本断言——把「漏绑」从静默
        读写旧库变成测试红灯。
        """
        p = make_pipeline(tmp_path)
        new_backend = InMemoryStateBackend()
        new_backend.append_failed("fetch::x", {"error_message": "moved"})
        p.backend = new_backend
        # 持有者逐一刻画：门面 property、runtime 派生引用、store 锚点
        assert p.backend is new_backend
        assert p._runtime.backend is new_backend
        assert p._runtime.store.backend is new_backend
        # 管理面（OpsConsole 经 store 派生）：读面命中新库数据
        assert any(e.job_uid == "fetch::x" for e in p.list_failures())
        # 摄入面（enqueue 经 store 派生）：写面命中新库
        p.register_task("fetch", ok_handler)
        p.enqueue(Job("fetch", "swapped"))
        assert [jd["job_id"] for jd in new_backend.load_queue()] == ["swapped"]

    def test_backend_setter_rejects_non_backend(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="AbstractStateBackend"):
            p.backend = "sqlite"

    def test_backend_setter_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="backend"):
                p.backend = InMemoryStateBackend()

    def test_backend_setter_rejects_duck_typed_object(self, tmp_path):
        """只带读写核心方法的鸭子对象显式 TypeError 拒绝（契约面单一）。"""
        p = make_pipeline(tmp_path)

        class PartialBackend:
            def load_queue(self):
                return []

            def commit_job_success(self, job_uid, meta, **kw):
                return True

        with pytest.raises(TypeError, match="AbstractStateBackend"):
            p.backend = PartialBackend()

    def test_explicit_subclass_backend_accepted(self, tmp_path):
        """显式 AbstractStateBackend 子类假后端：接受注入并重绑持库组件。"""
        p = make_pipeline(tmp_path)
        fake = DelegatingFakeBackend()
        fake.append_failed("fetch::x", {"error_message": "subclass"})
        p.backend = fake
        assert p.backend is fake
        assert p._runtime.store.backend is fake
        assert any(e.job_uid == "fetch::x" for e in p.list_failures())
