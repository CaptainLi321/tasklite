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

from tasklite.v2.backend.base import AbstractStateBackend
from tasklite.v2.backend.memory import InMemoryStateBackend
from tasklite.v2.engine.admission import (
    ImmediateRequeuePolicy,
    RequeuePlan,
    RequeuePolicy,
)
from tasklite.v2.engine.resource import CapacityResource, RateLimitResource
from tasklite.v2.engine.scheduler import FifoOrderingPolicy, OrderingPolicy
from tasklite.v2.engine.types import AttemptFinish, ExitReason, StopMode
from tasklite.v2.models.job import Job
from tasklite.v2.models.task import Task
from tasklite.v2.pipeline import TaskLite
from tasklite.v2.testing import running
from tasklite.v2.utils.ipc import ArtifactJournal


def ok_handler(job, ctx):
    """占位 handler：成功 + 空 meta（模块级可 pickle，满足 strict 预检）。"""
    return True, {}


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

    def plan_requeue(self, job_dict, *, transient_kind=None) -> RequeuePlan:
        return RequeuePlan(front=False, delay_seconds=0.0)


class DelegatingFakeBackend(AbstractStateBackend):
    """显式契约子类假后端：全部契约方法逐一转发内部真实后端。

    伪造后端的标准形态——契约缺口在类构造期即暴露（ABC 强制对齐），
    不依赖任何鸭子接受分支。
    """

    def __init__(self) -> None:
        self.inner = InMemoryStateBackend()

    def load_wall(self, *args, **kwargs):
        return self.inner.load_wall(*args, **kwargs)

    def load_failed(self, *args, **kwargs):
        return self.inner.load_failed(*args, **kwargs)

    def load_failed_payloads(self, *args, **kwargs):
        return self.inner.load_failed_payloads(*args, **kwargs)

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

    def commit_bulk_failure(self, *args, **kwargs):
        return self.inner.commit_bulk_failure(*args, **kwargs)

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

    def delete_wall(self, *args, **kwargs):
        return self.inner.delete_wall(*args, **kwargs)

    def seed_wall(self, *args, **kwargs):
        return self.inner.seed_wall(*args, **kwargs)

    def seed_cursor(self, *args, **kwargs):
        return self.inner.seed_cursor(*args, **kwargs)

    def append_attempt(self, *args, **kwargs):
        return self.inner.append_attempt(*args, **kwargs)

    def update_attempt(self, *args, **kwargs):
        return self.inner.update_attempt(*args, **kwargs)

    def load_attempts(self, *args, **kwargs):
        return self.inner.load_attempts(*args, **kwargs)

    def get_meta(self, *args, **kwargs):
        return self.inner.get_meta(*args, **kwargs)

    def set_meta(self, *args, **kwargs):
        return self.inner.set_meta(*args, **kwargs)


# ── fake 进程（写 IPC 结果文件，与真实 worker 同路径）──────────────────


def make_instant_process_class(retry_uid: str | None = None):
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
                result = {
                    "new_jobs": [],
                    "resource_suspensions": [],
                    "cursor_updates": {},
                    "auth": spec.result_token,
                }
                if retry_uid is not None and spec.job.uid == retry_uid:
                    result.update({"status": "retry", "error": "stub failure"})
                else:
                    result.update({"status": "success", "raw_result": True})
                ArtifactJournal(spec.ipc_dir).write_result_atomic(
                    spec.job.uid,
                    result,
                    incarnation=spec.incarnation,
                )
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
        p = make_pipeline(tmp_path, output_root=root)
        assert p.output_root == root.resolve()
        assert p.output_root.is_dir()

    def test_output_root_multi_root_all_created(self, tmp_path):
        roots = [tmp_path / "o1", tmp_path / "o2"]
        p = make_pipeline(tmp_path, output_root=roots)
        assert p.output_root == [r.resolve() for r in roots]
        assert all(r.is_dir() for r in p.output_root)

    def test_output_root_bad_type_rejected(self, tmp_path):
        with pytest.raises(TypeError, match="output_root"):
            make_pipeline(tmp_path, output_root=42)

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
        import tasklite.v2.pipeline as pipeline_mod

        def boom(_path):
            raise OSError("disk full")

        monkeypatch.setattr(pipeline_mod, "SQLiteStateBackend", boom)
        with pytest.raises(RuntimeError, match="SQLite backend initialization"):
            TaskLite("sp", tmp_path / "state", backend="sqlite")

    def test_max_workers_validation(self, tmp_path):
        for bad in (0, -1, True, "4", 2.5):
            with pytest.raises(ValueError, match="max_workers"):
                make_pipeline(tmp_path, max_workers=bad)

    def test_worker_resource_registered_with_capacity(self, tmp_path):
        p = make_pipeline(tmp_path, max_workers=7)
        assert "__workers__" in p.resources
        assert p.resources["__workers__"].capacity == 7.0

    def test_declared_exceptions_validated_at_construction(self, tmp_path):
        with pytest.raises((TypeError, ValueError)):
            make_pipeline(tmp_path, fatal_exceptions=(int,))

    def test_classifier_holds_declared_exceptions(self, tmp_path):
        p = make_pipeline(tmp_path, transient_exceptions=(BizError,))
        assert BizError in p.classifier.transient_exceptions

    def test_strict_picklable_flag_forwarded(self, tmp_path):
        assert make_pipeline(tmp_path).strict_picklable is True
        assert make_pipeline(tmp_path, strict_picklable=False).strict_picklable is False


# ── 注册面（六步之二/三）─────────────────────────────────────────────


class TestRegisterTask:
    """register_task 规格式/简式双形态与 TaskRegistry 契约。"""

    def test_register_task_spec_instance(self, tmp_path):
        p = make_pipeline(tmp_path)
        spec = Task("fetch", ok_handler, default_resources={"api": 1.0})
        p.register_task(spec)
        assert p.tasks.lookup("fetch") is spec

    def test_register_task_simple_form_builds_spec(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task(
            "fetch",
            ok_handler,
            default_resources={"api": 1.0},
            max_retries=1,
            timeout=30,
            timeout_is_transient=True,
        )
        spec = p.tasks.lookup("fetch")
        assert spec.handler is ok_handler
        assert spec.default_resources == {"api": 1.0}
        assert spec.max_retries == 1
        assert spec.timeout == 30
        assert spec.timeout_is_transient is True

    def test_simple_form_requires_handler(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="handler is required"):
            p.register_task("fetch")

    def test_simple_form_validates_task_type(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(ValueError, match="'::'"):
            p.register_task("fe::tch", ok_handler)

    def test_spec_form_rejects_stray_arguments(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), handler=ok_handler)
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), default_resources={"a": 1})
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), payload_schema=dict)
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), max_retries=0)
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), timeout=60)
        with pytest.raises(ValueError, match="extra spec"):
            p.register_task(Task("fetch", ok_handler), timeout_is_transient=True)

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
        assert p.resources["gpu"].capacity == 2.0
        assert any("Overwriting existing resource 'gpu'" in r.message for r in caplog.records)

    def test_override_workers_changes_concurrency(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_resource(CapacityResource("__workers__", 9.0))
        assert p.resources["__workers__"].capacity == 9.0

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="register_resource"):
                p.register_resource(CapacityResource("gpu", 1.0))


class TestSetDiscoveryRerun:
    """任务级默认 rerun 注入面（discovery 宿主接缝）。"""

    def test_default_rerun_injected_on_enqueue(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("discover", ok_handler)
        p.set_discovery_rerun("discover", "every_run")
        p.enqueue([Job("discover", "scan")])
        (row,) = p.backend.load_queue()
        assert row["rerun"] == "every_run"

    def test_explicit_rerun_respected(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("discover", ok_handler)
        p.set_discovery_rerun("discover", "every_run")
        p.enqueue([Job("discover", "scan", rerun="never")])
        (row,) = p.backend.load_queue()
        assert row["rerun"] == "never"

    def test_non_discovery_task_default_none(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        p.enqueue([Job("fetch", "a")])
        (row,) = p.backend.load_queue()
        assert row["rerun"] is None

    def test_invalid_rerun_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(ValueError, match="rerun must be one of"):
            p.set_discovery_rerun("discover", "sometimes")

    def test_invalid_task_type_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(ValueError, match="'::'"):
            p.set_discovery_rerun("dis::cover", "every_run")

    def test_overwrite_warns(self, tmp_path, caplog):
        p = make_pipeline(tmp_path)
        p.set_discovery_rerun("discover", "every_run")
        with caplog.at_level("WARNING"):
            p.set_discovery_rerun("discover", "on_failure")
        assert any(
            "Overwriting discovery rerun" in r.message for r in caplog.records
        )


class TestRegisterTransientException:
    """瞬态异常注册委托 classifier（per-pipeline 实例态）。"""

    def test_single_class_registered(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_transient_exception(BizError)
        assert p.classifier.matches(BizError("x"))

    def test_sequence_bulk_registered(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_transient_exception((BizError, OtherBizError))
        assert p.classifier.matches(BizError())
        assert p.classifier.matches(OtherBizError())

    def test_non_exception_class_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises((TypeError, ValueError)):
            p.register_transient_exception(int)

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="register_transient_exception"):
                p.register_transient_exception(BizError)


# ── 入队与过滤（六步之四）────────────────────────────────────────────


class TestEnqueue:
    """enqueue 摄入：front 语义、wall 不拦、注入管道。"""

    def test_single_job_wrapped(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.enqueue(Job("fetch", "a"))
        assert len(p.backend.load_queue()) == 1

    def test_list_of_jobs(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.enqueue([Job("fetch", "a"), Job("fetch", "b")])
        assert len(p.backend.load_queue()) == 2

    def test_empty_list_noop(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.enqueue([])
        assert p.backend.load_queue() == []

    def test_non_job_element_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="expects Job objects"):
            p.enqueue([Job("fetch", "a"), "bad"])

    def test_non_sequence_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="expects a Job"):
            p.enqueue(42)

    def test_duplicate_uid_silently_skipped(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.enqueue([Job("fetch", "a"), Job("fetch", "a")])
        assert len(p.backend.load_queue()) == 1

    def test_front_inserts_at_head(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.enqueue([Job("fetch", "tail")])
        p.enqueue(Job("fetch", "head"), front=True)
        queue = p.backend.load_queue()
        assert queue[0]["job_id"] == "head"
        assert queue[1]["job_id"] == "tail"

    def test_wall_hit_not_blocked_at_enqueue(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.seed_wall(["fetch::done"])
        p.enqueue(Job("fetch", "done"))
        assert len(p.backend.load_queue()) == 1

    def test_intake_pipeline_injections(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.enqueue(Job("fetch", "a"))
        (row,) = p.backend.load_queue()
        assert row["resources"]["__workers__"] == 1.0
        assert row["first_enqueued_at"]

    def test_front_is_keyword_only(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError):
            p.enqueue(Job("fetch", "a"), True)

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="enqueue"):
                p.enqueue(Job("fetch", "a"))


class TestUncompleted:
    """wall 过滤语义与顺序保持（委托 OpsConsole）。"""

    def test_wall_hits_filtered_order_preserved(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.seed_wall(["fetch::done1", "fetch::done2"])
        jobs = [
            Job("fetch", "new"),
            Job("fetch", "done1"),
            Job("fetch", "new2"),
            Job("fetch", "done2"),
        ]
        result = p.uncompleted(jobs)
        assert [j.uid for j in result] == ["fetch::new", "fetch::new2"]

    def test_empty_input_returns_empty(self, tmp_path):
        assert make_pipeline(tmp_path).uncompleted([]) == []

    def test_failed_entries_not_excluded(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.backend.append_failed("fetch::broken", {"error": "x"})
        jobs = [Job("fetch", "broken"), Job("fetch", "fresh")]
        assert [j.uid for j in p.uncompleted(jobs)] == [
            "fetch::broken",
            "fetch::fresh",
        ]

    def test_enqueue_composition_only_enqueues_uncompleted(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.seed_wall(["fetch::done"])
        p.enqueue(p.uncompleted([Job("fetch", "done"), Job("fetch", "new")]))
        queued = {f"{r['task_type']}::{r['job_id']}" for r in p.backend.load_queue()}
        assert queued == {"fetch::new"}

    def test_run_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="uncompleted"):
                p.uncompleted([])


# ── 六步契约与运行门面 ────────────────────────────────────────────────


class TestSixStepContract:
    """初始化 → register_resource → register_task/register_transient
    → enqueue → run → stop 的完整链路（fake 进程驱动）。"""

    def test_full_six_step_flow(self, tmp_path, monkeypatch):
        events: list[str] = []

        def on_run_start():
            events.append("run_start")

        def on_run_end(reason):
            events.append(f"run_end:{reason}")

        def on_attempt_finished(uid, *, outcome):
            events.append(f"attempt:{uid}:{outcome.success}")

        p = make_pipeline(
            tmp_path,
            on_run_start=on_run_start,
            on_run_end=on_run_end,
            on_attempt_finished=on_attempt_finished,
        )

        p.register_resource(CapacityResource("gpu", 2.0))
        p.register_task("fetch", ok_handler, default_resources={"gpu": 1.0})
        p.register_transient_exception(BizError)
        p.enqueue([Job("fetch", "a"), Job("fetch", "b")])

        monkeypatch.setattr(mp, "Process", make_instant_process_class())
        summary = p.run()
        p.stop()

        assert summary.exit_reason is ExitReason.COMPLETED
        assert summary.stats["completed"] == 2
        assert sorted(p.backend.load_wall()) == ["fetch::a", "fetch::b"]
        assert events[0] == "run_start"
        assert events[-1] == "run_end:completed"
        assert events[1:3] == ["attempt:fetch::a:True", "attempt:fetch::b:True"]

    def test_run_summary_and_hooks_via_session(self, tmp_path, monkeypatch):
        p = make_pipeline(tmp_path, on_run_start=lambda: None)
        assert p.on_run_start is not None
        assert p.on_attempt_finished is None
        assert p.on_run_end is None
        p.register_task("fetch", ok_handler)
        p.enqueue(Job("fetch", "a"))
        monkeypatch.setattr(mp, "Process", make_instant_process_class())
        summary = p.run()
        assert summary.run_id
        assert summary.duration_seconds >= 0
        assert p.stats["completed"] == 1

    def test_run_reentry_guard(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="already in progress"):
                p.run()

    def test_stop_transitions_session_state(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.stop()
        assert p._runtime.stop_mode is StopMode.DRAINING
        p.stop(force=True)
        assert p._runtime.stop_mode is StopMode.ABORTING

    def test_run_graceful_swallows_keyboard_interrupt(self, tmp_path, monkeypatch):
        p = make_pipeline(tmp_path)

        def raise_interrupt():
            raise KeyboardInterrupt()

        monkeypatch.setattr(p._runtime, "execute", raise_interrupt)
        p.run_graceful()
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


# ── 调度接缝构造参数（ordering / requeue_policy）─────────────────────


class TestSchedulingSeamWiring:
    """调度策略门面构造参数：注入生效、默认不传行为等价。"""

    def test_ordering_constructor_arg_reverses_visit_order(self, tmp_path, monkeypatch):
        order: list[str] = []

        def on_attempt_finished(uid, *, outcome):
            order.append(uid)

        p = make_pipeline(
            tmp_path,
            ordering=NewestFirstPolicy(),
            on_attempt_finished=on_attempt_finished,
        )
        p.register_task("fetch", ok_handler)
        p.enqueue([Job("fetch", "a"), Job("fetch", "b"), Job("fetch", "c")])
        monkeypatch.setattr(mp, "Process", make_instant_process_class())
        p.run()

        assert order == ["fetch::c", "fetch::b", "fetch::a"]

    def test_ordering_default_wiring_and_fifo_equivalence(self, tmp_path, monkeypatch):
        order: list[str] = []

        def on_attempt_finished(uid, *, outcome):
            order.append(uid)

        p = make_pipeline(tmp_path, on_attempt_finished=on_attempt_finished)
        p.register_task("fetch", ok_handler)
        p.enqueue([Job("fetch", "a"), Job("fetch", "b"), Job("fetch", "c")])
        monkeypatch.setattr(mp, "Process", make_instant_process_class())
        p.run()

        assert order == ["fetch::a", "fetch::b", "fetch::c"]
        # 默认解析收敛在 RunConfig.resolve：门面不传 → FIFO 装配件直达调度器
        assert isinstance(p.runtime_config.ordering, FifoOrderingPolicy)
        assert p._runtime.scheduler.ordering is p.runtime_config.ordering

    def test_requeue_policy_constructor_arg_controls_requeue_position(
        self, tmp_path, monkeypatch
    ):
        """队尾重入队注入：flaky 首试失败后排在 later 之后（默认应在之前）。"""
        order: list[tuple[str, bool]] = []

        def on_attempt_finished(uid, *, outcome):
            order.append((uid, outcome.going_to_retry))

        p = make_pipeline(
            tmp_path,
            max_workers=1,
            requeue_policy=TailRequeuePolicy(),
            on_attempt_finished=on_attempt_finished,
        )
        p.register_task("fetch", ok_handler)
        p.enqueue([Job("fetch", "flaky", max_retries=1), Job("fetch", "later")])
        monkeypatch.setattr(
            mp, "Process", make_instant_process_class(retry_uid="fetch::flaky")
        )
        p.run()

        assert [uid for uid, _ in order] == [
            "fetch::flaky", "fetch::later", "fetch::flaky",
        ]
        assert order[0][1] is True
        assert order[2][1] is False
        assert "fetch::later" in p.backend.load_wall()
        assert "fetch::flaky" in p.backend.load_failed()

    def test_requeue_default_wiring_and_front_equivalence(self, tmp_path, monkeypatch):
        """默认立即重入队（front=True）：flaky 重试先于 later 结算。"""
        order: list[str] = []

        def on_attempt_finished(uid, *, outcome):
            order.append(uid)

        p = make_pipeline(
            tmp_path,
            max_workers=1,
            on_attempt_finished=on_attempt_finished,
        )
        p.register_task("fetch", ok_handler)
        p.enqueue([Job("fetch", "flaky", max_retries=1), Job("fetch", "later")])
        monkeypatch.setattr(
            mp, "Process", make_instant_process_class(retry_uid="fetch::flaky")
        )
        p.run()

        assert order == ["fetch::flaky", "fetch::flaky", "fetch::later"]
        assert isinstance(p.runtime_config.requeue_policy, ImmediateRequeuePolicy)
        assert p._runtime.store._requeue_policy is p.runtime_config.requeue_policy


# ── 管理面委托（run() 外）────────────────────────────────────────────


class TestManageApis:
    """失败档案/挂起/种子/清史等运维 API 的委托与守卫。"""

    def test_list_failures_returns_structured_entries(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.backend.append_failed("fetch::bad", {"error": "boom", "fatal": False})
        entries = p.list_failures()
        assert len(entries) == 1
        assert entries[0].uid == "fetch::bad"
        assert entries[0].error == "boom"

    def test_clear_failures_removes_non_fatal(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.backend.append_failed("fetch::bad", {"error": "boom"})
        p.backend.append_failed("fetch::bug", {"error": "x", "fatal": True})
        removed = p.clear_failures()
        assert removed == 1
        assert sorted(p.backend.load_failed()) == ["fetch::bug"]

    def test_retry_failure_requeues_job(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_task("fetch", ok_handler)
        p.backend.append_failed("fetch::bad", {"error": "boom"})
        assert p.retry_failure("fetch::bad") is True
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
        assert p.seed_wall(["fetch::done"]) == 1
        assert "fetch::done" in p.backend.load_wall()
        p.seed_cursor("page", "7")
        assert p.backend.load_cursors()["page"] == "7"

    def test_clear_history_removes_from_both(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.seed_wall(["fetch::ok"])
        p.backend.append_failed("fetch::bad", {"error": "x", "fatal": False})
        removed = p.clear_history(["fetch::"])
        assert removed == 2
        assert p.backend.load_wall() == {}
        assert p.backend.load_failed() == {}

    def test_run_guards_across_manage_apis(self, tmp_path):
        guarded = [
            ("list_suspensions", lambda p: p.list_suspensions()),
            ("list_failures", lambda p: p.list_failures()),
            ("clear_failures", lambda p: p.clear_failures()),
            ("clear_history", lambda p: p.clear_history("fetch::")),
            ("seed_wall", lambda p: p.seed_wall(["fetch::a"])),
            ("seed_cursor", lambda p: p.seed_cursor("k", "v")),
        ]
        for api_name, call in guarded:
            p = make_pipeline(tmp_path)
            with running(p):
                with pytest.raises(RuntimeError, match=api_name):
                    call(p)


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
        new_backend.append_failed("fetch::x", {"error": "moved"})
        p.backend = new_backend
        # 持有者逐一刻画：门面 property、runtime 派生引用、store 锚点
        assert p.backend is new_backend
        assert p._runtime.backend is new_backend
        assert p._runtime.store.backend is new_backend
        # 管理面（OpsConsole 经 store 派生）：读面命中新库数据
        assert any(e.uid == "fetch::x" for e in p.list_failures())
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

            def commit_job_success(self, uid, meta, **kw):
                return True

        with pytest.raises(TypeError, match="AbstractStateBackend"):
            p.backend = PartialBackend()

    def test_explicit_subclass_backend_accepted(self, tmp_path):
        """显式 AbstractStateBackend 子类假后端：接受注入并重绑持库组件。"""
        p = make_pipeline(tmp_path)
        fake = DelegatingFakeBackend()
        fake.append_failed("fetch::x", {"error": "subclass"})
        p.backend = fake
        assert p.backend is fake
        assert p._runtime.store.backend is fake
        assert any(e.uid == "fetch::x" for e in p.list_failures())
