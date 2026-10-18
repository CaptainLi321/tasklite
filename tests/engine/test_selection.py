"""执行选择（Selection）：默认全执行、按 task_type 留队、正常收敛与跨 run 语义。

run 路径用确定性 fake 进程驱动（start() 直接写 IPC 结果文件，与真实子进程
共享同一收割代码路径），不启动真实子进程、不触网。
"""
from __future__ import annotations

import inspect
import multiprocessing as mp
import threading

import pytest

from tasklite import ERR_MALFORMED_JOB, Exclude, ExitReason, Job, TaskLite
from tasklite.engine.resource import Resource
from tasklite.engine.scheduler import JobScheduler
from tasklite.engine.types import EMPTY_STATS
from tasklite.models.state import PipelineState
from tasklite.Selection import Selection
from tasklite.testing import running
from tasklite.utils.ipc import ArtifactJournal


def ok_handler(job, ctx):
    return True, {}


class CountingResource(Resource):
    """记录 acquire 次数的容量资源（验证未选中作业零资源消耗）。"""

    def __init__(self, name: str = "gpu") -> None:
        super().__init__(name)
        self.acquires = 0
        self.used = 0.0

    def can_acquire(self, amount: float) -> tuple[bool, float]:
        return True, 0.0

    def acquire(self, amount: float) -> None:
        self.acquires += 1
        self.used += amount

    def release(self, amount: float) -> None:
        self.used -= amount

    def suspend(self, seconds: float) -> None:
        return None


def _fake_process_class(*, fail_uid=None, spawn_jobs=()):
    """FakeProcess：start() 即写 IPC 结果；可选指定失败 uid 与派生新作业。"""

    class InstantProcess:
        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self.args = args
            self.exitcode = 0

        def start(self):
            spec = self.args[0]
            if spec.job.uid == fail_uid:
                result = {
                    "status": "fatal",
                    "error": "FatalError: boom",
                    "traceback": "",
                }
            else:
                result = {
                    "status": "success",
                    "raw_result": True,
                    "new_jobs": [dict(job) for job in spawn_jobs],
                    "resource_suspensions": [],
                    "cursor_updates": {},
                }
            result["auth"] = spec.result_token
            ArtifactJournal(spec.ipc_dir).write_result_atomic(
                spec.job.uid, result, incarnation=spec.incarnation
            )

        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

        def kill(self):
            self.exitcode = -9

        def close(self):
            pass

    return InstantProcess


@pytest.fixture()
def fake_process(monkeypatch):
    monkeypatch.setattr(mp, "Process", _fake_process_class())


def _pipeline(root, name: str = "selection", **overrides) -> TaskLite:
    return TaskLite(name=name, state_dir=root / "state", backend="memory", **overrides)


def _uids(rows) -> list[str]:
    return [f"{row['task_type']}::{row['job_id']}" for row in rows]


def _queued(pipeline: TaskLite) -> list[str]:
    return _uids(pipeline.backend.load_queue())


def test_default_and_empty_rule_selection_execute_everything(fake_process, tmp_path):
    def run(reset: bool):
        pipeline = _pipeline(tmp_path / ("reset" if reset else "default"))
        pipeline.register_task("fetch", ok_handler)
        pipeline.register_task("convert", ok_handler)
        pipeline.enqueue([Job("fetch", "a"), Job("convert", "b")])
        if reset:
            pipeline.selection()  # 0 参数 = 重置为默认全执行
        summary = pipeline.run()
        return (
            summary.exit_reason,
            dict(summary.stats),
            sorted(pipeline.backend.load_wall()),
            _queued(pipeline),
        )

    default = run(False)
    assert default[0] is ExitReason.COMPLETED
    assert default[1]["completed"] == 2
    assert default[2] == ["convert::b", "fetch::a"]
    assert default[3] == []
    assert run(True) == default  # 空规则与扩展前逐字节等价


def test_excluded_and_downstream_blocked_jobs_stay_queued(fake_process, tmp_path):
    seen: list[str] = []
    pipeline = _pipeline(
        tmp_path, on_attempt_finished=lambda uid, *, outcome: seen.append(uid)
    )
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("convert", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(
        [
            Job("fetch", "a"),
            Job("convert", "b"),
            Job("rank", "r", depends_on=["convert::b"]),
        ]
    )
    before = {uid: row for uid, row in zip(_queued(pipeline), pipeline.backend.load_queue())}
    pipeline.selection(Exclude(type="convert"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    assert summary.stats["completed"] == 1
    assert summary.stats["failed"] == 0 and summary.stats["skipped"] == 0
    assert sorted(pipeline.backend.load_wall()) == ["fetch::a"]
    assert pipeline.backend.load_failed() == {} and pipeline.list_failures() == []
    assert _queued(pipeline) == ["convert::b", "rank::r"]
    # 留队行原样保留（pop 只改变剩余行顺序）：不写 attempts、不动实例位/预算位。
    kept = {uid: row for uid, row in zip(_queued(pipeline), pipeline.backend.load_queue())}
    assert kept == {uid: before[uid] for uid in ("convert::b", "rank::r")}
    attempts = pipeline.backend.load_attempts
    assert [attempts("convert::b"), attempts("rank::r")] == [[], []]
    assert seen == ["fetch::a"]


def test_only_excluded_jobs_finish_completed_without_failures(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("convert", ok_handler)
    pipeline.enqueue([Job("convert", "a"), Job("convert", "b"), Job("convert", "c")])
    pipeline.selection(Exclude(type="convert"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    assert dict(summary.stats) == dict(EMPTY_STATS)
    assert pipeline.backend.load_wall() == {}
    assert pipeline.backend.load_failed() == {}
    assert sorted(_queued(pipeline)) == ["convert::a", "convert::b", "convert::c"]
    # 再次 run 仍为正常完成（不空转、不判死锁、不写失败档案）。
    assert pipeline.run().exit_reason is ExitReason.COMPLETED
    assert pipeline.backend.load_failed() == {}


def test_selection_chains_replaces_and_persists_across_runs(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("convert", ok_handler)
    pipeline.enqueue([Job("fetch", "a"), Job("convert", "b")])
    # 链式调用 + 每次整体替换：最终只剩 convert 规则，fetch 恢复可执行。
    assert pipeline.selection(Exclude(type="fetch")) is pipeline
    assert pipeline.selection(Exclude(type="convert")) is pipeline

    assert pipeline.run().stats["completed"] == 1
    second = pipeline.run()  # 规则是 pipeline 属性：下一次 run 仍生效
    assert second.exit_reason is ExitReason.COMPLETED
    assert second.stats["completed"] == 0
    assert _queued(pipeline) == ["convert::b"]

    pipeline.selection()  # 重置后下一次 run 消费留队作业
    third = pipeline.run()
    assert third.stats["completed"] == 1
    assert sorted(pipeline.backend.load_wall()) == ["convert::b", "fetch::a"]
    assert _queued(pipeline) == []


def test_selection_call_during_run_raises(tmp_path):
    pipeline = _pipeline(tmp_path)
    with running(pipeline):
        with pytest.raises(RuntimeError, match="selection"):
            pipeline.selection(Exclude(type="fetch"))
        with pytest.raises(RuntimeError, match="selection"):  # 引擎注入点双保险
            pipeline._runtime.set_selection(None)


def test_exclude_validation_and_unknown_task_type(fake_process, tmp_path):
    with pytest.raises(TypeError):
        Exclude(type=123)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Exclude(type="")
    with pytest.raises(ValueError):
        Exclude(type="a::b")
    with pytest.raises(TypeError):
        Selection([object()])

    # 格式合法但未注册的 task_type：不报错、无效果（规则可先于注册表配置）。
    pipeline = _pipeline(tmp_path)
    pipeline.selection(Exclude(type="never_registered"))
    pipeline.register_task("fetch", ok_handler)
    pipeline.enqueue(Job("fetch", "a"))
    assert pipeline.run().stats["completed"] == 1


def test_excluded_job_does_not_consume_resources(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    gpu = CountingResource()
    pipeline.register_resource(gpu)
    pipeline.register_task("heavy", ok_handler, default_resources={"gpu": 1.0})
    pipeline.register_task("light", ok_handler)
    pipeline.enqueue([Job("heavy", "h"), Job("light", "l")])
    pipeline.selection(Exclude(type="heavy"))

    summary = pipeline.run()

    assert summary.stats["completed"] == 1
    assert gpu.acquires == 0 and gpu.used == 0.0
    assert _queued(pipeline) == ["heavy::h"]


def test_scheduler_skips_excluded_before_stall_facts():
    allowed = Job("rank", "r").to_dict()
    excluded = Job("pdf_to_md", "p").to_dict()
    blocked = Job("push", "x", depends_on=["pdf_to_md::p"]).to_dict()
    state = PipelineState(wall={}, failed={}, cursors={}, queue=[excluded, blocked, allowed])

    scheduler = JobScheduler(resources={})
    scheduler.selection = Selection([Exclude("pdf_to_md")])
    result = scheduler.scan_next_runnable(state, frozenset())
    assert result.is_runnable is True and result.runnable_idx == 2

    only_stalled = PipelineState(wall={}, failed={}, cursors={}, queue=[excluded, blocked])
    stalled = scheduler.scan_next_runnable(only_stalled, frozenset())
    assert stalled.kind == "none"
    assert stalled.attribution.has_deadlock_causes is False
    assert stalled.has_potential_spawners is False
    assert stalled.waiting_for_dependency is True
    assert stalled.min_wait == float("inf")
    # 默认（无选择）行为与扩展前一致：队首即可运行。
    assert JobScheduler(resources={}).scan_next_runnable(state, frozenset()).runnable_idx == 0


def test_selection_rules_require_matches_type_contract():
    class ByType:
        def __init__(self, task_type):
            self.task_type = task_type

        def matches_type(self, task_type):
            return task_type == self.task_type

    selection = Selection([ByType("pdf_to_md")])
    assert selection.is_active is True
    assert selection.allows_type("pdf_to_md") is False
    assert selection.allows_type("rank") is True
    assert Selection().is_active is False
    assert Selection().allows_type("anything") is True

    with pytest.raises(TypeError, match="matches_type"):
        Selection([object()])

    class LegacyMatchesOnly:  # 旧形态规则必须 fail-loud，不允许静默漏判
        def matches(self, job):
            return True

    with pytest.raises(TypeError, match="matches_type"):
        Selection([LegacyMatchesOnly()])


def test_public_surface_exports_only_exclude():
    import tasklite

    assert "Exclude" in tasklite.__all__
    assert "Selection" not in tasklite.__all__
    assert "Rule" not in tasklite.__all__
    # 子模块名不被同名类覆盖：from tasklite.Selection import Exclude 保持可用。
    assert tasklite.Selection.__name__ == "tasklite.Selection"
    assert tasklite.Selection.Exclude is Exclude


def test_failed_parent_cascade_keeps_excluded_downstream(monkeypatch, tmp_path):
    monkeypatch.setattr(mp, "Process", _fake_process_class(fail_uid="fetch::parent"))
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("downstream", ok_handler)
    pipeline.enqueue(
        [
            Job("fetch", "parent", max_retries=0),
            Job("excluded_t", "child", depends_on=["fetch::parent"]),
            Job("downstream", "grand", depends_on=["excluded_t::child"]),
        ]
    )
    pipeline.selection(Exclude(type="excluded_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    assert sorted(pipeline.backend.load_failed()) == ["fetch::parent"]
    assert summary.stats["failed"] == 1
    assert summary.stats["cascade_failed"] == 0
    assert sorted(_queued(pipeline)) == ["downstream::grand", "excluded_t::child"]
    assert pipeline.backend.load_attempts("excluded_t::child") == []


def test_governor_cycles_do_not_archive_excluded_jobs(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(
        [
            Job("excluded_t", "a", depends_on=["excluded_t::a"]),
            Job("rank", "c", depends_on=["rank::d"]),
            Job("rank", "d", depends_on=["rank::c"]),
        ]
    )
    pipeline.selection(Exclude(type="excluded_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    # 允许作业的依赖环照旧进失败档案；被排除的自环作业留队、不进 failed。
    assert sorted(pipeline.backend.load_failed()) == ["rank::c", "rank::d"]
    assert _queued(pipeline) == ["excluded_t::a"]
    assert summary.stats["failed"] == 2

    # 只剩被排除的自环作业时：批量失败过滤为空 → 下一拍正常收敛，队列保留。
    only = _pipeline(tmp_path / "only")
    only.enqueue([Job("excluded_t", "a", depends_on=["excluded_t::a"])])
    only.selection(Exclude(type="excluded_t"))
    only_summary = only.run()
    assert only_summary.exit_reason is ExitReason.COMPLETED
    assert dict(only_summary.stats) == dict(EMPTY_STATS)
    assert only.backend.load_failed() == {}
    assert _queued(only) == ["excluded_t::a"]


def test_spawned_excluded_child_stays_queued(monkeypatch, tmp_path):
    spawned = Job("excluded_t", "child").to_dict()
    monkeypatch.setattr(mp, "Process", _fake_process_class(spawn_jobs=[spawned]))
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("fetch", ok_handler)
    pipeline.enqueue(Job("fetch", "parent"))
    pipeline.selection(Exclude(type="excluded_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    assert sorted(pipeline.backend.load_wall()) == ["fetch::parent"]
    assert pipeline.backend.load_failed() == {}
    assert _queued(pipeline) == ["excluded_t::child"]
    assert pipeline.backend.load_attempts("excluded_t::child") == []


def test_malformed_excluded_row_finishes_via_existing_failure_semantics(
    fake_process, tmp_path
):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("rank", ok_handler)
    pipeline.selection(Exclude(type="excluded_t"))
    # 畸形行（缺 job_id）带被排除 task_type：不得被选择豁免，仍走 MALFORMED_JOB。
    pipeline.backend.save_queue(
        [{"task_type": "excluded_t"}, Job("rank", "ok").to_dict()]
    )
    outcome: dict = {}

    def run_pipeline():
        outcome["summary"] = pipeline.run(install_signals=False)

    worker = threading.Thread(target=run_pipeline, daemon=True)
    worker.start()
    worker.join(timeout=20)
    assert not worker.is_alive(), "selection 下畸形队列行导致 run() 挂死（忙旋）"

    summary = outcome["summary"]
    assert summary.exit_reason is ExitReason.COMPLETED
    assert sorted(pipeline.backend.load_wall()) == ["rank::ok"]
    failures = pipeline.list_failures()
    assert len(failures) == 1
    assert failures[0].uid.startswith("_unknown::")
    assert failures[0].meta["error"] == ERR_MALFORMED_JOB
    assert pipeline.backend.load_queue() == []


def test_mixed_deadlock_keeps_excluded_and_fails_allowed(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.enqueue(
        [
            Job("excluded_t", "a"),
            Job("rank", "unknown", resources={"ghost": 1.0}),
        ]
    )
    pipeline.selection(Exclude(type="excluded_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    # 允许作业的未知资源按既有死锁归因进失败档案；被排除作业留队。
    assert sorted(pipeline.backend.load_failed()) == ["rank::unknown"]
    assert summary.stats["failed"] == 1
    assert _queued(pipeline) == ["excluded_t::a"]


def test_run_signatures_unchanged():
    assert str(inspect.signature(TaskLite.run)) == (
        "(self, *, install_signals: 'bool' = True, "
        "acquire_run_lock: 'bool' = True) -> 'RunSummary'"
    )
    assert str(inspect.signature(TaskLite.run_graceful)) == "(self) -> 'None'"
