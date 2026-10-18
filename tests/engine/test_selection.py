"""执行选择（Selection）：Exclude 延迟留队与 Skip 终态跳过的语义锁定。

run 路径用确定性 fake 进程驱动（start() 直接写 IPC 结果文件，与真实子进程
共享同一收割代码路径），不启动真实子进程、不触网。
"""
from __future__ import annotations

import inspect
import multiprocessing as mp
import threading

import pytest

from tasklite import ERR_MALFORMED_JOB, Exclude, ExitReason, Job, Selection, Skip, TaskLite
from tasklite.engine.errorclass import (
    ERR_DEPENDENCY_DEADLOCK,
    ERR_DEPENDENCY_SKIPPED,
    ERR_SKIPPED,
)
from tasklite.engine.resource import Resource
from tasklite.engine.scheduler import JobScheduler
from tasklite.engine.types import EMPTY_STATS
from tasklite.models.state import PipelineState
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


def _attempt_errors(pipeline: TaskLite, uid: str) -> list[str]:
    return [attempt.error or "" for attempt in pipeline.backend.load_attempts(uid)]


# ── 默认全执行 / Exclude 延迟语义 ─────────────────────────────────────


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


def test_excluded_and_downstream_jobs_stay_queued_then_auto_retry(
    fake_process, tmp_path
):
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
    assert summary.stats["excluded_blocked"] == 1  # rank::r（下游阻塞）
    assert sorted(pipeline.backend.load_wall()) == ["fetch::a"]
    assert pipeline.backend.load_failed() == {} and pipeline.list_failures() == []
    assert _queued(pipeline) == ["convert::b", "rank::r"]
    # 留队行原样保留：不写 attempts、不动实例位/预算位。
    kept = {uid: row for uid, row in zip(_queued(pipeline), pipeline.backend.load_queue())}
    assert kept == {uid: before[uid] for uid in ("convert::b", "rank::r")}
    attempts = pipeline.backend.load_attempts
    assert [attempts("convert::b"), attempts("rank::r")] == [[], []]
    assert seen == ["fetch::a"]

    # 下一次 run（去掉 Exclude）自动恢复：留队下游全链执行完成。
    pipeline.selection()
    second = pipeline.run()
    assert second.exit_reason is ExitReason.COMPLETED
    assert second.stats["completed"] == 2
    assert sorted(pipeline.backend.load_wall()) == [
        "convert::b",
        "fetch::a",
        "rank::r",
    ]
    assert _queued(pipeline) == []


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
        Skip(type=123)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Skip(type="a::b")
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


# ── Exclude 停摆的拓扑剥离（缺陷 1、2 回归）───────────────────────────


def test_exclude_downstream_cycle_is_failed_not_masked(fake_process, tmp_path):
    """允许作业 b<->c 的环即便挂在 Exclude 下游，也必须按死锁归因熔断。"""
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(
        [
            Job("excluded_t", "a"),
            Job("rank", "b", depends_on=["excluded_t::a", "rank::c"]),
            Job("rank", "c", depends_on=["rank::b"]),
        ]
    )
    pipeline.selection(Exclude(type="excluded_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    failed = pipeline.backend.load_failed()
    assert sorted(failed) == ["rank::b", "rank::c"]
    assert all(ERR_DEPENDENCY_DEADLOCK in meta["error"] for meta in failed.values())
    assert summary.stats["failed"] == 2
    # 被排除作业不被环归因波及，原样留队。
    assert _queued(pipeline) == ["excluded_t::a"]

    # 下一次 run 去掉 Exclude：t 自行执行，已熔断的环成员保持失败档案终态。
    pipeline.selection()
    second = pipeline.run()
    assert second.stats["completed"] == 1
    assert "excluded_t::a" in pipeline.backend.load_wall()
    assert sorted(pipeline.backend.load_failed()) == ["rank::b", "rank::c"]


def test_exclude_downstream_self_loop_is_failed(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(
        [
            Job("excluded_t", "a"),
            Job("rank", "b", depends_on=["excluded_t::a", "rank::b"]),
        ]
    )
    pipeline.selection(Exclude(type="excluded_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    failed = pipeline.backend.load_failed()
    assert list(failed) == ["rank::b"]
    assert ERR_DEPENDENCY_DEADLOCK in failed["rank::b"]["error"]
    assert _queued(pipeline) == ["excluded_t::a"]


def test_exclude_downstream_ghost_dependency_is_failed(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(
        [
            Job("excluded_t", "a"),
            Job("rank", "b", depends_on=["excluded_t::a", "ghost::x"]),
        ]
    )
    pipeline.selection(Exclude(type="excluded_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    failed = pipeline.backend.load_failed()
    assert list(failed) == ["rank::b"]
    assert "DEPENDENCY_DEADLOCK" in failed["rank::b"]["error"]
    assert _queued(pipeline) == ["excluded_t::a"]


def test_exclude_downstream_blocks_completes_without_busy_spin(fake_process, tmp_path):
    """仅剩「排除 + 仅被其阻塞」时立即正常收尾，不得 0.5s 无限忙旋。"""
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(
        [
            Job("excluded_t", "a"),
            Job("rank", "b", depends_on=["excluded_t::a"]),
            Job("rank", "c", depends_on=["rank::b"]),
        ]
    )
    pipeline.selection(Exclude(type="excluded_t"))
    outcome: dict = {}

    def run_pipeline():
        outcome["summary"] = pipeline.run(install_signals=False)

    worker = threading.Thread(target=run_pipeline, daemon=True)
    worker.start()
    worker.join(timeout=10)
    assert not worker.is_alive(), "Exclude 下游停摆导致 run() 挂死（忙旋）"

    summary = outcome["summary"]
    assert summary.exit_reason is ExitReason.COMPLETED
    assert summary.stats["excluded_blocked"] == 2
    assert summary.stats["failed"] == 0 and summary.stats["skipped"] == 0
    assert pipeline.backend.load_failed() == {}
    assert _queued(pipeline) == ["excluded_t::a", "rank::b", "rank::c"]


def test_governor_cycles_do_not_archive_excluded_jobs(fake_process, tmp_path):
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

    # 只剩被排除的自环作业时：选择仲裁直接正常收敛，队列保留。
    only = _pipeline(tmp_path / "only")
    only.register_task("excluded_t", ok_handler)
    only.enqueue([Job("excluded_t", "a", depends_on=["excluded_t::a"])])
    only.selection(Exclude(type="excluded_t"))
    only_summary = only.run()
    assert only_summary.exit_reason is ExitReason.COMPLETED
    assert dict(only_summary.stats) == dict(EMPTY_STATS)
    assert only.backend.load_failed() == {}
    assert _queued(only) == ["excluded_t::a"]


def test_mixed_deadlock_keeps_excluded_and_fails_allowed(fake_process, tmp_path):
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


# ── Skip 终态跳过与级联 ───────────────────────────────────────────────


def test_skip_root_and_downstream_are_terminal_skipped(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("skip_t", ok_handler)
    pipeline.register_task("mid", ok_handler)
    pipeline.register_task("leaf", ok_handler)
    pipeline.enqueue(
        [
            Job("skip_t", "a"),
            Job("mid", "b", depends_on=["skip_t::a"]),
            Job("leaf", "c", depends_on=["mid::b"]),
            Job("fetch", "f"),
        ]
    )
    pipeline.selection(Skip(type="skip_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    assert summary.stats["completed"] == 1
    assert summary.stats["skipped"] == 3
    assert summary.stats["failed"] == 0 and summary.stats["cascade_failed"] == 0
    # 跳过是终态：不进 wall、不进失败档案、也不留在 queue。
    assert sorted(pipeline.backend.load_wall()) == ["fetch::f"]
    assert pipeline.backend.load_failed() == {}
    assert _queued(pipeline) == []
    assert _attempt_errors(pipeline, "skip_t::a")[-1] == ERR_SKIPPED
    assert _attempt_errors(pipeline, "mid::b")[-1].startswith(ERR_DEPENDENCY_SKIPPED)
    assert _attempt_errors(pipeline, "leaf::c")[-1].startswith(ERR_DEPENDENCY_SKIPPED)
    # 下次 run 不会重试（不在队列、不在失败档案）。
    second = pipeline.run()
    assert dict(second.stats) == dict(EMPTY_STATS)


def test_skip_cascade_wins_over_deadlock_cycle(fake_process, tmp_path):
    """Skip 子树内的依赖环按级联跳过归因：Skip 永久语义优先于拓扑死锁。

    判定优先级：Skip 级联阶段先于停摆仲裁执行。b 与 c 即便成环，也因 Skip
    上游永久不可执行而整链终态跳过；环只在「允许作业」残图中才由 governor
    归因（对照 test_exclude_downstream_cycle_is_failed_not_masked）。
    """
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("skip_t", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(
        [
            Job("skip_t", "s"),
            Job("rank", "b", depends_on=["skip_t::s", "rank::c"]),
            Job("rank", "c", depends_on=["rank::b"]),
        ]
    )
    pipeline.selection(Skip(type="skip_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    assert summary.stats["skipped"] == 3
    assert summary.stats["failed"] == 0
    assert _queued(pipeline) == []
    assert pipeline.backend.load_failed() == {}
    assert _attempt_errors(pipeline, "skip_t::s")[-1] == ERR_SKIPPED
    cycle_errors = (
        _attempt_errors(pipeline, "rank::b")[-1]
        + _attempt_errors(pipeline, "rank::c")[-1]
    )
    assert cycle_errors.count(ERR_DEPENDENCY_SKIPPED) == 2
    assert ERR_DEPENDENCY_DEADLOCK not in cycle_errors
    # 再次 run 无残留、无重试。
    assert dict(pipeline.run().stats) == dict(EMPTY_STATS)


def test_skip_wins_over_exclude_for_upstream_type(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("skip_t", ok_handler)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(
        [
            Job("skip_t", "a"),
            Job("excluded_t", "b", depends_on=["skip_t::a"]),
            Job("rank", "c", depends_on=["excluded_t::b"]),
        ]
    )
    pipeline.selection(Skip(type="skip_t"), Exclude(type="excluded_t"))

    summary = pipeline.run()

    # Skip 的终态意图优先：Exclude 命中的下游 b 也永久不可完成，整链跳过。
    assert summary.stats["skipped"] == 3
    assert summary.stats["failed"] == 0
    assert _queued(pipeline) == []
    assert pipeline.backend.load_failed() == {}


def test_skip_and_exclude_same_type_skip_wins(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("t", ok_handler)
    pipeline.enqueue([Job("t", "a")])
    pipeline.selection(Skip(type="t"), Exclude(type="t"))

    summary = pipeline.run()

    assert summary.stats["skipped"] == 1
    assert _queued(pipeline) == []
    assert pipeline.backend.load_wall() == {}


def test_skip_only_queue_completes_without_execution(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("skip_t", ok_handler)
    pipeline.enqueue([Job("skip_t", "a"), Job("skip_t", "b")])
    pipeline.selection(Skip(type="skip_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    assert summary.stats["skipped"] == 2
    assert summary.stats["completed"] == 0
    assert _queued(pipeline) == []
    assert pipeline.backend.load_wall() == {} and pipeline.backend.load_failed() == {}
    assert pipeline.run().stats["skipped"] == 0


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


def test_spawned_skip_child_is_terminal_skipped(monkeypatch, tmp_path):
    spawned = Job("skip_t", "child").to_dict()
    monkeypatch.setattr(mp, "Process", _fake_process_class(spawn_jobs=[spawned]))
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("skip_t", ok_handler)
    pipeline.enqueue(Job("fetch", "parent"))
    pipeline.selection(Skip(type="skip_t"))

    summary = pipeline.run()

    assert summary.exit_reason is ExitReason.COMPLETED
    assert summary.stats["completed"] == 1
    assert summary.stats["skipped"] == 1
    assert sorted(pipeline.backend.load_wall()) == ["fetch::parent"]
    assert _queued(pipeline) == []
    assert _attempt_errors(pipeline, "skip_t::child")[-1] == ERR_SKIPPED


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


# ── 规则对象与调度器接线 ─────────────────────────────────────────────


def test_scheduler_skips_selected_before_stall_facts():
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
    # Skip 命中也由调度器整体跳过（由运行期跳过阶段收敛为终态）。
    skipped = Job("skip_t", "s").to_dict()
    scheduler.selection = Selection([Skip("skip_t")])
    result = scheduler.scan_next_runnable(
        PipelineState(wall={}, failed={}, cursors={}, queue=[skipped, allowed]),
        frozenset(),
    )
    assert result.runnable_idx == 1
    # 默认（无选择）行为与扩展前一致：队首即可运行。
    assert JobScheduler(resources={}).scan_next_runnable(state, frozenset()).runnable_idx == 0


def test_selection_rules_contract_and_precedence():
    class ByType:
        def __init__(self, task_type):
            self.task_type = task_type

        def matches_type(self, task_type):
            return task_type == self.task_type

    selection = Selection([ByType("pdf_to_md")])
    assert selection.is_active is True
    # 非 Skip 形状的自定义规则归入 Exclude 语义。
    assert selection.is_excluded("pdf_to_md") is True
    assert selection.is_skipped("pdf_to_md") is False
    assert selection.allows_type("pdf_to_md") is False
    assert selection.allows_type("rank") is True
    assert Selection().is_active is False
    assert Selection().allows_type("anything") is True

    precedence = Selection([Skip("t"), Exclude("t")])
    assert precedence.is_skipped("t") is True and precedence.is_excluded("t") is False
    assert precedence.allows_type("t") is False
    assert precedence.has_skip_rules is True

    with pytest.raises(TypeError, match="matches_type"):
        Selection([object()])

    class LegacyMatchesOnly:  # 旧形态规则必须 fail-loud，不允许静默漏判
        def matches(self, job):
            return True

    with pytest.raises(TypeError, match="matches_type"):
        Selection([LegacyMatchesOnly()])


def test_public_surface_exports_selection_rule_types():
    import tasklite

    assert {"Exclude", "Selection", "Skip"} <= set(tasklite.__all__)
    assert {
        "ERR_SKIPPED",
        "ERR_DEPENDENCY_SKIPPED",
        "ERR_DEPENDENCY_EXCLUDED",
    } <= set(tasklite.__all__)
    # 子模块名不被同名类覆盖：from tasklite.selection import ... 保持可用。
    assert tasklite.selection.__name__ == "tasklite.selection"
    assert tasklite.selection.Exclude is Exclude
    assert tasklite.selection.Selection is Selection
    assert tasklite.selection.Skip is Skip


def test_run_signatures_unchanged():
    assert str(inspect.signature(TaskLite.run)) == (
        "(self, *, install_signals: 'bool' = True, "
        "acquire_run_lock: 'bool' = True) -> 'RunSummary'"
    )
    assert str(inspect.signature(TaskLite.run_graceful)) == "(self) -> 'None'"
