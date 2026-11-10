"""执行选择（Exclude）与依赖闭合契约测试。

验证范围：
1. 单条 Exclude 精确匹配、留队、下游留队与下次 run 恢复；
2. 依赖闭合：外部 enqueue 拒绝缺失依赖、spawn 顺序依赖合法性校验；
3. 历史成功被活跃排除上游遮蔽、上游仅失败被 trigger 移除后下游级联失败；
4. 拓扑环检测（含同 UID 重用成环）；
5. 内部临时排除入口（供授权拒绝等复用）。
"""
from __future__ import annotations

import inspect
import threading

import pytest

from tasklite import Job, TaskLite
from tasklite.selection import Exclude
from tasklite.engine.errorclass import (
    ERR_DEPENDENCY_DEADLOCK,
    ERR_MALFORMED_JOB,
)
from tasklite.engine.resource import Resource
from tasklite.engine.scheduler import JobScheduler
from tasklite.engine.selection import SelectionMachine, SelectionStandstill
from tasklite.engine.store import StateStore
from tasklite.exceptions import TaskLiteError
from tasklite.models.context import JobContext
from tasklite.models.result import TaskResult
from tasklite.models.state import DependencyStatus, PipelineState
from tests.helpers import install_fake_process_context


def ok_handler(job, ctx):
    return TaskResult.success()


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


def _fake_process_class(*, fail_uid=None, spawn_jobs=(), seen=None):
    """FakeProcess：start() 经 Pipe 发送结果；可选失败 UID 与派生作业。"""

    class InstantProcess:
        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self.args = args
            self.exitcode = 0

        def start(self):
            spec = self.args[0]
            if seen is not None:
                seen.append(spec.job.uid)
            if spec.job.uid == fail_uid:
                result = {
                    "status": "failure",
                    "message": "boom",
                    "metadata": {},
                }
            else:
                result = {
                    "status": "success",
                    "metadata": {},
                    "spawned_jobs": [dict(job) for job in spawn_jobs],
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


@pytest.fixture()
def fake_process():
    return _fake_process_class


def _pipeline(root=None, name: str = "selection", **overrides) -> TaskLite:
    return TaskLite(backend="memory", **overrides)


def _uids(rows) -> list[str]:
    return [f"{row['task_type']}::{row['job_id']}" for row in rows]


def _queued(pipeline: TaskLite) -> list[str]:
    return _uids(pipeline._runtime.store.backend.load_queue())


# ── 默认全执行 / Exclude 延迟语义 ─────────────────────────────────────


def test_default_and_none_exclude_execute_everything(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    install_fake_process_context(pipeline, fake_process())
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("convert", ok_handler)
    pipeline.enqueue(Job("fetch", "a"))
    pipeline.enqueue(Job("convert", "b"))

    pipeline.run(exclude=None)
    assert sorted(pipeline._runtime.store.backend.load_successes()) == ["convert::b", "fetch::a"]
    assert _queued(pipeline) == []


def test_excluded_and_downstream_jobs_stay_queued_then_auto_retry(
    tmp_path,
):
    seen: list[str] = []
    pipeline = _pipeline(tmp_path)
    install_fake_process_context(pipeline, _fake_process_class(seen=seen))
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("convert", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(Job("fetch", "a"))
    pipeline.enqueue(Job("convert", "b"))
    pipeline.enqueue(Job("rank", "r", depends_on=["convert::b"]))
    before = {uid: row for uid, row in zip(_queued(pipeline), pipeline._runtime.store.backend.load_queue())}

    pipeline.run(exclude=Exclude(task_type="convert"))

    assert sorted(pipeline._runtime.store.backend.load_successes()) == ["fetch::a"]
    assert pipeline._runtime.store.backend.load_failed() == {}
    assert _queued(pipeline) == ["convert::b", "rank::r"]
    kept = {uid: row for uid, row in zip(_queued(pipeline), pipeline._runtime.store.backend.load_queue())}
    assert kept == {uid: before[uid] for uid in ("convert::b", "rank::r")}
    attempts = pipeline._runtime.store.backend.load_attempts
    assert [attempts("convert::b"), attempts("rank::r")] == [[], []]
    assert seen == ["fetch::a"]

    # 下一次 run（不带 Exclude）自动恢复：留队下游全链执行完成
    pipeline.run()
    assert sorted(pipeline._runtime.store.backend.load_successes()) == [
        "convert::b",
        "fetch::a",
        "rank::r",
    ]
    assert _queued(pipeline) == []


def test_only_excluded_jobs_finish_completed_without_failures(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    install_fake_process_context(pipeline, fake_process())
    pipeline.register_task("convert", ok_handler)
    for job_id in ("a", "b", "c"):
        pipeline.enqueue(Job("convert", job_id))

    pipeline.run(exclude=Exclude(task_type="convert"))

    assert pipeline._runtime.store.backend.load_successes() == {}
    assert pipeline._runtime.store.backend.load_failed() == {}
    assert sorted(_queued(pipeline)) == ["convert::a", "convert::b", "convert::c"]
    assert pipeline.run() is None
    assert sorted(pipeline._runtime.store.backend.load_successes()) == [
        "convert::a",
        "convert::b",
        "convert::c",
    ]


def test_exclude_validation_and_rejection_of_multiple_rules(tmp_path):
    # Exclude 必须使用关键字参数
    with pytest.raises(TypeError):
        Exclude("convert")  # type: ignore[misc]

    with pytest.raises(TaskLiteError):
        Exclude(task_type=123)  # type: ignore[arg-type]
    with pytest.raises(TaskLiteError):
        Exclude(task_type="")

    ex = Exclude(task_type="convert")
    assert ex.task_type == "convert"
    assert ex.matches_task_type("convert") is True
    assert ex.matches_task_type("fetch") is False

    pipeline = _pipeline(tmp_path)
    # 拒绝列表或多规则
    with pytest.raises(TaskLiteError, match="exclude must be an Exclude instance or None"):
        pipeline.run(exclude=[Exclude(task_type="a")])  # type: ignore[arg-type]

    with pytest.raises(TaskLiteError, match="exclude must be an Exclude instance or None"):
        pipeline.run(exclude="convert")  # type: ignore[arg-type]


def test_excluded_job_does_not_consume_resources(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    gpu = CountingResource()
    pipeline.register_resource(gpu)
    pipeline.register_task("heavy", ok_handler, resources={"gpu": 1.0})
    pipeline.register_task("light", ok_handler)
    pipeline.enqueue(Job("heavy", "h"))
    pipeline.enqueue(Job("light", "l"))

    pipeline.run(exclude=Exclude(task_type="heavy"))

    assert len(pipeline._runtime.store.backend.load_successes()) == 1
    assert gpu.acquires == 0 and gpu.used == 0.0
    assert _queued(pipeline) == ["heavy::h"]


# ── Exclude 停摆与真实死锁剥离 ─────────────────────────────────────


def test_exclude_downstream_cycle_is_failed_not_masked(fake_process, tmp_path):
    """允许作业 b<->c 的环挂在 Exclude 下游，必须按死锁归因熔断，不被 Exclude 掩盖。"""
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("rank", ok_handler)

    pipeline.enqueue(Job("excluded_t", "a"))
    pipeline.enqueue(Job("rank", "b", depends_on=["excluded_t::a"]))
    pipeline.enqueue(Job("rank", "c", depends_on=["rank::b"]))
    pipeline.set_job_status("rank::b", "succeeded", recursive=False)
    pipeline.enqueue(
        Job("rank", "b", depends_on=["excluded_t::a", "rank::c"])
    )

    pipeline.run(exclude=Exclude(task_type="excluded_t"))

    failed = pipeline._runtime.store.backend.load_failed()
    assert sorted(failed) == ["rank::b", "rank::c"]
    assert all(ERR_DEPENDENCY_DEADLOCK in meta["error_message"] for meta in failed.values())
    # 被排除作业不被环归因波及，原样留队
    assert _queued(pipeline) == ["excluded_t::a"]


def test_exclude_downstream_purged_dependency_is_failed(fake_process, tmp_path):
    """下游依赖如果在入队后被人为清除，下游直接按缺失结算，排除作业原样留队。"""
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.register_task("ghost_task", ok_handler)

    pipeline.enqueue(Job("ghost_task", "x"))
    pipeline.enqueue(Job("excluded_t", "a"))
    pipeline.enqueue(Job("rank", "b", depends_on=["excluded_t::a", "ghost_task::x"]))

    pipeline.set_job_status("ghost_task::x", "succeeded", recursive=False)
    pipeline.clear_history("ghost_task::x", records=["successes"])

    pipeline.run(exclude=Exclude(task_type="excluded_t"))

    failed = pipeline._runtime.store.backend.load_failed()
    assert list(failed) == ["rank::b"]
    assert failed["rank::b"]["missing_dependency"] == "ghost_task::x"
    assert _queued(pipeline) == ["excluded_t::a"]


def test_exclude_downstream_blocks_completes_without_busy_spin(fake_process, tmp_path):
    """仅剩「排除 + 仅被其阻塞」时立即正常收尾，不得忙旋。"""
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(Job("excluded_t", "a"))
    pipeline.enqueue(Job("rank", "b", depends_on=["excluded_t::a"]))
    pipeline.enqueue(Job("rank", "c", depends_on=["rank::b"]))

    outcome: dict = {}

    def run_pipeline():
        outcome["res"] = pipeline.run(exclude=Exclude(task_type="excluded_t"), install_signals=False)

    worker = threading.Thread(target=run_pipeline, daemon=True)
    worker.start()
    worker.join(timeout=10)
    assert not worker.is_alive(), "Exclude 下游停摆导致 run() 挂死（忙旋）"

    assert outcome["res"] is None
    assert pipeline._runtime.store.backend.load_failed() == {}
    assert _queued(pipeline) == ["excluded_t::a", "rank::b", "rank::c"]


def test_malformed_excluded_row_finishes_via_existing_failure_semantics(
    fake_process, tmp_path
):
    pipeline = _pipeline(tmp_path)
    install_fake_process_context(pipeline, fake_process())
    pipeline.register_task("rank", ok_handler)
    # 畸形行（缺 job_id）带被排除 task_type：不得被选择豁免，仍走 MALFORMED_JOB
    pipeline._runtime.store.backend.save_queue(
        [{"task_type": "excluded_t"}, Job("rank", "ok").to_dict()]
    )
    outcome: dict = {}

    def run_pipeline():
        outcome["res"] = pipeline.run(exclude=Exclude(task_type="excluded_t"), install_signals=False)

    worker = threading.Thread(target=run_pipeline, daemon=True)
    worker.start()
    worker.join(timeout=20)
    assert not worker.is_alive()

    assert outcome["res"] is None
    assert sorted(pipeline._runtime.store.backend.load_successes()) == ["rank::ok"]
    failures = pipeline._runtime.store.backend.load_failed()
    assert len(failures) == 1
    (failed_uid, fail_meta), = failures.items()
    assert failed_uid.startswith("_unknown::")
    assert fail_meta["error_message"] == ERR_MALFORMED_JOB
    assert pipeline._runtime.store.backend.load_queue() == []


# ── 动态派生与排除 ───────────────────────────────────────────────────


def test_spawned_excluded_child_stays_queued(tmp_path):
    spawned = Job("excluded_t", "child").to_dict()
    pipeline = _pipeline(tmp_path)
    install_fake_process_context(
        pipeline,
        _fake_process_class(spawn_jobs=[spawned]),
    )
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.enqueue(Job("fetch", "parent"))

    pipeline.run(exclude=Exclude(task_type="excluded_t"))

    assert sorted(pipeline._runtime.store.backend.load_successes()) == ["fetch::parent"]
    assert pipeline._runtime.store.backend.load_failed() == {}
    assert _queued(pipeline) == ["excluded_t::child"]
    assert pipeline._runtime.store.backend.load_attempts("excluded_t::child") == []


def test_failed_parent_cascade_keeps_excluded_downstream(tmp_path):
    pipeline = _pipeline(tmp_path)
    install_fake_process_context(
        pipeline,
        _fake_process_class(fail_uid="fetch::parent"),
    )
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("downstream", ok_handler)
    pipeline.enqueue(Job("fetch", "parent", max_retries_per_run=0))
    pipeline.enqueue(Job("excluded_t", "child", depends_on=["fetch::parent"]))
    pipeline.enqueue(Job("downstream", "grand", depends_on=["excluded_t::child"]))

    pipeline.run(exclude=Exclude(task_type="excluded_t"))

    assert sorted(pipeline._runtime.store.backend.load_failed()) == ["fetch::parent"]
    assert sorted(_queued(pipeline)) == ["downstream::grand", "excluded_t::child"]
    assert pipeline._runtime.store.backend.load_attempts("excluded_t::child") == []


# ── 必须验收项：依赖闭合、遮蔽与 UID 重用成环 ───────────────────────


def test_external_enqueue_rejects_missing_dependency(tmp_path):
    """外部 enqueue 只能引用已存在的 Job（队列或终态档案）；不存在当场拒绝。"""
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("calc", ok_handler)

    # 引用完全不存在的依赖 -> 拒绝
    with pytest.raises(TaskLiteError, match="references non-existent dependency"):
        pipeline.enqueue(Job("calc", "b", depends_on=["calc::non_existent"]))

    # 依赖存在于队列中时允许引用
    pipeline.enqueue(Job("calc", "a"))
    assert pipeline.enqueue(Job("calc", "b", depends_on=["calc::a"])) is True


def test_spawn_dependency_order_contract(tmp_path):
    """父声明 A 后 B 依赖 A 合法，反序不合法；仅引用派发时已知、父及此前声明的 Job。"""
    parent = Job("gen", "root")
    ctx = JobContext(
        parent,
        frozenset(),
        frozenset(),
        {},
        known_job_uids=frozenset({"known::k1"}),
    )

    # 引用派发时已知 Job 合法
    child1 = Job("proc", "c1", depends_on=["known::k1"])
    ctx.spawn(child1)

    # 引用父 Job 合法
    child2 = Job("proc", "c2", depends_on=[parent.uid])
    ctx.spawn(child2)

    # 引用此前已声明的 child1 合法
    child3 = Job("proc", "c3", depends_on=[child1.uid])
    ctx.spawn(child3)

    # 反序声明：child_bad 引用尚未声明的 child_future -> 当场抛 ValueError
    child_bad = Job("proc", "c4", depends_on=["proc::c_future"])
    with pytest.raises(TaskLiteError, match="references non-existent, future, or cross-worker dependency"):
        ctx.spawn(child_bad)


def test_cross_worker_future_spawn_not_valid_dependency(tmp_path):
    """跨 worker 未来产出不作为依赖证据（不在已知集合中直接拒绝）。"""
    parent = Job("gen", "root")
    # worker 派发时，known_job_uids 确定
    ctx = JobContext(
        parent,
        frozenset(),
        frozenset(),
        {},
        known_job_uids=frozenset({"worker1::done"}),
    )

    # 依赖其他 worker 可能在未来产出的 job
    with pytest.raises(TaskLiteError, match="references non-existent, future, or cross-worker dependency"):
        ctx.spawn(Job("proc", "sub", depends_on=["worker2::future_child"]))


def test_upstream_only_failure_skipped_by_trigger_fails_downstream(fake_process, tmp_path):
    """上游 only failure 被 once 跳过时下游仍失败。"""
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("upstream", ok_handler)
    pipeline.register_task("downstream", ok_handler)

    pipeline.set_job_status(Job("upstream", "u"), "failed", recursive=False)
    pipeline.enqueue(Job("upstream", "u", trigger="once"))
    pipeline.enqueue(Job("downstream", "d", depends_on=["upstream::u"]))
    pipeline.run()

    failed = pipeline._runtime.store.backend.load_failed()
    assert "downstream::d" in failed
    from tasklite.engine.errorclass import ERR_JOB_DEPENDENCY
    assert ERR_JOB_DEPENDENCY in failed["downstream::d"]["error_message"]


def test_active_excluded_upstream_masks_historical_success(fake_process, tmp_path):
    """历史成功被活跃排除上游遮蔽：上游被 exclude 留队时，下游不得凭历史成功直接运行。"""
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("upstream", ok_handler)
    pipeline.register_task("downstream", ok_handler)

    pipeline.enqueue(Job("upstream", "u"))
    pipeline.run()
    pipeline.enqueue(Job("upstream", "u"))
    pipeline.enqueue(Job("downstream", "d", depends_on=["upstream::u"]))
    pipeline.run(exclude=Exclude(task_type="upstream"))

    # 历史成功被活跃候选遮蔽，upstream 被排除，downstream 也必须留队，不得偷跑
    assert sorted(_queued(pipeline)) == ["downstream::d", "upstream::u"]
    assert "downstream::d" not in pipeline._runtime.store.backend.load_successes()


def test_uid_reuse_cycle_detection(fake_process, tmp_path):
    """同 UID 重用造成环时有覆盖：实际拓扑环必须被检测并熔断为失败。"""
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("step", ok_handler)

    # 1. 先入队并运行 A 获得成功
    pipeline.enqueue(Job("step", "a"))
    pipeline.run()
    assert "step::a" in pipeline._runtime.store.backend.load_successes()

    # 2. 入队 B 依赖 A
    pipeline.enqueue(Job("step", "b", depends_on=["step::a"]))

    # 3. 跨 Run 复用 UID：重新入队 A，但让 A 依赖 B
    pipeline.enqueue(Job("step", "a", depends_on=["step::b"]))

    # 此时队列中 A 依赖 B，B 依赖 A，形成实际拓扑环
    pipeline.run()

    failed = pipeline._runtime.store.backend.load_failed()
    assert "step::a" in failed
    assert "step::b" in failed
    assert ERR_DEPENDENCY_DEADLOCK in failed["step::a"]["error_message"]
    assert ERR_DEPENDENCY_DEADLOCK in failed["step::b"]["error_message"]


# ── 内部排除入口与停摆判定 ───────────────────────────────────────────


def test_temporary_exclude_and_check_contract():
    """验证调度器与运行时内部临时排除机制（供授权拒绝等复用）。"""
    scheduler = JobScheduler(resources={})
    assert scheduler.is_job_excluded("t::1", "t") is False

    # 单次临时排除
    scheduler.temporarily_exclude("t::1")
    assert scheduler.is_job_excluded("t::1", "t") is True
    assert scheduler.is_job_excluded("t::2", "t") is False

    # Exclude 规则全局匹配
    scheduler.exclude = Exclude(task_type="t")
    assert scheduler.is_job_excluded("t::2", "t") is True
    assert scheduler.is_job_excluded("other::1", "other") is False

    # reset_run 清理临时排除
    scheduler.reset_run()
    assert scheduler.is_job_excluded_by_uid("t::1") is False


def test_selection_machine_kahn_partition(tmp_path):
    """SelectionMachine 仅保留 Exclude 停摆分析。"""
    backend = _pipeline(tmp_path, name="test")._runtime.store.backend
    store = StateStore(backend)

    # 无排除规则
    sm_inactive = SelectionMachine(store)
    assert sm_inactive.is_active is False
    assert sm_inactive.partition_standstill() is None

    # 有 Exclude 规则
    sm_active = SelectionMachine(store, exclude=Exclude(task_type="blocked_type"))
    assert sm_active.is_active is True

    store.state.replace_queue([
        Job("blocked_type", "b1").to_dict(),
        Job("normal", "n1", depends_on=["blocked_type::b1"]).to_dict(),
    ])
    standstill = sm_active.partition_standstill()
    assert isinstance(standstill, SelectionStandstill)
    assert standstill.excluded_uids == ("blocked_type::b1",)
    assert standstill.blocked_uids == ("normal::n1",)
    assert standstill.only_blocked_by_exclusion is True


def test_public_surface_and_signature_contract():
    import tasklite
    import tasklite.selection as sel

    assert "Exclude" in sel.__all__
    assert "Exclude" not in tasklite.__all__
    assert "Selection" not in tasklite.__all__
    assert "Skip" not in tasklite.__all__

    sig = inspect.signature(TaskLite.run)
    assert "exclude" in sig.parameters
    assert sig.parameters["exclude"].default is None
