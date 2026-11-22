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

from tasklite import Job, StateSnapshot, TaskLite
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.selection import Exclude
from tasklite.engine.governor import SelectionStandstill, partition_standstill
from tasklite.engine.resource import Resource
from tasklite.engine.session import RunSession
from tasklite.engine.store import StateStore
from tasklite.exceptions import TaskLiteError
from tasklite.models.context import JobContext
from tasklite.models.result import TaskResult
from tasklite.models.state import DependencyStatus, PipelineState
from tasklite.testing import in_process_workers, inject_resource


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


def _make_stub_launcher(*, fail_uid=None, spawn_jobs=(), seen=None):
    """受控 WorkerLauncher：release() 经 Pipe 发送结果；可选失败 UID 与派生作业。"""
    from tasklite.engine.launcher import PreparedWorker, WorkerLauncher
    from tasklite.utils.ipc import create_worker_pipe, create_worker_reply_pipe

    class _InstantHandle:
        pid = None
        exitcode = 0
        was_force_killed = False

        def __init__(self, sender, reply_receiver) -> None:
            self._sender = sender
            self._reply_receiver = reply_receiver

        def _close_pipes(self) -> None:
            if self._sender is not None:
                self._sender.close()
                self._sender = None
            if self._reply_receiver is not None:
                self._reply_receiver.close()
                self._reply_receiver = None

        def is_alive(self) -> bool:
            return False

        def join(self, timeout=None) -> None:
            self._close_pipes()

        def kill_tree(self) -> None:
            self.was_force_killed = True
            self.exitcode = -9
            self._close_pipes()

        def close(self) -> None:
            self._close_pipes()

    class _StubLauncher(WorkerLauncher):
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
            handle = _InstantHandle(sender, reply_receiver)

            def _release():
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
                sender.send_result(result)
                handle._close_pipes()

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

    return _StubLauncher()


@pytest.fixture()
def fake_process():
    return _make_stub_launcher


def _pipeline(root=None, name: str = "selection", **overrides) -> TaskLite:
    return TaskLite(backend="memory", **overrides)


def _uids(rows) -> list[str]:
    return [f"{row['task_type']}::{row['job_id']}" for row in rows]


def _pending_queue(pipeline: TaskLite) -> list[dict]:
    with StateSnapshot.open(pipeline) as snap:
        return [rec["job"] for rec in snap.iter_jobs(status="pending")]


def _queued(pipeline: TaskLite) -> list[str]:
    return _uids(_pending_queue(pipeline))


def _succeeded_uids(pipeline: TaskLite) -> list[str]:
    with StateSnapshot.open(pipeline) as snap:
        return sorted(rec["job_uid"] for rec in snap.iter_jobs(status="succeeded"))


def _failed_map(pipeline: TaskLite) -> dict[str, dict]:
    with StateSnapshot.open(pipeline) as snap:
        return {rec["job_uid"]: rec for rec in snap.iter_jobs(status="failed")}


# ── 默认全执行 / Exclude 延迟语义 ─────────────────────────────────────


def test_default_and_none_exclude_execute_everything(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("convert", ok_handler)
    pipeline.enqueue(Job("fetch", "a"))
    pipeline.enqueue(Job("convert", "b"))

    with in_process_workers(pipeline, fake_process()):
        pipeline.run(exclude=None)
    assert _succeeded_uids(pipeline) == ["convert::b", "fetch::a"]
    assert _queued(pipeline) == []


def test_excluded_and_downstream_jobs_stay_queued_then_auto_retry(
    tmp_path,
):
    seen: list[str] = []
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("convert", ok_handler)
    pipeline.register_task("rank", ok_handler)
    pipeline.enqueue(Job("fetch", "a"))
    pipeline.enqueue(Job("convert", "b"))
    pipeline.enqueue(Job("rank", "r", depends_on=["convert::b"]))
    before = {uid: row for uid, row in zip(_queued(pipeline), _pending_queue(pipeline))}

    with in_process_workers(pipeline, _make_stub_launcher(seen=seen)):
        pipeline.run(exclude=Exclude(task_type="convert"))

        assert _succeeded_uids(pipeline) == ["fetch::a"]
        assert _failed_map(pipeline) == {}
        assert _queued(pipeline) == ["convert::b", "rank::r"]
        kept = {uid: row for uid, row in zip(_queued(pipeline), _pending_queue(pipeline))}
        assert kept == {uid: before[uid] for uid in ("convert::b", "rank::r")}
        with StateSnapshot.open(pipeline) as snap:
            assert [
                list(snap.iter_attempts(job_uid="convert::b")),
                list(snap.iter_attempts(job_uid="rank::r")),
            ] == [[], []]
        assert seen == ["fetch::a"]

        # 下一次 run（不带 Exclude）自动恢复：留队下游全链执行完成
        pipeline.run()
    assert _succeeded_uids(pipeline) == [
        "convert::b",
        "fetch::a",
        "rank::r",
    ]
    assert _queued(pipeline) == []


def test_only_excluded_jobs_finish_completed_without_failures(fake_process, tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("convert", ok_handler)
    for job_id in ("a", "b", "c"):
        pipeline.enqueue(Job("convert", job_id))

    with in_process_workers(pipeline, fake_process()):
        pipeline.run(exclude=Exclude(task_type="convert"))

        assert _succeeded_uids(pipeline) == []
        assert _failed_map(pipeline) == {}
        assert sorted(_queued(pipeline)) == ["convert::a", "convert::b", "convert::c"]
        assert pipeline.run() is None
    assert _succeeded_uids(pipeline) == [
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
    pipeline.register_task("heavy", ok_handler, resources={"gpu": 1.0})
    pipeline.register_task("light", ok_handler)
    pipeline.enqueue(Job("heavy", "h"))
    pipeline.enqueue(Job("light", "l"))

    with inject_resource(pipeline, "gpu", gpu):
        pipeline.run(exclude=Exclude(task_type="heavy"))

    assert len(_succeeded_uids(pipeline)) == 1
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
    with pipeline.edit(dry_run=False) as tx:
        tx.replace("rank::b", Job("rank", "b", depends_on=["excluded_t::a"]), status="succeeded")
    pipeline.enqueue(
        Job("rank", "b", depends_on=["excluded_t::a", "rank::c"])
    )

    pipeline.run(exclude=Exclude(task_type="excluded_t"))

    failed = _failed_map(pipeline)
    assert sorted(failed) == ["rank::b", "rank::c"]
    assert all(
        "Dependency cycle prevents execution" in (rec["result"]["message"] or "")
        for rec in failed.values()
    )
    # 被排除作业不被环归因波及，原样留队
    assert _queued(pipeline) == ["excluded_t::a"]


def test_exclude_downstream_purged_dependency_is_failed(fake_process, tmp_path):
    """下游依赖如果在入队后被人为清除，下游直接按缺失结算，排除作业原样留队。"""
    from tasklite.backend.base import GraphEditCommit

    db_path = tmp_path / "purged_dep.db"
    with TaskLite(backend="sqlite", state_path=db_path) as setup:
        setup.register_task("excluded_t", ok_handler)
        setup.register_task("rank", ok_handler)
        setup.register_task("ghost_task", ok_handler)
        setup.enqueue(Job("ghost_task", "x"))
        setup.enqueue(Job("excluded_t", "a"))
        setup.enqueue(Job("rank", "b", depends_on=["excluded_t::a", "ghost_task::x"]))

    raw_backend = SQLiteStateBackend(db_path)
    raw_backend.commit_graph_edit(
        GraphEditCommit(queue_writes={"ghost_task::x": None})
    )

    with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
        pipeline.register_task("excluded_t", ok_handler)
        pipeline.register_task("rank", ok_handler)
        pipeline.register_task("ghost_task", ok_handler)
        pipeline.run(exclude=Exclude(task_type="excluded_t"))

        failed = _failed_map(pipeline)
        assert list(failed) == ["rank::b"]
        assert raw_backend.load_failed()["rank::b"]["missing_dependency"] == "ghost_task::x"
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
    assert _failed_map(pipeline) == {}
    assert _queued(pipeline) == ["excluded_t::a", "rank::b", "rank::c"]


def test_malformed_excluded_row_finishes_via_existing_failure_semantics(
    fake_process, tmp_path
):
    db_path = tmp_path / "malformed.db"
    # 畸形行（缺 job_id）带被排除 task_type：不得被选择豁免，仍结算为配置无效
    SQLiteStateBackend(db_path).enqueue_jobs(
        [{"task_type": "excluded_t"}, Job("rank", "ok").to_dict()]
    )
    with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
        pipeline.register_task("rank", ok_handler)
        outcome: dict = {}

        def run_pipeline():
            outcome["res"] = pipeline.run(exclude=Exclude(task_type="excluded_t"), install_signals=False)

        with in_process_workers(pipeline, fake_process()):
            worker = threading.Thread(target=run_pipeline, daemon=True)
            worker.start()
            worker.join(timeout=20)
        assert not worker.is_alive()

        assert outcome["res"] is None
        assert _succeeded_uids(pipeline) == ["rank::ok"]
        failures = _failed_map(pipeline)
        assert len(failures) == 1
        (failed_uid, fail_rec), = failures.items()
        assert failed_uid.startswith("_unknown::")
        assert fail_rec["result"]["message"] == "Queued job configuration is invalid"
        assert _pending_queue(pipeline) == []


# ── 动态派生与排除 ───────────────────────────────────────────────────


def test_spawned_excluded_child_stays_queued(tmp_path):
    spawned = Job("excluded_t", "child").to_dict()
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.enqueue(Job("fetch", "parent"))

    with in_process_workers(
        pipeline,
        _make_stub_launcher(spawn_jobs=[spawned]),
    ):
        pipeline.run(exclude=Exclude(task_type="excluded_t"))

    assert _succeeded_uids(pipeline) == ["fetch::parent"]
    assert _failed_map(pipeline) == {}
    assert _queued(pipeline) == ["excluded_t::child"]
    with StateSnapshot.open(pipeline) as snap:
        assert list(snap.iter_attempts(job_uid="excluded_t::child")) == []


def test_failed_parent_cascade_keeps_excluded_downstream(tmp_path):
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("fetch", ok_handler)
    pipeline.register_task("excluded_t", ok_handler)
    pipeline.register_task("downstream", ok_handler)
    pipeline.enqueue(Job("fetch", "parent", max_retries_per_run=0))
    pipeline.enqueue(Job("excluded_t", "child", depends_on=["fetch::parent"]))
    pipeline.enqueue(Job("downstream", "grand", depends_on=["excluded_t::child"]))

    with in_process_workers(
        pipeline,
        _make_stub_launcher(fail_uid="fetch::parent"),
    ):
        pipeline.run(exclude=Exclude(task_type="excluded_t"))

    assert sorted(_failed_map(pipeline)) == ["fetch::parent"]
    assert sorted(_queued(pipeline)) == ["downstream::grand", "excluded_t::child"]
    with StateSnapshot.open(pipeline) as snap:
        assert list(snap.iter_attempts(job_uid="excluded_t::child")) == []


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

    with pipeline.edit(dry_run=False) as tx:
        tx.add(Job("upstream", "u"), status="failed")
    pipeline.enqueue(Job("upstream", "u", trigger="once"))
    pipeline.enqueue(Job("downstream", "d", depends_on=["upstream::u"]))
    pipeline.run()

    failed = _failed_map(pipeline)
    assert "downstream::d" in failed

    assert "Dependency failed or has no record" in (failed["downstream::d"]["result"]["message"] or "")


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
    assert "downstream::d" not in _succeeded_uids(pipeline)


def test_uid_reuse_cycle_detection(fake_process, tmp_path):
    """同 UID 重用造成环时有覆盖：实际拓扑环必须被检测并熔断为失败。"""
    pipeline = _pipeline(tmp_path)
    pipeline.register_task("step", ok_handler)

    # 1. 先入队并运行 A 获得成功
    pipeline.enqueue(Job("step", "a"))
    pipeline.run()
    assert "step::a" in _succeeded_uids(pipeline)

    # 2. 入队 B 依赖 A
    pipeline.enqueue(Job("step", "b", depends_on=["step::a"]))

    # 3. 跨 Run 复用 UID：重新入队 A，但让 A 依赖 B
    pipeline.enqueue(Job("step", "a", depends_on=["step::b"]))

    # 此时队列中 A 依赖 B，B 依赖 A，形成实际拓扑环
    pipeline.run()

    failed = _failed_map(pipeline)
    assert "step::a" in failed
    assert "step::b" in failed
    assert "Dependency cycle prevents execution" in (failed["step::a"]["result"]["message"] or "")
    assert "Dependency cycle prevents execution" in (failed["step::b"]["result"]["message"] or "")


# ── 内部排除入口与停摆判定 ───────────────────────────────────────────


def test_temporary_exclude_and_check_contract():
    """验证 RunSession 运行期排除与临时排除唯一接口契约。"""
    session = RunSession()
    assert session.is_excluded("t::1", "t") is False

    # 单次临时排除
    session.temporarily_exclude("t::1")
    assert session.is_excluded("t::1", "t") is True
    assert session.is_excluded("t::2", "t") is False

    # Exclude 规则全局匹配
    session.exclude = Exclude(task_type="t")
    assert session.is_excluded("t::2", "t") is True
    assert session.is_excluded("other::1", "other") is False

    # clear_exclusions 清理临时排除并重置规则
    session.clear_exclusions()
    assert session.is_excluded_by_uid("t::1") is False


@pytest.mark.parametrize("special_task_type", ["ns:fetch", "rate%50", "ns:rate%50"])
def test_is_job_excluded_by_uid_decodes_colon_and_percent_task_type(special_task_type):
    """task_type 含 ':' 或 '%' 时，is_excluded_by_uid 须经 parse_job_uid 解码后匹配。"""
    session = RunSession()
    session.clear_exclusions(Exclude(task_type=special_task_type))
    uid = Job(special_task_type, "j1").uid
    assert session.is_excluded_by_uid(uid) is True
    assert session.is_excluded_by_uid(Job("other:type%1", "j1").uid) is False
    assert session.is_excluded_by_uid("malformed-uid") is False


def test_partition_standstill_kahn_analysis(tmp_path):
    """partition_standstill 基于 PipelineState 与 RunSession 执行 Kahn 停摆剥离。"""
    backend = InMemoryStateBackend()
    session = RunSession()
    store = StateStore(backend, session=session)

    # 无排除规则
    assert session.has_exclusions is False
    assert partition_standstill(store.state, session) is None

    # 有 Exclude 规则
    session.clear_exclusions(Exclude(task_type="blocked_type"))
    assert session.has_exclusions is True

    store.state.replace_queue([
        Job("blocked_type", "b1").to_dict(),
        Job("normal", "n1", depends_on=["blocked_type::b1"]).to_dict(),
    ])
    standstill = partition_standstill(store.state, session)
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
