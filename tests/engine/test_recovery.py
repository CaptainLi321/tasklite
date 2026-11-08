"""v2 恢复编排器测试：队列修复差量落盘、终态交集收敛、崩溃安全保存、
挂起恢复、残留信号回收、陈旧结果恢复与 TOCTOU 安全中止。

以窄契约替身（StoreShim / FakeChannel / RecordingCompletion）装配确定性
单测；中止与恢复路径用真实 ExecutionChannel + InFlightTracker 复现
受控时序（无真实多进程竞速）。
"""

import logging
import time
from unittest.mock import MagicMock

import pytest


from tasklite.triggers import BUILTIN_TRIGGER_FUNCTIONS
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.admission import TriggerRegistry
from tasklite.engine.channel import ExecutionChannel
from tasklite.engine.in_flight import InFlightJob, InFlightTracker
from tasklite.engine.recovery import RecoveryOrchestrator
from tasklite.engine.resource import (
    CapacityResource,
    ResourceManager,
)
from tasklite.engine.types import JobHandle
from tasklite.models.job import Job
from tasklite.models.state import PipelineState
from tasklite.utils.ipc import ArtifactJournal
from tasklite.utils.jsonutil import dumps, loads

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


class StoreShim:
    """StateStore 落地前的窄契约替身：backend 活引用 + set_state + queue。"""

    def __init__(self, backend, state=None):
        self.backend = backend
        self._state = state if state is not None else PipelineState({}, {}, {}, [])

    def set_state(self, state):
        self._state = state

    @property
    def state(self):
        return self._state

    @property
    def queue(self):
        return self._state.queue


class FakeChannel:
    """RecoveryOrchestrator 消费面的具名替身——字段即配置。"""

    def __init__(self, *, active_signals=(), abort_outcome=None):
        self.active_signals = list(active_signals)
        self.abort_outcome = abort_outcome

    def drain_active_signals(self, uids):
        return list(self.active_signals)

    def abort_in_flight(self, handles):
        from tasklite.engine.channel import AbortOutcome

        if self.abort_outcome is not None:
            return self.abort_outcome
        return AbortOutcome(completed=[], cancelled=[])


class RecordingCompletion:
    """完成机器窄契约替身：记录结算调用供断言。"""

    def __init__(self, *, complete_job_error=None):
        self.completed_jobs = []
        self.settled = []
        self._complete_job_error = complete_job_error

    def complete_job(self, entry, result):
        self.completed_jobs.append((entry, result))
        if self._complete_job_error is not None:
            raise self._complete_job_error

    def settle_aborted(self, cancelled_entries, done_entries):
        self.settled.append((list(cancelled_entries), list(done_entries)))


def make_recovery_orchestrator(backend=None, *, state=None, resource_mgr=None,
                               channel=None, in_flight=None, completion=None,
                               policy=None, sqlite_path=None):
    """窄依赖装配：未提供的协作方落 MagicMock（纯逻辑单测不触真实 IPC 面）。"""
    if backend is None:
        backend = SQLiteStateBackend(sqlite_path) if sqlite_path else InMemoryStateBackend()
    return RecoveryOrchestrator(
        store=StoreShim(backend, state=state),
        channel=channel if channel is not None else MagicMock(),
        resources=resource_mgr if resource_mgr is not None else ResourceManager(),
        in_flight=in_flight if in_flight is not None else MagicMock(),
        trigger_registry=policy if policy is not None else TriggerRegistry(functions=BUILTIN_TRIGGER_FUNCTIONS),
        completion=completion if completion is not None else MagicMock(),
    )


def _uids(jobs):
    return [f"{j['task_type']}::{j['job_id']}" for j in jobs]


class TestRepairQueueOnLoad:
    def test_repair_preserves_all_candidates_without_early_drop(self):
        """加载期保候选契约：所有候选原样保留进内存队列，不在加载期提前删行。"""
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(backend)

        job_wall = {"task_type": "t", "job_id": "w_blocked", "trigger": "once"}
        job_every_run = {"task_type": "t", "job_id": "w_rerun", "trigger": "always"}
        job_failed_rerun = {"task_type": "t", "job_id": "f_rerun", "trigger": "on_failure"}
        job_fresh = {"task_type": "t", "job_id": "fresh"}

        successes = {"t::w_blocked": {}, "t::w_rerun": {}}
        failed = {"t::f_rerun": {}}
        repaired = orchestrator.repair_queue_on_load(
            [job_wall, job_every_run, job_failed_rerun, job_fresh],
            successes=successes, failed=failed,
        )

        assert _uids(repaired) == ["t::w_blocked", "t::w_rerun", "t::f_rerun", "t::fresh"]

    def test_repair_runtime_passes_through_untouched(self):
        """无重试节奏字段的运行时状态原样穿透（重试节奏归 RequeuePolicy）。"""
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(backend)
        runtime = {"_commit_failures": 1, "_dispatch_failures": 2,
                   "_last_retry_error": "old", "user_extra": {"k": "v"}}
        job = {"task_type": "t", "job_id": "rt", "runtime": dict(runtime)}

        repaired = orchestrator.repair_queue_on_load([job], successes={}, failed={})

        assert repaired[0]["runtime"] == runtime, "修复不得改写 runtime 边带状态"


class TestRepairDeltaPersist:
    """repair 合并必须保留磁盘真相：陈旧加载快照不得全表覆盖磁盘真相。"""

    def test_repair_preserves_job_enqueued_during_repair_window(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "t", "job_id": "residue_row", "trigger": "once"},
            {"task_type": "t", "job_id": "keep_row"},
        ])
        q_data = backend.load_queue()
        # 注入「load 之后、repair 落盘之前」他进程已应答成功的入队
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        def _poison_full_rewrite(jobs):
            raise AssertionError("repair 不得以陈旧快照全表重写队列")

        backend.save_queue = _poison_full_rewrite

        orchestrator = make_recovery_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(
            q_data, successes={"t::residue_row": {}}, failed={}
        )

        disk_uids = set(_uids(backend.load_queue()))
        assert "t::late" in disk_uids
        assert "t::keep_row" in disk_uids
        assert "t::residue_row" in disk_uids
        # 窗口期入队的作业同时并入本次 run 的内存队列
        assert set(_uids(repaired)) == {"t::residue_row", "t::keep_row", "t::late"}

    def test_repair_preserves_candidate_rows_on_disk(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "t", "job_id": "residue_row", "trigger": "once"},
            {"task_type": "t", "job_id": "keep_row"},
            {"task_type": "t", "job_id": "failed_residue", "trigger": "once"},
            {"task_type": "t", "job_id": "keep_second"},
        ])
        q_data = backend.load_queue()

        orchestrator = make_recovery_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(
            q_data, successes={"t::residue_row": {}}, failed={"t::failed_residue": {}}
        )

        assert _uids(backend.load_queue()) == [
            "t::residue_row", "t::keep_row", "t::failed_residue", "t::keep_second"
        ]
        assert _uids(repaired) == [
            "t::residue_row", "t::keep_row", "t::failed_residue", "t::keep_second"
        ]

    def test_repair_delta_persist_aligned_on_memory_backend(self):
        backend = InMemoryStateBackend()
        backend.save_queue([
            {"task_type": "t", "job_id": "residue_row", "trigger": "once"},
            {"task_type": "t", "job_id": "keep_row"},
        ])
        q_data = backend.load_queue()
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        orchestrator = make_recovery_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(
            q_data, successes={"t::residue_row": {}}, failed={}
        )

        assert set(_uids(backend.load_queue())) == {"t::residue_row", "t::keep_row", "t::late"}
        assert set(_uids(repaired)) == {"t::residue_row", "t::keep_row", "t::late"}


class TestRepairDuplicateWarning:
    """repair 合并的去重告警语义：仅真实异常重复告警。

    磁盘合并重读必然与加载快照全员重逢（镜像行）——镜像行不得进入
    duplicate 告警分支，否则每次启动对队列每条存量作业误报一条，大队列
    日志洪水淹没真实漂移信号。
    """

    def test_disk_mirror_rows_do_not_emit_duplicate_warnings(self, tmp_path, caplog):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "t", "job_id": "first"},
            {"task_type": "t", "job_id": "second"},
            {"task_type": "t", "job_id": "third"},
        ])
        q_data = backend.load_queue()
        # 窗口期并发入队：磁盘重读比快照多一行（合并预期内的新增，非重复）
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        orchestrator = make_recovery_orchestrator(backend)
        with caplog.at_level(logging.WARNING, logger="tasklite"):
            repaired = orchestrator.repair_queue_on_load(q_data, successes={}, failed={})

        dup_msgs = [
            r.getMessage() for r in caplog.records if "duplicate uid" in r.getMessage()
        ]
        assert dup_msgs == [], f"磁盘镜像行不得触发 duplicate 告警: {dup_msgs}"
        assert [j["job_id"] for j in repaired] == ["first", "second", "third", "late"]

    def test_snapshot_internal_duplicate_still_warns(self, caplog):
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(backend)
        q_data = [
            {"task_type": "t", "job_id": "dup"},
            {"task_type": "t", "job_id": "dup"},
        ]

        with caplog.at_level(logging.WARNING, logger="tasklite"):
            repaired = orchestrator.repair_queue_on_load(q_data, successes={}, failed={})

        dup_msgs = [
            r.getMessage() for r in caplog.records if "duplicate uid" in r.getMessage()
        ]
        assert len(dup_msgs) == 1, f"快照内部真实重复必须恰好告警一次: {dup_msgs}"
        assert "t::dup" in dup_msgs[0]
        assert [j["job_id"] for j in repaired] == ["dup"]

    def test_disk_reload_failure_degrades_to_snapshot(self, tmp_path, caplog):
        """磁盘重读失败仅放弃合并并告警，清理正确性不受影响。"""
        backend = InMemoryStateBackend()
        backend.save_queue([{"task_type": "t", "job_id": "keep_row"}])
        q_data = backend.load_queue()

        def broken_load():
            raise OSError("disk unavailable")

        backend.load_queue = broken_load
        orchestrator = make_recovery_orchestrator(backend)
        with caplog.at_level(logging.WARNING, logger="tasklite"):
            repaired = orchestrator.repair_queue_on_load(q_data, successes={}, failed={})

        assert _uids(repaired) == ["t::keep_row"], "重读失败以加载快照为准，不丢行"
        assert any("repair merge" in r.getMessage() for r in caplog.records)


class TestTerminalOverlapConvergence:
    """加载期终态交集收敛：successes∩failed 违例数据在构建运行态前确定性收敛。

    不变式：successes/failed 全局互斥；违例交集若流入运行态，首个作业派发的
    in-flight 登记触发的全量互斥断言即崩溃循环。收敛必须 failed 优先
    （失败档案保留失败证据供人工核查重跑）、按 uid 差量定向删除（磁盘
    其余行原样保留），并留 WARNING 诊断证据。
    """

    def test_overlap_converged_to_failed_in_memory_and_disk(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.seed_successes(["t::x", "t::clean"])
        backend.append_failed("t::x", {"error_message": "boom"})
        successes = backend.load_successes()
        failed = backend.load_failed()
        orchestrator = make_recovery_orchestrator(backend)

        orchestrator.converge_terminal_overlap(successes, failed)

        # failed 优先：交集 uid 从内存 successes 剔除，失败档案证据保留
        assert "t::x" not in successes
        assert "t::clean" in successes
        assert "t::x" in failed
        # 磁盘差量收敛：仅违例行剔除，其余行原样保留
        assert "t::x" not in backend.load_successes()
        assert "t::clean" in backend.load_successes()
        assert "t::x" in backend.load_failed()

    def test_convergence_emits_warning_listing_conflict_uids(self, caplog):
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(backend)
        successes = {"t::x": {}, "t::y": {}}
        failed = {"t::x": {}}

        with caplog.at_level(logging.WARNING, logger="tasklite"):
            orchestrator.converge_terminal_overlap(successes, failed)

        warns = [
            r.getMessage() for r in caplog.records
            if "both successes and failed" in r.getMessage()
        ]
        assert len(warns) == 1, f"交集收敛必须留恰好一条 WARNING 证据: {warns}"
        assert "t::x" in warns[0]

    def test_disjoint_terminal_sets_untouched_and_silent(self, caplog):
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(backend)
        successes = {"t::w": {}}
        failed = {"t::f": {}}

        with caplog.at_level(logging.WARNING, logger="tasklite"):
            orchestrator.converge_terminal_overlap(successes, failed)

        assert successes == {"t::w": {}}
        assert failed == {"t::f": {}}
        assert caplog.records == []

    def test_backend_delete_failure_degrades_without_losing_convergence(self, caplog):
        backend = MagicMock()
        backend.delete_successes.side_effect = RuntimeError("disk gone")
        orchestrator = make_recovery_orchestrator(backend)
        successes = {"t::x": {}}
        failed = {"t::x": {}}

        with caplog.at_level(logging.WARNING, logger="tasklite"):
            orchestrator.converge_terminal_overlap(successes, failed)

        # 落盘失败不回退内存收敛（本次 run 不受影响），磁盘下次加载重判
        assert "t::x" not in successes
        assert any(
            "re-converge on next load" in r.getMessage() for r in caplog.records
        )


class TestRepairFrontPriority:
    """repair 窗口期 front 入队的队首优先语义（磁盘真相序权威）。

    窗口期他进程 front 入队的作业在磁盘真相中位于队首（seq 最小），合并
    必须保持其磁盘位置而非 append 到内存队尾——否则队首优先失效，且随后
    停机 crash-safe 落盘会把错误顺序固化到磁盘（磁盘/内存序分叉）。
    """

    def test_front_enqueued_during_window_keeps_head_position(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([{"task_type": "t", "job_id": "keep_row"}])
        q_data = backend.load_queue()
        # 窗口期他进程 front 入队：磁盘真相中位于队首
        backend.enqueue_jobs([{"task_type": "t", "job_id": "front_job"}], front=True)

        orchestrator = make_recovery_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(q_data, successes={}, failed={})
        assert [j["job_id"] for j in repaired] == ["front_job", "keep_row"]

        # 停机落盘固化验证：合并序写入 store 后经 crash-safe 落盘，磁盘序
        # 不得被内存中的错误队尾 append 翻转
        state = PipelineState(
            backend.load_successes(),
            backend.load_failed(),
            backend.load_cursors(),
            repaired,
        )
        orchestrator = make_recovery_orchestrator(backend, state=state)
        orchestrator.save_queue_crash_safe()
        assert [j["job_id"] for j in backend.load_queue()] == ["front_job", "keep_row"]

    def test_back_enqueued_during_window_appends_after_existing(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([{"task_type": "t", "job_id": "keep_row"}])
        q_data = backend.load_queue()
        backend.enqueue_jobs([{"task_type": "t", "job_id": "tail_job"}])

        orchestrator = make_recovery_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(q_data, successes={}, failed={})
        assert [j["job_id"] for j in repaired] == ["keep_row", "tail_job"]

    def test_snapshot_rows_missing_on_disk_are_kept_at_tail(self, tmp_path):
        """快照独有行（窗口期被他进程消费）不丢行，但让位磁盘真相序队尾。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "t", "job_id": "keep_first"},
            {"task_type": "t", "job_id": "keep_second"},
        ])
        q_data = backend.load_queue()
        # 窗口期他进程消费 keep_first（delta 删除），并 front 入队新作业
        backend.delete_queue_uids(["t::keep_first"])
        backend.enqueue_jobs([{"task_type": "t", "job_id": "front_job"}], front=True)

        orchestrator = make_recovery_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(q_data, successes={}, failed={})
        assert [j["job_id"] for j in repaired] == ["front_job", "keep_second", "keep_first"]


class TestCrashSafeSave:
    def test_crash_safe_save_recovers_missing_jobs_from_disk(self):
        backend = InMemoryStateBackend()
        # 磁盘上有 j_first, j_second
        backend.save_queue([
            {"task_type": "t", "job_id": "j_first"},
            {"task_type": "t", "job_id": "j_second"},
        ])

        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "j_second"}])

        orchestrator = make_recovery_orchestrator(backend, state=state)

        orchestrator.save_queue_crash_safe()

        disk_uids = _uids(backend.load_queue())
        # j_first 必须被补回队首
        assert disk_uids == ["t::j_first", "t::j_second"]

    def test_crash_safe_save_preserves_job_enqueued_in_merge_window(self, tmp_path, monkeypatch):
        """合并窗口内他进程已应答成功的入队必须在保存后仍存在于磁盘。

        磁盘上的 ``late`` 等价于「读磁盘真相与写回之间」他进程 enqueue
        已提交的作业（已落盘、本进程内存未感知）：整表覆盖保存会将其
        静默抹除。同时结构锁定：合并保存必须经单事务原语收敛读-改-写，
        禁止退化为独立 load/save 两段式。
        """
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([{"task_type": "t", "job_id": "keep_row"}])
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])
        orig_load = backend.load_queue  # monkeypatch 前保存原始读法（断言用）

        # 内存队列持 keep_row 的更新版本（runtime/payload 演进），未感知 late
        state = PipelineState(
            {}, {}, {}, [{"task_type": "t", "job_id": "keep_row", "payload": {"v": 2}}]
        )
        orchestrator = make_recovery_orchestrator(backend, state=state)

        legacy_calls = []
        monkeypatch.setattr(backend, "load_queue", lambda: legacy_calls.append("load"))
        monkeypatch.setattr(backend, "save_queue", lambda jobs: legacy_calls.append("save"))

        orchestrator.save_queue_crash_safe()

        assert legacy_calls == [], "合并保存必须经单事务原语，不得两段式 load/save"
        disk_q = orig_load()
        # 窗口期入队被磁盘真相合并保留并补回队首；keep_row 以内存版本内容落盘
        assert _uids(disk_q) == ["t::late", "t::keep_row"]
        assert disk_q[1]["payload"] == {"v": 2}

    def test_crash_safe_save_merge_window_aligned_on_memory_backend(self):
        backend = InMemoryStateBackend()
        backend.save_queue([{"task_type": "t", "job_id": "keep_row"}])
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "keep_row"}])
        orchestrator = make_recovery_orchestrator(backend, state=state)

        orchestrator.save_queue_crash_safe()

        assert _uids(backend.load_queue()) == ["t::late", "t::keep_row"]

    def test_crash_safe_save_skips_overwrite_if_disk_load_fails(self):
        backend = InMemoryStateBackend()
        backend.save_queue([{"task_type": "t", "job_id": "safe_on_disk"}])

        # 模拟保存原语失败（读真相/写回任一阶段）
        def _boom(compute):
            raise IOError("Disk corruption")

        backend.replace_queue_atomic = _boom

        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "in_mem"}])
        orchestrator = make_recovery_orchestrator(backend, state=state)

        # 不应抛出异常，也不应覆盖磁盘（内存独有作业由 at-least-once 吸收）
        orchestrator.save_queue_crash_safe()

        assert _uids(backend.load_queue()) == ["t::safe_on_disk"]

    def test_crash_safe_save_dedups_memory_duplicates(self):
        """异常路径可能重复 requeue 同一作业：保存前内存按 uid 去重。"""
        backend = InMemoryStateBackend()
        state = PipelineState({}, {}, {}, [
            {"task_type": "t", "job_id": "dup"},
            {"task_type": "t", "job_id": "dup"},
        ])
        orchestrator = make_recovery_orchestrator(backend, state=state)

        orchestrator.save_queue_crash_safe()

        assert _uids(backend.load_queue()) == ["t::dup"]


class TestLoadAndRepair:
    def test_load_and_repair_full_flow(self, tmp_path):
        """四连加载 → 交集收敛 → 队列修复 → 落 store。"""
        backend = InMemoryStateBackend()
        backend.seed_successes(["t::overlap", "t::done"])
        backend.append_failed("t::overlap", {"error_message": "legacy"})
        backend.enqueue_jobs([
            {"task_type": "t", "job_id": "done", "trigger": "once"},  # successes 残留
            {"task_type": "t", "job_id": "fresh"},
        ])
        channel = ExecutionChannel(tmp_path)
        resource_mgr = ResourceManager({"api": CapacityResource("api", max_capacity=1.0)})
        store = StoreShim(backend)
        orchestrator = RecoveryOrchestrator(
            store=store, channel=channel, resources=resource_mgr,
            in_flight=InFlightTracker(), trigger_registry=TriggerRegistry(functions=BUILTIN_TRIGGER_FUNCTIONS),
            completion=MagicMock(),
        )

        state = orchestrator.load_and_repair()

        assert store.state is state
        assert "t::overlap" not in state.successes and "t::overlap" in state.failed
        assert [f"{j['task_type']}::{j['job_id']}" for j in state.queue] == ["t::done", "t::fresh"]


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
        backend = InMemoryStateBackend()
        ipc_dir = tmp_path / "ipc"
        ipc_dir.mkdir(parents=True, exist_ok=True)
        channel = ExecutionChannel(ipc_dir)
        resource_mgr = resource_mgr if resource_mgr is not None else ResourceManager(
            {"api": CapacityResource("api", max_capacity=1.0)}
        )
        completion = RecordingCompletion()
        orchestrator = RecoveryOrchestrator(
            store=StoreShim(backend), channel=channel, resources=resource_mgr,
            in_flight=InFlightTracker(), trigger_registry=TriggerRegistry(functions=BUILTIN_TRIGGER_FUNCTIONS),
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
