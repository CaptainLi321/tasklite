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

from tasklite.v2.backend.memory import InMemoryStateBackend
from tasklite.v2.backend.sqlite_backend import SQLiteStateBackend
from tasklite.v2.engine.admission import RerunPolicy
from tasklite.v2.engine.channel import ExecutionChannel
from tasklite.v2.engine.in_flight import InFlightJob, InFlightTracker
from tasklite.v2.engine.recovery import RecoveryOrchestrator
from tasklite.v2.engine.resource import (
    META_RESOURCE_SUSPENSIONS,
    CapacityResource,
    RateLimitResource,
    ResourceManager,
)
from tasklite.v2.engine.types import JobHandle
from tasklite.v2.models.job import Job
from tasklite.v2.models.state import PipelineState
from tasklite.v2.utils.ipc import ArtifactJournal
from tasklite.v2.utils.jsonutil import dumps, loads

_INCARNATION = "deadbeefdeadbeefdeadbeefdeadbeef.1"


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

    def __init__(self, *, active_signals=(), all_signals=(), drain_all_error=None,
                 abort_outcome=None, claim_result=None):
        self.active_signals = list(active_signals)
        self.all_signals = list(all_signals)
        self.drain_all_error = drain_all_error
        self.abort_outcome = abort_outcome
        self.claim_result = claim_result

    def drain_active_signals(self, uids):
        return list(self.active_signals)

    def drain_all_signals(self):
        if self.drain_all_error is not None:
            raise self.drain_all_error
        return list(self.all_signals)

    def claim_stale_result(self, uid, job):
        return self.claim_result

    def abort_in_flight(self, handles):
        from tasklite.v2.engine.channel import AbortOutcome

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
        policy=policy if policy is not None else RerunPolicy(),
        completion=completion if completion is not None else MagicMock(),
    )


def _uids(jobs):
    return [f"{j['task_type']}::{j['job_id']}" for j in jobs]


class TestRepairQueueOnLoad:
    def test_repair_filters_wall_and_failed_unless_rerun(self):
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(backend)

        job_wall = {"task_type": "t", "job_id": "w_blocked", "rerun": "never"}
        job_every_run = {"task_type": "t", "job_id": "w_rerun", "rerun": "every_run"}
        job_failed_rerun = {"task_type": "t", "job_id": "f_rerun", "rerun": "on_failure"}
        job_fresh = {"task_type": "t", "job_id": "fresh"}

        wall = {"t::w_blocked": {}, "t::w_rerun": {}}
        failed = {"t::f_rerun": {}}
        repaired = orchestrator.repair_queue_on_load(
            [job_wall, job_every_run, job_failed_rerun, job_fresh],
            wall=wall, failed=failed,
        )

        assert _uids(repaired) == ["t::w_rerun", "t::f_rerun", "t::fresh"]
        # 放行保留的重跑行落字面 rerun 键（豁免登记以行内键为事实源）
        assert repaired[0]["rerun"] == "every_run"
        assert repaired[1]["rerun"] == "on_failure"

    def test_repair_runtime_passes_through_untouched(self):
        """无重试节奏字段的运行时状态原样穿透（重试节奏归 RequeuePolicy）。"""
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(backend)
        runtime = {"_commit_failures": 1, "_dispatch_failures": 2,
                   "_last_retry_error": "old", "user_extra": {"k": "v"}}
        job = {"task_type": "t", "job_id": "rt", "runtime": dict(runtime)}

        repaired = orchestrator.repair_queue_on_load([job], wall={}, failed={})

        assert repaired[0]["runtime"] == runtime, "修复不得改写 runtime 边带状态"

    def test_repair_keeps_rerun_none_row_when_exempt_by_task_default(self):
        """rerun=None 的行按有效策略（任务级默认）判定，落字面 effective 键。"""
        backend = InMemoryStateBackend()
        policy = RerunPolicy(discovery_rerun={"t": "every_run"})
        orchestrator = make_recovery_orchestrator(
            backend, policy=policy,
        )
        job = {"task_type": "t", "job_id": "dflt", "rerun": None}

        repaired = orchestrator.repair_queue_on_load(
            [job], wall={"t::dflt": {}}, failed={},
        )

        assert _uids(repaired) == ["t::dflt"]
        assert repaired[0]["rerun"] == "every_run", (
            "放行行必须落有效策略的字面键，豁免登记不得在加载态丢失"
        )


class TestRepairDeltaPersist:
    """repair 落盘必须是差量定向删除：陈旧加载快照不得全表覆盖磁盘真相。

    并发语义以受控注入模拟：在 load 之后、repair 落盘之前注入一次
    enqueue_jobs（等价于他进程已应答成功的一次单事务入队），不依赖真实
    多进程竞速。
    """

    def test_repair_preserves_job_enqueued_during_repair_window(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "t", "job_id": "residue_row", "rerun": "never"},  # wall 残留，应清理
            {"task_type": "t", "job_id": "keep_row"},
        ])
        q_data = backend.load_queue()
        # 注入「load 之后、repair 落盘之前」他进程已应答成功的入队
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        # 全表重写即回归本缺陷：一旦回退为 save_queue(clean_q) 此处立即失败
        def _poison_full_rewrite(jobs):
            raise AssertionError("repair 不得以陈旧快照全表重写队列")

        backend.save_queue = _poison_full_rewrite

        orchestrator = make_recovery_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(
            q_data, wall={"t::residue_row": {}}, failed={}
        )

        disk_uids = set(_uids(backend.load_queue()))
        assert "t::late" in disk_uids
        assert "t::keep_row" in disk_uids
        assert "t::residue_row" not in disk_uids
        # 窗口期入队的作业同时并入本次 run 的内存队列
        assert set(_uids(repaired)) == {"t::keep_row", "t::late"}

    def test_repair_deletes_only_residual_rows_from_disk(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "t", "job_id": "residue_row", "rerun": "never"},
            {"task_type": "t", "job_id": "keep_row"},
            {"task_type": "t", "job_id": "failed_residue", "rerun": "never"},
            {"task_type": "t", "job_id": "keep_second"},
        ])
        q_data = backend.load_queue()

        deleted_calls = []
        orig_delete = backend.delete_queue_uids

        def _recording_delete(uids):
            deleted_calls.append(list(uids))
            return orig_delete(uids)

        backend.delete_queue_uids = _recording_delete

        orchestrator = make_recovery_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(
            q_data, wall={"t::residue_row": {}}, failed={"t::failed_residue": {}}
        )

        # 只定向删除残留行，保留行保持原序
        assert _uids(backend.load_queue()) == ["t::keep_row", "t::keep_second"]
        assert _uids(repaired) == ["t::keep_row", "t::keep_second"]
        assert set(deleted_calls[0]) == {"t::residue_row", "t::failed_residue"}

    def test_repair_delta_persist_aligned_on_memory_backend(self):
        backend = InMemoryStateBackend()
        backend.save_queue([
            {"task_type": "t", "job_id": "residue_row", "rerun": "never"},
            {"task_type": "t", "job_id": "keep_row"},
        ])
        q_data = backend.load_queue()
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        orchestrator = make_recovery_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(
            q_data, wall={"t::residue_row": {}}, failed={}
        )

        assert set(_uids(backend.load_queue())) == {"t::keep_row", "t::late"}
        assert set(_uids(repaired)) == {"t::keep_row", "t::late"}

    @pytest.mark.parametrize(
        "backend_factory",
        [
            lambda tmp_path: InMemoryStateBackend(),
            lambda tmp_path: SQLiteStateBackend(tmp_path / "state.db"),
        ],
        ids=["memory", "sqlite"],
    )
    def test_delete_queue_uids_failure_degrades_instead_of_raising(
        self, tmp_path, monkeypatch, backend_factory
    ):
        """差量删除失败必须降级告警：内存态照常收敛，磁盘下次加载重判。"""
        backend = backend_factory(tmp_path)
        recovery = make_recovery_orchestrator(backend)

        def failing_delete(uids):
            raise OSError("database is locked")

        monkeypatch.setattr(backend, "delete_queue_uids", failing_delete)

        snapshot = [
            {"task_type": "t", "job_id": "residue", "rerun": "never"},
            {"task_type": "t", "job_id": "live"},
        ]
        clean_q = recovery.repair_queue_on_load(
            snapshot, {"t::residue": {"error": "stale"}}, {}
        )

        assert [jd["job_id"] for jd in clean_q] == ["live"], (
            "内存态照常收敛：本次 run 不受落盘降级影响"
        )


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
        with caplog.at_level(logging.WARNING, logger="tasklite.v2"):
            repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})

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

        with caplog.at_level(logging.WARNING, logger="tasklite.v2"):
            repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})

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
        with caplog.at_level(logging.WARNING, logger="tasklite.v2"):
            repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})

        assert _uids(repaired) == ["t::keep_row"], "重读失败以加载快照为准，不丢行"
        assert any("repair merge" in r.getMessage() for r in caplog.records)


class TestTerminalOverlapConvergence:
    """加载期终态交集收敛：wall∩failed 违例数据在构建运行态前确定性收敛。

    不变式：wall/failed 全局互斥；违例交集若流入运行态，首个作业派发的
    in-flight 登记触发的全量互斥断言即崩溃循环。收敛必须 failed 优先
    （失败档案保留失败证据供人工核查重跑）、按 uid 差量定向删除（磁盘
    其余行原样保留），并留 WARNING 诊断证据。
    """

    def test_overlap_converged_to_failed_in_memory_and_disk(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.seed_wall(["t::x", "t::clean"])
        backend.append_failed("t::x", {"error": "boom"})
        wall = backend.load_wall()
        failed = backend.load_failed()
        orchestrator = make_recovery_orchestrator(backend)

        orchestrator.converge_terminal_overlap(wall, failed)

        # failed 优先：交集 uid 从内存 wall 剔除，失败档案证据保留
        assert "t::x" not in wall
        assert "t::clean" in wall
        assert "t::x" in failed
        # 磁盘差量收敛：仅违例行剔除，其余行原样保留
        assert "t::x" not in backend.load_wall()
        assert "t::clean" in backend.load_wall()
        assert "t::x" in backend.load_failed()

    def test_convergence_emits_warning_listing_conflict_uids(self, caplog):
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(backend)
        wall = {"t::x": {}, "t::y": {}}
        failed = {"t::x": {}}

        with caplog.at_level(logging.WARNING, logger="tasklite.v2"):
            orchestrator.converge_terminal_overlap(wall, failed)

        warns = [
            r.getMessage() for r in caplog.records
            if "both wall and failed" in r.getMessage()
        ]
        assert len(warns) == 1, f"交集收敛必须留恰好一条 WARNING 证据: {warns}"
        assert "t::x" in warns[0]

    def test_disjoint_terminal_sets_untouched_and_silent(self, caplog):
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(backend)
        wall = {"t::w": {}}
        failed = {"t::f": {}}

        with caplog.at_level(logging.WARNING, logger="tasklite.v2"):
            orchestrator.converge_terminal_overlap(wall, failed)

        assert wall == {"t::w": {}}
        assert failed == {"t::f": {}}
        assert caplog.records == []

    def test_backend_delete_failure_degrades_without_losing_convergence(self, caplog):
        backend = MagicMock()
        backend.delete_wall.side_effect = RuntimeError("disk gone")
        orchestrator = make_recovery_orchestrator(backend)
        wall = {"t::x": {}}
        failed = {"t::x": {}}

        with caplog.at_level(logging.WARNING, logger="tasklite.v2"):
            orchestrator.converge_terminal_overlap(wall, failed)

        # 落盘失败不回退内存收敛（本次 run 不受影响），磁盘下次加载重判
        assert "t::x" not in wall
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
        repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})
        assert [j["job_id"] for j in repaired] == ["front_job", "keep_row"]

        # 停机落盘固化验证：合并序写入 store 后经 crash-safe 落盘，磁盘序
        # 不得被内存中的错误队尾 append 翻转
        state = PipelineState(
            backend.load_wall(),
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
        repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})
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
        repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})
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


class TestSuspendPersistenceAndRestore:
    def test_apply_pending_signals_persists_suspensions(self):
        backend = InMemoryStateBackend()
        resource_mgr = ResourceManager({"api": RateLimitResource("api", 1.0)})
        orchestrator = make_recovery_orchestrator(
            backend,
            resource_mgr=resource_mgr,
            channel=FakeChannel(active_signals=[("t::j1", "api", 30.0)]),
            in_flight=InFlightTracker(),
        )

        orchestrator.apply_pending_signals()

        assert backend.get_meta(META_RESOURCE_SUSPENSIONS) is not None

    def test_load_resource_suspensions_restores_into_manager(self):
        backend = InMemoryStateBackend()
        resource_mgr = ResourceManager({"api": CapacityResource("api", 1.0)})
        backend.set_meta(
            META_RESOURCE_SUSPENSIONS, dumps({"api": time.time() + 30.0})
        )
        orchestrator = make_recovery_orchestrator(backend, resource_mgr=resource_mgr)

        orchestrator.load_resource_suspensions()

        assert resource_mgr.collect_suspensions() != {}, "挂起截止必须被恢复注入"

    def test_load_resource_suspensions_ignores_corrupted_meta(self, caplog):
        backend = InMemoryStateBackend()
        resource_mgr = ResourceManager({"api": CapacityResource("api", 1.0)})
        backend.set_meta(META_RESOURCE_SUSPENSIONS, "not-json{{")
        orchestrator = make_recovery_orchestrator(backend, resource_mgr=resource_mgr)

        with caplog.at_level(logging.WARNING, logger="tasklite.v2"):
            orchestrator.load_resource_suspensions()

        assert resource_mgr.collect_suspensions() == {}
        assert any("Corrupted" in r.getMessage() for r in caplog.records)

    def test_load_resource_suspensions_ignores_non_dict_meta(self, caplog):
        backend = InMemoryStateBackend()
        resource_mgr = ResourceManager({"api": CapacityResource("api", 1.0)})
        backend.set_meta(META_RESOURCE_SUSPENSIONS, dumps([1, 2]))
        orchestrator = make_recovery_orchestrator(backend, resource_mgr=resource_mgr)

        with caplog.at_level(logging.WARNING, logger="tasklite.v2"):
            orchestrator.load_resource_suspensions()

        assert resource_mgr.collect_suspensions() == {}
        assert any("not a dict" in r.getMessage() for r in caplog.records)

    def test_persist_resource_suspensions_writes_meta(self):
        backend = InMemoryStateBackend()
        resource_mgr = ResourceManager({"api": RateLimitResource("api", 1.0)})
        resource_mgr.suspend_resource("api", 15.0)
        orchestrator = make_recovery_orchestrator(backend, resource_mgr=resource_mgr)

        orchestrator.persist_resource_suspensions()

        assert "api" in loads(backend.get_meta(META_RESOURCE_SUSPENSIONS))


class TestResidueSalvage:
    """启动期残留信号回收：排空应用 + 即时持久化 + 异常隔离。"""

    def test_salvage_residue_signals_applies_and_persists(self):
        backend = InMemoryStateBackend()
        resource_mgr = ResourceManager({"api": RateLimitResource("api", 1.0)})
        orchestrator = make_recovery_orchestrator(
            backend,
            resource_mgr=resource_mgr,
            channel=FakeChannel(all_signals=[("t::j1", "api", 30.0)]),
        )

        orchestrator.salvage_residue_signals()

        assert "api" in resource_mgr.collect_suspensions()
        persisted = loads(backend.get_meta(META_RESOURCE_SUSPENSIONS))
        assert "api" in persisted

    def test_salvage_residue_signals_channel_failure_is_isolated(self):
        """清扫原语异常只降级告警，不得打断启动序列。"""
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(
            backend,
            resource_mgr=ResourceManager({"api": RateLimitResource("api", 1.0)}),
            channel=FakeChannel(drain_all_error=OSError("ipc dir unavailable")),
        )

        orchestrator.salvage_residue_signals()

        assert backend.get_meta(META_RESOURCE_SUSPENSIONS) is None

    def test_salvage_residue_signals_skips_unregistered_resource(self):
        backend = InMemoryStateBackend()
        orchestrator = make_recovery_orchestrator(
            backend,
            resource_mgr=ResourceManager({"api": RateLimitResource("api", 1.0)}),
            channel=FakeChannel(all_signals=[("t::j1", "no_such", 9.0)]),
        )

        orchestrator.salvage_residue_signals()

        assert backend.get_meta(META_RESOURCE_SUSPENSIONS) is None

    def test_residue_signals_from_real_channel_journal(self, tmp_path):
        """真实 channel 落盘的跨 run 残留信号经启动清扫回收并应用。"""
        backend = InMemoryStateBackend()
        channel = ExecutionChannel(tmp_path)
        channel.journal.record_signal("t::j1", "api", 60.0)
        resource_mgr = ResourceManager({"api": CapacityResource("api", 1.0)})
        orchestrator = make_recovery_orchestrator(
            backend, resource_mgr=resource_mgr, channel=channel,
        )

        orchestrator.salvage_residue_signals()

        assert resource_mgr.collect_suspensions() != {}, "残留信号必须被应用"
        assert list(tmp_path.glob("*.signals.jsonl")) == [], "信号文件清扫干净"


class TestLoadAndRepair:
    def test_load_and_repair_full_flow(self, tmp_path):
        """四连加载 → 交集收敛 → 队列修复 → 挂起恢复 → 残留回收 → 落 store。"""
        backend = InMemoryStateBackend()
        backend.seed_wall(["t::overlap", "t::done"])
        backend.append_failed("t::overlap", {"error": "legacy"})
        backend.enqueue_jobs([
            {"task_type": "t", "job_id": "done", "rerun": "never"},  # wall 残留
            {"task_type": "t", "job_id": "fresh"},
        ])
        backend.set_meta(
            META_RESOURCE_SUSPENSIONS, dumps({"api": time.time() + 30.0})
        )
        channel = ExecutionChannel(tmp_path)
        channel.journal.record_signal("t::fresh", "api", 20.0)
        resource_mgr = ResourceManager({"api": CapacityResource("api", 1.0)})
        store = StoreShim(backend)
        orchestrator = RecoveryOrchestrator(
            store=store, channel=channel, resources=resource_mgr,
            in_flight=InFlightTracker(), policy=RerunPolicy(),
            completion=MagicMock(),
        )

        state = orchestrator.load_and_repair()

        assert store.state is state
        assert "t::overlap" not in state.wall and "t::overlap" in state.failed
        assert [f"{j['task_type']}::{j['job_id']}" for j in state.queue] == ["t::fresh"]
        assert resource_mgr.collect_suspensions() != {}, "挂起与残留信号都必须注入"
        assert list(tmp_path.glob("*.signals.jsonl")) == []


class TestRestoreStaleResult:
    def _make(self, tmp_path, *, channel=None, completion=None):
        backend = InMemoryStateBackend()
        channel = channel if channel is not None else ExecutionChannel(tmp_path)
        completion = completion if completion is not None else RecordingCompletion()
        orchestrator = make_recovery_orchestrator(
            backend, channel=channel,
            in_flight=InFlightTracker(), completion=completion,
        )
        return orchestrator, completion

    def test_no_residue_returns_false(self, tmp_path):
        orchestrator, completion = self._make(tmp_path)
        job = Job("t", "clean")

        assert orchestrator.restore_stale_result(job.uid, job, job.to_dict()) is False
        assert completion.completed_jobs == []

    def test_residue_consumed_via_pseudo_entry(self, tmp_path):
        """残留 success → 伪 entry 交完成机器统一收尾，不再派发子进程。"""
        orchestrator, completion = self._make(tmp_path)
        job = Job("t", "stale")
        ArtifactJournal(tmp_path).write_result_atomic(
            job.uid,
            {
                "status": "success",
                "raw_result": True,
                "new_jobs": [],
                "resource_suspensions": [],
                "cursor_updates": {},
            },
            incarnation=_INCARNATION,
        )

        assert orchestrator.restore_stale_result(job.uid, job, job.to_dict()) is True
        assert len(completion.completed_jobs) == 1
        entry, result = completion.completed_jobs[0]
        assert entry.is_pseudo, "残留消费必须走安全伪 entry（无资源、无进程）"
        assert entry.uid == job.uid
        assert result.success is True

    def test_forged_residue_rejected_by_token(self, tmp_path):
        """跨 run/伪造残留（令牌不匹配）按无残留处理，照常派发。"""
        channel = ExecutionChannel(tmp_path)
        channel.result_token = "b" * 64
        orchestrator, completion = self._make(tmp_path, channel=channel)
        job = Job("t", "forged")
        ArtifactJournal(tmp_path).write_result_atomic(
            job.uid,
            {
                "status": "success",
                "raw_result": {"poisoned": True},
                "new_jobs": [],
                "resource_suspensions": [],
                "cursor_updates": {},
            },
            incarnation=_INCARNATION,
        )

        assert orchestrator.restore_stale_result(job.uid, job, job.to_dict()) is False
        assert completion.completed_jobs == []

    def test_job_terminated_from_completion_is_contained(self, tmp_path):
        """残留提交触发失败档案阈值终结（_JobTerminated）不 re-raise。"""
        from tasklite.v2.exceptions import _JobTerminated

        completion = RecordingCompletion(complete_job_error=_JobTerminated("threshold"))
        orchestrator, _ = self._make(tmp_path, completion=completion)
        job = Job("t", "threshold")
        ArtifactJournal(tmp_path).write_result_atomic(
            job.uid, {"status": "fatal", "error": "boom", "traceback": "tb"},
            incarnation=_INCARNATION,
        )

        assert orchestrator.restore_stale_result(job.uid, job, job.to_dict()) is True


class _DoneProcess:
    """已完成 entry 的进程桩——abort 不得 kill 它（结果已落盘）。"""

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
    """kill() 时写最终结果 + suspend 信号的进程桩（TOCTOU 窗口复现）。"""

    def __init__(self, channel, uid, incarnation):
        self._alive = True
        self.exitcode = 0
        self.killed = False
        self._channel = channel
        self._uid = uid
        self._incarnation = incarnation

    def is_alive(self):
        return self._alive

    def join(self, timeout=None):
        pass

    def kill(self):
        self._alive = False
        self.killed = True
        self._channel.journal.write_result_atomic(
            self._uid,
            {"status": "success", "raw_result": {"done": 1}},
            incarnation=self._incarnation,
        )
        self._channel.journal.record_signal(self._uid, "api", 5.0)

    def close(self):
        pass


class TestAbortInFlight:
    def _make(self, tmp_path, *, resource_mgr=None):
        backend = InMemoryStateBackend()
        channel = ExecutionChannel(tmp_path)
        resource_mgr = resource_mgr if resource_mgr is not None else ResourceManager(
            {"api": CapacityResource("api", 1.0)}
        )
        completion = RecordingCompletion()
        orchestrator = RecoveryOrchestrator(
            store=StoreShim(backend), channel=channel, resources=resource_mgr,
            in_flight=InFlightTracker(), policy=RerunPolicy(),
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
        channel.journal.record_output(job.uid, str(out_file), cleanup=True)
        channel.journal.write_result_atomic(
            job.uid,
            {
                "status": "success", "raw_result": True,
                "new_jobs": [], "resource_suspensions": [], "cursor_updates": {},
            },
            incarnation=_INCARNATION,
        )
        process = _DoneProcess()
        handle = JobHandle(
            uid=job.uid, process=process,
            deadline=time.monotonic() + 60, timeout=60.0, job=job,
            ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )
        tracker = orchestrator._in_flight
        tracker.track(InFlightJob(job.uid, job.to_dict(), job, [], handle, None))

        orchestrator.abort_in_flight()

        assert process.killed is False, "已完成 entry 不得被 kill"
        cancelled, done = completion.settled[0]
        assert [e.uid for e, _ in done] == [job.uid], "已完成结果必须交完成机器提交"
        assert cancelled == []
        assert out_file.exists(), "成功产出的物理输出必须保留"

    def test_abort_toctou_result_appears_during_kill(self, tmp_path):
        """分类时无结果 → kill 窗口内 worker 写完结果 → 重探测移入 done 消费。"""
        orchestrator, channel, completion, _ = self._make(tmp_path)
        job = Job("fast", "toctou")
        process = _FinishOnKillProcess(channel, job.uid, _INCARNATION)
        handle = JobHandle(
            uid=job.uid, process=process,
            deadline=time.monotonic() + 60, timeout=60.0, job=job,
            ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )
        tracker = orchestrator._in_flight
        tracker.track(InFlightJob(job.uid, job.to_dict(), job, [], handle, None))

        orchestrator.abort_in_flight()

        cancelled, done = completion.settled[0]
        assert [e.uid for e, _ in done] == [job.uid], (
            "kill 窗口内落盘的结果必须被重探测消费（不删结果、不取消）"
        )
        assert cancelled == []
        assert done[0][1].resource_suspensions == [("api", 5.0)], (
            "终前写入的挂起信号并入结果（随完成提交链应用）"
        )

    def test_abort_preserves_suspend_signals(self, tmp_path):
        """abort 开头先排空 in-flight 的 suspend 信号并应用（kill 前捞回）。"""
        orchestrator, channel, _, resource_mgr = self._make(tmp_path)
        job = Job("fast", "suspend_job")
        channel.journal.record_signal(job.uid, "api", 60.0)
        process = _DoneProcess()
        handle = JobHandle(
            uid=job.uid, process=process,
            deadline=time.monotonic() + 60, timeout=60.0, job=job,
            ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )
        tracker = orchestrator._in_flight
        tracker.track(InFlightJob(job.uid, job.to_dict(), job, [], handle, None))

        orchestrator.abort_in_flight()

        assert resource_mgr.collect_suspensions() != {}, (
            "abort 必须消费并应用 suspend 信号，不随 kill 丢失"
        )

    def test_abort_applies_salvaged_signals_from_outcome(self, tmp_path):
        """channel 捞回的终前信号经 AbortOutcome 带回并应用（max 幂等合并）。"""
        from tasklite.v2.engine.channel import AbortOutcome, ExecutionResult

        backend = InMemoryStateBackend()
        resource_mgr = ResourceManager({"api": CapacityResource("api", 1.0)})
        completion = RecordingCompletion()
        job = Job("fast", "salvage_job")
        fake_channel = FakeChannel(
            abort_outcome=AbortOutcome(
                completed=[], cancelled=[],
                salvaged_signals=[(job.uid, "api", 30.0)],
            ),
            claim_result=None,
        )
        orchestrator = RecoveryOrchestrator(
            store=StoreShim(backend), channel=fake_channel, resources=resource_mgr,
            in_flight=InFlightTracker(), policy=RerunPolicy(),
            completion=completion,
        )
        tracker = orchestrator._in_flight
        tracker.track(InFlightJob(job.uid, job.to_dict(), job, [], None, None))

        orchestrator.abort_in_flight()

        assert resource_mgr.collect_suspensions() != {}, "捞回信号必须被应用"
        assert backend.get_meta(META_RESOURCE_SUSPENSIONS) is not None

    def test_abort_releases_resources_before_settlement(self, tmp_path):
        """已占用资源在结算前统一释放（防泄漏）。"""
        orchestrator, channel, completion, resource_mgr = self._make(tmp_path)
        job = Job("fast", "release_job")
        capacity = resource_mgr["api"]
        capacity.acquire(1.0)
        process = _DoneProcess()
        handle = JobHandle(
            uid=job.uid, process=process,
            deadline=time.monotonic() + 60, timeout=60.0, job=job,
            ipc_dir=str(tmp_path), incarnation=_INCARNATION,
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
