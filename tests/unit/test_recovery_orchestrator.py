"""RecoveryOrchestrator 故障恢复编排深模块单元测试套件。"""

import logging
import math
import time
from unittest.mock import MagicMock
import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.policy import PreflightPolicy
from tasklite.engine.recovery import RecoveryOrchestrator, RecoveryMachine
from tasklite.engine.resource import (
    META_RESOURCE_SUSPENDS,
    RateLimitResource,
    ResourceManager,
)
from tasklite.models.job import (
    RT_BACKOFF_UNTIL,
    RT_BACKOFF_WALL_DEADLINE,
)
from tasklite.engine.store import StateStore
from tasklite.models.job import Job
from tasklite.models.state import PipelineState


def make_orchestrator(backend, state=None, resource_mgr=None):
    """显式装配 RecoveryOrchestrator 的窄依赖集合。"""
    state = state or PipelineState({}, {}, {}, [])
    return RecoveryOrchestrator(
        store=StateStore(backend, state=state),
        channel=MagicMock(),
        resources=resource_mgr or ResourceManager(),
        in_flight=MagicMock(),
        policy=PreflightPolicy(),
        completion=MagicMock(),
    )


class TestRecoveryOrchestratorQueueRepair:
    def test_repair_queue_backoff_conversion(self):
        backend = InMemoryStateBackend()
        orchestrator = make_orchestrator(backend)

        now_wall = time.time()
        # 1. 过去已过期截止时间 -> 应当被清除
        expired_job = {"task_type": "t", "job_id": "j1", "runtime": {RT_BACKOFF_WALL_DEADLINE: now_wall - 10.0}}
        # 2. 未来截止时间 -> 应当被换算为 _backoff_until
        future_job = {"task_type": "t", "job_id": "j2", "runtime": {RT_BACKOFF_WALL_DEADLINE: now_wall + 5.0}}
        # 3. 重复 UID 作业 -> 应当保留第一条
        duplicate_job = {"task_type": "t", "job_id": "j1", "runtime": {}}

        q_data = [expired_job, future_job, duplicate_job]
        repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})

        assert len(repaired) == 2
        assert RT_BACKOFF_WALL_DEADLINE not in repaired[0]["runtime"]
        assert RT_BACKOFF_UNTIL not in repaired[0]["runtime"]

        assert RT_BACKOFF_UNTIL in repaired[1]["runtime"]
        assert repaired[1]["runtime"][RT_BACKOFF_UNTIL] > time.monotonic()

    def test_repair_queue_filters_wall_and_failed_unless_rerun(self):
        backend = InMemoryStateBackend()
        orchestrator = make_orchestrator(backend)

        # 在 wall 中且 rerun="never" -> 过滤
        job_wall = {"task_type": "t", "job_id": "w1", "rerun": "never"}
        # 在 wall 中但 rerun="every_run" -> 保留
        job_every_run = {"task_type": "t", "job_id": "w2", "rerun": "every_run"}
        # 新作业 -> 保留
        job_fresh = {"task_type": "t", "job_id": "f1"}

        wall = {"t::w1": {}, "t::w2": {}}
        repaired = orchestrator.repair_queue_on_load(
            [job_wall, job_every_run, job_fresh], wall=wall, failed={}
        )

        uids = [j["task_type"] + "::" + j["job_id"] for j in repaired]
        assert "t::w1" not in uids
        assert "t::w2" in uids
        assert "t::f1" in uids


class TestRecoveryOrchestratorRepairDeltaPersist:
    """repair 落盘必须是差量定向删除：陈旧加载快照不得全表覆盖磁盘真相。

    并发语义以受控注入模拟：在 load 之后、repair 落盘之前注入一次
    enqueue_jobs（等价于他进程已应答成功的一次单事务入队），不依赖真实
    多进程竞速。
    """

    @staticmethod
    def _uids(jobs):
        return [j["task_type"] + "::" + j["job_id"] for j in jobs]

    def test_repair_preserves_job_enqueued_during_repair_window(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "t", "job_id": "r1", "rerun": "never"},  # wall 残留，应清理
            {"task_type": "t", "job_id": "k1"},
        ])
        q_data = backend.load_queue()
        # 注入「load 之后、repair 落盘之前」他进程已应答成功的入队
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        # 全表重写即回归本缺陷：一旦回退为 save_queue(clean_q) 此处立即失败
        def _poison_full_rewrite(jobs):
            raise AssertionError("repair 不得以陈旧快照全表重写队列")

        backend.save_queue = _poison_full_rewrite

        orchestrator = make_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(
            q_data, wall={"t::r1": {}}, failed={}
        )

        disk_uids = set(self._uids(backend.load_queue()))
        assert "t::late" in disk_uids
        assert "t::k1" in disk_uids
        assert "t::r1" not in disk_uids
        # 窗口期入队的作业同时并入本次 run 的内存队列
        assert set(self._uids(repaired)) == {"t::k1", "t::late"}

    def test_repair_deletes_only_residual_rows_from_disk(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "t", "job_id": "r1", "rerun": "never"},
            {"task_type": "t", "job_id": "k1"},
            {"task_type": "t", "job_id": "r2", "rerun": "never"},
            {"task_type": "t", "job_id": "k2"},
        ])
        q_data = backend.load_queue()

        deleted_calls = []
        orig_delete = backend.delete_queue_uids

        def _recording_delete(uids):
            deleted_calls.append(list(uids))
            return orig_delete(uids)

        backend.delete_queue_uids = _recording_delete

        orchestrator = make_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(
            q_data, wall={"t::r1": {}}, failed={"t::r2": {}}
        )

        # 只定向删除残留行，保留行保持原序
        assert self._uids(backend.load_queue()) == ["t::k1", "t::k2"]
        assert self._uids(repaired) == ["t::k1", "t::k2"]
        assert set(deleted_calls[0]) == {"t::r1", "t::r2"}

    def test_repair_delta_persist_aligned_on_memory_backend(self):
        backend = InMemoryStateBackend()
        backend.save_queue([
            {"task_type": "t", "job_id": "r1", "rerun": "never"},
            {"task_type": "t", "job_id": "k1"},
        ])
        q_data = backend.load_queue()
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        orchestrator = make_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(
            q_data, wall={"t::r1": {}}, failed={}
        )

        disk_uids = set(self._uids(backend.load_queue()))
        assert disk_uids == {"t::k1", "t::late"}
        assert set(self._uids(repaired)) == {"t::k1", "t::late"}


class TestRecoveryOrchestratorRepairDuplicateWarning:
    """repair 合并的去重告警语义：仅真实异常重复告警。

    磁盘合并重读必然与加载快照全员重逢（镜像行）——镜像行不得进入
    duplicate 告警分支，否则每次启动对队列每条存量作业误报一条，大队列
    日志洪水淹没真实漂移信号；合并结果（行集与顺序）不受告警语义修正影响。
    """

    def test_disk_mirror_rows_do_not_emit_duplicate_warnings(self, tmp_path, caplog):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "t", "job_id": "j1"},
            {"task_type": "t", "job_id": "j2"},
            {"task_type": "t", "job_id": "j3"},
        ])
        q_data = backend.load_queue()
        # 窗口期并发入队：磁盘重读比快照多一行（合并预期内的新增，非重复）
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        orchestrator = make_orchestrator(backend)
        with caplog.at_level(logging.WARNING, logger="tasklite"):
            repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})

        dup_msgs = [
            r.getMessage() for r in caplog.records if "duplicate uid" in r.getMessage()
        ]
        assert dup_msgs == [], f"磁盘镜像行不得触发 duplicate 告警: {dup_msgs}"
        assert [j["job_id"] for j in repaired] == ["j1", "j2", "j3", "late"]

    def test_snapshot_internal_duplicate_still_warns(self, caplog):
        backend = InMemoryStateBackend()
        orchestrator = make_orchestrator(backend)
        q_data = [
            {"task_type": "t", "job_id": "j1"},
            {"task_type": "t", "job_id": "j1"},
        ]

        with caplog.at_level(logging.WARNING, logger="tasklite"):
            repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})

        dup_msgs = [
            r.getMessage() for r in caplog.records if "duplicate uid" in r.getMessage()
        ]
        assert len(dup_msgs) == 1, f"快照内部真实重复必须恰好告警一次: {dup_msgs}"
        assert "t::j1" in dup_msgs[0]
        assert [j["job_id"] for j in repaired] == ["j1"]


class TestRecoveryOrchestratorTerminalOverlapConvergence:
    """加载期终态交集收敛：wall∩failed 违例数据在构建运行态前确定性收敛。

    不变式：wall/failed 全局互斥；违例交集若流入运行态，首个作业派发的
    in-flight 登记触发的全量互斥断言即崩溃循环。收敛必须 failed 优先
    （DLQ 保留失败证据供人工核查重跑）、按 uid 差量定向删除（磁盘其余
    行原样保留），并留 WARNING 诊断证据。
    """

    def test_overlap_converged_to_failed_in_memory_and_disk(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.seed_wall(["t::x", "t::clean"])
        backend.append_failed("t::x", {"error": "boom"})
        wall = backend.load_wall()
        failed = backend.load_failed()
        orchestrator = make_orchestrator(backend)

        orchestrator.converge_terminal_overlap(wall, failed)

        # failed 优先：交集 uid 从内存 wall 剔除，DLQ 证据保留
        assert "t::x" not in wall
        assert "t::clean" in wall
        assert "t::x" in failed
        # 磁盘差量收敛：仅违例行剔除，其余行原样保留
        assert "t::x" not in backend.load_wall()
        assert "t::clean" in backend.load_wall()
        assert "t::x" in backend.load_failed()

    def test_convergence_emits_warning_listing_conflict_uids(self, caplog):
        backend = InMemoryStateBackend()
        orchestrator = make_orchestrator(backend)
        wall = {"t::x": {}, "t::y": {}}
        failed = {"t::x": {}}

        with caplog.at_level(logging.WARNING, logger="tasklite"):
            orchestrator.converge_terminal_overlap(wall, failed)

        warns = [
            r.getMessage() for r in caplog.records
            if "both wall and failed" in r.getMessage()
        ]
        assert len(warns) == 1, f"交集收敛必须留恰好一条 WARNING 证据: {warns}"
        assert "t::x" in warns[0]

    def test_disjoint_terminal_sets_untouched_and_silent(self, caplog):
        backend = InMemoryStateBackend()
        orchestrator = make_orchestrator(backend)
        wall = {"t::w": {}}
        failed = {"t::f": {}}

        with caplog.at_level(logging.WARNING, logger="tasklite"):
            orchestrator.converge_terminal_overlap(wall, failed)

        assert wall == {"t::w": {}}
        assert failed == {"t::f": {}}
        assert caplog.records == []

    def test_backend_delete_failure_degrades_without_losing_convergence(self, caplog):
        backend = MagicMock()
        backend.delete_wall.side_effect = RuntimeError("disk gone")
        orchestrator = make_orchestrator(backend)
        wall = {"t::x": {}}
        failed = {"t::x": {}}

        with caplog.at_level(logging.WARNING, logger="tasklite"):
            orchestrator.converge_terminal_overlap(wall, failed)

        # 落盘失败不回退内存收敛（本次 run 不受影响），磁盘下次加载重判
        assert "t::x" not in wall
        assert any(
            "re-converge on next load" in r.getMessage() for r in caplog.records
        )


class TestRecoveryOrchestratorRepairFrontPriority:
    """repair 窗口期 front 入队的队首优先语义（磁盘真相序权威）。

    窗口期他进程 front 入队的作业在磁盘真相中位于队首（seq 最小），合并
    必须保持其磁盘位置而非 append 到内存队尾——否则队首优先失效，且随后
    停机 crash-safe 落盘会把错误顺序固化到磁盘（磁盘/内存序分叉）。
    """

    def test_front_enqueued_during_window_keeps_head_position(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([{"task_type": "t", "job_id": "k1"}])
        q_data = backend.load_queue()
        # 窗口期他进程 front 入队：磁盘真相中位于队首
        backend.enqueue_jobs([{"task_type": "t", "job_id": "front_job"}], front=True)

        orchestrator = make_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})
        assert [j["job_id"] for j in repaired] == ["front_job", "k1"]

        # 停机落盘固化验证：合并序写入 store 后经 crash-safe 落盘，磁盘序
        # 不得被内存中的错误队尾 append 翻转
        state = PipelineState(
            backend.load_wall(),
            backend.load_failed(),
            backend.load_cursors(),
            repaired,
        )
        orchestrator = make_orchestrator(backend, state=state)
        orchestrator.save_queue_crash_safe()
        assert [j["job_id"] for j in backend.load_queue()] == ["front_job", "k1"]

    def test_back_enqueued_during_window_appends_after_existing(self, tmp_path):
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([{"task_type": "t", "job_id": "k1"}])
        q_data = backend.load_queue()
        backend.enqueue_jobs([{"task_type": "t", "job_id": "tail_job"}])

        orchestrator = make_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})
        assert [j["job_id"] for j in repaired] == ["k1", "tail_job"]

    def test_snapshot_rows_missing_on_disk_are_kept_at_tail(self, tmp_path):
        """快照独有行（窗口期被他进程消费）不丢行，但让位磁盘真相序队尾。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "t", "job_id": "k1"},
            {"task_type": "t", "job_id": "k2"},
        ])
        q_data = backend.load_queue()
        # 窗口期他进程消费 k1（delta 删除），并 front 入队新作业
        backend.delete_queue_uids(["t::k1"])
        backend.enqueue_jobs([{"task_type": "t", "job_id": "front_job"}], front=True)

        orchestrator = make_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})
        assert [j["job_id"] for j in repaired] == ["front_job", "k2", "k1"]

    def test_front_window_priority_aligned_on_memory_backend(self):
        backend = InMemoryStateBackend()
        backend.save_queue([{"task_type": "t", "job_id": "k1"}])
        q_data = backend.load_queue()
        backend.enqueue_jobs([{"task_type": "t", "job_id": "front_job"}], front=True)

        orchestrator = make_orchestrator(backend)
        repaired = orchestrator.repair_queue_on_load(q_data, wall={}, failed={})
        assert [j["job_id"] for j in repaired] == ["front_job", "k1"]


class TestRecoveryOrchestratorCrashSafeSave:
    def test_crash_safe_save_recovers_missing_jobs_from_disk(self):
        backend = InMemoryStateBackend()
        # 磁盘上有 j1, j2
        backend.save_queue([
            {"task_type": "t", "job_id": "j1"},
            {"task_type": "t", "job_id": "j2"},
        ])

        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "j2"}])

        orchestrator = make_orchestrator(backend, state=state)

        orchestrator.save_queue_crash_safe()

        disk_q = backend.load_queue()
        disk_uids = [j["task_type"] + "::" + j["job_id"] for j in disk_q]
        # j1 必须被补回队首
        assert disk_uids == ["t::j1", "t::j2"]

    def test_crash_safe_save_preserves_job_enqueued_in_merge_window(self, tmp_path, monkeypatch):
        """合并窗口内他进程已应答成功的入队必须在保存后仍存在于磁盘。

        磁盘上的 ``late`` 等价于「读磁盘真相与写回之间」他进程 enqueue
        已提交的作业（已落盘、本进程内存未感知）：整表覆盖保存会将其
        静默抹除（磁盘内存双失、永不再现）。同时结构锁定：合并保存必须
        经单事务原语收敛读-改-写，禁止退化为独立 load/save 两段式。
        """
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([{"task_type": "t", "job_id": "k1"}])
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])
        orig_load = backend.load_queue  # monkeypatch 前保存原始读法（断言用）

        # 内存队列持 k1 的更新版本（runtime/payload 演进），未感知 late
        state = PipelineState(
            {}, {}, {}, [{"task_type": "t", "job_id": "k1", "payload": {"v": 2}}]
        )
        orchestrator = make_orchestrator(backend, state=state)

        legacy_calls = []
        monkeypatch.setattr(backend, "load_queue", lambda: legacy_calls.append("load"))
        monkeypatch.setattr(backend, "save_queue", lambda jobs: legacy_calls.append("save"))

        orchestrator.save_queue_crash_safe()

        assert legacy_calls == [], "合并保存必须经单事务原语，不得两段式 load/save"
        disk_q = orig_load()
        disk_uids = [j["task_type"] + "::" + j["job_id"] for j in disk_q]
        # 窗口期入队被磁盘真相合并保留并补回队首；k1 以内存版本内容落盘
        assert disk_uids == ["t::late", "t::k1"]
        assert disk_q[1]["payload"] == {"v": 2}

    def test_crash_safe_save_merge_window_aligned_on_memory_backend(self):
        backend = InMemoryStateBackend()
        backend.save_queue([{"task_type": "t", "job_id": "k1"}])
        backend.enqueue_jobs([{"task_type": "t", "job_id": "late"}])

        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "k1"}])
        orchestrator = make_orchestrator(backend, state=state)

        orchestrator.save_queue_crash_safe()

        disk_uids = [j["task_type"] + "::" + j["job_id"] for j in backend.load_queue()]
        assert disk_uids == ["t::late", "t::k1"]

    def test_crash_safe_save_skips_overwrite_if_disk_load_fails(self):
        backend = InMemoryStateBackend()
        backend.save_queue([{"task_type": "t", "job_id": "safe_on_disk"}])

        # 模拟保存原语失败（读真相/写回任一阶段）
        def _boom(compute):
            raise IOError("Disk corruption")

        backend.replace_queue_atomic = _boom

        state = PipelineState({}, {}, {}, [{"task_type": "t", "job_id": "in_mem"}])
        orchestrator = make_orchestrator(backend, state=state)

        # 不应抛出异常，也不应覆盖磁盘（内存独有作业由 at-least-once 吸收）
        orchestrator.save_queue_crash_safe()

        disk_uids = [j["task_type"] + "::" + j["job_id"] for j in backend.load_queue()]
        assert disk_uids == ["t::safe_on_disk"]


class TestRecoveryOrchestratorSuspendPersistence:
    def test_apply_pending_signals_persists_suspensions(self):
        backend = InMemoryStateBackend()
        rm = ResourceManager({"api": RateLimitResource("api", 1.0)})
        in_flight = MagicMock()
        in_flight.active_uids.return_value = ["t::j1"]
        channel = MagicMock()
        # channel.drain_active_signals 返回一条挂起信号
        channel.drain_active_signals.return_value = [("t::j1", "api", 30.0)]

        orchestrator = RecoveryOrchestrator(
            store=StateStore(backend),
            channel=channel,
            resources=rm,
            in_flight=in_flight,
            policy=PreflightPolicy(),
            completion=MagicMock(),
        )

        orchestrator.apply_pending_signals()

        assert backend.get_meta(META_RESOURCE_SUSPENDS) is not None


class TestRecoveryOrchestratorResidueSalvage:
    """启动期残留信号回收：排空应用 + 即时持久化 + 异常隔离。"""

    def test_salvage_residue_signals_applies_and_persists(self):
        from tasklite.utils.jsonutil import loads

        backend = InMemoryStateBackend()
        rm = ResourceManager({"api": RateLimitResource("api", 1.0)})
        channel = MagicMock()
        channel.drain_all_signals.return_value = [("t::j1", "api", 30.0)]
        orchestrator = RecoveryOrchestrator(
            store=StateStore(backend),
            channel=channel,
            resources=rm,
            in_flight=MagicMock(),
            policy=PreflightPolicy(),
            completion=MagicMock(),
        )

        orchestrator.salvage_residue_signals()

        assert "api" in rm.collect_suspensions()
        persisted = loads(backend.get_meta(META_RESOURCE_SUSPENDS))
        assert "api" in persisted

    def test_salvage_residue_signals_channel_failure_is_isolated(self):
        """清扫原语异常只降级告警，不得打断启动序列。"""
        backend = InMemoryStateBackend()
        channel = MagicMock()
        channel.drain_all_signals.side_effect = OSError("ipc dir unavailable")
        orchestrator = RecoveryOrchestrator(
            store=StateStore(backend),
            channel=channel,
            resources=ResourceManager({"api": RateLimitResource("api", 1.0)}),
            in_flight=MagicMock(),
            policy=PreflightPolicy(),
            completion=MagicMock(),
        )

        orchestrator.salvage_residue_signals()

        assert backend.get_meta(META_RESOURCE_SUSPENDS) is None

    def test_salvage_residue_signals_skips_unregistered_resource(self):
        channel = MagicMock()
        channel.drain_all_signals.return_value = [("t::j1", "no_such", 9.0)]
        backend = InMemoryStateBackend()
        orchestrator = RecoveryOrchestrator(
            store=StateStore(backend),
            channel=channel,
            resources=ResourceManager({"api": RateLimitResource("api", 1.0)}),
            in_flight=MagicMock(),
            policy=PreflightPolicy(),
            completion=MagicMock(),
        )

        orchestrator.salvage_residue_signals()

        assert backend.get_meta(META_RESOURCE_SUSPENDS) is None


class TestRecoveryOrchestratorCompatibility:
    def test_alias_equivalence(self):
        assert RecoveryMachine is RecoveryOrchestrator
