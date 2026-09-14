"""repair_queue_on_load 差量落盘降级回归测试。

启动期队列修复的差量删除（``delete_queue_uids``）失败必须降级告警：
内存 clean_q 已过滤残留行，本次 run 不受影响，磁盘保持原状、下次加载
重判（幂等）——与姊妹操作 ``converge_terminal_overlap`` 的降级策略一致。
后端删除路径遇瞬态故障（锁忙、磁盘瞬时只读）直接 re-raise 会穿透启动
流程炸掉整个 run。
"""

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.policy import ExecutionPolicy
from tasklite.engine.recovery import RecoveryOrchestrator
from tasklite.engine.store import StateStore
from tasklite.models.state import PipelineState
from tasklite.taxonomy import ErrorTaxonomy


def _make_recovery(backend):
    """显式装配 repair_queue_on_load 的窄依赖（其余协作者本路径不触及）。"""
    policy = ExecutionPolicy()
    store = StateStore(
        backend,
        state=PipelineState({}, {}, {}, []),
        taxonomy=ErrorTaxonomy(),
        policy=policy,
    )
    return RecoveryOrchestrator(
        store=store,
        channel=None,
        resources=None,
        in_flight=None,
        policy=policy,
        completion=None,
    )


def _queue_snapshot():
    """一行 wall 残留（应被过滤删除）+ 一行正常作业（应原样保留）。"""
    return [
        {"task_type": "t", "job_id": "residue", "payload": {}},
        {"task_type": "t", "job_id": "live", "payload": {}},
    ]


class TestRepairPersistenceDegrades:
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
        backend = backend_factory(tmp_path)
        recovery = _make_recovery(backend)

        def failing_delete(uids):
            raise OSError("database is locked")

        monkeypatch.setattr(backend, "delete_queue_uids", failing_delete)

        wall = {"t::residue": {"error": "stale"}}
        clean_q = recovery.repair_queue_on_load(_queue_snapshot(), wall, {})

        # 内存态照常收敛：残留被过滤、正常行保留（本次 run 不受影响）
        assert [jd["job_id"] for jd in clean_q] == ["live"]
