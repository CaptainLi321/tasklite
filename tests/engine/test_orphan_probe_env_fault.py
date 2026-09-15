"""锁路径环境故障 fail-soft 契约测试。

不变式：锁文件遭遇环境故障（权限损坏/目录占位/路径超长等 OSError）时，
主进程孤儿探测与 worker 入口锁获取都必须按孤儿锁冲突瞬态信号同构降级
——零重试预算、降级写盘回队、零污染（不进 DLQ），绝不放大为整 run
崩溃；环境故障与「锁被占」两种语义在日志层保留区分（errno 归因）。
"""
import threading
import time
from pathlib import Path
from types import SimpleNamespace

from tasklite.engine.channel import WorkerLaunchSpec, _mp_worker_wrapper
from tasklite.utils.injective import safe_uid_filename
from tasklite.utils.ipc import ArtifactJournal
from tasklite.models.job import Job

from tests.helpers import make_pipeline


def _poison_lock_dir(ipc_dir: Path, uid: str) -> Path:
    """把 uid 的锁文件路径预置为目录，使 os.open 抛 IsADirectoryError。"""
    lock_path = Path(ipc_dir) / f"{safe_uid_filename(uid)}.lock"
    lock_path.mkdir(parents=True, exist_ok=True)
    return lock_path


class TestOrphanProbeEnvFault:
    """派发孤儿探测关：锁路径环境故障 defer，不穿透 run。"""

    def test_poisoned_lock_path_defers_without_crashing_run(self, tmp_path):
        """锁文件被目录占位 → run 正常终结、作业瞬态 defer、零 DLQ 污染。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        pipeline.enqueue([Job("t", "j1", payload={})])
        _poison_lock_dir(pipeline.ipc_dir, "t::j1")

        def controller():
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if pipeline.stats.get("deferred_orphan", 0) >= 1:
                    break
                time.sleep(0.01)
            pipeline.stop()

        t = threading.Thread(target=controller)
        t.start()
        # 环境故障必须收敛为作业级 defer，不得从 run() 穿透 OSError
        pipeline.run()
        t.join(timeout=10)

        assert pipeline.stats.get("deferred_orphan", 0) >= 1
        assert pipeline.list_dlq() == []


class TestWorkerEntryLockEnvFault:
    """worker 入口锁获取：环境故障写降级重试结果，不炸执行体。"""

    def test_lock_env_fault_writes_degraded_retry_result(self, tmp_path):
        """锁文件打不开（IsADirectoryError）→ 落盘 retry+lock_conflict 结果。"""
        ipc_dir = str(tmp_path / "ipc")
        uid = "t::j1"
        _poison_lock_dir(ipc_dir, uid)

        job = Job("t", "j1", payload={})
        spec = WorkerLaunchSpec(
            handler=lambda j, c: True,
            job=job,
            task_ctx=SimpleNamespace(new_jobs=[], resource_suspensions=[]),
            incarnation="r.1",
            ipc_dir=ipc_dir,
            timeout=10,
            result_token=None,
        )
        # 环境故障必须写降级结果后返回，不得以 OSError 炸穿执行体
        _mp_worker_wrapper(spec)

        journal = ArtifactJournal(ipc_dir)
        res = journal.read_result(journal.result_path(uid, "r.1"))
        assert res is not None
        assert res["status"] == "retry"
        assert res["lock_conflict"] is True
        assert "LOCK_ENV_FAULT" in res["error"]
