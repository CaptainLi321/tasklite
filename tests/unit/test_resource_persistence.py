"""2.8e：资源 suspend 状态跨重启持久化（meta 表）回归测试。

场景（ARCHITECTURE_REPORT 2.8e）：handler 对 "api" 资源 suspend 1 小时
（429 限流），进程 10 分钟后重启——若不持久化，suspend_until 随进程
消失，重启即放行再打 API。本文件验证挂起截止在 run 结束时写入 meta
表（wall-clock 语义）、重启后按换算恢复。
"""

import json
import time

from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.resource import RateLimitResource
from tasklite.models.job import Job

from tests.helpers import (
    make_fake_process_class,
    make_ipc_process_class,
    make_pipeline,
    patch_multiprocessing_for_fakes,
)

META_KEY = "resource_suspends"

# FakeProcess 内部用 ForkingPickler 校验可 pickle——handler 用模块级函数
def _ok_handler(job, ctx):
    return True


def _db_path(tmp_path):
    return tmp_path / "state" / "test_pipeline_state.db"


class TestResourceSuspendPersistence:
    def test_suspend_restored_after_pipeline_restart(self, tmp_path, monkeypatch):
        """suspend 后 run 结束 → meta 落盘 → 新 pipeline 加载恢复（±1s 容差）。

        fake 进程通过结果文件的 ``resource_suspensions`` 应用 300s 挂起
        （与真实子进程的 ctx.suspend_resource 同一条 _complete_job 路径）。
        """
        patch_multiprocessing_for_fakes(
            monkeypatch,
            fake_process_class=make_ipc_process_class(results=[
                {
                    "status": "success",
                    "raw_result": True,
                    "new_jobs": [],
                    "resource_suspensions": [("api", 300.0)],
                    "cursor_updates": {},
                }
            ]),
        )
        p1 = make_pipeline(tmp_path)
        p1.add_resource(RateLimitResource("api", interval_seconds=1.0))
        p1.register_handler("hit_api", _ok_handler)
        p1.enqueue([Job("hit_api", "j1")])
        p1.run()

        # run 结束时 _persist_resource_suspends 已把挂起截止写入 meta 表
        backend = SQLiteStateBackend(_db_path(tmp_path))
        raw = backend.get_meta(META_KEY)
        assert raw is not None
        deadlines = json.loads(raw)
        assert "api" in deadlines
        # wall-clock 截止 ≈ 持久化时刻 + 300s（允许 run 尾部耗时略少）
        assert 299.0 < deadlines["api"] - time.time() <= 300.0

        # 新 pipeline（同一 db）加载：挂起恢复，剩余时长 ≈ 300s（±1s 容差）
        p2 = make_pipeline(tmp_path)
        p2.add_resource(RateLimitResource("api", interval_seconds=1.0))
        p2.register_handler("hit_api", _ok_handler)
        p2.run()  # 空队列：_run_body 加载 meta 后立即退出
        remaining = p2.resources["api"].next_available - time.monotonic()
        assert abs(remaining - 300.0) <= 1.0

    def test_expired_suspend_not_restored(self, tmp_path, monkeypatch):
        """meta 中挂起已过期（过去时刻）→ 加载后资源不暂停。"""
        patch_multiprocessing_for_fakes(monkeypatch)
        backend = SQLiteStateBackend(_db_path(tmp_path))
        backend.set_meta(META_KEY, json.dumps({"api": time.time() - 100.0}))
        p = make_pipeline(tmp_path)
        p.add_resource(RateLimitResource("api", interval_seconds=1.0))
        p.run()  # 空队列：加载 meta 后立即退出，不抛异常
        assert p.resources["api"].next_available <= time.monotonic()

    def test_corrupted_meta_does_not_crash(self, tmp_path, monkeypatch):
        """损坏 JSON → 加载不崩、资源不暂停。"""
        patch_multiprocessing_for_fakes(monkeypatch)
        backend = SQLiteStateBackend(_db_path(tmp_path))
        backend.set_meta(META_KEY, "{corrupt json!!")
        p = make_pipeline(tmp_path)
        p.add_resource(RateLimitResource("api", interval_seconds=1.0))
        p.run()  # 空队列：加载 meta 后立即退出，不抛异常
        assert p.resources["api"].next_available <= time.monotonic()
