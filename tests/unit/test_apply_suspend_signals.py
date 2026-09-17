"""``apply_suspend_signals`` 共享助手契约：应用成功才持久化，坏信号降级跳过。

排空回收的 suspend 信号批（`(uid, resource, seconds)` 三元组）经单一
助手收敛：未注册资源的信号告警跳过不打断批处理；批内任一应用成功即
整体落盘一次 meta，空批/全失败批零落盘。
"""

import json

from tasklite.engine.resource import (
    META_RESOURCE_SUSPENDS,
    RateLimitResource,
    ResourceManager,
    apply_suspend_signals,
)


class _MetaRecorder:
    """记录 set_meta 调用的假后端（只实现助手依赖的最小面）。"""

    def __init__(self):
        self.calls = []

    def set_meta(self, key, value):
        self.calls.append((key, value))


def _mgr() -> ResourceManager:
    return ResourceManager(
        resources={"api": RateLimitResource("api", interval_seconds=1.0)}
    )


class TestApplySuspendSignals:
    def test_applied_signal_persists_once(self):
        backend = _MetaRecorder()
        mgr = _mgr()
        apply_suspend_signals([("j1", "api", 300.0)], backend, mgr, origin="from ")
        assert len(backend.calls) == 1
        key, raw = backend.calls[0]
        assert key == META_RESOURCE_SUSPENDS
        assert "api" in json.loads(raw)
        assert mgr["api"].suspended_until() is not None

    def test_unknown_resource_skipped_without_persist(self):
        backend = _MetaRecorder()
        mgr = _mgr()
        apply_suspend_signals([("j1", "nope", 10.0)], backend, mgr)
        assert backend.calls == []
        assert mgr["api"].suspended_until() is None

    def test_mixed_batch_applies_valid_and_persists(self):
        backend = _MetaRecorder()
        mgr = _mgr()
        apply_suspend_signals(
            [("j1", "nope", 10.0), ("j2", "api", 60.0)], backend, mgr
        )
        assert len(backend.calls) == 1
        assert mgr["api"].suspended_until() is not None

    def test_empty_batch_no_persist(self):
        backend = _MetaRecorder()
        apply_suspend_signals([], backend, _mgr())
        assert backend.calls == []
