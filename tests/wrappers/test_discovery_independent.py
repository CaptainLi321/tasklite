"""G-DISCOVERY：discovery 扫描核心框架无关性测试。
架构要求：``utils/discovery.py`` 的扫描核心（``DiscoveryHandler``）只依赖
``DiscoveryJob``/``DiscoveryContext`` 两个最小协议，**不 import 任何
tasklite 框架模块**；``register_discovery`` 是框架默认适配器，只依赖
``DiscoveryHost`` 公开协议（register_handler + set_discovery_rerun）。
本文件用完全自制的 FakeJob/FakeCtx/FakeHost 验证这两条边界，不经过
TaskLite/Job/TaskContext 任何一个真实框架类型。
"""
import pytest
from tasklite.wrappers.discovery import (
    DiscoveryHandler,
    DiscoveryHost,
    register_discovery,
)

class FakeJob:
    """自制最小 job：只提供 payload（DiscoveryJob 协议的全部要求）。"""
    def __init__(self, payload):
        self.payload = payload

class FakeCtx:
    """自制最小 ctx：只实现 is_completed/is_failed/attempted_uids（DiscoveryContext 协议）。"""
    def __init__(self, done=(), failed=(), seen=()):
        self._done = set(done)
        self._failed = set(failed)
        self._seen = set(seen)
    def is_completed(self, uid):
        return uid in self._done
    def is_failed(self, uid):
        return uid in self._failed
    def attempted_uids(self):
        return frozenset(self._seen)

# 模块级回调（保持与生产契约一致的可 pickle 形态）

def _fetch(job, ctx, page):
    if page == 1:
        return ["new-a", "old-b"]
    if page == 2:
        return ["old-c"]
    return []

def _item_id(item):
    return item

def _process(job, ctx, item, content_id):
    processed.append(content_id)

def _missing_cb(job, ctx, missing):
    missing_reported.extend(missing)

processed = []
missing_reported = []

def _make_handler(**kwargs):
    return DiscoveryHandler(
        _fetch, _item_id, _process, "child", **kwargs,
    )

class TestDiscoveryHandlerFrameworkIndependent:
    def test_scan_runs_against_fake_job_and_ctx(self):
        """扫描核心只依赖协议：FakeJob/FakeCtx 不经任何框架类型即可完整跑通。"""
        processed.clear()
        ctx = FakeCtx(done={"child::old-b"})
        handler = _make_handler()
        assert handler(FakeJob({"artist_id": "a1"}), ctx) is True
        # old-b 已见被跳过；old-c 在第二页且未见 → 应处理；空页终止
        assert processed == ["new-a", "old-c"]
        # 契约 T2：第二页未整页命中（old-c 是新的）→ 继续到空页
        # 通过 processed 包含 old-c 已隐式验证。
    def test_full_mode_uses_attempted_uids_protocol_only(self):
        """full + on_missing 的差集计算只依赖 ctx.attempted_uids()。"""
        processed.clear()
        missing_reported.clear()
        # child::vanished 已见但数据源里已不存在（源端删除）
        ctx = FakeCtx(seen={"child::vanished", "child::old-b"})
        handler = _make_handler(scan_mode="full", on_missing=_missing_cb)
        assert handler(FakeJob({}), ctx) is True
        assert sorted(missing_reported) == ["vanished"]
    def test_incremental_full_page_hit_stops(self):
        """整页命中在 FakeCtx 上同样成立（协议成员判定即可）。"""
        processed.clear()
        ctx = FakeCtx(done={"child::new-a", "child::old-b"})
        handler = _make_handler()
        assert handler(FakeJob({}), ctx) is True
        assert processed == [], "整页命中后不得处理任何 item"

class FakeHost:
    """实现 DiscoveryHost 协议的自制宿主（不是 TaskLite）。"""
    def __init__(self):
        self.registered = []
    def register_handler(self, task_type, handler_func,
                         default_resources=None, payload_schema=None):
        self.registered.append((task_type, handler_func, default_resources, payload_schema))
    def set_discovery_rerun(self, task_type, rerun):
        self.registered.append(("rerun", task_type, rerun))

class TestRegisterDiscoveryFrameworkIndependent:
    def test_register_discovery_only_needs_discovery_host_protocol(self):
        """适配器只调用宿主两个公开方法，可用 FakeHost 完成注册。"""
        host = FakeHost()
        register_discovery(
            host, "discover", _fetch, _item_id, _process, "child",
            rerun="every_run",
        )
        handler_entry = host.registered[0]
        assert handler_entry[0] == "discover"
        assert isinstance(handler_entry[1], DiscoveryHandler), (
            "register_discovery 必须注册框架无关的 DiscoveryHandler"
        )
        # 不触碰任何私有字段：默认 rerun 经公开方法登记
        assert host.registered[1] == ("rerun", "discover", "every_run")
    def test_register_discovery_rejects_bad_host_fail_loud(self):
        """防御测试（D-1）：宿主缺协议方法时立即 AttributeError，而非静默。"""
        class BrokenHost:
            def register_handler(self, *a, **k):
                pass
            # 缺 set_discovery_rerun
        with pytest.raises(AttributeError):
            register_discovery(
                BrokenHost(), "discover", _fetch, _item_id, _process, "child",
            )

class TestTaskLitePublicDiscoveryRerun:
    def test_pipeline_public_method_validation(self, tmp_path):
        """TaskLite 是 DiscoveryHost 的默认实现，公开入口自带校验。"""
        from tasklite.pipeline import TaskLite
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite")
        with pytest.raises(ValueError, match="rerun must be one of"):
            p.set_discovery_rerun("d", "invalid")
        p.set_discovery_rerun("d", "on_failure")
        assert p._discovery_rerun["d"] == "on_failure"
