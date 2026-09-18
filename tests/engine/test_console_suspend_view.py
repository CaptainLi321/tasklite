"""list_suspends 运维视图回归。

契约：run() 外查询资源挂起的唯一真相源是 meta 表（``resource_suspends``，
``{resource: 挂钟 deadline}``）——挂起仅在 run() 启动时恢复进内存
ResourceManager，管理段查询内存态恒为空。已解封条目过滤、坏数据降级
跳过，与启动期恢复路径 fail-soft 语义一致。
"""
import time

import pytest

from tasklite.engine.resource import META_RESOURCE_SUSPENDS
from tasklite.utils.jsonutil import dumps
from tasklite.testing import running
from tests.helpers import make_pipeline


def _seed_suspends(p, deadlines):
    p.backend.set_meta(META_RESOURCE_SUSPENDS, dumps(deadlines))


class TestListSuspendsView:
    """挂起条目的读取、换算与排序。"""

    def test_empty_meta_returns_empty(self, tmp_path):
        p = make_pipeline(tmp_path)
        assert p.list_suspends() == []

    def test_active_suspension_reported_with_remaining(self, tmp_path):
        p = make_pipeline(tmp_path)
        deadline = time.time() + 3600.0
        _seed_suspends(p, {"ieee_api": deadline})
        entries = p.list_suspends()
        assert len(entries) == 1
        entry = entries[0]
        assert entry.resource == "ieee_api"
        assert entry.resume_at == pytest.approx(deadline)
        assert entry.remaining_seconds == pytest.approx(3600.0, abs=5.0)

    def test_expired_suspension_filtered_out(self, tmp_path):
        p = make_pipeline(tmp_path)
        _seed_suspends(p, {"ieee_api": time.time() - 1.0})
        assert p.list_suspends() == []

    def test_entries_sorted_by_resume_at_ascending(self, tmp_path):
        p = make_pipeline(tmp_path)
        now = time.time()
        _seed_suspends(p, {
            "slow_host": now + 7200.0,
            "ieee_api": now + 3600.0,
        })
        assert [e.resource for e in p.list_suspends()] == ["ieee_api", "slow_host"]

    def test_run_outer_guard_rejects_during_run(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="list_suspends"):
                p.list_suspends()


class TestListSuspendsDegrade:
    """坏数据降级：告警 + 跳过，绝不抛穿运维查询。"""

    def test_corrupted_json_returns_empty(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.backend.set_meta(META_RESOURCE_SUSPENDS, "{not-json")
        assert p.list_suspends() == []

    def test_non_dict_payload_returns_empty(self, tmp_path):
        p = make_pipeline(tmp_path)
        _seed_suspends(p, ["ieee_api", 123])
        assert p.list_suspends() == []

    def test_invalid_entries_skipped_valid_kept(self, tmp_path):
        p = make_pipeline(tmp_path)
        # 坏值须为合法 JSON（bool/str）——NaN/Infinity 字面量在 loads 层
        # 即被拒绝，走整体降级路径（test_corrupted_json_returns_empty）。
        p.backend.set_meta(META_RESOURCE_SUSPENDS,
                           '{"ieee_api": 9999999999.0, "bad_bool": true, "bad_str": "soon"}')
        deadline = 9999999999.0
        entries = p.list_suspends()
        assert [e.resource for e in entries] == ["ieee_api"]
        assert entries[0].resume_at == deadline


class TestListSuspendsWithSwapBackend:
    """换库后挂起视图读新库（与 list_dlq 等管理 API 同契约）。"""

    def test_reads_new_backend_after_swap(self, tmp_path):
        from tasklite.backend.memory import InMemoryStateBackend

        p = make_pipeline(tmp_path)
        old_deadline = time.time() + 3600.0
        _seed_suspends(p, {"old_api": old_deadline})
        new_backend = InMemoryStateBackend()
        p.backend = new_backend
        new_deadline = time.time() + 1800.0
        new_backend.set_meta(META_RESOURCE_SUSPENDS, dumps({"new_api": new_deadline}))
        entries = p.list_suspends()
        assert [e.resource for e in entries] == ["new_api"]
