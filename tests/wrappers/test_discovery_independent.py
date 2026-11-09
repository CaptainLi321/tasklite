"""Discovery 扫描核心的框架无关协议边界测试。"""
from __future__ import annotations

import json

import pytest

from tasklite.models.result import TaskResult
from tasklite.wrappers.discovery import (
    DiscoveryHandler,
    DiscoveryHost,
    encode_content_id,
    register_discovery,
)

SEEN_PREFIX = "__tasklite_discovery_seen_v1__:"


class FakeJob:
    def __init__(self, payload):
        self.payload = payload


class FakeCtx:
    """只提供公开游标读写的最小上下文。"""

    def __init__(self, cursors=None):
        self.cursors = dict(cursors or {})
        self.cursor_updates: dict[str, str | None] = {}

    def get_cursor(self, key):
        return self.cursors.get(key)

    def set_cursor(self, key, value):
        self.cursor_updates[key] = value
        if value is None:
            self.cursors.pop(key, None)
        else:
            self.cursors[key] = value


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


processed: list[str] = []
missing_reported: list[str] = []


def _seen_key(*, task_type="discovery", process_type="child"):
    scope = (
        f"{len(task_type)}:{task_type}"
        f"{len(process_type)}:{process_type}"
        "0:"
    )
    return SEEN_PREFIX + scope


def _seen_key_with_group(*, task_type, process_type, group_key):
    prefix = f"{len(group_key)}:{group_key}"
    scope = (
        f"{len(task_type)}:{task_type}"
        f"{len(process_type)}:{process_type}"
        f"{len(prefix)}:{prefix}"
    )
    return SEEN_PREFIX + scope


def _seen_value(seen):
    return json.dumps(
        {"version": 1, "seen": sorted(seen)},
        ensure_ascii=True,
        separators=(",", ":"),
    )


def _ctx(seen=()):
    encoded = {encode_content_id(item) for item in seen}
    return FakeCtx({_seen_key(): _seen_value(encoded)})


def _make_handler(**kwargs):
    return DiscoveryHandler(
        _fetch,
        _item_id,
        _process,
        "child",
        **kwargs,
    )


class TestDiscoveryHandlerFrameworkIndependent:
    def test_scan_runs_against_fake_job_and_ctx(self):
        processed.clear()
        ctx = _ctx({"old-b"})
        handler = _make_handler()

        assert handler(FakeJob({"artist_id": "a1"}), ctx) == TaskResult.success()
        assert processed == ["new-a", "old-c"]
        stored = json.loads(ctx.cursor_updates[_seen_key()])["seen"]
        assert set(stored) == {
            encode_content_id(item)
            for item in ("new-a", "old-b", "old-c")
        }

    def test_full_mode_missing_uses_only_own_seen_cursor(self):
        processed.clear()
        missing_reported.clear()
        ctx = _ctx({"vanished", "old-b"})
        handler = _make_handler(scan_mode="full", on_missing=_missing_cb)

        assert handler(FakeJob({}), ctx) == TaskResult.success()
        assert sorted(missing_reported) == ["vanished"]

    def test_incremental_full_page_hit_stops(self):
        processed.clear()
        ctx = _ctx({"new-a", "old-b", "old-c"})
        handler = _make_handler()

        assert handler(FakeJob({}), ctx) == TaskResult.success()
        assert processed == []
        assert ctx.cursor_updates == {}

    def test_seen_cursor_scope_is_injective(self):
        keys = {
            _seen_key(task_type="a", process_type="bc"),
            _seen_key(task_type="ab", process_type="c"),
            _seen_key_with_group(
                task_type="a",
                process_type="bc",
                group_key="x",
            ),
            _seen_key_with_group(
                task_type="a",
                process_type="bc",
                group_key="2:x",
            ),
        }
        assert len(keys) == 4


class FakeHost:
    def __init__(self):
        self.registered = []

    def register_task(self, task_type, handler, *, resources=None, trigger="once"):
        self.registered.append((task_type, handler, resources, trigger))


class TestRegisterDiscoveryFrameworkIndependent:
    def test_register_discovery_only_needs_discovery_host_protocol(self):
        host = FakeHost()
        register_discovery(
            host,
            "discover",
            _fetch,
            _item_id,
            _process,
            "child",
            trigger="always",
        )
        task_type, handler, _, trigger = host.registered[0]
        assert task_type == "discover"
        assert isinstance(handler, DiscoveryHandler)
        assert trigger == "always"

    def test_register_discovery_rejects_bad_host_fail_loud(self):
        class BrokenHost:
            pass

        with pytest.raises(AttributeError):
            register_discovery(
                BrokenHost(),
                "discover",
                _fetch,
                _item_id,
                _process,
                "child",
            )
