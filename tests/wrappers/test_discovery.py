"""Discovery 扫描协议、增量/全量分页去重、缺失检测与端到端派发回归测试。"""
from __future__ import annotations

import json
import re
from typing import Any

import pytest

from tasklite import Job, StateSnapshot, TaskLite, TaskLiteError, TaskResult
from tasklite.testing import cursor_updates, fake_ctx, in_process_workers, spawned_jobs
from tasklite.wrappers.discovery import (
    DiscoveryHandler,
    encode_content_id,
    register_discovery,
)

PAGESIZE = 3
SEEN_PREFIX = "__tasklite_discovery_seen_v1__:"


def _seen_orig(seen: set[str]) -> set[str]:
    out = set()
    for content_id in seen:
        base = re.sub(r"%_[0-9a-f]{16}$", "", content_id)
        out.add(
            re.sub(
                r"%([0-9A-Fa-f]{2})",
                lambda match: chr(int(match.group(1), 16)),
                base,
            )
        )
    return out


def _group_prefix(group_key: str | None) -> str:
    return "" if group_key is None else f"{len(group_key)}:{group_key}"


def _seen_cursor_key(
    *,
    task_type: str = "discover",
    process_type: str = "child",
    group_key: str | None = None,
) -> str:
    prefix = _group_prefix(group_key)
    scope = (
        f"{len(task_type)}:{task_type}"
        f"{len(process_type)}:{process_type}"
        f"{len(prefix)}:{prefix}"
    )
    return SEEN_PREFIX + scope


def _seen_cursor_value(seen: set[str] | list[str]) -> str:
    return json.dumps(
        {"version": 1, "seen": sorted(seen)},
        ensure_ascii=True,
        separators=(",", ":"),
    )


def _make_ctx(
    job: Job | None = None,
    *,
    seen: set[str] | None = None,
    task_type: str = "discover",
    process_type: str = "child",
    group_key: str | None = None,
    cursors: dict[str, str] | None = None,
):
    if job is None:
        job = Job(task_type, "seed", payload={})
    if cursors is None:
        cursors = {}
        if seen is not None:
            key = _seen_cursor_key(
                task_type=task_type,
                process_type=process_type,
                group_key=group_key,
            )
            cursors[key] = _seen_cursor_value(seen)
    return fake_ctx(job, cursors=cursors)


def _advance_ctx(job: Job, previous):
    committed = {
        key: value
        for key, value in cursor_updates(previous).items()
        if isinstance(value, str)
    }
    return _make_ctx(job, cursors=committed)


def _stored_seen(
    ctx,
    *,
    task_type: str = "discover",
    process_type: str = "child",
    group_key: str | None = None,
) -> set[str]:
    key = _seen_cursor_key(
        task_type=task_type,
        process_type=process_type,
        group_key=group_key,
    )
    return set(json.loads(cursor_updates(ctx)[key])["seen"])


def _posts(n: int) -> list[dict[str, int]]:
    return sorted([{"id": i} for i in range(n)], key=lambda item: item["id"], reverse=True)


def _make_handler(
    *,
    source: list[dict[str, Any]],
    processed: list[str] | None = None,
    pagesize: int = PAGESIZE,
    fetch_pages: list[int] | None = None,
    id_func=None,
    process_func=None,
    task_type: str = "discover",
    process_type: str = "child",
    **kwargs: Any,
) -> DiscoveryHandler:
    def _fetch(job, ctx, page):
        if fetch_pages is not None:
            fetch_pages.append(page)
        start = (page - 1) * pagesize
        return list(source[start:start + pagesize])

    def _id(item):
        return str(item["id"])

    def _process(job, ctx, item, content_id):
        if processed is not None:
            processed.append(content_id)
        if hasattr(ctx, "spawn"):
            ctx.spawn(Job(process_type, content_id, payload={"item": item}))

    return DiscoveryHandler(
        _fetch,
        id_func or _id,
        process_func or _process,
        process_type,
        discovery_task_type=task_type,
        **kwargs,
    )


# ══════════════════════════════════════════════════════════════════════
# 1. 框架无关协议边界测试
# ══════════════════════════════════════════════════════════════════════


class _FakeJob:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload


class _FakeCtx:
    """只提供公开游标读写的最小上下文桩。"""

    def __init__(self, cursors: dict[str, str] | None = None) -> None:
        self.cursors = dict(cursors or {})
        self.cursor_updates: dict[str, str | None] = {}

    def get_cursor(self, key: str) -> str | None:
        return self.cursors.get(key)

    def set_cursor(self, key: str, value: str | None) -> None:
        self.cursor_updates[key] = value
        if value is None:
            self.cursors.pop(key, None)
        else:
            self.cursors[key] = value


class _FakeHost:
    def __init__(self) -> None:
        self.registered: list[tuple[Any, ...]] = []

    def register_task(self, task_type, handler, *, resources=None, trigger="always"):
        self.registered.append((task_type, handler, resources, trigger))


class TestDiscoveryProtocol:
    def test_scan_runs_against_minimal_protocol_stubs(self):
        processed: list[str] = []

        def fetch(job, ctx, page):
            if page == 1:
                return ["new-a", "old-b"]
            if page == 2:
                return ["old-c"]
            return []

        handler = DiscoveryHandler(
            fetch,
            lambda item: item,
            lambda job, ctx, item, cid: processed.append(cid),
            "child",
        )
        key = _seen_cursor_key(task_type="discovery", process_type="child")
        ctx = _FakeCtx({key: _seen_cursor_value({encode_content_id("old-b")})})

        assert handler(_FakeJob({"artist_id": "alpha"}), ctx) == TaskResult.success()
        assert processed == ["new-a", "old-c"]
        stored = json.loads(ctx.cursor_updates[key])["seen"]
        assert set(stored) == {encode_content_id(x) for x in ("new-a", "old-b", "old-c")}

    def test_incremental_full_page_hit_stops_with_protocol_stub(self):
        processed: list[str] = []

        def fetch(job, ctx, page):
            return ["old-a", "old-b"] if page == 1 else []

        handler = DiscoveryHandler(
            fetch,
            lambda item: item,
            lambda job, ctx, item, cid: processed.append(cid),
            "child",
        )
        key = _seen_cursor_key(task_type="discovery", process_type="child")
        ctx = _FakeCtx({key: _seen_cursor_value({encode_content_id("old-a"), encode_content_id("old-b")})})

        assert handler(_FakeJob({}), ctx) == TaskResult.success()
        assert processed == []
        assert ctx.cursor_updates == {}

    def test_register_discovery_protocol_and_fail_loud(self):
        host = _FakeHost()
        register_discovery(
            host,
            "discover",
            _pipeline_fetch,
            _pipeline_id,
            _pipeline_process,
            "child",
            trigger="always",
        )
        task_type, handler, _, trigger = host.registered[0]
        assert task_type == "discover"
        assert isinstance(handler, DiscoveryHandler)
        assert trigger == "always"

        class BrokenHost:
            pass

        with pytest.raises(AttributeError):
            register_discovery(
                BrokenHost(),  # type: ignore[arg-type]
                "discover",
                _pipeline_fetch,
                _pipeline_id,
                _pipeline_process,
                "child",
            )


# ══════════════════════════════════════════════════════════════════════
# 2. 终止条件、重发现、源端删除与崩溃重跑
# ══════════════════════════════════════════════════════════════════════


class TestTerminationAndRediscovery:
    def test_first_scan_full_pagination_until_empty(self):
        processed: list[str] = []
        source = _posts(7)
        handler = _make_handler(source=source, processed=processed)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)

        assert handler(job, ctx) == TaskResult.success()
        assert _seen_orig(set(processed)) == {str(i) for i in range(7)}
        assert _stored_seen(ctx) == set(processed)
        assert len(spawned_jobs(ctx)) == 7

    def test_partial_page_continues_and_full_page_hit_stops(self):
        processed: list[str] = []
        fetch_pages: list[int] = []
        source = _posts(10)
        handler = _make_handler(source=source, processed=processed, fetch_pages=fetch_pages)
        job = Job("discover", "seed")

        seen = {encode_content_id("9"), encode_content_id("8")}
        ctx = _make_ctx(job, seen=seen)
        assert handler(job, ctx) == TaskResult.success()
        assert _seen_orig(set(processed)) == {str(i) for i in range(8)}
        assert _stored_seen(ctx) == seen | set(processed)

        # 整页命中第一页时立即停止，不继续翻第二页
        processed.clear()
        fetch_pages.clear()
        ctx_full = _make_ctx(job, seen={encode_content_id(str(i)) for i in range(10)})
        assert handler(job, ctx_full) == TaskResult.success()
        assert processed == []
        assert fetch_pages == [1]
        assert cursor_updates(ctx_full) == {}

    def test_rediscovery_and_source_deletions(self):
        source = _posts(9)
        processed: list[str] = []
        handler = _make_handler(source=source, processed=processed)
        job = Job("discover", "seed")

        ctx = _make_ctx(job)
        assert handler(job, ctx) == TaskResult.success()
        assert len(processed) == 9

        # 删除中间 (3,4,5)、最旧 (0,1) 与新增 (12, 10)
        source[:] = sorted(
            [item for item in source if item["id"] not in {0, 1, 3, 4, 5}]
            + [{"id": 12}, {"id": 10}],
            key=lambda item: item["id"],
            reverse=True,
        )
        processed.clear()
        ctx_next = _advance_ctx(job, ctx)
        assert handler(job, ctx_next) == TaskResult.success()
        assert _seen_orig(set(processed)) == {"10", "12"}

        # 删除最新 (12) 后重扫无新增也无漏扫
        source[:] = [item for item in source if item["id"] != 12]
        processed.clear()
        ctx_after_newest_del = _advance_ctx(job, ctx_next)
        assert handler(job, ctx_after_newest_del) == TaskResult.success()
        assert processed == []

    def test_mid_scan_crash_does_not_publish_cursor_and_rerun_dedups(self):
        source = _posts(6)
        calls = {"n": 0}
        processed: list[str] = []

        def flaky_fetch(job, ctx, page):
            calls["n"] += 1
            if calls["n"] == 3:
                raise RuntimeError("simulated crash mid-scan")
            start = (page - 1) * PAGESIZE
            return list(source[start:start + PAGESIZE])

        handler = DiscoveryHandler(
            flaky_fetch,
            lambda item: str(item["id"]),
            lambda job, ctx, item, cid: processed.append(cid),
            "child",
            discovery_task_type="discover",
        )
        job = Job("discover", "seed")
        first_ctx = _make_ctx(job)
        with pytest.raises(RuntimeError, match="simulated crash"):
            handler(job, first_ctx)
        assert cursor_updates(first_ctx) == {}

        processed.clear()
        retry_ctx = _make_ctx(job)
        assert handler(job, retry_ctx) == TaskResult.success()
        assert _seen_orig(set(processed)) == {str(i) for i in range(6)}
        assert len(_stored_seen(retry_ctx)) == 6


# ══════════════════════════════════════════════════════════════════════
# 3. 游标隔离、Full 模式、on_missing 与截断契约
# ══════════════════════════════════════════════════════════════════════


class TestFullModeAndMissingDetection:
    def test_full_scans_to_empty_page_and_rerun_skips_respawn(self):
        source = _posts(5)
        fetch_pages: list[int] = []
        handler = _make_handler(
            source=source,
            pagesize=2,
            fetch_pages=fetch_pages,
            scan_mode="full",
        )
        job = Job("discover", "seed")

        ctx = _make_ctx(job)
        assert handler(job, ctx) == TaskResult.success()
        assert fetch_pages == [1, 2, 3, 4]
        assert len(spawned_jobs(ctx)) == 5

        fetch_pages.clear()
        ctx_rerun = _advance_ctx(job, ctx)
        assert handler(job, ctx_rerun) == TaskResult.success()
        assert fetch_pages == [1, 2, 3, 4]
        assert spawned_jobs(ctx_rerun) == []

    def test_missing_reports_deleted_contents_and_swallows_callback_errors(self):
        source = _posts(5)
        missing_calls: list[list[str]] = []
        handler = _make_handler(
            source=source,
            pagesize=2,
            scan_mode="full",
            on_missing=lambda job, ctx, missing: missing_calls.append(list(missing)),
        )
        job = Job("discover", "seed")
        ctx = _make_ctx(job)
        assert handler(job, ctx) == TaskResult.success()
        assert missing_calls == []

        source[:] = [{"id": 2}, {"id": 1}, {"id": 0}]
        ctx_next = _advance_ctx(job, ctx)
        assert handler(job, ctx_next) == TaskResult.success()
        assert missing_calls == [["3", "4"]]

        # on_missing 抛异常必须吞掉，不得阻断 discovery 成功
        def boom_missing(job, ctx, missing):
            raise RuntimeError("on_missing bug")

        source[:] = [{"id": 0}]
        handler_boom = _make_handler(
            source=source,
            pagesize=2,
            scan_mode="full",
            on_missing=boom_missing,
        )
        ctx_boom = _advance_ctx(job, ctx_next)
        assert handler_boom(job, ctx_boom) == TaskResult.success()

    def test_payload_override_scan_mode_and_max_pages(self):
        source = [{"id": str(i)} for i in range(50, 30, -1)]
        fetch_pages: list[int] = []
        missing_calls: list[list[str]] = []
        handler = _make_handler(
            source=source,
            pagesize=2,
            fetch_pages=fetch_pages,
            scan_mode="incremental",
            max_pages=1000,
            on_missing=lambda j, c, missing: missing_calls.append(list(missing)),
        )
        seeded = {encode_content_id("50"), encode_content_id("49")}
        job = Job("discover", "seed", payload={"scan_mode": "full", "max_pages": 2})
        ctx = _make_ctx(job, seen=seeded)
        assert handler(job, ctx) == TaskResult.success()
        assert fetch_pages == [1, 2]
        assert [j.job_id for j in spawned_jobs(ctx)] == ["48", "47"]
        # max_pages 截断不得触发 on_missing
        assert missing_calls == []

        # 无 max_pages 截断时 payload scan_mode=full 完整扫描只报真正缺失的条目
        missing_calls.clear()
        source[:] = [{"id": "50"}, {"id": "49"}, {"id": "48"}]
        job_full = Job("discover", "seed2", payload={"scan_mode": "full"})
        ctx_full = _advance_ctx(job_full, ctx)
        assert handler(job_full, ctx_full) == TaskResult.success()
        assert missing_calls == [["47"]]

    def test_incremental_mode_never_triggers_on_missing(self):
        source = _posts(3)
        missing_calls: list[list[str]] = []
        handler = _make_handler(
            source=source,
            scan_mode="incremental",
            on_missing=lambda j, c, missing: missing_calls.append(list(missing)),
        )
        job = Job("discover", "seed")
        ctx = _make_ctx(job, seen={encode_content_id("999")})
        assert handler(job, ctx) == TaskResult.success()
        assert missing_calls == []

    @pytest.mark.parametrize("mode", ["all_fail", "partial_raise", "partial_invalid"])
    def test_incomplete_id_func_scan_suppresses_on_missing(self, mode):
        source = _posts(6)
        missing_calls: list[list[str]] = []

        def custom_id(item):
            if mode == "all_fail":
                raise KeyError("boom")
            if item["id"] == 5:
                if mode == "partial_raise":
                    raise KeyError("broken item")
                return 42
            return str(item["id"])

        handler = _make_handler(
            source=source,
            pagesize=2,
            id_func=custom_id,
            scan_mode="full",
            on_missing=lambda j, c, missing: missing_calls.append(list(missing)),
        )
        job = Job("discover", "seed")
        ctx = _make_ctx(job, seen={encode_content_id(str(i)) for i in range(6)})
        assert handler(job, ctx) == TaskResult.success()
        assert missing_calls == []

    def test_on_missing_deduplicates_seen_entries(self):
        missing_calls: list[list[str]] = []
        handler = _make_handler(
            source=[],
            scan_mode="full",
            on_missing=lambda j, c, missing: missing_calls.append(list(missing)),
        )
        job = Job("discover", "empty")
        raw = json.dumps(
            {
                "version": 1,
                "seen": [
                    encode_content_id("1"),
                    encode_content_id("1"),
                    encode_content_id("2"),
                    encode_content_id("2"),
                ],
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )
        ctx = _make_ctx(job, cursors={_seen_cursor_key(): raw})
        assert handler(job, ctx) == TaskResult.success()
        assert missing_calls == [["1", "2"]]


class TestGroupIsolationAndEncoding:
    def test_group_key_isolates_cursors_content_ids_and_missing(self):
        source = [{"id": i} for i in range(4)]
        missing_calls: list[list[str]] = []
        handler = _make_handler(
            source=source,
            pagesize=2,
            group_key_func=lambda payload: f"g_{payload['group']}",
            scan_mode="full",
            on_missing=lambda j, c, missing: missing_calls.append(list(missing)),
        )
        job_a = Job("discover", "seed_a", payload={"group": "A"})
        job_b = Job("discover", "seed_b", payload={"group": "B"})
        ctx_a = _make_ctx(job_a)
        ctx_b = _make_ctx(job_b)

        assert handler(job_a, ctx_a) == TaskResult.success()
        source[:] = [{"id": 9}, {"id": 8}]
        assert handler(job_b, ctx_b) == TaskResult.success()
        assert missing_calls == []
        assert _stored_seen(ctx_a, group_key="g_A") != _stored_seen(ctx_b, group_key="g_B")

        # 组 A 仅剩 3, 2 → missing 仅报 A 的 0, 1，不混入 B 的 8, 9
        source[:] = [{"id": 3}, {"id": 2}]
        ctx_a_next = _advance_ctx(job_a, ctx_a)
        assert handler(job_a, ctx_a_next) == TaskResult.success()
        assert missing_calls == [["3%3Ag_A0", "3%3Ag_A1"]]

    def test_long_group_key_truncated_domain_preserves_missing_isolation(self):
        long_a = "a" * 200
        long_b = "b" * 200
        prefix_a = f"{len('g_' + long_a)}:g_{long_a}"
        prefix_b = f"{len('g_' + long_b)}:g_{long_b}"

        source = [{"id": i} for i in range(4)]
        missing_calls: list[list[str]] = []
        handler = _make_handler(
            source=source,
            pagesize=2,
            group_key_func=lambda payload: f"g_{payload['group']}",
            scan_mode="full",
            on_missing=lambda j, c, missing: missing_calls.append(list(missing)),
        )
        job_a = Job("discover", "seed_a", payload={"group": long_a})
        job_b = Job("discover", "seed_b", payload={"group": long_b})
        ctx_a = _make_ctx(job_a)
        ctx_b = _make_ctx(job_b)

        assert handler(job_a, ctx_a) == TaskResult.success()
        source[:] = [{"id": 9}, {"id": 8}]
        assert handler(job_b, ctx_b) == TaskResult.success()

        source[:] = [{"id": 1}, {"id": 0}]
        ctx_a_next = _advance_ctx(job_a, ctx_a)
        assert handler(job_a, ctx_a_next) == TaskResult.success()
        reported = set(missing_calls[0])
        assert reported == {encode_content_id(prefix_a + cid) for cid in ("2", "3")}
        assert not (reported & {encode_content_id(prefix_b + cid) for cid in ("8", "9")})

    def test_seen_cursor_scope_and_content_id_encoding_injective(self):
        keys = {
            _seen_cursor_key(task_type="a", process_type="bc"),
            _seen_cursor_key(task_type="ab", process_type="c"),
            _seen_cursor_key(task_type="a", process_type="bc", group_key="x"),
            _seen_cursor_key(task_type="a", process_type="bc", group_key="2:x"),
        }
        assert len(keys) == 4

        assert encode_content_id("2:ab" + "c_d") != encode_content_id("4:ab_c" + "d")
        assert encode_content_id("2:ab" + "x") != encode_content_id("2:cd" + "x")
        assert encode_content_id("ӢD") != encode_content_id("中")
        assert encode_content_id("中") == "%E4%B8%AD"
        assert encode_content_id("é") == "%C3%A9"
        assert encode_content_id("%E4%B8%AD") != encode_content_id("中")


# ══════════════════════════════════════════════════════════════════════
# 4. 参数校验与坏项防御
# ══════════════════════════════════════════════════════════════════════


class TestDefenseAndValidation:
    def test_registration_parameter_validation(self):
        host = _FakeHost()
        with pytest.raises(TaskLiteError, match="process_task_type"):
            register_discovery(host, "d", _pipeline_fetch, _pipeline_id, _pipeline_process, "")
        with pytest.raises(TaskLiteError, match="::"):
            register_discovery(host, "d", _pipeline_fetch, _pipeline_id, _pipeline_process, "bad::type")
        with pytest.raises(TaskLiteError, match="max_pages"):
            register_discovery(host, "d", _pipeline_fetch, _pipeline_id, _pipeline_process, "child", max_pages=0)
        with pytest.raises(TaskLiteError, match="scan_mode"):
            register_discovery(host, "d", _pipeline_fetch, _pipeline_id, _pipeline_process, "child", scan_mode="bogus")
        with pytest.raises(TaskLiteError, match="picklable"):
            register_discovery(
                host,
                "d",
                lambda j, c, p: [],
                _pipeline_id,
                _pipeline_process,
                "child",
            )

    def test_runtime_input_and_payload_validation(self):
        handler_bad_key = _make_handler(source=[{"id": 1}], group_key_func=lambda payload: "")
        with pytest.raises(TaskLiteError, match="group_key_func"):
            handler_bad_key(Job("discover", "s"), _make_ctx())

        handler_bad_fetch = DiscoveryHandler(
            lambda j, c, p: "not-a-list",  # type: ignore[arg-type, return-value]
            lambda i: "1",
            lambda j, c, i, cid: None,
            "child",
            discovery_task_type="discover",
        )
        with pytest.raises(TaskLiteError, match="fetch_func"):
            handler_bad_fetch(Job("discover", "s"), _make_ctx())

        handler = _make_handler(source=[{"id": 1}])
        job_bad_mode = Job("discover", "j1", payload={"scan_mode": "invalid_mode"})
        with pytest.raises(TaskLiteError, match="scan_mode"):
            handler(job_bad_mode, _make_ctx(job_bad_mode))

        for bad_max in (0, -1, True, "10"):
            job_bad_max = Job("discover", "j2", payload={"max_pages": bad_max})
            with pytest.raises(TaskLiteError, match="max_pages"):
                handler(job_bad_max, _make_ctx(job_bad_max))

    @pytest.mark.parametrize(
        "raw",
        [
            "not-json",
            '{"version":2,"seen":[]}',
            '{"version":true,"seen":[]}',
            '{"version":1,"seen":[1]}',
            '{"version":NaN,"seen":[]}',
            '{"version":1,"seen":[],"extra":1e400}',
        ],
    )
    def test_invalid_seen_cursor_rejected(self, raw):
        handler = _make_handler(source=[{"id": 1}])
        job = Job("discover", "seed")
        ctx = _make_ctx(job, cursors={_seen_cursor_key(): raw})
        with pytest.raises(ValueError, match="seen cursor"):
            handler(job, ctx)

    def test_item_level_failures_isolated_and_retried_next_run(self):
        processed: list[str] = []
        attempts = {"n": 0}

        def poison_process(job, ctx, item, content_id):
            if _seen_orig({content_id}) == {"2"}:
                attempts["n"] += 1
                raise RuntimeError("poison item boom")
            processed.append(content_id)

        handler = _make_handler(source=_posts(3), process_func=poison_process)
        job = Job("discover", "seed")
        ctx = _make_ctx(job)
        assert handler(job, ctx) == TaskResult.success()
        assert _seen_orig(set(processed)) == {"0", "1"}
        assert attempts["n"] == 1

        processed.clear()
        ctx_next = _advance_ctx(job, ctx)
        assert handler(job, ctx_next) == TaskResult.success()
        assert processed == []
        assert attempts["n"] == 2

        # id_func 返回空串或抛异常时跳过该项，不写入空游标
        handler_bad_id = _make_handler(
            source=[{"id": 1}],
            id_func=lambda item: "",
            processed=processed,
        )
        ctx_empty = _make_ctx(job)
        assert handler_bad_id(job, ctx_empty) == TaskResult.success()
        assert cursor_updates(ctx_empty) == {}


# ══════════════════════════════════════════════════════════════════════
# 5. TaskLite 核心联动端到端验证
# ══════════════════════════════════════════════════════════════════════


def _pipeline_fetch(job, ctx, page):
    posts = job.payload.get("posts", [])
    start = (page - 1) * 2
    return posts[start:start + 2]


def _pipeline_id(item):
    return str(item["id"])


def _pipeline_process(job, ctx, item, content_id):
    ctx.spawn(Job("rt_child", content_id, payload={"item": item}))


def _pipeline_bad_process(job, ctx, item, content_id):
    if item["id"] == 1:
        ctx.spawn(Job("unregistered_child", content_id))
    else:
        ctx.spawn(Job("rt_child", content_id, payload={"item": item}))


def _pipeline_group_key(payload):
    return f"rt_{payload['artist_id']}"


def _pipeline_child(job, ctx):
    return TaskResult.success()


def test_pipeline_rediscovery_end_to_end():
    with TaskLite(backend="memory", max_workers=2) as pipeline:
        register_discovery(
            pipeline,
            "rt_disc",
            _pipeline_fetch,
            _pipeline_id,
            _pipeline_process,
            "rt_child",
            group_key_func=_pipeline_group_key,
        )
        pipeline.register_task("rt_child", _pipeline_child)
        with in_process_workers(pipeline):
            pipeline.enqueue(
                Job("rt_disc", "seed", payload={"artist_id": "2", "posts": _posts(4)})
            )
            pipeline.run(install_signals=False)

            new_posts = [{"id": 11}, {"id": 10}] + _posts(4)
            pipeline.enqueue(
                Job("rt_disc", "seed2", payload={"artist_id": "2", "posts": new_posts})
            )
            pipeline.run(install_signals=False)

        with StateSnapshot.open(pipeline) as snap:
            children = [
                job["job_uid"]
                for job in snap.iter_jobs(task_type="rt_child", status="succeeded")
            ]
            assert len(children) == 6
            assert any(u.startswith("rt_child::4%253Art_210") for u in children)
            assert any(u.startswith("rt_child::4%253Art_211") for u in children)
        seen_cursor = pipeline.get_cursor(
            _seen_cursor_key(
                task_type="rt_disc",
                process_type="rt_child",
                group_key="rt_2",
            )
        )
        assert seen_cursor is not None
        assert len(json.loads(seen_cursor)["seen"]) == 6


def test_bad_child_fails_parent_without_partial_publish():
    with TaskLite(backend="memory", max_workers=2) as pipeline:
        register_discovery(
            pipeline,
            "rt_disc_bad",
            _pipeline_fetch,
            _pipeline_id,
            _pipeline_bad_process,
            "rt_child",
        )
        pipeline.register_task("rt_child", _pipeline_child)
        with in_process_workers(pipeline):
            pipeline.enqueue(
                Job("rt_disc_bad", "seed_bad", payload={"posts": [{"id": 1}, {"id": 0}]})
            )
            pipeline.run(install_signals=False)

        with StateSnapshot.open(pipeline) as snap:
            parent = snap.get_job("rt_disc_bad::seed_bad")
            assert parent is not None
            assert parent["status"] == "failed"
            assert "Invalid spawned job" in parent["result"]["message"]
            assert list(snap.iter_jobs(task_type="rt_child")) == []
        assert (
            pipeline.get_cursor(_seen_cursor_key(task_type="rt_disc_bad", process_type="rt_child"))
            is None
        )
