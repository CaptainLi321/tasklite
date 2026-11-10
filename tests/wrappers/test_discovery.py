"""Discovery 公开游标、分页去重与真实派生回归。"""
from __future__ import annotations

import json
import re
import sys
import tempfile
from pathlib import Path

import pytest

from tasklite import TaskLiteError

from tasklite import Job, TaskLite, TaskResult
from tasklite.testing import fake_ctx
from tasklite.wrappers.discovery import (
    encode_content_id,
    register_discovery,
)

PROJECT_ROOT = str(Path(__file__).resolve().parents[3])
PAGESIZE = 3
PTYPE = "download"
SEEN_PREFIX = "__tasklite_discovery_seen_v1__:"

_posts: list[dict] = []
_seen_by_handler: set[str] = set()
_processed_guard: list[str] = []
_poison_attempts = {"n": 0}
_crash_calls = {"n": 0}


def _reset_source(items: list[dict]) -> None:
    global _posts, _seen_by_handler
    _posts = sorted(items, key=lambda item: item["id"], reverse=True)
    _seen_by_handler = set()


def _add_posts(items: list[dict]) -> None:
    global _posts
    _posts = sorted(_posts + items, key=lambda item: item["id"], reverse=True)


def _delete_posts(ids: list[int]) -> None:
    global _posts
    deleted = set(ids)
    _posts = [item for item in _posts if item["id"] not in deleted]


def _page_slice(page: int) -> list[dict]:
    start = (page - 1) * PAGESIZE
    return _posts[start:start + PAGESIZE]


def fetch_func(job, ctx, page):
    return list(_page_slice(page))


def id_func(item):
    return str(item["id"])


def group_key_func(payload):
    return f"cursor_{payload['artist_id']}"


def _seen_orig(seen):
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


def process_item_func(job, ctx, item, content_id):
    _seen_by_handler.add(content_id)


def bad_key(payload):
    return ""


def bad_fetch(job, ctx, page):
    return "not-a-list"


def bad_id(item):
    return "" if item.get("good") else "x"


def record_processed(job, ctx, item, content_id):
    _processed_guard.append(content_id)


def raising_id(item):
    raise KeyError("id") from None


def crash_once_fetch(job, ctx, page):
    _crash_calls["n"] += 1
    if _crash_calls["n"] == 3:
        raise RuntimeError("simulated crash mid-scan")
    return list(_page_slice(page))


def poison_process(job, ctx, item, content_id):
    if _seen_orig({content_id}) == {"2"}:
        _poison_attempts["n"] += 1
        raise RuntimeError("poison item boom")
    _processed_guard.append(content_id)


def _group_prefix(group_key: str | None) -> str:
    return "" if group_key is None else f"{len(group_key)}:{group_key}"


def _seen_cursor_key(
    *,
    task_type: str = "discover",
    process_type: str = PTYPE,
    group_key: str | None = None,
) -> str:
    prefix = _group_prefix(group_key)
    scope = (
        f"{len(task_type)}:{task_type}"
        f"{len(process_type)}:{process_type}"
        f"{len(prefix)}:{prefix}"
    )
    return SEEN_PREFIX + scope


def _seen_cursor_value(seen) -> str:
    return json.dumps(
        {"version": 1, "seen": sorted(seen)},
        ensure_ascii=True,
        separators=(",", ":"),
    )


def _make_ctx(
    job=None,
    *,
    seen=None,
    group_key: str | None = None,
    cursors: dict[str, str] | None = None,
):
    if job is None:
        job = Job("discover", "seed", payload={})
    if cursors is None:
        cursors = {}
        if seen is not None:
            cursors[_seen_cursor_key(group_key=group_key)] = _seen_cursor_value(seen)
    return fake_ctx(job, cursors=cursors)


def _advance_ctx(job, previous):
    committed = {
        key: value
        for key, value in previous.cursor_updates.items()
        if isinstance(value, str)
    }
    return _make_ctx(job, cursors=committed)


def _make_pipeline(name="test_disc"):
    state_dir = tempfile.mkdtemp(prefix=f"tp_disc_{name}_")
    return TaskLite(backend="sqlite", state_path=Path(state_dir) / f"{name}.db")


def _register(pipeline, **kwargs):
    register_discovery(
        pipeline,
        "discover",
        fetch_func,
        id_func,
        process_item_func,
        PTYPE,
        **kwargs,
    )


def _handler(pipeline):
    return pipeline.tasks.get("discover").handler


def _stored_seen(ctx, *, task_type="discover", process_type=PTYPE, group_key=None):
    key = _seen_cursor_key(
        task_type=task_type,
        process_type=process_type,
        group_key=group_key,
    )
    return set(json.loads(ctx.cursor_updates[key])["seen"])


class TestTermination:
    def test_first_scan_full_pagination_until_empty(self):
        _reset_source([{"id": i} for i in range(7)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)

        assert _handler(pipeline)(job, ctx) == TaskResult.success()
        assert _seen_orig(_seen_by_handler) == {str(i) for i in range(7)}
        assert _stored_seen(ctx) == _seen_by_handler

    def test_partial_page_continues_scanning(self):
        _reset_source([{"id": i} for i in range(10)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        seen = {encode_content_id("9"), encode_content_id("8")}
        ctx = _make_ctx(job, seen=seen)

        assert _handler(pipeline)(job, ctx) == TaskResult.success()
        assert _seen_orig(_seen_by_handler) == {
            "7", "6", "5", "4", "3", "2", "1", "0",
        }
        assert _stored_seen(ctx) == seen | _seen_by_handler

    def test_full_page_hit_stops_scanning(self):
        _reset_source([{"id": i} for i in range(7)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        seen = {encode_content_id(str(i)) for i in range(7)}
        ctx = _make_ctx(job, seen=seen)

        assert _handler(pipeline)(job, ctx) == TaskResult.success()
        assert _seen_by_handler == set()
        assert ctx.cursor_updates == {}


class TestRediscoveryAndDeletion:
    def test_rediscovery_only_new_content(self):
        _reset_source([{"id": i} for i in range(6)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) == TaskResult.success()

        _add_posts([{"id": 10}, {"id": 11}])
        _seen_by_handler.clear()
        ctx2 = _advance_ctx(job, ctx)
        assert _handler(pipeline)(job, ctx2) == TaskResult.success()
        assert _seen_orig(_seen_by_handler) == {"10", "11"}

    def test_deletion_middle_no_rescan_no_loss(self):
        _reset_source([{"id": i} for i in range(9)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) == TaskResult.success()

        _delete_posts([3, 4, 5])
        _add_posts([{"id": 12}])
        _seen_by_handler.clear()
        ctx2 = _advance_ctx(job, ctx)
        assert _handler(pipeline)(job, ctx2) == TaskResult.success()
        assert _seen_orig(_seen_by_handler) == {"12"}

    def test_deletion_oldest_no_rescan(self):
        _reset_source([{"id": i} for i in range(6)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) == TaskResult.success()

        _delete_posts([0, 1])
        _add_posts([{"id": 20}])
        _seen_by_handler.clear()
        ctx2 = _advance_ctx(job, ctx)
        assert _handler(pipeline)(job, ctx2) == TaskResult.success()
        assert _seen_orig(_seen_by_handler) == {"20"}

    def test_deletion_newest_handled(self):
        _reset_source([{"id": i} for i in range(4)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) == TaskResult.success()

        _add_posts([{"id": 10}])
        _seen_by_handler.clear()
        ctx2 = _advance_ctx(job, ctx)
        assert _handler(pipeline)(job, ctx2) == TaskResult.success()
        assert _seen_orig(_seen_by_handler) == {"10"}

        _delete_posts([10])
        _seen_by_handler.clear()
        ctx3 = _advance_ctx(job, ctx2)
        assert _handler(pipeline)(job, ctx3) == TaskResult.success()
        assert _seen_orig(_seen_by_handler) == set()


class TestCrashRerun:
    def test_mid_scan_crash_propagates_without_cursor_publish(self):
        _reset_source([{"id": i} for i in range(7)])
        _crash_calls["n"] = 0
        pipeline = _make_pipeline()
        register_discovery(
            pipeline,
            "discover",
            crash_once_fetch,
            id_func,
            process_item_func,
            PTYPE,
        )
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)

        with pytest.raises(RuntimeError, match="simulated crash"):
            _handler(pipeline)(job, ctx)
        assert ctx.cursor_updates == {}

    def test_rerun_after_crash_rescans_and_dedups(self):
        _reset_source([{"id": i} for i in range(6)])
        _crash_calls["n"] = 0
        pipeline = _make_pipeline()
        register_discovery(
            pipeline,
            "discover",
            crash_once_fetch,
            id_func,
            process_item_func,
            PTYPE,
        )
        job = Job("discover", "seed", payload={"artist_id": "123"})
        first_ctx = _make_ctx(job)
        with pytest.raises(RuntimeError, match="simulated crash"):
            _handler(pipeline)(job, first_ctx)
        assert first_ctx.cursor_updates == {}

        _seen_by_handler.clear()
        retry_ctx = _make_ctx(job)
        assert _handler(pipeline)(job, retry_ctx) == TaskResult.success()
        assert _seen_orig(_seen_by_handler) == {str(i) for i in range(6)}
        assert len(_stored_seen(retry_ctx)) == 6


class TestInteractionMatrix:
    def test_seen_cursor_is_independent_of_child_terminal_state(self):
        _reset_source([{"id": 3}, {"id": 2}, {"id": 1}])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        seen = {encode_content_id("2")}
        ctx = _make_ctx(job, seen=seen)

        assert _handler(pipeline)(job, ctx) == TaskResult.success()
        assert _seen_orig(_seen_by_handler) == {"3", "1"}

    def test_lost_cursor_rescans_all(self):
        _reset_source([{"id": i} for i in range(6)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        assert _handler(pipeline)(job, _make_ctx(job)) == TaskResult.success()

        _seen_by_handler.clear()
        assert _handler(pipeline)(job, _make_ctx(job)) == TaskResult.success()
        assert _seen_orig(_seen_by_handler) == {str(i) for i in range(6)}

    def test_group_key_isolates_seen_cursors_and_content_ids(self):
        _reset_source([{"id": "dup"}])
        pipeline = _make_pipeline()
        register_discovery(
            pipeline,
            "discover",
            fetch_func,
            id_func,
            process_item_func,
            PTYPE,
            group_key_func=group_key_func,
        )
        job_a = Job("discover", "seed_a", payload={"artist_id": "A"})
        job_b = Job("discover", "seed_b", payload={"artist_id": "B"})
        ctx_a = _make_ctx(job_a)
        ctx_b = _make_ctx(job_b)

        assert _handler(pipeline)(job_a, ctx_a) == TaskResult.success()
        processed_a = set(_seen_by_handler)
        _seen_by_handler.clear()
        assert _handler(pipeline)(job_b, ctx_b) == TaskResult.success()
        processed_b = set(_seen_by_handler)

        assert len(processed_a) == len(processed_b) == 1
        assert next(iter(processed_a)).startswith("8%3Acursor_Adup")
        assert next(iter(processed_b)).startswith("8%3Acursor_Bdup")
        assert _stored_seen(ctx_a, group_key="cursor_A") == processed_a
        assert _stored_seen(ctx_b, group_key="cursor_B") == processed_b


class TestDefense:
    def test_cursor_key_invalid_rejected(self):
        _reset_source([{"id": 1}])
        pipeline = _make_pipeline()
        register_discovery(
            pipeline,
            "discover",
            fetch_func,
            id_func,
            process_item_func,
            PTYPE,
            group_key_func=bad_key,
        )
        job = Job("discover", "seed", payload={})
        with pytest.raises(TaskLiteError, match="group_key_func"):
            _handler(pipeline)(job, _make_ctx(job))

    @pytest.mark.parametrize(
        "raw",
        [
            "not-json",
            '{"version":2,"seen":[]}',
            '{"version":true,"seen":[]}',
            '{"version":1,"seen":[1]}',
        ],
    )
    def test_invalid_seen_cursor_rejected(self, raw):
        _reset_source([{"id": 1}])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={})
        ctx = _make_ctx(job, cursors={_seen_cursor_key(): raw})
        with pytest.raises(ValueError, match="seen cursor"):
            _handler(pipeline)(job, ctx)

    def test_fetch_non_list_rejected(self):
        _reset_source([{"id": 1}])
        pipeline = _make_pipeline()
        register_discovery(
            pipeline, "discover", bad_fetch, id_func, process_item_func, PTYPE
        )
        job = Job("discover", "seed", payload={})
        with pytest.raises(TaskLiteError, match="fetch_func"):
            _handler(pipeline)(job, _make_ctx(job))

    def test_id_func_bad_item_isolated(self):
        _reset_source([{"id": 1, "good": True}])
        _processed_guard.clear()
        pipeline = _make_pipeline()
        register_discovery(
            pipeline, "discover", fetch_func, bad_id, record_processed, PTYPE
        )
        job = Job("discover", "seed", payload={})
        ctx = _make_ctx(job)

        assert _handler(pipeline)(job, ctx) == TaskResult.success()
        assert _processed_guard == []
        assert ctx.cursor_updates == {}

        _reset_source([{"id": 2, "good": False}])
        ctx2 = _make_ctx(job)
        assert _handler(pipeline)(job, ctx2) == TaskResult.success()
        assert len(_processed_guard) == 1

    def test_id_func_raise_isolated(self):
        _reset_source([{"id": 1}])
        pipeline = _make_pipeline()
        register_discovery(
            pipeline, "discover", fetch_func, raising_id, process_item_func, PTYPE
        )
        job = Job("discover", "seed", payload={})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) == TaskResult.success()
        assert ctx.cursor_updates == {}

    def test_process_item_func_raise_is_retried_next_run(self):
        _reset_source([{"id": 3}, {"id": 2}, {"id": 1}])
        _processed_guard.clear()
        _poison_attempts["n"] = 0
        pipeline = _make_pipeline()
        register_discovery(
            pipeline, "discover", fetch_func, id_func, poison_process, PTYPE
        )
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)

        assert _handler(pipeline)(job, ctx) == TaskResult.success()
        assert _seen_orig(set(_processed_guard)) == {"3", "1"}
        assert _poison_attempts["n"] == 1

        _processed_guard.clear()
        ctx2 = _advance_ctx(job, ctx)
        assert _handler(pipeline)(job, ctx2) == TaskResult.success()
        assert _processed_guard == []
        assert _poison_attempts["n"] == 2

    def test_process_task_type_validation(self):
        _reset_source([])
        pipeline = _make_pipeline()
        with pytest.raises(TaskLiteError, match="process_task_type"):
            register_discovery(
                pipeline, "discover", fetch_func, id_func, process_item_func, ""
            )
        with pytest.raises(TaskLiteError, match="::"):
            register_discovery(
                pipeline,
                "discover",
                fetch_func,
                id_func,
                process_item_func,
                "bad::type",
            )

    def test_max_pages_validation(self):
        _reset_source([])
        pipeline = _make_pipeline()
        with pytest.raises(TaskLiteError, match="max_pages"):
            register_discovery(
                pipeline,
                "discover",
                fetch_func,
                id_func,
                process_item_func,
                PTYPE,
                max_pages=0,
            )


PAGESIZE_RT = 2


def _rt_fetch(job, ctx, page):
    posts = job.payload.get("posts", [])
    start = (page - 1) * PAGESIZE_RT
    return posts[start:start + PAGESIZE_RT]


def _rt_id(item):
    return str(item["id"])


def _rt_process(job, ctx, item, content_id):
    ctx.spawn(Job("rt_child", content_id, payload={"item": item}))


def _rt_process_with_bad_child(job, ctx, item, content_id):
    if item["id"] == 1:
        ctx.spawned_jobs.append(
            Job(
                "rt_child",
                content_id,
                depends_on=["rt_child::nonexistent_dep"],
            )
        )
    else:
        ctx.spawn(Job("rt_child", content_id, payload={"item": item}))


def _rt_cursor_key(payload):
    return f"rt_{payload['artist_id']}"


def _rt_child_handler(job, ctx):
    return TaskResult.success()


def _rt_posts_upto(n):
    return sorted(
        [{"id": i} for i in range(n)],
        key=lambda item: item["id"],
        reverse=True,
    )


def _rt_posts_with_new(old_count, new_ids):
    old = sorted(
        [{"id": i} for i in range(old_count)],
        key=lambda item: item["id"],
        reverse=True,
    )
    new = sorted(
        [{"id": i} for i in new_ids],
        key=lambda item: item["id"],
        reverse=True,
    )
    return new + old


def _rt_setup(tmp_path, monkeypatch, name="rt_disc"):
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
    monkeypatch.setenv("PYTHONPATH", PROJECT_ROOT)
    return TaskLite(
        backend="sqlite",
        state_path=tmp_path / "state" / f"{name}.db",
        max_workers=3,
    )


def _discovery_cursors(pipeline):
    return {
        key: value
        for key, value in pipeline._runtime.store.backend.load_cursors().items()
        if key.startswith(SEEN_PREFIX)
    }


def test_real_subprocess_discovery(tmp_path, monkeypatch):
    pipeline = _rt_setup(tmp_path, monkeypatch)
    register_discovery(
        pipeline,
        "rt_disc",
        _rt_fetch,
        _rt_id,
        _rt_process,
        "rt_child",
        group_key_func=_rt_cursor_key,
    )
    pipeline.register_task("rt_child", _rt_child_handler)
    pipeline.enqueue(
        Job(
            "rt_disc",
            "seed",
            payload={"artist_id": "1", "posts": _rt_posts_upto(5)},
        )
    )
    pipeline.run()

    successes = pipeline._runtime.store.backend.load_successes()
    child_uids = sorted(k for k in successes if k.startswith("rt_child::"))
    assert len(child_uids) == 5
    assert all(u.startswith("rt_child::4%253Art_1") for u in child_uids)

    cursors = _discovery_cursors(pipeline)
    assert len(cursors) == 1
    seen = json.loads(next(iter(cursors.values())))["seen"]
    assert len(seen) == 5


def test_real_subprocess_rediscovery(tmp_path, monkeypatch):
    pipeline = _rt_setup(tmp_path, monkeypatch, name="rt_disc2")
    register_discovery(
        pipeline,
        "rt_disc2",
        _rt_fetch,
        _rt_id,
        _rt_process,
        "rt_child",
        group_key_func=_rt_cursor_key,
    )
    pipeline.register_task("rt_child", _rt_child_handler)
    pipeline.enqueue(
        Job(
            "rt_disc2",
            "seed",
            payload={"artist_id": "2", "posts": _rt_posts_upto(4)},
        )
    )
    pipeline.run()

    pipeline.enqueue(
        Job(
            "rt_disc2",
            "seed2",
            payload={
                "artist_id": "2",
                "posts": _rt_posts_with_new(4, [10, 11]),
            },
        )
    )
    pipeline.run()

    successes = pipeline._runtime.store.backend.load_successes()
    child_uids = sorted(k for k in successes if k.startswith("rt_child::"))
    assert len(child_uids) == 6
    assert any(u.startswith("rt_child::4%253Art_210") for u in child_uids)
    assert any(u.startswith("rt_child::4%253Art_211") for u in child_uids)
    seen = json.loads(next(iter(_discovery_cursors(pipeline).values())))["seen"]
    assert len(seen) == 6


def test_real_subprocess_max_pages(tmp_path, monkeypatch):
    pipeline = _rt_setup(tmp_path, monkeypatch, name="rt_disc3")
    posts = sorted(
        [{"id": str(i)} for i in range(100)],
        key=lambda item: item["id"],
        reverse=True,
    )
    register_discovery(
        pipeline,
        "rt_disc3",
        _rt_fetch,
        _rt_id,
        _rt_process,
        "rt_child",
        max_pages=2,
    )
    pipeline.register_task("rt_child", _rt_child_handler)
    pipeline.enqueue(
        Job(
            "rt_disc3",
            "seed",
            payload={"artist_id": "3", "posts": posts},
        )
    )
    pipeline.run()

    successes = pipeline._runtime.store.backend.load_successes()
    child_uids = [k for k in successes if k.startswith("rt_child::")]
    assert len(child_uids) == PAGESIZE_RT * 2
    assert not pipeline._runtime.store.backend.load_failed()
    seen = json.loads(next(iter(_discovery_cursors(pipeline).values())))["seen"]
    assert len(seen) == PAGESIZE_RT * 2


def test_real_subprocess_full_mode_with_max_pages(tmp_path, monkeypatch):
    pipeline = _rt_setup(tmp_path, monkeypatch, name="rt_disc_full")
    posts = [{"id": str(i)} for i in range(100, 80, -1)]
    register_discovery(
        pipeline,
        "rt_disc_full",
        _rt_fetch,
        _rt_id,
        _rt_process,
        "rt_child",
        scan_mode="full",
        max_pages=3,
    )
    pipeline.register_task("rt_child", _rt_child_handler)

    seeded = {encode_content_id(job_id) for job_id in ("100", "99", "98", "97")}
    pipeline.set_cursor(
        _seen_cursor_key(
            task_type="rt_disc_full",
            process_type="rt_child",
        ),
        _seen_cursor_value(seeded),
    )
    pipeline.enqueue(
        Job(
            "rt_disc_full",
            "seed",
            payload={"artist_id": "full1", "posts": posts},
        )
    )
    pipeline.run()

    successes = pipeline._runtime.store.backend.load_successes()
    child_uids = [k for k in successes if k.startswith("rt_child::")]
    assert sorted(child_uids) == ["rt_child::95", "rt_child::96"]
    assert "rt_child::98" not in successes
    assert "rt_child::97" not in successes
    assert "rt_child::96" in successes
    assert "rt_child::95" in successes
    assert "rt_child::94" not in successes


def test_payload_dynamic_override_scan_mode_and_max_pages(tmp_path, monkeypatch):
    pipeline = _rt_setup(tmp_path, monkeypatch, name="rt_disc_dynamic")
    posts = [{"id": str(i)} for i in range(50, 30, -1)]
    register_discovery(
        pipeline,
        "rt_disc_dyn",
        _rt_fetch,
        _rt_id,
        _rt_process,
        "rt_child",
        scan_mode="incremental",
        max_pages=1000,
    )
    pipeline.register_task("rt_child", _rt_child_handler)

    seeded = {encode_content_id(job_id) for job_id in ("50", "49")}
    pipeline.set_cursor(
        _seen_cursor_key(
            task_type="rt_disc_dyn",
            process_type="rt_child",
        ),
        _seen_cursor_value(seeded),
    )
    pipeline.enqueue(
        Job(
            "rt_disc_dyn",
            "seed",
            payload={
                "artist_id": "dyn1",
                "posts": posts,
                "scan_mode": "full",
                "max_pages": 2,
            },
        )
    )
    pipeline.run()

    successes = pipeline._runtime.store.backend.load_successes()
    assert "rt_child::48" in successes
    assert "rt_child::47" in successes
    assert "rt_child::46" not in successes


def test_invalid_payload_scan_mode_and_max_pages_rejected():
    pipeline = _make_pipeline()
    _register(pipeline)
    handler = _handler(pipeline)

    job_bad_mode = Job("discover", "j1", payload={"scan_mode": "invalid_mode"})
    with pytest.raises(TaskLiteError, match="scan_mode"):
        handler(job_bad_mode, _make_ctx(job_bad_mode))

    for bad_max in (0, -1, True, "10"):
        job_bad_max = Job("discover", "j2", payload={"max_pages": bad_max})
        with pytest.raises(TaskLiteError, match="max_pages"):
            handler(job_bad_max, _make_ctx(job_bad_max))


def test_bad_child_fails_parent_without_partial_publish(tmp_path, monkeypatch):
    pipeline = _rt_setup(tmp_path, monkeypatch, name="rt_disc_bad_child")
    register_discovery(
        pipeline,
        "rt_disc_bad",
        _rt_fetch,
        _rt_id,
        _rt_process_with_bad_child,
        "rt_child",
    )
    pipeline.register_task("rt_child", _rt_child_handler)
    pipeline.enqueue(
        Job(
            "rt_disc_bad",
            "seed_bad",
            payload={"artist_id": "bad1", "posts": [{"id": 1}, {"id": 0}]},
        )
    )
    pipeline.run()

    successes = pipeline._runtime.store.backend.load_successes()
    assert "rt_disc_bad::seed_bad" not in successes
    failed = pipeline._runtime.store.backend.load_failed()
    assert "rt_disc_bad::seed_bad" in failed
    assert "Invalid spawned job" in failed["rt_disc_bad::seed_bad"]["error_message"]

    queue = pipeline._runtime.store.backend.load_queue()
    assert [row for row in queue if row["task_type"] == "rt_child"] == []
    assert [key for key in successes if key.startswith("rt_child::")] == []
    assert _discovery_cursors(pipeline) == {}
