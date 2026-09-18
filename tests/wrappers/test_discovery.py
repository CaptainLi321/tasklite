"""Tests for register_discovery() — REQUIREMENTS CONTRACT (see utils/discovery.py).

纯任务模型增量发现：已见集合 = wall/failed 快照（ctx.is_completed / ctx.is_failed），
无 cursor、无 seen 持久化、无 poison 表、无互斥注入。

V1 要求的契约验证场景（与 test_discovery.py 对偶）：
- 首次全量扫描（空 wall → 逐页 → 空页终止）
- 中间翻页运行（部分命中页继续翻，整页命中终止）
- 重新发现含新内容（新 id 被处理，旧 id 被 wall 快照跳过）
- 源端删除场景（删中间/删最旧/删最新 → 不重不漏）
- 崩溃重跑（异常传播 → 重扫 → wall 去重吸收）
- 确定性 job_id（净化后的 content_id 直接可用作 Job.job_id）
- 真实子进程（spawn 上下文，模块级回调可 pickle）
- 防御测试（D-1：id_func 异常、fetch 非 list、process_item_func 异常、
  cursor_key 非法、process_task_type 非法——每处防御分支配触发测试）
"""

import re
import tempfile

import pytest
from tasklite.testing import fake_ctx
from tasklite.pipeline import TaskLite
from tasklite.models.job import Job
from tasklite.models.context import TaskContext
from tasklite.wrappers.discovery import (
    sanitize_content_id,
    register_discovery,
)

# ══════════════════════════════════════════════════════════════════════
# 直调测试的数据源：模块级可变状态（handler 直调，同进程共享）
# ══════════════════════════════════════════════════════════════════════

PAGESIZE = 3
PTYPE = "download"  # process 子任务 task_type

_posts: list = []          # 当前内容源（按 id 倒序）
_seen_by_handler: set = set()  # process_item_func 实际处理过的 content_id

# 防御/崩溃测试的可变观测：模块级回调（可 pickle）与模块级状态配对
_processed_guard: list = []   # 防御测试里 process 回调记录的内容
_poison_attempts = {"n": 0}   # poison item 被尝试的次数
_crash_calls = {"n": 0}       # 崩溃 fetch 的调用计数


def _reset_source(items: list):
    """重置模拟数据源（id 倒序）与 handler 观测。"""
    global _posts, _seen_by_handler
    _posts = sorted(items, key=lambda p: p["id"], reverse=True)
    _seen_by_handler = set()


def _add_posts(items: list):
    """向数据源顶部追加新内容（模拟画师发新作品），保持 id 倒序。"""
    global _posts
    _posts = sorted(_posts + items, key=lambda p: p["id"], reverse=True)


def _delete_posts(ids: list):
    """从数据源删除指定 id（模拟源端删除），后续内容前移。"""
    global _posts
    _posts = [p for p in _posts if p["id"] not in set(ids)]


def _page_slice(page: int):
    """返回第 page 页的 items（从 1 开始）。"""
    start = (page - 1) * PAGESIZE
    return _posts[start:start + PAGESIZE]


# ── 模块级回调（可 pickle，供真实子进程测试）──────────────────────────


def fetch_func(job, ctx, page):
    """拉取一页；空列表表示页尾（T1a）。"""
    return list(_page_slice(page))


def id_func(item):
    return str(item["id"])


def cursor_key_func(payload):
    return f"cursor_{payload['artist_id']}"


def _seen_orig(seen):
    """从净化 content_id 还原原始 id：反转义 %XX + 兼容旧指纹后缀剥离。"""
    out = set()
    for cid in seen:
        m = re.match(r"^(.*)_[0-9a-f]{8}$", cid)
        base = m.group(1) if m else cid
        out.add(re.sub(r"%([0-9A-Fa-f]{2})", lambda mm: chr(int(mm.group(1), 16)), base))
    return out


def process_item_func(job, ctx, item, content_id):
    """记录处理过的 content_id（等价「spawn 子任务已提交」的观测）。"""
    _seen_by_handler.add(content_id)


# ── 防御/崩溃测试用的模块级回调（pickle preflight 要求模块级）──────────


def bad_key(payload):
    return ""


def bad_fetch(job, ctx, page):
    return "not-a-list"


def bad_id(item):
    return "" if item.get("good") else "x"


def ok_id(item):
    return "ok-item" if not item.get("good") else ""


def record_processed(job, ctx, item, content_id):
    _processed_guard.append(content_id)


def raising_id(item):
    raise KeyError("id") from None


def crash_fetch(job, ctx, page):
    _crash_calls["n"] += 1
    if _crash_calls["n"] >= 3:
        raise RuntimeError("simulated crash mid-scan")
    return list(_page_slice(page))


def poison_process(job, ctx, item, content_id):
    if _seen_orig({content_id}) == {"2"}:
        _poison_attempts["n"] += 1
        raise RuntimeError("poison item boom")
    _processed_guard.append(content_id)


# ── 工具 ──────────────────────────────────────────────────────────────


def _make_pipeline(name="test_disc"):
    """构造 pipeline（直调测试从不 run()，仅用其承载 handler 注册）。

    使用进程唯一临时目录（tempfile.mkdtemp），避免跨测试污染。
    """
    state_dir = tempfile.mkdtemp(prefix=f"tp_disc_{name}_")
    return TaskLite(name=name, state_dir=state_dir, backend="sqlite")


def _make_ctx(job=None, wall=None, failed=None):
    if job is None:
        job = Job("discover", "seed", payload={})
    return fake_ctx(job, wall=wall or (), failed=failed or ())


def _register(pipeline, **kwargs):
    """注册纯 discovery（无命名空间——prefix 为空，净化 = 单层指纹）。

    cursor_key_func 命名空间的语义由 test_cursor_key_namespace_prevents_id_collision
    单独覆盖；主路径测试用无前缀形态使 wall 快照构造简单且与 handler 一致。
    """
    register_discovery(
        pipeline,
        "discover",
        fetch_func,
        id_func,
        process_item_func,
        PTYPE,
        **kwargs,
    )


def _wall_from_seen():
    """把已处理的 content_id 转成 wall 快照（等价「子任务已完成」）。"""
    return {f"{PTYPE}::{cid}" for cid in _seen_by_handler}


def _handler(pipeline):
    # HandlerEntry 命名访问（替代三元组 [0] 下标）
    return pipeline.handlers["discover"].func


# ══════════════════════════════════════════════════════════════════════
# 终止条件（空页 / 整页命中）与部分命中继续翻页
# ══════════════════════════════════════════════════════════════════════


class TestTermination:
    def test_first_scan_full_pagination_until_empty(self):
        """首次全量扫描：空 wall → 逐页拉取直到空页终止。"""
        _reset_source([{"id": i} for i in range(7)])  # 7 条 → 3 页 + 1 空页
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) is True
        # 7 条全部是新的 → 全部被处理
        assert _seen_orig(_seen_by_handler) == {str(i) for i in range(7)}
        # 无任何 cursor 写入（纯任务模型的核心：不产生游标状态）
        assert ctx.cursor_updates == {}

    def test_partial_page_continues_scanning(self):
        """单页部分命中 → 继续翻页，直到整页命中或空页。"""
        _reset_source([{"id": i} for i in range(10)])  # 4 页 + 空页
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        # wall 预置 {9,8}（page1=[9,8,7] 部分命中）
        seen = {"9", "8"}
        wall = {f"{PTYPE}::{sanitize_content_id(c)}" for c in seen}
        ctx = _make_ctx(job, wall=wall)
        assert _handler(pipeline)(job, ctx) is True
        # 部分命中必须继续翻页：7 和后续页全部处理
        assert _seen_orig(_seen_by_handler) == {
            "7", "6", "5", "4", "3", "2", "1", "0",
        }

    def test_full_page_hit_stops_scanning(self):
        """整页命中 → 终止，不再翻页、不处理任何 item。"""
        _reset_source([{"id": i} for i in range(7)])  # page1=[6,5,4]
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        wall = {f"{PTYPE}::{sanitize_content_id(str(i))}" for i in range(7)}
        ctx = _make_ctx(job, wall=wall)
        assert _handler(pipeline)(job, ctx) is True
        assert _seen_by_handler == set(), "整页命中后不应处理任何 item"


# ══════════════════════════════════════════════════════════════════════
# 重新发现 / 源端删除不重不漏
# ══════════════════════════════════════════════════════════════════════


class TestRediscoveryAndDeletion:
    def test_rediscovery_only_new_content(self):
        """重新发现：只处理新增内容，旧内容被 wall 快照跳过。"""
        _reset_source([{"id": i} for i in range(6)])  # 首次 6 条
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) is True
        assert _seen_orig(_seen_by_handler) == {str(i) for i in range(6)}

        # 画师发 2 个新作品（id 10, 11 → 排到最前）
        _add_posts([{"id": 10}, {"id": 11}])
        wall2 = _wall_from_seen()
        _seen_by_handler.clear()
        ctx2 = _make_ctx(job, wall=wall2)
        assert _handler(pipeline)(job, ctx2) is True
        # 只处理新的 2 个，旧的 0-5 全部跳过
        assert _seen_orig(_seen_by_handler) == {"10", "11"}

    def test_deletion_middle_no_rescan_no_loss(self):
        """删中间：不触发全量重扫，前移内容仍被 wall 判定为已见。"""
        _reset_source([{"id": i} for i in range(9)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) is True
        assert _seen_orig(_seen_by_handler) == {str(i) for i in range(9)}

        # 删除中间 3 条（id 3,4,5）+ 发 1 个新作品 12
        _delete_posts([3, 4, 5])
        _add_posts([{"id": 12}])
        wall2 = _wall_from_seen()
        _seen_by_handler.clear()
        ctx2 = _make_ctx(job, wall=wall2)
        assert _handler(pipeline)(job, ctx2) is True
        # 只处理新的 12；前移的旧内容（如 page1 的 8,7）全部跳过
        assert _seen_orig(_seen_by_handler) == {"12"}

    def test_deletion_oldest_no_rescan(self):
        """删最旧：前移内容不触发重扫，不重复处理。"""
        _reset_source([{"id": i} for i in range(6)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) is True

        _delete_posts([0, 1])  # 删最旧
        _add_posts([{"id": 20}])
        wall2 = _wall_from_seen()
        _seen_by_handler.clear()
        ctx2 = _make_ctx(job, wall=wall2)
        assert _handler(pipeline)(job, ctx2) is True
        assert _seen_orig(_seen_by_handler) == {"20"}

    def test_deletion_newest_handled(self):
        """删最新（本次新增的内容被删）：重跑时不再处理已删内容。"""
        _reset_source([{"id": i} for i in range(4)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) is True

        # 发新作品 10 → 处理
        _add_posts([{"id": 10}])
        wall2 = _wall_from_seen()
        _seen_by_handler.clear()
        ctx2 = _make_ctx(job, wall=wall2)
        assert _handler(pipeline)(job, ctx2) is True
        assert _seen_orig(_seen_by_handler) == {"10"}

        # 新作品 10 被删 → 重跑：wall 快照仍含 10（历史），不重复处理
        _delete_posts([10])
        wall3 = wall2 | _wall_from_seen()  # 历史 0-3 ∪ 新增 10
        _seen_by_handler.clear()
        ctx3 = _make_ctx(job, wall=wall3)
        assert _handler(pipeline)(job, ctx3) is True
        assert _seen_orig(_seen_by_handler) == set()


# ══════════════════════════════════════════════════════════════════════
# 崩溃重跑（无游标 → 异常传播 → 重扫 → wall 去重吸收）
# ══════════════════════════════════════════════════════════════════════


class TestCrashRerun:
    def test_mid_scan_crash_propagates(self):
        """扫描中途 fetch 抛异常 → 异常传播（由错误分类决定重试/DLQ）。

        本模型无游标可推进，崩溃即整轮重扫——已 spawn 的子任务
        由确定性 job_id 的 wall 去重吸收，不重不漏。
        """
        _reset_source([{"id": i} for i in range(7)])
        _crash_calls["n"] = 0

        pipeline = _make_pipeline()
        register_discovery(
            pipeline, "discover", crash_fetch, id_func,
            process_item_func, PTYPE, cursor_key_func=cursor_key_func,
        )
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        with pytest.raises(RuntimeError, match="simulated crash"):
            _handler(pipeline)(job, ctx)

    def test_rerun_after_crash_rescans_and_dedups(self):
        """崩溃后重跑：完整重扫；已 spawn 的 item 被 wall 快照吸收。"""
        _reset_source([{"id": i} for i in range(6)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})

        # 第一次完整处理（模拟部分成功：page1/page2 已 spawn，page3 前崩溃）
        ctx = _make_ctx(job)
        _handler(pipeline)(job, ctx)
        assert _seen_orig(_seen_by_handler) == {str(i) for i in range(6)}

        # 重跑：wall 快照含全部已处理 uid → 整页命中终止，不重复处理
        wall2 = _wall_from_seen()
        _seen_by_handler.clear()
        ctx2 = _make_ctx(job, wall=wall2)
        assert _handler(pipeline)(job, ctx2) is True
        assert _seen_orig(_seen_by_handler) == set(), "重跑不得重复处理"


# ══════════════════════════════════════════════════════════════════════
# 交互矩阵：与任务模型既有特性的交叉
# ══════════════════════════════════════════════════════════════════════


class TestInteractionMatrix:
    def test_failed_child_is_completed_via_dlq(self):
        """process 子任务进 DLQ（FatalError）→ is_failed 判定为已见。

        poison 机制的替代：坏内容不再重新 spawn（等价旧版 dead-letter），
        且含坏 item 的页可正常整页命中（旧版含毒页永不命中，需 max_pages
        兜底——本模型严格更优）。
        """
        _reset_source([{"id": 3}, {"id": 2}, {"id": 1}])  # 一页内
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        # failed 快照含内容 "2"（上次 DLQ）
        failed = {f"{PTYPE}::{sanitize_content_id('2')}"}
        ctx = _make_ctx(job, failed=failed)
        assert _handler(pipeline)(job, ctx) is True
        # 坏内容 2 不再 spawn；3、1 正常处理
        assert _seen_orig(_seen_by_handler) == {"3", "1"}

    def test_wall_empty_rerun_rescans_all_dedup_by_framework(self):
        """wall 快照缺失（游标级状态丢失的等价物）→ 完整重扫。

        与旧版「游标丢失 → 从空游标重扫」对偶：handler 层必然重跑，
        去重由确定性 job_id 在框架 wall 层吸收（本测试只验证
        「重扫不丢内容」）。
        """
        _reset_source([{"id": i} for i in range(6)])
        pipeline = _make_pipeline()
        _register(pipeline)
        job = Job("discover", "seed", payload={"artist_id": "123"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) is True
        # 模拟 wall 快照丢失 → 空 wall 重扫
        _seen_by_handler.clear()
        ctx2 = _make_ctx(job)
        assert _handler(pipeline)(job, ctx2) is True
        assert _seen_orig(_seen_by_handler) == {str(i) for i in range(6)}

    def test_cursor_key_namespace_prevents_id_collision(self):
        """cursor_key 作命名空间：跨分组同 id 不串内容（job_id 含分组前缀）。"""
        _reset_source([{"id": "dup"}])
        pipeline = _make_pipeline()
        register_discovery(
            pipeline, "discover", fetch_func, id_func, process_item_func,
            PTYPE, cursor_key_func=cursor_key_func,
        )
        job = Job("discover", "seed", payload={"artist_id": "A"})
        ctx = _make_ctx(job)
        assert _handler(pipeline)(job, ctx) is True
        # 净化后的 content_id 必须以分组前缀开头（命名空间生效）。
        # prefix = f"{len(key)}:{key}"，`:` 单射转义为 %3A
        processed = list(_seen_by_handler)
        assert len(processed) == 1
        assert processed[0].startswith("8%3Acursor_Adup"), processed


# ══════════════════════════════════════════════════════════════════════
# 防御测试（D-1：每处防御分支必须配触发测试）
# ══════════════════════════════════════════════════════════════════════


class TestDefense:
    def test_cursor_key_invalid_rejected(self):
        """防御：cursor_key_func 返回空串 → fail-loud ValueError。"""
        _reset_source([{"id": 1}])
        pipeline = _make_pipeline()

        register_discovery(
            pipeline, "discover", fetch_func, id_func, process_item_func,
            PTYPE, cursor_key_func=bad_key,
        )
        job = Job("discover", "seed", payload={})
        with pytest.raises(ValueError, match="cursor_key_func"):
            _handler(pipeline)(job, _make_ctx(job))

    def test_fetch_non_list_rejected(self):
        """防御：fetch 返回非 list/tuple → fail-loud ValueError。"""
        _reset_source([{"id": 1}])
        pipeline = _make_pipeline()

        register_discovery(
            pipeline, "discover", bad_fetch, id_func, process_item_func, PTYPE,
        )
        job = Job("discover", "seed", payload={})
        with pytest.raises(ValueError, match="fetch_func"):
            _handler(pipeline)(job, _make_ctx(job))

    def test_id_func_bad_item_isolated(self):
        """防御：id_func 返回空串/非 str 的 item 被隔离，不崩扫描。"""
        _reset_source([{"id": 1, "good": True}])
        _processed_guard.clear()
        pipeline = _make_pipeline()

        register_discovery(
            pipeline, "discover", fetch_func, bad_id, record_processed, PTYPE,
        )
        job = Job("discover", "seed", payload={})
        # 整页 id_func 全失败 → 终止扫描但不抛异常（下轮重试）
        assert _handler(pipeline)(job, _make_ctx(job)) is True
        assert _processed_guard == []

        # 单 item id_func 失败 → 跳过该 item，其余照常处理
        _reset_source([{"id": 2, "good": False}])
        assert _handler(pipeline)(job, _make_ctx(job)) is True
        assert len(_processed_guard) == 1

    def test_id_func_raise_isolated(self):
        """防御：id_func 抛异常（如缺 id 键）逐 item 隔离，不崩 job。"""
        _reset_source([{"id": 1}])
        pipeline = _make_pipeline()

        register_discovery(
            pipeline, "discover", fetch_func, raising_id, process_item_func, PTYPE,
        )
        job = Job("discover", "seed", payload={})
        assert _handler(pipeline)(job, _make_ctx(job)) is True

    def test_process_item_func_raise_isolated(self):
        """防御：process_item_func 抛异常的坏 item 隔离，好 item 照常。

        与旧版 poison 语义的差异：无计数表——坏 item 每次 run 重试一次
        （有重试机会），扫描继续推进；页面级终止由 max_pages 兜底。
        """
        _reset_source([{"id": 3}, {"id": 2}, {"id": 1}])  # 一页内：坏 + 好
        _processed_guard.clear()
        _poison_attempts["n"] = 0

        pipeline = _make_pipeline()
        register_discovery(
            pipeline, "discover", fetch_func, id_func, poison_process, PTYPE,
        )
        job = Job("discover", "seed", payload={"artist_id": "123"})
        # 不抛异常（坏 item 被隔离），扫描正常完成
        assert _handler(pipeline)(job, _make_ctx(job)) is True
        assert _seen_orig(set(_processed_guard)) == {"3", "1"}
        assert _poison_attempts["n"] == 1
        _processed_guard.clear()
        wall = {f"{PTYPE}::{sanitize_content_id('3')}", f"{PTYPE}::{sanitize_content_id('1')}"}
        ctx2 = _make_ctx(job, wall=wall)
        assert _handler(pipeline)(job, ctx2) is True
        assert _processed_guard == [], "好 item 已见不得重复处理"
        assert _poison_attempts["n"] == 2, "坏 item 无 poison 表 → 每 run 重试一次"

    def test_process_task_type_validation(self):
        """防御：process_task_type 空/含 "::" → 注册时 fail-loud。"""
        _reset_source([])
        pipeline = _make_pipeline()
        with pytest.raises(TypeError, match="process_task_type"):
            register_discovery(
                pipeline, "discover", fetch_func, id_func, process_item_func, "",
            )
        with pytest.raises(ValueError, match="::"):
            register_discovery(
                pipeline, "discover", fetch_func, id_func, process_item_func,
                "bad::type",
            )

    def test_max_pages_validation(self):
        """防御：max_pages 非正整数 → 注册时 fail-loud。"""
        _reset_source([])
        pipeline = _make_pipeline()
        with pytest.raises(ValueError, match="max_pages"):
            register_discovery(
                pipeline, "discover", fetch_func, id_func, process_item_func,
                PTYPE, max_pages=0,
            )


# ══════════════════════════════════════════════════════════════════════
# 真实子进程（spawn 上下文）——数据源经 payload 传入（可 pickle）
# ══════════════════════════════════════════════════════════════════════

PAGESIZE_RT = 2


def _rt_fetch(job, ctx, page):
    # 数据源从 payload 传入：{"posts": [{"id": ...}, ...]}（按 id 倒序）
    posts = job.payload.get("posts", [])
    start = (page - 1) * PAGESIZE_RT
    return posts[start:start + PAGESIZE_RT]


def _rt_id(item):
    return str(item["id"])


def _rt_process(job, ctx, item, content_id):
    ctx.spawn(Job("rt_child", content_id, payload={"item": item}))


def _rt_cursor_key(payload):
    return f"rt_{payload['artist_id']}"


def _rt_child_handler(job, ctx):
    return True


def _rt_posts_upto(n):
    """生成按 id 倒序的 posts 列表（可 pickle 的纯数据）。"""
    return sorted([{"id": i} for i in range(n)], key=lambda p: p["id"], reverse=True)


def _rt_posts_with_new(old_count, new_ids):
    """旧内容 0..old_count-1（倒序）+ 新内容 new_ids（更大的 id，应排最前）。"""
    old = sorted([{"id": i} for i in range(old_count)], key=lambda p: p["id"], reverse=True)
    new = sorted([{"id": i} for i in new_ids], key=lambda p: p["id"], reverse=True)
    return new + old  # 新内容在最前（id 更大），保持全局倒序


def _rt_setup(tmp_path, monkeypatch, name="rt_disc"):
    import sys
    from pathlib import Path
    project_root = str(Path(__file__).resolve().parent.parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    monkeypatch.setenv("PYTHONPATH", project_root)
    return TaskLite(name=name, state_dir=tmp_path / "state", backend="sqlite", max_workers=3)


def test_real_subprocess_discovery(tmp_path, monkeypatch):
    """纯任务模型在真实 spawn 子进程中可运行（端到端全量发现）。"""
    pipeline = _rt_setup(tmp_path, monkeypatch)
    register_discovery(
        pipeline, "rt_disc", _rt_fetch, _rt_id, _rt_process, "rt_child",
        cursor_key_func=_rt_cursor_key,
    )
    pipeline.register_handler("rt_child", _rt_child_handler)
    pipeline.enqueue([Job("rt_disc", "seed", payload={
        "artist_id": "1", "posts": _rt_posts_upto(5),
    })])
    pipeline.run()

    wall = pipeline.backend.load_wall()
    child_uids = sorted(k for k in wall if k.startswith("rt_child::"))
    assert len(child_uids) == 5, f"expected 5 child jobs, got {child_uids}"
    # 命名空间前缀生效：job_id 以 5%3Art_1 开头（length-prefix，`:`→%3A）
    assert all(u.startswith("rt_child::4%3Art_1") for u in child_uids), child_uids
    # 纯任务模型不产生任何 cursor（区别于旧 discovery）
    cursors = pipeline.backend.load_cursors()
    assert cursors == {}, f"discovery must not write cursors: {cursors}"


def test_real_subprocess_rediscovery(tmp_path, monkeypatch):
    """真实子进程：重新发现只处理新增内容（端到端验证增量重新发现）。"""
    pipeline = _rt_setup(tmp_path, monkeypatch, name="rt_disc2")
    register_discovery(
        pipeline, "rt_disc2", _rt_fetch, _rt_id, _rt_process, "rt_child",
        cursor_key_func=_rt_cursor_key,
    )
    pipeline.register_handler("rt_child", _rt_child_handler)

    # 第一次发现：0..3
    pipeline.enqueue([Job("rt_disc2", "seed", payload={
        "artist_id": "2", "posts": _rt_posts_upto(4),
    })])
    pipeline.run()
    wall1 = pipeline.backend.load_wall()
    assert len([k for k in wall1 if k.startswith("rt_child::")]) == 4

    # 第二次发现：旧 0..3 + 新 10、11（新内容排最前）
    pipeline.enqueue([Job("rt_disc2", "seed2", payload={
        "artist_id": "2", "posts": _rt_posts_with_new(4, [10, 11]),
    })])
    pipeline.run()

    wall = pipeline.backend.load_wall()
    child_uids = sorted(k for k in wall if k.startswith("rt_child::"))
    assert len(child_uids) == 6, f"expected 6 total child jobs, got {len(child_uids)}"
    # 新增项：4%3Art_210 / 4%3Art_211（length-prefix，key="rt_2" len=4）
    assert any(u.startswith("rt_child::4%3Art_210") for u in child_uids), child_uids
    assert any(u.startswith("rt_child::4%3Art_211") for u in child_uids), child_uids


def test_real_subprocess_max_pages(tmp_path, monkeypatch):
    """持续更新源无法整页命中 → max_pages 停止扫描（不超时、不 DLQ）。"""
    pipeline = _rt_setup(tmp_path, monkeypatch, name="rt_disc3")
    posts = sorted([{"id": str(i)} for i in range(100)], key=lambda p: p["id"], reverse=True)
    register_discovery(
        pipeline, "rt_disc3", _rt_fetch, _rt_id, _rt_process, "rt_child",
        max_pages=2,
    )
    pipeline.register_handler("rt_child", _rt_child_handler)
    pipeline.enqueue([Job("rt_disc3", "seed", payload={"artist_id": "3", "posts": posts})])
    pipeline.run()  # 不应超时/DLQ

    wall = pipeline.backend.load_wall()
    child_uids = [k for k in wall if k.startswith("rt_child::")]
    # 只处理前 2 页 × 每页 2 条 = 4 个；不产生 partial cursor（旧版有）
    assert len(child_uids) == PAGESIZE_RT * 2, f"expected 4 children, got {child_uids}"
    failed = pipeline.backend.load_failed()
    assert not failed, f"max_pages 截断不应进 DLQ: {failed}"


def test_real_subprocess_full_mode_with_max_pages(tmp_path, monkeypatch):
    """full 模式下通过 max_pages 精确控制深扫页数，即使全命中也不提前终止。"""
    pipeline = _rt_setup(tmp_path, monkeypatch, name="rt_disc_full")
    posts = [{"id": str(i)} for i in range(100, 80, -1)]  # 100..81 倒序，每页 2 条共 10 页
    register_discovery(
        pipeline, "rt_disc_full", _rt_fetch, _rt_id, _rt_process, "rt_child",
        scan_mode="full",
        max_pages=3,
    )
    pipeline.register_handler("rt_child", _rt_child_handler)

    # 预先在 wall 中塞入前两页（100, 99, 98, 97）
    # 模拟历史深处有洞（第 3 页 96, 95 未下载）
    pipeline.seed_wall([
        "rt_child::100",
        "rt_child::99",
        "rt_child::98",
        "rt_child::97",
    ])

    # 派发 full 扫描：第 1、2 页即使整页全命中也不 break，继续扫到第 3 页，并在 max_pages=3 处截断
    pipeline.enqueue([Job("rt_disc_full", "seed", payload={"artist_id": "full1", "posts": posts})])
    pipeline.run()

    wall = pipeline.backend.load_wall()
    child_uids = [k for k in wall if k.startswith("rt_child::")]
    # 包含预置 4 个 + 第 3 页新处理的 2 个（96, 95）= 共 6 个
    assert len(child_uids) == 6
    assert "rt_child::96" in wall
    assert "rt_child::95" in wall
    # 第 4 页（94, 93）不应被扫描到
    assert "rt_child::94" not in wall


def test_payload_dynamic_override_scan_mode_and_max_pages(tmp_path, monkeypatch):
    """通过 Job.payload 动态传入 scan_mode='full' 和 max_pages 覆盖 handler 默认值。"""
    pipeline = _rt_setup(tmp_path, monkeypatch, name="rt_disc_dynamic")
    posts = [{"id": str(i)} for i in range(50, 30, -1)]  # 每页 2 条
    # 默认注册为 incremental + max_pages=1000
    register_discovery(
        pipeline, "rt_disc_dyn", _rt_fetch, _rt_id, _rt_process, "rt_child",
        scan_mode="incremental",
        max_pages=1000,
    )
    pipeline.register_handler("rt_child", _rt_child_handler)

    # 预置第 1 页（50, 49）到 wall
    pipeline.seed_wall([
        "rt_child::50",
        "rt_child::49",
    ])

    # 派发 job 时动态覆盖为 full 模式且 max_pages=2
    pipeline.enqueue([
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
    ])
    pipeline.run()

    wall = pipeline.backend.load_wall()
    # 预置 2 个 + 第 2 页新处理 2 个（48, 47）= 4 个
    assert "rt_child::48" in wall
    assert "rt_child::47" in wall
    # 第 3 页（46, 45）未被扫描
    assert "rt_child::46" not in wall


def test_invalid_payload_scan_mode_and_max_pages_rejected(tmp_path):
    """payload 中非法的 scan_mode 或 max_pages 应抛出 ValueError。"""
    pipeline = _make_pipeline()
    register_discovery(
        pipeline, "discover", fetch_func, id_func, process_item_func, PTYPE,
    )
    h = _handler(pipeline)

    # 非法 scan_mode
    job_bad_mode = Job("discover", "j1", payload={"scan_mode": "invalid_mode"})
    with pytest.raises(ValueError, match="scan_mode"):
        h(job_bad_mode, _make_ctx(job_bad_mode))

    # 非法 max_pages（负数 / 0 / bool / 字符串）
    for bad_max in (0, -1, True, "10"):
        job_bad_max = Job("discover", "j2", payload={"max_pages": bad_max})
        with pytest.raises(ValueError, match="max_pages"):
            h(job_bad_max, _make_ctx(job_bad_max))

