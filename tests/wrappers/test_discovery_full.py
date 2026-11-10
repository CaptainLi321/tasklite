"""Discovery full 扫描、missing 检测与前缀单射回归。

覆盖：
- scan_mode="full"：扫到空页终止（不整页命中提前停）；已见内容不重复 spawn
- on_missing 回调：full 模式收到「已见但未扫到」差集（源端缺失/删除检测）
- incremental 模式回归：整页命中终止
- cursor_key 前缀单射性（length-prefix 防跨组碰撞）
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tasklite import TaskLiteError

from tasklite import Job, TaskLite, TaskResult
from tasklite.wrappers.discovery import encode_content_id, register_discovery

PAGESIZE = 2
SEEN_PREFIX = "__tasklite_discovery_seen_v1__:"


# ── 模块级回调（可 pickle）───────────────────────────────────────────


def _fetch(job, ctx, page):
    """从 payload 的 posts 分页读取（可 pickle：数据经 payload 传入）。

    full/incremental 的可观测差异在 **fetch 页数**——若
    payload 带 fetch_log 路径，把每次调用的 page 号追加落盘（fetch 在子
    进程执行，模块级计数器不可见，用文件传递），测试据此断言页访问次数。
    """
    flog = job.payload.get("fetch_log")
    if flog:
        with open(flog, "a") as f:
            f.write(f"{page}\n")
    posts = job.payload.get("posts", [])
    start = (page - 1) * PAGESIZE
    return posts[start:start + PAGESIZE]


def _item_id(item):
    return str(item["id"])


def _item_id_fail(item):
    raise KeyError("boom")  # 模块级：模拟源结构异常（整页 id_func 全失败）


def _item_id_partial_raise(item):
    """模块级：仅 id=5 抛异常，其余正常（部分失败，非整页全失败）。"""
    if item["id"] == 5:
        raise KeyError("broken item")
    return str(item["id"])


def _item_id_partial_invalid(item):
    """模块级：仅 id=5 返回非 str，其余正常。"""
    if item["id"] == 5:
        return 42
    return str(item["id"])


def _process(job, ctx, item, content_id):
    ctx.spawn(Job("child", content_id))


def _group_key(payload):
    return f"g_{payload['group']}"  # 模块级（group_key_func 必须可 pickle）


def _child_ok(job, ctx):
    return TaskResult.success()


def _pipeline(tmp_path, name="r5"):
    return TaskLite(backend="sqlite", state_path=tmp_path / "state" / f"{name}.db", max_workers=3)


def _posts(n):
    return sorted([{"id": i} for i in range(n)], key=lambda p: p["id"], reverse=True)


def _seen_cursor_key(*, task_type="disc", process_type="child", group_key=None):
    prefix = "" if group_key is None else f"{len(group_key)}:{group_key}"
    scope = (
        f"{len(task_type)}:{task_type}"
        f"{len(process_type)}:{process_type}"
        f"{len(prefix)}:{prefix}"
    )
    return SEEN_PREFIX + scope


def _seen_cursor_value(seen):
    return json.dumps(
        {"version": 1, "seen": sorted(seen)},
        ensure_ascii=True,
        separators=(",", ":"),
    )


# ══════════════════════════════════════════════════════════════════════
# full 模式：扫到空页 + 不重复 spawn
# ══════════════════════════════════════════════════════════════════════


class TestFullMode:
    def _log_count(self, log_path):
        """读取 fetch 日志行数（fetch 页数）。"""
        if not Path(log_path).exists():
            return 0
        with open(log_path) as f:
            return len(f.readlines())

    def test_full_scans_to_empty_page(self, tmp_path):
        """full 模式扫到空页终止（不因整页命中提前停）——**fetch 4 次**
        （3 数据页 + 1 空页），不是首页整页命中即停。"""
        log_path = tmp_path / "fetches.log"
        p = _pipeline(tmp_path)
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", trigger="once")
        p.register_task("child", _child_ok)
        # 5 条 → 3 页（2/2/1）+ 空页；首次扫描无已见 → 全量 spawn
        p.enqueue(Job("disc", "seed", payload={"posts": _posts(5), "fetch_log": str(log_path)}))
        p.run()
        successes = p._runtime.store.backend.load_successes()
        children = [k for k in successes if k.startswith("child::")]
        assert len(children) == 5, f"full 模式首次扫描应 spawn 全部 5 个: {children}"
        assert self._log_count(log_path) == 4, \
            f"full 模式应 fetch 4 次（3 页+空页），实际: {self._log_count(log_path)}"

    def test_full_rerun_no_respawn_seen(self, tmp_path):
        """full 模式重跑：已见内容不重复 spawn（成本只在 fetch）——重跑仍
        fetch 4 次（3 页+空页），但不 spawn 已见内容。"""
        log_path = tmp_path / "fetches.log"
        p = _pipeline(tmp_path)
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full")
        p.register_task("child", _child_ok)
        p.enqueue(Job("disc", "seed", payload={"posts": _posts(5), "fetch_log": str(log_path)}))
        p.run()
        successes = p._runtime.store.backend.load_successes()
        assert len([k for k in successes if k.startswith("child::")]) == 5
        first_run_fetches = self._log_count(log_path)
        assert first_run_fetches == 4

        # 重跑（every_run 注入）→ full 扫完 3 页 + 空页，已见不重复 spawn
        p.enqueue(Job("disc", "seed2", payload={"posts": _posts(5), "fetch_log": str(log_path)}))
        p.run()
        successes = p._runtime.store.backend.load_successes()
        assert len([k for k in successes if k.startswith("child::")]) == 5, \
            "full 重跑不得重复 spawn 已见内容"
        second_run_fetches = self._log_count(log_path) - first_run_fetches
        assert second_run_fetches == 4, \
            f"full 重跑应 fetch 4 次（3 页+空页），实际增量: {second_run_fetches}"

    def test_incremental_still_terminates_on_full_page_hit(self, tmp_path):
        """incremental（默认）回归：整页命中终止，不扫到空页——重跑 fetch
        **仅 1 次**（page1 整页命中即停），区别于 full 的 4 次。"""
        log_path = tmp_path / "fetches.log"
        p = _pipeline(tmp_path)
        register_discovery(p, "disc", _fetch, _item_id, _process, "child")
        p.register_task("child", _child_ok)
        # 首次全量（5 条 → 3 页）
        p.enqueue(Job("disc", "seed", payload={"posts": _posts(5), "fetch_log": str(log_path)}))
        p.run()
        first_run_fetches = self._log_count(log_path)
        assert first_run_fetches == 4
        # 重跑：page1=[4,3] 全已见 → 整页命中即停（不翻后续页）
        p.enqueue(Job("disc", "seed2", payload={"posts": _posts(5), "fetch_log": str(log_path)}))
        p.run()
        successes = p._runtime.store.backend.load_successes()
        assert len([k for k in successes if k.startswith("child::")]) == 5
        second_run_fetches = self._log_count(log_path) - first_run_fetches
        assert second_run_fetches == 1, \
            f"incremental 重跑应 fetch 1 次（整页命中即停），实际增量: {second_run_fetches}"

    def test_invalid_scan_mode_rejected(self, tmp_path):
        p = _pipeline(tmp_path)
        with pytest.raises(TaskLiteError, match="scan_mode"):
            register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                               scan_mode="bogus")


# ══════════════════════════════════════════════════════════════════════
# on_missing 检测（源端删除/缺失）
# ══════════════════════════════════════════════════════════════════════


def _on_missing(job, ctx, missing_ids):
    """回调写文件观测（子进程隔离——模块级 dict 在子进程是副本）。"""
    report = job.payload.get("missing_report")
    if report:
        Path(report).write_text("\n".join(sorted(missing_ids)))


def _on_missing_boom(job, ctx, missing_ids):
    """on_missing 回调抛异常——框架必须吞掉
    （logger.error）而不让 discovery job 崩（否则发现链死亡）。"""
    raise RuntimeError("on_missing bug")


class TestMissingDetection:
    def test_missing_reports_deleted_contents(self, tmp_path):
        """full 模式 + on_missing：已见但本次未扫到 = 源站删除。"""
        p = _pipeline(tmp_path)
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"

        # 首次：5 条全扫 → 无 missing（报告文件不创建）
        p.enqueue(Job("disc", "seed", payload={"posts": _posts(5), "missing_report": str(report)}))
        p.run()
        assert not report.exists(), "首次扫描无已见 → 无 missing"

        # 删 2 条（id 4、3）→ 重跑：只见到 0..2 → missing = 已见 - 本次 = {3,4}
        posts = [{"id": 2}, {"id": 1}, {"id": 0}]
        p.enqueue(Job("disc", "seed2", payload={"posts": posts, "missing_report": str(report)}))
        p.run()
        missing = set(report.read_text().splitlines())
        assert missing == {"3", "4"}, f"源站删除应报 missing: {missing}"

    def test_payload_scan_mode_full_override_no_false_missing(self, tmp_path):
        """回归：payload 覆盖 scan_mode="full" 时，本次已扫到的内容
        不得被误报「已删除」。

        handler 按默认 incremental 注册，job.payload 动态指定
        scan_mode="full"；本次扫描事实必须始终参与 missing 差集。"""
        p = _pipeline(tmp_path)
        # 不传 scan_mode → handler 默认 incremental；仅挂 on_missing
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           trigger="once", on_missing=_on_missing)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"

        # 首次动态 full 完整扫描建立本工具自己的 seen 游标。
        p.enqueue(Job("disc", "seed", payload={
            "posts": _posts(5), "scan_mode": "full",
            "missing_report": str(report),
        }))
        p.run()
        assert not report.exists()

        # 再次完整扫描仍无删除。
        p.enqueue(Job("disc", "seed2", payload={
            "posts": _posts(5), "scan_mode": "full",
            "missing_report": str(report),
        }))
        p.run()
        assert not report.exists(), \
            "payload 覆盖 full 时，本次已扫到的内容不得误报 missing"

        # 正向对照：只扫到 2、3 → 真正删除的 0、1、4 才报 missing。
        p.enqueue(Job("disc", "seed3", payload={
            "posts": [{"id": 3}, {"id": 2}], "scan_mode": "full",
            "missing_report": str(report),
        }))
        p.run()
        missing = set(report.read_text().splitlines())
        assert missing == {"0", "1", "4"}, \
            f"应报真实删除的 0/1/4，不得混入本次已扫到的 2/3: {missing}"

    def test_incremental_no_missing_callback(self, tmp_path):
        """incremental 模式不触发 on_missing（整页命中提前停，差集无意义）。"""
        p = _pipeline(tmp_path)
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           on_missing=_on_missing)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"
        p.enqueue(Job("disc", "seed", payload={"posts": _posts(5), "missing_report": str(report)}))
        p.run()
        assert not report.exists(), "incremental 不得触发 on_missing"

    def test_max_pages_truncation_no_missing_report(self, tmp_path):
        """full 模式 max_pages 截断不触发 on_missing。"""
        p = _pipeline(tmp_path)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"

        # 先完整扫描，建立本工具自己的 seen 游标。
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing)
        p.enqueue(Job("disc", "seed", payload={"posts": _posts(6), "missing_report": str(report)}))
        p.run()
        assert not report.exists(), "完整扫描无删除 → 无 missing"

        # 同库新实例，max_pages=1 → 截断，只扫前 2 条。
        p2 = _pipeline(tmp_path)
        p2.register_task("child", _child_ok)
        register_discovery(p2, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing,
                           max_pages=1)
        p2.enqueue(Job("disc", "seed2", payload={"posts": _posts(6), "missing_report": str(report)}))
        p2.run()
        assert not report.exists(), "max_pages 截断不得误报 missing"

    def test_all_id_func_failed_early_stop_no_missing_report(self, tmp_path):
        """整页 id_func 全失败早停是不完整扫描，不触发 on_missing。"""
        p = _pipeline(tmp_path)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"

        # 先完成一次正常扫描。
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing)
        p.enqueue(Job("disc", "seed", payload={"posts": _posts(6), "missing_report": str(report)}))
        p.run()
        assert not report.exists()

        # 同库新实例，id_func 全失败 → page1 早停。
        p2 = _pipeline(tmp_path)
        p2.register_task("child", _child_ok)
        register_discovery(p2, "disc", _fetch, _item_id_fail, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing)
        p2.enqueue(Job("disc", "seed2", payload={"posts": _posts(6), "missing_report": str(report)}))
        p2.run()
        assert not report.exists(), "id_func 全失败早停不得误报 missing"

    def test_partial_id_func_raise_no_missing_report(self, tmp_path):
        """full 模式单条 id_func 抛异常是不完整扫描，不误报 missing。"""
        p = _pipeline(tmp_path)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"

        # 先完成一次正常扫描。
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing)
        p.enqueue(Job("disc", "seed", payload={"posts": _posts(6), "missing_report": str(report)}))
        p.run()
        assert not report.exists()

        # 同库新实例。源上 6 条全在，但 id=5 的 id_func 抛异常。
        p2 = _pipeline(tmp_path)
        p2.register_task("child", _child_ok)
        register_discovery(p2, "disc", _fetch, _item_id_partial_raise, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing)
        p2.enqueue(Job("disc", "seed2", payload={"posts": _posts(6), "missing_report": str(report)}))
        p2.run()
        assert not report.exists(), "id_func 部分失败的不完整扫描不得误报 missing"

    def test_partial_id_func_invalid_return_no_missing_report(self, tmp_path):
        """id_func 非法返回值同属扫描不完整，不触发 on_missing。"""
        p = _pipeline(tmp_path)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"

        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing)
        p.enqueue(Job("disc", "seed", payload={"posts": _posts(6), "missing_report": str(report)}))
        p.run()
        assert not report.exists()

        # 同库新实例，id_func 对 id=5 返回非 str。
        p2 = _pipeline(tmp_path)
        p2.register_task("child", _child_ok)
        register_discovery(p2, "disc", _fetch, _item_id_partial_invalid, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing)
        p2.enqueue(Job("disc", "seed2", payload={"posts": _posts(6), "missing_report": str(report)}))
        p2.run()
        assert not report.exists(), "id_func 非法返回的不完整扫描不得误报 missing"

    def test_on_missing_filters_own_cursor_key_group(self, tmp_path):
        """full 模式 on_missing 只报本组，不混入共享 process task_type 的他组。"""
        p = _pipeline(tmp_path)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           group_key_func=_group_key,
                           scan_mode="full", trigger="once", on_missing=_on_missing)

        # 组 A：完整扫描 4 条。
        p.enqueue(Job("disc", "seedA", payload={"posts": [{"id": i} for i in range(4)],
                                                 "group": "A", "missing_report": str(report)}))
        p.run()
        assert not report.exists()
        # 组 B：完整扫描 2 条，与 A 共享 process_task_type。
        p.enqueue(Job("disc", "seedB", payload={"posts": [{"id": 9}, {"id": 8}],
                                                 "group": "B", "missing_report": str(report)}))
        p.run()
        assert not report.exists()

        # 组 A 再次 full 扫描（只 A 的内容 0..3）→ missing 只应含 A 的已删内容
        p.enqueue(Job("disc", "seedA2", payload={"posts": [{"id": 3}, {"id": 2}],
                                                  "group": "A", "missing_report": str(report)}))
        p.run()
        missing = set(report.read_text().splitlines()) if report.exists() else set()
        # 本组 missing 恰好 = A 的已删内容（length-prefix：3%3Ag_A）
        assert missing == {"3%3Ag_A0", "3%3Ag_A1"}, f"本组已删应报全、跨组不得误报: {missing}"

    def test_on_missing_deduplicates_seen_entries(self, tmp_path):
        """重复游标项在 missing 差集中只回调一次。"""
        p = _pipeline(tmp_path)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"
        register_discovery(
            p,
            "disc",
            _fetch,
            _item_id,
            _process,
            "child",
            scan_mode="full",
            on_missing=_on_missing,
        )
        duplicated = [
            encode_content_id("1"),
            encode_content_id("1"),
            encode_content_id("2"),
            encode_content_id("2"),
        ]
        p.set_cursor(
            _seen_cursor_key(),
            json.dumps(
                {"version": 1, "seen": duplicated},
                ensure_ascii=True,
                separators=(",", ":"),
            ),
        )

        p.enqueue(
            Job(
                "disc",
                "empty",
                payload={"posts": [], "missing_report": str(report)},
            )
        )
        p.run()

        assert report.read_text().splitlines() == ["1", "2"]

    def test_on_missing_long_cursor_key_truncated_domain(self, tmp_path):
        """超长组前缀进入编码截断域时，on_missing 仍保持组隔离。"""
        p = _pipeline(tmp_path)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           group_key_func=_group_key,
                           scan_mode="full", trigger="once", on_missing=_on_missing)

        long_a = "a" * 200
        long_b = "b" * 200
        prefix_a = f"{len('g_' + long_a)}:g_{long_a}"
        prefix_b = f"{len('g_' + long_b)}:g_{long_b}"

        # 组 A（超长 key）：完整扫描 4 条。
        p.enqueue(Job("disc", "seedA", payload={"posts": [{"id": i} for i in range(4)],
                                                 "group": long_a, "missing_report": str(report)}))
        p.run()
        assert not report.exists()
        # 组 B（另一超长 key，共享 process_task_type）：完整扫描 2 条。
        p.enqueue(Job("disc", "seedB", payload={"posts": [{"id": 9}, {"id": 8}],
                                                 "group": long_b, "missing_report": str(report)}))
        p.run()
        assert not report.exists()

        # 组 A 只剩 0、1 → missing 应恰为 A 的已删 2、3，不混入 B 的 8、9
        p.enqueue(Job("disc", "seedA2", payload={"posts": [{"id": 1}, {"id": 0}],
                                                  "group": long_a, "missing_report": str(report)}))
        p.run()
        missing = set(report.read_text().splitlines()) if report.exists() else set()
        expected = {encode_content_id(prefix_a + cid) for cid in ("2", "3")}
        other = {encode_content_id(prefix_b + cid) for cid in ("8", "9")}
        assert missing == expected, f"超长 key 组的已删内容必须报出: {missing}"
        assert not (missing & other), f"跨组内容不得误报: {missing & other}"

    def test_on_missing_exception_is_swallowed(self, tmp_path):
        """on_missing 回调抛异常必须被框架吞掉
        （logger.error + 继续）——否则 discovery job 崩溃 → 发现链死亡。
        触发路径：full 模式扫描到已见内容缺失（有 missing 差集）。"""
        p = _pipeline(tmp_path)
        p.register_task("child", _child_ok)
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing_boom)

        # 首次：5 条全扫 → 无 missing（on_missing 不调用，无异常）
        p.enqueue(Job("disc", "seed", payload={"posts": _posts(5)}))
        p.run()
        successes = p._runtime.store.backend.load_successes()
        assert "disc::seed" in successes

        # 删 2 条 → 重跑：有 missing 差集 → on_missing 抛异常 → 必须被吞
        posts = [{"id": 2}, {"id": 1}, {"id": 0}]
        p.enqueue(Job("disc", "seed2", payload={"posts": posts}))
        p.run()
        successes = p._runtime.store.backend.load_successes()
        assert "disc::seed2" in successes, \
            "on_missing 抛异常不得让 discovery job 崩溃/进失败档案"

    def test_unicode_escape_injective(self):
        """回归：UTF-8 字节转义消除变长 hex 前缀歧义——不同 Unicode 输入
        派生全局唯一的 job_id（防止多对一碰撞）。"""
        cyr_yi = "Ӣ" + "D"   # U+04E2 + D
        zhong = "中"          # U+4E2D
        assert encode_content_id(cyr_yi) != encode_content_id(zhong), \
            "变长 hex 前缀歧义导致碰撞（UTF-8 字节转义应消除）"
        # UTF-8 转义后确定格式：非 ASCII 每字节 %XX
        assert encode_content_id(zhong) == "%E4%B8%AD"
        assert encode_content_id("é") == "%C3%A9"
        # 与字面 % 序列不混淆
        assert encode_content_id("%E4%B8%AD") != encode_content_id(zhong)


# ══════════════════════════════════════════════════════════════════════
# cursor_key 前缀单射与 missing 端到端回归
# ══════════════════════════════════════════════════════════════════════


class TestCursorKeyPrefixInjective:
    """cursor_key 前缀拼接必须保持单射。

    键名前缀转义必须保证单射性：
    `(key="ab", cid="c_d")` 与 `(key="ab_c", cid="d")` 派生相同 job_id
    → 跨组碰撞 → 提交去重静默吞内容。"""

    def test_prefix_collision_pair_now_distinct(self):
        # 2:ab 与 4:ab_c 的边界可复原，派生不同。
        new_a = encode_content_id("2:ab" + "c_d")
        new_b = encode_content_id("4:ab_c" + "d")
        assert new_a != new_b, f"length-prefix 必须消除碰撞: {new_a} vs {new_b}"

    def test_same_key_cid_stable_and_deterministic(self):
        a1 = encode_content_id("2:ab" + "c_d")
        a2 = encode_content_id("2:ab" + "c_d")
        assert a1 == a2
        # 不同 key 相同 cid → 不同 job_id
        assert encode_content_id("2:ab" + "x") != encode_content_id("2:cd" + "x")

    def test_end_to_end_group_isolation_preserved(self, tmp_path):
        """端到端：多组共享 process_task_type 时，on_missing 只报本组
        （语义在 length-prefix 下保持）。"""
        p = _pipeline(tmp_path)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing.txt"
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           group_key_func=_group_key,
                           scan_mode="full", trigger="once", on_missing=_on_missing)
        # 组 A：4 条
        p.enqueue(Job("disc", "seedA", payload={"posts": [{"id": i} for i in range(4)],
                                                 "group": "A", "missing_report": str(report)}))
        p.run()
        assert not report.exists()
        # 组 B：2 条
        p.enqueue(Job("disc", "seedB", payload={"posts": [{"id": 9}, {"id": 8}],
                                                 "group": "B", "missing_report": str(report)}))
        p.run()
        assert not report.exists()
        # 组 A 重扫（只 A 内容）→ missing 只含 A 的已删
        p.enqueue(Job("disc", "seedA2", payload={"posts": [{"id": 3}, {"id": 2}],
                                                  "group": "A", "missing_report": str(report)}))
        p.run()
        missing = set(report.read_text().splitlines()) if report.exists() else set()
        assert missing == {"3%3Ag_A0", "3%3Ag_A1"}, f"本组已删应报全、跨组不得误报: {missing}"


class TestIndependentSeenCursorForMissing:
    def test_on_missing_uses_discovery_seen_cursor(self, tmp_path):
        """on_missing 差集来自 Discovery 自己的 seen 游标。"""
        p = _pipeline(tmp_path)
        p.register_task("child", _child_ok)
        report = tmp_path / "missing2.txt"
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", trigger="once", on_missing=_on_missing)
        # 首次：spawn 2 条（无已见）
        p.enqueue(Job("disc", "seed1", payload={"posts": [{"id": 1}, {"id": 2}],
                                                 "missing_report": str(report)}))
        p.run()
        assert not report.exists()
        # 重扫只剩 id=2 → id=1 已删 → on_missing 报差集
        p.enqueue(Job("disc", "seed2", payload={"posts": [{"id": 2}],
                                                 "missing_report": str(report)}))
        p.run()
        missing = set(report.read_text().splitlines()) if report.exists() else set()
        assert missing == {"1"}, f"on_missing 应报已删内容 id=1: {missing}"
