"""R5发现模式增强测试 —— full 模式、missing 检测与死锁宽限。

覆盖：
- scan_mode="full"：扫到空页终止（不整页命中提前停）；已见内容不重复 spawn
- on_missing 回调：full 模式收到「已见但未扫到」差集（源端缺失/删除检测）
- incremental 模式回归：整页命中终止
- 死锁宽限（_dependency_grace）：可运行候选 → 宽限；无候选 → 立即 DLQ；超时 → DLQ
"""

import pytest
from pathlib import Path
from tasklite import TaskLite, Job
from tasklite.models.job import Job as J
from tasklite.models.state import PipelineState
from tasklite.wrappers.discovery import register_discovery

PAGESIZE = 2


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


def _process(job, ctx, item, content_id):
    ctx.spawn(Job("child", content_id))


def _group_key(payload):
    return f"g_{payload['group']}"  # 模块级（cursor_key_func 必须可 pickle）


def _child_ok(job, ctx):
    return True


def _pipeline(tmp_path, name="r5"):
    return TaskLite(name=name, state_dir=tmp_path / "state", backend="sqlite", max_workers=3)


def _posts(n):
    return sorted([{"id": i} for i in range(n)], key=lambda p: p["id"], reverse=True)


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
                           scan_mode="full", rerun="never")
        p.register_handler("child", _child_ok)
        # 5 条 → 3 页（2/2/1）+ 空页；首次扫描无已见 → 全量 spawn
        p.enqueue([Job("disc", "seed", payload={"posts": _posts(5), "fetch_log": str(log_path)})])
        p.run()
        wall = p.backend.load_wall()
        children = [k for k in wall if k.startswith("child::")]
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
        p.register_handler("child", _child_ok)
        p.enqueue([Job("disc", "seed", payload={"posts": _posts(5), "fetch_log": str(log_path)})])
        p.run()
        wall = p.backend.load_wall()
        assert len([k for k in wall if k.startswith("child::")]) == 5
        first_run_fetches = self._log_count(log_path)
        assert first_run_fetches == 4

        # 重跑（every_run 注入）→ full 扫完 3 页 + 空页，已见不重复 spawn
        p.enqueue([Job("disc", "seed2", payload={"posts": _posts(5), "fetch_log": str(log_path)})])
        p.run()
        wall = p.backend.load_wall()
        assert len([k for k in wall if k.startswith("child::")]) == 5, \
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
        p.register_handler("child", _child_ok)
        # 首次全量（5 条 → 3 页）
        p.enqueue([Job("disc", "seed", payload={"posts": _posts(5), "fetch_log": str(log_path)})])
        p.run()
        first_run_fetches = self._log_count(log_path)
        assert first_run_fetches == 4
        # 重跑：page1=[4,3] 全已见 → 整页命中即停（不翻后续页）
        p.enqueue([Job("disc", "seed2", payload={"posts": _posts(5), "fetch_log": str(log_path)})])
        p.run()
        wall = p.backend.load_wall()
        assert len([k for k in wall if k.startswith("child::")]) == 5
        second_run_fetches = self._log_count(log_path) - first_run_fetches
        assert second_run_fetches == 1, \
            f"incremental 重跑应 fetch 1 次（整页命中即停），实际增量: {second_run_fetches}"

    def test_invalid_scan_mode_rejected(self, tmp_path):
        p = _pipeline(tmp_path)
        with pytest.raises(ValueError, match="scan_mode"):
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
                           scan_mode="full", rerun="never", on_missing=_on_missing)
        p.register_handler("child", _child_ok)
        report = tmp_path / "missing.txt"

        # 首次：5 条全扫 → 无 missing（报告文件不创建）
        p.enqueue([Job("disc", "seed", payload={"posts": _posts(5), "missing_report": str(report)})])
        p.run()
        assert not report.exists(), "首次扫描无已见 → 无 missing"

        # 删 2 条（id 4、3）→ 重跑：只见到 0..2 → missing = 已见 - 本次 = {3,4}
        posts = [{"id": 2}, {"id": 1}, {"id": 0}]
        p.enqueue([Job("disc", "seed2", payload={"posts": posts, "missing_report": str(report)})])
        p.run()
        missing = set(report.read_text().splitlines())
        assert missing == {"3", "4"}, f"源站删除应报 missing: {missing}"

    def test_payload_scan_mode_full_override_no_false_missing(self, tmp_path):
        """回归：payload 覆盖 scan_mode="full" 时，本次已扫到的内容
        不得被误报「已删除」。

        handler 按默认 incremental 注册，job.payload 动态指定
        scan_mode="full"。修复前：seen_this_run 的收集条件误读 handler
        构造期默认值（恒为 "incremental"）而非 payload 覆盖后的值——
        收集端恒不收集，差集 = known - ∅ 把全部已见内容误报 missing，
        而 on_missing 误报会触发业务的破坏性动作。"""
        p = _pipeline(tmp_path)
        # 不传 scan_mode → handler 默认 incremental；仅挂 on_missing
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           rerun="never", on_missing=_on_missing)
        p.register_handler("child", _child_ok)
        report = tmp_path / "missing.txt"

        # 预置已见内容 0..4（known 非空，差集才有误报素材）
        p.seed_wall([f"child::{i}" for i in range(5)])

        # payload 覆盖为 full：本次完整扫到全部已见内容 → 无删除
        # 修复前：seen_this_run 恒空 → 0..4 全部误报「已删除」
        p.enqueue([Job("disc", "seed", payload={
            "posts": _posts(5), "scan_mode": "full",
            "missing_report": str(report),
        })])
        p.run()
        assert not report.exists(), \
            "payload 覆盖 full 时，本次已扫到的内容不得误报 missing"

        # 正向对照：只扫到 2、3 → 真正删除的 0、1、4 才报 missing
        # （确认修复没有把 on_missing 整个静默掉）
        p.enqueue([Job("disc", "seed2", payload={
            "posts": [{"id": 3}, {"id": 2}], "scan_mode": "full",
            "missing_report": str(report),
        })])
        p.run()
        missing = set(report.read_text().splitlines())
        assert missing == {"0", "1", "4"}, \
            f"应恰好报真实删除的 0/1/4，不得混入本次已扫到的 2/3: {missing}"

    def test_incremental_no_missing_callback(self, tmp_path):
        """incremental 模式不触发 on_missing（整页命中提前停，差集无意义）。"""
        p = _pipeline(tmp_path)
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           on_missing=_on_missing)
        p.register_handler("child", _child_ok)
        report = tmp_path / "missing.txt"
        p.enqueue([Job("disc", "seed", payload={"posts": _posts(5), "missing_report": str(report)})])
        p.run()
        assert not report.exists(), "incremental 不得触发 on_missing"

    def test_max_pages_truncation_no_missing_report(self, tmp_path):
        """ 回归：full 模式 max_pages 截断不触发 on_missing。

        原测试只跑一次（首次 run wall 空 → known 空 → on_missing
        不调用，pre-fix 也通过）——不 kill 变异。正确场景：先完整扫描建立
        wall（known 非空），再截断 run——pre-fix 会把更深页内容误报为
        「已删除」，post-fix 因 completed_full=False 不触发。
        """
        p = _pipeline(tmp_path)
        p.register_handler("child", _child_ok)
        report = tmp_path / "missing.txt"

        # Run 1：完整扫描（max_pages 默认大）→ 6 条进入 wall（known 非空）
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", rerun="never", on_missing=_on_missing)
        p.enqueue([Job("disc", "seed", payload={"posts": _posts(6), "missing_report": str(report)})])
        p.run()
        assert not report.exists(), "完整扫描无删除 → 无 missing"

        # Run 2：重新注册覆盖 max_pages=1 → 截断，只扫前 2 条
        # pre-fix：known 含 6 条、seen 只 2 条 → 误报 {0,1,2,3} 已删除
        # post-fix：completed_full=False → 不触发
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", rerun="never", on_missing=_on_missing,
                           max_pages=1)
        p.enqueue([Job("disc", "seed2", payload={"posts": _posts(6), "missing_report": str(report)})])
        p.run()
        assert not report.exists(), "max_pages 截断不得误报 missing"

    def test_all_id_func_failed_early_stop_no_missing_report(self, tmp_path):
        """ 残留回归：整页 id_func 全失败早停也是不完整扫描——不触发
        on_missing（该页 item 及更深页仍在源上，只是解析失败）。"""
        p = _pipeline(tmp_path)
        p.register_handler("child", _child_ok)
        report = tmp_path / "missing.txt"

        # Run 1：正常 id_func 完整扫描 → 6 条进入 wall
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", rerun="never", on_missing=_on_missing)
        p.enqueue([Job("disc", "seed", payload={"posts": _posts(6), "missing_report": str(report)})])
        p.run()
        assert not report.exists()

        # Run 2：id_func 全失败 → page1 早停（completed_full=False）
        # pre-fix：completed_full 恒 True → known 6 - seen 0 → 误报全 missing
        register_discovery(p, "disc", _fetch, _item_id_fail, _process, "child",
                           scan_mode="full", rerun="never", on_missing=_on_missing)
        p.enqueue([Job("disc", "seed2", payload={"posts": _posts(6), "missing_report": str(report)})])
        p.run()
        assert not report.exists(), "id_func 全失败早停不得误报 missing"

    def test_on_missing_filters_own_cursor_key_group(self, tmp_path):
        """ 回归：full 模式 on_missing 只报**本组**（cursor_key 命名空间）——
        多组共享 process_task_type 时，他组内容不得误报为「已删除」。"""
        p = _pipeline(tmp_path)
        p.register_handler("child", _child_ok)
        report = tmp_path / "missing.txt"
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           cursor_key_func=_group_key,
                           scan_mode="full", rerun="never", on_missing=_on_missing)

        # 组 A：完整扫描 4 条 → wall
        p.enqueue([Job("disc", "seedA", payload={"posts": [{"id": i} for i in range(4)],
                                                 "group": "A", "missing_report": str(report)})])
        p.run()
        assert not report.exists()
        # 组 B：完整扫描 2 条 → wall（与 A 共享 process_task_type="child"）
        p.enqueue([Job("disc", "seedB", payload={"posts": [{"id": 9}, {"id": 8}],
                                                 "group": "B", "missing_report": str(report)})])
        p.run()
        assert not report.exists()

        # 组 A 再次 full 扫描（只 A 的内容 0..3）→ missing 只应含 A 的已删内容
        # pre-fix：known 含 A+B 全部 → B 的 id 8、9 被误报
        p.enqueue([Job("disc", "seedA2", payload={"posts": [{"id": 3}, {"id": 2}],
                                                  "group": "A", "missing_report": str(report)})])
        p.run()
        missing = set(report.read_text().splitlines()) if report.exists() else set()
        # 本组 missing 恰好 = A 的已删内容（length-prefix：3%3Ag_A）；
        # pre-fix：known 含 A+B → B 的 3%3Ag_B_8/9 误报混入
        assert missing == {"3%3Ag_A0", "3%3Ag_A1"}, f"本组已删应报全、跨组不得误报: {missing}"

    def test_on_missing_exception_is_swallowed(self, tmp_path):
        """on_missing 回调抛异常必须被框架吞掉
        （logger.error + 继续）——否则 discovery job 崩溃 → 发现链死亡。
        触发路径：full 模式扫描到已见内容缺失（有 missing 差集）。"""
        p = _pipeline(tmp_path)
        p.register_handler("child", _child_ok)
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", rerun="never", on_missing=_on_missing_boom)

        # 首次：5 条全扫 → 无 missing（on_missing 不调用，无异常）
        p.enqueue([Job("disc", "seed", payload={"posts": _posts(5)})])
        p.run()
        wall = p.backend.load_wall()
        assert "disc::seed" in wall

        # 删 2 条 → 重跑：有 missing 差集 → on_missing 抛异常 → 必须被吞
        posts = [{"id": 2}, {"id": 1}, {"id": 0}]
        p.enqueue([Job("disc", "seed2", payload={"posts": posts})])
        p.run()
        wall = p.backend.load_wall()
        assert "disc::seed2" in wall, \
            "on_missing 抛异常不得让 discovery job 崩溃/DLQ"

    def test_unicode_escape_injective(self):
        """ 回归：UTF-8 字节转义消除变长 hex 前缀歧义——不同 Unicode
        单射编码保证不同 Unicode 输入派生全局唯一的 job_id：
        碰撞）。"""
        from tasklite.wrappers.discovery import sanitize_content_id

        cyr_d = "\u04E2" + "D"   # 不同 Unicode 字符串转义单射性验证：
        zhong = "\u4E2D"          # 防止多对一碰撞：
        assert sanitize_content_id(cyr_d) != sanitize_content_id(zhong), \
            "变长 hex 前缀歧义导致碰撞（UTF-8 字节转义应消除）"
        # UTF-8 转义后确定格式：非 ASCII 每字节 %XX
        assert sanitize_content_id(zhong) == "%E4%B8%AD"
        assert sanitize_content_id("\u00E9") == "%C3%A9"  # é
        # 与字面 % 序列不混淆
        assert sanitize_content_id("%E4%B8%AD") != sanitize_content_id(zhong)


# ══════════════════════════════════════════════════════════════════════
# 死锁宽限
# ══════════════════════════════════════════════════════════════════════


class TestDependencyGrace:
    def _init_state(self, p, queue):
        p._state = PipelineState(
            p.backend.load_wall(), p.backend.load_failed(),
            p.backend.load_cursors(), queue,
        )

    def test_grace_granted_when_runnable_candidate_exists(self, tmp_path):
        """有可运行候选（依赖在 wall）→ 宽限（等它 spawn 出依赖）。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", lambda j, c: True)
        p.seed_wall(["driver::spawner"])  # 候选的依赖已满足
        queue = [
            J("t", "consumer", depends_on=["child::c1"]).to_dict(),  # 缺失依赖
            J("t", "waiter", depends_on=["driver::spawner"]).to_dict(),  # 可运行候选
        ]
        self._init_state(p, queue)
        p._dep_grace_deadline = None
        assert p.store._dependency_grace([0]) is True, "存在可运行候选必须宽限"

    def test_grace_denied_when_all_waiting(self, tmp_path):
        """其余 job 都是等待者（依赖链尾）→ 不宽限，立即判死锁。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", lambda j, c: True)
        queue = [
            J("t", "j_missing", depends_on=["missing::dep"]).to_dict(),
            J("t", "j_downstream", depends_on=["t::j_missing"]).to_dict(),
        ]
        self._init_state(p, queue)
        p._dep_grace_deadline = None
        # j_downstream 依赖 t::j_missing（在队列，不在 wall）→ 非可运行候选
        assert p.store._dependency_grace([0]) is False, "无候选不得宽限"

    def test_grace_expires_after_deadline(self, tmp_path):
        """宽限超时 → 不再宽限（防依赖永不出现 → 无限等待）。"""
        import time as _t
        p = _pipeline(tmp_path)
        p.register_handler("t", lambda j, c: True)
        p.seed_wall(["driver::spawner"])
        queue = [
            J("t", "consumer", depends_on=["child::c1"]).to_dict(),
            J("t", "waiter", depends_on=["driver::spawner"]).to_dict(),
        ]
        self._init_state(p, queue)
        p._dep_grace_deadline = _t.monotonic() - 1.0  # 已过期
        assert p.store._dependency_grace([0]) is False, "宽限超时必须判死锁"

    def test_same_index_different_uid_resets_grace(self, tmp_path):
        """episode 判定必须用 **uid** 而非索引——B 组缺失
        job 恰好占据 A 组解决后的同位置（索引相同但身份不同）时，
        按 UID 重置截止，避免复用 A 组已过期 deadline。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", lambda j, c: True)
        p.seed_wall(["driver::spawner"])
        # 第一个 episode：consumerA 缺 child::a1（索引 0），同位置有可运行候选
        queue = [
            J("t", "consumerA", depends_on=["child::a1"]).to_dict(),
            J("t", "waiter", depends_on=["driver::spawner"]).to_dict(),
        ]
        self._init_state(p, queue)
        p._dep_grace_deadline = None
        assert p.store._dependency_grace([0]) is True
        deadline_after_first = p._dep_grace_deadline

        # 第二个 episode：consumerB 缺 child::b1（**同索引 0**，但 uid 不同）
        queue2 = [
            J("t", "consumerB", depends_on=["child::b1"]).to_dict(),
            J("t", "waiter", depends_on=["driver::spawner"]).to_dict(),
        ]
        self._init_state(p, queue2)
        assert p.store._dependency_grace([0]) is True
        assert p._dep_grace_deadline is not None
        import time as _t4
        assert p._dep_grace_deadline > deadline_after_first, \
            "同索引不同 uid 必须重置截止（新 episode），不得复用旧 deadline"

    def test_grace_resets_on_new_missing_episode(self, tmp_path):
        """宽限截止按 episode 重置——缺失集合变化（新的
        独立缺失场景）时重新授权，不被前一个已解决 episode 的截止吞掉。
        缺失依赖宽限期独立计时机制。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", lambda j, c: True)
        p.seed_wall(["driver::spawner"])  # 候选依赖已满足
        queue = [
            J("t", "consumerA", depends_on=["child::a1"]).to_dict(),
            J("t", "waiter", depends_on=["driver::spawner"]).to_dict(),
        ]
        self._init_state(p, queue)
        p._dep_grace_deadline = None
        # 第一个 episode：缺失 [0] → 授权（deadline 设置）
        assert p.store._dependency_grace([0]) is True
        deadline_after_first = p._dep_grace_deadline
        assert deadline_after_first is not None

        # 模拟第一个 episode 解决：队列换成 B 组（缺失集合 [1]，同候选）
        queue2 = [
            J("t", "waiter2", depends_on=["driver::spawner"]).to_dict(),
            J("t", "consumerB", depends_on=["child::b1"]).to_dict(),
        ]
        self._init_state(p, queue2)
        # 第二个 episode：缺失集合变化 → 截止重置 → 重新授权
        assert p.store._dependency_grace([1]) is True
        assert p._dep_grace_deadline is not None
        # 截止应被重置为新的 60s（明显晚于第一个 deadline）
        import time as _t2
        assert p._dep_grace_deadline > deadline_after_first, \
            "新 episode 必须重新授权（截止重置），而非沿用旧截止"

    def test_same_episode_deadline_kept(self, tmp_path):
        """同一缺失集合（持续未解决）→ 截止保持（不无限顺延），超时判死锁。"""
        import time as _t3
        p = _pipeline(tmp_path)
        p.register_handler("t", lambda j, c: True)
        p.seed_wall(["driver::spawner"])
        queue = [
            J("t", "consumer", depends_on=["child::c1"]).to_dict(),
            J("t", "waiter", depends_on=["driver::spawner"]).to_dict(),
        ]
        self._init_state(p, queue)
        p._dep_grace_deadline = None
        assert p.store._dependency_grace([0]) is True
        deadline = p._dep_grace_deadline
        # 同集合再次请求 → 截止不变（仍在宽限内）
        assert p.store._dependency_grace([0]) is True
        assert p._dep_grace_deadline == deadline
        # 截止已过 → 判死锁
        p._dep_grace_deadline = _t3.monotonic() - 1.0
        assert p.store._dependency_grace([0]) is False


class TestCursorKeyPrefixInjective:
    """cursor_key 前缀拼接必须保持单射。

    键名前缀转义必须保证单射性：
    `(key="ab", cid="c_d")` 与 `(key="ab_c", cid="d")` 派生相同 job_id
    → 跨组碰撞 → wall 去重静默吞内容。length-prefix（{len}:{key}）修复。"""

    def test_prefix_collision_pair_now_distinct(self):
        from tasklite.wrappers.discovery import sanitize_content_id
        # 验证 length-prefix 前缀隔离：2:ab vs 4:ab_c → 边界可复原，派生不同
        new_a = sanitize_content_id("2:ab" + "c_d")
        new_b = sanitize_content_id("4:ab_c" + "d")
        assert new_a != new_b, f"length-prefix 必须消除碰撞: {new_a} vs {new_b}"

    def test_same_key_cid_stable_and_deterministic(self):
        from tasklite.wrappers.discovery import sanitize_content_id
        a1 = sanitize_content_id("2:ab" + "c_d")
        a2 = sanitize_content_id("2:ab" + "c_d")
        assert a1 == a2
        # 不同 key 相同 cid → 不同 job_id
        assert sanitize_content_id("2:ab" + "x") != sanitize_content_id("2:cd" + "x")

    def test_end_to_end_group_isolation_preserved(self, tmp_path):
        """端到端：多组共享 process_task_type 时，on_missing 只报本组
        （语义在 length-prefix 下保持）。"""
        p = _pipeline(tmp_path)
        p.register_handler("child", _child_ok)
        report = tmp_path / "missing.txt"
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           cursor_key_func=_group_key,
                           scan_mode="full", rerun="never", on_missing=_on_missing)
        # 组 A：4 条
        p.enqueue([Job("disc", "seedA", payload={"posts": [{"id": i} for i in range(4)],
                                                 "group": "A", "missing_report": str(report)})])
        p.run()
        assert not report.exists()
        # 组 B：2 条
        p.enqueue([Job("disc", "seedB", payload={"posts": [{"id": 9}, {"id": 8}],
                                                 "group": "B", "missing_report": str(report)})])
        p.run()
        assert not report.exists()
        # 组 A 重扫（只 A 内容）→ missing 只含 A 的已删
        p.enqueue([Job("disc", "seedA2", payload={"posts": [{"id": 3}, {"id": 2}],
                                                  "group": "A", "missing_report": str(report)})])
        p.run()
        missing = set(report.read_text().splitlines()) if report.exists() else set()
        assert missing == {"3%3Ag_A0", "3%3Ag_A1"}, f"本组已删应报全、跨组不得误报: {missing}"


class TestSeenUidsAPI:
    """TaskContext.attempted_uids 公开只读快照 API。

    discovery 的 on_missing 差集计算改用该 API，替代 getattr 探测私有
    _wall_keys/_failed_keys。本测试锁定 API 契约：wall∪failed 并集、
    快照语义（返回值改动不影响内部集合）。"""

    def test_attempted_uids_union_of_wall_and_failed(self):
        from tasklite.models.context import TaskContext
        ctx = TaskContext(
            J("t", "j1"), {"w1": {}, "w2": {}}, {"f1": {}}, {},
        )
        assert ctx.attempted_uids() == frozenset({"w1", "w2", "f1"})

    def test_attempted_uids_empty_when_no_keys(self):
        from tasklite.models.context import TaskContext
        ctx = TaskContext(J("t", "j1"), set(), set(), {})
        assert ctx.attempted_uids() == frozenset()

    def test_attempted_uids_returns_snapshot_not_live_view(self):
        from tasklite.models.context import TaskContext
        wall = {"w1": {}}
        ctx = TaskContext(J("t", "j1"), wall, {"f1": {}}, {})
        snapshot = ctx.attempted_uids()
        wall["w2"] = {}  # 外部 mutate 源集合
        assert "w2" not in snapshot, "attempted_uids 必须返回快照而非活引用"

    def test_discovery_on_missing_uses_attempted_uids(self, tmp_path):
        """端到端回归：on_missing 差集计算经 attempted_uids 仍正确
        （替换 getattr 探测后行为不变）。"""
        p = _pipeline(tmp_path)
        p.register_handler("child", _child_ok)
        report = tmp_path / "missing2.txt"
        register_discovery(p, "disc", _fetch, _item_id, _process, "child",
                           scan_mode="full", rerun="never", on_missing=_on_missing)
        # 首次：spawn 2 条（无已见）
        p.enqueue([Job("disc", "seed1", payload={"posts": [{"id": 1}, {"id": 2}],
                                                 "missing_report": str(report)})])
        p.run()
        assert not report.exists()
        # 重扫只剩 id=2 → id=1 已删 → on_missing 报差集
        p.enqueue([Job("disc", "seed2", payload={"posts": [{"id": 2}],
                                                 "missing_report": str(report)})])
        p.run()
        missing = set(report.read_text().splitlines()) if report.exists() else set()
        assert missing == {"1"}, f"on_missing 应报已删内容 id=1: {missing}"
