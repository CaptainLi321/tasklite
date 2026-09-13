"""R2生命周期语义测试 —— rerun 四策略。

覆盖：
- Job.rerun 字段校验 + 持久化
- never（默认）/ every_run / on_failure 三策略在 wall/failed 拦截点的行为
- 加载期修复豁免（every_run 任务在 wall 中不是残留）
- spawn 去重豁免（every_run 子任务不被 wall 拦截；queue/in-flight 仍拦截）
- register_discovery 默认 every_run 注入
- wall meta 运行历史（run_count / last_run_at / last_run_id）
"""

import pytest
from tasklite import TaskLite, Job
from tasklite.wrappers.discovery import register_discovery

def _ok_handler(job, ctx):
    return True


def _fail_handler(job, ctx):
    raise RuntimeError("deterministic fail")


def _fail_deterministic(job, ctx):
    raise RuntimeError("parent deterministic fail")


def _parent_spawns_every_run_child(job, ctx):
    ctx.spawn(Job("child", "c1", rerun="every_run"))
    return True


def _child_ok(job, ctx):
    return True


def _fail_then_ok_by_payload(job, ctx):
    # attempt>1 才成功：同 uid 重跑换新 payload 后必须真实执行才转 wall。
    return job.payload["attempt"] > 1


def _always_retry_handler(job, ctx):
    from tasklite.exceptions import RetryError
    raise RetryError("transient forever")


def _child_ok2(job, ctx):
    return True


def _pipeline(tmp_path, name="rerun"):
    return TaskLite(name=name, state_dir=tmp_path / "state", backend="sqlite")


# ══════════════════════════════════════════════════════════════════════
# Job.rerun 字段
# ══════════════════════════════════════════════════════════════════════


class TestJobRerunField:
    def test_default_unspecified_sentinel(self):
        """默认 rerun=None（未指定哨兵）——可被 discovery
        默认注入；与显式 "never" 区分（显式值一律尊重，不再被覆盖）。"""
        assert Job("t", "a").rerun is None
        assert Job.from_dict(Job("t", "a").to_dict()).rerun is None

    def test_valid_values_accepted(self):
        for v in ("never", "on_failure", "every_run", "on_input_change"):
            assert Job("t", "a", rerun=v).rerun == v

    def test_invalid_rejected(self):
        with pytest.raises(ValueError, match="rerun"):
            Job("t", "a", rerun="sometimes")

    def test_roundtrip(self):
        j = Job("t", "a", rerun="every_run")
        assert Job.from_dict(j.to_dict()).rerun == "every_run"


# ══════════════════════════════════════════════════════════════════════
# never（默认）：wall/failed 命中跳过（现状语义）
# ══════════════════════════════════════════════════════════════════════


class TestRerunNever:
    def test_wall_hit_skips(self, tmp_path):
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        p.enqueue([Job("t", "x")])
        p.run()
        assert p.stats["completed"] == 1
        # 再跑一次同 uid（默认 never）→ 加载期清理（wall 命中即拦截）
        p.enqueue([Job("t", "x")])
        p.run()
        assert p.stats["completed"] == 0, "never 策略：wall 命中不得重跑"


# ══════════════════════════════════════════════════════════════════════
# every_run：wall/failed 命中重跑（固定 uid 每会话重扫）
# ══════════════════════════════════════════════════════════════════════


class TestRerunEveryRun:
    def test_wall_hit_reruns_and_counts(self, tmp_path):
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        p.enqueue([Job("t", "scan", rerun="every_run")])
        p.run()
        wall = p.backend.load_wall()
        assert wall["t::scan"]["run_count"] == 1
        assert wall["t::scan"]["last_run_at"]
        assert wall["t::scan"]["last_run_id"]

        # 同 uid 再入队 → 重跑（固定 uid 跨会话重扫），run_count 累计
        p.enqueue([Job("t", "scan", rerun="every_run")])
        p.run()
        wall = p.backend.load_wall()
        assert wall["t::scan"]["run_count"] == 2
        assert p.stats["completed"] == 1, "第二次 run 本次完成 1 个（stats 每 run 重置）"

    def test_failed_hit_reruns(self, tmp_path):
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        p.backend.append_failed("t::x", {"error": "old failure"})
        p.enqueue([Job("t", "x", rerun="every_run")])
        p.run()
        # failed 命中 → every_run 重跑成功 → DLQ 残行被 commit_job_success 清理
        assert "t::x" in p.backend.load_wall()
        assert "t::x" not in p.backend.load_failed()

    def test_loading_repair_keeps_every_run(self, tmp_path):
        """加载期修复：every_run 任务在 wall 中**不是残留**，保留在队列。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        p.seed_wall(["t::scan"])
        p.enqueue([Job("t", "scan", rerun="every_run")])
        p.run()
        wall = p.backend.load_wall()
        assert wall["t::scan"]["run_count"] == 1, "every_run 任务必须重跑（run_count 累计）"

    def test_rerun_failure_clears_wall_all_paths(self, tmp_path):
        """ 回归：rerun 任务重跑失败（max-retries DLQ 路径）→ wall 旧记录作废。

        修复前：只有普通 failure 路径清 wall；max-retries/commit-failure/
        dep-failure/no-handler/payload-validation/级联路径的 mark_failed 不清
        → uid 同时属于 wall 和 failed（DEBUG 断言崩、is_completed/is_failed 同真）。
        """
        p = _pipeline(tmp_path)
        p.register_handler("t", _always_retry_handler)
        # 上次成功（wall 有记录）→ every_run 重跑 → 重试耗尽 DLQ
        p.seed_wall(["t::x"])
        p.enqueue([Job("t", "x", rerun="every_run", max_retries=0)])
        p.run()
        assert "t::x" in p.backend.load_failed(), "重试耗尽应进 DLQ"
        assert "t::x" not in p.backend.load_wall(), "重跑失败后 wall 旧记录必须作废"

    def test_rerun_failure_clears_wall_no_handler_path(self, tmp_path):
        """ 补强：rerun 任务经 **no-handler** 直接 commit 失败 → wall 作废。"""
        p = _pipeline(tmp_path)
        # 不注册 handler → enqueue 的 job 走 no-handler 直接 commit 路径
        p.seed_wall(["nohandler::x"])  # 上次成功（wall 有记录）
        p.enqueue([Job("nohandler", "x", rerun="every_run")])
        p.run()
        assert "nohandler::x" in p.backend.load_failed(), "no-handler 应进 DLQ"
        assert "nohandler::x" not in p.backend.load_wall(), "no-handler 路径 wall 必须作废"

    def test_rerun_failure_clears_wall_payload_validation_path(self, tmp_path):
        """ 补强：rerun 任务经 **payload-validation** 直接 commit 失败 → wall 作废。"""
        from tasklite.taxonomy import validate_payload
        from typing import TypedDict

        class _Schema(TypedDict):
            required_key: int

        def _h(job, ctx):
            return True

        p = _pipeline(tmp_path)
        p.register_handler("t", _h, payload_schema=_Schema)
        p.seed_wall(["t::bad"])  # 上次成功（wall 有记录）
        # payload 缺 required_key → 走 payload-validation 直接 commit 路径
        p.enqueue([Job("t", "bad", payload={"wrong": 1}, rerun="every_run")])
        p.run()
        assert "t::bad" in p.backend.load_failed(), "payload 校验失败应进 DLQ"
        assert "t::bad" not in p.backend.load_wall(), "payload-validation 路径 wall 必须作废"

    def test_rerun_failure_clears_wall_dep_failure_path(self, tmp_path):
        """ 补强：rerun 任务因**依赖失败**级联 DLQ → wall 旧记录作废。"""
        p = _pipeline(tmp_path)

        p.register_handler("parent", _fail_deterministic)
        p.register_handler("child", _ok_handler)
        p.seed_wall(["child::c1"])  # child 上次成功（wall 有记录）
        p.enqueue([
            Job("parent", "p1"),
            Job("child", "c1", depends_on=["parent::p1"], rerun="every_run"),
        ])
        p.run()
        assert "child::c1" in p.backend.load_failed(), "依赖失败应级联 DLQ"
        assert "child::c1" not in p.backend.load_wall(), "dep-failure 路径 wall 必须作废"

    def test_spawn_dedup_exempts_every_run_child(self, tmp_path):
        """spawn 去重豁免：every_run 子任务重 spawn 不被 wall 拦截。"""
        p = _pipeline(tmp_path)
        p.register_handler("parent", _parent_spawns_every_run_child)
        p.register_handler("child", _child_ok)
        # 预置 child 已在 wall（上次已成功）
        p.seed_wall(["child::c1"])
        p.enqueue([Job("parent", "p1")])
        p.run()
        # every_run child 不被 wall 拦截 → 被重 spawn 并执行
        wall = p.backend.load_wall()
        assert wall["child::c1"]["run_count"] == 1, "every_run 子任务必须重跑"


    def test_clear_in_flight_preserves_rerun_exemption(self, tmp_path):
        """ 回归：abort 清空 in-flight 时，queue 中的 rerun 任务保留豁免。

        修复前：clear_in_flight 整体清空 _rerun_active_uids——requeue 后的
        every_run 任务在 wall（历史）+ queue 无豁免 → DEBUG 断言崩。
        """
        from tasklite.models.state import PipelineState

        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        # wall 有历史成功 + queue 有 every_run 任务
        p.seed_wall(["t::scan"])
        queue = [Job("t", "scan", rerun="every_run").to_dict()]
        p._runtime.store.set_state(PipelineState(
            p.backend.load_wall(), p.backend.load_failed(),
            p.backend.load_cursors(), queue,
        ))
        # 模拟 abort：requeue（已含）+ clear_in_flight
        p._runtime.state.clear_in_flight()
        # 断言通过即无 DEBUG 崩溃；豁免集合保留 queue 中的 rerun 任务
        assert "t::scan" in p._runtime.state._rerun_active_uids

    def test_clear_in_flight_removes_plain_tasks(self, tmp_path):
        """非 rerun 任务不被豁免集合保留。"""
        from tasklite.models.state import PipelineState

        p = _pipeline(tmp_path)
        queue = [Job("t", "plain").to_dict()]
        p._runtime.store.set_state(PipelineState(
            p.backend.load_wall(), p.backend.load_failed(),
            p.backend.load_cursors(), queue,
        ))
        p._runtime.state.clear_in_flight()
        assert "t::plain" not in p._runtime.state._rerun_active_uids

    def test_direct_commit_failure_does_not_leak_rerun_exemption(self, tmp_path):
        """ 回归：rerun 任务经 no-handler 直接 commit 失败 → 从豁免集合移除。

        修复前：直接 commit 路径（no-handler/dep-failure/payload-validation）
        pop 后从不 unregister → uid 永久留在 _rerun_active_uids → 该 uid
        后续 wall/failed∩in-flight 的断言被豁免绕过（陷阱弱化）。
        """
        from tasklite.models.state import PipelineState

        p = _pipeline(tmp_path)
        p.register_handler("t", _always_retry_handler)
        # no-handler：注册 handler 后 enqueue，但 task_type 用未注册的
        # "nohandler"——走 no-handler 直接 commit 路径
        p.seed_wall(["nohandler::x"])  # rerun 任务历史成功（wall）
        queue = [Job("nohandler", "x", rerun="every_run").to_dict()]
        p._runtime.store.set_state(PipelineState(
            p.backend.load_wall(), p.backend.load_failed(),
            p.backend.load_cursors(), queue,
        ))
        # 模拟 dispatch 的 pop：rerun 任务加入豁免集合
        p._runtime.state.pop_job(0)
        assert "nohandler::x" in p._runtime.state._rerun_active_uids
        # 直接 commit 路径（no-handler）内部会调 _mark_failed + unregister——
        # 这里手动模拟该路径的终止动作，验证豁免被移除
        from tasklite.pipeline import TaskLite
        from tasklite.taxonomy import ERR_NO_HANDLER
        p.backend.commit_job_failure("nohandler::x", {"error": ERR_NO_HANDLER})
        p._runtime.store.mark_failed("nohandler::x", {"error": ERR_NO_HANDLER})
        p._runtime.state.unregister_in_flight("nohandler::x")
        assert "nohandler::x" not in p._runtime.state._rerun_active_uids, \
            "直接 commit 失败后豁免集合必须移除该 uid"


# ══════════════════════════════════════════════════════════════════════
# on_failure：failed 命中重跑；wall 命中跳过
# ══════════════════════════════════════════════════════════════════════


class TestRerunOnFailure:
    def test_failed_hit_reruns_and_clears_dlq(self, tmp_path):
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        p.backend.append_failed("t::x", {"error": "net blip"})
        p.enqueue([Job("t", "x", rerun="on_failure")])
        p.run()
        assert "t::x" in p.backend.load_wall()
        assert "t::x" not in p.backend.load_failed(), "成功自动清 DLQ 残行"

    def test_wall_hit_skips(self, tmp_path):
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        p.seed_wall(["t::x"])
        p.enqueue([Job("t", "x", rerun="on_failure")])
        p.run()
        assert p.stats["completed"] == 0, "on_failure：wall 命中不得重跑"


# ══════════════════════════════════════════════════════════════════════
# register_discovery 默认 every_run
# ══════════════════════════════════════════════════════════════════════


def _fetch_empty(job, ctx, page):
    return []


def _item_id(item):
    return str(item)


def _process(job, ctx, item, content_id):
    ctx.spawn(Job("child", content_id))


class TestDiscoveryDefaultRerun:
    def test_discovery_job_gets_every_run_injected(self, tmp_path):
        p = _pipeline(tmp_path)
        register_discovery(p, "disc", _fetch_empty, _item_id, _process, "child")
        p.enqueue([Job("disc", "favorites", payload={})])
        jd = p.backend.load_queue()[0]
        assert jd["rerun"] == "every_run", "discovery job 默认注入 every_run"

    def test_explicit_rerun_respected(self, tmp_path):
        p = _pipeline(tmp_path)
        register_discovery(p, "disc", _fetch_empty, _item_id, _process, "child")
        p.enqueue([Job("disc", "favorites", payload={}, rerun="on_failure")])
        jd = p.backend.load_queue()[0]
        assert jd["rerun"] == "on_failure", "显式指定非默认策略必须尊重"

    def test_invalid_rerun_rejected(self, tmp_path):
        p = _pipeline(tmp_path)
        with pytest.raises(ValueError, match="rerun"):
            register_discovery(p, "disc", _fetch_empty, _item_id, _process,
                               "child", rerun="sometimes")


# ══════════════════════════════════════════════════════════════════════
# discovery 动态兜底放行与豁免登记的同源性
# ══════════════════════════════════════════════════════════════════════


class TestDiscoveryDynamicFallbackRerun:
    """策略晚于入队注册时，队列行无字面 rerun 键，派发期动态兜底放行的
    wall/failed 命中重跑必须与豁免登记同源。

    不变式：
    1. 凡准入层放行的重跑，其 uid 必须在 in-flight 登记前进入豁免
       集合——六集合互斥断言不得击落准入层自己放行的作业；
    2. DEBUG 断言是引擎不变式破坏信号，不是作业坏输入——不得计入
       派发失败预算（3-strike 会把可正常执行的作业误送 DLQ）。
    """

    def test_failed_hit_rerun_without_literal_key_executes(self, tmp_path):
        p = _pipeline(tmp_path, name="dyn_fb")
        p.register_handler("t", _fail_then_ok_by_payload)
        p.enqueue([Job("t", "x", payload={"attempt": 1}, max_retries=0)])
        p.run()
        assert "t::x" in p.backend.load_failed()

        # 关键时序：先入队（此刻 discovery 默认未注册 → 队列行无 rerun 键），
        # 后注册策略 → 派发期经动态兜底放行 failed 命中重跑。
        p.enqueue([Job("t", "x", payload={"attempt": 2}, max_retries=0)])
        p.set_discovery_rerun("t", "on_failure")
        p.run()
        wall = p.backend.load_wall()
        assert "t::x" in wall, "动态兜底放行的重跑必须真实执行并转 wall"
        assert "t::x" not in p.backend.load_failed(), "成功后 DLQ 残行清除"
        assert wall["t::x"]["run_count"] == 1

    def test_abort_of_inflight_dynamic_rerun_preserves_exemption(self, tmp_path, monkeypatch):
        """动态兜底重跑在途时 abort：豁免不得被按字面键的重建冲掉。

        不变式：豁免事实随行持久——准入层放行的重跑把有效策略落为行内
        字面键，abort 收尾的 settle→requeue→clear_in_flight 按字面键重建
        豁免后，六集合互斥断言仍不得击落重入队的重跑作业；作业保持队列
        （at-least-once），run() 以正常停机收尾而非 AssertionError。
        """
        import threading
        import time

        from tests.helpers import make_ipc_process_class, patch_multiprocessing_for_fakes

        p = _pipeline(tmp_path, name="dyn_abort")
        p.register_handler("t", _fail_then_ok_by_payload)
        p.enqueue([Job("t", "x", payload={"attempt": 1}, max_retries=0)])
        p.run()
        assert "t::x" in p.backend.load_failed()

        # 关键时序：先入队（discovery 默认未注册 → 队列行无字面 rerun 键），
        # 后注册策略 → 派发期动态兜底放行 failed 命中重跑并派发在途。
        p.enqueue([Job("t", "x", payload={"attempt": 2}, max_retries=0)])
        p.set_discovery_rerun("t", "on_failure")

        patch_multiprocessing_for_fakes(
            monkeypatch,
            fake_process_class=make_ipc_process_class(results=[None], stay_alive=True),
        )

        def controller():
            for _ in range(500):
                if p._runtime.in_flight:
                    break
                time.sleep(0.01)
            p.stop(force=True)

        t = threading.Thread(target=controller)
        t.start()
        p.run()
        t.join()

        assert "t::x" not in p.backend.load_wall(), "被 abort 的重跑不得误提交 wall"
        assert "t::x" in p._runtime.state._rerun_active_uids, \
            "abort 收尾后重入队的动态重跑必须保留豁免登记"
        remaining = [j for j in p.backend.load_queue() if j.get("job_id") == "x"]
        assert remaining, "被 abort 的重跑作业必须保留在磁盘队列（at-least-once）"
        assert remaining[0].get("rerun") == "on_failure", \
            "动态放行的有效策略必须落为行内字面键，豁免重建才不丢失"

    def test_loaded_dynamic_rerun_row_exempt_before_any_dispatch(self, tmp_path):
        """加载期滞留的动态重跑行：任何 in-flight 登记前必须已有豁免。

        策略晚于入队注册时，repair 保留的 failed 命中无字面键行若不携带
        豁免，先派发的任意其他作业在 in-flight 登记处即触发互斥断言。
        """
        p = _pipeline(tmp_path, name="dyn_load")
        p.register_handler("t", _fail_then_ok_by_payload)
        p.enqueue([Job("t", "x", payload={"attempt": 1}, max_retries=0)])
        p.run()
        assert "t::x" in p.backend.load_failed()

        # y 先入队（先派发）；x 的队列行无字面 rerun 键且命中 failed。
        p.enqueue([Job("t", "y", payload={"attempt": 2}, max_retries=0)])
        p.enqueue([Job("t", "x", payload={"attempt": 2}, max_retries=0)])
        p.set_discovery_rerun("t", "on_failure")
        p.run()
        wall = p.backend.load_wall()
        assert "t::y" in wall
        assert "t::x" in wall, "动态兜底放行的重跑必须真实执行并转 wall"
        assert "t::x" not in p.backend.load_failed(), "成功后 DLQ 残行清除"

    def test_invariant_assertion_bypasses_dispatch_failure_budget(self, tmp_path, monkeypatch):
        """不变式断言穿透派发异常链：不记账、不误送 DLQ，作业保持队列。"""
        import tasklite.engine.dispatch as dispatch_mod

        p = _pipeline(tmp_path, name="assert_gate")
        p.register_handler("t", _ok_handler)
        p.enqueue([Job("t", "x")])

        def _boom(*args, **kwargs):
            raise AssertionError("simulated invariant violation")

        monkeypatch.setattr(dispatch_mod, "TaskContext", _boom)
        with pytest.raises(AssertionError):
            p.run()
        jd = p.backend.load_queue()[0]
        rt = jd.get("runtime") or {}
        assert not rt.get("_dispatch_failures"), "不变式断言不得计入派发失败预算"


# ══════════════════════════════════════════════════════════════════════
# 防御：queue/in-flight 命中永远拦截（同轮不重复派发）
# ══════════════════════════════════════════════════════════════════════


class TestRerunQueueStillBlocks:
    def test_every_run_not_double_dispatched_same_run(self, tmp_path):
        """every_run 不改变同一轮内的去重：queue/in-flight 永远算数。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        # 同 uid 入队两次（same enqueue batch）→ 队列去重吸收
        p.enqueue([Job("t", "x", rerun="every_run"),
                   Job("t", "x", rerun="every_run")])
        p.run()
        assert p.stats["completed"] == 1, "同轮重复入队仍应去重"

class TestRerunInjectionSentinel:
    """rerun=None 哨兵语义——未指定注入 discovery 默认，
    显式指定的 rerun 策略严格保留：
    「显式 never」，覆盖时 enqueue 打 warning 而 spawn 静默，警告不对称；
    告警机制随哨兵语义退役）。"""

    def test_unspecified_injected_every_enqueue(self, tmp_path):
        """未指定（None）→ 每次 enqueue 都注入 discovery 默认。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        from tasklite.wrappers.discovery import register_discovery
        register_discovery(p, "disc", _fetch_pages, _page_ids, _process_items, "t")

        p.enqueue([Job("disc", "s1")])
        p.enqueue([Job("disc", "s2")])
        q = p.backend.load_queue()
        assert all(jd.get("rerun") == "every_run" for jd in q), \
            f"未指定 job 必须每次注入，实际: {[jd.get('rerun') for jd in q]}"

    def test_explicit_never_respected_no_override(self, tmp_path, caplog):
        """显式 rerun="never" → 尊重用户选择：不注入、不告警、不改写。"""
        import logging
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        from tasklite.wrappers.discovery import register_discovery
        register_discovery(p, "disc", _fetch_pages, _page_ids, _process_items, "t")

        with caplog.at_level(logging.WARNING, logger="tasklite"):
            p.enqueue([Job("disc", "keep", rerun="never")])
            overrides = [r.message for r in caplog.records if "overriding" in r.message]
            assert not overrides, f"显式 never 不得再被覆盖告警: {overrides}"
        q = p.backend.load_queue()
        assert q[0].get("rerun") == "never", "显式 never 必须原样保留"


def _fetch_pages(job, ctx, page):
    return []  # 空页即停，不需要真实数据


def _page_ids(item):
    return str(item)


def _process_items(job, ctx, item, content_id):
    ctx.spawn(Job("t", content_id))


class TestInputChangedDefense:
    """on_input_change 指纹比对的损坏数据防御。

    ``input_changed`` 在 wall 元数据损坏（inputs 含非 dict 元素）时必须
    fail-safe（视为变化 → 重跑），而非 AttributeError 崩掉 run。
    """

    def _import_input_changed(self):
        from tasklite.engine.policy import PreflightPolicy
        return PreflightPolicy().check_input_changed

    def test_non_dict_element_returns_true_not_crash(self):
        input_changed = self._import_input_changed()
        # 损坏指纹：inputs 是 list 但元素非 dict——修复前 entry.get 会 AttributeError
        meta = {"inputs": [42, "boom", {"kind": "uri", "path": "/x"}]}
        assert input_changed(meta) is True, "含非 dict 元素的损坏指纹应视为变化（不崩 run）"

    def test_all_dict_fingerprint_unchanged_returns_false(self, tmp_path):
        input_changed = self._import_input_changed()
        f = tmp_path / "in.txt"
        f.write_text("hello")
        st = f.stat()
        meta = {"inputs": [{"path": str(f), "size": st.st_size, "mtime_ns": st.st_mtime_ns}]}
        assert input_changed(meta) is False, "指纹不变时应返回 False（拦截重跑）"

    def test_non_str_path_returns_true_not_crash(self):
        """dict 元素内 path 为真值但非 str 时，
        os.stat(path) 抛 TypeError（不被 except OSError 捕获）→ 崩 run。
        应视为损坏 → 返回 True（变化重跑，fail-safe）。"""
        input_changed = self._import_input_changed()
        meta = {"inputs": [{"path": ["not", "a", "str"], "size": 1, "mtime_ns": 2}]}
        assert input_changed(meta) is True, "非 str path 应视为损坏（不崩 run）"


# ══════════════════════════════════════════════════════════════════════
# 防御：wall meta 脏 run_count 不崩成功提交路径
# ══════════════════════════════════════════════════════════════════════


class TestCorruptRunCountDefense:
    def test_dirty_run_count_degrades_to_zero_not_crash(self, tmp_path):
        """脏 run_count（外部改库/格式漂移）→ 降级 0 重计，不抛异常。

        裸 int 在成功提交路径抛 TypeError/ValueError 时，该 job 每次成功
        都在同一行崩掉 → 反复 requeue 的崩溃循环。修复后坏值降级 0 + warning。
        """
        p = _pipeline(tmp_path, name="dirty_count")
        p.register_handler("t", _ok_handler)
 # 预置脏 wall meta（与生产同路径：commit_job_success 写 meta）
        p.backend.commit_job_success(
            "t::scan", {"run_count": "corrupt"}, spawned_jobs=[], cursor_updates={})
        p.enqueue([Job("t", "scan", rerun="every_run")])
        p.run()  # 修复前此处抛 TypeError → 崩溃循环
        wall = p.backend.load_wall()
        assert wall["t::scan"]["run_count"] == 1, "脏值视为 0 后 +1"
        assert p.stats["completed"] == 1

    def test_dirty_run_count_variants(self, tmp_path):
        """None/list/dict 等变体同样降级；合法值链路不受影响。"""
        for i, dirty in enumerate([None, [1, 2], {"x": 1}]):
            p = _pipeline(tmp_path, name=f"variant{i}")
            p.register_handler("t", _ok_handler)
            p.backend.commit_job_success(
                "t::x", {"run_count": dirty}, spawned_jobs=[], cursor_updates={})
            p.enqueue([Job("t", "x", rerun="every_run")])
            p.run()
            assert p.backend.load_wall()["t::x"]["run_count"] == 1

        p2 = _pipeline(tmp_path, name="legal")
        p2.register_handler("t", _ok_handler)
        p2.backend.commit_job_success(
            "t::y", {"run_count": 5}, spawned_jobs=[], cursor_updates={})
        p2.enqueue([Job("t", "y", rerun="every_run")])
        p2.run()
        assert p2.backend.load_wall()["t::y"]["run_count"] == 6, "合法值正常累计"
