"""显式依赖图 + 环检测 + 级联阻断回归测试。

覆盖：
- find_dependency_cycles：精确找出环内成员（不误伤环外）
- fail_cascade：父失败 → O(1) 级联标记全部下游
- 环分支只失败环内成员，环外 job 保留
- 退避不遮蔽 unknown/impossible 资源死锁
"""

import logging
import pytest
import time

from tasklite.models.job import Job
from tasklite.models.state import PipelineState
from tasklite.engine.scheduler import JobScheduler
from tasklite.engine.resource import CapacityResource
from tasklite.pipeline import TaskLite


def _ok_handler(job, ctx):
    return True


def _parent_spawn_child_handler(job, ctx):
    """parent 成功时 spawn 一个依赖已失败父 a 的 child。"""
    ctx.spawn(Job("t", "child", depends_on=["t::a"]))
    return True


class TestFindDependencyCycles:
    def test_simple_cycle(self):
        """A→B→A 环：两个成员都被找出。"""
        st = PipelineState({}, {}, {}, [
            Job("t", "a", depends_on=["t::b"]).to_dict(),
            Job("t", "b", depends_on=["t::a"]).to_dict(),
        ])
        cycles = set(st.find_dependency_cycles())
        assert cycles == {"t::a", "t::b"}

    def test_cycle_with_outside_job(self):
        """环 A→B→A + 无关 job C：只找出环成员，C 不在环中。"""
        st = PipelineState({}, {}, {}, [
            Job("t", "a", depends_on=["t::b"]).to_dict(),
            Job("t", "b", depends_on=["t::a"]).to_dict(),
            Job("t", "c").to_dict(),  # 无关
        ])
        cycles = set(st.find_dependency_cycles())
        assert cycles == {"t::a", "t::b"}
        assert "t::c" not in cycles

    def test_longer_cycle(self):
        """A→B→C→A 三节点环。"""
        st = PipelineState({}, {}, {}, [
            Job("t", "a", depends_on=["t::b"]).to_dict(),
            Job("t", "b", depends_on=["t::c"]).to_dict(),
            Job("t", "c", depends_on=["t::a"]).to_dict(),
        ])
        cycles = set(st.find_dependency_cycles())
        assert cycles == {"t::a", "t::b", "t::c"}

    def test_no_cycle(self):
        """A→B→C（链）无环。"""
        st = PipelineState({}, {}, {}, [
            Job("t", "a", depends_on=["t::b"]).to_dict(),
            Job("t", "b", depends_on=["t::c"]).to_dict(),
            Job("t", "c").to_dict(),
        ])
        assert st.find_dependency_cycles() == []

    def test_diamond_no_cycle(self):
        """菱形 D→[B,C]，B→A，C→A：无环。"""
        st = PipelineState({}, {}, {}, [
            Job("t", "d", depends_on=["t::b", "t::c"]).to_dict(),
            Job("t", "b", depends_on=["t::a"]).to_dict(),
            Job("t", "c", depends_on=["t::a"]).to_dict(),
            Job("t", "a").to_dict(),
        ])
        assert st.find_dependency_cycles() == []


class TestFailCascade:
    def test_cascade_direct_and_indirect(self):
        """父失败 → 直接 + 间接下游全部级联。"""
        st = PipelineState({}, {}, {}, [
            Job("t", "a").to_dict(),
            Job("t", "b", depends_on=["t::a"]).to_dict(),      # 直接依赖 a
            Job("t", "c", depends_on=["t::b"]).to_dict(),      # 间接依赖 a
            Job("t", "d").to_dict(),                            # 无关
        ])
        cascaded = set(st.fail_cascade("t::a"))
        assert cascaded == {"t::b", "t::c"}
        assert "t::d" not in cascaded

    def test_cascade_skips_wall(self):
        """已完成的 job 不重复级联。"""
        st = PipelineState({"t::b": {}}, {}, {}, [
            Job("t", "a").to_dict(),
            Job("t", "b", depends_on=["t::a"]).to_dict(),  # b 已在 wall
        ])
        assert st.fail_cascade("t::a") == []


class TestCycleDeadlockOnlyCycleMembers:
    def test_cycle_failure_leaves_outside_jobs(self, tmp_path):
        """ 修复：环分支只失败环内成员，环外 job 保留并可运行。"""
        pipeline = TaskLite(name="t", state_dir=tmp_path / "state", backend="sqlite")
        pipeline.register_handler("t", _ok_handler)
        pipeline.enqueue([
            Job("t", "a", depends_on=["t::b"]),
            Job("t", "b", depends_on=["t::a"]),   # 环
            Job("t", "c"),                        # 环外，可运行
        ])
        pipeline.run()

        failed = pipeline.backend.load_failed()
        wall = pipeline.backend.load_wall()
        # 环成员 a、b 进 DLQ（DEPENDENCY_DEADLOCK）
        assert "t::a" in failed and "t::b" in failed
        # 环外 c 正常运行进 wall
        assert "t::c" in wall
        assert "t::c" not in failed


class TestBackoffDoesNotShadowDeadlock:
    def test_backoff_job_with_unknown_resource_still_detected(self):
        """ 修复：退避中的 job 引用未知资源 → 仍被归因为 unknown 死锁。

        退避与未知资源判定顺序：
        被无限推迟到退避结束。新实现资源检查先于退避，退避不遮蔽归因。
        """
        sched = JobScheduler({})  # 空资源表
        jd = Job("t", "x", payload={}, resources={"ghost": 1.0}).to_dict()
        # 退避状态在 runtime 子 dict；用大数退避（超过 now）
        jd["runtime"] = {"_backoff_until": 10**18}
        result = sched.pop_next_runnable(PipelineState({}, {}, {}, [jd]))
        assert 0 in result.unknown_resource_indices, \
            "退避中的 job 引用未知资源必须立即归因"
        assert result.min_wait == float('inf'), "unknown 资源 → min_wait=inf 触发死锁"

    def test_backoff_job_with_impossible_resource_still_detected(self):
        """退避中的 job 请求超容量资源 → 仍归因为 impossible。"""
        cap = CapacityResource("r", 1.0)
        sched = JobScheduler({"r": cap})
        jd = Job("t", "x", payload={}, resources={"r": 99.0}).to_dict()
        jd["runtime"] = {"_backoff_until": 10**18}  # 退避中
        result = sched.pop_next_runnable(PipelineState({}, {}, {}, [jd]))
        assert 0 in result.impossible_resource_indices
        assert result.min_wait == float('inf')

    def test_cycle_with_backoff_pipeline_detects_deadlock(self, tmp_path, caplog):
        """ 回归：依赖环 + 队列中另一 job 有限退避 → 主循环必须在
        elif 分支（min_wait 有限 + 等待依赖）直接检测环死锁，不被退避遮蔽。

        旧测试只构造「纯依赖环 + 环内成员退避」：环成员的依赖未满足时
        调度器在依赖检查处 ``continue``，永远到不了退避换算 → min_wait
        恒为 inf，主循环走的是 ``if sched.min_wait == inf`` 死锁分支，
        elif 分支（min_wait 有限 + waiting_for_dependency → 环检测）从未
        执行——删除修复代码测试仍绿（false positive）。

        新测试加入一个**无依赖、处于有限退避**的 job C：C 的退避使
        min_wait 变为有限，同时 A↔B 环使 waiting_for_dependency=True，
        且无 in-flight → 必须命中 elif 分支的环检测。
        """
        with caplog.at_level(logging.ERROR, logger="tasklite"):
            pipeline = TaskLite(name="t", state_dir=str(tmp_path / "s"), backend="sqlite")
            pipeline.register_handler("t", _ok_handler)

            # A→B→A 依赖环；C 无依赖但处于有限退避 → min_wait 有限
            a = Job("t", "a", depends_on=["t::b"]).to_dict()
            b = Job("t", "b", depends_on=["t::a"]).to_dict()
            c = Job("t", "c").to_dict()
            c["runtime"] = {"_backoff_until": time.monotonic() + 0.5}

            # 直接构造 PipelineState + 主循环（绕过 enqueue 便于注入退避字段）
            pipeline._state = PipelineState({}, {}, {}, [a, b, c])
            pipeline._run_loop()

            failed = pipeline.backend.load_failed()
            wall = pipeline.backend.load_wall()
            assert "t::a" in failed, "环成员 A 必须进 DLQ（退避不得遮蔽环死锁）"
            assert "t::b" in failed, "环成员 B 必须进 DLQ"
            assert "DEPENDENCY_DEADLOCK" in failed["t::a"]["error"]
            # 非环成员 C 未被误杀：退避结束后正常运行进 wall
            assert "t::c" in wall, f"非环成员 C 应正常运行进 wall, failed={failed}"

        # 必须命中 elif 分支：该分支独有日志「退避遮蔽环死锁」是修复生效的信号。
        # 若删除 elif 分支的环检测（或 find_dependency_cycles 失效），死锁会
        # 被推迟到 C 退避结束走 inf 分支——本断言失败，测试暴露 false positive。
        assert any(
            "masked by finite min_wait" in r.message for r in caplog.records
        ), "elif 分支（min_wait 有限 + 等待依赖 → 环检测）未执行，测试未覆盖修复分支"


class TestDependentsOnDemand:
    """级联按需计算回归测试（删除 eager dependents 索引）。

    dependents 反向依赖索引改为 fail_cascade 时按需从 queue 构建——
    不再有 pop/spawn/mark_success/replace 四条变更路径的增量同步，
    索引过期导致「级联漏标」的整类 bug 从根上消除。
    """

    def test_fail_cascade_covers_spawned_jobs(self):
        """spawn 的新 job（依赖已失败父）必须被 fail_cascade 标记。"""
        st = PipelineState({}, {}, {}, [
            Job("t", "a").to_dict(),
            Job("t", "b", depends_on=["t::a"]).to_dict(),
        ])
        # 模拟 A 成功并 spawn C（C 依赖 A）
        st.spawn_jobs([Job("t", "c", depends_on=["t::a"]).to_dict()], front=True)
        # 再 spawn D（依赖 A）
        st.spawn_jobs([Job("t", "d", depends_on=["t::a"]).to_dict()], front=True)
        cascaded = set(st.fail_cascade("t::a"))
        assert {"t::b", "t::c", "t::d"} <= cascaded, \
            f"spawn 后的依赖 job 必须被级联，漏标: {cascaded}"

    def test_fail_cascade_skips_failed(self):
        """已失败的 job 不重复级联（fail_cascade 排除 failed 集）。"""
        st = PipelineState({}, {"t::c": {"error": "x"}}, {}, [
            Job("t", "a").to_dict(),
            Job("t", "b", depends_on=["t::a"]).to_dict(),
            Job("t", "c", depends_on=["t::a"]).to_dict(),  # 已在 failed
        ])
        cascaded = set(st.fail_cascade("t::a"))
        assert "t::b" in cascaded
        assert "t::c" not in cascaded, "已失败的 job 不应被重复级联"

    def test_pop_job_excludes_dispatching_job_from_cascade(self):
        """pop（派发）后 job 不再作为依赖者被级联——已派发的 job 的
        依赖要么已满足、要么由 in-flight 豁免，父失败不再撤回它。"""
        st = PipelineState({}, {}, {}, [
            Job("t", "a").to_dict(),
            Job("t", "b", depends_on=["t::a"]).to_dict(),
        ])
        popped = st.pop_job(1)
        assert Job.from_dict(popped).uid == "t::b"
        # 按需计算：b 已出队，不再出现在 a 的级联结果中
        assert st.fail_cascade("t::a") == [], \
            "pop 后 job 已派发（不在队列），不应被级联"

    def test_mark_success_excludes_successful_job_from_cascade(self):
        """mark_success 后 job 已成功，不再被后续级联标记。"""
        st = PipelineState({}, {}, {}, [
            Job("t", "a").to_dict(),
            Job("t", "b", depends_on=["t::a"]).to_dict(),
        ])
        st.mark_success("t::b", {})  # b 先成功
        assert st.fail_cascade("t::a") == [], \
            "成功 job 的依赖者应被 wall 排除，不再触发级联"

    def test_spawned_cascade_end_to_end(self, tmp_path):
        """端到端：handler spawn 的依赖 job 在父失败后仍被级联。

        依赖已有回归测试覆盖，此处验证真实 pipeline 中 spawn 后的级联
        不依赖逐轮扫描兜底。
        """
        pipeline = TaskLite(name="t", state_dir=tmp_path / "state", backend="sqlite")
        pipeline.register_handler("t", _parent_spawn_child_handler)
        # a 失败；spawn parent（成功时 spawn child，child 依赖 a）
        pipeline.backend.commit_job_failure("t::a", {"error": "injected"})
        pipeline.enqueue([Job("t", "parent")])
        pipeline.run()

        failed = pipeline.backend.load_failed()
        wall = pipeline.backend.load_wall()
        assert "t::a" in failed
        assert "t::parent" in wall
        assert "t::child" in failed, \
            "spawn 后依赖已失败父的 child 必须被级联标记"
