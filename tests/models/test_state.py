"""v2 PipelineState 契约测试：六集合互斥、队首/队尾插队语义与派生索引。

移植 v1 互斥、队列、深图环检测与路径口径测试并按 v2 API 改写
（fail_cascade 统一为 cascade_fail）；互斥断言在 ``__debug__`` 下随
受控方法即时触发。环检测返回口径不变式：每次回边命中返回「当前 DFS
路径自闭点起的切片 + 闭点重复」按遍历序拼接（成员、顺序、重复闭点
是死锁归因的精确契约）。
"""

from __future__ import annotations

import pytest

from tasklite.models.job import Job
from tasklite.models.state import PipelineState, uid_from_job_dict


def _jid(job_id: str, depends_on: list[str] | None = None, rerun: str | None = None) -> dict:
    """队列用 job dict（job_id 恒为确定性字面量）。"""
    d = {"task_type": "t", "job_id": job_id}
    if depends_on is not None:
        d["depends_on"] = depends_on
    if rerun is not None:
        d["rerun"] = rerun
    return d


class TestQueueOrdering:
    """队首/队尾插队语义与 uid 索引同步。"""

    def test_spawn_front_preserves_order(self):
        """spawn 默认队首插队且保序（子任务优先于存量队列）。"""
        st = PipelineState({}, {}, {}, [_jid("old")])
        st.spawn_jobs([_jid("a"), _jid("b")])
        uids = [uid_from_job_dict(j) for j in st.queue]
        assert uids == ["t::a", "t::b", "t::old"]

    def test_spawn_back_appends(self):
        st = PipelineState({}, {}, {}, [_jid("old")])
        st.spawn_jobs([_jid("a")], front=False)
        uids = [uid_from_job_dict(j) for j in st.queue]
        assert uids == ["t::old", "t::a"]

    def test_requeue_front_is_spawn_front_semantics(self):
        """重入队（崩溃恢复/重试）语义同队首插入：重试作业回到队首。"""
        st = PipelineState({}, {}, {}, [_jid("next"), _jid("other")])
        popped = st.pop_job(0)  # 派发等价物：出队
        st.requeue_jobs([popped])  # 崩溃恢复等价物：队首重入队
        uids = [uid_from_job_dict(j) for j in st.queue]
        assert uids == ["t::next", "t::other"]

    def test_pop_job_syncs_queue_uids(self):
        st = PipelineState({}, {}, {}, [_jid("a"), _jid("b")])
        assert st.queue_uids == {"t::a", "t::b"}
        st.pop_job(0)
        assert st.queue_uids == {"t::b"}
        assert not st.is_empty
        st.pop_job(0)
        assert st.is_empty

    def test_replace_queue_rebuilds_indexes(self):
        """整体替换队列（死锁批量移除肇事者）重建 uid 索引。"""
        st = PipelineState({}, {}, {}, [_jid("a"), _jid("b")])
        st.replace_queue([_jid("c")])
        assert st.queue_uids == {"t::c"}
        st.queue = [_jid("d")]
        assert st.queue_uids == {"t::d"}


class TestSixSetMutex:
    """六集合互斥：queue / in-flight / wall / failed 与豁免集。"""

    def test_is_known_covers_all_four_sources(self):
        """is_known 涵盖 wall/failed/queue/in-flight 四者（统一事实源）。"""
        st = PipelineState({"w1": {}}, {"f1": {}}, {}, [_jid("q1")])
        st.register_in_flight("i1")
        assert st.is_known("w1")
        assert st.is_known("f1")
        assert st.is_known("t::q1")
        assert st.is_known("i1")
        assert not st.is_known("ghost")

    def test_dispatch_flow_keeps_queue_and_in_flight_mutually_exclusive(self):
        """派发即出队：pop → 登记 in-flight，二者互斥。"""
        st = PipelineState({}, {}, {}, [_jid("x")])
        st.pop_job(0)
        st.register_in_flight("t::x")
        assert "t::x" not in st._queue_uids
        assert "t::x" in st.in_flight_uids

    def test_queue_in_flight_overlap_triggers_assertion(self):
        """uid 同时落队列与 in-flight（跳过 pop 的违例协议）：断言 fail-loud。"""
        st = PipelineState({}, {}, {}, [_jid("x")])
        with pytest.raises(AssertionError, match="queue and in_flight"):
            st.register_in_flight("t::x")

    def test_terminal_uid_in_flight_triggers_assertion(self):
        """非豁免 uid 同时落 wall 与 in-flight：断言 fail-loud。"""
        st = PipelineState({}, {}, {}, [])
        st.mark_success("t::x", {"score": 1})
        with pytest.raises(AssertionError, match="wall/failed and in_flight"):
            st.register_in_flight("t::x")

    def test_rerun_exempt_uid_bypasses_terminal_mutex(self):
        """豁免策略作业可合法落在 wall/failed ∩ in-flight 交集（不触发断言）。"""
        st = PipelineState({}, {}, {}, [_jid("x", rerun="every_run")])
        st.mark_success("t::x", {"score": 1})
        st.pop_job(0)  # pop_job 登记字面豁免
        st.register_in_flight("t::x")  # 不抛
        assert "t::x" in st.rerun_active_uids
        st.unregister_in_flight("t::x")
        assert "t::x" not in st.rerun_active_uids

    def test_mark_rerun_active_registers_admission_granted_uids(self):
        """准入层放行的重跑（字面 rerun 为 None、默认策略生效）由此登记。"""
        st = PipelineState({}, {}, {}, [_jid("x")])
        st.mark_success("t::x", {})
        st.pop_job(0)  # 字面 None：不落字面豁免集
        st.mark_rerun_active("t::x")  # 准入登记补齐豁免
        st.register_in_flight("t::x")  # 不抛

    def test_unregister_and_clear_in_flight(self):
        st = PipelineState({}, {}, {}, [])
        st.register_in_flight("a")
        st.register_in_flight("b")
        assert st.in_flight_uids == frozenset({"a", "b"})
        st.unregister_in_flight("a")
        assert st.in_flight_uids == frozenset({"b"})
        st.clear_in_flight()
        assert st.in_flight_uids == frozenset()

    def test_clear_in_flight_rebuilds_rerun_actives_from_queue(self):
        """清空后按 queue 事实源重建字面豁免集。"""
        st = PipelineState({}, {}, {}, [_jid("r", rerun="on_failure")])
        st.pop_job(0)
        st.mark_rerun_active("t::r")
        st.register_in_flight("t::r")
        st.clear_in_flight()
        # t::r 已不在 queue：重建后豁免集为空
        assert "t::r" not in st.rerun_active_uids

    def test_wall_failed_mutex_through_inverse_transitions(self):
        """成功/失败互逆转移：任一终态写入都把 uid 从对侧集合精确移出。"""
        st = PipelineState({}, {}, {}, [])
        st.mark_failed("t::x", {"error": "boom"})
        assert st.failed_uids == {"t::x"}
        assert st.wall_uids == set()
        st.mark_success("t::x", {"score": 1})
        assert st.wall_uids == {"t::x"}
        assert st.failed_uids == set()
        st.mark_failed("t::x", {"error": "again"})
        assert st.failed_uids == {"t::x"}
        assert st.wall_uids == set()


class TestUidFromJobDict:
    """uid 提取与损坏容灾。"""

    def test_direct_extraction(self):
        assert uid_from_job_dict(_jid("j")) == "t::j"

    def test_partial_dict_reconstructs_via_job(self):
        d = {"task_type": "t", "job_id": "j", "payload": {"k": 1}}
        assert uid_from_job_dict(d) == "t::j"

    def test_full_job_dict_compatible(self):
        """v2 Job.to_dict 产物（含实例位字段）同样直接提取。"""
        assert uid_from_job_dict(Job("t", "j", attempt_no=2).to_dict()) == "t::j"

    def test_corrupt_dict_falls_back_to_content_hash(self):
        """task_type/job_id 均缺失且无法重建：按内容哈希兜底（恒定、可去重）。"""
        uid_a = uid_from_job_dict({"payload": {"k": 1}})
        assert uid_a.startswith("_unknown::")
        assert uid_a == uid_from_job_dict({"payload": {"k": 1}})
        assert uid_a != uid_from_job_dict({"payload": {"k": 2}})


class TestTerminalRecords:
    """终态记录与游标合并。"""

    def test_mark_success_deep_copies_meta(self):
        st = PipelineState({}, {}, {}, [])
        meta = {"nested": {"count": 1}}
        st.mark_success("t::x", meta)
        meta["nested"]["count"] = 99
        assert st.wall["t::x"]["nested"]["count"] == 1

    def test_update_cursors_merge_and_delete(self):
        st = PipelineState({}, {}, {"a": "1"}, [])
        st.update_cursors({"a": "2", "b": "3"})
        assert st.cursors == {"a": "2", "b": "3"}
        st.update_cursors({"a": None})
        assert st.cursors == {"b": "3"}

    def test_ops_seams_idempotent(self):
        st = PipelineState({}, {}, {}, [])
        st.add_wall("t::w", {})
        st.add_wall("t::w", {"again": True})
        assert st.wall["t::w"] == {"again": True}
        st.discard_wall("t::w")
        st.discard_wall("t::w")  # 幂等
        assert "t::w" not in st.wall_uids
        st.mark_failed("t::f", {})
        st.discard_failed("t::f")
        st.discard_failed("t::f")  # 幂等
        assert "t::f" not in st.failed_uids
        st.set_cursor("k", "v")
        assert st.cursors == {"k": "v"}


class TestFindDependencyCycles:
    """依赖环检测：精确找出环内成员（不误伤环外）。"""

    def test_simple_cycle(self):
        st = PipelineState({}, {}, {}, [
            _jid("a", ["t::b"]),
            _jid("b", ["t::a"]),
        ])
        assert set(st.find_dependency_cycles()) == {"t::a", "t::b"}

    def test_cycle_with_outside_job(self):
        st = PipelineState({}, {}, {}, [
            _jid("a", ["t::b"]),
            _jid("b", ["t::a"]),
            _jid("c"),
        ])
        cycles = set(st.find_dependency_cycles())
        assert cycles == {"t::a", "t::b"}
        assert "t::c" not in cycles

    def test_longer_cycle(self):
        st = PipelineState({}, {}, {}, [
            _jid("a", ["t::b"]),
            _jid("b", ["t::c"]),
            _jid("c", ["t::a"]),
        ])
        assert set(st.find_dependency_cycles()) == {"t::a", "t::b", "t::c"}

    def test_no_cycle_chain(self):
        st = PipelineState({}, {}, {}, [
            _jid("a", ["t::b"]),
            _jid("b", ["t::c"]),
            _jid("c"),
        ])
        assert st.find_dependency_cycles() == []

    def test_diamond_no_cycle(self):
        st = PipelineState({}, {}, {}, [
            _jid("d", ["t::b", "t::c"]),
            _jid("b", ["t::a"]),
            _jid("c", ["t::a"]),
            _jid("a"),
        ])
        assert st.find_dependency_cycles() == []


class TestDeepGraphCycleDetection:
    """深图环检测：线性迭代实现不受解释器递归上限约束（governor 每轮
    仲裁对全队列调用，环检测崩溃即整轮 run 崩溃）。"""

    @staticmethod
    def _linear_chain(n: int) -> list[dict]:
        """n 节点线性链：j_i 依赖 j_next，链尾无依赖。"""
        return [
            {"task_type": "t", "job_id": f"j{i}", "depends_on": [f"t::j{i + 1}"]}
            if i + 1 < n else {"task_type": "t", "job_id": f"j{i}"}
            for i in range(n)
        ]

    def test_deep_linear_chain_completes_without_recursion_error(self):
        """深度超解释器递归限制的线性链：无环返回空且检测本身不崩。"""
        st = PipelineState({}, {}, {}, self._linear_chain(5000))
        assert st.find_dependency_cycles() == []

    def test_deep_single_cycle_full_closure_exact_order(self):
        """5000 节点单环：路径切片 + 闭点重复构成完整闭环，顺序确定。"""
        n = 5000
        queue = [
            {"task_type": "t", "job_id": f"k{i}", "depends_on": [f"t::k{(i + 1) % n}"]}
            for i in range(n)
        ]
        st = PipelineState({}, {}, {}, queue)
        assert st.find_dependency_cycles() == [f"t::k{i}" for i in range(n)] + ["t::k0"]

    def test_deep_chain_with_cycle_exact_cycle_members(self):
        """深链（2500 节点）尾部挂接双节点环：仅环成员入结果且路径精确。"""
        queue = self._linear_chain(2500)
        queue[-1]["depends_on"] = ["t::c_head"]
        queue += [
            {"task_type": "t", "job_id": "c_head", "depends_on": ["t::c_tail"]},
            {"task_type": "t", "job_id": "c_tail", "depends_on": ["t::c_head"]},
        ]
        st = PipelineState({}, {}, {}, queue)
        assert st.find_dependency_cycles() == ["t::c_head", "t::c_tail", "t::c_head"]


class TestCyclePathSemantics:
    """环路径口径：链不入结果、自环、共享头双环与缺失依赖。"""

    def test_chain_leading_into_cycle_exact_path(self):
        """链 x→y 挂接环 z→w→z：仅闭环节点入结果，链上节点不入。"""
        st = PipelineState({}, {}, {}, [
            _jid("x", ["t::y"]),
            _jid("y", ["t::z"]),
            _jid("z", ["t::w"]),
            _jid("w", ["t::z"]),
        ])
        assert st.find_dependency_cycles() == ["t::z", "t::w", "t::z"]

    def test_self_loop_alongside_acyclic_chain(self):
        """自环与无环链共存：各自独立判定，链不入结果。"""
        st = PipelineState({}, {}, {}, [
            _jid("a", ["t::b"]),
            _jid("b", ["t::c"]),
            _jid("d", ["t::d"]),
        ])
        assert st.find_dependency_cycles() == ["t::d", "t::d"]

    def test_multiple_back_edges_from_shared_head(self):
        """共享头部的双环（a→{b,c}，b→a，c→a）：两道回边各闭一次。

        a 的依赖集为 set，遍历序随进程哈希而异，故按结构断言：
        两段各为 [a, 叶, a]，仅叶节点次序不定。
        """
        st = PipelineState({}, {}, {}, [
            _jid("a", ["t::b", "t::c"]),
            _jid("b", ["t::a"]),
            _jid("c", ["t::a"]),
        ])
        result = st.find_dependency_cycles()
        assert len(result) == 6
        assert result[0] == result[2] == result[3] == result[5] == "t::a"
        assert {result[1], result[4]} == {"t::b", "t::c"}

    def test_missing_dependency_outside_queue_is_not_cycle_member(self):
        """指向队列外的缺失依赖不构成环，返回空。"""
        st = PipelineState({}, {}, {}, [
            _jid("a", ["t::b"]),
            _jid("b", ["t::ghost"]),
        ])
        assert st.find_dependency_cycles() == []

    def test_unparseable_entries_skipped_graph_still_analyzed(self):
        """不可解析条目被跳过，其余图的环检测不受影响。"""
        st = PipelineState({}, {}, {}, [
            {"payload_not_task": True},
            _jid("a", ["t::b"]),
            _jid("b", ["t::a"]),
        ])
        assert st.find_dependency_cycles() == ["t::a", "t::b", "t::a"]


class TestCascadeFail:
    """级联失败：父失败 → 沿反向依赖递归标记全部下游。"""

    def test_cascade_direct_and_indirect(self):
        st = PipelineState({}, {}, {}, [
            _jid("a"),
            _jid("b", ["t::a"]),
            _jid("c", ["t::b"]),
            _jid("d"),
        ])
        cascaded = set(st.cascade_fail("t::a"))
        assert cascaded == {"t::b", "t::c"}
        assert "t::d" not in cascaded

    def test_cascade_skips_terminal_uids(self):
        """已完成/已失败的 job 不重复级联。"""
        st = PipelineState({"t::b": {}}, {"t::c": {}}, {}, [
            _jid("a"),
            _jid("b", ["t::a"]),
            _jid("c", ["t::a"]),
        ])
        assert st.cascade_fail("t::a") == []

    def test_cascade_skips_dispatching_job(self):
        """正在派发的 job 已出队（不在 _queue），不进级联名单。"""
        st = PipelineState({}, {}, {}, [
            _jid("a"),
            _jid("b", ["t::a"]),
        ])
        st.pop_job(1)
        assert st.cascade_fail("t::a") == []
