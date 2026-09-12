"""依赖环检测深图与精确路径语义的确定性回归测试。

不变式：
- find_dependency_cycles 为线性迭代实现（显式栈），遍历深度不受解释器
  递归上限约束——深度超过递归限制的线性依赖链不得使环检测本身抛
  RecursionError（governor 每轮仲裁对全队列调用，环检测崩溃即整轮 run 崩溃）；
- 返回口径为「每次回边命中：当前 DFS 路径自闭点起的切片 + 闭点重复」
  按遍历序拼接——成员、顺序、重复闭点是 governor 死锁归因依据的精确契约；
- 指向队列外的缺失依赖不构成环、不入结果；不可解析条目跳过且不中止整图。
"""

from tasklite.models.state import PipelineState


def _jd(jid, depends_on=None):
    d = {"task_type": "t", "job_id": jid}
    if depends_on is not None:
        d["depends_on"] = depends_on
    return d


def _linear_chain(n):
    """n 节点线性链：j_i 依赖 j_{i+1}，链尾无依赖。"""
    return [_jd(f"j{i}", [f"t::j{i+1}"] if i + 1 < n else None) for i in range(n)]


class TestDeepChainCycleDetection:
    def test_5000_node_linear_chain_completes_without_recursion_error(self):
        """深度超解释器递归限制的线性链：无环返回空且检测本身不崩。"""
        st = PipelineState({}, {}, {}, _linear_chain(5000))
        assert st.find_dependency_cycles() == []

    def test_5000_node_single_cycle_full_closure_exact_order(self):
        """5000 节点单环：路径切片 + 闭点重复构成完整闭环，顺序确定。"""
        n = 5000
        queue = [_jd(f"k{i}", [f"t::k{(i + 1) % n}"]) for i in range(n)]
        st = PipelineState({}, {}, {}, queue)
        assert st.find_dependency_cycles() == [f"t::k{i}" for i in range(n)] + ["t::k0"]

    def test_deep_chain_with_cycle_exact_cycle_members(self):
        """深链（2500 节点）尾部挂接双节点环：仅环成员入结果且路径精确。"""
        queue = _linear_chain(2500)
        queue[-1]["depends_on"] = ["t::c0"]
        queue += [
            _jd("c0", ["t::c1"]),
            _jd("c1", ["t::c0"]),
        ]
        st = PipelineState({}, {}, {}, queue)
        assert st.find_dependency_cycles() == ["t::c0", "t::c1", "t::c0"]


class TestCyclePathSemantics:
    def test_chain_leading_into_cycle_exact_path(self):
        """链 x→y 挂接环 z→w→z：仅闭环节点入结果，链上节点不入。"""
        st = PipelineState({}, {}, {}, [
            _jd("x", ["t::y"]),
            _jd("y", ["t::z"]),
            _jd("z", ["t::w"]),
            _jd("w", ["t::z"]),
        ])
        assert st.find_dependency_cycles() == ["t::z", "t::w", "t::z"]

    def test_self_loop_alongside_acyclic_chain(self):
        """自环与无环链共存：各自独立判定，链不入结果。"""
        st = PipelineState({}, {}, {}, [
            _jd("a", ["t::b"]),
            _jd("b", ["t::c"]),
            _jd("d", ["t::d"]),
        ])
        assert st.find_dependency_cycles() == ["t::d", "t::d"]

    def test_multiple_back_edges_from_shared_head(self):
        """共享头部的双环（a→{b,c}，b→a，c→a）：两道回边各闭一次。

        a 的依赖集为 set，遍历序随进程哈希而异，故按结构断言：
        两段各为 [a, 叶, a]，仅叶节点次序不定。
        """
        st = PipelineState({}, {}, {}, [
            _jd("a", ["t::b", "t::c"]),
            _jd("b", ["t::a"]),
            _jd("c", ["t::a"]),
        ])
        result = st.find_dependency_cycles()
        assert len(result) == 6
        assert result[0] == result[2] == result[3] == result[5] == "t::a"
        assert {result[1], result[4]} == {"t::b", "t::c"}

    def test_missing_dependency_outside_queue_is_not_cycle_member(self):
        """指向队列外的缺失依赖不构成环，返回空。"""
        st = PipelineState({}, {}, {}, [
            _jd("a", ["t::b"]),
            _jd("b", ["t::ghost"]),
        ])
        assert st.find_dependency_cycles() == []

    def test_unparseable_entries_skipped_graph_still_analyzed(self):
        """不可解析条目被跳过，其余图的环检测不受影响。"""
        st = PipelineState({}, {}, {}, [
            {"payload_not_task": True},
            _jd("a", ["t::b"]),
            _jd("b", ["t::a"]),
        ])
        assert st.find_dependency_cycles() == ["t::a", "t::b", "t::a"]
