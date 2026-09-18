"""rerun 策略矩阵与依赖环路径的确定性回归测试。

不变式：
- rerun 三策略（every_run / on_failure / on_input_change）的作业在
  spawn_jobs / pop_job / clear_in_flight / replace_queue 四处队列操作中
  一致进入 rerun 活动集合，非策略作业绝不误收；
- find_dependency_cycles 必须给出**精确的环路径**（成员、顺序、重复闭点），
  而非仅「有环」的集合布尔——路径切片/起点破坏类变异必红；
- fail_cascade 沿反向依赖完整传播且跳过不可解析节点（不中止整图）。
"""

import pytest

from tasklite.models.state import PipelineState


def _jd(task: str, jid: str, **extra) -> dict:
    d = {"task_type": task, "job_id": jid}
    d.update(extra)
    return d


def _active(state: PipelineState) -> set:
    return set(state.rerun_active_uids)


RERUN_POLICIES = ["every_run", "on_failure", "on_input_change"]


class TestRerunPolicyMatrix:
    @pytest.mark.parametrize("policy", RERUN_POLICIES)
    def test_spawn_jobs_tracks_each_policy_and_ignores_plain(self, policy):
        st = PipelineState({}, {}, {}, [])
        st.spawn_jobs([_jd("t", "r", rerun=policy), _jd("t", "plain")])
        assert _active(st) == {"t::r"}, f"策略 {policy} 必须进 rerun 活动集合"

    @pytest.mark.parametrize("policy", RERUN_POLICIES)
    def test_constructor_seeds_active_from_initial_queue(self, policy):
        st = PipelineState({}, {}, {}, [_jd("t", "r", rerun=policy), _jd("t", "plain")])
        assert _active(st) == {"t::r"}, f"构造期必须识别策略 {policy} 的作业"

    @pytest.mark.parametrize("policy", RERUN_POLICIES)
    def test_pop_job_tracks_each_policy_and_discards_plain(self, policy):
        st = PipelineState({}, {}, {}, [])
        # 手工装队并清空活动集合：隔离 pop_job 自身的登记贡献
        #（构造期会从初始队列预填活动集合，混用会掩盖漏登记）。
        st._queue = [_jd("t", "r", rerun=policy), _jd("t", "plain")]
        st._queue_uids = {"t::r", "t::plain"}
        st._rerun_active_uids = set()
        st.pop_job(0)
        st.pop_job(0)
        assert _active(st) == {"t::r"}, f"策略 {policy} 弹出后必须登记活动集合"

    @pytest.mark.parametrize("policy", RERUN_POLICIES)
    def test_clear_in_flight_rebuilds_active_from_queue(self, policy):
        """清空在途时活动集合必须按当前队列重建（残留旧成员即污染）。"""
        st = PipelineState({}, {}, {}, [
            _jd("t", "r", rerun=policy),
            _jd("t", "plain"),
        ])
        st.register_in_flight("t::stale")
        st.clear_in_flight()
        assert _active(st) == {"t::r"}, \
            f"clear_in_flight 必须按队列重建活动集合（策略 {policy}），且清掉旧在途残留"

    @pytest.mark.parametrize("policy", RERUN_POLICIES)
    def test_replace_queue_rebuilds_active_from_new_queue(self, policy):
        st = PipelineState({}, {}, {}, [_jd("t", "old", rerun=policy)])
        st.replace_queue([_jd("t", "new", rerun=policy), _jd("t", "plain")])
        assert _active(st) == {"t::new"}, \
            f"整体换队必须按新队列重建活动集合（策略 {policy}，旧成员不得残留）"


class TestDependencyCyclePaths:
    def test_three_node_cycle_exact_path(self):
        """A→B→C→A：路径必须是含闭点的完整列表（起点切片破坏即红）。"""
        st = PipelineState({}, {}, {}, [
            _jd("t", "a", depends_on=["t::b"]),
            _jd("t", "b", depends_on=["t::c"]),
            _jd("t", "c", depends_on=["t::a"]),
        ])
        assert st.find_dependency_cycles() == ["t::a", "t::b", "t::c", "t::a"]

    def test_nested_cycle_exact_path(self):
        """嵌套环 A→B→C→B：闭点 B 在路径中部——起点定位必须精确。"""
        st = PipelineState({}, {}, {}, [
            _jd("t", "a", depends_on=["t::b"]),
            _jd("t", "b", depends_on=["t::c"]),
            _jd("t", "c", depends_on=["t::b"]),
        ])
        assert st.find_dependency_cycles() == ["t::b", "t::c", "t::b"]

    def test_self_loop_exact_path(self):
        """自环：路径为 [自身, 自身]。"""
        st = PipelineState({}, {}, {}, [
            _jd("t", "d", depends_on=["t::d"]),
        ])
        assert st.find_dependency_cycles() == ["t::d", "t::d"]

    def test_two_disjoint_cycles_exact_paths(self):
        """双环不相交：两次闭包的路径按遍历序拼接，无多余成员。"""
        st = PipelineState({}, {}, {}, [
            _jd("t", "a", depends_on=["t::b"]),
            _jd("t", "b", depends_on=["t::a"]),
            _jd("t", "c", depends_on=["t::d"]),
            _jd("t", "d", depends_on=["t::c"]),
        ])
        assert st.find_dependency_cycles() == [
            "t::a", "t::b", "t::a", "t::c", "t::d", "t::c",
        ]

    def test_fail_cascade_full_chain_skips_unparseable(self):
        """级联传播沿反向依赖走完整链；不可解析节点被跳过而非中止。"""
        st = PipelineState({}, {}, {}, [
            _jd("t", "corrupt"),  # 缺依赖结构但可构造；此条放入不可解析形态
            _jd("t", "b", depends_on=["t::a"]),
            _jd("t", "c", depends_on=["t::b"]),
            _jd("t", "free"),     # 无关作业不得误伤
        ])
        st._queue[0] = {"payload_not_task": True}  # 真正的不可解析条目
        assert sorted(st.fail_cascade("t::a")) == ["t::b", "t::c"]
