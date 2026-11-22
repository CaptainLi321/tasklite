"""PipelineState 契约测试：集合互斥、队首/队尾插队、环检测与级联失败。"""
from __future__ import annotations

import pytest

from tasklite.models.job import Job
from tasklite.models.state import PipelineState, job_uid_from_snapshot


def _jid(job_id: str, depends_on: list[str] | None = None, trigger: str | None = None) -> dict:
    """队列用 job dict（job_id 恒为确定性字面量）。"""
    d = {"task_type": "t", "job_id": job_id}
    if depends_on is not None:
        d["depends_on"] = depends_on
    if trigger is not None:
        d["trigger"] = trigger
    return d


class TestQueueOrdering:
    """队首/队尾插队语义与 uid 索引同步。"""

    def test_spawn_front_and_back(self):
        st = PipelineState({}, {}, {}, [_jid("old")])
        st.spawn_jobs([_jid("a"), _jid("b")])
        assert [job_uid_from_snapshot(j) for j in st.queue] == ["t::a", "t::b", "t::old"]

        st2 = PipelineState({}, {}, {}, [_jid("old")])
        st2.spawn_jobs([_jid("a")], front=False)
        assert [job_uid_from_snapshot(j) for j in st2.queue] == ["t::old", "t::a"]

    def test_requeue_front_pop_and_replace(self):
        st = PipelineState({}, {}, {}, [_jid("next"), _jid("other")])
        popped = st.pop_job(0)
        st.requeue_jobs([popped])
        assert [job_uid_from_snapshot(j) for j in st.queue] == ["t::next", "t::other"]

        assert st.queue_uids == {"t::next", "t::other"}
        st.pop_job(0)
        assert st.queue_uids == {"t::other"} and not st.is_empty
        st.pop_job(0)
        assert st.is_empty

        st_rep = PipelineState({}, {}, {}, [_jid("a"), _jid("b")])
        st_rep.replace_queue([_jid("c")])
        assert st_rep.queue_uids == {"t::c"}
        st_rep.queue = [_jid("d")]
        assert st_rep.queue_uids == {"t::d"}


class TestSixSetMutex:
    """集合互斥：queue / in-flight / successes / failed 与状态一致性。"""

    def test_is_known_covers_all_four_sources(self):
        st = PipelineState({"w1": {}}, {"f1": {}}, {}, [_jid("q1")])
        st.register_in_flight("i1")
        assert st.is_known("w1")
        assert st.is_known("f1")
        assert st.is_known("t::q1")
        assert st.is_known("i1")
        assert not st.is_known("ghost")

    def test_dispatch_flow_and_overlap_assertion(self):
        st = PipelineState({}, {}, {}, [_jid("x")])
        st.pop_job(0)
        st.register_in_flight("t::x")
        assert "t::x" not in st.queue_uids
        assert "t::x" in st.in_flight_uids

        st_overlap = PipelineState({}, {}, {}, [_jid("x")])
        with pytest.raises(AssertionError, match="queue and in_flight"):
            st_overlap.register_in_flight("t::x")

    def test_terminal_uid_allowed_during_rerun_and_mutex_violation_detected(self):
        st_inf = PipelineState({}, {}, {}, [])
        st_inf.mark_success("t::x", {"score": 1})
        st_inf.register_in_flight("t::x")
        assert "t::x" in st_inf.in_flight_uids and "t::x" in st_inf.success_uids
        st_inf._assert_state_consistent()

        st_q = PipelineState({}, {}, {}, [_jid("x")])
        st_q.mark_success("t::x", {"score": 1})
        assert "t::x" in st_q.queue_uids and "t::x" in st_q.success_uids
        st_q._assert_state_consistent()

        st_bad = PipelineState({}, {}, {}, [])
        st_bad._successes["t::x"] = {}
        st_bad._failed["t::x"] = {}
        st_bad._success_uids.add("t::x")
        st_bad._failed_uids.add("t::x")
        with pytest.raises(AssertionError, match="both successes and failed"):
            st_bad._assert_state_consistent()

    def test_unregister_clear_and_inverse_transitions(self):
        st = PipelineState({}, {}, {}, [])
        st.register_in_flight("a")
        st.register_in_flight("b")
        assert st.in_flight_uids == frozenset({"a", "b"})
        st.unregister_in_flight("a")
        assert st.in_flight_uids == frozenset({"b"})
        st.clear_in_flight()
        assert st.in_flight_uids == frozenset()

        st.mark_failed("t::x", {"error_message": "boom"})
        assert st.failed_uids == {"t::x"} and st.success_uids == set()
        st.mark_success("t::x", {"score": 1})
        assert st.success_uids == {"t::x"} and st.failed_uids == set()
        st.mark_failed("t::x", {"error_message": "again"})
        assert st.failed_uids == {"t::x"} and st.success_uids == set()


class TestJobUidFromSnapshotAndTerminalRecords:
    """uid 提取与终态记录、游标合并。"""

    def test_uid_extraction_and_corrupt_fallback(self):
        assert job_uid_from_snapshot(_jid("j")) == "t::j"
        assert job_uid_from_snapshot({"task_type": "t", "job_id": "j", "payload": {"k": 1}}) == "t::j"
        assert job_uid_from_snapshot(Job._restore("t", "j", attempt_no=2).to_dict()) == "t::j"

        uid_a = job_uid_from_snapshot({"payload": {"k": 1}})
        assert uid_a.startswith("_unknown::")
        assert uid_a == job_uid_from_snapshot({"payload": {"k": 1}})
        assert uid_a != job_uid_from_snapshot({"payload": {"k": 2}})

    def test_terminal_records_and_cursors(self):
        st = PipelineState({}, {}, {"a": "1"}, [])
        meta = {"nested": {"count": 1}}
        st.mark_success("t::x", meta)
        meta["nested"]["count"] = 99
        assert st.successes["t::x"]["nested"]["count"] == 1

        st.update_cursors({"a": "2", "b": "3"})
        assert st.cursors == {"a": "2", "b": "3"}
        st.update_cursors({"a": None})
        assert st.cursors == {"b": "3"}

        st.mark_success("t::w", {})
        st.mark_success("t::w", {"again": True})
        assert st.successes["t::w"] == {"again": True}
        st.mark_failed("t::f", {})
        assert "t::f" in st.failed_uids
        st.set_cursor("k", "v")
        assert st.cursors["k"] == "v"


class TestFindDependencyCycles:
    """依赖环检测（含深图迭代环检测与子图作用域）。"""

    @staticmethod
    def _linear_chain(n: int) -> list[dict]:
        return [
            {"task_type": "t", "job_id": f"j{i}", "depends_on": [f"t::j{i + 1}"]}
            if i + 1 < n else {"task_type": "t", "job_id": f"j{i}"}
            for i in range(n)
        ]

    def test_simple_longer_outside_and_acyclic_graphs(self):
        assert set(PipelineState({}, {}, {}, [_jid("a", ["t::b"]), _jid("b", ["t::a"])]).find_dependency_cycles()) == {"t::a", "t::b"}
        assert set(PipelineState({}, {}, {}, [_jid("a", ["t::b"]), _jid("b", ["t::a"]), _jid("c")]).find_dependency_cycles()) == {"t::a", "t::b"}
        assert set(PipelineState({}, {}, {}, [_jid("a", ["t::b"]), _jid("b", ["t::c"]), _jid("c", ["t::a"])]).find_dependency_cycles()) == {"t::a", "t::b", "t::c"}
        assert PipelineState({}, {}, {}, [_jid("a", ["t::b"]), _jid("b", ["t::c"]), _jid("c")]).find_dependency_cycles() == []
        assert PipelineState({}, {}, {}, [_jid("d", ["t::b", "t::c"]), _jid("b", ["t::a"]), _jid("c", ["t::a"]), _jid("a")]).find_dependency_cycles() == []

    def test_node_scope_and_cycle_path_semantics(self):
        st_scope = PipelineState({}, {}, {}, [
            _jid("a", ["t::b"]),
            _jid("b", ["t::a"]),
            _jid("c", ["t::d"]),
            _jid("d", ["t::c"]),
            _jid("outside", ["t::a"]),
        ])
        assert set(st_scope.find_dependency_cycles(node_uids={"t::a", "t::b", "t::outside"})) == {"t::a", "t::b"}

        assert set(PipelineState({}, {}, {}, [
            _jid("x", ["t::y"]), _jid("y", ["t::z"]), _jid("z", ["t::w"]), _jid("w", ["t::z"])
        ]).find_dependency_cycles()) == {"t::z", "t::w"}
        assert set(PipelineState({}, {}, {}, [
            _jid("a", ["t::b"]), _jid("b", ["t::c"]), _jid("d", ["t::d"])
        ]).find_dependency_cycles()) == {"t::d"}
        assert set(PipelineState({}, {}, {}, [
            _jid("a", ["t::b", "t::c"]), _jid("b", ["t::a"]), _jid("c", ["t::a"])
        ]).find_dependency_cycles()) == {"t::a", "t::b", "t::c"}
        assert PipelineState({}, {}, {}, [_jid("a", ["t::b"]), _jid("b", ["t::ghost"])]).find_dependency_cycles() == []
        assert set(PipelineState({}, {}, {}, [
            {"payload_not_task": True}, _jid("a", ["t::b"]), _jid("b", ["t::a"])
        ]).find_dependency_cycles()) == {"t::a", "t::b"}

    def test_deep_graph_cycle_detection_without_recursion_error(self):
        assert PipelineState({}, {}, {}, self._linear_chain(5000)).find_dependency_cycles() == []

        n = 5000
        ring = [{"task_type": "t", "job_id": f"k{i}", "depends_on": [f"t::k{(i + 1) % n}"]} for i in range(n)]
        assert set(PipelineState({}, {}, {}, ring).find_dependency_cycles()) == {f"t::k{i}" for i in range(n)}

        tailed = self._linear_chain(2500)
        tailed[-1]["depends_on"] = ["t::c_head"]
        tailed += [
            {"task_type": "t", "job_id": "c_head", "depends_on": ["t::c_tail"]},
            {"task_type": "t", "job_id": "c_tail", "depends_on": ["t::c_head"]},
        ]
        assert set(PipelineState({}, {}, {}, tailed).find_dependency_cycles()) == {"t::c_head", "t::c_tail"}


class TestCascadeFail:
    """级联失败：父失败 → 沿反向依赖递归标记全部下游。"""

    def test_cascade_direct_indirect_and_exclusions(self):
        st = PipelineState({}, {}, {}, [_jid("a"), _jid("b", ["t::a"]), _jid("c", ["t::b"]), _jid("d")])
        cascaded = set(st.cascade_fail("t::a"))
        assert cascaded == {"t::b", "t::c"} and "t::d" not in cascaded

        st_term = PipelineState({"t::b": {}}, {"t::c": {}}, {}, [_jid("a"), _jid("b", ["t::a"]), _jid("c", ["t::a"])])
        assert st_term.cascade_fail("t::a") == []

        st_disp = PipelineState({}, {}, {}, [_jid("a"), _jid("b", ["t::a"])])
        st_disp.pop_job(1)
        assert st_disp.cascade_fail("t::a") == []
