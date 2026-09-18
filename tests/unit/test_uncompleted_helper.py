"""uncompleted 入队前过滤辅助回归。

契约：只按 wall（成功历史）过滤；DLQ（failed）中的 job 不排除——重跑
与否由 ``Job(rerun=...)`` 在派发层裁决，此处抢先过滤会吞掉 on_failure
的豁免语义；队列驻留重复不排除（去重是 enqueue 自身职责）。
"""
import pytest

from tasklite.models.job import Job
from tasklite.testing import running
from tests.helpers import make_pipeline


def _jobs(*uids):
    return [Job(u.split("::")[0], u.split("::")[1]) for u in uids]


class TestUncompletedFilter:
    """wall 过滤语义与顺序保持。"""

    def test_wall_hits_filtered_order_preserved(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.seed_wall(["fetch::done1", "fetch::done2"])
        jobs = _jobs("fetch::new", "fetch::done1", "fetch::new2", "fetch::done2")
        result = p.uncompleted(jobs)
        assert [j.uid for j in result] == ["fetch::new", "fetch::new2"]

    def test_empty_input_returns_empty(self, tmp_path):
        p = make_pipeline(tmp_path)
        assert p.uncompleted([]) == []

    def test_fresh_pipeline_returns_all(self, tmp_path):
        p = make_pipeline(tmp_path)
        jobs = _jobs("fetch::a", "fetch::b")
        assert [j.uid for j in p.uncompleted(jobs)] == ["fetch::a", "fetch::b"]

    def test_failed_dlq_entries_not_excluded(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.backend.append_failed("fetch::broken", {"error": "x"})
        jobs = _jobs("fetch::broken", "fetch::fresh")
        assert [j.uid for j in p.uncompleted(jobs)] == ["fetch::broken", "fetch::fresh"]

    def test_run_guard_rejects_during_run(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="uncompleted"):
                p.uncompleted([])


class TestUncompletedEnqueueComposition:
    """与 enqueue 组合的端到端语义：只入队未完成部分。"""

    def test_enqueue_of_uncompleted_skips_wall_hits(self, tmp_path):
        from tasklite.models.state import uid_from_job_dict

        p = make_pipeline(tmp_path)
        p.seed_wall(["fetch::done"])
        jobs = _jobs("fetch::done", "fetch::new")
        p.enqueue(p.uncompleted(jobs))
        queued = {uid_from_job_dict(row) for row in p.backend.load_queue()}
        assert queued == {"fetch::new"}
