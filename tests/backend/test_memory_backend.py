"""InMemoryStateBackend 契约与功能测试。"""

import pytest
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite


def _calc_worker(job, ctx):
    return {"doubled": job.payload["x"] * 2}


class TestInMemoryStateBackendContract:
    def test_basic_crud_and_isolation(self):
        b = InMemoryStateBackend()
        # 初始状态为空
        assert b.load_wall() == {}
        assert b.load_failed() == {}
        assert b.load_cursors() == {}
        assert b.load_queue() == []

        # enqueue_jobs
        j1 = {"task_type": "t", "job_id": "1", "payload": {"a": 1}}
        j2 = {"task_type": "t", "job_id": "2", "payload": {"a": 2}}
        inserted = b.enqueue_jobs([j1, j2])
        assert inserted == ["t::1", "t::2"]
        assert len(b.load_queue()) == 2

        # 幂等去重入队
        inserted2 = b.enqueue_jobs([j1, {"task_type": "t", "job_id": "3"}])
        assert inserted2 == ["t::3"]
        assert len(b.load_queue()) == 3

    def test_commit_job_success_with_spawn_and_cursor(self):
        b = InMemoryStateBackend()
        j1 = {"task_type": "t", "job_id": "1"}
        b.enqueue_jobs([j1])

        spawn = {"task_type": "t", "job_id": "child"}
        ok = b.commit_job_success(
            "t::1",
            {"status": "ok"},
            spawned_jobs=[spawn],
            cursor_updates={"page": "2"},
        )
        assert ok is True
        wall = b.load_wall()
        assert "t::1" in wall
        assert wall["t::1"]["status"] == "ok"
        assert b.load_cursors() == {"page": "2"}
        q = b.load_queue()
        assert len(q) == 1
        assert q[0]["job_id"] == "child"

    def test_commit_job_failure_and_dlq_metadata(self):
        b = InMemoryStateBackend()
        j1 = {"task_type": "t", "job_id": "fail1"}
        b.enqueue_jobs([j1])

        ok = b.commit_job_failure("t::fail1", {"error": "NETWORK_TIMEOUT"})
        assert ok is True
        failed = b.load_failed()
        assert "t::fail1" in failed
        entry = failed["t::fail1"]
        assert entry["error"] == "NETWORK_TIMEOUT"
        assert entry["_attempt"] == 1
        assert "failed_at" in entry
        assert entry["error_type"] == "unknown"
        assert b.load_queue() == []

        # 结构化错误与 fatal 分类测试
        b.commit_job_failure("t::fatal", {"error": "Boom", "fatal": True})
        b.commit_job_failure("t::deadlock", {"error": "DEADLOCK_CLASSIFICATION_GAP"})
        b.commit_job_failure("t::dep", {"error": "JOB_DEPENDENCY"})
        b.commit_job_failure("t::retries", {"error": "MAX_RETRIES_EXCEEDED"})

        res = b.load_failed()
        assert res["t::fatal"]["error_type"] == "fatal"
        assert res["t::deadlock"]["error_type"] == "deadlock"
        assert res["t::dep"]["error_type"] == "dependency"
        assert res["t::retries"]["error_type"] == "transient_exhausted"

    def test_commit_retry(self):
        b = InMemoryStateBackend()
        j1 = {"task_type": "t", "job_id": "retry1"}
        b.enqueue_jobs([j1])

        retry_job = {"task_type": "t", "job_id": "retry1", "retries": 1}
        ok = b.commit_retry("t::retry1", retry_job, front=True)
        assert ok is True
        q = b.load_queue()
        assert len(q) == 1
        assert q[0]["retries"] == 1
        assert b.load_wall() == {}

    def test_meta_persistence(self):
        b = InMemoryStateBackend()
        assert b.get_meta("key1") is None
        b.set_meta("key1", "val1")
        assert b.get_meta("key1") == "val1"


class TestTaskLiteWithMemoryBackend:
    def test_tasklite_runs_with_memory_backend(self, tmp_path):
        p = TaskLite("mem_test", state_dir=tmp_path, backend="memory")
        assert p.backend_type == "memory"

        p.register_handler("calc", _calc_worker)
        p.enqueue(Job("calc", "1", payload={"x": 10}))
        p.enqueue(Job("calc", "2", payload={"x": 20}))

        p.run()
        assert p.stats.completed == 2
        wall = p.backend.load_wall()
        assert "calc::1" in wall
        assert wall["calc::1"]["doubled"] == 20
        assert "calc::2" in wall
        assert wall["calc::2"]["doubled"] == 40
