"""统一状态管理与历史清理测试。

覆盖：
1. set_job_status 目标态（pending, succeeded, failed）与递归级联；
2. 传 Job（未知自动建立、冲突拒绝）与传 UID（未知快照 KeyError 拒绝）；
3. 人工标记属性（completion_source=manual、note、不伪造 Attempt、Trigger 行为）；
4. clear_history（显式 records 组合、不删队列与游标、单调编号不重置）；
5. run() 运行期互斥锁（抛 RuntimeError）。
"""

from __future__ import annotations

import threading
import time
import pytest

from tasklite import TaskLite
from tasklite.models.job import Job
from tasklite.models.attempt import AttemptRecord
from tasklite.models.result import TaskResult


def _dummy_handler(job, ctx):
    return TaskResult.success()


def _slow_handler(job, ctx):
    time.sleep(0.5)
    return TaskResult.success()


class TestSetJobStatusCore:
    """set_job_status 核心三目标态与入参类型行为。"""

    def test_set_status_pending_succeeded_failed_flow(self, tmp_path):
        app = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        app.register_task("work", _dummy_handler)

        job_a = Job("work", "a")
        # 1. 传入完整 Job 直接建立人工成功态
        app.set_job_status(job_a, "succeeded", recursive=False)
        assert "work::a" in app.backend.load_successes()
        meta = app.backend.load_successes()["work::a"]
        assert meta.get("annotations", {}).get("completion_source") == "manual"
        assert meta.get("job_snapshot", {}).get("job_id") == "a"
        # 确认不生成 Attempt 记录
        assert app.backend.load_attempts("work::a") == []

        # 2. 传入 UID 变更为 failed 态，并携带 note
        app.set_job_status("work::a", "failed", recursive=False, note="手工标记失败")
        assert "work::a" not in app.backend.load_successes()
        assert "work::a" in app.backend.load_failed()
        fail_meta = app.backend.load_failed()["work::a"]
        assert fail_meta.get("note") == "手工标记失败"
        assert fail_meta.get("annotations", {}).get("completion_source") == "manual"

        # 3. 传入 UID 恢复为 pending
        app.set_job_status("work::a", "pending", recursive=False)
        assert "work::a" not in app.backend.load_failed()
        queue = app.backend.load_queue()
        assert len(queue) == 1
        assert queue[0]["job_id"] == "a"

    def test_set_status_uid_unknown_raises_key_error(self, tmp_path):
        app = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        app.register_task("work", _dummy_handler)

        with pytest.raises(KeyError, match="No existing snapshot found"):
            app.set_job_status("work::unknown", "succeeded", recursive=False)

    def test_set_status_job_conflict_raises_value_error(self, tmp_path):
        app = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        app.register_task("work", _dummy_handler)

        job_original = Job("work", "a", payload={"ver": 1})
        app.set_job_status(job_original, "succeeded", recursive=False)

        # 传入配置不同的同一 UID Job，必须报错拒绝
        job_conflict = Job("work", "a", payload={"ver": 2})
        with pytest.raises(ValueError, match="conflict"):
            app.set_job_status(job_conflict, "succeeded", recursive=False)

    def test_set_status_invalid_arguments_raises(self, tmp_path):
        app = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        app.register_task("work", _dummy_handler)

        with pytest.raises(ValueError, match="status must be one of"):
            app.set_job_status(Job("work", "a"), "invalid_status", recursive=False)  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="recursive must be a bool"):
            app.set_job_status(Job("work", "a"), "succeeded", recursive="yes")  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="note must be a str or None"):
            app.set_job_status(Job("work", "a"), "succeeded", recursive=False, note=123)  # type: ignore[arg-type]


class TestSetJobStatusRecursive:
    """set_job_status 级联下游功能。"""

    def test_recursive_cascades_to_downstream(self, tmp_path):
        app = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        app.register_task("work", _dummy_handler)

        job_a = Job("work", "a")
        job_b = Job("work", "b", depends_on=["work::a"])
        job_c = Job("work", "c", depends_on=["work::b"])
        app.enqueue(job_a)
        app.enqueue(job_b)
        app.enqueue(job_c)

        # 非递归：仅更新目标 a
        app.set_job_status(job_a, "failed", recursive=False)
        assert "work::a" in app.backend.load_failed()
        assert "work::b" not in app.backend.load_failed()
        assert "work::c" not in app.backend.load_failed()

        # 递归：级联将直接与间接下游 (b, c) 一并置为目标态
        app.set_job_status(job_a, "succeeded", recursive=True)
        assert "work::a" in app.backend.load_successes()
        assert "work::b" in app.backend.load_successes()
        assert "work::c" in app.backend.load_successes()
        assert app.backend.load_failed() == {}
        assert app.backend.load_queue() == []


class TestClearHistory:
    """clear_history 行为与不变式锁定。"""

    def test_clear_history_granular_records(self, tmp_path):
        app = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        app.register_task("work", _dummy_handler)

        job = Job("work", "item")
        app.set_job_status(job, "succeeded", recursive=False)
        app.set_cursor("cur", "pos1")

        # 仅清除 successes
        deleted = app.clear_history("work::item", records=["successes"])
        assert deleted == 1
        assert "work::item" not in app.backend.load_successes()
        # 游标不受影响
        assert app.get_cursor("cur") == "pos1"

    def test_clear_history_does_not_reset_sequences_monotonicity(self, tmp_path):
        app = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        app.register_task("work", _dummy_handler)

        app.enqueue(Job("work", "item"))
        app.run()
        app.stop()

        attempts = app.backend.load_attempts("work::item")
        assert len(attempts) == 1
        first_attempt_no = attempts[0].attempt_no

        # 清除历史记录
        app.clear_history("work::item", records=["successes", "attempts"])
        assert app.backend.load_attempts("work::item") == []
        assert "work::item" not in app.backend.load_successes()

        # 重新入队并执行
        app.enqueue(Job("work", "item"))
        app.run()
        app.stop()

        new_attempts = app.backend.load_attempts("work::item")
        assert len(new_attempts) == 1
        # 不变式：内部编号计数器单调递增，不因 clear_history 重置
        assert new_attempts[0].attempt_no > first_attempt_no


class TestManagementConcurrencyLock:
    """运行期间管理 API 的互斥锁保护。"""

    def test_management_apis_rejected_during_run(self, tmp_path):
        app = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
        app.register_task("work", _slow_handler)
        app.enqueue(Job("work", "slow_job"))

        t = threading.Thread(target=app.run, kwargs={"install_signals": False})
        t.start()
        # 等待 run 启动并进入派发循环
        time.sleep(0.15)

        try:
            with pytest.raises(RuntimeError, match="outside run()"):
                app.set_job_status("work::slow_job", "succeeded", recursive=False)
            with pytest.raises(RuntimeError, match="outside run()"):
                app.clear_history("work::slow_job", records=["successes"])
            with pytest.raises(RuntimeError, match="outside run()"):
                app.get_cursor("c")
            with pytest.raises(RuntimeError, match="outside run()"):
                app.set_cursor("c", "val")
        finally:
            t.join()
            app.stop()

