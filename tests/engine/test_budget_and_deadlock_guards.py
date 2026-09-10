"""预算豁免与死锁归因遮蔽的防御分支回归测试。

覆盖两个 engine 层防御分支：
1. 重试预算耗尽的 job 撞孤儿锁（lock_conflict=True 的 retry 结果）必须
   回队自恢复而非直接进 DLQ——与派发侧 probe defer「孤儿死后自恢复、
   不烧预算」的设计意图对称（判定端读结构化字段，与 error 前缀无关）。
2. 队列同时含畸形 job 与处于退避的 job 时，调度扫描必须把 min_wait
   强制为 inf——畸形归因不依赖任何等待，被有限退避遮蔽会逐轮推迟
   （指数退避放大时可拖数十分钟，管线表现为卡死无日志）。
"""

import time

from tasklite import Job
from tasklite.engine.scheduler import JobScheduler
from tasklite.models.state import PipelineState, uid_from_job_dict

from tests.helpers import (
    make_ipc_process_class, make_pipeline, patch_multiprocessing_for_fakes,
)


class TestLockConflictBudgetExemption:
    """预算判定豁免 lock_conflict：孤儿锁不烧预算、不误进 DLQ。"""

    def test_budget_exhausted_lock_conflict_requeues_not_dlq(
        self, tmp_path, monkeypatch,
    ):
        """retries 已达 max_retries + 孤儿锁冲突 → 回队而非进 DLQ。

        场景：预算由此前业务失败耗尽（retries == max_retries == 3），
        最后一次尝试是框架锁冲突（worker 拿锁超时，handler 未执行）。
        锁冲突是瞬态（同 uid 孤儿执行体仍持锁，孤儿死后重跑本可成功），
        若按超限判 DLQ，与「孤儿死后自恢复、不烧预算」的设计意图矛盾。
        变异体（预算检查删去 lock_conflict 豁免）下 job 直接进 DLQ，
        本测试红。
        """
        import json as json_mod
        import sqlite3 as sqlite3_mod

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)

        # retries 预算已由此前业务失败耗尽
        jd = Job("t", "j1", payload={}).to_dict()
        jd["retries"] = 3
        jd["max_retries"] = 3
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json_mod.dumps(jd),))
        conn.commit()
        conn.close()

        captured = {}
        real_commit_retry = pipeline.backend.commit_retry

        def fake_commit_retry(uid, retry_dict, front=False):
            captured["retry_dict"] = retry_dict
            return real_commit_retry(uid, retry_dict, front=front)
        monkeypatch.setattr(pipeline.backend, "commit_retry", fake_commit_retry)

        # 第一次：撞孤儿锁（预算耗尽也须豁免 DLQ）；第二次：孤儿已死，成功
        LockThenSuccess = make_ipc_process_class(results=[
            {"status": "retry",
             "error": "LOCK_CONFLICT: another execution body holds t::j1 lock",
             "lock_conflict": True},
            {"status": "success", "raw_result": True,
             "new_jobs": [], "resource_suspensions": [], "cursor_updates": {}},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=LockThenSuccess)
        monkeypatch.setattr("time.sleep", lambda s: None)  # 跳过退避 sleep

        pipeline.run()

        # 回队而非 DLQ：孤儿锁冲突豁免预算判定，孤儿死后自愈成功进 wall
        dlq_uids = {e.uid for e in pipeline.list_dlq()}
        assert "t::j1" not in dlq_uids, \
            f"预算耗尽撞孤儿锁必须回队自恢复，不得进 DLQ，实际 DLQ: {dlq_uids}"
        wall = pipeline.backend.load_wall()
        assert "t::j1" in wall, "孤儿死后重跑应成功进 wall"
        # 零计数回队：retries 保持 3（lock_conflict 分支不递增计数）
        assert captured["retry_dict"]["retries"] == 3, \
            f"锁冲突回队不得递增重试计数，实际 retries: {captured['retry_dict']['retries']}"
        # 走锁冲突 defer 分支（与中断/计数退避区分的独有统计）
        assert pipeline.stats.get("deferred_orphan", 0) >= 1, \
            "预算耗尽的锁冲突必须走零计数 defer 分支（deferred_orphan 计数）"


class TestMalformedNotMaskedByBackoff:
    """畸形 job 归因不被队列中其他 job 的退避遮蔽。"""

    def test_malformed_plus_backoff_forces_inf_min_wait(self):
        """队列同时含畸形 job 与退避中的 job → min_wait 必须强制 inf。

        畸形 job 无法反序列化，其死锁归因（handle_deadlock 的 malformed
        分支）只在 min_wait=inf 时触达；若被另一 job 的有限退避压成有限
        值，归因每轮被推迟到退避终结。变异体（强制 inf 条件遗漏
        malformed_indices）下 min_wait 保持有限，本测试红。
        """
        sched = JobScheduler({})  # 空资源表
        # 缺 job_id → Job.from_dict 抛 KeyError → 记为 malformed
        malformed = {"task_type": "t"}
        backing_off = Job("t", "x", payload={}).to_dict()
        backing_off["runtime"] = {"_backoff_until": time.monotonic() + 300.0}

        result = sched.pop_next_runnable(
            PipelineState({}, {}, {}, [malformed, backing_off])
        )

        assert len(result.malformed_uids) == 1 and result.malformed_uids[0].startswith("_unknown::"), "缺 job_id 的条目必须归因为畸形"
        assert result.min_wait == float('inf'), (
            "畸形归因必须强制 min_wait=inf——退避中另一 job 的有限退避"
            "不得遮蔽畸形死锁判定"
        )

    def test_malformed_dlq_not_deferred_by_backoff(self, tmp_path, monkeypatch):
        """端到端：畸形 job 的 DLQ 终态必须先于退避 job 的成功终态。

        强制 inf 条件遗漏 malformed 时：min_wait 被退避 job 压成有限值 →
        主循环 sleep 等退避 → 退避 job 先完成，畸形 job 才在队列仅剩
        自己时归因 DLQ——终态集合相同但归因被推迟整个退避期（单轮最长
        300s，累计可达数十分钟）。用 on_job_completed 的调用顺序区分
        两者（变异体下顺序颠倒，本测试红）。
        """
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)

        calls = []
        pipeline.on_job_completed = (
            lambda uid, meta, success, going_to_retry:
                calls.append((uid, bool(success)))
        )

        malformed = {"task_type": "t"}  # 缺 job_id → 无法反序列化
        backing_off = Job("t", "c", payload={}).to_dict()
        backing_off["runtime"] = {"_backoff_until": time.monotonic() + 2.0}

        FakeP = make_ipc_process_class(results=[
            {"status": "success", "raw_result": True,
             "new_jobs": [], "resource_suspensions": [], "cursor_updates": {}},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        # 直接构造 state + 主循环（绕过 enqueue 便于注入退避字段与畸形条目）
        state = PipelineState({}, {}, {}, [malformed, backing_off])
        pipeline._runtime.store.set_state(state)
        pipeline._runtime._run_loop()

        malformed_uid = uid_from_job_dict(malformed)
        failed = pipeline.backend.load_failed()
        assert malformed_uid in failed, "畸形 job 必须进 DLQ"
        assert failed[malformed_uid].get("error") == "MALFORMED_JOB", \
            f"畸形 job 必须归因 MALFORMED_JOB，实际: {failed[malformed_uid]}"
        wall = pipeline.backend.load_wall()
        assert "t::c" in wall, "退避 job 退避结束后应正常运行进 wall"

        # 顺序：畸形 job 的 DLQ 终态必须先于退避 job 的成功终态
        # （归因不被退避推迟）
        assert calls, "两个 job 的终态都必须触发 on_job_completed"
        assert calls[0] == (malformed_uid, False), (
            f"畸形归因不得被退避推迟（必须先于退避 job 终结），实际顺序: {calls}"
        )
        assert calls[1] == ("t::c", True), \
            f"退避 job 应在畸形 DLQ 之后正常完成，实际顺序: {calls}"
