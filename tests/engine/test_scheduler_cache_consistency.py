"""调度缓存一致性回归测试：cached_job 内容键缓存 run 内同 uid 不同内容冲突。

结论：同 run 内同 uid 但内容变化时必须使缓存失效，重新解析。

触发路径（同一 run 内）：
- 父任务 A spawn 子任务 child::X（rerun=every_run, resources={"good":1}）。
- A 完成进 wall；B 依赖 A，A 完成进 wall 后 B 才可跑。
- B spawn 同 uid child::X（every_run, resources={"nosuch":1}）——
  every_run 使该重 spawn 不被 wall 拦截（pipeline.py spawn 去重豁免 wall
  命中），从而**同 run 内第二次入队，但内容(resources)不同**。
- 修复前：scheduler.pop_next_runnable 用 cached_job 命中内容键 (child,X)，
  返回 content1 的 Job（resources={"good"}）→ 判定可运行、未标 unknown。
  _dispatch_job 用 Job.from_dict(content2)（{"nosuch"}）acquire →
  KeyError('nosuch') 未捕获 → 整条 run 崩溃。
- 修复后：cached_job 对调度只读字段（resources/depends_on）做一致性校验，
  不一致视为 miss 重新解析 → content2 正确归因 unknown resource → 死锁
  细粒度 DLQ，不再崩溃。

本测试断言**修复后的正确行为**：run 不崩溃；child::X 因 unknown 资源进
DLQ（failed）；good 资源仍可用（回归保护）。
"""
import pytest
from tasklite import Job
from tasklite.engine.resource import CapacityResource

from tests.helpers import make_pipeline, make_ipc_process_class, patch_multiprocessing_for_fakes


def testcached_job_stale_resources_no_crash_unknown_dlq(tmp_path, monkeypatch):
    pipeline = make_pipeline(tmp_path)
    pipeline.add_resource(CapacityResource("good", max_capacity=10.0))
    pipeline.register_handler("parent", lambda j, c: (True, {}))
    pipeline.register_handler("child", lambda j, c: (True, {}))

    X1 = Job("child", "X", rerun="every_run", resources={"good": 1.0}).to_dict()
    X2 = Job("child", "X", rerun="every_run", resources={"nosuch": 1.0}).to_dict()
    spawn_x1 = {"status": "success", "raw_result": True,
                "new_jobs": [X1], "resource_suspensions": [], "cursor_updates": {}}
    spawn_x2 = {"status": "success", "raw_result": True,
                "new_jobs": [X2], "resource_suspensions": [], "cursor_updates": {}}
    plain = {"status": "success", "raw_result": True,
             "new_jobs": [], "resource_suspensions": [], "cursor_updates": {}}
    # 启动顺序：#1 A(→spawn X1 good)  #2 X1(plain)  #3 B(→spawn X2 nosuch)
    FakeP = make_ipc_process_class(results=[spawn_x1, plain, spawn_x2, plain])
    patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

    A = Job("parent", "A", rerun="never", resources={"good": 1.0})
    B = Job("parent", "B", rerun="never", resources={"good": 1.0},
            depends_on=["parent::A"])
    pipeline.enqueue([A])
    pipeline.enqueue([B])

    # 修复后：run 不崩溃（修复前抛 KeyError('nosuch') 击穿 run）。
    # child::X 的 content2（nosuch）被正确 unknown 归因进 DLQ。
    pipeline.run()

    failed = pipeline.backend.load_failed()
    assert "child::X" in failed, "同 uid 不同内容的二次入队应被 fresh 解析并正确 DLQ"
    # 修复后 content2（nosuch）被 fresh 解析 → 未知资源归因（RESOURCE_DEADLOCK /
    # error_type=deadlock），而非放行到 acquire 以裸 KeyError 崩掉整条 run。
    meta = failed.get("child::X", {})
    assert meta.get("error_type") == "deadlock" or meta.get("error") == "RESOURCE_DEADLOCK", \
        f"child::X 应归因 deadlock，实际: {meta}"


def test_same_uid_same_content_cache_retained(tmp_path, monkeypatch):
    """同 uid 同内容（resources/depends_on 一致）仍复用缓存——回归保护：
    性能优化不被误伤。ewer_run 重跑同 content 不应触发 miss 重新解析的
    错误路径（此处用 _sched_fields_match 直接断言一致判定）。"""
    from tasklite.engine.scheduler import JobScheduler
    sched = JobScheduler(resources={})
    job = Job("t", "j", rerun="every_run", resources={"a": 1.0},
              depends_on=["other::x"])
    jd = job.to_dict()
    assert sched._sched_fields_match(job, jd) is True, \
        "同内容应判定字段一致（复用缓存）"
    jd2 = dict(jd)
    jd2["resources"] = {"b": 2.0}
    assert sched._sched_fields_match(job, jd2) is False, \
        "resources 变化应判定不一致（重新解析）"
    jd3 = dict(jd)
    jd3["depends_on"] = []
    assert sched._sched_fields_match(job, jd3) is False, \
        "depends_on 变化应判定不一致（重新解析）"


def test_sched_fields_match_null_safe(tmp_path):
    """调度字段匹配一致性：`_sched_fields_match` 必须与
    `Job.from_dict` 的 null 容忍语义一致——job_dict 中 `resources: null` /
    `depends_on: null` 时不得抛 TypeError（曾把合法 job 误判畸形进 DLQ）。"""
    from tasklite.engine.scheduler import JobScheduler
    sched = JobScheduler(resources={})
    # null 语义：Job.from_dict 把 None → {} / []
    job_null = Job("t", "j", rerun="every_run", resources=None, depends_on=None)
    jd_null = {"task_type": "t", "job_id": "j", "rerun": "every_run",
               "resources": None, "depends_on": None}
    # 不抛 TypeError，且与解析后的 Job 判定一致（{} vs []）
    assert sched._sched_fields_match(job_null, jd_null) is True, \
        "null resources/depends_on 应判定一致（与 Job.from_dict 语义对齐）"
    # 畸形值（非 dict/list）→ 返回 False（不抛原始 TypeError）
    jd_bad = dict(jd_null)
    jd_bad["resources"] = "not-a-dict"
    assert sched._sched_fields_match(job_null, jd_bad) is False, \
        "非 dict resources 应判定不一致（不抛 TypeError）"
