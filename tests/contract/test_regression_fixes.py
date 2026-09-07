"""Regression tests for system fixes and invariants.

验证系统在异常与边界条件下的鲁棒性：
- _save_queue_crash_safe 从磁盘恢复 pop 后丢失的作业
- 调度器基于「handler 默认资源 ∪ job 资源」做检查（限速/容量/unknown 不被绕过）
- _release_acquired 单资源失败不中断后续释放
- ctx.spawn 立即预检 JSON 可序列化性
- depends_on 字符串被拒绝（不再静默肢解为字符列表）
- TypedDict(total=False) 可选字段允许缺失
- 其他：bool/int、null 字节路径、退避脏数据、_backoff_ 豁免收窄
"""

from typing import Literal, Optional, TypedDict

import pytest

from tasklite.engine.resource import CapacityResource
from tasklite.engine.scheduler import JobScheduler
from tasklite.models.context import TaskContext
from tasklite.models.job import Job
from tasklite.models.state import PipelineState, uid_from_job_dict
from tasklite.taxonomy import validate_payload
from tests.helpers import make_fake_process_class, patch_multiprocessing_for_fakes


# ── 崩溃路径保存以磁盘为基准，不丢作业 ────────────────────────────


@pytest.mark.parametrize("fixture_name", ["pipeline_sqlite", "pipeline_sqlite"])
def test_crash_safe_save_recovers_popped_job(request, fixture_name):
    """pop 之后、commit/requeue 之前崩溃：磁盘作业必须被 crash-safe save 找回。"""
    p = request.getfixturevalue(fixture_name)
    job = Job("t", "a", payload={})
    p.enqueue([job])

    # 模拟 _dispatch_job 的 pop 之后、进入 try 之前命中 KeyboardInterrupt：
    # 作业已从内存队列弹出，未 commit、未 requeue、未注册 in-flight。
    state = PipelineState(
        p.backend.load_wall(), p.backend.load_failed(),
        p.backend.load_cursors(), p.backend.load_queue(),
    )
    p.runtime.ctx.set_state(state)
    state.pop_job(0)
    assert state.queue == []  # 内存已丢

    p.runtime._recovery.save_queue_crash_safe()

    disk_q = p.backend.load_queue()
    assert any(uid_from_job_dict(jd) == job.uid for jd in disk_q), (
        "crash-safe save must recover the popped job from disk"
    )


def test_crash_safe_save_dedups_double_requeue(pipeline_sqlite):
    """异常路径双重 requeue 同一作业：crash-safe save 按 uid 去重。"""
    p = pipeline_sqlite
    job = Job("t", "a", payload={})
    p.enqueue([job])

    state = PipelineState(
        p.backend.load_wall(), p.backend.load_failed(),
        p.backend.load_cursors(), p.backend.load_queue(),
    )
    p.runtime.ctx.set_state(state)
    # 模拟窗口：同一 job 被 requeue 两次
    state.requeue_jobs([job.to_dict()], front=True)
    state.requeue_jobs([job.to_dict()], front=True)
    assert len(state.queue) == 3  # 磁盘原 1 + 重复 requeue 2

    p.runtime._recovery.save_queue_crash_safe()

    disk_uids = [uid_from_job_dict(jd) for jd in p.backend.load_queue()]
    assert disk_uids.count(job.uid) == 1, (
        f"duplicate uid must collapse to one, got {disk_uids}"
    )


def test_crash_safe_save_load_failure_preserves_disk(pipeline_sqlite, monkeypatch):
    """load_queue 失败时不得用空列表覆盖磁盘——磁盘上「已 commit 但
    内存未同步」的作业会在此次保存中被永久抹除。pre-fix：disk_q=[] 兜底 →
    save_queue(内存) 覆盖 → 磁盘队列被清空/截断。修复：跳过覆盖保留磁盘真相。"""
    p = pipeline_sqlite
    job = Job("t", "a", payload={})
    p.enqueue([job])

    orig_load = p.backend.load_queue  # monkeypatch 前保存原始读法（断言用）
    calls = []
    monkeypatch.setattr(p.backend, "save_queue", lambda q: calls.append(list(q)))
    monkeypatch.setattr(p.backend, "load_queue", lambda: (_ for _ in ()).throw(IOError("disk read error")))

    p.runtime._recovery.save_queue_crash_safe()

    assert calls == [], f"load 失败不得触发覆盖保存: {calls}"
    # 磁盘原样保留（enqueue 落盘仍在，load 失败未覆盖）——用原始读法绕开 patch
    disk_uids = [uid_from_job_dict(jd) for jd in orig_load()]
    assert job.uid in disk_uids


# ── 调度器合并 handler 默认资源检查 ───────────────────────────────


def test_scheduler_unknown_resource_from_handler_defaults():
    """handler 默认资源引用未注册资源 → 合并后检测为 unknown。"""
    from tasklite.pipeline import HandlerEntry
    handlers = {"fetch": HandlerEntry(lambda j, c: True, {"api": 1.0}, None)}
    sched = JobScheduler({}, handlers)  # 空资源表
    job_dict = Job("fetch", "x", payload={}).to_dict()
    result = sched.pop_next_runnable(PipelineState({}, {}, {}, [job_dict]))
    assert result.unknown_resource_indices == [0]


def test_scheduler_capacity_check_uses_merged_resources():
    """handler 默认资源（容量已满）→ 合并后调度器等待而非派发。"""
    from tasklite.pipeline import HandlerEntry
    cap = CapacityResource("api", 1.0)
    cap.acquire(1.0)  # 容量已满
    try:
        handlers = {"fetch": HandlerEntry(lambda j, c: True, {"api": 1.0}, None)}
        sched = JobScheduler({"api": cap}, handlers)
        job_dict = Job("fetch", "x", payload={}).to_dict()
        result = sched.pop_next_runnable(PipelineState({}, {}, {}, [job_dict]))
        assert result.runnable_idx is None
        # 容量满 → 等待轮询间隔（有限 min_wait），而不是被误派发
        assert result.min_wait == cap._CAPACITY_POLL_INTERVAL
    finally:
        cap.release(1.0)


# ── _release_acquired 逐资源释放 ──────────────────────────────────


def test_release_acquired_continues_after_single_failure(pipeline_sqlite):
    """第 1 个资源 release 抛异常：后续资源仍被释放（确保异常时无泄漏）。"""

    class BoomResource(CapacityResource):
        def release(self, amount):
            raise RuntimeError("boom")

    p = pipeline_sqlite
    p.resources["boom"] = BoomResource("boom", 10.0)
    p.resources["ok"] = CapacityResource("ok", 10.0)
    p.resources["boom"].acquire(1.0)
    p.resources["ok"].acquire(1.0)

    p.runtime._completion.release_acquired([("boom", 1.0), ("ok", 1.0)])

    assert p.resources["ok"].used == 0.0, "ok resource must still be released"


# ── ctx.spawn JSON 序列化预检 ─────────────────────────────────────


def test_spawn_rejects_non_json_payload():
    ctx = TaskContext(Job("t", "a", payload={}), set(), set(), {})
    with pytest.raises(ValueError, match="not JSON-serializable"):
        ctx.spawn(Job("t", "child", payload={"bad": object()}))
    assert ctx.new_jobs == []  # 坏作业不进入 new_jobs


def test_spawn_accepts_json_payload():
    ctx = TaskContext(Job("t", "a", payload={}), set(), set(), {})
    child = Job("t", "child", payload={"ok": [1, 2, 3]})
    ctx.spawn(child)
    assert ctx.new_jobs == [child]


def test_spawn_rejects_inf_payload():
    """payload 含 float('inf')/nan 时触发预检拒绝（防止产出非标准 JSON）。"""
    ctx = TaskContext(Job("t", "a", payload={}), set(), set(), {})
    with pytest.raises(ValueError, match="not JSON-serializable"):
        ctx.spawn(Job("t", "child", payload={"bad": float("inf")}))
    with pytest.raises(ValueError, match="not JSON-serializable"):
        ctx.spawn(Job("t", "child2", payload={"bad": float("nan")}))
    assert ctx.new_jobs == []


# ── depends_on 类型校验 ───────────────────────────────────────────


def test_depends_on_string_rejected():
    """'parent::id' 字符串不再被静默肢解为字符列表。"""
    with pytest.raises(TypeError):
        Job("t", "i", depends_on="parent::id")


def test_depends_on_list_still_accepted():
    job = Job("t", "i", depends_on=["p::q", "r::s"])
    assert job.depends_on == ["p::q", "r::s"]


# ── TypedDict total=False 可选字段 ────────────────────────────────


def test_total_false_typeddict_optional_fields():
    class PD(TypedDict, total=False):
        name: str
        count: int

    assert validate_payload({}, PD) == []
    assert validate_payload({"name": "x"}, PD) == []


def test_total_false_typeddict_wrong_type_still_rejected():
    class PD(TypedDict, total=False):
        name: str

    errors = validate_payload({"name": 1}, PD)
    assert any("expected str" in e for e in errors)


def test_total_true_typeddict_missing_required_rejected():
    class PD(TypedDict):
        name: str

    errors = validate_payload({}, PD)
    assert any("missing required field 'name'" in e for e in errors)


# ── 其他修复 ──────────────────────────────────────────────────────────


def test_int_field_rejects_bool():
    class Schema(TypedDict):
        count: int

    errors = validate_payload({"count": True}, Schema)
    assert any("expected int, got bool" in e for e in errors)


@pytest.mark.parametrize("output_root", ["sandboxed", None])
def test_declare_output_null_byte_rejected(tmp_path, output_root):
    root = tmp_path if output_root == "sandboxed" else None
    ctx = TaskContext(Job("t", "a", payload={}), set(), set(), {}, output_root=root)
    with pytest.raises(ValueError, match="null byte"):
        ctx.declare_output("bad\x00path.txt")


def test_backoff_dirty_string_does_not_crash():
    """_backoff_until 为字符串脏数据 → 视为无退避（避免加载期崩溃）。

    退避状态在 `runtime` 子 dict——注入到 runtime 内验证
    scheduler 对子 dict 内脏数据的防御。
    """
    sched = JobScheduler({})
    jd = Job("t", "x", payload={}).to_dict()
    jd["runtime"] = {"_backoff_until": "garbage"}
    result = sched.pop_next_runnable(PipelineState({}, {}, {}, [jd]))
    assert result.runnable_idx == 0


def test_backoff_dirty_string_until_does_not_crash_at_load(pipeline_sqlite, monkeypatch):
    """_backoff_until 为字符串脏数据 → 加载期不崩溃，job 照常执行成功。"""
    p = pipeline_sqlite
    p.register_handler("t", lambda j, c: (True, {}))
    job = Job("t", "x", payload={})
    p.enqueue([job])
    q = p.backend.load_queue()
    q[0]["_backoff_until"] = "garbage"   # 字符串脏数据
    p.backend.save_queue(q)

    FakeP = make_fake_process_class("success")
    patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

    p.run()

    assert "t::x" in p.backend.load_wall(), "字符串 _backoff_until 不应阻止 job 执行"


def test_backoff_wall_deadline_dirty_string_does_not_crash(pipeline_sqlite, monkeypatch):
    """退避时间 _backoff_wall_deadline 为字符串脏数据时在启动期清除，不崩溃，job 照常执行成功。"""
    p = pipeline_sqlite
    p.register_handler("t", lambda j, c: (True, {}))
    job = Job("t", "x", payload={})
    p.enqueue([job])
    q = p.backend.load_queue()
    q[0]["_backoff_wall_deadline"] = "garbage"
    p.backend.save_queue(q)

    FakeP = make_fake_process_class("success")
    patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

    p.run()

    assert "t::x" in p.backend.load_wall(), (
        "job with dirty _backoff_wall_deadline must still run and succeed"
    )


def test_backoff_prefix_exemption_removed():
    """payload 里的任意 _backoff_* 字段不再被豁免（框架字段在 job dict 顶层）。"""

    class Schema(TypedDict):
        name: str

    errors = validate_payload({"name": "x", "_backoff_evil": 1}, Schema)
    assert any("unexpected field '_backoff_evil'" in e for e in errors)

    # _seen_ids（旧 discovery 保留字段）随旧 discovery 移除也不再豁免
    errors = validate_payload({"name": "x", "_seen_ids": ["a"]}, Schema)
    assert any("unexpected field '_seen_ids'" in e for e in errors)


# ── Union 中的 int 成员拒绝 bool ────────────────────────────────


def test_optional_int_rejects_bool():
    """Optional[int] 不再接受 True/False（isinstance(True, int)
    放行的旧 bug）；None 与正常 int 仍接受。"""

    class Schema(TypedDict):
        count: Optional[int]

    errors = validate_payload({"count": True}, Schema)
    assert any("got bool" in e for e in errors), f"True must be rejected, got {errors}"
    errors = validate_payload({"count": False}, Schema)
    assert any("got bool" in e for e in errors), f"False must be rejected, got {errors}"
    assert validate_payload({"count": None}, Schema) == []
    assert validate_payload({"count": 42}, Schema) == []


def test_bool_union_member_still_accepts_bool():
    """bool 显式作为 Union 成员时 True 仍被接受（避免误伤）。"""

    class Schema(TypedDict):
        flag: Optional[bool]

    assert validate_payload({"flag": True}, Schema) == []
    assert validate_payload({"flag": None}, Schema) == []


# ── Literal 类型感知比较（True != 1 语义） ──────────────────────


def test_literal_int_rejects_bool():
    """Literal[1, 2] 拒绝 True（True == 1 但类型不符）；1/2 仍接受。"""

    class Schema(TypedDict):
        code: Literal[1, 2]

    errors = validate_payload({"code": True}, Schema)
    assert any("Literal" in e for e in errors), f"True must be rejected, got {errors}"
    assert validate_payload({"code": 1}, Schema) == []
    assert validate_payload({"code": 2}, Schema) == []


def test_literal_bool_members_still_accept_bool():
    """Literal[True, False] 显式声明 bool 成员时 True 仍被接受。"""

    class Schema(TypedDict):
        flag: Literal[True, False]

    assert validate_payload({"flag": True}, Schema) == []
    assert validate_payload({"flag": False}, Schema) == []


# ── missing dependency 不被 pending 依赖遮蔽 ─────────────────────


def test_scheduler_missing_dependency_not_shadowed_by_pending():
    """依赖列表中排在 pending 依赖（in-flight）之后的真正缺失依赖
    必须被记录为 missing（避免死锁漏检）。"""
    sched = JobScheduler({})
    job_x = Job("t", "x", payload={}, depends_on=["t::pending", "ghost::dep"]).to_dict()
    state = PipelineState({}, {}, {}, [job_x])
    # t::pending 正在运行（in-flight），ghost::dep 真正缺失
    result = sched.pop_next_runnable(state, in_flight_uids=frozenset({"t::pending"}))
    assert result.runnable_idx is None
    assert result.waiting_for_dependency is True
    assert 0 in result.missing_dependency_indices, (
        "truly-missing dep must be recorded even when an earlier dep is pending"
    )


# ── impossible resource 不被有限等待遮蔽 ─────────────────────────


def test_scheduler_impossible_resource_not_shadowed_by_finite_wait():
    """前面资源的有限等待不再遮蔽后面不可达（inf）资源——
    记录 impossible_resource_indices 并触发死锁判定（min_wait=inf），
    而非无限轮询。"""
    cap1 = CapacityResource("slot1", 1.0)
    cap1.acquire(1.0)  # 已满 → 有限等待
    cap2 = CapacityResource("slot2", 1.0)  # 请求 200 → inf 不可达
    try:
        sched = JobScheduler({"slot1": cap1, "slot2": cap2})
        jd = Job("t", "x", payload={}, resources={"slot1": 1.0, "slot2": 200.0}).to_dict()
        result = sched.pop_next_runnable(PipelineState({}, {}, {}, [jd]))
        assert 0 in result.impossible_resource_indices, (
            "impossible resource must be recorded despite the finite wait"
        )
        assert result.min_wait == float('inf'), (
            "impossible resource must force min_wait=inf (deadlock), "
            f"got {result.min_wait}"
        )
    finally:
        cap1.release(1.0)
