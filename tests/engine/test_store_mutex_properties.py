"""StateStore wall/failed 终态互斥性质测试（hypothesis）。

锁定不变式：对任意「入队/成功/失败/重试/跳过/批量失败」随机操作序列，
同一 uid 在任意时刻至多属于 wall 与 failed 之一，且内存态与后端持久化镜像
保持互斥一致；终态 uid 不残留在内存队列中。

终态转移遵循真实状态机协议：终结前先从内存队列弹出（等价派发时 pop_job），
重试/入队仅对非终态 uid 生效（等价 wall 去重语义）。
"""

from __future__ import annotations

import pytest
from hypothesis import example, given, settings, strategies as st

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.store import StateStore
from tasklite.models.state import PipelineState

# uid 池：task_type::job_id 复合形态（含 '::'）
_UID_POOL = [
    "encode::1",
    "encode::2",
    "render::1",
    "render::2",
]

_SUCCESS_OPS = ("success",)
_FAILURE_OPS = ("failure", "apply_failed", "bulk_failure")


def _job_dict(uid: str) -> dict:
    task_type, job_id = uid.split("::", 1)
    return {"task_type": task_type, "job_id": job_id}


# 操作形态：(动作, uid 池下标)
op_strategy = st.lists(
    st.tuples(
        st.sampled_from(
            ["enqueue", "success", "failure", "apply_failed", "retry", "skip", "bulk_failure"]
        ),
        st.integers(min_value=0, max_value=len(_UID_POOL) - 1),
    ),
    max_size=24,
)


class _MutexProbe:
    """对一组随机操作执行真实状态机协议，并在每步后断言互斥不变式。"""

    def __init__(self) -> None:
        self.backend = InMemoryStateBackend()
        self.state = PipelineState({}, {}, {}, [])
        self.store = StateStore(self.backend, self.state)

    # 协议前置：终结类操作要求 uid 已离开内存队列（等价派发 pop_job）
    def _pop_from_memory_queue(self, uid: str) -> None:
        for idx, jd in enumerate(self.store.queue):
            if f"{jd.get('task_type')}::{jd.get('job_id')}" == uid:
                self.store.pop_job(idx)
                return

    def _uid_at(self, i: int) -> str:
        return _UID_POOL[i]

    def apply(self, action: str, i: int) -> None:
        uid = self._uid_at(i)
        if action == "enqueue":
            # wall 去重语义：终态 uid 不再入队；重复排队也去重
            if self.store.is_known(uid):
                return
            if uid in self.store.queue_uids:
                return
            self.store.spawn_jobs([_job_dict(uid)], front=False)
        elif action == "success":
            self._pop_from_memory_queue(uid)
            self.store.apply_success(uid, {"score": i})
        elif action == "failure":
            self._pop_from_memory_queue(uid)
            self.store.apply_failure(uid, {"error": "boom"}, job_dict=_job_dict(uid))
        elif action == "apply_failed":
            # apply_failed 是失败登记的内存尾段：先完成后端 DLQ 提交再登记
            self._pop_from_memory_queue(uid)
            if self.backend.commit_job_failure(uid, {"error": "tail"}):
                self.store.apply_failed(uid, {"error": "tail"})
        elif action == "retry":
            # 重试仅对非终态 uid 有意义（等价真实调度协议）
            if self.store.is_completed(uid) or self.store.is_failed(uid):
                return
            self._pop_from_memory_queue(uid)
            self.store.apply_retry(uid, _job_dict(uid), _job_dict(uid))
        elif action == "skip":
            self._pop_from_memory_queue(uid)
            self.store.apply_skip(uid)
        elif action == "bulk_failure":
            self._pop_from_memory_queue(uid)
            self.store.apply_bulk_failure([(uid, {"error": "deadlock"})])
        self.assert_mutex()

    def assert_mutex(self) -> None:
        """核心不变式：wall ∩ failed = ∅（内存与后端镜像一致），终态不留在队列。"""
        mem_wall = set(self.store.wall_uids)
        mem_failed = set(self.store.failed_uids)
        assert mem_wall & mem_failed == set(), f"内存互斥破坏: {mem_wall & mem_failed}"
        disk_wall = set(self.backend.load_wall())
        disk_failed = set(self.backend.load_failed())
        assert disk_wall & disk_failed == set(), f"后端互斥破坏: {disk_wall & disk_failed}"
        assert mem_wall == disk_wall, f"wall 镜像漂移: 内存 {mem_wall} vs 后端 {disk_wall}"
        assert mem_failed == disk_failed, f"failed 镜像漂移: 内存 {mem_failed} vs 后端 {disk_failed}"
        assert mem_wall | mem_failed <= set(
            _UID_POOL
        )  # 池外 uid 不应出现（spawn 恒空）
        for uid in mem_wall | mem_failed:
            assert uid not in self.store.queue_uids, f"终态 {uid} 残留在队列"


@pytest.mark.hypothesis
@settings(max_examples=40, deadline=None)
@example(ops=[])
@example(ops=[("success", 0), ("failure", 0), ("success", 0)])
@example(ops=[("failure", 1), ("enqueue", 1), ("retry", 1)])
@given(ops=op_strategy)
def test_wall_failed_mutex_holds_for_arbitrary_operation_sequences(ops):
    """任意操作序列后（及每一步后），wall/failed 全局互斥且镜像一致。"""
    probe = _MutexProbe()
    probe.assert_mutex()
    for action, i in ops:
        probe.apply(action, i)
    probe.assert_mutex()


@pytest.mark.hypothesis
@settings(max_examples=40, deadline=None)
@given(i=st.integers(min_value=0, max_value=len(_UID_POOL) - 1))
def test_terminal_transitions_move_uid_exactly_out_of_opposite_set(i):
    """成功/失败互逆转移：任一终态写入都把 uid 从对侧终态集合精确移出。"""
    uid = _UID_POOL[i]
    probe = _MutexProbe()
    probe._pop_from_memory_queue(uid)

    probe.store.apply_failure(uid, {"error": "boom"}, job_dict=_job_dict(uid))
    assert probe.store.is_failed(uid)
    assert not probe.store.is_completed(uid)

    probe.store.apply_success(uid, {"score": 1})
    assert probe.store.is_completed(uid)
    assert not probe.store.is_failed(uid)

    probe.store.apply_failure(uid, {"error": "again"}, job_dict=_job_dict(uid))
    assert probe.store.is_failed(uid)
    assert not probe.store.is_completed(uid)
    probe.assert_mutex()
