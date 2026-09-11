"""DLQ ``_attempt`` 写入计数合并语义回归测试（memory / sqlite 双后端）。

不变式（失败历史可观测契约）：``_attempt`` 是「该 uid 被写入 DLQ 的
次数」，由两后端各自的写入单一出口（``_write_dlq_entry`` /
``_write_dlq_row``）统一维护——首写为 1、同 uid 重写递增、
损坏既有记录从 1 重计、结构化字段（error_type / failed_at）自动补全。
"""

import json
import sqlite3

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend


@pytest.fixture(params=["memory", "sqlite"])
def backend(request, tmp_path):
    if request.param == "memory":
        return InMemoryStateBackend()
    return SQLiteStateBackend(tmp_path / "state.db")


def _inject_corrupted_prev(backend, uid: str) -> None:
    """注入非 dict 的损坏既有 DLQ 记录（模拟位腐/手工误改）。"""
    if isinstance(backend, InMemoryStateBackend):
        backend._failed[uid] = "corrupted-not-a-dict"
        return
    conn = sqlite3.connect(str(backend.path))
    try:
        conn.execute(
            "INSERT INTO failed_dlq (uid, payload) VALUES (?, ?)", (uid, "{not-json")
        )
        conn.commit()
    finally:
        conn.close()


class TestAttemptMergeSemantics:
    def test_first_write_counts_from_one(self, backend):
        uid = "t::first"
        assert backend.commit_job_failure(uid, {"error": "boom"}) is True
        entry = backend.load_failed()[uid]
        assert entry["_attempt"] == 1, "首次写入 DLQ 计数必须从 1 起"
        assert entry["error_type"], "error_type 必须自动补全"
        assert entry["failed_at"], "failed_at 必须自动补全"

    def test_repeated_failure_increments_attempt(self, backend):
        uid = "t::dup"
        assert backend.commit_job_failure(uid, {"error": "first"}) is True
        assert backend.commit_job_failure(uid, {"error": "second"}) is True
        entry = backend.load_failed()[uid]
        assert entry["_attempt"] == 2, "同一 uid 重复失败必须递增写入计数"
        assert entry["error"] == "second", "最新失败元数据必须生效"

    def test_corrupted_prev_record_recounts_from_one(self, backend):
        """既有记录损坏时从 1 重计（韧性兜底分支），而非崩溃或继承脏值。"""
        uid = "t::corrupt"
        _inject_corrupted_prev(backend, uid)
        assert backend.commit_job_failure(uid, {"error": "boom"}) is True
        entry = backend.load_failed()[uid]
        assert entry["_attempt"] == 1, "损坏既有记录必须从 1 重新计数"
        assert json.loads(json.dumps(entry))["error"] == "boom"
