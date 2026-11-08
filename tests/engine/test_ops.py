"""v2 OpsConsole 运维控制台测试：失败档案管理面与状态/挂起视图。

覆盖：list_failures（失败档案只读面，含损坏行防御与 payload 快照）、
clear_history（显式单 job 历史删除通道）、list_suspensions（meta 真相源视图）、
filter_jobs_without_success（successes 过滤辅助）与换库重绑定一致性。
规则：全部管理 API 仅限 run() 外调用。
"""

from __future__ import annotations

import time

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.errorclass import (
    ERR_DEPENDENCY_DEADLOCK,
    ERR_DISPATCH_FAILURE,
    ErrorClassifier,
)
from tasklite.engine.ops import OpsConsole, SuspendEntry
from tasklite.engine.resource import ENGINE_METADATA_RESOURCE_SUSPENSIONS
from tasklite.engine.store import StateStore
from tasklite.models.attempt import AttemptRecord
from tasklite.models.job import Job
from tasklite.utils.jsonutil import dumps


def _make_console(backend: InMemoryStateBackend | None = None):
    backend = backend if backend is not None else InMemoryStateBackend()
    store = StateStore(backend)
    return backend, store, OpsConsole(store, ErrorClassifier())


def _ok_handler(job, ctx):
    """占位 handler（模块级，仅作 filter_jobs_without_success 过滤面参考）。"""
    return True


class TestListFailures:
    """失败档案只读查询。"""

    def test_structured_entries_with_payload_snapshot(self):
        backend, _, console = _make_console()
        backend.commit_job_failure(
            "t::a", {"error_message": ERR_DEPENDENCY_DEADLOCK}, payload={"k": 1}
        )
        backend.commit_job_failure(
            "t::b", {"error_message": "boom", "is_fatal": True}, payload={"k": 2}
        )

        entries = console.list_failures()
        by_uid = {e.job_uid: e for e in entries}
        assert set(by_uid) == {"t::a", "t::b"}
        assert by_uid["t::a"].error_message == ERR_DEPENDENCY_DEADLOCK
        assert by_uid["t::a"].metadata["error_category"]
        assert by_uid["t::a"].payload == {"k": 1}
        assert by_uid["t::b"].metadata["is_fatal"] is True
        assert by_uid["t::b"].payload == {"k": 2}

    def test_empty_archive_returns_empty(self):
        _, _, console = _make_console()
        assert console.list_failures() == []

    def test_entries_sorted_by_uid(self):
        backend, _, console = _make_console()
        backend.append_failed("t::z", {"error_message": "x"})
        backend.append_failed("t::a", {"error_message": "x"})
        assert [e.job_uid for e in console.list_failures()] == ["t::a", "t::z"]

    def test_bare_row_gains_classifier_error_type(self):
        """带外登记的裸行（无 error_type）在查询视图经 classifier 归档，
        且只读查询不回写持久层。"""
        backend, _, console = _make_console()
        backend.append_failed("t::bare", {"error_message": ERR_DISPATCH_FAILURE})
        assert "error_category" not in backend.load_failed()["t::bare"]

        entry = console.list_failures()[0]

        assert entry.metadata["error_category"], "查询视图必须补齐归档 error_type"
        assert "error_category" not in backend.load_failed()["t::bare"]

    def test_corrupt_meta_row_does_not_crash_query(self):
        """非 dict 损坏行归空视图（unknown 归档），不打断整条查询。"""
        backend, _, console = _make_console()
        backend.append_failed("t::ok", {"error_message": "x"})
        backend._failed["t::corrupt"] = "boom"  # 直接注入损坏行
        entries = {e.job_uid: e for e in console.list_failures()}
        assert set(entries) == {"t::ok", "t::corrupt"}
        assert entries["t::corrupt"].metadata["error_category"] == "unknown"
        assert entries["t::corrupt"].error_message == ""

    def test_no_snapshot_entry_payload_none(self):
        backend, _, console = _make_console()
        backend.append_failed("t::y", {"error_message": "x"})
        assert console.list_failures()[0].payload is None


class TestClearHistory:
    """clear_history 显式单 job 历史记录删除（successes, failures, attempts）。"""

    def test_clear_history_successes_and_failures(self):
        backend, _, console = _make_console()
        backend.apply_manual_status([("t::a", "succeeded", {"task_type": "t", "job_id": "a"}, None)])
        backend.append_failed("t::b", {"error_message": "boom"})

        # 只删 t::a 的 successes
        assert console.clear_history("t::a", records=["successes"]) == 1
        assert "t::a" not in backend.load_successes()

        # 只删 t::b 的 failures
        assert console.clear_history("t::b", records=["failures"]) == 1
        assert "t::b" not in backend.load_failed()

    def test_clear_history_attempts(self):
        backend, _, console = _make_console()
        aid = backend.append_attempt(AttemptRecord(
            job_uid="t::a",
            activation_no=1,
            attempt_no=1,
            execution_id="run_1.1",
            run_id="run_1",
            started_at="2026-05-01T00:00:00+00:00",
            outcome="running",
        ))
        assert len(backend.load_attempts("t::a")) == 1
        assert console.clear_history("t::a", records=["attempts"]) == 1
        assert len(backend.load_attempts("t::a")) == 0

    def test_clear_history_non_existent_returns_zero(self):
        _, _, console = _make_console()
        assert console.clear_history("t::non_existent", records=["successes", "failures", "attempts"]) == 0

    def test_clear_history_does_not_affect_queue_or_cursors(self):
        backend, store, console = _make_console()
        store.enqueue_jobs([Job("t", "stay_queued")])
        store.set_cursor("cur", "val")
        backend.apply_manual_status([("t::q", "succeeded", {"task_type": "t", "job_id": "q"}, None)])

        deleted = console.clear_history("t::q", records=["successes"])
        assert deleted == 1
        # queue 和 cursors 保持不变
        assert any(j["job_id"] == "stay_queued" for j in backend.load_queue())
        assert store.get_cursor("cur") == "val"

    def test_clear_history_records_validation(self):
        _, _, console = _make_console()
        with pytest.raises(ValueError, match="unknown target"):
            console.clear_history("t::a", records=["unknown"])  # type: ignore[list-item]

        with pytest.raises(ValueError, match="records must not be empty"):
            console.clear_history("t::a", records=[])

    def test_clear_history_uid_validation(self):
        _, _, console = _make_console()
        with pytest.raises(TypeError, match="uid"):
            console.clear_history(123, records=["successes"])  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="uid"):
            console.clear_history("", records=["successes"])
        with pytest.raises(ValueError, match="uid"):
            console.clear_history("bad_uid_no_colon", records=["successes"])


class TestStoreCursor:
    """Store 游标读写校验。"""

    def test_store_cursor_readable_via_backend(self):
        backend, store, _ = _make_console()
        store.set_cursor("progress", "2026-01-01")
        assert backend.load_cursors()["progress"] == "2026-01-01"
        assert store.get_cursor("progress") == "2026-01-01"
        assert store.state.cursors["progress"] == "2026-01-01"

    def test_store_cursor_rejects_bad_shapes(self):
        _, store, _ = _make_console()
        with pytest.raises(TypeError, match="cursor key"):
            store.set_cursor("", "v")
        with pytest.raises(TypeError, match="cursor value"):
            store.set_cursor("k", 42)  # type: ignore[arg-type]


class TestListSuspensions:
    """资源挂起只读视图（engine_metadata 表为唯一真相源）。"""

    def _seed_suspensions(self, backend, deadlines):
        backend.set_engine_metadata(ENGINE_METADATA_RESOURCE_SUSPENSIONS, dumps(deadlines))

    def test_empty_meta_returns_empty(self):
        _, _, console = _make_console()
        assert console.list_suspensions() == []

    def test_active_suspension_reported_with_remaining(self):
        backend, _, console = _make_console()
        deadline = time.time() + 3600.0
        self._seed_suspensions(backend, {"ieee_api": deadline})
        entries = console.list_suspensions()
        assert len(entries) == 1
        entry = entries[0]
        assert isinstance(entry, SuspendEntry)
        assert entry.resource == "ieee_api"
        assert entry.resume_at == pytest.approx(deadline)
        assert entry.remaining_seconds == pytest.approx(3600.0, abs=5.0)

    def test_expired_suspension_filtered_out(self):
        backend, _, console = _make_console()
        self._seed_suspensions(backend, {"ieee_api": time.time() - 1.0})
        assert console.list_suspensions() == []

    def test_entries_sorted_by_resume_at_ascending(self):
        backend, _, console = _make_console()
        now = time.time()
        self._seed_suspensions(backend, {
            "slow_host": now + 7200.0,
            "ieee_api": now + 3600.0,
        })
        assert [e.resource for e in console.list_suspensions()] == [
            "ieee_api", "slow_host",
        ]

    def test_corrupt_meta_degrades_to_empty(self):
        backend, _, console = _make_console()
        backend.set_engine_metadata(ENGINE_METADATA_RESOURCE_SUSPENSIONS, "not-json{")
        assert console.list_suspensions() == []
        backend.set_engine_metadata(ENGINE_METADATA_RESOURCE_SUSPENSIONS, dumps(["list", "not", "dict"]))
        assert console.list_suspensions() == []

    def test_non_numeric_deadline_skipped(self):
        backend, _, console = _make_console()
        now = time.time()
        self._seed_suspensions(backend, {
            "bad": "soon", "good": now + 60.0,
        })
        entries = console.list_suspensions()
        assert [e.resource for e in entries] == ["good"]

    def test_backend_read_failure_degrades_to_empty(self):
        backend, _, console = _make_console()

        def _boom(_key):
            raise OSError(5, "disk gone")

        backend.get_engine_metadata = _boom  # type: ignore[method-assign]
        assert console.list_suspensions() == []


class TestFilterJobsWithoutSuccess:
    """入队前 successes 过滤辅助。"""

    def test_filters_by_wall_only(self):
        backend, _, console = _make_console()
        backend.apply_manual_status([
            ("t::done", "succeeded", {"task_type": "t", "job_id": "done"}, None),
            ("t::failed_then_done", "succeeded", {"task_type": "t", "job_id": "failed_then_done"}, None),
        ])
        backend.append_failed("t::failed", {"error_message": "x"})
        backend.enqueue_jobs([Job("t", "queued").to_dict()])
        jobs = [
            Job("t", "done"), Job("t", "failed"), Job("t", "queued"), Job("t", "fresh"),
        ]
        remaining = console.filter_jobs_without_success(jobs)
        assert [j.uid for j in remaining] == ["t::failed", "t::queued", "t::fresh"]

    def test_empty_wall_returns_all_in_order(self):
        _, _, console = _make_console()
        jobs = [Job("t", "b"), Job("t", "a")]
        assert console.filter_jobs_without_success(jobs) == jobs


class TestBackendSwapRebind:
    """换库后管理面目标库一致性（锚点唯一收敛在 StateStore）。"""

    def test_management_apis_target_rebound_backend(self):
        old_backend, store, console = _make_console()
        old_backend.apply_manual_status([("w::old", "succeeded", {"task_type": "w", "job_id": "old"}, None)])
        old_backend.append_failed("a::old", {"error_message": "old"})
        old_backend.set_cursor("ck", "old")

        new_backend = InMemoryStateBackend()
        new_backend.append_failed("a::new", {"error_message": "new"})
        # 换库唯一动作：换 StateStore 锚点；OpsConsole 不独立持库，
        # 管理面经 store.backend 派生自然跟随新库。
        store.set_backend(new_backend)

        # 只读面读新库
        assert [e.job_uid for e in console.list_failures()] == ["a::new"]
        # 变更面写新库、旧库不受波及
        assert console.clear_history("a::new", records=["failures"]) == 1
        assert new_backend.load_failed() == {}
        assert "a::old" in old_backend.load_failed()
        store.set_job_status(Job("t", "new"), "succeeded", recursive=False)
        assert "t::new" in new_backend.load_successes()
        assert "t::new" not in old_backend.load_successes()
        store.set_cursor("k", "new")
        assert new_backend.load_cursors()["k"] == "new"
        assert old_backend.load_cursors()["ck"] == "old"
