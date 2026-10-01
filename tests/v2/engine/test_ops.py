"""v2 OpsConsole 运维控制台测试：失败档案管理面与种子化/挂起视图。

覆盖：list_failures / clear_failures / retry_failure（失败档案运维面，
含损坏行防御与 payload 快照）、clear_history（wall/failed 删除通道与
谓词）、seed_wall / seed_cursor（含六集合互斥零写入拒绝）、
list_suspensions（meta 真相源视图）、uncompleted（wall 过滤辅助）与
换库重绑定一致性。规则：全部管理 API 仅限 run() 外调用。
"""

from __future__ import annotations

import time

import pytest

from tasklite.v2.backend.memory import InMemoryStateBackend
from tasklite.v2.engine.errorclass import (
    ERR_DEPENDENCY_DEADLOCK,
    ERR_DISPATCH_FAILURE,
    ErrorClassifier,
)
from tasklite.v2.engine.ops import OpsConsole, SuspendEntry
from tasklite.v2.engine.resource import META_RESOURCE_SUSPENSIONS
from tasklite.v2.engine.store import StateStore
from tasklite.v2.models.attempt import AttemptRecord
from tasklite.v2.models.job import Job
from tasklite.v2.utils.jsonutil import dumps


def _make_console(backend: InMemoryStateBackend | None = None):
    backend = backend if backend is not None else InMemoryStateBackend()
    store = StateStore(backend)
    return backend, store, OpsConsole(backend, store, ErrorClassifier())


def _ok_handler(job, ctx):
    """占位 handler（模块级，仅作 uncompleted 过滤面参考）。"""
    return True


class TestListFailures:
    """失败档案只读查询。"""

    def test_structured_entries_with_payload_snapshot(self):
        backend, _, console = _make_console()
        backend.commit_job_failure(
            "t::a", {"error": ERR_DEPENDENCY_DEADLOCK}, job_payload={"k": 1}
        )
        backend.commit_job_failure(
            "t::b", {"error": "boom", "fatal": True}, job_payload={"k": 2}
        )

        entries = console.list_failures()
        by_uid = {e.uid: e for e in entries}
        assert set(by_uid) == {"t::a", "t::b"}
        assert by_uid["t::a"].error == ERR_DEPENDENCY_DEADLOCK
        assert by_uid["t::a"].meta["error_type"]
        assert by_uid["t::a"].job_payload == {"k": 1}
        assert by_uid["t::b"].meta["fatal"] is True
        assert by_uid["t::b"].job_payload == {"k": 2}

    def test_empty_archive_returns_empty(self):
        _, _, console = _make_console()
        assert console.list_failures() == []

    def test_entries_sorted_by_uid(self):
        backend, _, console = _make_console()
        backend.append_failed("t::z", {"error": "x"})
        backend.append_failed("t::a", {"error": "x"})
        assert [e.uid for e in console.list_failures()] == ["t::a", "t::z"]

    def test_bare_row_gains_classifier_error_type(self):
        """带外登记的裸行（无 error_type）在查询视图经 classifier 归档，
        且只读查询不回写持久层。"""
        backend, _, console = _make_console()
        backend.append_failed("t::bare", {"error": ERR_DISPATCH_FAILURE})
        assert "error_type" not in backend.load_failed()["t::bare"]

        entry = console.list_failures()[0]

        assert entry.meta["error_type"], "查询视图必须补齐归档 error_type"
        assert "error_type" not in backend.load_failed()["t::bare"]

    def test_corrupt_meta_row_does_not_crash_query(self):
        """非 dict 损坏行归空视图（unknown 归档），不打断整条查询。"""
        backend, _, console = _make_console()
        backend.append_failed("t::ok", {"error": "x"})
        backend._failed["t::corrupt"] = "boom"  # 直接注入损坏行
        entries = {e.uid: e for e in console.list_failures()}
        assert set(entries) == {"t::ok", "t::corrupt"}
        assert entries["t::corrupt"].meta["error_type"] == "unknown"
        assert entries["t::corrupt"].error == ""

    def test_no_snapshot_entry_payload_none(self):
        backend, _, console = _make_console()
        backend.append_failed("t::y", {"error": "x"})
        assert console.list_failures()[0].job_payload is None


class TestClearFailures:
    """失败档案删除面（默认保留 fatal）。"""

    def _seed(self, backend):
        backend.append_failed("dl::a", {"error": "boom", "fatal": True})
        backend.append_failed("dl::b", {"error": "net", "fatal": False})
        backend.append_failed("other::c", {"error": "x"})

    def test_keeps_fatal_by_default(self):
        backend, store, console = _make_console()
        self._seed(backend)
        # 预置内存镜像（run 外查询前内存 failed 为空占位）
        store.state.mark_failed("dl::b", {"error": "net"})
        assert console.clear_failures() == 2  # b、c 清除，fatal 的 a 保留
        assert set(backend.load_failed()) == {"dl::a"}
        assert "dl::b" not in store.state.failed

    def test_keep_fatal_false_deletes_all(self):
        backend, _, console = _make_console()
        self._seed(backend)
        assert console.clear_failures(keep_fatal=False) == 3
        assert backend.load_failed() == {}

    def test_task_types_filter(self):
        backend, _, console = _make_console()
        self._seed(backend)
        assert console.clear_failures(task_types=["dl"]) == 1  # 只 dl::b
        assert set(backend.load_failed()) == {"dl::a", "other::c"}

    def test_no_match_returns_zero(self):
        backend, _, console = _make_console()
        self._seed(backend)
        assert console.clear_failures(task_types=["nope"]) == 0

    def test_invalid_task_types_rejected(self):
        _, _, console = _make_console()
        with pytest.raises(TypeError, match="task_types"):
            console.clear_failures(task_types="dl")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="task_types"):
            console.clear_failures(task_types=["", "dl"])

    def test_clear_then_enqueue_reruns(self):
        """清除语义：删档案后驻留/新入队作业不再被 failed 残留过滤。"""
        backend, store, console = _make_console()
        backend.append_failed("t::x", {"error": "net"})
        store.enqueue_jobs([Job("t", "x", payload={})])
        assert console.clear_failures() == 1

        assert backend.load_failed() == {}
        queue = backend.load_queue()
        assert [jd.get("job_id") for jd in queue] == ["x"], \
            "清除后驻留队列的作业成为可运行状态"


class TestRetryFailure:
    """失败档案人工补跑面（v2 新增）。"""

    def test_retry_reenqueues_from_payload_snapshot(self):
        backend, store, console = _make_console()
        backend.commit_job_failure(
            "t::x", {"error": "boom"}, job_payload={"artist": "a"}
        )
        assert console.retry_failure("t::x") is True

        assert backend.load_failed() == {}, "补跑必须移出失败档案"
        queue = backend.load_queue()
        assert len(queue) == 1
        jd = queue[0]
        assert (jd["task_type"], jd["job_id"]) == ("t", "x")
        assert jd["payload"] == {"artist": "a"}
        # 无轨迹行（带外登记）→ 初激活 1；attempt_no 归 1（新激活首次尝试）
        assert jd["attempt_no"] == 1
        assert jd["activation_no"] == 1
        assert jd["first_enqueued_at"], "enqueue 必须填充首入队时间"
        assert "__workers__" in jd["resources"], "enqueue 必须注入工人资源"
        assert "t::x" not in store.state.failed

    def test_retry_advances_activation_from_attempt_trace(self):
        """轨迹有更高激活代时补跑必须 +1 推进——补跑执行与既有激活在

        attempts 表的 (activation_no, attempt_no) 逻辑键撞号会击穿
        「一行 = 一次物理执行」的追溯链。
        """
        backend, _, console = _make_console()
        backend.commit_job_failure("t::x", {"error": "boom"}, job_payload={"k": 1})
        # 轨迹行按真实派发协议落表：先插 running 行，再收尾为 failed
        attempt_id = backend.append_attempt(AttemptRecord(
            job_uid="t::x",
            activation_no=2,
            attempt_no=1,
            incarnation="run_a.1",
            run_id="run_a",
            started_at="2026-09-01T00:00:00+00:00",
            outcome="running",
        ))
        backend.update_attempt(
            attempt_id,
            outcome="failed",
            finished_at="2026-09-01T00:00:05+00:00",
            error="boom",
        )
        assert console.retry_failure("t::x") is True
        jd = backend.load_queue()[0]
        assert jd["activation_no"] == 3, "补跑 = failed 拦截点放行，激活代按轨迹 +1"
        assert jd["attempt_no"] == 1

    def test_retry_missing_uid_raises(self):
        _, _, console = _make_console()
        with pytest.raises(KeyError, match="not in failure archive"):
            console.retry_failure("t::typo")

    def test_retry_without_payload_uses_empty_payload(self):
        backend, _, console = _make_console()
        backend.append_failed("t::y", {"error": "x"})  # 无快照
        assert console.retry_failure("t::y") is True
        assert backend.load_queue()[0]["payload"] == {}

    def test_retry_idempotent_against_queue_residence(self):
        """uid 已驻留队列：不重复入队，档案行照删。"""
        backend, store, console = _make_console()
        backend.commit_job_failure("t::z", {"error": "x"}, job_payload={"k": 1})
        store.enqueue_jobs([Job("t", "z", payload={"k": 1})])
        assert console.retry_failure("t::z") is True
        assert len(backend.load_queue()) == 1
        assert backend.load_failed() == {}


class TestClearHistory:
    """wall / failed 删除通道。"""

    def test_forget_both_wall_and_failed(self):
        backend, _, console = _make_console()
        backend.seed_wall(["t::a", "t::b", "other::c"])
        backend.append_failed("t::b", {"error": "x"})
        backend.append_failed("t::d", {"error": "x"})

        assert console.clear_history("t::") == 4  # wall 2 + failed 2
        assert backend.load_wall() == {"other::c": {}}
        assert backend.load_failed() == {}

    def test_forget_exact_uid(self):
        backend, _, console = _make_console()
        backend.seed_wall(["t::a", "t::b"])
        assert console.clear_history("t::a") == 1
        assert set(backend.load_wall()) == {"t::b"}

    def test_forget_only_wall(self):
        backend, _, console = _make_console()
        backend.seed_wall(["t::a"])
        backend.append_failed("t::a", {"error": "x"})
        assert console.clear_history("t::a", where=("wall",)) == 1
        assert backend.load_wall() == {}
        assert set(backend.load_failed()) == {"t::a"}

    def test_prefix_requires_double_colon(self):
        """防前缀误匹配：无 :: 结尾的 pattern 只精确匹配完整 uid。"""
        backend, _, console = _make_console()
        backend.seed_wall(["download::x", "downloads::y"])
        assert console.clear_history("download") == 0
        assert set(backend.load_wall()) == {"download::x", "downloads::y"}
        assert console.clear_history("download::") == 1
        assert set(backend.load_wall()) == {"downloads::y"}

    def test_forget_uid_list(self):
        backend, _, console = _make_console()
        backend.seed_wall(["t::a", "t::b", "t::c"])
        assert console.clear_history(["t::a", "t::c"]) == 2
        assert set(backend.load_wall()) == {"t::b"}

    def test_unknown_where_rejected(self):
        _, _, console = _make_console()
        with pytest.raises(ValueError, match="where"):
            console.clear_history("t::", where=("wall", "queue"))  # type: ignore[arg-type]

    def test_invalid_targets_rejected(self):
        _, _, console = _make_console()
        with pytest.raises(TypeError, match="targets"):
            console.clear_history(42)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="targets"):
            console.clear_history(["t::a", 7])  # type: ignore[list-item]


class TestPredicateClearHistory:
    """谓词在 targets 命中集内收敛删除面。"""

    def test_predicate_filters_within_target_prefix(self):
        backend, _, console = _make_console()
        backend.seed_wall(["t::a", "t::b", "other::c"])
        assert console.clear_history("t::", predicate=lambda u: u == "t::a") == 1
        assert set(backend.load_wall()) == {"t::b", "other::c"}

    def test_predicate_applies_to_wall_and_failed(self):
        backend, _, console = _make_console()
        backend.seed_wall(["t::keep", "t::drop"])
        backend.append_failed("t::keep", {"error": "x"})
        backend.append_failed("t::drop", {"error": "x"})
        assert console.clear_history("t::", predicate=lambda u: "drop" in u) == 2
        assert set(backend.load_wall()) == {"t::keep"}
        assert set(backend.load_failed()) == {"t::keep"}

    def test_predicate_receives_matched_uids(self):
        backend, _, console = _make_console()
        backend.seed_wall(["t::a", "t::b"])
        seen: list[str] = []
        console.clear_history("t::", predicate=seen.append)
        assert sorted(seen) == ["t::a", "t::b"]

    def test_predicate_rejects_non_callable(self):
        _, _, console = _make_console()
        with pytest.raises(TypeError, match="predicate"):
            console.clear_history("t::", predicate="t::a")  # type: ignore[arg-type]


class TestSeeding:
    """种子化 API（存档迁移语义）。"""

    def test_seed_wall_makes_known(self):
        backend, _, console = _make_console()
        assert console.seed_wall(["t::archived_1", "t::archived_2"]) == 2
        assert backend.load_wall() == {
            "t::archived_1": {}, "t::archived_2": {},
        }

    def test_seed_wall_rejects_bad_uid(self):
        _, _, console = _make_console()
        with pytest.raises(ValueError, match="task_type::job_id"):
            console.seed_wall(["no-colon"])

    def test_seed_wall_rejects_uid_already_in_failed(self):
        """seed 已 failed 的 uid 被整体拒绝（wall/failed 全局互斥契约）。

        seed 链路（console 预检 → backend 持久层 → 内存 state）双腿零写入：
        静默覆盖会留下 wall∩failed 重叠，令后续派发的状态一致性断言崩溃；
        失败档案记录须先显式清除，不静默吞失败历史。
        """
        backend, store, console = _make_console()
        backend.append_failed("t::archived_1", {"error": "boom"})

        with pytest.raises(ValueError, match="already in the failure archive"):
            console.seed_wall(["t::archived_1", "t::fresh"])

        # 双腿零写入：持久层与内存 state 均不含种子，failed 记录原样保留
        assert backend.load_wall() == {}
        assert set(backend.load_failed()) == {"t::archived_1"}
        assert "t::archived_1" not in store.state.wall
        assert "t::fresh" not in store.state.wall

    def test_seed_wall_rejects_uid_resident_in_queue(self):
        """队列驻留（磁盘真相）同样拒绝种子——仅查内存腿会漏掉。"""
        backend, store, console = _make_console()
        store.enqueue_jobs([Job("t", "queued", payload={})])
        with pytest.raises(ValueError, match="still resident in the queue"):
            console.seed_wall(["t::queued"])

    def test_seed_wall_after_clear_failures_succeeds(self):
        """先显式清除失败档案再种子：合法存档迁移路径不受拒绝语义波及。"""
        backend, _, console = _make_console()
        backend.append_failed("t::archived_1", {"error": "boom"})
        assert console.clear_failures() == 1
        assert console.seed_wall(["t::archived_1"]) == 1
        assert backend.load_wall()["t::archived_1"] == {}
        assert set(backend.load_failed()) == set()

    def test_seed_cursor_readable_via_backend(self):
        backend, store, console = _make_console()
        console.seed_cursor("progress", "2026-01-01")
        assert backend.load_cursors()["progress"] == "2026-01-01"
        assert store.state.cursors["progress"] == "2026-01-01"

    def test_seed_cursor_rejects_bad_shapes(self):
        _, _, console = _make_console()
        with pytest.raises(TypeError, match="cursor key"):
            console.seed_cursor("", "v")
        with pytest.raises(TypeError, match="cursor value"):
            console.seed_cursor("k", 42)  # type: ignore[arg-type]


class TestListSuspensions:
    """资源挂起只读视图（meta 表为唯一真相源）。"""

    def _seed_suspensions(self, backend, deadlines):
        backend.set_meta(META_RESOURCE_SUSPENSIONS, dumps(deadlines))

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
        backend.set_meta(META_RESOURCE_SUSPENSIONS, "not-json{")
        assert console.list_suspensions() == []
        backend.set_meta(META_RESOURCE_SUSPENSIONS, dumps(["list", "not", "dict"]))
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

        backend.get_meta = _boom  # type: ignore[method-assign]
        assert console.list_suspensions() == []


class TestUncompleted:
    """入队前 wall 过滤辅助。"""

    def test_filters_by_wall_only(self):
        backend, _, console = _make_console()
        backend.seed_wall(["t::done", "t::failed_then_done"])
        backend.append_failed("t::failed", {"error": "x"})
        jobs = [
            Job("t", "done"), Job("t", "failed"), Job("t", "fresh"),
        ]
        remaining = console.uncompleted(jobs)
        assert [j.uid for j in remaining] == ["t::failed", "t::fresh"]

    def test_empty_wall_returns_all_in_order(self):
        _, _, console = _make_console()
        jobs = [Job("t", "b"), Job("t", "a")]
        assert console.uncompleted(jobs) == jobs


class TestBackendSwapRebind:
    """换库后管理面目标库一致性（set_backend 重绑定）。"""

    def test_management_apis_target_rebound_backend(self):
        old_backend, _, console = _make_console()
        old_backend.seed_wall(["w::old"])
        old_backend.append_failed("a::old", {"error": "old"})
        old_backend.seed_cursor("ck", "old")

        new_backend = InMemoryStateBackend()
        new_backend.append_failed("a::new", {"error": "new"})
        console.set_backend(new_backend)

        # 只读面读新库
        assert [e.uid for e in console.list_failures()] == ["a::new"]
        # 变更面写新库、旧库不受波及
        assert console.clear_failures() == 1
        assert new_backend.load_failed() == {}
        assert "a::old" in old_backend.load_failed()
        assert console.seed_wall(["t::new"]) == 1
        assert "t::new" in new_backend.load_wall()
        assert "t::new" not in old_backend.load_wall()
        console.seed_cursor("k", "new")
        assert new_backend.load_cursors()["k"] == "new"
        assert old_backend.load_cursors()["ck"] == "old"
