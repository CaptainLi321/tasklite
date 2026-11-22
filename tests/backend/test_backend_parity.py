"""SQLite 与 Memory 双后端等价契约与抽象接口锁定测试。

收敛原 test_base_contract.py、test_backend_parity.py、test_memory_backend.py、
test_authorization_store.py 与 test_state_primitives.py 中的双后端等价部分：
- 抽象契约集合锁定、替换集门卫与持久化分层红线；
- 核心队列 CRUD、四态结算互斥、事务原子性与失败回滚；
- attempt_no 单调推进（独立于档案移除）；
- 游标校验、快照隔离与路径授权存储/基准规范化。
"""
from __future__ import annotations

import ast
import copy
import pathlib
from pathlib import Path
from typing import Any

import pytest

from tasklite import TaskLiteError, parse_job_uid
from tasklite.backend import (
    AbstractStateBackend,
    GraphEditCommit,
    InMemoryStateBackend,
    RunningAttemptRecovery,
    SQLiteStateBackend,
)
from tasklite.backend.base import QUEUE_EXECUTION_ID_KEY, validate_queue_replacement
from tasklite.engine.store import StateStore
from tasklite.models.job import Job
from tasklite.models.state import job_uid_from_snapshot
from tasklite.testing import authorizer

BACKEND_PACKAGE_DIR = pathlib.Path(__file__).resolve().parents[2] / "tasklite" / "backend"

EXPECTED_ABSTRACT_METHODS = {
    "exclusive_access",
    "load_successes",
    "load_failed",
    "commit_job_failures",
    "commit_skip",
    "enqueue_jobs",
    "load_consistent_snapshot",
    "commit_graph_edit",
    "get_cursor",
    "set_cursor",
    "load_latest_succeeded_finished_at",
    "set_engine_metadata",
    "mark_job_running",
    "commit_job_settlement",
    "load_queue_jobs",
    "grant_authorization",
    "record_authorization",
    "list_authorizations",
    "is_path_authorized",
    "revoke_authorization",
}


def _pending_queue(backend: AbstractStateBackend) -> list[dict[str, Any]]:
    return [row["job"] for row in backend.load_queue_jobs(status="pending")]


def _failure_payloads(backend: AbstractStateBackend) -> dict[str, dict[str, Any]]:
    return backend.load_consistent_snapshot()["failure_payloads"]


def _cursors(backend: AbstractStateBackend) -> dict[str, str]:
    return backend.load_consistent_snapshot()["cursors"]


def _attempt_sequence(backend: AbstractStateBackend, job_uid: str) -> int:
    return backend.load_consistent_snapshot()["job_sequences"].get(job_uid, 0)


def _settled_attempts(backend: AbstractStateBackend, job_uid: str) -> list[dict[str, Any]]:
    attempts = [
        copy.deepcopy(att)
        for att in backend.load_consistent_snapshot()["attempts"]
        if att.get("job_uid") == job_uid
    ]
    attempts.sort(key=lambda item: item.get("attempt_no", 0))
    return attempts


def _job_snapshot(job_id: str, **overrides: Any) -> dict[str, Any]:
    data = Job("t", job_id).to_dict()
    data.update(overrides)
    return data


def _settle_success(
    backend: AbstractStateBackend,
    job_uid: str,
    result_metadata: dict[str, Any],
    *,
    attempt_no: int = 1,
    settled_at: str = "2026-05-01T00:00:05+00:00",
    **kwargs: Any,
) -> None:
    snapshot = next(
        (job for job in _pending_queue(backend) if job_uid_from_snapshot(job) == job_uid), None
    )
    if snapshot is None:
        task_type, job_id = parse_job_uid(job_uid)
        snapshot = Job(task_type, job_id).to_dict()
        backend.enqueue_jobs([snapshot])
    sequence = backend.mark_job_running(
        job_uid,
        execution_id=f"run_seed.{attempt_no}",
        run_id="run_seed",
        started_at="2026-05-01T00:00:00+00:00",
    )
    backend.commit_job_settlement(
        job_uid=job_uid,
        outcome="success",
        attempt_no=sequence,
        execution_id=f"run_seed.{attempt_no}",
        run_id="run_seed",
        settled_at=settled_at,
        started_at="2026-05-01T00:00:00+00:00",
        job_snapshot=snapshot,
        success_metadata=result_metadata,
        **kwargs,
    )


def _queue_uids(backend: AbstractStateBackend) -> list[str]:
    return [job_uid_from_snapshot(j) for j in _pending_queue(backend)]


def _snapshot(backend: AbstractStateBackend) -> dict[str, Any]:
    """后端可观察状态快照，用于故障原子性比对。"""
    consistent = backend.load_consistent_snapshot()
    return {
        "queue": _queue_uids(backend),
        "wall": consistent["successes"],
        "failed": consistent["failed"],
        "failure_payloads": consistent["failure_payloads"],
        "cursors": consistent["cursors"],
    }


def _state_facts(backend: AbstractStateBackend) -> dict[str, Any]:
    """提取后端全部可观测事实，用于双后端等价性核对。"""
    consistent = backend.load_consistent_snapshot()
    return {
        "queue": consistent["queue_jobs"],
        "successes": consistent["successes"],
        "failed": consistent["failed"],
        "failure_payloads": consistent["failure_payloads"],
        "cursors": consistent["cursors"],
    }


@pytest.fixture(params=["memory", "sqlite"])
def dual_backend(request, tmp_path):
    """同一断言集作用于 memory 与 sqlite 两腿，构成双后端行为对齐证明。"""
    if request.param == "memory":
        backend: AbstractStateBackend = InMemoryStateBackend()
    else:
        backend = SQLiteStateBackend(tmp_path / "parity_state.db")
    try:
        yield backend
    finally:
        close_fn = getattr(backend, "close", None)
        if callable(close_fn):
            close_fn()


# ── 抽象契约锁定与分层守卫 ──────────────────────────────────────────


class TestAbstractContractLock:
    """抽象方法集合锁定、不可实例化与替换集校验。"""

    def test_abstract_method_set_locked_and_non_instantiable(self):
        assert set(AbstractStateBackend.__abstractmethods__) == EXPECTED_ABSTRACT_METHODS
        with pytest.raises(TypeError):
            AbstractStateBackend()  # type: ignore[abstract]

        class HalfBackend(AbstractStateBackend):
            def load_successes(self):
                return {}

        with pytest.raises(TypeError):
            HalfBackend()  # type: ignore[abstract]
        assert "commit_job_settlement" in HalfBackend.__abstractmethods__

    def test_validate_queue_replacement(self):
        validate_queue_replacement([])
        validate_queue_replacement([{"task_type": "t", "job_id": "a"}])
        validate_queue_replacement(({"task_type": "t", "job_id": "a"},))

        for bad in (None, 42, "jobs", {"task_type": "t"}, {"a": 1}):
            with pytest.raises(TypeError, match="queue replacement must be a list"):
                validate_queue_replacement(bad)
        with pytest.raises(TypeError, match="queue replacement item #1 must be a job dict"):
            validate_queue_replacement([{"task_type": "t", "job_id": "a"}, "not-a-dict"])

    def test_backend_sources_respect_layering(self):
        assert BACKEND_PACKAGE_DIR.is_dir()
        offenders: list[str] = []
        banned_heads = ("engine", "wrappers")
        for source in sorted(BACKEND_PACKAGE_DIR.glob("*.py")):
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            roots: list[str] = []
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    roots.extend(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom):
                    roots.append("." * node.level + (node.module or ""))
            for root in roots:
                if root.startswith("."):
                    stripped = root.lstrip(".")
                    head = stripped.split(".")[0] if stripped else ""
                    if root.count(".") >= 2 and head in banned_heads:
                        offenders.append(f"{source.name}: 相对 import 越层 `{root}`")
                else:
                    head = root.split(".")[1] if root.startswith("tasklite.") and root.count(".") >= 1 else ""
                    if root.startswith("tasklite.") and head in banned_heads:
                        offenders.append(f"{source.name}: 核心层反向依赖 `{root}`")
        assert not offenders, "tasklite/backend 分层违例: " + "; ".join(offenders)


# ── 核心队列、终态互斥与档案隔离 ──────────────────────────────────────


class TestCoreContractProperties:
    """双后端核心读写、队列保序、终态互斥、元数据与快照隔离。"""

    def test_initial_state_empty(self, dual_backend):
        b = dual_backend
        assert b.load_successes() == {}
        assert b.load_failed() == {}
        assert _failure_payloads(b) == {}
        assert _cursors(b) == {}
        assert _pending_queue(b) == []
        assert _settled_attempts(b, "t::missing") == []

    def test_enqueue_visible_idempotent_and_ordered(self, dual_backend):
        b = dual_backend
        assert b.enqueue_jobs(job_snapshots=[_job_snapshot("one"), _job_snapshot("two")]) == ["t::one", "t::two"]
        assert _queue_uids(b) == ["t::one", "t::two"]
        assert b.enqueue_jobs([_job_snapshot("one"), _job_snapshot("three"), _job_snapshot("three")]) == ["t::three"]
        assert _queue_uids(b) == ["t::one", "t::two", "t::three"]
        assert b.enqueue_jobs([_job_snapshot("frontal")], front=True) == ["t::frontal"]
        assert _queue_uids(b) == ["t::frontal", "t::one", "t::two", "t::three"]
        assert b.enqueue_jobs([_job_snapshot("tail")], front=False) == ["t::tail"]
        assert _queue_uids(b) == ["t::frontal", "t::one", "t::two", "t::three", "t::tail"]

    def test_popped_uid_invisible_after_terminal_commit(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot(j) for j in ("success_one", "fail_one", "bulk_one", "skip_one", "keep_one")])

        assert _settle_success(b, job_uid="t::success_one", result_metadata={"ok": True}) is None
        assert b.commit_job_failures([("t::fail_one", {"error_message": "x"})]) is None
        assert b.commit_job_failures([("t::bulk_one", {"error_message": "y"})]) is None
        assert b.commit_skip(job_uid="t::skip_one") is None
        assert b.commit_job_failures([]) is None

        assert _queue_uids(b) == ["t::keep_one"]
        assert set(b.load_successes()) == {"t::success_one"}
        assert set(b.load_failed()) == {"t::fail_one", "t::bulk_one"}

    def test_success_failure_mutually_delete_opposite_side(self, dual_backend):
        b = dual_backend
        metadata = {"error_message": "stale", "payload": "档案元数据", "nested": {"new_jobs": [1]}}
        payload = {"error": "业务输入", "metadata": "业务值", "nested": {"timeout": 9}}
        assert b.commit_job_failures([("t::one", metadata, payload)]) is None
        assert b.load_failed()["t::one"] == metadata
        assert _failure_payloads(b)["t::one"] == payload
        _settle_success(b, "t::other", {"ok": True})

        assert _settle_success(b, "t::one", {"ok": True}) is None
        assert b.load_failed() == {}
        assert _failure_payloads(b) == {}

        assert b.commit_job_failures([("t::other", {"error_message": "boom"})]) is None
        assert b.load_successes() == {"t::one": {"ok": True}}
        assert not (set(b.load_successes()) & set(b.load_failed()))

    def test_failure_archive_overwrite_empty_and_payload_retention(self, dual_backend):
        b = dual_backend
        b.commit_job_failures([("t::nil", {})])
        assert b.load_failed() == {"t::nil": {}}
        assert _failure_payloads(b) == {}

        assert b.commit_job_failures([("t::dup", {"first": 1}, {"v": 1})]) is None
        assert _failure_payloads(b) == {"t::dup": {"v": 1}}
        assert b.commit_job_failures([("t::dup", {"second": 2}, {"v": 2})]) is None
        assert b.load_failed()["t::dup"] == {"second": 2}
        assert _failure_payloads(b) == {"t::dup": {"v": 2}}
        b.commit_job_failures([("t::dup", {"third": 3})])
        assert b.load_failed()["t::dup"] == {"third": 3}
        assert _failure_payloads(b) == {"t::dup": {"v": 2}}

    def test_meta_upsert_idempotent(self, dual_backend):
        b = dual_backend
        b.set_engine_metadata("fencing", "run_seed")
        b.set_engine_metadata("fencing", "run_next")

    def test_snapshot_isolation_on_reads_and_writes(self, dual_backend):
        b = dual_backend
        job = _job_snapshot("one", payload={"x": 1})
        b.enqueue_jobs([job])
        job["payload"]["x"] = 999
        q_snap = _pending_queue(b)
        q_snap[0]["payload"]["x"] = 888
        q_snap.pop()
        assert _pending_queue(b)[0]["payload"] == {"x": 1}

        _settle_success(b, "t::one", {"meta": {"v": 1}})
        b.commit_job_failures([("t::two", {"meta": {"v": 2}})])
        b.load_successes()["t::one"]["meta"]["v"] = 999
        b.load_failed()["t::two"]["meta"]["v"] = 999
        assert b.load_successes()["t::one"]["meta"] == {"v": 1}
        assert b.load_failed()["t::two"]["meta"] == {"v": 2}


# ── 事务原子性与回滚 ──────────────────────────────────────────────────


class _UncommittableMeta:
    """令失败行落库必然失败的载荷：memory 侧 deepcopy 拒绝，SQLite 侧 JSON 序列化拒绝。"""

    def __deepcopy__(self, memo):
        raise ValueError("uncommittable meta")


class TestBackendCommitAtomicity:
    """commit_* 故障时原样抛异常且后端状态与调用前完全一致。"""

    def test_bulk_failure_write_error_leaves_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one"), _job_snapshot("two")])
        b.commit_graph_edit(
            GraphEditCommit(success_writes={"t::nine": {"job_snapshot": _job_snapshot("nine")}})
        )
        before = _snapshot(b)

        with pytest.raises((ValueError, TypeError)):
            b.commit_job_failures([
                ("t::one", {"error_message": "d_one"}),
                ("t::two", {"payload": _UncommittableMeta()}),
            ])
        assert _snapshot(b) == before

    def test_job_failure_write_error_leaves_backend_unchanged(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one")])
        before = _snapshot(b)

        with pytest.raises((ValueError, TypeError)):
            b.commit_job_failures([("t::one", {"payload": _UncommittableMeta()})])
        assert _snapshot(b) == before
        assert "t::one" in _queue_uids(b)

    def test_success_commit_with_spawn_and_cursor_applies_atomically(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("one")])
        b.commit_job_failures([("t::one", {"error_message": "stale"})])

        _settle_success(
            b,
            "t::one",
            {"status": "ok"},
            spawned_job_snapshots=[_job_snapshot("child_one"), _job_snapshot("child_two")],
            cursor_updates={"page": "two"},
        )
        assert _queue_uids(b) == ["t::child_one", "t::child_two"]
        assert b.load_successes()["t::one"]["status"] == "ok"
        assert "t::one" not in b.load_failed()
        assert _cursors(b) == {"page": "two"}


# ── 管理状态切换与游标对齐 ────────────────────────────────────────────


class TestGraphEditCommitAndCursors:
    """commit_graph_edit、load_consistent_snapshot 与 set_cursor 双腿对齐。"""

    def test_commit_graph_edit_three_tables_and_untouched_preservation(self, dual_backend):
        b = dual_backend
        snap_a = _job_snapshot("a")
        snap_b = _job_snapshot("b")
        snap_c = _job_snapshot("c")
        b.enqueue_jobs([snap_a, snap_b, snap_c])
        b.mark_job_running(
            "t::c",
            execution_id="run_keep.1",
            run_id="run_keep",
            started_at="2026-05-01T00:00:00Z",
        )

        # 仅触碰 a/b，未触碰的遗留 running 行 c 保持原状
        b.commit_graph_edit(
            GraphEditCommit(
                queue_writes={"t::a": None, "t::b": None},
                success_writes={
                    "t::a": {"job_snapshot": snap_a, "annotations": {"completion_source": "manual"}},
                },
                failed_writes={
                    "t::b": ({"job_snapshot": snap_b, "note": "测试失败"}, {"k": "v"}),
                },
            )
        )
        assert "t::a" not in _queue_uids(b)
        assert b.load_successes()["t::a"]["annotations"]["completion_source"] == "manual"
        assert b.load_failed()["t::b"]["note"] == "测试失败"
        assert _failure_payloads(b)["t::b"] == {"k": "v"}
        q_rows = b.load_queue_jobs()
        assert len(q_rows) == 1 and q_rows[0]["job_uid"] == "t::c" and q_rows[0]["status"] == "running"

        # 切回 pending 并补写遗留 running 失败 Attempt
        rec = RunningAttemptRecovery(
            job_uid="t::c",
            attempt_no=1,
            execution_id="run_keep.1",
            run_id="run_keep",
            started_at=None,
            settled_at="2026-05-01T00:01:00Z",
            job_snapshot=snap_c,
            files={},
            result={"message": "unsettled", "exception": None, "metadata": {}, "annotations": {}},
            error_message="unsettled",
        )
        b.commit_graph_edit(
            GraphEditCommit(
                queue_writes={"t::a": snap_a, "t::b": snap_b, "t::c": None},
                success_writes={"t::a": None},
                failed_writes={"t::b": None, "t::c": ({"job_snapshot": snap_c}, None)},
                recovery_attempts=(rec,),
            )
        )
        assert "t::a" not in b.load_successes()
        assert "t::b" not in b.load_failed()
        assert "t::c" in b.load_failed()
        assert _queue_uids(b) == ["t::a", "t::b"]
        atts = _settled_attempts(b, "t::c")
        assert len(atts) == 1 and atts[0]["outcome"] == "failure" and atts[0]["attempt_no"] == 1

        # load_consistent_snapshot 包含 cursors 与 job_sequences 等完整 7 字段
        b.set_cursor("c_snap", "v_snap")
        consistent = b.load_consistent_snapshot()
        assert set(consistent) == {
            "queue_jobs", "successes", "failed", "failure_payloads", "cursors", "attempts", "job_sequences",
        }
        assert consistent["cursors"] == {"c_snap": "v_snap"}
        assert consistent["job_sequences"]["t::c"] == 1

    def test_commit_graph_edit_rejects_overlap_and_rolls_back_on_fault(self, dual_backend):
        b = dual_backend
        snap = _job_snapshot("x")
        b.enqueue_jobs([snap])
        before = _state_facts(b)

        with pytest.raises(TaskLiteError, match="both successes and failed"):
            b.commit_graph_edit(
                GraphEditCommit(
                    queue_writes={"t::x": None},
                    success_writes={"t::x": {"job_snapshot": snap}},
                    failed_writes={"t::x": ({"job_snapshot": snap}, None)},
                )
            )
        assert _state_facts(b) == before

        with pytest.raises((ValueError, TypeError)):
            b.commit_graph_edit(
                GraphEditCommit(
                    queue_writes={"t::x": None},
                    success_writes={"t::x": {"bad": _UncommittableMeta()}},
                )
            )
        assert _state_facts(b) == before

    def test_backend_has_no_attempts_deletion_path(self, dual_backend):
        assert not hasattr(dual_backend, "clear_history")
        assert not hasattr(dual_backend, "apply_manual_status")
        for source in sorted(BACKEND_PACKAGE_DIR.glob("*.py")):
            text = source.read_text(encoding="utf-8")
            assert "DELETE FROM attempts" not in text

    @pytest.mark.parametrize("key,value", [(None, "v"), ("k", 123), ("", "v")])
    def test_cursor_bad_types_rejected_zero_write(self, dual_backend, key, value):
        with pytest.raises(TypeError):
            dual_backend.set_cursor(key, value)
        assert _cursors(dual_backend) == {}

    def test_cursor_valid_roundtrip_upsert_empty_and_delete(self, dual_backend):
        b = dual_backend
        b.set_cursor("k", "v_one")
        assert b.get_cursor("k") == "v_one"
        b.set_cursor("k", "v_two")
        assert b.get_cursor("k") == "v_two"
        assert _cursors(b) == {"k": "v_two"}
        b.set_cursor("k", None)
        assert b.get_cursor("k") is None
        assert _cursors(b) == {}
        b.set_cursor("k", "")
        assert b.get_cursor("k") == ""
        assert _cursors(b) == {"k": ""}


@pytest.mark.parametrize(
    "payload",
    [None, {}, {"error": "业务", "error_message": "新业务", "nested": {"new_jobs": [], "spawned_jobs": [1]}}],
)
def test_archive_metadata_and_payload_are_independent_untranslated_copies(dual_backend, payload):
    metadata = {
        "error": "旧业务键",
        "error_message": "框架诊断",
        "fatal": "业务",
        "is_fatal": False,
        "_attempt": 9,
        "attempt_no": 0,
        "nested": {"timeout": 17, "timeout_seconds": 18},
    }
    expected_metadata = copy.deepcopy(metadata)
    expected_payload = copy.deepcopy(payload)
    dual_backend.commit_job_failures([("t::separate", metadata, payload)])
    metadata["nested"]["timeout"] = 99
    if payload:
        payload["nested"]["new_jobs"].append(99)
    assert dual_backend.load_failed() == {"t::separate": expected_metadata}
    expected_payloads = {} if payload is None else {"t::separate": expected_payload}
    assert _failure_payloads(dual_backend) == expected_payloads
    dual_backend.load_failed()["t::separate"]["nested"]["timeout"] = 999
    assert dual_backend.load_failed() == {"t::separate": expected_metadata}
    dual_backend.commit_job_failures([("t::separate", {"replacement": True}, None)])
    assert _failure_payloads(dual_backend) == expected_payloads


# ── 状态机四态结算、冲突回滚与 Attempt 编号独立性 ─────────────────────


class TestStatePrimitivesAndAttemptSequence:
    """执行校验、四态结算对齐、派生冲突回滚与 attempt_no 单调推进。"""

    @pytest.mark.parametrize(
        ("execution_id", "run_id", "attempt_no"),
        [("run.999", "run", 1), ("another-run.999", "another-run", 1), ("run.1", "run", 2)],
    )
    def test_settlement_for_other_execution_is_rejected(
        self, dual_backend, execution_id, run_id, attempt_no
    ):
        uid = "t::parent"
        dual_backend.enqueue_jobs([_job_snapshot("parent")])
        dual_backend.mark_job_running(
            uid, execution_id="run.1", run_id="run", started_at="2026-01-01T00:00:00Z"
        )
        before = _state_facts(dual_backend)
        with pytest.raises(TaskLiteError, match="does not match the running execution"):
            dual_backend.commit_job_settlement(
                job_uid=uid,
                outcome="success",
                execution_id=execution_id,
                run_id=run_id,
                attempt_no=attempt_no,
                settled_at="2026-01-01T00:00:01Z",
                success_metadata={"ok": True},
                spawned_job_snapshots=[_job_snapshot("child")],
                cursor_updates={"offset": "new"},
            )
        assert _state_facts(dual_backend) == before
        assert _settled_attempts(dual_backend, uid) == []

    def test_already_settled_running_execution_is_rejected(self, dual_backend):
        uid = "t::parent"
        job = _job_snapshot("parent")
        started_at = "2026-01-01T00:00:00Z"
        dual_backend.enqueue_jobs([job])
        attempt_no = dual_backend.mark_job_running(
            uid, execution_id="run.1", run_id="run", started_at=started_at
        )
        dual_backend.commit_job_settlement(
            job_uid=uid,
            outcome="retry",
            execution_id="run.1",
            run_id="run",
            attempt_no=attempt_no,
            settled_at="2026-01-01T00:00:01Z",
        )
        running_job = dict(job, status="running", run_id="run", attempt_no=attempt_no, started_at=started_at)
        running_job[QUEUE_EXECUTION_ID_KEY] = "run.1"
        dual_backend.commit_skip(uid)
        dual_backend.enqueue_jobs([running_job])
        before = _state_facts(dual_backend)
        attempts = _settled_attempts(dual_backend, uid)

        with pytest.raises(TaskLiteError, match="already settled"):
            dual_backend.commit_job_settlement(
                job_uid=uid,
                outcome="success",
                run_id="run",
                attempt_no=attempt_no,
                execution_id="run.1",
                settled_at="2026-01-01T00:00:02Z",
                spawned_job_snapshots=[_job_snapshot("child")],
                cursor_updates={"offset": "new"},
            )
        assert _state_facts(dual_backend) == before
        assert _settled_attempts(dual_backend, uid) == attempts

    def test_atomic_settlement_failure_leaves_no_partial_state(self, dual_backend):
        b = dual_backend
        b.enqueue_jobs([_job_snapshot("parent")])
        b.mark_job_running("t::parent", execution_id="run_1.1", run_id="run_1", started_at="2026-11-08T00:00:00Z")
        before = _state_facts(b)

        with pytest.raises(TypeError):
            b.commit_job_settlement(
                job_uid="t::parent",
                outcome="success",
                attempt_no=1,
                execution_id="run_1.1",
                run_id="run_1",
                settled_at="2026-11-08T00:00:01Z",
                success_metadata={"ok": True},
                spawned_job_snapshots=[_job_snapshot("child_1")],
                cursor_updates={"bad_cursor": 12345},  # type: ignore[dict-item]
            )
        assert _state_facts(b) == before
        assert _settled_attempts(b, "t::parent") == []

        b.enqueue_jobs([_job_snapshot("conflict_child")])
        before2 = _state_facts(b)
        with pytest.raises(TaskLiteError, match="[Cc]onflict"):
            b.commit_job_settlement(
                job_uid="t::parent",
                outcome="success",
                attempt_no=1,
                execution_id="run_1.1",
                run_id="run_1",
                settled_at="2026-11-08T00:00:02Z",
                success_metadata={"ok": True},
                spawned_job_snapshots=[_job_snapshot("conflict_child")],
                cursor_updates={"good_cursor": "val"},
            )
        assert _state_facts(b) == before2
        assert _settled_attempts(b, "t::parent") == []

    @pytest.mark.parametrize("conflict", ["existing", "duplicate"])
    def test_spawn_conflict_rolls_back_complete_settlement(self, dual_backend, conflict):
        backend = dual_backend
        backend.commit_job_failures([("t::parent", {"old": True}, {"input": 1})])
        backend.enqueue_jobs([_job_snapshot("parent"), _job_snapshot("existing")])
        backend.set_cursor("offset", "old")
        backend.mark_job_running("t::parent", execution_id="run.1", run_id="run", started_at="start")
        before = _snapshot(backend)
        before_queue = backend.load_queue_jobs()
        spawned = [_job_snapshot("existing")] if conflict == "existing" else [_job_snapshot("child")] * 2
        with pytest.raises(TaskLiteError, match="conflict"):
            backend.commit_job_settlement(
                job_uid="t::parent",
                outcome="success",
                attempt_no=1,
                execution_id="run.1",
                run_id="run",
                settled_at="finish",
                success_metadata={"new": True},
                spawned_job_snapshots=spawned,
                cursor_updates={"offset": "new"},
            )
        assert _snapshot(backend) == before
        assert backend.load_queue_jobs() == before_queue
        assert _settled_attempts(backend, "t::parent") == []
        assert _attempt_sequence(backend, "t::parent") == 1

    def test_settlement_outcomes_parity(self, dual_backend):
        b = dual_backend

        # success
        b.enqueue_jobs([_job_snapshot("job_s")])
        assert b.mark_job_running("t::job_s", execution_id="run_s.1", run_id="run_s", started_at="2026-11-08T00:00:00Z") == 1
        b.commit_job_settlement(
            job_uid="t::job_s",
            outcome="success",
            attempt_no=1,
            execution_id="run_s.1",
            run_id="run_s",
            started_at="2026-11-08T00:00:00Z",
            settled_at="2026-11-08T00:00:01Z",
            job_snapshot=_job_snapshot("job_s"),
            result={"message": "done", "metadata": {"x": 1}},
            success_metadata={"x": 1},
            spawned_job_snapshots=[_job_snapshot("child_s")],
            cursor_updates={"cur_1": "cval"},
        )
        assert "t::job_s" in b.load_successes()
        assert _cursors(b) == {"cur_1": "cval"}
        assert [(j["job_uid"], j["status"]) for j in b.load_queue_jobs()] == [("t::child_s", "pending")]
        assert _settled_attempts(b, "t::job_s")[0]["outcome"] == "success"

        # failure
        b.enqueue_jobs([_job_snapshot("job_f")])
        b.mark_job_running("t::job_f", execution_id="run_f.1", run_id="run_f", started_at="2026-11-08T01:00:00Z")
        b.commit_job_settlement(
            job_uid="t::job_f",
            outcome="failure",
            attempt_no=1,
            execution_id="run_f.1",
            run_id="run_f",
            settled_at="2026-11-08T01:00:01Z",
            failure_metadata={"error": "oom"},
            payload={"input": "data"},
        )
        assert "t::job_f" in b.load_failed()
        assert _failure_payloads(b).get("t::job_f") == {"input": "data"}
        assert _settled_attempts(b, "t::job_f")[0]["outcome"] == "failure"

        # retry
        b.enqueue_jobs([_job_snapshot("job_r")])
        b.mark_job_running("t::job_r", execution_id="run_r.1", run_id="run_r", started_at="2026-11-08T02:00:00Z")
        b.commit_job_settlement(
            job_uid="t::job_r",
            outcome="retry",
            attempt_no=1,
            execution_id="run_r.1",
            run_id="run_r",
            settled_at="2026-11-08T02:00:01Z",
        )
        r_jobs = [j for j in b.load_queue_jobs() if j["job_uid"] == "t::job_r"]
        assert len(r_jobs) == 1
        assert r_jobs[0]["status"] == "pending"
        assert r_jobs[0]["run_id"] is None
        assert r_jobs[0]["attempt_no"] is None
        assert r_jobs[0]["started_at"] is None
        assert r_jobs[0]["execution_id"] is None
        assert _settled_attempts(b, "t::job_r")[0]["outcome"] == "retry"

        # pending (authorization denied / abort)
        b.enqueue_jobs([_job_snapshot("job_p")])
        b.mark_job_running("t::job_p", execution_id="run_p.1", run_id="run_p", started_at="2026-11-08T03:00:00Z")
        b.commit_job_settlement(
            job_uid="t::job_p",
            outcome="pending",
            attempt_no=1,
            execution_id="run_p.1",
            run_id="run_p",
            settled_at="2026-11-08T03:00:01Z",
            error_message="authorization denied",
        )
        p_jobs = [j for j in b.load_queue_jobs() if j["job_uid"] == "t::job_p"]
        assert len(p_jobs) == 1
        assert p_jobs[0]["status"] == "pending"
        assert p_jobs[0]["run_id"] is None
        assert p_jobs[0]["attempt_no"] is None
        assert p_jobs[0]["started_at"] is None
        assert p_jobs[0]["execution_id"] is None
        p_att = _settled_attempts(b, "t::job_p")
        assert len(p_att) == 1 and p_att[0]["outcome"] == "failure" and p_att[0]["error_message"] == "authorization denied"

    def test_queue_row_strict_parity_across_running_retry_pending_and_spawn(self, tmp_path):
        """SQLite 与 Memory 双后端在 running / retry / pending / spawn 路径下队列行逐键等价且零脏键泄漏。"""
        dirty_keys = {"status", "run_id", "attempt_no", "started_at", QUEUE_EXECUTION_ID_KEY}
        mem = InMemoryStateBackend()
        sql = SQLiteStateBackend(tmp_path / "strict_parity.db")
        try:
            def assert_queue_strict_equal() -> None:
                mem_rows = mem.load_queue_jobs()
                sql_rows = sql.load_queue_jobs()
                assert mem_rows == sql_rows
                assert mem.load_consistent_snapshot()["queue_jobs"] == sql.load_consistent_snapshot()["queue_jobs"]
                for row in mem_rows:
                    assert dirty_keys.isdisjoint(row["job"].keys()), f"业务快照泄漏运行态键: {row['job']}"

            initial = [_job_snapshot("j_retry", payload={"v": 1}), _job_snapshot("j_pend", payload={"v": 2}), _job_snapshot("j_ok", payload={"v": 3})]
            mem.enqueue_jobs(copy.deepcopy(initial))
            sql.enqueue_jobs(copy.deepcopy(initial))
            assert_queue_strict_equal()

            # mark_job_running 后双后端行完全一致，且业务快照零脏键
            for b in (mem, sql):
                assert b.mark_job_running("t::j_retry", execution_id="r1.1", run_id="r1", started_at="2026-11-08T00:00:00Z") == 1
                assert b.mark_job_running("t::j_pend", execution_id="r1.2", run_id="r1", started_at="2026-11-08T00:00:01Z") == 1
                assert b.mark_job_running("t::j_ok", execution_id="r1.3", run_id="r1", started_at="2026-11-08T00:00:02Z") == 1
            assert_queue_strict_equal()

            # retry（无 requeued_job_snapshot）与 pending 结算：attempt_no 统一清零为 None
            for b in (mem, sql):
                b.commit_job_settlement(
                    job_uid="t::j_retry",
                    outcome="retry",
                    attempt_no=1,
                    execution_id="r1.1",
                    run_id="r1",
                    settled_at="2026-11-08T00:00:10Z",
                )
                b.commit_job_settlement(
                    job_uid="t::j_pend",
                    outcome="pending",
                    attempt_no=1,
                    execution_id="r1.2",
                    run_id="r1",
                    settled_at="2026-11-08T00:00:11Z",
                )
            assert_queue_strict_equal()
            for row in mem.load_queue_jobs():
                if row["job_uid"] in ("t::j_retry", "t::j_pend"):
                    assert row["status"] == "pending"
                    assert row["run_id"] is None
                    assert row["attempt_no"] is None
                    assert row["started_at"] is None
                    assert row["execution_id"] is None

            # retry（带 requeued_job_snapshot）后同样零脏键且 attempt_no 为 None
            requeued_snap = _job_snapshot("j_retry", payload={"v": 10})
            for b in (mem, sql):
                assert b.mark_job_running("t::j_retry", execution_id="r2.1", run_id="r2", started_at="2026-11-08T00:01:00Z") == 2
                b.commit_job_settlement(
                    job_uid="t::j_retry",
                    outcome="retry",
                    attempt_no=2,
                    execution_id="r2.1",
                    run_id="r2",
                    settled_at="2026-11-08T00:01:05Z",
                    requeued_job_snapshot=copy.deepcopy(requeued_snap),
                )
            assert_queue_strict_equal()

            # success + spawned_job_snapshots 派生入队行零脏键、逐键等价（注意 queue_position 语义比较）
            spawned = [_job_snapshot("child_1", payload={"c": 1})]
            for b in (mem, sql):
                b.commit_job_settlement(
                    job_uid="t::j_ok",
                    outcome="success",
                    attempt_no=1,
                    execution_id="r1.3",
                    run_id="r1",
                    settled_at="2026-11-08T00:02:00Z",
                    success_metadata={"ok": True},
                    spawned_job_snapshots=copy.deepcopy(spawned),
                )
            mem_rows = mem.load_queue_jobs()
            sql_rows = sql.load_queue_jobs()
            assert [
                {k: v for k, v in row.items() if k != "queue_position"} for row in mem_rows
            ] == [
                {k: v for k, v in row.items() if k != "queue_position"} for row in sql_rows
            ]
            for row in mem_rows:
                assert dirty_keys.isdisjoint(row["job"].keys())
        finally:
            mem.close()
            sql.close()

    def test_settled_history_is_immutable_isolated_and_orders_by_sequence(self, dual_backend):
        backend = dual_backend
        job = _job_snapshot("parent")
        backend.enqueue_jobs([job])
        for number, outcome in enumerate(("retry", "retry", "success"), 1):
            assert (
                backend.mark_job_running(
                    "t::parent", execution_id=f"run.{number}", run_id="run", started_at=f"start-{number}"
                )
                == number
            )
            assert len(_settled_attempts(backend, "t::parent")) == number - 1
            files_arg = {"output": {"path": "/output"}}
            result_arg = {"metadata": {"n": number}}
            backend.commit_job_settlement(
                job_uid="t::parent",
                outcome=outcome,
                attempt_no=number,
                execution_id=f"run.{number}",
                run_id="run",
                settled_at=f"finish-{number}",
                started_at=f"start-{number}",
                job_snapshot=job,
                files=files_arg,
                result=result_arg,
            )
            files_arg["output"]["path"] = "mutated-after-commit"
            result_arg["metadata"]["n"] = 999
            if number == 1:
                first = _settled_attempts(backend, "t::parent")[0]
        records = _settled_attempts(backend, "t::parent")
        assert records[0] == first
        assert [record["attempt_no"] for record in records] == [1, 2, 3]
        assert [record["outcome"] for record in records] == ["retry", "retry", "success"]
        assert len({record["attempt_id"] for record in records}) == 3
        assert backend.load_latest_succeeded_finished_at("t::parent") == "finish-3"
        records[0]["result"]["metadata"]["n"] = 999
        records[0]["files"]["output"]["path"] = "changed"
        assert _settled_attempts(backend, "t::parent")[0] == first
        assert backend.load_latest_succeeded_finished_at("t::missing") is None
        backend.commit_job_failures([("t::parent", {"preflight": True})])
        backend.commit_skip("t::parent")
        assert _settled_attempts(backend, "t::parent")[0] == first

    @pytest.mark.parametrize("snapshot_attempt_no", [999, -7])
    def test_removing_archive_preserves_attempts_and_attempt_sequence(
        self, dual_backend, snapshot_attempt_no
    ):
        b = dual_backend
        job_uid = "t::target_seq"
        b.enqueue_jobs([_job_snapshot("target_seq", attempt_no=snapshot_attempt_no)])
        assert b.mark_job_running(job_uid, execution_id="run_1.1", run_id="run_1", started_at="2026-11-08T00:00:00Z") == 1
        assert _attempt_sequence(b, job_uid) == 1
        b.commit_job_settlement(
            job_uid=job_uid,
            outcome="success",
            attempt_no=1,
            execution_id="run_1.1",
            run_id="run_1",
            settled_at="2026-11-08T00:00:01Z",
            success_metadata={"status": "ok"},
        )
        b.commit_graph_edit(GraphEditCommit(success_writes={job_uid: None}))
        assert job_uid not in b.load_successes()
        # attempts 严格 append-only 不可删除，job_sequences 亦永不重置
        assert len(_settled_attempts(b, job_uid)) == 1
        assert _attempt_sequence(b, job_uid) == 1
        assert _pending_queue(b) == [] and b.load_successes() == {} and b.load_failed() == {}

        b.enqueue_jobs([_job_snapshot("target_seq", attempt_no=snapshot_attempt_no)])
        assert b.mark_job_running(job_uid, execution_id="run_2.2", run_id="run_2", started_at="2026-11-08T01:00:00Z") == 2
        assert _attempt_sequence(b, job_uid) == 2


# ── 授权存储与 StateStore 授权校验对齐 ────────────────────────────────


class TestAuthorizationParity:
    """持久化授权存储、路径基准规范化与作用域升级防线。"""

    def test_record_list_scope_and_revoke(self, dual_backend, tmp_path):
        b = dual_backend
        f1 = str((tmp_path / "a.txt").resolve())
        d1 = str((tmp_path / "data").resolve())
        sub = str((tmp_path / "data" / "sub" / "file.csv").resolve())
        outside = str((tmp_path / "data_other" / "file.csv").resolve())

        assert not b.is_path_authorized(f1, "file")
        assert not b.is_path_authorized(sub, "file")

        b.record_authorization(f1, "file", authorized_at="2026-11-08T01:00:00Z")
        b.record_authorization(d1, "directory", authorized_at="2026-11-08T02:00:00Z")
        records = b.list_authorizations()
        assert len(records) == 2
        assert records[0] == {"path": f1, "target_type": "file", "authorized_at": "2026-11-08T01:00:00Z"}
        assert records[1] == {"path": d1, "target_type": "directory", "authorized_at": "2026-11-08T02:00:00Z"}

        assert b.is_path_authorized(f1, "file")
        assert not b.is_path_authorized(f1 + ".bak", "file")
        assert b.is_path_authorized(d1, "directory")
        assert b.is_path_authorized(sub, "file")
        assert not b.is_path_authorized(outside, "file")

        assert b.revoke_authorization(f1) is True
        assert not b.is_path_authorized(f1, "file")
        assert b.revoke_authorization(f1) is False

    def test_store_check_or_request_authorization(self, dual_backend, tmp_path, monkeypatch):
        store = StateStore(backend=dual_backend, base_dir=tmp_path)
        target = str((tmp_path / "work" / "out.txt").resolve())

        monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
        assert store.check_or_request_authorization(target) is False
        assert not store.is_path_authorized(target)

        with authorizer(store, lambda p, t: False):
            assert store.check_or_request_authorization(target) is False
            assert not store.is_path_authorized(target)

        with authorizer(store, lambda p, t: True):
            assert store.check_or_request_authorization(target) is True
            assert store.is_path_authorized(target)

        with authorizer(store, None):
            assert store.check_or_request_authorization(target) is True

        target2 = str((tmp_path / "work" / "out2.txt").resolve())
        monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
        assert store.check_or_request_authorization(target2) is True
        assert store.is_path_authorized(target2)

    @pytest.mark.parametrize("path_form", ["relative", "parent-link", "broken-link", "current", "parent", "root"])
    def test_store_authorization_paths_share_fixed_base_normalization(
        self, dual_backend, tmp_path, monkeypatch, path_form
    ):
        monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
        base = tmp_path / "base"
        other = tmp_path / "other"
        real = tmp_path / "real"
        for directory in (base, other, real):
            directory.mkdir()
        (base / "alias").symlink_to(real, target_is_directory=True)
        (base / "broken").symlink_to(real / "missing")
        cases = {
            "relative": ("nested/../file", base / "file"),
            "parent-link": ("alias/file", real / "file"),
            "broken-link": ("broken", base / "broken"),
            "current": (".", base),
            "parent": ("..", tmp_path),
            "root": (base.anchor, Path(base.anchor)),
        }
        path, expected = cases[path_form]
        store = StateStore(backend=dual_backend, base_dir=base)
        monkeypatch.chdir(other)
        calls: list[tuple[str, str]] = []
        target_type = "directory" if expected.is_dir() else "file"
        with authorizer(store, lambda *args: calls.append(args) or True):
            assert store.check_or_request_authorization(path) is True
        assert calls == [(str(expected), target_type)]
        assert store.is_path_authorized(path)
        store.record_authorization(path, target_type)
        (record,) = store.list_authorizations()
        assert record["path"] == str(expected)
        assert store.revoke_authorization(path) is True
        assert store.list_authorizations() == []

    @pytest.mark.parametrize("initial", ["file", "missing"])
    def test_file_scope_requires_new_authorization_for_directory(
        self, dual_backend, tmp_path, monkeypatch, initial
    ):
        monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
        target = tmp_path / "target"
        if initial == "file":
            target.write_text("file", encoding="utf-8")
        store = StateStore(backend=dual_backend, base_dir=tmp_path)
        calls: list[tuple[str, str]] = []
        with authorizer(store, lambda *args: calls.append(args) or True):
            assert store.check_or_request_authorization(target) is True
        if target.exists():
            target.unlink()
        target.mkdir()
        (target / "child").write_text("preserve", encoding="utf-8")
        assert not dual_backend.is_path_authorized(str(target), "directory")
        assert not store.is_path_authorized(target)
        with authorizer(store, lambda *args: calls.append(args) or None):
            assert store.check_or_request_authorization(target) is None
        assert dual_backend.list_authorizations()[0]["target_type"] == "file"
        with authorizer(store, lambda *args: calls.append(args) or True):
            assert store.check_or_request_authorization(target) is True
        assert [kind for _, kind in calls] == ["file", "directory", "directory"]
        assert store.is_path_authorized(target / "child")

    def test_parent_link_change_does_not_reuse_previous_target_authorization(
        self, dual_backend, tmp_path, monkeypatch
    ):
        monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
        first, second = tmp_path / "first", tmp_path / "second"
        first.mkdir()
        second.mkdir()
        alias = tmp_path / "alias"
        alias.symlink_to(first, target_is_directory=True)
        store = StateStore(backend=dual_backend, base_dir=tmp_path)
        calls: list[tuple[str, str]] = []
        with authorizer(store, lambda *args: calls.append(args) or True):
            assert store.check_or_request_authorization(alias / "item") is True
            alias.unlink()
            alias.symlink_to(second, target_is_directory=True)
            assert not store.is_path_authorized(alias / "item")
            assert store.check_or_request_authorization(alias / "item") is True
        assert calls == [(str(first / "item"), "file"), (str(second / "item"), "file")]

    def test_type_change_inside_authorizer_cannot_expand_recorded_file_scope(
        self, dual_backend, tmp_path, monkeypatch
    ):
        monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
        target = tmp_path / "target"
        store = StateStore(backend=dual_backend, base_dir=tmp_path)
        calls: list[tuple[str, str]] = []

        def authorize(path, kind):
            calls.append((path, kind))
            if kind == "file":
                target.mkdir()
                return True
            return None

        with authorizer(store, authorize):
            assert store.check_or_request_authorization(target) is None
        assert calls == [(str(target), "file"), (str(target), "directory")]
        assert not store.is_path_authorized(target)
        assert store.list_authorizations()[0]["target_type"] == "file"

    def test_auto_approval_records_directory_scope_after_file_replacement(
        self, dual_backend, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
        target = tmp_path / "target"
        target.write_text("file", encoding="utf-8")
        store = StateStore(backend=dual_backend, base_dir=tmp_path)
        assert store.check_or_request_authorization(target) is True
        assert store.list_authorizations()[0]["target_type"] == "file"
        target.unlink()
        target.mkdir()
        assert store.check_or_request_authorization(target) is True
        assert store.list_authorizations()[0]["target_type"] == "directory"

    def test_grant_authorization_parity_conflicts_and_nested_directories(
        self, dual_backend, tmp_path
    ):
        b = dual_backend
        f1 = str((tmp_path / "single.txt").resolve())
        d_root = str((tmp_path / "work").resolve())
        d_sub = str((tmp_path / "work" / "sub").resolve())
        f_inside = str((tmp_path / "work" / "sub" / "leaf.bin").resolve())
        f_parent = str((tmp_path / "conflict_parent").resolve())
        f_child = str((tmp_path / "conflict_parent" / "child.txt").resolve())
        d_child = str((tmp_path / "conflict_parent2" / "sub").resolve())
        f_ancestor = str((tmp_path / "conflict_parent2").resolve())

        with pytest.raises(TaskLiteError, match="target_type"):
            b.grant_authorization(f1, "symlink")  # type: ignore[arg-type]

        rec = b.grant_authorization(f1, "file", authorized_at="2026-11-20T00:00:00Z")
        assert rec == {
            "path": f1,
            "target_type": "file",
            "authorized_at": "2026-11-20T00:00:00Z",
        }
        assert b.is_path_authorized(f1, "file") is True
        assert b.is_path_authorized(f1, "directory") is False

        # 同路径重复授权（同类型或异类型）一律拒绝，且不刷新时间戳
        with pytest.raises(TaskLiteError, match="already exists"):
            b.grant_authorization(f1, "file", authorized_at="2026-11-21T00:00:00Z")
        with pytest.raises(TaskLiteError, match="already exists"):
            b.grant_authorization(f1, "directory", authorized_at="2026-11-21T00:00:00Z")
        assert b.list_authorizations() == [rec]

        # 祖先已按 file 授权时，禁止再授权任何子路径（file 或 directory）
        b.grant_authorization(f_parent, "file")
        with pytest.raises(TaskLiteError, match="existing file authorization"):
            b.grant_authorization(f_child, "file")
        with pytest.raises(TaskLiteError, match="existing file authorization"):
            b.grant_authorization(str(Path(f_parent) / "dir"), "directory")

        # 已存在子路径授权时，禁止将祖先按 file 授权
        b.grant_authorization(d_child, "directory")
        with pytest.raises(TaskLiteError, match="descendant authorization"):
            b.grant_authorization(f_ancestor, "file")

        # 父子目录嵌套及目录下文件允许独立存取与撤销
        r_root = b.grant_authorization(d_root, "directory")
        r_sub = b.grant_authorization(d_sub, "directory")
        r_leaf = b.grant_authorization(f_inside, "file")
        assert r_root["path"] == d_root and r_sub["path"] == d_sub and r_leaf["path"] == f_inside
        assert b.is_path_authorized(f_inside, "file") is True
        assert b.revoke_authorization(d_sub) is True
        # 撤销子目录后父目录仍覆盖该文件，但子目录独立条目已删除
        assert b.is_path_authorized(f_inside, "file") is True
        assert b.revoke_authorization(d_sub) is False
        assert b.revoke_authorization(d_root) is True
        # 撤销父目录后，显式授权的子文件条目依然保留
        assert b.is_path_authorized(f_inside, "file") is True
        assert b.revoke_authorization(f_inside) is True
        assert b.is_path_authorized(f_inside, "file") is False

    def test_windows_case_equivalence_in_backend_authorizations(self, dual_backend, tmp_path):
        import os

        if os.name != "nt":
            pytest.skip("仅在 Windows 验证大小写等价键行为")
        b = dual_backend
        lower_path = str((tmp_path / "case_dir" / "item.txt").resolve())
        upper_path = str((tmp_path / "CASE_DIR" / "ITEM.TXT").resolve())

        b.grant_authorization(lower_path, "file", authorized_at="2026-11-20T00:00:00Z")
        with pytest.raises(TaskLiteError, match="already exists"):
            b.grant_authorization(upper_path, "file")
        assert b.is_path_authorized(upper_path, "file") is True

        # 运行期 record_authorization 大小写差异原地升级，不留双行
        b.record_authorization(upper_path, "directory", authorized_at="2026-11-20T01:00:00Z")
        records = b.list_authorizations()
        assert len(records) == 1
        assert records[0]["target_type"] == "directory"
        assert b.revoke_authorization(lower_path) is True
        assert b.list_authorizations() == []
