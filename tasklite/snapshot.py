"""StateSnapshot：只读一致状态快照、拷贝模式与可读报告（ADR-0011 / ADR-0012）。

独立只读入口，支持从数据库路径或 TaskLite 实例打开；SQLite 与内存实例均
返回同一种只读快照。一个上下文提供一致快照，不创建/迁移/修复数据库，
关闭快照不关闭 TaskLite。

内部架构：单一深投影核心（StateSnapshot 统一承载优先级裁决、字段归一化与
过滤排序）+ 2 个薄数据适配器（SQLite 流式只读适配器与统一内存表视图适配器，
后者统一服务于 InMemoryStateBackend、SQLite copy 模式与 EditTransaction）。
"""
from __future__ import annotations

import copy
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol

from ._report import render_snapshot_report
from .backend.sqlite_backend import open_existing_sqlite_readonly
from .exceptions import TaskLiteError
from .models.records import (
    extract_archive_result,
    extract_effective_job_config,
    extract_job_snapshot_from_archive,
    normalize_outer_files,
    normalize_result_record,
)
from .utils.encoding import parse_job_uid
from .utils.jsonutil import loads

if TYPE_CHECKING:
    from .pipeline import TaskLite

_VALID_SNAPSHOT_MODES = ("lazy", "copy")


def _format_iso_z(ts: str | None) -> str | None:
    """时间戳统一规范为 UTC ISO 8601 字符串（以 Z 结尾，None 表示未知）。"""
    if not ts or not isinstance(ts, str):
        return None
    if ts.endswith("+00:00"):
        return ts[:-6] + "Z"
    return ts


def _project_queued_job(
    uid: str,
    job_snap: Mapping[str, Any],
    status: str,
    run_id: str | None,
    attempt_no: int | None,
    started_at: str | None = None,
) -> dict[str, Any]:
    """队列记录投影：pending 不携带执行身份与开始时间，running 投影已落盘字段。"""
    is_running = status == "running"
    return {
        "job_uid": uid,
        "status": status,
        "job": extract_effective_job_config(job_snap),
        "files": normalize_outer_files(job_snap.get("files")),
        "result": None,
        "run_id": run_id if is_running else None,
        "attempt_no": attempt_no if is_running else None,
        "started_at": _format_iso_z(started_at) if is_running else None,
        "settled_at": None,
    }


def _project_succeeded_job(
    uid: str,
    succ_meta: Mapping[str, Any],
) -> dict[str, Any]:
    """当前成功只投影自身档案，不从独立的 Attempt 历史补推事实。"""
    job_snap = extract_job_snapshot_from_archive(uid, succ_meta, fill_missing_identity=False)
    outer_files = normalize_outer_files(
        succ_meta.get("files") or job_snap.get("files")
    )
    return {
        "job_uid": uid,
        "status": "succeeded",
        "job": extract_effective_job_config(job_snap),
        "files": outer_files,
        "result": extract_archive_result(succ_meta, is_failure=False),
        "run_id": succ_meta.get("run_id"),
        "attempt_no": succ_meta.get("attempt_no"),
        "started_at": _format_iso_z(succ_meta.get("started_at")),
        "settled_at": _format_iso_z(succ_meta.get("settled_at")),
    }


def _project_failed_job(
    uid: str,
    fail_meta: Mapping[str, Any],
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """当前失败档案投影：仅在 job_snapshot 缺 payload 时补回独立 payload 列。"""
    job_snap = extract_job_snapshot_from_archive(
        uid, fail_meta, payload=payload, fill_missing_identity=False
    )
    outer_files = normalize_outer_files(
        fail_meta.get("files") or job_snap.get("files")
    )
    return {
        "job_uid": uid,
        "status": "failed",
        "job": extract_effective_job_config(job_snap),
        "files": outer_files,
        "result": extract_archive_result(fail_meta, is_failure=True),
        "run_id": fail_meta.get("run_id"),
        "attempt_no": fail_meta.get("attempt_no"),
        "started_at": _format_iso_z(fail_meta.get("started_at")),
        "settled_at": _format_iso_z(fail_meta.get("settled_at")),
    }


def _project_attempt(
    uid: str,
    attempt_no: int,
    run_id: str,
    outcome: str,
    started_at: str | None,
    settled_at: str | None,
    job_snap: Mapping[str, Any],
    files_data: Mapping[str, Any] | None,
    result_data: Any,
) -> dict[str, Any]:
    """物理 Attempt 历史行投影为统一只读字典。"""
    return {
        "job_uid": uid,
        "outcome": outcome,
        "job": extract_effective_job_config(job_snap),
        "files": normalize_outer_files(files_data),
        "result": normalize_result_record(result_data),
        "run_id": run_id,
        "attempt_no": attempt_no,
        "started_at": _format_iso_z(started_at),
        "settled_at": _format_iso_z(settled_at),
    }


def _matches_task_type(uid: str, task_type: str | None) -> bool:
    if task_type is None:
        return True
    try:
        tt, _ = parse_job_uid(uid)
    except Exception:
        return False
    return tt == task_type


def _decode_json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        val = loads(raw) if raw else {}
        return val if isinstance(val, dict) else {}
    return raw if isinstance(raw, dict) else {}


def _decode_json_optional_dict(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        if not raw:
            return None
        val = loads(raw)
        return val if isinstance(val, dict) else None
    return raw if isinstance(raw, dict) else None


def _decode_attempt_result(raw_result: Any, error_message: str | None) -> Any:
    """将 Attempt 原始结果解码为可交给 normalize_result_record 归一化为 4 键字典的载荷。"""
    if raw_result is not None:
        if isinstance(raw_result, str):
            if raw_result:
                decoded = loads(raw_result)
                if decoded is not None:
                    return decoded
        else:
            return raw_result
    return {"message": error_message}


# ── 2 个薄数据适配器（流式 SQLite + 统一内存表视图）────────────────────────


class _SnapshotAdapter(Protocol):
    def get_queued(
        self, uid: str
    ) -> tuple[Mapping[str, Any], str, str | None, int | None, str | None] | None: ...
    def get_success(self, uid: str) -> Mapping[str, Any] | None: ...
    def get_failed(self, uid: str) -> tuple[Mapping[str, Any], Mapping[str, Any] | None] | None: ...
    def iter_queued(
        self, *, status: str | None = None, task_type: str | None = None
    ) -> Iterator[tuple[str, Mapping[str, Any], str, str | None, int | None, str | None]]: ...
    def iter_non_queued_successes(
        self, *, task_type: str | None = None
    ) -> Iterator[tuple[str, Mapping[str, Any]]]: ...
    def iter_non_queued_failed(
        self, *, task_type: str | None = None
    ) -> Iterator[tuple[str, Mapping[str, Any], Mapping[str, Any] | None]]: ...
    def iter_attempts(
        self, *, job_uid: str | None = None, task_type: str | None = None
    ) -> Iterator[
        tuple[
            str,
            int,
            str,
            str,
            str | None,
            str | None,
            Mapping[str, Any],
            Mapping[str, Any] | None,
            Any,
        ]
    ]: ...
    def close(self) -> None: ...


class _SqliteStreamingAdapter:
    """SQLite 流式只读适配器（mode='lazy'）：仅持单一只读事务游标按需读取。"""

    def __init__(self, db_path: Path) -> None:
        self._conn = open_existing_sqlite_readonly(db_path, begin_transaction=True)
        self._closed = False

    def get_queued(
        self, uid: str
    ) -> tuple[Mapping[str, Any], str, str | None, int | None, str | None] | None:
        row = self._conn.execute(
            "SELECT job_snapshot, status, run_id, attempt_no, started_at "
            "FROM queue WHERE job_uid = ?",
            (uid,),
        ).fetchone()
        if not row:
            return None
        return (
            _decode_json_dict(row[0]),
            row[1] if row[1] else "pending",
            row[2],
            row[3],
            row[4],
        )

    def get_success(self, uid: str) -> Mapping[str, Any] | None:
        row = self._conn.execute(
            "SELECT metadata FROM successes WHERE job_uid = ?", (uid,)
        ).fetchone()
        if not row:
            return None
        return _decode_json_dict(row[0])

    def get_failed(self, uid: str) -> tuple[Mapping[str, Any], Mapping[str, Any] | None] | None:
        row = self._conn.execute(
            "SELECT metadata, payload FROM failed WHERE job_uid = ?", (uid,)
        ).fetchone()
        if not row:
            return None
        return _decode_json_dict(row[0]), _decode_json_optional_dict(row[1])

    def iter_queued(
        self, *, status: str | None = None, task_type: str | None = None
    ) -> Iterator[tuple[str, Mapping[str, Any], str, str | None, int | None, str | None]]:
        cursor = self._conn.execute(
            "SELECT job_uid, job_snapshot, status, run_id, attempt_no, started_at "
            "FROM queue ORDER BY queue_position ASC"
        )
        for r in cursor:
            uid = r[0]
            st = r[2] if r[2] else "pending"
            if status is not None and st != status:
                continue
            if not _matches_task_type(uid, task_type):
                continue
            yield uid, _decode_json_dict(r[1]), st, r[3], r[4], r[5]

    def iter_non_queued_successes(
        self, *, task_type: str | None = None
    ) -> Iterator[tuple[str, Mapping[str, Any]]]:
        cursor = self._conn.execute(
            "SELECT s.job_uid, s.metadata FROM successes AS s "
            "WHERE NOT EXISTS (SELECT 1 FROM queue AS q WHERE q.job_uid = s.job_uid)"
        )
        for r in cursor:
            uid = r[0]
            if not _matches_task_type(uid, task_type):
                continue
            yield uid, _decode_json_dict(r[1])

    def iter_non_queued_failed(
        self, *, task_type: str | None = None
    ) -> Iterator[tuple[str, Mapping[str, Any], Mapping[str, Any] | None]]:
        cursor = self._conn.execute(
            "SELECT f.job_uid, f.metadata, f.payload FROM failed AS f "
            "WHERE NOT EXISTS (SELECT 1 FROM queue AS q WHERE q.job_uid = f.job_uid)"
        )
        for r in cursor:
            uid = r[0]
            if not _matches_task_type(uid, task_type):
                continue
            yield uid, _decode_json_dict(r[1]), _decode_json_optional_dict(r[2])

    def iter_attempts(
        self, *, job_uid: str | None = None, task_type: str | None = None
    ) -> Iterator[
        tuple[
            str,
            int,
            str,
            str,
            str | None,
            str | None,
            Mapping[str, Any],
            Mapping[str, Any] | None,
            Any,
        ]
    ]:
        sql = (
            "SELECT attempt_no, run_id, outcome, started_at, settled_at, "
            "job_snapshot, files, result, error_message, job_uid "
            "FROM attempts"
        )
        params: list[Any] = []
        if job_uid is not None:
            sql += " WHERE job_uid = ? ORDER BY attempt_no ASC"
            params.append(job_uid)
        else:
            sql += " ORDER BY attempt_id ASC"

        cursor = self._conn.execute(sql, params)
        for row in cursor:
            uid = row[9]
            if not _matches_task_type(uid, task_type):
                continue
            yield (
                uid,
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                _decode_json_dict(row[5]),
                _decode_json_dict(row[6]),
                _decode_attempt_result(row[7], row[8]),
            )

    def close(self) -> None:
        if not self._closed:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass
            self._closed = True


class _MemoryTableAdapter:
    """统一内存表视图适配器：同时服务 InMemoryStateBackend、SQLite copy 与 EditTransaction。

    大字段 JSON 列在 SQLite copy 模式下保持原始 str，仅在命中且通过过滤条件后
    惰性解码，避免无谓全量反序列化并保证零数据库文件句柄占用。
    """

    def __init__(
        self,
        *,
        queue_rows: list[tuple[str, Any, str, str | None, int | None, str | None]],
        successes: dict[str, Any],
        failed: dict[str, tuple[Any, Any]],
        attempts: list[tuple[str, int, str, str, str | None, str | None, Any, Any, Any, str | None]],
    ) -> None:
        self._queue_rows = queue_rows
        self._queue_by_uid = {row[0]: row for row in queue_rows}
        self._successes = successes
        self._failed = failed
        self._attempts = attempts
        self._closed = False

    @classmethod
    def from_sqlite_copy(cls, db_path: Path) -> _MemoryTableAdapter:
        """在单一只读事务内读取原始行并立即释放连接（mode='copy'）。"""
        conn = open_existing_sqlite_readonly(db_path, begin_transaction=True)
        try:
            q_raw = conn.execute(
                "SELECT job_uid, job_snapshot, status, run_id, attempt_no, started_at "
                "FROM queue ORDER BY queue_position ASC"
            ).fetchall()
            s_raw = conn.execute("SELECT job_uid, metadata FROM successes").fetchall()
            f_raw = conn.execute("SELECT job_uid, metadata, payload FROM failed").fetchall()
            a_raw = conn.execute(
                "SELECT job_uid, attempt_no, run_id, outcome, started_at, settled_at, "
                "job_snapshot, files, result, error_message "
                "FROM attempts ORDER BY attempt_id ASC"
            ).fetchall()
        finally:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        queue_rows = [
            (r[0], r[1], r[2] if r[2] else "pending", r[3], r[4], r[5])
            for r in q_raw
        ]
        successes = {r[0]: r[1] for r in s_raw}
        failed = {r[0]: (r[1], r[2]) for r in f_raw}
        attempts = [
            (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9])
            for r in a_raw
        ]
        return cls(
            queue_rows=queue_rows,
            successes=successes,
            failed=failed,
            attempts=attempts,
        )

    @classmethod
    def from_consistent_snapshot(cls, snapshot: Mapping[str, Any]) -> _MemoryTableAdapter:
        """从内存后端 load_consistent_snapshot() 捕获的一致字典视图构建适配器。"""
        failure_payloads = snapshot["failure_payloads"]
        failed = {
            uid: (meta, failure_payloads.get(uid))
            for uid, meta in snapshot["failed"].items()
        }
        return cls.from_edit_tables(
            queue_rows=snapshot["queue_jobs"],
            successes=snapshot["successes"],
            failed=failed,
            attempts=snapshot["attempts"],
        )

    @classmethod
    def from_edit_tables(
        cls,
        *,
        queue_rows: Sequence[Mapping[str, Any]],
        successes: Mapping[str, Mapping[str, Any]],
        failed: Mapping[str, tuple[Mapping[str, Any], Mapping[str, Any] | None]],
        attempts: Sequence[Mapping[str, Any]],
    ) -> _MemoryTableAdapter:
        """从内存字典表视图（load_consistent_snapshot 与 EditTransaction 共用）构建适配器。"""
        q_list = [
            (
                row["job_uid"],
                row["job"],
                row["status"],
                row.get("run_id"),
                row.get("attempt_no"),
                row.get("started_at"),
            )
            for row in queue_rows
        ]
        a_list = [
            (
                item["job_uid"],
                item["attempt_no"],
                item["run_id"],
                item["outcome"],
                item.get("started_at"),
                item.get("settled_at"),
                item.get("job") or {},
                item.get("files"),
                item.get("result"),
                item.get("error_message"),
            )
            for item in attempts
            if isinstance(item.get("job_uid"), str) and item.get("job_uid")
        ]
        return cls(
            queue_rows=q_list,
            successes=dict(successes),
            failed=dict(failed),
            attempts=a_list,
        )

    def get_queued(
        self, uid: str
    ) -> tuple[Mapping[str, Any], str, str | None, int | None, str | None] | None:
        row = self._queue_by_uid.get(uid)
        if row is None:
            return None
        return _decode_json_dict(row[1]), row[2], row[3], row[4], row[5]

    def get_success(self, uid: str) -> Mapping[str, Any] | None:
        if uid not in self._successes:
            return None
        return _decode_json_dict(self._successes[uid])

    def get_failed(self, uid: str) -> tuple[Mapping[str, Any], Mapping[str, Any] | None] | None:
        if uid not in self._failed:
            return None
        raw_meta, raw_payload = self._failed[uid]
        return _decode_json_dict(raw_meta), _decode_json_optional_dict(raw_payload)

    def iter_queued(
        self, *, status: str | None = None, task_type: str | None = None
    ) -> Iterator[tuple[str, Mapping[str, Any], str, str | None, int | None, str | None]]:
        for row in self._queue_rows:
            uid = row[0]
            st = row[2]
            if status is not None and st != status:
                continue
            if not _matches_task_type(uid, task_type):
                continue
            yield uid, _decode_json_dict(row[1]), st, row[3], row[4], row[5]

    def iter_non_queued_successes(
        self, *, task_type: str | None = None
    ) -> Iterator[tuple[str, Mapping[str, Any]]]:
        for uid, raw_meta in self._successes.items():
            if uid in self._queue_by_uid:
                continue
            if not _matches_task_type(uid, task_type):
                continue
            yield uid, _decode_json_dict(raw_meta)

    def iter_non_queued_failed(
        self, *, task_type: str | None = None
    ) -> Iterator[tuple[str, Mapping[str, Any], Mapping[str, Any] | None]]:
        for uid, (raw_meta, raw_payload) in self._failed.items():
            if uid in self._queue_by_uid:
                continue
            if not _matches_task_type(uid, task_type):
                continue
            yield uid, _decode_json_dict(raw_meta), _decode_json_optional_dict(raw_payload)

    def iter_attempts(
        self, *, job_uid: str | None = None, task_type: str | None = None
    ) -> Iterator[
        tuple[
            str,
            int,
            str,
            str,
            str | None,
            str | None,
            Mapping[str, Any],
            Mapping[str, Any] | None,
            Any,
        ]
    ]:
        if job_uid is not None:
            rows = [r for r in self._attempts if r[0] == job_uid]
            rows.sort(key=lambda r: r[1] if r[1] is not None else 0)
        else:
            rows = self._attempts

        for row in rows:
            uid = row[0]
            if not _matches_task_type(uid, task_type):
                continue
            yield (
                uid,
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                _decode_json_dict(row[6]),
                _decode_json_optional_dict(row[7]),
                _decode_attempt_result(row[8], row[9]),
            )

    def close(self) -> None:
        if not self._closed:
            self._queue_rows.clear()
            self._queue_by_uid.clear()
            self._successes.clear()
            self._failed.clear()
            self._attempts.clear()
            self._closed = True


# ── 单一深投影核心 StateSnapshot ───────────────────────────────────────────


class StateSnapshot:
    """一致只读状态快照（JSON 兼容字典查询与可读报告）。"""

    def __init__(
        self,
        source: str | Path | TaskLite,
        *,
        mode: Literal["lazy", "copy"] = "lazy",
    ) -> None:
        if not isinstance(mode, str) or mode not in _VALID_SNAPSHOT_MODES:
            raise TaskLiteError(
                f"mode must be 'lazy' or 'copy', got {mode!r}"
            )
        self._source = source
        self._closed = False
        adapter: _SnapshotAdapter
        if isinstance(source, (str, Path)):
            db_path = Path(source)
            adapter = (
                _MemoryTableAdapter.from_sqlite_copy(db_path)
                if mode == "copy"
                else _SqliteStreamingAdapter(db_path)
            )
        else:
            from .pipeline import TaskLite

            if not isinstance(source, TaskLite):
                raise TaskLiteError(
                    "source must be a str, Path, or TaskLite instance, "
                    f"got {type(source).__name__}"
                )
            kind, payload = source._open_snapshot_source()
            if kind == "sqlite":
                assert isinstance(payload, Path)
                adapter = (
                    _MemoryTableAdapter.from_sqlite_copy(payload)
                    if mode == "copy"
                    else _SqliteStreamingAdapter(payload)
                )
            else:
                assert isinstance(payload, Mapping)
                adapter = _MemoryTableAdapter.from_consistent_snapshot(payload)
        self._adapter: _SnapshotAdapter = adapter

    @classmethod
    def open(
        cls,
        source: str | Path | TaskLite,
        *,
        mode: Literal["lazy", "copy"] = "lazy",
    ) -> StateSnapshot:
        """打开只读快照（支持数据库路径或 TaskLite 实例；mode='lazy'|'copy'）。"""
        return cls(source, mode=mode)

    @classmethod
    def _from_tables(
        cls,
        *,
        queue_rows: Sequence[Mapping[str, Any]],
        successes: Mapping[str, Mapping[str, Any]],
        failed: Mapping[str, tuple[Mapping[str, Any], Mapping[str, Any] | None]],
        attempts: Sequence[Mapping[str, Any]],
        source: Any = None,
    ) -> StateSnapshot:
        """包内受控表投影入口：将三表工作图与尝试轨迹副本封装为只读 StateSnapshot。"""
        instance = cls.__new__(cls)
        instance._source = source
        instance._closed = False
        instance._adapter = _MemoryTableAdapter.from_edit_tables(
            queue_rows=queue_rows,
            successes=successes,
            failed=failed,
            attempts=attempts,
        )
        return instance

    @staticmethod
    def report(
        first: StateSnapshot,
        second: StateSnapshot | None = None,
    ) -> str:
        """生成面向人类阅读的单快照状态报告或双快照增量对比报告。"""
        if not isinstance(first, StateSnapshot):
            raise TaskLiteError(
                f"first must be a StateSnapshot instance, got {type(first).__name__}"
            )
        if first.closed:
            raise TaskLiteError("Cannot generate report from a closed StateSnapshot")
        if second is not None:
            if not isinstance(second, StateSnapshot):
                raise TaskLiteError(
                    "second must be a StateSnapshot instance or None, "
                    f"got {type(second).__name__}"
                )
            if second.closed:
                raise TaskLiteError("Cannot generate report from a closed StateSnapshot")
        return render_snapshot_report(first, second)

    def get_job(self, uid: str) -> dict[str, Any] | None:
        """查询指定 Job 的当前状态与配置/结果投影（不存在返回 None）。"""
        if self._closed:
            raise TaskLiteError("StateSnapshot is closed")
        if not isinstance(uid, str):
            raise TaskLiteError(f"job uid must be a str, got {type(uid).__name__}")
        if not uid:
            raise TaskLiteError("job uid must be a non-empty str")
        try:
            parse_job_uid(uid)
        except Exception:
            return None

        queued = self._adapter.get_queued(uid)
        if queued is not None:
            job_snap, status, run_id, attempt_no, started_at = queued
            return _project_queued_job(uid, job_snap, status, run_id, attempt_no, started_at)

        succ_meta = self._adapter.get_success(uid)
        if succ_meta is not None:
            return _project_succeeded_job(uid, succ_meta)

        failed_entry = self._adapter.get_failed(uid)
        if failed_entry is not None:
            fail_meta, payload = failed_entry
            return _project_failed_job(uid, fail_meta, payload)

        return None

    def iter_jobs(
        self,
        *,
        status: str | None = None,
        task_type: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """按当前状态与 task_type 遍历 Job 记录（status=None 包含四态；不承诺顺序）。"""
        if self._closed:
            raise TaskLiteError("StateSnapshot is closed")
        if status is not None and status not in ("pending", "running", "succeeded", "failed"):
            raise TaskLiteError(
                f"status must be one of 'pending', 'running', 'succeeded', 'failed', or None; got {status!r}"
            )
        if task_type is not None and not isinstance(task_type, str):
            raise TaskLiteError(f"task_type must be a str or None, got {type(task_type).__name__}")

        if status is None or status in ("pending", "running"):
            for uid, job_snap, st, run_id, attempt_no, started_at in self._adapter.iter_queued(
                status=status, task_type=task_type
            ):
                if self._closed:
                    raise TaskLiteError("StateSnapshot is closed")
                yield _project_queued_job(uid, job_snap, st, run_id, attempt_no, started_at)

        if status is None or status == "succeeded":
            for uid, succ_meta in self._adapter.iter_non_queued_successes(task_type=task_type):
                if self._closed:
                    raise TaskLiteError("StateSnapshot is closed")
                yield _project_succeeded_job(uid, succ_meta)

        if status is None or status == "failed":
            for uid, fail_meta, payload in self._adapter.iter_non_queued_failed(
                task_type=task_type
            ):
                if self._closed:
                    raise TaskLiteError("StateSnapshot is closed")
                yield _project_failed_job(uid, fail_meta, payload)

    def iter_attempts(
        self,
        *,
        job_uid: str | None = None,
        task_type: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """遍历物理 Attempt 历史记录（指定 job_uid 时按 attempt_no 升序）。"""
        if self._closed:
            raise TaskLiteError("StateSnapshot is closed")
        if job_uid is not None and not isinstance(job_uid, str):
            raise TaskLiteError(f"job_uid must be a str or None, got {type(job_uid).__name__}")
        if task_type is not None and not isinstance(task_type, str):
            raise TaskLiteError(f"task_type must be a str or None, got {type(task_type).__name__}")

        for (
            uid,
            attempt_no,
            run_id,
            outcome,
            started_at,
            settled_at,
            job_snap,
            files_data,
            result_data,
        ) in self._adapter.iter_attempts(job_uid=job_uid, task_type=task_type):
            if self._closed:
                raise TaskLiteError("StateSnapshot is closed")
            yield _project_attempt(
                uid,
                attempt_no,
                run_id,
                outcome,
                started_at,
                settled_at,
                job_snap,
                files_data,
                result_data,
            )

    def close(self) -> None:
        """关闭只读快照（幂等；不关闭底层的 TaskLite 实例）。"""
        if not self._closed:
            self._adapter.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        """快照是否已关闭。"""
        return self._closed

    @property
    def source(self) -> Any:
        """打开快照的源对象或路径。"""
        return self._source

    def __enter__(self) -> StateSnapshot:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


__all__ = [
    "StateSnapshot",
]
