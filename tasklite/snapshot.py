"""StateSnapshot：只读一致状态快照与结构化 JSON 查询（ADR-0011）。

独立只读入口，支持从数据库路径或 TaskLite 实例打开；SQLite 与内存实例均
返回同一种只读快照。一个上下文提供一致快照，不创建/迁移/修复数据库，
关闭快照不关闭 TaskLite。
"""
from __future__ import annotations

import copy
from pathlib import Path
import sqlite3
from typing import TYPE_CHECKING, Any, Iterator, Mapping, Protocol

from .backend.sqlite_backend import _check_schema
from .exceptions import TaskLiteError
from .models.state import job_uid_from_snapshot
from .utils.encoding import parse_job_uid
from .utils.jsonutil import loads

if TYPE_CHECKING:
    from .pipeline import TaskLite


def _format_iso_z(ts: str | None) -> str | None:
    """时间戳统一规范为 UTC ISO 8601 字符串（以 Z 结尾，None 表示未知）。"""
    if not ts or not isinstance(ts, str):
        return None
    if ts.endswith("+00:00"):
        return ts[:-6] + "Z"
    return ts


def _normalize_job_config(snap: Mapping[str, Any]) -> dict[str, Any]:
    """提取有效配置快照（包含身份、payload、depends_on、files 及资源、预算、超时、Trigger）。"""
    raw_files = snap.get("files") or {}
    files_dict: dict[str, Any] = {}
    for name, spec in raw_files.items():
        if hasattr(spec, "to_dict"):
            files_dict[name] = spec.to_dict()
        elif isinstance(spec, dict):
            files_dict[name] = copy.deepcopy(spec)
        else:
            files_dict[name] = spec
    return {
        "task_type": snap.get("task_type"),
        "job_id": snap.get("job_id"),
        "payload": copy.deepcopy(snap.get("payload") or {}),
        "depends_on": list(snap.get("depends_on") or []),
        "files": files_dict,
        "resources": dict(snap.get("resources") or {}),
        "max_retries_per_run": snap.get("max_retries_per_run"),
        "timeout_seconds": snap.get("timeout_seconds"),
        "trigger": snap.get("trigger"),
    }


def _normalize_outer_files(raw_files: Mapping[str, Any] | None) -> dict[str, Any]:
    """外层 files 规范化：按名称记录已解析预声明与动态声明并集（必须含绝对 path）。"""
    if not raw_files:
        return {}
    normalized: dict[str, Any] = {}
    for name, spec in raw_files.items():
        if hasattr(spec, "to_dict"):
            d = spec.to_dict()
        elif isinstance(spec, dict):
            d = copy.deepcopy(spec)
        else:
            continue
        if d.get("path") is not None:
            normalized[name] = {
                "path": d["path"],
                "role": d.get("role", "output"),
                "delete_existing": bool(d.get("delete_existing", False)),
                "delete_on_failure": bool(d.get("delete_on_failure", False)),
                "delete_on_success": bool(d.get("delete_on_success", False)),
            }
    return normalized


def _normalize_result(raw_result: Any) -> dict[str, Any] | None:
    """结算结果规范化为 unified 字典形状（message / exception / metadata / annotations）。"""
    if raw_result is None:
        return None
    if not isinstance(raw_result, dict):
        return {
            "message": str(raw_result),
            "exception": None,
            "metadata": {},
            "annotations": {},
        }
    msg = raw_result.get("message")
    if msg is not None:
        msg = str(msg)
    raw_exc = raw_result.get("exception")
    exc_dict: dict[str, Any] | None = None
    if isinstance(raw_exc, dict):
        exc_dict = {
            "type": str(raw_exc.get("type", "Exception")),
            "message": str(raw_exc.get("message", "")),
            "traceback": (
                str(raw_exc["traceback"])
                if raw_exc.get("traceback") is not None
                else None
            ),
        }
    meta = raw_result.get("metadata")
    meta_dict = copy.deepcopy(meta) if isinstance(meta, dict) else {}
    ann = raw_result.get("annotations")
    ann_dict = copy.deepcopy(ann) if isinstance(ann, dict) else {}
    return {
        "message": msg,
        "exception": exc_dict,
        "metadata": meta_dict,
        "annotations": ann_dict,
    }


def _build_succeeded_job_dict(
    uid: str,
    succ_meta: dict[str, Any],
    att: dict[str, Any] | None,
) -> dict[str, Any]:
    is_manual = (
        succ_meta.get("annotations", {}).get("completion_source") == "manual"
    )
    if is_manual:
        run_id = None
        attempt_no = None
        started_at = None
        settled_at = _format_iso_z(
            succ_meta.get("settled_at") or succ_meta.get("last_succeeded_at")
        )
        job_snap = succ_meta.get("job_snapshot") or {}
        outer_files = _normalize_outer_files(
            succ_meta.get("files") or job_snap.get("files")
        )
        result = _normalize_result(
            succ_meta.get("result")
            or {
                "message": None,
                "exception": None,
                "metadata": {},
                "annotations": succ_meta.get("annotations") or {},
            }
        )
    else:
        if att:
            run_id = att.get("run_id")
            attempt_no = att.get("attempt_no")
            started_at = _format_iso_z(att.get("started_at"))
            settled_at = _format_iso_z(
                att.get("settled_at") or succ_meta.get("last_succeeded_at")
            )
            job_snap = att.get("job_snapshot") or succ_meta.get("job_snapshot") or {}
            files_data = (
                att.get("files")
                or succ_meta.get("files")
                or job_snap.get("files")
            )
            outer_files = _normalize_outer_files(files_data)
            result = _normalize_result(att.get("result") or succ_meta.get("result"))
        else:
            run_id = succ_meta.get("run_id") or succ_meta.get("last_success_run_id")
            attempt_no = succ_meta.get("attempt_no")
            started_at = _format_iso_z(succ_meta.get("started_at"))
            settled_at = _format_iso_z(
                succ_meta.get("last_succeeded_at") or succ_meta.get("settled_at")
            )
            job_snap = succ_meta.get("job_snapshot") or {}
            outer_files = _normalize_outer_files(
                succ_meta.get("files") or job_snap.get("files")
            )
            result = _normalize_result(succ_meta.get("result"))

    return {
        "job_uid": uid,
        "status": "succeeded",
        "job": _normalize_job_config(job_snap),
        "files": outer_files,
        "result": result,
        "run_id": run_id,
        "attempt_no": attempt_no,
        "started_at": started_at,
        "settled_at": settled_at,
    }


def _build_failed_job_dict(
    uid: str,
    fail_meta: dict[str, Any],
    payload: dict[str, Any] | None,
    att: dict[str, Any] | None,
) -> dict[str, Any]:
    is_manual = (
        fail_meta.get("annotations", {}).get("completion_source") == "manual"
    )
    if is_manual:
        run_id = None
        attempt_no = None
        started_at = None
        settled_at = _format_iso_z(fail_meta.get("settled_at"))
        job_snap = copy.deepcopy(fail_meta.get("job_snapshot") or {})
        if payload is not None and not job_snap.get("payload"):
            job_snap["payload"] = copy.deepcopy(payload)
        outer_files = _normalize_outer_files(
            fail_meta.get("files") or job_snap.get("files")
        )
        result = _normalize_result(
            fail_meta.get("result")
            or {
                "message": fail_meta.get("error_message") or fail_meta.get("message"),
                "exception": None,
                "metadata": {},
                "annotations": fail_meta.get("annotations") or {},
            }
        )
    else:
        if att:
            run_id = att.get("run_id")
            attempt_no = att.get("attempt_no")
            started_at = _format_iso_z(att.get("started_at"))
            settled_at = _format_iso_z(
                att.get("settled_at") or fail_meta.get("settled_at")
            )
            job_snap = copy.deepcopy(
                att.get("job_snapshot") or fail_meta.get("job_snapshot") or {}
            )
            if payload is not None and not job_snap.get("payload"):
                job_snap["payload"] = copy.deepcopy(payload)
            files_data = (
                att.get("files")
                or fail_meta.get("files")
                or job_snap.get("files")
            )
            outer_files = _normalize_outer_files(files_data)
            result = _normalize_result(
                att.get("result")
                or fail_meta.get("result")
                or {
                    "message": (
                        att.get("error_message")
                        or fail_meta.get("error_message")
                        or fail_meta.get("message")
                    )
                }
            )
        else:
            run_id = fail_meta.get("run_id")
            attempt_no = fail_meta.get("attempt_no")
            started_at = _format_iso_z(fail_meta.get("started_at"))
            settled_at = _format_iso_z(fail_meta.get("settled_at"))
            job_snap = copy.deepcopy(fail_meta.get("job_snapshot") or {})
            if payload is not None and not job_snap.get("payload"):
                job_snap["payload"] = copy.deepcopy(payload)
            outer_files = _normalize_outer_files(
                fail_meta.get("files") or job_snap.get("files")
            )
            result = _normalize_result(
                fail_meta.get("result")
                or {
                    "message": fail_meta.get("error_message")
                    or fail_meta.get("message")
                }
            )

    return {
        "job_uid": uid,
        "status": "failed",
        "job": _normalize_job_config(job_snap),
        "files": outer_files,
        "result": result,
        "run_id": run_id,
        "attempt_no": attempt_no,
        "started_at": started_at,
        "settled_at": settled_at,
    }


class _SnapshotReader(Protocol):
    def get_job(self, uid: str) -> dict[str, Any] | None: ...
    def iter_jobs(
        self,
        *,
        status: str | None = None,
        task_type: str | None = None,
    ) -> Iterator[dict[str, Any]]: ...
    def iter_attempts(
        self,
        *,
        job_uid: str | None = None,
        task_type: str | None = None,
    ) -> Iterator[dict[str, Any]]: ...
    def close(self) -> None: ...


class _SqliteSnapshotReader:
    """基于独立 SQLite 只读事务的一致快照读取器。"""

    def __init__(self, db_path: Path) -> None:
        if not db_path.exists():
            raise TaskLiteError(f"Database file does not exist: {db_path}")
        if db_path.is_dir():
            raise TaskLiteError(f"Path is a directory, not a database file: {db_path}")
        uri = db_path.resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=30.0)
        try:
            conn.execute("BEGIN")
            if _check_schema(conn, db_path) == 0:
                raise TaskLiteError(f"Uninitialized TaskLite database at {db_path}")
        except BaseException:
            conn.close()
            raise
        self._conn = conn
        self._closed = False

    def _load_latest_attempt(self, uid: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT attempt_no, run_id, outcome, started_at, settled_at, "
            "job_snapshot, files, result, error_message "
            "FROM attempts WHERE job_uid = ? ORDER BY attempt_id DESC LIMIT 1",
            (uid,),
        ).fetchone()
        if not row:
            return None
        return {
            "attempt_no": row[0],
            "run_id": row[1],
            "outcome": row[2],
            "started_at": row[3],
            "settled_at": row[4],
            "job_snapshot": loads(row[5]) if row[5] else None,
            "files": loads(row[6]) if row[6] else None,
            "result": loads(row[7]) if row[7] else None,
            "error_message": row[8],
        }

    def get_job(self, uid: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at "
            "FROM queue WHERE job_uid = ?",
            (uid,),
        ).fetchone()
        if row:
            job_snap = loads(row[2]) if row[2] else {}
            st = row[3] if row[3] else "pending"
            run_id = row[4] if st == "running" else None
            attempt_no = row[5] if st == "running" else None
            started_at = _format_iso_z(row[6]) if st == "running" else None
            return {
                "job_uid": uid,
                "status": st,
                "job": _normalize_job_config(job_snap),
                "files": _normalize_outer_files(job_snap.get("files")),
                "result": None,
                "run_id": run_id,
                "attempt_no": attempt_no,
                "started_at": started_at,
                "settled_at": None,
            }

        s_row = self._conn.execute(
            "SELECT metadata FROM successes WHERE job_uid = ?", (uid,)
        ).fetchone()
        if s_row:
            succ_meta = loads(s_row[0]) if s_row[0] else {}
            att = self._load_latest_attempt(uid)
            return _build_succeeded_job_dict(uid, succ_meta, att)

        f_row = self._conn.execute(
            "SELECT metadata, payload FROM failed WHERE job_uid = ?", (uid,)
        ).fetchone()
        if f_row:
            fail_meta = loads(f_row[0]) if f_row[0] else {}
            payload = loads(f_row[1]) if f_row[1] else None
            att = self._load_latest_attempt(uid)
            return _build_failed_job_dict(uid, fail_meta, payload, att)

        return None

    def iter_jobs(
        self,
        *,
        status: str | None = None,
        task_type: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        q_rows = self._conn.execute(
            "SELECT job_uid, queue_position, job_snapshot, status, run_id, attempt_no, started_at "
            "FROM queue ORDER BY queue_position ASC"
        )

        for r in q_rows:
            uid = r[0]
            st = r[3]
            if status is not None and st != status:
                continue
            if task_type is not None:
                try:
                    tt, _ = parse_job_uid(uid)
                except Exception:
                    continue
                if tt != task_type:
                    continue
            job_snap = loads(r[2]) if r[2] else {}
            run_id = r[4] if st == "running" else None
            attempt_no = r[5] if st == "running" else None
            started_at = _format_iso_z(r[6]) if st == "running" else None
            yield {
                "job_uid": uid,
                "status": st,
                "job": _normalize_job_config(job_snap),
                "files": _normalize_outer_files(job_snap.get("files")),
                "result": None,
                "run_id": run_id,
                "attempt_no": attempt_no,
                "started_at": started_at,
                "settled_at": None,
            }

        if status is None or status == "succeeded":
            s_rows = self._conn.execute(
                "SELECT s.job_uid, s.metadata FROM successes AS s "
                "WHERE NOT EXISTS ("
                "SELECT 1 FROM queue AS q WHERE q.job_uid = s.job_uid"
                ")"
            )
            for r in s_rows:
                uid = r[0]
                if task_type is not None:
                    try:
                        tt, _ = parse_job_uid(uid)
                    except Exception:
                        continue
                    if tt != task_type:
                        continue
                succ_meta = loads(r[1]) if r[1] else {}
                att = self._load_latest_attempt(uid)
                yield _build_succeeded_job_dict(uid, succ_meta, att)

        if status is None or status == "failed":
            f_rows = self._conn.execute(
                "SELECT f.job_uid, f.metadata, f.payload FROM failed AS f "
                "WHERE NOT EXISTS ("
                "SELECT 1 FROM queue AS q WHERE q.job_uid = f.job_uid"
                ")"
            )
            for r in f_rows:
                uid = r[0]
                if task_type is not None:
                    try:
                        tt, _ = parse_job_uid(uid)
                    except Exception:
                        continue
                    if tt != task_type:
                        continue
                fail_meta = loads(r[1]) if r[1] else {}
                payload = loads(r[2]) if r[2] else None
                att = self._load_latest_attempt(uid)
                yield _build_failed_job_dict(uid, fail_meta, payload, att)

    def iter_attempts(
        self,
        *,
        job_uid: str | None = None,
        task_type: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        sql = (
            "SELECT attempt_no, run_id, outcome, started_at, settled_at, "
            "job_snapshot, files, result, error_message, job_uid "
            "FROM attempts"
        )
        params: list[Any] = []
        if job_uid is not None:
            sql += " WHERE job_uid = ? ORDER BY attempt_no ASC"
            params.append(job_uid)

        cursor = self._conn.execute(sql, params)
        for row in cursor:
            uid = row[9]
            if task_type is not None:
                try:
                    tt, _ = parse_job_uid(uid)
                except Exception:
                    continue
                if tt != task_type:
                    continue
            job_snap = loads(row[5]) if row[5] else {}
            files_data = loads(row[6]) if row[6] else {}
            res_data = loads(row[7]) if row[7] else {"message": row[8]}
            yield {
                "job_uid": uid,
                "outcome": row[2],
                "job": _normalize_job_config(job_snap),
                "files": _normalize_outer_files(files_data),
                "result": _normalize_result(res_data),
                "run_id": row[1],
                "attempt_no": row[0],
                "started_at": _format_iso_z(row[3]),
                "settled_at": _format_iso_z(row[4]),
            }

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


class _MemorySnapshotReader:
    """基于内存版本快照的一致快照读取器（不在用户上下文持后端锁）。"""

    def __init__(self, backend: Any) -> None:
        with backend._lock:
            self._queue = copy.deepcopy(backend._queue)
            self._successes = copy.deepcopy(backend._successes)
            self._failed = copy.deepcopy(backend._failed)
            self._failure_payloads = copy.deepcopy(backend._failure_payloads)
            self._attempts = copy.deepcopy(backend._attempts)
        self._closed = False

    def _load_latest_attempt(self, uid: str) -> dict[str, Any] | None:
        matched = [v for v in self._attempts.values() if v["job_uid"] == uid]
        if not matched:
            return None
        latest = matched[-1]
        return dict(latest, job_snapshot=latest["job"])

    def get_job(self, uid: str) -> dict[str, Any] | None:
        for j in self._queue:
            if job_uid_from_snapshot(j) == uid:
                st = j.get("status", "pending")
                run_id = j.get("run_id") if st == "running" else None
                attempt_no = j.get("attempt_no") if st == "running" else None
                started_at = _format_iso_z(j.get("started_at")) if st == "running" else None
                return {
                    "job_uid": uid,
                    "status": st,
                    "job": _normalize_job_config(j),
                    "files": _normalize_outer_files(j.get("files")),
                    "result": None,
                    "run_id": run_id,
                    "attempt_no": attempt_no,
                    "started_at": started_at,
                    "settled_at": None,
                }

        if uid in self._successes:
            succ_meta = self._successes[uid]
            att = self._load_latest_attempt(uid)
            return _build_succeeded_job_dict(uid, succ_meta, att)

        if uid in self._failed:
            fail_meta = self._failed[uid]
            payload = self._failure_payloads.get(uid)
            att = self._load_latest_attempt(uid)
            return _build_failed_job_dict(uid, fail_meta, payload, att)

        return None

    def iter_jobs(
        self,
        *,
        status: str | None = None,
        task_type: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        queue_uids: set[str] = set()
        for j in self._queue:
            uid = job_uid_from_snapshot(j)
            queue_uids.add(uid)
            st = j.get("status", "pending")
            if status is not None and st != status:
                continue
            if task_type is not None:
                try:
                    tt, _ = parse_job_uid(uid)
                except Exception:
                    continue
                if tt != task_type:
                    continue
            run_id = j.get("run_id") if st == "running" else None
            attempt_no = j.get("attempt_no") if st == "running" else None
            started_at = _format_iso_z(j.get("started_at")) if st == "running" else None
            yield {
                "job_uid": uid,
                "status": st,
                "job": _normalize_job_config(j),
                "files": _normalize_outer_files(j.get("files")),
                "result": None,
                "run_id": run_id,
                "attempt_no": attempt_no,
                "started_at": started_at,
                "settled_at": None,
            }

        if status is None or status == "succeeded":
            for uid, succ_meta in self._successes.items():
                if uid in queue_uids:
                    continue
                if task_type is not None:
                    try:
                        tt, _ = parse_job_uid(uid)
                    except Exception:
                        continue
                    if tt != task_type:
                        continue
                att = self._load_latest_attempt(uid)
                yield _build_succeeded_job_dict(uid, succ_meta, att)

        if status is None or status == "failed":
            for uid, fail_meta in self._failed.items():
                if uid in queue_uids:
                    continue
                if task_type is not None:
                    try:
                        tt, _ = parse_job_uid(uid)
                    except Exception:
                        continue
                    if tt != task_type:
                        continue
                payload = self._failure_payloads.get(uid)
                att = self._load_latest_attempt(uid)
                yield _build_failed_job_dict(uid, fail_meta, payload, att)

    def iter_attempts(
        self,
        *,
        job_uid: str | None = None,
        task_type: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        raw_list = list(self._attempts.values())
        if job_uid is not None:
            raw_list = [a for a in raw_list if a["job_uid"] == job_uid]
            raw_list.sort(key=lambda x: x.get("attempt_no", 0))

        for item in raw_list:
            uid = item["job_uid"]
            if not uid:
                continue
            if task_type is not None:
                try:
                    tt, _ = parse_job_uid(uid)
                except Exception:
                    continue
                if tt != task_type:
                    continue
            job_snap = item["job"]
            yield {
                "job_uid": uid,
                "outcome": item["outcome"],
                "job": _normalize_job_config(job_snap),
                "files": _normalize_outer_files(item.get("files")),
                "result": _normalize_result(item.get("result")),
                "run_id": item["run_id"],
                "attempt_no": item["attempt_no"],
                "started_at": _format_iso_z(item["started_at"]),
                "settled_at": _format_iso_z(item["settled_at"]),
            }

    def close(self) -> None:
        if not self._closed:
            self._queue.clear()
            self._successes.clear()
            self._failed.clear()
            self._failure_payloads.clear()
            self._attempts.clear()
            self._closed = True


class StateSnapshot:
    """一致只读状态快照（JSON 兼容字典查询）。"""

    def __init__(self, source: str | Path | "TaskLite") -> None:
        self._source = source
        self._closed = False
        reader: _SnapshotReader
        if isinstance(source, (str, Path)):
            reader = _SqliteSnapshotReader(Path(source))
        else:
            from .pipeline import TaskLite

            if not isinstance(source, TaskLite):
                raise TaskLiteError(
                    "source must be a str, Path, or TaskLite instance, "
                    f"got {type(source).__name__}"
                )
            if source._closed:
                raise TaskLiteError(
                    "Cannot open StateSnapshot from a closed TaskLite instance."
                )
            backend = source._snapshot_backend()
            if source.state_path is not None:
                reader = _SqliteSnapshotReader(source.state_path)
            else:
                reader = _MemorySnapshotReader(backend)
        self._reader = reader

    @classmethod
    def open(cls, source: str | Path | "TaskLite") -> StateSnapshot:
        """打开只读快照（支持数据库路径或 TaskLite 实例）。"""
        return cls(source)

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
        return self._reader.get_job(uid)

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

        for job in self._reader.iter_jobs(status=status, task_type=task_type):
            if self._closed:
                raise TaskLiteError("StateSnapshot is closed")
            yield job

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

        for att in self._reader.iter_attempts(job_uid=job_uid, task_type=task_type):
            if self._closed:
                raise TaskLiteError("StateSnapshot is closed")
            yield att

    def close(self) -> None:
        """关闭只读快照（幂等；不关闭底层的 TaskLite 实例）。"""
        if not self._closed:
            self._reader.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        """快照是否已关闭。"""
        return self._closed

    @property
    def is_closed(self) -> bool:
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
