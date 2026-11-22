"""Job 快照与终态档案载体单一事实源。

统一承载：
1. 9 字段规范 ``job_snapshot`` 投影与从 ``Successes`` / ``Failures`` 档案反提快照；
2. Job 摄入期 Task→Job 四项规格继承与提交期 ``FileSpec`` 解析；
3. ``result``（4 键）、外层 ``files``（5 键）、``artifacts`` 以及执行态/人工态
   ``Successes`` / ``Failures`` 档案字典的规范构造与归一化。

分层红线：本模块位于 ``tasklite/models/``，严禁导入 ``engine`` / ``backend`` / ``wrappers``。
"""
from __future__ import annotations

import copy
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..utils.artifact import PathPolicy
from ..utils.clock import utc_now_iso
from ..utils.encoding import parse_job_uid
from ..utils.jsonutil import dumps
from .artifact import FileSpec, resolve_job_files_config
from .job import inject_worker_resource
from .result import validate_strict_json_object
from .task import Task

logger = logging.getLogger("tasklite")

EFFECTIVE_JOB_CONFIG_KEYS: tuple[str, ...] = (
    "task_type",
    "job_id",
    "payload",
    "depends_on",
    "files",
    "resources",
    "max_retries_per_run",
    "timeout_seconds",
    "trigger",
)


def extract_effective_job_config(job_snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """提取规范 9 字段作业配置投影（同 UID 幂等/冲突比较与只读快照共用）。"""
    raw_files = job_snapshot.get("files") or {}
    files_dict: dict[str, Any] = {}
    if isinstance(raw_files, Mapping):
        for name, spec in raw_files.items():
            if hasattr(spec, "to_dict"):
                files_dict[name] = spec.to_dict()
            elif isinstance(spec, Mapping):
                files_dict[name] = copy.deepcopy(dict(spec))
            else:
                files_dict[name] = spec
    return {
        "task_type": job_snapshot.get("task_type"),
        "job_id": job_snapshot.get("job_id"),
        "payload": copy.deepcopy(job_snapshot.get("payload") or {}),
        "depends_on": list(job_snapshot.get("depends_on") or []),
        "files": files_dict,
        "resources": dict(job_snapshot.get("resources") or {}),
        "max_retries_per_run": job_snapshot.get("max_retries_per_run"),
        "timeout_seconds": job_snapshot.get("timeout_seconds"),
        "trigger": job_snapshot.get("trigger"),
    }


def fallback_job_snapshot(uid: str) -> dict[str, Any]:
    """档案缺少 ``job_snapshot`` 时，按规范 UID 构造最小可恢复快照。"""
    task_type, job_id = parse_job_uid(uid)
    return {
        "task_type": task_type,
        "job_id": job_id,
        "payload": {},
        "depends_on": [],
        "files": {},
        "resources": {},
    }


def extract_job_snapshot_from_archive(
    uid: str,
    archive_meta: Mapping[str, Any],
    *,
    payload: Mapping[str, Any] | None = None,
    fill_missing_identity: bool = False,
) -> dict[str, Any]:
    """从 ``Successes`` 或 ``Failures`` 档案反提 ``job_snapshot``（失败档案按需回填独立 payload）。"""
    raw_snap = archive_meta.get("job_snapshot")
    if isinstance(raw_snap, Mapping):
        snap = copy.deepcopy(dict(raw_snap))
    elif fill_missing_identity:
        snap = fallback_job_snapshot(uid)
    else:
        snap = {}
    if fill_missing_identity and ("task_type" not in snap or "job_id" not in snap):
        task_type, job_id = parse_job_uid(uid)
        snap.setdefault("task_type", task_type)
        snap.setdefault("job_id", job_id)
    if payload is not None and not snap.get("payload"):
        snap["payload"] = copy.deepcopy(dict(payload))
    return snap


def normalize_job_intake_snapshot(
    job_snapshot: dict[str, Any],
    *,
    task: Task | None,
    base_dir: str | Path,
) -> dict[str, Any]:
    """摄入单点规范化：payload 严格隔离、Task 四项规格继承、提交期 FileSpec 解析与 worker 槽位注入。"""
    job_snapshot["payload"] = validate_strict_json_object(
        job_snapshot.get("payload"), field_name="payload"
    )
    merged_resources: dict[str, float] = (
        dict(task.resources) if task is not None and task.resources else {}
    )
    merged_resources.update(job_snapshot.get("resources") or {})
    job_snapshot["resources"] = merged_resources

    if job_snapshot.get("max_retries_per_run") is None:
        job_snapshot["max_retries_per_run"] = task.max_retries_per_run if task is not None else 3
    if job_snapshot.get("timeout_seconds") is None:
        job_snapshot["timeout_seconds"] = task.timeout_seconds if task is not None else 3600
    if job_snapshot.get("trigger") is None:
        job_snapshot["trigger"] = task.trigger if task is not None else "once"

    if job_snapshot.get("depends_on") is None:
        job_snapshot["depends_on"] = []
    else:
        job_snapshot["depends_on"] = list(job_snapshot["depends_on"])

    job_snapshot["files"] = resolve_job_files_config(
        job_snapshot.get("files"),
        base_dir=base_dir,
        depends_on=job_snapshot["depends_on"],
    )
    inject_worker_resource(job_snapshot)
    return job_snapshot


def extract_serializable_payload(job_snapshot: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """提取可 JSON 序列化的业务 payload 快照（失败档案落盘用）；不可序列化时降级为 None。"""
    if not isinstance(job_snapshot, Mapping):
        return None
    payload = job_snapshot.get("payload")
    if not isinstance(payload, dict):
        return None
    try:
        dumps(payload)
    except (TypeError, ValueError):
        logger.warning(
            f"Job payload for {job_snapshot.get('job_id', '?')} is not "
            f"JSON-serializable; failure archive payload snapshot skipped."
        )
        return None
    return payload


def build_result_record(
    *,
    message: Any = None,
    exception: Any = None,
    metadata: Mapping[str, Any] | None = None,
    annotations: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """构造规范 4 键结算结果字典（message / exception / metadata / annotations）。"""
    return {
        "message": message,
        "exception": copy.deepcopy(exception),
        "metadata": copy.deepcopy(dict(metadata)) if isinstance(metadata, Mapping) else {},
        "annotations": copy.deepcopy(dict(annotations)) if isinstance(annotations, Mapping) else {},
    }


def normalize_result_record(raw_result: Any) -> dict[str, Any] | None:
    """将任意原始结果归一化为规范 4 键字典形状（None 保持 None）。"""
    if raw_result is None:
        return None
    if not isinstance(raw_result, Mapping):
        return build_result_record(message=str(raw_result))
    msg = raw_result.get("message")
    if msg is not None:
        msg = str(msg)
    raw_exc = raw_result.get("exception")
    exc_dict: dict[str, Any] | None = None
    if isinstance(raw_exc, Mapping):
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
    ann = raw_result.get("annotations")
    return {
        "message": msg,
        "exception": exc_dict,
        "metadata": copy.deepcopy(dict(meta)) if isinstance(meta, Mapping) else {},
        "annotations": copy.deepcopy(dict(ann)) if isinstance(ann, Mapping) else {},
    }


def normalize_outer_files(raw_files: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    """规范化外层 files（5 键：path / role / delete_existing / delete_on_failure / delete_on_success）。"""
    if not isinstance(raw_files, Mapping) or not raw_files:
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for name, spec in raw_files.items():
        if hasattr(spec, "to_dict"):
            d: Mapping[str, Any] = spec.to_dict()
        elif isinstance(spec, Mapping):
            d = spec
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


def build_artifacts_manifest(
    declared_artifacts: Mapping[str, FileSpec | str] | None,
) -> dict[str, dict[str, str]]:
    """构造规范具名产出清单（name -> {'path': str}）。"""
    manifest: dict[str, dict[str, str]] = {}
    if not declared_artifacts:
        return manifest
    for name, spec_or_path in declared_artifacts.items():
        path_val = spec_or_path.path if isinstance(spec_or_path, FileSpec) else spec_or_path
        if isinstance(name, str) and name and isinstance(path_val, str) and path_val:
            manifest[name] = {"path": path_val}
    return manifest


def _execution_envelope(
    result: Mapping[str, Any],
    files: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """构造执行态统一结果壳（业务数据仅存 result.metadata，根部仅存框架字段）。"""
    result_copy = copy.deepcopy(dict(result))
    raw_ann = result_copy.get("annotations")
    result_copy["annotations"] = {
        **(dict(raw_ann) if isinstance(raw_ann, Mapping) else {}),
        "completion_source": "execution",
    }
    return {
        "result": result_copy,
        "annotations": {"completion_source": "execution"},
        "files": copy.deepcopy(dict(files or {})),
    }


def build_execution_success_record(
    *,
    result: Mapping[str, Any],
    files: Mapping[str, Any] | None = None,
    run_id: str | None = None,
    attempt_no: int = 1,
    started_at: str | None = None,
    settled_at: str | None = None,
    declared_inputs: Sequence[str] = (),
    declared_artifacts: Mapping[str, FileSpec | str] | None = None,
    job_snapshot: Mapping[str, Any] | None = None,
    prev_meta: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """构造执行态 ``Successes`` 档案字典。"""
    effective_settled_at = settled_at if settled_at is not None else utc_now_iso()
    record = _execution_envelope(result, files)
    record.update(
        run_id=run_id or "run",
        attempt_no=attempt_no,
        started_at=started_at,
        settled_at=effective_settled_at,
        artifacts=build_artifacts_manifest(declared_artifacts),
        last_succeeded_at=effective_settled_at,
    )
    if run_id is not None:
        record["last_success_run_id"] = run_id
    if job_snapshot is not None:
        record["job_snapshot"] = copy.deepcopy(dict(job_snapshot))
    if declared_inputs:
        input_paths = list(dict.fromkeys(
            path for path in declared_inputs if isinstance(path, str) and path
        ))
        if input_paths:
            record["inputs"] = input_paths

    prev_count = 0
    if isinstance(prev_meta, Mapping):
        try:
            prev_count = int(prev_meta.get("success_count", 0))
        except (TypeError, ValueError):
            prev_count = 0
    record["success_count"] = prev_count + 1
    return record


def build_execution_failure_record(
    metadata: Mapping[str, Any],
    *,
    job_snapshot: Mapping[str, Any] | None = None,
    settled_at: str | None = None,
    attempt_no: int | None = None,
    run_id: str | None = None,
    started_at: str | None = None,
    files: Mapping[str, Any] | None = None,
    result: Mapping[str, Any] | None = None,
    execution_id: str | None = None,
) -> dict[str, Any]:
    """构造执行态/预检态/级联态 ``Failures`` 档案字典（无执行时不借用历史 Attempt）。"""
    effective_settled_at = settled_at if settled_at is not None else utc_now_iso()
    record = copy.deepcopy(dict(metadata))
    record.update(
        settled_at=effective_settled_at,
        run_id=None,
        attempt_no=None,
        started_at=None,
        files=copy.deepcopy(dict((job_snapshot or {}).get("files") or {})),
        result=build_result_record(
            message=metadata.get("error_message") or metadata.get("message"),
            exception=metadata.get("exception"),
            metadata={},
            annotations=metadata.get("annotations") if isinstance(metadata.get("annotations"), Mapping) else {},
        ),
    )
    if job_snapshot is not None:
        record["job_snapshot"] = copy.deepcopy(dict(job_snapshot))
    if result is not None or attempt_no is not None:
        record.update(_execution_envelope(
            result if result is not None else record["result"],
            files if files is not None else record["files"],
        ))
    if attempt_no is not None:
        record.update(
            run_id=run_id or "run",
            attempt_no=attempt_no,
            started_at=started_at,
            execution_id=execution_id,
        )
    return record


def build_manual_success_record(
    job_snapshot: Mapping[str, Any],
    *,
    path_policy: PathPolicy,
    note: str | None = None,
    settled_at: str | None = None,
) -> dict[str, Any]:
    """构造人工 ``Successes`` 档案：双写保留型 output、清执行基线、零 Attempt。"""
    annotations: dict[str, Any] = {"completion_source": "manual"}
    if note is not None:
        annotations["note"] = note

    raw_files = job_snapshot.get("files") or {}
    retained_specs: dict[str, dict[str, Any]] = {}
    retained_paths: dict[str, str] = {}
    if isinstance(raw_files, Mapping):
        for name, raw_spec in raw_files.items():
            if not isinstance(raw_spec, Mapping):
                continue
            if raw_spec.get("role") == "output" and not bool(raw_spec.get("delete_on_success", False)):
                path = raw_spec.get("path")
                if isinstance(path, str) and path:
                    # 提前经 lookup 解析规范路径，使块内 tx.after 与退出后落盘快照一致；
                    # 非法或不可访问路径回退保留原值，留待提交期 strict 验盘统一汇报。
                    try:
                        effective_path = path_policy.inspect(path, mode="lookup").canonical_path
                    except (ValueError, OSError):
                        effective_path = path
                    retained_specs[name] = {
                        "path": effective_path,
                        "role": "output",
                        "delete_existing": bool(raw_spec.get("delete_existing", False)),
                        "delete_on_failure": bool(raw_spec.get("delete_on_failure", False)),
                        "delete_on_success": False,
                    }
                    retained_paths[name] = effective_path

    return {
        "job_snapshot": copy.deepcopy(dict(job_snapshot)),
        "files": normalize_outer_files(retained_specs),
        "artifacts": build_artifacts_manifest(retained_paths),
        "inputs": [],
        "result": build_result_record(annotations=annotations),
        "annotations": dict(annotations),
        "settled_at": settled_at if settled_at is not None else utc_now_iso(),
        "run_id": None,
        "attempt_no": None,
        "started_at": None,
    }


def build_manual_failure_entry(
    job_snapshot: Mapping[str, Any],
    *,
    note: str | None = None,
    settled_at: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """构造人工 ``Failures`` 档案与 payload 快照（note 仅写入 annotations，根部不重复写）。"""
    annotations: dict[str, Any] = {"completion_source": "manual"}
    if note is not None:
        annotations["note"] = note
    error_message = note if note is not None else "Manual status set to failed"
    fail_meta: dict[str, Any] = {
        "job_snapshot": copy.deepcopy(dict(job_snapshot)),
        "files": copy.deepcopy(dict(job_snapshot.get("files") or {})),
        "result": build_result_record(message=error_message, annotations=annotations),
        "annotations": dict(annotations),
        "error_message": error_message,
        "settled_at": settled_at if settled_at is not None else utc_now_iso(),
        "run_id": None,
        "attempt_no": None,
        "started_at": None,
    }
    raw_payload = job_snapshot.get("payload")
    payload_copy = copy.deepcopy(raw_payload) if isinstance(raw_payload, dict) else None
    return fail_meta, payload_copy


def extract_archive_result(
    archive_meta: Mapping[str, Any],
    *,
    is_failure: bool,
) -> dict[str, Any] | None:
    """从终态档案提取规范 4 键 result（向后兼容旧库根部 error_message / message / note / annotations）。"""
    raw_result = archive_meta.get("result")
    if isinstance(raw_result, Mapping) and raw_result:
        normalized = normalize_result_record(raw_result)
        if (
            normalized is not None
            and is_failure
            and "note" in archive_meta
            and "note" not in normalized["annotations"]
        ):
            normalized["annotations"]["note"] = archive_meta["note"]
        return normalized
    raw_ann = archive_meta.get("annotations")
    annotations: dict[str, Any] = dict(raw_ann) if isinstance(raw_ann, Mapping) else {}
    if is_failure and "note" in archive_meta and "note" not in annotations:
        annotations["note"] = archive_meta["note"]
    if is_failure:
        return normalize_result_record({
            "message": archive_meta.get("error_message") or archive_meta.get("message"),
            "exception": archive_meta.get("exception"),
            "metadata": {},
            "annotations": annotations,
        })
    return normalize_result_record({
        "message": None,
        "exception": None,
        "metadata": {},
        "annotations": annotations,
    })


__all__ = [
    "EFFECTIVE_JOB_CONFIG_KEYS",
    "build_artifacts_manifest",
    "build_execution_failure_record",
    "build_execution_success_record",
    "build_manual_failure_entry",
    "build_manual_success_record",
    "build_result_record",
    "extract_archive_result",
    "extract_effective_job_config",
    "extract_job_snapshot_from_archive",
    "extract_serializable_payload",
    "fallback_job_snapshot",
    "normalize_job_intake_snapshot",
    "normalize_outer_files",
    "normalize_result_record",
]
