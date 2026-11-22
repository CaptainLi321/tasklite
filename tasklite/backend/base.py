"""当前持久层内部 Interface 与共享入口门卫。

队列状态、追加式 Attempt、终态档案、派生 Job 和游标由一次结算原子提交。
successes 与 failed 按 UID 互斥；Attempt 仅作严格 append-only 执行轨迹，
编号独立于任何状态修整操作且永不可删。
"""
from __future__ import annotations

import copy
import os
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Sequence, TypeAlias, cast

from ..exceptions import TaskLiteError
from ..utils.artifact import TargetType
from ..utils.clock import utc_now_iso


QUEUE_EXECUTION_ID_KEY = "__tasklite_execution_id__"

FailureEntry: TypeAlias = (
    tuple[str, dict[str, Any]]
    | tuple[str, dict[str, Any], dict[str, Any] | None]
)


@dataclass(frozen=True)
class RunningAttemptRecovery:
    """遗留 running 执行的待补写失败 Attempt 载荷。"""

    job_uid: str
    attempt_no: int
    execution_id: str
    run_id: str
    started_at: str | None
    settled_at: str
    job_snapshot: dict[str, Any]
    files: dict[str, Any] | None
    result: dict[str, Any]
    error_message: str | None


@dataclass(frozen=True)
class GraphEditCommit:
    """图修整事务声明式后端提交载荷（O(Δ) 仅含触碰 uid）。

    - ``queue_writes``: ``uid -> job_snapshot | None``（``None`` 删除；非 ``None`` 写入为 ``pending`` 并排入队尾）
    - ``success_writes``: ``uid -> metadata | None``（``None`` 删除；非 ``None`` 写入 successes）
    - ``failed_writes``: ``uid -> (metadata, payload) | None``（``None`` 删除；非 ``None`` 写入 failed）
    - ``recovery_attempts``: 待补写的遗留 running 失败 Attempt 序列
    """

    queue_writes: Mapping[str, dict[str, Any] | None] = field(default_factory=dict)
    success_writes: Mapping[str, dict[str, Any] | None] = field(default_factory=dict)
    failed_writes: Mapping[str, tuple[dict[str, Any], dict[str, Any] | None] | None] = field(
        default_factory=dict
    )
    recovery_attempts: tuple[RunningAttemptRecovery, ...] = ()


def queue_snapshot_with_execution_id(
    job_snapshot: Mapping[str, Any], execution_id: str
) -> dict[str, Any]:
    """返回带内部执行代号的队列快照。"""
    stored = dict(job_snapshot)
    stored[QUEUE_EXECUTION_ID_KEY] = execution_id
    return stored


def queue_snapshot_execution_id(job_snapshot: Mapping[str, Any]) -> str | None:
    """读取队列快照内部执行代号；缺失或非法值返回 None。"""
    value = job_snapshot.get(QUEUE_EXECUTION_ID_KEY)
    return value if isinstance(value, str) and value else None


def public_queue_snapshot(job_snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """返回剥离内部执行代号的业务快照副本。"""
    public = copy.deepcopy(dict(job_snapshot))
    public.pop(QUEUE_EXECUTION_ID_KEY, None)
    return public


def execution_id_matches_run(execution_id: str, run_id: str) -> bool:
    """执行代号必须由本 Run 与正派发序号组成。"""
    prefix, separator, dispatch_no = execution_id.rpartition(".")
    return (
        bool(separator)
        and prefix == run_id
        and dispatch_no.isdigit()
        and int(dispatch_no) > 0
    )


def validate_queue_replacement(job_snapshots: Any) -> None:
    """队列快照序列形状校验（用于 ``commit_job_settlement`` 派生作业入队前守卫）。

    契约：入参必须是 job dict 的 list/tuple；None、非序列或非 dict 元素均属
    契约违约，双后端须在进入任何写事务前调用本校验并 fail-loud 抛 TypeError。
    """
    if not isinstance(job_snapshots, (list, tuple)):
        raise TypeError(
            f"queue replacement must be a list of job dicts, "
            f"got {type(job_snapshots).__name__}"
        )
    for i, job_snapshot in enumerate(job_snapshots):
        if not isinstance(job_snapshot, dict):
            raise TypeError(
                f"queue replacement item #{i} must be a job dict, "
                f"got {type(job_snapshot).__name__}"
            )


def normalize_failure_entries(
    entries: Sequence[FailureEntry],
) -> list[tuple[str, dict[str, Any], dict[str, Any] | None]]:
    """校验并展开批量失败条目，统一规范为 ``(job_uid, failure_metadata, payload)`` 三元组。"""
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        raise TypeError(f"failure entries must be a sequence, got {type(entries).__name__}")
    normalized: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = []
    for i, item in enumerate(entries):
        if not isinstance(item, tuple) or len(item) not in (2, 3):
            raise TypeError(
                f"failure entry #{i} must be a (job_uid, metadata[, payload]) tuple, "
                f"got {type(item).__name__}"
            )
        job_uid = item[0]
        meta = item[1]
        payload = item[2] if len(item) == 3 else None
        if not isinstance(job_uid, str) or not job_uid:
            raise TypeError(f"failure entry #{i} job_uid must be a non-empty str, got {job_uid!r}")
        if not isinstance(meta, dict):
            raise TypeError(
                f"failure entry #{i} metadata must be a dict, got {type(meta).__name__}"
            )
        if payload is not None and not isinstance(payload, dict):
            raise TypeError(
                f"failure entry #{i} payload must be a dict or None, got {type(payload).__name__}"
            )
        normalized.append((job_uid, meta, payload))
    return normalized


def validate_cursor_key(key: Any, *, error_type: type[Exception] = TypeError) -> str:
    """校验游标键必须为非空 str。"""
    if not isinstance(key, str) or not key:
        raise error_type(f"cursor key must be a non-empty str, got {key!r}")
    return key


def validate_cursor_value(value: Any) -> str | None:
    """校验游标值必须为 str 或 None。"""
    if value is not None and not isinstance(value, str):
        raise TypeError(f"cursor value must be str or None, got {type(value).__name__}")
    return value


def validate_mark_job_running_args(
    *, execution_id: Any, run_id: Any, started_at: Any
) -> None:
    """校验 ``mark_job_running`` 前置入参（SQLite / Memory 双后端同源调用）。"""
    if not isinstance(execution_id, str) or not execution_id:
        raise ValueError("execution_id must be a non-empty str")
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("run_id must be a non-empty str")
    if not execution_id_matches_run(execution_id, run_id):
        raise ValueError(f"execution_id {execution_id!r} does not belong to run {run_id!r}")
    if not isinstance(started_at, str) or not started_at:
        raise ValueError("started_at must be a non-empty str")


def validate_mark_job_running_state(
    job_uid: str,
    *,
    present_in_queue: bool,
    queue_status: str | None,
) -> None:
    """校验 ``mark_job_running`` 队列行当前状态（必须存在且未处于 running）。"""
    if not present_in_queue:
        raise ValueError(f"Job {job_uid} not found in queue")
    if queue_status == "running":
        raise ValueError(f"Job {job_uid} is already running")


def validate_settlement_args(
    *,
    outcome: Any,
    attempt_no: Any,
    execution_id: Any,
    run_id: Any,
    settled_at: Any,
    started_at: Any,
    cursor_updates: Mapping[str, str | None] | None,
    spawned_job_snapshots: Sequence[dict[str, Any]],
) -> None:
    """校验 ``commit_job_settlement`` 前置入参（SQLite / Memory 双后端同源调用）。"""
    if outcome not in ("success", "failure", "retry", "pending"):
        raise ValueError(f"Invalid outcome: {outcome}")
    if not isinstance(attempt_no, int) or isinstance(attempt_no, bool) or attempt_no < 1:
        raise TypeError(f"attempt_no must be int >= 1, got {attempt_no!r}")
    if not isinstance(execution_id, str) or not execution_id:
        raise ValueError(f"execution_id must be non-empty str, got {execution_id!r}")
    if not isinstance(run_id, str) or not run_id:
        raise ValueError(f"run_id must be non-empty str, got {run_id!r}")
    if not execution_id_matches_run(execution_id, run_id):
        raise ValueError(f"execution_id {execution_id!r} does not belong to run {run_id!r}")
    if not isinstance(settled_at, str) or not settled_at:
        raise ValueError(f"settled_at must be non-empty str, got {settled_at!r}")
    if started_at is not None and (not isinstance(started_at, str) or not started_at):
        raise ValueError("started_at must be non-empty str or None")
    if cursor_updates:
        for k, v in cursor_updates.items():
            validate_cursor_key(k, error_type=ValueError)
            validate_cursor_value(v)
    if spawned_job_snapshots:
        validate_queue_replacement(list(spawned_job_snapshots))


def validate_running_settlement_state(
    job_uid: str,
    *,
    present_in_queue: bool,
    queue_status: str | None,
    queue_run_id: str | None,
    queue_attempt_no: int | None,
    queue_execution_id: str | None,
    run_id: str,
    attempt_no: int,
    execution_id: str,
    already_settled: bool,
) -> None:
    """校验待结算执行与队列 running 身份严格匹配且对应 attempt 尚未结算。"""
    if (
        not present_in_queue
        or (queue_status, queue_run_id, queue_attempt_no, queue_execution_id)
        != ("running", run_id, attempt_no, execution_id)
    ):
        raise TaskLiteError(f"Settlement does not match the running execution for {job_uid}")
    if already_settled:
        raise TaskLiteError(f"Execution for {job_uid} attempt {attempt_no} is already settled")


def normalize_authorization_path(path: str | os.PathLike[str]) -> str:
    """规范化后端路径授权存储主键。"""
    return os.path.normpath(os.fspath(path))


def is_valid_authorization_target_type(target_type: Any) -> bool:
    """判断路径授权目标类型是否为合法的 'file' 或 'directory'。"""
    return target_type in ("file", "directory")


def validate_authorization_target_type(
    target_type: Any, *, error_type: type[Exception] = ValueError
) -> TargetType:
    """校验路径授权目标类型并返回类型收窄结果。"""
    if not is_valid_authorization_target_type(target_type):
        raise error_type(f"target_type must be 'file' or 'directory', got {target_type!r}")
    return cast(TargetType, target_type)


def resolve_authorized_at(authorized_at: str | None) -> str:
    """解析路径授权时间戳，缺省回填当前 UTC ISO 串。"""
    return authorized_at if authorized_at is not None else utc_now_iso()


def validate_graph_edit_commit(commit: Any) -> bool:
    """校验 GraphEditCommit 前置结构契约；若为空提交返回 False，否则返回 True。

    SQLite 与 Memory 双后端同源调用，确保类型错误、重复恢复载荷或缺失队列写变
    在进入任何写事务前 fail-loud 拒绝。
    """
    if not isinstance(commit, GraphEditCommit):
        raise TypeError(f"commit must be a GraphEditCommit, got {type(commit).__name__}")
    if (
        not commit.queue_writes
        and not commit.success_writes
        and not commit.failed_writes
        and not commit.recovery_attempts
    ):
        return False

    seen_recovery: set[str] = set()
    for rec in commit.recovery_attempts:
        if not isinstance(rec, RunningAttemptRecovery):
            raise TypeError(
                f"recovery_attempts items must be RunningAttemptRecovery, got {type(rec).__name__}"
            )
        if rec.job_uid in seen_recovery:
            raise TaskLiteError(
                f"Duplicate recovery attempt for running job {rec.job_uid!r}"
            )
        seen_recovery.add(rec.job_uid)
        if rec.job_uid not in commit.queue_writes:
            raise TaskLiteError(
                f"Recovery attempt for {rec.job_uid!r} must also update or remove its queue row"
            )
    return True


def validate_recovery_attempt_state(
    rec: RunningAttemptRecovery,
    *,
    present_in_queue: bool,
    queue_status: str | None,
    queue_run_id: str | None,
    queue_attempt_no: int | None,
    queue_execution_id: str | None,
    attempt_sequence: int | None,
    already_settled: bool,
) -> None:
    """校验遗留 running 恢复载荷与后端当前执行身份、序号进度及结算态的一致性。"""
    if not present_in_queue:
        raise TaskLiteError(
            f"Recovery target {rec.job_uid!r} is no longer present in queue"
        )
    if (
        queue_status,
        queue_run_id,
        queue_attempt_no,
        queue_execution_id,
    ) != ("running", rec.run_id, rec.attempt_no, rec.execution_id):
        raise TaskLiteError(
            f"Recovery attempt does not match the running execution for {rec.job_uid!r}"
        )
    if attempt_sequence is None or attempt_sequence != rec.attempt_no:
        raise TaskLiteError(
            f"Recovery attempt sequence mismatch for {rec.job_uid!r}"
        )
    if already_settled:
        raise TaskLiteError(
            f"Execution for {rec.job_uid!r} attempt {rec.attempt_no} is already settled"
        )


class AbstractStateBackend(ABC):
    """SQLite 与 Memory 共用的内部持久化 Interface。

    当前执行只经 ``commit_job_settlement`` 追加结算；执行前/批量失败与
    候选移除各自原子提交；独占执行权由后端经 ``exclusive_access`` 自治。
    写入故障原样传播，调用方提交成功后才发布内存；历史清理不触碰编号进度。
    """

    # 独占执行权 -----------------------------------------------------

    @abstractmethod
    def exclusive_access(self) -> AbstractContextManager[None]:
        """非阻塞取得本后端独占执行权，离开作用域时归还本次取得的锁。"""

    # 全量加载 -------------------------------------------------------

    @abstractmethod
    def load_successes(self) -> dict[str, dict[str, Any]]:
        """加载全部成功终态（uid → meta）。"""

    @abstractmethod
    def load_failed(self) -> dict[str, dict[str, Any]]:
        """加载全部失败档案终态（uid → 失败 meta）。"""

    @abstractmethod
    def load_cursors(self) -> dict[str, str]:
        """加载全部游标（key → value）。"""

    # delta 提交 -----------------------------------------------------

    @abstractmethod
    def commit_job_failures(self, entries: Sequence[FailureEntry]) -> None:
        """原子批量写失败档案（单条执行前拒绝 / 死锁 / 级联共用单一原语）。

        每项为 ``(job_uid, failure_metadata)`` 或 ``(job_uid, failure_metadata, payload)``：
        单事务效果为 failed INSERT/REPLACE（``payload=None`` 保留既有快照不覆盖）、
        queue 按 uid 精准 DELETE（剩余条目保持原顺序）、successes 批量 DELETE 同名行
        （维护 ``successes ∩ failed == ∅``）。空序列幂等无操作；任一行失败则整体回滚并上抛。
        """

    @abstractmethod
    def commit_skip(self, job_uid: str) -> None:
        """移除 Trigger False 或已终态重复候选的队列行。

        不写 successes/failed，不生成 Attempt。
        成功无返回值；失败时队列不变，原始异常继续传播。
        """

    # 增量入队 -------------------------------------------------------

    @abstractmethod
    def enqueue_jobs(self, job_snapshots: list[dict[str, Any]], *, front: bool = False) -> list[str]:
        """批量增量入队：按 front 在队列头/尾插入 job_snapshots（保序），单事务原子。

        跳过与现有队列重复的 uid（含批次内重复，首个存储），返回实际插入的 uid 列表；
        不做全表覆盖，失败时事务回滚并原样抛异常。
        """

    # 管理写、一致快照与游标 -----------------------------------------

    @abstractmethod
    def load_consistent_snapshot(self) -> dict[str, Any]:
        """在单一读事务或单锁内原子捕获一致状态快照（含 queue_jobs/successes/failed/failure_payloads/cursors/attempts/job_sequences）。"""

    @abstractmethod
    def commit_graph_edit(self, commit: GraphEditCommit) -> None:
        """在单一写事务或锁内原子应用声明式图修整变更，绝不删除 attempts 或重置 job_sequences。"""

    @abstractmethod
    def get_cursor(self, key: str) -> str | None:
        """查询单个游标的最新已提交值，不存在返回 None。"""

    @abstractmethod
    def set_cursor(self, key: str, value: str | None) -> None:
        """设置或删除单个游标（None 为删除）。"""

    # 尝试轨迹（append-only 旁路观测面）--------------------------------

    @abstractmethod
    def load_latest_succeeded_finished_at(self, job_uid: str) -> str | None:
        """读取指定 Job 最新一条 success Attempt 的 settled_at；无则返回 None。"""

    # 元数据 ---------------------------------------------------------

    @abstractmethod
    def set_engine_metadata(self, key: str, value: str) -> None:
        """写入一条框架级元数据（UPSERT 语义）。失败抛异常（不吞）。"""

    # 状态事务与编号 -------------------------------------------------

    @abstractmethod
    def mark_job_running(
        self, job_uid: str, *, execution_id: str, run_id: str, started_at: str
    ) -> int:
        """原子将 pending job 转为 running，持久化完整执行身份并递增 attempt_no。"""

    @abstractmethod
    def commit_job_settlement(
        self,
        *,
        job_uid: str,
        outcome: str,
        attempt_no: int,
        execution_id: str,
        run_id: str,
        settled_at: str,
        started_at: str | None = None,
        job_snapshot: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        error_message: str | None = None,
        success_metadata: dict[str, Any] | None = None,
        spawned_job_snapshots: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
        failure_metadata: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        requeued_job_snapshot: dict[str, Any] | None = None,
        record_attempt: bool = True,
    ) -> None:
        """原子执行结算：校验完整执行身份后写入 Attempt、Job 去向、档案与派生数据。

        成功无返回值；执行身份与 running 不匹配或已结算时抛 TaskLiteError。
        写入故障原样传播，失败时后端不变。
        """

    @abstractmethod
    def get_attempt_sequence(self, job_uid: str) -> int:
        """读取指定 job_uid 的最新 attempt_no 编号进度（未派发过返回 0）。"""

    @abstractmethod
    def load_settled_attempts(self, job_uid: str) -> list[dict[str, Any]]:
        """按 attempt_no 升序读取指定 job_uid 的全部已结算 Attempt 记录。"""

    @abstractmethod
    def load_queue_jobs(self, *, status: str | None = None) -> list[dict[str, Any]]:
        """按 queue_position 升序加载队列行的完整信息，可选按 status 过滤。"""

    @abstractmethod
    def grant_authorization(
        self,
        path: str,
        target_type: Literal["file", "directory"],
        *,
        authorized_at: str | None = None,
    ) -> dict[str, Any]:
        """原子校验冲突并新增持久化路径授权（重复或父子类型冲突抛 TaskLiteError）。"""

    @abstractmethod
    def record_authorization(
        self,
        path: str,
        target_type: str,
        *,
        authorized_at: str | None = None,
    ) -> None:
        """持久化记录一条文件系统路径授权（target_type 为 'file' 或 'directory'）。"""

    @abstractmethod
    def list_authorizations(self) -> list[dict[str, Any]]:
        """查询全部已持久化的授权记录。"""

    @abstractmethod
    def revoke_authorization(self, path: str) -> bool:
        """按规范绝对路径精确撤销授权，存在并删除返回 True，不存在返回 False。"""

    @abstractmethod
    def is_path_authorized(self, path: str, target_type: str) -> bool:
        """按实际目标类型检查规范绝对路径授权；文件精确匹配，目录覆盖整树。"""

    def close(self) -> None:
        """关闭后端并释放资源（幂等）。"""


__all__ = [
    "AbstractStateBackend",
    "FailureEntry",
    "GraphEditCommit",
    "QUEUE_EXECUTION_ID_KEY",
    "RunningAttemptRecovery",
    "execution_id_matches_run",
    "is_valid_authorization_target_type",
    "normalize_authorization_path",
    "normalize_failure_entries",
    "public_queue_snapshot",
    "queue_snapshot_execution_id",
    "queue_snapshot_with_execution_id",
    "resolve_authorized_at",
    "validate_authorization_target_type",
    "validate_cursor_key",
    "validate_cursor_value",
    "validate_graph_edit_commit",
    "validate_mark_job_running_args",
    "validate_mark_job_running_state",
    "validate_queue_replacement",
    "validate_recovery_attempt_state",
    "validate_running_settlement_state",
    "validate_settlement_args",
]
