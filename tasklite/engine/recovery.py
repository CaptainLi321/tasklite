"""装载持久化状态、结算遗留执行并回收异常退出的在途进程。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Set as AbstractSet
import copy
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .channel import ExecutionChannel
    from .completion import CompletionMachine
    from .in_flight import InFlightTracker
    from .store import StateStore

from ..backend.base import RunningAttemptRecovery, execution_id_matches_run
from ..exceptions import TaskLiteError
from ..models.records import build_result_record
from ..models.state import PipelineState, job_uid_from_snapshot
from ..utils.clock import utc_now_iso

UNSETTLED_RUNNING_ERROR_MESSAGE = "上次执行未完成结算，实际结果未知"
UNSETTLED_RUNNING_REASON = "unsettled_running_recovery"


def validate_running_execution_identity(
    row: Mapping[str, Any],
    *,
    attempt_sequence: int,
    settled_attempt_nos: AbstractSet[int],
) -> tuple[str, str, str, int]:
    """校验遗留 running 队列行的执行身份完整性（无 IO 副作用纯函数）。

    返回 ``(job_uid, execution_id, run_id, attempt_no)``；若身份非法、与序号进度
    不匹配或对应 attempt_no 已存在已结算轨迹，抛 TaskLiteError。
    """
    uid = row.get("job_uid")
    execution_id = row.get("execution_id")
    run_id = row.get("run_id")
    attempt_no = row.get("attempt_no")
    job_snap = row.get("job")
    if (
        not isinstance(uid, str) or not uid
        or not isinstance(execution_id, str) or not execution_id
        or not isinstance(run_id, str) or not run_id
        or not execution_id_matches_run(execution_id, run_id)
        or type(attempt_no) is not int or attempt_no < 1
        or not isinstance(job_snap, dict)
        or job_uid_from_snapshot(job_snap) != uid
        or attempt_sequence != attempt_no
        or attempt_no in settled_attempt_nos
    ):
        raise TaskLiteError(
            f"Invariant conflict: Job {uid!r} has an invalid or already settled running execution "
            f"(execution_id={execution_id!r}, run_id={run_id!r}, "
            f"attempt_no={attempt_no!r})"
        )
    return uid, execution_id, run_id, attempt_no


def build_unsettled_running_recovery(
    row: Mapping[str, Any],
    *,
    attempt_sequence: int,
    settled_attempt_nos: AbstractSet[int],
    settled_at: str | None = None,
) -> tuple[RunningAttemptRecovery, dict[str, Any]]:
    """校验遗留 running 执行身份并构造待补写失败 Attempt 与默认 failure_metadata（无 IO 副作用）。"""
    uid, execution_id, run_id, attempt_no = validate_running_execution_identity(
        row,
        attempt_sequence=attempt_sequence,
        settled_attempt_nos=settled_attempt_nos,
    )
    effective_settled_at = settled_at if settled_at is not None else utc_now_iso()
    job_snap = copy.deepcopy(row["job"])
    files = copy.deepcopy(job_snap.get("files") or {})
    result = build_result_record(
        message=UNSETTLED_RUNNING_ERROR_MESSAGE,
        annotations={"completion_source": "execution"},
    )
    failure_metadata: dict[str, Any] = {
        "error_message": UNSETTLED_RUNNING_ERROR_MESSAGE,
        "reason": UNSETTLED_RUNNING_REASON,
        "execution_id": execution_id,
        "run_id": run_id,
        "attempt_no": attempt_no,
    }
    recovery = RunningAttemptRecovery(
        job_uid=uid,
        attempt_no=attempt_no,
        execution_id=execution_id,
        run_id=run_id,
        started_at=None,
        settled_at=effective_settled_at,
        job_snapshot=job_snap,
        files=files,
        result=result,
        error_message=UNSETTLED_RUNNING_ERROR_MESSAGE,
    )
    return recovery, failure_metadata


class RecoveryOrchestrator:
    """按数据库事实恢复遗留执行，并在框架异常退出时单口径清扫内存与进程归属。"""

    def __init__(
        self,
        *,
        store: "StateStore",
        channel: "ExecutionChannel",
        in_flight: "InFlightTracker",
        completion: "CompletionMachine",
    ) -> None:
        self._store = store
        self._channel = channel
        self._in_flight = in_flight
        self._completion = completion

    def load_and_recover(self) -> PipelineState:
        """单次一致快照装载并校验全部冲突，再按原执行身份逐个结算遗留 running。"""
        snap = self._store.backend.load_consistent_snapshot()
        successes = snap["successes"]
        failed = snap["failed"]
        overlap = successes.keys() & failed.keys()
        if overlap:
            raise TaskLiteError(f"Invariant conflict: Jobs have both terminal records: {overlap}")

        queue_rows = snap["queue_jobs"]
        running = [row for row in queue_rows if row["status"] == "running"]
        prepared: list[tuple[dict[str, Any], RunningAttemptRecovery, dict[str, Any]]] = []
        if running:
            job_sequences = snap.get("job_sequences") or {}
            settled_by_uid: dict[str, set[int]] = defaultdict(set)
            for attempt in snap.get("attempts") or ():
                att_uid = attempt.get("job_uid")
                att_no = attempt.get("attempt_no")
                if isinstance(att_uid, str) and isinstance(att_no, int):
                    settled_by_uid[att_uid].add(att_no)
            for row in running:
                uid = row["job_uid"]
                recovery, failure_metadata = build_unsettled_running_recovery(
                    row,
                    attempt_sequence=job_sequences.get(uid, 0),
                    settled_attempt_nos=settled_by_uid.get(uid, frozenset()),
                )
                prepared.append((row, recovery, failure_metadata))

        state = PipelineState(
            successes,
            failed,
            snap["cursors"],
            [row["job"] for row in queue_rows if row["status"] == "pending"],
        )
        self._store.set_state(state)
        for row, recovery, failure_metadata in prepared:
            self._store.apply_failure(
                recovery.job_uid,
                failure_metadata,
                job_snapshot=row["job"],
                cascade=False,
                attempt_no=recovery.attempt_no,
                execution_id=recovery.execution_id,
                run_id=recovery.run_id,
                started_at=recovery.started_at,
            )
        return state

    def abort_in_flight(self) -> None:
        """框架异常退出单口径清扫：取消待启动 → 终止通道在途 → 统一释放资源、注销在途与清空授权等待。

        未提交的持久化 running 记录原样保留，留待下次启动按原执行身份恢复，绝不补提交结果。
        """
        self._channel.cancel_pending_launches()
        handles = self._in_flight.active_handles()
        cleanup_error: BaseException | None = None
        try:
            self._channel.cleanup_in_flight(handles)
        except BaseException as error:
            cleanup_error = cleanup_error or error
        try:
            self._completion.discard_in_flight()
        except BaseException as error:
            cleanup_error = cleanup_error or error
        if cleanup_error is not None:
            raise cleanup_error


__all__ = [
    "RecoveryOrchestrator",
    "UNSETTLED_RUNNING_ERROR_MESSAGE",
    "UNSETTLED_RUNNING_REASON",
    "build_unsettled_running_recovery",
    "validate_running_execution_identity",
]
