"""装载持久化状态、结算遗留执行并回收异常退出的在途进程。"""

from __future__ import annotations

from typing import Any, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ..backend.base import AbstractStateBackend
    from .channel import ExecutionChannel, ExecutionResult
    from .in_flight import InFlightJob, InFlightTracker
    from .resource import ResourceManager

    class RecoveryStore(Protocol):
        """装载状态和登记恢复失败所需的仓库接口。"""

        @property
        def backend(self) -> AbstractStateBackend: ...

        @property
        def state(self) -> PipelineState: ...

        def set_state(self, state: PipelineState | None) -> None: ...

        def apply_failure(
            self, job_uid: str, failure_metadata: dict[str, Any], *,
            job_snapshot: dict[str, Any] | None = None,
            cascade: bool = True, attempt_no: int | None = None,
            execution_id: str | None = None,
            run_id: str | None = None, started_at: str | None = None,
        ) -> Any: ...

    class RecoveryCompletion(Protocol):
        """恢复编排消费的完成机器契约（CompletionMachine 自动满足）。

        异常中止只清除内存在途归属，持久化 running 留给下次恢复。
        """

        def settle_aborted(
            self,
            cancelled_entries: Sequence[InFlightJob],
            done_entries: Sequence[tuple[InFlightJob, ExecutionResult]],
        ) -> None: ...

from ..models.state import PipelineState, job_uid_from_snapshot
from ..backend.base import execution_id_matches_run


class RecoveryOrchestrator:
    """按数据库事实恢复遗留执行。"""

    def __init__(
        self,
        *,
        store: "RecoveryStore",
        channel: "ExecutionChannel",
        resources: "ResourceManager",
        in_flight: "InFlightTracker",
        completion: "RecoveryCompletion",
    ) -> None:
        self._store = store
        self._channel = channel
        self._resources = resources
        self._in_flight = in_flight
        self._completion = completion

    def load_and_recover(self) -> PipelineState:
        """先检查全部冲突，再按原执行身份逐个结算遗留 running。"""
        backend = self._store.backend
        successes = backend.load_successes()
        failed = backend.load_failed()
        overlap = successes.keys() & failed.keys()
        if overlap:
            raise RuntimeError(f"Invariant conflict: Jobs have both terminal records: {overlap}")

        queue_rows = backend.load_queue_jobs()
        running = [row for row in queue_rows if row["status"] == "running"]
        for row in running:
            uid = row["job_uid"]
            execution_id, run_id, attempt_no = (
                row["execution_id"], row["run_id"], row["attempt_no"]
            )
            if (
                not isinstance(execution_id, str) or not execution_id
                or not isinstance(run_id, str) or not run_id
                or not execution_id_matches_run(execution_id, run_id)
                or type(attempt_no) is not int or attempt_no < 1
                or job_uid_from_snapshot(row["job"]) != uid
                or backend.get_attempt_sequence(uid) != attempt_no
                or any(
                    attempt["attempt_no"] == attempt_no
                    for attempt in backend.load_settled_attempts(uid)
                )
            ):
                raise RuntimeError(
                    f"Invariant conflict: Job {uid!r} has an invalid or already settled running execution "
                    f"(execution_id={execution_id!r}, run_id={run_id!r}, "
                    f"attempt_no={attempt_no!r})"
                )

        state = PipelineState(
            successes, failed, backend.load_cursors(),
            [row["job"] for row in queue_rows if row["status"] == "pending"],
        )
        self._store.set_state(state)
        for row in running:
            self._store.apply_failure(
                row["job_uid"],
                {
                    "error_message": "上次执行未完成结算，实际结果未知",
                    "reason": "unsettled_running_recovery",
                    "execution_id": row["execution_id"],
                    "run_id": row["run_id"],
                    "attempt_no": row["attempt_no"],
                },
                job_snapshot=row["job"],
                cascade=False,
                attempt_no=row["attempt_no"],
                execution_id=row["execution_id"],
                run_id=row["run_id"],
                started_at=None,
            )
        return state

    def abort_in_flight(self) -> None:
        """框架异常退出时结束 worker 并清除内存在途归属，不补提交结果。"""
        if not self._in_flight:
            return

        # 1. 释放已占用的资源（务必在 clear 前）
        self._in_flight.release_all_resources(self._resources)

        # 2. 委托 channel 执行底层 TOCTOU 闭环中止（kill、重查、清理）
        handles = self._in_flight.active_handles()
        outcome = self._channel.abort_in_flight(handles)

        completed_map = {h.job_uid: res for h, res in outcome.completed}
        cancelled_entries, done_entries = self._in_flight.classify_aborted(completed_map)

        # 未提交记录保持 running，不把收到结果等同于已经结算。
        self._completion.settle_aborted(cancelled_entries, done_entries)


__all__ = [
    "RecoveryOrchestrator",
]
