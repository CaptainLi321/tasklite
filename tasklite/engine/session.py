"""RunSession：单次 run() 的生命周期状态。

不变式：
- stop_mode 单调转移（NONE→DRAINING，唯一入口 request_stop，幂等且不升级强停）；
- begin() 是新 run 的复位入口（run_id/dispatch_no/stop_mode/候选来源与重试计数归零）。
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from .types import ExitReason, StopMode

logger = logging.getLogger("tasklite")


class RunSession:
    """一次 run() 的可变生命周期状态。"""

    def __init__(self) -> None:
        self.run_id: str | None = None
        self.dispatch_no: int = 0
        self.stop_mode: StopMode = StopMode.NONE
        self.retry_counts: dict[str, int] = {}
        self.retry_candidates: set[str] = set()
        self._exclude_chain_handler: Callable[[str], None] | None = None

    def bind_exclude_chain_handler(self, handler: Callable[[str], None] | None) -> None:
        """绑定临时级联排除处理器。"""
        self._exclude_chain_handler = handler

    def temporarily_exclude_chain(self, job_uid: str) -> None:
        """委托绑定的处理器将指定作业及其全部下游加入本 Run 临时排除。"""
        if self._exclude_chain_handler is not None and callable(self._exclude_chain_handler):
            self._exclude_chain_handler(job_uid)

    def begin(self, run_id: str | None = None) -> None:
        """新 run 的复位入口：全部生命周期状态归零，run_id 待分配时置 None。"""
        self.run_id = run_id
        self.dispatch_no = 0
        self.stop_mode = StopMode.NONE
        self.retry_counts = {}
        self.retry_candidates = set()

    def get_retry_count(self, job_uid: str) -> int:
        """查询当前 Run 内指定 uid 已消耗的重试预算次数。"""
        return self.retry_counts.get(job_uid, 0)

    def record_retry(self, job_uid: str) -> int:
        """记录当前 Run 内指定 uid 消耗一次重试预算并返回更新后的次数。"""
        updated = self.retry_counts.get(job_uid, 0) + 1
        self.retry_counts[job_uid] = updated
        return updated

    def mark_retry_candidate(self, job_uid: str) -> None:
        """登记本 Run 内由显式 retry 产生的下一次候选来源。"""
        self.retry_candidates.add(job_uid)

    def is_retry_candidate(self, job_uid: str) -> bool:
        """该 uid 的下一次候选是否由本 Run 的显式 retry 产生。"""
        return job_uid in self.retry_candidates

    def clear_retry_candidate(self, job_uid: str) -> None:
        """候选结算后清除来源；本 Run 已累计的重试预算保持不变。"""
        self.retry_candidates.discard(job_uid)

    def next_dispatch_no(self) -> int:
        """fence 序号单调递增（execution_id 身份隔离的序号来源）。"""
        self.dispatch_no += 1
        return self.dispatch_no

    def request_stop(self) -> StopMode:
        """停机状态机唯一入口（幂等设为 DRAINING，不升级强停）。"""
        if self.stop_mode == StopMode.NONE:
            self.stop_mode = StopMode.DRAINING
            logger.info("停机状态设置为 DRAINING（等待在途任务完成）")
        else:
            logger.info("已处于停机排空状态，正在等待在途任务完成")
        return self.stop_mode

    def exit_reason(self, exc: BaseException | None = None) -> ExitReason:
        """终局原因唯一推导实现：异常类型优先（KeyboardInterrupt→INTERRUPTED、
        其余异常→ERROR），无异常按 stop_mode。"""
        if isinstance(exc, KeyboardInterrupt):
            return ExitReason.INTERRUPTED
        if exc is not None:
            return ExitReason.ERROR
        if self.stop_mode is StopMode.DRAINING:
            return ExitReason.STOPPED_DRAINING
        return ExitReason.COMPLETED


__all__ = [
    "RunSession",
]
