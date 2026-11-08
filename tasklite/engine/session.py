"""RunSession：单次 run() 的生命周期状态。

不变式：
- stop_mode 单调转移（NONE→DRAINING，唯一入口 request_stop，幂等且不升级强停）；
- begin() 是新 run 的复位入口（run_id/dispatch_no/stop_mode/重试计数归零）。
"""

from __future__ import annotations

import logging

from .types import ExitReason, StopMode

logger = logging.getLogger("tasklite")


class RunSession:
    """一次 run() 的可变生命周期状态。"""

    def __init__(self) -> None:
        self.run_id: str | None = None
        self.dispatch_no: int = 0
        self.stop_mode: StopMode = StopMode.NONE
        self.retry_counts: dict[str, int] = {}

    def begin(self, run_id: str | None = None) -> None:
        """新 run 的复位入口：全部生命周期状态归零，run_id 待分配时置 None。"""
        self.run_id = run_id
        self.dispatch_no = 0
        self.stop_mode = StopMode.NONE
        self.retry_counts = {}

    def get_retry_count(self, job_uid: str) -> int:
        """查询当前 Run 内指定 uid 已消耗的重试预算次数。"""
        return self.retry_counts.get(job_uid, 0)

    def record_retry(self, job_uid: str) -> int:
        """记录当前 Run 内指定 uid 消耗一次重试预算并返回更新后的次数。"""
        updated = self.retry_counts.get(job_uid, 0) + 1
        self.retry_counts[job_uid] = updated
        return updated

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
