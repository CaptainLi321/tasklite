"""RunSession：单次 run() 的生命周期状态与钩子单一出口。

不变式：
- stop_mode 单调转移（NONE→DRAINING→ABORTING，唯一入口 request_stop）；
- fire_run_end 幂等（一个 run 至多发一次）；
- stats 无 public setter（结算记账经 StateStore）；
- begin() 是新 run 的复位入口（run_id/dispatch_seq/stats/stop_mode/幂等标志归零）。
"""

from __future__ import annotations

import logging
import secrets
from typing import Any, Callable

from .types import ExitReason, StopMode, TaskStats

logger = logging.getLogger("tasklite")


class RunSession:
    """一次 run() 的可变生命周期状态 + fire_* 钩子单一出口（红线 6）。"""

    def __init__(
        self,
        *,
        on_run_start: Callable[[], None] | None = None,
        on_job_completed: Callable[[str, dict[str, Any], bool, bool], None] | None = None,
        on_run_end: Callable[[str], None] | None = None,
    ) -> None:
        self.on_run_start = on_run_start
        self.on_job_completed = on_job_completed
        self.on_run_end = on_run_end
        self.run_id: str | None = None
        # 每 run 随机结果认证令牌：随 WorkerLaunchSpec 下发 worker、随结果
        # 落盘，主进程读取侧强校验（防 ipc_dir 写入者伪造结果文件投毒）
        self.result_token: str | None = None
        self.dispatch_seq: int = 0
        self.stop_mode: StopMode = StopMode.NONE
        self._stats = TaskStats()
        self._run_end_fired = False

    @property
    def stats(self) -> TaskStats:
        return self._stats

    def begin(self, run_id: str | None = None) -> None:
        """新 run 的复位入口：全部生命周期状态归零，run_id 待分配时置 None。"""
        self.run_id = run_id
        self.result_token = secrets.token_hex(32)
        self.dispatch_seq = 0
        self.stop_mode = StopMode.NONE
        self._stats = TaskStats()
        self._run_end_fired = False

    def next_dispatch_seq(self) -> int:
        """fence 序号单调递增（incarnation 身份隔离的序号来源）。"""
        self.dispatch_seq += 1
        return self.dispatch_seq

    def request_stop(self, force: bool = False) -> StopMode:
        """停机状态机唯一入口（单调：NONE→DRAINING→ABORTING）。"""
        if force or self.stop_mode == StopMode.DRAINING:
            self.stop_mode = StopMode.ABORTING
            logger.info("停机状态升级为 ABORTING（强制终止在途任务）")
        elif self.stop_mode == StopMode.NONE:
            self.stop_mode = StopMode.DRAINING
            logger.info("停机状态设置为 DRAINING（等待在途任务完成）")
        return self.stop_mode

    def exit_reason(self, exc: BaseException | None = None) -> ExitReason:
        """终局原因唯一推导实现：异常类型优先（KeyboardInterrupt→INTERRUPTED、
        其余异常→ERROR——信号 handler 可能已改写 stop_mode 仍以异常为准），
        无异常按 stop_mode 三态。"""
        if isinstance(exc, KeyboardInterrupt):
            return ExitReason.INTERRUPTED
        if exc is not None:
            return ExitReason.ERROR
        if self.stop_mode is StopMode.ABORTING:
            return ExitReason.STOPPED_ABORTING
        if self.stop_mode is StopMode.DRAINING:
            return ExitReason.STOPPED_DRAINING
        return ExitReason.COMPLETED

    # ── 钩子单一出口（同步、主线程、按不可信代码对待：异常只计数不外溢）──

    def fire_run_start(self) -> None:
        if self.on_run_start is None:
            return
        try:
            self.on_run_start()
        except Exception as e:
            logger.warning(f"on_run_start hook raised: {e}")
            self._stats["hook_errors"] += 1

    def fire_job_completed(
        self, uid: str, meta: dict[str, Any], success: bool, going_to_retry: bool,
    ) -> None:
        if self.on_job_completed is None:
            return
        try:
            self.on_job_completed(uid, meta, success, going_to_retry)
        except Exception as e:
            logger.warning(f"on_job_completed hook raised for {uid}: {e}")
            self._stats["hook_errors"] += 1

    def fire_run_end(self, reason: str) -> None:
        if self._run_end_fired:
            return
        self._run_end_fired = True
        if self.on_run_end is None:
            return
        try:
            self.on_run_end(reason)
        except Exception as e:
            logger.warning(f"on_run_end hook raised: {e}")
            self._stats["hook_errors"] += 1
