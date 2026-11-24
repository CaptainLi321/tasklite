"""StateSnapshot 可读报告生成（单快照与双快照增量对比，内部纯函数模块）。

不对外暴露为公开 API，仅由 ``StateSnapshot.report`` 调用；输出文本供人类
阅读展示，不作为跨版本编程解析契约。
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import statistics
from typing import TYPE_CHECKING, Any, Mapping

from .utils.clock import parse_utc_iso_to_us, utc_now_iso
from .utils.encoding import parse_job_uid

if TYPE_CHECKING:
    from .snapshot import StateSnapshot

_STATUS_ORDER = ("pending", "running", "succeeded", "failed")
_OUTCOME_ORDER = ("success", "failure", "retry")
_DEFAULT_ERROR_MAX_LEN = 120
_DEFAULT_PENDING_LIST_LIMIT = 50


@dataclass(frozen=True)
class _DurationStats:
    """历史成功 Attempt 耗时统计（单位：秒）。"""

    count: int
    total_s: float
    avg_s: float
    p50_s: float
    max_s: float


@dataclass(frozen=True)
class _PendingJobInfo:
    """当前排队中 pending 作业摘要（保持队列产出顺序）。"""

    job_uid: str
    task_type: str


@dataclass(frozen=True)
class _RunningJobInfo:
    """当前在途 running 作业摘要。"""

    job_uid: str
    task_type: str
    started_at: str | None
    attempt_no: int | None
    run_id: str | None


@dataclass(frozen=True)
class _FailedJobInfo:
    """当前失败作业摘要。"""

    job_uid: str
    task_type: str
    settled_at: str | None
    error_summary: str


@dataclass(frozen=True)
class _AttemptInfo:
    """单条 Attempt 轻量投影（用于区间增量对比与耗时聚合）。"""

    key: tuple[str, int | None, str | None]
    job_uid: str
    task_type: str
    outcome: str
    duration_s: float | None


@dataclass
class _TaskTypeCounts:
    """单个 task_type 下的四态计数。"""

    pending: int = 0
    running: int = 0
    succeeded: int = 0
    failed: int = 0

    @property
    def total(self) -> int:
        return self.pending + self.running + self.succeeded + self.failed


@dataclass
class _SnapshotSummary:
    """单份 StateSnapshot 的完整统计聚合结果。"""

    total_jobs: int = 0
    status_counts: dict[str, int] = field(
        default_factory=lambda: {s: 0 for s in _STATUS_ORDER}
    )
    succeeded_execution: int = 0
    succeeded_manual: int = 0
    by_task_type: dict[str, _TaskTypeCounts] = field(default_factory=dict)
    durations_by_task_type: dict[str, list[float]] = field(default_factory=dict)
    duration_stats_by_task_type: dict[str, _DurationStats] = field(default_factory=dict)
    pending_jobs: list[_PendingJobInfo] = field(default_factory=list)
    running_jobs: list[_RunningJobInfo] = field(default_factory=list)
    failed_jobs: list[_FailedJobInfo] = field(default_factory=list)
    job_statuses: dict[str, str] = field(default_factory=dict)
    failed_by_uid: dict[str, _FailedJobInfo] = field(default_factory=dict)
    attempts: list[_AttemptInfo] = field(default_factory=list)
    attempt_keys: set[tuple[str, int | None, str | None]] = field(default_factory=set)


@dataclass
class _SnapshotDelta:
    """双快照 first -> second 区间增量变化。"""

    total_delta: int
    status_deltas: dict[str, int]
    succeeded_execution_delta: int
    succeeded_manual_delta: int
    transitions: list[tuple[str, int]]
    new_attempts_total: int
    new_attempts_by_outcome: dict[str, int]
    new_success_duration_stats: _DurationStats | None
    newly_failed: list[tuple[_FailedJobInfo, str]]


def _extract_task_type(uid: str, job_cfg: Any) -> str:
    """优先取配置快照中的 task_type，缺失时从 job_uid 反解。"""
    if isinstance(job_cfg, Mapping):
        tt = job_cfg.get("task_type")
        if isinstance(tt, str) and tt:
            return tt
    try:
        tt, _ = parse_job_uid(uid)
        return tt
    except Exception:
        return "<unknown>"


def _classify_succeeded_source(job: Mapping[str, Any]) -> str:
    """区分成功记录来源：manual 或 execution（兼容旧记录空 annotations）。"""
    result = job.get("result")
    if isinstance(result, Mapping):
        annotations = result.get("annotations")
        if isinstance(annotations, Mapping):
            if annotations.get("completion_source") == "manual":
                return "manual"
    return "execution"


def _compute_duration_seconds(started_at: Any, settled_at: Any) -> float | None:
    """按微秒差计算耗时秒数；时间戳缺失/非法返回 None，时钟回拨归零。"""
    if not isinstance(started_at, str) or not started_at:
        return None
    if not isinstance(settled_at, str) or not settled_at:
        return None
    try:
        start_us = parse_utc_iso_to_us(started_at)
        end_us = parse_utc_iso_to_us(settled_at)
    except (TypeError, ValueError):
        return None
    return max(0.0, (end_us - start_us) / 1_000_000.0)


def _compute_duration_stats(samples: list[float]) -> _DurationStats | None:
    if not samples:
        return None
    ordered = sorted(samples)
    total_s = sum(ordered)
    return _DurationStats(
        count=len(ordered),
        total_s=total_s,
        avg_s=total_s / len(ordered),
        p50_s=float(statistics.median(ordered)),
        max_s=ordered[-1],
    )


def _extract_error_summary(
    job: Mapping[str, Any], max_len: int = _DEFAULT_ERROR_MAX_LEN
) -> str:
    """提取失败作业的单行可读错误摘要并按长度截断。"""
    result = job.get("result")
    if not isinstance(result, Mapping):
        return "(no error details)"
    msg = result.get("message")
    msg_str = str(msg).strip() if msg is not None else ""
    exc = result.get("exception")
    exc_str = ""
    if isinstance(exc, Mapping):
        exc_type = str(exc.get("type") or "").strip()
        exc_msg = str(exc.get("message") or "").strip()
        if exc_type and exc_msg:
            exc_str = f"{exc_type}: {exc_msg}"
        elif exc_type:
            exc_str = exc_type
        elif exc_msg:
            exc_str = exc_msg
    if msg_str and exc_str:
        if exc_str in msg_str:
            raw = msg_str
        elif msg_str in exc_str:
            raw = exc_str
        else:
            raw = f"{msg_str} ({exc_str})"
    else:
        raw = msg_str or exc_str or "(no error details)"
    single_line = " ".join(raw.splitlines()).strip()
    if not single_line:
        return "(no error details)"
    if len(single_line) > max_len:
        return single_line[: max_len - 3] + "..."
    return single_line


def _format_duration(seconds: float) -> str:
    """将秒数格式化为紧凑可读时长。"""
    clamped = max(0.0, seconds)
    if clamped < 10.0:
        return f"{clamped:.2f}s"
    if clamped < 60.0:
        return f"{clamped:.1f}s"
    if clamped < 3600.0:
        mins, rem = divmod(clamped, 60.0)
        return f"{int(mins)}m {rem:04.1f}s"
    hours, rem = divmod(clamped, 3600.0)
    mins, secs = divmod(rem, 60.0)
    return f"{int(hours)}h {int(mins):02d}m {int(secs):02d}s"


def _format_signed(delta: int) -> str:
    if delta > 0:
        return f"+{delta}"
    if delta < 0:
        return str(delta)
    return "+0"


def _summarize_snapshot(snapshot: StateSnapshot) -> _SnapshotSummary:
    """单次扫描快照内的 Job 与 Attempt，构造聚合统计。"""
    summary = _SnapshotSummary()

    for job in snapshot.iter_jobs():
        uid = str(job.get("job_uid") or "")
        status = str(job.get("status") or "")
        task_type = _extract_task_type(uid, job.get("job"))

        summary.total_jobs += 1
        summary.job_statuses[uid] = status
        if status in summary.status_counts:
            summary.status_counts[status] += 1

        tt_counts = summary.by_task_type.setdefault(task_type, _TaskTypeCounts())
        if status == "pending":
            tt_counts.pending += 1
            summary.pending_jobs.append(
                _PendingJobInfo(job_uid=uid, task_type=task_type)
            )
        elif status == "running":
            tt_counts.running += 1
            summary.running_jobs.append(
                _RunningJobInfo(
                    job_uid=uid,
                    task_type=task_type,
                    started_at=job.get("started_at"),
                    attempt_no=job.get("attempt_no"),
                    run_id=job.get("run_id"),
                )
            )
        elif status == "succeeded":
            tt_counts.succeeded += 1
            if _classify_succeeded_source(job) == "manual":
                summary.succeeded_manual += 1
            else:
                summary.succeeded_execution += 1
        elif status == "failed":
            tt_counts.failed += 1
            info = _FailedJobInfo(
                job_uid=uid,
                task_type=task_type,
                settled_at=job.get("settled_at"),
                error_summary=_extract_error_summary(job),
            )
            summary.failed_jobs.append(info)
            summary.failed_by_uid[uid] = info

    summary.running_jobs.sort(
        key=lambda item: (item.started_at is None, item.started_at or "", item.job_uid)
    )
    summary.failed_jobs.sort(key=lambda item: item.job_uid)

    for att in snapshot.iter_attempts():
        uid = str(att.get("job_uid") or "")
        outcome = str(att.get("outcome") or "")
        task_type = _extract_task_type(uid, att.get("job"))
        duration_s = (
            _compute_duration_seconds(att.get("started_at"), att.get("settled_at"))
            if outcome == "success"
            else None
        )
        if duration_s is not None:
            summary.durations_by_task_type.setdefault(task_type, []).append(duration_s)
        key = (uid, att.get("attempt_no"), att.get("run_id"))
        att_info = _AttemptInfo(
            key=key,
            job_uid=uid,
            task_type=task_type,
            outcome=outcome,
            duration_s=duration_s,
        )
        summary.attempts.append(att_info)
        summary.attempt_keys.add(key)

    for task_type, samples in summary.durations_by_task_type.items():
        stats = _compute_duration_stats(samples)
        if stats is not None:
            summary.duration_stats_by_task_type[task_type] = stats

    return summary


def _compute_delta(
    before: _SnapshotSummary, after: _SnapshotSummary
) -> _SnapshotDelta:
    """计算 first -> second 区间增量。"""
    status_deltas = {
        st: after.status_counts[st] - before.status_counts[st]
        for st in _STATUS_ORDER
    }

    transition_counter: Counter[str] = Counter()
    all_uids = set(before.job_statuses) | set(after.job_statuses)
    newly_failed: list[tuple[_FailedJobInfo, str]] = []

    for uid in sorted(all_uids):
        b_status = before.job_statuses.get(uid)
        a_status = after.job_statuses.get(uid)
        if b_status == a_status:
            continue
        src = b_status if b_status is not None else "(new)"
        dst = a_status if a_status is not None else "(removed)"
        transition_counter[f"{src} -> {dst}"] += 1
        if a_status == "failed" and b_status != "failed":
            failed_info = after.failed_by_uid.get(uid)
            if failed_info is not None:
                newly_failed.append((failed_info, f"{src} -> failed"))

    transitions = sorted(
        transition_counter.items(), key=lambda pair: (-pair[1], pair[0])
    )

    new_attempts_by_outcome: dict[str, int] = {o: 0 for o in _OUTCOME_ORDER}
    new_success_durations: list[float] = []
    new_attempts_total = 0

    for att in after.attempts:
        if att.key in before.attempt_keys:
            continue
        new_attempts_total += 1
        if att.outcome in new_attempts_by_outcome:
            new_attempts_by_outcome[att.outcome] += 1
        else:
            new_attempts_by_outcome[att.outcome] = (
                new_attempts_by_outcome.get(att.outcome, 0) + 1
            )
        if att.outcome == "success" and att.duration_s is not None:
            new_success_durations.append(att.duration_s)

    return _SnapshotDelta(
        total_delta=after.total_jobs - before.total_jobs,
        status_deltas=status_deltas,
        succeeded_execution_delta=after.succeeded_execution - before.succeeded_execution,
        succeeded_manual_delta=after.succeeded_manual - before.succeeded_manual,
        transitions=transitions,
        new_attempts_total=new_attempts_total,
        new_attempts_by_outcome=new_attempts_by_outcome,
        new_success_duration_stats=_compute_duration_stats(new_success_durations),
        newly_failed=newly_failed,
    )


def _compute_elapsed_seconds(started_at: str | None, now_us: int) -> float | None:
    """计算在途 running 作业距 now_us 的已运行秒数；缺失或非法时间戳返回 None。"""
    if not started_at:
        return None
    try:
        started_us = parse_utc_iso_to_us(started_at)
    except (TypeError, ValueError):
        return None
    return max(0.0, (now_us - started_us) / 1_000_000.0)


def _format_running_line(
    item: _RunningJobInfo,
    now_us: int,
    baseline: _DurationStats | None,
) -> str:
    """格式化单条在途 running 作业的已运行时间与 ETA。"""
    attempt_part = f", attempt={item.attempt_no}" if item.attempt_no is not None else ""
    elapsed_s = _compute_elapsed_seconds(item.started_at, now_us)

    if elapsed_s is None:
        return (
            f"  - {item.job_uid} (task_type={item.task_type}{attempt_part}): "
            "elapsed=N/A, ETA=N/A (missing started_at)"
        )

    elapsed_text = _format_duration(elapsed_s)
    if baseline is None:
        return (
            f"  - {item.job_uid} (task_type={item.task_type}{attempt_part}): "
            f"elapsed={elapsed_text}, ETA=N/A (no history)"
        )

    p50_text = _format_duration(baseline.p50_s)
    if elapsed_s <= baseline.p50_s:
        remaining_text = _format_duration(baseline.p50_s - elapsed_s)
        eta_text = f"~{remaining_text} (P50={p50_text})"
    else:
        overrun_text = _format_duration(elapsed_s - baseline.p50_s)
        max_text = _format_duration(baseline.max_s)
        eta_text = f"~0.00s (+{overrun_text} over P50={p50_text}, Max={max_text})"

    return (
        f"  - {item.job_uid} (task_type={item.task_type}{attempt_part}): "
        f"elapsed={elapsed_text}, ETA={eta_text}"
    )


def _format_remaining_eta(summary: _SnapshotSummary, now_us: int) -> str:
    """基于各 task_type 历史成功 P50 估算串行总剩余耗时。

    - running 作业：有基线时若 started_at 可解析则贡献 max(0.0, p50 - elapsed)，
      缺失或非法 started_at 时保守按完整 p50 计入；无基线计入未估算项。
    - pending 作业：有基线时贡献 p50；无基线计入未估算项。
    """
    active_count = len(summary.running_jobs) + len(summary.pending_jobs)
    if active_count == 0:
        return "0.00s"

    total_remaining_s = 0.0
    estimated_count = 0
    unestimated_count = 0

    for running_item in summary.running_jobs:
        baseline = summary.duration_stats_by_task_type.get(running_item.task_type)
        if baseline is None:
            unestimated_count += 1
            continue
        elapsed_s = _compute_elapsed_seconds(running_item.started_at, now_us)
        if elapsed_s is None:
            total_remaining_s += baseline.p50_s
        else:
            total_remaining_s += max(0.0, baseline.p50_s - elapsed_s)
        estimated_count += 1

    for pending_item in summary.pending_jobs:
        baseline = summary.duration_stats_by_task_type.get(pending_item.task_type)
        if baseline is None:
            unestimated_count += 1
            continue
        total_remaining_s += baseline.p50_s
        estimated_count += 1

    if estimated_count == 0:
        return f"N/A (no history for {unestimated_count} remaining jobs)"

    eta_text = _format_duration(total_remaining_s)
    if unestimated_count == 0:
        return f"~{eta_text} (serial, P50-based)"
    return (
        f"~{eta_text} (serial, P50-based; "
        f"excludes {unestimated_count} without history)"
    )


def render_snapshot_report(
    first: StateSnapshot,
    second: StateSnapshot | None = None,
) -> str:
    """渲染单快照或双快照对比报告（共享同一主干结构）。"""
    now_us = parse_utc_iso_to_us(utc_now_iso())

    if second is None:
        main = _summarize_snapshot(first)
        delta: _SnapshotDelta | None = None
    else:
        before = _summarize_snapshot(first)
        main = _summarize_snapshot(second)
        delta = _compute_delta(before, main)

    lines: list[str] = []
    if delta is None:
        lines.append("TaskLite State Report")
        lines.append("=====================")
    else:
        lines.append("TaskLite State Report (Comparison: before -> after)")
        lines.append("===================================================")

    # 1. 四态计数汇总（含 succeeded 的 execution / manual 构成）
    if delta is None:
        lines.append(f"Summary (total: {main.total_jobs}):")
        lines.append(f"  pending:   {main.status_counts['pending']}")
        lines.append(f"  running:   {main.status_counts['running']}")
        lines.append(
            f"  succeeded: {main.status_counts['succeeded']} "
            f"(execution: {main.succeeded_execution}, manual: {main.succeeded_manual})"
        )
        lines.append(f"  failed:    {main.status_counts['failed']}")
    else:
        lines.append(
            f"Summary (total: {main.total_jobs}, {_format_signed(delta.total_delta)}):"
        )
        lines.append(
            f"  pending:   {main.status_counts['pending']} "
            f"({_format_signed(delta.status_deltas['pending'])})"
        )
        lines.append(
            f"  running:   {main.status_counts['running']} "
            f"({_format_signed(delta.status_deltas['running'])})"
        )
        lines.append(
            f"  succeeded: {main.status_counts['succeeded']} "
            f"({_format_signed(delta.status_deltas['succeeded'])}) "
            f"(execution: {main.succeeded_execution} "
            f"({_format_signed(delta.succeeded_execution_delta)}), "
            f"manual: {main.succeeded_manual} "
            f"({_format_signed(delta.succeeded_manual_delta)}))"
        )
        lines.append(
            f"  failed:    {main.status_counts['failed']} "
            f"({_format_signed(delta.status_deltas['failed'])})"
        )
    lines.append(f"  estimated remaining: {_format_remaining_eta(main, now_us)}")

    # 2. 双快照区间增量块（仅对比模式叠加）
    if delta is not None:
        lines.append("")
        lines.append("Changes (before -> after):")
        lines.append("  Transitions:")
        if delta.transitions:
            for label, count in delta.transitions:
                lines.append(f"    {label}: {count}")
        else:
            lines.append("    (no state transitions)")

        outcome_parts = [
            f"{name}={delta.new_attempts_by_outcome.get(name, 0)}"
            for name in _OUTCOME_ORDER
        ]
        for extra_outcome in sorted(
            set(delta.new_attempts_by_outcome) - set(_OUTCOME_ORDER)
        ):
            outcome_parts.append(
                f"{extra_outcome}={delta.new_attempts_by_outcome[extra_outcome]}"
            )
        if delta.new_success_duration_stats is not None:
            ds = delta.new_success_duration_stats
            dur_part = (
                f"success duration (n={ds.count}): "
                f"total={_format_duration(ds.total_s)}, "
                f"Avg={_format_duration(ds.avg_s)}, "
                f"P50={_format_duration(ds.p50_s)}, "
                f"Max={_format_duration(ds.max_s)}"
            )
        else:
            dur_part = "success duration: N/A"
        lines.append(
            f"  New Attempts ({delta.new_attempts_total}): "
            f"{', '.join(outcome_parts)} | {dur_part}"
        )

        lines.append(f"  Newly Failed Jobs ({len(delta.newly_failed)}):")
        if delta.newly_failed:
            for failed_info, transition_label in delta.newly_failed:
                lines.append(
                    f"    - {failed_info.job_uid} ({transition_label}): "
                    f"{failed_info.error_summary}"
                )
        else:
            lines.append("    (none)")

    # 3. 各 task_type 进度与历史成功耗时统计
    lines.append("")
    lines.append("By Task Type:")
    all_task_types = sorted(
        set(main.by_task_type) | set(main.duration_stats_by_task_type)
    )
    if not all_task_types:
        lines.append("  (none)")
    else:
        for task_type in all_task_types:
            counts = main.by_task_type.get(task_type, _TaskTypeCounts())
            stats = main.duration_stats_by_task_type.get(task_type)
            if stats is not None:
                stats_part = (
                    f"success duration (n={stats.count}): "
                    f"Avg={_format_duration(stats.avg_s)}, "
                    f"P50={_format_duration(stats.p50_s)}, "
                    f"Max={_format_duration(stats.max_s)}"
                )
            else:
                stats_part = "success duration: N/A"
            lines.append(
                f"  {task_type}: pending={counts.pending}, running={counts.running}, "
                f"succeeded={counts.succeeded}, failed={counts.failed} "
                f"(total={counts.total}) | {stats_part}"
            )

    # 4. 在途 running 作业明细与 ETA
    lines.append("")
    lines.append(f"Running Jobs ({len(main.running_jobs)}):")
    if not main.running_jobs:
        lines.append("  (none)")
    else:
        for running_item in main.running_jobs:
            baseline = main.duration_stats_by_task_type.get(running_item.task_type)
            lines.append(_format_running_line(running_item, now_us, baseline))

    # 5. 排队中 pending 作业明细（保持队列顺序，超上限截断）
    lines.append("")
    lines.append(f"Pending Jobs ({len(main.pending_jobs)}):")
    if not main.pending_jobs:
        lines.append("  (none)")
    else:
        for pending_item in main.pending_jobs[:_DEFAULT_PENDING_LIST_LIMIT]:
            lines.append(
                f"  - {pending_item.job_uid} (task_type={pending_item.task_type})"
            )
        omitted = len(main.pending_jobs) - _DEFAULT_PENDING_LIST_LIMIT
        if omitted > 0:
            lines.append(f"  ... and {omitted} more pending jobs")

    # 6. 当前 failed 作业摘要
    lines.append("")
    lines.append(f"Failed Jobs ({len(main.failed_jobs)}):")
    if not main.failed_jobs:
        lines.append("  (none)")
    else:
        for failed_item in main.failed_jobs:
            lines.append(f"  - {failed_item.job_uid}: {failed_item.error_summary}")

    return "\n".join(lines)
