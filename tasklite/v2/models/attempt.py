"""v2 Attempt 轨迹层：一次物理执行的 append-only 记录（ADR-0004）。

Attempt = 一次物理执行的轨迹行：派发即插行（outcome=running），终态或
重入队时更新该行。追溯链：任一 attempt 行 → job_uid → 当前终态
（wall/failed）→ task_type → 进程内 Task 注册规格。

旁路观测面定位：attempts 不参与六集合互斥——wall/failed 仍按 uid 唯一
终态，「最终状态唯一」与「历史可追溯」由此解耦；``incarnation``
（``run_id.dispatch_seq``）随 attempt 落表，不再只存在于 IPC 文件名。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# outcome 词汇表（单一真相）：
# - running：已派发、尚未收尾；
# - succeeded：本次执行成功；
# - failed：本次执行失败且耗尽预算（job 进入失败档案）；
# - requeued：本次执行以重入队收尾（瞬态失败/瞬态信号，job 继续重试）；
# - skipped：激活被终态拦截点放行重跑前跳过（如 wall 命中跳过）。
ATTEMPT_OUTCOMES = ("running", "succeeded", "failed", "requeued", "skipped")
# 写侧派生常量（值解包自词汇表单一真相）：派发开行与终态/重入队收尾
# 的全部写点统一引用，拼写漂移由 _validate_outcome 入口拒绝。
(
    ATTEMPT_RUNNING,
    ATTEMPT_SUCCEEDED,
    ATTEMPT_FAILED,
    ATTEMPT_REQUEUED,
    ATTEMPT_SKIPPED,
) = ATTEMPT_OUTCOMES


def _validate_non_empty_str(field_name: str, value: Any) -> None:
    """字符串字段门卫：仅接受非空 str。

    空/非 str 值会以随机类型潜伏到落盘序列化处才崩（append-only 表写半
    行），入口拒绝。
    """
    if not isinstance(value, str):
        raise TypeError(
            f"{field_name} must be a str, got {type(value).__name__} ({value!r})"
        )
    if not value:
        raise ValueError(f"{field_name} must be a non-empty str, got {value!r}")


def _validate_optional_str(field_name: str, value: Any) -> None:
    """可空字符串字段门卫：非空 str 或 None（None = 尚未发生）。"""
    if value is not None:
        _validate_non_empty_str(field_name, value)


def _validate_counter(field_name: str, value: Any) -> None:
    """序号字段门卫：非 bool 的 int 且 >= 1（与 Job 实例位同规则）。"""
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(
            f"{field_name} must be an int, got {type(value).__name__} ({value!r})"
        )
    if value < 1:
        raise ValueError(f"{field_name} must be >= 1, got {value}")


def _validate_outcome(outcome: Any) -> None:
    """outcome 入口校验：仅接受词汇表内的值（fail-loud）。

    拼错的 outcome 会静默落为未知行，追溯链查询按词汇表过滤时漏看，
    入口拒绝。
    """
    if outcome not in ATTEMPT_OUTCOMES:
        raise ValueError(
            f"outcome must be one of {'/'.join(repr(v) for v in ATTEMPT_OUTCOMES)}, "
            f"got {outcome!r}"
        )


@dataclass(frozen=True)
class AttemptRecord:
    """一次物理执行的不可变轨迹记录（对应 attempts 表一行）。

    Attributes:
        job_uid: 归属 job 的 uid（``task_type::job_id``）。
        activation_no: 激活代（job 从 wall/failed 拦截点放行重跑时 +1）。
        attempt_no: 本次激活内尝试序号（1 起始含首次执行）。
        incarnation: 执行代标识 ``run_id.dispatch_seq``（fencing 依据）。
        run_id: 归属 run 的标识。
        started_at: 派发时刻（UTC ISO）。
        finished_at: 收尾时刻（UTC ISO）；None 表示仍在执行。
        outcome: 结局，取 ATTEMPT_OUTCOMES 词汇表。
        error: 失败/重入队时的错误串；成功/执行中为 None。
    """

    job_uid: str
    activation_no: int
    attempt_no: int
    incarnation: str
    run_id: str
    started_at: str
    outcome: str
    finished_at: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        _validate_non_empty_str("job_uid", self.job_uid)
        _validate_counter("activation_no", self.activation_no)
        _validate_counter("attempt_no", self.attempt_no)
        _validate_non_empty_str("incarnation", self.incarnation)
        _validate_non_empty_str("run_id", self.run_id)
        _validate_non_empty_str("started_at", self.started_at)
        _validate_outcome(self.outcome)
        _validate_optional_str("finished_at", self.finished_at)
        _validate_optional_str("error", self.error)

    @property
    def is_running(self) -> bool:
        """本次执行是否尚未收尾（唯一非终态 outcome 为 running）。"""
        return self.outcome == "running"

    def to_dict(self) -> dict[str, Any]:
        """导出持久化字典（对应 attempts 表列名；None 字段原样保留）。"""
        return {
            "job_uid": self.job_uid,
            "activation_no": self.activation_no,
            "attempt_no": self.attempt_no,
            "incarnation": self.incarnation,
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "outcome": self.outcome,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AttemptRecord:
        """从持久化字典重建（必填键缺失显式 KeyError，可空键缺省 None）。"""
        return cls(
            job_uid=data["job_uid"],
            activation_no=data["activation_no"],
            attempt_no=data["attempt_no"],
            incarnation=data["incarnation"],
            run_id=data["run_id"],
            started_at=data["started_at"],
            outcome=data["outcome"],
            finished_at=data.get("finished_at"),
            error=data.get("error"),
        )


__all__ = [
    "ATTEMPT_FAILED",
    "ATTEMPT_OUTCOMES",
    "ATTEMPT_REQUEUED",
    "ATTEMPT_RUNNING",
    "ATTEMPT_SKIPPED",
    "ATTEMPT_SUCCEEDED",
    "AttemptRecord",
]
