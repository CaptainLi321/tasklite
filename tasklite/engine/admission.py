"""触发器求值与重入队接缝：核心仅负责配置、继承与派发接线。"""
from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass
import os
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol

from ..exceptions import TaskLiteError
from ..models.artifact import resolve_job_files_config
from ..models.job import Job
from ..models.task import TaskRegistry, validate_trigger
from ..models.trigger import TriggerContext
from ..triggers import BUILTIN_TRIGGER_FUNCTIONS
from ..utils.clock import parse_utc_iso_to_us


def extract_trusted_metadata_timestamp_us(
    metadata: Mapping[str, Any] | None,
) -> int | None:
    """从 successes 档案 metadata 中提取并校验框架写入的可信完成时间戳（整数微秒）。

    过滤人工 seed_successes 的空字典 ``{}``、缺失或非正整数的 ``success_count``、
    非法 ``last_success_run_id`` 以及缺失或无法解析的 ``last_succeeded_at``。
    """
    if not isinstance(metadata, Mapping) or not metadata:
        return None
    success_count = metadata.get("success_count")
    if (
        not isinstance(success_count, int)
        or isinstance(success_count, bool)
        or success_count < 1
    ):
        return None
    if "last_success_run_id" in metadata:
        run_id = metadata.get("last_success_run_id")
        if not isinstance(run_id, str) or not run_id:
            return None
    last_succeeded_at = metadata.get("last_succeeded_at")
    if not isinstance(last_succeeded_at, str) or not last_succeeded_at:
        return None
    try:
        return parse_utc_iso_to_us(last_succeeded_at)
    except (TypeError, ValueError):
        return None


def is_trusted_success_metadata(metadata: Mapping[str, Any] | None) -> bool:
    """判断 successes 档案 metadata 是否具备完整的框架成功运行元数据。"""
    return extract_trusted_metadata_timestamp_us(metadata) is not None


EFFECTIVE_JOB_CONFIG_KEYS = (
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
    """提取已解析作业快照的有效配置投影（供同 UID 幂等/冲突比较）。"""
    return {
        "task_type": job_snapshot.get("task_type"),
        "job_id": job_snapshot.get("job_id"),
        "payload": job_snapshot.get("payload") or {},
        "depends_on": list(job_snapshot.get("depends_on") or []),
        "files": job_snapshot.get("files") or {},
        "resources": dict(job_snapshot.get("resources") or {}),
        "max_retries_per_run": job_snapshot.get("max_retries_per_run"),
        "timeout_seconds": job_snapshot.get("timeout_seconds"),
        "trigger": job_snapshot.get("trigger"),
    }


class TerminalHistory(Protocol):
    """准入判定消费的终态历史契约（PipelineState 满足）。"""

    @property
    def successes(self) -> Mapping[str, dict]: ...

    @property
    def failed(self) -> Mapping[str, dict]: ...


class TriggerRegistry:
    """触发器名称注册与主进程求值的唯一出口。"""

    def __init__(
        self,
        *,
        tasks: TaskRegistry | None = None,
        functions: Mapping[str, Callable[[TriggerContext], bool]] | None = None,
        base_dir: str | Path | None = None,
    ) -> None:
        self.tasks = tasks
        self.functions = dict(
            BUILTIN_TRIGGER_FUNCTIONS if functions is None else functions
        )
        self.base_dir: Path = (
            Path(os.path.abspath(os.fspath(base_dir)))
            if base_dir is not None
            else Path(os.path.abspath(os.getcwd()))
        )

    def register(self, name: str, fn: Callable[[TriggerContext], bool]) -> None:
        validate_trigger(name)
        if name in self.functions:
            raise TaskLiteError(f"trigger {name!r} already registered")
        if not callable(fn):
            raise TaskLiteError("trigger function must be callable")
        self.functions[name] = fn

    def normalize_job_snapshot(self, job_snapshot: dict, task_type: str) -> bool:
        """摄入时落实 Task 四项规格与 FileSpec 快照解析（普通路径按固定基准绝对化、校验上游引用直接依赖）。"""
        task = self.tasks.get(task_type) if self.tasks is not None else None
        merged_resources: dict[str, float] = (
            dict(task.resources) if task and task.resources else {}
        )
        defaults: dict[str, Any] = {
            "resources": merged_resources,
            "max_retries_per_run": task.max_retries_per_run if task else 3,
            "timeout_seconds": task.timeout_seconds if task else 3600,
            "trigger": task.trigger if task else "once",
        }
        changed = False
        for key, value in defaults.items():
            if key == "resources":
                merged_resources.update(job_snapshot.get(key) or {})
                if merged_resources != job_snapshot.get(key):
                    changed = True
                job_snapshot[key] = merged_resources
            elif job_snapshot.get(key) is None:
                job_snapshot[key] = value
                changed = True
        trigger = job_snapshot.get("trigger")
        if trigger not in self.functions:
            raise TaskLiteError(f"unregistered trigger {trigger!r}")
        if job_snapshot.get("payload") is None:
            job_snapshot["payload"] = {}
            changed = True
        else:
            job_snapshot["payload"] = copy.deepcopy(job_snapshot["payload"])
        if job_snapshot.get("depends_on") is None:
            job_snapshot["depends_on"] = []
            changed = True
        else:
            job_snapshot["depends_on"] = list(job_snapshot["depends_on"])
        raw_files = job_snapshot.get("files")
        normalized_files = resolve_job_files_config(
            raw_files,
            base_dir=self.base_dir,
            depends_on=job_snapshot["depends_on"],
        )
        if raw_files is None or normalized_files != raw_files:
            changed = True
        job_snapshot["files"] = normalized_files
        return changed

    def admit(
        self,
        job_snapshot: dict,
        history: TerminalHistory,
        *,
        baseline_lookup: Callable[[str], int | None] | None = None,
    ) -> bool:
        """依赖就绪后构造隔离快照；任何判定错误原样向外抛出。"""
        job = Job.from_dict(copy.deepcopy(job_snapshot))
        name = job.trigger or "once"
        try:
            fn = self.functions[name]
        except KeyError:
            raise TaskLiteError(f"unregistered trigger {name!r}") from None
        uid = job.uid
        metadata = history.successes.get(uid)
        ctx = TriggerContext(
            job=job,
            has_succeeded=uid in history.successes,
            has_failed=uid in history.failed,
            success_metadata=(
                MappingProxyType(copy.deepcopy(metadata)) if metadata is not None else None
            ),
            last_succeeded_at_us=(
                baseline_lookup(uid)
                if baseline_lookup is not None and uid in history.successes else None
            ),
        )
        result = fn(ctx)
        if type(result) is not bool:
            raise TaskLiteError(f"trigger {name!r} must return bool, got {type(result).__name__}")
        return result


@dataclass(frozen=True)
class RequeuePlan:
    """重入队节奏规划（不可变值对象）。

    ``front`` 为 True 表示插队首（同轮内优先重扫，重试不被新入队作业
    排挤到饥饿尾部）；``delay_seconds`` 是延迟类策略的表达位——默认
    实现恒为 0（立即），当前核心无任何延迟消费方，未来节奏策略经
    RequeuePolicy 接缝填充此字段即可生效，无需改动调用方。
    """

    front: bool
    delay_seconds: float = 0.0


class RequeuePolicy(ABC):
    """已决定重试的作业经此内部接缝规划重入队位置。"""

    @abstractmethod
    def plan_requeue(self, job_snapshot: dict) -> RequeuePlan:
        """为已获准重试的作业规划节奏与位置。"""


class ImmediateRequeuePolicy(RequeuePolicy):
    """默认立即插入队首；重试预算由结算机器判断。"""

    def plan_requeue(self, job_snapshot: dict) -> RequeuePlan:
        return RequeuePlan(front=True, delay_seconds=0.0)


__all__ = [
    "EFFECTIVE_JOB_CONFIG_KEYS",
    "ImmediateRequeuePolicy",
    "RequeuePlan",
    "RequeuePolicy",
    "TriggerRegistry",
    "TerminalHistory",
    "extract_effective_job_config",
    "extract_trusted_metadata_timestamp_us",
    "is_trusted_success_metadata",
]
