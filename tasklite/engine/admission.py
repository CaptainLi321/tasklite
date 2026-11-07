"""触发器求值与重入队接缝：核心仅负责配置、继承与派发接线。"""
from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol

from ..models.job import Job
from ..models.task import TaskRegistry, validate_trigger
from ..models.trigger import TriggerContext
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
    ) -> None:
        self.tasks = tasks
        self.functions = dict(functions or {})

    def register(self, name: str, fn: Callable[[TriggerContext], bool]) -> None:
        validate_trigger(name)
        if name in self.functions:
            raise ValueError(f"trigger {name!r} already registered")
        if not callable(fn):
            raise TypeError("trigger function must be callable")
        self.functions[name] = fn

    def normalize_job_snapshot(self, job_snapshot: dict, task_type: str) -> bool:
        """摄入时落实 Task 四项规格，显式资源键覆盖模板默认（空 dict 不清空默认）。"""
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
        if job_snapshot.get("files") is None:
            job_snapshot["files"] = {}
            changed = True
        else:
            job_snapshot["files"] = copy.deepcopy(job_snapshot["files"])
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
            raise ValueError(f"unregistered trigger {name!r}") from None
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
            raise TypeError(f"trigger {name!r} must return bool, got {type(result).__name__}")
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
    """重试节奏唯一出口的接缝契约。

    核心引擎不做任何节奏计算：需要把一个失败/瞬态作业放回队列时，
    一律经 ``plan_requeue`` 取得节奏规划（何时、何处）。瞬态信号
    （interrupted / lock_conflict / rate_limited）不烧重试预算的军规
    由「立即重入队」默认策略自然成立——零等待、零预算消耗。
    """

    @abstractmethod
    def plan_requeue(
        self,
        job_snapshot: dict,
        *,
        transient_kind: str | None = None,
    ) -> RequeuePlan:
        """为一个待重入队的作业规划节奏。

        ``transient_kind`` 非 None 表示瞬态信号（引擎值对象
        TRANSIENT_KIND_STAT_KEYS 登记的三类）——预算豁免语境下策略
        不得因预算耗尽拒绝重入队；返回值表达节奏与位置。
        """


class ImmediateRequeuePolicy(RequeuePolicy):
    """立即重入队策略（默认）：零延迟、插队首。

    语义与瞬态信号的队首重入队路径一致（锁冲突/限流/中断的降级回队
    均为 front 插队）；业务失败重试同享该节奏——预算判定在收尾机器
    侧先行，走到本策略的作业都已是「决定重试」的作业。
    """

    def plan_requeue(
        self,
        job_snapshot: dict,
        *,
        transient_kind: str | None = None,
    ) -> RequeuePlan:
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
