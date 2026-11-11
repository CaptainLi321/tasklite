"""Job 逻辑实例：业务身份、有效配置与只读执行观察字段。

身份不变式：uid 仅由 task_type 与 job_id 确定；执行编号由后端进入
running 的事务分配，跨 Run 单调递增，与重试预算相互独立。
"""
from __future__ import annotations

import copy
from collections.abc import Sequence
from typing import Any

from ..exceptions import TaskLiteError
from ..utils.encoding import make_job_uid
from .artifact import FileSpec, coerce_file_spec, parse_job_files_config
from .result import validate_strict_json_object
from .task import (
    validate_resource_amounts,
    validate_retry_budget,
    validate_task_type,
    validate_timeout_seconds,
    validate_trigger,
)

WORKER_RESOURCE = "__workers__"

# 内置触发器名称；自定义触发器同样仅以名称引用。
BUILTIN_TRIGGERS = ("once", "until_success", "always", "on_input_change")


def inject_worker_resource(job_snapshot: dict) -> None:
    """给 job_snapshot 的 resources 注入默认 worker 槽位。"""
    resources = dict(job_snapshot.get("resources") or {})
    if WORKER_RESOURCE not in resources:
        resources[WORKER_RESOURCE] = 1.0
    job_snapshot["resources"] = resources


def _validate_job_id(job_id: Any) -> None:
    """job_id 入口校验：非空 str。

    拒绝非 str——str 强转会使数值与字符串字面量静默碰撞，混合
    数值/字符串 job_id 的管线静默合并不同任务（successes 去重吞掉后者），
    与 task_type 的 str 校验对称。
    """
    if not isinstance(job_id, str):
        raise TaskLiteError(
            f"job_id must be a str, got {type(job_id).__name__} ({job_id!r})"
        )
    if not job_id:
        raise TaskLiteError(f"job_id must be a non-empty str, got {job_id!r}")
    try:
        job_id.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise TaskLiteError(
            f"job_id must be valid UTF-8 str, got {job_id!r}"
        ) from exc


def _normalize_payload(payload: Any) -> dict[str, Any]:
    """按严格 JSON 对象契约捕获 payload，拒绝隐式转换并隔离外部变异。"""
    return validate_strict_json_object(payload, field_name="payload")


def _normalize_files(
    files: Any, *, allow_dict: bool = False
) -> dict[str, FileSpec]:
    """files 入口校验与规范化：仅接受 dict[str, FileSpec] 或 None，构造独立映射。"""
    return parse_job_files_config(files, allow_dict=allow_dict)


def _normalize_resources(resources: Any) -> dict[str, float]:
    """resources 入口校验与规范化。

    非 dict 类型（str 等可迭代）会在迭代处抛原始 AttributeError
    （不可读），入口显式拒绝；数值规则委托模型层共享校验单点。
    """
    if resources is not None and not isinstance(resources, dict):
        raise TaskLiteError(
            f"resources must be a dict or None, got {type(resources).__name__} ({resources!r})"
        )
    normalized = dict(resources) if resources else {}
    validate_resource_amounts(normalized, "resources")
    return normalized


def _normalize_depends_on(depends_on: Any) -> list[str]:
    """depends_on 入口校验与规范化：list/tuple 且元素全为 str。

    先校验外层类型再迭代——str 可迭代出 str，若直接 all(isinstance)
    检查会把 "parent::id" 静默肢解为字符列表，作业以依赖死锁死亡。
    """
    if depends_on is not None and not isinstance(depends_on, (list, tuple)):
        raise TaskLiteError(
            f"depends_on must be a list of strings, got {type(depends_on).__name__}"
        )
    normalized = list(depends_on) if depends_on is not None else []
    if not all(isinstance(d, str) for d in normalized):
        raise TaskLiteError(
            f"depends_on must contain only strings, got {normalized!r}"
        )
    return normalized


def _validate_attempt_no(attempt_no: Any) -> None:
    """attempt_no 入口校验：非 bool 的 int 且 >= 1（1 起始含首次执行）。"""
    if not isinstance(attempt_no, int) or isinstance(attempt_no, bool):
        raise TypeError(
            f"attempt_no must be an int, got {type(attempt_no).__name__} "
            f"({attempt_no!r})"
        )
    if attempt_no < 1:
        raise ValueError(f"attempt_no must be >= 1, got {attempt_no}")


class Job:
    """具有稳定身份的逻辑实例（uid = make_job_uid(task_type, job_id) 身份不变）。

    Args:
        task_type: 任务类型名（须匹配已注册 Task 的 task_type）。非空 str。
        job_id: 任务类型内的唯一标识。非空 str。
        payload: 传给 handler 的业务数据 dict（须可 JSON 序列化，完整
            序列化预检在 enqueue/spawn 侧执行，构造期先做 dict 类型门卫）。
        depends_on: 上游 job uid 列表。失败的上游会级联阻断本 job。
        files: 文件角色到文件规格的映射（默认空 dict）。
        resources: None 继承 Task 默认资源；显式映射按键合并并覆盖同名资源。
        max_retries_per_run: None 继承 Task 重试预算；显式值覆盖模板。重试许可
            由本 Run 已消耗的重试次数判定；0 表示不允许消耗预算的重试。
        timeout_seconds: None 继承 Task；显式秒数须为有限正数。
        trigger: 首次尝试的触发器名称；None 继承 Task。
    """

    def __init__(
        self,
        task_type: str,
        job_id: str,
        *,
        payload: dict[str, Any] | None = None,
        depends_on: Sequence[str] | None = None,
        files: dict[str, FileSpec] | None = None,
        resources: dict[str, float] | None = None,
        max_retries_per_run: int | None = None,
        timeout_seconds: int | float | None = None,
        trigger: str | None = None,
    ) -> None:
        # 身份校验链：类型 → 非空 → 构造规范 uid
        validate_task_type(task_type)
        self.task_type = task_type
        _validate_job_id(job_id)
        self.job_id = job_id
        self._uid = make_job_uid(task_type, job_id)

        self.payload = _normalize_payload(payload)
        self.depends_on = _normalize_depends_on(depends_on)
        self.files = _normalize_files(files, allow_dict=False)
        self.resources = None if resources is None else _normalize_resources(resources)
        if max_retries_per_run is not None:
            validate_retry_budget(max_retries_per_run)
        self.max_retries_per_run = max_retries_per_run
        if timeout_seconds is not None:
            validate_timeout_seconds(timeout_seconds)
        self.timeout_seconds = timeout_seconds
        if trigger is not None:
            validate_trigger(trigger)
        self.trigger = trigger

        # 实例位：公开构造初始化为默认初态，恢复路径统一经 _restore 挂载
        self._attempt_no: int = 1

    @classmethod
    def _restore(
        cls,
        task_type: str,
        job_id: str,
        *,
        payload: dict[str, Any] | None = None,
        depends_on: Sequence[str] | None = None,
        files: dict[str, Any] | None = None,
        resources: dict[str, float] | None = None,
        max_retries_per_run: int | None = None,
        timeout_seconds: int | float | None = None,
        trigger: str | None = None,
        attempt_no: int = 1,
    ) -> Job:
        """内部恢复入口：重建有效配置并挂载执行编号。"""
        restored_files = _normalize_files(files, allow_dict=True)
        job = cls(
            task_type,
            job_id,
            payload=payload,
            depends_on=depends_on,
            files=restored_files,
            resources=resources,
            max_retries_per_run=max_retries_per_run,
            timeout_seconds=timeout_seconds,
            trigger=trigger,
        )
        _validate_attempt_no(attempt_no)
        job._attempt_no = attempt_no
        return job

    @property
    def attempt_no(self) -> int:
        """当前执行编号；由框架持久进入 running 时分配，与预算无关。"""
        return self._attempt_no

    def to_dict(self) -> dict[str, Any]:
        """捕获独立配置快照。"""
        return {
            "task_type": self.task_type,
            "job_id": self.job_id,
            "payload": copy.deepcopy(self.payload),
            "depends_on": list(self.depends_on),
            "files": {
                name: coerce_file_spec(spec, allow_dict=True).to_dict()
                for name, spec in _normalize_files(self.files, allow_dict=True).items()
            },
            "resources": None if self.resources is None else dict(self.resources),
            "max_retries_per_run": self.max_retries_per_run,
            "timeout_seconds": self.timeout_seconds,
            "trigger": self.trigger,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Job:
        """反序列化 job（缺键回默认值；trigger 缺键/None = 未指定哨兵）。"""
        if "rerun" in data:
            raise TaskLiteError("Job 快照不接受旧字段 rerun；请使用 trigger")
        if "timeout" in data:
            raise TaskLiteError("Job 快照不接受旧字段 timeout；请使用 timeout_seconds")
        if "max_retries" in data:
            raise TaskLiteError("Job 快照不接受旧字段 max_retries；请使用 max_retries_per_run")
        if "timeout_is_transient" in data:
            raise TaskLiteError("Job 快照不接受已移除字段 timeout_is_transient")
        return cls._restore(
            data["task_type"],
            data["job_id"],
            payload=data.get("payload", {}),
            depends_on=data.get("depends_on", []),
            files=data.get("files", {}),
            resources=data.get("resources"),
            max_retries_per_run=data.get("max_retries_per_run"),
            timeout_seconds=data.get("timeout_seconds"),
            trigger=data.get("trigger"),
            attempt_no=data["attempt_no"] if data.get("attempt_no") is not None else 1,
        )

    @property
    def uid(self) -> str:
        """唯一标识：make_job_uid(task_type, job_id)。"""
        return self._uid

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Job):
            return NotImplemented
        return self.uid == other.uid

    def __hash__(self) -> int:
        return hash(self.uid)


__all__ = [
    "Job",
    "BUILTIN_TRIGGERS",
    "WORKER_RESOURCE",
    "inject_worker_resource",
]
