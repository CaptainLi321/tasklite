"""v2 Job 模型：一次有界激活的逻辑实例（ADR-0004 三层模型的实例位）。

字段归位原则：本类只承载「这一次激活」的语义——

- 规格位（可覆盖 Task 默认）：task_type / job_id / payload / depends_on /
  files / resources / max_retries_per_run / timeout_seconds / trigger；
- 实例位（框架运行期管理，由私有恢复入口 ``_restore`` 挂载）：attempt_no
  （本次激活内尝试序号，1 起始含首次执行；预算判定：允许执行条件
  ``attempt_no <= max_retries_per_run + 1``）、activation_no（激活代，
  每次从 successes/failed 拦截点放行重跑时 +1）、first_enqueued_at（UTC
  ISO，首次入队时间，重试不刷新，由 enqueue 填充）；
- 运行期边带状态收敛到 ``runtime`` 单一命名空间（JobRuntimeState），随
  job_snapshot 落盘持久化——序列化只此一处，新增状态不会因散装下划线键
  漏写 to_dict 而丢失。

身份不变式：``uid = make_job_uid(task_type, job_id)``，queue/successes/failed
三表主键与六集合互斥均以 uid 为身份；``__eq__``/``__hash__`` 仅按 uid。
"""
from __future__ import annotations

import copy
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from ..exceptions import TaskLiteError
from ..utils.encoding import make_job_uid
from .artifact import FileSpec, coerce_file_spec, parse_job_files_config
from .task import (
    validate_resource_amounts,
    validate_retry_budget,
    validate_task_type,
    validate_timeout_seconds,
    validate_trigger,
)

WORKER_RESOURCE = "__workers__"

# 内置触发器名称；自定义触发器同样仅以名称引用。
BUILTIN_TRIGGERS = ("once", "on_failure", "always", "on_input_change")


def inject_worker_resource(job_snapshot: dict) -> None:
    """给 job_snapshot 的 resources 注入默认 worker 槽位。"""
    resources = dict(job_snapshot.get("resources") or {})
    if WORKER_RESOURCE not in resources:
        resources[WORKER_RESOURCE] = 1.0
    job_snapshot["resources"] = resources


@dataclass
class JobRuntimeState:
    """Job 运行期内部边带状态（强类型结构化存储）。

    不变式：runtime 命名空间内仅 `_` 前缀名为框架字段，其余任意键（含与
    字段同名的无下划线形态）一律是用户的 extra 数据——to_dict/from_dict
    往返对 extra 命名空间无损，框架读写永不劫持或丢弃 extra 键。
    """

    commit_failures: int = 0
    dispatch_failures: int = 0
    last_retry_error: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def record_retry_error(self, error: object) -> None:
        """记录最近一次重试的错误串（空值收敛为空串）。"""
        self.last_retry_error = str(error or "")

    def record_commit_failure(self) -> int:
        """记录一次 commit 失败并返回累计失败次数。"""
        self.commit_failures += 1
        return self.commit_failures

    def record_dispatch_failure(self) -> int:
        """记录一次 dispatch 失败并返回累计失败次数。"""
        self.dispatch_failures += 1
        return self.dispatch_failures

    def to_dict(self) -> dict[str, Any]:
        """导出持久化字典（完全保留 `_` 开头与任意 extra 字段）。"""
        d = dict(self.extra)
        for key, (attr, empty) in _RUNTIME_FIELDS.items():
            val = getattr(self, attr)
            if val != empty:
                d[key] = val
            else:
                d.pop(key, None)
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any] | JobRuntimeState | None) -> JobRuntimeState:
        if isinstance(data, JobRuntimeState):
            return cls(
                commit_failures=data.commit_failures,
                dispatch_failures=data.dispatch_failures,
                last_retry_error=data.last_retry_error,
                extra=dict(data.extra),
            )
        if not isinstance(data, dict):
            return cls()

        extra = dict(data)

        def _pop_val(key: str) -> Any:
            return extra.pop(key, None)

        raw_cf = _pop_val(RT_COMMIT_FAILURES)
        try:
            commit_failures = int(raw_cf) if raw_cf is not None else 0
        except (ValueError, TypeError):
            commit_failures = 0

        raw_df = _pop_val(RT_DISPATCH_FAILURES)
        try:
            dispatch_failures = int(raw_df) if raw_df is not None else 0
        except (ValueError, TypeError):
            dispatch_failures = 0

        raw_re = _pop_val(RT_LAST_RETRY_ERROR)
        last_retry_error = str(raw_re or "")

        return cls(
            commit_failures=commit_failures,
            dispatch_failures=dispatch_failures,
            last_retry_error=last_retry_error,
            extra=extra,
        )


# runtime 规范键注册表：框架字段的单一事实源。empty 为「未设置」哨兵
# （等于哨兵时 to_dict 导出省略）；写入侧的规范化（计数与取串）语义
# 保留在各写入方法内显式表达。
# runtime 规范键名常量（键名字符串的单一引用点；引擎写入侧与测试经此引用）
RT_COMMIT_FAILURES = "_commit_failures"
RT_DISPATCH_FAILURES = "_dispatch_failures"
RT_LAST_RETRY_ERROR = "_last_retry_error"

_RUNTIME_FIELDS: dict[str, tuple[str, Any]] = {
    RT_COMMIT_FAILURES: ("commit_failures", 0),
    RT_DISPATCH_FAILURES: ("dispatch_failures", 0),
    RT_LAST_RETRY_ERROR: ("last_retry_error", ""),
}


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
    """payload 入口校验与规范化：仅接受 dict 或 None，深拷贝隔离外部变异。"""
    if payload is not None and not isinstance(payload, dict):
        raise TypeError(
            f"payload must be a dict or None, got {type(payload).__name__} ({payload!r})"
        )
    return copy.deepcopy(payload) if payload else {}


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
        raise TypeError(
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
        raise TypeError(
            f"depends_on must be a list of strings, got {type(depends_on).__name__}"
        )
    normalized = list(depends_on) if depends_on is not None else []
    if not all(isinstance(d, str) for d in normalized):
        raise TypeError(
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


def _validate_activation_no(activation_no: Any) -> None:
    """activation_no 入口校验：非 bool 的 int 且 >= 1（初激活为 1）。"""
    if not isinstance(activation_no, int) or isinstance(activation_no, bool):
        raise TypeError(
            f"activation_no must be an int, got {type(activation_no).__name__} "
            f"({activation_no!r})"
        )
    if activation_no < 1:
        raise ValueError(f"activation_no must be >= 1, got {activation_no}")


def _validate_first_enqueued_at(first_enqueued_at: Any) -> None:
    """first_enqueued_at 入口校验：str（UTC ISO）或 None（尚未入队）。

    非-str 非-None 值会以随机类型潜伏到落盘序列化处才崩，入口拒绝；
    时间戳本身由 enqueue 单点填充，本处只做类型门卫。
    """
    if first_enqueued_at is not None and not isinstance(first_enqueued_at, str):
        raise TypeError(
            f"first_enqueued_at must be a str or None, got "
            f"{type(first_enqueued_at).__name__} ({first_enqueued_at!r})"
        )
    if first_enqueued_at == "":
        raise ValueError("first_enqueued_at must be a non-empty str or None")


class Job:
    """一次有界激活的逻辑实例（uid = make_job_uid(task_type, job_id) 身份不变）。

    Args:
        task_type: 任务类型名（须匹配已注册 Task 的 task_type）。非空 str。
        job_id: 任务类型内的唯一标识。非空 str。
        payload: 传给 handler 的业务数据 dict（须可 JSON 序列化，完整
            序列化预检在 enqueue/spawn 侧执行，构造期先做 dict 类型门卫）。
        depends_on: 上游 job uid 列表。失败的上游会级联阻断本 job。
        files: 文件角色到文件规格的映射（默认空 dict）。
        resources: None 继承 Task 默认资源；显式映射按键合并并覆盖同名资源。
        max_retries_per_run: None 继承 Task 重试预算；显式值覆盖模板。允许执行
            条件为 ``attempt_no <= max_retries_per_run + 1``；0 表示任何失败直接进
            失败档案。
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
        self.attempt_no: int = 1
        self.activation_no: int = 1
        self.first_enqueued_at: str | None = None
        self.runtime = None

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
        runtime: dict[str, Any] | JobRuntimeState | None = None,
        attempt_no: int = 1,
        activation_no: int = 1,
        first_enqueued_at: str | None = None,
    ) -> Job:
        """内部私有恢复入口：重建 Job 并挂载运行期实例位与边带状态。"""
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
        job.attempt_no = attempt_no
        _validate_activation_no(activation_no)
        job.activation_no = activation_no
        _validate_first_enqueued_at(first_enqueued_at)
        job.first_enqueued_at = first_enqueued_at
        job.runtime = runtime
        return job

    @property
    def runtime(self) -> JobRuntimeState:
        """Job 运行期内部边带状态（强类型结构化存储）。"""
        return self._runtime

    @runtime.setter
    def runtime(self, value: dict[str, Any] | JobRuntimeState | None) -> None:
        if isinstance(value, JobRuntimeState):
            self._runtime = value
        else:
            self._runtime = JobRuntimeState.from_dict(value)

    def to_dict(self) -> dict[str, Any]:
        """序列化 job 为字典（payload 深拷贝、files 导出 FileSpec JSON 字典、
        resources/depends_on 浅拷贝，防外部就地变异内部状态）；``runtime``
        子 dict 显式保留——边带状态集中序列化，不散装丢失。"""
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
            "runtime": self.runtime.to_dict(),
            "attempt_no": self.attempt_no,
            "activation_no": self.activation_no,
            "first_enqueued_at": self.first_enqueued_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Job:
        """反序列化 job（缺键回默认值；trigger 缺键/None = 未指定哨兵）。"""
        if "rerun" in data:
            raise ValueError("Job 快照不接受旧字段 rerun；请使用 trigger")
        if "timeout" in data:
            raise ValueError("Job 快照不接受旧字段 timeout；请使用 timeout_seconds")
        if "max_retries" in data:
            raise ValueError("Job 快照不接受旧字段 max_retries；请使用 max_retries_per_run")
        if "timeout_is_transient" in data:
            raise ValueError("Job 快照不接受已移除字段 timeout_is_transient")
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
            # runtime 子 dict 显式保留——边带状态只存这一处
            runtime=data.get("runtime", {}),
            attempt_no=data["attempt_no"] if data.get("attempt_no") is not None else 1,
            activation_no=data["activation_no"] if data.get("activation_no") is not None else 1,
            first_enqueued_at=data.get("first_enqueued_at"),
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
    "JobRuntimeState",
    "BUILTIN_TRIGGERS",
    "RT_COMMIT_FAILURES",
    "RT_DISPATCH_FAILURES",
    "RT_LAST_RETRY_ERROR",
    "WORKER_RESOURCE",
    "inject_worker_resource",
]
