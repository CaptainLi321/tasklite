"""v2 Job 模型：一次有界激活的逻辑实例（ADR-0004 三层模型的实例位）。

字段归位原则：本类只承载「这一次激活」的语义——

- 规格位（可覆盖 Task 默认）：task_type / job_id / payload / resources /
  depends_on / timeout / max_retries / timeout_is_transient / rerun；
- 实例位（框架运行期管理）：attempt_no（本次激活内尝试序号，1 起始含
  首次执行；预算判定：允许执行条件 ``attempt_no <= max_retries + 1``）、
  activation_no（激活代，每次从 wall/failed 拦截点放行重跑时 +1）、
  first_enqueued_at（UTC ISO，首次入队时间，重试不刷新，由 enqueue 填充）；
- 运行期边带状态收敛到 ``runtime`` 单一命名空间（JobRuntimeState），随
  job_dict 落盘持久化——序列化只此一处，新增状态不会因散装下划线键
  漏写 to_dict 而丢失。

身份不变式：``uid = task_type::job_id``，queue/wall/failed 三表主键与
六集合互斥均以 uid 为身份；``__eq__``/``__hash__`` 仅按 uid。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..utils.encoding import safe_uid_filename
from .task import (
    validate_resource_amounts,
    validate_retry_budget,
    validate_task_type,
    validate_timeout,
    validate_timeout_flag,
)

# uid 派生的 IPC 文件名（锁 / result / signals / outputs）
# 无截断——job_id/task_type 超长时文件名超 255 字节（EXT4 单文件名字节
# 上限）→ os.open ENAMETOOLONG → 派发侧异常未分类 → 重入队 + 崩溃重启
# → livelock（job 永远无法执行且不进失败档案）。入口按 safe_uid_filename
# 实际字节数（含转义膨胀）拒绝，fail-fast。
# 255 - 56 = 199：56 为最坏结果文件后缀 ``.{32-hex run_id}.{seq}.result.json``。
_MAX_SAFE_UID_BYTES = 199

WORKER_RESOURCE = "__workers__"

# rerun 策略合法值（校验点：Job 构造 / 任务级默认策略注册）与「豁免重跑」
# 子集（判定点：PipelineState 六集合互斥豁免）的单一真相；豁免集 =
# 合法集 − {"never"}，新增策略值漏改任一消费点即静默丢豁免。
RERUN_VALUES = ("never", "on_failure", "every_run", "on_input_change")
RERUN_EXEMPT_VALUES = ("every_run", "on_failure", "on_input_change")
assert set(RERUN_EXEMPT_VALUES) == set(RERUN_VALUES) - {"never"}


def inject_worker_resource(job_dict: dict) -> None:
    """给 job_dict 的 resources 注入默认 worker 槽位。"""
    resources = dict(job_dict.get("resources", {}))
    if WORKER_RESOURCE not in resources:
        resources[WORKER_RESOURCE] = 1.0
    job_dict["resources"] = resources


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

    def __getitem__(self, key: str) -> Any:
        spec = _RUNTIME_FIELDS.get(key)
        if spec is not None:
            val = getattr(self, spec[0])
            if val is not None:
                return val
            raise KeyError(key)
        return self.extra[key]

    def __setitem__(self, key: str, value: Any) -> None:
        if key == RT_COMMIT_FAILURES:
            self.commit_failures = int(value or 0)
        elif key == RT_DISPATCH_FAILURES:
            self.dispatch_failures = int(value or 0)
        elif key == RT_LAST_RETRY_ERROR:
            self.last_retry_error = str(value or "")
        else:
            self.extra[key] = value

    def __contains__(self, key: str) -> bool:
        spec = _RUNTIME_FIELDS.get(key)
        if spec is not None:
            return getattr(self, spec[0]) != spec[1]
        return key in self.extra

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def setdefault(self, key: str, default: Any = None) -> Any:
        if key not in self:
            self[key] = default
        return self[key]

    def pop(self, key: str, default: Any = None) -> Any:
        spec = _RUNTIME_FIELDS.get(key)
        if spec is not None:
            attr, empty = spec
            val = getattr(self, attr)
            setattr(self, attr, empty)
            return val if val != empty else default
        return self.extra.pop(key, default)


# runtime 规范键注册表：框架字段的单一事实源。empty 为「未设置」哨兵
# （读取等于哨兵时视同缺省、pop 清空回哨兵、to_dict 导出时省略）；
# 写入侧的规范化（计数与取串）语义保留在各写入方法内显式表达。
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
    """job_id 入口校验：非空 str 且不含 '::'。

    拒绝非 str——str 强转会使数值与字符串字面量静默碰撞
    （Job("t", 1.0).uid == Job("t", "1.0").uid == "t::1.0"），混合
    数值/字符串 job_id 的管线静默合并不同任务（wall 去重吞掉后者），
    与 task_type 的 str 校验对称。
    """
    if not isinstance(job_id, str):
        raise TypeError(
            f"job_id must be a str, got {type(job_id).__name__} ({job_id!r})"
        )
    if not job_id:
        raise ValueError(f"job_id must be a non-empty str, got {job_id!r}")
    if "::" in job_id:
        raise ValueError(f"job_id must not contain '::', got {job_id!r}")


def _validate_uid_ipc_filename(task_type: str, job_id: str) -> None:
    """uid 派生 IPC 文件名超 255 字节 → ENAMETOOLONG livelock，入口拒绝。"""
    if len(safe_uid_filename(f"{task_type}::{job_id}").encode("utf-8")) > _MAX_SAFE_UID_BYTES:
        raise ValueError(
            f"task_type+job_id too long: uid-derived IPC filename would exceed "
            f"filesystem name limit ({_MAX_SAFE_UID_BYTES} bytes max); got "
            f"{task_type!r}::{job_id!r} — shorten to avoid ENAMETOOLONG livelock"
        )


def _normalize_payload(payload: Any) -> dict[str, Any]:
    """payload 入口校验与规范化：仅接受 dict 或 None。

    非 dict payload 若放行，会潜伏到子进程序列化处才崩，入口拒绝。
    """
    if payload is not None and not isinstance(payload, dict):
        raise TypeError(
            f"payload must be a dict or None, got {type(payload).__name__} ({payload!r})"
        )
    return dict(payload) if payload else {}


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


def _validate_rerun(rerun: Any) -> None:
    """rerun 策略入口校验（fail-loud）——非法值会被静默当 "never" 处理。

    None 是「未指定」哨兵（可被任务级默认策略注入），显式字符串一律
    尊重；仅拒绝未知字符串值。
    """
    if rerun is not None and rerun not in RERUN_VALUES:
        raise ValueError(
            f"rerun must be None (unspecified) or one of "
            f"{'/'.join(repr(v) for v in RERUN_VALUES)}, got {rerun!r}"
        )


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
    """一次有界激活的逻辑实例（uid = task_type::job_id 身份不变）。

    Args:
        task_type: 任务类型名（须匹配已注册 Task 的 task_type）。非空 str
            且不含 ``::``（uid 分隔符）。
        job_id: 任务类型内的唯一标识。非空 str 且不含 ``::``。
        payload: 传给 handler 的业务数据 dict（须可 JSON 序列化，完整
            序列化预检在 enqueue/spawn 侧执行，构造期先做 dict 类型门卫）。
        resources: 要获取的资源占用映射（覆盖 Task 默认资源）。
        max_retries: 重试预算上限（覆盖 Task 默认 3）。允许执行条件为
            ``attempt_no <= max_retries + 1``；0 表示任何失败直接进
            失败档案。
        depends_on: 上游 job uid 列表（``task_type::job_id``）。失败的
            上游会级联阻断本 job。
        timeout: 执行超时秒数（覆盖 Task 默认 3600），有限正数。
        timeout_is_transient: 超时是否视为瞬态失败（keyword-only）。
            True 时看门狗超时按重试处理，而非直接进失败档案。
        rerun: 跨会话重跑策略——「成功进 wall 是否算数」由它决定
            （**不改变 uid 派生**，身份仍是 uid）：
            - None（默认，未指定哨兵）：未指定——任务级默认策略可注入；
              无默认策略的任务按 "never" 处理。与显式 "never" 的区别：
              显式值一律尊重，不再被任务级默认覆盖（保持 enqueue 与
              spawn 行为一致）；
            - "never"：wall/failed 命中即跳过；
            - "on_failure"：wall 命中跳过，failed 命中重跑（网络误失败
              可自愈；成功即自动清失败档案残行）；
            - "every_run"：wall/failed 命中都重跑（成功 REPLACE wall 行、
              run_count+1）——扫描类/orchestrator 任务；
            - "on_input_change"：wall 命中比对输入指纹，failed 命中重跑。
            策略只作用于「wall/failed 拦截点」；queue/in-flight 永远算数
            （同一轮内不重复派发/并发双跑）。随 job_dict 持久化。
        runtime: 运行期边带状态（dict 或 JobRuntimeState，由框架管理）。
        attempt_no: 本次激活内尝试序号（实例位，框架管理，1 起始含首次
            执行）。
        activation_no: 激活代（实例位，框架管理，每次从 wall/failed
            拦截点放行重跑时 +1，初激活为 1）。
        first_enqueued_at: 首次入队时间（实例位，UTC ISO，由 enqueue
            填充，重试不刷新；None 表示尚未入队）。
    """

    def __init__(
        self,
        task_type: str,
        job_id: str,
        payload: dict[str, Any] | None = None,
        resources: dict[str, float] | None = None,
        max_retries: int = 3,
        depends_on: list[str] | None = None,
        timeout: int | float = 3600,
        *,
        timeout_is_transient: bool = False,
        rerun: str | None = None,
        runtime: dict[str, Any] | None = None,
        attempt_no: int = 1,
        activation_no: int = 1,
        first_enqueued_at: str | None = None,
    ):
        # 身份校验链：类型 → 空值 → '::' 分隔符 → IPC 文件名长度上限
        validate_task_type(task_type)
        self.task_type = task_type
        _validate_job_id(job_id)
        self.job_id = job_id
        _validate_uid_ipc_filename(task_type, job_id)

        self.payload = _normalize_payload(payload)
        self.resources = _normalize_resources(resources)
        validate_retry_budget(max_retries)
        self.max_retries = max_retries
        self.depends_on = _normalize_depends_on(depends_on)
        validate_timeout(timeout)
        self.timeout = timeout
        validate_timeout_flag(timeout_is_transient)
        self.timeout_is_transient = timeout_is_transient
        _validate_rerun(rerun)
        self.rerun = rerun

        # 实例位：框架运行期管理的激活/尝试账目与首次入队时间
        _validate_attempt_no(attempt_no)
        self.attempt_no = attempt_no
        _validate_activation_no(activation_no)
        self.activation_no = activation_no
        _validate_first_enqueued_at(first_enqueued_at)
        self.first_enqueued_at = first_enqueued_at
        self.runtime = runtime

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

    def to_dict(self) -> dict:
        """序列化 job 为字典（payload/resources/depends_on 浅拷贝，防外部
        就地变异内部状态）；``runtime`` 子 dict 显式保留——边带状态集中
        序列化，不散装丢失。"""
        return {
            "task_type": self.task_type,
            "job_id": self.job_id,
            "payload": dict(self.payload),
            "resources": dict(self.resources),
            "max_retries": self.max_retries,
            "depends_on": list(self.depends_on),
            "timeout": self.timeout,
            "timeout_is_transient": self.timeout_is_transient,
            "rerun": self.rerun,
            "runtime": self.runtime.to_dict(),
            "attempt_no": self.attempt_no,
            "activation_no": self.activation_no,
            "first_enqueued_at": self.first_enqueued_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Job:
        """反序列化 job（缺键回默认值；rerun 缺键/None = 未指定哨兵）。"""
        return cls(
            data["task_type"],
            data["job_id"],
            data.get("payload", {}),
            data.get("resources", {}),
            data.get("max_retries", 3),
            data.get("depends_on", []),
            data.get("timeout", 3600),
            timeout_is_transient=data.get("timeout_is_transient", False),
            rerun=data.get("rerun"),
            # runtime 子 dict 显式保留——边带状态只存这一处
            runtime=data.get("runtime", {}),
            attempt_no=data.get("attempt_no", 1),
            activation_no=data.get("activation_no", 1),
            first_enqueued_at=data.get("first_enqueued_at"),
        )

    @property
    def uid(self) -> str:
        """唯一标识：task_type::job_id。"""
        return f"{self.task_type}::{self.job_id}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Job):
            return NotImplemented
        return self.uid == other.uid

    def __hash__(self) -> int:
        return hash(self.uid)


__all__ = [
    "Job",
    "JobRuntimeState",
    "RERUN_EXEMPT_VALUES",
    "RERUN_VALUES",
    "RT_COMMIT_FAILURES",
    "RT_DISPATCH_FAILURES",
    "RT_LAST_RETRY_ERROR",
    "WORKER_RESOURCE",
    "inject_worker_resource",
]
