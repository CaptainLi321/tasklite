"""Job data model for tasklite."""

from dataclasses import dataclass
import math
from typing import Any, Dict, List, Optional

from ..utils.lockfile import safe_uid_filename
from ..taxonomy import validate_resource_amounts

# uid 派生的 IPC 文件名（锁 / result / signals / outputs）
# 无截断——job_id/task_type 超长时文件名超 255 字节（EXT4 单文件名字节
# 上限）→ os.open ENAMETOOLONG → 派发侧异常未分类 → requeue + 退避/崩溃
# 重启 → livelock（job 永远无法执行且不进 DLQ）。入口按 safe_uid_filename
# 实际字节数（含转义膨胀）拒绝，fail-fast。
# 255 - 56 = 199：56 为最坏结果文件后缀 ``.{32-hex run_id}.{seq}.result.json``。
_MAX_SAFE_UID_BYTES = 199


@dataclass
class JobRuntimeState:
    """Job 运行期内部边带状态（强类型结构化存储，替代散装字典）。"""

    backoff_until: Optional[float] = None
    backoff_wall_deadline: Optional[float] = None
    commit_failures: int = 0
    dispatch_failures: int = 0
    last_retry_error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        if self.backoff_until is not None:
            d["_backoff_until"] = self.backoff_until
        if self.backoff_wall_deadline is not None:
            d["_backoff_wall_deadline"] = self.backoff_wall_deadline
        if self.commit_failures:
            d["_commit_failures"] = self.commit_failures
        if self.dispatch_failures:
            d["_dispatch_failures"] = self.dispatch_failures
        if self.last_retry_error:
            d["_last_retry_error"] = self.last_retry_error
        return d

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "JobRuntimeState":
        if not isinstance(data, dict):
            return cls()
        return cls(
            backoff_until=data.get("_backoff_until") or data.get("backoff_until"),
            backoff_wall_deadline=data.get("_backoff_wall_deadline") or data.get("backoff_wall_deadline"),
            commit_failures=int(data.get("_commit_failures") or data.get("commit_failures") or 0),
            dispatch_failures=int(data.get("_dispatch_failures") or data.get("dispatch_failures") or 0),
            last_retry_error=str(data.get("_last_retry_error") or data.get("last_retry_error") or ""),
        )


class Job:
    """Represents a discrete unit of work in the pipeline."""

    def __init__(
        self,
        task_type: str,
        job_id: str,
        payload: Optional[Dict[str, Any]] = None,
        resources: Optional[Dict[str, float]] = None,
        retries: int = 0,
        max_retries: int = 3,
        depends_on: Optional[List[str]] = None,
        timeout: int = 3600,
        backoff_base: float = 2.0,
        backoff_max: float = 300.0,
        timeout_is_transient: bool = False,
        rerun: Optional[str] = None,
        runtime: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a Job.

        Args:
            task_type: Task type name (must match a registered handler). Must not
                contain ``::`` (used as uid separator).
            job_id: Unique job identifier within the task type. Must not contain ``::``.
            payload: Business data dict passed to handler. Must be JSON-serializable.
            resources: Resource amounts to acquire (overrides handler defaults).
            retries: Current retry count (internal, managed by framework).
            max_retries: Max retry attempts before DLQ. Must be >= 0.
            depends_on: List of upstream job UIDs (``task_type::job_id``). Failed
                parents cascade-block this job.
            timeout: Execution timeout in seconds. Must be > 0.
            backoff_base: Exponential backoff base in seconds. Must be finite and >= 0.
            backoff_max: Backoff cap in seconds. Must be finite and >= 0.
            timeout_is_transient: 超时是否视为瞬态失败（默认 False 保持
                保守语义）。True 时看门狗 TIMEOUT 按 retry 处理（指数退避重试，
                达 max_retries 才 DLQ），而非直接 Unknown → DLQ。适用于设计上
                可能长跑的 job 类型（如 discovery 的整页扫描）——「超时」的
                语义是「没跑完」，不等于「确定性失败」。
            rerun: 跨会话重跑策略——「成功进 wall 是否算
                数」由它决定（**不改变 job_id 派生**，身份仍是 uid）：
                - None（默认，未指定哨兵）：**未指定**——discovery
                  任务注入 ``set_discovery_rerun`` 登记的默认策略；普通任务
                  按 "never" 处理。与显式 "never" 的区别：显式值一律尊重，
                  不再被 discovery 默认覆盖（保持 enqueue 与 spawn 行为一致）。
                - "never"：wall/failed 命中即跳过（现状语义）；
                - "on_failure"：wall 命中跳过，failed 命中**重跑**
                  （网络误失败可自愈；成功即自动清 DLQ 残行）；
                - "every_run"：wall/failed 命中都**重跑**（成功 REPLACE
                  wall 行、run_count+1）——discovery 扫描/orchestrator；
                - "on_input_change"：wall 命中比对输入指纹，failed 命中重跑。
                策略只作用于「wall/failed 拦截点」；queue/in-flight 永远
                算数（同一轮内不重复派发/并发双跑）。随 job_dict 持久化。
        """
        self.task_type = task_type
        # 非 str task_type：下方 "::" in task_type 会抛原始 TypeError，且与 job_id 的
        # str 归一化不对称，入口显式拒绝。
        if not isinstance(task_type, str):
            raise TypeError(
                f"task_type must be a str, got {type(task_type).__name__} ({task_type!r})"
            )
        # job_id 拒绝非 str——str 强转会使数值与字符串
        # 字面量静默碰撞（Job("t",1.0).uid == Job("t","1.0").uid == "t::1.0"），
        # 混合数值/字符串 job_id 的管线静默合并不同任务（wall 去重吞掉后者）。
        # 入口显式拒绝，与 task_type 对称。
        if not isinstance(job_id, str):
            raise TypeError(
                f"job_id must be a str, got {type(job_id).__name__} ({job_id!r})"
            )
        self.job_id = job_id
        # 校验 task_type/job_id 非空且不含 '::'，避免 uid 的 'task::id' 分隔符碰撞
        if not task_type:
            raise ValueError(f"task_type must be a non-empty str, got {task_type!r}")
        if not self.job_id:
            raise ValueError(f"job_id must be a non-empty str, got {job_id!r}")
        if "::" in task_type:
            raise ValueError(f"task_type must not contain '::', got {task_type!r}")
        if "::" in self.job_id:
            raise ValueError(f"job_id must not contain '::', got {job_id!r}")
        # uid 派生 IPC 文件名超 255 字节 → ENAMETOOLONG livelock，入口拒绝。
        if len(safe_uid_filename(f"{task_type}::{job_id}").encode("utf-8")) > _MAX_SAFE_UID_BYTES:
            raise ValueError(
                f"task_type+job_id too long: uid-derived IPC filename would exceed "
                f"filesystem name limit ({_MAX_SAFE_UID_BYTES} bytes max); got "
                f"{task_type!r}::{job_id!r} — shorten to avoid ENAMETOOLONG livelock"
            )
        # 非 dict payload：to_dict/enqueue 预检均放行、直到子进程才崩，入口拒绝。
        if payload is not None and not isinstance(payload, dict):
            raise TypeError(
                f"payload must be a dict or None, got {type(payload).__name__} ({payload!r})"
            )
        self.payload = dict(payload) if payload else {}
        # resources 必须为 dict——str 等可迭代类型在下方
        # ``for res_name, amount in self.resources.items`` 抛原始
        # AttributeError（不可读），None 被 or {} 兜底但其他类型漏过。
        # 与 payload 的 dict 校验对称，入口显式拒绝。
        if resources is not None and not isinstance(resources, dict):
            raise TypeError(
                f"resources must be a dict or None, got {type(resources).__name__} ({resources!r})"
            )
        self.resources = dict(resources) if resources else {}
        # 数值校验单点化（与 pipeline.register_handler 的默认资源
        # 校验共用 taxonomy.validate_resource_amounts）——负值/NaN/Inf
        # 会在调度器 acquire 时抛 ValueError（若抛在 try 之外，部分资源
        # 永久泄漏），且 NaN 会毒化 CapacityResource 的 used 账目导致 livelock。
        # 构造时即拒绝（bool 是 int 子类，isinstance(True,(int,float)) 为
        # True 会放行，故显式拒绝 bool）。
        validate_resource_amounts(self.resources, "resources")
        self.retries = retries
        # retries 必须为 int——字符串等脏数据会在 _complete_job 的
        # `job.retries >= job.max_retries` 比较处抛 TypeError，导致无限
        # 崩溃重启循环（job 从磁盘恢复 → 再跑再崩，且无 DLQ 兜底）。
        if not isinstance(retries, int) or isinstance(retries, bool):
            raise TypeError(f"retries must be an int, got {type(retries).__name__} ({retries!r})")
        # retries 下界校验：防止负数导致退避比较异常与无延迟重试，入口即拒绝。
        if retries < 0:
            raise ValueError(f"retries must be >= 0, got {retries}")
        # max_retries 的 isinstance 检查必须在 < 0 之前——字符串脏数据
        # 会先触发 "abc" < 0 的原始 TypeError，友好报错失效。
        if not isinstance(max_retries, int) or isinstance(max_retries, bool):
            raise TypeError(
                f"max_retries must be an int, got {type(max_retries).__name__} ({max_retries!r})"
            )
        if max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {max_retries}")
        self.max_retries = max_retries
        # 先校验外层类型再迭代——str 可迭代出 str，若直接 all(isinstance) 检查
        # 会把 "parent::id" 静默肢解为字符列表，作业以 DEPENDENCY_DEADLOCK 死亡。
        if depends_on is not None and not isinstance(depends_on, (list, tuple)):
            raise TypeError(
                f"depends_on must be a list of strings, got {type(depends_on).__name__}"
            )
        self.depends_on = list(depends_on) if depends_on is not None else []
        if not all(isinstance(d, str) for d in self.depends_on):
            raise TypeError(
                f"depends_on must contain only strings, got {self.depends_on!r}"
            )
        self.timeout = timeout
        # timeout 必须为有限正数——NaN 使 timeout <= 0 校验恒
        # False 而漏过（NaN <= 0 为 False），deadline = monotonic + NaN = NaN，
        # now > NaN 恒 False → 看门狗永不触发，挂死 job 永不 kill。
        # bool 是 int 子类，同样拒绝（timeout=True 被接受为 1 秒）。
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            raise TypeError(
                f"timeout must be a number, got {type(timeout).__name__} ({timeout!r})"
            )
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError(f"timeout must be finite and > 0, got {timeout}")
        # backoff_base/backoff_max 与 timeout 的校验对称——
        # 仅 math.isfinite + < 0 不够：字符串 "2.0" 抛原始 TypeError
        # （math.isfinite 不接收 str），True 被接受为 1.0（bool 是 int 子类）。
        if not isinstance(backoff_base, (int, float)) or isinstance(backoff_base, bool):
            raise TypeError(
                f"backoff_base must be a number, got {type(backoff_base).__name__} ({backoff_base!r})"
            )
        if not math.isfinite(backoff_base) or backoff_base < 0:
            raise ValueError(
                f"backoff_base must be finite and non-negative, got {backoff_base}"
            )
        if not isinstance(backoff_max, (int, float)) or isinstance(backoff_max, bool):
            raise TypeError(
                f"backoff_max must be a number, got {type(backoff_max).__name__} ({backoff_max!r})"
            )
        if not math.isfinite(backoff_max) or backoff_max < 0:
            raise ValueError(
                f"backoff_max must be finite and non-negative, got {backoff_max}"
            )
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        # 超时归类可配置。bool 类型校验——非 bool 值（如字符串）在
        # executor 的 ``handle.job.timeout_is_transient`` 判断时被当作真值，
        # 静默改变归类语义，入口拒绝。
        if not isinstance(timeout_is_transient, bool):
            raise TypeError(
                f"timeout_is_transient must be a bool, got "
                f"{type(timeout_is_transient).__name__} ({timeout_is_transient!r})"
            )
        self.timeout_is_transient = timeout_is_transient
        # rerun 策略入口校验（fail-loud）——非法值会被静默当 "never" 处理。
        # None 是「未指定」哨兵（可被 discovery 默认注入），
        # 显式字符串一律尊重；仅拒绝未知字符串值。
        if rerun is not None and rerun not in (
            "never", "on_failure", "every_run", "on_input_change",
        ):
            raise ValueError(
                f"rerun must be None (unspecified) or one of "
                f"'never'/'on_failure'/'every_run'/'on_input_change', got {rerun!r}"
            )
        self.rerun = rerun
        # 运行时边带状态（退避截止/3-strike 计数/最近重试错误）收敛到
        # `runtime` 单一命名空间，随 job_dict 落盘持久化——序列化只此
        # 一处，新增状态不会因散装下划线键漏写 to_dict 而丢失。
        self.runtime = dict(runtime) if runtime else {}

    def to_dict(self) -> dict:
        """Serialize job to dictionary.

        Returns shallow copies of payload/resources/depends_on to prevent
        external mutation of internal state. ``runtime`` 子 dict 显式保留
        ——边带状态集中序列化，不散装丢失。
        """
        return {
            "task_type": self.task_type,
            "job_id": self.job_id,
            "payload": dict(self.payload),
            "resources": dict(self.resources),
            "retries": self.retries,
            "max_retries": self.max_retries,
            "depends_on": list(self.depends_on),
            "timeout": self.timeout,
            "backoff_base": self.backoff_base,
            "backoff_max": self.backoff_max,
            "timeout_is_transient": self.timeout_is_transient,
            "rerun": self.rerun,
            "runtime": dict(self.runtime),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        """Deserialize job from dictionary."""
        return cls(
            data["task_type"],
            data["job_id"],
            data.get("payload", {}),
            data.get("resources", {}),
            data.get("retries", 0),
            data.get("max_retries", 3),
            data.get("depends_on", []),
            data.get("timeout", 3600),
            data.get("backoff_base", 2.0),
            data.get("backoff_max", 300.0),
            data.get("timeout_is_transient", False),
            data.get("rerun"), # 缺键/None = 未指定哨兵
            # runtime 子 dict 显式保留——边带状态只存这一处。
            data.get("runtime", {}),
        )

    @property
    def uid(self) -> str:
        """Return unique identifier for this job."""
        return f"{self.task_type}::{self.job_id}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Job):
            return NotImplemented
        return self.uid == other.uid

    def __hash__(self) -> int:
        return hash(self.uid)
