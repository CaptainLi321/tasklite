"""当前持久层内部 Interface 与共享入口门卫。

队列状态、追加式 Attempt、终态档案、派生 Job 和游标由一次结算原子提交。
successes 与 failed 按 UID 互斥；Attempt 仅作严格 append-only 执行轨迹，
编号独立于任何状态修整操作且永不可删。
"""
from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Sequence


QUEUE_EXECUTION_ID_KEY = "__tasklite_execution_id__"


@dataclass(frozen=True)
class RunningAttemptRecovery:
    """遗留 running 执行的待补写失败 Attempt 载荷。"""

    job_uid: str
    attempt_no: int
    execution_id: str
    run_id: str
    started_at: str | None
    settled_at: str
    job_snapshot: dict[str, Any]
    files: dict[str, Any] | None
    result: dict[str, Any]
    error_message: str | None


@dataclass(frozen=True)
class GraphEditCommit:
    """图修整事务声明式后端提交载荷（O(Δ) 仅含触碰 uid）。

    - ``queue_writes``: ``uid -> job_snapshot | None``（``None`` 删除；非 ``None`` 写入为 ``pending`` 并排入队尾）
    - ``success_writes``: ``uid -> metadata | None``（``None`` 删除；非 ``None`` 写入 successes）
    - ``failed_writes``: ``uid -> (metadata, payload) | None``（``None`` 删除；非 ``None`` 写入 failed）
    - ``recovery_attempts``: 待补写的遗留 running 失败 Attempt 序列
    """

    queue_writes: Mapping[str, dict[str, Any] | None] = field(default_factory=dict)
    success_writes: Mapping[str, dict[str, Any] | None] = field(default_factory=dict)
    failed_writes: Mapping[str, tuple[dict[str, Any], dict[str, Any] | None] | None] = field(
        default_factory=dict
    )
    recovery_attempts: tuple[RunningAttemptRecovery, ...] = ()


def queue_snapshot_with_execution_id(
    job_snapshot: Mapping[str, Any], execution_id: str
) -> dict[str, Any]:
    """返回带内部执行代号的队列快照。"""
    stored = dict(job_snapshot)
    stored[QUEUE_EXECUTION_ID_KEY] = execution_id
    return stored


def queue_snapshot_execution_id(job_snapshot: Mapping[str, Any]) -> str | None:
    """读取队列快照内部执行代号；缺失或非法值返回 None。"""
    value = job_snapshot.get(QUEUE_EXECUTION_ID_KEY)
    return value if isinstance(value, str) and value else None


def public_queue_snapshot(job_snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """返回剥离内部执行代号的业务快照副本。"""
    public = copy.deepcopy(dict(job_snapshot))
    public.pop(QUEUE_EXECUTION_ID_KEY, None)
    return public


def execution_id_matches_run(execution_id: str, run_id: str) -> bool:
    """执行代号必须由本 Run 与正派发序号组成。"""
    prefix, separator, dispatch_no = execution_id.rpartition(".")
    return (
        bool(separator)
        and prefix == run_id
        and dispatch_no.isdigit()
        and int(dispatch_no) > 0
    )


def validate_queue_replacement(job_snapshots: Any) -> None:
    """整表替换集统一形状校验（save_queue / replace_queue_atomic 共用）。

    契约：替换集必须是 job dict 的 list/tuple（与 load_queue 行形态一致）；
    None（compute 漏写 return 的笔误形态）或非序列、元素非 dict 均属契约
    违约，fail-loud 抛 TypeError。
    不变式：双腿必须在任何写变（DELETE / 赋值）之前调用本校验——None 若
    混过校验，SQLite 腿「DELETE 全表成功 + INSERT 全跳过」会静默清空整条
    队列且事务正常提交，与 Memory 腿抛 TypeError 且队列原状形成行为分叉。
    """
    if not isinstance(job_snapshots, (list, tuple)):
        raise TypeError(
            f"queue replacement must be a list of job dicts, "
            f"got {type(job_snapshots).__name__}"
        )
    for i, job_snapshot in enumerate(job_snapshots):
        if not isinstance(job_snapshot, dict):
            raise TypeError(
                f"queue replacement item #{i} must be a job dict, "
                f"got {type(job_snapshot).__name__}"
            )


class AbstractStateBackend(ABC):
    """SQLite 与 Memory 共用的内部持久化 Interface。

    当前执行只经 commit_job_settlement 追加结算；执行前失败、批量失败与
    候选移除各自原子提交。写入故障原样传播，调用方提交成功后才发布内存。
    整表读改写必须使用 replace_queue_atomic；历史清理不触碰编号进度。
    """

    _exec_lock: Any = None

    # 全量加载 -------------------------------------------------------

    @abstractmethod
    def load_successes(self) -> dict[str, dict[str, Any]]:
        """加载全部成功终态（uid → meta）。"""

    @abstractmethod
    def load_failed(self) -> dict[str, dict[str, Any]]:
        """加载全部失败档案终态（uid → 失败 meta）。"""

    @abstractmethod
    def load_failure_payloads(self) -> dict[str, dict[str, Any]]:
        """读取失败档案各 uid 的原始业务 payload 快照（无快照的 uid 不出现）。"""

    @abstractmethod
    def load_cursors(self) -> dict[str, str]:
        """加载全部游标（key → value）。"""

    @abstractmethod
    def load_queue(self) -> list[dict[str, Any]]:
        """按 queue_position 升序加载磁盘队列真相。"""

    @abstractmethod
    def load_known_job_snapshot(self, uid: str) -> dict[str, Any] | None:
        """读取单个已知 Job 的完整配置快照。

        优先级固定为队列（含 running）、successes、failed、Attempt 历史。
        找不到时返回 None；读取或解码故障必须原样传播。
        """

    @abstractmethod
    def load_known_job_snapshots(self) -> dict[str, dict[str, Any]]:
        """读取全部已知 Job 的完整配置快照映射。

        同一 UID 按队列（含 running）、successes、failed、Attempt 历史的
        优先级选取。读取或解码故障必须原样传播。
        """

    # delta 提交 -----------------------------------------------------


    @abstractmethod
    def commit_job_failure(
        self,
        job_uid: str,
        failure_metadata: dict,
        payload: dict[str, Any] | None = None,
    ) -> bool:
        """原子持久化一个失败 job 的 delta（写入失败档案）。

        单事务效果：failed INSERT/REPLACE uid → failure_metadata；
        queue DELETE uid；successes DELETE 同名行——重跑任务重试耗尽时旧成功
        记录作废（最终状态唯一），否则磁盘 successes∩failed 并存破坏互斥。
        ``payload`` 是原始业务输入快照（补跑自足，无需外部反查）；
        None 表示本路径无可存快照，保留既有快照不覆盖。
        成功返回 True；写入故障原样传播，失败时后端保持调用前状态。
        """


    @abstractmethod
    def commit_job_failures(self, job_uids_metadata: list[tuple[str, dict]]) -> None:
        """原子批量写失败档案（死锁/级联场景）的 delta。

        单事务效果：failed INSERT/REPLACE 每个 uid → metadata；
        queue 按 uid 精准 DELETE（剩余条目保持原顺序）；
        successes 批量 DELETE 同名行（与 commit_job_failure 对称的最终状态唯一）。
        任一行失败 ⇒ 整体回滚并原样抛出异常，绝不留下「队列已删、档案未落」
        的半成品失败态。
        """

    @abstractmethod
    def commit_skip(self, job_uid: str) -> None:
        """移除 Trigger False 或已终态重复候选的队列行。

        不写 successes/failed，不生成 Attempt。
        成功无返回值；失败时队列不变，原始异常继续传播。
        """


    # 增量入队 -------------------------------------------------------

    @abstractmethod
    def enqueue_jobs(self, job_snapshots: list[dict[str, Any]], *, front: bool = False) -> list[str]:
        """批量增量入队：按 front 在队列头/尾插入 job_snapshots（保序），单事务原子。

        跳过与现有队列重复的 uid（含批次内重复，首个存储），返回实际插入
        的 uid 列表。与 commit_* 的 delta 语义对齐：不做全表重写——运行中
        run() 的 delta commit 与该入队并发时，不会因全量 save_queue 覆盖而
        丢失 run() 已提交的变更（写入侧不覆盖，读-改-写窗口仍由
        ``replace_queue_atomic`` 收敛）。失败时事务回滚并抛异常（不吞）。
        """

    # 管理写、一致快照与游标 -----------------------------------------

    @abstractmethod
    def load_consistent_snapshot(self) -> dict[str, Any]:
        """在单一读事务或单锁内原子捕获一致状态快照（含 queue_jobs/successes/failed/failure_payloads/attempts/job_sequences）。"""

    @abstractmethod
    def commit_graph_edit(self, commit: GraphEditCommit) -> None:
        """在单一写事务或锁内原子应用声明式图修整变更，绝不删除 attempts 或重置 job_sequences。"""

    @abstractmethod
    def get_cursor(self, key: str) -> str | None:
        """查询单个游标的最新已提交值，不存在返回 None。"""

    @abstractmethod
    def set_cursor(self, key: str, value: str | None) -> None:
        """设置或删除单个游标（None 为删除）。"""

    # 尝试轨迹（append-only 旁路观测面）--------------------------------


    @abstractmethod
    def load_latest_succeeded_finished_at(self, job_uid: str) -> str | None:
        """读取指定 Job 最新一条 success Attempt 的 settled_at；无则返回 None。"""

    # 元数据 ---------------------------------------------------------

    @abstractmethod
    def get_engine_metadata(self, key: str) -> str | None:
        """读取一条框架级元数据（如 fencing 的 last_run_id）。无则返回 None。"""

    @abstractmethod
    def set_engine_metadata(self, key: str, value: str) -> None:
        """写入一条框架级元数据（UPSERT 语义）。失败抛异常（不吞）。"""

    # 状态事务与编号 -------------------------------------------------

    @abstractmethod
    def mark_job_running(
        self, job_uid: str, *, execution_id: str, run_id: str, started_at: str
    ) -> int:
        """原子将 pending job 转为 running，持久化完整执行身份并递增 attempt_no。"""

    @abstractmethod
    def commit_job_settlement(
        self,
        *,
        job_uid: str,
        outcome: str,
        attempt_no: int,
        execution_id: str,
        run_id: str,
        settled_at: str,
        started_at: str | None = None,
        job_snapshot: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        error_message: str | None = None,
        success_metadata: dict[str, Any] | None = None,
        spawned_job_snapshots: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
        failure_metadata: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        requeued_job_snapshot: dict[str, Any] | None = None,
        record_attempt: bool = True,
    ) -> None:
        """原子执行结算：校验完整执行身份后写入 Attempt、Job 去向、档案与派生数据。

        成功无返回值；执行身份与 running 不匹配或已结算时抛 TaskLiteError。
        写入故障原样传播，失败时后端不变。
        """

    @abstractmethod
    def get_attempt_sequence(self, job_uid: str) -> int:
        """读取指定 job_uid 的最新 attempt_no 编号进度（未派发过返回 0）。"""

    @abstractmethod
    def load_settled_attempts(self, job_uid: str) -> list[dict[str, Any]]:
        """按 attempt_no 升序读取指定 job_uid 的全部已结算 Attempt 记录。"""

    @abstractmethod
    def load_queue_jobs(self, *, status: str | None = None) -> list[dict[str, Any]]:
        """按 queue_position 升序加载队列行的完整信息，可选按 status 过滤。"""

    @abstractmethod
    def grant_authorization(
        self,
        path: str,
        target_type: Literal["file", "directory"],
        *,
        authorized_at: str | None = None,
    ) -> dict[str, Any]:
        """原子校验冲突并新增持久化路径授权（重复或父子类型冲突抛 TaskLiteError）。"""

    @abstractmethod
    def record_authorization(
        self,
        path: str,
        target_type: str,
        *,
        authorized_at: str | None = None,
    ) -> None:
        """持久化记录一条文件系统路径授权（target_type 为 'file' 或 'directory'）。"""

    @abstractmethod
    def list_authorizations(self) -> list[dict[str, Any]]:
        """查询全部已持久化的授权记录。"""

    @abstractmethod
    def revoke_authorization(self, path: str) -> bool:
        """按规范绝对路径精确撤销授权，存在并删除返回 True，不存在返回 False。"""

    @abstractmethod
    def is_path_authorized(self, path: str, target_type: str) -> bool:
        """按实际目标类型检查规范绝对路径授权；文件精确匹配，目录覆盖整树。"""

    def close(self) -> None:
        """关闭后端并释放资源（幂等）。"""


__all__ = [
    "AbstractStateBackend",
    "GraphEditCommit",
    "RunningAttemptRecovery",
    "validate_queue_replacement",
]
