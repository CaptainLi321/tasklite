"""当前持久层内部 Interface 与共享入口门卫。

队列状态、追加式 Attempt、终态档案、派生 Job 和游标由一次结算原子提交。
successes 与 failed 按 UID 互斥；Attempt 仅作执行轨迹，编号独立于可清理历史。
"""
from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from typing import Any, Callable, Mapping, Sequence


QUEUE_EXECUTION_ID_KEY = "__tasklite_execution_id__"


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

    # 整表重写 -------------------------------------------------------

    @abstractmethod
    def save_queue(self, job_snapshots: list[dict[str, Any]]) -> None:
        """整表重写队列（测试装配 / replace_queue_atomic 的事务内步骤）。

        红线：这是「无读基准的整表覆盖」——跨进程并发的 enqueue/commit
        若提交在本方法执行前的任意时刻，其已落盘行会被无条件抹除。进程内
        任何「先读磁盘真相再决定写什么」的合并保存（崩溃恢复路径）严禁
        直接调用本方法，必须走 ``replace_queue_atomic`` 把读-改-写收敛进
        单个写事务。

        替换集经 ``validate_queue_replacement`` 在写变前校验：None / 非序列 /
        元素非 dict 抛 TypeError，队列保持调用前状态（双腿一致）。
        """

    @abstractmethod
    def replace_queue_atomic(
        self,
        compute: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    ) -> None:
        """读-改-写收敛的整表替换：磁盘真相读取与写回在单个写事务内完成。

        ``compute`` 在写锁内接收按序排列的磁盘队列快照，返回替换后的完整
        队列。不变式：并发方（跨进程 enqueue / delta commit）要么先于本
        事务提交（其行进入磁盘真相、参与合并，绝不丢失），要么排队等本
        事务提交后再落盘——「读真相 → 计算 → 写回」之间不存在无锁窗口，
        陈旧快照永远无法覆盖窗口期他进程已应答的新写入。

        ``compute`` 必须是纯计算（锁内执行，不得调用任何后端写方法，否则
        跨连接写请求会以写锁互斥死等）。单事务原子：``compute`` 抛异常或
        写失败时整体回滚，磁盘保持调用前状态，异常向外传播。``compute``
        返回的替换集经 ``validate_queue_replacement`` 在写变前校验（None /
        非序列 / 元素非 dict 抛 TypeError，双腿一致，磁盘保持原状）。
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

    # 定向删除与种子 --------------------------------------------------

    @abstractmethod
    def delete_queue_uids(self, job_uids: list[str]) -> int:
        """按 uid 定向批量删除队列行（加载期 repair 的差量落盘唯一出口）。

        与 save_queue 的全表重写相对：只 DELETE 指定 uid 的行，其余行原样
        保留。不变式：删除集在调用前确定，不在删除集内的行（含并发进程
        刚入队的新行）无论与本事务先后提交都必然存活——陈旧加载快照永远
        不会覆盖他进程的新写入。单事务原子；失败抛异常（不吞），磁盘保持
        调用前状态（残留行由下次加载重新判定，天然幂等）。返回实际删除行数。
        """

    @abstractmethod
    def delete_failed(self, job_uids: list[str]) -> int:
        """从失败档案批量删除指定 uid（clear_failures 的后端实现）。

        返回实际删除行数（连带清除 payload 快照）。不触碰 queue/successes。
        仅限 run() 之外调用（改变 is_known 判定基础，与 enqueue 同纪律）。
        """

    @abstractmethod
    def delete_successes(self, job_uids: list[str]) -> int:
        """从 successes 批量删除指定 uid（clear_history 的后端实现 + commit 路径事务内清理）。

        两种调用上下文：
        - 运行外管理 API 的批量删除；
        - commit_job_failure / commit_job_failures 的同一事务内逐 uid 删除
          （重跑任务重跑失败时 successes 旧记录作废，最终状态唯一）。

        运行中的调用仅限 commit 事务内部（随失败档案写入原子提交，不改变
        进行中的派发判定）；独立调用仅限 run 之外（改变 is_known 判定
        基础，与 enqueue 同纪律）。
        """

    @abstractmethod
    def apply_manual_status(
        self,
        updates: Sequence[tuple[str, str, dict[str, Any], dict[str, Any] | None]],
    ) -> None:
        """批量原子应用人工状态变更 (job_uid, status, snapshot, metadata)。"""

    @abstractmethod
    def clear_history(self, uid: str, records: set[str]) -> int:
        """从 successes/failed/attempts 清除指定 job_uid 的记录，返回删除条数。"""

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
    def clear_attempts(self, job_uid: str) -> int:
        """删除指定 job_uid 的 Attempt 历史（不影响 job_sequences 编号进度）。"""

    @abstractmethod
    def load_queue_jobs(self, *, status: str | None = None) -> list[dict[str, Any]]:
        """按 queue_position 升序加载队列行的完整信息，可选按 status 过滤。"""

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
    "validate_queue_replacement",
]
