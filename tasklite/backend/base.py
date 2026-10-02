"""v2 持久层抽象契约：AbstractStateBackend 与共享入口门卫。

backend 只被单线程父进程调用；子进程 worker 不触碰持久层。契约按
delta 语义组织：队列行的「弹出」从不单独落盘——被弹出的 uid 保留在
磁盘队列中，直到 commit_job_success / commit_job_failure /
commit_retry / commit_skip 的事务内才删除（at-least-once 崩溃契约）。

失败档案（failed）术语：按 uid 唯一终态，wall/failed 全局互斥——
成功 commit 删 failed 同名行、失败 commit 删 wall 行，「最终状态唯一」
由此保证；失败历史与执行时间线由 attempts 旁路轨迹承接，本表不注入
计数字段。attempts 为 append-only 旁路观测面：不参与 wall/failed 的
互斥与 REPLACE 语义。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Mapping, Sequence

from ..models.attempt import ATTEMPT_OUTCOMES, AttemptRecord


def validate_queue_replacement(jobs: Any) -> None:
    """整表替换集统一形状校验（save_queue / replace_queue_atomic 共用）。

    契约：替换集必须是 job dict 的 list/tuple（与 load_queue 行形态一致）；
    None（compute 漏写 return 的笔误形态）或非序列、元素非 dict 均属契约
    违约，fail-loud 抛 TypeError。
    不变式：双腿必须在任何写变（DELETE / 赋值）之前调用本校验——None 若
    混过校验，SQLite 腿「DELETE 全表成功 + INSERT 全跳过」会静默清空整条
    队列且事务正常提交，与 Memory 腿抛 TypeError 且队列原状形成行为分叉。
    """
    if not isinstance(jobs, (list, tuple)):
        raise TypeError(
            f"queue replacement must be a list of job dicts, "
            f"got {type(jobs).__name__}"
        )
    for i, job in enumerate(jobs):
        if not isinstance(job, dict):
            raise TypeError(
                f"queue replacement item #{i} must be a job dict, "
                f"got {type(job).__name__}"
            )


def validate_attempt_dispatch(record: Any) -> None:
    """尝试轨迹插入门卫：只接受「派发即插行」形态。

    契约（attempts append-only 协议的插入侧）：AttemptRecord 且
    outcome="running"、finished_at 与 error 均为 None——收尾字段由
    ``update_attempt`` 唯一写入；插入侧出现收尾字段说明调用方绕过了
    派发点协议（终态行伪装成派发行，时间线出现空洞），fail-loud 拒绝。
    """
    if not isinstance(record, AttemptRecord):
        raise TypeError(
            f"attempt record must be an AttemptRecord, "
            f"got {type(record).__name__} ({record!r})"
        )
    if record.outcome != "running" or record.finished_at is not None or record.error is not None:
        raise ValueError(
            "dispatch append must be a fresh running row "
            "(outcome='running', finished_at=None, error=None), got "
            f"outcome={record.outcome!r} finished_at={record.finished_at!r} "
            f"error={record.error!r}"
        )


def validate_attempt_finish(outcome: Any, finished_at: Any, error: Any) -> None:
    """尝试轨迹收尾门卫：outcome 属终态词汇、收尾时刻与错误形态合法。

    running 是插入侧专属形态——收尾接口把 running 写回会把已派发行
    伪装成未派发，追溯链的起止时间语义被破坏，入口拒绝（与
    ``AttemptRecord`` 的词汇表校验同源、同口径）。
    """
    if outcome == "running" or outcome not in ATTEMPT_OUTCOMES:
        raise ValueError(
            f"attempt finish outcome must be one of "
            f"{'/'.join(repr(v) for v in ATTEMPT_OUTCOMES if v != 'running')}, "
            f"got {outcome!r}"
        )
    if not isinstance(finished_at, str):
        raise TypeError(
            f"finished_at must be a str, got {type(finished_at).__name__} "
            f"({finished_at!r})"
        )
    if not finished_at:
        raise ValueError("finished_at must be a non-empty str")
    if error is not None and not isinstance(error, str):
        raise TypeError(
            f"error must be a str or None, got {type(error).__name__} ({error!r})"
        )
    if error == "":
        raise ValueError("error must be a non-empty str or None")


class AbstractStateBackend(ABC):
    """管线状态持久化抽象契约（SQLite / Memory 双腿共同口径）。

    语义分组：
    - 全量加载（load_wall / load_failed / load_failed_payloads /
      load_cursors / load_queue）：仅启动装配与测试装配使用；
    - 整表重写（save_queue / replace_queue_atomic）：读-改-写必须经
      ``replace_queue_atomic`` 收敛进单个写事务；
    - delta 提交（commit_job_success / commit_job_failure /
      commit_retry / commit_bulk_failure / commit_skip）：逐 job 单事务
      碎片化，禁止多 job 混合长事务；返回 False 表示持久化失败且后端
      保持调用前状态，调用方不得推进内存镜像并走崩溃契约；
    - 增量入队（enqueue_jobs）：不做全量覆盖，与运行中 delta 提交并发
      安全（写入侧不覆盖）；
    - 定向删除与种子（delete_queue_uids / delete_failed / delete_wall /
      seed_wall / seed_cursor）：加载期 repair 差量落盘与运维接缝；
    - 尝试轨迹（append_attempt / update_attempt / load_attempts）：
      append-only 旁路观测面；
    - 元数据（get_meta / set_meta）：框架级键值（fencing 的 last_run_id 等）。
    """

    # 全量加载 -------------------------------------------------------

    @abstractmethod
    def load_wall(self) -> dict[str, dict[str, Any]]:
        """加载全部成功终态（uid → meta）。"""

    @abstractmethod
    def load_failed(self) -> dict[str, dict[str, Any]]:
        """加载全部失败档案终态（uid → 失败 meta）。"""

    @abstractmethod
    def load_failed_payloads(self) -> dict[str, dict[str, Any]]:
        """读取失败档案各 uid 的原始业务 payload 快照（无快照的 uid 不出现）。"""

    @abstractmethod
    def load_cursors(self) -> dict[str, str]:
        """加载全部游标（key → value）。"""

    @abstractmethod
    def load_queue(self) -> list[dict[str, Any]]:
        """按 seq 升序加载磁盘队列真相。"""

    # 整表重写 -------------------------------------------------------

    @abstractmethod
    def save_queue(self, jobs: list[dict[str, Any]]) -> None:
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
    def commit_job_success(
        self,
        uid: str,
        result_meta: dict,
        *,
        spawned_jobs: Sequence[dict[str, Any]] = (),
        cursor_updates: Mapping[str, str | None] | None = None,
    ) -> bool:
        """原子持久化一个成功 job 的 delta。

        单事务效果：
        - wall：INSERT/REPLACE uid → result_meta；
        - queue：DELETE 被弹出的 uid；
        - queue：spawned_jobs 队头插入（保序）；
        - failed：DELETE 同名残行（成功终态唯一，防 wall∩failed 并存）；
        - cursors：合并 cursor_updates（None 值表示删除游标）。

        返回 True 成功；返回 False 表示任一步持久化失败且后端保持调用前
        状态（被弹出的 uid 仍在磁盘队列），调用方不得更新内存 wall/cursor
        状态，并自行决定是否重入队（崩溃契约）。
        """

    @abstractmethod
    def commit_job_failure(
        self,
        uid: str,
        result_meta: dict,
        job_payload: dict[str, Any] | None = None,
    ) -> bool:
        """原子持久化一个失败 job 的 delta（写入失败档案）。

        单事务效果：failed INSERT/REPLACE uid → result_meta；
        queue DELETE uid；wall DELETE 同名行——重跑任务重试耗尽时旧成功
        记录作废（最终状态唯一），否则磁盘 wall∩failed 并存破坏互斥。
        ``job_payload`` 是原始业务 payload 快照（补跑自足，无需外部反查）；
        None 表示本路径无可存快照，保留既有快照不覆盖。
        返回语义与 ``commit_job_success`` 一致：False ⇒ 后端未变、崩溃契约。
        """

    @abstractmethod
    def commit_retry(
        self,
        popped_uid: str,
        requeued_job: dict[str, Any],
        *,
        front: bool = False,
    ) -> bool:
        """原子持久化一次重入队 delta：DELETE popped_uid + 按 front 插入 requeued_job。

        不写 wall/failed。同 uid 重试是 DELETE+INSERT（一次执行没有独立
        持久身份，身份仍是 uid）。重插不得静默丢弃：requeued uid 撞上删除
        popped 后的既有行冲突时返回 False 走崩溃契约，绝不产出重复队列
        条目却报成功。
        """

    @abstractmethod
    def commit_bulk_failure(self, uids_metas: list[tuple[str, dict]]) -> bool:
        """原子批量写失败档案（死锁/级联场景）的 delta。

        单事务效果：failed INSERT/REPLACE 每个 uid → meta；
        queue 按 uid 精准 DELETE（剩余条目保持原顺序）；
        wall 批量 DELETE 同名行（与 commit_job_failure 对称的最终状态唯一）。
        任一行失败 ⇒ 整体回滚、返回 False，绝不留下「队列已删、档案未落」
        的半成品失败态。
        """

    @abstractmethod
    def commit_skip(self, uid: str) -> bool:
        """delta 删除队列中已终态（重复）的 uid。

        去重命中（uid 已在 wall/failed）时调用——磁盘队列中该 uid 是残留
        条目，直接删除以同步内存/磁盘（内存 = 磁盘 − in-flight）。
        不写 wall/failed（重复条目不改变任何状态，只是清理）。
        返回 True 成功；False 时 on-disk 队列不变，调用方走崩溃契约。
        """

    @abstractmethod
    def append_failed(self, uid: str, payload: dict[str, Any] | None = None) -> None:
        """绕过队列直接写一条失败档案记录（带外登记面）。

        按 uid REPLACE（幂等）；不触碰 queue/wall/cursors。持久化失败
        抛异常（不吞）。
        """

    # 增量入队 -------------------------------------------------------

    @abstractmethod
    def enqueue_jobs(self, jobs: list[dict[str, Any]], *, front: bool = False) -> list[str]:
        """批量增量入队：按 front 在队列头/尾插入 jobs（保序），单事务原子。

        跳过与现有队列重复的 uid（含批次内重复，首个存储），返回实际插入
        的 uid 列表。与 commit_* 的 delta 语义对齐：不做全表重写——运行中
        run() 的 delta commit 与该入队并发时，不会因全量 save_queue 覆盖而
        丢失 run() 已提交的变更（写入侧不覆盖，读-改-写窗口仍由
        ``replace_queue_atomic`` 收敛）。失败时事务回滚并抛异常（不吞）。
        """

    # 定向删除与种子 --------------------------------------------------

    @abstractmethod
    def delete_queue_uids(self, uids: list[str]) -> int:
        """按 uid 定向批量删除队列行（加载期 repair 的差量落盘唯一出口）。

        与 save_queue 的全表重写相对：只 DELETE 指定 uid 的行，其余行原样
        保留。不变式：删除集在调用前确定，不在删除集内的行（含并发进程
        刚入队的新行）无论与本事务先后提交都必然存活——陈旧加载快照永远
        不会覆盖他进程的新写入。单事务原子；失败抛异常（不吞），磁盘保持
        调用前状态（残留行由下次加载重新判定，天然幂等）。返回实际删除行数。
        """

    @abstractmethod
    def delete_failed(self, uids: list[str]) -> int:
        """从失败档案批量删除指定 uid（clear_failures 的后端实现）。

        返回实际删除行数（连带清除 payload 快照）。不触碰 queue/wall。
        仅限 run() 之外调用（改变 is_known 判定基础，与 enqueue 同纪律）。
        """

    @abstractmethod
    def delete_wall(self, uids: list[str]) -> int:
        """从 wall 批量删除指定 uid（clear_history 的后端实现 + commit 路径事务内清理）。

        两种调用上下文：
        - 运行外管理 API 的批量删除；
        - commit_job_failure / commit_bulk_failure 的同一事务内逐 uid 删除
          （重跑任务重跑失败时 wall 旧记录作废，最终状态唯一）。

        运行中的调用仅限 commit 事务内部（随失败档案写入原子提交，不改变
        进行中的派发判定）；独立调用仅限 run 之外（改变 is_known 判定
        基础，与 enqueue 同纪律）。
        """

    @abstractmethod
    def seed_wall(self, uids: list[str]) -> int:
        """把 uid 批量写入 wall（meta 为空 dict）——存档迁移标记「已处理」。

        幂等：已存在的 uid 被覆盖（meta 重置为空）。返回实际写入行数。
        不变式（wall/failed 全局互斥）：已在 failed 的 uid 拒绝种子——
        冲突整体拒绝（零写入、抛 ValueError），不静默清除失败档案记录；
        持久层与管理 API（OpsConsole.seed_wall）两层同契约。
        """

    @abstractmethod
    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（UPSERT 语义，幂等）——存档迁移/进度书签恢复。

        入口校验 fail-loud：key 非空 str、value 必须 str——静默 str() 强转
        会让同一非法入参在 sqlite 腿抛异常、memory 腿写数字串（跨后端行为
        分歧），且与内存镜像值型漂移。discovery 的已见判定走 wall/failed，
        「已见预填」请用 ``seed_wall``；本方法服务通用业务 cursor。
        """

    # 尝试轨迹（append-only 旁路观测面）--------------------------------

    @abstractmethod
    def append_attempt(self, record: AttemptRecord) -> int:
        """派发时追加一条 running 尝试轨迹行，返回自增轨迹 id。

        协议：仅在派发点调用，record 必须是 outcome="running"、无收尾
        时刻/错误的派发即插行（``validate_attempt_dispatch`` 门卫），随行
        落盘 started_at / incarnation / run_id。attempts 不参与 wall/failed
        的互斥与 REPLACE 语义，本方法不触碰 queue/wall/failed/cursors；
        持久化失败抛异常（fail-loud，不吞）。
        """

    @abstractmethod
    def update_attempt(
        self,
        attempt_id: int,
        *,
        outcome: str,
        finished_at: str,
        error: str | None = None,
    ) -> bool:
        """终态/重试时收尾轨迹行：写 finished_at / outcome / error。

        收尾词汇与 ``AttemptRecord`` 一致（succeeded / failed / requeued /
        skipped），running 是插入侧专属形态（``validate_attempt_finish``
        门卫）。每个轨迹行只允许一次收尾：目标 id 不存在返回 False
        （旁路观测面缺行不触发崩溃契约）；对已收尾行再次收尾抛
        ValueError（引擎侧双重终结属于协议缺陷，fail-loud）。持久化故障
        抛异常。除本方法的收尾三列外无任何 UPDATE 路径——历史行只增不改。
        """

    @abstractmethod
    def load_attempts(self, job_uid: str) -> list[AttemptRecord]:
        """按 job_uid 读取全部尝试轨迹（按轨迹 id 升序）；无轨迹返回空列表。"""

    # 元数据 ---------------------------------------------------------

    @abstractmethod
    def get_meta(self, key: str) -> str | None:
        """读取一条框架级元数据（如 fencing 的 last_run_id）。无则返回 None。"""

    @abstractmethod
    def set_meta(self, key: str, value: str) -> None:
        """写入一条框架级元数据（UPSERT 语义）。失败抛异常（不吞）。"""


__all__ = [
    "AbstractStateBackend",
    "validate_attempt_dispatch",
    "validate_attempt_finish",
    "validate_queue_replacement",
]
