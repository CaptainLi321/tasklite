"""OpsConsole：run() 外纯运维接缝。

从 StateStore 剥离管理段逻辑（list_failures / clear_failures / retry_
failure / clear_history / seed_wall / seed_cursor / list_suspensions /
uncompleted），供门面管理 API 委托。不变式：管理 API 仅限 run() 外
调用（改变 is_known 判定基础，与 enqueue 同纪律）。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Sequence

from .errorclass import ErrorClassifier
from .resource import META_RESOURCE_SUSPENSIONS
from .store import FailureEntry, StateStore
from ..models.job import Job
from ..models.state import uid_from_job_dict
from ..utils.jsonutil import loads

logger = logging.getLogger("tasklite.v2")


@dataclass(frozen=True)
class SuspendEntry:
    """资源挂起条目（只读运维视图值对象）。

    ``resume_at`` 为挂钟时间戳（epoch 秒），``remaining_seconds`` 为查询
    时刻快照——耗时操作后应以 ``resume_at`` 为准重新换算。
    """

    resource: str
    resume_at: float
    remaining_seconds: float


class OpsConsole:
    """run() 外的纯运维接缝——管理段操作委托目标。

    构造依赖 (store, classifier)：持久化后端经 ``store.backend`` 只读
    派生（所有权唯一锚定 StateStore，换库随锚点自然跟随，本类不独立
    持库）；list_failures 的 error_type 归档视图依赖 classifier.classify。
    """

    def __init__(
        self,
        store: StateStore,
        classifier: ErrorClassifier,
    ) -> None:
        self._store = store
        self._classifier = classifier

    # ── 只读查询 ──────────────────────────────────────────────────────

    def list_suspensions(self) -> list[SuspendEntry]:
        """只读查询当前仍生效的资源挂起（跨重启持久化的限流/离线等待）。

        真相源是 meta 表（``resource_suspensions``）而非内存
        ResourceManager——挂起仅在 ``run()`` 启动时恢复进内存，run() 外
        查询内存态恒为空。已过期（解封时刻早于查询时刻）的条目过滤不
        返回；坏数据降级为告警 + 跳过，与启动期恢复路径的 fail-soft
        语义一致。

        Returns:
            list[SuspendEntry]: 按 ``resume_at`` 升序（最先解封在前）。
        """
        try:
            raw = self._store.backend.get_meta(META_RESOURCE_SUSPENSIONS)
        except Exception as e:
            logger.warning(f"Failed to load resource suspensions from meta: {e}")
            return []
        if raw is None:
            return []
        try:
            deadlines = loads(raw)
        except (ValueError, TypeError) as e:
            logger.warning(f"Corrupted resource_suspensions meta, ignoring: {e}")
            return []
        if not isinstance(deadlines, dict):
            logger.warning("resource_suspensions meta is not a dict, ignoring")
            return []

        now_wall = time.time()
        entries: list[SuspendEntry] = []
        for name, deadline in deadlines.items():
            # loads 契约保证键恒为 str、数值恒有限——仅需防字符串/布尔
            # 等合法 JSON 但语义非法的值（bool 是 int 子类，须先判）。
            if isinstance(deadline, bool) or not isinstance(deadline, (int, float)):
                logger.warning(
                    f"Suspension query: non-numeric deadline for {name!r}, skipping"
                )
                continue
            remaining = deadline - now_wall
            if remaining <= 0:
                continue
            entries.append(SuspendEntry(
                resource=name,
                resume_at=float(deadline),
                remaining_seconds=float(remaining),
            ))
        entries.sort(key=lambda e: e.resume_at)
        return entries

    def list_failures(self) -> list[FailureEntry]:
        """只读查询失败档案，返回结构化条目（uid / error / meta /
        job_payload）。

        meta 视图缺 error_type（带外登记的裸行）时经 classifier 就地归档
        （unknown 兜底）；损坏行（非 dict）归空视图，绝不让单条脏数据
        打断整条查询。
        """
        failed = self._store.backend.load_failed()
        payloads = self._store.backend.load_failed_payloads()
        entries: list[FailureEntry] = []
        for uid, meta in sorted(failed.items()):
            if not isinstance(meta, dict):
                meta = {}
            meta_view = dict(meta)
            if not meta_view.get("error_type"):
                meta_view["error_type"] = (
                    self._classifier.classify(meta).failed_error_type
                )
            entries.append(FailureEntry(
                uid=uid,
                error=str(meta_view.get("error", "")),
                meta=meta_view,
                job_payload=payloads.get(uid),
            ))
        return entries

    def uncompleted(self, jobs: Sequence[Job]) -> list[Job]:
        """过滤出尚未成功完成的 job（uid 不在 wall 的子集，保留输入顺序）。

        回溯/增量场景的入队前过滤辅助。边界语义：
        - 只按 wall（成功历史）过滤；失败档案（failed）中的 job **不**
          排除——重新入队后是否重跑由 ``Job(rerun=...)`` 在派发层裁决
          （默认 ``never`` 静默跳过，``on_failure`` 重跑），此处抢先过滤
          会吞掉重跑策略的豁免语义；
        - 已驻留队列的重复 uid 不排除——队列去重是 enqueue 自身的职责。
        """
        wall = self._store.backend.load_wall()
        return [j for j in jobs if j.uid not in wall]

    # ── 变更操作 ──────────────────────────────────────────────────────

    def clear_failures(
        self,
        task_types: Sequence[str] | None = None,
        *,
        keep_fatal: bool = True,
    ) -> int:
        """从失败档案删除匹配条目（默认保留 fatal=true 的确定性失败），
        返回删除数。"""
        if task_types is not None:
            if not isinstance(task_types, (list, tuple)):
                raise TypeError(
                    f"task_types must be a list/tuple of str or None, "
                    f"got {type(task_types).__name__}"
                )
            for t in task_types:
                if not isinstance(t, str) or not t:
                    raise TypeError(
                        f"task_types must contain only non-empty str, got {t!r}"
                    )
            task_types = list(task_types)
        failed = self._store.backend.load_failed()
        to_delete = [
            uid for uid, meta in failed.items()
            if (task_types is None or any(uid.startswith(t + "::") for t in task_types))
            and not (keep_fatal and isinstance(meta, dict) and meta.get("fatal"))
        ]
        if not to_delete:
            return 0
        deleted_count = self._store.backend.delete_failed(to_delete)
        state = self._store.state
        for uid in to_delete:
            state.discard_failed(uid)
        return deleted_count

    def retry_failure(self, uid: str) -> bool:
        """把失败档案中的单个作业移出档案并重新入队（人工补跑通道）。

        激活代按拦截点放行语义推进：档案补跑本质是 failed 拦截点放行
        重跑，重建作业的 ``activation_no`` 取该 uid 在 attempts 轨迹中的
        最大激活代 +1、``attempt_no`` 归 1——否则补跑执行与首激活在
        attempts 表共享同一 ``(activation_no, attempt_no)`` 逻辑键，
        追溯面撞号；无轨迹行（带外登记）回退初激活 1。业务 payload 取
        档案快照，经 store.enqueue_jobs 规范化（策略注入 + 资源注入 +
        首入队时间）队首入队，再删档案行。先入队后删除的次序保证崩溃
        窗口至多留下「queue∩failed 并存」——由加载期修复按 rerun 策略
        收敛，绝不丢作业。已在队列驻留的 uid 不重复入队（enqueue 去重），
        档案行照删（下次 run 从队列执行）。

        Returns:
            bool: 条目已从档案移除（入队完成或已驻留队列）。

        Raises:
            KeyError: uid 不在失败档案（typo fail-loud，静默 no-op 会
                掩盖补跑拼写错误）。
        """
        failed = self._store.backend.load_failed()
        if uid not in failed:
            raise KeyError(f"uid {uid!r} not in failure archive")
        task_type, _, job_id = uid.partition("::")
        payloads = self._store.backend.load_failed_payloads()
        # 激活代取轨迹最大值 +1（failed 拦截点放行 = 新激活代），attempt_no
        # 经 Job 模型默认归 1；payload 取档案快照（无快照回退空 dict）。
        prior_activation = max(
            (record.activation_no for record in self._store.backend.load_attempts(uid)),
            default=0,
        )
        job = Job(
            task_type=task_type,
            job_id=job_id,
            payload=payloads.get(uid) or {},
            activation_no=prior_activation + 1,
        )
        self._store.enqueue_jobs([job], front=True)
        self._store.backend.delete_failed([uid])
        state = self._store.state
        state.discard_failed(uid)
        return True

    def clear_history(
        self,
        targets: str | Sequence[str],
        *,
        where: Sequence[str] = ("wall", "failed"),
        predicate: Callable[[str], bool] | None = None,
    ) -> int:
        """从 wall 和/或失败档案删除条目。

        ``predicate`` 提供官方的「Python 侧判定 + 定向批删」通道：对
        targets 命中的 uid 逐条调用，返回 False 则保留。判定在本进程
        完成、删除仍走后端批量删除接口，业务无需再裸连数据库绕过
        WAL 校验与写事务纪律。
        """
        if isinstance(targets, str):
            patterns = [targets]
        elif isinstance(targets, (list, tuple)):
            patterns = list(targets)
        else:
            raise TypeError(
                f"targets must be a str or a list/tuple of str, "
                f"got {type(targets).__name__} ({targets!r})"
            )
        for p in patterns:
            if not isinstance(p, str):
                raise TypeError(
                    f"targets must contain only str, got {type(p).__name__} ({p!r})"
                )
        if predicate is not None and not callable(predicate):
            raise TypeError(
                f"predicate must be callable or None, got {type(predicate).__name__}"
            )
        if not isinstance(where, (list, tuple)):
            raise TypeError(
                f"where must be a sequence of 'wall'/'failed', "
                f"got {type(where).__name__}"
            )
        where_set = set(where)
        unknown = where_set - {"wall", "failed"}
        if unknown:
            raise ValueError(
                f"where contains unknown target(s): {sorted(unknown)!r}; "
                f"allowed: 'wall', 'failed'"
            )

        def _matches(uid: str) -> bool:
            hit = any(
                uid == p or (p.endswith("::") and uid.startswith(p))
                for p in patterns
            )
            return hit and (predicate is None or predicate(uid))

        state = self._store.state
        total = 0
        if "wall" in where:
            wall = self._store.backend.load_wall()
            matched = [u for u in wall if _matches(u)]
            if matched:
                total += self._store.backend.delete_wall(matched)
                for u in matched:
                    state.discard_wall(u)
        if "failed" in where:
            failed = self._store.backend.load_failed()
            matched = [u for u in failed if _matches(u)]
            if matched:
                total += self._store.backend.delete_failed(matched)
                for u in matched:
                    state.discard_failed(u)
        return total

    def seed_wall(self, uids: Sequence[str]) -> int:
        """把 uid 批量写入 wall（存档迁移标记「已处理」），返回实际写入数。

        不变式（六集合互斥）：已在失败档案或驻留队列的 uid 拒绝种子。
        failed 与 queue 两腿均以后端持久层为真相（run 前内存 state 是
        空占位，仅查内存腿会漏掉落盘队列驻留）。预检先于任何写入
        （backend 持久层与内存 state 双腿零写入），冲突整体拒绝——静默
        写入会留下 wall∩failed / wall∩queue 非豁免重叠，令下一次状态
        变更的一致性断言崩溃；失败档案记录须先经 clear_failures /
        clear_history 显式清除，队列驻留须待 run 排空或显式清理后再种子。
        """
        if not isinstance(uids, (list, tuple)):
            raise TypeError(
                f"uids must be a list/tuple of str, got {type(uids).__name__}"
            )
        for u in uids:
            if not isinstance(u, str) or u.count("::") != 1:
                raise ValueError(
                    f"seed_wall uid must be 'task_type::job_id' str with exactly "
                    f"one '::' separator, got {u!r}"
                )
            task_type, job_id = u.split("::", 1)
            if not task_type or not job_id:
                raise ValueError(
                    f"seed_wall uid must have non-empty task_type and job_id, got {u!r}"
                )
        failed = self._store.backend.load_failed()
        queue_uids = set(self._store.queue_uids)
        queue_uids.update(uid_from_job_dict(jd) for jd in self._store.backend.load_queue())
        conflict_failed = sorted({u for u in uids if u in failed})
        conflict_queue = sorted({u for u in uids if u in queue_uids})
        if conflict_failed:
            raise ValueError(
                f"seed_wall refuses uid(s) already in the failure archive: "
                f"{conflict_failed}; wall/failed must stay disjoint. Clear the "
                f"failure entries first (clear_failures/clear_history) if "
                f"archiving is intended."
            )
        if conflict_queue:
            raise ValueError(
                f"seed_wall refuses uid(s) still resident in the queue: "
                f"{conflict_queue}; wall/queue must stay disjoint. Drain or "
                f"clear the queue entries first if archiving is intended."
            )
        written = self._store.backend.seed_wall(list(uids))
        state = self._store.state
        for u in uids:
            state.add_wall(u, {})
        return written

    def seed_cursor(self, key: str, value: str) -> None:
        """预填一个 cursor（幂等）——存档迁移/进度书签恢复。

        入口即校验（与持久层 seed_cursor 同契约）：key 非空 str、value
        必须 str。委托前拒绝，保证持久层与内存镜像双腿零写入且双写值
        一致。
        """
        if not isinstance(key, str) or not key:
            raise TypeError(f"cursor key must be a non-empty str, got {key!r}")
        if not isinstance(value, str):
            raise TypeError(f"cursor value must be str, got {type(value).__name__}")
        self._store.backend.seed_cursor(key, value)
        self._store.state.set_cursor(key, value)


__all__ = [
    "OpsConsole",
    "SuspendEntry",
]
