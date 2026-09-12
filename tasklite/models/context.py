"""TaskContext for handler execution in tasklite."""
from __future__ import annotations

import logging
import math
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from .job import Job
from ..utils.ipc import ArtifactJournal
from ..utils.jsonutil import dumps

logger = logging.getLogger("tasklite")


class TaskContext:
    """Context object passed to handlers, providing spawn, output, cursor, and resource APIs."""

    def __init__(
        self,
        job: Job,
        wall_keys: set,
        failed_keys: set,
        cursors: Dict[str, str],
        output_root: Optional[Path] = None,
        ipc_dir: Optional[str] = None,
        transient_registry: Tuple = (),
        fatal_exceptions: Optional[Tuple] = None,
        transient_exceptions: Optional[Tuple] = None,
        resource_names: Optional[Union[frozenset, set]] = None,
    ):
        self.job = job
        self.new_jobs: List[Job] = []
        self.resource_suspensions: List[Tuple[str, float]] = []
        self._wall_keys = wall_keys
        self._failed_keys = failed_keys
        self._cursors = cursors
        self.cursor_updates: Dict[str, str] = {}
        # 已注册资源名快照——suspend_resource 据此 fail-loud。
        # None（直接构造 ctx 的旧测试路径）表示不校验；生产派发路径必传。
        self._resource_names: Optional[frozenset] = (
            frozenset(resource_names) if resource_names is not None else None
        )
        # output_root 支持多根（跨盘输出场景）——list 时
        # 路径属于**任一**根即通过沙盒校验。
        self.output_root = output_root
        # 注册表快照契约：瞬态异常注册表快照随 ctx pickle 下发子进程——
        # spawn 子进程不继承父进程函数作用域的注册，分类决策在子进程发生，
        # 快照在此携带、worker 入口重放（见 executor._mp_worker_wrapper）。
        self.transient_registry = tuple(transient_registry)
        # 启发式异常元组快照契约：与 transient_registry 同构随 ctx 下发。
        # 不变式：必须保真 None 与空元组的区别——None 表示未声明（worker
        # 回退内置默认启发式），空元组表示「整体替换为空」（关闭该侧启发
        # 式）；None 被误存为空元组会让子进程静默关闭全部默认启发式。
        self.fatal_exceptions = (
            tuple(fatal_exceptions) if fatal_exceptions is not None else None
        )
        self.transient_exceptions = (
            tuple(transient_exceptions) if transient_exceptions is not None else None
        )
        # suspend 信号走落盘文件（{ipc_dir}/{uid}.signals.jsonl）——
        # 进程被 kill 后文件仍在，信号不丢。
        self.ipc_dir = ipc_dir
        # fencing 的执行代标识（{run_id}.{seq}）不经 TaskContext 携带——
        # 归 WorkerLaunchSpec（进程 seam 具名契约），worker 据它构造带
        # incarnation 的结果文件名，孤儿进程的旧 incarnation 文件不被
        # 新 run 看见。
        self._journal_instance: Optional[ArtifactJournal] = None
        self._journal_instance_dir: Optional[str] = None

    @property
    def _journal(self) -> Optional[ArtifactJournal]:
        """当前 ipc_dir 对应的产物清单深模块实例（按目录缓存单例）。"""
        if self.ipc_dir is None:
            return None
        if self._journal_instance is None or self._journal_instance_dir != self.ipc_dir:
            self._journal_instance = ArtifactJournal(self.ipc_dir)
            self._journal_instance_dir = self.ipc_dir
        return self._journal_instance

    def spawn(self, job: Job) -> None:
        """Enqueue a child job.

        在子进程内立即预检 JSON 可序列化性（与 enqueue 的序列化预检对齐）。
        坏 payload 在此抛 ValueError → handler 异常 → 父作业进入 DLQ，
        而不是在 commit 阶段才崩掉整个 run。
        """
        try:
            # allow_nan=False —— 默认 allow_nan=True 会让
            # float('inf')/float('nan') 通过预检，产出非标准 JSON "Infinity"。
            dumps(job.to_dict())
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Payload for spawned job {job.uid} is not JSON-serializable: {e}"
            ) from e
        self.new_jobs.append(job)

    def declare_output(self, path: Union[str, Path], cleanup_on_fail: bool = True, *, sandbox: bool = True) -> str:
        """Declare an output file. Validates path sandbox if output_root is set.

        Automatically creates parent directories. On job failure (or retry),
        declared outputs with ``cleanup_on_fail=True`` are deleted to prevent
        half-written files from persisting.

        ``sandbox=False`` 显式豁免路径沙盒（跨盘输出等
        无法归入任何 output_root 的场景）——放弃越界保护，与
        ``output_root=None`` 同语义，但逐路径声明而非全局关闭。

        Returns:
            The **resolved absolute path** that the framework will verify/clean —
            handler 应用返回值写文件（而非原始参数），保证写入位置与校验/
            清理位置一致（相对路径按 output_root 重定位，原始
            相对字符串按 CWD 写会与校验脱节 → 误判 Missing output）。
        """
        return self._declare(path, cleanup_on_fail, sandbox, "output")

    def declare_cache(self, path: Union[str, Path], *, sandbox: bool = True) -> str:
        """Declare a **temporary/cache** file.

        语义：任务结束时（无论成败）该文件**不应存在**。
          - 成功路径：**跳过存在性校验**（原子产出的 ``.part`` 已被
            ``os.replace`` 到最终路径，校验必然失败），并 best-effort 尝试
            删除（rename 已发生则 no-op）；
          - 失败/中止路径：删除半成品。

        标准用法（原子产出模式）：``tmp = ctx.declare_cache("./v.mp4.part")``
        写 tmp → 自校验 → ``os.replace(tmp, final)``；最终路径用
        ``ctx.declare_output("./v.mp4")`` 声明（成功校验存在 + 失败清理）。

        路径沙盒与 ``declare_output`` 相同：``sandbox=False`` 显式豁免
        （跨盘缓存等无法归入任何 output_root 的场景）——放弃越界保护，
        逐路径声明而非全局关闭。
        返回解析后的规范绝对路径。
        """
        return self._declare(path, True, sandbox, "cache")

    def _declare(self, path: Union[str, Path], cleanup_on_fail: bool, sandbox: bool, kind: str) -> str:
        """declare_output/declare_cache 的共享实现（消除重复）。"""
        raw = str(path)
        resolved = ArtifactJournal.resolve_and_validate_path(raw, self.output_root, sandbox=sandbox)
        parent = Path(resolved).parent
        parent.mkdir(parents=True, exist_ok=True)
        if self._journal is not None:
            self._journal.record_output(self.job.uid, resolved, cleanup_on_fail, kind=kind)
        return resolved

    def declare_input(self, path: Union[str, Path]) -> str:
        """声明一个**输入文件**——记录 + 采集指纹，返回规范绝对路径。

        注意：本方法**没有** ``sandbox`` 参数，这是有意设计——输入声明只记录
        路径与 stat 指纹，框架不写该文件，因此不存在输出沙盒/清理语义；
        与 ``declare_output/declare_cache`` 的路径沙盒无关。

        指纹 ``{path, size, mtime_ns}``（stat 调用，微秒级，不用内容 hash
        避免大文件读盘）。任务成功时框架把输入清单写入 wall meta
        （``meta["inputs"]``）——排障/审计/种子化脚本都能回答「这个任务
        当时基于什么输入跑出来的」。

        与 ``rerun="on_input_change"`` 联动：下次 enqueue 时框架对
        wall 中的旧指纹重新 stat 比对，任一文件 size/mtime_ns 变化 → 重跑；
        不变 → 跳过（on_input_change 的变更检测只针对文件输入；URI 默认
        不检测，见 ``declare_input_uri``）。

        指纹在**声明时**采集（handler 要读的输入此刻应已存在）；stat 失败
        （文件不存在等）记录 path 但不带指纹（on_input_change 比对时视为
        变化 → 重跑，避免误跳过）。
        """
        raw = str(path)
        resolved = ArtifactJournal.resolve_and_validate_path(raw, self.output_root, sandbox=False)
        if self._journal is not None:
            self._journal.record_input_file(self.job.uid, resolved)
        return resolved

    def declare_input_uri(self, url: Union[str, Path], uri_fingerprint: Optional[str] = None) -> str:
        """声明一个**输入 URI**——仅记录（可追溯），返回 url 字符串。

        ``uri_fingerprint`` 可选：业务侧提供的指纹字符串（如 ETag/Last-
        Modified），落盘供审计。**不参与 on_input_change 比对**——URI 的
        内容是否变化需网络请求（ETag/Last-Modified），框架不在派发前免费
        检查（文档注明：URI 变更检测留给业务，可把指纹拼进 job_id 或由
        业务自建 checker）。
        """
        if uri_fingerprint is not None and not isinstance(uri_fingerprint, str):
            raise TypeError(
                f"uri_fingerprint must be a str or None, "
                f"got {type(uri_fingerprint).__name__}"
            )
        url_str = str(url)
        if self._journal is not None:
            self._journal.record_input_uri(self.job.uid, url_str, uri_fingerprint)
        return url_str

    def _resolve_path(self, raw: str, sandbox: bool) -> str:
        """解析声明路径为规范绝对路径（委托 ArtifactJournal）。"""
        return ArtifactJournal.resolve_and_validate_path(raw, self.output_root, sandbox=sandbox)

    def is_completed(self, uid: str) -> bool:
        """Check if a job is already in wall 快照（已成功完成过的内容）。

        Snapshot semantics（文档对齐）：TaskContext 在**每个 job 派发时**
        构造，快照 = 该 job 派发时刻的 wall 集合——同一 run 内后派发的 job 能
        看到先前完成的任务（README 承诺语义）。不反映构造之后其他并发
        in-flight job 的完成。
        """
        return uid in self._wall_keys

    def is_failed(self, uid: str) -> bool:
        """Check if a job has permanently failed (in the DLQ).

        Snapshot semantics（文档对齐）：同 is_completed——派发时刻快照，
        不反映构造之后其他并发 in-flight job 的失败。
        """
        return uid in self._failed_keys

    def attempted_uids(self) -> frozenset:
        """返回 wall∪failed 已见 uid 的只读快照。

        discovery 的 on_missing（``wrappers/discovery.py``）需要「可迭代的已见
        集合」——本方法是唯一公开入口，不暴露 ``_wall_keys`` 等私有字段
        （跨模块私有耦合，字段名变更会静默失效）。返回**快照**（frozenset）
        而非活引用：调用方改动
        返回值不影响框架内部集合（身份判定基础不被业务 mutate）。
        快照语义同 ``is_completed``/``is_failed``——构造时刻的 wall/failed 并集。
        """
        return frozenset(self._wall_keys) | frozenset(self._failed_keys)

    def get_cursor(self, key: str) -> Optional[str]:
        """Retrieve the value of a high-watermark cursor."""
        return self._cursors.get(key)

    def set_cursor(self, key: str, value: Optional[str]) -> None:
        """Update a cursor. It will be committed atomically when the job succeeds.

        Both key and value must be strings. Pass ``None`` as value to **delete**
        the cursor（与后端 None-删除语义对齐）。
        """
        if not isinstance(key, str):
            raise TypeError(f"cursor key must be str, got {type(key).__name__}")
        if value is not None and not isinstance(value, str):
            raise TypeError(f"cursor value must be str, got {type(value).__name__}")
        self.cursor_updates[key] = value
        if value is None:
            self._cursors.pop(key, None)
        else:
            self._cursors[key] = value

    def suspend_resource(self, resource_name: str, seconds: float) -> None:
        """Request global suspension of a resource (e.g., for 429 rate limits).

        API 边界即校验（与 set_cursor 的严格校验对称）——非数值/非有限/
        非正值在入口拒绝，而非延迟到 Resource._sanitize_suspend 静默忽略，
        避免调用方误以为 suspend 已生效。

        信号立即追加到 ``{ipc_dir}/{uid}.signals.jsonl``
        （落盘 flush），即使 handler 随后崩溃/超时，限流信息也不丢失。
        同时保留 resource_suspensions list 供正常完成路径的结果文件携带
        （去重由 suspend 的 max 语义保证）。
        """
        if not isinstance(resource_name, str) or not resource_name:
            raise TypeError(f"resource_name must be a non-empty str, got {resource_name!r}")
        # 未注册资源名 fail-loud——typo（如 "api_v1" vs
        # "api"）会静默追加，消费端 apply_result/apply_pending_signals
        # 按 `in resources` 静默跳过 → 用户以为限流已生效、管线继续猛打
        # 目标 API。生产派发路径携带注册集快照，入口即拒绝。
        if self._resource_names is not None and resource_name not in self._resource_names:
            raise ValueError(
                f"Unknown resource {resource_name!r} (registered: "
                f"{sorted(self._resource_names)}); suspend request ignored"
            )
        if not isinstance(seconds, (int, float)) or isinstance(seconds, bool):
            raise TypeError(f"seconds must be a number, got {type(seconds).__name__} ({seconds!r})")
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError(f"seconds must be finite and > 0, got {seconds!r}")
        self.resource_suspensions.append((resource_name, seconds))
        if self._journal is not None:
            self._journal.record_signal(self.job.uid, resource_name, seconds)
        logger.info(f"Task requested global suspension of resource '{resource_name}' for {seconds}s.")
