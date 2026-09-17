"""ExecutionChannel: 统一子进程执行、阶梯看门狗、IPC 通信与跨平台文件锁的深模块。

内敛子进程派发、看门狗阶梯终止、
两级降级落盘、跨平台排他文件锁与 TOCTOU 闭环清理的底层复杂性。
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import signal
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from ..exceptions import (
    FatalError,
    RateLimitHit,
    RetryError,
)
from ..taxonomy import _DEFAULT_TAXONOMY, classify_exception
from ..models.context import TaskContext
from ..models.job import Job
from ..utils.ipc import (
    ArtifactCleanupMode,
    ArtifactJournal,
    decode_raw_result as _decode_raw_result,
    encode_raw_result as _encode_raw_result,
)
from ..utils.jsonutil import dumps
from ..utils import lockfile

logger = logging.getLogger("tasklite")

_KEY_TRACEBACK = "traceback"
_RESULT_DIR_ENV = "TASKLITE_IPC_DIR"


def _normalize_handler_result(result: Any) -> Tuple[bool, Dict[str, Any]]:
    """Normalize handler return value to (success, metadata) tuple."""
    if result is None:
        return True, {}
    if isinstance(result, bool):
        return result, {}
    if isinstance(result, dict):
        try:
            dumps(result)
        except (TypeError, ValueError) as e:
            logger.error(
                f"Handler returned dict with non-JSON-serializable metadata: {e}. "
                f"Job will be marked as failed."
            )
            return False, {"error": f"invalid result metadata: not JSON-serializable: {e}"}
        return True, result
    if isinstance(result, tuple) and len(result) == 2:
        if not isinstance(result[0], bool) or not isinstance(result[1], dict):
            logger.error(
                f"Handler returned invalid tuple: expected (bool, dict), got "
                f"({type(result[0]).__name__}, {type(result[1]).__name__}). Job will be marked as failed."
            )
            return False, {"error": "invalid handler return tuple: expected (bool, dict)"}
        try:
            dumps(result[1])
        except (TypeError, ValueError) as e:
            logger.error(
                f"Handler returned tuple with non-JSON-serializable metadata: {e}. "
                f"Job will be marked as failed."
            )
            return False, {"error": f"invalid result metadata: not JSON-serializable: {e}"}
        return result[0], result[1]
    logger.error(
        f"Handler returned unrecognized type: {type(result).__name__}. "
        f"Expected None, bool, dict, or tuple(bool, dict). "
        f"Job will be marked as failed."
    )
    return False, {"error": f"invalid handler return type: {type(result).__name__}"}


def _decode_ipc_result(
    res: dict,
    p: Any,
    job: Job,
    ipc_dir: Optional[str] = None,
    *,
    output_roots: Optional[Sequence[Union[str, Path]]] = None,
) -> "ExecutionResult":
    """解析子进程通过结果文件回传的结果字典。"""
    success = False
    result_meta: Dict[str, Any] = {}
    retry_requested = False
    retry_error: Optional[str] = None
    new_jobs: List[Job] = []
    cursor_updates: Dict[str, str] = {}
    resource_suspensions: List[Tuple[str, float]] = []
    lock_conflict = False
    rate_limited = False
    interrupted = False

    if isinstance(res, dict) and "status" in res:
        if res["status"] == "success":
            if "raw_result" not in res:
                logger.error(f"Corrupt result file for {job.uid}: missing 'raw_result' key")
                result_meta = {"error": "CORRUPT_RESULT_FILE: missing raw_result"}
            else:
                try:
                    raw_result = _decode_raw_result(res["raw_result"])
                    success, result_meta = _normalize_handler_result(raw_result)
                except Exception as e:
                    success = False
                    result_meta = {
                        "error": f"CORRUPT_RESULT_FILE: decode failed: {e}",
                        _KEY_TRACEBACK: traceback.format_exc(),
                    }
                    logger.error(f"Result decode failed for {job.uid}: {e}")
            new_jobs = []
            _nj = res.get("new_jobs", [])
            # 不变式：结果文件任何异常形态都收敛为任务级失败记账，
            # 不得以 TypeError 穿透 drain/claim 使单任务损坏放大为整管崩溃。
            if isinstance(_nj, list):
                for jd in _nj:
                    try:
                        new_jobs.append(Job.from_dict(jd))
                    except (KeyError, TypeError, ValueError) as e:
                        success = False
                        result_meta = {
                            "error": f"invalid spawned job dict: {e}",
                            _KEY_TRACEBACK: traceback.format_exc(),
                        }
                        logger.error(f"Malformed spawned job in {job.uid}: {e}")
                        break
            else:
                success = False
                result_meta = {"error": f"invalid new_jobs type: {type(_nj).__name__}"}
                logger.error(f"Invalid new_jobs type in {job.uid}: {type(_nj).__name__}")
            if success:
                _cu = res.get("cursor_updates", {})
                _rs = res.get("resource_suspensions", [])
                if isinstance(_cu, dict):
                    valid_cu = {}
                    cu_ok = True
                    for k, v in _cu.items():
                        if isinstance(k, str) and (v is None or isinstance(v, str)):
                            valid_cu[k] = v
                        else:
                            success = False
                            result_meta = {"error": f"invalid cursor_updates value: {k!r}={v!r}"}
                            cu_ok = False
                            break
                    if cu_ok:
                        cursor_updates = valid_cu
                else:
                    success = False
                    result_meta = {"error": f"invalid cursor_updates type: {type(_cu).__name__}"}
                if isinstance(_rs, list):
                    valid_rs = []
                    for item in _rs:
                        if (
                            isinstance(item, (list, tuple))
                            and len(item) == 2
                            and isinstance(item[0], str)
                            and isinstance(item[1], (int, float))
                            and not isinstance(item[1], bool)
                        ):
                            valid_rs.append((item[0], float(item[1])))
                        else:
                            success = False
                            result_meta = {"error": f"invalid resource_suspension entry: {item!r}"}
                            break
                    if valid_rs:
                        resource_suspensions = valid_rs
                else:
                    success = False
                    result_meta = {"error": f"invalid resource_suspensions type: {type(_rs).__name__}"}
        elif res["status"] == "retry":
            retry_requested = True
            retry_error = res.get("error")
            lock_conflict = bool(res.get("lock_conflict", False))
            rate_limited = bool(res.get("rate_limited", False))
        elif res["status"] == "interrupted":
            retry_requested = True
            retry_error = res.get("error")
            interrupted = True
        elif res["status"] == "fatal":
            success = False
            result_meta = {
                "error": res.get("error", "FATAL (no message)"),
                _KEY_TRACEBACK: res.get(_KEY_TRACEBACK),
                "fatal": True,
            }
            logger.error(f"Fatal error in {job.uid}:\n{res.get(_KEY_TRACEBACK, '')}")
        else:
            success = False
            result_meta = {
                "error": res.get("error", f"unknown status {res['status']!r}"),
                _KEY_TRACEBACK: res.get(_KEY_TRACEBACK),
            }
            logger.error(f"Worker Crashed for {job.uid}:\n{res.get(_KEY_TRACEBACK, '')}")
    else:
        # 无有效结果文件 → 死亡归因全权交决策表（信号死亡与正码崩溃同表同形）
        att = _DEFAULT_TAXONOMY.attribute_process_death(
            getattr(p, "exitcode", None), timed_out=False
        )
        result_meta = att.result_meta
        retry_error = att.retry_error
        retry_requested = att.retry_requested
        retry_requested = True

    if success and ipc_dir is not None:
        ok, err = ArtifactJournal(ipc_dir, output_roots=output_roots).verify_outputs(job.uid)
        if not ok:
            logger.error(f"Verification failed for {job.uid}: {err}")
            success = False
            result_meta = {"error": err}

    return ExecutionResult(
        success=success,
        result_meta=result_meta,
        retry_requested=retry_requested,
        retry_error=retry_error,
        new_jobs=new_jobs,
        cursor_updates=cursor_updates,
        resource_suspensions=resource_suspensions,
        lock_conflict=lock_conflict,
        rate_limited=rate_limited,
        interrupted=interrupted,
    )


def _mp_worker_wrapper(spec: "WorkerLaunchSpec") -> None:
    """Wrapper for multiprocessing worker execution."""
    handler_func = spec.handler
    job = spec.job
    ctx = spec.task_ctx
    ipc_dir = spec.ipc_dir
    uid = job.uid
    incarnation = spec.incarnation
    if not incarnation:
        raise RuntimeError(
            f"worker started without incarnation for {uid}: "
            f"fencing requires WorkerLaunchSpec.incarnation"
        )
    journal = ArtifactJournal(ipc_dir)
    result_token = spec.result_token

    def _write_result(payload: Dict[str, Any]) -> None:
        # 结果认证令牌随全部状态通道落盘，主进程读取侧强校验
        payload["auth"] = result_token
        journal.write_result_with_degradation(uid, payload, incarnation=incarnation)

    transient_registry = getattr(ctx, "transient_registry", ()) or ()
    try:
        _lock_fd = lockfile.try_acquire_lock(ipc_dir, uid, timeout=2.0)
    except OSError as e:
        # 锁文件环境故障（权限/目录占位）写降级重试结果：与「锁被占」
        # 同走瞬态 defer 通道（零预算、零污染），不让单作业环境故障
        # 炸穿执行体；errno 归因保留供运维区分两种语义。
        _write_result({
            "status": "retry",
            "lock_conflict": True,
            "error": f"LOCK_ENV_FAULT: cannot open lock file for {uid} "
                     f"(errno={getattr(e, 'errno', None)}): {e}",
        })
        return
    if _lock_fd is None:
        _write_result({
            "status": "retry",
            "lock_conflict": True,
            "error": f"LOCK_CONFLICT: another execution body holds {uid} lock",
        })
        return
    try:
        raw_result = handler_func(job, ctx)
        _write_result({
            "status": "success",
            "raw_result": _encode_raw_result(raw_result),
            "new_jobs": [j.to_dict() for j in ctx.new_jobs],
            "resource_suspensions": ctx.resource_suspensions,
            "cursor_updates": ctx.cursor_updates,
        })
    except RetryError as e:
        # 限流瞬态判定必须在子进程编码侧完成：RateLimitHit 是 RetryError
        # 子类、与本类共享 status="retry" 通道，父进程侧已无异常类型可辨；
        # 预算豁免依赖该结构化字段（与 lock_conflict 同构）。
        retry_payload: Dict[str, Any] = {"status": "retry", "error": str(e)}
        if isinstance(e, RateLimitHit):
            retry_payload["rate_limited"] = True
        _write_result(retry_payload)
    except FatalError as e:
        _write_result({
            "status": "fatal",
            "error": str(e),
            _KEY_TRACEBACK: traceback.format_exc(),
        })
    except Exception as e:
        kind = classify_exception(
            e,
            transient_registry,
            fatal_exceptions=getattr(ctx, "fatal_exceptions", None),
            transient_exceptions=getattr(ctx, "transient_exceptions", None),
        )
        if kind == "retry":
            _write_result({
                "status": "retry",
                "error": f"{type(e).__name__}: {e}",
            })
        elif kind == "fatal":
            _write_result({
                "status": "fatal",
                "error": f"{type(e).__name__}: {e}",
                _KEY_TRACEBACK: traceback.format_exc(),
            })
        else:
            _write_result({
                "status": "error",
                "error": f"{type(e).__name__}: {e}",
                _KEY_TRACEBACK: traceback.format_exc(),
            })
    except KeyboardInterrupt as e:
        _write_result({
            "status": "interrupted",
            "error": f"WORKER_INTERRUPTED: {type(e).__name__}",
            _KEY_TRACEBACK: traceback.format_exc(),
        })
    except SystemExit as e:
        _write_result({
            "status": "error",
            "error": f"WORKER_INTERRUPTED: {type(e).__name__}",
            _KEY_TRACEBACK: traceback.format_exc(),
        })
    finally:
        lockfile.release_lock(_lock_fd)


@dataclass
class ExecutionResult:
    """Outcome of a single multiprocessing job execution."""
    success: bool = False
    result_meta: Dict[str, Any] = field(default_factory=dict)
    retry_requested: bool = False
    retry_error: Optional[str] = None
    new_jobs: List[Job] = field(default_factory=list)
    cursor_updates: Dict[str, str] = field(default_factory=dict)
    resource_suspensions: List[Tuple[str, float]] = field(default_factory=list)
    lock_conflict: bool = False
    rate_limited: bool = False
    interrupted: bool = False
    going_to_retry: Optional[bool] = None


@dataclass
class JobHandle:
    """一个 in-flight 子进程的句柄。"""
    uid: str
    process: Any
    deadline: float
    timeout: float
    job: Job
    ipc_dir: str
    incarnation: Optional[str] = None


@dataclass(frozen=True)
class WorkerLaunchSpec:
    """进程 seam 具名契约——spawn 下发子进程执行体的全部载荷。

    incarnation（{run_id}.{dispatch_seq} 执行身份）与结果认证令牌归本 spec，
    不借道 TaskContext 属性穿透进程边界。
    """

    handler: Callable[[Job, TaskContext], Any]
    job: Job
    task_ctx: TaskContext
    incarnation: str
    ipc_dir: str
    timeout: float
    result_token: Optional[str] = None


@dataclass(frozen=True)
class AbortOutcome:
    """强制终止/异常停机时的收尾结果。

    salvaged_signals：杀进程后补排空捞回的 (uid, resource, seconds)——
    cancelled 任务的信号文件已随半成品清理删除，其 suspend 信号只能经
    本列表带回上层应用（suspend 的 max 语义保证重复应用幂等）。
    """
    completed: List[Tuple[JobHandle, ExecutionResult]]
    cancelled: List[JobHandle]
    salvaged_signals: List[Tuple[str, str, float]] = field(default_factory=list)


class ExecutionChannel:
    """标准子进程执行通道深模块实现。"""

    _JOIN_REAP_TIMEOUT: float = 5.0

    def __init__(
        self,
        ipc_dir: Optional[Union[str, Path]] = None,
        *,
        mp_ctx: Optional[Any] = None,
        output_roots: Optional[Union[str, Path, Sequence[Union[str, Path]]]] = None,
    ) -> None:
        self._mp_ctx = mp_ctx or mp.get_context("spawn")
        self.ipc_dir = str(ipc_dir) if ipc_dir is not None else None
        # 输出沙盒信任根随 channel 注入 journal——清理消费侧的删除复检依赖
        self.output_roots = output_roots
        # 每 run 随机结果认证令牌（run 启动屏障由 runtime 同步）；None 表示未启用
        self.result_token: Optional[str] = None
        if self.ipc_dir:
            try:
                Path(self.ipc_dir).mkdir(parents=True, exist_ok=True)
            except OSError:
                pass

    @property
    def journal(self) -> ArtifactJournal:
        ipc = getattr(self, "ipc_dir", None)
        j = getattr(self, "_journal", None)
        if j is None or j.ipc_dir != ipc:
            j = ArtifactJournal(ipc, output_roots=getattr(self, "output_roots", None))
            self._journal = j
        return j

    @journal.setter
    def journal(self, value: ArtifactJournal) -> None:
        self._journal = value

    @staticmethod
    def _finalize_process(p: Any) -> None:
        """清理单个子进程：kill + join + close。"""
        try:
            if p.is_alive():
                p.kill()
        except Exception:
            pass
        try:
            p.join(timeout=ExecutionChannel._JOIN_REAP_TIMEOUT)
        except Exception:
            pass
        try:
            _p_close = getattr(p, "close", None)
            if _p_close is not None:
                _p_close()
        except Exception:
            pass

    def spawn(self, spec: WorkerLaunchSpec) -> JobHandle:
        """启动隔离子进程执行 handler，建立 incarnation fencing，立即返回句柄。"""
        effective_ipc_dir = spec.ipc_dir or self.ipc_dir or os.environ.get(_RESULT_DIR_ENV)
        if not effective_ipc_dir:
            raise ValueError("ipc_dir is required for ExecutionChannel.spawn()")
        Path(effective_ipc_dir).mkdir(parents=True, exist_ok=True)
        # 不变式：收割路径（journal/probe/drain）以实例属性为事实源，实例
        # 属性为空而经兜底解析成功时必须回写，保证与 handle 派发路径同源
        if self.ipc_dir is None:
            self.ipc_dir = str(effective_ipc_dir)
        spec.task_ctx.ipc_dir = effective_ipc_dir
        p = None
        try:
            p = self._mp_ctx.Process(target=_mp_worker_wrapper, args=(spec,))
            p.start()
        except Exception:
            if p is not None:
                self._finalize_process(p)
            raise
        deadline = time.monotonic() + spec.timeout
        return JobHandle(
            uid=spec.job.uid,
            process=p,
            deadline=deadline,
            timeout=spec.timeout,
            job=spec.job,
            ipc_dir=effective_ipc_dir,
            incarnation=spec.incarnation,
        )

    def _read_authenticated_result(self, path: Path) -> Optional[dict]:
        """读取侧强校验结果认证令牌，不匹配按无结果丢弃（瞬态、零预算）。

        威胁模型：ipc_dir 写入者可伪造结果文件驱动 wall 投毒、new_jobs
        子任务注入与 cursor 投毒；令牌不匹配的结果绝不进入解码管线。
        """
        res = self.journal.read_result(path)
        token = getattr(self, "result_token", None)
        if res is not None and token is not None and res.get("auth") != token:
            logger.error(
                f"Result auth token mismatch, discarding untrusted result: {path.name}"
            )
            return None
        return res

    def reap_completed(
        self, handles: Sequence[JobHandle]
    ) -> List[Tuple[JobHandle, ExecutionResult]]:
        """非阻塞扫描所有 in-flight handle，返回本次已完成的 (handle, result)。"""
        completed: List[Tuple[JobHandle, ExecutionResult]] = []

        for handle in handles:
            now = time.monotonic()
            p = handle.process
            res_path = self.journal.result_path(handle.uid, handle.incarnation)

            res = (
                self._read_authenticated_result(res_path)
                if res_path.exists() else None
            )
            if res is not None:
                completed.append((handle, self._collect_outcome(handle, res)))
                continue

            if not p.is_alive():
                res = (
                    self._read_authenticated_result(res_path)
                    if res_path.exists() else None
                )
                completed.append((handle, self._collect_outcome(handle, res)))
                continue

            if now > handle.deadline:
                p.join(timeout=0.5)
                if p.is_alive():
                    try:
                        p.kill()
                    except Exception:
                        pass
                    is_timeout = True
                else:
                    # join 窗口内已自然退出：死亡归因全权交 exitcode 分簇
                    # （0 → NO_IPC_RESULT 瞬态、<0 信号、>0 正码崩溃），与
                    # deadline 前同因事件同果，不折入 TIMEOUT；仅 kill 后
                    # 仍存活/被击杀的执行体才归 TIMEOUT。
                    is_timeout = False
                res = (
                    self._read_authenticated_result(res_path)
                    if res_path.exists() else None
                )
                completed.append(
                    (handle, self._collect_outcome(handle, res, is_timeout=is_timeout))
                )

        return completed

    def _collect_outcome(
        self, handle: JobHandle, res: Optional[dict], *, is_timeout: bool = False
    ) -> ExecutionResult:
        """收敛 drain 三段同构尾部：解析结果 -> 收割进程 -> 清理 IPC 文件。"""
        p = handle.process
        result: Optional[ExecutionResult] = None
        try:
            if res is not None:
                result = _decode_ipc_result(
                    res, p, handle.job, handle.ipc_dir,
                    output_roots=getattr(self, "output_roots", None),
                )
            else:
                result = self._build_terminal_failure(p, handle, is_timeout=is_timeout)
        finally:
            self._finalize_process(p)
            try:
                pending_signals = self.journal.drain_signals(handle.uid)
                if pending_signals:
                    if result is not None:
                        result.resource_suspensions = (
                            list(result.resource_suspensions) + list(pending_signals)
                        )
                        logger.info(
                            f"Salvaged {len(pending_signals)} suspend signal(s) "
                            f"from {handle.uid} before IPC cleanup"
                        )
            except Exception as e:
                logger.warning(f"Failed to salvage signals for {handle.uid}: {e}")
            self.journal.cleanup_ipc_files(handle.uid, handle.incarnation)
        return result

    def claim_stale_result(self, uid: str, job: Job) -> Optional[ExecutionResult]:
        """启动/派发前崩溃恢复：认领并消费上次 run 遗留的已落盘残留结果。

        残留结果必须携带本 run 的认证令牌——跨 run 残留与伪造残留一律
        丢弃（崩溃恢复退化为重跑，保守正确；伪造残留的注入链不可达）。
        """
        res = self.journal.claim_stale_result(uid)
        token = getattr(self, "result_token", None)
        if res is not None and token is not None and res.get("auth") != token:
            logger.error(
                f"Stale result auth token mismatch for {uid}; "
                f"discarding untrusted result"
            )
            return None
        if res is None:
            return None
        return _decode_ipc_result(
            res, None, job, self.ipc_dir,
            output_roots=getattr(self, "output_roots", None),
        )

    @staticmethod
    def _build_terminal_failure(
        p: Any, handle: JobHandle, *, is_timeout: bool
    ) -> ExecutionResult:
        """构造进程终止（崩溃/超时）但无结果文件时的 ExecutionResult。"""
        att = _DEFAULT_TAXONOMY.attribute_process_death(
            p.exitcode,
            timed_out=is_timeout,
            timeout_seconds=handle.timeout,
            timeout_is_transient=getattr(handle.job, "timeout_is_transient", False),
        )
        if att.retry_requested and "signal" in att.result_meta:
            logger.warning(
                f"Worker for {handle.uid} died by {att.result_meta['signal']}; "
                f"will retry with backoff"
            )
        return ExecutionResult(
            success=False,
            result_meta=att.result_meta,
            retry_requested=att.retry_requested,
            retry_error=att.retry_error,
        )

    def probe_orphan_lock(self, uid: str) -> bool:
        """非阻塞探测执行锁。True 表示无孤儿持锁可安全执行；False 表示孤儿活跃需 defer。"""
        return lockfile.probe_lock(self.ipc_dir, uid)

    def drain_active_signals(self, uids: Iterable[str]) -> List[Tuple[str, str, float]]:
        """原子排空所有在途任务追加的 suspend 信号。"""
        signals: List[Tuple[str, str, float]] = []
        for uid in uids:
            for r_name, secs in self.journal.drain_signals(uid):
                signals.append((uid, r_name, secs))
        return signals

    def drain_all_signals(self) -> List[Tuple[str, str, float]]:
        """启动期清扫 ipc_dir 全部残留 suspend 信号（含 .draining 孤儿回收）。"""
        return self.journal.drain_all_signals()

    def abort_in_flight(self, handles: Sequence[JobHandle]) -> AbortOutcome:
        """TOCTOU 闭环中止：排空信号 -> 初查分类 -> 进程终止 -> 重探测闭环 -> 残留清理。"""
        if not handles:
            return AbortOutcome(completed=[], cancelled=[])

        done_pairs: List[Tuple[JobHandle, ExecutionResult]] = []
        pending_handles: List[JobHandle] = []

        for h in handles:
            incarnation = getattr(h, "incarnation", None)
            res_p = self.journal.result_path(h.uid, incarnation)
            if res_p.exists():
                raw_res = self._read_authenticated_result(res_p)
                if (
                    raw_res is not None
                    and isinstance(raw_res, dict)
                    and "status" in raw_res
                    and raw_res.get("status") != "interrupted"
                ):
                    decoded = _decode_ipc_result(
                        raw_res, None, h.job, self.ipc_dir,
                        output_roots=getattr(self, "output_roots", None),
                    )
                    # 不变式：结果文件已原子落盘 ⇒ 本执行体的信号追加全部
                    # 早于落盘（record_signal 只发生在 handler 执行期内），
                    # 此刻排空无并发写者、无损；非 success payload 不携带
                    # 挂起字段，缺此步时 done 对经完成机器的收尾清理会把
                    # 「先排空阶段之后写入」的信号文件未读删除。
                    try:
                        decoded.resource_suspensions = (
                            list(decoded.resource_suspensions)
                            + self.journal.drain_signals(h.uid)
                        )
                    except Exception as e:
                        logger.warning(f"Failed to salvage signals for {h.uid}: {e}")
                    done_pairs.append((h, decoded))
                    continue
            pending_handles.append(h)

        if pending_handles:
            self.finalize_processes(pending_handles)

        # 杀进程后补排空（不变式：收尾排空必须晚于进程终止）。abort 序列
        # 「先排空后杀」窗口内 worker 终生前写入的 suspend 信号，唯一可靠
        # 捞回点是进程死亡后的最终排空（无并发写者，排空无损）；缺此步时
        # cancelled 分支的半成品清理会把信号文件连同信号一起删除。
        salvaged: List[Tuple[str, str, float]] = []
        truly_cancelled: List[JobHandle] = []
        for h in pending_handles:
            incarnation = getattr(h, "incarnation", None)
            res_p = self.journal.result_path(h.uid, incarnation)
            if res_p.exists():
                raw_res = self._read_authenticated_result(res_p)
                if (
                    raw_res is not None
                    and isinstance(raw_res, dict)
                    and "status" in raw_res
                    and raw_res.get("status") != "interrupted"
                ):
                    decoded = _decode_ipc_result(
                        raw_res, None, h.job, self.ipc_dir,
                        output_roots=getattr(self, "output_roots", None),
                    )
                    try:
                        decoded.resource_suspensions = (
                            list(decoded.resource_suspensions)
                            + self.journal.drain_signals(h.uid)
                        )
                    except Exception as e:
                        logger.warning(f"Failed to salvage signals for {h.uid}: {e}")
                    done_pairs.append((h, decoded))
                    continue
            try:
                for r_name, secs in self.journal.drain_signals(h.uid):
                    salvaged.append((h.uid, r_name, secs))
            except Exception as e:
                logger.warning(f"Failed to salvage signals for {h.uid}: {e}")
            self.cleanup_artifacts(h.uid, mode=ArtifactCleanupMode.FAILURE_OR_RETRY)
            truly_cancelled.append(h)

        return AbortOutcome(
            completed=done_pairs,
            cancelled=truly_cancelled,
            salvaged_signals=salvaged,
        )

    def cleanup_in_flight(self, handles: Sequence[JobHandle]) -> None:
        """主循环异常退出时 kill + join 所有残留 in-flight 子进程并清理 IPC。"""
        for handle in handles:
            try:
                self._finalize_process(handle.process)
                self.journal.cleanup_ipc_files(handle.uid, handle.incarnation)
            except Exception as e:
                logger.error(f"Error cleaning up in-flight job {handle.uid}: {e}")


    def finalize_processes(self, handles: Sequence[JobHandle]) -> None:
        """只 kill + join 残留子进程，不删 IPC 文件。"""
        for handle in handles:
            try:
                self._finalize_process(handle.process)
            except Exception as e:
                logger.error(f"Error finalizing in-flight job {handle.uid}: {e}")

    def cleanup_artifacts(self, uid: str, *, mode: ArtifactCleanupMode) -> None:
        """统一收敛产物与 IPC 临时文件的生命周期清理。"""
        self.journal.cleanup(uid, mode=mode)

    def read_declared_inputs(self, uid: str) -> List[dict]:
        """读取任务声明的输入清单。"""
        try:
            return self.journal.read_inputs(uid)
        except Exception:
            return []


__all__ = [
    "AbortOutcome",
    "ArtifactCleanupMode",
    "ExecutionChannel",
    "ExecutionResult",
    "JobHandle",
    "WorkerLaunchSpec",
    "_decode_ipc_result",
    "_decode_raw_result",
    "_encode_raw_result",
    "_mp_worker_wrapper",
    "_normalize_handler_result",
]
