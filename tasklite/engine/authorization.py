"""主进程文件声明与三类授权等待统一协调器。

统一收敛主进程侧全部三类文件授权等待生命周期：
1. 派发前预声明文件授权等待；
2. 运行期子进程动态文件声明校验、授权等待、前置清理与回复；
3. 执行收尾清理期（成功/失败/重试清理）授权等待与受阻进程退出等待。

核心不变式：
- ``StopMode != NONE`` → ``AuthorizationStatus.STOPPED`` 在本模块单点收敛；
- ``has_pending_authorization`` 为事件泵等待决策提供唯一授权等待查询；
- 动态声明与收尾等待期间的超时暂停/恢复按执行身份追踪，不污染 ``JobHandle`` 值对象；
- 内部短临界区锁（``self._lock``）仅保护内存状态跃迁与停机态二次复核，严禁持锁执行
  外部 authorizer 回调、文件系统 I/O、进程终止或管道回复，避免跨线程 ``request_stop()`` 死锁。
"""

from __future__ import annotations

import enum
import logging
import threading
import time
import traceback
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from ..exceptions import TaskLiteError
from ..models.artifact import (
    FileSpec,
    merge_named_file_spec,
    validate_file_name,
)
from ..models.job import Job
from .types import AuthorizationOutcome, AuthorizationStatus, JobHandle, StopMode

if TYPE_CHECKING:
    from .channel import ExecutionChannel, ExecutionResult
    from .in_flight import InFlightJob, InFlightTracker
    from .session import RunSession
    from .store import StateStore

logger = logging.getLogger("tasklite")

_CLEANUP_ERRORS = (OSError, ValueError, NotImplementedError)


class CleanupAuthorizationDecision(str, enum.Enum):
    """收尾清理授权评估的显式结论（替代控制流异常）。"""

    READY = "ready"
    PENDING = "pending"


@dataclass
class PendingDeclaration:
    """一次等待主进程决定的动态文件声明。"""

    request_id: str
    name: str
    source: FileSpec
    bound: FileSpec
    resolved_path: str
    status: AuthorizationStatus = AuthorizationStatus.PENDING


@dataclass(frozen=True)
class _PendingDispatchAuthorization:
    """一次派发前待决授权的追踪键。"""

    run_id: str | None
    job_uid: str
    file_name: str


@dataclass
class PendingCleanup:
    """一次收尾清理待决授权（或等待受阻进程退出）的暂挂项。"""

    entry: "InFlightJob"
    result: "ExecutionResult"
    prepared: Any


class AuthorizationCoordinator:
    """统一承接三类主进程文件授权等待的登记、复查、停机解除与超时暂挂。"""

    def __init__(
        self,
        *,
        store: "StateStore",
        channel: "ExecutionChannel",
        in_flight: "InFlightTracker",
        session: "RunSession",
    ) -> None:
        self._store = store
        self._channel = channel
        self._in_flight = in_flight
        self._session = session
        # 短临界区锁：仅保护状态字段原子跃迁与停机复核，绝不包裹外部回调或 I/O。
        self._lock = threading.RLock()

        # 三类授权等待与按 execution_id 追踪的运行期授权/超时状态
        self._dispatch_pending: _PendingDispatchAuthorization | None = None
        self._pending_declarations: dict[str, tuple[JobHandle, PendingDeclaration]] = {}
        self._declaration_outcomes: dict[str, AuthorizationOutcome] = {}
        self._paused_remaining: dict[str, float] = {}
        self._pending_cleanups: dict[str, PendingCleanup] = {}

    # ── 统一查询与状态清理 ────────────────────────────────────────────

    @property
    def has_pending_authorization(self) -> bool:
        """是否存在任意一类等待外部 authorizer 或受阻退出的授权暂挂。"""
        with self._lock:
            return (
                self._dispatch_pending is not None
                or bool(self._pending_declarations)
                or bool(self._pending_cleanups)
            )

    @property
    def has_in_flight_authorization_wait(self) -> bool:
        """是否存在在途作业的动态声明或收尾清理授权等待（用于暂停新派发）。"""
        with self._lock:
            return bool(self._pending_declarations) or bool(self._pending_cleanups)

    @property
    def has_dispatch_pending(self) -> bool:
        """是否存在派发前授权等待。"""
        with self._lock:
            return self._dispatch_pending is not None

    def has_pending_cleanup(self, job_uid: str) -> bool:
        """指定作业是否已收割并暂挂于收尾清理授权队列。"""
        with self._lock:
            return job_uid in self._pending_cleanups

    def pending_cleanups(self) -> list[PendingCleanup]:
        """返回当前全部收尾清理暂挂项的快照列表。"""
        with self._lock:
            return list(self._pending_cleanups.values())

    def is_waiting_declaration(self, handle: JobHandle) -> bool:
        """指定在途句柄是否正等待动态文件声明授权。"""
        with self._lock:
            item = self._pending_declarations.get(self._execution_key(handle))
            return item is not None and item[1].status is AuthorizationStatus.PENDING

    def get_declaration_outcome(self, handle: JobHandle) -> AuthorizationOutcome | None:
        """查询指定在途句柄是否已产出动态声明终态授权结果。"""
        with self._lock:
            return self._declaration_outcomes.get(self._execution_key(handle))

    def is_timeout_paused(self, handle: JobHandle) -> bool:
        """指定在途句柄是否因授权等待而暂停执行超时计时。"""
        with self._lock:
            return self._execution_key(handle) in self._paused_remaining

    def get_paused_remaining(self, handle: JobHandle) -> float | None:
        """查询指定在途句柄暂停时保存的剩余执行超时秒数。"""
        with self._lock:
            return self._paused_remaining.get(self._execution_key(handle))

    def _pause_timeout_locked(self, handle: JobHandle) -> None:
        key = self._execution_key(handle)
        if key not in self._paused_remaining:
            self._paused_remaining[key] = max(
                0.0, handle.timeout_at_monotonic - time.monotonic()
            )

    def _resume_timeout_locked(self, handle: JobHandle) -> None:
        key = self._execution_key(handle)
        remaining = self._paused_remaining.pop(key, None)
        if remaining is not None:
            handle.timeout_at_monotonic = time.monotonic() + remaining

    def pause_timeout(self, handle: JobHandle) -> None:
        """等待授权期间按执行身份暂停超时计时（幂等）。"""
        with self._lock:
            self._pause_timeout_locked(handle)

    def resume_timeout(self, handle: JobHandle) -> None:
        """授权解除后恢复执行超时计时（幂等）。"""
        with self._lock:
            self._resume_timeout_locked(handle)

    def clear_dispatch_pending(self) -> None:
        """清除派发前授权等待记录。"""
        with self._lock:
            self._dispatch_pending = None

    def clear_execution(
        self,
        execution_id: str | None,
        *,
        job_uid: str | None = None,
    ) -> None:
        """在途执行结算完成后清理对应授权与超时暂停状态。"""
        with self._lock:
            for key in (execution_id, job_uid):
                if key:
                    self._pending_declarations.pop(key, None)
                    self._declaration_outcomes.pop(key, None)
                    self._paused_remaining.pop(key, None)
            if job_uid:
                self._pending_cleanups.pop(job_uid, None)

    def clear_all(self) -> None:
        """清空全部三类授权等待与暂停状态（用于异常中止或新一轮重置）。"""
        with self._lock:
            self._dispatch_pending = None
            self._pending_declarations.clear()
            self._declaration_outcomes.clear()
            self._paused_remaining.clear()
            self._pending_cleanups.clear()

    def notify_stop(self) -> None:
        """停机请求到达时在短锁内原子清零派发前等待并收敛动态声明为 STOPPED，锁外终止进程。"""
        to_terminate: list[tuple[JobHandle, str, str]] = []
        with self._lock:
            self._dispatch_pending = None
            for _key, (handle, request) in list(self._pending_declarations.items()):
                entry = self._in_flight.get(handle.job_uid)
                if entry is None:
                    continue
                message = self._stopped_message(request.resolved_path)
                if self._transition_declaration_terminal_locked(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.STOPPED,
                    message,
                ):
                    to_terminate.append((handle, request.resolved_path, message))
        for handle, path, message in to_terminate:
            logger.info(
                f"FILE AUTHORIZATION STOPPED: {handle.job_uid} {path} ({message})"
            )
            self._channel.terminate_process(handle)

    @staticmethod
    def _execution_key(handle: JobHandle) -> str:
        return handle.execution_id if handle.execution_id else handle.job_uid

    def _is_stopping(self) -> bool:
        """停机态判定的唯一真相源。"""
        return self._session.stop_mode is not StopMode.NONE

    @staticmethod
    def _stopped_message(path: str) -> str:
        return f"authorization wait stopped for {path}"

    @staticmethod
    def _denied_message(path: str) -> str:
        return f"authorization denied for {path}"

    def _stopped_outcome(self, path: str) -> AuthorizationOutcome:
        return AuthorizationOutcome(
            status=AuthorizationStatus.STOPPED,
            message=self._stopped_message(path),
            path=path,
        )

    def _denied_outcome(self, path: str) -> AuthorizationOutcome:
        return AuthorizationOutcome(
            status=AuthorizationStatus.DENIED,
            message=self._denied_message(path),
            path=path,
        )

    def _set_dispatch_pending_if_allowed(
        self,
        pending: _PendingDispatchAuthorization,
    ) -> bool:
        """短锁内二次复核停机态后再写入派发前等待，杜绝与 notify_stop 竞态产生幽灵等待。"""
        with self._lock:
            if self._is_stopping():
                self._dispatch_pending = None
                return False
            self._dispatch_pending = pending
            return True

    # ── 类型 1：派发前清理授权等待 ────────────────────────────────────

    def _resolve_job_files(self, job: Job) -> dict[str, FileSpec]:
        """按统一 PathPolicy 单次原子解析作业的全部预声明文件。"""
        return {
            name: self._store.resolve_file_spec(spec, depends_on=job.depends_on)
            for name, spec in job.files.items()
        }

    def poll_dispatch_authorization(self) -> AuthorizationStatus:
        """复查上拍挂起的派发前授权。

        返回：
        - ``APPROVED``：无挂起或已放行，可继续扫描派发；
        - ``PENDING``：仍在等待外部决策；
        - ``DENIED``：拒绝并已将作业及其下游临时排除；
        - ``PREPARATION_FAILED``：重校验失败并已拒绝提交；
        - ``STOPPED``：已进入停机态，结束派发。
        """
        with self._lock:
            if self._is_stopping():
                self._dispatch_pending = None
                return AuthorizationStatus.STOPPED
            pending = self._dispatch_pending
            if pending is None:
                return AuthorizationStatus.APPROVED
            self._dispatch_pending = None

        store = self._store
        if pending.run_id != self._session.run_id or pending.job_uid not in store.state.queue_uids:
            return AuthorizationStatus.APPROVED

        pending_idx = next(
            index
            for index, snapshot in enumerate(store.state.queue)
            if Job.from_dict(snapshot).uid == pending.job_uid
        )
        try:
            job = Job.from_dict(store.state.queue[pending_idx])
            pending_files = self._resolve_job_files(job)
            path = pending_files[pending.file_name].path
            assert path is not None
        except Exception as exc:
            snapshot = store.state.pop_job(pending_idx)
            store.apply_failure(
                pending.job_uid,
                {
                    "error_message": str(exc),
                    "exception": {
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "traceback": traceback.format_exc(),
                    },
                },
                job_snapshot=snapshot,
                cascade=True,
            )
            return AuthorizationStatus.PREPARATION_FAILED

        authorized = store.check_or_request_authorization(path)
        if authorized is None:
            if self._set_dispatch_pending_if_allowed(pending):
                return AuthorizationStatus.PENDING
            return AuthorizationStatus.STOPPED
        if self._is_stopping():
            return AuthorizationStatus.STOPPED
        if authorized is False:
            self._session.temporarily_exclude_chain(pending.job_uid, store.state)
            return AuthorizationStatus.DENIED
        return AuthorizationStatus.APPROVED

    def authorize_dispatch_files(
        self,
        job_uid: str,
        resolved_files: Mapping[str, FileSpec],
    ) -> AuthorizationStatus:
        """派发前逐项检查预声明文件的删除授权，未决则在短锁停机复核后登记等待槽。"""
        store = self._store
        for fname, fspec in resolved_files.items():
            if not (fspec.delete_existing or fspec.delete_on_success or fspec.delete_on_failure):
                continue
            assert fspec.path is not None
            authorized = store.check_or_request_authorization(fspec.path)
            if authorized is None:
                pending = _PendingDispatchAuthorization(
                    run_id=self._session.run_id,
                    job_uid=job_uid,
                    file_name=fname,
                )
                if self._set_dispatch_pending_if_allowed(pending):
                    return AuthorizationStatus.PENDING
                return AuthorizationStatus.STOPPED
            if self._is_stopping():
                return AuthorizationStatus.STOPPED
            if authorized is False:
                self._session.temporarily_exclude_chain(job_uid, store.state)
                return AuthorizationStatus.DENIED
        return AuthorizationStatus.APPROVED

    # ── 类型 2：运行期动态文件声明授权 ────────────────────────────────

    def handle_declaration(
        self,
        handle: JobHandle,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """主进程动态文件声明仲裁出口：验证引用、授权检查、前置清理并回送路径。"""
        raw_req_id = payload.get("request_id")
        if not isinstance(raw_req_id, str) or not raw_req_id:
            return {
                "request_id": "",
                "status": "error",
                "error": "request_id must be a non-empty str",
            }
        req_id = raw_req_id
        name = payload.get("name", "")
        spec_data = payload.get("spec", {})
        entry = self._in_flight.get(handle.job_uid)
        if entry is None or not self._declaration_identity_matches(handle, entry):
            return {
                "request_id": req_id,
                "status": "error",
                "error": f"Job {handle.job_uid} not in flight",
            }
        exec_key = self._execution_key(handle)
        with self._lock:
            if exec_key in self._declaration_outcomes:
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"Job {handle.job_uid} execution is no longer accepting declarations",
                }
            if exec_key in self._pending_declarations:
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"Job {handle.job_uid} already has a pending declaration",
                }

        try:
            spec = FileSpec.from_dict(spec_data)
            validated_name = validate_file_name(name)
        except Exception as exc:
            return {"request_id": req_id, "status": "error", "error": str(exc)}

        # 1. 幂等与冲突检查
        try:
            with self._lock:
                existing_sources = dict(entry.active_file_sources)
                existing_files = dict(entry.active_files)
            merged, is_new = merge_named_file_spec(
                existing_sources,
                validated_name,
                spec,
                base_dir=self._store.base_dir,
                depends_on=handle.job.depends_on,
            )
        except Exception as exc:
            return {"request_id": req_id, "status": "error", "error": str(exc)}
        source_spec = merged[validated_name]
        if not is_new:
            existing = existing_files.get(validated_name)
            if existing is None or existing.path is None:
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": f"File declaration {validated_name!r} has no resolved path",
                }
            return {
                "request_id": req_id,
                "status": "ok",
                "resolved_path": existing.path,
            }

        # 2. 单次原子解析（上游引用、安全边界、存在性判定与路径绑定）
        needs_authorization = (
            source_spec.delete_existing
            or source_spec.delete_on_success
            or source_spec.delete_on_failure
        )
        if not needs_authorization:
            try:
                bound_spec = self._store.resolve_file_spec(
                    source_spec,
                    depends_on=handle.job.depends_on,
                    enforce_safety=False,
                )
            except Exception as exc:
                return {
                    "request_id": req_id,
                    "status": "error",
                    "error": str(exc),
                }
            assert bound_spec.path is not None
            resolved_path = bound_spec.path
            logger.info(
                f"FILE DECLARATION: {handle.job_uid} requested "
                f"{validated_name!r} at {resolved_path}"
            )
            request = PendingDeclaration(
                request_id=req_id,
                name=validated_name,
                source=source_spec,
                bound=bound_spec,
                resolved_path=resolved_path,
                status=AuthorizationStatus.APPROVED,
            )
            with self._lock:
                self._record_declaration(entry, request)
            logger.info(
                f"FILE DECLARATION APPROVED: {handle.job_uid} {resolved_path}"
            )
            return self._declaration_reply(request)

        try:
            bound_spec = self._store.resolve_file_spec(
                source_spec,
                depends_on=handle.job.depends_on,
                enforce_safety=True,
            )
        except (TaskLiteError, FileNotFoundError) as exc:
            return {
                "request_id": req_id,
                "status": "error",
                "error": str(exc),
            }
        except _CLEANUP_ERRORS as exc:
            error = {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            }
            fallback_path, fallback_bound = self._resolve_fallback_declaration_target(source_spec)
            request = PendingDeclaration(
                request_id=req_id,
                name=validated_name,
                source=source_spec,
                bound=fallback_bound,
                resolved_path=fallback_path,
            )
            with self._lock:
                if exec_key in self._declaration_outcomes or exec_key in self._pending_declarations:
                    return None
                self._pending_declarations[exec_key] = (handle, request)
                self._pause_timeout_locked(handle)
            self._finish_declaration(
                handle,
                entry,
                request,
                AuthorizationStatus.PREPARATION_FAILED,
                error["message"],
                error,
            )
            return None

        assert bound_spec.path is not None
        validated_path = bound_spec.path
        logger.info(
            f"FILE DECLARATION: {handle.job_uid} requested "
            f"{validated_name!r} at {validated_path}"
        )
        request = PendingDeclaration(
            request_id=req_id,
            name=validated_name,
            source=source_spec,
            bound=bound_spec,
            resolved_path=validated_path,
        )
        with self._lock:
            if exec_key in self._declaration_outcomes or exec_key in self._pending_declarations:
                return None
            self._pending_declarations[exec_key] = (handle, request)
            self._pause_timeout_locked(handle)
            stopping = self._is_stopping()
        if stopping:
            self._finish_declaration(
                handle,
                entry,
                request,
                AuthorizationStatus.STOPPED,
                self._stopped_message(validated_path),
            )
            return None
        logger.info(
            f"FILE AUTHORIZATION WAIT: {handle.job_uid} {validated_path}"
        )
        authorized = self._store.check_or_request_authorization(validated_path)
        if authorized is None:
            if self._is_stopping():
                self._finish_declaration(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.STOPPED,
                    self._stopped_message(validated_path),
                )
            return None
        return self._apply_declaration_decision(handle, entry, request, authorized)

    def poll_pending_declarations(self) -> None:
        """轮询处于等待中的动态文件声明授权；若已决策则恢复执行或终止。"""
        with self._lock:
            snapshot = list(self._pending_declarations.items())
        for exec_key, (handle, request) in snapshot:
            with self._lock:
                current = self._pending_declarations.get(exec_key)
                if (
                    current is None
                    or current[1] is not request
                    or request.status is not AuthorizationStatus.PENDING
                ):
                    continue
                entry = self._in_flight.get(handle.job_uid)
                if entry is None or not self._declaration_identity_matches(handle, entry):
                    self._pending_declarations.pop(exec_key, None)
                    continue
                stopping = self._is_stopping()
                current_path = request.resolved_path
                delete_existing = request.bound.delete_existing

            if stopping:
                self._finish_declaration(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.STOPPED,
                    self._stopped_message(current_path),
                )
                continue

            validated_path, error = self._validate_declaration_target(
                current_path, delete_existing=delete_existing
            )
            if error is not None:
                self._finish_declaration(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.PREPARATION_FAILED,
                    error["message"],
                    error,
                )
                continue
            if validated_path != current_path:
                rebound = request.bound._bind_output_path(validated_path)
                with self._lock:
                    current = self._pending_declarations.get(exec_key)
                    if (
                        current is None
                        or current[1] is not request
                        or request.status is not AuthorizationStatus.PENDING
                    ):
                        continue
                    request.bound = rebound
                    request.resolved_path = validated_path

            authorized = self._store.check_or_request_authorization(validated_path)
            if authorized is not None:
                reply = self._apply_declaration_decision(
                    handle, entry, request, authorized
                )
                if reply is None:
                    continue
                reply_sender = self._channel.get_reply_sender(handle.execution_id)
                if reply_sender is not None:
                    try:
                        reply_sender.send_reply(reply)
                    except OSError:
                        pass
            elif self._is_stopping():
                self._finish_declaration(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.STOPPED,
                    self._stopped_message(validated_path),
                )

    def _resolve_fallback_declaration_target(
        self,
        source_spec: FileSpec,
    ) -> tuple[str, FileSpec]:
        if source_spec.is_output_ref and source_spec.producer_uid and source_spec.output_name:
            desc = (
                self._store.state.successes.get(source_spec.producer_uid, {})
                .get("artifacts", {})
                .get(source_spec.output_name, {})
            )
            fallback_path = str(desc.get("path", "")) if isinstance(desc, dict) else ""
        else:
            fallback_path = source_spec.path or ""
        fallback_bound = source_spec
        if fallback_path:
            try:
                fallback_path = self._store.path_policy.inspect(
                    fallback_path, mode="lookup"
                ).canonical_path
            except _CLEANUP_ERRORS:
                pass
            try:
                fallback_bound = source_spec._bind_output_path(fallback_path)
            except (TaskLiteError, ValueError):
                fallback_bound = source_spec
        return fallback_path, fallback_bound

    def _declaration_identity_matches(self, handle: JobHandle, entry: "InFlightJob") -> bool:
        """请求句柄必须与当前在飞执行使用同一 job、run、attempt 与 execution 身份。"""
        if entry.handle is not handle or entry.job_uid != handle.job_uid:
            return False
        if not handle.execution_id or entry.execution_id != handle.execution_id:
            return False
        if handle.run_id is not None and entry.run_id != handle.run_id:
            return False
        if handle.attempt_no is not None and entry.attempt_no != handle.attempt_no:
            return False
        if self._session.run_id is not None and entry.run_id != self._session.run_id:
            return False
        return True

    @staticmethod
    def _record_declaration(entry: "InFlightJob", request: PendingDeclaration) -> None:
        entry.active_file_sources[request.name] = request.source
        entry.active_files[request.name] = request.bound

    def _validate_declaration_target(
        self,
        resolved_path: str,
        *,
        delete_existing: bool,
    ) -> tuple[str, dict[str, str] | None]:
        try:
            return (
                self._store.path_policy.inspect(
                    resolved_path,
                    mode="pre_delete" if delete_existing else "strict",
                ).canonical_path,
                None,
            )
        except _CLEANUP_ERRORS as exc:
            return resolved_path, {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            }

    def _apply_declaration_decision(
        self,
        handle: JobHandle,
        entry: "InFlightJob",
        request: PendingDeclaration,
        authorized: bool,
    ) -> dict[str, Any] | None:
        exec_key = self._execution_key(handle)
        with self._lock:
            pending = self._pending_declarations.get(exec_key)
            if (
                pending is None
                or pending[1] is not request
                or request.status is not AuthorizationStatus.PENDING
                or exec_key in self._declaration_outcomes
            ):
                return None
            stopping = self._is_stopping()
            resolved_path = request.resolved_path
            delete_existing = request.bound.delete_existing

        if stopping:
            message = self._stopped_message(resolved_path)
            if self._finish_declaration(
                handle, entry, request, AuthorizationStatus.STOPPED, message
            ):
                return {
                    "request_id": request.request_id,
                    "status": "stopped",
                    "error": message,
                }
            return None

        if authorized is False:
            message = self._denied_message(resolved_path)
            if self._finish_declaration(
                handle, entry, request, AuthorizationStatus.DENIED, message
            ):
                return {
                    "request_id": request.request_id,
                    "status": "denied",
                    "error": message,
                }
            return None

        if delete_existing:
            try:
                self._store.path_policy.delete(
                    resolved_path,
                    allow_broken_leaf_symlink=True,
                    authorize=self._store.backend.is_path_authorized,
                )
            except _CLEANUP_ERRORS as exc:
                error = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                }
                message = f"Artifact preparation failed for {resolved_path}: {exc}"
                self._finish_declaration(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.PREPARATION_FAILED,
                    message,
                    error,
                )
                return None

        # 短锁内二次复核：若锁外 I/O 期间 notify_stop() 已中止或已进入停机态，绝不覆盖为 APPROVED。
        approved_reply: dict[str, Any] | None = None
        approved_path = ""
        stopped_transition = False
        stopped_path = ""
        stopped_msg = ""
        with self._lock:
            pending = self._pending_declarations.get(exec_key)
            if (
                pending is None
                or pending[1] is not request
                or request.status is not AuthorizationStatus.PENDING
                or exec_key in self._declaration_outcomes
            ):
                return None
            if self._is_stopping():
                stopped_path = request.resolved_path
                stopped_msg = self._stopped_message(stopped_path)
                stopped_transition = self._transition_declaration_terminal_locked(
                    handle,
                    entry,
                    request,
                    AuthorizationStatus.STOPPED,
                    stopped_msg,
                )
            else:
                request.status = AuthorizationStatus.APPROVED
                self._pending_declarations.pop(exec_key, None)
                self._resume_timeout_locked(handle)
                self._record_declaration(entry, request)
                approved_path = request.resolved_path
                approved_reply = self._declaration_reply(request)

        if approved_reply is not None:
            logger.info(
                f"FILE AUTHORIZATION APPROVED: {handle.job_uid} {approved_path}"
            )
            return approved_reply
        if stopped_transition:
            logger.info(
                f"FILE AUTHORIZATION STOPPED: "
                f"{handle.job_uid} {stopped_path} ({stopped_msg})"
            )
            self._channel.terminate_process(handle)
        return None

    def _transition_declaration_terminal_locked(
        self,
        handle: JobHandle,
        entry: "InFlightJob",
        request: PendingDeclaration,
        status: AuthorizationStatus,
        message: str,
        exception: dict[str, str] | None = None,
    ) -> bool:
        """短锁内原子完成动态声明向终态的一次性跃迁（防双线程撕裂与双写）。"""
        exec_key = self._execution_key(handle)
        pending = self._pending_declarations.get(exec_key)
        if (
            pending is None
            or pending[1] is not request
            or request.status is not AuthorizationStatus.PENDING
            or exec_key in self._declaration_outcomes
        ):
            return False
        request.status = status
        self._pending_declarations.pop(exec_key, None)
        self._declaration_outcomes[exec_key] = AuthorizationOutcome(
            status=status,
            message=message,
            path=request.resolved_path,
            exception=exception,
        )
        self._resume_timeout_locked(handle)
        self._record_declaration(entry, request)
        return True

    def _finish_declaration(
        self,
        handle: JobHandle,
        entry: "InFlightJob",
        request: PendingDeclaration,
        status: AuthorizationStatus,
        message: str,
        exception: dict[str, str] | None = None,
    ) -> bool:
        with self._lock:
            transitioned = self._transition_declaration_terminal_locked(
                handle, entry, request, status, message, exception
            )
            resolved_path = request.resolved_path
        if not transitioned:
            return False
        logger.info(
            f"FILE AUTHORIZATION {status.value.upper()}: "
            f"{handle.job_uid} {resolved_path} ({message})"
        )
        self._channel.terminate_process(handle)
        return True

    @staticmethod
    def _declaration_reply(request: PendingDeclaration) -> dict[str, Any]:
        return {
            "request_id": request.request_id,
            "status": "ok",
            "resolved_path": request.resolved_path,
        }

    # ── 类型 3：收尾清理期授权等待 ────────────────────────────────────

    def evaluate_cleanup(
        self,
        entry: "InFlightJob | None",
        result: "ExecutionResult",
        active_files: Mapping[str, FileSpec],
    ) -> CleanupAuthorizationDecision:
        """评估执行收尾清理所需的全部路径授权；若待决或受阻进程仍在运行则返回 PENDING。"""
        handle = entry.handle if entry is not None else None
        waiting = entry is not None and self.has_pending_cleanup(entry.job_uid)
        if result.authorization_outcome is not None:
            if (
                waiting
                and handle is not None
                and self._channel.is_handle_alive(handle)
            ):
                return CleanupAuthorizationDecision.PENDING
            return CleanupAuthorizationDecision.READY

        policy = self._store.path_policy
        for spec in active_files.values():
            requested = spec.delete_on_success if result.success else spec.delete_on_failure
            if not requested or spec.path is None:
                continue
            try:
                if not policy.inspect(spec.path, mode="strict", verify_unchanged=True).exists:
                    continue
            except _CLEANUP_ERRORS:
                continue

            if self._is_stopping() and waiting:
                result.authorization_outcome = self._stopped_outcome(spec.path)
            else:
                decision = self._store.check_or_request_authorization(spec.path)
                if self._is_stopping() and (waiting or decision is None):
                    result.authorization_outcome = self._stopped_outcome(spec.path)
                elif decision is True:
                    continue
                elif decision is None:
                    return CleanupAuthorizationDecision.PENDING
                else:
                    result.authorization_outcome = self._denied_outcome(spec.path)

            if handle is not None:
                self._channel.terminate_process(handle)
                if self._channel.is_handle_alive(handle):
                    return CleanupAuthorizationDecision.PENDING
            return CleanupAuthorizationDecision.READY

        # 后续询问回调可能改变此前目标，整组复核通过后再交付实际清理。
        for spec in active_files.values():
            requested = spec.delete_on_success if result.success else spec.delete_on_failure
            if requested and spec.path and not self._store.is_path_authorized(spec.path):
                try:
                    present = policy.inspect(spec.path, mode="strict", verify_unchanged=True).exists
                except _CLEANUP_ERRORS:
                    continue
                if present:
                    return CleanupAuthorizationDecision.PENDING
        return CleanupAuthorizationDecision.READY

    def defer_cleanup(
        self,
        entry: "InFlightJob",
        result: "ExecutionResult",
        prepared: Any,
    ) -> None:
        """将尚未完成授权或待进程退出的收尾项暂挂，并暂停其执行超时计时。"""
        with self._lock:
            self._pending_cleanups[entry.job_uid] = PendingCleanup(
                entry=entry,
                result=result,
                prepared=prepared,
            )
            if entry.handle is not None:
                self._pause_timeout_locked(entry.handle)


__all__ = [
    "AuthorizationCoordinator",
    "CleanupAuthorizationDecision",
    "PendingCleanup",
    "PendingDeclaration",
]
