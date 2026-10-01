"""v2 执行通道测试：spawn 兜底、看门狗、结果认证、解码防御、fencing 认领与中止。

以受控进程桩与内联 worker 包装确定性复现时序（无真实多进程竞速）；
端到端（派发机器/完成机器装配后的 run 主循环）由后续门面阶段承接。
"""

import json
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from tasklite.v2.engine.channel import (
    ExecutionChannel,
    WorkerLaunchSpec,
    _decode_ipc_result,
    _mp_worker_wrapper,
    _normalize_handler_result,
)
from tasklite.v2.engine.types import JobHandle
from tasklite.v2.exceptions import RateLimitHit
from tasklite.v2.models.context import JobContext
from tasklite.v2.models.job import Job
from tasklite.v2.utils.encoding import safe_uid_filename
from tasklite.v2.utils.ipc import ArtifactJournal, encode_raw_result

_INCARNATION = "deadbeefdeadbeefdeadbeefdeadbeef.1"
_TOKEN = "b" * 64


def make_ctx(job: Job) -> JobContext:
    """最小 JobContext 装配（声明/挂起默认空，ipc_dir 由 spawn 回填）。"""
    return JobContext(job, wall_keys=set(), failed_keys=set(), cursors={})


def make_spec(
    job: Job,
    handler,
    ipc_dir,
    incarnation: str = _INCARNATION,
    *,
    result_token: str | None = None,
) -> WorkerLaunchSpec:
    return WorkerLaunchSpec(
        handler=handler,
        job=job,
        task_ctx=make_ctx(job),
        incarnation=incarnation,
        ipc_dir=str(ipc_dir) if ipc_dir is not None else None,
        timeout=60.0,
        result_token=result_token,
    )


class TestExecutionChannelBasics:
    def test_init_and_directory_creation(self, tmp_path):
        ipc_dir = tmp_path / "ipc"
        assert not ipc_dir.exists()
        channel = ExecutionChannel(ipc_dir)
        assert ipc_dir.exists()
        assert channel.ipc_dir == str(ipc_dir)

    def test_probe_orphan_lock(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        assert channel.probe_orphan_lock("job_probe") is True

    def test_drain_active_signals(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        channel.journal.record_signal("job_signals", "gpu", 15.0)
        channel.journal.record_signal("job_signals", "api", 3.0)

        signals = channel.drain_active_signals(["job_signals", "job_other"])
        assert ("job_signals", "gpu", 15.0) in signals
        assert ("job_signals", "api", 3.0) in signals

    def test_read_declared_inputs(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        channel.journal.record_input_entry(
            "job_inputs", {"kind": "file", "path": "/tmp/a.txt", "size": 100, "mtime_ns": 1000}
        )
        inputs = channel.read_declared_inputs("job_inputs")
        assert len(inputs) == 1
        assert inputs[0]["path"] == "/tmp/a.txt"
        assert inputs[0]["size"] == 100

    def test_journal_property_rebuilds_on_ipc_dir_change(self, tmp_path):
        channel = ExecutionChannel()
        assert channel.journal.ipc_dir is None
        channel.ipc_dir = str(tmp_path)
        assert channel.journal.ipc_dir == str(tmp_path)


class TestExecutionChannelArtifactCleanup:
    def test_cleanup_pre_submit(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        uid = "job_clean"
        channel.journal.record_input_entry(uid, {"kind": "file", "path": "/tmp/x"})
        channel.journal.record_output(uid, "/tmp/y", cleanup=False, kind="file")
        channel.journal.record_signal(uid, "gpu", 10.0)

        from tasklite.v2.utils.ipc import ArtifactCleanupMode

        channel.cleanup_artifacts(uid, mode=ArtifactCleanupMode.PRE_SUBMIT)

        assert not channel.journal.inputs_path(uid).exists()
        assert not channel.journal.outputs_path(uid).exists()
        assert not channel.journal.signals_path(uid).exists()

    def test_cleanup_success_deletes_cache_files_and_declarations(self, tmp_path):
        from tasklite.v2.utils.ipc import ArtifactCleanupMode

        channel = ExecutionChannel(tmp_path)
        uid = "job_success"
        cache_file = tmp_path / "cache.tmp"
        cache_file.write_text("temporary data")
        output_file = tmp_path / "final.txt"
        output_file.write_text("final data")

        channel.journal.record_output(uid, str(cache_file), cleanup=False, kind="cache")
        channel.journal.record_output(uid, str(output_file), cleanup=True, kind="file")

        channel.cleanup_artifacts(uid, mode=ArtifactCleanupMode.SUCCESS)

        assert not cache_file.exists()
        assert output_file.exists()
        assert not channel.journal.outputs_path(uid).exists()

    def test_cleanup_failure_deletes_cleanup_on_fail_outputs(self, tmp_path):
        from tasklite.v2.utils.ipc import ArtifactCleanupMode

        channel = ExecutionChannel(tmp_path)
        uid = "job_fail"
        fail_file = tmp_path / "temp_product.txt"
        fail_file.write_text("to delete")
        keep_file = tmp_path / "keep.txt"
        keep_file.write_text("to keep")

        channel.journal.record_output(uid, str(fail_file), cleanup=True, kind="file")
        channel.journal.record_output(uid, str(keep_file), cleanup=False, kind="file")

        channel.cleanup_artifacts(uid, mode=ArtifactCleanupMode.FAILURE_OR_RETRY)

        assert not fail_file.exists()
        assert keep_file.exists()
        assert not channel.journal.outputs_path(uid).exists()


class TestWorkerWrapper:
    """worker 执行体入口：认证令牌、锁冲突/环境故障降级、瞬态三 kind 落盘。"""

    def _result_file(self, tmp_path, job, incarnation=_INCARNATION) -> Path:
        return tmp_path / f"{safe_uid_filename(job.uid)}.{incarnation}.result.json"

    def test_worker_writes_success_with_auth_token(self, tmp_path):
        job = Job("t", "x")
        _mp_worker_wrapper(make_spec(
            job, lambda j, c: (True, {"ok": 1}), tmp_path, result_token=_TOKEN,
        ))
        data = json.loads(self._result_file(tmp_path, job).read_text(encoding="utf-8"))
        assert data["status"] == "success"
        assert data["auth"] == _TOKEN
        assert data["raw_result"] == ["__tl_tuple_v1", [True, {"ok": 1}]]

    def test_worker_error_payloads_carry_auth_token(self, tmp_path):
        """retry/error 各状态通道的 payload 同样携带令牌（降级不豁免）。"""
        def raise_transient(job, ctx):
            raise TimeoutError("t")

        def raise_system_exit(job, ctx):
            raise SystemExit(2)

        for handler, want_status, job_id in (
            (raise_transient, "retry", "yretry"),
            (raise_system_exit, "error", "yexit"),
        ):
            job = Job("t", job_id)
            _mp_worker_wrapper(make_spec(
                job, handler, tmp_path, result_token=_TOKEN,
            ))
            data = json.loads(self._result_file(tmp_path, job).read_text(encoding="utf-8"))
            assert data["status"] == want_status
            assert data["auth"] == _TOKEN

    def test_worker_rejects_missing_incarnation(self, tmp_path):
        """incarnation 缺失 fail-loud（fencing 契约破坏不得静默产出无名结果）。"""
        job = Job("t", "x")
        with pytest.raises(RuntimeError, match="incarnation"):
            _mp_worker_wrapper(make_spec(job, lambda j, c: True, tmp_path, incarnation=None))

    def test_lock_conflict_writes_transient_retry(self, tmp_path, monkeypatch):
        """锁被占 → retry + transient_kind=lock_conflict（瞬态军规 wire）。"""
        monkeypatch.setattr(
            "tasklite.v2.utils.lockfile.try_acquire_lock", lambda *a, **k: None
        )
        job = Job("h", "a")
        _mp_worker_wrapper(make_spec(job, lambda j, c: (True, {}), tmp_path))

        res = ArtifactJournal(tmp_path).read_result("h::a", incarnation=_INCARNATION)
        assert res is not None
        assert res["status"] == "retry"
        assert res["transient_kind"] == "lock_conflict"
        assert res["error"].startswith("LOCK_CONFLICT:")

    def test_lock_env_fault_writes_degraded_retry(self, tmp_path):
        """锁文件打不开（IsADirectoryError）→ 落盘 retry+lock_conflict 结果。

        环境故障与「锁被占」同走瞬态通道（零预算、零污染），errno 归因
        保留在 error 串供运维区分。
        """
        ipc_dir = str(tmp_path / "ipc")
        uid = "t::j1"
        lock_name = f"{safe_uid_filename(uid)}.lock"
        (Path(ipc_dir) / lock_name).mkdir(parents=True)

        job = Job("t", "j1")
        _mp_worker_wrapper(make_spec(
            job, lambda j, c: True, ipc_dir, incarnation="r.1",
        ))

        journal = ArtifactJournal(ipc_dir)
        res = journal.read_result(journal.result_path(uid, "r.1"))
        assert res is not None
        assert res["status"] == "retry"
        assert res["transient_kind"] == "lock_conflict"
        assert "LOCK_ENV_FAULT" in res["error"]

    def test_rate_limit_writes_transient_kind(self, tmp_path):
        """RateLimitHit 与业务 RetryError 共享 retry 通道，仅前者带 rate_limited。"""
        def rate_limited(job, ctx):
            raise RateLimitHit("HTTP 429 RateLimit hit (resource=api, ttl=60.0s)")

        def business_retry(job, ctx):
            from tasklite.v2.exceptions import RetryError

            raise RetryError("business transient")

        for handler, job_id, want_kind in (
            (rate_limited, "rl", "rate_limited"),
            (business_retry, "biz", None),
        ):
            job = Job("h", job_id)
            _mp_worker_wrapper(make_spec(job, handler, tmp_path))
            res = ArtifactJournal(tmp_path).read_result(job.uid, incarnation=_INCARNATION)
            assert res["status"] == "retry"
            assert res.get("transient_kind") == want_kind

    def test_keyboard_interrupt_writes_interrupted(self, tmp_path):
        def interrupted(job, ctx):
            raise KeyboardInterrupt()

        job = Job("h", "ki")
        _mp_worker_wrapper(make_spec(job, interrupted, tmp_path))
        res = ArtifactJournal(tmp_path).read_result(job.uid, incarnation=_INCARNATION)
        assert res["status"] == "interrupted"
        assert res["error"].startswith("WORKER_INTERRUPTED:")

    def test_registered_transient_exception_writes_retry(self, tmp_path):
        """per-pipeline 瞬态注册表随 ctx 下发，命中注册表按 retry 分类。"""
        job = Job("h", "reg")
        ctx = JobContext(
            job, wall_keys=set(), failed_keys=set(), cursors={},
            transient_registry=(ConnectionAbortedError,),
        )
        spec = WorkerLaunchSpec(
            handler=lambda j, c: (_ for _ in ()).throw(ConnectionAbortedError("reset")),
            job=job, task_ctx=ctx, incarnation=_INCARNATION,
            ipc_dir=str(tmp_path), timeout=60.0,
        )
        _mp_worker_wrapper(spec)
        res = ArtifactJournal(tmp_path).read_result(job.uid, incarnation=_INCARNATION)
        assert res["status"] == "retry"

    def test_fatal_heuristic_writes_fatal(self, tmp_path):
        """未注册的 TypeError 命中 fatal 启发式 → fatal 通道。"""
        job = Job("h", "tb")
        _mp_worker_wrapper(make_spec(
            job, lambda j, c: (_ for _ in ()).throw(TypeError("bug")), tmp_path,
        ))
        res = ArtifactJournal(tmp_path).read_result(job.uid, incarnation=_INCARNATION)
        assert res["status"] == "fatal"
        assert "TypeError" in res["error"]

    def test_plain_exception_writes_error(self, tmp_path):
        job = Job("h", "ve")
        _mp_worker_wrapper(make_spec(
            job, lambda j, c: (_ for _ in ()).throw(ValueError("plain")), tmp_path,
        ))
        res = ArtifactJournal(tmp_path).read_result(job.uid, incarnation=_INCARNATION)
        assert res["status"] == "error"
        assert "ValueError" in res["error"]


class TestWorkerResultWriteDegraded:
    """worker 落盘降级链：完整写两连败 → 降级最小结果（瞬态保真）。"""

    @staticmethod
    def _is_full_payload(result_dict):
        """区分完整 payload 与降级 payload（降级写的 error 带 DEGRADED 标记）。"""
        if "raw_result" in result_dict:
            return True
        return str(result_dict.get("error", "")).startswith("LOCK_CONFLICT:")

    def test_success_payload_write_failure_degrades_to_retry(self, tmp_path, monkeypatch):
        """完整结果写两连败 → 降级写 retry 结果（重跑而非静默丢失）。"""
        real_write = ArtifactJournal.write_result_atomic
        calls: list[dict] = []

        def flaky_write(journal_self, uid, result_dict, incarnation=None):
            calls.append(result_dict)
            if self._is_full_payload(result_dict):
                raise OSError(28, "No space left on device")
            return real_write(journal_self, uid, result_dict, incarnation=incarnation)

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", flaky_write)

        job = Job("h", "a")
        _mp_worker_wrapper(make_spec(job, lambda j, c: (True, {"v": 1}), tmp_path))

        res = ArtifactJournal(tmp_path).read_result("h::a", incarnation=_INCARNATION)
        assert res is not None, "降级写必须成功落盘（小 payload 可写）"
        assert res["status"] == "retry"
        assert "IPC_RESULT_WRITE_DEGRADED" in res["error"]
        full_attempts = [c for c in calls if self._is_full_payload(c)]
        assert len(full_attempts) == 2, "完整写应先试两次再降级"
        assert len(calls) == 3, "降级写只补一次"

    def test_lock_conflict_degraded_keeps_structured_field(self, tmp_path, monkeypatch):
        """锁冲突完整写失败 → 降级结果保留 transient_kind 结构化字段。

        判定端读 transient_kind 字段而非 error 前缀——降级写丢字段会把
        框架锁冲突误当业务 retry，烧重试预算。
        """
        real_write = ArtifactJournal.write_result_atomic

        def flaky_write(journal_self, uid, result_dict, incarnation=None):
            if self._is_full_payload(result_dict):
                raise OSError(28, "No space left on device")
            return real_write(journal_self, uid, result_dict, incarnation=incarnation)

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", flaky_write)
        monkeypatch.setattr(
            "tasklite.v2.utils.lockfile.try_acquire_lock", lambda *a, **k: None
        )

        job = Job("h", "a")
        _mp_worker_wrapper(make_spec(job, lambda j, c: (True, {}), tmp_path))

        res = ArtifactJournal(tmp_path).read_result("h::a", incarnation=_INCARNATION)
        assert res is not None
        assert res["status"] == "retry"
        assert res.get("transient_kind") == "lock_conflict"
        assert "IPC_RESULT_WRITE_DEGRADED" in res["error"]

    def test_rate_limit_degraded_keeps_structured_field(self, tmp_path, monkeypatch):
        """限流完整写失败 → 降级结果保留 rate_limited（预算豁免依据）。"""
        real_write = ArtifactJournal.write_result_atomic

        def flaky_write(journal_self, uid, result_dict, incarnation=None):
            if "IPC_RESULT_WRITE_DEGRADED" not in str(result_dict.get("error", "")):
                raise OSError(28, "No space left on device")
            return real_write(journal_self, uid, result_dict, incarnation=incarnation)

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", flaky_write)

        job = Job("h", "rl")

        def rate_limited_handler(job_arg, ctx):
            raise RateLimitHit("HTTP 429 RateLimit hit")

        _mp_worker_wrapper(make_spec(job, rate_limited_handler, tmp_path))

        res = ArtifactJournal(tmp_path).read_result("h::rl", incarnation=_INCARNATION)
        assert res is not None
        assert res["status"] == "retry"
        assert res.get("transient_kind") == "rate_limited"

    def test_all_writes_fail_worker_exits_without_crash(self, tmp_path, monkeypatch):
        """两级挽救均失效 → worker 不抛 OSError、无结果文件（按崩溃收割）。"""

        def refusing_write(journal_self, uid, result_dict, incarnation=None):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", refusing_write)

        job = Job("h", "a")
        _mp_worker_wrapper(make_spec(job, lambda j, c: (True, {}), tmp_path))

        journal = ArtifactJournal(tmp_path)
        assert not journal.result_path("h::a", _INCARNATION).exists(), (
            "全部写失败时不应存在结果文件"
        )


class TestEnvFallbackIpcDirCoherence:
    """env 兜底 ipc_dir：解析成功必须回写实例属性，收割路径同源可用。"""

    def test_env_fallback_spawn_reap_probe_share_same_ipc_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TASKLITE_IPC_DIR", str(tmp_path))

        class ResultWritingProcess:
            """start() 时经 spec.task_ctx.ipc_dir 写 success 结果的进程桩。"""

            def __init__(self, target=None, args=(), kwargs=None, **_kw):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                spec = self.args[0]
                self._alive = True
                ArtifactJournal(spec.task_ctx.ipc_dir).write_result_atomic(
                    spec.job.uid,
                    {
                        "status": "success",
                        "raw_result": True,
                        "new_jobs": [],
                        "resource_suspensions": [],
                        "cursor_updates": {},
                    },
                    incarnation=spec.incarnation,
                )

            def join(self, timeout=None):
                self._alive = False

            def is_alive(self):
                return self._alive

            def kill(self):
                self._alive = False

        fake_mp_ctx = type("Ctx", (), {"Process": ResultWritingProcess})()
        channel = ExecutionChannel(mp_ctx=fake_mp_ctx)

        job = Job("t", "x")
        handle = channel.spawn(make_spec(job, lambda j, c: (True, {}), None))

        # 实例属性、journal 与 handle 派发路径同源
        assert channel.ipc_dir == str(tmp_path), (
            "env 兜底解析成功后必须回写实例属性（收割路径的事实源）"
        )
        assert handle.ipc_dir == str(tmp_path)
        assert channel.journal.ipc_dir == str(tmp_path)

        completed = channel.reap_completed([handle])
        assert [(h.uid, r.success) for h, r in completed] == [("t::x", True)]
        assert channel.probe_orphan_lock("t::x") is True
        assert channel.drain_all_signals() == []


class TestSpawnFailureCleanup:
    def test_submit_start_failure_reraises(self, tmp_path):
        class ExplodingCtx:
            def Process(self, *args, **kwargs):
                raise RuntimeError("process start failed")

        channel = ExecutionChannel(mp_ctx=ExplodingCtx(), ipc_dir=str(tmp_path))
        job = Job("t", "j1")
        with pytest.raises(RuntimeError, match="process start failed"):
            channel.spawn(make_spec(job, lambda j, c: True, tmp_path))

    def test_submit_requires_ipc_dir(self, monkeypatch):
        monkeypatch.delenv("TASKLITE_IPC_DIR", raising=False)
        channel = ExecutionChannel(
            mp_ctx=type("Ctx", (), {"Process": lambda *a, **k: None})()
        )
        job = Job("t", "j1")
        with pytest.raises(ValueError, match="ipc_dir"):
            channel.spawn(make_spec(job, lambda j, c: True, None))


class TestNormalizeResultJsonSerializable:
    """dict/tuple 元数据 JSON 可序列化预检——坏元数据直接任务级失败。"""

    def test_dict_with_bytes_metadata_rejected(self):
        success, meta = _normalize_handler_result({"data": b"raw"})
        assert success is False
        assert "not JSON-serializable" in meta["error"]

    def test_tuple_with_datetime_metadata_rejected(self):
        success, meta = _normalize_handler_result((True, {"ts": datetime.now()}))
        assert success is False
        assert "not JSON-serializable" in meta["error"]

    def test_plain_dict_still_accepted(self):
        success, meta = _normalize_handler_result({"a": 1, "b": "x"})
        assert success is True
        assert meta == {"a": 1, "b": "x"}

    def test_failed_tuple_with_bad_meta_rejected(self):
        success, meta = _normalize_handler_result((False, {"blob": bytes(3)}))
        assert success is False
        assert "not JSON-serializable" in meta["error"]

    def test_nan_result_meta_rejected(self):
        success, meta = _normalize_handler_result({"size": float("nan")})
        assert success is False, "NaN result_meta 必须被拒绝"
        assert "not JSON-serializable" in meta["error"]

    def test_invalid_tuple_shape_rejected(self):
        success, meta = _normalize_handler_result(("yes", {"a": 1}))
        assert success is False
        assert "invalid handler return tuple" in meta["error"]

    def test_unrecognized_type_rejected(self):
        success, meta = _normalize_handler_result(42)
        assert success is False
        assert "invalid handler return type" in meta["error"]


_BASE_OK = {
    "status": "success",
    "raw_result": {"ok": True},
    "new_jobs": [],
    "cursor_updates": {},
    "resource_suspensions": [],
}


def _decode(res, p=None, ipc_dir=None):
    return _decode_ipc_result(res, p, Job("t", "a"), ipc_dir)


class TestDecodeIpcResultDefense:
    """损坏结果防御分支：任何异常形态收敛为任务级失败，不穿透崩 run。"""

    def test_missing_raw_result_key_marks_failed(self):
        res = dict(_BASE_OK)
        del res["raw_result"]
        result = _decode(res)
        assert result.success is False
        assert "CORRUPT_RESULT_FILE" in result.result_meta["error"]

    def test_success_decode_crash_converted_to_corrupt_result(self, monkeypatch):
        """解码异常兜底：success 分支解码/归一化段意外异常 → 转损坏结果。"""
        monkeypatch.setattr(
            "tasklite.v2.engine.channel._normalize_handler_result",
            lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        result = _decode(dict(_BASE_OK))
        assert result.success is False
        assert result.result_meta["error"].startswith("CORRUPT_RESULT_FILE: decode failed")
        assert "traceback" in result.result_meta

    def test_new_jobs_scalar_marks_failed_not_crash(self):
        for scalar in (7, 3.5, True):
            result = _decode(dict(_BASE_OK, new_jobs=scalar))
            assert result.success is False
            assert "invalid new_jobs type" in result.result_meta["error"]

    def test_malformed_spawned_job_marks_failed(self):
        res = dict(_BASE_OK, new_jobs=[{"task_type": "t"}])  # 缺 job_id
        result = _decode(res)
        assert result.success is False
        assert "invalid spawned job dict" in result.result_meta["error"]

    def test_valid_spawned_jobs_parsed(self):
        res = dict(_BASE_OK, new_jobs=[
            {"task_type": "t", "job_id": "child", "payload": {"v": 1}},
        ])
        result = _decode(res)
        assert result.success is True
        assert [j.uid for j in result.new_jobs] == ["t::child"]

    def test_cursor_updates_type_not_dict_marks_failed(self):
        res = dict(_BASE_OK, cursor_updates=["k", "v"])
        result = _decode(res)
        assert result.success is False
        assert "invalid cursor_updates type" in result.result_meta["error"]

    def test_cursor_updates_value_not_str_marks_failed(self):
        res = dict(_BASE_OK, cursor_updates={"k": 123})
        result = _decode(res)
        assert result.success is False
        assert "invalid cursor_updates value" in result.result_meta["error"]

    def test_resource_suspensions_bad_element_marks_failed(self):
        res = dict(_BASE_OK, resource_suspensions=[("api", "not-a-number")])
        result = _decode(res)
        assert result.success is False
        assert "invalid resource_suspension entry" in result.result_meta["error"]

    def test_resource_suspensions_not_list_marks_failed(self):
        res = dict(_BASE_OK, resource_suspensions={"api": 1.0})
        result = _decode(res)
        assert result.success is False
        assert "invalid resource_suspensions type" in result.result_meta["error"]

    def test_retry_status_extracts_transient_kind(self):
        result = _decode({"status": "retry", "error": "x", "transient_kind": "rate_limited"})
        assert result.retry_requested is True
        assert result.transient_kind == "rate_limited"

    def test_retry_status_non_string_kind_ignored(self):
        result = _decode({"status": "retry", "error": "x", "transient_kind": 7})
        assert result.retry_requested is True
        assert result.transient_kind is None

    def test_interrupted_status_forces_transient_kind(self):
        result = _decode({"status": "interrupted", "error": "sig"})
        assert result.retry_requested is True
        assert result.transient_kind == "interrupted"

    def test_fatal_status_marks_failed(self):
        res = {"status": "fatal", "error": "boom", "traceback": "tb"}
        result = _decode(res)
        assert result.success is False
        assert result.result_meta["fatal"] is True
        assert result.result_meta["error"] == "boom"

    def test_unknown_status_marks_failed(self):
        result = _decode({"status": "weird"})
        assert result.success is False
        assert "unknown status" in result.result_meta["error"]

    def test_crash_exitcode_marks_failed(self):
        class _P:
            exitcode = -9

        res = {"raw": "not-a-result-dict"}  # 无 status 键 → 走 exitcode 归因
        result = _decode(res, p=_P())
        assert result.success is False
        assert "PROCESS_CRASH_EXITCODE_-9" in result.result_meta["error"]

    def test_missing_status_without_process_requests_retry(self):
        """认领路径（p=None）读到无 status 的残留文件 → NO_IPC_RESULT 可重试。"""
        result = _decode({}, None)
        assert result.success is False
        assert result.retry_requested is True
        assert "NO_IPC_RESULT" in result.result_meta["error"]

    def test_nonzero_exitcode_without_status_requests_retry(self):
        res = {"raw": "not-a-result-dict"}
        result = _decode(res, SimpleNamespace(exitcode=1))
        assert result.success is False
        assert result.retry_requested is True
        assert "PROCESS_CRASH_EXITCODE_1" in result.result_meta["error"]
        assert "PROCESS_CRASH_EXIT" in (result.retry_error or "")

    def test_missing_output_marks_failed(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        journal.record_output("t::a", str(tmp_path / "missing.jpg"), cleanup=True)
        result = _decode(dict(_BASE_OK), ipc_dir=str(tmp_path))
        assert result.success is False
        assert "Missing output" in result.result_meta["error"]

    def test_present_output_passes(self, tmp_path):
        journal = ArtifactJournal(tmp_path)
        out = tmp_path / "present.jpg"
        out.write_bytes(b"x")
        journal.record_output("t::a", str(out), cleanup=True)
        result = _decode(dict(_BASE_OK), ipc_dir=str(tmp_path))
        assert result.success is True

    def test_cache_output_skips_existence_check(self, tmp_path):
        """kind=cache 的临时文件跳过存在性校验（原子产出 .part 已被 replace）。"""
        journal = ArtifactJournal(tmp_path)
        journal.record_output(
            "t::a", str(tmp_path / "cache.part"), cleanup=True, kind="cache"
        )
        result = _decode(dict(_BASE_OK), ipc_dir=str(tmp_path))
        assert result.success is True


class TestDecodeConvergencePaths:
    """收割/认领路径收敛：损坏结果归一为任务级结果，绝不以 TypeError 崩 run。"""

    _SCALAR_CORRUPT = {
        "status": "success",
        "raw_result": None,
        "new_jobs": 7,
        "cursor_updates": {},
        "resource_suspensions": [],
    }

    class _DeadProcess:
        exitcode = 0

        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

        def kill(self):
            pass

        def close(self):
            pass

    def test_reap_completed_contains_scalar_new_jobs_corruption(self, tmp_path):
        job = Job("t", "a")
        ArtifactJournal(tmp_path).write_result_atomic(
            job.uid, dict(self._SCALAR_CORRUPT), incarnation=_INCARNATION,
        )
        handle = JobHandle(
            uid=job.uid, process=self._DeadProcess(), deadline=1.0, timeout=5.0,
            job=job, ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )
        completed = ExecutionChannel(tmp_path).reap_completed([handle])
        assert len(completed) == 1, "损坏任务必须被收割而非打断收割循环"
        result = completed[0][1]
        assert result.success is False
        assert "invalid new_jobs type" in result.result_meta["error"]
        assert not ArtifactJournal(tmp_path).result_path(job.uid, _INCARNATION).exists(), (
            "收割后 IPC 结果文件必须照常清理"
        )

    def test_claim_stale_result_contains_scalar_new_jobs_corruption(self, tmp_path):
        job = Job("t", "a")
        incarnation = "deadbeefdeadbeefdeadbeefdeadbeef.2"
        ArtifactJournal(tmp_path).write_result_atomic(
            job.uid, dict(self._SCALAR_CORRUPT), incarnation=incarnation,
        )
        result = ExecutionChannel(tmp_path).claim_stale_result(job.uid, job)
        assert result is not None, "含 status 键的残留结果必须被认领"
        assert result.success is False
        assert "invalid new_jobs type" in result.result_meta["error"]
        assert not ArtifactJournal(tmp_path).result_path(job.uid, incarnation).exists(), (
            "认领后残留文件必须照常消费清理"
        )


class TestBuildTerminalFailureDefense:
    """无结果文件的终局构造：超时瞬态/终局、崩溃、无结果分类。"""

    class _JobStub:
        def __init__(self, timeout_is_transient):
            self.timeout_is_transient = timeout_is_transient

    @staticmethod
    def _handle(timeout_is_transient):
        return JobHandle(
            uid="t::a",
            process=SimpleNamespace(exitcode=None),
            deadline=1.0,
            timeout=5.0,
            job=TestBuildTerminalFailureDefense._JobStub(timeout_is_transient),
            ipc_dir="/tmp",
        )

    def test_timeout_transient_requests_retry(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=None), self._handle(True), is_timeout=True,
        )
        assert result.success is False
        assert result.retry_requested is True
        assert "TIMEOUT" in (result.retry_error or "")

    def test_timeout_not_transient_marks_failed(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=None), self._handle(False), is_timeout=True,
        )
        assert result.success is False
        assert result.retry_requested is False
        assert "TIMEOUT" in result.result_meta["error"]

    def test_crash_exitcode_classified(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=-11), self._handle(False), is_timeout=False,
        )
        assert result.success is False
        assert result.retry_requested is True
        assert "PROCESS_CRASH_EXITCODE_-11" in result.result_meta["error"]

    def test_clean_exit_without_result_classified(self):
        result = ExecutionChannel._build_terminal_failure(
            SimpleNamespace(exitcode=0), self._handle(False), is_timeout=False,
        )
        assert result.success is False
        assert result.retry_requested is True
        assert "NO_IPC_RESULT" in result.result_meta["error"]
        assert result.retry_error


class TestWatchdogReap:
    """看门狗：deadline 驱动 kill；join 窗口自然退出按 exitcode 归因。"""

    class _HungProcess:
        """kill 前恒活的进程桩（模拟死锁/卡死）。"""

        def __init__(self):
            self._alive = True
            self.exitcode = None

        def is_alive(self):
            return self._alive

        def join(self, timeout=None):
            pass

        def kill(self):
            self._alive = False
            self.exitcode = -9

        def close(self):
            pass

    class _DyingDuringJoinProcess:
        """join 窗口内自然退出的进程桩（exitcode 0，无结果文件）。"""

        def __init__(self):
            self._alive = True
            self.exitcode = 0

        def is_alive(self):
            return self._alive

        def join(self, timeout=None):
            self._alive = False

        def kill(self):
            self._alive = False

        def close(self):
            pass

    def test_deadline_kill_attributed_to_timeout(self, tmp_path):
        job = Job("t", "hang")
        handle = JobHandle(
            uid=job.uid, process=self._HungProcess(),
            deadline=time.monotonic() - 1, timeout=2.0,
            job=job, ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )
        completed = ExecutionChannel(tmp_path).reap_completed([handle])
        assert len(completed) == 1
        result = completed[0][1]
        assert result.success is False
        assert result.result_meta["error"].startswith("TIMEOUT")
        assert result.retry_requested is False, (
            "timeout_is_transient=False 的超时是终局，不烧重试预算"
        )

    def test_timeout_transient_job_requests_retry(self, tmp_path):
        job = Job("t", "hang_transient", timeout_is_transient=True)
        handle = JobHandle(
            uid=job.uid, process=self._HungProcess(),
            deadline=time.monotonic() - 1, timeout=2.0,
            job=job, ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )
        completed = ExecutionChannel(tmp_path).reap_completed([handle])
        result = completed[0][1]
        assert result.retry_requested is True, (
            "timeout_is_transient=True 的超时按瞬态重试处理"
        )

    def test_join_window_exit_attributed_by_exitcode(self, tmp_path):
        """deadline 触发 join 后自然退出 → 按 exitcode 归因（NO_IPC_RESULT），不折入 TIMEOUT。"""
        job = Job("t", "natural_exit")
        handle = JobHandle(
            uid=job.uid, process=self._DyingDuringJoinProcess(),
            deadline=time.monotonic() - 1, timeout=2.0,
            job=job, ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )
        completed = ExecutionChannel(tmp_path).reap_completed([handle])
        result = completed[0][1]
        assert "NO_IPC_RESULT" in result.result_meta["error"], (
            "join 窗口内自然退出且无结果 → NO_IPC_RESULT 瞬态，非 TIMEOUT"
        )
        assert result.retry_requested is True

    def test_completed_result_reaped_before_deadline(self, tmp_path):
        """结果文件已落盘且进程仍活 → 直接按结果收割（不等 deadline）。"""
        job = Job("t", "fast")
        ArtifactJournal(tmp_path).write_result_atomic(
            job.uid,
            {
                "status": "success",
                "raw_result": True,
                "new_jobs": [],
                "resource_suspensions": [],
                "cursor_updates": {},
            },
            incarnation=_INCARNATION,
        )
        handle = JobHandle(
            uid=job.uid, process=self._HungProcess(),
            deadline=time.monotonic() + 60, timeout=60.0,
            job=job, ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )
        completed = ExecutionChannel(tmp_path).reap_completed([handle])
        assert [(h.uid, r.success) for h, r in completed] == [("t::fast", True)]


class TestFinalizeProcessKillRace:
    def test_kill_race_still_joins_and_closes(self):
        """kill 抛 ProcessLookupError（is_alive 与 kill 之间退出）时 join/close 不跳过。"""

        class RaceProcess:
            def __init__(self):
                self.joined = False
                self.closed = False

            def is_alive(self):
                return True

            def kill(self):
                raise ProcessLookupError("no such process")

            def join(self, timeout=None):
                self.joined = True

            def close(self):
                self.closed = True

        p = RaceProcess()
        ExecutionChannel._finalize_process(p)
        assert p.joined, "join must still be called after kill race (reap zombie)"
        assert p.closed, "close must still be called after kill race"


class TestStaleResultClaimChannel:
    def test_claim_stale_result_success(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "claim")
        ArtifactJournal(tmp_path).write_result_atomic(
            job.uid,
            {
                "status": "success",
                "raw_result": {"status": "ok"},
                "new_jobs": [],
                "resource_suspensions": [],
                "cursor_updates": {},
            },
            incarnation=f"{'a' * 32}.1",
        )

        result = channel.claim_stale_result(job.uid, job)
        assert result is not None
        assert result.success is True
        assert result.result_meta == {"status": "ok"}

    def test_claim_stale_ignores_tmp_leftover(self, tmp_path):
        """认领必须忽略 .tmp 残留（孤儿 worker 正在写，unlink 会炸其 replace）。"""
        channel = ExecutionChannel(tmp_path)
        job = Job("h", "a")
        base = safe_uid_filename(job.uid)
        tmp_file = tmp_path / f"{base}.result.json.tmp"
        tmp_file.write_text('{"status": "suc')

        assert channel.claim_stale_result(job.uid, job) is None
        assert tmp_file.exists(), "认领不得 unlink 正在写的 .tmp 文件"

    def test_claim_stale_prefers_latest_incarnation_same_instant(self, tmp_path):
        """同刻多执行代残留按 (mtime_ns, incarnation_seq) 决胜取最新。"""
        import os

        run_id = "a" * 32
        journal = ArtifactJournal(tmp_path)
        journal.write_result_atomic(
            "t::x",
            {"status": "success", "raw_result": encode_raw_result((True, {"v": 1}))},
            incarnation=f"{run_id}.1",
        )
        journal.write_result_atomic(
            "t::x",
            {"status": "success", "raw_result": encode_raw_result((True, {"v": 2}))},
            incarnation=f"{run_id}.2",
        )
        for p in tmp_path.glob("*.result.json"):
            os.utime(p, ns=(1234567890, 1234567890))

        channel = ExecutionChannel(tmp_path)
        job = Job("t", "x")
        result = channel.claim_stale_result("t::x", job)
        assert result is not None and result.success
        assert result.result_meta == {"v": 2}, (
            f"同刻冲突必须取最新执行代: {result.result_meta}"
        )


class TestResultAuthTokenReaderSide:
    def _poison_payload(self):
        return {
            "status": "success",
            "raw_result": {"poisoned": True},
            "new_jobs": [{"task_type": "evil", "job_id": "injected", "payload": {}}],
            "cursor_updates": {"cursor": "poisoned"},
        }

    @staticmethod
    def _write_result_file(channel, job, payload, incarnation=_INCARNATION):
        p = channel.journal.result_path(job.uid, incarnation)
        p.write_text(json.dumps(payload), encoding="utf-8")
        return p

    class _DeadProcess:
        exitcode = 1

        def is_alive(self):
            return False

        def kill(self):
            pass

        def join(self, timeout=None):
            pass

        def close(self):
            pass

    def test_claim_rejects_forged_result_without_token(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        channel.result_token = _TOKEN
        job = Job("t", "x")
        self._write_result_file(channel, job, self._poison_payload())
        assert channel.claim_stale_result(job.uid, job) is None, (
            "无令牌的伪造结果必须按无结果丢弃"
        )

    def test_claim_rejects_result_with_wrong_token(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        channel.result_token = _TOKEN
        job = Job("t", "x")
        payload = self._poison_payload()
        payload["auth"] = "f" * 64
        self._write_result_file(channel, job, payload)
        assert channel.claim_stale_result(job.uid, job) is None

    def test_claim_accepts_result_with_matching_token(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        channel.result_token = _TOKEN
        job = Job("t", "x")
        payload = {"status": "success", "auth": _TOKEN, "raw_result": {"ok": 1}}
        self._write_result_file(channel, job, payload)

        result = channel.claim_stale_result(job.uid, job)
        assert result is not None and result.success
        assert result.result_meta == {"ok": 1}

    def test_reap_treats_forged_result_as_transient_no_result(self, tmp_path):
        """收割路径令牌不匹配按无结果处理：瞬态可重试，不投毒。"""
        channel = ExecutionChannel(tmp_path)
        channel.result_token = _TOKEN
        job = Job("t", "x")
        self._write_result_file(channel, job, self._poison_payload())
        handle = JobHandle(
            uid=job.uid, process=self._DeadProcess(), deadline=1e18, timeout=10.0,
            job=job, ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )

        completed = channel.reap_completed([handle])

        assert len(completed) == 1
        result = completed[0][1]
        assert result.success is False, "伪造结果绝不能驱动成功提交"
        assert result.retry_requested is True, "令牌不匹配走无结果瞬态（可重试）"
        assert "poisoned" not in json.dumps(result.result_meta)

    def test_abort_excludes_forged_result_from_completed(self, tmp_path):
        """中止分类路径令牌不匹配的结果不进入 completed（不提交伪造终态）。"""
        channel = ExecutionChannel(tmp_path)
        channel.result_token = _TOKEN
        job = Job("t", "x")
        self._write_result_file(channel, job, self._poison_payload())
        handle = JobHandle(
            uid=job.uid, process=self._DeadProcess(), deadline=1e18, timeout=10.0,
            job=job, ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )

        outcome = channel.abort_in_flight([handle])

        assert outcome.completed == [], "伪造结果不得经中止分类提交"


class TestAbortInFlight:
    class MockProcess:
        def __init__(self):
            self._alive = False
            self.exitcode = 0

        def is_alive(self):
            return self._alive

        def kill(self):
            pass

        def join(self, timeout=None):
            pass

        def close(self):
            pass

    def test_abort_with_completed_and_cancelled(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        job_done = Job("task", "done")
        job_pending = Job("task", "pending")

        res_done = channel.journal.result_path(job_done.uid, "inc_done")
        res_done.write_text(
            json.dumps({"status": "success", "raw_result": {"done": 1}})
        )

        h_done = JobHandle(
            uid=job_done.uid, process=self.MockProcess(),
            deadline=time.monotonic() + 10, timeout=10, job=job_done,
            ipc_dir=str(tmp_path), incarnation="inc_done",
        )
        h_pending = JobHandle(
            uid=job_pending.uid, process=self.MockProcess(),
            deadline=time.monotonic() + 10, timeout=10, job=job_pending,
            ipc_dir=str(tmp_path), incarnation="inc_pending",
        )

        outcome = channel.abort_in_flight([h_done, h_pending])
        assert len(outcome.completed) == 1
        assert outcome.completed[0][0].uid == job_done.uid
        assert outcome.completed[0][1].success is True
        assert outcome.completed[0][1].result_meta == {"done": 1}

        assert len(outcome.cancelled) == 1
        assert outcome.cancelled[0].uid == job_pending.uid

    def test_abort_salvages_signal_written_before_kill(self, tmp_path):
        """杀进程后补排空：「先排空后杀」窗口内终前写入的 suspend 信号捞回。

        受控注入：stub 进程在 kill() 时向信号文件追加一条信号（模拟 worker
        在上层预排空之后、被终止之前落盘的 suspend）。abort 的最终排空必须
        把它带回 AbortOutcome.salvaged_signals，且不得被随后的半成品清理
        一并删除。
        """
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "salvage")

        class KillWritingProcess:
            """kill() 时写入 suspend 信号的 stub：确定性复现终前写窗口。"""

            def __init__(self):
                self._alive = True

            def is_alive(self):
                return self._alive

            def kill(self):
                self._alive = False
                channel.journal.record_signal(job.uid, "gpu", 30.0)

            def join(self, timeout=None):
                pass

            def close(self):
                pass

        h = JobHandle(
            uid=job.uid, process=KillWritingProcess(),
            deadline=time.monotonic() + 10, timeout=10, job=job,
            ipc_dir=str(tmp_path), incarnation="inc_salvage",
        )

        outcome = channel.abort_in_flight([h])

        assert [handle.uid for handle in outcome.cancelled] == [job.uid]
        assert outcome.salvaged_signals == [(job.uid, "gpu", 30.0)]
        # 信号已被消费带走，半成品清理照常执行（不留残留文件）
        assert not channel.journal.signals_path(job.uid).exists()

    def test_abort_reprobe_completed_merges_post_drain_signals(self, tmp_path):
        """重探测完成分支：终前写入的结果与信号都被消费。

        stub 进程在 kill() 时同时写结果文件与 suspend 信号（worker 终前
        恰好完成的时序）。重探测按结果分类为已完成，最终排空捞回的信号
        并入 ExecutionResult.resource_suspensions（与常规收割路径的
        salvage 行为对齐）。
        """
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "reprobe")

        class FinishOnKillProcess:
            """kill() 时写最终结果 + suspend 信号的 stub。"""

            def __init__(self):
                self._alive = True

            def is_alive(self):
                return self._alive

            def kill(self):
                self._alive = False
                channel.journal.write_result_atomic(
                    job.uid,
                    {"status": "success", "raw_result": {"done": 1}},
                    incarnation="inc_reprobe",
                )
                channel.journal.record_signal(job.uid, "api", 5.0)

            def join(self, timeout=None):
                pass

            def close(self):
                pass

        h = JobHandle(
            uid=job.uid, process=FinishOnKillProcess(),
            deadline=time.monotonic() + 10, timeout=10, job=job,
            ipc_dir=str(tmp_path), incarnation="inc_reprobe",
        )

        outcome = channel.abort_in_flight([h])

        assert [handle.uid for handle, _ in outcome.completed] == [job.uid]
        assert outcome.completed[0][1].success is True
        assert outcome.completed[0][1].resource_suspensions == [("api", 5.0)]
        assert outcome.salvaged_signals == []

    def test_abort_interrupted_result_not_treated_as_terminal(self, tmp_path):
        """interrupted 报告按「未完成」处理：不进 completed，走 cancelled。"""
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "interrupted")
        channel.journal.write_result_atomic(
            job.uid,
            {"status": "interrupted", "error": "WORKER_INTERRUPTED: KeyboardInterrupt"},
            incarnation="inc_interrupted",
        )
        h = JobHandle(
            uid=job.uid, process=self.MockProcess(),
            deadline=time.monotonic() + 10, timeout=10, job=job,
            ipc_dir=str(tmp_path), incarnation="inc_interrupted",
        )

        outcome = channel.abort_in_flight([h])

        assert outcome.completed == [], "被中断报告不得作为终态提交"
        assert [handle.uid for handle in outcome.cancelled] == [job.uid]

    def test_abort_empty_handles_returns_empty_outcome(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        outcome = channel.abort_in_flight([])
        assert outcome.completed == []
        assert outcome.cancelled == []
        assert outcome.salvaged_signals == []


class TestCleanupInFlight:
    def test_cleanup_in_flight_kills_and_removes_ipc_files(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        job = Job("task", "residual")
        channel.journal.write_result_atomic(
            job.uid, {"status": "success"}, incarnation=_INCARNATION
        )
        channel.journal.record_signal(job.uid, "api", 1.0)
        handle = JobHandle(
            uid=job.uid, process=TestAbortInFlight.MockProcess(),
            deadline=time.monotonic() + 10, timeout=10, job=job,
            ipc_dir=str(tmp_path), incarnation=_INCARNATION,
        )

        channel.cleanup_in_flight([handle])

        assert not channel.journal.result_path(job.uid, _INCARNATION).exists()
        assert not channel.journal.signals_path(job.uid).exists()
