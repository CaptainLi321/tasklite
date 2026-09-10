"""崩溃恢复与鲁棒性回归测试。

覆盖三个关键场景：
1. ``_CommitCrashSignal`` 必须触发 ``_abort_in_flight`` + save_queue
   （否则 in-flight 子进程成为孤儿、资源永久泄漏）。
2. acquire 循环在 try 块内——部分 acquire 失败时已 acquire 资源必须释放；
   ``Job.__init__`` 拒绝负值/NaN/非数值资源。
3. 结果必须 JSON 可序列化（由落盘强制）+ IPC 文件原子写。
"""

import pickle as pickle_mod
import queue as queue_mod
import time
from datetime import datetime
from pathlib import Path

import pytest

from tasklite.engine.channel import (
    ExecutionChannel,
    _normalize_handler_result,
)
from tasklite.utils.ipc import ArtifactJournal
from tasklite.engine.resource import CapacityResource, Resource
from tasklite.exceptions import _CommitCrashSignal, _JobTerminated
from tasklite.models.context import TaskContext
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite
from tests.helpers import (
    _write_fake_result,
    FakeManager,
    make_fake_process_class,
    make_ipc_process_class,
    make_pipeline,
    patch_multiprocessing_for_fakes,
)


# ─── Job 资源值校验 ──────────────────────────────────────


class TestJobResourceValidation:
    """ 根因：负值/NaN/Inf 资源值必须在构造时拒绝。"""

    def test_negative_resource_amount_rejected(self):
        with pytest.raises(ValueError, match="non-negative"):
            Job("t", "id", resources={"cpu": -1.0})

    def test_nan_resource_amount_rejected(self):
        with pytest.raises(ValueError, match="finite"):
            Job("t", "id", resources={"cpu": float("nan")})

    def test_inf_resource_amount_rejected(self):
        with pytest.raises(ValueError, match="finite"):
            Job("t", "id", resources={"cpu": float("inf")})

    def test_non_numeric_resource_amount_rejected(self):
        with pytest.raises(TypeError, match="must be a number"):
            Job("t", "id", resources={"cpu": "lots"})

    def test_valid_resources_accepted(self):
        job = Job("t", "id", resources={"cpu": 1.0, "mem": 0})
        assert job.resources == {"cpu": 1.0, "mem": 0}

    def test_negative_resource_roundtrip_via_from_dict_rejected(self):
        # 磁盘脏数据（旧版本写入的负值资源）经 from_dict 反序列化时应被拒绝
        data = {
            "task_type": "t", "job_id": "id", "payload": {},
            "resources": {"cpu": -1.0}, "retries": 0, "max_retries": 3,
            "depends_on": [], "timeout": 60, "backoff_base": 2.0, "backoff_max": 300.0,
        }
        with pytest.raises(ValueError):
            Job.from_dict(data)


# ─── 部分 acquire 失败时资源释放 ─────────────────────────


class TestPartialAcquireRelease:
    """acquire 循环在 try 块内：第 N 个资源失败时，前 N-1 个必须释放。"""

    def test_second_acquire_failure_releases_first(self, tmp_path, monkeypatch):
        class ExplodingResource(Resource):
            """acquire 抛异常的坏资源（模拟负值/NaN 之外的校验失败）。"""

            def __init__(self, name):
                super().__init__(name)

            def can_acquire(self, amount):
                return True, 0.0

            def acquire(self, amount):
                raise ValueError(f"cannot acquire {amount} on {self.name}")

            def release(self, amount):
                pass

            def suspend(self, seconds):
                pass

        pipeline = make_pipeline(tmp_path)
        # 用真实 CapacityResource 作为第一个资源（会被 acquire 成功）
        cap = CapacityResource("slot", max_capacity=4.0)
        pipeline.add_resource(cap)
        pipeline.add_resource(ExplodingResource("bad"))
        pipeline.register_handler("t", lambda j, c: (True, {}),
                                  default_resources={"slot": 1.0})
        pipeline.enqueue([Job("t", "j1", payload={}, resources={"bad": 1.0})])

        from tasklite.taxonomy import ERR_NO_HANDLER as _ERR_NO_HANDLER  # noqa: F401

        # 合并后 resources = {slot:1, bad:1}；acquire slot 成功、acquire bad 抛异常
        with pytest.raises(Exception):
            pipeline.run()

        # 第一个资源 slot 已被释放：used 回到 0（而不是永久占用 1.0）
        assert cap.used == 0.0, (
            f"Resource 'slot' should be released after partial acquire failure, "
            f"used={cap.used}"
        )


# ─── _CommitCrashSignal 必须清理 in-flight ──────────────


class TestCommitCrashSignalCleanup:
    """_CommitCrashSignal 触发 _abort_in_flight + save_queue。"""

    def test_commit_crash_signal_triggers_abort_and_save(self, tmp_path, monkeypatch):
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: (True, {}))
        pipeline.enqueue([Job("t", "j1", payload={})])

        aborted = []
        original_abort = pipeline._runtime._recovery.abort_in_flight

        def spy_abort():
            aborted.append(True)
            return original_abort()

        monkeypatch.setattr(pipeline._runtime._recovery, "abort_in_flight", spy_abort)

        # 模拟后端 commit 失败 → _commit_failed_crash → _CommitCrashSignal。
        # 先捕获真实后端引用（替换后 pipeline.backend 指向伪造对象自身，
        # 委托 load_queue/save_queue 需指向真实后端避免递归）。
        real_backend = pipeline.backend
        class FailingBackend:
            def load_wall(self): return {}
            def load_failed(self): return {}
            def load_cursors(self): return {}
            def load_queue(self): return real_backend.load_queue()
            def save_queue(self, jobs): real_backend.save_queue(jobs)
            def commit_job_success(self, *a, **k): return False
            def commit_job_failure(self, *a, **k): return False
            def commit_retry(self, *a, **k): return False
            def commit_bulk_failure(self, *a, **k): return False
            def append_failed(self, *a, **k): return None
            def get_meta(self, key): return None
            def set_meta(self, key, value): return None

        pipeline.backend = FailingBackend()

        class StartFakeProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.target = target
                self.args = args
                self.exitcode = 0

            def start(self):
                # 写入完整 IPC 结果 → drain 拿到结果 → _complete_job →
                # commit_job_success 返回 False → _CommitCrashSignal
                if len(self.args) >= 4:
                    _write_fake_result(self.args[3], self.args[1].uid, {
                        "status": "success",
                        "raw_result": True,
                        "new_jobs": [],
                        "resource_suspensions": [],
                        "cursor_updates": {},
                    })

            def join(self, timeout=None): pass
            def is_alive(self): return False
            def kill(self): pass
            def close(self): pass

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=StartFakeProcess)

        with pytest.raises(_CommitCrashSignal):
            pipeline.run()

        # _abort_in_flight 必须被调用
        assert aborted, (
            "_abort_in_flight must run on _CommitCrashSignal; "
            "otherwise in-flight subprocesses leak"
        )
        # 队列已保存：job 在 on-disk 队列中（at-least-once）
        uids = [Job.from_dict(j).uid for j in pipeline.backend.load_queue()]
        assert "t::j1" in uids

    def test_payload_validation_commit_fail_no_double_release(self, tmp_path, monkeypatch):
        """承重墙回归测试：payload 校验失败 + commit 失败 → 资源恰好释放一次。

        Phase 2 承重墙约束：_dispatch_job 必须保留 ``except _CommitCrashSignal:
        raise`` 在 ``except Exception`` 之前。payload 校验失败路径先
        ``_release_acquired`` 再 commit；commit 返回 False 时 ``_commit_failed_crash``
        抛 ``_CommitCrashSignal``。若被 ``except Exception`` 捕获会**二次释放**
        资源（used 变负）。本测试断言资源账目恰好释放一次（used==0）。
        """
        from tasklite.taxonomy import validate_payload  # noqa: F401

        pipeline = make_pipeline(tmp_path)
        pipeline.add_resource(CapacityResource("slot", max_capacity=4.0))

        from typing import TypedDict
        class PayloadSchema(TypedDict):
            count: int

        # 用 payload_schema 注册 handler，job payload 非法（缺 count）→ 校验失败
        pipeline.register_handler(
            "t", lambda j, c: (True, {}),
            default_resources={"slot": 1.0},
            payload_schema=PayloadSchema,
        )
        pipeline.enqueue([Job("t", "j1", payload={}, resources={"slot": 1.0})])

        # 模拟 commit_job_failure 返回 False → _commit_failed_crash → 崩溃。
        # 先捕获真实后端引用（替换后 pipeline.backend 指向伪造对象自身，
        # 委托 load_queue/save_queue 需指向真实后端避免递归）。
        real_backend = pipeline.backend
        class FailingFailureBackend:
            def load_wall(self): return {}
            def load_failed(self): return {}
            def load_cursors(self): return {}
            def load_queue(self): return real_backend.load_queue()
            def save_queue(self, jobs): real_backend.save_queue(jobs)
            def commit_job_success(self, *a, **k): return False
            def commit_job_failure(self, *a, **k): return False
            def commit_retry(self, *a, **k): return False
            def commit_bulk_failure(self, *a, **k): return False
            def append_failed(self, *a, **k): return None
            def get_meta(self, key): return None
            def set_meta(self, key, value): return None

        pipeline.backend = FailingFailureBackend()
        monkeypatch.setattr("multiprocessing.Manager", lambda: FakeManager())
        monkeypatch.setattr("tasklite.pipeline.mp.Manager", lambda: FakeManager())

        with pytest.raises(_CommitCrashSignal):
            pipeline.run()

        # 资源账目必须恰好释放一次：used == 0（若有二次释放会变负或抛异常）
        slot = pipeline.resources["slot"]
        assert slot.used == 0.0, (
            f"Resource 'slot' must be released exactly once (no double release), "
            f"used={slot.used}"
        )


# ─── IPC 落盘文件读写 ─────────────────────────────────────────


class TestFileBasedIPC:
    """文件级 IPC：结果原子写、信号追加、损坏容错。"""

    def test_write_and_read_result_roundtrip(self, tmp_path):
        """结果原子写 → 可读回（无部分消息问题）。"""
        journal = ArtifactJournal(tmp_path)
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        journal.write_result_atomic(
            "t::j1", {"status": "success", "x": 1}, incarnation=inc
        )
        assert journal.result_path("t::j1", inc).exists()
        data = journal.read_result(journal.result_path("t::j1", inc))
        assert data == {"status": "success", "x": 1}

    def test_write_result_no_tmp_leftover(self, tmp_path):
        """原子写后 .tmp 文件被 rename 掉，不留残留。"""
        journal = ArtifactJournal(tmp_path)
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        journal.write_result_atomic(
            "t::j1", {"status": "success"}, incarnation=inc
        )
        assert not journal.result_tmp_path("t::j1", inc).exists()

    def test_read_missing_returns_none(self, tmp_path):
        """结果文件不存在 → None（父进程轮询未完成状态）。"""
        journal = ArtifactJournal(tmp_path)
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        assert journal.read_result(journal.result_path("t::ghost", inc)) is None

    def test_read_corrupt_returns_none(self, tmp_path):
        """损坏的结果文件 → None（宁可重跑，不可崩）。"""
        journal = ArtifactJournal(tmp_path)
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        p = journal.result_path("t::j1", inc)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("not json {{{")
        assert journal.read_result(p) is None

    def test_append_and_read_signals(self, tmp_path):
        """suspend 信号追加 → 读取并删除（排空语义）。"""
        journal = ArtifactJournal(tmp_path)
        journal.record_signal("t::j1", "api", 30.0)
        journal.record_signal("t::j1", "api", 5.0)
        signals = journal.drain_signals("t::j1")
        assert signals == [("api", 30.0), ("api", 5.0)]
        # 排空后文件删除
        assert not (tmp_path / "t::j1.signals.jsonl").exists()
        # 再次读取为空
        assert journal.drain_signals("t::j1") == []

    def test_cleanup_removes_all_files(self, tmp_path):
        """cleanup 删除结果 + 信号 + 临时文件。"""
        journal = ArtifactJournal(tmp_path)
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        journal.write_result_atomic(
            "t::j1", {"status": "success"}, incarnation=inc
        )
        journal.record_signal("t::j1", "api", 1.0)
        journal.cleanup_ipc_files("t::j1")
        assert list(Path(tmp_path).iterdir()) == []


# ─── _finalize_process kill 竞态 ──────────────────────────────


class TestFinalizeProcessKillRace:
    """kill 抛 ProcessLookupError（进程在 is_alive 与 kill 之间退出）
    时，join 收割与 close 不能被跳过（防止异常吞掉导致僵尸 + fd 泄漏）。"""

    def test_kill_race_still_joins_and_closes(self):
        class RaceProcess:
            def __init__(self):
                self.joined = False
                self.closed = False

            def is_alive(self):
                return True

            def kill(self):
                raise ProcessLookupError("no such process")  # 竞态：进程已退出

            def join(self, timeout=None):
                self.joined = True

            def close(self):
                self.closed = True

        p = RaceProcess()
        ExecutionChannel._finalize_process(p)
        assert p.joined, "join must still be called after kill race (reap zombie)"
        assert p.closed, "close must still be called after kill race"


# ─── submit 异常路径清理 ────────────────────────────────────


class TestSubmitStartFailureCleanup:
    """Process 创建/start 抛异常时，已创建的资源必须清理（避免泄漏），
    然后 re-raise（验证 ipc_dir 目录创建失败传播）。"""

    def test_submit_start_failure_reraises(self, tmp_path):
        class ExplodingCtx:
            def Process(self, *args, **kwargs):
                raise RuntimeError("process start failed")

        exec_ = ExecutionChannel(mp_ctx=ExplodingCtx(), ipc_dir=str(tmp_path))
        ctx = TaskContext(Job("t", "j1", payload={}), set(), set(), {})
        with pytest.raises(RuntimeError, match="process start failed"):
            exec_.submit(lambda j, c: True, Job("t", "j1", payload={}), ctx, 60, [])

    def test_submit_requires_ipc_dir(self):
        """未配置 ipc_dir → 明确报错（不静默 fallback）。"""
        exec_ = ExecutionChannel(mp_ctx=type("Ctx", (), {"Process": lambda *a, **k: None})())
        ctx = TaskContext(Job("t", "j1", payload={}), set(), set(), {})
        with pytest.raises(ValueError, match="ipc_dir"):
            exec_.submit(lambda j, c: True, Job("t", "j1", payload={}), ctx, 60, [])


# ─── result_meta JSON 可序列化预检 ─────────────────────────────


class TestNormalizeResultJsonSerializable:
    """dict 元数据必须 JSON 可序列化——含 bytes/datetime 等非序列化
    值的 dict 会让 SQLite commit_job_success 失败 → _CommitCrashSignal →
    无限重启循环。预检后坏 dict 直接进 DLQ。"""

    def test_dict_with_bytes_metadata_rejected(self, tmp_path):
        # 实例方法转发层已删——直接测 executor 模块级实现。
        success, meta = _normalize_handler_result({"data": b"raw"})
        assert success is False
        assert "not JSON-serializable" in meta["error"]

    def test_tuple_with_datetime_metadata_rejected(self, tmp_path):
        success, meta = _normalize_handler_result((True, {"ts": datetime.now()}))
        assert success is False
        assert "not JSON-serializable" in meta["error"]

    def test_plain_dict_still_accepted(self, tmp_path):
        success, meta = _normalize_handler_result({"a": 1, "b": "x"})
        assert success is True
        assert meta == {"a": 1, "b": "x"}

    def test_failed_tuple_with_bad_meta_rejected(self, tmp_path):
        success, meta = _normalize_handler_result((False, {"blob": bytes(3)}))
        assert success is False
        assert "not JSON-serializable" in meta["error"]


# ─── Deadlock: commit_bulk_failure 失败必须崩溃而非静默 break ─────


def _ok_handler(job, ctx):
    return True


class TestDeadlockBulkFailureCrashes:
    """死锁处理中 commit_bulk_failure 失败必须抛 _CommitCrashSignal。

    未提交且有剩余队列时的状态判断：
    committed=False 时静默 break——run() 正常返回但死锁 job 永久滞留
    队列，每次 run 重复死锁循环，无任何错误信号。仿 systemd
    ``transaction_activate``（失败必然上抛），统一走崩溃契约。
    """

    def test_deadlock_bulk_failure_raises(self, tmp_path, monkeypatch):
        from tasklite.backend.sqlite_backend import SQLiteStateBackend

        class FailingBulkBackend(SQLiteStateBackend):
            def commit_bulk_failure(self, uids_metas):
                return False  # 模拟环境故障（磁盘满/锁）

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        # 自依赖 → 确定性死锁 → _handle_deadlock → commit_bulk_failure
        pipeline.enqueue([Job("t", "j1", depends_on=["t::j1"])])

        aborted = []
        original_abort = pipeline._runtime._recovery.abort_in_flight

        def spy_abort():
            aborted.append(True)
            return original_abort()

        monkeypatch.setattr(pipeline._runtime._recovery, "abort_in_flight", spy_abort)
        pipeline.backend = FailingBulkBackend(pipeline.backend.path)

        with pytest.raises(_CommitCrashSignal, match="commit_bulk_failure"):
            pipeline.run()

        assert aborted, "commit 失败必须走崩溃契约（_abort_in_flight 被调用）"
        # at-least-once：死锁 job 保留在 on-disk 队列，下次 run 可重试
        uids = [Job.from_dict(j).uid for j in pipeline.backend.load_queue()]
        assert "t::j1" in uids

    def test_deadlock_bulk_failure_success_path_unchanged(self, tmp_path):
        """commit_bulk_failure 成功时行为不变：死锁 job 进 DLQ、剩余队列保留。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", _ok_handler)
        pipeline.enqueue([
            Job("t", "j1", depends_on=["t::j1"]),   # 自依赖 → 死锁
            Job("t", "ok"),                          # 环外，正常运行
        ])
        pipeline.run()

        failed = pipeline.backend.load_failed()
        wall = pipeline.backend.load_wall()
        assert "t::j1" in failed
        assert "t::ok" in wall
        assert "t::ok" not in failed

    def test_nan_result_meta_rejected_not_committed(self, tmp_path, monkeypatch):
        """handler 返回含 NaN 的 result_meta 必须被拒绝进 DLQ。

        allow_nan=False 确保 NaN/Infinity 在子进程侧即判失败，
        防止非标准 JSON 写入结果文件。
        """
        from tasklite.engine.channel import _normalize_handler_result
        ok, meta = _normalize_handler_result({"size": float("nan")})
        assert ok is False, "NaN result_meta 必须被拒绝"
        assert "not JSON-serializable" in meta["error"]

    def test_nan_in_result_tuple_rejected(self):
        """ 回归：tuple(bool, dict) 的 dict 含 NaN 同样被拒绝。"""
        from tasklite.engine.channel import _normalize_handler_result
        ok, meta = _normalize_handler_result((True, {"v": float("inf")}))
        assert ok is False, "Inf result_meta 必须被拒绝"
        assert "not JSON-serializable" in meta["error"]

    def test_write_result_atomic_rejects_nan(self, tmp_path):
        """ 回归：write_result_atomic 写含 NaN 的结果必须抛（不落非标准 JSON）。"""
        ipc_dir = tmp_path / "ipc"
        journal = ArtifactJournal(ipc_dir)
        inc = "deadbeefdeadbeefdeadbeefdeadbeef.1"
        with pytest.raises(ValueError):
            journal.write_result_atomic(
                "t::j1",
                {"status": "success", "raw_result": {"v": float("nan")}},
                incarnation=inc,
            )
        # 原子写失败 → 无残留文件（tmp 被清理）
        assert not journal.result_path("t::j1", inc).exists()


class TestDispatchFailureThreeStrike:
    """dispatch 阶段失败（submit pickle/启动报错）3-strike。

    修复前：submit 失败 → requeue + re-raise → run() 崩溃 → 重启同 job 同
    handler → **无限崩溃重启循环**（不可 pickle 的 lambda handler 无逃生口）。
    修复后：独立 _dispatch_failures 计数，达阈值 DLQ（DISPATCH_FAILURE）。"""

    def test_submit_failure_reaches_dlq_not_infinite_crash(self, tmp_path, monkeypatch):
        import pickle as pickle_mod
        import sqlite3 as sqlite3_mod
        import json as json_mod

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        # 注入 _dispatch_failures=2 → 本次 dispatch 失败即达 3-strike（独立计数）
        jd = Job("t", "j1", payload={}).to_dict()
        jd["runtime"] = {"_dispatch_failures": 2}
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json_mod.dumps(jd),))
        conn.commit()
        conn.close()

        def boom_submit(*a, **kw):
            raise pickle_mod.PicklingError("cannot pickle lambda handler")
        monkeypatch.setattr(pipeline.channel, "submit", boom_submit)

        # run 必须正常返回（DLQ 而非崩溃）
        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "t::j1" in failed, f"3-strike 应 DLQ: {failed}"
        assert failed["t::j1"]["error"] == "DISPATCH_FAILURE"
        assert pipeline.backend.load_queue() == []
        assert pipeline.stats["failed"] == 1

    def test_submit_failure_below_threshold_still_crashes(self, tmp_path, monkeypatch):
        """未达阈值（failures=1 < 3）仍 requeue + 崩溃（环境抖动语义）。"""
        import pickle as pickle_mod

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        pipeline.enqueue([Job("t", "j1", payload={})])

        def boom_submit(*a, **kw):
            raise pickle_mod.PicklingError("cannot pickle lambda handler")
        monkeypatch.setattr(pipeline.channel, "submit", boom_submit)

        import pytest as pytest_mod
        with pytest_mod.raises(pickle_mod.PicklingError):
            pipeline.run()


class TestCommitFailuresPreservation:
    """retry 保留 _commit_failures + dispatch 3-strike 钩子。"""

    def test_retry_preserves_commit_failures_counter(self, tmp_path, monkeypatch):
        """Job.to_dict() 不保留下划线字段——业务 retry 路径
        若不清恢复 _commit_failures，dispatch/commit 失败的 3-strike 计数会被
        清零 → 无限崩溃重启循环。修复后 retry_dict 保留计数。"""
        import sqlite3 as sqlite3_mod
        import json as json_mod

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        # 注入 _commit_failures=2（模拟此前 dispatch/commit 已失败 2 次）。
        # backoff_base=0.01：加速测试——否则真实退避等待 2+4+8=14s
        # （time.sleep mock 无效：退避用 monotonic 判定，mock sleep 只去掉
        # 主循环休眠、不缩短真实等待）。
        jd = Job("t", "j1", payload={}).to_dict()
        jd["runtime"] = {"_commit_failures": 2}
        jd["backoff_base"] = 0.01
        jd["backoff_max"] = 0.01
        jd["max_retries"] = 3
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json_mod.dumps(jd),))
        conn.commit()
        conn.close()

        captured = {}
        real_commit_retry = pipeline.backend.commit_retry

        def fake_commit_retry(uid, retry_dict, front=False):
            captured["retry_dict"] = retry_dict
            return real_commit_retry(uid, retry_dict, front=front)
        monkeypatch.setattr(pipeline.backend, "commit_retry", fake_commit_retry)

        RetryProcess = make_ipc_process_class(results=[{"status": "retry", "error": "business transient"}])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=RetryProcess)
        monkeypatch.setattr("time.sleep", lambda s: None)

        pipeline.run()

        assert "retry_dict" in captured
        assert captured["retry_dict"]["runtime"].get("_commit_failures") == 2, \
            f"retry_dict 必须保留 _commit_failures 计数（runtime 命名空间），实际: {captured['retry_dict'].get('runtime')}"
        # 对称路径：退避双时钟**写入侧**双字段同落盘——
        # retry_dict 必须同时携带 _backoff_until（monotonic）与
        # _backoff_wall_deadline（wall-clock）；漏写任一会使重启后退避
        # 语义错误（加载期只信任 wall_deadline，monotonic 归零失效）。
        backoff_keys = sorted(k for k in captured["retry_dict"]["runtime"] if k.startswith("_backoff"))
        assert {"_backoff_until", "_backoff_wall_deadline"} <= set(backoff_keys), \
            f"retry_dict 必须双写退避字段，实际: {backoff_keys}"

    def test_dispatch_3strike_dlq_fires_hook(self, tmp_path, monkeypatch):
        """dispatch 3-strike DLQ 必须触发 on_job_completed
        钩子（「每个 job 终结钩子恰好一次」）——此前新增终态漏触发。"""
        import pickle as pickle_mod
        import sqlite3 as sqlite3_mod
        import json as json_mod

        calls = []
        pipeline = make_pipeline(tmp_path)
        pipeline.on_job_completed = (
            lambda uid, meta, success, going_to_retry: calls.append((uid, meta, success, going_to_retry))
        )
        pipeline.register_handler("t", lambda j, c: True)
        jd = Job("t", "j1", payload={}).to_dict()
        jd["runtime"] = {"_dispatch_failures": 2}
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json_mod.dumps(jd),))
        conn.commit()
        conn.close()

        def boom_submit(*a, **kw):
            raise pickle_mod.PicklingError("cannot pickle lambda handler")
        monkeypatch.setattr(pipeline.channel, "submit", boom_submit)

        pipeline.run()

        assert len(calls) == 1, f"钩子必须恰好一次，实际 {len(calls)}: {calls}"
        uid, meta, success, going_to_retry = calls[0]
        assert uid == "t::j1"
        assert meta.get("error") == "DISPATCH_FAILURE"
        assert success is False


class TestLastRetryErrorPreservation:
    """_last_retry_error 保留旧值 + 条件覆盖
    零测试覆盖——三个变异体（删保留行 / 删整个覆盖逻辑 / 无条件覆盖）都不会被
    现有测试杀死。补直接断言。"""

    def _setup(self, tmp_path, monkeypatch, retry_error, is_lock_conflict=False):
        import sqlite3 as sqlite3_mod
        import json as json_mod
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        # 注入旧业务错误（模拟此前业务 retry 已持久化）
        jd = Job("t", "j1", payload={}).to_dict()
        jd["runtime"] = {"_last_retry_error": "old business error", "_commit_failures": 2}
        jd["backoff_base"] = 0.01
        jd["backoff_max"] = 0.01
        jd["max_retries"] = 3
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json_mod.dumps(jd),))
        conn.commit()
        conn.close()

        captured = {}
        real_commit_retry = pipeline.backend.commit_retry

        def fake_commit_retry(uid, retry_dict, front=False):
            captured["retry_dict"] = retry_dict
            return real_commit_retry(uid, retry_dict, front=front)
        monkeypatch.setattr(pipeline.backend, "commit_retry", fake_commit_retry)

        result = {"status": "retry", "error": retry_error}
        if is_lock_conflict:
            # 结构化字段（判定端不再 startswith 前缀）——框架锁冲突须
            # 带 lock_conflict=True 才会被识别为零计数重试。
            result["lock_conflict"] = True
        LockRetryProcess = make_ipc_process_class(results=[
            result,
            {"status": "success", "raw_result": True,
             "new_jobs": [], "resource_suspensions": [], "cursor_updates": {}},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=LockRetryProcess)
        monkeypatch.setattr("time.sleep", lambda s: None)
        pipeline.run()
        assert "retry_dict" in captured, "retry 必须触发 commit_retry"
        return captured["retry_dict"]

    def test_lock_conflict_preserves_previous_business_error(self, tmp_path, monkeypatch):
        """LOCK_CONFLICT 重试必须保留已持久化的旧业务错误——变异体（删保留行
        或无条件覆盖）下此断言失败。"""
        retry_dict = self._setup(
            tmp_path, monkeypatch,
            "LOCK_CONFLICT: another execution body holds t::j1 lock",
            is_lock_conflict=True,  # 框架锁冲突须带结构化字段（判定端不再 startswith）
        )
        assert retry_dict["runtime"].get("_last_retry_error") == "old business error", \
            f"LOCK_CONFLICT 不得覆盖/丢失旧业务错误（runtime），实际: {retry_dict['runtime'].get('_last_retry_error')!r}"

    def test_business_retry_overwrites_previous_error(self, tmp_path, monkeypatch):
        """业务 retry（非 LOCK_CONFLICT）必须用新错误覆盖旧值——变异体
        （删除覆盖分支）下旧值保留但新值不写，断言失败。"""
        retry_dict = self._setup(tmp_path, monkeypatch, "new business error")
        assert retry_dict["runtime"].get("_last_retry_error") == "new business error", \
            f"业务 retry 应覆盖为新错误（runtime），实际: {retry_dict['runtime'].get('_last_retry_error')!r}"


class TestLockConflictBudgetExhaustedSelfRecovers:
    """孤儿锁冲突豁免 max_retries 预算判定。

    场景：handler 一次都没跑、retries 预算由**此前业务失败**耗尽（如孤儿
    锁的上一轮业务 RetryError 已把 retries 推到 max_retries），最后一次
    尝试撞上孤儿锁（lock_conflict=True 的 retry 结果）。锁冲突是框架瞬态
    （同 uid 孤儿执行体仍持锁，孤儿死后重跑本可成功），预算耗尽也必须
    豁免 DLQ 回队自恢复——否则与「孤儿死后自恢复、不烧预算」的设计意图
    矛盾。变异体（预算检查删去 lock_conflict 豁免）下 job 直接进 DLQ，
    本测试红。
    """

    def _setup(self, tmp_path, monkeypatch):
        import sqlite3 as sqlite3_mod
        import json as json_mod
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        # retries 预算已由此前业务失败耗尽（retries == max_retries == 3）
        jd = Job("t", "j1", payload={}).to_dict()
        jd["retries"] = 3
        jd["max_retries"] = 3
        jd["runtime"] = {"_last_retry_error": "old business error"}
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json_mod.dumps(jd),))
        conn.commit()
        conn.close()

        # 最后一次尝试撞孤儿锁（结构化字段，判定端只读该字段——
        # 与判定语义一致，非 startswith 前缀匹配）；随后孤儿死亡，重跑成功
        LockConflictProcess = make_ipc_process_class(results=[
            {"status": "retry",
             "error": "LOCK_CONFLICT: another execution body holds t::j1 lock",
             "lock_conflict": True},
            {"status": "success", "raw_result": True,
             "new_jobs": [], "resource_suspensions": [], "cursor_updates": {}},
        ])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=LockConflictProcess)
        monkeypatch.setattr("time.sleep", lambda s: None)
        pipeline.run()
        return pipeline

    def test_lock_conflict_at_budget_limit_requeues_not_dlq(self, tmp_path, monkeypatch):
        """max_retries 耗尽 + lock_conflict → 回队自恢复（孤儿死后成功），不进 DLQ。

        变异体（预算检查删去 lock_conflict 豁免）下：job 走 DLQ 分支，
        本断言失败。"""
        pipeline = self._setup(tmp_path, monkeypatch)
        entries = pipeline.list_dlq()
        by_uid = {e.uid: e for e in entries}
        assert "t::j1" not in by_uid, \
            f"孤儿锁冲突须豁免预算判定回队自恢复，不得进 DLQ，实际 DLQ: {list(by_uid)}"
        wall = pipeline.backend.load_wall()
        assert "t::j1" in wall, "孤儿死后重跑应成功进 wall"


class TestDispatchExceptionEntryRegistered:
    """except Exception 分支 entry 已注册时不得二次 requeue。

    变异体（删 _in_flight.pop / unregister 行）下：entry 留在 _in_flight →
    raise 后 _abort_in_flight 对该 entry 二次 requeue → 内存队列重复 uid。
    触发面：register_in_flight 的 DEBUG 断言失败（状态已损坏）——本测试
    用 monkeypatch 模拟该断言失败。"""

    def test_dispatch_exception_entry_registered_no_double_requeue(self, tmp_path, monkeypatch):
        """断言必须读**内存队列**而非磁盘队列。

        变异体（删 requeue 分支的 ``_in_flight.pop``）下：entry 残留
        ``self._in_flight`` → 抛异常后 ``_abort_in_flight`` 对该 entry
        **二次 requeue** → 内存队列出现两条 t::j1。磁盘断言被掩盖：
        ``_save_queue_crash_safe`` 以磁盘为基准、按 uid 把内存重复条目
        合并成一条 → 磁盘恒 1 条 → 变异体存活。
        内存队列 ``pipeline.state.queue`` 无去重掩盖，直接暴露二次 requeue。"""
        from tasklite.models.state import PipelineState
        import pytest as pytest_mod

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        pipeline.enqueue([Job("t", "j1", payload={})])

        # 模拟 register_in_flight 的 DEBUG 断言失败：self._in_flight[uid]=entry
        # 已执行、state.register_in_flight 抛 AssertionError → except Exception
        def boom_register(self, uid):
            raise AssertionError("simulated DEBUG assertion failure")
        monkeypatch.setattr(PipelineState, "register_in_flight", boom_register)

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        with pytest_mod.raises(AssertionError):
            pipeline.run()

        # 内存队列不得出现重复 uid（变异体：二次 requeue → 两条 t::j1）。
        # 注意：不能断言磁盘队列——_save_queue_crash_safe 按 uid 去重把
        # 重复条目合并成一条，变异体下磁盘仍为 1 条（断言被掩盖）。
        q = pipeline._runtime.state.queue
        uids = [f"{jd['task_type']}::{jd['job_id']}" for jd in q]
        assert uids.count("t::j1") <= 1, f"队列不得出现重复 uid: {uids}"

    def test_dispatch_exception_dlq_branch_pops_entry(self, tmp_path, monkeypatch):
        """DLQ 分支的 ``_in_flight.pop`` 无防御测试覆盖。

        触发条件：entry 已注册（``self._in_flight`` 含 uid——register_in_flight
        的 DEBUG 断言失败）+ failures≥3（注入 ``_dispatch_failures=2`` →
        本次 dispatch 失败即达 3-strike，独立计数）→ 走 DLQ 分支而非
        requeue 分支。此前
        failures=1 只走 requeue 分支；boom_submit 在 submit 抛异常时 entry
        从未注册（pop 是 no-op）——两条现有路径都摸不到本分支的 pop。
        变异体（删 DLQ 分支 pop）下：entry 残留 ``self._in_flight`` → 下一轮
        drain 把残留 entry 当 in-flight 回收 → ``_complete_job`` →
        ``_apply_result`` 的 DEBUG 身份断言崩（uid 早已注销）→ run() 抛
        AssertionError。"""
        from tasklite.models.state import PipelineState
        import sqlite3 as sqlite3_mod
        import json as json_mod

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        # 注入 _dispatch_failures=2 → 本次 dispatch 失败即达 3-strike → DLQ 分支
        # （参考 TestDispatchFailureThreeStrike 的注入手法：直接写 job_dict 进磁盘队列）
        jd = Job("t", "j1", payload={}).to_dict()
        jd["runtime"] = {"_dispatch_failures": 2}
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json_mod.dumps(jd),))
        conn.commit()
        conn.close()

        # 模拟 register_in_flight 的 DEBUG 断言失败：self._in_flight[uid]=entry
        # 已执行、state.register_in_flight 抛 AssertionError → except Exception
        def boom_register(self, uid):
            raise AssertionError("simulated DEBUG assertion failure")
        monkeypatch.setattr(PipelineState, "register_in_flight", boom_register)

        FakeP = make_fake_process_class("success")
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=FakeP)

        # 固定代码下 run 正常返回（DLQ 分支 return 而非 raise——失败已在
        # except Exception 内消化）；变异体下残留 entry 被 drain 回收 →
        # _apply_result 身份断言崩 → 此处抛 AssertionError（测试红 = 变异体被杀）。
        pipeline.run()

        # DLQ 分支必须从 in_flight 移除 entry（变异体删 pop 后残留）
        assert "t::j1" not in pipeline._runtime.in_flight, \
            f"DLQ 分支必须移除 in_flight entry: {list(pipeline._runtime.in_flight)}"
        # job 进 DLQ（failed 集合 + 后端记录）
        failed = pipeline.backend.load_failed()
        assert "t::j1" in failed, f"3-strike 应 DLQ: {failed}"
        assert failed["t::j1"]["error"] == "DISPATCH_FAILURE"
        assert pipeline.backend.load_queue() == []
        assert pipeline.stats["failed"] == 1


class TestDispatchCommitCountersIndependent:
    """`_dispatch_failures` 与 `_commit_failures` 独立
    3-strike——曾 dispatch 失败 2 次的 job，之后 1 次 commit 失败**不得**
    因累计达阈值误进 DLQ（错误码误导排障）。"""

    def test_dispatch_failures_dont_leak_into_commit_counter(self, tmp_path, monkeypatch):
        """dispatch 失败 2 次（_dispatch_failures=2）+ commit 失败 1 次（_commit_failures=1）
        → 本次 dispatch 失败使 _dispatch_failures=3 达自身阈值 → DLQ，错误码
        DISPATCH_FAILURE（commit 计数不参与 dispatch 阈值判定——独立计数）。"""
        import sqlite3 as sqlite3_mod
        import json as json_mod
        from tasklite.pipeline import _CommitCrashSignal

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        jd = Job("t", "j1", payload={}).to_dict()
        # dispatch 已失败 2 次（独立计数），commit 失败 1 次（独立计数）
        jd["runtime"] = {"_dispatch_failures": 2, "_commit_failures": 1}
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json_mod.dumps(jd),))
        conn.commit()
        conn.close()

        def boom_submit(*a, **kw):
            raise pickle_mod.PicklingError("cannot pickle lambda handler")
        monkeypatch.setattr(pipeline.channel, "submit", boom_submit)

        # 本次 dispatch 失败 → _dispatch_failures=3 → 达 dispatch 阈值 DLQ
        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "t::j1" in failed, "dispatch 3-strike（独立计数）应 DLQ"
        assert failed["t::j1"]["error"] == "DISPATCH_FAILURE", \
            f"错误码必须是 DISPATCH_FAILURE（真实根因），实际: {failed['t::j1'].get('error')}"

    def test_commit_failures_dont_leak_into_dispatch_counter(self, tmp_path, monkeypatch):
        """commit 失败 2 次（_commit_failures=2）→ dispatch 失败 1 次
        → dispatch 计数从 1 开始（未达 3）→ requeue 不 DLQ。"""
        import sqlite3 as sqlite3_mod
        import json as json_mod

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        jd = Job("t", "j1", payload={}).to_dict()
        jd["runtime"] = {"_commit_failures": 2, "_dispatch_failures": 0}
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json_mod.dumps(jd),))
        conn.commit()
        conn.close()

        def boom_submit(*a, **kw):
            raise pickle_mod.PicklingError("cannot pickle lambda handler")
        monkeypatch.setattr(pipeline.channel, "submit", boom_submit)

        # 本次 dispatch 失败 → _dispatch_failures=1（独立）→ 未达阈值 → requeue
        # 但 submit 每次抛异常，run 会崩溃（requeue + re-raise）——验证
        # 崩溃后 job 保留在队列而非 DLQ（计数独立，未混淆阈值）。
        with pytest.raises(Exception):
            pipeline.run()

        assert "t::j1" not in pipeline.backend.load_failed(), \
            "commit 计数不得泄漏到 dispatch 阈值判定"
        q = pipeline.backend.load_queue()
        assert any(j.get("runtime", {}).get("_dispatch_failures") == 1 for j in q), \
            f"dispatch 计数应从 0→1（独立），实际队列: {q}"

    def test_dispatch_failures_preserved_across_retry(self, tmp_path, monkeypatch):
        """`_dispatch_failures` 跨业务 retry
        整包复制保留——dispatch 失败 1 次后 job 重新执行走业务 retry，计数
        不丢（否则后续 dispatch 失败从 0 重新累计，3-strike 语义被破坏）。"""
        import sqlite3 as sqlite3_mod
        import json as json_mod

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)
        jd = Job("t", "j1", payload={}).to_dict()
        # dispatch 已失败 1 次（此前 dispatch 3-strike 未达阈值 requeue）
        jd["runtime"] = {"_dispatch_failures": 1}
        jd["backoff_base"] = 0.01
        jd["backoff_max"] = 0.01
        jd["max_retries"] = 3
        conn = sqlite3_mod.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('t::j1', 0, ?)",
                     (json_mod.dumps(jd),))
        conn.commit()
        conn.close()

        captured = {}
        real_commit_retry = pipeline.backend.commit_retry

        def fake_commit_retry(uid, retry_dict, front=False):
            captured["retry_dict"] = retry_dict
            return real_commit_retry(uid, retry_dict, front=front)
        monkeypatch.setattr(pipeline.backend, "commit_retry", fake_commit_retry)

        RetryProcess = make_ipc_process_class(results=[{"status": "retry", "error": "business transient"}])
        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=RetryProcess)
        monkeypatch.setattr("time.sleep", lambda s: None)

        pipeline.run()

        assert "retry_dict" in captured
        assert captured["retry_dict"]["runtime"].get("_dispatch_failures") == 1, \
            f"retry 必须保留 _dispatch_failures 计数（整包复制），实际: {captured['retry_dict'].get('runtime')}"


# ─── _run_loop 承重网 _JobTerminated 分支 — 未绑定 e 修复 ─────────


class TestRunLoopJobTerminatedNet:
    """`_run_loop` 的 `except _JobTerminated` 承重网
    分支此前写 `{e}` 却未绑定异常变量（`except _JobTerminated:` 无 `as e`），
    一旦该防御分支被触发会抛 `NameError`，且其想守护的清理语义
    （`_abort_in_flight` + `_save_queue_crash_safe` + fail-loud re-raise）被跳过。

    本测试直接让 `_JobTerminated` 从 run 主循环逃逸，验证修复后：
    1. 不再抛 NameError，而是按契约 fail-loud re-raise `_JobTerminated`；
    2. `_abort_in_flight` 与 `_save_queue_crash_safe` 均被调用（崩溃安全契约）；
    3. 队列以磁盘为基准被 crash-safe 保存（at-least-once）。
    """

    def test_job_terminated_net_binds_exception_and_runs_cleanup(self, tmp_path, monkeypatch):
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: (True, {}))
        pipeline.enqueue([Job("t", "j1", payload={})])

        aborted = []
        saved = []
        original_abort = pipeline._runtime._recovery.abort_in_flight
        original_save = pipeline._runtime._recovery.save_queue_crash_safe

        def spy_abort():
            aborted.append(True)
            return original_abort()

        def spy_save():
            saved.append(True)
            return original_save()

        monkeypatch.setattr(pipeline._runtime._recovery, "abort_in_flight", spy_abort)
        monkeypatch.setattr(pipeline._runtime._recovery, "save_queue_crash_safe", spy_save)

        # 模拟未来新直调点漏承接：_JobTerminated 直接从 run 主体逃逸到承重网。
        def boom():
            raise _JobTerminated("job terminated outside expected handlers")
        monkeypatch.setattr(pipeline._runtime, "run_loop_impl", boom)

        # 修复后应按崩溃契约 fail-loud re-raise _JobTerminated，
        # 而非抛 NameError（原 bug：{e} 引用了未绑定的 e）。
        with pytest.raises(_JobTerminated):
            pipeline.run()

        assert aborted, "逃逸的 _JobTerminated 必须先 _abort_in_flight（杀 in-flight/requeue）"
        assert saved, "逃逸的 _JobTerminated 必须先 _save_queue_crash_safe（兼并以磁盘为准）"


# ─── 级联 commit_bulk_failure=False → _CommitCrashSignal ─────────


class TestCascadeBulkFailureCrash:
    """级联路径 `commit_bulk_failure` 返回 False
    必须走 3-strike 崩溃契约（`_CommitCrashSignal` + `_abort_in_flight`），
    而非静默吞掉——否则下游依赖失败 job 永久滞留队列、父失败信息丢失。

    已有的 commit_bulk_failure=False 用例是**单 job**（死锁路径）；本测试
    构造**级联**（1 父失败 → 多下游）场景，补齐对偶路径覆盖（Rule 4）。
    """

    def test_cascade_bulk_failure_raises(self, tmp_path, monkeypatch):
        from tasklite.backend.sqlite_backend import SQLiteStateBackend

        class FailingBulkBackend(SQLiteStateBackend):
            def commit_bulk_failure(self, uids_metas):
                return False  # 级联批量 DLQ 落盘失败（磁盘满/锁）

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("t", lambda j, c: True)

        # a 已失败（预置 failed）→ b 依赖 a（pending_dep_failure 直接 commit
        # 失败）；b 处理时 `_cascade_fail(b)` 批量级联到其下游 c——
        # 这才是 `_cascade_fail` 的 commit_bulk_failure 批量路径。
        # （b/c/d 若都直依赖 a 会走逐条 SKIP，不触发级联批量，故用链式。）
        a = Job("t", "a", payload={})
        b = Job("t", "b", payload={}, depends_on=[a.uid])
        c = Job("t", "c", payload={}, depends_on=[b.uid])
        pipeline.backend.commit_job_failure(a.uid, {"error": "injected"})
        pipeline.enqueue([b, c])

        aborted = []
        original_abort = pipeline._runtime._recovery.abort_in_flight

        def spy_abort():
            aborted.append(True)
            return original_abort()

        monkeypatch.setattr(pipeline._runtime._recovery, "abort_in_flight", spy_abort)
        pipeline.backend = FailingBulkBackend(pipeline.backend.path)

        # 级联批量 DLQ 失败 → _commit_failed_crash → _CommitCrashSignal
        with pytest.raises(_CommitCrashSignal, match="commit_bulk_failure"):
            pipeline.run()

        assert aborted, "级联 commit_bulk_failure=False 必须走崩溃契约（_abort_in_flight 被调用）"
        # at-least-once：下游 job 保留在 on-disk 队列（3-strike 未达阈值时保留）
        uids = [Job.from_dict(j).uid for j in pipeline.backend.load_queue()]
        assert c.uid in uids, "级联下游 c 必须保留在 on-disk 队列（崩溃后 at-least-once）"
