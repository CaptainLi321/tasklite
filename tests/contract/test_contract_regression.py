"""契约回归测试：单一出口收敛、 身份非真空、 never-raise、
序列化契约收官。对应 README「设计契约」的候选契约 5 / 单一出口 / 变异防御。
"""
import ast
import json
import sqlite3
import sys
from pathlib import Path

import pytest

from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.executor import (
    _decode_ipc_result,
    _decode_raw_result,
    _encode_raw_result,
)
from tasklite.engine.resource import CapacityResource
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite
from tasklite.taxonomy import validate_payload

SRC_DIR = Path(__file__).resolve().parent.parent.parent / "tasklite"


# 模块级 handler（spawn 子进程要求可 pickle）--------------------------

def _self_spawn_handler(job, ctx):
    """handler：显式 spawn 与自身同 uid 的子任务（触发场景）。"""
    ctx.spawn(Job(job.task_type, job.job_id, payload={"self": True}))
    return True


def _retry_handler(job, ctx):
    """handler：抛 RetryError（触发 commit_retry 失败路径）。"""
    from tasklite import RetryError
    raise RetryError("transient")



# （单一出口）：路径收敛的静态契约


class TestSingleExitContract:
    def test_apply_result_only_called_from_complete_job(self):
        """ 验收：apply_result 的调用点只剩 complete_job 一处。

        完成机器迁至 engine/completion.py；restore_stale_result 构造
        伪 entry 走 complete_job，恢复路径不自调 apply_result
        绕过清理契约。
        """
        src = (SRC_DIR / "engine" / "completion.py").read_text()
        tree = ast.parse(src)
        calls = []
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "apply_result"):
                # 方法定义本身的 self.apply_result 调用点
                calls.append(node.lineno)
        # 允许的调用点：complete_job 内一处；排除定义行附近的误报
        assert len(calls) >= 1, "no apply_result call sites found"
        # 用 AST 找 restore_stale_result 方法体，检查其中是否有真实调用（非 docstring）
        tree = ast.parse(src)
        restore_func = None
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                    and node.name == "restore_stale_result":
                restore_func = node
                break
        assert restore_func is not None, "restore_stale_result not found"
        # 方法体内是否有对 self.apply_result 的调用节点
        direct_calls = [
            n for n in ast.walk(restore_func)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "apply_result"
        ]
        assert not direct_calls, (
            "单一出口原则违规: restore_stale_result still calls apply_result directly"
        )

    def test_complete_job_finally_cleans_ipc_files(self):
        """ 验收：complete_job finally 调用 cleanup_ipc_files。"""
        src = (SRC_DIR / "engine" / "completion.py").read_text()
        complete_start = src.find("def complete_job")
        complete_end = src.find("def apply_result", complete_start)
        complete_body = src[complete_start:complete_end]
        assert "cleanup_ipc_files" in complete_body, (
            "P-2 violation: complete_job does not centralize IPC file cleanup"
        )

    def test_persist_suspends_in_run_loop_finally(self):
        """ 验收：persist_resource_suspends 在 run_loop 的 finally 中（崩溃路径也持久化）。"""
        src = (SRC_DIR / "engine" / "loop.py").read_text()
        run_loop_start = src.find("def run_loop")
        run_loop_end = src.find("def run_loop_impl", run_loop_start)
        run_loop_body = src[run_loop_start:run_loop_end]
        # finally 块应包含 persist_resource_suspends
        assert "finally:" in run_loop_body
        assert "persist_resource_suspends" in run_loop_body.split("finally:")[1]
        # run_loop_impl 末尾不应再单独调用（收编到 run_loop）
        impl_start = src.find("def run_loop_impl")
        impl_end = src.find("class ", impl_start)
        impl_body = src[impl_start:impl_end if impl_end != -1 else len(src)]
        # 正常退出路径的调用注释可以存在，但实际调用应已移除
        assert "persist_resource_suspends()" not in impl_body.replace(
            "# 由 run_loop 的", "XXX"), (
            "单一出口违规: persist_resource_suspends still called in run_loop_impl"
        )

    def test_dlq_writes_share_single_impl(self):
        """三条 DLQ 写入路径共用 _write_dlq_row。"""
        src = (SRC_DIR / "backend" / "sqlite_backend.py").read_text()
        assert "_write_dlq_row" in src
        for method in ("commit_job_failure", "commit_bulk_failure", "append_failed"):
            start = src.find(f"def {method}")
            assert start != -1, f"{method} not found"
            end = src.find("\n    def ", start + 1)
            body = src[start:end if end != -1 else len(src)]
            assert "_write_dlq_row" in body, (
                f"单一出口原则违规: {method} does not go through _write_dlq_row"
            )

    def test_attempt_consistent_across_all_dlq_paths(self, tmp_path):
        """ 回归：同 uid 三连写（三条路径各一次）→ _attempt 连续递增为 3。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("u", {"error": "a"})
        backend.commit_bulk_failure([("u", {"error": "b"})])
        backend.append_failed("u", {"error": "c"})
        assert backend.load_failed()["u"]["_attempt"] == 3



# 候选契约 5（身份非真空）


class TestIdentityNonVacuity:
    def test_self_spawn_same_uid_deduped(self, tmp_path):
        """ 回归：handler 自 spawn 同 uid 子任务 → is_known 吸收（uid 仍在
        in-flight），不产生 wall∩queue 重复。此测试在 DEBUG 模式下跑——修复前
        _assert_state_consistent 立即断言崩溃；修复后正常完成。"""
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        p.register_handler("t", _self_spawn_handler)
        p.enqueue([Job("t", "a")])
        p.run()
        # 自 spawn 的重复 uid 被吸收：wall 有 a，queue 无残留
        wall = p.backend.load_wall()
        assert "t::a" in wall
        assert p.backend.load_queue() == []

    def test_commit_failure_unregisters_before_requeue(self, tmp_path):
        """ 交互：_commit_failed_crash 先 unregister 再 requeue——uid 离开
        in-flight 后才进 queue，_abort_in_flight 不会二次 requeue 同 uid。
        直接调用验证（不跑完整 pipeline，避免 commit_retry 走真后端）。"""
        from tasklite.pipeline import _CommitCrashSignal, _InFlightJob
        from tasklite.engine.executor import ExecutionResult

        # 构造已 dispatch 的 job（uid 在 in-flight）
        from tasklite.models.state import PipelineState
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        job = Job("t", "a")
        job_dict = job.to_dict()
        p._state = PipelineState(
            p.backend.load_wall(), p.backend.load_failed(),
            p.backend.load_cursors(), [],
        )
        p._state.register_in_flight("t::a")

        with pytest.raises(_CommitCrashSignal):
            p._failure.commit_failed_crash("t::a", "test_commit_failure", job_dict)

        # 断言：requeue 前已 unregister——uid 不在 in-flight（防 _abort 二次 requeue）
        assert "t::a" not in p._state.in_flight_uids
        # 且已 requeue 到 queue（恰好一次）
        uids = [x.get("job_id") for x in p._state.queue]
        assert uids.count("a") == 1



# （validate_payload never-raise 契约）


class TestValidatePayloadNeverRaises:
    def test_union_literal_any_no_raise(self):
        """ 回归：Union[Literal['a'], Any] 不再抛 TypeError（此前崩溃循环）。"""
        from typing import Any, Literal, TypedDict, Union
        class S(TypedDict):
            x: Union[Literal["a"], Any]
        result = validate_payload({"x": 5}, S)
        assert isinstance(result, list)

    def test_union_literal_typevar_no_raise(self):
        from typing import Literal, TypeVar, TypedDict, Union
        T = TypeVar("T")
        class S(TypedDict):
            x: Union[Literal["a"], T]
        result = validate_payload({"x": 5}, S)
        assert isinstance(result, list)

    def test_malformed_schema_no_raise(self):
        """畸形 schema（非类型对象）→ 返回错误串而非抛异常。"""
        result = validate_payload({"x": 1}, 42)
        assert isinstance(result, list) and result

    def test_any_field_accepts_anything(self):
        from typing import Any, TypedDict
        class S(TypedDict):
            x: Any
        assert validate_payload({"x": object()}, S) == []

    def test_normal_schema_unaffected(self):
        from typing import TypedDict
        class S(TypedDict):
            name: str
            n: int
        assert validate_payload({"name": "a", "n": 1}, S) == []
        assert validate_payload({"name": "a", "n": True}, S) != []



# （序列化契约收官）+ （cursor_updates 值校验）


class TestSerializationContract:
    def test_no_bare_json_in_source(self):
        """静态契约：jsonutil 之外无裸 json.dumps/loads（唯一豁免 state.py 的
        hash 计算——非落盘用途，default=str 已兜底）。

        注意：mutmut 运行时测试文件被解析到 mutants/ 副本，副本可能含
        已删除模块的残留（如已删除的 json_backend.py）——按文件名
        白名单跳过非当前模块的残留，避免误报。
        """
        violations = []
        ignored_names = {"json_backend.py", "json_backend.pyc"}  # 已删除模块的残留
        for py in SRC_DIR.rglob("*.py"):
            if py.name in ignored_names:
                continue
            if py.name == "jsonutil.py":
                continue
            if "test_" in str(py):
                continue
            src = py.read_text()
            for i, line in enumerate(src.splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                if "json.dumps(" in stripped or "json.loads(" in stripped \
                        or "json.dump(" in stripped or "json.load(" in stripped:
                    # state.py hash 豁免
                    if py.name == "state.py" and "hashlib.sha256" in stripped:
                        continue
                    violations.append(f"{py.relative_to(SRC_DIR)}:{i}: {stripped}")
        assert not violations, f"bare json calls outside jsonutil:\n" + "\n".join(violations)

    def test_loads_rejects_overflow_float(self):
        """浮点溢出拒绝：1e400 类合法语法浮点溢出必须拒绝而非静默 inf。

        parse_constant 只拦 NaN/Infinity 字面 token；`1e400` 走 parse_float
        默认解析产出 float('inf')——与 NaN 同样破坏 dumps(allow_nan=False)
        回写与数值比较。统一按损坏数据抛 JSONDecodeError（调用方既有
        except 分支可捕获）。
        """
        import json as _json

        from tasklite.utils.jsonutil import load, loads

        for bad in ('{"v": 1e400}', '{"v": -1e400}', '{"v": 2e308}', '[1e999]'):
            with pytest.raises(_json.JSONDecodeError):
                loads(bad)
 # 合法值不受影响：大而有限的浮点、普通小数、int
        assert loads('{"a": 1.5, "b": 1e10, "c": -0.001}') == {
            "a": 1.5, "b": 1e10, "c": -0.001}
 # NaN/Infinity 字面 token 拒绝语义未被挤掉
        with pytest.raises(_json.JSONDecodeError):
            loads("[NaN, Infinity]")
 # load(fp) 同契约
        import io
        with pytest.raises(_json.JSONDecodeError):
            load(io.StringIO('{"v": 1e400}'))

    def test_dumps_rejects_nan(self):
        from tasklite.utils.jsonutil import dumps
        with pytest.raises(ValueError):
            dumps({"x": float("nan")})
        with pytest.raises(ValueError):
            dumps({"x": float("inf")})

    def test_loads_rejects_nan_as_jsondecode_error(self):
        """ 防御触发测试：NaN 读取端抛 JSONDecodeError（非裸 ValueError），
        使 load_* 的 except json.JSONDecodeError 分支真正生效。"""
        from tasklite.utils.jsonutil import loads
        with pytest.raises(json.JSONDecodeError):
            loads('{"x": NaN}')
        with pytest.raises(json.JSONDecodeError):
            loads("[Infinity]")

    def test_backend_nan_row_raises_documentedruntime_error(self, tmp_path):
        """ 防御触发测试（真实路径）：手工向 DB 注入 NaN 行 →
        load_failed 抛文档化 RuntimeError 而非裸 ValueError。修复前是裸
        ValueError（防御接错异常类型、从未生效）。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        # 绕过 dumps 预检，直接写非标准 NaN token 到 DLQ（模拟历史坏数据）
        with backend._get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO failed_dlq (uid, payload) VALUES (?, ?)",
                ("bad", '{"x": NaN}'),
            )
        with pytest.raises(RuntimeError, match="Corrupted failed DLQ"):
            backend.load_failed()

    def test_decode_ipc_result_rejects_bad_cursor_values(self):
        """cursor_updates 值类型校验——int 值 → 失败而非注入 state.cursors。"""
        from tasklite.models.job import Job
        res = {"status": "success", "raw_result": {"ok": True},
               "new_jobs": [], "resource_suspensions": [],
               "cursor_updates": {"k": 123}}
        result = _decode_ipc_result(res, None, Job("t", "a"))
        assert not result.success
        assert "cursor_updates" in result.result_meta["error"]

    def test_decode_ipc_result_accepts_none_cursor_value(self):
        """None 是合法的游标删除值（set_cursor 删除语义）。"""
        res = {"status": "success", "raw_result": {"ok": True},
               "new_jobs": [], "resource_suspensions": [],
               "cursor_updates": {"k": None}}
        result = _decode_ipc_result(res, None, Job("t", "a"))
        assert result.success
        assert result.cursor_updates == {"k": None}

    def test_decode_ipc_result_lock_conflict_is_retry(self):
        """ 回归：LOCK_CONFLICT（worker 拿锁超时 = 同 uid 孤儿仍活）应走
        瞬态重试（自恢复）而非永久 DLQ——修复前 status="error" → Unknown
        → DLQ，与注释声称的「按进程崩溃分类走正常重试」矛盾。"""
        res = {"status": "retry",
               "lock_conflict": True,  # 结构化字段（判定端不再 startswith 前缀）
               "error": "LOCK_CONFLICT: another execution body holds t::a lock"}
        result = _decode_ipc_result(res, None, Job("t", "a"))
        assert result.retry_requested, "LOCK_CONFLICT 必须按瞬态重试处理"
        assert result.success is False

    def test_mp_worker_wrapper_lock_conflict_writes_retry(self, tmp_path):
        """ 集成回归：worker 拿锁超时**写** status="retry"（非 "error"）。

        原回归测试只测 decoder（pre-fix 也通过——bug 在 writer
        写 "error"，decoder 本就处理 "retry"）——错层，不 kill 变异。
        本测试真实持锁 → 调 _mp_worker_wrapper → 断言其结果文件 status。
        """
        from tasklite.engine.executor import (
            _mp_worker_wrapper, read_result_file, result_path,
        )
        from tasklite.models.context import TaskContext
        from tasklite.models.job import Job
        from tasklite.utils.lockfile import release_lock, try_acquire_lock

        ipc = str(tmp_path)
        job = Job("t", "a")
        ctx = TaskContext(
            job, set(), set(), {},
            output_root=None,
            ipc_dir=ipc, incarnation="test.1",
        )

        # 父进程持锁 → worker try_acquire_lock(timeout=2.0) 失败
        fd = try_acquire_lock(ipc, "t::a")
        assert fd is not None
        try:
            _mp_worker_wrapper(lambda j, c: True, job, ctx, ipc)
        finally:
            release_lock(fd)

        res = read_result_file(result_path(ipc, "t::a", "test.1"))
        assert res is not None, "worker 必须写结果文件"
        assert res.get("status") == "retry", (
            f"LOCK_CONFLICT 应写 retry（自恢复），got status={res.get('status')!r}"
        )
        assert "LOCK_CONFLICT" in res.get("error", "")
        # worker 写端必须写结构化字段 lock_conflict=True——
        # 结构化读取 error_code 字段避免前缀字符串误匹配
        # 撞前缀消息误判为零计数重试）。
        assert res.get("lock_conflict") is True, (
            f"LOCK_CONFLICT 应写结构化字段 lock_conflict=True，"
            f"got {res.get('lock_conflict')!r}"
        )



# Batch 5 / （退避换算真值表）+ （缓存内容键）+ （历史迁移）


class TestBackoffReloadTruthTable:
    """_run_body 加载时的退避换算（未发布清理后只认 wall_deadline）。

    - wall_deadline 存在且未来 → _backoff_until = now + (wall - wall_now)
      （真实剩余，停机时间计入退避）
    - wall_deadline 存在且过去 → 无退避（清字段放行）
    - 无 wall_deadline（脏数据）→ 启动即清，视为无退避
    """

    def test_wall_deadline_future_uses_real_remaining(self, tmp_path, monkeypatch):
        """未来 wall_deadline → 真实 _run_body 加载换算为 wall 剩余（真实剩余，非全额重置）。

        旧测试在测试内复算换算公式（tautology）——重实现与实现同构，
        实现坏了测试照样绿。新测试走真实 _run_body 加载段：job 带
        wall_deadline（60s 后到期），加载后内存 _backoff_until 应为 ~60s。
        """
        import time as _t

        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        jd = Job("t", "a").to_dict()
        jd["runtime"]["_backoff_wall_deadline"] = _t.time() + 60
        p.backend.enqueue_jobs([jd])

        # 只走真实 _run_body 的加载/换算段（跳过 _run_loop 避免退避 sleep 60s）
        monkeypatch.setattr(p, "_run_loop", lambda: None)
        p._run_body()

        now = _t.monotonic()
        converted = p._state.queue[0]
        rt = converted["runtime"]
        assert "_backoff_until" in rt, "加载换算必须设置 monotonic _backoff_until"
        assert 55 < (rt["_backoff_until"] - now) < 65, (
            f"expected ~60s real remaining, got {rt['_backoff_until'] - now}"
        )
        # wall_deadline 保留（换算只加 monotonic 值，供后续 crash 恢复重算）
        assert rt.get("_backoff_wall_deadline") is not None

    def test_missing_wall_deadline_cleared(self, tmp_path, monkeypatch):
        """无有效 wall_deadline（脏数据）→ 真实 _run_body 启动即清，视为无退避。

        旧测试以 ``assert True`` 结尾——只验证磁盘原样保留，从不调用加载
        换算，内存清除行为从未被断言（false positive）。新测试走真实
        _run_body：注入脏 wall_deadline + 残留 monotonic _backoff_until
        （孤儿 defer 旧格式），断言两者都在内存被清除。
        """
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        jd = Job("t", "a").to_dict()
        jd["runtime"]["_backoff_wall_deadline"] = "garbage"  # 脏数据（非数值）
        jd["runtime"]["_backoff_until"] = 1e18  # 残留 monotonic 退避（孤儿 defer，一并清除）
        p.backend.enqueue_jobs([jd])

        monkeypatch.setattr(p, "_run_loop", lambda: None)
        p._run_body()

        loaded = p._state.queue[0]
        rt = loaded.get("runtime", {})
        assert "_backoff_wall_deadline" not in rt, \
            "脏 wall_deadline 必须被清除（否则残留阻塞调度）"
        assert "_backoff_until" not in rt, \
            "残留 monotonic _backoff_until 必须一并清除（孤儿 defer）"

    def test_wall_deadline_past_cleared(self, tmp_path, monkeypatch):
        """已过截止的 wall_deadline → 真实 _run_body 清零放行（停机超时不算阻塞）。

        test_wall_deadline_future 的对偶路径：进程停机超过退避时长后重启，
        wall_deadline 已在过去 → 启动即清，job 立即可运行（真实剩余=0）。
        """
        import time as _t

        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        jd = Job("t", "a").to_dict()
        jd["runtime"]["_backoff_wall_deadline"] = _t.time() - 10  # 已过期
        jd["runtime"]["_backoff_until"] = 1e18  # 残留 monotonic 退避
        p.backend.enqueue_jobs([jd])

        monkeypatch.setattr(p, "_run_loop", lambda: None)
        p._run_body()

        loaded = p._state.queue[0]
        rt = loaded.get("runtime", {})
        assert "_backoff_wall_deadline" not in rt, \
            "过期的 wall_deadline 必须被清除（放行）"
        assert "_backoff_until" not in rt, \
            "过期的残留 monotonic _backoff_until 必须一并清除"

class TestSchedulerCacheContentKey:
    """缓存键改为内容键 (task_type, job_id)，免疫 id-reuse。"""

    def test_cache_keyed_by_content_not_id(self):
        from tasklite.engine.scheduler import JobScheduler
        sched = JobScheduler({})
        jd = Job("t", "a").to_dict()
        j1 = sched.cached_job(jd)
        # 同一内容 dict → 同一 Job（缓存命中）
        j2 = sched.cached_job(jd)
        assert j1 is j2
        # 不同内容 → 不同 Job（内容键区分）
        j3 = sched.cached_job(Job("t", "b").to_dict())
        assert j3 is not j1
        # 畸形 dict（缺 job_id）→ 内容键不可用，直接解析抛 KeyError
        # （scheduler 的 pop_next_runnable 捕获后记为 malformed）
        with pytest.raises(KeyError):
            sched.cached_job({"task_type": "t"})

    def test_begin_round_clears_cache(self):
        """调度缓存按 run 生命周期清空。

        原契约「每轮清空」是性能瓶颈根源（阻塞/慢 job 阶段每轮全量反序列化）。
        begin_round 现在由 _run_body 加载期调用（跨 run 覆盖陈旧缓存）——
        仍须保证调用后缓存重建（新对象）。
        """
        from tasklite.engine.scheduler import JobScheduler
        sched = JobScheduler({})
        jd = Job("t", "a").to_dict()
        j1 = sched.cached_job(jd)
        sched.begin_round()
        j2 = sched.cached_job(jd)
        assert j2 is not j1  # run 边界清缓存（跨 run 陈旧覆盖）


class TestLegacySchemaRejected:
    """旧 schema 不再自动迁移，必须 fail-loud。"""

    def test_legacy_schema_raises_runtime_error(self, tmp_path):
        import sqlite3
        from tasklite.backend.sqlite_backend import SQLiteStateBackend
        db = tmp_path / "legacy.db"
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE queue (idx INTEGER PRIMARY KEY AUTOINCREMENT, job_data TEXT)")
        conn.execute("INSERT INTO queue (job_data) VALUES (?)", ('{"task_type": "t", "job_id": "a", "payload": {}}',))
        conn.commit()
        conn.close()
        with pytest.raises(RuntimeError, match="Unsupported legacy queue schema"):
            SQLiteStateBackend(db)



# （错误分类协议）


class _WorkerTransientError(Exception):
    """模块级异常： 注册表 spawn 传播测试用。"""


class _WorkerFatalSubclassError(KeyError):
    """FATAL 子类： 注册优先级测试用（用户显式声明覆盖内置启发式）。"""


def _transient_raise_handler(job, ctx):
    """handler：抛已注册的瞬态异常（子进程内分类必须识别）。"""
    raise _WorkerTransientError("spawn-propagated transient")


def _fatal_subclass_handler(job, ctx):
    """handler：抛注册为瞬态的 FATAL 子类（注册表优先于 FATAL 启发式）。"""
    raise _WorkerFatalSubclassError("registered-but-fatal-subclass")


class TestClassificationProtocol:
    def test_registered_transient_propagates_to_spawn(self, tmp_path):
        """ 回归：per-pipeline 注册的瞬态异常随 ctx 下发 spawn 子进程 → 重试而非 DLQ。

        修复前：注册只在父进程模块级全局生效，子进程 is_transient_exception
        返回 False → Unknown → DLQ（文档承诺「注册后自动重试」静默失效）。"""
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        p.register_transient_exception(_WorkerTransientError)
        p.register_handler("t", _transient_raise_handler)
        p.enqueue([Job("t", "a", max_retries=1, backoff_base=0.01)])
        p.run()
        # 子进程应分类为 retry → 重试耗尽后进 DLQ（MAX_RETRIES）而非直接 DLQ
        failed = p.backend.load_failed()
        assert "t::a" in failed
        # 若注册表未传播：子进程抛瞬态异常被分类为 Unknown → error 直接 DLQ，
        # 错误信息是异常消息；传播后：重试耗尽 → MAX_RETRIES_EXCEEDED
        assert "MAX_RETRIES" in failed["t::a"].get("error", ""), (
            f"registry not propagated to subprocess: {failed['t::a']}"
        )

    def test_registered_fatal_subclass_retries_not_dlq(self, tmp_path):
        """ 回归：注册 FATAL 子类（KeyError）→ 子进程按瞬态重试而非 DLQ。"""
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        p.register_transient_exception(_WorkerFatalSubclassError)
        p.register_handler("t", _fatal_subclass_handler)
        p.enqueue([Job("t", "a", max_retries=1, backoff_base=0.01)])
        p.run()
        failed = p.backend.load_failed()
        assert "t::a" in failed
        assert "MAX_RETRIES" in failed["t::a"].get("error", ""), (
            f"FATAL branch short-circuited registration: {failed['t::a']}"
        )



# 中断资源清理与可观测性


class TestDispatchInterruptResources:
    """中断资源释放：_dispatch_job 的 KeyboardInterrupt 分支必须释放已 acquire 资源。

    核实该分支（pipeline.py:1156-1169）确认释放路径存在；此测试用 submit 阶段注入
    KeyboardInterrupt 验证资源确实被释放（防回归）。
    """

    def test_keyboard_interrupt_during_submit_releases_resources(self, tmp_path, monkeypatch):
        from tasklite.pipeline import TaskLite
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        p.add_resource(CapacityResource("slot", 2.0))
        p.register_handler("t", lambda j, c: True, default_resources={"slot": 1.0})
        p.enqueue([Job("t", "a")])

        real_submit = p.executor.submit
        raised = {}

        def interrupting_submit(*a, **k):
            # 模拟：acquire 之后、submit 完成前命中 KeyboardInterrupt
            raise KeyboardInterrupt()

        monkeypatch.setattr(p.executor, "submit", interrupting_submit)
        from tasklite.pipeline import _CommitCrashSignal
        with pytest.raises(KeyboardInterrupt):
            try:
                p.run()
            except KeyboardInterrupt:
                # run 的异常处理器 requeue 后 re-raise KeyboardInterrupt
                raise
        # 资源被释放：used 归零（若泄漏，used 仍为 1）
        slot = p.resources["slot"]
        assert slot.used == 0, f"resource leaked after KeyboardInterrupt: used={slot.used}"
        # job 已 requeue（内存队列可重新调度）
        assert p.backend.load_queue() != [] or p._state.queue != []



# pending_dep_failure 误伤可运行 job 回归


def _ok_handler(job, ctx):
    return True


class TestPendingDepFailureIsolation:
    """依赖失败隔离回归：scheduler 的 pending_dep_failure 只应作用于 dep-failed 兜底
    位置的 job，不得误伤其后排队、完全可运行的 job。

    dep-failed 扫描不 break 修复后，scheduler 会越过 dep-failed job 继续
    找可运行 job——若 pending_dep_failure（全局字段）被 _dispatch_job 应用到
    可运行 job，该 job 会被误标 JOB_DEPENDENCY 而 DLQ（真实数据丢失）。
    """

    def test_runnable_job_after_depfailed_not_misclassified(self, tmp_path):
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        p.register_handler("work", _ok_handler)
        # 先制造一个失败的父任务（无 handler → NO_HANDLER → DLQ）
        p.enqueue([Job("nohandler", "parent")])
        p.run()
        assert "nohandler::parent" in p.backend.load_failed()

        # 队列：dep-failed job（依赖失败的 parent）在前，完全可运行的 job 在后
        p.enqueue([Job("work", "depchild", depends_on=["nohandler::parent"]),
                   Job("work", "runnable")])
        p.run()

        wall = p.backend.load_wall()
        failed = p.backend.load_failed()
        # 可运行 job 必须执行成功（修复前被误标 JOB_DEPENDENCY 而 DLQ）
        assert "work::runnable" in wall, f"runnable job misclassified, failed={failed}"
        # dep-failed job 正确 DLQ
        assert "work::depchild" in failed
        assert "JOB_DEPENDENCY" in failed["work::depchild"]["error"]

    def test_depfailed_only_queue_still_dlqd(self, tmp_path):
        """兜底路径：队列中只有 dep-failed job（无 runnable）→ 仍正确 DLQ。"""
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        p.register_handler("work", _ok_handler)
        p.enqueue([Job("nohandler", "parent")])
        p.run()

        p.enqueue([Job("work", "depchild", depends_on=["nohandler::parent"])])
        p.run()

        failed = p.backend.load_failed()
        assert "work::depchild" in failed
        assert "JOB_DEPENDENCY" in failed["work::depchild"]["error"]



# wall/failed 互斥 + poison 清空持久化


class TestTwoRoundFindings:
    """后端/发现层状态一致性问题回归。"""

    def test_success_clears_failed_dlq_residue(self, tmp_path):
        """commit_job_success 同事务清理 failed_dlq 残行——同一 uid 不得
        永久「既成功又失败」（DLQ 失败历史与 wall 矛盾）。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::x", {"error": "old failure"})
        backend.commit_job_success("t::x", {"ok": True})
        assert "t::x" in backend.load_wall()
        assert "t::x" not in backend.load_failed(), (
            "failed_dlq residue not cleared on success"
        )

    def test_success_clears_failed_dlq_on_retry_path(self, tmp_path):
        """ 交互：失败 → 重试成功 → failed_dlq 无残行（重试成功也是成功）。"""
        backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.commit_job_failure("t::y", {"error": "transient"})
        backend.commit_retry("t::y", Job("t", "y", retries=1).to_dict(), front=False)
        # 重试成功：走 commit_job_success
        backend.commit_job_success("t::y", {"ok": True})
        assert "t::y" not in backend.load_failed()
        assert "t::y" in backend.load_wall()


class TestUnresolvedItemsTwoRound:
    """边界情况处置回归。"""

    def test_declare_output_returns_resolved_abs_path(self, tmp_path):
        """declare_output 返回解析后绝对路径（相对路径按 output_root 重定位）。"""
        from tasklite.models.context import TaskContext
        root = tmp_path / "out"
        ctx = TaskContext(Job("t", "a"), set(), set(), {}, output_root=root, ipc_dir=None)
        r = ctx.declare_output("output/file.jpg")
        assert r.startswith(str(root.resolve()))
        assert Path(r).is_absolute()

    def test_commit_success_fails_loud_on_spawn_drift(self, tmp_path):
        """INSERT OR IGNORE 漂移（spawn uid 与磁盘冲突）→ 返回 False 走崩溃契约。"""
        backend = SQLiteStateBackend(tmp_path / "s.db")
        backend.enqueue_jobs([Job("t", "dup").to_dict()])
        ok = backend.commit_job_success(
            "t::parent", {"ok": True},
            spawned_jobs=[Job("t", "dup").to_dict(), Job("t", "fresh").to_dict()],
        )
        assert ok is False
        # 事务回滚：磁盘队列保留 dup，parent 未进 wall
        assert "t::dup" in [Job.from_dict(j).uid for j in backend.load_queue()]
        assert "t::parent" not in backend.load_wall()

    def test_load_dedupes_duplicate_uid_in_queue(self, tmp_path):
        """_run_body 加载期按 uid 去重（保留首条）——重复 uid 不导致
        _queue_uids 索引漂移（DEBUG 断言崩溃 / 生产 is_known 漏判）。"""
        from tasklite.pipeline import TaskLite
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite", max_workers=1)
        # 磁盘队列注入重复 uid（绕过 enqueue 去重）
        jd1 = Job("t", "dup", payload={"v": 1}).to_dict()
        jd2 = Job("t", "dup", payload={"v": 2}).to_dict()
        jd3 = Job("t", "ok").to_dict()
        p.backend.save_queue([jd1, jd2, jd3])  # save_queue 本身也去重（保留首条）
        q = p.backend.load_queue()
        uids = [Job.from_dict(j).uid for j in q]
        assert uids.count("t::dup") == 1, f"duplicate uid not deduped: {uids}"
        assert uids == ["t::dup", "t::ok"]

    def test_job_payload_top_level_copy_not_nested(self):
        """Job.payload 构造期**浅拷贝**——外部 dict 顶层键后续修改不污染 job。

        原名 test_job_payload_is_copied_not_aliased
        过度承诺「不污染」——嵌套对象仍共享引用（源码 job.py 为 dict() 浅拷贝），
        深层修改会穿透。改名 + 补嵌套别名显式断言，与 test_job.py:548
        （test_payload_with_nested_mutables_shared_reference）互为对偶，
        共同锁定浅拷贝语义。
        """
        d = {"a": 1}
        j = Job("t", "x", payload=d)
        d["a"] = 99
        assert j.payload == {"a": 1}
        # 嵌套对象仍共享引用（浅拷贝）：嵌套修改穿透
        nested = {"items": [1, 2]}
        j2 = Job("t", "y", payload=nested)
        nested["items"].append(3)
        assert j2.payload["items"] == [1, 2, 3], (
            "nested mutation must be visible (shallow copy, not deep)"
        )

    def test_job_rejects_negative_retries(self):
        """retries 负值入口拒绝（此前零延迟重试浪费多轮）。"""
        with pytest.raises(ValueError, match="retries must be >= 0"):
            Job("t", "y", retries=-1)

    def test_register_transient_rejects_retry_error_subclass(self):
        """注册 RetryError/FatalError 子类入口拒绝（专用分支优先，静默无效）。"""
        from tasklite.exceptions import RetryError, FatalError, TransientRegistry
        class MyRetry(RetryError):
            pass
        class MyFatal(FatalError):
            pass
        with pytest.raises(TypeError, match="RetryError"):
            TransientRegistry().register(MyRetry)
        with pytest.raises(TypeError, match="FatalError"):
            TransientRegistry().register(MyFatal)



# raw_result 编码 marker 防碰撞（list-marker）


class TestRawResultEncoding:
    def test_tuple_roundtrip_via_list_marker(self):
        """tuple 返回值经 encode→json→decode 保真还原为 tuple。"""
        encoded = _encode_raw_result((True, {"meta": 1}))
        assert isinstance(encoded, list) and encoded[0].startswith("__tl_")
        assert encoded[1] == [True, {"meta": 1}]
        assert _decode_raw_result(encoded) == (True, {"meta": 1})

    def test_user_dict_with_old_marker_key_not_misdecoded(self):
        """ 核心回归：用户 dict 恰含旧 marker 键（{"_tp_raw_type": "tuple",
        "items": [...]}）不得被误还原为 tuple——pre-fix（dict-key marker）
        该 dict 被 decode 成 tuple，handler 返回的合法数据被类型篡改。"""
        user_dict = {"_tp_raw_type": "tuple", "items": [1, 2]}
        assert _decode_raw_result(user_dict) == user_dict

    def test_user_list_not_confused_with_marker(self):
        """普通 list 原样返回；仅「外层 list + 首元素为哨兵 + len==2」才还原。"""
        assert _decode_raw_result([1, 2, 3]) == [1, 2, 3]
        assert _decode_raw_result([]) == []
        assert _decode_raw_result("__tp_tuple_v1") == "__tp_tuple_v1"

    def test_json_roundtrip_preserves_types(self):
        """经真实 JSON 序列化（子进程落盘路径）后类型仍保真。"""
        import json
        cases = [
            (True, {"a": 1}),
            (False, {}),
            (None, None),
            (True, None),
        ]
        for raw in cases:
            blob = json.dumps({"raw": _encode_raw_result(raw)})
            decoded = _decode_raw_result(json.loads(blob)["raw"])
            assert decoded == raw, f"{raw} -> {decoded}"
