"""纯校验入口防御触发测试（D-1 制度）。
验证 pipeline.py 若干 fail-loud 纯校验
构造函数入口与 wrappers/discovery.py 的注册参数 callable 校验长期无「触发该防御」
的测试（行覆盖 ≠ 防御生效覆盖）。本文件集中补上这些独立、易构造的防御分支
触发用例：
- TaskLite 构造：backend 非法对象类型、max_workers 非法
- register_handler：不可 callable 的 handler_func
- enqueue：误传单个 Job（而非列表）
- register_discovery：fetch/id/process/cursor_key 非 callable
其余 enqueue payload JSON 预检等已有测试覆盖（test_regression_fixes /
test_p0_fixes），此处不重复。
"""
import pytest
from tasklite.engine.resource import CapacityResource
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite
from tasklite.utils.lockfile import release_lock, try_acquire_lock
from tasklite.wrappers.discovery import register_discovery
from tasklite.testing import running
from tests.helpers import make_pipeline

# ─── TaskLite 构造：backend 非法对象类型 ─────────────────────

class TestBackendTypeGuard:
    def test_constructor_rejects_non_sqlite_object(self, tmp_path):
        """backend 必须是 'sqlite'、'memory' 或 AbstractStateBackend 实例，否则 TypeError。"""
        with pytest.raises(TypeError, match="backend must be"):
            TaskLite(name="t", state_dir=tmp_path / "s", backend=42)
    def test_constructor_rejects_unrecognized_string(self, tmp_path):
        # 非法字符串走 ValueError（Unknown backend），非字符串对象走 TypeError
        with pytest.raises(ValueError, match="Unknown backend"):
            TaskLite(name="t", state_dir=tmp_path / "s", backend="mysql")
    def test_constructor_accepts_memory_backend(self, tmp_path):
        p = TaskLite(name="t", state_dir=tmp_path / "s", backend="memory")
        assert p.backend_type == "memory"


# ─── TaskLite 构造：name 非法 ─────────────────────────────────

class TestPipelineNameGuard:
    """name 派生状态库文件名（{name}_state.db）：含路径分隔符会让
    SQLite 库逃逸 state_dir（持久态与 ipc/ 分离，备份/巡检漏库），
    构造期 fail-loud，与 task_type 的入口校验惯例同规。"""

    def test_rejects_empty_name(self, tmp_path):
        with pytest.raises(TypeError, match="name must be a non-empty str"):
            TaskLite(name="", state_dir=tmp_path / "s")

    def test_rejects_non_str_name(self, tmp_path):
        with pytest.raises(TypeError, match="name must be a non-empty str"):
            TaskLite(name=42, state_dir=tmp_path / "s")  # type: ignore

    def test_rejects_path_separators(self, tmp_path):
        for bad in ("../escaped", "a/b", "a\\b"):
            with pytest.raises(ValueError, match="path separators"):
                TaskLite(name=bad, state_dir=tmp_path / "s")

    def test_rejection_leaves_no_state_dir_side_effects(self, tmp_path):
        with pytest.raises(ValueError):
            TaskLite(name="../escaped", state_dir=tmp_path / "s")
        assert not (tmp_path / "s").exists(), "校验失败不得产生目录副作用"

    def test_valid_name_keeps_state_db_inside_state_dir(self, tmp_path):
        p = TaskLite(name="ok_name", state_dir=tmp_path / "s")
        assert (tmp_path / "s" / "ok_name_state.db").exists()
        assert p.name == "ok_name"


# ─── TaskLite 构造：max_workers 非法 ─────────────────────────

class TestMaxWorkersGuard:
    def test_rejects_zero(self, tmp_path):
        with pytest.raises(ValueError, match="max_workers"):
            TaskLite(name="t", state_dir=tmp_path / "s", max_workers=0)
    def test_rejects_negative(self, tmp_path):
        with pytest.raises(ValueError, match="max_workers"):
            TaskLite(name="t", state_dir=tmp_path / "s", max_workers=-1)
    def test_rejects_bool(self, tmp_path):
        # bool 是 int 子类，须显式拒绝（防 True→1 worker 静默）
        with pytest.raises(ValueError, match="max_workers"):
            TaskLite(name="t", state_dir=tmp_path / "s", max_workers=True)
    def test_rejects_float(self, tmp_path):
        with pytest.raises(ValueError, match="max_workers"):
            TaskLite(name="t", state_dir=tmp_path / "s", max_workers=2.5)

# ─── register_handler：不可 callable ─────────────────────────────

class TestRegisterHandlerCallableGuard:
    def test_rejects_none(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="must be callable"):
            p.register_handler("t", None)
    def test_rejects_non_callable_int(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="must be callable"):
            p.register_handler("t", 123)

# ─── enqueue：单 Job 兼容与非法输入防御 ───────────────────

class TestEnqueueSingleJobGuard:
    def test_accepts_single_job_like_list(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.enqueue(Job("t", "j1", payload={}))
        uids = [j["job_id"] for j in p.backend.load_queue()]
        assert "j1" in uids
    def test_rejects_non_job_non_list(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="expects a Job or a list"):
            p.enqueue("t::j1")

# ─── register_discovery：回调参数 callable 校验 ──────────────────

def _make_discovery_pipeline(tmp_path):
    p = make_pipeline(tmp_path)
    p.register_handler("process_t", lambda j, c: (True, {}))
    return p

# 模块级 dummy 回调（pickle 预检要求可 pickle）
def _fetch(job, ctx, page):
    return []

def _id(item):
    return str(item)

def _process(job, ctx, item, content_id):
    return None

class TestDiscoveryCallableGuard:
    def test_rejects_non_callable_fetch(self, tmp_path):
        p = _make_discovery_pipeline(tmp_path)
        with pytest.raises(TypeError, match="fetch_func must be callable"):
            register_discovery(
                p, task_type="discover", fetch_func=None,
                id_func=_id, process_item_func=_process,
                process_task_type="process_t",
            )
    def test_rejects_non_callable_id(self, tmp_path):
        p = _make_discovery_pipeline(tmp_path)
        with pytest.raises(TypeError, match="id_func must be callable"):
            register_discovery(
                p, task_type="discover", fetch_func=_fetch,
                id_func=123, process_item_func=_process,
                process_task_type="process_t",
            )
    def test_rejects_non_callable_process_item(self, tmp_path):
        p = _make_discovery_pipeline(tmp_path)
        with pytest.raises(TypeError, match="process_item_func must be callable"):
            register_discovery(
                p, task_type="discover", fetch_func=_fetch,
                id_func=_id, process_item_func="not-callable",
                process_task_type="process_t",
            )
    def test_rejects_non_callable_cursor_key(self, tmp_path):
        p = _make_discovery_pipeline(tmp_path)
        with pytest.raises(TypeError, match="cursor_key_func must be callable"):
            register_discovery(
                p, task_type="discover", fetch_func=_fetch,
                id_func=_id, process_item_func=_process,
                process_task_type="process_t", cursor_key_func=42,
            )
    def test_rejects_local_fetch_callback(self, tmp_path):
        """discovery 回调注册期做 pickle 预检，局部函数立即 fail-loud。"""
        p = _make_discovery_pipeline(tmp_path)
        def local_fetch(job, ctx, page):
            return []
        with pytest.raises(TypeError, match="module-level picklable"):
            register_discovery(
                p, task_type="discover", fetch_func=local_fetch,
                id_func=_id, process_item_func=_process,
                process_task_type="process_t",
            )

# ─── 代码级限制：run 期间管理 API 守卫 + payload_schema 类型守卫 ─────

class TestRunStateGuards:
    """把“仅限 run() 之外”的文档限制变成代码级 RuntimeError。"""
    def test_enqueue_during_run_rejected(self, tmp_path):
        from tasklite.models.job import Job
        p = make_pipeline(tmp_path)
        with running(p):
            with pytest.raises(RuntimeError, match="enqueue"):
                p.enqueue(Job("t", "j1"))
    def test_management_apis_during_run_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        with running(p):
            for api, call in [
                ("list_dlq", lambda: p.list_dlq()),
                ("clear_dlq", lambda: p.clear_dlq()),
                ("clear_history", lambda: p.clear_history("t::")),
                ("seed_wall", lambda: p.seed_wall(["t::a"])),
                ("seed_cursor", lambda: p.seed_cursor("k", "v")),
            ]:
                with pytest.raises(RuntimeError, match=api):
                    call()
    def test_guard_clears_after_run(self, tmp_path):
        from tasklite.models.job import Job
        p = make_pipeline(tmp_path)
        # run 结束（with 退出）后守卫标志被清除
        with running(p):
            pass
        p.enqueue(Job("t", "j1"))  # 不应抛

class TestRunLockGuard:
    """同一 state_dir 并发 run() 由 pipeline 级文件锁 fail-loud 拒绝。"""
    def test_concurrent_run_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        fd = try_acquire_lock(p.ipc_dir, "__pipeline_run__", timeout=0)
        assert fd is not None, "测试前置：应能先持有 pipeline run 锁"
        try:
            with pytest.raises(RuntimeError, match="Another run"):
                p.run()
        finally:
            release_lock(fd)

class TestPayloadSchemaTypeGuard:
    def test_rejects_non_type_payload_schema(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="payload_schema must be a type"):
            p.register_handler("t", lambda j, c: True, payload_schema={})  # type: ignore

class TestStrictPicklableGuard:
    def test_strict_picklable_rejects_lambda_handler(self, tmp_path):
        p = TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite",
                         strict_picklable=True)
        p.register_handler("t", lambda j, c: True)
        with pytest.raises(TypeError, match="strict_picklable"):
            p.run()
    def test_default_off_keeps_lambda_compat(self, tmp_path):
        p = make_pipeline(tmp_path)
        p.register_handler("t", lambda j, c: True)
        # 默认不预检（单测 lambda 兼容）；这里只验证构造不报错
        assert p.strict_picklable is False

# ─── 新增入口校验防御（API ）─────────────────────

class TestRegisterHandlerTaskTypeGuard:
    def test_rejects_empty_task_type(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="task_type must be a non-empty str"):
            p.register_handler("", lambda j, c: True)
    def test_rejects_task_type_with_separator(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(ValueError, match="must not contain '::'"):
            p.register_handler("a::b", lambda j, c: True)

class TestAddResourceGuard:
    def test_rejects_non_resource(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="Resource instance"):
            p.add_resource("api")  # type: ignore

class TestEnqueueElementGuard:
    def test_rejects_non_job_in_list(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="Job objects"):
            p.enqueue([Job("t", "j1"), object()])  # type: ignore

class TestClearDlqTaskTypesGuard:
    def test_rejects_str_task_types(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="task_types must be a list/tuple"):
            p.clear_dlq(task_types="download")  # type: ignore

class TestForgetWhereGuard:
    def test_rejects_unknown_where(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(ValueError, match="unknown target"):
            p.clear_history("t::a", where=("wall", "bogus")) # type: ignore

class TestSeedWallUidGuard:
    def test_rejects_empty_components(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(ValueError, match="exactly one '::'|non-empty"):
            p.seed_wall(["::"])

    def test_rejects_uid_resident_in_queue(self):
        """wall/queue 必须互斥：驻留内存队列的 uid 拒绝种子。

        上次 run 中断后内存队列仍驻留作业（内存态不重建，仅由 set_state
        整体重建），对此类 uid 种子会在内存态制造 wall∩queue 非豁免重叠，
        违反六集合互斥不变式——step() 直驱下后续任意状态变更即触发一致性
        断言崩溃。预检先于任何写入，冲突整体拒绝。
        """
        from tasklite.backend.memory import InMemoryStateBackend
        from tasklite.engine.console import OpsConsole
        from tasklite.engine.store import StateStore
        from tasklite.models.state import PipelineState
        from tasklite.taxonomy import _DEFAULT_TAXONOMY

        backend = InMemoryStateBackend()
        store = StateStore(backend)
        # 模拟中断 run 的残留：内存队列驻留一个 rerun=never 的作业
        store.set_state(PipelineState({}, {}, {}, [
            {"task_type": "download", "job_id": "1", "payload": {}, "rerun": "never"},
        ]))
        console = OpsConsole(backend, store, _DEFAULT_TAXONOMY)

        with pytest.raises(ValueError, match="queue"):
            console.seed_wall(["download::1"])
        # 双腿零写入：backend 持久层与内存 state 均无 wall 残留
        assert "download::1" not in backend.load_wall()
        assert "download::1" not in store.state.wall
        # 非队列驻留 uid 种子不受波及
        assert console.seed_wall(["download::2"]) == 1

    @pytest.mark.parametrize("backend_kind", ["memory", "sqlite"])
    def test_rejects_uid_resident_in_backend_queue_only(self, tmp_path, backend_kind):
        """队列驻留预检补后端持久腿：run 前内存占位为空时以后端队列为真相。

        进程崩溃后队列落盘、新进程重启后在首次 run() 前 seed_wall——内存
        state 是空占位，若不查后端队列，uid 会写入 wall 并在下次加载时被
        残留过滤从队列静默删除（作业被吞）。内存/SQLite 双后端同语义。
        """
        from tasklite.backend.memory import InMemoryStateBackend
        from tasklite.backend.sqlite_backend import SQLiteStateBackend
        from tasklite.engine.console import OpsConsole
        from tasklite.engine.store import StateStore
        from tasklite.taxonomy import _DEFAULT_TAXONOMY

        if backend_kind == "memory":
            backend = InMemoryStateBackend()
        else:
            backend = SQLiteStateBackend(tmp_path / "state.db")
        backend.save_queue([
            {"task_type": "download", "job_id": "1", "payload": {}, "rerun": "never"},
        ])
        # 模拟新进程：store 内存腿保持构造期空占位（队列尚未加载）
        store = StateStore(backend)
        assert "download::1" not in store.queue_uids
        console = OpsConsole(backend, store, _DEFAULT_TAXONOMY)

        with pytest.raises(ValueError, match="queue"):
            console.seed_wall(["download::1"])
        # 双腿零写入：后端 wall 无残留，队列行原样保留
        assert "download::1" not in backend.load_wall()
        assert len(backend.load_queue()) == 1
        # 非队列驻留 uid 种子不受波及
        assert console.seed_wall(["download::2"]) == 1


class TestTuningScalarGuard:
    """调优标量合法性校验（resolve_tuning 唯一规范化入口）。

    dep_grace_seconds 非有限/非正值会让依赖宽限立即判死（0/负）或永不
    裁决（NaN 比较恒 False → 永不裁决；inf 宽限 → 活锁）；commit_failure_dlq_threshold 与
    deadlock_gap_max_rounds 为 0 时提交失败/无根因零轮即升级整队列 DLQ。
    入口 fail-loud，而非延迟为运行期误杀/活锁。
    """

    @pytest.mark.parametrize("bad", [0, -1.0, -0.5, float("nan"), float("inf"), float("-inf")])
    def test_rejects_invalid_dep_grace_seconds(self, bad):
        from tasklite.engine.config import resolve_tuning
        with pytest.raises(ValueError, match="dep_grace_seconds"):
            resolve_tuning(dep_grace_seconds=bad)

    @pytest.mark.parametrize("bad", [2.9, 1.5, 3.0, "3", True, b"3", object()])
    def test_rejects_non_int_commit_failure_threshold(self, bad):
        """轮次阈值仅真 int 合法：浮点截断（2.9 → 2）与数字字符串强转均为类型错误。"""
        from tasklite.engine.config import resolve_tuning
        with pytest.raises(TypeError, match="commit_failure_dlq_threshold"):
            resolve_tuning(commit_failure_dlq_threshold=bad)

    @pytest.mark.parametrize("bad", [1.9, 1.0, "5", True])
    def test_rejects_non_int_deadlock_gap_rounds(self, bad):
        """gap 阈值仅真 int 合法：1.9 截断为 1 即零恢复窗口，必须显式拒绝。"""
        from tasklite.engine.config import resolve_tuning
        with pytest.raises(TypeError, match="deadlock_gap_max_rounds"):
            resolve_tuning(deadlock_gap_max_rounds=bad)

    @pytest.mark.parametrize("bad", ["5", True, object()])
    def test_rejects_non_numeric_dep_grace_seconds(self, bad):
        """宽限秒仅数值类型合法（int/float），bool 与数字字符串为类型错误。"""
        from tasklite.engine.config import resolve_tuning
        with pytest.raises(TypeError, match="dep_grace_seconds"):
            resolve_tuning(dep_grace_seconds=bad)

    def test_dep_grace_accepts_integral_seconds(self):
        from tasklite.engine.config import resolve_tuning
        assert resolve_tuning(dep_grace_seconds=5).dep_grace_seconds == 5.0

    def test_rejects_invalid_commit_failure_threshold(self, tmp_path):
        with pytest.raises(ValueError, match="commit_failure_dlq_threshold"):
            TaskLite(name="t", state_dir=tmp_path / "s", commit_failure_dlq_threshold=0)

    def test_rejects_negative_commit_failure_threshold(self, tmp_path):
        with pytest.raises(ValueError, match="commit_failure_dlq_threshold"):
            TaskLite(name="t", state_dir=tmp_path / "s", commit_failure_dlq_threshold=-1)

    def test_rejects_zero_deadlock_gap_rounds(self, tmp_path):
        with pytest.raises(ValueError, match="deadlock_gap_max_rounds"):
            TaskLite(name="t", state_dir=tmp_path / "s", deadlock_gap_max_rounds=0)

    def test_rejects_negative_deadlock_gap_rounds(self):
        from tasklite.engine.config import resolve_tuning
        with pytest.raises(ValueError, match="deadlock_gap_max_rounds"):
            resolve_tuning(deadlock_gap_max_rounds=-2)

    def test_rejects_non_positive_dep_grace_via_constructor(self, tmp_path):
        with pytest.raises(ValueError, match="dep_grace_seconds"):
            TaskLite(name="t", state_dir=tmp_path / "s", dep_grace_seconds=0)

    def test_valid_tuning_values_pass(self, tmp_path):
        from tasklite.engine.config import resolve_tuning
        tuning = resolve_tuning(
            dep_grace_seconds=1.0,
            commit_failure_dlq_threshold=1,
            deadlock_gap_max_rounds=1,
        )
        assert tuning.dep_grace_seconds == 1.0
        assert tuning.commit_failure_dlq_threshold == 1
        assert tuning.deadlock_gap_max_rounds == 1
