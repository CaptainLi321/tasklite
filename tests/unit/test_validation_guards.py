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
        p._runtime._is_running = True
        with pytest.raises(RuntimeError, match="enqueue"):
            p.enqueue(Job("t", "j1"))
    def test_management_apis_during_run_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        p._runtime._is_running = True
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
        # 模拟 run 完成：守卫标志被 finally 清除
        p._runtime._is_running = True
        p._runtime._is_running = False
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
