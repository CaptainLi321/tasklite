"""换库后管理 API 目标库一致性回归。

不变式：backend setter 换库后，凡持有后端引用的组件（StateStore /
OpsConsole）必须同步重绑定，全部管理 API 无条件作用于当前库；漏绑会使
list_dlq / clear_dlq / clear_history / seed_wall / seed_cursor 静默读写
旧库（seed_wall 写旧库导致新库 wall 无种子、去重失效任务重复执行）。
"""
import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tests.helpers import make_pipeline


def _preset_old_backend(p):
    """旧库预置 wall/failed/cursor 各一条，返回旧后端引用。"""
    old = p.backend
    old.seed_wall(["w::old"])
    old.append_failed("a::old", {"error": "old"})
    old.seed_cursor("ck", "old")
    return old


class TestBackendSwapManagementApis:
    """换库后五个管理 API 全部写读新库，旧库不受波及。"""

    def test_seed_wall_targets_new_backend(self, tmp_path):
        p = make_pipeline(tmp_path)
        old = _preset_old_backend(p)
        new = InMemoryStateBackend()
        p.backend = new
        assert p.seed_wall(["t::new"]) == 1
        assert "t::new" in new.load_wall()
        assert "t::new" not in old.load_wall()

    def test_list_dlq_reads_new_backend(self, tmp_path):
        p = make_pipeline(tmp_path)
        old = _preset_old_backend(p)
        new = InMemoryStateBackend()
        new.append_failed("a::new", {"error": "new"})
        p.backend = new
        uids = [e.uid for e in p.list_dlq()]
        assert uids == ["a::new"]
        assert "a::old" not in uids

    def test_clear_dlq_deletes_new_backend_entries(self, tmp_path):
        p = make_pipeline(tmp_path)
        old = _preset_old_backend(p)
        new = InMemoryStateBackend()
        new.append_failed("a::new", {"error": "new"})
        p.backend = new
        assert p.clear_dlq(["a"]) == 1
        assert p.list_dlq() == []
        # 旧库条目不受新库清理波及
        assert "a::old" in old.load_failed()

    def test_clear_history_targets_new_backend(self, tmp_path):
        p = make_pipeline(tmp_path)
        old = _preset_old_backend(p)
        new = InMemoryStateBackend()
        new.seed_wall(["w::new"])
        p.backend = new
        assert p.clear_history("w::", where=("wall",)) == 1
        assert "w::new" not in new.load_wall()
        assert "w::old" in old.load_wall()

    def test_seed_cursor_targets_new_backend(self, tmp_path):
        p = make_pipeline(tmp_path)
        old = _preset_old_backend(p)
        new = InMemoryStateBackend()
        p.backend = new
        p.seed_cursor("k", "new")
        assert new.load_cursors()["k"] == "new"
        assert "k" not in old.load_cursors()

    def test_backend_references_converge_after_swap(self, tmp_path):
        """store 与 console 持有的后端引用换库后统一收敛到同一实例。"""
        p = make_pipeline(tmp_path)
        new = InMemoryStateBackend()
        p.backend = new
        assert p.backend is new
        assert p._runtime.backend is new
        assert p._runtime.store._backend is new
        assert p._console._backend is new


class TestBackendSwapRunGuard:
    """run 期间换库被拒绝（管理段操作仅限 run() 外）。"""

    def test_backend_setter_during_run_rejected(self, tmp_path):
        p = make_pipeline(tmp_path)
        p._runtime._is_running = True
        try:
            with pytest.raises(RuntimeError, match="backend"):
                p.backend = InMemoryStateBackend()
        finally:
            p._runtime._is_running = False


class TestBackendSwapTypeGuard:
    """backend setter 入口类型校验：非 AbstractStateBackend 实例 fail-loud。

    零校验时 ``tl.backend = "sqlite"`` / None 被静默接受，失败延迟到
    管理 API 调用才以 AttributeError 爆发（归因断裂）；拒绝时原后端
    引用链（runtime / store / console）零扰动。
    """

    @pytest.mark.parametrize("bad", ["sqlite", "memory", None, 42])
    def test_setter_rejects_non_backend_object(self, tmp_path, bad):
        p = make_pipeline(tmp_path)
        original = p.backend
        with pytest.raises(TypeError, match="AbstractStateBackend"):
            p.backend = bad
        assert p.backend is original
        assert p._runtime.backend is original
        assert p._console._backend is original

    def test_setter_accepts_backend_instance(self, tmp_path):
        p = make_pipeline(tmp_path)
        new = InMemoryStateBackend()
        p.backend = new
        assert p.backend is new
