"""seed_cursor 入口校验与跨后端一致性测试。

契约（与 SQLiteStateBackend.seed_cursor / TaskContext.set_cursor 同规）：
cursor key 必须为非空 str、value 必须为 str，两个后端与 OpsConsole
入口三层同契约 fail-loud——静默 str() 强转会让同一调用在 sqlite 腿
抛异常、memory 腿写 '123'，且 console 双写（backend + 内存镜像）
值型分歧（库存 '123'、镜像存 123）。
"""
import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tests.helpers import make_pipeline


class TestSeedCursorEntryValidation:
    """OpsConsole.seed_cursor 入口校验（委托前 fail-loud，双腿零写入）。"""

    def test_rejects_non_str_value(self, tmp_path):
        """非 str value 在入口拒绝，不再静默 str() 强转写库。"""
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="cursor value must be str"):
            p.seed_cursor("k", 123)

    def test_rejects_non_str_key(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="cursor key"):
            p.seed_cursor(None, "v")  # type: ignore

    def test_rejects_empty_key(self, tmp_path):
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError, match="cursor key"):
            p.seed_cursor("", "v")

    def test_rejection_leaves_no_state_trace(self, tmp_path):
        """校验失败时持久层与内存镜像零写入。"""
        p = make_pipeline(tmp_path)
        with pytest.raises(TypeError):
            p.seed_cursor("k", 123)
        assert "k" not in p.backend.load_cursors()
        assert "k" not in p._runtime.store.state.cursors


class TestSeedCursorBackendParity:
    """双后端同一非法入参行为一致（不再一腿抛异常、一腿静默强转）。"""

    @pytest.mark.parametrize("key,value", [(None, "v"), ("k", 123), ("", "v")])
    def test_memory_backend_fails_loud(self, key, value):
        b = InMemoryStateBackend()
        with pytest.raises(TypeError):
            b.seed_cursor(key, value)

    def test_memory_backend_fails_loud_matches_sqlite(self, tmp_path):
        """同一非法 value：memory 与 sqlite 均抛 TypeError（行为对齐）。"""
        mem = InMemoryStateBackend()
        sql = SQLiteStateBackend(tmp_path / "parity_state.db")
        for b in (mem, sql):
            with pytest.raises(TypeError, match="cursor value must be str"):
                b.seed_cursor("k", 123)

    def test_valid_value_roundtrip_memory(self):
        """合法 str 值在 memory 腿原值落库（不再有强转歧义）。"""
        b = InMemoryStateBackend()
        b.seed_cursor("k", "v1")
        assert b.load_cursors() == {"k": "v1"}
