"""backend 抽象契约完整性测试：抽象方法集合锁定与共享入口门卫。

集合锁定测试的意义：抽象面是 SQLite / Memory 双腿的共同口径，新增或
遗漏任一方法都会让双腿对齐断言（test_backend_parity）静默失效——集合
必须显式变更、显式评审。
"""

from __future__ import annotations

import ast
import pathlib

import pytest

from tasklite.backend.base import (
    AbstractStateBackend,
    validate_attempt_dispatch,
    validate_attempt_finish,
    validate_queue_replacement,
)
from tasklite.models.attempt import ATTEMPT_OUTCOMES, AttemptRecord

BACKEND_PACKAGE_DIR = pathlib.Path(__file__).resolve().parent.parent.parent / "tasklite" / "backend"

EXPECTED_ABSTRACT_METHODS = {
    # 全量加载
    "load_wall",
    "load_failed",
    "load_failed_payloads",
    "load_cursors",
    "load_queue",
    # 整表重写
    "save_queue",
    "replace_queue_atomic",
    # delta 提交
    "commit_job_success",
    "commit_job_failure",
    "commit_retry",
    "commit_bulk_failure",
    "commit_skip",
    "append_failed",
    # 增量入队
    "enqueue_jobs",
    # 定向删除与种子
    "delete_queue_uids",
    "delete_failed",
    "delete_wall",
    "seed_wall",
    "seed_cursor",
    # 尝试轨迹（append-only 旁路观测面）
    "append_attempt",
    "update_attempt",
    "load_attempts",
    # 元数据
    "get_meta",
    "set_meta",
}


def _running_record(**overrides) -> AttemptRecord:
    """派发即插行的标准形态（outcome=running，无收尾时刻/错误）。"""
    defaults = {
        "job_uid": "encode::vid_001",
        "activation_no": 1,
        "attempt_no": 1,
        "incarnation": "run_ab12cd34.7",
        "run_id": "run_ab12cd34",
        "started_at": "2026-05-01T00:00:00+00:00",
        "outcome": "running",
    }
    defaults.update(overrides)
    return AttemptRecord(**defaults)


class TestAbstractContractLock:
    """抽象方法集合锁定与不可实例化。"""

    def test_abstract_method_set_locked(self):
        assert set(AbstractStateBackend.__abstractmethods__) == EXPECTED_ABSTRACT_METHODS

    def test_abstract_class_not_instantiable(self):
        with pytest.raises(TypeError):
            AbstractStateBackend()  # type: ignore[abstract]

    def test_partial_implementation_stays_abstract(self):
        class HalfBackend(AbstractStateBackend):
            def load_wall(self):
                return {}

        with pytest.raises(TypeError):
            HalfBackend()  # type: ignore[abstract]
        assert "commit_job_success" in HalfBackend.__abstractmethods__


class TestValidateQueueReplacement:
    """整表替换集形状门卫（写变前 fail-loud）。"""

    def test_valid_list_and_tuple_pass(self):
        validate_queue_replacement([])
        validate_queue_replacement([{"task_type": "t", "job_id": "a"}])
        validate_queue_replacement(({"task_type": "t", "job_id": "a"},))

    def test_none_rejected(self):
        with pytest.raises(TypeError, match="queue replacement must be a list"):
            validate_queue_replacement(None)

    @pytest.mark.parametrize("bad", [42, "jobs", {"task_type": "t"}, {"a": 1}])
    def test_non_sequence_rejected(self, bad):
        with pytest.raises(TypeError, match="queue replacement must be a list"):
            validate_queue_replacement(bad)

    def test_non_dict_item_rejected(self):
        with pytest.raises(TypeError, match="queue replacement item #1 must be a job dict"):
            validate_queue_replacement([{"task_type": "t", "job_id": "a"}, "not-a-dict"])


class TestValidateAttemptDispatch:
    """轨迹插入门卫：只接受派发即插行形态。"""

    def test_fresh_running_row_passes(self):
        validate_attempt_dispatch(_running_record())

    def test_non_record_type_rejected(self):
        with pytest.raises(TypeError, match="attempt record must be an AttemptRecord"):
            validate_attempt_dispatch({"job_uid": "encode::vid_001"})

    def test_terminal_outcome_rejected(self):
        with pytest.raises(ValueError, match="fresh running row"):
            validate_attempt_dispatch(_running_record(outcome="failed"))

    def test_preclosed_finished_at_rejected(self):
        with pytest.raises(ValueError, match="fresh running row"):
            validate_attempt_dispatch(
                _running_record(finished_at="2026-05-01T00:00:05+00:00")
            )

    def test_precarried_error_rejected(self):
        with pytest.raises(ValueError, match="fresh running row"):
            validate_attempt_dispatch(_running_record(error="boom"))


class TestValidateAttemptFinish:
    """轨迹收尾门卫：终态词汇 + 收尾时刻/错误形态。"""

    def test_all_terminal_outcomes_pass(self):
        terminals = [o for o in ATTEMPT_OUTCOMES if o != "running"]
        for outcome in terminals:
            validate_attempt_finish(outcome, "2026-05-01T00:00:05+00:00", None)
            validate_attempt_finish(outcome, "2026-05-01T00:00:05+00:00", "boom")

    def test_running_rejected(self):
        with pytest.raises(ValueError, match="outcome must be one of"):
            validate_attempt_finish("running", "2026-05-01T00:00:05+00:00", None)

    @pytest.mark.parametrize("bad", ["success", "", None, 7])
    def test_unknown_outcome_rejected(self, bad):
        with pytest.raises(ValueError, match="outcome must be one of"):
            validate_attempt_finish(bad, "2026-05-01T00:00:05+00:00", None)

    def test_finished_at_type_and_emptiness(self):
        with pytest.raises(TypeError, match="finished_at must be a str"):
            validate_attempt_finish("succeeded", 123, None)
        with pytest.raises(ValueError, match="finished_at must be a non-empty str"):
            validate_attempt_finish("succeeded", "", None)

    def test_error_optional_but_typed(self):
        with pytest.raises(TypeError, match="error must be a str or None"):
            validate_attempt_finish("failed", "2026-05-01T00:00:05+00:00", 99)
        with pytest.raises(ValueError, match="error must be a non-empty str or None"):
            validate_attempt_finish("failed", "2026-05-01T00:00:05+00:00", "")


class TestBackendLayeringGuard:
    """分层红线静态守卫：持久层不得 import engine/wrappers/contrib。"""

    def _import_roots(self, source: pathlib.Path) -> list[str]:
        """收集源文件全部 import 的顶层模块路径（相对 import 以 ``.`` 解析）。"""
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        roots: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                prefix = "." * node.level
                roots.append(prefix + (node.module or ""))
        return roots

    def test_backend_sources_respect_layering(self):
        assert BACKEND_PACKAGE_DIR.is_dir()
        offenders: list[str] = []
        banned_heads = ("engine", "wrappers", "contrib")
        for source in sorted(BACKEND_PACKAGE_DIR.glob("*.py")):
            for root in self._import_roots(source):
                if root.startswith("."):
                    # 相对 import：level >= 2 才可能越出本子包
                    stripped = root.lstrip(".")
                    head = stripped.split(".")[0] if stripped else ""
                    if root.count(".") >= 2 and head in banned_heads:
                        offenders.append(f"{source.name}: 相对 import 越层 `{root}`")
                else:
                    head = root.split(".")[1] if root.startswith("tasklite.") and root.count(".") >= 1 else ""
                    if root.startswith("tasklite.") and head in banned_heads:
                        offenders.append(f"{source.name}: 核心层反向依赖 `{root}`")
        assert not offenders, "tasklite/backend 分层违例: " + "; ".join(offenders)
