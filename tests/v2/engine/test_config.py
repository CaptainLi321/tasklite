"""v2 RunConfig 装配与调优标量唯一解析点测试。

锁定契约：
- resolve_tuning 是调优标量唯一规范化入口（None → 常量默认、int 宽限
  → float、非法值 fail-loud）；
- RunConfig.resolve 装配件按引用透传（冻结引用而非拷贝）、可空标量经
  resolve_tuning 收敛最终类型，此后任何模块不得二次默认；
- 零退避军规：调优面不存在任何退避类配置项。
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from tasklite.v2.backend.memory import InMemoryStateBackend
from tasklite.v2.engine.admission import (
    ImmediateRequeuePolicy,
    RequeuePolicy,
    RerunPolicy,
)
from tasklite.v2.engine.channel import ExecutionChannel
from tasklite.v2.engine.config import RunConfig, Tuning, resolve_tuning
from tasklite.v2.engine.errorclass import ErrorClassifier
from tasklite.v2.engine.governor import (
    DEADLOCK_GAP_MAX_ROUNDS,
    DEP_GRACE_SECONDS,
    DeadlockGovernor,
)
from tasklite.v2.engine.resource import ResourceManager
from tasklite.v2.engine.scheduler import FifoOrderingPolicy, OrderingPolicy
from tasklite.v2.engine.store import COMMIT_FAILURE_THRESHOLD
from tasklite.v2.models.task import TaskRegistry


class NewestFirstOrdering(OrderingPolicy):
    """队尾优先访问序样本（引用共享断言用）。"""

    def visit_order(self, queue):
        return range(len(queue) - 1, -1, -1)


class ConfigEnv:
    """最小装配集（字段即 RunConfig 装配件清单）。"""

    def __init__(self, tmp_path) -> None:
        self.ipc_dir = str(tmp_path / "ipc")
        Path(self.ipc_dir).mkdir(parents=True, exist_ok=True)
        self.backend = InMemoryStateBackend()
        self.resources = ResourceManager()
        self.tasks = TaskRegistry()
        self.channel = ExecutionChannel(self.ipc_dir)
        self.classifier = ErrorClassifier()
        self.governor = DeadlockGovernor()
        self.rerun_policy = RerunPolicy()
        self.requeue_policy = ImmediateRequeuePolicy()

    def resolve(self, **overrides) -> RunConfig:
        base = dict(
            name="test_config",
            ipc_dir=self.ipc_dir,
            backend=self.backend,
            resources=self.resources,
            tasks=self.tasks,
            channel=self.channel,
            classifier=self.classifier,
            governor=self.governor,
            rerun_policy=self.rerun_policy,
            requeue_policy=self.requeue_policy,
            output_root=None,
        )
        base.update(overrides)
        return RunConfig.resolve(**base)


class TestResolveTuning:
    """resolve_tuning 唯一规范化入口。"""

    def test_defaults_match_engine_constants(self):
        tuning = resolve_tuning()
        assert tuning == Tuning(
            dep_grace_seconds=DEP_GRACE_SECONDS,
            commit_failure_threshold=COMMIT_FAILURE_THRESHOLD,
            deadlock_gap_max_rounds=DEADLOCK_GAP_MAX_ROUNDS,
        )
        assert isinstance(tuning.dep_grace_seconds, float)
        assert isinstance(tuning.commit_failure_threshold, int)
        assert isinstance(tuning.deadlock_gap_max_rounds, int)

    def test_int_grace_normalized_to_float(self):
        assert resolve_tuning(dep_grace_seconds=5).dep_grace_seconds == 5.0
        assert isinstance(resolve_tuning(dep_grace_seconds=5).dep_grace_seconds, float)

    def test_explicit_rounds_passthrough(self):
        tuning = resolve_tuning(
            commit_failure_threshold=7, deadlock_gap_max_rounds=9,
        )
        assert tuning.commit_failure_threshold == 7
        assert tuning.deadlock_gap_max_rounds == 9

    @pytest.mark.parametrize("bad", [0, -1.5, float("inf"), float("nan"), -0.001])
    def test_rejects_invalid_dep_grace_seconds(self, bad):
        with pytest.raises(ValueError, match="dep_grace_seconds"):
            resolve_tuning(dep_grace_seconds=bad)

    @pytest.mark.parametrize("bad", ["60", True, [60], {"s": 1}, object()])
    def test_rejects_non_numeric_dep_grace_seconds(self, bad):
        with pytest.raises(TypeError, match="dep_grace_seconds"):
            resolve_tuning(dep_grace_seconds=bad)

    @pytest.mark.parametrize("bad", [True, 1.9, 2.0, "3"])
    def test_rejects_non_int_threshold(self, bad):
        with pytest.raises(TypeError, match="commit_failure_threshold"):
            resolve_tuning(commit_failure_threshold=bad)

    @pytest.mark.parametrize("bad", [True, 1.9, "3"])
    def test_rejects_non_int_gap_rounds(self, bad):
        with pytest.raises(TypeError, match="deadlock_gap_max_rounds"):
            resolve_tuning(deadlock_gap_max_rounds=bad)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"commit_failure_threshold": 0},
            {"commit_failure_threshold": -1},
            {"deadlock_gap_max_rounds": 0},
            {"deadlock_gap_max_rounds": -2},
        ],
    )
    def test_rejects_non_positive_rounds(self, kwargs):
        with pytest.raises(ValueError):
            resolve_tuning(**kwargs)

    def test_huge_int_grace_converges_to_value_error(self):
        """超大 int 转 float 溢出收敛为 ValueError（与 NaN/inf 同路 fail-loud）。"""
        with pytest.raises(ValueError, match="dep_grace_seconds"):
            resolve_tuning(dep_grace_seconds=10 ** 400)


class TestRunConfigResolve:
    """RunConfig.resolve 唯一默认值解析点。"""

    def test_assembled_components_shared_by_reference(self, tmp_path):
        env = ConfigEnv(tmp_path)
        config = env.resolve()
        assert config.backend is env.backend
        assert config.resources is env.resources
        assert config.tasks is env.tasks
        assert config.channel is env.channel
        assert config.classifier is env.classifier
        assert config.governor is env.governor
        assert config.rerun_policy is env.rerun_policy
        assert config.requeue_policy is env.requeue_policy
        assert config.name == "test_config"
        assert config.ipc_dir == env.ipc_dir

    def test_none_scalars_resolved_to_constant_defaults(self, tmp_path):
        config = ConfigEnv(tmp_path).resolve()
        assert config.dep_grace_seconds == DEP_GRACE_SECONDS
        assert config.commit_failure_threshold == COMMIT_FAILURE_THRESHOLD
        assert config.deadlock_gap_max_rounds == DEADLOCK_GAP_MAX_ROUNDS
        assert config.strict_picklable is True
        assert config.output_root is None
        assert config.on_run_start is None
        assert config.on_run_end is None
        assert config.on_attempt_finished is None

    def test_explicit_scalars_passthrough_without_second_default(self, tmp_path):
        config = ConfigEnv(tmp_path).resolve(
            dep_grace_seconds=1.5,
            commit_failure_threshold=5,
            deadlock_gap_max_rounds=2,
            strict_picklable=False,
            output_root=Path("/tmp/out"),
        )
        assert config.dep_grace_seconds == 1.5
        assert config.commit_failure_threshold == 5
        assert config.deadlock_gap_max_rounds == 2
        assert config.strict_picklable is False
        assert config.output_root == Path("/tmp/out")

    def test_invalid_tuning_fails_loud_at_resolve(self, tmp_path):
        env = ConfigEnv(tmp_path)
        with pytest.raises(ValueError, match="dep_grace_seconds"):
            env.resolve(dep_grace_seconds=0.0)

    def test_hooks_passthrough(self, tmp_path):
        fired_start = []
        fired_end = []
        fired_attempt = []
        config = ConfigEnv(tmp_path).resolve(
            on_run_start=lambda: fired_start.append(1),
            on_run_end=fired_end.append,
            on_attempt_finished=lambda uid, *, outcome: fired_attempt.append(uid),
        )
        config.on_run_start()
        config.on_run_end("completed")
        config.on_attempt_finished("t::1", outcome=None)
        assert fired_start == [1]
        assert fired_end == ["completed"]
        assert fired_attempt == ["t::1"]

    def test_config_is_frozen(self, tmp_path):
        config = ConfigEnv(tmp_path).resolve()
        with pytest.raises(dataclasses.FrozenInstanceError):
            config.name = "mutated"  # type: ignore[misc]

    def test_seam_policies_none_resolved_to_defaults(self, tmp_path):
        """调度策略可空透传：None → FIFO / 立即重入队（唯一解析点）。"""
        config = ConfigEnv(tmp_path).resolve(requeue_policy=None, ordering=None)
        assert isinstance(config.ordering, FifoOrderingPolicy)
        assert isinstance(config.requeue_policy, ImmediateRequeuePolicy)

    def test_seam_policies_passthrough_by_reference(self, tmp_path):
        """显式注入的策略按引用共享（冻结引用而非拷贝）。"""
        env = ConfigEnv(tmp_path)
        custom_ordering = NewestFirstOrdering()
        config = env.resolve(ordering=custom_ordering)
        assert config.ordering is custom_ordering
        assert config.requeue_policy is env.requeue_policy


class TestZeroBackoffSurface:
    """零退避军规：调优面不得出现任何退避类配置项（节奏唯一经
    RequeuePolicy seam，默认立即重入队）。"""

    _BANNED_TERM = "back" + "off"

    def test_no_retry_pacing_fields_on_config_surface(self):
        field_names = [f.name for f in dataclasses.fields(RunConfig)]
        assert field_names, "RunConfig 必须有字段"
        assert not any(self._BANNED_TERM in name for name in field_names), (
            f"调优面出现退避配置项: {field_names}"
        )

    def test_no_retry_pacing_fields_on_tuning(self):
        field_names = [f.name for f in dataclasses.fields(Tuning)]
        assert not any(self._BANNED_TERM in name for name in field_names)
