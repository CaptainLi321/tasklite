"""运行配置按引用装配，内部策略在装配时解析默认值。"""

from __future__ import annotations

from tasklite.triggers import BUILTIN_TRIGGER_FUNCTIONS

import dataclasses

import pytest

from tasklite.backend.memory import InMemoryStateBackend
from tasklite.engine.admission import (
    ImmediateRequeuePolicy,
    RequeuePolicy,
    TriggerRegistry,
)
from tasklite.engine.channel import ExecutionChannel
from tasklite.engine.config import RunConfig

from tasklite.engine.governor import (
    DeadlockGovernor,
)
from tasklite.engine.resource import ResourceManager
from tasklite.engine.scheduler import FifoOrderingPolicy, OrderingPolicy
from tasklite.models.task import TaskRegistry


class NewestFirstOrdering(OrderingPolicy):
    """队尾优先访问序样本（引用共享断言用）。"""

    def visit_order(self, queue):
        return range(len(queue) - 1, -1, -1)


class ConfigEnv:
    """最小装配集（字段即 RunConfig 装配件清单）。"""

    def __init__(self, tmp_path) -> None:
        self.backend = InMemoryStateBackend()
        self.resources = ResourceManager()
        self.tasks = TaskRegistry()
        self.channel = ExecutionChannel()
        self.governor = DeadlockGovernor()
        self.trigger_registry = TriggerRegistry(functions=BUILTIN_TRIGGER_FUNCTIONS)
        self.requeue_policy = ImmediateRequeuePolicy()

    def resolve(self, **overrides) -> RunConfig:
        base = dict(
            name="test_config",
            backend=self.backend,
            resources=self.resources,
            tasks=self.tasks,
            channel=self.channel,
            governor=self.governor,
            trigger_registry=self.trigger_registry,
            requeue_policy=self.requeue_policy,
            output_roots=None,
        )
        base.update(overrides)
        return RunConfig.resolve(**base)


class TestRunConfigResolve:
    """RunConfig.resolve 唯一默认值解析点。"""

    def test_assembled_components_shared_by_reference(self, tmp_path):
        env = ConfigEnv(tmp_path)
        config = env.resolve()
        assert config.backend is env.backend
        assert config.resources is env.resources
        assert config.tasks is env.tasks
        assert config.channel is env.channel
        assert config.governor is env.governor
        assert config.trigger_registry is env.trigger_registry
        assert config.requeue_policy is env.requeue_policy
        assert config.name == "test_config"


    @pytest.mark.parametrize("field", [
        "strict_picklable", "commit_failure_threshold", "deadlock_gap_max_rounds",
    ])
    def test_configuration_rejects_removed_tuning(self, tmp_path, field):
        with pytest.raises(TypeError, match=field):
            ConfigEnv(tmp_path).resolve(**{field: 1})

    def test_config_is_frozen(self, tmp_path):
        config = ConfigEnv(tmp_path).resolve()
        with pytest.raises(dataclasses.FrozenInstanceError):
            config.name = "mutated"  # type: ignore[misc]

    def test_seam_policies_none_resolved_to_defaults(self, tmp_path):
        """调度策略可空透传：None → FIFO / 立即重入队（唯一解析点）。"""
        config = ConfigEnv(tmp_path).resolve(requeue_policy=None, ordering_policy=None)
        assert isinstance(config.ordering_policy, FifoOrderingPolicy)
        assert isinstance(config.requeue_policy, ImmediateRequeuePolicy)

    def test_seam_policies_passthrough_by_reference(self, tmp_path):
        """显式注入的策略按引用共享（冻结引用而非拷贝）。"""
        env = ConfigEnv(tmp_path)
        custom_ordering = NewestFirstOrdering()
        config = env.resolve(ordering_policy=custom_ordering)
        assert config.ordering_policy is custom_ordering
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
