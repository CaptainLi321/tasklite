"""真实状态后端的执行权持有、争用与异常归还。"""
from __future__ import annotations

import pytest

from tasklite import TaskLiteError
from tasklite.backend.memory import InMemoryStateBackend
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.engine.execution_access import exclusive_state_access


@pytest.fixture(params=["memory", "sqlite"])
def backend(request, tmp_path):
    value = (
        InMemoryStateBackend()
        if request.param == "memory"
        else SQLiteStateBackend(tmp_path / "state.db")
    )
    yield value
    value.close()


def test_contender_cannot_release_owner_access(backend):
    with exclusive_state_access(backend):
        for _ in range(2):
            with pytest.raises(TaskLiteError, match="in progress"):
                with exclusive_state_access(backend):
                    pytest.fail("竞争者不能进入持有中的作用域")
    with exclusive_state_access(backend):
        pass


def test_base_exception_returns_access(backend):
    with pytest.raises(KeyboardInterrupt):
        with exclusive_state_access(backend):
            raise KeyboardInterrupt
    with exclusive_state_access(backend):
        pass


def test_memory_instances_have_independent_access():
    first, second = InMemoryStateBackend(), InMemoryStateBackend()
    try:
        with exclusive_state_access(first), exclusive_state_access(second):
            pass
    finally:
        first.close()
        second.close()
