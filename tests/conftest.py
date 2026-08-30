"""Shared pytest fixtures for tasklite tests.

All fixtures are function-scoped. Each test file can also be run standalone
without these fixtures — conftest is additive, not mandatory.
"""

from __future__ import annotations

import multiprocessing as mp
import time
import warnings
from pathlib import Path

import pytest

from tasklite.models.job import Job
from tasklite.pipeline import TaskLite
from tasklite.backend.sqlite_backend import SQLiteStateBackend


# ── multiprocessing start method 固定 ──────────────────────────────────
# 生产代码用 mp.get_context("spawn")（pipeline.py），而
# _patch_mp_context 让测试子进程走**系统默认** start method——fork 会让子
# 进程内存继承父进程运行态，「注册表必须显式下发」的契约验证假绿。
# 显式固定使测试环境语义与 Python 版本解耦：优先 forkserver（与 spawn 同样
# 通过 pickle 传输 target/args、不继承父进程注册表，且比 spawn 快），
# 不可用时回退 spawn。
# 幂等化（mutmut 兼容）：mutmut 的 PytestRunner.execute_pytest 在**自身
# 进程内嵌调用** pytest.main——每个变异体/每次 stats 收集都会重新
# import 本 conftest。start method 只能设置一次，二次调用抛
# RuntimeError（context has already been set）→ mutmut stats 阶段
# BadTestExecutionCommandsException（历史：mutmut 从未跑通）。try/except
# 使重复 import 静默通过；首次设置后进程内全局生效，语义不变。
if "forkserver" in mp.get_all_start_methods():
    try:
        mp.set_start_method("forkserver")
    except RuntimeError:
        pass  # 已设置（mutmut 内嵌 pytest.main 重复 import），幂等
else:
    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        pass


# ── multiprocessing context patch ──────────────────────────────────────


@pytest.fixture(autouse=True)
def _patch_mp_context(monkeypatch):
    """Make mp.get_context return the global mp module.

    The pipeline uses mp.get_context("spawn") to avoid mutating global state
    In tests, we want monkeypatching mp.Process / mp.Queue to still work.
    By making get_context return the global mp module, the executor's
    self._mp_ctx.Process resolves to the (patched) mp.Process.
    """
    monkeypatch.setattr(mp, "get_context", lambda method: mp)


# ── path fixtures ──────────────────────────────────────────────────────


@pytest.fixture
def tmp_state_dir(tmp_path: Path) -> Path:
    """Temporary state directory (tmp_path / "state"). Created on demand."""
    p = tmp_path / "state"
    p.mkdir(parents=True, exist_ok=True)
    return p


@pytest.fixture
def tmp_output_root(tmp_path: Path) -> Path:
    """Temporary output directory (tmp_path / "output"). Created on demand."""
    p = tmp_path / "output"
    p.mkdir(parents=True, exist_ok=True)
    return p


# ── job factory ────────────────────────────────────────────────────────


@pytest.fixture
def sample_job():
    """Return a factory function that creates Job objects with sensible defaults.

    Usage:
        job = sample_job()
        job = sample_job(task_type="download", payload={"url": "..."})
    """

    def make_job(**overrides) -> Job:
        defaults: dict = {
            "task_type": "test",
            "job_id": "test_1",
            "payload": {},
        }
        defaults.update(overrides)
        return Job(**defaults)

    return make_job


# ── state backends ─────────────────────────────────────────────────────


@pytest.fixture
def sqlite_db(tmp_path: Path) -> SQLiteStateBackend:
    """Create a SQLiteStateBackend in a temp file, clean up after the test."""
    db_path = tmp_path / "test_state.db"
    backend = SQLiteStateBackend(db_path)
    yield backend
    # Cleanup: remove the database file so tests don't leak state.
    if db_path.exists():
        db_path.unlink()


# ── pipeline fixtures ──────────────────────────────────────────────────


@pytest.fixture
def pipeline_sqlite(tmp_state_dir: Path) -> TaskLite:
    """TaskLite with SQLite backend, pointed at tmp_state_dir."""
    return TaskLite(name="test_sqlite", state_dir=tmp_state_dir, backend="sqlite")
