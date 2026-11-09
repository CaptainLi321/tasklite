"""TaskLite 测试共享夹具。

夹具均为函数级；测试文件不依赖隐式进程替换，可独立运行。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite


# ── 路径夹具 ──────────────────────────────────────────────────────────


@pytest.fixture
def tmp_state_dir(tmp_path: Path) -> Path:
    """临时状态目录。"""
    p = tmp_path / "state"
    p.mkdir(parents=True, exist_ok=True)
    return p


@pytest.fixture
def tmp_output_root(tmp_path: Path) -> Path:
    """临时产物目录。"""
    p = tmp_path / "output"
    p.mkdir(parents=True, exist_ok=True)
    return p


# ── Job 工厂 ──────────────────────────────────────────────────────────


@pytest.fixture
def sample_job():
    """返回带默认值的 Job 构造工厂。"""

    def make_job(**overrides) -> Job:
        defaults: dict = {
            "task_type": "test",
            "job_id": "test_1",
            "payload": {},
        }
        defaults.update(overrides)
        return Job(**defaults)

    return make_job


# ── 状态后端 ──────────────────────────────────────────────────────────


@pytest.fixture
def sqlite_db(tmp_path: Path) -> SQLiteStateBackend:
    """使用临时文件创建真实 SQLite 后端，并在测试后关闭、清理。"""
    db_path = tmp_path / "test_state.db"
    backend = SQLiteStateBackend(db_path)
    try:
        yield backend
    finally:
        backend.close()
        for suffix in ("", "-wal", "-shm"):
            db_path.with_name(db_path.name + suffix).unlink(missing_ok=True)


# ── 管线夹具 ──────────────────────────────────────────────────────────


@pytest.fixture
def pipeline_sqlite(tmp_state_dir: Path) -> TaskLite:
    """使用临时 SQLite 数据库的 TaskLite 实例。"""
    pipeline = TaskLite(backend="sqlite", state_path=tmp_state_dir / "test_state.db")
    try:
        yield pipeline
    finally:
        pipeline.close()
