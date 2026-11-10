"""TaskLite 测试共享夹具。

夹具均为函数级；测试文件不依赖隐式进程替换，可独立运行。
"""

from __future__ import annotations

from pathlib import Path

import pytest

@pytest.fixture
def tmp_state_dir(tmp_path: Path) -> Path:
    """临时状态目录。"""
    p = tmp_path / "state"
    p.mkdir(parents=True, exist_ok=True)
    return p
