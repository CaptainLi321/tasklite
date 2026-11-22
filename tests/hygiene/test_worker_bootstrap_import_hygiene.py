"""隔离子进程导入卫生守卫：锁定冷启动不加载父进程调度/存储树与根包延迟导出。"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]

FORBIDDEN_WORKER_MODULES: tuple[str, ...] = (
    "tasklite.pipeline",
    "tasklite.edit",
    "tasklite.snapshot",
    "tasklite.backend.sqlite_backend",
    "tasklite.engine.runtime",
    "tasklite.engine.channel",
    "tasklite.engine.launcher",
    "tasklite.engine.dispatch",
    "tasklite.engine.completion",
    "tasklite.engine.recovery",
    "tasklite.engine.store",
)


def _run_python(code: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )


def test_worker_bootstrap_cold_import_excludes_parent_scheduler_and_storage_tree() -> None:
    """子进程冷启动导入 ``tasklite.engine._worker_bootstrap`` 不得连带加载父进程调度/存储树。"""
    code = (
        "import json, sys; "
        "import tasklite.engine._worker_bootstrap; "
        "loaded = sorted(m for m in sys.modules if m == 'tasklite' or m.startswith('tasklite.')); "
        "print(json.dumps(loaded))"
    )
    result = _run_python(code)
    assert result.returncode == 0, result.stderr
    loaded = set(json.loads(result.stdout))
    leaked = sorted(mod for mod in FORBIDDEN_WORKER_MODULES if mod in loaded)
    assert not leaked, f"worker 冷启动泄露了父进程调度/存储模块: {leaked} (loaded={sorted(loaded)})"


def test_root_package_lazy_exports_defer_heavy_facades_until_accessed() -> None:
    """根包 PEP 562 延迟导出：仅 ``import tasklite`` 不触发重型门面，显式访问时透明可用。"""
    code = (
        "import json, sys, tasklite; "
        "before = sorted(m for m in sys.modules if m in "
        "('tasklite.pipeline', 'tasklite.snapshot', 'tasklite.backend.sqlite_backend')); "
        "assert 'TaskLite' in dir(tasklite) and 'StateSnapshot' in dir(tasklite); "
        "from tasklite import TaskLite, StateSnapshot; "
        "assert isinstance(TaskLite, type) and isinstance(StateSnapshot, type); "
        "print(json.dumps({'before': before, 'all_len': len(tasklite.__all__)}))"
    )
    result = _run_python(code)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["before"] == [], f"仅 import tasklite 不应迫切加载重型模块: {payload['before']}"
    assert payload["all_len"] == 10
