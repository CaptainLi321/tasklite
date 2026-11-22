"""examples/ 目录静态与受控执行卫生门禁。

不变式：
1. ``examples/`` 下全部示例脚本（含 ``examples/edit_migrations/``）必须语法合法、
   具备顶层 ``if __name__ == "__main__":`` 多进程保护，且不得残留已退役的旧符号
   （``output_roots``、``set_job_status``、``clear_history``、``pipeline.authorizer``、
   ``ctx.base_dir``）；
2. 每个示例脚本必须在隔离临时目录中经真实子进程零参数执行且以退出码 0 结束，
   防止示例随主线演进而静默腐烂。
"""
from __future__ import annotations

import ast
import os
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
EXAMPLES_DIR = ROOT / "examples"

_BANNED_NAMES_OR_ATTRS = frozenset(
    {
        "output_roots",
        "set_job_status",
        "clear_history",
    }
)
_BANNED_ATTRS = frozenset(
    {
        "authorizer",
        "base_dir",
    }
)


def _iter_example_scripts() -> tuple[pathlib.Path, ...]:
    scripts = [
        p
        for p in sorted(EXAMPLES_DIR.rglob("*.py"))
        if "__pycache__" not in p.parts
    ]
    return tuple(scripts)


def _has_main_guard(tree: ast.Module) -> bool:
    """检查模块顶层是否存在 ``if __name__ == "__main__":`` 保护。"""
    for node in tree.body:
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if (
            isinstance(test, ast.Compare)
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Eq)
            and len(test.comparators) == 1
        ):
            left, right = test.left, test.comparators[0]
            if (
                isinstance(left, ast.Name)
                and left.id == "__name__"
                and isinstance(right, ast.Constant)
                and right.value == "__main__"
            ) or (
                isinstance(right, ast.Name)
                and right.id == "__name__"
                and isinstance(left, ast.Constant)
                and left.value == "__main__"
            ):
                return True
    return False


def _find_retired_symbols(tree: ast.AST) -> list[str]:
    """扫描 AST 中残留的已退役旧符号。"""
    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id in _BANNED_NAMES_OR_ATTRS:
            hits.append(f"L{node.lineno}:{node.id}")
        elif isinstance(node, ast.Attribute):
            if node.attr in _BANNED_NAMES_OR_ATTRS or node.attr in _BANNED_ATTRS:
                hits.append(f"L{node.lineno}:.{node.attr}")
        elif isinstance(node, ast.keyword) and node.arg in _BANNED_NAMES_OR_ATTRS:
            hits.append(f"L{getattr(node, 'lineno', 0)}:{node.arg}=")
    return hits


_EXAMPLE_SCRIPTS = _iter_example_scripts()


def test_examples_discovery_is_non_empty() -> None:
    """防止目录变动导致示例门禁退化为空通过。"""
    assert len(_EXAMPLE_SCRIPTS) >= 9, (
        f"examples/ 扫描数量异常，期望至少 9 个，实际 {len(_EXAMPLE_SCRIPTS)}"
    )


@pytest.mark.parametrize(
    "script_path",
    _EXAMPLE_SCRIPTS,
    ids=lambda p: p.relative_to(ROOT).as_posix(),
)
def test_example_static_hygiene(script_path: pathlib.Path) -> None:
    """静态 AST 校验：语法合法、无已退役旧符号、具备 __main__ 保护。"""
    source = script_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(script_path))

    retired = _find_retired_symbols(tree)
    assert not retired, (
        f"示例 {script_path.relative_to(ROOT)} 残留已退役旧符号：{retired}"
    )
    assert _has_main_guard(tree), (
        f"示例 {script_path.relative_to(ROOT)} 缺少顶层 `if __name__ == '__main__':` 多进程保护"
    )


@pytest.mark.parametrize(
    "script_path",
    _EXAMPLE_SCRIPTS,
    ids=lambda p: p.relative_to(ROOT).as_posix(),
)
def test_example_runs_cleanly(
    script_path: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """受控子进程执行门禁：零参数自包含运行并正常退出。"""
    temp_dir = tmp_path / "tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    env["TEMP"] = str(temp_dir)
    env["TMP"] = str(temp_dir)

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, (
        f"示例运行失败：{script_path.relative_to(ROOT)}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    assert "[OK]" in result.stdout, (
        f"示例未输出预期 [OK] 验收标记：{script_path.relative_to(ROOT)}\n"
        f"STDOUT:\n{result.stdout}"
    )
