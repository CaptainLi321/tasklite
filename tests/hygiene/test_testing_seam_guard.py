"""测试面接缝守卫：禁止绕过官方 testing 句柄直改引擎私有状态。

`tasklite.testing.running()` 是把引擎置入「run 期间」状态的唯一测试入口；
直接赋值 ``_runtime._is_running`` 需自备 try/finally 复位，漏复位会污染
同会话后续用例。本守卫把该纪律从 code review 转为自动化锁定。
"""

import ast
import pathlib

TESTS_DIR = pathlib.Path(__file__).resolve().parent.parent


def test_no_direct_is_running_assignment_in_tests():
    """tests/ 内禁止对 ``_is_running`` 属性赋值（统一走 testing.running()）。"""
    offenders = []
    for path in sorted(TESTS_DIR.rglob("*.py")):
        if "hygiene" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
                targets = [node.target]
            for t in targets:
                if (
                    isinstance(t, ast.Attribute)
                    and t.attr == "_is_running"
                ):
                    offenders.append(f"{path.relative_to(TESTS_DIR)}:{node.lineno}")
    assert not offenders, (
        "tests 内禁止直接翻转 _is_running（用 tasklite.testing.running()）："
        + ", ".join(offenders)
    )
