"""核心层类定义禁止同名成员重复定义测试。

Python 类体中后定义的同名方法静默覆盖前者且不报任何错——后续维护
若只改动其中一份，会形成无告警的隐蔽回归。property 的 getter/setter
是合法同名模式（装饰器 ``<名>.setter``），予以豁免。
"""

import ast
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
CORE_PACKAGE_DIRS = (
    ROOT / "tasklite" / "utils",
    ROOT / "tasklite" / "models",
    ROOT / "tasklite" / "backend",
    ROOT / "tasklite" / "engine",
)


def _iter_core_python_files():
    for base in CORE_PACKAGE_DIRS:
        yield from sorted(base.rglob("*.py"))


def _duplicate_members(class_node: ast.ClassDef) -> list:
    names = [
        n.name
        for n in class_node.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        and not any(
            isinstance(d, ast.Attribute) and d.attr == "setter" for d in n.decorator_list
        )
    ]
    return sorted({x for x in names if names.count(x) > 1})


@pytest.mark.parametrize("file_path", _iter_core_python_files(), ids=lambda p: str(p.relative_to(ROOT)))
def test_no_duplicate_class_members(file_path):
    tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    duplicates = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for member in _duplicate_members(node):
                duplicates.append(f"{node.name}.{member}")
    assert not duplicates, f"类成员重复定义（后者静默覆盖前者）: {duplicates}"
