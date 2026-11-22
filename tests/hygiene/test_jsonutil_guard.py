"""全仓裸 json 序列化/反序列化 AST 静态门禁。

锁定 ``tasklite/utils/jsonutil.py`` 声明的序列化一致性契约：
除 ``jsonutil.py`` 自身与极窄显式白名单（仅非落盘/非传输的兜底哈希用途）外，
``tasklite/`` 下任何模块均不得直接调用标准库 ``json.dumps`` / ``json.loads`` /
``json.dump`` / ``json.load``，防止新代码绕过 ``allow_nan=False`` 与非有限浮点/
超长整数拒绝契约。
"""
from __future__ import annotations

import ast
import pathlib

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE_ROOT = PROJECT_ROOT / "tasklite"

_BANNED_JSON_FUNCS = frozenset({"dumps", "loads", "dump", "load"})

# 极窄显式白名单：(相对 tasklite/ 的路径, 所在函数名) -> 仅允许的 json 函数名集合。
# models/state.py 的 job_uid_from_snapshot 仅在畸形快照无法正常提取 uid 时用
# sort_keys=True, default=str 计算兜底哈希（非落盘/传输用途）。
_ALLOWLIST: dict[tuple[str, str | None], frozenset[str]] = {
    ("models/state.py", "job_uid_from_snapshot"): frozenset({"dumps"}),
}


class _RawJsonVisitor(ast.NodeVisitor):
    """AST 遍历收集对标准库 json 序列化/反序列化函数的直接调用。"""

    def __init__(self, rel_path: str) -> None:
        self.rel_path = rel_path
        self._json_module_aliases: set[str] = set()
        self._direct_func_aliases: dict[str, str] = {}
        self._func_stack: list[str] = []
        self.offenders: list[str] = []

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name == "json":
                self._json_module_aliases.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level == 0 and node.module == "json":
            for alias in node.names:
                if alias.name in _BANNED_JSON_FUNCS:
                    self._direct_func_aliases[alias.asname or alias.name] = alias.name
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._func_stack.append(node.name)
        self.generic_visit(node)
        self._func_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._func_stack.append(node.name)
        self.generic_visit(node)
        self._func_stack.pop()

    def visit_Call(self, node: ast.Call) -> None:
        called_func: str | None = None
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in self._json_module_aliases
            and node.func.attr in _BANNED_JSON_FUNCS
        ):
            called_func = node.func.attr
        elif isinstance(node.func, ast.Name) and node.func.id in self._direct_func_aliases:
            called_func = self._direct_func_aliases[node.func.id]

        if called_func is not None:
            enclosing = self._func_stack[-1] if self._func_stack else None
            allowed = _ALLOWLIST.get((self.rel_path, enclosing), frozenset())
            if called_func not in allowed:
                self.offenders.append(f"{self.rel_path}:{node.lineno} (json.{called_func})")
        self.generic_visit(node)


def _find_raw_json_calls(source: str, rel_path: str) -> list[str]:
    tree = ast.parse(source, filename=rel_path)
    visitor = _RawJsonVisitor(rel_path)
    visitor.visit(tree)
    return visitor.offenders


def test_no_raw_json_calls_outside_jsonutil() -> None:
    """除 jsonutil.py 与极窄显式白名单外，tasklite/ 禁止直接调用裸 json.dumps/loads/dump/load。"""
    offenders: list[str] = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        rel = path.relative_to(PACKAGE_ROOT).as_posix()
        if rel == "utils/jsonutil.py":
            continue
        offenders.extend(_find_raw_json_calls(path.read_text(encoding="utf-8"), rel))
    assert not offenders, (
        "检测到绕过 tasklite.utils.jsonutil 的裸 json 调用：" + ", ".join(offenders)
    )


def test_raw_json_guard_catches_direct_and_aliased_calls() -> None:
    """门禁自检：确认直接调用、模块别名与 from-import 别名均能被捕获，防止退化为空通过。"""
    sample = (
        "import json\n"
        "import json as j\n"
        "from json import loads as raw_loads, dump\n"
        "def f(x, fp):\n"
        "    json.dumps(x)\n"
        "    j.load(fp)\n"
        "    raw_loads('{}')\n"
        "    dump(x, fp)\n"
    )
    found = _find_raw_json_calls(sample, "sample.py")
    assert found == [
        "sample.py:5 (json.dumps)",
        "sample.py:6 (json.load)",
        "sample.py:7 (json.loads)",
        "sample.py:8 (json.dump)",
    ]
