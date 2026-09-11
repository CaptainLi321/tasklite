"""核心层公开 API 类型注解可内省性测试。

不变式：核心四层（utils/models/backend/engine）全部公开函数、方法与
property 的类型注解必须能被 ``typing.get_type_hints`` 解析。
``from __future__ import annotations`` 使注解惰性求值——「注解引用了
未导入名字」的缺陷在运行时静默潜伏，仅在内省场景（文档生成、依赖
注入、序列化框架）才以 NameError 爆炸，本测试把该爆炸提前到 CI。

检查范围以 AST 圈定源码真实定义的函数（typing.NamedTuple 等合成的
``__new__`` 的 globals 是合成命名空间，不是用户可修的注解代码，排除）。

按项目约定仅存在于静态检查分支（``if TYPE_CHECKING:`` 或其等价写法
``if False:``，后者用于规避运行时环形导入）的名字以 ``Any`` 兜底豁免；
完全未导入的名字不豁免，一律判红。
"""

import ast
import importlib
import inspect
import pkgutil
import typing

import pytest

CORE_PACKAGES = ("tasklite.utils", "tasklite.models", "tasklite.backend", "tasklite.engine")


def _is_static_only_branch(test) -> bool:
    """识别运行时恒假的静态检查分支（``if TYPE_CHECKING:`` / ``if False:``）。"""
    if isinstance(test, ast.Constant) and test.value is False:
        return True
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _source_defined_names(module) -> "dict[str, set]":
    """AST 收集模块源码真实定义的函数名：``{"": {...}, "ClassName": {...}}``。"""
    try:
        source = inspect.getsource(module)
    except (OSError, TypeError):
        return {}
    tree = ast.parse(source)
    defined: dict = {"": set()}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            defined[""].add(node.name)
        elif isinstance(node, ast.ClassDef):
            defined[node.name] = {
                n.name
                for n in node.body
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
    return defined


def _type_checking_only_names(module) -> set:
    """收集仅静态检查分支导入、运行时不可用的名字。"""
    try:
        source = inspect.getsource(module)
    except (OSError, TypeError):
        return set()
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.If) and _is_static_only_branch(node.test):
            for inner in ast.walk(node):
                if isinstance(inner, ast.ImportFrom):
                    names.update(a.asname or a.name for a in inner.names)
                elif isinstance(inner, ast.Import):
                    names.update((a.asname or a.name).split(".")[0] for a in inner.names)
    return {n for n in names if not hasattr(module, n)}


def _resolve_hints(func, module):
    """解析注解；仅对静态专用导入的名字以 Any 兜底重试。"""
    try:
        return typing.get_type_hints(func)
    except NameError:
        localns = {n: typing.Any for n in _type_checking_only_names(module)}
        return typing.get_type_hints(func, localns=localns)


def _iter_core_modules():
    for pkg_name in CORE_PACKAGES:
        pkg = importlib.import_module(pkg_name)
        pkg_path = getattr(pkg, "__path__", None)
        if pkg_path is None:
            yield pkg_name
            continue
        for mod in pkgutil.walk_packages(pkg_path, prefix=pkg_name + "."):
            yield mod.name


def _iter_annotated_callables():
    cases = []
    for mod_name in _iter_core_modules():
        module = importlib.import_module(mod_name)
        defined = _source_defined_names(module)
        if not defined:
            continue
        for name, value in vars(module).items():
            if name.startswith("_") or getattr(value, "__module__", None) != mod_name:
                continue
            if inspect.isfunction(value) and name in defined[""]:
                cases.append((mod_name, name, value))
            elif inspect.isclass(value) and name in defined:
                member_names = defined[name]
                for attr_name, attr in vars(value).items():
                    if attr_name not in member_names:
                        continue
                    if isinstance(attr, (staticmethod, classmethod)):
                        cases.append((mod_name, f"{name}.{attr_name}", attr.__func__))
                    elif inspect.isfunction(attr):
                        cases.append((mod_name, f"{name}.{attr_name}", attr))
                    elif isinstance(attr, property) and attr.fget is not None:
                        cases.append((mod_name, f"{name}.{attr_name}", attr.fget))
    return cases


_CASES = _iter_annotated_callables()


@pytest.mark.parametrize(
    "module_name, qualname",
    [(m, q) for m, q, _ in _CASES],
    ids=[f"{m}::{q}" for m, q, _ in _CASES],
)
def test_type_hints_resolvable(module_name, qualname):
    module = importlib.import_module(module_name)
    obj = module
    for part in qualname.split("."):
        obj = getattr(obj, part)
    if isinstance(obj, (staticmethod, classmethod)):
        obj = obj.__func__
    elif isinstance(obj, property):
        obj = obj.fget
    _resolve_hints(obj, module)
