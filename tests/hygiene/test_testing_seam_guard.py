"""测试面接缝守卫：禁止绕过官方 testing 句柄直改引擎私有状态。

`tasklite.testing.running()`、`tasklite.testing.authorizer()` 与
`tasklite.testing.in_process_workers()` 是操作引擎运行态标志、授权回调与
进程上下文的唯一官方测试入口；直接赋值私有属性需自备恢复逻辑且易污染同会话
后续用例。本守卫把该纪律从 code review 转为自动化锁定。
"""

import ast
import pathlib

TESTS_DIR = pathlib.Path(__file__).resolve().parent.parent

_GUARDED_ATTRS: dict[str, str] = {
    "_is_running": "tasklite.testing.running()",
    "authorizer": "tasklite.testing.authorizer()",
    "_launcher": "tasklite.testing.in_process_workers()",
    "_wait": "tasklite.testing.observe_wait()",
}

# 确需注入 SQLite 物理损坏/故障或底层落盘格式断言的最小化显式白名单。
_BACKEND_ACCESS_ALLOWLIST: frozenset[str] = frozenset(
    {
        "engine/test_database_faults.py",
        "engine/test_edit.py",
        "engine/test_file_runtime_contract.py",
        "engine/test_management.py",
        "engine/test_snapshots.py",
        "engine/test_sqlite_read_faults.py",
        "engine/test_store.py",
        "engine/test_subprocess_isolation.py",
    }
)


def _is_runtime_store_backend(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "backend"
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "store"
        and isinstance(node.value.value, ast.Attribute)
        and node.value.value.attr == "_runtime"
    )


def _find_runtime_store_backend_accesses() -> list[str]:
    offenders: list[str] = []
    for path in sorted(TESTS_DIR.rglob("*.py")):
        if "hygiene" in path.parts:
            continue
        rel = path.relative_to(TESTS_DIR).as_posix()
        if rel in _BACKEND_ACCESS_ALLOWLIST:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if _is_runtime_store_backend(node):
                offenders.append(f"{rel}:{node.lineno}")
    return offenders


def _find_guarded_mutations(attr_name: str) -> list[str]:
    offenders = []
    for path in sorted(TESTS_DIR.rglob("*.py")):
        if "hygiene" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        rel = path.relative_to(TESTS_DIR)
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
                targets = [node.target]
            for t in targets:
                if isinstance(t, ast.Attribute) and t.attr == attr_name:
                    offenders.append(f"{rel}:{node.lineno}")
            if isinstance(node, ast.Call):
                func = node.func
                is_setattr = (
                    (isinstance(func, ast.Name) and func.id == "setattr")
                    or (isinstance(func, ast.Attribute) and func.attr == "setattr")
                )
                if (
                    is_setattr
                    and len(node.args) >= 2
                    and isinstance(node.args[1], ast.Constant)
                    and node.args[1].value == attr_name
                ):
                    offenders.append(f"{rel}:{node.lineno}")
    return offenders


def test_no_direct_is_running_assignment_in_tests():
    """tests/ 内禁止对 ``_is_running`` 赋值（统一走 testing.running()）。"""
    offenders = _find_guarded_mutations("_is_running")
    assert not offenders, (
        f"tests 内禁止直接翻转 _is_running（用 {_GUARDED_ATTRS['_is_running']}）："
        + ", ".join(offenders)
    )


def test_no_direct_authorizer_assignment_in_tests():
    """tests/ 内禁止对 ``authorizer`` 赋值（统一走 testing.authorizer()）。"""
    offenders = _find_guarded_mutations("authorizer")
    assert not offenders, (
        f"tests 内禁止直接赋值 authorizer（用 {_GUARDED_ATTRS['authorizer']}）："
        + ", ".join(offenders)
    )


def test_no_direct_launcher_assignment_in_tests():
    """tests/ 内禁止对 ``_launcher`` 赋值（统一走 testing.in_process_workers()）。"""
    offenders = _find_guarded_mutations("_launcher")
    assert not offenders, (
        f"tests 内禁止直接赋值 _launcher（用 {_GUARDED_ATTRS['_launcher']}）："
        + ", ".join(offenders)
    )


def test_no_direct_wait_mutation_in_tests():
    """tests/ 内禁止直接改写 ``_wait``（统一走 testing.observe_wait()）。"""
    offenders = _find_guarded_mutations("_wait")
    assert not offenders, (
        f"tests 内禁止直接改写 _wait（用 {_GUARDED_ATTRS['_wait']}）："
        + ", ".join(offenders)
    )


def test_no_direct_runtime_store_backend_access_in_tests():
    """tests/ 内禁止绕过 StateSnapshot / TaskLite 访问 ``._runtime.store.backend``（仅物理故障最小白名单豁免）。"""
    offenders = _find_runtime_store_backend_accesses()
    assert not offenders, (
        "tests 内禁止直接访问 ._runtime.store.backend（只读查询请用 StateSnapshot.open() / TaskLite.get_cursor()）："
        + ", ".join(offenders)
    )
