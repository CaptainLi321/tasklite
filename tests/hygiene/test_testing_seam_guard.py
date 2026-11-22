"""测试面接缝守卫：禁止绕过官方 testing 句柄直改引擎私有状态。

`tasklite.testing.running()`、`tasklite.testing.authorizer()`、
`tasklite.testing.in_process_workers()`、`tasklite.testing.cleanup_hook()`、
`tasklite.testing.get_resource()` / `inject_resource()` 与
`tasklite.testing.observe_wait()` 是操作引擎运行态标志、授权回调、进程上下文、
清理钩子、内部资源与调度等待的唯一官方测试入口；直接访问或改写私有属性需自备恢复
逻辑且易污染同会话后续用例。本守卫把该纪律从 code review 转为自动化锁定。
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
        "engine/test_file_runtime_contract.py",
        "engine/test_management.py",
        "engine/test_snapshots.py",
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
                is_mutation_call = (
                    (isinstance(func, ast.Name) and func.id == "setattr")
                    or (
                        isinstance(func, ast.Attribute)
                        and func.attr in ("setattr", "object")
                    )
                )
                if not is_mutation_call:
                    continue
                target_attr = None
                if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
                    target_attr = node.args[1].value
                else:
                    for kw in node.keywords:
                        if kw.arg in ("attribute", "name") and isinstance(kw.value, ast.Constant):
                            target_attr = kw.value.value
                            break
                if target_attr == attr_name:
                    offenders.append(f"{rel}:{node.lineno}")
    return offenders


def _find_resources_attr_accesses() -> list[str]:
    """检测 tests/ 中任意对 ``._resources`` 的访问、赋值、getattr 或 patch.object。"""
    offenders: list[str] = []
    for path in sorted(TESTS_DIR.rglob("*.py")):
        if "hygiene" in path.parts:
            continue
        rel = path.relative_to(TESTS_DIR).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "_resources":
                offenders.append(f"{rel}:{node.lineno}")
            elif isinstance(node, ast.Call):
                func = node.func
                is_attr_hook = (
                    (isinstance(func, ast.Name) and func.id in ("setattr", "getattr"))
                    or (
                        isinstance(func, ast.Attribute)
                        and func.attr in ("setattr", "getattr", "object")
                    )
                )
                if not is_attr_hook:
                    continue
                target_attr = None
                if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
                    target_attr = node.args[1].value
                else:
                    for kw in node.keywords:
                        if kw.arg in ("attribute", "name") and isinstance(kw.value, ast.Constant):
                            target_attr = kw.value.value
                            break
                if target_attr == "_resources":
                    offenders.append(f"{rel}:{node.lineno}")
    return offenders


def _is_path_policy_expr(node: ast.AST) -> bool:
    return (
        (isinstance(node, ast.Attribute) and node.attr == "path_policy")
        or (isinstance(node, ast.Name) and node.id == "path_policy")
    )


def _find_cleanup_hook_bypasses() -> list[str]:
    """检测 tests/ 中直接猴补丁 ``path_policy.delete`` 或 ``delete_target`` 绕过 cleanup_hook 的行为。"""
    offenders: list[str] = []
    for path in sorted(TESTS_DIR.rglob("*.py")):
        if "hygiene" in path.parts:
            continue
        rel = path.relative_to(TESTS_DIR).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
                targets = [node.target]
            for target in targets:
                if isinstance(target, ast.Attribute):
                    if target.attr == "delete_target":
                        offenders.append(f"{rel}:{node.lineno}")
                    elif target.attr == "delete" and _is_path_policy_expr(target.value):
                        offenders.append(f"{rel}:{node.lineno}")
            if isinstance(node, ast.Call):
                func = node.func
                is_patch_call = (
                    (isinstance(func, ast.Name) and func.id == "setattr")
                    or (
                        isinstance(func, ast.Attribute)
                        and func.attr in ("setattr", "object")
                    )
                )
                if (
                    is_patch_call
                    and len(node.args) >= 2
                    and isinstance(node.args[1], ast.Constant)
                ):
                    attr_val = node.args[1].value
                    if attr_val == "delete_target":
                        offenders.append(f"{rel}:{node.lineno}")
                    elif attr_val == "delete" and _is_path_policy_expr(node.args[0]):
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


def test_no_direct_resources_access_in_tests():
    """tests/ 内禁止直接访问或修改 ``._resources``（统一走 testing.get_resource() / inject_resource()）。"""
    offenders = _find_resources_attr_accesses()
    assert not offenders, (
        "tests 内禁止直接访问 ._resources（用 tasklite.testing.get_resource() / inject_resource()）："
        + ", ".join(offenders)
    )


def test_no_direct_cleanup_delete_monkeypatch_in_tests():
    """tests/ 内禁止绕过 ``cleanup_hook`` 直接猴补丁 ``path_policy.delete`` 或 ``delete_target``。"""
    offenders = _find_cleanup_hook_bypasses()
    assert not offenders, (
        "tests 内禁止绕过 tasklite.testing.cleanup_hook() 直接猴补丁 path_policy.delete / delete_target："
        + ", ".join(offenders)
    )


def test_no_direct_runtime_store_backend_access_in_tests():
    """tests/ 内禁止绕过 StateSnapshot / TaskLite 访问 ``._runtime.store.backend``（仅物理故障最小白名单豁免）。"""
    offenders = _find_runtime_store_backend_accesses()
    assert not offenders, (
        "tests 内禁止直接访问 ._runtime.store.backend（只读查询请用 StateSnapshot.open() / TaskLite.get_cursor()）："
        + ", ".join(offenders)
    )
