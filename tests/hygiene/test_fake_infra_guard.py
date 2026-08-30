"""三层 fake 基础设施耦合链守卫（静态契约锁定，防重构静默破坏）。

fake 测试体系由三层耦合构成，任一层被生产侧或测试侧重构静默改动，都会
让全部 fake 测试失效或假绿。本文件用 AST 断言契约点仍然存在：

1. tests/conftest.py 的 autouse fixture 把 ``mp.get_context`` 重定向为返回
   全局 ``mp`` 模块——executor 持有的 ``self._mp_ctx``（get_context 返回值）
   才会解析到被 monkeypatch 的全局 ``mp.Process``；
2. tests/helpers.py 的 ``patch_multiprocessing_for_fakes`` 必须同时 patch
   全局 ``multiprocessing.Process`` 与 ``tasklite.pipeline.mp.Process``
   双路径（Manager 同理）——漏任一路径即真实子进程逃逸 fake 替换；
3. helpers 的 ``_ctx_incarnation`` 按 FakeProcess args 元组布局
   ``(handler_func, job, ctx, ipc_dir)`` 从 ``args[2]`` 取 ctx.incarnation，
   FakeProcess ``start()`` 从 ``args[1]``/``args[3]`` 取 job/ipc_dir 写
   fake 结果——子进程入口签名变更会静默错位。
"""

import ast
import pathlib

TESTS_DIR = pathlib.Path(__file__).resolve().parent.parent
CONFTEST = TESTS_DIR / "conftest.py"
HELPERS = TESTS_DIR / "helpers.py"


def _parse(path: pathlib.Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _functions(tree: ast.Module, name: str) -> list:
    return [
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
    ]


def _monkeypatch_setattr_calls(func_node):
    """产出 func_node 内所有 monkeypatch.setattr(...) 调用节点。"""
    for node in ast.walk(func_node):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "setattr"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "monkeypatch"
        ):
            yield node


def _fixture_marked_autouse(func_node) -> bool:
    """函数是否带 @pytest.fixture(autouse=True) 装饰。"""
    for dec in func_node.decorator_list:
        if not (
            isinstance(dec, ast.Call)
            and isinstance(dec.func, ast.Attribute)
            and dec.func.attr == "fixture"
        ):
            continue
        for kw in dec.keywords:
            if (
                kw.arg == "autouse"
                and isinstance(kw.value, ast.Constant)
                and kw.value.value is True
            ):
                return True
    return False


def _accesses_arg_index(func_node, index: int) -> bool:
    """func_node 内是否存在 args[index] / self.args[index] 访问。"""
    for node in ast.walk(func_node):
        if not isinstance(node, ast.Subscript):
            continue
        base = node.value
        is_args = (
            (isinstance(base, ast.Name) and base.id == "args")
            or (isinstance(base, ast.Attribute) and base.attr == "args")
        )
        if (
            is_args
            and isinstance(node.slice, ast.Constant)
            and node.slice.value == index
        ):
            return True
    return False


def test_conftest_autouse_fixture_redirects_get_context_to_global_mp():
    """契约 1：autouse fixture 把 mp.get_context 重定向为返回全局 mp。"""
    tree = _parse(CONFTEST)
    autouse_fixtures = [
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and _fixture_marked_autouse(node)
    ]
    assert autouse_fixtures, (
        "conftest.py 必须存在 autouse fixture——mp context 重定向是全部 "
        "fake 测试生效的前提"
    )

    redirects = []
    for fixture in autouse_fixtures:
        for call in _monkeypatch_setattr_calls(fixture):
            if (
                len(call.args) >= 3
                and isinstance(call.args[0], ast.Name)
                and call.args[0].id == "mp"
                and isinstance(call.args[1], ast.Constant)
                and call.args[1].value == "get_context"
                and isinstance(call.args[2], ast.Lambda)
                and isinstance(call.args[2].body, ast.Name)
                and call.args[2].body.id == "mp"
            ):
                redirects.append((fixture.name, call.lineno))
    assert redirects, (
        "autouse fixture 必须含 monkeypatch.setattr(mp, 'get_context', "
        "lambda method: mp)——get_context 若返回真实 spawn context，"
        "executor 的 self._mp_ctx.Process 将绕过全局 mp.Process patch，"
        "fake 测试全部静默失效"
    )


def test_helpers_patch_covers_global_and_pipeline_mp_paths():
    """契约 2：patch 函数同时覆盖全局与 tasklite.pipeline.mp 双路径。"""
    tree = _parse(HELPERS)
    funcs = _functions(tree, "patch_multiprocessing_for_fakes")
    assert funcs, "helpers.patch_multiprocessing_for_fakes 必须存在"

    string_targets = {}
    for call in _monkeypatch_setattr_calls(funcs[0]):
        if (
            call.args
            and isinstance(call.args[0], ast.Constant)
            and isinstance(call.args[0].value, str)
        ):
            string_targets[call.args[0].value] = (
                call.args[1] if len(call.args) > 1 else None
            )

    # Process 双路径：全局 multiprocessing.Process + tasklite.pipeline.mp.Process
    for target in (
        "multiprocessing.Process",
        "tasklite.pipeline.mp.Process",
    ):
        assert target in string_targets, (
            f"patch_multiprocessing_for_fakes 必须同时 patch {target}——"
            "漏任一路径，pipeline 侧真实子进程将逃逸 fake 替换"
        )
        value = string_targets[target]
        assert (
            isinstance(value, ast.Name) and value.id == "fake_process_class"
        ), f"{target} 必须 patch 为 fake_process_class"

    # Manager 双路径同理：fake Manager 防真实 server 进程拖慢/挂起测试
    for target in (
        "multiprocessing.Manager",
        "tasklite.pipeline.mp.Manager",
    ):
        assert target in string_targets, (
            f"patch_multiprocessing_for_fakes 必须同时 patch {target}"
        )


def test_ctx_incarnation_parses_fake_args_layout():
    """契约 3：_ctx_incarnation 按 (handler_func, job, ctx, ipc_dir) 布局取 args[2]。"""
    tree = _parse(HELPERS)
    funcs = _functions(tree, "_ctx_incarnation")
    assert funcs, "helpers._ctx_incarnation 必须存在（fake 进程 incarnation 提取）"
    inc = funcs[0]

    assert _accesses_arg_index(inc, 2), (
        "_ctx_incarnation 必须从 args[2] 取 ctx——FakeProcess args 布局 "
        "(handler_func, job, ctx, ipc_dir) 的第 3 位；子进程入口签名变更"
        "（新增/删除前置参数）会静默取错 incarnation，fencing 全部失效"
    )

    has_length_guard = any(
        isinstance(node, ast.Compare)
        and isinstance(node.left, ast.Call)
        and isinstance(node.left.func, ast.Name)
        and node.left.func.id == "len"
        for node in ast.walk(inc)
    )
    assert has_length_guard, (
        "_ctx_incarnation 必须带 len(args) 长度防御（短元组返回 None 而非 IndexError）"
    )

    reads_incarnation = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and node.args
        and isinstance(node.args[1], ast.Constant)
        and node.args[1].value == "incarnation"
        for node in ast.walk(inc)
    )
    assert reads_incarnation, (
        "_ctx_incarnation 必须读取 ctx.incarnation（fencing 契约的核心属性）"
    )


def test_fake_process_classes_read_job_and_ipc_dir_from_args_layout():
    """契约 3b：FakeProcess 从 args[1]/args[3] 取 job/ipc_dir 写 fake 结果。"""
    tree = _parse(HELPERS)
    for factory_name in (
        "make_fake_process_class",
        "make_ipc_process_class",
    ):
        funcs = _functions(tree, factory_name)
        assert funcs, f"helpers.{factory_name} 必须存在"
        factory = funcs[0]
        assert _accesses_arg_index(factory, 1), (
            f"{factory_name} 的 FakeProcess 必须从 self.args[1] 取 job——"
            "args 布局 (handler_func, job, ctx, ipc_dir) 第 2 位"
        )
        assert _accesses_arg_index(factory, 3), (
            f"{factory_name} 的 FakeProcess 必须从 self.args[3] 取 ipc_dir——"
            "args 布局 (handler_func, job, ctx, ipc_dir) 第 4 位，错位则 "
            "fake 结果写入错误目录、drain 永远读不到"
        )
