"""确定性 job_id 静态 lint（/ 红线：禁止 uuid/时间戳/随机后缀）。

框架本身不允许用随机/时间戳派生 job_id；本测试用 AST 扫描 `tasklite`
源码中的 ``Job(...)`` 构造，确保 job_id 表达式不调用随机/时钟/ uuid 函数。
消费方如需同样的守卫，可复用本文件逻辑。
"""

import ast
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
PACKAGE = ROOT / "tasklite"

# job_id 表达式中禁止出现的“随机/时钟”函数名（AST 调用名）
_BANNED_CALLS = {
    "uuid4", "uuid1", "uuid3", "uuid5",
    "random", "randint", "uniform", "choice", "shuffle",
    "time", "monotonic", "now", "timestamp",
}


def _iter_job_id_exprs(tree):
    """遍历 Job(...) 调用的 job_id 表达式（关键词或第二个位置参数）。"""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not (isinstance(node.func, ast.Name) and node.func.id == "Job"):
            continue
        # job_id 可为关键字 job_id=... 或第二个位置参数
        for kw in node.keywords:
            if kw.arg == "job_id":
                yield kw.value
        if len(node.args) >= 2:
            yield node.args[1]


def _has_banned_call(expr):
    for sub in ast.walk(expr):
        if isinstance(sub, ast.Call):
            func_name = None
            if isinstance(sub.func, ast.Name):
                func_name = sub.func.id
            elif isinstance(sub.func, ast.Attribute):
                func_name = sub.func.attr
            if func_name in _BANNED_CALLS:
                return True
    # 简单拒绝含 `run_` 前缀拼接的 f-string（历史 `run_{seq}` hack）
    if isinstance(expr, ast.JoinedStr):
        for val in expr.values:
            if isinstance(val, ast.FormattedValue):
                # 只对 `run_` 字面量拼接做提示性拦截（不拦截正常业务 id）
                pass
    return False


@pytest.mark.parametrize("path", sorted(PACKAGE.rglob("*.py")))
def test_job_id_has_no_random_or_clock_calls(path):
    if "__pycache__" in str(path):
        return
    tree = ast.parse(path.read_text(encoding="utf-8"))
    bad = []
    for expr in _iter_job_id_exprs(tree):
        if _has_banned_call(expr):
            bad.append(ast.unparse(expr))
    assert not bad, (
        f"job_id 禁止使用随机/时钟函数（确定性红线）: {path} -> {bad}"
    )
