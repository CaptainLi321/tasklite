"""代码与测试套件卫生规范静态测试（AST 静态检查门禁）。

自动守护以下红线与规范：
1. 确定性测试检查：测试代码中不得有漏调用的函数引用（例如裸 `time.perf_counter`、`Job.to_dict` 等漏加 `()`）。
2. 审查编号红线检查：源代码与测试代码中的注释与 docstring 中不得出现形如 P0/P1/NFT-xx 的历史审查批次编号。
3. 标识符编号形态检查：类/函数/变量标识符不得含字母-数字-字母交错形态——
   驼峰类名中嵌入的审查编号（如 TestC21 之类前缀）会因词边界逃逸文本扫描，
   AST 标识符检查补齐该盲区。
"""

import ast
import pathlib
import re
import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
PACKAGE = ROOT / "tasklite"
TESTS = ROOT / "tests"

UNATTACHED_CALL_NAMES = {"perf_counter", "monotonic"}

# 审查编号形态（字母 1-2 个 + 数字 1-4 个，含连字符变体）。两种边界形态都算违规：
# - 独立词（前后均非词字符）——与旧 \b 词边界语义等价；
# - 驼峰嵌入（前面紧邻小写字母、后面紧邻大写字母开头的新词，如
#   TestC21Dispatch 中的 C21 段）——\b 因前后均为字母而失配，必须显式放宽，
#   否则编号类名整体逃逸。数字后跟小写字母（如 seedA2 这类种子名）不算
#   驼峰嵌入，不误伤。
_REVIEW_NUMBER_FORMS = (
    r"P[0-9]|P[0-9]-[0-9]+|C[0-9]+|C-[0-9]+|M[0-9]+|M-[0-9]+|"
    r"D[0-9]+|G-[0-9]+|H[0-9]+|L[0-9]+|A[0-9]+|B[0-9]+|R[0-9]+|"
    r"F[0-9]+|J[0-9]+|KL-[0-9]+|Q-[0-9]+|RQ-[0-9]+|NFT-[0-9]+"
)
FORBIDDEN_REVIEW_PATTERN = re.compile(
    rf'((?<=[a-z])({_REVIEW_NUMBER_FORMS})(?=[A-Z])'
    rf'|(?<!\w)({_REVIEW_NUMBER_FORMS})(?!\w)'
    r'|报告[一二三四五六七八九十0-9]+)'
)

# 标识符编号形态：字母-数字-字母交错（编号段嵌在标识符中间，如
# TestBulk3Strike / TestC19StaleDeclarationCleanup）。只匹配交错形态——
# 纯数字结尾的惯用名（j1、sha256 等）不属于该形态，不误伤。
IDENTIFIER_NUMBER_SHAPE = re.compile(r"[A-Za-z]+\d+[A-Za-z]+")


def _check_uncalled_functions(file_path: pathlib.Path) -> list[str]:
    """AST 检查测试文件中是否有裸函数引用（未加括号调用）。"""
    tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    issues = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            if isinstance(node.value, ast.Attribute) and node.value.attr in UNATTACHED_CALL_NAMES:
                issues.append(f"{file_path.name}:{node.lineno} - 属性 `{node.value.attr}` 未被调用（缺少括号 `()`）")
        elif isinstance(node, ast.ListComp):
            if isinstance(node.elt, ast.Attribute) and node.elt.attr in UNATTACHED_CALL_NAMES:
                issues.append(f"{file_path.name}:{node.lineno} - 列表推导式中 `{node.elt.attr}` 未被调用（缺少括号 `()`）")
        elif isinstance(node, ast.BinOp):
            for operand in (node.left, node.right):
                if isinstance(operand, ast.Attribute) and operand.attr in UNATTACHED_CALL_NAMES:
                    issues.append(f"{file_path.name}:{node.lineno} - 表达式操作数 `{operand.attr}` 未被调用（缺少括号 `()`）")
    return issues


def _iter_identifiers(tree: ast.AST):
    """产出 AST 中出现的全部标识符（类名/函数名/参数名/变量引用/属性名等）。

    覆盖变量定义与使用的全部形态：赋值目标、for/with/except 绑定、
    函数与 lambda 形参、调用关键字实参、global/nonlocal、import 别名
    统一经 ast.Name/ast.arg 等节点收集；字符串字面量（job_id 等测试数据）
    不属于标识符，由文本正则另行扫描。
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            yield node.name
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node.name
        elif isinstance(node, ast.Name):
            yield node.id
        elif isinstance(node, ast.Attribute):
            yield node.attr
        elif isinstance(node, ast.arg):
            yield node.arg
        elif isinstance(node, ast.keyword) and node.arg:
            yield node.arg
        elif isinstance(node, ast.ExceptHandler) and node.name:
            yield node.name
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            yield from node.names


@pytest.mark.parametrize("path", sorted(TESTS.rglob("*.py")))
def test_no_uncalled_function_references_in_tests(path: pathlib.Path):
    """测试文件中不得出现裸引用函数导致未实际调用的缺陷（如 time.perf_counter 漏写括号）。"""
    issues = _check_uncalled_functions(path)
    assert not issues, f"在 {path.name} 中发现未调用的函数引用: {issues}"


@pytest.mark.parametrize("path", sorted(PACKAGE.rglob("*.py")) + sorted(TESTS.rglob("*.py")))
def test_no_forbidden_review_numbers_in_source_and_tests(path: pathlib.Path):
    """代码与测试注释/docstring 中一律不得出现形如 P0/P1/NFT-xx 的历史审查批次编号。"""
    if path.name in ("test_code_hygiene.py", "test_job_id_lint.py"):
        return
    content = path.read_text(encoding="utf-8")
    issues = []
    for lineno, line in enumerate(content.splitlines(), start=1):
        clean_line = re.sub(r'#\s*noqa:[^\n]+', '', line)
        clean_line = re.sub(r'%[0-9A-Fa-f]{2}', '', clean_line)
        match = FORBIDDEN_REVIEW_PATTERN.search(clean_line)
        if match:
            issues.append(f"{path.name}:{lineno} 发现审查编号 `{match.group(0)}`: {line.strip()}")
    assert not issues, f"在 {path.name} 中发现禁止的审查批次编号:\n" + "\n".join(issues)


@pytest.mark.parametrize("path", sorted(PACKAGE.rglob("*.py")) + sorted(TESTS.rglob("*.py")))
def test_no_review_number_shaped_identifiers(path: pathlib.Path):
    """类/函数/变量标识符不得含字母-数字-字母交错形态（编号段嫌疑）。

    驼峰类名中嵌入的审查编号段（如 TestC19StaleDeclarationCleanup 的
    C19 段）因 \b 词边界对前后均为字母的形态失配而逃逸文本扫描；本检查
    在 AST 标识符层面直接断言形态。纯数字结尾的惯用名（j1、sha256 等）
    不是交错形态，不误伤。命中即改为行为描述命名。
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    bad = sorted({
        ident for ident in _iter_identifiers(tree)
        if IDENTIFIER_NUMBER_SHAPE.search(ident)
    })
    assert not bad, (
        f"在 {path.name} 中发现编号形态标识符（字母-数字-字母交错，"
        f"应改为行为描述命名）: {bad}"
    )
