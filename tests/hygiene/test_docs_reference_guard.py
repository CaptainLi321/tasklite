"""文档契约门禁。

扫描 README.md 与 docs/GUIDE.md 全文，提取形如
tests/xxx.py、tasklite/xxx.py、scripts/xxx 的文件路径引用（反引号包裹
或裸文本均可——路径字符集在反引号 / 中英文标点 / 空格处自然截断，
无需对反引号做特殊处理），断言每个引用的路径相对仓库根真实存在
（文件与目录引用均合法）——防止文档漂移留下死链接。

示例性伪路径（如 ./state、./out、./downloads/）与 docs/ 前缀不在扫描
范围内：本门禁只认 tests/、tasklite/、scripts/ 三种前缀。

另在临时目录提取并运行两个最小示例，锁定生产 spawn 链路下的公开导入、
SQLite/内存状态、快照查询与文件声明，防止文档接口漂移。
"""

import ast
import os
import pathlib
import re
import subprocess
import sys
import textwrap

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

# 受扫描的文档（相对仓库根）
SCANNED_DOCS = (
    ROOT / "README.md",
    ROOT / "docs" / "GUIDE.md",
    ROOT / "docs" / "API.md",
)

# 路径引用提取模式：三种前缀之一开头，后接常规路径字符。
# 前置的负向断言拒绝「更长标识符的尾部」被误认成引用（如 xxxtests/）；
# 路径字符集不含反引号与标点，因此反引号包裹的引用天然截断。
_DOC_REF_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_./-])(?:tests|tasklite|scripts)/[A-Za-z0-9_./-]+"
)


def _doc_refs(doc: pathlib.Path) -> list:
    """提取单个文档中受扫描前缀约束的路径引用，返回 (行号, 引用) 列表。"""
    refs = []
    text = doc.read_text(encoding="utf-8")
    for lineno, line in enumerate(text.splitlines(), start=1):
        for match in _DOC_REF_PATTERN.finditer(line):
            refs.append((lineno, match.group(0)))
    return refs


def _minimal_example(doc: pathlib.Path, heading: str) -> str:
    """提取文档指定小节中的首个 Python 代码块。"""
    text = doc.read_text(encoding="utf-8")
    heading_at = text.find(heading)
    assert heading_at >= 0, f"{doc.name} 缺少最小示例小节 {heading!r}"
    block_at = text.find("```python\n", heading_at)
    assert block_at >= 0, f"{doc.name} 最小示例缺少 Python 代码块"
    code_at = block_at + len("```python\n")
    block_end = text.find("\n```", code_at)
    assert block_end >= 0, f"{doc.name} 最小示例代码围栏未闭合"
    return text[code_at:block_end]


def _extract_doc_tasklite_imports(doc: pathlib.Path) -> list[tuple[int, str]]:
    """提取文档中所有 python 代码块内针对 tasklite 的 import 语句（源码片段）。

    通过 AST 解析准确提取 import / from ... import 语句；
    若代码块存在语法错误直接抛出，杜绝将语法异常静默当空检查。
    """
    text = doc.read_text(encoding="utf-8")
    blocks = re.findall(r"```python\n(.+?)```", text, flags=re.DOTALL)
    imports = []
    for block in blocks:
        dedented = textwrap.dedent(block)
        tree = ast.parse(dedented)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and (node.module or "").startswith("tasklite")
            ) or (
                isinstance(node, ast.Import)
                and any(alias.name.startswith("tasklite") for alias in node.names)
            ):
                seg = ast.get_source_segment(dedented, node)
                assert seg is not None
                imports.append((getattr(node, "lineno", 1), seg))
    return imports


@pytest.mark.parametrize("doc", SCANNED_DOCS, ids=lambda d: d.name)
def test_doc_file_references_exist(doc: pathlib.Path) -> None:
    """文档引用的 tests/、tasklite/、scripts/ 路径必须在仓库根下存在。"""
    assert doc.exists(), f"待扫描文档不存在：{doc}"
    issues = []
    for lineno, ref in _doc_refs(doc):
        # 目录引用（以 / 结尾，如 tasklite/engine/）与文件引用同等对待：
        # 存在性检查对两者都成立即可
        if not (ROOT / ref).exists():
            issues.append(f"{doc.relative_to(ROOT)}:{lineno} 引用了不存在的路径 `{ref}`")
    assert not issues, "文档存在死引用（文档漂移）：\n" + "\n".join(issues)


def test_doc_reference_extraction_is_not_empty() -> None:
    """提取器必须从文档中捕获到引用——空提取会让存在性检查空通过。"""
    total = sum(len(_doc_refs(doc)) for doc in SCANNED_DOCS)
    assert total > 0, "扫描正则未捕获任何文档引用，存在性检查退化为空通过"


@pytest.mark.parametrize(
    ("doc_name", "heading"),
    (
        ("README.md", "### 最小示例"),
        ("docs/GUIDE.md", "### 最小可运行示例"),
    ),
)
def test_doc_minimal_example_runs_with_real_spawn(
    doc_name: str, heading: str, tmp_path: pathlib.Path
) -> None:
    """README/GUIDE 最小示例必须在新目录经生产 spawn 链路真实执行。"""
    doc = ROOT / doc_name
    script = tmp_path / "example.py"
    script.write_text(_minimal_example(doc, heading), encoding="utf-8")
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stderr
    if doc_name == "README.md":
        assert "succeeded" in result.stdout
        return

    assert (tmp_path / "output" / "files" / "item_1.txt").read_text(
        encoding="utf-8"
    ) == "示例一"
    assert (tmp_path / "output" / "files" / "item_2.txt").read_text(
        encoding="utf-8"
    ) == "示例二"


@pytest.mark.parametrize("doc", SCANNED_DOCS, ids=lambda d: d.name)
def test_doc_tasklite_imports_are_valid(doc: pathlib.Path) -> None:
    """文档中所有标注为 python 的导入块必须准确导入且在真实环境中执行成功。"""
    assert doc.exists(), f"待扫描文档不存在：{doc}"
    imports = _extract_doc_tasklite_imports(doc)
    assert len(imports) > 0, f"{doc.name} 未提取到任何 tasklite 导入语句，门禁可能退化"
    for lineno, stmt in imports:
        scope: dict = {}
        compiled = compile(stmt, f"<{doc.name}:{lineno}>", "exec")
        exec(compiled, scope)

