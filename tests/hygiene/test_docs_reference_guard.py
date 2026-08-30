"""文档文件引用存在性门禁（纯文件系统检查，毫秒级完成）。

扫描 README.md 与 docs/API_GUIDE.md 全文，提取形如 tests/xxx.py、
tasklite/xxx.py、scripts/xxx 的文件路径引用（反引号包裹或裸文本
均可——路径字符集在反引号 / 中英文标点 / 空格处自然截断，无需对反引号
做特殊处理），断言每个引用的路径相对仓库根真实存在（文件与目录引用
均合法）——防止文档漂移留下死链接。

示例性伪路径（如 ./state、./out、./downloads/）与 docs/ 前缀不在扫描
范围内：本门禁只认 tests/、tasklite/、scripts/ 三种前缀。
"""

import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

# 受扫描的文档（相对仓库根）
SCANNED_DOCS = (
    ROOT / "README.md",
    ROOT / "docs" / "API_GUIDE.md",
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
