"""v2 包面静态契约测试：主公开面导出、指南导入承诺与分层隔离红线。

锁定四条结构性契约（重构静默破坏即红）：
1. ``tasklite.v2`` 主公开面不吞并 wrappers/contrib——import 主包不触发
   二者加载（wrappers 经子包路径导入，ADR-0004 裁决）；
2. v2 核心层源码（models/utils/backend/engine + 顶层模块）严禁 import
   ``v2/wrappers`` 与 ``v2/contrib``（分层单向依赖红线）；
3. wrappers/contrib 严禁 import v1（tasklite 非 v2 子树任何模块）——
   v2 独立演进的隔离前提；
4. V2_GUIDE 代码块中的 tasklite 导入语句逐条可导入——文档承诺的
   导入路径（如 ``from tasklite.v2 import RequeuePlan``）不得漂移。
"""
from __future__ import annotations

import ast
import os
import pathlib
import re
import subprocess
import sys

import tasklite.v2.wrappers as wrappers_pkg
from tasklite.v2 import contrib

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
V2_ROOT = PROJECT_ROOT / "tasklite" / "v2"

# 核心层源文件：顶层模块 + 四个核心子包（不含 wrappers/contrib 自身）
CORE_FILES = sorted(
    list(V2_ROOT.glob("*.py"))
    + list((V2_ROOT / "models").glob("*.py"))
    + list((V2_ROOT / "utils").glob("*.py"))
    + list((V2_ROOT / "backend").glob("*.py"))
    + list((V2_ROOT / "engine").glob("*.py"))
)

ADAPTER_FILES = sorted(
    list((V2_ROOT / "wrappers").glob("*.py"))
    + list((V2_ROOT / "contrib").glob("*.py"))
)


def _imported_module_names(path: pathlib.Path) -> set[str]:
    """AST 收集文件内全部 import 目标模块名（绝对/相对统一还原为全名）。"""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                names.add(node.module or "")
            else:
                # 相对导入按包深度还原（文件所属包 = tasklite.v2[.parent dirs]）
                parts = path.relative_to(PROJECT_ROOT).with_suffix("").parts[:-1]
                base = ".".join(parts[: len(parts) - (node.level - 1)])
                names.add(f"{base}.{node.module}" if node.module else base)
    return names


class TestWrappersExportFace:
    """wrappers 子包导出面：仅子模块聚合，__all__ 与实际属性一一对应。"""

    def test_all_is_submodule_aggregation_only(self):
        """不设符号级再导出——__all__ 收敛为子模块名单（平行承诺面删除，
        符号一律经 ``tasklite.v2.wrappers.<模块>`` 全路径导入）。"""
        assert wrappers_pkg.__all__ == ["discovery", "http"]

    def test_all_names_importable_and_equal(self):
        for name in wrappers_pkg.__all__:
            assert hasattr(wrappers_pkg, name), f"__all__ 成员 {name!r} 不可导入"
            assert getattr(wrappers_pkg, name) is not None

    def test_discovery_and_http_submodules_exposed(self):
        from tasklite.v2.wrappers import discovery, http

        assert wrappers_pkg.discovery is discovery
        assert wrappers_pkg.http is http

    def test_v2_main_all_excludes_wrappers_symbols(self):
        """主公开面不吞并 wrappers：主 __all__ 不得出现 wrappers 符号。"""
        import tasklite.v2 as v2_pkg

        wrapper_only = {"register_discovery", "http_guard", "urllib_fetch", "requests_fetch"}
        assert wrapper_only.isdisjoint(v2_pkg.__all__)


class TestContribEntry:
    """contrib 生态入口：可导入、空导出面、不被核心依赖。"""

    def test_contrib_importable_with_empty_all(self):
        assert contrib.__all__ == []


class TestLayeringRedLine:
    """分层单向依赖红线（AST 静态锁定）。"""

    def test_core_never_imports_wrappers_or_contrib(self):
        """核心层（models/utils/backend/engine + 顶层）严禁依赖 wrappers/contrib。"""
        offenders = {}
        for path in CORE_FILES:
            banned = {
                name
                for name in _imported_module_names(path)
                if name.startswith(("tasklite.v2.wrappers", "tasklite.v2.contrib"))
            }
            if banned:
                offenders[path.name] = sorted(banned)
        assert not offenders, f"核心层出现反向依赖: {offenders}"

    def test_wrappers_and_contrib_never_import_v1(self):
        """wrappers/contrib 严禁 import tasklite 非 v2 子树（隔离红线）。"""
        offenders = {}
        for path in ADAPTER_FILES:
            banned = {
                name
                for name in _imported_module_names(path)
                if name.startswith("tasklite.")
                and not name.startswith("tasklite.v2")
            }
            if banned:
                offenders[path.name] = sorted(banned)
        assert not offenders, f"适配层出现 v1 依赖: {offenders}"


class TestMainSurfaceMigrationExports:
    """主公开面迁移期补齐的四个导出（__all__ 同步且可导入）。"""

    _MIGRATION_EXPORTS = frozenset(
        {"FATAL_EXCEPTIONS", "TRANSIENT_EXCEPTIONS", "RequeuePlan", "validate_resource_amounts"}
    )

    def test_migration_export_names_in_all(self):
        import tasklite.v2 as v2_pkg

        missing = self._MIGRATION_EXPORTS - set(v2_pkg.__all__)
        assert not missing, f"主公开面 __all__ 缺少迁移期补齐导出: {sorted(missing)}"

    def test_migration_export_names_importable(self):
        from tasklite.v2 import (
            FATAL_EXCEPTIONS,
            TRANSIENT_EXCEPTIONS,
            RequeuePlan,
            validate_resource_amounts,
        )

        assert isinstance(FATAL_EXCEPTIONS, tuple) and isinstance(FATAL_EXCEPTIONS[0], type)
        assert isinstance(TRANSIENT_EXCEPTIONS, tuple) and isinstance(
            TRANSIENT_EXCEPTIONS[0], type
        )
        assert isinstance(RequeuePlan, type)
        assert callable(validate_resource_amounts)


class TestGuideImportContract:
    """V2_GUIDE 代码块中的 tasklite 导入语句逐条可导入（文档承诺不漂移）。"""

    def test_guide_tasklite_imports_resolve(self):
        doc = (PROJECT_ROOT / "docs" / "V2_GUIDE.md").read_text(encoding="utf-8")
        blocks = re.findall(r"```python\n(.+?)```", doc, flags=re.DOTALL)
        assert blocks, "V2_GUIDE 未捕获 python 代码块，围栏标记已漂移"
        stmts: list[str] = []
        for block in blocks:
            for node in ast.walk(ast.parse(block)):
                if (
                    isinstance(node, ast.ImportFrom)
                    and node.level == 0
                    and (node.module or "").startswith("tasklite")
                ):
                    seg = ast.get_source_segment(block, node)
                    assert seg is not None
                    stmts.append(seg)
        assert stmts, "V2_GUIDE 代码块未捕获任何 tasklite 导入语句"
        joined = "\n".join(stmts)
        assert "RequeuePlan" in joined, "V2_GUIDE §5 的 RequeuePlan 导入承诺已漂移"
        for stmt in stmts:
            exec(compile(stmt, "<V2_GUIDE.md>", "exec"), {})


class TestMainSurfaceIsolation:
    """import tasklite.v2 不触发 wrappers/contrib 加载（子进程净 env 验证）。"""

    def test_main_import_skips_wrappers_and_contrib(self):
        code = (
            "import sys; import tasklite.v2; "
            "leaked = [m for m in sys.modules "
            "if m.startswith(('tasklite.v2.wrappers', 'tasklite.v2.contrib'))]; "
            "sys.exit(1 if leaked else 0)"
        )
        env = dict(os.environ)
        env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )
        assert result.returncode == 0, (
            f"import tasklite.v2 不应加载 wrappers/contrib: {result.stderr}"
        )

    def test_wrappers_subpackage_importable_standalone(self):
        code = (
            "import tasklite.v2.wrappers as w; "
            "assert w.__all__ == ['discovery', 'http']; "
            "from tasklite.v2.wrappers.discovery import register_discovery; "
            "assert callable(register_discovery)"
        )
        env = dict(os.environ)
        env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr


def test_no_banned_vocabulary_in_adapter_layer():
    """适配层（wrappers/contrib）禁用词零出现（v2 词汇表红线）。

    禁用词以碎片拼接构造，避免本测试文件自身携带完整词形。
    """
    banned = ["sani" + "tize", "tax" + "onomy", "D" + "LQ", "back" + "off"]
    for path in ADAPTER_FILES:
        content = path.read_text(encoding="utf-8").lower()
        for word in banned:
            assert word.lower() not in content, (
                f"{path.name} 含禁用词碎片 {word!r}"
            )
