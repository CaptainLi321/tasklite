"""包面静态契约与发布元数据测试：主公开面导出、分层单向依赖红线与 pyproject 一致性。

锁定契约：
1. ``tasklite`` 主公开面不吞并 wrappers/contrib（wrappers 经子包路径导入）；
2. 核心层源码（models/utils/backend/engine + 顶层模块）严禁 import
   ``tasklite/wrappers`` 与 ``tasklite/contrib``，且 models 严禁 import
   engine、utils 严禁 import wrappers；
3. contrib 空导出面且不被核心依赖；
4. GUIDE 代码块中的 tasklite 导入语句逐条可导入；
5. pyproject.toml 元数据（支持版本、SPDX 许可证、动态版本）与运行期一致。
"""
from __future__ import annotations

import ast
import inspect
import os
import pathlib
import re
import subprocess
import sys

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import tasklite.contrib as contrib
import tasklite.wrappers as wrappers_pkg

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE_ROOT = PROJECT_ROOT / "tasklite"
PYPROJECT = PROJECT_ROOT / "pyproject.toml"
SUPPORTED_PYTHONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]

# 核心层源文件：顶层模块 + 四个核心子包（不含 wrappers/contrib 自身）
CORE_FILES = sorted(
    list(PACKAGE_ROOT.glob("*.py"))
    + list((PACKAGE_ROOT / "models").glob("*.py"))
    + list((PACKAGE_ROOT / "utils").glob("*.py"))
    + list((PACKAGE_ROOT / "backend").glob("*.py"))
    + list((PACKAGE_ROOT / "engine").glob("*.py"))
)

ADAPTER_FILES = sorted(
    list((PACKAGE_ROOT / "wrappers").glob("*.py"))
    + list((PACKAGE_ROOT / "contrib").glob("*.py"))
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
                parts = path.relative_to(PROJECT_ROOT).with_suffix("").parts[:-1]
                base = ".".join(parts[: len(parts) - (node.level - 1)])
                names.add(f"{base}.{node.module}" if node.module else base)
    return names


def _load_pyproject() -> dict:
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)


class TestWrappersExportFace:
    """wrappers 子包导出面：仅子模块聚合，__all__ 与实际属性一一对应。"""

    def test_all_is_submodule_aggregation_only(self):
        assert wrappers_pkg.__all__ == ["discovery", "http"]

    def test_all_names_importable_and_equal(self):
        for name in wrappers_pkg.__all__:
            assert hasattr(wrappers_pkg, name), f"__all__ 成员 {name!r} 不可导入"
            assert getattr(wrappers_pkg, name) is not None

    def test_discovery_and_http_submodules_exposed(self):
        from tasklite.wrappers import discovery, http

        assert wrappers_pkg.discovery is discovery
        assert wrappers_pkg.http is http

    def test_main_all_excludes_wrappers_symbols(self):
        import tasklite as tasklite_pkg

        wrapper_only = {"register_discovery", "http_guard", "urllib_fetch", "requests_fetch"}
        assert wrapper_only.isdisjoint(tasklite_pkg.__all__)


class TestContribEntry:
    """contrib 生态入口：可导入、空导出面、不被核心依赖。"""

    def test_contrib_importable_with_empty_all(self):
        assert contrib.__all__ == []


class TestLayeringRedLine:
    """分层单向依赖红线（AST 静态锁定）。"""

    def test_core_never_imports_wrappers_or_contrib(self):
        offenders = {}
        for path in CORE_FILES:
            banned = {
                name
                for name in _imported_module_names(path)
                if name.startswith(("tasklite.wrappers", "tasklite.contrib"))
            }
            if banned:
                offenders[path.name] = sorted(banned)
        assert not offenders, f"核心层出现反向依赖: {offenders}"

    def test_models_never_imports_engine(self):
        offenders = {}
        for path in sorted((PACKAGE_ROOT / "models").glob("*.py")):
            banned = {
                name
                for name in _imported_module_names(path)
                if name.startswith("tasklite.engine")
            }
            if banned:
                offenders[path.name] = sorted(banned)
        assert not offenders, f"模型层出现引擎依赖: {offenders}"

    def test_utils_never_imports_wrappers(self):
        offenders = {}
        for path in sorted((PACKAGE_ROOT / "utils").glob("*.py")):
            banned = {
                name
                for name in _imported_module_names(path)
                if name.startswith("tasklite.wrappers")
            }
            if banned:
                offenders[path.name] = sorted(banned)
        assert not offenders, f"工具层出现 wrappers 反向依赖: {offenders}"


class TestMainSurfaceExports:
    """主公开面的核心导出（__all__ 同步且可导入）。"""

    _CORE_SYMBOLS = frozenset(
        {
            "TaskLite",
            "Task",
            "Job",
            "JobContext",
            "TaskResult",
            "FileSpec",
            "StateSnapshot",
            "TaskLiteError",
            "make_job_uid",
            "parse_job_uid",
        }
    )

    def test_package_version(self):
        import tasklite

        assert tasklite.__version__ == "3.0.0.dev0"

    def test_export_names_in_all(self):
        import tasklite as tasklite_pkg

        assert set(tasklite_pkg.__all__) == self._CORE_SYMBOLS, (
            f"主公开面 __all__ 契约不一致: 差异 {set(tasklite_pkg.__all__) ^ self._CORE_SYMBOLS}"
        )

    def test_export_names_importable(self):
        from tasklite import (
            FileSpec,
            Job,
            JobContext,
            StateSnapshot,
            Task,
            TaskLite,
            TaskLiteError,
            TaskResult,
            make_job_uid,
            parse_job_uid,
        )

        assert isinstance(TaskLite, type)
        assert isinstance(Task, type)
        assert isinstance(Job, type)
        assert isinstance(JobContext, type)
        assert isinstance(TaskResult, type)
        assert isinstance(FileSpec, type)
        assert isinstance(StateSnapshot, type)
        assert issubclass(TaskLiteError, Exception)
        assert callable(make_job_uid)
        assert callable(parse_job_uid)

    def test_submodules_exports(self):
        from tasklite import resource, selection

        assert resource.__all__ == ["CapacityResource", "IntervalResource"]
        assert selection.__all__ == ["Exclude"]
        assert hasattr(resource, "CapacityResource")
        assert hasattr(resource, "IntervalResource")
        assert not hasattr(resource, "Resource")
        assert hasattr(selection, "Exclude")

    def test_non_core_and_deleted_symbols_not_in_root(self):
        import tasklite as tasklite_pkg

        banned_from_root = [
            "CapacityResource",
            "IntervalResource",
            "Resource",
            "Exclude",
            "Selection",
            "Skip",
            "RateLimitResource",
            "AttemptRecord",
            "FailureEntry",
            "SuspendEntry",
            "ImmediateRequeuePolicy",
            "RequeuePlan",
            "RequeuePolicy",
            "TriggerRegistry",
            "TriggerContext",
            "FifoOrderingPolicy",
            "OrderingPolicy",
            "AttemptOutcome",
            "ExitReason",
            "StopMode",
            "InMemoryStateBackend",
            "SQLiteStateBackend",
            "RunSummary",
            "RunStats",
            "ErrorClassifier",
            "ErrorCategory",
            "job_ref",
            "slice_list",
            "progress_hook",
            "jsonutil",
            "percent_encode",
            "encode_identifier",
            "encode_job_component",
            "encode_content_id",
            "content_fingerprint",
        ]
        for name in banned_from_root:
            assert name not in tasklite_pkg.__all__, f"{name} 不应在 tasklite.__all__ 中"
            assert not hasattr(tasklite_pkg, name), f"{name} 不应在 tasklite 根包中直接导出"

    def test_tasklite_facade_no_duplicate_query_or_deleted_methods(self):
        from tasklite import TaskLite

        assert not hasattr(TaskLite, "backend")
        deleted_methods = [
            "list_failures",
            "list_suspensions",
            "filter_jobs_without_success",
            "run_graceful",
            "retry_failure",
            "seed_successes",
            "clear_failures",
            "seed_cursor",
            "progress_hook",
            "stats",
            "on_run_start",
            "on_attempt_finished",
            "on_run_end",
        ]
        for method_name in deleted_methods:
            assert not hasattr(TaskLite, method_name), f"TaskLite 不应暴露已删除方法 {method_name}"

    def test_tasklite_internal_attributes_not_exposed(self):
        from tasklite import TaskLite

        tl = TaskLite(backend="memory")
        try:
            for attr in (
                "backend_type",
                "tasks",
                "resources",
                "runtime_config",
                "state_path",
                "base_dir",
                "output_roots",
                "authorizer",
            ):
                assert not hasattr(tl, attr), f"TaskLite 不应暴露内部属性 {attr}"
        finally:
            tl.close()

    def test_job_context_internal_attributes_not_exposed(self):
        """JobContext 仅暴露受控交互方法，不向外部暴露累积容器与内部路径配置属性。"""
        from tasklite import Job, JobContext

        ctx = JobContext(Job("t", "1"), frozenset(), frozenset(), {})
        for attr in ("spawned_jobs", "cursor_updates", "base_dir", "output_roots"):
            assert not hasattr(ctx, attr), f"JobContext 不应暴露内部属性 {attr}"

    def test_tasklite_facade_signatures(self):
        from tasklite import StateSnapshot, TaskLite

        init_params = inspect.signature(TaskLite).parameters
        assert list(init_params) == [
            "backend",
            "state_path",
            "max_workers",
        ]
        assert all(
            param.kind is inspect.Parameter.KEYWORD_ONLY
            for param in init_params.values()
        )
        assert init_params["backend"].default is inspect.Parameter.empty

        clear_params = inspect.signature(TaskLite.clear_history).parameters
        assert clear_params["records"].kind is inspect.Parameter.KEYWORD_ONLY

        status_params = inspect.signature(TaskLite.set_job_status).parameters
        assert status_params["recursive"].kind is inspect.Parameter.KEYWORD_ONLY
        assert status_params["recursive"].default is inspect.Parameter.empty

        grant_params = inspect.signature(TaskLite.grant_authorization).parameters
        assert list(grant_params) == ["self", "path", "target_type"]
        assert grant_params["target_type"].kind is inspect.Parameter.KEYWORD_ONLY
        assert grant_params["target_type"].default is inspect.Parameter.empty

        open_params = inspect.signature(StateSnapshot.open).parameters
        assert list(open_params) == ["source", "mode"]
        assert open_params["mode"].kind is inspect.Parameter.KEYWORD_ONLY
        assert open_params["mode"].default == "lazy"

        report_params = inspect.signature(StateSnapshot.report).parameters
        assert list(report_params) == ["first", "second"]
        assert report_params["second"].default is None


class TestGuideImportContract:
    """GUIDE.md 代码块中的 tasklite 导入语句逐条可导入。"""

    def test_guide_tasklite_imports_resolve(self):
        doc = (PROJECT_ROOT / "docs" / "GUIDE.md").read_text(encoding="utf-8")
        blocks = re.findall(r"```python\n(.+?)```", doc, flags=re.DOTALL)
        assert blocks, "GUIDE.md 未捕获 python 代码块，围栏标记已漂移"
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
        assert stmts, "GUIDE.md 代码块未捕获任何 tasklite 导入语句"
        for stmt in stmts:
            exec(compile(stmt, "<GUIDE.md>", "exec"), {})


class TestMainSurfaceIsolation:
    """import tasklite 不触发 wrappers/contrib 加载（子进程净 env 验证）。"""

    def test_main_import_skips_wrappers_and_contrib(self):
        code = (
            "import sys; import tasklite; "
            "leaked = [m for m in sys.modules "
            "if m.startswith(('tasklite.wrappers', 'tasklite.contrib'))]; "
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
            f"import tasklite 不应加载 wrappers/contrib: {result.stderr}"
        )

    def test_wrappers_subpackage_importable_standalone(self):
        code = (
            "import tasklite.wrappers as w; "
            "assert w.__all__ == ['discovery', 'http']; "
            "from tasklite.wrappers.discovery import register_discovery; "
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


class TestPackagingMetadata:
    """pyproject 元数据与支持面承诺一致性。"""

    def test_classifiers_declare_full_interpreter_support(self):
        classifiers = _load_pyproject()["project"]["classifiers"]
        declared = {
            c.rsplit(" ", 1)[-1]
            for c in classifiers
            if c.startswith("Programming Language :: Python :: 3.")
        }
        assert set(SUPPORTED_PYTHONS) <= declared

    def test_license_expression_excludes_license_classifier(self):
        project = _load_pyproject()["project"]
        assert project["license"] == "MIT"
        license_classifiers = [c for c in project["classifiers"] if c.startswith("License ::")]
        assert license_classifiers == []

    def test_runtime_version_matches_naming_contract(self):
        import tasklite

        assert tasklite.__version__ == "3.0.0.dev0"
        assert _load_pyproject()["tool"]["setuptools"]["dynamic"]["version"] == {
            "attr": "tasklite.__version__"
        }


def test_no_banned_vocabulary_in_adapter_layer():
    """适配层（wrappers/contrib）禁用词零出现。"""
    banned = ["sani" + "tize", "tax" + "onomy", "D" + "LQ", "back" + "off"]
    for path in ADAPTER_FILES:
        content = path.read_text(encoding="utf-8").lower()
        for word in banned:
            assert word.lower() not in content, (
                f"{path.name} 含禁用词碎片 {word!r}"
            )
