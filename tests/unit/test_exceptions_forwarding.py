"""tasklite.exceptions PEP 562 动态转发面回归测试。

转发以 taxonomy 声明公共面（``tasklite.taxonomy.__all__``）为白名单：
历史从 tasklite.exceptions 导入 taxonomy 公共符号的调用方保持可用，
taxonomy 模块私有导入符号（typing/enum/datetime 等）不得经转发泄漏。
"""

import pytest

import tasklite.exceptions as exceptions_module
import tasklite.taxonomy as taxonomy_module


class TestExceptionsForwardingSurface:
    def test_legacy_taxonomy_symbols_still_forwarded(self):
        """既有调用方依赖的 taxonomy 公共符号经转发保持可导入。"""
        assert exceptions_module.FATAL_EXCEPTIONS is taxonomy_module.FATAL_EXCEPTIONS
        assert exceptions_module.TRANSIENT_EXCEPTIONS is taxonomy_module.TRANSIENT_EXCEPTIONS
        assert exceptions_module.TransientRegistry is taxonomy_module.TransientRegistry
        assert exceptions_module.ErrorTaxonomy is taxonomy_module.ErrorTaxonomy
        assert exceptions_module.classify_exception is taxonomy_module.classify_exception
        assert (
            exceptions_module.is_transient_exception
            is taxonomy_module.is_transient_exception
        )
        from tasklite.exceptions import FATAL_EXCEPTIONS, TransientRegistry  # noqa: F401

    def test_non_public_symbols_rejected(self):
        """taxonomy 顶层导入的 typing/enum/dataclass/datetime 私有符号不再经转发泄漏。"""
        for leaked in ("Union", "Enum", "dataclass", "field", "datetime", "Optional"):
            with pytest.raises(AttributeError):
                getattr(exceptions_module, leaked)

    def test_unknown_attribute_rejected(self):
        with pytest.raises(AttributeError, match="no attribute"):
            exceptions_module.definitely_not_a_symbol

    def test_forwarded_surface_equals_taxonomy_all(self):
        """转发面与 taxonomy 声明公共面严格一致（不多不少）。"""
        for name in taxonomy_module.__all__:
            assert getattr(exceptions_module, name) is getattr(taxonomy_module, name)
