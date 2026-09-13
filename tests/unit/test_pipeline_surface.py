"""pipeline 模块命名空间卫生回归测试。

门面模块只应暴露真实使用的符号：死导入既污染
``from tasklite.pipeline import *`` 面，也掩盖真实依赖关系。
"""

import tasklite.pipeline


class TestPipelineModuleSurface:
    def test_no_dead_imports(self):
        """pickle/classify_error_type 在模块内零使用且无补丁锚点，不得再绑入命名空间。"""
        assert not hasattr(tasklite.pipeline, "pickle")
        assert not hasattr(tasklite.pipeline, "classify_error_type")

    def test_time_seam_preserved(self):
        """time 是既有测试的 monkeypatch 锚点（tasklite.pipeline.time.sleep），必须保留。"""
        assert hasattr(tasklite.pipeline, "time")
