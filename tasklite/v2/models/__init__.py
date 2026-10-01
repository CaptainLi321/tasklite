"""v2 模型层：Task 规格 / Job 逻辑实例 / Attempt 执行轨迹 / 管线状态与执行上下文。

本包不设再导出面——``tasklite/v2/__init__.py`` 是唯一承诺稳定的导入面，
子模块一律经 ``tasklite.v2.models.<模块>`` 全路径导入：
task（Task 规格与注册表）、job（Job 实例与运行期边带状态）、
attempt（append-only 执行轨迹）、state（PipelineState 六集合状态）、
context（JobContext 执行上下文）。

分层红线：本包严禁 import ``v2/engine/``（IPC 声明读写一律下沉
``v2/utils/ipc.py``），亦不得依赖 v1 旧树任何模块。
"""
