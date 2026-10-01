"""v2 引擎层：调度 seam 与机器群。

本包不设再导出面——``tasklite/v2/__init__.py`` 是唯一承诺稳定的导入面，
子模块一律经 ``tasklite.v2.engine.<模块>`` 全路径导入。装配结构：

- 值对象与配置：types（引擎值对象/枚举）、config（RunConfig 装配快照）、
  errorclass（错误分类）、admission（rerun 准入与 RequeuePolicy 重入队策略）；
- 调度与资源：scheduler（只读扫描选择点，OrderingPolicy seam）、
  resource（资源体系与两阶段租约）、wait（等待决策）；
- 机器群：dispatch（五关预检派发）、channel（子进程执行通道）、
  in_flight（在飞追踪）、completion（完成机器）、recovery（恢复编排）、
  store（状态仓库）、governor（死锁治理）；
- 运行期：session（run 生命周期）、runtime（EngineRuntime 事件泵）、
  ops（OpsConsole 运维控制台）。

调度逻辑仅留 seam：选择点收敛在 scheduler 的 scan_next_runnable（访问
序经 OrderingPolicy），重试节奏收敛在 RequeuePolicy（默认立即重入队）。

分层红线：本包属核心层，严禁 import ``v2/wrappers/`` 与 ``v2/contrib/``，
亦不得依赖 v1 旧树任何模块；可依赖 ``v2/{models,utils,backend}``。
"""
