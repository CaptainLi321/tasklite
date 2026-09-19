"""Engine subpackage for tasklite.

核心执行与深模块架构：
- ``types``: 引擎公共值对象单一真相源（StepOutcome / RunSummary / TaskStats / StopMode 等）；
- ``config``: RunConfig 静态装配快照与 resolve_tuning 默认值唯一解析点；
- ``runtime``: 核心运行期深模块与主循环事件泵 (EngineRuntime)；
- ``session``: 单次 run 生命周期状态与钩子单一出口 (RunSession)；
- ``pacing``: 等待/空闲决策纯函数 (decide_wait / LoopFacts)；
- ``store``: 统一状态事务、3-strike 崩溃与死锁归因深模块 (StateStore)；
- ``dispatch``: 派发预检关与子进程派发编排 (DispatchMachine)；
- ``completion``: 结果提交、清理、释放与恢复收尾 (CompletionMachine)；
- ``recovery``: 崩溃恢复、TOCTOU 闭环 abort 与信号排空 (RecoveryOrchestrator)；
- ``channel``: 子进程生命周期、阶梯看门狗、IPC 通道与产物清理 (ExecutionChannel)；
- ``scheduler``: 队列只读扫描与不可变投影缓存 (JobScheduler)；
- ``resource``: 限速与容量资源抽象与挂起语义 (ResourceManager / Resource)；
- ``governor``: 死锁归因与依赖宽限治理 (DeadlockGovernor)；
- ``policy``: Rerun 决策、指纹缓存与退避状态机 (ExecutionPolicy)；
- ``inflight``: 在途作业生命周期跟踪与资源租约 (InFlightTracker)；
- ``console``: run() 外管理 API 运维接缝 (OpsConsole)。
"""
