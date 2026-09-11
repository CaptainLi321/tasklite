# 更新日志

本文件记录 TaskLite（PyPI 包名 `tasklite-engine`）的所有显著变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

暂无。

## [1.2.0] - 2026-09-10

架构深化版本：引擎全面完成深模块收敛，移除全部内部弃用面，并新增官方 HTTP 网络工具库。

### 新增

- 新增组合式轻量 HTTP 网络工具库 `tasklite.wrappers.http`：`http_guard` / `HttpPolicy` / `guard_request` / `guarded_fetch` 将 HTTP 状态码与传输异常收敛为引擎三分类异常（429 → `RateLimitHit`、5xx/超时 → `RetryError`、4xx → `FatalError`），429 时自动解析 `Retry-After` 并联动 `ctx.suspend_resource` 全管线退避，不烧重试预算（设计裁决见 ADR-0001）。
- 新增快照存储 `SnapshotStore` 族（`SQLiteSnapshotStore` / `MemorySnapshotStore`）：单射请求键（Query 排序、Method 归一、Body 摘要）持久化原始 HTTP 响应，支持全离线幂等重放；429 与 5xx 瞬态响应默认不入快照。
- 新增内置请求实现 `fetch_urllib`（零三方依赖）与 `fetch_requests`（软依赖 `requests`），以及 Netscape `cookies.txt` 解析工具 `parse_netscape_cookies` / `format_cookie_header` 与不可变容器 `HttpResponse`。
- 新增架构决策记录：ADR-0001（组合式 HTTP 工具与正交快照守卫设计）、ADR-0002（运行期终态架构防漂移基线）。
- 新增 `RunConfig` 静态装配快照：`resolve()` / `resolve_tuning()` 成为全部调优参数默认值的唯一解析点，`TaskLite` 门面仅透传 `Optional` 原始参数。
- 新增 `RunSession` 单次 `run()` 生命周期会话：停机状态机转移唯一入口、`fire_*` 运行钩子单一出口、`exit_reason` 唯一推导实现。
- 新增等待决策纯函数 `pacing.decide_wait`（输入 `LoopFacts` 现场快照，输出 `WaitDecision`），主循环轮询间隔与受控等待收敛于唯一实现。
- 新增 `OpsConsole` 运维接缝：`list_dlq` / `clear_dlq` / `clear_history` / `seed_wall` / `seed_cursor` 等管理 API 的内部统一委托目标。
- 新增 `WorkerLaunchSpec` 进程边界具名契约（frozen 值对象，含 `timeout`），取代子进程 spawn 的位置参数约定。
- 新增 `DeadlockGovernor` 死锁仲裁深模块：畸形数据、未知/超限资源、缺失依赖宽限与保守兜底四类归因统一裁决。
- 新增 `ExecutionPolicy` 深模块：准入预检、重试退避调度与跨会话重跑（rerun）决策统一治理。
- 新增 `StateStore` 深模块：入队规范化、wall 去重、六个 `apply_*` 状态转移唯一出口族、3-strike 提交失败契约与死锁批量熔断统一收敛。
- 新增 `ExecutionChannel` 深模块：子进程生命周期、IPC 协议与产物清理完全内敛。
- 新增 `ArtifactJournal` 深模块（`utils/ipc.py`）：产物清单、两级降级落盘与崩溃残留结果认领统一收敛。
- 新增 `ErrorTaxonomy` 深模块（`tasklite/taxonomy.py`）：错误三分类、瞬态异常注册与入口校验的全引擎单一真相。
- 新增 `ResourceManager` / `InFlightTracker` 深模块与 `ResourceLease` / `NullResourceLease` 两阶段资源租约。
- 新增 `engine/types.py` 引擎公共值对象单一真相源（`StopMode` / `ExitReason` / `TaskStats` / `StepOutcome` / `RunSummary` / `ExecutionOptions` / `HandlerEntry`），并激活 `JobRuntimeState` 强类型运行态值对象。
- 新增 `EngineRuntime` 深模块：引擎装配、主循环事件泵与异常承重网统一收敛；门面提供统一的 `is_running` 运行态属性。
- 装配与入队 API 增加 run 期守卫：`add_resource` / `register_handler` / `register_discovery` / `register_transient_exception` / `enqueue` 及全部管理 API 在 `run()` 进行中调用一律抛 `RuntimeError`，原「装配仅限 run() 之外」的文档约定升级为代码强制。
- 用户工具函数（`job_ref` / `progress_hook` / `slice_list`）迁至 `tasklite/hooks.py`，顶层 `from tasklite import ...` 导出面保持不变。
- 增强多解释器（Python 3.9–3.14）测试矩阵，并新增端到端多进程验证脚本。

### 变更

- `deadlock_gap_max_rounds` 生效默认值由 3 调整为 5（收敛门面字面量与 governor 常量的双默认值冲突，以常量为唯一真相源；依赖旧行为的管线请显式传 `deadlock_gap_max_rounds=3`）。
- 主循环等待聚合语义：资源挂起恢复时刻 `worker_wait` 由「后写覆盖」改为取最早时刻（min），多次挂起叠加时按最先解封时间恢复调度。
- `exit_reason` 推导语义统一：`KeyboardInterrupt` 优先判定为 `interrupted`（即使信号处理器已请求优雅停机），其余异常判定为 `error`，无异常时按停机模式三态。
- 引擎架构全面深模块化：派发、结算、恢复三台机器改为显式窄依赖构造，消除浅层转发垫片、Context 整袋穿透与双重集合记账。
- 死锁治理移除阻塞 `sleep`，仲裁收敛为纯值对象裁决。
- 主循环事件泵 `step()` 结构性瘦身至 58 行（行为零变更）。

### 移除

- 移除 `RunContext` 全局可变上下文袋，职责拆分为 `RunConfig`（静态装配快照）与 `RunSession`（生命周期会话）。
- 移除 `TaskLite` 门面六个内部深模块只读属性：`store` / `scheduler` / `governor` / `channel` / `state` / `in_flight`（零外部消费方，未经历弃用窗口直接移除）；运维操作请经管理 API 完成。
- 移除生命周期钩子 setter：钩子仅可经构造期参数传入，属性收敛为只读。
- 移除 `ExecutionChannel` 四组历史方法别名：`submit` / `poll_completed` / `consume_stale_result` / `cleanup`，统一为主名 `spawn` / `reap_completed` / `claim_stale_result` / `cleanup_in_flight`。
- 移除 `TaskContext.incarnation` 字段：子进程执行身份改由 `WorkerLaunchSpec` 携带，不再借道 TaskContext 穿透进程边界。
- 移除 `tasklite.pipeline_util` 模块：能力分别收敛至宿主门面与各深模块。
- 移除 `FailureMachine`：状态转移与 3-strike 事务统合入 `StateStore`。
- 清算单实现假想接缝：`DispatchView` / `CommitView` / `RecoveryView` / `ExecutionChannelProtocol` 及四个单实现 Protocol 全部删除。
- 移除 `error_codes` / `validation` 浅模块：错误码登记与输入校验收敛至 `ErrorTaxonomy`。
- 移除 `ScheduleResult` 遗留整型索引数组：死锁归因收敛至不可变 `DeadlockAttribution` 值对象，消除调度器与主循环间的下标时序耦合。
- 移除 `runtime.RuntimeConfig` 等零消费 re-export 垫片。

### 修复

- 修复 `sanitize_identifier` 超长截断时破坏完整百分号转义序列的问题，保证单射可逆、杜绝标识碰撞。
- 修复 `Job` 构造函数对 `resources` 字典浅拷贝导致的共享可变状态问题。
- 修复 `InMemoryStateBackend` 死信队列错误分类丢失问题。
- 修复 `CapacityResource` 挂起协议返回值不对齐问题。
- 补齐 Python 3.9 兼容性：`__future__ annotations`、前向导入与 `Job` 显式导入；统一注解并修复资源泄漏隐患。
- 修复 `enqueue()` 内部路径误触 `DeprecationWarning` 的问题（内部改走 `_runtime.store` 直达路径）。

## [1.1.0] - 2026-08-31

### 新增

- 新增 `InMemoryStateBackend` 纯内存状态后端（零 IO、快照隔离，测试专用），持久化抽象形成 SQLite / 内存双实现。
- 新增 `tasklite.utils.injective` 单射转义模块：统一可逆 `%XX` 百分号转义与安全标识派生，锁文件、Discovery content_id 净化与管线脚手架切换至同一实现。
- 新增 `CONTEXT.md` 领域模型总纲与核心术语表。

### 变更

- 死锁拆分逻辑内敛至 `FailureMachine._split_deadlock`，移除独立的 `engine/deadlock.py`。
- 深化 `TaskLite` 宿主接口并精简 `pipeline_util` 脚手架。

## [1.0.1] - 2026-08-31

维护性发布：仅校准包版本号，无代码行为与文档内容变更。

## [1.0.0] - 2026-08-30

### 新增

- 首次公开发布：零依赖（仅标准库 + SQLite）、进程隔离的轻量任务编排引擎，支持 Python 3.9+。
- 多进程物理隔离执行：每个任务在独立 `spawn` 子进程中运行，看门狗超时 `SIGKILL` 强杀，子进程崩溃或被终止不影响主调度进程。
- SQLite WAL 持久化（`synchronous=FULL`）：四集合状态机（wall / queue / in-flight / failed）单事务原子转移，断电不坏库，重启后断点续跑。
- 三层崩溃恢复防线：结果文件携带执行代标识隔离孤儿写入、启动期自动消费崩溃前已落盘的有效结果、孤儿文件锁探测保证同一 UID 全局唯一执行体。
- 函数式增量探索 Discovery：无游标去重、整页命中自动终止扫描，支持 `full` 扫描模式与源端缺失检测回调。
- 资源治理：令牌桶限速 `RateLimitResource` 与并发容量 `CapacityResource`；`ctx.suspend_resource` 全管线休眠且跨重启持久化。
- DAG 拓扑依赖：`depends_on` 声明依赖，父任务失败自动级联跳过下游；内置死锁归因与依赖宽限机制。
- 跨会话重跑策略：`rerun` 支持 `never` / `on_failure` / `every_run` / `on_input_change`（基于输入指纹变更检测）。
- 错误三分类与持久化死信队列（DLQ）：瞬态错误指数退避重试、致命错误直接归档，DLQ 条目带结构化 `error_type` 与 UTC 时间戳。
- 产物沙盒：`ctx.declare_output` / `declare_cache` / `declare_input`，路径遍历防御，任务失败自动清理半成品。
- 生命周期钩子 `on_run_start` / `on_run_end` / `on_job_completed` 与 `stats` 运行指标。
- 运维 API：`list_dlq` / `clear_dlq` / `clear_history` / `seed_wall` / `seed_cursor`。
- 优雅停机状态机：首次信号 DRAINING 停止派发并排空在途任务，二次信号 ABORTING 分类回收在途任务。

[Unreleased]: https://github.com/CaptainLi321/tasklite/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/CaptainLi321/tasklite/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/CaptainLi321/tasklite/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/CaptainLi321/tasklite/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/CaptainLi321/tasklite/releases/tag/v1.0.0
