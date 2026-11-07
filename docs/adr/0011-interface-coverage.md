# ADR-0011 公开 Interface 覆盖审计

状态：**盘点完成，逐项裁决未完成**。导出证据的代码基线为 `57b50b7`，覆盖状态随已接受设计更新。本文是设计工作清单，不是已发布手册，也不新增兼容承诺；已接受行为以 [ADR-0011](0011-stable-interface-baseline.md) 为准，实施顺序见 [ROADMAP](../ROADMAP.md)。不修改当前 GUIDE 来提前宣布尚未实现的接口。

## 范围与方法

核对根包与子模块的显式导出、TaskLite 方法和构造参数、JobContext、模型、资源与调度协议、后端、wrappers、hooks 和 testing。使用源码静态读取，不执行下游业务、不访问业务数据库。显式导出意味着需要裁决，不意味着最终必须承诺稳定；内部文件的 __all__ 也不能单独证明它是用户扩展协议。

根包当前显式导出 73 个名称；此外 HTTP 显式导出 13 个名称，Discovery 导出 6 个名称，testing 导出 2 个名称。计数按各导入面分别计算，存在重复再导出，不能相加作为独立能力数。覆盖清单包括现有入口与 ADR 新增入口；最终记录字段、默认值、异常类型尚未逐字段冻结。

状态含义：**已确认方向**表示 ADR 已裁决主要行为，仍可能缺签名；**部分覆盖**表示讨论过部分行为但没有闭合契约；**待系统分析**表示本轮尚未逐项分析。后两者不是代码缺陷结论。

## 功能覆盖矩阵

| 功能 | 源码入口 | 本轮覆盖 | 冻结前要回答的问题 |
| --- | --- | --- | --- |
| 初始化与生命周期 | TaskLite.__init__/run/stop/is_running；目标 close | 部分覆盖 | 参数默认值与位置参数；初始化副作用及失败后资源释放；重复运行、关闭；多线程调用边界 |
| 注册与模型 | register_task、Task、Job、TaskRegistry | 部分覆盖 | 重复注册禁止、可确定的提交错误立即拒绝已确认；具体错误类型及配置继承、payload_schema 校验、None 哨兵及 JSON 类型；快照字段与业务构造字段分离 |
| 提交与替换 | enqueue、ctx.spawn、filter_jobs_without_success | 部分覆盖 | 单条返回值、front 去留、显式替换；内容比较字段；过滤辅助方法是否保留；派生冲突如何导致父失败 |
| 执行与重试 | TaskResult、Attempt、Job 四态 | 已确认方向 | Attempt 编号作用域；新结果类型的最终签名、记录结构与框架原因码 |
| 依赖 | Job.depends_on、具名上游产物读取、governor | 部分覆盖 | 缺失与循环依赖、晚到生产者、重复 UID；历史终态与活跃候选优先级的完整矩阵；停止、取消、授权排除的交叉 |
| 启动规则 | 内置 Trigger；现有 register_trigger/TriggerContext | 部分覆盖 | 自定义扩展不冻结；输入视图与校验时点、人工成功和档案清理对内置规则的影响待处置 |
| 选择与排序 | Selection/Exclude/Skip、OrderingPolicy | 部分覆盖 | Skip 去留与取消的关系；选择粒度；已确认按序选最靠前可执行者、不队首阻塞；公开扩展协议暂不冻结，内部排序合法性由实现验证 |
| 重入队节奏 | RequeuePolicy、RequeuePlan、ImmediateRequeuePolicy | 扩展暂不承诺兼容 | 保留必要内置行为，协议和内部调用由实现收敛，不作为公开扩展定稿门禁 |
| 资源 | register_resource、Resource、CapacityResource、RateLimitResource | 部分覆盖 | 已确认主线程唯一入口、连续检查获取、多 worker 共用、查询三态、状态自管、通知且不轮询；只提供官方内置资源；三态类型与通知签名属于内部实现，不设计对外资源扩展 |
| 资源挂起 | ctx.suspend_resource、list_suspensions；曾提议 resume_resource | 已确认删除 | 删除显式挂起接口、查询与持久化/恢复，不新增结果控制回调；资源可用性通知不属于挂起 API |
| 文件管理 | FileSpec/from_output、declare_file/file_path | 已确认契约 | role 三内置值、清理默认否、Path 返回、上游显式依赖统一读取、允许路径重叠、不自动创建、删除缺失目标视为完成；无文件扩展或 handler 沙箱 |
| handler 读状态 | has_succeeded/has_failed/succeeded_or_failed_job_uids | 待系统分析 | 派发快照还是实时视图；人工成功、活跃候选遮蔽；是否暴露完整集合；与 StateSnapshot 保持语义一致 |
| 游标 | ctx.get_cursor/set_cursor、seed_cursor | 部分覆盖 | 键空间、值类型、None 删除、同键多任务成功提交；本地读自己未提交写入；预填是覆盖还是仅不存在时写入 |
| 查询 | StateSnapshot、现有 list_failures/list_suspensions | 部分覆盖 | 原门面查询去留；记录形状、缺失值、过滤分页；授权查询；快照关闭后的访问边界 |
| 管理与历史 | retry_failure、clear_failures、clear_history、seed_successes；目标取消 | 部分覆盖 | 返回值和冲突矩阵；清除终态是否保留 Attempt；人工成功与队列冲突；删除历史对 Trigger、依赖和 Discovery 的影响 |
| 事件与统计 | on_run_start/on_attempt_finished/on_run_end、RunSummary/RunStats | 公开回调已确认删除；摘要待收口 | 删除三个回调的构造参数和属性，不提供替代订阅；剩余统计累计范围、级联与恢复计数、ExitReason 成员待明确 |
| 错误与验证 | exceptions、ErrorClassifier、ERR_*、ValidationResult | 部分覆盖 | 自动异常重试已撤销后哪些符号删除；框架错误公开类型；原因码与消息边界；payload 校验是否保留独立工具 |
| 进程执行条件 | strict_picklable、handler、worker 启动与超时 | 部分覆盖 | 支持的 callable/payload/平台组合及验证时点；已确认启动至有效结果的单次执行超时、不等待进程退出才结算；信号安装选项；外部程序进程树边界，不承诺 OS 沙箱 |
| 后端 | AbstractStateBackend、SQLiteStateBackend、InMemoryStateBackend、backend 属性 | 部分覆盖 | 仅官方 SQLite/内存使用接口稳定，第三方协议不冻结；注入、所有权及关闭、内存模式与 StateSnapshot 接法待定 |
| Discovery | register_discovery、DiscoveryHandler 及三协议 | 可选工具，暂不承诺兼容 | 全页命中、截断、坏项、on_missing；人工成功/取消失败/档案清除如何影响已见；多子任务整体接受后的行为 |
| HTTP | HttpPolicy/HttpExecutor/guard 系列、响应、缓存、fetch | 可选工具，暂不承诺兼容 | 不靠异常隐式重试的易用接法；请求内部重试与 Job 重试关系；删除自动资源挂起；响应缓存的身份/有效性；是否保留多个包装入口 |
| 测试与辅助函数 | fake_ctx/running、job_ref/progress_hook/slice_list | 可选工具，暂不承诺兼容 | 当前旧文档的稳定承诺在新基线切换时同步调整；删除 progress_hook；模拟授权往返和副作用；避免依赖内部结构 |
| 标识与序列化工具 | encode_*、content_fingerprint、percent_encode、jsonutil；目标 make_job_uid/parse_job_uid | 部分覆盖 | 已确认透明可逆身份接口及取消文件名约束；本轮允许破坏性简化，新基线输出稳定；旧工具逐项去留与 JSON 契约待定 |
| 版本与文档 | 包导出、GUIDE、Schema/发布窗口 | 部分覆盖 | 新基线版本、记录格式支持窗口、弃用规则、官方子模块导入路径、每个公开接口的类型提示与文档 |

## 当前 TaskLite 面逐项登记

| 现有入口 | 已接受的目标或待裁决事项 |
| --- | --- |
| __init__ | 三个 hook 参数已确认删除；name/state_dir/backend/output_roots/max_workers、pickle、异常分类、策略及三个调优参数，继续逐项核定保留与默认值 |
| is_running | 可观察性已有，关闭与跨线程语义待固定 |
| backend 属性及 setter | 是否允许外部替换及何时替换，待裁决；不能靠可访问推定稳定 |
| stats、on_run_start、on_attempt_finished、on_run_end 属性 | 三个回调属性删除；stats 是否公开及形态待裁决 |
| register_resource、register_task、register_trigger | 能力保留，完整签名和校验契约待补 |
| register_transient_exception | 已撤销异常自动映射重试；不保留同语义的注册入口 |
| selection | 改为 run(selection=...)，不保留粘滞选择状态 |
| filter_jobs_without_success | 待裁决，需明确人工成功与活跃候选语义 |
| enqueue | 改为单 Job；返回值、front 与显式替换待裁决 |
| list_suspensions、list_failures | list_suspensions 删除；StateSnapshot 已确立，list_failures 门面便捷读取是否保留待裁决 |
| clear_failures、clear_history | 待系统分析；keep_fatal 与新错误模型的关系尤其需裁决 |
| retry_failure | 原子恢复完整失败快照并移除失败档案；精确返回值与异常类型待定 |
| seed_successes | 人工成功能力保留，仅 UID 的旧形式取消；完整 Job 新入口签名待定 |
| seed_cursor | 保留与否、覆盖规则及返回值待定 |
| stop | 幂等停止新派发，无 force；不阻塞等待在途，由 run 收尾 |
| run | 同步返回摘要或抛框架异常；selection、信号配置及其余最终参数待定，不公开绕过库级互斥 |
| run_graceful | 删除 |
| 当前构造器暴露的 tasks/resources/classifier/runtime_config 等属性 | 内部可变状态不成为稳定 Interface；仍需逐名迁移/删除清单 |

ADR 新增而当前尚未实现的 make_job_uid、parse_job_uid、TaskResult、FileSpec、StateSnapshot、close、取消、路径授权查询/撤销也必须进入最终签名表。不能只盘点旧方法而漏掉新承诺。

## 已确认的兼容范围

1. **常用 Interface**：编排、规格与实例、handler 上下文及结果、文件声明、只读记录、明确的管理操作，纳入稳定基线。
2. **本版收缩范围**：不设计或冻结资源扩展，不提供文件扩展，调度扩展暂不承诺兼容、不作为定稿门禁。内部保留抽象和 Seam；常用内置能力仍需明确行为。只承诺官方 SQLite／内存后端使用接口，不冻结第三方后端协议。
3. **可选工具**：HTTP、Discovery、测试辅助等本版本暂不保证兼容，文档必须标注。需要适配新核心，但不把这些工具的全面接口冻结作为本轮门禁。
4. **身份工具例外**：make_job_uid／parse_job_uid 与 Job.uid 纳入稳定接口，本轮先完成可读性简化。旧编码工具逐个核查用途；纯文件 IPC／逐 Job 锁用途移除，不将文件名字符或长度规则带进公开身份。
5. **内部 Implementation**：运行账目、注册表装配、调度状态机、底层事务校验器、可变容器不自动承诺稳定；仍需逐名整理导出清单。

## 已收束的生命周期决定

收到完整有效结果即开始结算，不等待 worker 退出，不将 failure/retry 或无结果退出当作成功。正常收尾采用校验、清理、资源释放、原子提交、完成事件的顺序，期间不插入新派发。执行超时从 worker 启动计至有效结果到达；正常停止排空，框架扩展异常直接使 Run 失败。

资源仅由主线程唯一入口管理，同一资源可以供多个 worker 使用；不引入跨执行器竞争、try_acquire 或预留协议。worker 逐 Job 锁已决定删除，保留状态库级执行／管理互斥。上述是已接受决定，不再作为待选设计反复讨论。

## 讨论与验收顺序

1. 完成导出分层和保留/调整/删除清单；每项落实唯一导入路径和承诺级别；不为已撤出本版的扩展接口继续冻结签名。
2. 注册、初始化与校验：用重复注册、未知 Task、坏 payload、显式覆盖、初始化失败五类场景验证。
3. 内置依赖、资源及调度行为：覆盖无解依赖/资源、扩展异常直接失败、正常停止排空、重试节奏及定时/主动通知唤醒。
4. handler 读取、游标、查询和管理：使用同 UID 活跃候选/旧档案、人工成功、清除档案、并发游标提交验证。
5. 官方后端完善使用接口；Discovery、HTTP、测试工具完成新核心适配，不要求可选工具本轮接口冻结，也不引入第二套隐式结果协议。
6. 汇总最终签名表：导入路径、参数/默认值、返回值、错误、允许调用阶段、副作用、持久化/恢复含义及最小验收场景。最后再做跨功能审查。

Discovery 当前明确采用成功/失败快照作为已见集合，不维护扫描游标。应分析它与档案管理的耦合，而不是为它补造游标推进机制。HTTP 响应缓存与 FileSpec.cache 也不能仅因同名就假定相同生命周期。

## 显式导出证据

以下是源码基线的登记，不是建议最终原样保留。根包列表完整覆盖其 73 个名称；子模块列表用于识别根包以外的候选承诺。内部模块即使有 __all__ 仍须按使用手册、协议和调用目的判断是否属于外部 Seam。

### tasklite/__init__.py

`TaskLite`, `Exclude`, `Selection`, `Skip`, `AttemptRecord`, `Job`, `JobContext`, `TriggerContext`, `JobRuntimeState`, `Task`, `TaskRegistry`, `validate_resource_amounts`, `FatalError`, `PipelineError`, `RateLimitHit`, `RetryError`, `job_ref`, `progress_hook`, `slice_list`, `ErrorCategory`, `ErrorClassification`, `ErrorClassifier`, `ValidationErrorItem`, `ValidationResult`, `classify_error_category`, `classify_exception`, `is_transient_exception`, `validate_payload_errors`, `ERR_DEPENDENCY_DEADLOCK`, `ERR_JOB_DEPENDENCY`, `ERR_PAYLOAD_VALIDATION`, `ERR_MAX_RETRIES`, `ERR_NO_HANDLER`, `ERR_RESOURCE_DEADLOCK`, `ERR_MALFORMED_JOB`, `ERR_COMMIT_FAILURE`, `ERR_DISPATCH_FAILURE`, `ERR_DEADLOCK_GAP`, `ERR_DEPENDENCY_EXCLUDED`, `ERR_DEPENDENCY_SKIPPED`, `ERR_SKIPPED`, `ERR_TIMEOUT_PREFIX`, `ERR_PROCESS_CRASH_PREFIX`, `ERR_NO_IPC_RESULT`, `ERR_PROCESS_SIGNAL_DEATH`, `ERR_IPC_WRITE_DEGRADED_PREFIX`, `FATAL_EXCEPTIONS`, `TRANSIENT_EXCEPTIONS`, `CapacityResource`, `RateLimitResource`, `Resource`, `FailureEntry`, `SuspendEntry`, `ImmediateRequeuePolicy`, `RequeuePlan`, `RequeuePolicy`, `TriggerRegistry`, `FifoOrderingPolicy`, `OrderingPolicy`, `AttemptOutcome`, `ExitReason`, `RunSummary`, `StopMode`, `RunStats`, `InMemoryStateBackend`, `SQLiteStateBackend`, `content_fingerprint`, `encode_content_id`, `encode_identifier`, `encode_job_component`, `percent_encode`, `encode_job_uid_filename`, `jsonutil`。

### tasklite/wrappers/http.py

`parse_netscape_cookies`, `format_cookie_header`, `HttpResponse`, `HttpPolicy`, `http_guard`, `guard_request`, `guarded`, `HttpExecutor`, `SnapshotStore`, `SQLiteSnapshotStore`, `MemorySnapshotStore`, `urllib_fetch`, `requests_fetch`。

### tasklite/wrappers/discovery.py

`DiscoveryContext`, `DiscoveryHandler`, `DiscoveryHost`, `DiscoveryJob`, `encode_content_id`, `register_discovery`。

### tasklite/testing.py

`fake_ctx`, `running`。

### tasklite/backend/__init__.py

`AbstractStateBackend`, `InMemoryStateBackend`, `SQLiteStateBackend`, `validate_attempt_dispatch`, `validate_attempt_finish`, `validate_queue_replacement`。

### tasklite/selection.py

`Exclude`, `Rule`, `Selection`, `Skip`。

### tasklite/hooks.py

`job_ref`, `progress_hook`, `slice_list`。

### tasklite/utils/encoding.py

`CONTENT_ID_ALLOWED`, `DEFAULT_FORBIDDEN`, `EMPTY_SENTINEL`, `FS_ESCAPE_CHARS`, `content_fingerprint`, `encode_content_id`, `encode_identifier`, `encode_job_component`, `percent_encode`, `encode_job_uid_filename`。

### tasklite/utils/jsonutil.py

`dump`, `dumps`, `load`, `loads`。
