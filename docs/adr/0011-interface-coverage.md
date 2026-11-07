# ADR-0011 公开 Interface 覆盖审计

状态：**核心行为已裁决，已有独立静态可行性核查结论，后续确认的模型签名／记录样本已同步，待实施验证**。导出证据的代码基线为 `57b50b7`，覆盖状态随已接受设计更新。本文是设计工作清单，不是已发布手册，也不新增兼容承诺；已接受行为以 [ADR-0011](0011-stable-interface-baseline.md) 为准，实施顺序见 [ROADMAP](../ROADMAP.md)。不修改当前 GUIDE 来提前宣布尚未实现的接口。

## 范围与方法

核对根包与子模块的显式导出、TaskLite 方法和构造参数、JobContext、模型、资源与调度协议、后端、wrappers、hooks 和 testing。使用源码静态读取，不执行下游业务、不访问业务数据库。显式导出意味着需要裁决，不意味着最终必须承诺稳定；内部文件的 __all__ 也不能单独证明它是用户扩展协议。

根包当前显式导出 73 个名称；此外 HTTP 显式导出 13 个名称，Discovery 导出 6 个名称，testing 导出 2 个名称。计数按各导入面分别计算，存在重复再导出，不能相加作为独立能力数。覆盖清单包括现有入口与 ADR 新增入口；目标模型签名、结果与查询记录样本见 ADR 正文；当前导出证据不代表目标保留名单。

状态含义：**已确认方向**表示 ADR 已裁决主要行为，仍可能缺签名；**部分覆盖**表示讨论过部分行为但没有闭合契约；**待系统分析**表示本轮尚未逐项分析。后两者不是代码缺陷结论。

## 功能覆盖矩阵

| 功能 | 源码入口 | 本轮覆盖 | 冻结前要回答的问题 |
| --- | --- | --- | --- |
| 初始化与生命周期 | TaskLite.__init__/run/stop/is_running/close | 已确认行为 | backend 显式选择、state_path 默认 None、max_workers=4；内置后端自主管理、构造失败释放资源；单控制线程，stop 可跨线程；独立静态核查未发现阻断，运行验证留待实施 |
| 注册与模型 | register_task、Task、Job、TaskRegistry | 已确认契约 | 身份/handler 可按位置传入，配置仅关键字；Job 独占 payload/depends_on/files；删除 payload_schema；标量 None 继承，默认 3/3600/once；handler(job, ctx) |
| 提交与替换 | enqueue、ctx.spawn | 已确认契约 | 单 Job bool 返回、同配置去重异配置冲突；删除 front/replace_pending；声明先于引用，派生随父成功整体提交 |
| 执行与重试 | TaskResult、Attempt、Job 四态 | 已确认方向 | attempt_no 按 Job 跨 Run 递增，删除 activation_no；三结果的 message/metadata 可选，仅 retry 有 consume_budget；结果字典样本已明确；框架原因仅说明与可得异常，不分类编码 |
| 依赖 | Job.depends_on、FileSpec.from_output、governor | 已确认契约 | 禁止悬空引用；spawn 按声明顺序；活跃上游遮蔽旧成功；缺失、失败或成环直接失败；删除宽限与潜在派生者猜测 |
| 启动规则 | 内置 Trigger；现有 register_trigger/TriggerContext | 已确认主要行为 | 跳过不作为成功证据；人工成功、档案清理及新 Run 重判按 ADR；扩展协议不冻结 |
| 选择与排序 | Selection/Exclude/Skip、OrderingPolicy | 已确认主要行为 | 删除 Selection/Skip；run(exclude=单个规则) 不接受列表；Exclude 按类型精确匹配，未来可扩展组合规则；排序独立，扩展协议暂不冻结 |
| 重入队节奏 | RequeuePolicy、RequeuePlan、ImmediateRequeuePolicy | 扩展暂不承诺兼容 | 保留必要内置行为，协议和内部调用由实现收敛，不作为公开扩展定稿门禁 |
| 资源 | register_resource、Resource、CapacityResource、IntervalResource | 已确认契约 | RateLimitResource 改名 IntervalResource，保留间隔乘用量算法；构造配置仅关键字、同名注册拒绝；主线程唯一入口、连续检查获取、多 worker 共用、查询三态、状态自管、通知且不轮询；只提供官方内置资源；三态类型与通知签名属于内部实现，不设计对外资源扩展 |
| 资源挂起 | ctx.suspend_resource、list_suspensions；曾提议 resume_resource | 已确认删除 | 删除显式挂起接口、查询与持久化/恢复，不新增结果控制回调；资源可用性通知不属于挂起 API |
| 文件管理 | FileSpec/from_output、declare_file/file_path | 已确认契约 | role 三内置值、清理默认否、Path 返回、上游显式依赖统一读取、允许路径重叠、不自动创建、删除缺失目标视为完成；无文件扩展或 handler 沙箱 |
| handler 读状态 | has_succeeded/has_failed/succeeded_or_failed_job_uids | 已确认删除 | handler 不读取成败集合，依赖调度由主进程判断 |
| 游标 | TaskLite/JobContext.get_cursor/set_cursor | 已确认契约 | 全局键、字符串值、None 删除、空字符串合法；worker 派发快照及自身暂存写，成功提交；主进程 Run 外直接读写；删除 seed_cursor |
| 查询 | StateSnapshot | 已确认入口 | 仅 get_job/iter_jobs/iter_attempts，返回 JSON 兼容字典；无分表及门面重复查询；配置/实际文件/结果分层，job_uid 和时间归属同名；单 Job Attempt 按编号递增，其余不承诺顺序；open 接受路径／实例 |
| 管理与历史 | set_job_status、clear_history | 已确认契约 | 三种目标状态显式 recursive；完整快照、原子级联、不造 Attempt；明确历史删除范围；删除独立取消/重跑/人工成功/失败清理入口 |
| 事件与统计 | on_run_start/on_attempt_finished/on_run_end、stats、RunSummary/RunStats | 已确认删除 | run() -> None；快照查询持久化事实，不承诺还原退出原因，不新增摘要状态 |
| 错误与验证 | TaskLiteError、底层异常、失败记录 | 已确认契约 | 自有异常仅主动拒绝，底层原样传播；删除分类器/重试异常/ERR_*；失败记录 message/exception/metadata，无公开原因码 |
| 进程执行条件 | handler、worker 启动与超时、run 信号 | 已确认主要行为 | 删除 strict_picklable，保留必要序列化校验；install_signals 默认 True，主线程安装失败明确抛错，后台线程显式 False；支持的 callable/平台在实施验收中写明 |
| 后端 | TaskLite backend 选择、StateSnapshot.open | 已确认契约 | 仅 sqlite/memory，不注入自定义后端、不公开 setter；open 接受路径或 TaskLite 实例，内存仅实例生命周期，不新增 snapshot 别名 |
| Discovery | register_discovery、DiscoveryHandler 及三协议 | 可选工具，暂不承诺兼容 | 全页命中、截断、坏项、on_missing；人工成功/取消失败/档案清除如何影响已见；多子任务整体接受后的行为 |
| HTTP | HttpPolicy/HttpExecutor/guard 系列、响应、缓存、fetch | 可选工具，暂不承诺兼容 | 不靠异常隐式重试的易用接法；请求内部重试与 Job 重试关系；删除自动资源挂起；响应缓存的身份/有效性；是否保留多个包装入口 |
| 测试与辅助函数 | fake_ctx/running、job_ref/progress_hook/slice_list | 可选工具，暂不承诺兼容 | 当前旧文档的稳定承诺在新基线切换时同步调整；删除 progress_hook；模拟授权往返和副作用；避免依赖内部结构 |
| 标识与序列化工具 | encode_*、content_fingerprint、percent_encode、jsonutil；目标 make_job_uid/parse_job_uid | 部分覆盖 | 已确认透明可逆身份接口及取消文件名约束；本轮允许破坏性简化，新基线输出稳定；旧工具逐项去留与 JSON 契约待定 |
| 版本与文档 | 包导出、GUIDE、Schema/发布窗口 | 部分覆盖 | 新基线版本、记录格式支持窗口、弃用规则、官方子模块导入路径、每个公开接口的类型提示与文档 |

## 当前 TaskLite 面逐项登记

| 现有入口 | 已接受的目标或待裁决事项 |
| --- | --- |
| __init__ | backend 必填、state_path=None、max_workers=4、output_roots=None；删除 name/state_dir、hook、pickle 开关、异常分类及三个调优参数 |
| is_running | 保留；单控制线程编排，跨线程 stop；不承诺任意属性并发读写 |
| backend 属性及 setter | 删除，不开放后端实例注入或替换 |
| stats、on_run_start、on_attempt_finished、on_run_end 属性 | 全部删除，不提供替代订阅或统计面 |
| register_resource、register_task、register_trigger | Task 简式／对象注册签名已定、同名拒绝；内置资源构造及注册已定；自定义 Trigger 不属于稳定扩展承诺 |
| register_transient_exception | 已撤销异常自动映射重试；不保留同语义的注册入口 |
| selection | 删除属性及 selection= 参数；run(exclude=None) 仅接受单规则，不接受列表 |
| filter_jobs_without_success | 不保留重复查询／过滤入口，查询统一到 StateSnapshot |
| enqueue | 单 Job 返回 bool；同配置去重、异配置冲突；无 front 或替换入口 |
| list_suspensions、list_failures | 删除；Job 查询统一到 StateSnapshot |
| clear_failures、clear_history | 删除前者；后者显式 records，返回删除数，无 keep_fatal |
| retry_failure | 删除；统一 set_job_status(..., "pending", recursive=...) |
| seed_successes | 删除；统一 set_job_status(..., "succeeded", recursive=...) |
| seed_cursor | 删除；TaskLite/JobContext 对称 get_cursor/set_cursor |
| stop | 幂等停止新派发，无 force；不阻塞等待在途，由 run 收尾 |
| run | 正常完成或排空返回 None；exclude=None、install_signals=True；底层异常原样传播，不公开跳过库锁 |
| run_graceful | 删除 |
| 当前构造器暴露的 tasks/resources/classifier/runtime_config 等属性 | 内部可变状态不成为稳定 Interface；仍需逐名迁移/删除清单 |

ADR 新增而当前尚未实现的 make_job_uid、parse_job_uid、TaskResult、FileSpec、StateSnapshot、close、set_job_status、clear_history、路径授权查询/撤销也必须进入最终签名表。不能只盘点旧方法而漏掉新承诺。

## 已确认的兼容范围

1. **常用 Interface**：编排、规格与实例、handler 上下文及结果、文件声明、只读记录、明确的管理操作，纳入稳定基线。
2. **本版收缩范围**：不设计或冻结资源扩展，不提供文件扩展，调度扩展暂不承诺兼容、不作为定稿门禁。内部保留抽象和 Seam；常用内置能力仍需明确行为。只允许通过 TaskLite 选择官方 SQLite／内存后端，不开放第三方后端注入或协议。
3. **可选工具**：HTTP、Discovery、测试辅助等本版本暂不保证兼容，文档必须标注。需要适配新核心，但不把这些工具的全面接口冻结作为本轮门禁。
4. **身份工具例外**：make_job_uid／parse_job_uid 与 Job.uid 纳入稳定接口，本轮先完成可读性简化。旧编码工具逐个核查用途；纯文件 IPC／逐 Job 锁用途移除，不将文件名字符或长度规则带进公开身份。
5. **内部 Implementation**：运行账目、注册表装配、调度状态机、底层事务校验器、可变容器不自动承诺稳定；仍需逐名整理导出清单。

## 已收束的生命周期决定

收到完整有效结果即开始结算，不等待 worker 退出，不将 failure/retry 或无结果退出当作成功。正常收尾采用校验、清理、资源释放、原子提交的顺序，期间不插入新派发。执行超时从 worker 启动计至有效结果到达；正常停止排空，框架扩展异常直接使 Run 失败。

资源仅由主线程唯一入口管理，同一资源可以供多个 worker 使用；不引入跨执行器竞争、try_acquire 或预留协议。worker 逐 Job 锁已决定删除，保留状态库级执行／管理互斥。上述是已接受决定，不再作为待选设计反复讨论。

## 讨论与验收顺序

1. 完成导出分层和保留/调整/删除清单；核心从根包、可扩展能力从职责模块导入，不重复导出；每项落实承诺级别；不为已撤出本版的扩展接口继续冻结签名。
2. 注册、初始化与校验：用重复注册、未知 Task、坏 payload、显式覆盖、初始化失败五类场景验证。
3. 按已定契约验证依赖闭合、实际成环、资源等待、框架异常、正常停止排空与通知唤醒，不重新引入缺失依赖宽限。
4. 按已定契约验证游标、查询和管理；handler 状态读取已删除，使用活跃候选/旧档案、人工成功、档案清除和并发游标提交检查边界。
5. 官方后端落实所有权与内存快照；Discovery、HTTP、测试工具完成新核心适配，不要求可选工具本轮接口冻结，也不引入第二套隐式结果协议。
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
