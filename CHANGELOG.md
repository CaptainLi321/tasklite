# 更新日志

本文件记录 TaskLite（PyPI 包名 `tasklite-engine`）的所有显著变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

兼容面清算与支持基线收缩窗口：移除为不存在下游保留的 API 垫片群与测试便利接缝，Python 支持基线升至 3.10。

### 移除（向后不兼容）

- **API 兼容垫片清算（八组）**：删除 `tasklite.exceptions` 的 PEP 562 动态转发（taxonomy 符号改由 `tasklite.taxonomy` 直接导入）、`engine/runtime` 历史 re-export 块、`PreflightPolicy = ExecutionPolicy` 与 `RecoveryMachine = RecoveryOrchestrator` 别名、`InFlightTracker` 的 register/dispatch/unregister/release_all_acquired 别名族、`CompletionMachine.cleanup_outputs` 死别名、`DeadlockDecision.__bool__` 布尔求值、`TransientRegistry` 兼容门面（注册表统一 `ErrorTaxonomy`）、`ExecutionPolicy.plan_retry` 位置参数双形态（收敛为仅关键字 `retry_error=` / `transient_kind=`）。
- **Python 3.9 支持移除**：`requires-python` 升至 `>=3.10`，classifiers 与 test-matrix 同步收敛为 3.10–3.14 五解释器。

### 变更（重构）

- **注解书写现代化**：包内 28 文件 749 处 `Optional` / `Union` / `typing.Dict` 等旧式泛型迁移至 PEP 585/604 内置泛型与 `X | Y` 联合语法（`from __future__ import annotations` 全部保留；`engine/config` 的 `if False:` 伪静态导入块转正为标准 `TYPE_CHECKING`）。
- **strict_picklable 默认 True**：run() 前 handler pickle 预检默认开启（fail-loud，spawn 上下文契约），单测 lambda handler 迁移为模块级函数或显式关闭。
- **TaskContext 资源名校验恒生效**：`resource_names=None` 不再跳过 `suspend_resource` 的注册名校验（一律归一空冻结集合，空集即全拒绝）；`tasklite.testing.fake_ctx` 的 resources 缺省语义同步。
- **测试补丁接缝拆除**：`tasklite.pipeline` 的零调用 `import time` monkeypatch 锚点删除，测试补丁迁移至真实时钟消费模块。

## [1.3.1] - 2026-09-19

文档匿名化修订版：ADR-0001 上下文章节移除下游生态仓库实名清单，改以泛化表述指代。仅文档变更，无代码与公开 API 变化。

### 文档

- **ADR-0001 匿名化**：上下文与业务痛点章节不再点名下游仓库，以「下游众多批处理与采集生态仓库」泛化指代——设计动机与架构裁决内容不变。

## [1.3.0] - 2026-09-19

功能演进与安全加固版本：新增运维挂起视图、入队前 wall 过滤辅助与官方测试构造器三项公开 API；完成八组架构深化收敛与瞬态信号具名化；修复 IPC 结果文件认证与产物清理沙盒复检两项安全缺陷，以及 HTTP 传输异常分类与快照写入谓词、错误分类法构造契约、单射转义与内容指纹、状态机六集合互斥、依赖宽限与挂起信号排空等多项缺陷。

### 变更（重构）

- **架构深化收敛（八项候选，27 个原子提交）**：本窗口按架构评审完成八组深化重构——
  ① 卫生批次：rerun 策略值单一真相（9 处字面量 → `RERUN_VALUES`/`RERUN_EXEMPT_VALUES`）、断头转发与死代码群清算（`all_known_uids`/`attempted_uids` 死链、channel 死常量与零消费别名、backend 19 个死导入等）；
  ② 死亡归因：`taxonomy.attribute_process_death` 决策表成为收割侧唯一裁决点（同一 exitcode 双路径同 meta 形状），死亡串族落 DLQ 不再 unknown，`_classify_exitcode` 语义相反死实现删除；
  ③ 瞬态信号具名化（见下条）；
  ④ 死锁仲裁收形：`StandstillFacts` 停摆投影值对象成为 governor 唯一入参形状，`DispatchOutcome` 删 `sched` 内嵌与 `attribution` 死别名，13 处防御 getattr 清零；
  ⑤ runtime 收敛：五连 except 塌缩单点崩溃网、step() 不可达惰性引导删除、RT 键名常量收敛注册表旁、启动装载修复序列下沉 `RecoveryMachine.load_and_repair`（ADR 依赖矩阵零变更）；
  ⑥ channel 读取原语化：`_consume_result_if_present` 等四原语收敛五处手抄读取序列，abort 双段逐字重复消除；
  ⑦ 3-strike 收敛：`_register_commit_failure` 单点、提交失败计数唯一表示（runtime 命名空间），旧顶层裸键 repair 单点迁移，governor 空挂件退场（ADR-0002 修订 6）；
  ⑧ 测试可构造性立法：`fake_ctx` 扩参收编 29 处手搓 `TaskContext`、`running()` 句柄收编 12 处 `_is_running` 私有翻转、`tests/machines.py` 共享装配库（`MachineEnv` NamedTuple + `CommitFailureBackend` 注入工厂 + `FakeChannel` 具名替身）、`rerun_active_uids` 只读视图，附 hygiene AST 守卫锁定测试面纪律。
- **瞬态信号具名化（`transient_kind`）**：worker 结果文件 `retry` 通道的 `lock_conflict` / `rate_limited` 两个旁挂布尔键合并为单一结构化字段 `transient_kind`（`"lock_conflict"` / `"rate_limited"`；`interrupted` 维持独立 status 通道，解码侧归一为 kind）；`ExecutionResult` / `RetryPlan` / `RetryOutcome` 三份平行布尔字段收敛为 `transient_kind: Optional[str]` 单字段，`plan_retry` 关键字参数同步收敛（鸭子类型反射提取删除）；登记表 `engine.types.TRANSIENT_KIND_STAT_KEYS`（kind → 统计键）为新增瞬态信号的唯一登记点，`apply_retry` 统计映射改查表；降级写盘按 kind 单键保真。统计键名不变（公开 API 兼容）；父子进程同版本部署且跨 run 残留结果经令牌轮换丢弃，wire 变更无兼容矩阵。

### 新增

- **运维挂起视图（`OpsConsole.list_suspends` / 门面 `TaskLite.list_suspends`）**：run() 外只读查询当前仍生效的资源挂起（限流/配额熔断期间「现在挂了谁、何时解封」）——返回 `SuspendEntry(resource, resume_at, remaining_seconds)` 按解封时刻升序；真相源为 meta 表（挂起仅在 run() 启动期恢复进内存，管理段查内存恒为空），已解封条目过滤、坏数据降级告警跳过。
- **入队前过滤辅助（`TaskLite.uncompleted(jobs)`）**：返回 uid 不在 wall 的 job 子集（保留输入顺序），标准姿势 `pipeline.enqueue(pipeline.uncompleted(jobs))`——此前调用方须自行 `backend.load_wall()` 过滤，漏接时整批 job 撞 wall 以 skipped 统计静默空转。DLQ 条目不排除（重跑与否由 `rerun` 策略在派发层裁决），队列驻留重复不排除（去重是 `enqueue` 自身职责）。
- **官方测试构造器（`tasklite.testing.fake_ctx`）**：handler 单测的 TaskContext 具名参数构造器（`wall`/`failed`/`cursors`/`resources`/`tmp_root`），兜底装配内部容器结构——下游不再以位置参数硬编码 `TaskContext(job, set(), set(), {})`（内部形态属非公开契约，随框架演进碎裂）；`tmp_root` 模式创建一次性沙盒 output_root 与 ipc 目录使产物声明在单测可用。参数形态与公开 API 同等对待。
- **`http_guard` 接管契约文档化**：守卫块内任何携带 `response.status_code` 的异常（含用户 `raise_for_status()` 主动抛出的 `httpx.HTTPStatusError` / `requests.HTTPError`）一律按状态码三分类接管（429 → `RateLimitHit` + 挂起，Retry-After 从 `exc.response.headers` 提取），不落传输层模块兜底——机制自软适配分支引入起即存在，本次补契约承诺（docstring/注释/API_GUIDE §16.2）并以参数化测试锁定三分类。

### 修复

- **死锁治理（DeadlockGovernor）**：
  - 缺口升级计数（`deadlock_gap_rounds`）改为随 episode 终结清零：仲裁回到正常等待结论（`none`）、依赖宽限裁决（`grace_waiting`）或派发前进信号（`note_dispatch_progress`）时计数归零——此前计数跨 episode 累积，长运行中两次不相关的分类缺口（队列拓扑抖动即可产生非连续缺口）累计达阈值后，新缺口第 1 轮即把整个存活队列误升级 DLQ（`DEADLOCK_CLASSIFICATION_GAP`），「连续 N 轮才升级」的恢复窗口承诺失真。
- **派发与资源租约**：
  - `reserve` 的限流二次检查失败改抛专用 `RateLimitUnavailable`（`RuntimeError` 子类，消息不变），派发侧按瞬态信号 defer（零重试预算、`rate_limited_reruns` 计数、短退避降级写盘回队、零污染，实际等待由资源挂起 TTL 承担）——此前调度评估与预约之间恰有残留挂起信号被排空应用时，良性的限流等待被误判为派发故障：计入 3-strike 崩溃预算且异常上抛，穿透事件泵以崩溃终结整 run 并误杀全部在途子进程。
- **IPC 结果解析容灾**：
  - `read_result` 与逐行声明/信号读取（`read_inputs`/`read_outputs`/`_read_suspend_lines`）的容灾 except 家族纳入 `RecursionError`：深嵌套 JSON 使解析器抛出的递归失控异常不再逃逸「损坏返回 None/跳过坏行」承诺——此前损坏结果文件可沿 `claim_stale_result` → `restore_stale_result` → 主循环穿透致整管崩溃，且残留文件在认领删除前即抛、重启后崩溃循环。
- **IPC 结果文件认证**：
  - 结果文件补每 run 随机认证令牌（`secrets` 标准库生成，经 `WorkerLaunchSpec.result_token` 下发 worker，随全部状态通道的 payload 落盘，两级降级写继承令牌），主进程收割/残留认领/中止分类三条读取路径强校验，不匹配按无结果处理（瞬态、零预算）——此前 IPC 结果文件零认证，具备 ipc_dir 写权限的本地攻击者可伪造 `status=success` 结果文件，经崩溃恢复认领实现 wall 投毒、`new_jobs` 子任务注入与游标投毒。行为变化：跨 run 崩溃残留结果因令牌轮换不再被认领（一律丢弃重跑，保守正确）；相关契约测试断言已按新安全契约更新。
- **IPC 产物清理安全**：
  - 清理消费侧对 `.outputs.jsonl` 声明路径补沙盒归属复检（解析符号链接与 `..` 后须落在 `output_roots ∪ ipc_dir` 内）：成功清 cache 与失败清半成品两分支在 unlink/rmtree 前复检，越界路径拒绝删除、仅告警（fail-safe）——此前读出即信任，伪造声明可驱动 `rmtree` 删除沙盒外任意目录（含 `/`、用户 home、state_dir 自身），实现任意路径数据破坏与引擎自毁。`sandbox=False` 豁免声明的信任随之收紧到「不删」为止（豁免路径不再参与自动清理）；`output_roots` 未注入时信任根退化为 ipc_dir 自身。
- **完成机器（CompletionMachine）**：
  - 动态 spawn 子作业管道补 JSON 序列化预检（与入队管道同规）：payload 不可序列化的坏子作业在提交前独立登记失败终态（`INVALID_SPAWNED_JOB` 进 DLQ，级联下游并触发完成事件），不再连坐已成功的父作业——此前 SQLite 后端落盘 `dumps` 失败返回 False，父作业被推入 3-strike 崩溃契约（整 run 崩溃重启、handler 副作用重复后误标 `ERR_COMMIT_FAILURE_DLQ`），Memory 后端则静默接受坏 payload 到队列（双后端行为分歧就此消除）。
- **执行通道（ExecutionChannel）**：
  - `spawn` 经 `TASKLITE_IPC_DIR` 环境变量兜底解析出执行目录后回写实例属性 `ipc_dir`：journal 构造、孤儿锁探测、信号排空等收割路径以实例属性为事实源，此前仅 handle 携带 env 目录导致 spawn 成功而 reap/`probe_orphan_lock`/drain 全部崩溃（`ValueError`/`TypeError`）。
  - 锁文件环境故障（权限损坏/目录占位/路径超长等 `OSError`）按孤儿锁冲突瞬态信号同构降级：主进程派发孤儿探测关捕获 `OSError` 后走既有 defer 通道（零重试预算、降级写盘回队、零污染，日志保留 errno 归因），worker 入口锁获取失败落盘 `retry` + `lock_conflict` 降级结果——此前单作业锁文件异常即穿透派发链炸掉整 run，且作业经 crash-safe 保存留在磁盘，跨重启持续崩溃循环。
- **启动恢复**：
  - `repair_queue_on_load` 差量落盘（`delete_queue_uids`）失败降级为告警而非 re-raise：内存态已收敛、磁盘保持原状下次加载重判（幂等），与 `converge_terminal_overlap` 同策略——后端瞬态故障（锁忙、磁盘瞬时只读）不再炸掉整个 run 启动。
- **运维接缝与后端一致性**：
  - `seed_wall` 的队列驻留预检补后端持久腿：queue 腿改为内存队列与后端 `load_queue()` 提取的 uid 集合合并对账（failed 腿原本即查后端）——此前 run 前内存 state 为空占位，跨进程场景（进程崩溃后队列落盘、新进程首次 run 前 seed_wall）预检漏掉落盘队列驻留，uid 写入 wall 后在下次加载时被残留过滤从队列静默删除（作业被吞）；内存/SQLite 双后端同语义，冲突仍整体拒绝零写入。
  - `seed_cursor` 入口（`OpsConsole`）与 `InMemoryStateBackend` 统一 fail-loud 校验（key 非空 `str`、value 必须 `str`），修复同一非法入参在 SQLite 腿抛异常、memory 腿静默 `str()` 强转的跨后端行为分歧，以及 console 双写（库存 `'123'` / 内存镜像 `123`）的值型漂移。
  - `TaskLite.backend` setter 入口类型校验：非后端对象（如后端名字符串、`None`）构造期即抛 `TypeError`，不再延迟到管理 API 调用才以 `AttributeError` 爆发；除 `AbstractStateBackend` 实例外，对提供读写核心方法（可调用 `load_queue` + `commit_job_success`）的鸭子类型对象放行（崩溃注入测试的部分伪造后端接缝）。
- **调优标量合法性**：
  - `resolve_tuning` 对三参数调优标量增加合法性域校验（fail-loud）：`dep_grace_seconds` 必须为有限正值（`0`/负值/`NaN`/`inf` 会导致依赖宽限立即误杀或永不裁决活锁）；`commit_failure_dlq_threshold` 与 `deadlock_gap_max_rounds` 必须为 `>= 1` 的整数（`0` 会导致零轮即升级整队列死锁）。
  - `resolve_tuning` 类型严格化：轮次阈值仅接受真 `int`（bool、任意浮点与数字字符串 `TypeError` 拒绝），`dep_grace_seconds` 仅接受数值类型（bool/str 等拒绝）——此前 `int()`/`float()` 强转使 `deadlock_gap_max_rounds=1.9` 静默截断为 1（零恢复窗口，无根因死锁首轮即整队 DLQ），`"5"` 等数字字符串让类型漂移无告警穿透合法性域。
- **HTTP 守卫与快照**：
  - `HttpExecutor` 本地就地重试退避改为指数形状（基数 × 2^(n-1)）并封顶 300 秒、叠加 ±25% 抖动，与引擎重试退避策略一致；线性无上限退避在大 `max_retries` 下不再累计出巨量睡眠。
  - `http_guard` 的 429 挂起入口故障（如未注册资源名的 `ValueError`、非法时长的 `TypeError`）与限流信号解耦：挂起失败降级为告警日志，瞬态 `RateLimitHit` 原样抛出，不再被入口校验异常覆盖（避免烧重试预算进死信队列且挂起信号丢失）。
  - `http.client.HTTPException` 家族（`BadStatusLine` / `LineTooLong` / `ResponseNotReady` 等）整体归为瞬态传输故障，残缺状态行等协议故障不再零重试直接进死信队列。
  - 快照缓存的状态提取默认值由 200 改为 `None`，`fetch_fn` 返回 urllib 原生响应时真实状态码得以透传，不再被恒记为 200 落盘。
  - 瞬态 4xx（408/425）纳入快照保底不写入谓词，防止过期的错误响应被永久快照后离线重放持续命中。
  - `params` 中 `None` 值的丢弃语义与 `requests` 全链路对齐（请求键指纹与 urllib wire 同步），消除跨后端同请求串快照。
- **Discovery**：
  - `on_missing` 组过滤改用未截断转义前缀匹配并辅以截断头部互补匹配，超长 `cursor_key` 的分组不再静默失效（源端缺失检测不再漏报本组成员）。
  - full 模式下单条 item 的 `id_func` 失败（抛异常或非法返回，同页其余成功）现在使本轮 `on_missing` 差集失效（与整页全失败早停同策略）：失败 item 无法进入 `seen_this_run`，扫描若仍以空页完整结束，差集会把仍在源上的它确定性误报为「已删除」并触发业务破坏性动作。
- **错误分类法**：
  - `classify` / `validate_payload` / `normalize_dlq_meta` 的 Never-Raise 契约不再被「`__str__` 会抛异常的对象」击穿：契约边界内所有对不可信对象的取串统一经安全降级（`_safe_str`，异常时返回类型占位串），子进程分类路径不再因取串失败携 traceback 崩溃（无结果文件、可重试失败被误归因为环境故障）。
  - `ErrorTaxonomy`/`TransientRegistry` 构造路径与注册/声明路径同规（fail-loud，破坏性收紧）：`RetryError`/`FatalError` 子类与不可 pickle 类在构造期即拒绝——此前构造器仅做底座校验，`RetryError`/`FatalError` 子类经构造进入注册表后，`classify` 的注册表命中判定先于 fatal 判定，fatal 异常被翻转为可重试（白烧重试预算）；函数作用域类滞后到 spawn 派发期才失败。
  - `classify_exception` 同时收到位置与关键字实参（`ErrorTaxonomy` 实例形态）时显式抛 `TypeError`（fail-loud，破坏性收紧），不再静默忽略其一。
  - 构造器策略序列逐成员 fail-loud 校验，`classify_*` 的「永不抛错」契约不再可被构造路径注入的坏成员击穿。
  - `tasklite.exceptions` 动态转发以 taxonomy `__all__` 为白名单，私有符号不再泄漏至公共导出面。
  - 资源名校验补非空 `str` 约束，非 `str` 键不再跨重启发生类型漂移。
- **单射转义与内容指纹**：
  - `escape_injective` 拒绝非 `str` 输入（fail-loud，破坏性收紧），`None` 不再被强转为字面 `"None"` 的同像碰撞。
  - `sanitize_identifier` / `sanitize_job_component` / `sanitize_content_id` 同步拒绝非 `str` 非 `None` 输入（fail-loud，破坏性收紧）：隐式 `str()` 强转使 `int 123` 与 `str "123"` 同像碰撞，公共导出 API 的 job_id 派生防线（wall 去重）破缺——跨型同像会静默吞任务；`None`/空值仍走像集外哨兵。
  - `content_fingerprint` 对嵌套 `dict` 全 token 化，任意层级容器的键序不再影响指纹。
- **数据模型**：
  - `Job` runtime 命名空间仅认 `_` 前缀规范键，extra 中的同名键往返无损。
- **状态机互斥与加载期收敛**：
  - 加载期确定性收敛存量 wall∩failed 终态交集，受影响状态库升级后不再「派发即崩溃」循环。
  - `seed_wall` 预检扩展至内存队列驻留 uid，杜绝 wall∩queue 互斥重叠。
  - 准入放行的重跑同步登记豁免，动态兜底重跑不再被互斥断言击落；派发链的 `AssertionError` 原样穿透交 run 级崩溃网收尾，不再计入派发 3-strike 失败预算（断言是引擎不变式破坏信号，非作业坏输入）。
  - 准入放行的动态重跑落行内字面键，豁免登记免遭按字面键重建冲掉。
- **依赖宽限与死锁治理**：
  - 依赖宽限 episode 在派发前进与消解两条路径上正确终结残留 deadline，同 uid 复发不再被零宽限批量误杀。
- **挂起信号与 IPC 排空**：
  - `.draining` 标记文件随排空回收，读者中途死亡不再丢失 suspend 信号。
  - 跨 run 崩溃残留的 suspend 信号在启动期回收并在派发前排空。
  - abort 初扫对已完成 job 补排空 suspend 信号，收尾清理不再未读删除。
- **重试与运行时**：
  - 子进程环境性死亡（正退出码崩溃、正常退出但无结果文件 `NO_IPC_RESULT`）与信号死亡路径对称，纳入瞬态重试预算（`retry_requested`），不再零重试直接进死信队列；重试耗尽后的 DLQ 行经 `retry_error` 携带终态原因。
  - 收割超时分支归因修正：deadline 后 join 窗口内已自然退出的执行体死亡分类全权交 exitcode 分簇（0 → `NO_IPC_RESULT` 瞬态、<0 信号、>0 正码崩溃），与 deadline 前同因事件同果；仅 join 后仍存活被击杀的执行体才归 `TIMEOUT`——此前窗口内 exitcode=0 的环境性无结果被误判 `TIMEOUT` 零重试直落死信队列，DLQ 归因偏离真实故障（结果写盘失败）。
  - 重试字典以原 job_dict 为基重建，job_dict 顶层自定义字段随重试往返保留。
  - 损坏结果中 `new_jobs` 的标量形态收敛为单任务失败，不再穿透 drain/claim 使整管崩溃。
  - `RunSummary.unhandled_exception` 死赋值收敛，契约固化为 raise 通道异常语义。
  - `TaskLite.name` 构造期校验，含路径分隔符时状态库不再逃逸 `state_dir`。
  - 清理零使用的 `pickle` 与 `classify_error_type` 死导入。

## [1.2.2] - 2026-09-13

缺陷修复与鲁棒性加固版本：全面修复单射转义与指纹碰撞、HTTP 快照与限流守卫、IPC 信号原子排空、依赖宽限与环检测、状态机互斥不变式以及并发队列事务等多项潜在缺陷。

### 修复

- **单射编码与指纹安全**：
  - `escape_injective` 在任意字符集配置下对 `%` 无条件先行转义为 `%25`，彻底杜绝自定义 `forbidden` 字符集下的输出同形碰撞。
  - `sanitize_content_id(None)` 统一返回像集外固定长度哨兵，与字面量 `"None"` 严格隔离，且不受 `max_len` 截断收缩影响。
  - `content_fingerprint` 序列化引入类型标记与 `repr` 转义，消除跨类型与定界符注入导致的哈希碰撞。
  - `jsonutil.loads` 对 Python 3.11+ 超长整数字面量转换触发的 `ValueError` 统一包装为 `JSONDecodeError`，保证容灾解析分支正常生效。
  - `math.isfinite` 针对超大整数溢出抛出的 `OverflowError` 统一收敛为非有限值处理（校验入口报 `ValueError`，持久化恢复降级为 `None`）。

- **HTTP 守卫与快照治理**：
  - `http_guard` 快照 key 全面纳入位置实参、body（`json_data` 等）、params 类型前缀单射指纹以及凭证/身份头（`Authorization` / `Cookie` 等）与 `cookies` / `auth` 关键字实参，杜绝跨身份与跨请求串号命中相同快照。
  - `_resolve_category` 归一化解析状态码与异常分类器返回值，支持用户自定义三分类子类，非法返回值 fail-loud 抛 `TypeError`。
  - 修正嵌套 `http_guard` 下 `_suspended` 标志置位时机，确保无资源绑定的内层守卫放行挂起信号由外层守卫正确持久化落盘。
  - `RateLimitHit` 瞬态信号在 worker 与主进程间保持结构化标记，纳入瞬态豁免集合（不消耗重试预算、短退避回队、不污染 DLQ），防止 429 风暴下任务被误终结。
  - `Retry-After` 非正数值（<= 0）自动回落到默认挂起时长，防止 0.0 秒击穿挂起入口校验；从 requests 异常的 `response.headers` 正确提取退避时间。
  - `default_suspend_ttl` 增加构造期 fail-loud 类型与正数值校验。

- **引擎运行时与并发事务**：
  - `drain_signals` 改用原子 rename 摘除文件后读取，消除 worker 并发追加写导致的信号丢失窗口；`abort_in_flight` 在终止子进程后执行最终排空并捞回挂起信号。
  - `execute()` 初始化段（锁获取与信号陷阱）纳入 `try/except BaseException` 保护，发生环境故障时自动复位 `_is_running` 标志并释放文件锁，防止实例永久处于不可用状态。
  - `on_run_end` 生命周期钩子仅在 `session.begin()` 成功后触发，与 `on_run_start` 恢复严格对称。
  - 构造器声明的 `fatal_exceptions` 与 `transient_exceptions` 元组随 `TaskContext` 下发至 worker 子进程并真实生效，且在构造期进行类型校验。
  - `find_dependency_cycles` 改为显式栈迭代 DFS 实现，消除深链队列下的 `RecursionError` 崩溃。
  - `DeadlockGovernor` 在依赖宽限超时裁决后立即终结当前 episode 并清理 deadline，防止后续相同缺失依赖的新等待者被误判死锁。
  - `seed_wall` 在持久层与内存层均增加已失败 UID 冲突预检，拒绝向已存在于 failed 集合的 UID 植入 wall，维护六集合全局互斥。
  - `repair_queue_on_load` 改为差量定向删除并以磁盘真实顺序为权威序，保证窗口期 `front=True` 入队作业队首优先，同时消除启动期磁盘镜像行的虚假重复告警。
  - `replace_queue_atomic` 与 `save_queue` 写变前增加 `validate_queue_replacement` 形状校验，防止因返回值异常静默清空队列。
  - `TaskLite.backend` setter 同步重绑定 `OpsConsole` 的后端引用，并增加运行期禁止换库守卫。

### 文档

- 修正 README Recipe 4 示例中对 `declare_output` 字符串返回值的路径包装调用。
- 修正 API 指南 §16.7 场景 4 中 Discovery 注册示例的代码签名与派发调用。

## [1.2.1] - 2026-09-12

缺陷修复版本：收敛夜间 bug 检查批次（静态检查、变异测试分诊、性质测试扩容）确认的缺陷，并修复下游反馈的 every_run 重试断言崩溃。

### 修复

- 修复 `every_run`/`on_failure` 等重跑任务带 wall/failed 历史行进入重试时，`InFlightTracker.settle` 对已注销并重入队的作业做第二次注销、抹掉 rerun 豁免集合，导致主循环 `AssertionError: uid in wall/failed and queue` 崩溃的问题；同时将 DEBUG 六集合互斥断言的豁免判定锚定到 queue 中 job dict 的 `rerun` 事实源（运行时语义无变化，无需数据迁移）。
- 修复 `sanitize_identifier` 家族两处单射性破坏（多对一碰撞会在 wall 去重时静默吞任务）：空值哨兵与字面输入 `"untitled"` 碰撞（改为像集外形态 `%untitled`，既有输入映射不变）；截断输出落入恒等域形成确定性自碰撞，且 8 位十六进制指纹生日界过弱（改为 `%_` 像集外标记 + 16 位十六进制全文指纹，`max_len` 不足以容纳截断形态时 fail-loud 抛 `ValueError`）。注意：空串与超长标识符的派生路径/UID 会与旧版本不同，其他输入的既有映射不受影响。
- 补齐 `engine/scheduler.py`、`engine/inflight.py` 注解引用的缺失 typing 导入（此前被惰性求值掩盖，内省时即 `NameError`），并新增核心层注解可内省性卫生门禁。
- 删除 `StateStore.backend` 同名重复 property 定义（后者静默覆盖前者），并新增类成员重名 AST 门禁。
- `StateStore.wall_uids` / `failed_uids` / `queue_uids` 兑现不可变快照契约（返回 frozenset，此前实际返回活引用集合）。

### 测试

- 新增 21 个 hypothesis 性质测试（单射转义全域单射与往返、RunSession 钩子契约与幂等、wall/failed 终态互斥）与 38 个「变异必红」回归测试（flock 锁互斥、`apply_failed` 与 in-flight 注销联动、`run()` 防重入、瞬态信号不烧重试预算零污染、DLQ `_attempt` 合并语义、rerun 策略矩阵与依赖环检测）。

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

[Unreleased]: https://github.com/CaptainLi321/tasklite/compare/v1.3.1...HEAD
[1.3.1]: https://github.com/CaptainLi321/tasklite/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/CaptainLi321/tasklite/compare/v1.2.2...v1.3.0
[1.2.2]: https://github.com/CaptainLi321/tasklite/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/CaptainLi321/tasklite/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/CaptainLi321/tasklite/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/CaptainLi321/tasklite/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/CaptainLi321/tasklite/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/CaptainLi321/tasklite/releases/tag/v1.0.0
