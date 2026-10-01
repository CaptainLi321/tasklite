# 0004 v2 并行重建：Task/Job/Attempt 三层模型与命名体系重塑

## 上下文与决策动因

三路调查（全库命名审计、任务模型现状调查、实时系统理论咨询）的结论触发本次重建：

1. **命名面**：`sanitize_*` 家族名不副实（实为可逆单射编码，与红线「严禁丢弃式非单射净化」的警示方向相反）；`taxonomy.py` / `ErrorTaxonomy` 学术腔且名不盖全责（错误分类、载荷校验、DLQ 元数据规范化、进程死亡归因混居）；`apply_failed` 与 `apply_failure` 一字之差两层语义；`ERROR_TYPE_*` 九常量与 `ErrorCategory` 平行公开且前者零真实消费方；`list_suspends` 为非词（同概念内部已用 `suspensions`）；`inflight.py` 与全库 `in_flight` 拼写漂移；动词约定（add/register、mark/set、as/to）混用。
2. **模型面**：规格与执行实例混于同一个 `Job`——`retries` 占构造规格位却被框架运行期递增；`rerun` 在派发/加载期被原地改写；重试是同 uid 行 DELETE+INSERT（一次执行没有独立持久身份，`incarnation` 只活在 IPC 文件名里）；wall/failed 按 uid REPLACE 只留最新终态且成功/失败互删对侧；无 `first_enqueued_at` 与 attempt 序号，端到端追溯不可达。
3. **实时系统理论**（Liu & Layland 1973 / Dertouzos 1974 / Buttazzo 体系）：Task = 产生同构 Job 序列的静态规格模板，Job = 一次有界激活实例；三层建模（规格 / 逻辑实例 / 执行轨迹）对标 Temporal 的 WorkflowId/RunId 与 Airflow 的 try_number；软 EDF、aging 防饥饿、周期错峰、准入控制为后续高收益方向，硬可调度性分析、WCET 预算、LLF、hyperperiod 静态排期为过度设计。

用户裁决：在仓库内新建 v2 子包**从零重写**——架构照搬 v1，命名与模型按本 ADR 重塑；先吸收 Task/Job/Attempt 模式，**调度逻辑仅留接口**（以 wrapper/util 形态后续插入）；resource 概念保留，backoff 机制砍除；「DLQ」术语更换；不考虑旧数据库兼容；代码书写项（长函数拆分、注解统一、docstring 统一）全量执行。

## 裁决

### 位置与隔离

- v2 落位于 `tasklite/v2/` 子包（单发行版不变，`packages.find` 的 `tasklite.*` 已覆盖）；导入路径 `tasklite.v2.*`。
- **v2 严禁 import v1**（`tasklite.v2` 不依赖旧树任何模块），保证独立演进与最终整体替换。
- v1 进入**冻结期**：仅允许缺陷修复，不接受新特性；v2 达到功能对等后另行裁决上位与 v1 退役。
- v2 完成前，`tasklite/__init__.py`（v1 公开面）不动；v2 的公开面在 `tasklite/v2/__init__.py` 内随阶段增长。

### Task / Job / Attempt 三层模型

| 层 | 定义 | 持久化 |
|---|---|---|
| **Task（规格）** | 进程内注册的静态模板：`task_type` 名、handler、默认 resources、payload_schema、默认 max_retries / timeout / timeout_is_transient | 代码即规格，不落盘；注册于 `register_task` |
| **Job（逻辑实例）** | `uid = task_type::job_id` **身份不变**（queue/wall/failed 三表 PK 与六集合互斥原样保留）；规格性字段与实例性字段归位（见下） | queue 行 job_data JSON（uid 为主键） |
| **Attempt（执行轨迹）** | 一次物理执行的记录：attempt 序号、incarnation、起止时间、结局 | **append-only** 新表 `attempts` |

- **Job 字段归位**：规格位保留 `max_retries` / `timeout` / `timeout_is_transient` / `rerun` / `depends_on` / `resources`；实例位收敛为 `attempt_no`（本次激活内尝试序号，**1 起始含首次执行**；预算判定对齐 v1 语义：允许执行条件 `attempt_no <= max_retries + 1`）、`activation_no`（激活代，每次从 wall/failed 拦截点放行重跑时 +1）、`first_enqueued_at`（UTC ISO，首次入队时间，重试不刷新，由 enqueue 填充）。v1 的 `retries` 构造参数废除。
- **attempts 表**：`(id 自增主键, job_uid, activation_no, attempt_no, incarnation, run_id, started_at, finished_at, outcome, error)`；派发即插行（outcome=running），终态/重试时更新该行。**不参与六集合互斥**（旁路观测面），wall/failed 仍按 uid 唯一终态——「最终状态唯一」与「历史可追溯」由此解耦。
- **追溯链**：任一 attempts 行 → job_uid → 当前终态（wall/failed）→ task_type → 进程内 Task 注册规格；`incarnation`（`run_id.dispatch_seq`）随 attempt 落表，不再只存在于 IPC 文件名。

### 调度 seam（本阶段只立接口，不实现调度逻辑）

- **OrderingPolicy**：`v2/engine/scheduler.py` 的 `scan_next_runnable` 是唯一选择点；默认实现 FIFO（按 seq）。未来软 EDF / 优先级 / aging 以 wrapper/util 提供新 OrderingPolicy 插入，核心不动。
- **RequeuePolicy**：重试节奏唯一出口。v2 默认实现为**立即重入队**（瞬态信号军规语义自然成立：不烧预算、零等待）；未来 backoff / 退避类策略经此 seam 以 util 形式回归。
- v2 核心禁止出现任何排序计算与退避计算；Job 模型**不携带** priority / deadline / period 字段（其存放与语义由未来调度 wrapper 的 ADR 决定）。

### 概念取舍

- **保留**：resource 体系（CapacityResource / RateLimitResource / 挂起）、瞬态信号军规（lock_conflict / interrupted / rate_limited 不烧预算）、看门狗 timeout、rerun 四策略矩阵、wall、cursors、`incarnation` 术语、DeadlockGovernor / arbitrate、Facts 值对象族、六步调用顺序、单一出口原则、WAL fail-loud 与 BEGIN IMMEDIATE 纪律、单射性编码红线。
- **砍除**：BackoffGovernor 与 `backoff_base` / `backoff_max` / `backoff_until` / `backoff_wall_deadline` 全族；`ERROR_TYPE_*` 九常量（`ErrorCategory` 唯一表示）；mark_seen（沿用 ADR-0003 裁决）。
- **术语更换**：「DLQ / 死信」→「**失败档案 failed**」（表 `failed`、记录类 `FailureEntry`、API `list_failures` / `clear_failures` / `retry_failure`）。

### 命名映射表（v1 → v2）

模块层：

| v1 | v2 |
|---|---|
| `tasklite/taxonomy.py` | `tasklite/v2/errorclass.py` |
| `tasklite/utils/injective.py` | `tasklite/v2/utils/encoding.py` |
| `tasklite/engine/inflight.py` | `tasklite/v2/engine/in_flight.py` |
| `tasklite/engine/pacing.py` | `tasklite/v2/engine/wait.py` |
| `tasklite/engine/policy.py` | `tasklite/v2/engine/admission.py`（RerunPolicy）；重试决策并入 store/completion |
| `tasklite/engine/console.py` | `tasklite/v2/engine/ops.py`（OpsConsole 类名保留） |

标识符层：

| v1 | v2 |
|---|---|
| `sanitize_identifier` | `encode_identifier` |
| `sanitize_job_component` | `encode_job_component` |
| `sanitize_content_id` | `encode_content_id` |
| `escape_injective` | `percent_encode` |
| `ErrorTaxonomy` | `ErrorClassifier` |
| `attribute_process_death` | `classify_process_death` |
| `apply_failed`（内存尾段语义） | `mark_failed_memory` |
| `pop_next_runnable` | `scan_next_runnable` |
| `list_suspends` / `load_resource_suspends` / `persist_resource_suspends` | `list_suspensions` / `load_resource_suspensions` / `persist_resource_suspensions` |
| `ERROR_TYPE_*` 常量 | 不迁移（`ErrorCategory` 唯一） |
| `TaskContext` | `JobContext`（执行上下文属于一次 job 执行，且 Task 在 v2 有了规格层新义） |
| `DLQEntry` | `FailureEntry` |
| `list_dlq` / `clear_dlq` | `list_failures` / `clear_failures` |
| `add_resource` | `register_resource` |
| `register_handler` | `register_task` |
| `AdmissionPolicy` | `RerunPolicy` |
| `BackoffGovernor` / `plan_retry` 退避计算 | 删除（RequeuePolicy seam 接管） |
| `on_job_completed(uid, meta, success, going_to_retry)` | `on_attempt_finished(uid, *, outcome)` |
| `_sanitize_suspend` | `_clamp_suspend` |
| `cascade_fail` / `fail_cascade` | 统一 `cascade_fail` |
| `note_dispatch_progress` | `record_dispatch_progress` |
| `as_error_strings` | `to_error_strings` |
| `validate_payload`（模块级、返回 list） | `validate_payload_errors` |
| `claim_stale_result`（utils/ipc 同名异构） | `claim_stale_result_payload` |
| `fetch_urllib` / `fetch_requests` | `urllib_fetch` / `requests_fetch` |
| `commit_failure_dlq_threshold` | `commit_failure_threshold` |
| `ERR_COMMIT_FAILURE_DLQ` | `ERR_COMMIT_FAILURE`（**值串同步变更** `"COMMIT_FAILURE_DLQ"` → `"COMMIT_FAILURE"`——下游按错误码串匹配的迁移必须改串，非仅改常量名） |
| `guarded_fetch` | `guarded`（wrappers/http 装饰器工厂） |
| http guard 参数 `backoff` | `retry_delay_base` |
| `HandlerEntry` | `Task` + `TaskRegistry`（结构性取代：handler 注册条目升级为规格模板与注册表两层） |
| 实例属性 `pipeline.taxonomy` / `pipeline.handlers` | `pipeline.classifier` / `pipeline.tasks` |

**保留不动**：`wall`、`seed_wall`、`seed_cursor`、`safe_uid_filename`、`content_fingerprint`、`incarnation`、`DeadlockGovernor.arbitrate`、`RunConfig` / `RunSession` / `EngineRuntime` / `PipelineState` / `DispatchMachine` / `CompletionMachine` / `StateStore` / `RecoveryOrchestrator` / `InFlightJob` / `WorkerLaunchSpec` / `OpsConsole`、`job_ref` / `progress_hook` / `slice_list`、`ERR_*` 错误码常量、`EMPTY_SENTINEL`。

### 代码迁移原则

1. **架构照搬**：模块边界、机器分工、全部系统级不变式原样保留，v2 不发明新架构。
2. **拷贝不引用**：v2 严禁 import v1。
3. **模型优先**：涉及三层化 / backoff 砍除 / 术语更换的代码段按新模型直接重写，不制造「先照搬后修」的中间态提交。
4. **公开面重塑不留别名**：无 deprecated 垫片、无平行常量。
5. **移植必配测试**：每个移植单元有对应 v2 测试（优先移植 v1 同源测试并按新 API 改写），置于 `tests/v2/`；断言语义而非实现细节。
6. **分层红线镜像**：`v2/models/` 严禁 import `v2/engine/`（IPC 声明读写下沉 `v2/utils/ipc.py`）；`v2/utils/` 严禁 import `v2/wrappers/`；v2 核心层严禁依赖 `v2/contrib/`。
7. **书写全量新标准**：新式类型注解（`dict[str, X] | None`，禁 `Dict/Optional/Union/List/Type` 大写形态）；布尔参数一律 keyword-only；标识符禁字母-数字-字母交错形态；v1 已知长函数在 v2 必须拆分（`TaskLite.__init__` 装配、`Job.__init__` 校验链、`DiscoveryHandler.__call__`、`dispatch_job`、`scan_next_runnable`、`_validate_payload_impl`、`_decode_ipc_result`、`resolve_deadlock`、`check_dependency_grace` 内部重复块）；docstring 统一中文。
8. **数据库零兼容**：v2 schema 全新（含 attempts 表与 failed 术语），不写迁移路径、不认旧 user_version。

### 注释迁移原则

1. **不变式与反直觉决策注释必迁**：单射性数学证明（先 `%25` 后 `::` 转义防 `t::x::y` 碰撞）、锁生命周期严格等于子进程执行体生命周期、六集合全局互斥、`_CommitCrashSignal` 继承 `BaseException` 的理由等，按 v2 新名改写标识符后迁入。
2. **历史叙事必删**：「旧实现如何 / 现已改为 / 历史垫片」类注释在 v2 中没有意义，直接丢弃；v2 只注释「现在为什么」。
3. **陈旧引用重写**：引用旧私有名的注释以 v2 现名为准重写或删除。
4. **时序防护转测试**：复杂并发时序 / TOCTOU 防御逻辑迁移时，对应 v1 回归测试必须同步移植锁定，代码中只留一行意图说明。
5. **术语按 v2 词汇表改写**：DLQ → 失败档案、sanitize → encode、taxonomy → 错误分类。
6. **审查编号与批次标签零容忍**（hygiene AST 门禁自动守护，含标识符交错形态）。

### 实施纪律

- 依赖序分阶段：基础层（exceptions / utils）→ 模型层（task / job / state / context / attempt）→ backend → engine 机器群 → 门面与包装器 → 文档同步。
- 每逻辑单元当场原子提交（中文规范 commit message），提交前 `python -m pytest tests/ -q -p no:cacheprovider` 全绿；阶段末跑 `make test-matrix`。
- 严禁 `git add -A`（工作区存在用户未跟踪文件，必须显式路径 add）；遇 index.lock 冲突 `sleep 2` 重试至多 5 次。

## 后果与重开条件

- 双轨期测试面翻倍：v1 测试原样不动，v2 测试置于 `tests/v2/`。
- attempts 表为旁路 append-only，不属六集合任何一员，互斥不变式无需重开；若未来需要「按 attempt 取消 / 重跑单次执行」，须重开本 ADR。
- v2 上位判据：对 v1 全部保留特性功能对等、测试覆盖对等、文档同步；届时新 ADR 裁决 v1 退役与 `tasklite.v2` 提升为主包路径。
- 调度接线的门面构造参数（`TaskLite(ordering=...)` / `TaskLite(requeue_policy=...)`）已按库主裁决提前落地：仅做接线——策略实例经 RunConfig 装配透传至 `JobScheduler(ordering=...)` 与重试节奏出口，默认不传行为不变（FIFO / 立即重入队，解析收敛在 `RunConfig.resolve` 唯一解析点），不实现任何策略。
- 调度 wrapper ADR（软 EDF / aging / 错峰 / 准入、延迟类重试策略）引入时的裁决范围收窄为：OrderingPolicy / RequeuePolicy 的**策略实现**与 **Job 调度属性（priority / deadline / period）的存放位置**；接缝本身与门面接线形态已由本 ADR 定案，不再是该 ADR 的事项。
- v1 冻结期发现的设计缺陷：v1 修复（保持行为）与 v2 修正**双落**，禁止只在 v2 修。
