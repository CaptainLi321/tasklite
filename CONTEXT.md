# TaskLite 领域概念词典

嵌入式 Python 任务编排引擎，采用子进程隔离、单次执行 Pipe 通信、状态库级互斥与 ACID 事务持久化。核心模型为 Task / Job / Attempt 三层（ADR-0004 / ADR-0005 / ADR-0011）。

## Language

### Core Domain

**Task**:
进程内静态规格模板：`task_type`、handler、`resources`、默认 `max_retries_per_run / timeout_seconds` 与默认 `trigger`；代码即规格，不落盘，注册于 `TaskRegistry`。
_Avoid_: Handler, JobTemplate, WorkType, Step

**Job**:
稳定身份为 `uid = make_job_uid(task_type, job_id)` 的逻辑实例。未指定的预算、超时与 Trigger 继承 Task，资源按键合并且显式同名键覆盖。执行账目不属于业务构造字段，由框架在接纳与派发时维护。
_Avoid_: Message, Item, Record, Execution

**Trigger**:
决定一次 Run 是否启动已入队 Job 的规则。内置 `once`、`until_success`、`always`、`on_input_change`；不主动入队，不控制单次启动后的故障重试。
_Avoid_: on_failure, RetryPolicy, SchedulerFlag

**TaskResult**:
任务 handler 的唯一合法显式返回类型：静态构造器 `TaskResult.success`、`TaskResult.failure`、`TaskResult.retry`。携带经过校验的严格 JSON `metadata` 与可选 `message`。
_Avoid_: ReturnTuple, BoolStatus, ExceptionRethrow

**FileSpec**:
统一文件与产物声明值对象：携带 `path`（相对或绝对路径）、`role`（output/input/cache）以及三项显式删除授权（`delete_existing`、`delete_on_failure`、`delete_on_success`）。
_Avoid_: ArtifactSpec, FileDecl, OutputPath

**StateSnapshot**:
统一只读一致状态快照与 JSON 查询入口：支持从数据库路径或 TaskLite 实例通过 `StateSnapshot.open(source)` 打开。提供 `get_job(uid)`、`iter_jobs(*, status=..., task_type=...)` 与 `iter_attempts(*, job_uid=..., task_type=...)`，返回符合 ADR-0011 契约的标准化 JSON 字典。
_Avoid_: StatusReader, StateInspector, DBViewer

**Attempt**:
一次物理执行的 append-only 轨迹记录：`(attempt_id, job_uid, attempt_no, run_id, outcome, started_at, settled_at, job_snapshot, files, result, error_message)`。派发时只持久化 Job 的 running 与 attempt_no；结算时与 Job 状态在同一事务中追加 Attempt，不更新已提交记录。outcome 仅为 success/failure/retry；failure 描述一次执行结果，授权拒绝等情形允许 Job 回到 pending。旁路观测面，不参与集合互斥。
_Avoid_: Trial, ExecutionLog, RunRecord

**TaskRegistry**:
进程内 Task 规格注册表：按 `task_type` 一对一绑定；重复注册显式抛出 `ValueError`，未注册查询抛出 `KeyError`。
_Avoid_: HandlerMap, Registry, TaskStore

**TaskLite**:
主调度器与用户入口门面：负责组装调度循环、资源与任务注册、入队与执行，并提供 Run 外统一运维管理 API。
_Avoid_: PipelineEngine, Runner, Master, Coordinator

**TaskLiteError**:
TaskLite 唯一自有公开异常，表示框架主动发现的调用条件不成立；底层 Python 或数据库异常保持原类型传播。
_Avoid_: TaskLiteException, ValidationError, RetryError, FatalError

**Successes（成功档案）**:
持久化成功完成作业集合及其最终元数据（`successes` 表）；按 `job_uid` 与失败档案互斥。
_Avoid_: SuccessTable, CompletedSet, HistoryStore

**Failed（失败档案）**:
持久化永久失败作业集合及其元数据与 payload 快照（`failed` 表）；按 `job_uid` 与成功档案互斥。统一经由 `set_job_status` 重置或 `clear_history` 清除，只读查询收敛至 `StateSnapshot`。
_Avoid_: FailureLog, ErrorQueue, Trash, DLQ, DeadLetter

**encode_* family**:
可逆 `%XX` 单射编码族（`encode_identifier` / `encode_job_component` / `encode_content_id` / `percent_encode` 以及 `make_job_uid` / `parse_job_uid`），保证标识符与路径转换无碰撞。
_Avoid_: Sanitizer, Slugger, Escaper, sanitize_*

**OrderingPolicy**:
队列访问序扩展接缝：默认 `FifoOrderingPolicy`（按 `queue_position` 升序访问），核心内部无调度排序计算。
_Avoid_: SortStrategy, PriorityScheme

**RequeuePolicy**:
重试节奏扩展接缝：默认 `ImmediateRequeuePolicy`（立即重入队），核心内部无退避计算。
_Avoid_: BackoffGovernor, RetryTimer, BackoffSchedule

**Exclude**:
流水线运行排除规则值对象：`Exclude(task_type=...)`，命中作业本次 Run 不派发、原样留队，后续未排除的 Run 重新参与候选判断。
_Avoid_: SkipRule, FilterRule, IgnoreSpec

### Engine Machines & Runtime

**DispatchMachine**:
负责检查依赖、分配单调递增 `attempt_no`、校验并申请所需资源、经 WorkerPipe 派发隔离子进程。
_Avoid_: Submitter, Launcher, DispatcherService

**CompletionMachine**:
负责接收子进程 Pipe 结果、执行主进程授权文件清理与准备、释放资源并触发原子写事务提交。执行收尾的唯一收口出口。
_Avoid_: Collector, ResultHandler, Finalizer

**StateStore**:
统一内存状态转移与数据库持久化事务的核心模块，负责四态与成功/失败档案互斥，收敛失败登记（`apply_failure`）。
_Avoid_: ErrorManager, DeadlockResolver, StateRepository

**RecoveryOrchestrator**:
负责装载持久状态，并按原执行身份把遗留 `running` 原子收敛为失败 Attempt 与 `failed`；保留 `pending`，不修复孤儿锁、不接续旧执行。进程内异常中止时也由其编排在途终止与统一结算。
_Avoid_: RepairService, AbortHandler, Rescuer

**JobContext**:
传递给 handler 的执行上下文，属于一次物理执行：提供 `spawn`、`declare_file`、`file_path`、`get_cursor` 与 `set_cursor`。
_Avoid_: TaskContext, ExecContext, HandlerEnv

### Persistence

**SQLiteStateBackend**:
默认 ACID 存储后端，采用 SQLite WAL 模式与 immediate 写事务。状态库采用 Schema 6（`user_version=6`），核心表包括 `queue`、`successes`、`failed`、`attempts`、`job_sequences`、`cursors` 与 `engine_metadata`。只读预检阶段 fail-loud 拒绝旧版本（Schema ≤ 5）与异形库。
_Avoid_: SQLiteDriver, DBBackend

**InMemoryStateBackend**:
纯内存零 IO 存储后端，提供一致快照隔离，用于快速单测与内存管线。
_Avoid_: MockBackend, FakeDB, MemoryStorage

### Optional Wrappers & Utilities

**wrappers.http**:
可选网络请求与快照缓存工具（`http_guard`, `urllib_fetch`, `SQLiteSnapshotStore`）。本版不作稳定兼容承诺，文档须显式标注。

**wrappers.discovery**:
可选增量分页扫描工具（`register_discovery`）。本版不作稳定兼容承诺，文档须显式标注。

**testing.fake_ctx**:
单元测试上下文构造辅助工具。本版不作稳定兼容承诺，文档须显式标注。
