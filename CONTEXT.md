# TaskLite

A robust, embedded Python task orchestration engine featuring subprocess isolation, file-based IPC, ACID delta transactions, and multi-round fault recovery. Built on the Task / Job / Attempt three-layer model (ADR-0004; promoted to the main package by ADR-0005).

## Language

### Core Domain

**Task**:
进程内静态规格模板：`task_type`、handler、`resources`、`payload_schema` 及默认 `max_retries / timeout_seconds / timeout_is_transient / trigger`；代码即规格，不落盘，注册于 `TaskRegistry`。
_Avoid_: Handler, JobTemplate, WorkType, Step

**Job**:
稳定身份为 `uid = f"{task_type}::{job_id}"` 的逻辑实例。五项规格 `resources / max_retries / timeout_seconds / timeout_is_transient / trigger` 为 `None` 时继承 Task，资源按键合并且显式同名键覆盖。实例位由框架管理：`attempt_no` 为同次激活内从 1 起始的尝试号，`activation_no` 为首次准入推进的激活代，`first_enqueued_at` 重试时不刷新。已持久化的显式规格恢复时不被新模板覆盖。
_Avoid_: Message, Item, Record, Execution

**TriggerContext / TriggerRegistry**:
Trigger 在主进程依赖就绪后、出队与写 Attempt 前决定是否开始一次激活；内置 `once / on_failure / on_input_change / always`，默认 once，Discovery 默认 always。应用通过 `register_trigger(name, fn)` 注册名称，Job 仅保存字符串引用。只读判定上下文提供隔离 Job 快照、历史终态和可信成功微秒基线。False 是本次无需执行，允许下游流转；Selection Skip 是永久跳过并级联，两者分离。故障重试 `attempt_no > 1` 不重复求值；异常原样上抛并保队。

**可信成功基线**:
当前成功档案的框架元数据与同事务收尾的 succeeded Attempt 相互核验后得到的 `finished_at_us`。人工 seed 空档案与清历史后残留的旧 Attempt 不组成可信基线；缺证据保守执行。`on_input_change` 仅检查声明输入路径当前 `mtime_ns // 1000` 是否严格晚于该基线，不存历史大小/mtime，不递归目录，不因输出缺失自动触发。

**Attempt**:
The append-only trace record of one physical execution: `(job_uid, activation_no, attempt_no, execution_id, run_id, started_at, finished_at, outcome, error_message)`. Row inserted at dispatch (`outcome=running`), updated at settlement; a bypass observation surface that never participates in the six-set mutual exclusion — "final state is unique" (successes/failed by uid) and "history is traceable" (attempts never deleted) are thereby decoupled.
_Avoid_: Trial, ExecutionLog, RunRecord

**TaskRegistry**:
The in-process registry of Task specifications: one-to-one by `task_type`; duplicate registration raises `ValueError` (silent overwrite would re-bind queued jobs to a new handler), unregistered lookup raises `KeyError`.
_Avoid_: HandlerMap, Registry, TaskStore

**TaskLite**:
The host orchestrator and primary user-facing facade configuring tasks, resources, and executing runs.
_Avoid_: PipelineEngine, Runner, Master, Coordinator

**RunConfig**:
The immutable static assembly snapshot resolved once by `RunConfig.resolve()` as the single point of default-value resolution (callers pass raw Optionals, no module re-derives defaults), carried into `EngineRuntime`.
_Avoid_: ConfigBag, SettingsDict

**RunSession**:
The per-`run()` lifecycle state holder (stop-mode transitions, stats, dispatch sequencing) serving as the single outlet for run-phase event hooks (`fire_*`) and exit-reason derivation.
_Avoid_: RunContext, SessionState

**EngineRuntime**:
The deep execution engine unifying the event pump, step advances, dispatch preflights, result settlement, crash recoveries, and process isolation.
_Avoid_: RunnerHelper, LoopExecutor, ExecutionService

**PipelineState**:
In-memory fast lookup container holding active collections (`_queue`, `_successes`, `_failed`, `_cursors`, and `_in_flight`) with invariant consistency checks and encapsulated mutation interfaces.
_Avoid_: StateHolder, Store, MemoryState

**Successes（成功档案）**:
The immutable historical set of successfully executed jobs and their metadata.
_Avoid_: SuccessTable, CompletedSet, HistoryStore

**Failed (失败档案)**:
The persistent failure archive of jobs that failed permanently or exhausted their retry budget (`failed` table); mutually exclusive with successes by uid (one final state per uid). APIs: `list_failures` / `clear_failures` / `retry_failure`.
_Avoid_: FailureLog, ErrorQueue, Trash, DLQ, DeadLetter

**FailureEntry**:
The structured archive entry: `job_uid`, `error_message`, `metadata` (full archive metadata view incl. `error_category`), `payload` (raw business payload snapshot — manual re-run needs no external lookup).
_Avoid_: DLQEntry, ErrorRecord

**ErrorClassifier**:
The single source of truth for error tri-classification, transient-exception registration, payload validation, and process-death attribution (`tasklite/engine/errorclass.py`; `ErrorCategory` is the sole representation — no parallel `ERROR_TYPE_*` constants).
_Avoid_: Taxonomy, ErrorTaxonomy, ClassifierService

**encode_* family**:
The reversible `%XX` percent-encoding family (`encode_identifier` / `encode_job_component` / `encode_content_id` / `percent_encode`) plus `encode_job_uid_filename` / `content_fingerprint`, guaranteeing injectivity (zero collisions) across file paths, job IDs, and discovery namespaces.
_Avoid_: Sanitizer, Slugger, Escaper, sanitize_*

**OrderingPolicy**:
The queue visit-order seam: `scan_next_runnable` is the single selection point; default `FifoOrderingPolicy` (by queue_position). Custom policies inject via the `ordering_policy` facade parameter — the core contains no scheduling arithmetic.
_Avoid_: SortStrategy, PriorityScheme

**RequeuePolicy**:
The single outlet for retry pacing: default `ImmediateRequeuePolicy` (`RequeuePlan(front=True, delay_seconds=0.0)`); delay-style strategies re-enter through this seam via a future ADR.
_Avoid_: BackoffGovernor, RetryTimer, BackoffSchedule

**Selection**:
流水线级候选准入规则集：`Exclude(task_type=...)` 延迟留队，`Skip(task_type=...)` 终态跳过并级联下游，同一任务类型由 Skip 优先。规则对象以 `task_type` 保存匹配目标，`Rule.matches_task_type(task_type)` 为匹配入口，`Selection.allows_task_type(task_type)` 判断本次运行是否允许派发；自定义非 Skip 规则采用 Exclude 语义。

### Engine Machines

**DispatchMachine**:
先等待本批依赖、处理依赖失败，再统一求值 trigger，随后处理任务查找、孤儿探测与残留认领，申请资源并派发 worker。活跃上游遮蔽历史终态；同批 trigger False 的就绪标记仅保留在当前运行内存中。
_Avoid_: Submitter, Launcher, DispatcherService

**CompletionMachine**:
Specialized machine handling subprocess result collection, output cleanup, resource release, and atomic backend commits.
_Avoid_: Collector, ResultHandler, Finalizer

**StateStore**:
Specialized deep module unifying memory state transitions, transactional backend persistence, 3-strike crash handling, downstream failure propagation, and the failure-write guard for excluded or skipped jobs.
_Avoid_: ErrorManager, DeadlockResolver, StateRepository

**RecoveryOrchestrator**:
Specialized machine responsible for startup repairs, TOCTOU-safe aborting, and crash-safe queue persistence.
_Avoid_: RepairService, AbortHandler, Rescuer

**InFlightJob**:
Runtime tracked context for a job dispatched to an active subprocess before completion or commit.
_Avoid_: RunningTask, ActiveProcess, WorkerHandle

**WorkerLaunchSpec**:
The frozen named contract crossing the process seam, carrying the full spawn payload (handler, job, execution context, execution_id, ipc_dir, timeout_seconds) for worker subprocess launch; execution_id belongs to the spec, not to JobContext.
_Avoid_: SpawnArgs, ProcessPayload

**JobContext**:
The execution context handed to a handler, belonging to one job execution (`spawn` / `declare_output` / `declare_input` / cursors / `suspend_resource` / successes-failed snapshot queries).
`has_succeeded(job_uid)` / `has_failed(job_uid)` 查询派发时的成功/失败档案快照；`succeeded_or_failed_job_uids()` 返回只读并集，不含 skipped、队列成员或 Attempt 历史，不实时刷新。
_Avoid_: TaskContext, ExecContext, HandlerEnv

**OpsConsole**:
The pure operations seam for management APIs used outside `run()` (list_failures / clear_failures / clear_history / seed_successes / seed_cursor), constructed with explicit `(backend, store, classifier)` dependencies and delegated to by the TaskLite facade. `filter_jobs_without_success(jobs)` 仅过滤成功档案命中者，保留输入顺序，failed 与 queued Job 仍可返回。
_Avoid_: AdminAPI, MaintenanceService

**wait / decide_wait**:
The pure wait/idle decision functions in `engine/wait.py` consuming a `LoopFacts`-style snapshot per event-pump tick — the single implementation of wait semantics in the run loop.
_Avoid_: WaitStrategy, SleepPolicy, pacing

### Persistence & Encoders

**StateBackend**:
Abstract storage interface implementing atomic delta commits for job success, failure, retries, and skips under transaction boundaries.
_Avoid_: Database, Repository, StorageDriver

**SQLiteStateBackend**:
Default ACID storage adapter using SQLite in WAL mode with immediate transactions; fresh schema (`user_version=5`, incl. the append-only `attempts` table), read-only preflight rejection of older versions (`user_version <= 4`), nonempty version 0 databases, and incomplete or mixed databases. Package version is set to 2.2.0 in local development (not yet released).
_Avoid_: SQLiteDriver, DBBackend

**InMemoryStateBackend**:
Zero-IO pure memory storage adapter providing snapshot-isolated transactions for ultra-fast headless testing and ephemeral pipelines.
_Avoid_: MockBackend, FakeDB, MemoryStorage

### Official Wrappers & Utilities

**HttpPolicy**:
Rule-based response classifier translating HTTP statuses and transport faults into TaskLite exception tri-classification (`RateLimitHit`, `RetryError`, `FatalError`).
_Avoid_: ErrorStrategy, RetryPolicy, StatusMapper

**SnapshotStore**:
Content-addressable raw HTTP transaction cache enabling offline deterministic replays and anti-crawl protection without re-fetching remote endpoints.
_Avoid_: ResponseCache, HttpCache, PayloadStore
