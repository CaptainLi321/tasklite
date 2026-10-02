# TaskLite

A robust, embedded Python task orchestration engine featuring subprocess isolation, file-based IPC, ACID delta transactions, and multi-round fault recovery. Built on the Task / Job / Attempt three-layer model (ADR-0004; promoted to the main package by ADR-0005).

## Language

### Core Domain

**Task**:
The static specification template registered in-process: `task_type` name, handler, `default_resources`, `payload_schema`, default `max_retries` / `timeout` / `timeout_is_transient`. Code-is-spec, never persisted; registered via `register_task` into a `TaskRegistry`.
_Avoid_: Handler, JobTemplate, WorkType, Step

**Job**:
The logical instance of one bounded activation, uniquely identified by `uid = f"{task_type}::{job_id}"` with associated payload, dependencies, and resource requirements. Specification slots (payload/resources/depends_on/max_retries/timeout/rerun) override Task defaults; instance slots are framework-managed: `attempt_no` (1-based try number within the activation; execution allowed while `attempt_no <= max_retries + 1`), `activation_no` (generation counter, +1 on each rerun released from a wall/failed interception), `first_enqueued_at` (UTC ISO of first enqueue, never refreshed on retry).
_Avoid_: Message, Item, Record, Execution

**Attempt**:
The append-only trace record of one physical execution: `(job_uid, activation_no, attempt_no, incarnation, run_id, started_at, finished_at, outcome, error)`. Row inserted at dispatch (`outcome=running`), updated at settlement; a bypass observation surface that never participates in the six-set mutual exclusion — "final state is unique" (wall/failed by uid) and "history is traceable" (attempts never deleted) are thereby decoupled.
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
In-memory fast lookup container holding active collections (`_queue`, `_wall`, `_failed`, `_cursors`, and `_in_flight`) with invariant consistency checks and encapsulated mutation interfaces.
_Avoid_: StateHolder, Store, MemoryState

**Wall**:
The immutable historical set of successfully executed jobs and their metadata.
_Avoid_: SuccessTable, CompletedSet, HistoryStore

**Failed (失败档案)**:
The persistent failure archive of jobs that failed permanently or exhausted their retry budget (`failed` table); mutually exclusive with wall by uid (one final state per uid). APIs: `list_failures` / `clear_failures` / `retry_failure`.
_Avoid_: FailureLog, ErrorQueue, Trash, DLQ, DeadLetter

**FailureEntry**:
The structured archive entry: `uid`, `error`, `meta` (full archive metadata view incl. `error_type`), `job_payload` (raw business payload snapshot — manual re-run needs no external lookup).
_Avoid_: DLQEntry, ErrorRecord

**ErrorClassifier**:
The single source of truth for error tri-classification, transient-exception registration, payload validation, and process-death attribution (`tasklite/engine/errorclass.py`; `ErrorCategory` is the sole representation — no parallel `ERROR_TYPE_*` constants).
_Avoid_: Taxonomy, ErrorTaxonomy, ClassifierService

**encode_* family**:
The reversible `%XX` percent-encoding family (`encode_identifier` / `encode_job_component` / `encode_content_id` / `percent_encode`) plus `safe_uid_filename` / `content_fingerprint`, guaranteeing injectivity (zero collisions) across file paths, job IDs, and discovery namespaces.
_Avoid_: Sanitizer, Slugger, Escaper, sanitize_*

**OrderingPolicy**:
The queue visit-order seam: `scan_next_runnable` is the single selection point; default `FifoOrderingPolicy` (by seq). Custom policies inject via the `ordering` facade parameter — the core contains no scheduling arithmetic.
_Avoid_: SortStrategy, PriorityScheme

**RequeuePolicy**:
The single outlet for retry pacing: default `ImmediateRequeuePolicy` (`RequeuePlan(front=True, delay_seconds=0.0)`); delay-style strategies re-enter through this seam via a future ADR.
_Avoid_: BackoffGovernor, RetryTimer, BackoffSchedule

### Engine Machines

**DispatchMachine**:
Specialized machine enforcing the 5-stage preflight checks (dedup, dependency, task lookup, orphan probe, stale restore), acquiring resources, and submitting worker subprocesses.
_Avoid_: Submitter, Launcher, DispatcherService

**CompletionMachine**:
Specialized machine handling subprocess result collection, output cleanup, resource release, and atomic backend commits.
_Avoid_: Collector, ResultHandler, Finalizer

**StateStore**:
Specialized deep module unifying memory state machines, transactional backend persistence, 3-strike crash contracts, cascade downstream failures, and deadlock attribution policies.
_Avoid_: ErrorManager, DeadlockResolver, StateRepository

**RecoveryOrchestrator**:
Specialized machine responsible for startup repairs, TOCTOU-safe aborting, and crash-safe queue persistence.
_Avoid_: RepairService, AbortHandler, Rescuer

**InFlightJob**:
Runtime tracked context for a job dispatched to an active subprocess before completion or commit.
_Avoid_: RunningTask, ActiveProcess, WorkerHandle

**WorkerLaunchSpec**:
The frozen named contract crossing the process seam, carrying the full spawn payload (handler, job, execution context, incarnation, ipc_dir, timeout) for worker subprocess launch; incarnation belongs to the spec, not to JobContext.
_Avoid_: SpawnArgs, ProcessPayload

**JobContext**:
The execution context handed to a handler, belonging to one job execution (`spawn` / `declare_output` / `declare_input` / cursors / `suspend_resource` / wall-failed snapshot queries).
_Avoid_: TaskContext, ExecContext, HandlerEnv

**OpsConsole**:
The pure operations seam for management APIs used outside `run()` (list_failures / clear_failures / clear_history / seed_wall / seed_cursor), constructed with explicit `(backend, store, classifier)` dependencies and delegated to by the TaskLite facade.
_Avoid_: AdminAPI, MaintenanceService

**wait / decide_wait**:
The pure wait/idle decision functions in `engine/wait.py` consuming a `LoopFacts`-style snapshot per event-pump tick — the single implementation of wait semantics in the run loop.
_Avoid_: WaitStrategy, SleepPolicy, pacing

### Persistence & Encoders

**StateBackend**:
Abstract storage interface implementing atomic delta commits for job success, failure, retries, and skips under transaction boundaries.
_Avoid_: Database, Repository, StorageDriver

**SQLiteStateBackend**:
Default ACID storage adapter using SQLite in WAL mode with immediate transactions; fresh schema (`user_version=3`, incl. the append-only `attempts` table), fail-loud rejection of any older library.
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
