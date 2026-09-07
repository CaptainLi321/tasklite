# TaskLite

A robust, embedded Python asynchronous task orchestration engine featuring subprocess isolation, file-based IPC, ACID delta transactions, and multi-round fault recovery.

## Language

### Core Domain

**Job**:
The atomic unit of execution in TaskLite, uniquely identified by `uid = f"{task_type}::{job_id}"` with associated payload, dependencies, and resource requirements.
_Avoid_: Task (overloaded), Message, Item, Record

**TaskLite**:
The host orchestrator and primary user-facing facade configuring handlers, resources, and executing runs.
_Avoid_: PipelineEngine, Runner, Master, Coordinator

**RunContext**:
The single-run mutable runtime container holding transient state (`PipelineState`, `in_flight`, stats, and episode tracking) across specialized engine machines.
_Avoid_: Session, ExecutionScope, ContextBag

**PipelineState**:
In-memory fast lookup container holding active collections (`queue`, `wall`, `failed`, `cursors`, and `in_flight`) with invariant consistency checks.
_Avoid_: StateHolder, Store, MemoryState

**Wall**:
The immutable historical set of successfully executed jobs and their metadata.
_Avoid_: SuccessTable, CompletedSet, HistoryStore

**DLQ (Dead Letter Queue)**:
The persistent collection of jobs that failed permanently or exhausted their retry budget (`failed` table).
_Avoid_: FailureLog, ErrorQueue, Trash

### Engine Machines

**DispatchMachine**:
Specialized machine enforcing the 5-stage preflight checks (dedup, dependency, handler, orphan probe, stale restore), acquiring resources, and submitting worker subprocesses.
_Avoid_: Submitter, Launcher, DispatcherService

**CompletionMachine**:
Specialized machine handling subprocess result collection, output cleanup, resource release, and atomic backend commits.
_Avoid_: Collector, ResultHandler, Finalizer

**StateStore**:
Specialized deep module unifying memory state machines, transactional backend persistence, 3-strike crash contracts, cascade downstream failures, and deadlock attribution policies.
_Avoid_: ErrorManager, DeadlockResolver, StateRepository

**RecoveryMachine**:
Specialized machine responsible for startup repairs, TOCTOU-safe aborting, and crash-safe queue persistence.
_Avoid_: RepairService, AbortHandler, Rescuer

**InFlightJob**:
Runtime tracked context for a job dispatched to an active subprocess before completion or commit.
_Avoid_: RunningTask, ActiveProcess, WorkerHandle

### Persistence & Encoders

**StateBackend**:
Abstract storage interface implementing atomic delta commits for job success, failure, retries, and skips under transaction boundaries.
_Avoid_: Database, Repository, StorageDriver

**SQLiteStateBackend**:
Default ACID storage adapter using SQLite in WAL mode with immediate transactions.
_Avoid_: SQLiteDriver, DBBackend

**InMemoryStateBackend**:
Zero-IO pure memory storage adapter providing snapshot-isolated transactions for ultra-fast headless testing and ephemeral pipelines.
_Avoid_: MockBackend, FakeDB, MemoryStorage

**InjectiveEncoder**:
Reversible mathematical `%XX` percent-encoding and SHA-256 fingerprinting utility guaranteeing zero naming collisions across file paths, job IDs, and discovery namespaces.
_Avoid_: Sanitizer, Slugger, Escaper

### Official Wrappers & Utilities

**HttpPolicy**:
Rule-based response classifier translating HTTP statuses and transport faults into TaskLite exception tri-classification (`RateLimitHit`, `RetryError`, `FatalError`).
_Avoid_: ErrorStrategy, RetryPolicy, StatusMapper

**SnapshotStore**:
Content-addressable raw HTTP transaction cache enabling offline deterministic replays and anti-crawl protection without re-fetching remote endpoints.
_Avoid_: ResponseCache, HttpCache, PayloadStore


