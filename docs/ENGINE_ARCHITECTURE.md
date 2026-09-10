# tasklite 引擎架构总览与设计手册

> 模块路径：`tasklite.engine`
> 本文为 **ADR-0002 固化的终态架构基线**（防漂移规范）；现状与终态的差异见
> [第六章·现状偏差与迁移基线](#六现状偏差与迁移基线)。
> 适用版本：v1.1.0+（迁移期）

---

## 目录

1. [架构分层与设计哲学](#一架构分层与设计哲学)
2. [核心数据流与执行闭环](#二核心数据流与执行闭环)
3. [核心状态机体系](#三核心状态机体系)
4. [目标模块职责与接口规范](#四目标模块职责与接口规范)
5. [接缝清单与不变式契约](#五接缝清单与不变式契约)
6. [现状偏差与迁移基线](#六现状偏差与迁移基线)

---

## 一、架构分层与设计哲学

`tasklite` 采用「单一宿主门面 + 静态装配快照 + 生命周期会话 + 职责特化机器群」的
解耦架构，全部由三条设计公理推出：

- **装配与状态分离**：`run()` 入口拍下不可变 `RunConfig` 快照；运行期可变状态只存在
  于 `RunSession` 一个模块。机器从显式窄依赖获取服务，不反向持有 `TaskLite`，也不
  经共享袋互见。
- **单一出口原则**：Job 终结唯一经 `CompletionMachine.complete_job`；失败终态登记
  唯一经 `StateStore.apply_failure` / `apply_failed`（wall / failed 互斥）；运行钩子
  唯一经 `RunSession.fire_*`；`exit_reason` 推导唯一经 `RunSession.exit_reason()`；
  等待决策唯一经 `pacing.decide_wait`。
- **fail-loud 纪律**：WAL 模式必须验证生效；Job 入参严格校验；单射转义拒绝静默碰撞；
  未知错误码与死锁即时归因。

分层依赖（单向，箭头指向被依赖方；违者即架构破坏）：

```
pipeline.py ──► engine/* ──► models/* ──► (stdlib / 无)
                  │              │
                  ▼              ▼
              backend/*       utils/*        wrappers/*（可选层，零引擎强耦合）
```

补充分层红线（继承自 `AGENTS.md`）：`models/` 严禁反向 import `engine/`；
`utils/` 严禁反向 import `wrappers/`；核心层严禁依赖 `contrib/`。

---

## 二、核心数据流与执行闭环

```
                   ┌──────────────────────────────────────┐
                   │            TaskLite.run()            │
                   │  拍 RunConfig 快照 → EngineRuntime    │
                   └──────────────────┬───────────────────┘
                                      ▼
                        ┌───────────────────────────┐
                        │ RecoveryMachine 启动修复   │ (孤儿锁清理 / 残留挂起恢复)
                        └─────────────┬─────────────┘
                                      ▼
             ┌─────────────────────────────────────────────┐
             │        EngineRuntime 事件泵 step()           │
             │  停机门 → 填池派发 → 死锁仲裁 → 回收结算      │
             │  → pacing.decide_wait → StepOutcome          │
             └───────────────┬───────────────▲─────────────┘
                             │               │ (等待/轮询 drain)
             ┌───────────────▼──────────┐    │
             │ 1. Scheduler 队列只读扫描 │    │
             │  - runnable / dep_failed │    │
             │  - 死锁与畸形细粒度归因   │    │
             └───────────────┬──────────┘    │
             ┌───────────────▼──────────┐    │
             │ 2. Dispatch 五关预检派发  │    │
             │  - dedup / dep-failed    │    │
             │  - no-handler / orphan   │    │
             │  - acquire + WorkerLaunch │    │
             │    Spec 提交子进程        │    │
             └───────────────┬──────────┘    │
             ┌───────────────▼──────────┐    │
             │ 3. Channel 回收落盘结果   │────┘
             │  - incarnation 身份校验   │
             │  - 超时与崩溃信号检测     │
             └───────────────┬──────────┘
             ┌───────────────▼──────────┐
             │ 4. Completion 事务结算    │
             │  - StateStore.apply_*    │
             │  - 成功 wall / 失败 DLQ  │
             │  - 释放租约 / 触发钩子    │
             └──────────────────────────┘
```

---

## 三、核心状态机体系

### 1. 停机状态机（`StopMode`，态转移唯一入口 `RunSession.request_stop`）

- `NONE`：正常执行；
- `DRAINING`：优雅停机（首次 `SIGTERM`/`SIGINT` 或 `stop()`）——停止派发新 job，
  等待当前 in-flight 自然完成后安全退出；
- `ABORTING`：强制停机（二次信号或 `stop(force=True)`）——对 in-flight 分类消费：
  已写好结果者正常提交，未完成者终止进程、清理半成品并 requeue 磁盘队列。
- 转移单调：`NONE → DRAINING → ABORTING`，不可逆。终局原因由
  `RunSession.exit_reason()` 唯一推导（execute / step / 主循环三处共用同一实现，
  禁止另写分支）。

### 2. 任务生命周期转移

- `queue` → `in_flight`（派发成功）→ `wall`（执行成功）
- `queue` → `in_flight` → `queue`（退避重试 / 孤儿锁延迟 / 优雅中断）
- `queue` → `failed_dlq`（达到最大重试 / 依赖失败 / 无 Handler / 畸形数据 / 确定性
  输入错误）

### 3. 死锁归因与宽限机理（`DeadlockGovernor`）

- **畸形数据（`malformed_uids`）**：反序列化失败，最高优先级直接入 DLQ；
- **未知/超限资源（`unknown` / `impossible`）**：强制 `min_wait=inf`，仅失败肇事者，
  下游走正常 cascade；
- **缺失依赖（`missing_dependency_uids`）**：
  - 若队列中存在潜在 spawner，授予 `dep_grace_seconds` 宽限期；
  - 宽限期满仍缺失，判为死锁入 DLQ 并级联标记；
- **保守兜底（`deadlock_gap_rounds`）**：连续多轮无已知根因时升级整队列 DLQ
  （`ERR_DEADLOCK_GAP`），恢复终止性。

---

## 四、目标模块职责与接口规范

> 签名为**契约规范**（参数顺序与命名以此为准）；实现细节由各模块自行内聚。

### 4.1 门面层

#### `pipeline.py` — `TaskLite`（用户唯一入口，六步标准调用顺序）

```python
class TaskLite:
    def __init__(name, state_dir, backend="sqlite", output_root=None, max_workers=4,
                 on_run_start=None, on_run_end=None, on_job_completed=None,
                 strict_picklable=False, fatal_exceptions=None, transient_exceptions=None,
                 dep_grace_seconds=None, commit_failure_dlq_threshold=None,
                 deadlock_gap_max_rounds=None)   # Optional 一律透传，不做默认值解析
    def add_resource(self, resource: Resource) -> None
    def register_handler(self, task_type, handler_func,
                         default_resources=None, payload_schema=None) -> None
    def set_discovery_rerun(self, task_type, rerun) -> None
    def register_transient_exception(self, exception_cls: type) -> None
    def enqueue(self, jobs, front=False) -> None
    def run(self) -> None            # 入口拍 RunConfig.resolve(...) 快照 → EngineRuntime
    def stop(self, force=False) -> None
    def run_graceful(self) -> None
    # 管理 API（run() 外专用，守卫 _ensure_not_running 留在门面）：
    def list_dlq / clear_dlq / clear_history / seed_wall / seed_cursor  # 委托 OpsConsole
```

门面红线：不解析默认值、不持有运行态可写属性、`run()` 期间仅响应 `stop()`。
用户工具函数（`job_ref` / `progress_hook` / `slice_list`）不放在门面文件。

### 4.2 引擎层

#### `engine/types.py` — 引擎公共值对象（零内部依赖叶子，单一真相源）

```python
StopMode(Enum)             # NONE / DRAINING / ABORTING
ExitReason(str, Enum)      # COMPLETED / STOPPED_DRAINING / STOPPED_ABORTING / INTERRUPTED / ERROR
TaskStats(dict)            # 8 计数器 + 只读属性
EMPTY_STATS: Mapping[str, int]
ExecutionOptions           # frozen: install_signals / acquire_run_lock
StepOutcome                # frozen: 单步事件泵产物
RunSummary                 # frozen: 终局摘要
HandlerEntry(NamedTuple)   # func / default_resources / payload_schema
```

#### `engine/config.py` — `RunConfig`（静态装配快照 + 默认值唯一落点）

```python
@dataclass(frozen=True)
class RunConfig:
    name: str; ipc_dir: str
    backend: AbstractStateBackend
    resources: ResourceManager
    handlers: Mapping[str, HandlerEntry]
    channel: ExecutionChannel
    taxonomy: ErrorTaxonomy
    discovery_rerun: Mapping[str, str]
    output_root: Optional[Union[Path, Sequence[Path]]]
    strict_picklable: bool
    dep_grace_seconds: float          # 字段类型即最终值（非 Optional）
    commit_failure_dlq_threshold: int
    deadlock_gap_max_rounds: int
    on_run_start / on_run_end / on_job_completed

    @classmethod
    def resolve(cls, **raw) -> RunConfig   # 唯一规范化入口：None → 常量默认
```

#### `engine/session.py` — `RunSession`（单次 run 生命周期状态 + 钩子单一出口）

```python
class RunSession:
    run_id: Optional[str]; dispatch_seq: int
    stop_mode: StopMode; stats: TaskStats      # 无 public setter
    def begin(self, run_id: str) -> None       # 重置 stats / 宽限轮次 / run_end 标志
    def next_dispatch_seq(self) -> int         # fence 序号单调递增
    def request_stop(self, force: bool) -> StopMode   # 单调状态机唯一入口
    def exit_reason(self, fallback: ExitReason) -> ExitReason  # 唯一推导实现
    def fire_run_start(self) -> None
    def fire_job_completed(self, uid, meta, success, going_to_retry) -> None
    def fire_run_end(self, reason: str) -> None         # 幂等，只发一次
```

禁止复活物：governor 代理属性、`episode` 双名、`stats` setter 双写、`in_flight`
setter 测试后门、`state` / `backend` 可写属性（ADR-0002 D1）。

#### `engine/pacing.py` — 等待决策纯函数

```python
@dataclass(frozen=True)
class LoopFacts:        # 一拍事件泵的纯数据快照
    stop_mode: StopMode; has_in_flight: bool; store_empty: bool
    dispatched: int; completed: int; has_runnable: bool
    min_wait: float     # 调度器候选最早可运行时刻（inf = 无候选）
    worker_wait: float  # 资源挂起最早恢复时刻
    deadlock_wait: float  # governor 宽限/gap 裁决等待

@dataclass(frozen=True)
class WaitDecision:
    should_wait: bool; wait_time: float

def decide_wait(facts: LoopFacts) -> WaitDecision   # 纯函数，表驱动单测锁定
```

#### `engine/runtime.py` — `EngineRuntime`（装配 + 薄事件泵）

```python
class EngineRuntime:
    def __init__(self, config: RunConfig) -> None   # 内部装配机器群，不外泄 ctx
    def execute(self, options: Optional[ExecutionOptions] = None) -> RunSummary
        # 锁 / 信号 / 钩子 / 异常承重网；exit_reason 一律委托 session
    def step(self, max_dispatch: Optional[int] = None) -> StepOutcome  # 目标 <60 行
    def request_stop(self, force: bool = False) -> StopMode            # 委托 session
    @property is_running / stats / stop_mode / store
```

`step()` 顺序管道：停机门（ABORTING / 排空完毕 / 空闲完成三个早退统一走
`_terminal_outcome(reason)`）→ 填池派发 → 死锁仲裁（仅无 in-flight 时）→ 回收结算
→ `decide_wait` → `StepOutcome`。

#### 三台机器 — 显式窄依赖构造（禁止 Context 整袋）

```python
class DispatchMachine:
    def __init__(self, store, scheduler, policy, resources, channel,
                 in_flight, session, completion) -> None
    def dispatch_next(self) -> DispatchOutcome        # 五关预检 + spawn

class CompletionMachine:
    def __init__(self, store, policy, channel, resources, in_flight,
                 session) -> None
    def complete_job(self, entry: InFlightJob, result: ExecutionResult) -> None
        # Job 终结唯一出口
    def settle_reaped(self, completed) -> int
    def settle_aborted(self, handles) -> None

class RecoveryMachine:
    def __init__(self, store, backend, channel, resources, in_flight,
                 session, completion) -> None
    def repair_queue_on_load(...) / load_resource_suspends() / persist_resource_suspends()
    def save_queue_crash_safe() / apply_pending_signals() / abort_in_flight()
```

#### `engine/store.py` — `StateStore`（入队规范化 + 转移事务 + 3-strike）

```python
class StateStore:
    def __init__(self, backend, *, taxonomy, governor, policy, stats,
                 on_job_completed) -> None      # 显式依赖，禁止 getattr 回查
    def enqueue_jobs(self, jobs, *, front=False) -> List[Job]   # 规范化 + wall 去重
    @property state / is_empty
    wall_uids() / failed_uids() / queue_uids() / in_flight_uids() / cursors()
    pop_job(idx) / requeue_jobs(job_dicts, *, front)
    register_in_flight(uid) / unregister_in_flight(uid) / clear_in_flight()
    # 结算：状态转移唯一出口（wall / failed 互斥在此保证）
    def apply_success(...) -> SuccessOutcome
    def apply_failure(...) -> FailureOutcome
    def apply_retry(...) -> RetryOutcome
    def apply_skip(...) -> SkipOutcome
    def cascade_fail(uid) -> List[str]
    def commit_failed_crash(...)               # 3-strike 崩溃契约
```

#### `engine/console.py` — `OpsConsole`（run() 外运维接缝）

```python
class OpsConsole:
    def __init__(self, backend, store) -> None
    def list_dlq(self) -> List[DLQEntry]
    def clear_dlq(self, task_types=None, *, keep_fatal=True) -> int
    def clear_history(self, targets, *, where=("wall", "failed")) -> int
    def seed_wall(self, uids) -> int
    def seed_cursor(self, key, value) -> None
```

#### `engine/channel.py` — `ExecutionChannel` + `WorkerLaunchSpec`

```python
@dataclass(frozen=True)
class WorkerLaunchSpec:
    """进程 seam 具名契约——取代位置参数元组。"""
    handler: Callable; job: Job; task_ctx: TaskContext
    incarnation: str; ipc_dir: str

class ExecutionChannel:
    def spawn(self, spec: WorkerLaunchSpec) -> JobHandle
    def reap_completed(self, handles) -> List[Tuple[JobHandle, ExecutionResult]]
    def probe_orphan_lock(self, uid) -> bool
    def claim_stale_result(self, uid, job) -> Optional[ExecutionResult]
    def drain_active_signals(self, uids) -> List[Tuple[str, str, float]]
    def abort_in_flight(self, handles) -> AbortOutcome
    def cleanup_artifacts(self, uid, *, mode: ArtifactCleanupMode) -> None
    def read_declared_inputs(self, uid) -> List[dict]
```

#### 已收敛深模块（interface 冻结，内部演进照常）

| 模块 | 冻结的核心 interface |
|---|---|
| `scheduler.py` | `JobScheduler.pop_next_runnable` / `begin_round` / `cached_job`；`JobFacts` / `ScheduleResult` / `DeadlockAttribution` |
| `governor.py` | `DeadlockGovernor.arbitrate` / `resolve_deadlock` / `reset`；`DeadlockDecision`；常量 `DEP_GRACE_SECONDS` / `DEADLOCK_GAP_MAX_ROUNDS` |
| `policy.py` | `ExecutionPolicy.admit` / `evaluate` / `plan_retry` / `compute_backoff_schedule`；`PreflightDecision` / `RetryPlan` / `BackoffSchedule` |
| `inflight.py` | `InFlightTracker.track / register / dispatch / settle / unregister / active_handles`；`InFlightJob`（租约生命周期内聚） |
| `resource.py` | `ResourceManager`（MutableMapping 语义 + 挂起收集）；`Resource` / `RateLimitResource` / `CapacityResource`；`ResourceLease` / `NullResourceLease`（两阶段租约） |

### 4.3 模型层

| 模块 | 职能 | interface 要点 |
|---|---|---|
| `models/job.py` | Job 领域数据 + 边带运行态 | `Job`（uid / deps / resources / rerun 哨兵）、`JobRuntimeState`（退避 / strike 计数）；**`WORKER_RESOURCE` 唯一定义点** |
| `models/context.py` | 子进程侧 handler API（纯） | `TaskContext.spawn / declare_output / declare_cache / declare_input(_uri) / is_completed / is_failed / get_cursor / set_cursor / suspend_resource`；`incarnation` 字段与 `attempted_uids()` 不存在（归 `WorkerLaunchSpec` 与 discovery adapter）；journal 构造时缓存 |
| `models/state.py` | StateStore 私有实现细节 | `PipelineState` 六集合容器 + 一致性断言；外部只经 StateStore 受控方法访问 |

### 4.4 持久层与工具层

| 模块 | 职能 |
|---|---|
| `backend/base.py` | `AbstractStateBackend`：delta commit 抽象（load_* / commit_* / enqueue_jobs / meta / seed_*）——**双 adapter 真 seam** |
| `backend/sqlite_backend.py` | WAL + `BEGIN IMMEDIATE` ACID adapter |
| `backend/memory.py` | 零 IO 快照隔离 adapter（测试专用） |
| `utils/ipc.py` | `ArtifactJournal`：产物清单、两级降级落盘、残留认领、IPC 生命周期 |
| `utils/injective.py` | `InjectiveEncoder`：可逆 `%XX` 单射转义与指纹 |
| `utils/lockfile.py` | 跨平台文件锁（含单射转义关键点） |
| `wrappers/http/` | 按概念拆包：`policy`（HttpPolicy）/ `guard`（http_guard）/ `cookies` / `snapshot`（SnapshotStore 族）/ `fetch`（HttpExecutor）；包导出面保持 `tasklite.wrappers.http` 不变（ADR-0001） |
| `wrappers/discovery.py` | Discovery 需求契约 + 宿主 adapter（消费 wall/failed 快照，不要求 TaskContext 定制方法） |

---

## 五、接缝清单与不变式契约

### 具名接缝（seam 及其契约，改动须对齐 ADR-0002 D3/D4）

| seam | 契约 | adapter |
|---|---|---|
| 进程边界 | `WorkerLaunchSpec`（frozen 值对象） | 真 `Process` / 测试 FakeProcess |
| 持久化 | `AbstractStateBackend` | SQLite / InMemory |
| 资源语义 | `Resource` 抽象 | `RateLimitResource` / `CapacityResource` |
| HTTP 快照 | `SnapshotStore` 接口 | 目录版 / 单文件版等（见 ADR-0001） |
| 状态转移 | `StateStore.apply_*`（唯一出口，非 Protocol） | 单实现，无假想 Protocol |

### 不变式契约

1. **Incarnation 身份隔离**：结果文件名携带 `{run_id}.{dispatch_seq}`，父进程只消费
   当前 incarnation 的结果，隔离孤儿进程写入；代次经 `WorkerLaunchSpec` 下发。
2. **六集合全局互斥**：`queue`、`wall`、`failed`、`in_flight`、`queue_uids`、
   `_rerun_active_uids` 严格互斥且终态单一。
3. **TOCTOU 闭环契约**：`abort_in_flight` 坚持「分类 → 终止未完成 → 重新探测 →
   安全清理」，并发写入的结果不被误删。
4. **单射性编码约定**：业务标识派生路径/复合 UID 一律可逆 `%XX` 百分号转义，消除
   多对一碰撞（wall 去重静默吞任务防线）。
5. **持久化崩溃安全**：SQLite `WAL + FULL`，读-改-写严格 `BEGIN IMMEDIATE` 显式
   写事务。
6. **瞬态信号军规**：`lock_conflict` / `interrupted` / `RateLimitHit` 等瞬态信号
   「不烧重试预算 + 降级写盘 + 零污染」。

---

## 六、现状偏差与迁移基线

> 本章是终态与当前工作树的**受控偏差登记表**；每完成一步即在表中勾销并原子提交。
> 迁移纪律（一步到位 / 两份真相禁令 / 反转留痕）见 ADR-0002 D5。

| # | 偏差 | 终态 | 迁移步骤（原子提交序列） |
|---|---|---|---|
| 1 | `engine/types.py` 零导入死代码，`runtime.py:23-160` 重复定义 | types.py 单一真相 | **S1** 完成 re-export 迁移 + `HandlerEntry` 迁入 + `WORKER_RESOURCE` 收敛 `models/job.py` |
| 2 | `RunContext` 全局可变袋（20+ 字段、三机器整袋构造、StateStore `getattr` 回查） | `RunConfig` + `RunSession` | **S2** `RunConfig.resolve` 默认值唯一化；**S3** `RunSession` 抽取后删除 RunContext |
| 3 | 机器以 ctx 构造，View Protocol 三件（单 implementor） | 显式窄依赖，删 Protocol | **S4** 机器构造签名改造 + 测试改直接装配 |
| 4 | `exit_reason` 三处推导；`step()` 158 行含 6 分支等待 if-elif | `session.exit_reason()` + `decide_wait` | **S5** 表驱动单测先行锁定语义，再瘦编排 |
| 5 | spawn 位置参数 `(handler, job, ctx, ipc_dir)`；incarnation 借道 TaskContext | `WorkerLaunchSpec` | **S6** channel + tests/helpers 同提交切换 |
| 6 | StateStore 混运维管理（list_dlq 等 ~150 行） | `OpsConsole` 拆分 | **S7** 门面管理 API 改委托 |
| 7 | `wrappers/http.py` 单文件 1028 行五概念 | `wrappers/http/` 按概念拆包 | **S8** 独立可做，导出面不变 |
| 8 | 默认值三层解析；TaskLite 持运行态可写属性 | 门面只透传 Optional | S2 一并完成 |

完成 S1–S8 后本章仅保留历史记录，终态即现状。
