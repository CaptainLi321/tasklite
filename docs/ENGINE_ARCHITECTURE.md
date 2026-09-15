# tasklite 引擎架构总览与设计手册

> 模块路径：`tasklite.engine`
> 本文为 **ADR-0002 固化的终态架构基线**（防漂移规范，经独立评审修订，见 ADR 修订记录）；
> 现状与终态的差异见 [第六章·现状偏差与迁移基线](#六现状偏差与迁移基线)。
> 适用版本：v1.2.0+（终态架构迁移 S1–S8 已全部落地，终态即现状）

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
  唯一经 `StateStore.apply_failure` / `apply_failed`（wall / failed 互斥）；状态转移
  唯一出口族为六个 `apply_*`（见 §4.2）；运行钩子唯一经 `RunSession.fire_*`；
  `exit_reason` 推导唯一经 `RunSession.exit_reason()`；等待决策唯一经
  `pacing.decide_wait`。
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

机器间依赖方向（唯一允许的形状，C3 裁决）：

```
Dispatch ──► Completion ◄── Recovery        （completion 不反向依赖任何机器）
```

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
  `RunSession.exit_reason(exc)` 唯一推导，语义为**异常类型优先**：
  `KeyboardInterrupt → INTERRUPTED`（即使信号 handler 已把 stop_mode 置为
  DRAINING/ABORTING——现状 `execute` / `_run_loop` 即此语义）；其余非 None 异常 →
  `ERROR`；无异常按 stop_mode 三态。execute / step / 主循环三处共用同一实现，
  禁止另写分支。

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
> 机器依赖以 §4.2.9 依赖矩阵为可验证依据，矩阵变更须修订 ADR-0002。

### 4.1 门面层

#### `pipeline.py` — `TaskLite`（用户唯一入口，六步标准调用顺序）

```python
class TaskLite:
    def __init__(name, state_dir, backend="sqlite", output_root=None, max_workers=4,
                 on_run_start=None, on_run_end=None, on_job_completed=None,
                 strict_picklable=False, fatal_exceptions=None, transient_exceptions=None,
                 dep_grace_seconds=None, commit_failure_dlq_threshold=None,
                 deadlock_gap_max_rounds=None)   # Optional 一律透传，不做默认值解析
    def add_resource(self, resource: Resource) -> None            # run 期守卫
    def register_handler(self, task_type, handler_func,
                         default_resources=None, payload_schema=None) -> None  # run 期守卫
    def set_discovery_rerun(self, task_type, rerun) -> None       # run 期守卫
    def register_transient_exception(self, exception_cls: type) -> None        # run 期守卫
    def enqueue(self, jobs, front=False) -> None                  # run 期守卫
    def run(self) -> None            # 入口拍 RunConfig.resolve(...) 快照 → EngineRuntime
    def stop(self, force=False) -> None
    def run_graceful(self) -> None
    # 管理 API（run() 外专用，守卫 _ensure_not_running 留在门面）：
    def list_dlq / clear_dlq / clear_history / seed_wall / seed_cursor  # 委托 OpsConsole
```

门面红线：不解析默认值、不持有运行态可写属性、run() 期间仅响应 `stop()`。
用户工具函数（`job_ref` / `progress_hook` / `slice_list`）不放在门面文件。

**门面属性去留表**（现状外泄属性的处置记录）：

| 属性 | 处置 |
|---|---|
| `is_running` | 保留 |
| `backend`（只读） | 保留（只读+setter：崩溃注入测试接缝，`tests/engine/test_crash_recovery_regressions.py` 依赖热切换） |
| `stats`（只读） | 保留；setter 删除 |
| `store` / `scheduler` / `governor` / `channel` / `state` / `in_flight` | 已于 1.2.0 移除（零外部消费方，弃用窗口豁免）；深模块经 `pipeline._runtime` 直达 |
| 钩子三件套（`on_run_start` 等）setter | 已于 1.2.0 移除（零外部消费方，弃用窗口豁免）；钩子仅构造期参数，属性只读 |

### 4.2 引擎层

#### 4.2.1 `engine/types.py` — 引擎公共值对象（零内部依赖叶子，单一真相源）

```python
StopMode(Enum)             # NONE / DRAINING / ABORTING
ExitReason(str, Enum)      # COMPLETED / STOPPED_DRAINING / STOPPED_ABORTING / INTERRUPTED / ERROR
TaskStats(dict)            # 9 计数器 + 只读属性
EMPTY_STATS: Mapping[str, int]
ExecutionOptions           # frozen: install_signals / acquire_run_lock
StepOutcome                # frozen: 单步事件泵产物
RunSummary                 # frozen: 终局摘要
HandlerEntry(NamedTuple)   # func / default_resources / payload_schema
```

#### 4.2.2 `engine/config.py` — `RunConfig`（静态装配快照 + 默认值唯一落点）

```python
@dataclass(frozen=True)
class RunConfig:
    name: str; ipc_dir: str
    backend: AbstractStateBackend
    governor: DeadlockGovernor     # 构造期即建（StateStore 结算依赖 + 主循环仲裁共用）
    policy: ExecutionPolicy        # 构造期即建（内部持有 discovery_rerun 共享引用）
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
    # 另有 resolve_tuning()：三参数调优标量的已解析形态，供 governor 等
    # 前置构造场景与 resolve 共用同一真相源
```

StateStore **不随快照携带**——由 EngineRuntime 构造期创建（其
`on_job_completed` 回调绑定 `RunSession.fire_job_completed`，钩子后置变更即时
生效；`enqueue()` 与 OpsConsole 经门面 `store` 属性在 run 前可用）。

**装配时序（三段式，frozen 的真实语义）**：

1. **构造期**（`TaskLite.__init__`）：backend、ResourceManager、ErrorTaxonomy、
   ExecutionChannel、governor、policy 即行构造——随后经 `RunConfig.resolve()`
   一次性装配并**持久构造 EngineRuntime**（StateStore 于其构造期创建）；
2. **装配期**（构造后 → 首次 run() 前，以及相邻 run() 之间）：装配 API 生效于共享
   注册表（`handlers` / `discovery_rerun` 按引用共享，变更对机器可见），全部带
   run 期守卫；
3. **run 期**：**冻结的是引用而非拷贝**——`resources` 含挂起时刻与 used 计数，
   本就不可深拷贝；内容不变性由「run 期守卫禁止装配 API」独立保证。每次
   `run()`（`execute()`）经 `RunSession.begin()` 复位会话（持久会话实例，
   语义等价于每次新建：run_id / dispatch_seq / stats / stop_mode 归零，store
   记账切换至新 stats），run 结束后装配期重新开放。

#### 4.2.3 `engine/session.py` — `RunSession`（单次 run 生命周期状态 + 钩子单一出口）

```python
class RunSession:
    run_id: Optional[str]; dispatch_seq: int
    stop_mode: StopMode; stats: TaskStats      # 无 public setter
    def begin(self, run_id: Optional[str] = None) -> None  # 新 run 复位：全部生命周期状态归零
    def next_dispatch_seq(self) -> int         # fence 序号单调递增
    def request_stop(self, force: bool) -> StopMode   # 单调状态机唯一入口
    def exit_reason(self, exc: Optional[BaseException] = None) -> ExitReason
        # 唯一推导实现：KeyboardInterrupt→INTERRUPTED；其余异常→ERROR；
        # 无异常按 stop_mode 三态。覆盖现状全部 22 处推导点。
    def fire_run_start(self) -> None
    def fire_job_completed(self, uid, meta, success, going_to_retry) -> None
    def fire_run_end(self, reason: str) -> None         # 幂等，只发一次
```

禁止复活物：governor 代理属性、`episode` 双名、`stats` setter 双写、`in_flight`
setter 测试后门、`state` / `backend` 可写属性（ADR-0002 D1）。

#### 4.2.4 `engine/pacing.py` — 等待决策纯函数

```python
@dataclass(frozen=True)
class LoopFacts:        # 一拍事件泵的纯数据快照（十字段）
    stop_mode: StopMode; has_in_flight: bool; store_empty: bool
    dispatched: int; completed: int
    has_runnable: bool         # 本拍是否存在调度候选；draining 无派发时为 False
    min_wait: float            # 候选最早可运行时刻；inf = 无候选，或死锁成因被强制置 inf
    worker_wait: float         # 资源挂起恢复时刻，按 min 聚合（见下）
    deadlock_wait: float       # governor 宽限/gap 裁决等待
    should_terminate: bool     # governor 仲裁的独立终止信号（区别于 is_idle）

@dataclass(frozen=True)
class WaitDecision:
    should_wait: bool; wait_time: float

def decide_wait(facts: LoopFacts) -> WaitDecision   # 纯函数，表驱动单测锁定
```

语义裁定：`worker_wait` 聚合采用 **min**（现状为 last-write-wins，属行为变更，迁移
时提交信息须留痕）；`min_wait` 的 inf 为双语义（无候选 / 死锁强制），表驱动测试须
覆盖两种。

#### 4.2.5 `engine/runtime.py` — `EngineRuntime`（装配 + 薄事件泵）

```python
class EngineRuntime:
    def __init__(self, config: RunConfig) -> None   # 内部装配机器群，不外泄 ctx
    def execute(self, options: Optional[ExecutionOptions] = None) -> RunSummary
        # 锁 / 信号 / 钩子 / 异常承重网；exit_reason 一律委托 session.exit_reason(exc)
    def step(self, max_dispatch: Optional[int] = None) -> StepOutcome  # 主体 <60 行已兑现（58 行）
    def request_stop(self, force: bool = False) -> StopMode            # 委托 session
    @property is_running / stats / stop_mode / store
```

`step()` 顺序管道：停机门（ABORTING / 排空完毕 / 空闲完成三个早退统一走
`_terminal_outcome()`）→ 填池派发（`_fill_dispatch_pool`，worker_wait 聚合取 min）
→ 死锁仲裁（`_arbitrate_deadlock`，仅无 in-flight 时）→ 回收结算
（`_drain_and_settle`）→ `decide_wait` → `StepOutcome`。

#### 4.2.6 三台机器 — 显式窄依赖构造（禁止 Context 整袋）

```python
class DispatchMachine:
    def __init__(self, *, store, scheduler, policy, resources, channel, in_flight,
                 session, completion, handlers, taxonomy, output_root, ipc_dir,
                 commit_failure_dlq_threshold) -> None
    def dispatch_next(self) -> DispatchOutcome        # 五关预检 + spawn

class CompletionMachine:
    def __init__(self, *, store, policy, channel, resources, in_flight,
                 session) -> None
    def complete_job(self, entry: InFlightJob, result: ExecutionResult) -> None
        # Job 终结唯一出口
    def settle_reaped(self, completed) -> int
    def settle_aborted(self, handles) -> None

class RecoveryMachine:
    def __init__(self, *, store, channel, resources, in_flight, policy,
                 completion) -> None
    def repair_queue_on_load(...) / load_resource_suspends() / persist_resource_suspends()
    def save_queue_crash_safe() / apply_pending_signals() / abort_in_flight()
```

**backend 活引用裁定**：两机器不持有 backend 字段，一律经 `store.backend`
读取——`TaskLite.backend = new_backend` 热切换（崩溃注入场景）对机器即时可见，
不产生陈旧引用。

**资源挂起持久化的归属（C2 裁决）**：收敛为 `resource.py` 模块级函数
`persist_resource_suspensions(backend, resource_mgr)`，completion 与 recovery 共用，
**不新增机器间耦合**（completion 的 backend 依赖仅为此用途与终局收尾）。

#### 4.2.7 `engine/store.py` — `StateStore`（入队规范化 + 转移事务 + 3-strike）

```python
class StateStore:
    def __init__(self, backend, *, commit_failure_dlq_threshold, taxonomy, governor,
                 policy, stats, on_job_completed) -> None   # 显式依赖，禁止 getattr 回查
    def enqueue_jobs(self, jobs, *, front=False) -> List[str]   # 返回插入的 UID；规范化 + wall 去重
    @property state / is_empty
    wall_uids() / failed_uids() / queue_uids() / in_flight_uids() / cursors()
    pop_job(idx) / requeue_jobs(job_dicts, *, front)
    register_in_flight(uid) / unregister_in_flight(uid) / clear_in_flight()
    # 结算：状态转移唯一出口族（六个；wall / failed 互斥在此保证）
    def apply_success(...) -> SuccessOutcome
    def apply_failure(...) -> FailureOutcome
    def apply_failed(...) -> FailureOutcome        # 内部失败登记路径（红线 6 点名）
    def apply_retry(...) -> RetryOutcome
    def apply_skip(...) -> SkipOutcome
    def apply_bulk_failure(...)                    # 死锁批量熔断（governor 调用）
    def cascade_fail(uid) -> List[str]
    def commit_failed_crash(...)                   # 3-strike 崩溃契约
```

#### 4.2.8 `engine/console.py` — `OpsConsole`（run() 外运维接缝）

```python
class OpsConsole:
    def __init__(self, backend, store, taxonomy) -> None   # list_dlq 分类依赖 taxonomy
    def list_dlq(self) -> List[DLQEntry]
    def clear_dlq(self, task_types=None, *, keep_fatal=True) -> int
    def clear_history(self, targets, *, where=("wall", "failed")) -> int
    def seed_wall(self, uids) -> int
    def seed_cursor(self, key, value) -> None
```

拆分前置：给 `PipelineState` 补公共 discard / add 方法，禁止把直触
`_failed_uids` / `_wall_uids` 私有属性原样搬入。

#### 4.2.9 机器 × 依赖矩阵（签名的可验证依据，变更须修订 ADR-0002）

| 依赖 | Dispatch | Completion | Recovery |
|---|:-:|:-:|:-:|
| `store` | ✓ | ✓ | ✓ |
| `scheduler` | ✓ | | |
| `policy` | ✓ | ✓ | ✓ |
| `resources` | ✓ | ✓ | ✓ |
| `channel` | ✓ | ✓ | ✓ |
| `in_flight` | ✓ | ✓ | ✓ |
| `session` | ✓ | ✓ | |
| `completion`（机器） | ✓ | | ✓ |
| `handlers` | ✓ | | |
| `taxonomy` | ✓ | | |
| `output_root` / `ipc_dir` | ✓ | | |
| `commit_failure_dlq_threshold` | ✓ | | |
| `backend`（活引用） | 经 store.backend | 经 store.backend | 经 store.backend |

#### 4.2.10 `engine/channel.py` — `ExecutionChannel` + `WorkerLaunchSpec`

```python
@dataclass(frozen=True)
class WorkerLaunchSpec:
    """进程 seam 具名契约——取代位置参数元组（含 timeout，防其漏回位置约定）。"""
    handler: Callable; job: Job; task_ctx: TaskContext
    incarnation: str; ipc_dir: str; timeout: Optional[float]

class ExecutionChannel:
    def spawn(self, spec: WorkerLaunchSpec) -> JobHandle
    def reap_completed(self, handles) -> List[Tuple[JobHandle, ExecutionResult]]
    def probe_orphan_lock(self, uid) -> bool
    def claim_stale_result(self, uid, job) -> Optional[ExecutionResult]
    def drain_active_signals(self, uids) -> List[Tuple[str, str, float]]
    def abort_in_flight(self, handles) -> AbortOutcome
    def cleanup_in_flight(self, handles) -> None
    def cleanup_artifacts(self, uid, *, mode: ArtifactCleanupMode) -> None
    def read_declared_inputs(self, uid) -> List[dict]
```

方法主名单一定（ADR-0002 D2）：`spawn` / `reap_completed` / `claim_stale_result` /
`cleanup_in_flight`——现存别名 `submit` / `poll_completed` / `consume_stale_result` /
`cleanup` 在迁移中删除。子进程执行体从 `spec.incarnation` 取执行身份（现状经
`getattr(ctx, "incarnation")`）。

#### 4.2.11 已收敛深模块（interface 连签名形状一起冻结）

| 模块 | 冻结的核心 interface |
|---|---|
| `scheduler.py` | `JobScheduler.pop_next_runnable` / `begin_round` / `cached_job`；`JobFacts` / `ScheduleResult` / `DeadlockAttribution` |
| `governor.py` | `DeadlockGovernor.arbitrate` / `resolve_deadlock` / `reset`；`DeadlockDecision`；常量 `DEP_GRACE_SECONDS` / `DEADLOCK_GAP_MAX_ROUNDS`。**冻结含形状**：`arbitrate` 的 `**kwargs` 与 `getattr(store, ...)` 回查在机器窄依赖化时一并清除，防止再成「收窄接缝」提交的种子 |
| `policy.py` | `ExecutionPolicy.admit` / `evaluate` / `plan_retry` / `compute_backoff_schedule`；`PreflightDecision` / `RetryPlan` / `BackoffSchedule` |
| `inflight.py` | `InFlightTracker.track / register / dispatch / settle / unregister / active_handles`；`InFlightJob`（租约生命周期内聚） |
| `resource.py` | `ResourceManager`（MutableMapping 语义 + 挂起收集 + `persist_resource_suspensions` 模块函数）；`Resource` / `RateLimitResource` / `CapacityResource`；`ResourceLease` / `NullResourceLease`（两阶段租约） |

### 4.3 模型层

| 模块 | 职能 | interface 要点 |
|---|---|---|
| `models/job.py` | Job 领域数据 + 边带运行态 | `Job`（uid / deps / resources / rerun 哨兵）、`JobRuntimeState`（退避 / strike 计数）；**`WORKER_RESOURCE` 唯一定义点** |
| `models/context.py` | 子进程侧 handler API（纯） | `TaskContext.spawn / declare_output / declare_cache / declare_input(_uri) / is_completed / is_failed / get_cursor / set_cursor / suspend_resource`；`incarnation` 字段不存在（归 `WorkerLaunchSpec`）；`attempted_uids()` **保留**为子进程侧只读 API——子进程内 TaskContext 是唯一状态视图，DiscoveryContext 协议（`wrappers/discovery.py`）消费 wall∪failed 快照；journal **构造时缓存一次**（现状为 property 每次新建，迁移收敛） |
| `models/state.py` | StateStore 私有实现细节 | `PipelineState` 六集合容器 + 一致性断言 + 公共 discard / add（OpsConsole 前置）；外部只经 StateStore 受控方法访问 |

### 4.4 持久层、工具层与独立模块

| 模块 | 职能 |
|---|---|
| `backend/base.py` | `AbstractStateBackend`：delta commit 抽象（load_* / commit_* / enqueue_jobs / meta / seed_*）——**双 adapter 真 seam** |
| `backend/sqlite_backend.py` | WAL + `BEGIN IMMEDIATE` ACID adapter |
| `backend/memory.py` | 零 IO 快照隔离 adapter（测试专用） |
| `tasklite/taxonomy.py` | `ErrorTaxonomy`：错误三分类 + 瞬态注册的**全引擎单一真相**；`validate_resource_amounts` 入口校验 |
| `tasklite/exceptions.py` | 异常层次叶子：三分类异常 + 承重网信号（`_CommitCrashSignal` / `_JobTerminated`，继承 `BaseException` 的理由见 lockfile 级注释纪律） |
| `utils/ipc.py` | `ArtifactJournal`：产物清单、两级降级落盘、残留认领、IPC 生命周期 |
| `utils/injective.py` | `InjectiveEncoder`：可逆 `%XX` 单射转义与指纹 |
| `utils/lockfile.py` | 跨平台文件锁（含单射转义关键点） |
| `utils/jsonutil.py` | 序列化落盘格式（dumps / loads） |
| `wrappers/http.py` | 组合式 HTTP 积木（HttpPolicy / http_guard / cookies / SnapshotStore 族 / fetch），概念正交性由 ADR-0001 裁决；**物理拆包为基线外可选项**，须自带 ADR 与 pickle 兼容别名（见 §6 S8） |
| `wrappers/discovery.py` | Discovery 需求契约 + 宿主 adapter（消费 wall/failed 快照，不要求 TaskContext 定制方法） |

---

## 五、接缝清单与不变式契约

### 具名接缝（seam 及其契约，改动须对齐 ADR-0002 D3/D4）

| seam | 契约 | adapter |
|---|---|---|
| 进程边界 | `WorkerLaunchSpec`（frozen 值对象，含 timeout） | 真 `Process` / 测试 FakeProcess（解 spec 字段，禁止位置反解） |
| 持久化 | `AbstractStateBackend` | SQLite / InMemory |
| 资源语义 | `Resource` 抽象 | `RateLimitResource` / `CapacityResource` |
| HTTP 快照 | `SnapshotStore` 接口 | 目录版 / 单文件版等（见 ADR-0001） |
| 状态转移 | `StateStore.apply_*` 六出口族（唯一，非 Protocol） | 单实现，无假想 Protocol |

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
7. **产物清理沙盒复检**：`.outputs.jsonl` 属 ipc_dir 上的不可信输入，删除动作
   （unlink/rmtree）前必须复检路径归属（解析符号链接与 `..` 后须落在
   `output_roots ∪ ipc_dir` 内），越界路径拒绝清理、仅告警（fail-safe：漏删可补救，误删不可逆）。

---

## 六、现状偏差与迁移基线

> 本章是终态与当前工作树的**受控偏差登记表**；每完成一步即在表中勾销并原子提交。
> 迁移纪律（一步到位 / 两份真相禁令 / 反转留痕）见 ADR-0002 D5。
> 序列经独立评审重排：**机器窄依赖化（S3）必须先于 RunContext 删除（S4）**，
> 否则三机器的 `self._ctx` 在 S4 处悬空。

| 步骤 | 偏差（现状证据） | 终态动作 | 备注 |
|---|---|---|---|
| **S1** | `engine/types.py` 零导入死代码 vs `runtime.py:23-160` 重复定义；`WORKER_RESOURCE` 三处定义；死参数 `executor`（runtime.py:376）与死别名 `episode` | types.py 单一真相 + re-export 兼容；`HandlerEntry` 迁入；常量收敛 `models/job.py`；死参数/死别名清除 | 机械、零风险，先行 |
| **S2** | 默认值三层解析，且 `deadlock_gap_max_rounds` 双默认值冲突（TaskLite 字面量 3 vs governor 常量 5，实际生效 3） | `RunConfig.resolve()` 唯一化，**以 governor 常量为准**；TaskLite 全部透传 Optional | 冲突收敛属行为变更，提交信息留痕 |
| **S3** | 三机器以 ctx 整袋构造；`DispatchView`/`CommitView`/`RecoveryView`（有注解方 dispatch.py:155/203、scheduler.py:224 与兼容测试 test_state_store.py:212-252）；`ExecutionChannelProtocol`（单 implementor 零注解） | 机器改显式窄依赖（§4.2.6 签名与 §4.2.9 矩阵）；四个 Protocol 连同注解方与兼容测试**同一提交**删除；governor.arbitrate 的 `**kwargs`/getattr 回查一并清除 | 可按机器拆 3–4 个原子提交；测试改为直接装配 |
| **S4** | `RunContext` 全局可变袋（26 公共字段 + 6 可写 setter 代理） | `RunSession` 抽取（含 `exit_reason(exc)` 新签名）+ `RunConfig` 装配时序落地（store / governor / policy 构造期即建并随 RunConfig 携带），**单提交删除 RunContext** | 前置于本步的 S3 已完成；大提交本身是单一语义单元，不违 D5-1 |
| **S5** | `exit_reason` 22 处推导点；`step()` 158 行含 6 分支等待 if-elif | `decide_wait` 纯函数 + LoopFacts 十字段；表驱动测试先行锁定（含「二次信号后 KeyboardInterrupt」「stop() 后正常排空」两 case 与 `worker_wait` min 聚合的行为变更） | pacing 作 S4 伴生收敛，非独立架构目标 |
| **S6** | spawn 位置参数 `(handler, job, ctx, timeout, …, ipc_dir=…)`；incarnation 借道 TaskContext；channel 4 组方法别名；journal property 每次新建 | `WorkerLaunchSpec`（含 timeout）落地 + 别名删除 + incarnation 归 spec + journal 构造缓存（`attempted_uids` 经勘察裁定**保留**为 TaskContext 只读 API，见 §4.3）；`tests/helpers.py`、`tests/hygiene/test_fake_infra_guard.py`（AST 布局守卫）、`tests/backend/test_pipeline_backend.py` 同提交改造 | 进程 seam 一步到位 |
| **S7** | StateStore 混运维 ~150 行（list_dlq 依赖 taxonomy；clear/seed 直触 `_failed_uids`/`_wall_uids`） | `OpsConsole(backend, store, taxonomy)` 拆出；`PipelineState` 补公共 discard/add；门面管理 API 改委托 | — |
| **S8** | 门面外泄属性与钩子 setter；README/API_GUIDE 未同步新概念 | 按门面属性去留表执行 deprecation；`job_ref`/`progress_hook`/`slice_list` 迁出；README / API_GUIDE / CONTEXT.md（RunConfig/RunSession/WorkerLaunchSpec/OpsConsole/pacing 词条）同步 | 文档与兼容面收尾 |
| **S9**（可选，基线外） | `wrappers/http.py` 1028 行五概念 | 物理拆包 | **须先有独立 ADR**（ADR-0001 未裁决拆包）+ pickle 兼容别名；触发条件：新增第六概念或子模块需独立演化；无触发则不做 |

**S1–S8 已全部落地（a97e3ad → 本次文档提交），本章转为历史记录，终态即现状**；原「完成 S1–S8 后本章仅保留历史记录，终态即现状」之约定就此兑现。
