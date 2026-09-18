# 0002 运行期终态架构：装配快照与生命周期会话分离（防漂移基线）

## 上下文与决策动因

v1.1.0 之后 4 天内落了 91 个深模块化提交（日均 23 个），对 git 史的量化审计显示出
**系统性反复修改**，而非按图施工的收敛：

1. **同一接缝反复开刀**：TaskLite 门面瘦身 ×8、EngineRuntime 收敛 ×6、
   ExecutionChannel ×5、ArtifactJournal 收敛 ×3；`InFlightTracker` 的同一句提交信息
   逐字出现两次（相隔两天，第一趟未做完）；
2. **写入代码的高周转**：+14,674 / −8,287 行，代码净增长仅 ~3,000 行——窗口内写下的
   代码 56% 被同窗口重写；7 个提交的标题即「消除……垫片」，垫片的产生与消亡同在
   窗口内，属自致性摩擦；
3. **无记录的反转**：`pipeline_util.py` 删除 → 27 分钟后以「下游兼容」恢复 → 次日再
   删除，两次删除均无决策记录说明兼容约束如何消解；
4. **两份真相**：`engine/types.py` 搬迁半途而废（零导入死代码），`runtime.py` 保留
   逐行重复定义；`WORKER_RESOURCE` 在三处各自定义。

根因诊断：**每趟改造没有先固定的目标 interface**——落一趟、发现下一层浅、再开一刀；
而 `RunContext`（20+ 公共可变字段的共享袋，三台机器以它为唯一构造参数，StateStore
构造时以 `ctx` 传引用并 `getattr` 回查）是编排层反复返工的共同病根，测试被迫穿透
`_runtime.ctx.*` 直改内部状态（宽口径 47 处、分布 16 个测试文件）。

**裁决**：先立法、后动刀。本 ADR 固定运行期终态契约；后续一切重构以本 ADR 与
`docs/ENGINE_ARCHITECTURE.md` 为准，**偏离契约须先修订本 ADR 再动代码**。

---

## 架构决策

### D1. 静态装配与生命周期状态彻底分离（RunContext 退场）

- **`RunConfig`**（frozen dataclass）：TaskLite 构造期经 `resolve()` 装配的不可变
  快照（**冻结的是字段引用**；`governor` / `policy` 亦构造期即建并随快照携带；
  StateStore 由 EngineRuntime 构造期创建——其 on_job_completed 回调需绑定
  RunSession，随会话存活）；`RunConfig.resolve()` / `resolve_tuning()`
  是全部调优参数默认值的**唯一解析点**；
- **`RunSession`**：一次 `run()` 唯一的可变状态容器（`run_id` / `dispatch_seq` /
  `stop_mode` / `stats`），`fire_*` 钩子单一出口，`exit_reason()` 唯一推导实现；
- Dispatch / Completion / Recovery 三台机器以**显式窄依赖**构造，依赖清单以
  「逐方法核对真实使用」为准（见手册 §4.2 依赖矩阵，矩阵变更须修订本 ADR）；
  **禁止任何「Context 整袋」构造参数形态复活**；
- `StateStore` 显式收依赖（backend / commit_failure_dlq_threshold / taxonomy /
  governor / policy / stats / on_job_completed），**禁止以 `ctx` 传引用 +
  `getattr` 回查调用方**。

**装配时序（三段式，frozen 的真实语义）**：

1. **构造期**（`TaskLite.__init__`）：backend、ResourceManager、ErrorTaxonomy、
   ExecutionChannel、StateStore（含 policy / governor）即行构造——`enqueue()` 与
   OpsConsole 在 run() 之前就可用；随后经 `RunConfig.resolve()` 一次性装配并
   **持久构造 EngineRuntime**；
2. **装配期**（构造后 → 首次 run() 前，以及相邻 run() 之间）：`add_resource` /
   `register_handler` / `set_discovery_rerun` / `register_transient_exception`
   生效于共享注册表（按引用共享，变更对机器可见）；**全部装配 API 必须带 run 期
   守卫**（run 期间抛 RuntimeError）；
3. **run 期**：**冻结的是引用而非拷贝**（resources 含挂起时刻与 used 计数，本就
   不可深拷贝）；内容不变性由「run 期守卫禁止装配 API」独立保证。每次 `run()`
   （`execute()`）经 `RunSession.begin()` 复位会话（持久会话实例，语义等价于
   每次新建——机器持有的 session 引用跨 run 稳定），run 结束后装配期重新开放。

### D2. 单一真相源清单

| 真相 | 唯一落点 |
|---|---|
| 引擎公共值对象（StopMode / ExitReason / TaskStats / StepOutcome / RunSummary / ExecutionOptions / HandlerEntry） | `engine/types.py`（零内部依赖叶子模块） |
| `WORKER_RESOURCE` 常量 | `models/job.py` |
| `exit_reason` 推导 | `RunSession.exit_reason(exc: Optional[BaseException] = None)`——异常类型优先（KeyboardInterrupt→INTERRUPTED、其余异常→ERROR），无异常按 stop_mode 三态；覆盖现状全部 22 处推导点 |
| 等待/空闲决策 | `pacing.decide_wait(LoopFacts) -> WaitDecision` 纯函数（LoopFacts 十字段，含 governor 终止信号 `should_terminate`） |
| 参数默认值 | `RunConfig.resolve()`（现存 `deadlock_gap_max_rounds` 双默认值冲突——TaskLite 字面量 3 vs governor 常量 5——以 governor 常量为准收敛） |
| channel 方法主名 | `spawn` / `reap_completed` / `claim_stale_result` / `cleanup_in_flight`——现存 4 组别名（submit、poll_completed、consume_stale_result、cleanup）收敛删除 |
| 状态转移出口 | `StateStore.apply_success / apply_failure / apply_failed / apply_retry / apply_skip / apply_bulk_failure`（六个，含批量熔断与内部失败登记路径） |

`runtime.py` 对旧符号的 re-export 仅作过渡兼容垫片，随版本移除。

### D3. 具名 seam 契约

- **进程边界**：唯一契约 `WorkerLaunchSpec(handler, job, task_ctx, incarnation,
  ipc_dir, timeout)`——**含 timeout**（JobHandle.deadline 与瞬态超时判定依赖它，
  现状为 spawn 第 4 个位置参数，不进 spec 就会漏回位置约定）；`incarnation` 归
  spec，**禁止借道 TaskContext 属性穿透进程边界**；测试 FakeProcess 解 spec 字段，
  禁止按位置参数反解（`tests/hygiene/test_fake_infra_guard.py` 的 AST 布局守卫与
  `tests/backend/test_pipeline_backend.py` 在同一提交内同步改造）；
- **状态转移**：唯一出口族 `StateStore.apply_*`（六个，见 D2；wall / failed 互斥
  在此保证）；Job 终结唯一出口 `CompletionMachine.complete_job`；
- **运维管理**：run() 外管理 API 唯一接缝 `OpsConsole(backend, store, taxonomy)`
  （自 StateStore 拆出；拆分时给 PipelineState 补公共 discard / add 方法，禁止把
  直触 `_failed_uids` / `_wall_uids` 私有属性原样搬入）。

### D4. 假想 seam 清算

- 删除 `DispatchView` / `CommitView` / `RecoveryView` 与
  `ExecutionChannelProtocol`——四者均为单 implementor。前三个**并非零引用**：
  `dispatch.py` / `scheduler.py` 有注解方，`tests/unit/test_state_store.py` 有兼容
  测试，删除必须在同一提交内清理注解与测试；
- adapter 只允许存在于**双实现及以上**的 seam：`StateBackend`（SQLite / Memory）、
  `SnapshotStore` 族、`Resource` 族；
- 新增 Protocol 前必须回答「第二个 adapter 在哪」，答不出则不引入。

### D5. 防漂移工程纪律（对上述 git 史的直接回应）

1. **一步到位纪律**：触碰某 seam 的重构必须在同一原子提交内完成「抽取 + 全部调用点
   + 对应测试」；禁止埋下待后续删除的转发垫片（对外兼容 re-export 除外，且须在提交
   信息中注明移除计划）；
2. **两份真相禁令**：任何提交边界不得存在同一定义的两份活跃拷贝；符号搬迁必须单
   提交完成（含全部 import 切换与测试更新）；
3. **反转须留痕**：推翻既有删除/恢复决策（含兼容性回滚）必须先写或修订 ADR，
   提交信息引用 ADR 编号——「删除→恢复→再删除」无记录链禁止重演；
4. **门面不解析默认值、装配受守卫**：`TaskLite` 只透传 `Optional` 原始参数，解析权
   唯一归 `RunConfig.resolve()`；`TaskLite` 不持有运行态可写属性，装配类 API
   （add_resource / register_* / enqueue）全部带 run 期守卫。

---

## 架构后果与收益

1. **测试面收敛**：机器测试显式装配（backend + store + policy 按需构造），不再穿透
   `_runtime.ctx.*`；等待决策可表驱动直测（含「二次信号后 KeyboardInterrupt」
   「stop() 后正常排空」两个锁定 case），编排层黑盒回归可瘦身；
2. **churn 抑制**：接缝契约固定后，重构单位从「发现式再开刀」变为「对着本 ADR 的
   定点收敛」，`git bisect` 独立可回滚；
3. **兼容成本可控**：`runtime.py` re-export 与 `tasklite.wrappers.http` 导出面保持，
   下游零改动；门面外泄属性按手册 §4.1 去留表分批 deprecate；
4. **代价**：机器构造参数变多（真实依赖显式化，DispatchMachine 13 参数为核对后的
   真实清单）；`RunContext` 语义承担者拆为两模块，需要一次性迁移成本（见手册
   §6 迁移基线；`wrappers/http.py` 拆包**不在此基线内**——ADR-0001 只裁决了概念
   正交，未裁决物理拆包，属独立决策，须自带 ADR 与 pickle 兼容别名后方可执行）。

---

## 修订记录

- **修订 1（2026-09-10）**：经独立子代理逐方法对照代码评审后修订——D1 机器依赖
  清单校正（Dispatch 补 handlers / taxonomy / output_root / ipc_dir /
  commit_failure_dlq_threshold，Completion 补 backend，Recovery 去 session 补
  policy）并增补三段式装配时序；D2 转移出口扩至六个、纳入 channel 方法别名收敛
  与 `deadlock_gap_max_rounds` 双默认值冲突（3 vs 5）的收敛裁决；D3
  WorkerLaunchSpec 增补 `timeout` 字段并扩大 S6 迁移面（hygiene AST 守卫）；
  D4 事实前提修正（三个 View Protocol 存在注解方与兼容测试）并扩及
  `ExecutionChannelProtocol`；D5-4 增补装配 API 守卫义务。迁移序列重排：**机器
  窄依赖化必须先于 RunContext 删除**（新编号 S3→S4；原草案顺序相反，按旧编号
  执行会使三机器的 ctx 悬空）；
  http 拆包降级为基线外可选项。评审原文结论：方向正确、无结构性缺陷，
  修订后可施工。
- **修订 2（2026-09-10）**：施工勘察后修订——RunConfig 字段面补 `store` /
  `governor` / `policy`（装配时序要求构造期即建并随快照携带）；EngineRuntime
  定为「构造期持久装配 + 每次 execute() 新建 RunSession」，冻结语义改为「冻结
  引用而非拷贝，内容不变性由 run 期守卫独立保证」（保留测试状态注入接缝）；
  `TaskContext.attempted_uids()` 裁定保留为子进程侧只读 API——子进程内 ctx 是
  唯一状态视图，DiscoveryContext 协议依赖之，「迁 discovery adapter」不可实现。
- **修订 3（2026-09-10，S4 施工修订）**：StateStore 不随 RunConfig 携带，改由
  EngineRuntime 构造期创建（其 on_job_completed 回调绑定 RunSession.fire_job_
  completed，钩子后置变更即时生效；enqueue/OpsConsole 经门面 store 属性在 run
  前依旧可用）；Completion/Recovery 机器不持有 backend 字段，一律经
  store.backend 活引用读取（TaskLite.backend 热切换对机器即时可见）；RunSession
  落地为持久实例 + execute() 经 begin() 复位（语义等价于每次新建，机器持有的
  session 引用跨 run 稳定）；config 增设 resolve_tuning() 供 governor 前置
  构造与 resolve 共用同一默认值真相源。
- **修订 4（2026-09-10，施工完成记录）**：S1–S8 全部落地，本 ADR 与手册 §6
  自此转为历史记录，终态即现状。提交清单：S1 `a97e3ad`（engine/types.py 单一
  真相源 + HandlerEntry / WORKER_RESOURCE 收敛）→ S2 `2fbd169`（RunConfig.resolve
  默认值唯一化 + 装配 API run 期守卫）→ S3a `7e51b5f`（StateStore 显式依赖、
  四个单实现 Protocol 删除）→ S3b `d1bc733`（DispatchMachine 13 参数窄依赖）→
  S3c `c32b148`（Completion / Recovery 窄依赖 + governor.arbitrate 收形）→
  S4 `a30b727`（RunSession 抽取、RunContext 整体删除、EngineRuntime(config)
  单参装配、持久会话 + begin() 复位）→ S5 `9645ffd`（decide_wait 纯函数、
  exit_reason 22 处收敛至 session.exit_reason）→ S6 `0f64897`（WorkerLaunchSpec
  进程 seam 具名契约、channel 别名清算）→ S7 `eeb9c5a`（OpsConsole 拆分、
  PipelineState 补公共方法）→ S8 `5455c22`（门面外泄属性与钩子 setter 加
  DeprecationWarning、用户工具函数迁 tasklite/hooks.py）→ 本提交（S8 文档同步）。
  行为变更重述（均已随对应提交信息留痕）：`deadlock_gap_max_rounds` 默认值
  3→5（S2，以 governor 常量为准）；`worker_wait` 聚合 last-write-wins→min
  （S5，多次挂起恢复取最早者）；装配 API run 期守卫从无到有（S2，run 期间调用
  抛 RuntimeError）。
  S6 兼容面变更重申：ExecutionChannel 四组历史别名（submit / poll_completed /
  consume_stale_result / cleanup）已删除，主名为 spawn / reap_completed /
  claim_stale_result / cleanup_in_flight；`TaskContext.incarnation` 字段已
  移除——下游如有直用须迁移至 `WorkerLaunchSpec`（incarnation 归 spec 携带）。
  backend setter 保留裁定：手册 §4.1 原列「setter 删除」，施工裁定**保留只读 +
  setter 且不发 DeprecationWarning**——`tests/engine/test_crash_recovery_
  regressions.py` 4 处依赖 `pipeline.backend = Failing*Backend()` 热切换做崩溃
  注入，属载荷测试接缝而非外泄门面属性；且修订 3 已确立机器经 store.backend
  活引用读取、热切换对机器即时可见的语义。§4.1 去留表已随本提交同步改写。
- **修订 5（2026-09-10，维护者裁定）**：本仓库无外部消费方（仅维护者自用），
  弃用窗口原则豁免——修订 4 所登记的九处弃用面（门面六个深模块只读属性
  `store` / `scheduler` / `governor` / `channel` / `state` / `in_flight` 与
  三个钩子 setter）与两处零消费 re-export（`runtime.RuntimeConfig = RunConfig`
  垫片、`pipeline.py` 尾部 hooks 三件套 noqa 导入）随 1.2.0 **直接移除**，
  不经历「次版本移除」的弃用窗口；钩子属性收敛为只读（getter 委托 session），
  深模块消费点（测试）迁移至 `pipeline._runtime.*` 直达路径。连带修复：
  `enqueue()` 内部路径经 `self.store` 过渡期属性委托导致用户调用点误触
  DeprecationWarning 的缺陷（改走 `self._runtime.store`，附回归测试）。
  事件泵瘦身兑现手册 §4.2.5 既定契约：`step()` 主体 142→58 行（<60），
  删除不可达的 `store is None` 早退，三组同形终态早退统一 `_terminal_outcome()`，
  填池派发 / 死锁仲裁 / drain 回收结算抽取为私有方法，行为零变更
  （1397 测试 + fencing/shutdown 时序测试全绿，未改任何断言）。
- **修订 6（2026-09-17，维护者裁定）**：StateStore 依赖清单去 `governor`
  ——逐方法核对确认全文件零使用（runtime 经 `config.governor` 自持，
  测试访问的 `governor` 属性是 EngineRuntime 自有属性），构造注入与
  property 为空挂件且是 store↔governor 运行时 import 环的成因。D1
  原则「依赖清单以逐方法核对真实使用为准」的直接兑现；对照修订 4 的
  setter 保留先例（彼处有 4 处测试真实依赖，此处为零）。连带：手册
  §4.2.2/§4.2.7 的 governor 归属描述同步更正。
