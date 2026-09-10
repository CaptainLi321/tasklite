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

- **`RunConfig`**（frozen dataclass）：`run()` 入口拍下的不可变装配快照；
  `RunConfig.resolve()` 是全部调优参数默认值的**唯一解析点**；
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
   OpsConsole 在 run() 之前就可用；
2. **装配期**（构造后 → run() 前）：`add_resource` / `register_handler` /
   `set_discovery_rerun` / `register_transient_exception` 生效于共享注册表；
   **全部装配 API 必须带 run 期守卫**（run 期间抛 RuntimeError）；
3. **run 期**：入口经 `RunConfig.resolve()` 拍快照——`handlers` /
   `discovery_rerun` 做一层浅拷贝隔离后续注册；`resources` / `channel` /
   `taxonomy` / `backend` / `store` 传引用（resources 含挂起时刻与 used 计数，
   不可深拷贝）。**frozen 仅保证字段引用不变，内容不变性由「装配期结束 + run 期
   守卫」共同保证**；多次 run() 各拍各的快照。

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
  `ExecutionChannelProtocol`；D5-4 增补装配 API 守卫义务。迁移序列重排为
  S1→S2→S4→S3（原 S3/S4 顺序矛盾：删 RunContext 的前提是机器已不再持 ctx）；
  http 拆包降级为基线外可选项。评审原文结论：方向正确、无结构性缺陷，
  修订后可施工。
