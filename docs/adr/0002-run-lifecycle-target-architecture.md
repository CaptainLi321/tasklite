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
构造后 `getattr` 回查）是编排层反复返工的共同病根，测试被迫 33 处穿透
`_runtime.ctx.state` 直改内部状态。

**裁决**：先立法、后动刀。本 ADR 固定运行期终态契约；后续一切重构以本 ADR 与
`docs/ENGINE_ARCHITECTURE.md` 为准，**偏离契约须先修订本 ADR 再动代码**。

---

## 架构决策

### D1. 静态装配与生命周期状态彻底分离（RunContext 退场）

- **`RunConfig`**（frozen dataclass）：`run()` 入口拍下的不可变装配快照；
  `RunConfig.resolve()` 是全部调优参数默认值的**唯一解析点**；
- **`RunSession`**：一次 `run()` 唯一的可变状态容器（`run_id` / `dispatch_seq` /
  `stop_mode` / `stats`），`fire_*` 钩子单一出口，`exit_reason()` 唯一推导实现；
- Dispatch / Completion / Recovery 三台机器以**显式窄依赖**构造（store、scheduler、
  policy、channel、resources、in_flight、session……按真实使用清单）；
  **禁止任何「Context 整袋」构造参数形态复活**；
- `StateStore` 显式收依赖（taxonomy / governor / policy / stats / on_job_completed），
  **禁止构造后 `getattr` 回查调用方**。

### D2. 单一真相源清单

| 真相 | 唯一落点 |
|---|---|
| 引擎公共值对象（StopMode / ExitReason / TaskStats / StepOutcome / RunSummary / ExecutionOptions / HandlerEntry） | `engine/types.py`（零内部依赖叶子模块） |
| `WORKER_RESOURCE` 常量 | `models/job.py` |
| `exit_reason` 推导 | `RunSession.exit_reason()` |
| 等待/空闲决策 | `pacing.decide_wait(LoopFacts) -> WaitDecision` 纯函数 |
| 参数默认值 | `RunConfig.resolve()` |

`runtime.py` 对旧符号的 re-export 仅作过渡兼容垫片，随次版本移除。

### D3. 具名 seam 契约

- **进程边界**：唯一契约 `WorkerLaunchSpec(handler, job, task_ctx, incarnation,
  ipc_dir)`；`incarnation` 归 spec，**禁止借道 TaskContext 属性穿透进程边界**；
  测试 FakeProcess 解 spec 字段，禁止按位置参数反解；
- **状态转移**：唯一出口 `StateStore.apply_success / apply_failure / apply_retry /
  apply_skip`（wall / failed 互斥在此保证）；Job 终结唯一出口
  `CompletionMachine.complete_job`；
- **运维管理**：run() 外管理 API 唯一接缝 `OpsConsole`（自 StateStore 拆出）。

### D4. 假想 seam 清算

- 删除 `DispatchView` / `CommitView` / `RecoveryView`（单 implementor 且无调用方以
  其注解——文档化 interface 与实际依赖两张皮）；
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
4. **门面不解析默认值**：`TaskLite` 只透传 `Optional` 原始参数，解析权唯一归
   `RunConfig.resolve()`；`TaskLite` 不持有运行态可写属性。

---

## 架构后果与收益

1. **测试面收敛**：机器测试显式装配（backend + store + policy 按需构造），不再穿透
   `_runtime.ctx.state`；等待决策可表驱动直测，编排层黑盒回归可瘦身；
2. **churn 抑制**：接缝契约固定后，重构单位从「发现式再开刀」变为「对着本 ADR 的
   定点收敛」，`git bisect` 独立可回滚；
3. **兼容成本可控**：`runtime.py` re-export 与 `tasklite.wrappers.http` 导出面保持，
   下游零改动；
4. **代价**：机器构造参数变多（真实依赖显式化）；`RunContext` 语义承担者拆为两模块，
   需要一次性的迁移成本（见 `ENGINE_ARCHITECTURE.md` 迁移基线八步）。
