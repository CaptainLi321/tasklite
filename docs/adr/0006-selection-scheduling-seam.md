# 0006 Selection：第三类调度扩展 seam（候选准入/选择）

## 上下文与决策动因

v2 面向「跨设备队列交接」的宿主（ZeroCite 等）：同一持久化队列会由不同设备/不同运行
窗口先后消费，设备需要按本机能力声明「某些 task_type 本轮不执行」，被跳过的作业必须
原样留在队列里交给下一台设备，而不是被派发、重试或写进失败档案。候选排序（谁先执行）
与重入队节奏（失败后何时回来）已有 OrderingPolicy / RequeuePolicy 两个 seam，但
「哪些候选允许进入本 run」此前没有扩展点。

库主于 2026-10-18 直接指示新增该能力（候选准入/选择）。它与 2026-10-02「调度策略族
已搁置」的裁决**不是同一事项**：被冻结的是软 EDF / 退避回归 / aging / jitter / 超期
准入等依赖 Job 调度属性（priority / deadline / period）的排序与节奏策略族；本次只按
`task_type` 维度做候选准入/选择，不给 Job 增加任何调度字段，也不改动既有两 seam 的
排序/节奏语义。

仓库红线（`AGENTS.md` §3「核心禁调度计算」）要求重试节奏、候选排序与候选准入一律经
RequeuePolicy / OrderingPolicy / Selection seam 以**各自 seam 形态**扩展（策略 wrapper/util
或选择规则值对象），且 **seam 扩展须另立 ADR**。Selection 同属调度扩展，故在本 ADR 中
登记为**第三类调度扩展 seam**并冻结其 API 与语义契约；`docs/V2_GUIDE.md` §5 的 seam
注册表、README 架构树与 ROADMAP §3 同步分类。

## 裁决

1. **分类：第三类调度扩展 seam（候选准入/选择）**，与 OrderingPolicy（候选排序）、
   RequeuePolicy（重入队节奏）并列。核心仍不做任何选择计算：选择判定唯一经
   `Selection.allows_type(task_type)`，引擎只在三处消费它：
   - **候选过滤**：`JobScheduler.scan_next_runnable`（`tasklite/engine/scheduler.py`）
     在依赖/资源评估之前按 `task_type` 跳过未选中作业——未选中作业不贡献
     missing / unknown / impossible / min_wait / has_potential_spawners 任何停摆事实；
   - **终态写入隔离**（2.1.0 实现新增的第三处必要接线）：`StateStore.cascade_fail` 与
     `StateStore.apply_bulk_failure`（`tasklite/engine/store.py`）在写入失败档案前过滤
     被排除 uid，其中级联路径经 `PipelineState.cascade_fail(..., *, excluded_uids=...)`
     （`tasklite/models/state.py`）把被排除作业作为**传播终止点**；
   - **收敛仲裁前置**：`EngineRuntime._remaining_only_selection_blocked`
     （`tasklite/engine/runtime.py`）在 governor 之前以无状态谓词判定「仅剩未选中 + 被其
     传递阻塞」为正常完成。

   **形态差异**：OrderingPolicy / RequeuePolicy 是**策略 ABC**（经门面参数注入具体
   策略实现）；Selection 是**规则值对象集合**（`Rule` 协议 + `Exclude` 值对象，经
   `pipeline.selection(*rules)` 组合）。另注意 `RerunPolicy` 管 wall/failed 历史终态的
   rerun 准入，与 Selection 的「本 run 候选准入」不同层，勿把两者的「准入」混为一谈。
   新增**同轴**（`task_type` 维度）规则可直接实现 `Rule`、无需重开本 ADR；非 `task_type`
   轴、优先级/期限类或改动语义须重开 ADR。

2. **API 形态**：
   - 单模块 `tasklite/selection.py`（不拆 models/wrappers/engine 多文件）；
   - `Rule` 协议只要求 `matches_type(task_type) -> bool`；当前唯一规则
     `Exclude(type=...)`（冻结 dataclass，按 `job.task_type` 精确匹配）；`Selection`
     是引擎内部规则集（`allows_type` / `is_active`），`Selection.__init__` 对缺少
     `matches_type` 的规则 fail-loud 抛 `TypeError`；
   - 配置方式是**方法调用**而非 `run()` 参数：`TaskLite.selection(*rules)`
     （`tasklite/pipeline.py`）为 pipeline 属性式配置——每次调用整体替换规则集、
     0 参重置全执行、返回 self 支持链式；仅允许 `run()` 外调用，run 期抛 `RuntimeError`；
   - `run()` / `run_graceful()` 签名不变；`RunSummary` / `EMPTY_STATS` /
     `ExecutionOptions` / `RunConfig` 零新增字段；per-run 快照经内部注入面
     （`EngineRuntime.set_selection` → `JobScheduler.selection` 与
     `StateStore._set_selection`，`execute()` 启动期固化）承载，不进公开选项；
   - 顶层包只导出 `Exclude`（`Rule` / `Selection` 经 `tasklite.selection` 子模块导入，
     避免同名类覆盖子模块属性）。

3. **语义契约**（关键语义由 `tests/engine/test_selection.py` 覆盖；规则 `matches_type`
   异常 fail-loud、missing-dependency / impossible-resource 混合死锁变体目前为实现语义
   约定（待补测试）；「不消耗 worker」由未派发事实推断）：
   - 默认空规则 = 全执行，行为与扩展前等价；
   - 未选中作业**不派发、不写 attempt 轨迹、不进入 wall/失败档案（含级联失败与批量
     熔断出口）、不消耗 worker/资源与重试预算**，持久化队列行原样保留；
   - 其下游作业因依赖仍挂在队列中而保持 PENDING_DEPENDENCY 留队；
   - 仅剩「未选中 + 被其传递阻塞」且无在途任务时，`run()` 正常以
     `ExitReason.COMPLETED` 收敛（不判死锁、不空转、不写失败档案）；
   - 终态写入隔离：被排除 uid 绝不进入失败档案（父失败级联不传播、批量熔断被过滤）；
   - 畸形队列行（缺 `task_type`/`job_id` 等）无可信身份，**不参与选择豁免**，仍按既有
     `MALFORMED_JOB` 归因进失败档案（与无选择时行为一致，且不得因过滤为空而忙旋）；
   - 混合真实死锁（允许作业存在缺失依赖/未知或超容资源/依赖环）仍走既有 governor
     归因语义——选择规则不等于死锁豁免；
   - 规则属配置代码：`matches_type` 抛异常不被吞，`run()` 以 ERROR 上抛（fail-loud）。

4. **明确不做**（YAGNI，超出即须重开本 ADR）：
   - Job 模型不加 `priority` / `deadline` / `period`；
   - 不改 `DeadlockGovernor`（不给熔断加选择豁免补丁）；
   - 不做 `Include` / 白名单 / 优先级 / 非 `task_type` 规则轴；
   - 不改 `run()` 签名、不加 `RunSummary` 留队统计、不公开 `ExecutionOptions` 字段。

## 后果与重开条件

- **集成方契约**：selection 激活时 `run()` 可能以 `COMPLETED` 退出而持久化队列非空
  （未选中作业留队），完成度必须按队列判定，不能只看退出原因。
- **分类固化**：后续任何调度扩展（含第三类 seam 的新规则轴）一律先立 ADR 再实现；
  `AGENTS.md` seam 注册句已同步为 OrderingPolicy / RequeuePolicy / Selection 三类。
- **与 ADR-0004 / ADR-0005 的关系**：本条是对调度 seam 清单的**增量修订**——由 2 类扩为
  3 类（新增候选准入/选择）；[ADR-0004](0004-v2-parallel-rebuild.md) /
  [ADR-0005](0005-v2-promotion.md) 既有的 OrderingPolicy / RequeuePolicy 与 Job 调度属性
  （priority / deadline / period）裁决**全部不变**，那两类演进仍各自须立 ADR。
- **与 ROADMAP 的关系**：本次实现不构成对被冻结「排序/退避调度策略族」的解禁；
  软 EDF / 退避回归 / aging / jitter / 超期准入（依赖 Job 调度字段）的冻结状态不变。
- **已知质量投资（非阻断）**：运行期对「governor resolved 但零归档」的退化口径只做
  告警 + 非零退避等待；若未来出现真实触发场景，应升级为 fail-loud 归因。
- **重开条件**：
  1. 需要按 `task_type` 之外的维度（payload / 源 / 标签等）做候选准入或选择；
  2. 需要引入优先级 / deadline / period 或超期准入等依赖 Job 调度属性的策略；
  3. 需要选择规则参与 governor 熔断豁免等语义变更；
  4. 队列规模使逐候选 `allows_type` 判定成为性能瓶颈（需新的投影/缓存 seam）。
