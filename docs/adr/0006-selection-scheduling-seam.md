# 0006 Selection：第三类调度扩展 seam（候选准入/选择）

## 上下文与决策动因

v2 需要「本 run 只消费队列的一个子集」的单机能力：同一持久化队列可能被不同运行窗口 / 宿主进程先后消费，本机需要声明「某些 task_type 本轮不执行」。候选排序（谁先执行）
与重入队节奏（失败后何时回来）已有 OrderingPolicy / RequeuePolicy 两个 seam，但
「哪些候选允许进入本 run」此前没有扩展点。

库主于 2026-10-18 直接指示新增该能力（候选准入/选择）。它与 2026-10-02「调度策略族
已搁置」的裁决**不是同一事项**：被冻结的是软 EDF / 退避回归 / aging / jitter / 超期
准入等依赖 Job 调度属性（priority / deadline / period）的排序与节奏策略族；Selection
只按 `task_type` 维度做候选准入/选择，不给 Job 增加任何调度字段，也不改动既有两 seam
的排序/节奏语义。

2.1.0 的首版实现暴露两个必须纠正的语义缺陷：①「仅剩未选中 + 被其阻塞」的闭包谓词
会把挂在 Exclude 下游的**允许作业依赖环**误判为正常完成（真死锁被掩盖为 COMPLETED，
队列永远无法前进）；②「resolved 但零归档」只做 0.5s 退避等待，不保证队列削减，存在
无限忙旋风险。本次修订以 **Kahn 拓扑入度消减**取代闭包谓词，并删除固定等待兜底；
同时把「跳过」拆成两种严格区分的单机语义：`Exclude`（延迟留队）与 `Skip`（终态跳过）。

## 裁决

1. **分类：第三类调度扩展 seam（候选准入/选择）**，与 OrderingPolicy（候选排序）、
   RequeuePolicy（重入队节奏）并列。选择判定唯一经 `Selection` 的 `task_type` 谓词，
   引擎只在四处消费它：
   - **候选过滤**：`JobScheduler.scan_next_runnable`（`tasklite/engine/scheduler.py`）
     在依赖/资源评估之前按 `task_type` 跳过命中作业——不贡献 missing / unknown /
     impossible / min_wait / has_potential_spawners 任何停摆事实；
   - **Skip 跳过阶段**：`EngineRuntime._apply_selection_skips`
     （`tasklite/engine/runtime.py`）在 run 启动与派生作业落队后，把 Skip 命中行及其
     下游闭包收敛为终态跳过（移出 queue + attempts 审计），删除序为下游优先；
   - **停摆拓扑剥离**：`DeadlockGovernor.analyze_selection_standstill`
     （`tasklite/engine/governor.py`）以 `wall_uids | deferred_uids` 为初始波前做
     Kahn 入度消减，区分「真死锁（stuck，交既有归因）」与「仅被 Exclude 阻塞
     （blocked，留队并终止本 run）」；运行期不再有固定等待兜底；
   - **终态写入隔离**：`StateStore.cascade_fail` 与 `StateStore.apply_bulk_failure`
     （`tasklite/engine/store.py`）在写入失败档案前过滤被排除/跳过 uid，级联路径经
     `PipelineState.cascade_fail(..., *, excluded_uids=...)`
     （`tasklite/models/state.py`）把被排除作业作为**传播终止点**。

   **形态差异**：OrderingPolicy / RequeuePolicy 是**策略 ABC**（经门面参数注入具体
   策略实现）；Selection 是**规则值对象集合**（`Rule` 协议 + `Exclude` / `Skip` 值对象，
   经 `pipeline.selection(*rules)` 组合）。另注意 `RerunPolicy` 管 wall/failed 历史终态的
   rerun 准入，与 Selection 的「本 run 候选准入」不同层，勿把两者的「准入」混为一谈。
   新增**同轴**（`task_type` 维度）规则可直接实现 `Rule`、无需重开本 ADR；非 `task_type`
   轴、优先级/期限类或改动语义须重开 ADR。

2. **API 形态**：
   - 单模块 `tasklite/selection.py`（全小写模块名，兼容 Linux 大小写敏感文件系统）；
   - `Rule` 协议只要求 `matches_type(task_type) -> bool`；内置两个冻结 dataclass 规则：
     `Exclude(type=...)`（延迟）与 `Skip(type=...)`（终态）；非 `Skip` 形状的自定义规则
     归入 Exclude 语义。`Selection` 是引擎内部规则集
     （`is_skipped` / `is_excluded` / `allows_type` / `is_active` / `has_skip_rules`），
     `Selection.__init__` 对缺少 `matches_type` 的规则 fail-loud 抛 `TypeError`；
   - 配置方式是**方法调用**而非 `run()` 参数：`TaskLite.selection(*rules)`
     （`tasklite/pipeline.py`）为 pipeline 属性式配置——每次调用整体替换规则集、
     0 参重置全执行、返回 self 支持链式；仅允许 `run()` 外调用，run 期抛 `RuntimeError`；
   - `run()` / `run_graceful()` 签名不变；`ExecutionOptions` / `RunConfig` 零新增字段；
     `EMPTY_STATS` 新增公开计数键 `excluded_blocked`（Exclude 下游延迟数），TaskStats
     监控面其余键不变；
   - 顶层包导出 `Exclude` / `Selection` / `Skip`，子模块路径 `tasklite.selection`
     同样承诺可用。

3. **语义契约**（关键语义由 `tests/engine/test_selection.py` 覆盖）：
   - **Exclude（延迟）**：命中作业不派发、不写 attempt 轨迹、不进入 wall/失败档案、
     不消耗 worker/资源与重试预算，队列行原样保留；其下游允许作业同样留队并在本次
     run 报 `ERR_DEPENDENCY_EXCLUDED` + 计入 `stats["excluded_blocked"]`，下一次 run
     （去掉 Exclude / 换 run 配置）自动恢复执行；
   - **Skip（终态）**：命中作业移出 queue，不进入 wall / 失败档案（不成功也不失败），
     `stats["skipped"]` 计数，审计落 append-only attempts（`outcome="skipped"`，
     错误码 `SKIPPED`）；直接或间接依赖它的下游整链终态跳过（attempt 错误码
     `DEPENDENCY_SKIPPED`，含上游 uid），下次运行不再重试；
   - **优先级**：同一 task_type 同时命中 Skip / Exclude 时 Skip 优先；Skip 级联同样
     优先于 Exclude（被 Skip 上游永久堵死的 Exclude 作业一并终态跳过）。
     **Skip 级联先于停摆仲裁执行**——Skip 子树内的依赖环/缺口一律按
     `DEPENDENCY_SKIPPED` 归因（Skip 上游是永久死因，环是否可解已无调度意义）；
     与之对照，Exclude 残图内的环/缺口优先走死锁归因（`ERR_DEPENDENCY_DEADLOCK`
     等），因为 Exclude 只是延迟、下一次 run 仍会面对同一拓扑；
   - **停摆仲裁**：仅当无 in-flight、无可派发候选且无既有归因原因（missing / unknown /
     impossible / malformed）时进行拓扑剥离。`stuck`（入度无法消减：依赖环、自环、
     缺口）交既有 governor 归因熔断（`ERR_DEPENDENCY_DEADLOCK` / `ERR_DEADLOCK_GAP` 等）；
     `blocked` 的允许作业全部「仅因 Exclude 而无法完成」时，本 run 以
     `ExitReason.COMPLETED` 收敛并保留队列；`unblocked`（如资源等待）非空时不终止；
   - **零归档不忙旋**：governor 报 resolved 但无失败 uid 时，按选择事实重新收敛
     （终止或把残余 stuck 允许作业按 `ERR_DEADLOCK_GAP` 强制熔断），保证每拍可观测进展，
     严禁固定等待；
   - **终态写入隔离**：被排除/跳过 uid 绝不进入失败档案（父失败级联不传播、批量熔断
     被过滤）；
   - **畸形队列行**（缺 `task_type`/`job_id` 等）无可信身份，不参与选择豁免，仍按既有
     `MALFORMED_JOB` 归因进失败档案；
   - **规则异常 fail-loud**：`matches_type` 抛异常不被吞，`run()` 以 ERROR 上抛。

4. **明确不做**（YAGNI，超出即须重开本 ADR）：
   - 不引入任何多机器/分布式调度概念（Skip/Exclude 均为单机语义）；
   - Job 模型不加 `priority` / `deadline` / `period`；
   - 不改既有死锁归因语义（不给 governor 熔断加选择豁免补丁；选择只做停摆拓扑剥离）；
   - 不做 `Include` / 白名单 / 非 `task_type` 规则轴；
   - 不改 `run()` / `run_graceful()` 签名，不公开 `ExecutionOptions` 字段。

## 后果与重开条件

- **集成方契约**：Exclude 激活时 `run()` 可能以 `COMPLETED` 退出而持久化队列非空
  （留队待下次），完成度必须按队列判定；`stats["excluded_blocked"]` 给出本次延迟的
  下游数量。Skip 是永久删除语义：命中行与下游不会再被任何后续 run 重试。
- **分类固化**：后续任何调度扩展（含第三类 seam 的新规则轴）一律先立 ADR 再实现；
  `AGENTS.md` seam 注册句已同步为 OrderingPolicy / RequeuePolicy / Selection 三类。
- **与 ADR-0004 / ADR-0005 的关系**：本条是对调度 seam 清单的**增量修订**——由 2 类扩为
  3 类（新增候选准入/选择）；[ADR-0004](0004-v2-parallel-rebuild.md) /
  [ADR-0005](0005-v2-promotion.md) 既有的 OrderingPolicy / RequeuePolicy 与 Job 调度属性
  （priority / deadline / period）裁决**全部不变**，那两类演进仍各自须立 ADR。
- **与 ROADMAP 的关系**：本次实现不构成对被冻结「排序/退避调度策略族」的解禁；
  软 EDF / 退避回归 / aging / jitter / 超期准入（依赖 Job 调度字段）的冻结状态不变。
- **重开条件**：
  1. 需要按 `task_type` 之外的维度（payload / 源 / 标签等）做候选准入或选择；
  2. 需要引入优先级 / deadline / period 或超期准入等依赖 Job 调度属性的策略；
  3. 需要选择规则参与 governor 熔断豁免等语义变更；
  4. 队列规模使逐候选 `allows_type` / 停摆拓扑剥离成为性能瓶颈（需新的投影/缓存 seam）。
