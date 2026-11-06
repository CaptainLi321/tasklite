# 演进路线图（ROADMAP）

> 本文件记录**已分析、未实施**的未来演进方向。每项实施前按 ADR 程序立项；
> 状态分：`分析完备`（方案已论证，待立项 ADR）/ `已搁置`（库主明确暂缓）/ `质量投资`。
> 已落地特性（如执行选择 [ADR-0006](adr/0006-selection-scheduling-seam.md)、make 式 `mtime` 触发器级联更新 [ADR-0008](adr/0008-trigger-contract-and-trusted-success.md)、具名磁盘产物 [ADR-0009](adr/0009-artifact-authorization-and-cleanup.md)、瞬时 Pipe 通信 [ADR-0010](adr/0010-ephemeral-worker-pipe.md)）已移出待办，详见 [adr/](adr/)、[GUIDE.md](GUIDE.md) 与 [README.md](../README.md)。

---

## 1. 单任务直达 CLI —— `分析完备，待立项 ADR`

**动机**：单目标走「收集→分发→执行」全链路不便（补跑/调试/ad-hoc 场景）；本质是 aperiodic
作业的直接准入（绕过 discovery 的 release 通道），与链路跑**共享同一 successes 去重与 attempts
轨迹**——是同一状态系统的第二入口，不是系统外野路径。

**形态**：`python -m tasklite run --app <module:attr> --task <name> --input X --output Y`，
argparse 标准库（零依赖红线无虞）；薄驱动器 = 六步调用顺序 1–4 步的模板化；`run()` 的
`RunSummary` 直接映射退出码与呈现。

**实施前须裁决的三决策**：

| 决策点 | 建议 |
|---|---|
| app 入口约定 | 装配函数 `--app mymod.pipeline:build_engine`（无参调用返回 TaskLite 实例） |
| **uid 一致性**（核心） | 直跑必须生成与链路相同的 uid 才能共享 successes 去重——装配约定可选工厂 `make_job(task_type, *, input, output) -> Job`；无工厂时要求显式 `--job-id`；派生规则是消费者域知识，不进库 |
| 输入/输出键映射 | 先文档约定 `payload["input"]/payload["output"]` + 通用 `--set k=v`；Task 加 input_key/output_key 属 YAGNI，按需再加 |

**红线**：CLI 不新增任何引擎语义（仅门面驱动 + ops 文本呈现）；`--input/--output` 快捷
方式仅适用单输入单输出型 task，上下文型任务走 `--set` 或链路。

**顺带收益**：运维子命令 `tasklite failures` / `retry <uid>` / `successes`（ops 面直映射）。

## 2. 调度策略族 —— `已搁置（库主 2026-10-02 裁决），解禁后逐策略小 ADR`

> **状态更新（2026-10-18，ADR-0006）**：候选准入/选择（`task_type` 维度）已按库主直接
> 指示于 2.1.0 立项实现为**第三类调度扩展 seam**（`pipeline.selection(...)` +
> `Exclude` / `Skip` 两类单机语义，见 [ADR-0006](adr/0006-selection-scheduling-seam.md)）。
> 本节冻结的排序/退避策略族
> （软 EDF、退避回归、aging、jitter、超期准入等依赖 Job 调度字段的方向）**状态不变**，
> 上述实现不构成解禁；第 5 项「超期准入控制」依赖 Job `deadline`、仍随本节冻结，与
> Selection 的 `task_type` 准入不同轴。

接线已全部就绪（`TaskLite(*, ordering_policy=..., requeue_policy=...)` 门面构造参数 + 引擎层两个
唯一出口——本节冻结族对应的排序/节奏两个出口；候选准入见 ADR-0006），核心禁排序/退避
计算红线不变。候选按收益排序：

1. **软 EDF**：Job 增 deadline 字段 + `DeadlineOrderingPolicy`（消除队头阻塞，10 分钟大任务
   压住毫秒级告警的场景）；与 `first_enqueued_at` 协同可延伸 aging；
2. **退避回归**：`DelayedRequeuePolicy`（`RequeuePlan.delay_seconds` 预留位实现指数退避/
   固定延迟），backoff 以 util 形态找回，核心仍零计算；
3. **aging 防饥饿**：低优先级随等待时长升优先级（`first_enqueued_at` 已备）；
4. **周期错峰 jitter**：discovery/cron 类任务随机相位，防惊群冲击单写者 SQLite；
5. **超期准入控制**：入队/派发前发现已过 deadline 直接进失败档案，不浪费子进程。

## 3. 质量投资 —— `按需`

- mutmut 变异测试实跑（source_paths 已对齐 v2 面）；
- 覆盖率短板补强：`utils/lockfile.py`（76%）、`utils/ipc.py`（85%，多为防御/降级分支）；
- 测试观测面：为 begin_round/崩溃网/fd 释放/调度缓存一致性立最小公开只读观测，消除四簇
  「断言实现细节」的直戳私有测试；
- attempts 推进双入口（run 内内存态 vs run 外持久态）同语义锁定测试。

## 4. 稳定 Interface 基线与文档改版 —— `已接受，待实施`

目标契约与未决细节集中见 [ADR-0011](adr/0011-stable-interface-baseline.md)。此处只记录工作顺序，不将设计提案冒充当前版本行为。旧库迁移不在范围内，全部验证使用临时数据，不修改或删除旧业务数据及数据库。

### 实施顺序与验收

| 顺序 | 工作单元 | 面向公开 Interface 的验收重点 |
| --- | --- | --- |
| 1 | 定稿 ADR 所列剩余细节，确定发布和格式基线 | 没有把未决状态、参数或错误处理隐式固化到实现 |
| 2 | TaskResult、业务 metadata 分离、完整 Job 快照、Attempt 与事件 | 三种返回结果、未捕获异常失败、完整快照重跑、回调区分 Attempt 与 Job 结局 |
| 3 | 每 Run 内存预算、Trigger 与生命周期 | 跨 Run 预算重置并重判 Trigger、同 Run 重试不重判、零预算新增 Attempt、Ctrl+C 排空、异常退出在途记失败 |
| 4 | 单任务提交与成功事务 | 内容冲突、快照隔离、派生任务与游标整体提交、取消级联写失败档案 |
| 5 | FileSpec 与主进程文件操作 | 预声明与动态声明取并集、判断执行前不删文件、所有框架清理由主进程执行、不增加跨 Job 路径锁 |
| 6 | 授权与瞬时响应通道 | 首次询问、目录授权复用、拒绝／无法询问结束 Run 并保 pending、等待不阻断在途通信、旧回复拒绝 |
| 7 | StateSnapshot 与管理操作 | 一致快照、真正只读、流式读取、状态库级执行／管理互斥、游标读取和资源恢复 |
| 8 | wrappers、测试工具与公开导出收敛 | 同步 handler 唯一结果协议、无隐式异常重试、真实进程场景覆盖 |
| 9 | 文档切换与发布门禁 | 示例可执行、公开 Interface 契约样本、全量检查、基线格式样本与兼容策略 |

各行可再拆为独立、自洽的原子提交，不合并无关变更。预算、文件往返通信、产物授权必须保留确定性并发与故障回归，不能仅通过返回形状单测验收。提交前执行全量 pytest 与 mypy；类型、签名或导入变更执行 test-matrix。不得因只重构 Interface 就删除仍保护系统不变式的测试。

### 文档改版清单

文档仅使用通用概念和示例，不写具体消费项目名称或功能。待相应实现验证完成后切换现行说明：

| 文档 | 改版内容 | 职责限制 |
| --- | --- | --- |
| README | 定位、含主入口保护的完整最小示例、导航；准确说明框架路径保护能力 | 不复制状态机或字段契约，不称可约束任意程序的安全沙盒 |
| GUIDE | 按注册提交、每 Run 启动、结果与副作用、文件授权、停止、观察管理、扩展组织 | 作为公开行为唯一完整手册，标清默认值、触发时机、失败后保留内容与下次运行行为 |
| CONTEXT | Run、Attempt、每 Run 重试预算、TaskResult、FileSpec、StateSnapshot 等术语 | 实现切换时同步，不提前把现行词典改成未实现状态，不重复指南全文 |
| ADR | 新旧决定的替代关系及取舍 | 保留历史决定，不重写历史来假装新行为一直存在 |
| CHANGELOG | 破坏性变更与调用迁移映射、明确不提供旧库迁移 | 不复制指南，不承诺未经验证的迁移工具 |
| AGENTS | 同步预算字段、生命周期、错误及文件操作红线 | 保留测试门禁、依赖方向、单射编码和单一收尾出口；不留下与已实施契约相反的旧指令 |
| wrappers 模块文档 | 适配结果返回、动态派生及授权行为 | Discovery 的需求契约仍归其模块，不在多处重复维护 |

GUIDE 须有 Trigger 行为表、结果与提交效果表、文件类型与清理表、运行结束与完成度表；每个重要行为提供最小示例与对应验收场景。新公开承诺应使用同一 Interface 进行测试，不靠读取内部可变状态来证明。

---

*本路线图由库主裁决驱动增删；条目实施时以对应 ADR 为准，本文件只保留方向与决策要点索引。*
