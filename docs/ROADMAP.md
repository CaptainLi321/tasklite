# 演进路线图（ROADMAP）

> 本文件记录**已分析、未实施**的未来演进方向。每项实施前按 ADR 程序立项；
> 状态分：`分析完备`（方案已论证，待立项 ADR）/ `已搁置`（库主明确暂缓）/ `质量投资`。
> 历史决策见 [adr/](adr/)；当前架构与使用见 [V2_GUIDE.md](V2_GUIDE.md) 与 [README.md](../README.md)。

---

## 1. 单任务直达 CLI —— `分析完备，待立项 ADR`

**动机**：单目标走「收集→分发→执行」全链路不便（补跑/调试/ad-hoc 场景）；本质是 aperiodic
作业的直接准入（绕过 discovery 的 release 通道），与链路跑**共享同一 wall 去重与 attempts
轨迹**——是同一状态系统的第二入口，不是系统外野路径。

**形态**：`python -m tasklite run --app <module:attr> --task <name> --input X --output Y`，
argparse 标准库（零依赖红线无虞）；薄驱动器 = 六步调用顺序 1–4 步的模板化；`run()` 的
`RunSummary` 直接映射退出码与呈现。

**实施前须裁决的三决策**：

| 决策点 | 建议 |
|---|---|
| app 入口约定 | 装配函数 `--app mymod.pipeline:build_engine`（无参调用返回 TaskLite 实例） |
| **uid 一致性**（核心） | 直跑必须生成与链路相同的 uid 才能共享 wall 去重——装配约定可选工厂 `make_job(task_type, *, input, output) -> Job`；无工厂时要求显式 `--job-id`；派生规则是消费者域知识，不进库 |
| 输入/输出键映射 | 先文档约定 `payload["input"]/payload["output"]` + 通用 `--set k=v`；Task 加 input_key/output_key 属 YAGNI，按需再加 |

**红线**：CLI 不新增任何引擎语义（仅门面驱动 + ops 文本呈现）；`--input/--output` 快捷
方式仅适用单输入单输出型 task，上下文型任务走 `--set` 或链路。

**顺带收益**：运维子命令 `tasklite failures` / `retry <uid>` / `wall`（ops 面直映射）。

## 2. make 式级联更新（内容指纹与自动发现下游）—— `分析完备，待立项 ADR`

**已交付基线（ADR-0008）**：下游通过 `ctx.declare_input(path)` 声明路径、设置 `trigger="on_input_change"`，每轮显式入队需要处理的上下游。对于本批 `A → B`，先完成 A 的本批处理，再按 B 的可信成功时间与当前输入 mtime 判定；逆序入队同样生效。A 未改输入时 B 可跳过，A 本批失败时 B 不借旧成功放行。目录仅 stat 自身，不递归；保持 mtime 的内容替换可能漏判，输出缺失不自动触发。完整契约见 [V2 使用指南](V2_GUIDE.md#触发器迁移与恢复边界)。

**未来方向**：内容指纹比较、消费者倒排索引及自动发现/入队未提交的下游均未实施，须另立 ADR。本轮交付不授权这些扩展。

**候选形态**：

- 在成功档案中表达产物路径与内容指纹，和下游声明输入建立可审查的关联；
- 比较上游产物内容与下游成功基线，避免仅 touch 引发空转；
- 先研究显式入队候选的惰性判定，再独立裁决是否支持成功提交时反查并创建下游候选；
- 保持稳定 Job 身份，明确与现有 `on_input_change` 判据及激活代推进的关系，不从实现便利反推新契约。

**风险面**：路径规范化与单射编码、内容基线的可信性、索引成本、共享路径和依赖环、崩溃后的候选恢复。任何 schema 扩展须明确版本边界；不因该方向引入核心排序或退避计算。

## 3. 调度策略族 —— `已搁置（库主 2026-10-02 裁决），解禁后逐策略小 ADR`

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

## 4. 质量投资 —— `按需`

- mutmut 变异测试实跑（source_paths 已对齐 v2 面）；
- 覆盖率短板补强：`utils/lockfile.py`（76%）、`utils/ipc.py`（85%，多为防御/降级分支）；
- 测试观测面：为 begin_round/崩溃网/fd 释放/调度缓存一致性立最小公开只读观测，消除四簇
  「断言实现细节」的直戳私有测试；
- attempts 推进双入口（run 内内存态 vs run 外持久态）同语义锁定测试。

---

*本路线图由库主裁决驱动增删；条目实施时以对应 ADR 为准，本文件只保留方向与决策要点索引。*
