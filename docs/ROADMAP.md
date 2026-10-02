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

## 2. make 式级联更新（on_input_change 扩展）—— `分析完备，待立项 ADR`

**动机**：上游重跑后下游躺在 wall 里永不失效；需要 make 的传递性 dirty propagation
（数据流触发），且可用内容指纹做得**比 mtime 语义的 make 更准**。

**第 0 档（零代码，即刻可用）**：下游 job 把上游产物路径声明进自己的 inputs +
`rerun="on_input_change"`——周期性链路下一轮 run 在 wall 拦截点自动比对 mtime 触发下游
重跑。限制：惰性（下轮 run 生效）、mtime 假阳性（touch 即触发）。

**第 1 档（完整 make 语义，约 300–500 行 + 测试）**：

- wall meta 增加 declared outputs 记录（path → `content_fingerprint`；`declare_output`
  采集点已存在）；
- 级联判定 = 「上游 outputs ∩ 下游 inputs」路径 join + **内容指纹对比**——上游重跑但产物
  未变时下游不空转（超越 make）；
- 触发形态：lazy（下游重新入队时在 wall 拦截点比对）先行，零新事务面；eager push（上游
  commit 时反查消费者重新入队）留二期；
- 语义归位：级联失效走 `on_input_change` 通道（`activation_no+1`），**不新开 rerun 策略**，
  四策略矩阵不动。

**风险面**：路径规范化（相对/绝对/symlink，复用 `encode_*` 单射族）；join 成本（wall meta
扩展或 products 倒排表，schema 变更升 user_version）；自产自销/环防护（文件系统天然无环，
仍需显式断言）。与调度域正交，不触碰 OrderingPolicy/RequeuePolicy 红线。

## 3. 调度策略族 —— `已搁置（库主 2026-10-02 裁决），解禁后逐策略小 ADR`

接线已全部就绪（`TaskLite(*, ordering=..., requeue_policy=...)` 门面构造参数 + 引擎层两个
唯一出口），核心禁排序/退避计算红线不变。候选按收益排序：

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
