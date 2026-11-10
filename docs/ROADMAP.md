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

## 4. 稳定 Interface 基线与文档改版 —— `已实施（ADR-0011）`

目标契约与接口标准集中见 [ADR-0011](adr/0011-stable-interface-baseline.md)。稳定承诺限常用 Interface、官方内置能力使用方式与已确认身份接口；不开放资源／文件扩展，调度扩展暂不承诺兼容，可选工具暂不承诺兼容，不开放自定义后端接口。旧库不提供原地迁移。

公开 Interface 的覆盖审计见 [ADR-0011 覆盖清单](adr/0011-interface-coverage.md)。原实施证据见[历史 ticket 归档](tickets/adr-0011/README.md)；当前修复与最终验收见[执行计划](tickets/adr-0011-review/plan.md)。行为契约以 ADR-0011 与 [GUIDE](GUIDE.md) 为准。

### Module 实现地图

本计划按 codebase-design 的 Depth、Locality 和 Interface 测试面组织工作；ADR 是行为依据，此处只规定代码归属与实施路径。下面的 Module 是职责集合，不要求为每一行新建类或目录。先复用已有单一出口，只有调用关系和测试需要时才移动文件；不以文件大小或类数量衡量 Depth。

| Module | 对调用者提供的 Interface | 现有代码落点与复用 | 收拢／删除的 Implementation |
| --- | --- | --- | --- |
| 提交与配置解析 | 注册模板、接纳单 Job、校验并准备父成功时的派生声明 | `models/task.py`、`models/job.py`、`engine/admission.py`、`engine/store.py` 的规范化入口、`pipeline.py` 注册提交 | 配置继承与快照形成一个权威入口；外部提交和派生复用规则。去掉 schema、运行字段构造、隐式替换、插队；不让 TriggerRegistry 继续兼任全部配置解析 |
| 状态与结算 | 进入 running、结算结果、恢复遗留执行、人工变更及清理历史 | `engine/completion.py`、`engine/store.py`、`engine/recovery.py`、`engine/ops.py`、`models/state.py`、两种 backend | `CompletionMachine.complete_job` 仍是执行收尾出口，失败登记仍收敛到 `StateStore.apply_failure`；后端负责事务事实，主进程负责业务决定。集中编号分配、四态及档案互斥，删除 strike、activation、隐式瞬态重入与旧运维别名 |
| 执行与调度 | 启动／停止一次 Run，在主控制线程推进准入、资源和结果 | `engine/runtime.py`、`scheduler.py`、`dispatch.py`、`in_flight.py`、`session.py`、`resource.py`、`wait.py` | 复用串行派发与在途归属；统一可唤醒等待。删逐 Job 锁、潜在派生者宽限、Skip、挂起资源、reserve/claim 类型特判及公开 stats/hooks；OrderingPolicy 继续负责访问顺序 |
| worker 通信 | 启动执行、接收声明／结果、回复文件请求、结束受阻执行、回收句柄 | `engine/channel.py`、`utils/ipc.py` 已有 Pipe sender/receiver、`models/context.py` | 通信返回事实，不自行决定 Job 成败或预算；正常结果与授权中止分开回收。删除文件信箱和孤儿锁探测；不要把 models 反向接到 engine |
| 文件声明与授权 | 解析文件、准备获准操作、结算清理、查询／撤销授权 | `models/artifact.py`、`utils/artifact.py`、`utils/ipc.py` 的旧声明记录、`engine/channel.py` 清理调用 | 复用受保护路径与删除原语，重写声明所有权为主进程；路径基准、role、动态往返与授权集中。删除 handler sandbox 开关、文件形式声明与独立类别列表；不扩大到 OS 隔离 |
| 只读观察 | `StateSnapshot.open` 与三种查询，稳定 JSON 记录 | backend 的当前读取基础、`engine/ops.py` 旧查询、`models/attempt.py` 记录信息 | 建立同一记录投影与一致快照；SQLite 独立读事务，内存捕获一致版本。移除分表便捷读取、记录包装类及 TaskLite 重复查询；不从查询执行恢复写入 |
| 公共装配 | TaskLite 生命周期、根包核心导出、职责模块入口 | `pipeline.py`、`__init__.py`、`selection.py`、新增公共 `resource.py` 门面位置 | 只组装上述 Module，不重复实现准入／结算规则。资源和选择不重复根包导出；不公开内部后端注入或配置容器 |

公共 `resource.py` 的作用是确立稳定导入路径，不应再包一层同名委托类。对现有 OpsConsole、配置转发和结果包装逐一做 deletion test：如果删除后只是减少转发且知识不散落到调用方，就合并；如果它集中事务、恢复或跨进程所有权，就保留其职责。不能仅凭名字或代码量批量删除。

依赖方向保持 `pipeline` 装配 engine／backend／models，核心不得反向依赖 wrappers／contrib。models 只声明值及依赖低层通信工具；utils 不反向依赖 engine 或 wrappers。跨 Module 只传必要的配置、事实和结果，不把整个 TaskLite 或全部可变运行容器交给每个调用者。

### Seam 与 Adapter 选择

| 依赖 | Seam 位置及测试方式 | 不引入的公开承诺 |
| --- | --- | --- |
| 纯配置、身份、记录投影 | 直接调用所属 Module 的 Interface；真实值对象测试 | 不为纯函数添加通用 Adapter |
| SQLite／内存状态 | 内部状态 Interface，两种真实 Adapter；同一行为场景参数化验证，SQLite 使用临时文件 | 不恢复用户 backend 注入或第三方后端协议 |
| 时间与唤醒 | 调度 Module 内部接受时钟／等待依赖；确定性时钟测试计算，真实通知测试检查至等待间不丢唤醒 | 不向 TaskLite 暴露测试时钟、轮询周期或新调度协议 |
| Pipe 与 worker | 通信事实通过内部 Interface 交给执行 Module；真实子进程验证结果、EOF、退出、信号与清理 | 不用全部 mock 的进程测试冒充 OS 行为保证 |
| 文件与授权输入 | 临时目录测试真实清理；授权交互的内部 Seam 使用脚本化回答与真实输入实现 | 不开放通用文件插件，不访问业务路径，不扩展删除权限 |
| HTTP 等可选工具 | 在各自 Module 的既有 Seam 测试外部依赖；对新核心做适配测试 | 不给调度核心引入外部网络调用或业务分类 |

依赖由内部装配接受，不意味着用户可以替换所有依赖。只有已有生产与测试用途，或两种真实实现需要变化时才设置 Seam；不为预想扩展增加空协议。

### 行为切片与提交顺序

以下顺序表达依赖，不表示每行必须恰好一个 Commit。每个切片开工先列其全部调用方与旧测试；若有两个可独立验收的行为应继续拆分。每次行为切换须连同必要调用方一起完成，不提交断开的代码，也不为保持旧测试而增加对用户可见的兼容解析。可先提交未接入的内部纯逻辑或事务能力，接入时在同一提交删除对应旧路径；未接入的能力不得冒充新公开能力已完成。

| 顺序 | 行为切片与依赖 | 代码范围／替换点 | 提交前的可观察完成条件 |
| --- | --- | --- | --- |
| 1 | 身份与模型、提交配置；基础 | models、encoding、注册／提交规范化及调用方 | UID 可逆且可读；配置关键字化，删除 schema；提交后修改原对象不改变快照，新模板不刷新旧 pending；同配置去重、异配置冲突；handler 仍收到 job 与 ctx |
| 2 | 新状态事务与结算记录；依赖 1 | 两 backend、store、attempt/state、completion、recovery | running 与编号推进原子；只在结算追加 Attempt；父成功／子声明／游标整体提交；SQLite 与内存结果一致；故障事务无半成品；只读识别不支持格式且不修改测试旧格式副本 |
| 3 | 最小真实执行闭环；依赖 1–2 | dispatch、channel、ipc、completion、runtime | 单 Job 经真实 Pipe 成功或失败；仅 TaskResult 被接受，原始异常信息保留；有效结果到达即开始结算、无结果退出失败；删除对应旧结果解析、回调和统计出口；普通文件任务的旧收尾能力不得被断开 |
| 4 | 预算、恢复与停止；依赖 2–3 | session、completion、recovery、runtime、channel | 零预算重试仍推进编号；同 Run 预算不重置，新 Run 重置；Ctrl+C 后 retry 留 pending；遗留 running 恢复失败且归属原 Run；停止期间在途失败仍级联；删除强停与自动异常重试 |
| 5 | 依赖与本次排除；依赖 1–4 | scheduler、governor、admission、selection、提交／派生校验 | 外部提交拒绝悬空依赖；spawn 按声明顺序整体验证；Trigger 跳过无成功证据则下游失败；单 Exclude 命中链留队且不误判死锁；实际环明确失败；删除宽限／Skip 分支 |
| 6 | 资源与统一唤醒；依赖 3–5 | resource、wait、dispatch、runtime、in_flight、公共 resource 模块 | 两个 worker 同时共享容量；排序靠前但资源不足不挡后方；主控制线程唯一分配／回收；一次计时或通知唤醒无周期资源扫描；检查与等待间通知不丢；IntervalResource 用量决定后续间隔；异常直接使 Run 失败 |
| 7 | FileSpec、授权往返与清理；依赖 1–6 | artifact、context、ipc、channel、completion、授权持久化 | 预声明与动态声明取并集；引用保留配置且运行时解析；授权前不做清理；所有清理主进程执行；拒绝先结束受阻执行再 failure/pending；停止解除授权等待；其他在途仍可结算；清理失败不发布成功效果 |
| 8 | 人工管理与历史清理；依赖 2、4–5 | ops、store、两个 backend、pipeline | 三态 recursive 显式且对称；不产生 Attempt／文件副作用；清空全部历史并重启后编号仍递增；内部编号不伪造 Job／依赖存在；游标主进程直接写与 worker 成功暂存语义明确 |
| 9 | 一致快照与公开记录；依赖 2、7–8 | StateSnapshot、backend 读取、统一记录投影 | path／实例打开同形记录；跨两次查询仍是同一版本；内存读快照不长期占执行锁；记录包含 job/files/result 与实际时间；单 Job Attempt 按编号递增；关闭快照不关闭 TaskLite |
| 10 | 公共装配与可选工具适配；依赖前述可用行为 | pipeline、根包／职责模块导出、wrappers、testing | backend/state_path 初始化、关闭及信号契约完整；资源同名拒绝；可选工具适配 TaskResult 与 FileSpec；无旧根包别名／Selection／schema／挂起入口；最小完整示例可运行 |
| 11 | 文档切换与发布；依赖 1–10 | GUIDE、README、CONTEXT、AGENTS、CHANGELOG、格式样本 | 新指南只描述已验证行为；发布版本／格式号／支持窗口明确；常用 Interface 与不承诺兼容的可选扩展明确分开 |

不能把最终导出整理理解为前面可长期保留旧公开别名：受影响入口在对应切片完成时就切换，最后一轮只检查遗漏。文件 IPC、逐 Job 锁、旧分类器等存在多个调用方的删除，按最后一个真实使用者迁出时完成；禁止为了先删除文件而复制旧逻辑到别处。

步骤 2 的持久化与步骤 3 的执行接入若在具体代码中无法独立保持完整不变式，应先拆出可独立验证的内部事务能力，再进行一次自洽的执行接入，不硬按表切出不可运行提交。同理，文件往返与授权状态切换必须完整进入同一可运行路径，不发布半套声明协议。

### 验收面与测试替换

使用者可见流程通过 TaskLite／JobContext／StateSnapshot 的 Interface 验证；状态与通信 Module 的内部 Interface 可用于确定性事务、崩溃及通知测试，不把测试专用依赖放进公开构造器。断言记录、文件效果、调用结果和进程是否结束，不依赖私有容器布局或临时类名称。

| 已有测试资产 | 迁移用途 |
| --- | --- |
| `tests/backend/test_backend_parity.py`、SQLite／memory 测试 | 复用双 Adapter 对等场景，增加 running/编号/结算/清理全历史事务；真实 SQLite 回滚不能只测内存 |
| `tests/engine/test_completion.py`、`test_recovery.py`、`tests/integration/test_trigger_lifecycle.py` | 保留故障与生命周期证据，改为新结果和每 Run 预算；删除只验证旧激活代或异常分类的断言 |
| `tests/integration/test_pipe_ipc_lifecycle.py`、`tests/utils/test_pipe_ipc.py` | 验证真实进程结果、EOF 与中止，不以串行假执行替代 |
| `tests/engine/test_concurrency.py`、`test_wait.py`、`test_resource.py` | 以屏障／事件覆盖通知丢失、主线程所有权及正常排空，能用确定性协调的场景不用脆弱 sleep |
| `tests/integration/test_artifact_references.py`、artifact transport／utils 测试 | 迁移 FileSpec、授权、清理和输入引用，临时目录执行真实文件操作 |
| `tests/test_pipeline.py`、`tests/integration/test_end_to_end.py` | 最终公开 Interface 的完整运行、管理、查询及导入契约样本 |

替换测试前逐项说明旧测试保护的行为由哪个新场景覆盖。只删除过时契约或纯粹穿透 Implementation 的测试；不通过删测试、标记跳过、减少真实并发验证或返回恒成功来实现“全绿”。删除测试的理由写提交说明，不在代码和测试名中放审查编号。保留的系统级不变式仍遵守仓库红线。

### 每次提交与发布门禁

每个行为切片完成后执行针对性验收，再执行 `python -m mypy` 与 `python -m pytest tests/ -q -p no:cacheprovider`；修改类型、签名或导入时执行 `make test-matrix`。全部符合仓库门禁后立即原子提交，中文说明具体行为与验证，不跨独立问题打包。高负载时序例外仅使用 AGENTS 明确允许的范围并如实记录。

全部开发与验证使用新临时目录、临时数据库及专门构造的格式样本；不读取运行或修改下游业务状态，不迁移存量库。文档一致性检查不能代替实现测试，旧测试全绿也不能证明新契约已经实现。发布前另运行新公开 Interface 的完整例子和格式支持／拒绝样本。

启动实现的第一项是步骤 1 的调用方清点与精确改动清单，然后选择一个可独立验收的身份／模型提交；不再为已定行为增加讨论回合，也不在没有行为收益时先做全仓目录搬迁。

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

GUIDE 须有 Trigger 行为表、结果与提交效果表、文件类型与清理表、停止与持久化结果表；每个重要行为提供最小示例与对应验收场景；加入先声明动态子任务、再声明汇总任务的用法，不增加组栅栏接口。新公开承诺应使用同一 Interface 进行测试，不靠读取内部可变状态来证明。


## 5. 执行目录绑定检查 — 后续候选

为同一状态库接续执行提供路径基准一致性检查，避免换目录后相对路径、外部程序配置及业务副作用改变语义。本版本由使用者保证路径基准一致，不检测跨目录使用，也不为其提供执行语义保证；路径授权和受保护路径规则不受影响。

后续可考虑队列非空时拒绝换目录执行，但仅检查队列长度不足：队列为空仍可能通过失败档案恢复历史 Job。立项时必须一起明确 pending/running 接续、历史失败重跑的检查范围、目录绑定时机及解除条件。实施前另立 ADR，本轮不增加检查或持久化机制。

---

*本路线图由库主裁决驱动增删；条目实施时以对应 ADR 为准，本文件只保留方向与决策要点索引。*
