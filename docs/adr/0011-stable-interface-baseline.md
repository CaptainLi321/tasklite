# 0011 稳定 Interface 基线与主进程文件授权

状态：**已接受，待实施**。本记录固化接口定型讨论的最终决定，不代表当前安装版本已经提供这些能力；发布版本号、数据库版本号及下文列出的细节尚未冻结。当前使用仍以 [GUIDE](../GUIDE.md) 为准，实施及文档改版清单见 [ROADMAP](../ROADMAP.md#4-稳定-interface-基线与文档改版--已实施adr-0011)。

## 背景与取舍

长期兼容需要覆盖调用方式、默认行为、错误结果、持久记录和扩展协议，而不只是导入名。选择在新基线发布前集中整理 Interface，不为被替代的旧返回形式、参数和入口增加兼容解析；新基线之后遵守兼容承诺。Module 的内部状态机、注册表和可变运行配置不因可访问就成为稳定 Interface。

常用 Interface 分为编排运行、任务执行、状态访问与管理三类职责，不要求机械增加三个类。已有排序、重入队和触发器 Seam 与常用 Interface 分层维护，不增加新的调度算法。内部单一终结出口、单向依赖、单射编码及数据库写事务纪律保持。

命名按语义层次统一：Job 的四个同级当前状态为 pending、running、succeeded、failed；成功与失败档案集合使用复数名词 successes、failures，目标设计不再使用 failed 作为档案名称。TaskResult 的构造方法和 Attempt 的结算结果使用 success、failure、retry。状态、档案集合与一次结果不得混用；现有数据库的 failed 表并未因此原地改名，格式切换仍遵守不迁移旧库的范围。

本文有意替代既有 ADR 中涉及结果归一化、重试预算、执行选择、文件声明和通信方向的部分决定；其他决定不随之失效。尤其 [ADR-0010](0010-ephemeral-worker-pipe.md) 的“无响应需求”前提不再成立，但执行通信仍不持久化、不跨重启接续。

## 故障处置原则

借鉴民航“八该一反对”在条件不满足时采取明确处置、反对勉强继续的思想（[中国民航局说明](https://www.caac.gov.cn/XWZX/DFDT/200901/t20090105_21230.html)），本项目采用：该失败就失败，该停止就停止，该请求授权就请求授权，反对为了表面上的继续运行而猜测成功、隐式重试或堆叠恢复分支。这是工程原则类比，不是航空规则在本项目中的直接应用。

主进程异常退出后，本次在途、尚未提交成功的执行及其产物不再被当作可信完成。后续打开执行状态时将这些执行按失败收敛，保留完整 Job 快照及可得证据；原因不明则记录未知，不根据残留文件或迟到结果补造成功，不自动接续旧执行。已提交成功记录不受影响；从未派发的 pending 也不因主进程退出而改判失败。此决定明确替代旧 ADR 中对此类在途执行自动接续的恢复假设，正常 Ctrl+C 排空不属于异常退出。

不为父死子存场景增加孤儿产物抢救、执行权交接或清理与旧 worker 协作恢复协议。也不以“不可信”为理由扩大文件删除权：残留产物仍受用户授权、声明清理策略与受保护路径规则约束，框架不承诺其完整或静止。保留状态库级执行／管理互斥；无法取得执行权或确认操作条件时明确拒绝或失败，不通过绕过互斥强行继续。不可靠不等于自动删除，也不等于杀死旧 worker 的保证。

删除逐 Job 的 worker 执行锁、孤儿锁探测及其锁冲突重试路径，不以文件锁推断旧 worker 是否仍在执行。failed 不代表旧 worker 或其启动的外部程序已经停止；异常退出后再次执行前，由使用者确认旧执行已经结束。只为该锁服务的文件名编码和 UID 文件名长度限制一并删除，其他调用用途逐项核查。此决定替代 ADR-0010 保留逐 Job 执行锁的约定，状态库级互斥不受影响。

## 初始化、后端与线程边界

TaskLite 是由用户程序导入并调用的 Python 库，不解析宿主程序命令行参数。构造入口收束为：

```python
TaskLite(
    *,
    backend: Literal["sqlite", "memory"],
    state_path: str | Path | None = None,
    max_workers: int = 4,
    output_roots: str | Path | Sequence[str | Path] | None = None,
)
```

```python
persistent = TaskLite(backend="sqlite", state_path="./state.sqlite")
ephemeral = TaskLite(backend="memory")
```

backend 必须显式选择；SQLite 要求提供数据库文件路径，memory 不要求传入 None，也不需要状态目录。删除 name、state_dir 及由名称推导数据库文件名的规则。路径遵循实例创建时捕获的主进程工作目录基准；构造不会替用户创建业务产物目录。IPC 使用 Pipe，不因选择内存状态而创建文件 IPC 目录。

仅提供内置 SQLite 和内存后端，不接受外部后端实例，不开放自定义后端接口，不公开 backend 属性或 setter。由 TaskLite 创建并关闭后端资源；构造失败释放本次已创建的资源，不删除已有数据。close() 与上下文管理行为见生命周期章节。内存模式没有跨进程持久化、重启恢复或跨进程查询能力；持久化授权在 SQLite 中跨进程复用，在内存模式仅随实例存活。

删除 strict_picklable 开关，保留实际进程执行需要的序列化校验，不增加备用序列化机制。删除 fatal_exceptions、transient_exceptions、三个公开回调，以及 dependency_grace_seconds、commit_failure_threshold、deadlock_gap_max_rounds 调优参数。调度扩展不纳入本版稳定构造签名，不为旧开关新增配置包装类。

注册、提交、管理、run() 和 close() 由同一个控制线程顺序调用，不承诺并发调用安全，不要求每次检查调用线程身份。控制线程即调用 run() 的线程，独占资源分配与回收；stop() 允许其他线程调用并唤醒运行循环。其他线程观察时独立打开 StateSnapshot，不共享快照对象。

run(*, exclude=None, install_signals=True) -> None 默认在 Python 主线程安装 SIGINT／SIGTERM 处理器，二者均请求停止派发并正常排空；正常退出恢复原处理器。安装失败明确抛异常，不静默忽略。后台控制线程使用 install_signals=False，由宿主主线程接收信号并调用 stop()。不提供绕过库级执行互斥的参数。

## 异常与失败信息

TaskResult 是任务结果协议，Python 异常是接口调用未完成的报告方式，两者不混用。只保留一个自有公开异常 TaskLiteError，用于框架主动发现的调用条件不成立，例如运行期执行受限操作、提交依赖不存在或 UID 配置冲突。不细分异常子类，不逐项规定 ValueError／KeyError／RuntimeError 分类；Python 调用及底层库自然产生的异常原样传播，不统一包装，保留原始 traceback 与异常链。

TaskLiteError 不承诺捕获所有框架故障，底层异常的类型和消息文本不属于稳定接口。框架运行异常使 run() 失败；worker 中未捕获异常按既定规则结算任务失败，记录可得信息，不要求在主进程重建或重新抛出业务异常对象。删除旧重试异常、瞬态／永久错误分类、公开 ErrorClassifier 与 ERR_* 原因码，不再从异常类型推导自动重试。

失败记录通过 status／outcome 表达结果，message 保存说明，exception 保存可得的类型名称、原始消息和 traceback，没有异常时为 None；metadata 只保存用户业务信息。框架主动判定的超时、依赖失败、人工失败等不必制造异常对象。记录不提供 category、retryable 或稳定错误码，用户不应解析消息文本来依赖框架分类行为。

## 编排、身份与启动

- 保留 Task 规格、Job 逻辑身份、Attempt 执行轨迹。Job 身份仍由 task_type 与 job_id 确定。
- 保留简式任务注册和 Task 对象注册。同名 Task 禁止重复注册并明确报错，本版本不提供覆盖注册；新进程可以加载更新后的 handler，不检查业务代码变化，不增加代码版本锁。Job 不再让业务构造内部运行账目；只读属性和类型提示提供有限约束，不递归冻结业务 payload，也不检查业务代码是否修改对象。
- 公开 enqueue(job: Job) -> bool 本版本只提交一个 Job；框架捕获独立快照。首次加入队列返回 True；同 UID 已排队且有效配置相同返回 False，不同配置明确冲突。不提供 front、replace_pending 或隐式重试插队，候选顺序由 OrderingPolicy 管理；不提供批量提交接口。普通 enqueue 不清除旧终态档案，仍由 Trigger 判断是否执行。
- 外部 enqueue 对提交时可确定的未知 Task、非法配置、不可序列化 payload 立即报错，不入队、不写 failures、不产生 Attempt。资源是否可满足不另加提交前检查；依赖引用存在性在提交时检查；输入文件及运行时依赖是否满足仍在对应阶段判断。
- ctx.spawn 调用时捕获独立的提交声明快照，使用所属 TaskLite 实例的固定路径基准；父任务成功时由主进程解析有效配置、校验并整体提交。worker 改变工作目录不改变子任务相对路径的含义。外部单次提交与父任务一次成功中提交多个派生任务是不同场景，后者仍要求整体提交。
- Trigger 表达本次 Run 对已入队 Job 的启动规则。它不主动入队，也不控制一次启动后的故障重试。on_failure 改名 until_success，默认仍为 once。

| Trigger | 无终态记录 | 当前 successes | 当前 failures（含取消） |
| --- | --- | --- | --- |
| once | 执行 | 跳过 | 跳过 |
| until_success | 执行 | 跳过 | 执行 |
| always | 执行 | 执行 | 执行 |
| on_input_change | 执行 | 按输入变化契约判断 | 执行 |

这些判断消费当前终态档案，不搜索全部 Attempt 历史。on_input_change 暂保留可信成功时间与声明输入自身 mtime 的判断，不扩大为内容哈希、递归目录或输出缺失检测。

| 场景 | Trigger 求值 |
| --- | --- |
| 本次 Run 准备启动一个普通候选 | 依赖就绪后求值 |
| 同 Run 内由 TaskResult.retry 产生的再次尝试 | 不重复求值 |
| 新 Run 接续仍在 pending 的未完成任务 | 重新求值，不能凭旧 Attempt 编号跳过 |
| 终态任务重新入队成为普通候选 | 按当前 Trigger 求值 |

新 Run 重判 Trigger 与预算重置是两个独立规则，明确替代旧指南关于跨 Run 恢复继续沿用原启动判断的约定。异常退出后已收敛到 failed 的任务不自动回队；只有重新入队或显式管理重跑才再次成为候选。自定义 Trigger 的判断可能在两次 Run 之间改变，因此停止前曾获准执行不保证下次仍执行。

Trigger 返回 False 沿用“本次无需执行”语义，不等于 排除规则留队，不生成新的成功证据或产物；依赖处理与具名产物存在性校验仍需遵守原契约，不能因跳过生产者就认为其产物存在。

不增加“每 UID 每 Run 最多启动一轮”的特判。同 UID 再次成为候选仍走普通去重、依赖和 Trigger；同 Run 内再次入队不得刷新重试预算。

运行选择使用 run(exclude=Exclude(task_type="download"))，本版本只接受单个 Exclude 或 None，不接收列表，不解析多规则组合。不保留 selection= 别名或公开 Selection 包装类。Exclude(*, task_type: str) 按完整类型名精确匹配；无匹配时无操作，None 表示不主动排除。命中任务及受影响下游本次留队，对后续派生任务同样生效；不改变排序，不提供运行中修改规则的接口。本版不实现交／并／补或自定义规则协议，但将来可通过单个组合规则对象扩展同一参数，不将“只支持简单规则”规定为永久限制。删除 Skip 及其终态跳过、skipped Attempt 语义。主动终结由 Run 外 set_job_status(..., "failed", recursive=...) 表达，不提供独立 cancel 接口。再次入队仍服从 Trigger，不增加永久禁止该 UID 或优先于 always 的屏障。

内部准入统一检查：Run 允许新派发、用户排除规则允许、当前 UID 未被本 Run 临时排除、依赖与资源就绪。临时排除仅存内存，下次 Run 清空；复用排除规则的依赖阻塞与停摆判断，不修改 OrderingPolicy 的排序职责，不新增公开的运行期 pending 控制 Interface。排除规则不能代替在途执行终止、清理或数据库结算。

### Task、Job 与注册签名

身份字段及 handler 可按位置传入，其余配置均为关键字参数：

```text
Task(task_type: str, handler, *, resources=None,
     max_retries_per_run=3, timeout_seconds=3600, trigger="once")
Job(task_type: str, job_id: str, *, payload=None, depends_on=None,
    files=None, resources=None, max_retries_per_run=None,
    timeout_seconds=None, trigger=None)
register_task(task: Task) -> None
register_task(task_type: str, handler, *, resources=None,
              max_retries_per_run=3, timeout_seconds=3600,
              trigger="once") -> None
```

两种注册形式使用同一模板配置契约；传入 Task 对象时不得附带 handler 或其他配置覆盖。重复注册同名 Task 明确拒绝。Job 构造不接受 runtime、attempt_no、activation_no、first_enqueued_at 等内部运行字段。

Task 是执行配置模板；payload、depends_on、files 仅属于 Job，不增加模板默认值。Job 的三者省略或为 None 时分别规范化为独立空字典、空列表、空字典；payload 要求 JSON 对象，depends_on 为 UID 序列，files 为名称到 FileSpec 的映射。资源按名称继承并覆盖，空映射不清空 Task 资源。三个标量的 None 表示继承 Task，具体值覆盖；max_retries_per_run 为非负整数，timeout_seconds 必须为有限正数，不新增无限时或关闭超时的特殊值。

删除 Task 与 register_task 的 payload_schema 参数及其框架业务结构校验，不提供替代验证器。框架只检查 payload 能否按 JSON 对象契约保存；业务字段、类型与值域由用户负责，不从注解自动推导运行时验证。

handler 调用固定为 handler(job: Job, ctx: JobContext) -> TaskResult，且仅同步执行。job 提供身份、业务输入和本次生效配置；ctx 提供文件声明／路径读取、派生和游标操作。修改 worker 内 Job 对象不代表修改主进程队列或状态，不提供 payload-only、ctx-only 等签名自动适配。

外部提交时解析并保存有效配置；派生任务按父成功提交时的配置解析契约处理。已经接纳的 pending 跨 Run 继续使用快照，不因新进程 Task 默认值改变而刷新；新提交的 Job 使用当前模板。handler 代码始终来自本次注册，不持久化代码版本。

### 依赖闭合与动态派生

采用 KISS 原则：声明先于引用，成功才满足依赖，缺失或成环直接失败，不猜测未来生产者。外部 enqueue 只能引用已存在的 Job（队列或当前终态档案）；不存在则当场拒绝，不入队、不生成失败档案或 Attempt。依赖存在不要求已经成功，pending/running 的上游允许被引用。仅有历史 Attempt 不作为依赖存在或成功的证据。

ctx.spawn 只能引用派发时已知的 Job、当前父 Job，以及本次 handler 此前已经 spawn 的 Job；不允许向后引用稍后才声明的 Job，也不依赖其他并发 worker 未来提交的任务。声明时捕获快照，父成功结算时由主进程按声明顺序最终校验；任一派生不合法，父任务失败，整个派生集合与游标更新不发布。声明顺序建立引用，成功事务保证整体可见，不新增批量提交 API。

| 运行时上游事实 | 下游行为 |
| --- | --- |
| 有活跃 pending/running 候选 | 等待本次结果，历史成功不能绕过活跃候选 |
| 被排除规则或授权拒绝本次排除 | 下游本次排除并留队，不当作依赖缺失 |
| 无活跃候选且有 successes | 依赖满足，再检查输入及具名产物 |
| 无活跃候选且只有 failures | 失败并级联 |
| 记录缺失，包括提交后被人工清除 | 失败并级联，不等待或从 Attempt 补推成功 |

Trigger False 只移除本次候选，不成为独立的依赖成功证据；移除后仍按上述终态档案判断。删除缺失依赖宽限期和潜在派生者猜测。保留实际依赖环检测，环内任务及受影响下游失败，未执行不生成 Attempt；不等待或自动修复。禁止悬空引用不意味着跨 Run 复用 UID 时天然无环，不为省去检测新增依赖配置终身不可变的限制。

本版本不引入 Group、Join、Task 类型栅栏或组计数器。文档提供动态分支汇总用法：生成者先逐一声明子任务并收集 UID，再声明依赖这些 UID 的汇总任务，最后返回成功。子任务数可在 handler 运行时确定；汇总依赖具体实例，不能将某个 Task 类型解释为封闭任务组。以下仅示意既有接口的组合，所有派生随父成功原子发布：

```python
def generate(job: Job, ctx: JobContext) -> TaskResult:
    child_uids = []
    for item_id in discover_item_ids():
        child = Job("process", item_id)
        ctx.spawn(child)
        child_uids.append(child.uid)
    ctx.spawn(Job("summarize", "summary", depends_on=child_uids))
    return TaskResult.success()
```

UID 是逻辑身份，不是完整代码、输入或外部环境的内容寻址。提交去重不等于 handler 副作用幂等，也不保证执行恰好一次。派生原子提交复用现有状态事务，不新增 Outbox 或消息转发层。

### 透明且可读的 Job 身份

Job 接收原始非空字符串 task_type、job_id，不要求用户预编码或手工拼接 UID。不隐式转换类型、不去空格、不改大小写、不做 Unicode 归一化。身份组合只转义百分号、冒号和不可打印字符，使用可逆 UTF-8 %XX；其他可打印字符原样保留，以 :: 分隔两个已转义组成部分。原始属性保留原文，不截断、不追加哈希、不施加文件名长度限制。

| 稳定入口 | 契约 |
| --- | --- |
| make_job_uid(task_type: str, job_id: str) -> str | 校验原始字段并生成规范 UID |
| parse_job_uid(uid: str) -> tuple[str, str] | 严格解析规范 UID，返回两个原始字段 |
| Job.uid: str | 只读属性，与 make_job_uid 的结果一致 |

身份工具主动检测到非法字段或 UID 时抛 TaskLiteError；Python 调用自身的异常原样保留。解析不猜测、不修复、不自动识别预编码输入。保证 parse_job_uid(make_job_uid(a, b)) == (a, b)。原始字段中的冒号由框架透明转义，但文档示例不鼓励使用冒号；此类输入仅在规则说明和边界测试中展示。

```python
from tasklite import Job, make_job_uid, parse_job_uid

job = Job("process", "report-001")
uid = make_job_uid("process", "report-001")
task_type, job_id = parse_job_uid(uid)
```

这属于本轮破坏性窗口内的身份简化，稳定承诺从新基线开始，不承诺新旧编码输出一致。纯文件 IPC／锁文件用途不纳入新的身份 Interface；仍有其他用途的旧编码工具须在导出清单中逐项决定，不盲目整体删除。

## 结果、重试与记录

handler 只支持同步调用；正常返回必须为 TaskResult。主动表达只有同级的三个结果：

```python
TaskResult.success(metadata={"count": 12})
TaskResult.failure(message="无法处理", metadata={"reason": "unsupported"})
TaskResult.retry(message="暂时不可用", consume_budget=True, metadata={})
```

三个结果构造方法的完整参数如下，均只接受关键字：

```text
TaskResult.success(*, message: str | None = None, metadata: dict | None = None)
TaskResult.failure(*, message: str | None = None, metadata: dict | None = None)
TaskResult.retry(*, message: str | None = None, metadata: dict | None = None,
                 consume_budget: bool = True)
```

metadata=None 规范化为空字典；message 为可选的可读说明，不强制失败或重试填写原因。metadata 是用户结构化业务结果，二者均不参与调度判断。consume_budget 只属于 retry；exception、annotations、文件记录由框架生成，不作为 TaskResult 构造参数。

- 不再接受 None、bool、dict、二元组作为隐式结果。
- retry 的 consume_budget 默认 True；零预算重试必须显式 False。返回 retry 不操纵资源；本版本不提供资源挂起能力。
- 未捕获异常、超时、worker 意外退出统一 failed，不自动归因后重试。能够捕获的异常、退出信息保留原文及实际证据；确实无原因才记录未知。不猜测原因。
- 不提供业务主动抛出重试异常的平行协议，也不保留异常类型自动映射重试。可恢复错误由 handler 捕获并返回 TaskResult.retry。
- 不增加默认等待、自动退避或零预算重试限次。文档说明没有资源准入等待或自定义节奏时，零预算重试可能持续立即执行；节奏仍通过既有 RequeuePolicy Seam 扩展。
- HTTP 等 wrapper 必须适配显式结果协议，不再靠隐藏的重试异常控制 handler。

预算字段统一为 **max_retries_per_run**：每个 Job、每次 run() 独立，在内存计数，不新增持久化预算字段。首次执行之外，最多允许该数量的消耗预算重试。预算耗尽记 failed，不自动启动下一次 Run。同一 Run 再次入队保留消耗；新的 run()（包括同进程 stop 后再次调用）从零计算。跨进程重启不保留已消耗预算。

每次持久进入 running 均推进该 Job 的 attempt_no，从 1 开始，跨 Run 单调递增，包括零预算重试；保留 run_id 表达执行归属，删除 activation_no。编号不是预算计数，人工状态修改不推进编号，清理 Attempt 历史不重置或复用编号；编号进度属于 Job 的内部运行信息，必须独立于可清除的队列／终态／Attempt 记录存活；进入 running 的同一事务推进。必要时保留内部每 UID 编号记录，不新增持久化预算或执行日志。该记录不作为依赖存在或成功的证据，也不让 get_job() 返回不存在的当前任务。结果结算时才追加 Attempt，不预先插入或更新 Attempt.running。尚未进入 running 的选择排除和授权拒绝不产生 Attempt；进入 running 后未启动 worker 就异常中断，也按未结算尝试处理，不探测 handler 是否执行到第一行。

| 结果 | 业务 metadata | 派生任务与游标 |
| --- | --- | --- |
| success | 写本次 Attempt 与成功档案 | 全部校验通过后同成功事务提交 |
| failure | 写本次 Attempt 与失败档案 | 不发布 |
| retry | 写本次 Attempt；耗尽时带入失败档案 | 不发布 |

框架身份、时间、原因、输入和产物描述与业务 metadata 分离，不向业务字典注入保留键。metadata 采用严格 JSON 对象契约。

成功档案增加结构化 annotations，保存可扩展的框架附加信息，与业务 metadata 分离。completion_source 是框架定义的键，取值为 execution 或 manual，分别表示实际执行成功和人工声明成功；不另设 origin 或独立的同义字段。note 可选，用于说明，框架不解析其文本来决定行为。允许未来增加可选键，读取方忽略未知键；已定义键的含义与类型必须在公开类型提示和契约中明确，不能用任意字典代替行为契约。

```python
annotations={"completion_source": "manual", "note": "已由使用者确认完成"}
```

人工声明成功统一通过 set_job_status 表达，完整配置来自传入 Job 或 UID 对应的既有快照。人工成功对应 Job.succeeded，不增加第五种状态，不生成 Attempt。可记录人工登记时间，但不得伪造成执行完成时间；once、until_success 认可人工成功，on_input_change 不把人工登记时间当作可信执行成功时间。

所有以 Job 为主体的记录都完整保存 Job 快照：队列保存已接纳的执行配置，successes 保存最近一次成功对应的配置，failures 保存失败或取消对应的配置，attempts 在结算时保存该次执行接纳时的配置。快照包括身份、payload、依赖、FileSpec 和解析后的有效执行配置，不保存 handler 代码；handler 由当前注册 Task 提供。不通过可变的“最新 Job”记录间接引用历史。

人工重跑统一使用 set_job_status(..., "pending", recursive=...)，恢复完整快照并移除对应当前终态档案，保留 Attempt 历史，不静默套用新 Task 默认值。不增加强制执行标记或绕过 Trigger；once 因终态档案已移除而自然允许执行。max_retries_per_run 配置入快照，已消耗预算仍只在内存。动态文件声明、业务结果和框架原因单独记录，不倒改执行开始时的快照。游标和路径授权不是 Job 记录，不重复嵌入 Job 快照。

父成功、派生任务、游标必须整体接受和提交；坏子任务不再独立失败而允许父任务成功。数据库环境故障不得伪装成业务结果；保留未提交任务的恢复条件。文件与外部请求不属于数据库事务，不能承诺回滚。

本版本删除公开回调 on_run_start、on_attempt_finished、on_run_end，包括构造参数与对应属性，不提供替代事件订阅接口。Run 前后操作由调用方围绕 run() 编排；执行过程使用框架日志，持久化执行结果通过只读查询观察。移除专用于完成回调的 progress_hook 便利工具。此取舍减少公开生命周期及事件载荷契约，不影响内部统一结算出口。Attempt.outcome 与结算后的 Job.status 仍分别表达执行结果和任务去向：授权拒绝可以同时是 Attempt.failure 和 Job.pending，预算耗尽后的实际结局是 failure；取消和 Trigger 跳过不伪造 Attempt。不新增 interrupted 结果；停止与尚未完成记录的映射见后文。

## 资源协议与事件驱动等待

Task 始终是 Job 的模板。资源需求按名称继承，同名 Job 声明覆盖 Task 默认值，未声明的键保留，空字典不清空模板。用量 0 不定义额外语义，不将其约定为删除资源需求或不消耗但受挂起约束的特殊操作。资源不足统一由正常准入逻辑判断，不新增提交阶段的可满足性预检查。

资源以 Job 的一次执行为分配单位，本版本只提供官方内置资源，不设计或冻结对外资源扩展接口。以下资源协议是内部实现约束，不要求用户实现。主进程同一调度线程通过唯一分配入口统一查询、获取和释放；Resource 封装可用性、占用或消费、归还或随时间恢复的具体含义。框架不通过 isinstance 判断限速或容量类型，不直接修改资源内部字段，不将某一种内置限速算法提升为所有资源的统一模型。

保留 can_acquire(amount)、acquire(amount)、release(amount) 的职责分离：查询不预占或消费；获取正常返回表示成功；释放结束本次获取的生命周期，但不保证撤销已经消费的配额。资源方法由主进程串行调用，必须及时返回，不在方法内部睡眠等待资源。资源扩展自行管理自身状态；变更通知不授予派发权，也不允许通知来源直接获取资源。

OrderingPolicy 决定候选检查顺序，选择最靠前且满足全部启动条件的 Job；前面的候选资源不足时保留 pending，继续检查后面的候选，不实行严格队首阻塞，也不为等待者预占部分资源。每次分配后按最新资源状态判断后续候选。

同一 Resource 可以同时供多个 worker 使用，但 worker 与通知线程不调用资源分配或回收方法。授权后的全部资源检查到连续获取完成之间，不插入其他 Job 的分配或回收；不引入多执行器竞争、try_acquire、两阶段预留或部分占用等待协议。外部通知可以提交待处理变化，分配账目仍由主线程处理，不把通知线程变成另一分配者。

资源查询区分三种含义：可用；暂不可用（给出下次检查时间或等待通知）；明确不可满足（说明原因）。明确不可满足由正常资源查询返回，目标 Job 及依赖下游按失败规则收敛，未执行不制造 Attempt；资源方法抛异常则属于 Run 级框架故障。三态的内部类型与字段由实现确定，不能用未知等待时间推断不可满足。

正常顺序为：准入与资源检查 → 必要文件授权 → 重新检查全部资源 → 持久化 Job.running → 逐项获取资源 → 产物准备与 worker 执行 → 收到有效结果及必要清理 → 释放已获取资源 → 原子提交结果。等待初始文件授权期间不占用业务资源或消费配额；资源异常遵循框架故障规则，不承诺多资源获取的事务回滚、补偿或异常后完整释放。

### 内置资源的公开使用

```python
from tasklite.resource import CapacityResource, IntervalResource

tasklite.register_resource(CapacityResource("slots", max_capacity=4))
tasklite.register_resource(IntervalResource("requests", interval_seconds=2))
```

构造签名为 CapacityResource(name: str, *, max_capacity: float) 与 IntervalResource(name: str, *, interval_seconds: float)，注册入口 register_resource(resource) -> None。仅资源名支持位置参数。用户构造、注册并在 Task／Job 中声明用量；can_acquire/acquire/release 是主进程内部协议，本版不承诺用户直接调用或自定义资源扩展。同名资源重复注册明确拒绝，内部 worker 容量不能通过资源注册覆盖，最大并发只通过 max_workers 配置。

RateLimitResource 改名 IntervalResource，不保留旧名别名，不改变现有间隔算法：本次获取获准后，下次获取至少等待 interval_seconds × amount，任务结束不返还时间配额。首次可立即获取；用量影响后续等待，不要求本次先积攒足够配额。它约束资源获取／Job 派发节奏，不控制 handler 内部外部请求，不承诺令牌桶或滑动窗口限速。文档示例通常使用 amount=1。

### 等待与通知契约

**不采用周期轮询，也不通过最大复查间隔定期扫描资源。** 资源可用性变化只有两类主动唤醒依据：

| 资源提供的信息 | 主进程行为 |
| --- | --- |
| 已知下一次可能可用的时间／等待时长 | 安排一次到期检查；主循环仍处理其他事件，不按该时长阻塞睡眠 |
| 容量释放或其他外部状态变化通知 | 唤醒主进程，提前重新检查相关候选，不受原等待时间限制 |

两种来源可以同时存在。通知只表示“状态可能变化”，不表示已获取资源；收到通知或计时到期后仍重新调用 can_acquire，由主进程统一决定获取与派发。提前通知到来时，旧计时不能阻止复查。例如原预计一小时后可用，十秒后发生释放并通知，主进程应提前处理，无须等满一小时。这里的及时响应不承诺硬实时延迟。

**内部资源实现若不知道下次恢复时间，必须在相关状态变化时通知调度器；若既不提供下次检查时间，也不发出变更通知，就不能保证等待它的 Job 会被重新调度。** 不用周期轮询替资源作者掩盖缺失通知，也不因等待很长或未知就擅自推断永久失败。已知时间资源可直接使用框架的一次计时，无需自行创建周期计时线程；外部变化由资源适配器接收并发出通知。

检查与等待之间到来的通知不得丢失。通知机制应保留待处理变更或使用变更序号，同一资源的重复通知可合并，但不能因合并丢掉检查后的变化。通知可能来自外部事件线程，其入口须安全地唤醒主进程，不在通知线程执行调度或资源获取。绑定、解除、等待时间表示、通知入口和具体同步机制均属于本版本的 Implementation，不作为公开扩展接口冻结。

### 不提供资源挂起控制

本版本删除 ctx.suspend_resource()、资源挂起查询及持久化／恢复，不新增 resume_resource()。也不增加 Resource.on_result 或主进程业务结果控制回调，不将 429 或业务 metadata 隐式转换成资源操作。这替代此前资源挂起作为独立即时效果的决定。

正常容量约束、限速和自定义可用性变化继续存在，主动变更通知不等于重新引入挂起 API。handler 可显式返回 TaskResult.retry，重入队节奏仍通过 RequeuePolicy；Resource 不解释业务结果、不决定 Job 是否重试。资源对象自行管理状态，框架不在 Run 边界自动重置，也不提供通用持久化；同一对象保留自身状态，进程重启后的恢复由具体资源实现负责。异常后的资源占用不保证自动恢复，不能承诺原对象直接安全复用。通知按每 Run 绑定和解除，解除不清空资源状态；新 Run 首次检查读取当前状态，旧通知不能作用于新 Run。

## 统一文件声明

采用一个 FileSpec，以必填 role 表达 input、output、cache 三种内置用途；不提供自定义 role 或文件扩展注册，不保留三种分类类，也不在 Job 并列 inputs/outputs/caches。名称只由 files 字典键或 declare_file 的第一个参数指定，不在 FileSpec 中重复配置。

```python
FileSpec(
    path: str | Path,
    *,
    role: Literal["input", "output", "cache"],
    delete_existing: bool = False,
    delete_on_failure: bool = False,
    delete_on_success: bool = False,
)

FileSpec.from_output(
    producer_uid: str,
    output_name: str,
    *,
    role: Literal["input", "output", "cache"],
    delete_existing: bool = False,
    delete_on_failure: bool = False,
    delete_on_success: bool = False,
) -> FileSpec

ctx.file_path(name: str) -> Path
ctx.declare_file(name: str, spec: FileSpec) -> Path
```

上述为目标签名示意，不是可直接执行的代码。普通路径与上游引用都返回同一 FileSpec；构造只保存声明，不要求产物此时存在。file_path 和 declare_file 统一返回规范绝对 Path。file_path 读取当前 Job 已声明路径，不重新准备或删除；declare_file 登记声明、完成必要授权和请求的准备操作后返回。文件或目录共用 path，不要求 is_dir，不自动创建文件、目录或父目录；由 handler 或外部程序负责创建。“产物准备”不暗含目录创建。

| role | 用途 | delete_existing | delete_on_failure | delete_on_success |
| --- | --- | --- | --- | --- |
| input | 记录输入，参与输入变化判断 | 禁止 True | 可配置，默认 False | 可配置，默认 False |
| output | 成功时发布具名产物描述 | 可配置，默认 False | 可配置，默认 False | 可配置，默认 False |
| cache | 辅助文件，不参与输入基线、不发布产物 | 可配置，默认 False | 可配置，默认 False | 可配置，默认 False |

全部删除参数默认 False；失败清理也适用于重试。此决定替代 input 不接受任何清理策略及 cache 固定成败清理的旧设计。role 表达用途，清理时机由用户显式决定。成功后可以删除 input，仍须遵守主进程删除授权与受保护路径规则。

相同名称、相同有效声明幂等复用，不重复前置删除；同名不同声明报冲突。允许不同名称指向相同路径或重叠目录，也允许同一路径分别声明为 input 和 output。使用者负责清理策略之间的关系，不因路径重叠取消某项明确请求；此决定替代旧声明对同 Job 路径重叠的一概拒绝。请求删除的目标已不存在视为完成；实际权限或其他删除错误仍按失败处理。

### 上游产物引用

from_output 保存生产者 UID 与精确的 output 名称，不自动加入 depends_on，不创建或入队生产者。生产者必须属于当前 Job 显式直接依赖：预声明在提交时检查，动态声明在调用时检查，不在运行中补依赖或挂起 worker 等待新生产者。不省略名称、不自动取唯一产物或第一个产物，也不增加通配／批量引用。

```python
producer = Job(
    "generate", "report-001",
    files={"report": FileSpec("results/report.csv", role="output")},
)
consumer = Job(
    "process", "report-001",
    depends_on=[producer.uid],
    files={
        "source": FileSpec.from_output(producer.uid, "report", role="input"),
        "result": FileSpec("results/processed.csv", role="output"),
    },
)

# consumer 的 handler 统一按本地声明名称读取。
source = ctx.file_path("source")
extra = ctx.declare_file(
    "extra", FileSpec.from_output(producer.uid, "report", role="input"),
)
```

引用来源与当前用途分开，通常使用 role="input"，不额外禁止 output 或 cache。预声明在依赖就绪后、输入检查与 Trigger 判断前解析；动态声明在调用时解析并按 role 登记。以 input 使用时无需另行调用输入声明。Job 配置快照保留引用本身，该次执行实际使用的解析路径另行记录；不在提交时绑定可能过期的成功产物。生产者未发布该名称，或解析为 input 时实际文件不存在，明确失败，不将产物描述视为文件存在的证明。

消费者不继承生产者的清理策略或删除授权，按自身声明和路径授权处理。所有已声明文件统一经 ctx.file_path 访问，不再另设 get_artifact_path、upstream_output_path 或 input_file_path 等平行读取入口。

输入与输出均取预声明和运行时动态声明的并集，不采用一方替换另一方的规则。动态信息只有声明后才可见，不能反向影响已经完成的本次启动判断；成功记录保存合并后的声明，供后续观察和启动判断使用。不因引入预声明而新增“输入集合变化即强制重跑”的规则。

不同 Job 的同路径或重叠目录范围由使用者负责协调；框架不新增路径独占锁、冲突等待或运行时路径冲突判失败机制。主进程集中操作不等于保证业务文件并发安全，用户目录授权也不是独占许可。

创建 TaskLite 实例时捕获主进程工作目录，作为该实例固定且统一的相对路径基准。enqueue、ctx.spawn 及运行时 ctx.declare_file 均使用此基准，不读取主进程或 worker 后来变化的工作目录。这只解释框架路径参数，不自动改变 handler 或外部程序的实际工作目录。此规则替代每次 enqueue 按调用时工作目录解析的决定。

enqueue 接受提交时，按当时注册的 Task 解析有效配置，将普通路径声明中的相对路径按实例固定基准解析为绝对路径，捕获独立快照。同 UID 幂等比较使用解析后的有效配置，不按是否显式传参区分。之后工作目录或注册默认值变化不改变已接受的配置。人工重跑保留历史快照中已解析的绝对路径，不重新解释已有声明。

使用同一状态库接续执行、恢复或重跑既有 Job 时，使用者必须保持主进程路径基准一致。跨目录接续属于本版本不支持的使用方式，不检测也不保证其执行语义；既有路径授权和受保护路径规则仍然有效。不承诺通过历史基准继承整条派生链来支持跨目录重跑。执行目录绑定检查留待后续版本立项，本版本不新增检测或持久化机制。

工作目录只是相对路径解析基准，不是文件访问边界。允许绝对路径和 ../ 指向工作目录外；默认不限制为工作目录内，只有用户显式配置 output_roots 时才额外限制框架操作范围，不隐式拼接路径。handler 及外部程序可以使用独立的固定输出目录，业务写入由用户全权负责。构造及入队不创建、删除或修改业务文件。执行顺序固定为：

**准入、依赖、输入、Trigger 及资源检查 → 必要路径授权 → 再次检查全部资源 → 持久化 Job.running → 获取资源 → 产物准备／前置删除 → 启动 worker。**

running 写入失败时禁止产物准备和 worker 启动。该记录只证明一次执行已被持久接纳、不证明 handler 已经开始；不将文件操作和进程创建伪装成数据库事务的一部分。

无需执行的任务不触发产物准备或删除。动态声明在调用时向主进程登记，并等待所请求的准备操作及其必要授权完成，才返回路径。纳入框架管理的产物必须先声明、完成准备，再写入；禁止本次写入后补声明。这是管理操作的顺序契约，不是对所有业务写入的权限接管；声明本身不授予或限制写权限。既有文件可以声明并显式要求覆盖，不以路径已存在就断定违规。

保留 delete_existing、delete_on_failure、delete_on_success 的时机区别。这些配置本身就是使用者向主进程发出的声明式删除请求；动态声明同样提交请求，不要求 handler 再调用一次删除方法。所有 role 都仅按显式配置请求清理，cache 不再自动清理。未请求的清理不擅自执行。请求不能替代授权，获得目录授权也不等于框架可以任意自动删除目录内容。

成功结算发布全部 output 声明，不因产物已删除而省略发布；允许 output 配置 delete_on_success，不强制改为 cache。发布的是产物描述，不保证路径仍存在、内容完整或不被其他任务修改；不能将成功产物记录视为磁盘可用性证明。此决定替代既有 ADR 对具名产物成功删除的限制，不增加按删除策略过滤发布的逻辑。

产物清理是便利包装，但请求的操作仍属于执行契约，该失败就失败：

| 清理故障 | 处置 |
| --- | --- |
| 前置删除失败 | 不启动 worker，结算为 failed |
| handler 成功但成功清理失败 | 结算为 failed，不提交成功档案、产物发布、派生任务或游标 |
| handler 已失败且失败清理再次失败 | 保留原始失败原因，附加清理错误 |
| 重试所需清理失败 | 结算为 failed，不继续重试 |

上述失败对应 Attempt.failure 和 Job.failed。已经发生的文件操作不回滚；数据库不可写时暴露错误，不声称已登记失败。授权拒绝不是获准执行后的清理故障，仍按本 Run 级联排除、保留 pending 的规则处理。

## 主进程所有权、授权与通信

框架所有按声明请求的前置删除、成功清理、失败／重试及停止相关清理全部由主进程执行。worker 只请求、等待准备结果及写入业务内容，不直接执行框架清理原语。

文件声明用于进程与产物管理、输入输出依赖传递和后续 Run 判断，不是权限系统。handler 的直接文件写入及其调用的外部程序写入由用户全权负责。本版本不提供 handler 文件访问沙箱，FileSpec 不包含 sandbox 开关；这替代此前独立可选沙箱的范围决定。保留可选 output_roots、框架文件操作授权及受保护路径规则，不允许借由逐文件开关绕过。保护不接管业务写入，不承诺拦截所有写后声明。

- 读取默认允许。框架首次请求需授权的写入、覆盖或删除操作时，展示规范绝对路径；文件授权覆盖该文件，目录授权覆盖该目录树。写入、覆盖和删除不拆分用户授权粒度。
- 授权持久化，跨 Run 和进程复用，提供查询与撤销。同路径未来内容同样在授权范围内，实际目标变化需重新确认。
- TASKLITE_AUTO_APPROVE=1 表示自动同意本进程实际请求的框架文件操作，记录实际授权路径，不预先授权整个工作目录，也不绕过受保护路径等硬性禁止规则。库不解析宿主命令行；用户可在自己的 CLI 中将选项接到此环境变量。
- 主进程询问。等待时暂停新派发，继续收消息、结算其他在途任务及处理超时；等待授权的 worker 不计执行超时。
- 拒绝授权或无法询问时，将当前 Job 及依赖它本次执行结果的直接、间接下游加入本 Run 临时排除，保持或回到 pending；无关任务继续，不因此结束整个 Run。不得在同 Run 重复派发和询问，也不得将这条依赖链误判死锁或级联业务失败。
- 临时排除同时作用于后续派生任务；活跃上游遮蔽历史成功，不能用上游旧成功绕过本次排除。正常依赖调度保证这些下游尚未派发，不增加批量终止下游 worker 的机制。排除不落库、不消耗预算、不发布成功效果，下一 Run 重新判断启动与授权。
- 运行期声明受阻时，先结束当前受阻执行并完成结算，才能把 running 改回 pending；仅修改排除规则 或发出拒绝消息不能证明 worker 已结束。不执行未经授权的清理。外部停止同样解除未完成授权等待。

授权管理仅提供 TaskLite.list_authorizations() -> list[dict] 与 revoke_authorization(path: str | Path) -> bool，均限 Run 外。查询记录包含规范绝对路径及文件／目录目标类型，结构样本如下：

```python
{
    "path": "/abs/path/to/target",
    "target_type": "file",  # 或 "directory"
    "authorized_at": "2026-11-08T08:30:00.123456Z",
}
```

撤销精确匹配规范化路径，删除成功返回 True，不存在返回 False；撤销目录下的某个子路径不能覆盖父目录授权，必须撤销对应目录记录。不增加拒绝列表、手工授权入口或授权管理器。

动态声明现在需要“请求—主进程决定并准备—回复—worker 继续”的往返。因此扩展一次执行内的响应通道，绑定执行身份，拒绝过期回复并处理停止唤醒。传输选择留在 Implementation，不暴露给 handler；不增加持久消息、跨重启重连或自动补发删除。文件已删除但回复丢失时，不能声称文件副作用已回滚。

## 生命周期与观察

### Job 与 Attempt 的持久化职责

Job 的 pending、running、succeeded、failed 是同级、互斥的当前逻辑状态；successes、failures 是终态档案名称，Attempt 是独立历史。重新入队的候选仍需读取前次终态供 Trigger 判断，不能因为存在旧档案就把本次 pending 或 running 误判为已结算。

```text
pending → running → succeeded
                  → failed
                  → pending
pending → failed（取消等执行前终结）
```

队列中的 Job 持久保存 pending/running 及本次执行归属，复用框架运行信息，不增加独立执行日志、心跳或持久化预算。运行时的进程句柄与临时排除仍在内存。StateSnapshot 的 pending 查询必须排除 running，不能将整张队列表直接解释为待派发集合。

主进程得到结算结果后，在同一个显式写事务内追加 Attempt、更新 Job 去向及对应档案；成功时还包括派生任务和游标。Attempt 只追加已结算记录，不保存 running 阶段。不允许先写 Attempt、再另一个事务改 Job，或反过来分步落盘。数据库失败时不更新内存假装结算完成。

| 结算依据 | Attempt.outcome | Job.status / 去向 |
| --- | --- | --- |
| success 且全部成功效果有效 | success | succeeded，写 successes |
| failure、未捕获异常、超时或无可信结果退出 | failure | failed，写 failures |
| retry 且预算允许 | retry | pending |
| retry 但预算耗尽 | failure，注明预算耗尽 | failed，写 failures |
| 动态声明授权被拒绝或等待被停止 | failure，记录实际授权原因 | pending，不写 failures |
| 启动时发现上次遗留 running | failure，注明未结算、结果未知 | failed，写 failures |

尚未进入 running 的临时排除、授权拒绝、依赖／资源等待没有 Attempt。Trigger False 移除本次候选而不制造成功或 Attempt。Run 外取消写 failures，但没有执行就不伪造 Attempt。pending 不表示从未产生外部副作用，只表示当前没有未结算执行且仍待处理。

### worker 返回、超时与正常收尾

worker 启动前完成必要授权、资源复查、Job.running 持久化、连续资源获取及主进程产物准备。worker 只执行 handler 并通过 Pipe 返回声明和结果，不拥有资源分配权。

主进程收到完整、有效的 TaskResult，即认为本次执行已经返回并开始结算，不将 worker 进程退出作为额外成功条件。success、failure、retry 仍按各自规则处理；仅收到进程退出、没有有效结果，不是成功，仍按失败结算。handler 应在返回前完成自己负责的工作，框架不保证返回后残留的后台线程或外部程序已结束。

正常收尾顺序固定为：**收到有效结果并完成结果及成功效果校验 → 主进程执行必要文件清理 → 释放本次已获取资源 → 原子提交 Attempt、Job 去向及成功效果 → 继续调度。** 清理失败按已定规则改判失败；资源释放异常使 Run 失败，本次尚未提交，不承诺补偿。资源已释放而提交失败，也不宣称本次已结算，遗留 running 下次按恢复规则处理。释放触发的通知只标记变化，不在当前结算完成前插入新派发。

进程句柄、Pipe 和接收线程的回收属于 Implementation，不增加“结果返回后还必须限时退出”的业务判定，不以阻塞 join 作为收到结果后的结算前置条件。进程回收的具体实现仍须验证不会阻塞主循环或遗留运行句柄，但不借此改写有效结果。

timeout_seconds 限制单次 Attempt：成功启动 worker 后开始计时，主进程收到有效结果后停止计时。排队、初始文件授权、资源等待、前置删除，以及返回后的清理和数据库提交不计入执行超时；运行期等待文件授权暂停计时。超时仍未返回有效结果时终止 worker，记录 Attempt.failure、Job.failed 并正常级联，不自动重试，不将其解释为整个 Run 的时限。

### Run 的派发与停止

run() 可重复调用。stop() 只发出停止当前 Run 新派发的请求，由 run() 等待在途结算，重复调用幂等，无活跃 Run 时无操作，不影响下一次 Run。close() 释放资源且运行期拒绝调用，重复关闭无操作，支持 with TaskLite(...) 自动关闭；关闭后注册、提交、运行和管理操作明确报错。正常阻塞 run() 返回后不要求再 stop()。

删除 run_graceful()，由 run() 统一处理正常运行与 Ctrl+C 收尾。不提供 stop(force=True) 或其他公开强制终止方法。第二次及后续 Ctrl+C 仍保持正常排空，并提示正在等待在途任务，不隐式升级为强制终止；需要立即退出时由用户外部终止，按已有异常退出与遗留 running 规则处理。

run() -> None：正常完成，包括发生 Job 失败的情况，以及 Ctrl+C／stop() 正常排空后，均返回 None。删除公开 stats 属性、RunSummary 和 RunStats，不提供运行摘要。数据库不可写、状态不变式损坏等框架故障直接抛异常。调用者通过 StateSnapshot 查询或比较持久化记录；快照不能完整还原 Run 退出原因，不为重建摘要增加持久化状态。

Trigger、排除规则、Resource 等框架扩展抛异常时，run() 直接失败并抛出异常，不将其当作普通 Job 失败后继续调度，不承诺正常排空、资源补偿、完整释放或逐条结算。Resource 的查询、获取及释放异常均适用；部分获取后出错不增加跨资源回滚协议。此决定替代此前“Trigger 异常后复用 Ctrl+C 排空再抛出”的方案，也替代 ADR-0008 中强制回收并退回 pending 的规则。

框架故障不保证正常收尾，不代表可以伪造或改写已经提交的结果。已提交记录保留；来不及结算的 running 由下次启动按恢复规则判失败。正常可用的运行循环仍按既定规则处理 worker 返回、超时和清理故障。Ctrl+C／stop() 是有收尾契约的正常停止，与框架故障直接失败分开。

**主进程收到 Ctrl+C 的含义是停止新派发，等待当前任务完成后退出。** 不主动转发给 worker，不记成任务失败；终端可能向整个进程组投递 SIGINT，Implementation 必须保证 worker 行为符合该契约。当前任务仍受执行超时约束，完成结果与清理正常提交。未被正常结算级联终结的未派发任务保持 pending。

停止只关闭新派发，不改变正常结算及失败级联；级联失败优先于未派发任务留队。Ctrl+C 和 stop() 正常排空期间，若在途 A 失败或超时，依赖 A 本次结果的待执行 B 仍级联 failed，记录依赖失败原因；B 未执行则不产生 Attempt。其余未派发任务保持 pending，不增加停止期间抑制级联的特殊规则。

Ctrl+C 关闭本 Run 的派发入口，不通过枚举现有 UID 实现排除。因此在途任务成功时派生的子任务仍原子入队为 pending，但不启动；在途任务返回 retry 时仍判断预算，允许则留 pending、不再派发，耗尽则 failed。同 Run 的预算不因停止请求清零。

等待授权的 worker 无法自然完成：停止时结束其等待与受阻执行，原子追加 Attempt.failure 并将 Job 改回 pending，不能无限等待。原因如实记录拒绝或等待期间停止，不写未知；不消耗预算、不发布成功效果、不进入 failures。不新增 interrupted 结果，不能遗留 Job.running。授权拒绝只触发本次级联排除；Ctrl+C 则停止全 Run 新派发，两者不能混同。

### 下次启动的数据库判定

取得状态库级独占权后，先收敛遗留状态，再开始新 Run 的 Trigger 判断和派发。只读 StateSnapshot 不执行恢复写入。

| 数据库事实 | 启动处理 |
| --- | --- |
| Job 为 pending | 保留，参与新 Run 启动判断，预算从零开始 |
| Job 为 running | 按原执行归属和完整快照，原子追加恢复失败 Attempt、转 failed、写 failures |
| 本次终态已提交 | 保持 succeeded/failed，不受之后进程退出影响 |
| 同次结果已提交但 Job 仍 running 等不变式冲突 | 明确报错停止，不猜测哪个记录正确 |

恢复判定理由为“上次执行未完成结算，实际结果未知”，不猜测 SIGKILL、OOM 或真实死亡时刻，不以残留产物证明成功，也不证明旧 worker 已经消失。恢复记录属于原执行的 Run，记录结算时刻不能冒充进程退出时刻；不改记为新 Run 的执行。完整事务与执行身份校验保证重复启动不会追加重复结算。

| 异常退出发生位置 | 下次可见记录及结论 |
| --- | --- |
| Job.running 提交前 | pending；框架尚未允许准备产物或执行 |
| running 提交后、worker 启动前 | running；保守判 failed，不增加额外阶段探测 |
| worker 执行中或结果尚未提交 | running；判 failed，即使产物已存在 |
| 成功／失败事务提交后 | 保持已提交终态及 Attempt |
| 重试或授权留队事务提交后 | pending 加已结算 Attempt，继续按 pending 处理 |
| 结算或恢复事务中 | 完整提交或整体回滚，下次幂等判定 |

主进程仍在时，worker 被终止且没有可正常结算的可信结果则 failed，保存可得退出证据；若已持有完整有效结果则按正常结算处理。主进程也来不及登记就退出时，统一由遗留 Job.running 规则收敛。数据库不可写则停止并暴露错误，不声称已完成失败登记。

### 只读观察

独立只读入口为 StateSnapshot，不再套外层 reader，不在 TaskLite 重复提供 Job 查询接口：

```python
with StateSnapshot.open(state_path) as state:
    current = state.get_job(job_uid)
    pending = list(state.iter_jobs(status="pending"))
    failures = list(state.iter_jobs(status="failed", task_type="process"))
    attempts = list(state.iter_attempts(job_uid=job_uid))
```

StateSnapshot.open(source: str | Path | TaskLite) 接受数据库路径或 TaskLite 实例，SQLite 与内存实例均返回同一种只读快照，不暴露后端。快照关闭不关闭 TaskLite；内存快照只能在所属实例存活时打开，不提供跨进程读取。不另增 tasklite.snapshot() 入口。

get_job(uid) 返回当前 Job 的 JSON 兼容字典，不存在时返回 None；有 pending/running 候选时优先反映该候选。iter_jobs(*, status=None, task_type=None) 按当前状态查询，status=None 包含四态；iter_attempts(*, job_uid=None, task_type=None) 查询执行历史。记录采用独立字典，不新增 JobRecord／AttemptRecord 包装类。Attempt.outcome 与 Job.status 分别表达一次结果和当前去向，不能把 Attempt.failure 自动等同于 Job.failed。

不提供 get_success/get_failure、iter_successes/iter_failures/iter_pending/iter_running 等分表便捷入口，也不保留 TaskLite.list_failures 等重复查询。旧终态仍供内部 Trigger 判断，不增加第二套公开档案查询面。StateSnapshot 不提供游标查询或 iter_cursors，游标通过下文的对称入口访问。

一个上下文提供一致快照，可在执行期间打开，不创建、迁移、修复数据库。重新打开才看见新提交；迭代在上下文内完成，已返回记录关闭后仍可用。采用迭代避免强制全量加载，不为分页另增公开接口。公开数据字段需要文档契约，SQL 表结构不承诺公开。待执行记录不是实时操作系统进程清单，修改返回字典不会改变框架状态。

### 查询记录结构与顺序

Job 与 Attempt 使用相同字段命名和分层，不将执行结果混入提交配置。以下是结构示意，省略号仅表示已有配置内容：

```python
# Job 当前记录
{
    "job_uid": "...",
    "status": "succeeded",
    "job": {...},
    "files": {...},
    "result": {...},
    "run_id": "...",
    "attempt_no": 1,
    "started_at": "2026-11-07T08:30:00.123456Z",
    "settled_at": "2026-11-07T08:30:02.123456Z",
}
# Attempt 历史记录：同形字段，但以 outcome 替代 status
{
    "job_uid": "...",
    "outcome": "success",
    "job": {...},
    "files": {...},
    "result": {...},
    "run_id": "...",
    "attempt_no": 1,
    "started_at": "2026-11-07T08:30:00.123456Z",
    "settled_at": "2026-11-07T08:30:02.123456Z",
}
```

job 是完整的有效配置快照，包含身份、payload、depends_on、files 配置及资源、预算、超时、Trigger；不含内部运行账目。普通声明路径遵守提交时绝对化规则，上游文件引用仍保留引用配置，不以解析结果倒改。外层 files 按名称记录本次已解析预声明与已接收动态声明的并集，每项包含绝对 path、role 及 delete_existing/delete_on_failure/delete_on_success，不另设 inputs/outputs/caches。中断时只记录主进程实际掌握的声明，不保证动态信息完整，也不保证文件仍存在。

```python
# job["files"] 配置快照（保留上游引用配置，普通路径已按实例固定基准绝对化）
{
    "source": {
        "producer_uid": "generate::report-001",
        "output_name": "report",
        "role": "input",
        "delete_existing": False,
        "delete_on_failure": False,
        "delete_on_success": False,
    },
    "result": {
        "path": "/work/results/processed.csv",
        "role": "output",
        "delete_existing": False,
        "delete_on_failure": False,
        "delete_on_success": False,
    },
}
# 记录外层 files（本次已解析预声明与已接收动态声明并集，复用同一路径型结构）
{
    "source": {
        "path": "/work/results/report.csv",
        "role": "input",
        "delete_existing": False,
        "delete_on_failure": False,
        "delete_on_success": False,
    },
    "result": {
        "path": "/work/results/processed.csv",
        "role": "output",
        "delete_existing": False,
        "delete_on_failure": False,
        "delete_on_success": False,
    },
}
```

result 有值时统一为下列形状：

```python
{
    "message": None,
    "exception": None,
    "metadata": {},
    "annotations": {},
}
# 有异常时，exception 为：
{
    "type": "ValueError",
    "message": "原始异常消息",
    "traceback": None,  # 有可得文本则保存文本
}
```

message 为说明，metadata 为业务数据，annotations 为框架附加信息；exception 无异常时为 None，不构造公开分类码。当前 pending/running 尚未结算时 result=None，不将上一次重试结果附作当前结果；历史结果通过 Attempt 查询。

执行归属和时间放在记录外层。无对应执行时 run_id/attempt_no/started_at 为 None；人工结算有 settled_at，但不伪造执行身份。Attempt 必须有 run_id 和 attempt_no；started_at 表示 worker 实际启动时间，未启动或恢复时无法确认则 None。settled_at 表示主进程结算时间，不冒充真实退出时刻。时间统一为 UTC ISO 8601 字符串，以 Z 表示 UTC，未知值为 None；不另存 duration。当前 pending 不挂载上一次 Attempt 的结果或执行归属；内部编号进度仍单独保留。

iter_attempts(job_uid=uid) 按 attempt_no 递增返回；iter_jobs、未指定 job_uid 或仅按 task_type 过滤的 iter_attempts 不承诺顺序。查询不增加 order_by、倒序或分页参数；查询顺序不代表调度顺序。

### Run 外管理与游标

常用状态管理仅保留下列入口，不增设管理器包装对象：

```text
tasklite.enqueue(job: Job) -> bool
tasklite.set_job_status(
    job_or_uid: Job | str,
    status: Literal["pending", "succeeded", "failed"],
    *, recursive: bool, note: str | None = None,
) -> None
tasklite.clear_history(
    uid: str,
    *, records: Collection[Literal["successes", "failures", "attempts"]],
) -> int
```

上表为接口签名示意。enqueue 表达提交执行意图；set_job_status 表达人工管理，不调用 handler。完整 Job 可直接创建未知任务的指定状态，无需先 enqueue；UID 必须能找到完整快照。传入 Job 与既有有效配置冲突时明确报错，不隐式替换。不能人工设置 running；遗留 running 先按既定恢复规则处理。

recursive 必须显式指定，无默认值，三种目标状态完全对称。False 只修改目标；True 对目标及已有记录可识别的直接、间接依赖下游设置相同状态，包括已经终结的下游，不推测尚未提交的任务。整个修改原子提交，每个 Job 使用自己的完整快照。pending 清除对应当前终态并恢复队列；succeeded/failed 移出队列并保持成功失败档案互斥。人工成功注明 completion_source=manual；人工失败及级联保留原因和可选 note。以上操作不生成 Attempt，不推进编号，不执行文件清理、派生或游标更新。

clear_history 必须显式指定 records，返回删除记录数，不存在返回 0；仅删除所选档案或 Attempt 历史，不移除队列、不级联、不入队、不清理文件或游标。不保留 keep_fatal 特例。只删 Attempt 不直接修改 Job 当前状态。删除 retry_failure、seed_successes、clear_failures 和独立 cancel，不提供同义兼容别名；不提供 replace_pending 或物理删除待执行队列的另一入口。

清理 Job 终态档案或 Attempt 历史均属于显式、破坏性的运维操作，清理范围及后果由用户自行负责。清理允许使依赖这些记录的查询信息、可信执行基线、Trigger 与依赖判断发生变化，不承诺清理后仍保持清理前的判断结果。例如，清除 Attempt 后失去可信成功基线，on_input_change 可以重新执行，即使输入没有变化。框架按清理后剩余的可信记录判断，不为维持原判断增加清理前置条件、依赖拦截或历史保留保护，不保留已删除历史的隐式副本，也不补造执行证据。

删除 handler 的 has_succeeded、has_failed、succeeded_or_failed_job_uids，handler 不读取任务成败集合，调度依赖判断由主进程负责；可选工具适配这一边界。

游标只是用户自定义的便捷状态记录，不承担框架进度语义。TaskLite 在 Run 外提供 get_cursor(key: str) -> str | None 和 set_cursor(key: str, value: str | None) -> None；JobContext 提供同名同形入口。键属于状态库全局空间，不自动加前缀；None 表示删除，空字符串是合法值。删除 seed_cursor，不提供 iter_cursors。

TaskLite 读取最新已提交值，写入直接提交；worker 读取派发快照加自己的暂存更新，支持读到自己刚写的值。worker 更新仅随父成功原子提交，失败或重试丢弃。多个任务写同键以最后提交为准，不增加锁、合并或冲突重试。

Run 外限制以状态库级互斥落实，而非只检查当前 Python 对象：不同进程的执行与管理写操作互斥，同时允许多个 StateSnapshot 只读观察。不提供绕过执行／管理互斥的公开参数。该限制不妨碍当前执行器自身的状态事务。

## 兼容承诺与非目标

本轮不提供旧库迁移。不支持的旧格式在只读检查后明确拒绝，不自动修复、覆盖或另建内容冒充恢复。开发、验证只使用新临时状态或显式副本，严禁修改、删除旧业务数据和旧业务数据库。

新基线的稳定范围为常用 Interface、官方内置能力的公开使用方式，以及明确保留的身份接口。本版不设计或冻结资源扩展接口，不提供文件扩展，调度扩展暂不承诺兼容、不作为定稿门禁；内部仍可使用资源抽象、OrderingPolicy、RequeuePolicy 等 Seam。HTTP、Discovery、测试辅助等可选工具本版暂不承诺兼容，文档须显式标注。后端只承诺通过 TaskLite 选择官方 SQLite、内存实现，不开放自定义后端注入或协议，也不承诺底层事务方法。内部可变注册表、运行账目和调度状态机不属于稳定 Interface。具体导入名仍按覆盖清单逐项落实。

对上述稳定范围，Python API 与数据库格式分别承诺兼容：同一主版本保留公开调用、默认值与行为，允许增加可选参数及字段；新版本对受支持旧状态提供读取支持或明确迁移方案，不静默丢弃数据。旧版本不保证读取新格式，遇不支持的格式必须明确拒绝，不尝试修改。发布时明确格式支持窗口与回退限制；本次基线仍不迁移存量数据库。内部重构与兼容增加能力允许。

本次不增加分布式执行、操作系统级文件隔离、异步／生成器 handler、批量提交协议、额外调度算法或任意自定义文件执行插件。不记录具体消费项目身份或功能。

## 剩余处置与实施事项

公开面盘点见 [覆盖审计](0011-interface-coverage.md)。以下区分仍需处置的用户可见契约与实现工作，不将已撤出本版的扩展接口重新列为定稿门禁。

### 定稿核查与发布事项

授权管理、初始化、官方后端所有权、内存快照、线程与信号、异常简化及兼容方向均已裁决，不再作为待选方案。核心行为已收口；以下是冻结前必须完成的核查，不代表可以自行增加功能：

1. 模型与注册参数、结果构造、记录字段和默认值已经收束；实施前后对照接口表及样本验证一致性，新出现的实质冲突单独提出，不在实现时暗定。
2. 独立对照代码验证依赖、编号、查询、授权及停止交叉行为的实现可行性，区分真实矛盾和普通实现工作。
3. 确定发布版本号、数据库格式号、支持窗口及迁移／回退说明；当前不修改存量库。

### 导入路径与扩展方向

根包保留执行核心、值对象、只读入口、异常与身份工具；可扩展或未来可能扩展的能力从各自模块导入，不在根包重复再导出。导入位置与兼容承诺分开：内置资源和 Exclude 在子模块中仍属于稳定 API，但不因此开放自定义扩展协议。

```python
from tasklite import (
    TaskLite, Task, Job, JobContext, TaskResult, FileSpec,
    StateSnapshot, TaskLiteError, make_job_uid, parse_job_uid,
)
from tasklite.resource import CapacityResource, IntervalResource
from tasklite.selection import Exclude
```

排序、重入队、Trigger 等现有或未来扩展同样归各自职责模块。HTTP、Discovery、测试辅助等保留各自模块与不保证兼容的范围，不混入稳定根包。删除公开 Selection 包装；未来交／并／补若引入，以单个组合规则对象接入 exclude，不要求当前用户理解额外容器。

### 常用公开接口索引

本表集中登记调用面；具体状态与副作用契约以上文为准。模型／注册签名与查询字段见对应章节；实施时将这些样本转为契约测试，不将内部数据库字段自动公开。

| 对象 | 保留或新增入口 | 调用边界 |
| --- | --- | --- |
| TaskLite | 构造、close、上下文管理、is_running | 内置后端；关闭规则见生命周期 |
| TaskLite | register_task、register_resource | Run 外；Task 简式／对象注册，资源仅内置 |
| TaskLite | enqueue(job) -> bool | Run 外，单 Job |
| TaskLite | run(*, exclude=None, install_signals=True) -> None、stop() -> None | 同一控制线程运行，stop 可跨线程 |
| TaskLite | set_job_status(job_or_uid, status, *, recursive, note=None) -> None | Run 外，三目标态，必须指定 recursive |
| TaskLite | clear_history(uid, *, records) -> int | Run 外，仅明确选中的历史记录 |
| TaskLite | get_cursor(key)、set_cursor(key, value) | Run 外读取最新提交、直接写入 |
| TaskLite | list_authorizations()、revoke_authorization(path) | Run 外查询及精确撤销 |
| JobContext | spawn(job)、declare_file(name, spec)、file_path(name) | worker 声明与文件管理；不读任务成败集合 |
| JobContext | get_cursor(key)、set_cursor(key, value) | 派发快照加自己的暂存写，成功提交 |
| StateSnapshot | open(path_or_tasklite)、get_job(uid)、iter_jobs(*, status=None, task_type=None)、iter_attempts(*, job_uid=None, task_type=None) | 一致只读快照，返回 JSON 兼容字典 |
| 规格与结果 | Task、Job、FileSpec、TaskResult.success/failure/retry | 三层模型、文件与结果章节定义行为 |
| 身份工具 | make_job_uid、parse_job_uid、Job.uid | 透明可逆身份，不受文件名约束 |
| 选择与内置资源 | selection.Exclude、resource.CapacityResource/IntervalResource | 从对应模块导入；不冻结自定义扩展协议 |
| 异常 | TaskLiteError | 仅框架主动拒绝；底层异常原样传播 |

记录字段以“查询记录结构与顺序”章节为准，发布前核对实现与样本一致；不得恢复公开记录包装类或将所有数据库列直接导出。

目标删除面包括：name/state_dir、外部 backend 注入与 setter、strict_picklable、异常分类参数及注册、三个内部调优参数、三个生命周期回调、progress_hook、stats/RunSummary/RunStats、run_graceful/强停/跳过库锁、Selection/Skip、selection=、多规则列表、payload_schema、RateLimitResource 旧名、front/replace_pending、独立取消/重跑/人工成功/失败清理、seed_cursor、handler 成败读取、分表及 TaskLite 重复查询、资源挂起、旧错误分类与 ERR_*。根包不再导出内部后端类作为稳定使用入口；可选工具及未冻结扩展的具体导入另列发布清单，不以现有 __all__ 自动产生承诺。

### 独立可行性核查结论

独立审查对照本记录、覆盖清单、路线图与现有源码，未发现必须重新裁决的阻断性契约冲突。这是静态设计可行性结论，不代表已实现或经过运行验证。实施必须满足以下条件：

- 清空某 UID 的全部档案和 Attempt、关闭后重新提交，该 UID 的编号仍递增；内部编号进度不被历史清理带走，不成为依赖或当前状态查询依据。不得通过剩余历史最大值恢复编号。
- SQLite 快照上下文持有独立只读事务；内存快照固定一个一致版本，不能通过整个用户上下文持有后端锁来阻塞执行。验收需覆盖两次读取之间发生原子提交，仍观察同一版本的场景。
- 有效结果到达后的正常结算与授权拒绝后的中止分别验证：前者不等待进程退出才能结算，后者必须先结束受阻 worker 才能将 running 改回 pending。不能机械复用一条终止／join 路径，也不新增返回后退出期限。

现有实现中编号依赖历史／快照、SQLite 按操作开关连接、内存逐次锁定读取及集中进程回收的结构需要适配；这些是实施工作，不是要求恢复已删除接口的理由。本轮新增签名与记录样本已补入相应章节；先前独立审查是静态可行性依据，不冒充这些接口已经完成运行验证。

### 按既定契约实施并验证

资源三态、一次定时与主动通知、主线程唯一入口及状态自管按内部协议实现，不冻结第三方资源通知接口。正常收尾、清理故障、声明与通信故障、状态写入故障通过确定性测试验证，不扩展孤儿抢救、预留事务或兜底轮询。

上游引用按依赖就绪后解析与声明并集规则实施，已解析路径与配置快照分开记录；路径准备和删除不额外推断业务意图。HTTP、Discovery、测试上下文适配显式结果与 FileSpec，但不要求其接口本轮冻结。现行 GUIDE、CONTEXT、AGENTS 与示例只在实现验证完成后切换契约。

实现中的普通类型、参数和错误细节由具体方案收敛；只有改变公开行为或兼容范围的实质取舍再提交裁决，不以假设场景不断扩大约束。
