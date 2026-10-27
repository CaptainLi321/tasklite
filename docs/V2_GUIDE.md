# TaskLite 使用指南（v2）

> 本包为 [`docs/adr/0004-v2-parallel-rebuild.md`](adr/0004-v2-parallel-rebuild.md) 重建、经 [ADR-0005](adr/0005-v2-promotion.md) 上位为主包的引擎：进程隔离、SQLite WAL 状态机、六步调用契约、瞬态信号军规全部保留，模型与命名按 ADR-0004 重塑。本指南是现行**权威**使用文档；v1 历史正文已移出工作树，以 Git 历史与 `v1-final` 标签为追溯来源。

---

## 1. 定位与导入

v2 即 `tasklite` 主包（上位裁决 [ADR-0005](adr/0005-v2-promotion.md)）。**唯一承诺稳定的导入面**是 `tasklite/__init__.py` 的集中导出：

```python
from tasklite import (
    TaskLite,                    # 门面（六步调用契约唯一入口）
    Job, Task, AttemptRecord,    # 三层模型：实例 / 规格 / 轨迹
    JobContext,                  # handler 执行上下文
    RetryError, FatalError, RateLimitHit,   # 异常三分类
    FailureEntry,                # 失败档案条目
    OrderingPolicy, FifoOrderingPolicy,     # 调度排序 seam
    RequeuePolicy, ImmediateRequeuePolicy,  # 重入队节奏 seam
    CapacityResource, RateLimitResource,    # 资源体系
    encode_identifier, encode_job_component, encode_content_id,  # 单射编码族
)
```

子包路径补充：`from tasklite.wrappers.discovery import register_discovery`（增量扫描适配）、`from tasklite.wrappers.http import http_guard, urllib_fetch, SQLiteSnapshotStore`（网络守卫与快照）、`from tasklite.testing import fake_ctx`（handler 单测构造器）。wrappers 不进主 `__all__`，经 `tasklite.wrappers.*` 导入。

**架构红线与边界**：

- 分层单向依赖：`tasklite/models/` 严禁 import `tasklite/engine/`（IPC 声明读写一律下沉 `tasklite/utils/ipc.py`），`tasklite/utils/` 严禁 import `tasklite/wrappers/`，核心层（engine/backend/models/utils）严禁依赖 `tasklite/contrib/`；
- 版本单一事实源：`tasklite.__version__`（pyproject 动态读取点）；
- **数据库零兼容**：schema 全新（含 attempts 表），旧库 fail-loud 拒识（`user_version=5`）；
- v1 已退役：git 历史与 `v1-final` 标签留档，迁移映射见第 6 节与 ADR-0004。

---

## 2. Task / Job / Attempt 三层模型

本引擎把规格与执行实例拆成三层——规格、逻辑实例、执行轨迹各有其身：

| 层 | 定义 | 持久化 |
|---|---|---|
| **Task（规格）** | 进程内注册的静态模板：`task_type` 名、handler、`resources`、`payload_schema`、默认 `max_retries` / `timeout_seconds` / `timeout_is_transient` | 代码即规格，不落盘；注册于 `register_task`（落点 `TaskRegistry`） |
| **Job（逻辑实例）** | 一次有界激活；`uid = task_type::job_id` 身份不变（queue/successes/failed 三表 PK 与六集合互斥均以 uid 为身份） | queue 行 job_snapshot JSON（job_uid 为主键） |
| **Attempt（执行轨迹）** | 一次物理执行的记录：序号、`execution_id`、起止时间、`outcome` | **append-only** 新表 `attempts` |

持久层 Python 接口以 `job_snapshot` / `job_snapshots` 表示完整 Job 字典，以 `job_uid` 表示跨对象作业身份；业务输入仍称 `payload`。引擎级键值读写使用 `backend.get_engine_metadata(key)` / `backend.set_engine_metadata(key, value)`，其中 `last_run_id` 仍表示最近一次运行。当前开发落盘格式为 schema 5（`user_version=5`，包版本已切换为 2.2.0，尚未发布）。仅接纳真正空库或完整且列集合匹配的 schema 5 库；版本 0 的非空库、`user_version <= 4` 旧库、混入 wall/meta 的库或半建库均明确拒绝，不自动迁移或补齐。拒绝前使用只读连接检查，保留原库；使用匹配版本读取历史，或显式选择新的状态库路径。

schema 5 六张核心表为 `queue(job_uid, queue_position, job_snapshot)`、`successes(job_uid, metadata)`、`failed(job_uid, metadata, payload)`、`attempts(attempt_id, job_uid, activation_no, attempt_no, execution_id, run_id, started_at, finished_at, outcome, error_message)`、`cursors(key, value)` 与 `engine_metadata(key, value)`。失败档案 `metadata` 与业务 `payload` 各自原样存取；HTTP 缓存 `http_snapshots` 是独立库，不随本 schema 切换。

失败 metadata 的框架键为 `error_message/error_category/is_fatal/attempt_no`（未知尝试号仍为 0）。成功 metadata 使用 `success_count/last_succeeded_at/last_success_run_id`；三者均由框架在同事务内强制写入（覆盖用户同名键），并与对应 running `attempts` 行收尾（`outcome='succeeded'`, `finished_at`）在同一 `BEGIN IMMEDIATE` 写事务内原子提交。业务 payload、runtime extra 及嵌套 metadata 的同拼写键均不翻译。


### Task：规格层

`Task` 是 frozen dataclass，一般经门面简式注册（首参传 task_type 字符串）：

```python
from tasklite import Task

task = Task(
    "download",                                   # task_type（非空 str，不含 "::"）
    download_handler,                             # handler，签名 handler(job, ctx)
    resources={"api": 1.0},                       # 该类任务默认资源占用
    payload_schema=DownloadPayload,               # 可选 TypedDict，派发前运行时校验
    max_retries=3,                                # 默认重试预算
    timeout_seconds=3600,                         # 默认执行超时秒
    timeout_is_transient=False,                   # 超时是否按瞬态失败重试
)
pipeline.register_task(task)        # 规格式：Task 实例原样注册，旁置参数一律拒绝
pipeline.register_task("download", download_handler, resources={"api": 1.0})  # 简式
```

`TaskRegistry` 契约：task_type 一对一；重复注册显式 `ValueError`（静默覆盖会让已入队 Job 挂到新 handler 上），未注册 lookup 显式 `KeyError`。

### Job：逻辑实例（字段归位）

Job 的字段按「规格位 / 实例位」归位：

- **规格位**（可覆盖 Task 默认）：`task_type` / `job_id` / `payload` / `resources` / `depends_on` / `max_retries` / `timeout_seconds` / `timeout_is_transient` / `trigger`；
- **实例位**（框架运行期管理）：
  - `attempt_no`——本次激活内尝试序号，**1 起始含首次执行**；预算判定：允许执行条件 `attempt_no <= max_retries + 1`；
  - `activation_no`——激活代，每次从 successes/failed 拦截点放行重跑时 +1（初激活为 1）；
  - `first_enqueued_at`——首次入队时间（UTC ISO），重试不刷新，由 enqueue 摄入管道填充（重试/spawn 回流的作业保留原值）。

```python
from tasklite import Job

job = Job(
    "download", "img_001",
    payload={"url": "https://example.com/1.jpg"},   # 须可 JSON 序列化
    max_retries=3,                                   # 覆盖 Task 默认；0 = 任何失败直接进失败档案
    depends_on=[],                                   # 上游 uid 列表（DAG 依赖）
    trigger=None,                                   # 跨会话重跑策略，None=未指定哨兵
)
job.uid          # "download::img_001"——身份不变式
```

`trigger` 在依赖就绪后、出队与开启 Attempt 前于主进程求值；新作业与历史作业统一处理。内置名称为 `once`（默认，无成功或失败历史才执行）、`on_failure`（未成功则执行）、`always`（每次入队执行，Discovery 默认）、`on_input_change`（有可信成功时，任一声明输入自身 `mtime_us` 严格晚于成功完成时间才执行）。输入时间相等不触发；缺可信基线保守执行，可信基线下空输入或缺路径直接报错并保留候选。

`Job` 的 `resources / max_retries / timeout_seconds / timeout_is_transient / trigger` 均以 `None` 表示未指定，在摄入时继承 `Task` 的对应规格；资源按键合并，显式同名键覆盖模板。`Task` 默认值依次为 `{}`、`3`、`3600`、`False`、`"once"`。快照保留未解析的 `None`，恢复后仍可继承；已解析规格随队列持久化。

运行前可用 `pipeline.register_trigger("name", fn)` 注册 `Callable[[TriggerContext], bool]`，Task 与 Job 仅保存名称。`TriggerContext` 提供隔离的 `job` 快照、`has_succeeded`、`has_failed`、只读 `success_metadata` 与 `last_succeeded_at_us`。禁止覆盖内置或重复名称，未注册名称、回调异常与非 `bool` 返回值使 `run()` 原样抛错、保留候选并回收在途任务，不消耗重试预算或写入失败档案。返回 `False` 正常跳过，本次 run 的下游继续流转；该就绪标记不跨 run 持久化。同次激活的故障重试（`attempt_no > 1`）不再求值。

### Attempt：执行轨迹（append-only 旁路面）

`AttemptRecord` 表达一次物理执行：`(job_uid, activation_no, attempt_no, execution_id, run_id, started_at, finished_at, outcome, error_message)`；`attempts` 表另有自增 `attempt_id`，执行身份列为 `execution_id`。**派发即插行**（outcome=running），终态或重入队时更新该行。`execution_id`（`run_id.dispatch_no` 执行代标识）随 attempt 落表。

`outcome` 词汇表（单一真相 `ATTEMPT_OUTCOMES`）：

| outcome | 语义 |
|---|---|
| `running` | 已派发、尚未收尾 |
| `succeeded` | 本次执行成功 |
| `failed` | 本次执行失败且耗尽预算（job 进失败档案） |
| `requeued` | 本次执行以重入队收尾（瞬态失败/瞬态信号，job 继续重试） |
| `skipped` | 激活被终态拦截点跳过（如 successes 命中且策略为 once） |

**旁路观测面定位**：attempts 不参与六集合互斥——successes/failed 仍按 uid 唯一终态，「最终状态唯一」与「历史可追溯」由此解耦。查询经后端只读口：

```python
for rec in pipeline.backend.load_attempts("download::img_001"):
    print(rec.activation_no, rec.attempt_no, rec.execution_id, rec.outcome, rec.error_message)
```

### 追溯链

```
attempts 行（job_uid, activation_no, attempt_no, execution_id, outcome, ...）
    │ job_uid
    ▼
Job uid = task_type::job_id ──当前终态──▶ successes（成功档案） / failed（失败档案）
    │ task_type
    ▼
Task 规格（进程内 TaskRegistry：handler / 默认资源 / payload_schema / 默认重试与超时）
```

任一 attempt 行出发都能回答「这是哪次激活的第几次执行、由哪个 run 派发、结局如何、当前终态在哪、规格是什么」——一次执行没有独立持久身份、重试即 DELETE+INSERT 的追溯断层由此补齐。

---

## 3. 六步调用顺序与最小示例

六步契约（顺序本身是设计的一部分，管理 API 仅限 `run()` 外调用）：

**初始化 → `register_resource` → `register_task` / `register_discovery` / `register_transient_exception` → `enqueue` → `run` → `stop`**

最小可运行示例：

```python
import logging
from pathlib import Path

from tasklite import Job, RateLimitResource, RetryError, TaskLite

logging.getLogger("tasklite").setLevel(logging.INFO)


def download_handler(job, ctx):
    """在独立子进程中执行——handler 必须模块级定义（spawn 经 pickle 下发）。"""
    out = Path(ctx.declare_output(f"{job.job_id}.jpg"))   # 声明产物（失败自动清理半成品）
    data = fetch(job.payload["url"])                       # 业务逻辑
    if data is None:
        raise RetryError("网络抖动，重入队重试")            # 瞬态失败信号
    out.write_bytes(data)
    return True, {"size": len(data)}                       # (成功, 业务元数据)


# 1. 初始化（name 派生状态库文件名 {name}_state.db）
pipeline = TaskLite(
    name="media",
    state_dir="./state",           # 持久化目录（SQLite WAL）
    output_roots="./downloads",     # 产物沙盒根（单根或多根序列）
    max_workers=4,                 # 并发子进程数（内部 __workers__ 容量资源，可覆盖）
)

# 2. 注册资源（限速/容量；max_workers 即经 __workers__ CapacityResource 实现）
pipeline.register_resource(RateLimitResource("api", interval_seconds=0.5))

# 3. 注册 Task 规格与瞬态异常
pipeline.register_task("download", download_handler, resources={"api": 1.0})
pipeline.register_transient_exception(MyTransientNetError)   # 可选：业务自有瞬态异常自动重试

# 4. 入队（重复 uid 静默跳过；first_enqueued_at 在此填充）
pipeline.enqueue([
    Job("download", "img_001", payload={"url": "https://example.com/1.jpg"}),
    Job("download", "img_002", payload={"url": "https://example.com/2.jpg"}),
])

# 5. 运行（阻塞直到队列排空，返回 RunSummary）
summary = pipeline.run()
print(summary.exit_reason, summary.stats["succeeded"], summary.run_id)

# 6. 停机（run() 自然返回后收尾；stop() 也可在 run 期从其他线程请求优雅停机）
pipeline.stop()
```

**handler 契约**：签名 `handler(job, ctx)`；返回值语义——`None`/`True` 成功，`False` 失败，`dict` 成功元数据，`tuple[bool, dict]` 组合；抛 `RetryError` 推回队列重试，抛 `FatalError` 直接进失败档案（不消耗重试预算），`RateLimitHit`（`RetryError` 子类）裸抛即按瞬态重试并遵守瞬态信号军规，其余异常失败进失败档案。`SystemExit` 同走错误通道计入重试预算（handler 内 `sys.exit()` 属主动退出/代码缺陷语义，不享受瞬态零预算；`KeyboardInterrupt` 才是唯一的瞬态中断信号——分野裁决见 ADR-0004「后果与重开条件」）。

**JobContext 常用 API**（handler 内执行上下文，属于一次 job 执行）：

| API | 用途 |
|---|---|
| `ctx.spawn(Job(...))` | 入队子 job（运行期派生唯一合法通道） |
| `ctx.declare_output(path)` / `ctx.declare_cache(path)` | 声明产物 / 临时半成品（返回解析后规范绝对路径，应使用返回值写文件） |
| `ctx.declare_input(path)` | 声明输入（校验存在性并记录规范绝对路径，联动 trigger / `trigger="on_input_change"`） |
| `ctx.get_cursor(key)` / `ctx.set_cursor(key, value)` | 高水位游标读写（成功时原子提交；`value=None` 删除） |
| `ctx.suspend_resource(name, seconds)` | 全局挂起资源（429/配额熔断，跨进程跨重启持久化） |
| `ctx.has_succeeded(job_uid)` / `ctx.has_failed(job_uid)` / `ctx.succeeded_or_failed_job_uids()` | 成功/失败档案快照查询；并集不含 skipped、队列成员或 Attempt 历史（派发时刻快照，不实时刷新） |

入队前可在 `run()` 外使用 `pipeline.enqueue(pipeline.filter_jobs_without_success(jobs))`：仅排除成功档案命中的 Job，保留输入顺序；失败档案或队列中已有的 Job 仍可返回，重跑与队列去重分别由派发和入队处理。

**run/stop 细节**：`run()` 返回 `RunSummary(exit_reason, stats, run_id, duration_seconds)`；`stop()` 请求 DRAINING（不再派发、在途自然完成），`stop(force=True)` 请求 ABORTING（分类消费在途任务）；`run_graceful()` 是统一包装（正常完成返回 `RunSummary`，捕获 KeyboardInterrupt 转 DRAINING 并返回 `None`）。构造期钩子三件：`on_run_start` / `on_attempt_finished(job_uid, *, outcome: AttemptOutcome)` / `on_run_end(exit_reason)`——同步、主线程、必须轻量非阻塞（抛异常只计数进 `stats["hook_errors"]`）。

handler 单测用官方构造器：`from tasklite.testing import fake_ctx`——`fake_ctx(job, successes=..., failed=..., cursors=..., resources=..., tmp_root=...)`，不硬编码 JobContext 内部容器结构。

---

## 4. 失败档案（failed）

重试预算耗尽、致命错误、依赖级联等**永久失败**的唯一归宿是失败档案（failed 表；successes/failed 按 uid 互斥且唯一终态）。运维面三 API 均限 `run()` 外调用：

```python
# 只读查询：结构化条目（job_uid / error_message / metadata / payload）
for entry in pipeline.list_failures():
    print(entry.job_uid, entry.error_message, entry.metadata.get("error_category"), entry.payload)

# 批量清理（默认保留 is_fatal=true 的确定性失败；返回删除数）
n = pipeline.clear_failures(task_types=["download"], keep_fatal=True)

# 单条人工补跑：移出档案、按档案 payload 快照队首重入队；job_uid 不在档案显式 KeyError
pipeline.retry_failure("download::img_001")
```

- **`FailureEntry`**：`job_uid`、`error_message`（原始错误消息/错误码）、`metadata`（完整档案元数据视图，含 `error_category` 分类码）、`payload`（原始业务 payload 快照——补跑无需外部反查参数；无可存快照时为 None）。
- **`clear_failures(task_types=None, *, keep_fatal=True)`**：按 task_type 前缀匹配删除；`keep_fatal=False` 连确定性失败一并清理。
- **`retry_failure(job_uid)`**：失败档案的人工补跑通道。激活代按拦截点放行语义推进——重建作业的 `activation_no` 取该 job_uid 在 attempts 轨迹中的最大激活代 +1、`attempt_no` 归 1（与 always 触发放行同构，保证 attempts 表 `(activation_no, attempt_no)` 逻辑键不撞号）；payload 取档案快照，先入队后删行（崩溃窗口至多留下 queue∩failed 并存，由加载期修复收敛，不丢作业）。

**与 attempts 轨迹的关系**：failed 表回答「当前终态是什么」（每 uid 至多一行，成功即被 successes 侧取代），attempts 表回答「历史上每次执行发生了什么」（append-only，永不删行）。`retry_failure` 补跑后，attempts 中旧的 `outcome="failed"` 行保留为轨迹证据，新激活以新 `(activation_no, attempt_no)` 追加——「最终状态唯一」与「历史可追溯」经两张面解耦。

---

## 5. 调度扩展 seam（三类注册表：OrderingPolicy / RequeuePolicy / Selection）

核心**不含任何调度策略计算**：候选排序、重入队节奏与候选准入各留一个接缝，扩展一律以**各自 seam 形态**插入（策略 wrapper/util 或选择规则值对象），核心不动；**seam 扩展须另立 ADR**。

| seam | 类型 | 唯一出口 | 决策 |
|---|---|---|---|
| `OrderingPolicy` | 候选排序（队列访问序） | `JobScheduler.scan_next_runnable` 按 `visit_order` 首中即停 | ADR-0004 |
| `RequeuePolicy` | 重入队节奏 | `RequeuePolicy.plan_requeue`（`StateStore.requeue_transient` / `apply_retry` 唯一消费） | ADR-0004 |
| `Selection` | 候选准入/选择（`task_type` 维度：Exclude 延迟 / Skip 终态） | `scan_next_runnable` 候选过滤 + Skip 跳过阶段（终态收敛） + 停摆仲裁 Kahn 拓扑剥离 + 终态写入出口（`cascade_fail` / `apply_bulk_failure`）隔离 | [ADR-0006](adr/0006-selection-scheduling-seam.md)、本指南第 8 节 |

### OrderingPolicy：队列访问序

`scan_next_runnable`（`tasklite/engine/scheduler.py`）是**唯一选择点**：调度器按 OrderingPolicy 给出的下标序逐条评估队列、首个可运行者处停止（首中即停）。默认实现 `FifoOrderingPolicy`（按 queue_position 升序）。自定义策略经门面构造参数 `ordering_policy` 注入（None → FIFO，默认解析收敛在 `RunConfig.resolve` 唯一解析点）：

```python
from collections.abc import Iterable, Sequence

from tasklite import OrderingPolicy, TaskLite


class NewestFirstPolicy(OrderingPolicy):
    """队尾优先访问序（示例：纯 wrapper，不改核心）。"""

    def visit_order(self, queue: Sequence[dict]) -> Iterable[int]:
        return range(len(queue) - 1, -1, -1)


pipeline = TaskLite(name="media", state_dir="./state", ordering_policy=NewestFirstPolicy())
```

### RequeuePolicy：重试节奏唯一出口

核心引擎把一个失败/瞬态作业放回队列时，一律经 `plan_requeue` 取得节奏规划，自身不做任何计算。自定义策略同样经门面构造参数 `requeue_policy` 注入（None → 立即重入队）：

```python
from tasklite import RequeuePlan, RequeuePolicy, TaskLite


class FixedDelayRequeuePolicy(RequeuePolicy):
    """固定延迟节奏（示例骨架：delay_seconds 是延迟类策略的表达位）。"""

    def plan_requeue(self, job_snapshot, *, transient_kind=None) -> RequeuePlan:
        return RequeuePlan(front=False, delay_seconds=30.0)


pipeline = TaskLite(name="media", state_dir="./state",
                    requeue_policy=FixedDelayRequeuePolicy())
```

- 默认实现 `ImmediateRequeuePolicy`：`RequeuePlan(front=True, delay_seconds=0.0)`——零延迟、插队首（重试不被新入队作业排挤到饥饿尾部）；
- `RequeuePlan.delay_seconds` 当前核心无延迟消费方（立即重入队语义下无人读它）——它是为未来节奏策略预留的表达位，届时经 seam 填充即可生效，无需改动调用方；
- `transient_kind` 非 None 表示瞬态信号（`interrupted` / `lock_conflict` / `rate_limited`）——预算豁免语境下策略不得因预算耗尽拒绝重入队。瞬态信号军规（不烧重试预算 + 降级写盘 + 零污染）在「立即重入队」默认策略下自然成立；
- 两个构造参数只做接线：不引入任何策略实现、不给 Job 增调度字段；`scan_next_runnable` 唯一选择点与 `plan_requeue` 唯一出口的结构不变。

### Selection：候选准入/选择（第三类 seam）

Selection 决定「本 run 是否允许某 `task_type` 的候选进入执行」，既不是候选排序也不是重入队节奏：`pipeline.selection(Exclude(task_type="pdf_to_md"), Skip(task_type="ocr"))` 配置规则集——Exclude 延迟留队（下次 run 自动恢复），Skip 终态跳过并级联下游。API 与语义契约见**第 8 节**；分类裁决、停摆拓扑剥离与重开条件见 [ADR-0006](adr/0006-selection-scheduling-seam.md)。

### 核心禁改红线

- 核心（models/backend/engine/utils）**禁止出现任何排序计算与退避计算**；
- **Job 模型不携带** `priority` / `deadline` / `period` 字段——其存放与语义由未来调度策略 ADR 裁决；
- 软 EDF / 优先级 / aging / 错峰 / 超期准入（依赖 Job deadline 等调度字段）等策略引入时**须新立 ADR**，以 wrapper/util 形态实现上述接缝，核心不动；
- `Selection` 的 `task_type` 准入轴已由 [ADR-0006](adr/0006-selection-scheduling-seam.md) 定案；新增规则轴、优先级/期限类策略或修改其语义须重开 ADR。

---

## 6. v1 → v2 命名映射

完整映射表（模块层 + 标识符层 + 保留不动清单）见 [ADR-0004 命名映射表](adr/0004-v2-parallel-rebuild.md#命名映射表v1--v2)。最常用 15 条摘录：

| v1 | v2 |
|---|---|
| `TaskLite.register_handler` | `TaskLite.register_task` |
| `TaskLite.add_resource` | `TaskLite.register_resource` |
| `TaskLite.list_dlq` | `TaskLite.list_failures` |
| `TaskLite.clear_dlq` | `TaskLite.clear_failures` |
| `DLQEntry` | `FailureEntry` |
| `TaskContext` | `JobContext` |
| `on_job_completed(uid, meta, success, going_to_retry)` | `on_attempt_finished(job_uid, *, outcome: AttemptOutcome)` |
| `Job(..., retries=...)` 构造参数 | 废除——预算位收敛 `max_retries`（允许执行条件 `attempt_no <= max_retries + 1`） |
| `sanitize_identifier` / `sanitize_job_component` / `sanitize_content_id` | `encode_identifier` / `encode_job_component` / `encode_content_id` |
| `escape_injective` | `percent_encode` |
| `ErrorTaxonomy`（taxonomy.py） | `ErrorClassifier`（errorclass.py） |
| `TaskLite.list_suspends` | `TaskLite.list_suspensions` |
| `pop_next_runnable` | `scan_next_runnable` |
| `AdmissionPolicy`（policy.py） | `TriggerRegistry`（admission.py） |
| `BackoffGovernor` / `plan_retry` 退避计算 | 删除——`RequeuePolicy` seam 接管（默认 `ImmediateRequeuePolicy`） |

其余高频变化：`fetch_urllib` / `fetch_requests` → `urllib_fetch` / `requests_fetch`；`ERROR_TYPE_*` 九常量不迁移（`ErrorCategory` 唯一表示）；「DLQ / 死信」术语一律改称「失败档案」。`seed_cursor` / `content_fingerprint` / `DeadlockGovernor.arbitrate` 等保留不动（完整清单见 ADR）。成功档案 Python API 现使用 `successes` / `load_successes` / `delete_successes` / `seed_successes`（状态集合属性为 `PipelineState.success_uids`）；`AttemptOutcome` 使用 `success`、`will_retry`、`metadata`，`ExecutionResult` 与 `JobContext` 的 Job 对象列表使用 `spawned_jobs`。运行统计 `EMPTY_STATS` 使用 `succeeded` 与 `lock_conflict_reruns`；资源公开属性对齐为 `RateLimitResource.interval_seconds` 与 `CapacityResource.max_capacity`；`register_discovery` 分组键参数为 `group_key_func`；顶层包公开导出 `SuspendEntry`。SQL 使用 `successes`、`attempts.error_message`，worker 信封使用 `spawned_jobs/error_message`；Job 快照外层使用 `timeout_seconds`，专属入口拒绝旧字段。执行身份与文件名编码使用 `execution_id` 和 `encode_job_uid_filename`，IPC 文件名字节保持不变。

---

## 7. backoff 移除说明

本引擎**砍除整个退避机制**：`BackoffGovernor` 与 `backoff_base` / `backoff_max` / `backoff_until` / `backoff_wall_deadline` 全族（v1 遗产）不再存在。重试语义是**立即重入队**：

- 失败（未耗尽预算）→ `ImmediateRequeuePolicy` 规划 `RequeuePlan(front=True, delay_seconds=0.0)` → 队首重入队 → 下一轮扫描即可再次派发；
- 重试间隔不再由框架注入——若业务需要节奏（固定退避、指数退避、限速窗），经门面构造参数 `requeue_policy` 注入自定义 `RequeuePolicy`（见第 5 节示例骨架），核心零改动；
- 瞬态信号（孤儿锁冲突 / 外部中断 / 限速）天然受益：零等待重入队 + 不烧重试预算 + 零污染，实际等待由资源挂起 TTL（`ctx.suspend_resource` 的跨重启持久化挂起）承担，而非框架级退避计时器。

这一取舍的裁决依据与重开条件（退避类策略经 seam 回归须新立 ADR）见 ADR-0004「调度 seam」与「后果与重开条件」章节。

---

## 8. 执行选择（Selection）：Exclude 延迟留队 + Skip 终态跳过

Selection 属**第 5 节调度扩展 seam 的第三类——候选准入/选择**（与 OrderingPolicy、RequeuePolicy 并列），分类裁决、停摆拓扑剥离与重开条件见 [ADR-0006](adr/0006-selection-scheduling-seam.md)。本节给出使用面与语义契约。

`pipeline.selection(*rules)` 是 **pipeline 属性**（不是 `run()` 的参数）：每次调用**整体替换**规则集，0 参数重置为默认全执行，返回 self 支持链式；`run()` / `run_graceful()` 签名不变，`RunSummary` / `ExecutionOptions` 无新增字段，`EMPTY_STATS` 新增 `excluded_blocked` 计数（本次 run 中「允许执行但仅因 Exclude 上游而无法完成」的留队作业数）。

```python
from tasklite import Exclude, Job, Skip, TaskLite

pipeline = TaskLite(name="research", state_dir="./state")
pipeline.selection(Exclude(task_type="pdf_to_md"))   # 本次不执行、留队（下次自动恢复）
pipeline.enqueue([Job("fetch", "a"), Job("pdf_to_md", "p")])
pipeline.run()                                  # 只执行 fetch，pdf_to_md 留在持久化队列

pipeline.selection(Skip(task_type="ocr"))            # 终态跳过：命中作业与下游永不执行
pipeline.selection(Exclude("a"), Skip("b"))     # 每次整体替换；同一 task_type Skip 优先
pipeline.selection()                            # 重置：恢复全执行
```

规则是职责单一的值对象：`Exclude(task_type=...)`（延迟）与 `Skip(task_type=...)`（终态）均按 `job.task_type` 精确匹配；`Rule` 协议只要求 `matches_task_type(task_type)`（引擎的调度热路径与终态写入出口都按 task_type 判定），`Selection.__init__` 对缺少该入口的规则 **fail-loud 抛 `TypeError`**，不会静默漏判。规则属配置代码：`matches_task_type` 抛异常不被吞，`run()` 以 ERROR 上抛（fail-loud，绝不静默降级为全执行/全排除）。顶层 `tasklite` 导出 `Exclude` / `Selection` / `Skip`，`from tasklite.selection import ...` 子模块路径同样可用（模块名全小写）。规则只做 task_type 格式校验，不校验注册表存在性——选择配置可以先于 `register_task`，也可跨运行窗口复用。

### 8.1 Exclude（延迟语义：留队、下次自动重试）

- 命中作业不派发、不写 attempt 轨迹、不进入 successes/失败档案、不消耗 worker/资源与重试预算，队列行原样保留；
- 其下游允许作业同样留队，本次 run 报 `ERR_DEPENDENCY_EXCLUDED` 并计入 `stats["excluded_blocked"]`，下一次 run（去掉 Exclude / 换 run 配置）自动恢复执行；
- 停摆仲裁用 **Kahn 入度消减**（初始波前 = `success_uids | excluded_uids`）严格区分：允许作业存在依赖环/缺口时按既有 governor 归因熔断（`ERR_DEPENDENCY_DEADLOCK` / `ERR_DEADLOCK_GAP` / `ERR_RESOURCE_DEADLOCK` / `ERR_MALFORMED_JOB`），绝不因 Exclude 命中而掩盖；仅当剩余允许作业全部可消减且仅被 Exclude 阻塞时，`run()` 以 `ExitReason.COMPLETED` 收敛并保留队列（无固定等待、无忙旋）；
- 终态写入出口隔离：被排除 uid 不进入失败档案（父失败级联不传播、批量熔断被过滤）。

### 8.2 Skip（终态语义：跳过、下游级联、不再重试）

- 命中作业从 queue 移除，不进入 successes、不进入失败档案（既不成功也不失败），`stats["skipped"]` 计数，审计落 append-only attempts（`outcome="skipped"`，错误码 `SKIPPED`）；
- 直接或间接依赖 Skip 作业的下游永远无法完成，整链级联跳过（attempt 错误码 `DEPENDENCY_SKIPPED`，含上游 uid），一并从 queue 移除，下次运行不再重试；
- Skip 与 Exclude 同时命中时 **Skip 优先**（终态意图强于延迟意图）；Skip 级联同样优先于 Exclude——被 Skip 上游永久堵死的 Exclude 作业也一并终态跳过；
- **判定优先级**：Skip 级联阶段在停摆仲裁之前执行——Skip 子树内即便存在依赖环/缺口，也一律按 `DEPENDENCY_SKIPPED` 归因（Skip 上游是永久死因，环是否可解已无调度意义）；Exclude 则相反，允许作业残图中的环/缺口优先按死锁归因熔断（§8.1）；
- 运行期派生（`ctx.spawn`）出的 Skip 命中作业在下一拍被跳过阶段收敛，不会派发。

### 8.3 集成方注意

- selection 激活时 `run()` 可能以 `COMPLETED` 退出而持久化队列非空（Exclude 留队），完成度请按 queue 判定，不能只看退出原因；`stats["excluded_blocked"]` 给出本次被延迟的下游数量；
- 混合场景（队列同时存在真实死锁成因的允许作业）仍按既有死锁归因/失败档案语义处理——选择规则不等于死锁豁免；
- `selection(...)` 仅允许 `run()` 外调用；run 期调用抛 `RuntimeError`。
