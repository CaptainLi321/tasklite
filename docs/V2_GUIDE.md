# TaskLite v2 使用指南

> v2 是 [`docs/adr/0004-v2-parallel-rebuild.md`](adr/0004-v2-parallel-rebuild.md) 裁决的**并行重建子包**：架构照搬 v1（进程隔离、SQLite WAL 状态机、六步调用契约、瞬态信号军规全部保留），命名与模型按 ADR-0004 重塑。本指南是 v2 的权威使用文档；架构总览与概念词典见 [`ENGINE_ARCHITECTURE.md`](ENGINE_ARCHITECTURE.md)（v1 视角），设计与迁移军规见 ADR-0004。

---

## 1. 定位与导入

v2 落位于 `tasklite/v2/` 子包，与 v1 同处一个发行版。**唯一承诺稳定的导入面**是 `tasklite/v2/__init__.py` 的集中导出：

```python
from tasklite.v2 import (
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

子包路径补充：`from tasklite.v2.wrappers.discovery import register_discovery`（增量扫描适配）、`from tasklite.v2.wrappers.http import http_guard, urllib_fetch, SQLiteSnapshotStore`（网络守卫与快照）、`from tasklite.v2.testing import fake_ctx`（handler 单测构造器）。wrappers 不进主 `__all__`，经 `tasklite.v2.wrappers.*` 导入。

**隔离与冻结（ADR-0004 裁决）**：

- **v2 严禁 import v1**——`tasklite.v2` 不依赖旧树任何模块，保证独立演进与最终整体替换；
- **v1 处于冻结期**——仅允许缺陷修复，不接受新特性；冻结期缺陷须 v1/v2 双落修复；
- v2 分层红线镜像：`v2/models/` 严禁 import `v2/engine/`，`v2/utils/` 严禁 import `v2/wrappers/`，v2 核心层严禁依赖 `v2/contrib/`；
- v2 无独立版本号，随主包 `tasklite.__version__` 单一事实源；
- **数据库零兼容**：v2 schema 全新（含 attempts 表），不认 v1 旧库。

---

## 2. Task / Job / Attempt 三层模型

v2 把 v1 混于一个 `Job` 的概念拆成三层——规格、逻辑实例、执行轨迹各有其身：

| 层 | 定义 | 持久化 |
|---|---|---|
| **Task（规格）** | 进程内注册的静态模板：`task_type` 名、handler、`default_resources`、`payload_schema`、默认 `max_retries` / `timeout` / `timeout_is_transient` | 代码即规格，不落盘；注册于 `register_task`（落点 `TaskRegistry`） |
| **Job（逻辑实例）** | 一次有界激活；`uid = task_type::job_id` 身份不变（queue/wall/failed 三表 PK 与六集合互斥均以 uid 为身份） | queue 行 job_data JSON（uid 为主键） |
| **Attempt（执行轨迹）** | 一次物理执行的记录：序号、`incarnation`、起止时间、`outcome` | **append-only** 新表 `attempts` |

### Task：规格层

`Task` 是 frozen dataclass，一般经门面简式注册（首参传 task_type 字符串）：

```python
from tasklite.v2 import Task

task = Task(
    "download",                                   # task_type（非空 str，不含 "::"）
    download_handler,                             # handler，签名 handler(job, ctx)
    default_resources={"api": 1.0},               # 该类任务默认资源占用
    payload_schema=DownloadPayload,               # 可选 TypedDict，派发前运行时校验
    max_retries=3,                                # 默认重试预算
    timeout=3600,                                 # 默认执行超时秒
    timeout_is_transient=False,                   # 超时是否按瞬态失败重试
)
pipeline.register_task(task)        # 规格式：Task 实例原样注册，旁置参数一律拒绝
pipeline.register_task("download", download_handler, default_resources={"api": 1.0})  # 简式
```

`TaskRegistry` 契约：task_type 一对一；重复注册显式 `ValueError`（静默覆盖会让已入队 Job 挂到新 handler 上），未注册 lookup 显式 `KeyError`。

### Job：逻辑实例（字段归位）

Job 的字段按「规格位 / 实例位」归位：

- **规格位**（可覆盖 Task 默认）：`task_type` / `job_id` / `payload` / `resources` / `depends_on` / `max_retries` / `timeout` / `timeout_is_transient` / `rerun`；
- **实例位**（框架运行期管理）：
  - `attempt_no`——本次激活内尝试序号，**1 起始含首次执行**；预算判定：允许执行条件 `attempt_no <= max_retries + 1`；
  - `activation_no`——激活代，每次从 wall/failed 拦截点放行重跑时 +1（初激活为 1）；
  - `first_enqueued_at`——首次入队时间（UTC ISO），重试不刷新，由 enqueue 摄入管道填充（重试/spawn 回流的作业保留原值）。

```python
from tasklite.v2 import Job

job = Job(
    "download", "img_001",
    payload={"url": "https://example.com/1.jpg"},   # 须可 JSON 序列化
    max_retries=3,                                   # 覆盖 Task 默认；0 = 任何失败直接进失败档案
    depends_on=[],                                   # 上游 uid 列表（DAG 依赖）
    rerun="never",                                   # 跨会话重跑策略，None=未指定哨兵
)
job.uid          # "download::img_001"——身份不变式
```

`rerun` 四策略：`"never"`（wall/failed 命中即跳过，默认）/ `"on_failure"`（failed 命中重跑）/ `"every_run"`（命中都重跑，成功 REPLACE wall 行且 run_count+1）/ `"on_input_change"`（wall 命中比对输入指纹）。`None` 哨兵可被任务级默认策略（`set_discovery_rerun` 注入面）覆盖，显式值一律尊重。

### Attempt：执行轨迹（append-only 旁路面）

`attempts` 表一行 = 一次物理执行：`(id 自增, job_uid, activation_no, attempt_no, incarnation, run_id, started_at, finished_at, outcome, error)`。**派发即插行**（outcome=running），终态或重入队时更新该行。`incarnation`（`run_id.dispatch_seq` 执行代标识）随 attempt 落表。

`outcome` 词汇表（单一真相 `ATTEMPT_OUTCOMES`）：

| outcome | 语义 |
|---|---|
| `running` | 已派发、尚未收尾 |
| `succeeded` | 本次执行成功 |
| `failed` | 本次执行失败且耗尽预算（job 进失败档案） |
| `requeued` | 本次执行以重入队收尾（瞬态失败/瞬态信号，job 继续重试） |
| `skipped` | 激活被终态拦截点跳过（如 wall 命中且策略为 never） |

**旁路观测面定位**：attempts 不参与六集合互斥——wall/failed 仍按 uid 唯一终态，「最终状态唯一」与「历史可追溯」由此解耦。查询经后端只读口：

```python
for rec in pipeline.backend.load_attempts("download::img_001"):
    print(rec.activation_no, rec.attempt_no, rec.incarnation, rec.outcome, rec.error)
```

### 追溯链

```
attempts 行（job_uid, activation_no, attempt_no, incarnation, outcome, ...）
    │ job_uid
    ▼
Job uid = task_type::job_id ──当前终态──▶ wall（成功档案） / failed（失败档案）
    │ task_type
    ▼
Task 规格（进程内 TaskRegistry：handler / 默认资源 / payload_schema / 默认重试与超时）
```

任一 attempt 行出发都能回答「这是哪次激活的第几次执行、由哪个 run 派发、结局如何、当前终态在哪、规格是什么」——v1 时代一次执行没有独立持久身份、重试即 DELETE+INSERT 的追溯断层由此补齐。

---

## 3. 六步调用顺序与最小示例

六步契约（顺序本身是设计的一部分，管理 API 仅限 `run()` 外调用）：

**初始化 → `register_resource` → `register_task` / `register_discovery` / `register_transient_exception` → `enqueue` → `run` → `stop`**

最小可运行示例：

```python
import logging
from pathlib import Path

from tasklite.v2 import Job, RateLimitResource, RetryError, TaskLite

logging.getLogger("tasklite.v2").setLevel(logging.INFO)


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
    output_root="./downloads",     # 产物沙盒根（单根或多根序列）
    max_workers=4,                 # 并发子进程数（内部 __workers__ 容量资源，可覆盖）
)

# 2. 注册资源（限速/容量；max_workers 即经 __workers__ CapacityResource 实现）
pipeline.register_resource(RateLimitResource("api", interval_seconds=0.5))

# 3. 注册 Task 规格与瞬态异常
pipeline.register_task("download", download_handler, default_resources={"api": 1.0})
pipeline.register_transient_exception(MyTransientNetError)   # 可选：业务自有瞬态异常自动重试

# 4. 入队（重复 uid 静默跳过；first_enqueued_at 在此填充）
pipeline.enqueue([
    Job("download", "img_001", payload={"url": "https://example.com/1.jpg"}),
    Job("download", "img_002", payload={"url": "https://example.com/2.jpg"}),
])

# 5. 运行（阻塞直到队列排空，返回 RunSummary）
summary = pipeline.run()
print(summary.exit_reason, summary.stats["completed"], summary.run_id)

# 6. 停机（run() 自然返回后收尾；stop() 也可在 run 期从其他线程请求优雅停机）
pipeline.stop()
```

**handler 契约**：签名 `handler(job, ctx)`；返回值语义——`None`/`True` 成功，`False` 失败，`dict` 成功元数据，`tuple[bool, dict]` 组合；抛 `RetryError` 推回队列重试，抛 `FatalError` 直接进失败档案（不消耗重试预算），`RateLimitHit`（`RetryError` 子类）裸抛即按瞬态重试并遵守瞬态信号军规，其余异常失败进失败档案。

**JobContext 常用 API**（handler 内执行上下文，属于一次 job 执行）：

| API | 用途 |
|---|---|
| `ctx.spawn(Job(...))` | 入队子 job（运行期派生唯一合法通道） |
| `ctx.declare_output(path)` / `ctx.declare_cache(path)` | 声明产物 / 临时半成品（返回解析后规范绝对路径，应使用返回值写文件） |
| `ctx.declare_input(path)` / `ctx.declare_input_uri(url)` | 声明输入（采集 stat 指纹，联动 `rerun="on_input_change"`） |
| `ctx.get_cursor(key)` / `ctx.set_cursor(key, value)` | 高水位游标读写（成功时原子提交；`value=None` 删除） |
| `ctx.suspend_resource(name, seconds)` | 全局挂起资源（429/配额熔断，跨进程跨重启持久化） |
| `ctx.is_completed(uid)` / `ctx.is_failed(uid)` / `ctx.attempted_uids()` | wall/failed 快照查询（派发时刻快照语义） |

**run/stop 细节**：`run()` 返回 `RunSummary(exit_reason, stats, run_id, duration_seconds)`；`stop()` 请求 DRAINING（不再派发、在途自然完成），`stop(force=True)` 请求 ABORTING（分类消费在途任务）；`run_graceful()` 是统一包装（捕获 KeyboardInterrupt 转 DRAINING）。构造期钩子三件：`on_run_start` / `on_attempt_finished(uid, *, outcome: AttemptFinish)` / `on_run_end(exit_reason)`——同步、主线程、必须轻量非阻塞（抛异常只计数进 `stats["hook_errors"]`）。

handler 单测用官方构造器：`from tasklite.v2.testing import fake_ctx`——`fake_ctx(job, wall=..., failed=..., cursors=..., resources=..., tmp_root=...)`，不硬编码 JobContext 内部容器结构。

---

## 4. 失败档案（failed）

重试预算耗尽、致命错误、依赖级联等**永久失败**的唯一归宿是失败档案（failed 表；wall/failed 按 uid 互斥且唯一终态）。运维面三 API 均限 `run()` 外调用：

```python
# 只读查询：结构化条目（uid / error / meta / job_payload）
for entry in pipeline.list_failures():
    print(entry.uid, entry.error, entry.meta.get("error_type"), entry.job_payload)

# 批量清理（默认保留 fatal=true 的确定性失败；返回删除数）
n = pipeline.clear_failures(task_types=["download"], keep_fatal=True)

# 单条人工补跑：移出档案、按档案 payload 快照队首重入队；uid 不在档案显式 KeyError
pipeline.retry_failure("download::img_001")
```

- **`FailureEntry`**：`uid`、`error`（原始错误消息/错误码）、`meta`（完整档案元数据视图，含 `error_type` 分类码）、`job_payload`（原始业务 payload 快照——补跑无需外部反查参数；无可存快照时为 None）。
- **`clear_failures(task_types=None, *, keep_fatal=True)`**：按 task_type 前缀匹配删除；`keep_fatal=False` 连确定性失败一并清理。
- **`retry_failure(uid)`**：失败档案的人工补跑通道。激活代按拦截点放行语义推进——重建作业的 `activation_no` 取该 uid 在 attempts 轨迹中的最大激活代 +1、`attempt_no` 归 1（与 every_run 豁免放行同构，保证 attempts 表 `(activation_no, attempt_no)` 逻辑键不撞号）；payload 取档案快照，先入队后删行（崩溃窗口至多留下 queue∩failed 并存，由加载期修复收敛，不丢作业）。

**与 attempts 轨迹的关系**：failed 表回答「当前终态是什么」（每 uid 至多一行，成功即被 wall 侧取代），attempts 表回答「历史上每次执行发生了什么」（append-only，永不删行）。`retry_failure` 补跑后，attempts 中旧的 `outcome="failed"` 行保留为轨迹证据，新激活以新 `(activation_no, attempt_no)` 追加——「最终状态唯一」与「历史可追溯」经两张面解耦。

---

## 5. 调度扩展 seam（OrderingPolicy / RequeuePolicy）

v2 核心**不含任何调度策略计算**：候选排序与重试节奏各留一个接缝，扩展一律以 wrapper/util 形态插入，核心不动。

### OrderingPolicy：队列访问序

`scan_next_runnable`（`v2/engine/scheduler.py`）是**唯一选择点**：调度器按 OrderingPolicy 给出的下标序逐条评估队列、首个可运行者处停止（首中即停）。默认实现 `FifoOrderingPolicy`（按 seq 升序）。自定义策略经门面构造参数 `ordering` 注入（None → FIFO，默认解析收敛在 `RunConfig.resolve` 唯一解析点）：

```python
from collections.abc import Iterable, Sequence

from tasklite.v2 import OrderingPolicy, TaskLite


class NewestFirstPolicy(OrderingPolicy):
    """队尾优先访问序（示例：纯 wrapper，不改核心）。"""

    def visit_order(self, queue: Sequence[dict]) -> Iterable[int]:
        return range(len(queue) - 1, -1, -1)


pipeline = TaskLite(name="media", state_dir="./state", ordering=NewestFirstPolicy())
```

### RequeuePolicy：重试节奏唯一出口

核心引擎把一个失败/瞬态作业放回队列时，一律经 `plan_requeue` 取得节奏规划，自身不做任何计算。自定义策略同样经门面构造参数 `requeue_policy` 注入（None → 立即重入队）：

```python
from tasklite.v2 import RequeuePlan, RequeuePolicy, TaskLite


class FixedDelayRequeuePolicy(RequeuePolicy):
    """固定延迟节奏（示例骨架：delay_seconds 是延迟类策略的表达位）。"""

    def plan_requeue(self, job_dict, *, transient_kind=None) -> RequeuePlan:
        return RequeuePlan(front=False, delay_seconds=30.0)


pipeline = TaskLite(name="media", state_dir="./state",
                    requeue_policy=FixedDelayRequeuePolicy())
```

- 默认实现 `ImmediateRequeuePolicy`：`RequeuePlan(front=True, delay_seconds=0.0)`——零延迟、插队首（重试不被新入队作业排挤到饥饿尾部）；
- `RequeuePlan.delay_seconds` 当前核心无延迟消费方（立即重入队语义下无人读它）——它是为未来节奏策略预留的表达位，届时经 seam 填充即可生效，无需改动调用方；
- `transient_kind` 非 None 表示瞬态信号（`interrupted` / `lock_conflict` / `rate_limited`）——预算豁免语境下策略不得因预算耗尽拒绝重入队。瞬态信号军规（不烧重试预算 + 降级写盘 + 零污染）在「立即重入队」默认策略下自然成立；
- 两个构造参数只做接线：不引入任何策略实现、不给 Job 增调度字段；`scan_next_runnable` 唯一选择点与 `plan_requeue` 唯一出口的结构不变。

### 核心禁改红线

- v2 核心（models/backend/engine/utils）**禁止出现任何排序计算与退避计算**；
- **Job 模型不携带** `priority` / `deadline` / `period` 字段——其存放与语义由未来调度策略 ADR 裁决；
- 软 EDF / 优先级 / aging / 错峰 / 准入控制等策略引入时**须新立 ADR**，以 wrapper/util 形态实现上述接缝，核心不动。

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
| `on_job_completed(uid, meta, success, going_to_retry)` | `on_attempt_finished(uid, *, outcome: AttemptFinish)` |
| `Job(..., retries=...)` 构造参数 | 废除——预算位收敛 `max_retries`（允许执行条件 `attempt_no <= max_retries + 1`） |
| `sanitize_identifier` / `sanitize_job_component` / `sanitize_content_id` | `encode_identifier` / `encode_job_component` / `encode_content_id` |
| `escape_injective` | `percent_encode` |
| `ErrorTaxonomy`（taxonomy.py） | `ErrorClassifier`（errorclass.py） |
| `TaskLite.list_suspends` | `TaskLite.list_suspensions` |
| `pop_next_runnable` | `scan_next_runnable` |
| `AdmissionPolicy`（policy.py） | `RerunPolicy`（admission.py） |
| `BackoffGovernor` / `plan_retry` 退避计算 | 删除——`RequeuePolicy` seam 接管（默认 `ImmediateRequeuePolicy`） |

其余高频变化：`fetch_urllib` / `fetch_requests` → `urllib_fetch` / `requests_fetch`；`ERROR_TYPE_*` 九常量不迁移（`ErrorCategory` 唯一表示）；「DLQ / 死信」术语一律改称「失败档案」。`wall` / `seed_wall` / `seed_cursor` / `safe_uid_filename` / `content_fingerprint` / `incarnation` / `DeadlockGovernor.arbitrate` 等保留不动（完整清单见 ADR）。

---

## 7. backoff 移除说明

v2 **砍除了整个退避机制**：`BackoffGovernor` 与 `backoff_base` / `backoff_max` / `backoff_until` / `backoff_wall_deadline` 全族不再存在。v2 的重试语义是**立即重入队**：

- 失败（未耗尽预算）→ `ImmediateRequeuePolicy` 规划 `RequeuePlan(front=True, delay_seconds=0.0)` → 队首重入队 → 下一轮扫描即可再次派发；
- 重试间隔不再由框架注入——若业务需要节奏（固定退避、指数退避、限速窗），经门面构造参数 `requeue_policy` 注入自定义 `RequeuePolicy`（见第 5 节示例骨架），核心零改动；
- 瞬态信号（孤儿锁冲突 / 外部中断 / 限速）天然受益：零等待重入队 + 不烧重试预算 + 零污染，实际等待由资源挂起 TTL（`ctx.suspend_resource` 的跨重启持久化挂起）承担，而非框架级退避计时器。

这一取舍的裁决依据与重开条件（退避类策略经 seam 回归须新立 ADR）见 ADR-0004「调度 seam」与「后果与重开条件」章节。
