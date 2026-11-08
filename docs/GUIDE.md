# TaskLite 使用手册

> 本文档是 TaskLite 的官方使用手册，系统定义公开导入面、Task / Job / Attempt 三层模型与数据库 Schema 6、`JobContext` 与统一文件声明授权、统一状态管理与历史清理、只读一致状态快照（`StateSnapshot`），以及调度扩展接缝（`OrderingPolicy` / `RequeuePolicy` / `Exclude`）。项目概览与快速上手见 [`README.md`](../README.md)，架构决策记录见 [`docs/adr/`](adr/)。

---

## 1. 公开导入面与架构边界

**唯一承诺稳定的顶层公开导入面**是 `tasklite/__init__.py` 导出的核心 10 个名称：

```python
from tasklite import (
    TaskLite, Task, Job, JobContext, TaskResult, FileSpec,
    StateSnapshot, TaskLiteError, make_job_uid, parse_job_uid,
)
```

内置资源与运行排除规则从各自职责模块导入，不在根包重复导出：

```python
from tasklite.resource import CapacityResource, IntervalResource
from tasklite.selection import Exclude
```

可选工具（HTTP 守卫、Discovery 扫描、测试辅助等）保留在各自模块中，**本版暂不承诺稳定兼容**，导入路径为：

```python
from tasklite.wrappers.http import http_guard, urllib_fetch, SQLiteSnapshotStore
from tasklite.wrappers.discovery import register_discovery
from tasklite.testing import fake_ctx
```

### 架构红线与分层依赖

- **分层单向依赖**：
  - `tasklite/models/` 严禁反向 import `tasklite/engine/`（IPC 声明读写下沉 `tasklite/utils/ipc.py`）；
  - `tasklite/utils/` 严禁反向 import `tasklite/wrappers/`；
  - 核心层（`tasklite/engine/`, `tasklite/backend/`, `tasklite/models/`, `tasklite/utils/`）严禁依赖 `tasklite/contrib/` 与 `tasklite/wrappers/`。
- **单射编码红线**：所有业务标识与复合 UID 派生，一律使用可逆单射编码（`make_job_uid` / `parse_job_uid`），禁止丢弃式净化。
- **单一出口原则**：Job 终结唯一经由 `CompletionMachine.complete_job` 收尾；失败终态登记唯一经由 `StateStore.apply_failure` 收敛。
- **数据库严格版本边界**：当前状态库格式为 Schema 6（`user_version=6`）。旧库（Schema ≤ 5、非空版本 0 库或异形库）在只读预检阶段 **fail-loud 拒绝启动**，不提供自动原地迁移。

---

## 2. Task / Job / Attempt 三层模型

TaskLite 采用规格、逻辑实例与物理轨迹正交的三层模型：

| 层级 | 职责与定义 | 持久化与生命周期 |
|---|---|---|
| **Task（规格）** | 进程内静态规格模板：`task_type`、handler、默认 `resources`、`max_retries_per_run`、`timeout_seconds` | 进程内代码规格，不持久化；注册于 `TaskLite.register_task` |
| **Job（逻辑实例）** | 一次有界激活的逻辑任务；`uid = make_job_uid(task_type, job_id)` 身份唯一且不变 | `queue` 行 `job_snapshot` JSON（`job_uid` 为主键） |
| **Attempt（物理轨迹）** | 单次物理进程执行的轨迹记录：执行代号、起止时间、`outcome` 终态、结果与错误信息 | **append-only** 物理记录于 `attempts` 表 |

### Task：规格层

`Task` 是不可变数据类，定义某类任务的处理逻辑与默认资源/预算约束：

```python
from tasklite import Task, TaskResult

def my_handler(job, ctx):
    return TaskResult.success(metadata={"processed": True})

task = Task(
    "download",                    # task_type（非空 str，不含 "::"）
    my_handler,                    # handler 签名 Callable[[Job, JobContext], TaskResult]
    resources={"api": 1.0},        # 默认资源占用
    max_retries_per_run=3,         # 单次运行内最大重试预算（默认 0）
    timeout_seconds=3600.0,        # 执行超时秒数（默认 None）
)
pipeline.register_task(task)
```

`TaskRegistry` 契约：`task_type` 一对一绑定；重复注册抛出 `ValueError`，未注册执行抛出 `KeyError`。

### Job：逻辑实例层

`Job` 表示具体的作业实例。未指定的规格参数（传 `None`）将在入队时自动继承对应 `Task` 模板的默认值：

```python
from tasklite import Job, FileSpec

job = Job(
    "download",
    "item_001",
    payload={"url": "https://example.com/file.zip"},  # 业务参数（须可 JSON 序列化）
    depends_on=["preprocess::batch_1"],              # DAG 依赖的上游 job_uid 列表
    files={                                          # 预声明文件规范
        "output_zip": FileSpec("data/file.zip", role="output", delete_existing=True),
    },
    resources={"api": 1.0},                          # 可覆盖 Task 默认值
    max_retries_per_run=3,                           # 可覆盖 Task 默认值
    timeout_seconds=1800.0,                          # 可覆盖 Task 默认值
)

print(job.uid)  # "download::item_001"
```

#### 身份与单射编码

`job.uid` 格式为 `{task_type}::{job_id}`。若标识符中包含 `::` 或 `%`，框架通过 `make_job_uid` 和 `parse_job_uid` 执行标准百分号转义，保证跨系统、跨数据库字段的单射可逆性。

### Attempt：执行轨迹（append-only 旁路面）

派发时只将 Job 持久化为 `running` 并分配单调递增的 `attempt_no`，不插入 Attempt。执行结算时，Attempt 与 Job 状态、档案及暂存效果在同一事务中追加；已提交的 Attempt 不再更新。异常退出遗留的 `running` 在下次启动时按失败结算，未派发的 `pending` 保留。

`outcome` 词汇表：

- `success`：执行成功并结算；
- `failure`：本次执行失败；通常 Job 进入 `failed`，但动态文件授权拒绝或等待期间停止时，受阻 worker 结束后 Job 回到 `pending`，本 Run 临时排除；
- `retry`：执行失败但已安排重入队重试。

**旁路观测解耦**：`attempts` 表为仅追加表，不参与成功/失败集合的互斥。系统由此实现「当前终态唯一（`successes` / `failed`）」与「历史完全可追溯（`attempts`）」的彻底正交。

### SQLite Schema 6 物理结构

Schema 6 包含以下核心表：
1. `queue(job_uid TEXT PRIMARY KEY, queue_position INTEGER, job_snapshot TEXT, status TEXT, run_id TEXT, attempt_no INTEGER, started_at TEXT)`
2. `successes(job_uid TEXT PRIMARY KEY, metadata TEXT)`
3. `failed(job_uid TEXT PRIMARY KEY, metadata TEXT, payload TEXT)`
4. `attempts(attempt_id INTEGER PRIMARY KEY AUTOINCREMENT, job_uid TEXT, attempt_no INTEGER, run_id TEXT, outcome TEXT, started_at TEXT, settled_at TEXT, job_snapshot TEXT, files TEXT, result TEXT, error_message TEXT)`
5. `job_sequences(job_uid TEXT PRIMARY KEY, last_attempt_no INTEGER)`
6. `cursors(key TEXT PRIMARY KEY, value TEXT)`
7. `engine_metadata(key TEXT PRIMARY KEY, value TEXT)`

---

## 3. 六步标准调用顺序与最小示例

TaskLite 遵循确定性的六步调用顺序：

**初始化 → `register_resource` → `register_task` → `enqueue` → `run` → `stop`**

> **注意**：管理类 API（`set_job_status` / `clear_history` / `get_cursor` / `set_cursor` / `list_authorizations` / `revoke_authorization`）必须在 `run()` 外调用。

### 最小可运行示例

```python
import logging
from pathlib import Path
from tasklite import TaskLite, Task, Job, TaskResult, FileSpec
from tasklite.resource import IntervalResource

logging.getLogger("tasklite").setLevel(logging.INFO)

# 1. 任务处理器（独立子进程中执行，必须在模块级定义以便 pickle）
def fetch_handler(job: Job, ctx) -> TaskResult:
    # 声明产物文件：相对路径按 output_roots 基准解析
    out_file = ctx.declare_file(
        "dest",
        FileSpec(f"files/{job.job_id}.txt", role="output", delete_existing=True),
    )
    url = job.payload.get("url")
    if not url:
        return TaskResult.failure(message="URL 缺失，永久失败")

    # 业务写入
    out_file.write_text(f"content from {url}", encoding="utf-8")
    return TaskResult.success(metadata={"bytes": 1024})

# 2. 初始化引擎（使用独立临时文件）
pipeline = TaskLite(
    backend="sqlite",
    state_path="./state.db",       # 状态库文件路径
    output_roots="./output",        # 产物根目录
    max_workers=2,                 # 并发子进程数
)
pipeline.authorizer = lambda path, target_type: True  # 自动同意文件操作

# 3. 注册资源与 Task 规格
pipeline.register_resource(IntervalResource("net", interval_seconds=0.1))
pipeline.register_task(Task("fetch", fetch_handler, resources={"net": 1.0}, max_retries_per_run=2))

# 4. 入队作业
pipeline.enqueue(Job("fetch", "item_1", payload={"url": "https://example.com/1"}))
pipeline.enqueue(Job("fetch", "item_2", payload={"url": "https://example.com/2"}))

# 5. 启动运行（阻塞至队列清空）
pipeline.run()

# 6. 停机与资源释放
pipeline.stop()
pipeline.close()
```

### Handler 返回值与 TaskResult

Handler **必须显式返回 `TaskResult`**，严禁返回布尔值、元组或裸数据：

- `TaskResult.success(*, message=None, metadata=None)`：执行成功，`metadata` 必须为严格合法的 JSON 对象；
- `TaskResult.failure(*, message=None, metadata=None)`：确定性永久失败，直接耗尽预算进失败档案；
- `TaskResult.retry(*, message=None, metadata=None, consume_budget=True)`：请求重试，按重入队接缝安排下次执行。

### JobContext 运行时接口

传递给 handler 的 `ctx` 对象提供以下运行时能力：
- `ctx.spawn(job: Job) -> None`：在子进程中动态派生子任务（父任务成功提交时原子入队，父任务失败则零发布）；
- `ctx.declare_file(name: str, spec: FileSpec) -> Path`：动态声明文件并请求主进程授权与父目录准备；
- `ctx.file_path(name: str) -> Path`：查询已声明文件的绝对路径；
- `ctx.get_cursor(key: str) -> Any`：读取事务游标快照；
- `ctx.set_cursor(key: str, value: Any) -> None`：更新游标（父任务成功提交时原子持久化）。

### 文件操作授权管理

当任务声明 `delete_existing`、`delete_on_failure` 或 `delete_on_success` 时，主进程拥有排他授权检查职责：
- 环境变量 `TASKLITE_AUTO_APPROVE=1`：对所有处于受保护根目录内的常规请求自动同意；
- `pipeline.authorizer = Callable[[str, str], bool | None]`：自定义授权回调，返回 `True` 批准，`False` 拒绝，`None` 挂起等待；
- 授权拒绝会使作业安全返回 pending 并不写失败档案；受保护的核心文件（状态库与锁文件）绝对禁止声明或删除；
- Run 外可调用 `pipeline.list_authorizations()` 查看当前已持久化授权，调用 `pipeline.revoke_authorization(path)` 精准撤销。

---

## 4. 统一状态管理与历史运维（Run 外 API）

所有运维与状态变更操作收敛在 `TaskLite` 门面，且**严格限于 `run()` 外部调用**（`run()` 执行期间调用将抛出 `RuntimeError`）：

### 人工变更作业状态：`set_job_status`

```python
# 将作业重置为 pending，级联重置下游依赖
pipeline.set_job_status("fetch::item_1", "pending", recursive=True)

# 或标记人工成功并附加说明
pipeline.set_job_status("fetch::item_1", "succeeded", recursive=False, note="人工标记成功")
```

- `status` 仅支持 `"pending"`、`"succeeded"`、`"failed"` 三个合法目标态；
- `recursive=True` 时，状态变更自动级联到其下游所有依赖作业；
- 人工设置成功不会伪造 Attempt 记录，但在元数据中记录 `completion_source="manual"` 与可选的 `note`。

### 清理历史记录：`clear_history`

```python
# 精准清理选中的历史档案（返回值表示删除记录数）
deleted_count = pipeline.clear_history("fetch::item_1", records=["successes", "attempts"])
```

- 精准删除指定的历史集合（`successes` / `failed` / `attempts`）；
- **不变式：内部单调编号计数器绝不重置**。清空历史后重新入队相同 `job_uid`，其新产生的 Attempt 序号依然保持单调自增（从历史最大序号之后继续），不回退到 1。

### 高水位游标读写

```python
pipeline.set_cursor("offset", "10050")
value = pipeline.get_cursor("offset")
```

---

## 5. 只读一致状态快照与 JSON 查询

`StateSnapshot` 是对状态库进行只读观察的单一权威入口。支持从数据库路径或 `TaskLite` 实例打开：

```python
from tasklite import StateSnapshot

# 方式一：从数据库路径只读打开（持有 SQLite 独立读事务）
with StateSnapshot.open("./state.db") as snap:
    job = snap.get_job("fetch::item_1")
    print(job["status"], job["result"])

# 方式二：从 TaskLite 实例打开
with StateSnapshot.open(pipeline) as snap:
    for job in snap.iter_jobs(status="succeeded"):
        print(job["job_uid"])
```

### 一致性保证

- SQLite 快照持有只读连接与独立事务，外部正在进行的写入与提交不会破坏快照打开时的一致性视图；
- 内存快照在打开瞬间捕获一致性版本；快照只读，不阻塞写入者。

### 结构化 JSON 查询结果样本

`get_job(uid)` 与 `iter_jobs(...)` 返回标准规范字典：

```python
{
    "job_uid": "fetch::item_1",
    "status": "succeeded",              # "pending" | "running" | "succeeded" | "failed"
    "job": {                            # 静态配置快照
        "task_type": "fetch",
        "job_id": "item_1",
        "payload": {"url": "..."},
        "depends_on": [],
        "files": {
            "dest": {
                "path": "/abs/output/files/item_1.txt",
                "role": "output",
                "delete_existing": True,
                "delete_on_failure": False,
                "delete_on_success": False,
            }
        },
        "resources": {"net": 1.0},
        "max_retries_per_run": 2,
        "timeout_seconds": 1800.0,
    },
    "files": {                          # 外层解析后的文件实际路径映射
        "dest": {
            "path": "/abs/output/files/item_1.txt",
            "role": "output",
            "delete_existing": True,
            "delete_on_failure": False,
            "delete_on_success": False,
        }
    },
    "result": {                         # 终态结果（未完成时为 None）
        "message": None,
        "exception": None,
        "metadata": {"bytes": 1024},
        "annotations": {},
    },
    "run_id": "run_20261108_...",
    "attempt_no": 1,
    "started_at": "2026-11-08T10:00:00.123456Z",
    "settled_at": "2026-11-08T10:00:01.654321Z",
}
```

`iter_attempts(job_uid=...)` 按 `attempt_no` 升序产出物理执行轨迹：

```python
{
    "job_uid": "fetch::item_1",
    "attempt_no": 1,
    "run_id": "run_20261108_...",
    "outcome": "success",               # "success" | "failure" | "retry"
    "started_at": "2026-11-08T10:00:00.123456Z",
    "settled_at": "2026-11-08T10:00:01.654321Z",
    "job": {...},
    "files": {...},
    "result": {...},
    "error_message": None,
}
```

---

## 6. 调度扩展接缝与规则排除

核心层绝对禁止任何调度排序与退避计算。调度节奏与候选准入经由三个扩展接缝以各自 seam 形态扩展：

### OrderingPolicy：队列访问序

调度器通过 `OrderingPolicy.visit_order` 获取候选队列的评估顺序。默认实现为 `FifoOrderingPolicy`（按入队顺序升序扫描）。

```python
from tasklite.engine.scheduler import OrderingPolicy, FifoOrderingPolicy

class CustomOrderingPolicy(OrderingPolicy):
    """自定义队列访问序策略。"""
    def visit_order(self, queue):
        return range(len(queue) - 1, -1, -1)
```

### RequeuePolicy：重试节奏

当任务请求重试时，通过 `RequeuePolicy.plan_requeue` 规划重入队行为。默认实现为 `ImmediateRequeuePolicy`（立即插回队首重试）。

```python
from tasklite.engine.admission import RequeuePolicy, ImmediateRequeuePolicy, RequeuePlan

class CustomRequeuePolicy(RequeuePolicy):
    """自定义重试节奏策略。"""
    def plan_requeue(self, job_snapshot, *, transient_kind=None) -> RequeuePlan:
        return RequeuePlan(front=True, delay_seconds=0.0)
```

### Exclude：运行期临时排除

通过 `run(exclude=Exclude(task_type="..."))` 排除特定类型的任务：
- 命中作业本次 run 不派发、不执行、不消耗重试预算，**原样保留在队列中**；
- 下次未指定排除规则的 `run()` 启动时，留队作业自动恢复正常调度与执行。

---

## 7. 破坏性调用迁移指引与旧库说明

### 接口变更对照表

| 历史用法 / 预 ADR-0011 接口 | ADR-0011 稳定基线标准 | 说明 |
|---|---|---|
| `TaskLite(name=..., state_dir=...)` | `TaskLite(state_path="./state.db")` | 废除名称与目录组合，统一显式指定状态库文件路径 |
| `return True, {"data": 1}` | `return TaskResult.success(metadata={"data": 1})` | handler 返回值统一收敛为显式 `TaskResult` 值对象 |
| `raise RetryError(...)` | `return TaskResult.retry(message=...)` | 显式重试返回，消除内部魔法异常 |
| `ctx.declare_output(path)` | `ctx.declare_file(name, FileSpec(path, role="output"))` | 统一文件声明对象，包含明确角色与清理权限 |
| `ctx.get_artifact_path(...)` | `ctx.file_path(name)` 或上游预声明 | 消除旧产物查找协议，规范为已声明文件路径解析 |
| `RateLimitResource(...)` | `IntervalResource(name, interval_seconds)` | 资源命名统一，从 `tasklite.resource` 导入 |
| `list_failures` / `clear_failures` / `retry_failure` | `set_job_status` / `clear_history` / `StateSnapshot` | 失败档案管理收敛为统一状态与历史管理 API |
| `pipeline.selection(...)` / `Skip` | `pipeline.run(exclude=Exclude(...))` | 删除复杂规则容器与 Skip 终态跳过，仅保留 Exclude 延迟留队 |
| `RunSummary` / `pipeline.stats` | `pipeline.run()` 返回 `None`，查询用 `StateSnapshot` | 移除易过期的统计快照，由一致只读快照提供权威观察 |

### 存量数据库说明

**本引擎不提供旧版状态库的原地自动迁移**：
- SQLite Schema 6 为全新结构（新增 `job_sequences` 表与完整 `attempts` 记录）；
- 打开 Schema ≤ 5、非空版本 0 库或异形库时，引擎在只读预检阶段直接抛出 `RuntimeError` 拒绝启动；
- 存量生产业务请保留原状态数据库，使用匹配旧版本读取历史，升级时请通过新的 `state_path` 进行冷启动。
