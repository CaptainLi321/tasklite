# TaskLite 使用手册

> 本文档是 TaskLite 的官方使用手册，系统定义公开导入面、Task / Job / Attempt 三层模型与数据库 Schema 6、Trigger 与恢复边界、`JobContext` 与统一文件声明授权、统一状态管理与历史清理，以及只读一致状态快照（`StateSnapshot`）。项目概览与快速上手见 [`README.md`](../README.md)，架构决策记录见 [`docs/adr/`](adr/)。

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
    max_retries_per_run=3,         # 单次运行内最大重试预算（默认 3）
    timeout_seconds=3600.0,        # 执行超时秒数（默认 3600，必须为有限正数）
    trigger="once",                # 本次 Run 的启动规则（默认 once）
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

#### Trigger：本次 Run 的启动判断

Trigger 只决定一次 Run 中已入队 Job 是否启动，不主动入队，也不控制一次启动后的故障重试。内置名称为 `once`、`until_success`、`always` 与 `on_input_change`；`until_success` 在已有失败终态时允许再次启动。

| Trigger | 无终态记录 | 当前成功档案 | 当前失败档案（含取消） |
| --- | --- | --- | --- |
| `once` | 执行 | 跳过 | 跳过 |
| `until_success` | 执行 | 跳过 | 执行 |
| `always` | 执行 | 执行 | 执行 |
| `on_input_change` | 执行 | 按输入变化契约判断 | 执行 |

同一 Run 内由 `TaskResult.retry` 产生的重试不重复求值 Trigger；新 Run 接续仍在 `pending` 的任务会重新求值。Trigger 返回 False 表示本次无需执行，不产生成功证据或 Attempt，也不等同于 `Exclude` 的留队。

### Attempt：执行轨迹（append-only 旁路面）

派发时只将 Job 持久化为 `running` 并分配单调递增的 `attempt_no`，不插入 Attempt。执行结算时，Attempt 与 Job 状态、档案及暂存效果在同一事务中追加；已提交的 Attempt 不再更新。异常退出遗留的 `running` 在下次取得状态库执行权后按失败结算，未派发的 `pending` 保留并参与新 Run 的 Trigger 判断。

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

`job_sequences` 是独立编号进度，不从当前队列、终态档案或剩余 Attempt 最大值推导。清空某 UID 的全部档案与 Attempt 后重新入队，`attempt_no` 仍从该 UID 已分配到的位置继续单调递增；人工状态修改不推进编号。

### 下次启动的恢复边界

取得状态库级执行权后，框架先收敛遗留状态，再开始新 Run 的 Trigger 判断和派发：

| 数据库事实 | 启动处理 |
| --- | --- |
| Job 为 `pending` | 保留，参与新 Run 启动判断，本次 Run 的重试预算从零开始 |
| Job 为 `running` | 按原 `run_id` 与 `attempt_no` 原子追加一条失败 Attempt，转为 `failed`，失败原因记录为“上次执行未完成结算，实际结果未知” |
| 本次终态已提交 | 保持 `succeeded` / `failed`，不受之后进程退出影响 |
| 终态档案与当前记录冲突 | 明确报错停止，不猜测哪条记录正确 |

恢复不根据残留产物或迟到 Pipe 消息补造成功，不自动接续旧执行，也不声称旧 worker 或它启动的外部程序已经停止。正常 Ctrl+C 或 `stop()` 的排空不属于异常退出恢复；只读 `StateSnapshot` 不执行恢复写入。

---

## 3. 六步标准调用顺序与最小示例

TaskLite 遵循确定性的六步调用顺序：

**初始化 → `register_resource` → `register_task` → `enqueue` → `run` → `stop`**

> **注意**：管理类 API（`set_job_status` / `clear_history` / `get_cursor` / `set_cursor` / `list_authorizations` / `revoke_authorization`）必须在 `run()` 外调用。

### 最小可运行示例

```python
from tasklite import FileSpec, Job, Task, TaskLite, TaskResult
from tasklite.resource import IntervalResource


def fetch_handler(job: Job, ctx) -> TaskResult:
    # 相对路径按 TaskLite 构造时固定的工作目录解析。
    out_file = ctx.declare_file(
        "dest",
        FileSpec(f"output/files/{job.job_id}.txt", role="output"),
    )
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(job.payload["content"], encoding="utf-8")
    return TaskResult.success()


if __name__ == "__main__":
    with TaskLite(backend="sqlite", state_path="./state.db", output_roots="./output") as pipeline:
        pipeline.register_resource(IntervalResource("net", interval_seconds=0.1))
        pipeline.register_task(Task("fetch", fetch_handler, resources={"net": 1.0}))
        pipeline.enqueue(Job("fetch", "item_1", payload={"content": "示例一"}))
        pipeline.enqueue(Job("fetch", "item_2", payload={"content": "示例二"}))
        pipeline.run()
        pipeline.stop()
```

实例构造时捕获主进程当前工作目录，作为该实例固定且统一的相对路径基准。`enqueue`、`ctx.spawn` 与运行期 `ctx.declare_file` 都使用该基准，不读取主进程或 worker 后来改变的工作目录；worker 改变自身 CWD 不改变子任务相对路径的含义。使用同一状态库接续执行时，应保持相同的实例路径基准；跨目录接续不在本版支持范围内。

### Handler 返回值与 TaskResult

Handler **必须显式返回 `TaskResult`**，严禁返回布尔值、元组或裸数据：

- `TaskResult.success(*, message=None, metadata=None)`：执行成功，`metadata` 必须为严格合法的 JSON 对象；
- `TaskResult.failure(*, message=None, metadata=None)`：确定性永久失败，直接耗尽预算进失败档案；
- `TaskResult.retry(*, message=None, metadata=None, consume_budget=True)`：请求重试，按当前重入队机制安排本次 Run 内的下一次执行；预算耗尽或 Run 已停止时不会自动开启下一次 Run。

### JobContext 运行时接口

传递给 handler 的 `ctx` 对象提供以下运行时能力：
- `ctx.spawn(job: Job) -> None`：在子进程中动态派生子任务（父任务成功提交时原子入队，父任务失败则零发布）；
- `ctx.declare_file(name: str, spec: FileSpec) -> Path`：动态声明文件并请求主进程授权与父目录准备；
- `ctx.file_path(name: str) -> Path`：查询已声明文件的绝对路径；
- `ctx.get_cursor(key: str) -> str | None`：读取派发快照与本次暂存更新；
- `ctx.set_cursor(key: str, value: str | None) -> None`：更新或删除游标（父任务成功提交时原子持久化，失败或重试丢弃）。

### 统一文件声明

预声明与运行期声明都使用同一个 `FileSpec`：

```text
FileSpec(
    path,
    *,
    role="input" | "output" | "cache",
    delete_existing=False,
    delete_on_failure=False,
    delete_on_success=False,
)
```

- 名称只由 `Job(files={...})` 的字典键或 `ctx.declare_file(name, spec)` 的参数指定，不写进 `FileSpec`；
- `role` 必填，仅支持 `input`、`output`、`cache`；`input` 禁止 `delete_existing=True`；
- 三项删除标记默认均为 False，只有显式请求时才可能由主进程执行清理；
- 框架不因声明自动创建文件、目录或父目录，handler 与外部程序负责实际业务写入；
- `ctx.file_path(name)` 与 `ctx.declare_file(name, spec)` 都返回规范绝对 `Path`，且统一使用实例构造时固定的路径基准。

### 文件操作授权管理

文件声明用于路径管理、依赖传递与清理请求，不是 handler 的写入沙箱。读取默认允许；当任务声明 `delete_existing`、`delete_on_failure` 或 `delete_on_success` 时，主进程拥有授权检查与清理执行职责：
- 环境变量 `TASKLITE_AUTO_APPROVE=1`：自动同意本进程实际请求且未命中硬性保护规则的框架文件操作，并记录实际授权路径；
- `pipeline.authorizer = Callable[[str, str], bool | None]`：自定义授权判定，参数为规范绝对路径与 `file` / `directory`；返回 `True` 批准并持久化，`False` 拒绝，`None` 表示暂不能决定；
- 等待 `None` 结果时暂停新派发，继续处理已派发执行；不占用业务资源，也不消耗执行超时预算；
- 授权拒绝或等待期间收到停止请求时，先结束受阻 worker，再追加 `failure` Attempt、把 Job 回到 `pending`，并将受影响依赖链加入本 Run 临时排除；不写失败档案、不消耗重试预算；
- 受保护的状态库、WAL／SHM 副文件与库级锁文件绝对禁止声明或删除；
- Run 外可调用 `pipeline.list_authorizations()` 查看当前已持久化授权，调用 `pipeline.revoke_authorization(path)` 精准撤销。

授权等待是文件准备协议的一部分，不是资源挂起。`TaskResult.retry`、资源不足和授权等待具有不同语义，不共享重试或挂起入口。

---

## 4. 统一状态管理与历史运维（Run 外 API）

所有运维与状态变更操作收敛在 `TaskLite` 门面，且**严格限于 `run()` 外部调用**。在 `run()` 执行期间调用公开管理 API 或再次调用 `run()`，框架主动抛出 `TaskLiteError`；底层库自然产生的异常保持原类型传播：

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
- **不变式：内部单调编号进度绝不重置**。清空历史后重新入队相同 `job_uid`，其新产生的 Attempt 序号仍从该 UID 的独立编号进度继续单调自增，不依赖剩余历史，也不回退到 1。

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

## 6. 候选顺序、重试节奏与运行排除

核心层不包含业务排序或退避计算。`OrderingPolicy` 与 `RequeuePolicy` 是当前内部实现接缝，本版不承诺其类名、方法签名或自定义子类兼容，也不属于公开导入面；需要冻结新的调度扩展协议时必须另立 ADR。

### Exclude：运行期临时排除

公开、稳定的运行期规则是 `run(exclude=Exclude(task_type="..."))`，其中 `Exclude` 从 `tasklite.selection` 导入，本版只接受单个规则或 `None`：
- 命中作业本次 run 不派发、不执行、不消耗重试预算，**原样保留在队列中**；
- 命中任务及受影响下游本次留队；规则不改变排序，也不终止已经派发的执行；
- 下次未指定排除规则的 `run()` 启动时，留队作业重新参与候选与 Trigger 判断。

---

## 7. 破坏性调用迁移指引与旧库说明

### 接口变更对照表

| 历史用法 / 预 ADR-0011 接口 | ADR-0011 稳定基线标准 | 说明 |
|---|---|---|
| `TaskLite(name=..., state_dir=...)` | `TaskLite(backend="sqlite", state_path="./state.db")` | 后端必须显式选择；SQLite 显式指定状态库文件路径 |
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
