# TaskLite 公开 API 参考手册

> 本文档是 TaskLite 3.0.0.dev0（Schema 6 稳定接口基线版本）的权威公开 API 参考手册。系统定义所有公开类、构造器、方法、属性、参数约束、默认值、状态转移规则、精确异常边界、命令行运维契约以及数据字典结构。  
> 概念架构、核心模型与使用指南参见 [`docs/GUIDE.md`](GUIDE.md)，架构决策背景参见 [`docs/adr/0011-stable-interface-baseline.md`](adr/0011-stable-interface-baseline.md) 与 [`docs/adr/0012-graph-edit-snapshot-preauth-and-platform-parity.md`](adr/0012-graph-edit-snapshot-preauth-and-platform-parity.md)，领域术语规范参见 [`CONTEXT.md`](../CONTEXT.md)。

---

## 目录

1. [导入面、稳定性分级与全局契约](#1-导入面稳定性分级与全局契约)
   - [1.1 导入入口与稳定性分级](#11-导入入口与稳定性分级)
   - [1.2 全局横切契约与严格 JSON 校验](#12-全局横切契约与严格-json-校验)
   - [1.3 异常传播原则与触发边界](#13-异常传播原则与触发边界)
   - [1.4 进程隔离与可序列化契约](#14-进程隔离与可序列化契约)
2. [主调度门面 TaskLite](#2-主调度门面-tasklite)
   - [2.1 构造签名与初始化装配](#21-构造签名与初始化装配)
   - [2.2 公开属性与生命周期状态](#22-公开属性与生命周期状态)
   - [2.3 上下文管理与 close() 机制](#23-上下文管理与-close-机制)
   - [2.4 注册接口（资源、任务与触发规则）](#24-注册接口资源任务与触发规则)
   - [2.5 入队与运行控制（enqueue, run, stop）](#25-入队与运行控制enqueue-run-stop)
   - [2.6 Run 外状态运维、图修整与授权接口](#26-run-外状态运维图修整与授权接口)
3. [核心模型与运行时上下文](#3-核心模型与运行时上下文)
   - [3.1 Task 静态规格](#31-task-静态规格)
   - [3.2 Job 逻辑实例](#32-job-逻辑实例)
   - [3.3 JobContext 运行时上下文](#33-jobcontext-运行时上下文)
   - [3.4 TaskResult 执行结果值对象](#34-taskresult-执行结果值对象)
   - [3.5 FileSpec 统一文件声明与清理授权](#35-filespec-统一文件声明与清理授权)
4. [只读一致状态快照 StateSnapshot](#4-只读一致状态快照-statesnapshot)
   - [4.1 快照建立与会话生命周期](#41-快照建立与会话生命周期)
   - [4.2 get_job 查询与标准化 Job 字典](#42-get_job-查询与标准化-job-字典)
   - [4.3 iter_jobs 迭代与状态过滤](#43-iter_jobs-迭代与状态过滤)
   - [4.4 iter_attempts 迭代与物理执行轨迹字典](#44-iter_attempts-迭代与物理执行轨迹字典)
   - [4.5 StateSnapshot.report 状态报告与前后对比](#45-statesnapshotreport-状态报告与前后对比)
5. [单射编码工具与公开异常](#5-单射编码工具与公开异常)
   - [5.1 规范 UID 单射编解码（make_job_uid, parse_job_uid）](#51-规范-uid-单射编解码make_job_uid-parse_job_uid)
   - [5.2 公开异常 TaskLiteError](#52-公开异常-taskliteerror)
6. [内置资源与运行排除](#6-内置资源与运行排除)
   - [6.1 CapacityResource 并发容量资源](#61-capacityresource-并发容量资源)
   - [6.2 IntervalResource 时间间隔资源](#62-intervalresource-时间间隔资源)
   - [6.3 Exclude 运行排除规则](#63-exclude-运行排除规则)
7. [极简运维命令行入口](#7-极简运维命令行入口)
   - [7.1 子命令语法与行为契约](#71-子命令语法与行为契约)
   - [7.2 安全守卫、退出码与非目标](#72-安全守卫退出码与非目标)
8. [环境变量](#8-环境变量)
   - [8.1 TASKLITE_DANGEROUS_AUTO_APPROVE 危险自动授权开关](#81-tasklite_dangerous_auto_approve-危险自动授权开关)
9. [可选扩展与测试辅助模块](#9-可选扩展与测试辅助模块)
   - [9.1 tasklite.testing 单元测试工具](#91-tasklitetesting-单元测试工具)
   - [9.2 tasklite.wrappers.http HTTP 工具集](#92-tasklitewrappershttp-http-工具集)
   - [9.3 tasklite.wrappers.discovery 增量发现组件](#93-tasklitewrappersdiscovery-增量发现组件)
   - [9.4 tasklite.hooks 辅助工具](#94-tasklitehooks-辅助工具)

---

## 1. 导入面、稳定性分级与全局契约

### 1.1 导入入口与稳定性分级

TaskLite 严格划分稳定核心面与可选辅助面：

#### 稳定核心面（承诺向后兼容契约）

1. **顶层导出（`tasklite`）**：仅导出 10 个核心符号：

```python
from tasklite import (
    TaskLite,
    Task,
    Job,
    JobContext,
    TaskResult,
    FileSpec,
    StateSnapshot,
    TaskLiteError,
    make_job_uid,
    parse_job_uid,
)
```

2. **内置资源（`tasklite.resource`）**：

```python
from tasklite.resource import CapacityResource, IntervalResource
```

3. **运行排除（`tasklite.selection`）**：

```python
from tasklite.selection import Exclude
```

> [!IMPORTANT]
> **主包隔离保证**：执行 `import tasklite` 保证**不隐式加载**任何可选适配层（`tasklite.wrappers` 或 `tasklite.contrib`）。`tasklite.__all__` 仅含上述 10 个顶层名称。

#### 可选辅助面（显式声明：本版不作稳定兼容承诺）

以下模块为特定业务场景封装、测试工具或扩展接缝，扩展协议与方法签名可能随小版本演化：
- `pipeline.register_trigger` 与 `TriggerContext`：自定义触发器扩展接口与判定回调上下文对象；
- `tasklite.wrappers.http`：HTTP 请求、响应与快照存储工具；
- `tasklite.wrappers.discovery`：基于游标去重的增量分页扫描器；
- `tasklite.testing`：单元测试辅助工具（`fake_ctx`, `running`, `authorizer`, `in_process_workers`, `spawned_jobs`, `cursor_updates`）；
- `tasklite.hooks`：业务展示切片工具（`job_ref`, `slice_list`）。

---

### 1.2 全局横切契约与严格 JSON 校验

TaskLite 内部所有需持久化或跨进程 IPC 传递的配置与数据（包括 `Job.payload`、`TaskResult.metadata`、`TriggerContext.success_metadata`），均经过统一的**严格 JSON 递归校验**：

1. **合法标量类型**：
   - `None`：归一为 `None`；
   - `bool`：仅精确 `bool` 类型；
   - `int`：仅精确 `int` 类型（排除 `bool`，即使 `bool` 是 `int` 的子类）；
   - `float`：仅有限浮点数（`math.isfinite(val)` 必须为 `True`）；任何 `NaN`、`Infinity`、`-Infinity` 均直接抛出 `TaskLiteError`；
   - `str`：必须是严格合法的 UTF-8 字符串，若包含无法 UTF-8 编码的字符抛出 `TaskLiteError`。
2. **合法容器类型**：
   - `dict`：顶层与嵌套字典的键**必须且只能为精确 `str`**（不可为整数或其他可哈希对象），且键必须为合法 UTF-8 字符串；字典值递归校验；
   - `list`：元素递归校验。
3. **非法类型硬拒绝**：
   - 严禁传入 `tuple`、`set`、`bytes`、`Path`、`datetime`、自定义类实例或任何内置类型的自定义子类；
   - 违规时直接抛出 `TaskLiteError`（附带出错路径定位，如 `metadata.details[0]`），不进行任何静默强转。

---

### 1.3 异常传播原则与触发边界

TaskLite 遵循明确的异常边界划分：

| 异常分类 | 抛出类型 | 触发边界与场景 |
| :--- | :--- | :--- |
| **框架契约异常** | `tasklite.exceptions.TaskLiteError` | 框架主动检测到前置条件不满足、状态冲突、非法参数、未注册类型、Schema 版本不匹配、严格 JSON 校验失败或调用时序违规。是 TaskLite **唯一的公开异常类**。 |
| **底层环境异常** | Python 原生异常 | 文件系统权限缺失（`PermissionError`）、磁盘 IO 错误（`OSError`）、输入声明文件未就绪（`FileNotFoundError`）等，**原样向上传播**，保留原始异常类型与 traceback 堆栈。 |
| **数据库底层异常** | `sqlite3.Error` 原生异常 | SQLite 锁定超时、数据库磁盘损坏等 SQLite 原生异常原样向上抛出，不作掩盖。 |
| **工作进程内部崩溃** | 收敛为失败 Attempt | handler 执行期间抛出的任何异常或子进程崩溃（SIGSEGV / OOM / 超时强杀），主进程均将其捕获并结构化登记至 `attempts` 表与 `failed` 失败档案，不拖垮主调度循环。 |

---

### 1.4 进程隔离与可序列化契约

1. **工作子进程（Worker Subprocesses）**：
   - 任务在通过标准库 `multiprocessing.get_context("spawn")` 创建的独立隔离子进程中执行；
   - `Task.handler` 必须是**模块顶层可寻址、可 pickle 序列化**的函数，禁止使用局部函数、闭包或未绑定 `lambda`。
2. **主调度进程（Master Process）**：
   - 调度循环、状态持久化、文件授权检查、清理以及 `Task.trigger` 回调均**在主进程同步执行**；
   - 自定义触发器回调严禁发起可能导致主循环死锁的阻塞或重入操作。

---

## 2. 主调度门面 TaskLite

`TaskLite` 位于 [`tasklite/pipeline.py`](../tasklite/pipeline.py)，是调度循环、资源与任务注册、入队与执行的总控门面。

### 2.1 构造签名与初始化装配

```text
TaskLite(
    *,
    backend: Literal["sqlite", "memory"],
    state_path: str | Path | None = None,
    max_workers: int = 4,
)
```

#### 参数约束与校验
- **`backend`**（`Literal["sqlite", "memory"]`，必填，仅关键字参数）：
  - 必须为 `"sqlite"` 或 `"memory"`。传入其它类型或未知字符串抛出 `TaskLiteError`。
- **`state_path`**（`str | Path | None`，默认 `None`，仅关键字参数）：
  - 当 `backend="sqlite"` 时为**必填项**（为 `None` 或全空白字符串时抛出 `TaskLiteError`）。构造时自动转换为解析后的绝对路径 `Path(state_path).resolve()` 供内部后端使用。
  - 当 `backend="memory"` 时固定为 `None`。
- **`max_workers`**（`int`，默认 `4`，仅关键字参数）：
  - 最大并发工作子进程数。必须为非 `bool` 的 `int` 且 `>= 1`；非法值抛出 `TaskLiteError`。
  - 内部自动以此上限向资源管理器注册内置并发容量。
- **内部基准目录**：
  - 构造时内部自动捕获当时的绝对工作目录 `Path(os.path.abspath(os.getcwd()))`，后续相对路径声明的解析均基于此固定基准，不受运行时工作目录改变影响。
- **SQLite 状态库 Schema 6 校验**：
  - 若 `backend="sqlite"`，初始化时以 WAL 模式（`journal_mode=WAL`）连接。
  - 若数据库文件已存在且非空，严格校验 `PRAGMA user_version == 6` 及核心表结构。若版本 `<= 5`、异形库或含旧表，**fail-loud 直接抛出 `TaskLiteError` 拒绝启动**，不提供自动原地迁移。

---

### 2.2 公开属性与生命周期状态

- **`is_running`**（`bool`，只读）：管线当前是否正在执行 `run()` 循环中。

> [!NOTE]
> **私有化内部边界说明**：为保障面向稳定基线（[ADR-0011](adr/0011-stable-interface-baseline.md)）的封装纯洁性，状态库路径、基准目录、内部任务/资源注册表、运行时装配配置、后端类型标识及内部授权回调等实现字段均彻底私有化，不再向外暴露为公开属性。调用方应始终通过 `register_task`、`register_resource`、`list_authorizations`、`revoke_authorization` 等标准公开方法与管线交互，不得将内部字段当做稳定公开 API。

---

### 2.3 上下文管理与 close() 机制

`TaskLite` 支持上下文管理器协议：

```python
with TaskLite(backend="sqlite", state_path="./state.db") as pipeline:
    ...
```

- **`__enter__() -> TaskLite`**：若实例已关闭，抛出 `TaskLiteError("Cannot enter context of a closed TaskLite instance.")`。
- **`__exit__(...) -> None`**：自动调用 `self.close()`。
- **`close() -> None`**：
  - 释放底层执行通道通道、进程池与数据库句柄；
  - 若当前管线正在执行中（`self.is_running is True`），抛出 `TaskLiteError("Cannot close TaskLite while run() is in progress.")`；
  - 幂等操作：对已关闭的实例重复调用 `close()` 静默无操作；
  - 关闭后，调用任何管理、入队或注册接口均抛出 `TaskLiteError`。

---

### 2.4 注册接口（资源、任务与触发规则）

所有注册接口必须在 `run()` 运行之外调用。

#### 1. `register_resource(resource: Resource) -> None`
- 注册自定义容量或时间间隔资源；
- `resource` 必须为 `Resource` 实例，否则抛出 `TaskLiteError`；
- **保留名称防护**：严禁注册名为 `"__workers__"` 的资源（此为框架内部工作进程并发控制保留名，必须通过 `max_workers` 配置），违规抛出 `TaskLiteError`；
- **重名拒绝**：若同名资源已存在，直接抛出 `TaskLiteError`，拒绝静默覆盖。

#### 2. `register_task(task_or_type, handler=..., *, ...)`
支持两种注册形式：
- **方式 A（规格式注册）**：传入预先构造好的 `Task` 实例：
  ```python
  pipeline.register_task(task_instance)
  ```
  - **混传拦截**：若传入 `Task` 实例的同时，附带了任何额外覆盖参数（`handler`、`resources`、`max_retries_per_run`、`timeout_seconds`、`trigger`），直接抛出 `TaskLiteError`，防止参数被静默忽略。
- **方式 B（简式注册）**：按参数就地构造 `Task` 并注册：
  ```text
  pipeline.register_task(
      task_type: str,
      handler: Callable[[Job, JobContext], TaskResult],
      *,
      resources: dict[str, float] | None = None,
      max_retries_per_run: int = 3,
      timeout_seconds: int | float = 3600,
      trigger: str = "once",
  )
  ```
  - `handler` 为必填项，未提供抛出 `TaskLiteError`。
- **重名拒绝**：若 `task_type` 已在内部注册表中存在，抛出 `TaskLiteError`。

#### 3. `register_trigger(name: str, fn: Callable[[TriggerContext], bool]) -> None`

> [!WARNING]
> **非稳定扩展接缝声明**：`register_trigger` 以及作为判定回调入参的 `TriggerContext` 属于**实验性/非稳定扩展接缝**，TaskLite 不作稳定向后兼容承诺。其方法签名、上下文对象属性与回调生命周期可能随小版本演化。请勿将通过该接缝可访问的内部对象视为稳定公开 API 依赖。

```python
from tasklite.models.trigger import TriggerContext
```

- 注册自定义触发器判定函数；
- `name` 必须为非空字符串；`fn` 必须为可调用对象；
- **覆盖拦截**：内置触发器名称（`once`、`until_success`、`always`、`on_input_change`）及已注册的自定义触发器均**严禁重复注册或覆盖**，违规抛出 `TaskLiteError`；
- **`TriggerContext` 契约**：传递给触发器函数的只读上下文值对象，属性定义如下：
  - `ctx.job`（`Job`）：待评估的逻辑作业实例；
  - `ctx.has_succeeded`（`bool`）：该作业此前是否曾在成功档案中持久化成功；
  - `ctx.has_failed`（`bool`）：该作业此前是否曾在失败档案中持久化失败；
  - `ctx.success_metadata`（`Mapping[str, Any] | None`）：该作业历史成功提交时的元数据映射（含 `inputs`、`artifacts` 等），无成功记录时为 `None`；
  - `ctx.last_succeeded_at_us`（`int | None`）：历史成功的微秒级时间戳，无成功记录时为 `None`。

内置触发器判定逻辑：
- `"once"`：当且仅当历史既未成功也未失败时触发（`not ctx.has_succeeded and not ctx.has_failed`）；
- `"until_success"`：当且仅当历史尚未成功时触发（`not ctx.has_succeeded`，允许自动重试此前进入失败档案的作业）；
- `"always"`：恒返回 `True`，每次调度均启动；
- `"on_input_change"`：当尚未成功或无时间基准时返回 `True`；已成功时检查 `ctx.success_metadata["inputs"]` 声明的各输入文件微秒修改时间（`mtime_ns`），只要任一输入文件较历史成功时间更新即触发。若 `inputs` 为空列表或非字符串路径，抛出 `TaskLiteError`；若输入文件在物理磁盘不存在，原样抛出 `FileNotFoundError`。

---

### 2.5 入队与运行控制（enqueue, run, stop）

#### 1. `enqueue(job: Job) -> bool`
- 将作业加入待调度队列；
- **参数类型校验**：必须传入 `Job` 实例，否则抛出 `TaskLiteError`；
- **所属任务校验**：作业的 `job.task_type` 必须已在当前管线中注册，否则抛出 `TaskLiteError`；
- **依赖闭合校验**：若作业显式声明了 `job.depends_on`，其引用的所有依赖 UID 必须**已经存在于当前队列、成功档案或失败档案中**，否则抛出 `TaskLiteError`；
- **幂等性判定比对集合**：
  - 若同 `uid` 作业已在当前队列（`queue` 表）中，提取**有效配置投影**进行全字段等价比对：
    ```python
    {
        "task_type": ...,
        "job_id": ...,
        "payload": ...,
        "depends_on": ...,
        "files": ...,
        "resources": ...,
        "max_retries_per_run": ...,
        "timeout_seconds": ...,
        "trigger": ...,
    }
    ```
  - 若所有有效字段严格一致：判定为幂等重复，**跳过入队并返回 `False`**；
  - 若任何有效字段存在差异：判定为配置冲突，**直接抛出 `TaskLiteError`**；
  - 若同 `uid` 作业不在当前待办队列中（即便其存在于历史成功档案或失败档案中）：成功写入队列并返回 `True`。后续该作业在本次 Run 中是否派发，交由其 `trigger` 规则在派发前依据历史档案判定。

#### 2. `run(*, exclude=None, install_signals=True) -> None`
- 启动调度循环，驱动作业派发与执行，直至队列排空或停机；正常完成返回 `None`。
- **参数约束**：
  - `exclude`：必须为 `Exclude` 实例或 `None`，非法类型抛出 `TaskLiteError`；
  - `install_signals`（`bool`，默认 `True`）：是否在主线程安装优雅停机信号处理器——POSIX 监听 `SIGTERM` 与 `SIGINT`，Windows 监听 `SIGTERM`、`SIGINT` 与 `SIGBREAK`（对应 `CTRL_BREAK_EVENT`），统一路由至线程安全的 `stop()`，退出时按 LIFO 逆序恢复原处理器；无关联控制台的 Windows 纯后台服务（如 `pythonw.exe` 或 Windows Service）无法接收 `CTRL_BREAK_EVENT`，须由宿主在进程内跨线程调用 `pipeline.stop()`。
- **内部核心执行流程**：
  1. **状态库排他锁定**：获取 `<state_path>.lock` 单库文件锁（例如 `state.db.lock`）与写事务互斥，确保单状态库同一时刻只有一个调度实例写入；
  2. **启动崩溃恢复（Recovery）**：启动预检扫描状态库。若发现上次运行异常崩溃遗留的 `running` 状态作业，将其原子收敛为失败 Attempt 并移入 `failed` 失败档案；保留处于 `pending` 状态的待办作业，不自动接续旧执行；
  3. **预检 Handler 可序列化性**：验证所有注册任务的 handler 均可被隔离子进程安全 pickle；
  4. **四相事件泵调度循环**：
     - **准入与选择（Admission & Selection）**：评估 `trigger` 规则与 `exclude` 排除规则（被排除的作业原样留队）；
     - **拓扑依赖与资源仲裁（Scheduling & Resources）**：解析 DAG 依赖，检查 `CapacityResource` 与 `IntervalResource` 可用性；
     - **物理派发与跨平台进程树隔离（Dispatching）**：通过独立引导子进程与双单向连接握手屏障派发执行——POSIX 以 `start_new_session=True` 建立独立进程组，超时或强杀时严格先 `os.killpg(..., SIGKILL)` 后 `wait` 原子回收整棵进程组；Windows 以 `CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP` 隔离主进程控制台信号穿透，并在放行用户代码前零竞态绑定仅含 `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` 的匿名 Job Object，确保超时、中止或主进程退出时整棵子进程树（含多层外部孙进程）被内核原子回收；
     - **结算收口（Completion）**：在独立写事务中原子提交结果、推进 Attempt 轨迹、更新游标与释放资源。

#### 3. `stop() -> None`
- 请求管线优雅停止。
- **行为与状态语义**：
  - 若在 `TaskLite` 已关闭后调用，抛出 `TaskLiteError`；
  - 幂等操作：多次调用无副作用；
  - 若在空闲态（非 `run()` 期间）调用，安全记录标记，不影响后续新启动的 `run()`；
  - 若在启动预检或运行期调用：发出单调状态转移请求，立刻停止调度循环派发**任何新作业**，主循环保持存活并安全等待所有当前在途（in-flight）的工作子进程完成结算后优雅退出。

---

### 2.6 Run 外状态运维、图修整与授权接口

> [!CAUTION]
> 以下运维管理接口**仅允许在 `run()` 之外调用**。若在管线执行期或关闭后调用，直接抛出 `TaskLiteError`。

#### 1. 原子图修整事务：`edit(*, dry_run=True, note=None) -> EditTransaction`

位于 [`tasklite/edit.py`](../tasklite/edit.py)，是 Run 外唯一的状态图增、删、替换与状态流转入口（取代并移除了预冻结版本的 `set_job_status` 与 `clear_history`）。

```text
with pipeline.edit(
    *,
    dry_run: bool = True,
    note: str | None = None,
) as tx:
    ...
```

- **生命周期与单写者互斥**：
  - 仅限 `run()` 外且实例未关闭时调用；`dry_run` 必须为精确 `bool`，`note` 必须为 `str | None`，否则抛 `TaskLiteError`；
  - 进入 `with` 块时获取状态库单写者互斥锁（SQLite 为 `<state_path>.lock`，内存后端为写锁）并加载一致基准状态，同时锁定当前 `TaskLite` 实例处于编辑态（事务期间调用同一实例的 `enqueue`、`set_cursor`、`get_cursor`、`grant_authorization`、`list_authorizations`、`revoke_authorization`、`register_*`、`run`、嵌套 `edit` 或 `close` 一律立即抛 `TaskLiteError`），退出 `with` 块时释放；同一 `EditTransaction` 实例不可重复进入，退出后禁止再调用写原语（但 `tx.before`、`tx.after`、`tx.diff()` 与 `uid in tx` 仍可只读访问）；
  - 默认 `dry_run=True`：执行完整闭环校验并生成 `tx.before`、`tx.after` 与 `tx.diff()`，退出时丢弃暂存修改、零磁盘写入；仅当显式传入 `dry_run=False` 且全部校验通过时，才在单个写事务内原子落盘。
- **三表字面工作图模型与核心不变式**：
  - 事务内部直接维护内存三表工作图（`queue` / `successes` / `failed`）以及待补写的遗留 `running` 失败 `Attempt`；块内每次原语调用按字面顺序即时更新对应分区，仅强制维护 Schema 6 固有的 `successes ∩ failed == ∅` 互斥不变式；
  - **明确边界与 Non-goal**：`remove(entries=...)` 仅按字面清理指定分区，不做跨表隐式联动；`replace` / `replace_many` 不承诺保留原 `queue_position`；异名改名替换（`old_uid != new_uid`）会清空 `old_uid` 全部三张表分区以彻底消除残留旧节点；显式指定 `status="pending"` 会同步清除历史 `successes`/`failed` 档案；仅同名替换且 `status=None` 自动继承 `"pending"` 时保留旧终态档案（并通过 `tx.diff()` 的 `[WARNINGS]` 段与 `logger.warning` 提示用户自决）；
  - **严格不变式**：`attempts` 物理执行轨迹**永不可删**；`job_sequences` 单调递增编号进度**永不重置**；图修整事务**绝不触发任何磁盘文件删除**。

##### `EditTransaction` 公开属性与方法

1. **`uid in tx`（`__contains__(uid: object) -> bool`）**：
   - 校验 `uid` 符合 `parse_job_uid` 规范语法（非法抛 `TaskLiteError`），返回该 `uid` 当前是否存在于三表工作图（`queue ∪ successes ∪ failed`）中。
2. **`tx.add(job: Job, *, status: Literal["pending", "succeeded", "failed"]) -> None`**：
   - 纯新增节点至指定分区；要求 `job` 为 `Job` 实例且 `job.task_type` 已注册、静态配置合法；若 `job.uid` 已存在于工作图中，立即抛 `TaskLiteError`（防误覆盖）。
3. **`tx.replace(old_uid: str, new_job: Job, *, status: Literal["pending", "succeeded", "failed"] | None = None) -> None`** 与 **`tx.replace_many(mapping: Mapping[str, Job], *, status: Literal["pending", "succeeded", "failed"] | None = None) -> None`**：
   - 演进替换既有节点（支持原地更新配置或改换 `uid`）；要求 `old_uid`（或 `mapping` 全部键）存在于工作图中、`new_job.task_type` 已注册，且目标 `new_job.uid` 不与工作图中其余存活节点冲突；`replace_many` 先统一摘除全部旧键再统一放入新节点，天然支持编号顺移或互换（`mapping` 内目标 `uid` 重复抛 `TaskLiteError`）；
   - **状态继承（`status=None`）**：原有效态为 `pending` / `succeeded` / `failed` 时自动继承原态；若原条目为前次崩溃遗留的 `running`，自动登记一条待补写的异常中断失败 `Attempt`（`outcome="failure"`），并将继承目标态收敛为 `"failed"`（显式指定 `status="pending"` 或 `"succeeded"` 时仍补写该失败 `Attempt`，目标态按显式参数设置）；
   - 不改动既有 `attempts` 与 `job_sequences`（换用新 `uid` 时，旧 `attempts` 原样归属 `old_uid`）。
4. **`tx.set_status(uid: str, status: Literal["pending", "failed"], *, recursive: bool) -> None`**：
   - 纯状态流转（零依赖 `TaskRegistry`，空管线实例与 CLI 可直接调用）；`recursive` 为必填关键字参数（精确 `bool`，无默认值）；`uid` 必须存在于工作图中；
   - 沿用既有静态 `job_snapshot` 将 `{uid}`（及 `recursive=True` 时工作图正向 `depends_on` 闭包内的全部下游）流转为 `"pending"` 或 `"failed"`；命中遗留 `running` 自动登记补写失败 `Attempt`；
   - 刻意不包含 `"succeeded"`——任何非执行 `"succeeded"` 必须经 `tx.add` 或 `tx.replace` 显式提供 `Job` 并通过产出验盘。
5. **`tx.remove(uid: str, *, entries: Collection[Literal["queue", "successes", "failures"]] = ("queue", "successes", "failures"), recursive: bool) -> int`**：
   - 按字面从指定分区移除条目（零依赖 `TaskRegistry`）；`recursive` 为必填关键字参数（无默认值）；`entries` 必须为非空集合（不可传裸 `str`/`bytes`），且仅允许 `"queue"` / `"successes"` / `"failures"`（传入 `"attempts"` 等非法值立即抛 `TaskLiteError`）；
   - `recursive=True` 且 `uid` 存在时级联覆盖正向 `depends_on` 闭包下游；若 `"queue" in entries` 且命中遗留 `running`，自动登记补写失败 `Attempt`；返回本次在工作图中实际移除的 Job 条目数（每个 `uid` 至多计 `1`）。
6. **`tx.before` 与 `tx.after`（`StateSnapshot`，只读属性）**：
   - 分别为进入事务前的基准只读快照与当前暂存工作图的实时只读投影（含待补写遗留 `running` `Attempt`），与 `StateSnapshot.open(..., mode="copy")` 100% 同形，退出 `with` 块后仍永久可读且不持有底层连接。
7. **`tx.diff() -> str`**：
   - 返回面向人类阅读的紧凑对比文本（包含 `Before -> After` 四态计数、`[- REMOVED]` 且置顶高亮被删 `[succeeded]`、`[⇄ REPLACED]` 仅展示变化字段、`[+ ADDED]`、`[↻ STATUS]`、`[WARNINGS]` 与 `[VALIDATION]`），不承诺跨版本格式解析稳定；
   - **永不因校验失败抛异常中断渲染**：即便存在多处校验错误，仍完整返回对比文本与全部错误明细；
   - **4 类纯只读非阻断 Warning**（同时同步输出 `logger.warning`）：
     1. 移除 `[succeeded]` 节点；
     2. 单分区 `remove` 后同 `uid` 仍在另一分区保留；
     3. `pending` 节点同时存在历史 `successes`/`failed` 档案（下次 `run()` 将由 `Trigger` 依残留档案判定）；
     4. 非 `succeeded` 上游的 `from_output(p, key)` 未出现在静态 `job_snapshot["files"]` 中（依赖运行期 `ctx.declare_file` 动态声明）。

##### 非执行 `"succeeded"` 写入边界与 `__exit__` 全量攒错闭环校验

- **非执行 `"succeeded"` 边界**：凡经 `tx.add` 或 `tx.replace` 置为 `"succeeded"` 时，零文件删除副作用（忽略 `FileSpec` 删除标志、不要求路径授权、不检查输入磁盘存在性）；不伪造 `Attempt`；在写入工作图与验盘时即于外层 `files` 与 `artifacts` 双写所有保留型 `role="output"`（`delete_on_success=False`）的规范绝对路径（确保 `with` 块内外 `tx.after` 与落盘重开快照 100% 同形，等价路径不产生 `tx.diff()` 假阳性）；清除旧执行时间基线并在 `annotations` 写入 `{"completion_source": "manual"}`（附可选 `"note"`），不产生 `trigger="on_input_change"` 的可信执行时间基线。
- **提交期 4 项硬 Error 单次全量攒错校验**（`with` 块无异常退出时一次性收集全部违规项，不遇错即停）：
  1. **存活节点不受损（防删出新孤儿）**：基准与最终工作图中共同存活的节点，其原已存在且仍留存于 `depends_on` 中的上游不得消失；
  2. **活跃/成功/触碰节点零孤儿与全图无环**：所有 `pending`、`succeeded` 及本次 `add`/`replace` 触碰的节点，其 `depends_on` 全部上游必须存在于最终工作图中，且 `depends_on` 有向图严禁成环（历史未触碰的 `failed` 死记录不连坐阻断无关分支）；
  3. **`from_output` 契约闭合**：上述节点声明的每个 `FileSpec.from_output(p, key)` 必须满足 `p in depends_on` 且 `p` 存在；`p` 为 `succeeded` 时 `key` 必须存在于 `p` 已发布的外层 `files` / `artifacts` 中且为保留型 `role="output"`；`p` 非 `succeeded` 且其静态 `files` 已声明 `key` 时，强制校验其为保留型 `role="output"`；
  4. **物理产出真实在场**：本次 `add`/`replace` 置为 `"succeeded"` 的所有节点，其声明的保留型 `role="output"` 路径必须全部通过统一 `PathPolicy` 严格检查（受保护 5 路径、根路径与符号链接/Junction 拦截）并在磁盘真实存在。
- **退出结算规则**：存在任何校验错误时，无论 `dry_run` 取何值，一律回滚丢弃暂存修改并将**全部错误汇总抛出单一 `TaskLiteError`**（非预期底层 OS / `sqlite3` 异常原样传播）；全部通过时，`dry_run=True` 零写入退出，`dry_run=False` 经单个 `BEGIN IMMEDIATE` SQLite 写事务（或内存写锁 Copy-on-Write）原子落盘。

#### 2. `get_cursor(key: str) -> str | None` 与 `set_cursor(key: str, value: str | None) -> None`
- `get_cursor`：读取持久化游标表中最新已提交的字符串值，键不存在返回 `None`；`key` 必须为非空字符串；
- `set_cursor`：在状态库单写者互斥下直接以原子事务持久化写入游标键值。`value` 为 `None` 表示从表中删除该游标键；`value` 必须为 `str` 或 `None`。

#### 3. 路径授权管理：`grant_authorization`、`list_authorizations` 与 `revoke_authorization`

- **`grant_authorization(path: str | Path, *, target_type: Literal["file", "directory"]) -> dict[str, Any]`**：
  - Run 外一次性持久化预授权入口，在状态库单写者互斥下执行；
  - 相对路径基于 `TaskLite` 构造期固定的 `base_dir`，经统一 `PathPolicy`（仅展开最长已存在父前缀、绝不跟随叶节点链接、统一平台盘符/长名）解析为规范绝对路径；
  - `target_type` 为必填关键字参数（仅允许 `"file"` 或 `"directory"`）；**不要求目标路径在磁盘上已存在**，也不创建任何文件或目录；
  - **受保护路径硬拦截**：自动保护基于 `state_path` 派生的全部 5 项内部路径（`state_path`、`-wal`、`-shm`、`-journal`、`<state_path>.lock`）以及文件系统根 / 盘符根 / UNC 根，按路径段元组双向重叠拦截，命中一律抛 `TaskLiteError`；
  - **无 `exist_ok`，严格冲突拦截**：
    - 若库中已存在**相同规范绝对路径**（或 Windows 下经 `os.path.normcase` 判定的大小写等价路径），无论 `target_type` 是否相同，均直接抛 `TaskLiteError`（不静默幂等、不刷新时间戳）；
    - 若存在父子类型矛盾（如祖先已按 `"file"` 授权又授权子路径，或已存在子路径授权又将祖先按 `"file"` 授权），一律抛 `TaskLiteError`；
    - 父子目录授权各自按精确规范绝对路径独立存取与撤销，互不隐式合并或覆盖；运行期交互式授权升级（`record_authorization` 的 `file -> directory` 覆盖）与运维期 `grant_authorization` 严格分离；
  - **返回值**：返回与 `list_authorizations()` 单项同构的字典 `{"path": str, "target_type": str, "authorized_at": str}`。
- **`list_authorizations() -> list[dict[str, Any]]`**：只读返回所有持久化在库中的主进程路径授权记录列表，按 `authorized_at ASC, path ASC` 排序。字典字段为：
  ```python
  {
      "path": str,           # 授权的规范绝对路径
      "target_type": str,    # 路径类型："file" 或 "directory"
      "authorized_at": str,  # 授权时间（ISO 8601 UTC 字符串）
  }
  ```
- **`revoke_authorization(path: str | Path) -> bool`**：按统一 `PathPolicy` 规范化绝对路径精确撤销单条持久化授权。删除成功返回 `True`，未找到返回 `False`。

---

## 3. 核心模型与运行时上下文

### 3.1 Task 静态规格

位于 [`tasklite/models/task.py`](../tasklite/models/task.py)，是进程内注册的不可变静态规格模板（代码即规格，不落盘）。

```text
Task(
    task_type: str,
    handler: Callable[[Job, JobContext], TaskResult],
    *,
    resources: dict[str, float] | None = None,
    max_retries_per_run: int = 3,
    timeout_seconds: int | float = 3600,
    trigger: str = "once",
)
```

- **不变性**：`@dataclass(frozen=True)`，实例化后禁止属性就地变异。
- **参数校验规则（校验失败均抛出 `TaskLiteError`）**：
  - `task_type`：非空 `str` 且必须为合法的 UTF-8 字符串；
  - `handler`：必须为可调用对象（`callable(handler)` 必须为 `True`）；
  - `resources`：必须为 `None` 或字典。键必须为非空 `str`，值必须为非 `bool` 的有限数值且 `>= 0`；实例化后内部进行浅拷贝隔离外部字典；
  - `max_retries_per_run`：单次 Run 内消耗预算重试的上限次数。必须为非 `bool` 的 `int` 且 `>= 0`；
  - `timeout_seconds`：执行超时秒数。必须为非 `bool` 的有限数值（`int` 或 `float`）且 `> 0`；
  - `trigger`：触发器名称字符串，必须为非空 `str`。

---

### 3.2 Job 逻辑实例

位于 [`tasklite/models/job.py`](../tasklite/models/job.py)，是具有稳定持久化身份的逻辑作业实例。

```text
Job(
    task_type: str,
    job_id: str,
    *,
    payload: dict[str, Any] | None = None,
    depends_on: Sequence[str] | None = None,
    files: dict[str, FileSpec] | None = None,
    resources: dict[str, float] | None = None,
    max_retries_per_run: int | None = None,
    timeout_seconds: int | float | None = None,
    trigger: str | None = None,
)
```

- **身份唯一性**：作业的稳定唯一身份为 `job.uid = make_job_uid(task_type, job_id)`。
- **等价性与哈希**：`Job.__eq__` 与 `Job.__hash__` 仅取决于 `uid`。
- **构造限制与执行编号 `attempt_no`（只读属性）**：
  - `Job(...)` 公开构造函数**不接受** `attempt_no` 参数（若传入直接抛出 `TypeError`）；
  - `job.attempt_no`（`int`，`@property` 只读属性）：仅为运行时只读观察属性，新建作业实例时内部默认初始化为 `1`；实际物理执行编号由调度引擎在原子进入 `running` 事务中分配并跨 Run 单调自增；
  - **静态有效配置与运行时状态彻底解耦**：调用 `job.to_dict()` 导出的快照字典**不含 `attempt_no`**（仅捕获作业静态配置字段：`task_type`、`job_id`、`payload`、`depends_on`、`files`、`resources`、`max_retries_per_run`、`timeout_seconds`、`trigger`）。在状态快照 `StateSnapshot.get_job()` 返回的标准化字典中，内层有效配置字典 `record["job"]` 亦不包含 `attempt_no`（`attempt_no` 仅作为运行时物理执行状态字段置于外层记录 `record["attempt_no"]`）。
- **参数继承规则（摄入时生效）**：
  - 若 `resources`、`max_retries_per_run`、`timeout_seconds`、`trigger` 传入 `None`，在管线摄入（`enqueue` / `spawn`）时自动继承对应 `Task` 规格定义的默认值；
  - `resources` 若显式指定字典，将与 `Task.resources` 按键合并，显式同名键覆盖默认值；
  - `payload`：经过严格 JSON 对象校验后深拷贝存储，为 `None` 时归一为 `{}`；
  - `files`：仅接受 `dict[str, FileSpec]` 或 `None`（归一为 `{}`），字典键名不可含空字节。
- **序列化与反序列化**：
  - `job.to_dict() -> dict[str, Any]`：导出作业静态配置快照字典（不含 `attempt_no`）；
  - `Job.from_dict(data: dict[str, Any]) -> Job`：从快照还原 `Job`（支持缺键回默认值，`attempt_no` 默认初始化为 `1`；若快照中存在 `attempt_no` 亦可兼容恢复）。若快照中包含旧废弃字段（`rerun`、`timeout`、`max_retries`、`timeout_is_transient`），主动抛出 `TaskLiteError` 拒绝解析。

---

### 3.3 JobContext 运行时上下文

位于 [`tasklite/models/context.py`](../tasklite/models/context.py)，传递给 `Task.handler` 的执行上下文环境（每次物理执行构造独立实例）。

#### 公开属性
- `ctx.job`（`Job`）：当前正在执行的作业逻辑实例；

> [!NOTE]
> **内部状态彻底私有化**：`JobContext` 仅暴露 `ctx.job` 及受控交互方法（`spawn`, `declare_file`, `file_path`, `get_cursor`, `set_cursor`）。内部路径基准以及衍生作业、游标变更的暂存容器已彻底私有化，不向外部暴露任何内部配置或累积容器属性。所有衍生作业与游标变更必须经由 `ctx.spawn()` 与 `ctx.set_cursor()` 受控登记，并在作业成功结算时由主进程原子提交（单元测试观测可通过 [`tasklite.testing`](../tasklite/testing.py) 提供的 `spawned_jobs` 与 `cursor_updates` 只读辅助完成）。

#### 公开方法
1. **`file_path(name: str) -> Path`**：
   - 返回当前作业中已声明文件（预声明或已完成动态声明）的规范绝对 `Path`；
   - 若名称未在当前作业声明中找到，抛出 `TaskLiteError`。
2. **`declare_file(name: str, spec: FileSpec) -> Path`**：
   - 在执行期间动态声明文件需求，向主进程发起授权与清理准备，成功后返回规范绝对 `Path`；
   - **同名声明幂等与冲突判定**：
     - 若同名声明已存在且配置完全一致：直接幂等复用，返回先前已绑定的路径；
     - 若同名声明已存在但配置不一致：立即抛出 `TaskLiteError`；
   - **授权等待与超时暂停机制**：
     - 当通过 IPC 向主进程请求授权或清理时，主进程看门狗计时器**自动暂停倒计时**；
     - 授权处理完毕并回复子进程后，看门狗计时器**恢复剩余时长并顺延截止时间**，防止等待主进程人工授权导致作业被超时强杀；
     - 若主进程拒绝授权（denied），子进程抛出 `TaskLiteError("authorization denied")`。
3. **`spawn(job: Job) -> None`**：
   - 动态衍生子作业。子作业依赖必须受限闭合于当前已知作业与父作业；
   - **原子提交语义**：衍生作业仅在当前父作业执行成功（`success`）并在主进程事务提交时**一同原子持久化入队**；若父作业最终重试或失败，本次衍生出的所有子作业**全部丢弃**。
4. **`get_cursor(key: str) -> str | None`**：
   - 读取游标值（优先读取当前作业尚未提交的自身暂存修改，未暂存时读取派发时刻的快照值）。
5. **`set_cursor(key: str, value: str | None) -> None`**：
   - 暂存游标变更。`value` 为 `None` 表示标记删除；
   - **原子提交语义**：游标变更仅在当前作业执行成功结算时随写事务原子提交；若作业失败或重试，暂存的游标更新**直接丢弃**。

---

### 3.4 TaskResult 执行结果值对象

位于 [`tasklite/models/result.py`](../tasklite/models/result.py)，是任务 handler 唯一的合法显式返回值类型。

#### 构造约定（禁止直接实例化）
直接调用 `TaskResult(...)` 会抛出 `TaskLiteError`。必须且只能通过以下三个仅关键字静态构造器创建：

```text
TaskResult.success(*, message: str | None = None, metadata: dict[str, Any] | None = None) -> TaskResult
TaskResult.failure(*, message: str | None = None, metadata: dict[str, Any] | None = None) -> TaskResult
TaskResult.retry(*, message: str | None = None, metadata: dict[str, Any] | None = None, consume_budget: bool = True) -> TaskResult
```

#### 属性与语义
- **`status`**（`Literal["success", "failure", "retry"]`）：结果终态；
- **`message`**（`str | None`）：人类可读的执行结果摘要说明；
- **`metadata`**（`dict[str, Any]`）：业务返回的元数据字典深拷贝副本（经严格 JSON 校验，不含框架保留键）；
- **`consume_budget`**（`bool`）：本次重试是否扣减重试预算。仅在 `TaskResult.retry` 中生效（默认为 `True`），在成功与永久失败中恒为 `False`。
- **不可变性**：对 `TaskResult` 实例属性的赋值或删除均会抛出 `AttributeError`。

#### 异常与收敛规则
- **重试预算耗尽**：若 `return TaskResult.retry(consume_budget=True)`，但当前 Run 的剩余重试预算已为 0，框架自动将其转换为 `failure`，记入失败档案；
- **子进程异常崩溃**：未捕获异常、段错误崩溃（SIGSEGV）或超时强杀（SIGKILL），框架自动收敛为 `failure`，生成携带异常类型与 traceback 的失败记录，不进行隐式重试。

---

### 3.5 FileSpec 统一文件声明与清理授权

位于 [`tasklite/models/artifact.py`](../tasklite/models/artifact.py)，用于规范作业所需的物理文件、清理策略及上游产物引用。

#### 构造签名

1. **直接文件声明**：
   ```text
   FileSpec(
       path: str | Path,
       *,
       role: Literal["input", "output", "cache"],
       delete_existing: bool = False,
       delete_on_failure: bool = False,
       delete_on_success: bool = False,
   )
   ```
2. **上游产物引用声明**：
   ```text
   FileSpec.from_output(
       producer_uid: str,
       output_name: str,
       *,
       role: Literal["input", "output", "cache"],
       delete_existing: bool = False,
       delete_on_failure: bool = False,
       delete_on_success: bool = False,
   )
   ```

#### 公开属性与序列化
- `spec.path`（`str | None`，只读）：词法规范化路径；上游产物引用在尚未绑定前为 `None`；
- `spec.producer_uid` / `spec.output_name`（`str | None`，只读）：上游引用信息；
- `spec.role`（`"input" | "output" | "cache"`，只读）：文件角色；
- `spec.delete_existing` / `spec.delete_on_failure` / `spec.delete_on_success`（`bool`，只读）：三项显式删除清理开关；
- `spec.is_output_ref`（`bool`，只读）：是否为上游产物引用；
- `spec.to_dict() -> dict[str, Any]` 与 `FileSpec.from_dict(data) -> FileSpec`：严格 JSON 序列化与还原（未知键抛出 `TaskLiteError`）。

#### 校验与清理契约
- **角色与删除组合校验**：
  - 当 `role="input"` 时，**严禁设置 `delete_existing=True`**，违规抛出 `TaskLiteError("input FileSpec cannot set delete_existing=True")`；
- **上游直接依赖约束**：
  - `from_output` 引用的 `producer_uid`，**必须出现在当前 Job 的显式 `depends_on` 列表中**，否则在解析与动态声明时直接抛出 `TaskLiteError`；
- **受保护路径硬拦截**：状态库自身及自动派生的全部 5 项内部路径（`state_path`、`-wal`、`-shm`、`-journal`、`<state_path>.lock`）与文件系统根 / 盘符根 / UNC 根均属于受保护路径，任何删除或写授权请求一律按路径段元组双向重叠硬拦截拒绝；
- **平台支持边界**：主进程物理文件/目录安全清理（[`tasklite/utils/artifact.py`](../tasklite/utils/artifact.py) 的 `PathPolicy`）已支持 **Linux** 与 **Windows**：
  - **Linux（POSIX）**：依赖 `dir_fd` / `O_NOFOLLOW` / `O_DIRECTORY`、`(st_dev, st_ino)` 身份校验及 `/proc/self/mountinfo` 挂载边界校验；
  - **Windows**：采用无特权 `kernel32`（`CreateFileW` 省略 `FILE_SHARE_DELETE` 内核级钉锁父目录 + `GetFileInformationByHandle` 身份复核 + `CloseHandle`）与 `os.lstat` 原生 `st_file_attributes` / `st_reparse_tag` 重解析点分类（放行已知非名称代理数据重解析点如 OneDrive Cloud / Dedup / WOF，精准区分 symlink）；
  - **Windows 物理限制**：NTFS Junction / 卷挂载点与未知重解析点一律 fail-closed 拒绝；不隐式清除只读位、不掩盖文件占用错误（`WinError 32` / `WinError 5` 原样向上传播）；保留与 POSIX `fstatat -> unlinkat` 同构的同目录普通叶替换微窗口（因 `unlink` / `rmdir` 不跟随链接且 `rmdir` 拒删非空目录，绝不穿透目录外）；
- **成功结算产物检查与时序**：
  - 任务返回 `success` 后，主进程核验所有声明为 `role="output"` 的产物：
    - 若物理文件不存在，且 `delete_on_success=False`，主进程将结果改判为失败并报错 `Missing output <path>`；
    - **成功即删语义**：若 `delete_on_success=True`，契约明确允许工作子进程在完成处理后原子 `rename` 移走该临时产物，主进程检测到物理文件缺失时视为已合法移除，不判失败，且仍向成功档案发布产物描述记录；
    - 验证通过后，主进程按授权执行标记为 `delete_on_success=True` 的残留文件清理。

---

## 4. 只读一致状态快照 StateSnapshot

位于 [`tasklite/snapshot.py`](../tasklite/snapshot.py)，是 TaskLite 权威的只读一致状态观察入口。

### 4.1 快照建立与会话生命周期

```text
StateSnapshot.open(
    source: str | Path | TaskLite,
    *,
    mode: Literal["lazy", "copy"] = "lazy",
) -> StateSnapshot
```

- **模式选择（`mode`）**：
  - `mode="lazy"`（默认）：SQLite 源持有只读事务流式按需读取，关闭快照时释放连接；内存源捕获一致版本副本；
  - `mode="copy"`：SQLite 源在单一只读事务内读取一致快照后**立即关闭底层数据库连接**（零长期句柄占用，适合长耗时 `run()` 前后拍照对比；内部对大字段 JSON 保留原始字符串按需惰性反序列化）；内存源对只追加不可变的 `attempts` 列表采用切片浅共享以控制内存峰值；
  - 非法 `mode`（非 `"lazy"` / `"copy"`）抛出 `TaskLiteError`。
- **数据源语义**：
  - **传入数据库路径（`str | Path`）**：以只读 URI 模式（`?mode=ro`）建立连接并启动只读事务。若数据库不存在、为目录或 Schema 未就绪，直接抛出 `TaskLiteError`，**绝不新建空库文件**；在只读事务内保证跨表查询的一致性快照；
  - **传入 `TaskLite` 实例**：
    - 若实例已关闭，抛出 `TaskLiteError`；
    - SQLite 后端按所选 `mode` 连接其状态库打开只读快照；
    - 内存后端（`memory`）在实例锁保护下建立一致版本快照，后续查询与内存变动隔离；
    - **支持在管线运行中或空闲态安全打开**。
- **公开属性、生命周期与释放**：
  - **`source`**（`Any`，`@property` 只读属性）：打开该快照时传入的原始源对象或数据库路径（如 `str`、`Path` 或 `TaskLite` 实例）；
  - **`closed` / `is_closed`**（`bool`，`@property` 只读属性）：检查快照是否已关闭；关闭后调用任何查询方法均抛出 `TaskLiteError("StateSnapshot is closed")`；
  - 支持上下文管理器协议：`with StateSnapshot.open(...) as snapshot:`；
  - **`close() -> None`**：释放只读连接与内存副本；**关闭快照绝不会关闭底层的 `TaskLite` 实例**；`mode="copy"` 同样兼容 `with` 与 `close()`，显式 `close()` 后继续访问仍抛 `TaskLiteError`。

---

### 4.2 get_job 查询与标准化 Job 字典

```text
snapshot.get_job(uid: str) -> dict[str, Any] | None
```
- `uid` 必须为非空字符串。若不符合规范单射编码格式，返回 `None`；
- 查询顺序：`queue` 待办队列 -> `successes` 成功档案 -> `failed` 失败档案；不存在返回 `None`。

#### 标准化 Job 字典完整字段清单

```python
{
    "job_uid": str,            # 作业规范 UID，如 "etl::chunk_1"
    "status": str,             # 状态："pending" | "running" | "succeeded" | "failed"
    "job": {                   # 有效配置投影快照（已剥离 attempt_no 等运行时账目字段）
        "task_type": str,
        "job_id": str,
        "payload": dict,       # 业务输入载荷字典
        "depends_on": list,    # 上游依赖 UID 列表
        "files": dict,         # 提交期解析的规范文件字典
        "resources": dict,     # 包含 __workers__ 在内的资源字典
        "max_retries_per_run": int,
        "timeout_seconds": int | float,
        "trigger": str,
    },
    "files": dict,             # 已解析外层产物绝对路径字典，结构见下
    "result": dict | None,     # 结算结果，仅 succeeded 与 failed 存在，未结算为 None
    "run_id": str | None,      # 运行会话标识（pending 态恒为 None）
    "attempt_no": int | None,  # 执行编号（pending 态恒为 None）
    "started_at": str | None,  # UTC ISO 8601 worker 实际启动时间戳（以 'Z' 结尾；running 态准确投影已落盘启动时间，pending 态恒为 None）
    "settled_at": str | None,  # UTC ISO 8601 结算时间戳（以 'Z' 结尾，未结算为 None）
}
```

其中 `files` 字典的每一项结构为：
```python
{
    "path": "/abs/path/to/file",
    "role": "output",
    "delete_existing": False,
    "delete_on_failure": False,
    "delete_on_success": False
}
```

其中 `result` 字典（已结算时）的结构为：
```python
{
    "message": str | None,     # 结果摘要说明
    "exception": dict | None,  # 异常详情（无异常为 None；存在时含 type, message, traceback）
    "metadata": dict,          # 严格 JSON 业务结果元数据
    "annotations": dict,       # 框架内部标注（如 {"completion_source": "manual"}）
}
```

---

### 4.3 iter_jobs 迭代与状态过滤

```text
snapshot.iter_jobs(
    *,
    status: Literal["pending", "running", "succeeded", "failed"] | None = None,
    task_type: str | None = None,
) -> Iterator[dict[str, Any]]
```
- **参数校验**：`status` 必须为上述四个合法状态之一或 `None`；`task_type` 必须为 `str` 或 `None`；非法值抛出 `TaskLiteError`；
- **排序与一致性**：`status=None` 时包含全部四态作业；队列内部按 `queue_position ASC` 产生；全局不作跨表全序排序承诺；
- 返回元素与 `get_job` 输出的标准化 Job 字典形状完全一致。

---

### 4.4 iter_attempts 迭代与物理执行轨迹字典

```text
snapshot.iter_attempts(
    *,
    job_uid: str | None = None,
    task_type: str | None = None,
) -> Iterator[dict[str, Any]]
```
- **参数校验**：`job_uid` 与 `task_type` 为可选的 `str`；非法类型抛出 `TaskLiteError`；
- **排序**：当指定 `job_uid` 时，结果严格按照 `attempt_no ASC` 升序产生。

#### 标准化 Attempt 字典完整字段清单

```python
{
    "job_uid": str,            # 作业规范 UID
    "outcome": str,            # 物理尝试结果："success" | "failure" | "retry"
    "job": dict,               # 本次执行所依据的有效配置快照
    "files": dict,             # 本次执行时绑定的外层文件绝对路径映射
    "result": dict,            # 本次执行产生的标准化结果字典（含 message, exception, metadata, annotations）
    "run_id": str,             # 产生该轨迹的 Run 会话 ID
    "attempt_no": int,         # 物理执行代次编号（1 起始自增）
    "started_at": str | None,  # UTC ISO 8601 格式启动时间戳（以 'Z' 结尾）
    "settled_at": str | None,  # UTC ISO 8601 格式结算时间戳（以 'Z' 结尾）
}
```

---

### 4.5 StateSnapshot.report 状态报告与前后对比

位于 [`tasklite/_report.py`](../tasklite/_report.py)，经 `StateSnapshot.report` 静态方法公开。

```text
StateSnapshot.report(
    first: StateSnapshot,
    second: StateSnapshot | None = None,
) -> str
```

- **入参校验**：`first` 必须为未关闭的 `StateSnapshot` 实例；`second` 为 `None` 或未关闭的 `StateSnapshot` 实例；传入非 `StateSnapshot` 或已关闭快照均抛出 `TaskLiteError`。
- **单快照模式（`second=None`）**：
  - 输出四态计数总览（含 `succeeded` 中 `execution` / `manual` 构成）；
  - 按 `task_type` 分组展示完成进度及历史成功 `Attempt` 耗时统计（`Avg` / `P50` / `Max`）；
  - 展示当前在途 `running` 作业已运行秒数与基于历史成功基线的预估剩余时间（ETA）；
  - 展示当前 `failed` 作业摘要。
- **双快照对比模式（`first=before, second=after`）**：
  - 以 `second`（`after`）为主干结构，叠加 `first -> second` 区间四态计数增减、状态流转汇总、新增 `Attempt` 次数/耗时统计以及本次区间新增失败明细。
- **非编程解析契约**：返回的多行 `str` 仅供人类阅读展示，不承诺文本排版或字段格式跨版本稳定；程序化消费状态数据请使用 `get_job` / `iter_jobs` / `iter_attempts`。

---

## 5. 单射编码工具与公开异常

位于 [`tasklite/utils/encoding.py`](../tasklite/utils/encoding.py) 与 [`tasklite/exceptions.py`](../tasklite/exceptions.py)。

### 5.1 规范 UID 单射编解码（make_job_uid, parse_job_uid）

TaskLite 核心红线禁止丢弃字符式的模糊化处理，所有标识符派生使用**可逆单射编码**。

```text
make_job_uid(task_type: str, job_id: str) -> str
parse_job_uid(uid: str) -> tuple[str, str]
```

- **编码规则**：
  - 对 `task_type` 与 `job_id`，将 `%`、`:` 以及不可打印字符转义为大写 `%XX` UTF-8 字节十六进制序列；其余字符原样保留；
  - 以双冒号 `"::"` 拼接两部分：`"{encoded_task_type}::{encoded_job_id}"`；
  - 非空校验：`task_type` 或 `job_id` 为空或非 `str` 时抛出 `TaskLiteError`。
- **解码规则与异常校验**：
  - 输入必须为 `str` 且必须包含**唯一的一个** `"::"` 分隔符，否则抛出 `TaskLiteError`；
  - 解码时严格检查 `%` 转义语法：不完整的转义符、非十六进制字符、非法 UTF-8 字节序列、未转义的保留字符以及**过度转义（over-escaped）的普通可打印字符**均会直接抛出 `TaskLiteError`；
  - 成功解码返回 `(task_type, job_id)` 原始字符串二元组。

---

### 5.2 公开异常 TaskLiteError

```python
class TaskLiteError(Exception):
    """框架主动发现调用条件不成立时抛出的公开异常。"""
```
- 直接继承自 Python 标准库 `Exception`；
- 是 TaskLite 唯一对外公开承诺的异常类型；
- 所有调用参数违规、非法状态转移、Schema 6 版本阻断、触发器与资源重名均抛出此异常。

---

## 6. 内置资源与运行排除

### 6.1 CapacityResource 并发容量资源

从 `tasklite.resource` 导入，用于管理最大并发上限（如并发连接数、显存容量）：

```text
CapacityResource(name: str, *, max_capacity: float)
```

- **参数约束**：
  - `name`（`str`）：非空资源名称；
  - `max_capacity`（`float`）：非 `bool` 的有限数值且 `>= 0`，非法值抛出 `TaskLiteError`。
- **调度语义与准入规则**：
  - 调度引擎在派发作业前检查当前可用容量；
  - **明确不可满足**：若作业声明的请求用量 `amount > max_capacity`，调度引擎判定为明确不可满足，直接将该作业收敛为失败并级联下游；
  - **容量可用**：若 `当前占用 + amount <= max_capacity`，准予获取容量并启动作业；
  - **暂不可用等待**：若当前可用容量不足，作业保持 `pending` 留队，等待在途任务执行完成释放容量后自动唤醒调度循环重新评估；
  - 任务执行完毕完成结算后，占用的容量由主进程自动归还。
- **内部协议说明**：
  - 底层资源查询与获取协议（`can_acquire`, `acquire`, `release`）由调度引擎内部在主进程调度线程串行调用，**不作为对外公开接口**；用户仅需通过 `pipeline.register_resource()` 注册并在 Task/Job 的 `resources` 中声明用量。

---

### 6.2 IntervalResource 时间间隔资源

从 `tasklite.resource` 导入，用于控制全局请求调用频率（速率限制）：

```text
IntervalResource(name: str, *, interval_seconds: float)
```

- **参数约束**：
  - `name`（`str`）：非空资源名称；
  - `interval_seconds`（`float`，**仅关键字参数**）：有限正数（`> 0`），非法值抛出 `TaskLiteError`。
- **调度语义与准入规则**：
  - 基于 `time.monotonic()` 跟踪下一次允许获取的时间基准；
  - 首次请求可立即获准派发；
  - 每次获准后，基于单调时间按 `interval_seconds * amount` 顺延下次允许获取的时间点；
  - 若当前单调时间尚未达到下一次可用时间点，作业保持 `pending` 留队等待计时器到期唤醒，主循环在此期间可继续处理其他无冲突事件；
  - **配额不可撤销**：时间配额为纯单向推进机制，任务执行完成不返还时间配额。
- **内部协议说明**：
  - 底层间隔查询与配额推进由调度引擎内部串行调度，**不作为对外公开接口**。

---

### 6.3 Exclude 运行排除规则

从 `tasklite.selection` 导入，用于单次 `run()` 中排除指定任务类型的派发：

```text
Exclude(*, task_type: str)
```

- **参数与属性**：
  - `task_type`（`str`，**仅关键字参数**）：待排除的任务类型名称。必须为合法的非空 UTF-8 字符串，否则抛出 `TaskLiteError`。
  - `matches_task_type(task_type: str) -> bool`：精确全字匹配。
- **调度行为**：
  - 通过 `pipeline.run(exclude=Exclude(task_type="..."))` 传入；
  - 命中的作业及其受影响的下游作业**本次 Run 不派发、不执行、不消耗重试预算与资源，原样保留在待办队列中**；
  - 下次未指定排除该类型的 `run()` 启动时，留队作业重新参与正常的候选判断。

---

## 7. 极简运维命令行入口

位于 [`tasklite/__main__.py`](../tasklite/__main__.py)，通过 `python -m tasklite` 调用。严格定位于**只读观测与纯 `uid` 驱动的轻量状态运维**，不引入任何引擎调度语义，不提供需构造 `Job` 的 `add` / `replace`。

### 7.1 子命令语法与行为契约

```bash
# 1. 只读状态报告（薄封装 StateSnapshot.open + StateSnapshot.report）
python -m tasklite status <state_path>

# 2. 纯状态流转（底层委托 TaskLite.edit().set_status）
python -m tasklite set-status <state_path> <uid> <pending|failed> (--recursive | --no-recursive) [--note TEXT] [--apply]

# 3. 移除状态条目（底层委托 TaskLite.edit().remove）
python -m tasklite remove <state_path> <uid> [--entries queue,successes,failures] (--recursive | --no-recursive) [--note TEXT] [--apply]
```

- **`status <state_path>`**：以只读模式打开 SQLite 状态库并输出 `StateSnapshot.report(snap)`；绝不创建或修改数据库文件。
- **`set-status <state_path> <uid> <pending|failed>`**：仅允许 `pending` 或 `failed`；`--recursive` 与 `--no-recursive` 为**必填互斥参数**（无默认值）；底层 100% 委托 `TaskLite.edit(dry_run=not apply, note=note)` 与 `tx.set_status(...)`。
- **`remove <state_path> <uid>`**：`--entries` 为逗号分隔的目标分区列表（默认 `queue,successes,failures`，仅允许该三项，传入 `attempts` 等非法值报错）；`--recursive` 与 `--no-recursive` 为**必填互斥参数**；底层 100% 委托 `TaskLite.edit(dry_run=not apply, note=note)` 与 `tx.remove(...)`，永不删除 `attempts` 或磁盘文件，永不重置 `job_sequences`。

### 7.2 安全守卫、退出码与非目标

- **强制 `<state_path>` 与防建空库**：所有子命令必须显式传入 `<state_path>`（无默认路径）；`set-status` 与 `remove` 前置校验路径必须已存在且非目录，绝不因拼错路径静默新建空库文件。
- **默认 dry-run 与显式 `--apply`**：修改类命令默认以 `dry_run=True` 运行并向 `stdout` 打印 `tx.diff()` 及未落盘提示；仅当显式传入 `--apply` 且闭环校验通过时才原子提交落盘。
- **无 `--json` 选项**：CLI 输出面向人类终端阅读，不提供 `--json` 序列化协议（程序化读取请直接调用 Python `StateSnapshot` API）。
- **标准进程退出码**：
  - `0`：执行成功（包括 dry-run 校验通过并打印预览）；
  - `1`：业务校验失败、状态库不存在/版本不匹配、或捕获 `TaskLiteError` / `OSError` / `sqlite3.Error`（错误信息输出至 `stderr`）；
  - `2`：命令行参数缺失、互斥冲突或非法选项（`argparse` 标准退出码）。

---

## 8. 环境变量

### 8.1 TASKLITE_DANGEROUS_AUTO_APPROVE 危险自动授权开关

- **精确匹配条件**：
  - 框架仅以 `os.environ.get("TASKLITE_DANGEROUS_AUTO_APPROVE") == "1"` 做精确判定；
  - 只有当环境变量值**严格等于字符串 `"1"`** 时生效（诸如 `"true"`、`"yes"`、`"True"` 均不生效）。
- **持久化授权效果**：
  - 当命中 `"1"` 时，主进程遇到未授权的文件清理或准备操作时自动批准，并**将该授权永久写入状态库的 `authorizations` 表**中；后续执行直接命中库中授权。
- **受保护路径硬拦截边界**：
  - 即使设置了 `TASKLITE_DANGEROUS_AUTO_APPROVE="1"`，若目标路径属于状态库自身、锁文件或引擎受保护目录，框架底层安全防护机制依然**硬拦截拒绝**，杜绝误删状态库风险。

---

## 9. 可选扩展与测试辅助模块

> [!NOTE]
> 本章节模块属于可选辅助工具，本版不作稳定兼容承诺。

### 9.1 tasklite.testing 单元测试工具

位于 [`tasklite/testing.py`](../tasklite/testing.py)，为业务 handler 与引擎集成单测提供官方的隔离构造与受控接缝句柄。

```python
from tasklite.testing import (
    authorizer,
    cursor_updates,
    fake_ctx,
    in_process_workers,
    running,
    spawned_jobs,
)
```

```text
fake_ctx(
    job: Job,
    *,
    successes: Iterable[str] | None = None,
    artifacts: dict[str, dict[str, dict[str, str]]] | None = None,
    protected_roots: tuple[str | Path, ...] = (),
    failed: Iterable[str] | None = None,
    cursors: dict[str, str] | None = None,
    tmp_root: Any | None = None,
    base_dir: str | Path | None = None,
    known_job_uids: Iterable[str] | None = None,
    files: Mapping[str, Any] | None = None,
) -> JobContext
```
- **核心职责**：构建真实的 `JobContext` 实例。严格遵循受保护路径（`protected_roots`）及上游依赖闭合校验（`from_output` 要求 `producer_uid` 位于 `job.depends_on` 中，缺失 `input` 文件与真实主进程一致抛出 `FileNotFoundError`）；
- **参数说明**：
  - `tmp_root`：提供时自动在其下创建临时目录作为相对路径解析基准（`base_dir`）；
  - `successes` / `failed`：模拟历史完成与失败作业集合；
  - `artifacts`：模拟上游作业发布的具名产物描述（用于测试 `from_output` 依赖解析）；
  - `protected_roots`：不可声明为产物或请求删除的框架受保护路径；
  - `cursors`：注入初始游标字典。

```text
spawned_jobs(ctx: JobContext) -> list[Job]
cursor_updates(ctx: JobContext) -> dict[str, str | None]
```
- **核心职责**：只读观测辅助工具，分别返回 `ctx.spawn()` 登记的子作业列表浅拷贝与 `ctx.set_cursor()` 暂存的游标更新字典浅拷贝，消除测试代码对 `JobContext` 内部容器的直接访问。

```text
@contextmanager
def running(target: Any) -> Iterator[Any]
```
- **核心职责**：测试专用上下文管理器。将 `TaskLite` / `EngineRuntime` 实例临时置为运行中状态，用于测试各种管理 API 在运行期的状态守卫，退出上下文时自动复位。

```text
@contextmanager
def authorizer(
    target: Any,
    fn: Callable[[str, str], bool | None] | None,
) -> Iterator[Any]
```
- **核心职责**：测试专用授权回调注入上下文管理器。为 `TaskLite` / `EngineRuntime` / `StateStore` 临时注入文件授权决策回调 `fn`，退出上下文时自动恢复原回调，防止跨用例污染。

```text
@contextmanager
def in_process_workers(
    target: Any,
    launcher: WorkerLauncher | None = None,
) -> Iterator[Any]
```
- **核心职责**：轻量进程替身上下文管理器。保留真实的单向 `Pipe` 通信边界，仅将子进程启动器替换为标准 `WorkerLauncher` 实例（默认使用进程内线程替身 `InProcessWorkerLauncher()`），退出上下文时自动恢复原有启动器。

---

### 9.2 tasklite.wrappers.http HTTP 工具集

位于 [`tasklite/wrappers/http.py`](../tasklite/wrappers/http.py)。

```python
from tasklite.wrappers.http import (
    HttpResponse,
    urllib_fetch,
    requests_fetch,
    SnapshotStore,
    SQLiteSnapshotStore,
    MemorySnapshotStore,
    HttpExecutor,
    parse_netscape_cookies,
    format_cookie_header,
)
```

#### 1. 不变响应值对象 `HttpResponse`

```text
HttpResponse(
    status_code: int,
    headers: Mapping[str, str],
    body: bytes | str,
    url: str = "",
)
```
- **属性**：
  - `status_code`（`int`）：HTTP 状态码；
  - `headers`（`dict[str, str]`）：响应头映射，初始化时键名自动归一化为全小写；
  - `body`（`bytes`）：原始二进制响应体；
  - `text`（`str`，属性）：按 UTF-8 解码的响应文本；
  - `url`（`str`）：请求对应的目标 URL；
  - `ok`（`bool`，属性）：`200 <= status_code < 300` 时为 `True`。
- **方法**：
  - `json() -> Any`：将响应体解析为 JSON 数据；
  - `header(name: str, default: str | None = None) -> str | None`：大小写不敏感查询指定响应头；
  - **`to_task_result() -> TaskResult`**：显式转换为任务结果值对象：
    - `2xx` / `3xx`：返回 `TaskResult.success(metadata={"status_code": ...})`；
    - `429`：返回 `TaskResult.retry(...)` 并附带解析到的 `retry_after` 秒数；
    - `>= 500`：返回 `TaskResult.retry(...)`；
    - 其余（4xx 等）：返回 `TaskResult.failure(...)`。

#### 2. Cookie 解析与格式化工具

- **`parse_netscape_cookies(file_or_content: str | Path) -> dict[str, str]`**：
  - 解析 Netscape / MozillaCookieJar 格式的 `cookies.txt` 文件路径或文本内容；
  - 自动跳过空行与 `#` 注释行，兼容 `#HttpOnly_` 前缀；同名 Cookie 后出现的覆盖先出现的；文件不存在或为空时返回空字典 `{}`。
- **`format_cookie_header(cookies: Mapping[str, str]) -> str`**：
  - 将 Cookie 字典格式化为标准请求头字符串（形如 `"k1=v1; k2=v2"`）。

#### 3. 网络抓取函数

- **`urllib_fetch(...)`**：基于 Python 标准库 `urllib` 的零外部依赖请求实现：
  ```text
  urllib_fetch(
      url: str,
      *,
      method: str = "GET",
      headers: Mapping[str, str] | None = None,
      params: Mapping[str, Any] | None = None,
      data: bytes | str | Mapping[str, Any] | None = None,
      timeout: float = 30.0,
      proxies: Mapping[str, str] | None = None,
  ) -> HttpResponse
  ```
  - `params`：查询参数字典，值为 `None` 的参数自动丢弃；
  - `data`：请求体，`dict` / `list` 自动序列化为 JSON 并补齐 `Content-Type: application/json` 头；`str` 按 UTF-8 编码为字节；
  - `proxies`：显式代理字典；访问 `localhost` / `127.0.0.1` 时默认不走环境变量代理；
  - 即使远端返回 4xx/5xx `HTTPError`，亦安全封装为 `HttpResponse` 返回而不向上抛出；底层连接与超时异常原样向上抛出。

- **`requests_fetch(...)`**：基于 `requests` 库的高级请求实现（运行环境需安装 `requests`）：
  ```text
  requests_fetch(
      url: str,
      *,
      session: Any | None = None,
      method: str = "GET",
      headers: Mapping[str, str] | None = None,
      params: Mapping[str, Any] | None = None,
      data: Any = None,
      json_data: Any = None,
      timeout: float = 30.0,
      proxies: Mapping[str, str] | None = None,
      trust_env: bool | None = None,
      **kwargs: Any,
  ) -> HttpResponse
  ```
  - 未显式传入 `session` 时，内部自动创建临时 `Session` 并在请求完成后安全关闭；
  - `json_data` 对应 `requests` 的 `json` 参数；其他关键字参数透传至底层请求。

#### 4. 快照存储器 `SQLiteSnapshotStore` 与 `MemorySnapshotStore`

- **`SQLiteSnapshotStore(db_path: str | Path)`**：
  - 基于独立 SQLite 数据库文件（WAL 模式）的 HTTP 响应快照存储；
  - `has(key: str) -> bool`：检查快照键是否存在；
  - `get(key: str) -> HttpResponse | None`：读取快照响应；
  - `put(key: str, url: str, status_code: int, headers: Mapping[str, str], body: bytes | str | Mapping[str, Any], method: str = "GET") -> None`：持久化快照；
  - `cached(fetch_fn: Callable[..., Any], ignore_statuses: Sequence[int] = (), **kwargs: Any) -> Callable[..., HttpResponse]`：包装请求函数，自动生成单射缓存键。瞬态状态码（408, 425, 429 与 5xx）及 `ignore_statuses` 指定的状态码绝对不写入快照；
  - `make_key(url: str, *, method: str = "GET", params: Mapping[str, Any] | None = None, body: bytes | str | Mapping[str, Any] | None = None, headers: Mapping[str, str] | None = None) -> str`：单射确定性请求缓存键生成器。
- **`MemorySnapshotStore()`**：
  - 纯内存字典快照存储，接口与 `SQLiteSnapshotStore` 一致，适合单元测试与轻量临时会话。

#### 5. 本地重试执行器 `HttpExecutor`

```text
HttpExecutor(
    snapshot_store: SnapshotStore | None = None,
    max_retries: int = 0,
    retry_delay_base: float = 1.0,
)
```
- **参数说明**：
  - `snapshot_store`：可选绑定的快照存储器；
  - `max_retries`：自然网络异常与 5xx 状态码在本地工作进程内的最大就地重试次数（独立于 Job 重试预算，默认 0）；
  - `retry_delay_base`：指数递增等待基数秒数（单次等待上限 300.0s 并带有 ±25% 抖动）。
- **方法**：
  - `execute(fetch_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any`：执行请求函数；遇到网络异常或 5xx 本地重试；429 立即返回，不进行本地长休眠阻塞；
  - `wrap(fetch_fn: Callable[..., Any]) -> Callable[..., Any]`：将目标抓取函数包装为绑定当前执行器策略的函数。

---

### 9.3 tasklite.wrappers.discovery 增量发现组件

位于 [`tasklite/wrappers/discovery.py`](../tasklite/wrappers/discovery.py)。

```python
from tasklite.wrappers.discovery import register_discovery
```

```text
register_discovery(
    host: DiscoveryHost,
    task_type: str,
    fetch_func: Callable[[DiscoveryJob, DiscoveryContext, int], list[Any]],
    id_func: Callable[[Any], str],
    process_item_func: Callable[[DiscoveryJob, DiscoveryContext, Any, str], None],
    process_task_type: str,
    group_key_func: Callable[[dict[str, Any]], str] | None = None,
    resources: dict[str, float] | None = None,
    max_pages: int = 1000,
    trigger: str = "always",
    scan_mode: str = "incremental",
    on_missing: Callable[[DiscoveryJob, DiscoveryContext, list[str]], None] | None = None,
) -> None
```
- **核心职责**：注册纯任务模型的增量分页扫描器。通过公开游标自主持久化已见集合（seen IDs）；
- **终止条件**：从第 1 页推进至空页（T1a）或某页内容全部命中已见集合（整页命中，T1b）；
- **参数说明**：
  - `host`：实现 `DiscoveryHost` 协议的宿主管线（如 `TaskLite`）；
  - `task_type`：当前发现任务的注册类型名称；
  - `fetch_func(job, ctx, page) -> list[Any]`：分页抓取回调，`page` 从 1 开始；
  - `id_func(item) -> str`：从单项数据提取唯一业务 ID；
  - `process_item_func(job, ctx, item, content_id) -> None`：单项处理或派生子作业回调；
  - `process_task_type`：下游处理子任务类型名称，用于游标命名空间与依赖隔离；
  - `group_key_func`：可选分组函数，提取游标隔离键；
  - `resources`：发现任务运行所需资源声明；
  - `max_pages`：活性保障硬上限（默认 1000）；
  - `trigger`：发现任务跨会话重跑策略（默认 `"always"`）；
  - `scan_mode`：`"incremental"`（默认，整页命中终止）或 `"full"`（全量扫到底）；
  - `on_missing`：仅在 `full` 模式下有效，扫描结束后接收源端缺失 ID 差集回调。
- **协议与辅助工具**：
  - `DiscoveryHost` 协议：要求提供 `register_task(task_type, handler, *, resources=..., trigger=...)`；
  - `DiscoveryHandler`：扫描核心 handler 类，框架无关；
  - `encode_content_id(content_id: str | None, *, max_len: int = 120) -> str`：内容 ID 确定性单射编码工具。

---

### 9.4 tasklite.hooks 辅助工具

位于 [`tasklite/hooks.py`](../tasklite/hooks.py)。

```python
from tasklite.hooks import job_ref, slice_list
```

- **`job_ref(metadata: Any) -> str`**：
  - 从结果元数据中提取人类可读标识（若包含 `post_id` 返回 `f"#{post_id}"`，否则依次尝试 `artist`、`id` 并转为字符串；未找到或非字典返回 `""`）；
- **`slice_list(items: list[Any], *, start: int | None = None, count: int | None = None, limit: int | None = None) -> list[Any]`**：
  - 仅关键字参数。分批切片工具：先按 `limit` 截断总量，再按 `[start : start + count]` 截取窗口。
