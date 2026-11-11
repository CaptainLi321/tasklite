# TaskLite 公开 API 参考手册

> 本文档是 TaskLite 3.0.0.dev0（Schema 6 稳定接口基线版本）的权威公开 API 参考手册。系统定义所有公开类、构造器、方法、属性、参数约束、默认值、状态转移规则、精确异常边界以及数据字典结构。  
> 概念架构、核心模型与使用指南参见 [`docs/GUIDE.md`](GUIDE.md)，架构决策背景参见 [`docs/adr/0011-stable-interface-baseline.md`](adr/0011-stable-interface-baseline.md)，领域术语规范参见 [`CONTEXT.md`](../CONTEXT.md)。

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
   - [2.6 Run 外状态运维、历史清理与授权接口](#26-run-外状态运维历史清理与授权接口)
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
5. [单射编码工具与公开异常](#5-单射编码工具与公开异常)
   - [5.1 规范 UID 单射编解码（make_job_uid, parse_job_uid）](#51-规范-uid-单射编解码make_job_uid-parse_job_uid)
   - [5.2 辅助可逆编码族](#52-辅助可逆编码族)
   - [5.3 公开异常 TaskLiteError](#53-公开异常-taskliteerror)
6. [内置资源与运行排除](#6-内置资源与运行排除)
   - [6.1 CapacityResource 并发容量资源](#61-capacityresource-并发容量资源)
   - [6.2 IntervalResource 时间间隔资源](#62-intervalresource-时间间隔资源)
   - [6.3 Exclude 运行排除规则](#63-exclude-运行排除规则)
7. [环境变量](#7-环境变量)
   - [7.1 TASKLITE_AUTO_APPROVE 自动授权开关](#71-tasklite_auto_approve-自动授权开关)
8. [可选扩展与测试辅助模块](#8-可选扩展与测试辅助模块)
   - [8.1 tasklite.testing 单元测试工具](#81-tasklitetesting-单元测试工具)
   - [8.2 tasklite.wrappers.http HTTP 工具集](#82-tasklitewrappershttp-http-工具集)
   - [8.3 tasklite.wrappers.discovery 增量发现组件](#83-tasklitewrappersdiscovery-增量发现组件)
   - [8.4 tasklite.hooks 辅助工具](#84-tasklitehooks-辅助工具)

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
- `tasklite.testing`：单元测试辅助工具（`fake_ctx`, `running`）；
- `tasklite.hooks`：业务展示切片工具（`job_ref`, `slice_list`）。

---

### 1.2 全局横切契约与严格 JSON 校验

TaskLite 内部所有需持久化或跨进程 IPC 传递的配置与数据（包括 `Job.payload`、`TaskResult.metadata`、`TriggerContext.success_metadata`），均经过统一的**严格 JSON 递归校验**（`validate_strict_json_value`）：

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
   - 调度循环、状态持久化、文件授权检查、清理以及 `Task.trigger` 与 `TaskLite.authorizer` 回调均**在主进程同步执行**；
   - 自定义触发器回调与 `authorizer` 严禁发起可能导致主循环死锁的阻塞或重入操作。

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
    output_roots: str | Path | Sequence[str | Path] | None = None,
)
```

#### 参数约束与校验
- **`backend`**（`Literal["sqlite", "memory"]`，必填，仅关键字参数）：
  - 必须为 `"sqlite"` 或 `"memory"`。传入其它类型或未知字符串抛出 `TaskLiteError`。
- **`state_path`**（`str | Path | None`，默认 `None`，仅关键字参数）：
  - 当 `backend="sqlite"` 时为**必填项**（为 `None` 或全空白字符串时抛出 `TaskLiteError`）。构造时自动转换为解析后的绝对路径 `Path(state_path).resolve()` 并保存在 `self.state_path`。
  - 当 `backend="memory"` 时，`self.state_path` 恒固定为 `None`。
- **`max_workers`**（`int`，默认 `4`，仅关键字参数）：
  - 最大并发工作子进程数。必须为非 `bool` 的 `int` 且 `>= 1`；非法值抛出 `TaskLiteError`。
  - 内部自动以此上限向资源管理器注册名为 `__workers__` 的内置 `CapacityResource`。
- **`output_roots`**（`str | Path | Sequence[str | Path] | None`，默认 `None`，仅关键字参数）：
  - 产物沙盒根目录。构造时不触碰磁盘、不自动创建目录。
  - 内部解析保存形态：
    - 若传入 `None`，属性 `self.output_roots` 为 `None`；
    - 若传入单个 `str` 或 `Path`，属性 `self.output_roots` 为单个规范绝对 `Path` 对象；
    - 若传入序列（`list` 或 `tuple`），属性 `self.output_roots` 为绝对路径列表 `list[Path]`。
    - 传入其他类型抛出 `TaskLiteError`。
- **基准目录 `base_dir`**：
  - 构造时自动固定为当前的绝对工作目录 `Path(os.path.abspath(os.getcwd()))`，后续相对路径的词法解析均基于此固定基准，不受运行时工作目录改变影响。
- **SQLite 状态库 Schema 6 校验**：
  - 若 `backend="sqlite"`，初始化时以 WAL 模式（`journal_mode=WAL`）连接。
  - 若数据库文件已存在且非空，严格校验 `PRAGMA user_version == 6` 及核心表结构。若版本 `<= 5`、异形库或含旧表，**fail-loud 直接抛出 `TaskLiteError` 拒绝启动**，不提供自动原地迁移。

---

### 2.2 公开属性与生命周期状态

- **`state_path`**（`Path | None`，只读）：SQLite 状态库绝对路径，内存后端为 `None`。
- **`output_roots`**（`Path | list[Path] | None`，只读）：沙盒产物根目录。
- **`base_dir`**（`Path`，只读）：构造时固定的绝对路径基准。
- **`is_running`**（`bool`，只读）：管线当前是否正在执行 `run()` 循环中。
- **`authorizer`**（`Callable[[str, str], bool | None] | None`，读写属性）：
  - 主进程文件操作授权回调。签名为 `(path: str, target_type: str) -> bool | None`。
  - 仅限在 `run()` 之外读取或修改，运行期或关闭后访问抛出 `TaskLiteError`。

> [!NOTE]
> **私有化内部边界说明**：为保障面向稳定基线（[ADR-0011](adr/0011-stable-interface-baseline.md)）的封装纯洁性，原内部组件属性 `tasks`（任务注册表）、`resources`（资源管理器）、`runtime_config`（引擎装配配置）及 `backend_type`（后端标识）已彻底私有化为内部字段（`_tasks`, `_resources`, `_runtime_config`, `_backend_type`），不再向外暴露为公开属性。调用方应始终通过 `register_task`、`register_resource` 等标准公开方法与管线交互，不得将可访问的内部对象或私有结构当做稳定公开 API。

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
  - `install_signals`（`bool`，默认 `True`）：是否在主进程安装 `SIGTERM` 与 `SIGINT` 信号监听。
- **内部核心执行流程**：
  1. **状态库排他锁定**：获取 `__tasklite__.lock` 单库文件锁与写事务互斥，确保单状态库同一时刻只有一个调度实例写入；
  2. **启动崩溃恢复（Recovery）**：启动预检扫描状态库。若发现上次运行异常崩溃遗留的 `running` 状态作业，将其原子收敛为失败 Attempt 并移入 `failed` 失败档案；保留处于 `pending` 状态的待办作业，不自动接续旧执行；
  3. **预检 Handler 可序列化性**：验证所有注册任务的 handler 均可被 `spawn` 子进程安全 pickle；
  4. **四相事件泵调度循环**：
     - **准入与选择（Admission & Selection）**：评估 `trigger` 规则与 `exclude` 排除规则（被排除的作业原样留队）；
     - **拓扑依赖与资源仲裁（Scheduling & Resources）**：解析 DAG 依赖，检查 `CapacityResource` 与 `IntervalResource` 可用性；
     - **物理派发（Dispatching）**：向工作子进程派发执行；
     - **结算收口（Completion）**：在独立写事务中原子提交结果、推进 Attempt 轨迹、更新游标与释放资源。

#### 3. `stop() -> None`
- 请求管线优雅停止。
- **行为与状态语义**：
  - 若在 `TaskLite` 已关闭后调用，抛出 `TaskLiteError`；
  - 幂等操作：多次调用无副作用；
  - 若在空闲态（非 `run()` 期间）调用，安全记录标记，不影响后续新启动的 `run()`；
  - 若在启动预检或运行期调用：发出单调状态转移请求，立刻停止调度循环派发**任何新作业**，主循环保持存活并安全等待所有当前在途（in-flight）的工作子进程完成结算后优雅退出。

---

### 2.6 Run 外状态运维、历史清理与授权接口

> [!CAUTION]
> 以下运维管理接口**仅允许在 `run()` 之外调用**。若在管线执行期或关闭后调用，直接抛出 `TaskLiteError`。

#### 1. `set_job_status(job_or_uid, status, *, recursive, note=None) -> None`
人工设置作业状态并原子处理级联下游。
- **参数**：
  - `job_or_uid`（`Job | str`）：目标作业实例或其规范 UID 字符串；
  - `status`（`Literal["pending", "succeeded", "failed"]`）：目标状态，仅允许此三项；
  - `recursive`（`bool`，**必填且仅关键字参数**）：是否递归将该作业的所有已知下游作业同步置为目标状态；
  - `note`（`str | None`，默认 `None`）：人工操作备注，写入结果元数据中。
- **缺失快照的处理差异（源码真实行为）**：
  - **传入 `Job` 实例**：若状态库中尚未存在该作业的历史快照，框架将就地校验并接受传入的 `Job` 作为有效快照，并要求其 `task_type` 已注册；若库中已存在该作业快照，会比对其有效配置，发生冲突时抛出 `TaskLiteError`；
  - **传入 `str` 字符串（UID）**：必须先经 `parse_job_uid` 语法校验。SQLite 与内存双后端实现完全一致（均由底层 `load_known_job_snapshot` 提供），按以下优先级回退顺序检索既有快照：
    1. **`queue`**（待办/重入队队列中的作业快照）；
    2. **`successes`**（成功档案元数据中的作业快照）；
    3. **`failed`**（失败档案元数据中的作业快照）；
    4. **`attempts`**（历史物理执行记录，按 `attempt_id` 倒序取最近一次执行中的完整作业快照）。  
    若在上述全部四级来源中**均无任何既有快照**，直接抛出 `TaskLiteError(f"No existing snapshot found for job UID {target_uid!r}")`，拒绝凭空捏造未知的配置与载荷。

#### 2. `clear_history(uid: str, *, records: Collection[str]) -> int`
从持久化归档中精准清理指定作业的历史记录，返回删除的总行数。
- **参数**：
  - `uid`（`str`）：必须是符合规范单射编码的 `task_type::job_id` 字符串；
  - `records`（`Collection[Literal["successes", "failures", "attempts"]]`，**仅关键字参数**）：
    - 允许传入 `list`、`tuple`、`set`、`frozenset` 容器；容器不能为空；
    - 集合元素**仅允许**包含 `"successes"`（成功档案）、`"failures"`（失败档案，对应数据库 `failed` 表）、`"attempts"`（物理执行轨迹表）；若传入未知目标抛出 `TaskLiteError`。
- **编号进度保留**：历史清理仅删除上述归档行，**绝不重置或修改 `job_sequences` 表中的单调递增编号进度**。
- **返回值**：`int`，受影响并被删除的数据库记录条数总和。

#### 3. `get_cursor(key: str) -> str | None` 与 `set_cursor(key: str, value: str | None) -> None`
- `get_cursor`：读取持久化游标表中最新已提交的字符串值，键不存在返回 `None`；`key` 必须为非空字符串；
- `set_cursor`：直接以原子事务持久化写入游标键值。`value` 为 `None` 表示从表中删除该游标键；`value` 必须为 `str` 或 `None`。

#### 4. `list_authorizations() -> list[dict[str, Any]]` 与 `revoke_authorization(path: str | Path) -> bool`
- **`list_authorizations()`**：只读返回所有持久化在库中的主进程路径授权记录列表。按 `authorized_at ASC, path ASC` 排序。字典字段为：
  ```python
  {
      "path": str,           # 授权的规范绝对路径
      "target_type": str,    # 路径类型："file" 或 "directory"
      "authorized_at": str,  # 授权时间（ISO 8601 UTC 字符串）
  }
  ```
- **`revoke_authorization(path)`**：按词法规范绝对路径精确撤销单条持久化授权。删除成功返回 `True`，未找到返回 `False`。

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
- `ctx.base_dir`（`Path`）：当前管线基准目录；
- `ctx.output_roots`（`Path | list[Path] | None`）：产物沙盒根目录。

> [!NOTE]
> **内部状态彻底私有化**：`JobContext` 仅暴露受控交互方法（`spawn`, `declare_file`, `file_path`, `get_cursor`, `set_cursor`）。内部累积容器 `_spawned_jobs` 与 `_cursor_updates` 已彻底私有化，不向外部暴露任何公开累积容器属性。所有衍生作业与游标变更必须经由 `ctx.spawn()` 与 `ctx.set_cursor()` 受控登记，并在作业成功结算时由主进程原子提交。

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
     - 当通过 IPC 向主进程请求授权或清理时，看门狗计时器**自动暂停**（`pause_timeout()`）；
     - 授权处理完毕并回复子进程后，看门狗计时器恢复剩余时长（`resume_timeout()`），防止等待主进程人工授权导致作业被超时强杀；
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

#### 属性与方法
- `spec.path`（`str | None`）：词法规范化路径；上游产物引用在尚未绑定前为 `None`；
- `spec.producer_uid` / `spec.output_name`（`str | None`）：上游引用信息；
- `spec.role`（`"input" | "output" | "cache"`）：文件角色；
- `spec.delete_existing` / `spec.delete_on_failure` / `spec.delete_on_success`（`bool`）：三项显式删除授权开关；
- `spec.is_output_ref`（`bool`，属性）：是否为上游产物引用；
- `spec.resolve_against(base_dir) -> FileSpec`：按基准目录绝对化普通路径，上游引用保持原样；
- `spec.bind_output_path(resolved_path, ...) -> FileSpec`：绑定为绝对路径 `FileSpec` 并保留清理开关；
- `spec.to_dict() -> dict[str, Any]` 与 `FileSpec.from_dict(data) -> FileSpec`：严格 JSON 序列化与还原（未知键抛出 `TaskLiteError`）。

#### 校验与清理契约
- **角色与删除组合校验**：
  - 当 `role="input"` 时，**严禁设置 `delete_existing=True`**，违规抛出 `TaskLiteError("input FileSpec cannot set delete_existing=True")`；
- **上游直接依赖约束**：
  - `from_output` 引用的 `producer_uid`，**必须出现在当前 Job 的显式 `depends_on` 列表中**，否则在解析与动态声明时直接抛出 `TaskLiteError`；
- **受保护路径硬拦截**：状态库文件、系统关键路径属于受保护路径，任何删除或写授权请求一律硬拦截拒绝；
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
StateSnapshot.open(source: str | Path | TaskLite) -> StateSnapshot
```

- **数据源语义**：
  - **传入数据库路径（`str | Path`）**：以只读 URI 模式（`?mode=ro`）建立连接，并执行 `BEGIN` 持有只读事务。若数据库不存在、为目录或 Schema 未就绪，抛出 `TaskLiteError`；在只读事务生命周期内保证跨表查询的一致性快照；
  - **传入 `TaskLite` 实例**：
    - 若实例已关闭，抛出 `TaskLiteError`；
    - SQLite 后端连接其状态库打开只读快照；
    - 内存后端（`memory`）在实例锁保护下建立队列与档案的完整深拷贝版本快照，后续查询与内存变动隔离；
    - **支持在管线运行中或空闲态安全打开**。
- **公开属性、生命周期与释放**：
  - **`source`**（`Any`，`@property` 只读属性）：打开该快照时传入的原始源对象或数据库路径（如 `str`、`Path` 或 `TaskLite` 实例）；
  - **`closed` / `is_closed`**（`bool`，`@property` 只读属性）：检查快照是否已关闭；关闭后调用任何查询方法均抛出 `TaskLiteError("StateSnapshot is closed")`；
  - 支持上下文管理器协议：`with StateSnapshot.open(...) as snapshot:`；
  - **`close() -> None`**：释放只读连接与内存副本；**关闭快照绝不会关闭底层的 `TaskLite` 实例**。

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
    "started_at": str | None,  # UTC ISO 8601 启动时间戳（以 'Z' 结尾，未启动为 None）
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

### 5.2 辅助可逆编码族

- **`percent_encode(text: str, forbidden=..., *, allowed=...) -> str`**：
  - 严格百分号转义，转义符 `%` 无条件优先按 `%25` 处理，杜绝歧义；非 `str` 抛出 `TaskLiteError`。
- **`encode_identifier(value: Any, *, max_len: int = 120, ...) -> str`**：
  - 确定性单射标识符。编码后 `<= max_len` 时直通；`> max_len` 时采用 `前缀 + "%_" + 64位SHA-256指纹` 截断，标记 `"%_"` 确保截断值绝不会与任何未截断值的像集重叠。
- **`encode_job_component(value: Any, *, max_len: int = 120) -> str`**：
  - `job_id` 组装片段编码，规则同 `encode_identifier`。
- **`encode_content_id(content_id: str | None, *, max_len: int = 120) -> str`**：
  - 用于内容扫描任务的单射编码；空值返回固定哨兵 `"%untitled"`。

---

### 5.3 公开异常 TaskLiteError

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

- **参数与属性**：
  - `name`（`str`）：非空资源名称；
  - `max_capacity`（`float`）：非 `bool` 的有限数值且 `>= 0`，非法值抛出 `TaskLiteError`；
  - `used`（`float`）：当前已被占用的资源总量，初始为 `0.0`。
- **方法行为**：
  - `can_acquire(amount: float) -> ResourceQueryResult`：
    - `amount < 0` 抛出 `TaskLiteError`；
    - `amount > max_capacity` 返回 `is_available=False, is_impossible=True`（明确不可满足，调度器直接登记该作业失败）；
    - `used + amount <= max_capacity` 返回 `is_available=True, wait_seconds=0.0`；
    - 否则返回 `is_available=False, wait_seconds=None, is_impossible=False`（等待在途任务释放并触发唤醒）。
  - `acquire(amount: float) -> None`：实际占用容量；超额抛出 `TaskLiteError`。
  - `release(amount: float) -> None`：释放容量（`used = max(0.0, used - amount)`）并自动触发通知器唤醒等待中的调度循环。

---

### 6.2 IntervalResource 时间间隔资源

从 `tasklite.resource` 导入，用于控制全局请求调用频率（速率限制）：

```text
IntervalResource(name: str, *, interval_seconds: float)
```

- **参数与属性**：
  - `name`（`str`）：非空资源名称；
  - `interval_seconds`（`float`，**仅关键字参数**）：有限正数（`> 0`），非法值抛出 `TaskLiteError`；
  - `next_available`（`float`）：基于 `time.monotonic()` 跟踪的下一次允许获取时间戳。
- **方法行为**：
  - `can_acquire(amount: float) -> ResourceQueryResult`：
    - 若当前单调时间已达到 `next_available`，返回可用；否则返回暂不可用及需等待的秒数 `wait_seconds = next_available - now`。
  - `acquire(amount: float) -> None`：
    - 推进 `next_available = max(now, next_available) + (interval_seconds * amount)`。
  - `release(amount: float) -> None`：**空操作（no-op）**。已消耗的时间配额不可撤销，任务完成不返还时间配额。

---

### 6.3 Exclude 运行排除规则

从 `tasklite.selection` 导入，用于单次 `run()` 中排除指定任务类型的派发：

```text
Exclude(*, task_type: str)
```

- **参数与属性**：
  - `task_type`（`str`，**仅关键字参数**）：待排除的任务类型名称。必须通过 `validate_task_type` 校验。
  - `matches_task_type(task_type: str) -> bool`：精确全字匹配。
- **调度行为**：
  - 通过 `pipeline.run(exclude=Exclude(task_type="..."))` 传入；
  - 命中的作业及其受影响的下游作业**本次 Run 不派发、不执行、不消耗重试预算与资源，原样保留在待办队列中**；
  - 下次未指定排除该类型的 `run()` 启动时，留队作业重新参与正常的候选判断。

---

## 7. 环境变量

### 7.1 TASKLITE_AUTO_APPROVE 自动授权开关

- **精确匹配条件**：
  - 框架仅以 `os.environ.get("TASKLITE_AUTO_APPROVE") == "1"` 做精确判定；
  - 只有当环境变量值**严格等于字符串 `"1"`** 时生效（诸如 `"true"`、`"yes"`、`"True"` 均不生效）。
- **持久化授权效果**：
  - 当命中 `"1"` 时，主进程遇到未授权的文件清理或准备操作时自动批准，并**将该授权永久写入状态库的 `authorizations` 表**中；后续执行直接命中库中授权。
- **受保护路径硬拦截边界**：
  - 即使设置了 `TASKLITE_AUTO_APPROVE="1"`，若目标路径属于状态库自身、锁文件或引擎受保护目录，框架底层安全防护机制依然**硬拦截拒绝授权**，杜绝误删状态库风险。

---

## 8. 可选扩展与测试辅助模块

> [!NOTE]
> 本章节模块属于可选辅助工具，本版不作稳定兼容承诺。

### 8.1 tasklite.testing 单元测试工具

位于 [`tasklite/testing.py`](../tasklite/testing.py)，为业务 handler 提供官方的隔离单测构造句柄。

```python
from tasklite.testing import fake_ctx, running
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
    output_roots: Any | None = None,
    base_dir: str | Path | None = None,
    known_job_uids: Iterable[str] | None = None,
    files: Mapping[str, Any] | None = None,
) -> JobContext
```
- **核心职责**：构建真实的 `JobContext` 实例。严格遵循产物沙盒策略、受保护路径及依赖合法性校验；
- **参数说明**：
  - `tmp_root`：提供时自动在其下创建临时目录作为产物根（与 `output_roots` 互斥）；
  - `successes` / `failed`：模拟历史完成与失败作业集合；
  - `artifacts`：模拟上游作业发布的具名产物描述（用于测试 `from_output` 依赖解析）；
  - `cursors`：注入初始游标字典。

```text
@contextmanager
def running(target: TaskLite | EngineRuntime) -> Iterator[Any]
```
- **核心职责**：测试专用上下文管理器。将 `TaskLite` 实例私有状态临时置为运行中（`_is_running = True`），用于测试各种管理 API 的运行期守卫，退出上下文时自动复位。

---

### 8.2 tasklite.wrappers.http HTTP 工具集

位于 [`tasklite/wrappers/http.py`](../tasklite/wrappers/http.py)。

```python
from tasklite.wrappers.http import (
    HttpResponse,
    urllib_fetch,
    requests_fetch,
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
  - `ok`（`bool`，属性）：`status_code < 400` 时为 `True`。
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

### 8.3 tasklite.wrappers.discovery 增量发现组件

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

### 8.4 tasklite.hooks 辅助工具

位于 [`tasklite/hooks.py`](../tasklite/hooks.py)。

```python
from tasklite.hooks import job_ref, slice_list
```

- **`job_ref(metadata: Any) -> str`**：
  - 从结果元数据中提取人类可读标识（依次尝试 `post_id`、`artist`、`id` 字段；未找到返回 `"-"`）；
- **`slice_list(items: list[Any], *, start: int | None = None, count: int | None = None, limit: int | None = None) -> list[Any]`**：
  - 仅关键字参数。分批切片工具：先按 `limit` 截断总量，再按 `[start : start + count]` 截取窗口。
