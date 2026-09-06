# TaskLite — API 参考与运维指南

> **定位**：完整 API 功能用法 + 生产运维手册。README 只承载「3 分钟入门」（是什么 + 怎么跑通）；本文档是使用者与运维者的详细手册——API 语义、注册/运行/停机契约、状态管理、已知限制、监控、故障排查、性能调优与备份恢复。
>
> **读者**：正在写 handler / 集成管道的开发者 + 负责生产运行与排障的运维者。
>
> **配套文档**：唯一总纲见 [`README.md`](../README.md)（入门 + 架构 + 设计契约 + 开发/Agent 约束 + 重构决策）；Discovery 需求契约见 `tasklite/wrappers/discovery.py` 头部。

---

## 1. API 概览与规范导入路径

公共 API 优先从顶层导入：

```python
from tasklite import (
    TaskLite, Job, TaskContext,
    RetryError, FatalError, PipelineError,
    FATAL_EXCEPTIONS, TRANSIENT_EXCEPTIONS,
    DLQEntry, WORKER_RESOURCE,
    Resource, RateLimitResource, CapacityResource,
)
```

从子模块导入的扩展与包装工具：

| 符号 | 导入位置 | 说明 |
|------|---------|------|
| `register_discovery` | `from tasklite.wrappers.discovery import register_discovery` | 增量发现注册 |
| `validate_payload` | `from tasklite.utils import validate_payload` | payload schema 校验 |
| `sanitize_content_id` | `from tasklite.wrappers.discovery import sanitize_content_id` | content_id 净化（job_id 派生单点） |
| `http_guard`, `SnapshotStore` 等 | `from tasklite.wrappers.http import http_guard, SQLiteSnapshotStore, ...` | 官方轻量网络守卫与快照工具（见 §16） |

`engine/` 与 `backend/` 子包不做再导出（如 `SQLiteStateBackend` 须从 `tasklite.backend.sqlite_backend` 导入）。


**标准调用顺序（六步，顺序本身是设计的一部分）**：

```
① 初始化 TaskLite → ② add_resource → ③ register_handler /
   register_discovery / pipeline.register_transient_exception → ④ enqueue → ⑤ run → ⑥ stop
```

| 步骤 | 调用 | 必须遵守 |
|------|------|----------|
| ① 初始化 | `TaskLite(name, state_dir, output_root=..., max_workers=..., strict_picklable=False)` | `state_dir` 单实例独占；`output_root` 提供路径沙盒（多根）；`max_workers` 默认 4；`strict_picklable=True` 时 `run()` 前对所有 handler 做 pickle 预检（fail-loud） |
| ② 资源 | `add_resource(RateLimitResource / CapacityResource)` | amount 有限非负；同名覆盖会告警 |
| ③a handler | `register_handler(task_type, fn, default_resources=..., payload_schema=...)` | 签名 `(job, ctx)`；返回 None/True/False/dict/(bool, dict) |
| ③b discovery | `register_discovery(host, task_type, fetch_func, id_func, process_item_func, process_task_type, ...)` | `host` 实现 `DiscoveryHost` 协议（TaskLite 为默认）；回调必须模块级可 pickle——**注册期即做 pickle 预检**（fail-loud） |
| ③c 瞬态异常 | `pipeline.register_transient_exception(cls)` | per-pipeline 注册表；模块级 `Exception` 子类；`RetryError`/`FatalError` 子类入口即拒 |
| ④ 入队 | `enqueue(Job(...))` 或 `enqueue([Job(...)], front=False)` | 单 Job 自动包成列表；在 `run()` 启动前完成；同 uid 静默去重；payload 必须 JSON 可序列化 |
| ⑤ 点火 | `run()` | 阻塞至队列排空；首次 SIGTERM 优雅 DRAINING、二次强制 ABORTING；同一 `state_dir` 并发 `run()` 由 pipeline 级文件锁 fail-loud 拒绝 |
| ⑥ 停机 | `stop(force=False)` / `stop(force=True)` | **请求**停机（非阻塞，由 `run()` 消费）：生产用 DRAINING；`force=True` → ABORTING（见 §7） |

---

## 2. Job 构造与硬约束

```python
Job(
    task_type="download",      # 必填：任务类型（对应注册的 handler）
    job_id="img_001",          # 必填：唯一标识
    payload={"url": "..."},    # 可选：业务数据（必须 JSON 可序列化）
    resources={"api": 1.0},    # 可选：覆盖 handler 默认资源
    max_retries=3,             # 重试上限（耗尽进 DLQ）
    depends_on=["parent::id"], # DAG 依赖（"task_type::job_id" 形式）
    timeout=3600,              # 超时秒数（看门狗 SIGKILL）
    backoff_base=2.0,          # 退避基数
    backoff_max=300.0,         # 退避上限（软上限，抖动后可达 1.25×）
    timeout_is_transient=False,# True 时看门狗超时按重试处理而非直接 DLQ
    rerun="never",             # 跨会话重跑策略（never/on_failure/every_run/on_input_change）
)
```

**身份与去重**：Job 的 `uid`（`task_type::job_id`）是身份，`payload` 不参与——enqueue 两个同 uid 不同 payload 的 job 时，后者静默跳过。

**构造硬约束**（违反即入口抛错，fail-fast）：

| 约束 | 违反后果 |
|------|----------|
| `task_type` / `job_id` 必须为 `str` 且不含 `"::"` | TypeError / ValueError——`::` 是 uid 分隔符，碰撞即任务合并 |
| `job_id` 必须**确定性**（禁止 uuid/时间戳随机后缀） | wall 去重失效 → 崩溃重跑后同一内容重复执行（「每次重跑」用 `rerun` 策略，见 §8）；框架源码由 `tests/hygiene/test_job_id_lint.py` AST lint 兜底 |
| `payload` / `resources` 必须 dict；payload 必须 JSON 可序列化（NaN/Inf 拒绝） | 子进程 IPC 崩溃 / 非标准 JSON 毒化下游（JSON 预检在 **enqueue 与 `ctx.spawn`**——构造期只查 dict 类型，`Job("t","a",payload={"x": nan})` 可构造、入队时拒） |
| `timeout` / `backoff_base` / `backoff_max` 有限非负（拒绝 bool/NaN/str/Inf；仅 `timeout` 要求 > 0） | 看门狗永不触发 / 退避 NaN 污染无限空转 |
| `depends_on` 为 `"task_type::job_id"` 形式 str 列表 | 环检测与级联阻断失效（构造只校验「list 且元素全为 str」，`::` 格式是使用约定——非法格式按 uid 字符串静默不匹配，依赖永不满足 → 最终走死锁分类） |
| 资源 amount 有限非负（拒绝负值/NaN/Inf/bool） | acquire 崩溃 / CapacityResource 账目 NaN 死锁 |
| `rerun` 为 `never`/`on_failure`/`every_run`/`on_input_change` 之一 | 非法值入口拒绝（fail-loud） |

---

## 3. TaskContext — handler 视角的 API

每个 Handler 接收 `(job, ctx)`。`ctx` 提供：

| 方法 | 用途 |
|------|------|
| `ctx.spawn(job)` | 入队子任务（立即 JSON 预检） |
| `ctx.declare_output(path, cleanup_on_fail=True, sandbox=True)` | 声明输出文件。**返回解析后的绝对路径**（相对路径按 `output_root` 重定位——多根（list）时固定拼到**第一个**根）——用返回值写文件。`sandbox=False` 显式豁免路径沙盒（跨盘） |
| `ctx.declare_cache(path)` | 声明**临时文件**：任务结束时（无论成败）该文件不应存在——成功跳过存在性校验 + 尝试删除，失败/中止删半成品。原子产出的 `.part` 用它声明 |
| `ctx.declare_input(path)` | 声明**输入文件**：采集指纹 `{path, size, mtime_ns}` 落 wall meta——排障/审计可追溯；与 `rerun="on_input_change"` 联动做变更检测；返回规范绝对路径 |
| `ctx.declare_input_uri(url, uri_fingerprint=None)` | 声明**输入 URI**：仅记录（可追溯）；URI 变更检测默认关闭（需网络请求），留给业务；返回 url 字符串 |
| `ctx.is_completed(uid)` / `ctx.is_failed(uid)` | 检查任务是否已完成 / 永久失败（**每个 job 派发时的** wall/failed 快照——同一 run 内后派发的 job 能看到先前完成的任务） |
| `ctx.attempted_uids()` | 返回 wall∪failed 已见 uid 的**只读快照**（frozenset）——供 `on_missing` 差集计算等「需要可迭代已见集合」的场景；快照语义同 `is_completed`/`is_failed` |
| `ctx.get_cursor(key)` / `ctx.set_cursor(key, value)` | 读写高水位游标；`set_cursor(key, None)` 删除 |
| `ctx.suspend_resource(name, seconds)` | 全局暂停资源（429 限流 / 配额耗尽熔断），跨重启持久化（注：`RateLimitResource` 的正常限速余量同样会被持久化恢复——「资源不可用状态」整体落盘，语义上无害） |

> **原子产出标准模式**（文档化）：大文件/需校验的产物，一律「写 `*.part` → 自校验 → `os.replace` 到最终路径」。`.part` 用 `declare_cache` 声明（半成品自动清理、成功跳过存在性校验），最终路径用 `declare_output` 声明（存在性校验 + 失败清理）。禁止直接写最终路径（崩溃留下半截文件，且无法与「从未写过」区分）。同盘 `os.replace` 是原子操作，可覆盖已存在目标。

**Handler 返回值**：`None`/`True`/`dict`/`(True, dict)` → 成功；`False`/`(False, dict)` → 失败进 DLQ；`raise RetryError` → 退避重试；`raise FatalError` → 直接 DLQ。

**Handler 语义契约速查**：

| 行为 | ✅ 正确做法 | ❌ 反模式 |
|------|------------|-----------|
| 瞬态失败 | `raise RetryError` 或抛已注册瞬态类 | `try/except` 吞异常 |
| 速率控制 | `resources={"api": 1.0}`；限流熔断 `ctx.suspend_resource("api", 60)` | 手动 `time.sleep` |
| 输出文件 | `ctx.declare_output(path)` + 用**返回值**写 | 手动 `os.makedirs` / 用原始相对路径写 |
| 去重 | 交给框架 wall 去重（同 uid 自动跳过） | 手动检查「做过没」 |
| 子任务 | `ctx.spawn(Job(...))`（框架落盘 IPC + 去重） | 串行内联调用 |
| 状态查询 | `ctx.is_completed(uid)` / `ctx.is_failed(uid)` | 自己拼 wall/failed 集合 |
| 下游副作用 | `job.uid`（`task_type::job_id`）作幂等键 | 假设重跑后副作用只执行一次（框架保证 at-least-once，不保证副作用恰好一次） |

---

## 4. 资源（Resources）

| 类型 | 用途 | 示例 |
|------|------|------|
| `RateLimitResource(name, interval)` | 频率控制（任务派发粒度） | `RateLimitResource("api", 2.0)` — 每 2 秒 1 次 |
| `CapacityResource(name, max)` | 并发限制 | `CapacityResource("gpu", 5.0)` — 最多 5 个并行 |
| `__workers__`（内部） | 并发子进程数 | `add_resource(CapacityResource("__workers__", 8))` 覆盖 `max_workers` |

注意：`RateLimitResource` 只串行化**任务派发**粒度；一个任务内部的多次请求（如逐页拉取）需在 handler 内自行控制节奏。

**挂起语义**：`ctx.suspend_resource(name, seconds)` 全管线休眠（跨重启持久化，上限 24h）。框架通过 `Resource.suspended_until()` 协议访问器在 run 结束时把挂起截止换算为 wall-clock 落 meta 表，重启加载期反向换算为 monotonic——无挂起/无限速等待返回 `None`；新增自定义 `Resource` 若需要持久化挂起/等待状态，应覆写该方法，否则默认不持久化。

---

## 5. 错误处理（三分类）

| 类别 | 异常 | 行为 |
|------|------|------|
| **Transient**（自动重试） | `RetryError`；连接/超时类内置异常（`TRANSIENT_EXCEPTIONS`：ConnectionError/TimeoutError/ConnectionRefusedError/ConnectionResetError/ConnectionAbortedError/BrokenPipeError）；`pipeline.register_transient_exception(cls)` 注册的自有异常 | 退回队列，指数退避（±25% 抖动），达 `max_retries` 进 DLQ |
| **Fatal**（直接 DLQ） | `FatalError`；`FATAL_EXCEPTIONS`（`TypeError`/`KeyError`/`AttributeError`/`IndexError`/`StopIteration`/`ArithmeticError`/`ImportError`/`NotImplementedError`/`RecursionError`） | 直接 DLQ，不消耗重试次数（确定性 bug） |
| **Unknown**（DLQ） | 其余 `Exception` | 永久失败，触发级联阻断 |

```python
pipeline = TaskLite(...)

class MyApiClientError(Exception): ...           # 必须模块级定义（可 pickle）
pipeline.register_transient_exception(MyApiClientError)   # 此后该异常自动重试
```

注意：
- 注册的异常类必须**模块级定义**（可 pickle）——函数内定义的类注册时立即抛 `TypeError`（fail-loud，避免「注册只在父进程生效、子进程静默判死」）。
- 第三方库异常（如 `requests.exceptions.ConnectionError`）**不是**内置 `ConnectionError` 的子类，需显式注册才会自动重试。
- 用户注册的瞬态类判定**先于** `FATAL_EXCEPTIONS` 兜底——注册了 FATAL 子类会按瞬态重试。

---

## 6. 注册增量探索（Discovery）

```python
from tasklite.wrappers.discovery import register_discovery  # 注意：顶层不导出

def fetch_page(job, ctx, page):
    # 框架从 page=1 开始逐页调用；空列表表示页尾
    return api.get_posts(artist_id=job.payload["artist_id"], page=page)

def item_id(post):
    return str(post.id)  # 内容全局唯一 id（不要求单调递增）

def group_key(payload):
    return f"c_{payload['artist_id']}"  # 模块级函数（回调必须可 pickle）

def process_item(job, ctx, post, content_id):
    # content_id 已净化，可直接用作确定性 job_id（去重依据）
    ctx.spawn(Job("download", content_id, payload={"url": post.image_url}))

register_discovery(
    pipeline,                      # 第一个参数是 pipeline 实例（模块级函数，从 tasklite.wrappers.discovery 导入）
    task_type="discover",
    fetch_func=fetch_page,
    id_func=item_id,
    process_item_func=process_item,
    process_task_type="download",        # process 子任务的 task_type（已见判定用）
    cursor_key_func=group_key,           # 可选：job_id 命名空间前缀（必须模块级可 pickle）
    default_resources={"api": 1},
    scan_mode="incremental",             # "incremental"（整页命中终止）/ "full"（扫到空页）
    on_missing=None,                     # 可选：full 模式的源端缺失记录检测回调 (job, ctx, missing_ids)
)
```

**语义**（完整契约见 `tasklite/wrappers/discovery.py` 头部注释）：框架从第 1 页逐页调用 `fetch_func`，「已见」判定直接用框架 wall/failed 集合（`ctx.is_completed` / `ctx.is_failed` 快照）——**无 cursor、无 seen 持久化、无互斥注入**；**整页命中**或空页时终止本次扫描；每个新内容以确定性 job_id 交给 `process_item_func`。重新发现只处理新增内容、免疫源端删除带来的游标漂移，崩溃重跑由确定性 job_id 经 wall 去重吸收。**回调必须为模块级可 pickle 函数**（spawn 进程隔离约束——lambda/闭包在子进程 import 时失败）；**`register_discovery()` 注册期即做 pickle 预检**（fail-loud，不再等到 spawn）。

**架构性质（框架无关）**：扫描核心是 `DiscoveryHandler`——运行时**不 import 任何 tasklite 框架模块**，只依赖 `DiscoveryJob`（`payload`）与 `DiscoveryContext`（`is_completed`/`is_failed`/`attempted_uids`）两个最小协议。`register_discovery` 是框架提供的默认封装函数，只通过 `DiscoveryHost` 公开协议（`register_handler` + `set_discovery_rerun`）挂载扫描器；任何实现该协议的宿主/测试桩都可直接复用。`TaskLite` 只是默认宿主实现。

**回调契约**：

| 回调 | 签名 | 职责 |
|------|------|------|
| `fetch_func` | `(job, ctx, page) -> list[item]` | 从 page=1 逐页拉取；**必须返回 list/tuple**（`""`/`None` 等非 list/tuple 被拒并**抛 ValueError**（fail-loud），非告警跳过）；空列表 = 页尾 |
| `id_func` | `(item) -> str` | 内容全局唯一 id（不要求单调递增）；单个坏 item 只跳过该条，不崩整轮扫描 |
| `process_item_func` | `(job, ctx, item, content_id) -> None` | `content_id` 已净化（单射转义），可直接作子任务 `job_id`；通常 `ctx.spawn(Job(process_task_type, content_id, ...))` |
| `process_task_type`（参数） | `str` | process 子任务的 task_type——整页命中判定按 `f"{process_task_type}::{content_id}"` 查询 wall/failed，**必须与 spawn 时一致**（否则已见判定永不命中） |
| `cursor_key_func`（可选参数） | `(payload) -> str` | 仅作 job_id 命名空间前缀（防跨分组内容 id 碰撞），**无互斥、无游标语义**；返回非空 str |

**scan_mode="full"**：跳过整页命中终止，一路扫到空页或 `max_pages`——全量成本只在 fetch（已见内容仍逐条跳过，不重复 spawn）。配合 `on_missing` 回调可做**源端删除/缺失检测**：扫描结束把「已见但本次未扫到」的 content_id 差集交给业务（`(job, ctx, missing_ids)`）。

### 6.1 通用管线脚手架（pipeline_util）

`tasklite.pipeline_util` 是各类任务编排消费方共用的**数据与展示工具**：

```python
from tasklite.pipeline_util import (
    content_fingerprint, sanitize_job_component,
    progress_hook, job_ref, slice_list,
)
```

- `content_fingerprint(parts, version="")`：确定性内容指纹（sha1 截 16 位，
  任何输入/版本盐变化 → job_id 变化 → wall 重跑）；
- `sanitize_job_component(s)`：净化任意串为无 `::`/路径分隔/控制字符的 job_id 成分（单射转义，底层委托 `utils.injective`）；
- `progress_hook` / `job_ref`：父进程进度回调；
- `slice_list(items, start, count, limit)`：分批切片工具。

> **主机生命周期与瞬态注册（直接由 `TaskLite` 原生提供）**：
> - `pipeline.run_graceful()`：统一 run + 优雅停机包装（Ctrl+C 转 DRAINING，自然等在途完成后安全退出）；
> - `pipeline.register_transient_exceptions([cls1, cls2])`：批量注册业务瞬态异常；
> - `pipeline.register_file_transients()`：把常见文件系统异常批量注册为瞬态（`PermissionError/BlockingIOError/ConnectionResetError`）。


---

## 7. 状态管理 API（DLQ / 运维通道 / 种子化）

> 仅限 `run()` 之外调用——改变 `is_known` 判定基础，与 `enqueue` 同纪律。**由代码强制**：run() 进行中调用这些方法会抛 `RuntimeError`。

### 7.1 DLQ 查询与清除

```python
entries = pipeline.list_dlq()            # 只读查询：[(uid, error_type, error, attempts, failed_at, meta)]
n = pipeline.clear_dlq()        # 清除：删 DLQ 条目（默认保留 fatal=true），随后由调用方 enqueue 同名任务重跑
n = pipeline.clear_dlq(task_types=["download"], keep_fatal=False)
n = pipeline.clear_history("download::")   # 删除 wall/DLQ 条目（强制重下/垃圾清理）；"download::" 前缀匹配
n = pipeline.clear_history("t::a", where=("wall",))   # 精确 uid；where 可选 "wall"/"failed"
```

- **`list_dlq()`**：只读查询，返回结构化条目 `DLQEntry(uid, error_type, error, attempts, failed_at, meta)`。损坏行（非 dict meta / 非 int `_attempt`）被兜底为 unknown 分类展示，不炸查询。
- **`clear_dlq`**：清除 = 删 DLQ（**不自动 enqueue**，由调用方随后 `enqueue` 同名任务重跑）。默认保留 `fatal=true` 的确定性失败（`FatalError`），`keep_fatal=False` 一并删除；`task_types` 按 task_type 前缀过滤。
- **`clear_history`**：完整 uid 精确删除；**以 `::` 结尾**的字符串按前缀匹配（防 `"download"` 误匹配 `"downloads::"`）。用于「手动误删文件强制重下」（wall 清掉该 uid）与历史垃圾清理。
- **DLQ 结构化字段**：每条 DLQ 记录统一带 `error_type`（`fatal` / `dependency` / `deadlock` / `transient_exhausted` / `no_handler` / `validation` / `commit_failure` / `dispatch` / `unknown`）与 `failed_at`（UTC ISO 时间戳）——排障不用再翻整份日志。错误码登记于 `tasklite/error_codes.py`。

### 7.2 种子化 API（存档迁移官方通道）

```python
pipeline.seed_wall(["download::img_001", "download::img_002"])  # 标记「已处理」（meta 空 dict）
pipeline.seed_cursor("progress", "2026-01-01")                  # 预填通用业务 cursor
from tasklite.wrappers.discovery import sanitize_content_id  # 公开的 content_id 净化（job_id 派生单点）
```

- **`seed_wall`**：存档迁移（硬链接 + wall 种子）标记历史内容已处理——之后 enqueue 同 uid 被去重吸收。**新 discovery 的「已见预填」就用它**（把 process 任务 uid 写入 wall；discovery 已见判定不再用 cursor）。
- **`seed_cursor`**：通用业务 cursor 预填（`ctx.get_cursor` 可读）。
- **`sanitize_content_id`**：与框架同规则的 content_id 净化（**单射转义**：干净 id 原样、脏 id 百分号转义、超长截断 + hash），存档迁移/派生 job_id 不再 import 私有函数。

### 7.3 停机状态机（run/stop/SIGTERM）

| 触发 | 语义 |
|------|------|
| 首次 `stop()` 或 SIGTERM | **DRAINING**：不再派发新 job，等在途 job 自然完成后再退出 |
| 二次 `stop(force=True)` 或 SIGTERM | **ABORTING**：**已完成**（结果文件已落盘、只差 drain 回收）的 job 被消费提交（进 wall/failed，不 kill、不删产出、不 requeue）；仅对**进行中** job 执行 kill + 清半成品 + requeue 后退出 |
| Ctrl+C（KeyboardInterrupt） | 立即中止：已完成 job 同样被消费提交，仅进行中 job requeue（不进 DLQ） |

**崩溃恢复三层防线**：结果文件携带执行代标识（`{uid}.{run_id}.{seq}.result.json`，孤儿进程的旧结果对新 run 不可见）；派发前消费上次崩溃残留的结果文件（drain_stale）；`{uid}.lock` 文件锁探测孤儿执行体（同 uid 同时只有一个执行体）。

---

## 8. rerun 策略（跨会话重跑）

「成功进 wall 是否算数」由 `Job(rerun=...)` 决定——**不改变 job_id 派生**（身份仍是 uid），只作用于 wall/failed 拦截点：

| 策略 | wall 命中 | failed 命中 | 典型用途 |
|------|-----------|-------------|---------|
| `"never"`（默认） | 跳过 | 跳过 | 一切内容任务（现状语义） |
| `"on_failure"` | 跳过 | **重跑**（成功自动清 DLQ 残行） | 网络误失败可自愈的任务 |
| `"every_run"` | **重跑**（wall REPLACE，run_count+1） | **重跑** | discovery 扫描、scan orchestrator |
| `"on_input_change"` | 比对输入指纹：文件 `size`/`mtime_ns` 变则重跑 | 转码重编、stats 刷新 |

**语义要点**：
- **queue/in-flight 永远算数**——同一轮内不重复派发/并发双跑，互斥语义不受影响（策略只豁免 wall/failed）。
- **运行历史落 wall meta**：每次成功在 wall 条目写 `run_count`（成功次数）、`last_run_at`、`last_run_id`——debug 时看 wall 一行就知道「跑过几次、最近一次什么时候」。
- **输入指纹落 wall meta**：`ctx.declare_input(path)` 采集的 `{path, size, mtime_ns}` 随成功写入 `meta["inputs"]`——审计可追溯，且是 `on_input_change` 的比对依据（下次 enqueue 时框架对旧指纹重新 stat，任一文件变化/消失 → 重跑；无历史指纹 → 视为变化重跑）。URI 声明不参与比对（变更检测默认关闭，需网络请求，留给业务）。
- **策略随 job_dict 持久化**，加载期修复（契约 3）对 every_run/on_failure/on_input_change 任务豁免「残留清理」——跨崩溃重启后策略仍生效。
- **discovery 默认 every_run**：`register_discovery(...)` 的 `rerun` 参数（默认 `"every_run"`）在 enqueue 时自动注入——discovery 任务用固定 uid（如 `discover::favorites`）每会话重扫，不再需要时间戳后缀。Job 默认 `rerun=None`（未指定哨兵）——未指定时注入 discovery 默认；**显式指定（含 `rerun="never"`）一律尊重、不再覆盖告警**。
- **失败语义不变**：rerun 只影响跨会话要不要再次执行，单次执行内部的 RetryError 退避/max_retries/DLQ 流程一字不改。rerun 任务重跑失败 → wall 旧成功记录作废（最终状态唯一）。

```python
Job("download", "img_001")                        # never（默认，现状）
Job("scan", "favorites", rerun="every_run")       # 固定 uid 每会话重扫
Job("sync", "artist_5", rerun="on_failure")       # 网络误失败自愈
```

---

## 9. 存储后端

仅支持 **SQLite**（默认，WAL 模式 + `synchronous=FULL`；`backend` 参数也可传 `AbstractStateBackend` 实例，SQLite 是唯一内置实现）。断电不损坏数据库，已应答的事务保证落盘（确保断电不丢失已应答任务）。执行中事务回滚仍由 at-least-once 语义吸收（对应 job 重跑，不数据损坏）。

**三条状态契约**（内存↔磁盘一致性，详见 `README.md「架构概览」`）：
1. 内存 queue = 磁盘 queue − in-flight（commit 成功才从磁盘删除）；
2. wall/failed/cursors 仅在后端事务提交成功后才推进内存（提交失败 → requeue + 故意崩溃，重启 at-least-once 重跑；同 job 连续 3 次 commit 失败转 DLQ）；
3. 每次 `run()` 启动时做加载期修复，兜底任何历史漂移。

---

## 10. 监控与可观测性

### 10.1 生命周期钩子

```python
pipeline = TaskLite(
    name="my_pipeline",
    state_dir="./state",
    on_run_start=on_start,              # run() 开始前同步调用（无参数）
    on_run_end=on_end,                  # run() 结束时调用，参数 exit_reason:
                                        #   "completed" | "stopped_draining" |
                                        #   "stopped_aborting" | "interrupted" | "error"
    on_job_completed=on_job_done,       # 每次 attempt 完成时调用
)
```

- **`on_job_completed(uid, result_meta, success, going_to_retry)`**：**每次 attempt 完成时同步调用**（同一 job 跨重试生命周期会触发多次）——`going_to_retry=True` 表示将退避重试、`False` 才是终局（成功/DLQ）。在 stats 更新之后、下一 job 派发之前调用；钩子内读 stats 保证一致。监控计数器应只在 `going_to_retry=False` 时累加「终结」指标。
- 钩子契约：同步、主线程执行、必须轻量非阻塞（重活业务方自丢线程池）；抛异常 → catch + warning + `stats["hook_errors"]` 计数，**绝不影响主循环**——钩子按不可信代码对待。多方订阅由业务自封装分发器，框架不维护监听器列表。

### 10.2 stats 运行指标

`pipeline.stats`（run 开始时重置）：

| 键 | 含义 |
|----|------|
| `completed` | 成功完成数 |
| `failed` | 进 DLQ 数 |
| `retried` | 瞬态失败退回队列重试数 |
| `skipped` | 去重跳过数 |
| `hook_errors` | 钩子抛异常计数 |
| `deferred_orphan` | 孤儿锁探测 defer 计数（短退避等待） |
| `interrupted_reruns` | worker 被中断（Ctrl+C/SIGTERM）零计数回队数 |
| `cascade_failed` | 因上游失败被级联阻断进 DLQ 的下游数（JOB_DEPENDENCY）——与 `failed` 分开，DLQ 总量 = failed + cascade_failed + 死锁等批量终态 |

### 10.3 关键日志行

| 日志（logger `tasklite`，默认 WARNING） | 含义 / 排障提示 |
|------|------|
| `SKIP: {uid} (Dependency {dep} failed)` | 依赖已失败，job 直接 JOB_DEPENDENCY 进 DLQ |
| `No handler for: {task_type}` | NO_HANDLER——handler 未注册 |
| `Payload validation failed for {uid}` | 校验失败进 DLQ |
| `FAIL: {uid} (... Sent to DLQ)` | 运行期失败，meta 含 error/traceback |
| `FAIL: {uid} exceeded max retries` | MAX_RETRIES_EXCEEDED |
| `Backend commit failure; aborting in-flight jobs` | 故意崩溃路径（磁盘故障）——重启恢复 |
| `Force abort requested. Killing in-flight jobs.` | 二次 SIGTERM / stop(force=True) |
| `Pipeline scheduler crashed with unhandled exception` | 主进程异常——重启恢复（at-least-once） |
| `DEPENDENCY GRACE: N job(s) waiting` | 依赖宽限中（60s 内可运行候选可能 spawn 依赖） |
| `Deadlock detected during backoff/wait: dependency cycle` | 依赖环被退避遮蔽——环成员进 DLQ |

### 10.4 运行历史（wall meta）

wall 条目除业务 meta 外携带：`run_count`（成功次数）、`last_run_at`（UTC ISO）、`last_run_id`、`inputs`（输入指纹）——`backend.load_wall()` 或 `ctx.is_completed` 可读，排障「跑过几次、最近一次什么时候、基于什么输入」。

### 10.5 DLQ 结构化排障

`pipeline.list_dlq()` 每条含 `error_type`（fatal/dependency/deadlock/transient_exhausted/no_handler/validation/commit_failure/dispatch/unknown）+ `failed_at`（UTC ISO）——按类型聚合即可定位批量失败根因，不必翻日志。

---

## 11. 已知限制（设计边界）

1. **双实例并发（代码级强制）**：同一 `state_dir` 并发 `run()` 会被 pipeline 级文件锁 fail-loud 拒绝（`RuntimeError`），不再只是文档约定；历史语义是去重被击穿、commit 互相打崩。不同业务线仍请用不同 `state_dir`。
2. **`backoff_max` 是软上限**：抖动后实际退避可达 `1.25 × backoff_max`。需硬上限请按 `backoff_max / 1.25` 设置。
3. **`enqueue()` 与 `run()` 不可并发**：运行中的 `run()` 看不到之后入队的任务（无告警）。正确用法：`run()` 前完成 enqueue，或在 handler 内 `ctx.spawn()`。
4. **路径沙盒 TOCTOU 窗口**：路径检查与实际写文件之间存在理论性 symlink 替换窗口。不可信环境下在 handler 内用 `os.open(..., O_NOFOLLOW)`。
5. **依赖「动态 spawn 的 uid」死锁宽限**：`DEPENDENCY_DEADLOCK` 判定加了宽限期（`_dependency_grace`，60s）——队列中存在可运行候选（潜在 spawner）时给宽限而非立即 DLQ，spawner 退避/阻塞期间依赖其未来子任务的 job 不会被误杀；宽限超时才判死锁。极端场景（spawner 长时间不可运行）仍建议「同批先 spawn 再引用」。
6. **3-strike commit 守卫覆盖 bulk 路径**：死锁/级联的批量提交失败现在同样计数（`_commit_bulk_failed_crash`，`_commit_failures` 持久化），达阈值转单条 DLQ——持久性 DB 故障下不再无限崩溃重启循环（达阈值后单条 DLQ 也失败时仍需人工介入）。
7. **job_id 结尾含 `.<32hex>.<数字>` 的残留结果文件歧义（理论）**：结果文件命名 `{safe_uid}.{run_id}.{seq}.result.json` 的孵化段用 32-hex + 数字——若某 job 的 `job_id` 恰好以 `.<32hex>.<数字>` 结尾（如 `t::a.abcdef01234567890123456789012345.3`），其残留文件与 `t::a` 的孵化文件路径形状重合，`_iter_stale_result_paths` 可能把兄弟 uid 的残留误收为该 uid 的（崩溃恢复时跨 uid 消费结果）。触发需 job_id 恰好满足该形状（正常内容 id 极少 32-hex 结尾），且仅在崩溃恢复的 drain_stale 路径——低概率理论缺陷，记录为已知限制。
8. **孤儿 defer 无升级路径（只写不修）**：派发前 `probe_lock` 探测到孤儿执行体持锁时，job 以约 0.75–1s 短退避 requeue 等待（抖动截顶 ≤1s），**无连续 defer 计数、无升级到 DLQ 的路径**。极端场景——D-state 孤儿（NFS/网盘挂起的 IO，SIGKILL 无效）——可导致该 uid **无限 defer**：管线永不排空、job 永不执行也永不进 DLQ，`min_wait` 有限使死锁检测不触发，唯一信号是每秒一条 warning 日志。根因：flock API 不暴露持有者 PID，无法可靠判定「孤儿永久卡死」；即便拿到 PID，D-state 进程 SIGKILL 也无效（`_JOIN_REAP_TIMEOUT=5s` 已防主循环被 join 拖死）。**运维逃生口**：停 pipeline 后整目录离线清理 `ipc/` 下残留的 `{uid}.lock` 锁文件与孤儿进程。长线改进（`{uid}.lock` 记录 PID + 活性探测 + 超时升级 DLQ）需改锁定机制，单独立项，不做小补丁。

---

## 12. 故障排查

### 12.1 管线卡住 / 永不排空

| 症状 | 诊断 | 处理 |
|------|------|------|
| 日志每秒一条孤儿 defer warning，job 永不执行 | 已知限制 #8（D-state 孤儿持锁） | 停 pipeline → 清 `ipc/` 残留 `{uid}.lock` → 重启 |
| `DEPENDENCY GRACE` 日志持续、队列不前进 | 依赖缺失宽限（60s）——等待潜在 spawner | 检查 spawner job 是否存活；确认「同批先 spawn 再引用」 |
| `min_wait` 无限 sleep、无日志 | 全部 job 退避/资源不可用 | 检查资源是否被 suspend（`resource_suspends` meta）；检查退避参数 |
| `Deadlock detected` | 依赖环 | 环成员自动进 DLQ（DEPENDENCY_DEADLOCK），其余保留 |

### 12.2 崩溃 / 无限重启循环

| 症状 | 诊断 | 处理 |
|------|------|------|
| `Backend commit failure` + 崩溃重启 | 磁盘/DB 故障（环境性） | 修复磁盘后重启；同 job 连续 3 次 commit 失败自动转 DLQ 防循环 |
| 每次重启后同一批 job 重跑 | at-least-once 语义（结果文件丢失窗口） | 业务副作用必须幂等（用 `job.uid` 作幂等键） |
| `Pipeline scheduler crashed with unhandled exception` | 未知异常（防御网兜底） | 看 traceback 定位；重启恢复（磁盘为真相源） |

### 12.3 DLQ 分类速查

| error_type | 含义 | 处理 |
|------------|------|------|
| `fatal` | FatalError / FATAL_EXCEPTIONS（确定性 bug） | 修 handler，`clear_dlq(keep_fatal=False)` 后重跑 |
| `transient_exhausted` | 瞬态重试耗尽（MAX_RETRIES_EXCEEDED） | 检查外部服务；调大 `max_retries` 或 `backoff_*` 后 `clear_dlq` 再 enqueue |
| `dependency` | 父任务失败级联 | 先修父任务 |
| `deadlock` | 依赖环 / 畸形 / 资源不可达 | 修 DAG 或资源声明 |
| `no_handler` / `validation` | 未注册 / payload 校验失败 | 修注册或 schema |
| `commit_failure` / `dispatch` | 磁盘故障 / 派发失败（如不可 pickle handler） | 修复环境 / 改模块级 handler |
| `unknown` | 其余 Exception | 看 meta.traceback |

### 12.4 运维逃生口汇总

- **孤儿锁**：停 pipeline → 离线清 `ipc/` 下 `{uid}.lock` 与孤儿进程（已知限制 #8）。
- **强制重下**：`clear_history("task::uid")` 清 wall → enqueue 同名任务。
- **批量清除**：`clear_dlq(task_types=[...])` → 由调用方 enqueue 重跑。
- **已见预填**：`seed_wall([...])`（存档迁移 / discovery 冷启动）。
- **手动修库**：所有裸 `json.dumps/loads` 必须经 `tasklite/utils/jsonutil.py`（禁 NaN）；修改 SQLite 请先停 pipeline。

---

## 13. 性能调优

### 13.1 并发度

- `max_workers`（默认 4）或 `add_resource(CapacityResource("__workers__", N))` 覆盖——子进程数是吞吐主调节旋钮。
- `CapacityResource("gpu", ...)` 等按业务资源限并发；`RateLimitResource` 限任务派发频率。
- 注意：`RateLimitResource` 只串行化**派发**粒度；handler 内多次请求自行控速。

### 13.2 调度器扫描

主循环每轮从队列挑可运行 job。已针对大队列优化：
- **workers 耗尽预检**：workers 满时跳过全队列扫描（避免 CPU 空烧）。
- **反序列化缓存按 run 生命周期**：阻塞/退避阶段不重复解析队列（`scheduler.cached_job` 内容键缓存）。
- 依赖缺失宽限（`_dependency_grace`）复用缓存而非裸反序列化。

大队列（N ≥ 10 万）注意事项：
- 每轮首次扫描仍需一次全量 `Job.from_dict` 成本（约 240ms/10 万）——正常。
- 派发时 `ctx` 携带 wall/failed **全量快照**（`is_completed`/`is_failed` 语义所需）——wall 越大每次派发成本越高（wall=10 万约 6.5ms + 2.4MB pickle）。大 wall 长期运行建议定期用 `clear_history` 清理历史 wall 条目。

### 13.3 持久化

- SQLite WAL + `synchronous=FULL`（默认）：断电不丢已应答事务——enqueue 应答后的任务不会静默蒸发（NORMAL 下会）。FULL 在 WAL 下每次 commit 仅多一次 WAL fsync。
- 需要更高吞吐且接受「enqueue 应答后断电可能丢任务」时：自行子类化 backend 设 `synchronous=NORMAL`（纯持久化微基准下单事务开销可降一个数量级；端到端吞吐收益取决于 handler 执行时间占比）。
- 高吞吐（>500 job/s）下每个 commit 新建连接的代价可感知——考虑批量提交（`_cascade_fail`/`_handle_deadlock` 的 bulk 路径已合并事务）。

### 13.4 资源挂起持久化

`ctx.suspend_resource` 跨重启恢复（上限 24h）。`RateLimitResource` 的正常限速余量也会被持久化——「资源不可用状态」整体落盘，语义上无害（重启后按余量继续限速）。

---

## 14. 备份与恢复

- **state_dir 备份**：pipeline 停止后复制整个 `state_dir/`（含 `{name}_state.db` 与 `ipc/`）即可。运行中备份 DB 文件请用 SQLite 在线备份（WAL 下 `VACUUM INTO` 或 `sqlite3 .backup`）。
- **恢复**：恢复 `state_dir` 后直接 `run()`——加载期修复（契约 3）自动兜底残留漂移。
- **迁移到新机器/新 state_dir**：`seed_wall` + `seed_cursor` + 硬链接产物目录是官方通道（见 §7.2）；`output_root` 提供路径沙盒（多根）。
- **job_id 派生变更**：净化规则变更使同一内容派生新 job_id → 存量 wall 去重失效、历史内容一次性重复处理（幂等下游可吸收）——迁移前确认下游幂等。
- **无 fence 危险**：每次 run 生成新 `run_id` 并持久化（meta `last_run_id`）——结果文件带执行代标识，孤儿进程的旧结果对新 run 不可见。**不要手工篡改 meta 表**（会破坏 fence 语义）。

---

## 15. 错误码与 DLQ 字段参考

`tasklite/error_codes.py` 是错误码单一事实来源：

| 错误码 | error_type |
|--------|-----------|
| `DEPENDENCY_DEADLOCK` / `RESOURCE_DEADLOCK` / `MALFORMED_JOB` / `DEADLOCK_CLASSIFICATION_GAP` | `deadlock` |
| `JOB_DEPENDENCY` | `dependency` |
| `MAX_RETRIES_EXCEEDED` | `transient_exhausted` |
| `NO_HANDLER` | `no_handler` |
| `PAYLOAD_VALIDATION_FAILED` | `validation` |
| `COMMIT_FAILURE_DLQ` | `commit_failure` |
| `DISPATCH_FAILURE` | `dispatch` |
| `fatal: true` 标志（FatalError 路径） | `fatal` |
| 其余 / 非 dict meta | `unknown` |

新增 DLQ 写入路径前必须先在此登记错误码（AGENTS.md 纪律）。

---

## 16. 官方网络工具库（HTTP Wrappers）

> 详见架构决策记录 [`docs/adr/0001-composable-http-wrappers.md`](./adr/0001-composable-http-wrappers.md)。

TaskLite 官方提供 `tasklite.wrappers.http` 模块，为各种爬虫与网络请求业务提供轻量、组合式、正交解耦的工具库。

### 16.1 核心设计原则

1. **用户自主优先**：用户可完全自由选用 `requests`、`httpx`、`urllib` 或第三方 SDK；
2. **正交独立**：快照去重（`SnapshotStore`）与 429 异常守卫（`http_guard` / `HttpPolicy`）互不依赖，可自由组合；
3. **限速全权复用引擎**：多进程限速统一由 `RateLimitResource` 声明管理，不建冗余抽象；
4. **零外部强制依赖**：默认基于 Python 标准库，软适配 `requests`。

### 16.2 典型用法范例

#### 范例一：配合 `http_guard` 与 `RateLimitResource` 实现 429 自动挂起与退避

```python
from tasklite import TaskLite, RateLimitResource
from tasklite.wrappers.http import http_guard, fetch_urllib

pipeline = TaskLite("crawler", state_dir="./states")
# 1. 注册全局限速资源（每 2 秒最多派发 1 个请求）
pipeline.add_resource(RateLimitResource("api_twitter", interval_seconds=2.0))

def fetch_user_handler(job, ctx):
    # 2. 用 http_guard 守卫任意网络调用
    # 遇到 429 时：自动提取 Retry-After，自动执行 ctx.suspend_resource("api_twitter", ttl)，抛出 RateLimitHit
    # 遇到 5xx/超时：抛出 RetryError（消耗任务重试预算）
    # 遇到 404/403：抛出 FatalError（直送 DLQ）
    with http_guard(ctx=ctx, resource="api_twitter"):
        resp = fetch_urllib(job.payload["url"])
        return resp.json()

pipeline.register_handler("fetch_user", fetch_user_handler, default_resources={"api_twitter": 1.0})
```

#### 范例二：集成自定义 `requests.Session` 或平台专属客户端

```python
import requests
from tasklite.wrappers.http import http_guard, parse_netscape_cookies

# 加载 Mozilla/Netscape cookies.txt
cookies = parse_netscape_cookies("./cookies.txt")

def custom_api_handler(job, ctx):
    # 子进程内部创建 Session（禁止跨进程 pickle 共享 Session）
    session = requests.Session()
    session.cookies.update(cookies)
    
    with http_guard(ctx=ctx, resource="api_custom"):
        r = session.get(job.payload["url"], timeout=20)
        # 显式校验响应状态码
        if r.status_code != 200:
            raise requests.HTTPError(response=r)
        return r.json()
```

#### 范例三：使用 `SQLiteSnapshotStore` 原始 HTTP 快照实现离线幂等重放

```python
from tasklite.wrappers.http import SQLiteSnapshotStore, fetch_urllib

# 创建/打开独立的快照数据库（与 tasklite 主状态库解耦）
store = SQLiteSnapshotStore("./snapshots.db")

# 方式 A：使用 @store.cached 装饰/包装 fetch 函数（自动拦截命中或落盘）
cached_fetch = store.cached(fetch_urllib)
resp = cached_fetch("https://api.example.com/user/100")

# 方式 B：显式操作底层 CRUD
key = store.make_key("https://api.example.com/user/100", method="GET")
if not store.has(key):
    resp = fetch_urllib("https://api.example.com/user/100")
    store.put(key, url=resp.url, status_code=resp.status_code, headers=resp.headers, body=resp.body)
```

