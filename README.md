# ⚡ TaskLite — 零依赖、进程隔离的轻量任务编排引擎

> **A Lightweight Task Orchestration Engine for Agent & Batch Pipelines.**  
> 专为 **AI Agent 批量处理**、数据同步与采集、音视频转码、离线 ETL 与后台批处理任务打造。  
> 核心解决「单线程处理耗时长」、「中途失败需全量重跑」、「子任务卡死拖垮主流程」等工程痛点。采用主控守护与物理子进程隔离架构，内置 SQLite WAL 事务持久化：已提交终态跨进程保留；异常退出遗留的 `running` 下次启动按失败收敛，未派发的 `pending` 留队，不自动接续旧执行。

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Version](https://img.shields.io/badge/version-3.0.0.dev0-orange.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-0%20external-brightgreen.svg)]()
[![ACID Persistence](https://img.shields.io/badge/persistence-SQLite%20WAL%20(ACID)-orange.svg)]()
[![Tests](https://img.shields.io/badge/tests-2600%2B%20passed-success.svg)]()

---

## 💡 为什么选择 TaskLite？

TaskLite 面向 AI Agent 批量调用、大模型推理批处理、多媒体文件处理与离线数据流转，提供**零依赖的单机任务编排能力**，通过 `pip install tasklite-engine` 即可安装。

1. **多 Worker 并发执行**：以独立子进程并行处理任务，支持配置并发数量与资源容量；
2. **持久状态防重跑**：以 SQLite WAL 事务表记录状态，已提交成功由后续 Trigger 判断是否跳过；handler 显式请求重试时按本次 Run 的预算重入队，预算耗尽后进入失败档案，减少个别失败导致的全量重跑；
3. **进程隔离与超时守护**：业务逻辑在独立子进程中运行，超时后终止执行并回收资源；已提交状态由 SQLite 持久保存，异常退出后的遗留执行按恢复规则收敛。

---

## 🏗️ 核心架构与设计哲学

```mermaid
flowchart TD
    subgraph Host ["主调度进程 (Master Scheduler) - 永不运行业务 Handler"]
        State[("SQLite WAL 状态机\n(Schema 6: successes / queue / failed / attempts)")]
        Scheduler["JobScheduler (资源调度 / 拓扑依赖)"]
        Watchdog["进程看门狗 (超时 SIGKILL / 单库级锁防护)"]
    end

    subgraph Workers ["独立 Worker 子进程 (Isolated Subprocesses)"]
        W1["Worker 1 (Handler)"]
        W2["Worker 2 (Handler)"]
        WN["Worker N (Handler)"]
    end

    Scheduler -->|"1. 检查资源 & 拓扑依赖"| State
    Scheduler -->|"2. Spawn 独立子进程 (隔离)"| Workers
    Workers -.->|"3. 独立单向 Pipe 传递声明与 TaskResult"| State
    Watchdog -->|"4. 轮询超时 SIGKILL 强杀"| Workers
    State -->|"5. BEGIN IMMEDIATE 原子收尾"| State
```

### 三层可靠性边界
1. **执行代标识与通道绑定 (Generation Fencing)**：每 worker 绑定独立单向 `Pipe` 与执行代标识，校验 `job_uid` 与 `execution_id`，旧执行与伪造消息无法污染状态机；
2. **已结算状态事务持久化**：IPC 仅负责本次运行的瞬时通信；已提交成功、失败与 `attempts` 由 SQLite 强事务持久化。异常退出遗留的 `running` 下次按失败收敛，`pending` 留队，不自动接续旧执行；
3. **单库级文件锁防护 (Library Lock Fencing)**：通过 `__tasklite__.lock` 互斥保护，同一状态库同时只允许一个活动调度实例或管理写操作，避免并发写入破坏。

---

## 🌟 核心特性

### 1. 🛡️ 进程隔离与守护 (Process Isolation)
- 主进程作为总控调度器，不执行具体的业务代码；
- 每一个任务均在独立的 `spawn` 子进程中隔离执行；
- 当子进程发生死循环或超时，看门狗将在指定超时后发送 `SIGKILL` 强杀子进程并自动回收资源；
- 子进程发生异常退出或被操作系统终止时，不影响主调度进程的稳定运行。

### 2. ⚡ SQLite WAL 状态持久化 (ACID)
- 基于 SQLite WAL + `synchronous=FULL` 模式；
- 状态机（`successes` 成功档案 / `queue` 待办 / `failed` 失败档案 / `attempts` 轨迹）在单次事务中原子转移；
- 已提交终态不会因之后异常退出被改写；遗留 `running` 以“上次执行未完成结算，实际结果未知”登记失败，不根据残留产物或迟到结果补造成功。

### 3. 🔍 函数式增量探索 (Functional Discovery)
- 针对数据同步、采集与拉取场景的无游标 (Cursor-less) 增量扫描；
- 直接复用成功档案 (`successes`) 即时判定已见内容，整页命中自动刹车，天然免疫上游记录删除、插入导致的游标漂移。

### 4. ⏸️ 内置容量与限速控制 (Resource Control)
- 内置 `IntervalResource`（调用间隔限速）与 `CapacityResource`（并发量限制）；
- 核心零调度计算，调度器以事件驱动等待与主动通知推进，避免轮询忙旋。

### 5. 🔗 DAG 拓扑依赖与级联熔断
- 任务通过 `depends_on` 声明依赖列表；
- 上游失败进入失败档案时，受影响下游按依赖规则级联失败；未执行的下游不生成 Attempt，避免无效计算。

### 6. 📦 统一文件声明与主进程清理授权
- `FileSpec` 明确文件角色（`output` / `input` / `cache`）与清理权限（`delete_existing` / `delete_on_failure` / `delete_on_success`）；
- 主进程持有排他文件授权与清理职责，收尾时先执行授权清理后提交事务。

### 7. 🎛️ 运行排除与重试节奏
- 内置候选访问序与重入队机制；其内部策略接缝不属于本版稳定公开扩展面；
- `pipeline.run(exclude=Exclude(task_type="..."))` 支持按任务类型将作业安全留队至下次运行。

---

## 🚀 快速上手 (Quickstart)

### 安装

要求 **Python ≥ 3.10**，零外部依赖：

```bash
pip install tasklite-engine
```

> **三方命名对照**：仓库名 `tasklite`，PyPI 包名 `tasklite-engine`（PyPI 的 `tasklite` 名已被无关项目占用），Python 导入名 `tasklite`——安装 `tasklite-engine`、`import tasklite`。

### 最小示例

```python
from tasklite import Job, StateSnapshot, TaskLite, TaskResult


def greet(job, ctx):
    return TaskResult.success(metadata={"message": f"你好，{job.payload['name']}"})


if __name__ == "__main__":
    with TaskLite(backend="memory") as pipeline:
        pipeline.register_task("greet", greet)
        pipeline.enqueue(Job("greet", "one", payload={"name": "TaskLite"}))
        pipeline.run()
        pipeline.stop()
        with StateSnapshot.open(pipeline) as state:
            print(state.get_job("greet::one"))
```

> 引擎采用 **Task（静态规格） / Job（逻辑实例） / Attempt（物理轨迹）** 三层模型，完整 API 签名、只读快照（`StateSnapshot`）、文件授权、状态管理与运维接口请查阅 **[`docs/GUIDE.md`](docs/GUIDE.md)**（使用手册）。

---

## 🍳 常见场景速览 (Recipes)

### Recipe 1: 增量分页扫描器 (Discovery)

无需游标，增量同步时只要发现整页内容已全部处理过，自动刹车停止扫描：

```python
from tasklite import TaskResult
from tasklite.wrappers.discovery import register_discovery

def fetch_page(job, ctx, page: int):
    return api.get_author_posts(author_id=job.payload["author_id"], page=page)

def item_id(post) -> str:
    return str(post["id"])

def process_item(job, ctx, post, content_id: str):
    ctx.spawn(Job("download", content_id, payload={"url": post["url"]}))

register_discovery(
    pipeline,
    task_type="discover_author",
    fetch_func=fetch_page,
    id_func=item_id,
    process_item_func=process_item,
    process_task_type="download",
    resources={"api": 1.0},
)
```

### Recipe 2: DAG 依赖级联与产物传递

通过 `depends_on` 声明拓扑依赖，上游成功后再执行下游，上游失败则下游自动级联阻断（完整可运行示例见 [`examples/trigger_chain.py`](examples/trigger_chain.py)）：

```python
download_job = Job("download", "video_101", payload={"url": "https://example.com/v.mp4"})
transcode_job = Job(
    "transcode",
    "video_101",
    payload={"preset": "1080p"},
    depends_on=[download_job.uid],  # 声明上游依赖
)

pipeline.enqueue(download_job)
pipeline.enqueue(transcode_job)
pipeline.run()
```

### Recipe 3: 周期限速调用与重试

```python
from tasklite import TaskResult

def fetch_handler(job, ctx):
    resp = requests.get(job.payload["url"])
    if resp.status_code == 429:
        # 请求重试并指明重试原因
        return TaskResult.retry(message="Rate limited (429), retry later")
    if resp.status_code != 200:
        return TaskResult.failure(message=f"HTTP status {resp.status_code}")
    return TaskResult.success(metadata=resp.json())
```

### Recipe 4: AI Agent / 大模型批量任务持久化

```python
import json
from pathlib import Path
from tasklite import TaskLite, Task, Job, TaskResult, FileSpec
from tasklite.resource import IntervalResource

def agent_worker(job: Job, ctx) -> TaskResult:
    result = call_llm(job.payload["prompt"])
    out_file = ctx.declare_file(
        "res",
        FileSpec(f"results/{job.job_id}.json", role="output", delete_existing=True),
    )
    out_file.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    return TaskResult.success(metadata={"tokens": result.get("usage", 0)})

pipeline = TaskLite(backend="sqlite", state_path="./agent_state.db", max_workers=8)
pipeline.authorizer = lambda p, t: True
pipeline.register_resource(IntervalResource("llm_rpm", interval_seconds=0.1))
pipeline.register_task(Task("analyze", agent_worker, resources={"llm_rpm": 1.0}))

for i in range(100):
    pipeline.enqueue(Job("analyze", f"doc_{i}", payload={"prompt": f"分析文本 {i}"}))

pipeline.run()
pipeline.stop()
pipeline.close()
```

---

## 📚 文档导航与职责划分

仓库内各文档按受众与职责严格正交划分，避免重复维护：

| 文档 | 定位与职责 |
| :--- | :--- |
| **[`README.md`](README.md)** | **项目门户与快速上手**：解决痛点、架构概览、安装与最小示例、常见场景 Recipes 速览、目录结构 |
| **[`docs/GUIDE.md`](docs/GUIDE.md)** | **使用手册**：公开导入面、三层模型与 Schema 6、`JobContext` 与统一文件声明授权、失败档案与运维 API、调度扩展接缝 |
| **[`docs/API.md`](docs/API.md)** | **公开 API 权威参考手册**：全公开类、方法、属性签名、严格 JSON 规则、返回结构与异常契约速查 |
| **[`CONTEXT.md`](CONTEXT.md)** | **领域通用语言词典**：核心领域模型与引擎内部组件的标准术语定义及禁用词表 |
| **[`docs/adr/`](docs/adr/)** | **架构决策记录（ADR）**：关键架构演进、设计取舍与边界裁决依据 |
| **[`docs/ROADMAP.md`](docs/ROADMAP.md)** | **未来演进路线图**：已分析但尚未立项实施的储备方向（实施前须立项 ADR） |
| **[`AGENTS.md`](AGENTS.md)** | **研发红线与工作流规范**：测试门槛、原子提交纪律、分层依赖红线与注释规范 |
| **[`CHANGELOG.md`](CHANGELOG.md)** | **版本变更日志**：各发布版本的显著特性与破坏性变更记录 |

---

## 📁 目录结构

```
tasklite/
├── pipeline.py          # TaskLite 核心门面与装配
├── selection.py         # 执行选择规则值对象 Exclude（候选准入 seam）
├── resource.py          # 内置资源公共导出门面 (CapacityResource, IntervalResource)
├── snapshot.py          # StateSnapshot：只读一致状态快照与标准 JSON 查询
├── exceptions.py        # 异常族 (TaskLiteError)
├── testing.py           # fake_ctx：handler 单测的官方 JobContext 构造辅助
├── engine/              # 核心调度与执行引擎群
│   ├── runtime.py       # 核心运行期深模块、主循环事件泵 (EngineRuntime)
│   ├── config.py        # RunConfig 静态装配快照、默认值唯一解析点
│   ├── session.py       # RunSession 单次 run 生命周期状态
│   ├── wait.py          # 事件泵等待/空闲决策纯函数
│   ├── types.py         # 引擎公共值对象单一真相源
│   ├── store.py         # 状态事务与失败写入隔离 (StateStore)
│   ├── ops.py           # OpsConsole：run() 外纯运维接缝（管理 API 委托目标）
│   ├── dispatch.py      # 任务派发状态机 (DispatchMachine)
│   ├── completion.py    # 任务完成与提交单一收口 (CompletionMachine)
│   ├── recovery.py      # 遗留 running 收敛与在途中止编排 (RecoveryOrchestrator)
│   ├── channel.py       # IPC 与子进程执行通道 (ExecutionChannel)
│   ├── scheduler.py     # 资源调度与 DAG 依赖 (JobScheduler / OrderingPolicy 排序 seam)
│   ├── admission.py     # 重入队节奏 (RequeuePolicy seam)
│   ├── governor.py      # 死锁归因仲裁与依赖宽限 (DeadlockGovernor)
│   ├── in_flight.py     # 在途作业租约追踪 (InFlightTracker)
│   └── resource.py      # 内置资源实现
├── backend/             # SQLite WAL 强一致事务持久化后端与内存后端
├── models/              # Task / Job / Attempt / JobContext / TaskResult / FileSpec 数据模型
├── wrappers/            # 可选工具：discovery.py（增量扫描）/ http.py（网络守卫与快照）
├── contrib/             # 生态扩展套件与参考实现（核心不反向依赖）
└── utils/               # 单射编码 (%XX 转义) / 瞬时 Pipe 通道 / 文件锁 / 时钟 / JSON 校验
```

---

## 📄 开源许可证

本项目基于 [MIT License](LICENSE) 开源。
