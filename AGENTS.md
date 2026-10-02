# AGENTS.md（自动加载指针与开发红线）

> 权威使用指南见 **[`docs/V2_GUIDE.md`](docs/V2_GUIDE.md)**（Task/Job/Attempt 三层模型与全部公开契约）。
> 文档唯一总纲见 **[`README.md`](README.md)**（以及「设计契约」章节）。
> 本文件定义 Agent 在本仓库开发、重构、修复时**必须绝对遵守的硬性红线与工作流纪律**。

---

## 一、核心红线与开发纪律（必须遵守）

1. **测试全绿才允许提交**：
   - 提交前必须执行全量测试：`python -m pytest tests/ -q -p no:cacheprovider`（必须 100% 绿灯，约 2200 个测试，以 `pytest --collect-only` 实测为准）。
   - 涉及类型注解、API 签名或模块导入修改时，必须运行多解释器矩阵：`make test-matrix`（验证 Python 3.10/3.11/3.12/3.13/3.14 兼容性与类型求值）。
   - **测试命令必须真实 exit code 判定**：不得修改 `scripts/pre-push` 让测试恒 exit 0。
   - **高负载时序豁免（TLE 容忍）**：宿主机后台重负载（如视频转码满载）期间，壁钟/耗时断言类用例（当前已知：`tests/engine/test_concurrency.py` 全部用例）允许抖动失败。放行条件：失败**仅限**时序敏感用例且其余测试全绿；放行时须在提交信息或交付报告注明「负载抖动放行」，负载恢复后补跑确认；本豁免严禁扩大化为忽略任何非时序失败或让测试恒过。
2. **始终使用中文**：中文回答、中文代码注释与规范中文 Commit 提交信息。
3. **严禁混合提交（独立原子化提交）**：
   - 多个独立的问题修复、特性演进或重构，**严禁揉杂在同一个 Commit 中**；
   - 必须按功能/缺陷单元拆分为独立的原子提交（Atomic Commit），每个 Commit 仅包含该单元的实现代码及其对应的回归测试；
   - 保证每个 Commit 独立自洽，在 `git bisect` 或 `cherry-pick` 时具备独立可验证性与可回滚性；
   - 单元改完并跑通对应测试后**当场 `git commit`**，不得长期堆积滞留工作区后一次性大包提交；遇 `index.lock` 冲突 `sleep 2` 重试最多 5 次。
4. **分层单向依赖红线**：
   - `tasklite/models/` 严禁反向 import `tasklite/engine/`（IPC 声明读写一律下沉 `tasklite/utils/ipc.py`）；
   - `tasklite/utils/` 严禁反向 import `tasklite/wrappers/`（无反向垫片）；
   - 核心层（`tasklite/engine/`, `tasklite/backend/`, `tasklite/models/`, `tasklite/utils/`）严禁反向依赖 `tasklite/contrib/` 与 `tasklite/wrappers/`。
5. **单射性编码红线**：
   - 所有业务标识派生复合 UID 或文件系统路径（如 `safe_uid_filename`、`encode_content_id`、`encode_job_component`），**必须使用可逆 `%XX` 百分号单射转义**；
   - 严禁丢弃式非单射净化（非单射净化会导致多对一碰撞，在 wall 去重时静默吞任务）。
6. **单一出口原则**：
   - Job 终结（成功/失败/重试）必须唯一经由 `CompletionMachine.complete_job` 收尾；
   - 失败终态登记必须唯一经由 `StateStore.apply_failure` 收敛（内存尾段经 `mark_failed_memory`，保证 wall/failed 互斥）；
   - 运行态事件钩子必须唯一经由 `RunSession.fire_*` 单一出口触发。
7. **持久化与并发事务纪律**：
   - SQLite `journal_mode=WAL` 必须在启动时验证生效（fail-loud，拒绝在断电可损坏模式下启动）；
   - `sqlite_backend.py` 中的序号分配与读-改-写操作必须在显式 `BEGIN IMMEDIATE` 写事务保护下执行。
8. **瞬态信号军规**：
   - 孤儿锁冲突（`lock_conflict`）、外部中断（`interrupted`）、限速（`RateLimitHit`）等瞬态信号，必须做到**「不烧重试预算 + 降级写盘 + 零污染」**。
9. **六步标准调用顺序**：
   - 初始化 → `register_resource` → `register_task` / `register_discovery` / `register_transient_exception` → `enqueue` → `run` → `stop`；管理 API 仅限 `run()` 外调用。

---

## 二、注释瘦身与防 1:1 膨胀军规（红线）

代码注释必须保持**高信息密度与自解释性**，杜绝历史包袱与噪音，严格遵守「三删、三留、三转移」：

### 1. 绝对禁止写入的注释内容（三删）
- ❌ **严禁审查编号与批次标签**：一律不得出现 `盲审-1`、`缺陷B`、`方案E`、`架构调整X`、`硬约束N`、`验证轮`、`二轮审查`、`C19`、`M5`、`D8`、`P0-1` 等标签（测试类名与 docstring 同样禁止）。
- ❌ **严禁时序与历史演进叙事**：不得在代码中写「旧实现如何……现在改为……」、「此处原本有 bug……已修复」、「从 TaskLite 迁出」、「历史垫片已砍除」等。历史决策写进 Commit message 或 CHANGELOG，不进代码。
- ❌ **严禁字面代码复读**：不写翻译代码语法的无意义废话（如 `# 遍历队列检查依赖`）。

### 2. 必须保留的高价值注释（三留）
- ✅ **不可破坏的系统级不变式（Invariants）**：如「不变式：锁生命周期严格等于子进程执行体生命周期」、「六集合全局互斥」。
- ✅ **反直觉 / 非显然的设计决策（Non-obvious Decisions）**：如 `_CommitCrashSignal` 继承 `BaseException` 的理由（“防止用户 handler 的 `except Exception` 误吞崩溃信号”）。
- ✅ **单射性数学证明关键点**：如 `lockfile.py` 中「先 `%25` 后 `::` 转义以防 `t::x::y` 碰撞」。

### 3. 时序防护转移为自动化测试（三转移）
- 凡是复杂并发时序、TOCTOU 闭环或极端防御逻辑，**必须优先编写确定性回归测试 / 变异测试锁定**（参考 `tests/engine/test_concurrency.py`），代码中只保留一行意图说明，严禁用大段注释替代测试。

---

## 三、v2 模型与术语红线（转正，详见 [`docs/adr/0004-v2-parallel-rebuild.md`](docs/adr/0004-v2-parallel-rebuild.md) 与 [`docs/adr/0005-v2-promotion.md`](docs/adr/0005-v2-promotion.md)）

1. **术语红线**：禁用 `sanitize` / `taxonomy` / `DLQ` 旧词——编码族统一 `encode_*`，错误分类统一 `ErrorClassifier`（`engine/errorclass.py`，`ErrorCategory` 唯一表示），失败集合统一「失败档案 failed」（`FailureEntry` / `list_failures` / `clear_failures` / `retry_failure`）。
2. **核心禁调度计算**：核心层（engine/backend/models/utils）内禁止任何排序计算与退避计算（backoff 全族已砍除）；重试节奏与候选排序一律经 OrderingPolicy / RequeuePolicy seam 以 wrapper/util 形态扩展，**seam 扩展须另立 ADR**；Job 模型不携带 priority / deadline / period 字段。
3. **三层模型语义**：Task（规格，注册于 `TaskRegistry`）/ Job（逻辑实例，`uid = task_type::job_id` 身份不变）/ Attempt（append-only 执行轨迹，旁路观测面，不参与六集合互斥）；`retries` 构造参数已废除，预算位收敛 `max_retries`。

---

## 四、文档与权威索引

- **权威使用指南**：[`docs/V2_GUIDE.md`](docs/V2_GUIDE.md)（Task/Job/Attempt 三层模型、六步契约、失败档案、调度 seam 与 v1→v2 命名映射）
- **文档唯一总纲**：[`README.md`](README.md)
- **架构决策记录（ADR）**：[`docs/adr/`](docs/adr/)（v2 重建总纲：ADR-0004；上位与 v1 退役：ADR-0005）
- **领域概念词典**：[`CONTEXT.md`](CONTEXT.md)
- **Discovery 需求契约**：[`tasklite/wrappers/discovery.py`](tasklite/wrappers/discovery.py)
- **v1 历史归档**（描述已退役的 v1 公开面，以 git 历史为真相）：[`docs/API_GUIDE.md`](docs/API_GUIDE.md)、[`docs/ENGINE_ARCHITECTURE.md`](docs/ENGINE_ARCHITECTURE.md)
