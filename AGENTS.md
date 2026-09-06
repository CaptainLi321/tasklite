# AGENTS.md（自动加载指针与开发红线）

> 完整架构设计、状态机模型与概念词典见 **[`docs/ENGINE_ARCHITECTURE.md`](docs/ENGINE_ARCHITECTURE.md)**。
> 文档唯一总纲见 **[`README.md`](README.md)**（以及「设计契约」章节）。
> 完整 API 与运维参考见 **[`docs/API_GUIDE.md`](docs/API_GUIDE.md)**。
> 本文件定义 Agent 在本仓库开发、重构、修复时**必须绝对遵守的硬性红线与工作流纪律**。

---

## 一、核心红线与开发纪律（必须遵守）

1. **测试全绿才允许提交**：
   - 提交前必须执行全量测试：`python -m pytest tests/ -q -p no:cacheprovider`（必须 100% 绿灯，约 1170 个测试）。
   - 涉及类型注解、API 签名或模块导入修改时，必须运行多解释器矩阵：`make test-matrix`（验证 Python 3.9/3.10/3.11/3.12/3.13/3.14 兼容性与类型求值）。
   - **测试命令必须真实 exit code 判定**：不得修改 `scripts/pre-push` 让测试恒 exit 0。
2. **始终使用中文**：中文回答、中文代码注释与规范中文 Commit 提交信息。
3. **严禁混合提交（独立原子化提交）**：
   - 多个独立的问题修复、特性演进或重构，**严禁揉杂在同一个 Commit 中**；
   - 必须按功能/缺陷单元拆分为独立的原子提交（Atomic Commit），每个 Commit 仅包含该单元的实现代码及其对应的回归测试；
   - 保证每个 Commit 独立自洽，在 `git bisect` 或 `cherry-pick` 时具备独立可验证性与可回滚性；
   - 单元改完并跑通对应测试后**当场 `git commit`**，不得长期堆积滞留工作区后一次性大包提交；遇 `index.lock` 冲突 `sleep 2` 重试最多 5 次。
4. **分层单向依赖红线**：
   - `models/` 严禁反向 import `engine/`（IPC 声明读写一律下沉 `utils/ipc.py`）；
   - `utils/` 严禁反向 import `wrappers/`（无历史垫片）；
   - 核心层（`engine/`, `backend/`, `models/`, `utils/`）严禁反向依赖 `contrib/`。
5. **单射性编码红线**：
   - 所有业务标识派生复合 UID 或文件系统路径（如 `safe_uid_filename`、`sanitize_content_id`、`sanitize_job_component`），**必须使用可逆 `%XX` 百分号单射转义**；
   - 严禁丢弃式非单射净化（非单射净化会导致多对一碰撞，在 wall 去重时静默吞任务）。
6. **单一出口原则**：
   - Job 终结（成功/失败/重试）必须唯一经由 `CompletionMachine.complete_job` 收尾；
   - 失败终态登记必须唯一经由 `FailureMachine.apply_failed` 收敛（保证 wall/failed 互斥）；
   - 运行态事件钩子必须唯一经由 `RunContext.fire_*` 单一出口触发。
7. **持久化与并发事务纪律**：
   - SQLite `journal_mode=WAL` 必须在启动时验证生效（fail-loud，拒绝在断电可损坏模式下启动）；
   - `sqlite_backend.py` 中的序号分配与读-改-写操作必须在显式 `BEGIN IMMEDIATE` 写事务保护下执行。
8. **瞬态信号军规**：
   - 孤儿锁冲突（`lock_conflict`）、外部中断（`interrupted`）、限速（`RateLimitHit`）等瞬态信号，必须做到**「不烧重试预算 + 降级写盘 + 零污染」**。
9. **六步标准调用顺序**：
   - 初始化 → `add_resource` → `register_handler` / `register_discovery` / `register_transient_exception` → `enqueue` → `run` → `stop`；管理 API 仅限 `run()` 外调用。

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
- 凡是复杂并发时序、TOCTOU 闭环或极端防御逻辑，**必须优先编写确定性回归测试 / 变异测试锁定**（参考 `tests/engine/test_shutdown.py`），代码中只保留一行意图说明，严禁用大段注释替代测试。

---

## 三、文档与权威索引

- **引擎架构总览与概念词典**：[`docs/ENGINE_ARCHITECTURE.md`](docs/ENGINE_ARCHITECTURE.md)
- **文档唯一总纲**：[`README.md`](README.md)
- **完整 API 参考与运维手册**：[`docs/API_GUIDE.md`](docs/API_GUIDE.md)
- **Discovery 需求契约**：[`tasklite/wrappers/discovery.py`](tasklite/wrappers/discovery.py)
