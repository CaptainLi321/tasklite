# AGENTS.md（自动加载指针与开发红线）

> 公开契约唯一使用手册见 **[`docs/GUIDE.md`](docs/GUIDE.md)**（Task/Job/Attempt 三层模型与全部公开接口）。
> 项目门户与快速上手见 **[`README.md`](README.md)**；架构裁决见 **[`docs/adr/`](docs/adr/)**。
> 本文件定义 Agent 在本仓库开发、重构、修复时**必须绝对遵守的硬性红线与工作流纪律**。

---

## ADR-0011 稳定基线标准

本仓库已全面接入 [ADR-0011](docs/adr/0011-stable-interface-baseline.md) 稳定接口基线。目标行为以 ADR-0011 契约章节为准。旧 max_retries、错误分类、worker 锁、瞬态恢复、旧 Selection/Skip、生命周期回调及旧管理接口已由 ADR-0011 全面替代。

本基线严守测试／矩阵、原子提交、单向依赖、单射编码、单一收尾、WAL／写事务及业务数据保护要求。编号进度保留与结果结算规则按 ADR。代码、测试名和注释不写 ticket 编号。

**本轮 ADR-0011 修复的用户裁决**：接手本轮调查、修复或验收时，先读[执行计划](docs/tickets/adr-0011-review/plan.md)。本阶段仅用 **WSL Python 3.10** 执行类型检查及全量测试，不跑多解释器矩阵；此约定覆盖下文的矩阵要求。本轮暂不新增 Windows 原生安全删除能力，保留能力不足时的明确拒绝行为，不能将该能力列为本轮修复前置，也不能宣称 Windows 全量通过。平台缺口与后续实施条件见 [Windows 删除基线记录](docs/tickets/adr-0011-review/implementation/00-baseline-windows-deletion.md)。此裁决仅限定本轮范围，不改变项目长期平台支持承诺。

---

## 一、核心红线与开发纪律（必须遵守）

1. **测试全绿才允许提交**：
   - 提交前必须执行全量测试与静态类型检查：`python -m mypy` 与 `python -m pytest tests/ -q -p no:cacheprovider`（必须 100% 绿灯，测试数量以 `pytest --collect-only` 实测为准）。
   - 涉及类型注解、API 签名或模块导入修改时，必须运行多解释器矩阵：`make test-matrix`（包含 `mypy` 静态类型检查以及 Python 3.10/3.11/3.12/3.13/3.14 兼容性与类型求值）。
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
   - 所有业务标识派生复合 UID 或文件系统路径（如 `encode_job_uid_filename`、`encode_content_id`、`encode_job_component`），**必须使用可逆 `%XX` 百分号单射转义**；
   - 严禁丢弃式非单射净化（非单射净化会导致多对一碰撞，在 successes 去重时静默吞任务）。
6. **单一出口原则**：
   - Job 终结（成功/失败/重试）必须唯一经由 `CompletionMachine.complete_job` 收尾；
   - 失败终态登记必须唯一经由 `StateStore.apply_failure` 收敛（内存尾段经 `mark_failed_memory`，保证 successes/failed 互斥）。
7. **持久化与并发事务纪律**：
   - SQLite `journal_mode=WAL` 必须在启动时验证生效（fail-loud，拒绝在断电可损坏模式下启动）；
   - `sqlite_backend.py` 中的序号分配与读-改-写操作必须在显式 `BEGIN IMMEDIATE` 写事务保护下执行。
8. **停止、超时、授权与恢复纪律**：
   - `stop()`／Ctrl+C 只停止新派发并正常排空在途执行，不伪造 `interrupted` 结果；
   - 超时与 worker 无可信结果退出统一结算为 `failure`，Job 转 `failed`，不自动重试；
   - 授权拒绝或等待期间停止必须先结束受阻 worker，再追加 `failure` Attempt、把 Job 回到 `pending`，不消耗重试预算、不写失败档案；
   - 下次启动只能按完整执行身份把遗留 `running` 收敛为失败，保留 `pending`，不接续旧执行；库级单锁（`__tasklite__.lock`）防多实例并发写冲突。
9. **六步标准调用顺序**：
   - 初始化 → `register_resource` → `register_task` → `enqueue` → `run` → `stop`；管理 API（`set_job_status` / `clear_history` / `get_cursor` / `set_cursor` / `list_authorizations` / `revoke_authorization`）仅限 `run()` 外调用，违规由框架主动抛 `TaskLiteError`。

---

## 二、注释瘦身与防 1:1 膨胀军规（红线）

代码注释必须保持**高信息密度与自解释性**，杜绝历史包袱与噪音，严格遵守「三删、三留、三转移」：

### 1. 绝对禁止写入的注释内容（三删）
- ❌ **严禁审查编号与批次标签**：一律不得出现 `盲审-1`、`缺陷B`、`方案E`、`架构调整X`、`硬约束N`、`验证轮`、`二轮审查`、`C19`、`M5`、`D8`、`P0-1` 等标签（测试类名与 docstring 同样禁止）。
- ❌ **严禁时序与历史演进叙事**：不得在代码中写「旧实现如何……现在改为……」、「此处原本有 bug……已修复」、「从 TaskLite 迁出」、「历史垫片已砍除」等。历史决策写进 Commit message 或 CHANGELOG，不进代码。
- ❌ **严禁字面代码复读**：不写翻译代码语法的无意义废话（如 `# 遍历队列检查依赖`）。

### 2. 必须保留的高价值注释（三留）
- ✅ **不可破坏的系统级不变式（Invariants）**：如「不变式：锁生命周期严格等于主实例执行生命周期」、「状态互斥与单调自增编号」。
- ✅ **反直觉 / 非显然的设计决策（Non-obvious Decisions）**：如 `_CommitCrashSignal` 继承 `BaseException` 的理由（“防止用户 handler 的 `except Exception` 误吞崩溃信号”）。
- ✅ **单射性数学证明关键点**：如 `lockfile.py` 中「先 `%25` 后 `::` 转义以防 `t::x::y` 碰撞」。

### 3. 时序防护转移为自动化测试（三转移）
- 凡是复杂并发时序、TOCTOU 闭环或极端防御逻辑，**必须优先编写确定性回归测试 / 变异测试锁定**（参考 `tests/engine/test_concurrency.py`），代码中只保留一行意图说明，严禁用大段注释替代测试。

---

## 三、v2 模型与术语红线（转正，详见 [`docs/adr/0004-v2-parallel-rebuild.md`](docs/adr/0004-v2-parallel-rebuild.md)、[`docs/adr/0005-v2-promotion.md`](docs/adr/0005-v2-promotion.md) 与 [`docs/adr/0011-stable-interface-baseline.md`](docs/adr/0011-stable-interface-baseline.md)）

1. **术语红线**：禁用 `sanitize` / `taxonomy` / `DLQ` 旧词——编码族统一 `encode_*`，失败集合统一「失败档案 failed」（状态管理统一 `set_job_status` / `clear_history`，只读查询统一 `StateSnapshot`）。
2. **核心禁调度计算**：核心层（engine/backend/models/utils）内禁止任何业务排序与退避计算；`OrderingPolicy` / `RequeuePolicy` 是当前内部接缝，不属于本版稳定公开扩展面。候选准入的公开规则为 `Exclude`；新增或冻结调度扩展协议须另立 ADR；Job 模型不携带 priority / deadline / period 字段。
3. **三层模型语义**：Task（规格，注册于 `TaskRegistry`）/ Job（逻辑实例，`uid = task_type::job_id` 身份不变）/ Attempt（append-only 执行轨迹，旁路观测面，不参与集合互斥）；预算位收敛 `max_retries_per_run`，handler 返回值统一收敛为 `TaskResult`（`TaskResult.success` / `TaskResult.failure` / `TaskResult.retry`）。编号进度独立于可清理的队列、终态与 Attempt 记录，只随进入 `running` 单调推进。

---

## 四、文档体系与职责边界

仓库内各文档按职责严格正交划分，修改文档时须放入对应归属文件，禁止跨文档重复堆砌契约细节：

- **项目门户与快速上手（[`README.md`](README.md)）**：面向初次接入者，仅放项目定位、架构图、核心特性概览、安装与最小示例、常见场景 Recipes 速览、目录结构与文档导航；**严禁在 `README.md` 中重复堆砌底层协议、状态机规则或字段级契约**。
- **使用手册（[`docs/GUIDE.md`](docs/GUIDE.md)）**：面向深度使用者，系统定义公开导入面、Task/Job/Attempt 三层模型与数据库 Schema 6、Trigger 与恢复边界、`JobContext` 与产物清理授权、失败档案与运维 API，以及公开的 `Exclude` 运行排除规则；内部 `OrderingPolicy` / `RequeuePolicy` 只说明其非稳定边界。
- **领域概念词典（[`CONTEXT.md`](CONTEXT.md)）**：核心模型与内部组件的标准术语及禁用同义词（`_Avoid_`）。
- **架构决策记录（[`docs/adr/`](docs/adr/)）**：记录关键架构决策、演进背景与取舍依据。
- **演进路线图（[`docs/ROADMAP.md`](docs/ROADMAP.md)）**：记录已分析未实施方向（单任务直达 CLI、调度策略族；实施前须立项 ADR）。
- **Discovery 需求契约**：[`tasklite/wrappers/discovery.py`](tasklite/wrappers/discovery.py) 模块级文档。
