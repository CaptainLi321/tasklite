# 0007 命名统一与持久 schema 的破坏边界

状态：已接受并实施（命名统一与 schema 4 基础已落地，后续由 [ADR-0008](0008-trigger-contract-and-trusted-success.md) 升级至 schema 5）。

框架在身份、完整 Job 快照、业务 payload、结果 metadata 与成功/失败档案之间存在同名异义及缩写。决定采用已实施定稿的命名计划精确映射，以无歧义的统一名称替换旧接口，放弃兼容别名、弃用过渡和双写；保留 Task / Job / Attempt 模型、稳定 UID 与现有行为。这一选择接受调用方一次性修改的成本，避免长期维护两套字段及误读业务数据。

## 版本与交付顺序

- 目标发布版本为 **2.2.0**，目标命名 schema 为 **4**；当前本地开发版本已切换为 2.2.0/schema 4（尚未发布）。本 ADR 不授权发布；运行时包版本在命名最终交付票统一调整，schema 4 在 SQL 切换票实现。
- N1 交付本决策、命名所有权和本地任务图。N2 先按 Python 身份、轨迹、元数据与快照的生产消费闭环拆原子票，第一代码票只改 `dispatch_seq/next_dispatch_seq→dispatch_no/next_dispatch_no`，物理身份字符串格式 `run_id.N` 不变。
- 全部 SQL 表列在单独一票中一次切换为命名计划第 2 节的完整目标形状，并将 schema 3 升级边界固定为 schema 4；不因拆 Python 票连续产生中间 schema。Python 字段与 SQL 列之间允许有显式映射，但不增加旧公开 API 兼容别名、不双写、不在同一已交付接口上接受两套字段。
- N2 完成后执行 N3A/N3B，不与重跑成功事务、trigger、产物授权或新 IPC 行为混合提交。相关权威指南随实际代码交付更新；本次不把尚未实现的目标 API 写成可用契约，不修改历史 ADR。

## 数据所有权与不变式

精确表列及全部命名目标以命名计划为准；其「框架键所有权与生产消费边界」清单是改名审查依据。

1. `Job.uid` 及 `task_type::job_id` 身份保留，跨对象 Job UID 字段/参数统一为 `job_uid`。`attempt_id`、`execution_id`、`run_id`、`activation_no`、`attempt_no` 各守原语义，不合并，也不改变百分号单射编码。
2. `job_snapshot` 表达完整 Job 序列化对象，`payload` 只表达业务输入。完整快照外层是框架契约，业务 payload 内的键和值保持原样。Job runtime 只转换已登记的框架字段，其余 extra 原样往返。
3. metadata 仍是现有混合字典，不增加结构拆分。只在明确的框架生产消费位置修改本层框架键，保留自定义 metadata、任意嵌套字典、游标名和资源名。相同拼写不证明框架所有权，严禁递归替换所有 `error/fatal/_attempt/last_run_id` 等用户键。
4. failed 原 `payload` 列是失败 metadata，原 `job_payload` 列是业务输入，分别映射为 `metadata` 与 `payload`。必须用两种内容不同的样本验证，不能靠字符串顺序替换。
5. 失败 metadata 的 `error_type` 是分类字符串，`fatal` 是布尔，分别改为 `error_category/is_fatal`；状态字符串 `fatal` 及 ErrorCategory 成员值保留。失败 metadata 的 `_attempt→attempt_no` 保留既有未知值 **0**；未知不等于真实执行，不补造轨迹、不匹配真实 AttemptRecord。Job 与 AttemptRecord 自身的尝试号仍从 1 开始。
6. 成功档案 `success_count/last_succeeded_at/last_success_run_id` 只提高名称精度，保持现有计数、setdefault 和覆盖规则；不据此宣称成功可信，也不建立新的 successes→attempts 关联。可信基线仍由 R1 独立裁决。
7. IPC 必需消息字段、执行身份参数、错误串、派生 Job 快照的生产消费两端在对应命名票同步。待退役的文件协议私有实现不大规模纯改名，留给 I2；命名不能改变锁生命周期、身份认证、瞬态降级和恢复语义。

## schema 接纳与旧库保护

SQL 切换票必须明确接受「真正的新空库」或「schema 4 且目标表形状正确的库」。schema 3 及其他旧版本明确拒绝；`user_version=0` 但已有业务表的库不能自动当作新库升级，也不能通过建目标表掩盖旧表。版本检查与表形状检查必须同时覆盖手改版本号和半建库。

本票接纳判定如下，作为上述边界的精确化：

| 实际库状态 | 判定 |
| --- | --- |
| 指定路径不存在；或合法 `user_version=0` 且无应用表、视图、触发器、索引等对象的真正空库（含只读确认的零字节文件） | 接纳，在指定路径创建六表、索引及版本 4 |
| `user_version=0` 但含任意应用对象，包括部分或完整目标表、无关表、仅 HTTP 表 | 拒绝，不补齐、不升级 |
| schema 3 或其他非 0/4 版本 | 拒绝 |
| schema 4，完整六核心表均为 table，列集合精确匹配命名计划 | 接纳；允许额外用户表，但拒绝混入异代 `wall` / `meta` 表 |
| schema 4 缺表、错列、额外核心列或同名视图 | 拒绝，不自动重建 |
| 损坏、读不出或权限错误 | 直接报错，不当空库，不改用可写连接兜底 |

现有字段类型与约束设计保留；形状检查不扩大为完整 SQL 约束审计。只读预检使用普通 `mode=ro` URI（路径经 `Path.as_uri` 编码），不得用 `immutable` 忽略 WAL。接受后在写连接设置 WAL 前复核，并在 `BEGIN IMMEDIATE` 内再次复核；空库 DDL、索引及版本写入在同一事务完成。拒绝保护指应用数据、结构、版本与 journal 模式不被初始化写 SQL 修改；不承诺 SQLite 侧文件的所有字节或 mtime 恒定，不清理 sidecar。不增加 inode fence 或跨进程迁移协议，升级前须先停止外部写入。

框架 JSON 在本票一次切换：Job 外层 `timeout_seconds`，worker 信封 `spawned_jobs/error_message`，失败 metadata 本层 `error_message/error_category/is_fatal/attempt_no`，成功 metadata 本层 `success_count/last_succeeded_at/last_success_run_id`。专属 `Job.from_dict`、`AttemptRecord.from_dict` 和消息入口明确拒绝各自旧字段，不做 alias、fallback 或双写；新字段缺省仍用既有默认值。业务 payload、runtime extra、自定义或嵌套 metadata 不扫描翻译。引擎 KV 的 `last_run_id`、状态 `error/fatal`、ErrorCategory 字符串值及 HTTP 独立 `http_snapshots` 七列和 version 0 机制保持。

已有库首先通过只读连接进行版本和形状预检；拒绝应发生在设置 `journal_mode=WAL`、建表、建索引、写版本号等持久操作之前。预检失败或不能确认版本时直接报错，不改以可写连接重试。进入接受路径后仍须复核接纳条件，防止检查与写入之间状态变化；不能把预检当作替代事务校验的依据。若外部进程同时修改或替换状态库，使用者应先停写并确认 worker 与锁归属，再升级；本 ADR 不引入跨进程库升级协议。

错误提示须包含路径、实际版本或形状、目标 schema 4 及处置建议：保留原库，使用匹配版本读取历史，或显式选择一个新的状态库路径。不自动迁移、不删除旧数据库或历史、不自动创建替代库、不让错误提示要求删除原库。不得自动清理旧 worker、锁或 IPC 残留。

接受路径继续设置并验证 WAL 生效，失败即拒绝启动；SQLite 序号分配和读改写继续受显式 `BEGIN IMMEDIATE` 保护。schema 改名不改变成功提交点、Attempt 收尾事务和降级策略；可靠成功事务另属 R2。

## 验收与后果

后续命名票须覆盖新名称调用与序列化往返、生产消费一致、用户同名键保持、failed 双载荷正确映射、旧 schema 拒绝且原库内容及 journal_mode 不变，以及新 schema 的 SQLite/MemoryBackend 契约一致。现有行为保持测试继续生效；不得借破坏版本改变重试预算、筛选、默认值、时间、清理权限或事务边界。

所有提交遵守仓库全量 pytest 门槛；类型、API 或导入变更另跑 `make test-matrix`，报告实际解释器及缺失依赖。文档契约完成不代表实现已完成；全局真实版本与公开使用示例在对应交付票对齐。
