# 0001 组合式轻量 HTTP 工具与正交快照守卫设计

## 上下文与业务痛点
在 TaskLite 下游众多批处理与采集生态仓库中，网络请求与 API 交互是最高频的操作。长期以来，各项目在网络层存在如下痛点与重复劳动：
1. **429 速率限制退避逻辑碎片化**：不同下游各自手写 `time.sleep`、`Retry-After` 解析与异常抛出，容易误烧重试预算或造成子进程占死 worker 槽位；
2. **Netscape `cookies.txt` 解析重复造轮子**：多个项目重复手写基于 `MozillaCookieJar` 或文本行分割的 cookie 解析与覆盖逻辑；
3. **缺少统一的请求幂等快照（Raw HTTP Snapshots）**：爬虫调试或增量重跑时，重复向源站发送相同的静态 GET 请求，增加风控封禁风险；
4. **底层库选型差异巨大**：部分平台依赖 `urllib`，部分重度依赖 `requests.Session` 并挂载复杂签名与受保护头（如抖音 `a_bogus`/`uifid`），另有部分依赖 `httpx` 或 GraphQL 客户端。

---

## 架构决策

### 1. 坚决不重造统一重量级客户端抽象层（拒绝 monolithic HttpClient）
- **裁决**：TaskLite 不提供包装所有网络库的重型单体 Client，而是提供**开放 Callable 与组合式积木（Composable Blocks）**；
- **理由**：
  - 强行统一 `requests`、`httpx`、`urllib` 的配置项（连接池、SSL 上下文、代理、重定向规则）会导致 TaskLite 膨胀为重型 HTTP SDK，违背轻量嵌入式初心；
  - 跨进程传递 `Session` 对象极易引发套接字锁死或 pickle 序列化崩溃；
  - 开放 Callable 设计赋予用户 100% 网络选型自主权，用户代码可直接使用平台专用 SDK。

### 2. 快照去重（SnapshotStore）与 429 守卫（http_guard）严格正交解耦
- **裁决**：去重与异常守卫彻底拆为两个正交独立模块：
  - **`SnapshotStore`**：专职负责内容寻址（URL + Method + Query + Body 单射哈希）的原始响应持久化与离线幂等重放。零引擎依赖，可在独立离线分析脚本中单用；
  - **`http_guard` / `HttpPolicy`**：专职将底层网络故障与状态码收敛为 TaskLite 三分类异常（`RateLimitHit` / `RetryError` / `FatalError`），并负责 429 `Retry-After` 解析与 `ctx.suspend_resource` 资源挂起。
- **理由**：并非所有需要 429 防护的请求都适合做快照（例如动态变动的搜索接口、状态查询），反之亦然。正交设计允许用户自由按需组合。

### 3. 限速机制全权复用 TaskLite 核心 `RateLimitResource`
- **裁决**：不新建独立的 `Pacer` 类或进程内节拍器，限速统一由 TaskLite 原生 `RateLimitResource` 与 `ResourceManager` 提供调度保障；
- **理由**：TaskLite 已经在调度器层提供了跨多 Worker、跨物理子进程的精确令牌桶限速，且支持 429 挂起状态跨崩溃持久化。在网络库重复造 Pacer 会导致单进程节拍与全局调度的概念冗余和冲突。

---

## 架构后果与收益

1. **子进程隔离与零套接字污染**：
   - 每个 Worker 子进程在自身执行体内部独立创建/回收会话，杜绝跨进程 IPC 序列化风险；
2. **三分类调度闭环**：
   - 429 触发 `RateLimitHit` + `ctx.suspend_resource`，做到**不烧重试预算、全管线同步退避、零污染**；
   - 5xx/超时触发 `RetryError`，走主调度器指数退避；
   - 4xx 触发 `FatalError`，直送 DLQ 归档，防止空耗资源。
3. **零外部强制依赖**：
   - 基于标准库即可完整运行，软支持 `requests`。
