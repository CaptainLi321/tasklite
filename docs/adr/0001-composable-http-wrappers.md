# 0001 组合式轻量 HTTP 工具与正交快照守卫设计

## 上下文
下游业务项目普遍存在 HTTP 网络请求、429 限流退避、Netscape Cookies 解析与原始响应快照缓存需求。由于不同平台对于请求库（`urllib`、`requests`、`httpx`、自定义签名 SDK）偏好各异，若由 TaskLite 强行抽象重量级统管客户端，会破坏用户自主权并增加进程间 Session 共享隐患。

## 架构决策
1. **拒绝重造统一客户端抽象层**：采用开放 Callable 与组合式工具设计，用户完全自主决定请求库，TaskLite 仅提供轻量组合积木；
2. **去重与 429 防护严格正交解耦**：
   - `SnapshotStore`（快照去重）：专职内容寻址的原始响应持久化与幂等离线重放，零引擎依赖；
   - `http_guard` / `HttpPolicy`（429 与异常守卫）：专职解析 `Retry-After`、调用 `ctx.suspend_resource` 并收敛为 TaskLite 三分类异常（`RateLimitHit` / `RetryError` / `FatalError`）；
3. **限速全权复用核心机制**：不建独立的 Pacer，跨进程多 Worker 限速完全依托 TaskLite 现有的 `RateLimitResource` 与 `ResourceManager`。

## 架构后果
- 保持子进程执行体完全独立自洽，杜绝跨进程套接字句柄污染；
- 用户代码与 TaskLite 引擎三分类调度契约无缝闭环，同时保留 100% 网络层自主权；
- 零强制外部依赖（标准库即可运行，软适配 `requests`）。
