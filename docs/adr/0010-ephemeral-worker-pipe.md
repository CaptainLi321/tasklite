# 0010 本次运行的瞬时 worker Pipe 通信

状态：**已接受并实施**。2026-11-02，用户确认 IPC 只负责本次运行的瞬时通信，选择每 worker 独立的 `multiprocessing.Pipe`，由一个专用 Module 封装传输与回收复杂度。业务 SQLite 恢复继续保留，通信消息不持久化、不接续、不补发，不增加持久收件箱或应用层 ACK。

生产实现已落位于 `tasklite/utils/ipc.py`（`WorkerPipeSender` / `WorkerPipeReceiver` / `create_worker_pipe`），并与 `ExecutionChannel`、`ArtifactJournal`、`JobContext`、`DispatchMachine`、`RecoveryOrchestrator`、`EngineRuntime` 完成全链路接线；旧文件 IPC 协议（`.result.json`、`.outputs.jsonl`、`.inputs.jsonl`、`.signals.jsonl`、`.draining`、`result_token` 及陈旧结果接续恢复）已全部退役。现行行为见 [使用手册](../GUIDE.md)；产物授权及清理遵循 [ADR-0009](0009-artifact-authorization-and-cleanup.md)。

## 已确认的范围与取舍

- 只支持单机、框架创建的 worker；Linux 优先，Windows 是目标，macOS 不在支持范围。没有当前双向请求需求，使用单向 Pipe；将来确有需求再扩展，不预建多个 Adapter 或注册框架。
- 每 worker 独立连接，避免单个 worker 的半帧阻塞全局读取。原型以每连接一条接收线程和有界内存队列隔离阻塞；该线程、缓冲和帧细节藏在 Module 的 Implementation 中。
- 所有消息统一直接使用 Pipe 原生 `send_bytes` / `recv_bytes`，不做大小分类、不设应用层大小上限、不分块/重组、不按大小选择其他传输路径。发送遇背压等待，主进程运行期间持续消费；发送完成只代表传输已接受数据，不代表业务提交或父进程确认。不增加心跳、重连或无预算的通用断线重试；发送错误、EOF、残缺帧及崩溃沿用现有超时/崩溃归因和预算规则。
- 删除 result、声明 JSONL、signals、token 等专用通信文件及陈旧结果接续协议；保留执行锁、流水线锁和现有单射身份编码。独立连接由框架传递端点，不对外开放连接，也不以此声称任意不可信本地进程可被安全隔离。
- `declare_input` / `declare_output` 的磁盘语义、返回值、授权与清理规则不重设计。声明在调用点即时发送主进程；没有父进程接纳 ACK，不承诺声明返回时主进程已经收到。尚未完整收到的声明允许丢失，不能据此扩大删除权。
- 主进程已经接收并持久化的资源挂起继续跨重启生效；未收到的 suspend 允许丢失。跨重启恢复的是业务状态，不是待交付消息。

## 小 Interface 与所有权

Module 落位于 `tasklite/utils/ipc.py`，模型可依赖它，不让 `models` 反向依赖 `engine`，核心也不依赖 `wrappers`/`contrib`。

| 调用面 | 调用者需要知道的契约 |
|---|---|
| 为一次执行创建通道并交付发送端 | 一个 worker 对应一个独立连接，绑定完整 `job_uid` 与本次 `execution_id`；主进程及时关闭自身的发送端副本，worker 关闭不需要的端点 |
| 发送消息 | worker 按顺序发送完整消息；Module 负责帧编码、发送串行化及背压。错误可观察，不静默转为成功，也不写通信文件降级 |
| 非阻塞取消息/读取通道结束或错误 | 主循环不被某连接的半帧卡住；同连接保序，跨 worker 不承诺全局顺序。连接绑定和信封身份共同校验，旧执行不能更新新执行状态 |
| 关闭通道 | 幂等释放端点和接收线程；调用者必须先结束 writer，并交付已完整接收的消息。停止标志不能打断阻塞中的半帧 `recv_bytes` |

消息仅分四类：

| 消息 | 内容与处理 |
|---|---|
| input | 本执行规范输入路径，交付主进程的执行局部声明集合 |
| output | 本执行产物声明及三项删除授权，沿用 `ArtifactDeclaration` 语义，交付执行局部集合 |
| suspend | 资源名称及挂起时长，由主进程按现有规则应用及持久化 |
| result | 终态结果与现有错误信息；spawn/cursor 随结果交付，并在既有成功事务内提交 |

终态结果排在该执行的声明之后；result 不等于提交成功。主进程唯一拥有六集合及业务状态，worker 只执行 handler 并发送消息；传输 Module 不执行清理、调度或终态提交。错误类型、身份不匹配、未知消息或损坏载荷明确拒绝，不伪造成功。

## 收尾顺序与故障边界

运行中持续读取，包括等待 worker 正常退出的阶段，避免大结果因背压阻塞而与 `join` 互等。关闭或中止时先确保 worker 结束（必要时按现有超时/强杀流程），再排空本连接能完整读取的消息，将声明和 suspend 交付相应消费者，然后按现有完成出口验证、授权清理及提交，最后关闭通道。正常成功/失败、取消和异常退出路径均不丢弃已完整接收的声明；损坏尾帧不使此前完整声明消失。

所有终结仍经 `CompletionMachine.complete_job`；失败终态及运行钩子沿用现有唯一出口。授权清理先于业务成功提交；未提交的业务允许恢复后重做，已提交业务以 SQLite 为准，不以 Pipe 的发送/接收证明提交。产物落盘并不证明执行已成功，通信消失也不授权扫描或删除未知残留。

主进程被杀不会自动杀死 worker。孤儿可能继续执行，执行锁必须严格覆盖其执行体生命周期，新主进程不能因 IPC 已消失而与同 uid 孤儿并行执行。

## 验证覆盖

生产通道与多进程生命周期行为由 `tests/utils/test_pipe_ipc.py`（单元级）与 `tests/integration/test_pipe_ipc_lifecycle.py`（真实 `spawn` 子进程端到端）锁定：

- **顺序、背压与大载荷**：顺序传递、幂等重复关闭、有界消息队列背压与恢复消费、多兆字节大帧完整收发。
- **半帧与断链隔离**：单个连接写入半帧并停住时，不阻塞其他 worker 正常交付；writer 被强杀后报告残缺帧/EOF 错误并干净回收接收线程与句柄。
- **完成事务与重开一致性**：Pipe 的声明与结果进入 `CompletionMachine`，具名产物、`inputs`、成功 `Attempt`/`finished_at`、`spawn` 与 `cursor` 在同一 SQLite 写事务中提交；已接收并持久化的资源挂起跨重启保持有效。
- **异常退出与授权清理**：真实 worker 被强杀、非零退出或超时后，主进程排空已完整接收的声明并仅删除获得失败清理授权的文件；清理原语抛错时传播环境异常、保留原队列快照与重试预算。
- **DRAINING / ABORTING 停机排空**：`DRAINING` 等待在途完成并提交声明、结果与 `suspend`；`ABORTING` 先终止 writer、再排空交付已收到的声明与 `suspend` 并执行失败清理（若已收到完整 `result` 则提交终态）。
- **协议与身份防御**：损坏 JSON、未知消息种类、错误 `job_uid`、错误 `execution_id` 均明确拒绝成功提交，此前已完整收到的声明仍按失败授权清理。
- **父死子存与孤儿锁恢复**：主进程崩溃后孤儿 worker 存活持锁期间，新 `TaskLite` 实例探测到锁冲突自动回队（不烧重试预算、不登记失败、不污染错误），且填池循环在收到停止请求时立即响应退出；孤儿回收释放锁后，原队列任务正常执行成功。
