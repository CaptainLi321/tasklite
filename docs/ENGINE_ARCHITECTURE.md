# tasklite 引擎架构总览与设计手册

> 模块路径：`tasklite.engine`
> 适用版本：v1.0.0+

---

## 目录

1. [架构分层与设计哲学](#一架构分层与设计哲学)
2. [核心数据流与执行闭环](#二核心数据流与执行闭环)
3. [核心状态机体系](#三核心状态机体系)
4. [执行机器职责划分（12 模块）](#四执行机器职责划分12-模块)
5. [概念词典与不变式契约](#五概念词典与不变式契约)

---

## 一、架构分层与设计哲学

`tasklite` 采用「单一宿主编排 + 职责特化机器群 + 单向共享运行态」的解耦架构：

- **单向依赖中心**：`RunContext` 承载单次 `run()` 的全部运行态（state / in_flight / stats / stop_mode / run_id / episode），机器模块从 `RunContext` 获取服务，不反向持有 `TaskLite`。
- **单一出口原则**：所有 job 终结（成功/失败/重试）统一由 `CompletionMachine.complete_job` 收尾；所有失败终态登记统一由 `StateStore.apply_failed` / `StateStore.apply_failure` 收敛；所有运行钩子统一由 `RunContext.fire_*` 出口。
- **文件 IPC 与执行身份隔离**：结果走落盘原子文件而非 `multiprocessing.Queue`（消除管道伪非阻塞卡死风险）；结果文件携带 `run_id` + `seq` 组成的 `incarnation` 执行身份隔离，避免孤儿进程结果污染新会话。
- **fail-loud 纪律**：WAL 模式必须验证生效；Job 入参严格校验；单射转义拒绝静默碰撞；未知错误码与死锁即时归因。

---

## 二、核心数据流与执行闭环

```
                   ┌──────────────────────────────────────┐
                   │           TaskLite.run()         │
                   └──────────────────┬───────────────────┘
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │   Recovery.repair_on_load │ (清理孤儿锁 / 恢复残留挂起)
                        └─────────────┬─────────────┘
                                      │
                                      ▼
             ┌─────────────────────────────────────────────────┐
             │            LoopRunner.run_loop_impl             │
             └───────────────┬─────────────────▲───────────────┘
                             │                 │
             ┌───────────────▼──────────────┐  │ (等待/轮询 drain)
             │   1. Scheduler 队列只读扫描   │  │
             │   - 找 runnable / dep_failed │  │
             │   - 死锁与畸形任务细粒度归因  │  │
             └───────────────┬──────────────┘  │
                             │                 │
             ┌───────────────▼──────────────┐  │
             │   2. Dispatch 五关预检派发    │  │
             │   - dedup / dep-failed       │  │
             │   - no-handler / orphan      │  │
             │   - acquire + submit 子进程  │  │
             └───────────────┬──────────────┘  │
                             │                 │
             ┌───────────────▼──────────────┐  │
             │   3. Executor.drain() 收集   │  │
             │   - 轮询落盘结果文件          │  │
             │   - 超时与崩溃信号检测       │  │
             └───────────────┬──────────────┘  │
                             │                 │
             ┌───────────────▼──────────────┐  │
             │   4. Completion 事务收尾     │──┘
             │   - apply_result 内存与磁盘  │
             │   - 成功 wall / 失败 DLQ     │
             │   - 释放资源 / 触发钩子      │
             └──────────────────────────────┘
```

---

## 三、核心状态机体系

### 1. 停机状态机（`StopMode`）
- `NONE`：正常执行；
- `DRAINING`：优雅停机（首次收到 `SIGTERM`/`SIGINT` 或调用 `stop()`）——停止派发新 job，等待当前 in-flight 自然完成后安全退出；
- `ABORTING`：强制停机（二次信号或 `stop(force=True)`）——对 in-flight 执行分类消费：已写好结果者正常提交，未完成者终止进程、清理半成品并 requeue 磁盘队列。

### 2. 任务生命周期转移
- `queue` $\to$ `in_flight`（派发成功） $\to$ `wall`（执行成功）
- `queue` $\to$ `in_flight` $\to$ `queue`（退避重试 / 孤儿锁延迟 / 优雅中断）
- `queue` $\to$ `failed_dlq`（达到最大重试 / 依赖失败 / 无 Handler / 畸形数据 / 确定性输入错误）

### 3. 死锁归因与宽限机理
- **畸形数据（`malformed_uids`）**：反序列化失败，最高优先级直接入 DLQ；
- **未知/超限资源（`unknown`/`impossible`）**：强制 `min_wait=inf`，仅失败肇事者，下游走正常 cascade；
- **缺失依赖（`missing_dependency_uids`）**：
  - 若队列中存在潜在 spawner，授予 `dep_grace_seconds` 宽限期；
  - 宽限期满仍缺失，判为死锁入 DLQ 并级联标记；
- **保守兜底（`deadlock_gap_rounds`）**：连续多轮无已知根因时升级整队列 DLQ（`ERR_DEADLOCK_GAP`），恢复终止性。

---

## 四、执行核心与深模块职责划分

| 模块 | 职责定位 | 核心接口 / 概念 |
|---|---|---|
| `runtime.py` | 统一运行期深模块与共享运行态容器 | `EngineRuntime`, `RunContext`, `RuntimeConfig`, `StepOutcome`, `RunSummary`, `StopMode`, `TaskStats` |
| `store.py` | 统一状态事务、3-strike 崩溃与死锁归因深模块 | `StateStore.apply_failure`, `apply_success`, `apply_retry`, `apply_skip`, `apply_bulk_failure`, `handle_deadlock` |
| `channel.py` | 子进程生命周期、阶梯看门狗、IPC 通道与产物清理深模块 | `ExecutionChannel`, `JobHandle`, `write_result_atomic`, `probe_orphan_lock`, `abort_in_flight` |
| `scheduler.py` | 队列只读扫描与不可变投影缓存 | `JobScheduler`, `JobFacts`, `ScheduleResult` |
| `dispatch.py` | 派发五关预检与子进程 submit 编排 | `DispatchMachine.dispatch_job`, `dispatch_next` |
| `loop.py` | 事件驱动主循环与异常承重网 | `LoopRunner.run_loop`, `step`, `run_loop_impl` |
| `completion.py`| 结果提交、清理、释放与恢复收尾 | `CompletionMachine.complete_job`, `apply_result` |
| `recovery.py` | 崩溃恢复、TOCTOU 闭环 abort、信号排空 | `RecoveryOrchestrator`, `RecoveryMachine.abort_in_flight`, `save_queue_crash_safe` |
| `resource.py` | 限速与容量资源抽象与挂起语义 | `Resource`, `RateLimitResource`, `CapacityResource`, `ResourceManager` |

---

## 五、概念词典与不变式契约

1. **Incarnation 身份隔离**：文件名携带 `{run_id}.{dispatch_seq}`，父进程只消费当前 incarnation 的结果，隔离孤儿进程写入。
2. **六集合互斥不变式**：`queue`、`wall`、`failed`、`in_flight`、`queue_uids`、`_rerun_active_uids` 状态严格互斥且终态单一。
3. **TOCTOU 闭环契约**：在 `abort_in_flight` 过程中，坚持「分类 $\to$ 终止未完成 $\to$ 重新探测 $\to$ 安全清理」，确保并发写入的结果不被误删。
4. **单射性编码约定**：所有从业务标识派生文件路径或复合 UID 的地方（如 `safe_uid_filename`、`sanitize_content_id`、`sanitize_job_component`），统一使用可逆百分号转义，消除命名碰撞。
5. **持久化崩溃安全**：SQLite 开启 `WAL + FULL` 模式，所有读-改-写操作严格在 `BEGIN IMMEDIATE` 显式写事务保护下完成。
