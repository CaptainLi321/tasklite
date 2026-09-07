"""Engine subpackage for tasklite.

执行机器模块（11 个）：
- ``channel``: 子进程生命周期、看门狗与 IPC 结果处理（落盘文件模型）；
- ``scheduler``: 队列只读扫描，找下一个 runnable job；
- ``dispatch``: 派发预检关（去重/依赖/资源）与子进程派发；
- ``loop``: 事件驱动主循环（填池 → drain → 等待）；
- ``completion``: 成功/retry/失败的事务性提交与内存 apply；
- ``failure``: 3-strike 崩溃契约 / 级联 / 死锁归因 / 宽限；
- ``recovery``: 崩溃恢复 / suspend 信号排空 / abort 分类消费；
- ``runtime``: ``RunContext``（一次 run 的运行态真相源）；
- ``resource``: 资源抽象（限速/容量）与挂起语义；
- ``retry``: 退避计算与 rerun 策略判定（纯逻辑）；
- ``inflight``: in-flight job 的运行时条目（三机器共享数据类）。
"""

