"""TaskLite v2 子包：Task/Job/Attempt 三层模型的并行重建（ADR-0004）。

v2 在 ``tasklite/v2/`` 下从零重写：架构照搬 v1 的机器分工与系统级不变式，
命名与模型按 ADR-0004 重塑——Task 为进程内注册的静态规格模板，Job 为
一次有界激活的逻辑实例（uid = task_type::job_id 身份不变），Attempt 为
append-only 的执行轨迹；``sanitize_*`` 家族更名为 ``encode_*`` 单射编码族；
「DLQ / 死信」术语更换为「失败档案 failed」；backoff 机制砍除，重试节奏
由 RequeuePolicy seam 接管。

隔离红线：v2 严禁 import v1——``tasklite.v2`` 不依赖旧树任何模块，保证
独立演进与最终整体替换；v1 处于冻结期，仅允许缺陷修复。

公开面随实施阶段在包内各模块增长：功能对等达成前本文件不集中导出符号、
不设 __version__。分层依赖红线镜像 v1：``v2/models/`` 严禁 import
``v2/engine/``，``v2/utils/`` 严禁 import ``v2/wrappers/``，v2 核心层
严禁依赖 ``v2/contrib/``。

设计契约、命名映射表与迁移军规总纲见 docs/adr/0004-v2-parallel-rebuild.md。
"""
