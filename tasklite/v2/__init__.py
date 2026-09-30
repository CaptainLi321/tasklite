"""TaskLite v2 子包：Task/Job/Attempt 三层模型（ADR-0004）。

模型：Task 为进程内注册的静态规格模板；Job 为一次有界激活的逻辑实例
（uid = task_type::job_id）；Attempt 为 append-only 的执行轨迹。编码族
统一 ``encode_*`` 可逆单射转义；失败终态集合称「失败档案 failed」；
重试节奏由 RequeuePolicy seam 收敛，核心不含退避与排序计算。

隔离红线：v2 严禁 import v1——``tasklite.v2`` 不依赖旧树任何模块，保证
独立演进与最终整体替换；v1 处于冻结期，仅允许缺陷修复。

公开面随实施阶段在包内各模块增长：功能对等达成前本文件不集中导出符号、
不设 __version__。分层依赖红线镜像 v1：``v2/models/`` 严禁 import
``v2/engine/``，``v2/utils/`` 严禁 import ``v2/wrappers/``，v2 核心层
严禁依赖 ``v2/contrib/``。

设计契约、命名映射表与迁移军规总纲见 docs/adr/0004-v2-parallel-rebuild.md。
"""
