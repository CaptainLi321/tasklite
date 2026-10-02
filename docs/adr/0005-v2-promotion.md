# 0005 v2 上位：v1 退役与主包路径提升

## 上下文与决策动因

ADR-0004 设定的上位判据已全部达成：

- **功能对等**：对等审计通过——门面方法面 1:1（含增强项 `retry_failure`/`RunSummary`）、backend 面为 v1 超集、context/hooks/testing/wrappers 等价迁移；测试覆盖缺口经修复批 4 补齐（perf/并发/归因对称性/端到端场景）。
- **质量关账**：双路架构审查（深度审查 × 对等审计）发现的问题经修复批 1–6 全部处置（单一出口收敛、结构契约实名化、backend 单一锚点、死面清扫、文档契约测试化）。
- **净体验证**：无负载环境全量 4102 passed 零豁免零抖动，五解释器（3.10–3.14）矩阵通过；v2 覆盖率 94.32%。
- **双包过渡税**（维护面/测试面/门禁双轨、`import tasklite.v2` 触发 v1 面加载）持续存在，唯一消解路径即上位。

库主裁决（2026-10-02）：**本仓库直接升级为 v2**；转码管线暂不适配。注意：本机 `import tasklite` 经仓库路径实时解析（site-packages 为壳），上位后未适配的 v1 消费方在下一次运行前须适配（见 ADR-0004 映射表与 V2_GUIDE），或临时 `git checkout v1-final` 标签运行旧版。

## 裁决

1. **物理上位**：`tasklite/v2/*` 内容提升为 `tasklite/` 主包；v1 树移除（git 历史与 `v1-final` 标签留档）；`tasklite.v2` 导入路径随之消亡，全库引用（含 tests/、docs 示例、scripts/）重写为 `tasklite.*`，**不留路径别名**（「公开面不留别名」军规的路径版）。v2 内部本为相对导入，代码零改写；上位为**单个原子提交**（repo 世界切换不可半应用，git bisect 语义完整）。
2. **测试树上移**：v1 测试全部移除；`tests/v2/*` 上移为 `tests/*`（engine/models/backend/utils/wrappers/integration/perf 结构保持）；`conftest.py`/`helpers.py` 适配（v2 与 v1 同名模块路径使多数导入自然成立，逐条核实语义）；hygiene（审查编号/标识符形态/job_id 确定性/文档死链）作用域自然延续。
3. **版本与发行**：`__version__ = "2.0.0"`；CHANGELOG `[Unreleased]` → `[2.0.0]`。破坏性面向 v1 用户如实声明：v1 公开面移除、SQLite schema 全新（user_version=3，旧库 fail-loud 拒识）、布尔参数 keyword-only、错误码值串变更（`"COMMIT_FAILURE_DLQ"→"COMMIT_FAILURE"`）、SystemExit 契约显式化——迁移映射见 ADR-0004。
4. **文档治理**：README 总纲 v2 化；`V2_GUIDE.md` 为权威使用指南（导入路径重写）；v1 时代的 `API_GUIDE.md`/`ENGINE_ARCHITECTURE.md` 头部加「v1 历史归档」声明并从权威索引摘除（内容留档，git 历史即真相）；`CONTEXT.md` 词汇表 v2 化（Task 不再列入 Avoid、新增 Attempt/失败档案/ErrorClassifier/encode 编码族词条）；AGENTS.md「v2 双包期红线」转正为单包红线（术语纪律、核心无调度/退避计算、分层镜像、TLE 清单路径合并），移除双包隔离条款。
5. **工具链同步**：`scripts/verify_matrix.py` 导入面切换；pyproject coverage `source` 与 mutmut `source_paths` 更新为上位后路径。

## 后果与重开条件

- 未适配的 v1 消费方（转码管线）上位即断：适配前以 `v1-final` 标签运行，或按 ADR-0004 映射表迁移（主要触点：`register_handler→register_task`、`add_resource→register_resource`、`list_dlq/clear_dlq→list_failures/clear_failures`、`sanitize_*→encode_*`、`TaskContext→JobContext`、钩子签名值对象化）。
- SystemExit 错误通道契约、调度 seam 立法（OrderingPolicy/RequeuePolicy 已上提门面；**策略实现与 Job 调度属性存放仍须另立 ADR**）原样延续。
- 「v1 绝对冻结停止修补」的先前裁决随 v1 移除自然终止；v1 的历史问题以 git 历史 + 归档文档为最终处置。
- 本 ADR 为终态裁决，无重开条件；后续架构演进（调度策略、运维面扩展）各自另立 ADR。
