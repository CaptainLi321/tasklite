# 执行触发时机调研（2026-10）

访问日期：2026-10-25。本文是官方证据与推断边界记录，不是 ADR，不变更 TaskLite 契约。TaskLite 已决定继续自研；本次不评估迁移，也不将自动关联、级联其他 Job 作为目标。

## 阅读边界

本文将三个时点分开观察：**条件求值**（工作是否过期或到期）、**请求接收**（执行需求是否被接受及保存）、**执行**（何时获得资源实际启动）。防止同身份并发执行，不等于保证运行期间收到更新后再执行一次。文中的“尾随补跑”仅指后一种保证；它也不等于失败重试、错过执行时间后的补执行或事件窗口去重。

| 项目与证据基线 | 条件求值与准入 | 运行期间的新需求 | 同身份忙时请求的边界 |
| --- | --- | --- | --- |
| Luigi 3.8.1 官方文档及内嵌源码 | 准入时调用 `complete()`；默认检查输出存在 | 其他 worker 可向中央 scheduler 登记；动态依赖由 `yield` 准入 | 合并同一 `task_id`，不保证结束后补跑 |
| Snakemake 9.27.1 官方文档 | 构图计算需要执行的 job；checkpoint 可重新求值 | 可选 `from_queue` 接收输入路径流 | DAG 身份复用；重复路径不代表额外执行次数 |
| doit 0.37.0 与 doit-auto1 0.1.0 | 依赖完成后、actions 前懒检查状态 | auto 在批次之间通过监听启动下一批 | 忙时变化合并为下一批机会，不是可靠事件收件箱 |
| Prefect v3 滚动官方文档 | 事件规则或计划创建独立 Run | worker 稍后领取 Scheduled Run | ENQUEUE 等待或 CANCEL_NEW；不是稳定身份的忙时更新标记 |
| APScheduler 3.11.3 | 周期 trigger 计算到期时刻 | 运行中 `add_job()` 可唤醒 scheduler | `max_instances` 超限跳过到期执行；coalesce 不保证尾随补跑 |

下文将官方明示行为、可选模式、基于源码的推断分别说明。未核验的第三方 watcher、daemon 包装及插件不视为项目标准行为。

## Luigi：准入时检查完成度，中央调度器负责协调执行

证据基线是标注为 **3.8.1** 的 stable 文档及其内嵌源码。本次未成功获取对应 Git tag，故不声称已经固定到某个 Git SHA。

### 标准行为与判断时点

`Worker.add(task)` 先调用 `complete()`：已完成的任务登记为 DONE，否则展开依赖并登记 PENDING。默认 `Task.complete()` 只判断所有输出 Target 是否存在，不比较输入 mtime、内容或代码变化；业务可以覆盖完成判据。[Task 源码](https://luigi.readthedocs.io/en/stable/_modules/luigi/task.html#Task.complete)、[Worker.add 源码](https://luigi.readthedocs.io/en/stable/_modules/luigi/worker.html#Worker.add)

执行前默认再检查依赖是否完成，用于发现输出丢失等不一致；缺失依赖可以重新准入，次数受 `max_reschedules` 限制。这不是全局扫描，也不是默认重新检查当前任务输入是否更新。任务正常返回默认即登记 DONE；可选 `check_complete_on_run=true` 才在结束后检查完成判据。可选 `cache_task_completion=true` 会缓存成功判定，明确假定完成状态不会再变化。[配置](https://luigi.readthedocs.io/en/stable/configuration.html)

中央 scheduler 本身不执行任务，也不自动触发周期任务；官方建议由 cron 或持续运行的外部进程调用工作流。其他 worker 可以在当前工作流执行期间登记新任务，但不能据此承诺并发调用同一个 Worker 对象的线程安全；Worker 模块大部分实现是私有接口。[中央 scheduler](https://luigi.readthedocs.io/en/stable/central_scheduler.html)、[执行模型](https://luigi.readthedocs.io/en/stable/execution_model.html)、[Worker 模块说明](https://luigi.readthedocs.io/en/stable/_modules/luigi/worker.html)

### 重复身份、动态准入与结束

scheduler 按 `task_id` 合并任务。运行中的任务不能被其他 worker 的 PENDING 登记重新排队；源码没有为此记录一次结束后的执行欠账。DONE 后短期还可能通过 `stable_done_cooldown_secs` 忽略 PENDING 报告。因此，防并发重复不能推导为尾随补跑保证。[Scheduler.add_task 源码](https://luigi.readthedocs.io/en/stable/_modules/luigi/scheduler.html#Scheduler.add_task)

`run()` 中 `yield OtherTask()` 会检查动态依赖；未完成则摄入并挂起父任务，父任务以后从 `run()` 开头重新执行，要求幂等。它与 TaskLite 父任务完成后摄入 spawn 的语义不同。[动态依赖](https://luigi.readthedocs.io/en/latest/tasks.html#dynamic-dependencies)

普通 worker 默认在没有可执行任务、也没有本地在途任务时退出。可选 `keep_alive` 主要为仍有 pending 的情况保活；assistant 配合 keep_alive 可以持续等待。可选 `retry_external_tasks` 能在运行期间重查原本不完整的外部依赖，默认关闭，且未完成检查会进入其失败与重试逻辑；这不等于监视已完成任务的输入。[配置](https://luigi.readthedocs.io/en/stable/configuration.html)、[Worker.run 源码](https://luigi.readthedocs.io/en/stable/_modules/luigi/worker.html#Worker.run)

### 时间线与可借鉴边界

以下例子由上述契约推导：09:00 提交 `Report(day=D)`，输出已存在便跳过；09:01 修改源数据，默认存在判据仍为真。若任务正在执行，09:02 第二 worker 再提交相同身份，scheduler 保持单个执行；09:05 成功后不会因为第二次提交自动再执行。09:10 外部重新调用工作流，并通过自定义 `complete()` 判定过期，才可能开始新一轮，还需考虑 DONE 冷却。

对 TaskLite 可借鉴的是明确记录“谁发现变化、何时重新准入、如何处理运行中重复身份”；不能用 Luigi 的共享去重证明更新不会丢失。

## Snakemake：增量规划与显式运行期输入入口

官方文档固定到 **9.27.1**；[发布记录](https://github.com/snakemake/snakemake/releases/tag/v9.27.1) 标明 2026-10-21 发布，对应提交 `f79d9eb5e2b679e1191818f24335907b041578be`。固定 tag 源码抓取未成功；本次读到的 DAG 源码来自浮动 main，仅用于印证实现结构，不视为 9.27.1 的精确实现快照。

### 标准判断与运行期变化

常规调用依据目标构建 DAG，计算哪些 job 需要执行。默认重跑触发包括 mtime、输入集合、参数、代码与软件环境；也可缩窄触发器或显式 force。`input` 变化不等于持续对所有输入内容做哈希；小文件 checksum 可以避免单纯更新时间引发重跑，阈值由 `--max-checksum-file-size` 控制。[固定版本 CLI](https://snakemake.readthedocs.io/en/v9.27.1/executing/cli.html)

DAG 源码将 needrun 更新放在构图及拓扑更新处理，并维护 running/finished 集合。由这些实现和公开接口范围可推断：普通模式不能承诺持续监听任意外部变化，并在同一 job 成功后自动再执行。可靠的重新规划边界是下一次调用；这不是对所有插件能力的穷尽否定。[DAG 源码（浮动 main）](https://raw.githubusercontent.com/snakemake/snakemake/refs/heads/main/src/snakemake/dag.py)

### 两种官方动态准入模式

- **checkpoint**：完成后重新求值相关 input function，以输出结果决定后续需要哪些 job。这是声明的数据依赖准入点。[checkpoint 文档](https://snakemake.readthedocs.io/en/v9.27.1/snakefiles/rules.html#data-dependent-conditional-execution)
- **`from_queue`，从 8.2 起提供**：指定规则通过同步 Python queue 接受新输入路径，规则自身不能含 wildcards；到达条目会更新输入与 DAG。调用方负责把外部消息接到该 queue，它不是远程通用 enqueue 服务。必须定义结束 sentinel，收到后等待剩余工作完成再退出。[连续输入文档](https://snakemake.readthedocs.io/en/v9.27.1/snakefiles/rules.html#continuously-updated-input)

单个 DAG 会复用同规则、同 wildcards 的 job，并对重复输入文件去重。没有查到 `from_queue` 对运行中同一路径重复通知提供尾随补跑保证，不能把两次 put 解读为两次执行请求。另起进程也不等于向现有运行热入队：默认有工作目录锁，`--nolock` 只关闭锁，不提供共享请求合并协议。[DAG 源码（浮动 main）](https://raw.githubusercontent.com/snakemake/snakemake/refs/heads/main/src/snakemake/dag.py)、[CLI 锁选项](https://snakemake.readthedocs.io/en/v9.27.1/executing/cli.html#behavior)

### 时间线与可借鉴边界

普通模式例子：09:00 构图发现输入更新，执行 `result.txt` 对应 job；09:01 执行期间输入再次变化，不能承诺09:05成功后自动补跑；09:10重新调用时重新规划。若最终输出时间更新，仅靠 mtime 也不能证明执行读到了最终内容，这属于推断边界。

`from_queue` 例子：09:00 启动接入 queue 的工作流；09:01 put `test1.txt`，09:02 put `test2.txt`，分别将必要工作纳入；09:03 put sentinel，完成已有工作后退出。同一路径重复出现没有公开的额外执行次数保证。

TaskLite 可借鉴显式开放准入与显式结束信号，也可借鉴在明确完成点重新求值的边界；无需引入其他 Job 的自动关联或级联。

## Prefect：创建独立 Run，再由 worker 消费

证据基线是访问当日的 **v3 滚动官方文档**，不固定具体小版本或源码提交。

事件触发规则或 deployment 计划产生独立 Scheduled Run，worker 稍后消费；条件命中、Run 创建和实际启动因此可以分开观察。[事件触发器](https://docs.prefect.io/v3/concepts/event-triggers)、[Deployments](https://docs.prefect.io/v3/concepts/deployments)

deployment 并发上限需要配置；配置后默认碰撞策略为 ENQUEUE 等待，也可选择 CANCEL_NEW。事件 `within` 窗口决定是否创建第二个 Run；Run 已经创建后，等待或取消才由并发策略决定。不能据此推导“同身份运行期标记更新，完成后合并补跑一次”。[Deployments](https://docs.prefect.io/v3/concepts/deployments)、[事件防抖](https://docs.prefect.io/v3/advanced/debouncing-events)

事件 HTTP 接口接受请求、publish 消息，不等于事件已经写入事件数据库；而 Scheduled Run 创建 API 有数据库事务，两种接收边界不能混用，也不据此承诺端到端 exactly-once。[事件 API 实现参考](https://reference.prefect.io/prefect/server/api/events/)、[Deployment API 实现参考](https://reference.prefect.io/prefect/server/api/deployments/)

时间线例子：09:00 一个事件规则命中并创建 Run A；09:01 worker 启动 A；09:02 新事件只有满足触发器窗口等条件才会创建 B。若创建 B 且已配置的 deployment 并发上限被占满，B 等待或取消取决于碰撞策略，而不是把 A 的稳定身份标记为有更新。此例不承诺事件必达、持久提交时点或精确执行一次。

对 TaskLite 可借鉴的是把接收回执、持久化承诺与执行准入分开；如果采用稳定 uid，也不能直接套用独立 Run 的排队语义。

## APScheduler：到期扫描、实例限制与错过时刻的合并

证据基线固定为 **3.11.3** 源码，搭配官方 3.x 文档；不混用 4.x 的任务模型。[用户指南](https://apscheduler.readthedocs.io/en/3.x/userguide.html)、[3.11.3 scheduler 源码](https://raw.githubusercontent.com/agronholm/apscheduler/3.11.3/src/apscheduler/schedulers/base.py)

周期 trigger 计算 `next_run_time`；scheduler 扫描到期工作后交给 executor。`max_instances` 默认 1，忙时到期执行被跳过且后续时间继续推进，不保证原执行结束后补跑一次。默认 `coalesce=True`，合并的是扫描时发现的多个到期时刻，不是运行中记录一次更新；默认 `misfire_grace_time` 为 1 秒，过期执行仍受此限制。[用户指南](https://apscheduler.readthedocs.io/en/3.x/userguide.html)、[3.11.3 scheduler 源码](https://raw.githubusercontent.com/agronholm/apscheduler/3.11.3/src/apscheduler/schedulers/base.py)

运行期间 `add_job()` 可以唤醒 scheduler；默认使用内存 JobStore，跨重启恢复计划需要持久 JobStore，恢复后的错过执行仍受 grace 限制。启动前的 pending job 不能仅凭“已 add”就视为获得持久保存保证。这里的 Job 是计划身份，与 Prefect 每个 Run 的独立执行身份不同。[BaseScheduler API](https://apscheduler.readthedocs.io/en/3.x/modules/schedulers/base.html)、[3.11.3 scheduler 源码](https://raw.githubusercontent.com/agronholm/apscheduler/3.11.3/src/apscheduler/schedulers/base.py)

时间线例子：每分钟到期的 job 在09:00启动并运行90秒；09:01到期时因单实例限制跳过，09:01:30结束后不会立即补执行，09:02再由下一次到期触发。若 scheduler 停顿后一次扫描发现多个过去时刻，coalesce 才负责合并这些时刻；还要遵守 misfire 等配置。

对 TaskLite 可借鉴的是为“到期但繁忙”的处理提供明确结果；不能把实例限制或 coalesce 描述为更新保留机制。

## doit

核心源码固定到 **doit 0.37.0**；自动监听部分独立检查官方发布包 **doit-auto1 0.1.0** 的 `cmd_auto.py`，不将其与核心 runner 混为一层。[Runner 源码](https://github.com/pydoit/doit/blob/0.37.0/doit/runner.py#L91)、[依赖判定源码](https://github.com/pydoit/doit/blob/0.37.0/doit/dependency.py#L484)、[auto 官方文档](https://pydoit.org/cmd-other.html#auto-watch)、[auto 发布包](https://pypi.org/project/doit-auto1/#files)

`Runner.select_task` 在依赖执行后、actions 执行前调用 `get_status`，因此是批内懒检查，不是持续轮询已经完成的任务。动态任务可由 delayed task creation 进入任务生成过程；这也不表示运行中的 DAG 对外暴露通用热入队服务。[Runner 源码](https://github.com/pydoit/doit/blob/0.37.0/doit/runner.py#L91)、[动态任务创建](https://pydoit.org/task-creation.html#delayed-task-creation)

auto 先执行初始一批；之后每次重新运行都启动新进程、重新加载 dodo。监听事件不会直接强制任务过期，下一批仍然依据 `uptodate`、`file_dep` 等判定。运行中发生的外部变化不会直接热入当前 DAG。[auto 官方文档](https://pydoit.org/cmd-other.html#auto-watch)、[auto 发布包中的 cmd_auto.py](https://pypi.org/project/doit-auto1/#files)

批末，auto 检查非 targets 的监听路径是否有 mtime 晚于本批开始时间；命中则立即开启下一批，否则建立 watcher，首次匹配事件触发下一批。因此多次变化可以合并成下一批机会，并不保证每个事件各执行一次。mtime 恢复或时间粒度、targets 排除以及 stat 检查到 watcher 建立之间的窗口，都限制了不漏变化的保证，不能视为可靠收件箱。官方也专门讨论了文件变化与任务执行之间的时序边界。[auto 发布包中的 cmd_auto.py](https://pypi.org/project/doit-auto1/#files)、[依赖与 targets 的时序说明](https://pydoit.org/tasks.html#dependencies-targets)

时间线例子：09:00 启动一批，某任务在依赖完成后检查状态并执行；09:01执行期间修改受监听输入多次；09:02批末若发现其 mtime 晚于本批开始，则开启一个新进程重新加载任务，再按状态判据决定哪些任务执行。若没有发现变化，则开始监听后续事件；不承诺每次编辑都独立执行，也不承诺检查与监听交接绝无遗漏。

对 TaskLite 可借鉴的是批内懒检查与批外监听的职责划分。跨批可保持稳定 `task.name`，不必把一次变化编码成新身份；这里不引申为自动关联其他 Job，也不将 watcher 当作持久请求队列。

## GNU make：先更新前置条件，再按文件时间判断目标

证据基线为 GNU 官方手册及 **GNU make 4.4.1** 的 `src/remake.c`。以下限定普通文件目标；phony、FORCE、强制执行选项和特殊中间文件另有规则，不将它们混入普通文件的过期判据。

### 普通文件与判断顺序

普通目标不存在，或者任一 normal prerequisite 的修改时间严格晚于目标时，需要更新；相同时间不算更晚。make 先处理目标的前置条件，再判断是否执行目标的 recipe，不以“上游执行过”直接代替过期判断。源码 `check_dep` 先调用 `update_file`，再读取前置文件时间与目标时间比较。[手册 §2.3](https://www.gnu.org/software/make/manual/html_node/How-Make-Works.html)、[手册 §4](https://www.gnu.org/s/make/manual/html_node/Rules.html)、[4.4.1 remake.c](https://github.com/mirror/make/blob/4.4.1/src/remake.c#L1005)

因此，A 成功执行但产物时间未推进，不会仅因执行行为强制 B 更新；内容相同但 touch 到晚于 B 输出，仍会令 B 过期。内容改变却保留时间，make 可能看不见。这里比较的是输入与输出的时间关系，不是内容 hash，也不是输入相对于上次成功执行的变化。[手册 §4.3](https://www.gnu.org/s/make/manual/html_node/Prerequisite-Types.html)、[4.4.1 remake.c](https://github.com/mirror/make/blob/4.4.1/src/remake.c#L1017)

### 顺序关系、特殊目标与文件边界

normal prerequisite 同时表达执行顺序和过期关系；order-only prerequisite 只表达执行顺序，不参与目标是否过期的判断，即使它被标记为 phony 也不会因此令目标重建。这可以类比 `depends_on` 的执行门槛与 `declare_input` 的数据关系分工，但不能直接等同：TaskLite 仍有自己的成功历史、失败档案、Selection 和依赖归因契约。[手册 §4.3](https://www.gnu.org/s/make/manual/html_node/Prerequisite-Types.html)

- 普通目标存在且没有前置条件时，在无其他强制条件下不执行；目标缺失则需要构建。phony 不受同名文件存在影响；不存在且无 recipe、无前置条件的显式 FORCE 目标可以强制其依赖方执行。这些特殊规则不能自动映射为 TaskLite 的默认重跑策略。[手册 §4.6](https://www.gnu.org/s/make/manual/html_node/Phony-Targets.html)、[手册 §4.7](https://www.gnu.org/s/make/manual/make.html#Force-Targets)
- 缺失输入若有适用规则，make 会尝试构建；既缺失又无规则则报错。TaskLite 不因此获得自动发现或创建未入批上游 Job 的语义。[错误消息附录](https://www.gnu.org/software/make/manual/make.html#Error-Messages)
- `a b:` 表示独立目标；`a b &:` 表示同一次 recipe 产生的一组输出。组内任一目标缺失或过期便需要处理；实现取已有组内最旧时间参与判断。一个 Job 产生多输出更接近后一种形态，但采用它需要知道完整输出集合。[手册 §4.10](https://www.gnu.org/s/make/manual/html_node/Multiple-Targets.html)、[4.4.1 remake.c](https://github.com/mirror/make/blob/4.4.1/src/remake.c#L492)
- 低精度时间可能把真实变化压进相同时间值；`.LOW_RESOLUTION_TIME` 针对输出时间截断造成的误判作特殊处理，不能提供内容变化识别保证。[手册 §4.9](https://www.gnu.org/s/make/manual/html_node/Special-Targets.html)

### 与 TaskLite 历史输入指纹的算法差别

当前 TaskLite `on_input_change` 将输入的当前 `(size, mtime_ns)` 与 wall 中上次成功记录比较；make 的普通文件判据则将当前输入 mtime 与当前输出 mtime 比较。下表为两种算法的推导，不是 GNU make 之外的新契约。假设上次输入时间为 100、输出时间为 200，且无其他变化或强制条件。

| 本次状态 | make 普通文件时间判据 | TaskLite 当前输入历史比较 |
| --- | --- | --- |
| 输入时间改为 150 | 输入仍不晚于输出，不因此重跑 | 时间指纹变化，重跑 |
| 内容相同，输入 touch 到 250 | 输入晚于输出，重跑 | 时间指纹变化，重跑 |
| 输入时间仍为 100，但 size 改变 | 不因此重跑 | size 指纹变化，重跑 |
| 输入指纹不变，输出被删除 | 目标缺失，需要构建 | 输入变化判断本身不检查输出缺失 |
| 输入指纹不变，输出被 touch | 依据新的输入/输出时间关系判断 | 不改变输入变化判断 |
| 内容改变但 size 与时间都不变 | 可能漏判 | 也可能漏判 |

TaskLite 当前实现出处为 [`RerunPolicy.check_input_changed`](../tasklite/engine/admission.py)：比较 size 与 mtime_ns，URI 声明不参与文件 stat 比较。make 判据出处见本节的普通文件规则；两者不应因为都使用 mtime 而被描述成同一算法。

### 已确认范围与尚未裁决事项

本轮已确认：run 期间参与的 input/output 只由当前执行 Job 修改，其他程序不得修改；B 依赖 A 且二者都入批时，先确定或完成 A 的本批处理，再判断 B 的 `on_input_change`。A 执行本身不自动令 B 执行，仍由 B 的输入变化判断；不自动把下游加入本批，`depends_on` 与 resources 保持分离。

这些决定意味着不能在本批上游尚未处理时，仅凭旧输入指纹提前删除 B 候选。它们尚未决定最终比较算法：**保留历史 `(size, mtime_ns)`，还是改用 make 的输入/输出时间关系及输出缺失判据，仍待裁决**。“参考 make”不视为已经授权迁移精确算法；后者还涉及完整输出集合、无文件输出和多输出等配套契约。本文不把任何推荐方案记录为用户决定。

### 已确认方向，未实施

以下为最新确认，取代前述“最终比较算法仍待裁决”的描述；当前实现与公开 API 说明尚未变更。

- run 期间，声明的 input/output 只由本次运行中的 Job 修改；其他程序及其他运行实例不得修改。保持自研，不自动将其他 Job 纳入本批；`depends_on` 与 `resources` 保持分离。
- 对已入本批的 A → B，先确定或完成 A 的本批处理，再判断 B 的 `on_input_change`；A 需要执行时必须等其成功，A 失败时 B 不能凭 A 的历史成功放行。A 执行本身不直接决定 B 是否重跑。
- 外部 trigger 提供“是否重跑”的判断函数，由引擎控制求值时机与激活；判断异常时明确报错，不解释为 `False` 或无需重跑，停止本次 run，未执行工作保留供下次恢复，不消耗 Job 执行重试预算，也不冒充 handler 失败。实现时停止与回收沿用现有停止及恢复机制，在途处理细节尚未单独裁决；函数参数等实现细节仍待设计。声明的 input 在本批上游处理完成后仍不存在时，直接报错，不能当作未变跳过，也不交由 handler 隐式修补。
- `on_input_change` 的设计改为：任一声明输入的当前 mtime **严格晚于本 Job 上次成功执行时间**时运行；相等不触发。只保留输入声明路径，不保存每个输入的历史 size/mtime。框架允许声明文件或目录；输入集合新增场景可通过声明其所在目录覆盖，不新增输入集合快照机制。当前实现直接对声明路径执行 `os.stat`，不递归；目录 mtime 通常覆盖直接成员增删或改名（如新增分页），不覆盖既有文件内容改写或深层目录变化，内容变化仍需声明相应文件。
- 正常路径不新增 wall → Attempt 关联字段。当前 uid 没有成功 wall 时直接执行，不借历史 Attempt 阻止；当前有可信成功档案时，取该 uid 最近成功 Attempt 的 `finished_at`，时间由框架采集，缺可信基线则执行。人工 seed 或不完整旧数据不得拼接旧 Attempt 伪造可信基线，也不以 `wall.last_run_at` 替代。
- Job 正常完成就是成功，不设计半成功；Attempt 的运行中、重排等过程状态不改变这一业务终结语义。
- 任务只负责自身执行，业务终结为成功或失败。跨任务业务数据统一通过声明产物传递，本轮以 `declare_output` 声明文件或目录产物，消费方声明 input。内存 output 仅为未来方向：真实需求证明文件传输成为瓶颈时才评估，不纳入本轮实现；不预先承诺跨进程共享、零拷贝或持久恢复，其生命周期、所有权、容量背压及恢复语义届时独立设计。父进程与 worker 的 IPC 仅用于引擎控制及收尾，不建立任务间消息系统；现有 ctx 对其他 uid 的查询与 Discovery 兼容边界仍待核实，不据此删除 API。
- Job 成功状态与对应 Attempt 的 `succeeded` 终结及 `finished_at` 应在同一个成功事务中提交；正常成功所需的 Attempt 开行缺失或终结失败时，不能仍报告成功。Attempt 仍不进入六集合，但其成功时间既作为重跑依据，关键写入就不再仅是可降级的旁路观测。
- IPC 恢复以引擎记录为准：Job 成功状态与对应成功 Attempt 的原子事务提交才确认完成。worker 已执行业务或返回、但尚未成功提交时，崩溃恢复允许重新执行；仍须遵守同 uid 执行互斥，不能与孤儿 worker 并行。不要求通过持久保存未确认终态的 IPC 结果避免重做，本轮不新增结果持久收件箱。
- 上述事务要求属于未来设计：当前实现仍分开事务，尚未实施；历史数据可信性识别、恢复、历史清理及迁移的配套细节仍待核实。
- 保留 mtime 的内容替换可能漏判；输出缺失也不由这一输入时间规则检测。这些是算法边界，不据此增加其他重跑条件。
- 执行前删除已有输出与失败后清理输出，由两个独立显式参数分别授权，默认均关闭；参数名称尚未定为公开 API。没有前置删除授权而已有输出文件或目录时明确报错，不静默覆盖。失败清理授权不蕴含删除旧输出，前置删除授权也不蕴含失败清理；显式删除不承诺保留或恢复旧内容，框架不自动备份回滚。
- 目录声明代表整个目录为本 Job 产物；只有显式前置删除授权才可递归删除旧输出目录，只有显式失败清理授权才可递归清理该输出目录，不另增第三个递归开关。保留原有路径边界校验，不据此推导任意系统根目录授权。共享目录不整体声明为可删除产物，改为声明归本 Job 管理的文件或子目录。
- 由上述权限边界推导的实现要求：输出声明及冲突检查须先于该输出首次写入；声明未成功接纳时，不得隐式扩大删除权限。
- `declare_cache` 作为 `declare_output` 的薄封装，复用声明、路径验证及清理流程，不引入独立 cache 类型或独立清理状态机。cache 固定启用成功后与失败后清理，调用 cache 即明确选择这两项；删除已存在路径仍须调用方独立显式授权，不因 cache 隐含前置删除权限。声明检查未成功接纳时，不得将原有路径加入清理范围；共享目录、目录递归授权及路径边界沿用上述决定。
- 由 cache 封装推导的实现要求：统一底层还需表达成功后清理，普通 output 默认不启用；此前前置删除与失败清理的两个独立参数不排斥这一能力。具体参数名称及签名尚未实施，不作为正式 API。

重跑核心行为、上述 IPC 恢复边界、文件/目录输出的独立删除授权及 cache 封装方向已确认，尚未实施。IPC 具体传输方式仍待设计，独立 Pipe 仅为候选；新通道断链与回收实现待设计，沿用现有 DRAINING/ABORTING 及同 uid 执行锁契约。声明及限流信号的崩溃保留、清理错误及跨 Job 路径冲突等细节仍待裁决。

## 对 TaskLite 当前边界的观察

TaskLite 的稳定 uid、运行外管理 API、运行期 `ctx.spawn` 以及 `run()` 完成边界由现有指南约束；selection 等模式下可能保留延迟队列，不能把所有 COMPLETED 都解释为物理 queue 为空。[V2 使用指南](V2_GUIDE.md)

在当前讨论前提下，rerun 判断发生于启动恢复、spawn 摄入和派发前；未重新准入的 wall 记录不会仅因外部输入变化自动被发现。这一前提与项目比较不能代替具体实现审计。

上述项目共同提示需要分别写清：谁产生重新检查的机会、何时接受执行请求、忙时重复身份是否保留更新、何种信号代表输入结束。本文只记录这些问题的证据与边界；是否增加热准入、尾随补跑或常驻能力，均不在本文裁决。
