# Discovery「记见不 spawn」原语 —— 契约论证设计文档

> 状态：论证稿（未实现）。本文档论证在 `tasklite/wrappers/discovery.py` 的
> REQUIREMENTS CONTRACT 之下，为「业务回调决定不 spawn 的 item」提供显式
> `mark_seen` 原语是否成立、影响面多大、如何实现。全文只引用本仓库代码
> 证据（file:line），自包含。

---

## 0. 术语与问题陈述（框架层面）

Discovery 扫描循环中，`process_item_func` 回调对每个「未见」item 被调用
（`wrappers/discovery.py:396-413`），其典型职责是 spawn 一个 process 子任务。
但业务上存在合法的一类 item：**进入 id 流、回调决定不 spawn 任何子任务**
（例如该 item 属于业务当前不处理的类型——是否处理是业务回调的自由，契约
从未要求回调必须 spawn）。

这类 item 的命运由契约唯一决定：**它永远不会进入已见集合**。业务当前的
变通办法是 spawn 一个 no-op 占位子任务，让该 uid 经正常终结路径进入 wall，
从而「被看见」。这污染了 wall 的语义（wall 是成功执行历史，不是已见登记簿），
并虚增 wall 体量。

拟议原语：在扫描上下文上提供显式 `mark_seen(content_id)`，把该 item 记入一个
**独立于 wall/failed** 的「已见标记」集合；仅在业务回调显式调用时写入，
框架不自动标记。

---

## 1. 现状与问题定量定性

### 1.1 已见集合的机制现状

契约条款（`wrappers/discovery.py`）：

- **S3**（`discovery.py:75-77`）：已见集合 = 框架 wall/failed 快照
  （`ctx.is_completed` / `ctx.is_failed`），**无独立 seen 持久化**。
- **S4**（`discovery.py:78-79`）：成员判定
  `f"{process_task_type}::{content_id}" in wall∪failed` 必须覆盖全部已见 id。
- **T1b/T2**（`discovery.py:49-54`）：某页**所有**内容 id 都已在已见集合中
  （整页命中）才允许终止。

判定在代码中的落点：

- 整页命中判定 `page_all_seen = all(ctx.is_completed(uid) or ctx.is_failed(uid) ...)`
  （`discovery.py:388-391`）；
- 逐条跳过判定同口径（`discovery.py:403`）；
- 快照在 discovery job **派发时刻**固定（`models/context.py:193-209`，
  构造点 `engine/dispatch.py:351-352, 364-365`）。

### 1.2 「不 spawn = 永远未见」是契约推论，不是缺陷

uid 进入 wall/failed 的**唯一**路径是「spawn 子任务 → 该子任务经
`CompletionMachine.complete_job` 成功收尾进 wall，或经
`StateStore.apply_failed`/`apply_failure` 失败收敛进 failed」（AGENTS.md
单一出口原则；后端写入点 `backend/sqlite_backend.py:399, 494`）。

S3/S4 把已见集合**严格定义**为 wall/failed 的成员关系。于是对回调未 spawn
的 item：它没有派生任何 uid 进入任何终态集合 → S4 成员判定恒为假 →
它按定义「永远未见」。这是 S3+S4+单一出口原则三者的**逻辑推论**，代码
行为与契约完全一致——不存在实现 bug。因此引入 `mark_seen` 是
**重开契约**（修订 S3/S4/T2 的定义域），不是修 bug。这个定性决定了后文
的影响面评估方式：不是评估「补丁波及面」，而是评估「契约条款改写面」。

### 1.3 这类 item 在 T2/S3/S4 下的精确行为与成本

设某页含 k 个「不 spawn」item（其余为已见 item）：

1. **T2 永不触发（含该 item 的页）**。`page_all_seen` 要求页内**所有** uid
   命中 wall∪failed（`discovery.py:388-391`），不 spawn 的 item 恒未命中 →
   整页命中恒为假 → 扫描继续翻页（`discovery.py:392-394` 的 break 不执行）。
2. **整页全由这类 item 组成时，只能白烧到 max_pages**。终止条件仅剩
   T1a 空页与 T4 max_pages 硬上限（默认 1000，`discovery.py:62-66`）；
   截断路径打 warning 后终止（`discovery.py:320-328`），且按 T4 语义
   「不算完整扫描」。下一 run 从第 1 页完整重扫（`discovery.py:29`），
   **同样的白烧逐 run 重复**。
3. **重扫税的构成**（每个 run）：
   - fetch 成本：至多 `max_pages` 页的完整拉取与解析（`discovery.py:329-336`）；
   - id_func 成本：每 item 一次（`discovery.py:344-361`）；
   - **process_item_func 重复回调成本**：这类 item 每 run、且同 run 内跨页
     重复出现时**逐页重复**进入 `process_item_func`（`discovery.py:402-406`）。
     已见 item 的重复 spawn 尚有 wall 去重吸收（`discovery.py:27-28`），
     而不 spawn 的 item **连去重吸收都不存在**——每次重遇都是一次完整的
     业务重判执行。
4. **与正常已见 item 的对比**：正常 item 在快照命中后被 `continue` 跳过
   （`discovery.py:403-404`），成本仅为一次集合查询；不 spawn 的 item 的
   成本是每 run 一次（跨页则多次）完整回调 + 阻断其所在页的整页命中。

**定性结论**：这不是正确性问题（不会漏、不会重），是**活性与成本问题**——
T2 刹车被永久垫高，最坏情形退化为「每 run 全量翻页到 max_pages」。T4 本身
是契约认可的显式退化路径（`discovery.py:62-66`），但当退化由「合法不处理的
item」系统性触发、且逐 run 重现时，退化成了常态。

### 1.4 现状 workaround（no-op 占位任务）的代价

业务现以「spawn 一个 no-op 占位子任务」让 uid 进入 wall。代价：

- **污染 wall 语义**：wall 的语义是「成功完成的工作历史」（
  `engine/store.py:232-234`「成功历史集合视图」；S3 论证「百万级 seen 由
  wall 表承载（wall 本就是完整历史），语义无损」——该论证前提被 no-op
  条目破坏：这些条目**不是历史**，是已见登记）。统计、审计、
  `clear_history`、`seed_wall` 等一切以 wall 为口径的面都被稀释。
- **虚增 wall 体量与快照成本**：每次派发都把 `wall_uids` 全量快照 pickle
  进 TaskContext（`engine/dispatch.py:351, 364-365`）——no-op 条目推高
  **所有**任务（不只 discovery）的派发快照成本。
- **误报失败信号**：no-op handler 一旦异常进入 failed，DLQ 出现业务无意义的
  假失败条目，污染告警与 `list_dlq()` 口径（`backend/sqlite_backend.py:445-496`
  的失败历史契约）。
- **每次标记一次完整派发周期**：子进程 spawn、ctx 构造、事务提交，只为
  写一个「已见」事实——用最重的机制表达最轻的语义。
- 唯一优点：零框架代码。

---

## 2. 重开契约的影响面（逐条）

若引入 `mark_seen`，以下契约条款必须修订（行号指 `wrappers/discovery.py`）：

| 条款 | 现文 | 修订 |
|---|---|---|
| 头部承诺（9-11 行） | 「无 cursor、无 seen 持久化、无互斥注入、无 poison 表」 | 改为「无 cursor、无**自动** seen 持久化、无互斥注入、无 poison 表；唯一例外是业务回调显式 `mark_seen` 写入的独立标记集合」 |
| S1（71-74 行） | 已见集合 = 历次见过所有 id 的并集，**必须完整** | 拆分为两部分：wall/failed 部分维持「必须完整」；显式标记部分是**业务选择性子集**，框架不承诺也不要求其完整，且**禁止框架自动标记**（自动标记会让「不处理」决策脱离业务掌控） |
| S2（72-74 行） | 禁止把已见集合截断为滑动窗口 | 显式适用于标记集合：标记表**无滑动窗口清理**，否则窗口外标记 item 被重判 → 重复回调，且破坏 T2（同 S2 原文理由） |
| S3（75-77 行） | 已见集合 = wall/failed 快照 | 改写为：已见集合 = wall/failed 快照 **∪ 显式已见标记集合**。原「无独立 seen 持久化」承诺在显式标记维度上重开 |
| S4（78-79 行） | 成员判定 `uid in wall∪failed` | 改写为 `uid in wall∪failed∪marks`（marks 键与 process uid 同构，见 §4.4） |
| T1b/T2（49-54 行） | 整页命中 = 页内全部 id 已在已见集合 | **整页命中判定（`discovery.py:388-391`）必须把 `is_marked_seen` 纳入成员判定**——否则标记对终止判定无效，原语失去存在意义。这是本次重开的核心语义变更 |
| 防删除/漂移（98-103 行） | 已见内容换页仍是已见 | 补一条：**被标记 item 重遇时不回调 `process_item_func`**（与已见 item 同等待遇，`discovery.py:403-404` 的跳过分支扩展第三判定项）；重遇重判即标记失效 |
| full 模式 on_missing（415-441 行） | 差集 = wall/failed 口径的 known − seen_this_run | **结论：不把标记条目纳入 known**，详见下文专项论证 |
| 去重与 job_id（84-93 行） | uid 派生单射 | 补：mark 键派生与 process uid 同构、保持单射（论证见 §4.4） |
| V1（115-121 行） | 回归测试场景清单 | 新增场景，见下文 |

### 2.1 专项论证：full 模式 on_missing 差集是否把 seen 条目算「已见」

现状机制（`discovery.py:310-318, 368-369, 422-441`）：

- `seen_this_run` 在 full 模式下对**每个 id_func 解析成功的 item** 无条件
  收集（`discovery.py:368-369`）——无论其已见、被标记还是新 spawn；
- `known` 从 `ctx.attempted_uids()`（wall∪failed 快照，
  `models/context.py:211-221`）按 `process_task_type::` 前缀过滤而来；
- `missing = known − seen_this_run`（`discovery.py:435`）。

两个方向的分析：

- **被标记 item 本次扫到**：它进 `seen_this_run` → 不会因「不在 known」而
  误报（它根本不在 known 里，差集方向是 known−seen_this_run，known 缺它
  只是意味着它不参与报告）→ **现状代码已天然无害，无需改动即不误报**。
- **被标记 item 从源上消失**：若纳入 known → 会进 missing 被上报；若不纳入
  → 不上报。**结论：不纳入**。理由：
  1. `on_missing` 的语义是「已**处理**内容的源端删除检测」，触发的是业务
     破坏性动作；被标记 item 从未被处理，没有处理状态需要清理，其消失对
     处理链无影响；
  2. 差集机制的军规是「宁可不报，不可误报破坏性动作」
     （`discovery.py:313-318` completed_full 不变式同源）；纳入标记条目会把
     破坏性动作面从「处理过的内容」扩大到「曾见过的全部内容」，与该军规
     相悖；
  3. 纳入还会让 `known` 的来源从 `attempted_uids()`（单一快照入口，
     `models/context.py:211-221`）扩为双源拼接，增加差集口径的解释成本。

  若未来业务需要「曾见内容删除检测」，应另立回调（如 `on_mark_missing`），
  不复用 `on_missing`（开放问题 OQ6）。

### 2.2 V1 需新增的测试场景

在既有 `tests/test_discovery.py` / `test_discovery_full.py` /
`test_discovery_independent.py` 基础上至少新增：

1. **标记驱动整页命中**：页内含标记条目 → `page_all_seen` 为真 → 终止，
   且标记条目**不**重调 `process_item_func`；
2. **标记跨 run 持久**：run N 标记 → run N+1 该条目按已见跳过、其所在页可
   整页命中；
3. **崩溃重跑幂等**：discovery job commit 前崩溃 → 标记未落盘 → 重跑重判
   重标（`INSERT OR REPLACE` 幂等），不重不漏；
4. **full 模式差集不受标记污染**：标记条目被扫到不进 missing；从源上消失
   也不进 missing（§2.1 结论的回归锁定）；
5. **框架无关降级**：未实现标记扩展协议的宿主 ctx → 扫描循环探测降级，
   行为等同现状契约（不崩、不探测失败）；
6. **fail-loud 校验**：`mark_seen("")`、非 str、含 `":"` 的入参在入口抛
   `TypeError/ValueError`；
7. **run 内即时可见**：同 run 后页重遇本 run 先前页标记的条目 → 跳过不
   回调（live 读语义，§4.2）；
8. **标记与 wall/failed 重叠无害**：同 uid 既在 wall 又有标记 → 判定并集
   语义正确、无断言触发（§4.3）。

---

## 3. 候选方案对比

### 方案 a：spawn no-op 占位任务（现状 workaround，零代码）

- 原理：让 uid 经正常终结路径入 wall，S3/S4 判定自然命中。
- 优点：零框架代码；同时修复 T2 垫高。
- 缺点：见 §1.4（wall 语义污染、全体任务派发快照膨胀、DLQ 假信号、
  每标记一次完整派发周期）。
- 判定：**可用但不可长期化**。它把框架语义缺口转嫁为业务的数据污染。

### 方案 b：`mark_seen` 独立命名空间（推荐）

- 原理：`seen::{process_task_type}::{content_id}` 形态的键入**独立持久
  集合**（独立 SQLite 表），与 wall/failed 互斥语义隔离；不参与任务去重
  （不是 job、不进 queue/in_flight），仅参与 discovery 已见判定。
- 优点：语义精确（「已见」与「已处理」是两个概念，各自有集合）；wall
  保持纯历史；互斥断言零接触（§4.3）；单事务原子持久（§4.1）。
- 缺点：重开契约（§2 全部条款）；新增一张表、一条提交路径、一个协议
  分层与一组回归测试。
- 判定：**推荐**。详见 §4。

### 方案 c：其他形态（论证后排除或限定）

**c1. `fetch_func` 内过滤（零代码，维持现状 + 文档化）**——这是最强的
「不作为」候选，必须正面论证：

- 原理：业务在 `fetch_func` 返回前把不处理的 item 剔除。过滤后整页命中
  判定只作用于剩余条目（`discovery.py:343-363` 以 fetch 返回值为界），T2
  恢复正常刹车。
- **终止正确性条件**：过滤谓词必须与「新旧」无关（如按 item 类型判定）。
  此时「过滤后整页命中」仍蕴含「本页及以后无新内容」——因为新内容若存在
  且属于处理范围，必在过滤后集合中显形。若过滤谓词依赖新旧信息（如
  「只过滤旧条目」），则过滤后整页命中不再蕴含安全终止，**禁止**该用法。
- 适用：分类所需信息在**列表页 payload 内可得**且谓词稳定。
- 不适用：(1) 分类需逐 item 二次请求或仅 spawn 后处理才可判定——把分类
  塞进 fetch_func 会让 fetch 成本爆炸或不可实现；(2) 业务需要「曾见但不
  处理」这一决策**持久可追溯**（过滤是静默遗忘）；(3) full 模式下业务
  希望不处理条目也保持源端删除免疫的判定口径一致。
- 判定：**能覆盖的场景优先用它**（零代码、零契约重开）；不能覆盖的场景
  才需要方案 b。这是 §6 结论的前置判据。

**c2. 契约硬化：强制业务必须 spawn**——把「不处理的 item 也必须 spawn」
写成契约。否决：处理与否是业务回调的自由（契约从未约束回调必须 spawn），
强制 spawn 等于把框架的表达力缺口立法成业务的义务；且 §1.4 的代价一项
不少。

**c3. 生成器 fetch / fetch 协议改造**（fetch 返回带分类通道的结构）——
否决：改 `fetch_func` 签名破坏全部既有回调与测试，破坏面远大于收益；
分类本质是回调的运行期决策，不应前移到 fetch 协议。

**c4. 复用 cursors 表存标记**——否决：cursor 的语义是高水位键值
（`models/context.py:223-241`），业务可经 `set_cursor` 读写同名键（误覆盖
无防护）；discovery 契约明言「无 cursor」（`discovery.py:11, 22-23`），把
seen 塞进 cursors 是语义自相矛盾；且 cursor_updates 的 None-删除语义会让
误传 `None` 静默清标记。

**c5. 复用 wall 表加 `seen::` 前缀**——否决，三个硬伤：
1. **键空间碰撞**：`process_task_type` 的注册校验只禁 `"::"`
   （`discovery.py:524-528`），字面 `seen` 是合法 task_type——该业务的
   process uid `seen::{content_id}` 与标记键完全同形，wall 表内碰撞；
2. **互斥断言路径把标记当任务对待**：wall∩failed 加载期收敛
   （`engine/recovery.py:193-215`）、`seed_wall` 的 failed 拒绝
   （`backend/sqlite_backend.py:638-658`）、六集合互斥 DEBUG 断言
   （`models/state.py:368-389`）全部以「wall 行 = 任务终态」为前提，
   标记行混入即语义越界；
3. `clear_history`/`delete_wall`（`sqlite_backend.py:620-631`）会无差别
   清掉标记，标记生命周期被任务历史生命周期绑架。

---

## 4. 关键设计决策逐项论证（对推荐方案 b）

### 4.1 持久化载体：独立表

新增表（与 `cursors`/`meta` 同层，`sqlite_backend.py:115-127`）：

```sql
CREATE TABLE IF NOT EXISTS seen_marks (
    uid TEXT PRIMARY KEY,
    payload TEXT
)
```

- **ACID**：与 wall 同库、同 WAL + `synchronous=FULL`（README「SQLite WAL
  状态持久化」承诺），复用现有 `BEGIN IMMEDIATE` 事务纪律。
- **崩溃恢复**：见 §4.2 写路径——标记随 discovery job 成功事务一并提交，
  不存在「标记在而扫描未完成」或反向的中间态。
- **互斥断言零接触**：六集合互斥断言只覆盖 queue/in_flight/wall/failed
  （`models/state.py:368-389`），标记表是正交第五集合，任何断言路径都不
  读它；wall∩failed 收敛（`recovery.py:193-215`）与 `seed_wall` 拒绝
  （`sqlite_backend.py:651-658`）不涉及标记行 → **wall∩failed 全局互斥
  检查不会被 seen 条目破坏**。
- 对比 meta 表：meta 是框架级 fencing 元数据（`sqlite_backend.py:121-127`），
  单值语义，无常集操作；百万级标记塞 meta 与建表无异但失掉专属语义与
  演进空间。

### 4.2 快照语义与写边界：run 内即时可见（读）+ 事务延迟提交（写）

**读语义（is_marked_seen）**：

- `is_completed`/`is_failed` 是派发时刻快照（`models/context.py:196-209`，
  构造点 `dispatch.py:351-352`）。标记判定采用**双轨**：
  `is_marked_seen(uid) = uid ∈ 构造时刻 marks 快照 ∪ 本 run live 标记`。
  - 构造快照必须存在，否则 run N 打的标记在 run N+1 不可见，原语失效
    （跨 run 持久是本原语的存在理由）；
  - live 部分使同 run 后页重遇先前页标记的条目即时跳过。若改为纯快照
    （本 run 标记对后页不可见），同 run 跨页重遇仍重调回调，T2 在长页链
    中弱化——live 读以一个进程内 set 的成本消除之。
- **并发扫描影响**：并发同 cursor_key 的多个 discovery job 各持各的 ctx，
  互不可见对方 live 标记——与 wall 快照「最坏重复 fetch，正确性不受影响」
  的既有论证同构（`discovery.py:13-17`）：标记写入幂等，重复回调只是成本。
- **ctx 注入面最小化**：marks 快照只对需要它的 discovery 派发注入（宿主
  知道哪些 task_type 注册了 discovery），避免重演「wall 全量快照进每个
  ctx」的既有成本被标记表再度放大。

**写语义（mark_seen）**：

- `ctx.mark_seen(content_id)` 只把 uid 追加进 ctx 内的 live 集合
  （`models/context.py` 新字段，同 `new_jobs`/`cursor_updates` 的暂存
  模式）；**落盘发生在 discovery job 的成功事务里**：
  `commit_job_success` 增加可选 `seen_marks` 参数，在既有
  `BEGIN IMMEDIATE` 事务内 `INSERT OR REPLACE`（`sqlite_backend.py:394-405`
  同一事务块，不新增事务边界）。
- **为何不用即时落盘**（如 `suspend_resource` 的信号文件通道，
  `models/context.py:243-279`）：(1) handler 跑在子进程、无 DB 句柄，即时
  落盘需要新开 ctx→主进程 IPC 通道，机制成本高；(2) 失去「标记与扫描轮次
  终态」的原子绑定，产生「部分页标记已落盘、扫描中途崩溃」的碎片状态——
  虽因幂等而语义无害，但与「job 提交 = 一切暂存原子生效」的既有心智模型
  冲突。

### 4.3 崩溃恢复幂等与三集合关系

- **重跑不重复**：标记是集合成员语义，`INSERT OR REPLACE` 天然幂等；
  重跑重判重标结果收敛于同一键集。
- **崩溃不丢失（也不幽灵存活）**：标记的唯一落盘点是 discovery job 的
  `commit_job_success` 事务。commit 失败走 3-strike 崩溃契约
  （`engine/store.py:381-383, 650-691`）时标记与 wall 记录**同生共死**：
  事务回滚 → 标记未落盘 → 下次 run 完整重扫重判（与无游标模型「崩溃即
  整轮重扫」既有语义完全一致，`discovery.py:445-447`）。不存在「item 未
  被标记判定依据却已含标记」的幽灵态。
- **同 uid 三集合关系（明确定义）**：
  - `wall ∩ failed = ∅`：既有全局互斥不变式（`state.py:380-383` 断言、
    `recovery.py:193-215` 收敛、`sqlite_backend.py:405, 522` 双向清理），
    **不变**；
  - `marks ∩ (wall ∪ failed)`：**允许重叠，重叠时 wall/failed 优先于语义
    解释（已处理 ⊃ 已见），判定取并集**。合法场景：业务先标记后改变主意
    spawn 同 uid、或 spawn 完成后回调又标记（防御性写法）。并集判定下
    重叠无害，框架**不得**对重叠做断言；
  - 标记表不参与任务去重：它不是 job、不进 queue/in_flight，`is_known`
    （`engine/store.py:282-284`）口径不变。

### 4.4 键的单射性

- mark 键 = `f"{process_task_type}::{content_id}"`，与整页命中判定用的
  process uid **同构**（`discovery.py:262-270`）——判定与标记查同一键空间，
  杜绝「两处派生不一致 → 永不整页命中」（`discovery.py:266-268` 既有警告
  的标记版）。
- **`seen::` 前缀拼接是否保持单射**：成立。content_id 是
  `sanitize_content_id` 输出（`discovery.py:362`），其字符域为
  `CONTENT_ID_ALLOWED`（字母/数字/`-_`/`.`，`utils/injective.py:24-27`，
  **不含 `:`**）∪ `%XX` 转义序列 ∪ `%_` 截断标记——输出中不存在字面
  `:`（契约明言「不含 `::`」，`discovery.py:90-91`）。因此键中的 `::`
  **只出现一次且必是分隔符**，`(process_task_type, content_id)` ↔ 键
  双射可解析复原；`process_task_type` 侧已由注册校验禁 `::`
  （`discovery.py:524-528`）。无需二次转义。
- 键存储在独立表，与 wall/failed uid **不共域**——即使某业务 task_type
  恰为字面 `seen` 也不碰撞（对比 §3-c5 否决理由 1）。
- 符合 AGENTS.md 单射性编码红线：标记键是派生复合 UID，采用可逆拼接，
  无丢弃式净化。

### 4.5 API 形态与框架无关协议的兼容

- **签名**：`ctx.mark_seen(content_id: str) -> None`。语义：把
  `f"{process_task_type}::{content_id}"` 记入已见标记集合，本 run 及后续
  run 的判定中按已见对待；幂等。
- **校验（fail-loud，与 `set_cursor`/`suspend_resource` 的入口校验对称，
  `models/context.py:233-236, 255-276`）**：
  - 非 str 或空串 → `TypeError/ValueError`；
  - **含任何字面 `:`** → `ValueError`。理由：合法 content_id 必已净化、
    不含 `:`（§4.4）；含 `:` 的入参要么是未净化的裸 id（判定 uid 与标记
    键将分叉 → 永不整页命中），要么是业务自造的复合形态——两者都必须
    在入口炸掉，不得静默写出永不命中的键。校验「不含 `:`」比校验
    「不含 `::`」更简单更强。
  - 重复标记：幂等 no-op（集合语义），不报错。
- **与 DiscoveryContext 最小协议的关系**：`DiscoveryContext`
  （`discovery.py:152-163`）刻意最小化，并明文声明「本模块不要求也不探测
  任何其他 ctx 字段/方法」（157-158 行）——这是「任何实现协议的宿主皆可
  复用」承诺的根基。**新增方法为协议必需成员会破坏该承诺**。兼容策略：
  **协议分层 + 能力探测降级**：
  1. 定义扩展协议 `SeenMarkingContext(Protocol)`（`@runtime_checkable`）：
     `mark_seen(uid)` + `is_marked_seen(uid)`。基础 `DiscoveryContext`
     **原样不动**，既有宿主/测试桩零改动；
  2. 扫描循环在整页命中判定与跳过判定处探测：
     `isinstance(ctx, SeenMarkingContext)` → 三项并集判定；未实现 → 两项
     判定（现状语义）。降级是**语义回退而非错误**——宿主没有标记集合时，
     契约回退到 S3 原口径，正确性不受损（最坏回到 §1.3 的成本问题）；
  3. 业务回调直接调用 `ctx.mark_seen` 在未实现宿主上抛
     `AttributeError`——**保留不兜底**：业务既然决定使用标记原语，静默
     no-op 会让标记从不生效、T2 永不触发（§1.3 的重扫税以更隐蔽的方式
     回归），违反 fail-loud 军规；文档写明「使用 mark_seen 的宿主必须
     实现扩展协议」。
- **fetch/process 约定改造**：不做。`process_item_func` 签名不变（回调
  拿到的就是同一 ctx，`discovery.py:406`），回调内按需调用即可；fetch
  协议不变（§3-c3 否决理由）。

---

## 5. 收益与代价清单

### 收益

1. T2 整页命中在被标记条目存在的页恢复正常刹车，消除 max_pages 白烧与
   逐 run 重扫税（§1.3）；
2. 被标记条目不再被逐 run 重判重调（`process_item_func` 调用次数从
   O(run × 页 × 重遇) 降为 O(1)）；
3. wall 回归纯「成功执行历史」语义：统计、审计、快照体量、DLQ 口径全部
   不再被占位条目稀释（§1.4）；
4. 「已见」与「已处理」两个概念获得各自独立的持久表示，语义可解释、
   可观测、可测试；
5. 标记与扫描轮次同事务原子，崩溃恢复语义与无游标模型完全同构（§4.3）。

### 代价

1. **契约重开**：§2 表列 10 处条款修订 + 头部承诺改写 + docstring/
   README/API_GUIDE 三处文档同步；
2. 实现：一张新表 + 迁移（`_ensure_*` 模式，`sqlite_backend.py:152-186`）、
   `commit_job_success` 加参、`TaskContext` 加字段与方法、快照注入路径、
   协议分层与扫描循环判定扩展；
3. 测试：§2.2 至少 8 个新场景，全量测试回归 + `make test-matrix`
   （类型注解与签名变更触发 AGENTS.md 红线 1）；
4. 长期：标记表无截断（S2）持续增长，与 wall 同量级；
5. 认知：宿主实现者需理解「基础协议 / 扩展协议」两层与降级语义。

---

## 6. Devil's Advocate：什么时候「维持现状 + 文档化」才是正确选择

诚实的反方论证——以下条件**全部**成立时，不该实现 mark_seen：

1. **fetch 过滤可行**：业务能在 `fetch_func` 内以「与新旧无关」的稳定谓词
   完成分类（§3-c1）。此时零代码、零契约重开，T2 正常工作。方案 b 的全部
   收益中只剩「决策持久可追溯」一项，而该项可以用业务自建状态满足，不必
   动框架。
2. **问题出现频率低**：不处理条目占比小、所在页总混有已见处理条目时，
   T2 虽被垫高但仍会在某页触发，白烧页数有限，重扫税可接受。
3. **团队承受不了契约重开成本**：本次变更触及的是 discovery 契约最核心的
   S3/S4/T2 三角，任何实现偏差（如判定漏并 marks、差集口径改错）都会以
   「静默全量重扫」或「on_missing 误报破坏性动作」的形式兑现——测试矩阵
   与文档同步的工程成本必须被如实计入。

反方不成立的情形：分类必须逐 item 二次请求或依赖 spawn 后才可得的信息、
不处理条目在页面上成片出现（T2 系统性失效）、业务需要持久记录「曾见不
处理」决策——此时 no-op workaround 的污染是持续纳税，fetch 过滤不可实现，
维持现状等于把 Liveness 退化（T4）固化为常态。这正是 mark_seen 的目标
场景。

**判定**：方案 b 的价值真实，但应以 §3-c1 的可行性为前置判据；fetch 过滤
可行的消费方应继续用现状并文档化，原语只为过滤不可行的场景而立。

---

## 7. 开放问题清单（留给实现阶段）

- **OQ1（最重要）标记的生命周期与管理面**：wall 有 `clear_history`/
  `delete_wall`/`seed_wall`（`sqlite_backend.py:620-666`），标记没有任何
  对等管理 API。业务规则改变后想**重新处理**已标记 item，框架内无路径：
  标记持久 → 该 item 永判已见、永不重判、**且无任何失败信号**（比 wall
  条目更隐蔽——wall 清除后任务会重跑，标记不清除则 item 永不回归）。实现
  必须同时定义：按 uid 删标记、批量清除、以及「clear_history 是否联动清
  标记」（联动则标记丢失致重判回调风暴；不联动则语义分叉，倾向不联动 +
  提供独立 `clear_seen_marks`，需在实现时定案）。
- **OQ2** 标记 payload 是否记录原因/时间戳（可观测性与排障），以及
  `list_seen_marks()` 类查询 API 的口径。
- **OQ3** 多个 discovery 注册共享同一 `process_task_type` 时标记天然共享
  （键同构）——是期望语义还是需要 cursor_key 维度隔离？键已含 cursor_key
  前缀（`discovery.py:280-287`），但共享 process_task_type 的两组对同一
  content_id 的「不处理」决策是否应互相可见，需业务语义定夺。
- **OQ4** marks 快照的派发注入路径：宿主按 discovery 注册表定向注入
  （§4.2）的具体机制（dispatch 侧识别、懒加载、或全量注入）需实现时权衡。
- **OQ5** 标记表长期膨胀的治理：S2 禁止截断，与 wall「本就是完整历史」
  同构；是否需要（以及如何在不破坏 T2 的前提下做）compact，契约明言
  「无 compact」（`discovery.py:76-77`），重开与否另行论证。
- **OQ6** 「曾见内容删除检测」需求若出现，另立 `on_mark_missing` 回调，
  不复用 `on_missing`（§2.1）。

---

## 8. 论证结论

1. **定性**：「process 回调不 spawn = 永远未见」是 S3+S4+单一出口原则的
   逻辑推论，代码行为与契约一致——引入 `mark_seen` 是**重开契约**，不是
   修 bug。重开影响面已逐条列明（§2），核心是 S3/S4 定义域扩展与 T2 成员
   判定并入 `is_marked_seen`；full 模式 on_missing 差集**维持 wall/failed
   口径、不纳入标记条目**（§2.1）。
2. **推荐**：**推荐实现方案 b**（`mark_seen` 独立命名空间：独立 SQLite 表、
   与 process uid 同构的键、run 内 live 读 + 成功事务延迟写、协议分层 +
   能力探测降级、fail-loud 入口校验）。核心理由：no-op workaround 以污染
   wall 语义和全体任务派发快照为代价换取已见判定，成本逐 run 重复发生；
   而 mark_seen 把「已见」与「已处理」两个本就不同的概念分离到各自集合，
   实现面收敛在一个新表、一条既有事务路径和一个可选协议扩展内，互斥
   断言与既有不变式零接触。
3. **前置判据**：若某消费方的「不处理」分类可在 `fetch_func` 内以与新旧
   无关的稳定谓词完成，该消费方应维持现状（fetch 过滤）并文档化，不使用
   本原语——原语只为过滤不可行的场景而立（§6）。
4. **推荐路径**（实现阶段，遵守 AGENTS.md 原子提交纪律）：
   ① 契约条款修订 + 协议分层 + 扫描循环判定扩展与 V1 新场景回归测试
   （先行，可用内存桩验证语义）；② 后端 seen_marks 表 + commit 路径 +
   崩溃幂等回归；③ TaskContext.mark_seen + 校验 + 快照注入 + 框架无关
   降级测试；④ 文档同步（本文件转「已实现」状态、README/API_GUIDE/
   discovery docstring）。每步测试全绿后独立提交。
