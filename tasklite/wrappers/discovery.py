"""纯任务模型增量发现 —— ``register_discovery``（框架默认适配器）。

架构性质：**扫描核心框架无关**——``DiscoveryHandler`` 只依赖
``DiscoveryJob``/``DiscoveryContext`` 两个最小协议，运行时**不 import
任何 tasklite 模块**；``register_discovery`` 是框架提供的默认封装
函数，只通过 ``DiscoveryHost`` 公开协议把扫描器挂到宿主上（TaskLite
是其默认实现）。任何实现该协议的宿主/测试桩均可直接复用。

「已见集合」的实现载体：框架的 wall/failed 集合
（``ctx.is_completed`` / ``ctx.is_failed`` 的 run 派发时刻快照）——
**无 cursor、无 seen 持久化、无互斥注入、无 poison 表**。

为什么成立：discovery 的 seen 集合与框架 wall/failed 集合是同一份数据
（「这个内容处理过了」）——后者由后端 ACID 事务保证完整与持久。
并发同 cursor_key 扫描无需互斥（「共享可变 seen 集合可被并发 REPLACE
覆盖」的前提在本模型下不存在）：wall 由后端事务写入，最坏代价
是重复 fetch，正确性不受影响。

参数与行为要点：
- ``process_task_type``（必填）：process 子任务的 task_type——整页命中判定
  按 ``f"{process_task_type}::{content_id}"`` 查询 wall/failed 快照。
- ``cursor_key_func``（可选）：仅作 job_id 命名空间前缀（防止跨分组内容 id
  碰撞导致 wall 去重误吞），无游标语义。
- 坏 item 由 process 子任务的任务层 retry/DLQ 吸收——子任务进 DLQ 即
  ``is_failed`` = 已见，不再重新 spawn；DLQ 内容在 failed 快照中算「已见」，
  含坏 item 的页可正常整页命中。
- wall/failed 快照在 discovery job 派发时固定：同一内容本次 run 内跨页重复
  出现时会重复 spawn（框架 wall 去重吸收，仅浪费一次 spawn 调用）。
- 达 ``max_pages`` 截断仅记日志、不落任何游标：下次 run 仍从第 1 页完整重扫。

回调必须为模块级可 pickle 函数（spawn 进程隔离约束，与普通 handler 一致）。

================================================================================
需求契约全文（REQUIREMENTS CONTRACT —— 任何实现/重构必须逐条满足）
================================================================================

------------------------------------------------------------------------------
数据前提（由业务方保证，框架据此设计）
------------------------------------------------------------------------------
前提 1. 内容源按**时间倒序**分页：最新内容在第 1 页，越翻越旧。
前提 2. 每个内容项有**全局唯一 id**（字符串），用于识别「已见」。
前提 3. id **不是单调递增的**（新内容的 id 不一定比旧内容大）——「高水位 /
    max-id 游标」方案**不可用**，必须用「完整已见 id 集合」做成员判定。
    禁止假设 id 可比大小。

------------------------------------------------------------------------------
终止条件（唯一的合法终止方式）—— 核心语义
------------------------------------------------------------------------------
T1. 发现扫描从第 1 页开始逐页推进，直至满足**任一**终止条件：
    a) fetch 返回空页（页尾）；
    b) 某一页的**所有**内容 id 都已在「已见集合」中（整页命中）。
T2. 当且仅当整页命中时，才说明「本页及以后全是旧内容」，扫描结束。
      - 单页**部分**命中不算终止：必须继续翻页（本页前半仍有新内容，
        后续页仍可能有新内容）。
T3. 仅「完整扫描」结束（整页命中或空页）才返回成功。中途停止（job 失败/
    重启、id_func 异常中断）**不得**视为完整——本模型无游标可推进，未完成
    的扫描必须在下一次运行完整重扫（重不漏由确定性 job_id + wall 去重吸收）。
    **退化路径例外**：max_pages 截断与 id_func 全失败
    早停两处实现返回**成功**（进 wall）并打醒目日志——语义上「未完整」由
    discovery 默认 rerun="every_run" 保证下一会话重扫；若用户将 rerun 覆盖
    为 never，截断后增量链静默死亡（漏内容且无失败信号），需自行权衡。
T4. **liveness 例外（max_pages 硬上限，默认 1000）**：持续更新源无法整页
    命中时按页数上限停止——这是对完整扫描的显式退化路径，**不算完整扫描**：
    未扫到的更深页内容仍在源上。退化路径**不得**触发 on_missing 差集
    （已见但本次未扫到 = 源端删除检测）——截断时更深页内容并未删除，误报会
    触发业务的破坏性动作。宁可跳过未扫页，不可无限翻页直到超时判死。

------------------------------------------------------------------------------
已见集合 —— 必须完整，禁止截断
------------------------------------------------------------------------------
S1. 已见集合是「自首次运行以来见过的所有内容 id 的并集」，**必须完整**。
S2. **禁止**把已见集合截断为「最旧 N 个」「最近 N 个」或任何滑动窗口：
    截断后窗口外的已见 id 下次被误判为「新内容」→ 重复处理，且破坏
    T2 的整页命中判定。
S3. 已见集合 = 框架 wall/failed 快照（``ctx.is_completed`` / ``ctx.is_failed``），
    完整与持久由后端 ACID 事务保证。**无独立 seen 持久化、无 compact 编码**
    ——「百万级 seen」由 wall 表承载（wall 本就是完整历史），语义无损。
S4. 成员判定 ``f"{process_task_type}::{content_id}" in wall∪failed`` 必须
    覆盖全部已见 id。

------------------------------------------------------------------------------
去重与 job_id —— 必须确定性派生
------------------------------------------------------------------------------
- 同一内容项（同 cursor_key 组 + 内容 id）在**任何运行/重启**中必须生成
  **相同**的 job_id，使框架的 wall/queue 去重能吸收重复 spawn（崩溃重跑、
  并发重复扫描）。
- **禁止**用随机后缀（uuid/时间戳）派生 job_id——随机 id 使去重失效，
  同一内容被重复执行。
- job_id 派生 = ``sanitize_content_id(prefix + content_id)``：content_id
  是框架净化后的单射转义形式（干净 id 原样、脏 id UTF-8 字节百分号转义、
  超长截断 + sha256 指纹，不含 ``::``），**直接**用作子任务 job_id。
  ``cursor_key_func`` 返回值仅作命名空间前缀（防跨组 id 碰撞），无游标
  语义。派生必须与 ``process_task_type`` 的已见判定一致（同一 content_id）。

------------------------------------------------------------------------------
防源端删除/漂移语义（REQ-3）
------------------------------------------------------------------------------
- 内容源删除某项记录后，其后续内容前移补位，页内容漂移。
- 基于「完整已见集合 + 整页命中终止」，源端删除天然免疫：
      - 已见内容即使换页也仍是「已见」→ 不会被当新内容；
      - 新内容总是「未见」→ 在整页命中之前一定会被扫描到。
- **禁止**用「历史边界 id 集合」代替完整集合做碰撞检测——边界可能被
  删除，导致后续内容永远无法触发终止 → 全量重扫或无限扫描。

------------------------------------------------------------------------------
重新发现（下一次运行的语义）
------------------------------------------------------------------------------
- 每次运行从第 1 页重新开始（不依赖上一次的页码——页内容已漂移，页码无意义）。
- 重新发现**不会漏新内容**：新内容总在较前页，整页命中之前必被扫描到；已见内容因完整集合被正确跳过。
- 重新发现**不会重复处理**：已见内容被集合吸收。

------------------------------------------------------------------------------
可验证性
------------------------------------------------------------------------------
V1. 契约正确性必须由回归测试固定，至少覆盖：首次全量扫描（空集合 → 逐页
    → 空页终止）；中间翻页运行（部分命中页继续翻，整页命中终止）；重新发现
    含新内容（新 id 被处理，旧 id 被跳过）；源端删除场景（删中间/删最旧/删最新
    → 不重不漏）；崩溃重跑（wall 快照未推进 → 完整重扫 → 去重吸收）；
    max_pages 截断不触发 on_missing；id_func 全失败早停不触发
    on_missing；多 cursor_key 组共享 process_task_type 时 on_missing
    只报本组。
"""
from __future__ import annotations

import hashlib
import logging
import pickle
import re
from typing import Any, Callable, Iterable, Protocol

from ..models.job import RERUN_VALUES

logger = logging.getLogger("tasklite")


# 框架无关协议（G-DISCOVERY）：本模块的扫描核心**不 import 任何框架模块**。
#
# ``DiscoveryJob`` / ``DiscoveryContext`` 是扫描循环对运行时的全部要求；
# ``DiscoveryHost`` 是注册适配器对宿主框架的全部要求。
#
# 任何实现这三个协议的框架/测试桩都可以复用本模块：TaskLite 只是
# 框架提供的**默认宿主适配**，不是唯一宿主。



class DiscoveryJob(Protocol):
    """扫描任务的最小接口（框架无关）：payload 提供命名空间 key 的输入。"""

    payload: dict[str, Any]


class DiscoveryContext(Protocol):
    """扫描循环需要的最小上下文接口（框架无关）。

    语义契约（由宿主保证）：
    - ``is_completed`` / ``is_failed``：派发时刻 wall/failed 快照成员判定；
    - ``attempted_uids``：wall∪failed 的只读快照（full 模式 on_missing 差集用）。
    本模块不要求也不探测任何其他 ctx 字段/方法。
    """

    def is_completed(self, uid: str) -> bool: ...
    def is_failed(self, uid: str) -> bool: ...
    def attempted_uids(self) -> Iterable[str]: ...


class DiscoveryHost(Protocol):
    """``register_discovery`` 对宿主框架的全部要求（框架无关协议）。

    宿主只需提供两个公开能力：注册一个普通 handler、登记 discovery 默认
    rerun。TaskLite 的实现是框架默认适配，但任何实现本协议的对象都可
    以直接使用 ``register_discovery``。
    """

    def register_handler(
        self,
        task_type: str,
        handler_func: Callable[..., Any],
        default_resources: dict[str, float] | None = None,
        payload_schema: type | None = None,
    ) -> None: ...

    def set_discovery_rerun(self, task_type: str, rerun: str) -> None: ...

# 内容 id 单射净化：实现已统一收敛至 utils.injective
from ..utils.injective import CONTENT_ID_ALLOWED, escape_injective, sanitize_content_id



def _in_cursor_group(content_part: str, group_prefix: str) -> bool:
    """判定净化后的 content 片段是否派生自本 cursor_key 组的未截断转义前缀。

    直通域成员恰为 ``group_prefix + escape(cid)``，前缀比较精确；截断域成员
    尾部是「%_ + 全文指纹」，其保留头部仍是转义全文的前缀，故互补以
    「组前缀以该头部为前缀」判定。不同组仅当转义前缀共享约百字符以上时才
    可能误判——这是截断域残留信息的边界，组前缀侧永不截断使其收窄到极限。
    """
    if content_part.startswith(group_prefix):
        return True
    head = content_part.split("%_", 1)[0]
    return len(head) < len(content_part) and group_prefix.startswith(head)



class DiscoveryHandler:
    """按 REQUIREMENTS CONTRACT 实现的增量发现 handler（纯任务模型）。

    **框架无关**：本类及其扫描循环只依赖 ``DiscoveryJob`` / ``DiscoveryContext``
    两个最小协议（payload + is_completed/is_failed/attempted_uids），运行时**不 import
    任何 tasklite 框架模块**，可被任何实现这两个协议的宿主直接复用；
    TaskLite 只是框架默认宿主。

    框架负责契约规定的全部语义：
      - 从第 1 页起逐页扫描，page 由框架传入，用户 fetch_func 只拉一页；
      - 「已见」判定 = wall/failed 快照成员判定（``ctx.is_completed`` /
        ``ctx.is_failed``），完整集合由框架保证；
      - 整页命中或空页才终止；仅完整扫描结束才返回成功（本模型无游标，
        崩溃即整轮重扫，重不漏由确定性 job_id + wall 去重吸收）；
      - 每个新 item 以净化后的 content_id 交给 ``process_item_func``，
        用户在其中 spawn 子任务，job_id 直接用框架传入的 content_id；
      - 防源端删除与记录漂移由「完整 wall/failed 集合 + 整页命中」天然保证。

    可 pickle 性：本类是模块级类；回调由用户提供，必须为模块级可 pickle
    函数（与普通 handler 的 spawn 约束一致）。
    """

    def __init__(
        self,
        fetch_func: Callable[[DiscoveryJob, DiscoveryContext, int], list[Any]],
        id_func: Callable[[Any], str],
        process_item_func: Callable[[DiscoveryJob, DiscoveryContext, Any, str], None],
        process_task_type: str,
        cursor_key_func: Callable[[dict[str, Any]], str] | None = None,
        max_pages: int = 1000,
        scan_mode: str = "incremental",
        on_missing: Callable[[DiscoveryJob, DiscoveryContext, list[str]], None] | None = None,
    ):
        self.fetch_func = fetch_func
        self.id_func = id_func
        self.process_item_func = process_item_func
        # process 子任务的 task_type——整页命中判定用 f"{ptype}::{content_id}"
        # 查询 wall/failed，必须与用户 spawn 时一致（见 register_discovery
        # 的校验：非空 str 且不含 "::"）。
        self.process_task_type = process_task_type
        self.cursor_key_func = cursor_key_func
        # 单次扫描的页数硬上限。持续更新的内容源可能永远无法整页命中
        # （新内容不断把旧内容顶到后面）——无上限时扫描无限翻页，直到超时
        # 被 TIMEOUT kill → Unknown → DLQ 不重试 → 发现链死亡。
        self.max_pages = max_pages
        # 扫描模式："incremental"（默认，整页命中终止）/
        # "full"（跳过整页命中，一路扫到空页或 max_pages；已见内容仍逐条
        # 跳过，成本只在 fetch 不在处理）。
        if scan_mode not in ("incremental", "full"):
            raise ValueError(
                f"scan_mode must be 'incremental' or 'full', got {scan_mode!r}"
            )
        self.scan_mode = scan_mode
        # full 模式可选的 missing 检测回调——扫描结束后把
        # 「已见但本次未扫到」的 content_id 差集交给业务（= 源端删除/缺失检测）。
        # 模块级可 pickle（spawn 约束）。
        self.on_missing = on_missing

    def _process_uid(self, content_id: str) -> str:
        """构造 process 子任务的 uid——与用户 spawn 时的一致。

        框架传入的 content_id 已净化，用户应直接用其作 ``Job.job_id``：
        ``Job(process_task_type, content_id, ...)``。判定与 spawn 用同一个
        uid 派生，是「已见判定永不失效」的前提（两处不一致 → 永不整页命中
        → 全量重扫到 max_pages）。
        """
        return f"{self.process_task_type}::{content_id}"

    def __call__(self, job: DiscoveryJob, ctx: DiscoveryContext) -> bool:
        # 可选命名空间前缀（cursor_key_func 仅作 job_id 分组，无游标语义）
        # prefix 不能是 `key + "_"` 的裸拼接——`_` 属于
        # 干净字符集，`(key="ab", cid="c_d")` 与 `(key="ab_c", cid="d")`
        # 派生相同 job_id → 跨组碰撞 → wall 去重静默吞内容（数据丢失）。
        # 采用 length-prefix（`{len(key)}:{key}`）：`:` 会被单射转义为
        # %3A，`%3A` 前的数字给出 key 长度 → 拼接边界可复原 → 不同
        # (key, cid) 对永不派生相同字符串（单射保持）。
        prefix = ""
        if self.cursor_key_func is not None:
            key = self.cursor_key_func(job.payload)
            if not isinstance(key, str) or not key:
                raise ValueError(
                    f"cursor_key_func must return a non-empty str, got {key!r}"
                )
            prefix = f"{len(key)}:{key}"

        # 扫描模式与页数上限：优先尊重 job.payload 中的动态指定，未指定则退回 handler 默认值
        scan_mode = self.scan_mode
        if isinstance(job.payload, dict) and "scan_mode" in job.payload:
            p_scan_mode = job.payload["scan_mode"]
            if p_scan_mode not in ("incremental", "full"):
                raise ValueError(
                    f"scan_mode in payload must be 'incremental' or 'full', got {p_scan_mode!r}"
                )
            scan_mode = p_scan_mode

        effective_max_pages = self.max_pages
        if isinstance(job.payload, dict) and "max_pages" in job.payload:
            p_max = job.payload["max_pages"]
            if not isinstance(p_max, int) or isinstance(p_max, bool) or p_max < 1:
                raise ValueError(
                    f"max_pages in payload must be a positive int, got {p_max!r}"
                )
            effective_max_pages = p_max

        # 从第 1 页开始逐页扫描
        page = 1
        # full 模式收集「本次扫描见过的 content_id」——
        # 扫描结束后与已见集合做差集 = 源站删除检测（missing）。
        seen_this_run: set = set()
        # 不变式：on_missing 差集仅在完整扫描（空页终止）时语义成立。
        # max_pages 截断与 id_func 部分失败都意味着源上仍有本轮无法归位的
        # 内容——失败 item 无法入 seen_this_run，差集会把它确定性误报为
        # 「已删除」。任何不完整因素必须置 completed_full=False 使本轮
        # on_missing 失效（宁可不报，不可误报破坏性动作）。
        completed_full = True
        while True:
            if page > effective_max_pages:
                # 页数硬上限。无法整页命中的持续更新源或 full 模式指定页数在此停止——
                # 已 spawn 的子任务由 wall 吸收；下次 run 从第 1 页重扫。
                completed_full = False
                logger.warning(
                    f"Discovery: reached max_pages={effective_max_pages} without "
                    f"full-page hit; stopping (next run rescans from page 1)."
                )
                break
            items = self.fetch_func(job, ctx, page)
            # 类型校验必须先于判空——""/None/False 等 falsy 非 list
            # 返回值不得被静默当空页终止（fail-loud：结构 bug 不伪装页尾）。
            if not isinstance(items, (list, tuple)):
                raise ValueError(
                    f"fetch_func must return a list of items, "
                    f"got {type(items).__name__}"
                )
            if not items:
                break  # T1a: 空页 → 页尾，终止

            # id_func 逐 item 隔离——单个坏 item（抛异常/返回空串/非 str）
            # 跳过（不入任何集合、下次 run 重试），不崩整个扫描（否则该
            # discovery 链永久死）。
            page_entries = []
            for item in items:
                try:
                    cid = self.id_func(item)
                except Exception as e:
                    # 部分失败同样使本轮差集失效（见 completed_full 不变式）
                    completed_full = False
                    logger.error(
                        f"Discovery: id_func raised for an item; "
                        f"skipping (will retry next run): {e}"
                    )
                    continue
                if not isinstance(cid, str) or not cid:
                    completed_full = False
                    logger.error(
                        f"Discovery: id_func returned non-empty str? "
                        f"{cid!r}; skipping (will retry next run)"
                    )
                    continue
                content_id = sanitize_content_id(prefix + cid)
                page_entries.append((item, content_id, self._process_uid(content_id)))
                # 收集判定必须读 payload 覆盖后的 scan_mode（与结尾差集判定
                # 同一变量）：误读构造期默认值时，「默认 incremental + payload
                # 指定 full」的组合会让 seen_this_run 恒空 → 差集把全部已见
                # 内容误报「已删除」，触发业务的破坏性动作。
                if scan_mode == "full":
                    seen_this_run.add(content_id)

            # 整页 id_func 全失败 = 源结构异常：终止本轮扫描避免死循环
            # （每页都失败、wall 零增长 → max_pages 前先止损），下轮重试。
            if not page_entries:
                # 此早停也是不完整扫描——该页 item 及
                # 更深页内容仍在源上（只是 id_func 解析失败），此时计算
                # on_missing 差集会把这些内容误报为「已删除」。
                completed_full = False
                logger.error(
                    f"Discovery: page {page} had {len(items)} items "
                    f"but ALL id_func failed; stopping (will retry next run)."
                )
                break

            # 单页「部分命中」不算终止——整页命中才说明本页及以后全是
            # 旧内容。判定依据完整 wall/failed 快照。
            # scan_mode="full" 跳过整页命中 break——一路扫到
            # 空页或 max_pages（全量成本只在 fetch，已见内容仍逐条跳过）。
            page_all_seen = all(
                ctx.is_completed(uid) or ctx.is_failed(uid)
                for _, _, uid in page_entries
            )
            if page_all_seen:
                if scan_mode != "full":
                    break  # 整页命中 → 终止（增量模式）

            # 处理本页新 item（process_item_func 逐 item 隔离——单个
            # 坏 item 不崩整轮扫描；跳过不入任何集合，下次 run 重试。达
            # DLQ 阈值由 process 子任务的任务层 retry/DLQ 吸收，本层不计数）。
            # 整页已见时复用 page_all_seen 短路——循环内判定与 all 判定
            # 语义一致（均为 is_completed or is_failed），无需对同批 entry
            # 重复查询快照。
            for item, content_id, uid in page_entries:
                if page_all_seen or ctx.is_completed(uid) or ctx.is_failed(uid):
                    continue
                try:
                    self.process_item_func(job, ctx, item, content_id)
                except Exception as e:
                    logger.error(
                        f"Discovery: process_item_func failed for content "
                        f"{content_id!r}; skipping, will retry next run: {e}"
                    )
                    continue
            page += 1

        # full 模式 + on_missing 回调 → 已见但本次未扫到 =
        # 源站删除检测。已见集合从 wall/failed 快照筛 process_task_type 前缀。
        # 仅当完整扫到空页才计算差集——max_pages 截断、id_func 部分失败
        # 时源上仍有本轮无法归位的内容，误报「已删除」会触发业务的破坏性动作。
        # 只取本组（cursor_key 命名空间）的已见内容——多组共享
        # process_task_type 时，他组内容在本组 seen_this_run 之外，不过滤
        # 会被误报为「已删除」。
        if scan_mode == "full" and self.on_missing is not None and completed_full:
            try:
                known = set()
                ptype_prefix = f"{self.process_task_type}::"
                # 组前缀必须用未截断的单射转义形态：净化输出的截断形态尾部
                # 带全文指纹，两个独立净化值之间不存在前缀关系，组过滤只能
                # 依托未截断前缀 + 截断头部互补匹配保持精确（_in_cursor_group）。
                group_prefix = escape_injective(prefix, allowed=CONTENT_ID_ALLOWED) if prefix else ""
                for uid in ctx.attempted_uids():
                    if uid.startswith(ptype_prefix):
                        c = uid[len(ptype_prefix):]
                        if not group_prefix or _in_cursor_group(c, group_prefix):
                            known.add(c)
                missing = sorted(known - seen_this_run)
                if missing:
                    logger.warning(
                        f"Discovery full scan: {len(missing)} previously-seen "
                        f"content(s) not found this run (deleted on source?)."
                    )
                    self.on_missing(job, ctx, missing)
            except Exception as e:
                logger.error(f"Discovery: on_missing callback failed: {e}")

        # 本模型无游标可推进——崩溃（fetch 抛异常 → 异常传播，由用户
        # 按错误分类决定重试/DLQ）即整轮重扫；已 spawn 的子任务由确定性
        # job_id 的 wall 去重吸收，不重不漏。
        return True


def register_discovery(
    host: DiscoveryHost,
    task_type: str,
    fetch_func: Callable[[DiscoveryJob, DiscoveryContext, int], list[Any]],
    id_func: Callable[[Any], str],
    process_item_func: Callable[[DiscoveryJob, DiscoveryContext, Any, str], None],
    process_task_type: str,
    cursor_key_func: Callable[[dict[str, Any]], str] | None = None,
    default_resources: dict[str, float] | None = None,
    payload_schema: type | None = None,
    max_pages: int = 1000,
    rerun: str = "every_run",
    scan_mode: str = "incremental",
    on_missing: Callable[[DiscoveryJob, DiscoveryContext, list[str]], None] | None = None,
) -> None:
    """注册纯任务模型增量发现 handler（per REQUIREMENTS CONTRACT）。

    按 REQUIREMENTS CONTRACT（T/S/D/M/R 条款）实现，「已见集合」直接
    使用框架 wall/failed 集合——无 cursor、无 seen 持久化、无 poison 表、
    无同 cursor_key 互斥注入。

    Args:
        host: 实现 ``DiscoveryHost`` 协议的宿主（框架默认适配 = TaskLite；
            任何只实现 register_handler + set_discovery_rerun 两方法的
            对象亦可——本函数不依赖框架具体类型）。
        task_type: 发现任务的 task_type（handler 名）。
        fetch_func: ``(job, ctx, page) -> list[item]``。每次扫描从 page=1
            开始；返回空列表表示页尾（T1a）。
        id_func: ``(item) -> content_id: str``。内容项全局唯一 id（不要求
            单调递增）。返回非空 str，否则该 item 被隔离跳过。
        process_item_func: ``(job, ctx, item, content_id) -> None``。对每个
            新 item 的处理逻辑（通常 spawn 一个子任务）。content_id 已经
            单射转义净化（不含 "::"、长度受限），可直接用作
            ``Job.job_id``；spawn 的 task_type 必须是 ``process_task_type``。
        process_task_type: process 子任务的 task_type——整页命中判定按
            ``f"{process_task_type}::{content_id}"`` 查询 wall/failed，
            与用户 spawn 时必须一致。
        cursor_key_func: 可选。``(payload) -> str``，仅作 job_id 命名空间
            前缀（防跨分组内容 id 碰撞），无任何游标/互斥语义。
        default_resources: 发现任务自身的默认资源需求。
        payload_schema: 发现任务 payload 的运行时校验 schema。
        max_pages: 单次扫描页数硬上限（活性保障条款，默认 1000）。
        rerun: discovery job 的默认跨会话重跑策略（默认
            ``"every_run"``）——enqueue 该 task_type 的 job 时自动注入
            （用户显式指定非默认策略则尊重）。发现任务用固定 uid
            （如 ``discover::favorites``）每会话重扫，无需时间戳后缀。
        scan_mode: 扫描模式：``"incremental"``（默认，
            整页命中终止）/ ``"full"``（跳过整页命中，一路扫到空页或
            max_pages——全量成本只在 fetch，已见内容仍逐条跳过不重复
            spawn）。
        on_missing: 可选回调（仅 full 模式有意义）。
            ``(job, ctx, missing_content_ids) -> None``——扫描结束后把
            「已见但本次未扫到」的 content_id 差集交给业务（= 源端删除/缺失
            检测；增量模式原理上给不出该信息）。必须模块级可 pickle。

    回调必须为模块级可 pickle 函数（spawn 进程隔离约束，与普通 handler
    一致——lambda/闭包在子进程 import 时失败）。
    """
    if not isinstance(task_type, str) or not task_type:
        raise TypeError(
            f"task_type must be a non-empty str, "
            f"got {type(task_type).__name__} ({task_type!r})"
        )
    if "::" in task_type:
        raise ValueError(
            f"task_type must not contain '::' (Job.uid separator), "
            f"got {task_type!r}"
        )
    if not isinstance(process_task_type, str) or not process_task_type:
        raise TypeError(
            f"process_task_type must be a non-empty str, "
            f"got {type(process_task_type).__name__} ({process_task_type!r})"
        )
    if "::" in process_task_type:
        raise ValueError(
            f"process_task_type must not contain '::' (Job.uid separator), "
            f"got {process_task_type!r}"
        )
    for name, fn in (
        ("fetch_func", fetch_func),
        ("id_func", id_func),
        ("process_item_func", process_item_func),
    ):
        if not callable(fn):
            raise TypeError(f"{name} must be callable, got {type(fn).__name__}")
    if cursor_key_func is not None and not callable(cursor_key_func):
        raise TypeError(
            f"cursor_key_func must be callable or None, "
            f"got {type(cursor_key_func).__name__}"
        )
    # 代码级强制：discovery 回调必须模块级可 pickle，否则 spawn
    # 子进程必然失败——把文档约定变成注册期 fail-loud。
    for name, fn in (
        ("fetch_func", fetch_func),
        ("id_func", id_func),
        ("process_item_func", process_item_func),
        ("cursor_key_func", cursor_key_func),
        ("on_missing", on_missing),
    ):
        if fn is None:
            continue
        try:
            pickle.dumps(fn)
        except Exception as e:
            raise TypeError(
                f"{name} must be a module-level picklable function for "
                f"spawn subprocess propagation, got {fn!r}: {e}"
            ) from e
    if not isinstance(max_pages, int) or isinstance(max_pages, bool) or max_pages < 1:
        raise ValueError(f"max_pages must be a positive int, got {max_pages!r}")
    if rerun not in RERUN_VALUES:
        raise ValueError(
            f"rerun must be one of "
            f"{'/'.join(repr(v) for v in RERUN_VALUES)}, got {rerun!r}"
        )

    handler = DiscoveryHandler(
        fetch_func,
        id_func,
        process_item_func,
        process_task_type,
        cursor_key_func=cursor_key_func,
        max_pages=max_pages,
        scan_mode=scan_mode,
        on_missing=on_missing,
    )
    # 框架无关适配：只调用宿主公开协议的两个方法，不触碰任何私有字段/
    # 内部注册表——并发安全由 wall ACID + 去重保证。
    host.register_handler(task_type, handler, default_resources, payload_schema)
    # 记录 discovery 默认 rerun 供 enqueue 注入
    host.set_discovery_rerun(task_type, rerun)
