"""框架默认的**管线脚手架**通用工具。

与非领域相关，任何“驱动 pipeline → 跑 → 停机 / 清除 DLQ / 确定性 job_id /
进度输出 / 文件系统瞬态注册”的消费方都可以复用。各种领域编排管线均可依赖本模块。

涵盖：
- ``run_pipeline`` / ``clear_dlq`` / ``job_ref`` / ``progress_hook`` /
  ``slice_list``：驱动与运维公共部分；
- ``content_fingerprint``（确定性版本盐指纹）与 ``sanitize_job_component``：
  内容任务 job_id 的通用派生原语（对应“job_id 必须确定性、不含 ::、
  无随机后缀”红线）；
- ``register_file_transients``：把常见文件系统环境错误注册为本 pipeline
  的瞬态异常（磁盘满/只读/IO 抖动可重试，文件消失等确定性错误不注册）。
"""

import hashlib
import logging
import re
from typing import Iterable, List, Optional, Sequence, Union

from .pipeline import TaskLite

logger = logging.getLogger(__name__)


def run_pipeline(pipeline: TaskLite) -> None:
    """统一 run + 停机包装：Ctrl+C 触发优雅 DRAINING 后正常返回。

    库函数不做进程级副作用（不 sys.exit、不 print）——退出码与用户提示
    属于 CLI 层职责。KeyboardInterrupt 经 stop 转为 DRAINING 优雅停机，
    run 自然返回后本函数也返回；stop 自身的异常记录到 logger 而非
    静默吞掉。
    """
    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，请求优雅停机（DRAINING）……")
    finally:
        try:
            pipeline.stop()
        except Exception:
            # stop 失败不掩盖原始异常，但必须留下痕迹而非静默
            logger.exception("run_pipeline 收尾 stop 失败")


def clear_dlq(
    pipeline: TaskLite,
    *,
    task_types: Optional[Sequence[str]] = None,
    keep_fatal: bool = True,
) -> int:
    """官方 ``pipeline.clear_dlq`` 的便捷包装（清 DLQ 供重跑）。

    - 前置任务类型（discovery / orchestrator）中断残留 → 清后可重跑；
    - ``keep_fatal=False`` 连确定性失败一并清（如用户修复 Cookie/LLM 欠费后）。

    严格透传：仅 ``None`` 表示全部；空列表如实传递（按 0 个类型过滤 =
    删除 0 条），不做 truthy 改写——否则 ``task_types=[]`` 会被静默
    当作 ``None``（清除全部 DLQ 条目）。
    """
    return pipeline.clear_dlq(
        task_types=list(task_types) if task_types is not None else None,
        keep_fatal=keep_fatal)


def job_ref(meta) -> str:
    """从 result_meta 提取人类可读引用（#作品id / 画师名 / 任意 id 字段）。"""
    if isinstance(meta, dict):
        if "post_id" in meta:
            return f"#{meta['post_id']}"
        if "artist" in meta:
            return meta["artist"]
        if "id" in meta:
            return str(meta["id"])
    return ""


def progress_hook(uid: str, meta, success: bool, going_to_retry: bool) -> None:
    """父进程侧每 job 终结回调：一行进度输出（钩子）。"""
    task_type = uid.split("::", 1)[0]
    ref = job_ref(meta)
    label = f"{task_type} {ref}" if ref else task_type
    if going_to_retry:
        print(f"  ⟳ {label} 失败，退避重试", flush=True)
    elif success:
        print(f"  ✓ {label}", flush=True)
    else:
        print(f"  ✗ {label} → DLQ", flush=True)


def slice_list(items: List, start: Optional[int], count: Optional[int], limit: Optional[int]) -> List:
    """分批切片：先 limit，再 [start:start+count]。"""
    if limit is not None:
        items = items[:limit]
    if start is not None or count is not None:
        s = start if start is not None else 0
        c = count if count is not None else len(items)
        items = items[s:s + c]
    return items



# 确定性 job_id 原语


def content_fingerprint(parts: Iterable[Union[str, int, float]], *, version: str = "") -> str:
    """内容指纹 → 确定性 job_id 片段（sha1，截 16 位，可读）。

    任何输入变化（含 bump ``version`` 盐）→ 指纹变化 → wall 不命中 → 重跑。
    与“job_id 确定性、禁止随机后缀”红线一致：给定输入恒同指纹。
    """
    h = hashlib.sha1()
    if version:
        h.update(str(version).encode("utf-8", errors="replace"))
        h.update(b"\x00")
    for p in parts:
        h.update(str(p).encode("utf-8", errors="replace"))
        h.update(b"\x00")
    return h.hexdigest()[:16]


# job_id 成分转义禁止集：路径分隔符（文件名安全）、冒号（uid 的 ::
# 分隔符冲突）、转义符本身（先转义 % 才能保证编码序列无歧义）。
_JOB_COMPONENT_FORBIDDEN = "/\\:%"


def _escape_job_component(text: str) -> str:
    """单射转义：禁止字符/不可打印字符按 UTF-8 字节 → ``%XX``（固定 2 位）。

    转义符 ``%`` 本身属于禁止集、总是先被转义为 ``%25``，因此输出中的
    ``%`` 只可能开启转义序列且必跟恰好 2 位 hex，永不与用户输入的 ``%``
    混淆——这是映射可唯一逆解析（单射）的关键。
    """
    parts = []
    for ch in text:
        if ch.isprintable() and ch not in _JOB_COMPONENT_FORBIDDEN:
            parts.append(ch)
        else:
            parts.append("".join(f"%{b:02X}" for b in ch.encode("utf-8")))
    return "".join(parts)


def sanitize_job_component(value: str, *, max_len: int = 120) -> str:
    r"""把任意字符串**转义**净化为可作 job_id 成分的串（单射，不删字符）。

    转义规则（与 ``discovery.sanitize_content_id`` 同款百分号转义哲学）：
    可打印且非 ``/ \ : %`` 的字符原样保留；其余字符——路径分隔符 ``/``
    与 ``\``、冒号 ``:``（uid 的 ``::`` 分隔符冲突）、不可打印字符、转义符
    ``%`` 本身——按 UTF-8 字节逐个转义为 ``%XX``（固定 2 位 hex）。输出
    因此不含 ``::``、路径分隔符与控制字符，可直接作文件名安全成分；
    全程无随机化，同一输入恒同输出。

    单射性保证（不同输入必产生不同输出）：用户输入的 ``%`` 总被转义为
    ``%25``，故输出中的 ``%`` 只可能开启一个转义序列且必跟恰好 2 位 hex；
    据此输出可被唯一切分为「字面字符 | %XX 字节」标记流，字节流再经
    UTF-8（前缀无关编码）唯一还原为输入字符序列，映射在数学上单射。

    为什么删除式净化不可用：直接删掉禁止字符是多对一映射——
    ``'a/b'``、``'a:b'``、``'a\tb'`` 全部变 ``'ab'``，``':::'`` 与
    ``'///'`` 全部变 ``'untitled'``。本函数的输出会拼进 job_id → uid，
    两个不同任务净化后撞同一 uid 时，wall 去重会把后到任务判定为已完成
    而静默吞掉（数据丢失且无任何报错）。转义保留全部信息，从根上消除
    这类碰撞；仅空串输入无信息可保留，返回 ``'untitled'`` 兜底。

    超长输入（转义后超 ``max_len``）：截断 + 8 位 SHA256 指纹后缀——纯
    截断同样破坏单射（前缀相同的长输入会相撞），指纹使截断碰撞概率可
    忽略（与 sanitize_content_id 的超长策略一致）。
    """
    text = str(value)
    if not text:
        return "untitled"
    escaped = _escape_job_component(text)
    if len(escaped) <= max_len:
        return escaped
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]
    base_limit = max(max_len - 9, 0)  # "_" 分隔符 + 8 位指纹；极小 max_len 下退化为仅指纹
    return f"{escaped[:base_limit]}_{digest}"



# 文件系统瞬态注册（转码等本地 IO 重的消费方典型需求）


# 默认注册为瞬态的文件系统异常（磁盘满/只读/IO 抖动等环境问题重试合理）
_DEFAULT_TRANSIENT_FS = (PermissionError, BlockingIOError, ConnectionResetError)


def register_file_transients(
    pipeline: TaskLite,
    *,
    classes: Sequence[type] = _DEFAULT_TRANSIENT_FS,
    message: str = "文件系统异常已注册为瞬态（可重试）",
) -> None:
    """把文件系统环境错误注册为 pipeline 瞬态异常。

    注意：``FileNotFoundError`` 是确定性错误（源文件消失重试无意义），默认
    不注册；需要时调用方可显式传入。
    """
    for cls in classes:
        pipeline.register_transient_exception(cls)
    import logging
    logging.getLogger("tasklite").info(
        f"{message}: {', '.join(c.__name__ for c in classes)}"
    )
