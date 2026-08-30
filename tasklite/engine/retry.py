"""重试与退避的纯逻辑。

与 pipeline 的边界：本模块不含任何状态——输入输出皆为纯值。退避计算
（``compute_backoff``）与 rerun 策略判定（``rerun_skips``/``input_changed``）
是调度循环的纯逻辑部分。
"""

import logging
import math
import os
import random

from typing import Optional

logger = logging.getLogger("tasklite")


def compute_backoff(
    retries: int,
    backoff_base: float = 2.0,
    backoff_max: float = 300.0,
) -> float:
    """Compute exponential backoff delay with ±25% jitter.

    Formula:  min(base * 2^(retries-1), backoff_max) * [0.75, 1.25]

    Returns a non-negative delay in seconds.
    - ``retries <= 0`` returns 0.0 immediately (no delay for first attempt).
    - Negative retries are treated as 0 with a warning.
    - NaN/Inf inputs for backoff_base/backoff_max fall back to defaults (2.0/300.0).
    """
    if retries < 0:
        logger.warning(f"compute_backoff called with negative retries={retries}, treating as 0")
        return 0.0
    if retries == 0:
        return 0.0
    # 防御：NaN/Inf 输入 fallback 到默认值（Job.__init__ 已校验，此处防御直接调用）
    if not math.isfinite(backoff_base) or backoff_base < 0:
        backoff_base = 2.0
    if not math.isfinite(backoff_max) or backoff_max < 0:
        backoff_max = 300.0
    # retries 很大时 2**(retries-1) 会溢出 float
    # （OverflowError: int too large to convert to float，retries>=~1025），
    # 直接炸掉重试路径。封顶指数（2**60 远高于任何合理的 backoff_max，
    # 随后的 min(..., backoff_max) 兜底），保持公式语义不变。
    exp = min(retries - 1, 60)
    base_delay = backoff_base * (2 ** exp)
    delay = min(base_delay, backoff_max)
    jitter = random.uniform(-0.25, 0.25) * delay
    return max(0.0, delay + jitter)


def input_changed(wall_meta: dict) -> bool:
    """on_input_change：wall 里的输入指纹与当前文件重新 stat 比对。

    任一文件输入 size/mtime_ns 变化（或文件消失、
    或 wall 无历史指纹）→ 视为变化（True）→ 重跑。URI 声明不参与
    比对（变更检测默认关闭）。

    纯静态逻辑（无 self 依赖；比对只依赖 wall 历史指纹与磁盘现状）。
    """
    prev_inputs = wall_meta.get("inputs") if isinstance(wall_meta, dict) else None
    # meta["inputs"] 脏数据防御——非 list（dict/str）按无历史指纹处理，
    # 避免 for 迭代出 str 后 entry.get AttributeError 在加载期崩溃。
    if not isinstance(prev_inputs, list):
        return True  # 无历史指纹/损坏 → 首次 on_input_change，视为变化
    # per-element 防御——损坏的 wall 指纹若为 ``[非 dict]``
    # （如 [42, "str"]），``entry.get`` 会 AttributeError 崩掉 run（加载期
    # 判定 on_input_change 时）。非 dict 元素视为损坏条目，整个按无历史
    # 指纹处理（返回 True = 视为变化重跑，fail-safe）。
    for entry in prev_inputs:
        if not isinstance(entry, dict):
            return True
        if entry.get("kind") == "uri":
            continue
        path = entry.get("path")
        if not path:
            continue
        if not isinstance(path, str):
            # path 为 dict/list 等真值但非 str 时，
            # os.stat(path) 抛 TypeError（不被 except OSError 捕获）→ 穿透
            # run 崩溃。视为损坏条目 → 返回 True（变化重跑，fail-safe）。
            return True
        size = entry.get("size")
        mtime_ns = entry.get("mtime_ns")
        if size is None or mtime_ns is None:
            return True  # 历史指纹缺失（stat 失败过）→ 视为变化
        try:
            st = os.stat(path)
        except OSError:
            return True  # 文件消失 → 变化
        if st.st_size != size or st.st_mtime_ns != mtime_ns:
            return True
    return False


def rerun_skips(
    job_dict: dict, *, wall_hit: bool, failed_hit: bool,
    wall_meta: Optional[dict] = None,
) -> bool:
    """rerun 策略决定 wall/failed 命中是否拦截（True=跳过，False=放行重跑）。

    策略只豁免 **wall/failed 集合**的拦截；queue/in-flight
    永远算数（同一轮内不重复派发/并发双跑）。调用方必须先确认命中来自
    wall/failed（而非 queue/in-flight），再传入对应的 wall_hit/failed_hit。

    策略矩阵（None/缺键同按 "never"——未指定哨兵语义）：
    - None / "never"：wall/failed 都拦截（现状语义）；
    - "on_failure"：仅 failed 拦截豁免（重跑），wall 仍拦截；
    - "every_run"：wall/failed 都豁免（重跑）；
    - "on_input_change"：failed 命中豁免；wall 命中比对输入指纹
      （``input_changed``）——变则豁免（重跑），
      不变则拦截。需要调用方传入 ``wall_meta``（wall 中的旧指纹）。
    """
    # None（未指定哨兵）与缺键同按 "never" 处理
    rerun = job_dict.get("rerun") or "never"
    if rerun == "every_run":
        return False
    if rerun == "on_failure":
        return not failed_hit  # failed 命中豁免；wall 命中拦截
    if rerun == "on_input_change":
        if failed_hit:
            return False
        if wall_hit:
            return not input_changed(wall_meta or {})
        return True
    return True # never


def apply_discovery_rerun(
    job_dict: dict, task_type: str, discovery_rerun: dict,
) -> bool:
    """discovery 默认 rerun 注入单点。

    Job.rerun=None 哨兵区分「未指定」（注入 ``set_discovery_rerun`` 登记
    的默认策略）与「显式指定」（含显式 "never"，一律尊重——不区分的话
    显式 never 会被默认策略静默覆盖）。enqueue 与 spawn 两条路径都必须
    经本函数注入。

    Returns:
        bool: 是否发生了注入。
    """
    disc_rerun = discovery_rerun.get(task_type)
    if disc_rerun and job_dict.get("rerun") is None:
        job_dict["rerun"] = disc_rerun
        return True
    return False
