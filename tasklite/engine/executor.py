"""Multiprocessing executor for tasklite.

Encapsulates the subprocess lifecycle and IPC result handling.

IPC 模型：结果走**落盘文件**而非 mp.Queue——``mp.Queue.get_nowait()``
是伪非阻塞（管道中出现「部分消息 + 进程存活(挂起)」时会在 ``_recv``
上无限阻塞，主循环卡死，超时检查与 SIGKILL 永远执行不到），且超时宽限
``join(5.0)`` 串行阻塞主循环、拖累其他 in-flight handle 的回收与
SIGTERM 响应。

- 子进程把结果写 ``{uid}.{incarnation}.result.json.tmp`` → ``os.replace``
  → ``{uid}.{incarnation}.result.json``（与后端 ``_atomic_write_json`` 同构的
  原子写；文件系统保证「要么全有要么全无」，不存在部分消息）。
- 父进程 ``drain()`` 只做 ``os.path.exists(result_path)`` 非阻塞轮询——无管道、
  无阻塞点、无 EOF 问题；超时判定独立于文件读取。
- suspend 信号写 ``{uid}.signals.jsonl`` 追加行——进程被 kill 后文件仍在，
  信号不丢（即时性由文件 flush 保证）。
- 附带收益：写盘时就必须 JSON 序列化（result_meta JSON 预检变强制）。
"""

# 本文件签名注解引用 Callable/mp 等名字——
# Python 3.10–3.13 注解在 def 执行时立即求值，缺导入即 NameError（声明
# 的 >=3.10 实际不可用）；3.14 因 PEP 649 惰性注解侥幸存活，但任何注解
# 求值（inspect.signature/get_type_hints）仍炸。future import 兜底 +
# 补齐真实导入双保险。
from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import re
import signal
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..exceptions import (
    FatalError, RetryError, classify_exception,
)
from ..models.job import Job
# IPC 写入侧（append_*/路径构造/后缀常量）已下沉 utils.ipc——TaskContext
#（models 层）直接消费，models 不得 import engine。此处导入保持 executor
# 模块内名字可用（既有外部引用经 executor 路径取这些名字）。
from ..utils.ipc import (
    append_input, append_output, append_signal,
    inputs_path, outputs_path, signals_path,
)
from ..utils.jsonutil import dump, dumps, loads, load as json_load
from ..utils.lockfile import safe_uid_filename

logger = logging.getLogger("tasklite")

_KEY_TRACEBACK = "traceback"

# 结果文件的扩展名（inputs/outputs/signals 声明文件的后缀在 utils.ipc）
_RESULT_TMP_SUFFIX = ".result.json.tmp"
_RESULT_SUFFIX = ".result.json"

# 执行身份 fencing：结果文件名携带 incarnation
# ``{uid}.{run_id}.{seq}.result.json``，run_id 为 32 位 hex uuid。
# ``drain`` 只查当前 incarnation 路径——上次 run 崩溃后遗留的孤儿进程
# 写出的旧 incarnation 结果文件不会被新 run 看见（防止「孤儿结果被
# 误认为新子进程的结果而 kill 新子进程 + commit 孤儿上下文」）。
# 精确匹配正则（32-hex + 数字）：消除 uid 前缀歧义（job_id 可含点，
# glob ``{uid}.*`` 会误匹配 ``{uid}.X`` 这类更长 uid 的文件）。
_INCARNATION_RE = re.compile(r"\.([0-9a-f]{32})\.(\d+)\.result\.json$")

# 结果目录环境变量（测试可覆盖；默认由 pipeline 在 state_dir 下创建）
_RESULT_DIR_ENV = "TASKLITE_IPC_DIR"

# raw_result 的类型标记：JSON 无法保留 Python tuple，落盘前
# 编码类型、读取后还原。None/True/False/数字/字符串/列表/字典 JSON 原生可
# 保真；只有 tuple 需要编码（decode 后还原为 tuple 传给
# _normalize_handler_result）。
# list-marker 方案：tuple → [哨兵, items]，decode 要求外层是 list 且首元素
# 为带版本号的哨兵——用户 dict 永不误判；用户 list 撞哨兵需 len==2 且
# [0] 恰为哨兵字符串（带 v1 版本号，不可枚举）。
_RAW_TUPLE_SENTINEL = "__tl_tuple_v1"


def _encode_raw_result(raw_result: Any) -> Any:
    """编码 handler 返回值以便 JSON 落盘。

    JSON 只支持 list，不支持 tuple——tuple(bool, dict) 是合法 handler
    返回类型，必须保真。编码：tuple → [哨兵, list(items)]，
    其余类型原样（JSON 原生保真）。
    """
    if isinstance(raw_result, tuple):
        return [_RAW_TUPLE_SENTINEL, list(raw_result)]
    return raw_result


def _decode_raw_result(encoded: Any) -> Any:
    """读取结果文件后还原 handler 返回值（_encode_raw_result 的逆操作）。

    健壮性保障：还原段必须是 list/tuple，否则视为用户原始 list 原样返回
    ——用户 handler 合法返回 ``["__tl_tuple_v1", 123]``（如透传上游协议标记）
    时，``tuple(123)`` 会抛 TypeError 穿透 drain 崩掉整个 run；
    ``[..., {"a": 1}]`` 会被静默转成 ``('a',)`` 使成功的 job 被假 DLQ。
    下游 _normalize_handler_result 会以明确的 invalid-return-type
    消息判失败进 DLQ——宁可 DLQ，不可崩。
    """
    if (isinstance(encoded, list) and len(encoded) == 2
            and encoded[0] == _RAW_TUPLE_SENTINEL
            and isinstance(encoded[1], (list, tuple))):
        return tuple(encoded[1])
    return encoded



# 文件级 IPC 辅助



def result_path(ipc_dir, uid: str, incarnation: str) -> Path:
    """某个 job 的最终结果文件路径。

    返回 ``{uid}.{incarnation}.result.json``，incarnation 为执行代标识。
    """
    suffix = f".{incarnation}{_RESULT_SUFFIX}"
    return Path(ipc_dir) / f"{safe_uid_filename(uid)}{suffix}"


def result_tmp_path(ipc_dir, uid: str, incarnation: str) -> Path:
    """某个 job 的结果临时文件路径（子进程写入中）。"""
    suffix = f".{incarnation}{_RESULT_TMP_SUFFIX}"
    return Path(ipc_dir) / f"{safe_uid_filename(uid)}{suffix}"


def _iter_stale_result_paths(ipc_dir, uid: str) -> List[Path]:
    """枚举某个 uid 的全部**残留**结果文件路径。

    返回任意 incarnation 的 ``{uid}.{run_id}.{seq}.result.json`` 匹配项。
    用正则精确过滤（job_id 可含点：glob ``{uid}.*`` 会误匹配 ``{uid}.X``
    这类更长 uid 的文件，正则要求 incarnation 段为 32-hex + 数字）。

    同时枚举 ``.result.json.tmp`` 半成品（kill 中断原子写的残留）——
    不清理会随崩溃/重试无限累积（长跑管线 ipc 目录膨胀）。
    """
    d = Path(ipc_dir)
    base = f"{safe_uid_filename(uid)}"
    found: List[Path] = []
    try:
        for pat in (f"{base}.*{_RESULT_SUFFIX}", f"{base}.*{_RESULT_TMP_SUFFIX}"):
            for p in d.glob(pat):
                # .match 从 ``len(base)`` 处锚定，兄弟剩余部分
                # （如 `.1.`）不满足 ``\.hex32\.\d+`` 而整体失配。
                if _INCARNATION_RE.match(p.name, pos=len(base)):
                    found.append(p)
    except OSError:
        pass
    return found


def read_inputs(ipc_dir, uid: str) -> List[dict]:
    """读取一个 job 的全部输入声明（落盘文件）。

    返回 ``[entry, ...]``；文件不存在/坏行跳过。**不删除文件**。
    """
    path = inputs_path(ipc_dir, uid)
    entries: List[dict] = []
    try:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = loads(line)
                        if isinstance(data, dict) and isinstance(data.get("path"), str):
                            entries.append(data)
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue
    except OSError:
        pass
    return entries


def read_outputs(ipc_dir, uid: str) -> List[Tuple[str, bool, str]]:
    """读取一个 job 的全部已声明输出（落盘文件）。

    返回 ``[(path, cleanup, kind), ...]``；文件不存在/坏行跳过。
    **不删除文件**——outputs.jsonl 的生命周期由 ``_complete_job``
    （成功路径 finally 读后删除）与 ``_abort_in_flight``（经
    ``_cleanup_outputs`` 读后删除）管理，**不属于本函数、也不属于
    cleanup_ipc_files**（其不清理 outputs.jsonl）。``_dispatch_job`` 也会在
    submit 前清崩溃残留的声明文件（outputs.jsonl/inputs.jsonl，幂等 no-op）。
    声明行必须带 ``kind``（``"output"`` / ``"cache"``）；缺 kind 的旧行跳过。
    """
    path = outputs_path(ipc_dir, uid)
    outputs: List[Tuple[str, bool, str]] = []
    try:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = loads(line)
                        if isinstance(data, dict) and isinstance(data.get("path"), str):
                            kind = data.get("kind")
                            if not isinstance(kind, str):
                                continue
                            outputs.append((
                                data["path"],
                                bool(data.get("cleanup", True)),
                                kind,
                            ))
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue  # 坏行跳过
    except OSError:
        pass
    return outputs


def write_result_atomic(
    ipc_dir, uid: str, result_dict: dict, incarnation: str
) -> None:
    """原子写结果：先写 .tmp 再 os.replace。失败时清理 .tmp。

    allow_nan=False——子进程侧对 handler 返回值的最后一道序列化关卡，
    与 _normalize_handler_result 的预检对齐，杜绝 NaN/Infinity 写入结果文件。
    rename 后 fsync 父目录（与后端原子写同构），
    保证断电后 rename 持久化，否则崩溃恢复（consume_stale_result）可能读不到结果。
    """
    tmp = result_tmp_path(ipc_dir, uid, incarnation)
    final = result_path(ipc_dir, uid, incarnation)
    tmp.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            dump(result_dict, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, final)
        # 目录 fsync：确保 rename 的目录条目在断电时随文件一起落盘
        try:
            dir_fd = os.open(str(final.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _write_result_with_degradation(
    ipc_dir, uid: str, payload: Dict[str, Any], incarnation: str
) -> None:
    """worker 结果落盘的唯一出口：完整写失败时两级降级，绝不裸抛 OSError。

    主路径各分支若裸调 write_result_atomic——磁盘满/瞬态 IO 故障时
    OSError 直接穿透，worker 裸崩退出（无结果文件）→ 父进程按崩溃收割
    且不设 retry_requested → **成功执行的 job 被误判 DLQ、成果永久丢失**。
    统一降级链：

    1. 完整 payload 写盘（含 raw_result/new_jobs/cursor_updates 全部字段）；
    2. 失败（OSError）记 warning，短暂停顿后重试一次完整写——给瞬态
       IO 故障一个自愈窗口；
    3. 仍失败则写「降级结果」——原 status 为 "success" 时改写
       {"status": "retry", "error": "IPC_RESULT_WRITE_DEGRADED: ..."}：
       成果虽未能落盘，但让 job 走重跑（at-least-once 契约本就以重跑
       吸收副作用，静默丢失 spawned jobs/cursor_updates 更糟）；其他
       status 保持原语义（retry/fatal/error/interrupted 不因写失败
       改变判定）。无 status 的异常 payload 也归入 retry（宁可重跑）。
       结构化 lock_conflict 字段保留——判定端读字段而非 error 前缀，
       丢失会把框架锁冲突误当业务 retry 烧 max_retries 预算；
    4. 降级写也失败（磁盘满到连小 payload 都写不下）记 error 后放弃
       ——worker 无结果退出，由 drain 按崩溃语义收割。
    """
    try:
        write_result_atomic(ipc_dir, uid, payload, incarnation=incarnation)
        return
    except OSError as e:
        logger.warning(
            f"result write failed for {uid}: {e}; "
            f"retrying full payload once after brief pause"
        )
    time.sleep(0.05)
    try:
        write_result_atomic(ipc_dir, uid, payload, incarnation=incarnation)
        return
    except OSError as e:
        # except 块结束后 ``as e`` 绑定即被清除——降级 payload 的错误
        # 消息必须在块内先取出，块外引用会 UnboundLocalError。
        write_err = str(e)
        logger.warning(
            f"full result write retry also failed for {uid}: {e}; "
            f"falling back to degraded result"
        )
    orig_status = payload.get("status")
    degraded_status = "retry" if orig_status in (None, "success") else orig_status
    degraded: Dict[str, Any] = {
        "status": degraded_status,
        "error": f"IPC_RESULT_WRITE_DEGRADED: {write_err}",
    }
    if payload.get("lock_conflict"):
        degraded["lock_conflict"] = True
    try:
        write_result_atomic(ipc_dir, uid, degraded, incarnation=incarnation)
    except OSError as e2:
        logger.error(
            f"degraded result write also failed for {uid}: {e2}; "
            f"worker exiting without IPC result"
        )


def read_result_file(path: Path) -> Optional[dict]:
    """读取结果文件；损坏/不存在返回 None（宁可重跑，不可崩）。"""
    try:
        with open(path, encoding="utf-8") as f:
            data = json_load(f)
        if isinstance(data, dict):
            return data
        logger.warning(f"Corrupt result file {path}: not a dict, ignoring")
        return None
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, OSError, TypeError, ValueError) as e:
        logger.warning(f"Corrupt result file {path}: {e}, ignoring")
        return None


def read_signals(ipc_dir, uid: str) -> List[Tuple[str, float]]:
    """读取并**删除**一个 job 的 suspend 信号文件（排空语义）。

    防止并发写入丢失：读后截断再删——「读完整个文件 → unlink」会把
    worker 在读取进行中追加的**未读到信号**一并删除（丢失）。读后
    ``truncate(0)`` 再删：读取期间追加的行保留在文件里，由下一轮
    drain 消费，不随本轮丢失。
    """
    path = signals_path(ipc_dir, uid)
    signals: List[Tuple[str, float]] = []
    try:
        if path.exists():
            # "r+" 读写模式——截断需要写权限；只读模式下 f.truncate 抛
            # io.UnsupportedOperation（OSError 子类）被下方防御静默吞掉，
            # 截断沦为死代码。权限不足时 open 即抛
            # PermissionError，同样走外层 OSError 防御：信号照常读出，
            # 仅放弃截断（优雅降级）。
            with open(path, "r+", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = loads(line)
                        if isinstance(data, dict) and "suspend" in data:
                            r_name, secs = data["suspend"]
                            signals.append((r_name, secs))
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue # 坏行跳过
                # 读后先截断：读取期间 worker 追加的新信号不随本轮丢失
                try:
                    f.seek(0)
                    f.truncate(0)
                except OSError:
                    pass
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass
    except OSError:
        pass
    return signals


def cleanup_ipc_files(ipc_dir, uid: str, incarnation: Optional[str] = None) -> None:
    """删除某个 job 的结果/信号/临时文件（**不含** outputs.jsonl）。

    incarnation 提供时精确删除当前执行代的文件；同时始终清理任意
    incarnation 的残留变体（孤儿文件由 consume_stale_result 消费后清理，
    防止目录无限膨胀）。

    outputs.jsonl 的生命周期由 ``_complete_job``/``_abort_in_flight``
    管理（``_cleanup_outputs`` 读后删除）——drain 在此处删掉它，
    会让随后的输出清理读不到声明。

    ``{uid}.lock`` 锁文件**永不在此清理**——unlink 后
    新进程 create 同名文件拿到的是新 inode 的锁，与旧持锁者不互斥
    （经典 unlink-recreate 竞争）。锁文件跨 run 同名、累积可接受，
    由运维在 pipeline 停止时整目录离线清理。
    """
    targets = [signals_path(ipc_dir, uid)]
    if incarnation is not None:
        targets += [
            result_path(ipc_dir, uid, incarnation),
            result_tmp_path(ipc_dir, uid, incarnation),
        ]
    targets += _iter_stale_result_paths(ipc_dir, uid)
    for p in targets:
        try:
            p.unlink()
        except FileNotFoundError:
            pass  # 文件本就不存在（未写入/已删除）
        except OSError:
            pass



# Worker 包装（在子进程内运行）



def _mp_worker_wrapper(handler_func, job, ctx, ipc_dir) -> None:
    """Wrapper for multiprocessing worker execution.

    异常分类三态契约（retry / fatal / error）：
    - RetryError（显式抛出）→ "retry"
    - FatalError（显式抛出）→ "fatal"
    - 注册表（用户显式声明，优先于内置 FATAL 启发式）→ "retry"
    - FATAL_EXCEPTIONS（内置启发式）→ "fatal"
    - TRANSIENT_EXCEPTIONS（内置启发式）→ "retry"
    - 其余 Exception（Unknown）→ "error"

    注册表快照契约：父进程在 submit 时把 **per-pipeline 注册表快照**
    随 ctx pickle 下发（``ctx.transient_registry``，不可变 tuple），此处
    直接用 ``classify_exception`` 消费——**不重放、不写任何模块级可变
    注册表**。spawn 子进程是全新解释器，函数作用域的注册不随进程继承；
    模块级类可 pickle（注册时已预检 fail-loud），快照判定与父进程一致。

    结果经 ``_write_result_with_degradation`` 落盘（原子 rename + 完整写
    失败时的两级降级），文件名携带 incarnation（ctx.incarnation 由
    submit 时写入）——崩溃后的孤儿进程用旧 incarnation
    写文件，新 run 的 drain 不会看见。
    """
    uid = job.uid
    incarnation = getattr(ctx, "incarnation", None)
    # incarnation 缺失 fail-loud——结果文件名
    # 携带执行代标识，None 会写出 "{uid}.None.result.json" 垃圾路径：drain
    # 永远看不见它 → job 被判 NO_IPC_RESULT 判死，且孤儿结果文件永久残留。
    # 这只可能是框架内部装配错误（submit 未写 ctx.incarnation），尽早暴露。
    if not incarnation:
        raise RuntimeError(
            f"worker started without incarnation for {uid}: "
            f"fencing requires ctx.incarnation set by submit"
        )
    # 注册表快照是 ctx 的不可变字段——分类只读它，不触碰任何
    # 父进程模块级全局。
    transient_registry = getattr(ctx, "transient_registry", ()) or ()
    # worker 子进程入口持有 {uid}.lock 排他锁（≤2s 短等待）。
    # 锁的生命周期 = 执行体生命周期——主进程崩溃后孤儿 worker 仍持锁，
    # 新 run 的探测失败 → requeue 而非双跑。拿锁超时 = 另有
    # 执行体（同 uid 孤儿仍活）→ 按瞬态重试（自恢复：孤儿死后重跑成功）。
    from ..utils.lockfile import try_acquire_lock, release_lock
    # None 仅=「锁被其他执行体占用」（瞬态，孤儿死后自恢复）；锁文件
    # 打不开/建不出（权限、磁盘满等环境故障）由 try_acquire_lock 抛
    # OSError——此处不捕获：环境错误应 fail-loud（worker 非零退出 →
    # drain 按崩溃收割判死），而非误当锁冲突无限静默重试。
    _lock_fd = try_acquire_lock(ipc_dir, uid, timeout=2.0)
    if _lock_fd is None:
        # lock_conflict 走零计数重试（同 uid 孤儿仍持锁时 handler 未
        # 执行，孤儿死后重跑本可成功，判死进 DLQ 会永久误杀）。
        # status="retry" 使 _decode_ipc_result 走退避重试。
        # 同时写结构化字段 lock_conflict=True——判定端读字段
        # 而非 error 字符串前缀（业务 RetryError 消息可能撞 "LOCK_CONFLICT"
        # 前缀，读前缀会误判为零计数重试）。
        # 此写在主 try 之外——写失败（磁盘满等 OSError）若让 worker 裸崩
        # → 无结果文件 → NO_IPC_RESULT 判死进 DLQ，与「孤儿死后自恢复
        # 重试」的设计意图相反。与主路径共用 _write_result_with_degradation
        # 的两级降级（完整写 → 重试 → 降级写最小结果）；降级时保留
        # lock_conflict 结构化字段（判定端读字段而非 error 前缀，误丢会把
        # 框架锁冲突当业务 retry 烧 max_retries 预算）。
        _write_result_with_degradation(ipc_dir, uid, {
            "status": "retry",
            "lock_conflict": True,
            "error": f"LOCK_CONFLICT: another execution body holds {uid} lock",
        }, incarnation=incarnation)
        return
    try:
        raw_result = handler_func(job, ctx)
        _write_result_with_degradation(ipc_dir, uid, {
            "status": "success",
            "raw_result": _encode_raw_result(raw_result),
            "new_jobs": [j.to_dict() for j in ctx.new_jobs],
            "resource_suspensions": ctx.resource_suspensions,
            "cursor_updates": ctx.cursor_updates
        }, incarnation=incarnation)
    except RetryError as e:
        _write_result_with_degradation(ipc_dir, uid, {"status": "retry", "error": str(e)}, incarnation=incarnation)
    except FatalError as e:
        _write_result_with_degradation(ipc_dir, uid, {
            "status": "fatal",
            "error": str(e),
            _KEY_TRACEBACK: traceback.format_exc()
        }, incarnation=incarnation)
    except Exception as e:
        # 注册表判定先于内置 FATAL 启发式——用户显式声明永远优先。用户
        # 注册了 FATAL 子类时，此处按瞬态重试而非被 FATAL 启发式短路判死。
        # 分类语义唯一来源是 exceptions.classify_exception；fatal/transient
        # 覆盖集合同 transient_registry 一样经 ctx 快照下发（None=用模块
        # 默认元组），子进程分类不读模块级可变全局。
        kind = classify_exception(
            e, transient_registry,
            fatal_exceptions=getattr(ctx, "fatal_exceptions", None),
            transient_exceptions=getattr(ctx, "transient_exceptions", None),
        )
        if kind == "retry":
            # 瞬态异常（连接断开/超时/注册表命中等）自动重试
            _write_result_with_degradation(ipc_dir, uid, {"status": "retry", "error": f"{type(e).__name__}: {e}"}, incarnation=incarnation)
        elif kind == "fatal":
            _write_result_with_degradation(ipc_dir, uid, {
                "status": "fatal",
                "error": f"{type(e).__name__}: {e}",
                _KEY_TRACEBACK: traceback.format_exc()
            }, incarnation=incarnation)
        else:
            _write_result_with_degradation(ipc_dir, uid, {
                "status": "error",
                "error": f"{type(e).__name__}: {e}",
                _KEY_TRACEBACK: traceback.format_exc()
            }, incarnation=incarnation)
    except KeyboardInterrupt as e:
        # 前台进程组的 Ctrl+C 同时送达 worker——写结构化
        # "interrupted" status，消费端零计数回队（不进 DLQ 不级联），兑现
        # README「仅进行中 job requeue」承诺。只覆盖 KeyboardInterrupt：
        # handler 显式 raise SystemExit 是代码 bug/主动退出请求，走下方
        # 分支维持 Unknown 判死（否则会以 1s 间隔无限零计数重跑打转，
        # run 永不返回）。
        _write_result_with_degradation(ipc_dir, uid, {
            "status": "interrupted",
            "error": f"WORKER_INTERRUPTED: {type(e).__name__}",
            _KEY_TRACEBACK: traceback.format_exc()
        }, incarnation=incarnation)
    except SystemExit as e:
        # handler 主动 sys.exit/raise SystemExit——按 handler bug 处理
        # （Unknown 判死进 DLQ），与 KeyboardInterrupt 的外部中断语义分离。
        _write_result_with_degradation(ipc_dir, uid, {
            "status": "error",
            "error": f"WORKER_INTERRUPTED: {type(e).__name__}",
            _KEY_TRACEBACK: traceback.format_exc()
        }, incarnation=incarnation)
    finally:
        # 执行体结束（成功/失败/异常）必须释放锁——锁的
        # 生命周期 = 执行体生命周期。不释放则同 uid 后续派发永远探测失败。
        release_lock(_lock_fd)


def _normalize_handler_result(result: Any) -> Tuple[bool, Dict[str, Any]]:
    """Normalize handler return value to (success, metadata) tuple.

    Accepted return types:
    - None → success, no metadata
    - bool → success/failure, no metadata
    - dict → success, dict as metadata
    - tuple(bool, dict) → explicit success/failure + metadata

    Any other type (int, str, list, object, etc.) is treated as a handler
    bug: returns failure with an error message so the job goes to DLQ
    instead of being silently marked as successful.
    """
    if result is None:
        return True, {}
    if isinstance(result, bool):
        return result, {}
    if isinstance(result, dict):
        # dict 元数据必须 JSON 可序列化。含 bytes/datetime/自定义
        # 对象的 dict 会让 SQLite commit_job_success 失败 → _CommitCrashSignal
        # → 无限重启循环。此处预检，坏 dict 直接进 DLQ。
        # allow_nan=False 与 ctx.spawn/enqueue 的序列化预检对齐——默认
        # allow_nan=True 会让 float('nan')/float('inf') 通过预检，产出非标准
        # JSON "NaN"/"Infinity" 落盘，读回 NaN 污染 wall/cursor 计算。
        try:
            dumps(result)
        except (TypeError, ValueError) as e:
            logger.error(
                f"Handler returned dict with non-JSON-serializable metadata: {e}. "
                f"Job will be marked as failed."
            )
            return False, {"error": f"invalid result metadata: not JSON-serializable: {e}"}
        return True, result
    if isinstance(result, tuple) and len(result) == 2:
        if not isinstance(result[0], bool) or not isinstance(result[1], dict):
            logger.error(
                f"Handler returned invalid tuple: expected (bool, dict), got "
                f"({type(result[0]).__name__}, {type(result[1]).__name__}). Job will be marked as failed."
            )
            return False, {"error": "invalid handler return tuple: expected (bool, dict)"}
        try:
            dumps(result[1])
        except (TypeError, ValueError) as e:
            logger.error(
                f"Handler returned tuple with non-JSON-serializable metadata: {e}. "
                f"Job will be marked as failed."
            )
            return False, {"error": f"invalid result metadata: not JSON-serializable: {e}"}
        return result[0], result[1]
    logger.error(
        f"Handler returned unrecognized type: {type(result).__name__}. "
        f"Expected None, bool, dict, or tuple(bool, dict). "
        f"Job will be marked as failed."
    )
    return False, {"error": f"invalid handler return type: {type(result).__name__}"}


def _decode_ipc_result(
    res: dict, p, job: Job, ipc_dir: Optional[str] = None
) -> "ExecutionResult":
    """解析子进程通过结果文件回传的结果字典。

    复用原 ``execute()`` 中的解析逻辑（含 output 校验）。调用方需保证
    ``res`` 是一个含 ``status`` 键的完整结果 dict。

    输出存在性校验读落盘的 ``{uid}.outputs.jsonl``——handler 声明的
    输出在子进程内立即落盘，主进程不依赖 Manager RPC。``ipc_dir``
    提供时校验；consume_stale_result 路径（无 in-flight）可传 ipc_dir
    完成同样校验。
    """
    success = False
    result_meta: Dict[str, Any] = {}
    retry_requested = False
    retry_error: Optional[str] = None
    new_jobs: List[Job] = []
    cursor_updates: Dict[str, str] = {}
    resource_suspensions: List[Tuple[str, float]] = []
    lock_conflict = False
    interrupted = False

    if isinstance(res, dict) and "status" in res:
        if res["status"] == "success":
            # schema 防御——read_result_file 只保证「能解析成 JSON dict」，
            # 不保证键完整。缺 raw_result 的损坏/外来文件若直接 res["raw_result"]
            # 会抛 KeyError 崩掉整个 run（违背「宁可重跑，不可崩」契约）。
            if "raw_result" not in res:
                logger.error(f"Corrupt result file for {job.uid}: missing 'raw_result' key")
                result_meta = {"error": "CORRUPT_RESULT_FILE: missing raw_result"}
            else:
                # 解码/归一化段的任何意外异常（哨兵碰撞、
                # 损坏文件的边角形状）不得穿透 drain 崩掉整个 run——
                # 兑现本模块「宁可重跑，不可崩」契约，统一转失败结果。
                try:
                    raw_result = _decode_raw_result(res["raw_result"])
                    success, result_meta = _normalize_handler_result(raw_result)
                except Exception as e: # noqa: BLE001——防御矩阵最后一级
                    success = False
                    result_meta = {
                        "error": f"CORRUPT_RESULT_FILE: decode failed: {e}",
                        _KEY_TRACEBACK: traceback.format_exc(),
                    }
                    logger.error(f"Result decode failed for {job.uid}: {e}")
            # 单个坏子任务 dict（如 handler 构造 Job 后篡改 resources）
            # 不应使整个 run 崩溃——逐个防御，坏条目记为失败而非上抛。
            new_jobs = []
            for jd in res.get("new_jobs") or []:
                try:
                    new_jobs.append(Job.from_dict(jd))
                except (KeyError, TypeError, ValueError) as e:
                    success = False
                    result_meta = {
                        "error": f"invalid spawned job dict: {e}",
                        _KEY_TRACEBACK: traceback.format_exc(),
                    }
                    logger.error(f"Malformed spawned job in {job.uid}: {e}")
                    break
            if success:
                # cursor_updates/resource_suspensions 类型校验——损坏文件
                # 中它们可能是 None/list 等错误形状，下游 _apply_result 迭代时
                # AttributeError/TypeError 崩 run。
                _cu = res.get("cursor_updates", {})
                _rs = res.get("resource_suspensions", [])
                if isinstance(_cu, dict):
                    # 仅校验顶层 dict 不够——值类型未校验时，损坏/敌意
                    # 文件可注入非 str 值（如 int），state.cursors 与磁盘
                    # 值类型不一致（内存 int、磁盘 str），且 discovery 的
                    # _decode_seen_set 遇非 str 走 json.loads(int) → TypeError
                    # → 退化空集合 → 整段重扫。逐值校验：非 str/None 记为失败。
                    valid_cu = {}
                    cu_ok = True
                    for k, v in _cu.items():
                        if isinstance(k, str) and (v is None or isinstance(v, str)):
                            valid_cu[k] = v
                        else:
                            success = False
                            result_meta = {
                                "error": f"invalid cursor_updates value: {k!r}={v!r}",
                            }
                            cu_ok = False
                            break
                    if cu_ok:
                        cursor_updates = valid_cu
                else:
                    success = False
                    result_meta = {"error": f"invalid cursor_updates type: {type(_cu).__name__}"}
                if isinstance(_rs, list):
                    # 只校验顶层 list 不够——元素形状未校验时，
                    # pipeline._apply_result 的 ``for r_name, secs in ...``
                    # 解包崩溃（元素为 int/str/单元素 list 时抛
                    # ValueError/TypeError，崩掉整个 run）。逐元素防御：
                    # 非法形状 → 记为失败而非上抛（宁可 DLQ，不可崩 run）。
                    valid_rs = []
                    for item in _rs:
                        if (isinstance(item, (list, tuple)) and len(item) == 2
                                and isinstance(item[0], str)
                                and isinstance(item[1], (int, float))
                                and not isinstance(item[1], bool)):
                            valid_rs.append((item[0], float(item[1])))
                        else:
                            success = False
                            result_meta = {
                                "error": f"invalid resource_suspension entry: {item!r}",
                            }
                            break
                    if valid_rs:
                        resource_suspensions = valid_rs
                else:
                    success = False
                    result_meta = {"error": f"invalid resource_suspensions type: {type(_rs).__name__}"}
        elif res["status"] == "retry":
            retry_requested = True
            retry_error = res.get("error")  # 提取 RetryError message
            # 框架内部锁冲突（LOCK_CONFLICT——孤儿持锁导致
            # 本 worker 拿锁超时）是**结构化信号**而非业务错误：
            # worker 写端写 `lock_conflict` 字段、判定端只读该字段，
            # 业务 RetryError 消息恰好以 "LOCK_CONFLICT"
            # 开头时不再被误判为零计数重试（会走正常计数退避 + 覆盖
            # _last_retry_error）。
            lock_conflict = bool(res.get("lock_conflict", False))
        elif res["status"] == "interrupted":
            # worker 被 Ctrl+C/SIGINT/SIGTERM 中断——不是
            # 业务失败。retry_requested + 结构化 interrupted 标记：消费端
            # （completion）零计数短退避回队（不烧 max_retries 预算、不进
            # DLQ、不级联），兑现 README「仅进行中 job requeue」承诺。
            retry_requested = True
            retry_error = res.get("error")
            interrupted = True
        elif res["status"] == "fatal":
            success = False
            result_meta = {
                "error": res.get("error", "FATAL (no message)"),
                _KEY_TRACEBACK: res.get(_KEY_TRACEBACK),
                "fatal": True
            }
            logger.error(f"Fatal error in {job.uid}:\n{res.get(_KEY_TRACEBACK, '')}")
        else:
            success = False
            # 未知 status 时 res["error"] 可能缺失，用 .get 防御。
            result_meta = {
                "error": res.get("error", f"unknown status {res['status']!r}"),
                _KEY_TRACEBACK: res.get(_KEY_TRACEBACK),
            }
            logger.error(f"Worker Crashed for {job.uid}:\n{res.get(_KEY_TRACEBACK, '')}")
    elif p.exitcode is not None and p.exitcode != 0:
        success = False
        result_meta = {"error": f"PROCESS_CRASH_EXITCODE_{p.exitcode}"}
    else:
        result_meta = {"error": "NO_IPC_RESULT"}

    if success and ipc_dir is not None:
        # 输出存在性校验——读落盘 outputs.jsonl（handler 声明的输出），
        # 无 mp.Manager 单点与 RPC 开销。
        # kind=="cache" 的临时文件**跳过存在性校验**
        # （原子产出的 .part 已被 os.replace 到最终路径，校验必然失败）。
        for out_path, _, kind in read_outputs(ipc_dir, job.uid):
            if kind == "cache":
                continue
            if not Path(out_path).exists():
                logger.error(f"Verification failed for {job.uid}: Missing output -> {out_path}")
                success = False
                result_meta = {"error": f"Missing output {out_path}"}
                break

    return ExecutionResult(
        success=success,
        result_meta=result_meta,
        retry_requested=retry_requested,
        retry_error=retry_error,
        new_jobs=new_jobs,
        cursor_updates=cursor_updates,
        resource_suspensions=resource_suspensions,
        lock_conflict=lock_conflict,
        interrupted=interrupted,
    )


@dataclass
class ExecutionResult:
    """Outcome of a single multiprocessing job execution."""
    success: bool = False
    result_meta: Dict[str, Any] = field(default_factory=dict)
    retry_requested: bool = False
    retry_error: Optional[str] = None  # RetryError message from last retry
    new_jobs: List[Job] = field(default_factory=list)
    cursor_updates: Dict[str, str] = field(default_factory=dict)
    resource_suspensions: List[Tuple[str, float]] = field(default_factory=list)
    # 框架内部锁冲突结构化标记（worker 拿锁超时）——worker
    # 写端写 `lock_conflict` 字段、判定端（_decode_ipc_result）读该字段、
    # _apply_result 消费该字段，全链路不依赖 retry_error 字符串前缀（业务
    # RetryError 消息撞 "LOCK_CONFLICT" 前缀不再被误判为零计数重试）。
    lock_conflict: bool = False
    # worker 被中断（Ctrl+C/SIGINT/SIGTERM 前台进程组信号）
    # 的结构化标记——消费端零计数短退避回队，不烧 max_retries 预算、不进
    # DLQ、不级联下游（README「仅进行中 job requeue」承诺）。
    interrupted: bool = False
    # 本次失败是否将走重试（而非进 DLQ）。由 _apply_result 在决定去向
    # 后填充——钩子据此区分「可恢复的瞬态失败」与「终局失败（DLQ）」。
    # 默认 None 表示调用方未显式设置（_apply_result 总是显式赋值）。
    going_to_retry: Optional[bool] = None


@dataclass
class JobHandle:
    """一个 in-flight 子进程的句柄。

    由 ``submit()`` 创建，传给 ``drain()`` 轮询，完成后从 in-flight 集合移除。
    ``deadline`` 基于 ``time.monotonic()``，用于超时判定。
    ``result_file`` / ``signals_file`` 是落盘 IPC 路径——
    结果文件存在 = 子进程已写入完成（原子 rename），信号文件追加 suspend。
    ``incarnation``：本次执行代的唯一标识 ``{run_id}.{seq}``，
    drain 只轮询 ``{uid}.{incarnation}.result.json``——崩溃后遗留的孤儿
    进程用旧 incarnation 写文件，本 handle 永远看不见它。
    """
    uid: str
    process: Any           # mp.Process
    deadline: float        # time.monotonic() + timeout
    timeout: float         # 原始 timeout 值，用于构造错误信息
    job: Job
    ipc_dir: str           # 结果/信号文件所在目录
    incarnation: Optional[str] = None  # fencing 执行代标识


class MultiprocessingExecutor:
    """Runs handlers in isolated subprocesses (non-blocking, concurrent).

    结果经文件落盘（``{uid}.{incarnation}.result.json`` 原子
    rename），``drain()`` 只 ``os.path.exists`` 轮询——无 mp.Queue 的伪阻塞点。
    """

    def __init__(self, mp_ctx: Any = None, ipc_dir: Optional[str] = None):
        self._mp_ctx = mp_ctx or mp
        self.ipc_dir = ipc_dir

    # 单个 handle 的资源清理 ------------------------------------------

    # join 收割超时（秒）：D-state（不可中断睡眠，如 NFS/网盘挂起 IO）
    # 进程无法被 SIGKILL 终止，join 会无限阻塞主循环。
    # 超时后放弃收割：进程留作僵尸由 OS 收养，主循环不被拖死。
    _JOIN_REAP_TIMEOUT: float = 5.0

    @staticmethod
    def _finalize_process(p) -> None:
        """清理单个子进程：kill + join + close，释放资源。

        kill 与 join 分属独立 try/except——若进程在
        ``is_alive()`` 与 ``kill()`` 之间自然退出，kill 抛
        ``ProcessLookupError`` 时不得跳过整个清理块（否则留下僵尸
        进程）；无论 kill 是否成功都必须 ``join()`` 收割。

        收割超时保护：join 用 ``_JOIN_REAP_TIMEOUT`` 加界——D-state 进程
        （不可中断睡眠，NFS/网盘输出目录挂起 IO 的常见状态）SIGKILL
        无效，``p.join()`` 会无限阻塞主循环（看门狗承诺击穿）。超时后
        放弃收割（进程由 init 收养），不阻塞。
        """
        try:
            if p.is_alive():
                p.kill()
        except Exception:
            pass
        try:
            p.join(timeout=MultiprocessingExecutor._JOIN_REAP_TIMEOUT)
        except Exception:
            pass
        try:
            _p_close = getattr(p, 'close', None)
            if _p_close is not None:
                _p_close()
        except Exception:
            pass

    # 异步派发 --------------------------------------------------------

    def submit(self, handler_func: Callable, job: "Job", ctx: "TaskContext",
               timeout: float, ipc_dir: Optional[str] = None) -> JobHandle:
        """启动子进程执行 handler，立即返回 JobHandle（不阻塞）。

        ``_mp_worker_wrapper`` 在子进程中把结果原子写盘（文件名带
        ctx.incarnation），``drain()`` 负责轮询回收。
        """
        ipc_dir = ipc_dir or self.ipc_dir or os.environ.get(_RESULT_DIR_ENV)
        if not ipc_dir:
            raise ValueError("ipc_dir is required for executor.submit()")
        Path(ipc_dir).mkdir(parents=True, exist_ok=True)
        ctx.ipc_dir = ipc_dir  # 供 suspend_resource 追加信号文件
        incarnation = getattr(ctx, "incarnation", None)
        p = None
        try:
            p = self._mp_ctx.Process(
                target=_mp_worker_wrapper, args=(handler_func, job, ctx, ipc_dir)
            )
            p.start()
        except Exception:
            # start 抛异常时清理已创建的资源
            if p is not None:
                self._finalize_process(p)
            raise
        deadline = time.monotonic() + timeout
        return JobHandle(
            uid=job.uid,
            process=p,
            deadline=deadline,
            timeout=timeout,
            job=job,
            ipc_dir=ipc_dir,
            incarnation=incarnation,
        )

    # 非阻塞收集 ------------------------------------------------------

    def reap_completed(
        self, handles: List[JobHandle]
    ) -> List[Tuple[JobHandle, ExecutionResult]]:
        """非阻塞扫描所有 in-flight handle，返回本次已完成的 (handle, result)。

        对每个 handle（只轮询结果文件，无管道阻塞）：
        1. 结果文件存在 → 读取解析 → 清理 → 完成。
        2. 文件不存在 → 检查进程：
           - 进程已死 → 构造 CRASH/NO_IPC 结果（超时 kill 前曾尝试读文件）。
           - 进程存活且超时 → kill+join，构造 TIMEOUT。
           - 进程存活且未超时 → 跳过（仍在运行）。

        未完成的 handle 不在返回列表中，调用方继续持有。
        """
        completed: List[Tuple[JobHandle, ExecutionResult]] = []

        for handle in handles:
            now = time.monotonic()
            p = handle.process
            # 只轮询**当前 incarnation** 的结果文件路径——
            # 崩溃后遗留的孤儿进程写的是旧 incarnation 文件名，本 handle
            # 永不看见（防止「孤儿结果被误认为新子进程结果而 kill 新子进程」）。
            res_path = result_path(handle.ipc_dir, handle.uid, handle.incarnation)

            # 1. 尝试读完整结果文件（存在即完成——原子 rename 保证完整）
            res = read_result_file(res_path) if res_path.exists() else None

            if res is not None:
                completed.append((handle, self._collect_outcome(handle, res)))
                continue

            # 2. 进程已死（无结果文件）——is_alive 内部经 waitpid(WNOHANG)
            # 已收割已退出进程并设置 returncode，无需先 join：join 对活进程
            # 会真实阻塞满 timeout（100 个 in-flight 时每轮 drain 纯耗
            # 100ms），对已退出进程则收割已由 is_alive 完成、纯冗余。
            if not p.is_alive():
                # exists 检查与进程退出之间存在 TOCTOU——结果文件可能在
                # is_alive 判定前刚被原子 rename（handler 成功但进程尚未
                # 退出）。与超时路径（kill 后重读）对称，死亡分支同样重读
                # 一次，避免成功 job 被误判 NO_IPC_RESULT 而永久 DLQ +
                # 输出被删。
                res = read_result_file(res_path) if res_path.exists() else None
                completed.append((handle, self._collect_outcome(handle, res)))
                continue

            # 3. 进程存活，检查是否超时
            if now > handle.deadline:
                # 先给短暂优雅退出窗口（0.5s），随后 kill——主循环不被
                # 阻塞式 join 拖慢。
                p.join(timeout=0.5)
                if p.is_alive():
                    # is_alive 与 kill 之间进程可能自然退出，裸 kill 抛
                    # ProcessLookupError 会崩主循环——与 _finalize_process 的
                    # 防护同款，此处 try/except 兜底。
                    try:
                        p.kill()
                    except Exception:
                        pass
                    # kill 后**不再**在此 join——收割统一
                    # 由 _collect_outcome 的 finally（_finalize_process）执行：
                    # D-state 进程 SIGKILL 无效，重复 join 最坏让主循环阻塞
                    # 2×_JOIN_REAP_TIMEOUT（10s），违背「挂起 job 不阻塞其他
                    # job」承诺。kill 后立即重读结果文件是安全的（原子 rename
                    # 保证 final 文件要么全有要么全无，无部分写入）。
                    is_timeout = True
                else:
                    # 进程在 0.5s 优雅窗口内**自行**退出（非被 kill）——
                    # 先查 exitcode：非 0 表示崩溃（PROCESS_CRASH 分类），
                    # timeout_is_transient 只作用于真超时（被 kill 的挂起
                    # 进程），自崩进程是确定性错误，重试无益。
                    is_timeout = (p.exitcode == 0 or p.exitcode is None)
                # kill 后结果文件可能刚写入（handler 在超时前完成）→ 再读一次
                res = read_result_file(res_path) if res_path.exists() else None
                completed.append(
                    (handle, self._collect_outcome(handle, res, is_timeout=is_timeout))
                )
            # else: 进程存活且未超时 → 跳过

        return completed

    def _collect_outcome(
        self, handle: JobHandle, res: Optional[dict], *, is_timeout: bool = False
    ) -> ExecutionResult:
        """收敛 drain 三段同构尾部：解析结果 → 收割进程 → 清理 IPC 文件。

        正常完成/死亡重读/超时 kill 后重读三段的差异只有 res 从哪来与
        失败结果怎么构造（is_timeout 标记）——统一在此完成。重读的
        时序契约（TOCTOU/kill 后重读）由调用方 drain 在传参前保证，
        本方法只负责「拿到 res 后的收敛尾部」。

        资源防泄漏：解析/构造失败结果放 try，收割与清理放
        finally——_decode_ipc_result/_build_terminal_failure 抛未预期
        异常时（如损坏结果文件触发未防御的异常类型），进程与 IPC 文件
        仍被清理，异常穿透 drain 后也不会泄漏子进程（宁可重跑，不可崩）。

        信号防丢失：清理前消费信号文件——handler 写入 suspend 信号后
        job 超时/立即崩溃时，该 entry 即刻离开 in-flight，主循环的
        apply_pending_signals 永远轮不到它，而下方 cleanup_ipc_files 会删除
        信号文件——限流信息静默丢失。清理前读取并把挂起折叠进
        ``ExecutionResult.resource_suspensions``，由 completion.apply_result
        统一应用并即时持久化（幂等 max 语义，重复应用无害）。
        """
        p = handle.process
        result: Optional[ExecutionResult] = None
        try:
            if res is not None:
                result = _decode_ipc_result(res, p, handle.job, handle.ipc_dir)
            else:
                result = self._build_terminal_failure(p, handle, is_timeout=is_timeout)
        finally:
            self._finalize_process(p)
            # 清理前最后一读（try 块抛异常时 result 为 None，仅告警不阻断清理）
            try:
                pending_signals = read_signals(handle.ipc_dir, handle.uid)
                if pending_signals:
                    if result is not None:
                        result.resource_suspensions = (
                            list(result.resource_suspensions) + list(pending_signals))
                        logger.info(
                            f"Salvaged {len(pending_signals)} suspend signal(s) "
                            f"from {handle.uid} before IPC cleanup"
                        )
                    else:
                        logger.warning(
                            f"Suspend signals on {handle.uid} discarded: "
                            f"outcome construction failed"
                        )
            except Exception as e:
                logger.warning(f"Failed to salvage signals for {handle.uid}: {e}")
            cleanup_ipc_files(handle.ipc_dir, handle.uid, handle.incarnation)
        return result

    def consume_stale_result(self, uid: str, job: Job) -> Optional[ExecutionResult]:
        """崩溃恢复：检查 ipc_dir 中该 uid 的残留结果文件并消费。

        上次 run 主进程 SIGKILL/OOM/断电 崩溃时，子进程可能已写好结果文件但
        主进程未及 commit。本方法在**派发新子进程之前**由 ``_dispatch_job``
        调用，把这类残留结果直接解析出来（消费），从而：
        - 避免「新子进程已启动、却被旧结果文件误判完成而 kill」的双重执行窗口；
        - 不浪费已完成的执行（at-least-once 语义下可省一次无谓重跑）。

        与 ``drain()`` 的区别：drain 按 JobHandle 轮询 in-flight 进程的结果
        文件（当前 incarnation 路径）；本方法**不依赖任何进程句柄**，按 uid
        枚举任意 incarnation 的残留文件（旧 run 的子进程
        可能在崩溃后以孤儿身份继续运行并写入其旧 incarnation 路径——若文件
        已存在则消费它，若孤儿仍在运行（文件未写）则返回 None 照常派发）。

        Returns:
            ExecutionResult: 解析后的残留结果；若无残留文件（或文件损坏/
            非结果格式）返回 ``None``，调用方照常派发新子进程。
        """
        res_paths = _iter_stale_result_paths(self.ipc_dir, uid)
        # 只消费 final 结果文件（.result.json）——.tmp 变体
        # 表示孤儿 worker **仍在写**（dump/fsync 未完成）。消费 .tmp 会读到
        # 部分 JSON（read_result_file 返回 None）后 unlink 正在写的文件 →
        # 孤儿 os.replace(tmp, final) 抛 FileNotFoundError → 写 error 结果 →
        # 下次派发消费 error → **成功执行的 job 被虚假 DLQ**。.tmp 的存在
        # 意味着孤儿活着持锁，交给 probe_lock 的 defer 路径处理（孤儿死后
        # 自愈，rename 完成后再消费 final）。
        res_paths = [p for p in res_paths if p.name.endswith(_RESULT_SUFFIX)]
        if not res_paths:
            return None
        # 消费即删：防止下次 run 重复提交同一残留结果（任意变体都清）。
        # 多个变体并存时（多次崩溃遗留）取最新的——孤儿是最新执行体。
        # 决胜键 (st_mtime_ns, incarnation_seq)：ns 精度 + 单调递增序号
        # 保证同刻冲突时取最新执行代（秒级 mtime 在同秒内多个变体时
        # 排序不稳定，可能消费**较旧执行代**的结果）。
        def _freshness_key(p):
            st = p.stat()
            try:
                seq = int(p.name.removesuffix(_RESULT_SUFFIX).rsplit(".", 1)[-1])
            except (ValueError, IndexError):
                seq = -1
            return (st.st_mtime_ns, seq)

        res_path = max(res_paths, key=_freshness_key)
        res = read_result_file(res_path)
        for p in res_paths:
            try:
                p.unlink()
            except OSError:
                pass
        # 损坏/非标准结果（无 status 键）视为无残留：宁可重跑，不可误判。
        if not isinstance(res, dict) or "status" not in res:
            logger.warning(
                f"Discarding stale result file for {uid}: not a valid result dict: {res!r}"
            )
            return None
        return _decode_ipc_result(res, None, job, self.ipc_dir)

    @staticmethod
    def _build_terminal_failure(
        p, handle: JobHandle, *, is_timeout: bool
    ) -> ExecutionResult:
        """构造进程终止（崩溃/超时）但无结果文件时的 ExecutionResult。"""
        exitcode = p.exitcode
        retry_requested = False
        retry_error: Optional[str] = None
        if is_timeout:
            if getattr(handle.job, "timeout_is_transient", False):
                # 该 job 声明超时是瞬态（如可能长跑的 discovery 扫描）
                # 「没跑完」不等于「确定性失败」，按 retry 处理：
                # 指数退避重试，达 max_retries 才 DLQ（避免超时即判死）。
                return ExecutionResult(
                    success=False,
                    retry_requested=True,
                    retry_error=f"TIMEOUT ({handle.timeout}s)",
                )
            # 超时：优先用 TIMEOUT 错误（即使 exitcode 非零，也是被 kill 导致）
            result_meta = {"error": f"TIMEOUT ({handle.timeout}s)"}
        elif exitcode is not None and exitcode < 0:
            # 非超时路径的负 exitcode = 进程死于信号
            # （-9=SIGKILL——OS OOM killer 与外部 kill 共用、-11=SIGSEGV
            # 等）。环境性瞬态故障（内存压力高峰过去即可成功）默认走正常
            # 退避重试（消耗 max_retries 预算，达上限才 DLQ），meta 富化
            # 结构化信号名；与既有 timeout_is_transient 开关对称。
            try:
                sig_name = signal.Signals(-exitcode).name
            except (ValueError, AttributeError):
                sig_name = f"SIGNO{-exitcode}"
            result_meta = {
                "error": f"PROCESS_CRASH_EXITCODE_{exitcode}",
                "signal": sig_name,
                "oom_hint": -exitcode == int(signal.SIGKILL),
            }
            retry_requested = True
            retry_error = f"PROCESS_SIGNAL_DEATH: killed by {sig_name} ({exitcode})"
            logger.warning(
                f"Worker for {handle.uid} died by {sig_name}; will retry with backoff"
            )
        elif exitcode is not None and exitcode != 0:
            result_meta = {"error": f"PROCESS_CRASH_EXITCODE_{exitcode}"}
        else:
            result_meta = {"error": "NO_IPC_RESULT"}

        return ExecutionResult(
            success=False,
            result_meta=result_meta,
            retry_requested=retry_requested,
            retry_error=retry_error,
        )

    # 异常退出时的强制清理 --------------------------------------------

    def cleanup(self, handles: List[JobHandle]) -> None:
        """主循环异常退出时 kill + join 所有残留 in-flight 子进程。

        用于 KeyboardInterrupt / RuntimeError 等场景，释放资源避免泄漏。
        不构造 ExecutionResult（这些 job 由调用方 requeue 回队列）。
        """
        for handle in handles:
            try:
                self._finalize_process(handle.process)
                cleanup_ipc_files(handle.ipc_dir, handle.uid)
            except Exception as e:
                logger.error(f"Error cleaning up in-flight job {handle.uid}: {e}")

    def finalize_processes(self, handles: List[JobHandle]) -> None:
        """只 kill + join 残留子进程，**不删 IPC 文件**。

        ``cleanup`` 的「kill + cleanup_ipc_files 一体」在 abort 消费结果
        路径存在 TOCTOU 窗口：``_abort_in_flight`` 先分类（读结果
        文件判 done/pending），随后 kill——分类读到无结果（归 pending）与
        kill 之间 worker 可能恰好完成 ``write_result_atomic``（结果文件
        此刻才出现）。若 kill 后立即 ``cleanup_ipc_files`` 删结果文件，
        刚写好的成功结果被清掉 → 成功 job 被误判未完成 → requeue →
        重启必重跑，非幂等副作用被重复执行。

        调用方时序契约（先 kill 再清理）：本方法先只做进程收割
        （kill + join + close，``_finalize_process`` 内部逐段 try/except
        防护），调用方随后**重查**结果文件（把 kill 后新出现的 done 移出
        pending），再对剩余 pending 执行 ``cleanup_ipc_files`` ——此时
        kill/join 已停止写入，不再有新结果出现，无竞态窗口。

        与 ``cleanup`` 的分工：``cleanup``（kill+清理一体）用于不消费
        结果的路径（如 ``_dispatch_job`` 的失败分支）；本方法用于 abort
        消费路径（先 kill、重查结果、再清理）。
        """
        for handle in handles:
            try:
                self._finalize_process(handle.process)
            except Exception as e:
                logger.error(f"Error finalizing in-flight job {handle.uid}: {e}")
