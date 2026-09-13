"""可逆单射转义与安全标识派生工具（数学不变式单点）。

设计原则：
1. 单射性（Injective / Bijection）：对于任意 a != b，encode(a) != encode(b)。
   任何非单射净化（如丢弃字符、简单多对一替换）在拼装 UID 或文件路径时会导致碰撞，
   进而导致 wall 去重静默吞任务或锁竞争错乱。
2. 转义符优先转义：% 必须首先转义为 %25，因此输出中的 % 唯一且确定地引导一个 %XX 序列。
   推论：含孤立 %（% 后不跟两位大写 hex）的串不在像集中——哨兵与截断标记均利用此性质
   选取「任何输入都映射不出」的形态。
3. 超长截断兜底：截断输出 = 前缀 + "%_" + 64 位 SHA-256 指纹。标记 "%_" 使截断输出
   落在转义像集之外，与「转义后 ≤ max_len 直通域」结构性不相交（无定点自碰撞）；
   同前缀长输入之间依赖 64 位指纹概率防撞（生日界约 2^32 条）。有界输出对无限定义域
   的全域单射数学上不可能，此处为工程实用界而非数学保证。
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, Iterable, Sequence, Union

_PERCENT_ESCAPE = "%"
_PERCENT_ESCAPED = "%25"

# 默认禁止字符集（路径分隔符、冒号、转义符本身）
DEFAULT_FORBIDDEN = "/\\:%"

# 内容 ID 允许字符集（仅字母、数字、短横线、下划线、点）
CONTENT_ID_ALLOWED = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."

# 空值哨兵：含孤立 %（% 后不跟两位大写 hex）。不变式：单射转义的像集中 % 只以
# %XX 成对出现，故任何非空输入都不可能映射出该形态——空值与真实输入（含字面
# "untitled"）零碰撞。哨兵为固定最短安全形态，不随 max_len 收缩。
EMPTY_SENTINEL = "%untitled"

# 截断指纹宽度：64 位 hex。同前缀长输入之间为概率防撞，生日界约 2^32 条（50% 碰撞
# 概率所需的同前缀长 ID 量级），工程上视为实用安全界而非数学保证。
_TRUNC_DIGEST_HEX = 16

# 截断标记：含孤立 %（_ 非 hex 位），处于转义像集之外。不变式：直通域输出继承
# escape_injective 的「% 只以 %XX 成对出现」形态，故截断输出与直通域结构性不相交，
# 杜绝「截断输出自身作为输入再次净化」时的定点自碰撞。
_TRUNC_MARKER = "%_"

# 截断形态的固定开销（标记 + 指纹），max_len 低于此值时截断无法在不破上限的前提下完成
_TRUNC_FIXED_LEN = len(_TRUNC_MARKER) + _TRUNC_DIGEST_HEX

# 文件系统危险字符映射（POSIX /、Windows \、NUL、glob *?[]、冒号 :）
FS_ESCAPE_CHARS: Dict[str, str] = {
    "/": "%2F",
    "\\": "%5C",
    "\x00": "%00",
    "*": "%2A",
    "?": "%3F",
    "[": "%5B",
    "]": "%5D",
    ":": "%3A",
}


def escape_injective(
    text: str,
    forbidden: Union[str, Sequence[str], None] = DEFAULT_FORBIDDEN,
    *,
    allowed: Union[str, Sequence[str], None] = None,
) -> str:
    """可逆单射转义：
    - 若指定 allowed：仅 allowed 中的字符原样保留，其余字符转为 UTF-8 %XX；
    - 若指定 forbidden：可打印且非 forbidden 的字符原样保留，其余字符转为 UTF-8 %XX。

    转义符 % 无条件优先按 %25 处理（不依赖 % 是否落在 forbidden/allowed 集内），
    保证任意配置下输出中的 %XX 序列无二义性——否则自定义 forbidden="/" 时
    "/" 与字面 "%2F" 输出同形碰撞。
    """
    parts = []
    text_str = str(text)
    if allowed is not None:
        allowed_set = set(allowed)
        for ch in text_str:
            if ch == _PERCENT_ESCAPE:
                parts.append(_PERCENT_ESCAPED)
            elif ch in allowed_set:
                parts.append(ch)
            else:
                parts.append("".join(f"%{b:02X}" for b in ch.encode("utf-8")))
    else:
        forbidden_set = set(forbidden or DEFAULT_FORBIDDEN)
        for ch in text_str:
            if ch == _PERCENT_ESCAPE:
                parts.append(_PERCENT_ESCAPED)
            elif ch.isprintable() and ch not in forbidden_set:
                parts.append(ch)
            else:
                parts.append("".join(f"%{b:02X}" for b in ch.encode("utf-8")))
    return "".join(parts)


def sanitize_identifier(
    value: Any,
    *,
    max_len: int = 120,
    forbidden: Union[str, Sequence[str], None] = DEFAULT_FORBIDDEN,
    allowed: Union[str, Sequence[str], None] = None,
    fallback: str = EMPTY_SENTINEL,
) -> str:
    """净化任意值为确定性、单射安全的标识符（如 job_id 或 content_id）。单射契约分级：

    - 空值（None/""）返回 fallback；默认哨兵 EMPTY_SENTINEL 处于单射转义像集之外，
      任何非空输入都映射不出该值（零碰撞）。自定义 fallback 必须同样选用像集外
      形态（含孤立 %），否则与字面输入的碰撞由调用方自负；哨兵形态固定，不随
      max_len 收缩；
    - 转义后 ≤ max_len 的输入：直通输出，全域单射（继承 escape_injective）；
    - 转义后 > max_len 的输入：截断 + "%_" + 64 位 SHA-256 全文指纹。输出带像集外
      标记，与直通域结构性不相交；同前缀长输入之间为概率防撞（生日界约 2^32 条
      同前缀长 ID），非数学全域保证；
    - 需截断而 max_len < _TRUNC_FIXED_LEN 时抛 ValueError（截断形态无法压入上限，
      fail-loud）；直通输入对任意 max_len 均可用。
    """
    text = str(value) if value is not None else ""
    if not text:
        return fallback
    escaped = escape_injective(text, forbidden=forbidden, allowed=allowed)
    if len(escaped) <= max_len:
        return escaped
    # 指纹覆盖全文（非截断前缀），同前缀不同尾部的长输入由此区分
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:_TRUNC_DIGEST_HEX]
    base_limit = max_len - _TRUNC_FIXED_LEN
    if base_limit < 0:
        raise ValueError(
            f"max_len={max_len} 不足以容纳截断指纹形态"
            f"（标记与指纹固定占 {_TRUNC_FIXED_LEN} 字符），拒绝静默突破长度上限"
        )
    prefix = escaped[:base_limit]
    if prefix.endswith("%"):
        prefix = prefix[:-1]
    elif len(prefix) >= 2 and prefix[-2] == "%":
        prefix = prefix[:-2]
    return f"{prefix}{_TRUNC_MARKER}{digest}"


def sanitize_job_component(value: Any, *, max_len: int = 120) -> str:
    r"""把任意字符串**转义**净化为可作 job_id 成分的串（不删字符）。

    单射契约分级见 sanitize_identifier：直通域全域单射；截断输出带像集外标记，
    与直通域结构性不相交，截断域内为 64 位指纹概率防撞。
    """
    return sanitize_identifier(value, max_len=max_len)


def sanitize_content_id(content_id: Union[str, None], *, max_len: int = 120) -> str:
    """按严密 allowlist 净化 content_id（如用于 discovery 子任务派发）。

    None/空值返回像集外哨兵（固定形态，不随 max_len 收缩）；单射契约分级
    与截断行为同 sanitize_identifier。
    """
    if content_id is None:
        return EMPTY_SENTINEL
    text = str(content_id)
    if not text:
        return EMPTY_SENTINEL
    # 若全部是 clean 字符且未超长，直接返回
    if all(c in CONTENT_ID_ALLOWED for c in text) and len(text) <= max_len:
        return text
    return sanitize_identifier(text, max_len=max_len, allowed=CONTENT_ID_ALLOWED)


def _fingerprint_token(value: Any) -> str:
    """指纹项 → 带类型标记的确定性 token。

    类型标记隔离跨型碰撞（1 与 "1" 编码必不同）；repr 把内容中的控制字符
    （含 \\x00）转义为字面反斜杠序列，token 中不出现裸 \\x00，项内注入
    定界符伪装项边界的碰撞被结构性排除。dict 递归 token 化后按键 token
    排序——键 token 含类型标记且对任意不同键必不同，排序仅由键决定；
    任意嵌套层级（含 dict 作 value）均序无关。
    """
    if isinstance(value, bool):
        return "b" + repr(value)
    if isinstance(value, int):
        return "i" + repr(value)
    if isinstance(value, float):
        return "f" + repr(value)
    if isinstance(value, str):
        return "s" + repr(value)
    if isinstance(value, dict):
        return "d" + repr(sorted(
            (_fingerprint_token(k), _fingerprint_token(v))
            for k, v in value.items()
        ))
    if isinstance(value, (list, tuple)):
        return "l" + repr([_fingerprint_token(v) for v in value])
    return "o" + repr(value)


def content_fingerprint(
    parts: Iterable[Union[str, int, float]],
    *,
    version: str = "",
) -> str:
    """内容指纹 → 确定性 job_id 片段（sha1，截 16 位，可读）。

    任何输入变化（含 bump version 盐）→ 指纹变化 → wall 不命中 → 重跑。
    与“job_id 确定性、禁止随机后缀”红线一致：给定输入恒同指纹。
    """
    h = hashlib.sha1()
    if version:
        h.update(_fingerprint_token(version).encode("utf-8", errors="replace"))
        h.update(b"\x00")
    for p in parts:
        h.update(_fingerprint_token(p).encode("utf-8", errors="replace"))
        h.update(b"\x00")
    return h.hexdigest()[:16]


def safe_uid_filename(uid: str) -> str:
    """把 job uid 映射为对任意文件系统安全（无 : 等保留字符）的文件名。

    与 executor 的 result/signals/outputs/lock 路径函数共享。
    """
    escaped = uid.replace(_PERCENT_ESCAPE, _PERCENT_ESCAPED)
    for ch, enc in FS_ESCAPE_CHARS.items():
        escaped = escaped.replace(ch, enc)
    return escaped

