"""可逆单射转义与安全标识派生工具（数学不变式单点）。

设计原则：
1. 单射性（Injective / Bijection）：对于任意 a != b，encode(a) != encode(b)。
   任何非单射净化（如丢弃字符、简单多对一替换）在拼装 UID 或文件路径时会导致碰撞，
   进而导致 wall 去重静默吞任务或锁竞争错乱。
2. 转义符优先转义：% 必须首先转义为 %25，因此输出中的 % 唯一且确定地引导一个 %XX 序列。
3. 超长截断单射兜底：截断时附加 SHA-256 摘要（8 字符），保证同前缀不同尾部的长字符串不碰撞。
"""

import hashlib
from typing import Any, Dict, Sequence, Union

_PERCENT_ESCAPE = "%"
_PERCENT_ESCAPED = "%25"

# 默认禁止字符集（路径分隔符、冒号、转义符本身）
DEFAULT_FORBIDDEN = "/\\:%"

# 内容 ID 允许字符集（仅字母、数字、短横线、下划线、点）
CONTENT_ID_ALLOWED = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."

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

    转义符 % 总是优先按 %25 处理，保证输出中的 %XX 序列无二义性。
    """
    parts = []
    text_str = str(text)
    if allowed is not None:
        allowed_set = set(allowed)
        for ch in text_str:
            if ch in allowed_set:
                parts.append(ch)
            else:
                parts.append("".join(f"%{b:02X}" for b in ch.encode("utf-8")))
    else:
        forbidden_set = set(forbidden or DEFAULT_FORBIDDEN)
        for ch in text_str:
            if ch.isprintable() and ch not in forbidden_set:
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
    fallback: str = "untitled",
) -> str:
    """净化任意值为确定性、单射安全的标识符（如 job_id 或 content_id）。

    - 空值返回 fallback（默认 "untitled"）；
    - 禁止字符与不可打印字符可逆单射转义；
    - 超长输入（转义后 > max_len）：截断 + 8 位 SHA-256 指纹后缀。
    """
    text = str(value) if value is not None else ""
    if not text:
        return fallback
    escaped = escape_injective(text, forbidden=forbidden, allowed=allowed)
    if len(escaped) <= max_len:
        return escaped
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]
    base_limit = max(max_len - 9, 0)
    prefix = escaped[:base_limit]
    if prefix.endswith("%"):
        prefix = prefix[:-1]
    elif len(prefix) >= 2 and prefix[-2] == "%":
        prefix = prefix[:-2]
    return f"{prefix}_{digest}"


def sanitize_content_id(content_id: str, *, max_len: int = 120) -> str:
    """按严密 allowlist 净化 content_id（如用于 discovery 子任务派发）。"""
    text = str(content_id)
    if not text:
        return "untitled"
    # 若全部是 clean 字符且未超长，直接返回
    if all(c in CONTENT_ID_ALLOWED for c in text) and len(text) <= max_len:
        return text
    return sanitize_identifier(text, max_len=max_len, allowed=CONTENT_ID_ALLOWED)



def safe_uid_filename(uid: str) -> str:
    """把 job uid 映射为对任意文件系统安全（无 : 等保留字符）的文件名。

    与 executor 的 result/signals/outputs/lock 路径函数共享。
    """
    escaped = uid.replace(_PERCENT_ESCAPE, _PERCENT_ESCAPED)
    for ch, enc in FS_ESCAPE_CHARS.items():
        escaped = escaped.replace(ch, enc)
    return escaped
