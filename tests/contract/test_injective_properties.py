"""单射转义性质测试（hypothesis）。

被测契约范围（严格按各函数 docstring 声称）：
- ``escape_injective`` / ``safe_uid_filename``：全域单射（任意 x != y ⟹ f(x) != f(y)）；
- ``sanitize_identifier`` / ``sanitize_content_id``：仅对「转义后不超 max_len 的非空输入」
  声称单射；超长截断路径声称「同前缀不同尾部长串不碰撞」与「输出 ≤ max_len（max_len ≥ 9）」。
  超长截断路径的全域单射不在声称范围内，不做全域断言。
"""

from __future__ import annotations

import hashlib
import re

import pytest
from hypothesis import example, given, settings, strategies as st

from tasklite.utils.injective import (
    CONTENT_ID_ALLOWED,
    EMPTY_SENTINEL,
    escape_injective,
    safe_uid_filename,
    sanitize_content_id,
    sanitize_identifier,
)
from tasklite.utils.lockfile import _lock_path

# 折磨字母表：转义符、路径分隔符、glob 元字符、控制字符、多字节与高低码位字符
_TORTURE_ALPHABET = "%:/\\*?[]_\t\n\r\x00\x1b\x7f é中\U0001F600\uFFFD\u2028"

any_text = st.one_of(
    st.text(max_size=60),
    st.text(alphabet=_TORTURE_ALPHABET, max_size=40),
)
distinct_pair = st.tuples(any_text, any_text).filter(lambda p: p[0] != p[1])

# 违规形态：% 后未紧跟两位大写十六进制（含 % 收尾、% 后小写 hex）
_BARE_PERCENT = re.compile(r"%(?![0-9A-F]{2})")


def _percent_only_as_escapes(out: str) -> bool:
    """输出中每个 % 后必须紧跟两位大写十六进制（%25 先行转义的无二义形态）。"""
    return _BARE_PERCENT.search(out) is None


# ── escape_injective：全域单射与输出形态 ───────────────────────────────


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(("%", "%25"))
@example(("t::x::y", "t::x%3A%3Ay"))
@example(("a%2Fb", "a/b"))
@given(pair=distinct_pair)
def test_escape_injective_default_forbidden_is_injective(pair):
    """任意 x != y：默认禁止集下 escape(x) != escape(y)（全域单射）。"""
    x, y = pair
    assert escape_injective(x) != escape_injective(y)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(("中", "中中"))
@given(pair=distinct_pair)
def test_escape_injective_allowlist_mode_is_injective(pair):
    """任意 x != y：allowlist 模式（content_id 字符集）下同样单射。"""
    x, y = pair
    assert escape_injective(x, allowed=CONTENT_ID_ALLOWED) != escape_injective(
        y, allowed=CONTENT_ID_ALLOWED
    )


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example("")
@example("%/:\\\x00")
@given(text=any_text)
def test_escape_output_has_no_forbidden_or_bare_percent(text):
    """输出不含默认禁止字符（/ \\ :），且每个 % 都以 %XX 形态出现。"""
    out = escape_injective(text)
    assert "/" not in out
    assert "\\" not in out
    assert ":" not in out
    assert _percent_only_as_escapes(out)
    assert all(ch.isprintable() for ch in out)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example("")
@example("a\tb%2Fc")
@given(text=any_text)
def test_escape_roundtrip_via_reference_decoder(text):
    """参考解码器（%XX → 字节、其余字符原样）逆映射恒等于原文（可逆性）。"""
    out = escape_injective(text)
    chars = []
    buf = bytearray()
    i = 0
    while i < len(out):
        ch = out[i]
        if ch == "%":
            buf.extend(bytes([int(out[i + 1 : i + 3], 16)]))
            i += 3
        else:
            if buf:
                chars.append(buf.decode("utf-8"))
                buf.clear()
            chars.append(ch)
            i += 1
    if buf:
        chars.append(buf.decode("utf-8"))
    assert "".join(chars) == text


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example("ab")
@given(base=st.text(min_size=1, max_size=30, alphabet="abcXYZ0189-_"))
def test_percent25_first_escape_prevents_aliasing(base):
    """朴素按序替换会混淆的输入对（% 片段 vs 被转义字符）在单射转义下必不混淆。"""
    assert escape_injective(base + "%25") != escape_injective(base + "%")
    assert escape_injective("%25" + base) != escape_injective("%" + base)
    assert escape_injective(base + "%2F") != escape_injective(base + "/")
    assert escape_injective(base + "%3A") != escape_injective(base + ":")


# ── sanitize_identifier：声称范围内的单射、截断兜底与长度上限 ──────────


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(pair=("x", "y"), max_len=120)
@example(pair=("untitled", "untitled\x00"), max_len=120)
@given(pair=distinct_pair, max_len=st.integers(min_value=1, max_value=200))
def test_sanitize_short_inputs_injective_within_claimed_scope(pair, max_len):
    """转义后均 ≤ max_len 的非空输入对：sanitize 输出必不同（声称的单射域）。"""
    x, y = pair
    ex = escape_injective(x)
    ey = escape_injective(y)
    if not x or not y or len(ex) > max_len or len(ey) > max_len:
        return
    assert sanitize_identifier(x, max_len=max_len) != sanitize_identifier(y, max_len=max_len)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(text="x" * 500, max_len=120)
@example(text="aaaa////", max_len=20)
@given(text=any_text, max_len=st.integers(min_value=9, max_value=200))
def test_sanitize_output_respects_length_cap_and_aligned_prefix(text, max_len):
    """max_len ≥ 9 时输出恒 ≤ max_len；截断路径带 8 位指纹后缀且无悬挂 % 碎片。"""
    out = sanitize_identifier(text, max_len=max_len)
    assert len(out) <= max_len
    escaped = escape_injective(text)
    if text and len(escaped) > max_len:
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]
        assert out.endswith("_" + digest)
        prefix = out[: -(len(digest) + 1)]
        assert not prefix.endswith("%")
        assert not (len(prefix) >= 2 and prefix[-2] == "%")
    elif text:
        assert out == escaped


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(prefix="y" * 200, tail_a="z", tail_b="w", max_len=60)
@given(
    prefix=st.text(min_size=1, max_size=60, alphabet="xy%:/\\_0189"),
    tail_a=st.text(min_size=1, max_size=6, alphabet="abcdefghpq"),
    tail_b=st.text(min_size=1, max_size=6, alphabet="abcdefghpq"),
    max_len=st.integers(min_value=20, max_value=120),
)
def test_truncated_same_prefix_different_tail_never_collides(prefix, tail_a, tail_b, max_len):
    """模块 docstring 声称的兜底性质：同前缀不同尾部的长串截断后不碰撞。"""
    if tail_a == tail_b:
        return
    text_a = prefix + tail_a
    text_b = prefix + tail_b
    if len(escape_injective(text_a)) <= max_len:
        return
    out_a = sanitize_identifier(text_a, max_len=max_len)
    out_b = sanitize_identifier(text_b, max_len=max_len)
    assert out_a != out_b


# ── sanitize_content_id：allowlist 直通与输出字符域 ────────────────────


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(text="normal-1.2_abc", max_len=120)
@example(text="a::b", max_len=120)
@example(text="中", max_len=120)
@given(text=any_text, max_len=st.integers(min_value=9, max_value=200))
def test_content_id_output_charset_is_allowlist_or_escapes(text, max_len):
    """干净输入恒等直通；其余输出字符全部落在 allowlist ∪ {%}，且 % 后跟两位大写 hex。"""
    out = sanitize_content_id(text, max_len=max_len)
    if text and all(c in CONTENT_ID_ALLOWED for c in text) and len(text) <= max_len:
        assert out == text
    elif not text:
        # 空值哨兵为像集外形态（含孤立 %），不适用「% 只以 %XX 出现」约束
        assert out == EMPTY_SENTINEL
    else:
        assert out
        assert all(c in CONTENT_ID_ALLOWED or c == "%" for c in out)
        assert _percent_only_as_escapes(out)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(pair=("a", "b"), max_len=120)
@given(pair=distinct_pair, max_len=st.integers(min_value=9, max_value=200))
def test_content_id_short_inputs_injective_within_claimed_scope(pair, max_len):
    """转义后均 ≤ max_len 的非空输入对：content_id 净化输出必不同。"""
    x, y = pair
    ex = escape_injective(x, allowed=CONTENT_ID_ALLOWED)
    ey = escape_injective(y, allowed=CONTENT_ID_ALLOWED)
    if not x or not y or len(ex) > max_len or len(ey) > max_len:
        return
    assert sanitize_content_id(x, max_len=max_len) != sanitize_content_id(y, max_len=max_len)


# ── 空值哨兵：像集外形态与零碰撞 ──────────────────────────────────────


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example("")
@example("untitled")
@example("%untitled")
@example("%25untitled")
@example("untitled\x00")
@given(text=any_text)
def test_empty_sentinel_outside_image_never_collides(text):
    """空值哨兵处于单射转义像集之外：任何非空输入（含字面 untitled）都映射不出哨兵。"""
    sentinel_id = sanitize_identifier("")
    sentinel_cid = sanitize_content_id("")
    assert sanitize_identifier(None) == sentinel_id
    assert sanitize_content_id("") == sentinel_cid
    # 哨兵含孤立 %（像集外形态的充要特征）
    assert not _percent_only_as_escapes(sentinel_id)
    assert not _percent_only_as_escapes(sentinel_cid)
    if not text:
        return
    assert sanitize_identifier(text) != sentinel_id
    assert sanitize_content_id(text) != sentinel_cid


# ── safe_uid_filename 与 _lock_path 组合单射 ──────────────────────────


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(("t::1", "t::2"))
@example(("t::x::y", "t::x%3A%3Ay"))
@given(pair=distinct_pair)
def test_safe_uid_filename_is_injective(pair):
    """任意 uid1 != uid2：安全文件名映射必不同（含 '::' 复合 uid 场景）。"""
    uid_a, uid_b = pair
    assert safe_uid_filename(uid_a) != safe_uid_filename(uid_b)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(pair=("t::1", "t::2"), ipc_dir="/tmp/ipc")
@given(pair=distinct_pair, ipc_dir=st.sampled_from(["/tmp/ipc", "ipc"]))
def test_lock_path_derives_injectively_from_uid(pair, ipc_dir):
    """uid1 != uid2 ⟹ _lock_path 不同：锁文件名继承 safe_uid_filename 的单射性。"""
    uid_a, uid_b = pair
    path_a = _lock_path(ipc_dir, uid_a)
    path_b = _lock_path(ipc_dir, uid_b)
    assert path_a != path_b
    assert path_a.name.endswith(".lock")
    assert path_a.parent.name == ipc_dir.split("/")[-1]


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example("t::a/../../../etc/passwd")
@example("t::a\x00b*?[")
@given(uid=any_text)
def test_lock_filename_free_of_filesystem_hazards(uid):
    """锁文件名不含文件系统危险字符与裸 %（POSIX/Windows 保留字符与 glob 元字符）。"""
    name = _lock_path("ipc", uid).name
    for ch in "/\\:*?[]\x00":
        assert ch not in name
    assert _percent_only_as_escapes(name)
