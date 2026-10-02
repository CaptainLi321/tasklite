"""v2 单射编码族契约测试（确定性用例 + hypothesis 属性测试）。

被测契约范围（严格按各函数 docstring 声称）：
- ``percent_encode`` / ``safe_uid_filename``：全域单射（任意 x != y ⟹
  f(x) != f(y)）；
- ``encode_identifier`` / ``encode_content_id``：编码后 ≤ max_len 的非空
  输入全域单射；空值哨兵处于像集外（零碰撞）；超长截断输出带像集外标记
  "%_" + 16 位 hex 全文指纹，与直通域结构性不相交，同前缀长输入之间为
  概率防撞；需截断而 max_len < 18 时抛 ValueError。
"""

from __future__ import annotations

import hashlib
import re

import pytest
from hypothesis import example, given, settings, strategies as st

from tasklite.utils.encoding import (
    CONTENT_ID_ALLOWED,
    EMPTY_SENTINEL,
    content_fingerprint,
    encode_content_id,
    encode_identifier,
    encode_job_component,
    percent_encode,
    safe_uid_filename,
)

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


# ── percent_encode：基础行为与全域单射 ────────────────────────────────


def test_percent_encode_basic_and_utf8():
    # 基础字符原样保留
    assert percent_encode("hello-world_123") == "hello-world_123"
    # % 优先转义为 %25
    assert percent_encode("100%_pure") == "100%25_pure"
    # 路径分隔符与冒号
    assert percent_encode("a/b:c\\d") == "a%2Fb%3Ac%5Cd"
    # 不可打印控制字符转义
    assert percent_encode("a\tb") == "a%09b"
    # 当指定 allowed 时，非 ASCII / 特殊字符转义为 %XX
    assert percent_encode("中", allowed="abc") == "%E4%B8%AD"
    assert percent_encode("é", allowed="abc") == "%C3%A9"


def test_percent_encode_bijection_no_collisions():
    inputs = [
        "a/b", "a:b", "a\\b", "a\tb", "ab",
        ":::", "///", "\\\\",
        "a%b", "a%2Fb", "a%25b", "a%252Fb",
        "t::x::y", "t::x_%3A%3A_y", "t::x%3A%3Ay",
        "图片 01", "こんにちは", "مرحبا",
    ]
    outputs = [percent_encode(s) for s in inputs]
    assert len(set(outputs)) == len(inputs), "相异输入必产出相异输出"


def test_percent_encode_escapes_percent_first_under_custom_charset():
    # % 无条件先行转义，不依赖其是否落入自定义 forbidden/allowed 集：
    # 否则 forbidden="/" 时 "/" 与字面 "%2F" 输出同形碰撞
    assert percent_encode("/", forbidden="/") == "%2F"
    assert percent_encode("%2F", forbidden="/") == "%252F"
    assert percent_encode("%", forbidden="/") == "%25"
    assert percent_encode("a/%", forbidden="/") == "a%2F%25"
    # allowed 模式同样先行转义
    assert percent_encode("a%b", allowed="ab") == "a%25b"


def test_percent_encode_rejects_non_str():
    # 隐式 str() 强转会让 None 与字面 "None" 同像碰撞（多对一，破坏单射契约），
    # 非 str 输入入口显式拒绝，不做静默强转
    with pytest.raises(TypeError):
        percent_encode(None)
    with pytest.raises(TypeError):
        percent_encode(123)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(("%", "%25"))
@example(("t::x::y", "t::x%3A%3Ay"))
@example(("a%2Fb", "a/b"))
@given(pair=distinct_pair)
def test_percent_encode_default_forbidden_is_injective(pair):
    """任意 x != y：默认禁止集下 encode(x) != encode(y)（全域单射）。"""
    x, y = pair
    assert percent_encode(x) != percent_encode(y)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(("中", "中中"))
@given(pair=distinct_pair)
def test_percent_encode_allowlist_mode_is_injective(pair):
    """任意 x != y：allowlist 模式（content_id 字符集）下同样单射。"""
    x, y = pair
    assert percent_encode(x, allowed=CONTENT_ID_ALLOWED) != percent_encode(
        y, allowed=CONTENT_ID_ALLOWED
    )


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example("")
@example("%/:\\\x00")
@given(text=any_text)
def test_percent_encode_output_has_no_forbidden_or_bare_percent(text):
    """输出不含默认禁止字符（/ \\ :），且每个 % 都以 %XX 形态出现。"""
    out = percent_encode(text)
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
def test_percent_encode_roundtrip_via_reference_decoder(text):
    """参考解码器（%XX → 字节、其余字符原样）逆映射恒等于原文（可逆性）。"""
    out = percent_encode(text)
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
def test_percent_escaped_first_prevents_aliasing(base):
    """朴素按序替换会混淆的输入对（% 片段 vs 被转义字符）在单射编码下必不混淆。"""
    assert percent_encode(base + "%25") != percent_encode(base + "%")
    assert percent_encode("%25" + base) != percent_encode("%" + base)
    assert percent_encode(base + "%2F") != percent_encode(base + "/")
    assert percent_encode(base + "%3A") != percent_encode(base + ":")


# ── encode_identifier：声称范围内的单射、截断兜底与长度上限 ────────────


def test_encode_identifier_fallbacks_and_limits():
    # 空值哨兵处于编码像集之外：与字面 "untitled" 等任何真实输入零碰撞
    assert encode_identifier("") == EMPTY_SENTINEL
    assert encode_identifier(None) == EMPTY_SENTINEL
    assert encode_identifier("untitled") == "untitled"
    assert encode_identifier("") != encode_identifier("untitled")
    assert encode_content_id("") == EMPTY_SENTINEL
    assert encode_content_id("untitled") == "untitled"
    assert encode_content_id("") != encode_content_id("untitled")
    assert encode_identifier("", fallback="custom") == "custom"

    # 超长截断带像集外标记 + 64 位 SHA-256 全文指纹后缀
    long_a = "x" * 200
    long_b = "x" * 150 + "y" + "x" * 49
    res_a = encode_identifier(long_a, max_len=120)
    res_b = encode_identifier(long_b, max_len=120)
    assert len(res_a) <= 120
    assert len(res_b) <= 120
    assert res_a != res_b
    assert res_a.endswith("%_" + hashlib.sha256(long_a.encode()).hexdigest()[:16])


def test_encode_identifier_truncation_aligns_percent_escape():
    # 截断命中 %XX 序列内部时，对齐修剪不完整碎片
    res_one = encode_identifier("aaaa////", max_len=20)
    prefix_one = res_one.rsplit("_", 1)[0]
    assert not prefix_one.endswith("%")
    assert not (len(prefix_one) >= 2 and prefix_one[-2] == "%")
    assert len(res_one) <= 20

    res_two = encode_identifier("aaaa////", max_len=21)
    prefix_two = res_two.rsplit("_", 1)[0]
    assert not prefix_two.endswith("%")
    assert not (len(prefix_two) >= 2 and prefix_two[-2] == "%")
    assert len(res_two) <= 21


def test_encode_job_component_delegation_and_invariants():
    assert encode_job_component("a::b/c\\d") == "a%3A%3Ab%2Fc%5Cd"
    assert encode_job_component("") == EMPTY_SENTINEL
    assert "::" not in encode_job_component("x::y")
    assert encode_job_component("abc-_.123") == "abc-_.123"


def test_encode_family_rejects_non_str_non_none():
    """非 str 非 None 入参显式 TypeError 拒绝，与 percent_encode 拒绝原则对齐。

    隐式 str() 强转会让 int 123 与 str "123" 同像碰撞（多对一，破坏单射
    契约）——三个派生编码函数是公共导出 API 的 job_id 派生防线，wall 去
    重依赖其单射性，跨型同像会静默吞任务。
    """
    for fn in (encode_identifier, encode_job_component, encode_content_id):
        for bad in (123, 1.5, True, b"123", ["123"], ("123",)):
            with pytest.raises(TypeError):
                fn(bad)
    # None 与空串仍走哨兵（不拒绝）
    assert encode_identifier(None) == EMPTY_SENTINEL
    assert encode_job_component(None) == EMPTY_SENTINEL
    assert encode_content_id(None) == EMPTY_SENTINEL
    # 拒绝发生在哨兵与截断逻辑之前：即便 max_len 非法也先报 TypeError
    with pytest.raises(TypeError):
        encode_identifier(123, max_len=10)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(pair=("x", "y"), max_len=120)
@example(pair=("untitled", "untitled\x00"), max_len=120)
@given(pair=distinct_pair, max_len=st.integers(min_value=1, max_value=200))
def test_encode_short_inputs_injective_within_claimed_scope(pair, max_len):
    """编码后均 ≤ max_len 的非空输入对：encode 输出必不同（声称的单射域）。"""
    x, y = pair
    ex = percent_encode(x)
    ey = percent_encode(y)
    if not x or not y or len(ex) > max_len or len(ey) > max_len:
        return
    assert encode_identifier(x, max_len=max_len) != encode_identifier(y, max_len=max_len)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(text="x" * 500, max_len=120)
@example(text="aaaaaaaa/////", max_len=20)
@example(text="y", max_len=18)
@given(text=any_text, max_len=st.integers(min_value=18, max_value=200))
def test_encode_output_respects_length_cap_and_aligned_prefix(text, max_len):
    """max_len ≥ 18 时输出恒 ≤ max_len；截断路径带 "%_" 标记 + 16 位指纹
    且前缀无悬挂 % 碎片。"""
    out = encode_identifier(text, max_len=max_len)
    assert len(out) <= max_len
    escaped = percent_encode(text)
    if text and len(escaped) > max_len:
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
        assert out.endswith("%_" + digest)
        assert not _percent_only_as_escapes(out)
        prefix = out[: -(len(digest) + 2)]
        assert not prefix.endswith("%")
        assert not (len(prefix) >= 2 and prefix[-2] == "%")
    elif text:
        assert out == escaped
        assert _percent_only_as_escapes(out)


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
    if len(percent_encode(text_a)) <= max_len:
        return
    out_a = encode_identifier(text_a, max_len=max_len)
    out_b = encode_identifier(text_b, max_len=max_len)
    assert out_a != out_b


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(text="y" * 500, max_len=120)
@given(text=any_text, max_len=st.integers(min_value=18, max_value=200))
def test_truncation_output_never_self_collides_via_passthrough(text, max_len):
    """反例形态锁定：f(f(y)) != f(y)——截断输出不得落入直通域成为定点。"""
    escaped = percent_encode(text)
    if len(escaped) <= max_len:
        return
    out = encode_identifier(text, max_len=max_len)
    # f(out) 只可能走两条路：out 中标记的孤立 % 被再转义为 %25（恒等直通被
    # 排除）；或再次截断，指纹取自全文 out。两者均不与 f(y)=out 相等——
    # 除非 out 与 y 全文相等，而那要求构造出 SHA-256 前像固定点（密码学上
    # 不可行）。
    assert encode_identifier(out, max_len=max_len) != out


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(pair=("u", "y" * 500), max_len=120)
@given(pair=distinct_pair, max_len=st.integers(min_value=18, max_value=200))
def test_truncation_domain_disjoint_from_passthrough_domain(pair, max_len):
    """短输入（直通域）与超长输入（截断域）的输出结构性不相交：f(x) != f(y)。"""
    x, y = pair
    if not x or not y or x == y:
        return
    ex = percent_encode(x)
    ey = percent_encode(y)
    if len(ex) > max_len or len(ey) <= max_len:
        return
    out_x = encode_identifier(x, max_len=max_len)
    out_y = encode_identifier(y, max_len=max_len)
    # 直通输出无孤立 %；截断输出必带 "%_" 标记（孤立 %），两域不相交
    assert _percent_only_as_escapes(out_x)
    assert not _percent_only_as_escapes(out_y)
    assert out_x != out_y


@pytest.mark.hypothesis
@settings(max_examples=30, deadline=None)
@example(text="x" * 100, max_len=5)
@example(text="y", max_len=17)
@given(text=any_text, max_len=st.integers(min_value=1, max_value=17))
def test_small_max_len_passthrough_only_or_fails_loud(text, max_len):
    """max_len < 18：直通输入恒等且守上限；需截断时 fail-loud 拒绝，
    绝不静默破上限。"""
    if not text:
        # 空值哨兵为固定形态，不受 max_len 收缩（docstring 明示）
        assert encode_identifier(text, max_len=max_len) == EMPTY_SENTINEL
        assert encode_content_id(text, max_len=max_len) == EMPTY_SENTINEL
        return
    escaped = percent_encode(text)
    escaped_cid = percent_encode(text, allowed=CONTENT_ID_ALLOWED)
    if len(escaped) <= max_len:
        assert encode_identifier(text, max_len=max_len) == escaped
    else:
        with pytest.raises(ValueError):
            encode_identifier(text, max_len=max_len)
    if len(escaped_cid) <= max_len:
        assert encode_content_id(text, max_len=max_len) == escaped_cid
    else:
        with pytest.raises(ValueError):
            encode_content_id(text, max_len=max_len)


# ── encode_content_id：allowlist 直通与输出字符域 ─────────────────────


def test_encode_content_id_strict_allowlist():
    # 干净字符完全零转义
    assert encode_content_id("12345") == "12345"
    assert encode_content_id("normal-1.2_abc") == "normal-1.2_abc"

    # 包含特殊符号或非 ASCII 触发转义
    assert encode_content_id("a::b") == "a%3A%3Ab"
    assert encode_content_id("中") == "%E4%B8%AD"
    assert encode_content_id("///") == "%2F%2F%2F"


def test_encode_content_id_none_returns_sentinel_outside_image():
    # None 与空值同路返回像集外哨兵：不 str() 成字面 "None" 与
    # encode_content_id("None") 碰撞；哨兵固定形态不受 max_len 收缩
    assert encode_content_id(None) == EMPTY_SENTINEL
    assert encode_content_id(None) != encode_content_id("None")
    assert encode_content_id(None, max_len=3) == EMPTY_SENTINEL
    assert encode_content_id(None, max_len=3) != encode_content_id("None", max_len=4)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(text="normal-1.2_abc", max_len=120)
@example(text="a::b", max_len=120)
@example(text="中", max_len=120)
@given(text=any_text, max_len=st.integers(min_value=18, max_value=200))
def test_content_id_output_charset_is_allowlist_or_escapes(text, max_len):
    """干净输入恒等直通；其余输出字符全部落在 allowlist ∪ {%}，且 % 后跟
    两位大写 hex。"""
    out = encode_content_id(text, max_len=max_len)
    if text and all(c in CONTENT_ID_ALLOWED for c in text) and len(text) <= max_len:
        assert out == text
    elif not text:
        # 空值哨兵为像集外形态（含孤立 %），不适用「% 只以 %XX 出现」约束
        assert out == EMPTY_SENTINEL
    else:
        assert out
        assert all(c in CONTENT_ID_ALLOWED or c == "%" for c in out)
        if len(percent_encode(text, allowed=CONTENT_ID_ALLOWED)) > max_len:
            # 截断输出带像集外标记（孤立 %），豁免「% 只以 %XX 出现」约束
            assert not _percent_only_as_escapes(out)
        else:
            assert _percent_only_as_escapes(out)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(pair=("a", "b"), max_len=120)
@given(pair=distinct_pair, max_len=st.integers(min_value=9, max_value=200))
def test_content_id_short_inputs_injective_within_claimed_scope(pair, max_len):
    """编码后均 ≤ max_len 的非空输入对：content_id 编码输出必不同。"""
    x, y = pair
    ex = percent_encode(x, allowed=CONTENT_ID_ALLOWED)
    ey = percent_encode(y, allowed=CONTENT_ID_ALLOWED)
    if not x or not y or len(ex) > max_len or len(ey) > max_len:
        return
    assert encode_content_id(x, max_len=max_len) != encode_content_id(y, max_len=max_len)


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
    """空值哨兵处于单射编码像集之外：任何非空输入（含字面 untitled）都
    映射不出哨兵。"""
    sentinel_id = encode_identifier("")
    sentinel_cid = encode_content_id("")
    assert encode_identifier(None) == sentinel_id
    assert encode_content_id("") == sentinel_cid
    # 哨兵含孤立 %（像集外形态的充要特征）
    assert not _percent_only_as_escapes(sentinel_id)
    assert not _percent_only_as_escapes(sentinel_cid)
    if not text:
        return
    assert encode_identifier(text) != sentinel_id
    assert encode_content_id(text) != sentinel_cid


# ── safe_uid_filename：单射性与文件系统危险字符逃逸 ────────────────────


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(("t::1", "t::2"))
@example(("t::x::y", "t::x%3A%3Ay"))
@given(pair=distinct_pair)
def test_safe_uid_filename_is_injective(pair):
    """任意 uid_a != uid_b：安全文件名映射必不同（含 '::' 复合 uid 场景）。"""
    uid_a, uid_b = pair
    assert safe_uid_filename(uid_a) != safe_uid_filename(uid_b)


def test_safe_uid_filename_escapes_filesystem_hazards():
    # 路径穿越
    assert "/" not in safe_uid_filename("t::a/../../../etc/passwd")
    assert "\\" not in safe_uid_filename("t::a\\..\\..\\b")
    # Windows 冒号
    assert ":" not in safe_uid_filename("t::a:b")
    # NUL
    assert "\x00" not in safe_uid_filename("t::a\x00b")
    # Glob 元字符
    for ch in "*?[]":
        assert ch not in safe_uid_filename(f"t::a{ch}b")


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example("t::a/../../../etc/passwd")
@example("t::a\x00b*?[")
@given(uid=any_text)
def test_safe_uid_filename_free_of_bare_percent(uid):
    """安全文件名不含裸 %（% 只以 %XX 成对出现，'::' 复合 uid 亦然）。"""
    name = safe_uid_filename(uid)
    assert _percent_only_as_escapes(name)


# ── content_fingerprint：确定性、跨型隔离与序无关 ──────────────────────


def test_content_fingerprint_deterministic_and_version_salted():
    parts = ["encode", "dir/a.mkv", 100, "sub.ass"]
    a = content_fingerprint(parts, version="2")
    assert a == content_fingerprint(parts, version="2")
    assert a != content_fingerprint(parts, version="3")
    assert a != content_fingerprint(["encode", "dir/a.mkv", 101, "sub.ass"], version="2")
    assert len(a) == 16


def test_content_fingerprint_rejects_type_and_delimiter_collisions():
    # 跨型隔离：逐项 str() 强转使 1 与 "1" 不可区分，类型标记后必不同
    assert content_fingerprint([1, "2"]) != content_fingerprint(["1", 2])
    # 定界符注入隔离：项内 \x00 不得伪装项边界
    assert content_fingerprint(["a\x00"]) != content_fingerprint(["a", ""])
    assert content_fingerprint(["a\x00s:b"]) != content_fingerprint(["a", "b"])
    # dict 项按键排序，插入序无关
    assert content_fingerprint([{"a": 1, "b": 2}]) == content_fingerprint([{"b": 2, "a": 1}])


def test_content_fingerprint_nested_containers_order_insensitive():
    # 嵌套结构全 token 化：任意层级 dict（含 dict 作 value）插入序无关，
    # 逻辑相等必同指纹（否则确定性 job_id 分叉 → wall 不命中 → 重复执行）
    assert content_fingerprint([{"k": {"a": 1, "b": 2}}]) == content_fingerprint(
        [{"k": {"b": 2, "a": 1}}]
    )
    assert content_fingerprint([{"a": [{"x": 1, "y": 2}, "s"]}]) == content_fingerprint(
        [{"a": [{"y": 2, "x": 1}, "s"]}]
    )
    assert content_fingerprint([{"k": {"deep": {"z": 0, "a": [1, {"m": 3, "n": 4}]}}}]) == (
        content_fingerprint([{"k": {"deep": {"a": [1, {"n": 4, "m": 3}], "z": 0}}}])
    )
    # 序无关不放宽单射方向：内容不同的嵌套结构、跨型 value 仍必不同
    assert content_fingerprint([{"k": {"a": 1}}]) != content_fingerprint([{"k": {"a": 2}}])
    assert content_fingerprint([{"k": {"a": 1}}]) != content_fingerprint([{"k": {"a": "1"}}])
    assert content_fingerprint([{"k": {"a": 1}}]) != content_fingerprint([{"k": [["a", 1]]}])
