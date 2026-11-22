"""单射编码族契约测试（确定性用例 + hypothesis 属性测试）。

被测契约范围（严格按各函数 docstring 声称）：
- ``percent_encode`` / ``encode_job_uid_filename``：全域单射（任意 x != y ⟹
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

from tasklite.exceptions import TaskLiteError
from tasklite.models.job import Job
from tasklite.utils.encoding import (
    CONTENT_ID_ALLOWED,
    EMPTY_SENTINEL,
    encode_content_id,
    encode_identifier,
    make_job_uid,
    parse_job_uid,
    percent_encode,
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
    assert percent_encode("hello-world_123") == "hello-world_123"
    assert percent_encode("100%_pure") == "100%25_pure"
    assert percent_encode("a/b:c\\d") == "a%2Fb%3Ac%5Cd"
    assert percent_encode("a\tb") == "a%09b"
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
    assert percent_encode("/", forbidden="/") == "%2F"
    assert percent_encode("%2F", forbidden="/") == "%252F"
    assert percent_encode("%", forbidden="/") == "%25"
    assert percent_encode("a/%", forbidden="/") == "a%2F%25"
    assert percent_encode("a%b", allowed="ab") == "a%25b"


def test_percent_encode_rejects_non_str():
    with pytest.raises(TaskLiteError):
        percent_encode(None)  # type: ignore[arg-type]
    with pytest.raises(TaskLiteError):
        percent_encode(123)  # type: ignore[arg-type]


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
    assert encode_identifier("") == EMPTY_SENTINEL
    assert encode_identifier(None) == EMPTY_SENTINEL
    assert encode_identifier("untitled") == "untitled"
    assert encode_identifier("") != encode_identifier("untitled")
    assert encode_content_id("") == EMPTY_SENTINEL
    assert encode_content_id("untitled") == "untitled"
    assert encode_content_id("") != encode_content_id("untitled")
    assert encode_identifier("", fallback="custom") == "custom"

    long_a = "x" * 200
    long_b = "x" * 150 + "y" + "x" * 49
    res_a = encode_identifier(long_a, max_len=120)
    res_b = encode_identifier(long_b, max_len=120)
    assert len(res_a) <= 120
    assert len(res_b) <= 120
    assert res_a != res_b
    assert res_a.endswith("%_" + hashlib.sha256(long_a.encode()).hexdigest()[:16])


def test_encode_identifier_truncation_aligns_percent_escape():
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


def test_encode_identifier_component_invariants():
    assert encode_identifier("a::b/c\\d") == "a%3A%3Ab%2Fc%5Cd"
    assert encode_identifier("") == EMPTY_SENTINEL
    assert "::" not in encode_identifier("x::y")
    assert encode_identifier("abc-_.123") == "abc-_.123"


def test_encode_family_rejects_non_str_non_none():
    for fn in (encode_identifier, encode_content_id):
        for bad in (123, 1.5, True, b"123", ["123"], ("123",)):
            with pytest.raises(TaskLiteError):
                fn(bad)  # type: ignore[arg-type]
    assert encode_identifier(None) == EMPTY_SENTINEL
    assert encode_content_id(None) == EMPTY_SENTINEL
    with pytest.raises(TaskLiteError):
        encode_identifier(123, max_len=10)  # type: ignore[arg-type]


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(pair=("x", "y"), max_len=120)
@example(pair=("untitled", "untitled\x00"), max_len=120)
@given(pair=distinct_pair, max_len=st.integers(min_value=1, max_value=200))
def test_encode_short_inputs_injective_within_claimed_scope(pair, max_len):
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
    escaped = percent_encode(text)
    if len(escaped) <= max_len:
        return
    out = encode_identifier(text, max_len=max_len)
    assert encode_identifier(out, max_len=max_len) != out


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(pair=("u", "y" * 500), max_len=120)
@given(pair=distinct_pair, max_len=st.integers(min_value=18, max_value=200))
def test_truncation_domain_disjoint_from_passthrough_domain(pair, max_len):
    x, y = pair
    if not x or not y or x == y:
        return
    ex = percent_encode(x)
    ey = percent_encode(y)
    if len(ex) > max_len or len(ey) <= max_len:
        return
    out_x = encode_identifier(x, max_len=max_len)
    out_y = encode_identifier(y, max_len=max_len)
    assert _percent_only_as_escapes(out_x)
    assert not _percent_only_as_escapes(out_y)
    assert out_x != out_y


@pytest.mark.hypothesis
@settings(max_examples=30, deadline=None)
@example(text="x" * 100, max_len=5)
@example(text="y", max_len=17)
@given(text=any_text, max_len=st.integers(min_value=1, max_value=17))
def test_small_max_len_passthrough_only_or_fails_loud(text, max_len):
    if not text:
        assert encode_identifier(text, max_len=max_len) == EMPTY_SENTINEL
        assert encode_content_id(text, max_len=max_len) == EMPTY_SENTINEL
        return
    escaped = percent_encode(text)
    escaped_cid = percent_encode(text, allowed=CONTENT_ID_ALLOWED)
    if len(escaped) <= max_len:
        assert encode_identifier(text, max_len=max_len) == escaped
    else:
        with pytest.raises(TaskLiteError):
            encode_identifier(text, max_len=max_len)
    if len(escaped_cid) <= max_len:
        assert encode_content_id(text, max_len=max_len) == escaped_cid
    else:
        with pytest.raises(TaskLiteError):
            encode_content_id(text, max_len=max_len)


# ── encode_content_id：allowlist 直通与输出字符域 ─────────────────────


def test_encode_content_id_strict_allowlist():
    assert encode_content_id("12345") == "12345"
    assert encode_content_id("normal-1.2_abc") == "normal-1.2_abc"
    assert encode_content_id("a::b") == "a%3A%3Ab"
    assert encode_content_id("中") == "%E4%B8%AD"
    assert encode_content_id("///") == "%2F%2F%2F"


def test_encode_content_id_none_returns_sentinel_outside_image():
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
    out = encode_content_id(text, max_len=max_len)
    if text and all(c in CONTENT_ID_ALLOWED for c in text) and len(text) <= max_len:
        assert out == text
    elif not text:
        assert out == EMPTY_SENTINEL
    else:
        assert out
        assert all(c in CONTENT_ID_ALLOWED or c == "%" for c in out)
        if len(percent_encode(text, allowed=CONTENT_ID_ALLOWED)) > max_len:
            assert not _percent_only_as_escapes(out)
        else:
            assert _percent_only_as_escapes(out)


@pytest.mark.hypothesis
@settings(max_examples=50, deadline=None)
@example(pair=("a", "b"), max_len=120)
@given(pair=distinct_pair, max_len=st.integers(min_value=9, max_value=200))
def test_content_id_short_inputs_injective_within_claimed_scope(pair, max_len):
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
    sentinel_id = encode_identifier("")
    sentinel_cid = encode_content_id("")
    assert encode_identifier(None) == sentinel_id
    assert encode_content_id("") == sentinel_cid
    assert not _percent_only_as_escapes(sentinel_id)
    assert not _percent_only_as_escapes(sentinel_cid)
    if not text:
        return
    assert encode_identifier(text) != sentinel_id
    assert encode_content_id(text) != sentinel_cid


# ── make_job_uid / parse_job_uid：透明可逆的 Job 身份 ────────────────


non_empty_text = st.one_of(
    st.text(min_size=1, max_size=60),
    st.text(alphabet=_TORTURE_ALPHABET, min_size=1, max_size=40),
)


def test_make_and_parse_job_uid_preserves_printable_and_escapes_only_required():
    uid = make_job_uid("process/sub\\task", "report-001 *?[] 日本語 🚀")
    assert uid == "process/sub\\task::report-001 *?[] 日本語 🚀"
    assert parse_job_uid(uid) == ("process/sub\\task", "report-001 *?[] 日本語 🚀")
    job = Job("process/sub\\task", "report-001 *?[] 日本語 🚀")
    assert job.uid == uid
    assert job.task_type == "process/sub\\task"
    assert job.job_id == "report-001 *?[] 日本語 🚀"

    uid2 = make_job_uid("a:b%c\t\n\x00", "x::y%25\x1b\x7f\u2028")
    assert uid2 == "a%3Ab%25c%09%0A%00::x%3A%3Ay%2525%1B%7F%E2%80%A8"
    assert parse_job_uid(uid2) == ("a:b%c\t\n\x00", "x::y%25\x1b\x7f\u2028")
    assert Job("a:b%c\t\n\x00", "x::y%25\x1b\x7f\u2028").uid == uid2


def test_make_job_uid_does_not_normalize_or_truncate():
    nfc = "\u00e9"
    nfd = "e\u0301"
    assert nfc != nfd
    uid_nfc = make_job_uid("  Task  ", nfc + "x" * 500)
    uid_nfd = make_job_uid("  Task  ", nfd + "x" * 500)
    assert uid_nfc != uid_nfd
    assert parse_job_uid(uid_nfc) == ("  Task  ", nfc + "x" * 500)
    assert parse_job_uid(uid_nfd) == ("  Task  ", nfd + "x" * 500)


def test_make_and_parse_job_uid_collision_resistance():
    pairs = [
        ("a", "b::c"),
        ("a::b", "c"),
        ("a", "b%3A%3Ac"),
        ("a%3A%3Ab", "c"),
        ("a%", "3Ab"),
        ("a", ":b"),
        ("a:", "b"),
        ("t", "%"),
        ("t", "%25"),
        ("t", "%2525"),
        ("t", "/"),
        ("t", "%2F"),
    ]
    uids = [make_job_uid(a, b) for a, b in pairs]
    assert len(set(uids)) == len(pairs)
    for (a, b), u in zip(pairs, uids):
        assert parse_job_uid(u) == (a, b)
        assert Job(a, b).uid == u


@pytest.mark.hypothesis
@settings(max_examples=80, deadline=None)
@example(a="a::b", b="c%3A%3Ad")
@example(a="%25", b="%")
@example(a="path/to/file", b="name\x00\t\n:1")
@given(a=non_empty_text, b=non_empty_text)
def test_make_parse_job_uid_roundtrip_property(a, b):
    uid = make_job_uid(a, b)
    assert parse_job_uid(uid) == (a, b)
    assert Job(a, b).uid == uid


@pytest.mark.hypothesis
@settings(max_examples=80, deadline=None)
@example(p1=("a::b", "c"), p2=("a", "b::c"))
@example(p1=("t", ":"), p2=("t", "%3A"))
@given(
    p1=st.tuples(non_empty_text, non_empty_text),
    p2=st.tuples(non_empty_text, non_empty_text),
)
def test_make_job_uid_injective_property(p1, p2):
    if p1 == p2:
        return
    assert make_job_uid(*p1) != make_job_uid(*p2)


def test_make_job_uid_rejects_invalid_inputs_with_tasklite_error():
    for bad in ("", None, 123, 1.5, True, b"a", ["a"]):
        with pytest.raises(TaskLiteError):
            make_job_uid(bad, "id")  # type: ignore[arg-type]
        with pytest.raises(TaskLiteError):
            make_job_uid("type", bad)  # type: ignore[arg-type]
    with pytest.raises(TaskLiteError):
        make_job_uid("t", "\ud800")


def test_parse_job_uid_rejects_non_canonical_uids_with_tasklite_error():
    invalid_uids = [
        None,
        123,
        "",
        "no_separator",
        "::id",
        "type::",
        "::",
        "a::b::c",
        "a:b::c",
        "a::b:c",
        "a%b::c",
        "a::b%",
        "a::b%2",
        "a::b%3a",
        "a::b%GG",
        "a::%41",
        "a::%2F",
        "a\tb::c",
        "a::b\x00c",
        "a::%FF",
    ]
    for bad in invalid_uids:
        with pytest.raises(TaskLiteError):
            parse_job_uid(bad)  # type: ignore[arg-type]


def test_top_level_exports_identity_and_exception():
    import tasklite

    assert tasklite.make_job_uid is make_job_uid
    assert tasklite.parse_job_uid is parse_job_uid
    assert tasklite.TaskLiteError is TaskLiteError
    assert "make_job_uid" in tasklite.__all__
    assert "parse_job_uid" in tasklite.__all__
    assert "TaskLiteError" in tasklite.__all__
