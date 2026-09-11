"""tasklite.utils.injective 专用单射编码测试套件。"""

import hashlib
import pytest
from tasklite.utils.injective import (
    EMPTY_SENTINEL,
    content_fingerprint,
    escape_injective,
    safe_uid_filename,
    sanitize_content_id,
    sanitize_identifier,
    sanitize_job_component,
)



def test_escape_injective_basic_and_utf8():
    # 基础字符原样保留
    assert escape_injective("hello-world_123") == "hello-world_123"
    # % 优先转义为 %25
    assert escape_injective("100%_pure") == "100%25_pure"
    # 路径分隔符与冒号
    assert escape_injective("a/b:c\\d") == "a%2Fb%3Ac%5Cd"
    # 不可打印控制字符转义
    assert escape_injective("a\tb") == "a%09b"
    # 当指定 allowed 时，非 ASCII / 特殊字符转义为 %XX
    assert escape_injective("中", allowed="abc") == "%E4%B8%AD"
    assert escape_injective("é", allowed="abc") == "%C3%A9"



def test_escape_injective_bijection_no_collisions():
    inputs = [
        "a/b", "a:b", "a\\b", "a\tb", "ab",
        ":::", "///", "\\\\",
        "a%b", "a%2Fb", "a%25b", "a%252Fb",
        "t::x::y", "t::x_%3A%3A_y", "t::x%3A%3Ay",
        "图片 01", "こんにちは", "مرحبا",
    ]
    outputs = [escape_injective(s) for s in inputs]
    assert len(set(outputs)) == len(outputs), "All distinct inputs must produce distinct outputs"


def test_sanitize_identifier_fallbacks_and_limits():
    # 空值哨兵处于单射转义像集之外：与字面 "untitled" 等任何真实输入零碰撞
    assert sanitize_identifier("") == EMPTY_SENTINEL
    assert sanitize_identifier(None) == EMPTY_SENTINEL
    assert sanitize_identifier("untitled") == "untitled"
    assert sanitize_identifier("") != sanitize_identifier("untitled")
    assert sanitize_content_id("") == EMPTY_SENTINEL
    assert sanitize_content_id("untitled") == "untitled"
    assert sanitize_content_id("") != sanitize_content_id("untitled")
    assert sanitize_identifier("", fallback="custom") == "custom"

    # 超长截断带像集外标记 + 64 位 SHA-256 全文指纹后缀
    long_a = "x" * 200
    long_b = "x" * 150 + "y" + "x" * 49
    res_a = sanitize_identifier(long_a, max_len=120)
    res_b = sanitize_identifier(long_b, max_len=120)
    assert len(res_a) <= 120
    assert len(res_b) <= 120
    assert res_a != res_b
    assert res_a.endswith("%_" + hashlib.sha256(long_a.encode()).hexdigest()[:16])


def test_sanitize_identifier_truncation_aligns_percent_escape():
    # 截断命中 %XX 序列内部时，对齐修剪不完整碎片
    res1 = sanitize_identifier("aaaa////", max_len=20)
    prefix1 = res1.rsplit("_", 1)[0]
    assert not prefix1.endswith("%")
    assert not (len(prefix1) >= 2 and prefix1[-2] == "%")
    assert len(res1) <= 20

    res2 = sanitize_identifier("aaaa////", max_len=21)
    prefix2 = res2.rsplit("_", 1)[0]
    assert not prefix2.endswith("%")
    assert not (len(prefix2) >= 2 and prefix2[-2] == "%")
    assert len(res2) <= 21



def test_sanitize_content_id_strict_allowlist():
    # 干净字符完全零转义
    assert sanitize_content_id("12345") == "12345"
    assert sanitize_content_id("normal-1.2_abc") == "normal-1.2_abc"

    # 包含特殊符号或非 ASCII 触发转义
    assert sanitize_content_id("a::b") == "a%3A%3Ab"
    assert sanitize_content_id("中") == "%E4%B8%AD"
    assert sanitize_content_id("///") == "%2F%2F%2F"


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


def test_content_fingerprint_deterministic_and_version_salted():
    a = content_fingerprint(["encode", "dir/a.mkv", 100, "sub.ass"], version="2")
    assert a == content_fingerprint(["encode", "dir/a.mkv", 100, "sub.ass"], version="2")
    assert a != content_fingerprint(["encode", "dir/a.mkv", 100, "sub.ass"], version="3")
    assert a != content_fingerprint(["encode", "dir/a.mkv", 101, "sub.ass"], version="2")
    assert len(a) == 16


def test_sanitize_job_component_delegation_and_invariants():
    assert sanitize_job_component("a::b/c\\d") == "a%3A%3Ab%2Fc%5Cd"
    assert sanitize_job_component("") == EMPTY_SENTINEL
    assert "::" not in sanitize_job_component("x::y")
    assert sanitize_job_component("abc-_.123") == "abc-_.123"

