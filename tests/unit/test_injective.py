"""tasklite.utils.injective 专用单射编码测试套件。"""

import hashlib
import pytest
from tasklite.utils.injective import (
    escape_injective,
    sanitize_identifier,
    sanitize_content_id,
    safe_uid_filename,
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
    assert sanitize_identifier("") == "untitled"
    assert sanitize_identifier(None) == "untitled"
    assert sanitize_identifier("", fallback="custom") == "custom"

    # 超长截断带 SHA-256 后缀
    long_a = "x" * 200
    long_b = "x" * 150 + "y" + "x" * 49
    res_a = sanitize_identifier(long_a, max_len=120)
    res_b = sanitize_identifier(long_b, max_len=120)
    assert len(res_a) <= 120
    assert len(res_b) <= 120
    assert res_a != res_b
    assert res_a.endswith("_" + hashlib.sha256(long_a.encode()).hexdigest()[:8])


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
